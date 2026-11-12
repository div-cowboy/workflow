import type { Queue, Storage, ValidQueueName } from '@workflow/world';
import {
  getQueueTopicPrefix,
  getRunQueueNamespace,
  resolveQueueNamespace,
} from '@workflow/world';
import { and, eq, inArray } from 'drizzle-orm';
import type { Drizzle } from './drizzle/index.js';
import * as Schema from './drizzle/schema.js';

/**
 * Prefix for the Graphile `jobKey` attached to startup-recovery enqueues.
 *
 * This is a queue-job identity, not a message `idempotencyKey`. Putting the
 * recovery key on the message would skip per-run replay serialization and
 * could replace a parked run's own future wake job. Graphile's default
 * `preserve_run_at` mode updates an unlocked job holding the same key
 * instead of inserting another, so repeated boots cannot accumulate
 * duplicate recovery jobs. Once the job completes the key is released, so
 * later legitimate wakes are never suppressed.
 *
 * See https://github.com/vercel/workflow/issues/3119.
 */
export const STARTUP_RECOVERY_JOB_KEY_PREFIX = 'startup-recovery:';

export function startupRecoveryJobKey(runId: string): string {
  return `${STARTUP_RECOVERY_JOB_KEY_PREFIX}${runId}`;
}

/** Step statuses that represent interrupted, still-runnable work. */
const RUNNABLE_STEP_STATUSES = ['pending', 'running'] as const;

type RecoverableRun = {
  runId: string;
  workflowName: string;
  status: 'pending' | 'running' | string;
  specVersion?: number;
  executionContext?: unknown;
};

/**
 * Classify a page of `running` runs into runnable vs. durably parked using
 * only persisted state.
 *
 * A run counts as runnable (worth replaying on startup) when:
 * - `wake_pending` is set: a wake event was stored and the orchestrator has
 *   not yet completed a delivery for it (the crash window after
 *   `hook_received` / `wait_completed` / a terminal step and before its
 *   queue job is inserted), or
 * - it has a step in a non-terminal state (interrupted step work), or
 * - it has a `waiting` wait whose `resumeAt` is already due, or
 * - it has no persisted suspension state at all (no open hooks, no waiting
 *   waits, no runnable steps). Such a run crashed between making progress
 *   and suspending, so it must be replayed; we fail open for this
 *   ambiguous case because the recovery job key keeps it from accumulating
 *   jobs across restarts.
 *
 * A run counts as parked (skipped) when `wake_pending` is false and its
 * only live state is open hooks and/or waits that are not yet due. Hook
 * deliveries and due timers enqueue their own wake-up jobs, and those jobs
 * live durably in the same Postgres database, so they survive restarts.
 * "A hook row exists" is not enough on its own: a crash between
 * `hook_received` and the wake enqueue leaves exactly that shape, which is
 * why `wake_pending` is the consumed/acknowledged checkpoint.
 */
async function classifyRunnableRuns(
  drizzle: Drizzle,
  runIds: string[],
  now: Date
): Promise<Set<string>> {
  const [stepRows, waitRows, hookRows, runRows] = await Promise.all([
    drizzle
      .select({ runId: Schema.steps.runId })
      .from(Schema.steps)
      .where(
        and(
          inArray(Schema.steps.runId, runIds),
          inArray(Schema.steps.status, [...RUNNABLE_STEP_STATUSES])
        )
      ),
    drizzle
      .select({ runId: Schema.waits.runId, resumeAt: Schema.waits.resumeAt })
      .from(Schema.waits)
      .where(
        and(
          inArray(Schema.waits.runId, runIds),
          eq(Schema.waits.status, 'waiting')
        )
      ),
    drizzle
      .select({ runId: Schema.hooks.runId })
      .from(Schema.hooks)
      .where(inArray(Schema.hooks.runId, runIds)),
    drizzle
      .select({
        runId: Schema.runs.runId,
        wakePending: Schema.runs.wakePending,
      })
      .from(Schema.runs)
      .where(inArray(Schema.runs.runId, runIds)),
  ]);

  const runnableStepRunIds = new Set(stepRows.map((row) => row.runId));
  const waitingRunIds = new Set(waitRows.map((row) => row.runId));
  const dueWaitRunIds = new Set(
    waitRows
      .filter(
        (row) => row.resumeAt != null && row.resumeAt.getTime() <= now.getTime()
      )
      .map((row) => row.runId)
  );
  const openHookRunIds = new Set(hookRows.map((row) => row.runId));
  const wakePendingRunIds = new Set(
    runRows.filter((row) => row.wakePending).map((row) => row.runId)
  );

  const runnable = new Set<string>();
  for (const runId of runIds) {
    if (
      wakePendingRunIds.has(runId) ||
      runnableStepRunIds.has(runId) ||
      dueWaitRunIds.has(runId)
    ) {
      runnable.add(runId);
      continue;
    }
    if (openHookRunIds.has(runId) || waitingRunIds.has(runId)) {
      // Durably parked: only live state is an unresolved hook and/or a wait
      // that is not due yet, and no unconsumed orchestrator wake.
      continue;
    }
    // No persisted suspension state — cannot be classified as parked, so
    // fail open and recover it.
    runnable.add(runId);
  }
  return runnable;
}

/**
 * Classify a page of runs with fail-open semantics: pending runs are always
 * runnable, and if the persisted-state queries fail the whole page is
 * treated as runnable (the stable job key still prevents duplicate job
 * accumulation across restarts).
 */
async function classifyPageSafe(
  drizzle: Drizzle,
  pageRuns: RecoverableRun[]
): Promise<Set<string> | null> {
  const runningIds = pageRuns
    .filter((run) => run.status === 'running')
    .map((run) => run.runId);
  if (runningIds.length === 0) {
    return null;
  }
  try {
    const runnableRunning = await classifyRunnableRuns(
      drizzle,
      runningIds,
      new Date()
    );
    const runnable = new Set<string>();
    for (const run of pageRuns) {
      if (run.status !== 'running' || runnableRunning.has(run.runId)) {
        runnable.add(run.runId);
      }
    }
    return runnable;
  } catch (err) {
    console.warn(
      `[world-postgres] Failed to classify parked runs during startup recovery, ` +
        `re-enqueueing all active runs in page: ${err}`
    );
    return null;
  }
}

async function recoverPage(
  drizzle: Drizzle,
  enqueue: Queue['queue'],
  defaultNamespace: string | undefined,
  pageRuns: RecoverableRun[]
): Promise<{ reenqueued: number; skippedParked: number }> {
  const runnableRunIds = await classifyPageSafe(drizzle, pageRuns);
  let reenqueued = 0;
  let skippedParked = 0;
  for (const run of pageRuns) {
    if (runnableRunIds && !runnableRunIds.has(run.runId)) {
      skippedParked++;
      continue;
    }
    if (await enqueueRecoveryJob(enqueue, defaultNamespace, run)) {
      reenqueued++;
    }
  }
  return { reenqueued, skippedParked };
}

async function enqueueRecoveryJob(
  enqueue: Queue['queue'],
  defaultNamespace: string | undefined,
  run: RecoverableRun
): Promise<boolean> {
  try {
    const prefix = getQueueTopicPrefix(
      'workflow',
      getRunQueueNamespace(run.executionContext) ?? defaultNamespace
    );
    const queueName: ValidQueueName = `${prefix}${run.workflowName}`;
    await enqueue(
      queueName,
      {
        runId: run.runId,
        ...(typeof run.specVersion === 'number' && {
          runSpecVersion: run.specVersion,
        }),
      },
      { jobKey: startupRecoveryJobKey(run.runId) }
    );
    return true;
  } catch (err) {
    console.warn(
      `[world-postgres] Failed to re-enqueue run ${run.runId}: ${err}`
    );
    return false;
  }
}

/**
 * Re-enqueue persisted runnable workflow runs so they resume processing
 * after a world restart.
 *
 * Unlike the generic `reenqueueActiveRuns` helper in `@workflow/world`,
 * this Postgres-specific variant:
 * - skips runs that are durably parked on unresolved hooks or future waits
 *   (their wake-up jobs are persisted in the same database and survive the
 *   restart), recovering the crash window where a wake event is stored but
 *   its queue job is not via `workflow_runs.wake_pending`, and
 * - attaches a stable per-run Graphile job key so repeated restarts replace
 *   the outstanding recovery job instead of accumulating duplicates, without
 *   stamping a message-level idempotency key.
 *
 * See https://github.com/vercel/workflow/issues/3119.
 */
export async function reenqueueRecoverableRuns({
  runs,
  drizzle,
  enqueue,
  namespace,
}: {
  runs: Storage['runs'];
  drizzle: Drizzle;
  enqueue: Queue['queue'];
  namespace?: string;
}): Promise<void> {
  const defaultNamespace = resolveQueueNamespace(namespace);
  // Validates the default up front, as before: a bad WORKFLOW_QUEUE_NAMESPACE
  // fails recovery once instead of once per run.
  getQueueTopicPrefix('workflow', defaultNamespace);

  let reenqueued = 0;
  let skippedParked = 0;
  let cursor: string | undefined;
  let hasMore = true;
  while (hasMore) {
    const page = await runs.list({
      status: ['pending', 'running'],
      resolveData: 'none',
      pagination: { cursor },
    });
    const result = await recoverPage(
      drizzle,
      enqueue,
      defaultNamespace,
      page.data
    );
    reenqueued += result.reenqueued;
    skippedParked += result.skippedParked;
    hasMore = page.hasMore;
    cursor = page.cursor ?? undefined;
  }
  if ((reenqueued > 0 || skippedParked > 0) && isDebugEnabled()) {
    console.debug(
      `[world-postgres] Re-enqueued ${reenqueued} runnable run(s) on startup` +
        (skippedParked > 0
          ? ` (skipped ${skippedParked} durably parked run(s))`
          : '')
    );
  }
}

function isDebugEnabled(): boolean {
  const debug = typeof process !== 'undefined' ? process.env.DEBUG : undefined;
  if (typeof debug !== 'string') return false;
  return debug.includes('workflow:') || debug === '*';
}
