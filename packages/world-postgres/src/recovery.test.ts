import type { Storage } from '@workflow/world';
import { describe, expect, it, vi } from 'vitest';
import type { Drizzle } from './drizzle/index.js';
import * as Schema from './drizzle/schema.js';
import {
  reenqueueRecoverableRuns,
  STARTUP_RECOVERY_JOB_KEY_PREFIX,
  startupRecoveryJobKey,
} from './recovery.js';

type FakeRun = {
  runId: string;
  workflowName: string;
  status?: 'pending' | 'running';
};

type FakePersistedState = {
  steps?: Array<{ runId: string; status: string }>;
  waits?: Array<{ runId: string; status: string; resumeAt: Date | null }>;
  hooks?: Array<{ runId: string }>;
  wakePending?: string[];
};

/**
 * Minimal fake for the drizzle `select().from().where()` chains used by the
 * startup-recovery classifier. Applies the same status filters the real SQL
 * applies, keyed off the schema table identity.
 */
function createFakeDrizzle(state: FakePersistedState): Drizzle {
  return {
    select: (_fields: unknown) => ({
      from: (table: unknown) => ({
        where: (_condition: unknown) => {
          if (table === Schema.steps) {
            return Promise.resolve(
              (state.steps ?? [])
                .filter(
                  (step) =>
                    step.status === 'pending' || step.status === 'running'
                )
                .map(({ runId }) => ({ runId }))
            );
          }
          if (table === Schema.waits) {
            return Promise.resolve(
              (state.waits ?? [])
                .filter((wait) => wait.status === 'waiting')
                .map(({ runId, resumeAt }) => ({ runId, resumeAt }))
            );
          }
          if (table === Schema.hooks) {
            return Promise.resolve(
              (state.hooks ?? []).map(({ runId }) => ({ runId }))
            );
          }
          if (table === Schema.runs) {
            const pending = new Set(state.wakePending ?? []);
            const runIds = new Set<string>([
              ...(state.steps ?? []).map((row) => row.runId),
              ...(state.waits ?? []).map((row) => row.runId),
              ...(state.hooks ?? []).map((row) => row.runId),
              ...pending,
            ]);
            return Promise.resolve(
              [...runIds].map((runId) => ({
                runId,
                wakePending: pending.has(runId),
              }))
            );
          }
          return Promise.reject(
            new Error('Unexpected table in recovery query')
          );
        },
      }),
    }),
  } as unknown as Drizzle;
}

function createFakeRuns(runs: FakeRun[]): Storage['runs'] {
  return {
    list: vi.fn(async () => ({
      data: runs.map((run) => ({
        ...run,
        status: run.status ?? 'running',
      })),
      hasMore: false,
      cursor: null,
    })),
  } as unknown as Storage['runs'];
}

function createEnqueueSpy() {
  return vi.fn(async () => ({ messageId: null }));
}

const FUTURE = new Date(Date.now() + 60 * 60 * 1000);
const PAST = new Date(Date.now() - 60 * 60 * 1000);

describe('reenqueueRecoverableRuns', () => {
  it('re-enqueues pending runs with a stable recovery job key, not an idempotency key', async () => {
    const enqueue = createEnqueueSpy();
    await reenqueueRecoverableRuns({
      runs: createFakeRuns([
        { runId: 'wrun_pending', workflowName: 'wfA', status: 'pending' },
      ]),
      drizzle: createFakeDrizzle({}),
      enqueue,
    });

    expect(enqueue).toHaveBeenCalledTimes(1);
    expect(enqueue).toHaveBeenCalledWith(
      expect.stringContaining('wfA'),
      { runId: 'wrun_pending' },
      { jobKey: `${STARTUP_RECOVERY_JOB_KEY_PREFIX}wrun_pending` }
    );
    expect(
      (enqueue.mock.calls[0]?.[2] as { idempotencyKey?: string } | undefined)
        ?.idempotencyKey
    ).toBeUndefined();
  });

  it('re-enqueues running runs with interrupted step work', async () => {
    const enqueue = createEnqueueSpy();
    await reenqueueRecoverableRuns({
      runs: createFakeRuns([{ runId: 'wrun_step', workflowName: 'wfA' }]),
      drizzle: createFakeDrizzle({
        steps: [{ runId: 'wrun_step', status: 'running' }],
      }),
      enqueue,
    });

    expect(enqueue).toHaveBeenCalledTimes(1);
    expect(enqueue).toHaveBeenCalledWith(
      expect.anything(),
      { runId: 'wrun_step' },
      { jobKey: startupRecoveryJobKey('wrun_step') }
    );
  });

  it('skips running runs parked solely on unresolved hooks', async () => {
    const enqueue = createEnqueueSpy();
    await reenqueueRecoverableRuns({
      runs: createFakeRuns([{ runId: 'wrun_hook', workflowName: 'wfA' }]),
      drizzle: createFakeDrizzle({
        hooks: [{ runId: 'wrun_hook' }],
        // Completed steps do not make a run runnable.
        steps: [{ runId: 'wrun_hook', status: 'completed' }],
      }),
      enqueue,
    });

    expect(enqueue).not.toHaveBeenCalled();
  });

  it('re-enqueues a hook-parked run whose wake was stored but not yet delivered', async () => {
    const enqueue = createEnqueueSpy();
    await reenqueueRecoverableRuns({
      runs: createFakeRuns([{ runId: 'wrun_wake', workflowName: 'wfA' }]),
      drizzle: createFakeDrizzle({
        hooks: [{ runId: 'wrun_wake' }],
        wakePending: ['wrun_wake'],
      }),
      enqueue,
    });

    expect(enqueue).toHaveBeenCalledTimes(1);
    expect(enqueue).toHaveBeenCalledWith(
      expect.anything(),
      { runId: 'wrun_wake' },
      { jobKey: startupRecoveryJobKey('wrun_wake') }
    );
  });

  it('skips running runs parked on a wait that is not due yet', async () => {
    const enqueue = createEnqueueSpy();
    await reenqueueRecoverableRuns({
      runs: createFakeRuns([{ runId: 'wrun_sleep', workflowName: 'wfA' }]),
      drizzle: createFakeDrizzle({
        waits: [{ runId: 'wrun_sleep', status: 'waiting', resumeAt: FUTURE }],
      }),
      enqueue,
    });

    expect(enqueue).not.toHaveBeenCalled();
  });

  it('skips running runs parked on an indefinite wait (no resumeAt)', async () => {
    const enqueue = createEnqueueSpy();
    await reenqueueRecoverableRuns({
      runs: createFakeRuns([{ runId: 'wrun_wait', workflowName: 'wfA' }]),
      drizzle: createFakeDrizzle({
        waits: [{ runId: 'wrun_wait', status: 'waiting', resumeAt: null }],
      }),
      enqueue,
    });

    expect(enqueue).not.toHaveBeenCalled();
  });

  it('re-enqueues running runs whose wait is already due', async () => {
    const enqueue = createEnqueueSpy();
    await reenqueueRecoverableRuns({
      runs: createFakeRuns([{ runId: 'wrun_due', workflowName: 'wfA' }]),
      drizzle: createFakeDrizzle({
        waits: [{ runId: 'wrun_due', status: 'waiting', resumeAt: PAST }],
      }),
      enqueue,
    });

    expect(enqueue).toHaveBeenCalledTimes(1);
    expect(enqueue).toHaveBeenCalledWith(
      expect.anything(),
      { runId: 'wrun_due' },
      { jobKey: startupRecoveryJobKey('wrun_due') }
    );
  });

  it('fails open for running runs with no persisted suspension state', async () => {
    const enqueue = createEnqueueSpy();
    await reenqueueRecoverableRuns({
      runs: createFakeRuns([{ runId: 'wrun_ambiguous', workflowName: 'wfA' }]),
      drizzle: createFakeDrizzle({}),
      enqueue,
    });

    expect(enqueue).toHaveBeenCalledTimes(1);
    expect(enqueue).toHaveBeenCalledWith(
      expect.anything(),
      { runId: 'wrun_ambiguous' },
      { jobKey: startupRecoveryJobKey('wrun_ambiguous') }
    );
  });

  it('only recovers the runnable subset of a mixed page', async () => {
    const enqueue = createEnqueueSpy();
    await reenqueueRecoverableRuns({
      runs: createFakeRuns([
        { runId: 'wrun_hook', workflowName: 'wfA' },
        { runId: 'wrun_sleep', workflowName: 'wfA' },
        { runId: 'wrun_step', workflowName: 'wfA' },
        { runId: 'wrun_wake', workflowName: 'wfA' },
        { runId: 'wrun_ambiguous', workflowName: 'wfA' },
      ]),
      drizzle: createFakeDrizzle({
        hooks: [{ runId: 'wrun_hook' }, { runId: 'wrun_wake' }],
        waits: [{ runId: 'wrun_sleep', status: 'waiting', resumeAt: FUTURE }],
        steps: [{ runId: 'wrun_step', status: 'pending' }],
        wakePending: ['wrun_wake'],
      }),
      enqueue,
    });

    const enqueuedRunIds = enqueue.mock.calls.map(
      (call) => (call[1] as { runId: string }).runId
    );
    expect(enqueuedRunIds.sort()).toEqual([
      'wrun_ambiguous',
      'wrun_step',
      'wrun_wake',
    ]);
  });

  it('uses the same job key on every startup so restarts cannot accumulate duplicate jobs', async () => {
    const enqueue = createEnqueueSpy();
    const args = {
      runs: createFakeRuns([{ runId: 'wrun_restart', workflowName: 'wfA' }]),
      drizzle: createFakeDrizzle({
        steps: [{ runId: 'wrun_restart', status: 'running' }],
      }),
      enqueue,
    };

    for (let boot = 0; boot < 5; boot++) {
      await reenqueueRecoverableRuns(args);
    }

    expect(enqueue).toHaveBeenCalledTimes(5);
    const keys = enqueue.mock.calls.map(
      (call) => (call[2] as { jobKey: string }).jobKey
    );
    expect(new Set(keys).size).toBe(1);
    expect(keys[0]).toBe(startupRecoveryJobKey('wrun_restart'));
  });

  it('re-enqueues the whole page when parked-state classification fails', async () => {
    const enqueue = createEnqueueSpy();
    const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
    const brokenDrizzle = {
      select: () => ({
        from: () => ({
          where: () => Promise.reject(new Error('database exploded')),
        }),
      }),
    } as unknown as Drizzle;

    try {
      await reenqueueRecoverableRuns({
        runs: createFakeRuns([
          { runId: 'wrun_a', workflowName: 'wfA' },
          { runId: 'wrun_b', workflowName: 'wfA' },
        ]),
        drizzle: brokenDrizzle,
        enqueue,
      });
    } finally {
      warnSpy.mockRestore();
    }

    expect(enqueue).toHaveBeenCalledTimes(2);
    for (const call of enqueue.mock.calls) {
      expect((call[2] as { jobKey: string }).jobKey).toMatch(
        new RegExp(`^${STARTUP_RECOVERY_JOB_KEY_PREFIX}`)
      );
    }
  });
});
