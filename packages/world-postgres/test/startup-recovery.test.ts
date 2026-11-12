import { createServer } from 'node:net';
import { fileURLToPath } from 'node:url';
import { PostgreSqlContainer } from '@testcontainers/postgresql';
import { SPEC_VERSION_CURRENT } from '@workflow/world';
import { drizzle } from 'drizzle-orm/node-postgres';
import { migrate } from 'drizzle-orm/node-postgres/migrator';
import type { Pool } from 'pg';
import { ulid } from 'ulid';
import { afterAll, beforeAll, describe, expect, test } from 'vitest';
import { createWorld } from '../src/index.js';
import { MessageData } from '../src/message.js';
import { startupRecoveryJobKey } from '../src/recovery.js';
import { TestPool } from './pool.js';

/**
 * Startup recovery must not replay durably parked runs, must still recover
 * runnable work (including the crash window after a wake is stored), and
 * must not accumulate duplicate Graphile jobs across restarts. See #3119.
 */
describe('startup recovery of parked vs runnable runs', () => {
  if (process.platform === 'win32') {
    test.skip('skipped on Windows since it relies on a docker container', () => {});
    return;
  }

  let container: Awaited<ReturnType<PostgreSqlContainer['start']>>;
  let pool: Pool;
  const originalBaseUrl = process.env.WORKFLOW_LOCAL_BASE_URL;

  beforeAll(async () => {
    container = await new PostgreSqlContainer('postgres:15-alpine').start();
    const dbUrl = container.getConnectionUri();
    process.env.DATABASE_URL = dbUrl;
    process.env.WORKFLOW_POSTGRES_URL = dbUrl;

    pool = new TestPool({ connectionString: dbUrl });
    await migrate(drizzle(pool), {
      migrationsFolder: fileURLToPath(
        new URL('../src/drizzle/migrations', import.meta.url)
      ),
      migrationsTable: 'workflow_migrations',
      migrationsSchema: 'workflow_drizzle',
    });

    const port = await new Promise<number>((resolve, reject) => {
      const server = createServer();
      server.once('error', reject);
      server.listen(0, '127.0.0.1', () => {
        const address = server.address();
        server.close(() => {
          if (address && typeof address === 'object') resolve(address.port);
          else reject(new Error('no port'));
        });
      });
    });
    process.env.WORKFLOW_LOCAL_BASE_URL = `http://127.0.0.1:${port}`;
  }, 120_000);

  afterAll(async () => {
    if (originalBaseUrl === undefined) {
      delete process.env.WORKFLOW_LOCAL_BASE_URL;
    } else {
      process.env.WORKFLOW_LOCAL_BASE_URL = originalBaseUrl;
    }
    await pool?.end();
    await container?.stop();
  });

  const runCreated = (workflowName: string) => ({
    eventType: 'run_created' as const,
    specVersion: SPEC_VERSION_CURRENT,
    eventData: {
      deploymentId: 'postgres',
      workflowName,
      input: new Uint8Array([1]),
    },
  });

  async function outstandingJobs(): Promise<
    Array<{ runId: string; key: string | null }>
  > {
    const { rows } = await pool.query<{
      key: string | null;
      payload: unknown;
    }>(
      `SELECT jobs.key, jobs.payload FROM graphile_worker._private_jobs AS jobs
         JOIN graphile_worker._private_tasks AS tasks ON tasks.id = jobs.task_id
         WHERE tasks.identifier = 'workflow_flows'`
    );
    return rows.map(({ key, payload }) => ({
      key,
      runId: JSON.parse(MessageData.parse(payload).data.toString())
        .runId as string,
    }));
  }

  test('skips parked runs, recovers runnable work including a stored wake, and dedupes across boots', async () => {
    const world = createWorld({ pool });
    const parkedHook = `wrun_${ulid()}`;
    const parkedWait = `wrun_${ulid()}`;
    const runnableStep = `wrun_${ulid()}`;
    const crashWindow = `wrun_${ulid()}`;
    const dueWait = `wrun_${ulid()}`;
    const ambiguous = `wrun_${ulid()}`;
    const pending = `wrun_${ulid()}`;
    const ours = new Set([
      parkedHook,
      parkedWait,
      runnableStep,
      crashWindow,
      dueWait,
      ambiguous,
      pending,
    ]);

    const startRun = async (runId: string, workflowName: string) => {
      await world.events.create(runId, runCreated(workflowName));
      await world.events.create(runId, {
        eventType: 'run_started',
        specVersion: SPEC_VERSION_CURRENT,
        eventData: {
          deploymentId: 'postgres',
          workflowName,
          input: new Uint8Array([1]),
        },
      });
    };

    try {
      await startRun(parkedHook, 'wfParkedHook');
      await world.events.create(parkedHook, {
        eventType: 'hook_created',
        correlationId: `hook_${parkedHook}`,
        eventData: { token: `tok_${parkedHook}` },
      });

      await startRun(parkedWait, 'wfParkedWait');
      await world.events.create(parkedWait, {
        eventType: 'wait_created',
        correlationId: `wait_${parkedWait}`,
        eventData: { resumeAt: new Date(Date.now() + 60 * 60 * 1000) },
      });

      await startRun(runnableStep, 'wfStep');
      await world.events.create(runnableStep, {
        eventType: 'step_created',
        correlationId: `step_${runnableStep}`,
        eventData: {
          stepName: 'work',
          input: new Uint8Array([1]),
        },
      });

      await startRun(crashWindow, 'wfCrashWindow');
      await world.events.create(crashWindow, {
        eventType: 'hook_created',
        correlationId: `hook_${crashWindow}`,
        eventData: { token: `tok_${crashWindow}` },
      });
      await world.events.create(crashWindow, {
        eventType: 'hook_received',
        correlationId: `hook_${crashWindow}`,
        eventData: { payload: new Uint8Array([1]) },
      });
      const { rows: wakeRows } = await pool.query<{ wake_pending: boolean }>(
        'SELECT wake_pending FROM workflow.workflow_runs WHERE id = $1',
        [crashWindow]
      );
      expect(wakeRows[0]?.wake_pending).toBe(true);
      const { rows: parkedWake } = await pool.query<{ wake_pending: boolean }>(
        'SELECT wake_pending FROM workflow.workflow_runs WHERE id = $1',
        [parkedHook]
      );
      expect(parkedWake[0]?.wake_pending).toBe(false);

      await startRun(dueWait, 'wfDueWait');
      await world.events.create(dueWait, {
        eventType: 'wait_created',
        correlationId: `wait_${dueWait}`,
        eventData: { resumeAt: new Date(Date.now() - 60 * 1000) },
      });

      await startRun(ambiguous, 'wfAmbiguous');

      await world.events.create(pending, runCreated('wfPending'));

      await world.start();
      const first = (await outstandingJobs()).filter((job) =>
        ours.has(job.runId)
      );
      const firstIds = first.map((job) => job.runId).sort();
      expect(firstIds).toEqual(
        [ambiguous, crashWindow, dueWait, pending, runnableStep].sort()
      );
      expect(firstIds).not.toContain(parkedHook);
      expect(firstIds).not.toContain(parkedWait);
      for (const job of first) {
        expect(job.key).toBe(startupRecoveryJobKey(job.runId));
      }

      await world.close();

      for (let boot = 0; boot < 4; boot++) {
        const next = createWorld({ pool });
        await next.start();
        await next.close();
      }

      const after = (await outstandingJobs()).filter((job) =>
        ours.has(job.runId)
      );
      expect(after.map((job) => job.runId).sort()).toEqual(firstIds);
      const byRun = new Map<string, number>();
      for (const job of after) {
        byRun.set(job.runId, (byRun.get(job.runId) ?? 0) + 1);
        expect(job.key).toBe(startupRecoveryJobKey(job.runId));
      }
      expect([...byRun.values()].every((count) => count === 1)).toBe(true);
    } finally {
      await world.close().catch(() => {});
    }
  }, 60_000);
});
