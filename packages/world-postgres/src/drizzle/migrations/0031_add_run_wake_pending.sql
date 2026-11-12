-- Durable orchestrator-wake checkpoint used by startup recovery. `IF NOT
-- EXISTS` because two `bootstrap` processes started together can both apply
-- a migration.
ALTER TABLE "workflow"."workflow_runs" ADD COLUMN IF NOT EXISTS "wake_pending" boolean DEFAULT false NOT NULL;
