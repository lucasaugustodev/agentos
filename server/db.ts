import pg from 'pg';
import type { PoolClient } from 'pg';
import { config } from './config.js';
import { installWorkstreamPolicy } from './workstream-policy.js';
import { installArtifactRecoveryPolicy } from './artifact-recovery-policy.js';
import { installWorkforcePolicy } from './workforce-policy.js';
import { migrateCycles } from './cycles/store.js';
import { installTaskInstructions } from './task-instructions.js';
import { installTaskRetryGuidance } from './task-retry-guidance-schema.js';
import { installRuntimeLookupIndexes } from './runtime-lookup-indexes.js';
import { installObjectiveModelRoutes } from './objective-model-route-schema.js';
export const pool = new pg.Pool({
  connectionString: config.databaseUrl,
  max: 15,
  connectionTimeoutMillis: 10000,
});
export type DB = Pick<PoolClient, 'query'>;
export async function tx<T>(fn: (db: DB) => Promise<T>): Promise<T> {
  const c = await pool.connect();
  try {
    await c.query('BEGIN');
    const value = await fn(c);
    await c.query('COMMIT');
    return value;
  } catch (e) {
    await c.query('ROLLBACK');
    throw e;
  } finally {
    c.release();
  }
}
export async function event(
  db: DB,
  kind: string,
  entityId: string | null,
  payload: Record<string, unknown> = {},
) {
  await db.query('INSERT INTO events(kind,entity_id,payload) VALUES($1,$2,$3)', [
    kind,
    entityId,
    JSON.stringify(payload),
  ]);
}
export async function lock(db: DB) {
  await db.query('SELECT pg_advisory_xact_lock(4280001)');
}
export async function migrate() {
  await tx(async (db) => {
    await lock(db);
    await db.query(`
CREATE TABLE IF NOT EXISTS settings (id text PRIMARY KEY, value jsonb NOT NULL);
INSERT INTO settings VALUES('capacity','{"maxRuns":4,"browserSlots":2}') ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS agents (
 id text PRIMARY KEY, name text NOT NULL, department text NOT NULL, reports_to text,
 capabilities jsonb NOT NULL, runtime text NOT NULL, version int NOT NULL DEFAULT 1,
 description text NOT NULL, status text NOT NULL DEFAULT 'available');
CREATE TABLE IF NOT EXISTS accounts (
 id text PRIMARY KEY, label text NOT NULL, provider text NOT NULL, model text NOT NULL,
 enabled boolean NOT NULL DEFAULT false, max_concurrency int NOT NULL DEFAULT 1 CHECK(max_concurrency>0),
 quota_status text NOT NULL DEFAULT 'unknown', remaining_percent real, reset_at timestamptz,
 observed_at timestamptz, allow_unknown boolean NOT NULL DEFAULT false);
CREATE TABLE IF NOT EXISTS goals (
 id uuid PRIMARY KEY, title text NOT NULL, description text NOT NULL, project text NOT NULL,
 state text NOT NULL DEFAULT 'active', is_demo boolean NOT NULL DEFAULT false,
 idempotency_key text UNIQUE NOT NULL, created_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS tasks (
 id uuid PRIMARY KEY, goal_id uuid NOT NULL REFERENCES goals(id), key text NOT NULL,
 title text NOT NULL, description text NOT NULL, agent_id text NOT NULL REFERENCES agents(id),
 account_id text REFERENCES accounts(id), state text NOT NULL DEFAULT 'ready',
 priority int NOT NULL DEFAULT 0, resources jsonb NOT NULL DEFAULT '[]',
 criteria jsonb NOT NULL, required_memories jsonb NOT NULL DEFAULT '[]',
 attempt_count int NOT NULL DEFAULT 0, max_attempts int NOT NULL DEFAULT 3,
 next_attempt_at timestamptz, reason text, created_at timestamptz NOT NULL DEFAULT now(),
 updated_at timestamptz NOT NULL DEFAULT now(), UNIQUE(goal_id,key));
CREATE TABLE IF NOT EXISTS dependencies (task_id uuid REFERENCES tasks(id), depends_on uuid REFERENCES tasks(id), PRIMARY KEY(task_id,depends_on), CHECK(task_id<>depends_on));
CREATE TABLE IF NOT EXISTS runs (
 id uuid PRIMARY KEY, task_id uuid NOT NULL REFERENCES tasks(id), attempt int NOT NULL,
 state text NOT NULL DEFAULT 'queued', fence uuid NOT NULL, worker_id text,
 account_id text, runtime text NOT NULL, agent_version int NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now(), started_at timestamptz, finished_at timestamptz,
 heartbeat_at timestamptz, context_id uuid, summary text, error_code text,
 usage jsonb, checkpoint jsonb, UNIQUE(task_id,attempt));
CREATE UNIQUE INDEX IF NOT EXISTS one_live_run ON runs(task_id) WHERE state IN ('queued','running','cancelling','reconciling');
CREATE TABLE IF NOT EXISTS leases (resource text NOT NULL, run_id uuid REFERENCES runs(id), PRIMARY KEY(resource,run_id));
CREATE TABLE IF NOT EXISTS artifacts (
 id uuid PRIMARY KEY, run_id uuid NOT NULL REFERENCES runs(id), name text NOT NULL,
 mime text NOT NULL, digest text NOT NULL, size int NOT NULL, created_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS acceptances (id uuid PRIMARY KEY, task_id uuid REFERENCES tasks(id), run_id uuid REFERENCES runs(id), passed boolean NOT NULL, checks jsonb NOT NULL, reviewer text NOT NULL, created_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS memories (
 id uuid PRIMARY KEY, project text NOT NULL, title text NOT NULL, content text NOT NULL,
 kind text NOT NULL, verification text NOT NULL, source text NOT NULL,
 version int NOT NULL DEFAULT 1, supersedes uuid REFERENCES memories(id),
 created_at timestamptz NOT NULL DEFAULT now(), expires_at timestamptz);
CREATE TABLE IF NOT EXISTS contexts (
 id uuid PRIMARY KEY, run_id uuid NOT NULL REFERENCES runs(id), manifest jsonb NOT NULL,
 content text NOT NULL, created_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS events (id bigserial PRIMARY KEY, kind text NOT NULL, entity_id text, payload jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS outbox (id bigserial PRIMARY KEY, run_id uuid NOT NULL UNIQUE REFERENCES runs(id), sent_at timestamptz);
CREATE TABLE IF NOT EXISTS memory_outbox (memory_id uuid PRIMARY KEY REFERENCES memories(id), state text NOT NULL DEFAULT 'pending', attempts int NOT NULL DEFAULT 0, next_attempt_at timestamptz, synced_at timestamptz, error text);
CREATE INDEX IF NOT EXISTS tasks_state_idx ON tasks(state,next_attempt_at);
CREATE INDEX IF NOT EXISTS events_entity_idx ON events(entity_id,id);
CREATE INDEX IF NOT EXISTS memories_project_idx ON memories(project);
ALTER TABLE goals ADD COLUMN IF NOT EXISTS input_hash text;
CREATE UNIQUE INDEX IF NOT EXISTS memory_one_successor ON memories(supersedes) WHERE supersedes IS NOT NULL;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS source text NOT NULL DEFAULT 'manual';
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS bridge_id text;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS identity jsonb;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS models jsonb NOT NULL DEFAULT '[]';
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS quota jsonb NOT NULL DEFAULT '{}';
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS connection_status text NOT NULL DEFAULT 'unknown';
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS execution_feedback jsonb NOT NULL DEFAULT '{}';
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS model text;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS execution jsonb;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS recovery jsonb;
CREATE TABLE IF NOT EXISTS recoveries (
 source_run_id uuid PRIMARY KEY REFERENCES runs(id),
 task_id uuid NOT NULL REFERENCES tasks(id), target_task_id uuid NOT NULL REFERENCES tasks(id),
 strategy text NOT NULL, status text NOT NULL, reason text NOT NULL,
 checkpoint jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT now());
ALTER TABLE runs ADD COLUMN IF NOT EXISTS model text;
UPDATE runs r SET model=COALESCE(t.model,a.model) FROM tasks t,accounts a WHERE r.task_id=t.id AND r.account_id=a.id AND r.model IS NULL;
CREATE TABLE IF NOT EXISTS chats(id uuid PRIMARY KEY,title text NOT NULL,project text NOT NULL DEFAULT 'chat',created_at timestamptz NOT NULL DEFAULT now(),updated_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS chat_messages(id uuid PRIMARY KEY,chat_id uuid NOT NULL REFERENCES chats(id),role text NOT NULL,content text NOT NULL DEFAULT '',state text NOT NULL DEFAULT 'completed',request_key text UNIQUE,account_id text,model text,native jsonb,usage jsonb,error text,created_at timestamptz NOT NULL DEFAULT now(),updated_at timestamptz NOT NULL DEFAULT now());
CREATE UNIQUE INDEX IF NOT EXISTS one_active_chat_reply ON chat_messages(chat_id) WHERE role='assistant' AND state IN('queued','running','cancelling');
CREATE TABLE IF NOT EXISTS objectives (
 id uuid PRIMARY KEY REFERENCES goals(id), request jsonb NOT NULL, request_hash text NOT NULL,
 mode text NOT NULL, status text NOT NULL DEFAULT 'planning', revision int NOT NULL DEFAULT 0,
 reason text, chat_id uuid REFERENCES chats(id), created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS planning_runs (
 id uuid PRIMARY KEY, objective_id uuid NOT NULL REFERENCES objectives(id), base_revision int NOT NULL,
 trigger_key text NOT NULL, state text NOT NULL DEFAULT 'queued', fence uuid NOT NULL,
 account_id text, model text, context jsonb, output jsonb, feedback jsonb NOT NULL DEFAULT '{}',
 attempt int NOT NULL DEFAULT 0, next_attempt_at timestamptz NOT NULL DEFAULT now(), heartbeat_at timestamptz,
 started_at timestamptz, finished_at timestamptz, error text, created_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE(objective_id,trigger_key));
CREATE UNIQUE INDEX IF NOT EXISTS one_live_planning ON planning_runs(objective_id) WHERE state IN('queued','running','waiting_quota');
ALTER TABLE planning_runs ADD COLUMN IF NOT EXISTS usage JSONB;
ALTER TABLE planning_runs ADD COLUMN IF NOT EXISTS progress JSONB;
ALTER TABLE planning_runs ADD COLUMN IF NOT EXISTS recovery_counts JSONB NOT NULL DEFAULT '{}';
CREATE TABLE IF NOT EXISTS planning_attempts (
 planning_run_id uuid NOT NULL REFERENCES planning_runs(id), attempt int NOT NULL,
 output text, usage jsonb, error text, created_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(planning_run_id,attempt));
CREATE TABLE IF NOT EXISTS plan_revisions (
 id uuid PRIMARY KEY, objective_id uuid NOT NULL REFERENCES objectives(id), revision int NOT NULL,
 planning_run_id uuid NOT NULL REFERENCES planning_runs(id), plan jsonb NOT NULL, status text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now(), UNIQUE(objective_id,revision));
CREATE TABLE IF NOT EXISTS objective_feedback (
 id uuid PRIMARY KEY, objective_id uuid NOT NULL REFERENCES objectives(id), trigger_key text NOT NULL,
 description text NOT NULL, task_ids jsonb NOT NULL DEFAULT '[]', created_at timestamptz NOT NULL DEFAULT now(), UNIQUE(objective_id,trigger_key));
ALTER TABLE objective_feedback ADD COLUMN IF NOT EXISTS source_replan jsonb;
CREATE OR REPLACE FUNCTION agentos_source_replan_feedback_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF OLD.source_replan IS NOT NULL OR (TG_OP='UPDATE' AND NEW.source_replan IS NOT NULL) THEN
  RAISE EXCEPTION 'SOURCE_REPLAN_RECEIPT_IMMUTABLE';
 END IF;
 IF TG_OP='DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;
END $$;
DROP TRIGGER IF EXISTS agentos_source_replan_feedback_immutable ON objective_feedback;
CREATE TRIGGER agentos_source_replan_feedback_immutable BEFORE UPDATE OR DELETE ON objective_feedback
 FOR EACH ROW EXECUTE FUNCTION agentos_source_replan_feedback_immutable();
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS plan_revision int;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS stage text;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS covers jsonb NOT NULL DEFAULT '[]';
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS allowed_accounts jsonb NOT NULL DEFAULT '[]';
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS parent_task_id uuid REFERENCES tasks(id);
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS agent_role text;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS agent_level int DEFAULT 0;
ALTER TABLE runs ADD COLUMN IF NOT EXISTS parent_task_id uuid REFERENCES tasks(id);
ALTER TABLE runs ADD COLUMN IF NOT EXISTS agent_role text;
ALTER TABLE runs ADD COLUMN IF NOT EXISTS agent_level int DEFAULT 0;
CREATE TABLE IF NOT EXISTS recall_imports (source_id text PRIMARY KEY, version text NOT NULL, memory_id uuid REFERENCES memories(id), project text NOT NULL);
INSERT INTO settings VALUES('remote-capacity','{"maxRuns":2,"memoryMiB":3600,"cpuPercent":300}') ON CONFLICT DO NOTHING;
`);
    await installWorkstreamPolicy(db);
    await installWorkforcePolicy(db);
    await installArtifactRecoveryPolicy(db);
    await migrateCycles(db);
    await installObjectiveModelRoutes(db);
  });
  await installTaskInstructions(pool);
  await installTaskRetryGuidance(pool);
  await installRuntimeLookupIndexes(pool);
}
