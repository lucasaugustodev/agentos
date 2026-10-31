import type {DB} from './db.js';

/** Additive schema, installed only by the canonical migration owner. */
export async function installTaskRetryGuidance(db:DB) {
  await db.query(`CREATE TABLE IF NOT EXISTS task_retry_guidance(
    id uuid PRIMARY KEY,task_id uuid NOT NULL REFERENCES tasks(id),source_run_id uuid NOT NULL REFERENCES runs(id),
    request_id uuid NOT NULL,request_digest text NOT NULL,payload jsonb NOT NULL,created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE(task_id,request_id),UNIQUE(task_id,source_run_id));
    CREATE TABLE IF NOT EXISTS task_retry_guidance_bindings(
    id uuid PRIMARY KEY,guidance_id uuid NOT NULL UNIQUE REFERENCES task_retry_guidance(id),
    run_id uuid NOT NULL UNIQUE REFERENCES runs(id),context_id uuid NOT NULL UNIQUE REFERENCES contexts(id),
    payload jsonb NOT NULL,created_at timestamptz NOT NULL DEFAULT now());
    CREATE OR REPLACE FUNCTION agentos_retry_guidance_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN RAISE EXCEPTION 'RETRY_GUIDANCE_IMMUTABLE'; END $$;
    DROP TRIGGER IF EXISTS agentos_retry_guidance_immutable ON task_retry_guidance;
    CREATE TRIGGER agentos_retry_guidance_immutable BEFORE UPDATE OR DELETE ON task_retry_guidance
      FOR EACH ROW EXECUTE FUNCTION agentos_retry_guidance_immutable();
    DROP TRIGGER IF EXISTS agentos_retry_guidance_binding_immutable ON task_retry_guidance_bindings;
    CREATE TRIGGER agentos_retry_guidance_binding_immutable BEFORE UPDATE OR DELETE ON task_retry_guidance_bindings
      FOR EACH ROW EXECUTE FUNCTION agentos_retry_guidance_immutable();`);
}
