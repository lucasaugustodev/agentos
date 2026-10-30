import type {DB} from './db.js';

/** Installed only by the controlled migration; request handlers never run DDL. */
export async function installObjectiveModelRoutes(db:DB){
 await db.query(`
CREATE TABLE IF NOT EXISTS objective_model_routes (
 id uuid PRIMARY KEY, objective_id uuid NOT NULL REFERENCES objectives(id),
 generation int NOT NULL CHECK(generation>0), idempotency_key text NOT NULL,
 payload jsonb NOT NULL, receipt_digest text NOT NULL, created_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE(objective_id,generation), UNIQUE(objective_id,idempotency_key));
ALTER TABLE planning_runs ADD COLUMN IF NOT EXISTS route_generation_id uuid REFERENCES objective_model_routes(id);
CREATE OR REPLACE FUNCTION agentos_objective_model_route_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'OBJECTIVE_MODEL_ROUTE_IMMUTABLE'; END $$;
DROP TRIGGER IF EXISTS agentos_objective_model_route_immutable ON objective_model_routes;
CREATE TRIGGER agentos_objective_model_route_immutable BEFORE UPDATE OR DELETE ON objective_model_routes
 FOR EACH ROW EXECUTE FUNCTION agentos_objective_model_route_immutable();
CREATE OR REPLACE FUNCTION agentos_planning_route_generation() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE current_id uuid;
BEGIN
 IF TG_OP='UPDATE' THEN
  IF NEW.route_generation_id IS DISTINCT FROM OLD.route_generation_id OR
     NEW.objective_id IS DISTINCT FROM OLD.objective_id THEN
   RAISE EXCEPTION 'PLANNING_ROUTE_GENERATION_IMMUTABLE';
  END IF;
 ELSE
  SELECT id INTO current_id FROM objective_model_routes WHERE objective_id=NEW.objective_id ORDER BY generation DESC LIMIT 1;
  IF NEW.route_generation_id IS NOT NULL AND NEW.route_generation_id IS DISTINCT FROM current_id THEN
   RAISE EXCEPTION 'PLANNING_ROUTE_GENERATION_STALE';
  END IF;
  NEW.route_generation_id:=current_id;
 END IF;
 RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS agentos_planning_route_generation ON planning_runs;
CREATE TRIGGER agentos_planning_route_generation BEFORE INSERT OR UPDATE ON planning_runs
 FOR EACH ROW EXECUTE FUNCTION agentos_planning_route_generation();
`);
}
