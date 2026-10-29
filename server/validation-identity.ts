import type { DB } from './db.js';
import { pool } from './db.js';

// Bump whenever criterion evaluation semantics change. Receipts never replace
// immutable acceptances; they only suppress repeat work for identical inputs.
export const validationVersion = 'deterministic-contract/v2';
export const validationReceiptPrefix = 'validation-receipt:';
const reviewerIdentity = (reviewer?: string) => reviewer ?? 'deterministic-contract';

/** All inputs to criterion evaluation, plus the current instruction identity.
 * No execution blobs or inline source bytes are read by the polling query.
 * JSONB canonicalization and SHA256 are evaluated by PostgreSQL in both paths. */
const inputsSql = (filter: string) => `
  WITH current_tasks AS MATERIALIZED (
    SELECT t.id,t.goal_id,t.title,t.state,t.criteria,t.attempt_count,t.instruction_revision_id,t.stage,t.covers,
      r.id run_id,r.fence run_fence,r.attempt run_attempt,r.state run_state,
      NULLIF(r.instruction_snapshot->'workerValidation','null'::jsonb) worker_validation
    FROM tasks t JOIN runs r ON r.task_id=t.id AND r.attempt=t.attempt_count
    WHERE ${filter} AND r.state='submitted'
      AND NOT EXISTS(SELECT 1 FROM runs newer WHERE newer.task_id=t.id AND newer.attempt>r.attempt)
  ), manifests AS MATERIALIZED (
    SELECT t.*,COALESCE(a.artifacts,'[]'::jsonb) artifacts
    FROM current_tasks t LEFT JOIN LATERAL(
      SELECT jsonb_agg(jsonb_build_object('id',a.id,'run_id',a.run_id,'name',a.name,
        'mime',a.mime,'digest',a.digest,'size',a.size)
        ORDER BY a.created_at DESC,a.id DESC) artifacts
      FROM artifacts a WHERE a.run_id=t.run_id AND a.name<>'task-instructions-history.json'
    )a ON true
  ), validation_inputs AS (
    SELECT m.*,jsonb_build_object('version',$1::text,'reviewer',$2::text,
      'reviewAttested',$3::boolean,'taskId',m.id,'goalId',m.goal_id,
      'runId',m.run_id,'fence',m.run_fence,'attempt',m.run_attempt,
      'contract',jsonb_build_object('criteria',m.criteria,'revision',m.instruction_revision_id,
        'stage',m.stage,'covers',m.covers),'artifacts',m.artifacts)
      ||CASE WHEN m.worker_validation IS NULL THEN '{}'::jsonb ELSE jsonb_build_object('workerValidation',
        jsonb_build_object('policy',m.worker_validation,'currentPolicy',p.value,'handoff',h.value,
          'state',j.value->'state','result',j.value->'result','code',j.value->'code',
          'evidenceState',e.value->'state','evidenceDigest',encode(sha256(convert_to((e.value->'body')::text,'UTF8')),'hex')))
        END validation_input
    FROM manifests m
    LEFT JOIN settings p ON m.worker_validation IS NOT NULL AND p.id='worker-validation:'||m.goal_id
    LEFT JOIN settings h ON m.worker_validation IS NOT NULL AND h.id='worker-validation-handoff:'||m.run_id
    LEFT JOIN settings j ON j.id='candidate-validation:'||(h.value->>'actionId')
    LEFT JOIN settings e ON e.id='candidate-validation-evidence:'||(h.value->>'actionId')
  ), keyed AS (
    SELECT v.*,encode(sha256(convert_to(v.validation_input::text,'UTF8')),'hex') validation_key
    FROM validation_inputs v
  )`;
const receiptJoin = `
  LEFT JOIN settings receipt ON receipt.id='${validationReceiptPrefix}'||v.id::text||':'||v.validation_key
  LEFT JOIN LATERAL(
    SELECT id,run_id,passed,checks,reviewer FROM acceptances
    WHERE run_id=v.run_id ORDER BY created_at DESC,id DESC LIMIT 1
  )last_acceptance ON true`;
const receiptMatches = `(
  receipt.value->'input'=v.validation_input
  AND receipt.value->>'acceptanceId'=last_acceptance.id::text
  AND receipt.value->'passed'=to_jsonb(last_acceptance.passed)
  AND last_acceptance.reviewer=$2::text
)`;

export type ValidationSnapshot = {
  id: string; goal_id: string; title: string; state: string; criteria: any[];
  attempt_count: number; run_id: string; run_fence: string; run_attempt: number; run_state: string;
  artifacts: any[]; validation_input: any; validation_key: string;
  receipt_match: boolean; acceptance_id: string | null; accepted_passed: boolean | null; accepted_checks: any[] | null;
};
export function validationReceiptId(snapshot: ValidationSnapshot) {
  return validationReceiptPrefix + snapshot.id + ':' + snapshot.validation_key;
}

export async function readValidationSnapshot(db: DB, taskId: string, reviewer?: string): Promise<ValidationSnapshot | null> {
  const row = (await db.query(inputsSql("t.id=$4::uuid AND t.state IN('submitted','accepted')") + `
    SELECT v.*,COALESCE(${receiptMatches},false) receipt_match,
      last_acceptance.id acceptance_id,last_acceptance.passed accepted_passed,last_acceptance.checks accepted_checks
    FROM keyed v ${receiptJoin}`,
    [validationVersion, reviewerIdentity(reviewer), !!reviewer, taskId])).rows[0];
  return row ?? null;
}

/** Historical submitted runs cannot nominate their task repeatedly. A new
 * artifact, criterion, instruction revision or validator version becomes
 * eligible again without erasing any previous negative result. */
export async function readPendingValidations(db: DB = pool, limit = 20) {
  return (await db.query(inputsSql("t.state='submitted'") + `
    SELECT v.id FROM keyed v ${receiptJoin}
    WHERE NOT COALESCE(${receiptMatches},false)
      AND (v.worker_validation IS NULL
        OR v.validation_input#>'{workerValidation,currentPolicy}' IS DISTINCT FROM v.worker_validation
        OR v.validation_input#>>'{workerValidation,handoff,state}'='rejected'
        OR v.validation_input#>>'{workerValidation,state}' IN('failed','rejected')
        OR v.validation_input#>>'{workerValidation,evidenceState}'='published')
    ORDER BY v.id LIMIT $4`,
    [validationVersion, reviewerIdentity(), false, limit])).rows as { id: string }[];
}
