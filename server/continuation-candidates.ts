import type { DB } from './db.js';
import { requireTaskExecutionMetadataReady, taskExecutionMetadataVersion } from './task-execution-metadata.js';

/** Select the same due attempts without unpacking embedded inputs merely to
 * discard another runtime or planning task. Metadata is a canonical projection,
 * not execution authority: return the real task and current run for the existing
 * controller's cancellation, ownership, physical-stop and CAS guards.
 *
 * The metadata sentinel covers the whole relational candidate set BEFORE any
 * projected predicate or limit, even if the missing row would otherwise vanish.
 * The due predicate deliberately reads raw continuation. The panel projection
 * coerces readOnlySource and cannot represent every legacy execution accepted
 * by this reader. Both materialized stages and hydration share one SQL snapshot.
 */
export async function readContinuationCandidates(db: DB, now: string): Promise<any[]> {
  await requireTaskExecutionMetadataReady(db);
  const rows = (await db.query(`WITH relational AS MATERIALIZED (
    SELECT t.id,t.goal_id,t.state,t.updated_at,r.id run_id,r.fence,r.state run_state,
      r.error_code,r.account_id run_account,r.model run_model,g.state goal_state,
      metadata.task_id metadata_id,metadata.version metadata_version,
      metadata.goal_id metadata_goal,metadata.control
    FROM tasks t JOIN goals g ON g.id=t.goal_id
    LEFT JOIN settings s ON s.id='autonomy:'||g.project
    JOIN LATERAL(SELECT id,fence,state,error_code,account_id,model
      FROM runs WHERE task_id=t.id ORDER BY attempt DESC LIMIT 1)r ON true
    LEFT JOIN task_execution_metadata metadata ON metadata.task_id=t.id
    WHERE g.state IN ('active','failed','replanning')
      AND t.state IN ('failed','submitted','recovery_blocked','reconciling','recovering_artifacts','blocked_failed')
      AND COALESCE(s.value->>'enabled','true')='true'
      AND (t.recovery->>'mode' IS DISTINCT FROM 'manual' OR s.value->>'enabled'='true')
  ), guard AS MATERIALIZED (
    SELECT CASE WHEN EXISTS(SELECT 1 FROM relational
      WHERE metadata_id IS DISTINCT FROM id OR metadata_version IS DISTINCT FROM ${taskExecutionMetadataVersion}
        OR metadata_goal IS DISTINCT FROM goal_id)
      THEN 'TASK_EXECUTION_METADATA_MISSING' END selection_error
  ), eligible AS MATERIALIZED (
    SELECT relational.* FROM relational CROSS JOIN guard
    WHERE guard.selection_error IS NULL
      AND (state IN ('failed','submitted','recovery_blocked','reconciling','recovering_artifacts')
        OR (state='blocked_failed' AND control->>'readOnlySource'='true' AND run_state='submitted'))
      AND control->>'workKind' IS DISTINCT FROM 'planning'
      AND control->>'kind' IN ('harness','opencode')
      AND (goal_state<>'replanning' OR (control->>'readOnlySource'='true' AND run_state='submitted'))
  ), due AS MATERIALIZED (
    SELECT eligible.* FROM eligible JOIN tasks t ON t.id=eligible.id
    WHERE (t.execution->'continuation'->>'runId' IS DISTINCT FROM eligible.run_id::text
      OR COALESCE(t.execution->'continuation'->>'nextAt','') <= $1)
    ORDER BY eligible.updated_at LIMIT 30
  )
  SELECT guard.selection_error continuation_selection_error,selected.* FROM guard
  LEFT JOIN LATERAL (
    SELECT t.*,due.run_id,due.fence,due.run_state,due.error_code,due.run_account,due.run_model
    FROM due JOIN tasks t ON t.id=due.id
  )selected ON guard.selection_error IS NULL
  ORDER BY selected.updated_at`, [now])).rows;
  const failure = rows.find(row => row.continuation_selection_error)?.continuation_selection_error;
  if (failure) throw Object.assign(new Error(failure), { code: failure });
  return rows.filter(row => row.id !== null).map(({ continuation_selection_error: _sentinel, ...row }) => row);
}
