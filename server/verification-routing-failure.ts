import {createHash} from 'node:crypto';
import {event, type DB} from './db.js';

const codes = new Set(['REPAIR_TARGET_MISSING','VERIFICATION_CANDIDATE_UNBOUND',
  'VERIFICATION_CANDIDATE_INVALID','TASK_CHANGED','GOAL_NOT_ACTIVE','NO_FAILED_CHECK',
  'BRANCH_BUSY','VERIFICATION_RECHECK_REQUIRED','VERIFICATION_NO_PROGRESS','VERIFICATION_REPAIR_REQUIRED']);

/** Error text can contain paths or provider data. Persist only known codes. */
export function verificationRoutingCode(error: unknown): string {
  const value=error as {code?:unknown;message?:unknown}|null;
  for(const candidate of [value?.code,value?.message])
    if(typeof candidate==='string' && codes.has(candidate))return candidate;
  return 'VERIFICATION_ROUTING_UNKNOWN';
}

/** Called under the recovery transaction after current-run/cancellation checks.
 * Metadata identifies the failing edge; neither file bytes nor logs are copied. */
export async function verificationRoutingFailureEvidence(db:DB,taskId:string,runId:string,code:string) {
  const safeCode=verificationRoutingCode({code});
  const source=(await db.query(`SELECT t.id task_id,t.attempt_count,r.id run_id,r.fence,
    t.execution->>'readOnlySource' read_only,r.attempt,
    r.instruction_snapshot->'task'->'execution'->>'sourceWorkspace' frozen_source,
    t.execution->>'sourceWorkspace' current_source
    FROM tasks t JOIN runs r ON r.task_id=t.id AND r.attempt=t.attempt_count
    WHERE t.id=$1 AND r.id=$2 AND r.state='submitted'`,[taskId,runId])).rows[0];
  if(!source || source.read_only!=='true')return null;
  const candidates=(await db.query(`SELECT a.id,a.run_id,a.digest,a.size FROM artifacts a
    WHERE a.run_id=$1 AND a.name='candidate.json' ORDER BY a.id LIMIT 21`,[runId])).rows;
  const targets=(await db.query(`SELECT t.id task_id,t.state,t.attempt_count,r.id run_id,r.state run_state,
    (t.execution->>'workspace'=$2) source_matches,
    (COALESCE(t.execution->>'readOnlySource','false')='true') read_only
    FROM dependencies d JOIN tasks t ON t.id=d.depends_on
    LEFT JOIN runs r ON r.task_id=t.id AND r.attempt=t.attempt_count
    WHERE d.task_id=$1 ORDER BY t.id LIMIT 21`,[taskId,source.current_source])).rows;
  const evidence={kind:'VERIFICATION_ROUTING_FAILURE',version:1,code:safeCode,
    sourceTaskId:taskId,sourceRunId:runId,sourceAttempt:source.attempt,sourceFence:source.fence,
    sourceWorkspaceMatches:typeof source.frozen_source==='string' && source.frozen_source.length>0 &&
      source.frozen_source===source.current_source,
    candidates:candidates.slice(0,20),candidateSelectionTruncated:candidates.length>20,
    dependencies:targets.slice(0,20),dependencySelectionTruncated:targets.length>20};
  return {...evidence,fingerprint:createHash('sha256').update(JSON.stringify(evidence)).digest('hex')};
}

export async function appendVerificationRoutingEvidence(db:DB,caseId:string,evidence:NonNullable<Awaited<ReturnType<typeof verificationRoutingFailureEvidence>>>) {
  const changed=await db.query(`UPDATE supervision_cases SET evidence=evidence||$2::jsonb,updated_at=now()
    WHERE id=$1 AND source_run_id=$3 AND state IN('open','decided','delegated','waiting_evidence')
    AND NOT EXISTS(SELECT 1 FROM jsonb_array_elements(evidence) e WHERE e->>'fingerprint'=$4)
    RETURNING goal_id,task_id`,[caseId,JSON.stringify([evidence]),evidence.sourceRunId,evidence.fingerprint]);
  if(changed.rowCount)await event(db,'SUPERVISION_EVIDENCE_RECORDED',changed.rows[0].task_id,
    {caseId,goalId:changed.rows[0].goal_id,sourceRunId:evidence.sourceRunId,
      code:evidence.code,fingerprint:evidence.fingerprint,kind:evidence.kind});
}
