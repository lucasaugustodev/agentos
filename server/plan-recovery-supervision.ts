import {randomUUID} from 'node:crypto';
import {pool,tx,lock,event,type DB} from './db.js';
import {inspectStopped,assertStopped} from './continuation.js';
import {hash} from './memory.js';
import {routeVerificationPlanningRecovery} from './verification-planning-recovery.js';

/** A blocked recovery is a structural request for a decision, not permission
 * to retry, waive a criterion, or replace its source with an earlier run. */
export async function planRecoveryCandidates(db:DB,taskId:string|null=null){
  return (await db.query(`SELECT t.id,t.goal_id,t.attempt_count,t.instruction_revision_id,
      r.id run_id,r.runtime,r.fence,r.context_id,r.checkpoint,
      r.instruction_snapshot->'schema' snapshot_schema,
      r.instruction_snapshot->'task'->>'id' snapshot_task_id,
      r.instruction_snapshot->'task'->>'goal_id' snapshot_goal_id,
      r.instruction_snapshot->'task'->'execution'->>'kind' snapshot_kind,
      r.instruction_snapshot->'task'->'execution'->>'workspace' snapshot_workspace,
      r.instruction_snapshot->>'revisionId' snapshot_revision_id,
      control.kind,control.workspace,recovery.source_run_id,
      r.state run_state,r.error_code,r.summary,r.attempt,a.checks
    FROM tasks t JOIN goals g ON g.id=t.goal_id
    JOIN objectives o ON o.id=g.id
    JOIN settings s ON s.id='supervision:'||g.id
    LEFT JOIN settings autonomy ON autonomy.id='autonomy:'||g.project
    JOIN LATERAL(SELECT * FROM runs WHERE task_id=t.id ORDER BY attempt DESC LIMIT 1)r ON true
    JOIN recoveries recovery ON recovery.source_run_id=r.id AND recovery.task_id=t.id AND recovery.target_task_id=t.id
    LEFT JOIN LATERAL(SELECT passed,checks FROM acceptances WHERE run_id=r.id AND task_id=t.id ORDER BY created_at DESC LIMIT 1)a ON true
    CROSS JOIN LATERAL jsonb_to_record(t.execution) AS control(kind text,workspace text,continuation jsonb)
    WHERE ($1::uuid IS NULL OR t.id=$1) AND g.state='active'
      AND s.value->>'enabled'='true' AND COALESCE(autonomy.value->>'enabled','true')='true'
      AND (t.recovery->>'mode' IS DISTINCT FROM 'manual' OR autonomy.value->>'enabled'='true')
      AND t.state='recovery_blocked' AND t.stage IN('integrate','verify')
      AND recovery.status='blocked' AND recovery.strategy IN('targeted_repair','replan_and_repair')
      AND r.attempt=t.attempt_count AND r.finished_at IS NOT NULL
      AND control.kind IN('harness','opencode')
      AND COALESCE(control.continuation->>'owner','supervisor')<>'user'
      AND COALESCE(r.error_code,'') !~ 'AUTH|QUOTA|RATE_LIMIT|CAPACITY|CONFIG|CONTEXT_TOO_LARGE|REQUIRED_CONTEXT|INPUTS_TOO_LARGE|CORRUPT|DIGEST|INVALID_WRITE|CANCEL'
      AND (r.state='failed' OR (r.state='submitted' AND a.passed=false AND EXISTS(
        SELECT 1 FROM jsonb_array_elements(COALESCE(a.checks,'[]'::jsonb)) c
        WHERE c->>'passed'='false' AND c->>'type' IS DISTINCT FROM 'review')))
      AND NOT EXISTS(SELECT 1 FROM supervision_cases c WHERE c.task_id=t.id
        AND (c.source_run_id=r.id OR c.state IN('open','decided','delegated','waiting_evidence')))
      AND NOT EXISTS(SELECT 1 FROM runs active WHERE active.task_id=t.id AND active.state IN('queued','running','cancelling','reconciling'))
      AND NOT EXISTS(SELECT 1 FROM events e WHERE e.kind='CANCEL_REQUESTED' AND e.payload->>'runId'=r.id::text)
    ORDER BY t.id`,[taskId])).rows;
}
function identity(t:any){return t.snapshot_schema===1 && t.snapshot_task_id===t.id &&
  t.snapshot_goal_id===t.goal_id && t.snapshot_kind===t.kind && t.runtime===t.kind &&
  typeof t.snapshot_workspace==='string' && t.snapshot_workspace.length>0 && t.workspace===t.snapshot_workspace &&
  t.instruction_revision_id===t.snapshot_revision_id;}
export async function requestPlanRecoverySupervision(inspect=inspectStopped){
  const candidates=await planRecoveryCandidates(pool),added:string[]=[];
  for(const observed of candidates){
    if(!identity(observed))continue;
    try{
      const proof=await inspect(observed.run_id,observed.kind==='opencode');
      const id=await tx(async db=>{
        await lock(db);
        const current=(await planRecoveryCandidates(db,observed.id))[0];
        if(!current||!identity(current)||hash(JSON.stringify(current))!==hash(JSON.stringify(observed)))return;
        assertStopped(proof,{id:current.run_id,checkpoint:current.checkpoint},
          {execution:{workspace:current.snapshot_workspace}});
        if(await routeVerificationPlanningRecovery(db,current.id,current.run_id))return;
        const caseId=randomUUID(),reason='Recuperação estrutural bloqueada; exige decisão de supervisão antes de nova tentativa.';
        const evidence=[{kind:'PLAN_RECOVERY_BLOCKED',runId:current.run_id,attempt:current.attempt,
          code:current.error_code,summary:current.summary,checks:current.checks}];
        const result=await db.query(`INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,reason,evidence)
          VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING RETURNING id`,
          [caseId,current.goal_id,current.id,current.run_id,reason,JSON.stringify(evidence)]);
        if(!result.rowCount)return;
        await event(db,'SUPERVISION_REQUESTED',current.id,{caseId,goalId:current.goal_id,sourceRunId:current.run_id,
          reason,planRecovery:{kind:'PLAN_RECOVERY_BLOCKED',stopCheckedAt:proof.checkedAt}});
        return caseId;
      });
      if(id)added.push(id);
    }catch{ /* Stale/failed stop proof grants no authority. Next tick may inspect again. */ }
  }
  return added;
}
