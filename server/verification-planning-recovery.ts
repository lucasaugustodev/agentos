import {randomUUID} from 'node:crypto';
import {event,type DB} from './db.js';

/** Called under the scheduler lock. The existing objective feedback owns this
 * material failure; recording evidence never reopens a goal or admits a writer. */
export async function routeVerificationPlanningRecovery(db:DB,taskId:string,runId?:string) {
  const row=(await db.query(`SELECT t.*,r.id run_id,r.summary,g.state goal_state FROM tasks t
    JOIN goals g ON g.id=t.goal_id JOIN objectives o ON o.id=g.id AND o.mode='execute'
    JOIN LATERAL(SELECT * FROM runs WHERE task_id=t.id ORDER BY attempt DESC LIMIT 1)r ON true
    JOIN LATERAL(SELECT * FROM acceptances WHERE run_id=r.id ORDER BY created_at DESC,id DESC LIMIT 1)a ON true
    LEFT JOIN settings autonomy ON autonomy.id='autonomy:'||g.project
    WHERE t.id=$1 AND ($2::uuid IS NULL OR r.id=$2) AND r.attempt=t.attempt_count
      AND g.state IN('active','replanning','failed') AND t.state IN('submitted','recovery_blocked','blocked_failed')
      AND t.execution->>'readOnlySource'='true' AND r.state='submitted' AND a.passed=false
      AND EXISTS(SELECT 1 FROM jsonb_array_elements(a.checks) c WHERE c->>'passed'='false' AND c->>'type' IS DISTINCT FROM 'review')
      AND COALESCE(autonomy.value->>'enabled','true')='true'
      AND (t.recovery->>'mode' IS DISTINCT FROM 'manual' OR autonomy.value->>'enabled'='true')
      AND t.execution#>>'{continuation,owner}' IS DISTINCT FROM 'user'
      AND NOT EXISTS(SELECT 1 FROM settings w WHERE w.id='workforce:'||g.id AND w.value->>'enabled'='true')
      AND NOT EXISTS(SELECT 1 FROM runs live WHERE live.task_id=t.id AND live.state IN('queued','running','cancelling','reconciling'))
      AND NOT EXISTS(SELECT 1 FROM leases l JOIN runs held ON held.id=l.run_id WHERE held.task_id=t.id)
      AND NOT EXISTS(SELECT 1 FROM events e WHERE e.kind='CANCEL_REQUESTED' AND e.payload->>'runId'=r.id::text)
    FOR UPDATE OF t`,[taskId,runId??null])).rows[0];
  if(!row)return false;
  const key='run:'+row.run_id;
  const inserted=await db.query(`INSERT INTO objective_feedback(id,objective_id,trigger_key,description,task_ids)
    VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING RETURNING id`,[randomUUID(),row.goal_id,key,
    `${row.title}: ${row.summary??row.reason}`,JSON.stringify([row.id])]);
  const feedbackId=inserted.rows[0]?.id??(await db.query(
    'SELECT id FROM objective_feedback WHERE objective_id=$1 AND trigger_key=$2',[row.goal_id,key])).rows[0]?.id;
  if(inserted.rowCount)await event(db,'PLANNING_FEEDBACK',row.goal_id,
    {reason:`${row.title}: ${row.summary??row.reason}`,taskIds:[row.id]});
  if(!(await db.query(`SELECT 1 FROM events WHERE kind='VERIFICATION_PLANNING_ROUTED' AND entity_id=$1
    AND payload->>'sourceRunId'=$2`,[row.id,row.run_id])).rowCount)
    await event(db,'VERIFICATION_PLANNING_ROUTED',row.id,{goalId:row.goal_id,sourceRunId:row.run_id,
      sourceAttempt:row.attempt_count,feedbackId,triggerKey:key,recoveryOwner:'planner'});
  // Keep an existing pending supervisor projection intact. It cannot admit in
  // replanning; the objective feedback remains durable for its sole planner.
  if(!(await db.query(`SELECT 1 FROM supervision_cases WHERE task_id=$1
    AND state IN('open','decided','delegated','waiting_evidence')`,[row.id])).rowCount){
    const continuation={state:'supervising',owner:'controller',recoveryOwner:'planner',runId:row.run_id,
      reasonCode:'VERIFICATION_PLANNING_REQUIRED',feedbackId,
      message:'A reprovação foi encaminhada ao planejamento; aguarda reparo antes de nova QA.',
      updatedAt:new Date().toISOString(),nextAt:new Date(Date.now()+60000).toISOString()};
    await db.query("UPDATE tasks SET execution=jsonb_set(execution,'{continuation}',$2::jsonb) WHERE id=$1",
      [row.id,JSON.stringify(continuation)]);
  }
  return true;
}
