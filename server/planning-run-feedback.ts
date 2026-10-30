import {type DB} from './db.js';

/** Called under the scheduler lock. A recorded run failure keeps its original
 * owner and content even when later reconciliation changes task projections. */
export async function planningRunFeedback(db:DB,task:{id:string;goal_id:string;run_id:string;title:string;summary?:string|null;reason?:string|null}) {
  const key='run:'+task.run_id;
  const existing=(await db.query('SELECT description,task_ids FROM objective_feedback WHERE objective_id=$1 AND trigger_key=$2',
    [task.goal_id,key])).rows[0];
  if(existing)return {key,description:existing.description as string,taskIds:existing.task_ids as string[]};
  const acceptance=(await db.query(`SELECT id,passed,checks FROM acceptances WHERE task_id=$1 AND run_id=$2
    ORDER BY created_at DESC,id DESC LIMIT 1`,[task.id,task.run_id])).rows[0];
  const failed=acceptance?.passed===false?(acceptance.checks as any[]).filter(check=>check.passed===false&&check.type!=='review'):[];
  const description=failed.length?
    `${task.title}\nWorker summary (claim, not acceptance): ${task.summary??'(absent)'}\nDeterministic acceptance rejection: ${JSON.stringify({
      acceptanceId:acceptance.id,runId:task.run_id,checks:failed.map(({id,type,label,detail})=>({id,type,label,passed:false,detail}))})}`:
    `${task.title}: ${task.summary??task.reason}`;
  return {key,description,taskIds:[task.id]};
}
