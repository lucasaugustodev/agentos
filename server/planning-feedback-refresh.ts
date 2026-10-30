import {event,type DB} from './db.js';

/** Called under the planner admission lock, before selecting an account. This
 * only refreshes an unstarted decision: a persisted output or any durable source
 * preparation intent freezes the old context until its existing recovery gate.
 * No artifact body, task graph, source override or completed decision is edited. */
export async function refreshQueuedPlanningFeedback(db:DB,planningRunId:string){
 const row=(await db.query(`WITH eligible AS MATERIALIZED (
   SELECT p.id,p.objective_id,p.feedback FROM planning_runs p
   WHERE p.id=$1 AND p.state IN('queued','waiting_quota') AND p.output IS NULL
     AND NOT EXISTS(SELECT 1 FROM settings s WHERE s.id LIKE 'planning-source-preparation:'||p.id::text||':%')
   FOR UPDATE OF p
 ) SELECT e.*,COALESCE((SELECT jsonb_agg(jsonb_build_object('id',f.id,'description',f.description)
     ORDER BY f.created_at,f.id) FROM objective_feedback f
   WHERE f.objective_id=e.objective_id AND f.source_replan IS NULL AND NOT(COALESCE(e.feedback->'feedbackIds','[]') ? f.id::text)
     AND NOT EXISTS(SELECT 1 FROM planning_runs consumed WHERE consumed.objective_id=f.objective_id
       AND consumed.state='completed' AND (consumed.feedback->'feedbackIds' ? f.id::text OR consumed.created_at>f.created_at))
 ),'[]'::jsonb) pending FROM eligible e`,[planningRunId])).rows[0];
 if(!row||!row.pending.length)return undefined;
 const old=row.feedback;
 if(!old||typeof old!=='object'||Array.isArray(old)||
   (old.feedbackIds!==undefined&&(!Array.isArray(old.feedbackIds)||old.feedbackIds.some((x:unknown)=>typeof x!=='string')))||
   (old.reports!==undefined&&(!Array.isArray(old.reports)||old.reports.some((x:unknown)=>typeof x!=='string'))))
  throw Object.assign(Error('PLANNING_FEEDBACK_INVALID'),{code:'PLANNING_FEEDBACK_INVALID'});
 const feedback={...old,feedbackIds:[...new Set<string>([...(old.feedbackIds??[]),...row.pending.map((f:any)=>f.id)])],
  reports:[...(old.reports??[]),...row.pending.map((f:any)=>f.description)]};
 await db.query('UPDATE planning_runs SET feedback=$2 WHERE id=$1',[planningRunId,JSON.stringify(feedback)]);
 await event(db,'PLANNING_FEEDBACK_REFRESHED',row.objective_id,{planningRunId,
  addedFeedbackIds:row.pending.map((f:any)=>f.id),addedReports:row.pending.length});
 return feedback;
}
