import { createHash } from 'node:crypto';
import { requireRunCriteriaMetadataReady, runCriteriaMetadataColumns,
  checkedRunCriteriaMetadata } from './run-criteria-metadata.js';
import type { DB } from './db.js';
import { checkedExecutionMetadata, executionMetadataColumns } from './task-execution-metadata.js';
import {acceptedRequirementOutcomes,resolveRequirementOutcomes,type RequirementOutcomeProof} from './requirement-outcomes.js';

const live = new Set(['queued', 'running', 'cancelling', 'reconciling']);
const retired = new Set(['cancelled', 'superseded']);
const waits = new Set(['waiting_quota', 'waiting_resource', 'waiting_configuration', 'retry_scheduled']);
const sha = (value: unknown) => createHash('sha256').update(JSON.stringify(value)).digest('hex');

/** Domain-neutral input. Only adapters reading controller state may set accepted. */
export type FrontierNode = {
  id: string; title: string; state: string; scope: string; stage?: string;
  dependencies: string[]; criteria: Array<{ id: string; label: string }>;
  accepted: boolean; live: boolean; auxiliary?: boolean; reason?: string | null;
  runId?: string | null; attempt?: number; receipt?: string | null;
  failedCriteria?: string[]; covers?: string[];
  planRevision?:number;requirementOutcomes?:RequirementOutcomeProof|null;
};
export type FrontierRequirement = { id: string; description: string };

/** Heartbeats, summaries, new attempts and memory changes cannot advance this frontier.
 * Different recurrence IDs are different boundaries, even with identical artifacts. */
export function projectWorkFrontier(
  boundary: { kind: 'objective' | 'occurrence'; id: string; version?: number },
  nodes: FrontierNode[], requirements: FrontierRequirement[] = [],
  completionContractVersion?:2,
) {
  const active = nodes.filter(n => !retired.has(n.state));
  const byId = new Map(active.map(n => [n.id, n]));
  const deliveries = active.filter(n => !n.auxiliary && (n.stage === 'verify' ||
    !active.some(other => !other.auxiliary && other.dependencies.includes(n.id))));
  const accepted = (n: FrontierNode) => n.accepted && !n.live && n.criteria.length > 0;
  const missingDependencies = (n: FrontierNode) => n.dependencies.filter(id => !byId.has(id) || !accepted(byId.get(id)!));
  const entries = active.map(n => {
    const missing = missingDependencies(n);
    const state = accepted(n) ? 'accepted' : n.live ? 'executing' : missing.length ? 'blocked_dependency' :
      waits.has(n.state) ? 'waiting_capacity' : n.state === 'ready' ? 'ready' : 'needs_supervision';
    return { taskId:n.id, title:n.title, scope:n.scope, stage:n.stage ?? 'work', state,taskState:n.state,
      auxiliary:!!n.auxiliary, expectedAttempt:n.attempt ?? 0, runId:n.runId ?? null,
      blockedBy:missing, failedCriteria:n.failedCriteria ?? [], reason:n.reason ?? null,
      criteria:n.criteria.map(c => ({...c,state:accepted(n) ? 'accepted' :
        n.failedCriteria?.includes(c.id) ? 'failed' : 'unproven'})) };
  });
  const requirementEntries = completionContractVersion===2?resolveRequirementOutcomes(requirements,active):requirements.map(r => {
    const owners = deliveries.filter(n => n.covers?.includes(r.id));
    return {...r,taskIds:owners.map(n=>n.id),state:owners.length && owners.every(accepted) ? 'accepted' :
      !owners.length ? 'unplanned' : 'pending'};
  });
  const obligations = deliveries.flatMap(n => n.criteria.map(c=>({taskId:n.id,id:c.id,
    accepted:accepted(n),receipt:n.receipt ?? null})));
  const receipts = obligations.filter(c=>c.accepted).map(c=>sha([boundary,c.taskId,c.id,c.receipt])).sort();
  const rootBlockers = entries.filter(e=>e.state==='needs_supervision' ||
    (e.state==='blocked_dependency' && e.blockedBy.some(id=>!byId.has(id))));
  const counts = {
    accepted:obligations.filter(c=>c.accepted).length,total:obligations.length,
    ready:entries.filter(e=>e.state==='ready').length,
    executing:entries.filter(e=>e.state==='executing').length,
    blocked:entries.filter(e=>e.state==='blocked_dependency').length,
    waitingCapacity:entries.filter(e=>e.state==='waiting_capacity').length,
    needsSupervision:rootBlockers.length,
  };
  const complete = obligations.length>0 && obligations.every(c=>c.accepted) &&
    requirementEntries.every(r=>r.state==='accepted');
  const externalWait=completionContractVersion===2&&requirementEntries.some(r=>r.state==='externally_blocked')&&
    requirementEntries.every(r=>['accepted','externally_blocked'].includes(r.state))&&
    !counts.ready&&!counts.executing&&!counts.blocked&&!counts.waitingCapacity&&!counts.needsSupervision;
  const next = complete ? 'acceptance_ready' : externalWait?'await_external_dependency':requirementEntries.some(r=>r.state==='unplanned') || !deliveries.length ?
    'planning_gap' : rootBlockers.length ? 'supervise_blockers' : counts.ready ? 'dispatch_ready' :
    counts.executing ? 'prepare_independent_work' : counts.waitingCapacity ? 'await_capacity' : 'planning_gap';
  return {version:1,boundary,counts,next,complete,requirements:requirementEntries,entries,
    rootBlockerTaskIds:rootBlockers.map(e=>e.taskId),receipts,
    deliveryFingerprint:sha([boundary,receipts]),
    stateFingerprint:sha([boundary,entries.map(e=>[e.taskId,e.state,e.blockedBy,e.failedCriteria]),requirementEntries]),
    scopeSummary:[...new Set(entries.map(e=>e.scope))].sort().map(scope=>({scope,
      pending:entries.filter(e=>e.scope===scope && e.taskState!=='accepted').length,
      taskIds:entries.filter(e=>e.scope===scope && e.taskState!=='accepted').map(e=>e.taskId)}))};
}

/** A bounded operational projection, not another copy of historical task text. */
export function compactWorkFrontier(frontier:ReturnType<typeof projectWorkFrontier>,maxCharacters=6000) {
  const pending=frontier.entries.filter(e=>!retired.has(e.taskState) && e.taskState!=='accepted');
  const visible=new Set(pending.map(e=>e.taskId));
  const compact={version:frontier.version,boundary:frontier.boundary,counts:frontier.counts,
    next:frontier.next,complete:frontier.complete,deliveryFingerprint:frontier.deliveryFingerprint,
    rootBlockerTaskIds:frontier.rootBlockerTaskIds.filter(id=>visible.has(id)).slice(0,12),
    scopeSummary:[] as any[],requirements:[] as any[],entries:[] as any[],
    selection:{characterLimit:maxCharacters,totalPending:pending.length,omittedEntries:pending.length,
      omittedRequirements:frontier.requirements.length,omittedScopes:frontier.scopeSummary.length,
      truncated:false,recovery:{taskEntity:'tasks',criterionField:'criteria',planEntity:'plan_revisions'}}};
  const append=(array:any[],value:any)=>{
    array.push(value);
    if(JSON.stringify(compact).length<=maxCharacters-200)return true;
    array.pop();return false;
  };
  for(const scope of frontier.scopeSummary)append(compact.scopeSummary,{scope:scope.scope,pending:scope.pending,
    taskIds:scope.taskIds.filter(id=>visible.has(id)).slice(0,6),
    omittedTaskIds:Math.max(0,scope.taskIds.filter(id=>visible.has(id)).length-6)});
  for(const requirement of frontier.requirements)append(compact.requirements,{id:requirement.id,
    description:requirement.description.slice(0,180),state:requirement.state,
    taskIds:requirement.taskIds.slice(0,4),omittedTaskIds:Math.max(0,requirement.taskIds.length-4)});
  for(const entry of pending)append(compact.entries,{taskId:entry.taskId,scope:entry.scope,state:entry.state,
    stage:entry.stage,expectedAttempt:entry.expectedAttempt,runId:entry.runId,
    blockedBy:entry.blockedBy.slice(0,6),omittedDependencies:Math.max(0,entry.blockedBy.length-6),
    failedCriteria:entry.failedCriteria.slice(0,6),
    criteria:entry.criteria.slice(0,6).map(c=>({id:c.id,state:c.state})),omittedCriteria:Math.max(0,entry.criteria.length-6)});
  compact.selection.omittedEntries=pending.length-compact.entries.length;
  compact.selection.omittedRequirements=frontier.requirements.length-compact.requirements.length;
  compact.selection.omittedScopes=frontier.scopeSummary.length-compact.scopeSummary.length;
  compact.selection.truncated=compact.selection.omittedEntries>0 || compact.selection.omittedRequirements>0 ||
    compact.selection.omittedScopes>0 || compact.scopeSummary.some(s=>s.omittedTaskIds>0) ||
    compact.entries.some(e=>e.omittedCriteria>0 || e.omittedDependencies>0);
  return compact;
}

export function objectiveFrontierNodes(tasks:any[], runs:any[], acceptances:any[], dependencies:any[], artifacts:any[]=[]):FrontierNode[] {
  // Reviews are supporting dependencies. Omitting their nodes makes an already
  // accepted review appear missing to every downstream delivery and planner.
  return tasks.filter(t=>t.stage!=='coordinate').map(t=>{
    const run = runs.filter(r=>r.task_id===t.id).sort((a,b)=>b.attempt-a.attempt)[0];
    const acceptance = run && acceptances.filter(a=>a.run_id===run.id)
      .sort((a,b)=>Date.parse(b.created_at)-Date.parse(a.created_at))[0];
    const isLive = runs.some(r=>r.task_id===t.id && live.has(r.state)) || live.has(t.state);
    const frozenCriteria=run?.accepted_criteria ?? run?.instruction_snapshot?.task?.criteria;
    const sameCriteria=!frozenCriteria || JSON.stringify(frozenCriteria)===JSON.stringify(t.criteria);
    return {id:t.id,title:t.title,state:t.state,stage:t.stage,planRevision:t.plan_revision,
      requirementOutcomes:acceptance?.passed===true&&run?acceptedRequirementOutcomes(t.criteria??[],acceptance.checks??[],run.id,artifacts,run.fence):null,
      scope:t.execution?.baseWorkspace ?? t.execution?.workspace ?? 'objective',
      dependencies:dependencies.filter(d=>d.task_id===t.id).map(d=>d.depends_on),criteria:t.criteria ?? [],
      accepted:t.state==='accepted' && run?.state==='submitted' && run.attempt===t.attempt_count &&
        acceptance?.passed===true && sameCriteria && t.criteria?.every((c:any)=>
          acceptance.checks?.some((check:any)=>check.id===c.id && check.passed===true)),live:isLive,
      auxiliary:t.stage==='review' || ['diagnostic','repair','planning'].includes(t.execution?.workKind),reason:t.reason,
      runId:run?.id,attempt:t.attempt_count,covers:t.covers ?? [],
      receipt:acceptance ? sha([acceptance.checks?.map((c:any)=>[c.id,c.passed]),
        artifacts.filter(a=>a.run_id===run.id && t.criteria?.some((c:any)=>c.name===a.name))
          .map(a=>[a.name,a.digest]).sort()]) : null,
      failedCriteria:acceptance?.checks?.filter((c:any)=>c.passed===false).map((c:any)=>c.id) ?? []};
  });
}

export function occurrenceFrontier(occurrence:any, steps:any[], attempts:any[], artifacts:any[]) {
  if (steps.some(s=>s.occurrence_id && s.occurrence_id!==occurrence.id)) throw Error('FRONTIER_SCOPE');
  const byKey = new Map(steps.map(s=>[s.key,s.id]));
  const nodes:FrontierNode[] = steps.map(s=>{
    const attempt = attempts.filter(a=>a.step_id===s.id).sort((a,b)=>b.number-a.number)[0];
    const criteria = s.definition.criteria?.length ? s.definition.criteria :
      [{id:'execution_receipt',label:'Etapa concluída pelo controlador'}];
    return {id:s.id,title:s.definition.title,state:s.state==='queued' ? 'ready' : s.state==='waiting' ? 'retry_scheduled' : s.state,scope:'occurrence:'+occurrence.id,
      stage:'work',dependencies:(s.definition.dependsOn ?? []).map((key:string)=>byKey.get(key) ?? 'missing:'+key),
      criteria,accepted:s.state==='completed' && attempt?.state==='completed',
      live:attempts.some(a=>a.step_id===s.id && ['running','cancelling','reconciling'].includes(a.state)),
      reason:s.reason,runId:attempt?.id,attempt:attempt?.number,
      receipt:attempt ? sha([attempt.id,attempt.receipt,artifacts.filter(a=>a.attempt_id===attempt.id)
        .map(a=>[a.name,a.digest]).sort()]) : null};
  });
  return projectWorkFrontier({kind:'occurrence',id:occurrence.id,version:occurrence.version},nodes);
}

/** This snapshot is metadata only; execution and acceptance remain with their controllers. */
export async function readObjectiveFrontierTasks(db:DB,goalId:string) {
  return checkedExecutionMetadata((await db.query(`SELECT t.id,t.title,t.state,t.stage,t.criteria,t.covers,t.attempt_count,t.reason,t.plan_revision,
    ${executionMetadataColumns},
    CASE WHEN NOT metadata.execution_present THEN NULL ELSE jsonb_build_object(
      'baseWorkspace',control."baseWorkspace",'workspace',control.workspace,
      'workKind',control."workKind") END execution
    FROM tasks t LEFT JOIN task_execution_metadata metadata ON metadata.task_id=t.id
    LEFT JOIN LATERAL jsonb_to_record(metadata.control)
      AS control("baseWorkspace" text,workspace text,"workKind" text) ON true
    WHERE t.goal_id=$1 AND t.stage IS DISTINCT FROM 'coordinate'`,[goalId])).rows);
}

export async function readObjectiveFrontier(db:DB,goalId:string) {
  await requireRunCriteriaMetadataReady(db);
  const tasks=await readObjectiveFrontierTasks(db,goalId);
  // Progress needs the current attempt and any older uncertain actor, never
  // historical prompts, provider payloads or base64 inputs from every attempt.
  const runs=checkedRunCriteriaMetadata((await db.query(`SELECT r.id,r.task_id,r.attempt,r.state,r.fence,
    ${runCriteriaMetadataColumns},
    CASE WHEN t.state='accepted' AND r.state='submitted' AND r.attempt=t.attempt_count
      THEN criteria_metadata.criteria END accepted_criteria
    FROM runs r JOIN tasks t ON t.id=r.task_id
    LEFT JOIN run_criteria_metadata criteria_metadata ON criteria_metadata.run_id=r.id
    WHERE t.goal_id=$1 AND t.stage IS DISTINCT FROM 'coordinate'
    AND (r.attempt=t.attempt_count OR r.state IN('queued','running','cancelling','reconciling'))`,[goalId])).rows);
  const acceptances=(await db.query('SELECT id,task_id,run_id,passed,checks,created_at FROM acceptances WHERE run_id=ANY($1::uuid[])',[runs.map(run=>run.id)])).rows;
  const dependencies=(await db.query('SELECT d.* FROM dependencies d JOIN tasks t ON t.id=d.task_id WHERE t.goal_id=$1',[goalId])).rows;
  const artifacts=(await db.query('SELECT a.id,a.run_id,a.name,a.digest,a.size FROM artifacts a JOIN runs r ON r.id=a.run_id JOIN tasks t ON t.id=r.task_id WHERE t.goal_id=$1 AND r.attempt=t.attempt_count',[goalId])).rows;
  const plan=(await db.query('SELECT revision,plan FROM plan_revisions WHERE objective_id=$1 ORDER BY revision DESC LIMIT 1',[goalId])).rows[0];
  return projectWorkFrontier({kind:'objective',id:goalId,version:plan?.revision},
    objectiveFrontierNodes(tasks,runs,acceptances,dependencies,artifacts),plan?.plan.requirements ?? [],plan?.plan.completionContractVersion);
}
