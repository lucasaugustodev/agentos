import {createHash,randomUUID} from 'node:crypto';
import {z} from 'zod';
import {pool,tx,lock,event,type DB} from './db.js';
import {DomainError} from './contracts.js';
import {checkMemoryContent} from './memory.js';
import {queueFeedback} from './planner.js';
import {captureSourceReplanInputs,type SourceReuseCapture} from './planning-source-reuse-adapter.js';
import {assertPlanningSourcesIdle} from './planning-explicit-source.js';
import {inspectPublishedBaseline,validatePublishedBaseline} from './published-rebase.js';
import {inspectPlanningSource} from './workspace-inspection.js';
import {accountBridge} from './bridge.js';
import {readArtifact} from './service.js';
import {runtimeCandidateDigest} from './operation-progress.js';
import {preparationHash as hash} from './source-preparation-contract.js';
import {currentObjectiveRouteGeneration} from './objective-model-route.js';

const digest=z.string().regex(/^[a-f0-9]{64}$/),workspace=z.string().regex(/^agentos\/src[0-9a-f]{16}$/);
const contribution=z.object({taskId:z.uuid(),runId:z.uuid(),acceptanceId:z.uuid(),
 candidateArtifactId:z.uuid(),candidateArtifactDigest:digest,patchArtifactId:z.uuid(),patchArtifactDigest:digest}).strict();
export const sourceReplanSchema=z.object({version:z.literal(1),expectedRevision:z.number().int().positive(),baseWorkspace:workspace,
 published:z.object({kind:z.literal('current_published_runtime'),workspace,releaseDigest:digest,registeredReceiptDigest:digest}).strict(),
 contributions:z.array(contribution).min(1).max(64)}).strict();
export const sourceReplanFeedbackSchema=z.object({idempotencyKey:z.string().min(8).max(160),
 description:z.string().min(5).max(5000),sourceReplan:sourceReplanSchema}).strict();
type Request=z.infer<typeof sourceReplanFeedbackSchema>;
type Auth={channel:'api-token'|'operator-session'};
type Inputs=ReturnType<typeof inputProjection>;
type Source={workspace:string;candidateDigest:string;snapshotDigest:string;sourceGenerationKind:'snapshot'|'unmarked'};
type Payload={version:1;feedbackId:string;goalId:string;request:Request;requestDigest:string;authChannel:Auth['channel'];
 inputs:Inputs;inputsDigest:string;publishedBaseline:any;source:Source;acceptance:false;finalVerificationRequired:true;
 routeGeneration?:{id:string;receiptDigest:string}|null};
type Receipt={payload:Payload;receiptDigest:string};
const fail=(code:string)=>new DomainError(code,'O replanejamento exige a publicação e todas as contribuições aceitas exatas, sem execução ou autoridade concorrente.');
const sha=(v:Buffer)=>createHash('sha256').update(v).digest('hex');
const same=(a:unknown,b:unknown)=>hash(a)===hash(b);

function inputProjection(c:SourceReuseCapture){
 return {scope:c.scope,rootWorkspace:c.rootWorkspace,requestDigest:c.planningAuthority.requestDigest,
  contributors:c.contributors,artifactMimes:c.artifactMimes};
}
function contributions(inputs:Inputs){
 return inputs.contributors.map(c=>{
  const candidate=c.artifacts.filter(a=>a.name==='candidate.json'),patch=c.artifacts.filter(a=>a.name==='changes.patch');
  if(c.execution.readOnlySource||candidate.length!==1||patch.length!==1)throw fail('SOURCE_REPLAN_ARTIFACTS_REQUIRED');
  return {taskId:c.taskId,runId:c.runId,acceptanceId:c.latestAcceptance.id,candidateArtifactId:candidate[0].id,
   candidateArtifactDigest:candidate[0].digest,patchArtifactId:patch[0].id,patchArtifactDigest:patch[0].digest};
 }).sort((a,b)=>a.taskId.localeCompare(b.taskId));
}
async function capture(db:DB,goalId:string,request:Request,planningRunId?:string):Promise<Inputs>{
 const s=request.sourceReplan;
 const objective=(await db.query(`SELECT o.revision,o.mode,o.request,g.state FROM objectives o JOIN goals g ON g.id=o.id WHERE o.id=$1`,[goalId])).rows[0];
 if(!objective||objective.revision!==s.expectedRevision||objective.mode!=='execute'||
   !['active','failed','replanning'].includes(objective.state)||objective.request.project!=='agentos'||
   !same(objective.request.workspaces,[s.baseWorkspace]))throw fail('SOURCE_REPLAN_TARGET_CHANGED');
 if((await db.query(`SELECT 1 FROM settings WHERE id='workforce:'||$1 AND value->>'enabled'='true'
   UNION ALL SELECT 1 FROM planning_runs WHERE objective_id=$1::uuid AND state IN('queued','running','waiting_quota')
     AND ($2::uuid IS NULL OR id<>$2::uuid)
   UNION ALL SELECT 1 FROM settings WHERE value->>'goalId'=$1 AND
     (id LIKE 'published-rebase:%' AND value->>'state' IN('proposed','preparing','prepared','applying','uncertain') OR
      id LIKE 'planning-source-preparation:%' AND value->>'state'<>'applied' AND
        ($2::text IS NULL OR value->>'planningRunId'<>$2::text)) LIMIT 1`,[goalId,planningRunId??null])).rowCount)
  throw fail('SOURCE_REPLAN_OWNER_BUSY');
 const tasks=(await db.query('SELECT id,execution->>\'workspace\' workspace FROM tasks WHERE goal_id=$1',[goalId])).rows;
 await assertPlanningSourcesIdle(db,{feedback:{replaceTaskIds:tasks.map(t=>t.id)}},
  [...tasks.map(t=>t.workspace).filter(Boolean),s.published.workspace]);
 const published=(await db.query("SELECT value->>'workspace' workspace FROM settings WHERE id='agentos-source-workspace'")).rows[0]?.workspace;
 if(published!==s.published.workspace)throw fail('SOURCE_REPLAN_PUBLICATION_CHANGED');
 const result=await captureSourceReplanInputs(db,{goalId,revision:s.expectedRevision,baseWorkspace:s.baseWorkspace});
 if(!result.eligible)throw fail('SOURCE_REPLAN_INPUTS_'+result.reason);
 const inputs=inputProjection(result.capture);
 const count=Number((await db.query("SELECT count(*) n FROM tasks WHERE goal_id=$1 AND plan_revision=$2 AND state='accepted' AND stage IN('work','integrate')",[goalId,s.expectedRevision])).rows[0].n);
 if(count!==inputs.contributors.length||!same(contributions(inputs),s.contributions))throw fail('SOURCE_REPLAN_CONTRIBUTIONS_CHANGED');
 return inputs;
}

/** Artifact bytes are inputs only. This validates their immutable identity and
 * source edges; it does not transplant a workspace or certify a merged result. */
async function inspectInputs(inputs:Inputs){
 const candidates=new Map<string,any>();let total=0;
 for(const c of inputs.contributors){
  for(const a of c.artifacts.filter(a=>['candidate.json','changes.patch'].includes(a.name))){
   total+=a.size;if(total>32*1024*1024)throw fail('SOURCE_REPLAN_EVIDENCE_BOUND');
   const actual=await readArtifact(a.id);
   if(actual.run_id!==a.runId||actual.name!==a.name||actual.digest!==a.digest||actual.size!==a.size||
     actual.bytes.length!==a.size||sha(actual.bytes)!==a.digest)throw fail('SOURCE_REPLAN_ARTIFACT_CHANGED');
   if(a.name==='candidate.json'){
    if(!runtimeCandidateDigest(actual.bytes,a.digest))throw fail('SOURCE_REPLAN_CANDIDATE_INVALID');
    candidates.set(c.taskId,JSON.parse(actual.bytes.toString('utf8')));
   }
  }
 }
 const byWorkspace=new Map(inputs.contributors.map(c=>[c.execution.workspace,c]));
 if(byWorkspace.size!==inputs.contributors.length)throw fail('SOURCE_REPLAN_SOURCE_EDGE_UNPROVEN');
 for(const c of inputs.contributors){
  const visited=new Set<string>();let current:typeof c|undefined=c;
  while(current){
   if(visited.has(current.taskId))throw fail('SOURCE_REPLAN_SOURCE_EDGE_UNPROVEN');visited.add(current.taskId);
   const parent=byWorkspace.get(current.execution.sourceWorkspace);
   if(parent){
    if(!current.dependencyTaskIds.includes(parent.taskId)||candidates.get(current.taskId).baseDigest!==candidates.get(parent.taskId).candidateDigest||
      parent.artifacts.filter(a=>['candidate.json','changes.patch'].includes(a.name)).some(a=>!current!.frozenParentInputs.some(r=>same(r,a))))
     throw fail('SOURCE_REPLAN_SOURCE_EDGE_UNPROVEN');
   }else if(current.execution.sourceWorkspace!==inputs.rootWorkspace)throw fail('SOURCE_REPLAN_SOURCE_EDGE_UNPROVEN');
   current=parent;
  }
 }
}
async function observePublication(request:Request){
 const baseline=validatePublishedBaseline(await inspectPublishedBaseline()),expected=request.sourceReplan.published;
 if(baseline.sourceWorkspace!==expected.workspace||baseline.releaseDigest!==expected.releaseDigest||
   baseline.registeredReceiptDigest!==expected.registeredReceiptDigest)throw fail('SOURCE_REPLAN_PUBLICATION_CHANGED');
 const observed=await inspectPlanningSource(expected.workspace,'root',(method,params,options)=>accountBridge.request(method,params,options));
 const age=Date.now()-Date.parse(observed.checkedAt);
 if(!Number.isFinite(age)||age<0||age>60000)throw fail('SOURCE_REPLAN_OBSERVATION_STALE');
 const source:Source={workspace:observed.workspace,candidateDigest:observed.sourceDigest,snapshotDigest:observed.generationDigest,
  sourceGenerationKind:observed.sourceGenerationKind};
 return {baseline,source,checkedAt:Date.parse(observed.checkedAt)};
}
async function verified(db:DB,row:any):Promise<Receipt>{
 const r=row?.source_replan as Receipt,p=r?.payload;
 if(!p||p.version!==1||p.goalId!==row.objective_id||p.feedbackId!==row.id||hash(p)!==r.receiptDigest||
   !['api-token','operator-session'].includes(p.authChannel)||p.request.idempotencyKey!==row.trigger_key||p.request.description!==row.description||
   hash(p.request)!==p.requestDigest||hash(p.inputs)!==p.inputsDigest||p.acceptance!==false||p.finalVerificationRequired!==true||
   !sourceReplanFeedbackSchema.safeParse(p.request).success||!same(contributions(p.inputs),p.request.sourceReplan.contributions))
  throw fail('SOURCE_REPLAN_RECEIPT_INVALID');
 if(!(await db.query(`SELECT 1 FROM events WHERE kind='OBJECTIVE_SOURCE_REPLAN_REQUESTED' AND entity_id=$1
   AND payload->>'feedbackId'=$2 AND payload->>'receiptDigest'=$3`,[p.goalId,p.feedbackId,r.receiptDigest])).rowCount)
  throw fail('SOURCE_REPLAN_RECEIPT_INVALID');
 return r;
}
function response(r:Receipt,duplicate:boolean){return {recorded:true,feedbackId:r.payload.feedbackId,receiptDigest:r.receiptDigest,
 duplicate,activation:'new_planning_run',oldAttemptsChanged:false,acceptance:false};}

export async function registerSourceReplanFeedback(goalId:string,raw:unknown,auth:Auth){
 z.uuid().parse(goalId);z.enum(['api-token','operator-session']).parse(auth?.channel);
 const request=sourceReplanFeedbackSchema.parse(raw);
 request.sourceReplan.contributions.sort((a,b)=>a.taskId.localeCompare(b.taskId));
 if(new Set(request.sourceReplan.contributions.map(c=>c.taskId)).size!==request.sourceReplan.contributions.length)
  throw fail('SOURCE_REPLAN_CONTRIBUTIONS_CHANGED');
 checkMemoryContent(request.description);const requestDigest=hash(request);
 const prior=async(db:DB)=>{
  const row=(await db.query('SELECT * FROM objective_feedback WHERE objective_id=$1 AND trigger_key=$2',[goalId,request.idempotencyKey])).rows[0];
  if(!row)return undefined;
  if(row.source_replan?.payload?.requestDigest!==requestDigest)throw fail('IDEMPOTENCY_CONFLICT');
  return verified(db,row);
 };
 const initial=await tx(async db=>{await lock(db);const old=await prior(db);
  if(old)return {old};
  if((await db.query(`SELECT 1 FROM objective_feedback f WHERE objective_id=$1 AND source_replan IS NOT NULL
    AND NOT EXISTS(SELECT 1 FROM planning_runs p WHERE p.objective_id=f.objective_id AND p.feedback->'feedbackIds' ? f.id::text)`,[goalId])).rowCount)
   throw fail('SOURCE_REPLAN_OWNER_BUSY');
  return {inputs:await capture(db,goalId,request),routeGeneration:await currentObjectiveRouteGeneration(db,goalId)};});
 if(initial.old)return response(initial.old,true);
 const inputs=initial.inputs!;await inspectInputs(inputs);const observed=await observePublication(request);
 return tx(async db=>{await lock(db);const old=await prior(db);if(old)return response(old,true);
  if(!same(await capture(db,goalId,request),inputs))throw fail('SOURCE_REPLAN_CONTRIBUTIONS_CHANGED');
  if(!same(await currentObjectiveRouteGeneration(db,goalId),initial.routeGeneration))throw fail('SOURCE_REPLAN_ROUTE_CHANGED');
  if(Date.now()-observed.checkedAt>60000)throw fail('SOURCE_REPLAN_OBSERVATION_STALE');
  if((await db.query(`SELECT 1 FROM objective_feedback f WHERE objective_id=$1 AND source_replan IS NOT NULL
    AND NOT EXISTS(SELECT 1 FROM planning_runs p WHERE p.objective_id=f.objective_id AND p.feedback->'feedbackIds' ? f.id::text)`,[goalId])).rowCount)
   throw fail('SOURCE_REPLAN_OWNER_BUSY');
  const feedbackId=randomUUID(),payload:Payload={version:1,feedbackId,goalId,request,requestDigest,authChannel:auth.channel,
   inputs,inputsDigest:hash(inputs),publishedBaseline:observed.baseline,source:observed.source,acceptance:false,finalVerificationRequired:true,
   routeGeneration:initial.routeGeneration};
  const receipt:Receipt={payload,receiptDigest:hash(payload)};
  await queueFeedback(db,goalId,request.idempotencyKey,request.description,[],{id:feedbackId,sourceReplan:receipt});
  await event(db,'OBJECTIVE_SOURCE_REPLAN_REQUESTED',goalId,{feedbackId,receiptDigest:receipt.receiptDigest,requestDigest,
   authChannel:auth.channel,expectedRevision:request.sourceReplan.expectedRevision,oldAttemptsChanged:false,acceptance:false});
  return response(receipt,false);
 });
}

/** The existing planning run is the sole consumer. Old receipts carried as
 * history never become fresh authority for a later run/revision. */
export async function sourceReplanForFeedback(db:DB,goalId:string,pending:any[]){
 const rows=pending.filter(f=>f.source_replan);
 const fresh=[];
 for(const row of rows)if(!(await db.query("SELECT 1 FROM planning_runs WHERE objective_id=$1 AND feedback->'feedbackIds' ? $2",[goalId,row.id])).rowCount)fresh.push(row);
 if(!fresh.length)return undefined;if(fresh.length!==1)throw fail('SOURCE_REPLAN_OWNER_BUSY');
 const receipt=await verified(db,fresh[0]);
 if(!same(receipt.payload.routeGeneration??null,await currentObjectiveRouteGeneration(db,goalId)))throw fail('SOURCE_REPLAN_ROUTE_CHANGED');
 return receipt;
}
export async function assertSourceReplanCurrent(db:DB,run:any){
 const ref=run.feedback?.sourceReplan;if(!ref)return;
 const row=(await db.query('SELECT * FROM objective_feedback WHERE id=$1 AND objective_id=$2',[ref.feedbackId,run.objective_id])).rows[0];
 const receipt=await verified(db,row),p=receipt.payload,s=p.request.sourceReplan;
 if((p.routeGeneration?.id??null)!==(run.route_generation_id??null)||
   !same(p.routeGeneration??null,await currentObjectiveRouteGeneration(db,run.objective_id)))throw fail('SOURCE_REPLAN_ROUTE_CHANGED');
 if(receipt.receiptDigest!==ref.receiptDigest||!run.feedback.feedbackIds?.includes(row.id)||run.base_revision!==s.expectedRevision||
   run.feedback.sourceOverrides?.[s.baseWorkspace]!==p.source.workspace||
   !same(run.feedback.sourceReplanInputs,p.inputs)||p.inputs.contributors.some(c=>c.artifacts
     .filter(a=>['candidate.json','changes.patch'].includes(a.name)).some(a=>
       !run.feedback.artifacts?.some((r:any)=>r.id===a.id&&r.name===a.name&&r.digest===a.digest)))||
   (await db.query(`SELECT 1 FROM planning_runs WHERE objective_id=$1 AND id<>$2 AND feedback->'sourceReplan'->>'feedbackId'=$3`,[run.objective_id,run.id,row.id])).rowCount)
  throw fail('SOURCE_REPLAN_RECEIPT_INVALID');
 if(!same(await capture(db,run.objective_id,p.request,run.id),p.inputs))throw fail('SOURCE_REPLAN_CONTRIBUTIONS_CHANGED');
 return receipt;
}
/** Called outside admission transactions, before any source preparation. */
export async function inspectSourceReplanCurrent(run:any){
 const receipt=await assertSourceReplanCurrent(pool,run);if(!receipt)return;
 await inspectInputs(receipt.payload.inputs);
 const observed=await observePublication(receipt.payload.request);
 if(!same(observed.baseline,receipt.payload.publishedBaseline)||!same(observed.source,receipt.payload.source))
  throw fail('SOURCE_REPLAN_PUBLICATION_CHANGED');
 return receipt;
}
