import {randomUUID} from 'node:crypto';
import {z} from 'zod';
import {readModelPolicy} from '../bridge/model-policy.mjs';
import {tx,lock,event,type DB} from './db.js';
import {DomainError} from './contracts.js';
import {objectiveSchema,effectivePlanningRequest,type ObjectiveInput} from './objective-contracts.js';
import {preparationHash as hash} from './source-preparation-contract.js';
import {assertPlanningSourcesIdle} from './planning-explicit-source.js';
import {checkMemoryContent} from './memory.js';

const digest=z.string().regex(/^[a-f0-9]{64}$/);
export const objectiveModelRouteSchema=z.object({idempotencyKey:z.string().min(8).max(160),
 expectedGoalId:z.uuid(),expectedRevision:z.number().int().nonnegative(),expectedRequestDigest:digest,expectedPolicyDigest:digest,
 expectedGenerationId:z.uuid().nullable(),reason:z.string().min(10).max(2000)}).strict();
type Request=z.infer<typeof objectiveModelRouteSchema>;
type Auth={channel:'api-token'|'operator-session'};
type Payload={version:1;id:string;goalId:string;generation:number;request:Request;requestDigest:string;
 authChannel:Auth['channel'];originalRequest:ObjectiveInput;originalRequestDigest:string;
 policy:any;policyDigest:string;effectiveRequest:ObjectiveInput;effectiveRequestDigest:string};
const same=(a:unknown,b:unknown)=>hash(a)===hash(b);
const fail=(code:string)=>new DomainError(code,'A transição exige objetivo inativo e a revisão, pedido e política autenticados exatos.');
async function objective(db:DB,id:string){
 const row=(await db.query(`SELECT o.*,g.state goal_state,
  encode(sha256(convert_to(o.request::text,'UTF8')),'hex') original_digest
  FROM objectives o JOIN goals g ON g.id=o.id WHERE o.id=$1`,[id])).rows[0];
 if(!row)throw fail('OBJECTIVE_MODEL_ROUTE_NOT_FOUND');
 return row;
}
async function policy(db:DB){
 const current=readModelPolicy(),stored=(await db.query("SELECT value FROM settings WHERE id='model-policy'")).rows[0]?.value;
 if(current?.version!==2||!same(current,stored))throw fail('OBJECTIVE_MODEL_ROUTE_POLICY_CHANGED');
 const account=(await db.query('SELECT * FROM accounts WHERE id=$1',[current.accountId])).rows[0];
 if(!account?.enabled||account.bridge_id!==current.bridgeId||account.connection_status!=='connected'||
   !account.models.includes(current.model))throw fail('OBJECTIVE_MODEL_ROUTE_ACCOUNT_UNAVAILABLE');
 return current;
}
async function latest(db:DB,id:string){
 return (await db.query('SELECT * FROM objective_model_routes WHERE objective_id=$1 ORDER BY generation DESC LIMIT 1',[id])).rows[0];
}
async function verify(db:DB,row:any):Promise<Payload>{
 const p=row?.payload as Payload;
 if(!p||p.version!==1||p.id!==row.id||p.goalId!==row.objective_id||p.generation!==row.generation||
   p.request.expectedGoalId!==p.goalId||p.request.idempotencyKey!==row.idempotency_key||hash(p)!==row.receipt_digest||hash(p.request)!==p.requestDigest||
   !objectiveModelRouteSchema.safeParse(p.request).success||!['api-token','operator-session'].includes(p.authChannel)||
   p.policy?.version!==2||hash(p.policy)!==p.policyDigest||p.originalRequestDigest!==p.request.expectedRequestDigest||
   p.policyDigest!==p.request.expectedPolicyDigest||
   hash(p.effectiveRequest)!==p.effectiveRequestDigest||
   !same(p.effectiveRequest,{...p.originalRequest,plannerAccountId:p.policy.accountId,plannerModel:p.policy.model,allowedAccounts:[p.policy.accountId]})||
   !objectiveSchema.safeParse(p.effectiveRequest).success)
  throw fail('OBJECTIVE_MODEL_ROUTE_RECEIPT_INVALID');
 if(!(await db.query(`SELECT 1 FROM events WHERE kind='OBJECTIVE_MODEL_ROUTE_REGISTERED' AND entity_id=$1
  AND payload->>'generationId'=$2 AND payload->>'receiptDigest'=$3`,[p.goalId,p.id,row.receipt_digest])).rowCount)
  throw fail('OBJECTIVE_MODEL_ROUTE_RECEIPT_INVALID');
 return p;
}
async function quiet(db:DB,o:any){
 if(o.mode!=='execute'||!['active','failed','replanning'].includes(o.goal_state))throw fail('OBJECTIVE_MODEL_ROUTE_STATE');
 if((await db.query(`SELECT 1 FROM settings WHERE id='workforce:'||$1 AND value->>'enabled'='true'
  UNION ALL SELECT 1 FROM settings WHERE id='maintenance' AND value->>'draining'='true'
  UNION ALL SELECT 1 FROM planning_runs WHERE objective_id=$1::uuid AND state IN('queued','running','waiting_quota')
  UNION ALL SELECT 1 FROM objective_feedback f WHERE objective_id=$1::uuid AND source_replan IS NOT NULL AND
    NOT EXISTS(SELECT 1 FROM planning_runs p WHERE p.objective_id=f.objective_id AND p.feedback->'feedbackIds' ? f.id::text)
  UNION ALL SELECT 1 FROM settings WHERE value->>'goalId'=$1 AND
   (id LIKE 'published-rebase:%' AND value->>'state' IN('proposed','preparing','prepared','applying','uncertain') OR
    id LIKE 'planning-source-preparation:%' AND value->>'state'<>'applied') LIMIT 1`,[o.id])).rowCount)
  throw fail('OBJECTIVE_MODEL_ROUTE_OWNER_BUSY');
 // Native executor rows can outlive a terminal logical run. They must be
 // reconciled by their owner, never treated as stopped by this API.
 for(const [table,terminal] of [['opencode_jobs','finished'],['supervisor_jobs','completed']] as const){
  if(!(await db.query('SELECT to_regclass($1) relation',['public.'+table])).rows[0].relation)continue;
  if((await db.query(`SELECT 1 FROM ${table} j JOIN runs r ON r.id=j.run_id JOIN tasks t ON t.id=r.task_id
   WHERE t.goal_id=$1 AND j.state IS DISTINCT FROM $2 LIMIT 1`,[o.id,terminal])).rowCount)
   throw fail('OBJECTIVE_MODEL_ROUTE_OWNER_BUSY');
 }
 if((await db.query("SELECT to_regclass('public.workspace_preparations') relation")).rows[0].relation&&
   (await db.query("SELECT 1 FROM workspace_preparations WHERE goal_id=$1 AND state IS DISTINCT FROM 'prepared' LIMIT 1",[o.id])).rowCount)
  throw fail('OBJECTIVE_MODEL_ROUTE_OWNER_BUSY');
 const tasks=(await db.query("SELECT id,execution->>'workspace' workspace FROM tasks WHERE goal_id=$1",[o.id])).rows;
 await assertPlanningSourcesIdle(db,{feedback:{replaceTaskIds:tasks.map(t=>t.id)}},
  [...o.request.workspaces,...tasks.map(t=>t.workspace).filter(Boolean)]);
}
export async function objectiveModelRoute(goalId:string){
 z.uuid().parse(goalId);
 return tx(async db=>{await lock(db);const o=await objective(db,goalId),p=await policy(db),route=await latest(db,goalId);
  if(route)await verify(db,route);
  return {goalId,expectedGoalId:goalId,expectedRevision:o.revision,expectedRequestDigest:o.original_digest,expectedPolicyDigest:hash(p),
   expectedGenerationId:route?.id??null,target:{plannerAccountId:p.accountId,plannerModel:p.model,allowedAccounts:[p.accountId]},
   currentReceipt:route?{generationId:route.id,generation:route.generation,receiptDigest:route.receipt_digest,
    idempotencyKey:route.idempotency_key}:null,
   activation:'new_planning_run',oldAttemptsChanged:false};
 });
}
export async function registerObjectiveModelRoute(goalId:string,raw:unknown,auth:Auth){
 z.uuid().parse(goalId);z.enum(['api-token','operator-session']).parse(auth?.channel);
 const request=objectiveModelRouteSchema.parse(raw);checkMemoryContent(request.reason);
 if(request.expectedGoalId!==goalId)throw fail('OBJECTIVE_MODEL_ROUTE_TARGET_CHANGED');
  return tx(async db=>{await lock(db);const o=await objective(db,goalId),current=await latest(db,goalId);
  const prior=(await db.query('SELECT * FROM objective_model_routes WHERE objective_id=$1 AND idempotency_key=$2',[goalId,request.idempotencyKey])).rows[0];
  if(prior){
   const saved=await verify(db,prior);
   if(!same(saved.request,request))throw fail('IDEMPOTENCY_CONFLICT');
   let stillCurrent=current?.id===prior.id&&o.revision===request.expectedRevision&&o.original_digest===request.expectedRequestDigest;
   try{stillCurrent=stillCurrent&&hash(await policy(db))===request.expectedPolicyDigest;await quiet(db,o);}catch{stillCurrent=false;}
   return {recorded:true,generationId:prior.id,generation:prior.generation,receiptDigest:prior.receipt_digest,duplicate:true,
    current:stillCurrent,activation:'none',authority:'historical_receipt',oldAttemptsChanged:false};
  }
  const p=await policy(db);await quiet(db,o);
  if(o.revision!==request.expectedRevision||o.original_digest!==request.expectedRequestDigest||hash(p)!==request.expectedPolicyDigest)
   throw fail('OBJECTIVE_MODEL_ROUTE_TARGET_CHANGED');
  if((current?.id??null)!==request.expectedGenerationId)throw fail('OBJECTIVE_MODEL_ROUTE_GENERATION_CHANGED');
  if(current){const prior=await verify(db,current);
   if(same(prior.policy,p)&&prior.originalRequestDigest===o.original_digest)throw fail('OBJECTIVE_MODEL_ROUTE_ALREADY_CURRENT');
  }
  const id=randomUUID(),generation=(current?.generation??0)+1;
  const effectiveRequest={...o.request,plannerAccountId:p.accountId,plannerModel:p.model,allowedAccounts:[p.accountId]};
  const payload:Payload={version:1,id,goalId,generation,request,requestDigest:hash(request),authChannel:auth.channel,
   originalRequest:o.request,originalRequestDigest:o.original_digest,policy:p,policyDigest:hash(p),
   effectiveRequest,effectiveRequestDigest:hash(effectiveRequest)};
  const receiptDigest=hash(payload);
  await db.query('INSERT INTO objective_model_routes(id,objective_id,generation,idempotency_key,payload,receipt_digest) VALUES($1,$2,$3,$4,$5,$6)',
   [id,goalId,generation,request.idempotencyKey,JSON.stringify(payload),receiptDigest]);
  await event(db,'OBJECTIVE_MODEL_ROUTE_REGISTERED',goalId,{generationId:id,generation,receiptDigest,authChannel:auth.channel,
   expectedRevision:o.revision,requestDigest:o.original_digest,policyDigest:hash(p),oldAttemptsChanged:false,acceptance:false});
  return {recorded:true,generationId:id,generation,receiptDigest,duplicate:false,current:true,activation:'new_planning_run',oldAttemptsChanged:false};
 });
}
/** Only the INSERT-bound generation authorizes a new planning attempt. A retry
 * of an older run can never acquire a newer route by reading current settings. */
export async function routedPlanningRequest(db:DB,run:any):Promise<ObjectiveInput>{
 const o=await objective(db,run.objective_id),current=await latest(db,run.objective_id);
 if((run.route_generation_id??null)!==(current?.id??null))throw fail('OBJECTIVE_MODEL_ROUTE_RUN_STALE');
 if(!current)return effectivePlanningRequest(o.request,run.feedback);
 const p=await verify(db,current),activePolicy=await policy(db);
 if(p.originalRequestDigest!==o.original_digest||!same(p.originalRequest,o.request)||!same(p.policy,activePolicy)||
   o.revision< p.request.expectedRevision)throw fail('OBJECTIVE_MODEL_ROUTE_TARGET_CHANGED');
 return effectivePlanningRequest(p.effectiveRequest,run.feedback);
}
export async function currentObjectiveRouteGeneration(db:DB,goalId:string){
 const row=await latest(db,goalId);if(!row)return null;
 await routedPlanningRequest(db,{objective_id:goalId,route_generation_id:row.id,feedback:{}});
 return {id:row.id,receiptDigest:row.receipt_digest};
}
