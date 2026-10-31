import {randomUUID} from 'node:crypto';
import {z} from 'zod';
import {pool,tx,lock,event,type DB} from './db.js';
import {DomainError} from './contracts.js';
import {candidateReadinessMapSchema,candidateReadinessCriterionSchema,candidateReadinessReportSchema,
  type CandidateReadinessMap} from './candidate-readiness-contract.js';
import {validationDigest,workerValidationPolicySchema,readWorkerValidationIdentity,
  verifyWorkerValidationRegistration} from './worker-validation.js';
import {frameworkProjectAuthorization} from './candidate-validation.js';
import {objectiveReviewControllerRelease,assertObjectiveReviewConsumerReady} from './objective-review-readiness.js';
import type {WorkerRelease} from './worker-rollout.js';
import type {ProposedPlan} from './objective-contracts.js';
import {bindCandidateReadinessQaPlan} from './candidate-readiness-qa-contract.js';

const failure=(code:string)=>new DomainError(code,code,409);
function require(ok:unknown,code:string):asserts ok {if(!ok)throw failure(code);}
const same=(a:any,b:any)=>validationDigest(a)===validationDigest(b);
const sorted=(items:any[])=>items.map(validationDigest).sort();
const mapId=(goalId:string)=>'candidate-readiness:'+goalId;
export const candidateReadinessRegistrationSchema=z.object({version:z.literal(1),id:z.string(),registrationId:z.uuid(),
  goalId:z.uuid(),contract:candidateReadinessMapSchema,contractDigest:z.string(),policyRegistrationId:z.uuid(),
  policyDigest:z.string(),requiredDatabaseTests:z.array(z.object({file:z.string(),name:z.string()}).strict()),
  channel:z.enum(['api-token','operator-session'])}).strict();
type Registration=z.infer<typeof candidateReadinessRegistrationSchema>;

async function originalRequirements(db:DB,goalId:string){
  const plan=(await db.query('SELECT plan FROM plan_revisions WHERE objective_id=$1 ORDER BY revision DESC LIMIT 1',[goalId])).rows[0]?.plan;
  if(plan?.requirements)return plan.requirements.map((r:any)=>({id:r.id,digest:validationDigest(r.description)}));
  const request=(await db.query('SELECT request FROM objectives WHERE id=$1',[goalId])).rows[0]?.request;
  require(Array.isArray(request?.requirements)&&request.requirements.length,'READINESS_ORIGINAL_REQUIREMENTS_MISSING');
  return request.requirements.map((description:string,i:number)=>({id:'req-'+(i+1),digest:validationDigest(description)}));
}
async function policy(db:DB,goalId:string){
  const value=(await db.query('SELECT value FROM settings WHERE id=$1',['worker-validation:'+goalId])).rows[0]?.value;
  require(value?.version===1&&value.goalId===goalId&&value.id==='worker-validation:'+goalId&&
    ['api-token','operator-session'].includes(value.channel),'READINESS_POLICY_UNBOUND');
  const p=workerValidationPolicySchema.safeParse(value.policy);
  require(p.success&&p.data.enabled&&p.data.profile==='agentos.validate'&&p.data.project==='agentos'&&
    p.data.stages.includes('integrate')&&value.policyDigest===validationDigest(p.data),'READINESS_POLICY_UNBOUND');
  require((await db.query("SELECT 1 FROM events WHERE kind='WORKER_VALIDATION_POLICY_REGISTERED' AND entity_id=$1 AND payload=$2::jsonb",
    [goalId,JSON.stringify(value)])).rowCount,'READINESS_POLICY_UNBOUND');
  return value;
}
export function validateCandidateReadinessPartition(map:CandidateReadinessMap,required:any[]){
  const req=map.requirements.map(x=>x.id),ids=map.obligations.map(x=>x.id);
  require(new Set(req).size===req.length&&new Set(ids).size===ids.length,'READINESS_OBLIGATIONS_INVALID');
  require(map.obligations.every(o=>new Set(o.requirements).size===o.requirements.length&&o.requirements.every(id=>req.includes(id)))&&
    req.every(id=>map.obligations.some(o=>o.requirements.includes(id))),'READINESS_OBLIGATIONS_UNMAPPED');
  const local=map.obligations.filter(o=>o.kind==='local');
  const deferred=map.obligations.filter(o=>o.kind==='registered_ci').map(o=>({file:o.file,name:o.name}));
  require(local.length>0&&new Set(local.map(x=>x.name)).size<=20,'READINESS_LOCAL_OBLIGATIONS_REQUIRED');
  require(new Set(sorted(deferred)).size===deferred.length&&same(sorted(deferred),sorted(required)),
    'READINESS_CI_OBLIGATIONS_UNBOUND');
}
export function readinessCriterion(reg:Registration){
  return candidateReadinessCriterionSchema.parse({id:'integrated-readiness',label:'Candidata integrada pronta; aceite depende de CI canônica',
    type:'candidate_readiness',version:1,name:'integration-readiness.json',registrationId:reg.registrationId,
    mapDigest:reg.contractDigest,policyRegistrationId:reg.policyRegistrationId,policyDigest:reg.policyDigest,
    obligationsDigest:validationDigest(reg.requiredDatabaseTests),localArtifacts:[...new Set(reg.contract.obligations.filter(o=>o.kind==='local').map(o=>o.name))]});
}
async function registration(db:DB,goalId:string){
  const raw=(await db.query('SELECT value FROM settings WHERE id=$1',[mapId(goalId)])).rows[0]?.value;
  const parsed=candidateReadinessRegistrationSchema.safeParse(raw);
  require(parsed.success,'READINESS_MAP_UNREGISTERED');const reg=parsed.data!;
  require(reg.id===mapId(goalId)&&reg.goalId===goalId&&reg.contract.enabled&&
    reg.contractDigest===validationDigest(reg.contract),'READINESS_MAP_UNBOUND');
  require((await db.query("SELECT 1 FROM events WHERE kind='CANDIDATE_READINESS_MAP_REGISTERED' AND entity_id=$1 AND payload=$2::jsonb",
    [goalId,JSON.stringify(reg)])).rowCount,'READINESS_MAP_UNREGISTERED');
  const p=await policy(db,goalId);
  require(reg.policyRegistrationId===p.registrationId&&reg.contract.policyRegistrationId===p.registrationId&&
    reg.policyDigest===p.policyDigest&&same(reg.requiredDatabaseTests,p.policy.requiredDatabaseTests),'READINESS_POLICY_CHANGED');
  validateCandidateReadinessPartition(reg.contract,p.policy.requiredDatabaseTests);
  require(same(sorted(reg.contract.requirements),sorted(await originalRequirements(db,goalId))),'READINESS_REQUIREMENTS_CHANGED');
  return reg;
}
/** Future plans only. The map does not amend any already admitted task/attempt. */
export async function registerCandidateReadinessMap(goalId:string,raw:unknown,auth:{channel:'api-token'|'operator-session'}){
  z.uuid().parse(goalId);const contract=candidateReadinessMapSchema.parse(raw);
  const channel=z.enum(['api-token','operator-session']).parse(auth.channel);
  return tx(async db=>{
    await lock(db);const framework=await frameworkProjectAuthorization(db,goalId);
    require(framework.project==='agentos','READINESS_PROFILE_UNSUPPORTED');
    const p=await policy(db,goalId);
    require(contract.policyRegistrationId===p.registrationId,'READINESS_POLICY_CHANGED');
    validateCandidateReadinessPartition(contract,p.policy.requiredDatabaseTests);
    require(same(sorted(contract.requirements),sorted(await originalRequirements(db,goalId))),'READINESS_REQUIREMENTS_CHANGED');
    const reg:Registration={version:1,id:mapId(goalId),registrationId:randomUUID(),goalId,contract,
      contractDigest:validationDigest(contract),policyRegistrationId:p.registrationId,policyDigest:p.policyDigest,
      requiredDatabaseTests:p.policy.requiredDatabaseTests,channel};
    await event(db,'CANDIDATE_READINESS_MAP_REGISTERED',goalId,reg);
    await db.query('INSERT INTO settings(id,value) VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET value=excluded.value',[reg.id,JSON.stringify(reg)]);
    return reg;
  });
}
async function consumer(db:DB,options:{consumerRelease?:WorkerRelease}={}){
  try{const release=options.consumerRelease??await objectiveReviewControllerRelease();await assertObjectiveReviewConsumerReady(db,release);return release;}
  catch{throw failure('READINESS_CONSUMER_NOT_READY');}
}
/** Called on controller-compiled incremental plans, before source preparation.
 * Text is presentation; only the registered finite map defines the controls. */
export async function bindCandidateReadinessPlan(db:DB,goalId:string,plan:ProposedPlan,options:{consumerRelease?:WorkerRelease}={}){
  require(!plan.tasks.some(t=>t.criteria.some(c=>c.type==='candidate_readiness'||c.type==='candidate_ci')),'READINESS_PREDECLARED');
  const configured=(await db.query('SELECT value FROM settings WHERE id=$1',[mapId(goalId)])).rows[0]?.value;
  if(!configured?.contract?.enabled)return plan;
  const reg=await registration(db,goalId);await consumer(db,options);
  const integrations=plan.tasks.filter(t=>t.stage==='integrate');
  require(!!plan.incremental&&integrations.length===1,'READINESS_SCOPE_UNMAPPED');
  const original=integrations[0];
  require(same([...original.covers].sort(),reg.contract.requirements.map(r=>r.id).sort())&&
    same(sorted(plan.requirements.map(r=>({id:r.id,digest:validationDigest(r.description)}))),sorted(reg.contract.requirements)),
    'READINESS_OBLIGATIONS_UNMAPPED');
  require(original.criteria.length===1&&original.criteria[0].type==='json_field'&&original.criteria[0].name==='delivery.json'&&
    original.criteria[0].field==='passed'&&original.criteria[0].equals===true,'READINESS_ORIGINAL_CRITERIA_UNMAPPED');
  const result=structuredClone(plan),task=result.tasks.find(t=>t.key===original.key)!;
  task.criteria=[readinessCriterion(reg)];
  const prior=task.description;
  task.description='Integrate the canonical accepted patches and preserve all prior deliveries, source contracts and requirement scope. '+
    'The original requirement descriptions below remain the product obligation. Validation controls are the finite authenticated map, '+
    'not freeform verification prose. Complete its local controls, report readiness honestly, and finish so the controller runs its database controls. '+
    'Do not create databases, patch tools or claim database tests executed locally. Acceptance and independent QA remain pending.\n'+
    'Requirements: '+JSON.stringify(result.requirements)+'\nRegistered controls: '+JSON.stringify(reg.contract.obligations)+
    '\nIntegration dependencies: '+JSON.stringify(task.dependencies)+'\nOriginal integration description (historical context, superseded only for CI handoff wording): '+
    prior.replace(/Execute os controles combinados:[\s\S]*$/,'Combined validation obligations are prescribed by the registered map above.');
  require(task.description.length<=20000,'READINESS_CONTEXT_TOO_LARGE');
  const withQa=bindCandidateReadinessQaPlan(result,{...reg,policyStages:(await policy(db,goalId)).policy.stages});
  await event(db,'CANDIDATE_READINESS_PLAN_BOUND',goalId,{version:1,registrationId:reg.registrationId,
    criterion:task.criteria[0],qaCriteria:withQa.tasks.flatMap(t=>t.criteria.filter(c=>c.type==='candidate_ci')),
    covers:task.covers,requirements:reg.contract.requirements,acceptance:false});
  return withQa;
}
async function taskAuthority(db:DB,task:any){
  const items=(task.criteria??[]).filter((c:any)=>c.type==='candidate_readiness');
  if(!items.length)return null;
  require(items.length===1&&task.stage==='integrate'&&task.execution?.kind==='harness','READINESS_ADMISSION_UNBOUND');
  const reg=await registration(db,task.goal_id),criterion=readinessCriterion(reg);
  require(same(items[0],criterion)&&same([...task.covers].sort(),reg.contract.requirements.map(r=>r.id).sort()),'READINESS_ADMISSION_UNBOUND');
  require((await db.query("SELECT 1 FROM events WHERE kind='CANDIDATE_READINESS_PLAN_BOUND' AND entity_id=$1 AND payload->'criterion'=$2::jsonb AND payload->>'registrationId'=$3",
    [task.goal_id,JSON.stringify(criterion),reg.registrationId])).rowCount,'READINESS_PLAN_UNBOUND');
  return {registration:reg,criterion};
}
export async function verifyCandidateReadinessAdmission(db:DB,task:any,options:{consumerRelease?:WorkerRelease}={}){
  const proof=await taskAuthority(db,task);if(!proof)return null;
  await consumer(db,options);return proof;
}
/** The applied plan distinguishes a future task from a legacy task before its
 * first run exists. Removing the marker from the mutable task cannot downgrade
 * that planned contract. The SQL trigger repeats this check for old consumers. */
export async function assertCandidateReadinessTaskPlan(db:DB,task:any){
  const planned=(await db.query(`SELECT item.value FROM plan_revisions p
    CROSS JOIN LATERAL jsonb_array_elements(p.plan->'tasks') item(value)
    WHERE p.objective_id=$1 AND p.revision=$2 AND p.status='applied'
      AND 'r'||p.revision::text||'-'||(item.value->>'key')=$3`,[task.goal_id,task.plan_revision??null,task.key])).rows[0]?.value;
  const future=(items:any[])=>items?.some(c=>['candidate_readiness','candidate_ci'].includes(c.type));
  if(!future(task.criteria)&&!future(planned?.criteria))return;
  require(planned&&same(task.criteria,planned.criteria)&&same(task.covers,planned.covers)&&task.stage===planned.stage,'READINESS_PLAN_TASK_UNBOUND');
}
/** The SQL admission trigger accepts only the exact run/fence checked by this
 * consumer in the same transaction. An older worker cannot reuse a plan event. */
export async function recordCandidateReadinessAdmission(db:DB,task:any,runId:string,fence:string,options:{consumerRelease?:WorkerRelease}={}){
  // Scheduler candidates deliberately omit criteria and project execution. Read
  // only authority fields from the canonical row while holding admission lock.
  const current=(await db.query(`SELECT id,goal_id,key,plan_revision,stage,covers,criteria,attempt_count,
    jsonb_build_object('kind',execution->'kind') execution FROM tasks WHERE id=$1 FOR UPDATE`,[task.id])).rows[0];
  require(current&&current.attempt_count===task.attempt_count,'READINESS_ADMISSION_UNBOUND');
  await assertCandidateReadinessTaskPlan(db,current);
  const proof=await verifyCandidateReadinessAdmission(db,current,options);if(!proof)return null;
  const transactionId=(await db.query('SELECT pg_current_xact_id()::text id')).rows[0].id;
  await event(db,'CANDIDATE_READINESS_ADMISSION_AUTHORIZED',task.id,
    {version:1,runId,fence,attempt:task.attempt_count+1,transactionId,proof});
  return proof;
}
export async function verifyBoundCandidateReadinessPlan(db:DB,goalId:string,plan:ProposedPlan){
  for(const task of plan.tasks)if(task.criteria.some(c=>c.type==='candidate_readiness'))
    await verifyCandidateReadinessAdmission(db,{...task,goal_id:goalId,execution:{kind:'harness'}});
  for(const task of plan.tasks)for(const criterion of task.criteria)if(criterion.type==='candidate_ci'){
    const reg=await registration(db,goalId);await consumer(db);
    require(criterion.registrationId===reg.registrationId&&criterion.mapDigest===reg.contractDigest&&
      !!(await db.query("SELECT 1 FROM events WHERE kind='CANDIDATE_READINESS_PLAN_BOUND' AND entity_id=$1 AND payload->>'registrationId'=$2 AND payload->'qaCriteria' @> $3::jsonb",
        [goalId,reg.registrationId,JSON.stringify([criterion])])).rowCount,'READINESS_QA_PLAN_UNBOUND');
  }
}
/** No semantic reinterpretation for legacy reports. Readiness is a separate
 * artifact and is never itself canonical CI evidence. */
export async function readCandidateReadinessProof(db:DB,runId:string){
  const row=(await db.query(`SELECT r.id,r.fence,r.context_id,r.attempt,r.state,
    r.instruction_snapshot->'candidateReadiness' admitted_readiness,
    r.instruction_snapshot->>'workerValidationContract' contract_version,
    r.instruction_snapshot#>'{task,criteria}' admitted_criteria,
    t.id task_id,t.goal_id,t.stage,t.criteria,t.covers,jsonb_build_object('kind',t.execution->'kind') execution,t.attempt_count
    FROM runs r JOIN tasks t ON t.id=r.task_id WHERE r.id=$1`,[runId])).rows[0];
  const present=(row?.criteria??[]).some((c:any)=>c.type==='candidate_readiness')||
    (row?.admitted_criteria??[]).some((c:any)=>c.type==='candidate_readiness')||row?.contract_version==='2';
  if(!present)return null;
  require(row.contract_version==='2'&&row.admitted_readiness&&row.context_id&&row.attempt===row.attempt_count&&
    same(row.criteria,row.admitted_criteria),'READINESS_ADMISSION_UNBOUND');
  const authority=await taskAuthority(db,{...row,id:row.task_id});
  require(authority&&same(row.admitted_readiness,authority),'READINESS_ADMISSION_UNBOUND');
  const identity=await readWorkerValidationIdentity(db,runId);
  require(identity?.worker_validation,'READINESS_ADMISSION_UNBOUND');
  const p=await verifyWorkerValidationRegistration(db,identity);
  require(p.registrationId===authority.registration.policyRegistrationId,'READINESS_POLICY_CHANGED');
  const {readArtifact}=await import('./service.js');
  const refs=(await db.query('SELECT id,run_id,name,digest,size FROM artifacts WHERE run_id=$1 ORDER BY name,id',[runId])).rows;
  const load=async(name:string)=>{
    const found=refs.filter((a:any)=>a.name===name);require(found.length===1,'READINESS_ARTIFACT_IDENTITY');
    const a=await readArtifact(found[0].id);
    require(a.run_id===runId&&a.digest===found[0].digest&&a.bytes.length===found[0].size,'READINESS_ARTIFACT_IDENTITY');
    return {ref:found[0],body:JSON.parse(a.bytes.toString())};
  };
  const artifact=await load('integration-readiness.json'),parsed=candidateReadinessReportSchema.safeParse(artifact.body);
  require(parsed.success,'READINESS_REPORT_INVALID');const report=parsed.data!;
  const local=authority.registration.contract.obligations.filter(o=>o.kind==='local');
  const required=p.policy.requiredDatabaseTests;
  require(report.controllerChecks.obligationsDigest===validationDigest(required)&&
    same(sorted(report.controllerChecks.tests),sorted(required))&&new Set(sorted(report.controllerChecks.tests)).size===required.length,
    'READINESS_CI_OBLIGATIONS_UNBOUND');
  require(report.localChecks.length===local.length&&new Set(report.localChecks.map(c=>c.id)).size===local.length,'READINESS_LOCAL_OBLIGATIONS_UNBOUND');
  const localEvidence=[];
  for(const obligation of local){
    const check=report.localChecks.find(c=>c.id===obligation.id);
    require(check?.name===obligation.name,'READINESS_LOCAL_OBLIGATIONS_UNBOUND');
    const item=await load(obligation.name);
    require(check.digest===item.ref.digest&&Object.hasOwn(item.body,obligation.field)&&
      item.body[obligation.field]===obligation.equals,'READINESS_LOCAL_CHECK_FAILED');
    localEvidence.push(item.ref);
  }
  return {version:1,runId,runFence:row.fence,contextVersion:row.context_id,attempt:row.attempt,
    registrationId:authority.registration.registrationId,mapDigest:authority.registration.contractDigest,
    criterionDigest:validationDigest(authority.criterion),policyDigest:p.policyDigest,
    readiness:artifact.ref,localEvidence,acceptance:false};
}
