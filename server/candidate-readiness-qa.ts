import {createHash} from 'node:crypto';
import {event,type DB} from './db.js';
import {DomainError} from './contracts.js';
import {candidateCiCriterionSchema} from './candidate-readiness-qa-contract.js';
import {readCandidateReadinessProof,assertCandidateReadinessTaskPlan} from './candidate-readiness.js';
import {validationDigest,verifiedWorkerValidationGate,workerValidationCommitIdentity} from './worker-validation.js';
import {readRuntimeCandidateValidationEvidence} from './candidate-validation-evidence.js';
import {readDeliverySourceIdentity,sourceIdentityAccepted,sourceIdentityFrozen} from './delivery-source-resolution.js';
import {objectiveReviewControllerRelease,assertObjectiveReviewConsumerReady} from './objective-review-readiness.js';
import {runtimeCandidateDigest} from './operation-progress.js';
import type {WorkerRelease} from './worker-rollout.js';

function fail(code:string):never{throw new DomainError(code,code,409);}
const same=(a:any,b:any)=>validationDigest(a)===validationDigest(b);
const sha=(bytes:Buffer)=>createHash('sha256').update(bytes).digest('hex');
function criterion(task:any){
  const found=(task.criteria??[]).filter((c:any)=>c.type==='candidate_ci');
  if(!found.length)return null;
  const parsed=candidateCiCriterionSchema.safeParse(found[0]);
  if(found.length!==1||!parsed.success||task.stage!=='verify'||task.execution?.kind!=='harness'||
    task.execution.readOnlySource!==true||!Number.isSafeInteger(task.plan_revision)||task.plan_revision<1)fail('READINESS_QA_ADMISSION_UNBOUND');
  return parsed.data;
}
async function canonicalTask(db:DB,id:string){
  return (await db.query(`SELECT id,goal_id,key,plan_revision,stage,covers,criteria,attempt_count,instruction_revision_id,
    jsonb_build_object('kind',execution->'kind','workspace',execution->'workspace','sourceWorkspace',execution->'sourceWorkspace',
      'readOnlySource',execution->'readOnlySource') execution FROM tasks WHERE id=$1`,[id])).rows[0];
}
async function source(db:DB,task:any){
  const c=criterion(task);if(!c)return null;
  if(!(await db.query(`SELECT 1 FROM events WHERE kind='CANDIDATE_READINESS_PLAN_BOUND' AND entity_id=$1
    AND payload->>'registrationId'=$2 AND payload->'qaCriteria' @> $3::jsonb LIMIT 1`,
    [task.goal_id,c.registrationId,JSON.stringify([c])])).rowCount)fail('READINESS_QA_PLAN_UNBOUND');
  const candidates=(await db.query(`SELECT s.id,s.goal_id,s.plan_revision,s.execution->>'workspace' workspace,
    r.id run_id,r.fence,r.context_id,r.attempt,r.instruction_snapshot->'candidateReadiness' readiness
    FROM dependencies d JOIN tasks s ON s.id=d.depends_on
    JOIN plan_revisions p ON p.objective_id=s.goal_id AND p.revision=s.plan_revision AND p.status='applied'
    JOIN LATERAL(SELECT * FROM runs WHERE task_id=s.id ORDER BY attempt DESC LIMIT 1)r ON true
    WHERE d.task_id=$1 AND s.goal_id=$2 AND s.plan_revision=$3 AND s.stage='integrate'
      AND s.key='r'||p.revision::text||'-'||$4 AND s.execution->>'workspace'=$5`,
    [task.id,task.goal_id,task.plan_revision,c.sourceTaskKey,task.execution.sourceWorkspace])).rows;
  if(candidates.length!==1)fail('READINESS_QA_SOURCE_UNBOUND');
  const producer=candidates[0],identity=await readDeliverySourceIdentity(db,producer.id);
  if(!sourceIdentityAccepted(identity)||!sourceIdentityFrozen(identity)||identity.run_id!==producer.run_id)
    fail('READINESS_QA_SOURCE_CHANGED');
  const registration=producer.readiness?.registration;
  if(!registration||registration.registrationId!==c.registrationId||registration.contractDigest!==c.mapDigest||
    registration.policyRegistrationId!==c.policyRegistrationId||registration.policyDigest!==c.policyDigest)
    fail('READINESS_QA_MAP_UNBOUND');
  const policy=(await db.query('SELECT value FROM settings WHERE id=$1',['worker-validation:'+task.goal_id])).rows[0]?.value;
  if(policy?.registrationId!==c.policyRegistrationId||policy.policyDigest!==c.policyDigest||policy.policy?.stages?.includes('verify'))
    fail('READINESS_QA_POLICY_CONFLICT');
  const readiness=await readCandidateReadinessProof(db,producer.run_id);
  if(!readiness||readiness.mapDigest!==c.mapDigest)fail('READINESS_QA_MAP_UNBOUND');
  const gate=await verifiedWorkerValidationGate(db,producer.run_id);
  if(!gate.gate.required||gate.gate.state!=='passed'||!gate.gate.actionId)fail('READINESS_QA_CI_REQUIRED');
  const body=await readRuntimeCandidateValidationEvidence(db,gate.gate.actionId);
  const authority=body?.authority;
  if(!authority||authority.outcome.passed!==true||authority.producer.runId!==producer.run_id||
    authority.producer.taskId!==producer.id||authority.producer.runFence!==producer.fence||
    authority.coverage?.obligationsDigest!==c.obligationsDigest)fail('READINESS_QA_CI_UNBOUND');
  const refs=(await db.query("SELECT id,run_id,name,digest,size FROM artifacts WHERE run_id=$1 AND name='candidate.json' ORDER BY id LIMIT 2",[producer.run_id])).rows;
  if(refs.length!==1)fail('READINESS_QA_SOURCE_UNBOUND');
  const {readArtifact}=await import('./service.js'),candidate=await readArtifact(refs[0].id);
  const candidateDigest=runtimeCandidateDigest(candidate.bytes,refs[0].digest);
  if(candidate.digest!==refs[0].digest||candidateDigest!==authority.producer.candidateDigest)fail('READINESS_QA_SOURCE_CHANGED');
  const local=registration.contract.obligations.filter((o:any)=>o.kind==='local');
  if(!same([...new Set(local.map((o:any)=>o.name))].sort(),[...c.localArtifacts].sort())||
    !same([...task.covers].sort(),registration.contract.requirements.map((r:any)=>r.id).sort()))fail('READINESS_QA_MAP_UNBOUND');
  return {version:1,criterion:c,source:{taskId:producer.id,runId:producer.run_id,runFence:producer.fence,
    workspace:producer.workspace,contractDigest:identity.contract_digest,candidateDigest,candidate:refs[0]},
    ci:{actionId:gate.gate.actionId,receiptDigest:body!.receiptDigest,authorityDigest:validationDigest(authority),
      coverage:authority.coverage},registrationId:c.registrationId,mapDigest:c.mapDigest,
    policyRegistrationId:c.policyRegistrationId,policyDigest:c.policyDigest,localObligations:local,acceptance:false};
}
export async function verifyCandidateReadinessQaAdmission(db:DB,task:any,options:{consumerRelease?:WorkerRelease}={}){
  const current=await canonicalTask(db,task.id);if(!current||!criterion(current))return null;
  const proof=await source(db,current);
  if(!proof)fail('READINESS_QA_SOURCE_UNBOUND');
  try{await assertObjectiveReviewConsumerReady(db,options.consumerRelease??await objectiveReviewControllerRelease());}
  catch{fail('READINESS_CONSUMER_NOT_READY');}
  return proof;
}
export async function recordCandidateReadinessQaAdmission(db:DB,task:any,runId:string,fence:string,options:{consumerRelease?:WorkerRelease}={}){
  const current=await canonicalTask(db,task.id);if(!current)return null;
  await assertCandidateReadinessTaskPlan(db,current);
  if(!criterion(current))return null;
  const proof=await verifyCandidateReadinessQaAdmission(db,current,options);
  const transactionId=(await db.query('SELECT pg_current_xact_id()::text id')).rows[0].id;
  await event(db,'CANDIDATE_CI_ADMISSION_AUTHORIZED',current.id,
    {version:1,runId,fence,attempt:current.attempt_count+1,transactionId,proof});
  return proof;
}
export async function prepareCandidateReadinessQaContext(db:DB,run:any,task:any){
  const current=await canonicalTask(db,task.id);
  if(!current||!criterion(current)){
    if(run.instruction_snapshot?.candidateCiAdmission)fail('READINESS_QA_ADMISSION_UNBOUND');
    return null;
  }
  const admitted=run.instruction_snapshot?.candidateCiAdmission;
  const proof=await source(db,current);
  if(!proof)fail('READINESS_QA_SOURCE_UNBOUND');
  if(!admitted||!same(proof,admitted)||!same(current.criteria,run.instruction_snapshot?.task?.criteria))fail('READINESS_QA_ADMISSION_UNBOUND');
  const manifest={...proof,consumer:{taskId:current.id,runId:run.id,runFence:run.fence}};
  const text='CANONICAL CI FOR THE EXACT ACCEPTED READ-ONLY SOURCE: '+JSON.stringify(manifest)+
    '\nThe controller checked the authenticated CI receipt and frozen producer. Verify its coverage; do not claim you ran its database tests. '+
    'Execute the local obligations independently, publish their real evidence and verification.json. Any candidate source change invalidates this inherited proof.';
  if(Buffer.byteLength(text)>128*1024)fail('READINESS_QA_CONTEXT_TOO_LARGE');
  return {manifest,text};
}
export async function readCandidateReadinessQaProof(db:DB,runId:string){
  const row=(await db.query(`SELECT r.id,r.task_id,r.fence,r.context_id,r.attempt,
    jsonb_build_object('candidateCiAdmission',r.instruction_snapshot->'candidateCiAdmission',
      'task',jsonb_build_object('criteria',r.instruction_snapshot#>'{task,criteria}')) instruction_snapshot,
    c.manifest->'candidateReadinessQa' qa_manifest FROM runs r LEFT JOIN contexts c ON c.id=r.context_id WHERE r.id=$1`,[runId])).rows[0];
  if(!row)return null;
  const task=await canonicalTask(db,row.task_id),c=task&&criterion(task);
  if(!c&&!row.instruction_snapshot?.candidateCiAdmission)return null;
  if(!c||row.attempt!==task.attempt_count||!row.context_id||!same(task.criteria,row.instruction_snapshot?.task?.criteria))fail('READINESS_QA_ADMISSION_UNBOUND');
  const identity=await readDeliverySourceIdentity(db,task.id);
  if(!sourceIdentityFrozen(identity)||identity.run_id!==runId||identity.fence!==row.fence)fail('READINESS_QA_SOURCE_CHANGED');
  const expected=await prepareCandidateReadinessQaContext(db,row,task);
  if(!same(expected?.manifest,row.qa_manifest))fail('READINESS_QA_CONTEXT_CHANGED');
  const {readArtifact}=await import('./service.js');
  const refs=(await db.query('SELECT id,run_id,name,digest,size FROM artifacts WHERE run_id=$1 ORDER BY id',[runId])).rows;
  const load=async(name:string)=>{
    const found=refs.filter((a:any)=>a.name===name);if(found.length!==1)fail('READINESS_QA_ARTIFACT_INVALID');
    const a=await readArtifact(found[0].id);
    if(a.run_id!==runId||sha(a.bytes)!==found[0].digest||a.bytes.length!==found[0].size)fail('READINESS_QA_ARTIFACT_INVALID');
    return {ref:found[0],bytes:a.bytes};
  };
  const candidate=await load('candidate.json');
  if(runtimeCandidateDigest(candidate.bytes,candidate.ref.digest)!==expected!.manifest.source.candidateDigest)
    fail('READINESS_QA_CANDIDATE_CHANGED');
  const localEvidence=[];
  for(const o of expected!.manifest.localObligations){
    const a=await load(o.name);let body:any;
    try{body=JSON.parse(a.bytes.toString());}catch{fail('READINESS_QA_LOCAL_CHECK_FAILED');}
    if(!body||!Object.hasOwn(body,o.field)||body[o.field]!==o.equals)fail('READINESS_QA_LOCAL_CHECK_FAILED');
    localEvidence.push(a.ref);
  }
  return {version:1,manifest:expected!.manifest,candidate:candidate.ref,localEvidence,acceptance:false};
}
/** Small transactional projection used after byte reads; no bodies or prompts. */
export async function candidateReadinessQaCommitIdentity(db:DB,runId:string){
  const value=(await db.query(`SELECT r.id,r.task_id,r.fence,r.context_id,r.attempt,
    encode(sha256(convert_to((r.instruction_snapshot->'candidateCiAdmission')::text,'UTF8')),'hex') admitted,
    encode(sha256(convert_to((c.manifest->'candidateReadinessQa')::text,'UTF8')),'hex') context,
    c.manifest#>>'{candidateReadinessQa,source,taskId}' source_task_id,
    c.manifest#>>'{candidateReadinessQa,source,runId}' source_run_id,
    encode(sha256(convert_to(m.value::text,'UTF8')),'hex') map,
    encode(sha256(convert_to(p.value::text,'UTF8')),'hex') policy,
    EXISTS(SELECT 1 FROM events e WHERE e.kind='CANDIDATE_READINESS_MAP_REGISTERED' AND e.entity_id=t.goal_id::text AND e.payload=m.value) map_registered,
    encode(sha256(convert_to(jsonb_build_object('planRevision',t.plan_revision,'stage',t.stage,'covers',t.covers,'criteria',t.criteria,
      'kind',t.execution->'kind','workspace',t.execution->'workspace','sourceWorkspace',t.execution->'sourceWorkspace',
      'readOnlySource',t.execution->'readOnlySource')::text,'UTF8')),'hex') task_contract,
    (SELECT jsonb_agg(d.depends_on ORDER BY d.depends_on) FROM dependencies d WHERE d.task_id=t.id) dependencies,
    EXISTS(SELECT 1 FROM events e WHERE e.kind='CANDIDATE_READINESS_PLAN_BOUND' AND e.entity_id=t.goal_id::text
      AND e.payload->>'registrationId'=r.instruction_snapshot#>>'{candidateCiAdmission,registrationId}'
      AND e.payload->'qaCriteria' @> jsonb_build_array(r.instruction_snapshot#>'{candidateCiAdmission,criterion}')) plan_bound,
    (SELECT jsonb_agg(jsonb_build_object('id',a.id,'digest',a.digest,'size',a.size) ORDER BY a.id) FROM artifacts a WHERE a.run_id=r.id) artifacts
    FROM runs r JOIN tasks t ON t.id=r.task_id LEFT JOIN contexts c ON c.id=r.context_id
    LEFT JOIN settings m ON m.id='candidate-readiness:'||t.goal_id
    LEFT JOIN settings p ON p.id='worker-validation:'||t.goal_id WHERE r.id=$1`,[runId])).rows[0]??null;
  if(!value)return null;
  const uuid=/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
  return {...value,consumer:await readDeliverySourceIdentity(db,value.task_id),
    source:uuid.test(value.source_task_id??'')?await readDeliverySourceIdentity(db,value.source_task_id):null,
    sourceCi:uuid.test(value.source_run_id??'')?await workerValidationCommitIdentity(db,value.source_run_id):null};
}
