import {createHash, randomUUID} from 'node:crypto';
import {z} from 'zod';
import {event, lock, pool, tx, type DB} from './db.js';
import {DomainError} from './contracts.js';
import {readArtifact} from './service.js';
import {frameworkProjectAuthorization} from './candidate-validation.js';
import {readDeliverySourceIdentity} from './delivery-source-resolution.js';

export const canonicalValidationValue = (v:any):any => Array.isArray(v) ? v.map(canonicalValidationValue)
  : v && typeof v==='object' ? Object.fromEntries(Object.keys(v).sort().map(k=>[k,canonicalValidationValue(v[k])])) : v;
export const validationDigest = (v:any) => createHash('sha256').update(JSON.stringify(canonicalValidationValue(v))).digest('hex');
const sha = (v:Buffer) => createHash('sha256').update(v).digest('hex');
export const requiredDatabaseTestSchema=z.object({file:z.string().max(300).regex(/^tests\/(?:[a-zA-Z0-9_-]+\/)*[a-zA-Z0-9_-]+(?:\.[a-zA-Z0-9_-]+)*\.test\.[cm]?[jt]s$/),
  name:z.string().min(1).max(500).refine(value=>!value.includes('\0'))}).strict();
export const workerValidationPolicySchema=z.object({version:z.literal(1),enabled:z.boolean(),
  project:z.string().regex(/^[a-z0-9_-]+$/),profile:z.literal('agentos.validate'),
  stages:z.array(z.enum(['work','integrate','verify'])).min(1).max(3),required:z.literal(true),
  requiredDatabaseTests:z.array(requiredDatabaseTestSchema).min(1).max(128)}).strict();
const registrationSchema=z.object({version:z.literal(1),id:z.string(),registrationId:z.uuid(),goalId:z.uuid(),
  policy:workerValidationPolicySchema,policyDigest:z.string().regex(/^[a-f0-9]{64}$/),
  channel:z.enum(['api-token','operator-session'])}).strict();
export const validationIntentSchema=z.object({version:z.literal(1),profile:z.literal('agentos.validate'),
  reason:z.string().min(20).max(2000)}).strict();

/** Admission policy identity only. Never transfer task prompts, contexts or
 * execution.inputs merely to inspect a CI gate. The original snapshot is not rebuilt. */
export async function readWorkerValidationIdentity(db:DB,runId:string) {
  return (await db.query(`SELECT r.id,r.task_id,r.fence,r.context_id,r.attempt,r.state,r.finished_at,
    t.goal_id,t.state task_state,t.attempt_count,
    r.instruction_snapshot->'workerValidation' worker_validation,
    r.instruction_snapshot#>>'{task,id}' admitted_task_id,
    r.instruction_snapshot#>>'{task,goal_id}' admitted_goal_id,
    r.instruction_snapshot#>>'{task,stage}' admitted_stage,
    r.instruction_snapshot#>>'{task,execution,kind}' admitted_kind
    FROM runs r JOIN tasks t ON t.id=r.task_id WHERE r.id=$1`,[runId])).rows[0]??null;
}

/** Only the authenticated configuration route calls this function. The event is
 * the registration receipt; a model artifact or an unregistered setting is not authority. */
export async function registerWorkerValidationPolicy(goalId:string,raw:unknown,auth:{channel:'api-token'|'operator-session'}) {
  z.uuid().parse(goalId); const policy=workerValidationPolicySchema.parse(raw);
  if(new Set(policy.stages).size!==policy.stages.length) throw new DomainError('VALIDATION_POLICY_FORMAT','Etapas duplicadas.');
  if(new Set(policy.requiredDatabaseTests.map(validationDigest)).size!==policy.requiredDatabaseTests.length)
    throw new DomainError('VALIDATION_POLICY_FORMAT','Casos obrigatórios duplicados.');
  const channel=z.enum(['api-token','operator-session']).parse(auth.channel);
  return tx(async db=>{
    await lock(db);
    const framework=await frameworkProjectAuthorization(db,goalId);
    if(policy.project!==framework.project) throw new DomainError('VALIDATION_PROJECT_UNREGISTERED','Projeto divergente.');
    if(policy.enabled) await assertWorkerValidationRegistrationOwner(db,goalId);
    const registration={version:1,id:'worker-validation:'+goalId,registrationId:randomUUID(),goalId,
      policy,policyDigest:validationDigest(policy),channel};
    await event(db,'WORKER_VALIDATION_POLICY_REGISTERED',goalId,registration);
    await db.query('INSERT INTO settings(id,value) VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET value=excluded.value',
      [registration.id,JSON.stringify(registration)]);
    return registration;
  });
}

/** Registration configures future admissions only. It is not execution authority
 * and cannot resume a goal or amend a run's immutable admission snapshot. The
 * ordinary active-owner check below remains the execution/acceptance boundary. */
async function assertWorkerValidationRegistrationOwner(db:DB,goalId:string) {
  const row=(await db.query(`SELECT g.state,o.mode,
    EXISTS(SELECT 1 FROM settings s WHERE s.id IN('management:'||g.id,'workforce:'||g.id) AND s.value->>'enabled'='true') manager,
    (SELECT value->>'enabled' FROM settings WHERE id='autonomy:'||g.project) autonomy,
    EXISTS(SELECT 1 FROM runs r JOIN tasks t ON t.id=r.task_id
      LEFT JOIN settings h ON h.id='worker-validation-handoff:'||r.id WHERE t.goal_id=g.id
      AND (r.state IN('queued','running','cancelling','reconciling') OR EXISTS(SELECT 1 FROM leases l WHERE l.run_id=r.id)
        OR h.value->>'state'='pending' OR (r.state='submitted' AND t.state='submitted' AND r.attempt=t.attempt_count
          AND r.instruction_snapshot->'workerValidation' IS NOT NULL AND r.instruction_snapshot->'workerValidation'<>'null'::jsonb
          AND COALESCE(h.value->>'state','pending')='pending'))) live,
    EXISTS(SELECT 1 FROM settings s WHERE s.id LIKE 'candidate-validation:%' AND s.value->>'goalId'=g.id::text
      AND s.value->>'state' IN('proposed','freezing','frozen','running','uncertain')) validation_owner
    FROM goals g LEFT JOIN objectives o ON o.id=g.id WHERE g.id=$1`,[goalId])).rows[0];
  if(row?.state==='active')return assertWorkerValidationOwner(db,goalId);
  const preparable=['planning','replanning','failed','planned'];
  if(!row || !preparable.includes(row.state) || row.mode!=='execute' ||
    row.autonomy==='false' || row.manager || row.live || row.validation_owner)throw Error('VALIDATION_RUNTIME_UNAUTHORIZED');
}

async function assertWorkerValidationOwner(db:DB,goalId:string,acceptedRunId?:string) {
  const row=(await db.query(`SELECT g.state,
    EXISTS(SELECT 1 FROM settings s WHERE s.id IN('management:'||g.id,'workforce:'||g.id) AND s.value->>'enabled'='true') manager,
    (SELECT value->>'enabled' FROM settings WHERE id='autonomy:'||g.project) autonomy,
    EXISTS(SELECT 1 FROM runs r JOIN tasks t ON t.id=r.task_id WHERE r.id=$2::uuid
      AND t.goal_id=g.id AND t.state='accepted' AND r.state='submitted' AND r.attempt=t.attempt_count) accepted
    FROM goals g WHERE g.id=$1`,[goalId,acceptedRunId??null])).rows[0];
  if(!row || !(row.state==='active'||row.state==='completed'&&row.accepted) || row.manager || row.autonomy==='false')
    throw Error('VALIDATION_RUNTIME_UNAUTHORIZED');
}

/** Revalidation never adopts a newer policy for an existing run. Revocation or
 * replacement fails closed; old runs without an admission snapshot stay legacy. */
export async function verifyWorkerValidationRegistration(db:DB,run:any,mode:'current'|'historical_evidence'='current') {
  const raw=run.worker_validation;
  const parsed=registrationSchema.safeParse(raw);
  if(!parsed.success) throw Error('VALIDATION_POLICY_UNBOUND');
  const registration=parsed.data;
  if(run.admitted_task_id!==run.task_id || run.admitted_goal_id!==run.goal_id ||
    registration.id!=='worker-validation:'+run.admitted_goal_id || registration.goalId!==run.admitted_goal_id ||
    !registration.policy.enabled || !registration.policy.stages.includes(run.admitted_stage) ||
    !['harness','opencode'].includes(run.admitted_kind) ||
    validationDigest(registration.policy)!==registration.policyDigest) throw Error('VALIDATION_POLICY_UNBOUND');
  if(mode==='current') {
    const current=(await db.query('SELECT value FROM settings WHERE id=$1',[registration.id])).rows[0]?.value;
    if(validationDigest(current??null)!==validationDigest(registration)) throw Error('VALIDATION_POLICY_REVOKED');
  }
  if(!(await db.query(`SELECT 1 FROM events WHERE kind='WORKER_VALIDATION_POLICY_REGISTERED'
    AND entity_id=$1 AND payload=$2::jsonb LIMIT 1`,[run.admitted_goal_id,JSON.stringify(registration)])).rowCount)
    throw Error('VALIDATION_POLICY_UNREGISTERED');
  const project=(await db.query('SELECT project FROM goals WHERE id=$1',[run.admitted_goal_id])).rows[0]?.project;
  if(project!==registration.policy.project) throw Error('VALIDATION_POLICY_UNBOUND');
  if(mode==='current') {
    await assertWorkerValidationOwner(db,run.admitted_goal_id,run.id);
    await frameworkProjectAuthorization(db,run.admitted_goal_id);
  }
  return registration;
}

export async function workerValidationAuthority(db:DB,run:any,mode:'current'|'historical_evidence'='current') {
  const policyRegistration=await verifyWorkerValidationRegistration(db,run,mode);
  const refs=(await db.query(`SELECT id,digest,size FROM artifacts WHERE run_id=$1
    AND name='validation-intent.json' ORDER BY id LIMIT 2`,[run.id])).rows;
  if(refs.length!==1 || refs[0].size>4096 || refs[0].size<1) throw Error('VALIDATION_INTENT_REQUIRED');
  const ref=refs[0],a=await readArtifact(ref.id);
  if(a.run_id!==run.id || a.bytes.length!==ref.size || sha(a.bytes)!==ref.digest) throw Error('VALIDATION_INTENT_CORRUPT');
  const parsed=validationIntentSchema.safeParse(JSON.parse(a.bytes.toString()));
  if(!parsed.success) throw Error('VALIDATION_INTENT_FORMAT');
  return {authority:{version:1,kind:'runtime_artifact',profile:'agentos.validate',policyRegistration,
    intent:{artifactId:ref.id,digest:ref.digest,size:ref.size,base64:a.bytes.toString('base64')},
    snapshotDigest:validationDigest(policyRegistration)},reason:parsed.data.reason};
}

export async function verifyWorkerValidationAuthority(db:DB,value:any,mode:'current'|'historical_evidence'='current') {
  if(value.authority!=='runtime_artifact' || value.request?.authority!=='runtime_artifact' ||
    value.managerRunId || value.request.managerRunId || value.request.decisionProof) throw Error('VALIDATION_AUTHORITY_UNBOUND');
  const run=await readWorkerValidationIdentity(db,value.runId);
  if(!run || run.task_id!==value.taskId || run.fence!==value.runFence || run.context_id!==value.contextVersion ||
    run.attempt!==value.expectedAttempt || !['submitted','failed'].includes(run.state) || !run.finished_at)
    throw Error('VALIDATION_BINDING_STALE');
  const proof=await workerValidationAuthority(db,run,mode);
  if(validationDigest(proof.authority)!==validationDigest(value.request.runtimeAuthority)) throw Error('VALIDATION_AUTHORITY_UNBOUND');
  return run;
}

/** Rendered only for admissions carrying this contract version; no live prompt is rewritten. */
export function workerValidationInstruction(run:any) {
  if(run.instruction_snapshot?.workerValidationContract!==1 || run.instruction_snapshot.task?.stage==='coordinate') return '';
  if(run.instruction_snapshot.workerValidation) return 'REGISTERED CI HANDOFF: request_validation is a manager action, not a worker tool. After local checks, write evidence/validation-intent.json with exactly {"version":1,"profile":"agentos.validate","reason":"Explain required candidate CI in at least 20 characters"} and include {"name":"validation-intent.json","path":"evidence/validation-intent.json","mime":"application/json"} in your final artifacts alongside the normal delivery artifacts. candidate.json is produced by the runtime; never list or forge that reserved artifact. Finish the attempt; the controller confirms stop, binds this candidate and runs the registered isolated CI. Do not access controller databases, credentials or start the validator yourself. Local passed:true is not registered CI proof. Report unavailable database coverage honestly; the controller keeps acceptance pending until canonical CI evidence exists.';
  return 'REGISTERED CI CAPABILITY: request_validation is a manager action and is not available as a worker tool. This attempt has no registered worker CI policy. Execute only available checks, report unavailable database tests and coverage as limitations, and never claim they ran or invent CI receipts.';
}

export type WorkerValidationGate={required:boolean,state:'legacy'|'pending'|'failed'|'passed',detail:string,actionId?:string};
export async function workerValidationGate(db:DB,runId:string):Promise<WorkerValidationGate> {
  const run=await readWorkerValidationIdentity(db,runId);
  if(!run?.worker_validation) return {required:false,state:'legacy',detail:''};
  try { await verifyWorkerValidationRegistration(db,run); }
  catch(e:any) { return {required:true,state:'failed',detail:e.message}; }
  const handoff=(await db.query('SELECT value FROM settings WHERE id=$1',['worker-validation-handoff:'+runId])).rows[0]?.value;
  if(handoff?.state==='rejected') return {required:true,state:'failed',detail:handoff.code};
  if(!handoff?.actionId) return {required:true,state:'pending',detail:'VALIDATION_INTENT_HANDOFF_PENDING'};
  const job=(await db.query(`SELECT (value-'request')||jsonb_build_object('request',(value->'request')-'candidateBase64') value
    FROM settings WHERE id=$1`,['candidate-validation:'+handoff.actionId])).rows[0]?.value;
  if(!job) return {required:true,state:'pending',detail:'VALIDATION_OWNER_PENDING'};
  try {await verifyWorkerValidationAuthority(db,job);} catch(e:any){return {required:true,state:'failed',detail:e.message};}
  const base={required:true,actionId:job.actionId};
  if(['failed','rejected'].includes(job.state)) return {...base,state:'failed',detail:job.result?.code??job.code??'VALIDATION_REJECTED'};
  if(job.state!=='completed') return {...base,state:'pending',detail:'REGISTERED_CI_PENDING'};
  const {readRuntimeCandidateValidationEvidence}=await import('./candidate-validation-evidence.js');
  try {
    const body=await readRuntimeCandidateValidationEvidence(db,job.actionId);
    if(!body) return {...base,state:'pending',detail:'REGISTERED_CI_EVIDENCE_PENDING'};
    if(body.authority.outcome.passed!==true) return {...base,state:'failed',detail:body.authority.outcome.code};
    const coverage=body.authority.coverage,required=run.worker_validation.policy.requiredDatabaseTests;
    if(!coverage) return {...base,state:'failed',detail:'CI_REQUIRED_COVERAGE_MISSING'};
    if(coverage.profile!=='agentos.validate' || coverage.candidateDigest!==job.candidateDigest ||
      coverage.obligationsDigest!==validationDigest(required)) return {...base,state:'failed',detail:'CI_REQUIRED_COVERAGE_UNBOUND'};
    const actual=coverage.requiredTests.map(({file,name}:any)=>validationDigest({file,name})).sort();
    if(validationDigest(actual)!==validationDigest(required.map(validationDigest).sort()) || new Set(actual).size!==actual.length)
      return {...base,state:'failed',detail:'CI_REQUIRED_COVERAGE_UNBOUND'};
    if(coverage.requiredTests.some((test:any)=>test.status!=='passed'))
      return {...base,state:'failed',detail:'CI_REQUIRED_TESTS_NOT_PASSED'};
    return {...base,state:'passed',detail:'Registered candidate CI and required database coverage verified: '+body.receiptDigest};
  } catch(e:any){return {...base,state:'failed',detail:e.message};}
}

/** Small DB identity for the final CAS. No artifact, log or filesystem reads;
 * payload digests are calculated in PostgreSQL instead of returning their bytes. */
export async function workerValidationCommitIdentity(db:DB,runId:string) {
  const run=await readWorkerValidationIdentity(db,runId);
  if(!run?.worker_validation) return {required:false};
  let authority:any;
  try {
    await verifyWorkerValidationRegistration(db,run);
    authority=(await frameworkProjectAuthorization(db,run.admitted_goal_id)).authorization;
  } catch(e:any) {authority={error:e.message};}
  const refs=(await db.query(`SELECT id,name,digest,size FROM artifacts WHERE run_id=$1
    AND name IN('candidate.json','validation-intent.json') ORDER BY name,id`,[runId])).rows;
  const metadata=(await db.query(`SELECT
      encode(sha256(convert_to(p.value::text,'UTF8')),'hex') policy_digest,
      encode(sha256(convert_to(h.value::text,'UTF8')),'hex') handoff_digest,
      encode(sha256(convert_to(j.value::text,'UTF8')),'hex') job_digest,
      encode(sha256(convert_to((e.value-'body')::text,'UTF8')),'hex') evidence_identity,
      encode(sha256(convert_to((e.value#>'{body,authority}')::text,'UTF8')),'hex') evidence_authority,
      encode(sha256(convert_to(COALESCE(c.manifest->'dependencyArtifacts','[]'::jsonb)::text,'UTF8')),'hex') dependencies_digest
    FROM runs r LEFT JOIN settings p ON p.id='worker-validation:'||$2::text
      LEFT JOIN settings h ON h.id='worker-validation-handoff:'||r.id
      LEFT JOIN settings j ON j.id='candidate-validation:'||(h.value->>'actionId')
      LEFT JOIN settings e ON e.id='candidate-validation-evidence:'||(h.value->>'actionId')
      LEFT JOIN contexts c ON c.id=r.context_id AND c.run_id=r.id WHERE r.id=$1`,[runId,run.goal_id])).rows[0];
  const source=await readDeliverySourceIdentity(db,run.task_id);
  const sourceIdentity=source?Object.fromEntries(['id','goal_id','attempt_count','run_id','fence','run_state','attempt','snapshot_schema',
    'scope','workspace','work_kind','contract_digest','other_fields_equal','execution_equal','live','leased','cancelled'].map(k=>[k,source[k]])):null;
  return {required:true,run:{id:run.id,task_id:run.task_id,goal_id:run.goal_id,fence:run.fence,context_id:run.context_id,
    attempt:run.attempt,state:run.state,finished_at:run.finished_at?.toISOString?.()??run.finished_at,policyDigest:validationDigest(run.worker_validation)},
    authority,refs,metadata,source:sourceIdentity};
}

/** The complete proof is checked outside admission locks, then sealed by a
 * before/after identity. A change while reading bytes cannot be blessed by the CAS. */
export async function verifiedWorkerValidationGate(db:DB,runId:string) {
  const before=await workerValidationCommitIdentity(db,runId);
  const gate=await workerValidationGate(db,runId);
  const identity=await workerValidationCommitIdentity(db,runId);
  if(validationDigest(before)!==validationDigest(identity))
    throw new DomainError('STATE_CHANGED','A autoridade ou evidência de CI mudou durante a leitura.');
  return {gate,identity};
}
