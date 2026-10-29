import {createHash, randomUUID} from 'node:crypto';
import {z} from 'zod';
import {event, lock, pool, tx, type DB} from './db.js';
import {DomainError} from './contracts.js';
import {readArtifact} from './service.js';
import {frameworkProjectAuthorization} from './candidate-validation.js';

export const canonicalValidationValue = (v:any):any => Array.isArray(v) ? v.map(canonicalValidationValue)
  : v && typeof v==='object' ? Object.fromEntries(Object.keys(v).sort().map(k=>[k,canonicalValidationValue(v[k])])) : v;
export const validationDigest = (v:any) => createHash('sha256').update(JSON.stringify(canonicalValidationValue(v))).digest('hex');
const sha = (v:Buffer) => createHash('sha256').update(v).digest('hex');
export const requiredDatabaseTestSchema=z.object({file:z.string().regex(/^tests\/(?:[a-zA-Z0-9_-]+\/)*[a-zA-Z0-9_-]+(?:\.[a-zA-Z0-9_-]+)*\.test\.[cm]?[jt]s$/),
  name:z.string().min(1).max(500)}).strict();
export const workerValidationPolicySchema=z.object({version:z.literal(1),enabled:z.boolean(),
  project:z.string().regex(/^[a-z0-9_-]+$/),profile:z.literal('agentos.validate'),
  stages:z.array(z.enum(['work','integrate','verify'])).min(1).max(3),required:z.literal(true),
  requiredDatabaseTests:z.array(requiredDatabaseTestSchema).min(1).max(128)}).strict();
const registrationSchema=z.object({version:z.literal(1),id:z.string(),registrationId:z.uuid(),goalId:z.uuid(),
  policy:workerValidationPolicySchema,policyDigest:z.string().regex(/^[a-f0-9]{64}$/),
  channel:z.enum(['api-token','operator-session'])}).strict();
export const validationIntentSchema=z.object({version:z.literal(1),profile:z.literal('agentos.validate'),
  reason:z.string().min(20).max(2000)}).strict();

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
    if(policy.enabled) await assertWorkerValidationOwner(db,goalId);
    const registration={version:1,id:'worker-validation:'+goalId,registrationId:randomUUID(),goalId,
      policy,policyDigest:validationDigest(policy),channel};
    await event(db,'WORKER_VALIDATION_POLICY_REGISTERED',goalId,registration);
    await db.query('INSERT INTO settings(id,value) VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET value=excluded.value',
      [registration.id,JSON.stringify(registration)]);
    return registration;
  });
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
export async function verifyWorkerValidationRegistration(db:DB,run:any) {
  const raw=run.instruction_snapshot?.workerValidation;
  const parsed=registrationSchema.safeParse(raw);
  if(!parsed.success) throw Error('VALIDATION_POLICY_UNBOUND');
  const registration=parsed.data, task=run.instruction_snapshot?.task;
  if(!task || registration.id!=='worker-validation:'+task.goal_id || registration.goalId!==task.goal_id ||
    !registration.policy.enabled || !registration.policy.stages.includes(task.stage) ||
    !['harness','opencode'].includes(task.execution?.kind) ||
    validationDigest(registration.policy)!==registration.policyDigest) throw Error('VALIDATION_POLICY_UNBOUND');
  const current=(await db.query('SELECT value FROM settings WHERE id=$1',[registration.id])).rows[0]?.value;
  if(validationDigest(current??null)!==validationDigest(registration)) throw Error('VALIDATION_POLICY_REVOKED');
  if(!(await db.query(`SELECT 1 FROM events WHERE kind='WORKER_VALIDATION_POLICY_REGISTERED'
    AND entity_id=$1 AND payload=$2::jsonb LIMIT 1`,[task.goal_id,JSON.stringify(registration)])).rowCount)
    throw Error('VALIDATION_POLICY_UNREGISTERED');
  await assertWorkerValidationOwner(db,task.goal_id,run.id);
  const framework=await frameworkProjectAuthorization(db,task.goal_id);
  if(framework.project!==registration.policy.project) throw Error('VALIDATION_POLICY_UNBOUND');
  return registration;
}

export async function workerValidationAuthority(db:DB,run:any) {
  const policyRegistration=await verifyWorkerValidationRegistration(db,run);
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

export async function verifyWorkerValidationAuthority(db:DB,value:any) {
  if(value.authority!=='runtime_artifact' || value.request?.authority!=='runtime_artifact' ||
    value.managerRunId || value.request.managerRunId || value.request.decisionProof) throw Error('VALIDATION_AUTHORITY_UNBOUND');
  const run=(await db.query('SELECT * FROM runs WHERE id=$1',[value.runId])).rows[0];
  if(!run || run.task_id!==value.taskId || run.fence!==value.runFence || run.context_id!==value.contextVersion ||
    run.attempt!==value.expectedAttempt || !['submitted','failed'].includes(run.state) || !run.finished_at)
    throw Error('VALIDATION_BINDING_STALE');
  const proof=await workerValidationAuthority(db,run);
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
  const run=(await db.query('SELECT * FROM runs WHERE id=$1',[runId])).rows[0];
  if(!run?.instruction_snapshot?.workerValidation) return {required:false,state:'legacy',detail:''};
  try { await verifyWorkerValidationRegistration(db,run); }
  catch(e:any) { return {required:true,state:'failed',detail:e.message}; }
  const handoff=(await db.query('SELECT value FROM settings WHERE id=$1',['worker-validation-handoff:'+runId])).rows[0]?.value;
  if(handoff?.state==='rejected') return {required:true,state:'failed',detail:handoff.code};
  if(!handoff?.actionId) return {required:true,state:'pending',detail:'VALIDATION_INTENT_HANDOFF_PENDING'};
  const job=(await db.query('SELECT value FROM settings WHERE id=$1',['candidate-validation:'+handoff.actionId])).rows[0]?.value;
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
    const coverage=body.authority.coverage,required=run.instruction_snapshot.workerValidation.policy.requiredDatabaseTests;
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
