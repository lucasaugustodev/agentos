import {afterAll,beforeAll,beforeEach,expect,it,vi} from 'vitest';
import {createHash,randomUUID} from 'node:crypto';
import {pool,migrate} from '../server/db.js';
import {seedRegistry} from '../server/registry.js';
import {installManagement} from '../server/management.js';
import {writeArtifact,completeRun,validateTask} from '../server/service.js';
import {reconcileWorkerValidationIntents,reconcileCandidateValidations} from '../server/candidate-validation.js';
import {reconcileCandidateValidationEvidence,runtimeValidationRepairInputs} from '../server/candidate-validation-evidence.js';
import {registerWorkerValidationPolicy,validationDigest,canonicalValidationValue,workerValidationInstruction,
  workerValidationGate} from '../server/worker-validation.js';
import {readPendingValidations} from '../server/validation-identity.js';

const sha=(bytes:Buffer|string)=>createHash('sha256').update(bytes).digest('hex');
const required=[{file:'tests/transaction-pg.test.ts',name:'transaction rejects a stale writer'}];
const policy={version:1,enabled:true,project:'agentos',profile:'agentos.validate',stages:['verify'],required:true,requiredDatabaseTests:required};
let f:any;
beforeAll(async()=>{await migrate();await installManagement();});
beforeEach(async()=>{
  await pool.query("DELETE FROM settings WHERE id LIKE 'candidate-validation:%' OR id LIKE 'worker-validation:%' OR id LIKE 'worker-validation-handoff:%'");
  await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');await seedRegistry();
  f={goal:randomUUID(),task:randomUUID(),run:randomUUID(),fence:randomUUID(),context:randomUUID(),candidateDigest:sha('[]')};
  await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1::uuid,'Framework CI','No manager','agentos',$1::text)",[f.goal]);
});
afterAll(async()=>{await pool.end();});
async function admit(options:{registered?:boolean,stage?:string,intent?:unknown,omitIntent?:boolean,submit?:boolean,passed?:boolean}={}) {
  if(options.registered!==false)f.registration=await registerWorkerValidationPolicy(f.goal,policy,{channel:'api-token'});
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,attempt_count,covers,recovery)
    VALUES($1,$2,'candidate','Candidate verification','Frozen worker contract','qa.integration','running',$3,$4,$5,1,'["requirement-a"]','{"mode":"automatic"}')`,
    [f.task,f.goal,options.stage??'verify',JSON.stringify([{id:'proof',label:'Original criterion',type:'json_field',name:'verification.json',field:'passed',equals:true}]),
      JSON.stringify({kind:'harness',workspace:'agentos/oworkerfixture',baseWorkspace:'agentos/base',readOnlySource:true})]);
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,context_id) VALUES($1,$2,1,$3,'harness',1,'running',$4)",
    [f.run,f.task,f.fence,f.context]);
  await pool.query("INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,'{}','Context')",[f.context,f.run]);
  f.candidate=await writeArtifact(f.run,f.fence,'candidate.json',JSON.stringify({baseDigest:f.candidateDigest,candidateDigest:f.candidateDigest,files:[]}));
  await writeArtifact(f.run,f.fence,'verification.json',JSON.stringify({passed:options.passed??true}));
  if(!options.omitIntent)await writeArtifact(f.run,f.fence,'validation-intent.json',JSON.stringify(options.intent??{
    version:1,profile:'agentos.validate',reason:'Run the registered database checks on this candidate.'}));
  if(options.submit!==false)await completeRun(f.run,f.fence,'Local verification complete; registered CI pending.');
}
const run=async()=>(await pool.query('SELECT * FROM runs WHERE id=$1',[f.run])).rows[0];
const handoff=async()=>(await pool.query('SELECT value FROM settings WHERE id=$1',['worker-validation-handoff:'+f.run])).rows[0]?.value;
const inspect=async()=>({runId:f.run,workspace:'agentos/oworkerfixture',checkedAt:new Date().toISOString(),
  candidate:{candidateDigest:f.candidateDigest},process:{unit:'agentos-run-'+f.run,stopped:true,mainPid:0,populated:false,activeState:'inactive'}});
const stored=async(id:string)=>(await pool.query('SELECT value FROM settings WHERE id=$1',['candidate-validation:'+id])).rows[0]?.value;
async function completed() {
  await reconcileWorkerValidationIntents(inspect);const h=await handoff();expect(h.state).toBe('reserved');
  const dispatch=async(mode:string,request:any)=>({actionId:request.actionId,candidateDigest:request.candidateDigest,requestDigest:validationDigest(request),
    ...(mode==='freeze'?{state:'frozen',frozenWorkspace:'agentos/ocvfixture'}:{state:'completed',passed:true,code:'VALIDATION_PASSED',
      resultDigest:'e'.repeat(64),publishedBaselineDigest:'f'.repeat(64),registeredReceiptSha:'a'.repeat(64)})});
  await reconcileCandidateValidations(dispatch,inspect);await reconcileCandidateValidations(dispatch,inspect);
  return stored(h.actionId);
}
function exported(job:any,coverage:any={version:1,source:'registered_vitest_json',profile:'agentos.validate',
  candidateDigest:f.candidateDigest,obligationsDigest:validationDigest(required),reportDigest:'c'.repeat(64),
  requiredTests:required.map(t=>({...t,status:'passed'}))},extra:any={}) {
  const log=Buffer.from('Registered isolated CI executed the required case.\n');
  const authority={version:1,kind:'registered_candidate_validation',actionId:job.actionId,requestDigest:validationDigest(job.request),
    resultDigest:job.result.resultDigest,receiptDigest:'d'.repeat(64),producer:{...Object.fromEntries([
      'goalId','taskId','runId','runFence','contextVersion','expectedAttempt','candidateDigest'].map(k=>[k,job[k]])),contractDigest:job.request.contractDigest},
    outcome:{passed:job.result.passed,code:job.result.code,state:'completed'},
    registeredActionSha:'02a92158a4084a1aeb3b4c55998b6740d90428f80acb06a225730a1423ba9c26',
    registeredCommand:['/usr/bin/python3','/opt/agentos-control/agentos-release-action.py','validate'],
    publishedBaselineDigest:job.result.publishedBaselineDigest,publicationProofDigest:'f'.repeat(64),validationProofDigest:'f'.repeat(64),
    registeredReceiptSha:job.result.registeredReceiptSha,validatedReleaseDigest:'b'.repeat(64),
    log:{rawDigest:sha(log),rawBytes:log.length,digest:sha(log),bytes:log.length,sanitized:true,completeRegisteredArtifact:true,
      source:'registered_action_validation.log',commandStdoutMayBeTruncatedByRegisteredAction:true},acceptance:false,
    ...(coverage?{coverage}:{}),...extra};
  const proof=Buffer.from(JSON.stringify(canonicalValidationValue(authority)));
  return {version:1,actionId:job.actionId,requestDigest:authority.requestDigest,resultDigest:authority.resultDigest,
    receiptDigest:authority.receiptDigest,authority,inputs:[
      {name:'ci-receipt.json',mime:'application/json',digest:sha(proof),size:proof.length,base64:proof.toString('base64')},
      {name:'validation.log',mime:'text/plain',digest:sha(log),size:log.length,base64:log.toString('base64')} ]};
}
async function publish(job:any,coverage?:any,extra?:any) {
  await reconcileCandidateValidationEvidence(async()=>exported(job,coverage,extra));
}

it('freezes authenticated policy at admission and ignores registration after an existing run began',async()=>{
  await admit({registered:false,submit:false});const before=(await run()).instruction_snapshot;
  expect(before.workerValidation).toBeNull();
  await registerWorkerValidationPolicy(f.goal,policy,{channel:'operator-session'});
  expect((await run()).instruction_snapshot).toEqual(before);
  await completeRun(f.run,f.fence,'Legacy run remains legacy.');
  await reconcileWorkerValidationIntents(inspect);expect(await handoff()).toBeUndefined();
  expect((await validateTask(f.task)).passed).toBe(true);
});
it('applies required CI only to declared stages and does not change manager instructions',async()=>{
  await admit({stage:'work'});expect((await run()).instruction_snapshot.workerValidation).toBeNull();
  expect((await validateTask(f.task)).passed).toBe(true);
  expect(workerValidationInstruction({instruction_snapshot:{workerValidationContract:1,task:{stage:'coordinate'}}})).toBe('');
  expect(workerValidationInstruction({instruction_snapshot:{workerValidationContract:1,task:{stage:'work'}}})).toContain('not available as a worker tool');
});
it('does not accept worker passed:true before handoff, frozen CI and canonical evidence',async()=>{
  await admit();expect((await validateTask(f.task))).toMatchObject({passed:false,pending:true});
  expect(await readPendingValidations()).toEqual([]);
  const job=await completed();expect((await validateTask(f.task))).toMatchObject({passed:false,pending:true});
  expect((await pool.query('SELECT count(*)::int n FROM acceptances')).rows[0].n).toBe(0);
  await publish(job);expect(await readPendingValidations()).toEqual([{id:f.task}]);
  const [a,b]=await Promise.all([validateTask(f.task),validateTask(f.task)]);
  expect(a.passed).toBe(true);expect(b.passed).toBe(true);
  expect((await pool.query('SELECT count(*)::int n FROM acceptances')).rows[0].n).toBe(1);
});
it('reserves one existing CI owner for duplicate handoff without manager identity or command input',async()=>{
  await admit();const before=(await run()).instruction_snapshot;
  await Promise.all([reconcileWorkerValidationIntents(inspect),reconcileWorkerValidationIntents(inspect)]);
  const h=await handoff(),job=await stored(h.actionId);
  expect(job.request.authority).toBe('runtime_artifact');expect(job.request.managerRunId).toBeUndefined();
  expect(job.request.runtimeAuthority.policyRegistration).toEqual(f.registration);
  expect(job.request.runtimeAuthority.snapshotDigest).toBe(validationDigest(f.registration));
  expect((await pool.query("SELECT count(*)::int n FROM settings WHERE id LIKE 'candidate-validation:%'")).rows[0].n).toBe(1);
  expect((await pool.query('SELECT count(*)::int n FROM management_actions')).rows[0].n).toBe(0);
  expect((await run()).instruction_snapshot).toEqual(before);
});
it.each(['missing','command','profile'])('rejects %s intent without granting worker execution authority',async(mode)=>{
  await admit({omitIntent:mode==='missing',intent:{version:1,profile:mode==='profile'?'arbitrary':'agentos.validate',
    reason:'Run the registered checks for this exact candidate.',...(mode==='command'?{command:'arbitrary shell'}:{})}});
  await reconcileWorkerValidationIntents(inspect);expect((await handoff()).state).toBe('rejected');
  expect((await validateTask(f.task)).passed).toBe(false);
  expect((await pool.query("SELECT count(*)::int n FROM settings WHERE id LIKE 'candidate-validation:%'")).rows[0].n).toBe(0);
});
it('does not inspect a running attempt and keeps unconfirmed physical stop pending',async()=>{
  await admit({submit:false});const probe=vi.fn(inspect);
  await reconcileWorkerValidationIntents(probe);expect(probe).not.toHaveBeenCalled();
  await completeRun(f.run,f.fence,'Submitted but stop must be independently checked.');
  await reconcileWorkerValidationIntents(async()=>{throw Error('ssh disconnected');});
  expect(await handoff()).toMatchObject({state:'pending',code:'STOP_UNCONFIRMED'});
  expect((await validateTask(f.task))).toMatchObject({pending:true,passed:false});
  await expect(pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,2,$3,'harness',1,'queued')",[randomUUID(),f.task,randomUUID()]))
    .rejects.toThrow('CANDIDATE_VALIDATION_OWNS_TASK');
});
it('refuses policy revocation after reservation and never dispatches frozen CI',async()=>{
  await admit();await reconcileWorkerValidationIntents(inspect);const h=await handoff();
  await registerWorkerValidationPolicy(f.goal,{...policy,enabled:false},{channel:'api-token'});
  const dispatch=vi.fn();await reconcileCandidateValidations(dispatch,inspect);
  expect(dispatch).not.toHaveBeenCalled();expect((await stored(h.actionId)).code).toBe('VALIDATION_POLICY_REVOKED');
  expect((await validateTask(f.task)).passed).toBe(false);
});
it('rejects candidate mismatch before freezing',async()=>{
  await admit();await reconcileWorkerValidationIntents(async()=>({...await inspect(),candidate:{candidateDigest:'a'.repeat(64)}}));
  expect(await handoff()).toMatchObject({state:'rejected',code:'VALIDATION_CANDIDATE_CHANGED'});
  expect((await validateTask(f.task)).passed).toBe(false);
});
it.each(['fence','context_id'])('refuses a changed %s identity after reservation',async(field)=>{
  await admit();await reconcileWorkerValidationIntents(inspect);const h=await handoff();
  await pool.query(`UPDATE runs SET ${field}=$2 WHERE id=$1`,[f.run,randomUUID()]);
  const dispatch=vi.fn();await reconcileCandidateValidations(dispatch,inspect);
  expect(dispatch).not.toHaveBeenCalled();expect((await stored(h.actionId)).code).toBe('VALIDATION_BINDING_STALE');
  expect((await validateTask(f.task)).passed).toBe(false);
});
it('refuses runtime policy for a goal already owned by management and workforce',async()=>{
  await pool.query('INSERT INTO settings(id,value) VALUES($1,\'{"enabled":true}\')',['management:'+f.goal]);
  await expect(registerWorkerValidationPolicy(f.goal,policy,{channel:'api-token'})).rejects.toThrow('VALIDATION_RUNTIME_UNAUTHORIZED');
});
it('preserves uncertain owner and probes only, never starts a second validation',async()=>{
  await admit();await reconcileWorkerValidationIntents(inspect);const h=await handoff();
  const modes:string[]=[];const dispatch=async(mode:string,request:any)=>{
    modes.push(mode);if(mode==='freeze')throw Error('lost response');
    return {actionId:request.actionId,candidateDigest:request.candidateDigest,requestDigest:validationDigest(request),state:'uncertain'};
  };
  await reconcileCandidateValidations(dispatch,inspect);await reconcileCandidateValidations(dispatch,inspect);
  expect(modes).toEqual(['freeze','receipt']);expect((await stored(h.actionId)).state).toBe('uncertain');
  expect((await validateTask(f.task))).toMatchObject({pending:true,passed:false});
});
it.each(['missing','skipped','failed','wrong-candidate','wrong-obligations'])('does not accept %s required database coverage',async(mode)=>{
  await admit();const job=await completed();const body=exported(job);
  let coverage:any=body.authority.coverage;
  if(mode==='missing')coverage=null;
  if(mode==='skipped'||mode==='failed')coverage.requiredTests[0].status=mode;
  if(mode==='wrong-candidate')coverage.candidateDigest='0'.repeat(64);
  if(mode==='wrong-obligations')coverage.obligationsDigest='0'.repeat(64);
  await publish(job,coverage);const result=await validateTask(f.task);
  expect(result.passed).toBe(false);expect(result.checks.find(c=>c.id==='registered-ci')?.passed).toBe(false);
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1',[f.task])).rows[0].state).toBe('submitted');
});
it('registered technical CI does not override a failed original criterion',async()=>{
  await admit({passed:false});const job=await completed();await publish(job);
  const result=await validateTask(f.task);expect(result.passed).toBe(false);
  expect(result.checks.find(c=>c.id==='registered-ci')?.passed).toBe(true);
  expect(result.checks.find(c=>c.id==='proof')?.passed).toBe(false);
  const before=(await pool.query('SELECT id,digest FROM artifacts WHERE run_id=$1 ORDER BY id',[f.run])).rows;
  const inputs=await runtimeValidationRepairInputs(pool,f.goal,[f.task],['requirement-a']);
  expect(inputs.map((i:any)=>i.name)).toEqual(['ci-receipt.json','validation.log']);
  expect((await pool.query('SELECT id,digest FROM artifacts WHERE run_id=$1 ORDER BY id',[f.run])).rows).toEqual(before);
  expect(await runtimeValidationRepairInputs(pool,f.goal,[f.task],['other-requirement'])).toEqual([]);
});
it('rejects an arbitrary executor SHA and unregistered settings without an authenticated receipt',async()=>{
  await admit();const job=await completed();await publish(job,undefined,{registeredActionSha:'0'.repeat(64)});
  expect((await workerValidationGate(pool,f.run)).state).toBe('pending');
  await pool.query("DELETE FROM events WHERE kind='WORKER_VALIDATION_POLICY_REGISTERED' AND entity_id=$1",[f.goal]);
  expect(await workerValidationGate(pool,f.run)).toMatchObject({state:'failed',detail:'VALIDATION_POLICY_UNREGISTERED'});
});
