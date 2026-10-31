import {afterAll,afterEach,beforeAll,beforeEach,expect,it,vi} from 'vitest';
import {createHash,randomUUID} from 'node:crypto';
import type {PoolClient} from 'pg';
import {pool,migrate,tx,lock} from '../server/db.js';
import {seedRegistry} from '../server/registry.js';
import {installManagement} from '../server/management.js';
import {writeArtifact,completeRun,validateTask} from '../server/service.js';
import {registerWorkerValidationPolicy,validationDigest,canonicalValidationValue,workerValidationInstruction,workerValidationGate} from '../server/worker-validation.js';
import {registerCandidateReadinessMap,bindCandidateReadinessPlan,recordCandidateReadinessAdmission,readCandidateReadinessProof} from '../server/candidate-readiness.js';
import {reconcileWorkerValidationIntents,reconcileCandidateValidations} from '../server/candidate-validation.js';
import {reconcileCandidateValidationEvidence} from '../server/candidate-validation-evidence.js';
import {taskDeliveryContract} from '../bridge/task-delivery-contract.mjs';
import {readValidationSnapshot} from '../server/validation-identity.js';
import {planSchema} from '../server/objective-contracts.js';
import {makeApp} from '../server/app.js';

// Real PostgreSQL, admission triggers, artifacts, acceptance CAS and canonical
// evidence parser. Physical inspector and registered external CI are controlled
// fixture boundaries: this suite does not claim a production build was run.
const expected={candidate:'/opt/agentos/releases/managed-11111111-1111-4111-8111-111111111111',digest:'a'.repeat(64)};
const required=[{file:'tests/transaction-pg.test.ts',name:'transaction rejects a stale writer'}];
const policy={version:1,enabled:true,project:'agentos',profile:'agentos.validate',stages:['integrate'],required:true,requiredDatabaseTests:required};
const description='Preserve all inherited source and independently validate the candidate.';
const sha=(value:string|Buffer)=>createHash('sha256').update(value).digest('hex');
let f:any,owner:PoolClient|undefined;
beforeAll(async()=>{await migrate();await installManagement();});
beforeEach(async()=>{
  await pool.query("DELETE FROM settings WHERE id='maintenance' OR id LIKE 'worker-readiness:%' OR id LIKE 'candidate-readiness:%' OR id LIKE 'worker-validation:%' OR id LIKE 'worker-validation-handoff:%' OR id LIKE 'candidate-validation:%'");
  await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');await seedRegistry();
  f={goal:randomUUID(),task:randomUUID(),run:randomUUID(),fence:randomUUID(),context:randomUUID(),candidateDigest:sha('[]')};
  await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1::uuid,'Readiness CI flow','Isolated fixture','agentos',$1::text)",[f.goal]);
  await pool.query("INSERT INTO objectives(id,request,request_hash,mode,status) VALUES($1,$2,'fixture','execute','executing')",[f.goal,JSON.stringify({project:'agentos',requirements:[description]})]);
  f.policy=await registerWorkerValidationPolicy(f.goal,policy,{channel:'api-token'});
  f.map={version:1,enabled:true,profile:'agentos.validate',policyRegistrationId:f.policy.registrationId,
    requirements:[{id:'req-1',digest:validationDigest(description)}],obligations:[
      {id:'local-source',requirements:['req-1'],kind:'local',name:'local-evidence.json',field:'passed',equals:true},
      {id:'ci-source',requirements:['req-1'],kind:'registered_ci',...required[0]}]};
  f.registration=await registerCandidateReadinessMap(f.goal,f.map,{channel:'operator-session'});
  owner=await pool.connect();await owner.query('SELECT pg_advisory_lock(4280027),pg_advisory_lock(4280050)');
  const backendPid=(await owner.query('SELECT pg_backend_pid() pid')).rows[0].pid;
  const readiness={service:'agentos-workers',pid:12345,...expected,startupId:randomUUID(),readyAt:new Date(Date.now()-1000).toISOString(),locks:[{key:4280027,backendPid},{key:4280050,backendPid}]};
  await pool.query('INSERT INTO settings(id,value) VALUES($1,$2)',['worker-readiness:agentos-workers',JSON.stringify(readiness)]);
  await pool.query("INSERT INTO events(kind,payload) VALUES('WORKER_SERVICE_UPDATED',$1)",[JSON.stringify({service:'agentos-workers',candidate:expected.candidate,pid:readiness.pid,readiness,
    stopped:{unit:'agentos-workers',previousPid:12344,activeState:'inactive',mainPid:0,oldPidGone:true,cgroupEmpty:true,checkedAt:new Date(Date.now()-2000).toISOString()}})]);
  const plan=planSchema.parse({title:'Readiness closed fixture',summary:'A finite registered integration flow',requirements:[{id:'req-1',description}],
    incremental:{version:1,selectedRequirementId:'req-1',selectedRequirementIds:['req-1'],completedRequirementIds:[],pendingRequirementIds:[]},
    tasks:[{key:'integrate',title:'Integrate candidate',description:'Combine all accepted changes. Execute os controles combinados: original prose.',agentId:'engineering.integration',stage:'integrate',baseWorkspace:'agentos/base',dependencies:[],covers:['req-1'],
      criteria:[{id:'integrated',label:'Original check',type:'json_field',name:'delivery.json',field:'passed',equals:true}]}]});
  f.plan=await tx(async db=>{await lock(db);return bindCandidateReadinessPlan(db,f.goal,plan,{consumerRelease:expected});});
  f.criterion=f.plan.tasks[0].criteria[0];
  f.planning=randomUUID();f.revision=randomUUID();
  await pool.query("INSERT INTO planning_runs(id,objective_id,base_revision,trigger_key,state,fence) VALUES($1,$2,0,'fixture','completed',$3)",[f.planning,f.goal,randomUUID()]);
  await pool.query("INSERT INTO plan_revisions(id,objective_id,revision,planning_run_id,plan,status) VALUES($1,$2,1,$3,$4,'applied')",[f.revision,f.goal,f.planning,JSON.stringify(f.plan)]);
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,attempt_count,covers,recovery)
    VALUES($1,$2,'integrate','Integrate candidate',$3,'engineering.integration','ready','integrate',$4,$5,0,'["req-1"]','{"mode":"automatic"}')`,
    [f.task,f.goal,f.plan.tasks[0].description,JSON.stringify([f.criterion]),JSON.stringify({kind:'harness',workspace:'agentos/oreadinessflow',baseWorkspace:'agentos/base',readOnlySource:false})]);
  await pool.query("UPDATE tasks SET key='r1-integrate',plan_revision=1 WHERE id=$1",[f.task]);
});
afterEach(async()=>{if(owner){await owner.query('SELECT pg_advisory_unlock_all()');owner.release();owner=undefined;}vi.restoreAllMocks();vi.unstubAllEnvs();});
afterAll(()=>pool.end());
const run=async()=>(await pool.query('SELECT * FROM runs WHERE id=$1',[f.run])).rows[0];
const handoff=async()=>(await pool.query('SELECT value FROM settings WHERE id=$1',['worker-validation-handoff:'+f.run])).rows[0]?.value;
const inspect=async()=>({runId:f.run,workspace:'agentos/oreadinessflow',checkedAt:new Date().toISOString(),candidate:{candidateDigest:f.candidateDigest},
  process:{unit:'agentos-run-'+f.run,stopped:true,mainPid:0,populated:false,activeState:'inactive'}});
async function admit(options:{localPassed?:boolean,submit?:boolean,report?: (r:any)=>void}={}){
  await tx(async db=>{await lock(db);
    await recordCandidateReadinessAdmission(db,{id:f.task,attempt_count:0},f.run,f.fence,{consumerRelease:expected});
    await db.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,context_id) VALUES($1,$2,1,$3,'harness',1,'running',$4)",[f.run,f.task,f.fence,f.context]);
    await db.query("UPDATE tasks SET state='running',attempt_count=1 WHERE id=$1",[f.task]);
    await db.query("INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,'{}','Frozen readiness context')",[f.context,f.run]);
  });
  f.local=await writeArtifact(f.run,f.fence,'local-evidence.json',JSON.stringify({passed:options.localPassed??true,observed:'actual fixture assertion'}));
  const report={version:1,state:'ready_for_ci',localChecks:[{id:'local-source',status:'passed',name:'local-evidence.json',digest:f.local.digest}],
    controllerChecks:{profile:'agentos.validate',status:'pending_controller_ci',obligationsDigest:f.criterion.obligationsDigest,tests:required},blockingFailures:[],limitations:[]};
  options.report?.(report);
  await writeArtifact(f.run,f.fence,'integration-readiness.json',JSON.stringify(report));
  await writeArtifact(f.run,f.fence,'validation-intent.json',JSON.stringify({version:1,profile:'agentos.validate',reason:'Run the registered database controls on the exact candidate.'}));
  await writeArtifact(f.run,f.fence,'candidate.json',JSON.stringify({baseDigest:f.candidateDigest,candidateDigest:f.candidateDigest,files:[]}));
  if(options.submit!==false)await completeRun(f.run,f.fence,'Real local result; controller CI pending.');
}
async function completed(){
  await reconcileWorkerValidationIntents(inspect);const h=await handoff();expect(h).toMatchObject({version:2,state:'reserved'});
  const dispatch=async(mode:string,request:any)=>({actionId:request.actionId,candidateDigest:request.candidateDigest,requestDigest:validationDigest(request),
    ...(mode==='freeze'?{state:'frozen',frozenWorkspace:'agentos/ocvfixture'}:{state:'completed',passed:true,code:'VALIDATION_PASSED',registeredActionSha:'cf4351977a994457dd0b74da8449c05597479ba4410e3015e499d7359d094383',
      resultDigest:'e'.repeat(64),publishedBaselineDigest:'f'.repeat(64),registeredReceiptSha:'a'.repeat(64)})});
  await reconcileCandidateValidations(dispatch,inspect);await reconcileCandidateValidations(dispatch,inspect);
  return (await pool.query('SELECT value FROM settings WHERE id=$1',['candidate-validation:'+h.actionId])).rows[0].value;
}
function exported(job:any,change?: (v:any)=>void){
  const log=Buffer.from('Isolated fixture registered CI evidence.\n');
  const authority:any={version:1,kind:'registered_candidate_validation',actionId:job.actionId,requestDigest:validationDigest(job.request),resultDigest:job.result.resultDigest,receiptDigest:'d'.repeat(64),
    producer:{...Object.fromEntries(['goalId','taskId','runId','runFence','contextVersion','expectedAttempt','candidateDigest'].map(k=>[k,job[k]])),contractDigest:job.request.contractDigest},
    outcome:{passed:true,code:'VALIDATION_PASSED',state:'completed'},registeredActionSha:'39843bdbde81e84b5093ed6667484378d1f4444c66aa33424b52d28b45b0cc44',
    registeredCommand:['/usr/bin/python3','/opt/agentos-control/agentos-release-action.py','validate'],publishedBaselineDigest:job.result.publishedBaselineDigest,publicationProofDigest:'f'.repeat(64),validationProofDigest:'f'.repeat(64),
    registeredReceiptSha:job.result.registeredReceiptSha,validatedReleaseDigest:'b'.repeat(64),
    log:{rawDigest:sha(log),rawBytes:log.length,digest:sha(log),bytes:log.length,sanitized:true,completeRegisteredArtifact:true,source:'registered_action_validation.log',commandStdoutMayBeTruncatedByRegisteredAction:true},acceptance:false,
    coverage:{version:1,source:'registered_vitest_json',profile:'agentos.validate',candidateDigest:f.candidateDigest,obligationsDigest:validationDigest(required),reportDigest:'c'.repeat(64),requiredTests:required.map(t=>({...t,status:'passed'}))}};
  change?.(authority);const proof=Buffer.from(JSON.stringify(canonicalValidationValue(authority)));
  return {version:1,actionId:job.actionId,requestDigest:authority.requestDigest,resultDigest:authority.resultDigest,receiptDigest:authority.receiptDigest,authority,inputs:[
    {name:'ci-receipt.json',mime:'application/json',digest:sha(proof),size:proof.length,base64:proof.toString('base64')},
    {name:'validation.log',mime:'text/plain',digest:sha(log),size:log.length,base64:log.toString('base64')}]};
}
it('requires stopped readiness, exact CI and an immutable acceptance; local readiness never accepts alone',async()=>{
  await admit();const original=(await run()).instruction_snapshot;
  expect(original.workerValidationContract).toBe(2);
  expect(workerValidationInstruction(await run())).toContain('pending_controller_ci');
  expect(taskDeliveryContract([f.criterion]).artifacts.map(a=>a.name).sort()).toEqual(['integration-readiness.json','local-evidence.json','validation-intent.json']);
  expect(await readCandidateReadinessProof(pool,f.run)).toMatchObject({runFence:f.fence,acceptance:false});
  expect(await validateTask(f.task)).toMatchObject({pending:true,passed:false});
  const job=await completed();expect(job.request.runtimeAuthority.version).toBe(1);
  expect(job.request.runtimeAuthority).not.toHaveProperty('readiness');
  await reconcileCandidateValidationEvidence(async()=>exported(job));
  const results=await Promise.all([validateTask(f.task),validateTask(f.task)]);
  expect(results.every(r=>r.passed)).toBe(true);
  expect((await pool.query('SELECT count(*)::int n FROM acceptances WHERE run_id=$1',[f.run])).rows[0].n).toBe(1);
  expect((await run()).instruction_snapshot).toEqual(original);
  expect((await readValidationSnapshot(pool,f.task))?.validation_input.version).toBe('deterministic-contract/candidate-readiness-v1');
});
it('cannot reuse an admission authorization from another transaction or fence',async()=>{
  await tx(async db=>{await lock(db);await recordCandidateReadinessAdmission(db,{id:f.task,attempt_count:0},f.run,f.fence,{consumerRelease:expected});});
  await expect(pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version) VALUES($1,$2,1,$3,'harness',1)",[f.run,f.task,f.fence])).rejects.toThrow('READINESS_ADMISSION_UNBOUND');
});
it.each(['removed','legacy-replacement'])('does not downgrade an applied readiness plan before its first admission: %s',async(mode)=>{
  const criteria=mode==='removed'?[]:[{id:'legacy',label:'Legacy report',type:'json_field',name:'delivery.json',field:'passed',equals:true}];
  await pool.query('UPDATE tasks SET criteria=$2 WHERE id=$1',[f.task,JSON.stringify(criteria)]);
  await expect(tx(async db=>{await lock(db);return recordCandidateReadinessAdmission(db,{id:f.task,attempt_count:0},f.run,f.fence,{consumerRelease:expected});}))
    .rejects.toMatchObject({code:'READINESS_PLAN_TASK_UNBOUND'});
  // An old consumer that lacks the new JS guard is refused by the database too.
  await expect(pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version) VALUES($1,$2,1,$3,'harness',1)",[f.run,f.task,f.fence]))
    .rejects.toThrow('READINESS_PLAN_TASK_UNBOUND');
  expect((await pool.query('SELECT count(*)::int n FROM runs WHERE task_id=$1',[f.task])).rows[0].n).toBe(0);
});
it.each(['failed-local','omitted-local','copied-digest','omitted-ci','extra-ci'])('rejects %s before any CI is dispatched',async(mode)=>{
  await admit({localPassed:mode!=='failed-local',report:r=>{
    if(mode==='omitted-local')r.localChecks=[];
    if(mode==='copied-digest')r.localChecks[0].digest='f'.repeat(64);
    if(mode==='omitted-ci')r.controllerChecks.tests=[];
    if(mode==='extra-ci')r.controllerChecks.tests=[...required,{file:required[0].file,name:'foreign test'}];
  }});
  await reconcileWorkerValidationIntents(inspect);expect((await handoff()).state).toBe('rejected');
  expect((await validateTask(f.task)).passed).toBe(false);
  expect((await pool.query("SELECT count(*)::int n FROM settings WHERE id LIKE 'candidate-validation:%'")).rows[0].n).toBe(0);
});
it('keeps a submitted candidate pending when physical stop is uncertain and reserves once after confirmation',async()=>{
  await admit();await reconcileWorkerValidationIntents(async()=>{throw Error('inspection unavailable');});
  expect(await handoff()).toMatchObject({state:'pending',code:'STOP_UNCONFIRMED'});
  await pool.query("UPDATE settings SET value=value-'nextProbeAt' WHERE id=$1",['worker-validation-handoff:'+f.run]);
  await Promise.all([reconcileWorkerValidationIntents(inspect),reconcileWorkerValidationIntents(inspect)]);
  expect((await handoff()).state).toBe('reserved');
  expect((await pool.query("SELECT count(*)::int n FROM settings WHERE id LIKE 'candidate-validation:%'")).rows[0].n).toBe(1);
});
it.each(['missing','wrong-candidate','wrong-obligations','skipped'])('does not accept %s registered CI coverage',async(mode)=>{
  await admit();const job=await completed();
  await reconcileCandidateValidationEvidence(async()=>exported(job,v=>{
    if(mode==='missing')delete v.coverage;
    if(mode==='wrong-candidate')v.coverage.candidateDigest='0'.repeat(64);
    if(mode==='wrong-obligations')v.coverage.obligationsDigest='0'.repeat(64);
    if(mode==='skipped')v.coverage.requiredTests[0].status='skipped';
  }));
  expect((await validateTask(f.task)).passed).toBe(false);
});
it('revoking the map after handoff prevents registered CI dispatch without rewriting the historical snapshot',async()=>{
  await admit();await reconcileWorkerValidationIntents(inspect);const before=(await run()).instruction_snapshot;
  await registerCandidateReadinessMap(f.goal,{...f.map,enabled:false},{channel:'api-token'});
  const dispatch=vi.fn();await reconcileCandidateValidations(dispatch,inspect);
  expect(dispatch).not.toHaveBeenCalled();expect((await workerValidationGate(pool,f.run)).state).toBe('failed');
  expect((await run()).instruction_snapshot).toEqual(before);
});
it('allows an authenticated operator session but never an observations-only principal to register a map',async()=>{
  const token='fixture-observation-'+randomUUID();
  vi.stubEnv('AGENTOS_OPERATIONS_ACCESS_JSON',JSON.stringify([{id:'fixture-observer',tokenSha256:sha(token),goalIds:[f.goal],projects:[],scopes:['observe']} ]));
  const app=makeApp();
  try{
    const rejected=await app.inject({method:'POST',url:'/api/objectives/'+f.goal+'/candidate-readiness',headers:{host:'localhost',authorization:'Bearer '+token},payload:f.map});
    expect(rejected.statusCode).toBe(401);
    const session=await app.inject({method:'GET',url:'/api/session',headers:{host:'localhost'}});
    const cookie=String(session.headers['set-cookie']).split(';')[0];
    const accepted=await app.inject({method:'POST',url:'/api/objectives/'+f.goal+'/candidate-readiness',headers:{host:'localhost',cookie},payload:f.map});
    expect(accepted.statusCode).toBe(200);expect(accepted.json().channel).toBe('operator-session');
  }finally{await app.close();}
});
