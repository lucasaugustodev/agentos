import {config} from '../server/config.js';
import * as service from '../server/service.js';
import {assembleContext} from '../server/memory.js';
import {acceptedDependencyArtifacts} from '../server/recovery.js';
import {bindCandidateReadinessQaPlan} from '../server/candidate-readiness-qa-contract.js';
import {recordCandidateReadinessQaAdmission,prepareCandidateReadinessQaContext,readCandidateReadinessQaProof} from '../server/candidate-readiness-qa.js';
import {afterAll,afterEach,beforeAll,beforeEach,expect,it,vi} from 'vitest';
import {createHash,randomUUID} from 'node:crypto';
import type {PoolClient} from 'pg';
import {pool,migrate,tx,lock} from '../server/db.js';
import {seedRegistry} from '../server/registry.js';
import {installManagement} from '../server/management.js';
import {writeArtifact,completeRun,validateTask} from '../server/service.js';
import {registerWorkerValidationPolicy,validationDigest,canonicalValidationValue,workerValidationInstruction,workerValidationGate} from '../server/worker-validation.js';
import {registerCandidateReadinessMap,bindCandidateReadinessPlan,recordCandidateReadinessAdmission,readCandidateReadinessProof,
  verifyBoundCandidateReadinessPlan} from '../server/candidate-readiness.js';
import {candidateReadinessMapSchema} from '../server/candidate-readiness-contract.js';
import {reconcileWorkerValidationIntents,reconcileCandidateValidations} from '../server/candidate-validation.js';
import {reconcileCandidateValidationEvidence} from '../server/candidate-validation-evidence.js';
import {taskDeliveryContract} from '../bridge/task-delivery-contract.mjs';
import {readValidationSnapshot} from '../server/validation-identity.js';
import {planSchema} from '../server/objective-contracts.js';
import {assertIncrementalAuthority} from '../server/incremental-planning.js';

// Real PostgreSQL, admission triggers, artifacts, acceptance CAS and canonical
// evidence parser. Physical inspector and registered external CI are controlled
// fixture boundaries: this suite does not claim a production build was run.
const expected={candidate:'/opt/agentos/releases/managed-11111111-1111-4111-8111-111111111111',digest:'a'.repeat(64)};
const required=[{file:'tests/transaction-pg.test.ts',name:'transaction rejects a stale writer'}];
const policy={version:1,enabled:true,project:'agentos',profile:'agentos.validate',stages:['integrate'],required:true,requiredDatabaseTests:required};
const description='Preserve all inherited source and independently validate the candidate.';
const sha=(value:string|Buffer)=>createHash('sha256').update(value).digest('hex');
let f:any,owner:PoolClient|undefined;
beforeAll(async()=>{expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');await migrate();await installManagement();});
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
  plan.tasks.push({...plan.tasks[0],key:'verify',title:'Independent QA',stage:'verify',sourceTask:'integrate',dependencies:['integrate'],
    criteria:[{id:'verified',label:'Independent checks',type:'json_field',name:'verification.json',field:'passed',equals:true}]});
  f.originalPlan=structuredClone(plan);f.revision=randomUUID();f.planning=randomUUID();
  f.plan=await tx(async db=>{await lock(db);return bindCandidateReadinessPlan(db,f.goal,plan,{consumerRelease:expected});});
  f.criterion=f.plan.tasks[0].criteria[0];
  await pool.query("INSERT INTO planning_runs(id,objective_id,base_revision,trigger_key,state,fence) VALUES($1,$2,0,'fixture','completed',$3)",[f.planning,f.goal,randomUUID()]);
  await pool.query("INSERT INTO plan_revisions(id,objective_id,revision,planning_run_id,plan,status) VALUES($1,$2,1,$3,$4,'applied')",[f.revision,f.goal,f.planning,JSON.stringify(f.plan)]);
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,attempt_count,covers,recovery)
    VALUES($1,$2,'integrate','Integrate candidate',$3,'engineering.integration','ready','integrate',$4,$5,0,'["req-1"]','{"mode":"automatic"}')`,
    [f.task,f.goal,f.plan.tasks[0].description,JSON.stringify([f.criterion]),JSON.stringify({kind:'harness',workspace:'agentos/oreadinessflow',baseWorkspace:'agentos/base',readOnlySource:false})]);
  await pool.query("UPDATE tasks SET key='r1-integrate',plan_revision=1 WHERE id=$1",[f.task]);
  f.qa={task:randomUUID(),run:randomUUID(),fence:randomUUID(),context:randomUUID()};
  const qa=f.plan.tasks.find((t:any)=>t.key==='verify');
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,attempt_count,covers,recovery,plan_revision)
    VALUES($1,$2,'r1-verify','Independent QA',$3,'qa.integration','blocked_dependencies','verify',$4,$5,0,'["req-1"]','{"mode":"automatic"}',$6)`,
    [f.qa.task,f.goal,qa.description,JSON.stringify(qa.criteria),JSON.stringify({kind:'harness',workspace:'agentos/oreadinessqa',sourceWorkspace:'agentos/oreadinessflow',baseWorkspace:'agentos/base',readOnlySource:true}),1]);
  await pool.query('INSERT INTO dependencies(task_id,depends_on) VALUES($1,$2)',[f.qa.task,f.task]);
});
afterEach(async()=>{if(owner){await owner.query('SELECT pg_advisory_unlock_all()');owner.release();owner=undefined;}vi.restoreAllMocks();});
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
async function acceptedIntegration(){
  await admit();const job=await completed();
  await reconcileCandidateValidationEvidence(async()=>exported(job));
  expect((await validateTask(f.task)).passed).toBe(true);return job;
}
async function configureThreeOfSixBatch(){
  const requirements=Array.from({length:6},(_,i)=>({id:'req-'+(i+1),description:i?`Preserve independently verified objective requirement ${i+1}.`:description}));
  const covers=['req-1','req-2','req-3'],pending=['req-4','req-5','req-6'];
  const plan=structuredClone(f.originalPlan);
  plan.requirements=requirements;
  plan.incremental={version:1,selectedRequirementId:'req-1',selectedRequirementIds:covers,completedRequirementIds:[],pendingRequirementIds:pending};
  for(const task of plan.tasks)task.covers=covers;
  await pool.query('UPDATE objectives SET request=$2 WHERE id=$1',[f.goal,JSON.stringify({project:'agentos',requirements:requirements.map(r=>r.description)})]);
  await pool.query('UPDATE plan_revisions SET plan=$2 WHERE id=$1',[f.revision,JSON.stringify(plan)]);
  f.map={...f.map,scope:{kind:'batch',originalRequirements:requirements.map(r=>({id:r.id,digest:validationDigest(r.description)}))},
    requirements:requirements.slice(0,3).map(r=>({id:r.id,digest:validationDigest(r.description)})),
    obligations:f.map.obligations.map((o:any)=>({...o,requirements:covers}))};
  f.registration=await registerCandidateReadinessMap(f.goal,f.map,{channel:'operator-session'});
  f.originalPlan=plan;
  f.plan=await tx(async db=>{await lock(db);return bindCandidateReadinessPlan(db,f.goal,plan,{consumerRelease:expected});});
  f.criterion=f.plan.tasks[0].criteria[0];
  await pool.query('UPDATE plan_revisions SET plan=$2 WHERE id=$1',[f.revision,JSON.stringify(f.plan)]);
  for(const [key,id] of [['integrate',f.task],['verify',f.qa.task]]){
    const task=f.plan.tasks.find((t:any)=>t.key===key);
    await pool.query('UPDATE tasks SET description=$2,criteria=$3,covers=$4 WHERE id=$1',[id,task.description,JSON.stringify(task.criteria),JSON.stringify(covers)]);
  }
  return {requirements,covers,pending};
}
async function admitQa(options:{localPassed?:boolean,changedCandidate?:boolean,submit?:boolean,realContext?:boolean}={}){
  await tx(async db=>{
    await lock(db);
    await recordCandidateReadinessQaAdmission(db,{id:f.qa.task},f.qa.run,f.qa.fence,{consumerRelease:expected});
    await db.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,context_id) VALUES($1,$2,1,$3,'harness',1,'running',$4)",
      [f.qa.run,f.qa.task,f.qa.fence,options.realContext?null:f.qa.context]);
    await db.query("UPDATE tasks SET state='running',attempt_count=1 WHERE id=$1",[f.qa.task]);
    if(options.realContext)return;
    const run=(await db.query('SELECT * FROM runs WHERE id=$1',[f.qa.run])).rows[0];
    const task=(await db.query('SELECT * FROM tasks WHERE id=$1',[f.qa.task])).rows[0];
    const prepared=await prepareCandidateReadinessQaContext(db,run,task);
    expect(prepared).not.toBeNull();
    await db.query('INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,$3,$4)',
      [f.qa.context,f.qa.run,JSON.stringify({candidateReadinessQa:prepared!.manifest}),prepared!.text]);
  });
  if(options.realContext){
    const run=(await pool.query('SELECT * FROM runs WHERE id=$1',[f.qa.run])).rows[0];
    const task=(await pool.query('SELECT * FROM tasks WHERE id=$1',[f.qa.task])).rows[0];
    const context=await assembleContext(run,task,await acceptedDependencyArtifacts(task.id));
    f.qa.context=context.id;
  }
  await writeArtifact(f.qa.run,f.qa.fence,'local-evidence.json',JSON.stringify({passed:options.localPassed??true,observed:'Independent local fixture assertion'}));
  await writeArtifact(f.qa.run,f.qa.fence,'verification.json',JSON.stringify({passed:true,ci:'Inspected the controller-verified inherited receipt; did not run DB locally.'}));
  const files=options.changedCandidate?[{path:'src/changed.ts',baseSha256:null,candidateSha256:sha('unreviewed source'),status:'added'}]:[];
  const candidateDigest=options.changedCandidate?sha('different candidate'):f.candidateDigest;
  await writeArtifact(f.qa.run,f.qa.fence,'candidate.json',JSON.stringify({baseDigest:f.candidateDigest,candidateDigest,files}));
  if(options.submit!==false)await completeRun(f.qa.run,f.qa.fence,'Independent local checks with inherited canonical CI.');
}
it('carries canonical CI into independent QA context and accepts only the same source with its own local checks',async()=>{
  const job=await acceptedIntegration();await admitQa();
  const stored=(await pool.query('SELECT manifest,content FROM contexts WHERE id=$1',[f.qa.context])).rows[0];
  expect(stored.manifest.candidateReadinessQa).toMatchObject({source:{taskId:f.task,runId:f.run,candidateDigest:f.candidateDigest},
    ci:{actionId:job.actionId,coverage:{requiredTests:required.map(t=>({...t,status:'passed'}))}}});
  expect(stored.content).toContain(job.actionId);expect(stored.content).toContain(required[0].name);
  expect((await pool.query('SELECT instruction_snapshot FROM runs WHERE id=$1',[f.qa.run])).rows[0].instruction_snapshot.workerValidation).toBeNull();
  expect(await readCandidateReadinessQaProof(pool,f.qa.run)).toMatchObject({manifest:{source:{runId:f.run}},acceptance:false});
  const result=await validateTask(f.qa.task);expect(result.passed).toBe(true);
  expect(result.checks.find(c=>c.type==='candidate_ci')?.passed).toBe(true);
  expect(result.checks.find(c=>c.id==='verified')?.passed).toBe(true);
  expect((await pool.query("SELECT count(*)::int n FROM settings WHERE id LIKE 'candidate-validation:%'")).rows[0].n).toBe(1);
  expect((await pool.query('SELECT state FROM goals WHERE id=$1',[f.goal])).rows[0].state).toBe('completed');
});
it('accepts the exact three-requirement batch through CI and QA while all six original requirements remain and the goal stays active',async()=>{
  const {requirements,covers,pending}=await configureThreeOfSixBatch();
  expect(f.registration.requiredDatabaseTests).toEqual(required);
  expect(f.registration.policyDigest).toBe(f.policy.policyDigest);
  const job=await acceptedIntegration();await admitQa({realContext:true});
  const sourceRun=await run();
  expect(sourceRun.instruction_snapshot.candidateReadiness.registration.contract.scope.originalRequirements)
    .toEqual(f.map.scope.originalRequirements);
  expect(sourceRun.instruction_snapshot.workerValidation.policy.requiredDatabaseTests).toEqual(required);
  const result=await validateTask(f.qa.task);expect(result.passed).toBe(true);
  const latest=(await pool.query('SELECT plan FROM plan_revisions WHERE id=$1',[f.revision])).rows[0].plan;
  expect(latest.requirements).toEqual(requirements);expect(latest.incremental.pendingRequirementIds).toEqual(pending);
  expect(latest.incremental.completedRequirementIds).toEqual([]);
  expect((await pool.query('SELECT state FROM goals WHERE id=$1',[f.goal])).rows[0].state).toBe('active');
  const accepted=(await pool.query("SELECT t.covers,a.passed FROM acceptances a JOIN tasks t ON t.id=a.task_id WHERE t.goal_id=$1",[f.goal])).rows;
  expect(accepted).toHaveLength(2);expect(accepted.every(r=>r.passed&&JSON.stringify(r.covers)===JSON.stringify(covers))).toBe(true);
  expect((await pool.query("SELECT count(*)::int n FROM settings WHERE id LIKE 'candidate-validation:%'")).rows[0].n).toBe(1);
  expect(job.request.runtimeAuthority.policyRegistration.policy.requiredDatabaseTests).toEqual(required);
});
it.each(['remove-universe','change-universe','change-universe-id','expand-covers','drop-pending','change-selected'])(
  'refuses batch plan tampering: %s',async(mode)=>{
    await configureThreeOfSixBatch();const plan=structuredClone(f.originalPlan);
    if(mode==='remove-universe')plan.requirements.pop();
    if(mode==='change-universe')plan.requirements[5].description+=' changed';
    if(mode==='change-universe-id')plan.requirements[5].id='req-another';
    if(mode==='expand-covers')plan.tasks[0].covers.push('req-4');
    if(mode==='drop-pending')plan.incremental.pendingRequirementIds=[];
    if(mode==='change-selected')plan.incremental.selectedRequirementIds=['req-1','req-2'];
    await expect(tx(async db=>{await lock(db);return bindCandidateReadinessPlan(db,f.goal,plan,{consumerRelease:expected});})).rejects.toMatchObject({code:expect.stringMatching(/^READINESS_(OBLIGATIONS_UNMAPPED|BATCH_PARTITION_UNBOUND)$/)});
    expect((await pool.query('SELECT count(*)::int n FROM runs WHERE task_id=$1',[f.task])).rows[0].n).toBe(0);
  });
it('refuses a changed original requirement after registration before admission and preserves accepted CI history after later drift',async()=>{
  await configureThreeOfSixBatch();const before=structuredClone(f.plan);
  const altered=structuredClone(f.plan);altered.requirements[5].description+=' changed';
  await pool.query('UPDATE plan_revisions SET plan=$2 WHERE id=$1',[f.revision,JSON.stringify(altered)]);
  await expect(admit()).rejects.toMatchObject({code:'READINESS_REQUIREMENTS_CHANGED'});
  await pool.query('UPDATE plan_revisions SET plan=$2 WHERE id=$1',[f.revision,JSON.stringify(before)]);
  await acceptedIntegration();await admitQa();
  const frozen=(await run()).instruction_snapshot;
  await pool.query('UPDATE plan_revisions SET plan=$2 WHERE id=$1',[f.revision,JSON.stringify(altered)]);
  expect((await validateTask(f.qa.task)).passed).toBe(false);
  expect((await run()).instruction_snapshot).toEqual(frozen);
  expect((await pool.query('SELECT count(*)::int n FROM acceptances WHERE run_id=$1 AND passed',[f.run])).rows[0].n).toBe(1);
  expect((await pool.query('SELECT count(*)::int n FROM acceptances WHERE run_id=$1 AND passed',[f.qa.run])).rows[0].n).toBe(0);
});
it('does not let a saved batch plan remove readiness and fall back to a legacy passed boolean',async()=>{
  await configureThreeOfSixBatch();const stripped=structuredClone(f.plan);
  stripped.tasks[0].criteria=structuredClone(f.originalPlan.tasks[0].criteria);
  await expect(tx(async db=>{await lock(db);return verifyBoundCandidateReadinessPlan(db,f.goal,stripped);})).rejects.toMatchObject({code:'READINESS_BATCH_CRITERION_REMOVED'});
});
it('binds only the final accumulated integration when intermediate source joins cover the same three-of-six batch',async()=>{
  const {requirements,covers,pending}=await configureThreeOfSixBatch();
  const plan=structuredClone(f.originalPlan),integration=plan.tasks.find((t:any)=>t.key==='integrate');
  const producer={...structuredClone(integration),key:'source',stage:'work',agentId:'engineering.backend.node'};
  const join={...structuredClone(integration),key:'source-join',dependencies:['source']};
  const dependent={...structuredClone(producer),key:'dependent',sourceTask:'source-join',dependencies:['source-join']};
  integration.dependencies=['source','dependent'];
  plan.tasks.unshift(producer,join,dependent);
  const before=structuredClone(plan);
  const bound=await tx(async db=>{await lock(db);return bindCandidateReadinessPlan(db,f.goal,plan,{consumerRelease:expected});});
  expect(plan).toEqual(before);
  expect(bound.tasks.filter(t=>t.criteria.some(c=>c.type==='candidate_readiness')).map(t=>t.key)).toEqual(['integrate']);
  expect(bound.tasks.find(t=>t.key==='source-join')).toEqual(join);
  expect(bound.tasks.find(t=>t.key==='verify')?.criteria).toContainEqual(expect.objectContaining({type:'candidate_ci',sourceTaskKey:'integrate'}));
  expect(bound.requirements).toEqual(requirements);
  expect(bound.incremental).toMatchObject({selectedRequirementIds:covers,pendingRequirementIds:pending,completedRequirementIds:[]});
  const stripped=structuredClone(bound);
  stripped.tasks.find(t=>t.key==='integrate')!.criteria=integration.criteria;
  await expect(tx(async db=>{await lock(db);return verifyBoundCandidateReadinessPlan(db,f.goal,stripped);})).rejects.toMatchObject({code:'READINESS_BATCH_CRITERION_REMOVED'});
  expect((await pool.query('SELECT count(*)::int n FROM runs')).rows[0].n).toBe(0);
});
it('refuses an intermediate source branch outside the registered batch even when the final integration covers only that batch',async()=>{
  await configureThreeOfSixBatch();const plan=structuredClone(f.originalPlan);
  const integration=plan.tasks.find((t:any)=>t.key==='integrate');
  plan.tasks.unshift({...structuredClone(integration),key:'foreign-source',stage:'work',agentId:'engineering.backend.node',covers:['req-4']});
  integration.dependencies=['foreign-source'];
  await expect(tx(async db=>{await lock(db);return bindCandidateReadinessPlan(db,f.goal,plan,{consumerRelease:expected});}))
    .rejects.toMatchObject({code:'READINESS_OBLIGATIONS_UNMAPPED'});
  expect((await pool.query('SELECT count(*)::int n FROM runs')).rows[0].n).toBe(0);
});
it('preserves the canonical incremental authority: a batch cannot move an unverified outside requirement to completed',async()=>{
  await configureThreeOfSixBatch();const changed=structuredClone(f.originalPlan);
  changed.incremental.completedRequirementIds=['req-4'];changed.incremental.pendingRequirementIds=['req-5','req-6'];
  const bound=await tx(async db=>{await lock(db);return bindCandidateReadinessPlan(db,f.goal,changed,{consumerRelease:expected});});
  const previous=(await pool.query('SELECT plan FROM plan_revisions WHERE id=$1',[f.revision])).rows[0].plan;
  await expect(assertIncrementalAuthority(pool,{objective_id:f.goal,feedback:{reports:[]},
    request:{requirements:previous.requirements.map((r:any)=>r.description)}},bound,previous)).rejects.toMatchObject({code:'PLAN_INVALID'});
  expect((await pool.query('SELECT plan FROM plan_revisions WHERE id=$1',[f.revision])).rows[0].plan.incremental.completedRequirementIds).toEqual([]);
});
it('does not let a saved batch plan remove inherited CI from its independent QA',async()=>{
  await configureThreeOfSixBatch();const stripped=structuredClone(f.plan);
  stripped.tasks.find((t:any)=>t.key==='verify').criteria=stripped.tasks.find((t:any)=>t.key==='verify').criteria.filter((c:any)=>c.type!=='candidate_ci');
  await expect(tx(async db=>{await lock(db);return verifyBoundCandidateReadinessPlan(db,f.goal,stripped);})).rejects.toMatchObject({code:'READINESS_BATCH_QA_REMOVED'});
});
it('keeps the absent-scope contract and digest byte-compatible and rejects implicit subsets or mismatched universe identities',async()=>{
  const legacy=structuredClone(f.map);
  expect(candidateReadinessMapSchema.parse(legacy)).toEqual(legacy);
  expect(Object.hasOwn(candidateReadinessMapSchema.parse(legacy),'scope')).toBe(false);
  expect(validationDigest(candidateReadinessMapSchema.parse(legacy))).toBe(validationDigest(legacy));
  await configureThreeOfSixBatch();
  const implicit=structuredClone(f.map);delete implicit.scope;
  await expect(registerCandidateReadinessMap(f.goal,implicit,{channel:'operator-session'})).rejects.toMatchObject({code:'READINESS_REQUIREMENTS_CHANGED'});
  const changed=structuredClone(f.map);changed.requirements[0].digest='f'.repeat(64);
  await expect(registerCandidateReadinessMap(f.goal,changed,{channel:'operator-session'})).rejects.toMatchObject({code:'READINESS_BATCH_REQUIREMENTS_UNBOUND'});
  const duplicated=structuredClone(f.map);duplicated.scope.originalRequirements.push(duplicated.scope.originalRequirements[0]);
  await expect(registerCandidateReadinessMap(f.goal,duplicated,{channel:'operator-session'})).rejects.toMatchObject({code:'READINESS_BATCH_REQUIREMENTS_UNBOUND'});
  const replaced=structuredClone(f.map);replaced.scope.originalRequirements[5].id='req-another';
  await expect(registerCandidateReadinessMap(f.goal,replaced,{channel:'operator-session'})).rejects.toMatchObject({code:'READINESS_REQUIREMENTS_CHANGED'});
  const outside=structuredClone(f.map);outside.requirements[0].id='req-outside';
  await expect(registerCandidateReadinessMap(f.goal,outside,{channel:'operator-session'})).rejects.toMatchObject({code:'READINESS_BATCH_REQUIREMENTS_UNBOUND'});
});
it('assembles the real initial QA context and artifact contract before any model execution',async()=>{
  const job=await acceptedIntegration();await admitQa({realContext:true,submit:false});
  const row=(await pool.query('SELECT * FROM runs WHERE id=$1',[f.qa.run])).rows[0];
  const context=(await pool.query('SELECT manifest,content FROM contexts WHERE id=$1',[row.context_id])).rows[0];
  const task=(await pool.query('SELECT * FROM tasks WHERE id=$1',[f.qa.task])).rows[0];
  expect(row.state).toBe('running');expect(row.context_id).toBe(f.qa.context);
  expect(context.manifest.candidateReadinessQa).toMatchObject({consumer:{runId:f.qa.run,runFence:f.qa.fence},
    source:{runId:f.run,candidateDigest:f.candidateDigest},ci:{actionId:job.actionId}});
  expect(context.content).toContain('CANONICAL CI FOR THE EXACT ACCEPTED READ-ONLY SOURCE');
  expect(context.content).toContain(required[0].name);expect(context.content).toContain(job.actionId);
  expect(context.manifest.dependencyArtifacts.some((a:any)=>a.task_id===f.task&&a.name==='candidate.json'&&
    a.id===context.manifest.candidateReadinessQa.source.candidate.id)).toBe(true);
  expect(workerValidationInstruction(row)).toContain('INHERITED CANONICAL CI');
  expect(workerValidationInstruction(row)).not.toContain('write evidence/validation-intent.json');
  expect(taskDeliveryContract(task.criteria).artifacts.map(a=>a.name).sort()).toEqual(['local-evidence.json','verification.json']);
  expect((await pool.query("SELECT count(*)::int n FROM events WHERE entity_id=$1 AND kind LIKE 'MODEL_%'",[f.qa.run])).rows[0].n).toBe(0);
});
it.each(['local-failure','candidate-change'])('does not turn inherited CI into acceptance of QA %s',async(mode)=>{
  await acceptedIntegration();await admitQa({localPassed:mode!=='local-failure',changedCandidate:mode==='candidate-change'});
  const result=await validateTask(f.qa.task);expect(result.passed).toBe(false);
  expect(result.checks.find(c=>c.type==='candidate_ci')?.passed).toBe(false);
  expect(result.checks.find(c=>c.id==='verified')?.passed).toBe(true);
});
it('rejects a changed accepted producer after the QA context was frozen',async()=>{
  await acceptedIntegration();await admitQa();
  await pool.query("UPDATE tasks SET description=description||' changed after admission' WHERE id=$1",[f.task]);
  await expect(readCandidateReadinessQaProof(pool,f.qa.run)).rejects.toMatchObject({code:'READINESS_QA_SOURCE_CHANGED'});
  expect((await validateTask(f.qa.task)).passed).toBe(false);
});
it('a worker copied CI receipt cannot restore a revoked canonical policy',async()=>{
  const job=await acceptedIntegration();await admitQa({submit:false});
  const copied=exported(job).inputs[0];
  const historical=(await pool.query('SELECT value FROM settings WHERE id=$1',['candidate-validation-evidence:'+job.actionId])).rows[0].value;
  await writeArtifact(f.qa.run,f.qa.fence,'ci-receipt.json',Buffer.from(copied.base64,'base64'));
  await registerWorkerValidationPolicy(f.goal,{...policy,enabled:false},{channel:'operator-session'});
  await completeRun(f.qa.run,f.qa.fence,'Copied report is untrusted evidence.');
  expect((await validateTask(f.qa.task)).passed).toBe(false);
  expect((await pool.query('SELECT count(*)::int n FROM acceptances WHERE run_id=$1 AND passed',[f.qa.run])).rows[0].n).toBe(0);
  expect((await pool.query('SELECT value FROM settings WHERE id=$1',['candidate-validation-evidence:'+job.actionId])).rows[0].value).toEqual(historical);
});
it('does not inherit CI from a different source workspace or missing dependency',async()=>{
  await acceptedIntegration();
  await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{sourceWorkspace}','\"agentos/another-source\"') WHERE id=$1",[f.qa.task]);
  await expect(tx(async db=>{await lock(db);await recordCandidateReadinessQaAdmission(db,{id:f.qa.task},f.qa.run,f.qa.fence,{consumerRelease:expected});}))
    .rejects.toMatchObject({code:'READINESS_QA_SOURCE_UNBOUND'});
  await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{sourceWorkspace}','\"agentos/oreadinessflow\"') WHERE id=$1",[f.qa.task]);
  await pool.query('DELETE FROM dependencies WHERE task_id=$1',[f.qa.task]);
  await expect(tx(async db=>{await lock(db);await recordCandidateReadinessQaAdmission(db,{id:f.qa.task},f.qa.run,f.qa.fence,{consumerRelease:expected});}))
    .rejects.toMatchObject({code:'READINESS_QA_SOURCE_UNBOUND'});
});
it('refuses to suppress an independently required verify policy or accept a model predeclared QA criterion',async()=>{
  const plan=structuredClone(f.plan);
  plan.tasks.find((t:any)=>t.key==='verify').criteria=plan.tasks.find((t:any)=>t.key==='verify').criteria.filter((c:any)=>c.type!=='candidate_ci');
  expect(()=>bindCandidateReadinessQaPlan(plan,{...f.registration,policyStages:['integrate','verify']})).toThrow('READINESS_QA_POLICY_CONFLICT');
  expect(()=>bindCandidateReadinessQaPlan(f.plan,{...f.registration,policyStages:['integrate']})).toThrow('READINESS_QA_PREDECLARED');
});
it('does not admit a copied QA criterion after its plan binding receipt disappears',async()=>{
  await acceptedIntegration();
  await pool.query("DELETE FROM events WHERE kind='CANDIDATE_READINESS_PLAN_BOUND' AND entity_id=$1",[f.goal]);
  await expect(tx(async db=>{await lock(db);await recordCandidateReadinessQaAdmission(db,{id:f.qa.task},f.qa.run,f.qa.fence,{consumerRelease:expected});}))
    .rejects.toMatchObject({code:'READINESS_QA_PLAN_UNBOUND'});
});
it('cannot remove the inherited CI criterion from an admitted QA and fall back to a legacy passed boolean',async()=>{
  await acceptedIntegration();await admitQa();
  const before=(await pool.query('SELECT instruction_snapshot FROM runs WHERE id=$1',[f.qa.run])).rows[0].instruction_snapshot;
  await pool.query(`UPDATE tasks SET criteria=(SELECT jsonb_agg(c) FROM jsonb_array_elements(criteria)c WHERE c->>'type'<>'candidate_ci') WHERE id=$1`,[f.qa.task]);
  await expect(validateTask(f.qa.task)).rejects.toMatchObject({code:'STATE_CHANGED'});
  expect((await pool.query('SELECT count(*)::int n FROM acceptances WHERE run_id=$1 AND passed',[f.qa.run])).rows[0].n).toBe(0);
  expect((await pool.query('SELECT instruction_snapshot FROM runs WHERE id=$1',[f.qa.run])).rows[0].instruction_snapshot).toEqual(before);
});
it('refuses a removed QA criterion before the first run in both the current admission and an old consumer INSERT',async()=>{
  await acceptedIntegration();
  await pool.query(`UPDATE tasks SET criteria=(SELECT jsonb_agg(c) FROM jsonb_array_elements(criteria)c WHERE c->>'type'<>'candidate_ci') WHERE id=$1`,[f.qa.task]);
  await expect(tx(async db=>{await lock(db);await recordCandidateReadinessQaAdmission(db,{id:f.qa.task},f.qa.run,f.qa.fence,{consumerRelease:expected});}))
    .rejects.toMatchObject({code:'READINESS_PLAN_TASK_UNBOUND'});
  await expect(pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version) VALUES($1,$2,1,$3,'harness',1)",[f.qa.run,f.qa.task,f.qa.fence]))
    .rejects.toThrow('READINESS_PLAN_TASK_UNBOUND');
  expect((await pool.query('SELECT count(*)::int n FROM runs WHERE task_id=$1',[f.qa.task])).rows[0].n).toBe(0);
});
it.each(['source-contract','dependency','plan-binding'])('refuses %s change during final QA byte reads instead of committing a stale acceptance',async(mode)=>{
  await acceptedIntegration();await admitQa();
  const local=(await pool.query("SELECT id FROM artifacts WHERE run_id=$1 AND name='local-evidence.json'",[f.qa.run])).rows[0];
  const read=service.readArtifact;let changed=false;
  vi.spyOn(service,'readArtifact').mockImplementation(async id=>{
    const value=await read(id);
    if(id===local.id&&!changed){
      changed=true;
      if(mode==='source-contract')await pool.query("UPDATE tasks SET description=description||' concurrent change' WHERE id=$1",[f.task]);
      if(mode==='dependency')await pool.query('DELETE FROM dependencies WHERE task_id=$1',[f.qa.task]);
      if(mode==='plan-binding')await pool.query("DELETE FROM events WHERE kind='CANDIDATE_READINESS_PLAN_BOUND' AND entity_id=$1",[f.goal]);
    }
    return value;
  });
  await expect(validateTask(f.qa.task)).rejects.toMatchObject({code:'STATE_CHANGED'});
  expect(changed).toBe(true);
  expect((await pool.query('SELECT count(*)::int n FROM acceptances WHERE run_id=$1 AND passed',[f.qa.run])).rows[0].n).toBe(0);
});
