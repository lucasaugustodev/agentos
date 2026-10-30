import {beforeAll,beforeEach,afterAll,it,expect} from 'vitest';
import {randomUUID,createHash} from 'node:crypto';
import {pool,tx,lock} from '../server/db.js';
import {config} from '../server/config.js';
import {prepareSchema} from '../server/schema-readiness.js';
import {prepareBacklogSchema} from '../server/backlog-schema-readiness.js';
import {installTaskExecutionMetadata} from '../server/task-execution-metadata.js';
import {installRunCriteriaMetadata} from '../server/run-criteria-metadata.js';
import {installObjectiveInputs,registerObjectiveInput} from '../server/objective-inputs.js';
import {seedRegistry} from '../server/registry.js';
import {completeRun,writeArtifact,validateTask,goalDetail} from '../server/service.js';
import {requirementContractsSchema,requirementOutcomeCriterion} from '../server/requirement-outcomes.js';
import {recordManagementReviews} from '../server/management.js';
import {readObjectiveFrontier} from '../server/work-frontier.js';
import {queueNextDelivery,verifiedBatch,assertIncrementalAuthority,compileDelivery} from '../server/incremental-planning.js';
import {reconcilePlanning} from '../server/planner.js';
import {objectiveSchema} from '../server/objective-contracts.js';

const hash=(b:string|Buffer)=>createHash('sha256').update(b).digest('hex');
const contracts=requirementContractsSchema.parse([{id:'req-1',externalDependencies:[{key:'external-window',kind:'external_event',name:'external-event.json',field:'available',equals:true}],observables:[{id:'result',name:'result.json',authority:'current_run',
  type:'json_field',field:'delivered',equals:true,description:'The actual requested product was delivered'}]}]);
const gate=requirementOutcomeCriterion(contracts);
beforeAll(async()=>{
  expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');
  await prepareSchema();await prepareBacklogSchema();await installTaskExecutionMetadata();await installRunCriteriaMetadata();await installObjectiveInputs();
});
beforeEach(async()=>{
  await pool.query('TRUNCATE goals,agents,accounts,memories,events CASCADE');
  await pool.query("DELETE FROM settings WHERE id LIKE 'validation-receipt:%' OR id LIKE 'workforce:%'");
  await seedRegistry();
});
afterAll(()=>pool.end());
async function fixture(status:'completed'|'partial'|'externally_blocked'='externally_blocked'){
  const goal=randomUUID(),work=randomUUID(),task=randomUUID(),run=randomUUID(),fence=randomUUID(),workRun=randomUUID(),workFence=randomUUID(),planning=randomUUID();
  const request=objectiveSchema.parse({message:'Prepare and deliver a concrete external product',project:'outcome',workspaces:['outcome/base'],
    mode:'execute',requirements:['Deliver the real external result, not merely its offline preparation'],planningStrategy:'incremental',
    completionContractVersion:2,idempotencyKey:goal});
  const plan:any={title:'External product',summary:'Separate preparation from final delivery',completionContractVersion:2,requirementContracts:contracts,
    requirements:[{id:'req-1',description:request.requirements[0]}],tasks:[],
    incremental:{version:1,selectedRequirementId:'req-1',completedRequirementIds:[],pendingRequirementIds:[]}};
  await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1::uuid,'External product','Deliver actual result','outcome',$1::text)",[goal]);
  await pool.query("INSERT INTO objectives(id,request,request_hash,mode,status,revision) VALUES($1,$2,'test','execute','executing',1)",[goal,JSON.stringify(request)]);
  await pool.query("INSERT INTO planning_runs(id,objective_id,base_revision,trigger_key,state,fence,finished_at) VALUES($1,$2,0,'initial','completed',$3,now())",[planning,goal,randomUUID()]);
  await pool.query("INSERT INTO plan_revisions(id,objective_id,revision,planning_run_id,plan,status) VALUES($1,$2,1,$3,$4,'applied')",[randomUUID(),goal,planning,JSON.stringify(plan)]);
  for(const [id,rid,rf,key,stage,criteria,execution] of [
    [work,workRun,workFence,'work','work',[{id:'gate',type:'artifact',label:'Offline gate',name:'gate.txt'}],{kind:'harness',workspace:'outcome/work'}],
    [task,run,fence,'verify','verify',[gate],{kind:'harness',workspace:'outcome/verify',sourceWorkspace:'outcome/work'}],
  ] as const){
    await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria,attempt_count,stage,plan_revision,covers,execution)
      VALUES($1,$2,$3,'Product verification','Verify actual evidence','qa.integration','running',$4,1,$5,1,'["req-1"]',$6)`,
      [id,goal,key,JSON.stringify(criteria),stage,JSON.stringify(execution)]);
    await pool.query("INSERT INTO runs(id,task_id,attempt,state,fence,runtime,agent_version) VALUES($1,$2,1,'running',$3,'harness',1)",[rid,id,rf]);
  }
  await pool.query('INSERT INTO dependencies(task_id,depends_on) VALUES($1,$2)',[task,work]);
  const files=[{path:'src/gate.txt',sha256:hash('offline gate')}],inventory=hash(JSON.stringify(files.map(f=>[f.path,f.sha256])));
  const candidate={baseDigest:inventory,candidateDigest:inventory,baseFiles:files,files};
  await writeArtifact(workRun,workFence,'candidate.json',JSON.stringify(candidate));
  await writeArtifact(workRun,workFence,'gate.txt','Offline gate implemented');
  await completeRun(workRun,workFence,'Local preparation ready',{});await validateTask(work);
  await writeArtifact(run,fence,'candidate.json',JSON.stringify(candidate));
  const result=JSON.stringify({delivered:true});await writeArtifact(run,fence,'result.json',result);
  await writeArtifact(run,fence,'requirement-outcomes.json',JSON.stringify({version:2,outcomes:[{
    requirementId:'req-1',status,scope:status==='completed'?'full':'partial',
    observations:status==='completed'?[{observableId:'result',name:'result.json',digest:hash(result)}]:[],
    ...(status==='externally_blocked'?{dependency:{key:'external-window',kind:'external_event',reason:'External service window remains closed'}}:{}),
  }]}));
  await completeRun(run,fence,'Independent gate observation',{});
  if(status==='completed'){
    const bytes=Buffer.from(JSON.stringify({available:true}));
    await registerObjectiveInput(goal,{requestId:randomUUID(),expectedRevision:1,name:'external-event.json',digest:hash(bytes),base64:bytes.toString('base64'),requirementIds:['req-1'],sourceRef:'operator:test-event',reason:'The externally required event was supplied by the authenticated operator',requestReplan:false},{channel:'api-token'});
  }
  return {goal,task,run,fence,work,workRun,plan,request};
}
const next=(f:Awaited<ReturnType<typeof fixture>>)=>tx(async db=>{await lock(db);return queueNextDelivery(db,{id:f.goal,revision:1,request:f.request},f.plan);});

it('keeps a canonically accepted gate and verifier while the v2 goal waits for an external result',async()=>{
  const f=await fixture();expect((await validateTask(f.task)).passed).toBe(true);
  const detail=await goalDetail(f.goal);
  expect(detail.tasks.every(t=>t.state==='accepted')).toBe(true);
  expect(detail.goal.state).toBe('active');expect(detail.frontier.complete).toBe(false);
  expect(detail.frontier.requirements[0].state).toBe('externally_blocked');
  expect(detail.frontier.next).toBe('await_external_dependency');
  expect((await pool.query('SELECT reason FROM objectives WHERE id=$1',[f.goal])).rows[0].reason).toBe('REQUIREMENTS_EXTERNALLY_BLOCKED');
  expect(await verifiedBatch(pool,f.goal,1)).not.toBeNull();
  expect(await Promise.all([next(f),next(f)])).toEqual([false,false]);
  await reconcilePlanning();await reconcilePlanning();
  expect((await pool.query('SELECT id FROM planning_runs WHERE objective_id=$1',[f.goal])).rows).toHaveLength(1);
  expect((await pool.query('SELECT id FROM runs WHERE task_id=$1',[f.task])).rows).toHaveLength(1);
});
it('completes only after all v2 product predicates pass and makes concurrent validation idempotent',async()=>{
  const f=await fixture('completed');const results=await Promise.all([validateTask(f.task),validateTask(f.task)]);
  expect(results.every(r=>r.passed)).toBe(true);expect(results.filter(r=>r.duplicate)).toHaveLength(1);
  expect((await pool.query('SELECT id FROM acceptances WHERE run_id=$1',[f.run])).rows).toHaveLength(1);
  expect((await goalDetail(f.goal)).goal.state).toBe('completed');
});
it('replans a truthful partial result once instead of silently completing its selected requirement',async()=>{
  const f=await fixture('partial');await validateTask(f.task);
  expect((await goalDetail(f.goal)).goal.state).toBe('active');
  expect(await next(f)).toBe(true);expect(await next(f)).toBe(false);
  const planning=(await pool.query("SELECT feedback FROM planning_runs WHERE objective_id=$1 AND trigger_key LIKE 'incremental:%'",[f.goal])).rows;
  expect(planning).toHaveLength(1);expect(planning[0].feedback.requirementResolution.completedRequirementIds).toEqual([]);
});
it('preserves frozen outcome contracts across a PostgreSQL JSONB round trip and rejects changed predicates',async()=>{
  const f=await fixture('partial');await validateTask(f.task);expect(await next(f)).toBe(true);
  const previous=(await pool.query('SELECT plan FROM plan_revisions WHERE objective_id=$1 AND revision=1',[f.goal])).rows[0].plan;
  const planning=(await pool.query("SELECT * FROM planning_runs WHERE objective_id=$1 AND trigger_key LIKE 'incremental:%'",[f.goal])).rows[0];
  const decision={kind:'delivery',requirementId:'req-1',title:'Complete the actual product',
    action:'Implement the missing product behavior and preserve the earlier accepted gate.',
    verification:'Verify the product predicates using the same frozen contract and exact candidate.',
    agentId:'engineering.backend.node',requirementContracts:previous.requirementContracts};
  const nextPlan=compileDelivery(decision,f.request,previous,true,undefined,planning.feedback.requirementResolution);
  expect(nextPlan.requirementContracts).toEqual(contracts);
  await expect(assertIncrementalAuthority(pool,{...planning,request:f.request},nextPlan,previous)).resolves.toBeUndefined();
  const weakened=structuredClone(decision);weakened.requirementContracts[0].observables[0].equals=false;
  expect(()=>compileDelivery(weakened,f.request,previous,true,undefined,planning.feedback.requirementResolution)).toThrow();
});
it.each(['paused','cancelled'])('does not wake a %s objective on late acceptance or new input',async state=>{
  const f=await fixture();await pool.query('UPDATE goals SET state=$2 WHERE id=$1',[f.goal,state]);
  await validateTask(f.task);expect(await next(f)).toBe(false);
  const bytes=Buffer.from(JSON.stringify({available:true}));
  const request={requestId:randomUUID(),expectedRevision:1,name:'external-event.json',digest:hash(bytes),base64:bytes.toString('base64'),
    requirementIds:['req-1'],sourceRef:'operator:test-event',reason:'Authenticated external event without overriding a user stop',requestReplan:true};
  if(state==='cancelled')await expect(registerObjectiveInput(f.goal,request,{channel:'api-token'})).rejects.toThrow();
  else await registerObjectiveInput(f.goal,request,{channel:'api-token'});
  await reconcilePlanning();
  expect((await pool.query('SELECT state FROM goals WHERE id=$1',[f.goal])).rows[0].state).toBe(state);
  expect((await pool.query('SELECT id FROM planning_runs WHERE objective_id=$1',[f.goal])).rows).toHaveLength(1);
});
it('refuses stale revision advancement while preserving the accepted observations',async()=>{
  const f=await fixture('partial');await validateTask(f.task);
  const before=(await pool.query('SELECT * FROM acceptances WHERE run_id=$1',[f.run])).rows;
  await pool.query('UPDATE objectives SET revision=2 WHERE id=$1',[f.goal]);
  expect(await next(f)).toBe(false);
  expect((await pool.query('SELECT * FROM acceptances WHERE run_id=$1',[f.run])).rows).toEqual(before);
  expect((await pool.query('SELECT id FROM planning_runs WHERE objective_id=$1',[f.goal])).rows).toHaveLength(1);
});
it('wakes through one existing immutable input event, with replay and revision fences preserved',async()=>{
  const f=await fixture();await validateTask(f.task);
  const old=(await pool.query('SELECT * FROM acceptances WHERE run_id=$1',[f.run])).rows;
  const bytes=Buffer.from(JSON.stringify({available:true}));
  const request={requestId:randomUUID(),expectedRevision:1,name:'external-event.json',digest:hash(bytes),base64:bytes.toString('base64'),
    requirementIds:['req-1'],sourceRef:'operator:test-event',reason:'Authenticated operator supplied the missing external event',requestReplan:true};
  const results=await Promise.all([registerObjectiveInput(f.goal,request,{channel:'api-token'}),registerObjectiveInput(f.goal,request,{channel:'api-token'})]);
  expect(results.filter(r=>r.duplicate)).toHaveLength(1);
  await reconcilePlanning();await reconcilePlanning();
  expect((await pool.query('SELECT id FROM objective_feedback WHERE objective_id=$1',[f.goal])).rows).toHaveLength(1);
  expect((await pool.query("SELECT id FROM planning_runs WHERE objective_id=$1 AND trigger_key LIKE 'feedback:%'",[f.goal])).rows).toHaveLength(1);
  expect((await pool.query('SELECT * FROM acceptances WHERE run_id=$1',[f.run])).rows).toEqual(old);
  await expect(registerObjectiveInput(f.goal,{...request,requestId:randomUUID(),expectedRevision:0},{channel:'api-token'})).rejects.toThrow();
});
it('an inert input registration does not wake an external dependency',async()=>{
  const f=await fixture();await validateTask(f.task);const bytes=Buffer.from('Provenance only, no replan requested');
  await registerObjectiveInput(f.goal,{requestId:randomUUID(),expectedRevision:1,name:'provenance.txt',digest:hash(bytes),base64:bytes.toString('base64'),
    requirementIds:['req-1'],sourceRef:'operator:provenance',reason:'Preserve contribution ownership before its external review is finished',requestReplan:false},{channel:'api-token'});
  await reconcilePlanning();expect((await pool.query('SELECT id FROM objective_feedback WHERE objective_id=$1',[f.goal])).rows).toHaveLength(0);
});
it.each(['externally_blocked','partial'] as const)('management distinguishes canonical %s outcomes from absence-of-progress replanning',async status=>{
  const f=await fixture(status);await validateTask(f.task);
  const frontier=await readObjectiveFrontier(pool,f.goal),manager=randomUUID(),run=randomUUID(),fence=randomUUID();
  await pool.query('INSERT INTO settings(id,value) VALUES($1,$2)',[
    'work-frontier:'+f.goal,JSON.stringify({...frontier,knownReceipts:frontier.receipts,unchangedReviews:3,observedAt:new Date(0).toISOString()})]);
  await pool.query('INSERT INTO settings(id,value) VALUES($1,$2)',[
    'management:'+f.goal,JSON.stringify({enabled:true,enabledAt:new Date(0).toISOString()})]);
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,attempt_count)
    VALUES($1,$2,'manager','Inspect outcome','MANAGEMENT_V1','qa.integration','running','coordinate',
      '[{"id":"review","type":"artifact","name":"coordination.json","label":"Management observation"}]',1)`,[manager,f.goal]);
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'test',1,'running')",[run,manager,fence]);
  await writeArtifact(run,fence,'coordination.json',JSON.stringify({management:{summary:'Inspect the canonical outcome state',
    capacityReason:'Preserve accepted source and the unmet requirement',actions:[]}}));
  await completeRun(run,fence,'Observed outcome');expect((await validateTask(manager)).passed).toBe(true);
  await recordManagementReviews();await recordManagementReviews();
  const rows=(await pool.query('SELECT receipts FROM management_reviews WHERE run_id=$1',[run])).rows;
  expect(rows).toHaveLength(1);const codes=rows[0].receipts.map((r:any)=>r.code);
  if(status==='externally_blocked'){
    expect(codes).toContain('REQUIREMENTS_EXTERNALLY_BLOCKED');
    expect(codes).not.toContain('DELIVERY_STAGNATION');expect(codes).not.toContain('OPERATION_REPLAN_REQUIRED');
    expect(codes).not.toContain('CAPACITY_ASSESSMENT_REQUIRED');
  }else{
    expect(codes).toContain('DELIVERY_STAGNATION');expect(codes).toContain('OPERATION_REPLAN_REQUIRED');
    expect(codes).not.toContain('REQUIREMENTS_EXTERNALLY_BLOCKED');
  }
  expect((await pool.query('SELECT state FROM goals WHERE id=$1',[f.goal])).rows[0].state).toBe('active');
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1',[f.task])).rows[0].state).toBe('accepted');
  expect((await pool.query('SELECT id FROM planning_runs WHERE objective_id=$1',[f.goal])).rows).toHaveLength(1);
});
