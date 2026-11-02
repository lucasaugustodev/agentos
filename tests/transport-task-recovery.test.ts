import {beforeAll,beforeEach,afterAll,afterEach,it,expect,vi} from 'vitest';
import {randomUUID,createHash} from 'node:crypto';
import {writeFile} from 'node:fs/promises';
import path from 'node:path';
import {pool,migrate,event,tx,lock} from '../server/db.js';
import {config} from '../server/config.js';
import {seedRegistry} from '../server/registry.js';
import {installSupervision,recordSupervisionDecisions,applySupervisionDecisions} from '../server/supervision.js';
import {installTaskExecutionMetadata} from '../server/task-execution-metadata.js';
import {requestRecoveryReview} from '../server/recovery-supervision.js';
import {registerTaskRetryGuidance,retryGuidancePrompt} from '../server/task-retry-guidance.js';
import {executeRuntime} from '../server/runtime.js';
import {accountBridge} from '../server/bridge.js';
import {writeArtifact,readArtifact,completeRun,validateTask,failRun,schedule} from '../server/service.js';
import {acceptedDependencyArtifacts,recoverStoppedTasks} from '../server/recovery.js';
import {assembleContext} from '../server/memory.js';
import {reconcilePlanning} from '../server/planner.js';
import * as continuation from '../server/continuation.js';
import {recoverTransportTasks,transportInputProjection,transportDigest,assertTransportRetryCurrent} from '../server/transport-task-recovery.js';

const sha=(x:string)=>createHash('sha256').update(x).digest('hex');
const task=async(id:string)=>(await pool.query('SELECT * FROM tasks WHERE id=$1',[id])).rows[0];
const run=async(id:string)=>(await pool.query('SELECT * FROM runs WHERE id=$1',[id])).rows[0];
beforeAll(async()=>{expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');await migrate();await installSupervision();await installTaskExecutionMetadata();});
beforeEach(async()=>{
  await pool.query('TRUNCATE goals,agents,accounts,memories,events CASCADE');await seedRegistry();
  await pool.query("DELETE FROM settings WHERE id LIKE 'autonomy:%' OR id LIKE 'workforce:%' OR id LIKE 'supervision:%'");
  config.remoteExecution=true;config.bridgeHost='fixture-no-network';
});
afterEach(()=>{vi.restoreAllMocks();config.remoteExecution=false;config.bridgeHost='';});
afterAll(()=>pool.end());

async function fixture({code='MODEL_REQUEST_FAILED',max=3,marker=true,payload={}}:{code?:string;max?:number;marker?:boolean;payload?:any}={}){
  const goal=randomUUID(),id=randomUUID(),source=randomUUID(),fence=randomUUID();
  await pool.query("INSERT INTO accounts(id,label,provider,model,models,enabled,allow_unknown,max_concurrency,bridge_id) VALUES('transport-fixture','Fixture','openai','deepseek-v4-pro-0813','[\"deepseek-v4-pro-0813\"]',true,true,6,'fixture') ON CONFLICT DO NOTHING");
  await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1::uuid,'Integrate retained work','Keep three deliveries',$2,$1::text)",[goal,'transport-'+goal]);
  await pool.query("INSERT INTO objectives(id,request,request_hash,mode,status,revision) VALUES($1,'{\"requestedCapacity\":6}','fixture','execute','executing',7)",[goal]);
  await pool.query('INSERT INTO settings(id,value) VALUES($1,$2)',['supervision:'+goal,'{"enabled":true}']);
  const input='preserve the five materialized inputs and all accepted producers';
  const execution={kind:'harness',profile:'code',workspace:'fixture/integrator',baseWorkspace:'fixture/base',
    inputs:[{id:randomUUID(),name:'scope.txt',digest:sha(input),base64:Buffer.from(input).toString('base64')}]};
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,attempt_count,max_attempts,plan_revision,account_id)
    VALUES($1,$2,'r7-integrate','Integrator','Integrate exact accepted inputs; preserve all criteria.','engineering.integration','running','integrate',
      '[{"id":"delivery","type":"artifact","name":"integration.json"}]',$3,1,$4,7,'transport-fixture')`,[id,goal,JSON.stringify(execution),max]);
  const producers=[];
  for(let i=0;i<3;i++){
    const pid=randomUUID(),rid=randomUUID(),rf=randomUUID();producers.push(pid);
    await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,attempt_count)
      VALUES($1,$2,$3,'Producer','Publish concrete evidence','qa.backend','running','work','[{"id":"out","type":"artifact","name":"result.txt"}]',1)`,[pid,goal,'deliver-'+i]);
    await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'harness',1,'running')",[rid,pid,rf]);
    await writeArtifact(rid,rf,'result.txt','accepted producer '+i);await completeRun(rid,rf,'Concrete delivered result',{});
    expect((await validateTask(pid)).passed).toBe(true);await pool.query('INSERT INTO dependencies VALUES($1,$2)',[id,pid]);
  }
  await pool.query("UPDATE goals SET state='active' WHERE id=$1",[goal]);
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,account_id) VALUES($1,$2,1,$3,'harness',1,'running','transport-fixture')",[source,id,fence]);
  const context=await assembleContext(await run(source),await task(id),await acceptedDependencyArtifacts(id));
  await writeArtifact(source,fence,'partial.patch','retained partial integration; not accepted');
  if(marker)await event(pool,'MODEL_TRANSPORT_EXHAUSTED',id,{version:1,source:'runtime-generation-boundary',runId:source,runFence:fence,
    contextVersion:context.id,requestId:randomUUID(),generationAttempt:3,reason:'provider_unavailable',code,
    responseDelivered:false,traceComplete:true,pendingTools:0,cancelled:false,...payload});
  await failRun(source,fence,code,'Late 502: headers=251485ms bodyBytes=0; fixture transport exhausted');
  await event(pool,'HARNESS_STOPPED',id,{runId:source,confirmed:true});
  const proof=()=>({runId:source,executionRunId:source,workspace:execution.workspace,checkedAt:new Date().toISOString(),
    candidate:{baseDigest:sha('base'),candidateDigest:sha('partial-integrator')},
    process:{unit:'agentos-run-'+source,stopped:true,mainPid:0,populated:false,activeState:'inactive'}});
  return {goal,id,source,fence,context,execution,producers,proof};
}
async function next(f:any){
  await pool.query("UPDATE tasks SET next_attempt_at=now()-interval '1 second' WHERE id=$1",[f.id]);
  const queued=await schedule();expect(queued).toHaveLength(1);
  const r=await run(queued[0]);
  await pool.query("UPDATE runs SET state='running' WHERE id=$1",[r.id]);await pool.query("UPDATE tasks SET state='running' WHERE id=$1",[f.id]);
  const current=await run(r.id),t=await task(f.id),c=await assembleContext(current,t,await acceptedDependencyArtifacts(f.id));
  return {run:await run(r.id),task:t,context:c,signal:new AbortController().signal};
}
it('late 502 after three accepted producers resumes same task, frozen inputs and partial workspace; no new plan',async()=>{
  const f=await fixture(),history=await run(f.source),accepted=(await pool.query('SELECT * FROM acceptances ORDER BY id')).rows;
  const partial=(await pool.query("SELECT id FROM artifacts WHERE run_id=$1 AND name='partial.patch'",[f.source])).rows[0];
  vi.spyOn(continuation,'inspectStopped').mockImplementation(async()=>f.proof());
  expect(await recoverTransportTasks(async()=>f.proof())).toEqual([f.id]);
  expect(await recoverStoppedTasks()).toEqual([]);
  await reconcilePlanning();
  expect((await pool.query('SELECT * FROM planning_runs')).rowCount).toBe(0);
  expect((await pool.query('SELECT * FROM objective_feedback')).rowCount).toBe(0);
  const input=await next(f);await assertTransportRetryCurrent(input,async()=>f.proof());
  expect(input.run.task_id).toBe(f.id);expect(input.run.attempt).toBe(2);expect(input.run.fence).not.toBe(f.fence);
  expect(input.task.execution).toEqual(f.execution);
  expect(transportInputProjection(input.context.manifest)).toEqual(transportInputProjection(f.context.manifest));
  expect(input.run.checkpoint.nextAction).toContain('do not reapply');
  expect(await run(f.source)).toEqual(history);expect((await readArtifact(partial.id)).bytes.toString()).toContain('retained partial');
  expect((await pool.query('SELECT * FROM acceptances ORDER BY id')).rows).toEqual(accepted);
  expect((await pool.query("SELECT count(*)::int n FROM tasks WHERE id=ANY($1::uuid[]) AND state='accepted'",[f.producers])).rows[0].n).toBe(3);
  expect((await pool.query('SELECT count(*)::int n FROM tasks WHERE goal_id=$1',[f.goal])).rows[0].n).toBe(4);
});
it.each(['pending','delivered','trace','cancelled','foreignFence','foreignContext','noMarker'])('rejects %s provenance',async kind=>{
  const payload:any=kind==='pending'?{pendingTools:1}:kind==='delivered'?{responseDelivered:true}:kind==='trace'?{traceComplete:false}:
    kind==='cancelled'?{cancelled:true}:kind==='foreignFence'?{runFence:randomUUID()}:kind==='foreignContext'?{contextVersion:randomUUID()}:{};
  await fixture({payload,marker:kind!=='noMarker'});const inspect=vi.fn();
  expect(await recoverTransportTasks(inspect)).toEqual([]);expect(inspect).not.toHaveBeenCalled();
});
it('retains transport recovery after rate waits raised the physical generation counter',async()=>{
  const f=await fixture({payload:{generationAttempt:13,transportAttempt:3}});
  expect(await recoverTransportTasks(async()=>f.proof())).toEqual([f.id]);
});
it.each([{generationAttempt:4},{generationAttempt:3,transportAttempt:2},
  {generationAttempt:4,transportAttempt:4},{generationAttempt:2,transportAttempt:3},
  {generationAttempt:3.5,transportAttempt:3},{generationAttempt:4,transportAttempt:null}])
  ('rejects invalid exhaustion counters %j',async payload=>{
    await fixture({payload});const inspect=vi.fn();
    expect(await recoverTransportTasks(inspect)).toEqual([]);expect(inspect).not.toHaveBeenCalled();
  });
it.each(['paused','manual','user','lease','cancel','dependency','input','source','contract','race'])('preserves %s authority and refuses automatic retry',async kind=>{
  const f=await fixture();
  if(kind==='paused')await pool.query("UPDATE goals SET state='paused' WHERE id=$1",[f.goal]);
  if(kind==='manual')await pool.query("UPDATE tasks SET recovery='{\"mode\":\"manual\"}' WHERE id=$1",[f.id]);
  if(kind==='user')await pool.query("UPDATE tasks SET execution=execution||'{\"continuation\":{\"owner\":\"user\"}}' WHERE id=$1",[f.id]);
  if(kind==='lease')await pool.query("INSERT INTO leases VALUES('held-fixture',$1)",[f.source]);
  if(kind==='cancel')await event(pool,'CANCEL_REQUESTED',f.id,{runId:f.source});
  if(kind==='dependency')await pool.query("UPDATE tasks SET state='failed' WHERE id=$1",[f.producers[0]]);
  if(kind==='input')await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{inputs}','[]') WHERE id=$1",[f.id]);
  if(kind==='source')await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{workspace}','\"fixture/other\"') WHERE id=$1",[f.id]);
  if(kind==='contract')await pool.query("UPDATE tasks SET criteria='[]' WHERE id=$1",[f.id]);
  const inspect=async()=>{if(kind==='race')await pool.query("UPDATE goals SET state='paused' WHERE id=$1",[f.goal]);return f.proof();};
  expect(await recoverTransportTasks(inspect)).toEqual([]);
  expect((await pool.query("SELECT * FROM recoveries WHERE strategy='transport_resume'")).rowCount).toBe(0);
  expect((await task(f.id)).attempt_count).toBe(1);
});
it('existing attempt exhaustion requests supervision once, stays visible and never creates generic planner feedback',async()=>{
  const f=await fixture({max:1});vi.spyOn(continuation,'inspectStopped').mockImplementation(async()=>f.proof());
  await recoverTransportTasks(async()=>f.proof());await recoverTransportTasks(async()=>f.proof());
  await recoverStoppedTasks();await reconcilePlanning();
  expect((await task(f.id)).state).toBe('recovery_blocked');
  expect((await pool.query('SELECT * FROM supervision_cases WHERE task_id=$1',[f.id])).rowCount).toBe(1);
  expect((await pool.query('SELECT * FROM planning_runs')).rowCount).toBe(0);
  expect((await pool.query('SELECT * FROM objective_feedback')).rowCount).toBe(0);
});
it('an applied supervisor decision owns exhausted transport and remains consumable through the existing continuation',async()=>{
  const f=await fixture({max:1});await recoverTransportTasks(async()=>f.proof());
  const review=(await pool.query('SELECT * FROM supervision_cases WHERE task_id=$1',[f.id])).rows[0];
  const manager=randomUUID(),rid=randomUUID(),fence=randomUUID();
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,attempt_count)
    VALUES($1,$2,'manager','Manager','Resolve provider outage','qa.backend','running','coordinate','[{"id":"decision","type":"artifact","name":"coordination.json"}]',1)`,[manager,f.goal]);
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'harness',1,'running')",[rid,manager,fence]);
  await writeArtifact(rid,fence,'coordination.json',JSON.stringify({supervision:{actions:[{caseId:review.id,kind:'revise',
    diagnosis:'Provider transport returned; no functional rejection.',instruction:'Resume the preserved partial workspace with the same acceptance criteria after provider recovery.'}]}}));
  await completeRun(rid,fence,'Scoped recovery decision',{});expect((await validateTask(manager)).passed).toBe(true);
  await recordSupervisionDecisions();await applySupervisionDecisions(async()=>f.proof());
  expect((await pool.query('SELECT state FROM supervision_cases WHERE id=$1',[review.id])).rows[0].state).toBe('applied');
  await recoverTransportTasks(async()=>f.proof());
  expect((await pool.query('SELECT * FROM supervision_cases WHERE task_id=$1',[f.id])).rowCount).toBe(1);
  await expect(continuation.resumeFailedHarness(f.source,f.fence,f.proof())).resolves.toBeTruthy();
});
it('semantic tool failure retains planner recovery and cannot claim transport continuation',async()=>{
  const f=await fixture({code:'TOOL_FAILED',marker:false});await recoverStoppedTasks();
  expect((await pool.query('SELECT status,strategy FROM recoveries WHERE source_run_id=$1',[f.source])).rows[0])
    .toEqual({status:'blocked',strategy:'targeted_repair'});
  await reconcilePlanning();expect((await pool.query('SELECT * FROM objective_feedback')).rowCount).toBe(1);
});
it('fresh stop proof and partial-source digest are both required at dispatch',async()=>{
  const f=await fixture();await recoverTransportTasks(async()=>f.proof());const input=await next(f);
  await expect(assertTransportRetryCurrent(input,async()=>({...f.proof(),candidate:{baseDigest:sha('base'),candidateDigest:sha('changed')}})))
    .rejects.toMatchObject({code:'TRANSPORT_RETRY_SOURCE_CHANGED'});
  await expect(assertTransportRetryCurrent(input,async()=>({...f.proof(),checkedAt:'2000-01-01T00:00:00Z'}))).rejects.toThrow('STOP_UNCONFIRMED');
  await pool.query("UPDATE goals SET state='paused' WHERE id=$1",[f.goal]);
  await expect(assertTransportRetryCurrent(input,async()=>f.proof())).rejects.toMatchObject({code:'TRANSPORT_RETRY_BINDING_CHANGED'});
});
it('admission CAS sees an input change after the outside-lock stop inspection',async()=>{
  const f=await fixture();await recoverTransportTasks(async()=>f.proof());
  await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{inputs}','[]'),next_attempt_at=now() WHERE id=$1",[f.id]);
  expect(await schedule()).toEqual([]);expect((await task(f.id)).state).toBe('recovery_blocked');
});
it('a changed contract on a failed goal requests a physically proven review without a new batch',async()=>{
  const f=await fixture();await pool.query("UPDATE goals SET state='failed' WHERE id=$1",[f.goal]);
  await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{inputs}','[]') WHERE id=$1",[f.id]);
  expect(await recoverTransportTasks(async()=>({...f.proof(),process:{...f.proof().process,mainPid:42}}))).toEqual([]);
  expect((await pool.query('SELECT * FROM supervision_cases WHERE task_id=$1',[f.id])).rowCount).toBe(0);
  await pool.query("UPDATE tasks SET execution=execution- 'continuation' WHERE id=$1",[f.id]);
  expect(await recoverTransportTasks(async()=>f.proof())).toEqual([]);
  expect((await pool.query('SELECT state FROM goals WHERE id=$1',[f.goal])).rows[0].state).toBe('active');
  expect((await pool.query('SELECT * FROM supervision_cases WHERE task_id=$1',[f.id])).rowCount).toBe(1);
  expect((await task(f.id)).state).toBe('recovery_blocked');await reconcilePlanning();
  expect((await pool.query('SELECT * FROM planning_runs')).rowCount).toBe(0);
  expect((await pool.query('SELECT * FROM objective_feedback')).rowCount).toBe(0);
});
it('artifact byte corruption enters supervised input reconciliation instead of an endless proof wait',async()=>{
  const f=await fixture();await pool.query("UPDATE goals SET state='failed' WHERE id=$1",[f.goal]);
  const ref=f.context.manifest.dependencyArtifacts[0];
  await writeFile(path.join(config.dataDir,'artifacts',ref.id),'corrupted fixture bytes');
  expect(await recoverTransportTasks(async()=>f.proof())).toEqual([]);
  const c=(await pool.query('SELECT * FROM supervision_cases WHERE task_id=$1',[f.id])).rows[0];
  expect(c.evidence[0].code).toBe('TRANSPORT_RETRY_INPUTS_CHANGED');
  expect((await task(f.id)).execution.continuation?.reasonCode).not.toBe('TRANSPORT_RETRY_PROOF_PENDING');
  const inspect=vi.fn();await recoverTransportTasks(inspect);expect(inspect).not.toHaveBeenCalled();
  expect((await pool.query('SELECT * FROM supervision_cases WHERE task_id=$1',[f.id])).rowCount).toBe(1);
  expect((await pool.query('SELECT * FROM recoveries WHERE source_run_id=$1',[f.source])).rowCount).toBe(0);
});
it.each(['new','held'])('supervised rows do not consume the bounded %s recovery window',async kind=>{
  for(let i=0;i<5;i++){
    const f=await fixture({max:1});
    if(kind==='held')await recoverTransportTasks(async()=>f.proof());
    else await requestRecoveryReview(f.id,f.source,'TRANSPORT_RETRY_INPUTS_CHANGED','Fixture supervised owner',{stopProof:f.proof()});
  }
  const f=await fixture({max:kind==='held'?1:3});
  if(kind==='held'){
    await pool.query("UPDATE settings SET value='{\"enabled\":false}' WHERE id=$1",['supervision:'+f.goal]);
    await recoverTransportTasks(async()=>f.proof());
    await pool.query("UPDATE settings SET value='{\"enabled\":true}' WHERE id=$1",['supervision:'+f.goal]);
  }
  const inspect=vi.fn(async()=>f.proof());
  const recovered=await recoverTransportTasks(inspect);expect(inspect).toHaveBeenCalledTimes(1);
  if(kind==='new')expect(recovered).toEqual([f.id]);
  else expect((await pool.query('SELECT * FROM supervision_cases WHERE task_id=$1',[f.id])).rowCount).toBe(1);
});
it('freezes transport retry and authenticated next-attempt guidance in the same context and initial runtime prompt',async()=>{
  const f=await fixture(),history=await run(f.source),instruction='Preserve all accepted deliveries and partial edits; reproduce the smallest remaining integration test before continuing.';
  await recoverTransportTasks(async()=>f.proof());
  const guidance=await registerTaskRetryGuidance(f.id,{requestId:randomUUID(),expectedSourceRunId:f.source,
    expectedSourceFence:f.fence,expectedAttempt:1,expectedRevision:7,instruction,instructionDigest:sha(instruction)},
    {channel:'api-token'},async()=>f.proof());
  const input=await next(f);
  expect(input.context.manifest).toHaveProperty('transportRetry.sourceRunId',f.source);
  expect(input.context.manifest).toHaveProperty('retryGuidance.guidanceId',guidance.guidanceId);
  expect(input.context.content).toContain(instruction);
  expect((await retryGuidancePrompt(input.run,input.context))?.text).toContain(instruction);
  vi.spyOn(continuation,'inspectStopped').mockImplementation(async()=>f.proof());
  let sent:any;
  vi.spyOn(accountBridge,'request').mockImplementation(async(method:string,payload:any)=>{
    expect(method).toBe('worker.execute');sent=payload;throw Error('CAPTURED_COMBINED_PROMPT');
  });
  await expect(executeRuntime({...input,account:{bridge_id:'fixture',model:'deepseek-v4-pro-0813'},onEvent:async()=>{}}))
    .rejects.toThrow('CAPTURED_COMBINED_PROMPT');
  expect(sent.prompt).toContain(instruction);expect(sent.retryGuidance.promptDigest).toBe(sha(sent.prompt));
  expect(sent.contextVersion).toBe(input.context.id);expect(sent.runFence).toBe(input.run.fence);
  expect((await pool.query('SELECT count(*)::int n FROM task_retry_guidance_bindings WHERE run_id=$1',[input.run.id])).rows[0].n).toBe(1);
  expect((await pool.query("SELECT * FROM events WHERE kind='TASK_RETRY_GUIDANCE_PROMPT_DELIVERED'")).rowCount).toBe(0);
  expect(await run(f.source)).toEqual(history);
  expect((await pool.query("SELECT count(*)::int n FROM tasks WHERE id=ANY($1::uuid[]) AND state='accepted'",[f.producers])).rows[0].n).toBe(3);
});
