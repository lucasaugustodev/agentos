import {beforeAll,beforeEach,afterEach,afterAll,it,expect,vi} from 'vitest';
import {randomUUID,createHash} from 'node:crypto';
import {pool,migrate,tx,lock} from '../server/db.js';
import {seedRegistry} from '../server/registry.js';
import {registerTaskRetryGuidance,retryGuidancePrompt} from '../server/task-retry-guidance.js';
import {assembleContext} from '../server/memory.js';
import {executeRuntime} from '../server/runtime.js';
import {config} from '../server/config.js';
import {accountBridge} from '../server/bridge.js';
import {makeApp} from '../server/app.js';
import * as continuation from '../server/continuation.js';
import {stoppedRunProof} from '../bridge/stopped-run-proof.mjs';

const sha=(s:string)=>createHash('sha256').update(s).digest('hex');
const auth={channel:'api-token' as const};
const instruction='Restore only the newly introduced skips. Run a bounded pure test. Keep database coverage pending for registered integration CI; preserve accepted siblings and original criteria.';
beforeAll(()=>migrate());
beforeEach(async()=>{
  await pool.query('TRUNCATE goals,agents,accounts,events,memories CASCADE');
  await pool.query("DELETE FROM settings WHERE id LIKE 'worker-validation:%' OR id LIKE 'candidate-validation:%'");
  await seedRegistry();config.bridgeHost='';config.remoteExecution=false;
});
afterEach(()=>{vi.restoreAllMocks();vi.unstubAllEnvs();config.remoteExecution=false;});
afterAll(()=>pool.end());
async function fixture(){
  const goal=randomUUID(),task=randomUUID(),runId=randomUUID(),fence=randomUUID();
  await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'retry guidance','original objective','test',$2)",[goal,goal]);
  await pool.query("INSERT INTO objectives(id,request,request_hash,mode,status,revision) VALUES($1,'{}',$2,'execute','executing',8)",[goal,sha('{}')]);
  const siblings=[randomUUID(),randomUUID()];
  for(const [i,id] of [task,...siblings].entries())await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,plan_revision,covers,criteria,execution)
    VALUES($1,$2,$3,'original title','Original immutable task','engineering.backend.node',$4,'work',8,$5,$6,$7)`,
    [id,goal,'r8-deliver-'+(i+1),i?'accepted':'waiting_resource',JSON.stringify(['req-'+(i+1)]),
      JSON.stringify([{id:'original',label:'original tests',type:'artifact',name:'delivery.json'}]),JSON.stringify({kind:'harness',workspace:'test/retry-'+i,profile:'code'})]);
  await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,finished_at,error_code,checkpoint)
    VALUES($1,$2,1,$3,'harness',1,'failed',now(),'HARNESS_SANDBOX_OOM','{"kind":"automatic_recovery","original":"preserve"}')`,[runId,task,fence]);
  await pool.query('UPDATE tasks SET attempt_count=1 WHERE id=$1',[task]);
  return {goal,task,runId,fence,siblings};
}
type Fixture=Awaited<ReturnType<typeof fixture>>;
function request(f:Fixture){return {requestId:randomUUID(),expectedSourceRunId:f.runId,expectedSourceFence:f.fence,
  expectedAttempt:1,expectedRevision:8,instruction,instructionDigest:sha(instruction)};}
function proof(f:Fixture){return {runId:f.runId,workspace:'test/retry-0',state:'failed',candidate:{},checkedAt:new Date().toISOString(),
  process:{unit:'agentos-run-'+f.runId,stopped:true,mainPid:0,populated:false,activeState:'inactive'}};}
async function historical(f:Fixture){return (await pool.query(`SELECT jsonb_build_object('tasks',(SELECT jsonb_agg(t ORDER BY id) FROM tasks t WHERE goal_id=$1),
  'runs',(SELECT jsonb_agg(r ORDER BY r.id) FROM runs r JOIN tasks t ON t.id=r.task_id WHERE t.goal_id=$1),
  'feedback',(SELECT count(*) FROM objective_feedback WHERE objective_id=$1)) value`,[f.goal])).rows[0].value;}
async function successor(f:Fixture){
  return tx(async db=>{await lock(db);const id=randomUUID(),fence=randomUUID();
    await db.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,2,$3,'harness',1,'running')`,[id,f.task,fence]);
    await db.query("UPDATE tasks SET state='running',attempt_count=2 WHERE id=$1",[f.task]);
    return (await db.query('SELECT * FROM runs WHERE id=$1',[id])).rows[0];});
}
async function contextFor(run:any){const task=(await pool.query('SELECT * FROM tasks WHERE id=$1',[run.task_id])).rows[0];
  return {task,context:await assembleContext(run,task,[])};}

it('registers immutable guidance without changing the stopped attempt, contract, siblings or planning',async()=>{
  const f=await fixture(),before=await historical(f),body=request(f);
  const result=await registerTaskRetryGuidance(f.task,body,auth,async()=>proof(f));
  expect(result).toMatchObject({duplicate:false,historicalAttemptsChanged:false,dispatchRequested:false});
  expect(await historical(f)).toEqual(before);
  const stored=(await pool.query('SELECT payload FROM task_retry_guidance WHERE id=$1',[result.guidanceId])).rows[0].payload;
  expect(stored.stopProof).toMatchObject({runId:f.runId,workspace:'test/retry-0',process:{stopped:true,mainPid:0,populated:false}});
  expect(stored.stopProof).not.toHaveProperty('candidate');expect(stored.stopProof).not.toHaveProperty('runFence');
  expect(stored.request.expectedSourceFence).toBe(f.fence);
  await expect(pool.query("UPDATE task_retry_guidance SET payload='{}' WHERE id=$1",[result.guidanceId])).rejects.toThrow('RETRY_GUIDANCE_IMMUTABLE');
});
it('binds exactly the next run and delivers guidance in its initial runtime prompt while preserving historical evidence',async()=>{
  const f=await fixture(),old=(await pool.query('SELECT * FROM runs WHERE id=$1',[f.runId])).rows[0];
  const registered=await registerTaskRetryGuidance(f.task,request(f),auth,async()=>proof(f));
  const run=await successor(f),{task,context}=await contextFor(run);
  expect(context.manifest).toHaveProperty('retryGuidance.guidanceId',registered.guidanceId);
  expect(context.content).toContain(instruction);
  expect((await pool.query('SELECT * FROM runs WHERE id=$1',[f.runId])).rows[0]).toEqual(old);
  expect((await pool.query('SELECT state FROM tasks WHERE id=ANY($1::uuid[])',[f.siblings])).rows.map(r=>r.state)).toEqual(['accepted','accepted']);
  const verified=await retryGuidancePrompt(run,context);expect(verified?.text).toContain(instruction);
  let payload:any;config.remoteExecution=true;
  vi.spyOn(accountBridge,'request').mockImplementation(async(method:string,value:any)=>{expect(method).toBe('worker.execute');payload=value;throw Error('CAPTURED_INITIAL_PROMPT');});
  await expect(executeRuntime({run,task,context,account:{bridge_id:'fixture',model:'fixture'},signal:new AbortController().signal,onEvent:async()=>{}})).rejects.toThrow('CAPTURED_INITIAL_PROMPT');
  expect(payload.prompt).toContain(instruction);expect(payload.retryGuidance.promptDigest).toBe(sha(payload.prompt));
  expect(payload.contextVersion).toBe(context.id);
  expect((await pool.query("SELECT count(*)::int n FROM events WHERE kind='TASK_RETRY_GUIDANCE_BOUND'")).rows[0].n).toBe(1);
  await expect(contextFor(run)).rejects.toThrow('RETRY_GUIDANCE_ALREADY_BOUND');
});
it('serializes concurrent duplicate registration and rejects conflicting content or a second instruction',async()=>{
  const f=await fixture(),body=request(f),inspect=async()=>proof(f);
  const a=await Promise.all([registerTaskRetryGuidance(f.task,body,auth,inspect),registerTaskRetryGuidance(f.task,body,auth,inspect)]);
  expect(a[0].guidanceId).toBe(a[1].guidanceId);expect(a.map(v=>v.duplicate).sort()).toEqual([false,true]);
  const different=instruction+' Different hypothesis.';
  await expect(registerTaskRetryGuidance(f.task,{...body,instruction:different,instructionDigest:sha(different)},auth,inspect)).rejects.toThrow('IDEMPOTENCY_CONFLICT');
  await expect(registerTaskRetryGuidance(f.task,request(f),auth,inspect)).rejects.toThrow('RETRY_GUIDANCE_ALREADY_REGISTERED');
  await successor(f);
  expect((await registerTaskRetryGuidance(f.task,body,auth,async()=>{throw Error('NO_REPEAT_INSPECTION');})).duplicate).toBe(true);
});
it('refuses registration if admission wins while the physical-stop inspection is outside the lock',async()=>{
  const f=await fixture();let entered!:()=>void,release!:()=>void;
  const started=new Promise<void>(r=>entered=r),wait=new Promise<void>(r=>release=r);
  const pending=registerTaskRetryGuidance(f.task,request(f),auth,async()=>{entered();await wait;return proof(f);});
  const observed=pending.catch(e=>e);await started;await successor(f);release();
  expect((await observed).message).toBe('RETRY_GUIDANCE_TARGET_CHANGED');
  expect((await pool.query('SELECT count(*)::int n FROM task_retry_guidance')).rows[0].n).toBe(0);
});
it.each(['paused','cancelled','revision','cancel-event','live','uncertain','stop','fence'] as const)('refuses unsafe %s ownership without registering guidance',async kind=>{
  const f=await fixture(),body=request(f);let stopped:any=proof(f);
  if(kind==='paused'||kind==='cancelled')await pool.query('UPDATE goals SET state=$2 WHERE id=$1',[f.goal,kind]);
  if(kind==='revision')await pool.query('UPDATE objectives SET revision=9 WHERE id=$1',[f.goal]);
  if(kind==='cancel-event')await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('CANCEL_REQUESTED',$1,$2)",[f.task,JSON.stringify({runId:f.runId})]);
  if(kind==='live')await successor(f);
  if(kind==='uncertain')await pool.query('INSERT INTO settings(id,value) VALUES($1,$2)',['candidate-validation:'+randomUUID(),JSON.stringify({taskId:f.task,state:'uncertain'})]);
  if(kind==='stop')stopped.process.mainPid=12;
  if(kind==='fence')body.expectedSourceFence=randomUUID();
  await expect(registerTaskRetryGuidance(f.task,body,auth,async()=>stopped)).rejects.toThrow();
  expect((await pool.query('SELECT count(*)::int n FROM task_retry_guidance')).rows[0].n).toBe(0);
});
it('accepts the real canonical stoppedRunProof shape without inventing a physical runFence field',async()=>{
  const f=await fixture();let inspections=0;
  const actual=await stoppedRunProof(f.runId,undefined,{
    readState:async()=>({runId:f.runId,workspace:'test/retry-0',state:'failed'}),
    inspectUnit:async()=>{inspections++;return {...proof(f).process,checkedAt:new Date().toISOString()};},
    inspectWorkspace:async()=>({candidateDigest:'a'.repeat(64)}),
  });
  expect(actual).not.toHaveProperty('runFence');expect(inspections).toBe(2);
  expect((await registerTaskRetryGuidance(f.task,request(f),auth,async()=>actual)).duplicate).toBe(false);
});
it('refuses stale contract or revision at context binding rather than silently dropping guidance',async()=>{
  const f=await fixture();await registerTaskRetryGuidance(f.task,request(f),auth,async()=>proof(f));
  await pool.query("UPDATE tasks SET description='Changed contract' WHERE id=$1",[f.task]);
  const run=await successor(f);await expect(contextFor(run)).rejects.toThrow('RETRY_GUIDANCE_BINDING_STALE');
  expect((await pool.query('SELECT count(*)::int n FROM task_retry_guidance_bindings')).rows[0].n).toBe(0);
});
it('refuses missing or tampered bound context at the runtime boundary',async()=>{
  const f=await fixture();await registerTaskRetryGuidance(f.task,request(f),auth,async()=>proof(f));
  const run=await successor(f);
  await expect(retryGuidancePrompt(run,{content:'',manifest:{}})).rejects.toThrow('RETRY_GUIDANCE_UNBOUND');
  const {context}=await contextFor(run);
  await expect(retryGuidancePrompt(run,{...context,content:context.content.replace(instruction,'altered')})).rejects.toThrow('RETRY_GUIDANCE_CONTEXT_CHANGED');
  await expect(retryGuidancePrompt(run,{...context,manifest:{}})).rejects.toThrow('RETRY_GUIDANCE_CONTEXT_CHANGED');
});
it('binds one context under concurrent assembly instead of delivering the same instruction twice',async()=>{
  const f=await fixture();await registerTaskRetryGuidance(f.task,request(f),auth,async()=>proof(f));
  const run=await successor(f),results=await Promise.allSettled([contextFor(run),contextFor(run)]);
  expect(results.map(r=>r.status).sort()).toEqual(['fulfilled','rejected']);
  expect((await pool.query('SELECT count(*)::int n FROM task_retry_guidance_bindings')).rows[0].n).toBe(1);
  expect((await pool.query('SELECT count(*)::int n FROM contexts WHERE run_id=$1',[run.id])).rows[0].n).toBe(1);
});
it.each(['revision','cancel','uncertain','runtime'] as const)('refuses %s changes after registration before binding',async kind=>{
  const f=await fixture();await registerTaskRetryGuidance(f.task,request(f),auth,async()=>proof(f));
  const run=await successor(f);
  if(kind==='revision')await pool.query('UPDATE objectives SET revision=9 WHERE id=$1',[f.goal]);
  if(kind==='cancel')await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('CANCEL_REQUESTED',$1,$2)",[f.task,JSON.stringify({runId:run.id})]);
  if(kind==='uncertain')await pool.query('INSERT INTO settings(id,value) VALUES($1,$2)',['candidate-validation:'+randomUUID(),JSON.stringify({taskId:f.task,state:'uncertain'})]);
  if(kind==='runtime')await pool.query("UPDATE runs SET runtime='opencode' WHERE id=$1",[run.id]);
  await expect(contextFor(run)).rejects.toThrow();
  expect((await pool.query('SELECT count(*)::int n FROM task_retry_guidance_bindings')).rows[0].n).toBe(0);
});
it('requires the existing operator HTTP authentication and rejects extra policy fields',async()=>{
  const f=await fixture(),body=request(f),app=makeApp();
  vi.spyOn(continuation,'inspectStopped').mockResolvedValue(proof(f) as any);
  const url='/api/tasks/'+f.task+'/retry-guidance';
  expect((await app.inject({method:'POST',url,payload:body})).statusCode).toBe(401);
  const operationsToken='operations-only-retry-guidance-fixture';
  vi.stubEnv('AGENTOS_OPERATIONS_ACCESS_JSON',JSON.stringify([{id:'read-only',tokenSha256:sha(operationsToken),goalIds:[f.goal],projects:[],scopes:['observe','evidence']}]));
  expect((await app.inject({method:'POST',url,headers:{authorization:'Bearer '+operationsToken},payload:body})).statusCode).toBe(401);
  expect((await app.inject({method:'POST',url,headers:{authorization:'Bearer '+config.apiToken},payload:{...body,model:'other'}})).statusCode).toBe(400);
  expect((await app.inject({method:'POST',url,headers:{authorization:'Bearer '+config.apiToken},payload:body})).statusCode).toBe(200);
  await app.close();
});
