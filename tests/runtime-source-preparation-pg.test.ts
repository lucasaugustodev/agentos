import {beforeAll,beforeEach,afterEach,afterAll,it,expect,vi} from 'vitest';
import {randomUUID,createHash} from 'node:crypto';
import {migrate,pool} from '../server/db.js';
import {seedRegistry} from '../server/registry.js';
import {accountBridge} from '../server/bridge.js';
import * as continuation from '../server/continuation.js';
import {prepareBoundRuntimeWorkspace,assertBoundRuntimePreparationCurrent} from '../server/runtime-source-preparation.js';
import {preparationHash,verifySourcePreparation,preparedBaselineBinding,type SourcePreparationRequest} from '../server/source-preparation-contract.js';
import {executeRuntime,type RuntimeInput} from '../server/runtime.js';
import {config} from '../server/config.js';

const sha=(v:string|Buffer)=>createHash('sha256').update(v).digest('hex');
const canonical=(v:any):any=>Array.isArray(v)?v.map(canonical):v&&typeof v==='object'
  ?Object.fromEntries(Object.keys(v).sort().map(k=>[k,canonical(v[k])])):v;
beforeAll(async()=>{expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');await migrate();await seedRegistry();});
beforeEach(async()=>{
  await pool.query('TRUNCATE goals CASCADE');
  await pool.query("DELETE FROM settings WHERE id LIKE 'planning-source-preparation:%'");
});
afterEach(()=>{vi.restoreAllMocks();config.remoteExecution=false;});
afterAll(async()=>{await pool.end();});

function inspection(request:SourcePreparationRequest,changed=false){
  const binding={version:1,preparationIdentity:request.preparationIdentity,sourceWorkspace:request.sourceWorkspace,
    targetWorkspace:request.workspace,expectedSourceDigest:request.expectedSourceDigest,expectedSourceSnapshotDigest:request.expectedSourceSnapshotDigest};
  const body={...binding,bindingDigest:preparationHash(binding),observedSourceDigest:request.expectedSourceDigest,
    sourceSnapshotDigest:request.expectedSourceSnapshotDigest,cloneBaseDigest:request.expectedSourceDigest,
    baselineDigest:request.expectedSourceDigest,createdAt:'2026-01-01T00:00:00+00:00'};
  const receipt={...body,receiptDigest:preparationHash(body)};
  const marker={workspace:request.workspace,sourceWorkspace:request.sourceWorkspace,baseDigest:request.expectedSourceDigest,
    sourceSnapshotDigest:request.expectedSourceSnapshotDigest,createdAt:receipt.createdAt,
    preparation:{version:1,preparationIdentity:request.preparationIdentity,bindingDigest:receipt.bindingDigest,receiptDigest:receipt.receiptDigest}};
  const raw=Buffer.from(JSON.stringify(canonical(marker)));
  return {version:1,exists:true,workspace:request.workspace,preparationReceipt:receipt,markerBase64:raw.toString('base64'),
    generation:{workspace:request.workspace,sourceWorkspace:request.sourceWorkspace,baseDigest:request.expectedSourceDigest,
      snapshotDigest:sha(raw),sourceSnapshotDigest:request.expectedSourceSnapshotDigest},
    candidate:{baseDigest:request.expectedSourceDigest,candidateDigest:changed?sha('preserved edits'):request.expectedSourceDigest},
    destinationChanged:changed,checkedAt:Date.now()};
}
async function fixture(prior=0,knownNotStarted=false,inputBytes=0){
  const goalId=randomUUID(),taskId=randomUUID(),planningRunId=randomUUID(),planFence=randomUUID(),identity=randomUUID();
  const base='sample/o'+goalId.replaceAll('-','').slice(0,12)+'r2-base-0-p'+identity.replaceAll('-','');
  const baseRequest={workspace:base,sourceWorkspace:'sample/accepted',preparationIdentity:identity,
    expectedSourceDigest:sha('accepted source'),expectedSourceSnapshotDigest:sha('accepted generation')};
  const basePrepared=verifySourcePreparation(baseRequest,inspection(baseRequest),{requireUnchanged:true});
  const request=preparedBaselineBinding(basePrepared,base,'sample/consumer',randomUUID())!;
  const artifacts=['candidate.json','changes.patch'].map(name=>({id:randomUUID(),runId:randomUUID(),name,digest:sha(name),size:11}));
  const proof={version:1,kind:'repair_source_reuse',proofDigest:sha('controller verified proof'),acceptance:false,finalVerificationRequired:true,
    scope:{goalId,project:'sample',revision:1,baseWorkspace:'sample/logical'},
    selectedSource:{workspace:baseRequest.sourceWorkspace,candidateDigest:baseRequest.expectedSourceDigest,snapshotDigest:baseRequest.expectedSourceSnapshotDigest},
    incorporatedContributors:[{candidateArtifact:artifacts[0],patchArtifact:artifacts[1]}]};
  const intent={version:1,planningRunId,ownerFence:planFence,goalId,baseRevision:1,baseIndex:0,baseWorkspace:'sample/logical',state:'applied',
    request:baseRequest,prepared:basePrepared,proof};
  const intentKey=`planning-source-preparation:${planningRunId}:0`;
  const sourceReuse={version:1,planningRunId,proofDigest:proof.proofDigest,preparationIdentity:identity,
    receiptDigest:basePrepared.receipt.receiptDigest,baselineWorkspace:base,incorporatedArtifacts:artifacts,acceptance:false,finalVerificationRequired:true};
  const payload=Buffer.alloc(inputBytes,97);
  const execution={kind:'harness',workspace:request.workspace,sourceWorkspace:base,baseWorkspace:'sample/logical',
    profile:'code',sourcePreparation:request,sourceReuse,...(inputBytes?{inputs:[{id:randomUUID(),name:'context.txt',
      digest:sha(payload),base64:payload.toString('base64')}]}:{})};
  const resources=['remote-slot','workspace:'+request.workspace];
  await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1::uuid,'Bound clone','Fixture','sample',$1::text)",[goalId]);
  await pool.query("INSERT INTO objectives(id,request,request_hash,mode,status,revision) VALUES($1,'{}','fixture','execute','executing',2)",[goalId]);
  await pool.query("INSERT INTO planning_runs(id,objective_id,base_revision,trigger_key,state,fence,output) VALUES($1,$2,1,'fixture','completed',$3,'{}')",[planningRunId,goalId,planFence]);
  await pool.query('INSERT INTO settings(id,value) VALUES($1,$2)',[intentKey,JSON.stringify(intent)]);
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,resources,attempt_count,plan_revision)
    VALUES($1,$2,'consumer','Bound task','Execute the supplied task.','qa.backend','running','work','[]',$3,$4,$5,2)`,
    [taskId,goalId,JSON.stringify(execution),JSON.stringify(resources),prior+1]);
  const previous:any[]=[];
  for(let attempt=1;attempt<=prior;attempt++){
    const id=randomUUID(),fence=randomUUID();
    await pool.query("INSERT INTO runs(id,task_id,attempt,state,fence,runtime,agent_version,worker_id) VALUES($1,$2,$3,'failed',$4,'harness',1,'fixture-worker')",[id,taskId,attempt,fence]);
    if(knownNotStarted)await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('HARNESS_NOT_STARTED',$1,$2)",[taskId,JSON.stringify({runId:id,confirmed:true})]);
    previous.push({id,fence});
  }
  const runId=randomUUID(),runFence=randomUUID(),contextId=randomUUID();
  await pool.query("INSERT INTO runs(id,task_id,attempt,state,fence,runtime,agent_version,worker_id) VALUES($1,$2,$3,'running',$4,'harness',1,'fixture-worker')",[runId,taskId,prior+1,runFence]);
  const manifest={selected:[],dependencyArtifacts:[],executionInputs:[],runIdentity:{runId,runFence,taskId,goalId}};
  await pool.query("INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,$3,'Fixture')",[contextId,runId,JSON.stringify(manifest)]);
  await pool.query('UPDATE runs SET context_id=$2 WHERE id=$1',[runId,contextId]);
  for(const resource of resources)await pool.query('INSERT INTO leases(resource,run_id) VALUES($1,$2)',[resource,runId]);
  const run=(await pool.query('SELECT * FROM runs WHERE id=$1',[runId])).rows[0],task=(await pool.query('SELECT * FROM tasks WHERE id=$1',[taskId])).rows[0];
  const input:RuntimeInput={run,task,account:{bridge_id:'fixture-account',model:'glm-5.3-flash'},
    context:{id:contextId,content:'Fixture',manifest},signal:new AbortController().signal,onEvent:async()=>{}};
  const stop=()=>({runId:previous.at(-1)?.id,workspace:request.workspace,checkedAt:new Date().toISOString(),
    state:knownNotStarted?'not_started':{state:'failed'},process:{unit:'agentos-run-'+previous.at(-1)?.id,
      stopped:true,mainPid:0,populated:false,activeState:'inactive'}});
  const inspectStop=vi.spyOn(continuation,'inspectStopped').mockImplementation(async()=>stop());
  const missing=()=>({version:1,exists:false,confirmed:true,workspace:request.workspace,preparationIdentity:request.preparationIdentity,checkedAt:Date.now()});
  const rpc=vi.spyOn(accountBridge,'request').mockImplementation(async method=>{
    if(method==='worker.inspectPreparation')return inspection(request,prior>0&&!knownNotStarted);
    if(method==='worker.prepare')return {prepared:true};
    if(method==='worker.execute')return {summary:'Worker result',artifacts:[],evidence:{stopped:true}};
    throw Error('UNEXPECTED_RPC');
  });
  return {input,request,intent,intentKey,rpc,inspectStop,previous,stop,missing,goalId,taskId,runId,planningRunId};
}

it('keeps legacy preparation entirely outside the new database and RPC path',async()=>{
  const db=vi.spyOn(pool,'query'),rpc=vi.spyOn(accountBridge,'request');
  const input={run:{},task:{execution:{kind:'harness',sourceWorkspace:'sample/base',workspace:'sample/worker'}}} as RuntimeInput;
  expect(await prepareBoundRuntimeWorkspace(input)).toBe(false);await assertBoundRuntimePreparationCurrent(input);
  expect(db).not.toHaveBeenCalled();expect(rpc).not.toHaveBeenCalled();
});

it('checks the first unchanged clone and applied intent without preparing it again',async()=>{
  const f=await fixture();expect(await prepareBoundRuntimeWorkspace(f.input)).toBe(true);
  await assertBoundRuntimePreparationCurrent(f.input);
  expect(f.rpc.mock.calls.map(x=>x[0])).toEqual(['worker.inspectPreparation']);expect(f.inspectStop).not.toHaveBeenCalled();
  expect((await pool.query('SELECT count(*)::int n FROM acceptances')).rows[0].n).toBe(0);
});

it('keeps large frozen execution inputs out of runtime authority projections',async()=>{
  const f=await fixture(0,false,1024*1024),queries=vi.spyOn(pool,'query');
  expect(await prepareBoundRuntimeWorkspace(f.input)).toBe(true);
  const captures=queries.mock.calls.map((args,index)=>({sql:String(args[0]),result:queries.mock.results[index].value}))
    .filter(x=>x.sql.startsWith('SELECT r.id,r.task_id'));
  expect(captures).toHaveLength(1);
  const returned=JSON.stringify((await captures[0].result).rows);
  expect(returned.length).toBeLessThan(20000);
  expect(returned).not.toContain(f.input.task.execution.inputs[0].base64.slice(0,100));
  expect(returned).not.toContain('context.txt');
});

it('prepares confirmed absence using exactly the frozen UUID and checks the resulting clone',async()=>{
  const f=await fixture();f.rpc.mockResolvedValueOnce(f.missing()).mockResolvedValueOnce({prepared:true}).mockResolvedValueOnce(inspection(f.request));
  expect(await prepareBoundRuntimeWorkspace(f.input)).toBe(true);
  expect(f.rpc.mock.calls.map(x=>x[0])).toEqual(['worker.inspectPreparation','worker.prepare','worker.inspectPreparation']);
  expect(f.rpc.mock.calls.every(x=>preparationHash(x[1])===preparationHash(f.request))).toBe(true);
});

it('retains a modified retry candidate after physical stop without ever calling prepare',async()=>{
  const f=await fixture(2);expect(await prepareBoundRuntimeWorkspace(f.input)).toBe(true);
  await assertBoundRuntimePreparationCurrent(f.input);
  expect(f.inspectStop).toHaveBeenCalledWith(f.previous[1].id,false);
  expect(f.rpc.mock.calls.map(x=>x[0])).toEqual(['worker.inspectPreparation']);
});

it('permits missing retry creation only when every prior attempt has confirmed non-start and the predecessor is physically absent',async()=>{
  const f=await fixture(2,true);f.rpc.mockResolvedValueOnce(f.missing()).mockResolvedValueOnce({prepared:true}).mockResolvedValueOnce(inspection(f.request));
  expect(await prepareBoundRuntimeWorkspace(f.input)).toBe(true);
  expect(f.rpc.mock.calls.filter(x=>x[0]==='worker.prepare')).toHaveLength(1);
  expect(f.rpc.mock.calls[1][1]).toEqual(f.request);
});

it.each(['missing','partial','changed-first','unknown-absence'])(
  'does not reset or fall back to legacy after %s preparation evidence',async mode=>{
    const f=await fixture(mode==='missing'?1:0);
    f.rpc.mockResolvedValueOnce(mode==='missing'?f.missing():mode==='partial'?{exists:true}:mode==='changed-first'?inspection(f.request,true):{...f.missing(),confirmed:false});
    await expect(prepareBoundRuntimeWorkspace(f.input)).rejects.toHaveProperty('code');
    expect(f.rpc.mock.calls.some(x=>x[0]==='worker.prepare')).toBe(false);
  },
);

it.each(['fence','context','lease','intent','revision','frozen-input','receipt','reuse'])(
  'rejects changed %s authority before remote preparation',async change=>{
    const f=await fixture();
    if(change==='fence')await pool.query('UPDATE runs SET fence=$2 WHERE id=$1',[f.runId,randomUUID()]);
    if(change==='context')await pool.query('UPDATE runs SET context_id=$2 WHERE id=$1',[f.runId,randomUUID()]);
    if(change==='lease')await pool.query('DELETE FROM leases WHERE run_id=$1',[f.runId]);
    if(change==='intent')await pool.query("UPDATE settings SET value=jsonb_set(value,'{state}','\"uncertain\"') WHERE id=$1",[f.intentKey]);
    if(change==='revision')await pool.query('UPDATE objectives SET revision=3 WHERE id=$1',[f.goalId]);
    if(change==='frozen-input')f.input.task.execution.sourcePreparation.expectedSourceDigest=sha('forged');
    if(change==='receipt')await pool.query("UPDATE settings SET value=jsonb_set(value,'{prepared,receipt,cloneBaseDigest}',to_jsonb($2::text)) WHERE id=$1",[f.intentKey,sha('forged')]);
    if(change==='reuse')f.input.task.execution.sourceReuse.proofDigest=sha('forged');
    await expect(prepareBoundRuntimeWorkspace(f.input)).rejects.toHaveProperty('code');
    expect(f.rpc).not.toHaveBeenCalled();
  },
);

it('rejects a changed retry binding even when its current input and receipt agree',async()=>{
  const f=await fixture(1);
  // Change the task between admissions, preserving the immutable first attempt.
  await pool.query("UPDATE runs SET state='failed' WHERE id=$1",[f.runId]);
  const nextExecution=structuredClone(f.input.task.execution);nextExecution.sourcePreparation.preparationIdentity=randomUUID();
  await pool.query("UPDATE tasks SET execution=$2,attempt_count=3,state='running' WHERE id=$1",[f.taskId,JSON.stringify(nextExecution)]);
  const nextId=randomUUID(),nextFence=randomUUID(),contextId=randomUUID();
  await pool.query("INSERT INTO runs(id,task_id,attempt,state,fence,runtime,agent_version,worker_id) VALUES($1,$2,3,'running',$3,'harness',1,'fixture-worker')",[nextId,f.taskId,nextFence]);
  const manifest={...f.input.context.manifest,runIdentity:{runId:nextId,runFence:nextFence,taskId:f.taskId,goalId:f.goalId}};
  await pool.query("INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,$3,'fixture')",[contextId,nextId,JSON.stringify(manifest)]);
  await pool.query('UPDATE runs SET context_id=$2 WHERE id=$1',[nextId,contextId]);
  await pool.query('UPDATE leases SET run_id=$2 WHERE run_id=$1',[f.runId,nextId]);
  f.input.run=(await pool.query('SELECT * FROM runs WHERE id=$1',[nextId])).rows[0];
  f.input.task=(await pool.query('SELECT * FROM tasks WHERE id=$1',[f.taskId])).rows[0];f.input.context={...f.input.context,id:contextId,manifest};
  await expect(prepareBoundRuntimeWorkspace(f.input)).rejects.toMatchObject({code:'SOURCE_PREPARATION_RETRY_BINDING_CHANGED'});
  expect(f.rpc).not.toHaveBeenCalled();
});

it.each(['live','stale','wrong-unit'])( 'requires fresh physical termination for a %s predecessor',async state=>{
  const f=await fixture(1),proof=f.stop();
  if(state==='live')proof.process.mainPid=123;
  if(state==='stale')proof.checkedAt=new Date(Date.now()-61000).toISOString();
  if(state==='wrong-unit')proof.process.unit='agentos-run-'+randomUUID();
  f.inspectStop.mockResolvedValueOnce(proof);
  await expect(prepareBoundRuntimeWorkspace(f.input)).rejects.toThrow('STOP_UNCONFIRMED');expect(f.rpc).not.toHaveBeenCalled();
});

it('refuses a non-start history contradicted by an actual provider request',async()=>{
  const f=await fixture(2,true);
  await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('MODEL_REQUEST',$1,$2)",[f.taskId,JSON.stringify({runId:f.previous[0].id})]);
  f.rpc.mockResolvedValueOnce(f.missing());
  await expect(prepareBoundRuntimeWorkspace(f.input)).rejects.toMatchObject({code:'SOURCE_PREPARATION_RETRY_SOURCE_MISSING'});
  expect(f.rpc.mock.calls.some(x=>x[0]==='worker.prepare')).toBe(false);
});

it('rechecks authority after RPC observation and again immediately before dispatch',async()=>{
  const f=await fixture();
  f.rpc.mockImplementationOnce(async()=>{await pool.query('DELETE FROM leases WHERE run_id=$1',[f.runId]);return inspection(f.request);});
  await expect(prepareBoundRuntimeWorkspace(f.input)).rejects.toMatchObject({code:'SOURCE_PREPARATION_OWNER_CHANGED'});
  for(const resource of f.input.task.resources)await pool.query('INSERT INTO leases(resource,run_id) VALUES($1,$2)',[resource,f.runId]);
  await prepareBoundRuntimeWorkspace(f.input);
  await pool.query("UPDATE settings SET value=jsonb_set(value,'{state}','\"uncertain\"') WHERE id=$1",[f.intentKey]);
  await expect(assertBoundRuntimePreparationCurrent(f.input)).rejects.toMatchObject({code:'SOURCE_PREPARATION_AUTHORITY_CHANGED'});
});

it('refreshes stale observations after a slow preflight without a new clone and with no RPC under the admission lock',async()=>{
  const f=await fixture(1),clock=Date.now();let current=clock;
  vi.spyOn(Date,'now').mockImplementation(()=>current);
  f.inspectStop.mockImplementation(async()=>({...f.stop(),checkedAt:new Date(current).toISOString()}));
  f.rpc.mockImplementation(async method=>{
    expect(method).toBe('worker.inspectPreparation');
    const client=await pool.connect();
    try{
      const acquired=(await client.query('SELECT pg_try_advisory_lock(4280001) acquired')).rows[0].acquired;
      expect(acquired).toBe(true);if(acquired)await client.query('SELECT pg_advisory_unlock(4280001)');
    }finally{client.release();}
    return inspection(f.request,true);
  });
  await prepareBoundRuntimeWorkspace(f.input);current+=61000;
  await assertBoundRuntimePreparationCurrent(f.input);
  expect(f.rpc).toHaveBeenCalledTimes(2);expect(f.inspectStop).toHaveBeenCalledTimes(2);
});

it('uses the bound preparation path through executeRuntime and preserves the stopped result contract',async()=>{
  const f=await fixture();config.remoteExecution=true;
  expect(await executeRuntime(f.input)).toMatchObject({summary:'Worker result',artifacts:[]});
  expect(f.rpc.mock.calls.map(x=>x[0])).toEqual(['worker.inspectPreparation','worker.execute']);
  expect(f.rpc.mock.calls[1][1]).toMatchObject({runId:f.runId,runFence:f.input.run.fence,contextVersion:f.input.context.id});
});

it('never dispatches from a bound partial clone and records only the current attempt as not started',async()=>{
  const f=await fixture();config.remoteExecution=true;f.rpc.mockResolvedValueOnce({exists:true});
  const events:any[]=[];f.input.onEvent=async(kind,payload)=>{events.push({kind,payload});};
  await expect(executeRuntime(f.input)).rejects.toHaveProperty('code');
  expect(f.rpc.mock.calls.map(x=>x[0])).toEqual(['worker.inspectPreparation']);
  expect(events).toEqual([{kind:'HARNESS_NOT_STARTED',payload:{confirmed:true,code:'SOURCE_PREPARATION_PROOF_INVALID'}}]);
});
