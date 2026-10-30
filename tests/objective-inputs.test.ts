import {afterAll,afterEach,beforeAll,beforeEach,expect,it,vi} from 'vitest';
import {createHash,randomUUID} from 'node:crypto';
import {pool,migrate} from '../server/db.js';
import {seedRegistry} from '../server/registry.js';
import {config} from '../server/config.js';
import {makeApp} from '../server/app.js';
import {objectiveSchema} from '../server/objective-contracts.js';
import {compileDelivery} from '../server/incremental-planning.js';
import {submitObjective,claimPlanning,applyPlannedResult,reconcilePlanning,PlanningEngine} from '../server/planner.js';
import {accountBridge} from '../server/bridge.js';
import {installObjectiveInputs,registerObjectiveInput,readObjectiveInputs,inputsForRequirements} from '../server/objective-inputs.js';
import {executionInputManifest,runtimeInputs} from '../server/execution-inputs.js';
import {planningInputContext} from '../server/planning-input-context.js';

const auth={channel:'api-token' as const};
const digest=(bytes:Buffer)=>createHash('sha256').update(bytes).digest('hex');
const decision={kind:'delivery',requirementId:'req-1',title:'Corrigir o consumidor de entrada',
  action:'Reproduzir o defeito e corrigir o consumidor preservando os controles originais.',
  verification:'Reexecutar o contrato original e os negativos na candidata imutável.',agentId:'engineering.backend.node',
  sourceContract:{version:1,required:[],preserve:[],noSourceReason:'Synthetic immutable-input controller fixture records evidence without claiming source implementation.'}};
beforeAll(async()=>{await migrate();await installObjectiveInputs();});
beforeEach(async()=>{
  await pool.query('TRUNCATE goals,agents,accounts,memories,events CASCADE');
  await pool.query("DELETE FROM settings WHERE id LIKE 'workforce:%'");
  await seedRegistry();
  await pool.query(`UPDATE accounts SET provider='codex',model='test-model',models='["test-model"]',bridge_id='test-account',max_concurrency=4,allow_unknown=true`);
  config.bridgeHost='fixture';config.remoteExecution=true;
});
afterEach(()=>{vi.restoreAllMocks();config.bridgeHost='';config.remoteExecution=false;});
afterAll(()=>pool.end());

async function fixture(){
  const input=objectiveSchema.parse({message:'Corrigir dois consumidores independentes e verificar o original.',project:'test',
    workspaces:['test/base'],mode:'execute',planningStrategy:'incremental',sourceContractVersion:1,
    requirements:['A entrada original precisa ser preservada.','O outro consumidor não deve receber contexto alheio.'],idempotencyKey:randomUUID()});
  const {objectiveId}=await submitObjective(input);
  const run=await claimPlanning(),plan=compileDelivery(decision,input);
  await applyPlannedResult(run,plan);
  return {objectiveId,input,plan};
}
function request(extra:Record<string,unknown>={}){
  const bytes=Buffer.from("console.log('original fixture');\n");
  return {requestId:randomUUID(),expectedRevision:1,name:'acceptance.mjs',digest:digest(bytes),base64:bytes.toString('base64'),
    requirementIds:['req-1'],sourceRef:'operator-upload:original-fixture',reason:'O consumidor precisa dos bytes originais para verificar o critério.',...extra};
}

it('registers inert operator bytes and injects them into future work and independent QA with the original criteria',async()=>{
  const f=await fixture(),body=request();
  const result=await registerObjectiveInput(f.objectiveId,body,auth);
  expect(result).toMatchObject({duplicate:false,liveAttemptsChanged:false,activation:'future_plan_attempts'});
  expect(JSON.stringify(result)).not.toContain(body.base64);
  expect((await pool.query('SELECT description FROM objective_feedback WHERE objective_id=$1',[f.objectiveId])).rows[0].description).toContain(result.inputId);
  await reconcilePlanning();
  const planning=await claimPlanning();
  await applyPlannedResult(planning,compileDelivery(decision,f.input,f.plan,false,planning.feedback.reports));
  const tasks=(await pool.query('SELECT stage,execution,criteria FROM tasks WHERE goal_id=$1 AND plan_revision=2 ORDER BY stage',[f.objectiveId])).rows;
  expect(tasks.map(t=>t.stage).sort()).toEqual(['verify','work']);
  for(const task of tasks){
    expect(task.execution.inputs).toEqual([{id:result.inputId,name:body.name,digest:body.digest,base64:body.base64}]);
    const manifest=executionInputManifest(task.execution.inputs);
    const transported=await runtimeInputs(task,{executionInputs:manifest},async()=>{throw Error('NO_ARTIFACT_IO_EXPECTED');});
    expect(Buffer.from(transported[0].base64,'base64')).toEqual(Buffer.from(body.base64,'base64'));
    expect(task.criteria[0].equals).toBe(true);
  }
});

it('keeps a live attempt and its immutable input snapshot unchanged when an input arrives',async()=>{
  const f=await fixture();
  const task=(await pool.query("SELECT * FROM tasks WHERE goal_id=$1 AND stage='work'",[f.objectiveId])).rows[0];
  const id=randomUUID(),fence=randomUUID();
  await pool.query(`INSERT INTO runs(id,task_id,attempt,state,fence,runtime,agent_version) VALUES($1,$2,1,'running',$3,'harness',1)`,[id,task.id,fence]);
  await pool.query("UPDATE tasks SET state='running',attempt_count=1 WHERE id=$1",[task.id]);
  const before=(await pool.query('SELECT state,fence,instruction_snapshot FROM runs WHERE id=$1',[id])).rows[0];
  await registerObjectiveInput(f.objectiveId,request(),auth);
  await reconcilePlanning();
  expect((await pool.query('SELECT state,fence,instruction_snapshot FROM runs WHERE id=$1',[id])).rows[0]).toEqual(before);
  expect((await pool.query('SELECT execution,criteria FROM tasks WHERE id=$1',[task.id])).rows[0]).toEqual({execution:task.execution,criteria:task.criteria});
  expect((await pool.query("SELECT 1 FROM planning_runs WHERE objective_id=$1 AND state='queued'",[f.objectiveId])).rowCount).toBe(0);
});

it('is idempotent under concurrent requests and permits replay after the revision advances',async()=>{
  const f=await fixture(),body=request({requestReplan:false});
  const [a,b]=await Promise.all([registerObjectiveInput(f.objectiveId,body,auth),registerObjectiveInput(f.objectiveId,body,auth)]);
  expect(a.inputId).toBe(b.inputId);expect([a.duplicate,b.duplicate].sort()).toEqual([false,true]);
  await pool.query('UPDATE objectives SET revision=2 WHERE id=$1',[f.objectiveId]);
  expect((await registerObjectiveInput(f.objectiveId,body,auth)).duplicate).toBe(true);
  expect((await pool.query('SELECT count(*)::int n FROM objective_inputs WHERE goal_id=$1',[f.objectiveId])).rows[0].n).toBe(1);
  expect((await pool.query('SELECT count(*)::int n FROM objective_feedback WHERE objective_id=$1',[f.objectiveId])).rows[0].n).toBe(0);
});

it('rejects conflicting content for an existing request identity',async()=>{
  const f=await fixture(),body=request();await registerObjectiveInput(f.objectiveId,body,auth);
  const bytes=Buffer.from('different original');
  await expect(registerObjectiveInput(f.objectiveId,{...body,digest:digest(bytes),base64:bytes.toString('base64')},auth)).rejects.toMatchObject({code:'IDEMPOTENCY_CONFLICT'});
});

it.each(['wrong-digest','noncanonical-base64','too-large','unknown-requirement','duplicate-requirement','stale-revision','unsafe-name'])('rejects %s without storing bytes or feedback',async kind=>{
  const f=await fixture();let body=request();
  if(kind==='wrong-digest')body.digest='0'.repeat(64);
  if(kind==='noncanonical-base64')body.base64+='\n';
  if(kind==='too-large'){const bytes=Buffer.alloc(65537,120);body={...body,digest:digest(bytes),base64:bytes.toString('base64')};}
  if(kind==='unknown-requirement')body.requirementIds=['missing'];
  if(kind==='duplicate-requirement')body.requirementIds=['req-1','req-1'];
  if(kind==='stale-revision')body.expectedRevision=0;
  if(kind==='unsafe-name')body.name='../acceptance.mjs';
  await expect(registerObjectiveInput(f.objectiveId,body,auth)).rejects.toThrow();
  expect((await pool.query('SELECT count(*)::int n FROM objective_inputs WHERE goal_id=$1',[f.objectiveId])).rows[0].n).toBe(0);
  expect((await pool.query('SELECT count(*)::int n FROM objective_feedback WHERE objective_id=$1',[f.objectiveId])).rows[0].n).toBe(0);
});

it('rejects embedded credential material before any stored input or feedback',async()=>{
  const f=await fixture(),bytes=Buffer.from(['-----BEGIN','PRIVATE KEY-----'].join(' '));
  await expect(registerObjectiveInput(f.objectiveId,request({digest:digest(bytes),base64:bytes.toString('base64')}),auth)).rejects.toMatchObject({code:'SECRET_REJECTED'});
});

it('treats a source reference as inert provenance and keeps unrelated requirements isolated',async()=>{
  const f=await fixture();await registerObjectiveInput(f.objectiveId,request({sourceRef:'operator-reference:/nonexistent/not-a-host-read',requestReplan:false}),auth);
  const inputs=await readObjectiveInputs(pool,f.objectiveId);
  expect(inputsForRequirements(inputs,['req-2'])).toEqual([]);
  expect(inputsForRequirements(inputs,['req-1'])).toHaveLength(1);
  const other=await fixture();expect(await readObjectiveInputs(pool,other.objectiveId)).toEqual([]);
});

it('requires operator authentication and rejects requests for cancelled objectives',async()=>{
  const f=await fixture();
  await expect(registerObjectiveInput(f.objectiveId,request(),{channel:'model'} as any)).rejects.toMatchObject({code:'OPERATOR_REQUIRED'});
  await pool.query("UPDATE goals SET state='cancelled' WHERE id=$1",[f.objectiveId]);
  await expect(registerObjectiveInput(f.objectiveId,request(),auth)).rejects.toMatchObject({code:'OBJECTIVE_CLOSED'});
});

it('preserves input bytes against update/delete and refuses input records without their receipt event',async()=>{
  const f=await fixture(),result=await registerObjectiveInput(f.objectiveId,request({requestReplan:false}),auth);
  await expect(pool.query("UPDATE objective_inputs SET payload='{}' WHERE id=$1",[result.inputId])).rejects.toThrow('OBJECTIVE_INPUT_IMMUTABLE');
  await expect(pool.query('DELETE FROM objective_inputs WHERE id=$1',[result.inputId])).rejects.toThrow('OBJECTIVE_INPUT_IMMUTABLE');
  await pool.query("DELETE FROM events WHERE kind='OBJECTIVE_INPUT_REGISTERED' AND payload->>'inputId'=$1",[result.inputId]);
  await expect(readObjectiveInputs(pool,f.objectiveId)).rejects.toMatchObject({code:'OBJECTIVE_INPUT_INTEGRITY'});
});

it('exposes registration only through authenticated API requests and returns metadata rather than file bytes',async()=>{
  const f=await fixture(),app=makeApp(),body=request({requestReplan:false});
  try{
    const url='/api/objectives/'+f.objectiveId+'/inputs';
    const denied=await app.inject({method:'POST',url,payload:body});expect(denied.statusCode).toBe(401);
    const allowed=await app.inject({method:'POST',url,payload:body,headers:{authorization:'Bearer '+config.apiToken}});
    expect(allowed.statusCode).toBe(200);expect(allowed.json().liveAttemptsChanged).toBe(false);expect(allowed.body).not.toContain(body.base64);
  }finally{await app.close();}
});

it('supplies verified operator context to the real compact planner before choosing source paths',async()=>{
  const f=await fixture(),bytes=Buffer.from('Actual runtime source: bridge/runtime-runner.mjs. Use the registered protected release chain, not an invented pipeline.');
  const body=request({name:'runtime-source.txt',digest:digest(bytes),base64:bytes.toString('base64')});
  const registered=await registerObjectiveInput(f.objectiveId,body,auth);
  await reconcilePlanning();const run=await claimPlanning();let generated=0;
  vi.spyOn(accountBridge,'request').mockImplementation(async(method,p:any)=>{
    if(method==='memory.search')return {items:[]};
    if(method==='worker.workspaces')return [{id:'test/base'}];
    if(method==='worker.prepare')return {};
    if(method==='generate'){
      generated++;
      const message=JSON.parse(p.messages[0].content),entry=message.objectiveInputs.inputs[0];
      expect(entry).toMatchObject({id:registered.inputId,digest:body.digest,content:bytes.toString()});
      expect(p.system).toContain('not authorization');expect(p.system).toContain('bounded investigation');
      return {message:{content:[{type:'text',text:JSON.stringify(decision)}]}};
    }
    throw Error('ranking fallback');
  });
  await new PlanningEngine().execute(run,new AbortController().signal);
  expect(generated).toBe(1);
  const saved=(await pool.query('SELECT state,context FROM planning_runs WHERE id=$1',[run.id])).rows[0];
  expect(saved.state).toBe('completed');expect(saved.context.objectiveInputs.inputs[0].digest).toBe(body.digest);
  const event=(await pool.query("SELECT payload FROM events WHERE entity_id=$1 AND kind='PLANNING_DECISION_REQUESTED'",[f.objectiveId])).rows[0].payload;
  expect(event.objectiveInputs).toEqual([{id:registered.inputId,digest:body.digest,included:true}]);
  expect(JSON.stringify(event)).not.toContain(bytes.toString());
});

it('fails closed before model generation when operator provenance is missing',async()=>{
  const f=await fixture(),registered=await registerObjectiveInput(f.objectiveId,request(),auth);
  await reconcilePlanning();const run=await claimPlanning();let generated=0;
  await pool.query("DELETE FROM events WHERE kind='OBJECTIVE_INPUT_REGISTERED' AND payload->>'inputId'=$1",[registered.inputId]);
  vi.spyOn(accountBridge,'request').mockImplementation(async(method)=>{
    if(method==='memory.search')return {items:[]};if(method==='worker.workspaces')return [{id:'test/base'}];
    if(method==='generate')generated++;throw Error('unexpected bridge method');
  });
  await new PlanningEngine().execute(run,new AbortController().signal);
  expect(generated).toBe(0);
  const failure=(await pool.query('SELECT state,error FROM planning_runs WHERE id=$1',[run.id])).rows[0];
  expect(failure.state).toBe('failed');expect(failure.error).toContain('Proveniência da entrada ausente');
});

it('binds a compact summary to the original bytes without changing worker input transport',async()=>{
  const f=await fixture(),bytes=Buffer.from('large diagnostic record\n'.repeat(1400));
  const summary='The new profile identifies memory traffic as a hypothesis. Verify the original artifact on the exact candidate before accepting any gain.';
  const body=request({name:'profile.txt',digest:digest(bytes),base64:bytes.toString('base64'),planningSummary:summary,requestReplan:false});
  const registered=await registerObjectiveInput(f.objectiveId,body,auth);
  const inputs=await readObjectiveInputs(pool,f.objectiveId);
  expect(inputs[0]).toMatchObject({id:registered.inputId,digest:body.digest,planningSummary:summary});
  expect(inputsForRequirements(inputs,['req-1'])).toEqual([{id:registered.inputId,name:body.name,digest:body.digest,base64:body.base64}]);
  const projection=planningInputContext(inputs,['req-1']);
  expect(projection.contentComplete).toBe(false);
  expect(projection.inputs[0]).toMatchObject({digest:body.digest,omitted:'per-input-budget',summary,originalContentRead:false});
  expect(projection.inputs[0]).not.toHaveProperty('content');
  await expect(registerObjectiveInput(f.objectiveId,{...body,planningSummary:summary+' Changed claim.'},auth))
    .rejects.toMatchObject({code:'IDEMPOTENCY_CONFLICT'});
});

it.each(['oversized','nul','secret'])('rejects %s summaries before registration',async kind=>{
  const f=await fixture();
  const planningSummary=kind==='oversized'?'x'.repeat(3001):kind==='nul'?'This summary contains\0unacceptable null bytes.':
    ['-----BEGIN','PRIVATE KEY-----'].join(' ');
  await expect(registerObjectiveInput(f.objectiveId,request({planningSummary}),auth)).rejects.toThrow();
  expect((await pool.query('SELECT count(*)::int n FROM objective_inputs WHERE goal_id=$1',[f.objectiveId])).rows[0].n).toBe(0);
});

it('delivers a receipt-bound summary to the real planner and records summary-only consumption',async()=>{
  const f=await fixture(),bytes=Buffer.from('full diagnostic data\n'.repeat(1000));
  const summary='Compare the exact-source GPU receipt before repeating this experiment. The full profile is an input for the worker; this is only its operator summary.';
  const body=request({name:'profile.txt',digest:digest(bytes),base64:bytes.toString('base64'),planningSummary:summary});
  const registered=await registerObjectiveInput(f.objectiveId,body,auth);
  await reconcilePlanning();const run=await claimPlanning();let generated=0;
  vi.spyOn(accountBridge,'request').mockImplementation(async(method,p:any)=>{
    if(method==='memory.search')return {items:[]};if(method==='worker.workspaces')return [{id:'test/base'}];if(method==='worker.prepare')return {};
    if(method==='generate'){
      generated++;
      const entry=JSON.parse(p.messages[0].content).objectiveInputs.inputs[0];
      expect(entry).toMatchObject({id:registered.inputId,digest:body.digest,summary,originalContentRead:false,omitted:'per-input-budget'});
      expect(entry).not.toHaveProperty('content');expect(p.system).toContain('not as the full document');
      return {message:{content:[{type:'text',text:JSON.stringify(decision)}]}};
    }
    throw Error('ranking fallback');
  });
  await new PlanningEngine().execute(run,new AbortController().signal);
  expect(generated).toBe(1);
  expect((await pool.query('SELECT state FROM planning_runs WHERE id=$1',[run.id])).rows[0].state).toBe('completed');
  const event=(await pool.query("SELECT payload FROM events WHERE entity_id=$1 AND kind='PLANNING_DECISION_REQUESTED'",[f.objectiveId])).rows[0].payload;
  expect(event.objectiveInputs).toEqual([{id:registered.inputId,digest:body.digest,included:false,omitted:'per-input-budget',summaryIncluded:true,summaryDigest:digest(Buffer.from(summary))}]);
  expect(JSON.stringify(event)).not.toContain(summary);
});
