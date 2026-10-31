import {afterAll,afterEach,beforeAll,beforeEach,expect,it,vi} from 'vitest';
import {randomUUID} from 'node:crypto';
import {readModelPolicy} from '../bridge/model-policy.mjs';
import {pool,migrate} from '../server/db.js';
import {seedRegistry} from '../server/registry.js';
import {submitObjective} from '../server/planner.js';
import {objectiveSchema} from '../server/objective-contracts.js';
import {makeApp} from '../server/app.js';
import {config} from '../server/config.js';
import {cancelTask} from '../server/service.js';
import {DomainError} from '../server/contracts.js';
import {installObjectiveReview,configureObjectiveReview,recordObjectiveLoop,objectiveReviews,ObjectiveReviewEngine} from '../server/objective-review.js';
vi.mock('../bridge/model-policy.mjs',async importOriginal=>({...await importOriginal<any>(),readModelPolicy:vi.fn()}));

// Synthetic PostgreSQL/transport fixtures, never autonomy qualification or real provider calls.
const policy={version:1,mode:'glm-only',accountId:'agentos-api-00000000-0000-4000-8000-000000000001',
  bridgeId:'agentos:api:00000000-0000-4000-8000-000000000001',model:'glm-5.3-flash',requestLimit:6,sessionLimit:12} as const;
let f:any,engine:ObjectiveReviewEngine|undefined;
beforeAll(async()=>{
  await migrate();await installObjectiveReview();
});
beforeEach(async()=>{
  vi.mocked(readModelPolicy).mockReturnValue(policy);
  await pool.query('TRUNCATE goals,agents,accounts,memories,events CASCADE');
  await pool.query("DELETE FROM settings WHERE id LIKE 'workforce:%'");await seedRegistry();
  await pool.query(`INSERT INTO accounts(id,label,provider,model,models,bridge_id,enabled,allow_unknown,max_concurrency)
    VALUES($1,'Synthetic GLM','openai',$2,$3,$4,true,true,12)`,[policy.accountId,policy.model,JSON.stringify([policy.model]),policy.bridgeId]);
  const {objectiveId:goal}=await submitObjective(objectiveSchema.parse({message:'Preservar o insumo e corrigir o consumidor.',project:'test',
    workspaces:['test/base'],mode:'execute',requirements:['O original deve passar.'],idempotencyKey:randomUUID()}));
  await pool.query("UPDATE goals SET state='active' WHERE id=$1",[goal]);
  const taskId=randomUUID(),runId=randomUUID(),fence=randomUUID(),context=randomUUID();
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,attempt_count,covers)
    VALUES($1,$2,'target','Corrigir consumidor','Immutable task','qa.backend','running','work','[]','{"kind":"harness"}',1,'["req-1"]')`,[taskId,goal]);
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,context_id) VALUES($1,$2,1,$3,'harness',1,'running',$4)",[runId,taskId,fence,context]);
  await pool.query("INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,'{}','Immutable scope')",[context,runId]);
  f={goal,taskId,runId,fence,context};
});
afterEach(async()=>{await engine?.close();engine=undefined;vi.restoreAllMocks();});
afterAll(()=>pool.end());
const observation=(offset=0,start=Date.now()-10000)=>({runId:f.runId,observedAt:new Date().toISOString(),files:[],evidence:[],
  actions:Array.from({length:6},(_,i)=>({id:'call-'+(offset+i),sequence:offset+i,tool:'bash',command:'read missing original',
    output:'not found',state:'returned',exitCode:0,startedAt:new Date(start+i*50).toISOString(),finishedAt:new Date(start+i*50+1).toISOString()}))});
async function record(value=observation()){
  const id=(await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('EXECUTION_OBSERVED',$1,$2) RETURNING id",
    [f.taskId,JSON.stringify({runId:f.runId,observation:value})])).rows[0].id;
  return recordObjectiveLoop(f.runId,String(id),value);
}
const runtimeObservation=()=>{const base=observation(),start=Date.now()-10000;return {...base,actions:Array.from({length:12},(_,i)=>({
 ...base.actions[0],id:'diagnostic-'+i,sequence:i,command:'node isolated-'+i+'.ts',
 output:`Error [ERR_MODULE_NOT_FOUND]: Cannot load different-module-${i}.ts`,
 startedAt:new Date(start+i*50).toISOString(),finishedAt:new Date(start+i*50+1).toISOString()}))};};
async function current(){return (await pool.query('SELECT * FROM objective_review_signals WHERE run_id=$1',[f.runId])).rows[0];}
async function until(state:string){await vi.waitFor(async()=>expect((await current())?.state).toBe(state),{timeout:5000,interval:25});}
function bridge(options:{kind?:string;failGenerate?:boolean;ambiguous?:boolean;pending?:Promise<any>}={}){
  let intent:any;const messageId=randomUUID();
  const dispatch=vi.fn(async(method:string,params:any)=>{
    if(method==='generate'){
      expect(params.accountId).toBe(policy.bridgeId);expect(params.model).toBe(policy.model);
      if(options.failGenerate)throw Error('provider timeout');
      if(options.pending)return options.pending;
      const evidence=JSON.parse(params.messages[0].content);
      return {message:{content:[{type:'text',text:JSON.stringify({kind:options.kind??'guide',evidenceDigest:evidence.signal.evidenceDigest,
        reason:'The original input is missing, so another identical read cannot help.',
        instruction:'Preserve the exact negative and identify the missing original input within the existing task scope.'})}]}};
    }
    if(method==='worker.guide'){
      intent=params;if(options.ambiguous)throw Error('reply lost after remote queue');
      return {...params,messageId,state:'queued_next_step'};
    }
    if(method==='worker.guidanceReceipt')return {...intent,messageId,state:'consumed',consumedAt:new Date(Date.now()-1000).toISOString()};
    throw Error('UNEXPECTED_METHOD');
  });
  return dispatch;
}

it('observes by default and coalesces repeated evidence without model calls or worker mutation',async()=>{
  const a=await record(),b=await record(observation(10));
  expect(a?.id).toBe(b?.id);expect((await current()).occurrences).toBe(2);
  const dispatch=bridge();engine=new ObjectiveReviewEngine(dispatch);await engine.tick();
  expect(dispatch).not.toHaveBeenCalled();expect((await current()).state).toBe('observed');
  expect((await pool.query('SELECT state FROM runs WHERE id=$1',[f.runId])).rows[0].state).toBe('running');
});
it('uses one GLM review and fenced guidance, preserving criteria, snapshots and acceptance',async()=>{
  await configureObjectiveReview(f.goal,{mode:'supervised'});await record();
  const before=(await pool.query('SELECT instruction_snapshot FROM runs WHERE id=$1',[f.runId])).rows[0];
  const dispatch=bridge();engine=new ObjectiveReviewEngine(dispatch);await engine.tick();await until('queued_next_step');
  await engine.tick();expect(dispatch.mock.calls.filter(c=>c[0]==='generate')).toHaveLength(1);
  expect(dispatch.mock.calls.filter(c=>c[0]==='worker.guide')).toHaveLength(1);
  expect((await pool.query('SELECT instruction_snapshot FROM runs WHERE id=$1',[f.runId])).rows[0]).toEqual(before);
  expect((await pool.query('SELECT 1 FROM acceptances WHERE run_id=$1',[f.runId])).rowCount).toBe(0);
  expect((await objectiveReviews(f.goal)).stopAuthority).toBe(false);
});
it('coalesces varied commands with one recurring runtime diagnostic into one advisory GLM owner',async()=>{
 await configureObjectiveReview(f.goal,{mode:'supervised'});
 const first=await record(runtimeObservation()),second=runtimeObservation();
 second.actions=second.actions.map((a,i)=>({...a,sequence:i+20,command:'node changed-'+i+'.ts',output:a.output+'\nnew stack location'}));
 expect((await record(second))?.id).toBe(first?.id);
 expect((await current()).signal).toMatchObject({pattern:'recurring_runtime_error',runtimeErrors:[{code:'ERR_MODULE_NOT_FOUND',count:12}]});
 const dispatch=bridge();engine=new ObjectiveReviewEngine(dispatch);await engine.tick();await until('queued_next_step');
 await engine.tick();expect(dispatch.mock.calls.filter(c=>c[0]==='generate')).toHaveLength(1);
 const guide=dispatch.mock.calls.find(c=>c[0]==='worker.guide')![1];
 expect(guide).toMatchObject({runId:f.runId,runFence:f.fence,contextVersion:f.context});
 expect(guide.facts[0].statement).toContain('intentional negatives');
 expect(dispatch.mock.calls.some(c=>/cancel|execute/.test(c[0]))).toBe(false);
 expect((await pool.query('SELECT count(*)::int n FROM acceptances WHERE run_id=$1',[f.runId])).rows[0].n).toBe(0);
 expect((await pool.query('SELECT state FROM runs WHERE id=$1',[f.runId])).rows[0].state).toBe('running');
});
it('allows recurring diagnostics to be intentional negative tests without stopping or guiding the worker',async()=>{
 await configureObjectiveReview(f.goal,{mode:'supervised'});await record(runtimeObservation());
 const dispatch=bridge({kind:'continue'});engine=new ObjectiveReviewEngine(dispatch);await engine.tick();await until('monitoring');
 await engine.tick();expect(dispatch.mock.calls.filter(c=>c[0]==='generate')).toHaveLength(1);
 expect(dispatch.mock.calls.some(c=>c[0].startsWith('worker.'))).toBe(false);
 expect((await pool.query('SELECT state FROM runs WHERE id=$1',[f.runId])).rows[0].state).toBe('running');
});
it('does not review a historical diagnostic window after fresh observations show successful work',async()=>{
 await configureObjectiveReview(f.goal,{mode:'supervised'});await record(runtimeObservation());
 const resolved=runtimeObservation();resolved.actions=resolved.actions.map((a,i)=>({...a,sequence:i+20,output:'new bounded successful check '+i}));
 await record(resolved);
 const dispatch=bridge();engine=new ObjectiveReviewEngine(dispatch);await engine.tick();
 expect(dispatch).not.toHaveBeenCalled();expect((await current()).state).toBe('monitoring');
 expect((await current()).error_code).toBe('PATTERN_NO_LONGER_CURRENT');
});
it('queues canonical input/replan feedback without broadening the live task or stopping it',async()=>{
  await configureObjectiveReview(f.goal,{mode:'supervised'});await record();
  const dispatch=bridge({kind:'request_input'});engine=new ObjectiveReviewEngine(dispatch);await engine.tick();await until('queued_next_step');
  expect((await pool.query('SELECT 1 FROM objective_feedback WHERE objective_id=$1',[f.goal])).rowCount).toBe(1);
  expect((await pool.query("SELECT 1 FROM events WHERE kind='OBJECTIVE_INPUT_REQUESTED' AND entity_id=$1",[f.goal])).rowCount).toBe(1);
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1',[f.taskId])).rows[0].state).toBe('running');
  expect(dispatch.mock.calls.some(c=>c[0].includes('cancel'))).toBe(false);
});
it('retains the actor and schedules review retry after provider failure, never a stop decision',async()=>{
  await configureObjectiveReview(f.goal,{mode:'supervised'});await record();
  const dispatch=bridge({failGenerate:true});engine=new ObjectiveReviewEngine(dispatch);await engine.tick();
  await vi.waitFor(async()=>expect((await current()).error_code).toBe('REVIEW_RETRY_PENDING'),{timeout:5000});
  await engine.tick();expect(dispatch).toHaveBeenCalledTimes(1);
  expect((await pool.query('SELECT state FROM runs WHERE id=$1',[f.runId])).rows[0].state).toBe('running');
});
it('respects authentication and exhausted quota metadata without substituting another model',async()=>{
  await configureObjectiveReview(f.goal,{mode:'supervised'});await record();
  await pool.query("UPDATE accounts SET quota_status='exhausted' WHERE id=$1",[policy.accountId]);
  const dispatch=bridge();engine=new ObjectiveReviewEngine(dispatch);await engine.tick();
  expect(dispatch).not.toHaveBeenCalled();expect((await current()).error_code).toBe('REVIEW_PROVIDER_UNAVAILABLE');
  expect((await pool.query('SELECT state FROM runs WHERE id=$1',[f.runId])).rows[0].state).toBe('running');
});
it('does not apply a late judgment after the actor advances or cancellation is requested',async()=>{
  await configureObjectiveReview(f.goal,{mode:'supervised'});await record();
  let finish!:(value:any)=>void;const pending=new Promise(r=>{finish=r;});
  const dispatch=bridge({pending});engine=new ObjectiveReviewEngine(dispatch);await engine.tick();await until('generating');
  await cancelTask(f.taskId);
  finish({message:{content:[{type:'text',text:JSON.stringify({kind:'guide',evidenceDigest:(await current()).evidence_digest,
    reason:'The repeated calls do not change the negative.',instruction:'Seal the actual bounded negative without changing criteria.'})}]}});
  await until('superseded');expect(dispatch.mock.calls.filter(c=>c[0]==='worker.guide')).toHaveLength(0);
});
it('invalidates a pending review when a fresh snapshot shows new progress',async()=>{
  await configureObjectiveReview(f.goal,{mode:'supervised'});await record();
  const changed=observation(20);changed.actions[5].output='new result';await record(changed);
  const dispatch=bridge();engine=new ObjectiveReviewEngine(dispatch);await engine.tick();
  expect(dispatch).not.toHaveBeenCalled();expect((await current()).state).toBe('monitoring');
});
it('probes an ambiguous guidance receipt instead of sending it twice and requires real acceptance for effect',async()=>{
  await configureObjectiveReview(f.goal,{mode:'supervised'});await record();
  const dispatch=bridge({ambiguous:true});engine=new ObjectiveReviewEngine(dispatch);await engine.tick();await until('uncertain');
  await pool.query("UPDATE objective_review_signals SET last_receipt_at=now()-interval '1 minute' WHERE run_id=$1",[f.runId]);
  await engine.tick();expect((await current()).state).toBe('consumed');
  expect(dispatch.mock.calls.filter(c=>c[0]==='worker.guide')).toHaveLength(1);
  expect((await pool.query("SELECT 1 FROM events WHERE kind='OBJECTIVE_REVIEW_EFFECT_VERIFIED' AND entity_id=$1",[f.goal])).rowCount).toBe(0);
  await pool.query("UPDATE tasks SET state='accepted' WHERE id=$1",[f.taskId]);
  await pool.query("INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) VALUES($1,$2,$3,true,'[]','synthetic-test')",[randomUUID(),f.taskId,f.runId]);
  await pool.query("UPDATE objective_review_signals SET last_receipt_at=now()-interval '1 minute' WHERE run_id=$1",[f.runId]);
  await engine.tick();expect((await current()).state).toBe('verified');
});
it('reopens post-consumption repetition with immutable action history, not a duplicate pending instruction',async()=>{
  await configureObjectiveReview(f.goal,{mode:'supervised'});await record();
  const dispatch=bridge();engine=new ObjectiveReviewEngine(dispatch);await engine.tick();await until('queued_next_step');
  await pool.query("UPDATE objective_review_signals SET last_receipt_at=now()-interval '1 minute' WHERE run_id=$1",[f.runId]);
  await engine.tick();const consumed=await current();expect(consumed.state).toBe('consumed');
  await record(observation(100,Date.now()-500));const followup=await current();
  expect(followup.state).toBe('queued');expect(followup.history).toHaveLength(1);
  expect(followup.history[0].intent.actionId).toBe(consumed.action_intent.actionId);
  expect(followup.action_intent).toBeNull();
  await expect(pool.query("UPDATE objective_review_signals SET history='[]' WHERE id=$1",[followup.id])).rejects.toThrow('OBJECTIVE_REVIEW_HISTORY_IMMUTABLE');
  expect(dispatch.mock.calls.filter(c=>c[0]==='worker.guide')).toHaveLength(1);
});
it('does not create a second supervisor for workforce-owned objectives and protects policy routes',async()=>{
  await pool.query('INSERT INTO settings VALUES($1,$2)',['workforce:'+f.goal,JSON.stringify({enabled:true})]);
  expect(await record()).toBeNull();
  await expect(configureObjectiveReview(f.goal,{mode:'supervised'})).rejects.toMatchObject({code:'OBJECTIVE_REVIEW_EXISTING_OWNER'});
  await pool.query('DELETE FROM settings WHERE id=$1',['workforce:'+f.goal]);
  const app=makeApp();
  try{
    const url='/api/objectives/'+f.goal+'/review-policy';
    expect((await app.inject({method:'POST',url,payload:{mode:'supervised'}})).statusCode).toBe(401);
    expect((await app.inject({method:'POST',url,payload:{mode:'supervised'},headers:{authorization:'Bearer '+config.apiToken}})).statusCode).toBe(200);
  }finally{await app.close();}
});

const privateDiagnosticSentinel='PRIVATE_MODEL_BODY_do_not_persist';
async function diagnostic(){
  return (await pool.query("SELECT payload FROM events WHERE kind='OBJECTIVE_REVIEW_ATTEMPT_FAILED' AND entity_id=$1 ORDER BY id DESC LIMIT 1",[f.goal])).rows[0]?.payload;
}
it.each([
  ['extract_response',{message:{content:null}},'REVIEW_RESPONSE_INVALID'],
  ['parse_decision',{message:{content:[{type:'text',text:privateDiagnosticSentinel}]}},'REVIEW_JSON_INVALID'],
  ['validate_decision',{message:{content:[{type:'text',text:JSON.stringify({kind:'invalid',secret:privateDiagnosticSentinel})}]}},'REVIEW_SCHEMA_INVALID'],
  ['validate_evidence',{message:{content:[{type:'text',text:JSON.stringify({kind:'continue',evidenceDigest:'0'.repeat(64),reason:'Keep the original task unchanged.'})}]}},'REVIEW_EVIDENCE_CHANGED'],
] as const)('records the exact %s boundary without retaining model output or changing retry behavior',async(phase,response,code)=>{
  await configureObjectiveReview(f.goal,{mode:'supervised'});await record();
  const dispatch=vi.fn(async()=>response);engine=new ObjectiveReviewEngine(dispatch);await engine.tick();
  await vi.waitFor(async()=>expect(await diagnostic()).toMatchObject({phase,code,responseReceived:true,modelResultRecorded:true}),{timeout:5000});
  const row=await current(),d=await diagnostic();
  expect(row).toMatchObject({state:'queued',error_code:'REVIEW_RETRY_PENDING',generation_attempt:1,decision:null,action_intent:null});
  expect(d).toEqual({version:1,reviewId:row.id,runId:f.runId,generationFence:row.generation_fence,generationAttempt:1,
    phase,code,responseReceived:true,modelResultRecorded:true,retryAfterSeconds:60,stopAuthority:false});
  expect(+new Date(row.next_attempt_at)-+new Date(row.updated_at)).toBe(60000);
  const events=(await pool.query("SELECT kind,payload FROM events WHERE entity_id=$1 AND kind LIKE 'OBJECTIVE_REVIEW_%' ORDER BY id",[f.goal])).rows;
  expect(events.map(e=>e.kind).slice(-3)).toEqual(['OBJECTIVE_REVIEW_STARTED','OBJECTIVE_REVIEW_MODEL_RESULT','OBJECTIVE_REVIEW_ATTEMPT_FAILED']);
  expect(JSON.stringify(events)).not.toContain(privateDiagnosticSentinel);
  await engine.tick();expect(dispatch).toHaveBeenCalledTimes(1);
  expect((await pool.query('SELECT state FROM runs WHERE id=$1',[f.runId])).rows[0].state).toBe('running');
  expect((await pool.query('SELECT 1 FROM acceptances WHERE run_id=$1',[f.runId])).rowCount).toBe(0);
});
it.each(['typed','untrusted'] as const)('records a %s dispatch error using only controller-owned codes and the generation fence',async(kind)=>{
  await configureObjectiveReview(f.goal,{mode:'supervised'});await record();
  const error=kind==='typed'?new DomainError('STOP_UNCONFIRMED',privateDiagnosticSentinel):
    Object.assign(Error(privateDiagnosticSentinel),{code:privateDiagnosticSentinel,headers:{authorization:privateDiagnosticSentinel}});
  const dispatch=vi.fn(async()=>{throw error;});engine=new ObjectiveReviewEngine(dispatch);await engine.tick();
  await vi.waitFor(async()=>expect(await diagnostic()).toMatchObject({phase:'model_request',code:kind==='typed'?'STOP_UNCONFIRMED':'REVIEW_GENERATION_FAILED',
    responseReceived:false,modelResultRecorded:false}),{timeout:5000});
  const row=await current(),d=await diagnostic();
  const started=(await pool.query("SELECT payload FROM events WHERE kind='OBJECTIVE_REVIEW_STARTED' AND entity_id=$1",[f.goal])).rows[0].payload;
  expect(started).toMatchObject({reviewId:row.id,generationFence:d.generationFence,generationAttempt:1});
  expect((await pool.query("SELECT 1 FROM events WHERE kind='OBJECTIVE_REVIEW_MODEL_RESULT' AND entity_id=$1",[f.goal])).rowCount).toBe(0);
  expect(JSON.stringify(d)).not.toContain(privateDiagnosticSentinel);
  await engine.tick();expect(dispatch).toHaveBeenCalledTimes(1);
});
it('does not attribute a late failure to a superseding generation or requeue its owner',async()=>{
  await configureObjectiveReview(f.goal,{mode:'supervised'});await record();
  let reject!:(reason:unknown)=>void;
  const pending=new Promise((_,r)=>{reject=r;});
  engine=new ObjectiveReviewEngine(vi.fn(async()=>pending));await engine.tick();await until('generating');
  const row=await current(),newFence=randomUUID();
  await pool.query("UPDATE objective_review_signals SET generation_fence=$2,generation_attempt=2 WHERE id=$1",[row.id,newFence]);
  reject(Error(privateDiagnosticSentinel));await engine.close();engine=undefined;
  expect(await diagnostic()).toBeUndefined();
  expect(await current()).toMatchObject({state:'generating',generation_fence:newFence,generation_attempt:2,error_code:null});
});
