import {afterAll,afterEach,beforeAll,beforeEach,expect,it,vi} from 'vitest';
import {randomUUID} from 'node:crypto';
import {readModelPolicy} from '../bridge/model-policy.mjs';
import {pool,migrate} from '../server/db.js';
import {seedRegistry} from '../server/registry.js';
import {submitObjective} from '../server/planner.js';
import {objectiveSchema} from '../server/objective-contracts.js';
import {config} from '../server/config.js';
import {makeApp} from '../server/app.js';
import {operationHash} from '../server/operations-contract.js';
import {installObjectiveReview,configureObjectiveReview,requestObjectiveReview,ObjectiveReviewEngine} from '../server/objective-review.js';
vi.mock('../bridge/model-policy.mjs',async original=>({...await original<any>(),readModelPolicy:vi.fn()}));

// Real transactions and HTTP auth in the isolated PostgreSQL test database;
// synthetic transport only. These are not provider/autonomy qualification.
const policy={version:1,mode:'glm-only',accountId:'agentos-api-00000000-0000-4000-8000-000000000001',
  bridgeId:'agentos:api:00000000-0000-4000-8000-000000000001',model:'glm-5.3-flash',requestLimit:6,sessionLimit:12} as const;
let f:any,engine:ObjectiveReviewEngine|undefined;
const auth={channel:'api-token'} as const;
beforeAll(async()=>{await migrate();await installObjectiveReview();});
beforeEach(async()=>{
  vi.mocked(readModelPolicy).mockReturnValue(policy);
  await pool.query('TRUNCATE goals,agents,accounts,memories,events CASCADE');
  await pool.query("DELETE FROM settings WHERE id LIKE 'workforce:%' OR id LIKE 'objective-review:%'");
  await seedRegistry();
  await pool.query(`INSERT INTO accounts(id,label,provider,model,models,bridge_id,enabled,allow_unknown,max_concurrency)
    VALUES($1,'Synthetic GLM','openai',$2,$3,$4,true,true,12)`,[policy.accountId,policy.model,JSON.stringify([policy.model]),policy.bridgeId]);
  const {objectiveId:goal}=await submitObjective(objectiveSchema.parse({message:'Fix the exact input consumer and preserve scope.',project:'test',
    workspaces:['test/base'],mode:'execute',requirements:['Original tests must pass.'],idempotencyKey:randomUUID()}));
  await pool.query("UPDATE goals SET state='active' WHERE id=$1",[goal]);
  const taskId=randomUUID(),runId=randomUUID(),fence=randomUUID(),context=randomUUID();
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,attempt_count,covers)
    VALUES($1,$2,'target','Input consumer','Immutable task','qa.backend','running','work','[]','{"kind":"harness"}',1,'["req-1"]')`,[taskId,goal]);
  await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,context_id,instruction_snapshot)
    VALUES($1,$2,1,$3,'harness',1,'running',$4,$5)`,[runId,taskId,fence,context,JSON.stringify({
      task:{description:'Immutable scope: fix only the consumer and its original tests.',criteria:[{id:'original',type:'test',description:'Preserve the original input.'}]}})]);
  await pool.query("INSERT INTO contexts(id,run_id,manifest,content,created_at) VALUES($1,$2,'{}','Immutable scope',now()-interval '1 minute')",[context,runId]);
  f={goal,taskId,runId,fence,context};
  await observe();
});
afterEach(async()=>{await engine?.close();engine=undefined;vi.unstubAllEnvs();vi.restoreAllMocks();});
afterAll(()=>pool.end());
function snapshot(){return {runId:f.runId,observedAt:new Date().toISOString(),workspace:'test/base',runtimeState:'running',traceTruncated:true,
  actions:[{id:'redacted',sequence:1,tool:'bash',command:'[Comando envolvendo credenciais omitido]',
    output:'[Saida omitida]',state:'returned',exitCode:0},
    {id:'test',sequence:2,tool:'bash',command:'run original test',output:'DomainError: receipt mismatch',state:'error',exitCode:1}],
  files:[{path:'consumer.ts',status:'modified',added:'2',removed:'1'}],evidence:[]};}
async function observe(value=snapshot()){
  f.observation=value;
  f.eventId=String((await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('EXECUTION_OBSERVED',$1,$2) RETURNING id",
    [f.taskId,JSON.stringify({runId:f.runId,observation:value})])).rows[0].id);
  return f.eventId;
}
function packet(){return {requestKey:'review-synthetic-1',taskId:f.taskId,runId:f.runId,runFence:f.fence,
  contextVersion:f.context,expectedAttempt:1,expectedRevision:0,observationEventId:f.eventId,
  reason:'The persisted test failure may be caused by different hash projections. Review the visible consumer failure without changing its criteria.'};}
const request=(raw:any=packet())=>requestObjectiveReview(f.goal,raw,auth);
async function row(){return (await pool.query('SELECT * FROM objective_review_signals WHERE run_id=$1 ORDER BY created_at DESC',[f.runId])).rows[0];}
async function until(state:string){await vi.waitFor(async()=>expect((await row())?.state).toBe(state),{timeout:5000,interval:20});}
function decision(digest:string,kind='guide'){return {message:{content:[{type:'text',text:JSON.stringify({kind,evidenceDigest:digest,
  reason:'The two hash projections differ in the existing consumer.',
  instruction:'Compare the canonical input fields on write and replay; preserve the integrity assertion and original test input.'})}]}};}
function transport(options:{kind?:string;pending?:Promise<any>;ambiguous?:boolean;fail?:boolean}={}){
  let intent:any;const messageId=randomUUID();
  return vi.fn(async(method:string,params:any)=>{
    if(method==='generate'){
      expect(params.accountId).toBe(policy.bridgeId);expect(params.model).toBe(policy.model);
      if(options.fail)throw Error('GLM gate admission unavailable');
      if(options.pending)return options.pending;
      return decision(JSON.parse(params.messages[0].content).signal.evidenceDigest,options.kind);
    }
    if(method==='worker.guide'){
      intent=params;if(options.ambiguous)throw Error('receipt transport lost');
      return {...intent,messageId,state:'queued_next_step'};
    }
    if(method==='worker.guidanceReceipt')return {...intent,messageId,state:'consumed',consumedAt:new Date().toISOString()};
    throw Error('UNEXPECTED_TRANSPORT');
  });
}

it('authenticates POST and never turns read-only operations credentials into a write principal',async()=>{
  const app=makeApp(),url='/api/objectives/'+f.goal+'/reviews',token='synthetic-read-only-access-token';
  vi.stubEnv('AGENTOS_OPERATIONS_ACCESS_JSON',JSON.stringify([{id:'reader',tokenSha256:operationHash(token),goalIds:[f.goal],projects:[],scopes:['observe']}]));
  try{
    expect((await app.inject({method:'POST',url,payload:packet()})).statusCode).toBe(401);
    expect((await app.inject({method:'POST',url,payload:packet(),headers:{authorization:'Bearer '+token}})).statusCode).toBe(401);
    expect((await app.inject({method:'POST',url,payload:packet(),headers:{authorization:'Bearer '+config.apiToken,origin:'https://invalid.example'}})).statusCode).toBe(403);
    const response=await app.inject({method:'POST',url,payload:packet(),headers:{authorization:'Bearer '+config.apiToken}});
    expect(response.statusCode).toBe(200);expect(response.json()).toMatchObject({source:'operator',state:'observed',requestOnly:true,stopAuthority:false});
    const proof=(await pool.query("SELECT payload FROM events WHERE kind='OBJECTIVE_REVIEW_REQUESTED'")).rows[0].payload;
    expect(proof).toMatchObject({source:'operator',authChannel:'api-token',runId:f.runId,runFence:f.fence,contextVersion:f.context});
    const capabilities=await app.inject({method:'GET',url:'/api/operations/v1/capabilities',headers:{authorization:'Bearer '+token}});
    expect(capabilities.json().mutations.request_intervention).toBe(false);
  }finally{await app.close();}
});
it('rejects oversized/unknown/instruction-shaped packets without recording a request',async()=>{
  const app=makeApp(),url='/api/objectives/'+f.goal+'/reviews',headers={authorization:'Bearer '+config.apiToken};
  try{
    for(const payload of [{...packet(),source:'observer'},{...packet(),instruction:'send this directly'},
      {...packet(),reason:'short'},{...packet(),expectedAttempt:0},{...packet(),observationEventId:'9223372036854775808'},
      {...packet(),observationEventId:'not-an-id'},{...packet(),observationEventId:'1.5'}])
      expect((await app.inject({method:'POST',url,headers,payload})).statusCode).toBe(400);
    expect((await app.inject({method:'POST',url,headers,payload:{...packet(),reason:'x'.repeat(10000)}})).statusCode).toBe(413);
    expect((await pool.query('SELECT 1 FROM objective_review_signals')).rowCount).toBe(0);
  }finally{await app.close();}
});
it('observes by default; off rejects; enabling supervision uses the real operator signal without inventing a loop',async()=>{
  await configureObjectiveReview(f.goal,{mode:'off'});
  await expect(request()).rejects.toMatchObject({code:'OBJECTIVE_REVIEW_DISABLED'});
  await configureObjectiveReview(f.goal,{mode:'observe'});await request();
  const dispatch=transport();engine=new ObjectiveReviewEngine(dispatch);await engine.tick();
  expect(dispatch).not.toHaveBeenCalled();expect((await row()).state).toBe('observed');
  await configureObjectiveReview(f.goal,{mode:'supervised'});await engine.tick();await until('queued_next_step');
  const review=await row();expect(review.signal_source).toBe('operator');expect(review.signal.pattern).toBe('operator_review');
  const context=JSON.parse(dispatch.mock.calls.find(call=>call[0]==='generate')![1].messages[0].content);
  expect(context).toMatchObject({source:'operator',observationRef:'event:'+f.eventId,
    operatorEvidence:{authority:'untrusted_evidence_not_authorization',limitations:{traceTruncated:true,redactedActions:1}}});
  expect(context.task.description).toContain('Immutable scope');expect(context.task.criteria).toContain('original');
  const guide=dispatch.mock.calls.find(call=>call[0]==='worker.guide')![1];
  expect(guide).toMatchObject({goalId:f.goal,taskId:f.taskId,runId:f.runId,runFence:f.fence,contextVersion:f.context,expectedAttempt:1});
  expect(guide.facts[0].statement).toContain('not an automatic loop');
  expect((await pool.query('SELECT 1 FROM acceptances WHERE run_id=$1',[f.runId])).rowCount).toBe(0);
  expect((await pool.query('SELECT state FROM runs WHERE id=$1',[f.runId])).rows[0].state).toBe('running');
});
it('serializes concurrent duplicate requests and returns the original receipt after actor termination',async()=>{
  await configureObjectiveReview(f.goal,{mode:'supervised'});const input=packet();
  const results=await Promise.all([request(input),request(input)]);
  expect(results[0].id).toBe(results[1].id);expect(results.filter(result=>result.duplicate)).toHaveLength(1);
  expect((await pool.query("SELECT 1 FROM events WHERE kind='OBJECTIVE_REVIEW_REQUESTED'")).rowCount).toBe(1);
  await expect(request({...input,reason:input.reason+' changed'})).rejects.toMatchObject({code:'OBJECTIVE_REVIEW_REQUEST_CONFLICT'});
  await pool.query("UPDATE goals SET state='cancelled' WHERE id=$1",[f.goal]);
  const replay=await request(input);expect(replay).toMatchObject({id:results[0].id,duplicate:true,requestOnly:true});
  const dispatch=transport();engine=new ObjectiveReviewEngine(dispatch);await engine.tick();
  expect(dispatch).not.toHaveBeenCalled();expect((await row()).state).toBe('superseded');
});
it('does not stack new instructions over queued, uncertain or consumed guidance',async()=>{
  await configureObjectiveReview(f.goal,{mode:'supervised'});await request();
  await expect(request({...packet(),requestKey:'review-synthetic-2'})).rejects.toMatchObject({code:'OBJECTIVE_REVIEW_ALREADY_PENDING'});
  const dispatch=transport({ambiguous:true});engine=new ObjectiveReviewEngine(dispatch);await engine.tick();await until('uncertain');
  await expect(request({...packet(),requestKey:'review-synthetic-2'})).rejects.toMatchObject({code:'OBJECTIVE_REVIEW_ALREADY_PENDING'});
  await pool.query("UPDATE objective_review_signals SET last_receipt_at=now()-interval '1 minute'");
  await engine.tick();expect((await row()).state).toBe('consumed');
  await expect(request({...packet(),requestKey:'review-synthetic-2'})).rejects.toMatchObject({code:'OBJECTIVE_REVIEW_ALREADY_PENDING'});
  expect(dispatch.mock.calls.filter(call=>call[0]==='worker.guide')).toHaveLength(1);
});
it.each(['taskId','runId','runFence','contextVersion'] as const)('rejects a stale or cross-goal %s',async key=>{
  await expect(request({...packet(),[key]:randomUUID()})).rejects.toMatchObject({code:'OBJECTIVE_REVIEW_TARGET_CHANGED'});
  expect((await pool.query('SELECT 1 FROM objective_review_signals')).rowCount).toBe(0);
});
it.each(['expectedAttempt','expectedRevision'] as const)('rejects changed %s',async key=>{
  const input=packet();await expect(request({...input,[key]:input[key]+1})).rejects.toMatchObject({code:'OBJECTIVE_REVIEW_TARGET_CHANGED'});
});
it('cannot target the right run through a different objective URL',async()=>{
  await expect(requestObjectiveReview(randomUUID(),packet(),auth)).rejects.toMatchObject({code:'OBJECTIVE_REVIEW_TARGET_CHANGED'});
});
it.each(['paused','cancelled','completed'])('rejects goal state %s',async state=>{
  await pool.query('UPDATE goals SET state=$2 WHERE id=$1',[f.goal,state]);
  await expect(request()).rejects.toMatchObject({code:'OBJECTIVE_REVIEW_TARGET_CHANGED'});
});
it.each(['queued','failed','submitted','cancelling','reconciling'])('rejects non-running execution %s',async state=>{
  await pool.query('UPDATE runs SET state=$2 WHERE id=$1',[f.runId,state]);
  await expect(request()).rejects.toMatchObject({code:'OBJECTIVE_REVIEW_TARGET_CHANGED'});
});
it('rejects a cancellation request, another supervisor owner and a non-harness runtime',async()=>{
  await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('CANCEL_REQUESTED',$1,$2)",[f.taskId,JSON.stringify({runId:f.runId})]);
  await expect(request()).rejects.toMatchObject({code:'OBJECTIVE_REVIEW_TARGET_CHANGED'});
  await pool.query("DELETE FROM events WHERE kind='CANCEL_REQUESTED'");
  await pool.query('INSERT INTO settings VALUES($1,$2)',['workforce:'+f.goal,JSON.stringify({enabled:true})]);
  await expect(request()).rejects.toMatchObject({code:'OBJECTIVE_REVIEW_TARGET_CHANGED'});
  await pool.query('DELETE FROM settings WHERE id=$1',['workforce:'+f.goal]);
  await pool.query("UPDATE runs SET runtime='opencode' WHERE id=$1",[f.runId]);
  await expect(request()).rejects.toMatchObject({code:'OBJECTIVE_REVIEW_TARGET_CHANGED'});
});
it('requires the latest bound observation and rejects snapshots predating the context',async()=>{
  const old=packet();await observe();await expect(request(old)).rejects.toMatchObject({code:'OBJECTIVE_REVIEW_EVIDENCE_UNBOUND'});
  await pool.query("UPDATE contexts SET created_at=now()+interval '1 second' WHERE id=$1",[f.context]);
  await expect(request()).rejects.toMatchObject({code:'OBJECTIVE_REVIEW_EVIDENCE_UNBOUND'});
});
it('does not turn unavailable or stale observation into a model call',async()=>{
  await observe({...snapshot(),unavailable:true});await expect(request()).rejects.toMatchObject({code:'OBJECTIVE_REVIEW_EVIDENCE_STALE'});
  await pool.query("UPDATE contexts SET created_at=now()-interval '1 hour' WHERE id=$1",[f.context]);
  await observe({...snapshot(),observedAt:new Date(Date.now()-240000).toISOString()});
  await expect(request()).rejects.toMatchObject({code:'OBJECTIVE_REVIEW_EVIDENCE_STALE'});
});
it('does not bypass GLM policy, quota or the existing bridge generate route when the gate fails',async()=>{
  await configureObjectiveReview(f.goal,{mode:'supervised'});await request();
  vi.mocked(readModelPolicy).mockReturnValue(null as any);
  const dispatch=transport({fail:true});engine=new ObjectiveReviewEngine(dispatch);await engine.tick();
  expect(dispatch).not.toHaveBeenCalled();expect((await row()).error_code).toBe('GLM_POLICY_REQUIRED');
  vi.mocked(readModelPolicy).mockReturnValue(policy);
  await pool.query("UPDATE objective_review_signals SET next_attempt_at=now()");
  await pool.query("UPDATE accounts SET quota_status='exhausted'");await engine.tick();
  expect(dispatch).not.toHaveBeenCalled();expect((await row()).error_code).toBe('REVIEW_PROVIDER_UNAVAILABLE');
  await pool.query("UPDATE accounts SET quota_status='unknown'");
  await pool.query("UPDATE objective_review_signals SET next_attempt_at=now()");await engine.tick();
  await vi.waitFor(async()=>expect((await row()).error_code).toBe('REVIEW_RETRY_PENDING'));
  expect(dispatch.mock.calls.map(call=>call[0])).toEqual(['generate']);
  expect((await pool.query('SELECT state FROM runs WHERE id=$1',[f.runId])).rows[0].state).toBe('running');
});
it('rejects a late judgment after new material, without silently rebinding its reason',async()=>{
  await configureObjectiveReview(f.goal,{mode:'supervised'});await request();
  let resolve!:(value:any)=>void;const pending=new Promise(r=>{resolve=r;});
  const dispatch=transport({pending});engine=new ObjectiveReviewEngine(dispatch);await engine.tick();await until('generating');
  const before=await row();const changed=snapshot();changed.actions[1].output='Original test now passes';await observe(changed);
  resolve(decision(before.evidence_digest));await until('superseded');
  expect((await row()).error_code).toBe('OPERATOR_REVIEW_NO_LONGER_CURRENT');
  expect((await row()).signal.materialDigest).toBe(before.signal.materialDigest);
  expect(dispatch.mock.calls.filter(call=>call[0]==='worker.guide')).toHaveLength(0);
});
it('allows identical rereads/new tokens during generation and anchors guidance after the latest sequence',async()=>{
  await configureObjectiveReview(f.goal,{mode:'supervised'});await request();
  let resolve!:(value:any)=>void;const pending=new Promise(r=>{resolve=r;});
  const dispatch=transport({pending});engine=new ObjectiveReviewEngine(dispatch);await engine.tick();await until('generating');
  const changed:any=snapshot();changed.actions.push({...changed.actions[1],id:'same-read-again',sequence:50});
  changed.traceModifiedAt=new Date().toISOString();changed.toolCalls=51;changed.process={cpuNs:900000,memoryBytes:600000};await observe(changed);
  resolve(decision((await row()).evidence_digest));await until('queued_next_step');
  expect((await row()).action_sequence).toBe(50);expect(dispatch.mock.calls.filter(call=>call[0]==='worker.guide')).toHaveLength(1);
});
it.each(['revision','context','fence','attempt','paused','cancellation','observe'] as const)('rechecks %s after GLM returns',async change=>{
  await configureObjectiveReview(f.goal,{mode:'supervised'});await request();
  let resolve!:(value:any)=>void;const pending=new Promise(r=>{resolve=r;});
  const dispatch=transport({pending});engine=new ObjectiveReviewEngine(dispatch);await engine.tick();await until('generating');
  const before=await row();
  if(change==='revision')await pool.query('UPDATE objectives SET revision=revision+1 WHERE id=$1',[f.goal]);
  if(change==='context'){
    const context=randomUUID();await pool.query("INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,'{}','Changed context')",[context,f.runId]);
    await pool.query('UPDATE runs SET context_id=$2 WHERE id=$1',[f.runId,context]);
  }
  if(change==='fence')await pool.query('UPDATE runs SET fence=$2 WHERE id=$1',[f.runId,randomUUID()]);
  if(change==='attempt'){
    await pool.query("UPDATE runs SET state='failed' WHERE id=$1",[f.runId]);
    await pool.query('UPDATE tasks SET attempt_count=2 WHERE id=$1',[f.taskId]);
    await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,2,$3,'harness',1,'running')",[randomUUID(),f.taskId,randomUUID()]);
  }
  if(change==='paused')await pool.query("UPDATE goals SET state='paused' WHERE id=$1",[f.goal]);
  if(change==='cancellation')await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('CANCEL_REQUESTED',$1,$2)",[f.taskId,JSON.stringify({runId:f.runId})]);
  if(change==='observe')await configureObjectiveReview(f.goal,{mode:'observe'});
  resolve(decision(before.evidence_digest));await until('superseded');
  expect(dispatch.mock.calls.filter(call=>call[0]==='worker.guide')).toHaveLength(0);
  expect((await pool.query('SELECT 1 FROM acceptances')).rowCount).toBe(0);
});
it.each(['signal','provenance','snapshot'] as const)('rejects tampered %s proof before model generation',async kind=>{
  await configureObjectiveReview(f.goal,{mode:'supervised'});await request();const current=await row();
  if(kind==='signal')await pool.query("UPDATE objective_review_signals SET signal=jsonb_set(signal,'{reason}','\"A different claim injected after registration.\"') WHERE id=$1",[current.id]);
  if(kind==='provenance')await pool.query("UPDATE events SET payload=jsonb_set(payload,'{authChannel}','\"observer\"') WHERE id=$1",[current.evidence_event]);
  if(kind==='snapshot')await pool.query("UPDATE events SET payload=jsonb_set(payload,'{observation,actions,1,output}','\"Different unbound output\"') WHERE id=$1",[f.eventId]);
  const dispatch=transport();engine=new ObjectiveReviewEngine(dispatch);await engine.tick();
  expect(dispatch).not.toHaveBeenCalled();expect((await row()).state).toBe('superseded');
});
it('keeps a continue judgment advisory and does not mark any result accepted',async()=>{
  await configureObjectiveReview(f.goal,{mode:'supervised'});await request();
  const dispatch=transport({kind:'continue'});engine=new ObjectiveReviewEngine(dispatch);await engine.tick();await until('monitoring');
  expect(dispatch.mock.calls.map(call=>call[0])).toEqual(['generate']);
  expect((await pool.query('SELECT 1 FROM acceptances')).rowCount).toBe(0);
});
