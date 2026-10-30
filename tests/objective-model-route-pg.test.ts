import {afterAll,afterEach,beforeAll,beforeEach,expect,it,vi} from 'vitest';
import {randomUUID} from 'node:crypto';
import {migrate,pool} from '../server/db.js';
import {config} from '../server/config.js';
import {seedRegistry} from '../server/registry.js';
import {objectiveSchema} from '../server/objective-contracts.js';
import {objectiveModelRoute,registerObjectiveModelRoute,routedPlanningRequest} from '../server/objective-model-route.js';
import {claimPlanning} from '../server/planner.js';
import {makeApp} from '../server/app.js';
import {operationHash} from '../server/operations-contract.js';
import {preparationHash as hash} from '../server/source-preparation-contract.js';

const policyState=vi.hoisted(()=>({current:null as any}));
vi.mock('../bridge/model-policy.mjs',async original=>({...await original<any>(),readModelPolicy:()=>policyState.current}));
const auth={channel:'api-token' as const};
const accountId='agentos-api-00000000-0000-4000-8000-000000000042';
const target={version:2,mode:'exclusive-model',accountId,bridgeId:'agentos:api:00000000-0000-4000-8000-000000000042',
 model:'deepseek-v4-pro-0813',requestLimit:6,sessionLimit:12};
beforeAll(async()=>{expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');await migrate();});
beforeEach(async()=>{
 await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');await seedRegistry();
 await pool.query("DELETE FROM settings WHERE id LIKE 'workforce:%' OR id LIKE 'planning-source-preparation:%' OR id LIKE 'candidate-validation:%' OR id='maintenance'");
 policyState.current={...target};
 await pool.query("INSERT INTO settings(id,value) VALUES('model-policy',$1) ON CONFLICT(id) DO UPDATE SET value=EXCLUDED.value",[JSON.stringify(target)]);
 await pool.query(`INSERT INTO accounts(id,label,provider,model,models,enabled,bridge_id,connection_status,max_concurrency,allow_unknown)
  VALUES($1,'Route fixture','qwen',$2,$3,true,$4,'connected',12,true)`,[accountId,target.model,JSON.stringify([target.model]),target.bridgeId]);
});
afterEach(()=>{policyState.current=null;vi.restoreAllMocks();delete process.env.AGENTOS_OPERATIONS_ACCESS_JSON;});
afterAll(()=>pool.end());
async function fixture(){
 const id=randomUUID(),request=objectiveSchema.parse({message:'Keep the original six accepted products and repair the remaining work.',
  project:'test',mode:'execute',workspaces:['test/base'],requirements:['Keep the original acceptance criteria.'],
  plannerAccountId:'legacy',plannerModel:'glm-5.3-flash',allowedAccounts:['legacy'],idempotencyKey:id});
 await pool.query("INSERT INTO goals(id,title,description,project,state,idempotency_key) VALUES($1::uuid,'Route','Fixture','test','failed',$1::text)",[id]);
 await pool.query("INSERT INTO objectives(id,request,request_hash,mode,status,revision) VALUES($1,$2,$3,'execute','executing',2)",[id,JSON.stringify(request),hash(request)]);
 const old=randomUUID();await pool.query("INSERT INTO planning_runs(id,objective_id,base_revision,trigger_key,state,attempt,model) VALUES($1,$2,1,'old','failed',2,'glm-5.3-flash')",[old,id]);
 const snapshot=await objectiveModelRoute(id);
 const {expectedGoalId,expectedRevision,expectedRequestDigest,expectedPolicyDigest,expectedGenerationId}=snapshot;
 const body={idempotencyKey:randomUUID(),expectedGoalId,expectedRevision,expectedRequestDigest,expectedPolicyDigest,expectedGenerationId,
  reason:'Explicit operator transition to the installed exclusive model policy.'};
 const register=()=>registerObjectiveModelRoute(id,body,auth);
 return {id,request,old,body,register};
}
async function newRun(id:string){
 const run=randomUUID();await pool.query("INSERT INTO planning_runs(id,objective_id,base_revision,trigger_key,state) VALUES($1::uuid,$2,2,$1::text,'queued')",[run,id]);
 return (await pool.query('SELECT * FROM planning_runs WHERE id=$1',[run])).rows[0];
}
it('records one immutable generation under concurrent replay; only new runs receive all three routing fields',async()=>{
 const f=await fixture(),old=(await pool.query('SELECT * FROM planning_runs WHERE id=$1',[f.old])).rows[0];
 const [a,b]=await Promise.all([f.register(),f.register()]);
 expect(a.generationId).toBe(b.generationId);expect([a.duplicate,b.duplicate].sort()).toEqual([false,true]);
 expect((await pool.query('SELECT request FROM objectives WHERE id=$1',[f.id])).rows[0].request).toEqual(f.request);
 expect((await pool.query('SELECT * FROM planning_runs WHERE id=$1',[f.old])).rows[0]).toEqual(old);
 const run=await newRun(f.id);expect(run.route_generation_id).toBe(a.generationId);
 expect(await routedPlanningRequest(pool,run)).toEqual({...f.request,plannerAccountId:accountId,plannerModel:target.model,allowedAccounts:[accountId]});
 const claimed=await claimPlanning();expect(claimed).toMatchObject({id:run.id,model:target.model,account:{id:accountId},route_generation_id:a.generationId});
 expect(claimed.request.allowedAccounts).toEqual([accountId]);
});
it('refuses a different operation for the already-current route and keeps receipts immutable',async()=>{
 const f=await fixture(),r=await f.register();
 await expect(registerObjectiveModelRoute(f.id,{...f.body,idempotencyKey:randomUUID(),expectedGenerationId:r.generationId},auth))
  .rejects.toMatchObject({code:'OBJECTIVE_MODEL_ROUTE_ALREADY_CURRENT'});
 await expect(pool.query("UPDATE objective_model_routes SET payload='{}' WHERE id=$1",[r.generationId])).rejects.toThrow('OBJECTIVE_MODEL_ROUTE_IMMUTABLE');
 await expect(pool.query('DELETE FROM objective_model_routes WHERE id=$1',[r.generationId])).rejects.toThrow('OBJECTIVE_MODEL_ROUTE_IMMUTABLE');
});
it('serializes competing operator requests with compare-and-set on the prior generation',async()=>{
 const f=await fixture(),results=await Promise.allSettled([f.register(),registerObjectiveModelRoute(f.id,{...f.body,idempotencyKey:randomUUID()},auth)]);
 expect(results.filter(r=>r.status==='fulfilled')).toHaveLength(1);
 expect(results.filter(r=>r.status==='rejected').map(r=>(r as PromiseRejectedResult).reason.code)).toEqual(['OBJECTIVE_MODEL_ROUTE_GENERATION_CHANGED']);
 expect((await pool.query('SELECT count(*)::int n FROM objective_model_routes')).rows[0].n).toBe(1);
});
it('never binds an old retry, including null-to-current, and rejects a foreign generation at INSERT',async()=>{
 const f=await fixture(),r=await f.register();
 await expect(pool.query('UPDATE planning_runs SET route_generation_id=$2 WHERE id=$1',[f.old,r.generationId])).rejects.toThrow('PLANNING_ROUTE_GENERATION_IMMUTABLE');
 await pool.query("UPDATE planning_runs SET state='queued',next_attempt_at=now() WHERE id=$1",[f.old]);
 expect(await claimPlanning()).toBeNull();
 expect((await pool.query('SELECT route_generation_id,model,error FROM planning_runs WHERE id=$1',[f.old])).rows[0])
  .toEqual({route_generation_id:null,model:'glm-5.3-flash',error:'OBJECTIVE_MODEL_ROUTE_RUN_STALE'});
 await expect(pool.query("INSERT INTO planning_runs(id,objective_id,base_revision,trigger_key,route_generation_id) VALUES($1,$2,2,'forged',$3)",
  [randomUUID(),f.id,randomUUID()])).rejects.toThrow('PLANNING_ROUTE_GENERATION_STALE');
});
it.each(['paused','cancelled','completed','revision','request','policy','file-policy','generation','goal','workforce','planner','maintenance','uncertain','live','native','lease','disabled-account'] as const)
 ('refuses %s without granting a route',async kind=>{
  const f=await fixture();
  if(['paused','cancelled','completed'].includes(kind))await pool.query('UPDATE goals SET state=$2 WHERE id=$1',[f.id,kind]);
  if(kind==='revision')f.body.expectedRevision++;
  if(kind==='request')f.body.expectedRequestDigest='f'.repeat(64);
  if(kind==='policy')f.body.expectedPolicyDigest='f'.repeat(64);
  if(kind==='file-policy')policyState.current=null;
  if(kind==='generation')f.body.expectedGenerationId=randomUUID() as any;
  if(kind==='goal')f.body.expectedGoalId=randomUUID();
  if(kind==='workforce')await pool.query('INSERT INTO settings VALUES($1,$2)',['workforce:'+f.id,JSON.stringify({enabled:true})]);
  if(kind==='planner')await newRun(f.id);
  if(kind==='maintenance')await pool.query("INSERT INTO settings VALUES('maintenance','{\"draining\":true}')");
  if(kind==='uncertain')await pool.query('INSERT INTO settings VALUES($1,$2)',['planning-source-preparation:'+randomUUID(),JSON.stringify({goalId:f.id,state:'uncertain'})]);
  if(kind==='disabled-account')await pool.query('UPDATE accounts SET enabled=false WHERE id=$1',[accountId]);
  if(['live','native','lease'].includes(kind)){
   const task=randomUUID(),run=randomUUID();
   await pool.query("INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria,execution,attempt_count) VALUES($1,$2,'old-worker','Old worker','Fixture','engineering.backend.node','failed','[]','{\"kind\":\"harness\",\"workspace\":\"test/base\"}',1)",[task,f.id]);
   await pool.query("INSERT INTO runs(id,task_id,attempt,state,fence,runtime,agent_version) VALUES($1,$2,1,$3,$4,'harness',1)",[run,task,kind==='live'?'running':'failed',randomUUID()]);
   if(kind==='native')await pool.query("INSERT INTO opencode_jobs(run_id,state) VALUES($1,'running')",[run]);
   if(kind==='lease')await pool.query("INSERT INTO leases(resource,run_id) VALUES('workspace:test/base',$1)",[run]);
  }
  await expect(f.register()).rejects.toThrow();
  expect((await pool.query('SELECT count(*)::int n FROM objective_model_routes')).rows[0].n).toBe(0);
 });
it('reconciles a lost reply after pause as a historical receipt with no renewed authority',async()=>{
 const f=await fixture(),r=await f.register();await pool.query("UPDATE goals SET state='paused' WHERE id=$1",[f.id]);
 expect(await f.register()).toMatchObject({generationId:r.generationId,duplicate:true,current:false,activation:'none',authority:'historical_receipt'});
 await expect(registerObjectiveModelRoute(f.id,{...f.body,idempotencyKey:randomUUID()},auth)).rejects.toMatchObject({code:'OBJECTIVE_MODEL_ROUTE_STATE'});
 await expect(registerObjectiveModelRoute(f.id,{...f.body,reason:'Different operator message.'},auth)).rejects.toMatchObject({code:'IDEMPOTENCY_CONFLICT'});
 expect((await objectiveModelRoute(f.id)).currentReceipt).toMatchObject({generationId:r.generationId,receiptDigest:r.receiptDigest,idempotencyKey:f.body.idempotencyKey});
 await pool.query('UPDATE objectives SET revision=3 WHERE id=$1',[f.id]);policyState.current=null;
 expect(await f.register()).toMatchObject({generationId:r.generationId,current:false,activation:'none'});
});
it('rejects post-registration policy/request drift at the execution boundary',async()=>{
 const f=await fixture();await f.register();const run=await newRun(f.id);
 await pool.query("UPDATE objectives SET request=jsonb_set(request,'{message}','\"Changed scope\"') WHERE id=$1",[f.id]);
 await expect(routedPlanningRequest(pool,run)).rejects.toMatchObject({code:'OBJECTIVE_MODEL_ROUTE_TARGET_CHANGED'});
 await pool.query('UPDATE objectives SET request=$2 WHERE id=$1',[f.id,JSON.stringify(f.request)]);
 policyState.current=null;await expect(routedPlanningRequest(pool,run)).rejects.toMatchObject({code:'OBJECTIVE_MODEL_ROUTE_POLICY_CHANGED'});
});
it('requires operator authentication and does not grant operations-only credentials the mutation',async()=>{
 const f=await fixture(),app=makeApp(),url='/api/objectives/'+f.id+'/model-route',serviceToken='fixture-operations-only-token';
 process.env.AGENTOS_OPERATIONS_ACCESS_JSON=JSON.stringify([{id:'fixture-service',tokenSha256:operationHash(serviceToken),goalIds:[f.id],projects:[],scopes:['observe']}]);
 try{
  expect((await app.inject({method:'POST',url,payload:f.body})).statusCode).toBe(401);
  expect((await app.inject({method:'POST',url,payload:f.body,headers:{authorization:'Bearer '+serviceToken}})).statusCode).toBe(401);
  const result=await app.inject({method:'POST',url,payload:f.body,headers:{authorization:'Bearer '+config.apiToken}});
  expect(result.statusCode).toBe(200);expect(result.json()).toMatchObject({recorded:true,oldAttemptsChanged:false});
  expect((await app.inject({method:'POST',url,payload:{...f.body,plannerAccountId:'arbitrary'},headers:{authorization:'Bearer '+config.apiToken}})).statusCode).toBe(400);
 }finally{await app.close();}
});
