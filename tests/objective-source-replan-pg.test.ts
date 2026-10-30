import {afterAll,afterEach,beforeAll,beforeEach,expect,it,vi} from 'vitest';
import {createHash,randomUUID} from 'node:crypto';
import {migrate,pool} from '../server/db.js';
import {config} from '../server/config.js';
import {seedRegistry} from '../server/registry.js';
import {accountBridge} from '../server/bridge.js';
import * as service from '../server/service.js';
import * as publication from '../server/published-rebase.js';
import {objectiveSchema,effectivePlanningRequest} from '../server/objective-contracts.js';
import {compileDelivery} from '../server/incremental-planning.js';
import {reconcilePlanning,claimPlanning,applyPlannedResult,feedbackObjective} from '../server/planner.js';
import {registerSourceReplanFeedback,assertSourceReplanCurrent,inspectSourceReplanCurrent} from '../server/objective-source-replan.js';
import {preparePlanningSources} from '../server/planning-source-preparation.js';
import {preparationHash as hash} from '../server/source-preparation-contract.js';
import {planningPreparationFixture} from './fixtures/planning-preparation.js';
import * as modelPolicy from '../bridge/model-policy.mjs';
import {objectiveModelRoute,registerObjectiveModelRoute} from '../server/objective-model-route.js';

const sha=(v:string|Buffer)=>createHash('sha256').update(v).digest('hex');
const auth={channel:'api-token' as const},base='agentos/src'+'1'.repeat(16);
const decision={kind:'delivery',requirementId:'req-1',title:'Preserve accepted contributions',
 action:'Port every canonical accepted contribution onto the pinned published root and resolve all conflicts explicitly.',
 verification:'Verify the original complete requirement on the newly composed candidate.',agentId:'engineering.backend.node',
 sourceContract:{version:1,required:[],preserve:[],noSourceReason:'Synthetic controller fixture verifies immutable source and input authority without claiming feature delivery.'}};
beforeAll(async()=>{expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');await migrate();});
beforeEach(async()=>{
 await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');await seedRegistry();
 await pool.query("DELETE FROM settings WHERE id LIKE 'planning-source-preparation:%' OR id LIKE 'candidate-validation:%' OR id LIKE 'workforce:%'");
 await pool.query(`UPDATE accounts SET provider='codex',model='test-model',models='["test-model"]',bridge_id='test-account',max_concurrency=4,allow_unknown=true`);
 config.bridgeHost='fixture';config.remoteExecution=true;
});
afterEach(async()=>{vi.restoreAllMocks();config.bridgeHost='';config.remoteExecution=false;
 await pool.query("DELETE FROM settings WHERE id='model-policy'");});
afterAll(async()=>{await pool.query("DELETE FROM settings WHERE id='model-policy'");await pool.end();});

async function fixture(requestOverrides={}){
 const goalId=randomUUID(),request=objectiveSchema.parse({message:'Preserve three accepted source chains across the published baseline change.',
  project:'agentos',workspaces:[base],mode:'execute',planningStrategy:'incremental',sourceContractVersion:1,
  requirements:['Preserve every accepted contribution and independently verify it.'],idempotencyKey:goalId,...requestOverrides});
 const previous=compileDelivery(decision,request),priorPlanning=randomUUID();
 await pool.query("INSERT INTO goals(id,title,description,project,state,idempotency_key) VALUES($1::uuid,'Replan','Fixture','agentos','failed',$1::text)",[goalId]);
 await pool.query("INSERT INTO objectives(id,request,request_hash,mode,status,revision) VALUES($1,$2,$3,'execute','executing',2)",[goalId,JSON.stringify(request),hash(request)]);
 await pool.query("INSERT INTO planning_runs(id,objective_id,base_revision,trigger_key,state,fence,feedback,output) VALUES($1,$2,1,'prior','completed',$3,'{}',$4)",[priorPlanning,goalId,randomUUID(),JSON.stringify(previous)]);
 await pool.query("INSERT INTO plan_revisions(id,objective_id,revision,planning_run_id,plan,status) VALUES($1,$2,2,$3,$4,'applied')",[randomUUID(),goalId,priorPlanning,JSON.stringify(previous)]);
 const refs=new Map<string,any>(),root='agentos/old-prepared-root',producers:any[]=[];
 const inventory=(text:string)=>[{path:'src/file.ts',sha256:sha(text)}];
 const inventoryHash=(files:any[])=>sha(JSON.stringify(files.map(f=>[f.path,f.sha256])));
 const add=async(parent:any=null,revision=2)=>{
  const taskId=randomUUID(),runId=randomUUID(),fence=randomUUID(),contextId=randomUUID(),acceptanceId=randomUUID();
  const workspace='agentos/w-'+taskId.replaceAll('-',''),sourceWorkspace=parent?.workspace??root;
  const execution={kind:'harness',workspace,sourceWorkspace,baseWorkspace:base,readOnlySource:false};
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,attempt_count,plan_revision,covers)
   VALUES($1::uuid,$2,$1::text,'Accepted input','Fixture','engineering.backend.node','accepted','work','[]',$3,1,$4,'["req-1"]')`,[taskId,goalId,JSON.stringify(execution),revision]);
  if(parent)await pool.query('INSERT INTO dependencies(task_id,depends_on) VALUES($1,$2)',[taskId,parent.taskId]);
  await pool.query("INSERT INTO runs(id,task_id,attempt,state,fence,runtime,agent_version) VALUES($1,$2,1,'submitted',$3,'harness',1)",[runId,taskId,fence]);
  await pool.query("INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,$3,'fixture')",[contextId,runId,JSON.stringify({
   runIdentity:{goalId,taskId,runId,runFence:fence},dependencyArtifacts:(parent?.artifacts??[]).map((a:any)=>({id:a.id,name:a.name,digest:a.digest,task_id:parent.taskId}))})]);
  await pool.query('UPDATE runs SET context_id=$2 WHERE id=$1',[runId,contextId]);
  const files=inventory(taskId),candidate={baseDigest:parent?.candidate.candidateDigest??inventoryHash(inventory('original')),
   candidateDigest:inventoryHash(files),files};
  const artifacts=[];
  for(const item of [{name:'candidate.json',bytes:Buffer.from(JSON.stringify(candidate)),mime:'application/json'},
   {name:'changes.patch',bytes:Buffer.from('--- a/src/file.ts\n+++ b/src/file.ts\n@@ -1 +1 @@\n-before\n+'+taskId+'\n'),mime:'text/plain'}]){
   const a={id:randomUUID(),run_id:runId,name:item.name,digest:sha(item.bytes),size:item.bytes.length,mime:item.mime,bytes:item.bytes};
   await pool.query('INSERT INTO artifacts(id,run_id,name,mime,digest,size) VALUES($1,$2,$3,$4,$5,$6)',[a.id,runId,a.name,a.mime,a.digest,a.size]);
   refs.set(a.id,a);artifacts.push(a);
  }
  await pool.query("INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) VALUES($1,$2,$3,true,'[]','fixture')",[acceptanceId,taskId,runId]);
  const result={taskId,runId,fence,contextId,acceptanceId,workspace,sourceWorkspace,candidate,artifacts};
  if(revision===2)producers.push(result);return result;
 };
 for(let i=0;i<3;i++){await add(null,1);const parent=await add();await add(parent);}
 const cancelledTask=randomUUID(),cancelledRun=randomUUID();
 await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,attempt_count,plan_revision)
  VALUES($1,$2,'r2-integrate','Cancelled integration','Fixture','engineering.backend.node','cancelled','integrate','[]',$3,1,2)`,[cancelledTask,goalId,JSON.stringify({kind:'harness',workspace:'agentos/cancelled-integration',sourceWorkspace:root,baseWorkspace:base})]);
 await pool.query("INSERT INTO runs(id,task_id,attempt,state,fence,runtime,agent_version) VALUES($1,$2,1,'cancelled',$3,'harness',1)",[cancelledRun,cancelledTask,randomUUID()]);
 for(const p of producers)await pool.query('INSERT INTO dependencies(task_id,depends_on) VALUES($1,$2)',[cancelledTask,p.taskId]);
 const frozen=(await pool.query('SELECT instruction_snapshot FROM runs WHERE id=$1',[cancelledRun])).rows[0].instruction_snapshot;
 const releaseDigest=sha('published'),publishedWorkspace='agentos/src'+releaseDigest.slice(0,16),sourceManifestHashes={'source.txt':sha('fixture')};
 const baseline={version:1,release:'/opt/agentos/releases/managed-'+randomUUID(),releaseDigest,sourceWorkspace:publishedWorkspace,
  sourceManifestHashes,sourceManifestDigest:hash(sourceManifestHashes),generatedReleaseHashes:{},sealedReleaseExtraHashes:{},workspaceManifestDigest:hash(sourceManifestHashes),
  publicationProofRef:'/var/lib/agentos-build/fixture/publication.json',publicationProofDigest:sha('publication'),
  validationProofRef:'/var/lib/agentos-build/fixture/validation.json',validationProofDigest:sha('validation'),
  registeredReceiptRef:'/var/lib/agentos/releases/fixture.json',registeredReceiptDigest:sha('registered'),registeredActionDigest:sha('action')};
 await pool.query("INSERT INTO settings(id,value) VALUES('agentos-source-workspace',$1) ON CONFLICT(id) DO UPDATE SET value=excluded.value",[JSON.stringify({workspace:publishedWorkspace})]);
 const publicationSpy=vi.spyOn(publication,'inspectPublishedBaseline').mockResolvedValue({baseline});
 vi.spyOn(service,'readArtifact').mockImplementation(async id=>{const a=refs.get(id);if(!a)throw Error('UNKNOWN_ARTIFACT');return a;});
 const preparation=planningPreparationFixture(),calls:any[]=[];
 const bridgeSpy=vi.spyOn(accountBridge,'request').mockImplementation(async(method,params)=>{calls.push({method,params});
  const result=preparation(method,params);if(result===undefined)throw Error('UNEXPECTED_RPC_'+method);return result;});
 const body={idempotencyKey:randomUUID(),description:'Preserve all canonical accepted patches while moving to the verified published root.',
  sourceReplan:{version:1 as const,expectedRevision:2,baseWorkspace:base,
   published:{kind:'current_published_runtime' as const,workspace:publishedWorkspace,releaseDigest,registeredReceiptDigest:baseline.registeredReceiptDigest},
   contributions:producers.map(p=>({taskId:p.taskId,runId:p.runId,acceptanceId:p.acceptanceId,candidateArtifactId:p.artifacts[0].id,
    candidateArtifactDigest:p.artifacts[0].digest,patchArtifactId:p.artifacts[1].id,patchArtifactDigest:p.artifacts[1].digest}))}};
 const register=()=>registerSourceReplanFeedback(goalId,body,auth);
 const plan=async()=>{await reconcilePlanning();return (await pool.query("SELECT * FROM planning_runs WHERE objective_id=$1 AND state='queued'",[goalId])).rows[0];};
 return {goalId,request,previous,body,producers,refs,add,cancelledTask,cancelledRun,frozen,baseline,publishedWorkspace,publicationSpy,bridgeSpy,calls,register,plan};
}

it('records one immutable feedback under concurrent replay and retains the receipt after revision/pause/publication changes',async()=>{
 const f=await fixture();const [a,b]=await Promise.all([f.register(),f.register()]);
 expect(a.feedbackId).toBe(b.feedbackId);expect([a.duplicate,b.duplicate].sort()).toEqual([false,true]);
 expect((await pool.query('SELECT count(*)::int n FROM objective_feedback WHERE objective_id=$1',[f.goalId])).rows[0].n).toBe(1);
 await pool.query("UPDATE goals SET state='paused' WHERE id=$1",[f.goalId]);await pool.query('UPDATE objectives SET revision=3 WHERE id=$1',[f.goalId]);
 f.publicationSpy.mockRejectedValue(Error('not consulted on replay'));
 expect(await f.register()).toMatchObject({feedbackId:a.feedbackId,receiptDigest:a.receiptDigest,duplicate:true});
 await expect(registerSourceReplanFeedback(f.goalId,{...f.body,description:'Different request content.'},auth)).rejects.toMatchObject({code:'IDEMPOTENCY_CONFLICT'});
 await expect(pool.query("UPDATE objective_feedback SET description='replacement' WHERE id=$1",[a.feedbackId])).rejects.toThrow('SOURCE_REPLAN_RECEIPT_IMMUTABLE');
});

it('pins exactly six current producers in three chains, creates a new plan and UUID preparations, and preserves every old attempt',async()=>{
 const f=await fixture(),receipt=await f.register();await f.plan();await reconcilePlanning();
 const run=await claimPlanning();expect(run.feedback.sourceReplan.feedbackId).toBe(receipt.feedbackId);
 expect(run.feedback.sourceOverrides).toEqual({[base]:f.publishedWorkspace});
 expect(run.feedback.sourceReplanInputs.contributors).toHaveLength(6);
 expect(run.feedback.sourceReplanInputs.contributors.filter((c:any)=>c.dependencyTaskIds.length===1)).toHaveLength(3);
 expect(run.feedback.artifacts.filter((a:any)=>a.name==='changes.patch')).toHaveLength(6);
 const plan=compileDelivery(decision,effectivePlanningRequest(f.request,run.feedback),f.previous,false,run.feedback.reports);
 run.output=plan;await pool.query('UPDATE planning_runs SET output=$2 WHERE id=$1',[run.id,JSON.stringify(plan)]);
 await expect(applyPlannedResult(run,plan)).rejects.toMatchObject({code:'SOURCE_REPLAN_PREPARATION_REQUIRED'});
 const prepared=await preparePlanningSources(run,plan,new AbortController().signal);
 const source=prepared.get(base)!;
 expect(source.intent.kind).toBe('explicit_source');expect(source.intent.request.sourceWorkspace).toBe(f.publishedWorkspace);
 expect(source.intent.request.workspace).toMatch(/-p[0-9a-f]{32}$/);
 await applyPlannedResult(run,plan,prepared);
 const tasks=(await pool.query('SELECT id,execution FROM tasks WHERE goal_id=$1 AND plan_revision=3',[f.goalId])).rows;
 expect(tasks.length).toBeGreaterThan(0);expect(tasks.every(t=>t.id!==f.cancelledTask&&!f.producers.some(p=>p.taskId===t.id))).toBe(true);
 expect(tasks.every(t=>t.execution.revisionFeedback.sourceReplan.receiptDigest===receipt.receiptDigest)).toBe(true);
 const old=(await pool.query('SELECT r.state,r.instruction_snapshot,t.state task_state FROM runs r JOIN tasks t ON t.id=r.task_id WHERE r.id=$1',[f.cancelledRun])).rows[0];
 expect(old).toEqual({state:'cancelled',task_state:'cancelled',instruction_snapshot:f.frozen});
 expect((await pool.query("SELECT count(*)::int n FROM tasks WHERE goal_id=$1 AND state='accepted'",[f.goalId])).rows[0].n).toBe(9);
 expect((await pool.query('SELECT count(*)::int n FROM acceptances')).rows[0].n).toBe(9);
 await reconcilePlanning();expect((await pool.query('SELECT count(*)::int n FROM planning_runs WHERE objective_id=$1',[f.goalId])).rows[0].n).toBe(2);
});

it('uses an explicitly migrated planning generation for source replan while preserving all six accepted inputs and original history',async()=>{
 const f=await fixture({plannerAccountId:'legacy',plannerModel:'glm-5.3-flash',allowedAccounts:['legacy']});
 const id='agentos-api-00000000-0000-4000-8000-000000000042';
 const policy={version:2 as const,mode:'exclusive-model' as const,accountId:id,bridgeId:'agentos:api:00000000-0000-4000-8000-000000000042',
  model:'deepseek-v4-pro-0813' as const,requestLimit:6 as const,sessionLimit:12 as const};
 const policySpy=vi.spyOn(modelPolicy,'readModelPolicy').mockReturnValue(policy);
 await pool.query("INSERT INTO settings(id,value) VALUES('model-policy',$1) ON CONFLICT(id) DO UPDATE SET value=EXCLUDED.value",[JSON.stringify(policy)]);
 await pool.query(`INSERT INTO accounts(id,label,provider,model,models,enabled,bridge_id,connection_status,max_concurrency,allow_unknown)
  VALUES($1,'Route fixture','qwen',$2,$3,true,$4,'connected',12,true)`,[id,policy.model,JSON.stringify([policy.model]),policy.bridgeId]);
 const state=await objectiveModelRoute(f.goalId);
 const {expectedGoalId,expectedRevision,expectedRequestDigest,expectedPolicyDigest,expectedGenerationId}=state;
 const route=await registerObjectiveModelRoute(f.goalId,{idempotencyKey:randomUUID(),expectedGoalId,expectedRevision,
  expectedRequestDigest,expectedPolicyDigest,expectedGenerationId,reason:'Operator authorizes the new model route while preserving all source contributions.'},auth);
 const receipt=await f.register(),queued=await f.plan();
 expect(queued.route_generation_id).toBe(route.generationId);
 const saved=(await pool.query('SELECT source_replan FROM objective_feedback WHERE id=$1',[receipt.feedbackId])).rows[0].source_replan;
 expect(saved.payload.routeGeneration).toEqual({id:route.generationId,receiptDigest:route.receiptDigest});
 expect(saved.payload.inputs.requestDigest).toBe(expectedRequestDigest);
 const run=await claimPlanning();expect(run.model).toBe(policy.model);expect(run.request.allowedAccounts).toEqual([id]);
 expect(run.feedback.sourceReplanInputs.contributors).toHaveLength(6);
 const plan=compileDelivery(decision,run.request,f.previous,false,run.feedback.reports);
 run.output=plan;await pool.query('UPDATE planning_runs SET output=$2 WHERE id=$1',[run.id,JSON.stringify(plan)]);
 policySpy.mockReturnValue(null);
 await expect(preparePlanningSources(run,plan,new AbortController().signal)).rejects.toMatchObject({code:'OBJECTIVE_MODEL_ROUTE_POLICY_CHANGED'});
 expect(f.calls.filter(c=>c.method==='worker.prepare')).toHaveLength(0);
 policySpy.mockReturnValue(policy);
 const prepared=await preparePlanningSources(run,plan,new AbortController().signal);
 policySpy.mockReturnValue(null);await expect(applyPlannedResult(run,plan,prepared)).rejects.toMatchObject({code:'OBJECTIVE_MODEL_ROUTE_POLICY_CHANGED'});
 policySpy.mockReturnValue(policy);await applyPlannedResult(run,plan,prepared);
 const tasks=(await pool.query('SELECT allowed_accounts FROM tasks WHERE goal_id=$1 AND plan_revision=3',[f.goalId])).rows;
 expect(tasks.length).toBeGreaterThan(0);expect(tasks.every(t=>hash(t.allowed_accounts)===hash([id]))).toBe(true);
 expect((await pool.query('SELECT request FROM objectives WHERE id=$1',[f.goalId])).rows[0].request).toEqual(f.request);
 expect((await pool.query('SELECT instruction_snapshot FROM runs WHERE id=$1',[f.cancelledRun])).rows[0].instruction_snapshot).toEqual(f.frozen);
 expect((await pool.query('SELECT count(*)::int n FROM acceptances')).rows[0].n).toBe(9);
 await pool.query("DELETE FROM settings WHERE id='model-policy'");
});

it.each(['paused','cancelled','wrong-revision','missing-producer','historical-producer','acceptance','literal-lease','planner-owner','publication'] as const)
 ('refuses %s before recording source authority',async kind=>{
  const f=await fixture(),p=f.producers[0];
  if(kind==='paused'||kind==='cancelled')await pool.query('UPDATE goals SET state=$2 WHERE id=$1',[f.goalId,kind]);
  if(kind==='wrong-revision')f.body.sourceReplan.expectedRevision=1;
  if(kind==='missing-producer')f.body.sourceReplan.contributions.pop();
  if(kind==='historical-producer')await f.add(null,1).then(p=>f.body.sourceReplan.contributions.push({taskId:p.taskId,runId:p.runId,acceptanceId:p.acceptanceId,candidateArtifactId:p.artifacts[0].id,candidateArtifactDigest:p.artifacts[0].digest,patchArtifactId:p.artifacts[1].id,patchArtifactDigest:p.artifacts[1].digest}));
  if(kind==='acceptance')await pool.query("INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer,created_at) VALUES($1,$2,$3,false,'[]','fixture',now()+interval '1 second')",[randomUUID(),p.taskId,p.runId]);
  if(kind==='literal-lease')await pool.query('INSERT INTO leases(resource,run_id) VALUES($1,$2)',['workspace:'+p.workspace,f.cancelledRun]);
  if(kind==='planner-owner')await pool.query("INSERT INTO planning_runs(id,objective_id,base_revision,trigger_key,fence,state) VALUES($1,$2,2,'competing',$3,'queued')",[randomUUID(),f.goalId,randomUUID()]);
  if(kind==='publication')f.body.sourceReplan.published.registeredReceiptDigest=sha('different');
  await expect(f.register()).rejects.toThrow();
  expect((await pool.query('SELECT count(*)::int n FROM objective_feedback WHERE objective_id=$1',[f.goalId])).rows[0].n).toBe(0);
 });

it.each(['acceptance','new-contribution','source-setting','paused','lease','artifact-bytes','source-generation'] as const)
 ('fails closed on changed %s after registration without adopting a new root or requeuing the receipt',async kind=>{
  const f=await fixture();await f.register();const run=await f.plan(),p=f.producers[0];
  if(kind==='acceptance')await pool.query("INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer,created_at) VALUES($1,$2,$3,true,'[]','fixture',now()+interval '1 second')",[randomUUID(),p.taskId,p.runId]);
  if(kind==='new-contribution')await f.add();
  if(kind==='source-setting')await pool.query("UPDATE settings SET value=$1 WHERE id='agentos-source-workspace'",[JSON.stringify({workspace:'agentos/src'+'f'.repeat(16)})]);
  if(kind==='paused')await pool.query("UPDATE goals SET state='paused' WHERE id=$1",[f.goalId]);
  if(kind==='lease')await pool.query('INSERT INTO leases(resource,run_id) VALUES($1,$2)',['workspace:'+p.workspace,f.cancelledRun]);
  if(kind==='artifact-bytes')f.refs.get(p.artifacts[1].id).bytes=Buffer.from('changed bytes');
  if(kind==='source-generation'){
   const original=f.bridgeSpy.getMockImplementation()!;
   f.bridgeSpy.mockImplementation(async(method,params,options)=>{const value=await original(method,params,options);
    if(method==='worker.inspectPlanningSource'){value.directoryIdentity.inode='99';value.generationDigest=hash({version:1,kind:'unmarked',workspace:value.workspace,sourceDigest:value.sourceDigest,...value.directoryIdentity});}return value;});
  }
  await expect(inspectSourceReplanCurrent(run)).rejects.toThrow();
  expect(f.calls.filter(x=>x.method==='worker.prepare')).toHaveLength(0);
  if(kind!=='paused'&&kind!=='artifact-bytes'&&kind!=='source-generation'){
   expect(await claimPlanning()).toBeNull();await reconcilePlanning();
   expect((await pool.query('SELECT state FROM planning_runs WHERE id=$1',[run.id])).rows[0].state).toBe('failed');
   expect((await pool.query('SELECT count(*)::int n FROM planning_runs WHERE objective_id=$1',[f.goalId])).rows[0].n).toBe(2);
  }
 });

it('does not upgrade a textual feedback key into source authority or replay an old consumed receipt into a later run',async()=>{
 const f=await fixture();await feedbackObjective(f.goalId,f.body.idempotencyKey,f.body.description);
 await expect(f.register()).rejects.toMatchObject({code:'IDEMPOTENCY_CONFLICT'});
 f.body.idempotencyKey=randomUUID();await f.register();const first=await f.plan();
 await pool.query("UPDATE planning_runs SET state='failed',error='terminal fixture' WHERE id=$1",[first.id]);
 await feedbackObjective(f.goalId,randomUUID(),'A new textual observation does not renew source authorization.');
 const second=await f.plan();expect(second.id).not.toBe(first.id);expect(second.feedback.sourceReplan).toBeUndefined();
 const forged={...second,feedback:{...first.feedback}};
 await expect(assertSourceReplanCurrent(pool,forged)).rejects.toMatchObject({code:'SOURCE_REPLAN_RECEIPT_INVALID'});
});

it('serializes different authenticated requests so only one pending source receipt owns the boundary',async()=>{
 const f=await fixture(),other={...f.body,idempotencyKey:randomUUID()};
 const outcomes=await Promise.allSettled([f.register(),registerSourceReplanFeedback(f.goalId,other,auth)]);
 expect(outcomes.filter(x=>x.status==='fulfilled')).toHaveLength(1);
 expect(outcomes.find(x=>x.status==='rejected')).toMatchObject({reason:{code:'SOURCE_REPLAN_OWNER_BUSY'}});
 expect((await pool.query("SELECT count(*)::int n FROM events WHERE entity_id=$1 AND kind='OBJECTIVE_SOURCE_REPLAN_REQUESTED'",[f.goalId])).rows[0].n).toBe(1);
});
