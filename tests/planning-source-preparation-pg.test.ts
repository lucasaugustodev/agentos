import {beforeAll,beforeEach,afterEach,afterAll,it,expect,vi} from 'vitest';
import {createHash,randomUUID} from 'node:crypto';
import {migrate,pool} from '../server/db.js';
import {config} from '../server/config.js';
import {seedRegistry} from '../server/registry.js';
import {accountBridge} from '../server/bridge.js';
import * as service from '../server/service.js';
import {objectiveSchema,planSchema} from '../server/objective-contracts.js';
import {applyPlannedResult} from '../server/planner.js';
import {preparePlanningSources} from '../server/planning-source-preparation.js';
import {preparationHash} from '../server/source-preparation-contract.js';

const sha=(v:string|Buffer)=>createHash('sha256').update(v).digest('hex');
const canonical=(v:any):any=>Array.isArray(v)?v.map(canonical):v&&typeof v==='object'
 ?Object.fromEntries(Object.keys(v).sort().map(k=>[k,canonical(v[k])])):v;
const files=(v:string)=>[{path:'src/code.ts',sha256:sha(v)}];
const inventoryDigest=(f:ReturnType<typeof files>)=>sha(JSON.stringify(f.map(x=>[x.path,x.sha256])));
const signal=()=>new AbortController().signal;
beforeAll(async()=>{expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');await migrate();});
beforeEach(async()=>{
 await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');
 await pool.query("DELETE FROM settings WHERE id LIKE 'planning-source-preparation:%' OR id LIKE 'candidate-validation:%'");
 await seedRegistry();
});
afterEach(()=>{vi.restoreAllMocks();vi.useRealTimers();});
afterAll(()=>pool.end());

function preparedResponse(request:any,sourceFiles:ReturnType<typeof files>){
 const binding={version:1,preparationIdentity:request.preparationIdentity,sourceWorkspace:request.sourceWorkspace,
  targetWorkspace:request.workspace,expectedSourceDigest:request.expectedSourceDigest,expectedSourceSnapshotDigest:request.expectedSourceSnapshotDigest,
  ...(request.sourceGenerationKind?{sourceGenerationKind:request.sourceGenerationKind}:{})};
 const body={...binding,bindingDigest:preparationHash(binding),observedSourceDigest:request.expectedSourceDigest,
  sourceSnapshotDigest:request.expectedSourceSnapshotDigest,cloneBaseDigest:request.expectedSourceDigest,
  baselineDigest:request.expectedSourceDigest,createdAt:'2026-10-30T00:00:00Z'};
 const receipt={...body,receiptDigest:preparationHash(body)};
 const marker={workspace:request.workspace,sourceWorkspace:request.sourceWorkspace,baseDigest:request.expectedSourceDigest,
  sourceSnapshotDigest:request.expectedSourceSnapshotDigest,createdAt:receipt.createdAt,
  preparation:{version:1,preparationIdentity:request.preparationIdentity,bindingDigest:receipt.bindingDigest,receiptDigest:receipt.receiptDigest}};
 const bytes=Buffer.from(JSON.stringify(canonical(marker)));
 return {version:1,exists:true,workspace:request.workspace,preparationReceipt:receipt,markerBase64:bytes.toString('base64'),
  generation:{workspace:request.workspace,sourceWorkspace:request.sourceWorkspace,baseDigest:request.expectedSourceDigest,
   snapshotDigest:sha(bytes),sourceSnapshotDigest:request.expectedSourceSnapshotDigest},
  candidate:{baseDigest:request.expectedSourceDigest,candidateDigest:request.expectedSourceDigest,files:sourceFiles},destinationChanged:false,checkedAt:new Date().toISOString()};
}

async function fixture(withAccepted=true){
 const goalId=randomUUID(),revision=withAccepted?2:0,base='example/logical',taskId=randomUUID(),runId=randomUUID(),fence=randomUUID(),contextId=randomUUID();
 const request=objectiveSchema.parse({message:'Continue the accepted feature and independently verify the result.',project:'example',
  workspaces:[base],mode:'execute',idempotencyKey:goalId,requirements:['The feature must pass independent checks.']});
 const criterion={id:'passed',label:'Independent check',type:'json_field',name:'result.json',field:'passed',equals:true};
 const plan=planSchema.parse({title:'Continue accepted feature',summary:'Reuse the exact accepted source and retain independent verification.',
  approachChange:'Reuse the accepted candidate instead of reconstructing its patch from scratch.',
  requirements:[{id:'working',description:request.requirements[0]}],tasks:[
   {key:'deliver',title:'Finish feature',description:'Implement only the remaining feature work.',agentId:'engineering.backend.node',stage:'work',
    baseWorkspace:base,covers:['working'],criteria:[criterion]},
   {key:'verify',title:'Verify feature',description:'Verify the exact produced candidate independently.',agentId:'qa.integration',stage:'verify',
    baseWorkspace:base,covers:['working'],sourceTask:'deliver',dependencies:['deliver'],criteria:[criterion]}]});
 await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1::uuid,'Reuse','Fixture','example',$1::text)",[goalId]);
 await pool.query("INSERT INTO objectives(id,request,request_hash,mode,status,revision) VALUES($1,$2,$3,'execute','replanning',$4)",[goalId,JSON.stringify(request),sha('request'),revision]);
 const freshPlanning=async()=>{
  const id=randomUUID(),planningFence=randomUUID();
  await pool.query("INSERT INTO planning_runs(id,objective_id,base_revision,trigger_key,state,fence,output,feedback) VALUES($1::uuid,$2,$3,$1::text,'running',$4,$5,'{}')",
   [id,goalId,revision,planningFence,JSON.stringify(plan)]);
  return (await pool.query('SELECT * FROM planning_runs WHERE id=$1',[id])).rows[0];
 };
 const planning=await freshPlanning();
 const baseFiles=files('base'),baseDigest=inventoryDigest(baseFiles),root='example/prepared-root',workspace='example/accepted-source';
 const baseMarker=Buffer.from(JSON.stringify({workspace:root,sourceWorkspace:base,baseDigest,sourceSnapshotDigest:sha('original-generation'),createdAt:'2026-10-30T00:00:00Z'}));
 const baseInspection={version:1,workspace:root,generation:{workspace:root,sourceWorkspace:base,baseDigest,snapshotDigest:sha(baseMarker),sourceSnapshotDigest:sha('original-generation')},
  markerBase64:baseMarker.toString('base64'),legacyParentGeneration:false,candidate:{baseDigest,candidateDigest:baseDigest,files:baseFiles},checkedAt:''};
 const candidate={baseDigest,candidateDigest:inventoryDigest(files('accepted')),files:files('accepted')};
 const marker=Buffer.from(JSON.stringify({workspace,sourceWorkspace:root,baseDigest,sourceSnapshotDigest:sha(baseMarker),createdAt:'2026-10-30T00:00:00Z'}));
 const generation={workspace,sourceWorkspace:root,baseDigest,snapshotDigest:sha(marker),sourceSnapshotDigest:sha(baseMarker)};
 const refs:any[]=[],artifacts=new Map<string,any>();
 if(withAccepted){
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,attempt_count,plan_revision)
   VALUES($1,$2,'accepted','Accepted source','Fixture','qa.backend','accepted','work','[]',$3,1,2)`,[taskId,goalId,
   JSON.stringify({kind:'harness',workspace,sourceWorkspace:root,baseWorkspace:base})]);
  await pool.query("INSERT INTO runs(id,task_id,attempt,state,fence,runtime,agent_version) VALUES($1,$2,1,'submitted',$3,'harness',1)",[runId,taskId,fence]);
  await pool.query('INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,$3,$4)',[contextId,runId,
   JSON.stringify({runIdentity:{goalId,taskId,runId,runFence:fence},dependencyArtifacts:[]}),'fixture context']);
  await pool.query('UPDATE runs SET context_id=$2 WHERE id=$1',[runId,contextId]);
  for(const item of [{name:'candidate.json',bytes:Buffer.from(JSON.stringify(candidate)),mime:'application/json'},
   {name:'changes.patch',bytes:Buffer.from('--- a/src/code.ts\n+++ b/src/code.ts\n@@ -1 +1 @@\n-base\n+accepted\n'),mime:'text/plain'}]){
   const a={id:randomUUID(),run_id:runId,name:item.name,digest:sha(item.bytes),size:item.bytes.length,mime:item.mime,bytes:item.bytes};
   refs.push(a);artifacts.set(a.id,a);
   await pool.query('INSERT INTO artifacts(id,run_id,name,mime,digest,size) VALUES($1,$2,$3,$4,$5,$6)',[a.id,runId,a.name,a.mime,a.digest,a.size]);
  }
  await pool.query("INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) VALUES($1,$2,$3,true,'[]','fixture')",[randomUUID(),taskId,runId]);
 }
 const runtimeInspection={version:1,runId,workspace,state:{runId,workspace,runtime:'harness',state:'completed',runFence:fence,stopped:true,
  artifacts:refs.map(a=>({name:a.name,digest:a.digest,mime:a.mime}))},admission:{authority:'runtime_operation_policy',goalId,taskId,runId,runFence:fence,
  contextVersion:contextId,workspace:'/workspace',sourceSnapshotDigest:sha(marker),policyDigest:sha('policy')},generation,
  markerBase64:marker.toString('base64'),legacyParentGeneration:false,candidate,process:{unit:'agentos-run-'+runId,checkedAt:'',stopped:true,
   loadState:'loaded',activeState:'inactive',subState:'dead',mainPid:0,cgroupExists:false,populated:false},checkedAt:''};
 vi.spyOn(service,'readArtifact').mockImplementation(async id=>{const a=artifacts.get(id);if(!a)throw Error('UNKNOWN_ARTIFACT');return a;});
 const preparations=new Map<string,any>(),calls:Array<{method:string;params:any}>=[];
 const sourceInspections=new Map<string,any>([[workspace,runtimeInspection]]);
 const behavior={loseReplyOnce:false,abortBeforeReserve:false,unconfirmed:false,beforeCopy:null as null|(()=>Promise<void>),
  beforeStop:null as null|(()=>void)};
 const spy=vi.spyOn(accountBridge,'request').mockImplementation(async(method,params:any)=>{
  calls.push({method,params:structuredClone(params)});
  if(method==='worker.inspectGeneration'){
   if(behavior.abortBeforeReserve)throw Object.assign(Error('cancelled'),{code:'ABORTED'});
   return {...baseInspection,checkedAt:new Date().toISOString()};
  }
  if(method==='worker.inspectPlanningSource'){
   const checkedAt=new Date().toISOString(),origin=sourceInspections.get(params.workspace);
   if(origin){
    const snapshot={version:1,workspace:params.workspace,generation:origin.generation,markerBase64:origin.markerBase64,
      legacyParentGeneration:false,candidate:origin.candidate,checkedAt};
    return {version:1,workspace:params.workspace,sourceGenerationKind:'snapshot',sourceDigest:origin.candidate.candidateDigest,
      generationDigest:origin.generation.snapshotDigest,snapshot,checkedAt};
   }
   const directoryIdentity={device:'1',inode:'2'};
   return {version:1,workspace:params.workspace,sourceGenerationKind:'unmarked',sourceDigest:baseDigest,
    generationDigest:preparationHash({version:1,kind:'unmarked',workspace:params.workspace,sourceDigest:baseDigest,...directoryIdentity}),
    directoryIdentity,files:baseFiles,checkedAt};
  }
  if(method==='worker.inspectSourceBinding'){
   const checkedAt=new Date().toISOString();return {...runtimeInspection,checkedAt,process:{...runtimeInspection.process,checkedAt}};
  }
  if(method==='worker.inspectRetrySourceBinding'){
   if(behavior.beforeStop){const action=behavior.beforeStop;behavior.beforeStop=null;action();}
   const origin=sourceInspections.get(params.workspace),checkedAt=new Date().toISOString();
   const {artifacts,...state}=origin.state;
   return {...origin,state,checkedAt,process:{...origin.process,checkedAt}};
  }
  if(method==='worker.inspectPreparation'){
   if(behavior.beforeCopy){const mutate=behavior.beforeCopy;behavior.beforeCopy=null;await mutate();}
   const prepared=preparations.get(params.preparationIdentity);
   return prepared?{...prepared,checkedAt:new Date().toISOString()}:{version:1,exists:false,confirmed:!behavior.unconfirmed,
    workspace:params.workspace,preparationIdentity:params.preparationIdentity,checkedAt:new Date().toISOString()};
  }
  if(method==='worker.prepare'){
   if(!params.preparationIdentity)return {workspace:params.workspace};
   preparations.set(params.preparationIdentity,preparedResponse(params,sourceInspections.get(params.sourceWorkspace)?.candidate.files??baseFiles));
   if(behavior.loseReplyOnce){behavior.loseReplyOnce=false;throw Object.assign(Error('lost reply'),{code:'PROVIDER_TIMEOUT'});}
   return {workspace:params.workspace};
  }
  throw Error('UNEXPECTED_RPC_'+method);
 });
 const saved=async()=>((await pool.query('SELECT value FROM settings WHERE id=$1',[`planning-source-preparation:${planning.id}:0`])).rows[0]?.value);
 const override=async(source:string,p=planning,extra:any={})=>{
  p.feedback={sourceOverrides:{[base]:source},...extra};
  await pool.query('UPDATE planning_runs SET feedback=$2 WHERE id=$1',[p.id,JSON.stringify(p.feedback)]);return p;
 };
 const addSource=async(name:string)=>{
  const task=randomUUID(),run=randomUUID(),runFence=randomUUID(),context=randomUUID();
  const sourceCandidate={baseDigest,candidateDigest:inventoryDigest(files(name)),files:files(name)};
  const raw=Buffer.from(JSON.stringify({workspace:name,sourceWorkspace:root,baseDigest,sourceSnapshotDigest:sha(baseMarker),createdAt:'2026-10-30T00:00:00Z'}));
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,attempt_count,plan_revision)
   VALUES($1,$2,$3,'Source','Fixture','qa.backend','accepted','work','[]',$4,1,$5)`,
   [task,goalId,task,JSON.stringify({kind:'harness',workspace:name,sourceWorkspace:root,baseWorkspace:base}),revision]);
  await pool.query("INSERT INTO runs(id,task_id,attempt,state,fence,runtime,agent_version) VALUES($1,$2,1,'submitted',$3,'harness',1)",[run,task,runFence]);
  await pool.query("INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,$3,'source')",[context,run,
   JSON.stringify({runIdentity:{goalId,taskId:task,runId:run,runFence},dependencyArtifacts:[]})]);
  await pool.query('UPDATE runs SET context_id=$2 WHERE id=$1',[run,context]);
  sourceInspections.set(name,{...runtimeInspection,runId:run,workspace:name,candidate:sourceCandidate,markerBase64:raw.toString('base64'),
   generation:{workspace:name,sourceWorkspace:root,baseDigest,snapshotDigest:sha(raw),sourceSnapshotDigest:sha(baseMarker)},
   state:{...runtimeInspection.state,runId:run,workspace:name,runFence},
   admission:{...runtimeInspection.admission,taskId:task,runId:run,runFence,contextVersion:context,sourceSnapshotDigest:sha(raw)},
   process:{...runtimeInspection.process,unit:'agentos-run-'+run}});
  return {taskId:task,runId:run,workspace:name};
 };
 return {goalId,planning,plan,taskId,runId,workspace,base,baseInspection,runtimeInspection,refs,preparations,calls,behavior,spy,saved,freshPlanning,override,addSource};
}

it('binds an initial unmarked root without inventing accepted reuse authority',async()=>{
 const f=await fixture(false),sources=await preparePlanningSources(f.planning,f.plan,signal());
 expect(sources.size).toBe(1);expect(f.calls.filter(c=>c.method==='worker.prepare')).toHaveLength(1);
 expect((await f.saved()).request).toMatchObject({sourceWorkspace:f.base,sourceGenerationKind:'unmarked'});
 expect((await f.saved()).proof.kind).toBe('explicit_planning_source');await applyPlannedResult(f.planning,f.plan,sources);
 const rows=(await pool.query('SELECT execution FROM tasks WHERE goal_id=$1',[f.goalId])).rows;
 expect(rows).toHaveLength(2);expect(rows.every(x=>!x.execution.sourceReuse&&x.execution.planningSource.acceptance===false)).toBe(true);
 expect(rows.filter(x=>x.execution.sourcePreparation)).toHaveLength(1);
});

it('retains the first prepared target after rejected apply and binds a later override to its own identity',async()=>{
 const f=await fixture();await f.override(f.workspace);
 const first=await preparePlanningSources(f.planning,f.plan,signal()),firstSource=first.get(f.base)!;
 const nextSource=await f.addSource('example/second-source');
 // A branch becomes busy after preparation, so application must not erase the clone.
 await f.override(f.workspace,f.planning,{replaceTaskIds:[nextSource.taskId]});
 await pool.query("UPDATE runs SET state='running' WHERE id=$1",[nextSource.runId]);
 await expect(applyPlannedResult(f.planning,f.plan,first)).rejects.toMatchObject({code:'SOURCE_PREPARATION_BRANCH_BUSY'});
 expect((await f.saved()).state).toBe('prepared');expect(f.preparations.has(firstSource.intent.request.preparationIdentity)).toBe(true);
 await pool.query("UPDATE runs SET state='submitted' WHERE id=$1",[nextSource.runId]);
 await pool.query("UPDATE planning_runs SET state='failed' WHERE id=$1",[f.planning.id]);
 const next=await f.freshPlanning();await f.override(nextSource.workspace,next);
 const second=await preparePlanningSources(next,f.plan,signal()),secondSource=second.get(f.base)!;
 expect(secondSource.intent.request.sourceWorkspace).toBe(nextSource.workspace);
 expect(secondSource.intent.request.workspace).not.toBe(firstSource.intent.request.workspace);
 expect(secondSource.intent.request.preparationIdentity).not.toBe(firstSource.intent.request.preparationIdentity);
 expect(f.preparations.size).toBe(2);
 await applyPlannedResult(next,f.plan,second);
 expect(f.preparations.has(firstSource.intent.request.preparationIdentity)).toBe(true);
});

it.each(['root','override'] as const)('reconciles a lost %s preparation reply without another copy',async mode=>{
 const f=await fixture(mode==='override');if(mode==='override')await f.override(f.workspace);
 f.behavior.loseReplyOnce=true;
 await expect(preparePlanningSources(f.planning,f.plan,signal())).rejects.toMatchObject({code:'SOURCE_PREPARATION_RECONCILE'});
 const saved=await f.saved();const result=await preparePlanningSources(f.planning,f.plan,signal());
 expect(result.get(f.base)!.intent.request.preparationIdentity).toBe(saved.request.preparationIdentity);
 expect(f.calls.filter(c=>c.method==='worker.prepare')).toHaveLength(1);
});

it.each(['source','replacement'] as const)('does not prepare while a %s run is queued',async owner=>{
 const f=await fixture(),other=await f.addSource('example/other');
 await f.override(f.workspace,f.planning,owner==='replacement'?{replaceTaskIds:[other.taskId]}:{});
 await pool.query("UPDATE runs SET state='queued' WHERE id=$1",[owner==='source'?f.runId:other.runId]);
 await expect(preparePlanningSources(f.planning,f.plan,signal())).rejects.toMatchObject({code:'SOURCE_PREPARATION_BRANCH_BUSY'});
 expect(f.calls).toHaveLength(0);expect(await f.saved()).toBeUndefined();
});

it('allows an unrelated queued run and still binds the explicit source',async()=>{
 const f=await fixture(),other=await f.addSource('example/unrelated');await f.override(f.workspace);
 await pool.query("UPDATE runs SET state='queued' WHERE id=$1",[other.runId]);
 const result=await preparePlanningSources(f.planning,f.plan,signal());
 expect(result.get(f.base)!.intent.request.sourceWorkspace).toBe(f.workspace);
});

it('does not renew an old source observation through a later stop probe',async()=>{
 const f=await fixture();await f.override(f.workspace);vi.useFakeTimers({toFake:['Date']});
 f.behavior.beforeStop=()=>vi.setSystemTime(Date.now()+61000);
 await expect(preparePlanningSources(f.planning,f.plan,signal())).rejects.toMatchObject({code:'SOURCE_PREPARATION_STALE'});
 expect(f.calls.some(c=>c.method==='worker.prepare')).toBe(false);expect(await f.saved()).toBeUndefined();
});

it('rejects missing ownership metadata instead of silently treating the task source as an unmarked root',async()=>{
 const f=await fixture();await f.override(f.workspace);
 await pool.query('DELETE FROM task_execution_metadata WHERE task_id=$1',[f.taskId]);
 await expect(preparePlanningSources(f.planning,f.plan,signal())).rejects.toMatchObject({code:'SOURCE_PREPARATION_AUTHORITY_CHANGED'});
 expect(f.calls).toHaveLength(0);expect(await f.saved()).toBeUndefined();
});

it('keeps large execution inputs out of explicit-source ownership scans and captures',async()=>{
 const f=await fixture();await f.override(f.workspace);
 const bytes=Buffer.alloc(1024*1024,97),payload=bytes.toString('base64');
 await pool.query("UPDATE tasks SET execution=execution||$2::jsonb WHERE id=$1",[f.taskId,
  JSON.stringify({inputs:[{id:randomUUID(),name:'large-context.txt',digest:sha(bytes),base64:payload}]})]);
 const queries=vi.spyOn(pool,'query');await preparePlanningSources(f.planning,f.plan,signal());
 const captures=queries.mock.calls.map((args,index)=>({sql:String(args[0]),result:queries.mock.results[index].value}))
  .filter(x=>x.sql.startsWith('SELECT g.project'));
 expect(captures.length).toBeGreaterThan(0);
 for(const captured of captures){
  expect(captured.sql).not.toContain("t.execution->>");
  const returned=JSON.stringify((await captured.result).rows);
  expect(returned.length).toBeLessThan(20000);expect(returned).not.toContain(payload.slice(0,100));
 }
});

it.each(['fence','feedback','sourceOverride','writer','validation'] as const)('recaptures %s before the preparation effect',async mutation=>{
 const f=await fixture();await f.override(f.workspace);
 f.behavior.beforeCopy=async()=>{
  if(mutation==='fence')await pool.query('UPDATE planning_runs SET fence=$2 WHERE id=$1',[f.planning.id,randomUUID()]);
  if(mutation==='sourceOverride')await pool.query("UPDATE planning_runs SET feedback=jsonb_set(feedback,'{sourceOverrides}', $2) WHERE id=$1",[f.planning.id,JSON.stringify({[f.base]:'example/other'})]);
  if(mutation==='feedback')await pool.query("INSERT INTO objective_feedback(id,objective_id,trigger_key,description) VALUES($1,$2,'new','New evidence')",[randomUUID(),f.goalId]);
  if(mutation==='writer')await pool.query("UPDATE runs SET state='running' WHERE id=$1",[f.runId]);
  if(mutation==='validation')await pool.query('INSERT INTO settings(id,value) VALUES($1,$2)',[
   'candidate-validation:'+randomUUID(),JSON.stringify({state:'uncertain',request:{workspace:f.workspace}})]);
 };
 await expect(preparePlanningSources(f.planning,f.plan,signal())).rejects.toBeDefined();
 expect(f.calls.some(c=>c.method==='worker.prepare')).toBe(false);
});

it('reserves a bound source, prepares its receipt and atomically installs a future consumer without accepting the objective',async()=>{
 const f=await fixture(),sources=await preparePlanningSources(f.planning,f.plan,signal()),source=sources.get(f.base)!;
 expect(sources.size).toBe(1);expect((await f.saved()).state).toBe('prepared');
 expect(source.intent.request.sourceWorkspace).toBe(f.workspace);
 expect(source.intent.request.workspace).toContain('-p'+source.intent.request.preparationIdentity.replaceAll('-',''));
 await applyPlannedResult(f.planning,f.plan,sources);
 const tasks=(await pool.query('SELECT stage,execution FROM tasks WHERE goal_id=$1 AND plan_revision=3 ORDER BY stage',[f.goalId])).rows;
 const worker=tasks.find(t=>t.stage==='work')!.execution,qa=tasks.find(t=>t.stage==='verify')!.execution;
 expect(worker.sourceWorkspace).toBe(source.intent.request.workspace);
 expect(worker.sourcePreparation).toMatchObject({workspace:worker.workspace,sourceWorkspace:worker.sourceWorkspace,
  expectedSourceDigest:source.prepared.currentCandidateDigest,expectedSourceSnapshotDigest:source.prepared.targetSnapshotDigest});
 expect(qa.sourceWorkspace).toBe(worker.workspace);expect(qa.sourcePreparation).toBeUndefined();
 expect(worker.sourceReuse).toMatchObject({acceptance:false,finalVerificationRequired:true,proofDigest:source.intent.proof.proofDigest});
 expect((await f.saved()).state).toBe('applied');
 expect((await pool.query('SELECT count(*)::int n FROM acceptances')).rows[0].n).toBe(1);
 expect((await pool.query('SELECT state FROM goals WHERE id=$1',[f.goalId])).rows[0].state).toBe('active');
});

it('reconciles a lost prepare reply with the same identity and no second copy or legacy fallback',async()=>{
 const f=await fixture();f.behavior.loseReplyOnce=true;
 await expect(preparePlanningSources(f.planning,f.plan,signal())).rejects.toMatchObject({code:'SOURCE_PREPARATION_RECONCILE'});
 const uncertain=await f.saved();expect(uncertain.state).toBe('uncertain');
 const sources=await preparePlanningSources(f.planning,f.plan,signal());
 expect(sources.get(f.base)!.intent.request.preparationIdentity).toBe(uncertain.request.preparationIdentity);
 expect(f.calls.filter(c=>c.method==='worker.prepare')).toHaveLength(1);
 expect(f.calls.filter(c=>c.method==='worker.inspectPreparation').every(c=>c.params.preparationIdentity===uncertain.request.preparationIdentity)).toBe(true);
});

it('does not copy or fall back when an existing reserved preparation is unconfirmed',async()=>{
 const f=await fixture();f.behavior.unconfirmed=true;
 await expect(preparePlanningSources(f.planning,f.plan,signal())).rejects.toThrow('WORKSPACE_INSPECTION_RESPONSE_INVALID');
 expect((await f.saved()).state).toBe('uncertain');expect(f.calls.some(c=>c.method==='worker.prepare')).toBe(false);
});

it.each(['source','acceptance','feedback','planOutput'] as const)('rejects changed %s authority between preparation and apply',async mutation=>{
 const f=await fixture(),sources=await preparePlanningSources(f.planning,f.plan,signal());
 if(mutation==='source')await pool.query("UPDATE tasks SET execution=execution||'{\"sourceWorkspace\":\"example/changed\"}' WHERE id=$1",[f.taskId]);
 if(mutation==='acceptance')await pool.query("INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer,created_at) VALUES($1,$2,$3,false,'[]','fixture',now()+interval '1 second')",[randomUUID(),f.taskId,f.runId]);
 if(mutation==='feedback')await pool.query("INSERT INTO objective_feedback(id,objective_id,trigger_key,description) VALUES($1,$2,'fresh-evidence','New requirement evidence must be considered.')",[randomUUID(),f.goalId]);
 if(mutation==='planOutput')await pool.query("UPDATE planning_runs SET output=output||'{\"summary\":\"another plan\"}' WHERE id=$1",[f.planning.id]);
 await expect(applyPlannedResult(f.planning,f.plan,sources)).rejects.toMatchObject({code:mutation==='feedback'?'PLANNING_CONTEXT_CHANGED':mutation==='planOutput'?'STALE_PLAN':'SOURCE_REUSE_AUTHORITY_CHANGED'});
 expect((await pool.query('SELECT count(*)::int n FROM tasks WHERE goal_id=$1 AND plan_revision=3',[f.goalId])).rows[0].n).toBe(0);
 expect((await f.saved()).state).toBe('prepared');
});

it('uses a distinct target and preparation identity for a later planner at the same objective revision',async()=>{
 const f=await fixture(),first=await preparePlanningSources(f.planning,f.plan,signal());
 await pool.query("UPDATE planning_runs SET state='failed' WHERE id=$1",[f.planning.id]);
 const next=await f.freshPlanning(),second=await preparePlanningSources(next,f.plan,signal());
 expect(first.get(f.base)!.intent.request.workspace).not.toBe(second.get(f.base)!.intent.request.workspace);
 expect(first.get(f.base)!.intent.request.preparationIdentity).not.toBe(second.get(f.base)!.intent.request.preparationIdentity);
 expect(f.preparations.size).toBe(2);
});

it('does not start legacy preparation when initial observation is aborted',async()=>{
 const f=await fixture(),controller=new AbortController();controller.abort();f.behavior.abortBeforeReserve=true;
 await expect(preparePlanningSources(f.planning,f.plan,controller.signal)).rejects.toBeDefined();
 expect(f.calls.some(c=>c.method==='worker.prepare')).toBe(false);expect(await f.saved()).toBeUndefined();
});
