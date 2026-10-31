import {beforeAll,beforeEach,afterAll,it,expect} from 'vitest';
import {randomUUID,createHash} from 'node:crypto';
import {pool,migrate,tx,lock} from '../server/db.js';
import {seedRegistry} from '../server/registry.js';
import {installTaskExecutionMetadata} from '../server/task-execution-metadata.js';
import {installSupervision,recordSupervisionDecisions,applySupervisionDecisions} from '../server/supervision.js';
import {installDeferredHandoffs,stageDeferredHandoff,applyDeferredHandoff} from '../server/deferred-handoffs.js';
import {reconcileDeliveryConsumptions} from '../server/delivery-consumption.js';
import {writeArtifact,completeRun,validateTask,failRun,readArtifact} from '../server/service.js';
import {glmRepairCriteria} from '../server/glm-repair.js';
import {acceptedDependencyArtifacts} from '../server/recovery.js';
import {assembleContext} from '../server/memory.js';
import {runtimeInputs} from '../server/execution-inputs.js';
import {patchComposition} from '../server/patch-composition.js';
import {effectiveSourceManifest} from '../server/effective-source-manifest.js';
import {resolvedInputSelection,assertResolvedInputSelection} from '../server/resolved-input-selection.js';
import {observeInputFailure} from '../server/input-failure-observation.js';
import {bindCompositionInputs} from '../bridge/composition-inputs.mjs';
import {recoverTransportTasks,transportRetryAdmission,admitTransportRetry,transportInputProjection} from '../server/transport-task-recovery.js';
import {event} from '../server/db.js';

const sha=(s:string)=>createHash('sha256').update(s).digest('hex');
const task=async(id:string)=>(await pool.query('SELECT * FROM tasks WHERE id=$1',[id])).rows[0];
beforeAll(async()=>{await migrate();await installTaskExecutionMetadata();await installDeferredHandoffs();await installSupervision();});
beforeEach(async()=>{await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');await seedRegistry();});
afterAll(()=>pool.end());
async function run(id:string){
  const t=await task(id),rid=randomUUID(),fence=randomUUID();
  await pool.query("UPDATE tasks SET state='running',attempt_count=attempt_count+1 WHERE id=$1",[id]);
  return (await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state)
    VALUES($1,$2,$3,$4,'harness',1,'running') RETURNING *`,[rid,id,t.attempt_count+1,fence])).rows[0];
}
async function publish(r:any,values:Record<string,unknown>){
  for(const [name,value] of Object.entries(values))await writeArtifact(r.id,r.fence,name,typeof value==='string'?value:JSON.stringify(value));
  await completeRun(r.id,r.fence,'Actual fixture result',{});
  expect((await validateTask(r.task_id)).passed).toBe(true);
  await pool.query("UPDATE goals SET state='active' WHERE id=(SELECT goal_id FROM tasks WHERE id=$1)",[r.task_id]);
}
async function stop(id:string){
  const r=(await pool.query('SELECT * FROM runs WHERE id=$1',[id])).rows[0],t=await task(r.task_id);
  const refs=(await pool.query("SELECT id FROM artifacts WHERE run_id=$1 AND name='candidate.json' ORDER BY id LIMIT 2",[id])).rows;
  const candidate=refs.length===1?JSON.parse((await readArtifact(refs[0].id)).bytes.toString()):null;
  return {runId:id,workspace:t.execution.workspace,checkedAt:new Date().toISOString(),
    ...(candidate?{candidate:{candidateDigest:candidate.candidateDigest}}:{}),
    process:{unit:'agentos-run-'+id,stopped:true,mainPid:0,populated:false,activeState:'inactive'}};
}
const candidateFor=(bytes:string)=>{
  const files=[{path:'src/app.js',sha256:sha(bytes)}],digest=sha(JSON.stringify(files.map(f=>[f.path,f.sha256])));
  return {files,baseDigest:digest,candidateDigest:digest};
};
async function repair(goal:string,legacy=false,scope='test/effective',workKind='repair'){
  const id=randomUUID();
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution)
    VALUES($1,$2,$3,'Repair','A versioned exact repair','engineering.integration','ready','work',$4,$5)`,[
    id,goal,'repair-'+id,JSON.stringify(glmRepairCriteria),JSON.stringify({kind:'harness',workKind,
      baseWorkspace:scope,workspace:'test/o-'+id,sourceWorkspace:'test/source'})]);
  let r:any;
  if(legacy)await pool.query('ALTER TABLE runs DISABLE TRIGGER agentos_instruction_snapshot');
  try{r=await run(id);}finally{if(legacy)await pool.query('ALTER TABLE runs ENABLE TRIGGER agentos_instruction_snapshot');}
  await publish(r,{
    'implementation.patch':'--- a/src/app.js\n+++ b/src/app.js\n@@ -1 +1 @@\n-original\n+fixed\n',
    'repair-report.json':{fixVerified:true,sourceHashes:[{path:'src/app.js',before:sha('original\n'),after:sha('fixed\n')}]},
    'repair-tests.log':'Actual retained diagnostic: before exit1; after exit0'});
  return {id,r,refs:(await pool.query('SELECT id,run_id,name,digest,size FROM artifacts WHERE run_id=$1 ORDER BY id',[r.id])).rows};
}
async function attach(f:any,p:any){
  await tx(async db=>stageDeferredHandoff(db,f.goal,p.id,await task(p.id),await task(f.consumer),
    {taskId:p.id,plannerRunId:p.r.id,coverageKey:'repair-'+p.id,lane:'glm',workKind:'repair'}));
  const h=(await pool.query('SELECT id FROM deferred_handoffs WHERE producer_task_id=$1',[p.id])).rows[0];
  expect(await applyDeferredHandoff(h.id,await stop(f.old.id),await stop(p.r.id))).toBe(true);
}
async function fixture(legacy=false,includeStoppedCandidate=false){
  const goal=randomUUID(),consumer=randomUUID();
  await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1::uuid,'Effective sources','Preserve QA',$2,$1::text)",[goal,'effective-'+goal]);
  await pool.query('INSERT INTO settings(id,value) VALUES($1,$2)',['supervision:'+goal,'{"enabled":true}']);
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution)
    VALUES($1,$2,$3,'Integrator','Preserve every criterion','engineering.integration','ready','integrate',
    '[{"id":"pass","label":"Original acceptance","type":"json_field","name":"verification.json","field":"passed","equals":true}]',$4)`,
    [consumer,goal,'consumer-'+consumer,JSON.stringify({kind:'harness',baseWorkspace:'test/effective',workspace:'test/o-'+consumer,sourceWorkspace:'test/source'})]);
  const old=await run(consumer);
  if(includeStoppedCandidate)await writeArtifact(old.id,old.fence,'candidate.json',JSON.stringify(candidateFor('original\n')));
  await failRun(old.id,old.fence,'RESULT_FORMAT','Fixture failed output');
  const a=await repair(goal,legacy),f={goal,consumer,old,a};await attach(f,a);return f;
}
async function resolve(f:any,original:any,replacement:any,mutate=true,candidateOverride?:string|false){
  if(mutate)await pool.query(`UPDATE tasks SET execution=jsonb_set(execution,'{recoveryContext}',$2::jsonb) WHERE id=$1`,
    [original.id,JSON.stringify({instruction:'Recertify '+randomUUID()})]);
  // Large fixtures use the real bounded/fair queue until this source is inspected.
  // No delivery receipt or supervision case is fabricated by the fixture.
  for(let sweep=0;sweep<(f.resolveSweeps??1);sweep++){
    await reconcileDeliveryConsumptions(stop);
    if(f.resolveSweeps){
      const pending=(await pool.query(`SELECT 1 FROM delivery_consumptions d JOIN supervision_cases c
        ON c.id::text=d.receipt->>'caseId' WHERE d.consumer_task_id=$1 AND d.producer_task_id=$2
        AND c.state='open' AND d.receipt->'sourceConflict' IS NOT NULL`,[f.consumer,f.a.id])).rowCount;
      if(pending)break;
    }
  }
  const d=(await pool.query('SELECT * FROM delivery_consumptions WHERE consumer_task_id=$1 AND producer_task_id=$2',[f.consumer,f.a.id===original.id?original.id:f.a.id])).rows[0];
  const c=(await pool.query('SELECT * FROM supervision_cases WHERE id=$1',[d.receipt.caseId])).rows[0];
  expect(c.state).toBe('open');
  const manager=randomUUID(),action={caseId:c.id,kind:'revise',diagnosis:'Use the recertified immutable source.',
    instruction:'Compose the exact replacement, preserving original criteria and independent QA.',sourceTaskId:replacement.id,
    deliveryResolution:{obligationId:d.id,generation:d.receipt.sourceConflict.generation,expectedSourceRunId:replacement.r.id}};
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria)
    VALUES($1,$2,$3,'Manager','Select an exact source','qa.backend','ready','coordinate',
    '[{"id":"decision","type":"artifact","name":"coordination.json"}]')`,[manager,f.goal,'manager-'+manager]);
  const review=await run(manager);await publish(review,{'coordination.json':{supervision:{actions:[action]}}});
  await recordSupervisionDecisions();
  const inspect=async(id:string)=>{
    const proof=await stop(id);
    if(id!==f.old.id||candidateOverride===undefined)return proof;
    return {...proof,candidate:candidateOverride===false?undefined:{candidateDigest:candidateOverride}};
  };
  await applySupervisionDecisions(inspect);
  if(candidateOverride!==undefined){
    expect((await pool.query('SELECT state FROM supervision_cases WHERE id=$1',[c.id])).rows[0].state).toBe('open');
    expect((await pool.query("SELECT payload->>'code' code FROM events WHERE kind='DELIVERY_SOURCE_DECISION_REJECTED' AND payload->>'caseId'=$1 ORDER BY id DESC LIMIT 1",[c.id])).rows[0].code)
      .toBe('DELIVERY_RESOLUTION_CANDIDATE_PROOF_CHANGED');
    return null;
  }
  expect((await pool.query('SELECT state FROM supervision_cases WHERE id=$1',[c.id])).rows[0].state).toBe('applied');
  return (await pool.query("SELECT id::text,payload FROM events e WHERE kind='DELIVERY_SOURCE_RESOLVED' AND entity_id=$1 ORDER BY e.id DESC LIMIT 1",[d.id])).rows[0];
}
async function prepared(){const f=await fixture(),b=await repair(f.goal),resolution=await resolve(f,f.a,b);return {...f,b,resolution};}
async function pipeline(f:any){
  const r=await run(f.consumer),t=await task(f.consumer),dependencies=await acceptedDependencyArtifacts(f.consumer);
  const context=await assembleContext(r,t,dependencies),inputs=await runtimeInputs(t,context.manifest,readArtifact);
  return {r,t,dependencies,context,inputs,composition:patchComposition(t,context.manifest,inputs)};
}
it.each(['ordinary','target-and-history'])('transport retry recomputes %s resolved/effective sources without reintroducing replaced patches',async kind=>{
  const f=kind==='ordinary'?await prepared():await mixedHistoricalTargets(1);
  if(kind==='target-and-history'){
    const newer=await run(f.a.id);
    await publish(newer,{'implementation.patch':'--- a/src/app.js\n+++ b/src/app.js\n@@ -1 +1 @@\n-original\n+alternative\n',
      'repair-report.json':{fixVerified:true,sourceHashes:[{path:'src/app.js',before:sha('original\n'),after:sha('alternative\n')}]},
      'repair-tests.log':'New accepted historical source, not a new consumer handoff.'});
  }
  await pool.query("INSERT INTO objectives(id,request,request_hash,mode,status,revision) VALUES($1,'{}','fixture','execute','executing',1)",[f.goal]);
  await pool.query('UPDATE tasks SET plan_revision=1,max_attempts=6 WHERE id=$1',[f.consumer]);
  const first=await pipeline(f);
  expect(first.context.manifest).toHaveProperty('effectiveSources');
  expect(first.r.instruction_snapshot.revisionId).toBeTruthy();
  if(kind==='target-and-history'){
    expect(first.context.manifest).toHaveProperty('resolvedInputSelection');
    expect(first.context.manifest).toHaveProperty('effectiveSources.compositionSelection.targetAuthority');
  }
  await event(pool,'MODEL_TRANSPORT_EXHAUSTED',f.consumer,{version:1,source:'runtime-generation-boundary',runId:first.r.id,
    runFence:first.r.fence,contextVersion:first.context.id,requestId:randomUUID(),generationAttempt:3,
    reason:'provider_unavailable',code:'MODEL_REQUEST_FAILED',responseDelivered:false,traceComplete:true,pendingTools:0,cancelled:false});
  await failRun(first.r.id,first.r.fence,'MODEL_REQUEST_FAILED','Late 502 without any delivered response');
  const proof=async()=>({runId:first.r.id,executionRunId:first.r.id,workspace:first.t.execution.workspace,
    checkedAt:new Date().toISOString(),candidate:{baseDigest:sha('base'),candidateDigest:sha('partial')},
    process:{unit:'agentos-run-'+first.r.id,stopped:true,mainPid:0,populated:false,activeState:'inactive'}});
  expect(await recoverTransportTasks(proof)).toEqual([f.consumer]);
  const rid=randomUUID(),fence=randomUUID();
  await tx(async db=>{
    await lock(db);const receipt=await transportRetryAdmission(db,f.consumer);expect(receipt).toBeTruthy();
    await db.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,$3,$4,'harness',1,'running')",[rid,f.consumer,first.r.attempt+1,fence]);
    await admitTransportRetry(db,receipt,rid,fence);
    await db.query("UPDATE tasks SET state='running',attempt_count=attempt_count+1 WHERE id=$1",[f.consumer]);
  });
  const r=(await pool.query('SELECT * FROM runs WHERE id=$1',[rid])).rows[0];
  const t=await task(f.consumer),next=await assembleContext(r,t,await acceptedDependencyArtifacts(t.id));
  expect(transportInputProjection(next.manifest)).toEqual(transportInputProjection(first.context.manifest));
  expect(next.manifest).toHaveProperty('transportRetry');
  const inputs=await runtimeInputs(t,next.manifest,readArtifact),composition=patchComposition(t,next.manifest,inputs);
  expect(composition?.patches).toEqual(first.composition?.patches);
  expect(composition?.expectedCandidateDigest).toEqual(first.composition?.expectedCandidateDigest);
  expect(bindCompositionInputs(composition,inputs)).toBeTruthy();
});
function hasActiveCompositionSelection(value:unknown):value is {effectiveSources:{compositionSelection:{active:{producerTaskId:string}[]}}}{
  if(!value||typeof value!=='object'||!('effectiveSources' in value))return false;
  const effectiveSources=value.effectiveSources;
  if(!effectiveSources||typeof effectiveSources!=='object'||!('compositionSelection' in effectiveSources))return false;
  const selection=effectiveSources.compositionSelection;
  return !!selection&&typeof selection==='object'&&'active' in selection&&Array.isArray(selection.active)&&
    selection.active.every((item:unknown)=>!!item&&typeof item==='object'&&'producerTaskId' in item&&typeof item.producerTaskId==='string');
}

it('freezes the next real context from applied replacement, preserving DAG, diagnoses, old snapshot and acceptance',async()=>{
  const f=await prepared(),before=(await pool.query('SELECT instruction_snapshot FROM runs WHERE id=$1',[f.old.id])).rows[0];
  const deps=(await pool.query('SELECT * FROM dependencies WHERE task_id=$1 ORDER BY depends_on',[f.consumer])).rows;
  const p=await pipeline(f),m:any=p.context.manifest;
  expect(p.composition?.patches.map(x=>x.producerTaskId)).toEqual([f.b.id]);
  expect(p.inputs.filter(x=>x.name==='implementation.patch').map(x=>x.id)).toEqual([f.b.refs.find(x=>x.name==='implementation.patch').id]);
  expect(m.dependencyArtifacts.some((a:any)=>a.task_id===f.a.id&&a.name==='repair-tests.log')).toBe(true);
  expect(m.effectiveSources).toMatchObject({runId:p.r.id,runFence:p.r.fence,acceptance:false,compositionRequired:true,independentQARequired:true});
  expect(m.effectiveSources).not.toHaveProperty('compositionSelection');
  expect(m.effectiveSources.bindings[0].eventIds).toEqual([f.resolution.id]);
  expect(m.effectiveSources.historicalArtifacts).toHaveLength(2);
  expect((await pool.query('SELECT * FROM dependencies WHERE task_id=$1 ORDER BY depends_on',[f.consumer])).rows).toEqual(deps);
  expect((await pool.query('SELECT instruction_snapshot FROM runs WHERE id=$1',[f.old.id])).rows[0]).toEqual(before);
  expect((await task(f.a.id)).state).toBe('accepted');expect((await task(f.b.id)).state).toBe('accepted');
  expect((await pool.query('SELECT count(*)::int n FROM acceptances WHERE run_id=$1',[p.r.id])).rows[0].n).toBe(0);
  let queries=0;const started=performance.now();
  const again=await tx(async db=>{await lock(db);return effectiveSourceManifest({query:((...args:any[])=>{
    queries++;return (db.query as any)(...args);
  }) as typeof db.query},p.r,p.t,p.dependencies);});
  expect(again.effectiveSources).toEqual(m.effectiveSources);
  console.log(JSON.stringify({probe:'effective-source-under-lock',queries,elapsedMs:Math.round(performance.now()-started),
    obligations:m.effectiveSources.bindings.length,activeArtifacts:again.dependencyArtifacts.length}));
});

it('requires accepted dependency transport before freezing an applied source resolution',async()=>{
  const f=await prepared(),r=await run(f.consumer),t=await task(f.consumer);
  const before=(await pool.query('SELECT instruction_snapshot FROM runs WHERE id=$1',[r.id])).rows[0];
  await expect(assembleContext(r,t,[])).rejects.toMatchObject({code:'INPUT_MANIFEST_CHANGED',
    inputFailure:{phase:'effective_source_manifest',subcode:'EFFECTIVE_INPUT_MISSING'}});
  expect((await pool.query('SELECT id FROM contexts WHERE run_id=$1',[r.id])).rowCount).toBe(0);
  const dependencies=await acceptedDependencyArtifacts(t.id);
  expect(dependencies.some(a=>a.task_id===f.b.id&&a.name==='implementation.patch')).toBe(true);
  const context=await assembleContext(r,t,dependencies);
  const inputs=await runtimeInputs(t,context.manifest,readArtifact);
  expect(patchComposition(t,context.manifest,inputs)?.patches.map(a=>a.producerTaskId)).toEqual([f.b.id]);
  expect((context.manifest as any).effectiveSources).toMatchObject({runId:r.id,runFence:r.fence,acceptance:false});
  expect((await pool.query('SELECT instruction_snapshot FROM runs WHERE id=$1',[r.id])).rows[0]).toEqual(before);
  expect((await pool.query('SELECT id FROM acceptances WHERE run_id=$1',[r.id])).rowCount).toBe(0);
});

it.each(['acceptance-revoked','source-contract-changed'] as const)
('does not make a resolved %s dependency usable through the canonical transport',async change=>{
  const f=await prepared();
  if(change==='acceptance-revoked')await pool.query(`INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer,created_at)
    VALUES($1,$2,$3,false,'[]','fixture-revocation',clock_timestamp())`,[randomUUID(),f.b.id,f.b.r.id]);
  else await pool.query("UPDATE tasks SET description=description||' Changed after acceptance' WHERE id=$1",[f.b.id]);
  const r=await run(f.consumer),t=await task(f.consumer),dependencies=await acceptedDependencyArtifacts(t.id);
  await expect(assembleContext(r,t,dependencies)).rejects.toMatchObject({code:'INPUT_MANIFEST_CHANGED'});
  expect((await pool.query('SELECT id FROM contexts WHERE run_id=$1',[r.id])).rowCount).toBe(0);
  expect((await pool.query('SELECT id FROM acceptances WHERE run_id=$1',[r.id])).rowCount).toBe(0);
});

it('keeps the default inline capacity guard and records its structured refusal without launching',async()=>{
  const f=await fixture(),r=await run(f.consumer),t=await task(f.consumer),dependencies=await acceptedDependencyArtifacts(t.id);
  const context=await assembleContext(r,t,dependencies),bytes=Buffer.alloc(7*1024*1024,65);
  const refs=Array.from({length:4},(_,i)=>({id:randomUUID(),name:'large-'+i+'.bin',digest:sha(bytes.toString())}));
  let launchCount=0,error:any;
  // Exercise the real default transport, with bounded artifact I/O substituted.
  // No referenced transport, slicing or dropped refs can make this preflight pass.
  try{
    await runtimeInputs(t,{...context.manifest,dependencyArtifacts:refs},async id=>{
      const ref=refs.find(ref=>ref.id===id)!;return {...ref,bytes};
    });
    launchCount++;
  }catch(e){error=e;}
  expect(launchCount).toBe(0);
  expect(error).toMatchObject({code:'SUPERVISION_INPUTS_TOO_LARGE',inputFailure:{phase:'runtime_inputs',
    subcode:'TOTAL_BYTES',inputCount:4,totalBytes:28*1024*1024,limitBytes:24*1024*1024}});
  expect(await observeInputFailure(pool,r,error)).toBe(true);
  expect((await pool.query("SELECT payload FROM events WHERE kind='RUN_INPUT_FAILURE' AND entity_id=$1",[t.id])).rows[0].payload)
    .toMatchObject({runId:r.id,code:'SUPERVISION_INPUTS_TOO_LARGE',phase:'runtime_inputs',subcode:'TOTAL_BYTES',limitBytes:24*1024*1024});
  expect((await pool.query("SELECT id FROM events WHERE entity_id=$1 AND kind IN('OPENCODE_LAUNCH_ATTEMPTED','HARNESS_LAUNCH_ATTEMPTED')",[t.id])).rowCount).toBe(0);
  expect((await pool.query('SELECT id FROM acceptances WHERE run_id=$1',[r.id])).rowCount).toBe(0);
});

it('binds a supervisor-applied source resolution to the stopped candidate artifact through context and composer',async()=>{
  const f=await fixture(false,true),targetRef=(await pool.query("SELECT id,run_id,name,digest,size FROM artifacts WHERE run_id=$1 AND name='candidate.json'",[f.old.id])).rows[0];
  const b=await repair(f.goal),resolution=await resolve(f,f.a,b),p=await pipeline(f),m:any=p.context.manifest,selection=m.effectiveSources.compositionSelection;
  expect(selection).toMatchObject({target:{consumerTaskId:f.consumer,consumerRunId:f.old.id,
    candidateArtifactId:targetRef.id,candidateArtifactDigest:targetRef.digest,candidateDigest:candidateFor('original\n').candidateDigest}});
  expect(p.composition).toMatchObject({version:2,expectedCandidateDigest:candidateFor('original\n').candidateDigest,
    selection:{resolutionEventIds:[resolution.id]}});
  expect(p.composition?.patches.map((x:any)=>x.producerTaskId)).toEqual([b.id]);
});

it('keeps unrelated accepted dependency repairs active in the bound composition selection',async()=>{
  const f=await fixture(false,true),independent=await repair(f.goal);await attach(f,independent);
  const replacement=await repair(f.goal),resolution=await resolve(f,f.a,replacement),p=await pipeline(f);
  if(!hasActiveCompositionSelection(p.context.manifest))throw new Error('COMPOSITION_SELECTION_MISSING');
  const selection=p.context.manifest.effectiveSources.compositionSelection;
  expect(selection.active.map((item:any)=>item.producerTaskId).sort()).toEqual([independent.id,replacement.id].sort());
  expect(p.composition?.patches.map((item:any)=>item.producerTaskId).sort()).toEqual([independent.id,replacement.id].sort());
  expect(p.composition?.selection.resolutionEventIds).toContain(resolution.id);
});

async function mixedHistoricalTargets(historicalCount=1){
  const base=await fixture(),f={...base,resolveSweeps:8},resolutions:any[]=[],replacements:any[]=[];
  for(let index=0;index<historicalCount;index++){
    const original=index===0?f.a:await repair(f.goal);
    if(index>0)await attach(f,original);
    const replacement=await repair(f.goal);
    resolutions.push(await resolve({...f,a:original},original,replacement));replacements.push(replacement);
  }
  const current=await run(f.consumer);
  await writeArtifact(current.id,current.fence,'candidate.json',JSON.stringify(candidateFor('original\n')));
  await failRun(current.id,current.fence,'RESULT_FORMAT','New stopped candidate after historical resolutions');
  const original=await repair(f.goal),replacement=await repair(f.goal),latest={...f,old:current,a:original};
  await attach(latest,original);
  resolutions.push(await resolve(latest,original,replacement));replacements.push(replacement);
  return {...latest,resolutions,replacements,historicalRun:base.old};
}

it('carries one newer applied candidate authority across five older targetless resolutions without rewriting history',async()=>{
  const f=await mixedHistoricalTargets(5),eventIds=f.resolutions.map(e=>e.id);
  const before=(await pool.query('SELECT id::text,payload FROM events WHERE id=ANY($1::bigint[]) ORDER BY id',[eventIds])).rows;
  const oldSnapshots=(await pool.query('SELECT id,instruction_snapshot FROM runs WHERE id=ANY($1::uuid[]) ORDER BY id',
    [[f.historicalRun.id,f.old.id]])).rows;
  const p=await pipeline(f),m:any=p.context.manifest,selection=m.effectiveSources.compositionSelection;
  expect(m.effectiveSources.bindings).toHaveLength(6);
  expect(m.effectiveSources.bindings.filter((b:any)=>b.targetCandidate===null)).toHaveLength(5);
  expect(selection.target.consumerRunId).toBe(f.old.id);
  expect(selection.targetAuthority).toMatchObject({version:1,authority:'applied_consumer_revision',
    consumerRunId:f.old.id,consumerAttempt:2});
  expect(selection.targetAuthority.targetResolutionWitnesses).toHaveLength(1);
  expect(selection.targetAuthority.historicalResolutionWitnesses).toHaveLength(5);
  const newest=selection.targetAuthority.targetResolutionWitnesses[0];
  expect(selection.targetAuthority.historicalResolutionWitnesses.every((w:any)=>w.consumerAttempt===1&&
    w.revisionOriginId===newest.revisionOriginId&&w.revisionNumber<newest.revisionNumber)).toBe(true);
  expect(selection.active).toHaveLength(6);expect(selection.superseded).toHaveLength(6);
  expect(p.composition?.patches.map(x=>x.producerTaskId).sort()).toEqual(f.replacements.map(r=>r.id).sort());
  expect((bindCompositionInputs(p.composition,p.inputs) as any).selection).toEqual(selection);
  expect((await pool.query('SELECT id::text,payload FROM events WHERE id=ANY($1::bigint[]) ORDER BY id',[eventIds])).rows).toEqual(before);
  expect((await pool.query('SELECT id,instruction_snapshot FROM runs WHERE id=ANY($1::uuid[]) ORDER BY id',
    [[f.historicalRun.id,f.old.id]])).rows).toEqual(oldSnapshots);
  expect((await pool.query('SELECT id FROM acceptances WHERE run_id=$1',[p.r.id])).rowCount).toBe(0);
});

it.each(['acceptance','contract'] as const)('preserves the historical-source %s guard when a newer global target exists',async drift=>{
  const f=await mixedHistoricalTargets(),replacement=f.replacements[0];
  if(drift==='acceptance')await pool.query(`INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer,created_at)
    VALUES($1,$2,$3,false,'[]','fixture-revocation',clock_timestamp())`,[randomUUID(),replacement.id,replacement.r.id]);
  else await pool.query("UPDATE tasks SET description=description||' drift after acceptance' WHERE id=$1",[replacement.id]);
  await expect(pipeline(f)).rejects.toMatchObject({code:'INPUT_MANIFEST_CHANGED',inputFailure:{subcode:'EFFECTIVE_SOURCE_CHANGED'}});
  expect((await pool.query('SELECT id FROM contexts WHERE run_id=(SELECT id FROM runs WHERE task_id=$1 ORDER BY attempt DESC LIMIT 1)',[f.consumer])).rowCount).toBe(0);
});

it.each(['missing-origin','foreign-origin'] as const)('refuses a %s history witness without persisting an invented global authority',async mode=>{
  const f=await mixedHistoricalTargets(),r=await run(f.consumer),t=await task(f.consumer),dependencies=await acceptedDependencyArtifacts(t.id);
  await expect(tx(async db=>{await lock(db);return effectiveSourceManifest({query:(async(sql:any,args:any)=>{
    const result=await db.query(sql,args);
    return String(sql).includes('history.origin_id revision_origin_id')?{...result,rows:result.rows.map((e:any)=>
      e.event_id===f.resolutions[0].id?{...e,revision_origin_id:mode==='missing-origin'?null:randomUUID()}:e)}:result;
  }) as typeof db.query},r,t,dependencies);})).rejects.toMatchObject({code:'INPUT_MANIFEST_CHANGED',
    inputFailure:{subcode:'TARGET_CANDIDATE_AUTHORITY_UNPROVEN'}});
  expect((await pool.query('SELECT id FROM contexts WHERE run_id=$1',[r.id])).rowCount).toBe(0);
});

it.each([
  ['same-goal repair from another scope','other/effective','repair',false],
  ['same-scope diagnostic task','test/effective','diagnostic',false],
  ['accepted repair whose frozen contract changed','test/effective','repair',true],
] as const)('refuses an active dependency that is not a frozen same-scope repair: %s',async(_label,scope,workKind,mutate)=>{
  const f=await fixture(false,true),invalid=await repair(f.goal,false,scope,workKind);
  await attach(f,invalid);
  if(mutate)await pool.query("UPDATE tasks SET description=description||' changed after acceptance' WHERE id=$1",[invalid.id]);
  const replacement=await repair(f.goal),resolution=await resolve(f,f.a,replacement);
  expect(resolution).toBeTruthy();
  await expect(pipeline(f)).rejects.toMatchObject({code:'INPUT_MANIFEST_CHANGED',
    inputFailure:{subcode:'COMPOSITION_ACTIVE_SOURCE_CHANGED'}});
  expect((await pool.query('SELECT count(*)::int n FROM contexts WHERE run_id=(SELECT id FROM runs WHERE task_id=$1 ORDER BY attempt DESC LIMIT 1)',[f.consumer])).rows[0].n).toBe(0);
});

it.each([['drifted-stop-candidate','f'.repeat(64)],['missing-stop-candidate',false]] as const)
('rejects %s when a canonical candidate artifact exists',async(_name,override)=>{
  const f=await fixture(false,true),b=await repair(f.goal);
  await resolve(f,f.a,b,true,override);
  expect((await pool.query("SELECT 1 FROM events WHERE kind='DELIVERY_SOURCE_RESOLVED' AND entity_id=(SELECT id::text FROM delivery_consumptions WHERE consumer_task_id=$1)",[f.consumer])).rowCount).toBe(0);
});

it('preserves explicit frozen historical bytes without reintroducing an active compositor patch',async()=>{
  const f=await prepared(),ref=f.a.refs.find(x=>x.name==='implementation.patch'),bytes=await readArtifact(ref.id);
  await pool.query(`UPDATE tasks SET execution=jsonb_set(execution,'{inputs}',$2::jsonb) WHERE id=$1`,
    [f.consumer,JSON.stringify([{id:ref.id,name:ref.name,digest:ref.digest,base64:bytes.bytes.toString('base64')}])]);
  const p=await pipeline(f),m:any=p.context.manifest;
  expect(p.inputs.some(x=>x.id===ref.id)).toBe(true);expect(m.effectiveSources.explicitHistoricalInputIds).toEqual([ref.id]);
  expect(p.composition?.patches.map(x=>x.producerTaskId)).toEqual([f.b.id]);
  expect(p.r.instruction_snapshot.executionInputs[0]).toMatchObject({id:ref.id,digest:ref.digest});
  expect(p.context.content).toContain('retained historical evidence');
});

it('retains the exact legacy list when no formal resolution exists',async()=>{
  const f=await fixture(),p=await pipeline(f);
  expect(p.context.manifest.dependencyArtifacts).toEqual(p.dependencies);
  expect(p.context.manifest).not.toHaveProperty('effectiveSources');
  expect(p.composition?.patches.map(x=>x.producerTaskId)).toEqual([f.a.id]);
});

it('follows the complete same-obligation chain and archives both superseded pairs',async()=>{
  const base=await fixture(false,true),b=await repair(base.goal),first=await resolve(base,base.a,b),f={...base,b,resolution:first};
  const attempt=await pipeline(f);
  await writeArtifact(attempt.r.id,attempt.r.fence,'candidate.json',JSON.stringify(candidateFor('fixed\n')));
  await failRun(attempt.r.id,attempt.r.fence,'RESULT_FORMAT','Second real stopped candidate fixture');
  const c=await repair(f.goal),second=await resolve(f,b,c),p=await pipeline({...f,c}),m:any=p.context.manifest;
  expect(m.effectiveSources.bindings[0].eventIds).toEqual([f.resolution.id,second.id]);
  expect(m.effectiveSources.historicalArtifacts).toHaveLength(4);
  expect(m.effectiveSources.compositionSelection.target).toMatchObject({consumerRunId:attempt.r.id,
    candidateDigest:candidateFor('fixed\n').candidateDigest});
  expect(p.composition?.patches.map(x=>x.producerTaskId)).toEqual([c.id]);
  await pool.query("UPDATE events SET payload=payload-'targetCandidate' WHERE id=$1",[second.id]);
  await expect(tx(async db=>{await lock(db);return effectiveSourceManifest(db,p.r,p.t,p.dependencies);})).rejects.toThrow('TARGET_CANDIDATE_CHANGED');
});

it.each(['source-contract','source-acceptance','replacement-artifact','dependency','consumer-contract','fence','snapshot','chain','original-binding','original-manifest','wrong-consumer'] as const)
 ('refuses %s drift without persisting a partial context or changing prior proof',async mode=>{
  const f=await prepared(),r=await run(f.consumer),t=await task(f.consumer),refs=await acceptedDependencyArtifacts(f.consumer);
  const old=(await pool.query('SELECT instruction_snapshot FROM runs WHERE id=$1',[f.old.id])).rows[0];
  if(mode==='source-contract')await pool.query("UPDATE tasks SET description=description||' drift' WHERE id=$1",[f.b.id]);
  if(mode==='source-acceptance')await pool.query("INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) VALUES($1,$2,$3,false,'[]','test')",[randomUUID(),f.b.id,f.b.r.id]);
  if(mode==='replacement-artifact')await pool.query("UPDATE artifacts SET digest=$2 WHERE id=$1",[f.b.refs[0].id,'0'.repeat(64)]);
  if(mode==='dependency')await pool.query('DELETE FROM dependencies WHERE task_id=$1 AND depends_on=$2',[f.consumer,f.b.id]);
  if(mode==='consumer-contract')t.criteria=[];
  if(mode==='fence')r.fence=randomUUID();
  if(mode==='snapshot')r.instruction_snapshot=null;
  if(mode==='chain')await pool.query("UPDATE events SET payload=jsonb_set(payload,'{previousResolutionEventId}','\"999999\"') WHERE id=$1",[f.resolution.id]);
  if(mode==='original-binding')await pool.query("UPDATE delivery_consumptions SET binding=jsonb_set(binding,'{repairArtifacts}', '[]') WHERE id=$1",[f.resolution.payload.obligationId]);
  if(mode==='original-manifest')await pool.query("UPDATE delivery_consumptions SET binding=jsonb_set(binding,'{producerArtifacts}', '[]') WHERE id=$1",[f.resolution.payload.obligationId]);
  if(mode==='wrong-consumer')await pool.query("UPDATE events SET payload=jsonb_set(payload,'{consumerTaskId}',to_jsonb($2::text)) WHERE id=$1",[f.resolution.id,randomUUID()]);
  if(mode==='consumer-contract')await expect(tx(async db=>{await lock(db);return effectiveSourceManifest(db,r,t,refs);})).rejects.toThrow();
  else await expect(assembleContext(r,t,refs)).rejects.toThrow();
  expect((await pool.query('SELECT count(*)::int n FROM contexts WHERE run_id=$1',[r.id])).rows[0].n).toBe(0);
  expect((await pool.query('SELECT count(*)::int n FROM artifacts WHERE run_id=$1',[r.id])).rows[0].n).toBe(0);
  expect((await pool.query('SELECT instruction_snapshot FROM runs WHERE id=$1',[f.old.id])).rows[0]).toEqual(old);
});

it('uses a proved replacement for a legacy source without inventing its missing snapshot',async()=>{
  const f=await fixture(true),b=await repair(f.goal);expect(f.a.r.instruction_snapshot).toBeNull();
  await resolve(f,f.a,b);const p=await pipeline(f);
  expect(p.composition?.patches.map(x=>x.producerTaskId)).toEqual([b.id]);
  expect((await pool.query('SELECT instruction_snapshot FROM runs WHERE id=$1',[f.a.r.id])).rows[0].instruction_snapshot).toBeNull();
});

it('deduplicates one identical effective source shared by two explicitly resolved obligations',async()=>{
  const f=await fixture(),d=await repair(f.goal),b=await repair(f.goal);await attach(f,d);
  await resolve(f,f.a,b);await resolve({...f,a:d},d,b);
  const p=await pipeline(f),m:any=p.context.manifest;
  expect(m.effectiveSources.bindings).toHaveLength(2);expect(m.effectiveSources.historicalArtifacts).toHaveLength(4);
  expect(p.composition?.patches.map(x=>x.producerTaskId)).toEqual([b.id]);
  expect(p.inputs.filter(x=>x.name==='implementation.patch')).toHaveLength(1);
});

it('refuses conflicting current candidate proofs across resolved obligations without falling back to v1',async()=>{
  const f=await fixture(false,true),d=await repair(f.goal),b=await repair(f.goal);await attach(f,d);
  await resolve(f,f.a,b);await resolve({...f,a:d},d,b);
  const other=(await pool.query("SELECT id FROM delivery_consumptions WHERE consumer_task_id=$1 AND producer_task_id=$2",[f.consumer,d.id])).rows[0];
  const event=(await pool.query("SELECT id,payload FROM events WHERE kind='DELIVERY_SOURCE_RESOLVED' AND entity_id=$1 ORDER BY id DESC LIMIT 1",[other.id])).rows[0];
  await pool.query('UPDATE events SET payload=jsonb_set(payload,\'{targetCandidate,candidateDigest}\',$2::jsonb) WHERE id=$1',
    [event.id,JSON.stringify('f'.repeat(64))]);
  await expect(pipeline(f)).rejects.toMatchObject({code:'INPUT_MANIFEST_CHANGED',inputFailure:{subcode:'TARGET_CANDIDATE_AMBIGUOUS'}});
  expect((await pool.query('SELECT count(*)::int n FROM contexts WHERE run_id=(SELECT id FROM runs WHERE task_id=$1 ORDER BY attempt DESC LIMIT 1)',[f.consumer])).rows[0].n).toBe(0);
});

it('does not infer transitive authority across separate obligations',async()=>{
  const f=await fixture(),b=await repair(f.goal),c=await repair(f.goal);await attach(f,b);
  await resolve(f,f.a,b);await resolve({...f,a:b},b,c);
  await expect(pipeline(f)).rejects.toMatchObject({code:'INPUT_MANIFEST_CHANGED'});
});

it('does not replace an existing immutable context when a later resolution appears',async()=>{
  const f=await prepared(),p=await pipeline(f),before=(await pool.query('SELECT manifest,content FROM contexts WHERE id=$1',[p.context.id])).rows[0];
  // Simulate a newly committed receipt after the context was frozen; the latest
  // event cannot be replayed into this already-admitted run.
  await pool.query(`INSERT INTO events(kind,entity_id,payload,created_at)
    SELECT kind,entity_id,payload,clock_timestamp() FROM events WHERE id=$1`,[f.resolution.id]);
  await expect(assembleContext(p.r,p.t,p.dependencies)).rejects.toMatchObject({code:'INPUT_MANIFEST_CHANGED'});
  expect((await pool.query('SELECT manifest,content FROM contexts WHERE id=$1',[p.context.id])).rows[0]).toEqual(before);
  expect((await pool.query('SELECT context_id FROM runs WHERE id=$1',[p.r.id])).rows[0].context_id).toBe(p.context.id);
});

it('uses immutable applied revision proof after the mutable case moves to another lifecycle state',async()=>{
  const f=await prepared(),r=await run(f.consumer),t=await task(f.consumer);
  await pool.query("UPDATE supervision_cases SET state='resolved',decision_run_id=NULL,decision=NULL,applied_attempt=NULL WHERE id=$1",[f.resolution.payload.caseId]);
  const ctx=await assembleContext(r,t,await acceptedDependencyArtifacts(f.consumer));
  const inputs=await runtimeInputs(t,ctx.manifest,readArtifact);
  expect(patchComposition(t,ctx.manifest,inputs)?.patches.map(x=>x.producerTaskId)).toEqual([f.b.id]);
});

async function sameTaskResolved(){
  const f=await fixture(),r=await run(f.a.id),values:Record<string,string>={};
  for(const ref of f.a.refs)values[ref.name]=(await readArtifact(ref.id)).bytes.toString();
  await publish(r,values);
  const replacement={id:f.a.id,r,refs:(await pool.query('SELECT id,run_id,name,digest,size FROM artifacts WHERE run_id=$1 ORDER BY id',[r.id])).rows};
  await resolve(f,f.a,replacement,false);return {f,r,replacement};
}
it('accepts the same repair task at a newly accepted frozen run rather than misclassifying a task identity as a cycle',async()=>{
  const {f,r,replacement}=await sameTaskResolved(),p=await pipeline(f),m:any=p.context.manifest;
  expect(p.composition?.patches).toHaveLength(1);
  expect(p.composition?.patches[0].patch.id).toBe(replacement.refs.find(x=>x.name==='implementation.patch').id);
  expect(m.effectiveSources.bindings[0].original.runId).toBe(f.a.r.id);
  expect(m.effectiveSources.bindings[0].effectiveSource.runId).toBe(r.id);
});

it.each(['original','new'] as const)('distinguishes an unresolved sibling on the %s source attempt',async version=>{
  const {f,r}=await sameTaskResolved(),current=await run(f.consumer),t=await task(f.consumer),deps=await acceptedDependencyArtifacts(f.consumer);
  // Historical imports can contain an independent attachment. Inject only its
  // identity in the read projection, without fabricating a decision or writer.
  const pending={id:randomUUID(),producer_task_id:f.a.id,producer_run_id:version==='original'?f.a.r.id:r.id};
  const read=()=>tx(async db=>{await lock(db);return effectiveSourceManifest({query:(async(sql:any,values:any)=>{
    const result=await db.query(sql,values);
    return String(sql).includes('SELECT id,producer_task_id,producer_run_id FROM delivery_consumptions')?
      {...result,rows:[...result.rows,pending]}:result;
  }) as typeof db.query},current,t,deps);});
  if(version==='original')await expect(read()).rejects.toMatchObject({code:'INPUT_MANIFEST_CHANGED'});
  else expect((await read()).effectiveSources?.bindings).toHaveLength(1);
});

it('uses the frozen revision chain rather than a transaction start timestamp for admission ordering',async()=>{
  const f=await prepared(),r=await run(f.consumer),t=await task(f.consumer);
  await pool.query("UPDATE runs SET created_at=(SELECT created_at-interval '1 second' FROM events WHERE id=$2) WHERE id=$1",[r.id,f.resolution.id]);
  const ctx=await assembleContext(r,t,await acceptedDependencyArtifacts(f.consumer));
  expect((ctx.manifest as any).effectiveSources.bindings[0].resolutionEventId).toBe(f.resolution.id);
});

it('classifies later output of an entirely replaced producer as history while freezing the approved replacement and preserving every source',async()=>{
  const f=await prepared(),before=(await pool.query('SELECT instruction_snapshot FROM runs WHERE id=$1',[f.old.id])).rows[0];
  const a=await run(f.a.id);
  await publish(a,{'implementation.patch':'--- a/src/app.js\n+++ b/src/app.js\n@@ -1 +1 @@\n-original\n+alternative\n',
    'repair-report.json':{fixVerified:true,sourceHashes:[{path:'src/app.js',before:sha('original\n'),after:sha('alternative\n')}]},
    'repair-tests.log':'Actual newer producer result; no new consumer handoff.'});
  const newer=(await pool.query('SELECT id,name,digest FROM artifacts WHERE run_id=$1',[a.id])).rows;
  const p=await pipeline(f),m:any=p.context.manifest;
  expect(p.composition?.patches.map(x=>x.producerTaskId)).toEqual([f.b.id]);
  expect(m.resolvedInputSelection).toMatchObject({authority:'selection_only',historyPreserved:true,acceptance:false});
  expect(m.resolvedInputSelection.historicalLatestArtifacts.map((a:any)=>a.id).sort())
    .toEqual(newer.filter(a=>['implementation.patch','repair-report.json'].includes(a.name)).map(a=>a.id).sort());
  expect(m.dependencyArtifacts.some((a:any)=>a.task_id===f.a.id&&a.name==='repair-tests.log')).toBe(true);
  expect((await pool.query('SELECT instruction_snapshot FROM runs WHERE id=$1',[f.old.id])).rows[0]).toEqual(before);
  expect((await pool.query('SELECT count(*)::int n FROM artifacts WHERE run_id=$1',[a.id])).rows[0].n).toBe(3);
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1',[f.a.id])).rows[0].state).toBe('accepted');
});

it('refuses historical classification with an unresolved sibling, forged artifact metadata or an unproven frozen replacement',async()=>{
  const f=await prepared(),r=await run(f.consumer),t=await task(f.consumer),deps=await acceptedDependencyArtifacts(f.consumer);
  await tx(async db=>{
    await lock(db);
    const intercepted={query:(async(sql:any,args:any)=>{
      const value=await db.query(sql,args);
      return String(sql).includes('SELECT id,producer_task_id,producer_run_id FROM delivery_consumptions')?
        {...value,rows:[...value.rows,{id:randomUUID(),producer_task_id:f.a.id,producer_run_id:randomUUID()}]}:value;
    }) as typeof db.query};
    expect((await resolvedInputSelection(intercepted,r,t,deps)).selection).toBeNull();
    const forged=deps.map(a=>a.task_id===f.a.id&&a.name==='implementation.patch'?{...a,digest:'f'.repeat(64)}:a);
    await expect(resolvedInputSelection(db,r,t,forged)).rejects.toMatchObject({code:'INPUT_MANIFEST_CHANGED',inputFailure:{subcode:'HISTORICAL_DEPENDENCY_UNVERIFIED'}});
    const selected=await resolvedInputSelection(db,r,t,deps);
    expect(selected.selection).not.toBeNull();
    expect(()=>assertResolvedInputSelection(selected.selection,{effectiveSources:{runId:r.id,runFence:r.fence,bindings:[]}})).toThrow();
  });
});
