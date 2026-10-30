import {beforeAll,beforeEach,afterEach,afterAll,it,expect} from 'vitest';
import {createHash,randomUUID} from 'node:crypto';
import type {PoolClient} from 'pg';
import {migrate,pool} from '../server/db.js';
import {seedRegistry} from '../server/registry.js';
import {captureSourceReuseAuthority,recaptureSourceReuseAuthority,completeSourceReuseCapture,
 type SourceReuseScope} from '../server/planning-source-reuse-adapter.js';

const now=Date.parse('2026-10-30T03:00:00Z'),sha=(v:string|Buffer)=>createHash('sha256').update(v).digest('hex');
const inventory=(v:string)=>[{path:'src/code.ts',sha256:sha(v)}];
const sourceDigest=(f:ReturnType<typeof inventory>)=>sha(JSON.stringify(f.map(x=>[x.path,x.sha256])));
const clone=<T>(v:T):T=>structuredClone(v);
let db:PoolClient;
beforeAll(async()=>{await migrate();await seedRegistry();});
beforeEach(async()=>{db=await pool.connect();await db.query('BEGIN');await db.query('TRUNCATE goals CASCADE');});
afterEach(async()=>{await db.query('ROLLBACK');db.release();});
afterAll(async()=>{await pool.end();});

async function fixture(){
 const goalId=randomUUID(),scope:SourceReuseScope={goalId,revision:2,baseWorkspace:'example/logical'};
 await db.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1::uuid,'Reuse','Fixture','example',$1::text)",[goalId]);
 await db.query("INSERT INTO objectives(id,request,request_hash,mode,status,revision) VALUES($1,'{\"project\":\"example\"}',$2,'execute','executing',2)",[goalId,sha('request')]);
 const baseFiles=inventory('base'),baseDigest=sourceDigest(baseFiles);
 const baseMarker=Buffer.from(JSON.stringify({workspace:'example/prepared-root',sourceWorkspace:scope.baseWorkspace,baseDigest,sourceSnapshotDigest:sha('original-generation')}));
 const baseInspection={version:1,workspace:'example/prepared-root',generation:{workspace:'example/prepared-root',sourceWorkspace:scope.baseWorkspace,
   baseDigest,snapshotDigest:sha(baseMarker),sourceSnapshotDigest:sha('original-generation')},markerBase64:baseMarker.toString('base64'),
   legacyParentGeneration:false,candidate:{baseDigest,candidateDigest:baseDigest,files:baseFiles},checkedAt:new Date(now).toISOString()};
 const runInspections=new Map<string,any>(),artifactBytes=new Map<string,Buffer>();
 const add=async(parent:any=null,text='change',readOnly=false)=>{
  const taskId=randomUUID(),runId=randomUUID(),fence=randomUUID(),contextId=randomUUID();
  const workspace='example/w-'+taskId.replaceAll('-',''),sourceWorkspace=parent?.workspace??baseInspection.workspace;
  const before=parent?.candidate??baseInspection.candidate,files=inventory(text),candidateDigest=sourceDigest(files);
  const execution={kind:'harness',workspace,sourceWorkspace,baseWorkspace:scope.baseWorkspace,readOnlySource:readOnly};
  await db.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,attempt_count,plan_revision)
    VALUES($1::uuid,$2,$1::text,'Task','Fixture','qa.backend','accepted',$3,'[]',$4,1,2)`,[taskId,goalId,readOnly?'verify':'work',JSON.stringify(execution)]);
  if(parent)await db.query('INSERT INTO dependencies(task_id,depends_on) VALUES($1,$2)',[taskId,parent.taskId]);
  await db.query("INSERT INTO runs(id,task_id,attempt,state,fence,runtime,agent_version) VALUES($1,$2,1,'submitted',$3,'harness',1)",[runId,taskId,fence]);
  const refs=(parent?.refs??[]).map((r:any)=>({id:r.id,name:r.name,digest:r.digest,task_id:parent.taskId}));
  const manifest={runIdentity:{goalId,taskId,runId,runFence:fence},dependencyArtifacts:refs};
  await db.query('INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,$3,$4)',[contextId,runId,JSON.stringify(manifest),'fixture context']);
  await db.query('UPDATE runs SET context_id=$2 WHERE id=$1',[runId,contextId]);
  const candidate={baseDigest:before.candidateDigest,candidateDigest,files};
  const patch=before.candidateDigest===candidateDigest?Buffer.alloc(0):Buffer.from('--- a/src/code.ts\n+++ b/src/code.ts\n@@ -1 +1 @@\n-before\n+after\n');
  const ownRefs=[];
  for(const a of [{name:'candidate.json',bytes:Buffer.from(JSON.stringify(candidate)),mime:'application/json'},
    {name:'changes.patch',bytes:patch,mime:'text/plain'}]){
   const ref={id:randomUUID(),runId,name:a.name,digest:sha(a.bytes),size:a.bytes.length};ownRefs.push(ref);artifactBytes.set(ref.id,a.bytes);
   await db.query('INSERT INTO artifacts(id,run_id,name,mime,digest,size) VALUES($1,$2,$3,$4,$5,$6)',[ref.id,runId,a.name,a.mime,ref.digest,ref.size]);
  }
  await db.query("INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) VALUES($1,$2,$3,true,'[]','fixture')",[randomUUID(),taskId,runId]);
  const sourceSnapshotDigest=parent?.generation.snapshotDigest??baseInspection.generation.snapshotDigest;
  const marker=Buffer.from(JSON.stringify({workspace,sourceWorkspace,baseDigest:before.candidateDigest,sourceSnapshotDigest}));
  const generation={workspace,sourceWorkspace,baseDigest:before.candidateDigest,snapshotDigest:sha(marker),sourceSnapshotDigest};
  const checkedAt=new Date(now).toISOString();
  runInspections.set(runId,{version:1,runId,workspace,state:{runId,workspace,runtime:'harness',state:'completed',runFence:fence,stopped:true,
    artifacts:ownRefs.map(a=>({name:a.name,digest:a.digest,mime:a.name==='candidate.json'?'application/json':'text/plain'}))},
    admission:{authority:'runtime_operation_policy',goalId,taskId,runId,runFence:fence,contextVersion:contextId,workspace:'/workspace',
      sourceSnapshotDigest:generation.snapshotDigest,policyDigest:sha('protected-policy-'+runId)},generation,
    markerBase64:marker.toString('base64'),legacyParentGeneration:false,candidate,
    process:{unit:'agentos-run-'+runId,checkedAt,stopped:true,loadState:'loaded',activeState:'inactive',subState:'dead',mainPid:0,cgroupExists:false,populated:false},checkedAt});
  return {taskId,runId,fence,contextId,workspace,execution,candidate,generation,refs:ownRefs};
 };
 const capture=async()=>{const result=await captureSourceReuseAuthority(db,scope);expect(result.eligible,JSON.stringify(result)).toBe(true);
  if(!result.eligible)throw Error(result.reason);return result.capture;};
 const observations=()=>({baseInspection,runInspections,artifactBytes});
 return {goalId,scope,baseInspection,runInspections,artifactBytes,add,capture,observations};
}

it('captures a complete DB chain, enriches frozen refs and proves the actual prepared root without advancing acceptance',async()=>{
 const f=await fixture(),p=await f.add(),q=await f.add(p,'change',true),c=await f.capture();
 expect(c.rootWorkspace).toBe('example/prepared-root');expect(c.scope.baseWorkspace).toBe('example/logical');
 expect(c.contributors.find(x=>x.taskId===q.taskId)?.frozenParentInputs).toEqual(expect.arrayContaining(p.refs));
 expect(c.contributors.every(x=>!('generation' in x))).toBe(true);
 const result=completeSourceReuseCapture(c,f.observations(),now);expect(result.eligible,JSON.stringify(result)).toBe(true);
 if(!result.eligible)throw Error(result.reason);
 expect(result.proof.selectedSource.taskId).toBe(p.taskId);expect(result.proof.unchangedWitnessTaskIds).toEqual([q.taskId]);
 expect(result.proof).toMatchObject({acceptance:false,finalVerificationRequired:true});
 expect(await recaptureSourceReuseAuthority(db,c)).toEqual({eligible:true,capture:c});
 expect((await db.query('SELECT state FROM goals WHERE id=$1',[f.goalId])).rows[0].state).toBe('active');
 expect((await db.query('SELECT count(*)::int n FROM acceptances')).rows[0].n).toBe(2);
});

it('includes every accepted parallel branch and falls back rather than selecting one or claiming merge provenance',async()=>{
 const f=await fixture(),a=await f.add(null,'a'),b=await f.add(null,'b'),c=await f.capture();
 expect(c.catalogTaskIds.sort()).toEqual([a.taskId,b.taskId].sort());
 expect(completeSourceReuseCapture(c,f.observations(),now)).toEqual({eligible:false,reason:'SOURCE_BRANCHES'});
});

it('detects a newly accepted contribution at recapture',async()=>{
 const f=await fixture();await f.add();const c=await f.capture();await f.add(null,'another');
 expect((await recaptureSourceReuseAuthority(db,c)).eligible).toBe(false);
});

it.each(['fence','context','execution','attempt','acceptance','objective'] as const)('rejects changed %s authority after capture',async mutation=>{
 const f=await fixture(),p=await f.add(),c=await f.capture();
 if(mutation==='fence')await db.query('UPDATE runs SET fence=$2 WHERE id=$1',[p.runId,randomUUID()]);
 if(mutation==='context')await db.query('UPDATE runs SET context_id=$2 WHERE id=$1',[p.runId,randomUUID()]);
 if(mutation==='execution')await db.query("UPDATE tasks SET execution=execution||'{\"readOnlySource\":true}' WHERE id=$1",[p.taskId]);
 if(mutation==='attempt')await db.query('UPDATE tasks SET attempt_count=2 WHERE id=$1',[p.taskId]);
 if(mutation==='acceptance')await db.query("INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer,created_at) VALUES($1,$2,$3,false,'[]','fixture',now()+interval '1 second')",[randomUUID(),p.taskId,p.runId]);
 if(mutation==='objective')await db.query("UPDATE objectives SET request=request||'{\"newInstruction\":true}' WHERE id=$1",[f.goalId]);
 expect((await recaptureSourceReuseAuthority(db,c)).eligible).toBe(false);
});

it('fails closed on missing projection instead of omitting the accepted task',async()=>{
 const f=await fixture(),p=await f.add();await db.query('DELETE FROM task_execution_metadata WHERE task_id=$1',[p.taskId]);
 expect(await captureSourceReuseAuthority(db,f.scope)).toEqual({eligible:false,reason:'EXECUTION_METADATA_MISSING'});
});

it('does not accept a harness run whose actual execution kind is different',async()=>{
 const f=await fixture(),p=await f.add();await db.query("UPDATE tasks SET execution=execution||'{\"kind\":\"opencode\"}' WHERE id=$1",[p.taskId]);
 expect(await captureSourceReuseAuthority(db,f.scope)).toEqual({eligible:false,reason:'IDENTITY_NOT_CURRENT'});
});

it('rejects retained ownership even when the accepted run is already submitted',async()=>{
 const f=await fixture(),p=await f.add();await db.query('INSERT INTO leases(resource,run_id) VALUES($1,$2)',['workspace:'+p.workspace,p.runId]);
 expect(await captureSourceReuseAuthority(db,f.scope)).toEqual({eligible:false,reason:'LIVE_OR_UNCERTAIN_SOURCE'});
});

it('rejects a live actor owning the base while allowing an unrelated analytical coordinator without a workspace',async()=>{
 const f=await fixture();await f.add();const t=randomUUID(),r=randomUUID();
 await db.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,attempt_count,plan_revision)
  VALUES($1::uuid,$2,$1::text,'Coordinator','Fixture','qa.backend','running','coordinate','[]','{"kind":"analysis"}',1,2)`,[t,f.goalId]);
 await db.query("INSERT INTO runs(id,task_id,attempt,state,fence,runtime,agent_version) VALUES($1,$2,1,'running',$3,'harness',1)",[r,t,randomUUID()]);
 expect((await captureSourceReuseAuthority(db,f.scope)).eligible).toBe(true);
 await db.query('UPDATE tasks SET execution=$2 WHERE id=$1',[t,JSON.stringify({kind:'harness',workspace:f.baseInspection.workspace})]);
 expect(await captureSourceReuseAuthority(db,f.scope)).toEqual({eligible:false,reason:'BASE_WORKSPACE_OWNED'});
});

it('rejects a frozen dependency reference whose actual owner was not the claimed parent',async()=>{
 const f=await fixture(),p=await f.add(),q=await f.add(p,'change',true);
 await db.query("UPDATE contexts SET manifest=jsonb_set(manifest,'{dependencyArtifacts,0,task_id}',to_jsonb($2::text)) WHERE id=$1",[q.contextId,randomUUID()]);
 expect(await captureSourceReuseAuthority(db,f.scope)).toEqual({eligible:false,reason:'FROZEN_INPUT_CHANGED'});
});

it('does not trust a truncated accepted catalogue',async()=>{
 const f=await fixture();for(let i=0;i<65;i++)await f.add(null,'change-'+i);
 expect(await captureSourceReuseAuthority(db,f.scope)).toEqual({eligible:false,reason:'COHORT_BOUND'});
});

it('requires the immutable admission snapshot trigger for the fast recapture path',async()=>{
 const f=await fixture();await f.add();const c=await f.capture();
 await db.query('ALTER TABLE runs DISABLE TRIGGER agentos_instruction_snapshot');
 expect(await recaptureSourceReuseAuthority(db,c)).toEqual({eligible:false,reason:'IMMUTABLE_SNAPSHOT_REQUIRED'});
});

it.each(['policyContext','policyMarker','stateArtifact','currentCandidate','staleStop','legacyGeneration','candidateBytes','baseDigest'] as const)
 ('rejects unbound or changed trusted inspection evidence: %s',async mutation=>{
  const f=await fixture(),p=await f.add(),c=await f.capture(),observed=f.observations(),v=observed.runInspections.get(p.runId);
  if(mutation==='policyContext')v.admission.contextVersion=randomUUID();
  if(mutation==='policyMarker')v.admission.sourceSnapshotDigest=sha('wrong-marker');
  if(mutation==='stateArtifact')v.state.artifacts[0].digest=sha('worker-authored-artifact');
  if(mutation==='currentCandidate')v.candidate.candidateDigest=sha('changed-workspace');
  if(mutation==='staleStop')v.process.checkedAt=v.checkedAt=new Date(now-60001).toISOString();
  if(mutation==='legacyGeneration'){
   const marker=JSON.parse(Buffer.from(v.markerBase64,'base64').toString());delete marker.sourceSnapshotDigest;
   const raw=Buffer.from(JSON.stringify(marker));v.markerBase64=raw.toString('base64');v.generation.snapshotDigest=sha(raw);
   v.generation.sourceSnapshotDigest=null;v.legacyParentGeneration=true;v.admission.sourceSnapshotDigest=sha(raw);
  }
  if(mutation==='candidateBytes')observed.artifactBytes.set(p.refs[0].id,Buffer.from('tampered'));
  if(mutation==='baseDigest')observed.baseInspection.candidate.baseDigest=sha('not-baseline');
  expect(completeSourceReuseCapture(c,observed,now).eligible).toBe(false);
 });

it('keeps a persisted capture tamper evident without treating its hash as external authentication',async()=>{
 const f=await fixture();await f.add();const c=await f.capture(),changed=clone(c);changed.rootWorkspace='example/arbitrary';
 expect(await recaptureSourceReuseAuthority(db,changed)).toEqual({eligible:false,reason:'CAPTURE_CHANGED'});
 expect(completeSourceReuseCapture(changed,f.observations(),now)).toEqual({eligible:false,reason:'CAPTURE_CHANGED'});
});
