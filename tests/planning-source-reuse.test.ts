import {it,expect} from 'vitest';
import {createHash,randomUUID} from 'node:crypto';
import {proveAcceptedSourceChain,sourceReuseAuthorityMatches,sourceReuseAuthorityProjection,sourceReusePreparation,
  type SourceReuseSnapshot,type SourceReuseContributor,type SourceReuseEvidence} from '../server/planning-source-reuse.js';

const now=100000,hash=(v:string|Buffer)=>createHash('sha256').update(v).digest('hex');
const files=(value:string)=>[{path:'src/code.ts',sha256:hash(value)}];
const sourceDigest=(f:ReturnType<typeof files>)=>hash(JSON.stringify([...f].sort((a,b)=>a.path.localeCompare(b.path)).map(x=>[x.path,x.sha256])));
const clone=<T>(v:T):T=>structuredClone(v);
function fixture(){
  const baseFiles=files('base'),baseMarker=Buffer.from(JSON.stringify({workspace:'example/base',baseDigest:sourceDigest(baseFiles)}));
  const snapshot:SourceReuseSnapshot={version:1,scope:{goalId:randomUUID(),project:'example',revision:2,baseWorkspace:'example/original'},
    complete:true,totalContributors:0,catalogTaskIds:[],base:{workspace:'example/base',sourceDigest:sourceDigest(baseFiles),snapshotDigest:hash(baseMarker),files:baseFiles},contributors:[]};
  const artifacts=new Map<string,Buffer>(),markers=new Map<string,Buffer>(),stops=new Map<string,any>();
  const evidence:SourceReuseEvidence={baseMarker,artifacts,markers,stops};
  const manifest=(c:SourceReuseContributor)=>JSON.parse(artifacts.get(c.artifacts.find(a=>a.name==='candidate.json')!.id)!.toString());
  const add=(parent:SourceReuseContributor|null,after:ReturnType<typeof files>,readOnly=false)=>{
    const taskId=randomUUID(),runId=randomUUID(),fence=randomUUID(),contextId=randomUUID();
    const before=parent?manifest(parent):{candidateDigest:snapshot.base.sourceDigest,files:baseFiles};
    const target='example/source-'+snapshot.contributors.length,sourceWorkspace=parent?.execution.workspace??snapshot.base.workspace;
    const candidate=Buffer.from(JSON.stringify({baseDigest:before.candidateDigest,candidateDigest:sourceDigest(after),files:after}));
    const patch=before.candidateDigest===sourceDigest(after)?Buffer.alloc(0):Buffer.from('--- a/src/code.ts\n+++ b/src/code.ts\n@@ -1 +1 @@\n-before\n+after\n');
    const refs=([{name:'candidate.json',bytes:candidate},{name:'changes.patch',bytes:patch}] as const).map(a=>{
      const ref={id:randomUUID(),runId,name:a.name,digest:hash(a.bytes),size:a.bytes.length};artifacts.set(ref.id,a.bytes);return ref;});
    const parentGeneration=parent?.generation.snapshotDigest??snapshot.base.snapshotDigest;
    const marker=Buffer.from(JSON.stringify({workspace:target,sourceWorkspace,baseDigest:before.candidateDigest,sourceSnapshotDigest:parentGeneration,createdAt:'2026-01-01T00:00:00Z'}));
    markers.set(target,marker);
    const execution={workspace:target,sourceWorkspace,baseWorkspace:snapshot.scope.baseWorkspace,readOnlySource:readOnly,writePaths:null};
    const c:SourceReuseContributor={taskId,goalId:snapshot.scope.goalId,project:'example',revision:2,taskState:'accepted',attempt:1,currentAttempt:1,
      runId,latestRunId:runId,runState:'submitted',fence,contextId,instructionSnapshotDigest:hash('frozen-'+taskId),
      latestAcceptance:{id:randomUUID(),taskId,runId,passed:true},execution,frozenExecution:clone(execution),
      dependencyTaskIds:parent?[parent.taskId]:[],frozenParentInputs:parent?clone(parent.artifacts):[],artifacts:refs,artifactsComplete:true,
      generation:{workspace:target,sourceWorkspace,baseDigest:before.candidateDigest,snapshotDigest:hash(marker),sourceSnapshotDigest:parentGeneration},
      liveRunCount:0,activeLeaseCount:0,ownershipUncertain:false};
    stops.set(runId,{taskId,runId,fence,contextId,workspace:target,candidateDigest:sourceDigest(after),snapshotDigest:hash(marker),checkedAt:now,
      stopped:true,mainPid:0,cgroupPopulated:false,liveWorkspaceActors:0,ownershipUncertain:false});
    snapshot.contributors.push(c);snapshot.catalogTaskIds.push(taskId);snapshot.totalContributors++;
    return c;
  };
  const setArtifact=(c:SourceReuseContributor,name:'candidate.json'|'changes.patch',bytes:Buffer)=>{
    const ref=c.artifacts.find(a=>a.name===name)!;ref.digest=hash(bytes);ref.size=bytes.length;artifacts.set(ref.id,bytes);};
  const prove=()=>proveAcceptedSourceChain(snapshot,evidence,now);
  return {snapshot,evidence,artifacts,markers,stops,add,setArtifact,manifest,prove};
}
const accepted=(f:ReturnType<typeof fixture>)=>{const r=f.prove();expect(r.eligible).toBe(true);if(!r.eligible)throw Error(r.reason);return r.proof;};

it('selects the producer with a generation-bound unchanged witness without accepting any requirement',()=>{
  const f=fixture(),producer=f.add(null,files('change')),witness=f.add(producer,files('change'),true);
  const proof=accepted(f);
  expect(proof.selectedSource.taskId).toBe(producer.taskId);
  expect(proof.incorporatedContributors.map(c=>c.taskId).sort()).toEqual([producer.taskId,witness.taskId].sort());
  expect(proof.unchangedWitnessTaskIds).toEqual([witness.taskId]);
  expect(proof).toMatchObject({acceptance:false,finalVerificationRequired:true,kind:'repair_source_reuse'});
  expect(proof).not.toHaveProperty('incrementalAdvance');expect(proof).not.toHaveProperty('completedRequirementIds');
  const preparationIdentity=randomUUID();
  expect(sourceReusePreparation(proof,preparationIdentity)).toEqual({sourceWorkspace:producer.execution.workspace,preparationIdentity,
    expectedSourceDigest:f.manifest(producer).candidateDigest,expectedSourceSnapshotDigest:producer.generation.snapshotDigest});
  expect(sourceReuseAuthorityMatches(proof,sourceReuseAuthorityProjection(f.snapshot))).toBe(true);
  expect(sourceReuseAuthorityProjection(f.snapshot).base).not.toHaveProperty('files');
});

it('follows a producer, unchanged copy, second producer and final unchanged copy regardless of catalog order',()=>{
  const f=fixture(),a=f.add(null,files('a')),q=f.add(a,files('a'),true),b=f.add(q,files('b')),last=f.add(b,files('b'),true);
  const proof=accepted(f);expect(proof.selectedSource.taskId).toBe(b.taskId);
  expect(proof.incorporatedContributors).toHaveLength(4);expect(proof.unchangedWitnessTaskIds.sort()).toEqual([q.taskId,last.taskId].sort());
  f.snapshot.contributors.reverse();f.snapshot.catalogTaskIds.reverse();
  expect(accepted(f)).toEqual(proof);
});

it('retains the complete accepted artifact catalog including independent QA evidence without parsing its contents',()=>{
  const f=fixture(),p=f.add(null,files('a')),q=f.add(p,files('a'),true);
  const report={id:randomUUID(),runId:q.runId,name:'verification.json',digest:hash('private QA bytes'),size:16};q.artifacts.push(report);
  const proof=accepted(f);expect(proof.incorporatedContributors.find(c=>c.taskId===q.taskId)!.artifactCatalog).toContainEqual(report);
  q.artifacts[2].digest=hash('changed report');expect(sourceReuseAuthorityMatches(proof,sourceReuseAuthorityProjection(f.snapshot))).toBe(false);
  q.artifactsComplete=false;expect(f.prove()).toEqual({eligible:false,reason:'INCOMPLETE_CATALOG'});
});

it('allows independent unchanged witnesses of the same exact final source generation',()=>{
  const f=fixture(),p=f.add(null,files('a')),q1=f.add(p,files('a'),true),q2=f.add(p,files('a'),true);
  const proof=accepted(f);expect(proof.selectedSource.taskId).toBe(p.taskId);
  expect(proof.unchangedWitnessTaskIds.sort()).toEqual([q1.taskId,q2.taskId].sort());
});

it('falls back for independent changed branches even if a DAG dependency exists',()=>{
  const f=fixture(),a=f.add(null,files('a')),b=f.add(null,files('b'));
  b.dependencyTaskIds.push(a.taskId);b.frozenParentInputs.push(...clone(a.artifacts));
  expect(f.prove()).toEqual({eligible:false,reason:'SOURCE_BRANCHES'});
});

it('does not silently omit accepted contributions or trust a truncated catalog',()=>{
  const f=fixture();f.add(null,files('a'));const b=f.add(null,files('b'));
  f.snapshot.contributors=f.snapshot.contributors.filter(c=>c.taskId!==b.taskId);
  expect(f.prove()).toEqual({eligible:false,reason:'INCOMPLETE_CATALOG'});
  f.snapshot.totalContributors=1;f.snapshot.catalogTaskIds=f.snapshot.contributors.map(c=>c.taskId);f.snapshot.complete=false;
  expect(f.prove()).toEqual({eligible:false,reason:'INCOMPLETE_CATALOG'});
});

it('preserves independent unchanged artifacts via explicit fallback instead of claiming they witnessed the selected source',()=>{
  const f=fixture();f.add(null,files('a'));f.add(null,files('base'),true);
  expect(f.prove()).toEqual({eligible:false,reason:'CONTRIBUTOR_NOT_COVERED'});
});

it.each(['candidate.json','changes.patch'] as const)('rejects tampered canonical %s bytes',name=>{
  const f=fixture(),c=f.add(null,files('a')),ref=c.artifacts.find(a=>a.name===name)!;
  f.artifacts.set(ref.id,Buffer.concat([f.artifacts.get(ref.id)!,Buffer.from('tampered')]));
  expect(f.prove()).toEqual({eligible:false,reason:'ARTIFACT_BYTES'});
});

it('recomputes candidate file inventory instead of trusting a declared digest',()=>{
  const f=fixture(),c=f.add(null,files('a')),m=f.manifest(c);m.candidateDigest=hash('fabricated');
  f.setArtifact(c,'candidate.json',Buffer.from(JSON.stringify(m)));
  expect(f.prove()).toEqual({eligible:false,reason:'CANDIDATE_INVALID'});
});

it('rejects cross-run and duplicate canonical artifact identities',()=>{
  const f=fixture(),c=f.add(null,files('a'));c.artifacts[0].runId=randomUUID();
  expect(f.prove()).toEqual({eligible:false,reason:'ARTIFACT_IDENTITY'});
  c.artifacts[0].runId=c.runId;c.artifacts[1].id=c.artifacts[0].id;
  expect(f.prove()).toEqual({eligible:false,reason:'ARTIFACT_IDENTITY'});
});

it.each(['fence','contextId'] as const)('binds physical stop to the exact %s',key=>{
  const f=fixture(),c=f.add(null,files('a'));c[key]=randomUUID();
  expect(f.prove()).toEqual({eligible:false,reason:'STOP_UNCONFIRMED'});
});

it('rejects a later attempt and a newer failed acceptance',()=>{
  const f=fixture(),c=f.add(null,files('a'));c.currentAttempt++;
  expect(f.prove()).toEqual({eligible:false,reason:'IDENTITY_NOT_CURRENT'});
  c.currentAttempt=c.attempt;c.latestAcceptance.passed=false;
  expect(f.prove()).toEqual({eligible:false,reason:'IDENTITY_NOT_CURRENT'});
});

it('requires actual source edge, frozen artifact inputs and the same generation rather than digest alone',()=>{
  const f=fixture(),a=f.add(null,files('a')),q=f.add(a,files('a'),true);
  q.frozenParentInputs=q.frozenParentInputs.filter(x=>x.name!=='changes.patch');
  expect(f.prove()).toEqual({eligible:false,reason:'SOURCE_EDGE_UNPROVEN'});
  q.frozenParentInputs=clone(a.artifacts);q.dependencyTaskIds=[];
  expect(f.prove()).toEqual({eligible:false,reason:'SOURCE_EDGE_UNPROVEN'});
});

it('rejects a reused parent path with identical files but a different protected marker generation',()=>{
  const f=fixture(),a=f.add(null,files('a'));f.add(a,files('a'),true);
  const marker=JSON.parse(f.markers.get(a.execution.workspace)!.toString());marker.createdAt='2026-01-02T00:00:00Z';
  const bytes=Buffer.from(JSON.stringify(marker));f.markers.set(a.execution.workspace,bytes);
  a.generation.snapshotDigest=hash(bytes);f.stops.get(a.runId).snapshotDigest=hash(bytes);
  expect(f.prove()).toEqual({eligible:false,reason:'SOURCE_EDGE_UNPROVEN'});
});

it('never invents missing legacy generation binding',()=>{
  const f=fixture(),a=f.add(null,files('a'));a.generation.sourceSnapshotDigest=null;
  expect(f.prove()).toEqual({eligible:false,reason:'GENERATION_REQUIRED'});
});

it('rejects readonly writes and requires an exactly empty canonical patch for unchanged sources',()=>{
  const f=fixture(),a=f.add(null,files('a')),q=f.add(a,files('different'),true);
  expect(f.prove()).toEqual({eligible:false,reason:'READ_ONLY_SOURCE_CHANGED'});
  q.execution.readOnlySource=false;q.frozenExecution.readOnlySource=false;f.setArtifact(q,'changes.patch',Buffer.alloc(0));
  expect(f.prove()).toEqual({eligible:false,reason:'PATCH_STATE_MISMATCH'});
  const g=fixture(),p=g.add(null,files('a')),w=g.add(p,files('a'),true);g.setArtifact(w,'changes.patch',Buffer.from('\n'));
  expect(g.prove()).toEqual({eligible:false,reason:'PATCH_STATE_MISMATCH'});
});

it('uses frozen write scopes and never confuses an accepted report with source write authority',()=>{
  const f=fixture(),c=f.add(null,files('a'));c.execution.writePaths=['docs'];c.frozenExecution.writePaths=['docs'];
  expect(f.prove()).toEqual({eligible:false,reason:'WRITE_SCOPE_VIOLATION'});
});

it('accepts canonical directory scopes and readonly attempt-local QA paths without adding them to source',()=>{
  const f=fixture(),p=f.add(null,files('a')),q=f.add(p,files('a'),true);
  p.execution.writePaths=['src/'];p.frozenExecution.writePaths=['src/'];
  q.execution.writePaths=['evidence/','data/','qa-protocol/'];q.frozenExecution.writePaths=clone(q.execution.writePaths);
  expect(accepted(f).selectedSource.taskId).toBe(p.taskId);
  expect(f.manifest(q).files.map((x:{path:string})=>x.path)).toEqual(['src/code.ts']);
  p.execution.writePaths=[];p.frozenExecution.writePaths=[];
  expect(accepted(f).selectedSource.taskId).toBe(p.taskId);
});

it.each(['../src/','/src/','src//','src/../','src\\code','./src/','.git/','assets/'])('does not normalize unsafe scope %s into authority',scope=>{
  const f=fixture(),p=f.add(null,files('a'));p.execution.writePaths=[scope];p.frozenExecution.writePaths=[scope];
  expect(f.prove()).toEqual({eligible:false,reason:'INVALID_SNAPSHOT'});
});

it.each(['liveRunCount','activeLeaseCount'] as const)('rejects occupied source with %s',key=>{
  const f=fixture(),c=f.add(null,files('a'));c[key]=1;
  expect(f.prove()).toEqual({eligible:false,reason:'LIVE_OR_UNCERTAIN_SOURCE'});
});

it.each([{checkedAt:now-60001},{checkedAt:now+1},{mainPid:7},{cgroupPopulated:true},{liveWorkspaceActors:1},{ownershipUncertain:true}])('requires fresh physical quiescence %j',change=>{
  const f=fixture(),c=f.add(null,files('a'));Object.assign(f.stops.get(c.runId),change);
  expect(f.prove()).toEqual({eligible:false,reason:'STOP_UNCONFIRMED'});
});

it('detects authority changes after proof creation without rereading artifact bodies',()=>{
  const f=fixture(),c=f.add(null,files('a')),proof=accepted(f);
  for(const mutate of [
    (s:SourceReuseSnapshot)=>{s.contributors[0].latestAcceptance.id=randomUUID();},
    (s:SourceReuseSnapshot)=>{s.contributors[0].instructionSnapshotDigest=hash('changed');},
    (s:SourceReuseSnapshot)=>{s.contributors[0].contextId=randomUUID();},
    (s:SourceReuseSnapshot)=>{s.contributors[0].fence=randomUUID();},
    (s:SourceReuseSnapshot)=>{s.contributors[0].artifacts[0].digest=hash('changed');},
    (s:SourceReuseSnapshot)=>{s.contributors[0].generation.snapshotDigest=hash('new-generation');},
    (s:SourceReuseSnapshot)=>{s.contributors[0].execution.readOnlySource=true;},
    (s:SourceReuseSnapshot)=>{s.catalogTaskIds.push(randomUUID());},
  ]){const current=clone(f.snapshot);mutate(current);expect(sourceReuseAuthorityMatches(proof,sourceReuseAuthorityProjection(current))).toBe(false);}
  expect(c.taskState).toBe('accepted');expect(sourceReuseAuthorityMatches({...proof,proofDigest:hash('forged')},sourceReuseAuthorityProjection(f.snapshot))).toBe(false);
});

it('rejects empty or unchanged-only cohorts and leaves callers on the existing artifact path',()=>{
  const f=fixture();expect(f.prove()).toEqual({eligible:false,reason:'NO_CONTRIBUTORS'});
  f.add(null,files('base'),true);expect(f.prove()).toEqual({eligible:false,reason:'NO_NEW_SOURCE'});
});
