import {createHash} from 'node:crypto';
import {z} from 'zod';
import {runtimeCandidateDigest} from './operation-progress.js';

const sha=(value:Buffer|string)=>createHash('sha256').update(value).digest('hex');
const canonical=(v:any):any=>Array.isArray(v)?v.map(canonical):v&&typeof v==='object'
  ?Object.fromEntries(Object.keys(v).sort().map(k=>[k,canonical(v[k])])):v;
const digest=(v:unknown)=>sha(JSON.stringify(canonical(v)));
const hash=z.string().regex(/^[a-f0-9]{64}$/),id=z.uuid();
const workspace=z.string().regex(/^[a-z0-9_-]+\/[a-z0-9_-]+$/);
const forbidden=new Set(['.git','assets','node_modules','evidence','data','.agentos-snapshot.json','.ssh','.aws','.codex','.claude','.local-test','test-results','playwright-report','__pycache__']);
const sourcePath=(p:string)=>p.length>0&&p.length<=500&&!p.includes('\\')&&!p.includes('\0')&&
  !p.startsWith('/')&&!p.endsWith('.log')&&!p.endsWith('.pyc')&&
  p.split('/').every(x=>x!==''&&x!=='.'&&x!=='..'&&!forbidden.has(x)&&!x.startsWith('.env'));
const file=z.object({path:z.string().refine(sourcePath),sha256:hash}).strict();
// A write scope is not a source inventory path. Existing plans allow a trailing
// slash and attempt-local evidence/data scopes, which never enter the inventory.
const writeScope=z.string().min(1).max(200)
  .regex(/^(?![/\\])(?!.*\.\.)(?!.*\\)(?!\.git(?:\/|$))(?!node_modules(?:\/|$))(?!assets(?:\/|$))[a-zA-Z0-9._/-]+$/)
  .refine(p=>!p.includes('//')&&!p.split('/').includes('.'));
const artifact=z.object({id,runId:id,name:z.string().regex(/^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$/),digest:hash,
  size:z.number().int().min(0).max(8*1024*1024)}).strict();
const execution=z.object({workspace,sourceWorkspace:workspace,baseWorkspace:workspace,
  readOnlySource:z.boolean(),writePaths:z.array(writeScope).max(128).nullable()}).strict();
const generation=z.object({workspace,sourceWorkspace:workspace,baseDigest:hash,snapshotDigest:hash,
  sourceSnapshotDigest:hash.nullable()}).strict();
const contributor=z.object({taskId:id,goalId:id,project:z.string().regex(/^[a-z0-9_-]+$/),revision:z.number().int().positive(),
  taskState:z.string(),attempt:z.number().int().positive(),currentAttempt:z.number().int().positive(),
  runId:id,latestRunId:id,runState:z.string(),fence:id,contextId:id,instructionSnapshotDigest:hash,
  latestAcceptance:z.object({id,taskId:id,runId:id,passed:z.boolean()}).strict(),
  execution,frozenExecution:execution,dependencyTaskIds:z.array(id).max(128),
  frozenParentInputs:z.array(artifact).max(128),artifacts:z.array(artifact).max(64),artifactsComplete:z.boolean(),generation,
  liveRunCount:z.number().int().nonnegative(),activeLeaseCount:z.number().int().nonnegative(),ownershipUncertain:z.boolean()}).strict();

/** Controller-only projection. `complete` and catalogTaskIds must come from a
 * bounded, complete DB query, never a model's list or a paged tool catalogue.
 * Artifact/marker bytes and physical stops come from trusted runtime adapters.
 * This pure module does not establish that external authority by itself. */
export const sourceReuseSnapshotSchema=z.object({version:z.literal(1),
  scope:z.object({goalId:id,project:z.string().regex(/^[a-z0-9_-]+$/),revision:z.number().int().positive(),baseWorkspace:workspace}).strict(),
  complete:z.boolean(),totalContributors:z.number().int().nonnegative(),catalogTaskIds:z.array(id).max(64),
  base:z.object({workspace,sourceDigest:hash,snapshotDigest:hash,files:z.array(file).max(10000)}).strict(),
  contributors:z.array(contributor).max(64)}).strict();
export type SourceReuseSnapshot=z.infer<typeof sourceReuseSnapshotSchema>;
export const sourceReuseAuthoritySchema=sourceReuseSnapshotSchema.extend({
  base:sourceReuseSnapshotSchema.shape.base.omit({files:true})});
export type SourceReuseAuthority=z.infer<typeof sourceReuseAuthoritySchema>;
export type SourceReuseContributor=SourceReuseSnapshot['contributors'][number];
export type SourceReuseArtifact=z.infer<typeof artifact>;
export type SourceReuseStop={taskId:string;runId:string;fence:string;contextId:string;workspace:string;
  candidateDigest:string;snapshotDigest:string;checkedAt:number;stopped:boolean;mainPid:number;
  cgroupPopulated:boolean;liveWorkspaceActors:number;ownershipUncertain:boolean};
export type SourceReuseEvidence={baseMarker:Buffer;artifacts:ReadonlyMap<string,Buffer>;
  markers:ReadonlyMap<string,Buffer>;stops:ReadonlyMap<string,SourceReuseStop>};
export type SourceReuseSkip='INVALID_SNAPSHOT'|'INCOMPLETE_CATALOG'|'NO_CONTRIBUTORS'|'SCOPE_MISMATCH'|
  'IDENTITY_NOT_CURRENT'|'LIVE_OR_UNCERTAIN_SOURCE'|'ARTIFACT_IDENTITY'|'ARTIFACT_BYTES'|
  'CANDIDATE_INVALID'|'GENERATION_REQUIRED'|'GENERATION_CHANGED'|'STOP_UNCONFIRMED'|
  'SOURCE_EDGE_UNPROVEN'|'SOURCE_CYCLE'|'SOURCE_BRANCHES'|'CONTRIBUTOR_NOT_COVERED'|
  'PATCH_STATE_MISMATCH'|'READ_ONLY_SOURCE_CHANGED'|'WRITE_SCOPE_VIOLATION'|'NO_NEW_SOURCE';
type Identity={taskId:string;runId:string;fence:string;contextId:string;attempt:number;acceptanceId:string;
  workspace:string;candidateDigest:string;snapshotDigest:string;candidateArtifact:SourceReuseArtifact;patchArtifact:SourceReuseArtifact;
  artifactCatalog:SourceReuseArtifact[]};
export type AcceptedSourceChainProof={version:1;kind:'repair_source_reuse';acceptance:false;finalVerificationRequired:true;
  snapshotFingerprint:string;scope:SourceReuseSnapshot['scope'];selectedSource:Identity;
  incorporatedContributors:Identity[];unchangedWitnessTaskIds:string[];proofDigest:string};
export type SourceReuseDecision={eligible:true;proof:AcceptedSourceChainProof}|{eligible:false;reason:SourceReuseSkip};
const skip=(reason:SourceReuseSkip):SourceReuseDecision=>({eligible:false,reason});
const sameSet=(a:string[],b:string[])=>a.length===b.length&&new Set(a).size===a.length&&new Set(b).size===b.length&&a.every(x=>b.includes(x));
const encodedFiles=(files:Array<{path:string;sha256:string}>)=>JSON.stringify(files.map(f=>[f.path,f.sha256])
  .sort((a,b)=>Buffer.compare(Buffer.from(a[0]),Buffer.from(b[0]))))
  .replace(/[\u0080-\uffff]/g,c=>'\\u'+c.charCodeAt(0).toString(16).padStart(4,'0'));
const filesDigest=(files:Array<{path:string;sha256:string}>)=>sha(encodedFiles(files));
const json=(bytes:Buffer)=>{try{return JSON.parse(bytes.toString('utf8'));}catch{return null;}};

/** Small authority fingerprint: no prompts, artifact bodies, or source files.
 * Use it against a freshly captured complete DB/inspection projection. */
export function sourceReuseAuthorityProjection(snapshot:SourceReuseSnapshot):SourceReuseAuthority {
  return {...snapshot,base:{workspace:snapshot.base.workspace,sourceDigest:snapshot.base.sourceDigest,snapshotDigest:snapshot.base.snapshotDigest}};
}
export function sourceReuseSnapshotFingerprint(snapshot:SourceReuseSnapshot|SourceReuseAuthority):string {
  return digest({...snapshot,base:{workspace:snapshot.base.workspace,sourceDigest:snapshot.base.sourceDigest,
    snapshotDigest:snapshot.base.snapshotDigest},catalogTaskIds:[...snapshot.catalogTaskIds].sort(),
    contributors:snapshot.contributors.map(c=>({...c,dependencyTaskIds:[...c.dependencyTaskIds].sort(),
      artifacts:[...c.artifacts].sort((a,b)=>a.id.localeCompare(b.id)),
      frozenParentInputs:[...c.frozenParentInputs].sort((a,b)=>a.id.localeCompare(b.id))})).sort((a,b)=>a.taskId.localeCompare(b.taskId))});
}

/** No filesystem, SQL, patch application or model calls. The selected source
 * must still be copied by digest+generation under workspace-manager locks. */
export function proveAcceptedSourceChain(input:unknown,evidence:SourceReuseEvidence,now=Date.now()):SourceReuseDecision {
  const parsed=sourceReuseSnapshotSchema.safeParse(input);
  if(!parsed.success||!Number.isFinite(now))return skip('INVALID_SNAPSHOT');
  const s=parsed.data,items=s.contributors;
  if(!s.complete||s.totalContributors!==items.length||!sameSet(s.catalogTaskIds,items.map(c=>c.taskId)))return skip('INCOMPLETE_CATALOG');
  if(!items.length)return skip('NO_CONTRIBUTORS');
  if(new Set(items.map(c=>c.runId)).size!==items.length||new Set(items.map(c=>c.latestAcceptance.id)).size!==items.length)return skip('IDENTITY_NOT_CURRENT');
  if(new Set(items.map(c=>c.execution.workspace)).size!==items.length||items.some(c=>c.execution.workspace===s.base.workspace))return skip('SOURCE_EDGE_UNPROVEN');
  if(new Set(s.base.files.map(f=>f.path)).size!==s.base.files.length||filesDigest(s.base.files)!==s.base.sourceDigest)return skip('CANDIDATE_INVALID');
  if(evidence.baseMarker.length>128*1024||sha(evidence.baseMarker)!==s.base.snapshotDigest||json(evidence.baseMarker)?.workspace!==s.base.workspace)return skip('GENERATION_CHANGED');
  const byWorkspace=new Map(items.map(c=>[c.execution.workspace,c]));
  const candidates=new Map<string,{baseDigest:string;candidateDigest:string;files:Array<{path:string;sha256:string}>}>();
  const identities=new Map<string,Identity>(),patches=new Map<string,Buffer>();
  const artifactIds=new Set<string>();let totalBytes=0;
  for(const c of items){
    if(c.goalId!==s.scope.goalId||c.project!==s.scope.project||c.revision!==s.scope.revision||c.execution.baseWorkspace!==s.scope.baseWorkspace||
      [c.execution.workspace,c.execution.sourceWorkspace,s.base.workspace,s.scope.baseWorkspace].some(w=>w.split('/')[0]!==s.scope.project))return skip('SCOPE_MISMATCH');
    if(c.taskState!=='accepted'||c.runState!=='submitted'||c.runId!==c.latestRunId||c.attempt!==c.currentAttempt||
      c.latestAcceptance.taskId!==c.taskId||c.latestAcceptance.runId!==c.runId||!c.latestAcceptance.passed||
      digest(c.execution)!==digest(c.frozenExecution))return skip('IDENTITY_NOT_CURRENT');
    if(c.liveRunCount||c.activeLeaseCount||c.ownershipUncertain)return skip('LIVE_OR_UNCERTAIN_SOURCE');
    const candidate=c.artifacts.filter(a=>a.name==='candidate.json'),patch=c.artifacts.filter(a=>a.name==='changes.patch');
    if(!c.artifactsComplete)return skip('INCOMPLETE_CATALOG');
    if(candidate.length!==1||patch.length!==1||new Set(c.artifacts.map(a=>a.id)).size!==c.artifacts.length||
      c.artifacts.some(a=>a.runId!==c.runId||artifactIds.has(a.id)))return skip('ARTIFACT_IDENTITY');
    c.artifacts.forEach(a=>artifactIds.add(a.id));
    for(const a of [candidate[0],patch[0]]){
      const bytes=evidence.artifacts.get(a.id);totalBytes+=a.size;
      if(totalBytes>32*1024*1024||!Buffer.isBuffer(bytes)||bytes.length!==a.size||sha(bytes)!==a.digest)return skip('ARTIFACT_BYTES');
    }
    const bytes=evidence.artifacts.get(candidate[0].id)!;
    const candidateDigest=runtimeCandidateDigest(bytes,candidate[0].digest),manifest=json(bytes);
    if(!candidateDigest||!Buffer.from(bytes.toString('utf8')).equals(bytes)||!Array.isArray(manifest.files)||manifest.files.length>10000||
      manifest.files.some((f:any)=>!file.safeParse(f).success))return skip('CANDIDATE_INVALID');
    candidates.set(c.taskId,manifest);patches.set(c.taskId,evidence.artifacts.get(patch[0].id)!);
    const g=c.generation,marker=evidence.markers.get(c.execution.workspace);
    if(!g.sourceSnapshotDigest)return skip('GENERATION_REQUIRED');
    if(!Buffer.isBuffer(marker)||marker.length>128*1024||sha(marker)!==g.snapshotDigest)return skip('GENERATION_CHANGED');
    const m=json(marker);
    if(g.workspace!==c.execution.workspace||g.sourceWorkspace!==c.execution.sourceWorkspace||g.baseDigest!==manifest.baseDigest||
      m?.workspace!==g.workspace||m?.sourceWorkspace!==g.sourceWorkspace||m?.baseDigest!==g.baseDigest||m?.sourceSnapshotDigest!==g.sourceSnapshotDigest)return skip('GENERATION_CHANGED');
    const stop=evidence.stops.get(c.runId);
    if(!stop||stop.taskId!==c.taskId||stop.runId!==c.runId||stop.fence!==c.fence||stop.contextId!==c.contextId||
      stop.workspace!==c.execution.workspace||stop.candidateDigest!==candidateDigest||stop.snapshotDigest!==g.snapshotDigest||
      stop.stopped!==true||stop.mainPid!==0||stop.cgroupPopulated!==false||stop.liveWorkspaceActors!==0||stop.ownershipUncertain!==false||
      !Number.isFinite(stop.checkedAt)||stop.checkedAt>now||now-stop.checkedAt>60000)return skip('STOP_UNCONFIRMED');
    identities.set(c.taskId,{taskId:c.taskId,runId:c.runId,fence:c.fence,contextId:c.contextId,attempt:c.attempt,
      acceptanceId:c.latestAcceptance.id,workspace:c.execution.workspace,candidateDigest,snapshotDigest:g.snapshotDigest,
      candidateArtifact:candidate[0],patchArtifact:patch[0],artifactCatalog:[...c.artifacts].sort((a,b)=>a.id.localeCompare(b.id))});
  }
  const parents=new Map<string,string|null>(),changed=new Set<string>();
  for(const c of items){
    const parent=byWorkspace.get(c.execution.sourceWorkspace);
    if(!parent&&c.execution.sourceWorkspace!==s.base.workspace)return skip('SOURCE_EDGE_UNPROVEN');
    const before=parent?candidates.get(parent.taskId)!:{candidateDigest:s.base.sourceDigest,files:s.base.files};
    const after=candidates.get(c.taskId)!;
    if(after.baseDigest!==before.candidateDigest||c.generation.sourceSnapshotDigest!==(parent?parent.generation.snapshotDigest:s.base.snapshotDigest))return skip('SOURCE_EDGE_UNPROVEN');
    if(parent&&(!c.dependencyTaskIds.includes(parent.taskId)||parent.artifacts.filter(a=>['candidate.json','changes.patch'].includes(a.name)).some(a=>
      !c.frozenParentInputs.some(input=>digest(input)===digest(a)))))return skip('SOURCE_EDGE_UNPROVEN');
    parents.set(c.taskId,parent?.taskId??null);
    const beforeFiles=new Map(before.files.map(f=>[f.path,f.sha256])),afterFiles=new Map(after.files.map(f=>[f.path,f.sha256]));
    const paths=[...new Set([...beforeFiles.keys(),...afterFiles.keys()])].filter(p=>beforeFiles.get(p)!==afterFiles.get(p));
    const patch=patches.get(c.taskId)!;
    if((paths.length===0)!==(patch.length===0)||!Buffer.from(patch.toString('utf8')).equals(patch))return skip('PATCH_STATE_MISMATCH');
    if(c.execution.readOnlySource&&paths.length)return skip('READ_ONLY_SOURCE_CHANGED');
    if(c.execution.writePaths?.length&&paths.some(p=>!c.execution.writePaths!.some(scope=>{
      const prefix=scope.replace(/\/$/,'');return p===prefix||p.startsWith(prefix+'/');})))return skip('WRITE_SCOPE_VIOLATION');
    if(paths.length)changed.add(c.taskId);
  }
  const ancestors=new Map<string,Set<string>>();
  for(const c of items){
    const seen=new Set<string>();let id:string|null=c.taskId;
    while(id!==null){if(seen.has(id))return skip('SOURCE_CYCLE');seen.add(id);id=parents.get(id)??null;}
    ancestors.set(c.taskId,seen);
  }
  if(!changed.size)return skip('NO_NEW_SOURCE');
  const sinks=[...changed].filter(id=>[...changed].every(other=>ancestors.get(id)!.has(other)));
  if(sinks.length!==1)return skip('SOURCE_BRANCHES');
  const selected=sinks[0],selectedIdentity=identities.get(selected)!;
  for(const c of items){
    if(ancestors.get(selected)!.has(c.taskId))continue;
    if(changed.has(c.taskId)||!ancestors.get(c.taskId)!.has(selected)||
      candidates.get(c.taskId)!.candidateDigest!==selectedIdentity.candidateDigest)return skip('CONTRIBUTOR_NOT_COVERED');
  }
  const body={version:1 as const,kind:'repair_source_reuse' as const,acceptance:false as const,finalVerificationRequired:true as const,
    snapshotFingerprint:sourceReuseSnapshotFingerprint(s),scope:s.scope,selectedSource:selectedIdentity,
    incorporatedContributors:[...identities.values()].sort((a,b)=>a.taskId.localeCompare(b.taskId)),
    unchangedWitnessTaskIds:items.filter(c=>!changed.has(c.taskId)).map(c=>c.taskId).sort()};
  return {eligible:true,proof:{...body,proofDigest:digest(body)}};
}

/** Metadata-only revalidation hook, not a substitute for physical inspection or
 * the planner's own fence/feedback/goal checks. No source or artifact bytes read. */
export function sourceReuseAuthorityMatches(proof:AcceptedSourceChainProof,current:unknown):boolean {
  const {proofDigest,...body}=proof,parsed=sourceReuseAuthoritySchema.safeParse(current);
  return digest(body)===proofDigest&&parsed.success&&parsed.data.complete&&
    parsed.data.totalContributors===parsed.data.contributors.length&&
    sameSet(parsed.data.catalogTaskIds,parsed.data.contributors.map(c=>c.taskId))&&
    sourceReuseSnapshotFingerprint(parsed.data)===proof.snapshotFingerprint;
}

/** proofDigest stays in the controller's planning intent, linked by this UUID;
 * it is deliberately not an extra workspace-manager prepare field. */
export function sourceReusePreparation(proof:AcceptedSourceChainProof,preparationIdentity:string){
  const {proofDigest,...body}=proof;
  if(!id.safeParse(preparationIdentity).success||digest(body)!==proofDigest)throw Error('SOURCE_REUSE_PROOF_INVALID');
  return {sourceWorkspace:proof.selectedSource.workspace,preparationIdentity,
    expectedSourceDigest:proof.selectedSource.candidateDigest,expectedSourceSnapshotDigest:proof.selectedSource.snapshotDigest};
}
