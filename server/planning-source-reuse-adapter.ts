import {createHash} from 'node:crypto';
import {z} from 'zod';
import type {DB} from './db.js';
import {runtimeCandidateDigest} from './operation-progress.js';
import {proveAcceptedSourceChain,sourceReuseSnapshotSchema,type SourceReuseContributor,
  type SourceReuseSnapshot,type SourceReuseEvidence,type AcceptedSourceChainProof} from './planning-source-reuse.js';

const canonical=(v:any):any=>Array.isArray(v)?v.map(canonical):v&&typeof v==='object'
  ?Object.fromEntries(Object.keys(v).sort().map(k=>[k,canonical(v[k])])):v;
const sha=(v:Buffer|string)=>createHash('sha256').update(v).digest('hex');
const digest=(v:unknown)=>sha(JSON.stringify(canonical(v)));
const id=z.uuid(),hash=z.string().regex(/^[a-f0-9]{64}$/),workspace=z.string().regex(/^[a-z0-9_-]+\/[a-z0-9_-]+$/);
const scopeSchema=z.object({goalId:id,revision:z.number().int().positive(),baseWorkspace:workspace}).strict();
export type SourceReuseScope=z.infer<typeof scopeSchema>;
type Contributor=Omit<SourceReuseContributor,'generation'>;
export type SourceReuseCapture={version:1;scope:SourceReuseSnapshot['scope'];rootWorkspace:string;
  planningAuthority:{goalState:string;objectiveState:string;mode:string;revision:number;requestDigest:string};
  totalContributors:number;catalogTaskIds:string[];contributors:Contributor[];
  /** Fixed canonical runtime names are not authority by themselves. Completion
   * must match these DB descriptors to the protected runtime's state. */
  artifactMimes:Record<string,string>;databaseFingerprint:string};
export type SourceReuseFallback={eligible:false;reason:string};
export type SourceReuseCaptureResult={eligible:true;capture:SourceReuseCapture}|SourceReuseFallback;
export type TrustedSourceReuseInspections={baseInspection:unknown;runInspections:ReadonlyMap<string,unknown>;
  artifactBytes:ReadonlyMap<string,Buffer>};
export type CompletedSourceReuseCapture={eligible:true;capture:SourceReuseCapture;snapshot:SourceReuseSnapshot;
  evidence:SourceReuseEvidence;proof:AcceptedSourceChainProof}|SourceReuseFallback;
const fallback=(reason:string):SourceReuseFallback=>({eligible:false,reason});
const fail=(reason:string):never=>{throw Object.assign(Error(reason),{sourceReuseReason:reason});};
const check=(condition:unknown,reason:string)=>{if(!condition)fail(reason);};
const fingerprint=(c:Omit<SourceReuseCapture,'databaseFingerprint'>)=>digest(c);
const contributorSchema=sourceReuseSnapshotSchema.shape.contributors.element.omit({generation:true});
const projection=(expression:string)=>`jsonb_build_object('workspace',${expression}->'workspace',
  'sourceWorkspace',${expression}->'sourceWorkspace','baseWorkspace',${expression}->'baseWorkspace',
  'readOnlySource',COALESCE(${expression}->'readOnlySource','false'::jsonb),
  'writePaths',COALESCE(${expression}->'writePaths','null'::jsonb))`;

/** One statement/one MVCC snapshot. No body, source tree, prompt, or Recall data
 * leaves PostgreSQL. Missing metadata is included, never filtered out of a
 * cohort by the very projection whose completeness is being checked.
 * The fast path does not detoast/hash instruction_snapshot: the enabled immutable
 * admission trigger plus unchanged run identity preserves the captured bytes. */
function captureSql(frozen:boolean,inputsOnly=false){return `WITH scope_tasks AS MATERIALIZED (
 SELECT t.id,t.goal_id,t.state,t.stage,t.plan_revision,t.attempt_count,t.execution,m.workspace,m.base_workspace,
   m.task_id metadata_id,m.goal_id metadata_goal,m.version metadata_version,m.execution_type,m.inputs_valid
 FROM tasks t LEFT JOIN task_execution_metadata m ON m.task_id=t.id
 WHERE t.goal_id=$1 AND t.plan_revision=$2 AND t.state='accepted' AND t.stage IN(${inputsOnly?"'work','integrate'":"'work','integrate','verify'"})
   AND (m.base_workspace=$3 OR m.task_id IS NULL OR m.version<>1 OR m.goal_id<>t.goal_id OR m.base_workspace IS NULL)
), cohort AS MATERIALIZED (SELECT * FROM scope_tasks ORDER BY id LIMIT 65),
 active AS MATERIALIZED (
 SELECT r.id,r.task_id,r.state,m.workspace,m.task_id metadata_id,m.version metadata_version,m.goal_id metadata_goal,t.goal_id,m.control->>'kind' execution_kind
 FROM runs r JOIN tasks t ON t.id=r.task_id LEFT JOIN task_execution_metadata m ON m.task_id=t.id
 WHERE r.state IN('queued','running','cancelling','reconciling')
 UNION SELECT r.id,r.task_id,r.state,m.workspace,m.task_id,m.version,m.goal_id,t.goal_id,m.control->>'kind'
 FROM leases l JOIN runs r ON r.id=l.run_id JOIN tasks t ON t.id=r.task_id
 LEFT JOIN task_execution_metadata m ON m.task_id=t.id
), records AS (
 SELECT t.id,t.goal_id,t.state,t.stage,t.plan_revision,t.attempt_count,t.workspace,t.base_workspace,
   t.metadata_id,t.metadata_goal,t.metadata_version,t.execution_type,t.inputs_valid,
   t.execution->>'kind' execution_kind,r.id run_id,r.attempt,r.fence,r.context_id,r.state run_state,r.runtime,
   ${projection('t.execution')} current_execution,
   ${frozen?`${projection("(r.instruction_snapshot->'task'->'execution')")} frozen_execution,
   encode(sha256(convert_to(r.instruction_snapshot::text,'UTF8')),'hex') instruction_digest,
   r.instruction_snapshot->>'schema' instruction_schema,r.instruction_snapshot->'task'->>'id' frozen_task,
   r.instruction_snapshot->'task'->>'goal_id' frozen_goal,`:''}
   c.run_id context_run,c.manifest->'runIdentity' context_identity,
   jsonb_typeof(c.manifest->'dependencyArtifacts')='array' frozen_refs_valid,
   CASE WHEN jsonb_typeof(c.manifest->'dependencyArtifacts')='array' THEN jsonb_array_length(c.manifest->'dependencyArtifacts') ELSE -1 END frozen_ref_count,
   (SELECT jsonb_agg(jsonb_build_object('reference',ref,'actual',jsonb_build_object('id',a.id,'runId',a.run_id,
     'taskId',ar.task_id,'name',a.name,'digest',a.digest,'size',a.size)) ORDER BY ref->>'id')
     FROM (SELECT value ref FROM jsonb_array_elements(CASE WHEN jsonb_typeof(c.manifest->'dependencyArtifacts')='array'
       THEN c.manifest->'dependencyArtifacts' ELSE '[]'::jsonb END) LIMIT 129) input
     LEFT JOIN artifacts a ON a.id=CASE WHEN ref->>'id' ~ '^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' THEN (ref->>'id')::uuid END
     LEFT JOIN runs ar ON ar.id=a.run_id) frozen_refs,
   (SELECT jsonb_build_object('id',ac.id,'taskId',ac.task_id,'runId',ac.run_id,'passed',ac.passed)
     FROM acceptances ac WHERE ac.task_id=t.id ORDER BY ac.created_at DESC,ac.id DESC LIMIT 1) acceptance,
   (SELECT count(*)::int FROM artifacts a WHERE a.run_id=r.id) artifact_count,
   (SELECT jsonb_agg(jsonb_build_object('id',a.id,'runId',a.run_id,'name',a.name,'digest',a.digest,'size',a.size,'mime',a.mime) ORDER BY a.id)
     FROM (SELECT * FROM artifacts WHERE run_id=r.id ORDER BY id LIMIT 65) a) artifacts,
   (SELECT count(*)::int FROM dependencies d WHERE d.task_id=t.id) dependency_count,
   (SELECT jsonb_agg(d.depends_on ORDER BY d.depends_on) FROM (SELECT depends_on FROM dependencies WHERE task_id=t.id ORDER BY depends_on LIMIT 129) d) dependencies,
   (SELECT count(*)::int FROM active a WHERE a.workspace=t.workspace AND a.state IN('queued','running','cancelling','reconciling')) live_count,
   (SELECT count(*)::int FROM leases l JOIN active a ON a.id=l.run_id WHERE a.workspace=t.workspace) lease_count
 FROM cohort t LEFT JOIN LATERAL(SELECT * FROM runs WHERE task_id=t.id ORDER BY attempt DESC,id DESC LIMIT 1) r ON true
 LEFT JOIN contexts c ON c.id=r.context_id
)
 SELECT jsonb_build_object('goalId',g.id,'project',g.project,'goalState',g.state,'objectiveState',o.status,'mode',o.mode,
 'revision',o.revision,'requestDigest',encode(sha256(convert_to(o.request::text,'UTF8')),'hex'),
 'total',(SELECT count(*)::int FROM scope_tasks),'rows',COALESCE((SELECT jsonb_agg(to_jsonb(x) ORDER BY x.id) FROM records x),'[]'::jsonb),
 'unknownOwnership',EXISTS(SELECT 1 FROM active a WHERE a.metadata_id IS NULL OR a.metadata_version<>1 OR a.metadata_goal<>a.goal_id
   OR (a.execution_kind IN('harness','opencode') AND a.workspace IS NULL)),
 'ownedWorkspaces',COALESCE((SELECT jsonb_agg(DISTINCT workspace) FROM active WHERE workspace IS NOT NULL),'[]'::jsonb),
 'immutableSnapshotTrigger',EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid='public.runs'::regclass AND tgname='agentos_instruction_snapshot'
   AND tgenabled IN('O','A') AND NOT tgisinternal AND tgfoid=to_regprocedure('public.agentos_instruction_snapshot()')))
 value FROM goals g JOIN objectives o ON o.id=g.id WHERE g.id=$1`;}

async function capture(db:DB,scope:SourceReuseScope,prior?:SourceReuseCapture,inputsOnly=false):Promise<SourceReuseCaptureResult>{
 try{
  check(scopeSchema.safeParse(scope).success,'INVALID_SCOPE');
  const value=(await db.query(captureSql(!prior,inputsOnly),[scope.goalId,scope.revision,scope.baseWorkspace])).rows[0]?.value;
  check(value&&value.goalId===scope.goalId&&value.revision===scope.revision&&value.mode==='execute'&&
    (inputsOnly?['active','replanning','failed']:['active','replanning']).includes(value.goalState),'PLANNING_AUTHORITY_CHANGED');
  check(value.immutableSnapshotTrigger===true,'IMMUTABLE_SNAPSHOT_REQUIRED');
  check(value.unknownOwnership===false,'OWNERSHIP_METADATA_MISSING');
  check(value.total<=64&&value.total===value.rows.length,'COHORT_BOUND');
  check(value.total>0,'NO_CONTRIBUTORS');
  const contributors:Contributor[]=[],artifactMimes:Record<string,string>={};
  for(const row of value.rows){
   check(row.metadata_id===row.id&&row.metadata_goal===scope.goalId&&row.metadata_version===1&&
     row.execution_type==='object'&&row.inputs_valid===true&&row.base_workspace===scope.baseWorkspace,'EXECUTION_METADATA_MISSING');
   check(row.runtime==='harness'&&row.execution_kind==='harness'&&row.run_state==='submitted'&&row.state==='accepted'&&
     row.attempt===row.attempt_count&&row.context_run===row.run_id,'IDENTITY_NOT_CURRENT');
   check(row.context_identity?.runId===row.run_id&&row.context_identity?.runFence===row.fence&&
     row.context_identity?.taskId===row.id&&row.context_identity?.goalId===scope.goalId,'FROZEN_CONTEXT_INVALID');
   check(row.frozen_refs_valid===true&&row.frozen_ref_count<=128&&row.dependency_count<=128&&row.artifact_count<=64,
     'ARTIFACT_CATALOG_BOUND');
   const old=prior?.contributors.find(c=>c.taskId===row.id);
   if(prior)check(old&&old.runId===row.run_id&&old.fence===row.fence&&old.contextId===row.context_id,'IDENTITY_NOT_CURRENT');
   else check(row.instruction_schema==='1'&&row.frozen_task===row.id&&row.frozen_goal===scope.goalId&&
     hash.safeParse(row.instruction_digest).success,'IMMUTABLE_SNAPSHOT_REQUIRED');
   const frozen=prior?old!.frozenExecution:row.frozen_execution;
   check(digest(row.current_execution)===digest(frozen),'FROZEN_EXECUTION_CHANGED');
   const refs=row.frozen_refs??[];
   check(refs.length===row.frozen_ref_count&&new Set(refs.map((x:any)=>x.reference?.id)).size===refs.length,'FROZEN_INPUT_CHANGED');
   const frozenParentInputs=refs.map(({reference:r,actual:a}:any)=>{
    check(a&&r.id===a.id&&r.name===a.name&&r.digest===a.digest&&r.task_id===a.taskId&&
      (r.run_id===undefined||r.run_id===a.runId)&&(r.size===undefined||r.size===a.size),'FROZEN_INPUT_CHANGED');
    const {taskId,...artifact}=a;return artifact;
   });
   const artifacts=(row.artifacts??[]).map(({mime,...artifact}:any)=>{artifactMimes[artifact.id]=mime;return artifact;});
   check(artifacts.length===row.artifact_count&&(row.dependencies??[]).length===row.dependency_count,'INCOMPLETE_CATALOG');
   const current:Contributor={taskId:row.id,goalId:scope.goalId,project:value.project,revision:scope.revision,
     taskState:row.state,attempt:row.attempt,currentAttempt:row.attempt_count,runId:row.run_id,latestRunId:row.run_id,
     runState:row.run_state,fence:row.fence,contextId:row.context_id,
     instructionSnapshotDigest:prior?old!.instructionSnapshotDigest:row.instruction_digest,latestAcceptance:row.acceptance,
     execution:row.current_execution,frozenExecution:frozen,dependencyTaskIds:row.dependencies??[],frozenParentInputs,
     artifacts,artifactsComplete:true,liveRunCount:row.live_count,activeLeaseCount:row.lease_count,
     ownershipUncertain:row.live_count>0||row.lease_count>0};
   check(current.latestAcceptance?.passed===true&&current.latestAcceptance.taskId===current.taskId&&
     current.latestAcceptance.runId===current.runId,'LATEST_ACCEPTANCE_CHANGED');
   check(!current.ownershipUncertain,'LIVE_OR_UNCERTAIN_SOURCE');
   check(contributorSchema.safeParse(current).success,'CONTRIBUTOR_METADATA_INVALID');contributors.push(current);
  }
  const workspaces=new Set(contributors.map(c=>c.execution.workspace));
  const roots=[...new Set(contributors.map(c=>c.execution.sourceWorkspace).filter(w=>!workspaces.has(w)))];
  check(roots.length===1&&workspace.safeParse(roots[0]).success,'SOURCE_ROOT_AMBIGUOUS');
  check(!value.ownedWorkspaces.includes(roots[0]),'BASE_WORKSPACE_OWNED');
  const body:Omit<SourceReuseCapture,'databaseFingerprint'>={version:1,
   scope:{...scope,project:value.project},rootWorkspace:roots[0],
   planningAuthority:{goalState:value.goalState,objectiveState:value.objectiveState,mode:value.mode,revision:value.revision,requestDigest:value.requestDigest},
   totalContributors:contributors.length,catalogTaskIds:contributors.map(c=>c.taskId),contributors,artifactMimes};
  return {eligible:true,capture:{...body,databaseFingerprint:fingerprint(body)}};
 }catch(error:any){
  if(error.sourceReuseReason)return fallback(error.sourceReuseReason);
  // Unavailable schema/DB and malformed historical metadata never select a source.
  return fallback('SOURCE_AUTHORITY_UNAVAILABLE');
 }
}

/** Read-only, single-statement capture; caller supplies a trusted controller DB.
 * Do not call with projections supplied by a worker or a model. */
export const captureSourceReuseAuthority=(db:DB,scope:SourceReuseScope)=>capture(db,scope);

/** Canonical artifacts for an authenticated new plan, not authority to reuse a
 * workspace or claim incorporation. Failed objectives may supply immutable
 * inputs; their old attempts and physical generations are never reopened. */
export const captureSourceReplanInputs=(db:DB,scope:SourceReuseScope)=>capture(db,scope,undefined,true);

/** Intended under the planner admission lock. SQL only: no artifact/SSH/filesystem
 * reads, source hashing, queue or acceptance mutation. The existing immutable
 * instruction_snapshot trigger protects frozen data for an unchanged run. */
export async function recaptureSourceReuseAuthority(db:DB,prior:SourceReuseCapture):Promise<SourceReuseCaptureResult>{
 const {databaseFingerprint,...body}=prior;
 if(fingerprint(body)!==databaseFingerprint)return fallback('CAPTURE_CHANGED');
 const current=await capture(db,{goalId:prior.scope.goalId,revision:prior.scope.revision,baseWorkspace:prior.scope.baseWorkspace},prior);
 if(!current.eligible)return current;
 return current.capture.databaseFingerprint===databaseFingerprint?current:fallback('DATABASE_AUTHORITY_CHANGED');
}

const inspectedCandidate=z.object({baseDigest:hash,candidateDigest:hash,
 files:z.array(z.object({path:z.string(),sha256:hash}).strict()).max(10000)}).strict();
const inspectedGeneration=sourceReuseSnapshotSchema.shape.contributors.element.shape.generation;
const baseInspectionSchema=z.object({version:z.literal(1),workspace,generation:inspectedGeneration,
 markerBase64:z.string().max(180000),legacyParentGeneration:z.boolean(),candidate:inspectedCandidate,checkedAt:z.string()}).strict();
const runtimeInspectionSchema=baseInspectionSchema.omit({checkedAt:true}).extend({runId:id,
 state:z.object({runId:id,workspace,runtime:z.literal('harness'),state:z.literal('completed'),runFence:id,
   stopped:z.literal(true),artifacts:z.array(z.object({name:z.string(),digest:hash,mime:z.string()}).strict()).length(2)}).strict(),
 admission:z.object({authority:z.literal('runtime_operation_policy'),goalId:id,taskId:id,runId:id,runFence:id,contextVersion:id,
   workspace:z.literal('/workspace'),sourceSnapshotDigest:hash,policyDigest:hash}).strict(),
 process:z.object({unit:z.string(),checkedAt:z.string(),stopped:z.literal(true),mainPid:z.literal(0),populated:z.literal(false),
   loadState:z.enum(['loaded','not-found']),activeState:z.enum(['inactive','failed']),subState:z.string(),cgroupExists:z.boolean()}).strict(),
 checkedAt:z.string()}).strict();
function marker(value:z.infer<typeof baseInspectionSchema>){
 const raw=Buffer.from(value.markerBase64,'base64');
 check(raw.length<=128*1024&&raw.toString('base64')===value.markerBase64&&sha(raw)===value.generation.snapshotDigest,'INSPECTED_MARKER_CHANGED');
 const data=JSON.parse(raw.toString('utf8'));
 check(value.workspace===value.generation.workspace&&data.workspace===value.workspace&&data.sourceWorkspace===value.generation.sourceWorkspace&&
   data.baseDigest===value.generation.baseDigest&&(data.sourceSnapshotDigest??null)===value.generation.sourceSnapshotDigest&&
   value.candidate.baseDigest===value.generation.baseDigest&&
   value.legacyParentGeneration===(value.generation.sourceSnapshotDigest===null),'INSPECTED_GENERATION_CHANGED');
 return raw;
}

/** Pure completion of TRUSTED controller RPC responses. These schemas validate
 * identity and integrity, not origin: never expose this as an API accepting
 * worker-provided attestations. No historical metadata is backfilled. */
export function completeSourceReuseCapture(capture:SourceReuseCapture,observed:TrustedSourceReuseInspections,
 now=Date.now()):CompletedSourceReuseCapture{
 try{
  const {databaseFingerprint,...body}=capture;
  check(fingerprint(body)===databaseFingerprint&&Number.isFinite(now),'CAPTURE_CHANGED');
  const base=baseInspectionSchema.parse(observed.baseInspection);
  check(base.workspace===capture.rootWorkspace,'BASE_WORKSPACE_CHANGED');
  const age=now-Date.parse(base.checkedAt);check(Number.isFinite(age)&&age>=0&&age<=60000,'INSPECTION_STALE');
  const baseMarker=marker(base),markers=new Map<string,Buffer>(),stops:SourceReuseEvidence['stops']=new Map();
  const mutableStops=stops as Map<string,any>;
  const contributors=capture.contributors.map(c=>{
   const value=runtimeInspectionSchema.parse(observed.runInspections.get(c.runId));
   const a=value.admission;
   check(value.runId===c.runId&&value.workspace===c.execution.workspace&&value.state.runId===c.runId&&
     value.state.workspace===c.execution.workspace&&value.state.runFence===c.fence&&a.runId===c.runId&&a.runFence===c.fence&&
     a.contextVersion===c.contextId&&a.taskId===c.taskId&&a.goalId===c.goalId&&
     a.sourceSnapshotDigest===value.generation.snapshotDigest,'RUNTIME_ADMISSION_CHANGED');
   check(value.process.unit==='agentos-run-'+c.runId&&value.checkedAt===value.process.checkedAt,'STOP_UNCONFIRMED');
   markers.set(c.execution.workspace,marker(value));
   for(const name of ['candidate.json','changes.patch']){
    const refs=c.artifacts.filter(r=>r.name===name),published=value.state.artifacts.filter(r=>r.name===name);
    check(refs.length===1&&published.length===1&&refs[0].digest===published[0].digest&&
      capture.artifactMimes[refs[0].id]===published[0].mime,'RUNTIME_ARTIFACT_ORIGIN_CHANGED');
   }
   const ref=c.artifacts.find(r=>r.name==='candidate.json')!,bytes=observed.artifactBytes.get(ref.id);
   check(Buffer.isBuffer(bytes)&&runtimeCandidateDigest(bytes!,ref.digest)===value.candidate.candidateDigest,
     'CURRENT_CANDIDATE_CHANGED');
   const actual=JSON.parse(bytes!.toString('utf8'));
   check(actual.baseDigest===value.candidate.baseDigest&&digest(actual.files)===digest(value.candidate.files),'CURRENT_CANDIDATE_CHANGED');
   mutableStops.set(c.runId,{taskId:c.taskId,runId:c.runId,fence:c.fence,contextId:c.contextId,workspace:c.execution.workspace,
     candidateDigest:value.candidate.candidateDigest,snapshotDigest:value.generation.snapshotDigest,checkedAt:Date.parse(value.checkedAt),
     stopped:true,mainPid:0,cgroupPopulated:false,liveWorkspaceActors:c.liveRunCount,ownershipUncertain:c.ownershipUncertain});
   return {...c,generation:value.generation};
  });
  const snapshot:SourceReuseSnapshot={version:1,scope:capture.scope,complete:true,totalContributors:capture.totalContributors,
   catalogTaskIds:capture.catalogTaskIds,base:{workspace:base.workspace,sourceDigest:base.candidate.candidateDigest,
     snapshotDigest:base.generation.snapshotDigest,files:base.candidate.files},contributors};
  const evidence:SourceReuseEvidence={baseMarker,artifacts:observed.artifactBytes,markers,stops};
  const result=proveAcceptedSourceChain(snapshot,evidence,now);
  return result.eligible?{eligible:true,capture,snapshot,evidence,proof:result.proof}:result;
 }catch(error:any){return fallback(error.sourceReuseReason??'INSPECTION_INVALID');}
}
