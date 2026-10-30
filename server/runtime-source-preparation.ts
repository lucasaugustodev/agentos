import {z} from 'zod';
import {pool,tx,lock,type DB} from './db.js';
import {accountBridge} from './bridge.js';
import {DomainError} from './contracts.js';
import {inspectStopped,assertStopped} from './continuation.js';
import {preparationHash,sourcePreparationRequestSchema,preparedBaselineBinding,verifySourcePreparation,
  type SourcePreparationRequest,type VerifiedSourcePreparation} from './source-preparation-contract.js';
import type {RuntimeInput} from './runtime.js';

const uuid=z.uuid(),sha=z.string().regex(/^[a-f0-9]{64}$/);
const artifact=z.object({id:uuid,runId:uuid,name:z.enum(['candidate.json','changes.patch']),digest:sha,size:z.number().int().nonnegative()}).strict();
const reuseSchema=z.object({version:z.literal(1),planningRunId:uuid,proofDigest:sha,preparationIdentity:uuid,
  receiptDigest:sha,baselineWorkspace:z.string(),incorporatedArtifacts:z.array(artifact).min(2).max(128),
  acceptance:z.literal(false),finalVerificationRequired:z.literal(true)}).strict();
const planningSourceSchema=reuseSchema.omit({incorporatedArtifacts:true}).strict();
const terminal=new Set(['submitted','failed','cancelled']);
const failure=(code:string)=>new DomainError(code,'A preparação vinculada exige reconciliação; o workspace existente foi preservado.');
const requireProof=(ok:unknown,code='SOURCE_PREPARATION_AUTHORITY_CHANGED')=>{if(!ok)throw failure(code);};
const same=(a:unknown,b:unknown)=>preparationHash(a)===preparationHash(b);
const control=(e:any)=>Object.fromEntries(['kind','workspace','sourceWorkspace','baseWorkspace','sourcePreparation','sourceReuse','planningSource'].map(k=>[k,e?.[k]??null]));
const sqlControl=(e:string)=>`jsonb_build_object(${['kind','workspace','sourceWorkspace','baseWorkspace','sourcePreparation','sourceReuse','planningSource'].map(k=>`'${k}',${e}->'${k}'`).join(',')})`;
function authority(execution:any){
  const explicit=Object.hasOwn(execution,'planningSource');
  requireProof(explicit!==Object.hasOwn(execution,'sourceReuse'),'SOURCE_PREPARATION_BINDING_INVALID');
  const parsed=explicit?planningSourceSchema.safeParse(execution.planningSource):reuseSchema.safeParse(execution.sourceReuse);
  requireProof(parsed.success,'SOURCE_PREPARATION_BINDING_INVALID');
  return {explicit,value:parsed.data!};
}
type Captured={fingerprint:string;previous:any[];request:SourcePreparationRequest};
type Prepared={capture:Captured;verified:VerifiedSourcePreparation;stop:any|null;admission:any|null;requireUnchanged:boolean};
const prepared=new WeakMap<RuntimeInput,Prepared>();

/** One MVCC observation, projected to identity/contract metadata only. Large
 * instructions, execution inputs, candidate bodies and source trees stay out
 * of both the initial read and the admission-lock recapture. */
async function capture(db:DB,input:RuntimeInput,request:SourcePreparationRequest):Promise<Captured>{
  const {explicit,value:reuse}=authority(input.task.execution);
  const row=(await db.query(`SELECT r.id,r.task_id,r.attempt,r.fence,r.context_id,r.state,r.runtime,r.worker_id,
    t.goal_id,t.plan_revision,t.attempt_count,t.state task_state,g.state goal_state,g.project,o.revision objective_revision,o.mode,
    r.instruction_snapshot->>'schema' instruction_schema,r.instruction_snapshot#>>'{task,id}' frozen_task,
    r.instruction_snapshot#>>'{task,goal_id}' frozen_goal,r.instruction_snapshot#>'{task,plan_revision}' frozen_revision,
    ${sqlControl("(r.instruction_snapshot#>'{task,execution}')")} frozen_execution,
    ${sqlControl('t.execution')} current_execution,r.instruction_snapshot#>'{task,resources}' frozen_resources,t.resources,
    c.run_id context_run,c.manifest->'runIdentity' context_identity,
    (SELECT jsonb_agg(resource ORDER BY resource) FROM leases WHERE run_id=r.id) leases,
    EXISTS(SELECT 1 FROM leases WHERE resource=$4 AND run_id<>r.id) conflicting_lease,
    EXISTS(SELECT 1 FROM runs other JOIN tasks ot ON ot.id=other.task_id WHERE other.id<>r.id
      AND other.state IN('queued','running','cancelling','reconciling')
      AND (other.task_id=t.id OR ot.execution->>'workspace'=$5)) conflicting_actor,
    (SELECT count(*)::int FROM runs WHERE task_id=t.id AND attempt<r.attempt) prior_count,
    (SELECT max(attempt) FROM runs WHERE task_id=t.id) latest_attempt,
    (SELECT jsonb_agg(jsonb_build_object('id',p.id,'task_id',p.task_id,'attempt',p.attempt,'fence',p.fence,
      'state',p.state,'runtime',p.runtime,'context_id',p.context_id,'checkpoint',jsonb_build_object('kind',p.checkpoint->'kind','sourceRunId',p.checkpoint->'sourceRunId'),
      'instruction_snapshot',jsonb_build_object('schema',p.instruction_snapshot->'schema','task',jsonb_build_object(
        'id',p.instruction_snapshot#>'{task,id}','goal_id',p.instruction_snapshot#>'{task,goal_id}',
        'plan_revision',p.instruction_snapshot#>'{task,plan_revision}','execution',${sqlControl("(p.instruction_snapshot#>'{task,execution}')")})),
      'known_not_started',EXISTS(SELECT 1 FROM events e WHERE e.entity_id=t.id::text AND e.kind='HARNESS_NOT_STARTED'
        AND e.payload->>'runId'=p.id::text AND e.payload->>'confirmed'='true')
        AND NOT EXISTS(SELECT 1 FROM events e WHERE e.entity_id=t.id::text AND e.payload->>'runId'=p.id::text
          AND e.kind IN('HARNESS_STARTED','RUNTIME_STARTED','MODEL_REQUEST','MODEL_RESPONSE','TOOL_CALL','TOOL_RESULT','HARNESS_TOOL_EVENT')))
      ORDER BY p.attempt DESC) FROM (SELECT id,task_id,attempt,fence,state,runtime,context_id,checkpoint,instruction_snapshot
        FROM runs WHERE task_id=t.id AND attempt<r.attempt ORDER BY attempt DESC LIMIT 65) p) previous,
    (SELECT jsonb_agg(jsonb_build_object('key',s.id,'version',s.value->'version','state',s.value->'state',
      'planningRunId',s.value->'planningRunId','ownerFence',s.value->'ownerFence','goalId',s.value->'goalId','baseRevision',s.value->'baseRevision',
      'kind',s.value->'kind','explicitProof',CASE WHEN s.value->>'kind'='explicit_source' THEN s.value->'proof' ELSE NULL END,
      'baseIndex',s.value->'baseIndex','baseWorkspace',s.value->'baseWorkspace','request',s.value->'request','prepared',s.value->'prepared',
      'proofVersion',s.value#>'{proof,version}','proofKind',s.value#>'{proof,kind}','proofDigest',s.value#>'{proof,proofDigest}',
      'proofScope',s.value#>'{proof,scope}','selectedSource',jsonb_build_object('workspace',s.value#>'{proof,selectedSource,workspace}',
        'candidateDigest',s.value#>'{proof,selectedSource,candidateDigest}','snapshotDigest',s.value#>'{proof,selectedSource,snapshotDigest}'),
      'acceptance',s.value#>'{proof,acceptance}','finalVerificationRequired',s.value#>'{proof,finalVerificationRequired}',
      'incorporatedArtifacts',(SELECT jsonb_agg(a ORDER BY n,m) FROM jsonb_array_elements(s.value#>'{proof,incorporatedContributors}')
        WITH ORDINALITY p(v,n) CROSS JOIN LATERAL jsonb_array_elements(jsonb_build_array(v->'candidateArtifact',v->'patchArtifact')) WITH ORDINALITY x(a,m)),
      'planningGoal',p.objective_id,'planningState',p.state,'planningRevision',p.base_revision,'planningFence',p.fence))
      FROM (SELECT id,value FROM settings WHERE id LIKE $2 AND value#>>'{request,workspace}'=$3 ORDER BY id LIMIT 2) s
      LEFT JOIN planning_runs p ON p.id=$6) intents
    FROM runs r JOIN tasks t ON t.id=r.task_id JOIN goals g ON g.id=t.goal_id JOIN objectives o ON o.id=g.id
    LEFT JOIN contexts c ON c.id=r.context_id WHERE r.id=$1`,
    [input.run.id,`planning-source-preparation:${reuse.planningRunId}:%`,request.sourceWorkspace,'workspace:'+request.workspace,
      request.workspace,reuse.planningRunId])).rows[0];
  requireProof(row&&row.id===input.run.id&&row.task_id===input.task.id&&row.fence===input.run.fence&&
    row.worker_id===input.run.worker_id&&row.context_id===input.context.id&&row.context_run===row.id&&row.runtime==='harness'&&
    row.state==='running'&&row.task_state==='running'&&row.goal_state==='active'&&row.mode==='execute'&&
    row.attempt===input.run.attempt&&row.attempt_count===row.attempt&&row.latest_attempt===row.attempt&&row.goal_id===input.task.goal_id&&
    row.plan_revision===input.task.plan_revision&&row.objective_revision===row.plan_revision&&
    row.instruction_schema==='1'&&row.frozen_task===input.task.id&&row.frozen_goal===input.task.goal_id&&row.frozen_revision===row.plan_revision);
  requireProof(same(row.frozen_execution,control(input.task.execution))&&same(row.current_execution,row.frozen_execution)&&
    same(row.context_identity,{runId:input.run.id,runFence:input.run.fence,taskId:input.task.id,goalId:input.task.goal_id})&&
    same(input.context.manifest.runIdentity,row.context_identity));
  const resources=z.array(z.string()).min(1).max(100).safeParse(row.frozen_resources);
  requireProof(resources.success&&same(row.resources,row.frozen_resources)&&same(input.task.resources,row.frozen_resources)&&
    resources.data!.includes('workspace:'+request.workspace)&&new Set(resources.data).size===resources.data!.length&&
    same([...resources.data!].sort(),row.leases)&&!row.conflicting_lease&&!row.conflicting_actor,'SOURCE_PREPARATION_OWNER_CHANGED');
  requireProof(row.intents?.length===1);
  const intent=row.intents[0],stored=intent.prepared as VerifiedSourcePreparation,baseRequest=sourcePreparationRequestSchema.safeParse(intent.request);
  requireProof(baseRequest.success&&intent.version===1&&intent.state==='applied'&&intent.planningRunId===reuse.planningRunId&&
    intent.goalId===input.task.goal_id&&intent.planningGoal===input.task.goal_id&&intent.planningState==='completed'&&
    intent.ownerFence===intent.planningFence&&uuid.safeParse(intent.ownerFence).success&&
    intent.baseRevision+1===row.plan_revision&&intent.planningRevision===intent.baseRevision&&Number.isInteger(intent.baseIndex)&&intent.baseIndex>=0&&
    intent.key===`planning-source-preparation:${reuse.planningRunId}:${intent.baseIndex}`&&
    intent.baseWorkspace===input.task.execution.baseWorkspace&&intent.proofScope?.goalId===input.task.goal_id&&
    intent.proofScope?.project===row.project&&request.workspace.split('/')[0]===row.project&&
    intent.proofScope?.revision===intent.baseRevision&&intent.proofScope?.baseWorkspace===intent.baseWorkspace&&
    intent.proofVersion===1&&intent.proofKind===(explicit?'explicit_planning_source':'repair_source_reuse')&&
    (explicit?intent.kind==='explicit_source':!intent.kind||intent.kind==='accepted_reuse')&&
    intent.acceptance===false&&intent.finalVerificationRequired===true);
  if(explicit){
    const proof=intent.explicitProof,{proofDigest,...body}=proof??{};
    requireProof(proof&&proofDigest===preparationHash(body)&&proofDigest===intent.proofDigest&&
      sha.safeParse(proof.databaseFingerprint).success&&
      ['snapshot','unmarked'].includes(proof.selectedSource?.sourceGenerationKind)&&
      (proof.selectedSource.sourceGenerationKind==='unmarked')===(intent.request.sourceGenerationKind==='unmarked'));
  }
  requireProof(stored?.version===1&&same(stored.request,intent.request)&&sha.safeParse(stored.targetSnapshotDigest).success&&
    stored.destinationChanged===false&&stored.currentCandidateDigest===intent.request.expectedSourceDigest&&
    intent.selectedSource.workspace===intent.request.sourceWorkspace&&intent.selectedSource.candidateDigest===intent.request.expectedSourceDigest&&
    intent.selectedSource.snapshotDigest===intent.request.expectedSourceSnapshotDigest);
  const receipt=stored.receipt,{receiptDigest,...body}=receipt??{};
  const binding={version:1,preparationIdentity:intent.request.preparationIdentity,sourceWorkspace:intent.request.sourceWorkspace,
    targetWorkspace:intent.request.workspace,expectedSourceDigest:intent.request.expectedSourceDigest,expectedSourceSnapshotDigest:intent.request.expectedSourceSnapshotDigest,
    ...(intent.request.sourceGenerationKind?{sourceGenerationKind:intent.request.sourceGenerationKind}:{})};
  requireProof(receipt&&Object.entries(binding).every(([k,v])=>receipt[k as keyof typeof receipt]===v)&&
    receipt.bindingDigest===preparationHash(binding)&&receiptDigest===preparationHash(body)&&
    receipt.sourceSnapshotDigest===intent.request.expectedSourceSnapshotDigest&&receipt.sourceGenerationKind===intent.request.sourceGenerationKind&&
    [receipt.observedSourceDigest,receipt.cloneBaseDigest,receipt.baselineDigest].every(d=>d===intent.request.expectedSourceDigest));
  requireProof(same(reuse,{version:1,planningRunId:intent.planningRunId,proofDigest:intent.proofDigest,
    preparationIdentity:intent.request.preparationIdentity,receiptDigest,baselineWorkspace:intent.request.workspace,
    ...(!explicit?{incorporatedArtifacts:intent.incorporatedArtifacts}:{}),acceptance:false,finalVerificationRequired:true})&&
    same(request,preparedBaselineBinding(stored,request.sourceWorkspace,request.workspace,request.preparationIdentity)));
  const previous=row.previous??[];
  requireProof(row.prior_count<=64&&previous.length===row.prior_count&&row.prior_count===row.attempt-1,'SOURCE_PREPARATION_HISTORY_INCOMPLETE');
  for(let n=0;n<previous.length;n++){
    const p=previous[n],frozen=p.instruction_snapshot;
    requireProof(p.task_id===input.task.id&&p.attempt===row.attempt-n-1&&terminal.has(p.state)&&p.runtime==='harness'&&frozen?.schema===1&&
      frozen.task.id===input.task.id&&frozen.task.goal_id===input.task.goal_id&&frozen.task.plan_revision===row.plan_revision&&
      same(frozen.task.execution,row.frozen_execution),'SOURCE_PREPARATION_RETRY_BINDING_CHANGED');
  }
  return {fingerprint:preparationHash(row),previous,request};
}

const fresh=(checkedAt:unknown)=>{const at=typeof checkedAt==='number'?checkedAt:Date.parse(String(checkedAt));return Number.isFinite(at)&&Date.now()>=at&&Date.now()-at<=60000;};
function absent(request:SourcePreparationRequest,value:any){
  return value?.version===1&&value.exists===false&&value.confirmed===true&&value.workspace===request.workspace&&
    value.preparationIdentity===request.preparationIdentity&&fresh(value.checkedAt);
}
async function inspect(request:SourcePreparationRequest,input:RuntimeInput){
  if(input.signal.aborted)throw failure('CANCELLED');
  return accountBridge.request('worker.inspectPreparation',request,{signal:input.signal,timeout:30000});
}
async function stopped(input:RuntimeInput,current:Captured){
  const previous=current.previous[0];if(!previous)return null;
  const proof=await inspectStopped(previous.id,false);
  assertStopped(proof,previous,input.task);
  return proof;
}
async function retryAdmission(input:RuntimeInput,current:Captured,verified:VerifiedSourcePreparation){
  // Later failed preflights may have no policy of their own. The last actual
  // executor still owns the preserved edits; inspect that exact run, never an
  // arbitrary historical receipt or the new attempt's not-started observation.
  const previous=current.previous.find(p=>!p.known_not_started);if(!previous)return null;
  const value=await accountBridge.request('worker.inspectRetrySourceBinding',
    {runId:previous.id,workspace:current.request.workspace},{signal:input.signal,timeout:30000});
  assertStopped(value,previous,input.task);
  const a=value?.admission,g=value?.generation,s=value?.state,r=current.request;
  requireProof(value.version===1&&value.workspace===r.workspace&&value.runId===previous.id&&
    a?.authority==='runtime_operation_policy'&&a.goalId===input.task.goal_id&&a.taskId===input.task.id&&
    a.runId===previous.id&&a.runFence===previous.fence&&a.contextVersion===previous.context_id&&uuid.safeParse(a.contextVersion).success&&
    a.workspace==='/workspace'&&sha.safeParse(a.policyDigest).success&&a.sourceSnapshotDigest===verified.targetSnapshotDigest&&
    g?.workspace===r.workspace&&g.sourceWorkspace===r.sourceWorkspace&&g.baseDigest===r.expectedSourceDigest&&
    g.snapshotDigest===verified.targetSnapshotDigest&&g.sourceSnapshotDigest===r.expectedSourceSnapshotDigest&&
    s?.runId===previous.id&&s.workspace===r.workspace&&s.runtime==='harness'&&s.runFence===previous.fence&&s.stopped===true&&
    ['completed','failed','cancelled'].includes(s.state)&&fresh(value.checkedAt)&&
    value.candidate?.baseDigest===r.expectedSourceDigest&&value.candidate?.candidateDigest===verified.currentCandidateDigest,
    'SOURCE_PREPARATION_RETRY_ADMISSION_INVALID');
  // Keep only identity and physical observation for the SQL-only recapture.
  return {runId:value.runId,workspace:value.workspace,process:value.process,checkedAt:value.checkedAt,
    policyDigest:a.policyDigest,sourceSnapshotDigest:a.sourceSnapshotDigest};
}
async function recapture(input:RuntimeInput,state:Prepared){
  if(input.signal.aborted)throw failure('CANCELLED');
  await tx(async db=>{
    await lock(db);
    const current=await capture(db,input,state.capture.request);
    requireProof(current.fingerprint===state.capture.fingerprint);
    requireProof(fresh(state.verified.checkedAt)&&(!state.stop||fresh(state.stop.checkedAt))&&
      (!state.admission||fresh(state.admission.checkedAt)),'SOURCE_PREPARATION_INSPECTION_STALE');
    if(state.stop)assertStopped(state.stop,current.previous[0],input.task);
    if(state.admission)assertStopped(state.admission,current.previous.find(p=>p.id===state.admission.runId),input.task);
  });
}

/** Only absence of the frozen opt-in selects legacy preparation. Bound partial,
 * unknown or changed state is never retried through legacy or a new UUID. */
export async function prepareBoundRuntimeWorkspace(input:RuntimeInput):Promise<boolean>{
  prepared.delete(input);
  const liveHas=Object.hasOwn(input.task.execution??{},'sourcePreparation');
  const frozen=input.run.instruction_snapshot?.task;
  const frozenHas=Object.hasOwn(frozen?.execution??{},'sourcePreparation');
  if(!liveHas&&!frozenHas)return false;
  requireProof(liveHas&&frozenHas&&input.run.instruction_snapshot.schema===1&&frozen.id===input.task.id&&
    frozen.goal_id===input.task.goal_id&&same(control(frozen.execution),control(input.task.execution))&&
    input.task.execution.kind==='harness','SOURCE_PREPARATION_FROZEN_BINDING_REQUIRED');
  const parsed=sourcePreparationRequestSchema.safeParse(frozen.execution.sourcePreparation);
  requireProof(parsed.success,'SOURCE_PREPARATION_BINDING_INVALID');
  authority(frozen.execution);
  const request=parsed.data!;
  requireProof(request.workspace===input.task.execution.workspace&&request.sourceWorkspace===input.task.execution.sourceWorkspace);
  const captured=await capture(pool,input,request),stop=await stopped(input,captured);
  const neverStarted=captured.previous.length>0&&captured.previous.every(p=>p.known_not_started===true)&&
    (stop?.state==='not_started'||stop?.notStarted===true);
  if(captured.previous.length&&captured.previous.every(p=>p.known_not_started===true))
    requireProof(neverStarted,'SOURCE_PREPARATION_NONSTART_UNCONFIRMED');
  let inspection=await inspect(request,input),requireUnchanged=captured.previous.length===0||neverStarted;
  if(absent(request,inspection)){
    // A disappeared modified clone must not erase a prior attempt's effects.
    // Only fully evidenced non-start histories may create the original UUID.
    requireProof(!captured.previous.length||neverStarted,'SOURCE_PREPARATION_RETRY_SOURCE_MISSING');
    requireUnchanged=true;
    const current=await capture(pool,input,request);requireProof(current.fingerprint===captured.fingerprint);
    await accountBridge.request('worker.prepare',request,{signal:input.signal,timeout:180000});
    inspection=await inspect(request,input);
  }
  const verified=verifySourcePreparation(request,inspection,{requireUnchanged});
  const state:Prepared={capture:captured,verified,stop,requireUnchanged,admission:await retryAdmission(input,captured,verified)};
  // Preparation can be long; refresh the predecessor's physical proof outside
  // the admission transaction instead of aging into a permanent retry loop.
  if(state.stop&&!fresh(state.stop.checkedAt))state.stop=await stopped(input,captured);
  await recapture(input,state);prepared.set(input,state);
  return true;
}

/** Called after input materialization, immediately before worker.execute. No
 * source is recopied: stale observations are refreshed read-only outside the
 * scheduler lock; the final lock phase performs metadata checks only. */
export async function assertBoundRuntimePreparationCurrent(input:RuntimeInput):Promise<void>{
  const state=prepared.get(input),present=Object.hasOwn(input.task.execution??{},'sourcePreparation'),
    frozen=Object.hasOwn(input.run.instruction_snapshot?.task?.execution??{},'sourcePreparation');
  if(!present&&!frozen&&!state)return;
  requireProof(state&&present&&frozen,'SOURCE_PREPARATION_NOT_VERIFIED');
  if(!fresh(state!.verified.checkedAt)||(state!.stop&&!fresh(state!.stop.checkedAt))||
    (state!.admission&&!fresh(state!.admission.checkedAt))){
    const current=await capture(pool,input,state!.capture.request);requireProof(current.fingerprint===state!.capture.fingerprint);
    state!.stop=await stopped(input,current);
    state!.verified=verifySourcePreparation(current.request,await inspect(current.request,input),{requireUnchanged:state!.requireUnchanged});
    state!.admission=await retryAdmission(input,current,state!.verified);
  }
  await recapture(input,state!);
}
