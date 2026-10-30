import {randomUUID} from 'node:crypto';
import {pool,tx,lock,event,type DB} from './db.js';
import {accountBridge} from './bridge.js';
import {readArtifact} from './service.js';
import {assertSourceReplanCurrent,inspectSourceReplanCurrent} from './objective-source-replan.js';
import {DomainError} from './contracts.js';
import {assertPlanningFeedbackCurrent} from './planning-supervision.js';
import {captureSourceReuseAuthority,completeSourceReuseCapture,recaptureSourceReuseAuthority,
  type SourceReuseCapture} from './planning-source-reuse-adapter.js';
import {sourceReusePreparation,type AcceptedSourceChainProof} from './planning-source-reuse.js';
import {preparationHash,verifySourcePreparation,preparedBaselineBinding,
  type SourcePreparationRequest,type VerifiedSourcePreparation} from './source-preparation-contract.js';
import type {ProposedPlan} from './objective-contracts.js';
import {inspectWorkspaceGeneration,inspectWorkspacePreparation,inspectWorkerSourceBinding,
  type WorkspaceInspectionRequest} from './workspace-inspection.js';
import {assertPlanningSourcesIdle,captureExplicitSourceAuthority,recaptureExplicitSourceAuthority,observeExplicitSource,
  type ExplicitSourceCapture,type ExplicitSourceProof} from './planning-explicit-source.js';

type Intent={version:1;planningRunId:string;goalId:string;baseRevision:number;baseIndex:number;baseWorkspace:string;
  planDigest:string;request:SourcePreparationRequest;
  state:'reserved'|'preparing'|'uncertain'|'prepared'|'applied';ownerFence:string;prepared?:VerifiedSourcePreparation;
  errorCode?:string;updatedAt:string}&({kind?:'accepted_reuse';capture:SourceReuseCapture;proof:AcceptedSourceChainProof}|
    {kind:'explicit_source';capture:ExplicitSourceCapture;proof:ExplicitSourceProof});
export type PreparedPlanningSource={intent:Intent;prepared:VerifiedSourcePreparation;proofCheckedAt:number};
export type PreparedPlanningSources=Map<string,PreparedPlanningSource>;
const key=(runId:string,index:number)=>`planning-source-preparation:${runId}:${index}`;
const failure=(code:string)=>new DomainError(code,'A origem preparada exige reconciliação antes de executar a revisão.');
const target=(run:any,base:string,index:number)=>`${base.split('/')[0]}/o${run.objective_id.replaceAll('-','').slice(0,12)}r${run.base_revision+1}-base-${index}`;

async function assertPlanOwner(db:DB,run:any,planDigest:string){
  const row=(await db.query(`SELECT p.state,p.fence,p.base_revision,p.output,p.feedback,o.revision,o.mode,g.state goal_state
    FROM planning_runs p JOIN objectives o ON o.id=p.objective_id JOIN goals g ON g.id=o.id WHERE p.id=$1 AND o.id=$2`,
    [run.id,run.objective_id])).rows[0];
  if(!row||row.state!=='running'||row.fence!==run.fence||row.base_revision!==run.base_revision||
    row.revision!==run.base_revision||row.mode!=='execute'||['paused','cancelled','completed'].includes(row.goal_state)||
    preparationHash(row.output)!==planDigest||preparationHash(row.feedback??{})!==preparationHash(run.feedback??{}))throw failure('STALE_PLAN');
  await assertPlanningFeedbackCurrent(db,run);
  await assertSourceReplanCurrent(db,run);
  const bases=[...new Set((row.output?.tasks??[]).map((task:any)=>task.baseWorkspace))] as string[];
  await assertPlanningSourcesIdle(db,run,bases.map(base=>run.feedback?.sourceOverrides?.[base]??base));
}

async function recapture(db:DB,run:any,intent:Intent){
  if(intent.kind==='explicit_source')return recaptureExplicitSourceAuthority(db,run,intent.capture);
  if(!(await recaptureSourceReuseAuthority(db,intent.capture)).eligible)throw failure('SOURCE_REUSE_AUTHORITY_CHANGED');
}
async function refresh(intent:Intent,signal:AbortSignal){
  if(intent.kind==='explicit_source')return observeExplicitSource(intent.capture,
    (method,params,options)=>accountBridge.request(method,params,{...options,signal}));
  const result=await observe(intent.capture,signal);
  if(!result.eligible)throw failure('SOURCE_REUSE_AUTHORITY_CHANGED');
  return result;
}

/** All artifact/remote I/O stays outside admission transactions. A bounded
 * cohort is inspected sequentially: an overlong observation fails freshness
 * rather than renewing an old physical-stop proof with a heartbeat. */
async function observe(capture:SourceReuseCapture,signal:AbortSignal){
  const request:WorkspaceInspectionRequest=(method,params,options)=>accountBridge.request(method,params,{...options,signal});
  const artifacts=new Map<string,Buffer>();let total=0;
  for(const c of capture.contributors)for(const a of c.artifacts.filter(a=>['candidate.json','changes.patch'].includes(a.name))){
    total+=a.size;if(total>32*1024*1024)throw failure('SOURCE_REUSE_EVIDENCE_BOUND');
    const actual=await readArtifact(a.id);
    if(actual.run_id!==a.runId||actual.name!==a.name||actual.digest!==a.digest||actual.size!==a.size)
      throw failure('SOURCE_REUSE_ARTIFACT_CHANGED');
    artifacts.set(a.id,actual.bytes);
  }
  const baseInspection=await inspectWorkspaceGeneration(capture.rootWorkspace,request);
  const runInspections=new Map<string,unknown>();
  for(const c of capture.contributors)runInspections.set(c.runId,await inspectWorkerSourceBinding(c.runId,c.execution.workspace,request));
  const result=completeSourceReuseCapture(capture,{baseInspection,runInspections,artifactBytes:artifacts});
  return result.eligible?{...result,observationCheckedAt:Math.min(Date.parse(baseInspection.checkedAt),
    ...[...result.evidence.stops.values()].map(s=>s.checkedAt))}:result;
}

async function mark(run:any,index:number,state:Intent['state'],extra:Partial<Intent>={}){
  await tx(async db=>{await lock(db);
    const stored=(await db.query('SELECT value FROM settings WHERE id=$1',[key(run.id,index)])).rows[0]?.value as Intent|undefined;
    if(!stored||stored.state==='applied')return;
    await assertPlanOwner(db,run,stored.planDigest);
    const value={...stored,...extra,state,ownerFence:run.fence,updatedAt:new Date().toISOString()};
    await db.query('UPDATE settings SET value=$2 WHERE id=$1',[key(run.id,index),JSON.stringify(value)]);
    await event(db,'PLANNING_SOURCE_PREPARATION',run.objective_id,{planningRunId:run.id,baseIndex:index,
      preparationIdentity:stored.request.preparationIdentity,state,...(value.errorCode?{code:value.errorCode}:{})});
  });
}

/** A durable intent is read BEFORE considering a fallback. After reservation,
 * retries only inspect/reconcile that same UUID and exact source generation.
 * A transport timeout never permits legacy preparation or another origin. */
export async function preparePlanningSources(run:any,plan:ProposedPlan,signal:AbortSignal):Promise<PreparedPlanningSources>{
  const sourceReplan=await inspectSourceReplanCurrent(run);
  const request:WorkspaceInspectionRequest=(method,params,options)=>accountBridge.request(method,params,{...options,signal});
  const prepared=new Map<string,PreparedPlanningSource>(),bases=[...new Set(plan.tasks.map(t=>t.baseWorkspace))];
  const planDigest=preparationHash(plan);
  await tx(async db=>{await lock(db);await assertPlanOwner(db,run,planDigest);});
  for(let index=0;index<bases.length;index++){
    const base=bases[index],destination=target(run,base,index);
    let intent=(await pool.query('SELECT value FROM settings WHERE id=$1',[key(run.id,index)])).rows[0]?.value as Intent|undefined;
    if(!intent&&run.base_revision>0&&!run.feedback?.sourceOverrides?.[base]){
      const capture=await captureSourceReuseAuthority(pool,{goalId:run.objective_id,revision:run.base_revision,baseWorkspace:base});
      let result:any=capture;
      if(capture.eligible){
        try{result=await observe(capture.capture,signal);}catch(e:any){
          if(signal.aborted)throw e;
          result={eligible:false,reason:e.code??'INSPECTION_UNAVAILABLE'};
        }
      }
      if(result.eligible){
        const preparationIdentity=randomUUID();
        // A discarded planning run must not leave a bound baseline at the
        // legacy name used by a later plan for the same objective revision.
        const request={...sourceReusePreparation(result.proof,preparationIdentity),
          workspace:destination+'-p'+preparationIdentity.replaceAll('-','')};
        intent={version:1,planningRunId:run.id,goalId:run.objective_id,baseRevision:run.base_revision,baseIndex:index,
          baseWorkspace:base,planDigest,capture:result.capture,proof:result.proof,request,state:'reserved',ownerFence:run.fence,
          updatedAt:new Date().toISOString()};
        const proposal=intent;
        intent=await tx(async db=>{await lock(db);await assertPlanOwner(db,run,planDigest);
          const existing=(await db.query('SELECT value FROM settings WHERE id=$1',[key(run.id,index)])).rows[0]?.value;
          if(existing)return existing;
          await recapture(db,run,proposal);
          await db.query('INSERT INTO settings(id,value) VALUES($1,$2)',[key(run.id,index),JSON.stringify(proposal)]);
          await event(db,'PLANNING_SOURCE_RESERVED',run.objective_id,{planningRunId:run.id,baseIndex:index,
            preparationIdentity:request.preparationIdentity,proofDigest:proposal.proof.proofDigest,
            sourceWorkspace:request.sourceWorkspace,targetWorkspace:request.workspace,acceptance:false});
          return proposal;
        });
      }else await tx(db=>event(db,'PLANNING_SOURCE_REUSE_SKIPPED',run.objective_id,
        {planningRunId:run.id,baseIndex:index,reason:result.reason,acceptance:false}));
    }
    if(!intent){
      const capture=await captureExplicitSourceAuthority(pool,run,base);
      const observed=await observeExplicitSource(capture,request),preparationIdentity=randomUUID(),selected=observed.proof.selectedSource;
      if(sourceReplan&&(capture.authority!=='published_root'||preparationHash(selected)!==preparationHash(sourceReplan.payload.source)))
        throw failure('SOURCE_REPLAN_PUBLICATION_CHANGED');
      const preparation:SourcePreparationRequest={workspace:destination+'-p'+preparationIdentity.replaceAll('-',''),
        sourceWorkspace:selected.workspace,preparationIdentity,expectedSourceDigest:selected.candidateDigest,
        expectedSourceSnapshotDigest:selected.snapshotDigest,
        ...(selected.sourceGenerationKind==='unmarked'?{sourceGenerationKind:'unmarked' as const}:{})};
      const proposal:Intent={version:1,kind:'explicit_source',planningRunId:run.id,goalId:run.objective_id,
        baseRevision:run.base_revision,baseIndex:index,baseWorkspace:base,planDigest,capture,proof:observed.proof,
        request:preparation,state:'reserved',ownerFence:run.fence,updatedAt:new Date().toISOString()};
      intent=await tx(async db=>{await lock(db);await assertPlanOwner(db,run,planDigest);
        const existing=(await db.query('SELECT value FROM settings WHERE id=$1',[key(run.id,index)])).rows[0]?.value;
        if(existing)return existing;
        await recapture(db,run,proposal);
        await db.query('INSERT INTO settings(id,value) VALUES($1,$2)',[key(run.id,index),JSON.stringify(proposal)]);
        await event(db,'PLANNING_SOURCE_RESERVED',run.objective_id,{planningRunId:run.id,baseIndex:index,
          preparationIdentity,proofDigest:proposal.proof.proofDigest,sourceWorkspace:selected.workspace,
          targetWorkspace:preparation.workspace,kind:'explicit_source',acceptance:false});
        return proposal;
      });
    }
    if(!intent||intent.version!==1||intent.planningRunId!==run.id||intent.goalId!==run.objective_id||intent.baseIndex!==index||
      intent.baseRevision!==run.base_revision||intent.baseWorkspace!==base||intent.planDigest!==planDigest||
      intent.request.workspace!==destination+'-p'+intent.request.preparationIdentity.replaceAll('-','')||
      intent.state==='applied')throw failure('SOURCE_PREPARATION_INTENT_CHANGED');
    try{
      await tx(async db=>{await lock(db);await assertPlanOwner(db,run,planDigest);
        await recapture(db,run,intent!);});
      let inspection=await inspectWorkspacePreparation(intent.request,request);
      if(inspection?.exists===false&&inspection?.confirmed===true){
        // A successful old inspection is never a renewed stop observation.
        const beforeCopy=await refresh(intent,signal);
        if(beforeCopy.proof.proofDigest!==intent.proof.proofDigest)throw failure('SOURCE_PREPARATION_AUTHORITY_CHANGED');
        await tx(async db=>{await lock(db);await assertPlanOwner(db,run,planDigest);await recapture(db,run,intent!);});
        await mark(run,index,'preparing');
        await accountBridge.request('worker.prepare',intent.request,{signal,timeout:180000});
        inspection=await inspectWorkspacePreparation(intent.request,request);
      }
      verifySourcePreparation(intent.request,inspection,{requireUnchanged:true});
      // A long copy does not extend the producer's old stop observation.
      const refreshed=await refresh(intent,signal);
      if(refreshed.proof.proofDigest!==intent.proof.proofDigest)throw failure('SOURCE_PREPARATION_AUTHORITY_CHANGED');
      // Re-observe the actual destination after the producer checks; neither a
      // receipt nor the pre-copy timestamp extends its freshness.
      inspection=await inspectWorkspacePreparation(intent.request,request);
      const verified=verifySourcePreparation(intent.request,inspection,{requireUnchanged:true});
      await mark(run,index,'prepared',{prepared:verified,errorCode:undefined});
      intent={...intent,state:'prepared',ownerFence:run.fence,prepared:verified};
      const proofCheckedAt=refreshed.observationCheckedAt;
      prepared.set(base,{intent,prepared:verified,proofCheckedAt});
    }catch(e:any){
      await mark(run,index,'uncertain',{errorCode:/^[A-Z_]{3,100}$/.test(e.code??'')?e.code:'SOURCE_PREPARATION_UNKNOWN'}).catch(()=>{});
      if(signal.aborted)throw e;
      // A changed authority/receipt is not a transport outage. Preserve it for
      // supervised replanning instead of retrying the same proof indefinitely.
      if(['PROVIDER_TIMEOUT','BRIDGE_DISCONNECTED','SOURCE_BUSY','HOST_CAPACITY','STOP_UNCONFIRMED'].includes(e.code))
        throw failure('SOURCE_PREPARATION_RECONCILE');
      throw e;
    }
  }
  return prepared;
}

/** Called inside the planner's existing admission transaction, before any task
 * is inserted. No filesystem or provider I/O; application shares its commit. */
export async function assertPreparedPlanningSources(db:DB,run:any,plan:ProposedPlan,sources:PreparedPlanningSources){
  const replan=await assertSourceReplanCurrent(db,run);
  if(replan){
    const source=sources.get(replan.payload.request.sourceReplan.baseWorkspace);
    if(!source||source.intent.kind!=='explicit_source'||source.intent.capture.authority!=='published_root'||
      preparationHash(source.intent.proof.selectedSource)!==preparationHash(replan.payload.source))throw failure('SOURCE_REPLAN_PREPARATION_REQUIRED');
  }
  const stored=(await db.query("SELECT value FROM settings WHERE id LIKE $1",[`planning-source-preparation:${run.id}:%`])).rows;
  if(stored.length!==sources.size)throw failure('SOURCE_PREPARATION_REQUIRED');
  if(stored.length)await assertPlanOwner(db,run,preparationHash(plan));
  for(const row of stored){
    const current=row.value as Intent,source=sources.get(current.baseWorkspace);
    if(!source||current.state!=='prepared'||current.ownerFence!==run.fence||current.planDigest!==preparationHash(plan)||
      current.proof.proofDigest!==source.intent.proof.proofDigest||preparationHash(current.request)!==preparationHash(source.intent.request)||
      preparationHash(current.prepared)!==preparationHash(source.prepared)||Date.now()-source.proofCheckedAt>60000||
      source.proofCheckedAt>Date.now()||Date.now()-source.prepared.checkedAt>60000)throw failure('SOURCE_PREPARATION_STALE');
    await recapture(db,run,current);
  }
}

export function planningSourceExecution(source:PreparedPlanningSource|undefined,execution:any,preparationIdentity:string){
  if(!source)return {};
  const proof=source.intent.proof;
  const sourcePreparation=preparedBaselineBinding(source.prepared,execution.sourceWorkspace,execution.workspace,preparationIdentity);
  if(source.intent.kind==='explicit_source')return {
    planningSource:{version:1,planningRunId:source.intent.planningRunId,proofDigest:proof.proofDigest,
      preparationIdentity:source.intent.request.preparationIdentity,receiptDigest:source.prepared.receipt.receiptDigest,
      baselineWorkspace:source.intent.request.workspace,acceptance:false,finalVerificationRequired:true},
    ...(sourcePreparation?{sourcePreparation}:{})};
  const incorporatedArtifacts=source.intent.proof.incorporatedContributors.flatMap(c=>[c.candidateArtifact,c.patchArtifact]);
  return {sourceReuse:{version:1,planningRunId:source.intent.planningRunId,proofDigest:proof.proofDigest,
    preparationIdentity:source.intent.request.preparationIdentity,receiptDigest:source.prepared.receipt.receiptDigest,
    baselineWorkspace:source.intent.request.workspace,incorporatedArtifacts,acceptance:false,finalVerificationRequired:true},
    ...(sourcePreparation?{sourcePreparation}:{})};
}

export async function applyPreparedPlanningSources(db:DB,run:any,sources:PreparedPlanningSources){
  for(const {intent} of sources.values()){
    await db.query("UPDATE settings SET value=value||$2::jsonb WHERE id=$1",[key(run.id,intent.baseIndex),
      JSON.stringify({state:'applied',updatedAt:new Date().toISOString()})]);
    await event(db,intent.kind==='explicit_source'?'PLANNING_SOURCE_PREPARED':'PLANNING_SOURCE_REUSED',run.objective_id,{planningRunId:run.id,baseIndex:intent.baseIndex,
      proofDigest:intent.proof.proofDigest,preparationIdentity:intent.request.preparationIdentity,acceptance:false});
  }
}
