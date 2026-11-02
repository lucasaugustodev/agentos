import {createHash} from 'node:crypto';
import {pool,tx,lock,event,type DB} from './db.js';
import {DomainError} from './contracts.js';
import {inspectStopped,assertStopped,setContinuation} from './continuation.js';
import {runtimeInputs} from './execution-inputs.js';
import {requestRecoveryReview,appliedSupervisionOwnsContinuation,pendingSupervisionOwnsContinuation} from './recovery-supervision.js';

const canonical=(v:any):any=>Array.isArray(v)?v.map(canonical):v&&typeof v==='object'
  ?Object.fromEntries(Object.keys(v).sort().map(k=>[k,canonical(v[k])])):v;
export const transportDigest=(v:any)=>createHash('sha256').update(JSON.stringify(canonical(v))).digest('hex');
const equal=(a:any,b:any)=>transportDigest(a)===transportDigest(b);
const sha=/^[a-f0-9]{64}$/;
const uuid=/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/;
const failed=(code='TRANSPORT_RETRY_BINDING_CHANGED')=>new DomainError(code,'Retomada técnica exige reconciliação; trabalho e critérios preservados.',409);
const sorted=(xs:any[])=>xs.map(canonical).sort((a,b)=>JSON.stringify(a).localeCompare(JSON.stringify(b)));
const refs=(xs:any[]=[])=>sorted(xs.map(({id,name,digest,task_id,run_id,size,source})=>({id,name,digest,task_id,run_id,size,source})));

/** Explicit projection: only consumer attempt identities and their derived
 * digests change. Resolution authority/target/source IDs and hashes do not. */
export function transportInputProjection(m:any){
  const effective=m.effectiveSources;
  const selection=effective?.compositionSelection;
  const {runId:_run,runFence:_fence,instructionSnapshotDigest:_snapshot,digest:_digest,
    compositionSelection:_composition,...sourceBody}=effective??{};
  const {consumerRunId:_consumer,consumerRunFence:_consumerFence,digest:_selectionDigest,...compositionBody}=selection??{};
  const {runId:_selectedRun,runFence:_selectedFence,digest:_selectedDigest,...resolvedBody}=m.resolvedInputSelection??{};
  return {dependencyArtifacts:refs(m.dependencyArtifacts),executionInputs:refs(m.executionInputs),
    recoveryArtifacts:refs(m.recoveryArtifacts),recoveryArtifactAliases:sorted(m.recoveryArtifactAliases??[]),
    // The same immutable instruction archive is materialized afresh per run.
    // Its generated artifact UUID changes; name, bytes, revision and hashes do not.
    instructionArtifacts:sorted((m.instructionArtifacts??[]).map(({name,digest,size}:any)=>({name,digest,size}))),
    taskInstructions:m.taskInstructions?{...m.taskInstructions,history:m.taskInstructions.history?
      {name:m.taskInstructions.history.name,digest:m.taskInstructions.history.digest,size:m.taskInstructions.history.size}:null}:null,
    requiredMemories:sorted((m.selected??[]).filter((x:any)=>x.required).map(({id,hash}:any)=>({id,hash}))),
    sourceReuse:m.sourceReuse??null,
    effectiveSources:effective?{...sourceBody,...(selection?{compositionSelection:compositionBody}:{})}:null,
    resolvedInputSelection:m.resolvedInputSelection?resolvedBody:null};
}
function contract(t:any){
  // Continuation is an observation/routing projection. Every other execution
  // field, including inputs, preparation and supervision authority, is pinned.
  const {continuation:_continuation,...execution}=t.execution??{};
  return transportDigest({id:t.id,goalId:t.goal_id,revision:t.plan_revision,instructionRevision:t.instruction_revision_id,
    title:t.title,description:t.description,criteria:t.criteria,covers:t.covers,stage:t.stage,agentId:t.agent_id,
    resources:t.resources,requiredMemories:t.required_memories,recovery:t.recovery,maxAttempts:t.max_attempts,execution});
}

/** A generic error string or provider-authored body cannot authorize replay. */
export async function transportCause(db:DB,run:any){
  if(run.state!=='failed'||run.runtime!=='harness'||!run.context_id)return null;
  const rows=(await db.query(`SELECT id::text,payload FROM events WHERE entity_id=$1
    AND kind='MODEL_TRANSPORT_EXHAUSTED' AND payload->>'runId'=$2 ORDER BY id LIMIT 2`,[run.task_id,run.id])).rows;
  if(rows.length!==1)return null;
  const p=rows[0].payload;
  const exhausted=p.transportAttempt===undefined?p.generationAttempt===3:
    p.transportAttempt===3&&Number.isSafeInteger(p.generationAttempt)&&p.generationAttempt>=3;
  if(p.version!==1||p.source!=='runtime-generation-boundary'||p.runId!==run.id||p.runFence!==run.fence||
    p.contextVersion!==run.context_id||!uuid.test(p.requestId??'')||!exhausted||p.code!==run.error_code||
    !['connection_interrupted','connection_timeout','provider_unavailable'].includes(p.reason)||
    p.responseDelivered!==false||p.traceComplete!==true||p.pendingTools!==0||p.cancelled!==false)return null;
  return {eventId:rows[0].id,...p};
}
export async function hasTransportCause(db:DB,taskId:string,runId:string){
  const r=(await db.query('SELECT * FROM runs WHERE id=$1 AND task_id=$2',[runId,taskId])).rows[0];
  return r?!!await transportCause(db,r):false;
}
async function dependencies(db:DB,taskId:string){
  return (await db.query(`SELECT d.depends_on task_id,t.state,r.id run_id,r.fence,
    (SELECT passed FROM acceptances WHERE run_id=r.id ORDER BY created_at DESC,id DESC LIMIT 1) passed,
    (SELECT jsonb_agg(jsonb_build_object('id',a.id,'name',a.name,'digest',a.digest,'size',a.size) ORDER BY a.id)
      FROM artifacts a WHERE a.run_id=r.id AND a.name<>'task-instructions-history.json') artifacts
    FROM dependencies d JOIN tasks t ON t.id=d.depends_on
    LEFT JOIN LATERAL(SELECT id,fence FROM runs WHERE task_id=t.id ORDER BY attempt DESC LIMIT 1)r ON true
    WHERE d.task_id=$1 ORDER BY d.depends_on`,[taskId])).rows;
}
async function capture(db:DB,runId:string){
  const r=(await db.query('SELECT * FROM runs WHERE id=$1',[runId])).rows[0];
  if(!r)return null;
  const cause=await transportCause(db,r);if(!cause)return null;
  const t=(await db.query('SELECT * FROM tasks WHERE id=$1',[r.task_id])).rows[0];
  const o=(await db.query(`SELECT g.state,o.revision,o.mode,a.value->>'enabled' autonomy FROM goals g
    JOIN objectives o ON o.id=g.id LEFT JOIN settings a ON a.id='autonomy:'||g.project WHERE g.id=$1`,[t.goal_id])).rows[0];
  if(!o||!['active','failed'].includes(o.state)||o.mode!=='execute'||o.autonomy==='false'||
    t.recovery?.mode==='manual'||t.execution?.continuation?.owner==='user'||t.stage!=='integrate'||
    t.execution?.kind!=='harness'||t.execution?.readOnlySource||r.attempt!==t.attempt_count||!r.finished_at||
    t.plan_revision!==o.revision||r.instruction_snapshot?.schema!==1||r.instruction_snapshot.task?.id!==t.id||
    !equal(contract(t),contract(r.instruction_snapshot.task)))return null;
  if((await db.query(`SELECT 1 FROM runs WHERE task_id=$1 AND (attempt>$3 OR state IN('queued','running','cancelling','reconciling'))
    UNION ALL SELECT 1 FROM leases l JOIN runs q ON q.id=l.run_id WHERE q.task_id=$1
    UNION ALL SELECT 1 FROM events WHERE kind='CANCEL_REQUESTED' AND payload->>'runId'=$2
    UNION ALL SELECT 1 FROM settings WHERE (id LIKE 'candidate-validation:%' OR id LIKE 'published-rebase:%')
      AND value->>'taskId'=$1::text AND value->>'state' IN('proposed','freezing','frozen','running','uncertain','preparing','prepared','applying')
    UNION ALL SELECT 1 FROM supervision_cases WHERE task_id=$1 AND (state IN('open','decided','delegated','waiting_evidence')
      OR (state='applied' AND applied_attempt=$3 AND source_run_id=$2::uuid)) LIMIT 1`,[t.id,r.id,r.attempt])).rowCount)return null;
  const c=(await db.query('SELECT * FROM contexts WHERE id=$1 AND run_id=$2',[r.context_id,r.id])).rows[0];
  if(!c||!equal(c.manifest.runIdentity,{taskId:t.id,goalId:t.goal_id,runId:r.id,runFence:r.fence}))return null;
  const deps=await dependencies(db,t.id);
  if(deps.some(d=>d.state!=='accepted'||d.passed!==true))return null;
  const inputProjection=transportInputProjection(c.manifest);
  const authority={taskId:t.id,goalId:t.goal_id,sourceRunId:r.id,sourceFence:r.fence,sourceContextId:c.id,
    sourceAttempt:r.attempt,revision:t.plan_revision,contractDigest:contract(t),dependencies:deps,inputProjection,
    sourceContextDigest:transportDigest({manifest:c.manifest,content:c.content}),cause};
  return {t,r,c,authority,fingerprint:transportDigest(authority)};
}
const stopProjection=(p:any)=>({runId:p.runId,executionRunId:p.executionRunId,workspace:p.workspace,checkedAt:p.checkedAt,
  process:{unit:p.process.unit,stopped:p.process.stopped,mainPid:p.process.mainPid,populated:p.process.populated,activeState:p.process.activeState},
  candidate:{baseDigest:p.candidate?.baseDigest,candidateDigest:p.candidate?.candidateDigest}});
// Filter owners before LIMIT, then repeat authority checks under the lock.
// Otherwise the same supervised rows can indefinitely hide later ready work.
const unowned=`NOT EXISTS(SELECT 1 FROM supervision_cases owner WHERE owner.task_id=t.id
  AND (owner.state IN('open','decided','delegated','waiting_evidence')
    OR (owner.state='applied' AND owner.applied_attempt=r.attempt AND owner.source_run_id=r.id)))`;
async function review(db:DB,taskId:string,runId:string,code:string,reason:string,inspect:typeof inspectStopped,stopProof?:any){
  const allowed=(await db.query(`SELECT 1 FROM tasks t JOIN runs r ON r.task_id=t.id AND r.attempt=t.attempt_count
    JOIN goals g ON g.id=t.goal_id WHERE t.id=$1 AND r.id=$2 AND g.state IN('active','failed')
      AND t.execution#>>'{continuation,owner}' IS DISTINCT FROM 'user'
      AND t.recovery->>'mode' IS DISTINCT FROM 'manual'
      AND NOT EXISTS(SELECT 1 FROM leases l JOIN runs x ON x.id=l.run_id WHERE x.task_id=t.id)
      AND NOT EXISTS(SELECT 1 FROM events WHERE kind='CANCEL_REQUESTED' AND payload->>'runId'=r.id::text)`,[taskId,runId])).rowCount;
  if(allowed&&!await appliedSupervisionOwnsContinuation(db,taskId,runId)){
    const owned=await pendingSupervisionOwnsContinuation(db,taskId,runId);if(owned)return owned;
    try{return await requestRecoveryReview(taskId,runId,code,reason,{stopProof:stopProof??await inspect(runId,false)});}
    catch{await setContinuation(taskId,runId,'waiting','Revisão técnica aguardando confirmação física do executor.',30,
      {owner:'controller',reasonCode:'TRANSPORT_RETRY_PROOF_PENDING'});}
  }
  return null;
}

/** Finite, no model/dispatch here. SSH and byte reads are outside admission lock. */
export async function recoverTransportTasks(inspect:typeof inspectStopped=inspectStopped){
  // A deferred/unavailable supervisor remains visible and is retried through
  // its existing idempotent review path, never through a new planner revision.
  const held=(await pool.query(`SELECT r.id,t.id task_id,t.reason,z.status FROM tasks t JOIN runs r ON r.task_id=t.id AND r.attempt=t.attempt_count
    JOIN recoveries z ON z.source_run_id=r.id AND z.strategy='transport_resume'
    WHERE t.stage='integrate' AND t.state='recovery_blocked' AND r.state='failed'
      AND EXISTS(SELECT 1 FROM events WHERE kind='MODEL_TRANSPORT_EXHAUSTED' AND payload->>'runId'=r.id::text)
      AND ${unowned}
    ORDER BY t.updated_at LIMIT 4`)).rows;
  for(const row of held)if(await hasTransportCause(pool,row.task_id,row.id)&&!await appliedSupervisionOwnsContinuation(pool,row.task_id,row.id))
    await review(pool,row.task_id,row.id,row.status==='blocked'?'TRANSPORT_RETRY_EXHAUSTED':'TRANSPORT_RETRY_AUTHORITY_CHANGED',
      row.reason??'Disponibilidade do transporte requer supervisão.',inspect);
  const rows=(await pool.query(`SELECT r.id FROM tasks t JOIN runs r ON r.task_id=t.id AND r.attempt=t.attempt_count
    WHERE t.stage='integrate' AND t.state IN('failed','recovery_blocked') AND r.state='failed'
      AND EXISTS(SELECT 1 FROM events e WHERE e.kind='MODEL_TRANSPORT_EXHAUSTED' AND e.payload->>'runId'=r.id::text)
      AND NOT EXISTS(SELECT 1 FROM recoveries z WHERE z.source_run_id=r.id)
      AND ${unowned}
      AND (t.execution#>>'{continuation,nextAt}' IS NULL OR t.execution#>>'{continuation,nextAt}'<=$1)
    ORDER BY t.updated_at LIMIT 4`,[new Date().toISOString()])).rows;
  const scheduled:string[]=[];
  for(const row of rows){
    const before=await capture(pool,row.id);
    if(!before){
      const task=(await pool.query('SELECT task_id FROM runs WHERE id=$1',[row.id])).rows[0];
      if(task&&await hasTransportCause(pool,task.task_id,row.id)&&!await appliedSupervisionOwnsContinuation(pool,task.task_id,row.id))
        await review(pool,task.task_id,row.id,'TRANSPORT_RETRY_AUTHORITY_CHANGED',
          'Falha de transporte comprovada, mas autoridade ou insumos mudaram. Conferir o vínculo atual; não recompor o lote por indisponibilidade.',inspect);
      continue;
    }
    let proof:any,stopped=false;
    try{
      proof=await inspect(row.id,false);assertStopped(proof,before.r,before.t);stopped=true;
      // This uses the immutable source instruction and the real artifact reader.
      const {readArtifact}=await import('./service.js');
      await runtimeInputs(before.r.instruction_snapshot.task,before.c.manifest,readArtifact,{transport:'referenced-v1'});
      if(!sha.test(proof.candidate?.candidateDigest??'')||!sha.test(proof.candidate?.baseDigest??''))throw failed();
    }catch{
      if(stopped){
        await review(pool,before.t.id,row.id,'TRANSPORT_RETRY_INPUTS_CHANGED',
          'Executor parado, mas integridade dos insumos ou da candidata não foi comprovada. Supervisão deve reconciliar a fonte; nenhuma retomada autorizada.',inspect,proof);
        continue;
      }
      await setContinuation(before.t.id,row.id,'waiting','Transporte interrompido: aguardando prova física e integridade dos insumos.',30,
        {owner:'controller',reasonCode:'TRANSPORT_RETRY_PROOF_PENDING'});
      continue;
    }
    const outcome=await tx(async db=>{
      await lock(db);const now=await capture(db,row.id);
      if(!now||now.fingerprint!==before.fingerprint||!['failed','recovery_blocked'].includes(now.t.state)||
        (await db.query('SELECT 1 FROM recoveries WHERE source_run_id=$1',[row.id])).rowCount)return null;
      assertStopped(proof,now.r,now.t);
      const resourceAttempts=Number((await db.query("SELECT count(*) FROM recoveries WHERE target_task_id=$1 AND strategy='resource_wait'",[now.t.id])).rows[0].count);
      const exhausted=now.t.attempt_count-resourceAttempts>=now.t.max_attempts;
      const checkpoint={version:1,...now.authority,authorityDigest:now.fingerprint,stopProof:stopProjection(proof),
        nextAttempt:now.r.attempt+1,acceptance:false};
      const reason=exhausted?'Transporte indisponível: limite existente de tentativas atingido; supervisão deve decidir disponibilidade, sem invalidar o plano.'
        :'Transporte interrompido: retomada da mesma tarefa, workspace e insumos após parada confirmada.';
      await db.query(`INSERT INTO recoveries(source_run_id,task_id,target_task_id,strategy,status,reason,checkpoint)
        VALUES($1,$2,$2,'transport_resume',$3,$4,$5)`,[row.id,now.t.id,exhausted?'blocked':'scheduled',reason,JSON.stringify(checkpoint)]);
      await event(db,'TRANSPORT_RETRY_PREPARED',now.t.id,{sourceRunId:row.id,sourceFence:now.r.fence,
        receiptDigest:transportDigest(checkpoint),exhausted,reason,acceptance:false});
      await db.query(`UPDATE tasks SET state=$2,reason=$3,next_attempt_at=CASE WHEN $2='retry_scheduled' THEN now()+interval '30 seconds' ELSE NULL END,
        updated_at=now() WHERE id=$1`,[now.t.id,exhausted?'recovery_blocked':'retry_scheduled',reason]);
      await db.query("UPDATE goals SET state='active' WHERE id=$1 AND state='failed'",[now.t.goal_id]);
      return {taskId:now.t.id,exhausted,reason};
    });
    if(outcome?.exhausted)await review(pool,outcome.taskId,row.id,'TRANSPORT_RETRY_EXHAUSTED',outcome.reason,inspect,proof);
    else if(outcome)scheduled.push(outcome.taskId);
  }
  return scheduled;
}

async function receipt(db:DB,taskId:string,sourceRunId:string){
  const row=(await db.query("SELECT * FROM recoveries WHERE source_run_id=$1 AND task_id=$2 AND target_task_id=$2 AND strategy='transport_resume'",[sourceRunId,taskId])).rows[0];
  if(!row)return null;
  const p=row.checkpoint;
  if(p?.version!==1||p.taskId!==taskId||p.sourceRunId!==sourceRunId||p.acceptance!==false||
    !(await db.query(`SELECT 1 FROM events WHERE kind='TRANSPORT_RETRY_PREPARED' AND entity_id=$1
      AND payload->>'sourceRunId'=$2 AND payload->>'receiptDigest'=$3`,[taskId,sourceRunId,transportDigest(p)])).rowCount)throw failed();
  return row;
}
/** The ordinary scheduler still owns quota, capacity, leases and run creation. */
export async function transportRetryAdmission(db:DB,taskId:string){
  const r=(await db.query('SELECT * FROM runs WHERE task_id=$1 ORDER BY attempt DESC LIMIT 1',[taskId])).rows[0];
  if(!r)return null;
  const row=await receipt(db,taskId,r.id);if(!row||row.status!=='scheduled')return null;
  const current=await capture(db,r.id);
  if(!current||current.fingerprint!==row.checkpoint.authorityDigest||
    !['retry_scheduled','waiting_resource','waiting_quota','waiting_configuration','ready'].includes(current.t.state))throw failed();
  return row.checkpoint;
}
export async function admitTransportRetry(db:DB,p:any,runId:string,fence:string){
  const source=(await db.query('SELECT checkpoint FROM runs WHERE id=$1',[p.sourceRunId])).rows[0];
  await db.query('UPDATE runs SET checkpoint=$2 WHERE id=$1',[runId,JSON.stringify({
    ...(source?.checkpoint??{}),kind:'automatic_recovery',sourceRunId:p.sourceRunId,preservedWorkspace:p.stopProof.workspace,
    transportResume:{version:1,sourceRunId:p.sourceRunId,receiptDigest:transportDigest(p)},
    nextAction:'Resume the preserved partial workspace. This was a proven model transport outage, not a rejected implementation. Inspect existing edits and evidence; do not reapply already present patches or redo accepted producers. Complete the original task and tests. No acceptance is granted.'})]);
  await event(db,'TRANSPORT_RETRY_ADMITTED',p.taskId,{sourceRunId:p.sourceRunId,sourceFence:p.sourceFence,
    runId,runFence:fence,attempt:p.nextAttempt,receiptDigest:transportDigest(p)});
}
export async function bindTransportRetryContext(db:DB,run:any,task:any,manifest:any,contextId:string){
  const marker=run.checkpoint?.transportResume;if(!marker)return null;
  const row=await receipt(db,task.id,marker.sourceRunId),p=row?.checkpoint;
  if(!p||marker.receiptDigest!==transportDigest(p)||run.attempt!==p.nextAttempt||contract(task)!==p.contractDigest||
    !equal(transportInputProjection(manifest),p.inputProjection)||!equal(await dependencies(db,task.id),p.dependencies)||
    !(await db.query(`SELECT 1 FROM events WHERE kind='TRANSPORT_RETRY_ADMITTED' AND entity_id=$1
      AND payload->>'runId'=$2 AND payload->>'runFence'=$3 AND payload->>'receiptDigest'=$4`,[task.id,run.id,run.fence,marker.receiptDigest])).rowCount)
    throw failed('TRANSPORT_RETRY_INPUTS_CHANGED');
  const binding={version:1,sourceRunId:p.sourceRunId,runId:run.id,runFence:run.fence,contextId,receiptDigest:marker.receiptDigest};
  await event(db,'TRANSPORT_RETRY_CONTEXT_BOUND',task.id,binding);
  return binding;
}
/** Before dispatch, no stale stop receipt or changed partial source can launch. */
export async function assertTransportRetryCurrent(input:any,inspect:typeof inspectStopped=inspectStopped){
  const marker=input.run.checkpoint?.transportResume;if(!marker)return;
  const row=await receipt(pool,input.task.id,marker.sourceRunId),p=row?.checkpoint;
  if(!p||marker.receiptDigest!==transportDigest(p))throw failed();
  const old=(await pool.query('SELECT * FROM runs WHERE id=$1',[p.sourceRunId])).rows[0];
  const proof=await inspect(p.sourceRunId,false);assertStopped(proof,old,input.task);
  if(!equal(proof.candidate?.candidateDigest,p.stopProof.candidate.candidateDigest)||
    !equal(proof.candidate?.baseDigest,p.stopProof.candidate.baseDigest))throw failed('TRANSPORT_RETRY_SOURCE_CHANGED');
  await tx(async db=>{
    await lock(db);
    const r=(await db.query('SELECT r.*,t.state task_state,t.execution,g.state goal_state FROM runs r JOIN tasks t ON t.id=r.task_id JOIN goals g ON g.id=t.goal_id WHERE r.id=$1',[input.run.id])).rows[0];
    if(!r||r.fence!==input.run.fence||r.context_id!==input.context.id||r.state!=='running'||r.task_state!=='running'||r.goal_state!=='active'||input.signal.aborted||
      (await db.query("SELECT 1 FROM events WHERE kind='CANCEL_REQUESTED' AND payload->>'runId'=ANY($1::text[])",[[p.sourceRunId,r.id]])).rowCount)throw failed();
    assertStopped(proof,old,input.task);
    const t=(await db.query('SELECT * FROM tasks WHERE id=$1',[r.task_id])).rows[0];
    const c=(await db.query('SELECT * FROM contexts WHERE id=$1 AND run_id=$2',[r.context_id,r.id])).rows[0];
    const source=(await db.query('SELECT * FROM runs WHERE id=$1',[p.sourceRunId])).rows[0];
    const sourceContext=(await db.query('SELECT * FROM contexts WHERE id=$1 AND run_id=$2',[p.sourceContextId,p.sourceRunId])).rows[0];
    const o=(await db.query(`SELECT o.revision,a.value->>'enabled' autonomy FROM objectives o
      LEFT JOIN goals g ON g.id=o.id LEFT JOIN settings a ON a.id='autonomy:'||g.project WHERE o.id=$1`,[t.goal_id])).rows[0];
    if(!c||!o||!source||!sourceContext||source.fence!==p.sourceFence||source.context_id!==p.sourceContextId||
      !equal(await transportCause(db,source),p.cause)||transportDigest({manifest:sourceContext.manifest,content:sourceContext.content})!==p.sourceContextDigest||
      r.attempt!==p.nextAttempt||t.attempt_count!==r.attempt||o.revision!==p.revision||o.autonomy==='false'||t.recovery?.mode==='manual'||t.execution?.continuation?.owner==='user'||
      contract(t)!==p.contractDigest||!equal(await dependencies(db,t.id),p.dependencies)||
      c.content!==input.context.content||!equal(c.manifest,input.context.manifest)||!equal(transportInputProjection(c.manifest),p.inputProjection)||
      !equal(c.manifest.transportRetry,{version:1,sourceRunId:p.sourceRunId,runId:r.id,runFence:r.fence,contextId:r.context_id,receiptDigest:marker.receiptDigest})||
      (await db.query(`SELECT 1 FROM jsonb_array_elements_text($1::jsonb) resource WHERE NOT EXISTS
        (SELECT 1 FROM leases l WHERE l.resource=resource AND l.run_id=$2)`,[JSON.stringify(t.resources),r.id])).rowCount)throw failed();
  });
}
