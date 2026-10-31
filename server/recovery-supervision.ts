import {createHash,randomUUID} from 'node:crypto';
import {tx,lock,event,type DB} from './db.js';
import {providerAvailabilityFailure} from '../shared/recovery.js';
import {supervisionGenerationReady} from './supervision-generations.js';
import {assertStopped} from './continuation.js';
import {runtimeProgressCode} from './runtime-progress-contract.js';
import {operatorRecoveryCode,operatorRecoveryNextAction,hasOperatorRecoveryIntent} from './operator-recovery-contract.js';
import {operatorOpenCodeRecoveryNextAction} from './operator-opencode-recovery-contract.js';
import {hasOperatorOpenCodeRecoveryIntent} from './operator-opencode-recovery-contract.js';
import {appliedOperatorOpenCodeReviewIsFresh,recordOperatorOpenCodeReviewReady} from './operator-opencode-review.js';
import {scheduledSupervisionAdmission} from './supervised-admission.js';
import {verificationRoutingFailureEvidence,appendVerificationRoutingEvidence} from './verification-routing-failure.js';

/** Mirrors the existing supervision hold for this current attempt. A pending
 * case may originate from an older attempt, but still owns the same task.
 * This only prevents generic recovery; it grants neither acceptance nor launch.
 * Admission callers must recheck inside their transaction before any effects. */
export async function pendingSupervisionOwnsContinuation(db:DB,taskId:string,runId:string) {
  return (await db.query(`SELECT c.id case_id,c.state FROM tasks t JOIN goals g ON g.id=t.goal_id
    JOIN settings s ON s.id='supervision:'||g.id
    LEFT JOIN settings a ON a.id='autonomy:'||g.project
    JOIN LATERAL(SELECT id,attempt FROM runs WHERE task_id=t.id ORDER BY attempt DESC LIMIT 1)r ON true
    JOIN supervision_cases c ON c.task_id=t.id AND c.goal_id=t.goal_id
    WHERE t.id=$1 AND r.id=$2 AND r.attempt=t.attempt_count
      AND g.state='active' AND s.value->>'enabled'='true' AND COALESCE(a.value->>'enabled','true')='true'
      AND (t.recovery->>'mode' IS DISTINCT FROM 'manual' OR a.value->>'enabled'='true')
      AND c.state IN('open','decided','delegated','waiting_evidence')
    ORDER BY c.created_at,c.id LIMIT 1`,[taskId,runId])).rows[0] ?? null;
}

/** An applied decision owns its exact stopped attempt while admission waits for
 * quota/capacity. This is routing, not permission to launch: the supervised
 * consumer still validates stop proof, dependencies and the full contract.
 * The artifact-recovery trigger can project an applied Harness or OpenCode decision back
 * to recovering_artifacts. Its exact applied owner still takes precedence over
 * generic collection, without granting that precedence to unsupported states.
 * Call again under the review transaction lock before reopening a case. */
export async function appliedSupervisionOwnsContinuation(db:DB,taskId:string,runId:string) {
  return Boolean((await db.query(`SELECT 1 FROM tasks t JOIN goals g ON g.id=t.goal_id
    JOIN settings s ON s.id='supervision:'||g.id
    LEFT JOIN settings a ON a.id='autonomy:'||g.project
    JOIN LATERAL(SELECT id,attempt,state,error_code,runtime FROM runs WHERE task_id=t.id ORDER BY attempt DESC LIMIT 1)r ON true
    JOIN supervision_cases c ON c.task_id=t.id AND c.goal_id=t.goal_id
    WHERE t.id=$1 AND r.id=$2 AND r.attempt=t.attempt_count
      AND r.state IN('failed','submitted') AND (t.state IN('failed','submitted','recovery_blocked')
        OR (t.state='recovering_artifacts' AND r.state='failed'
          AND (r.runtime='harness' OR (r.runtime='opencode' AND t.execution->>'kind'='opencode'))
          AND r.error_code IN('ARTIFACT_PATH','ARTIFACT_NAME','ARTIFACT_NAME_COLLISION','ARTIFACT_MISSING','RESULT_FORMAT','RESERVED_ARTIFACT_NAME')))
      AND COALESCE(r.error_code,'') !~ 'DIGEST|CORRUPT|INVALID_WRITE_SCOPE|INVALID_WORKSPACE'
      AND g.state='active' AND s.value->>'enabled'='true' AND COALESCE(a.value->>'enabled','true')='true'
      AND (t.recovery->>'mode' IS DISTINCT FROM 'manual' OR a.value->>'enabled'='true')
      AND t.execution->>'kind' IN('harness','opencode')
      AND NOT(COALESCE(t.execution->>'readOnlySource','false')='true' AND t.stage IN('verify','review'))
      AND c.state='applied' AND c.applied_attempt=r.attempt
      AND c.id::text IN(t.execution->'supervision'->>'caseId',t.execution->'supervisionHandoff'->>'caseId')
      AND COALESCE(CASE WHEN t.execution->'supervisionHandoff'->>'caseId'=c.id::text
        THEN t.execution->'supervisionHandoff'->>'sourceRunId' ELSE t.execution->'supervision'->>'sourceRunId' END,
        c.source_run_id::text)=r.id::text
      AND NOT EXISTS(SELECT 1 FROM runs live WHERE live.task_id=t.id AND live.state IN('queued','running','reconciling','cancelling'))
      AND NOT EXISTS(SELECT 1 FROM events WHERE kind='CANCEL_REQUESTED' AND payload->>'runId'=r.id::text)
      AND NOT EXISTS(SELECT 1 FROM supervision_cases pending WHERE pending.task_id=t.id
        AND pending.state IN('open','decided','delegated','waiting_evidence')) LIMIT 1`,[taskId,runId])).rowCount);
}

/** Ownership of a stopped technical failure; never admits another execution. */
export async function requestRecoveryReview(taskId:string,runId:string,code:string,message:string,
  options:{stopProof?:any;routingFailureCode?:string}={}) {
  if(([runtimeProgressCode,operatorRecoveryCode].includes(code)||code.startsWith('TRANSPORT_RETRY_')) && !options.stopProof)return null;
  return tx(async db=>{
    await lock(db);
    const operatorRecovery=code===operatorRecoveryCode;
    const stoppedTransport=code.startsWith('TRANSPORT_RETRY_')&&!!options.stopProof;
    const stoppedProgress=([runtimeProgressCode,operatorRecoveryCode].includes(code)||stoppedTransport) && !!options.stopProof;
    const task=(await db.query(`SELECT t.id,t.goal_id,t.state,t.recovery,t.attempt_count,g.state goal_state FROM tasks t JOIN goals g ON g.id=t.goal_id
      JOIN settings s ON s.id='supervision:'||g.id LEFT JOIN settings a ON a.id='autonomy:'||g.project
      WHERE t.id=$1 AND (g.state='active' OR ($2::boolean AND g.state='failed')) AND s.value->>'enabled'='true'
      AND COALESCE(a.value->>'enabled','true')='true'
      AND (t.recovery->>'mode' IS DISTINCT FROM 'manual' OR a.value->>'enabled'='true') FOR UPDATE OF t,g`,[taskId,stoppedProgress])).rows[0];
    if(!task || ['accepted','cancelled','superseded','queued','running','cancelling','reconciling'].includes(task.state))return null;
    // An applied decision can arrive while the generic inspector is outside
    // the lock. Its missing resume token must not revoke that valid decision.
    if((code==='NO_FAILED_CHECK' || code===runtimeProgressCode || operatorRecovery) &&
      await appliedSupervisionOwnsContinuation(db,taskId,runId)){
      if(!operatorRecovery)return null;
      const administrative=(await db.query('SELECT * FROM runs WHERE id=$1 AND task_id=$2',[runId,taskId])).rows[0];
      if(!administrative||!await hasOperatorOpenCodeRecoveryIntent(db,administrative))return null;
      const applied=(await db.query(`SELECT c.* FROM supervision_cases c JOIN tasks t ON t.id=c.task_id
        WHERE c.task_id=$1 AND c.source_run_id=$2 AND c.state='applied' AND c.applied_attempt=t.attempt_count
          AND c.id::text IN(t.execution#>>'{supervision,caseId}',t.execution#>>'{supervisionHandoff,caseId}')
        ORDER BY c.updated_at DESC,c.id DESC LIMIT 1`,[taskId,runId])).rows[0];
      // A generic revision predating the administrative evidence cannot refresh
      // a planner's context. Preserve it and create a separate exact generation.
      if(!applied||await appliedOperatorOpenCodeReviewIsFresh(db,applied))return null;
    }
    const run=(await db.query('SELECT id,task_id,state,error_code,attempt,fence FROM runs WHERE task_id=$1 ORDER BY attempt DESC LIMIT 1',[taskId])).rows[0];
    if(run?.id!==runId || !['failed','submitted'].includes(run.state) || providerAvailabilityFailure(run.error_code))return null;
    if(code.startsWith('TRANSPORT_RETRY_')){
      const {hasTransportCause}=await import('./transport-task-recovery.js');
      if(!await hasTransportCause(db,taskId,runId)||await appliedSupervisionOwnsContinuation(db,taskId,runId)||
        (await db.query(`SELECT 1 FROM tasks WHERE id=$1
        AND (execution#>>'{continuation,owner}'='user' OR recovery->>'mode'='manual')
        UNION ALL SELECT 1 FROM leases l JOIN runs r ON r.id=l.run_id WHERE r.task_id=$1 LIMIT 1`,[taskId])).rowCount)return null;
    }
    // An inspection may finish after the exact applied decision has queued
    // the next attempt. Its old failure is no new recovery episode. This is
    // checked inside the lock, before any new case or goal reactivation.
    if(await scheduledSupervisionAdmission(db,taskId,runId))return null;
    let operatorReceipt:any;
    if(operatorRecovery){
      const recorded=(await db.query('SELECT checkpoint FROM runs WHERE id=$1',[runId])).rows[0];
      if(!recorded||!await hasOperatorRecoveryIntent(db,{...run,checkpoint:recorded.checkpoint}))return null;
      if(recorded.checkpoint.nativeStopReceipt?.runFence!==run.fence||
        !(await db.query(`SELECT 1 FROM events WHERE kind='DETACHED_RUN_STOP_RECONCILED' AND entity_id=$1
          AND payload->>'runId'=$2 AND payload->>'cancelled'='false'
          AND payload#>>'{receipt,runFence}'=$3`,[taskId,runId,run.fence])).rowCount)return null;
      operatorReceipt=recorded.checkpoint.operatorRecoveryStop;
    }
    if(stoppedProgress || task.goal_state==='failed'){
      if(!stoppedProgress || run.state!=='failed' || (!stoppedTransport&&run.error_code!==code) || run.attempt!==task.attempt_count)return null;
      const snapshot=(await db.query('SELECT instruction_snapshot FROM runs WHERE id=$1',[runId])).rows[0]?.instruction_snapshot;
      if(snapshot?.schema!==1 || snapshot.task?.id!==taskId || snapshot.task?.goal_id!==task.goal_id || !snapshot.task?.execution?.workspace)return null;
      try {assertStopped(options.stopProof,{...run,instruction_snapshot:snapshot},task);}catch{return null;}
    }
    if((await db.query(`SELECT 1 FROM runs WHERE task_id=$1 AND state IN('queued','running','cancelling','reconciling')
      UNION ALL SELECT 1 FROM events WHERE kind='CANCEL_REQUESTED' AND payload->>'runId'=$2`,[taskId,runId])).rowCount)return null;
    const requestKey='recovery:'+createHash('sha256').update(code).digest('hex')+
      (operatorReceipt?.request.runtime==='opencode'?':'+operatorReceipt.request.requestId:'');
    const operatorEvidence=operatorRecovery?{kind:operatorRecoveryCode,runId,requestId:operatorReceipt.request.requestId,
      requestedAt:operatorReceipt.requestedAt,administrative:true,gameFailureClaimed:false,
      ...(operatorReceipt.request.runtime==='opencode'?{runtime:'opencode',
        observedAt:(await db.query('SELECT clock_timestamp()::text at')).rows[0].at}:{}),
      evidenceRefs:operatorReceipt.request.evidenceRefs,operatorSupplied:true,contextOnly:true,
      verifiedByServer:false,stopConfirmed:true,nextAction:operatorReceipt.request.runtime==='opencode'
        ?operatorOpenCodeRecoveryNextAction:operatorRecoveryNextAction}:null;
    const routingEvidence=code==='VERIFICATION_ROUTING_FAILED' && options.routingFailureCode
      ? await verificationRoutingFailureEvidence(db,taskId,runId,options.routingFailureCode):null;
    const reactivate=async(caseId:string)=>{
      if(task.goal_state!=='failed')return;
      await db.query("UPDATE goals SET state='active' WHERE id=$1 AND state='failed'",[task.goal_id]);
      await event(db,'GOAL_ACTIVE',task.goal_id,{source:operatorRecovery?'stopped_operator_recovery_review':stoppedTransport?'stopped_transport_review':'stopped_runtime_progress_review',caseId,sourceRunId:runId});
    };
    const existing=(await db.query(`SELECT id,state,source_run_id,request_key FROM supervision_cases WHERE task_id=$1
      AND state IN('open','decided','delegated','waiting_evidence') ORDER BY created_at LIMIT 1`,[taskId])).rows[0];
    if(existing){
      if(operatorRecovery && existing.source_run_id!==runId)return null;
      if(operatorEvidence){
        const appended=await db.query(`UPDATE supervision_cases SET evidence=evidence||$2::jsonb,updated_at=clock_timestamp()
          WHERE id=$1 AND source_run_id=$3 AND state IN('open','decided','delegated','waiting_evidence')
          AND NOT EXISTS(SELECT 1 FROM jsonb_array_elements(evidence)e
            WHERE e->>'kind'=$4 AND e->>'requestId'=$5)`,
          [existing.id,JSON.stringify([operatorEvidence]),runId,operatorRecoveryCode,operatorReceipt.request.requestId]);
        if(appended.rowCount)await event(db,'SUPERVISION_EVIDENCE_RECORDED',taskId,
          {caseId:existing.id,goalId:task.goal_id,sourceRunId:runId,code:operatorRecoveryCode,
            fingerprint:operatorReceipt.requestDigest,kind:operatorRecoveryCode});
        await recordOperatorOpenCodeReviewReady(db,taskId,runId,existing.id,operatorReceipt);
      }
      if(routingEvidence && existing.source_run_id===runId)
        await appendVerificationRoutingEvidence(db,existing.id,routingEvidence);
      if(task.goal_state==='failed'){
        if(existing.source_run_id!==runId || existing.request_key!==requestKey)return null;
        await reactivate(existing.id);
      }
      return {caseId:existing.id,state:existing.state};
    }
    const recorded=(await db.query(`SELECT id,state FROM supervision_cases
      WHERE task_id=$1 AND source_run_id=$2 AND request_key=$3`,[taskId,runId,requestKey])).rows[0];
    if(recorded)return null; // Closed history is not a currently actionable review.
    if(!await supervisionGenerationReady(db))return null;
    // Preserve the decision which actually ran. A new technical cause receives
    // its own idempotent generation; polling cannot reopen fulfilled requests.
    const previous=(await db.query(`SELECT id FROM supervision_cases WHERE task_id=$1 AND source_run_id=$2
      ORDER BY created_at DESC,id DESC LIMIT 1`,[taskId,runId])).rows[0];
    const caseId=randomUUID();
    await db.query(`INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,reason,evidence,request_key,parent_case_id)
      VALUES($1,$2,$3,$4,$5,$6,$7,$8)`,[caseId,task.goal_id,taskId,runId,message,
      JSON.stringify([{kind:'RECOVERY_REVIEW_REQUIRED',runId,code},...(operatorEvidence?[operatorEvidence]:[]),
        ...(routingEvidence?[routingEvidence]:[])]),requestKey,previous?.id??null]);
    if(operatorReceipt)await recordOperatorOpenCodeReviewReady(db,taskId,runId,caseId,operatorReceipt);
    await db.query("UPDATE tasks SET state='recovery_blocked' WHERE id=$1",[taskId]);
    // A lone failed task makes refreshGoal project the objective to failed.
    // Reopen only the proven stopped-progress review, atomically with its new
    // case; this does not start a model or reuse an already fulfilled case.
    await reactivate(caseId);
    await event(db,'SUPERVISION_REQUESTED',taskId,{caseId,goalId:task.goal_id,sourceRunId:runId,code,reason:message,
      requestKey,parentCaseId:previous?.id??null});
    return {caseId,state:'open'};
  });
}
