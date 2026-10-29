import { randomUUID } from 'node:crypto';
import { mkdir, writeFile, readFile } from 'node:fs/promises';
import path from 'node:path';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import type { PoolClient } from 'pg';
import { pool, tx, lock, event, type DB } from './db.js';
import { config } from './config.js';
import { bridgeScript } from './bridge-path.js';
import { hasStoragePreflightReceipt } from './not-started.js';
import { hash } from './memory.js';
import { quotaFor } from './accounts.js';
import { instructionTask } from './task-instructions.js';
import { reconcileUnstartedPreflight } from './preflight-recovery.js';
import { readArtifact } from './service.js';
import { recoverOpenCodeRun } from './opencode-recovery.js';
import {
  adoptRecoveredArtifacts,
  collectStoppedArtifacts,
  packagingFailure,
} from './artifact-recovery.js';
import { chooseWorkforceRoute } from './workforce-policy.js';
import { requireSchemaReady } from './schema-readiness.js';
import { readContinuationCandidates } from './continuation-candidates.js';
import { reconcileWorkforces } from './workforce.js';
import { assertSupervisedResume, continuationSignature } from './supervision-handoff.js';
import { requestProgressReplan } from './retry-progress.js';
import {reconcileVerificationRevisions} from './verification-revision-recovery.js';
import {providerAvailabilityFailure,PLAN_STALL_REASON} from '../shared/recovery.js';
import {requestRecoveryReview,appliedSupervisionOwnsContinuation,pendingSupervisionOwnsContinuation} from './recovery-supervision.js';
import {verificationRoutingCode} from './verification-routing-failure.js';
import {interruptedVerification,interruptedVerificationReason} from './verification-interruption.js';
import { scheduledSupervisionAdmission } from './supervised-admission.js';
import { runtimeProgressCode } from './runtime-progress-contract.js';
import {operatorRecoveryCode} from './operator-recovery-contract.js';
import { preservedAcceptedAuxiliaryIds } from './accepted-auxiliary.js';
import { protocolReportRepairInstruction } from '../bridge/protocol-report-contract.mjs';
import { SOURCE_BINARY_CHECK } from '../bridge/source-patch-contract.mjs';
import {
  requestSemanticReplan,
  semanticRetryCode,
  semanticEvidenceCode,
  currentSemanticDecision,
} from './semantic-retry.js';

const exec = promisify(execFile);
const liveStates = ['queued', 'running', 'cancelling', 'reconciling'];
export function repairInstruction(code: string, expectedTestFile?: string) {
  if (code === 'OPENCODE_SANDBOX_OOM' || code === 'HARNESS_SANDBOX_OOM')
    return 'O systemd confirmou oom-kill neste executor isolado. Inspecione o teste e seus subprocessos antes de repetir: restrinja leituras/tamanhos e use arquivos regulares em data/ para fault injection. /dev/full pode fornecer zeros sem fim quando lido; não o use como arquivo que o servidor lê. Provoque falha de write/rename por fixture regular local, preservando a intenção e assertivas da regressão. Faça cleanup somente dos PIDs próprios. Não aumente recursos nem declare resultado para esconder leitura infinita; preserve o checkpoint e execute o caso mínimo corrigido.';
  if (/RESULT_FORMAT|ARTIFACT_|RESERVED_ARTIFACT|DELIVERY_INVALID/.test(code))
    return (
      'Recupere os arquivos já produzidos. Não refaça o trabalho. ' +
      protocolReportRepairInstruction(code, expectedTestFile) +
      ' Confira o contrato de saída, liste arquivos existentes e entregue somente JSON válido com summary e artifacts. Cada path deve ser relativo a /workspace, por exemplo evidence/protocol-report.json. Não use /workspace no path. Inclua todos os arquivos exigidos, logs reais e relatório de limitações; não transforme testes que falham em aprovados.'
    );
  if (code === 'PATCH_BINARY_SOURCE')
    return 'A entrega anterior foi rejeitada só porque o patch de fonte inclui um arquivo não-texto (imagem, banco, upload ou saída de teste) adicionado, alterado ou APAGADO fora de data/, evidence/ e assets/; o código em si não foi julgado. Não refaça a implementação. Rode ' +
      SOURCE_BINARY_CHECK +
      ' e siga cada linha: RESTORE devolve o arquivo da base intacto (binário que já estava na base não pode ser apagado nem alterado), MOVE leva um binário novo para data/ (por exemplo data/uploads/), ajustando o código que o grava. Repita até imprimir OK, rode os testes e entregue.';
  if (/AUTH|QUOTA|RATE_LIMIT/.test(code))
    return 'A conta anterior não conseguiu continuar. Retome os arquivos existentes usando esta nova rota. Execute somente o trabalho restante e preserve os critérios e evidências reais.';
  return 'Inspecione o checkpoint e o workspace preservado. Reproduza primeiro o menor caso que falha; escolha uma hipótese diferente da tentativa anterior, corrija dentro do escopo e execute os testes relevantes. Registre comandos e resultados reais. Entregue os artefatos exigidos, inclusive falhas e limitações. Não reinicie o trabalho do zero e não marque sucesso para apenas liberar a fila. Trabalhe de forma incremental e verificavel: escreva primeiro um esqueleto minimo dos arquivos de entrega e sele evidencias a cada etapa concluida, em vez de auditar a arvore inteira antes da primeira escrita. Limite a exploracao aos arquivos do contrato e das dependencias diretas. Nao crie bancos, containers ou dados de runtime locais (nada de pgdata, docker ou DATABASE_URL inventada): validacao com PostgreSQL e somente pelo request_validation protegido; sem ele, registre a limitacao em evidence/.';
}
export function repeatedWithoutProgress(history: any[], signature: string) {
  return history.length >= 6 && history.slice(0, 6).every((e) => e.signature === signature);
}

export async function setContinuation(
  taskId: string,
  runId: string,
  state: string,
  message: string,
  seconds = 10,
  ownership: {owner?:'scheduler'|'supervisor'|'controller'|'user';reasonCode?:string;routingCode?:string;caseId?:string;
    humanRequest?:{kind:'authentication'|'information'|'authorization';question:string}} = {},
) {
  return tx(async (db) => {
    await lock(db);
    const t = (await db.query('SELECT * FROM tasks WHERE id=$1', [taskId])).rows[0];
    const latest = (
      await db.query('SELECT id FROM runs WHERE task_id=$1 ORDER BY attempt DESC LIMIT 1', [taskId])
    ).rows[0];
    if (!t || latest?.id !== runId || ['accepted', 'cancelled', 'superseded'].includes(t.state))
      return false;
    // The scan may have observed this failed run before a supervisor queued
    // its next attempt. Preserve that current authority under the same lock;
    // overwriting queued here would erase the requester's final guard.
    if (await scheduledSupervisionAdmission(db, taskId, runId) ||
      await appliedSupervisionOwnsContinuation(db, taskId, runId)) return false;
    // Pending supervision also owns the failure. Keep collection available for
    // its decision, but do not overwrite that ownership with a generic retry
    // inspection or advertise a queue which the supervision trigger will hold.
    if (['checking','waiting_slot','waiting_quota','needs_action'].includes(state) &&
      await pendingSupervisionOwnsContinuation(db, taskId, runId)) return false;
    const value = {
      state,
      message,
      runId,
      updatedAt: new Date().toISOString(),
      nextAt: new Date(Date.now() + seconds * 1000).toISOString(),
      owner: state==='needs_action' || state==='supervising'?'supervisor':'scheduler',
      ...ownership,
    };
    await db.query(
      "UPDATE tasks SET execution=jsonb_set(execution,'{continuation}',$2::jsonb) WHERE id=$1",
      [taskId, JSON.stringify(value)],
    );
    if (
      t.execution?.continuation?.state !== state ||
      t.execution?.continuation?.message !== message ||
      t.execution?.continuation?.owner !== value.owner
    )
      await event(db, 'CONTINUATION_STATUS', taskId, value);
    return true;
  });
}

export type StopInspectionExecutor=(file:string,args:string[],options:{timeout:number;maxBuffer:number;windowsHide:boolean})=>Promise<{stdout:string}>;
export async function inspectStopped(runId: string, native: boolean, execute:StopInspectionExecutor = exec) {
  const recorded = (
    await pool.query(
      `SELECT r.checkpoint,r.state,r.error_code,r.instruction_snapshot,r.task_id,t.goal_id,t.execution,
    EXISTS(SELECT 1 FROM events e WHERE e.kind=$2
      AND e.entity_id=t.id::text AND e.payload->>'runId'=r.id::text
      AND e.payload->>'confirmed'='true') not_started
    FROM runs r JOIN tasks t ON t.id=r.task_id WHERE r.id=$1`,
      [runId, native ? 'OPENCODE_NOT_STARTED' : 'HARNESS_NOT_STARTED'],
    )
  ).rows[0];
  if(recorded && !native && !recorded.not_started)
    recorded.not_started=await reconcileUnstartedPreflight(runId,readArtifact);
  const frozenTask = recorded && instructionTask(recorded,
    {id:recorded.task_id,goal_id:recorded.goal_id,execution:recorded.execution});
  const executionId =
    recorded?.checkpoint?.kind === 'artifact_recovery' ? recorded.checkpoint.sourceRunId : runId;
  const notStarted =
    executionId === runId &&
    ['failed','reconciling'].includes(recorded?.state) &&
    recorded?.not_started &&
    recorded?.execution?.kind === (native ? 'opencode' : 'harness');
  const { stdout } = await execute(
    'ssh',
    [
      '-T',
      '-o',
      'BatchMode=yes',
      '-o',
      'ConnectTimeout=8',
      config.bridgeHost,
      '/opt/deepseek-harness/node/bin/node',
      native && !notStarted
        ? bridgeScript('inspect-opencode-recovery.mjs')
        : bridgeScript('inspect-stopped-run.mjs'),
      executionId,
      ...(notStarted ? ['--not-started', frozenTask.execution.workspace] : []),
    ],
    { timeout: 45000, maxBuffer: 24 * 1024 * 1024, windowsHide: true },
  );
  const proof = JSON.parse(stdout);
  if (native && notStarted) {
    if (proof.state !== 'not_started') throw Error('NOT_STARTED_RECEIPT_CONFLICT');
    return {
      ...proof,
      runId,
      executionRunId: executionId,
      runtime: 'opencode',
      notStarted: true,
      state: {
        runId: executionId,
        runtime: 'opencode',
        workspace: proof.workspace,
        state: 'not_started',
        code: recorded.error_code,
      },
    };
  }
  return { ...proof, runId, executionRunId: executionId };
}

/** Close a never-started attempt after fresh stop inspection. This releases its
 * reservation; semantic recovery still belongs to the existing supervisor. */
export async function reconcileUnstartedHarnessRun(runId:string,inspect=inspectStopped) {
  const initial=(await pool.query(`SELECT r.id FROM runs r JOIN tasks t ON t.id=r.task_id
    JOIN goals g ON g.id=t.goal_id WHERE r.id=$1 AND r.runtime='harness' AND r.state='reconciling'
    AND t.state='reconciling' AND t.attempt_count=r.attempt AND g.state='active'`,[runId])).rows[0];
  if(!initial)return false;
  if((await inspect(runId,false)).state!=='not_started')return false;
  return tx(async db=>{
    await lock(db);
    const r=(await db.query('SELECT * FROM runs WHERE id=$1 FOR UPDATE',[runId])).rows[0];
    const t=r&&(await db.query('SELECT * FROM tasks WHERE id=$1 FOR UPDATE',[r.task_id])).rows[0];
    if(!r||r.runtime!=='harness'||r.state!=='reconciling'||t?.state!=='reconciling'||t.attempt_count!==r.attempt)return false;
    if(!(await db.query(`SELECT 1 FROM goals g LEFT JOIN settings a ON a.id='autonomy:'||g.project
      WHERE g.id=$1 AND g.state='active' AND COALESCE(a.value->>'enabled','true')='true'
      AND ($2::text IS DISTINCT FROM 'manual' OR a.value->>'enabled'='true')`,[t.goal_id,t.recovery?.mode??null])).rowCount)return false;
    if(!(await db.query(`SELECT 1 FROM events WHERE kind='HARNESS_NOT_STARTED' AND entity_id=$1
      AND payload->>'runId'=$2 AND payload->>'confirmed'='true'`,[t.id,r.id])).rowCount)return false;
    if((await db.query(`SELECT 1 FROM events WHERE payload->>'runId'=$1 AND kind IN
      ('CANCEL_REQUESTED','HARNESS_STARTED','RUNTIME_STARTED','MODEL_RESPONSE','TOOL_CALL','TOOL_RESULT','HARNESS_TOOL_EVENT')
      UNION ALL SELECT 1 FROM runs WHERE task_id=$2 AND id<>$1::uuid AND state IN('queued','running','cancelling','reconciling') LIMIT 1`,[r.id,t.id])).rowCount)return false;
    // Refresh after acquiring the scheduler lock; a queued inspection must not
    // authorize releasing resources from an expired observation.
    const proof=await inspect(r.id,false);
    if(proof.state!=='not_started')return false;
    assertStopped(proof,r,t);
    await db.query("UPDATE runs SET state='failed',finished_at=clock_timestamp() WHERE id=$1",[r.id]);
    await db.query("UPDATE tasks SET state='failed',reason='Runtime não iniciou; encerramento confirmado. Aguarda recuperação supervisionada.',updated_at=clock_timestamp() WHERE id=$1",[t.id]);
    await db.query('DELETE FROM leases WHERE run_id=$1',[r.id]);
    await event(db,'UNSTARTED_RUN_RECONCILED',t.id,{runId:r.id,attempt:r.attempt,code:r.error_code,
      process:proof.process,workspace:proof.workspace,checkedAt:proof.checkedAt,
      candidateDigest:proof.candidate?.candidateDigest,baseDigest:proof.candidate?.baseDigest,semanticResumeRequired:true});
    await event(db,'RUN_FAILED',t.id,{runId:r.id,code:r.error_code,notStarted:true,stopped:true});
    return true;
  });
}

export function assertStopped(proof: any, run: any, task: any) {
  // A prepared continuation can already point the task at another workspace.
  // Confirm the process belonging to the immutable attempt being inspected.
  const attemptTask = run.instruction_snapshot ? instructionTask(run, task) : task;
  const p = proof?.process,
    age = Date.now() - Date.parse(proof?.checkedAt);
  if (
    proof?.runId !== run.id ||
    proof.workspace !== attemptTask.execution.workspace ||
    p?.unit !==
      `agentos-run-${run.checkpoint?.kind === 'artifact_recovery' ? run.checkpoint.sourceRunId : run.id}` ||
    p.stopped !== true ||
    p.mainPid !== 0 ||
    p.populated !== false ||
    !['inactive', 'failed'].includes(p.activeState) ||
    !Number.isFinite(age) ||
    age < -5000 ||
    age > 60000
  )
    throw Error('STOP_UNCONFIRMED');
}

export function chooseRepairRoute(task: any, run: any, accounts: any[], now = Date.now()) {
  const candidates = accounts.filter(
    (a) =>
      a.enabled &&
      a.bridge_id &&
      !['agy', 'local'].includes(a.provider) &&
      (!task.allowed_accounts?.length || task.allowed_accounts.includes(a.id)) &&
      !['exhausted', 'auth_required'].includes(quotaFor(a, a.model).status) &&
      (quotaFor(a, a.model).status !== 'unknown' || a.allow_unknown),
  );
  const failedRoutes = task.execution?.continuationRoutes ?? [];
  const blocked = (id: string, model: string) =>
    failedRoutes.some(
      (r: any) => r.accountId === id && r.model === model && Date.parse(r.until) > now,
    );
  const switching = /AUTH|QUOTA|RATE_LIMIT/.test(run.error_code ?? '') || task.attempt_count >= 3;
  const buildRoutes = (keepFailedRoute: boolean) => candidates.flatMap((a) => {
    const desired =
      task.execution?.profile === 'browser' || ['verify', 'integrate'].includes(task.stage)
        ? 'gpt-6-sol'
        : 'gpt-6-luna';
    const models = [
      ...new Set(
        [
          ...(switching
            ? [desired, a.model, task.model]
            : [task.model, run.model, desired, a.model]),
        ].filter(Boolean),
      ),
    ];
    return models
      .filter(
        (m) =>
          a.models.includes(m) &&
          !blocked(a.id, m) &&
          !(switching && !keepFailedRoute && a.id === run.account_id && m === run.model),
      )
      .map((m) => ({
        accountId: a.id,
        model: m,
        score:
          (a.provider === 'codex' ? 0 : 10) +
          (m === desired ? 0 : 2) +
          (a.id === run.account_id ? 1 : 0),
      }));
  });
  // Switching away from the route that just failed is a preference, not a
  // precondition. An installation with one account and one model has no other
  // route to move to, so insisting on one leaves the repair with no candidate
  // at all and the task waits for an account that is already connected. The
  // cooldown in blocked() still applies, so a route that keeps failing is held
  // back by time rather than by the absence of an alternative.
  const routes = buildRoutes(false);
  const usable = routes.length ? routes : buildRoutes(true);
  return usable.sort((a, b) => a.score - b.score)[0] ?? null;
}

export async function routeVerificationRepair(sourceId: string) {
  return tx(async (db) => {
    await lock(db);
    // A rejected admission belongs to its fresh supervisor review. Automatic
    // candidate rechecks must not close that case while it is being reviewed.
    const supervised=(await db.query("SELECT to_regclass('supervision_cases') relation")).rows[0].relation;
    if (supervised && (await db.query(`SELECT 1 FROM tasks t JOIN supervision_cases c ON c.task_id=t.id
      AND c.source_run_id=(SELECT id FROM runs WHERE task_id=t.id ORDER BY attempt DESC LIMIT 1)
      WHERE t.id=$1 AND c.state IN('open','decided','delegated','waiting_evidence')
      AND EXISTS(SELECT 1 FROM events e WHERE e.kind='RUN_ADMISSION_BLOCKED' AND e.entity_id=t.id::text
        AND e.payload->>'goalId'=t.goal_id::text
        AND e.payload->>'instructionRevisionId' IS NOT DISTINCT FROM t.instruction_revision_id::text)`,
      [sourceId])).rowCount) return null;
    try {
      return await routeVerificationRepairInTransaction(db, sourceId);
    } catch (error: any) {
      if (error.message !== 'VERIFICATION_RECHECK_REQUIRED') throw error;
      await queueVerificationRecheckInTransaction(db, error.verificationRecheck);
      return null;
    }
  });
}

async function verificationCandidate(db: DB, runId: string) {
  const refs = (
    await db.query(
      "SELECT id,run_id,name,digest,size FROM artifacts WHERE run_id=$1 AND name='candidate.json'",
      [runId],
    )
  ).rows;
  if (refs.length !== 1) throw Error('VERIFICATION_CANDIDATE_UNBOUND');
  const ref = refs[0];
  try {
    if (!Number.isSafeInteger(ref.size) || ref.size < 0 || ref.size > 8 * 1024 * 1024)
      throw Error('INVALID_SIZE');
    const bytes = await readFile(path.join(config.dataDir, 'artifacts', ref.id));
    if (bytes.length !== ref.size || hash(bytes) !== ref.digest) throw Error('INVALID_DIGEST');
    const candidate = JSON.parse(bytes.toString());
    if (
      !/^[0-9a-f]{64}$/.test(candidate?.candidateDigest) ||
      !/^[0-9a-f]{64}$/.test(candidate?.baseDigest)
    )
      throw Error('INVALID_CANDIDATE');
    return { ref, candidate };
  } catch {
    throw Error('VERIFICATION_CANDIDATE_INVALID');
  }
}

/** Recheck replaces only a stopped QA attempt, never its implementation or evidence. */
export async function queueVerificationRecheckInTransaction(db: DB, decision: any) {
  return queueVerificationRecheck(db,decision);
}

/** Only a byte-verified applied revision plus fresh stop proof can restart an
 * interrupted readonly QA. No failed attempt is relabeled as submitted. */
export async function queueInterruptedVerificationRecheckInTransaction(db:DB,decision:any,
 authority:{caseId:string;proof:any}) {
  return queueVerificationRecheck(db,{...decision,reason:interruptedVerificationReason},authority);
}

async function queueVerificationRecheck(db:DB,decision:any,interruption?:{caseId:string;proof:any}) {
  await lock(db);
  const source = (
    await db.query('SELECT * FROM tasks WHERE id=$1 FOR UPDATE', [decision.sourceTaskId])
  ).rows[0];
  const latest = (
    await db.query('SELECT * FROM runs WHERE task_id=$1 ORDER BY attempt DESC LIMIT 1 FOR UPDATE', [
      decision.sourceTaskId,
    ])
  ).rows[0];
  if (
    !source ||
    source.attempt_count !== latest?.attempt ||
    latest.id !== decision.sourceRunId ||
    !source.execution?.readOnlySource ||
    (interruption?!interruptedVerification(latest):latest.state !== 'submitted') ||
    !(interruption?['failed','recovery_blocked']:['submitted', 'recovery_blocked']).includes(source.state)
  )
    throw Error('TASK_CHANGED');
  if(interruption){
    if(!['verify','review'].includes(source.stage))throw Error('TASK_CHANGED');
    const c=(await db.query('SELECT * FROM supervision_cases WHERE id=$1 AND task_id=$2 AND goal_id=$3',
      [interruption.caseId,source.id,source.goal_id])).rows[0];
    const actions=c?.decision?.actions??[c?.decision];
    if(!Array.isArray(actions)||actions.length!==1||actions[0]?.kind!=='revise'||actions[0]?.sourceTaskId||
      c?.source_run_id!==latest.id||!await currentSemanticDecision(db,source,latest,c))
      throw Error('VERIFICATION_INTERRUPTION_DECISION_UNBOUND');
    assertStopped(interruption.proof,latest,source);
    if((await db.query('SELECT 1 FROM leases l JOIN runs r ON r.id=l.run_id WHERE r.task_id=$1 LIMIT 1',[source.id])).rowCount||
      (await db.query('SELECT passed FROM acceptances WHERE task_id=$1 AND run_id=$2 ORDER BY created_at DESC,id DESC LIMIT 1',[source.id,latest.id])).rows[0]?.passed===true)
      throw Error('BRANCH_BUSY');
  }
  if (
    (await db.query("SELECT 1 FROM goals WHERE id=$1 AND state='active'", [source.goal_id]))
      .rowCount !== 1
  )
    throw Error('GOAL_NOT_ACTIVE');
  if (
    (
      await db.query('SELECT 1 FROM runs WHERE task_id=$1 AND state=ANY($2::text[])', [
        source.id,
        liveStates,
      ])
    ).rowCount ||
    (
      await db.query(
        "SELECT 1 FROM events WHERE kind='CANCEL_REQUESTED' AND payload->>'runId'=$1",
        [latest.id],
      )
    ).rowCount
  )
    throw Error('BRANCH_BUSY');
  const target = (
    await db.query(
      "SELECT t.id,r.id run_id FROM dependencies d JOIN tasks t ON t.id=d.depends_on JOIN runs r ON r.task_id=t.id AND r.attempt=t.attempt_count WHERE d.task_id=$1 AND t.id=$2 AND t.goal_id=$3 AND t.execution->>'workspace'=$4 AND COALESCE(t.execution->>'readOnlySource','false')='false' AND r.id=$5 AND r.state='submitted'",
      [
        source.id,
        decision.targetTaskId,
        source.goal_id,
        source.execution.sourceWorkspace,
        decision.targetRunId,
      ],
    )
  ).rows[0];
  if (!target) throw Error('TASK_CHANGED');
  const priorCandidate=(await db.query("SELECT 1 FROM artifacts WHERE run_id=$1 AND name='candidate.json' LIMIT 1",[latest.id])).rowCount;
  // An interrupted turn may have published no candidate artifact. Never use
  // partial output as a result: the fresh QA binds the accepted implementation
  // below. If a candidate exists, its complete original integrity guard stays.
  if(!interruption||priorCandidate){
    const tested = await verificationCandidate(db, latest.id);
    if (tested.candidate.baseDigest !== tested.candidate.candidateDigest)
      throw Error('VERIFICATION_CANDIDATE_INVALID');
  }
  if(interruption && (await db.query(`SELECT 1 FROM tasks t JOIN runs r ON r.task_id=t.id AND r.attempt=t.attempt_count
    WHERE t.id=$1 AND t.goal_id=$2 AND t.state='accepted' AND r.id=$3 AND r.state='submitted'
    AND (SELECT passed FROM acceptances WHERE task_id=t.id AND run_id=r.id ORDER BY created_at DESC,id DESC LIMIT 1)=true
    AND NOT EXISTS(SELECT 1 FROM runs active WHERE active.task_id=t.id AND active.state=ANY($4::text[]))
    AND NOT EXISTS(SELECT 1 FROM leases l JOIN runs held ON held.id=l.run_id WHERE held.task_id=t.id)`,
    [target.id,source.goal_id,target.run_id,liveStates])).rowCount!==1)throw Error('TASK_CHANGED');
  const candidate = await verificationCandidate(db, target.run_id);
  if (decision.candidateDigest && decision.candidateDigest !== candidate.candidate.candidateDigest)
    throw Error('TASK_CHANGED');
  decision = { ...decision, candidateDigest: candidate.candidate.candidateDigest };
  const workspace = source.execution.workspace.split('/')[0] + '/oqa-' + latest.id;
  const execution = {
    ...source.execution,
    workspace,
    verificationRecheck: { ...decision, workspace },
  };
  delete execution.continuation;
  // Resolve this obsolete QA's pending case before its task update. The supervision
  // trigger correctly prevents admission while an unresolved case still owns it.
  if ((await db.query("SELECT to_regclass('supervision_cases') relation")).rows[0].relation) {
    await db.query(
      "UPDATE supervision_cases SET state='applied',applied_attempt=$3,reason='QA anterior substituída por nova verificação da candidata atual.',updated_at=now() WHERE task_id=$1 AND source_run_id=$2 AND state IN('open','decided','delegated','waiting_evidence')",
      [source.id, latest.id, latest.attempt],
    );
  }
  await db.query(
    "UPDATE tasks SET state='blocked_dependencies',execution=$2,max_attempts=GREATEST(max_attempts,attempt_count+1),next_attempt_at=now(),reason='A candidata mudou ou este QA já foi encaminhado; aguardando nova verificação independente.',updated_at=now() WHERE id=$1",
    [source.id, JSON.stringify(execution)],
  );
  await event(db, 'VERIFICATION_RECHECK_QUEUED', source.id, { ...decision, workspace });
}

/** Reuse the exact QA-to-implementation boundary inside an existing transaction. */
export async function routeVerificationRepairInTransaction(db: DB, sourceId: string) {
  await lock(db);
  const source = (await db.query('SELECT * FROM tasks WHERE id=$1 FOR UPDATE', [sourceId])).rows[0];
  if (
    !source?.execution?.readOnlySource ||
    !['submitted', 'recovery_blocked'].includes(source.state)
  )
    throw Error('TASK_CHANGED');
  const run = (
    await db.query('SELECT * FROM runs WHERE task_id=$1 ORDER BY attempt DESC LIMIT 1', [sourceId])
  ).rows[0];
  if (run?.state !== 'submitted' || run.attempt !== source.attempt_count)
    throw Error('TASK_CHANGED');
  if (
    (await db.query("SELECT 1 FROM goals WHERE id=$1 AND state='active'", [source.goal_id]))
      .rowCount !== 1
  )
    throw Error('GOAL_NOT_ACTIVE');
  if (
    (
      await db.query(
        "SELECT 1 FROM events WHERE kind='CANCEL_REQUESTED' AND payload->>'runId'=$1",
        [run.id],
      )
    ).rowCount
  )
    throw Error('TASK_CHANGED');
  const acceptance = (
    await db.query(
      'SELECT id,passed,checks FROM acceptances WHERE run_id=$1 ORDER BY created_at DESC,id DESC LIMIT 1',
      [run.id],
    )
  ).rows[0];
  const checks = acceptance?.checks ?? [];
  if (acceptance?.passed !== false) throw Error('NO_FAILED_CHECK');
  if (!checks.some((c: any) => !c.passed && c.type !== 'review')) throw Error('NO_FAILED_CHECK');
  const target = (
    await db.query(
      "SELECT t.* FROM dependencies d JOIN tasks t ON t.id=d.depends_on WHERE d.task_id=$1 AND t.execution->>'workspace'=$2 AND COALESCE(t.execution->>'readOnlySource','false')='false' ORDER BY t.updated_at DESC LIMIT 1 FOR UPDATE OF t",
      [sourceId, source.execution.sourceWorkspace],
    )
  ).rows[0];
  if (!target) throw Error('REPAIR_TARGET_MISSING');
  const branch = (
    await db.query(
      'WITH RECURSIVE b(id) AS (SELECT $1::uuid UNION SELECT d.task_id FROM dependencies d JOIN b ON b.id=d.depends_on) SELECT t.* FROM b JOIN tasks t ON t.id=b.id',
      [target.id],
    )
  ).rows;
  if (branch.some((t) => [...liveStates, 'cancelled'].includes(t.state)))
    throw Error('BRANCH_BUSY');
  if (
    (
      await db.query(
          `SELECT 1 FROM runs r JOIN tasks t ON t.id=r.task_id WHERE r.task_id=ANY($1::uuid[])
           AND (r.state=ANY($2::text[]) OR (r.attempt=t.attempt_count AND EXISTS(
             SELECT 1 FROM events e WHERE e.kind='CANCEL_REQUESTED' AND e.entity_id=t.id::text AND e.payload->>'runId'=r.id::text))) LIMIT 1`,
        [branch.map((t) => t.id), liveStates],
      )
    ).rowCount
  )
    throw Error('BRANCH_BUSY');
  const targetRun = (
    await db.query('SELECT * FROM runs WHERE task_id=$1 ORDER BY attempt DESC LIMIT 1 FOR UPDATE', [
      target.id,
    ])
  ).rows[0];
  if (
    !targetRun ||
    targetRun.state !== 'submitted' ||
    targetRun.attempt !== target.attempt_count ||
    target.goal_id !== source.goal_id
  )
    throw Error('REPAIR_TARGET_MISSING');
  const tested = await verificationCandidate(db, run.id);
  if (tested.candidate.baseDigest !== tested.candidate.candidateDigest)
    throw Error('VERIFICATION_CANDIDATE_INVALID');
  const consumed =
    Boolean(
      (
        await db.query(
          "SELECT 1 FROM events WHERE kind='VERIFICATION_REPAIR_ROUTED' AND entity_id=$1 AND payload->>'sourceRunId'=$2 AND payload->>'targetTaskId'=$3 LIMIT 1",
          [sourceId, run.id, target.id],
        )
      ).rowCount,
    ) || target.execution?.repairFeedback?.sourceRunId === run.id;
  // Legacy receipts lack candidate fields, but still consumed this exact negative QA.
  if (consumed && target.state !== 'accepted') {
    await db.query(
      "UPDATE tasks SET state='blocked_dependencies',reason='Aguarda a correção já encaminhada por este QA.' WHERE id=$1",
      [sourceId],
    );
    return target.id;
  }
  const recheck = (reason: string, candidateDigest: string | null = null) =>
    Object.assign(Error('VERIFICATION_RECHECK_REQUIRED'), {
      verificationRecheck: {
        version: 1,
        reason,
        sourceTaskId: sourceId,
        sourceRunId: run.id,
        sourceAttempt: run.attempt,
        targetTaskId: target.id,
        targetRunId: targetRun.id,
        targetAttempt: targetRun.attempt,
        candidateDigest,
      },
    });
  if (consumed) throw recheck('QA_ALREADY_ROUTED');
  const frozen = run.instruction_snapshot?.task;
  if (
    frozen &&
    (frozen.id !== sourceId ||
      frozen.goal_id !== source.goal_id ||
      frozen.execution?.readOnlySource !== true ||
      frozen.execution?.workspace !== source.execution.workspace ||
      frozen.execution?.sourceWorkspace !== source.execution.sourceWorkspace)
  )
    throw Error('TASK_CHANGED');
  const context = (
    await db.query('SELECT manifest FROM contexts WHERE id=$1 AND run_id=$2', [
      run.context_id,
      run.id,
    ])
  ).rows[0];
  const inputs = context?.manifest?.dependencyArtifacts;
  const refs = Array.isArray(inputs)
    ? inputs.filter((a: any) => a.task_id === target.id && a.name === 'candidate.json')
    : [];
  if (refs.length !== 1) throw Error('VERIFICATION_CANDIDATE_UNBOUND');
  if (
    typeof refs[0].id !== 'string' ||
    !/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(refs[0].id) ||
    typeof refs[0].digest !== 'string' ||
    !/^[0-9a-f]{64}$/.test(refs[0].digest)
  )
    throw Error('VERIFICATION_CANDIDATE_INVALID');
  const binding = (
    await db.query(
      "SELECT a.*,r.task_id,r.attempt FROM artifacts a JOIN runs r ON r.id=a.run_id WHERE a.id=$1 AND r.task_id=$2 AND a.name='candidate.json'",
      [refs[0].id, target.id],
    )
  ).rows[0];
  if (!binding || binding.digest !== refs[0].digest) throw Error('VERIFICATION_CANDIDATE_INVALID');
  const bound = await verificationCandidate(db, binding.run_id);
  if (bound.ref.id !== binding.id) throw Error('VERIFICATION_CANDIDATE_INVALID');
  const current =
    binding.run_id === targetRun.id ? bound : await verificationCandidate(db, targetRun.id);
  if (
    binding.run_id !== targetRun.id ||
    binding.attempt !== target.attempt_count ||
    binding.id !== current.ref.id
  )
    throw recheck('SOURCE_ATTEMPT_CHANGED', current.candidate.candidateDigest);
  if (tested.candidate.candidateDigest !== current.candidate.candidateDigest)
    throw recheck('TESTED_CANDIDATE_CHANGED', current.candidate.candidateDigest);
  const routingKey = hash(
    JSON.stringify([
      run.id,
      target.id,
      targetRun.id,
      current.ref.digest,
      current.candidate.candidateDigest,
    ]),
  );
  const artifacts = (
    await db.query(
      "SELECT id,name,digest FROM artifacts WHERE run_id=$1 AND name<>'task-instructions-history.json'",
      [run.id],
    )
  ).rows;
  const feedback = {
    sourceTaskId: sourceId,
    sourceRunId: run.id,
    sourceAttempt: run.attempt,
    targetRunId: targetRun.id,
    targetAttempt: targetRun.attempt,
    candidateDigest: current.candidate.candidateDigest,
    candidateArtifact: { id: current.ref.id, digest: current.ref.digest },
    testedCandidateArtifact: { id: tested.ref.id, digest: tested.ref.digest },
    routingKey,
    summary: run.summary,
    checks: checks.filter((c: any) => !c.passed),
    artifacts,
  };
  await db.query(
    "UPDATE tasks SET state='failed',execution=jsonb_set(execution,'{repairFeedback}',$2::jsonb),reason='Correção automática solicitada pela verificação independente.' WHERE id=$1",
    [target.id, JSON.stringify(feedback)],
  );
  const preserved = await preservedAcceptedAuxiliaryIds(db, branch.filter(t => t.id !== target.id));
  await db.query(
    "UPDATE tasks SET state='blocked_dependencies',reason='Aguarda correção da candidata e nova verificação.' WHERE id=ANY($1::uuid[]) AND id<>$2 AND NOT(id=ANY($3::uuid[]))",
    [branch.map((t) => t.id), target.id, preserved],
  );
  await event(db, 'VERIFICATION_REPAIR_ROUTED', sourceId, {
    targetTaskId: target.id,
    sourceRunId: run.id,
    sourceAttempt: run.attempt,
    targetRunId: targetRun.id,
    targetAttempt: targetRun.attempt,
    candidateDigest: current.candidate.candidateDigest,
    candidateArtifact: { id: current.ref.id, digest: current.ref.digest },
    routingKey,
    preservedAuxiliaryTaskIds: preserved,
  });
  return target.id;
}

// This transition is separately testable; remote inspection is supplied by the supervisor.
export async function resumeFailedHarness(id: string, fence: string, proof: any) {
  return tx(async (db) => {
    await lock(db);
    const r = (
      await db.query('SELECT * FROM runs WHERE id=$1 AND fence=$2 FOR UPDATE', [id, fence])
    ).rows[0];
    if (!r || r.runtime !== 'harness' || !['failed', 'submitted', 'reconciling'].includes(r.state))
      throw Error('STALE_RUN');
    const t = (await db.query('SELECT * FROM tasks WHERE id=$1 FOR UPDATE', [r.task_id])).rows[0];
    const g = (await db.query('SELECT * FROM goals WHERE id=$1', [t.goal_id])).rows[0];
    if (
      !['active', 'failed'].includes(g.state) ||
      !['failed', 'submitted', 'recovery_blocked', 'reconciling', 'recovering_artifacts'].includes(
        t.state,
      ) ||
      t.attempt_count !== r.attempt
    )
      throw Error('TASK_CHANGED');
    assertStopped(proof, r, t);
    if (await pendingSupervisionOwnsContinuation(db, t.id, id)) throw Error('SUPERVISION_PENDING');
    await assertSupervisedResume(db, t, r, proof);
    if (
      (
        await db.query('SELECT 1 FROM runs WHERE task_id=$1 AND id<>$2 AND state=ANY($3::text[])', [
          t.id,
          id,
          liveStates,
        ])
      ).rowCount
    )
      throw Error('TASK_CHANGED');
    const branch = (
      await db.query(
        `WITH RECURSIVE b(id) AS (SELECT $1::uuid UNION SELECT d.task_id FROM dependencies d JOIN b ON b.id=d.depends_on) SELECT t.* FROM b JOIN tasks t ON t.id=b.id`,
        [t.id],
      )
    ).rows;
    if (branch.some((c) => c.id !== t.id && [...liveStates, 'cancelled'].includes(c.state)))
      throw Error('BRANCH_BUSY');
    if ((await db.query(`SELECT 1 FROM runs r JOIN tasks b ON b.id=r.task_id WHERE r.task_id=ANY($1::uuid[]) AND r.task_id<>$2
      AND (r.state=ANY($3::text[]) OR (r.attempt=b.attempt_count AND EXISTS(
        SELECT 1 FROM events e WHERE e.kind='CANCEL_REQUESTED' AND e.entity_id=b.id::text AND e.payload->>'runId'=r.id::text))) LIMIT 1`,
      [branch.map(c => c.id), t.id, liveStates])).rowCount) throw Error('BRANCH_BUSY');
    const policy = (
      await db.query('SELECT value FROM settings WHERE id=$1', ['workforce:' + t.goal_id])
    ).rows[0]?.value;
    const accounts = (await db.query('SELECT * FROM accounts')).rows;
    const route = policy?.enabled
      ? chooseWorkforceRoute(t, accounts, policy)
      : chooseRepairRoute(t, r, accounts);
    if (!route) throw Error('ACCOUNT_UNAVAILABLE');
    const artifacts = (
      await db.query(
        "SELECT id,name,digest FROM artifacts WHERE run_id=$1 AND name<>'task-instructions-history.json'",
        [id],
      )
    ).rows;
    for (const a of t.execution.repairFeedback?.artifacts ?? [])
      if (!artifacts.some((x) => x.id === a.id)) artifacts.push(a);
    const files = [
      {
        name: 'continuation-changes.patch',
        mime: 'text/plain',
        bytes: Buffer.from(proof.candidate?.patch ?? ''),
      },
      {
        name: 'continuation-diagnostic.json',
        mime: 'application/json',
        bytes: Buffer.from(
          JSON.stringify(
            { ...proof, candidate: { ...proof.candidate, patch: undefined } },
            null,
            2,
          ),
        ),
      },
    ].map((a) => ({ ...a, id: randomUUID(), digest: hash(a.bytes) }));
    if (files.some((f) => f.bytes.length > 8 * 1024 * 1024)) throw Error('CHECKPOINT_TOO_LARGE');
    await mkdir(path.join(config.dataDir, 'artifacts'), { recursive: true });
    for (const f of files) {
      await writeFile(path.join(config.dataDir, 'artifacts', f.id), f.bytes, { flag: 'wx' });
      await db.query(
        'INSERT INTO artifacts(id,run_id,name,mime,digest,size) VALUES($1,$2,$3,$4,$5,$6)',
        [f.id, id, f.name, f.mime, f.digest, f.bytes.length],
      );
      artifacts.push({ id: f.id, name: f.name, digest: f.digest });
    }
    const instruction =
      repairInstruction(r.error_code ?? 'VALIDATION_FAILED', t.execution.testFile) +
      (proof.collectionFailure
        ? ` Coleta automática: ${proof.collectionFailure}. Use nomes de arquivo no campo name (verification.json), nunca títulos descritivos. Confira que todos os arquivos listados existem.`
        : '');
    const checkpoint = {
      kind: 'automatic_recovery',
      strategy: 'targeted_repair',
      sourceRunId: id,
      previousSummary: r.summary,
      instruction,
      artifacts,
      preservedWorkspace: t.execution.workspace,
    };
    const routeHistory = [
      ...(t.execution.continuationRoutes ?? []).filter(
        (v: any) => Date.parse(v.until) > Date.now(),
      ),
      ...(/AUTH|QUOTA|RATE_LIMIT/.test(r.error_code ?? '')
        ? [
            {
              accountId: r.account_id,
              model: r.model,
              until: new Date(Date.now() + 30 * 60000).toISOString(),
            },
          ]
        : []),
    ];
    const continuation = {
      state: 'queued',
      message: 'Correção preparada; aguarda a vez deste jogo.',
      runId: id,
      updatedAt: new Date().toISOString(),
    };
    const execution = {
      ...t.execution,
      continuation,
      continuationRoutes: routeHistory,
      revisionFeedback: {
        ...t.execution.revisionFeedback,
        reports: [
          instruction,
          ...(t.execution.repairFeedback ? [JSON.stringify(t.execution.repairFeedback)] : []),
        ],
        artifacts,
      },
    };
    await db.query(
      "UPDATE runs SET state=CASE WHEN state='reconciling' THEN 'failed' ELSE state END,finished_at=COALESCE(finished_at,now()),checkpoint=$2 WHERE id=$1",
      [id, JSON.stringify(checkpoint)],
    );
    await db.query('DELETE FROM leases WHERE run_id=$1', [id]);
    await db.query(
      "UPDATE tasks SET state='retry_scheduled',account_id=$2,model=$3,execution=$4,max_attempts=GREATEST(max_attempts,attempt_count+1),next_attempt_at=now(),reason='Retomada automática com checkpoint e rota conferida.',updated_at=now() WHERE id=$1",
      [t.id, route.accountId, route.model, JSON.stringify(execution)],
    );
    const preserved = await preservedAcceptedAuxiliaryIds(db, branch.filter(c => c.id !== t.id));
    await db.query(
      "UPDATE tasks SET state='blocked_dependencies',reason='Aguarda correção automática da etapa anterior.' WHERE id=ANY($1::uuid[]) AND id<>$2 AND NOT(id=ANY($3::uuid[]))",
      [branch.map((c) => c.id), t.id, preserved],
    );
    await db.query("UPDATE goals SET state='active' WHERE id=$1 AND state='failed'", [t.goal_id]);
    // The planner can declare a stall in the instant between this failure and
    // this admission. A queued continuation is work in progress, and a parked
    // objective is what the panel shows as having no active step.
    await db.query(
      "UPDATE objectives SET status='executing',reason=NULL,updated_at=now() WHERE id=$1 AND status='waiting_external' AND reason=$2",
      [t.goal_id, PLAN_STALL_REASON],
    );
    await db.query("UPDATE recoveries SET status='superseded' WHERE source_run_id=$1", [id]);
    await event(db, 'CONTINUATION_QUEUED', t.id, {
      runId: id,
      signature: proof.continuationSignature,
      route,
      instruction,
      artifactCount: artifacts.length,
      preservedAuxiliaryTaskIds: preserved,
    });
    return { taskId: t.id, route };
  });
}

export class ContinuationSupervisor {
  private owner?: PoolClient;
  private timer?: ReturnType<typeof setTimeout>;
  private closing = false;
  private pending?: Promise<void>;
  constructor(
    private inspect = inspectStopped,
    private collect = collectStoppedArtifacts,
  ) {}
  async start() {
    if (!config.bridgeHost) return false;
    // Both policies belong to migrate()'s exact source receipt. Boot only
    // verifies that proof, before owning a session or dispatching any scan.
    await requireSchemaReady();
    this.owner = await pool.connect();
    if (!(await this.owner.query('SELECT pg_try_advisory_lock(4280050) owned')).rows[0].owned) {
      this.owner.release();
      this.owner = undefined;
      return false;
    }
    this.owner.on('error', () => {
      this.closing = true;
      clearTimeout(this.timer);
    });
    await this.owner.query(
      "INSERT INTO settings VALUES('artifact-recovery','{\"enabled\":true}') ON CONFLICT(id) DO UPDATE SET value=EXCLUDED.value",
    );
    const loop = () => {
      if (this.closing) return;
      this.pending = this.scan()
        .catch(async () => {
          await this.health('error');
        })
        .finally(() => {
          if (!this.closing) this.timer = setTimeout(loop, 5000);
        });
    };
    loop();
    return true;
  }
  private async health(state = 'running') {
    await pool.query(
      "INSERT INTO settings(id,value) VALUES('continuation-supervisor',$1) ON CONFLICT(id) DO UPDATE SET value=EXCLUDED.value",
      [JSON.stringify({ state, checkedAt: new Date().toISOString(), pid: process.pid })],
    );
  }
  async scan() {
    if (this.owner) await this.owner.query('SELECT 1');
    await this.health();
    await reconcileWorkforces();
    await reconcileVerificationRevisions(this.inspect);
    const rows = await readContinuationCandidates(pool, new Date().toISOString());
    for (const t of rows) {
      if (this.closing) return;
      // Disk failures belong to the shared resource probe, not an ever-growing
      // tree of semantic investigations which cannot start on the full volume.
      if(t.error_code==='DISK_CAPACITY' && await hasStoragePreflightReceipt(pool,t.run_id,t.id))continue;
      if(await scheduledSupervisionAdmission(pool,t.id,t.run_id))continue;
      const previous = t.execution.continuation;
      if (previous?.runId === t.run_id && Date.parse(previous.nextAt) > Date.now()) continue;
      // Explicit cancellation is never revived, including an unconfirmed cancellation.
      if (
        (
          await pool.query(
            "SELECT 1 FROM events WHERE kind='CANCEL_REQUESTED' AND payload->>'runId'=$1",
            [t.run_id],
          )
        ).rowCount
      )
        continue;
      // The supervised consumer retains its exact applied decision through
      // admission waits. A generic retry has no bound supervision token.
      if((t.execution.supervision || t.execution.supervisionHandoff) &&
        await appliedSupervisionOwnsContinuation(pool,t.id,t.run_id))continue;
      if (t.run_state === 'submitted') {
        const checks =
          (
            await pool.query(
              'SELECT checks FROM acceptances WHERE run_id=$1 ORDER BY created_at DESC LIMIT 1',
              [t.run_id],
            )
          ).rows[0]?.checks ?? [];
        if (
          !checks.some((c: any) => !c.passed && c.type !== 'review') &&
          !t.execution.repairFeedback
        )
          continue;
        // Verification must repair its implementation, not rewrite immutable QA input.
        if (t.execution.readOnlySource) {
          try {
            await routeVerificationRepair(t.id);
          } catch (e: any) {
            const routingCode=verificationRoutingCode(e),busy=routingCode==='BRANCH_BUSY';
            const review=busy?null:await requestRecoveryReview(t.id,t.run_id,'VERIFICATION_ROUTING_FAILED',
              'A verificação não pôde encaminhar sua reprovação à implementação. Resolver o vínculo e a causa raiz preservando a QA somente leitura.',
              {routingFailureCode:routingCode});
            await setContinuation(
              t.id,
              t.run_id,
              busy ? 'waiting_slot' : review?.state==='waiting_evidence'?'waiting_evidence':review?'supervising':'needs_action',
              busy
                ? 'Aguarda a execução atual para corrigir a candidata.'
                : 'O encaminhamento da verificação precisa de correção: '+routingCode+'.',
              60,
              {owner:review?'supervisor':'controller',reasonCode:'VERIFICATION_ROUTING_FAILED',routingCode,...(review?{caseId:review.caseId}:{})},
            );
          }
          continue;
        }
      }
      if (/DIGEST|CORRUPT|INVALID_WRITE_SCOPE|INVALID_WORKSPACE/.test(t.error_code ?? '')) {
        await setContinuation(
          t.id,
          t.run_id,
          'needs_action',
          'A integridade ou o escopo da execução precisa ser conferido.',
          120,
        );
        continue;
      }
      let collectionFailure: string | undefined;
      if (
        t.run_state === 'failed' &&
        t.execution.kind === 'harness' &&
        packagingFailure(t.error_code ?? '')
      ) {
        try {
          if (!await setContinuation(
            t.id,
            t.run_id,
            'collecting',
            'Recuperando arquivos da entrega, sem nova chamada ao modelo.',
            20,
          )) continue;
          const delivery = await this.collect(t.run_id, Boolean(t.execution.readOnlySource));
          if (this.closing) return;
          await adoptRecoveredArtifacts(t.run_id, t.fence, delivery);
          continue;
        } catch (e: any) {
          if (['STALE_RUN', 'TASK_CHANGED'].includes(e.message)) continue;
          if (
            ['COLLECTION_UNAVAILABLE', 'STOP_UNCONFIRMED', 'WORKSPACE_BUSY'].includes(e.message)
          ) {
            await setContinuation(
              t.id,
              t.run_id,
              'collecting',
              'Aguardando confirmação para recolher os arquivos preservados.',
              30,
            );
            continue;
          }
          collectionFailure = String(e.message);
          const recent = (
            await pool.query(
              'SELECT error_code FROM runs WHERE task_id=$1 ORDER BY attempt DESC LIMIT 2',
              [t.id],
            )
          ).rows;
          if (recent.length === 2 && recent.every((r) => packagingFailure(r.error_code ?? ''))) {
            await setContinuation(
              t.id,
              t.run_id,
              'needs_action',
              `A coleta dos arquivos não pôde ser corrigida automaticamente (${collectionFailure}). Novas chamadas repetidas ao modelo foram interrompidas.`,
              120,
            );
            continue;
          }
        }
      }
      const occupied = (
        await pool.query(
          `SELECT 1 FROM runs r JOIN tasks x ON x.id=r.task_id WHERE r.id<>$1 AND r.state=ANY($2::text[]) AND x.execution->>'baseWorkspace'=$3 LIMIT 1`,
          [t.run_id, liveStates, t.execution.baseWorkspace],
        )
      ).rowCount;
      const workforce = (
        await pool.query('SELECT value FROM settings WHERE id=$1', ['workforce:' + t.goal_id])
      ).rows[0]?.value;
      if (occupied && t.execution.kind === 'opencode' && !workforce?.enabled) {
        await setContinuation(t.id, t.run_id, 'waiting_slot', 'Correção na fila deste jogo.', 15);
        continue;
      }
      let stoppedProof: Awaited<ReturnType<typeof inspectStopped>> | undefined;
      try {
        if(t.error_code===operatorRecoveryCode){
          // The explicit operator request is not a spontaneous functional
          // failure. Do not collect/queue generic continuation or infer stop.
          if(t.run_state!=='failed')continue;
          const proof=await this.inspect(t.run_id,t.execution.kind==='opencode');if(this.closing)return;
          await requestRecoveryReview(t.id,t.run_id,operatorRecoveryCode,
            'Interrupção administrativa da tentativa, solicitada explicitamente pelo operador; não representa falha espontânea do jogo. Executor e sandbox já foram inspecionados. Preserve critérios, arquivos e artefatos, confira o resultado existente e defina a próxima ação verificável sem repetir polling improdutivo. Aceite e QA independente permanecem obrigatórios.',
            {stopProof:proof});
          continue;
        }
        if (!await setContinuation(
          t.id,
          t.run_id,
          'checking',
          'Conferindo encerramento e arquivos para retomar.',
          20,
        )) continue;
        const proof = await this.inspect(t.run_id, t.execution.kind === 'opencode');
        stoppedProof=proof;
        proof.collectionFailure = collectionFailure;
        if (this.closing) return;
        if(t.run_state==='reconciling' && t.execution.kind==='harness' && proof.state==='not_started'){
          await reconcileUnstartedHarnessRun(t.run_id,this.inspect);
          continue;
        }
        if (t.error_code === runtimeProgressCode) {
          // Poll repetition is a semantic dead end, not a transient runtime
          // failure. Keep the preserved files and require a different plan.
          assertStopped(proof, {id:t.run_id}, t);
          const review = t.run_state === 'failed' ? await requestRecoveryReview(t.id,t.run_id,runtimeProgressCode,
            'RUNTIME_PROGRESS_STALLED: a execução repetiu consultas ao mesmo resultado e arquivo com falha explícita, sem alteração verificável. O checkpoint e os arquivos foram preservados. Inspecione a falha já observada, corrija o menor caso e use espera limitada com resultado terminal; não repita o mesmo polling. Preserve critérios e QA independente.',{stopProof:proof}) : null;
          await setContinuation(t.id,t.run_id,review ? 'supervising' : 'checking',
            review ? 'Supervisor definindo nova abordagem para a espera repetida.' :
              'Aguardando confirmação completa do encerramento antes da supervisão.',
            30,{owner:review ? 'supervisor' : 'controller',reasonCode:runtimeProgressCode,
              ...(review ? {caseId:review.caseId} : {})});
          continue;
        }
        const signature = await continuationSignature(
          pool,
          t,
          t.error_code ?? 'VALIDATION_FAILED',
          proof.candidate?.candidateDigest ?? null,
        );
        const history = (
          await pool.query(
            "SELECT payload FROM events WHERE kind='CONTINUATION_QUEUED' AND entity_id=$1 ORDER BY id DESC LIMIT 6",
            [t.id],
          )
        ).rows.map((e) => e.payload);
        if (!providerAvailabilityFailure(t.error_code) && proof.candidate?.candidateDigest && repeatedWithoutProgress(history, signature)) {
          const requested=await requestProgressReplan(t.id,t.run_id,signature,{stopProof:proof});
          const review=requested?null:await requestRecoveryReview(t.id,t.run_id,'REPEATED_NO_PROGRESS',
            'REPEATED_NO_PROGRESS: recuperação repetida sem mudança verificável; o supervisor deve definir uma investigação discriminante.');
          await setContinuation(
            t.id,
            t.run_id,
            requested || review?'supervising':'needs_action',
            'A mesma falha voltou repetidamente sem alteração nos arquivos. É necessário um novo diagnóstico.',
            120,
            {owner:requested || review?'supervisor':'controller',reasonCode:'REPEATED_NO_PROGRESS',...(review?{caseId:review.caseId}:{})},
          );
          continue;
        }
        proof.continuationSignature = signature;
        if (t.execution.kind === 'opencode') {
          await pool.query("UPDATE goals SET state='active' WHERE id=$1 AND state='failed'", [
            t.goal_id,
          ]);
          const result = await recoverOpenCodeRun(t.run_id, t.fence, proof);
          await event(pool, 'CONTINUATION_QUEUED', t.id, {
            runId: t.run_id,
            nextRunId: result.runId,
            signature,
            instruction: repairInstruction(t.error_code ?? ''),
          });
        } else await resumeFailedHarness(t.run_id, t.fence, proof);
      } catch (e: any) {
        if (['STALE_RUN', 'TASK_CHANGED', 'GOAL_NOT_ACTIVE', 'SUPERVISION_PENDING'].includes(e.message)) continue;
        if (e.message === 'NO_FAILED_CHECK') {
          const review = await requestRecoveryReview(t.id, t.run_id, 'NO_FAILED_CHECK',
            'A entrega anterior passou no contrato auxiliar, mas recebeu uma solicitação posterior de correção. O supervisor deve reconciliar a evidência e definir uma nova abordagem; repetir a inspeção não resolve o conflito. Preserve critérios, artefatos e aceite final.');
          if(!review && await appliedSupervisionOwnsContinuation(pool,t.id,t.run_id))continue;
          await setContinuation(t.id, t.run_id,
            review?.state === 'waiting_evidence' ? 'waiting_evidence' : review ? 'supervising' : 'needs_action',
            review ? 'Supervisor reconciliando o contrato da correção com a entrega anterior.'
              : 'A correção precisa de revisão do contrato antes de uma nova execução.',
            60, { owner: review ? 'supervisor' : 'controller', reasonCode: 'NO_FAILED_CHECK',
              ...(review ? { caseId: review.caseId } : {}) });
          continue;
        }
        if ([semanticRetryCode, semanticEvidenceCode].includes(e.message)) {
          if (!(await requestSemanticReplan(t.id, t.run_id)))
            await setContinuation(
              t.id,
              t.run_id,
              'needs_action',
              e.message === semanticEvidenceCode
                ? 'A integridade da evidência de reparo precisa ser conferida antes de retomar.'
                : 'A entrega negativa exige nova decisão de supervisão antes de retomar.',
              60,
            );
          continue;
        }
        if (e.message === 'REPEATED_NO_PROGRESS') {
          if (!(await requestProgressReplan(t.id, t.run_id, e.progressSignature,{stopProof:stoppedProof})))
            await setContinuation(
              t.id,
              t.run_id,
              'needs_action',
              'Retomadas repetidas sem mudança verificável; habilite supervisão para replanejar.',
              120,
            );
          continue;
        }
        if (e.message === 'PROVIDER_RETRY_PENDING') {
          await setContinuation(t.id, t.run_id, 'waiting_slot',
            'Aguarda o prazo de nova tentativa informado pelo provedor.',
            Math.max(1, Math.floor((Date.parse(e.notBefore) - Date.now()) / 1000)),
            { owner: 'scheduler', reasonCode: 'PROVIDER_RETRY_PENDING' });
          continue;
        }
        const dependencies = e.message === 'DEPENDENCIES_PENDING';
        const waiting = [
          'RESOURCE_BUSY',
          'CAPACITY_UNAVAILABLE',
          'EXCLUSIVE_HOST_BUSY',
          'BRANCH_BUSY',
          'DEPENDENCIES_PENDING',
        ].includes(e.message);
        const quota = e.message === 'ACCOUNT_UNAVAILABLE';
        if (quota) {
          const accounts = (await pool.query('SELECT * FROM accounts')).rows.filter(
            (a) =>
              a.enabled &&
              a.bridge_id &&
              !['agy', 'local'].includes(a.provider) &&
              (!t.allowed_accounts?.length || t.allowed_accounts.includes(a.id)) &&
              quotaFor(a, a.model).status !== 'auth_required',
          );
          if (!accounts.length) {
            await setContinuation(
              t.id,
              t.run_id,
              'needs_action',
              'Reconecte pelo menos uma conta autorizada em Contas e cotas para continuar.',
              60,
              {owner:'user',reasonCode:'AUTH_REQUIRED',humanRequest:{kind:'authentication',
                question:'Reconecte uma das contas autorizadas para esta tarefa em Contas e cotas.'}},
            );
            continue;
          }
        }
        await setContinuation(
          t.id,
          t.run_id,
          waiting ? 'waiting_slot' : quota ? 'waiting_quota' : 'checking',
          dependencies
            ? 'Aguarda o aceite das dependências antes de retomar.'
            : waiting
              ? 'Correção na fila; aguarda recurso livre.'
              : quota
                ? 'Buscando conta autorizada com cota disponível.'
                : 'Verificando o executor antes de liberar nova tentativa.',
          waiting ? 15 : 30,
        );
      }
      await this.health();
    }
  }
  async close() {
    this.closing = true;
    clearTimeout(this.timer);
    await this.pending;
    if (this.owner) {
      await this.owner.query('SELECT pg_advisory_unlock(4280050)');
      this.owner.release();
      this.owner = undefined;
    }
  }
}
