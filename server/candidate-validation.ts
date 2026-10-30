import { managerAuthoritySql } from './manager-authority.js';
import { createHash, randomUUID } from 'node:crypto';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { z } from 'zod';
import { config } from './config.js';
import { event, lock, pool, tx, type DB } from './db.js';
import { assertStopped, inspectStopped } from './continuation.js';
import { readArtifact } from './service.js';
import { runtimeCandidateDigest } from './operation-progress.js';
import { readDeliverySourceIdentity, sourceIdentityFrozen } from './delivery-source-resolution.js';
import {workerValidationAuthority,verifyWorkerValidationAuthority,readWorkerValidationIdentity} from './worker-validation.js';

const owned = ['proposed', 'freezing', 'frozen', 'running', 'uncertain'];
const sha = (bytes: Buffer | string) => createHash('sha256').update(bytes).digest('hex');
function canonical(value: any): any {
  if (Array.isArray(value)) return value.map(canonical);
  if (value && typeof value === 'object')
    return Object.fromEntries(
      Object.keys(value)
        .sort()
        .map((key) => [key, canonical(value[key])]),
    );
  return value;
}
const requestDigest = (request: any) => sha(JSON.stringify(canonical(request)));
const frameworkRegistrySchema = z
  .object({
    version: z.literal(1),
    enabled: z.literal(true),
    project: z.string().regex(/^[a-z0-9_-]+$/),
    role: z.literal('framework'),
    sourceKind: z.literal('agentos_published_runtime'),
    goalIds: z.array(z.uuid()).min(1).max(256),
  })
  .strict();

/** Operator-owned allowlist, not a model-supplied project or workspace. */
export async function frameworkProjectAuthorization(db: DB, goalId: string) {
  const goal = (await db.query('SELECT project FROM goals WHERE id=$1', [goalId])).rows[0];
  if (!goal || !/^[a-z0-9_-]+$/.test(goal.project)) throw Error('VALIDATION_PROJECT_UNREGISTERED');
  if (goal.project === 'agentos')
    return {
      project: goal.project,
      authorization: { kind: 'registered_agentos_source', project: goal.project, goalId },
    };
  const id = 'framework-project:' + goal.project;
  const row = (await db.query('SELECT value FROM settings WHERE id=$1', [id])).rows[0];
  const parsed = frameworkRegistrySchema.safeParse(row?.value);
  if (
    !parsed.success ||
    parsed.data.project !== goal.project ||
    !parsed.data.goalIds.includes(goalId)
  )
    throw Error('VALIDATION_PROJECT_UNREGISTERED');
  return {
    project: goal.project,
    authorization: {
      kind: 'registered_framework_project',
      project: goal.project,
      goalId,
      registryId: id,
      registry: parsed.data,
      registryDigest: requestDigest(parsed.data),
    },
  };
}

/** Bind this index to the actual immutable action emitted by an accepted Sol
 * coordinator. Artifact existence and a passed row alone are not authority. */
export async function verifyAcceptedManagementAction(
  db: DB,
  goalId: string,
  managerRunId: string,
  index: number,
  raw: unknown,
) {
  if (!Number.isSafeInteger(index) || index < 0) throw Error('VALIDATION_DECISION_UNBOUND');
  const manager = (
    await db.query(
      `SELECT 1 FROM runs r JOIN tasks t ON t.id=r.task_id JOIN goals g ON g.id=t.goal_id
    JOIN accounts a ON a.id=r.account_id WHERE r.id=$1 AND t.goal_id=$2 AND t.stage='coordinate'
    AND r.state='submitted' AND r.runtime='harness' AND ${managerAuthoritySql()}
    AND (SELECT passed FROM acceptances ac WHERE ac.run_id=r.id AND ac.task_id=t.id ORDER BY created_at DESC,id DESC LIMIT 1)`,
      [managerRunId, goalId],
    )
  ).rowCount;
  if (!manager) throw Error('VALIDATION_MANAGER_UNAUTHORIZED');
  const refs = (
    await db.query(
      "SELECT id,digest,size FROM artifacts WHERE run_id=$1 AND name='coordination.json' ORDER BY id LIMIT 2",
      [managerRunId],
    )
  ).rows;
  if (refs.length !== 1) throw Error('VALIDATION_DECISION_UNBOUND');
  const ref = refs[0],
    artifact = await readArtifact(ref.id);
  if (artifact.bytes.length !== ref.size || sha(artifact.bytes) !== ref.digest)
    throw Error('VALIDATION_DECISION_CORRUPT');
  let action: any;
  try {
    action = JSON.parse(artifact.bytes.toString()).management?.actions?.[index];
  } catch {
    throw Error('VALIDATION_DECISION_CORRUPT');
  }
  if (!action || requestDigest(action) !== requestDigest(raw))
    throw Error('VALIDATION_DECISION_UNBOUND');
  return {
    coordinationArtifactId: ref.id,
    coordinationArtifactDigest: ref.digest,
    coordinationSize: ref.size,
    actionIndex: index,
    actionDigest: requestDigest(raw),
  };
}
export const candidateValidationSchema = z
  .object({
    kind: z.literal('request_validation'),
    taskId: z.uuid(),
    expectedAttempt: z.number().int().positive(),
    runId: z.uuid(),
    runFence: z.uuid(),
    contextVersion: z.uuid(),
    candidateArtifactId: z.uuid(),
    candidateArtifactDigest: z.string().regex(/^[a-f0-9]{64}$/),
    candidateDigest: z.string().regex(/^[a-f0-9]{64}$/),
    reason: z.string().min(20).max(2000),
  })
  .strict();
type Proposal = z.infer<typeof candidateValidationSchema>;
type Dispatch = (mode: 'freeze' | 'execute' | 'receipt', request: any) => Promise<any>;
function proposal(value: any): Proposal {
  return candidateValidationSchema.parse(
    Object.fromEntries(
      Object.entries(value).filter(([key]) =>
        [
          'kind',
          'taskId',
          'expectedAttempt',
          'runId',
          'runFence',
          'contextVersion',
          'candidateArtifactId',
          'candidateArtifactDigest',
          'candidateDigest',
          'reason',
        ].includes(key),
      ),
    ),
  );
}

/** These guards are installed only by the existing reviewed management schema
 * installation, never by the worker reconciler. No task is accepted here. */
export async function installCandidateValidation(db: DB = pool) {
  await db.query(`CREATE OR REPLACE FUNCTION agentos_candidate_validation_owned(target uuid) RETURNS boolean
    LANGUAGE sql STABLE AS $$ SELECT EXISTS(SELECT 1 FROM settings s JOIN tasks t ON t.id=target
      JOIN runs r ON r.task_id=t.id AND r.attempt=t.attempt_count
      WHERE s.id LIKE 'candidate-validation:%' AND s.value->>'taskId'=target::text
        AND s.value->>'runId'=r.id::text AND s.value->>'runFence'=r.fence::text
        AND s.value->>'contextVersion'=r.context_id::text
        AND s.value->>'state' IN('proposed','freezing','frozen','running','uncertain'))
      OR EXISTS(SELECT 1 FROM runs r JOIN tasks t ON t.id=r.task_id
        LEFT JOIN settings h ON h.id='worker-validation-handoff:'||r.id
        WHERE t.id=target AND r.attempt=t.attempt_count AND r.state='submitted'
          AND NULLIF(r.instruction_snapshot->'workerValidation','null'::jsonb) IS NOT NULL
          AND COALESCE(h.value->>'state','pending')='pending') $$;
    CREATE OR REPLACE FUNCTION agentos_candidate_validation_guard() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
      PERFORM pg_advisory_xact_lock(4280001);
      IF TG_TABLE_NAME='runs' THEN
        IF agentos_candidate_validation_owned(NEW.task_id) THEN RAISE EXCEPTION 'CANDIDATE_VALIDATION_OWNS_TASK'; END IF;
      ELSIF agentos_candidate_validation_owned(OLD.id) THEN
        IF NEW.description IS DISTINCT FROM OLD.description OR NEW.criteria IS DISTINCT FROM OLD.criteria
          OR (NEW.execution-'continuation') IS DISTINCT FROM (OLD.execution-'continuation')
          OR NEW.instruction_revision_id IS DISTINCT FROM OLD.instruction_revision_id
          OR NEW.agent_id IS DISTINCT FROM OLD.agent_id OR NEW.resources IS DISTINCT FROM OLD.resources
          OR NEW.required_memories IS DISTINCT FROM OLD.required_memories
          OR NEW.allowed_accounts IS DISTINCT FROM OLD.allowed_accounts THEN RAISE EXCEPTION 'CANDIDATE_VALIDATION_CONTRACT_FROZEN'; END IF;
        IF NEW.state IN('ready','retry_scheduled','queued','running') OR NEW.attempt_count<>OLD.attempt_count THEN
          NEW.state:=OLD.state; NEW.attempt_count:=OLD.attempt_count; NEW.reason:=OLD.reason;
        END IF;
      END IF;
      RETURN NEW;
    END $$;
    DROP TRIGGER IF EXISTS a_agentos_candidate_validation_guard ON tasks;
    CREATE TRIGGER a_agentos_candidate_validation_guard BEFORE UPDATE ON tasks FOR EACH ROW EXECUTE FUNCTION agentos_candidate_validation_guard();
    DROP TRIGGER IF EXISTS a_agentos_candidate_validation_guard ON runs;
    CREATE TRIGGER a_agentos_candidate_validation_guard BEFORE INSERT ON runs FOR EACH ROW EXECUTE FUNCTION agentos_candidate_validation_guard();`);
}

async function binding(db: DB, goalId: string, managerRunId: string, value: Proposal) {
  const manager = (
    await db.query(
      `SELECT 1 FROM goals g JOIN settings m ON m.id='management:'||g.id
    JOIN settings w ON w.id='workforce:'||g.id LEFT JOIN settings au ON au.id='autonomy:'||g.project
    JOIN runs r ON r.id=$2 JOIN tasks t ON t.id=r.task_id AND t.goal_id=g.id
    JOIN accounts a ON a.id=r.account_id
    WHERE g.id=$1 AND g.state='active' AND m.value->>'enabled'='true'
      AND w.value->>'enabled'='true' AND COALESCE(au.value->>'enabled','true')='true'
      AND t.stage='coordinate' AND r.state='submitted' AND r.runtime='harness'
      AND ${managerAuthoritySql()}
      AND (SELECT passed FROM acceptances ac WHERE ac.run_id=r.id AND ac.task_id=t.id ORDER BY created_at DESC,id DESC LIMIT 1)
      AND EXISTS(SELECT 1 FROM artifacts ar WHERE ar.run_id=r.id AND ar.name='coordination.json')`,
      [goalId, managerRunId],
    )
  ).rowCount;
  if (!manager) throw Error('VALIDATION_MANAGER_UNAUTHORIZED');
  return candidateTargetBinding(db,goalId,value);
}

async function candidateTargetBinding(db:DB,goalId:string,value:Proposal) {
  const framework = await frameworkProjectAuthorization(db, goalId);
  const task = (
    await db.query('SELECT * FROM tasks WHERE id=$1 AND goal_id=$2', [value.taskId, goalId])
  ).rows[0];
  const run =
    task &&
    (await db.query('SELECT * FROM runs WHERE task_id=$1 ORDER BY attempt DESC LIMIT 1', [task.id]))
      .rows[0];
  if (
    !task ||
    !run ||
    [
      'accepted',
      'cancelled',
      'superseded',
      'paused',
      'queued',
      'running',
      'cancelling',
      'reconciling',
    ].includes(task.state) ||
    !['submitted', 'failed'].includes(run.state) ||
    run.id !== value.runId ||
    run.fence !== value.runFence ||
    run.context_id !== value.contextVersion ||
    run.attempt !== value.expectedAttempt ||
    task.attempt_count !== value.expectedAttempt ||
    !['harness', 'opencode'].includes(task.execution?.kind) ||
    task.stage === 'coordinate' ||
    !/^[a-z0-9_-]+\/o[a-z0-9_-]+$/.test(task.execution?.workspace ?? '') ||
    task.execution.workspace.split('/')[0] !== framework.project ||
    (task.recovery?.mode === 'manual' &&
      !(
        await db.query("SELECT 1 FROM settings WHERE id=$1 AND value->>'enabled'='true'", [
          'autonomy:' + framework.project,
        ])
      ).rowCount)
  )
    throw Error('VALIDATION_BINDING_STALE');
  if (
    (
      await db.query(
        `SELECT 1 FROM settings WHERE id LIKE 'published-rebase:%' AND value->>'taskId'=$1
    AND value->>'runId'=$2 AND value->>'runFence'=$3 AND value->>'contextVersion'=$4
    AND value->>'state' IN('proposed','preparing','prepared','applying','uncertain')`,
        [task.id, run.id, run.fence, run.context_id],
      )
    ).rowCount
  )
    throw Error('VALIDATION_REBASE_OWNS_TASK');
  if (
    !(
      await db.query('SELECT 1 FROM contexts WHERE id=$1 AND run_id=$2', [
        value.contextVersion,
        run.id,
      ])
    ).rowCount
  )
    throw Error('VALIDATION_BINDING_STALE');
  const current = await readDeliverySourceIdentity(db, task.id);
  if (!sourceIdentityFrozen(current) || current.live || current.leased || current.cancelled)
    throw Error('VALIDATION_BINDING_STALE');
  return { task, run, contractDigest: current.contract_digest, ...framework };
}

async function authorizedBinding(db:DB,value:any) {
  if(value.authority==='runtime_artifact') {
    await verifyWorkerValidationAuthority(db,value);
    return candidateTargetBinding(db,value.goalId,proposal(value));
  }
  await verifyAcceptedManagementAction(db,value.goalId,value.managerRunId,value.index,proposal(value));
  return binding(db,value.goalId,value.managerRunId,proposal(value));
}

/** A durable handoff receipt, not another execution queue. Only current stopped
 * submitted attempts with an admission policy may reserve the existing owner. */
export async function reconcileWorkerValidationIntents(inspect=inspectStopped) {
  const rows=(await pool.query(`SELECT r.id FROM runs r JOIN tasks t ON t.id=r.task_id
    LEFT JOIN settings h ON h.id='worker-validation-handoff:'||r.id
    WHERE r.state='submitted' AND t.state='submitted' AND r.attempt=t.attempt_count
      AND r.instruction_snapshot->'workerValidation' IS NOT NULL
      AND r.instruction_snapshot->'workerValidation'<>'null'::jsonb
      AND COALESCE(h.value->>'state','pending')='pending'
      AND COALESCE((h.value->>'nextProbeAt')::timestamptz,'epoch')<now()
    ORDER BY r.finished_at,r.id LIMIT 20`)).rows;
  for(const row of rows) {
    let proof:any,observationError:string|undefined;
    try {
      const raw=await runtimeProposal(pool,row.id);
      const target=await candidateTargetBinding(pool,raw.goalId,raw.value);
      try {
        proof=await inspect(row.id,target.task.execution.kind==='opencode');
        assertStopped(proof,target.run,target.task);
      } catch {observationError='STOP_UNCONFIRMED';}
    } catch(e:any){observationError=/^[A-Z_]+$/.test(e.message)?e.message:'VALIDATION_INTENT_FORMAT';}
    await tx(async db=>{
      await lock(db);
      const key='worker-validation-handoff:'+row.id;
      const previous=(await db.query('SELECT value FROM settings WHERE id=$1 FOR UPDATE',[key])).rows[0]?.value;
      if(previous && previous.state!=='pending') return;
      let receipt:any={version:1,runId:row.id,state:'pending'};
      try {
        const raw=await runtimeProposal(db,row.id),value=raw.value;
        const target=await candidateTargetBinding(db,raw.goalId,value);
        const existing=(await db.query(`SELECT value FROM settings WHERE id LIKE 'candidate-validation:%'
          AND value->>'runId'=$1 ORDER BY id LIMIT 2`,[row.id])).rows;
        if(existing.length) {
          if(existing.length!==1 || existing[0].value.authority!=='runtime_artifact' ||
            existing[0].value.candidateDigest!==value.candidateDigest) throw Error('VALIDATION_OWNER_CONFLICT');
          await verifyWorkerValidationAuthority(db,existing[0].value);
          receipt={...receipt,state:'reserved',actionId:existing[0].value.actionId};
        } else {
          if(observationError) throw Error(observationError);
          assertStopped(proof,target.run,target.task);
          const candidate=await canonicalCandidate(db,value,proof),actionId=randomUUID();
          const request={actionId,authority:'runtime_artifact',runtimeAuthority:raw.authority,goalId:raw.goalId,
            ...Object.fromEntries(Object.entries(value).filter(([k])=>!['kind','reason'].includes(k))),
            workspace:target.task.execution.workspace,contractDigest:target.contractDigest,project:target.project,
            frameworkAuthorization:target.authorization,...candidate};
          const job={version:1,actionId,authority:'runtime_artifact',goalId:raw.goalId,...value,
            state:'proposed',request,proposedAt:new Date().toISOString()};
          await db.query('INSERT INTO settings(id,value) VALUES($1,$2)',
            ['candidate-validation:'+actionId,JSON.stringify(job)]);
          receipt={...receipt,state:'reserved',actionId};
          await event(db,'CANDIDATE_VALIDATION_PROPOSED',value.taskId,{actionId,goalId:raw.goalId,
            runId:row.id,authority:'runtime_artifact',candidateDigest:value.candidateDigest,acceptance:false});
        }
      } catch(e:any) {
        const code=/^[A-Z_]+$/.test(e.message)?e.message:'VALIDATION_INTENT_FORMAT';
        // Physical uncertainty does not free ownership or manufacture failure.
        const pending=/STOP|LIVE|INSPECT|UNCONFIRMED|TRANSPORT|PROCESS/.test(code);
        receipt={...receipt,state:pending?'pending':'rejected',code,
          ...(pending?{nextProbeAt:new Date(Date.now()+15000).toISOString()}: {})};
      }
      await db.query('INSERT INTO settings(id,value) VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET value=excluded.value',
        [key,JSON.stringify(receipt)]);
      await event(db,'WORKER_VALIDATION_HANDOFF',row.id,receipt);
    });
  }
}

async function runtimeProposal(db:DB,runId:string) {
  const run=await readWorkerValidationIdentity(db,runId);
  if(!run || run.state!=='submitted' || run.task_state!=='submitted' || run.attempt!==run.attempt_count || !run.finished_at)
    throw Error('VALIDATION_BINDING_STALE');
  const {authority,reason}=await workerValidationAuthority(db,run);
  const refs=(await db.query("SELECT id,digest FROM artifacts WHERE run_id=$1 AND name='candidate.json' ORDER BY id LIMIT 2",[runId])).rows;
  if(refs.length!==1) throw Error('VALIDATION_ARTIFACT_CHANGED');
  const candidate=await readArtifact(refs[0].id),candidateDigest=runtimeCandidateDigest(candidate.bytes,refs[0].digest);
  if(!candidateDigest) throw Error('VALIDATION_CANDIDATE_CHANGED');
  return {authority,goalId:run.goal_id,value:candidateValidationSchema.parse({kind:'request_validation',
    taskId:run.task_id,expectedAttempt:run.attempt,runId:run.id,runFence:run.fence,contextVersion:run.context_id,
    candidateArtifactId:refs[0].id,candidateArtifactDigest:refs[0].digest,candidateDigest,reason})};
}

async function canonicalCandidate(db: DB, value: Proposal, proof: any) {
  const refs = (
    await db.query(
      "SELECT id,digest,size FROM artifacts WHERE run_id=$1 AND name='candidate.json' ORDER BY id LIMIT 2",
      [value.runId],
    )
  ).rows;
  if (
    refs.length !== 1 ||
    refs[0].id !== value.candidateArtifactId ||
    refs[0].digest !== value.candidateArtifactDigest
  )
    throw Error('VALIDATION_ARTIFACT_CHANGED');
  const artifact = await readArtifact(value.candidateArtifactId);
  if (
    artifact.bytes.length !== refs[0].size ||
    sha(artifact.bytes) !== value.candidateArtifactDigest ||
    runtimeCandidateDigest(artifact.bytes, value.candidateArtifactDigest) !==
      value.candidateDigest ||
    proof?.candidate?.candidateDigest !== value.candidateDigest
  )
    throw Error('VALIDATION_CANDIDATE_CHANGED');
  return {
    candidateBase64: artifact.bytes.toString('base64'),
    candidateSize: artifact.bytes.length,
  };
}

export async function proposeCandidateValidation(
  goalId: string,
  managerRunId: string,
  index: number,
  raw: unknown,
  inspect = inspectStopped,
) {
  const parsed = candidateValidationSchema.safeParse(raw);
  let proof: any;
  if (parsed.success) {
    // Do not inspect a live or foreign actor, even before the transaction.
    try {
      await verifyAcceptedManagementAction(pool, goalId, managerRunId, index, raw);
      const target = await binding(pool, goalId, managerRunId, parsed.data);
      proof = await inspect(target.run.id, target.task.execution.kind === 'opencode');
    } catch {
      /* exact rejection below */
    }
  }
  return tx(async (db) => {
    await lock(db);
    const prior = (
      await db.query('SELECT receipt FROM management_actions WHERE run_id=$1 AND action_index=$2', [
        managerRunId,
        index,
      ])
    ).rows[0];
    if (prior) {
      try {
        await verifyAcceptedManagementAction(db, goalId, managerRunId, index, raw);
        return prior.receipt;
      } catch {
        return {
          kind: 'request_validation',
          index,
          actionId: null,
          taskId: parsed.success ? parsed.data.taskId : null,
          code: 'VALIDATION_DECISION_UNBOUND',
          acceptance: false,
        };
      }
    }
    let code = 'VALIDATION_FORMAT',
      actionId: string | null = null;
    try {
      if (!parsed.success || !Number.isSafeInteger(index) || index < 0) throw Error(code);
      const value = parsed.data,
        target = await binding(db, goalId, managerRunId, value);
      const decisionProof = await verifyAcceptedManagementAction(
        db,
        goalId,
        managerRunId,
        index,
        raw,
      );
      const existing = (
        await db.query(
          `SELECT value FROM settings WHERE id LIKE 'candidate-validation:%'
        AND value->>'taskId'=$1 AND value->>'runId'=$2 AND value->>'candidateDigest'=$3 ORDER BY id LIMIT 1`,
          [value.taskId, value.runId, value.candidateDigest],
        )
      ).rows[0]?.value;
      if (existing) {
        actionId = existing.actionId;
        code = 'VALIDATION_ALREADY_REQUESTED';
      } else {
        assertStopped(proof, target.run, target.task);
        const candidate = await canonicalCandidate(db, value, proof);
        actionId = randomUUID();
        const request = {
          actionId,
          managerRunId,
          goalId,
          taskId: value.taskId,
          runId: value.runId,
          runFence: value.runFence,
          contextVersion: value.contextVersion,
          expectedAttempt: value.expectedAttempt,
          candidateArtifactId: value.candidateArtifactId,
          candidateArtifactDigest: value.candidateArtifactDigest,
          candidateDigest: value.candidateDigest,
          workspace: target.task.execution.workspace,
          contractDigest: target.contractDigest,
          project: target.project,
          frameworkAuthorization: target.authorization,
          decisionProof,
          ...candidate,
        };
        await db.query('INSERT INTO settings(id,value) VALUES($1,$2)', [
          'candidate-validation:' + actionId,
          JSON.stringify({
            version: 1,
            actionId,
            managerRunId,
            goalId,
            index,
            decisionProof,
            ...value,
            state: 'proposed',
            request,
            proposedAt: new Date().toISOString(),
          }),
        ]);
        code = 'VALIDATION_PROPOSED';
      }
    } catch (error: any) {
      code = /^[A-Z_]+$/.test(error.message) ? error.message : 'VALIDATION_REJECTED';
    }
    const receipt = {
      kind: 'request_validation',
      index,
      actionId,
      taskId: parsed.success ? parsed.data.taskId : null,
      code,
      acceptance: false,
    };
    await db.query('INSERT INTO management_actions(run_id,action_index,receipt) VALUES($1,$2,$3)', [
      managerRunId,
      index,
      JSON.stringify(receipt),
    ]);
    await event(db, 'MANAGEMENT_ACTION', goalId, { runId: managerRunId, ...receipt });
    return receipt;
  });
}

export async function dispatchCandidateValidation(
  mode: 'freeze' | 'execute' | 'receipt',
  request: any,
): Promise<any> {
  const script = fileURLToPath(
    new URL('../scripts/agentos-candidate-validation.py', import.meta.url),
  ).replaceAll('\\', '/');
  // Only the immutable managed release path is admissible on the remote host.
  if (
    !/^\/opt\/agentos\/releases\/managed-[0-9a-f-]{36}\/scripts\/agentos-candidate-validation.py$/.test(
      script,
    )
  )
    throw Error('VALIDATION_BROKER_UNDEPLOYED');
  return new Promise((resolve, reject) => {
    const child = spawn(
      'ssh',
      [
        '-T',
        '-o',
        'BatchMode=yes',
        '-o',
        'ConnectTimeout=8',
        config.bridgeHost,
        '/usr/bin/python3',
        script,
        mode,
      ],
      { windowsHide: true, stdio: ['pipe', 'pipe', 'pipe'] },
    );
    let stdout = '';
    const timer = setTimeout(() => child.kill('SIGTERM'), mode === 'execute' ? 1950000 : 45000);
    child.stdin.on('error', () => {});
    child.stdin.end(JSON.stringify(request));
    child.stdout.on('data', (b) => {
      stdout += b.toString();
      if (stdout.length > 1024 * 1024) child.kill();
    });
    child.stderr.resume();
    child.once('error', () => {
      clearTimeout(timer);
      reject(Error('VALIDATION_TRANSPORT_FAILED'));
    });
    child.once('close', (code) => {
      clearTimeout(timer);
      try {
        if (code !== 0) throw Error();
        resolve(JSON.parse(stdout));
      } catch {
        reject(Error('VALIDATION_TRANSPORT_FAILED'));
      }
    });
  });
}

async function update(db: DB, value: any, state: string, extra: any = {}) {
  const next = { ...value, ...extra, state, updatedAt: new Date().toISOString() };
  await db.query('UPDATE settings SET value=$2 WHERE id=$1', [
    'candidate-validation:' + value.actionId,
    JSON.stringify(next),
  ]);
  await event(db, 'CANDIDATE_VALIDATION_' + state.toUpperCase(), value.taskId, {
    actionId: value.actionId,
    goalId: value.goalId,
    runId: value.runId,
    candidateDigest: value.candidateDigest,
    code: next.result?.code ?? next.code,
    acceptance: false,
  });
}

/** Remote command completion is never task acceptance. Ambiguous ownership
 * survives restart and probes only; lease age never permits a second command. */
export async function reconcileCandidateValidations(
  dispatch: Dispatch = dispatchCandidateValidation,
  inspect = inspectStopped,
) {
  const rows = (
    await pool.query(
      "SELECT id,value FROM settings WHERE id LIKE 'candidate-validation:%' AND value->>'state'=ANY($1::text[]) ORDER BY id",
      [owned],
    )
  ).rows;
  for (const row of rows) {
    const claim = await tx(async (db) => {
      await lock(db);
      const current = (
        await db.query('SELECT value FROM settings WHERE id=$1 FOR UPDATE', [row.id])
      ).rows[0]?.value;
      if (!current || !owned.includes(current.state)) return null;
      if (
        ['freezing', 'running'].includes(current.state) &&
        Date.now() - Date.parse(current.claimedAt ?? '') < 60000
      )
        return null;
      if (['freezing', 'running', 'uncertain'].includes(current.state)) {
        if (Date.now() < Date.parse(current.nextProbeAt ?? '')) return null;
        await db.query('UPDATE settings SET value=$2 WHERE id=$1', [
          row.id,
          JSON.stringify({ ...current, nextProbeAt: new Date(Date.now() + 15000).toISOString() }),
        ]);
        return { value: current, mode: 'receipt' as const };
      }
      try {
        const target = await authorizedBinding(db,current);
        if (target.contractDigest !== current.request.contractDigest)
          throw Error('VALIDATION_CONTRACT_CHANGED');
        if (
          target.project !== current.request.project ||
          requestDigest(target.authorization) !==
            requestDigest(current.request.frameworkAuthorization)
        )
          throw Error('VALIDATION_PROJECT_AUTHORIZATION_CHANGED');
      } catch (error: any) {
        await update(db, current, 'rejected', { code: error.message });
        return null;
      }
      const claimId = randomUUID(),
        state = current.state === 'proposed' ? 'freezing' : 'running';
      await update(db, current, state, { claimId, claimedAt: new Date().toISOString() });
      return {
        value: { ...current, state, claimId },
        mode: state === 'freezing' ? ('freeze' as const) : ('execute' as const),
      };
    });
    if (!claim) continue;
    let result: any;
    try {
      if (claim.mode === 'freeze') {
        const target = await authorizedBinding(pool,claim.value);
        const proof = await inspect(target.run.id, target.task.execution.kind === 'opencode');
        assertStopped(proof, target.run, target.task);
        await canonicalCandidate(pool, claim.value, proof);
      }
      result = await dispatch(claim.mode, claim.value.request);
    } catch {
      result = { state: 'uncertain', code: 'VALIDATION_OUTCOME_UNCONFIRMED' };
    }
    await tx(async (db) => {
      await lock(db);
      const current = (
        await db.query('SELECT value FROM settings WHERE id=$1 FOR UPDATE', [row.id])
      ).rows[0]?.value;
      if (
        !current ||
        !owned.includes(current.state) ||
        (claim.mode !== 'receipt' && current.claimId !== claim.value.claimId)
      )
        return;
      if (
        result.actionId !== current.actionId ||
        result.candidateDigest !== current.candidateDigest ||
        result.requestDigest !== requestDigest(current.request) ||
        !['frozen', 'running', 'completed', 'failed', 'rejected', 'uncertain'].includes(
          result.state,
        )
      ) {
        await update(db, current, 'uncertain', { code: 'VALIDATION_RECEIPT_UNCONFIRMED' });
        return;
      }
      if (result.state === 'frozen' && ['freeze', 'receipt'].includes(claim.mode))
        await update(db, current, 'frozen', { frozenWorkspace: result.frozenWorkspace });
      else if (['completed', 'failed', 'rejected'].includes(result.state)) {
        // Keep immutable evidence but discriminate stale current authority.
        const target = await readDeliverySourceIdentity(db, current.taskId);
        const stale =
          !target ||
          target.run_id !== current.runId ||
          target.fence !== current.runFence ||
          target.contract_digest !== current.request.contractDigest;
        await update(db, current, result.state, {
          result: { ...result, stale, acceptance: false },
          completedAt: new Date().toISOString(),
        });
      } else
        await update(db, current, 'uncertain', {
          result,
          code: result.code ?? 'VALIDATION_OWNER_UNCONFIRMED',
        });
    });
  }
}

export async function readCandidateValidations(db: DB, goalId: string) {
  return (
    await db.query(
      `SELECT value-'request' AS value FROM settings WHERE id LIKE 'candidate-validation:%'
    AND value->>'goalId'=$1 ORDER BY value->>'proposedAt' DESC LIMIT 20`,
      [goalId],
    )
  ).rows.map((row) => row.value);
}
