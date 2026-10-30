import { createHash, randomUUID } from 'node:crypto';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { config } from './config.js';
import { event, pool, type DB } from './db.js';
import { frameworkProjectAuthorization } from './candidate-validation.js';
import { acceptedSnapshotProof } from './deferred-handoffs.js';
import { readArtifact } from './service.js';
import { runtimeCandidateDigest } from './operation-progress.js';
import {migrationTableLocks} from './migration-table-locks.js';

const failure = (code: string) => Object.assign(new Error(code), { code });
const sha = (value: string | Buffer) => createHash('sha256').update(value).digest('hex');
const canonical = (value: any): any => Array.isArray(value) ? value.map(canonical) : value && typeof value === 'object'
  ? Object.fromEntries(Object.keys(value).sort().map(key => [key, canonical(value[key])])) : value;
const digest = (value: any) => sha(JSON.stringify(canonical(value)));
const jobId = (taskId: string) => `verification-source-binding:${taskId}`;

type BindingParent = { task: any; proof: any; artifact: any; candidate: any; rebase: any };
type BindingSnapshot = { task: any; goal: any; authorization: any; parent: BindingParent; dependencies: any[] };

async function readSnapshot(db: DB, taskId: string, includeCandidateBytes = true, expectedCandidateDigest?: string): Promise<BindingSnapshot> {
  const task = (await db.query('SELECT * FROM tasks WHERE id=$1', [taskId])).rows[0];
  if (!task) throw failure('VERIFICATION_BINDING_TASK_MISSING');
  const goal = (await db.query('SELECT id,project,state FROM goals WHERE id=$1', [task.goal_id])).rows[0];
  const authorization = await frameworkProjectAuthorization(db, task.goal_id);
  const dependencies = (await db.query(`SELECT d.depends_on task_id,p.goal_id,p.stage,p.state,p.execution
    FROM dependencies d JOIN tasks p ON p.id=d.depends_on WHERE d.task_id=$1 ORDER BY p.id`, [taskId])).rows;
  if (task.stage !== 'verify' || task.execution?.kind !== 'harness' || task.execution?.workKind !== 'verification' ||
    task.execution?.readOnlySource !== true || !task.execution?.workspace || !task.execution?.sourceWorkspace ||
    !task.execution?.baseWorkspace || task.attempt_count !== 0 || task.goal_id !== goal?.id ||
    goal?.state !== 'active' || authorization.project !== goal.project || dependencies.some(d => d.goal_id !== goal.id || d.state !== 'accepted'))
    throw failure('VERIFICATION_BINDING_TASK_INELIGIBLE');
  const allRuns = await db.query('SELECT count(*)::int count FROM runs WHERE task_id=$1', [taskId]);
  if (allRuns.rows[0]?.count !== 0 || (await db.query('SELECT 1 FROM leases l JOIN runs r ON r.id=l.run_id WHERE r.task_id=$1 LIMIT 1', [taskId])).rowCount ||
      (await db.query('SELECT 1 FROM opencode_jobs j JOIN runs r ON r.id=j.run_id WHERE r.task_id=$1 LIMIT 1', [taskId])).rowCount ||
      (await db.query("SELECT 1 FROM events WHERE entity_id=$1 AND kind='CANCEL_REQUESTED' LIMIT 1", [taskId])).rowCount ||
      (await db.query("SELECT 1 FROM supervision_cases WHERE task_id=$1 AND state IN('open','decided','delegated','waiting_evidence') LIMIT 1", [taskId])).rowCount)
    throw failure('VERIFICATION_BINDING_TASK_HAS_HISTORY');
  const scope = (execution: any) => execution?.scope ?? execution?.baseWorkspace;
  const candidates = dependencies.filter(d => d.stage === 'integrate' && d.execution?.baseWorkspace === task.execution.baseWorkspace &&
    scope(d.execution) === scope(task.execution) &&
    d.execution?.kind === 'harness' && d.execution?.readOnlySource !== true && d.execution?.publishedRebase?.version === 1 &&
    d.execution?.publishedRebase?.priorWorkspace === task.execution.sourceWorkspace &&
    d.execution?.workspace === d.execution?.publishedRebase?.workspace);
  if (candidates.length !== 1) throw failure('VERIFICATION_BINDING_PARENT_AMBIGUOUS');
  const parent = (await db.query('SELECT * FROM tasks WHERE id=$1 AND goal_id=$2', [candidates[0].task_id, goal.id])).rows[0];
  const rebase = parent?.execution?.publishedRebase;
  if (!parent || parent.state !== 'accepted' || parent.execution?.workspace !== rebase?.workspace ||
    parent.execution?.sourceWorkspace !== rebase?.baselineWorkspace || rebase?.priorWorkspace !== task.execution.sourceWorkspace ||
    !/^[0-9a-f-]{36}$/.test(rebase?.actionId ?? '') || !/^[a-f0-9]{64}$/.test(rebase?.requestDigest ?? '') ||
    !/^[a-f0-9]{64}$/.test(rebase?.publishedBaselineDigest ?? '') || !/^[a-f0-9]{64}$/.test(rebase?.preparedReceiptDigest ?? ''))
    throw failure('VERIFICATION_BINDING_PARENT_CHANGED');
  const receipt = (await db.query('SELECT value FROM settings WHERE id=$1', ['published-rebase:' + rebase.actionId])).rows[0]?.value;
  if (!receipt || receipt.state !== 'applied' || receipt.actionId !== rebase.actionId || receipt.taskId !== parent.id || receipt.goalId !== goal.id ||
    receipt.requestDigest !== rebase.requestDigest || receipt.targetWorkspace !== parent.execution.workspace ||
    receipt.currentWorkspace !== rebase.priorWorkspace || receipt.prepared?.sourceChanged !== true ||
    receipt.prepared?.receiptDigest !== rebase.preparedReceiptDigest)
    throw failure('VERIFICATION_BINDING_REBASE_PROOF_INVALID');
  const proof = await acceptedSnapshotProof(db, parent);
  if (proof.workspace !== parent.execution.workspace) throw failure('VERIFICATION_BINDING_PARENT_NOT_CURRENT');
  const refs = (await db.query("SELECT id,run_id,name,digest,size FROM artifacts WHERE run_id=$1 AND name='candidate.json' ORDER BY id", [proof.runId])).rows;
  if (refs.length !== 1) throw failure('VERIFICATION_BINDING_CANDIDATE_MISSING');
  let artifact: any = refs[0], candidate: any = { candidateDigest: expectedCandidateDigest ?? rebase.candidateDigest };
  if (includeCandidateBytes) {
    artifact = await readArtifact(refs[0].id);
    if (artifact.run_id !== proof.runId || artifact.name !== 'candidate.json' || artifact.bytes.length !== refs[0].size || sha(artifact.bytes) !== refs[0].digest)
      throw failure('VERIFICATION_BINDING_CANDIDATE_CORRUPT');
    try { candidate = JSON.parse(artifact.bytes.toString('utf8')); } catch { throw failure('VERIFICATION_BINDING_CANDIDATE_INVALID'); }
    if (!candidate || !/^[a-f0-9]{64}$/.test(candidate.candidateDigest ?? '') ||
      runtimeCandidateDigest(artifact.bytes, artifact.digest) !== candidate.candidateDigest)
      throw failure('VERIFICATION_BINDING_CANDIDATE_CHANGED');
  } else if (artifact.digest !== refs[0].digest || !Number.isSafeInteger(artifact.size) ||
    !/^[a-f0-9]{64}$/.test(candidate.candidateDigest ?? '')) {
    throw failure('VERIFICATION_BINDING_CANDIDATE_CHANGED');
  }
  return { task, goal, authorization, parent: { task: parent, proof, artifact, candidate, rebase: receipt }, dependencies };
}

function requestDigest(value: any) { const copy = { ...value }; delete copy.requestDigest; delete copy.state;
  for (const key of ['claimedAt', 'prepared', 'preparedAt', 'errorCode', 'updatedAt', 'appliedAt']) delete copy[key];
  return digest(copy); }

function makeRequest(snapshot: BindingSnapshot, id: string) {
  const { task, goal, parent } = snapshot;
  const targetWorkspace = `${goal.project}/o-vb-${id.replaceAll('-', '')}`;
  const request: any = { version: 1, actionId: id, taskId: task.id, goalId: goal.id, project: goal.project,
    authorization: snapshot.authorization.authorization, expectedTask: { attempt: task.attempt_count, state: task.state,
      workspace: task.execution.workspace, sourceWorkspace: task.execution.sourceWorkspace, baseWorkspace: task.execution.baseWorkspace,
      criteriaDigest: digest(task.criteria), dependencies: snapshot.dependencies.map(d => d.task_id), dependenciesDigest: digest(snapshot.dependencies.map(d => d.task_id)) },
    parent: { taskId: parent.task.id, attempt: parent.proof.attempt, runId: parent.proof.runId, fence: parent.proof.fence,
      candidateArtifactId: parent.artifact.id, candidateArtifactDigest: parent.artifact.digest, candidateArtifactSize: parent.artifact.bytes.length,
      candidateDigest: parent.candidate.candidateDigest, rebaseActionId: parent.rebase.actionId,
      rebaseRequestDigest: parent.rebase.requestDigest, rebaseReceiptDigest: parent.rebase.preparedReceiptDigest,
      workspace: parent.task.execution.workspace, baselineWorkspace: parent.task.execution.sourceWorkspace,
      priorWorkspace: parent.rebase.priorWorkspace, proofManifestDigest: parent.proof.manifestDigest },
    targetWorkspace, sourceWorkspace: parent.task.execution.workspace,
    originalWorkspace: task.execution.workspace, originalSourceWorkspace: task.execution.sourceWorkspace,
    baseWorkspace: task.execution.baseWorkspace, state: 'proposed' };
  request.requestDigest = requestDigest(request);
  return request;
}

export async function installVerificationSourceBinding(db: DB = pool) {
  await db.query(migrationTableLocks(['acceptances','artifacts','dependencies','runs','settings','tasks'])+`CREATE OR REPLACE FUNCTION agentos_verification_binding_owned(target uuid) RETURNS boolean LANGUAGE sql STABLE AS $$
    SELECT EXISTS(SELECT 1 FROM settings s WHERE s.id='verification-source-binding:'||target::text
      AND s.value->>'state'=ANY(ARRAY['proposed','preparing','prepared','uncertain','applying'])) $$;
    CREATE OR REPLACE FUNCTION agentos_verification_binding_required(target uuid) RETURNS boolean LANGUAGE sql STABLE AS $$
      SELECT EXISTS(SELECT 1 FROM tasks q JOIN dependencies d ON d.task_id=q.id JOIN tasks p ON p.id=d.depends_on
        JOIN settings rb ON rb.id='published-rebase:'||(p.execution#>>'{publishedRebase,actionId}')
        WHERE q.id=target AND q.stage='verify' AND q.execution->>'kind'='harness'
          AND q.execution->>'workKind'='verification' AND q.execution->>'readOnlySource'='true'
          AND p.stage='integrate' AND p.goal_id=q.goal_id AND p.execution->>'baseWorkspace'=q.execution->>'baseWorkspace'
          AND rb.value->>'state'='applied' AND rb.value->>'taskId'=p.id::text
          AND rb.value->>'requestDigest'=p.execution#>>'{publishedRebase,requestDigest}'
          AND p.execution->>'workspace'=p.execution#>>'{publishedRebase,workspace}'
          ) $$;
    CREATE OR REPLACE FUNCTION agentos_verification_binding_guard() RETURNS trigger LANGUAGE plpgsql AS $$
    DECLARE row_value jsonb; task_id uuid; expected jsonb;
    BEGIN
      PERFORM pg_advisory_xact_lock(4280001);
      IF TG_TABLE_NAME='runs' THEN
        task_id:=NEW.task_id;
        IF agentos_verification_binding_owned(task_id) THEN RAISE EXCEPTION 'VERIFICATION_BINDING_OWNS_TASK'; END IF;
        IF agentos_verification_binding_required(task_id) AND NOT EXISTS(SELECT 1 FROM tasks q JOIN settings b
          ON b.id='verification-source-binding:'||q.id::text AND b.value->>'state'='applied'
          WHERE q.id=task_id AND q.execution->'verificationSourceBinding'=b.value->'binding'
            AND q.execution#>>'{verificationSourceBinding,actionId}'=b.value->>'actionId'
            AND q.execution#>>'{verificationSourceBinding,requestDigest}'=b.value->>'requestDigest'
            AND q.execution#>>'{verificationSourceBinding,sourceWorkspace}'=q.execution->>'sourceWorkspace'
            AND q.execution#>>'{verificationSourceBinding,targetWorkspace}'=q.execution->>'workspace'
            AND q.execution#>>'{verificationSourceBinding,rebaseActionId}'=b.value#>>'{parent,rebaseActionId}'
            AND q.execution#>>'{verificationSourceBinding,parentTaskId}'=b.value#>>'{parent,taskId}'
            AND EXISTS(SELECT 1 FROM tasks p JOIN dependencies d ON d.depends_on=p.id
              JOIN runs pr ON pr.id=(b.value#>>'{parent,runId}')::uuid
              JOIN acceptances pa ON pa.run_id=pr.id AND pa.task_id=p.id AND pa.passed=true
              JOIN artifacts ca ON ca.run_id=pr.id AND ca.id=(b.value#>>'{parent,candidateArtifactId}')::uuid
                AND ca.name='candidate.json' AND ca.digest=b.value#>>'{parent,candidateArtifactDigest}'
                AND ca.size=(b.value#>>'{parent,candidateArtifactSize}')::bigint
              JOIN settings rb ON rb.id='published-rebase:'||(p.execution#>>'{publishedRebase,actionId}')
              WHERE d.task_id=q.id AND p.id=(b.value#>>'{parent,taskId}')::uuid AND p.goal_id=q.goal_id
                AND p.state='accepted' AND p.execution->>'workspace'=b.value#>>'{parent,workspace}'
                AND p.execution#>>'{publishedRebase,actionId}'=b.value#>>'{parent,rebaseActionId}'
                AND p.execution#>>'{publishedRebase,requestDigest}'=b.value#>>'{parent,rebaseRequestDigest}'
                AND rb.value->>'state'='applied' AND rb.value#>>'{prepared,receiptDigest}'=b.value#>>'{parent,rebaseReceiptDigest}'
                AND pr.state='submitted' AND pr.fence::text=b.value#>>'{parent,fence}'
                AND pr.attempt=(b.value#>>'{parent,attempt}')::int AND pr.finished_at IS NOT NULL)) THEN
          RAISE EXCEPTION 'VERIFICATION_SOURCE_BINDING_REQUIRED';
        END IF;
        RETURN NEW;
      ELSIF TG_TABLE_NAME='dependencies' THEN
        IF (TG_OP<>'DELETE' AND agentos_verification_binding_owned(NEW.task_id)) OR
           (TG_OP<>'INSERT' AND agentos_verification_binding_owned(OLD.task_id)) THEN RAISE EXCEPTION 'VERIFICATION_BINDING_OWNS_DEPENDENCIES'; END IF;
        IF (TG_OP<>'DELETE' AND agentos_verification_binding_required(NEW.task_id) AND
            NOT EXISTS(SELECT 1 FROM runs r WHERE r.task_id=NEW.task_id)) OR
           (TG_OP<>'INSERT' AND agentos_verification_binding_required(OLD.task_id) AND
            NOT EXISTS(SELECT 1 FROM runs r WHERE r.task_id=OLD.task_id)) THEN
          RAISE EXCEPTION 'VERIFICATION_SOURCE_BINDING_CONTRACT_FROZEN';
        END IF;
        IF TG_OP='DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;
      END IF;
      IF NOT agentos_verification_binding_owned(OLD.id) THEN
        IF agentos_verification_binding_required(OLD.id) AND NOT EXISTS(SELECT 1 FROM runs r WHERE r.task_id=OLD.id) THEN
          IF NEW.execution IS DISTINCT FROM OLD.execution THEN RAISE EXCEPTION 'VERIFICATION_SOURCE_BINDING_REQUIRED'; END IF;
          IF NEW.goal_id<>OLD.goal_id OR NEW.description<>OLD.description OR NEW.criteria<>OLD.criteria OR
            NEW.resources<>OLD.resources OR NEW.required_memories<>OLD.required_memories OR NEW.allowed_accounts<>OLD.allowed_accounts OR
            NEW.attempt_count<>OLD.attempt_count OR NEW.max_attempts<>OLD.max_attempts OR
            NEW.instruction_revision_id IS DISTINCT FROM OLD.instruction_revision_id OR
            (to_jsonb(NEW)-ARRAY['state','reason','next_attempt_at','updated_at']) IS DISTINCT FROM
              (to_jsonb(OLD)-ARRAY['state','reason','next_attempt_at','updated_at']) THEN
            RAISE EXCEPTION 'VERIFICATION_SOURCE_BINDING_CONTRACT_FROZEN';
          END IF;
        END IF;
        RETURN NEW;
      END IF;
      SELECT value INTO row_value FROM settings WHERE id='verification-source-binding:'||OLD.id::text FOR SHARE;
      IF row_value IS NULL THEN RAISE EXCEPTION 'VERIFICATION_BINDING_RECEIPT_MISSING'; END IF;
      IF row_value->>'state'='applying' THEN
        expected:=jsonb_set(jsonb_set(OLD.execution,'{workspace}',to_jsonb(row_value->>'targetWorkspace')),
          '{sourceWorkspace}',to_jsonb(row_value#>>'{parent,workspace}')) ||
          jsonb_build_object('verificationSourceBinding',row_value->'binding');
        IF NEW.execution IS DISTINCT FROM expected OR NEW.goal_id<>OLD.goal_id OR NEW.description<>OLD.description OR NEW.criteria<>OLD.criteria OR
          NEW.resources<>OLD.resources OR NEW.required_memories<>OLD.required_memories OR NEW.allowed_accounts<>OLD.allowed_accounts OR
          NEW.attempt_count<>0 OR NEW.max_attempts<>OLD.max_attempts OR NEW.instruction_revision_id IS DISTINCT FROM OLD.instruction_revision_id OR
          NEW.state IS DISTINCT FROM OLD.state OR NEW.reason IS DISTINCT FROM OLD.reason OR NEW.next_attempt_at IS DISTINCT FROM OLD.next_attempt_at OR
          (to_jsonb(NEW)-ARRAY['execution','updated_at']) IS DISTINCT FROM (to_jsonb(OLD)-ARRAY['execution','updated_at']) THEN
          RAISE EXCEPTION 'VERIFICATION_BINDING_CONTRACT_CHANGED';
        END IF;
        RETURN NEW;
      END IF;
      IF NEW.execution IS DISTINCT FROM OLD.execution OR NEW.goal_id<>OLD.goal_id OR NEW.description<>OLD.description OR NEW.criteria<>OLD.criteria OR
        NEW.resources<>OLD.resources OR NEW.required_memories<>OLD.required_memories OR NEW.allowed_accounts<>OLD.allowed_accounts OR
        NEW.attempt_count<>OLD.attempt_count OR NEW.instruction_revision_id IS DISTINCT FROM OLD.instruction_revision_id OR
        (to_jsonb(NEW)-ARRAY['state','reason','next_attempt_at','updated_at']) IS DISTINCT FROM
          (to_jsonb(OLD)-ARRAY['state','reason','next_attempt_at','updated_at']) THEN
        RAISE EXCEPTION 'VERIFICATION_BINDING_OWNS_TASK';
      END IF;
      RETURN NEW;
    END $$;
    CREATE OR REPLACE FUNCTION agentos_verification_binding_settings_guard() RETURNS trigger LANGUAGE plpgsql AS $$
    DECLARE old_state text; new_state text; before_value jsonb; after_value jsonb;
    BEGIN
      IF TG_OP='DELETE' THEN IF OLD.id LIKE 'verification-source-binding:%' THEN RAISE EXCEPTION 'VERIFICATION_BINDING_RECEIPT_IMMUTABLE'; END IF; RETURN OLD; END IF;
      IF TG_OP='UPDATE' AND OLD.id LIKE 'verification-source-binding:%' AND NEW.id<>OLD.id THEN RAISE EXCEPTION 'VERIFICATION_BINDING_RECEIPT_IMMUTABLE'; END IF;
      IF NEW.id !~ '^verification-source-binding:[0-9a-f-]{36}$' THEN RETURN NEW; END IF;
      IF TG_OP='INSERT' THEN
        IF NEW.id<>'verification-source-binding:'||(NEW.value->>'taskId') OR NEW.value->>'state'<>'proposed' OR
          NEW.value->>'actionId'!~'^[0-9a-f-]{36}$' OR NEW.value->>'requestDigest'!~'^[a-f0-9]{64}$' THEN RAISE EXCEPTION 'VERIFICATION_BINDING_RECEIPT_INVALID'; END IF;
        RETURN NEW;
      END IF;
      before_value:=OLD.value; after_value:=NEW.value; old_state:=before_value->>'state'; new_state:=after_value->>'state';
      IF (before_value-'state'-'claimedAt'-'prepared'-'preparedAt'-'applyingAt'-'appliedAt'-'errorCode'-'updatedAt'-'binding') IS DISTINCT FROM
         (after_value-'state'-'claimedAt'-'prepared'-'preparedAt'-'applyingAt'-'appliedAt'-'errorCode'-'updatedAt'-'binding') THEN RAISE EXCEPTION 'VERIFICATION_BINDING_RECEIPT_IMMUTABLE'; END IF;
      IF before_value->'applyingAt' IS DISTINCT FROM after_value->'applyingAt' AND NOT
        (old_state='prepared' AND new_state='applying' AND before_value->'applyingAt' IS NULL AND jsonb_typeof(after_value->'applyingAt')='string') THEN
        RAISE EXCEPTION 'VERIFICATION_BINDING_RECEIPT_IMMUTABLE';
      END IF;
      IF before_value->'appliedAt' IS DISTINCT FROM after_value->'appliedAt' AND NOT
        (old_state='applying' AND new_state='applied' AND before_value->'appliedAt' IS NULL AND jsonb_typeof(after_value->'appliedAt')='string') THEN
        RAISE EXCEPTION 'VERIFICATION_BINDING_RECEIPT_IMMUTABLE';
      END IF;
      IF before_value->'binding' IS DISTINCT FROM after_value->'binding' AND NOT
        (old_state='prepared' AND new_state='applying' AND before_value->'binding' IS NULL AND jsonb_typeof(after_value->'binding')='object') THEN
        RAISE EXCEPTION 'VERIFICATION_BINDING_RECEIPT_IMMUTABLE';
      END IF;
      IF old_state='prepared' AND before_value->'prepared' IS DISTINCT FROM after_value->'prepared' THEN RAISE EXCEPTION 'VERIFICATION_BINDING_PREPARED_IMMUTABLE'; END IF;
      IF old_state='prepared' AND new_state='applying' AND (
        after_value#>>'{binding,actionId}' IS DISTINCT FROM after_value->>'actionId' OR
        after_value#>>'{binding,requestDigest}' IS DISTINCT FROM after_value->>'requestDigest' OR
        after_value#>>'{binding,parentTaskId}' IS DISTINCT FROM after_value#>>'{parent,taskId}' OR
        after_value#>>'{binding,parentRunId}' IS DISTINCT FROM after_value#>>'{parent,runId}' OR
        after_value#>>'{binding,parentFence}' IS DISTINCT FROM after_value#>>'{parent,fence}' OR
        after_value#>>'{binding,parentAttempt}' IS DISTINCT FROM after_value#>>'{parent,attempt}' OR
        after_value#>>'{binding,candidateArtifactId}' IS DISTINCT FROM after_value#>>'{parent,candidateArtifactId}' OR
        after_value#>>'{binding,candidateArtifactDigest}' IS DISTINCT FROM after_value#>>'{parent,candidateArtifactDigest}' OR
        after_value#>>'{binding,candidateDigest}' IS DISTINCT FROM after_value#>>'{parent,candidateDigest}' OR
        after_value#>>'{binding,sourceManifestDigest}' IS DISTINCT FROM after_value#>>'{prepared,sourceManifestDigest}' OR
        after_value#>>'{binding,sourceProofDigest}' IS DISTINCT FROM after_value#>>'{prepared,receiptDigest}' OR
        after_value#>>'{binding,sourceWorkspace}' IS DISTINCT FROM after_value->>'sourceWorkspace' OR
        after_value#>>'{binding,targetWorkspace}' IS DISTINCT FROM after_value->>'targetWorkspace' OR
        after_value#>>'{binding,baseWorkspace}' IS DISTINCT FROM after_value->>'baseWorkspace' OR
        after_value#>>'{binding,rebaseActionId}' IS DISTINCT FROM after_value#>>'{parent,rebaseActionId}' OR
        after_value#>>'{binding,rebaseReceiptDigest}' IS DISTINCT FROM after_value#>>'{parent,rebaseReceiptDigest}' OR
        after_value#>>'{binding,acceptance}' IS DISTINCT FROM 'false') THEN
        RAISE EXCEPTION 'VERIFICATION_BINDING_PROOF_INVALID';
      END IF;
      IF NOT ((old_state='proposed' AND new_state IN('preparing','failed')) OR
        (old_state='preparing' AND new_state IN('prepared','uncertain','failed') OR
          (new_state='proposed' AND after_value->>'errorCode'='VERIFICATION_BINDING_SOURCE_BUSY')) OR
        (old_state='uncertain' AND new_state IN('prepared','failed')) OR
        (old_state='prepared' AND new_state IN('applying','uncertain','failed')) OR
        (old_state='applying' AND new_state='applied')) THEN RAISE EXCEPTION 'VERIFICATION_BINDING_STATE_INVALID'; END IF;
      RETURN NEW;
    END $$;
    DROP TRIGGER IF EXISTS agentos_verification_binding_settings_guard ON settings;
    CREATE TRIGGER agentos_verification_binding_settings_guard BEFORE INSERT OR UPDATE OR DELETE ON settings FOR EACH ROW EXECUTE FUNCTION agentos_verification_binding_settings_guard();
    DROP TRIGGER IF EXISTS a_agentos_verification_binding_guard ON tasks;
    CREATE TRIGGER a_agentos_verification_binding_guard BEFORE UPDATE ON tasks FOR EACH ROW EXECUTE FUNCTION agentos_verification_binding_guard();
    DROP TRIGGER IF EXISTS a_agentos_verification_binding_guard ON runs;
    CREATE TRIGGER a_agentos_verification_binding_guard BEFORE INSERT ON runs FOR EACH ROW EXECUTE FUNCTION agentos_verification_binding_guard();
    DROP TRIGGER IF EXISTS a_agentos_verification_binding_guard ON dependencies;
    CREATE TRIGGER a_agentos_verification_binding_guard BEFORE INSERT OR UPDATE OR DELETE ON dependencies FOR EACH ROW EXECUTE FUNCTION agentos_verification_binding_guard();`);
}

async function dispatch(mode: 'prepare' | 'receipt', taskId: string) {
  if (!/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(taskId)) throw failure('VERIFICATION_BINDING_ID_INVALID');
  const script = fileURLToPath(new URL('../scripts/agentos-verification-source-binding.py', import.meta.url)).replaceAll('\\', '/');
  if (!/^\/opt\/agentos\/releases\/managed-[0-9a-f-]{36}\/scripts\/agentos-verification-source-binding\.py$/.test(script)) throw failure('VERIFICATION_BINDING_BROKER_UNDEPLOYED');
  return new Promise<any>((resolve, reject) => {
    const child = spawn('ssh', ['-T', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=8', config.bridgeHost,
      '/usr/bin/python3', script, mode, taskId], { windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'] });
    let out = ''; const timer = setTimeout(() => child.kill('SIGTERM'), 60000);
    child.stdout.on('data', b => { out += b.toString(); if (out.length > 128 * 1024) child.kill(); }); child.stderr.resume();
    child.once('error', () => { clearTimeout(timer); reject(failure('VERIFICATION_BINDING_TRANSPORT_UNCERTAIN')); });
    child.once('close', code => { clearTimeout(timer); try { const value = JSON.parse(out); if (code !== 0) throw failure(value?.error ?? 'VERIFICATION_BINDING_TRANSPORT_UNCERTAIN'); resolve(value); }
      catch (e: any) { reject(/^VERIFICATION_BINDING_[A-Z_]+$/.test(e?.message ?? '') ? e : failure('VERIFICATION_BINDING_TRANSPORT_UNCERTAIN')); } });
  });
}

export async function readVerificationSourceBindingCandidates(db: DB = pool) {
  return (await db.query(`SELECT q.id FROM tasks q JOIN goals g ON g.id=q.goal_id
    WHERE q.stage='verify' AND q.execution->>'kind'='harness' AND q.execution->>'readOnlySource'='true'
      AND q.execution->>'workKind'='verification' AND q.attempt_count=0 AND g.state='active'
      AND NOT EXISTS(SELECT 1 FROM runs r WHERE r.task_id=q.id)
      AND EXISTS(SELECT 1 FROM dependencies d JOIN tasks p ON p.id=d.depends_on
        JOIN settings rb ON rb.id='published-rebase:'||(p.execution#>>'{publishedRebase,actionId}')
        WHERE d.task_id=q.id AND p.goal_id=q.goal_id AND p.stage='integrate' AND p.execution->>'baseWorkspace'=q.execution->>'baseWorkspace'
          AND rb.value->>'state'='applied' AND q.execution->>'sourceWorkspace'=p.execution#>>'{publishedRebase,priorWorkspace}')
      AND NOT EXISTS(SELECT 1 FROM settings b WHERE b.id='verification-source-binding:'||q.id::text AND b.value->>'state' IN('applied','failed'))
    ORDER BY q.created_at,q.id LIMIT 8`)).rows;
}

export async function reconcileVerificationSourceBindings() {
  const possible = await readVerificationSourceBindingCandidates(pool);
  const results: any[] = [];
  for (const observed of possible) {
    const taskId = observed.id;
    const owner = await pool.connect(); let held = false;
    try {
      held = !!(await owner.query('SELECT pg_try_advisory_lock(hashtextextended($1,4280411)) held', [taskId])).rows[0]?.held;
      if (!held) continue;
      const existing = (await owner.query('SELECT value FROM settings WHERE id=$1', [jobId(taskId)])).rows[0]?.value;
      if (!existing) {
        let snapshot: BindingSnapshot;
        try { snapshot = await readSnapshot(owner, taskId); } catch { continue; }
        const id = randomUUID(), request = makeRequest(snapshot, id);
        const inserted = await owner.query('INSERT INTO settings(id,value) VALUES($1,$2) ON CONFLICT DO NOTHING RETURNING id', [jobId(taskId), JSON.stringify(request)]);
        if (!inserted.rowCount) continue;
        await event(owner, 'VERIFICATION_SOURCE_BINDING_PROPOSED', taskId, { actionId: id, requestDigest: request.requestDigest,
          parentTaskId: request.parent.taskId, parentRunId: request.parent.runId, candidateDigest: request.parent.candidateDigest, acceptance: false });
        results.push({ taskId, state: 'proposed' });
        continue;
      }
      if (!['proposed','preparing','prepared','uncertain'].includes(existing.state)) continue;
      let current = existing;
      if (current.state === 'preparing') {
        current = { ...current, state: 'uncertain', updatedAt: new Date().toISOString() };
        await owner.query(`UPDATE settings SET value=$2 WHERE id=$1 AND value->>'state'=$3`, [jobId(taskId), JSON.stringify(current), 'preparing']);
      }
      try {
        if (current.state === 'proposed') {
          const retryable = { ...current }; delete retryable.errorCode;
          current = { ...retryable, state: 'preparing', claimedAt: new Date().toISOString() };
          await owner.query(`UPDATE settings SET value=$2 WHERE id=$1 AND value->>'state'=$3`, [jobId(taskId), JSON.stringify(current), 'proposed']);
        }
        const sourceProof = await dispatch(current.state === 'uncertain' || current.state === 'prepared' ? 'receipt' : 'prepare', taskId);
        const verified = await verifyRemoteReceipt(current, sourceProof);
        const latest = (await owner.query('SELECT value FROM settings WHERE id=$1', [jobId(taskId)])).rows[0]?.value;
        if (!latest || latest.actionId !== current.actionId || latest.requestDigest !== current.requestDigest || !['preparing','uncertain','prepared'].includes(latest.state)) continue;
        if (latest.state === 'prepared' && latest.prepared?.receiptDigest !== digest(verified)) throw failure('VERIFICATION_BINDING_RECEIPT_CHANGED');
        const cleanLatest = { ...latest }; delete cleanLatest.errorCode;
        current = { ...cleanLatest, state: 'prepared', prepared: { ...verified, receiptDigest: digest(verified) }, preparedAt: new Date().toISOString() };
        const persisted = await owner.query(`UPDATE settings SET value=$2 WHERE id=$1 AND value->>'state'=ANY(ARRAY['preparing','uncertain','prepared'])
          AND value->>'actionId'=$3 AND value->>'requestDigest'=$4 RETURNING id`,
          [jobId(taskId), JSON.stringify(current), current.actionId, current.requestDigest]);
        if (!persisted.rowCount) continue;
        await event(owner, 'VERIFICATION_SOURCE_BINDING_PREPARED', taskId, { actionId: current.actionId, requestDigest: current.requestDigest,
          parentTaskId: current.parent.taskId, parentRunId: current.parent.runId, candidateDigest: current.parent.candidateDigest,
          sourceManifestDigest: verified.sourceManifestDigest, acceptance: false });
        results.push({ taskId, state: 'prepared' });
      } catch (error: any) {
        const safeCode = /^VERIFICATION_BINDING_[A-Z_]+$/.test(error?.message ?? '') ? error.message : 'VERIFICATION_BINDING_TRANSPORT_UNCERTAIN';
        if (safeCode === 'VERIFICATION_BINDING_SOURCE_BUSY') {
          const latest = (await owner.query('SELECT value FROM settings WHERE id=$1', [jobId(taskId)])).rows[0]?.value;
          const retryState = current.state === 'preparing' ? 'proposed' : current.state === 'prepared' ? 'uncertain' : null;
          if (retryState && latest?.state === current.state && latest.actionId === current.actionId && latest.requestDigest === current.requestDigest)
            await owner.query(`UPDATE settings SET value=$2 WHERE id=$1 AND value->>'state'=$5
              AND value->>'actionId'=$3 AND value->>'requestDigest'=$4`,
              [jobId(taskId), JSON.stringify({ ...latest, state: retryState, errorCode: safeCode, updatedAt: new Date().toISOString() }),
                current.actionId, current.requestDigest, current.state]);
        } else if (safeCode === 'VERIFICATION_BINDING_TRANSPORT_UNCERTAIN') {
          const latest = (await owner.query('SELECT value FROM settings WHERE id=$1', [jobId(taskId)])).rows[0]?.value;
          if (latest && ['preparing','prepared'].includes(latest.state)) await owner.query(`UPDATE settings SET value=$2 WHERE id=$1
            AND value->>'state'=ANY(ARRAY['preparing','prepared']) AND value->>'actionId'=$3 AND value->>'requestDigest'=$4`,
            [jobId(taskId), JSON.stringify({ ...latest, state: 'uncertain', errorCode: safeCode, updatedAt: new Date().toISOString() }),
              latest.actionId, latest.requestDigest]);
        } else {
          const latest = (await owner.query('SELECT value FROM settings WHERE id=$1', [jobId(taskId)])).rows[0]?.value;
          if (latest && ['proposed','preparing','prepared','uncertain'].includes(latest.state)) await owner.query(`UPDATE settings SET value=$2 WHERE id=$1
            AND value->>'state'=ANY(ARRAY['proposed','preparing','prepared','uncertain']) AND value->>'actionId'=$3 AND value->>'requestDigest'=$4`,
            [jobId(taskId), JSON.stringify({ ...latest, state: 'failed', errorCode: safeCode, updatedAt: new Date().toISOString() }),
              latest.actionId, latest.requestDigest]);
        }
      }
    } finally {
      if (held) await owner.query('SELECT pg_advisory_unlock(hashtextextended($1,4280411))', [taskId]).catch(() => {});
      owner.release();
    }
  }
  return results;
}

function verifyRemoteReceipt(request: any, receipt: any) {
  const value = { ...receipt }; delete value.receiptDigest;
  if (receipt?.version !== 1 || receipt?.actionId !== request.actionId || receipt?.requestDigest !== request.requestDigest ||
    receipt?.taskId !== request.taskId || receipt?.goalId !== request.goalId || receipt?.project !== request.project ||
    receipt?.parentTaskId !== request.parent.taskId || receipt?.parentRunId !== request.parent.runId || receipt?.parentFence !== request.parent.fence ||
    receipt?.parentAttempt !== request.parent.attempt || receipt?.candidateArtifactId !== request.parent.candidateArtifactId ||
    receipt?.candidateArtifactDigest !== request.parent.candidateArtifactDigest || receipt?.candidateDigest !== request.parent.candidateDigest ||
    receipt?.sourceWorkspace !== request.sourceWorkspace || receipt?.targetWorkspace !== request.targetWorkspace ||
    receipt?.baseWorkspace !== request.baseWorkspace || receipt?.sourceChanged !== true ||
    !/^[a-f0-9]{64}$/.test(receipt?.sourceManifestDigest ?? '') || receipt.sourceManifestDigest !== request.parent.candidateDigest)
    throw failure('VERIFICATION_BINDING_REMOTE_PROOF_INVALID');
  return value;
}

export async function bindVerificationSourceForAdmission(db: DB, taskId: string) {
  const task = (await db.query('SELECT * FROM tasks WHERE id=$1 FOR UPDATE', [taskId])).rows[0];
  if (!task) return { required: false };
  const trigger = (await db.query(`SELECT p.id,p.execution FROM dependencies d JOIN tasks task ON task.id=d.task_id
    JOIN tasks p ON p.id=d.depends_on
    JOIN settings rb ON rb.id='published-rebase:'||(p.execution#>>'{publishedRebase,actionId}')
    WHERE d.task_id=$1 AND task.goal_id=$2 AND p.goal_id=$2 AND p.stage='integrate' AND p.execution->>'baseWorkspace'=$3
      AND task.stage='verify' AND task.execution->>'kind'='harness' AND task.execution->>'workKind'='verification'
      AND task.execution->>'readOnlySource'='true'
      AND COALESCE(p.execution->>'scope',p.execution->>'baseWorkspace')=COALESCE(task.execution->>'scope',task.execution->>'baseWorkspace')
      AND rb.value->>'state'='applied' AND rb.value->>'taskId'=p.id::text
      AND p.execution->>'workspace'=p.execution#>>'{publishedRebase,workspace}'
      AND $4=p.execution#>>'{publishedRebase,priorWorkspace}' ORDER BY p.id`,
    [taskId, task.goal_id, task.execution?.baseWorkspace, task.execution?.sourceWorkspace])).rows;
  if (!trigger.length) return { required: false };
  if (trigger.length !== 1) throw failure('VERIFICATION_SOURCE_BINDING_PARENT_AMBIGUOUS');
  const row = (await db.query('SELECT value FROM settings WHERE id=$1 FOR UPDATE', [jobId(taskId)])).rows[0]?.value;
  if (row?.state === 'failed') throw failure('VERIFICATION_SOURCE_BINDING_FAILED');
  if (!row || row.state !== 'prepared' || row.taskId !== taskId || row.expectedTask?.workspace !== task.execution.workspace ||
    row.expectedTask?.sourceWorkspace !== task.execution.sourceWorkspace || row.expectedTask?.baseWorkspace !== task.execution.baseWorkspace)
    throw failure('VERIFICATION_SOURCE_BINDING_NOT_PREPARED');
  if (!row.prepared) throw failure('VERIFICATION_SOURCE_BINDING_PROOF_MISSING');
  const { receiptDigest: pinnedReceiptDigest, ...sourceProof } = row.prepared;
  if (pinnedReceiptDigest !== digest(sourceProof) || sourceProof.sourceManifestDigest !== row.parent.candidateDigest ||
    sourceProof.sourceWorkspace !== row.sourceWorkspace || sourceProof.targetWorkspace !== row.targetWorkspace)
    throw failure('VERIFICATION_SOURCE_BINDING_PROOF_CHANGED');
  const parent = (await db.query('SELECT * FROM tasks WHERE id=$1 AND goal_id=$2 FOR UPDATE', [row.parent.taskId, task.goal_id])).rows[0];
  if (!parent || parent.state !== 'accepted' || parent.execution?.workspace !== row.parent.workspace ||
    parent.execution?.publishedRebase?.actionId !== row.parent.rebaseActionId || task.attempt_count !== 0 ||
    (await db.query('SELECT 1 FROM runs WHERE task_id=$1 LIMIT 1', [task.id])).rowCount)
    throw failure('VERIFICATION_SOURCE_BINDING_STALE');
  const current = await readSnapshot(db, taskId, false, row.parent.candidateDigest);
  if (current.parent.task.id !== row.parent.taskId || current.parent.proof.runId !== row.parent.runId ||
    current.parent.proof.fence !== row.parent.fence || current.parent.candidate.candidateDigest !== row.parent.candidateDigest ||
    current.parent.artifact.digest !== row.parent.candidateArtifactDigest || current.parent.artifact.id !== row.parent.candidateArtifactId ||
    current.parent.task.execution.workspace !== row.parent.workspace || digest(current.task.criteria) !== row.expectedTask.criteriaDigest ||
    digest(current.dependencies.map(d => d.task_id)) !== row.expectedTask.dependenciesDigest)
    throw failure('VERIFICATION_SOURCE_BINDING_STALE');
  const binding = { version: 1, actionId: row.actionId, requestDigest: row.requestDigest, state: 'applied',
    parentTaskId: row.parent.taskId, parentRunId: row.parent.runId, parentFence: row.parent.fence, parentAttempt: row.parent.attempt,
    candidateArtifactId: row.parent.candidateArtifactId, candidateArtifactDigest: row.parent.candidateArtifactDigest,
    candidateDigest: row.parent.candidateDigest, sourceManifestDigest: row.prepared.sourceManifestDigest,
    sourceWorkspace: row.sourceWorkspace, targetWorkspace: row.targetWorkspace, baseWorkspace: row.baseWorkspace,
    originalWorkspace: row.originalWorkspace, originalSourceWorkspace: row.originalSourceWorkspace,
    rebaseActionId: row.parent.rebaseActionId, rebaseReceiptDigest: row.parent.rebaseReceiptDigest,
    sourceProofDigest: row.prepared.receiptDigest, acceptance: false };
  const latest = { ...row, state: 'applying', binding, applyingAt: new Date().toISOString() };
  await db.query('UPDATE settings SET value=$2 WHERE id=$1', [jobId(taskId), JSON.stringify(latest)]);
  const execution = { ...task.execution, workspace: row.targetWorkspace, sourceWorkspace: row.parent.workspace, verificationSourceBinding: binding };
  await db.query('UPDATE tasks SET execution=$2,updated_at=now() WHERE id=$1', [taskId, JSON.stringify(execution)]);
  await db.query('UPDATE settings SET value=$2 WHERE id=$1', [jobId(taskId), JSON.stringify({ ...latest, state: 'applied', appliedAt: new Date().toISOString() })]);
  await event(db, 'VERIFICATION_SOURCE_BINDING_APPLIED', taskId, { actionId: row.actionId, requestDigest: row.requestDigest,
    parentTaskId: row.parent.taskId, parentRunId: row.parent.runId, candidateDigest: row.parent.candidateDigest,
    sourceWorkspace: row.parent.workspace, targetWorkspace: row.targetWorkspace, acceptance: false });
  return { required: true, applied: true };
}

