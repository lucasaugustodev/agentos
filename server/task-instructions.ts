import { createHash, randomUUID } from 'node:crypto';
import { readFile, writeFile, mkdir } from 'node:fs/promises';
import path from 'node:path';
import type { DB } from './db.js';
import { config } from './config.js';
import { DomainError } from './contracts.js';
import { glmRepairHandoff } from './glm-repair.js';
import { executionInputManifest } from './execution-inputs.js';
import { acceptedSupervisionActions } from './supervision-actions.js';
import { reconcileLegacyBacklogHandoffs } from './backlog-handoffs.js';

const sha = (value: string | Buffer) => createHash('sha256').update(value).digest('hex');
const live = "'queued','running','cancelling','reconciling'";
export const repairDeliveryInstruction = (id: string) =>
  '\nREPARO ACEITO: ' +
  id +
  ' foi ligado como dependência. Receba o patch e o relatório desse produtor, confira a pré-imagem e hashes da candidata e integre somente o delta compatível. Preserve critérios e valide a candidata resultante; a entrega do patch não aprova este jogo.';
export const handoffInstruction =
  '\nSUPERVISOR: Evidências e patches das correções estão em /home/agent/inputs/<artifact-id>/<name>. Leia os relatórios e integre somente alterações permitidas no escopo antes de repetir o teste.';
export function reviseInstruction(action: any, repair?: string) {
  return (
    '\nSUPERVISOR: ' +
    action.diagnosis +
    '\n' +
    action.instruction +
    (repair ? repairDeliveryInstruction(repair) : '')
  );
}
export function delegateInstruction(keys: string[], actions: any[]) {
  const revision = actions.find((a) => a.kind === 'revise');
  return (
    '\nSUPERVISOR: Receba o patch e relatório de ' +
    keys.join(', ') +
    '. Integre estas correções e execute somente validação relevante antes do aceite original.' +
    (actions.some((a) => a.kind === 'delegate' && a.lane === 'glm' && a.workKind === 'repair')
      ? glmRepairHandoff
      : '') +
    (revision ? '\n' + revision.diagnosis + '\n' + revision.instruction : '')
  );
}
const invalid = (code = 'TASK_INSTRUCTIONS_CHANGED') =>
  new DomainError(
    code,
    'Contrato da tentativa não possui proveniência verificável; preservado sem projeção.',
  );

/** Run outside the scheduler transaction: ALTER/trigger table locks must precede
 * admission's advisory lock. No existing run or context is backfilled. */
export async function installTaskInstructions(db: DB) {
  await db.query(`CREATE TABLE IF NOT EXISTS task_instruction_origins(
    id uuid UNIQUE NOT NULL,task_id uuid PRIMARY KEY REFERENCES tasks(id),goal_id uuid NOT NULL REFERENCES goals(id),
    baseline text NOT NULL,baseline_hash text NOT NULL,original text NOT NULL,original_hash text NOT NULL,
    proof jsonb NOT NULL,created_at timestamptz NOT NULL DEFAULT now());
    CREATE TABLE IF NOT EXISTS task_instruction_revisions(
    id uuid PRIMARY KEY,task_id uuid NOT NULL REFERENCES tasks(id),goal_id uuid NOT NULL REFERENCES goals(id),
    origin_id uuid NOT NULL REFERENCES task_instruction_origins(id),number int NOT NULL,predecessor uuid REFERENCES task_instruction_revisions(id),
    operation_key text NOT NULL,instruction text NOT NULL,instruction_hash text NOT NULL,provenance jsonb NOT NULL,
    rendered text NOT NULL,rendered_hash text NOT NULL,created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE(task_id,number),UNIQUE(task_id,operation_key));
    ALTER TABLE tasks ADD COLUMN IF NOT EXISTS instruction_revision_id uuid REFERENCES task_instruction_revisions(id);
    ALTER TABLE runs ADD COLUMN IF NOT EXISTS instruction_snapshot jsonb;
    CREATE OR REPLACE FUNCTION agentos_instruction_snapshot() RETURNS trigger LANGUAGE plpgsql AS $$
    DECLARE t tasks; r task_instruction_revisions; item jsonb; prior jsonb; entries jsonb='[]'; bytes bytea; total bigint=0; ci jsonb; BEGIN
      IF TG_OP='UPDATE' THEN
        IF NEW.instruction_snapshot IS DISTINCT FROM OLD.instruction_snapshot THEN RAISE EXCEPTION 'INSTRUCTION_SNAPSHOT_IMMUTABLE'; END IF; RETURN NEW;
      END IF;
      PERFORM pg_advisory_xact_lock(4280001);
      SELECT * INTO STRICT t FROM tasks WHERE id=NEW.task_id FOR UPDATE;
      IF t.instruction_revision_id IS NOT NULL THEN
        SELECT * INTO STRICT r FROM task_instruction_revisions WHERE id=t.instruction_revision_id AND task_id=t.id AND goal_id=t.goal_id;
        IF r.rendered IS DISTINCT FROM t.description THEN RAISE EXCEPTION 'TASK_INSTRUCTIONS_CHANGED'; END IF;
        PERFORM c.id FROM supervision_cases c WHERE c.id::text=r.provenance->>'caseId'
          AND c.task_id=t.id AND c.goal_id=t.goal_id AND c.decision=r.provenance->'decisionSnapshot'
          AND c.decision_run_id::text=r.provenance->>'decisionRunId'
          AND c.applied_attempt=(r.provenance->>'sourceAttempt')::int
          AND (CASE WHEN r.provenance->>'kind'='delegate' THEN c.state IN('delegated','decided') ELSE c.state='applied' END)
          AND NOT EXISTS(SELECT 1 FROM supervision_cases newer WHERE newer.task_id=t.id AND newer.created_at>c.created_at) FOR SHARE;
        IF NOT FOUND THEN RAISE EXCEPTION 'INSTRUCTION_CASE_CHANGED'; END IF;
      END IF;
      IF t.execution ? 'inputs' AND jsonb_typeof(t.execution->'inputs')<>'array' THEN RAISE EXCEPTION 'SUPERVISION_INPUTS_INVALID'; END IF;
      FOR item IN SELECT value FROM jsonb_array_elements(COALESCE(t.execution->'inputs','[]')) LOOP
        IF COALESCE(item->>'id','') !~ '^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
          OR COALESCE(item->>'name','') !~ '^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,100}$'
          OR COALESCE(item->>'digest','') !~ '^[0-9a-f]{64}$' OR jsonb_typeof(item->'base64') IS DISTINCT FROM 'string'
          THEN RAISE EXCEPTION 'SUPERVISION_INPUTS_INVALID'; END IF;
        bytes:=decode(item->>'base64','base64');
        IF replace(encode(bytes,'base64'),chr(10),'')<>item->>'base64' OR encode(sha256(bytes),'hex')<>item->>'digest'
          THEN RAISE EXCEPTION 'SUPERVISION_INPUT_DIGEST'; END IF;
        IF octet_length(bytes)>8388608 THEN RAISE EXCEPTION 'SUPERVISION_INPUTS_TOO_LARGE'; END IF;
        SELECT value INTO prior FROM jsonb_array_elements(entries) WHERE value->>'id'=item->>'id';
        IF prior IS NOT NULL THEN
          IF prior->>'name'<>item->>'name' OR prior->>'digest'<>item->>'digest' THEN RAISE EXCEPTION 'SUPERVISION_INPUT_COLLISION'; END IF;
        ELSE
          total:=total+octet_length(bytes); IF total>25165824 THEN RAISE EXCEPTION 'SUPERVISION_INPUTS_TOO_LARGE'; END IF;
          entries:=entries||jsonb_build_array(jsonb_build_object('id',item->>'id','name',item->>'name','digest',item->>'digest','size',octet_length(bytes),'source','execution_input'));
        END IF;
      END LOOP;
      NEW.instruction_snapshot:=jsonb_build_object('schema',1,'task',to_jsonb(t),'revisionId',r.id,'executionInputs',entries);
      SELECT value INTO ci FROM settings WHERE id='worker-validation:'||t.goal_id
        AND value#>>'{policy,enabled}'='true' AND (value#>'{policy,stages}') ? t.stage
        AND t.execution->>'kind' IN('harness','opencode');
      IF ci IS NOT NULL AND NOT EXISTS(SELECT 1 FROM events WHERE kind='WORKER_VALIDATION_POLICY_REGISTERED'
        AND entity_id=t.goal_id::text AND payload=ci) THEN RAISE EXCEPTION 'VALIDATION_POLICY_UNREGISTERED'; END IF;
      NEW.instruction_snapshot:=NEW.instruction_snapshot||jsonb_build_object('workerValidationContract',1,'workerValidation',ci);
      RETURN NEW; END $$;
    DROP TRIGGER IF EXISTS agentos_instruction_snapshot ON runs;
    CREATE TRIGGER agentos_instruction_snapshot BEFORE INSERT OR UPDATE OF instruction_snapshot ON runs FOR EACH ROW EXECUTE FUNCTION agentos_instruction_snapshot();
    CREATE OR REPLACE FUNCTION agentos_instruction_immutable() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'INSTRUCTION_HISTORY_IMMUTABLE'; END $$;
    DROP TRIGGER IF EXISTS agentos_instruction_immutable ON task_instruction_origins;
    CREATE TRIGGER agentos_instruction_immutable BEFORE UPDATE OR DELETE ON task_instruction_origins FOR EACH ROW EXECUTE FUNCTION agentos_instruction_immutable();
    DROP TRIGGER IF EXISTS agentos_instruction_immutable ON task_instruction_revisions;
    CREATE TRIGGER agentos_instruction_immutable BEFORE UPDATE OR DELETE ON task_instruction_revisions FOR EACH ROW EXECUTE FUNCTION agentos_instruction_immutable();`);
}

const actionKeys = [
  'caseId',
  'kind',
  'diagnosis',
  'instruction',
  'lane',
  'sourceTaskId',
  'title',
  'workKind',
  'waitingFor',
  'evidenceArtifactIds',
];
const normalizedAction = (a: any) =>
  JSON.stringify(actionKeys.map((key) => [key, a?.[key] ?? null]));
async function decisionProof(db: DB, c: any, depth = 0): Promise<any> {
  const refs = (
    await db.query(
      `SELECT a.* FROM artifacts a JOIN runs r ON r.id=a.run_id JOIN tasks t ON t.id=r.task_id
    WHERE r.id=$1 AND t.goal_id=$2 AND t.stage='coordinate' AND a.name='coordination.json'
    AND EXISTS(SELECT 1 FROM acceptances v WHERE v.run_id=r.id AND v.passed)`,
      [c.decision_run_id, c.goal_id],
    )
  ).rows;
  const actions = c.decision?.actions ?? [c.decision];
  if (!actions.length || actions.some((a: any) => a?.caseId !== c.id)) return null;
  for (const ref of refs) {
    try {
      const bytes = await readFile(path.join(config.dataDir, 'artifacts', ref.id));
      if (sha(bytes) !== ref.digest || bytes.length !== ref.size) continue;
      const raw = JSON.parse(bytes.toString()).supervision?.actions;
      if (
        Array.isArray(raw) &&
        actions.every((a: any) => raw.some((v: any) => normalizedAction(v) === normalizedAction(a)))
      )
        return {
          artifactId: ref.id,
          digest: ref.digest,
          decisionRunId: c.decision_run_id,
          decisionHash: sha(JSON.stringify(c.decision)),
        };
    } catch {}
  }
  if (depth < 1) {
    const routed = (
      await db.query(
        `SELECT parent.*,e.id routed_event_id FROM supervision_cases parent JOIN events e
      ON e.payload->>'caseId'=parent.id::text AND e.entity_id=parent.task_id::text
      WHERE parent.goal_id=$1 AND parent.state='forwarded' AND parent.decision_run_id=$2
      AND parent.decision->>'forwardedToCaseId'=$3 AND parent.decision->>'implementationTaskId'=$4
      AND e.kind='SUPERVISION_IMPLEMENTATION_ROUTED' AND e.payload->>'targetCaseId'=$3 AND e.payload->>'targetTaskId'=$4
      AND e.payload->>'decisionRunId'=$2::text`,
        [c.goal_id, c.decision_run_id, c.id, c.task_id],
      )
    ).rows;
    for (const parent of routed) {
      const expected = (parent.decision.actions ?? [parent.decision])
        .filter((a: any) => a.kind === 'delegate')
        .map((a: any) => ({ ...a, caseId: c.id }));
      if (
        expected.length !== actions.length ||
        !expected.every((a: any) =>
          actions.some((v: any) => normalizedAction(a) === normalizedAction(v)),
        )
      )
        continue;
      const verified = await decisionProof(db, parent, depth + 1);
      if (verified)
        return {
          ...verified,
          forwardedFromCaseId: parent.id,
          forwardingEventId: String(parent.routed_event_id),
          decisionHash: sha(JSON.stringify(c.decision)),
        };
    }
  }
  return null;
}

async function historicalDecision(db: DB, op: any, operations: any[]) {
  const recorded =
    op.kind === 'SUPERVISION_HANDOFF_READY'
      ? operations
          .filter(
            (e) =>
              e.case_id === op.case_id &&
              e.kind === 'SUPERVISION_DELEGATED' &&
              BigInt(e.id) < BigInt(op.id),
          )
          .at(-1)
      : op;
  if (!recorded?.payload.decisionRunId) return null;
  const requested = recorded.payload.sourceRunId
    ? null
    : (
        await db.query(
          "SELECT payload->>'sourceRunId' source FROM events WHERE kind='SUPERVISION_REQUESTED' AND entity_id=$1 AND payload->>'caseId'=$2 AND id<$3 ORDER BY id DESC LIMIT 1",
          [op.task_id, op.case_id, recorded.id],
        )
      ).rows[0];
  const source = (
    await db.query('SELECT 1 FROM runs WHERE id=$1 AND task_id=$2', [
      recorded.payload.sourceRunId ?? requested?.source,
      op.task_id,
    ])
  ).rowCount;
  if (!source) return null;
  const refs = (
    await db.query(
      `SELECT a.* FROM artifacts a JOIN runs r ON r.id=a.run_id JOIN tasks t ON t.id=r.task_id
    WHERE r.id=$1 AND t.goal_id=$2 AND t.stage='coordinate' AND a.name='coordination.json'
    AND EXISTS(SELECT 1 FROM acceptances v WHERE v.run_id=r.id AND v.passed)`,
      [recorded.payload.decisionRunId, op.goal_id],
    )
  ).rows;
  for (const ref of refs) {
    try {
      const bytes = await readFile(path.join(config.dataDir, 'artifacts', ref.id));
      if (sha(bytes) !== ref.digest || bytes.length !== ref.size) continue;
      const proposed = JSON.parse(bytes.toString()).supervision?.actions;
      if (!Array.isArray(proposed)) continue;
      const raw = acceptedSupervisionActions(proposed);
      let actions = raw.filter((a: any) => a.caseId === op.case_id),
        routing: any = null;
      if (!actions.length) {
        routing = (
          await db.query(
            `SELECT e.* FROM events e JOIN supervision_cases parent ON parent.id::text=e.payload->>'caseId'
          WHERE e.kind='SUPERVISION_IMPLEMENTATION_ROUTED' AND parent.goal_id=$1
          AND e.entity_id=parent.task_id::text AND e.payload->>'targetCaseId'=$2 AND e.payload->>'targetTaskId'=$3
          AND e.payload->>'decisionRunId'=$4 AND e.id<$5 ORDER BY e.id DESC LIMIT 1`,
            [op.goal_id, op.case_id, op.task_id, recorded.payload.decisionRunId, op.id],
          )
        ).rows[0];
        if (routing)
          actions = raw
            .filter((a: any) => a.caseId === routing.payload.caseId && a.kind === 'delegate')
            .map((a: any) => ({ ...a, caseId: op.case_id }));
      }
      if (
        !actions.length ||
        actions.some((a: any) => !['revise', 'delegate', 'wait_for_evidence'].includes(a.kind))
      )
        continue;
      return {
        actions,
        proof: {
          artifactId: ref.id,
          digest: ref.digest,
          decisionRunId: recorded.payload.decisionRunId,
          decisionHash: sha(JSON.stringify(actions)),
          ...(routing
            ? { forwardingEventId: String(routing.id), forwardedFromCaseId: routing.payload.caseId }
            : {}),
        },
      };
    } catch {}
  }
  return null;
}

/** Remove only a continuous, exactly verified suffix, never a marker split.
 * The first unknown operation and every earlier byte remain in the prefix. */
export async function projectLegacyInstructions(db: DB, task: any, excludeDelegation?: string) {
  const operations = (
    await db.query(
      `SELECT e.*,c.id case_id,c.task_id,c.goal_id,c.decision,c.decision_run_id,c.support_task_id,c.support_task_ids
    FROM events e JOIN supervision_cases c ON e.payload->>'caseId'=c.id::text
    WHERE e.entity_id=$1 AND c.task_id=$1::uuid AND c.goal_id=$2 AND e.kind IN('SUPERVISION_APPLIED','SUPERVISION_DELEGATED','SUPERVISION_HANDOFF_READY')
    ORDER BY e.id`,
      [task.id, task.goal_id],
    )
  ).rows;
  const pieces: any[] = [];
  let failure: string | null = null;
  let checkedEventId: string | null = null;
  let original = task.description;
  for (const op of [...operations].reverse()) {
    checkedEventId = String(op.id);
    if (
      op.kind === 'SUPERVISION_DELEGATED' &&
      op.case_id === excludeDelegation &&
      op.payload.decisionRunId === op.decision_run_id
    )
      continue;
    const c = { ...op, id: op.case_id };
    const historical = await historicalDecision(db, op, operations);
    if (!historical) {
      failure = 'decision_or_event_unverified';
      break;
    }
    const { actions, proof } = historical;
    let text = '';
    if (op.kind === 'SUPERVISION_APPLIED') {
      const a = actions.find((a: any) => a.kind === 'revise');
      if (!a || actions.filter((a: any) => a.kind === 'revise').length !== 1) {
        failure = 'appender_unverified';
        break;
      }
      if (
        a.sourceTaskId &&
        !(
          await db.query(
            "SELECT 1 FROM events WHERE entity_id=$1 AND kind='SUPERVISION_REPAIR_ATTACHED' AND payload->>'caseId'=$2 AND payload->>'repairTaskId'=$3 AND payload->>'sourceRunId'=$4",
            [task.id, c.id, a.sourceTaskId, op.payload.sourceRunId],
          )
        ).rowCount
      ) {
        failure = 'repair_attachment_unverified';
        break;
      }
      text = reviseInstruction(a, a.sourceTaskId);
    } else if (op.kind === 'SUPERVISION_DELEGATED') {
      // One consumer append follows a whole delegation, whose members are events.
      if (
        pieces.some(
          (p) =>
            p.kind === 'delegate' && p.caseId === c.id && p.decisionRunId === proof.decisionRunId,
        )
      )
        continue;
      const members = operations.filter(
        (e) =>
          e.kind === 'SUPERVISION_DELEGATED' &&
          e.case_id === c.id &&
          e.payload.decisionRunId === proof.decisionRunId,
      );
      if (
        members.length !== actions.filter((a: any) => a.kind === 'delegate').length ||
        members.some((e) => !e.payload.taskId || e.payload.decisionRunId !== proof.decisionRunId)
      ) {
        failure = 'delegation_members_unverified';
        break;
      }
      const keys = [];
      for (const member of members) {
        const child = (
          await db.query('SELECT key FROM tasks WHERE id=$1 AND goal_id=$2', [
            member.payload.taskId,
            task.goal_id,
          ])
        ).rows[0];
        if (!child) {
          failure = 'delegation_scope_unverified';
          break;
        }
        keys.push(child.key);
      }
      if (failure) break;
      text = delegateInstruction(keys, actions);
    } else {
      const previous = (
        await db.query(
          'SELECT runtime FROM runs WHERE task_id=$1 AND created_at<=$2 ORDER BY attempt DESC LIMIT 1',
          [task.id, op.created_at],
        )
      ).rows[0];
      if (previous?.runtime === 'opencode' && op.payload.artifacts?.length)
        text = handoffInstruction;
      else if (!previous || !Array.isArray(op.payload.artifacts)) {
        failure = 'handoff_unverified';
        break;
      }
    }
    if (text) {
      if (!original.endsWith(text)) {
        failure = 'append_replay_mismatch';
        break;
      }
      original = original.slice(0, -text.length);
      pieces.push({
        kind: op.kind === 'SUPERVISION_DELEGATED' ? 'delegate' : op.kind,
        caseId: c.id,
        eventId: String(op.id),
        text,
        characters: text.length,
        hash: sha(text),
        ...proof,
      });
    }
  }
  return {
    original,
    proof: {
      method: 'verified_append_replay_v1',
      verified: !failure,
      reason: failure,
      failureEventId: failure ? checkedEventId : null,
      projection: pieces.length ? 'verified_suffix' : 'preserved_integral',
      partial: !!failure && pieces.length > 0,
      retainedUnknownReason: failure,
      operationCount: operations.length,
      removedCount: pieces.length,
      pieces: [...pieces].reverse().map(({ text, ...p }) => p),
    },
  };
}

export async function setSupervisionInstruction(
  db: DB,
  taskId: string,
  caseId: string,
  kind: 'revise' | 'delegate' | 'delivery',
  instruction: string,
  sourceRunId: string,
  projectExisting = false,
) {
  await db.query('SELECT pg_advisory_xact_lock(4280001)');
  let task = (await db.query('SELECT * FROM tasks WHERE id=$1 FOR UPDATE', [taskId])).rows[0];
  if (
    !task ||
    (await db.query(`SELECT 1 FROM runs WHERE task_id=$1 AND state IN(${live})`, [taskId])).rowCount
  )
    throw invalid('TASK_INSTRUCTIONS_LIVE');
  const c = (
    await db.query(
      'SELECT * FROM supervision_cases WHERE id=$1 AND task_id=$2 AND goal_id=$3 FOR UPDATE',
      [caseId, taskId, task.goal_id],
    )
  ).rows[0];
  if (
    !c ||
    !(kind === 'delegate' ? ['delegated', 'decided'] : ['applied']).includes(c.state) ||
    (
      await db.query(
        'SELECT 1 FROM supervision_cases WHERE task_id=$1 AND created_at>(SELECT created_at FROM supervision_cases WHERE id=$2)',
        [taskId, c.id],
      )
    ).rowCount
  )
    throw invalid('INSTRUCTION_CASE_INVALID');
  const source = projectExisting
    ? (
        await db.query('SELECT id,attempt FROM runs WHERE task_id=$1 AND id=$2', [
          taskId,
          sourceRunId,
        ])
      ).rows[0]
    : (
        await db.query(
          'SELECT id,attempt FROM runs WHERE task_id=$1 ORDER BY attempt DESC LIMIT 1',
          [taskId],
        )
      ).rows[0];
  const bound =
    projectExisting &&
    [task.execution?.supervisionHandoff, task.execution?.supervision].some(
      (v) => v?.caseId === c.id && v?.sourceRunId === sourceRunId,
    );
  if (
    !source ||
    source.id !== sourceRunId ||
    c.applied_attempt !== source.attempt ||
    (projectExisting && !bound)
  )
    throw invalid('INSTRUCTION_SOURCE_INVALID');
  const decision = await decisionProof(db, c);
  if (!decision) throw invalid('INSTRUCTION_DECISION_UNVERIFIED');
  if(await reconcileLegacyBacklogHandoffs(db,task))
    task=(await db.query('SELECT * FROM tasks WHERE id=$1 FOR UPDATE',[taskId])).rows[0];
  const operationKey = caseId + ':' + c.decision_run_id + ':' + kind;
  const existing = (
    await db.query(
      'SELECT * FROM task_instruction_revisions WHERE task_id=$1 AND operation_key=$2',
      [taskId, operationKey],
    )
  ).rows[0];
  if (existing) {
    if (existing.instruction !== instruction || task.instruction_revision_id !== existing.id)
      throw invalid();
    return existing;
  }
  const prior = task.instruction_revision_id
    ? (
        await db.query('SELECT * FROM task_instruction_revisions WHERE id=$1 AND task_id=$2', [
          task.instruction_revision_id,
          taskId,
        ])
      ).rows[0]
    : null;
  if (prior && prior.rendered !== task.description) throw invalid();
  let origin = (await db.query('SELECT * FROM task_instruction_origins WHERE task_id=$1', [taskId]))
    .rows[0];
  if (!origin) {
    const projected = await projectLegacyInstructions(
      db,
      task,
      kind === 'delegate' && !projectExisting ? caseId : undefined,
    );
    origin = (
      await db.query(
        'INSERT INTO task_instruction_origins(id,task_id,goal_id,baseline,baseline_hash,original,original_hash,proof) VALUES($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *',
        [
          randomUUID(),
          taskId,
          task.goal_id,
          task.description,
          sha(task.description),
          projected.original,
          sha(projected.original),
          JSON.stringify(projected.proof),
        ],
      )
    ).rows[0];
  }
  const id = randomUUID(),
    number = (prior?.number ?? 0) + 1;
  const rendered =
    origin.original +
    '\n\nDIRETRIZ ATUAL DA SUPERVISÃO (substitui diretrizes anteriores; não altera o contrato original):\n' +
    instruction;
  const provenance = {
    caseId,
    kind,
    sourceRunId,
    sourceAttempt: source.attempt,
    decisionSnapshot: c.decision,
    ...decision,
  };
  const revision = (
    await db.query(
      'INSERT INTO task_instruction_revisions(id,task_id,goal_id,origin_id,number,predecessor,operation_key,instruction,instruction_hash,provenance,rendered,rendered_hash) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) RETURNING *',
      [
        id,
        taskId,
        task.goal_id,
        origin.id,
        number,
        prior?.id ?? null,
        operationKey,
        instruction,
        sha(instruction),
        JSON.stringify(provenance),
        rendered,
        sha(rendered),
      ],
    )
  ).rows[0];
  await db.query('UPDATE tasks SET description=$2,instruction_revision_id=$3 WHERE id=$1', [
    taskId,
    rendered,
    id,
  ]);
  await db.query(
    "INSERT INTO events(kind,entity_id,payload) VALUES('TASK_INSTRUCTIONS_VERSIONED',$1,$2)",
    [
      taskId,
      JSON.stringify({
        revisionId: id,
        number,
        originId: origin.id,
        baselineHash: origin.baseline_hash,
        originalHash: origin.original_hash,
        renderedHash: revision.rendered_hash,
        provenance,
        legacy: origin.proof,
      }),
    ],
  );
  return revision;
}

/** Only structured applied handoff metadata authorizes projecting an old task.
 * Run admission and this preparation share the scheduler transaction/fence. */
export async function prepareTaskInstructions(db: DB, taskId: string) {
  let task = (await db.query('SELECT * FROM tasks WHERE id=$1 FOR UPDATE', [taskId])).rows[0];
  if(await reconcileLegacyBacklogHandoffs(db,task))
    task=(await db.query('SELECT * FROM tasks WHERE id=$1 FOR UPDATE',[taskId])).rows[0];
  executionInputManifest(task.execution?.inputs ?? []);
  if (task.instruction_revision_id) return;
  const metadata = [task.execution?.supervisionHandoff, task.execution?.supervision].filter(
    (v) =>
      typeof v?.caseId === 'string' &&
      /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i.test(v.caseId) &&
      typeof v.sourceRunId === 'string',
  );
  if (!metadata.length) return;
  const c = (
    await db.query(
      "SELECT * FROM supervision_cases c WHERE id=ANY($1::uuid[]) AND task_id=$2 AND goal_id=$3 AND state='applied' AND NOT EXISTS(SELECT 1 FROM supervision_cases newer WHERE newer.task_id=c.task_id AND newer.created_at>c.created_at) ORDER BY created_at DESC LIMIT 1",
      [metadata.map((v) => v.caseId), task.id, task.goal_id],
    )
  ).rows[0];
  if (!c) return;
  const handoff = metadata.find((v) => v.caseId === c.id)!;
  const actions = c.decision?.actions ?? [c.decision];
  const delegated = actions.some((a: any) => a.kind === 'delegate');
  let instruction: string;
  if (delegated) {
    const ids = c.support_task_ids?.length ? c.support_task_ids : [c.support_task_id];
    const children = (
      await db.query('SELECT id,key FROM tasks WHERE id=ANY($1::uuid[]) AND goal_id=$2', [
        ids,
        task.goal_id,
      ])
    ).rows;
    if (children.length !== ids.length) throw invalid('INSTRUCTION_CASE_INVALID');
    instruction =
      delegateInstruction(
        ids.map((id: string) => children.find((t: any) => t.id === id).key),
        actions,
      ) +
      (task.execution?.inputs?.length
        ? handoffInstruction
        : '\nSUPERVISOR: As entregas das correções estão disponíveis como entradas. Preserve o contrato original e integre somente o delta autorizado.');
  } else {
    const action = actions.find((a: any) => a.kind === 'revise');
    if (!action) return;
    instruction = reviseInstruction(action, action.sourceTaskId);
  }
  return setSupervisionInstruction(
    db,
    task.id,
    c.id,
    delegated ? 'delivery' : 'revise',
    instruction,
    handoff.sourceRunId,
    true,
  );
}

export function instructionTask(run: any, fallback: any, manifest?: any) {
  const snapshot = run.instruction_snapshot;
  if (!snapshot) {
    // Synthetic adapter inputs are permitted; persisted pre-feature runs cannot
    // be reconstructed from a mutable task after publication.
    if (run.created_at) throw invalid('INSTRUCTION_SNAPSHOT_MISSING');
    return fallback;
  }
  if (
    snapshot.schema !== 1 ||
    snapshot.task?.id !== run.task_id ||
    snapshot.task?.goal_id !== fallback.goal_id
  )
    throw invalid('INSTRUCTION_SNAPSHOT_INVALID');
  if (
    manifest?.taskInstructions &&
    (manifest.taskInstructions.taskId !== snapshot.task.id ||
      manifest.taskInstructions.goalId !== snapshot.task.goal_id ||
      manifest.taskInstructions.revisionId !== snapshot.revisionId ||
      manifest.taskInstructions.descriptionHash !== sha(snapshot.task.description))
  )
    throw invalid('INSTRUCTION_MANIFEST_INVALID');
  return { ...fallback, ...snapshot.task };
}

export async function instructionEvidence(db: DB, run: any, task: any) {
  const snapshot = run.instruction_snapshot;
  if (!snapshot) throw invalid('INSTRUCTION_SNAPSHOT_MISSING');
  let archive: any = {
    schema: 1,
    taskId: task.id,
    goalId: task.goal_id,
    baseline: task.description,
    baselineHash: sha(task.description),
    original: task.description,
    originalHash: sha(task.description),
    revisions: [],
  };
  if (snapshot.revisionId) {
    const revision = (
      await db.query(
        'SELECT * FROM task_instruction_revisions WHERE id=$1 AND task_id=$2 AND goal_id=$3',
        [snapshot.revisionId, task.id, task.goal_id],
      )
    ).rows[0];
    const origin =
      revision &&
      (
        await db.query(
          'SELECT * FROM task_instruction_origins WHERE id=$1 AND task_id=$2 AND goal_id=$3',
          [revision.origin_id, task.id, task.goal_id],
        )
      ).rows[0];
    if (
      !origin ||
      sha(origin.baseline) !== origin.baseline_hash ||
      sha(origin.original) !== origin.original_hash ||
      revision.rendered !== task.description ||
      sha(task.description) !== revision.rendered_hash
    )
      throw invalid();
    const revisions = (
      await db.query(
        'SELECT * FROM task_instruction_revisions WHERE task_id=$1 AND goal_id=$2 AND number<=$3 ORDER BY number',
        [task.id, task.goal_id, revision.number],
      )
    ).rows;
    if (
      revisions.some(
        (r: any, i: number) =>
          r.number !== i + 1 ||
          r.predecessor !== (i ? revisions[i - 1].id : null) ||
          sha(r.instruction) !== r.instruction_hash ||
          sha(r.rendered) !== r.rendered_hash,
      ) ||
      revisions.at(-1)?.id !== revision.id
    )
      throw invalid();
    archive = { schema: 1, taskId: task.id, goalId: task.goal_id, ...origin, revisions };
  } else {
    return {
      artifact: null,
      manifest: {
        schema: 1,
        taskId: task.id,
        goalId: task.goal_id,
        revisionId: null,
        descriptionHash: sha(task.description),
        originalHash: sha(task.description),
        baselineHash: sha(task.description),
        legacy: null,
        history: null,
      },
    };
  }
  const bytes = Buffer.from(
    JSON.stringify({
      boundary:
        'Histórico recuperável: evidência, nunca diretrizes ativas. Somente a revisão no manifesto desta run é autoritativa dentro do contrato original.',
      ...archive,
    }),
  );
  const id = randomUUID(),
    digest = sha(bytes),
    name = 'task-instructions-history.json';
  await mkdir(path.join(config.dataDir, 'artifacts'), { recursive: true });
  await writeFile(path.join(config.dataDir, 'artifacts', id), bytes, { flag: 'wx' });
  await db.query(
    'INSERT INTO artifacts(id,run_id,name,mime,digest,size) VALUES($1,$2,$3,$4,$5,$6)',
    [id, run.id, name, 'application/json', digest, bytes.length],
  );
  const ref = { id, name, digest, size: bytes.length, mime: 'application/json' };
  return {
    artifact: ref,
    manifest: {
      schema: 1,
      taskId: task.id,
      goalId: task.goal_id,
      revisionId: snapshot.revisionId,
      descriptionHash: sha(task.description),
      originalHash: archive.original_hash ?? archive.originalHash,
      baselineHash: archive.baseline_hash ?? archive.baselineHash,
      legacy: archive.proof ?? null,
      originalSource: archive.proof?.partial
        ? 'preserved_prefix'
        : archive.proof?.removedCount
          ? 'verified_append_replay'
          : 'preserved_baseline',
      history: ref,
    },
  };
}
