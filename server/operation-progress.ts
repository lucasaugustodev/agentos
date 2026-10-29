import { createHash } from 'node:crypto';
import type { DB } from './db.js';
import { checkedExecutionMetadata, executionMetadataColumns } from './task-execution-metadata.js';

const sha = (value: string | Buffer) => createHash('sha256').update(value).digest('hex');
const digestPattern = /^[a-f0-9]{64}$/;
const liveStates = ['queued', 'running', 'cancelling', 'reconciling'];
const closedStates = ['accepted', 'cancelled', 'superseded'];
export const operationProgressThreshold = 3;
type ArtifactReader = (id: string) => Promise<{ bytes: Buffer }>;
type MilestoneState = 'not_planned' | 'pending' | 'running' | 'accepted';
export type DeliveryMilestone = {
  state: MilestoneState;
  selectionIssue?: 'ambiguous_dag' | 'invalid_dag';
  taskId?: string;
  runId?: string;
  candidateDigest: string | null;
  artifactDigests: string[];
};
export type OperationProgressObservation = {
  fingerprint: string;
  inputFingerprint: string;
  milestones: Array<{
    scope: string;
    integration: DeliveryMilestone;
    verification: DeliveryMilestone;
    verificationMatchesCandidate: boolean;
    diagnostics: { accepted: number; total: number };
  }>;
  delivery: { integrated: number; verified: number; total: number };
  liveRuns: number;
  lastDeliveryAt: string | null;
  /** Stored for novelty comparison; omitted from the manager's compact context. */
  inputDigests: string[];
  deliveryReceipts: string[];
};
export type OperationProgress = OperationProgressObservation & {
  version: 1;
  lastReviewRunId: string | null;
  reviewedAt: string | null;
  observedAt?: string;
  stagnation: {
    unchangedReviews: number;
    threshold: number;
    detected: boolean;
    lastAdvanceAt: string | null;
  };
  nextTransition: {
    kind: 'integrate' | 'verify' | 'replan' | 'wait_live' | 'await_input' | 'complete';
    scope?: string;
    taskId?: string;
    reason: string;
  };
};

/** Match workspace-manager.py's source_hash; never read a digest from a worker report. */
export function runtimeCandidateDigest(bytes: Buffer, artifactDigest: string): string | null {
  try {
    if (!digestPattern.test(artifactDigest) || sha(bytes) !== artifactDigest) return null;
    const candidate = JSON.parse(bytes.toString('utf8'));
    if (
      !digestPattern.test(candidate.baseDigest ?? '') ||
      !digestPattern.test(candidate.candidateDigest ?? '') ||
      !Array.isArray(candidate.files)
    )
      return null;
    const files = candidate.files as Array<{ path: string; sha256: string }>;
    const paths = new Set<string>();
    for (const file of files) {
      if (
        !file ||
        typeof file.path !== 'string' ||
        !file.path ||
        file.path.includes('\\') ||
        file.path.startsWith('/') ||
        file.path.split('/').some((p) => !p || p === '.' || p === '..') ||
        !digestPattern.test(file.sha256 ?? '') ||
        paths.has(file.path)
      )
        return null;
      paths.add(file.path);
    }
    const entries = files
      .map((f) => [f.path, f.sha256])
      .sort((a, b) => Buffer.compare(Buffer.from(a[0]), Buffer.from(b[0])));
    // Python json.dumps defaults to ensure_ascii, including surrogate pairs.
    const encoded = JSON.stringify(entries).replace(
      /[\u0080-\uffff]/g,
      (c) => '\\u' + c.charCodeAt(0).toString(16).padStart(4, '0'),
    );
    return sha(encoded) === candidate.candidateDigest ? candidate.candidateDigest : null;
  } catch {
    return null;
  }
}

const canonicalDigests = (values: unknown[]) =>
  [
    ...new Set(values.filter((v): v is string => typeof v === 'string' && digestPattern.test(v))),
  ].sort();
const scopeOf = (task: any) =>
  task.execution?.baseWorkspace ??
  task.execution?.workspace ??
  (task.covers?.length ? 'requirements:' + [...task.covers].sort().join(',') : 'objective');
const currentAcceptance = (task: any) =>
  task.state === 'accepted' &&
  !task.has_live_run &&
  task.last_run_state === 'submitted' &&
  task.last_run_attempt === task.attempt_count &&
  task.acceptance_passed === true;

/** Select a delivery boundary, never a lucky UUID among tasks created in the
 * same planning transaction. Legacy unversioned graphs retain temporal
 * replacement only between disconnected generations, not within a DAG. */
function deliveryBoundary(tasks: any[], scope: string) {
  const eligible=tasks.filter(task=>scopeOf(task)===scope && !['cancelled','superseded'].includes(task.state) &&
    ['work','integrate','verify'].includes(task.stage) && !['diagnostic','repair','planning'].includes(task.execution?.workKind));
  const revisions=eligible.map(task=>task.plan_revision).filter(Number.isInteger);
  if(revisions.length)return eligible.filter(task=>task.plan_revision===Math.max(...revisions));
  const byId=new Map(eligible.map(task=>[task.id,task]));
  const adjacent=new Map(eligible.map(task=>[task.id,new Set<string>()]));
  for(const task of eligible)for(const parent of task.dependency_ids??[]){
    if(!byId.has(parent))continue;
    adjacent.get(task.id)!.add(parent);adjacent.get(parent)!.add(task.id);
  }
  const visited=new Set<string>(),components:any[][]=[];
  for(const task of eligible){
    if(visited.has(task.id))continue;
    const component:any[]=[],pending=[task.id];
    while(pending.length){
      const id=pending.pop()!;
      if(visited.has(id))continue;
      visited.add(id);component.push(byId.get(id));pending.push(...adjacent.get(id)!);
    }
    if(component.some(node=>['integrate','verify'].includes(node.stage)))components.push(component);
  }
  const created=(component:any[])=>Math.max(...component.filter(node=>['integrate','verify'].includes(node.stage))
    .map(node=>Date.parse(node.created_at??'')||0));
  const latest=Math.max(...components.map(created));
  return components.filter(component=>created(component)===latest).flat();
}

function deliveryGates(tasks:any[],selected:any[]) {
  const byId=new Map(tasks.map(task=>[task.id,task]));
  const cache=new Map<string,Set<string>>(),visiting=new Set<string>();
  const ancestors=(id:string):Set<string>=>{
    if(cache.has(id))return cache.get(id)!;
    if(visiting.has(id)||!byId.has(id))throw Error('INVALID_PROGRESS_DAG');
    visiting.add(id);const found=new Set<string>();
    for(const parent of byId.get(id).dependency_ids??[]){
      found.add(parent);for(const ancestor of ancestors(parent))found.add(ancestor);
    }
    visiting.delete(id);cache.set(id,found);return found;
  };
  const integrations=selected.filter(task=>task.stage==='integrate');
  const verifications=selected.filter(task=>task.stage==='verify');
  const choose=(candidates:any[],required:any[])=>{
    const finals=candidates.filter(task=>required.every(other=>other.id===task.id||ancestors(task.id).has(other.id)));
    return finals.length===1?{task:finals[0]}:candidates.length?{selectionIssue:'ambiguous_dag' as const}:{};
  };
  try{
    for(const task of selected)ancestors(task.id);
    return {integration:choose(integrations,integrations),verification:choose(verifications,selected)};
  }catch{
    return {integration:{selectionIssue:'invalid_dag' as const},verification:{selectionIssue:'invalid_dag' as const}};
  }
}

/** Attempts, fresh case IDs and diagnostic acceptance are deliberately absent from delivery identity. */
export function observeOperationProgress(
  tasks: any[],
  manifests: Record<string, string | null>,
  artifacts: Array<{ run_id: string; digest: string }> = [],
  inputDigests: string[] = [],
): OperationProgressObservation {
  const deliveryTasks = tasks.filter(
    (t) =>
      ['integrate', 'verify'].includes(t.stage) && !['cancelled', 'superseded'].includes(t.state),
  );
  const scopes = [...new Set(deliveryTasks.map(scopeOf))].sort();
  const milestone = (selection:{task?:any;selectionIssue?:'ambiguous_dag'|'invalid_dag'}): DeliveryMilestone => {
    const {task,selectionIssue}=selection;
    if(selectionIssue)return {state:'pending',selectionIssue,candidateDigest:null,artifactDigests:[]};
    if (!task) return { state: 'not_planned', candidateDigest: null, artifactDigests: [] };
    const accepted = currentAcceptance(task);
    return {
      state: accepted ? 'accepted' : task.has_live_run ? 'running' : 'pending',
      taskId: task.id,
      runId: task.last_run_id ?? undefined,
      candidateDigest: manifests[task.id] ?? null,
      artifactDigests: canonicalDigests(
        artifacts.filter((a) => a.run_id === task.last_run_id).map((a) => a.digest),
      ),
    };
  };
  const milestones = scopes.map((scope) => {
    const scoped = tasks.filter((t) => scopeOf(t) === scope);
    const gates=deliveryGates(tasks,deliveryBoundary(tasks,scope));
    const integration=milestone(gates.integration);
    const verification=milestone(gates.verification);
    const diagnostics = scoped.filter(
      (t) =>
        t.execution?.workKind === 'diagnostic' && !['cancelled', 'superseded'].includes(t.state),
    );
    return {
      scope,
      integration,
      verification,
      verificationMatchesCandidate:
        integration.state === 'accepted' &&
        verification.state === 'accepted' &&
        !!integration.candidateDigest &&
        integration.candidateDigest === verification.candidateDigest,
      diagnostics: {
        accepted: diagnostics.filter(currentAcceptance).length,
        total: diagnostics.length,
      },
    };
  });
  const delivered = milestones.flatMap((m) => [
    ...(m.integration.state === 'accepted' && m.integration.candidateDigest
      ? [[m.scope, 'integrate', m.integration.candidateDigest]]
      : []),
    ...(m.verificationMatchesCandidate
      ? [[m.scope, 'verify', m.verification.candidateDigest]]
      : []),
  ]);
  const acceptedGateIds=new Set(milestones.flatMap(m=>[
    ...(m.integration.state==='accepted'&&m.integration.candidateDigest?[m.integration.taskId]:[]),
    ...(m.verificationMatchesCandidate?[m.verification.taskId]:[]),
  ]));
  const acceptedDelivery = deliveryTasks.filter((t) => acceptedGateIds.has(t.id)&&currentAcceptance(t)&&manifests[t.id]);
  const dates = acceptedDelivery
    .map((t) => t.accepted_at)
    .filter(Boolean)
    .map((d) => new Date(d).toISOString())
    .sort();
  return {
    fingerprint: sha(JSON.stringify(delivered)),
    inputFingerprint: sha(JSON.stringify(canonicalDigests(inputDigests))),
    inputDigests: canonicalDigests(inputDigests),
    deliveryReceipts: delivered.map((value) => sha(JSON.stringify(value))).sort(),
    milestones,
    delivery: {
      integrated: milestones.filter(
        (m) => m.integration.state === 'accepted' && m.integration.candidateDigest,
      ).length,
      verified: milestones.filter((m) => m.verificationMatchesCandidate).length,
      total: scopes.length,
    },
    liveRuns: tasks.filter((t) => t.has_live_run || liveStates.includes(t.state)).length,
    lastDeliveryAt: dates.at(-1) ?? null,
  };
}

/** A read refreshes visibility; only recording a new coordinator review increments the counter. */
export function projectOperationProgress(
  observation: OperationProgressObservation,
  previous: OperationProgress | null,
  reviewRunId: string | null = null,
  now = new Date().toISOString(),
): OperationProgress {
  const newInput = observation.inputDigests.some((d) => !previous?.inputDigests?.includes(d));
  const newDelivery = observation.deliveryReceipts.some(
    (d) => !previous?.deliveryReceipts?.includes(d),
  );
  // Losing an accepted gate or recreating it on the same candidate is not an advance.
  const changed = !previous || newDelivery || newInput;
  const newReview = !!reviewRunId && reviewRunId !== previous?.lastReviewRunId;
  const unchangedReviews = changed
    ? 0
    : (previous?.stagnation.unchangedReviews ?? 0) + Number(newReview);
  const remaining = observation.milestones.find((m) => !m.verificationMatchesCandidate);
  const detected =
    (!!remaining || !observation.milestones.length) &&
    unchangedReviews >= operationProgressThreshold;
  let nextTransition: OperationProgress['nextTransition'];
  const running = observation.milestones.find(
    (m) => m.integration.state === 'running' || m.verification.state === 'running',
  );
  const selectionIssue=observation.milestones.find(m=>m.integration.selectionIssue||m.verification.selectionIssue);
  if (running)
    nextTransition = {
      kind: 'wait_live',
      scope: running.scope,
      taskId:
        running.integration.state === 'running'
          ? running.integration.taskId
          : running.verification.taskId,
      reason: detected
        ? 'Há execução viva; replaneje apenas trabalho independente com causa, hipótese e prova esperada.'
        : 'Aguarde a execução viva; atividade não comprova entrega.',
    };
  else if(selectionIssue)
    nextTransition={kind:observation.liveRuns?'wait_live':'replan',scope:selectionIssue.scope,
      reason:'O DAG não identifica uma entrega final única ou contém dependências inválidas. Preserve execuções vivas e corrija o vínculo dos gates; aceites intermediários não concluem a entrega.'};
  else if (detected)
    nextTransition = {
      kind: 'replan',
      scope: remaining?.scope,
      taskId:
        remaining?.integration.state === 'accepted'
          ? remaining.verification.taskId
          : remaining?.integration.taskId,
      reason:
        'Três revisões sem entrega ou insumo novo: defina causa, hipótese e próxima transição verificável.',
    };
  else if (remaining) {
    const integrated =
      remaining.integration.state === 'accepted' && !!remaining.integration.candidateDigest;
    nextTransition = {
      kind: integrated ? 'verify' : 'integrate',
      scope: remaining.scope,
      taskId: integrated ? remaining.verification.taskId : remaining.integration.taskId,
      reason: integrated
        ? 'Valide a mesma candidata integrada e preserve os resultados de todos os gates.'
        : 'Integre as correções aceitas e entregue manifesto real da candidata e prova de integração.',
    };
  } else
    nextTransition = observation.milestones.length
      ? {
          kind: 'complete',
          reason:
            'Integração e validação aceitas na mesma candidata; o encerramento segue o contrato do objetivo.',
        }
      : {
          kind: 'await_input',
          reason:
            'Não há marcos de integração/validação planejados; defina a entrega verificável do objetivo.',
        };
  return {
    ...observation,
    inputDigests: canonicalDigests([
      ...(previous?.inputDigests ?? []),
      ...observation.inputDigests,
    ]),
    deliveryReceipts: canonicalDigests([
      ...(previous?.deliveryReceipts ?? []),
      ...observation.deliveryReceipts,
    ]),
    version: 1,
    lastReviewRunId: newReview ? reviewRunId : (previous?.lastReviewRunId ?? null),
    reviewedAt: newReview ? now : (previous?.reviewedAt ?? null),
    stagnation: {
      unchangedReviews,
      threshold: operationProgressThreshold,
      detected,
      lastAdvanceAt: newDelivery
        ? observation.lastDeliveryAt
        : (previous?.stagnation.lastAdvanceAt ?? observation.lastDeliveryAt),
    },
    nextTransition,
  };
}

export async function collectOperationProgress(db: DB, goalId: string, readArtifact: ArtifactReader) {
  const tasks = checkedExecutionMetadata((
    await db.query(
      `SELECT t.id,t.stage,t.state,t.attempt_count,t.created_at,t.plan_revision,t.covers,${executionMetadataColumns},
    ARRAY(SELECT d.depends_on FROM dependencies d WHERE d.task_id=t.id) dependency_ids,
    jsonb_build_object('baseWorkspace',control."baseWorkspace",'workspace',control.workspace,
      'workKind',control."workKind",'inputs',CASE WHEN t.state IN('accepted','cancelled','superseded')
        THEN '[]'::jsonb ELSE metadata.input_digests END) execution,
    r.id last_run_id,r.attempt last_run_attempt,r.state last_run_state,
    v.passed acceptance_passed,v.created_at accepted_at,
    EXISTS(SELECT 1 FROM runs l WHERE l.task_id=t.id AND l.state IN('queued','running','cancelling','reconciling')) has_live_run
    FROM tasks t LEFT JOIN task_execution_metadata metadata ON metadata.task_id=t.id
    LEFT JOIN LATERAL jsonb_to_record(metadata.control)
      AS control("baseWorkspace" jsonb,workspace jsonb,"workKind" jsonb) ON true
    LEFT JOIN LATERAL(SELECT id,attempt,state FROM runs WHERE task_id=t.id ORDER BY attempt DESC LIMIT 1)r ON true
    LEFT JOIN LATERAL(SELECT * FROM acceptances WHERE run_id=r.id ORDER BY created_at DESC LIMIT 1)v ON true
    WHERE t.goal_id=$1 AND t.stage IS DISTINCT FROM 'coordinate'`,
      [goalId],
    )
  ).rows, { inputsRequired: row => !closedStates.includes(row.state) });
  const refs = (
    await db.query(
      `SELECT a.id,a.run_id,a.name,a.digest FROM artifacts a
    JOIN runs r ON r.id=a.run_id JOIN tasks t ON t.id=r.task_id
    WHERE t.goal_id=$1 AND r.attempt=t.attempt_count AND t.stage IN('integrate','verify')
    ORDER BY a.created_at DESC`,
      [goalId],
    )
  ).rows;
  const manifests: Record<string, string | null> = {};
  for (const task of tasks.filter((t) => ['integrate', 'verify'].includes(t.stage))) {
    const ref = refs.find((a) => a.run_id === task.last_run_id && a.name === 'candidate.json');
    if (!ref) continue;
    try {
      manifests[task.id] = runtimeCandidateDigest((await readArtifact(ref.id)).bytes, ref.digest);
    } catch {
      manifests[task.id] = null;
    }
  }
  const memories = (
    await db.query(
      `SELECT DISTINCT m.content FROM tasks t
    JOIN memories m ON t.required_memories @> jsonb_build_array(m.id::text)
    WHERE t.goal_id=$1 AND t.state NOT IN('accepted','cancelled','superseded')`,
      [goalId],
    )
  ).rows;
  const dependencyEvidence = (
    await db.query(
      `SELECT DISTINCT a.digest FROM dependencies d
    JOIN tasks consumer ON consumer.id=d.task_id JOIN tasks source ON source.id=d.depends_on
    JOIN runs r ON r.task_id=source.id AND r.attempt=source.attempt_count JOIN artifacts a ON a.run_id=r.id
    WHERE consumer.goal_id=$1 AND consumer.state NOT IN('accepted','cancelled','superseded') AND source.state='accepted'
    AND EXISTS(SELECT 1 FROM acceptances v WHERE v.run_id=r.id AND v.passed)
    AND a.name IN('implementation.patch','repair-report.json','protocol-report.json','verification.json')`,
      [goalId],
    )
  ).rows;
  const inputs = tasks
    .filter((t) => !closedStates.includes(t.state))
    .flatMap((t) => (t.execution?.inputs ?? []).map((i: any) => i.digest));
  inputs.push(...memories.map((m) => sha(m.content)), ...dependencyEvidence.map((a) => a.digest));
  return observeOperationProgress(tasks, manifests, refs, inputs);
}

export async function readOperationProgress(db: DB, goalId: string, readArtifact: ArtifactReader) {
  const previous =
    (await db.query('SELECT value FROM settings WHERE id=$1', ['operation-progress:' + goalId]))
      .rows[0]?.value ?? null;
  return projectOperationProgress(
    await collectOperationProgress(db, goalId, readArtifact),
    previous,
  );
}

/** Caller holds the management transaction lock, after inserting this review exactly once. */
export async function recordOperationProgress(
  db: DB,
  goalId: string,
  reviewRunId: string,
  readArtifact: ArtifactReader,
  prepared?:{observedAt:string;observation:OperationProgressObservation},
) {
  const previous =
    (
      await db.query('SELECT value FROM settings WHERE id=$1 FOR UPDATE', [
        'operation-progress:' + goalId,
      ])
    ).rows[0]?.value ?? null;
  // The slow observation can be collected outside admission, while novelty,
  // review idempotency and monotonic receipt unions still use the locked latest
  // value. A late older observation must not overwrite a newer actual delivery.
  if(prepared && !Number.isFinite(Date.parse(prepared.observedAt)))throw Error('OPERATION_OBSERVATION_TIME');
  const older=!!(prepared && previous?.observedAt && Date.parse(prepared.observedAt)<Date.parse(previous.observedAt));
  const observation=older?previous:(prepared?.observation??await collectOperationProgress(db,goalId,readArtifact));
  const progress = {...projectOperationProgress(
    observation,
    previous,
    reviewRunId,
  ),observedAt:older?previous.observedAt:(prepared?.observedAt??new Date().toISOString())};
  await db.query(
    'INSERT INTO settings VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET value=excluded.value',
    ['operation-progress:' + goalId, JSON.stringify(progress)],
  );
  return progress;
}
