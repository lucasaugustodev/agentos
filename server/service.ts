import { modelRouteAllowed, readModelPolicy } from '../bridge/model-policy.mjs';
import { randomUUID, timingSafeEqual } from 'node:crypto';
import { mkdir, writeFile, readFile } from 'node:fs/promises';
import path from 'node:path';
import { pool, tx, event, lock, type DB } from './db.js';
import { objectiveFrontierNodes, projectWorkFrontier } from './work-frontier.js';
import { config } from './config.js';
import {
  goalSchema,
  taskSchema,
  validateDag,
  DomainError,
  type GoalInput,
  type Criterion,
} from './contracts.js';
import { hash, saveMemory } from './memory.js';
import { quotaFor, supportsAccountExecution } from './accounts.js';
import { workforceCapacityReason } from './workforce-policy.js';
import { prepareTaskInstructions } from './task-instructions.js';
import { workerSchedulerMaintenance, workerRunAdmission } from './worker-rollout.js';
import { independentNativeSupervisorSql } from './native-supervision.js';
import { readPreparedNativeAdmission, validateNativeAdmission, consumeNativeAdmission, finishNativeAdmission, inspectPreparedNativeRun, checkNativePreparation } from './supervised-opencode-admission.js';
import { verifyRepairSourceProof } from './repair-proof.js';
import { readValidationSnapshot, validationReceiptId } from './validation-identity.js';
import {assertVerificationAdmissionProgress} from './verification-progress.js';
import {routeVerificationPlanningRecovery} from './verification-planning-recovery.js';
import {sourceContractPlanningFailure} from './source-output-validation.js';
export async function createGoal(raw: unknown, isDemo = false) {
  const input = goalSchema.parse(raw);
  validateDag(input.tasks);
  return tx(async (db) => {
    await lock(db);
    const inputHash = hash(JSON.stringify(input));
    const old = (
      await db.query('SELECT id,input_hash FROM goals WHERE idempotency_key=$1', [
        input.idempotencyKey,
      ])
    ).rows[0];
    if (old) {
      if (old.input_hash !== inputHash)
        throw new DomainError(
          'IDEMPOTENCY_CONFLICT',
          'Esta chave já foi usada para um plano diferente.',
        );
      return { id: old.id, duplicate: true };
    }
    for (const t of input.tasks) {
      const a = (await db.query('SELECT * FROM agents WHERE id=$1', [t.agentId])).rows[0];
      if (!a) throw new DomainError('AGENT_MISSING', 'Agente não encontrado: ' + t.agentId, 400);
      if (a.runtime === 'builtin' && !isDemo)
        throw new DomainError(
          'DEMO_ONLY',
          'O executor técnico é exclusivo do exemplo isolado.',
          400,
        );
      if (
        t.accountId &&
        !(await db.query('SELECT id FROM accounts WHERE id=$1', [t.accountId])).rowCount
      )
        throw new DomainError('ACCOUNT_MISSING', 'Conta não encontrada.', 400);
    }
    const id = randomUUID();
    await db.query(
      'INSERT INTO goals(id,title,description,project,idempotency_key,is_demo,input_hash) VALUES($1,$2,$3,$4,$5,$6,$7)',
      [id, input.title, input.description, input.project, input.idempotencyKey, isDemo, inputHash],
    );
    const ids = new Map(input.tasks.map((t) => [t.key, randomUUID()]));
    for (const t of input.tasks)
      await db.query(
        `INSERT INTO tasks(id,goal_id,key,title,description,agent_id,account_id,state,priority,resources,criteria,required_memories,max_attempts) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
        [
          ids.get(t.key),
          id,
          t.key,
          t.title,
          t.description,
          t.agentId,
          t.accountId ?? null,
          t.dependencies.length ? 'blocked_dependencies' : 'ready',
          t.priority,
          JSON.stringify(t.resources),
          JSON.stringify(t.criteria),
          JSON.stringify(t.requiredMemories),
          t.maxAttempts,
        ],
      );
    for (const t of input.tasks)
      if (t.recovery)
        await db.query('UPDATE tasks SET recovery=$2 WHERE id=$1', [
          ids.get(t.key),
          JSON.stringify(t.recovery),
        ]);
    for (const t of input.tasks)
      if (t.model)
        await db.query('UPDATE tasks SET model=$2 WHERE id=$1', [ids.get(t.key), t.model]);
    for (const t of input.tasks)
      if (t.execution)
        await db.query('UPDATE tasks SET execution=$2,resources=$3 WHERE id=$1', [
          ids.get(t.key),
          JSON.stringify(t.execution),
          JSON.stringify([
            ...new Set([
              ...t.resources,
              'host:agentos-remote',
              'workspace:' + t.execution.workspace,
            ]),
          ]),
        ]);
    for (const t of input.tasks)
      for (const dep of t.dependencies)
        await db.query('INSERT INTO dependencies VALUES($1,$2)', [ids.get(t.key), ids.get(dep)]);
    await event(db, 'GOAL_CREATED', id, { title: input.title, tasks: input.tasks.length, isDemo });
    return { id, duplicate: false };
  });
}
export async function createDemo() {
  const key = randomUUID();
  const stages = [
    ['scope', 'Conferir contrato', []],
    ['backend', 'Validar caminho backend', ['scope']],
    ['frontend', 'Validar caminho frontend', ['scope']],
    ['context', 'Conferir contexto compartilhado', ['scope']],
    ['integration', 'Reunir artefatos', ['backend', 'frontend', 'context']],
    ['acceptance', 'Verificar entrega integrada', ['integration']],
  ] as const;
  await saveMemory({
    project: 'orchestrator-demo',
    title: 'Contrato da demonstração',
    content:
      'Esta operação valida a fila, dependências, artefatos e memória do AgentOS. Não executa jogos nem utiliza um modelo de IA.',
    kind: 'decision',
    verification: 'confirmed',
    source: 'system:demo-contract',
  });
  return createGoal(
    {
      title: 'Validar a operação do AgentOS',
      description:
        'Exemplo técnico isolado com três frentes paralelas, contexto automático e aceite por artefato. Nenhuma tarefa de produção.',
      project: 'orchestrator-demo',
      idempotencyKey: key,
      tasks: stages.map(([k, title, deps]) => ({
        key: k,
        title,
        description: 'Produzir e verificar um artefato JSON do contrato local.',
        agentId: 'system.validation',
        accountId: 'local-validation',
        dependencies: [...deps],
        resources: [],
        criteria: [
          {
            id: 'result',
            label: 'Resultado JSON íntegro com marcador validado',
            type: 'json_field',
            name: 'result.json',
            field: 'validated',
            equals: true,
          },
        ],
      })),
    },
    true,
  );
}
export async function refreshGoal(db: DB, id: string) {
  const rows = (
    await db.query(`SELECT t.state FROM tasks t WHERE t.goal_id=$1 AND t.state<>'superseded'
      AND (t.plan_revision IS NULL OR t.plan_revision=COALESCE(
        (SELECT max(p.revision) FROM plan_revisions p WHERE p.objective_id=t.goal_id),t.plan_revision))`, [id])
  ).rows;
  if (!rows.length) return;
  let state = 'active';
  if (rows.every((t) => t.state === 'accepted')) state = 'completed';
  else if (
    rows.every((t) => ['accepted', 'failed', 'cancelled', 'blocked_failed'].includes(t.state))
  )
    state = rows.some((t) => t.state === 'failed' || t.state === 'blocked_failed')
      ? 'failed'
      : 'cancelled';
  if (state === 'completed') {
    const latest = (await db.query('SELECT plan FROM plan_revisions WHERE objective_id=$1 ORDER BY revision DESC LIMIT 1',[id])).rows[0]?.plan;
    if (latest?.incremental?.pendingRequirementIds?.length) state = 'active';
    // Same supersede rule as the planner advance: feedback a later completed
    // plan could already see cannot veto completion forever (legacy zombies
    // attached only to terminally failed planning runs never get consumed).
    if (state === 'completed' && (await db.query(`SELECT 1 FROM objective_feedback f
      WHERE f.objective_id=$1 AND NOT EXISTS(SELECT 1 FROM planning_runs p
        WHERE p.objective_id=f.objective_id AND p.state='completed'
          AND p.feedback->'feedbackIds' ? f.id::text)
        AND NOT EXISTS(SELECT 1 FROM planning_runs c
          WHERE c.objective_id=f.objective_id AND c.state='completed'
            AND c.created_at>f.created_at) LIMIT 1`,[id])).rowCount)
      state = 'active';
  }
  const old = (await db.query('SELECT state FROM goals WHERE id=$1', [id])).rows[0];
  if (old.state !== state && !['paused','cancelled'].includes(old.state)) {
    await db.query('UPDATE goals SET state=$2 WHERE id=$1', [id, state]);
    if (['completed', 'cancelled'].includes(state))
      await db.query('UPDATE objectives SET status=$2,reason=NULL,updated_at=now() WHERE id=$1', [
        id,
        state,
      ]);
    await event(db, 'GOAL_' + state.toUpperCase(), id, {});
  }
}
/** Admission needs routing/resource metadata, never every waiting worker's
 * embedded inputs. The full contract is read by prepareTaskInstructions only
 * after all dependency, quota, capacity and lease checks have succeeded.
 * Delivery urgency follows real, same-goal dependency edges to unfinished
 * prerequisites. Aging adds one point per five waiting minutes outside backoff (60
 * maximum); this is a bounded fairness aid, not a latency guarantee. No slots
 * are reserved here: blocked critical work lends capacity to other candidates. */
export async function readScheduleCandidates(db:DB=pool,service?:'agentos-opencode') {
  return (await db.query(`WITH RECURSIVE delivery_path(task_id,goal_id,inherited_priority) AS (
      SELECT t.id,t.goal_id,t.priority+40
      FROM tasks t JOIN goals g ON g.id=t.goal_id
      JOIN settings w ON w.id='workforce:'||g.id AND w.value->>'deliveryFirstEnabled'='true'
      WHERE g.state='active' AND t.stage IN('integrate','verify','review')
        AND t.state NOT IN('accepted','cancelled','superseded')
      UNION
      SELECT prerequisite.id,p.goal_id,p.inherited_priority
      FROM delivery_path p JOIN dependencies d ON d.task_id=p.task_id
      JOIN tasks prerequisite ON prerequisite.id=d.depends_on AND prerequisite.goal_id=p.goal_id
      WHERE prerequisite.state NOT IN('accepted','cancelled','superseded')
    ), inherited AS (
      SELECT task_id,max(inherited_priority) inherited_priority FROM delivery_path GROUP BY task_id
    ), candidates AS MATERIALIZED (
    SELECT t.id,t.goal_id,t.state,t.reason,t.stage,t.next_attempt_at,t.priority,t.created_at,t.key,
      t.account_id,t.allowed_accounts,t.model,t.resources,t.attempt_count,t.instruction_revision_id,
      t.parent_task_id,t.agent_role,t.agent_level,
      CASE WHEN t.execution IS NULL THEN NULL ELSE jsonb_build_object(
        'kind',t.execution->>'kind','workforceLane',t.execution->>'workforceLane','readOnlySource',t.execution->'readOnlySource',
        'supervisedAdmission',t.execution->'supervisedAdmission') END execution,
      g.state goal_state,a.runtime,a.version agent_version,
      prior.id prepared_source_run_id,
      (prior.runtime='opencode' AND EXISTS(SELECT 1 FROM events pe
        WHERE pe.entity_id=t.id::text AND pe.kind='SUPERVISION_RESUME_PREPARED'
          AND pe.payload->>'sourceRunId'=prior.id::text)) supervised_native_prepared,
      CASE WHEN workforce.value->>'deliveryFirstEnabled'='true' THEN
        CASE WHEN t.stage IN('integrate','verify','review') THEN 40
          WHEN t.execution->>'workKind'='repair' THEN 20 ELSE 0 END ELSE 0 END delivery_boost,
      inherited.inherited_priority,
      CASE WHEN workforce.value->>'deliveryFirstEnabled'='true' THEN
        LEAST(60,GREATEST(0,floor(extract(epoch FROM (now()-GREATEST(t.created_at,
          COALESCE(prior.finished_at,t.created_at),COALESCE(t.next_attempt_at,t.created_at))))/300)))::int
        ELSE 0 END age_priority
    FROM tasks t JOIN goals g ON g.id=t.goal_id JOIN agents a ON a.id=t.agent_id
    LEFT JOIN settings workforce ON workforce.id='workforce:'||g.id
    LEFT JOIN inherited ON inherited.task_id=t.id
    LEFT JOIN runs prior ON prior.task_id=t.id AND prior.attempt=t.attempt_count
    WHERE t.state IN('ready','blocked_dependencies','blocked_failed','waiting_quota','waiting_resource','waiting_configuration','retry_scheduled')
      AND NOT EXISTS(SELECT 1 FROM settings validation WHERE validation.id LIKE 'candidate-validation:%'
        AND validation.value->>'taskId'=t.id::text AND validation.value->>'runId'=prior.id::text
        AND validation.value->>'runFence'=prior.fence::text AND validation.value->>'contextVersion'=prior.context_id::text
        AND validation.value->>'state' IN('proposed','freezing','frozen','running','uncertain'))
      AND NOT EXISTS(SELECT 1 FROM settings rebase WHERE rebase.id LIKE 'published-rebase:%'
        AND rebase.value->>'taskId'=t.id::text AND rebase.value->>'runId'=prior.id::text
        AND rebase.value->>'runFence'=prior.fence::text AND rebase.value->>'contextVersion'=prior.context_id::text
        AND rebase.value->>'state' IN('proposed','preparing','prepared','applying','uncertain'))
      AND g.state='active' AND ($1::boolean=false OR (workforce.value->>'enabled'='true' AND
        (t.execution->>'kind'='opencode' OR (t.execution->>'kind'='harness' AND workforce.value->>'detachedHarnessEnabled'='true'))))
    )
    SELECT c.*,GREATEST(c.priority+c.delivery_boost,COALESCE(c.inherited_priority,c.priority))+c.age_priority effective_priority
    FROM candidates c ORDER BY effective_priority DESC,c.created_at,c.key`,
    [service==='agentos-opencode'])).rows;
}

export async function schedule(service?:'agentos-opencode',inspect=inspectPreparedNativeRun) {
  // Inspection is only requested after all ordinary admission checks pass.
  // Leave the transaction before SSH, then restart the COMPLETE ordering. A
  // competing scheduler reaches the same prepared request rather than taking
  // its slot. No persistent capacity reservation or second recovery owner.
  const observed=new Map<string,{proof?:any;failed?:boolean}>();
  const allDispatched:string[]=[];
  for(let inspections=0;inspections<=4;inspections++){
  const pass=await tx(async (db) => {
    await lock(db);
    const rolloutMaintenance = await workerSchedulerMaintenance(db);
    const settings = (await db.query("SELECT value FROM settings WHERE id='capacity'")).rows[0]
      .value;
    const waiting = await readScheduleCandidates(db,service);
    const dispatched: string[] = [];
    const storageBlocked=(await db.query("SELECT value->>'state' state FROM settings WHERE id='storage:agentos-workspaces'")).rows[0]?.state==='blocked';
    for (const t of waiting) {
      await db.query('SAVEPOINT task_admission');
      try {
      if (t.next_attempt_at && new Date(t.next_attempt_at) > new Date()) continue;
      // Earlier candidates may change dependency states in this transaction.
      // Read only their small current state here, before any full contract.
      const deps = (await db.query(
        'SELECT t.state FROM dependencies d JOIN tasks t ON t.id=d.depends_on WHERE d.task_id=$1',
        [t.id],
      )).rows;
      const setWait = async (state: string, reason: string) => {
        if (t.state !== state || t.reason !== reason) {
          await db.query('UPDATE tasks SET state=$2,reason=$3,updated_at=now() WHERE id=$1', [
            t.id,
            state,
            reason,
          ]);
          await event(db, 'TASK_WAITING', t.id, { state, reason });
        }
      };
      if(storageBlocked && ['harness','opencode'].includes(t.execution?.kind)){
        await setWait('waiting_resource','Volume sem capacidade disponível; o controlador retoma após conferir o armazenamento.');
        continue;
      }
      if (deps.some(d=>['failed','cancelled','blocked_failed'].includes(d.state))) {
        await setWait('blocked_failed', 'Uma dependência terminou sem aceite.');
        await refreshGoal(db, t.goal_id);
        continue;
      }
      if (deps.some(d=>d.state!=='accepted')) {
        await setWait('blocked_dependencies', 'Aguardando aceite das dependências.');
        continue;
      }
      await assertVerificationAdmissionProgress(db,t);
      // Skip a draining executor before INSERT, preserving the rest of the batch.
      const laneFlags = (
        await db.query(
          `SELECT
        EXISTS(SELECT 1 FROM settings WHERE id='supervision:'||$1 AND value->>'enabled'='true') AS "supervisionEnabled",
        EXISTS(SELECT 1 FROM settings WHERE id='workforce:'||$1 AND value->>'enabled'='true') AS "workforceEnabled",
        EXISTS(SELECT 1 FROM settings WHERE id='workforce:'||$1 AND value->>'enabled'='true'
          AND value->>'detachedHarnessEnabled'='true') AS "detachedHarnessEnabled"`,
          [t.goal_id],
        )
      ).rows[0];
      const admission = workerRunAdmission(rolloutMaintenance, { ...t, ...laneFlags }, t.runtime);
      if (!admission.allowed) {
        await setWait(
          'waiting_resource',
          'Atualização do executor ' + admission.service + ': aguardando adoção comprovada.',
        );
        continue;
      }
      const objective = (await db.query('SELECT request FROM objectives WHERE id=$1', [t.goal_id]))
        .rows[0];
      if (objective) {
        const capacityOverride=(await db.query('SELECT value FROM settings WHERE id=$1',['objective-capacity:'+t.goal_id])).rows[0]?.value;
        const objectiveCapacity=capacityOverride?.maxRuns??objective.request.requestedCapacity;
        const n = Number(
          (
            await db.query(
              "SELECT count(*) FROM runs r JOIN tasks x ON x.id=r.task_id WHERE x.goal_id=$1 AND r.state IN('queued','running','cancelling','reconciling')",
              [t.goal_id],
            )
          ).rows[0].count,
        );
        if (n >= objectiveCapacity) {
          await setWait('waiting_resource', 'Vagas solicitadas para este objetivo ocupadas.');
          continue;
        }
      }
      if (t.runtime === 'harness' && !config.dshCommand && !config.bridgeHost) {
        await setWait(
          'waiting_configuration',
          'Configure um runtime Harness isolado para esta especialização.',
        );
        continue;
      }
      if (t.execution && (!config.remoteExecution || !config.bridgeHost)) {
        await setWait('waiting_configuration', 'Executor remoto isolado ainda não habilitado.');
        continue;
      }
      const accounts = (
        await db.query(
          'SELECT * FROM accounts WHERE enabled=true AND ($1::text IS NULL OR id=$1) ORDER BY id',
          [t.account_id],
        )
      ).rows.filter((a) =>
        !supportsAccountExecution(a, t.execution) ||
        (t.allowed_accounts?.length && !t.allowed_accounts.includes(a.id))
          ? false
          : t.runtime === 'builtin'
            ? a.provider === 'local'
            : a.provider !== 'local' && (config.dshCommand || (a.bridge_id && config.bridgeHost)),
      );
      let account: any = null,
        why = 'Nenhuma conta habilitada para este runtime.';
      for (const a of accounts) {
        if (a.provider !== 'local' && !modelRouteAllowed(a.id, t.model ?? a.model)) {
          why = 'Política exclusiva GLM: aguardando rota autorizada.'; continue;
        }
        if (
          t.execution &&
          (!a.bridge_id || a.provider.startsWith('agy') || a.provider === 'local')
        ) {
          why = 'Esta conta ainda não tem suporte a ferramentas no executor isolado.';
          continue;
        }
        if (a.bridge_id) {
          if (t.model && !a.models.includes(t.model)) {
            why = 'Modelo indisponível nesta conta.';
            continue;
          }
          const quota = quotaFor(a, t.model ?? a.model);
          a.quota_status = quota.status;
          a.remaining_percent = quota.remaining;
        }
        const stale = a.observed_at && Date.now() - new Date(a.observed_at).getTime() > 600000;
        if (
          a.quota_status === 'exhausted' ||
          (a.remaining_percent !== null && a.remaining_percent <= 0)
        ) {
          why = 'Cota esgotada. Aguardando nova observação de disponibilidade.';
          continue;
        }
        if ((a.quota_status === 'unknown' || stale) && !a.allow_unknown) {
          why = 'Cota desconhecida ou observação vencida.';
          continue;
        }
        if (a.quota_status === 'auth_required') {
          why = 'Autenticação da conta precisa ser renovada.';
          continue;
        }
        const n = Number(
          (
            await db.query(
              "SELECT (SELECT count(*) FROM runs WHERE account_id=$1 AND state IN('queued','running','cancelling','reconciling'))+(SELECT count(*) FROM cycle_attempts WHERE account_id=$1 AND state IN('running','cancelling','reconciling'))+(SELECT count(*) FROM chat_messages WHERE account_id=$1 AND state IN('queued','running','cancelling'))+(SELECT count(*) FROM planning_runs WHERE account_id=$1 AND state='running' AND output IS NULL) count",
              [a.id],
            )
          ).rows[0].count,
        );
        const exclusive = readModelPolicy();
        const sessionCeiling = exclusive
          ? Math.min(a.max_concurrency, exclusive.sessionLimit - (t.stage === 'coordinate' ? 0 : 1)) : a.max_concurrency;
        if (n >= sessionCeiling) {
          why = 'Vagas desta conta ocupadas.';
          continue;
        }
        account = a;
        break;
      }
      if (!account) {
        await setWait(accounts.length ? 'waiting_quota' : 'waiting_configuration', why);
        continue;
      }
      const workforce = (
        await db.query('SELECT value FROM settings WHERE id=$1', ['workforce:' + t.goal_id])
      ).rows[0]?.value;
      if (workforce?.enabled) {
        const runs = (
          await db.query(
            `SELECT r.state,r.model,r.runtime,x.goal_id,x.stage,
              CASE WHEN x.execution IS NULL THEN NULL ELSE jsonb_build_object(
                'kind',x.execution->>'kind','workforceLane',x.execution->>'workforceLane') END execution
              FROM runs r JOIN tasks x ON x.id=r.task_id WHERE x.goal_id=$1 AND r.state IN ('queued','running','cancelling','reconciling')`,
            [t.goal_id],
          )
        ).rows;
        const capacityReason = workforceCapacityReason(
          { ...t, account_id: account.id, model: t.model ?? account.model },
          runs,
          workforce,
        );
        if (capacityReason) {
          await setWait(
            'waiting_resource',
            capacityReason === 'WORKFORCE_LUNA_CAPACITY'
              ? 'As duas vagas GPT-6 Luna estão ocupadas; próxima tarefa na fila.'
              : capacityReason === 'WORKFORCE_GLM_CAPACITY'
                ? 'As vagas GLM estão ocupadas; próxima tarefa na fila.'
                : capacityReason === 'WORKFORCE_MUSE_CAPACITY'
                  ? Number(workforce.museTarget) === 0
                    ? 'Execuções Muse desativadas nesta operação; aguardando reuso validado ou encaminhamento supervisionado.'
                    : 'As vagas Muse estão ocupadas; próxima tarefa na fila.'
                : 'Vaga reservada à coordenação da operação.',
          );
          continue;
        }
      }
      const active = Number(
        (
          await db.query(
            "SELECT (SELECT count(*) FROM runs WHERE state IN('queued','running','cancelling','reconciling'))+(SELECT count(*) FROM cycle_attempts WHERE account_id IS NOT NULL AND state IN('running','cancelling','reconciling')) count",
          )
        ).rows[0].count,
      );
      if (active >= settings.maxRuns) {
        await setWait('waiting_resource', 'Capacidade global ocupada.');
        continue;
      }
      if(workforce?.enabled && t.stage!=='coordinate' && active>=settings.maxRuns-1 &&
        (await db.query(`SELECT 1 FROM tasks c JOIN goals g ON g.id=c.goal_id JOIN settings s ON s.id='workforce:'||g.id
          WHERE g.state='active' AND s.value->>'enabled'='true' AND c.stage='coordinate'
          AND c.state NOT IN('cancelled','superseded') AND NOT EXISTS(SELECT 1 FROM runs r WHERE r.task_id=c.id
            AND r.state IN('queued','running','cancelling','reconciling')) LIMIT 1`)).rowCount){
        await setWait('waiting_resource','Uma vaga global reservada à supervisão.');continue;
      }
      let occupied = false;
      const remote = (await db.query("SELECT value FROM settings WHERE id='remote-capacity'"))
        .rows[0]?.value ?? { maxRuns: 1 };
      if (
        t.resources.includes('remote-slot') &&
        (await db.query("SELECT 1 FROM leases WHERE resource='host:agentos-remote' LIMIT 1"))
          .rowCount
      )
        occupied = true;
      if (
        t.resources.includes('host:agentos-remote') &&
        (await db.query("SELECT 1 FROM leases WHERE resource='remote-slot' LIMIT 1")).rowCount
      )
        occupied = true;
      for (const resource of t.resources) {
        const n = Number(
          (await db.query('SELECT count(*) FROM leases WHERE resource=$1', [resource])).rows[0]
            .count,
        );
        if (
          n >=
          (resource === 'browser'
            ? settings.browserSlots
            : resource === 'remote-slot'
              ? remote.maxRuns
              : 1)
        )
          occupied = true;
      }
      if (occupied) {
        await setWait('waiting_resource', 'Recurso exclusivo ou vaga de navegador ocupada.');
        continue;
      }
      let prepared:Awaited<ReturnType<typeof readPreparedNativeAdmission>>=null;
      if(t.supervised_native_prepared || t.execution?.supervisedAdmission){
        prepared=await readPreparedNativeAdmission(db,t.id);
        if(!prepared)throw Object.assign(Error('BINDING_CHANGED'),{code:'SUPERVISED_NATIVE_BINDING_CHANGED'});
        // The route is immutable across the wait; do not use another account
        // selected from a subsequently edited task.
        if(account.id!==prepared.receipt.route.accountId || (t.model??account.model)!==prepared.receipt.route.model)
          throw Object.assign(Error('BINDING_CHANGED'),{code:'SUPERVISED_NATIVE_BINDING_CHANGED'});
        await checkNativePreparation(db,prepared.task,prepared.run,{supervisionResumeCaseId:prepared.receipt.caseId});
        const observation=observed.get(prepared.receipt.id);
        if(!observation)return {dispatched,inspect:prepared.receipt};
        if(observation.failed)throw Object.assign(Error('INSPECTION_FAILED'),{code:'SUPERVISED_NATIVE_INSPECTION_FAILED'});
      }
      await prepareTaskInstructions(db, t.id);
      if(prepared){
        prepared=await readPreparedNativeAdmission(db,t.id,prepared.receipt.id);
        if(!prepared)throw Object.assign(Error('BINDING_CHANGED'),{code:'SUPERVISED_NATIVE_BINDING_CHANGED'});
        await validateNativeAdmission(db,prepared,observed.get(prepared.receipt.id)!.proof);
      }
      const id = randomUUID(),
        fence = randomUUID();
      if(prepared)await consumeNativeAdmission(db,prepared,id);
      await db.query(
        'INSERT INTO runs(id,task_id,attempt,fence,account_id,runtime,agent_version,checkpoint,parent_task_id,agent_role,agent_level) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)',
        [id, t.id, t.attempt_count + 1, fence, account.id, prepared?'opencode':t.runtime, prepared?.receipt.route.agentVersion??t.agent_version,
          prepared?JSON.stringify(prepared.checkpoint):null,t.parent_task_id??null,t.agent_role??null,t.agent_level??0],
      );
      if(!prepared)await db.query(
        'UPDATE runs SET checkpoint=(SELECT checkpoint FROM runs WHERE task_id=$2 AND id<>$1 AND checkpoint IS NOT NULL ORDER BY attempt DESC LIMIT 1) WHERE id=$1',
        [id, t.id],
      );
      await db.query('UPDATE runs SET model=$2 WHERE id=$1', [id, t.model ?? account.model]);
      for (const r of [...new Set<string>(t.resources)])
        await db.query('INSERT INTO leases VALUES($1,$2)', [r, id]);
      await db.query(
        "UPDATE tasks SET state='queued',reason=NULL,attempt_count=attempt_count+1,next_attempt_at=NULL,updated_at=now() WHERE id=$1",
        [t.id],
      );
      if(prepared){
        await finishNativeAdmission(db,prepared,id);
        await db.query('INSERT INTO opencode_jobs(run_id) VALUES($1)',[id]);
      }else await db.query('INSERT INTO outbox(run_id) VALUES($1)', [id]);
      await event(db, 'RUN_QUEUED', t.id, {
        runId: id,
        accountId: account.id,
        attempt: t.attempt_count + 1,
        dispatchPriority: { base: t.priority, inherited: t.inherited_priority,
          aging: t.age_priority, effective: t.effective_priority },
      });
      dispatched.push(id);
      } catch(e:any) {
        await db.query('ROLLBACK TO SAVEPOINT task_admission');
        const code=e?.code==='P0001'?e.message:e?.code;
        const nativeRefusals=['SUPERVISION_HANDOFF_CHANGED','SUPERVISION_DISABLED','DEPENDENCIES_PENDING',
          'REPEATED_NO_PROGRESS','SEMANTIC_RETRY_REQUIRES_SUPERVISION','SEMANTIC_RETRY_EVIDENCE_INVALID','INVALID_PROVIDER_BACKOFF_PROOF'];
        if((t.supervised_native_prepared || t.execution?.supervisedAdmission) && (/^SUPERVISED_NATIVE_/.test(code??'') ||
          (!e?.code && nativeRefusals.includes(e?.message)))){
          // Preserve the receipt on a transient inspection/provider wait. A
          // changed contract/owner is returned to the existing supervision.
          const safeCode=/^[A-Z][A-Z0-9_]+$/.test(e?.message??'')?e.message:'ADMISSION_FAILED';
          const transient=['INSPECTION_FAILED','STOP_UNCONFIRMED','PROVIDER_RETRY_PENDING','SUPERVISION_DISABLED'].includes(safeCode);
          const notBefore=Date.parse(e?.notBefore);
          const nextAt=new Date(safeCode==='PROVIDER_RETRY_PENDING'&&Number.isFinite(notBefore)?Math.max(Date.now()+15000,notBefore):Date.now()+15000);
          const state=transient?'waiting_resource':'recovery_blocked';
          const reason='Retomada supervisionada aguardando conferência: '+safeCode+'.';
          // Rollback restored this task's receipt. A later transaction may
          // replace it, so bind the wait to precisely the inspected request.
          const receipt=t.execution?.supervisedAdmission??null;
          const changed=await db.query(`UPDATE tasks SET state=$2,reason=$3,next_attempt_at=$4,updated_at=now()
            WHERE id=$1 AND COALESCE(execution->'supervisedAdmission','null'::jsonb)=$5::jsonb AND attempt_count=$6`,
            [t.id,state,reason,nextAt,JSON.stringify(receipt),t.attempt_count]);
          if(changed.rowCount)await event(db,'SUPERVISED_NATIVE_ADMISSION_DEFERRED',t.id,{code:safeCode,state,
            admissionReceiptId:typeof receipt?.id==='string'&&/^[a-f0-9-]{36}$/i.test(receipt.id)?receipt.id:null,
            sourceRunId:t.prepared_source_run_id,
            nextAt:nextAt.toISOString()});
          continue;
        }
        if(code==='SCOPED_PLANNER_SCOPE_BUSY'){
          const reason='Já existe um planejador ativo neste escopo; aguardando sua conclusão.';
          if(t.state!=='waiting_resource'||t.reason!==reason){
            await db.query("UPDATE tasks SET state='waiting_resource',reason=$2,updated_at=now() WHERE id=$1",[t.id,reason]);
            await event(db,'TASK_WAITING',t.id,{state:'waiting_resource',reason,code});
          }
          continue;
        }
        if(code==='SCOPED_PLANNER_CAPACITY'){
          await db.query("UPDATE tasks SET state='waiting_resource',reason='Dois planejadores por escopo ocupados; aguardando vaga.',updated_at=now() WHERE id=$1",[t.id]);
          continue;
        }
        if(!/^(TASK_INSTRUCTIONS_CHANGED|INSTRUCTION_CASE_CHANGED|INSTRUCTION_CASE_INVALID|INSTRUCTION_SOURCE_INVALID|INSTRUCTION_DECISION_UNVERIFIED|SUPERVISION_INPUTS_INVALID|SUPERVISION_INPUT_DIGEST|SUPERVISION_INPUTS_TOO_LARGE|SUPERVISION_INPUT_COLLISION|VERIFICATION_NO_PROGRESS|VERIFICATION_CANDIDATE_UNBOUND|VERIFICATION_CANDIDATE_INVALID)$/.test(code??''))throw e;
        const reason='Admissão bloqueada: '+code+'. Contrato e evidências preservados; outras tarefas continuam.';
        await db.query("UPDATE tasks SET state='recovery_blocked',reason=$2,execution=CASE WHEN execution IS NULL THEN NULL ELSE jsonb_set(execution,'{continuation}',jsonb_build_object('state','needs_action','message',$2::text,'updatedAt',now())) END,updated_at=now() WHERE id=$1",
          [t.id,reason]);
        await event(db,'RUN_ADMISSION_BLOCKED',t.id,{code,goalId:t.goal_id,instructionRevisionId:t.instruction_revision_id,preserved:true});
        if(code.startsWith('VERIFICATION_'))await routeVerificationPlanningRecovery(db,t.id);
      } finally {
        await db.query('RELEASE SAVEPOINT task_admission');
      }
    }
    return {dispatched,inspect:null};
  });
  allDispatched.push(...pass.dispatched);
  if(!pass.inspect || inspections===4)return allDispatched;
  try {observed.set(pass.inspect.id,{proof:await inspect(pass.inspect.sourceRunId)});}
  catch {observed.set(pass.inspect.id,{failed:true});}
  }
  return allDispatched;
}
export async function startRun(id: string, workerId: string) {
  return tx(async (db) => {
    await lock(db);
    const run = (
      await db.query(
        "UPDATE runs SET state='running',worker_id=$2,started_at=clock_timestamp(),heartbeat_at=clock_timestamp() WHERE id=$1 AND state='queued' RETURNING *",
        [id, workerId],
      )
    ).rows[0];
    if (!run) return null;
    await db.query("UPDATE tasks SET state='running',updated_at=now() WHERE id=$1", [run.task_id]);
    await event(db, 'RUN_STARTED', run.task_id, { runId: id, workerId });
    return run;
  });
}
export async function heartbeat(id: string, fence: string) {
  const r = (
    await pool.query(
      "UPDATE runs SET heartbeat_at=clock_timestamp() WHERE id=$1 AND fence=$2 AND state IN('running','cancelling') RETURNING state",
      [id, fence],
    )
  ).rows[0];
  return r ?? null;
}
export async function markUncertain() {
  await tx(async (db) => {
    await lock(db);
    const hasNative=(await db.query("SELECT to_regclass('opencode_jobs') present")).rows[0]?.present;
    // Detached owners hold a version/fence-bound PostgreSQL advisory lock.
    // Delayed heartbeat writes do not make that actor absent. The independent
    // supervisor reconciler handles an actually lost owner and remote stop.
    const owned=hasNative?`EXISTS(SELECT 1 FROM opencode_jobs j WHERE j.run_id=r.id
      AND j.state='running' AND ${independentNativeSupervisorSql()})`:'false';
    const rows = (
      await db.query(
        `UPDATE runs r SET state='reconciling',error_code='HEARTBEAT_LOST'
          WHERE r.state IN('running','cancelling') AND r.heartbeat_at<clock_timestamp()-interval '45 seconds'
          AND NOT (${owned}) RETURNING r.*`,
      )
    ).rows;
    for (const r of rows) {
      await db.query(
        "UPDATE tasks SET state='reconciling',reason='Executor sem heartbeat recente; efeitos precisam de reconciliação.',updated_at=now() WHERE id=$1",
        [r.task_id],
      );
      await event(db, 'RUN_RECONCILING', r.task_id, { runId: r.id });
    }
  });
}
export async function writeArtifact(
  runId: string,
  fence: string,
  name: string,
  content: string | Buffer,
  mime = 'application/json',
) {
  if (
    name === 'task-instructions-history.json' ||
    !/^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,100}$/.test(name) ||
    Buffer.byteLength(content) > 8 * 1024 * 1024
  )
    throw new DomainError('ARTIFACT_INVALID', 'Nome ou tamanho de artefato inválido.', 400);
  const bytes = Buffer.from(content),
    id = randomUUID(),
    digest = hash(bytes);
  const dir = path.join(config.dataDir, 'artifacts');
  await mkdir(dir, { recursive: true });
  await writeFile(path.join(dir, id), bytes, { flag: 'wx' });
  await tx(async (db) => {
    await lock(db);
    const r = (
      await db.query("SELECT * FROM runs WHERE id=$1 AND fence=$2 AND state='running'", [
        runId,
        fence,
      ])
    ).rows[0];
    if (!r) throw new DomainError('STALE_RUN', 'A tentativa não pode mais publicar artefatos.');
    await db.query(
      'INSERT INTO artifacts(id,run_id,name,mime,digest,size) VALUES($1,$2,$3,$4,$5,$6)',
      [id, runId, name, mime, digest, bytes.length],
    );
    await event(db, 'ARTIFACT_CREATED', r.task_id, { runId, artifactId: id, name, digest });
  });
  return { id, name, digest };
}
export async function readArtifact(id: string) {
  const a = (await pool.query('SELECT * FROM artifacts WHERE id=$1', [id])).rows[0];
  if (!a) throw new DomainError('NOT_FOUND', 'Artefato não encontrado.', 404);
  const bytes = await readFile(path.join(config.dataDir, 'artifacts', a.id));
  if (hash(bytes) !== a.digest)
    throw new DomainError('ARTIFACT_CORRUPT', 'A integridade do artefato não confere.');
  return { ...a, bytes };
}
export async function completeRun(
  id: string,
  fence: string,
  summary: string,
  usage: unknown = null,
) {
  return tx(async (db) => {
    await lock(db);
    const r = (
      await db.query(
        "UPDATE runs SET state='submitted',summary=$3,usage=$4,finished_at=now() WHERE id=$1 AND fence=$2 AND state='running' RETURNING *",
        [id, fence, summary, usage ? JSON.stringify(usage) : null],
      )
    ).rows[0];
    if (!r) throw new DomainError('STALE_RUN', 'Tentativa expirada, cancelada ou já entregue.');
    await db.query('DELETE FROM leases WHERE run_id=$1', [id]);
    await db.query("UPDATE tasks SET state='submitted',reason=NULL,updated_at=now() WHERE id=$1", [
      r.task_id,
    ]);
    await event(db, 'RUN_SUBMITTED', r.task_id, { runId: id });
    return r;
  });
}
export async function validateTask(taskId: string, reviewer?: string) {
  const task = await readValidationSnapshot(pool, taskId, reviewer);
  if (!task)
    throw new DomainError('NOT_SUBMITTED', 'A tarefa ainda não está pronta para validação.');
  const {verifiedWorkerValidationGate,workerValidationCommitIdentity,validationDigest}=await import('./worker-validation.js');
  const proof=task.validation_input.workerValidation?await verifiedWorkerValidationGate(pool,task.run_id):null;
  const ci:import('./worker-validation.js').WorkerValidationGate=proof?.gate??{required:false,state:'legacy',detail:''};
  if(ci.state==='pending') return {passed:false,pending:true,checks:[{id:'registered-ci',label:'CI registrado da candidata',
    type:'registered_ci',passed:false,detail:ci.detail}],duplicate:false};
  if(task.receipt_match && ci.required && ci.state!=='passed' && task.accepted_passed)
    throw new DomainError('STATE_CHANGED','A prova de CI perdeu autoridade; o aceite histórico foi preservado.');
  if (task.receipt_match)
    return { passed: task.accepted_passed!, checks: task.accepted_checks!, duplicate: true };
  if (task.state !== 'submitted')
    throw new DomainError('NOT_SUBMITTED', 'A tarefa ainda não está pronta para validação.');
  const run = { id: task.run_id, fence: task.run_fence };
  const artifacts = task.artifacts;
  const checks: Array<{
    id: string;
    label: string;
    type: string;
    passed: boolean;
    detail: string;
    evidence?: ReturnType<typeof verifyRepairSourceProof> & { patchArtifactId: string; reportArtifactId: string };
  }> = [];
  if(ci.required) checks.push({id:'registered-ci',label:'CI registrado da candidata',type:'registered_ci',
    passed:ci.state==='passed',detail:ci.detail});
  const {sourceOutputValidation}=await import('./source-output-validation.js');
  const sourceOutput=await sourceOutputValidation(task,readArtifact);
  if(sourceOutput)checks.push(sourceOutput);
  for (const c of task.criteria as Criterion[]) {
    let passed = false,
      detail = '';
    let evidence: typeof checks[number]['evidence'];
    if (c.type === 'review') {
      passed = !!reviewer;
      detail = reviewer ? 'Revisão atestada por ' + reviewer : 'Aguardando revisão do critério.';
    } else if (c.type === 'repair_source_proof') {
      try {
        if (c.version !== 1 || c.name !== 'repair-report.json' || c.patch !== 'implementation.patch')
          throw new Error('REPAIR_SOURCE_PROOF_VERSION');
        const patchRefs = artifacts.filter(a => a.name === c.patch), reportRefs = artifacts.filter(a => a.name === c.name);
        if (patchRefs.length !== 1 || reportRefs.length !== 1) throw new Error('REPAIR_SOURCE_PROOF_ARTIFACT_IDENTITY');
        const [patch, report] = await Promise.all([readArtifact(patchRefs[0].id), readArtifact(reportRefs[0].id)]);
        for (const [artifact, ref] of [[patch, patchRefs[0]], [report, reportRefs[0]]])
          if (artifact.run_id !== run.id || artifact.digest !== ref.digest || artifact.bytes.length !== ref.size)
            throw new Error('REPAIR_SOURCE_PROOF_ARTIFACT_IDENTITY');
        evidence = { ...verifyRepairSourceProof(patch.bytes, report.bytes), patchArtifactId: patch.id, reportArtifactId: report.id };
        passed = true;
        detail = 'Estrutura do patch e cobertura dos hashes conferidas. Aplicabilidade e QA permanecem pendentes.';
      } catch (error: any) {
        const code = /^REPAIR_SOURCE_PROOF_[A-Z_]+$/.test(error?.code ?? error?.message ?? '')
          ? error.code ?? error.message : 'REPAIR_SOURCE_PROOF_ARTIFACT_INVALID';
        detail = code + ': entregar patch íntegro e sourceHashes válidos para todos os arquivos; não inventar hashes ou dispensar integração/QA.';
      }
    } else {
      const a = artifacts.find((a) => a.name === c.name);
      if (a) {
        try {
          const { bytes } = await readArtifact(a.id);
          passed =
            c.type === 'artifact' ||
            (Object.hasOwn(JSON.parse(bytes.toString()), c.field) &&
              JSON.parse(bytes.toString())[c.field] === c.equals);
          detail = passed
            ? 'Artefato íntegro; contrato atendido.'
            : 'Conteúdo não atende ao contrato.';
        } catch {
          detail = 'Artefato inválido ou corrompido.';
        }
      } else detail = 'Artefato obrigatório ausente.';
    }
    checks.push({ id: c.id, label: c.label, type: c.type, passed, detail, ...(evidence ? { evidence } : {}) });
  }
  const passed = checks.every((c) => c.passed);
  return tx(async (db) => {
    await lock(db);
    await db.query('SELECT id FROM tasks WHERE id=$1 FOR UPDATE', [taskId]);
    const fresh = await readValidationSnapshot(db, taskId, reviewer);
    if (!fresh || fresh.validation_key !== task.validation_key)
      throw new DomainError('STATE_CHANGED', 'A tentativa, contrato ou evidência mudou durante a validação.');
    if(proof && validationDigest(await workerValidationCommitIdentity(db,fresh.run_id))!==validationDigest(proof.identity))
      throw new DomainError('STATE_CHANGED','A autoridade ou evidência de CI mudou durante a validação.');
    // Another validator may have completed while this call was reading bytes.
    if (fresh.receipt_match)
      return { passed: fresh.accepted_passed!, checks: fresh.accepted_checks!, duplicate: true };
    if (fresh.acceptance_id !== task.acceptance_id)
      throw new DomainError('STATE_CHANGED', 'Uma nova decisão foi registrada durante a validação.');
    if (fresh.state !== 'submitted')
      throw new DomainError('STATE_CHANGED', 'Estado mudou durante a validação.');
    const acceptanceId = randomUUID();
    await db.query(
      'INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) VALUES($1,$2,$3,$4,$5,$6)',
      [
        acceptanceId,
        taskId,
        run.id,
        passed,
        JSON.stringify(checks),
        reviewer ?? 'deterministic-contract',
      ],
    );
    await db.query(`INSERT INTO settings(id,value) VALUES($1,$2)
      ON CONFLICT(id) DO UPDATE SET value=EXCLUDED.value`, [validationReceiptId(fresh),
      JSON.stringify({ input: fresh.validation_input, acceptanceId, passed })]);
    if (passed)
      await db.query("UPDATE tasks SET state='accepted',reason=NULL,updated_at=now() WHERE id=$1", [
        taskId,
      ]);
    else
      await db.query('UPDATE tasks SET reason=$2 WHERE id=$1', [
        taskId,
        checks
          .filter((c) => !c.passed)
          .map((c) => c.detail)
          .join(' '),
      ]);
    await event(db, passed ? 'TASK_ACCEPTED' : 'VALIDATION_PENDING', taskId, {
      runId: run.id,
      checks,
      validationKey: fresh.validation_key,
      acceptanceId,
    });
    if(!passed && ci.required && ci.state==='failed' &&
      (await db.query('SELECT 1 FROM objectives WHERE id=$1',[task.goal_id])).rowCount) {
      const {queueFeedback}=await import('./planner.js');
      await queueFeedback(db,task.goal_id,'registered-ci:'+task.run_id+':'+fresh.validation_key,
        'Registered candidate CI did not satisfy acceptance: '+ci.detail+
        '. Correct the specific failure and preserve the original criteria. '+
        (ci.actionId?'Canonical CI action '+ci.actionId+' supplies immutable receipt/log inputs to the repair.':'No registered CI was dispatched; correct the declared intent/policy handoff.'),[taskId]);
    }
    if(!passed&&sourceContractPlanningFailure(checks,run.id))
      await routeVerificationPlanningRecovery(db,taskId,run.id);
    await refreshGoal(db, task.goal_id);
    if (passed) {
      await db.query(
        "UPDATE recoveries SET status='resolved' WHERE target_task_id=$1 AND status='scheduled'",
        [taskId],
      );
      const goal = (await db.query('SELECT project FROM goals WHERE id=$1', [task.goal_id]))
        .rows[0];
      await saveMemory(
        {
          project: goal.project,
          title: 'Entrega verificada: ' + task.title,
          content: JSON.stringify({
            taskId,
            runId: run.id,
            checks,
            artifacts: artifacts.map((a) => ({ id: a.id, sha256: a.digest, name: a.name })),
          }),
          kind: 'evidence',
          verification: 'confirmed',
          source: 'acceptance:' + run.id,
        },
        db,
      );
    }
    return { passed, checks, duplicate: false };
  });
}
export async function failRun(
  id: string,
  fence: string,
  code: string,
  message: string,
  retryable = false,
) {
  return tx(async (db) => {
    await lock(db);
    const r = (
      await db.query(
        "UPDATE runs SET state='failed',error_code=$3,summary=$4,finished_at=now() WHERE id=$1 AND fence=$2 AND state IN('running','queued') RETURNING *",
        [id, fence, code, message],
      )
    ).rows[0];
    if (!r) return;
    if(code==='DISK_CAPACITY'){
      const {recordStorageFailure}=await import('./storage-recovery.js');
      await recordStorageFailure(db,r.id);
    }
    if(code==='QUOTA_EXCEEDED' && r.account_id && r.model)
      await db.query(`UPDATE accounts SET execution_feedback=jsonb_set(execution_feedback,ARRAY[$2]::text[],$3::jsonb)
        WHERE id=$1`,[r.account_id,r.model,JSON.stringify({code,sourceRunId:r.id,
          blockedUntil:new Date(Date.now()+30*60000).toISOString()})]);
    const t = (await db.query('SELECT * FROM tasks WHERE id=$1', [r.task_id])).rows[0];
    const retry = retryable && t.attempt_count < t.max_attempts;
    await db.query(
      "UPDATE tasks SET state=$2,reason=$3,next_attempt_at=CASE WHEN $4 THEN now()+interval '3 seconds' ELSE NULL END,updated_at=now() WHERE id=$1",
      [t.id, retry ? 'retry_scheduled' : 'failed', message, retry],
    );
    await db.query('DELETE FROM leases WHERE run_id=$1', [id]);
    await event(db, retry ? 'TASK_RETRY_SCHEDULED' : 'RUN_FAILED', t.id, { runId: id, code });
    await refreshGoal(db, t.goal_id);
  });
}
export async function cancelTask(id: string) {
  return tx(async (db) => {
    await lock(db);
    const t = (await db.query('SELECT * FROM tasks WHERE id=$1', [id])).rows[0];
    if (!t) throw new DomainError('NOT_FOUND', 'Tarefa não encontrada.', 404);
    if (['accepted', 'cancelled', 'failed'].includes(t.state)) return t;
    const r = (
      await db.query(
        "SELECT * FROM runs WHERE task_id=$1 AND state IN('running','queued','cancelling','reconciling')",
        [id],
      )
    ).rows[0];
    if (r && r.state !== 'queued') {
      await db.query(
        "UPDATE runs SET state=CASE WHEN state='reconciling' THEN state ELSE 'cancelling' END WHERE id=$1",
        [r.id],
      );
      await db.query(
        "UPDATE tasks SET state='cancelling',reason='Cancelamento solicitado; aguardando confirmação do executor.' WHERE id=$1",
        [id],
      );
    } else {
      if (r) {
        await db.query("UPDATE runs SET state='cancelled',finished_at=now() WHERE id=$1", [r.id]);
        await db.query('DELETE FROM leases WHERE run_id=$1', [r.id]);
      }
      await db.query("UPDATE tasks SET state='cancelled' WHERE id=$1", [id]);
    }
    await event(db, 'CANCEL_REQUESTED', id, { runId: r?.id });
    await refreshGoal(db, t.goal_id);
    return { state: r && r.state !== 'queued' ? 'cancelling' : 'cancelled' };
  });
}
export async function confirmStopped(id: string, fence: string) {
  await tx(async (db) => {
    await lock(db);
    const r = (
      await db.query(
        "UPDATE runs SET state='cancelled',finished_at=now() WHERE id=$1 AND fence=$2 AND state='cancelling' RETURNING *",
        [id, fence],
      )
    ).rows[0];
    if (!r) return;
    const t = (
      await db.query(
        "UPDATE tasks SET state='cancelled',reason=NULL,updated_at=now() WHERE id=$1 RETURNING goal_id",
        [r.task_id],
      )
    ).rows[0];
    await db.query('DELETE FROM leases WHERE run_id=$1', [id]);
    await event(db, 'RUN_CANCELLED', r.task_id, { runId: id });
    await refreshGoal(db, t.goal_id);
  });
}

export async function delegateTasks(goalId: string, raw: unknown) {
  const tasks = taskSchema.array().min(1).max(100).parse(raw);
  return tx(async (db) => {
    await lock(db);
    const goal = (await db.query('SELECT * FROM goals WHERE id=$1', [goalId])).rows[0];
    if (!goal || !['active', 'paused'].includes(goal.state))
      throw new DomainError('GOAL_NOT_ACTIVE', 'O objetivo precisa estar ativo ou pausado.');
    const existing = (await db.query('SELECT * FROM tasks WHERE goal_id=$1', [goalId])).rows;
    if (existing.length + tasks.length > 250)
      throw new DomainError('PLAN_TOO_LARGE', 'Separe a operação em objetivos de até 250 tarefas.');
    const dependencies = (
      await db.query(
        'SELECT d.task_id,t.key FROM dependencies d JOIN tasks t ON t.id=d.depends_on WHERE t.goal_id=$1',
        [goalId],
      )
    ).rows;
    validateDag([
      ...existing.map((t) => ({
        ...t,
        recovery: t.recovery ?? undefined,
        dependencies: dependencies.filter((d) => d.task_id === t.id).map((d) => d.key),
      })),
      ...tasks,
    ]);
    const ids = new Map(existing.map((t) => [t.key, t.id]));
    for (const t of tasks) ids.set(t.key, randomUUID());
    for (const t of tasks) {
      const agent = (await db.query('SELECT * FROM agents WHERE id=$1', [t.agentId])).rows[0];
      if (!agent || (agent.runtime === 'builtin' && !goal.is_demo))
        throw new DomainError('AGENT_INVALID', 'Agente indisponível para esta operação.');
      if (
        t.accountId &&
        !(await db.query('SELECT id FROM accounts WHERE id=$1', [t.accountId])).rowCount
      )
        throw new DomainError('ACCOUNT_MISSING', 'Conta não encontrada.');
      await db.query(
        `INSERT INTO tasks(id,goal_id,key,title,description,agent_id,account_id,state,priority,resources,criteria,required_memories,max_attempts) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
        [
          ids.get(t.key),
          goalId,
          t.key,
          t.title,
          t.description,
          t.agentId,
          t.accountId ?? null,
          t.dependencies.length ? 'blocked_dependencies' : 'ready',
          t.priority,
          JSON.stringify(t.resources),
          JSON.stringify(t.criteria),
          JSON.stringify(t.requiredMemories),
          t.maxAttempts,
        ],
      );
    }
    for (const t of tasks)
      if (t.recovery)
        await db.query('UPDATE tasks SET recovery=$2 WHERE id=$1', [
          ids.get(t.key),
          JSON.stringify(t.recovery),
        ]);
    for (const t of tasks)
      if (t.model)
        await db.query('UPDATE tasks SET model=$2 WHERE id=$1', [ids.get(t.key), t.model]);
    for (const t of tasks)
      if (t.execution)
        await db.query('UPDATE tasks SET execution=$2,resources=$3 WHERE id=$1', [
          ids.get(t.key),
          JSON.stringify(t.execution),
          JSON.stringify([
            ...new Set([
              ...t.resources,
              'host:agentos-remote',
              'workspace:' + t.execution.workspace,
            ]),
          ]),
        ]);
    for (const t of tasks)
      for (const dep of t.dependencies)
        await db.query('INSERT INTO dependencies VALUES($1,$2)', [ids.get(t.key), ids.get(dep)]);
    await event(db, 'TASKS_DELEGATED', goalId, { keys: tasks.map((t) => t.key) });
    return { tasks: tasks.map((t) => ({ key: t.key, id: ids.get(t.key) })) };
  });
}
export async function retryTask(
  id: string,
  checkpoint: string,
  route: { accountId?: string | null; model?: string } = {},
) {
  return tx(async (db) => {
    await lock(db);
    const t = (await db.query('SELECT * FROM tasks WHERE id=$1', [id])).rows[0];
    if (!t || !['failed', 'submitted'].includes(t.state))
      throw new DomainError(
        'RETRY_STATE',
        'A tentativa precisa estar encerrada antes da retomada.',
      );
    if (t.attempt_count >= t.max_attempts)
      throw new DomainError('RETRY_LIMIT', 'Tentativas esgotadas. Revise e decomponha o plano.');
    if (route.model !== undefined || route.accountId !== undefined) {
      const accountId = route.accountId === undefined ? t.account_id : route.accountId;
      const model = route.model ?? t.model;
      if (accountId) {
        const a = (await db.query('SELECT * FROM accounts WHERE id=$1', [accountId])).rows[0];
        if (!a || (a.bridge_id && model && !a.models.includes(model)))
          throw new DomainError(
            'ROUTE_INVALID',
            'Conta ou modelo indisponível para esta retomada.',
          );
      }
      await db.query('UPDATE tasks SET account_id=$2,model=$3 WHERE id=$1', [id, accountId, model]);
      await event(db, 'TASK_ROUTE_CHANGED', id, {
        previousAccountId: t.account_id,
        previousModel: t.model,
        accountId,
        model,
      });
    }
    await db.query(
      "UPDATE tasks SET state='retry_scheduled',reason=NULL,next_attempt_at=now() WHERE id=$1",
      [id],
    );
    await db.query('UPDATE runs SET checkpoint=$2 WHERE task_id=$1 AND attempt=$3', [
      id,
      JSON.stringify({ summary: checkpoint, effectsReviewed: true }),
      t.attempt_count,
    ]);
    await db.query(
      `WITH RECURSIVE descendants(id) AS(SELECT task_id FROM dependencies WHERE depends_on=$1 UNION SELECT d.task_id FROM dependencies d JOIN descendants p ON d.depends_on=p.id) UPDATE tasks SET state='blocked_dependencies',reason=NULL WHERE id IN(SELECT id FROM descendants) AND state='blocked_failed'`,
      [id],
    );
    await db.query("UPDATE goals SET state='active' WHERE id=$1 AND state<>'paused'", [t.goal_id]);
    await event(db, 'RETRY_AUTHORIZED', id, { checkpoint });
    return { scheduled: true };
  });
}
export async function snapshot() {
  const [goals, tasks, runs, agents, accounts, events, memories, contexts, artifacts, settings] =
    await Promise.all(
      [
        'goals',
        'tasks',
        'runs',
        'agents',
        'accounts',
        'events',
        'memories',
        'contexts',
        'artifacts',
        'settings',
      ].map((table) =>
        pool.query(
          (table === 'runs'
            ? 'SELECT id,task_id,attempt,state,worker_id,account_id,runtime,agent_version,created_at,started_at,finished_at,heartbeat_at,context_id,summary,error_code,usage,checkpoint,model FROM '
            : 'SELECT * FROM ') +
            table +
            (table === 'events'
              ? ' ORDER BY id DESC LIMIT 100'
              : table === 'contexts'
                ? ' ORDER BY created_at DESC LIMIT 100'
                : ''),
        ),
      ),
    );
  return {
    goals: goals.rows,
    tasks: tasks.rows.filter((t) => t.state !== 'superseded').map(taskReadModel),
    runs: runs.rows.map(({ fence, instruction_snapshot, ...r }) => r),
    agents: agents.rows,
    accounts: accounts.rows,
    events: events.rows,
    memories: memories.rows,
    contexts: contexts.rows,
    artifacts: artifacts.rows,
    settings: Object.fromEntries(settings.rows.map((s) => [s.id, s.value])),
    integrations: {
      harness: !!config.dshCommand || (config.remoteExecution && !!config.bridgeHost),
      laya: !!config.layaUrl || !!config.bridgeHost,
      recall: (!!config.recallUrl && !!config.recallKey) || !!config.bridgeHost,
    },
    serverTime: new Date().toISOString(),
  };
}
function taskReadModel(task: any) {
  return {
    ...task,
    execution: task.execution?.inputs
      ? { ...task.execution, inputs: task.execution.inputs.map(({ base64, ...ref }: any) => ref) }
      : task.execution,
  };
}
export async function goalDetail(id: string) {
  const goal = (await pool.query('SELECT * FROM goals WHERE id=$1', [id])).rows[0];
  if (!goal) throw new DomainError('NOT_FOUND', 'Objetivo não encontrado.', 404);
  const tasks = (
    await pool.query('SELECT * FROM tasks WHERE goal_id=$1 ORDER BY created_at,key', [id])
  ).rows.map(taskReadModel);
  const dependencies = (
    await pool.query(
      'SELECT d.* FROM dependencies d JOIN tasks t ON t.id=d.task_id WHERE t.goal_id=$1',
      [id],
    )
  ).rows;
  const runs = (
    await pool.query(
      `SELECT r.id,r.task_id,r.attempt,r.state,r.worker_id,r.account_id,r.runtime,r.agent_version,
       r.parent_task_id,r.agent_role,r.agent_level,
       r.created_at,r.started_at,r.finished_at,r.heartbeat_at,r.context_id,r.summary,r.error_code,r.usage,r.checkpoint,r.model,
       r.instruction_snapshot->'task'->'criteria' accepted_criteria
       FROM runs r JOIN tasks t ON t.id=r.task_id WHERE t.goal_id=$1 ORDER BY r.created_at DESC`,
      [id],
    )
  ).rows;
  const acceptances = (
    await pool.query(
      'SELECT a.* FROM acceptances a JOIN tasks t ON t.id=a.task_id WHERE t.goal_id=$1 ORDER BY a.created_at DESC',
      [id],
    )
  ).rows;
  const objective =
    (await pool.query('SELECT * FROM objectives WHERE id=$1', [id])).rows[0] ?? null;
  const frontierArtifacts=(await pool.query(`SELECT a.run_id,a.name,a.digest FROM artifacts a
    JOIN runs r ON r.id=a.run_id JOIN tasks t ON t.id=r.task_id
    WHERE t.goal_id=$1 AND r.attempt=t.attempt_count`,[id])).rows;
  const revisions = objective
    ? (
        await pool.query(
          'SELECT * FROM plan_revisions WHERE objective_id=$1 ORDER BY revision DESC',
          [id],
        )
      ).rows
    : [];
  const planning = objective
    ? (
        await pool.query(
          'SELECT * FROM planning_runs WHERE objective_id=$1 ORDER BY created_at DESC',
          [id],
        )
      ).rows
    : [];
  return {
    goal,
    tasks: tasks.filter((t) => t.state !== 'superseded'),
    retiredTasks: tasks.filter((t) => t.state === 'superseded'),
    dependencies,
    runs:runs.map(({accepted_criteria,...run})=>run),
    acceptances,
    objective,
    revisions,
    planning,
    frontier:projectWorkFrontier({kind:'objective',id,version:revisions[0]?.revision},
      objectiveFrontierNodes(tasks,runs,acceptances,dependencies,frontierArtifacts),revisions[0]?.plan.requirements ?? []),
    planningAttempts: objective
      ? (
          await pool.query(
            'SELECT a.attempt,a.error,a.usage,a.created_at,a.planning_run_id,(a.output IS NOT NULL) has_output FROM planning_attempts a JOIN planning_runs p ON p.id=a.planning_run_id WHERE p.objective_id=$1 ORDER BY a.created_at DESC',
            [id],
          )
        ).rows
      : [],
  };
}
