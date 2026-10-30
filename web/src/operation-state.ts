import { currentRun, pendingExecutionDependencies } from './task-attention';
import { modelActivity } from './execution-status';
import {humanRecoveryRequest} from '../../shared/recovery';
type Row = Record<string, any>;
const failed = new Set(['failed', 'recovery_blocked', 'reconciling', 'recovering_artifacts']);
export function supervisionHealthy(data: Row, now = Date.now()) {
  const h = data.settings?.['continuation-supervisor'];
  return h?.state === 'running' && now - Date.parse(h.checkedAt) < 90000;
}
export function taskFlow(task: Row, data: Row, now = Date.now()) {
  const run = currentRun(task, data.runs),
    c = task.execution?.continuation;
  const fresh = supervisionHealthy(data, now);
  const matching =
    c &&
    (c.runId === run?.id ||
      ['retry_scheduled', 'waiting_resource', 'waiting_quota'].includes(task.state));
  if (task.state === 'accepted')
    return { kind: 'done', label: 'Concluída', detail: 'Etapa aceita', run, action: false };
  if (task.state === 'running') {
    const model = modelActivity(run, data.events);
    const recovering = run?.checkpoint?.kind && /recovery/.test(run.checkpoint.kind);
    return {
      kind: 'running',
      label: recovering
        ? 'Corrigindo'
        : model?.pending && model.latestKind !== 'MUSE_TOOL_EVENT'
          ? 'Aguardando modelo'
          : 'Executando',
      detail: 'Execução iniciada',
      run,
      action: false,
    };
  }
  const prerequisites = pendingExecutionDependencies(task, data);
  if (task.state === 'recovery_blocked' && matching && c.state === 'waiting_evidence')
    return { kind: 'waiting', label: 'Aguardando insumos', detail: c.message, run, action: false };
  if (prerequisites.length)
    return {
      kind: 'waiting',
      label: 'Aguardando etapas',
      detail: prerequisites.map((t: Row) => t.title ?? t.id).join('; '),
      run,
      action: false,
    };
  if (matching && c.state === 'needs_action' && humanRecoveryRequest(c,run?.error_code))
    return { kind: 'action', label: 'Precisa de você', detail: c.message, run, action: true };
  if (providerAvailabilityFailureForRun(run) || task.state==='waiting_quota')
    return {kind:'waiting',label:'Aguardando rota com cota',detail:'O sistema busca uma rota autorizada ou aguarda disponibilidade.',run,action:false};
  if (matching && c.state==='needs_action')
    return {kind:'stalled',label:'Encaminhamento técnico pendente',detail:c.message,run,action:false};
  if (matching && c.state === 'supervising' && fresh)
    return { kind: 'repair', label: 'Em supervisão', detail: c.message, run, action: false };
  if (failed.has(task.state)) {
    if (matching && fresh)
      return {
        kind: 'repair',
        label:
          c.state === 'collecting'
            ? 'Recuperando arquivos'
            : c.state === 'waiting_slot'
              ? 'Correção na fila'
              : c.state === 'waiting_quota'
                ? 'Aguardando cota'
                : 'Preparando correção',
        detail: c.message,
        run,
        action: false,
      };
    return {
      kind: 'stalled',
      label: 'Recuperação pendente',
      detail: 'Sem confirmação de retomada automática',
      run,
      action: false,
    };
  }
  if (['retry_scheduled', 'waiting_resource', 'queued'].includes(task.state) && matching)
    return {
      kind: 'repair',
      label: 'Correção na fila',
      detail: 'Retoma quando houver vaga para este jogo',
      run,
      action: false,
    };
  if (task.state === 'waiting_quota')
    return {
      kind: 'waiting',
      label: 'Aguardando cota',
      detail: 'O agendador procura uma rota disponível',
      run,
      action: false,
    };
  if (task.state === 'waiting_configuration')
    return { kind: 'stalled', label: 'Configuração do executor pendente', detail: task.reason, run, action: false };
  if (task.state === 'submitted')
    return {
      kind: 'repair',
      label: 'Validando entrega',
      detail: 'Conferindo os critérios da etapa',
      run,
      action: false,
    };
  return {
    kind: 'waiting',
    label: 'Na fila',
    detail: 'Espera a etapa anterior ou uma vaga',
    run,
    action: false,
  };
}
function providerAvailabilityFailureForRun(run:any) {
  return run?.state==='failed' && /^(QUOTA_EXCEEDED|RATE_LIMIT|RATE_LIMITED)$/.test(run.error_code??'');
}
export function operationGames(tasks: Row[], data: Row, now = Date.now()) {
  const map = new Map<string, Row[]>();
  for (const t of tasks) {
    if (['cancelled', 'superseded'].includes(t.state)) continue;
    if (
      t.stage === 'coordinate' ||
      (t.stage === 'review' && !t.execution?.baseWorkspace && !t.execution?.workspace)
    )
      continue;
    const key = t.execution?.baseWorkspace ?? t.execution?.workspace ?? t.id;
    map.set(key, [...(map.get(key) ?? []), t]);
  }
  return [...map].map(([id, list]) => {
    const title = id.includes('/')
      ? id
          .split('/')
          .at(-1)!
          .replaceAll('-', ' ')
          .replace(/\b\w/g, (c) => c.toUpperCase())
      : list[0].title;
    const flows = list.map((task) => ({ task, ...taskFlow(task, data, now) }));
    const rank: Record<string, number> = {
      running: 0,
      action: 1,
      repair: 2,
      stalled: 3,
      waiting: 4,
      done: 5,
    };
    const focus = [...flows].sort((a, b) => rank[a.kind] - rank[b.kind])[0];
    const done = flows.filter((f) => f.kind === 'done').length;
    return {
      id,
      title,
      tasks: list,
      flows,
      focus,
      done,
      total: list.length,
      delivery: operationDelivery(list),
      complete: done === list.length,
      actions: flows.filter((f) => f.action),
    };
  });
}

/** Supplementary work is evidence, not a delivered, independently verified product. */
export function operationDelivery(tasks: Row[]) {
  const current = tasks.filter((task) => !['cancelled', 'superseded'].includes(task.state));
  const integration = current.filter((task) => task.stage === 'integrate');
  const verification = current.filter((task) => task.stage === 'verify');
  const diagnostics = current.filter((task) => !['integrate', 'verify'].includes(task.stage));
  return {
    gates: [
      {
        id: 'integrate',
        label: 'Integração',
        tasks: integration,
        complete: integration.length > 0 && integration.every((task) => task.state === 'accepted'),
      },
      {
        id: 'verify',
        label: 'QA final',
        tasks: verification,
        complete:
          verification.length > 0 && verification.every((task) => task.state === 'accepted'),
      },
    ],
    diagnostics: diagnostics.length,
    acceptedDiagnostics: diagnostics.filter((task) => task.state === 'accepted').length,
  };
}

export function operationWorkforce(goalId: string, tasks: Row[], data: Row) {
  const policy = data.settings?.['workforce:' + goalId];
  if (!policy?.enabled) return null;
  const glmRequestLimit = Number(policy.glmRequestLimit);
  const taskMap = new Map(tasks.map((t) => [t.id, t]));
  const groups = [
    { id: 'coordinator', label: 'Sol · coordenação', target: 1 },
    { id: 'glm', label: 'GLM · OpenCode', target: Number(policy.glmTarget) || 0 },
    { id: 'muse', label: 'Muse', target: Number(policy.museTarget) || 0 },
    { id: 'luna', label: 'GPT-6 Luna · Codex', target: Number(policy.lunaTarget) || 0 },
    { id: 'kimi', label: 'Kimi', target: null },
    { id: 'qwen', label: 'Qwen', target: null },
    { id: 'agy', label: 'AGY', target: null },
    { id: 'sol-worker', label: 'Sol · execução', target: null },
    { id: 'other', label: 'Outros modelos', target: null },
  ].map((g) => ({
    ...g,
    requestLimit:
      g.id === 'glm' && Number.isInteger(glmRequestLimit) && glmRequestLimit > 0
        ? glmRequestLimit
        : null,
    running: 0,
    queued: 0,
    confirming: 0,
  }));
  const live = (data.runs ?? [])
    .filter(
      (r: Row) =>
        taskMap.has(r.task_id) &&
        ['queued', 'running', 'cancelling', 'reconciling'].includes(r.state),
    )
    .map((run: Row) => {
      const task = taskMap.get(run.task_id)!;
      const account = (data.accounts ?? []).find((a: Row) => a.id === run.account_id);
      const model = String(run.model ?? '').toLowerCase();
      const provider = String(account?.provider ?? run.provider ?? '').toLowerCase();
      const native = run.runtime === 'opencode' || /^glm[-/]/.test(model);
      const sol = /(?:^|\/)gpt-6(?:\.1)?-sol(?:$|[-/])/.test(model);
      const groupId =
        task.stage === 'coordinate' && sol
          ? 'coordinator'
          : native
            ? 'glm'
            : task.execution?.workforceLane === 'muse' ||
                provider === 'muse' ||
                /^muse[-/]/.test(model)
              ? 'muse'
              : provider === 'agy' || provider === 'antigravity'
                ? 'agy'
                : provider === 'kimi' || provider === 'k3' || /^k3(?:$|[-/])|kimi/.test(model)
                  ? 'kimi'
                  : provider === 'qwen' || /qwen/.test(model)
                    ? 'qwen'
                    : /(?:^|\/)gpt-6-luna(?:$|[-/])/.test(model)
                      ? 'luna'
                      : sol
                        ? 'sol-worker'
                        : 'other';
      const group = groups.find((g) => g.id === groupId)!;
      const state =
        run.state === 'running' ? 'running' : run.state === 'queued' ? 'queued' : 'confirming';
      group[state]++;
      return { run, task, account, group: group.label };
    });
  return {
    groups: groups.filter(
      (g) =>
        !['sol-worker', 'other', 'luna'].includes(g.id) ||
        (g.id === 'luna' && Number(g.target) > 0) ||
        g.running + g.queued + g.confirming > 0,
    ),
    live,
    running: live.filter(({ run }: { run: Row }) => run.state === 'running').length,
    queued: live.filter(({ run }: { run: Row }) => run.state === 'queued').length,
    confirming: live.filter(({ run }: { run: Row }) =>
      ['cancelling', 'reconciling'].includes(run.state),
    ).length,
  };
}

export function operationProductivityLine(detail: Row, data: Row) {
  const tasks: Row[] = (detail.tasks ?? []).filter((t: Row) => !['cancelled', 'superseded'].includes(t.state));
  const ready = tasks.filter((t: Row) => ['ready', 'queued'].includes(t.state)).length;
  const activeSessions = tasks.filter((t: Row) => ['running', 'queued', 'cancelling', 'reconciling'].includes(t.state)).length;
  const poolHealth = data.settings?.['workforce-health:' + detail.goal?.id];
  const runs: Row[] = detail.runs ?? data.runs ?? [];
  const runningRuns = runs.filter((r: Row) => r.state === 'running');
  // The GLM gate is global. Only a real gate reading may fill this number;
  // running runs of this objective are not model calls and must never be
  // saturated into a plausible-looking count.
  const gate = data.settings?.['glm-gate'];
  const glmCalls: number | null = Number.isInteger(gate?.effectiveActive)
    ? gate.effectiveActive
    : Number.isInteger(gate?.active)
      ? gate.active
      : Number.isInteger(poolHealth?.glm?.activeRequests)
        ? poolHealth.glm.activeRequests
        : null;
  const glmOverLimit = glmCalls !== null && glmCalls > 6;
  const taskIds = new Set(tasks.map((t: Row) => t.id));
  const objectiveModelCalls =
    runningRuns.filter(
      (r: Row) => taskIds.has(r.task_id) && modelActivity(r, data.events)?.pending,
    ).length + ((detail.planning ?? []).some((p: Row) => p.state === 'running') ? 1 : 0);
  const revisions: Row[] = detail.revisions ?? [];
  const incrementalRevisions = revisions.filter((r: Row) => r.plan?.incremental);
  let acceptedDeliveries: number;
  if (incrementalRevisions.length) {
    const done = new Set<string>();
    for (const r of incrementalRevisions)
      for (const id of r.plan.incremental.completedRequirementIds ?? []) done.add(id);
    if (detail.goal?.state === 'completed') {
      const latest = revisions[0]?.plan?.incremental;
      for (const id of latest?.selectedRequirementIds ?? []) done.add(id);
      if (latest?.selectedRequirementId) done.add(latest.selectedRequirementId);
    }
    acceptedDeliveries = done.size;
  } else if (Array.isArray(detail.deliveryFlow?.accepted)) {
    acceptedDeliveries = detail.deliveryFlow.accepted.length;
  } else {
    acceptedDeliveries = tasks.filter(
      (t: Row) => t.stage === 'verify' && t.state === 'accepted',
    ).length;
  }
  let blockReason = 'nenhum';
  if (ready === 0 && !['completed', 'cancelled'].includes(detail.goal?.state)) {
    const storageBlocked = data.settings?.['storage:agentos-workspaces']?.state === 'blocked' ||
      tasks.some((t: Row) => /DISK_CAPACITY/i.test(t.reason ?? ''));
    const reconciling = tasks.some((t: Row) => ['reconciling', 'recovering_artifacts'].includes(t.state)) ||
      runs.some((r: Row) => r.state === 'reconciling');
    const planningBlocked = ['planning', 'replanning'].includes(detail.goal?.state) ||
      ['planning', 'replanning', 'waiting_external'].includes(detail.objective?.status) ||
      ['running', 'queued', 'failed'].includes(detail.planning?.[0]?.state) ||
      tasks.some((t: Row) => t.state === 'recovery_blocked');
    const providerBlocked = tasks.some((t: Row) => t.state === 'waiting_quota') ||
      detail.planning?.[0]?.state === 'waiting_quota' ||
      runs.some((r: Row) => providerAvailabilityFailureForRun(r));
    const conflictBlocked = tasks.some((t: Row) => t.state === 'waiting_resource' || /CONFLICT|REBASE|PREIMAGE/i.test(t.reason ?? ''));
    const depBlocked =
      tasks.some((t: Row) => t.state === 'blocked_failed') ||
      (tasks.some((t: Row) => t.state === 'blocked_dependencies') &&
        !tasks.some((t: Row) => ['running', 'ready', 'queued'].includes(t.state)));
    const dispatchFailed = Boolean(data.engine?.error);
    if (storageBlocked) blockReason = 'armazenamento';
    else if (reconciling) blockReason = 'reconciliação';
    else if (planningBlocked) blockReason = 'planejamento';
    else if (providerBlocked) blockReason = 'provedor';
    else if (conflictBlocked) blockReason = 'conflito';
    else if (depBlocked) blockReason = 'dependências';
    else if (dispatchFailed) blockReason = 'falha de despacho';
  }
  return {
    ready,
    activeSessions,
    maxSessions: 12,
    glmCalls,
    glmOverLimit,
    objectiveModelCalls,
    maxGlmCalls: 6,
    acceptedDeliveries,
    blockReason,
    line: `trabalho pronto: ${ready} | sessões ativas: ${activeSessions}/12 | chamadas ao provedor: ${glmCalls === null ? '?/6' : `${glmCalls}/6${glmOverLimit ? ' (acima do limite!)' : ''}`} | entregas aceitas: ${acceptedDeliveries} | bloqueio: ${blockReason}`,
  };
}
