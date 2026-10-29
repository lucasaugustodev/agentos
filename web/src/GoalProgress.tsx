import React, { useEffect, useRef, useState } from 'react';
import {
  Activity,
  AlertTriangle,
  Check,
  ChevronRight,
  Clock,
  Cpu,
  Download,
  FileText,
} from 'lucide-react';
import './goals.css';
import { ExecutionDetails } from './ExecutionDetails';
import {
  affectedTasks,
  taskAttention,
  latestObservation,
  type TaskAttention,
} from './task-attention';
import { ObjectivePlan } from './ObjectivePlan';
import { OperationBoard } from './OperationBoard';
import { goalStanding } from './goal-standing.js';
import { taskFlow } from './operation-state';
import { runRoute } from './run-route';

type Row = Record<string, any>;
const activeStates = new Set(['running', 'cancelling', 'reconciling']);
const stateLabels: Record<string, string> = {
  running: 'Em execução',
  accepted: 'Aceita',
  submitted: 'Entrega sem aceite',
  queued: 'Na fila',
  ready: 'Pronta para despacho',
  blocked_dependencies: 'Espera uma etapa anterior',
  blocked_failed: 'Etapa anterior falhou',
  waiting_quota: 'Espera cota',
  waiting_resource: 'Espera recurso',
  waiting_configuration: 'Configuração pendente',
  retry_scheduled: 'Retomada agendada',
  reconciling: 'Execução sem confirmação',
  cancelling: 'Cancelamento em curso',
  cancelled: 'Cancelada',
  failed: 'Falhou',
  recovering_artifacts: 'Recuperando arquivos',
  recovery_blocked: 'Correção precisa de novo plano',
};
export const taskLabel = (state: string) => stateLabels[state] ?? state;
const date = (s: string) => new Date(s).toLocaleString('pt-BR');
const ago = (s?: string) => {
  if (!s) return 'sem registro';
  const n = Math.max(0, Math.floor((Date.now() - new Date(s).getTime()) / 1000));
  return n < 60
    ? `${n}s`
    : n < 3600
      ? `${Math.floor(n / 60)}min`
      : `${Math.floor(n / 3600)}h ${Math.floor((n % 3600) / 60)}min`;
};
const newest = (rows: Row[]) =>
  [...rows].sort((a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime());
const latestRun = (task: Row, runs: Row[]) => newest(runs.filter((r) => r.task_id === task.id))[0];
const latestAcceptance = (run: Row | undefined, acceptances: Row[]) =>
  run && newest(acceptances.filter((a) => a.run_id === run.id))[0];
function needsAttention(task: Row, data: Row) {
  return taskAttention(task, data) !== null;
}
function withDetail(data: Row, detail: Row) {
  return {
    ...data,
    tasks: [
      ...data.tasks.filter((t: Row) => !detail.tasks.some((d: Row) => d.id === t.id)),
      ...detail.tasks,
    ],
    runs: [
      ...data.runs.filter((r: Row) => !detail.runs.some((d: Row) => d.id === r.id)),
      ...detail.runs,
    ],
    dependencies: detail.dependencies,
    acceptances: detail.acceptances,
  };
}
function AttentionReason({
  alert,
  inspect,
}: {
  alert: TaskAttention;
  inspect?: (id: string) => void;
}) {
  return (
    <div className={'attention-reason ' + alert.kind}>
      <span className="attention-owner">{alert.owner}</span>
      <h4>{alert.title}</h4>
      <p>{alert.detail}</p>
      <p className="attention-next">
        <strong>Próximo passo: </strong>
        {alert.nextStep}
      </p>
      {alert.code && (
        <small>
          Código registrado: <code>{alert.code}</code>
        </small>
      )}
      {!!alert.causes.length && (
        <div className="attention-causes">
          {alert.causes.map((cause) =>
            inspect ? (
              <button className="text-button" key={cause.id} onClick={() => inspect(cause.id)}>
                Ver causa: {cause.title} <ChevronRight size={14} />
              </button>
            ) : (
              <p key={cause.id}>Causa: {cause.title}</p>
            ),
          )}
        </div>
      )}
    </div>
  );
}
const titleCase = (text: string) =>
  text.replaceAll('-', ' ').replace(/\b\w/g, (c) => c.toUpperCase());
const routeLabel = (task: Row, run: Row | undefined, accounts: Row[]) => {
  const account = accounts.find((a) => a.id === (run?.account_id ?? task.account_id));
  return `${run?.model ?? task.model ?? account?.model ?? 'Modelo a selecionar'} · ${account?.label ?? run?.account_id ?? task.account_id ?? 'Conta automática'}`;
};
function TaskStatus({ state }: { state: string }) {
  return (
    <span className={'task-status ' + state}>
      <i />
      {taskLabel(state)}
    </span>
  );
}
function ordered(tasks: Row[], dependencies: Row[]) {
  const depths = new Map<string, number>();
  const parentsByTask = new Map<string, string[]>();
  for (const dependency of dependencies) {
    const parents = parentsByTask.get(dependency.task_id) ?? [];
    parents.push(dependency.depends_on);
    parentsByTask.set(dependency.task_id, parents);
  }
  const depth = (id: string, seen = new Set<string>()): number => {
    if (depths.has(id)) return depths.get(id)!;
    if (seen.has(id)) return 0;
    seen.add(id);
    const parents = parentsByTask.get(id) ?? [];
    const result = parents.length
      ? 1 + Math.max(...parents.map((parent) => depth(parent, new Set(seen))))
      : 0;
    depths.set(id, result);
    return result;
  };
  return [...tasks].sort((a, b) => depth(a.id) - depth(b.id) || a.title.localeCompare(b.title));
}
function groupsFor(tasks: Row[], dependencies: Row[]) {
  const groups = new Map<string, { id: string; title: string; tasks: Row[] }>();
  for (const task of ordered(tasks, dependencies)) {
    const workspace = task.execution?.baseWorkspace ?? task.execution?.workspace;
    const id = workspace ?? task.id;
    if (!groups.has(id))
      groups.set(id, {
        id,
        title: workspace ? titleCase(workspace.split('/').at(-1)) : task.title,
        tasks: [],
      });
    groups.get(id)!.tasks.push(task);
  }
  return [...groups.values()];
}
function reasonFor(task: Row, tasks: Row[], dependencies: Row[], goal: Row) {
  const pending = dependencies
    .filter((d) => d.task_id === task.id)
    .map((d) => tasks.find((t) => t.id === d.depends_on))
    .filter((t) => t && t.state !== 'accepted');
  if (task.state === 'accepted') return 'Critérios desta etapa atendidos.';
  if (task.state === 'running') return 'O agente está executando esta tentativa.';
  if (task.state === 'reconciling')
    return 'O término do processo ainda não foi confirmado. Nova tentativa bloqueada para evitar duplicação.';
  if (task.state === 'submitted')
    return task.reason
      ? `Aceite pendente: ${task.reason}`
      : 'Resultado entregue; aguardando conferência dos critérios.';
  if (pending.length) return `Aguarda aceite de: ${pending.map((t) => t!.title).join('; ')}.`;
  if (
    goal.state === 'paused' &&
    !activeStates.has(task.state) &&
    !['cancelled', 'failed'].includes(task.state)
  )
    return 'Objetivo pausado: novos despachos suspensos.';
  if (task.state === 'queued') return 'Despachada para a fila; o executor ainda não iniciou.';
  if (task.state === 'waiting_resource') {
    const workstream = task.resources?.find((r: string) => r.startsWith('workstream:'));
    const owner =
      workstream &&
      tasks.find(
        (t) => t.id !== task.id && activeStates.has(t.state) && t.resources?.includes(workstream),
      );
    if (owner)
      return `Aguarda “${owner.title}”: uma execução por jogo. Esta etapa continua na fila.`;
  }
  if (task.state === 'retry_scheduled')
    return `${task.reason ?? 'Retomada automática.'} Próxima tentativa ${task.next_attempt_at ? date(task.next_attempt_at) : 'pelo agendador'}.`;
  return task.reason ?? taskLabel(task.state);
}
function eventText(event: Row) {
  const p = event.payload ?? {};
  const labels: Record<string, string> = {
    EXECUTION_OBSERVED: 'Atividade do executor consultada',
    MODEL_REQUEST: `Consultando o modelo${p.requests ? ` · chamada ${p.requests}` : ''}`,
    MODEL_RESPONSE: 'Modelo respondeu',
    MODEL_ERROR: 'Falha na resposta do modelo',
    HARNESS_TOOL_EVENT: p.type === 'tool/call' ? 'Ferramenta acionada' : 'Ferramenta retornou',
    HARNESS_STARTED: 'Harness iniciou',
    HARNESS_STOPPED: 'Harness encerrou',
    OPENCODE_TOOL_EVENT: `OpenCode · ${p.tool ?? 'ferramenta'}${p.detail ? `: ${p.detail}` : ''}`,
    OPENCODE_STOPPED: 'OpenCode encerrou com confirmação',
    MUSE_TOOL_EVENT: `Muse Code · ${p.operation ?? 'ferramenta'}`,
    MUSE_STOPPED: p.confirmed
      ? 'Muse Code encerrou com confirmação'
      : 'Encerramento do Muse Code sem confirmação',
    RUNTIME_STARTED: p.runtime === 'muse' ? 'Executor Muse Code iniciou' : 'Executor iniciou',
    RUN_RECONCILING: 'Encerramento do executor ainda não confirmado',
    RUN_STARTED: 'Tentativa iniciou',
    RUN_SUBMITTED: 'Resultado entregue',
    RUN_FAILED: 'Tentativa falhou',
    TASK_ACCEPTED: 'Etapa aceita',
    VALIDATION_PENDING: 'Critérios de aceite pendentes',
    RECOVERY_SCHEDULED: p.reason ?? 'Falha encaminhada para correção automática',
    RECOVERY_BLOCKED: p.reason ?? 'Correção precisa de novo plano',
    ARTIFACT_CREATED: 'Evidência preservada',
    TASK_ROUTE_CHANGED: 'Conta ou modelo alterado para a próxima tentativa',
    TASK_RETRY_SCHEDULED: 'Nova tentativa agendada',
    TASK_WAITING: p.reason ?? 'Tarefa em espera',
  };
  return labels[event.kind] ?? event.kind.replaceAll('_', ' ').toLocaleLowerCase('pt-BR');
}
function taskEvents(task: Row, run: Row | undefined, events: Row[]) {
  return newest(
    events.filter(
      (e) =>
        e.kind !== 'EXECUTION_OBSERVED' &&
        (run ? e.payload?.runId === run.id || e.entity_id === run.id : e.entity_id === task.id),
    ),
  );
}

export function QueueStatus({ data }: { data: Row }) {
  const [, tick] = useState(0);
  useEffect(() => {
    const id = setInterval(() => tick((n) => n + 1), 5000);
    return () => clearInterval(id);
  }, []);
  const fresh = Date.now() - new Date(data.serverTime).getTime() < 20000;
  const healthy =
    fresh &&
    data.engine?.lastTick &&
    !data.engine?.error &&
    Date.now() - new Date(data.engine.lastTick).getTime() < 15000;
  return (
    <div className={'queue-status ' + (healthy ? 'healthy' : 'stale')} role="status">
      {healthy ? <Activity size={17} /> : <AlertTriangle size={17} />}
      <div>
        <strong>{healthy ? 'Agendador ativo' : 'Agendador sem confirmação recente'}</strong>
        <span>Fila, banco e executores no servidor · acesso privado pela tailnet</span>
        <small>Fechar esta página não interrompe a operação.</small>
      </div>
      <span className="sync-time" title={date(data.serverTime)}>
        {fresh ? 'Atualizado há' : 'Dados desatualizados há'} {ago(data.serverTime)}
      </span>
    </div>
  );
}

export function GoalDirectory({
  goals,
  data,
  open,
}: {
  goals: Row[];
  data: Row;
  open: (id: string) => void;
}) {
  const ongoing = goals.filter((g) => !g.is_demo && !['completed', 'cancelled'].includes(g.state));
  const past = goals.filter((g) => !g.is_demo && ['completed', 'cancelled'].includes(g.state));
  const demos = goals.filter((g) => g.is_demo);
  const cards = (list: Row[]) => (
    <div className="operation-cards">
      {newest(list).map((goal) => {
        const tasks = data.tasks.filter((t: Row) => t.goal_id === goal.id);
        const active = tasks.filter((t: Row) => t.state === 'running');
        const attention = tasks.filter((t: Row) => needsAttention(t, data));
        const actions = tasks.filter((t: Row) => taskFlow(t, data).action);
        const repairs = tasks.filter((t: Row) => taskFlow(t, data).kind === 'repair');
        const accepted = goal.accepted_count ?? tasks.filter((t: Row) => t.state === 'accepted').length;
        const total = goal.task_count ?? tasks.length;
        const delivery = data.settings?.['operation-progress:' + goal.id]?.delivery;
        const hasDelivery = Number.isInteger(delivery?.total) && delivery.total > 0 &&
          Number.isInteger(delivery.verified) && delivery.verified >= 0 && delivery.verified <= delivery.total;
        const focus = attention[0] ?? active[0];
        const ready = tasks.filter((t: Row) => t.state === 'ready').length;
        const standing = goalStanding(goal, {
          active: active.length,
          attention: actions.length,
          ready,
        });
        return (
          <button
            className="operation-card"
            key={goal.id}
            onClick={() => open(goal.merged_into ?? goal.id)}
          >
            <div className="operation-top">
              <span>{goal.project}</span>
              <span className={'operation-standing ' + standing.tone}>
                {standing.label} <ChevronRight size={15} />
              </span>
            </div>
            <h3>{goal.title}</h3>
            <div className="operation-counts">
              <span>
                <Activity size={14} />
                {active.length} em execução
              </span>
              <span className={actions.length ? 'attention-text' : ''}>
                {actions.length} precisam de você
              </span>
              {!!repairs.length && <span>{repairs.length} em recuperação automática</span>}
              <span>
                {hasDelivery ? `${delivery.verified}/${delivery.total} entregas validadas` : `${accepted}/${total} etapas aceitas`}
              </span>
              {hasDelivery && <span>{delivery.integrated}/{delivery.total} integrações validadas</span>}
            </div>
            {!goal.merged_into &&
              active.map((task: Row) => {
                const run = latestRun(task, data.runs);
                return run ? (
                  <p className="operation-live-route" key={task.id}>
                    <Cpu size={13} />
                    {runRoute(run, data.accounts)} · {task.title}
                  </p>
                ) : null;
              })}
            <p>
              {/* A named step in flight is the most useful sentence; otherwise
                  say plainly what the objective is waiting on. */}
              {focus && !goal.merged_into
                ? `${taskLabel(focus.state)}: ${focus.title}`
                : standing.detail}
            </p>
            {!!actions.length && (
              <div className="operation-alerts">
                {actions.slice(0, 2).map((task: Row) => (
                  <span key={task.id}>
                    <AlertTriangle size={13} />
                    {task.title}: {taskFlow(task, data).label}
                  </span>
                ))}
                {actions.length > 2 && (
                  <small>Mais {actions.length - 2} itens · abrir detalhes</small>
                )}
              </div>
            )}
            <div className="progress-track" aria-label={hasDelivery ? 'Progresso das entregas finais' : 'Progresso das etapas'}>
              <i style={{ width: `${hasDelivery ? (100 * delivery.verified) / delivery.total : (100 * accepted) / Math.max(1, total)}%` }} />
            </div>
          </button>
        );
      })}
    </div>
  );
  return (
    <div className="goal-directory">
      <QueueStatus data={data} />
      <h2>
        Trabalho em andamento <small>{ongoing.length}</small>
      </h2>
      {ongoing.length ? (
        cards(ongoing)
      ) : (
        <p className="directory-empty">Nenhum objetivo em andamento neste filtro.</p>
      )}
      {!!past.length && (
        <details className="goal-archive">
          <summary>Histórico de objetivos · {past.length}</summary>
          {cards(past)}
        </details>
      )}
      {!!demos.length && (
        <details className="goal-archive">
          <summary>Exemplos técnicos · {demos.length}</summary>
          <p>Testes do framework; não contam como entregas dos jogos.</p>
          {cards(demos)}
        </details>
      )}
    </div>
  );
}

export function GoalOverview(props: { detail: Row; data: Row; inspect: (id: string) => void }) {
  const resolved = { ...props, data: withDetail(props.data, props.detail) };
  return <OperationBoard {...resolved} technical={() => <TechnicalGoalOverview {...resolved} />} />;
}
function TechnicalGoalOverview({
  detail,
  data,
  inspect,
}: {
  detail: Row;
  data: Row;
  inspect: (id: string) => void;
}) {
  const { goal, tasks, dependencies, runs, acceptances } = detail;
  data = withDetail(data, detail);
  const [filter, setFilter] = useState('all');
  const attentionPanel = useRef<HTMLElement>(null);
  const groups = groupsFor(tasks, dependencies);
  const active = tasks.filter((t: Row) => t.state === 'running');
  const attention = tasks
    .filter((t: Row) => needsAttention(t, data))
    .sort(
      (a: Row, b: Row) =>
        Number(taskAttention(a, data)?.kind !== 'blocked') -
          Number(taskAttention(b, data)?.kind !== 'blocked') ||
        Number(a.state === 'blocked_failed') - Number(b.state === 'blocked_failed') ||
        a.title.localeCompare(b.title),
    );
  const waiting = tasks.filter((t: Row) =>
    [
      'ready',
      'queued',
      'blocked_dependencies',
      'waiting_quota',
      'waiting_resource',
      'retry_scheduled',
    ].includes(t.state),
  );
  const done = groups.filter((g) => g.tasks.every((t) => t.state === 'accepted')).length;
  const focuses = ordered(
    tasks.filter(
      (t: Row) =>
        activeStates.has(t.state) || t.state === 'submitted' || t.state === 'retry_scheduled',
    ),
    dependencies,
  );
  const eligible = ordered(
    tasks.filter((t: Row) => !['accepted', 'cancelled'].includes(t.state)),
    dependencies,
  );
  const matchesFilter = (t: Row) =>
    filter === 'all' ||
    (filter === 'running'
      ? t.state === 'running'
      : filter === 'attention'
        ? needsAttention(t, data)
        : waiting.some((w: Row) => w.id === t.id));
  const shown = groups.filter((g) => g.tasks.some(matchesFilter));
  const recent = newest(
    data.events.filter(
      (e: Row) =>
        e.entity_id === goal.id ||
        tasks.some((t: Row) => t.id === e.entity_id) ||
        runs.some((r: Row) => r.id === e.entity_id),
    ),
  ).slice(0, 6);
  return (
    <div className="goal-overview">
      <QueueStatus data={data} />
      {goal.merged_into && (
        <p role="status">
          Este objetivo foi consolidado. As tarefas e evidências estão na{' '}
          <a href={'#goal=' + encodeURIComponent(goal.merged_into)}>operação principal</a>.
        </p>
      )}
      {!goal.merged_into && data.settings?.['exclusive-workstreams:' + goal.project]?.enabled && (
        <p className="workstream-policy">
          Uma execução por jogo. Jogos diferentes podem avançar em paralelo; as etapas de cada jogo
          entram na fila.
        </p>
      )}
      <div className="delivery-metrics">
        <div>
          <strong>
            {done}
            <em> / {groups.length}</em>
          </strong>
          <span>Entregas aceitas</span>
          <small>Todas as etapas da entrega concluídas</small>
        </div>
        <div>
          <strong>{active.length}</strong>
          <span>Em execução</span>
          <small>Agentes com tentativa iniciada</small>
        </div>
        <div>
          <strong>{waiting.length}</strong>
          <span>Etapas em espera</span>
          <small>Dependência, fila, cota ou recurso</small>
        </div>
        <div className={attention.length ? 'needs-attention' : ''}>
          <button
            className="attention-metric"
            aria-label={`Ver ${attention.length} avisos e bloqueios`}
            onClick={() => {
              attentionPanel.current?.scrollIntoView({ behavior: 'smooth', block: 'start' });
              attentionPanel.current?.focus({ preventScroll: true });
            }}
          >
            <strong>{attention.length}</strong>
            <span>Pedem atenção</span>
            <small>
              Ver motivos e próximos passos <ChevronRight size={12} />
            </small>
          </button>
        </div>
      </div>
      <section
        ref={attentionPanel}
        tabIndex={-1}
        className="attention-panel"
        aria-label="Avisos e bloqueios"
      >
        <div className="section-caption">
          <h2>Avisos e bloqueios</h2>
          <span>
            {attention.filter((t: Row) => taskAttention(t, data)?.kind === 'blocked').length}{' '}
            bloqueios ·{' '}
            {attention.filter((t: Row) => taskAttention(t, data)?.kind !== 'blocked').length} avisos
            de acompanhamento
          </span>
        </div>
        {!attention.length && <p>Nenhum aviso identificado no estado atual.</p>}
        {attention.map((task: Row) => {
          const alert = taskAttention(task, data)!;
          const run = latestRun(task, runs);
          const affected = affectedTasks(task.id, data);
          return (
            <article key={task.id} className={'attention-item ' + alert.kind}>
              <div className="attention-task">
                <h3>{task.title}</h3>
                <TaskStatus state={task.state} />
                <small>{routeLabel(task, run, data.accounts)}</small>
              </div>
              <AttentionReason alert={alert} inspect={inspect} />
              {!!affected.length && (
                <details className="attention-impact">
                  <summary>Impacto: {affected.length} etapas posteriores dependem desta</summary>
                  {affected.map((child) => (
                    <button
                      className="text-button"
                      key={child.id}
                      onClick={() => inspect(child.id)}
                    >
                      {child.title}
                    </button>
                  ))}
                </details>
              )}
              <div className="attention-footer">
                <button className="button small" onClick={() => inspect(task.id)}>
                  Ver motivo e execução <ChevronRight size={14} />
                </button>
                <small>
                  {run
                    ? `Tentativa ${run.attempt} · ${run.id.slice(0, 8)}`
                    : 'Esta etapa ainda não iniciou'}
                  {run?.started_at ? ` · iniciada ${date(run.started_at)}` : ''}
                </small>
              </div>
            </article>
          );
        })}
      </section>
      <ObjectivePlan detail={detail} />
      {goal.state === 'paused' && (
        <p className="goal-notice">
          Objetivo pausado. Novas tentativas não serão despachadas; execuções já iniciadas
          continuam.
        </p>
      )}
      <section className="focus-section" aria-label="O que acontece agora">
        <div className="section-caption">
          <h2>O que acontece agora</h2>
          <span>Atividade não equivale a entrega aprovada</span>
        </div>
        {focuses.map((task: Row) => {
          const run = latestRun(task, runs);
          const event = taskEvents(task, run, data.events)[0];
          const lastAction = run && latestObservation(run, data.events)?.actions?.at(-1);
          const acceptance = latestAcceptance(run, acceptances);
          return (
            <article
              key={task.id}
              className={'focus-card ' + (needsAttention(task, data) ? 'attention' : '')}
            >
              <div className="focus-main">
                <TaskStatus state={task.state} />
                <h3>{task.title}</h3>
                <p>{routeLabel(task, run, data.accounts)}</p>
                <p className="focus-reason">
                  {taskAttention(task, data)?.detail ??
                    reasonFor(task, data.tasks, dependencies, goal)}
                </p>
                {run?.checkpoint?.kind === 'automatic_recovery' && (
                  <div className="recovery-notice">
                    <strong>
                      {run.checkpoint.strategy === 'replan_and_repair'
                        ? 'Replanejando e corrigindo'
                        : 'Correção automática em andamento'}
                    </strong>
                    <p>{run.checkpoint.previousSummary}</p>
                    <small>
                      O agente recebeu os erros e as evidências da tentativa anterior. O aceite
                      continua pendente de novos testes.
                    </small>
                  </div>
                )}
                {acceptance && !acceptance.passed && (
                  <p className="attention-text">
                    {acceptance.checks.filter((c: Row) => !c.passed).length} critérios ainda não
                    atendidos.
                  </p>
                )}
                <button className="button small" onClick={() => inspect(task.id)}>
                  Ver tarefa e evidências <ChevronRight size={14} />
                </button>
              </div>
              <div className="focus-activity">
                <span>
                  Tentativa {run?.attempt ?? task.attempt_count} ·{' '}
                  {run?.started_at ? `iniciada há ${ago(run.started_at)}` : 'aguardando início'}
                </span>
                <strong>
                  {event
                    ? eventText(event)
                    : lastAction
                      ? `Última ferramenta: ${lastAction.tool ?? 'comando'}`
                      : 'Ainda sem atividade detalhada disponível'}
                </strong>
                <span>
                  {event
                    ? `Registrado há ${ago(event.created_at)} · ${date(event.created_at)}`
                    : lastAction?.finishedAt
                      ? `Último retorno há ${ago(lastAction.finishedAt)} · ${date(lastAction.finishedAt)}`
                      : 'Abra as tentativas para consultar o último resultado.'}
                </span>
                {run?.heartbeat_at && activeStates.has(task.state) && (
                  <small>
                    Sinal do executor há {ago(run.heartbeat_at)}. Confirma presença, não avanço da
                    tarefa.
                  </small>
                )}
              </div>
              {run && ['harness', 'opencode'].includes(task.execution?.kind) && (
                <div className="focus-execution">
                  <div className="next-milestone">
                    <strong>O que falta nesta etapa</strong>
                    <ul>
                      {task.criteria
                        .filter(
                          (c: Row) =>
                            !acceptance?.checks.some(
                              (check: Row) => check.id === c.id && check.passed,
                            ),
                        )
                        .map((c: Row) => (
                          <li key={c.id}>{c.label}</li>
                        ))}
                    </ul>
                    <p>
                      Depois:{' '}
                      {dependencies
                        .filter((d: Row) => d.depends_on === task.id)
                        .map((d: Row) => tasks.find((t: Row) => t.id === d.task_id)?.title)
                        .join(' · ') || 'Conferir o aceite final do objetivo.'}
                    </p>
                  </div>
                  <ExecutionDetails task={task} run={run} data={data} compact />
                </div>
              )}
            </article>
          );
        })}
        {!focuses.length && (
          <div className="idle-focus">
            <Clock size={20} />
            <p>
              {goal.state === 'completed'
                ? 'Objetivo concluído: todas as etapas foram aceitas.'
                : goal.state === 'cancelled'
                  ? 'Objetivo cancelado.'
                  : eligible[0]
                    ? reasonFor(eligible[0], tasks, dependencies, goal)
                    : 'Nenhuma etapa pendente.'}
            </p>
          </div>
        )}
      </section>
      <section className="delivery-section">
        <div className="section-caption">
          <div>
            <h2>Entregas e próximas etapas</h2>
            <p>Na ordem das dependências. Uma etapa só libera a seguinte após o aceite.</p>
          </div>
        </div>
        <div className="delivery-filters" role="group" aria-label="Filtrar etapas">
          {[
            ['all', 'Todas'],
            ['running', 'Em execução'],
            ['attention', 'Pedem atenção'],
            ['waiting', 'Em espera'],
          ].map(([key, label]) => (
            <button
              key={key}
              aria-pressed={filter === key}
              className={filter === key ? 'selected' : ''}
              onClick={() => setFilter(key)}
            >
              {label}
            </button>
          ))}
        </div>
        <div className="delivery-list">
          {shown.map((group) => {
            const accepted = group.tasks.filter((t) => t.state === 'accepted').length;
            const current =
              group.tasks.find((t) => needsAttention(t, data)) ??
              group.tasks.find((t) => activeStates.has(t.state)) ??
              group.tasks.find((t) => t.state !== 'accepted');
            return (
              <article
                className={
                  'delivery-row ' +
                  (group.tasks.some((t) => t.state === 'running') ? 'is-active' : '')
                }
                key={group.id}
              >
                <div className="delivery-title">
                  <span className="delivery-number">
                    {accepted === group.tasks.length ? (
                      <Check size={17} />
                    ) : (
                      groups.indexOf(group) + 1
                    )}
                  </span>
                  <div>
                    <h3>{group.title}</h3>
                    <span>
                      {accepted}/{group.tasks.length} etapas aceitas
                    </span>
                  </div>
                </div>
                <div className="delivery-steps">
                  {group.tasks.filter(matchesFilter).map((task) => (
                    <button
                      key={task.id}
                      className={'delivery-step ' + task.state}
                      onClick={() => inspect(task.id)}
                      aria-label={`${task.title} — ${taskLabel(task.state)}`}
                    >
                      <strong>
                        {task.title.includes(':')
                          ? task.title.split(':').slice(1).join(':').trim()
                          : task.title}
                      </strong>
                      <TaskStatus state={task.state} />
                      <small>{routeLabel(task, latestRun(task, runs), data.accounts)}</small>
                    </button>
                  ))}
                </div>
                <p className="delivery-next">
                  {current
                    ? (taskAttention(current, data)?.detail ??
                      reasonFor(current, data.tasks, dependencies, goal))
                    : 'Entrega aceita. Evidências disponíveis nas etapas acima.'}
                </p>
              </article>
            );
          })}
          {!shown.length && <p className="directory-empty">Nenhuma entrega neste filtro.</p>}
        </div>
      </section>
      <section className="recent-activity">
        <div className="section-caption">
          <h2>Últimos sinais da operação</h2>
          <span>Recorte do histórico recente</span>
        </div>
        {recent.map((e) => (
          <div className="activity-line" key={e.id}>
            <time title={date(e.created_at)}>
              {new Date(e.created_at).toLocaleTimeString('pt-BR')}
            </time>
            <div>
              <strong>{eventText(e)}</strong>
              <small>
                {tasks.find(
                  (t: Row) =>
                    t.id === e.entity_id ||
                    runs.find((r: Row) => r.id === e.payload?.runId)?.task_id === t.id,
                )?.title ?? goal.title}
              </small>
            </div>
          </div>
        ))}
        {!recent.length && (
          <p className="directory-empty">
            Nenhum evento desta operação no recorte recente. As tentativas e evidências permanecem
            disponíveis.
          </p>
        )}
      </section>
    </div>
  );
}

export function TaskInspection({
  task,
  detail,
  data,
  inspect,
}: {
  task: Row;
  detail: Row;
  data: Row;
  inspect?: (id: string) => void;
}) {
  data = withDetail(data, detail);
  const attempts = newest(detail.runs.filter((r: Row) => r.task_id === task.id));
  const current = attempts[0];
  const check = latestAcceptance(current, detail.acceptances);
  const alert = taskAttention(task, data);
  return (
    <div className="task-inspection">
      <TaskStatus state={task.state} />
      {alert ? (
        <AttentionReason alert={alert} inspect={inspect} />
      ) : (
        <p>{reasonFor(task, data.tasks, detail.dependencies, detail.goal)}</p>
      )}
      <dl>
        <div>
          <dt>Conta e modelo</dt>
          <dd>{routeLabel(task, current, data.accounts)}</dd>
        </div>
        <div>
          <dt>Workspace</dt>
          <dd>{task.execution?.workspace ?? 'Sem workspace configurado'}</dd>
        </div>
        <div>
          <dt>Executor</dt>
          <dd>
            {current?.runtime === 'muse' ||
            data.accounts?.some(
              (a: Row) =>
                a.id === (current?.account_id ?? task.account_id) && a.provider === 'muse',
            )
              ? 'Muse Code'
              : task.execution?.kind === 'opencode'
                ? 'OpenCode'
                : task.execution?.kind === 'harness'
                  ? 'DeepSeek Harness'
                  : (task.execution?.kind ?? 'Não configurado')}
          </dd>
        </div>
      </dl>
      {current && ['harness', 'opencode'].includes(task.execution?.kind) && (
        <ExecutionDetails task={task} run={current} data={data} />
      )}
      <h3>Critérios desta tentativa</h3>
      <p className="inspection-note">
        Resultado entregue não significa tarefa aceita. Os critérios abaixo controlam a liberação da
        próxima etapa.
      </p>
      <ul className="acceptance-checks">
        {task.criteria.map((c: Row) => {
          const result = check?.checks.find((item: Row) => item.id === c.id);
          return (
            <li key={c.id} className={result?.passed ? 'passed' : result ? 'not-passed' : ''}>
              {result?.passed ? <Check size={17} /> : <Clock size={17} />}
              <div>
                <strong>{c.label}</strong>
                <small>{result?.detail ?? 'Ainda não verificado nesta tentativa.'}</small>
              </div>
            </li>
          );
        })}
      </ul>
      <h3>Tentativas e evidências · {attempts.length}</h3>
      {attempts.map((run: Row, index: number) => {
        const acceptance = latestAcceptance(run, detail.acceptances);
        const artifacts = data.artifacts.filter((a: Row) => a.run_id === run.id);
        const collection = run.checkpoint?.kind === 'artifact_recovery';
        const recoveredBy = attempts.find(
          (a: Row) =>
            a.checkpoint?.kind === 'artifact_recovery' && a.checkpoint.sourceRunId === run.id,
        );
        return (
          <details className="attempt-detail" key={run.id} open={index === 0}>
            <summary>
              {collection ? 'Coleta' : 'Tentativa'} {run.attempt} ·{' '}
              {acceptance?.passed
                ? 'Aceita'
                : run.state === 'submitted'
                  ? 'Entregue sem aceite'
                  : recoveredBy
                    ? 'Falhou · arquivos recuperados'
                    : taskLabel(run.state)}
              <span>
                {collection
                  ? 'Automática · sem chamada ao modelo'
                  : (run.model ?? 'Modelo não registrado')}
              </span>
            </summary>
            <p>
              {collection
                ? 'Recuperação dos arquivos já produzidos; nenhuma nova sessão de agente.'
                : routeLabel(task, run, data.accounts)}
            </p>
            <small>
              {run.started_at ? `Início: ${date(run.started_at)}` : 'Ainda não iniciou'}
              {run.finished_at ? ` · Fim: ${date(run.finished_at)}` : ''}
            </small>
            <p className="attempt-summary">
              {run.summary ??
                (activeStates.has(run.state)
                  ? 'Execução ainda sem resultado final.'
                  : 'Sem resumo registrado.')}
            </p>
            {run.error_code && <p className="attention-text">{run.error_code}</p>}
            {acceptance && !acceptance.passed && (
              <ul>
                {acceptance.checks
                  .filter((c: Row) => !c.passed)
                  .map((c: Row) => (
                    <li key={c.id}>
                      {c.label}: {c.detail}
                    </li>
                  ))}
              </ul>
            )}
            <div className="attempt-artifacts">
              {artifacts.map((a: Row) => (
                <a key={a.id} href={'/api/artifacts/' + a.id} title={`SHA256: ${a.digest}`}>
                  <FileText size={16} />
                  <span>
                    {a.name}
                    <small>
                      {Math.ceil(a.size / 1024)} KB · {a.digest.slice(0, 12)}
                    </small>
                  </span>
                  <Download size={15} />
                </a>
              ))}
            </div>
            {!artifacts.length && (
              <p className="inspection-note">
                {recoveredBy
                  ? `Arquivos disponíveis na coleta ${recoveredBy.attempt}, acima.`
                  : 'Nenhuma evidência publicada nesta tentativa.'}
              </p>
            )}
            <small>ID da execução: {run.id}</small>
          </details>
        );
      })}
      {!attempts.length && <p>Esta tarefa ainda não iniciou nenhuma tentativa.</p>}
      <details className="task-contract">
        <summary>Instruções completas da tarefa</summary>
        <pre>{task.description}</pre>
      </details>
    </div>
  );
}
