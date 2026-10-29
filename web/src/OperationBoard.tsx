import React, { useState, useEffect } from 'react';
import { Activity, CheckCircle2, ChevronRight, Clock, AlertCircle, Cpu } from 'lucide-react';
import {
  operationGames,
  operationProductivityLine,
  operationWorkforce,
  supervisionHealthy,
  taskFlow,
} from './operation-state';
import { latestObservation } from './task-attention';
import { ScopedPlanners } from './ScopedPlanners';
import {deliveryStatus} from './delivery-status';
import { SourceDownload } from './SourceDownload';
import { runRoute, elapsed } from './run-route';
import './operation-board.css';
type Row = Record<string, any>;
const shortAge = (at?: string) => {
  if (!at) return 'sem registro';
  const n = Math.max(0, Math.floor((Date.now() - Date.parse(at)) / 1000));
  return n < 60 ? n + 's' : n < 3600 ? Math.floor(n / 60) + 'min' : Math.floor(n / 3600) + 'h';
};
function activityAt(run: Row | undefined, data: Row) {
  if (!run) return undefined;
  const event = data.events
    ?.filter(
      (e: Row) =>
        e.payload?.runId === run.id &&
        [
          'MODEL_RESPONSE',
          'OPENCODE_TOOL_EVENT',
          'HARNESS_TOOL_EVENT',
          'MUSE_TOOL_EVENT',
          'ARTIFACT_CREATED',
        ].includes(e.kind),
    )
    .sort((a: Row, b: Row) => Date.parse(b.created_at) - Date.parse(a.created_at))[0];
  const obs = latestObservation(run, data.events);
  return [event?.created_at, obs?.actions?.at(-1)?.finishedAt]
    .filter(Boolean)
    .sort((a, b) => Date.parse(b) - Date.parse(a))[0];
}
export function OperationBoard({
  detail,
  data,
  inspect,
  technical,
}: {
  detail: Row;
  data: Row;
  inspect: (id: string) => void;
  technical: () => React.ReactNode;
}) {
  const [expanded, setExpanded] = useState<string | null>(null),
    [showTechnical, setTechnical] = useState(false);
  const [, tick] = useState(0);
  useEffect(() => {
    const id = setInterval(() => tick((n) => n + 1), 5000);
    return () => clearInterval(id);
  }, []);
  const progress = data.settings?.['operation-progress:' + detail.goal.id];
  const games = operationGames(detail.tasks, data).map((game) => {
      const milestone = progress?.milestones?.find((item: Row) => item.scope === game.id);
      if (!milestone) return game;
      return {
        ...game,
        complete: game.complete && milestone.verificationMatchesCandidate === true,
        delivery: {
          ...game.delivery,
          gates: game.delivery.gates.map((gate) => ({
            ...gate,
            complete:
              gate.id === 'integrate'
                ? milestone.integration?.state === 'accepted' &&
                  !!milestone.integration?.candidateDigest
                : milestone.verificationMatchesCandidate === true,
          })),
        },
      };
    }),
    healthy = supervisionHealthy(data),
    workforce = operationWorkforce(detail.goal.id, detail.tasks, data);
  const liveRuns = (detail.runs ?? [])
    .filter((run: Row) => ['queued', 'running', 'cancelling', 'reconciling'].includes(run.state))
    .map((run: Row) => ({ run, task: detail.tasks.find((t: Row) => t.id === run.task_id) }))
    .filter((entry: Row) => entry.task);
  const done = games.filter((g) => g.complete).length,
    working = games.filter((g) => g.focus.kind === 'running').length;
  const repairing = games.filter((g) => g.flows.some((f) => f.kind === 'repair')).length;
  const actions = detail.tasks
      .filter((task: Row) => !['cancelled', 'superseded'].includes(task.state))
      .map((task: Row) => ({ task, ...taskFlow(task, data) }))
      .filter((flow: Row) => flow.action),
    stalled = games.filter((g) => g.flows.some((f) => f.kind === 'stalled'));
  const paused = detail.goal.state === 'paused';
  const backlog = data.settings?.['backlog-status:' + detail.goal.id];
  const managerEnabled = data.settings?.['management:' + detail.goal.id]?.enabled;
  const manager = data.settings?.['management-state:' + detail.goal.id];
  const poolHealth = data.settings?.['workforce-health:' + detail.goal.id];
  const managerTask = detail.tasks.find((t: Row) => t.stage === 'coordinate');
  const managerRunning = ['running', 'queued', 'reconciling'].includes(managerTask?.state);
  const title = paused
    ? 'Operação pausada'
    : actions.length
      ? 'Sua ação é necessária'
      : stalled.length
        ? 'Há retomadas sem confirmação'
        : working
          ? 'Trabalho em andamento'
          : done === games.length && games.length
            ? 'Operação concluída'
            : repairing
              ? 'Recuperando execuções'
              : 'Aguardando execução';
  const subtitle = paused
    ? 'Novas execuções estão suspensas.'
    : actions.length
      ? actions.length + ' itens precisam de uma decisão sua.'
      : stalled.length
        ? 'A recuperação automática ainda precisa ser conferida.'
        : healthy
          ? 'Você não precisa agir agora. As falhas técnicas entram em recuperação automática.'
          : 'Supervisão automática sem confirmação recente.';
  return (
    <div className="operation-board">
      {detail.goal.merged_into ? (
        <div className="operation-banner">
          <a href={'#goal=' + detail.goal.merged_into}>
            Abrir operação unificada <ChevronRight size={16} />
          </a>
        </div>
      ) : (
        <>
          <header
            className={'operation-banner ' + (actions.length || stalled.length ? 'attention' : '')}
          >
            <div className="operation-status-icon">
              {actions.length || stalled.length ? (
                <AlertCircle />
              ) : done === games.length ? (
                <CheckCircle2 />
              ) : (
                <Activity />
              )}
            </div>
            <div>
              <h2>{title}</h2>
              <p>{subtitle}</p>
            </div>
            <span className={'supervision-indicator ' + (healthy ? 'online' : '')}>
              {healthy ? 'Supervisão ativa' : 'Supervisão indisponível'}
            </span>
          </header>
          <SourceDownload goal={detail.goal} />
          <div className="operation-productivity-line" aria-label="Métrica de produtividade">
            {operationProductivityLine(detail, data).line}
          </div>
          {managerEnabled && (
            <details className="operation-manager">
              <summary>
                Gestor automático ·{' '}
                {paused ? 'pausado' : managerRunning ? 'revisando agora' : 'a cada 2 min'}
                {manager?.reviewedAt && ` · última revisão há ${shortAge(manager.reviewedAt)}`}
              </summary>
              <p>{manager?.summary ?? 'Aguardando a primeira revisão automática.'}</p>
              {manager?.capacityReason && <p>{manager.capacityReason}</p>}
              {!!manager?.receipts?.length && (
                <ul>
                  {manager.receipts.map((receipt: Row, i: number) => (
                    <li key={i}>
                      {receipt.code === 'APPLIED' ? 'Aplicado' : 'Não aplicado: ' + receipt.code} —{' '}
                      {receipt.reason}
                    </li>
                  ))}
                </ul>
              )}
              {managerTask && (
                <button onClick={() => inspect(managerTask.id)}>Ver revisão e evidências</button>
              )}
            </details>
          )}
          {progress?.stagnation?.detected && (
            <section className="operation-process-alert" aria-label="Avanço da entrega">
              <strong>Entrega sem avanço confirmado</strong>
              <span>
                {progress.nextTransition?.reason ??
                  'O gestor precisa rever o plano. Revisões e novas tentativas não contam como entrega.'}
              </span>
              {managerTask && (
                <button className="text-button" onClick={() => inspect(managerTask.id)}>Ver revisão do gestor</button>
              )}
            </section>
          )}
          <div className="operation-kpis" aria-label="Resumo da operação">
            <div>
              <strong>
                {done}
                <small>/{games.length}</small>
              </strong>
              <span>Jogos concluídos</span>
            </div>
            <div>
              <strong>{working}</strong>
              <span>Em execução</span>
            </div>
            <div>
              <strong>{repairing}</strong>
              <span>Com correção automática</span>
            </div>
            <div>
              <strong>{actions.length}</strong>
              <span>Precisam de você</span>
            </div>
          </div>
          {!!liveRuns.length && (
            <section className="operation-live" aria-label="Executando agora">
              <header>
                <strong>Executando agora</strong>
                <span>{liveRuns.length} {liveRuns.length === 1 ? 'tarefa' : 'tarefas'}</span>
              </header>
              {liveRuns.map(({ run, task }: Row) => (
                <button key={run.id} onClick={() => inspect(task.id)}>
                  <Cpu size={15} />
                  <span>
                    <strong>{task.title}</strong>
                    <small>{runRoute(run, data.accounts ?? [])}</small>
                  </span>
                  <small>
                    {run.state === 'running' ? 'há ' + elapsed(run.started_at) : 'na fila'}
                    {run.heartbeat_at ? ' · sinal há ' + elapsed(run.heartbeat_at) : ''}
                  </small>
                  <ChevronRight size={14} />
                </button>
              ))}
            </section>
          )}
          {workforce && (
            <section className="operation-workforce" aria-label="Equipe da operação">
              <header>
                <strong>Agentes</strong>
                <span>
                  {workforce.running} em execução
                  {!!workforce.queued && ` · ${workforce.queued} na fila`}
                  {!!workforce.confirming && ` · ${workforce.confirming} em confirmação`}
                </span>
              </header>
              {poolHealth &&
                ['glm', 'muse'].map(
                  (lane) =>
                    poolHealth[lane] && (
                      <section
                        key={lane}
                        className="workforce-pool-health"
                        aria-label={'Reposição das sessões ' + (lane === 'glm' ? 'GLM' : 'Muse')}
                      >
                        <strong>
                          {lane === 'glm' ? 'GLM' : 'Muse'}: {(poolHealth[lane].running ?? 0) - (lane==='glm' ? poolHealth.glm.planning?.running??0 : 0)}/
                          {poolHealth[lane].target ?? 0} sessões em execução
                        </strong>
                        <span>
                          {poolHealth[lane].ready ?? 0} tarefas prontas ·{' '}
                          {(poolHealth[lane].blocked ?? 0) +
                            (poolHealth[lane].parked ?? 0) +
                            (poolHealth[lane].delayed ?? 0)}{' '}
                          bloqueadas
                        </span>
                        <span>{poolHealth[lane].reason ?? poolHealth.reason}</span>
                        {poolHealth.observedAt && (
                          <small>
                            Fila conferida há {shortAge(poolHealth.observedAt)}
                            {lane === 'glm' &&
                              ` · limite de ${poolHealth.glm?.requestLimit ?? 6} chamadas ao modelo`}
                          </small>
                        )}
                      </section>
                    ),
                )}
              {backlog && (
                <div className="workforce-backlog">
                  <strong>
                    Fila GLM: {backlog.glm.ready} prontas · {backlog.glm.blocked} aguardam
                    dependências
                  </strong>
                  <span>
                    {backlog.plannerRunning
                      ? 'Supervisor definindo prioridades e próximo trabalho.'
                      : backlog.reason}
                  </span>
                  <details>
                    <summary>Plano de reposição</summary>
                    <p>{backlog.lastPlan?.reason ?? 'Aguardando primeiro plano.'}</p>
                    <small>
                      Vagas livres usam tarefas prontas automaticamente. O supervisor cria trabalho
                      adicional e encaminha correções quando há bloqueios recorrentes.
                    </small>
                  </details>
                </div>
              )}
              <div className="workforce-groups">
                {workforce.groups.map((group) => (
                  <div className="workforce-group" key={group.id}>
                    <span>{group.label}</span>
                    <strong>
                      {group.running} <small>em execução</small>
                    </strong>
                    <small>
                      {group.target !== null
                        ? `Meta: ${group.target} ${group.target === 1 ? 'sessão' : 'sessões'}`
                        : 'Conforme demanda e cota'}
                      {!!group.queued && ` · ${group.queued} na fila`}
                      {!!group.confirming && ` · ${group.confirming} em confirmação`}
                    </small>
                    {group.requestLimit !== null && (
                      <small>Máx. {group.requestLimit} requisições simultâneas ao modelo</small>
                    )}
                  </div>
                ))}
              </div>
              {!!workforce.live.length && (
                <details className="workforce-details">
                  <summary>Ver agentes</summary>
                  {workforce.live.map(({ run, task, account }: Row) => (
                    <button key={run.id} onClick={() => inspect(task.id)}>
                      <span>
                        <strong>{task.title}</strong>
                        <small>
                          {run.model} · {account?.label ?? run.account_id}
                        </small>
                      </span>
                      <small>
                        {run.state === 'running'
                          ? 'Executando'
                          : run.state === 'queued'
                            ? 'Na fila'
                            : run.state === 'cancelling'
                              ? 'Encerrando'
                              : 'Confirmando execução'}
                      </small>
                      <ChevronRight size={14} />
                    </button>
                  ))}
                </details>
              )}
            </section>
          )}
          {(workforce || data.settings?.['planning-demand:' + detail.goal.id]) && (
            <ScopedPlanners key={detail.goal.id} goalId={detail.goal.id} inspect={inspect} />
          )}
          {!!actions.length && (
            <section className="operation-actions" aria-label="O que você precisa fazer">
              {actions.map((a: Row) => (
                <button key={a.task.id} onClick={() => inspect(a.task.id)}>
                  <AlertCircle size={17} />
                  <span>
                    <strong>{a.task.title}</strong>
                    {a.detail}
                  </span>
                  <ChevronRight size={17} />
                </button>
              ))}
            </section>
          )}
          <div className="game-board" aria-label="Progresso por jogo">
            {games.map((g) => {
              const f = g.focus,
                run = f.run,
                last = activityAt(run, data),
                isOpen = expanded === g.id;
              const delivery=deliveryStatus(g,detail,data);
              const stage = f.task.title.includes('—')
                ? f.task.title.split('—').slice(1).join('—').trim()
                : f.task.title.includes(':')
                  ? f.task.title.split(':').slice(1).join(':').trim()
                  : f.task.title;
              return (
                <article className={'game-card ' + (g.complete ? 'done' : f.kind)} key={g.id}>
                  <div className="game-card-head">
                    <h3>{g.title}</h3>
                    <span className={'flow-badge ' + (g.complete ? 'done' : f.kind)}>
                      {g.complete ? 'Concluído' : f.label}
                    </span>
                  </div>
                  <p className="game-stage">{g.complete ? 'Entrega aceita' : stage}</p>
                  <div className="game-progress" aria-label="Integração e QA final">
                    {g.delivery.gates.map((gate) => (
                      <i
                        key={gate.id}
                        className={gate.complete ? 'done' : 'waiting'}
                        title={gate.label + ': ' + (gate.complete ? 'aceita' : 'pendente')}
                      />
                    ))}
                  </div>
                  <div className="game-card-meta">
                    <span>
                      {g.delivery.gates
                        .map((gate) => gate.label + ': ' + (gate.complete ? 'aceita' : 'pendente'))
                        .join(' · ')}
                    </span>
                    <span>
                      {last
                        ? 'Atividade há ' + shortAge(last)
                        : run?.started_at
                          ? 'Iniciada há ' + shortAge(run.started_at)
                          : 'Ainda não iniciou'}
                    </span>
                  </div>
                  <div className="delivery-next" aria-label={'Encaminhamento de '+g.title}>
                    <span>Responsável: <strong>{delivery.owner}</strong></span>
                    <span>{delivery.reason}</span>
                    <span>Próximo: {delivery.next}</span>
                    <small>{delivery.lastAccepted?'Último aceite de integração/QA há '+shortAge(delivery.lastAccepted):'Integração e QA ainda sem aceite registrado.'}</small>
                    <details>
                      <summary>Ver pendência e candidata</summary>
                      <p>{delivery.candidate?'Candidata: '+delivery.candidate.slice(0,12):'Candidata atual ainda sem identificação confirmada.'}</p>
                      <p>{delivery.pendingCases} casos em supervisão{delivery.observedAt?' · conferido há '+shortAge(delivery.observedAt):''}</p>
                      {delivery.taskId && <button className="text-button" onClick={()=>inspect(delivery.taskId)}>Abrir a etapa responsável <ChevronRight size={14}/></button>}
                    </details>
                  </div>
                  <footer>
                    <button
                      className="text-button"
                      onClick={() => setExpanded(isOpen ? null : g.id)}
                      aria-expanded={isOpen}
                    >
                      {isOpen ? 'Ocultar etapas' : 'Ver etapas'} <ChevronRight size={14} />
                    </button>
                    <button className="button small" onClick={() => inspect(f.task.id)}>
                      Ver execução <ChevronRight size={14} />
                    </button>
                  </footer>
                  {isOpen && (
                    <div className="game-stage-list">
                      <p className="game-evidence-count">
                        {g.delivery.acceptedDiagnostics}/{g.delivery.diagnostics} entregas
                        complementares aceitas. Esse número não aprova o jogo.
                      </p>
                      {g.flows.map((s) => (
                        <button key={s.task.id} onClick={() => inspect(s.task.id)}>
                          <span className={'stage-dot ' + s.kind} />
                          <span>{s.task.title}</span>
                          <small>{s.label}</small>
                          <ChevronRight size={14} />
                        </button>
                      ))}
                    </div>
                  )}
                </article>
              );
            })}
          </div>
          <div className="operation-footnote">
            <Clock size={14} />
            <span>Conclusão: ainda sem previsão confiável.</span>
            <span>{workforce ? 'Frentes em cópias isoladas.' : 'Uma execução por jogo.'}</span>
          </div>
        </>
      )}
      <details
        className="operation-technical"
        open={showTechnical}
        onToggle={(e) => setTechnical(e.currentTarget.open)}
      >
        <summary>Histórico, plano e detalhes técnicos</summary>
        {showTechnical && technical()}
      </details>
    </div>
  );
}
