import { Autonomy } from './Autonomy';
import React, { useEffect, useState, useCallback, useRef } from 'react';
import { createRoot } from 'react-dom/client';
import {
  Activity,
  ArrowDownLeft,
  ArrowRight,
  ArrowUpRight,
  BarChart3,
  Bell,
  BookOpen,
  Box,
  Check,
  CheckCircle2,
  ChevronDown,
  ChevronRight,
  Clock,
  Code2,
  Command,
  Database,
  FileText,
  GitBranch,
  LayoutDashboard,
  ListTodo,
  Loader2,
  MemoryStick,
  Network,
  Play,
  Plus,
  Search,
  Settings,
  ShieldCheck,
  Sparkles,
  Users,
  Wallet,
  X,
  PauseCircle,
  AlertTriangle,
  ExternalLink,
  Download,
  RefreshCw,
  MessageSquare,
  Cpu,
} from 'lucide-react';
import './style.css';
import './chat.css';
import './live-routes.css';
import { SimpleChat, ConnectionsPanel, ConnectAccountDialog } from './Chat';
import { Cycles } from './Cycles';
import { runRoute, elapsed } from './run-route';
import { GoalDirectory, GoalOverview, TaskInspection } from './GoalProgress';
import { ExecutionTreeView } from './ExecutionTreeView';
import { retainExecutionEvents } from './task-attention';
type Row = Record<string, any>;
type State = {
  goals: Row[];
  tasks: Row[];
  runs: Row[];
  agents: Row[];
  accounts: Row[];
  events: Row[];
  memories: Row[];
  contexts: Row[];
  artifacts: Row[];
  settings: Row;
  integrations: Row;
  engine: Row;
  recall: Row;
  serverTime: string;
};
const labels: Row = {
  active: 'Em andamento',
  planning: 'Planejando',
  replanning: 'Replanejando',
  planned: 'Plano pronto',
  waiting_predecessor: 'Aguardando a leva anterior',
  paused: 'Pausado',
  completed: 'Concluído',
  ready: 'Pronta',
  queued: 'Na fila',
  running: 'Em execução',
  accepted: 'Aceita',
  submitted: 'Entrega sem aceite',
  failed: 'Falhou',
  recovering_artifacts: 'Recuperando arquivos',
  cancelled: 'Cancelada',
  cancelling: 'Cancelando',
  reconciling: 'Em reconciliação',
  waiting_quota: 'Aguardando cota',
  waiting_resource: 'Aguardando recurso',
  waiting_configuration: 'Configuração pendente',
  blocked_dependencies: 'Aguardando dependências',
  blocked_failed: 'Dependência falhou',
  retry_scheduled: 'Retomada agendada',
  recovery_blocked: 'Correção precisa de novo plano',
  available: 'Disponível',
  validated: 'Validado',
  confirmed: 'Confirmado',
  unverified: 'Não verificado',
  unknown: 'Desconhecida',
  not_applicable: 'Não se aplica',
};
const navItems = [
  ['chat', 'Chat', MessageSquare],
  ['dashboard', 'Visão geral', LayoutDashboard],
  ['goals', 'Objetivos e tarefas', ListTodo],
  ['cycles', 'Ciclos', RefreshCw],
  ['autonomy', 'Autonomia', Activity],
  ['organization', 'Organização', Network],
  ['registry', 'Registro de agentes', Users],
  ['workers', 'Execuções', Box],
  ['artifacts', 'Artefatos', FileText],
  ['memory', 'Memória', Database],
  ['accounts', 'Contas e cotas', Wallet],
  ['events', 'Eventos', Activity],
  ['settings', 'Configurações', Settings],
] as const;
const departmentLabels: Row = {
  engineering: 'Engineering',
  research: 'Research',
  ops: 'Operations',
  security: 'Security',
  qa: 'Quality Assurance',
};
const relative = (s: string) => {
  const n = Math.max(0, Math.floor((Date.now() - new Date(s).getTime()) / 1000));
  return n < 60 ? n + 's' : n < 3600 ? Math.floor(n / 60) + 'min' : Math.floor(n / 3600) + 'h';
};
const time = (s: string) =>
  new Date(s).toLocaleTimeString('pt-BR', {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
async function api(url: string, method = 'GET', body?: unknown, signal?: AbortSignal) {
  const r = await fetch('/api' + url, {
    method,
    headers: body ? { 'Content-Type': 'application/json' } : undefined,
    body: body ? JSON.stringify(body) : undefined,
    signal: signal ? AbortSignal.any([signal, AbortSignal.timeout(20000)]) : AbortSignal.timeout(20000),
  });
  const data = await r.json();
  if (!r.ok) throw Error(data.message ?? 'Falha na solicitação');
  return data;
}
function Badge({ state }: { state: string }) {
  return (
    <span className={'badge ' + state}>
      <i />
      {labels[state] ?? state}
    </span>
  );
}
function Empty({
  icon: Icon = Box,
  title,
  description,
  action,
}: {
  icon?: any;
  title: string;
  description: string;
  action?: React.ReactNode;
}) {
  return (
    <div className="empty">
      <span className="empty-icon">
        <Icon size={26} />
      </span>
      <h3>{title}</h3>
      <p>{description}</p>
      {action}
    </div>
  );
}
function Panel({
  title,
  subtitle,
  children,
  action,
  className = '',
}: {
  title: string;
  subtitle?: string;
  children: React.ReactNode;
  action?: React.ReactNode;
  className?: string;
}) {
  return (
    <section className={'panel ' + className}>
      <div className="panel-heading">
        <div>
          <h2>{title}</h2>
          {subtitle && <p>{subtitle}</p>}
        </div>
        {action}
      </div>
      {children}
    </section>
  );
}
function App() {
  const [snapshot, setData] = useState<State | null>(null),
    [page, setPage] = useState(() =>
      new URLSearchParams(location.hash.slice(1)).has('goal')
        ? 'goals'
        : location.hash === '#autonomy' ? 'autonomy' : location.hash.includes('cycle')
          ? 'cycles'
          : 'chat',
    ),
    [goalId, setGoalId] = useState<string | null>(() =>
      new URLSearchParams(location.hash.slice(1)).get('goal'),
    ),
    [detail, setDetail] = useState<Row | null>(null),
    [authenticated, setAuthenticated] = useState(false),
    [detailError, setDetailError] = useState(''),
    [tab, setTab] = useState('overview'),
    [query, setQuery] = useState(''),
    [modal, setModal] = useState<string | null>(null),
    [error, setError] = useState(''),
    [busy, setBusy] = useState(false),
    [selectedContext, setSelectedContext] = useState<Row | null>(null),
    [menuOpen, setMenuOpen] = useState(false),
    [connecting, setConnecting] = useState(false),
    [collections, setCollections] = useState<Record<string, Row>>({});
  const data: State | null = snapshot && {...snapshot,
    runs: page === 'workers' ? collections.runs?.rows ?? snapshot.runs : snapshot.runs,
    artifacts: collections.artifacts?.rows ?? snapshot.artifacts,
    memories: collections.memories?.rows ?? snapshot.memories,
    contexts: collections.contexts?.rows ?? snapshot.contexts};
  const refreshing = useRef(false);
  const observedEvents = useRef<Row[]>([]);
  useEffect(() => {
    const followLocation = () => {
      const id = new URLSearchParams(location.hash.slice(1)).get('goal');
      setGoalId(id);
      setDetail(null);
      setTab('overview');
      setPage(id ? 'goals' : location.hash === '#autonomy' ? 'autonomy' : location.hash.includes('cycle') ? 'cycles' : 'chat');
    };
    window.addEventListener('hashchange', followLocation);
    window.addEventListener('popstate', followLocation);
    return () => {
      window.removeEventListener('hashchange', followLocation);
      window.removeEventListener('popstate', followLocation);
    };
  }, []);
  const refresh = useCallback(async () => {
    if (refreshing.current) return;
    refreshing.current = true;
    try {
      const value = await api('/panel/state');
      const observed = observedEvents.current;
      observedEvents.current = [];
      setData((previous) => ({
        ...previous,
        ...value,
        memories: previous?.memories ?? value.memories,
        contexts: previous?.contexts ?? value.contexts,
        artifacts: previous?.artifacts ?? value.artifacts,
        events: retainExecutionEvents([...(previous?.events ?? []), ...observed], value.events),
      }));
      return value;
    } catch (e: any) {
      setError(e.message);
    } finally {
      refreshing.current = false;
    }
  }, []);
  useEffect(() => {
    let stream: EventSource | undefined, pending: ReturnType<typeof setTimeout> | undefined;
    let current = true, sessionReady = false;
    const connect = (value: Row | undefined) => {
      if (!current || stream || !value?.eventCursor) return;
      stream = new EventSource('/api/events?after=' + encodeURIComponent(value.eventCursor));
      stream.onmessage = (message) => {
        try {
          const event = JSON.parse(message.data);
          if (event.payload?.runId)
            observedEvents.current = retainExecutionEvents(observedEvents.current, [event]);
        } catch { /* Keep the persisted snapshot when a live event is malformed. */ }
        if (!pending) pending = setTimeout(() => {
          pending = undefined;
          void refresh();
        }, 1500);
      };
    };
    api('/session')
      .then(async () => {
        if (!current) return;
        sessionReady = true;
        setAuthenticated(true);
        const initial = await refresh();
        connect(initial);
      })
      .catch((e) => setError(e.message));
    const timer = setInterval(() => {
      if (sessionReady) void refresh().then(connect);
    }, 6000);
    return () => {
      current = false;
      stream?.close();
      clearInterval(timer);
      clearTimeout(pending);
    };
  }, [refresh]);
  useEffect(() => {
    if (!goalId || !authenticated) return;
    let current = true, pending: ReturnType<typeof setTimeout> | undefined;
    const controller = new AbortController();
    setDetailError('');
    const load = async () => {
      try {
        const value = await api('/panel/goals/' + goalId, 'GET', undefined, controller.signal);
        if (current) { setDetail(value); setDetailError(''); }
      } catch (e: any) { if (current) setDetailError(e.message); }
      finally { if (current) pending = setTimeout(() => void load(), 6000); }
    };
    void load();
    return () => {
      current = false;
      controller.abort();
      clearTimeout(pending);
    };
  }, [goalId, authenticated]);
  useEffect(() => {
    if (!authenticated || !['artifacts','memory','workers'].includes(page)) return;
    let current = true;
    const kinds = page === 'memory' ? ['memories','contexts'] : page === 'workers' ? ['runs'] : ['artifacts'];
    for (const kind of kinds) void api('/panel/collections/' + kind).then(value => {
      if(current) setCollections(previous => ({...previous,[kind]:value}));
    }).catch(e => { if(current) setError(e.message); });
    return () => {current=false;};
  }, [page, authenticated]);
  function collectionPager(kind: string) {
    const collection=collections[kind];
    if (!collection) return <p role="status">Carregando histórico…</p>;
    if (!collection.page.nextBefore) return null;
    return <button className="button small" onClick={async()=>{
      try {
        const next=await api('/panel/collections/'+kind+'?before='+collection.page.nextBefore);
        setCollections(previous=>({...previous,[kind]:{...next,rows:[...previous[kind].rows,...next.rows]}}));
      } catch(e:any) {setError(e.message);}
    }}>Carregar mais · {collection.rows.length}/{collection.page.total}</button>;
  }
  async function action(fn: () => Promise<any>) {
    setBusy(true);
    setError('');
    try {
      const value = await fn();
      await refresh();
      return value;
    } catch (e: any) {
      setError(e.message);
      return null;
    } finally {
      setBusy(false);
    }
  }
  function navigate(next: string) {
    history.replaceState(null, '', location.pathname + location.search + (next === 'autonomy' ? '#autonomy' : ''));
    setPage(next);
    setGoalId(null);
    setDetail(null);
    setMenuOpen(false);
  }
  function openGoal(id: string) {
    history.replaceState(null, '', '#goal=' + encodeURIComponent(id));
    setDetail(null);
    setGoalId(id);
    setPage('goals');
    setTab('overview');
  }
  async function demo() {
    const r = await action(() => api('/demo', 'POST', {}));
    if (r) openGoal(r.id);
  }
  const tasks = data?.tasks ?? [],
    runs = data?.runs ?? [],
    agents = data?.agents ?? [],
    goals = data?.goals ?? [];
  const activeRuns = runs.filter((r) => r.state === 'running'),
    // panelState omits accepted tasks; the per-goal count comes from the server.
    acceptedCount = goals
      .filter((g) => !g.is_demo)
      .reduce((n, g) => n + (g.accepted_count ?? 0), 0),
    inMotion = goals.filter(
      (g) => !g.is_demo && !g.merged_into && !['completed', 'cancelled', 'failed'].includes(g.state),
    ),
    waiting = tasks.filter(
      (t) => t.state.startsWith('waiting') || t.state.startsWith('blocked') || t.state === 'queued',
    );
  const healthy =
    !!data?.engine?.lastTick &&
    !data.engine.error &&
    Date.now() - new Date(data.engine.lastTick).getTime() < 15000;
  const search = (r: Row) =>
    !query || JSON.stringify(r).toLocaleLowerCase().includes(query.toLocaleLowerCase());
  const title = goalId
    ? 'Detalhe do objetivo'
    : (navItems.find((n) => n[0] === page)?.[1] ?? 'Visão geral');
  return (
    <div className="shell">
      <aside className={'sidebar ' + (menuOpen ? 'open' : '')}>
        <a
          className="brand"
          href="#"
          onClick={(e) => {
            e.preventDefault();
            navigate('dashboard');
          }}
        >
          <span className="brand-mark">
            <Network size={25} />
          </span>
          <span>
            AgentOS<small>Multi-Agent Platform</small>
          </span>
          <span className="version">0.1</span>
        </a>
        <div className="nav-label">WORKSPACE</div>
        <nav>
          {navItems.map(([key, label, Icon]) => (
            <button
              key={key}
              className={page === key ? 'selected' : ''}
              onClick={() => navigate(key)}
            >
              <Icon size={18} />
              <span>{label}</span>
              {key === 'goals' && goals.length > 0 && <small>{goals.length}</small>}
            </button>
          ))}
        </nav>
        <div className="sidebar-bottom">
          <div className="environment">
            <span className="pulse-dot" />
            <div>
              Ambiente privado<small>Dados e serviços próprios</small>
            </div>
          </div>
          <button className="sidebar-create" onClick={() => setModal('goal')}>
            <Plus size={17} />
            Novo objetivo
          </button>
          <div className="footer-label">
            <ShieldCheck size={13} />
            Produção preservada
          </div>
        </div>
      </aside>
      <div className="main-shell">
        <header className="topbar">
          <button
            className="mobile-menu icon-btn"
            aria-label="Abrir menu"
            onClick={() => setMenuOpen(!menuOpen)}
          >
            <ListTodo size={21} />
          </button>
          <label className="search">
            <Search size={17} />
            <input
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder="Buscar agentes, tarefas, artefatos..."
              aria-label="Buscar"
            />
            <kbd>
              <Command size={11} /> K
            </kbd>
          </label>
          <div className="topbar-right">
            <span className="env-tag">
              <span className="pulse-dot" />
              AgentOS
              <ChevronDown size={13} />
            </span>
            <button
              className="icon-btn notification"
              aria-label="Ver eventos"
              onClick={() => navigate('events')}
            >
              <Bell size={18} />
              {data?.events.length ? <i /> : null}
            </button>
            <div className="avatar">LC</div>
            <div className="user">
              Lucas<small>Operador</small>
            </div>
          </div>
        </header>
        <main>
          <div className="breadcrumb">
            Workspace
            <ChevronRight size={12} />
            <span>{title}</span>
          </div>
          {error && (
            <div role="alert" className="error-banner">
              <AlertTriangle size={17} />
              {error}
              <button aria-label="Fechar aviso" onClick={() => setError('')}>
                <X size={16} />
              </button>
            </div>
          )}
          {!data ? (
            <Empty
              icon={Loader2}
              title="Conectando ao control plane"
              description="Carregando o estado persistido do workspace."
            />
          ) : (
            <>
              {!goalId && page !== 'chat' && (
                <div className="page-heading">
                  <div>
                    <h1>{title}</h1>
                    <p>
                      {page === 'dashboard'
                        ? 'Uma visão clara do trabalho, do contexto e de cada entrega.'
                        : page === 'registry'
                          ? 'Especializações disponíveis. Instâncias são criadas somente quando há trabalho.'
                          : page === 'memory'
                            ? 'Conhecimento compartilhado, com origem, versão e contexto por execução.'
                            : 'Acompanhe o estado real deste ambiente.'}
                    </p>
                  </div>
                  <div className="page-actions">
                    {page === 'dashboard' ? (
                      <>
                        <span className={'health ' + (healthy ? '' : 'warn')}>
                          <i />
                          {healthy ? 'Control plane operacional' : 'Verificar control plane'}
                        </span>
                        <button className="button" onClick={() => setModal('goal')}>
                          <Plus size={16} />
                          Novo objetivo
                        </button>
                      </>
                    ) : page === 'memory' ? (
                      <button className="button primary" onClick={() => setModal('memory')}>
                        <Plus size={16} />
                        Adicionar memória
                      </button>
                    ) : page === 'accounts' ? (
                      <button className="button" onClick={() => setConnecting(true)}>
                        <Plus size={16} />
                        Conectar conta
                      </button>
                    ) : page === 'goals' ? (
                      <button className="button primary" onClick={() => setModal('goal')}>
                        <Plus size={16} />
                        Novo objetivo
                      </button>
                    ) : null}
                  </div>
                </div>
              )}
              {page === 'dashboard' && (
                <>
                  <div className="metrics">
                    {[
                      [
                        Users,
                        'Agentes definidos',
                        agents.length,
                        'Especializações no registro',
                        'blue',
                      ],
                      [
                        Activity,
                        'Execuções ativas',
                        activeRuns.length,
                        'Capacidade: ' + data.settings.capacity.maxRuns,
                        'teal',
                      ],
                      [
                        Play,
                        'Tarefas em execução',
                        tasks.filter((t) => t.state === 'running').length,
                        'Instâncias trabalhando agora',
                        'cyan',
                      ],
                      [
                        GitBranch,
                        'Na fila ou aguardando',
                        waiting.length,
                        'Dependências, recursos e cotas',
                        'purple',
                      ],
                      [
                        CheckCircle2,
                        'Entregas aceitas',
                        acceptedCount,
                        'Critérios realmente verificados',
                        'green',
                      ],
                      [Wallet, 'Custo reportado', '—', 'Sem telemetria de provedor', 'teal'],
                    ].map(([Icon, label, value, note, color]: any) => (
                      <div className="metric" key={label}>
                        <span className={'metric-icon ' + color}>
                          <Icon size={21} />
                        </span>
                        <div>
                          <span className="metric-label">{label}</span>
                          <strong>{value}</strong>
                          <small>{note}</small>
                        </div>
                      </div>
                    ))}
                  </div>
                  <div className="dashboard-main">
                    <Panel
                      title="Objetivos em movimento"
                      subtitle="Da intenção à entrega verificada"
                      action={
                        <button className="text-button" onClick={() => navigate('goals')}>
                          Ver todos
                          <ArrowRight size={13} />
                        </button>
                      }
                    >
                      <GoalsList
                        goals={inMotion.filter(search).slice(0, 4)}
                        tasks={tasks}
                        runs={runs}
                        accounts={data.accounts}
                        open={openGoal}
                        emptyAction={
                          <button className="button primary" disabled={busy} onClick={demo}>
                            <Play size={15} />
                            Executar exemplo isolado
                          </button>
                        }
                      />
                    </Panel>
                    <Panel title="Saúde da operação" subtitle="Estado atual dos componentes">
                      <div className="health-list">
                        {[
                          ['Scheduler', healthy ? 'Operacional' : 'Aguardando', healthy],
                          ['PostgreSQL', 'Conectado', true],
                          [
                            'Runtime Harness',
                            data.integrations.harness ? 'Configurado' : 'Não conectado',
                            data.integrations.harness,
                          ],
                          [
                            'Laya',
                            data.integrations.laya ? 'Configurado' : 'Seleção lexical',
                            false,
                          ],
                          [
                            'Recall',
                            data.integrations.recall ? 'Configurado' : 'Memória local',
                            false,
                          ],
                        ].map(([name, value, ok]: any) => (
                          <div key={name}>
                            <span>{name}</span>
                            <span className={ok ? 'status-ok' : 'status-muted'}>
                              <i />
                              {value}
                            </span>
                          </div>
                        ))}
                      </div>
                      <div className="inline-note">
                        <ShieldCheck size={16} />
                        <span>Este workspace não utiliza a produção do Harness.</span>
                      </div>
                    </Panel>
                  </div>
                  <div className="dashboard-triple">
                    <Panel title="Equipes" subtitle="Organização por responsabilidade">
                      {Object.keys(departmentLabels).map((d, i) => (
                        <div className="team-row" key={d}>
                          <span className={'team-icon tone-' + i}>
                            {
                              [
                                <Code2 size={19} />,
                                <Search size={19} />,
                                <Network size={19} />,
                                <ShieldCheck size={19} />,
                                <CheckCircle2 size={19} />,
                              ][i]
                            }
                          </span>
                          <div>
                            <strong>{departmentLabels[d]}</strong>
                            <small>
                              {agents.filter((a) => a.department === d).length} definições
                            </small>
                          </div>
                          <span className="team-count">
                            {
                              activeRuns.filter(
                                (r) =>
                                  agents.find(
                                    (a) => a.id === tasks.find((t) => t.id === r.task_id)?.agent_id,
                                  )?.department === d,
                              ).length
                            }
                            <small>ativos</small>
                          </span>
                        </div>
                      ))}
                    </Panel>
                    <Panel
                      title="Eventos recentes"
                      subtitle="Rastro persistido de cada transição"
                      action={
                        <button className="text-button" onClick={() => navigate('events')}>
                          Ver todos
                          <ArrowRight size={13} />
                        </button>
                      }
                    >
                      <EventList events={data.events.slice(0, 6)} />
                    </Panel>
                    <Panel
                      title="Contexto compartilhado"
                      subtitle="Memórias entregues a cada execução"
                    >
                      <div className="memory-overview">
                        <span className="memory-visual">
                          <Database size={34} />
                          <span>
                            <Sparkles size={15} />
                          </span>
                        </span>
                        <strong>
                          {data.memories.length}
                          <small>memórias neste ambiente</small>
                        </strong>
                      </div>
                      <div className="memory-summary">
                        <span>
                          Pacotes de contexto<strong>{data.contexts.length}</strong>
                        </span>
                        <span>
                          Sincronizadas no Recall<strong>{data.recall.synced}</strong>
                        </span>
                        <span>
                          Selecionador
                          <strong>
                            {data.integrations.laya ? 'Laya configurado' : 'Lexical · fallback'}
                          </strong>
                        </span>
                      </div>
                      <button className="button full" onClick={() => navigate('memory')}>
                        Explorar memória
                        <ArrowRight size={14} />
                      </button>
                    </Panel>
                  </div>
                  <Panel
                    title="Tarefas recentes"
                    subtitle="Execução e validação são etapas distintas"
                  >
                    <TaskTable
                      tasks={tasks.filter(search).slice(-8).reverse()}
                      agents={agents}
                      open={(t) => openGoal(t.goal_id)}
                    />
                  </Panel>
                </>
              )}
              {page === 'chat' && (
                <SimpleChat
                  onConnect={() => setConnecting(true)}
                  onGoals={() => navigate('goals')}
                />
              )}
              {page === 'autonomy' && <Autonomy api={api} />}
              {page === 'cycles' && (
                <Cycles
                  onManager={(message) => {
                    sessionStorage.setItem('agentos-manager-draft', message);
                    navigate('chat');
                  }}
                />
              )}
              {page === 'goals' && !goalId && (
                <Panel
                  title="Todos os objetivos"
                  subtitle={`${goals.length} objetivos persistidos`}
                  action={
                    <button disabled={busy} className="button" onClick={demo}>
                      <Play size={14} />
                      Exemplo técnico
                    </button>
                  }
                >
                  <GoalDirectory goals={goals.filter(search)} data={data} open={openGoal} />
                </Panel>
              )}
              {goalId && detail && (
                <GoalDetail
                  key={detail.goal.id}
                  detail={detail}
                  data={data}
                  tab={tab}
                  setTab={setTab}
                  onBack={() => {
                    history.replaceState(null, '', location.pathname + location.search);
                    setGoalId(null);
                    setDetail(null);
                  }}
                  onAction={action}
                  onContext={setSelectedContext}
                />
              )}
              {goalId && !detail && (
                <Empty icon={Loader2} title={detailError ? 'Não foi possível abrir o objetivo' : 'Carregando o objetivo'}
                  description={detailError || 'Consultando tarefas, bloqueios e entregas atuais.'} />
              )}
              {goalId && detail && detailError && <p role="status">Última atualização falhou: {detailError}. Exibindo o último retrato disponível.</p>}
              {page === 'organization' && (
                <>
                  <div className="org-root">
                    <span className="avatar large">LC</span>
                    <h3>Operador</h3>
                    <p>Objetivos, prioridades e decisões</p>
                  </div>
                  <div className="org-accounts">
                    {data.accounts
                      .filter((a) => a.enabled && a.provider !== 'local')
                      .map((a) => {
                        const live = runs.filter(
                          (r) =>
                            r.account_id === a.id &&
                            ['queued', 'running', 'cancelling', 'reconciling'].includes(r.state),
                        );
                        return (
                          <Panel
                            key={a.id}
                            title={a.label}
                            subtitle={`${a.model} · ${live.length}/${a.max_concurrency} vagas em uso`}
                          >
                            <div className="org-load">
                              <i
                                style={{
                                  width:
                                    Math.min(100, (live.length / Math.max(1, a.max_concurrency)) * 100) +
                                    '%',
                                }}
                              />
                            </div>
                            {live.map((r) => {
                              const task = tasks.find((t) => t.id === r.task_id);
                              const goal = goals.find((g) => g.id === task?.goal_id);
                              return (
                                <button
                                  className="org-live-run"
                                  key={r.id}
                                  onClick={() => goal && openGoal(goal.id)}
                                >
                                  <Cpu size={15} />
                                  <span>
                                    <strong>{task?.title ?? r.task_id}</strong>
                                    <small>
                                      {goal?.project ?? 'objetivo'} · {runRoute(r, data.accounts)}
                                    </small>
                                  </span>
                                  <small>
                                    {r.state === 'running' ? 'há ' + elapsed(r.started_at) : 'na fila'}
                                    {r.heartbeat_at ? ' · sinal há ' + elapsed(r.heartbeat_at) : ''}
                                  </small>
                                </button>
                              );
                            })}
                            {!live.length && <p className="org-idle">Nenhuma tarefa nesta conta agora.</p>}
                          </Panel>
                        );
                      })}
                  </div>
                  <h3 className="org-section-title">Especializações disponíveis</h3>
                  <div className="department-grid">
                    {Object.entries(departmentLabels).map(([d, label]) => (
                      <Panel key={d} title={String(label)} subtitle="Supervisor sob demanda">
                        <div className="supervisor">
                          <Network size={20} />
                          <div>
                            {d}.supervisor<small>Decisões semânticas da equipe</small>
                          </div>
                        </div>
                        <div className="org-agents">
                          {agents
                            .filter((a) => a.department === d && !a.id.endsWith('supervisor'))
                            .map((a) => (
                              <div key={a.id}>
                                <span className="tiny-dot" />
                                <span>{a.name}</span>
                              </div>
                            ))}
                        </div>
                      </Panel>
                    ))}
                  </div>
                </>
              )}
              {page === 'registry' && (
                <Panel
                  title="Catálogo de especializações"
                  subtitle={`${agents.filter(search).length} definições · nenhum processo ocioso necessário`}
                >
                  <div className="registry-grid">
                    {agents.filter(search).map((a) => (
                      <article className="agent-card" key={a.id}>
                        <div className="agent-card-top">
                          <span
                            className={
                              'team-icon tone-' +
                              Object.keys(departmentLabels).indexOf(a.department)
                            }
                          >
                            <Code2 size={18} />
                          </span>
                          <span className="runtime-tag">
                            {a.runtime === 'builtin' ? 'Validação técnica' : 'DeepSeek Harness'}
                          </span>
                        </div>
                        <h3>{a.name}</h3>
                        <code>{a.id}</code>
                        <p>{a.description}</p>
                        <div className="tags">
                          {a.capabilities.map((c: string) => (
                            <span key={c}>{c}</span>
                          ))}
                        </div>
                        <footer>
                          <span>
                            v{a.version} · {a.department}
                          </span>
                          <Badge state={a.status} />
                        </footer>
                      </article>
                    ))}
                  </div>
                </Panel>
              )}
              {page === 'workers' && (
                <Panel
                  title="Histórico de execuções"
                  subtitle="Cada tentativa tem sua própria identidade e contexto"
                >
                  <div className="table-scroll">
                    <table>
                      <thead>
                        <tr>
                          <th>Execução</th>
                          <th>Tarefa</th>
                          <th>Runtime</th>
                          <th>Conta</th>
                          <th>Estado</th>
                          <th>Tentativa</th>
                          <th>Contexto</th>
                        </tr>
                      </thead>
                      <tbody>
                        {runs
                          .filter(search)
                          .slice()
                          .reverse()
                          .map((r) => (
                            <tr key={r.id}>
                              <td>
                                <code>{r.id.slice(0, 8)}</code>
                              </td>
                              <td>{r.task_title ?? tasks.find((t) => t.id === r.task_id)?.title}</td>
                              <td>{r.runtime === 'builtin' ? 'Validação local' : 'Harness'}</td>
                              <td>{r.account_id}</td>
                              <td>
                                <Badge state={r.state} />
                              </td>
                              <td>{r.attempt}</td>
                              <td>
                                <button
                                  className="text-button"
                                  disabled={!r.context_id}
                                  onClick={async () => {
                                    try { setSelectedContext(await api('/panel/contexts/'+r.context_id)); }
                                    catch(e:any){setError(e.message);}
                                  }}
                                >
                                  Inspecionar
                                  <ArrowRight size={12} />
                                </button>
                              </td>
                            </tr>
                          ))}
                      </tbody>
                    </table>
                  </div>
                  {collectionPager('runs')}
                  {!runs.length && collections.runs && (
                    <Empty
                      title="Nenhuma execução iniciada"
                      description="As instâncias aparecem aqui quando uma tarefa pronta é despachada."
                    />
                  )}
                </Panel>
              )}
              {page === 'artifacts' && (
                <Panel
                  title="Entregas e evidências"
                  subtitle="Arquivos preservados com integridade SHA-256"
                >
                  <div className="artifact-grid">
                    {data.artifacts.filter(search).map((a) => (
                      <article className="artifact-card" key={a.id}>
                        <FileText size={26} />
                        <div>
                          <h3>{a.name}</h3>
                          <small>
                            {(a.size / 1024).toFixed(1)} KB · {time(a.created_at)}
                          </small>
                          <code>SHA256 {a.digest.slice(0, 18)}…</code>
                        </div>
                        <a
                          className="icon-btn"
                          aria-label={'Baixar ' + a.name}
                          href={'/api/artifacts/' + a.id}
                        >
                          <Download size={18} />
                        </a>
                      </article>
                    ))}
                  </div>
                  {collectionPager('artifacts')}
                  {!data.artifacts.length && collections.artifacts && (
                    <Empty
                      icon={FileText}
                      title="As entregas ficam aqui"
                      description="Cada arquivo estará vinculado à execução que o produziu e aos critérios que comprova."
                    />
                  )}
                </Panel>
              )}
              {page === 'memory' && (
                <>
                  <div className="memory-banner">
                    <span className="team-icon">
                      <Database size={24} />
                    </span>
                    <div>
                      <h3>Uma base compartilhada. Contexto específico para cada tarefa.</h3>
                      <p>
                        Decisões obrigatórias são preservadas. O selecionador ordena os demais
                        trechos por relevância.
                      </p>
                    </div>
                    <span className="runtime-tag">
                      {data.integrations.recall
                        ? 'Recall configurado'
                        : 'Armazenamento local isolado'}
                    </span>
                  </div>
                  <div className="two-col">
                    <Panel
                      title="Conhecimento registrado"
                      subtitle="Origem e versão acompanham cada registro"
                    >
                      <div className="memory-list">
                        {data.memories
                          .filter(search)
                          .slice()
                          .reverse()
                          .map((m) => (
                            <article key={m.id}>
                              <div>
                                <span className="tag">
                                  {m.kind === 'decision'
                                    ? 'Decisão'
                                    : m.kind === 'evidence'
                                      ? 'Evidência'
                                      : 'Observação'}
                                </span>
                                <small>
                                  {m.project} · v{m.version}
                                </small>
                              </div>
                              <h3>{m.title}</h3>
                              <p>
                                {m.content.length > 180 ? m.content.slice(0, 180) + '…' : m.content}
                              </p>
                              <footer>
                                <Badge state={m.verification} />
                                <span>{m.source}</span>
                              </footer>
                            </article>
                          ))}
                      </div>
                      {collectionPager('memories')}
                      {!data.memories.length && collections.memories && (
                        <Empty
                          icon={Database}
                          title="Comece pelas decisões importantes"
                          description="Adicione contexto ao projeto e ele será considerado automaticamente nas próximas execuções."
                        />
                      )}
                    </Panel>
                    <Panel
                      title="Contexto entregue aos agentes"
                      subtitle="Inspecione a seleção exata de cada tentativa"
                    >
                      <div className="context-list">
                        {data.contexts
                          .slice()
                          .reverse()
                          .map((c) => (
                            <button key={c.id} onClick={async () => {
                              try { setSelectedContext(c.manifest.summary ? await api('/panel/contexts/'+c.id) : c); }
                              catch(e:any){setError(e.message);}
                            }}>
                              <span className="context-icon">
                                <BookOpen size={18} />
                              </span>
                              <div>
                                <strong>
                                  {tasks.find(
                                    (t) => t.id === runs.find((r) => r.id === c.run_id)?.task_id,
                                  )?.title ?? c.run_id.slice(0, 8)}
                                </strong>
                                <small>
                                  {c.manifest.selected.length} memórias · {c.manifest.selector} · ~
                                  {c.manifest.estimatedTokens} tokens
                                </small>
                              </div>
                              <ChevronRight size={15} />
                            </button>
                          ))}
                      </div>
                      {collectionPager('contexts')}
                      {!data.contexts.length && collections.contexts && (
                        <Empty
                          icon={BookOpen}
                          title="Nenhum pacote montado ainda"
                          description="O contexto é preparado automaticamente quando uma execução começa."
                        />
                      )}
                    </Panel>
                  </div>
                </>
              )}
              {page === 'accounts' && <ConnectionsPanel onConnect={() => setConnecting(true)} />}
              {page === 'events' && (
                <Panel
                  title="Linha do tempo persistida"
                  subtitle="Eventos de estado, memória, execução e aceite"
                >
                  <EventList events={data.events.filter(search)} expanded />
                </Panel>
              )}
              {page === 'settings' && (
                <div className="two-col">
                  <Panel
                    title="Capacidade da operação"
                    subtitle="Vagas só são preenchidas quando há tarefas prontas"
                  >
                    <form
                      className="form"
                      onSubmit={(e) => {
                        e.preventDefault();
                        const f = new FormData(e.currentTarget);
                        void action(() =>
                          api('/capacity', 'PATCH', {
                            maxRuns: Number(f.get('maxRuns')),
                            browserSlots: Number(f.get('browserSlots')),
                          }),
                        );
                      }}
                    >
                      <label>
                        Execuções simultâneas
                        <input
                          name="maxRuns"
                          type="number"
                          min="1"
                          max="100"
                          defaultValue={data.settings.capacity.maxRuns}
                        />
                      </label>
                      <label>
                        Vagas de navegador
                        <input
                          name="browserSlots"
                          type="number"
                          min="0"
                          max="32"
                          defaultValue={data.settings.capacity.browserSlots}
                        />
                      </label>
                      <button className="button primary" disabled={busy}>
                        Salvar capacidade
                      </button>
                      <p className="form-note">
                        Este processo possui {data.engine?.workerSlots ?? 16} consumidores. Aumentar
                        o limite global não cria consumidores nem adiciona recursos à máquina.
                      </p>
                    </form>
                  </Panel>
                  <Panel
                    title="Isolamento do ambiente"
                    subtitle="Recursos independentes do Harness atual"
                  >
                    <div className="settings-list">
                      <div>
                        <span>Ambiente</span>
                        <strong>AgentOS privado</strong>
                      </div>
                      <div>
                        <span>Banco</span>
                        <code>agentos</code>
                      </div>
                      <div>
                        <span>Execução</span>
                        <code>Serviços independentes</code>
                      </div>
                      <div>
                        <span>Controle de custo</span>
                        <strong>Sem teto arbitrário de tokens</strong>
                      </div>
                      <div>
                        <span>Dados operacionais</span>
                        <strong>PostgreSQL + fila durável</strong>
                      </div>
                      <div>
                        <span>Produção</span>
                        <strong>Fora deste workspace</strong>
                      </div>
                    </div>
                  </Panel>
                </div>
              )}
              <footer className="main-footer">
                <span>
                  <ShieldCheck size={13} />
                  AgentOS · Ambiente privado
                </span>
                <span>Atualizado {time(data.serverTime)} · Estado persistido</span>
              </footer>
            </>
          )}
        </main>
      </div>
      {connecting && (
        <ConnectAccountDialog
          onClose={() => {
            setConnecting(false);
            void refresh();
          }}
        />
      )}
      {modal && (
        <Modal
          title={
            modal === 'goal'
              ? 'Novo objetivo'
              : modal === 'memory'
                ? 'Registrar conhecimento'
                : 'Adicionar rota de execução'
          }
          onClose={() => setModal(null)}
        >
          <form
            className="form"
            onSubmit={async (e) => {
              e.preventDefault();
              const f = new FormData(e.currentTarget);
              const values = Object.fromEntries(f.entries());
              let r;
              if (modal === 'goal') {
                let tasks;
                try {
                  tasks = JSON.parse(String(values.plan));
                } catch {
                  setError('Plano JSON inválido.');
                  return;
                }
                r = await action(() =>
                  api('/goals', 'POST', {
                    title: values.title,
                    description: values.description,
                    project: values.project,
                    idempotencyKey: crypto.randomUUID(),
                    tasks,
                  }),
                );
                if (r) openGoal(r.id);
              } else if (modal === 'memory')
                r = await action(() =>
                  api('/memories', 'POST', { ...values, source: 'local-operator' }),
                );
              else
                r = await action(() =>
                  api('/accounts', 'POST', {
                    id: values.id,
                    label: values.label,
                    provider: values.provider,
                    model: values.model,
                    maxConcurrency: Number(values.maxConcurrency),
                    enabled: values.enabled === 'on',
                    allowUnknown: values.allowUnknown === 'on',
                  }),
                );
              if (r) setModal(null);
            }}
          >
            {modal === 'goal' ? (
              <>
                <label>
                  Nome do objetivo
                  <input
                    name="title"
                    required
                    minLength={3}
                    placeholder="O que precisamos entregar?"
                  />
                </label>
                <label>
                  Projeto
                  <input name="project" required pattern="[a-z0-9_-]+" defaultValue="meu-projeto" />
                </label>
                <label>
                  Contexto e escopo
                  <textarea
                    name="description"
                    rows={3}
                    placeholder="Descreva o resultado esperado e as restrições."
                  />
                </label>
                <label>
                  Plano de tarefas <small>JSON · dependências por chave</small>
                  <textarea
                    className="code-input"
                    name="plan"
                    rows={10}
                    defaultValue={JSON.stringify(
                      [
                        {
                          key: 'analysis',
                          title: 'Analisar o objetivo',
                          description: 'Investigar e produzir análise com fontes.',
                          agentId: 'research.documentation',
                          criteria: [
                            {
                              id: 'analysis',
                              label: 'Documento de análise entregue',
                              type: 'artifact',
                              name: 'analysis.txt',
                            },
                            { id: 'review', label: 'Revisão do conteúdo', type: 'review' },
                          ],
                        },
                      ],
                      null,
                      2,
                    )}
                  />
                </label>
                <p className="form-note">
                  As tarefas de IA aguardam um runtime isolado e uma conta configurada. O exemplo
                  técnico da visão geral pode ser executado agora.
                </p>
              </>
            ) : modal === 'memory' ? (
              <>
                <label>
                  Título
                  <input name="title" required minLength={3} />
                </label>
                <label>
                  Projeto
                  <input name="project" required pattern="[a-z0-9_-]+" defaultValue="meu-projeto" />
                </label>
                <label>
                  Conhecimento
                  <textarea name="content" required rows={6} minLength={3} />
                </label>
                <div className="form-row">
                  <label>
                    Tipo
                    <select name="kind">
                      <option value="decision">Decisão</option>
                      <option value="observation">Observação</option>
                      <option value="evidence">Evidência</option>
                    </select>
                  </label>
                  <label>
                    Verificação
                    <select name="verification">
                      <option value="confirmed">Confirmado</option>
                      <option value="unverified">Não verificado</option>
                    </select>
                  </label>
                </div>
                <p className="form-note">
                  Decisões confirmadas são incluídas obrigatoriamente nas execuções deste projeto.
                  Não registre chaves ou senhas.
                </p>
              </>
            ) : (
              <>
                <label>
                  Identificador
                  <input
                    name="id"
                    required
                    pattern="[a-z0-9_-]+"
                    placeholder="codex-desenvolvimento"
                  />
                </label>
                <label>
                  Nome
                  <input name="label" required placeholder="Conta de desenvolvimento" />
                </label>
                <div className="form-row">
                  <label>
                    Provider
                    <input name="provider" required placeholder="deepseek-official" />
                  </label>
                  <label>
                    Modelo
                    <input name="model" required placeholder="modelo configurado no runtime" />
                  </label>
                </div>
                <label>
                  Vagas
                  <input name="maxConcurrency" type="number" min="1" max="32" defaultValue="1" />
                </label>
                <label className="checkbox">
                  <input type="checkbox" name="enabled" />
                  Habilitar rota
                </label>
                <label className="checkbox">
                  <input type="checkbox" name="allowUnknown" />
                  Permitir uso com cota desconhecida
                </label>
                <p className="form-note">
                  Registrar a rota não autentica uma conta. As credenciais e o runtime são
                  configurados separadamente neste projeto.
                </p>
              </>
            )}
            <button className="button primary" disabled={busy}>
              {busy ? <Loader2 size={16} /> : <Plus size={16} />}Salvar
            </button>
          </form>
        </Modal>
      )}
      {selectedContext && (
        <Modal title="Contexto desta execução" onClose={() => setSelectedContext(null)} wide>
          <div className="context-meta">
            <span className="tag">{selectedContext.manifest.selector}</span>
            <span>~{selectedContext.manifest.estimatedTokens} tokens estimados</span>
            <span>{selectedContext.manifest.selected.length} memórias</span>
          </div>
          {selectedContext.manifest.fallbackReason && (
            <p className="form-note">{selectedContext.manifest.fallbackReason}</p>
          )}
          <pre className="context-content">
            {selectedContext.content ||
              'Nenhuma memória pertinente disponível. O contrato da tarefa continua presente.'}
          </pre>
          <h3>Manifesto de seleção</h3>
          <pre className="context-json">{JSON.stringify(selectedContext.manifest, null, 2)}</pre>
        </Modal>
      )}
    </div>
  );
}
function GoalsList({
  goals,
  tasks,
  runs,
  accounts,
  open,
  emptyAction,
}: {
  goals: Row[];
  tasks: Row[];
  runs: Row[];
  accounts: Row[];
  open: (id: string) => void;
  emptyAction?: React.ReactNode;
}) {
  return goals.length ? (
    <div className="goals-list">
      {goals.map((g) => {
        const list = tasks.filter((t) => t.goal_id === g.id),
          done = g.accepted_count ?? 0,
          total = g.task_count ?? list.length,
          progress = Math.round((done / Math.max(1, total)) * 100),
          live = runs.filter(
            (r) => r.state === 'running' && list.some((t) => t.id === r.task_id),
          );
        return (
          <button key={g.id} className="goal-row" onClick={() => open(g.id)}>
            <span className="goal-symbol">
              <GitBranch size={20} />
            </span>
            <div className="goal-main">
              <div>
                <h3>{g.title}</h3>
                {g.is_demo && <span className="demo-tag">Exemplo técnico</span>}
              </div>
              <p>
                {g.project} · {total} tarefas · criado há {relative(g.created_at)}
              </p>
              {live.map((r) => (
                <p className="goal-live-route" key={r.id}>
                  <Cpu size={13} />
                  {runRoute(r, accounts)} · {list.find((t) => t.id === r.task_id)?.title}
                </p>
              ))}
              <div className="goal-progress">
                <div className="progress-track">
                  <i style={{ width: progress + '%' }} />
                </div>
                <span>
                  {done}/{total} aceitas
                </span>
              </div>
            </div>
            <Badge state={g.state} />
            <ChevronRight size={16} />
          </button>
        );
      })}
    </div>
  ) : (
    <Empty
      icon={GitBranch}
      title="Seu próximo objetivo começa aqui"
      description="Crie uma entrega ou execute o exemplo isolado para acompanhar a fila, a memória e a validação funcionando juntas."
      action={emptyAction}
    />
  );
}
function TaskTable({
  tasks,
  agents,
  open,
}: {
  tasks: Row[];
  agents: Row[];
  open: (t: Row) => void;
}) {
  return tasks.length ? (
    <div className="table-scroll">
      <table>
        <thead>
          <tr>
            <th>Tarefa</th>
            <th>Estado</th>
            <th>Agente</th>
            <th>Tentativas</th>
            <th>Última atualização</th>
          </tr>
        </thead>
        <tbody>
          {tasks.map((t) => (
            <tr key={t.id} onClick={() => open(t)} className="clickable">
              <td>
                <strong>{t.title}</strong>
                <small className="table-sub">{t.reason ?? t.key}</small>
              </td>
              <td>
                <Badge state={t.state} />
              </td>
              <td>{agents.find((a) => a.id === t.agent_id)?.name ?? t.agent_id}</td>
              <td>{t.attempt_count}</td>
              <td>{relative(t.updated_at)} atrás</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  ) : (
    <Empty
      icon={ListTodo}
      title="Nenhuma tarefa neste momento"
      description="O plano de um objetivo define as tarefas e suas dependências."
    />
  );
}
function EventList({ events, expanded = false }: { events: Row[]; expanded?: boolean }) {
  return events.length ? (
    <div className={'event-list ' + (expanded ? 'expanded' : '')}>
      {events.map((e) => (
        <div className="event-row" key={e.id}>
          <time>{time(e.created_at)}</time>
          <i
            className={
              e.kind.includes('ACCEPT') || e.kind.includes('COMPLETED')
                ? 'green'
                : e.kind.includes('FAIL')
                  ? 'red'
                  : 'blue'
            }
          />
          <div>
            <strong>{e.kind.replaceAll('_', ' ')}</strong>
            <p>
              {e.payload.title ??
                e.payload.reason ??
                (e.payload.runId
                  ? 'Execução ' + e.payload.runId.slice(0, 8)
                  : (e.entity_id?.slice(0, 8) ?? 'Sistema'))}
            </p>
            {expanded && <code>{JSON.stringify(e.payload)}</code>}
          </div>
        </div>
      ))}
    </div>
  ) : (
    <Empty
      icon={Activity}
      title="O histórico começa com o trabalho"
      description="Cada transição será registrada de forma persistente."
    />
  );
}
function Dag({ tasks, dependencies }: { tasks: Row[]; dependencies: Row[] }) {
  const cols = new Map<string, number>();
  function depth(id: string, seen = new Set<string>()): number {
    if (cols.has(id)) return cols.get(id)!;
    if (seen.has(id)) return 0;
    seen.add(id);
    const parents = dependencies
      .filter((d) => d.task_id === id)
      .map((d) => depth(d.depends_on, new Set(seen)));
    const n = parents.length ? Math.max(...parents) + 1 : 0;
    cols.set(id, n);
    return n;
  }
  tasks.forEach((t) => depth(t.id));
  const used: Row = {},
    positions = new Map<string, { x: number; y: number }>();
  for (const t of tasks) {
    const c = cols.get(t.id)!;
    positions.set(t.id, { x: 28 + c * 225, y: 25 + (used[c] ?? 0) * 108 });
    used[c] = (used[c] ?? 0) + 1;
  }
  const width = Math.max(640, (Math.max(0, ...cols.values()) + 1) * 225 + 40),
    height = Math.max(230, ...Object.values(used).map((n: any) => n * 108 + 35));
  return (
    <div className="dag-scroll">
      <div className="dag-canvas" style={{ width, height }}>
        <svg width={width} height={height}>
          <defs>
            <marker
              id="arrow"
              viewBox="0 0 10 10"
              refX="9"
              refY="5"
              markerWidth="5"
              markerHeight="5"
              orient="auto-start-reverse"
            >
              <path d="M 0 0 L 10 5 L 0 10 z" fill="#465e78" />
            </marker>
          </defs>
          {dependencies.map((d) => {
            const a = positions.get(d.depends_on),
              b = positions.get(d.task_id);
            if (!a || !b) return null;
            return (
              <path
                key={d.task_id + d.depends_on}
                d={`M${a.x + 190},${a.y + 35} C${a.x + 210},${a.y + 35} ${b.x - 20},${b.y + 35} ${b.x},${b.y + 35}`}
                fill="none"
                stroke={
                  tasks.find((t) => t.id === d.depends_on)?.state === 'accepted'
                    ? '#2baf87'
                    : '#465e78'
                }
                strokeWidth="1.5"
                markerEnd="url(#arrow)"
              />
            );
          })}
        </svg>
        {tasks.map((t) => {
          const p = positions.get(t.id)!;
          return (
            <div className={'dag-node ' + t.state} key={t.id} style={{ left: p.x, top: p.y }}>
              <span>{t.key}</span>
              <strong>{t.title}</strong>
              <small>
                {t.state === 'accepted' ? (
                  <Check size={12} />
                ) : t.state === 'running' ? (
                  <Loader2 size={12} />
                ) : (
                  <Clock size={12} />
                )}{' '}
                {labels[t.state]}
              </small>
            </div>
          );
        })}
      </div>
    </div>
  );
}
function GoalDetail({
  detail,
  data,
  tab,
  setTab,
  onBack,
  onAction,
  onContext,
}: {
  detail: Row;
  data: State;
  tab: string;
  setTab: (s: string) => void;
  onBack: () => void;
  onAction: (fn: () => Promise<any>) => Promise<any>;
  onContext: (c: Row) => void;
}) {
  const { goal, tasks, dependencies, runs, acceptances } = detail;
  const [selectedTaskId, setSelectedTaskId] = useState<string | null>(null);
  const [relatedDetail, setRelatedDetail] = useState<Row | null>(null);
  const [inspectionError, setInspectionError] = useState('');
  const [taskEvidence, setTaskEvidence] = useState<Row | null>(null);
  const [tabEvidence,setTabEvidence]=useState<Row | null>(null);
  const [tabError,setTabError]=useState('');
  useEffect(()=>{
    setTabEvidence(null);setTabError('');
    const kind=tab==='artifacts'?'artifacts':tab==='context'?'contexts':tab==='runs'?'runs':null;
    if(!kind) return;
    let current=true;
    void api('/panel/collections/'+kind+'?goalId='+goal.id).then(value=>{
      if(current) setTabEvidence({...value,kind});
    }).catch(e=>{if(current)setTabError(e.message);});
    return ()=>{current=false;};
  },[tab,goal.id]);
  data={...data,tasks:[...data.tasks.filter((t:Row)=>t.goal_id!==goal.id),...tasks],
    runs:[...new Map([...data.runs,...runs].map((r:Row)=>[r.id,r])).values()]};
  const selectedTask =
    (taskEvidence?.task.id===selectedTaskId ? taskEvidence.task : undefined) ??
    tasks.find((t: Row) => t.id === selectedTaskId) ??
    data.tasks.find((t: Row) => t.id === selectedTaskId);
  const relatedGoalId =
    selectedTask && selectedTask.goal_id !== goal.id ? selectedTask.goal_id : null;
  useEffect(() => {
    let current = true;
    setInspectionError('');
    if (relatedGoalId)
      void api('/panel/goals/' + relatedGoalId)
        .then((value) => {
          if (current) setRelatedDetail(value);
        })
        .catch((e) => {
          if (current) setInspectionError(e.message);
        });
    return () => {
      current = false;
    };
  }, [relatedGoalId]);
  useEffect(() => {
    setTaskEvidence(null);
    if (!selectedTaskId) return;
    let current = true, timer: ReturnType<typeof setTimeout> | undefined;
    const controller = new AbortController();
    const load = async () => {
      try {
        const value = await api('/panel/tasks/' + selectedTaskId,'GET',undefined,controller.signal);
        if(current) setTaskEvidence(previous => {
          if(!previous || previous.task.id!==value.task.id) return value;
          const runs=[...new Map([...previous.runs,...value.runs].map((r:Row)=>[r.id,r])).values()];
          return {...value,runs,acceptances:[...new Map([...previous.acceptances,...value.acceptances].map((a:Row)=>[a.id ?? a.run_id,a])).values()],
            artifacts:[...new Map([...previous.artifacts,...value.artifacts].map((a:Row)=>[a.id,a])).values()],
            page:{total:value.page.total,nextBefore:runs.length<value.page.total ? Math.min(...runs.map((r:Row)=>r.attempt)) : null}};
        });
      } catch(e:any) {if(current) setInspectionError(e.message);}
      finally {if(current) timer=setTimeout(()=>void load(),6000);}
    };
    void load();
    return ()=>{current=false;controller.abort();clearTimeout(timer);};
  },[selectedTaskId]);
  const baseInspectionDetail = relatedGoalId
    ? relatedDetail?.goal.id === relatedGoalId
      ? relatedDetail
      : null
    : detail;
  const inspectionDetail=baseInspectionDetail && taskEvidence?.task.id===selectedTaskId
    ? {...baseInspectionDetail,runs:[...baseInspectionDetail.runs.filter((r:Row)=>r.task_id!==selectedTaskId),...taskEvidence.runs],
      acceptances:[...new Map([...baseInspectionDetail.acceptances,...taskEvidence.acceptances].map((a:Row)=>[a.id ?? a.run_id,a])).values()]} : baseInspectionDetail;
  const inspectedTask=taskEvidence?.task.id===selectedTaskId ? taskEvidence.task : selectedTask;
  const inspectionData=taskEvidence ? {...data,artifacts:[...new Map([...data.artifacts,...taskEvidence.artifacts].map((a:Row)=>[a.id,a])).values()]} : data;
  const accepted = tasks.filter((t: Row) => t.state === 'accepted').length,
    progress = Math.round((accepted / Math.max(1, tasks.length)) * 100);
  const ids = new Set(runs.map((r: Row) => r.id)),
    artifacts = tabEvidence?.kind==='artifacts' ? tabEvidence.rows : data.artifacts.filter((a) => ids.has(a.run_id)),
    contexts = tabEvidence?.kind==='contexts' ? tabEvidence.rows : data.contexts.filter((c) => ids.has(c.run_id));
  return (
    <>
      <button className="text-button back" onClick={onBack}>
        <ArrowDownLeft size={14} />
        Todos os objetivos
      </button>
      <div className="detail-heading">
        <div>
          <div className="detail-id">
            <GitBranch size={21} />
            <span>OBJ-{goal.id.slice(0, 8).toUpperCase()}</span>
            <Badge state={goal.state} />
            {goal.is_demo && <span className="demo-tag">Exemplo técnico isolado</span>}
          </div>
          <h1>{goal.title}</h1>
          <details className="original-request">
            <summary>Ver pedido original</summary>
            <p>{goal.description}</p>
          </details>
          <div className="tags">
            <span>{goal.project}</span>
            <span>{tasks.length} tarefas</span>
          </div>
        </div>
        <div className="detail-progress">
          {tab !== 'overview' && (
            <>
              <small>Etapas aceitas</small>
              <strong>{progress}%</strong>
              <div className="progress-track">
                <i style={{ width: progress + '%' }} />
              </div>
              <span>
                {accepted} de {tasks.length} tarefas aceitas
              </span>
            </>
          )}
          {['active', 'paused'].includes(goal.state) && (
            <button
              className="button small"
              style={{ marginTop: 12 }}
              onClick={() =>
                void onAction(() =>
                  api('/goals/' + goal.id + '/state', 'POST', {
                    state: goal.state === 'paused' ? 'active' : 'paused',
                  }),
                )
              }
            >
              {goal.state === 'paused' ? <Play size={12} /> : <PauseCircle size={12} />}
              {goal.state === 'paused' ? 'Retomar despachos' : 'Pausar novos despachos'}
            </button>
          )}
        </div>
      </div>
      <div className="tabs">
        {[
          ['overview', 'Visão geral'],
          ['tree', 'Árvore de execução'],
          ['plan', 'Plano'],
          ['runs', 'Execuções'],
          ['artifacts', 'Artefatos'],
          ['context', 'Memória e contexto'],
          ['events', 'Eventos'],
        ].map(([k, l]) => (
          <button key={k} onClick={() => setTab(k)} className={tab === k ? 'active' : ''}>
            {l}
          </button>
        ))}
      </div>
      {tab === 'overview' && (
        <GoalOverview detail={detail} data={data} inspect={setSelectedTaskId} />
      )}
      {tab === 'tree' && (
        <ExecutionTreeView
          detail={detail}
          data={data}
          inspect={setSelectedTaskId}
          loadTask={(id, signal) => api('/panel/tasks/' + id, 'GET', undefined, signal)}
        />
      )}
      {tab === 'plan' && (
        <>
          <Panel title="Plano de execução" subtitle="As dependências são liberadas após aceite">
            <Dag tasks={tasks} dependencies={dependencies} />
          </Panel>
          <Panel title="Estado de cada tarefa">
            <TaskTable tasks={tasks} agents={data.agents} open={(t) => setSelectedTaskId(t.id)} />
          </Panel>
        </>
      )}
      {selectedTask && (
        <Modal title={selectedTask.title} onClose={() => setSelectedTaskId(null)} wide>
          {inspectionError && (
            <p role="alert">Não foi possível carregar a causa: {inspectionError}</p>
          )}
          {inspectionDetail ? (
            <TaskInspection
              task={inspectedTask!}
              detail={inspectionDetail}
              data={inspectionData}
              inspect={setSelectedTaskId}
            />
          ) : (
            <p>Carregando a execução do outro objetivo…</p>
          )}
          {taskEvidence?.page.nextBefore && <button className="button small" onClick={async()=>{
            const id=selectedTaskId;
            try {
              const older=await api('/panel/tasks/'+id+'?before='+taskEvidence.page.nextBefore);
              setTaskEvidence(previous=>previous?.task.id===id ? {...previous,
                runs:[...new Map([...previous.runs,...older.runs].map((r:Row)=>[r.id,r])).values()],
                acceptances:[...previous.acceptances,...older.acceptances],artifacts:[...previous.artifacts,...older.artifacts],page:older.page} : previous);
            }catch(e:any){setInspectionError(e.message);}
          }}>Carregar tentativas anteriores · {taskEvidence.runs.length}/{taskEvidence.page.total}</button>}
          {['running', 'queued', 'waiting_configuration'].includes(selectedTask.state) && (
            <div className="task-control">
              <button
                className="button small"
                onClick={() =>
                  void onAction(() => api('/tasks/' + selectedTask.id + '/cancel', 'POST', {}))
                }
              >
                Solicitar cancelamento desta tarefa
              </button>
            </div>
          )}
          {selectedTask.state === 'submitted' &&
            selectedTask.criteria.some((c: Row) => c.type === 'review') && (
              <div className="task-control">
                <button
                  className="button small"
                  onClick={() =>
                    void onAction(() =>
                      api('/tasks/' + selectedTask.id + '/review', 'POST', { reviewed: true }),
                    )
                  }
                >
                  Atestar que revisei os critérios e as evidências
                </button>
              </div>
            )}
        </Modal>
      )}
      {tab === 'runs' && (
        <Panel title="Tentativas desta operação">
          <div className="run-list">
            {(tabEvidence?.kind==='runs' ? tabEvidence.rows : runs).map((r: Row) => (
              <article key={r.id}>
                <div>
                  <code>{r.id.slice(0, 8)}</code>
                  <Badge state={r.state} />
                </div>
                <h3>{tasks.find((t: Row) => t.id === r.task_id)?.title}</h3>
                <p>{r.summary ?? 'Aguardando entrega do executor.'}</p>
                <footer>
                  {r.model ?? 'Modelo não registrado'} · {r.account_id} · tentativa {r.attempt}
                </footer>
                <button className="button small" onClick={() => setSelectedTaskId(r.task_id)}>
                  Ver tarefa e execução
                </button>
              </article>
            ))}
          </div>
          {tabEvidence?.kind==='runs' && !tabEvidence.rows.length && (
            <Empty
              title="Aguardando primeira execução"
              description="Confira se o runtime e uma conta estão configurados."
            />
          )}
        </Panel>
      )}
      {tabError && <p role="alert">Não foi possível carregar o histórico: {tabError}</p>}
      {['artifacts','context','runs'].includes(tab) && !tabEvidence && !tabError && <p role="status">Carregando histórico…</p>}
      {tabEvidence?.page.nextBefore && <button className="button small" onClick={async()=>{
        try{
          const next=await api('/panel/collections/'+tabEvidence.kind+'?goalId='+goal.id+'&before='+tabEvidence.page.nextBefore);
          setTabEvidence(previous=>previous && previous.kind===tabEvidence.kind ? {...next,kind:previous.kind,rows:[...previous.rows,...next.rows]} : previous);
        }catch(e:any){setTabError(e.message);}
      }}>Carregar mais · {tabEvidence.rows.length}/{tabEvidence.page.total}</button>}
      {tab === 'artifacts' && (
        <Panel title="Artefatos da operação">
          <div className="artifact-grid">
            {artifacts.map((a: Row) => (
              <a className="artifact-card" key={a.id} href={'/api/artifacts/' + a.id}>
                <FileText size={24} />
                <div>
                  <h3>{a.name}</h3>
                  <code>{a.digest.slice(0, 24)}…</code>
                </div>
                <Download size={17} />
              </a>
            ))}
          </div>
          {tabEvidence?.kind==='artifacts' && !artifacts.length && (
            <Empty
              title="Nenhuma entrega ainda"
              description="Os arquivos aparecerão aqui quando publicados por uma execução."
            />
          )}
        </Panel>
      )}
      {tab === 'context' && (
        <Panel title="Contexto por execução" subtitle="Conteúdo exato entregue a cada agente">
          <div className="context-list">
            {contexts.map((c: Row) => (
              <button key={c.id} onClick={async () => {
                try { onContext(c.manifest.summary ? await api('/panel/contexts/'+c.id) : c); }
                catch(e:any){setTabError(e.message);}
              }}>
                <BookOpen size={20} />
                <div>
                  <strong>
                    {
                      tasks.find(
                        (t: Row) => t.id === runs.find((r: Row) => r.id === c.run_id)?.task_id,
                      )?.title
                    }
                  </strong>
                  <small>
                    {c.manifest.selected.length} memórias · {c.manifest.selector}
                  </small>
                </div>
                <ChevronRight size={15} />
              </button>
            ))}
          </div>
          {tabEvidence?.kind==='contexts' && !contexts.length && (
            <Empty
              title="Contexto será montado ao iniciar"
              description="Memórias e artefatos necessários são selecionados antes de cada tentativa."
            />
          )}
        </Panel>
      )}
      {tab === 'events' && (
        <Panel title="Eventos desta operação">
          <EventList
            expanded
            events={data.events.filter(
              (e) =>
                e.entity_id === goal.id ||
                tasks.some((t: Row) => t.id === e.entity_id) ||
                ids.has(e.entity_id),
            )}
          />
        </Panel>
      )}
    </>
  );
}
function Modal({
  title,
  children,
  onClose,
  wide = false,
}: {
  title: string;
  children: React.ReactNode;
  onClose: () => void;
  wide?: boolean;
}) {
  useEffect(() => {
    const listener = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    document.addEventListener('keydown', listener);
    return () => document.removeEventListener('keydown', listener);
  }, [onClose]);
  return (
    <div className="modal-backdrop" onClick={onClose}>
      <section
        className={'modal ' + (wide ? 'wide' : '')}
        role="dialog"
        aria-modal="true"
        aria-label={title}
        onClick={(e) => e.stopPropagation()}
      >
        <div className="modal-heading">
          <h2>{title}</h2>
          <button className="icon-btn" onClick={onClose} aria-label="Fechar">
            <X size={20} />
          </button>
        </div>
        {children}
      </section>
    </div>
  );
}
createRoot(document.getElementById('root')!).render(<App />);
