import React, { useState, useEffect, useMemo, useRef } from 'react';
import {
  Activity,
  AlertCircle,
  AlertTriangle,
  Check,
  CheckCircle2,
  ChevronDown,
  ChevronRight,
  Clock,
  Code2,
  Cpu,
  ExternalLink,
  FileCode,
  FileText,
  Filter,
  FolderGit2,
  Layers,
  Maximize2,
  Minimize2,
  Play,
  RefreshCw,
  Search,
  ShieldCheck,
  Sparkles,
  Terminal,
  Wrench,
  X,
} from 'lucide-react';
import './execution-tree.css';
import { taskAttention, latestObservation } from './task-attention';
import { actionSummary } from './execution-status';

type Row = Record<string, any>;

interface ExecutionTreeViewProps {
  detail: Row;
  data: Row;
  inspect: (taskId: string) => void;
  // The goal read model omits task instructions and accepted attempts; the
  // drawer reads the per-task panel model for those.
  loadTask: (taskId: string, signal: AbortSignal) => Promise<Row>;
}

interface TreeNode {
  id: string;
  task: Row;
  role: 'coordinator' | 'specialist' | 'worker' | 'validator';
  level: number;
  children: TreeNode[];
  latestRun?: Row;
  writePaths: string[];
  totalTokens: number;
  hasAttention: boolean;
  attentionReason?: string;
  lastEventText?: string;
}

const activeStates = new Set(['running', 'cancelling', 'reconciling']);

const stateLabels: Record<string, string> = {
  running: 'Em execução',
  accepted: 'Aceita',
  submitted: 'Aguardando validação',
  queued: 'Na fila',
  ready: 'Pronta para despacho',
  blocked_dependencies: 'Aguardando dependência',
  blocked_failed: 'Dependência falhou',
  waiting_quota: 'Aguardando cota',
  waiting_resource: 'Aguardando recurso',
  waiting_configuration: 'Configuração pendente',
  retry_scheduled: 'Retomada agendada',
  reconciling: 'Sem confirmação',
  cancelling: 'Cancelando',
  cancelled: 'Cancelada',
  failed: 'Falhou',
  recovering_artifacts: 'Recuperando artefatos',
  recovery_blocked: 'Correção requer plano',
};

function formatTokens(n: number): string {
  if (!n) return '0';
  if (n < 1000) return `${n}`;
  if (n < 1_000_000) return `${(n / 1000).toFixed(1)}k`;
  return `${(n / 1_000_000).toFixed(2)}M`;
}

function formatDuration(startedAt?: string, finishedAt?: string): string {
  if (!startedAt) return '—';
  const start = new Date(startedAt).getTime();
  const end = finishedAt ? new Date(finishedAt).getTime() : Date.now();
  const diffSec = Math.max(0, Math.floor((end - start) / 1000));
  if (diffSec < 60) return `${diffSec}s`;
  if (diffSec < 3600) return `${Math.floor(diffSec / 60)}m ${diffSec % 60}s`;
  return `${Math.floor(diffSec / 3600)}h ${Math.floor((diffSec % 3600) / 60)}m`;
}

function resolveRole(task: Row): 'coordinator' | 'specialist' | 'worker' | 'validator' {
  if (task.agent_role) {
    const r = task.agent_role.toLowerCase();
    if (r.includes('coord') || r.includes('architect')) return 'coordinator';
    if (r.includes('spec') || r.includes('lead')) return 'specialist';
    if (r.includes('valida') || r.includes('qa') || r.includes('test')) return 'validator';
    return 'worker';
  }
  const title = (task.title || '').toLowerCase();
  if (title.startsWith('coord') || title.startsWith('arquiteto') || title.includes('planejador')) return 'coordinator';
  if (title.startsWith('especialista') || title.includes('specialist') || title.includes('backend') || title.includes('frontend')) return 'specialist';
  if (title.startsWith('valida') || title.includes('qa') || title.includes('test') || title.includes('linter')) return 'validator';
  return 'worker';
}

function resolveWritePaths(task: Row): string[] {
  const execution = task.execution || {};
  if (Array.isArray(execution.writePaths) && execution.writePaths.length > 0) {
    return execution.writePaths;
  }
  if (Array.isArray(execution.paths) && execution.paths.length > 0) {
    return execution.paths;
  }
  return [];
}

function runTokens(run: Row): number {
  const u = run.usage;
  if (!u) return 0;
  return u.totalTokens ?? u.total_tokens ??
    ((u.input ?? u.prompt_tokens ?? 0) + (u.output ?? u.completion_tokens ?? 0) + (u.cacheRead ?? 0));
}

export function ExecutionTreeView({ detail, data, inspect, loadTask }: ExecutionTreeViewProps) {
  const { tasks: history = [], dependencies = [], runs = [], acceptances = [], goal } = detail;
  const tasks = useMemo(() => history.filter((task: Row) => !['cancelled', 'superseded'].includes(task.state)), [history]);
  const [filterRole, setFilterRole] = useState<string>('all');
  const [filterState, setFilterState] = useState<string>('all');
  const [searchQuery, setSearchQuery] = useState<string>('');
  const [expandedNodes, setExpandedNodes] = useState<Set<string>>(new Set());
  const [inspectedId, setInspectedId] = useState<string | null>(null);
  const expandedGoal = useRef<string | null>(null);
  const [drawerTab, setDrawerTab] = useState<'overview' | 'prompt' | 'files' | 'logs' | 'criteria'>('overview');
  const [now, setNow] = useState(Date.now());

  // Live timer tick for active runtimes
  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(timer);
  }, []);

  // Compute Tree Nodes
  const { treeRoots, allNodeIds, nodes, metrics } = useMemo(() => {
    const runsByTask = new Map<string, Row[]>();
    for (const r of runs) {
      const list = runsByTask.get(r.task_id) || [];
      list.push(r);
      runsByTask.set(r.task_id, list);
    }

    // Build raw nodes
    const nodeMap = new Map<string, TreeNode>();
    let runningCount = 0;
    let acceptedCount = 0;
    let attentionCount = 0;
    let globalTokens = 0;

    for (const task of tasks) {
      const taskRuns = (runsByTask.get(task.id) || []).sort(
        (a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime()
      );
      const latestRun = taskRuns[0];
      const role = resolveRole(task);
      const writePaths = resolveWritePaths(task);
      const alert = taskAttention(task, data);

      let nodeTokens = 0;
      for (const r of taskRuns) nodeTokens += runTokens(r);
      globalTokens += nodeTokens;

      if (task.state === 'running') runningCount++;
      if (task.state === 'accepted') acceptedCount++;
      if (alert !== null) attentionCount++;

      // Event snippet
      const latestEvent = data.events?.find(
        (e: Row) =>
          (latestRun && (e.payload?.runId === latestRun.id || e.entity_id === latestRun.id)) ||
          e.entity_id === task.id
      );

      let lastEventText = '';
      if (latestEvent) {
        const p = latestEvent.payload || {};
        if (latestEvent.kind === 'OPENCODE_TOOL_EVENT') lastEventText = `OpenCode · ${p.tool || 'tool'}${p.detail ? ` (${p.detail})` : ''}`;
        else if (latestEvent.kind === 'MODEL_REQUEST') lastEventText = 'Consultando modelo LLM...';
        else if (latestEvent.kind === 'MODEL_RESPONSE') lastEventText = 'Resposta recebida do modelo';
        else if (latestEvent.kind === 'RUN_STARTED') lastEventText = 'Sessão do agente iniciada';
      }

      nodeMap.set(task.id, {
        id: task.id,
        task,
        role,
        level: task.agent_level ?? (role === 'coordinator' ? 0 : role === 'specialist' ? 1 : 2),
        children: [],
        latestRun,
        writePaths,
        totalTokens: nodeTokens,
        hasAttention: alert !== null,
        attentionReason: alert?.detail,
        lastEventText,
      });
    }

    // Parent-child relationships
    const roots: TreeNode[] = [];
    const childIds = new Set<string>();

    // Pass 1: explicit parent_task_id
    for (const node of nodeMap.values()) {
      const parentId = node.task.parent_task_id;
      if (parentId && nodeMap.has(parentId) && parentId !== node.id) {
        nodeMap.get(parentId)!.children.push(node);
        childIds.add(node.id);
      }
    }

    // Dependencies are execution order, not evidence of delegation. Legacy
    // tasks without explicit parents stay at the root rather than inventing
    // a reporting relationship from the first dependency.
    // Root nodes are those not claimed as children
    for (const node of nodeMap.values()) {
      if (!childIds.has(node.id)) {
        roots.push(node);
      }
    }

    // Sort roots by role (coordinators first), then level, then title
    const roleWeight = { coordinator: 0, specialist: 1, worker: 2, validator: 3 };
    roots.sort((a, b) => roleWeight[a.role] - roleWeight[b.role] || a.task.title.localeCompare(b.task.title));

    const allIds = new Set(nodeMap.keys());
    return {
      treeRoots: roots,
      nodes: nodeMap,
      allNodeIds: allIds,
      metrics: {
        total: tasks.length,
        running: runningCount,
        accepted: acceptedCount,
        attention: attentionCount,
        tokens: globalTokens,
      },
    };
  }, [tasks, dependencies, runs, data, acceptances]);

  const inspectedNode = inspectedId ? nodes.get(inspectedId) ?? null : null;
  const setInspectedNode = (node: TreeNode | null) => setInspectedId(node?.id ?? null);

  const [taskEvidence, setTaskEvidence] = useState<Row | null>(null);
  const [taskEvidenceError, setTaskEvidenceError] = useState('');
  useEffect(() => {
    setTaskEvidence(null);
    setTaskEvidenceError('');
    if (!inspectedId) return;
    let current = true, timer: ReturnType<typeof setTimeout> | undefined;
    const controller = new AbortController();
    const load = async () => {
      try {
        const value = await loadTask(inspectedId, controller.signal);
        if (current) { setTaskEvidence(value); setTaskEvidenceError(''); }
      } catch (e: any) { if (current) setTaskEvidenceError(e.message); }
      finally { if (current) timer = setTimeout(() => void load(), 6000); }
    };
    void load();
    return () => { current = false; controller.abort(); clearTimeout(timer); };
  }, [inspectedId]);

  // Expand all by default on first load
  useEffect(() => {
    if (allNodeIds.size > 0 && expandedGoal.current !== (goal?.id ?? 'current')) {
      setExpandedNodes(new Set(allNodeIds));
      expandedGoal.current = goal?.id ?? 'current';
    }
  }, [allNodeIds, goal?.id]);

  const toggleExpand = (id: string, e: React.MouseEvent) => {
    e.stopPropagation();
    setExpandedNodes((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const expandAll = () => setExpandedNodes(new Set(allNodeIds));
  const collapseAll = () => setExpandedNodes(new Set());

  // Filter matching
  const matchesFilter = (node: TreeNode): boolean => {
    // Role filter
    if (filterRole !== 'all' && node.role !== filterRole) return false;

    // State filter
    if (filterState === 'running' && node.task.state !== 'running') return false;
    if (filterState === 'accepted' && node.task.state !== 'accepted') return false;
    if (filterState === 'waiting' && !['queued', 'ready', 'blocked_dependencies', 'waiting_quota', 'waiting_resource'].includes(node.task.state)) return false;
    if (filterState === 'attention' && !node.hasAttention) return false;

    // Search query
    if (searchQuery.trim()) {
      const q = searchQuery.toLowerCase();
      const matchTitle = (node.task.title || '').toLowerCase().includes(q);
      const matchKey = (node.task.key || '').toLowerCase().includes(q);
      const matchRole = node.role.toLowerCase().includes(q);
      const matchScope = node.writePaths.some((p) => p.toLowerCase().includes(q)) ||
        (node.task.execution?.workspace || '').toLowerCase().includes(q);
      const matchModel = ((node.latestRun?.model || node.task.model || '')).toLowerCase().includes(q);
      return matchTitle || matchKey || matchRole || matchScope || matchModel;
    }

    return true;
  };

  // Check if subtree has any matching node
  const subtreeHasMatch = (node: TreeNode): boolean => {
    if (matchesFilter(node)) return true;
    return node.children.some((child) => subtreeHasMatch(child));
  };

  // Render individual tree node
  const renderNode = (node: TreeNode, depth: number = 0) => {
    const isExpanded = expandedNodes.has(node.id);
    const hasChildren = node.children.length > 0;
    const isRunning = node.task.state === 'running';
    const isAccepted = node.task.state === 'accepted';
    const hasAttention = node.hasAttention;
    const isSelected = inspectedNode?.id === node.id;

    // Resolve model name
    const account = data.accounts?.find(
      (a: Row) => a.id === (node.latestRun?.account_id ?? node.task.account_id)
    );
    const modelName = node.latestRun?.model ?? node.task.model ?? account?.model ?? 'Auto LLM';

    // Role Icon
    const roleIcon = {
      coordinator: <Layers size={13} />,
      specialist: <Cpu size={13} />,
      worker: <Wrench size={13} />,
      validator: <ShieldCheck size={13} />,
    }[node.role];

    const roleName = {
      coordinator: 'Coordenador',
      specialist: 'Especialista',
      worker: 'Operário',
      validator: 'Validador QA',
    }[node.role];

    // Status class
    const statusClass = isRunning
      ? 'running'
      : isAccepted
        ? 'accepted'
        : hasAttention
          ? 'attention'
          : 'waiting';

    return (
      <div className="tree-node-wrapper" key={node.id}>
        <div
          className={`tree-node-card level-${Math.min(node.level, 3)} ${isSelected ? 'selected' : ''}`}
          onClick={() => {
            setInspectedNode(node);
            setDrawerTab('overview');
          }}
          title="Clique para abrir inspeção no painel lateral"
        >
          {/* Header Row */}
          <div className="node-card-header">
            <div className="node-identity">
              {hasChildren ? (
                <button
                  className="expand-toggle-btn"
                  onClick={(e) => toggleExpand(node.id, e)}
                  title={isExpanded ? 'Recolher subagentes' : 'Expandir subagentes'}
                >
                  {isExpanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
                </button>
              ) : (
                <span style={{ width: 24 }} />
              )}

              <span className={`role-badge ${node.role}`}>
                {roleIcon}
                {roleName} (N{node.level})
              </span>

              <div className="node-title-group">
                <span className="node-title">{node.task.title}</span>
                {node.task.key && <span className="node-key">{node.task.key}</span>}
              </div>
            </div>

            <div className="node-status-area">
              <span className={`node-status-badge ${statusClass}`}>
                {isRunning && <span className="pulse-indicator" />}
                {isAccepted && <CheckCircle2 size={13} />}
                {hasAttention && <AlertTriangle size={13} />}
                {!isRunning && !isAccepted && !hasAttention && <Clock size={13} />}
                {stateLabels[node.task.state] || node.task.state}
              </span>
            </div>
          </div>

          {/* Body Row: Bounded write paths and active actions */}
          <div className="node-card-body">
            {node.writePaths.length > 0 && (
              <div className="node-scopes-wrapper">
                <span className="scope-tag-label">
                  <FileCode size={12} />
                  Escopo delimitado:
                </span>
                {node.writePaths.map((p, i) => (
                  <span className="scope-pill" key={i}>
                    {p}
                  </span>
                ))}
              </div>
            )}

            {isRunning && node.lastEventText && (
              <div className="node-live-action">
                <Activity size={13} className="live-action-icon" />
                <span>{node.lastEventText}</span>
              </div>
            )}
          </div>

          {/* Footer Row: Telemetry, Tokens, Duration and Inspect Button */}
          <div className="node-card-footer">
            <div className="node-meta-left">
              <span className="node-model-badge" title="Modelo de IA atribuído">
                ⚡ {modelName}
              </span>

              <span className="node-meta-item" title="Tokens consumidos por este agente">
                <Sparkles size={12} />
                {formatTokens(node.totalTokens)} tokens
              </span>

              <span className="node-meta-item" title="Tempo de execução">
                <Clock size={12} />
                {formatDuration(node.latestRun?.started_at, node.latestRun?.finished_at)}
              </span>

              {node.latestRun && (
                <span className="node-meta-item">
                  Tentativa {node.latestRun.attempt}
                </span>
              )}

              {hasChildren && (
                <span className="node-meta-item">
                  <FolderGit2 size={12} />
                  {node.children.length} {node.children.length === 1 ? 'subagente' : 'subagentes'}
                </span>
              )}
            </div>

            <div style={{ display: 'flex', alignItems: 'center', gap: '0.4rem' }}>
              <button
                className="action-btn-secondary"
                onClick={(e) => {
                  e.stopPropagation();
                  setInspectedNode(node);
                }}
              >
                Inspecionar
                <ChevronRight size={13} />
              </button>

              <button
                className="action-btn-secondary"
                onClick={(e) => {
                  e.stopPropagation();
                  inspect(node.id);
                }}
                title="Abrir em janela completa"
              >
                <ExternalLink size={12} />
              </button>
            </div>
          </div>
        </div>

        {/* Children Subtree */}
        {hasChildren && isExpanded && (
          <div className="tree-children-container">
            {node.children
              .filter((child) => subtreeHasMatch(child))
              .map((child) => renderNode(child, depth + 1))}
          </div>
        )}
      </div>
    );
  };

  const visibleRoots = treeRoots.filter((r) => subtreeHasMatch(r));

  // Compute percentage progress
  const progressPercent = metrics.total > 0
    ? Math.round((metrics.accepted / metrics.total) * 100)
    : 0;

  // Lateral drawer: the per-task read model (newest attempt first) supersedes
  // the goal summary, which has no description and skips accepted tasks' runs.
  const evidence = inspectedNode && taskEvidence?.task?.id === inspectedNode.id ? taskEvidence : null;
  const drawerTask: Row = evidence ? { ...inspectedNode!.task, ...evidence.task } : inspectedNode?.task ?? {};
  const drawerRuns: Row[] = evidence?.runs?.length
    ? evidence.runs
    : inspectedNode?.latestRun ? [inspectedNode.latestRun] : [];
  const drawerRun: Row | undefined = drawerRuns[0];
  const drawerTokens = drawerRuns.reduce((sum, r) => sum + runTokens(r), 0);
  const drawerObservation = drawerRun ? latestObservation(drawerRun, data.events) : null;
  const drawerFiles = drawerObservation?.files || [];
  const drawerActions = drawerObservation?.actions || [];
  const drawerAcceptance = drawerRun
    ? (evidence?.acceptances ?? acceptances).find((a: Row) => a.run_id === drawerRun.id)
    : null;
  const drawerArtifacts = drawerRun
    ? (evidence?.artifacts ?? data.artifacts ?? []).filter((a: Row) => a.run_id === drawerRun.id)
    : [];
  const drawerAccountId = drawerRun?.account_id ?? drawerTask.account_id;
  const drawerAccount = drawerAccountId
    ? data.accounts?.find((a: Row) => a.id === drawerAccountId)?.label ?? drawerAccountId
    : 'Automática';
  const drawerLoading = !evidence && !taskEvidenceError;

  return (
    <div className="execution-tree-container">
      {/* Goal Master Banner */}
      {goal && (
        <div className="tree-goal-banner">
          <div className="goal-banner-top">
            <div className="goal-banner-meta">
              <span className="goal-project-pill">{goal.project || 'AgentOS'}</span>
              <span className={`goal-status-badge ${goal.state === 'paused' ? 'paused' : 'active'}`}>
                {({paused:'Pausado',completed:'Concluído',active:'Ativo',planning:'Planejando',replanning:'Replanejando',cancelled:'Cancelado',failed:'Falhou',blocked:'Bloqueado'} as Record<string,string>)[goal.state] ?? goal.state}
              </span>
            </div>
            <span style={{ fontSize: '0.78rem', color: '#94a3b8' }}>
              Meta ID: <code>{goal.id.slice(0, 13)}...</code>
            </span>
          </div>

          <h3 className="goal-banner-title">{goal.title}</h3>

          <div className="goal-banner-progress-area">
            <div className="goal-progress-stats">
              <span>Aceite das tarefas atuais · não representa o aceite final do objetivo</span>
              <strong>{metrics.accepted} de {metrics.total} etapas aceitas ({progressPercent}%)</strong>
            </div>
            <div className="goal-progress-bar-wrap">
              <div
                className="goal-progress-bar-fill"
                style={{ width: `${progressPercent}%` }}
              />
            </div>
          </div>
        </div>
      )}

      {/* Telemetry Dashboard Header */}
      <div className="tree-dashboard">
        <div className="tree-metric-card">
          <div className="metric-icon-box purple">
            <Layers size={22} />
          </div>
          <div className="metric-data">
            <span className="metric-label">Tarefas atuais</span>
            <span className="metric-value">{metrics.total}</span>
            <span className="metric-sub">Inclui fila, execução e aceite</span>
          </div>
        </div>

        <div className="tree-metric-card">
          <div className="metric-icon-box blue">
            <Activity size={22} />
          </div>
          <div className="metric-data">
            <span className="metric-label">Em Execução Simultânea</span>
            <span className="metric-value">{metrics.running} ativos</span>
            <span className="metric-sub">Workers processando tarefas</span>
          </div>
        </div>

        <div className="tree-metric-card">
          <div className="metric-icon-box green">
            <CheckCircle2 size={22} />
          </div>
          <div className="metric-data">
            <span className="metric-label">Etapas Aceitas</span>
            <span className="metric-value">{metrics.accepted} validadas</span>
            <span className="metric-sub">Critérios e testes aprovados</span>
          </div>
        </div>

        <div className="tree-metric-card">
          <div className="metric-icon-box cyan">
            <Sparkles size={22} />
          </div>
          <div className="metric-data">
            <span className="metric-label">Consumo de Tokens</span>
            <span className="metric-value">{formatTokens(metrics.tokens)}</span>
            <span className="metric-sub">Uso registrado nas tentativas</span>
          </div>
        </div>
      </div>

      {/* Controls & Search Toolbar */}
      <div className="tree-controls-bar">
        <div className="tree-search-wrapper">
          <Search size={15} />
          <input
            type="text"
            className="tree-search-input"
            placeholder="Buscar agente, arquivo ou modelo..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
          />
        </div>

        <div className="tree-filter-pills">
          <span style={{ fontSize: '0.78rem', color: '#8190a7', marginRight: 4 }}>Papel:</span>
          {[
            ['all', 'Todos'],
            ['coordinator', 'Coordenadores'],
            ['specialist', 'Especialistas'],
            ['worker', 'Operários'],
            ['validator', 'Validadores'],
          ].map(([k, l]) => (
            <button
              key={k}
              className={`filter-pill ${filterRole === k ? 'active' : ''}`}
              onClick={() => setFilterRole(k)}
            >
              {l}
            </button>
          ))}
        </div>

        <div className="tree-filter-pills">
          <span style={{ fontSize: '0.78rem', color: '#8190a7', marginRight: 4 }}>Status:</span>
          {[
            ['all', 'Todos'],
            ['running', 'Em execução'],
            ['waiting', 'Na fila / Espera'],
            ['accepted', 'Aceitas'],
            ['attention', 'Atenção'],
          ].map(([k, l]) => (
            <button
              key={k}
              className={`filter-pill ${filterState === k ? 'active' : ''}`}
              onClick={() => setFilterState(k)}
            >
              {l}
            </button>
          ))}
        </div>

        <div className="tree-view-actions">
          <button className="action-btn-secondary" onClick={expandAll} title="Expandir toda a árvore">
            <Maximize2 size={13} />
            Expandir
          </button>
          <button className="action-btn-secondary" onClick={collapseAll} title="Recolher toda a árvore">
            <Minimize2 size={13} />
            Recolher
          </button>
        </div>
      </div>

      {/* Hierarchical Execution Tree */}
      <div className="execution-tree">
        {visibleRoots.length > 0 ? (
          visibleRoots.map((root) => renderNode(root))
        ) : (
          <div className="tree-empty-state">
            <AlertCircle size={32} color="#8190a7" />
            <h4>Nenhum agente ou tarefa corresponde aos filtros selecionados</h4>
            <p>Ajuste os filtros de papel, status ou o termo de busca para visualizar as tarefas.</p>
          </div>
        )}
      </div>

      {/* Lateral Sliding Inspection Drawer */}
      {inspectedNode && (
        <>
          <div
            className="tree-drawer-backdrop"
            onClick={() => setInspectedNode(null)}
          />

          <aside className="tree-drawer" role="dialog" aria-modal="true">
            <div className="drawer-header">
              <div className="drawer-header-left">
                <div className="drawer-header-meta">
                  <span className={`role-badge ${inspectedNode.role}`}>
                    {inspectedNode.role.toUpperCase()} (N{inspectedNode.level})
                  </span>
                  <span className={`node-status-badge ${inspectedNode.task.state}`}>
                    {stateLabels[inspectedNode.task.state] || inspectedNode.task.state}
                  </span>
                  {inspectedNode.task.key && (
                    <span className="node-key">{inspectedNode.task.key}</span>
                  )}
                </div>
                <h3 className="drawer-title">{inspectedNode.task.title}</h3>
              </div>

              <button
                className="drawer-close-btn"
                onClick={() => setInspectedNode(null)}
                aria-label="Fechar painel"
              >
                <X size={18} />
              </button>
            </div>

            {/* Drawer Tabs */}
            <div className="drawer-tabs">
              {[
                ['overview', 'Visão Geral'],
                ['prompt', 'Prompt & Contrato'],
                ['files', `Arquivos & Diffs (${drawerFiles.length + drawerArtifacts.length})`],
                ['logs', `Logs ao Vivo (${drawerActions.length})`],
                ['criteria', `Critérios (${drawerTask.criteria?.length || 0})`],
              ].map(([k, l]) => (
                <button
                  key={k}
                  className={`drawer-tab ${drawerTab === k ? 'active' : ''}`}
                  onClick={() => setDrawerTab(k as any)}
                >
                  {l}
                </button>
              ))}
            </div>

            {/* Drawer Body Content */}
            <div className="drawer-content">
              {drawerTab === 'overview' && (
                <>
                  <div className="drawer-section">
                    <h4>
                      <Sparkles size={15} color="#528fff" />
                      Telemetria do Agente
                    </h4>
                    <dl className="drawer-dl">
                      <div>
                        <dt>Modelo & Provedor</dt>
                        <dd>
                          {drawerRun?.model ?? drawerTask.model ?? 'Ainda não selecionado'}
                        </dd>
                      </div>
                      <div>
                        <dt>Conta Vinculada</dt>
                        <dd>{drawerAccount}</dd>
                      </div>
                      <div>
                        <dt>Tokens Consumidos</dt>
                        <dd>
                          {formatTokens(drawerTokens)} tokens
                          {drawerRuns.length > 1 && ` · ${drawerRuns.length} tentativas`}
                        </dd>
                      </div>
                      <div>
                        <dt>Tempo Decorrido</dt>
                        <dd>{formatDuration(drawerRun?.started_at, drawerRun?.finished_at)}</dd>
                      </div>
                      <div>
                        <dt>Tentativa Atual</dt>
                        <dd>
                          {drawerRun
                            ? `${drawerRun.attempt}ª tentativa · ${stateLabels[drawerRun.state] || drawerRun.state}`
                            : drawerLoading ? 'Carregando…' : 'Nenhuma tentativa ainda'}
                        </dd>
                      </div>
                      <div>
                        <dt>Executor / Runtime</dt>
                        <dd>{drawerRun?.runtime ?? drawerTask.execution?.kind ?? 'Não registrado'}</dd>
                      </div>
                    </dl>
                  </div>

                  {drawerTask.reason && !['accepted'].includes(drawerTask.state) && (
                    <div className="drawer-section">
                      <h4>
                        <AlertTriangle size={15} color="#f59e0b" />
                        Situação Registrada
                      </h4>
                      <p style={{ margin: 0, fontSize: '0.82rem', color: '#cbd5e1', lineHeight: 1.55 }}>
                        {drawerTask.reason}
                      </p>
                    </div>
                  )}

                  <div className="drawer-section">
                    <h4>
                      <FileCode size={15} color="#3bd6a4" />
                      Workspace & Escopo
                    </h4>
                    <dl className="drawer-dl">
                      <div>
                        <dt>Workspace</dt>
                        <dd><code>{drawerTask.execution?.workspace || 'Não registrado'}</code></dd>
                      </div>
                      {drawerTask.execution?.baseWorkspace && (
                        <div>
                          <dt>Base</dt>
                          <dd><code>{drawerTask.execution.baseWorkspace}</code></dd>
                        </div>
                      )}
                      {drawerTask.execution?.workKind && (
                        <div>
                          <dt>Tipo de trabalho</dt>
                          <dd>{drawerTask.execution.workKind}</dd>
                        </div>
                      )}
                      <div>
                        <dt>Fonte</dt>
                        <dd>{drawerTask.execution?.readOnlySource ? 'Somente leitura' : 'Gravável'}</dd>
                      </div>
                    </dl>
                    {inspectedNode.writePaths.length > 0 && (
                      <div className="diff-files-list">
                        {inspectedNode.writePaths.map((path, idx) => (
                          <div className="diff-file-row" key={idx}>
                            <span className="diff-file-path">{path}</span>
                            <span className="diff-stat-add">Escopo Autorizado</span>
                          </div>
                        ))}
                      </div>
                    )}
                  </div>

                  {drawerRun?.summary && (
                    <div className="drawer-section">
                      <h4>
                        <FileText size={15} color="#c084fc" />
                        Sumário da Tentativa
                      </h4>
                      <p style={{ margin: 0, fontSize: '0.82rem', color: '#cbd5e1', lineHeight: 1.55, whiteSpace: 'pre-wrap' }}>
                        {drawerRun.summary}
                      </p>
                    </div>
                  )}
                </>
              )}

              {drawerTab === 'prompt' && (
                <div className="drawer-section">
                  <h4>
                    <Code2 size={15} color="#528fff" />
                    Contrato de Instrução do Subagente
                  </h4>
                  <p style={{ margin: 0, fontSize: '0.78rem', color: '#94a3b8' }}>
                    Instruções e escopo registrados para esta tarefa.
                  </p>
                  {evidence ? (
                    drawerTask.description ? (
                      <pre className="code-pre-box">{drawerTask.description}</pre>
                    ) : (
                      <p className="drawer-note">Nenhuma instrução registrada para esta tarefa.</p>
                    )
                  ) : taskEvidenceError ? (
                    <p className="drawer-note" role="alert">
                      Não foi possível carregar as instruções: {taskEvidenceError}
                    </p>
                  ) : (
                    <p className="drawer-note">Carregando instruções…</p>
                  )}
                </div>
              )}

              {drawerTab === 'files' && (
                <div className="drawer-section">
                  <h4>
                    <FileCode size={15} color="#3bd6a4" />
                    Arquivos Alterados & Evidências
                  </h4>
                  {drawerFiles.length > 0 ? (
                    <div className="diff-files-list">
                      {drawerFiles.map((f: Row, i: number) => (
                        <div className="diff-file-row" key={f.path ?? i}>
                          <span className="diff-file-path">{f.path}</span>
                          <div>
                            {f.status === 'new' ? (
                              <span className="diff-stat-add">Novo arquivo</span>
                            ) : (
                              <>
                                {f.added > 0 && <span className="diff-stat-add">+{f.added} </span>}
                                {f.removed > 0 && <span className="diff-stat-remove">−{f.removed}</span>}
                              </>
                            )}
                          </div>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <p className="drawer-note">
                      {drawerObservation
                        ? 'A última observação desta tentativa não registrou arquivos alterados.'
                        : drawerRun
                          ? 'Sem observação recente do workspace desta tentativa no painel.'
                          : 'Nenhuma tentativa iniciada ainda.'}
                    </p>
                  )}

                  {drawerArtifacts.length > 0 && (
                    <>
                      <h4 style={{ marginTop: '1rem' }}>Artefatos Preservados</h4>
                      <div className="diff-files-list">
                        {drawerArtifacts.map((a: Row) => (
                          <div className="diff-file-row" key={a.id}>
                            <a
                              href={`/api/artifacts/${a.id}`}
                              className="diff-file-path"
                              target="_blank"
                              rel="noreferrer"
                            >
                              📄 {a.name}
                            </a>
                            <span style={{ fontSize: '0.72rem', color: '#8190a7' }}>
                              {Math.ceil(a.size / 1024)} KB
                            </span>
                          </div>
                        ))}
                      </div>
                    </>
                  )}
                </div>
              )}

              {drawerTab === 'logs' && (
                <div className="drawer-section">
                  <h4>
                    <Terminal size={15} color="#528fff" />
                    Histórico de Ferramentas & Comandos
                  </h4>
                  {drawerActions.length > 0 ? (
                    <div style={{ display: 'flex', flexDirection: 'column', gap: '0.75rem' }}>
                      {drawerObservation?.toolCalls > drawerActions.length && (
                        <p className="drawer-note">
                          {drawerObservation.toolCalls} chamadas no registro
                          {drawerObservation.traceTruncated ? ' parcial' : ''}; exibindo as últimas {drawerActions.length}.
                        </p>
                      )}
                      {drawerActions.slice().reverse().map((act: Row, i: number) => (
                        <details key={act.id || i} open={i === 0}>
                          <summary style={{ cursor: 'pointer', fontSize: '0.8rem', color: '#cbd5e1' }}>
                            <strong>{actionSummary(act)}</strong> ·{' '}
                            {act.state === 'running'
                              ? 'Em execução'
                              : act.state === 'error'
                                ? 'Erro'
                                : `Concluído${act.exitCode != null ? ` · saída ${act.exitCode}` : ''}`}
                          </summary>
                          <h5 className="drawer-subheading">Comando</h5>
                          <pre className="code-pre-box">{act.command || 'Não registrado'}</pre>
                          <h5 className="drawer-subheading">Resultado</h5>
                          <pre className="code-pre-box">
                            {act.output || (act.state === 'running' ? 'Ainda sem resultado.' : 'Saída não incluída nesta observação.')}
                          </pre>
                        </details>
                      ))}
                    </div>
                  ) : (
                    <p className="drawer-note">
                      {drawerRun
                        ? 'Ainda sem comandos ou chamadas de ferramentas registradas nesta tentativa.'
                        : 'Nenhuma tentativa iniciada ainda.'}
                    </p>
                  )}
                </div>
              )}

              {drawerTab === 'criteria' && (
                <div className="drawer-section">
                  <h4>
                    <ShieldCheck size={15} color="#f59e0b" />
                    Critérios de Aceite Obrigatórios
                  </h4>
                  <p style={{ margin: 0, fontSize: '0.78rem', color: '#94a3b8' }}>
                    Resultado dos critérios registrados. O aceite final do objetivo é verificado separadamente.
                  </p>
                  <ul className="criteria-list-check">
                    {(drawerTask.criteria || []).map((c: Row) => {
                      const check = drawerAcceptance?.checks?.find((chk: Row) => chk.id === c.id);
                      const status = check?.passed === true ? 'passed' : check?.passed === false ? 'failed' : 'pending';
                      return (
                        <li className={`criteria-item-check ${status}`} key={c.id}>
                          {status === 'passed' ? <Check size={16} /> : status === 'failed' ? <X size={16} /> : <Clock size={16} />}
                          <div className="criteria-item-info">
                            <strong>{c.label}</strong>
                            <small>
                              {check?.detail ||
                                (status === 'pending'
                                  ? drawerLoading
                                    ? 'Carregando resultado…'
                                    : 'Ainda não verificado nesta tentativa.'
                                  : status === 'passed' ? 'Aprovado.' : 'Reprovado.')}
                            </small>
                          </div>
                        </li>
                      );
                    })}
                  </ul>
                </div>
              )}
            </div>

            {/* Drawer Footer Actions */}
            <div className="drawer-footer">
              <button
                className="action-btn-secondary"
                onClick={() => {
                  const id = inspectedNode.id;
                  setInspectedNode(null);
                  inspect(id);
                }}
              >
                <ExternalLink size={13} />
                Abrir em Janela Completa
              </button>

              <button
                className="action-btn-secondary"
                onClick={() => setInspectedNode(null)}
              >
                Fechar
              </button>
            </div>
          </aside>
        </>
      )}
    </div>
  );
}
