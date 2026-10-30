import { it, expect } from 'vitest';
import {
  operationGames,
  operationWorkforce,
  operationDelivery,
  operationProductivityLine,
  taskFlow,
} from '../web/src/operation-state';
const now = Date.now();
it('keeps delivery gates pending when many diagnostic reports have been accepted', () => {
  const diagnostics = Array.from({ length: 25 }, (_, n) => ({
    id: 'd' + n,
    stage: 'work',
    state: 'accepted',
  }));
  const delivery = operationDelivery([
    ...diagnostics,
    { id: 'i', stage: 'integrate', state: 'running' },
    { id: 'q', stage: 'verify', state: 'blocked_dependencies' },
  ]);
  expect(delivery.acceptedDiagnostics).toBe(25);
  expect(delivery.gates.map((gate) => gate.complete)).toEqual([false, false]);
  expect(operationDelivery(diagnostics).gates.every((gate) => gate.complete)).toBe(false);
  expect(
    operationDelivery([
      { id: 'i', stage: 'integrate', state: 'accepted' },
      { id: 'q', stage: 'verify', state: 'accepted' },
      { id: 'old', stage: 'integrate', state: 'superseded' },
    ]).gates.map((gate) => gate.complete),
  ).toEqual([true, true]);
});
const data = {
  settings: {
    'continuation-supervisor': { state: 'running', checkedAt: new Date(now).toISOString() },
  },
  events: [],
  runs: [{ id: 'r', task_id: 't', attempt: 1, state: 'failed' }],
};
it('does not turn 25 quota failures and one technical routing failure into 26 human requests', () => {
  const tasks = Array.from({length:26}, (_,i)=>({id:'blocked-'+i,state:'failed',
    execution:{continuation:{runId:'run-'+i,state:'needs_action',message:'Old retry diagnosis'}}}));
  const snapshot = {...data,runs:tasks.map((t,i)=>({id:'run-'+i,task_id:t.id,attempt:1,state:'failed',
    error_code:i<25?'QUOTA_EXCEEDED':'EXECUTION_FAILED'}))};
  const flows=tasks.map(t=>taskFlow(t,snapshot,now));
  expect(flows.filter(f=>f.action)).toHaveLength(0);
  expect(flows.filter(f=>f.label==='Aguardando rota com cota')).toHaveLength(25);
  expect(flows[25]).toMatchObject({kind:'stalled',detail:'Old retry diagnosis'});
});
it('a technical failure with a live supervisor is automatic, not a demand on the user', () => {
  const t = {
    id: 't',
    state: 'failed',
    execution: { continuation: { runId: 'r', state: 'waiting_slot', message: 'Waiting' } },
  };
  expect(taskFlow(t, data, now)).toMatchObject({ kind: 'repair', action: false });
  expect(taskFlow(t, { ...data, settings: {} }, now)).toMatchObject({
    kind: 'stalled',
    action: false,
  });
});
it('shows explicit external intervention and ignores old errors after acceptance', () => {
  const t = {
    id: 't',
    state: 'failed',
    execution: {
      continuation: { runId: 'r', state: 'needs_action', message: 'Renew credentials',
        owner: 'user', humanRequest: {kind: 'authentication', question: 'Reconnect this account'} },
    },
  };
  expect(taskFlow(t, data, now)).toMatchObject({ kind: 'action', action: true });
  expect(taskFlow({ ...t, state: 'accepted' }, data, now)).toMatchObject({
    kind: 'done',
    action: false,
  });
});
it('shows a specific evidence wait without promising recovery or requesting approval', () => {
  const task = {
    id: 't',
    state: 'recovery_blocked',
    execution: {
      continuation: { runId: 'r', state: 'waiting_evidence', message: 'Original atlas missing' },
    },
  };
  expect(taskFlow(task, data, now)).toMatchObject({
    kind: 'waiting',
    label: 'Aguardando insumos',
    detail: 'Original atlas missing',
    action: false,
  });
  expect(taskFlow(task, { ...data, settings: {} }, now)).toMatchObject({
    kind: 'waiting',
    action: false,
  });
  expect(
    taskFlow(
      { ...task, state: 'running' },
      { ...data, runs: [{ ...data.runs[0], state: 'running' }] },
      now,
    ),
  ).toMatchObject({ kind: 'running', action: false });
});
it('shows an automatically assigned supervision case without requesting human action', () => {
  expect(
    taskFlow(
      {
        id: 't',
        state: 'recovery_blocked',
        execution: {
          continuation: {
            runId: 'r',
            state: 'supervising',
            message: 'Sol is choosing a new approach',
          },
        },
      },
      data,
      now,
    ),
  ).toMatchObject({ kind: 'repair', label: 'Em supervisão', action: false });
});
it('shows a prerequisite wait instead of an older needs-action diagnosis', () => {
  const task = {
    id: 't',
    state: 'submitted',
    execution: {
      continuation: {
        runId: 'r',
        state: 'needs_action',
        message: 'Old repeated-failure diagnosis',
      },
    },
  };
  const prerequisite = { id: 'p', title: 'Native animation repair', state: 'running' };
  const d = {
    ...data,
    tasks: [task, prerequisite],
    dependencies: [{ task_id: 't', depends_on: 'p' }],
  };
  expect(taskFlow(task, d, now)).toMatchObject({
    kind: 'waiting',
    action: false,
    detail: 'Native animation repair',
  });
  expect(
    taskFlow(task, { ...d, tasks: [task, { ...prerequisite, state: 'accepted' }] }, now),
  ).toMatchObject({ kind: 'stalled', action: false });
  expect(
    taskFlow(task, { ...d, runs: [{ ...data.runs[0], error_code: 'AUTH_REQUIRED' }] }, now),
  ).toMatchObject({ kind: 'action', action: true });
});
it('groups isolated workspaces by game and gives the running stage focus', () => {
  const tasks = [
    {
      id: 't',
      state: 'failed',
      title: 'Test',
      execution: {
        workspace: 'p/a',
        baseWorkspace: 'p/game',
        continuation: { runId: 'r', state: 'waiting_slot' },
      },
    },
    {
      id: 'u',
      title: 'Build',
      state: 'running',
      execution: { workspace: 'p/b', baseWorkspace: 'p/game' },
    },
  ];
  const d = {
    ...data,
    runs: [...data.runs, { id: 's', task_id: 'u', attempt: 1, state: 'running' }],
  };
  const games = operationGames(tasks, d, now);
  expect(games).toHaveLength(1);
  expect(games[0].focus.task.id).toBe('u');
  expect(games[0].actions).toHaveLength(0);
});

it('keeps coordinators and global reviews out of game cards but retains scoped reviews', () => {
  const tasks = [
    { id: 'coord', stage: 'coordinate', state: 'running', title: 'Coordenar' },
    { id: 'review', stage: 'review', state: 'running', title: 'Revisar operação' },
    {
      id: 'code',
      stage: 'work',
      state: 'accepted',
      title: 'Implementar jogo',
      execution: { workspace: 'p/copy', baseWorkspace: 'p/game' },
    },
    {
      id: 'qa',
      stage: 'review',
      state: 'running',
      title: 'Revisar jogo',
      execution: { baseWorkspace: 'p/game' },
    },
  ];
  const games = operationGames(tasks, { ...data, runs: [] }, now);
  expect(games).toHaveLength(1);
  expect(games[0]).toMatchObject({ done: 1, total: 2, complete: false });
  expect(games[0].tasks.map((t) => t.id)).toEqual(['code', 'qa']);
});

it('counts actual workforce separately from targets, queue and unconfirmed stops', () => {
  const tasks = ['coord', 'glm1', 'glm2', 'kimi', 'qwen', 'agy', 'sol', 'old'].map((id) => ({
    id,
    stage: id === 'coord' ? 'coordinate' : 'work',
  }));
  const d = {
    settings: { 'workforce:g': { enabled: true, glmTarget: 6 } },
    accounts: [
      { id: 'k', provider: 'kimi' },
      { id: 'q', provider: 'qwen' },
      { id: 'a', provider: 'agy' },
    ],
    runs: [
      { id: 'r1', task_id: 'coord', model: 'gpt-6-sol', state: 'running' },
      { id: 'r2', task_id: 'glm1', runtime: 'opencode', model: 'glm-5.3-flash', state: 'running' },
      { id: 'r3', task_id: 'glm2', model: 'glm-5.3-flash', state: 'queued' },
      { id: 'r4', task_id: 'kimi', account_id: 'k', model: 'k3', state: 'running' },
      {
        id: 'r5',
        task_id: 'qwen',
        account_id: 'q',
        model: 'deepseek-v4-flash-0731',
        state: 'reconciling',
      },
      { id: 'r6', task_id: 'agy', account_id: 'a', model: 'claude-opus', state: 'running' },
      { id: 'r7', task_id: 'sol', model: 'gpt-6-sol', state: 'cancelling' },
      { id: 'old', task_id: 'old', model: 'glm-5.3-flash', state: 'submitted' },
      { id: 'another-goal', task_id: 'outside', model: 'glm-5.3-flash', state: 'running' },
    ],
  };
  const workforce = operationWorkforce('g', tasks, d)!;
  expect(workforce).toMatchObject({ running: 4, queued: 1, confirming: 2 });
  expect(workforce.groups.find((g) => g.id === 'glm')).toMatchObject({
    target: 6,
    running: 1,
    queued: 1,
    confirming: 0,
  });
  expect(workforce.groups.find((g) => g.id === 'coordinator')).toMatchObject({
    running: 1,
    confirming: 0,
  });
  expect(workforce.groups.find((g) => g.id === 'sol-worker')).toMatchObject({ confirming: 1 });
  expect(workforce.groups.find((g) => g.id === 'qwen')).toMatchObject({ confirming: 1 });
  expect(workforce.groups.find((g) => g.id === 'agy')).toMatchObject({ running: 1 });
  expect(workforce.groups.find((g) => g.id === 'luna')).toBeUndefined();
  expect(workforce.live).toHaveLength(7);
  expect(operationWorkforce('other', tasks, d)).toBeNull();
  expect(
    operationWorkforce('g', tasks, { ...d, settings: { 'workforce:g': { enabled: false } } }),
  ).toBeNull();
});

it.each(['gpt-6-sol', 'gpt-6.1-sol', 'openai/gpt-6.1-sol'])(
  'separates Luna workers from coordinator %s and older Luna models',
  (solModel) => {
    const tasks = ['sol', 'luna-running', 'luna-queued', 'luna-confirming', 'old-luna'].map(
      (id) => ({
        id,
        stage: id === 'sol' ? 'coordinate' : 'work',
      }),
    );
    const d = {
      settings: { 'workforce:g': { enabled: true, glmTarget: 6, lunaTarget: 2 } },
      runs: [
        { task_id: 'sol', model: solModel, state: 'running' },
        { task_id: 'luna-running', model: 'gpt-6-luna', state: 'running' },
        { task_id: 'luna-queued', model: 'gpt-6-luna', state: 'queued' },
        { task_id: 'luna-confirming', model: 'openai/gpt-6-luna', state: 'reconciling' },
        { task_id: 'old-luna', model: 'gpt-5.6-luna', state: 'running' },
      ],
    };
    const workforce = operationWorkforce('g', tasks, d)!;
    expect(workforce.groups.find((g) => g.id === 'luna')).toMatchObject({
      label: 'GPT-6 Luna · Codex',
      target: 2,
      running: 1,
      queued: 1,
      confirming: 1,
    });
    expect(workforce.groups.find((g) => g.id === 'coordinator')).toMatchObject({ running: 1 });
    expect(workforce.groups.find((g) => g.id === 'other')).toMatchObject({ running: 1 });
    expect(
      operationWorkforce('g', tasks, { ...d, runs: [] })?.groups.find((g) => g.id === 'luna'),
    ).toMatchObject({ target: 2, running: 0, queued: 0, confirming: 0 });
  },
);

it('keeps GLM sessions independent of the shared request limit without inventing request activity', () => {
  const tasks = Array.from({ length: 8 }, (_, index) => ({ id: `glm-${index}` }));
  const d = {
    settings: { 'workforce:g': { enabled: true, glmTarget: 12, glmRequestLimit: 6 } },
    runs: tasks.map((task) => ({
      task_id: task.id,
      model: 'glm-5.3-flash',
      runtime: 'opencode',
      state: 'running',
    })),
  };
  const workforce = operationWorkforce('g', tasks, d)!;
  const glm = workforce.groups.find((group) => group.id === 'glm')!;
  expect(glm).toMatchObject({ target: 12, requestLimit: 6, running: 8 });
  expect(glm).not.toHaveProperty('activeRequests');
  expect(workforce.groups.find((group) => group.id === 'coordinator')).toMatchObject({
    target: 1,
    requestLimit: null,
  });
  for (const requestLimit of [undefined, 0, -1, 'unknown']) {
    expect(
      operationWorkforce('g', tasks, {
        ...d,
        settings: {
          'workforce:g': { enabled: true, glmTarget: 12, glmRequestLimit: requestLimit },
        },
      })?.groups.find((group) => group.id === 'glm'),
    ).toMatchObject({ target: 12, requestLimit: null, running: 8 });
  }
});

it('builds the one-line productivity metric with single-cause idle block classification', () => {
  const detail = {
    goal: { id: 'g1', state: 'active' },
    objective: { id: 'g1', status: 'active' },
    tasks: [
      { id: 't1', stage: 'work', state: 'ready', execution: { baseWorkspace: 'p/a', workspace: 'p/a1' } },
      { id: 't2', stage: 'work', state: 'running', execution: { baseWorkspace: 'p/b', workspace: 'p/b1' } },
      { id: 't3', stage: 'verify', state: 'accepted', execution: { baseWorkspace: 'p/c', workspace: 'p/c1' } },
    ],
  };
  const activeSnapshot = {
    settings: { 'workforce:g1': { enabled: true, glmTarget: 12, glmRequestLimit: 6 } },
    runs: [
      { id: 'r1', task_id: 't2', model: 'glm-5.3-flash', state: 'running' },
      { id: 'r2', task_id: 'other', model: 'glm-5.3-flash', state: 'running' },
    ],
    dependencies: [],
  };
  expect(operationProductivityLine(detail, activeSnapshot)).toMatchObject({
    ready: 1,
    activeSessions: 1,
    maxSessions: 12,
    glmCalls: null,
    glmOverLimit: false,
    objectiveModelCalls: 0,
    maxGlmCalls: 6,
    acceptedDeliveries: 1,
    blockReason: 'nenhum',
    line: 'trabalho pronto: 1 | sessões ativas: 1/12 | chamadas ao provedor: ?/6 | entregas aceitas: 1 | bloqueio: nenhum',
  });
  expect(
    operationProductivityLine(
      { ...detail, goal: { id: 'g1', state: 'replanning' }, tasks: [] },
      { settings: {}, runs: [], dependencies: [] },
    ).blockReason,
  ).toBe('planejamento');
  expect(
    operationProductivityLine(
      {
        ...detail,
        tasks: [{ id: 't1', stage: 'verify', state: 'blocked_dependencies' }],
      },
      { settings: {}, runs: [], dependencies: [{ task_id: 't1', depends_on: 't0' }] },
    ).blockReason,
  ).toBe('dependências');
  expect(
    operationProductivityLine(
      {
        ...detail,
        tasks: [
          { id: 't1', stage: 'work', state: 'waiting_resource', reason: 'REBASE_CONFLICT' },
        ],
      },
      { settings: {}, runs: [], dependencies: [] },
    ).blockReason,
  ).toBe('conflito');
});
it('never saturates a real gate reading and flags values above the global limit', () => {
  const detail = {
    goal: { id: 'g1', state: 'active' },
    objective: { id: 'g1', status: 'active' },
    tasks: [{ id: 't1', stage: 'work', state: 'running' }],
    runs: [{ id: 'r1', task_id: 't1', model: 'glm-5.3-flash', state: 'running' }],
  };
  const over = operationProductivityLine(detail, {
    settings: { 'glm-gate': { effectiveActive: 7, limit: 6 } },
    runs: [],
    dependencies: [],
  });
  expect(over.glmCalls).toBe(7);
  expect(over.glmOverLimit).toBe(true);
  expect(over.line).toContain('chamadas ao provedor: 7/6 (acima do limite!)');
  const within = operationProductivityLine(detail, {
    settings: { 'glm-gate': { effectiveActive: 3, limit: 6 } },
    runs: [],
    dependencies: [],
  });
  expect(within.glmCalls).toBe(3);
  expect(within.glmOverLimit).toBe(false);
  expect(within.line).toContain('chamadas ao provedor: 3/6');
});

it('counts accepted deliveries as the union of completed requirements across revisions', () => {
  const base = {
    objective: { id: 'g1', status: 'active' },
    tasks: [],
    revisions: [
      { plan: { incremental: { completedRequirementIds: ['req-1'], selectedRequirementId: 'req-2' } } },
      { plan: { incremental: { completedRequirementIds: [], selectedRequirementId: 'req-1' } } },
    ],
  };
  const snapshot = { settings: {}, runs: [], dependencies: [] };
  expect(
    operationProductivityLine({ ...base, goal: { id: 'g1', state: 'active' } }, snapshot)
      .acceptedDeliveries,
  ).toBe(1);
  expect(
    operationProductivityLine({ ...base, goal: { id: 'g1', state: 'completed' } }, snapshot)
      .acceptedDeliveries,
  ).toBe(2);
  const parallel = {
    ...base,
    revisions: [
      {
        plan: {
          incremental: {
            completedRequirementIds: ['req-1'],
            selectedRequirementIds: ['req-2', 'req-3'],
          },
        },
      },
    ],
  };
  expect(
    operationProductivityLine({ ...parallel, goal: { id: 'g1', state: 'completed' } }, snapshot)
      .acceptedDeliveries,
  ).toBe(3);
});

it('does not call a normal QA dependency a block while its work is running', () => {
  const detail = {
    goal: { id: 'g1', state: 'active' },
    objective: { id: 'g1', status: 'active' },
    tasks: [
      { id: 't1', stage: 'work', state: 'running' },
      { id: 't2', stage: 'verify', state: 'blocked_dependencies' },
    ],
  };
  expect(
    operationProductivityLine(detail, { settings: {}, runs: [], dependencies: [] }).blockReason,
  ).toBe('nenhum');
});
