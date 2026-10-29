import { it, expect } from 'vitest';
import {
  executionStatus,
  historicalEstimate,
  observedProblems,
  modelActivity,
} from '../web/src/execution-status';
const now = Date.parse('2026-10-05T17:00:00Z');
const run = { state: 'running', model: 'test-model', started_at: '2026-10-05T16:55:00Z' };
it('highlights current log failures without treating old files or source code as failures', () => {
  const observation = {
    evidence: [
      {
        name: 'browser.log',
        modifiedAt: '2026-10-05T16:58:00Z',
        preview: 'BROWSER_FAILURE page closed',
      },
      { name: 'old.log', modifiedAt: '2026-10-05T16:20:00Z', preview: 'Error: old failure' },
      { name: 'script.js', modifiedAt: '2026-10-05T16:58:00Z', preview: 'Error: source code' },
    ],
  };
  expect(observedProblems(observation, run)).toHaveLength(1);
  expect(observedProblems(observation, run)[0].title).toBe(
    'Falha registrada no teste de navegador',
  );
});
it('distinguishes active processes from missing results and stale observations', () => {
  const observation = {
    observedAt: new Date(now).toISOString(),
    process: { available: true, activeState: 'active' },
    actions: [{ state: 'running', startedAt: '2026-10-05T16:54:00Z' }],
  };
  expect(executionStatus(observation, run, now)).toMatchObject({ level: 'warning' });
  observation.observedAt = '2026-10-05T16:00:00Z';
  expect(executionStatus(observation, run, now)).toMatchObject({ level: 'unknown' });
  expect(executionStatus(undefined, run, now).title).toContain('Sem observação');
  expect(executionStatus(observation, { ...run, state: 'submitted' }, now).level).toBe('ended');
});
it('does not fabricate an ETA from running tasks or failed and demo attempts', () => {
  const task = { id: 'current', agent_id: 'qa', execution: { kind: 'harness' } };
  const data = {
    goals: [{ id: 'g', is_demo: true }],
    tasks: [{ ...task, id: 't', goal_id: 'g', state: 'accepted' }],
    runs: [
      {
        id: 'r',
        task_id: 't',
        attempt: 1,
        model: 'test-model',
        started_at: '2026-10-05T16:00:00Z',
        finished_at: '2026-10-05T16:10:00Z',
      },
    ],
  };
  expect(historicalEstimate(task, run, data, now).title).toBe('Sem previsão confiável');
  expect(historicalEstimate(task, run, data, now).detail).toContain('0 etapas');
});
it('forms a range only from comparable accepted final attempts and expires overruns', () => {
  const task = { id: 'current', agent_id: 'qa', execution: { kind: 'harness' } };
  const tasks = [1, 2, 3].map((i) => ({ ...task, id: 't' + i, goal_id: 'g', state: 'accepted' }));
  const runs = tasks.map((t) => ({
    task_id: t.id,
    attempt: 2,
    model: 'test-model',
    started_at: '2026-10-05T16:00:00Z',
    finished_at: '2026-10-05T16:10:00Z',
  }));
  const data = {
    goals: [{ id: 'g', is_demo: false }],
    tasks,
    runs: [...runs, { ...runs[0], attempt: 1, finished_at: '2026-10-05T16:59:00Z' }],
  };
  expect(historicalEstimate(task, run, data, now).title).toBe('5–5 min restantes');
  expect(historicalEstimate(task, run, data, now + 600000).title).toBe('Além da faixa histórica');
});

it('distinguishes a pending model response from tool execution and warns on a long wait', () => {
  const r = { ...run, id: 'r' };
  const events = [
    {
      id: 2,
      kind: 'MODEL_REQUEST',
      created_at: new Date(now - 60000).toISOString(),
      payload: { runId: 'r', requests: 30, contextBytes: 180000 },
    },
  ];
  const observation = {
    observedAt: new Date(now).toISOString(),
    process: { available: true, activeState: 'active' },
    actions: [],
  };
  const model = modelActivity(r, events);
  // The request started 60s before `now`, and the elapsed time is always named:
  // a fixed "over 5 minutes" hid both how long it waited and how close it was
  // to being abandoned, which is the whole question while it hangs.
  expect(executionStatus(observation, r, now, model)).toMatchObject({
    level: 'active',
    title: 'Aguardando o modelo há 1 min',
  });
  // Half the request budget is where an ordinary wait becomes a warning.
  const halfBudget = executionStatus(
    { ...observation, observedAt: new Date(now + 300000).toISOString() },
    r,
    now + 300000,
    model,
  );
  expect(halfBudget.level).toBe('warning');
  expect(halfBudget.title).toBe('Modelo sem resposta há 6 min de 12 min');
  // A shorter budget moves the warning earlier, without touching the wording.
  expect(executionStatus(observation, r, now, model, 60_000)).toMatchObject({
    level: 'warning',
    title: 'Modelo sem resposta há 1 min de 1 min',
  });
  expect(
    modelActivity(r, [
      ...events,
      {
        id: 3,
        kind: 'MODEL_RESPONSE',
        created_at: new Date(now).toISOString(),
        payload: { runId: 'r' },
      } as any,
    ]).pending,
  ).toBe(false);
  expect(modelActivity({ ...r, state: 'failed' }, events).pending).toBe(false);
});

it('uses real Muse tool events for activity freshness without treating them as model replies or process proof', () => {
  const r = { ...run, id: 'muse-run', started_at: new Date(now - 600000).toISOString() };
  const events = [
    {
      id: 1,
      kind: 'MODEL_REQUEST',
      created_at: new Date(now - 540000).toISOString(),
      payload: { runId: r.id },
    },
    {
      id: 2,
      kind: 'MUSE_TOOL_EVENT',
      created_at: new Date(now - 10000).toISOString(),
      payload: { runId: r.id, operation: 'shell', type: 'task.lifecycle.completed', toolEvents: 7 },
    },
  ];
  const observation = {
    observedAt: new Date(now).toISOString(),
    process: { available: true, activeState: 'active' },
    actions: [],
  };
  const activity = modelActivity(r, events);
  expect(activity.pending).toBe(true); // A tool event never invents a model response.
  expect(activity.museTool?.payload.toolEvents).toBe(7);
  expect(activity.lastActivityAt).toBe(events[1].created_at);
  expect(executionStatus(observation, r, now, activity).level).toBe('active');
  expect(executionStatus(undefined, r, now, activity).level).toBe('unknown');
  expect(
    executionStatus(
      { ...observation, observedAt: new Date(now + 360000).toISOString() },
      r,
      now + 360000,
      activity,
    ).level,
  ).toBe('warning');
  expect(modelActivity({ ...r, id: 'different-run' }, events).museTool).toBeUndefined();
  const next = modelActivity(r, [
    ...events,
    {
      id: 3,
      kind: 'MODEL_REQUEST',
      created_at: new Date(now).toISOString(),
      payload: { runId: r.id },
    },
  ]);
  expect(next.pending).toBe(true);
  expect(next.latestKind).toBe('MODEL_REQUEST');
  expect(executionStatus(observation, r, now, next).title).toBe('Aguardando o modelo há 0ms');
});
