// Live observation of one objective: provider permits, runs and the agent tree.
// Read-only. It never starts, stops or alters an execution, and it prints no
// credential, prompt body or artifact content.
import { pool } from '../server/db.js';
import { config } from '../server/config.js';
import path from 'node:path';
// @ts-ignore JavaScript gate client.
import { getGlmRequestGateStatus } from '../bridge/glm-request-slot.mjs';

const goal = process.argv[2];
if (!goal) { console.error('uso: tsx scripts/agentos-live-monitor.ts <goalId> [intervaloMs]'); process.exit(2); }
const intervalMs = Number(process.argv[3] ?? 2000);
const socketPath = process.env.AGENTOS_GLM_GATE_SOCKET || path.join(config.bridgeDataDir, 'glm-gate.sock');
const stamp = () => new Date().toISOString().slice(11, 23);
const counts = (rows: any[], key: string) =>
  rows.map((r) => `${r[key]}=${r.n}`).join(' ') || '-';

let lastEventId = 0;
let lastLine = '';
// Permit occupancy is sampled, so the peak between two polls would be lost.
let peakActive = 0, peakQueued = 0;

async function gate() {
  try {
    const status: any = await getGlmRequestGateStatus({ socketPath });
    peakActive = Math.max(peakActive, status.active ?? 0);
    peakQueued = Math.max(peakQueued, status.queued ?? 0);
    return `gate ativo=${status.active}/${status.limit} fila=${status.queued} pico=${status.maxObserved ?? peakActive} concedidas=${status.granted ?? '?'}`;
  } catch (error: any) {
    return `gate INDISPONIVEL(${error?.code ?? 'erro'}) — requisicoes NAO estao sendo limitadas`;
  }
}

async function tick() {
  const [runs, tasks, tree, events] = await Promise.all([
    pool.query(`SELECT r.state, count(*)::int n FROM runs r JOIN tasks t ON t.id=r.task_id
      WHERE t.goal_id=$1 GROUP BY 1 ORDER BY 1`, [goal]),
    pool.query('SELECT state, count(*)::int n FROM tasks WHERE goal_id=$1 GROUP BY 1 ORDER BY 1', [goal]),
    pool.query(`SELECT COALESCE(agent_role,'(sem papel)') role, COALESCE(agent_level,0) lvl,
      count(*)::int n, count(*) FILTER (WHERE state='accepted')::int aceitas,
      count(*) FILTER (WHERE state IN('ready','blocked_dependencies'))::int prontas
      FROM tasks WHERE goal_id=$1 GROUP BY 1,2 ORDER BY 2,1`, [goal]),
    pool.query(`SELECT e.id, e.kind, e.entity_id, e.payload, e.created_at FROM events e
      WHERE e.id>$2 AND (e.entity_id=$1 OR e.entity_id IN(SELECT id::text FROM tasks WHERE goal_id=$1::uuid)
        OR e.entity_id IN(SELECT r.id::text FROM runs r JOIN tasks t ON t.id=r.task_id WHERE t.goal_id=$1::uuid))
      ORDER BY e.id LIMIT 60`, [goal, lastEventId]),
  ]);

  for (const row of events.rows) {
    lastEventId = Number(row.id);
    const p = row.payload ?? {};
    const detail = [p.active !== undefined ? `ativo=${p.active}` : '', p.queued !== undefined ? `fila=${p.queued}` : '',
      p.position !== undefined ? `pos=${p.position}` : '', p.code ? `code=${p.code}` : '',
      p.model ? `model=${p.model}` : '', p.requests !== undefined ? `req=${p.requests}` : '',
      p.state ? `state=${p.state}` : ''].filter(Boolean).join(' ');
    console.log(`${stamp()} EVENTO ${row.kind} ${String(row.entity_id).slice(0, 8)} ${detail}`);
  }

  const line = [await gate(),
    `| runs ${counts(runs.rows, 'state')}`,
    `| tarefas ${counts(tasks.rows, 'state')}`,
    `| arvore ${tree.rows.map((r) => `N${r.lvl}:${r.role}=${r.n}(ok${r.aceitas})`).join(' ') || '-'}`,
  ].join(' ');
  // Repeating an unchanged snapshot hides the moments that did change.
  if (line !== lastLine) { console.log(`${stamp()} ${line}`); lastLine = line; }
}

console.log(`${stamp()} monitorando objetivo ${goal} | gate=${socketPath} | intervalo=${intervalMs}ms`);
const timer = setInterval(() => void tick().catch((e) => console.error(`${stamp()} ERRO ${e?.code ?? e?.message}`)), intervalMs);
for (const signal of ['SIGINT', 'SIGTERM'] as const)
  process.once(signal, () => { clearInterval(timer); void pool.end().then(() => process.exit(0)); });
await tick().catch((e: any) => console.error(`${stamp()} ERRO inicial ${e?.code ?? e?.message}`));
