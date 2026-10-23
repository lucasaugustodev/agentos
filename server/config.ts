import 'dotenv/config';
import path from 'node:path';
import { randomBytes } from 'node:crypto';
function bounded(value: string | undefined, fallback: number, min: number, max: number) {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < min || parsed > max) return fallback;
  return parsed;
}
export const config = {
  port: Number(process.env.AGENTOS_PORT ?? 4280),
  host: process.env.AGENTOS_HOST ?? '127.0.0.1',
  origin: process.env.AGENTOS_ORIGIN ?? 'http://127.0.0.1:4280',
  databaseUrl: process.env.DATABASE_URL ?? '',
  apiToken: process.env.AGENTOS_API_TOKEN ?? randomBytes(32).toString('hex'),
  dataDir: path.resolve(process.env.AGENTOS_DATA_DIR ?? '.local'),
  layaUrl: process.env.AGENTOS_LAYA_URL ?? '',
  recallUrl: process.env.AGENTOS_RECALL_URL ?? '',
  recallKey: process.env.AGENTOS_RECALL_KEY ?? '',
  recallNamespace: process.env.AGENTOS_RECALL_NAMESPACE ?? 'agentos-development',
  dshCommand: process.env.AGENTOS_DSH_COMMAND ?? '',
  dshArgs: JSON.parse(process.env.AGENTOS_DSH_ARGS ?? '[]') as string[],
  workerSlots: Number(process.env.AGENTOS_WORKER_SLOTS ?? 16),
  scopedPlannerLimit: Number(process.env.AGENTOS_SCOPED_PLANNER_LIMIT ?? 5),
  bridgeHost: process.env.AGENTOS_BRIDGE_HOST ?? '',
  remoteExecution: process.env.AGENTOS_REMOTE_EXECUTION === '1',
  role: process.env.AGENTOS_ROLE ?? 'all',
  tailnetHost: process.env.AGENTOS_TAILNET_HOST ?? '',
  workspacesRoot: path.resolve(process.env.AGENTOS_WORKSPACES_ROOT ?? path.join(process.env.AGENTOS_DATA_DIR ?? '.local', 'workspaces')),
  bridgeDataDir: path.resolve(process.env.AGENTOS_BRIDGE_DATA ?? path.join(process.env.AGENTOS_DATA_DIR ?? '.local', 'bridge')),
  runtimeMode: process.env.AGENTOS_RUNTIME_MODE ?? 'direct',
  // Seats are counted separately from provider requests: these size the local
  // scheduler, while the GLM gate keeps simultaneous model calls at six.
  pgPoolMax: bounded(process.env.AGENTOS_PG_POOL_MAX, 15, 1, 200),
  capacityMaxRuns: bounded(process.env.AGENTOS_CAPACITY_MAX_RUNS, 4, 1, 64),
  browserSlots: bounded(process.env.AGENTOS_BROWSER_SLOTS, 2, 1, 32),
  remoteMaxRuns: bounded(process.env.AGENTOS_REMOTE_MAX_RUNS, 2, 1, 64),
  remoteMemoryMiB: bounded(process.env.AGENTOS_REMOTE_MEMORY_MIB, 3600, 512, 262144),
  remoteCpuPercent: bounded(process.env.AGENTOS_REMOTE_CPU_PERCENT, 300, 50, 6400),
  accountMaxConcurrency: bounded(process.env.AGENTOS_ACCOUNT_MAX_CONCURRENCY, 4, 1, 64),
};
if (!['all', 'api', 'workers'].includes(config.role)) throw Error('Invalid AgentOS role');
if (!['127.0.0.1', '::1'].includes(config.host))
  throw Error(
    'Development server must bind loopback; use an authenticated private ingress for remote access.',
  );
if (
  config.databaseUrl &&
  !new URL(config.databaseUrl).pathname.endsWith('/agentos') &&
  !new URL(config.databaseUrl).pathname.endsWith('/agentos_test')
)
  throw Error('Use a dedicated agentos or agentos_test database.');
if (!Number.isInteger(config.workerSlots) || config.workerSlots < 1 || config.workerSlots > 100)
  throw Error('Worker slots must be between 1 and 100.');
if (!Number.isInteger(config.scopedPlannerLimit) || config.scopedPlannerLimit < 1 || config.scopedPlannerLimit > 30)
  throw Error('Scoped planner limit must be between 1 and 30.');
if (!/^agentos[-_]/.test(config.recallNamespace))
  throw Error('Use a separate Recall namespace starting with agentos-.');
