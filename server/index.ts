import { config } from './config.js';
import { pool } from './db.js';
import { requireSchemaReady } from './schema-readiness.js';
import { requireTaskExecutionMetadataReady } from './task-execution-metadata.js';
import { requireRunCriteriaMetadataReady } from './run-criteria-metadata.js';
import { requireTaskPanelMetadataReady } from './task-panel-metadata.js';
import { seedRegistry } from './registry.js';
import { WorkerEngine } from './worker.js';
import { requestConfirmedShutdown } from './candidate-validation-lifecycle.js';
import { makeApp } from './app.js';
import { syncAccounts } from './accounts.js';
import { accountBridge } from './bridge.js';
import { chatEngine } from './chat.js';
import { RunObserver } from './observations.js';
import { ContinuationSupervisor } from './continuation.js';
import { CycleEngine } from './cycles/engine.js';
import { recordWorkerReadiness } from './worker-readiness.js';
await requireSchemaReady();
await requireTaskExecutionMetadataReady();
await requireRunCriteriaMetadataReady();
if (config.role !== 'workers') await requireTaskPanelMetadataReady();
await seedRegistry();
if (config.role !== 'workers') await chatEngine.recover();
if (config.bridgeHost)
  await syncAccounts(true).catch(() =>
    console.warn('Contas: conexão privada temporariamente indisponível.'),
  );
const accountTimer = setInterval(() => {
  if (config.bridgeHost) void syncAccounts().catch(() => {});
}, 30000);
const engine = new WorkerEngine();
if (config.role !== 'api') await engine.start();
const app = makeApp(engine);
if (config.role !== 'workers') await app.listen({ host: config.host, port: config.port });
const observer = new RunObserver();
const observerReady = config.role !== 'api' ? await observer.start() : false;
const continuation = new ContinuationSupervisor();
const continuationReady = config.role !== 'api' ? await continuation.start() : false;
const cycles = new CycleEngine();
if (config.role !== 'api' && process.env.AGENTOS_CYCLES_DEDICATED !== '1') await cycles.start();
if (
  config.role !== 'api' &&
  (!config.bridgeHost || continuationReady) &&
  (!config.bridgeHost || !config.remoteExecution || observerReady) &&
  (process.env.AGENTOS_CYCLES_DEDICATED === '1' || cycles.owner)
)
  await recordWorkerReadiness('agentos-workers', [
    ...(observerReady ? [4280027] : []),
    ...(continuationReady ? [4280050] : []),
    ...(cycles.owner ? [4280088] : []),
  ]);
console.log(`AgentOS development: ${config.origin}`);
let closing = false;
async function close() {
  if (closing) return;
  closing = true;
  clearInterval(accountTimer);
  await cycles.close();
  await observer.close();
  await continuation.close();
  await chatEngine.close();
  if (config.role !== 'api') await engine.stop();
  accountBridge.close();
  await app.close();
  await pool.end();
  process.exit(0);
}
function requestClose() {
  void requestConfirmedShutdown(close, () => {
    console.error('SHUTDOWN_UNCONFIRMED: execution ownership remains reserved.');
    process.exitCode = 1;
  });
}
process.on('SIGINT', requestClose);
process.on('SIGTERM', requestClose);
