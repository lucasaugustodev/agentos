// Runs inside a separate network and mount namespace; only this model socket
// leaves it. The same entry also serves a development host without those
// namespaces, where every path below is supplied explicitly instead of being
// the fixed location it has inside the sandbox.
import http from 'node:http';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const sock = process.env.AGENTOS_MODEL_SOCK || '/run/model.sock';
const workspace = process.env.AGENTOS_WORKSPACE_DIR || '/workspace';
const agentHome = process.env.AGENTOS_AGENT_HOME || '/home/agent';
const browser = process.env.AGENTOS_PLAYWRIGHT_ENTRY || '/opt/agentos-runtime/node_modules/playwright/index.mjs';
// Beside this file in both modes: /opt/agentos-bridge in the sandbox, the
// checkout's bridge/ directory on a development host.
const patch = (name) => fileURLToPath(new URL(name, import.meta.url));
const dshBin = process.env.AGENTOS_DSH_BIN
  || '/opt/deepseek-harness/app/node_modules/@deepseek-ai/dsh/lib/bin.js';
const proxy = http.createServer((req, res) => {
  const upstream = http.request(
    {
      socketPath: sock,
      path: req.url,
      method: req.method,
      headers: { 'content-type': 'application/json' },
    },
    (r) => {
      res.writeHead(r.statusCode, r.headers);
      r.pipe(res);
    },
  );
  upstream.on('error', () => {
    if (!res.headersSent) res.writeHead(502);
    res.end();
  });
  res.on('close', () => upstream.destroy());
  req.pipe(upstream);
});
await new Promise((r) => proxy.listen(17820, '127.0.0.1', r));
const child = spawn(
  process.execPath,
  [
    dshBin,
    '--profile',
    'sdk-minimal',
    '--patch',
    patch('./worker.patch.yml'),
    ...(process.env.AGENTOS_RUN_ID && process.env.AGENTOS_RUN_FENCE ? ['--patch', patch('./sdk-control/worker-steering.patch.yml')] : []),
  ],
  {
    cwd: workspace,
    env: {
      ...process.env,
      DEEPSEEK_API_KEY: 'socket-scoped-no-provider-secret',
      DEEPSEEK_BASE_URL: 'http://127.0.0.1:17820/v1',
      DSH_HOME: agentHome,
      DSH_CONTEXT_WINDOW: '128000',
      DSH_SYSTEM_PROMPT:
        `You are an engineering worker in an isolated local workspace. Complete the assigned work using bash, files and Playwright. External network and production files are unavailable. Never fabricate execution or acceptance. Preserve evidence and report limitations. Use ${browser} for the browser. Save output files under ${workspace}/evidence. The orchestrator collects only explicitly named artifact paths. Shell operations in this workspace are authorized.`,
    },
    stdio: ['pipe', 'pipe', 'pipe'],
  },
);
process.stdin.pipe(child.stdin);
child.stdout.pipe(process.stdout);
child.stderr.pipe(process.stderr);
child.on('close', (code) => {
  proxy.closeAllConnections();
  proxy.close();
  process.exit(code ?? 1);
});
child.on('error', () => process.exit(1));
