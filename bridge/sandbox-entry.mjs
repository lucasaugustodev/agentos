// Runs inside a separate network and mount namespace; only this model socket leaves it.
import http from 'node:http';
import { spawn } from 'node:child_process';
import {supervisorToolPatchArgs} from './runtime-tool-policy.mjs';
const proxy = http.createServer((req, res) => {
  const upstream = http.request(
    {
      socketPath: '/run/model.sock',
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
    '/opt/deepseek-harness/app/node_modules/@deepseek-ai/dsh/lib/bin.js',
    '--profile',
    'sdk-minimal',
    '--patch',
    '/opt/agentos-bridge/worker.patch.yml',
    ...(process.env.AGENTOS_RUN_ID && process.env.AGENTOS_RUN_FENCE ? ['--patch','/opt/agentos-bridge/sdk-control/worker-steering.patch.yml'] : []),
    ...supervisorToolPatchArgs(process.env.AGENTOS_SUPERVISOR_TOOL_PROFILE),
  ],
  {
    cwd: '/workspace',
    env: {
      ...process.env,
      DEEPSEEK_API_KEY: 'socket-scoped-no-provider-secret',
      DEEPSEEK_BASE_URL: 'http://127.0.0.1:17820/v1',
      DSH_HOME: '/home/agent',
      DSH_CONTEXT_WINDOW: '128000',
      DSH_SYSTEM_PROMPT:
        'You are an engineering worker in an isolated local workspace. Complete the assigned work using bash, files and Playwright. External network and production files are unavailable. Never fabricate execution or acceptance. Preserve evidence and report limitations. Use /opt/agentos-runtime/node_modules/playwright/index.mjs for the browser. Save output files under /workspace/evidence. The orchestrator collects only explicitly named artifact paths. Shell operations in this workspace are authorized.',
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
