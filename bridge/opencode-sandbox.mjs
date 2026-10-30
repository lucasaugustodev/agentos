// This entrypoint runs only inside the private mount, PID and network namespaces.
import http from 'node:http';
import { readFile } from 'node:fs/promises';
import { spawn } from 'node:child_process';

const proxy = http.createServer((req, res) => {
  if (req.method !== 'POST' || req.url !== '/v1/chat/completions') {
    res.writeHead(404).end();
    return;
  }
  const upstream = http.request({
    socketPath: '/run/model.sock', path: req.url, method: req.method,
    headers: { 'content-type': 'application/json' },
  }, (reply) => {
    // The trusted gateway supplies canonical integer seconds only. Forward no
    // arbitrary provider headers or data across the sandbox boundary.
    const retry = reply.headers['retry-after'];
    const safeRetry = typeof retry === 'string' && /^\d{1,13}$/.test(retry) && Number.isSafeInteger(Number(retry));
    res.writeHead(reply.statusCode, {
      'content-type': reply.headers['content-type'] ?? 'application/json',
      'cache-control': 'no-store',
      ...(reply.statusCode === 429 && safeRetry ? { 'retry-after': retry } : {}),
    });
    reply.pipe(res);
  });
  upstream.on('error', () => {
    if (!res.headersSent) res.writeHead(502);
    res.end();
  });
  res.on('close', () => upstream.destroy());
  req.pipe(upstream);
});
await new Promise((resolve, reject) => {
  proxy.once('error', reject);
  proxy.listen(17820, '127.0.0.1', resolve);
});
const config = await readFile('/home/agent/opencode.json', 'utf8');
const selectedModel=JSON.parse(config).model;
if (!['agentos/glm-5.3-flash','agentos/deepseek-v4-pro-0813'].includes(selectedModel)) throw Error('INVALID_ROUTE');
const prompt = await readFile('/home/agent/prompt.txt', 'utf8');
const child = spawn('/opt/agentos-opencode/opencode', [
  'run', '--pure', '--format', 'json', '--model', selectedModel,
  '--agent', 'agentos-worker', '--title', 'AgentOS isolated task',
], {
  cwd: '/workspace',
  env: {
    ...process.env,
    XDG_CONFIG_HOME: '/home/agent/.config',
    XDG_CACHE_HOME: '/home/agent/.cache',
    XDG_DATA_HOME: '/home/agent/.local/share',
    XDG_STATE_HOME: '/home/agent/.local/state',
    OPENCODE_CONFIG: '/home/agent/opencode.json',
    OPENCODE_CONFIG_CONTENT: config,
    OPENCODE_DISABLE_PROJECT_CONFIG: 'true',
    OPENCODE_DISABLE_AUTOUPDATE: 'true',
    OPENCODE_DISABLE_LSP_DOWNLOAD: 'true',
    OPENCODE_DISABLE_MODELS_FETCH: 'true',
    OPENCODE_DISABLE_CLAUDE_CODE: 'true',
    OPENCODE_DISABLE_EXTERNAL_SKILLS: 'true',
  },
  // Keep task/context text out of argv: filename-based cleanup inside the
  // namespace must not match and kill the model process carrying that text.
  stdio: ['pipe', 'pipe', 'pipe'],
});
// OpenCode reads non-TTY stdin to EOF before starting. end(prompt) flushes the
// entire context, including backpressure, and closes our dedicated input pipe.
child.stdin.on('error', () => {}); // Child exit/error remains authoritative below.
child.stdin.end(prompt);
child.stdout.pipe(process.stdout);
child.stderr.pipe(process.stderr);
child.on('error', () => process.exit(1));
child.on('close', (code) => {
  proxy.closeAllConnections();
  proxy.close();
  process.exit(code ?? 1);
});
