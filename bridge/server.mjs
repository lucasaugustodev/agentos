import { assertModelRoute, readModelPolicy } from './model-policy.mjs';
import { operatorModelProfile } from './operator-model-profile.mjs';
// Runs on agentos-server, through a private SSH stdio channel. No HTTP listener.
// Production credentials are read-only. New accounts live only in DATA.
import {
  readFileSync,
  writeFileSync,
  renameSync,
  mkdirSync,
  existsSync,
  chmodSync,
  statSync,
} from 'node:fs';
import { createRequire } from 'node:module';
import { createInterface } from 'node:readline';
import { randomUUID } from 'node:crypto';
import { spawn, execFileSync } from 'node:child_process';
import path from 'node:path';
import { sendRuntimeGuidance,readRuntimeGuidanceReceipt } from './runtime-guidance.mjs';
import { runState, workerWorkspaces } from './runtime-runner.mjs';
import { agyPrintRequest, parseAgyPrintResult } from './agy-print.mjs';
import { withAccountRequestGate } from './gated-generation.mjs';
import { museLogin, museSnapshot } from './muse-accounts.mjs';
import { resolveGenerationAccount } from './generation-account.mjs';
import { openAICompletionsFallback } from './openai-completions-fallback.mjs';
import {generationObserver} from './model-transport-failure.mjs';

const DSH = '/opt/deepseek-harness/app/node_modules';
const DATA = process.env.AGENTOS_BRIDGE_DATA || '/var/lib/agentos-bridge';
mkdirSync(DATA, { recursive: true, mode: 0o700 });
const yaml = createRequire(DSH + '/@deepseek-ai/dsh-credentials-local/lib/index.js')('yaml');
const pi = DSH + '/@earendil-works/pi-ai/dist/';
const { openaiCodexProvider } = await import(pi + 'providers/openai-codex.js');
const { anthropicProvider } = await import(pi + 'providers/anthropic.js');
const { kimiCodingProvider } = await import(pi + 'providers/kimi-coding.js');
const { openAICompletionsApi } = await import(pi + 'api/openai-completions.lazy.js');
const providers = {
  codex: openaiCodexProvider(),
  claude: anthropicProvider(),
  kimi: kimiCodingProvider(),
};
const controllers = new Map(),
  attempts = new Map(),
  refreshes = new Map();
const credentialFile = DATA + '/accounts.json';
const own = () =>
  existsSync(credentialFile) ? JSON.parse(readFileSync(credentialFile, 'utf8')) : [];
function saveAccount(account) {
  const rows = own().filter((r) => r.id !== account.id);
  rows.push(account);
  writeFileSync(credentialFile + '.tmp', JSON.stringify(rows), { mode: 0o600 });
  renameSync(credentialFile + '.tmp', credentialFile);
}
const settings = () =>
  yaml.parse(readFileSync('/var/lib/deepseek-harness/.dsh/settings.yaml', 'utf8'))?.['llm-pi-ai']
    ?.providers ?? {};
const credentials = () =>
  yaml.parse(readFileSync('/var/lib/deepseek-harness/.dsh/.credentials.yaml', 'utf8')) ?? {};
function modelSettings(route) {
  const inherited = settings()[route]?.models ?? [];
  const file = DATA + '/provider-models.json';
  const extra = existsSync(file) ? JSON.parse(readFileSync(file, 'utf8')) : {};
  const additions = extra[route] ?? (route?.startsWith('openai-codex') ? extra.codex : []) ?? [];
  return [...inherited, ...additions];
}
const familyOf = (id) =>
  id.startsWith('openai-codex')
    ? 'codex'
    : id.startsWith('anthropic')
      ? 'claude'
      : id.startsWith('agy-')
        ? 'agy'
        : id === 'kimi-coding'
          ? 'kimi'
          : 'qwen';
const agyModels = [
  'gemini-3.8-flash-high',
  'gemini-3.8-flash-medium',
  'gemini-3.7-flash-high',
  'gemini-3.6-flash-high',
  'gemini-3.1-pro-high',
  'gemini-3.1-pro-low',
  'claude-sonnet-4-6',
  'claude-opus-4-6-thinking',
  'gpt-oss-120b-medium',
];
function catalog(family, route, account) {
  if (family === 'agy') return agyModels;
  if (family === 'muse' && account?.models?.length) return account.models;
  if (account?.model) return [account.model];
  const native = providers[family]?.getModels().map((m) => m.id) ?? [];
  const inheritedRoute =
    route ?? { codex: 'openai-codex', claude: 'anthropic', kimi: 'kimi-coding' }[family];
  const extra = modelSettings(inheritedRoute);
  return [...new Set([...native, ...extra.map((m) => m.id).filter(Boolean)])];
}
function defaultModel(family, models) {
  const choice = {
    codex: 'gpt-6-luna',
    claude: 'claude-opus-5-5',
    kimi: 'k3',
    agy: 'gemini-3.8-flash-high',
    qwen: 'deepseek-v4-flash-0731',
  }[family];
  return models.includes(choice) ? choice : models[0];
}
function identity(credential) {
  for (const token of [credential?.idToken, credential?.id_token, credential?.access]) {
    try {
      const p = JSON.parse(Buffer.from(token.split('.')[1], 'base64url'));
      const email = p.email ?? p['https://api.openai.com/profile']?.email;
      if (email) return { email, name: p.name ?? null };
    } catch {}
  }
  return null;
}
let inheritedCache,
  inheritedAt = 0,
  inheritedPending;
async function inheritedState() {
  if (inheritedCache && Date.now() - inheritedAt < 5000) return inheritedCache;
  if (!inheritedPending)
    inheritedPending = (async () => {
      const response = await fetch('http://127.0.0.1:3084/state', {
        signal: AbortSignal.timeout(20000),
      });
      if (!response.ok) throw Error('INHERITANCE_UNAVAILABLE');
      const result = await response.json();
      inheritedCache = result;
      inheritedAt = Date.now();
      return result;
    })().finally(() => {
      inheritedPending = undefined;
    });
  return inheritedPending;
}
async function state() {
  const data = await inheritedState();
  const accounts = [...data.accounts, ...(data.agyAccounts ?? [])];
  if (data.qwen?.configured)
    accounts.push({ provider: 'qwen-cloud', label: 'Qwen Cloud', connected: true, identity: null });
  const inherited = accounts.map((a) => {
    const family = familyOf(a.provider),
      models = catalog(family, a.provider),
      q = data.quotas?.accounts?.[a.provider] ?? {};
    return {
      id: 'harness:' + a.provider,
      provider: a.provider,
      family,
      label: a.label,
      connected: a.connected,
      identity: a.identity,
      source: 'harness',
      models,
      defaultModel: defaultModel(family, models),
      quota: q,
      authStatus: q.reconnectRequired
        ? 'reconnect_required'
        : a.connected
          ? 'connected'
          : 'disconnected',
    };
  });
  const additional = await Promise.all(own().map(async (a) => {
    const models = catalog(a.family, null, a);
    let muse = {};
    if (a.family === 'muse') {
      try { const state = await museSnapshot(a); muse = { ...state, defaultModel: state.model, runtimeOnly: 'muse' }; }
      catch (e) { muse = { runtimeOnly: 'muse', ...(e.code === 'AUTH_REQUIRED' ? { connected: false, authStatus: 'reconnect_required' } : {}) }; }
    }
    return {
      id: a.id,
      provider: a.family,
      family: a.family,
      label: a.label,
      identity: a.identity,
      connected: !!a.credential || a.family === 'agy',
      source: 'agentos',
      models,
      defaultModel: defaultModel(a.family, models),
      quota: { status: 'unknown', groups: [] },
      authStatus: 'connected',
      ...muse,
    };
  }));
  return { accounts: [...inherited, ...additional], checkedAt: new Date().toISOString() };
}
function safeError(error) {
  const inspectionError=workspaceInspectionError(error);
  if(inspectionError)return inspectionError;
  if (
    [
      'GUIDANCE_IDENTITY_INVALID','GUIDANCE_IDENTITY_MISMATCH','GUIDANCE_REQUEST_INVALID','GUIDANCE_ACTION_CHANGED','GUIDANCE_RUNTIME_CLOSED','GUIDANCE_FRAME_TOO_LARGE','GUIDANCE_FAILED','GUIDANCE_OUTCOME_UNCONFIRMED','GUIDANCE_CHANNEL_UNAVAILABLE','GUIDANCE_RECEIPT_INVALID',
      'STOP_UNCONFIRMED',
      'INPUT_BUNDLE_INVALID',
      'INPUT_BUNDLE_DIGEST',
      'INPUT_BUNDLE_CAPACITY',
      'INVALID_WORKSPACE',
      'INVALID_RUN',
      'HOST_CAPACITY',
      'SOURCE_BUSY',
      'SOURCE_MISSING',
      'DISK_CAPACITY',
      'SNAPSHOT_CONFLICT',
      'SOURCE_SYMLINK',
      'SOURCE_CHANGED_DURING_VALIDATION',
      'RESERVED_ARTIFACT_NAME',
      'PATCH_BINARY_SOURCE',
      'RUN_ALREADY_EXISTS',
      'RUNTIME_EXITED',
      'HARNESS_SANDBOX_OOM',
      'RUNTIME_PROGRESS_STALLED',
      'RUNTIME_START_FAILED',
      'RUNTIME_RPC_ERROR',
      'INITIALIZE_TIMEOUT',
      'INVALID_RUNTIME',
      'MISSING_TURN_END',
      'RESULT_FORMAT',
      'ARTIFACT_PATH',
      'ARTIFACT_NAME',
      'ARTIFACT_NAME_COLLISION',
      'ARTIFACT_MISSING',
      'ARTIFACT_TOO_LARGE',
      'MODEL_POLICY_GLM_ONLY',
      'MODEL_POLICY_UNAVAILABLE',
      'MODEL_POLICY_INVALID',
      'MODEL_REQUEST_FAILED',
      'RATE_LIMIT',
      'AUTH_REQUIRED',
      'QUOTA_EXCEEDED',
      'MODEL_TIMEOUT',
      'MODEL_IDLE_TIMEOUT',
      'CONTEXT_TOO_LARGE',
      'GLM_GATE_UNAVAILABLE',
      'GLM_GATE_LOST',
      'GLM_GATE_PROTOCOL',
      'GLM_GATE_RUN_INVALID',
      'INCOMPLETE_TURN',
      'MODEL_OUTPUT_LIMIT',
      'MISSING_MODEL_RESPONSE',
      'MODEL_CONTEXT_LIMIT',
      'MODEL_INCOMPLETE_RESPONSE',
      'MODEL_RESPONSE_TOO_LARGE',
    ].includes(error?.code)
  )
    return {
      code: error.code,
      message: 'Executor isolado: ' + error.code + '. Evidências preservadas para diagnóstico.',
    };
  const text = String(error?.message ?? error);
  if (error?.name === 'TimeoutError' || /due to timeout/i.test(text))
    return {
      code: 'PROVIDER_TIMEOUT',
      message: 'O serviço excedeu o tempo de resposta. Tente novamente após conferir o estado.',
    };
  if (/quota|rate.limit|429|usage.limit/i.test(text))
    return {
      code: 'QUOTA_EXCEEDED',
      message: 'O provedor informou limite de uso. Escolha outra conta ou aguarde a renovação.',
    };
  if (/auth|credential|token|401|403|invalid_grant/i.test(text))
    return {
      code: 'AUTH_REQUIRED',
      message: 'A autenticação precisa ser renovada. Reconecte a conta ou escolha outra.',
    };
  if (/abort|cancel/i.test(text)) return { code: 'CANCELLED', message: 'Execução interrompida.' };
  const known = {
    INHERITANCE_UNAVAILABLE: 'O catálogo do Harness está temporariamente indisponível.',
    ACCOUNT_MISSING: 'Conta não encontrada.',
    MODEL_NOT_AVAILABLE: 'Este modelo não consta no catálogo desta conta.',
    LOGIN_MISSING: 'Este login expirou; inicie outro.',
    LOGIN_NOT_WAITING: 'Este login não está aguardando um código.',
    API_CONFIGURATION_INVALID: 'Informe uma chave, um modelo e um endpoint HTTPS válido.',
    AGY_REQUEST_FAILED:
      'O Antigravity não concluiu a solicitação. Confira a conta ou selecione outro modelo.',
    LOGIN_IN_PROGRESS: 'Já existe um login em andamento. Conclua ou cancele primeiro.',
    MUSE_LOGIN_FAILED: 'O login do Muse não foi concluído. Inicie outro login.',
    MUSE_LOGIN_START_FAILED: 'Não foi possível iniciar o login do Muse.',
    MUSE_DISCOVERY_FAILED: 'Não foi possível consultar o catálogo do Muse.',
    NATIVE_RUNTIME_ONLY: 'Use esta conta do Muse em uma tarefa com executor nativo.',
  };
  return {
    code: 'PROVIDER_ERROR',
    message:
      known[text] ??
      'O provedor não concluiu a solicitação. Confira a conta e o modelo selecionados.',
  };
}
async function getCredential(account, signal) {
  if (account.source === 'harness') {
    if (account.family === 'qwen') {
      const refs = credentials().refs ?? {};
      let key = typeof refs.QWEN_CLOUD_API_KEY === 'string' ? refs.QWEN_CLOUD_API_KEY : null;
      if (!key) {
        const pid = execFileSync(
          'systemctl',
          ['show', 'deepseek-harness', '--property=MainPID', '--value'],
          { encoding: 'utf8' },
        ).trim();
        const rows = readFileSync('/proc/' + pid + '/environ', 'utf8').split('\0');
        key = rows.find((s) => s.startsWith('QWEN_CLOUD_API_KEY='))?.slice(19);
      }
      if (!key) throw Error('AUTH_REQUIRED');
      return { type: 'api_key', key };
    }
    const record = credentials().records?.['llm-pi-ai/' + account.provider];
    if (record?.kind !== 'grant') throw Error('AUTH_REQUIRED');
    // The production owner remains the sole refresh-token writer.
    if (record.payload.expires && record.payload.expires < Date.now())
      throw Error('AUTH_REFRESH_PENDING');
    return { ...record.payload, type: 'oauth' };
  }
  let stored = own().find((a) => a.id === account.id);
  if (!stored?.credential) throw Error('AUTH_REQUIRED');
  if (stored.credential.type === 'oauth' && stored.credential.expires < Date.now() + 30000) {
    if (!refreshes.has(stored.id))
      refreshes.set(
        stored.id,
        (async () => {
          const credential = await providers[stored.family].auth.oauth.refresh(
            stored.credential,
            signal,
          );
          const current = own().find((a) => a.id === stored.id);
          saveAccount({ ...current, credential });
        })().finally(() => refreshes.delete(stored.id)),
      );
    await refreshes.get(stored.id);
    stored = own().find((a) => a.id === account.id);
  }
  return stored.credential;
}
function checkQuota(account, model) {
  const q = account.quota;
  if (account.authStatus !== 'connected') throw Error('AUTH_REQUIRED');
  const group =
    account.family === 'agy'
      ? q.groups?.find((g) => g.label === (/claude|gpt/i.test(model) ? 'Claude e GPT' : 'Gemini'))
      : q.groups?.find(
          (g) =>
            g.label ===
            { codex: 'codex', claude: 'Claude', kimi: 'Kimi', qwen: 'Qwen' }[account.family],
        );
  if (
    q.ordinaryUsageAllowed === false ||
    group?.windows?.some((w) => !w.disabled && w.remainingPercent <= 0)
  )
    throw Error('QUOTA_EXCEEDED');
}
async function generate(params, signal, emit, admission) {
  assertModelRoute(params.accountId, params.model);
  const account = await resolveGenerationAccount(params.accountId, { own, inheritedState, catalog, familyOf });
  if (account.family === 'muse') throw Error('NATIVE_RUNTIME_ONLY');
  if (!account.models.includes(params.model)) throw Error('MODEL_NOT_AVAILABLE');
  checkQuota(account, params.model);
  if (account.family === 'agy') return agyGenerate(account, params, signal);
  const credential = await getCredential(account, signal);
  let model,
    provider = providers[account.family],
    auth;
  if (provider) {
    const route =
      account.source === 'harness'
        ? account.provider
        : { codex: 'openai-codex', claude: 'anthropic', kimi: 'kimi-coding' }[account.family];
    const custom = modelSettings(route).find((m) => m.id === params.model);
    const base = provider.getModels().find((m) => m.id === params.model) ?? provider.getModels()[0];
    model = {
      ...base,
      ...custom,
      id: params.model,
      provider: provider.id,
      name: custom?.name ?? params.model,
    };
    auth =
      credential.type === 'oauth'
        ? await provider.auth.oauth.toAuth(credential)
        : { apiKey: credential.key };
  } else {
    const saved = own().find((a) => a.id === account.id),
      custom = settings()[account.provider] ?? {};
    model = {
      id: params.model,
      name: params.model,
      provider: 'openai',
      api: 'openai-completions',
      baseUrl:
        saved?.baseUrl ??
        custom.baseURL ??
        'https://token-plan.maas.qwencloudapi.com/compatible-mode/v1',
      reasoning: false,
      input: ['text'],
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
      contextWindow: 128000,
      maxTokens: 8192,
      ...operatorModelProfile(account.id, params.model),
    };
    auth = { apiKey: credential.key };
    provider = { streamSimple: (m, c, o) => openAICompletionsApi().streamSimple(m, c, o) };
    // Use the inspected transport on the operator-selected API route, even when the
    // optional SDK is installed. Other providers retain their existing path.
    if (operatorModelProfile(account.id,params.model).reasoning)
      provider = openAICompletionsFallback();
  }
  // One identity before admission binds the gate and transport observations.
  const requestId = typeof params.requestId === 'string' &&
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(params.requestId)
    ? params.requestId : randomUUID();
  const observe=generationObserver(emit,{requestId,generationAttempt:params.generationAttempt});
  return withAccountRequestGate(
    {...params,requestId},
    signal,
    async (requestSignal) => {
      const stream = provider.streamSimple(
        model,
        { systemPrompt: params.system ?? '', messages: params.messages, tools: params.tools ?? [] },
        {
          ...auth,
          signal: requestSignal,
          reasoning: params.reasoning ?? 'low',
          transport: 'sse',
          maxTokens: params.maxOutputTokens ?? 4096,
          onTelemetry: observe,
          onProgress: progress => observe('MODEL_STREAM_PROGRESS', {
            characters: progress.characters,
            reasoningCharacters: progress.reasoningCharacters,
            toolCharacters: progress.toolCharacters,
            semanticCharacters: progress.semanticCharacters,
            chunks: progress.chunks,
            elapsedMs: progress.elapsedMs,
            done: progress.done === true,
          }),
          onRetry: retry => observe('MODEL_REQUEST_RETRY', retry),
          onFailure: failure => observe('MODEL_REQUEST_DIAGNOSTIC', failure),
        },
      );
      const result = await stream.result();
      if (credential.type === 'api_key' && !providers[account.family] && result.usage) {
        result.usage.cost = null;
      }
      if (['error', 'aborted'].includes(result.stopReason))
        throw Error(result.errorMessage ?? result.stopReason);
      return { message: result, accountId: account.id, model: params.model };
    },
    observe,
    undefined,
    admission,
  );
}
async function agyGenerate(account, params, signal) {
  const run = path.join(DATA, 'runs', randomUUID()),
    home = path.join(run, 'home');
  mkdirSync(home + '/.gemini/antigravity-cli', { recursive: true, mode: 0o700 });
  const source =
    account.source === 'harness'
      ? account.provider === 'agy-1'
        ? '/var/lib/deepseek-harness'
        : '/var/lib/deepseek-harness/agy-profiles/agy-2'
      : own().find((a) => a.id === account.id).home;
  for (const name of ['antigravity-oauth-token', 'antigravity-user']) {
    const file = source + '/.gemini/antigravity-cli/' + name;
    if (existsSync(file))
      writeFileSync(home + '/.gemini/antigravity-cli/' + name, readFileSync(file), { mode: 0o600 });
  }
  const hook = DATA + '/deny-tools.mjs';
  writeFileSync(
    hook,
    `process.stdout.write(JSON.stringify({decision:'deny',reason:'Chat AgentOS: ferramentas locais indisponíveis; use os contratos de tarefas.'}));`,
  );
  mkdirSync(home + '/.gemini/config', { recursive: true });
  writeFileSync(
    home + '/.gemini/config/hooks.json',
    JSON.stringify({
      'agentos-chat': {
        PreToolUse: [
          {
            matcher: '*',
            hooks: [{ type: 'command', command: process.execPath + ' ' + hook, timeout: 1500 }],
          },
        ],
      },
    }),
  );
  const prompt =
    params.system +
    '\n\n' +
    params.messages
      .map(
        (m) =>
          m.role +
          ': ' +
          (typeof m.content === 'string'
            ? m.content
            : (m.content ?? [])
                .filter((b) => b.type === 'text')
                .map((b) => b.text)
                .join('\n')),
      )
      .join('\n\n');
  const request = agyPrintRequest(prompt, params.model);
  const child = spawn('/usr/local/bin/agy', request.args, {
    cwd: run,
    env: { PATH: process.env.PATH, HOME: home, NO_COLOR: '1' },
    detached: true,
    stdio: ['pipe', 'pipe', 'ignore'],
  });
  let killTimer;
  const stop = () => {
    try {
      process.kill(-child.pid, 'SIGTERM');
    } catch {}
    killTimer ??= setTimeout(() => {
      try {
        process.kill(-child.pid, 'SIGKILL');
      } catch {}
    }, 1500);
  };
  signal.addEventListener('abort', stop, { once: true });
  if (signal.aborted) stop();
  try {
    return await new Promise((resolve, reject) => {
      let output = '';
      let inputError;
      child.stdout.setEncoding('utf8');
      child.stdout.on('data', (c) => {
        output += c;
        if (output.length > 4e6) {
          stop();
        }
      });
      child.on('error', reject);
      child.stdin.on('error', (error) => {
        inputError = error;
        stop();
      });
      child.on('close', (code) => {
        try {
          if (signal.aborted) throw Error('CANCELLED');
          if (inputError) throw inputError;
          if (output.length > 4e6) throw Error('OUTPUT_TOO_LARGE');
          if (code !== 0) throw Error(signal.aborted ? 'CANCELLED' : 'AGY_REQUEST_FAILED');
          const r = parseAgyPrintResult(output);
          resolve({
            accountId: account.id,
            model: params.model,
            message: {
              role: 'assistant',
              content: [{ type: 'text', text: r.response }],
              api: 'anthropic-messages',
              provider: 'agy',
              model: params.model,
              stopReason: 'stop',
              timestamp: Date.now(),
              usage: {
                input: r.usage?.input_tokens ?? 0,
                output: r.usage?.output_tokens ?? 0,
                totalTokens: r.usage?.total_tokens ?? 0,
                cost: null,
              },
            },
          });
        } catch (e) {
          reject(e);
        }
      });
      child.stdin.end(request.input);
    });
  } finally {
    clearTimeout(killTimer);
    signal.removeEventListener('abort', stop);
  }
}
function publicAttempt(a) {
  return {
    id: a.id,
    family: a.family,
    label: a.label,
    status: a.status,
    accountId: a.accountId,
    notices: a.notices,
    prompt: a.prompt,
    error: a.error,
  };
}
function startLogin(params) {
  if (!['codex', 'claude', 'kimi', 'agy', 'muse'].includes(params.family))
    throw Error('LOGIN_FAMILY_INVALID');
  if ([...attempts.values()].some((a) => a.status === 'pending')) throw Error('LOGIN_IN_PROGRESS');
  const id = randomUUID(),
    accountId = 'agentos:' + params.family + ':' + randomUUID(),
    controller = new AbortController();
  const a = {
    id,
    accountId,
    status: 'pending',
    notices: [],
    prompt: null,
    controller,
    label: String(params.label || params.family + ' adicional').slice(0, 100),
    family: params.family,
  };
  attempts.set(id, a);
  const timer = setTimeout(() => controller.abort(), 600000);
  timer.unref();
  const complete = (credential) => {
    if (controller.signal.aborted || a.status !== 'pending') return;
    saveAccount({
      id: accountId,
      family: params.family,
      label: a.label,
      credential,
      identity: identity(credential),
    });
    a.status = 'completed';
    a.prompt = null;
    a.notices = [];
  };
  controller.signal.addEventListener(
    'abort',
    () => {
      if (a.status === 'pending') a.status = 'cancelled';
      a.prompt = null;
      a.answer = null;
      a.notices = [];
      clearTimeout(timer);
    },
    { once: true },
  );
  if (params.family === 'muse') {
    const home = DATA + '/muse/' + accountId.split(':').at(-1);
    const account = { id: accountId, family: 'muse', label: a.label, home, authPath: home + '/.config/muse/auth.json' };
    void museLogin(account, controller.signal, notice => { if (a.status === 'pending') a.notices = [notice]; })
      .then(info => {
        if (controller.signal.aborted || a.status !== 'pending') return;
        saveAccount({ ...account, model: info.model, models: info.models, identity: info.identity, credential: { type: 'muse', authPath: account.authPath } });
        a.status = 'completed'; a.prompt = null; a.notices = [];
      }, e => {
        a.status = controller.signal.aborted ? 'cancelled' : 'failed';
        a.error = controller.signal.aborted ? undefined : safeError(e);
        a.prompt = null;
      }).finally(() => clearTimeout(timer));
  } else if (params.family === 'agy') {
    const home = DATA + '/agy/' + accountId.split(':').at(-1);
    mkdirSync(home, { recursive: true, mode: 0o700 });
    const workspace = home + '/workspace';
    mkdirSync(workspace, { recursive: true });
    const src = readFileSync('/opt/harness-v2/agy-bridge.py', 'utf8')
      .replace(
        /^HOMES=.*$/m,
        'HOMES=' +
          JSON.stringify({ 'agy-1': home, 'agy-2': home }).replace(/"([^"\n]*)":/g, '"$1":'),
      )
      .replace(/^HOMES=(.*)$/m, 'HOMES={k:Path(v) for k,v in ($1).items()}')
      .replaceAll('/srv/agents', workspace);
    const script = home + '/login.py';
    writeFileSync(script, src, { mode: 0o600 });
    const child = spawn('/usr/bin/python3', [script, 'login', 'agy-2'], {
      stdio: ['pipe', 'pipe', 'ignore'],
      detached: true,
    });
    const lines = createInterface({ input: child.stdout });
    controller.signal.addEventListener(
      'abort',
      () => {
        try {
          process.kill(-child.pid, 'SIGTERM');
        } catch {}
      },
      { once: true },
    );
    lines.on('line', (line) => {
      try {
        const e = JSON.parse(line);
        if (e.event === 'login') {
          a.notices = [{ type: 'auth_url', url: e.url, instructions: e.message }];
          a.prompt = {
            type: 'manual_code',
            message: 'Cole o código de autorização mostrado pelo Google.',
          };
          a.answer = (value) => {
            child.stdin.write(JSON.stringify({ code: value }) + '\n');
            a.prompt = null;
          };
        }
        if (e.event === 'complete' && a.status === 'pending') {
          saveAccount({ id: accountId, family: 'agy', label: a.label, home, identity: e.identity });
          a.status = 'completed';
          a.prompt = null;
          a.notices = [];
        }
        if (e.event === 'error') {
          a.error = safeError(e.message);
          a.status = 'failed';
        }
      } catch {}
    });
    child.on('error', () => {
      a.status = 'failed';
      a.error = { message: 'Não foi possível iniciar login AGY.' };
    });
    child.on('close', () => {
      clearTimeout(timer);
      if (a.status === 'pending') a.status = 'failed';
    });
  } else {
    void providers[params.family].auth.oauth
      .login({
        signal: controller.signal,
        notify: (e) => {
          if (a.status !== 'pending') return;
          a.notices.push(e);
          a.notices = a.notices.slice(-8);
        },
        prompt: (p) =>
          new Promise((resolve, reject) => {
            if (controller.signal.aborted) {
              reject(Error('CANCELLED'));
              return;
            }
            a.prompt = { type: p.type, message: p.message, options: p.options };
            const signal = p.signal
              ? AbortSignal.any([p.signal, controller.signal])
              : controller.signal;
            const abort = () => {
              a.prompt = null;
              a.answer = null;
              reject(Error('CANCELLED'));
            };
            a.answer = (value) => {
              signal.removeEventListener('abort', abort);
              a.prompt = null;
              a.answer = null;
              resolve(value);
            };
            signal.addEventListener('abort', abort, { once: true });
            if (signal.aborted) abort();
          }),
      })
      .then(complete, (e) => {
        a.status = controller.signal.aborted ? 'cancelled' : 'failed';
        a.error = controller.signal.aborted ? undefined : safeError(e);
        a.prompt = null;
      })
      .finally(() => clearTimeout(timer));
  }
  return publicAttempt(a);
}
async function dispatch(method, params, signal, emit) {
  if (['context.rank', 'memory.search', 'memory.push'].includes(method))
    return contextService(method, params);
  if (method === 'worker.workspaces') return workerWorkspaces();
  if (method === 'worker.prepare') return workspaceOperation({ ...params, action: 'prepare' });
  if (['worker.inspectGeneration','worker.inspectPlanningSource','worker.inspectPreparation','worker.inspectSourceBinding','worker.inspectRetrySourceBinding'].includes(method))
    return workspaceInspection(method,params);
  if (method === 'worker.checkComposition') return workspaceOperation({ ...params, action: 'compose', dryRun: true });
  if (method === 'worker.storage') return workspaceOperation({ action: 'capacity' });
  if (method === 'worker.execute') {
    try { assertModelRoute(params.accountId, params.model); }
    catch(error) { emit('HARNESS_NOT_STARTED',{confirmed:true,phase:'model_policy',code:error.code??'MODEL_POLICY_GLM_ONLY'}); throw error; }
    // Existing executions retain their module. New runs see deployed fixes.
    const version = statSync(new URL('./runtime-runner.mjs', import.meta.url)).mtimeMs;
    const runtime = await import('./runtime-runner.mjs?v=' + version);
    return runtime.runHarness(
      params,
      signal,
      (input, requestSignal, admission) =>
        generate({ ...input, runId: params.runId }, requestSignal, emit, admission),
      emit,
    );
  }
  if (method === 'worker.guide') return sendRuntimeGuidance('/var/lib/agentos-bridge/worker-runs',params);
  if (method === 'worker.guidanceReceipt') return readRuntimeGuidanceReceipt('/var/lib/agentos-bridge/worker-runs',params);
  if (method === 'worker.state') return runState(params.runId);
  if (method === 'state') return state();
  if (method === 'generate') return generate(params, signal, emit);
  if (method === 'login.start') return startLogin(params);
  if (method === 'login.active') {
    const a = [...attempts.values()].find((a) => a.status === 'pending');
    return a ? publicAttempt(a) : null;
  }
  if (method === 'login.state') {
    const a = attempts.get(params.id);
    if (!a) throw Error('LOGIN_MISSING');
    return publicAttempt(a);
  }
  if (method === 'login.answer') {
    const a = attempts.get(params.id);
    if (!a?.answer || typeof params.value !== 'string' || params.value.length > 16000)
      throw Error('LOGIN_NOT_WAITING');
    a.answer(params.value);
    return publicAttempt(a);
  }
  if (method === 'login.cancel') {
    const a = attempts.get(params.id);
    a?.controller.abort();
    return { cancelled: true };
  }
  if (method === 'account.addKey') {
    const url = new URL(params.baseUrl);
    if (url.protocol !== 'https:' || url.username || url.password || !params.key || !params.model)
      throw Error('API_CONFIGURATION_INVALID');
    const a = {
      id: 'agentos:api:' + randomUUID(),
      family: params.family === 'qwen' ? 'qwen' : 'openai',
      label: String(params.label).slice(0, 100),
      baseUrl: url.href.replace(/\/$/, ''),
      model: String(params.model).slice(0, 120),
      credential: { type: 'api_key', key: String(params.key) },
      identity: null,
    };
    saveAccount(a);
    return { accountId: a.id };
  }
  throw Error('METHOD_NOT_FOUND');
}
const lines = createInterface({ input: process.stdin });
lines.on('line', (line) => {
  if (line.length > 36 * 1024 * 1024) {
    process.stderr.write('Oversized bridge frame rejected.\n');
    process.stdin.destroy();
    return;
  }
  let request;
  try {
    request = JSON.parse(line);
  } catch {
    return;
  }
  if (request.method === 'cancel') {
    controllers.get(request.params?.id)?.abort();
    return;
  }
  const controller = new AbortController();
  controllers.set(request.id, controller);
  void dispatch(request.method, request.params ?? {}, controller.signal, (kind, payload) =>
    process.stdout.write(JSON.stringify({ id: request.id, event: { kind, payload } }) + '\n'),
  )
    .then(
      (result) => process.stdout.write(JSON.stringify({ id: request.id, result }) + '\n'),
      (e) => process.stdout.write(JSON.stringify({ id: request.id, error: safeError(e) }) + '\n'),
    )
    .finally(() => controllers.delete(request.id));
});
process.stdin.on('end', () => {
  for (const c of controllers.values()) c.abort();
  for (const a of attempts.values()) a.controller.abort();
  setTimeout(() => process.exit(0), 2000).unref();
});
import { workspaceOperation } from './workspaces.mjs';
import { workspaceInspection,workspaceInspectionError } from './workspace-inspection.mjs';
import { contextService } from './context-services.mjs';
