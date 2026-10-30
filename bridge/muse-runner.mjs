import { spawn } from 'node:child_process';
import { createInterface } from 'node:readline';
import { mkdir, readFile, writeFile, rename, realpath, lstat } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { workspaceOperation } from './workspaces.mjs';
import { stopUnit } from './process-stop.mjs';
import { collectArtifacts, parseArtifactResult } from './artifact-collector.mjs';
import { createMuseGateway, MuseEvents } from './muse-protocol.mjs';
import { materializeSourceInputs } from './source-inputs.mjs';
import {bindCompositionInputs} from './composition-inputs.mjs';
import { preserveRuntimeArtifactClaims } from './runtime-artifacts.mjs';
import { museTaskDeliveryInstruction } from './task-delivery-contract.mjs';
import { deliveryContract, extractPromptJsonFields, prepareDeliveryManifest, collectDeliveryManifest, deliveryManifestInstruction } from './delivery-manifest.mjs';

const DATA = '/var/lib/agentos-bridge/worker-runs';
const ROOT = '/srv/agentos/workspaces';
const NODE = '/opt/deepseek-harness/node/bin/node';
const BINARY = '/opt/agentos-muse/muse-bin-1.4.3-R5018.1';
const uuid = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/;
const workspaceKey = /^[a-z0-9_-]+\/[a-z0-9_-]+$/;
const digest = (bytes) => createHash('sha256').update(bytes).digest('hex');
const failure = (code) => Object.assign(new Error(code), { code });
const resultSchema = {
  type: 'object',
  additionalProperties: false,
  required: ['summary', 'artifacts'],
  properties: {
    summary: { type: 'string' },
    artifacts: {
      type: 'array',
      maxItems: 30,
      items: {
        type: 'object',
        additionalProperties: false,
        required: ['name', 'path', 'mime'],
        properties: {
          name: { type: 'string' },
          path: { type: 'string' },
          mime: { type: 'string' },
        },
      },
    },
  },
};

export function musePrompt(params, sourceInstruction = '') {
  return `${params.prompt}${sourceInstruction}\n\nContext manifest and shared memory: /home/agent/context.json\nInputs: /home/agent/inputs/\nComplete and test only the assigned task in /workspace. ${params.readOnlySource ? 'Source files are read-only; write evidence in evidence/ and temporary test state in data/.' : 'The workspace is isolated; assets and the controller snapshot are read-only.'} Ordinary errors that can be fixed in this scope are implementation work, not external blockers. Do not delegate or open other agent sessions. No external network or production files are available. Browser library: /opt/agentos-runtime/node_modules/playwright/index.mjs. Save actual evidence as files. Final response MUST be JSON {"summary":"what actually changed and was tested, including limitations","artifacts":[{"name":"report.json","path":"evidence/report.json","mime":"application/json"}]}. Paths must be real files relative to /workspace. Never claim independent acceptance. Do not use reserved artifact names candidate.json or changes.patch; the controller generates them.${museTaskDeliveryInstruction(params.deliveryContract)}`;
}

export async function runMuse(params, signal, account, emit = () => {}) {
  try {
    return await executeMuse(params, signal, account, emit);
  } catch (e) {
    // A duplicate ID may still name a live run. Every other preflight happens before spawn.
    if (typeof e.stopped !== 'boolean') e.stopped = e.code !== 'RUN_ALREADY_EXISTS';
    throw e;
  }
}

async function executeMuse(params, signal, account, emit) {
  if (
    !uuid.test(params.runId) ||
    !workspaceKey.test(params.workspace) ||
    typeof params.prompt !== 'string' ||
    !params.prompt.trim() ||
    params.prompt.length > 120000 ||
    (params.sourceWorkspace && !workspaceKey.test(params.sourceWorkspace))
  )
    throw failure('INVALID_REQUEST');
  const models = [
    account?.model,
    ...(account?.models ?? []).map((m) => (typeof m === 'string' ? m : m.id)),
  ];
  if (
    account?.family !== 'muse' ||
    !account.credential?.key ||
    !account.credential?.baseUrl ||
    !models.includes(params.model) ||
    (params.accountId && ![account.id, account.id.replaceAll(':', '-')].includes(params.accountId))
  )
    throw failure('ACCOUNT_CONFIGURATION');
  const reasoning = params.reasoning ?? 'high';
  if (!['none', 'minimal', 'low', 'medium', 'high', 'xhigh', 'max', 'ultra'].includes(reasoning))
    throw failure('INVALID_REQUEST');
  const mem = await readFile('/proc/meminfo', 'utf8');
  if (Number(mem.match(/MemAvailable:\s+(\d+)/)?.[1] ?? 0) < 1900000)
    throw failure('HOST_CAPACITY');
  await mkdir(DATA, { recursive: true, mode: 0o700 });
  const dir = `${DATA}/${params.runId}`,
    home = `${dir}/home`,
    workspace = `${ROOT}/${params.workspace}`;
  try {
    await mkdir(dir, { mode: 0o700 });
  } catch (e) {
    if (e.code === 'EEXIST') throw failure('RUN_ALREADY_EXISTS');
    throw e;
  }
  const unit = `agentos-run-${params.runId}`,
    internal = new AbortController();
  const outputContract = deliveryContract(params.runId, params.deliveryContract?.artifacts?.map(a => a.name) ?? [], extractPromptJsonFields(params.prompt));
  const combined = AbortSignal.any([signal, internal.signal]);
  const events = new MuseEvents(params.runId);
  let child,
    ended = false,
    exitCode,
    stderr = '',
    finalText = '',
    terminalError,
    stopPromise,
    gateway,
    stats,
    composition=null,
    deliveryFinished=false;
  let saveSequence = Promise.resolve();
  const save = (state, extra = {}) =>
    (saveSequence = saveSequence.then(async () => {
      await writeFile(
        `${dir}/state.json.tmp`,
        JSON.stringify({
          runId: params.runId,
          workspace: params.workspace,
          state,
          at: new Date().toISOString(),
          unit,
          runtime: 'muse',
          deliveryContract: outputContract,
          ...(params.sourceContract?{sourceContractVersion:1}:{}),
          model: params.model,
          requests: stats?.requests ?? 0,
          toolCalls: events.toolCalls,
          usage: stats?.usage,
          ...extra,
        }),
        { mode: 0o600 },
      );
      await rename(`${dir}/state.json.tmp`, `${dir}/state.json`);
    }));
  const stop = () =>
    (stopPromise ??= (async () => {
      internal.abort();
      if (!child) return;
      const proof = await stopUnit(unit, () => ended);
      await writeFile(`${dir}/stop-receipt.json`, JSON.stringify(proof), { mode: 0o600 });
      emit('MUSE_STOPPED', { confirmed: true, unit, proof });
    })());
  const abort = () => {
    terminalError ??= failure('CANCELLED');
    void stop().catch(() => {});
  };
  signal.addEventListener('abort', abort, { once: true });
  let lines;
  try {
    await save('starting');
    combined.throwIfAborted();
    if (params.sourceWorkspace)
      await workspaceOperation({
        action: 'prepare',
        workspace: params.workspace,
        sourceWorkspace: params.sourceWorkspace,
      });
    if ((await realpath(workspace)) !== workspace) throw failure('INVALID_WORKSPACE');
    const managed = Boolean(params.managed || params.sourceWorkspace);
    if(params.composition && managed && !params.readOnlySource){
      composition=await workspaceOperation({action:'compose',workspace:params.workspace,composition:bindCompositionInputs(params.composition,params.inputs)});
      emit('PATCH_COMPOSITION_RECORDED',composition);
      params={...params,prompt:params.prompt+'\nRUNTIME PATCH COMPOSITION (not acceptance): '+
        JSON.stringify({complete:composition.complete,applied:composition.receipts.filter(r=>r.status!=='conflict').length,
          conflicts:composition.receipts.filter(r=>r.status==='conflict').length,report:'/home/agent/runtime-composition.json'})+
        '\nCompatible patches are already applied; resolve only reported conflicts and run all existing candidate validation criteria.'};
    }
    for (const name of ['evidence', 'data']) {
      await mkdir(`${workspace}/${name}`, { recursive: true });
      if ((await realpath(`${workspace}/${name}`)) !== `${workspace}/${name}`)
        throw failure('INVALID_WORKSPACE');
    }
    const assets = await lstat(`${workspace}/assets`).catch((e) => {
      if (e.code === 'ENOENT') return null;
      throw e;
    });
    if (assets?.isSymbolicLink()) throw failure('INVALID_WORKSPACE');
    if (
      managed &&
      (await realpath(`${workspace}/.agentos-snapshot.json`)) !==
        `${workspace}/.agentos-snapshot.json`
    )
      throw failure('INVALID_WORKSPACE');
    await mkdir(home, { mode: 0o700 });
    await prepareDeliveryManifest(home, outputContract);
    if(composition)await writeFile(`${home}/runtime-composition.json`,JSON.stringify(composition));
    const sourceInputs = materializeSourceInputs(params.inputs);
    if (sourceInputs.telemetry.materialized) emit('SOURCE_INPUTS_MATERIALIZED', sourceInputs.telemetry);
    for (const input of sourceInputs.inputs) {
      if (!uuid.test(input.id) || !/^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,100}$/.test(input.name))
        throw failure('INVALID_INPUT');
      const bytes = Buffer.from(input.base64, 'base64');
      if (bytes.length > 8 * 1024 * 1024 || digest(bytes) !== input.digest)
        throw failure('INPUT_DIGEST');
      await mkdir(`${home}/inputs/${input.id}`, { recursive: true });
      await writeFile(`${home}/inputs/${input.id}/${input.name}`, bytes, {
        flag: 'wx',
        mode: 0o600,
      });
    }
    await writeFile(
      `${home}/context.json`,
      JSON.stringify({ ...(params.context ?? {}), recovery: params.recoveryContext ?? null }),
      { mode: 0o600 },
    );
    await writeFile(
      `${home}/muse-run.json`,
      JSON.stringify({ runId: params.runId, model: params.model, reasoning }),
      { mode: 0o600 },
    );
    await writeFile(`${home}/result-schema.json`, JSON.stringify(resultSchema), { mode: 0o600 });
    await writeFile(
      `${home}/prompt.txt`,
      musePrompt(params, sourceInputs.instruction) + deliveryManifestInstruction,
      { mode: 0o600 },
    );
    const proxy = createMuseGateway({
      credential: account.credential,
      model: params.model,
      accountId: account.id,
      signal: combined,
      emit,
      onFailure: (e) => {
        terminalError ??= e;
        void stop().catch(() => {});
      },
    });
    gateway = proxy.server;
    stats = proxy.stats;
    const sock = `${dir}/model.sock`;
    await new Promise((resolve, reject) => {
      gateway.once('error', reject);
      gateway.listen(sock, resolve);
    });
    combined.throwIfAborted();
    const mounts = [
      '--unshare-all',
      '--die-with-parent',
      '--new-session',
      '--ro-bind',
      '/usr',
      '/usr',
      '--symlink',
      'usr/bin',
      '/bin',
      '--symlink',
      'usr/sbin',
      '/sbin',
      '--symlink',
      'usr/lib',
      '/lib',
      '--symlink',
      'usr/lib64',
      '/lib64',
      '--proc',
      '/proc',
      '--dev',
      '/dev',
      '--tmpfs',
      '/tmp',
      '--dir',
      '/run',
      '--ro-bind',
      '/etc/fonts',
      '/etc/fonts',
      '--ro-bind',
      '/etc/ld.so.cache',
      '/etc/ld.so.cache',
      '--ro-bind',
      '/opt/deepseek-harness/node',
      '/opt/deepseek-harness/node',
      '--ro-bind',
      '/opt/agentos-runtime',
      '/opt/agentos-runtime',
      '--ro-bind-try', '/opt/agentos-project-deps', '/opt/agentos-project-deps',
      '--ro-bind',
      '/opt/agentos-runtime/browsers',
      '/opt/browsers',
      '--ro-bind',
      BINARY,
      '/opt/agentos-muse/muse',
      '--ro-bind',
      fileURLToPath(new URL('./muse-sandbox.mjs', import.meta.url)),
      '/opt/agentos-bridge/muse-sandbox.mjs',
      params.readOnlySource ? '--ro-bind' : '--bind',
      workspace,
      '/workspace',
      ...(assets?.isDirectory() ? ['--ro-bind', `${workspace}/assets`, '/workspace/assets'] : []),
      ...(managed
        ? ['--ro-bind', `${workspace}/.agentos-snapshot.json`, '/workspace/.agentos-snapshot.json']
        : []),
      ...(params.readOnlySource
        ? [
            '--bind',
            `${workspace}/evidence`,
            '/workspace/evidence',
            '--bind',
            `${workspace}/data`,
            '/workspace/data',
          ]
        : []),
      '--bind',
      home,
      '/home/agent',
      '--ro-bind',
      sock,
      '/run/model.sock',
      '--chdir',
      '/workspace',
      '--clearenv',
      '--setenv',
      'HOME',
      '/home/agent',
      '--setenv', 'PYTHONPYCACHEPREFIX', '/home/agent/.cache/python',
      '--setenv',
      'PATH',
      '/opt/deepseek-harness/node/bin:/usr/bin:/bin',
      '--setenv',
      'PLAYWRIGHT_BROWSERS_PATH',
      '/opt/browsers',
      '--setenv',
      'LANG',
      'C.UTF-8',
      '--',
      NODE,
      '/opt/agentos-bridge/muse-sandbox.mjs',
    ];
    child = spawn(
      'systemd-run',
      [
        '--quiet',
        '--wait',
        '--pipe',
        '--collect',
        `--unit=${unit}`,
        '-p',
        'MemoryMax=1800M',
        '-p',
        'CPUQuota=150%',
        '-p',
        'TasksMax=256',
        '-p',
        'KillMode=control-group',
        '-p',
        'TimeoutStopSec=4',
        '--',
        '/usr/bin/python3',
        fileURLToPath(new URL('./slot-runner.py', import.meta.url)),
        params.workspace,
        '1800',
        '/usr/bin/bwrap',
        ...mounts,
      ],
      { stdio: ['ignore', 'pipe', 'pipe'] },
    );
    const exited = new Promise((resolve) => {
      child.once('error', () => {
        terminalError ??= failure('RUNTIME_START_FAILED');
        ended = true;
        resolve();
      });
      child.once('close', (code) => {
        exitCode = code;
        ended = true;
        resolve();
      });
    });
    child.stderr.on('data', (chunk) => {
      if (stderr.length < 100000) stderr += chunk;
    });
    lines = createInterface({ input: child.stdout });
    lines.on('line', (line) => {
      if (line.length > 2 * 1024 * 1024) return;
      try {
        const tool = events.accept(JSON.parse(line));
        if (tool) emit('MUSE_TOOL_EVENT', tool);
      } catch {}
    });
    await save('running');
    emit('RUNTIME_STARTED', {
      unit,
      runtime: 'muse',
      model: params.model,
      workspace: params.workspace,
      isolation: 'mount+pid+network+cgroup',
      memoryMiB: 1800,
      cpuPercent: 150,
    });
    await exited;
    if (signal.aborted) throw failure('CANCELLED');
    if (terminalError) throw terminalError;
    if (exitCode !== 0)
      throw failure(
        stderr.includes('AGENTOS_HOST_CAPACITY')
          ? 'HOST_CAPACITY'
          : stderr.includes('AGENTOS_SOURCE_BUSY')
            ? 'SOURCE_BUSY'
            : 'RUNTIME_EXITED',
      );
    finalText = events.finalText();
    deliveryFinished = true;
    if (!stats.requests || !stats.responses) throw failure('MISSING_MODEL_RESPONSE');
    // Freeze the cgroup before reading artifacts or computing the candidate snapshot.
    await stop();
    const manifestDelivery = await collectDeliveryManifest(workspace, home, outputContract);
    const value = manifestDelivery?.value ?? parseArtifactResult(finalText);
    const artifacts = manifestDelivery?.artifacts ?? await collectArtifacts(value, workspace);
    if (manifestDelivery) emit('DELIVERY_MANIFEST_COLLECTED', { runId: params.runId,
      digest: manifestDelivery.manifestDigest, artifacts: manifestDelivery.artifacts.length });
    if (managed) {
      const candidate = await workspaceOperation({
        action: 'inspect',
        workspace: params.workspace,
        ...(params.sourceContract?{includeBaseFiles:true}:{}),
      });
      if (params.readOnlySource && candidate.baseDigest !== candidate.candidateDigest)
        throw failure('SOURCE_CHANGED_DURING_VALIDATION');
      const renamed = preserveRuntimeArtifactClaims(artifacts);
      if (renamed.length) emit('RUNTIME_ARTIFACT_CLAIMS_PRESERVED', { claims: renamed });
      for (const [name, content] of [
        ['candidate.json', JSON.stringify({ ...candidate, patch: undefined })],
        ['changes.patch', candidate.patch],
      ]) {
        if (artifacts.some((item) => item.name === name)) throw failure('RESERVED_ARTIFACT_NAME');
        const bytes = Buffer.from(content);
        if (bytes.length > 8 * 1024 * 1024) throw failure('ARTIFACT_TOO_LARGE');
        artifacts.push({
          name,
          mime: name.endsWith('.json') ? 'application/json' : 'text/plain',
          digest: digest(bytes),
          base64: bytes.toString('base64'),
        });
      }
    }
    if (
      artifacts.reduce((sum, item) => sum + Buffer.byteLength(item.base64, 'base64'), 0) >
      24 * 1024 * 1024
    )
      throw failure('ARTIFACT_TOO_LARGE');
    const result = {
      summary: value.summary,
      artifacts,
      usage: stats.usage,
      composition,
      evidence: {
        runtime: 'muse',
        requests: stats.requests,
        toolCalls: events.toolCalls,
        exitCode,
        finishReason: 'completed',
        stopped: true,
      },
    };
    await writeFile(`${dir}/result.json`, JSON.stringify(result), { mode: 0o600 });
    await save('completed', {
      stopped: true,
      exitCode,
      finishReason: 'completed',
      artifacts: artifacts.map(({ base64, ...item }) => item),
    });
    return result;
  } catch (e) {
    const originalCode = e.code ?? (signal.aborted ? 'CANCELLED' : 'RUNTIME_FAILED');
    let stopped = false,
      stopProbe;
    try {
      await stop();
      stopped = true;
    } catch (stopError) {
      stopProbe = stopError.lastProbe;
      e = failure('STOP_UNCONFIRMED');
    }
    const code = e.code ?? originalCode;
    try {
      await save(signal.aborted ? 'cancelled' : 'failed', {
        code,
        originalCode,
        stopProbe,
        stopped,
        exitCode,
        finishReason: deliveryFinished ? 'completed' : undefined,
      });
    } catch {}
    const redact = (text) => text.replaceAll(account.credential.key, '[redacted]');
    try {
      await writeFile(`${dir}/diagnostic.log`, redact(stderr), { mode: 0o600 });
    } catch {}
    try {
      await writeFile(
        `${dir}/response.txt`,
        redact(finalText || events.terminal?.text || '').slice(-2000000),
        { mode: 0o600 },
      );
    } catch {}
    throw Object.assign(failure(code), { stopped });
  } finally {
    signal.removeEventListener('abort', abort);
    lines?.close();
    gateway?.closeAllConnections();
    gateway?.close();
  }
}
