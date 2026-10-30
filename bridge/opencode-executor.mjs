import { classifyProviderError, temporaryProviderReply, providerBackoff, isRateLimitClientError, mergeProviderBackoff, successfulProviderRequest, requestWithProviderBackoff } from './opencode-provider-error.mjs';
// One JSON request on stdin, JSON-line events/result on stdout. No provider secret enters bwrap.
import { spawn } from 'node:child_process';
import { createServer } from 'node:http';
import { createInterface } from 'node:readline';
import { mkdir, readFile, writeFile, appendFile, rename, realpath, stat, cp, lstat } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { pathToFileURL, fileURLToPath } from 'node:url';
import path from 'node:path';
import { workspaceOperation } from './workspaces.mjs';
import { stopUnit } from './process-stop.mjs';
import { openCodeMemory } from './runtime-memory.mjs';
import { requestGatedModel } from './gated-generation.mjs';
import { collectArtifacts, parseArtifactResult } from './artifact-collector.mjs';
import { museTaskDeliveryInstruction } from './task-delivery-contract.mjs';
import { deliveryContract, extractPromptJsonFields, prepareDeliveryManifest, collectDeliveryManifest, deliveryManifestInstructionFor } from './delivery-manifest.mjs';
import { protocolDelivery } from './protocol-delivery.mjs';
import { materializeSourceInputs } from './source-inputs.mjs';
import { verifyProtocolReport, protocolReportDiagnostic, protocolReportRepairInstruction } from './protocol-report-contract.mjs';
import { withProtocolArtifacts } from './protocol-artifacts.mjs';
import { inspectUnitExit } from './unit-exit.mjs';
import { preserveRuntimeArtifactClaims } from './runtime-artifacts.mjs';
import { watchOrphanFixtures } from './opencode-orphan-watch.mjs';
import { prepareOpenCodeWorkspace } from './opencode-preflight.mjs';
import { prepareRepairFinalization, finalizeRepairArtifacts, repairFinalizationInstruction } from './repair-finalizer.mjs';
export { collectArtifacts } from './artifact-collector.mjs';

const DATA = '/var/lib/agentos-bridge/worker-runs';
const ROOT = '/srv/agentos/workspaces';
const NODE = '/opt/deepseek-harness/node/bin/node';
const PROFILE = '/var/lib/agentos-bridge/opencode';
const ACCOUNT = 'agentos:api:00000000-0000-4000-8000-000000000001';
const MODEL = 'glm-5.3-flash';
const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const workspaceKey = /^[a-z0-9_-]+\/[a-z0-9_-]+$/;
const digest = (bytes) => createHash('sha256').update(bytes).digest('hex');
const failure = (code) => Object.assign(new Error(code), { code });

export function parseResult(texts) {
  // Earlier draft messages cannot certify the final response.
  for (const text of texts.filter((text) => text.trim()).slice(-1)) {
    try { return parseArtifactResult(text); } catch {}
  }
  throw failure('RESULT_FORMAT');
}

export function usageFromFrames(text, usage) {
  // Called once for each complete SSE data line. Never estimate missing usage.
  try {
    const value = JSON.parse(text);
    if (value.usage) {
      const u = value.usage;
      usage.input = u.prompt_tokens ?? u.input_tokens ?? usage.input;
      usage.output = u.completion_tokens ?? u.output_tokens ?? usage.output;
      usage.cacheRead = u.prompt_tokens_details?.cached_tokens ?? usage.cacheRead;
      usage.totalTokens = u.total_tokens ?? usage.totalTokens;
      usage.reported = true;
    }
  } catch {}
}

export async function runOpenCode(params, signal, emit = () => {}) {
  try { return await executeOpenCode(params, signal, emit); }
  catch (e) {
    // Errors inside the runtime carry a stop receipt. Other errors are preflight,
    // except duplicate IDs, which might refer to an already-running process.
    if (typeof e.stopped !== 'boolean') e.stopped = e.code !== 'RUN_ALREADY_EXISTS';
    throw e;
  }
}

async function executeOpenCode(params, signal, emit) {
  if (!uuid.test(params.runId) || !workspaceKey.test(params.workspace) ||
      typeof params.prompt !== 'string' || !params.prompt.trim() || params.prompt.length > 120000 ||
      (params.sourceWorkspace && !workspaceKey.test(params.sourceWorkspace))) throw failure('INVALID_REQUEST');
  const writePaths = params.writePaths ?? ['qa-protocol/', 'evidence/'];
  if (!Array.isArray(writePaths) || !writePaths.length ||
      new Set(writePaths).size !== writePaths.length ||
      writePaths.some((item) => !['qa-protocol/', 'evidence/'].includes(item))) throw failure('INVALID_WRITE_SCOPE');
  if ((params.accountId && params.accountId !== ACCOUNT && params.accountId !== ACCOUNT.replaceAll(':', '-')) ||
      (params.model && params.model !== MODEL)) throw failure('INVALID_ROUTE');
  const memory = openCodeMemory(params);
  const account = JSON.parse(await readFile('/var/lib/agentos-bridge/accounts.json', 'utf8'))
    .find((item) => item.id === ACCOUNT);
  if (account?.baseUrl !== 'https://code.verboo.ai/router/v1' || account?.model !== MODEL ||
      typeof account?.credential?.key !== 'string' || !account.credential.key) throw failure('ACCOUNT_CONFIGURATION');
  const mem = await readFile('/proc/meminfo', 'utf8');
  if (Number(mem.match(/MemAvailable:\s+(\d+)/)?.[1] ?? 0) < memory.minAvailableKiB) throw failure('HOST_CAPACITY');
  await mkdir(DATA, { recursive: true, mode: 0o700 });
  const dir = `${DATA}/${params.runId}`;
  try { await mkdir(dir, { mode: 0o700 }); }
  catch (e) { if (e.code === 'EEXIST') throw failure('RUN_ALREADY_EXISTS'); throw e; }
  const workspace = `${ROOT}/${params.workspace}`;
  const home = `${dir}/home`;
  const outputContract = deliveryContract(params.runId, params.deliveryContract?.artifacts?.map(a => a.name) ??
    (params.testFile ? ['protocol-report.json', 'protocol-tests.log'] : params.workKind === 'planning' ? ['scope-plan.json'] : []),
    extractPromptJsonFields(params.prompt));
  let repairSnapshot = null;
  const sock = `${dir}/model.sock`;
  const unit = `agentos-run-${params.runId}`;
  const internal = new AbortController();
  const combined = AbortSignal.any([signal, internal.signal]);
  let requests = 0, toolCalls = 0, child, ended = false, exitCode, stderr = '', gateway;
  let modelOperations = 0, stopOrphanWatch;
  let state = 'starting', terminalError, stopPromise, exitPromise, finishReason, termination;
  let providerWait = null, lastRateLimit = null, savePending = Promise.resolve();
  const texts = [];
  const usage = { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0, cost: null, reported: false };
  const save = (next, extra = {}) => {
    state = next;
    // Serialize immutable snapshots: model calls and physical stop may overlap.
    const bytes = JSON.stringify({
      runId: params.runId, workspace: params.workspace, state, at: new Date().toISOString(),
      unit, runtime: 'opencode', model: MODEL, requests, toolCalls, usage,
      deliveryContract: outputContract, providerBackoff: providerWait, ...extra,
      ...(params.sourceContract?{sourceContractVersion:1}:{}),
    });
    const pending = savePending.then(async () => {
      await writeFile(`${dir}/state.json.tmp`, bytes, { mode: 0o600 });
      await rename(`${dir}/state.json.tmp`, `${dir}/state.json`);
    });
    savePending = pending.catch(() => {});
    return pending;
  };
  const stop = () => stopPromise ??= (async () => {
    internal.abort();
    if (!child) return;
    const proof = await stopUnit(unit, () => ended);
    await writeFile(`${dir}/stop-receipt.json`, JSON.stringify(proof), { mode: 0o600 });
    emit('OPENCODE_STOPPED', { confirmed: true, unit, proof });
  })();
  const abort = () => { terminalError ??= failure('CANCELLED'); void stop().catch(() => {}); };
  signal.addEventListener('abort', abort, { once: true });
  try {
    await save('starting');
    await prepareOpenCodeWorkspace(params, { signal, emit,
      persist: receipt => save('failed', { code: 'DISK_CAPACITY', stopped: true, notStarted: receipt }) });
    if ((await realpath(workspace)) !== workspace) throw failure('INVALID_WORKSPACE');
    const managed = Boolean(params.managed || params.sourceWorkspace);
    for (const name of [...new Set(['evidence', 'data', ...writePaths.map((item) => item.slice(0, -1))])]) {
      await mkdir(`${workspace}/${name}`, { recursive: true });
      if ((await realpath(`${workspace}/${name}`)) !== `${workspace}/${name}`) throw failure('INVALID_WORKSPACE');
    }
    await mkdir(`${home}/.config/opencode`, { recursive: true });
    await mkdir(`${home}/.cache/opencode`, { recursive: true });
    // This executor always ro-binds the source; only data/, evidence/ and
    // qa-protocol/ are writable. Legacy supervised repairs omit readOnlySource,
    // so derive this fact from the enforced mount contract, not task metadata.
    repairSnapshot = await prepareRepairFinalization({...params, managed, readOnlySource: true},
      () => workspaceOperation({action: 'inspect', workspace: params.workspace}), home);
    if (repairSnapshot) outputContract.requiredArtifacts = outputContract.requiredArtifacts.filter(name => name !== 'implementation.patch');
    await prepareDeliveryManifest(home, outputContract);
    // Only dependency files/model catalog are imported. Never auth.json, logs, sessions or configs.
    await cp(`${PROFILE}/.config/opencode/node_modules`, `${home}/.config/opencode/node_modules`, { recursive: true });
    for (const file of ['package.json', 'package-lock.json']) {
      try { await cp(`${PROFILE}/.config/opencode/${file}`, `${home}/.config/opencode/${file}`); }
      catch (e) { if (e.code !== 'ENOENT') throw e; }
    }
    try { await cp(`${PROFILE}/.cache/opencode/models.json`, `${home}/.cache/opencode/models.json`); }
    catch (e) { if (e.code !== 'ENOENT') throw e; }
    try { await cp(`${PROFILE}/.cache/opencode/bin`, `${home}/.cache/opencode/bin`, { recursive: true }); }
    catch (e) { if (e.code !== 'ENOENT') throw e; }
    const sourceInputs = materializeSourceInputs(params.inputs);
    if (sourceInputs.telemetry.materialized) emit('SOURCE_INPUTS_MATERIALIZED', sourceInputs.telemetry);
    for (const input of sourceInputs.inputs) {
      if (!uuid.test(input.id) || !/^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,100}$/.test(input.name)) throw failure('INVALID_INPUT');
      const bytes = Buffer.from(input.base64, 'base64');
      if (bytes.length > 8 * 1024 * 1024 || digest(bytes) !== input.digest) throw failure('INPUT_DIGEST');
      await mkdir(`${home}/inputs/${input.id}`, { recursive: true });
      await writeFile(`${home}/inputs/${input.id}/${input.name}`, bytes, { flag: 'wx' });
    }
    await writeFile(`${home}/context.json`, JSON.stringify({ ...(params.context ?? {}), recovery: params.recoveryContext ?? null }));
    await writeFile(`${home}/opencode.json`, JSON.stringify({
      $schema: 'https://opencode.ai/config.json', model: `agentos/${MODEL}`, small_model: `agentos/${MODEL}`,
      enabled_providers: ['agentos'], autoupdate: false, share: 'disabled', snapshot: false,
      plugin: [], mcp: {}, lsp: false,
      permission: { '*': 'allow', task: 'deny', question: 'deny', webfetch: 'deny', websearch: 'deny', skill: 'deny' },
      agent: { 'agentos-worker': { mode: 'primary', description: 'Isolated AgentOS implementation worker',
        prompt: 'Complete only the assigned task. Tool actions inside this isolated filesystem are authorized. Never claim tests or acceptance without evidence. No delegation. Read /home/agent/context.json. Return the required JSON result.',
        permission: { task: 'deny', question: 'deny', webfetch: 'deny', websearch: 'deny', skill: 'deny' } } },
      provider: { agentos: { npm: '@ai-sdk/openai-compatible', name: 'AgentOS scoped gateway',
        options: { baseURL: 'http://127.0.0.1:17820/v1', apiKey: 'no-provider-secret' },
        models: { [MODEL]: { name: 'GLM 5.3 Flash', tool_call: true, reasoning: true, attachment: true,
          modalities: { input: ['text', 'image'], output: ['text'] },
          interleaved: { field: 'reasoning_content' }, limit: { context: 1048576, output: 32768 },
          options: { reasoningEffort: 'low' }, variants: {
            low: { reasoningEffort: 'low' }, high: { reasoningEffort: 'high' }, max: { reasoningEffort: 'max' },
          } } } } },
    }));
    const exampleArtifacts = params.deliveryContract?.artifacts?.length
      ? params.deliveryContract.artifacts
      : [{ name: 'report.json', path: 'evidence/report.json', mime: 'application/json' }];
    await writeFile(`${home}/prompt.txt`, `${params.prompt}${sourceInputs.instruction}\n\nContext manifest and shared memory: /home/agent/context.json\nInputs: /home/agent/inputs/\nImplement and test the scoped work in /workspace. Source files are mounted read-only; write only under ${writePaths.join(', ')} and use data/ for temporary test state. Routine errors are work to fix within this scope, not grounds to stop. No external network or production files are available. Browser library if needed: /opt/agentos-runtime/node_modules/playwright/index.mjs. Save real evidence in /workspace/evidence. Final response MUST be only JSON {"summary":"what actually changed and was tested, including limitations","artifacts":${JSON.stringify(exampleArtifacts)}}. Paths must be real files relative to /workspace. Do not claim independent acceptance. Do not use reserved artifact names candidate.json or changes.patch; the controller generates these.` + museTaskDeliveryInstruction(params.deliveryContract));
    await appendFile(`${home}/prompt.txt`, deliveryManifestInstructionFor(outputContract));
    if (repairSnapshot) await appendFile(`${home}/prompt.txt`, repairFinalizationInstruction);
    if (params.testFile) await appendFile(`${home}/prompt.txt`, '\n\nContrato da suíte atribuída: ' + protocolReportRepairInstruction('', params.testFile));
    gateway = createServer(async (req, res) => {
      modelOperations++;
      const requestController = new AbortController();
      const requestStartedAt = Date.now();
      res.once('close', () => requestController.abort());
      try {
        if (req.method !== 'POST' || req.url !== '/v1/chat/completions') { res.writeHead(404).end(); return; }
        let body = '';
        for await (const chunk of req) {
          body += chunk;
          if (body.length > 4 * 1024 * 1024) throw failure('CONTEXT_TOO_LARGE');
        }
        const payload = JSON.parse(body);
        if (payload.model !== MODEL) throw failure('INVALID_ROUTE');
        const identity = { runId: params.runId, workspace: params.workspace, model: MODEL };
        const upstreamFailure = await requestWithProviderBackoff(
          { ...identity, providerBackoff: providerWait }, identity, () => requestGatedModel(
          {
            runId: params.runId,
            signal: AbortSignal.any([combined, requestController.signal]),
            emit,
            beforeRequest: () => requestWithProviderBackoff(
              { ...identity, providerBackoff: providerWait }, identity, () => undefined),
            onRequest: () => {
              requests++;
              emit('MODEL_REQUEST', {
                requests,
                contextBytes: Buffer.byteLength(body),
                accountId: ACCOUNT,
                model: MODEL,
              });
            },
          },
          account.baseUrl + '/chat/completions',
          {
            method: 'POST',
            redirect: 'error',
            headers: {
              'content-type': 'application/json',
              authorization: `Bearer ${account.credential.key}`,
            },
            body: JSON.stringify(payload),
          },
          async (response) => {
            if (!response.ok) {
              const observed = await classifyProviderError(response);
              if (observed.code === 'RATE_LIMIT') {
                // Make the admission fence visible before this permit releases.
                lastRateLimit = { ...observed, observedAt: Date.now() };
                providerWait = mergeProviderBackoff(providerWait, providerBackoff(observed, identity));
                return observed;
              }
              emit('MODEL_UPSTREAM_STATUS', { status: observed.status, category: observed.category,
                retryAfterSeconds: observed.retryAfterSeconds });
              throw failure(observed.code);
            }
            const successful = successfulProviderRequest(lastRateLimit, providerWait, requestStartedAt);
            lastRateLimit = successful.failure;
            // An older in-flight success must not erase a newer provider wait.
            if (providerWait !== successful.wait) {
              providerWait = successful.wait;
              await save(state);
            }
            res.writeHead(200, {
              'content-type': response.headers.get('content-type') ?? 'text/event-stream',
              'cache-control': 'no-store',
            });
            const perRequest = {
              input: 0,
              output: 0,
              cacheRead: 0,
              cacheWrite: 0,
              totalTokens: 0,
              reported: false,
            };
            const decoder = new TextDecoder();
            let pending = '';
            for await (const chunk of response.body) {
              res.write(chunk);
              pending += decoder.decode(chunk, { stream: true });
              let newline;
              while ((newline = pending.indexOf('\n')) !== -1) {
                const line = pending.slice(0, newline).trim();
                pending = pending.slice(newline + 1);
                if (line.startsWith('data:')) usageFromFrames(line.slice(5).trim(), perRequest);
              }
              if (pending.length > 8 * 1024 * 1024) throw failure('MODEL_RESPONSE_TOO_LARGE');
            }
            pending += decoder.decode();
            if (pending.trim())
              usageFromFrames(pending.replace(/^data:\s*/, '').trim(), perRequest);
            for (const key of ['input', 'output', 'cacheRead', 'cacheWrite', 'totalTokens'])
              usage[key] += perRequest[key];
            usage.reported ||= perRequest.reported;
            emit('MODEL_RESPONSE', {
              requests,
              tokens: perRequest.reported ? perRequest.totalTokens : null,
            });
          },
          ),
        );
        if (upstreamFailure?.code === 'RATE_LIMIT') {
          combined.throwIfAborted(); requestController.signal.throwIfAborted();
          if (upstreamFailure.localBackoff) {
            lastRateLimit ??= { code: 'RATE_LIMIT', status: 429, observedAt: upstreamFailure.observedAt };
            await save(state);
            const reply = temporaryProviderReply(upstreamFailure);
            res.writeHead(reply.status, reply.headers).end(reply.body);
            return;
          }
          // Gate/transport have already unwound. Persist before the SDK can
          // retry; a restart must not erase the provider's stated minimum wait.
          await save(state);
          emit('MODEL_RATE_LIMIT', { status: 429, category: 'rate_limit',
            retryAfterSeconds: upstreamFailure.retryAfterSeconds });
          const reply = temporaryProviderReply(upstreamFailure);
          res.writeHead(reply.status, reply.headers).end(reply.body);
          return;
        }
        res.end();
      } catch (e) {
        if (combined.aborted || requestController.signal.aborted) { res.destroy(); return; }
        terminalError ??= failure(e.name === 'TimeoutError' ? 'MODEL_REQUEST_TIMEOUT' : e.code && /^[A-Z_]+$/.test(e.code) ? e.code : 'MODEL_REQUEST_FAILED');
        emit('MODEL_ERROR', { code: terminalError.code });
        if (res.headersSent) res.destroy();
        else {
          res.writeHead(502, { 'content-type': 'application/json' });
          res.end(JSON.stringify({ error: { message: terminalError.code, type: 'agentos_gateway' } }));
        }
        void stop().catch(() => {});
      } finally {
        modelOperations--;
      }
    });
    await new Promise((resolve, reject) => { gateway.once('error', reject); gateway.listen(sock, resolve); });
    if (combined.aborted) throw failure('CANCELLED');
    const assets = await lstat(`${workspace}/assets`).catch(() => null);
    if (assets?.isSymbolicLink()) throw failure('INVALID_WORKSPACE');
    const mounts = [
      '--unshare-all', '--die-with-parent', '--new-session',
      '--ro-bind', '/usr', '/usr', '--symlink', 'usr/bin', '/bin', '--symlink', 'usr/sbin', '/sbin',
      '--symlink', 'usr/lib', '/lib', '--symlink', 'usr/lib64', '/lib64',
      '--proc', '/proc', '--dev', '/dev', '--tmpfs', '/tmp', '--dir', '/run',
      '--ro-bind', '/etc/fonts', '/etc/fonts', '--ro-bind', '/etc/ld.so.cache', '/etc/ld.so.cache',
      '--ro-bind', '/opt/deepseek-harness/node', '/opt/deepseek-harness/node',
      '--ro-bind', '/opt/agentos-runtime', '/opt/agentos-runtime',
      '--ro-bind-try', '/opt/agentos-project-deps', '/opt/agentos-project-deps',
      '--ro-bind', '/opt/agentos-runtime/browsers', '/opt/browsers',
      '--ro-bind', '/opt/agentos-opencode/node_modules/opencode-linux-x64/bin/opencode', '/opt/agentos-opencode/opencode',
      '--ro-bind', fileURLToPath(new URL('./opencode-sandbox.mjs', import.meta.url)), '/opt/agentos-bridge/opencode-sandbox.mjs',
      '--ro-bind', workspace, '/workspace',
      ...(assets?.isDirectory() ? ['--ro-bind', `${workspace}/assets`, '/workspace/assets'] : []),
      ...(managed ? ['--ro-bind', `${workspace}/.agentos-snapshot.json`, '/workspace/.agentos-snapshot.json'] : []),
      ...[...new Set(['data/', ...writePaths])].flatMap((item) => [
        '--bind', `${workspace}/${item.slice(0, -1)}`, `/workspace/${item.slice(0, -1)}`,
      ]),
      '--bind', home, '/home/agent', '--ro-bind', sock, '/run/model.sock',
      '--chdir', '/workspace', '--clearenv', '--setenv', 'HOME', '/home/agent',
      '--setenv', 'PYTHONPYCACHEPREFIX', '/home/agent/.cache/python',
      '--setenv', 'PATH', '/opt/deepseek-harness/node/bin:/usr/bin:/bin',
      '--setenv', 'PLAYWRIGHT_BROWSERS_PATH', '/opt/browsers', '--setenv', 'LANG', 'C.UTF-8',
      '--', NODE, '/opt/agentos-bridge/opencode-sandbox.mjs',
    ];
    child = spawn('systemd-run', [
      '--quiet', '--wait', '--pipe', '--collect', `--unit=${unit}`, '-p', memory.memoryMax,
      '-p', 'CPUQuota=150%', '-p', 'TasksMax=256', '-p', 'KillMode=control-group',
      '-p', 'TimeoutStopSec=4', '--', '/usr/bin/python3', fileURLToPath(new URL('./slot-runner.py', import.meta.url)),
      params.workspace, memory.slotMemory, '/usr/bin/bwrap', ...mounts,
    ], { stdio: ['ignore', 'pipe', 'pipe'] });
    exitPromise = new Promise((resolve) => {
      child.once('error', () => { terminalError ??= failure('RUNTIME_START_FAILED'); ended = true; resolve(); });
      child.once('close', (code) => { exitCode = code; ended = true; resolve(); });
    });
    child.stderr.on('data', (chunk) => { if (stderr.length < 100000) stderr += chunk; });
    const lines = createInterface({ input: child.stdout });
    lines.on('line', (line) => {
      if (line.length > 2 * 1024 * 1024) return;
      let item;
      try { item = JSON.parse(line); } catch { return; }
      if (item.type === 'text' && typeof item.part?.text === 'string') {
        texts.push(item.part.text); if (texts.length > 200) texts.shift();
      }
      if (item.type === 'step_finish') finishReason = item.part?.reason;
      if (item.type === 'tool_use' || item.type === 'tool_result') {
        toolCalls++;
        const input = item.part?.state?.input ?? {};
        const detail = String(input.description ?? input.command ?? input.filePath ?? item.part?.state?.title ?? '')
          .replace(/[\u0000-\u001f]/g, ' ').slice(0, 240);
        emit('OPENCODE_TOOL_EVENT', { type: item.type, tool: item.part?.tool ?? null,
          status: item.part?.state?.status ?? null, detail, toolEvents: toolCalls });
      }
      if (item.type === 'error') terminalError ??= failure(lastRateLimit && isRateLimitClientError(item) ? 'RATE_LIMIT' : 'OPENCODE_ERROR');
    });
    await save('running');
    stopOrphanWatch = watchOrphanFixtures(params.runId,
      () => !ended && !combined.aborted && modelOperations === 0, emit);
    emit('RUNTIME_STARTED', { unit, runtime: 'opencode', model: MODEL, workspace: params.workspace,
      isolation: 'mount+pid+network+cgroup', memoryMiB: memory.memoryMiB, memoryReservationId: memory.reservationId, cpuPercent: 150 });
    await exitPromise;
    lines.close();
    if (signal.aborted) throw failure('CANCELLED');
    if (terminalError) throw terminalError;
    if (exitCode !== 0) {
      if (stderr.includes('AGENTOS_HOST_CAPACITY')) throw failure('HOST_CAPACITY');
      if (stderr.includes('AGENTOS_SOURCE_BUSY')) throw failure('SOURCE_BUSY');
      termination = await inspectUnitExit(unit);
      if (termination) emit('RUNTIME_TERMINATION', termination);
      throw failure(termination?.result === 'oom-kill' ? 'OPENCODE_SANDBOX_OOM' : 'RUNTIME_EXITED');
    }
    if (finishReason !== 'stop') throw failure('INCOMPLETE_TURN');
    await stop();
    const manifestDelivery = await collectDeliveryManifest(workspace, home, outputContract);
    let value;
    try { value = manifestDelivery?.value ?? parseResult(texts); }
    catch (e) {
      // A malformed final message must not discard a completed, verifiable suite.
      if (e.code !== 'RESULT_FORMAT' || !params.testFile) throw e;
      value = await protocolDelivery(workspace, params.testFile);
      emit('RESULT_RECOVERED', { reason:'RESULT_FORMAT' });
    }
    const delivery = withProtocolArtifacts(value, params.testFile);
    const artifacts = await collectArtifacts(delivery, workspace);
    if (manifestDelivery) {
      for (const original of manifestDelivery.artifacts)
        if (!artifacts.some(a => a.name === original.name && a.digest === original.digest)) throw failure('ARTIFACT_CORRUPT');
      emit('DELIVERY_MANIFEST_COLLECTED', { runId: params.runId, digest: manifestDelivery.manifestDigest, artifacts: manifestDelivery.artifacts.length });
    }
    if (delivery.artifacts.length !== value.artifacts.length)
      emit('PROTOCOL_ARTIFACTS_INCLUDED', { count: delivery.artifacts.length - value.artifacts.length });
    if (params.testFile) {
      const report = artifacts.find((item) => item.name === 'protocol-report.json');
      let parsed;
      try { parsed = report && JSON.parse(Buffer.from(report.base64, 'base64').toString('utf8')); }
      catch { /* Fixed contract diagnostic below; never emit private report contents. */ }
      verifyProtocolReport(parsed, params.testFile);
    }
    await finalizeRepairArtifacts({params, workspace, snapshot: repairSnapshot, stopped: true, artifacts, emit});
    if (managed) {
      const candidate = await workspaceOperation({ action: 'inspect', workspace: params.workspace,
        ...(params.sourceContract?{includeBaseFiles:true}:{}) });
      if (params.readOnlySource && candidate.baseDigest !== candidate.candidateDigest) throw failure('SOURCE_CHANGED_DURING_VALIDATION');
      const renamed = preserveRuntimeArtifactClaims(artifacts);
      if (renamed.length) emit('RUNTIME_ARTIFACT_CLAIMS_PRESERVED', { claims: renamed });
      for (const [name, content] of [['candidate.json', JSON.stringify({ ...candidate, patch: undefined })], ['changes.patch', candidate.patch]]) {
        if (artifacts.some((item) => item.name === name)) throw failure('RESERVED_ARTIFACT_NAME');
        const bytes = Buffer.from(content);
        if (bytes.length > 8 * 1024 * 1024) throw failure('ARTIFACT_TOO_LARGE');
        artifacts.push({ name, mime: name.endsWith('.json') ? 'application/json' : 'text/plain',
          digest: digest(bytes), base64: bytes.toString('base64') });
      }
    }
    if (artifacts.reduce((sum, item) => sum + Buffer.byteLength(item.base64, 'base64'), 0) > 24 * 1024 * 1024) throw failure('ARTIFACT_TOO_LARGE');
    const result = { summary: value.summary, artifacts, usage,
      ...(manifestDelivery ? { manifest: { version: 1, runId: params.runId, digest: manifestDelivery.manifestDigest } } : {}),
      evidence: { runtime: 'opencode', requests, toolCalls, exitCode, finishReason, stopped: true } };
    await writeFile(`${dir}/result.json`, JSON.stringify(result), { mode: 0o600 });
    await save('completed', { stopped: true, exitCode, finishReason, artifacts: artifacts.map(({ base64, ...item }) => item) });
    return result;
  } catch (e) {
    const originalCode = e.code ?? 'RUNTIME_FAILED';
    const diagnostic = protocolReportDiagnostic(originalCode, params.testFile);
    if (diagnostic) emit('PROTOCOL_REPORT_REJECTED', { code: originalCode, ...diagnostic });
    let stopped = false;
    let stopProbe;
    try { await stop(); stopped = true; } catch (stopError) { stopProbe = stopError.lastProbe; e = failure('STOP_UNCONFIRMED'); }
    try { await save(signal.aborted ? 'cancelled' : 'failed', { code: e.code ?? 'RUNTIME_FAILED', originalCode, stopProbe, stopped, exitCode, finishReason, ...(e.notStarted ? { notStarted: e.notStarted } : {}), ...(diagnostic ? { diagnostic } : {}), ...(termination ? {termination} : {}) }); } catch {}
    // Only sandbox stderr: the sandbox never received provider credentials.
    try { await writeFile(`${dir}/diagnostic.log`, stderr, { mode: 0o600 }); } catch {}
    try { await writeFile(`${dir}/response.txt`, texts.join('\n').slice(-2000000), { mode:0o600 }); } catch {}
    throw Object.assign(failure(e.code ?? 'RUNTIME_FAILED'), { stopped, ...(diagnostic ? { diagnostic } : {}) });
  } finally {
    stopOrphanWatch?.();
    signal.removeEventListener('abort', abort);
    gateway?.closeAllConnections(); gateway?.close();
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const controller = new AbortController();
  for (const name of ['SIGTERM', 'SIGINT', 'SIGHUP']) process.once(name, () => controller.abort());
  process.stdout.on('error', () => controller.abort());
  const send = (value) => { if (!process.stdout.destroyed) process.stdout.write(JSON.stringify(value) + '\n'); };
  try {
    let body = '';
    for await (const chunk of process.stdin) {
      body += chunk;
      if (body.length > 36 * 1024 * 1024) throw failure('REQUEST_TOO_LARGE');
    }
    // Normal stdin EOF frames the request; it is not cancellation.
    const result = await runOpenCode(JSON.parse(body), controller.signal,
      (event, data) => send({ type: 'event', event, data }));
    send({ type: 'result', result });
  } catch (e) {
    send({ type: 'error', error: { code: e.code ?? 'RUNTIME_FAILED', stopped: e.stopped ?? true } });
    process.exitCode = 1;
  }
}
