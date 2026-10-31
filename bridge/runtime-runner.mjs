import { assertModelRoute, usesSharedRequestGate } from './model-policy.mjs';
import { workerModelSettings } from './operator-model-profile.mjs';
import { createRuntimeGuidanceServer } from './runtime-guidance.mjs';
import { prepareSandboxLocalhost } from './sandbox-localhost.mjs';
import { prepareRepairFinalization, finalizeRepairArtifacts, repairFinalizationInstruction } from './repair-finalizer.mjs';
import { spawn } from 'node:child_process';
import { createServer } from 'node:http';
import { createInterface } from 'node:readline';
import { mkdir, readFile, writeFile, realpath, stat, readdir } from 'node:fs/promises';
import { createHash, randomUUID } from 'node:crypto';
import { GLM_REQUEST_ADMITTED, withAccountRequestGate } from './gated-generation.mjs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';
import { toGeneration, completion } from './runtime-gateway.mjs';
import { bufferedModelChannel, WORKER_MODEL_TIMEOUT_MS } from './runtime-model-channel.mjs';
import { workspaceOperation } from './workspaces.mjs';
import { materializeSourceInputs } from './source-inputs.mjs';
import {hydrateArtifactInputBundle,referencedInputLimitBytes} from './artifact-input-bundle.mjs';
import {bindCompositionInputs} from './composition-inputs.mjs';
import { preserveRuntimeArtifactClaims, runtimeArtifactInstruction } from './runtime-artifacts.mjs';
import { museTaskDeliveryInstruction } from './task-delivery-contract.mjs';
import { deliveryContract, extractPromptJsonFields, prepareDeliveryManifest, collectDeliveryManifest, deliveryManifestInstructionFor } from './delivery-manifest.mjs';
import {analyzePollTrace} from './runtime-progress.mjs';
import {inspectRuntimeProgress,checkpointRuntimeProgress} from './inspect-runtime-progress.mjs';
import {captureRuntimeHandoff,runtimeHandoffInstruction} from './runtime-handoff.mjs';
import {modelTransportFailure,generationObserver} from './model-transport-failure.mjs';
import {harnessMemory} from './runtime-memory.mjs';
import {stopUnit} from './process-stop.mjs';
import {inspectUnitExit} from './unit-exit.mjs';
import {harnessFailureCode,harnessTurnFailureCode} from './harness-termination.mjs';
import {projectHarnessToolFailure,rememberHarnessToolCall,forgetHarnessToolCall} from './runtime-tool-failure.mjs';

const DATA = '/var/lib/agentos-bridge/worker-runs';
const ROOT = '/srv/agentos/workspaces';
const NODE = '/opt/deepseek-harness/node/bin/node';
const uuid = /^[0-9a-f-]{36}$/;
const key = /^[a-z0-9_-]+\/[a-z0-9_-]+$/;
const digest = (b) => createHash('sha256').update(b).digest('hex');
const error = (code, message = code) => Object.assign(new Error(message), { code });
export async function runState(id) {
  if (!uuid.test(id)) throw error('INVALID_RUN');
  return JSON.parse(await readFile(`${DATA}/${id}/state.json`, 'utf8'));
}
export async function workerWorkspaces() {
  const result = [];
  for (const p of await readdir(ROOT, { withFileTypes: true })) {
    if (!p.isDirectory() || !/^[a-z0-9_-]+$/.test(p.name)) continue;
    for (const w of await readdir(`${ROOT}/${p.name}`, { withFileTypes: true }))
      if (w.isDirectory() && /^[a-z0-9_-]+$/.test(w.name))
        result.push({ id: p.name + '/' + w.name, project: p.name, name: w.name });
  }
  return result;
}
export async function runHarness(params, signal, generate, emit = () => {}) {
  try { assertModelRoute(params.accountId, params.model); }
  catch(e){emit('HARNESS_NOT_STARTED',{confirmed:true,phase:'model_policy',code:e.code??'MODEL_POLICY_GLM_ONLY'});throw e;}
  try{params=await hydrateArtifactInputBundle(params,signal);}
  catch(e){emit('HARNESS_NOT_STARTED',{confirmed:true,phase:'input_bundle',code:e.code??'INPUT_BUNDLE_INVALID'});throw e;}
  const retryGuidance=params.retryGuidance;
  const guidanceUuid=/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/i;
  // Bind the controller's received prompt before legitimate runtime enrichment
  // such as patch-composition findings appends context to it.
  if(retryGuidance && (Object.keys(retryGuidance).sort().join(',')!=='bindingId,instructionDigest,promptDigest,requestId'||
    !guidanceUuid.test(retryGuidance.bindingId??'')||!guidanceUuid.test(retryGuidance.requestId??'')||
    !/^[a-f0-9]{64}$/.test(retryGuidance.instructionDigest??'')||
    typeof params.prompt!=='string'||createHash('sha256').update(params.prompt).digest('hex')!==retryGuidance.promptDigest)){
    emit('HARNESS_NOT_STARTED',{confirmed:true,phase:'initial_guidance',code:'RETRY_GUIDANCE_PROMPT_CHANGED'});
    throw error('RETRY_GUIDANCE_PROMPT_CHANGED');
  }
  if (params.accountId?.startsWith('agentos:muse:')) {
    const accounts = JSON.parse(await readFile('/var/lib/agentos-bridge/accounts.json', 'utf8'));
    const account = accounts.find(a => a.id === params.accountId && a.family === 'muse');
    if (!account) throw error('ACCOUNT_MISSING');
    const { museCredential } = await import('./muse-accounts.mjs');
    const museVersion = digest(await readFile(new URL('./muse-runner.mjs', import.meta.url)));
    const { runMuse } = await import('./muse-runner.mjs?v=' + museVersion);
    return runMuse(params, signal, { ...account, credential: await museCredential(account) }, emit);
  }
  const collectorVersion = digest(await readFile(new URL('./artifact-collector.mjs', import.meta.url)));
  const { collectArtifacts, parseArtifactResult } = await import('./artifact-collector.mjs?v=' + collectorVersion);
  if (!uuid.test(params.runId) || !key.test(params.workspace)) throw error('INVALID_WORKSPACE');
  const workspace = `${ROOT}/${params.workspace}`;
  if ((await realpath(workspace)) !== workspace) throw error('INVALID_WORKSPACE');
  const memory = harnessMemory(params), memoryMiB = memory.memoryMiB;
  if (params.managed)
    for (const name of ['evidence', 'data']) {
      await mkdir(`${workspace}/${name}`, { recursive: true });
      if ((await realpath(`${workspace}/${name}`)) !== `${workspace}/${name}`)
        throw error('INVALID_WORKSPACE');
    }
  const assetsReadOnly =
    params.managed &&
    (await stat(`${workspace}/assets`).then(
      (s) => s.isDirectory(),
      () => false,
    ));
  // Admission includes host activity outside AgentOS; the slot runner also checks before exec.
  const mem = await readFile('/proc/meminfo', 'utf8');
  if (Number(mem.match(/MemAvailable:\s+(\d+)/)?.[1] ?? 0) < memory.minAvailableKiB) throw error('HOST_CAPACITY');
  await mkdir(DATA, { recursive: true, mode: 0o700 });
  const dir = `${DATA}/${params.runId}`,
    home = `${dir}/home`,
    sock = `${dir}/model.sock`;
  try {
    await mkdir(dir, { mode: 0o700 });
  } catch (e) {
    if (e.code === 'EEXIST') throw error('RUN_ALREADY_EXISTS');
    throw e;
  }
  const loopbackHosts = await prepareSandboxLocalhost(home);
  const outputContract = deliveryContract(params.runId, params.deliveryContract?.artifacts?.map(a => a.name) ?? [], extractPromptJsonFields(params.prompt));
  const repairSnapshot = await prepareRepairFinalization(params,
    () => workspaceOperation({action: 'inspect', workspace: params.workspace}), home);
  if (repairSnapshot) outputContract.requiredArtifacts = outputContract.requiredArtifacts.filter(name => name !== 'implementation.patch');
  await prepareDeliveryManifest(home, outputContract);
  const save = async (state, extra = {}) =>
    writeFile(
      `${dir}/state.json`,
      JSON.stringify({
        runId: params.runId,
        ...(params.runFence ? {runFence:params.runFence} : {}),
        workspace: params.workspace,
        runtime: 'harness',
        memoryMiB,
        ...(memory.reservationId ? {memoryReservationId: memory.reservationId} : {}),
        deliveryContract: outputContract,
        ...(params.sourceContract?{sourceContractVersion:1}:{}),
        state,
        at: new Date().toISOString(),
        ...extra,
      }),
      { mode: 0o600 },
    );
  await save('starting');
  signal.throwIfAborted();
  const composition=params.composition && params.managed && !params.readOnlySource
    ? await workspaceOperation({action:'compose',workspace:params.workspace,composition:bindCompositionInputs(params.composition,params.inputs)}) : null;
  if(composition){
    await writeFile(`${home}/runtime-composition.json`,JSON.stringify(composition));
    emit('PATCH_COMPOSITION_RECORDED',composition);
    params={...params,prompt:params.prompt+'\nRUNTIME PATCH COMPOSITION (not acceptance): '+
      JSON.stringify({complete:composition.complete,applied:composition.receipts.filter(r=>r.status!=='conflict').length,
        conflicts:composition.receipts.filter(r=>r.status==='conflict').length,report:'/home/agent/runtime-composition.json'})+
      '\nRead the exact runtime composition report. Compatible patches are already applied; resolve only reported conflicts and run the candidate regressions plus all existing delivery criteria.'};
  }
  const sourceInputs = materializeSourceInputs(params.inputs,params.inputBundle?{totalBytes:referencedInputLimitBytes}:{});
  if (sourceInputs.telemetry.materialized) emit('SOURCE_INPUTS_MATERIALIZED', sourceInputs.telemetry);
  for (const input of sourceInputs.inputs) {
    if (!uuid.test(input.id) || !/^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,100}$/.test(input.name))
      throw error('INVALID_INPUT');
    const dest = `${home}/inputs/${input.id}`;
    await mkdir(dest, { recursive: true });
    const bytes = Buffer.from(input.base64, 'base64');
    if (digest(bytes) !== input.digest) throw error('INPUT_DIGEST');
    await writeFile(`${dest}/${input.name}`, bytes, { flag: 'wx' });
  }
  await writeFile(`${home}/context.json`, JSON.stringify(params.context));
  let requests = 0,
    toolCalls = 0,
    toolResults = 0,
    toolEvents = 0,
    gatewayError,
    cancelled = false;
  const usage = { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0, cost: null };
  // Only a cheap in-memory hint runs on tool events. Authoritative inspection
  // happens at the next generation boundary, while no new tool can be issued.
  const progressRows=[];
  // Count identities, not just totals: an orphan result cannot conceal an
  // unfinished tool. Malformed/duplicate identities disable this proof.
  const pendingTransportTools=new Set(), completedTransportTools=new Set();
  let transportToolTraceComplete=true;
  let progressSequence=0,progressProbeDue=false,progressCheckpoint=null;
  let handoffAt=0,handoffDigest=null;
  const checkpointHandoff=async()=>{
    if (!params.managed || !params.runFence) return;
    handoffAt=Date.now();
    const handoff=await captureRuntimeHandoff(params);
    if(handoff && handoff.digest!==handoffDigest){handoffDigest=handoff.digest;emit('RUNTIME_HANDOFF',handoff);}
  };
  const gateway = createServer(async (req, res) => {
    const channel = bufferedModelChannel(res, signal);
    const requestId = randomUUID();
    let requestSequence;
    let generationAttempts=0;
    // GLM uses a semantic-idle watchdog in its actual provider transport,
    // after admission. Queuing and a progressing stream do not spend a fixed
    // wall-clock generation deadline; caller cancellation still owns the run.
    const gatedRequest = usesSharedRequestGate(params.accountId);
    const requestTimeout = gatedRequest ? null : AbortSignal.timeout(WORKER_MODEL_TIMEOUT_MS);
    try {
      if (req.method !== 'POST' || req.url !== '/v1/chat/completions') {
        res.writeHead(404);
        res.end();
        return;
      }
      let body = '';
      for await (const chunk of req) {
        body += chunk;
        if (body.length > 1500000) throw error('CONTEXT_TOO_LARGE');
      }
      channel.start();
      if(progressProbeDue&&uuid.test(params.runFence??'')) {
        progressProbeDue=false;
        const proof=await inspectRuntimeProgress(params.runId,params.workspace);
        if(proof.stalled&&!signal.aborted) {
          try {
            progressCheckpoint=await checkpointRuntimeProgress(proof,
              {runId:params.runId,runFence:params.runFence,workspace:params.workspace},signal);
          } catch(e) {
            if(signal.aborted)throw error('CANCELLED');
            emit('RUNTIME_PROGRESS_OBSERVATION_FAILED',{runId:params.runId,code:'CHECKPOINT_UNAVAILABLE'});
          }
          if(progressCheckpoint&&!signal.aborted) {
            emit('RUNTIME_PROGRESS_STALL_CONFIRMED',progressCheckpoint);
            // The ordinary catch owns shutdown and verifies the sandbox stopped.
            // This does not send a prompt or pretend the task has been accepted.
            throw error('RUNTIME_PROGRESS_STALLED');
          }
        }
      }
      if(Date.now()-handoffAt>60000)await checkpointHandoff();
      const input = toGeneration(JSON.parse(body), {
        accountId: params.accountId,
        model: params.model,
        reasoning: workerModelSettings(params.accountId, params.model).reasoning,
      });
      input.requestId = requestId;
      requestSequence = ++requests;
      emit('MODEL_REQUEST', {
        requestId, requests: requestSequence,
        contextBytes: body.length,
        accountId: params.accountId,
        model: params.model,
      });
      // Own admission here as well: an older long-lived bridge can load this
      // runtime while retaining its original ungated generate callback. Newer
      // bridges recognize the private symbol and do not acquire twice.
      const generationSignal=requestTimeout?AbortSignal.any([channel.signal,requestTimeout]):channel.signal;
      let result;
      for(let generationAttempt=1;;generationAttempt++){
        generationAttempts=generationAttempt;
        const generationInput={...input,generationAttempt};
        try {
          result=await withAccountRequestGate(
            gatedRequest ? { ...generationInput, runId: params.runId } : generationInput,
            generationSignal,
            (requestSignal) => generate(generationInput, requestSignal, GLM_REQUEST_ADMITTED),
            generationObserver(emit,{requestId,generationAttempt}),
          );
          break;
        } catch(e) {
          // Each rejected generation has fully unwound its transport and gate.
          // Nothing but keepalive comments has reached the SDK; earlier tools
          // are settled. Retry this immutable generation, never the tool loop.
          const reason=modelTransportFailure(e);
          if(!reason||generationAttempt>=3||generationSignal.aborted||
            !transportToolTraceComplete||pendingTransportTools.size!==0)throw e;
          emit('MODEL_GENERATION_RETRY',{requestId,requests:requestSequence,generationAttempt,
            reason,delayMs:750*generationAttempt,responseDelivered:false});
          await delay(750*generationAttempt,undefined,{signal:generationSignal});
        }
      }
      for (const k of ['input', 'output', 'cacheRead', 'cacheWrite', 'totalTokens'])
        usage[k] += result.message.usage?.[k] ?? 0;
      emit('MODEL_RESPONSE', {
        requestId, requests: requestSequence,generationAttempt:generationAttempts,
        stopReason: result.message.stopReason,
        tokens: result.message.usage?.totalTokens ?? null,
      });
      channel.finish(completion(result.message, params.model));
    } catch (e) {
      gatewayError = channel.failureCode(requestTimeout?.aborted ? requestTimeout.reason : e);
      emit('MODEL_ERROR', { requestId, requests: requestSequence,generationAttempt:generationAttempts,code: gatewayError });
      channel.fail(gatewayError);
      // A broken provider call must not leave an apparently running, idle agent loop.
      fail(error(gatewayError));
    } finally {
      channel.dispose();
    }
  });
  await new Promise((resolve, reject) => {
    gateway.once('error', reject);
    gateway.listen(sock, resolve);
  });
  const guidanceIdentity = uuid.test(params.goalId ?? '') && uuid.test(params.taskId ?? '') && uuid.test(params.contextVersion ?? '') && uuid.test(params.runFence ?? '') ? {goalId:params.goalId,taskId:params.taskId,runId:params.runId,runFence:params.runFence,contextVersion:params.contextVersion} : null;
  // Authority is supplied by the owning runtime, outside worker-writable home.
  // The immutable mount binds every managed operation to this attempt/input.
  const operationPolicy = params.managed && guidanceIdentity ? {
    version:1,...guidanceIdentity,workspace:'/workspace',
    sourceSnapshotDigest:digest(await readFile(`${workspace}/.agentos-snapshot.json`)),
    maxDeadlineMs:3600000,
  } : null;
  if(operationPolicy)await writeFile(`${dir}/operation-policy.json`,JSON.stringify(operationPolicy),{flag:'wx',mode:0o600});
  let guidance;
  const observedToolCalls = new Map();
  const unit = 'agentos-run-' + params.runId;
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
    ...loopbackHosts.mount,
    '--ro-bind',
    '/opt/deepseek-harness/node',
    '/opt/deepseek-harness/node',
    '--ro-bind',
    '/opt/agentos-runtime/harness-node_modules',
    '/opt/deepseek-harness/app/node_modules',
    '--ro-bind',
    fileURLToPath(new URL('.', import.meta.url)),
    '/opt/agentos-bridge',
    '--ro-bind',
    '/opt/agentos-runtime',
    '/opt/agentos-runtime',
    '--ro-bind-try', '/opt/agentos-project-deps', '/opt/agentos-project-deps',
    '--ro-bind',
    '/opt/agentos-runtime/browsers',
    '/opt/browsers',
    params.readOnlySource ? '--ro-bind' : '--bind',
    workspace,
    '/workspace',
    ...(assetsReadOnly ? ['--ro-bind', `${workspace}/assets`, '/workspace/assets'] : []),
    ...(params.managed
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
    ...(operationPolicy?['--ro-bind',`${dir}/operation-policy.json`,'/run/operation-policy.json']:[]),
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
    ...(operationPolicy?['--setenv','AGENTOS_MANAGED_OPERATIONS','1']:[]),
    ...(guidanceIdentity ? ['--setenv','AGENTOS_RUN_ID',params.runId,'--setenv','AGENTOS_RUN_FENCE',params.runFence,
      '--setenv','AGENTOS_GUIDANCE_DEADLINE_NS',(process.hrtime.bigint()+600_000_000_000n).toString()] : []),
    '--',
    NODE,
    '/opt/agentos-bridge/sandbox-entry.mjs',
  ];
  const child = spawn(
    'systemd-run',
    [
      '--quiet',
      '--wait',
      '--pipe',
      '--collect',
      '--unit=' + unit,
      '-p',
      memory.memoryMax,
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
      memory.slotMemory,
      '/usr/bin/bwrap',
      ...mounts,
    ],
    { stdio: ['pipe', 'pipe', 'pipe'] },
  );
  child.stdin.on('error', () => {});
  let ended = false,
    exitCode,
    stderr = '',
    finalText = '',
    turnReason,
    running = false,
    receiptId,
    readyIdle = false,
    nextId = 0;
  const receipts = new Set(),
    pending = new Map();
  let doneResolve, doneReject;
  const done = new Promise((resolve, reject) => {
    doneResolve = resolve;
    doneReject = reject;
  });
  done.catch(() => {});
  const fail = (e) => {
    doneReject(e);
    for (const p of pending.values()) p.reject(e);
    pending.clear();
  };
  const exited = new Promise((resolve) => {
    child.on('close', (code) => {
      ended = true;
      exitCode = code;
      fail(
        error(
          stderr.includes('AGENTOS_HOST_CAPACITY')
            ? 'HOST_CAPACITY'
            : stderr.includes('AGENTOS_SOURCE_BUSY')
              ? 'SOURCE_BUSY'
              : 'RUNTIME_EXITED',
        ),
      );
      resolve();
    });
    child.on('error', () => {
      ended = true;
      fail(error('RUNTIME_START_FAILED'));
      resolve();
    });
  });
  child.stderr.on('data', (b) => {
    if (stderr.length < 100000) stderr += b;
  });
  const lines = createInterface({ input: child.stdout });
  const finish = () => {
    if (running && readyIdle && receipts.has(receiptId)) doneResolve();
  };
  lines.on('line', (line) => {
    let m;
    try {
      m = JSON.parse(line);
    } catch {
      return;
    }
    if (m.id !== undefined) {
      const p = pending.get(m.id);
      if (p) {
        pending.delete(m.id);
        m.error ? p.reject(error('RUNTIME_RPC_ERROR')) : p.resolve(m.result);
      }
      return;
    }
    if (m.params?.sessionId !== params.runId) return;
    if (m.method === 'session.status') {
      if (m.params.status === 'running') {
        running = true;
        readyIdle = false;
      } else if (m.params.status === 'idle') readyIdle = true;
      finish();
    }
    if (m.method === 'session.guidanceClaimed' && guidance)
      void guidance.observe([m.params?.messageId],m.params).catch(() => emit('RUNTIME_GUIDANCE_OBSERVATION_FAILED', {runId:params.runId}));
    if (m.method === 'session.event') {
      const e = m.params.event;
      if(e?.type==='tool/call'){
        const id=e.data?.callId;
        if(typeof id!=='string'||!id||pendingTransportTools.has(id)||completedTransportTools.has(id))transportToolTraceComplete=false;
        else pendingTransportTools.add(id);
      }
      if(e?.type==='tool/result'){
        const id=e.data?.message?.source?.callId,blocks=e.data?.message?.content;
        if(typeof id!=='string'||!pendingTransportTools.has(id)||!Array.isArray(blocks)||
          blocks.filter(b=>b?.type==='tool-result'&&b.toolCallId===id).length!==1)transportToolTraceComplete=false;
        else {pendingTransportTools.delete(id);completedTransportTools.add(id);}
      }
      rememberHarnessToolCall(e, observedToolCalls);
      if(['tool/call','tool/result','agent/inbox/spliced','turn/end'].includes(e?.type)) {
        progressRows.push({...e,seq:++progressSequence,time:Date.now()});
        if(progressRows.length>64)progressRows.shift();
        progressProbeDue=!!analyzePollTrace(progressRows.map(row=>JSON.stringify(row)).join('\n'));
      }
      if (e?.type === 'agent/inbox/spliced')
        for (const r of e.data?.inserted ?? []) receipts.add(r.id);
      if (e?.type === 'assistant/message')
        finalText = (e.data?.message?.content ?? [])
          .filter((b) => b.type === 'text')
          .map((b) => b.text)
          .join('\n');
      if (e?.type === 'turn/end') turnReason = e.data?.reason;
      // Calls and results are separate SDK events. Keep the historical event
      // counter for activity consumers without reporting each result as a call.
      if (e?.type === 'tool/call') toolCalls++;
      if (e?.type === 'tool/result') toolResults++;
      if (/tool/.test(e?.type ?? '')) {
        toolEvents++;
        emit('HARNESS_TOOL_EVENT', { type: e.type, toolEvents, toolCalls, toolResults });
      }
      const toolFailure = projectHarnessToolFailure(e, guidanceIdentity, observedToolCalls);
      if (toolFailure) emit('HARNESS_TOOL_FAILURE', toolFailure);
      forgetHarnessToolCall(e, observedToolCalls);
      finish();
    }
  });
  const rpc = (method, args) =>
    new Promise((resolve, reject) => {
      const id = ++nextId;
      pending.set(id, { resolve, reject });
      child.stdin.write(JSON.stringify({ jsonrpc: '2.0', id, method, params: args }) + '\n');
    });
  let stopPromise, stopProof;
  const stop = () =>
    (stopPromise ??= (async () => {
      const guidanceClosed = guidance?.close();
      child.stdin.end();
      await Promise.race([exited, delay(1500)]);
      stopProof = await stopUnit(unit, () => ended);
      await guidanceClosed;
      emit('HARNESS_STOPPED', { confirmed: true, unit, proof: stopProof,
        workspace:params.workspace, ...(params.runFence ? {runFence:params.runFence} : {}) });
    })());
  const abort = () => {
    cancelled = true;
    fail(error('CANCELLED'));
    void stop().catch(() => {});
  };
  signal.addEventListener('abort', abort, { once: true });
  let initTimer;
  try {
    if (signal.aborted) throw error('CANCELLED');
    const info = await Promise.race([
      rpc('initialize', {
        cwd: '/workspace',
        provider: 'deepseek-official',
        model: params.model,
        maxTokens: 8192,
      }),
      new Promise((_, reject) => {
        initTimer = setTimeout(() => reject(error('INITIALIZE_TIMEOUT')), 45000);
      }),
    ]).finally(() => clearTimeout(initTimer));
    if (info.serverInfo?.name !== 'deepseek-harness-sdk-runtime') throw error('INVALID_RUNTIME');
    await save('running', { unit });
    emit('HARNESS_STARTED', {
      unit,
      runtime: 'dsh-0.1.6-alpha.2',
      isolation: 'mount+pid+network+cgroup',
      workspace: params.workspace,
      memoryMiB,
      ...(memory.reservationId ? {memoryReservationId: memory.reservationId} : {}),
    });
    const roleClosure = params.readOnlySource && params.workKind !== 'repair'
      ? '\nThis is independent read-only verification. Publish a valid negative or inconclusive verification delivery when a bounded discriminating test proves a defect or cannot validate a required criterion. Keep passed:false and identify the exact missing evidence; finishing this scoped verification is not acceptance of the objective. The supervisor owns implementation repair. Preserve the immutable source. Do not repeat an already proven broad suite or keep polling the same failed browser; use an external watchdog, preserve partial tool evidence and return the bound report and manifest.\n'
      : '';
    const exampleArtifacts = params.deliveryContract?.artifacts?.length
      ? params.deliveryContract.artifacts
      : [{ name: 'report.json', path: 'evidence/report.json', mime: 'application/json' }];
    const prompt = `${params.prompt}${roleClosure}${sourceInputs.instruction}\n\nContext manifest and shared memory: /home/agent/context.json\nInputs: /home/agent/inputs/\nRoutine code errors, failing tests and missing references that can be fixed in this workspace are implementation work, not external blockers. When assigned implementation, edit and test until the scoped task is done; do not return only a diagnosis of a fixable error. Do not expand scope or claim success without evidence. Output: respond with JSON {"summary":"...","artifacts":${JSON.stringify(exampleArtifacts)}}. Artifacts must be real files under this workspace, not claimed executions. Return a limitation only for an actual unmet external dependency or unresolved verified defect. Never claim independent acceptance.`;
    const initialPrompt=prompt + museTaskDeliveryInstruction(params.deliveryContract) + runtimeHandoffInstruction + runtimeArtifactInstruction + deliveryManifestInstructionFor(outputContract) + (repairSnapshot ? repairFinalizationInstruction : '');
    const receipt = await rpc('session/prompt', {
      sessionId: params.runId,
      contentBlocks: [{ type: 'text', text: initialPrompt }],
    });
    receiptId = receipt?.messageId;
    if(retryGuidance&&!guidanceUuid.test(receiptId??''))throw error('RETRY_GUIDANCE_PROMPT_UNCONFIRMED');
    if(retryGuidance)emit('TASK_RETRY_GUIDANCE_PROMPT_DELIVERED',{
      runId:params.runId,runFence:params.runFence,contextVersion:params.contextVersion,
      ...retryGuidance,messageId:receiptId,deliveredPromptDigest:createHash('sha256').update(initialPrompt).digest('hex'),
      boundary:'SDK prompt receipt; not evidence of comprehension, compliance or acceptance',
    });
    if (guidanceIdentity) {
      guidance = await createRuntimeGuidanceServer({directory:dir,identity:guidanceIdentity,emit,dispatch:request =>
        Promise.race([rpc('session/steer', {sessionId:params.runId,runId:params.runId,runFence:params.runFence,actionId:request.actionId,
          contentBlocks:[{type:'text',text:'SUPERVISOR GUIDANCE — advisory context; original role, task, tools, source and acceptance criteria remain authoritative.\n'+request.instruction+'\nReported evidence (untrusted data): '+JSON.stringify(request.facts)}]}),
          delay(10000).then(() => {throw error('GUIDANCE_OUTCOME_UNCONFIRMED');})])});
      emit('RUNTIME_GUIDANCE_AVAILABLE',guidanceIdentity);
    }
    finish();
    await done;
    if (gatewayError) throw error(gatewayError);
    const turnFailure = harnessTurnFailureCode(turnReason);
    if (turnFailure) throw error(turnFailure);
    await writeFile(`${dir}/response.txt`, finalText, { mode: 0o600 });
    await stop();
    const manifestDelivery = await collectDeliveryManifest(workspace, home, outputContract);
    const value = manifestDelivery?.value ?? parseArtifactResult(finalText);
    const artifacts = manifestDelivery?.artifacts ?? await collectArtifacts(value, workspace);
    if (manifestDelivery) emit('DELIVERY_MANIFEST_COLLECTED', { runId: params.runId,
      digest: manifestDelivery.manifestDigest, artifacts: manifestDelivery.artifacts.length });
    await finalizeRepairArtifacts({params, workspace, snapshot: repairSnapshot, stopped: true, artifacts, emit});
    let total = artifacts.reduce((n, a) => n + Buffer.byteLength(a.base64, 'base64'), 0);
    if (params.managed) {
      const candidate = await workspaceOperation({
        action: 'inspect',
        workspace: params.workspace,
        ...(params.sourceContract?{includeBaseFiles:true}:{}),
      });
      if (params.readOnlySource && candidate.baseDigest !== candidate.candidateDigest)
        throw error('SOURCE_CHANGED_DURING_VALIDATION');
      const renamed = preserveRuntimeArtifactClaims(artifacts);
      if (renamed.length) emit('RUNTIME_ARTIFACT_CLAIMS_PRESERVED', { claims: renamed });
      for (const [name, content] of [
        ['candidate.json', JSON.stringify({ ...candidate, patch: undefined })],
        ['changes.patch', candidate.patch],
      ]) {
        const bytes = Buffer.from(content);
        total += bytes.length;
        if (bytes.length > 8 * 1024 * 1024 || total > 24 * 1024 * 1024)
          throw error('ARTIFACT_TOO_LARGE');
        if (artifacts.some((a) => a.name === name)) throw error('RESERVED_ARTIFACT_NAME');
        artifacts.push({
          name,
          mime: name.endsWith('.json') ? 'application/json' : 'text/plain',
          digest: digest(bytes),
          base64: bytes.toString('base64'),
        });
      }
    }
    await save('completed', {
      unit,
      requests,
      toolCalls,
      toolResults,
      toolEvents,
      usage,
      turnReason,
      stopped: true,
      stopProof,
      artifacts: artifacts.map(({ base64, ...a }) => a),
    });
    return {
      summary: value.summary,
      artifacts,
      usage,
      composition,
      evidence: { requests, toolCalls, toolResults, toolEvents, turnReason, stopped: true },
    };
  } catch (e) {
    await stop();
    await checkpointHandoff();
    const termination = await inspectUnitExit(unit);
    if (termination) emit('RUNTIME_TERMINATION', {...termination,workspace:params.workspace,
      ...(params.runFence ? {runFence:params.runFence} : {})});
    const originalCode = e.code ?? 'RESULT_FORMAT';
    const failureCode = cancelled || signal.aborted ? originalCode :
      harnessFailureCode(originalCode, termination, unit);
    await save(cancelled || signal.aborted ? 'cancelled' : 'failed', {
      unit,
      code: failureCode,
      ...(failureCode !== originalCode ? {originalCode} : {}),
      reason: e.reason,
      artifactIndex: e.index,
      requests,
      toolCalls,
      toolResults,
      toolEvents,
      exitCode,
      turnReason,
      stopped: true,
      stopProof,
      ...(termination ? {termination} : {}),
      usage,
      ...(progressCheckpoint?{progressCheckpoint}:{}),
    });
    if (finalText) await writeFile(`${dir}/response.txt`, finalText.slice(0, 2000000), { mode: 0o600 });
    // Private diagnostic, never a prompt or API artifact; no credentials were supplied to this process.
    await writeFile(`${dir}/diagnostic.log`, stderr, { mode: 0o600 });
    throw error(failureCode);
  } finally {
    signal.removeEventListener('abort', abort);
    lines.close();
    gateway.closeAllConnections();
    gateway.close();
  }
}
