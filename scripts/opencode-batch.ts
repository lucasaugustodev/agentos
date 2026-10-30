import { assertModelRoute } from '../bridge/model-policy.mjs';
// Dedicated native OpenCode worker queue. Shares AgentOS runs, leases, context and artifacts.
// Does not restart the controller or publish jobs to its Harness-only queue.
import { randomUUID } from 'node:crypto';
import { spawn, execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { createInterface } from 'node:readline';
import { writeFile } from 'node:fs/promises';
import { pool, tx, lock, event } from '../server/db.js';
import { config } from '../server/config.js';
import { reconcileRuntimeMemory, releaseRuntimeMemory } from '../server/runtime-memory.js';
import { bridgeScript } from '../server/bridge-path.js';
import { taskDeliveryContract } from '../bridge/task-delivery-contract.mjs';
import {ReconciliationLoop} from '../server/reconciliation-loop.js';
import {RunHeartbeat} from '../server/run-heartbeat.js';
import {reconcileStoppedDetachedHarness} from '../server/harness-supervision.js';
import {reconcileStoppedDetachedOpenCode} from '../server/reconcile-stopped-opencode.js';
import {reconcileOperatorOpenCodeRecoveries} from '../server/operator-opencode-recovery.js';
import {inspectStopped} from '../server/continuation.js';
import { quotaFor, syncAccounts } from '../server/accounts.js';
import { accountBridge } from '../server/bridge.js';
import { verifyOpenCodeDelivery, verifyGenericNativeDelivery } from '../server/opencode-delivery.js';
import { verifyScopedPlanningDelivery,validateScopedPlan } from '../server/scoped-plan-contract.js';
import { assembleContext, hash, saveMemory } from '../server/memory.js';
import { acceptedDependencyArtifacts } from '../server/recovery.js';
import { observeInputFailureBounded } from '../server/input-failure-observation.js';
import { instructionTask } from '../server/task-instructions.js';
import {workerValidationInstruction} from '../server/worker-validation.js';
import { runtimeInputs } from '../server/execution-inputs.js';
import { recordOpenCodeNotStarted } from '../server/not-started.js';
import { recordWorkerReadiness } from '../server/worker-readiness.js';
import {
  installNativeSupervision,
  reserveNativeSupervisor,
  reconcileNativeSupervisor,
  startDetachedNativeRun,
  superviseNativeRun,
  type NativeReservation,
} from '../server/native-supervision.js';
import {
  launchNativeSupervisor,
  observeNativeSupervisor,
  nativeSupervisorSelfProof,
} from './native-supervisor-unit.js';
import {
  heartbeat,
  writeArtifact,
  completeRun,
  validateTask,
  failRun,
  confirmStopped,
  readArtifact,
  schedule,
} from '../server/service.js';

const accountId = 'agentos-api-00000000-0000-4000-8000-000000000001';
const parentGoal = '3ccc273d-e790-43a8-ba86-82c1287907c5';
const batchKey = 'opencode-glm-five-protocol-suites-20261005';
const games = [
  ['wb', 'wild-bandito', 'Wild Bandito', 104],
  ['mw', 'mahjong-ways', 'Mahjong Ways', 65],
  ['mw2', 'mahjong-ways-2', 'Mahjong Ways 2', 74],
  ['ln', 'lucky-neko', 'Lucky Neko', 89],
  ['ta', 'treasures-of-aztec', 'Treasures of Aztec', 87],
] as const;
const model = 'glm-5.3-flash';
const exec = promisify(execFile);
let closing = false;

async function setup() {
  await installNativeSupervision(pool);
}

async function admit() {
  // This historical occurrence is GLM-authorized; never reinterpret it under a new policy.
  assertModelRoute(accountId, model);
  await syncAccounts(true);
  const memory = await saveMemory({
    project: 'pg5-20261005',
    title: 'Cinco frentes nativas OpenCode: testes complementares por jogo',
    kind: 'decision',
    verification: 'confirmed',
    source: 'user:opencode-five-20261005',
    content:
      'Usuário autorizou cinco agentes paralelos no OpenCode com GLM. Uma frente para cada jogo: Wild Bandito, Mahjong Ways, Mahjong Ways 2, Lucky Neko e Treasures of Aztec. Criar testes complementares de protocolo/retomada/idempotência e isolamento, sem editar implementação ativa. Cada sessão usa cópia própria; grava fontes somente em qa-protocol/ e relatos em evidence/. Integração recebe os patches; aceite final dos jogos mantém os critérios anteriores, RTP local 90% e regras originais intactas. Esta entrega de testes não significa jogo aprovado.',
  });
  return tx(async (db) => {
    await lock(db);
    const existing = (await db.query('SELECT id FROM goals WHERE idempotency_key=$1', [batchKey]))
      .rows[0];
    if (existing) return existing;
    const account = (await db.query('SELECT * FROM accounts WHERE id=$1', [accountId])).rows[0];
    if (
      !account?.enabled ||
      !account.models.includes(model) ||
      ['exhausted', 'auth_required'].includes(quotaFor(account, model).status) ||
      (quotaFor(account, model).status === 'unknown' && !account.allow_unknown)
    )
      throw Error('GLM_NOT_AVAILABLE');
    const current = Number(
      (
        await db.query(
          "SELECT count(*) FROM runs WHERE state IN('queued','running','cancelling','reconciling')",
        )
      ).rows[0].count,
    );
    const remote = Number(
      (await db.query("SELECT count(*) FROM leases WHERE resource='remote-slot'")).rows[0].count,
    );
    const used = Number(
      (
        await db.query(
          "SELECT (SELECT count(*) FROM runs WHERE account_id=$1 AND state IN('queued','running','cancelling','reconciling'))+(SELECT count(*) FROM chat_messages WHERE account_id=$1 AND state IN('queued','running','cancelling'))+(SELECT count(*) FROM planning_runs WHERE account_id=$1 AND state='running' AND output IS NULL) count",
          [accountId],
        )
      ).rows[0].count,
    );
    if (current + 5 > 13 || remote + 5 > 13 || used > 0) throw Error('BATCH_CAPACITY_UNAVAILABLE');
    if ((await db.query("SELECT 1 FROM leases WHERE resource='host:agentos-remote'")).rowCount)
      throw Error('LEGACY_EXCLUSIVE_WORKER');
    const goalId = randomUUID();
    await db.query("UPDATE settings SET value=$1 WHERE id='capacity'", [
      JSON.stringify({ maxRuns: 13, browserSlots: 3 }),
    ]);
    await db.query("UPDATE settings SET value=$1 WHERE id='remote-capacity'", [
      JSON.stringify({ maxRuns: 13, memoryMiB: 23400, cpuPercent: 1950 }),
    ]);
    await db.query('UPDATE accounts SET max_concurrency=5 WHERE id=$1', [accountId]);
    await db.query(
      'INSERT INTO goals(id,title,description,project,idempotency_key,input_hash) VALUES($1,$2,$3,$4,$5,$6)',
      [
        goalId,
        'OpenCode GLM — cinco jogos em paralelo',
        'Cinco suítes adicionais de protocolo em OpenCode nativo, com cópias isoladas e integração no piloto existente.',
        'pg5-20261005',
        batchKey,
        hash(batchKey),
      ],
    );
    for (const [prefix, slug, title, gameId] of games) {
      const source = (
        await db.query('SELECT execution FROM tasks WHERE goal_id=$1 AND key=$2', [
          parentGoal,
          'r1-' + prefix + '_a',
        ])
      ).rows[0]?.execution?.sourceWorkspace;
      const integration = (
        await db.query('SELECT id,state FROM tasks WHERE goal_id=$1 AND key=$2', [
          parentGoal,
          'r1-' + prefix + '_integrar',
        ])
      ).rows[0];
      if (
        !source ||
        !integration ||
        !['blocked_dependencies', 'waiting_resource', 'ready', 'blocked_failed'].includes(
          integration.state,
        )
      )
        throw Error('INTEGRATION_ALREADY_ACTIVE');
      const taskId = randomUUID(),
        runId = randomUUID(),
        fence = randomUUID();
      const workspace = 'pg5-20261005/opencode-' + prefix + '-protocol';
      const description = `Implement and execute additional protocol regression tests for ${title}, game id ${gameId}. Your scope is exclusively qa-protocol/ source files and evidence/ reports. Other workers own gameplay, UI and qa-suite/: do not modify their files. Read the real server.mjs, pg-session.mjs/session-factory and history-store to discover APIs. Use built-in Node tests and HTTP on loopback. Cover concurrent duplicate requests if supported, retry with identical round id, two isolated sessions, history/balance conservation, restore/reload after a completed round and during an available feature, deterministic reproducibility with multiple seeds. Treat unsupported behavior honestly; write a failing regression or clear missing-contract report rather than fabricate a passing test. Do not tune RTP, change rules text, fake provider payloads or alter assets. The suite must be usable against the integrated candidate with a documented command and explicit game id. Run it against this snapshot and retain test stdout/stderr and exit code, individual case counts and defects. No browser needed in this task. Write qa-protocol/${slug}.test.mjs (and small helpers under qa-protocol/ only), evidence/protocol-report.json with suiteExecuted:true ONLY if you actually ran it, gameId, command, exitCode, cases, limitations; evidence/protocol-report.md; evidence/protocol-tests.log. Runtime generates canonical changes.patch/candidate.json; do not return these names yourself. Finish with JSON {"summary":"...","artifacts":[{"name":"protocol-report.json","path":"evidence/protocol-report.json","mime":"application/json"},{"name":"protocol-report.md","path":"evidence/protocol-report.md","mime":"text/markdown"},{"name":"protocol-tests.log","path":"evidence/protocol-tests.log","mime":"text/plain"}]}. A reported failing game test does not invalidate a well-implemented test suite; distinguish test delivery from game acceptance. Do not stop with just a plan; create and execute the suite.`;
      const criteria = [
        {
          id: 'executed',
          type: 'json_field',
          name: 'protocol-report.json',
          field: 'suiteExecuted',
          equals: true,
          label: 'Suíte complementar realmente executada',
        },
        {
          id: 'report',
          type: 'artifact',
          name: 'protocol-report.md',
          label: 'Comandos, defeitos e limitações',
        },
        {
          id: 'output',
          type: 'artifact',
          name: 'protocol-tests.log',
          label: 'Saída real dos testes',
        },
        {
          id: 'patch',
          type: 'artifact',
          name: 'changes.patch',
          label: 'Patch canônico da cópia isolada',
        },
      ];
      await db.query(
        `INSERT INTO tasks(id,goal_id,key,title,description,agent_id,account_id,model,state,priority,resources,criteria,required_memories,max_attempts,attempt_count,execution,recovery)
        VALUES($1,$2,$3,$4,$5,'qa.backend',$6,$7,'queued',20,$8,$9,$10,1,1,$11,'{"mode":"manual"}')`,
        [
          taskId,
          goalId,
          prefix + '-protocol',
          title + ' — retomada e integridade do protocolo',
          description +
            '\nExecute the suite using node --test qa-protocol/' +
            slug +
            '.test.mjs. The report command must name this file, exitCode must be an integer, and cases must be a nonempty array. Use data/ or /tmp for temporary server state.',
          accountId,
          model,
          JSON.stringify(['remote-slot', 'workspace:' + workspace]),
          JSON.stringify(criteria),
          JSON.stringify([memory.id]),
          JSON.stringify({
            kind: 'opencode',
            workspace,
            sourceWorkspace: source,
            baseWorkspace: 'pg5-20261005/' + slug,
            profile: 'code',
            writePaths: ['qa-protocol/', 'evidence/'],
            testFile: 'qa-protocol/' + slug + '.test.mjs',
          }),
        ],
      );
      await db.query(
        "INSERT INTO runs(id,task_id,attempt,fence,account_id,model,runtime,agent_version) VALUES($1,$2,1,$3,$4,$5,'opencode',1)",
        [runId, taskId, fence, accountId, model],
      );
      for (const resource of ['remote-slot', 'workspace:' + workspace])
        await db.query('INSERT INTO leases VALUES($1,$2)', [resource, runId]);
      await db.query('INSERT INTO opencode_jobs(run_id) VALUES($1)', [runId]);
      await db.query('INSERT INTO dependencies VALUES($1,$2)', [integration.id, taskId]);
      await db.query('UPDATE tasks SET description=description || $2 WHERE id=$1', [
        integration.id,
        '\nAdditional dependency: merge the accepted OpenCode qa-protocol/ canonical patch and execute its documented command against the integrated candidate. Fix actual defects in your implementation; do not discard failing regressions. Do not confuse delivery of a test suite with approval of the game.',
      ]);
      await event(db, 'RUN_QUEUED', taskId, {
        runId,
        accountId,
        model,
        runtime: 'opencode',
        attempt: 1,
      });
      await event(db, 'OPENCODE_DEPENDENCY_ADDED', integration.id, {
        supportTaskId: taskId,
        supportGoalId: goalId,
      });
    }
    await event(db, 'GOAL_CREATED', goalId, {
      title: 'OpenCode GLM — cinco jogos em paralelo',
      tasks: 5,
      parentGoal,
      runtime: 'opencode',
    });
    return { id: goalId };
  });
}

async function execute(id: string, reservation: NativeReservation) {
  const workerId = 'opencode-native-' + process.pid;
  const run = await tx((db) => startDetachedNativeRun(db, reservation, workerId));
  if (!run) return;
  let child: ReturnType<typeof spawn> | undefined,
    terminal = false,
    cancelRequested = false,
    confirmedStop = true,
    remoteStartAttempted = false;
  let lastSandboxStop: any;
  let pendingEvents = Promise.resolve(),
    observing = false;
  const actions: any[] = [];
  const emit = (kind: string, payload: Record<string, unknown>) => {
    if (kind === 'OPENCODE_STOPPED' && payload.confirmed === true && payload.proof) lastSandboxStop = payload.proof;
    pendingEvents = pendingEvents.then(() =>
      event(pool, kind, run.task_id, { runId: id, ...payload }),
    );
    return pendingEvents;
  };
  const observe = async () => {
    if (observing) return;
    observing = true;
    try {
      const { stdout } = await exec(
        'ssh',
        [
          '-T',
          '-o',
          'BatchMode=yes',
          '-o',
          'ConnectTimeout=8',
          config.bridgeHost,
          '/opt/deepseek-harness/node/bin/node',
          bridgeScript('observe-run.mjs'),
          id,
        ],
        { windowsHide: true, timeout: 20000, maxBuffer: 1024 * 1024 },
      );
      const observation = JSON.parse(stdout);
      if (observation.runId === id && !observation.error)
        await emit('EXECUTION_OBSERVED', {
          observation: {
            ...observation,
            runtime: 'opencode',
            actions: [...actions],
            toolCalls: actions.length,
          },
        });
    } catch {
    } finally {
      observing = false;
    }
  };
  const observationTimer = setInterval(() => void observe(), 15000);
  const pulse=new RunHeartbeat(async()=>
    // Preserve the native worker's existing cancellation semantics. A missing
    // row is checked again at preflight/completion; only cancelling sends stop.
    (await heartbeat(id,run.fence))??{state:'running'},()=>{
        if (!terminal && !cancelRequested) {
          cancelRequested = true;
          const stop = spawn(
            'ssh',
            [
              '-T',
              '-o',
              'BatchMode=yes',
              config.bridgeHost,
              'systemctl',
              'stop',
              'agentos-run-' + id,
            ],
            { windowsHide: true, stdio: 'ignore' },
          );
          stop.on('error', () => {});
        }
  });
  const timer=setInterval(()=>void pulse.tick(),3000);
  try {
    const task = instructionTask(
      run,
      (await pool.query('SELECT * FROM tasks WHERE id=$1', [run.task_id])).rows[0],
    );
    const context = await assembleContext(run, task, await acceptedDependencyArtifacts(task.id));
    instructionTask(run, task, context.manifest);
    const inputs = await runtimeInputs(task, context.manifest, readArtifact);
    const deliveryContract = taskDeliveryContract(task.criteria);
    if ((await heartbeat(id, run.fence))?.state !== 'running')
      throw Object.assign(Error('CANCELLED_BEFORE_START'), {
        code: 'CANCELLED_BEFORE_START',
        stopped: true,
      });
    // Set before any possible launch, even a synchronous spawn failure. No
    // preflight receipt may turn an ambiguous transport into a safe replay.
    await emit('OPENCODE_LAUNCH_ATTEMPTED', { workerId });
    const result: any = await new Promise((resolve, reject) => {
      confirmedStop = false;
      remoteStartAttempted = true;
      child = spawn(
        'ssh',
        [
          '-T',
          '-o',
          'BatchMode=yes',
          '-o',
          'ConnectTimeout=8',
          '-o',
          'ServerAliveInterval=15',
          '-o',
          'ServerAliveCountMax=2',
          config.bridgeHost,
          '/opt/deepseek-harness/node/bin/node',
          bridgeScript('opencode-executor.mjs'),
        ],
        { windowsHide: true, stdio: ['pipe', 'pipe', 'pipe'] },
      );
      child.stdin!.on('error', () => {});
      child.stderr!.resume();
      const lines = createInterface({ input: child.stdout! });
      child.once('error', () =>
        reject(Object.assign(Error('SSH_CONNECTION_FAILED'), { code: 'STOP_UNCONFIRMED' })),
      );
      lines.on('line', (line) => {
        let value: any;
        try {
          value = JSON.parse(line);
        } catch {
          return;
        }
        if (value.type === 'event') {
          void emit(value.event, value.data ?? {}).catch(() => {});
          if (/TOOL/.test(value.event)) {
            actions.push({
              id: id + '-' + value.data.toolEvents,
              tool: value.data.tool,
              command: value.data.detail,
              state: value.data.status === 'completed' ? 'returned' : 'error',
              finishedAt: new Date().toISOString(),
              output: '',
              exitCode: null,
            });
            if (actions.length > 12) actions.shift();
          }
          if (/STARTED/.test(value.event)) void observe();
        } else if (value.type === 'result') {
          terminal = true;
          confirmedStop = value.result?.evidence?.stopped === true;
          resolve(value.result);
        } else if (value.type === 'error') {
          terminal = true;
          confirmedStop = value.error?.stopped === true;
          reject(
            Object.assign(Error(value.error?.code ?? 'OPENCODE_ERROR'), {
              code: value.error?.code ?? 'OPENCODE_ERROR',
              stopped: value.error?.stopped ?? false,
            }),
          );
        }
      });
      child.once('close', () => {
        if (!terminal)
          reject(Object.assign(Error('SSH_LOST_WITHOUT_RECEIPT'), { code: 'STOP_UNCONFIRMED' }));
      });
      child.stdin!.end(
        JSON.stringify({
          runId: id,
          ...task.execution,
          runFence: run.fence,
          goalId: task.goal_id,
          taskId: task.id,
          contextVersion: context.id,
          inputs,
          deliveryContract,
          memoryReservation: reservation.resources,
          managed: true,
          context,
          prompt:
            task.title +
            '\n\n' +
            task.description +
            '\n\n'+workerValidationInstruction(run)+
            (task.execution.revisionFeedback ? '\n\nRECIBOS DA PROPOSTA ANTERIOR (corrija a proposta, não repita o JSON recusado): '+JSON.stringify(task.execution.revisionFeedback) : '') +
            '\n\nINSTRUCTION MANIFEST: ' +
            JSON.stringify((context.manifest as any).taskInstructions),
        }) + '\n',
      );
    });
    if (!result.evidence?.stopped)
      throw Object.assign(Error('STOP_UNCONFIRMED'), { code: 'STOP_UNCONFIRMED' });
    await pendingEvents;
    if ((await heartbeat(id, run.fence))?.state === 'cancelling') {
      await confirmStopped(id, run.fence);
      return;
    }
    const decoded = task.execution.workKind==='planning'?verifyScopedPlanningDelivery(result):
      Object.hasOwn(task.execution,'testFile')?verifyOpenCodeDelivery(result, task.execution.testFile):verifyGenericNativeDelivery(result,id,task.execution);
    if(task.execution.workKind==='planning'){
      for(const artifact of decoded)await writeArtifact(id,run.fence,artifact.name,artifact.content,artifact.mime);
      const request=(await pool.query('SELECT * FROM planning_requests WHERE id=$1 AND task_id=$2',[task.execution.planningRequestId,task.id])).rows[0];
      if(!request)throw Object.assign(Error('PLANNING_REQUEST_INVALID'),{code:'PLANNING_REQUEST_INVALID'});
      try{validateScopedPlan(JSON.parse(decoded.find(a=>a.name==='scope-plan.json')!.content.toString()),request);}
      catch(e:any){throw Object.assign(e,{code:/^[A-Z][A-Z0-9_]{2,80}$/.test(e.message??'')?e.message:'PLANNING_CONTRACT_INVALID'});}
      decoded.splice(0,decoded.length,{name:'runtime-plan-check.json',content:Buffer.from(JSON.stringify({verified:true,requestId:request.id,sourceRevision:request.source_revision})),mime:'application/json'});
    }
    for (const artifact of decoded) {
      await writeArtifact(id, run.fence, artifact.name, artifact.content, artifact.mime);
    }
    await completeRun(id, run.fence, result.summary, result.usage);
    await validateTask(run.task_id).catch((e: any) => {
      if (!['NOT_SUBMITTED', 'STATE_CHANGED'].includes(e.code)) throw e;
    });
  } catch (e: any) {
    try {
      await pendingEvents.catch(() => {});
      if (!remoteStartAttempted && !cancelRequested && e.code !== 'CANCELLED_BEFORE_START')
        await tx(async (db) => {
          await lock(db);
          await recordOpenCodeNotStarted(db, {
            runId: id,
            fence: run.fence,
            workerId,
            remoteStartAttempted,
            cancelRequested,
            errorCode: e.code,
          });
        });
      confirmedStop ||= e.stopped === true;
      if (!confirmedStop) {
        await tx(async (db) => {
          await lock(db);
          await db.query(
            "UPDATE runs SET state='reconciling',error_code='STOP_UNCONFIRMED' WHERE id=$1 AND state IN('running','cancelling')",
            [id],
          );
          await db.query(
            "UPDATE tasks SET state='reconciling',reason='Conferir encerramento do OpenCode antes de retomar.' WHERE id=$1 AND state IN('running','cancelling')",
            [run.task_id],
          );
          await event(db, 'RUN_RECONCILING', run.task_id, { runId: id, code: e.code });
        });
      } else if ((await heartbeat(id, run.fence))?.state === 'cancelling')
        await confirmStopped(id, run.fence);
      else
        await failRun(
          id,
          run.fence,
          e.code ?? 'OPENCODE_ERROR',
          'OpenCode: ' + (e.code ?? 'OPENCODE_ERROR') + '. Evidências preservadas.',
          false,
        );
    } finally {
      await observeInputFailureBounded(run,e);
    }
  } finally {
    // Only a fresh sandbox proof can release resident memory. If publication or
    // shutdown took too long, the dispatcher re-inspects it asynchronously.
    if (lastSandboxStop && reservation.resources) {
      await tx(db=>releaseRuntimeMemory(db,reservation,{runId:id,executionRunId:id,
        workspace:reservation.resources!.workspace,checkedAt:lastSandboxStop.checkedAt,
        process:lastSandboxStop})).catch(()=>{});
    }
    clearInterval(timer);
    await pulse.close();
    clearInterval(observationTimer);
  }
}

async function serve() {
  const owner = await pool.connect();
  const admissions=new ReconciliationLoop(async()=>{
    const started=Date.now();
    await schedule('agentos-opencode');
    await pool.query("INSERT INTO settings VALUES('native-scheduler-engine',$1) ON CONFLICT(id) DO UPDATE SET value=excluded.value",
      [JSON.stringify({state:'healthy',checkedAt:new Date().toISOString(),durationMs:Date.now()-started})]);
  },async(e:any)=>{
    const code=/^[A-Z][A-Z0-9_]{2,80}$/.test(e?.code??'')?e.code:'NATIVE_SCHEDULER_ERROR';
    await pool.query("INSERT INTO settings VALUES('native-scheduler-engine',$1) ON CONFLICT(id) DO UPDATE SET value=excluded.value",
      [JSON.stringify({state:'error',code,checkedAt:new Date().toISOString()})]);
  });
  const memoryReconciliation = new ReconciliationLoop(()=>reconcileRuntimeMemory(),async()=>{});
  let owns = false;
  try {
    owns = (await owner.query('SELECT pg_try_advisory_lock(4280040) owned')).rows[0].owned;
    if (!owns) throw Error('OPENCODE_WORKER_ALREADY_RUNNING');
    owner.on('error', () => {
      closing = true;
    });
    // Legacy callbacks can only be reclaimed after the former queue owner is
    // gone and the run never started. Detached launches require their own proof.
    await pool.query(
      "UPDATE opencode_jobs j SET state='queued',started_at=NULL FROM runs r WHERE r.id=j.run_id AND r.state='queued' AND j.state='running' AND j.supervision IS NULL AND r.started_at IS NULL AND r.worker_id IS NULL",
    );
    const readiness = await recordWorkerReadiness('agentos-opencode', [4280040]);
    if (!readiness) throw Error('NATIVE_DISPATCHER_RELEASE_REQUIRED');
    admissions.start();
    memoryReconciliation.start();
    console.log('OpenCode dispatcher ready');
    const tick = async () => {
      await reconcileOperatorOpenCodeRecoveries();
      const launched = (
        await pool.query(`SELECT supervision FROM opencode_jobs
      WHERE state IN('launching','running','orphaned') AND supervision->>'mode'='detached'`)
      ).rows;
      for (const row of launched) {
        if (closing) break;
        const reservation = row.supervision as NativeReservation;
        try {
          const observation = await observeNativeSupervisor(reservation);
          if (['inactive', 'failed'].includes(observation.activeState)){
            const state=await tx((db) => reconcileNativeSupervisor(db, reservation, observation));
            if(state==='reconciling' && reservation.runtime==='harness')
              await reconcileStoppedDetachedHarness(reservation,observation);
            if(state==='reconciling' && reservation.runtime!=='harness'){
              const inspection=await inspectStopped(reservation.runId,true);
              if(['failed','cancelled'].includes(inspection.state?.state))
                await reconcileStoppedDetachedOpenCode(reservation,observation,inspection);
            }
          }
        } catch {
          // Observation failure is not a stop receipt and never makes replay safe.
        }
      }
      const rows = (
        await pool.query(
          "SELECT j.run_id FROM opencode_jobs j JOIN runs r ON r.id=j.run_id WHERE j.state='queued' AND r.state='queued' ORDER BY j.created_at",
        )
      ).rows;
      for (const row of rows) {
        if (closing) break;
        const reservation = await tx((db) => reserveNativeSupervisor(db, row.run_id, readiness));
        if (!reservation) continue;
        try {
          await launchNativeSupervisor(reservation);
        } catch {
          // systemd-run may have been accepted before its client failed. Inspect
          // the independent unit; never interpret the launch error as no-start.
          try {
            const observation = await observeNativeSupervisor(reservation);
            await tx((db) => reconcileNativeSupervisor(db, reservation, observation));
          } catch {}
        }
      }
    };
    process.on('SIGINT', () => {
      closing = true;
    });
    process.on('SIGTERM', () => {
      closing = true;
    });
    while (!closing) {
      await tick();
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }
  } finally {
    await Promise.all([admissions.close(),memoryReconciliation.close()]);
    // No gateway/SSH actor belongs to this process or cgroup. Existing
    // supervisors keep their own owner lock, lease, immutable context and fence.
    if (owns) await owner.query('SELECT pg_advisory_unlock(4280040)').catch(() => {});
    owner.release();
  }
}

try {
  // The dispatcher owns schema setup. A dozen independent supervisors do not
  // take concurrent ALTER TABLE locks on their shared queue when starting.
  if (process.argv[2] !== 'supervise') await setup();
  if (process.argv[2] === 'admit') {
    const value = await admit();
    await writeFile('.local/opencode-five-goal.json', JSON.stringify(value));
    console.log(JSON.stringify(value));
  } else if (process.argv[2] === 'serve') await serve();
  else if (process.argv[2] === 'supervise') {
    // A separate process has a small private pool; model work stays in its
    // independent bridge sandbox. No dispatcher/admission owner is acquired.
    pool.options.max = 4;
    await superviseNativeRun(process.argv[3], process.argv[4], nativeSupervisorSelfProof, execute);
  } else {
    const rows = (
      await pool.query(
        `SELECT g.id goal_id,t.key,t.state,r.id run_id,r.error_code,r.heartbeat_at,r.runtime,
      (SELECT count(*) FROM events e WHERE e.payload->>'runId'=r.id::text AND e.kind='OPENCODE_TOOL_EVENT') tool_events
      FROM goals g JOIN tasks t ON t.goal_id=g.id JOIN runs r ON r.task_id=t.id WHERE g.idempotency_key=$1 ORDER BY t.key`,
        [batchKey],
      )
    ).rows;
    console.log(JSON.stringify(rows));
  }
} finally {
  accountBridge.close();
  await pool.end();
}
