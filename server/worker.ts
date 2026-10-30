import { PgBoss } from 'pg-boss';
import { randomUUID } from 'node:crypto';
import { config } from './config.js';
import { pool, tx, event, lock } from './db.js';
import {
  schedule,
  startRun,
  heartbeat,
  markUncertain,
  writeArtifact,
  completeRun,
  validateTask,
  failRun,
  confirmStopped,
} from './service.js';
import { assembleContext } from './memory.js';
import {inputFailureMetadata} from './input-failure.js';
import { executeRuntime } from './runtime.js';
import { syncRecallOnce } from './recall.js';
import { recoverStoppedTasks, acceptedDependencyArtifacts } from './recovery.js';
import { PlanningEngine } from './planner.js';
import { RunHeartbeat } from './run-heartbeat.js';
import { readPendingValidations } from './validation-identity.js';
import { reconcileCandidateValidations,reconcileWorkerValidationIntents } from './candidate-validation.js';
import { reconcileCandidateValidationEvidence } from './candidate-validation-evidence.js';
import { reconcilePublishedRebases } from './published-rebase.js';
import { reconcileVerificationSourceBindings } from './verification-source-binding.js';
import {PlanningWake} from './planning-wake.js';
import {installObjectiveReview,ObjectiveReviewEngine} from './objective-review.js';
export class WorkerEngine {
  planner = new PlanningEngine();
  reviews=new ObjectiveReviewEngine();
  wake=new PlanningWake(()=>this.requestWake());
  private wakeRequested=false;
  private reviewSync:Promise<void>|null=null;
  private lastReviewScan=0;
  boss = new PgBoss({ connectionString: config.databaseUrl, schema: 'agentos_queue' });
  workerId = 'worker-' + randomUUID().slice(0, 8);
  timer?: ReturnType<typeof setInterval>;
  busy = false;
  stopped = false;
  active = new Map<string, AbortController>();
  lastTick: string | null = null;
  lastError: string | null = null;
  healthPublishedAt = 0;
  memoryTimer?: ReturnType<typeof setInterval>;
  memorySync: Promise<unknown> | null = null;
  candidateValidationSync: Promise<unknown> | null = null;
    candidateValidationEvidenceSync: Promise<unknown> | null = null;
  publishedRebaseSync: Promise<unknown> | null = null;
  verificationSourceBindingSync: Promise<unknown> | null = null;
  async start() {
    await installObjectiveReview();
    this.boss.on('error', () => {
      this.lastError = 'Falha na fila; confira a conexão PostgreSQL.';
    });
    await this.boss.start();
    await this.boss.createQueue('runs', {
      retryLimit: 3,
      expireInSeconds: 86400,
      heartbeatSeconds: 30,
    });
    await this.boss.updateQueue('runs', { heartbeatSeconds: 30, retryLimit: 3 });
    await this.boss.work<{ runId: string }>(
      'runs',
      { localConcurrency: config.workerSlots, pollingIntervalSeconds: 0.5 },
      async ([job]) => {
        await this.execute(job.data.runId);
      },
    );
    this.memoryTimer = setInterval(() => {
      if (!this.memorySync)
        this.memorySync = syncRecallOnce()
          .catch(() => {})
          .finally(() => {
            this.memorySync = null;
          });
    }, 10000);
    this.timer = setInterval(() => void this.tick(), 1000);
    await this.wake.start();
    await this.tick();
  }
  private requestWake(){this.wakeRequested=true;if(!this.busy)void this.tick();}
  async tick() {
    if (this.busy || this.stopped) return;
    const urgent=this.wakeRequested;this.wakeRequested=false;
    if(!this.reviewSync&&(urgent||Date.now()-this.lastReviewScan>=10000)){
      this.lastReviewScan=Date.now();this.reviewSync=this.reviews.tick().catch(()=>{}).finally(()=>{this.reviewSync=null;});
    }
    if (!this.candidateValidationSync)
      this.candidateValidationSync = reconcileWorkerValidationIntents().then(()=>reconcileCandidateValidations()).catch(() => {}).finally(() => { this.candidateValidationSync = null; });
      if (!this.candidateValidationEvidenceSync)
        this.candidateValidationEvidenceSync = reconcileCandidateValidationEvidence().catch(() => {}).finally(() => { this.candidateValidationEvidenceSync = null; });
    if (!this.publishedRebaseSync)
      this.publishedRebaseSync = reconcilePublishedRebases().catch(() => {}).finally(() => { this.publishedRebaseSync = null; });
    if (!this.verificationSourceBindingSync)
      this.verificationSourceBindingSync = reconcileVerificationSourceBindings().catch(() => {}).finally(() => { this.verificationSourceBindingSync = null; });
    this.busy = true;
    try {
      await markUncertain();
      const submitted = await readPendingValidations();
      for (const row of submitted) {
        try {
          await validateTask(row.id);
        } catch (e: any) {
          if (!['STATE_CHANGED', 'NOT_SUBMITTED'].includes(e?.code)) throw e;
        }
      }
      await recoverStoppedTasks();
      await this.planner.tick();
      await schedule();
      // Publish and mark the outbox in the SAME PostgreSQL transaction.
      await tx(async (db) => {
        await lock(db);
        const rows = (
          await db.query('SELECT * FROM outbox WHERE sent_at IS NULL ORDER BY id FOR UPDATE')
        ).rows;
        for (const row of rows) {
          await this.boss.send(
            'runs',
            { runId: row.run_id },
            {
              db: { executeSql: (text: string, values?: any[]) => db.query(text, values) },
              singletonKey: row.run_id,
              retryLimit: 3,
              heartbeatSeconds: 30,
            },
          );
          await db.query('UPDATE outbox SET sent_at=now() WHERE id=$1', [row.id]);
        }
      });
      this.lastTick = new Date().toISOString();
      this.lastError = null;
    } catch {
      this.lastError = 'Ciclo de coordenação falhou; próxima tentativa automática.';
    } finally {
      // The API process does not run this scheduler. Publish its real completed
      // tick so another process can report health without treating API uptime
      // or worker startup as proof that coordination continues.
      if (Date.now() - this.healthPublishedAt >= 5000) {
        try {
          await pool.query(
            "INSERT INTO settings VALUES('scheduler-health',$1) ON CONFLICT(id) DO UPDATE SET value=excluded.value",
            [JSON.stringify({lastTick:this.lastTick,error:this.lastError,workerId:this.workerId,
              workerSlots:config.workerSlots,active:this.active.size,checkedAt:new Date().toISOString()})],
          );
          this.healthPublishedAt = Date.now();
        } catch { /* An old health timestamp remains visibly stale. */ }
      }
      this.busy = false;
      if(this.wakeRequested&&!this.stopped)setImmediate(()=>void this.tick());
    }
  }
  async execute(id: string) {
    const run = await startRun(id, this.workerId);
    if (!run) return;
    const controller = new AbortController();
    let runtimeStarted = false;
    this.active.set(id, controller);
    const pulse=new RunHeartbeat(()=>heartbeat(id,run.fence),()=>controller.abort());
    const timer=setInterval(()=>void pulse.tick(),3000);
    try {
      const task = (await pool.query('SELECT * FROM tasks WHERE id=$1', [run.task_id])).rows[0],
        account = (await pool.query('SELECT * FROM accounts WHERE id=$1', [run.account_id]))
          .rows[0];
      task.model = run.model ?? task.model;
      const dependencies = await acceptedDependencyArtifacts(task.id);
      const context = await assembleContext(run, task, dependencies);
      if ((await heartbeat(id, run.fence))?.state !== 'running') controller.abort();
      runtimeStarted = true;
      const result = await executeRuntime({
        run,
        task,
        account,
        context,
        signal: controller.signal,
        onEvent: async (kind, payload) => {
          await event(pool, kind, task.id, { runId: id, ...payload });
        },
      });
      if (controller.signal.aborted) throw Error('CANCELLED');
      for (const a of result.artifacts)
        await writeArtifact(id, run.fence, a.name, a.content, a.mime);
      await completeRun(id, run.fence, result.summary, result.usage);
      try {
        await validateTask(task.id);
      } catch (e: any) {
        if (!['STATE_CHANGED', 'NOT_SUBMITTED'].includes(e?.code)) throw e;
      }
    } catch (e: any) {
      const inputFailure=inputFailureMetadata(e);
      if(inputFailure)await event(pool,'RUN_INPUT_FAILURE',run.task_id,{runId:id,...inputFailure});
      if (!runtimeStarted)
        await event(pool, 'HARNESS_NOT_STARTED', run.task_id, {
          runId: id,
          confirmed: true,
          code: e?.code,
        });
      if (e?.code === 'STOP_UNCONFIRMED') {
        await tx(async (db) => {
          await db.query(
            "UPDATE runs SET state='reconciling',error_code='STOP_UNCONFIRMED' WHERE id=$1",
            [id],
          );
          await db.query(
            "UPDATE tasks SET state='reconciling',reason='Encerramento não confirmado; recursos preservados.' WHERE id=$1",
            [run.task_id],
          );
          await event(db, 'RUN_RECONCILING', run.task_id, { runId: id });
        });
      } else if (controller.signal.aborted) {
        await confirmStopped(id, run.fence);
        const r = (await pool.query('SELECT state FROM runs WHERE id=$1', [id])).rows[0];
        if (r?.state === 'running')
          await failRun(
            id,
            run.fence,
            'WORKER_STOPPED',
            'Executor interrompido com término confirmado.',
            true,
          );
      } else
        await failRun(
          id,
          run.fence,
          e?.code ?? 'RUNTIME_ERROR',
          e?.code
            ? e.message
            : 'O executor não produziu uma entrega válida. Artefatos anteriores preservados.',
        );
    } finally {
      clearInterval(timer);
      await pulse.close();
      this.active.delete(id);
      void this.tick();
    }
  }
  async stop() {
    this.stopped = true;
    await this.wake.close();
    await this.reviews.close();
    await this.reviewSync;
    await this.planner.close();
    if (this.timer) clearInterval(this.timer);
    if (this.memoryTimer) clearInterval(this.memoryTimer);
    for (const a of this.active.values()) a.abort();
    await this.boss.stop({ graceful: true, timeout: 10000 });
    await this.memorySync;
    while (this.busy) await new Promise((r) => setTimeout(r, 20));
  }
}
