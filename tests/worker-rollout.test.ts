import { beforeAll, beforeEach, afterEach, afterAll, it, expect, vi } from 'vitest';
import { randomUUID, createHash } from 'node:crypto';
import { mkdtemp, mkdir, writeFile, symlink, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { PgBoss } from 'pg-boss';
import { pool, migrate, tx } from '../server/db.js';
import { seedRegistry } from '../server/registry.js';
import { config } from '../server/config.js';
import { DomainError } from '../server/contracts.js';
import { startRun, completeRun, heartbeat, createGoal, schedule } from '../server/service.js';
import { WorkerEngine } from '../server/worker.js';
import { workerSourceDigest } from '../server/worker-readiness.js';
import { createCycle, runCycle, occurrenceDetail } from '../server/cycles/store.js';
import {
  claimWorkerDrain,
  admitWorkerRestart,
  finishWorkerDrain,
  installWorkerRolloutAdmission,
  reconcileWorkerRollout,
  dispatchAdmittedWorkerRuns,
  workerServices,
  validWorkerServiceProof,
  admitWorkerServiceRestart,
  adoptWorkerService,
  recordWorkerServiceStart,
  type WorkerRelease,
  type WorkerService,
} from '../server/worker-rollout.js';

const readinessOwners: any[] = [];
it('binds readiness to publication bytes including docs and dist while ignoring dependency/test directories, runtime bytecode caches and symlinks', async () => {
  const directory = await mkdtemp(path.join(tmpdir(), 'agentos-readiness-'));
  try {
    for (const name of ['docs', 'dist', 'node_modules', '.local-test', '.git']) {
      await mkdir(path.join(directory, name));
      await writeFile(path.join(directory, name, 'file.txt'), name);
    }
    await symlink(path.join(directory, 'docs/file.txt'), path.join(directory, 'ignored-link'));
    const expected = createHash('sha256')
      .update('dist/file.txt')
      .update('dist')
      .update('docs/file.txt')
      .update('docs')
      .digest('hex');
    expect(await workerSourceDigest(directory)).toBe(expected);
    await writeFile(path.join(directory, 'node_modules/file.txt'), 'mutable');
    expect(await workerSourceDigest(directory)).toBe(expected);
    await mkdir(path.join(directory, 'bridge/__pycache__'), { recursive: true });
    await writeFile(path.join(directory, 'bridge/__pycache__/mod.cpython-312.pyc'), 'bytecode');
    expect(await workerSourceDigest(directory)).toBe(expected);
    await writeFile(path.join(directory, 'bridge/__pycache__/mod.cpython-312.pyc'), 'regenerated');
    expect(await workerSourceDigest(directory)).toBe(expected);
    await writeFile(path.join(directory, 'docs/file.txt'), 'changed documentation');
    const updated = await workerSourceDigest(directory);
    expect(updated).not.toBe(expected);
    await writeFile(path.join(directory, 'dist/file.txt'), 'changed bundle');
    expect(await workerSourceDigest(directory)).not.toBe(updated);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
afterEach(async () => {
  for (const { client, key } of readinessOwners.splice(0)) {
    await client.query('SELECT pg_advisory_unlock($1)', [key]);
    client.release();
  }
});
async function phasedIO(record = release()) {
  const base = fakeIO(record),
    ready: any = {},
    stopped: any = {},
    ownerKeys: any = {
      'agentos-opencode': 4280040,
      'agentos-backlog': 4280066,
      'agentos-cycles': 4280088,
    };
  const owners: any = {};
  for (const [service, key] of Object.entries(ownerKeys)) {
    const client = await pool.connect();
    await client.query('SELECT pg_advisory_lock($1)', [key]);
    owners[service] = {
      key,
      backendPid: (await client.query('SELECT pg_backend_pid() pid')).rows[0].pid,
    };
    readinessOwners.push({ client, key });
  }
  const io = {
    ...base.io,
    restartService: vi.fn(async (service: WorkerService) => {
      stopped[service] = {
        unit: service,
        previousPid: 1,
        activeState: 'inactive',
        mainPid: 0,
        oldPidGone: true,
        cgroupEmpty: true,
        checkedAt: new Date().toISOString(),
      };
      ready[service] = {
        service,
        pid: 101 + workerServices.indexOf(service),
        candidate: record.candidate,
        digest: record.digest,
        startupId: randomUUID(),
        readyAt: new Date().toISOString(),
        locks: owners[service] ? [owners[service]] : [],
      };
      await pool.query(
        'INSERT INTO settings(id,value) VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET value=excluded.value',
        ['worker-readiness:' + service, JSON.stringify(ready[service])],
      );
      return { previousPid: 1, stopped: stopped[service] };
    }),
    serviceProof: vi.fn(async (service: WorkerService) => ({
      unit: service,
      active: true as const,
      pid: ready[service].pid,
      cwd: record.candidate,
      digest: record.digest,
      checkedAt: new Date().toISOString(),
      oldPidGone: true as const,
      readiness: ready[service],
      stopped: stopped[service],
    })),
    scheduleAdmitted: vi.fn(async () => []),
  };
  return { ...base, io, ready, owners };
}

it('adopts idle native/control services while retaining a live Muse worker and partial acknowledgement', async () => {
  const f = await fixture(),
    before = (await pool.query('SELECT * FROM runs WHERE id=$1', [f.run])).rows[0];
  const { io, store } = await phasedIO();
  expect(await reconcileWorkerRollout(io)).toMatchObject({
    status: 'partial',
    adopted: ['agentos-opencode', 'agentos-backlog', 'agentos-cycles'],
    waiting: { 'agentos-workers': { runs: 1 } },
  });
  expect(io.restartService.mock.calls.map(([service]) => service)).toEqual([
    'agentos-opencode',
    'agentos-backlog',
    'agentos-cycles',
  ]);
  expect(store.pending).not.toBeNull();
  expect(store.last).toBeNull();
  expect((await pool.query('SELECT * FROM runs WHERE id=$1', [f.run])).rows[0]).toEqual(before);
  const receipts = (await status()).workerRollout.services;
  expect(receipts['agentos-workers']).toBeUndefined();
  expect(receipts['agentos-opencode'].proof.cwd).toBe(release().candidate);
});

it('does not restart an adopted native service when new native work appears before Muse finishes', async () => {
  await fixture();
  const { io } = await phasedIO();
  await reconcileWorkerRollout(io);
  // Already admitted execution becomes native; it remains live throughout the next tick.
  await pool.query("UPDATE runs SET runtime='opencode'");
  expect(await reconcileWorkerRollout(io)).toMatchObject({ status: 'updated' });
  expect(
    io.restartService.mock.calls.filter(([service]) => service === 'agentos-opencode'),
  ).toHaveLength(1);
  expect((await pool.query('SELECT state FROM runs')).rows[0].state).toBe('running');
});

it('admits independently supervised Harness work through the adopted dispatcher while an old Harness actor drains',async()=>{
  const old=await fixture();
  const f=await fixture('submitted');
  await pool.query("UPDATE tasks SET state='ready',execution=$2 WHERE id=$1",[f.task,
    JSON.stringify({kind:'harness',workspace:'detached/'+f.task,profile:'code'})]);
  await pool.query('INSERT INTO settings VALUES($1,$2)',['workforce:'+f.goal,
    JSON.stringify({enabled:true,detachedHarnessEnabled:true,glmTarget:12})]);
  const {io}=await phasedIO();
  expect(await reconcileWorkerRollout(io)).toMatchObject({status:'partial'});
  const next=randomUUID();
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version) VALUES($1,$2,2,$3,'harness',1)",
    [next,f.task,randomUUID()]);
  await pool.query('INSERT INTO outbox(run_id) VALUES($1)',[next]);
  expect((await pool.query('SELECT state FROM opencode_jobs WHERE run_id=$1',[next])).rows[0]?.state).toBe('queued');
  expect((await pool.query('SELECT 1 FROM outbox WHERE run_id=$1',[next])).rowCount).toBe(0);
  expect((await pool.query('SELECT state FROM runs WHERE id=$1',[old.run])).rows[0].state).toBe('running');
  expect((await status()).workerRollout.services['agentos-workers']).toBeUndefined();
  const owner=readinessOwners.find(item=>item.key===4280040);
  await owner.client.query('SELECT pg_advisory_unlock($1)',[owner.key]);
  await expect(pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version) VALUES($1,$2,3,$3,'harness',1)",
    [randomUUID(),f.task,randomUUID()])).rejects.toThrow('MAINTENANCE_DRAIN');
});

it('keeps the native lane closed while a native job callback is still running, even after its run submits', async () => {
  await pool.query(
    "CREATE TABLE IF NOT EXISTS opencode_jobs(run_id uuid PRIMARY KEY REFERENCES runs(id),state text DEFAULT 'queued',created_at timestamptz DEFAULT now(),started_at timestamptz,finished_at timestamptz)",
  );
  const f = await fixture('submitted');
  await pool.query("INSERT INTO opencode_jobs(run_id,state) VALUES($1,'running')", [f.run]);
  const { io } = await phasedIO();
  expect(await reconcileWorkerRollout(io)).toMatchObject({
    status: 'partial',
    waiting: { 'agentos-opencode': { jobs: 1 } },
  });
  expect(io.restartService.mock.calls.some(([service]) => service === 'agentos-opencode')).toBe(
    false,
  );
  await pool.query("UPDATE opencode_jobs SET state='finished' WHERE run_id=$1", [f.run]);
  expect(await reconcileWorkerRollout(io)).toMatchObject({ status: 'updated' });
});

it('does not record an adopted lane after failed restart, but leaves successful lanes usable', async () => {
  await fixture();
  const { io } = await phasedIO();
  const original = io.restartService.getMockImplementation()!;
  io.restartService.mockImplementation(async (service) => {
    if (service === 'agentos-opencode') throw Error('failed start');
    return original(service);
  });
  expect(await reconcileWorkerRollout(io)).toMatchObject({
    status: 'partial_failed',
    errors: { 'agentos-opencode': 'ROLLOUT_SERVICE_RETRY_REQUIRED' },
    adopted: ['agentos-backlog', 'agentos-cycles'],
  });
  expect((await status()).workerRollout.services['agentos-opencode'].phase).toBe('failed');
});

it('rechecks the same started worker after delayed readiness instead of restarting its initialization',async()=>{
 const {io,ready}=await phasedIO(),original=io.serviceProof.getMockImplementation()!;
 let delayed=true;
 io.serviceProof.mockImplementation(async service=>{
   if(service==='agentos-workers'&&delayed)throw new DomainError('ROLLOUT_SERVICE_PROOF','Startup still initializing');
   return original(service);
 });
 expect(await reconcileWorkerRollout(io)).toMatchObject({status:'partial_failed'});
 const phase=(await status()).workerRollout.services['agentos-workers'];
 expect(phase).toMatchObject({phase:'awaiting_readiness',previousPid:1,stopped:{oldPidGone:true,cgroupEmpty:true}});
 const startupId=ready['agentos-workers'].startupId;
 delayed=false;
 expect(await reconcileWorkerRollout(io)).toMatchObject({status:'updated'});
 expect(io.restartService.mock.calls.filter(([s])=>s==='agentos-workers')).toHaveLength(1);
 expect(ready['agentos-workers'].startupId).toBe(startupId);
});
it('refreshes adoption evidence under the scheduler lock after an expired warmup observation',async()=>{
 const {io}=await phasedIO(),original=io.serviceProof.getMockImplementation()!;
 const calls:any[]=[];
 io.serviceProof.mockImplementation(async(service:any,_record?:any,_phase?:any,options?:{once?:boolean})=>{
   const held=(await pool.query("SELECT count(*)::int n FROM pg_locks WHERE locktype='advisory' AND objid=4280001 AND granted AND database=(SELECT oid FROM pg_database WHERE datname=current_database())")).rows[0].n;
   expect(held).toBe(options?.once?1:0);calls.push({service,once:!!options?.once});
   const proof=await original(service);
   return{...proof,checkedAt:options?.once?new Date().toISOString():'2020-01-01'};
 });
 expect(await reconcileWorkerRollout(io)).toMatchObject({status:'updated'});
 for(const service of workerServices){
   expect(calls.filter(c=>c.service===service).map(c=>c.once)).toEqual([false,true]);
 }
 expect(io.restartService).toHaveBeenCalledTimes(4);
});
it('retains the start receipt when the fresh observation loses release identity instead of trusting warmup',async()=>{
 const {io}=await phasedIO(),original=io.serviceProof.getMockImplementation()!;
 io.serviceProof.mockImplementation(async(service:any,_record?:any,_phase?:any,options?:{once?:boolean})=>{
   const proof=await original(service);
   return service==='agentos-workers'&&options?.once?{...proof,cwd:'/opt/agentos/releases/other'}:proof;
 });
 expect(await reconcileWorkerRollout(io)).toMatchObject({status:'partial_failed',errors:{'agentos-workers':'ROLLOUT_SERVICE_PROOF'}});
 expect((await status()).workerRollout.services['agentos-workers'].phase).toBe('awaiting_readiness');
 expect(io.restartService.mock.calls.filter(([service])=>service==='agentos-workers')).toHaveLength(1);
});
it('retries a confirmed stopped startup through a new quiet and stop boundary',async()=>{
 const {io}=await phasedIO(),original=io.serviceProof.getMockImplementation()!;
 let stopped=true;
 io.serviceProof.mockImplementation(async service=>{
   if(service==='agentos-workers'&&stopped)throw new DomainError('ROLLOUT_SERVICE_START_FAILED','Confirmed stopped startup');
   return original(service);
 });
 expect(await reconcileWorkerRollout(io)).toMatchObject({status:'partial_failed'});
 expect((await status()).workerRollout.services['agentos-workers'].phase).toBe('failed');
 stopped=false;
 expect(await reconcileWorkerRollout(io)).toMatchObject({status:'updated'});
 expect(io.restartService.mock.calls.filter(([s])=>s==='agentos-workers')).toHaveLength(2);
});
it('does not persist a start receipt with a false stop proof or a superseded owner',async()=>{
 const {io}=await phasedIO(),drain=await tx(db=>claimWorkerDrain(db,release(),'per_service'));
 if(drain.status!=='draining')throw Error('Unexpected drain');
 const admitted=await tx(db=>admitWorkerServiceRestart(db,drain.token,'agentos-workers'));
 const started=await io.restartService('agentos-workers');
 await expect(tx(db=>recordWorkerServiceStart(db,drain.token,'agentos-workers',admitted.phase.restartAt,
 {...started,stopped:{...started.stopped,cgroupEmpty:false}}))).rejects.toMatchObject({code:'ROLLOUT_SERVICE_STOP_PROOF'});
 expect(await tx(db=>recordWorkerServiceStart(db,randomUUID(),'agentos-workers',admitted.phase.restartAt,started))).toBe(false);
 expect((await status()).workerRollout.services['agentos-workers'].phase).toBe('restarting');
});

it('rejects a stale startup receipt, unchanged old PID, false stop proof and a spoofed source digest', async () => {
  const { io, ready } = await phasedIO();
  const drain = await tx((db) => claimWorkerDrain(db, release(), 'per_service'));
  if (drain.status !== 'draining') throw Error('unexpected');
  const admission = await tx((db) =>
    admitWorkerServiceRestart(db, drain.token, 'agentos-opencode'),
  );
  await io.restartService('agentos-opencode');
  const proof = await io.serviceProof('agentos-opencode');
  expect(
    validWorkerServiceProof('agentos-opencode', release(), proof, admission.phase.restartAt, 1),
  ).toBe(true);
  for (const forged of [
    { ...proof, pid: 1, readiness: { ...ready['agentos-opencode'], pid: 1 } },
    { ...proof, oldPidGone: false },
    { ...proof, stopped: { ...proof.stopped, cgroupEmpty: false } },
    { ...proof, digest: 'b'.repeat(64) },
    { ...proof, readiness: { ...proof.readiness, readyAt: '2000-01-01T00:00:00Z' } },
    { ...proof, readiness: { ...proof.readiness, locks: [] } },
  ])
    expect(
      validWorkerServiceProof('agentos-opencode', release(), forged, admission.phase.restartAt, 1),
    ).toBe(false);
  await pool.query("UPDATE settings SET value=jsonb_set(value,'{pid}','999') WHERE id=$1", [
    'worker-readiness:agentos-opencode',
  ]);
  await expect(
    tx((db) => adoptWorkerService(db, drain.token, release(), 'agentos-opencode', proof, 1)),
  ).rejects.toMatchObject({ code: 'ROLLOUT_SERVICE_PROOF' });
  expect((await status()).workerRollout.services['agentos-opencode'].phase).toBe('restarting');
});

it('rejects readiness after the owner connection loses its advisory lock', async () => {
  const { io, owners } = await phasedIO();
  const drain = await tx((db) => claimWorkerDrain(db, release(), 'per_service'));
  if (drain.status !== 'draining') throw Error('unexpected');
  await tx((db) => admitWorkerServiceRestart(db, drain.token, 'agentos-opencode'));
  await io.restartService('agentos-opencode');
  const proof = await io.serviceProof('agentos-opencode');
  const held = readinessOwners.find((row) => row.key === owners['agentos-opencode'].key);
  await held.client.query('SELECT pg_advisory_unlock($1)', [held.key]);
  await expect(
    tx((db) => adoptWorkerService(db, drain.token, release(), 'agentos-opencode', proof, 1)),
  ).rejects.toMatchObject({ code: 'ROLLOUT_SERVICE_PROOF' });
});

it('preserves a foreign maintenance owner in phased mode without restarting any unit', async () => {
  const { io } = await phasedIO(),
    foreign = { owner: 'disk-maintenance', draining: true };
  await pool.query("INSERT INTO settings VALUES('maintenance',$1)", [JSON.stringify(foreign)]);
  expect(await reconcileWorkerRollout(io)).toEqual({ status: 'foreign_maintenance' });
  expect(io.restartService).not.toHaveBeenCalled();
  expect(await status()).toEqual(foreign);
});

const release = (id = '1', digest = 'a'): WorkerRelease => ({
  candidate:
    '/opt/agentos/releases/managed-' +
    id.repeat(8) +
    '-' +
    id.repeat(4) +
    '-' +
    id.repeat(4) +
    '-' +
    id.repeat(4) +
    '-' +
    id.repeat(12),
  digest: digest.repeat(64),
  workersPendingRestart: true,
});
beforeAll(async () => {
  await migrate();
  await installWorkerRolloutAdmission();
});
beforeEach(async () => {
  await pool.query("DELETE FROM settings WHERE id='maintenance'");
  await pool.query("DELETE FROM settings WHERE id LIKE 'worker-readiness:%'");
  await pool.query('TRUNCATE goals,agents,accounts,cycles,events CASCADE');
  await seedRegistry();
});
afterEach(async () => {
  await pool.query("DELETE FROM settings WHERE id='maintenance'");
  vi.restoreAllMocks();
});
afterAll(() => pool.end());
async function fixture(state = 'running') {
  const goal = randomUUID(),
    task = randomUUID(),
    run = randomUUID(),
    fence = randomUUID();
  await pool.query(
    "INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Drain fixture','Drain fixture','rollout-test',$2)",
    [goal, goal],
  );
  await pool.query(
    "INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria,attempt_count) VALUES($1,$2,'task','Drain task','Preserved execution','qa.backend',$3,'[]',1)",
    [task, goal, state],
  );
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,heartbeat_at) VALUES($1,$2,1,$3,'harness',1,$4,now())",
    [run, task, fence, state],
  );
  return { goal, task, run, fence };
}
async function cycleFixture(state = 'running') {
  const cycle = await createCycle(
    {
      name: 'Drain cycle',
      project: 'rollout-test',
      trigger: { kind: 'manual' },
      steps: [{ key: 'action', title: 'Inline coordinator', kind: 'wait', seconds: 1 }],
    },
    randomUUID(),
  );
  const occurrence = await runCycle(cycle.id, randomUUID());
  const detail = await occurrenceDetail(occurrence.id);
  const id = randomUUID();
  await pool.query(
    "INSERT INTO cycle_attempts(id,step_id,number,fence,owner,state,account_id,receipt) VALUES($1,$2,1,$3,'live-inline-owner',$4,NULL,$5)",
    [
      id,
      detail.steps[0].id,
      randomUUID(),
      state,
      JSON.stringify({ process: { unitMissing: true }, confirmedStopped: false }),
    ],
  );
  return id;
}
function fakeIO(record = release()) {
  const store: any = { pending: { record, version: 'v1' }, last: null };
  const io = {
    readPending: vi.fn(async () => store.pending),
    readLast: vi.fn(async () => store.last),
    verifyRelease: vi.fn(async (_record: WorkerRelease) => {}),
    dispatchAdmitted: vi.fn(async () => 0),
    restart: vi.fn(async () => {}),
    healthy: vi.fn(async () => {}),
    archive: vi.fn(async (expected: any, completed: any) => {
      if (store.pending?.version !== expected.version)
        throw new DomainError('ROLLOUT_PENDING_CHANGED', 'Changed');
      store.last = completed;
      store.pending = null;
    }),
  };
  return { store, io };
}
const status = async () =>
  (await pool.query("SELECT value FROM settings WHERE id='maintenance'")).rows[0]?.value;

it('starts draining before waiting for live sessions and retains the same owner/token across timer ticks', async () => {
  const f = await fixture(),
    { io } = fakeIO();
  const original = (await pool.query('SELECT * FROM runs WHERE id=$1', [f.run])).rows[0];
  expect(await reconcileWorkerRollout(io)).toMatchObject({
    status: 'draining',
    counts: { runs: 1, cycles: 0, planning: 0 },
  });
  const token = (await status()).workerRollout.token;
  expect(await reconcileWorkerRollout(io)).toMatchObject({ status: 'draining' });
  expect((await status()).workerRollout.token).toBe(token);
  expect((await pool.query('SELECT * FROM runs WHERE id=$1', [f.run])).rows[0]).toEqual(original);
  expect(io.restart).not.toHaveBeenCalled();
  expect(
    (
      await pool.query(
        "SELECT count(*)::int n FROM events WHERE kind='WORKER_ROLLOUT_DRAIN_STARTED'",
      )
    ).rows[0].n,
  ).toBe(1);
});

it.each(['queued', 'running', 'cancelling', 'reconciling'])(
  'does not restart with a globally %s run',
  async (state) => {
    await fixture(state);
    const { io } = fakeIO();
    expect(await reconcileWorkerRollout(io)).toMatchObject({
      status: 'draining',
      counts: { runs: 1 },
    });
    expect(io.restart).not.toHaveBeenCalled();
  },
);
it.each(['queued', 'running', 'cancelling', 'reconciling'])(
  'does not infer an inline coordinator stopped from missing unit metadata (%s)',
  async (state) => {
    const id = await cycleFixture(state),
      { io } = fakeIO();
    const before = (await pool.query('SELECT * FROM cycle_attempts WHERE id=$1', [id])).rows[0];
    expect(await reconcileWorkerRollout(io)).toMatchObject({
      status: 'draining',
      counts: { cycles: 1 },
    });
    expect((await pool.query('SELECT * FROM cycle_attempts WHERE id=$1', [id])).rows[0]).toEqual(
      before,
    );
    expect(io.restart).not.toHaveBeenCalled();
  },
);

it('blocks new attempts while preserving heartbeats, terminal receipts and execution of already queued runs', async () => {
  const f = await fixture('queued'),
    { io } = fakeIO();
  await reconcileWorkerRollout(io);
  await expect(
    pool.query(
      "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version) VALUES($1,$2,2,$3,'harness',1)",
      [randomUUID(), f.task, randomUUID()],
    ),
  ).rejects.toThrow('MAINTENANCE_DRAIN');
  const run = await startRun(f.run, 'original-worker');
  expect(run.fence).toBe(f.fence);
  expect((await heartbeat(f.run, f.fence))?.state).toBe('running');
  await completeRun(f.run, f.fence, 'Preserved natural completion', {});
  expect(await reconcileWorkerRollout(io)).toMatchObject({ status: 'updated' });
  expect(io.restart).toHaveBeenCalledOnce();
  expect(await status()).toBeUndefined();
});
it('guards new cycle attempts without changing queued cycle steps or completed receipts', async () => {
  const id = await cycleFixture('running'),
    { io } = fakeIO();
  await reconcileWorkerRollout(io);
  const a = (await pool.query('SELECT * FROM cycle_attempts WHERE id=$1', [id])).rows[0];
  await expect(
    pool.query(
      "INSERT INTO cycle_attempts(id,step_id,number,fence,owner) VALUES($1,$2,2,$3,'new-owner')",
      [randomUUID(), a.step_id, randomUUID()],
    ),
  ).rejects.toThrow('MAINTENANCE_DRAIN');
  await pool.query(
    "UPDATE cycle_attempts SET state='completed',finished_at=now(),receipt=$2 WHERE id=$1",
    [id, JSON.stringify({ completedNaturally: true })],
  );
  expect(await reconcileWorkerRollout(io)).toMatchObject({ status: 'updated' });
});
it.each([true, false])(
  'preserves foreign maintenance ownership when draining=%s',
  async (draining) => {
    const foreign = {
      owner: 'server-migration',
      draining,
      reason: 'Independent work',
      metadata: { keep: 7 },
    };
    await pool.query("INSERT INTO settings VALUES('maintenance',$1)", [JSON.stringify(foreign)]);
    const { io } = fakeIO();
    expect(await reconcileWorkerRollout(io)).toEqual({ status: 'foreign_maintenance' });
    expect(await status()).toEqual(foreign);
    expect(io.restart).not.toHaveBeenCalled();
  },
);
it('restores inactive maintenance metadata exactly after a successful owned update', async () => {
  const previous = {
    draining: false,
    reason: 'Preserved previous intent',
    metadata: { keep: 'value' },
  };
  await pool.query("INSERT INTO settings VALUES('maintenance',$1)", [JSON.stringify(previous)]);
  const { io } = fakeIO();
  expect(await reconcileWorkerRollout(io)).toMatchObject({ status: 'updated' });
  expect(await status()).toEqual(previous);
});
it('retains pending update and admission gates after restart failure, then retries while globally quiet', async () => {
  const { io, store } = fakeIO();
  io.restart.mockRejectedValueOnce(Error('service failed'));
  expect(await reconcileWorkerRollout(io)).toMatchObject({ status: 'failed' });
  expect((await status()).workerRollout.phase).toBe('failed');
  expect(store.pending).not.toBeNull();
  expect(await reconcileWorkerRollout(io)).toMatchObject({ status: 'updated' });
  expect(io.restart).toHaveBeenCalledTimes(2);
  expect(store.pending).toBeNull();
  expect(await status()).toBeUndefined();
});
it('does not release a foreign owner that takes over before restart admission', async () => {
  const foreign = { owner: 'operator-maintenance', draining: true, reason: 'Keep' };
  const { io } = fakeIO();
  io.verifyRelease
    .mockImplementationOnce(async () => {})
    .mockImplementationOnce(async () => {
      await pool.query("UPDATE settings SET value=$1 WHERE id='maintenance'", [
        JSON.stringify(foreign),
      ]);
    });
  expect(await reconcileWorkerRollout(io)).toMatchObject({ status: 'ownership_changed' });
  expect(io.restart).not.toHaveBeenCalled();
  expect(await status()).toEqual(foreign);
});
it('does not erase a concurrent pending revision and carries the original restoration snapshot to its retry', async () => {
  const { io, store } = fakeIO();
  io.restart.mockImplementationOnce(async () => {
    store.pending = { record: release('2', 'b'), version: 'v2' };
  });
  expect(await reconcileWorkerRollout(io)).toMatchObject({
    status: 'failed',
    code: 'ROLLOUT_PENDING_CHANGED',
  });
  expect(store.pending.version).toBe('v2');
  expect(await reconcileWorkerRollout(io)).toMatchObject({
    status: 'updated',
    candidate: release('2', 'b').candidate,
  });
  expect(await status()).toBeUndefined();
});
it('recovers a crash after durable acknowledgement without restarting workers twice', async () => {
  const { io, store } = fakeIO();
  io.archive.mockImplementationOnce(async (_expected, completed) => {
    store.last = completed;
    store.pending = null;
    throw Error('interrupted after archive');
  });
  expect(await reconcileWorkerRollout(io)).toMatchObject({ status: 'failed' });
  expect(await reconcileWorkerRollout(io)).toMatchObject({ status: 'updated' });
  expect(io.restart).toHaveBeenCalledOnce();
  expect(await status()).toBeUndefined();
});
it('fails closed for missing pending/acknowledgement data and a changed release digest', async () => {
  const f = await fixture(),
    { io, store } = fakeIO();
  await reconcileWorkerRollout(io);
  store.pending = null;
  expect(await reconcileWorkerRollout(io)).toEqual({ status: 'missing_pending_record' });
  expect((await status()).draining).toBe(true);
  expect(io.restart).not.toHaveBeenCalled();
  store.pending = { record: release(), version: 'v1' };
  io.verifyRelease.mockRejectedValueOnce(new DomainError('ROLLOUT_RELEASE_CHANGED', 'Changed'));
  await expect(reconcileWorkerRollout(io)).rejects.toMatchObject({
    code: 'ROLLOUT_RELEASE_CHANGED',
  });
  expect(io.restart).not.toHaveBeenCalled();
  expect((await pool.query('SELECT state FROM runs WHERE id=$1', [f.run])).rows[0].state).toBe(
    'running',
  );
});
it('serializes competing drain owners and fences cleanup from an older rollout token', async () => {
  const [a, b] = await Promise.all([
    tx((db) => claimWorkerDrain(db, release())),
    tx((db) => claimWorkerDrain(db, release())),
  ]);
  if (a.status !== 'draining' || b.status !== 'draining')
    throw Error('Unexpected foreign maintenance');
  expect(a.token).toBe(b.token);
  const c = await tx((db) => claimWorkerDrain(db, release('2', 'b')));
  if (c.status !== 'draining') throw Error('Unexpected foreign maintenance');
  expect(c.token).not.toBe(a.token);
  expect(await tx((db) => finishWorkerDrain(db, a.token!, release()))).toBe(false);
  expect((await status()).workerRollout.token).toBe(c.token);
});
it('serializes raw attempt admission against a committed drain transaction', async () => {
  const f = await fixture('submitted');
  let releaseDrain!: () => void, drainReady!: () => void;
  const gate = new Promise<void>((resolve) => {
    releaseDrain = resolve;
  });
  const ready = new Promise<void>((resolve) => {
    drainReady = resolve;
  });
  const drain = tx(async (db) => {
    const result = await claimWorkerDrain(db, release());
    drainReady();
    await gate;
    return result;
  });
  await ready;
  const admission = pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version) VALUES($1,$2,2,$3,'harness',1)",
    [randomUUID(), f.task, randomUUID()],
  );
  const rejected = expect(admission).rejects.toThrow('MAINTENANCE_DRAIN');
  releaseDrain();
  await drain;
  await rejected;
  expect(
    (await pool.query('SELECT count(*)::int n FROM runs WHERE task_id=$1', [f.task])).rows[0].n,
  ).toBe(1);
});
it('publishes only the pre-drain queued outbox atomically and idempotently without creating another attempt', async () => {
  const f = await fixture('queued');
  await pool.query('INSERT INTO outbox(run_id) VALUES($1)', [f.run]);
  const boss = new PgBoss({ connectionString: config.databaseUrl, schema: 'agentos_queue' });
  boss.on('error', () => {});
  await boss.start();
  await boss.createQueue('runs', { retryLimit: 3, heartbeatSeconds: 30 });
  try {
    await tx((db) => claimWorkerDrain(db, release()));
    const results = await Promise.all([dispatchAdmittedWorkerRuns(), dispatchAdmittedWorkerRuns()]);
    expect(results.reduce((n, value) => n + value, 0)).toBe(1);
    expect(
      (await pool.query('SELECT sent_at FROM outbox WHERE run_id=$1', [f.run])).rows[0].sent_at,
    ).not.toBeNull();
    expect(
      (await pool.query('SELECT count(*)::int n FROM runs WHERE task_id=$1', [f.task])).rows[0].n,
    ).toBe(1);
    const token = (await status()).workerRollout.token;
    expect((await tx((db) => admitWorkerRestart(db, token))).ready).toBe(false);
  } finally {
    await boss.stop({ graceful: true });
  }
});
it('includes an active planner in the quiet proof and blocks its next claim while preserving its output', async () => {
  const f = await fixture('submitted'),
    planner = randomUUID();
  await pool.query(
    "INSERT INTO objectives(id,request,request_hash,mode) VALUES($1,'{}','fixture','execute')",
    [f.goal],
  );
  await pool.query(
    "INSERT INTO planning_runs(id,objective_id,base_revision,trigger_key,fence,state,output) VALUES($1,$2,0,'planner',$3,'running',$4)",
    [planner, f.goal, randomUUID(), JSON.stringify({ preserved: true })],
  );
  const { io } = fakeIO();
  expect(await reconcileWorkerRollout(io)).toMatchObject({
    status: 'draining',
    counts: { planning: 1 },
  });
  await pool.query("UPDATE planning_runs SET state='completed' WHERE id=$1", [planner]);
  await expect(
    pool.query("UPDATE planning_runs SET state='running' WHERE id=$1", [planner]),
  ).rejects.toThrow('MAINTENANCE_DRAIN');
  expect(
    (await pool.query('SELECT output FROM planning_runs WHERE id=$1', [planner])).rows[0].output,
  ).toEqual({ preserved: true });
  expect(await reconcileWorkerRollout(io)).toMatchObject({ status: 'updated' });
});
it('allows queued planning requests but blocks their execution claim during drain', async () => {
  const f = await fixture('running'),
    planner = randomUUID();
  await pool.query(
    "INSERT INTO objectives(id,request,request_hash,mode) VALUES($1,'{}','fixture','execute')",
    [f.goal],
  );
  const { io } = fakeIO();
  await reconcileWorkerRollout(io);
  await pool.query(
    "INSERT INTO planning_runs(id,objective_id,base_revision,trigger_key,fence,state) VALUES($1,$2,0,'queued',$3,'queued')",
    [planner, f.goal, randomUUID()],
  );
  await expect(
    pool.query("UPDATE planning_runs SET state='running' WHERE id=$1", [planner]),
  ).rejects.toThrow('MAINTENANCE_DRAIN');
  await expect(
    pool.query(
      "INSERT INTO planning_runs(id,objective_id,base_revision,trigger_key,fence,state) VALUES($1,$2,0,'running',$3,'running')",
      [randomUUID(), f.goal, randomUUID()],
    ),
  ).rejects.toThrow('MAINTENANCE_DRAIN');
  expect(
    (await pool.query('SELECT state FROM planning_runs WHERE id=$1', [planner])).rows[0].state,
  ).toBe('queued');
});
it('serializes publication swap and pending write with the rollout lock in an isolated filesystem', async () => {
  const { stdout } = await promisify(execFile)(
    'python3',
    ['tests/fixtures/worker-rollout-publication.py'],
    { timeout: 15000 },
  );
  expect(JSON.parse(stdout)).toEqual({
    invalidOriginRejected: true,
    originChecked: true,
    lockedBeforeSwap: true,
    lockedThroughPending: true,
    publishedAfterRelease: true,
  });
});
it('skips new admissions during drain while the real worker publishes and completes its already queued run', async () => {
  await pool.query(
    "UPDATE settings SET value='{" + '"maxRuns":1,"browserSlots":2' + "}' WHERE id='capacity'",
  );
  await pool.query("UPDATE accounts SET max_concurrency=2 WHERE id='local-validation'");
  const created = await createGoal(
    {
      title: 'Real drain',
      project: 'rollout-test',
      idempotencyKey: randomUUID(),
      tasks: ['first', 'next'].map((key) => ({
        key,
        title: key,
        agentId: 'system.validation',
        accountId: 'local-validation',
        criteria: [
          {
            id: 'verified',
            type: 'json_field',
            label: 'Verified',
            name: 'result.json',
            field: 'validated',
            equals: true,
          },
        ],
      })),
    },
    true,
  );
  const [run] = await schedule();
  expect(run).toBeDefined();
  await pool.query(
    "UPDATE settings SET value='{" + '"maxRuns":2,"browserSlots":2' + "}' WHERE id='capacity'",
  );
  await tx((db) => claimWorkerDrain(db, release()));
  const engine = new WorkerEngine();
  try {
    await engine.start();
    expect(engine.lastError).toBeNull();
    expect(
      (await pool.query('SELECT sent_at FROM outbox WHERE run_id=$1', [run])).rows[0].sent_at,
    ).not.toBeNull();
    expect(await dispatchAdmittedWorkerRuns()).toBe(0);
    await expect
      .poll(
        async () =>
          (
            await pool.query("SELECT state FROM tasks WHERE goal_id=$1 AND key='first'", [
              created.id,
            ])
          ).rows[0].state,
        { timeout: 7000, interval: 100 },
      )
      .toBe('accepted');
    expect(
      (
        await pool.query(
          'SELECT count(*)::int n FROM runs WHERE task_id IN(SELECT id FROM tasks WHERE goal_id=$1)',
          [created.id],
        )
      ).rows[0].n,
    ).toBe(1);
    expect(
      (
        await pool.query("SELECT attempt_count FROM tasks WHERE goal_id=$1 AND key='next'", [
          created.id,
        ])
      ).rows[0].attempt_count,
    ).toBe(0);
    expect((await status()).draining).toBe(true);
    expect(engine.active.size).toBe(0);
  } finally {
    await engine.stop();
  }
});
it('installs admission DDL without a scheduler/writer lock inversion', async () => {
  const f = await fixture('submitted');
  await pool.query('DROP TRIGGER agentos_worker_rollout_admission ON planning_runs');
  let releaseScheduler!: () => void, schedulerReady!: () => void;
  const gate = new Promise<void>((resolve) => {
    releaseScheduler = resolve;
  });
  const ready = new Promise<void>((resolve) => {
    schedulerReady = resolve;
  });
  const scheduler = tx(async (db) => {
    await claimWorkerDrain(db, release());
    schedulerReady();
    await gate;
  });
  await ready;
  const writer = await pool.connect();
  const writerPid = (await writer.query('SELECT pg_backend_pid() pid')).rows[0].pid;
  const admission = writer.query(
    "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version) VALUES($1,$2,2,$3,'harness',1)",
    [randomUUID(), f.task, randomUUID()],
  );
  const rejected = expect(admission).rejects.toThrow('MAINTENANCE_DRAIN');
  try {
    await expect
      .poll(
        async () =>
          (
            await pool.query('SELECT count(*)::int n FROM pg_locks WHERE pid=$1 AND NOT granted', [
              writerPid,
            ])
          ).rows[0].n,
        { timeout: 2000, interval: 20 },
      )
      .toBeGreaterThan(0);
    const installing = installWorkerRolloutAdmission();
    await expect
      .poll(
        async () =>
          (
            await pool.query(
              "SELECT count(*)::int n FROM pg_stat_activity a JOIN pg_locks l ON l.pid=a.pid WHERE a.query LIKE 'CREATE OR REPLACE FUNCTION agentos_worker_rollout_admission%' AND NOT l.granted",
            )
          ).rows[0].n,
        { timeout: 2000, interval: 20 },
      )
      .toBeGreaterThan(0);
    releaseScheduler();
    await scheduler;
    await rejected;
    await installing;
    expect(
      (await pool.query('SELECT count(*)::int n FROM runs WHERE task_id=$1', [f.task])).rows[0].n,
    ).toBe(1);
  } finally {
    releaseScheduler();
    await scheduler;
    writer.release();
  }
});
