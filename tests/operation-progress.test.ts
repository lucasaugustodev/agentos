import { beforeAll, afterAll, it, expect,vi } from 'vitest';
import { createHash, randomUUID } from 'node:crypto';
import { pool, migrate, tx, lock } from '../server/db.js';
import { installManagement, recordManagementReviews } from '../server/management.js';
import { installBacklogPlanner } from '../server/backlog.js';
import {installSupervision} from '../server/supervision.js';
import { writeArtifact, completeRun, validateTask } from '../server/service.js';
import {
  runtimeCandidateDigest,
  observeOperationProgress,
  projectOperationProgress,
  readOperationProgress,
  collectOperationProgress,
  recordOperationProgress,
  type OperationProgress,
} from '../server/operation-progress.js';

const hash = (value: string | Buffer) => createHash('sha256').update(value).digest('hex');
const a = 'a'.repeat(64),
  b = 'b'.repeat(64);
const integrated = (id = 'integration', scope = 'documentation/release') => ({
  id,
  stage: 'integrate',
  state: 'accepted',
  attempt_count: 1,
  last_run_attempt: 1,
  last_run_id: 'run-' + id,
  last_run_state: 'submitted',
  acceptance_passed: true,
  dependency_ids: [] as string[],
  execution: { baseWorkspace: scope },
  created_at: '2026-10-01T12:00:00Z',
  accepted_at: '2026-10-06T15:00:00Z',
});
const pending = () => ({ ...integrated(), state: 'recovery_blocked', acceptance_passed: false });
function manifest(files = [{ path: 'server.ts', sha256: a }]) {
  const encoded = JSON.stringify(
    [...files]
      .sort((x, y) => Buffer.compare(Buffer.from(x.path), Buffer.from(y.path)))
      .map((f) => [f.path, f.sha256]),
  ).replace(/[\u0080-\uffff]/g, (c) => '\\u' + c.charCodeAt(0).toString(16).padStart(4, '0'));
  const candidateDigest = hash(encoded);
  const bytes = Buffer.from(JSON.stringify({ baseDigest: b, candidateDigest, files }));
  return { bytes, digest: hash(bytes), candidateDigest };
}
function stagnant(live = false) {
  const task = { ...pending(), has_live_run: live };
  const observation = observeOperationProgress([task], {});
  let progress: OperationProgress | null = null;
  for (let i = 0; i < 4; i++)
    progress = projectOperationProgress(observation, progress, 'review-' + i);
  return progress!;
}

it('uses an intact reserved runtime manifest and recomputes the candidate identity', () => {
  const m = manifest();
  expect(runtimeCandidateDigest(m.bytes, m.digest)).toBe(m.candidateDigest);
  expect(runtimeCandidateDigest(m.bytes, a)).toBeNull();
  const tampered = Buffer.from(
    JSON.stringify({ ...JSON.parse(m.bytes.toString()), candidateDigest: a }),
  );
  expect(runtimeCandidateDigest(tampered, hash(tampered))).toBeNull();
  const duplicate = Buffer.from(
    JSON.stringify({
      ...JSON.parse(m.bytes.toString()),
      files: [
        { path: 'server.ts', sha256: a },
        { path: 'server.ts', sha256: a },
      ],
    }),
  );
  expect(runtimeCandidateDigest(duplicate, hash(duplicate))).toBeNull();
  expect(runtimeCandidateDigest(Buffer.from('{"candidateDigest":"' + a + '"}'), a)).toBeNull();
});

it('matches the Python manifest encoding for non-ASCII source paths', () => {
  const m = manifest([
    { path: 'ação/😀.ts', sha256: a },
    { path: 'z.ts', sha256: b },
  ]);
  expect(runtimeCandidateDigest(m.bytes, m.digest)).toBe(m.candidateDigest);
});

it('counts integration and QA only from current real acceptances on the same candidate', () => {
  const tasks = [integrated(), { ...integrated('qa'), stage: 'verify',dependency_ids:['integration'] }];
  let p = observeOperationProgress(tasks, { integration: a, qa: a });
  expect(p.delivery).toEqual({ integrated: 1, verified: 1, total: 1 });
  expect(projectOperationProgress(p, null).nextTransition.kind).toBe('complete');
  p = observeOperationProgress(tasks, { integration: a, qa: b });
  expect(p.delivery.verified).toBe(0);
  expect(p.milestones[0].verificationMatchesCandidate).toBe(false);
  expect(projectOperationProgress(p, null).nextTransition.kind).toBe('verify');
  expect(observeOperationProgress(tasks, {}).delivery.integrated).toBe(0);
  expect(
    observeOperationProgress([{ ...integrated(), last_run_attempt: 0 }], { integration: a })
      .delivery.integrated,
  ).toBe(0);
  expect(
    observeOperationProgress([{ ...integrated(), acceptance_passed: false }], { integration: a })
      .delivery.integrated,
  ).toBe(0);
});

it('keeps twenty accepted diagnostics separate from the final delivery gate', () => {
  const diagnostics = Array.from({ length: 20 }, (_, i) => ({
    ...integrated('diagnostic-' + i),
    stage: 'work',
    execution: { baseWorkspace: 'documentation/release', workKind: 'diagnostic' },
  }));
  const before = observeOperationProgress([pending()], {});
  const after = observeOperationProgress([pending(), ...diagnostics], {});
  expect(after.fingerprint).toBe(before.fingerprint);
  expect(after.delivery).toEqual({ integrated: 0, verified: 0, total: 1 });
  expect(after.milestones[0].diagnostics).toEqual({ accepted: 20, total: 20 });
});

it('does not treat attempts, new case IDs, task labels or report timestamps as delivery', () => {
  const before = observeOperationProgress([pending()], {}, [
    { run_id: 'run-integration', digest: a },
  ]);
  const after = observeOperationProgress(
    [
      {
        ...pending(),
        attempt_count: 99,
        last_run_attempt: 99,
        last_run_id: 'new-run',
        title: 'Another attempt',
        updated_at: '2026-10-06',
        execution: {
          baseWorkspace: 'documentation/release',
          supervision: { caseId: randomUUID() },
        },
      },
    ],
    {},
    [{ run_id: 'new-run', digest: b }],
  );
  expect(after.fingerprint).toBe(before.fingerprint);
});

it('requires three distinct unchanged reviews; reads and duplicate recording do not increment', () => {
  const observation = observeOperationProgress([pending()], {});
  let p = projectOperationProgress(observation, null, 'review-0');
  for (let i = 0; i < 10; i++) p = projectOperationProgress(observation, p);
  expect(p.stagnation.unchangedReviews).toBe(0);
  p = projectOperationProgress(observation, p, 'review-0');
  expect(p.stagnation.unchangedReviews).toBe(0);
  for (let i = 1; i <= 3; i++) p = projectOperationProgress(observation, p, 'review-' + i);
  expect(p.stagnation.detected).toBe(true);
  expect(p.nextTransition.kind).toBe('replan');
});

it('requires new input content, not a renamed ID, and removal/reintroduction is not novelty', () => {
  const p = stagnant();
  const input = observeOperationProgress([pending()], {}, [], [a, a]);
  let next = projectOperationProgress(input, p, 'review-5');
  expect(next.stagnation.unchangedReviews).toBe(0);
  next = projectOperationProgress(
    observeOperationProgress([pending()], {}, [], []),
    next,
    'review-6',
  );
  expect(next.stagnation.unchangedReviews).toBe(1);
  next = projectOperationProgress(input, next, 'review-7');
  expect(next.stagnation.unchangedReviews).toBe(2);
  expect(observeOperationProgress([pending()], {}, [], [a, b, a]).inputFingerprint).toBe(
    observeOperationProgress([pending()], {}, [], [b, a]).inputFingerprint,
  );
});

it('clears delivery stagnation only on an accepted candidate milestone, preserving proof digests', () => {
  const before = stagnant();
  const observation = observeOperationProgress(
    [integrated(), { ...integrated('qa'), stage: 'verify', state: 'ready',dependency_ids:['integration'] }],
    { integration: a },
    [{ run_id: 'run-integration', digest: b }],
  );
  const next = projectOperationProgress(observation, before, 'review-5');
  expect(next.stagnation.unchangedReviews).toBe(0);
  expect(next.nextTransition.kind).toBe('verify');
  expect(next.milestones[0].integration.artifactDigests).toEqual([b]);
});

it('preserves live execution while distinguishing activity from delivery', () => {
  const p = stagnant(true);
  expect(p.liveRuns).toBe(1);
  expect(p.stagnation.detected).toBe(true);
  expect(p.nextTransition.kind).toBe('wait_live');
  expect(p.delivery.integrated).toBe(0);
});

it('does not let a formerly accepted integration hide a newer planned candidate', () => {
  const newer = {
    ...pending(),
    id: 'new-integration',
    created_at: new Date('2026-10-05T12:00:00Z'),
  };
  const observation = observeOperationProgress([integrated(), newer], { integration: a });
  expect(observation.delivery.integrated).toBe(0);
  expect(observation.milestones[0].integration.taskId).toBe(newer.id);
});

it('does not reset stagnation when a gate is withdrawn or recreated on the already accepted candidate', () => {
  const delivered = observeOperationProgress([integrated()], { integration: a });
  let p = projectOperationProgress(delivered, null, 'review-0');
  for (let i = 1; i <= 3; i++) p = projectOperationProgress(delivered, p, 'review-' + i);
  expect(p.stagnation.detected).toBe(true);
  p = projectOperationProgress(observeOperationProgress([pending()], {}), p, 'review-4');
  expect(p.stagnation.unchangedReviews).toBe(4);
  p = projectOperationProgress(delivered, p, 'review-5');
  expect(p.stagnation.unchangedReviews).toBe(5);
  expect(p.stagnation.detected).toBe(true);
});

it('requests a delivery plan when the objective has no integration or verification milestones', () => {
  let p: OperationProgress | null = null;
  for (let i = 0; i < 4; i++)
    p = projectOperationProgress(observeOperationProgress([], {}), p, 'review-' + i);
  expect(p!.stagnation.detected).toBe(true);
  expect(p!.nextTransition.kind).toBe('replan');
});

function targetedValidationFixture(finalAccepted=false){
  const producer={...integrated('producer'),stage:'work',plan_revision:4};
  const intermediate={...integrated('z-intermediate'),stage:'verify',plan_revision:4,
    dependency_ids:[producer.id],accepted_at:'2026-10-06T18:00:00Z'};
  const integration={...integrated('m-integration'),plan_revision:4,dependency_ids:[producer.id]};
  const final={...integrated('a-final'),stage:'verify',plan_revision:4,dependency_ids:[integration.id,intermediate.id],
    ...(finalAccepted?{}:{state:'blocked_dependencies',acceptance_passed:false})};
  return {producer,intermediate,integration,final,tasks:[producer,intermediate,integration,final]};
}

it('does not promote a same-digest intermediate check over the pending final QA by UUID',()=>{
  const f=targetedValidationFixture(),digests={[f.integration.id]:a,[f.intermediate.id]:a};
  const observation=observeOperationProgress(f.tasks,digests);
  expect(observation.milestones[0].verification).toMatchObject({taskId:f.final.id,state:'pending'});
  expect(observation.delivery).toEqual({integrated:1,verified:0,total:1});
  expect(projectOperationProgress(observation,null).nextTransition.kind).toBe('verify');
  // The later intermediate acceptance is not the time of a final delivery.
  expect(observation.lastDeliveryAt).toBe(new Date(f.integration.accepted_at).toISOString());
});

it.each([a,b])('selects the accepted final QA by ancestry regardless of intermediate digest %s',auxiliaryDigest=>{
  const f=targetedValidationFixture(true),digests={[f.integration.id]:a,[f.intermediate.id]:auxiliaryDigest,[f.final.id]:a};
  for(const tasks of [f.tasks,[...f.tasks].reverse()]){
    const observation=observeOperationProgress(tasks,digests);
    expect(observation.milestones[0].verification.taskId).toBe(f.final.id);
    expect(observation.delivery.verified).toBe(1);
    expect(projectOperationProgress(observation,null).nextTransition.kind).toBe('complete');
  }
});

it('uses the final integration rather than a same-transaction join with a higher UUID',()=>{
  const f=targetedValidationFixture(),join={...integrated('zz-join'),plan_revision:4,dependency_ids:[f.producer.id]},
    consumer={...integrated('consumer'),stage:'work',plan_revision:4,dependency_ids:[join.id]};
  f.integration.dependency_ids.push(consumer.id);
  const observation=observeOperationProgress([...f.tasks,join,consumer],{[join.id]:b,[f.integration.id]:a});
  expect(observation.milestones[0].integration).toMatchObject({taskId:f.integration.id,candidateDigest:a});
});

it('does not hide ambiguous terminal validations or invalid dependencies behind passing manifests',()=>{
  const f=targetedValidationFixture(true),other={...f.final,id:'zz-other-final'},
    digests={[f.integration.id]:a,[f.intermediate.id]:a,[f.final.id]:a,[other.id]:a};
  const ambiguous=observeOperationProgress([...f.tasks,other],digests);
  expect(ambiguous.milestones[0].verification).toMatchObject({state:'pending',selectionIssue:'ambiguous_dag'});
  expect(ambiguous.delivery.verified).toBe(0);
  expect(projectOperationProgress(ambiguous,null).nextTransition.kind).toBe('replan');
  f.producer.dependency_ids=[f.intermediate.id];
  const cyclic=observeOperationProgress(f.tasks,digests);
  expect(cyclic.delivery.verified).toBe(0);
  expect(cyclic.milestones[0].verification.selectionIssue).toBe('invalid_dag');
  f.producer.dependency_ids=['missing-parent'];
  expect(observeOperationProgress(f.tasks,digests).milestones[0].verification.selectionIssue).toBe('invalid_dag');
  const live=observeOperationProgress([...f.tasks,{...other,has_live_run:true}],digests);
  expect(projectOperationProgress(live,null).nextTransition.kind).toBe('wait_live');
});

it('uses the current plan revision before task timestamps and refuses an unversioned UUID tie',()=>{
  const f=targetedValidationFixture(),historical={...integrated('old-accepted'),plan_revision:3,created_at:'2026-10-20T00:00:00Z'};
  const observation=observeOperationProgress([...f.tasks,historical],{[historical.id]:b,[f.integration.id]:a});
  expect(observation.milestones[0].integration.taskId).toBe(f.integration.id);
  const tied=observeOperationProgress([integrated('a-candidate'),integrated('z-candidate')],{'a-candidate':a,'z-candidate':a});
  expect(tied.milestones[0].integration.selectionIssue).toBe('ambiguous_dag');
  expect(tied.delivery.integrated).toBe(0);
});

beforeAll(async () => {
  await migrate();
  await installBacklogPlanner();
  await installSupervision();
  await installManagement();
});
afterAll(async () => {
  await pool.end();
});
const unavailable = async () => {
  throw Error('No runtime artifact supplied');
};
async function databaseFixture() {
  const goal = randomUUID(),
    task = randomUUID(),
    agent = 'progress-' + randomUUID();
  await pool.query(
    "INSERT INTO agents(id,name,department,capabilities,runtime,version,description) VALUES($1,'Fixture','test','[]','test',1,'Own test fixture')",
    [agent],
  );
  await pool.query(
    "INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Documentation release','Generic delivery','progress-test',$2)",
    [goal, goal],
  );
  await pool.query(
    "INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria,stage,execution) VALUES($1,$2,'integrate','Integrate docs','Fixture',$3,'ready','[]','integrate',$4)",
    [task, goal, agent, JSON.stringify({ baseWorkspace: 'docs/' + goal })],
  );
  return { goal, task, agent };
}

it('projects persisted dependency edges and revision without promoting an intermediate acceptance',async()=>{
  const f=await databaseFixture(),producer=randomUUID(),intermediate='f'+randomUUID().slice(1),final='0'+randomUUID().slice(1),
    created='2026-10-06T12:00:00Z',scope='docs/'+f.goal,candidate=manifest(),artifacts=new Map<string,Buffer>();
  await pool.query('UPDATE tasks SET plan_revision=4,created_at=$2 WHERE id=$1',[f.task,created]);
  for(const [id,stage] of [[producer,'work'],[intermediate,'verify'],[final,'verify']]){
    await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria,stage,execution,plan_revision,created_at)
      VALUES($1,$2,$1::text,'Synthetic gate','Projection fixture',$3,'ready','[]',$4,$5,4,$6)`,
      [id,f.goal,f.agent,stage,JSON.stringify({baseWorkspace:scope}),created]);
  }
  for(const [consumer,parent] of [[intermediate,producer],[f.task,producer],[final,f.task],[final,intermediate]])
    await pool.query('INSERT INTO dependencies VALUES($1,$2)',[consumer,parent]);
  const accept=async(taskId:string)=>{
    const runId=randomUUID(),artifactId=randomUUID();
    await pool.query("INSERT INTO runs(id,task_id,attempt,state,fence,runtime,agent_version) VALUES($1,$2,1,'submitted',$3,'test',1)",[runId,taskId,randomUUID()]);
    await pool.query("UPDATE tasks SET state='accepted',attempt_count=1 WHERE id=$1",[taskId]);
    await pool.query("INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) VALUES($1,$2,$3,true,'[]','synthetic-projection-test')",[randomUUID(),taskId,runId]);
    await pool.query("INSERT INTO artifacts(id,run_id,name,mime,digest,size) VALUES($1,$2,'candidate.json','application/json',$3,$4)",[artifactId,runId,candidate.digest,candidate.bytes.length]);
    artifacts.set(artifactId,candidate.bytes);
  };
  const read=async(id:string)=>({bytes:artifacts.get(id)!});
  await accept(producer);await accept(f.task);await accept(intermediate);
  const pending=await collectOperationProgress(pool,f.goal,read);
  expect(pending.milestones[0].verification).toMatchObject({taskId:final,state:'pending'});
  expect(pending.delivery).toEqual({integrated:1,verified:0,total:1});
  await accept(final);
  const complete=await collectOperationProgress(pool,f.goal,read);
  expect(complete.milestones[0].verification.taskId).toBe(final);
  expect(complete.delivery.verified).toBe(1);
  await pool.query('DELETE FROM dependencies WHERE task_id=$1 AND depends_on=$2',[final,intermediate]);
  const ambiguous=await collectOperationProgress(pool,f.goal,read);
  expect(ambiguous.milestones[0].verification.selectionIssue).toBe('ambiguous_dag');
  expect(ambiguous.delivery.verified).toBe(0);
  expect((await pool.query('SELECT state,criteria FROM tasks WHERE goal_id=$1',[f.goal])).rows.every(row=>
    row.state==='accepted'&&row.criteria.length===0)).toBe(true);
});

it('reads progress and input identity without transferring large embedded files or changing stored evidence',async()=>{
 const f=await databaseFixture(),bytes=Buffer.from('Relevant file contents\n'.repeat(50000));
 const execution={baseWorkspace:'docs/'+f.goal,inputs:[{id:randomUUID(),name:'relevant.txt',digest:hash(bytes),base64:bytes.toString('base64')}]};
 await pool.query('UPDATE tasks SET execution=$2 WHERE id=$1',[f.task,JSON.stringify(execution)]);
 const originalQuery=pool.query.bind(pool);let transferred=0;
 const spy=vi.spyOn(pool,'query').mockImplementation((async(...args:any[])=>{
  const result=await (originalQuery as any)(...args);
  if(result.rows?.some((row:any)=>Object.hasOwn(row,'last_run_state')))transferred=Buffer.byteLength(JSON.stringify(result.rows));
  return result;
 }) as any);
 try{
  const progress=await readOperationProgress(pool,f.goal,unavailable);
  expect(progress.inputDigests).toEqual([hash(bytes)]);
  expect(progress.delivery).toEqual({integrated:0,verified:0,total:1});
  expect(transferred).toBeGreaterThan(0);
  expect(transferred).toBeLessThan(4096);
 }finally{spy.mockRestore()}
 expect((await pool.query('SELECT execution FROM tasks WHERE id=$1',[f.task])).rows[0].execution).toEqual(execution);
});

it('excludes closed input bundles from novelty while preserving legacy evidence and delivery gates', async () => {
 const f=await databaseFixture(),current=hash('current input');
 await pool.query('UPDATE tasks SET execution=$2 WHERE id=$1',[
  f.task,JSON.stringify({baseWorkspace:'docs/'+f.goal,inputs:[{digest:current}]})]);
 const history=[] as Array<{id:string;execution:unknown}>;
 for(const state of ['accepted','cancelled','superseded']){
  // Closed historical payloads are no longer planner input. Even a legacy
  // malformed bundle must not stall the live queue or fabricate new input.
  const id=randomUUID(),execution={baseWorkspace:'docs/'+f.goal,workKind:'diagnostic',
   inputs:{legacyBundle:Buffer.from('Closed evidence\n'.repeat(100000)).toString('base64'),digest:hash(state)}};
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria,stage,execution)
   VALUES($1,$2,$3,'Closed evidence','Preserve historical payload',$4,$3,'[]','work',$5)`,[
   id,f.goal,state,f.agent,JSON.stringify(execution)]);
  history.push({id,execution});
 }
 const p=await readOperationProgress(pool,f.goal,unavailable);
 expect(p.inputDigests).toEqual([current]);
 expect(p.delivery).toEqual({integrated:0,verified:0,total:1});
 expect(p.milestones[0].integration.state).toBe('pending');
 for(const h of history)
  expect((await pool.query('SELECT execution FROM tasks WHERE id=$1',[h.id])).rows[0].execution).toEqual(h.execution);
});

it('persists counters once per recorded review under concurrent PostgreSQL admission', async () => {
  const f = await databaseFixture(),
    review = randomUUID();
  const record = (runId: string) =>
    tx(async (db) => {
      await lock(db);
      return recordOperationProgress(db, f.goal, runId, unavailable);
    });
  await Promise.all([record(review), record(review)]);
  for (let i = 0; i < 3; i++) await record(randomUUID());
  const before = (
    await pool.query('SELECT value FROM settings WHERE id=$1', ['operation-progress:' + f.goal])
  ).rows[0].value;
  expect(before.stagnation.unchangedReviews).toBe(3);
  for (let i = 0; i < 3; i++)
    expect(
      (await readOperationProgress(pool, f.goal, unavailable)).stagnation.unchangedReviews,
    ).toBe(3);
  const after = (
    await pool.query('SELECT value FROM settings WHERE id=$1', ['operation-progress:' + f.goal])
  ).rows[0].value;
  expect(after).toEqual(before);
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1', [f.task])).rows[0].state).toBe(
    'ready',
  );
});

it('records prepared observations without analytics under admission and rejects stale replacement', async () => {
  const f=await databaseFixture(),review=randomUUID();
  const prepare=()=>tx(async db=>{
    await db.query('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY');
    const observedAt=new Date((await db.query('SELECT transaction_timestamp() observed_at')).rows[0].observed_at).toISOString();
    return {observedAt,observation:await collectOperationProgress(db,f.goal,unavailable)};
  });
  const initial=await prepare();
  const record=(id:string,prepared:typeof initial)=>tx(async db=>{
    await lock(db);
    const onlySettings={query:async(sql:string,args?:unknown[])=>{
      // Slow task/run/artifact/memory reads belong to the prior read-only snapshot.
      expect(sql).toMatch(/^(SELECT value FROM settings|INSERT INTO settings)/);
      return db.query(sql,args);
    }};
    return recordOperationProgress(onlySettings as any,f.goal,id,unavailable,prepared);
  });
  await Promise.all([record(review,initial),record(review,initial)]);
  let progress;
  for(let i=0;i<3;i++)progress=await record(randomUUID(),initial);
  expect(progress!.stagnation.unchangedReviews).toBe(3);
  expect(progress!.observedAt).toBe(initial.observedAt);
  expect(progress!.delivery).toEqual({integrated:0,verified:0,total:1});

  const digest=hash('new verified source for the next attempt');
  await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{inputs}',$2::jsonb) WHERE id=$1",[
    f.task,JSON.stringify([{digest}])]);
  const newer=await prepare();
  // Explicit timestamps also exercise the late-arrival fence independent of clock granularity.
  newer.observedAt=new Date(Date.parse(initial.observedAt)+1000).toISOString();
  const advanced=await record(randomUUID(),newer);
  expect(advanced.stagnation.unchangedReviews).toBe(0);
  expect(advanced.inputDigests).toEqual([digest]);
  const late=await record(randomUUID(),initial);
  expect(late.observedAt).toBe(newer.observedAt);
  expect(late.inputFingerprint).toBe(advanced.inputFingerprint);
  expect(late.inputDigests).toEqual([digest]);
  expect(late.delivery).toEqual(advanced.delivery);
  expect(late.stagnation.unchangedReviews).toBe(1);
  await expect(record(randomUUID(),{...initial,observedAt:'invalid'})).rejects.toThrow('OPERATION_OBSERVATION_TIME');
  expect((await pool.query('SELECT value FROM settings WHERE id=$1',['operation-progress:'+f.goal])).rows[0].value).toEqual(late);
  expect((await pool.query('SELECT state,attempt_count FROM tasks WHERE id=$1',[f.task])).rows[0]).toMatchObject({state:'ready',attempt_count:0});
});

it('hashes relevant memory content rather than IDs or timestamps in the PostgreSQL snapshot', async () => {
  const f = await databaseFixture();
  const addMemory = async (content: string) => {
    const id = randomUUID();
    await pool.query(
      "INSERT INTO memories(id,project,title,content,kind,verification,source) VALUES($1,'progress-test','Input',$2,'fact','verified','test')",
      [id, content],
    );
    await pool.query('UPDATE tasks SET required_memories=$2 WHERE id=$1', [
      f.task,
      JSON.stringify([id]),
    ]);
  };
  await addMemory('Verified source metadata');
  const first = await readOperationProgress(pool, f.goal, unavailable);
  await addMemory('Verified source metadata');
  expect((await readOperationProgress(pool, f.goal, unavailable)).inputFingerprint).toBe(
    first.inputFingerprint,
  );
  await addMemory('A new verified source preimage');
  expect((await readOperationProgress(pool, f.goal, unavailable)).inputFingerprint).not.toBe(
    first.inputFingerprint,
  );
});

it('keeps a live PostgreSQL run untouched at the stagnation threshold', async () => {
  const f = await databaseFixture(),
    run = randomUUID(),
    fence = randomUUID();
  await pool.query("UPDATE tasks SET state='running',attempt_count=1 WHERE id=$1", [f.task]);
  await pool.query(
    "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'test',1,'running')",
    [run, f.task, fence],
  );
  for (let i = 0; i < 4; i++)
    await tx(async (db) => {
      await lock(db);
      await recordOperationProgress(db, f.goal, randomUUID(), unavailable);
    });
  const p = await readOperationProgress(pool, f.goal, unavailable);
  expect(p.stagnation.detected).toBe(true);
  expect(p.nextTransition.kind).toBe('wait_live');
  const row = (await pool.query('SELECT state,fence FROM runs WHERE id=$1', [run])).rows[0];
  expect(row).toEqual({ state: 'running', fence });
});

it('records an explicit replan receipt after distinct real coordinator reviews and preserves the proposed next transition', async () => {
  const f = await databaseFixture(),
    coordinator = randomUUID();
  await pool.query('INSERT INTO settings VALUES($1,$2)', [
    'management:' + f.goal,
    JSON.stringify({ enabled: true, enabledAt: '2026-01-01T00:00:00Z' }),
  ]);
  await pool.query(
    "INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria,stage) VALUES($1,$2,'coordinate','Review delivery','MANAGEMENT_V1',$3,'ready',$4,'coordinate')",
    [
      coordinator,
      f.goal,
      f.agent,
      JSON.stringify([
        { id: 'plan', label: 'Review', type: 'artifact', name: 'coordination.json' },
      ]),
    ],
  );
  const review = async (attempt: number, nextTransition?: any) => {
    const run = randomUUID(),
      fence = randomUUID();
    await pool.query("UPDATE tasks SET state='running',attempt_count=$2 WHERE id=$1", [
      coordinator,
      attempt,
    ]);
    await pool.query(
      "INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,$3,$4,'test',1,'running')",
      [run, coordinator, attempt, fence],
    );
    await writeArtifact(
      run,
      fence,
      'coordination.json',
      JSON.stringify({
        management: {
          summary: 'The integration gate has not advanced.',
          capacityReason: 'A discriminating repair input is still required.',
          actions: [],
          nextTransition,
        },
      }),
      'application/json',
    );
    await completeRun(run, fence, 'Review completed', {});
    await validateTask(coordinator);
    await recordManagementReviews();
    return run;
  };
  let run = '';
  for (let i = 1; i <= 4; i++) run = await review(i);
  let row = (await pool.query('SELECT receipts FROM management_reviews WHERE run_id=$1', [run]))
    .rows[0];
  expect(row.receipts).toContainEqual(
    expect.objectContaining({ code: 'OPERATION_REPLAN_REQUIRED' }),
  );
  expect(row.receipts).toContainEqual(expect.objectContaining({code:'DELIVERY_STAGNATION'}));
  const frontierBefore=(await pool.query('SELECT value FROM settings WHERE id=$1',['work-frontier:'+f.goal])).rows[0].value;
  await recordManagementReviews();
  expect((await pool.query('SELECT value FROM settings WHERE id=$1',['work-frontier:'+f.goal])).rows[0].value.unchangedReviews)
    .toBe(frontierBefore.unchangedReviews);
  const nextTransition = {
    cause: 'The required source producer is not yet linked.',
    hypothesis: 'Integrating the accepted patch supplies that producer.',
    expectedArtifact: 'candidate.json plus the actual integration gate report',
    taskId: f.task,
  };
  run = await review(5, nextTransition);
  row = (await pool.query('SELECT receipts FROM management_reviews WHERE run_id=$1', [run]))
    .rows[0];
  expect(row.receipts).toContainEqual(
    expect.objectContaining({ code: 'OPERATION_REPLAN_REQUIRED' }),
  );
  expect(row.receipts).toContainEqual(expect.objectContaining({code:'MANAGEMENT_TRANSITION_REJECTED'}));
  const state = (
    await pool.query('SELECT value FROM settings WHERE id=$1', ['management-state:' + f.goal])
  ).rows[0].value;
  expect(state.nextTransition).toEqual(nextTransition);
  await recordManagementReviews();
  expect((await readOperationProgress(pool, f.goal, unavailable)).stagnation.unchangedReviews).toBe(
    4,
  );
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1', [f.task])).rows[0].state).toBe(
    'ready',
  );
  await pool.query('INSERT INTO settings VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET value=excluded.value',
    ['supervision:'+f.goal,JSON.stringify({enabled:true})]);
  await pool.query('UPDATE tasks SET criteria=$2 WHERE id=$1',[f.task,
    JSON.stringify([{id:'delivery',label:'Delivery evidence',type:'artifact',name:'integration.json'}])]);
  const corrected={...nextTransition,expectedAttempt:0,expectedArtifact:'integration.json'};
  run=await review(6,corrected);
  const applied=(await pool.query('SELECT receipts FROM management_reviews WHERE run_id=$1',[run])).rows[0].receipts;
  expect(applied).toContainEqual(expect.objectContaining({code:'MANAGEMENT_DISPATCH_OWNED',satisfied:true,taskId:f.task}));
  expect(applied).not.toContainEqual(expect.objectContaining({code:'OPERATION_REPLAN_REQUIRED'}));
});
