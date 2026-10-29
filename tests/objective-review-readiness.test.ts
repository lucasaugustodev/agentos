import {afterAll,afterEach,beforeAll,beforeEach,expect,it} from 'vitest';
import {randomUUID} from 'node:crypto';
import type {PoolClient} from 'pg';
import {pool,migrate,tx,lock} from '../server/db.js';
import {config} from '../server/config.js';
import {workerReleaseIdentity} from '../server/worker-rollout.js';
import {assertObjectiveReviewConsumerReady} from '../server/objective-review-readiness.js';

// Real PostgreSQL ownership in agentos_test; release/process receipts below are
// synthetic dependency-injected evidence, not a production adoption claim.
const expected={candidate:'/opt/agentos/releases/managed-11111111-1111-4111-8111-111111111111',digest:'a'.repeat(64)};
let owner:PoolClient|undefined,ready:any,stopped:any;
const fixtureEvents:string[]=[];
beforeAll(async()=>{
  expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');
  await migrate();
});
beforeEach(async()=>{
  await pool.query("DELETE FROM settings WHERE id='maintenance' OR id LIKE 'worker-readiness:%'");
  owner=await pool.connect();
  await owner.query('SELECT pg_advisory_lock(4280027),pg_advisory_lock(4280050)');
  const backendPid=(await owner.query('SELECT pg_backend_pid() pid')).rows[0].pid;
  ready={service:'agentos-workers',pid:12345,candidate:expected.candidate,digest:expected.digest,
    startupId:randomUUID(),readyAt:new Date(Date.now()-1000).toISOString(),
    locks:[{key:4280027,backendPid},{key:4280050,backendPid}]};
  stopped={unit:'agentos-workers',previousPid:12344,activeState:'inactive',mainPid:0,oldPidGone:true,
    cgroupEmpty:true,checkedAt:new Date(Date.now()-2000).toISOString()};
});
afterEach(async()=>{
  if(owner){await owner.query('SELECT pg_advisory_unlock_all()');owner.release();owner=undefined;}
  await pool.query("DELETE FROM settings WHERE id='maintenance' OR id LIKE 'worker-readiness:%'");
  if(fixtureEvents.length)await pool.query('DELETE FROM events WHERE id=ANY($1::bigint[])',[fixtureEvents.splice(0)]);
});
afterAll(()=>pool.end());
async function setting(id:string,value:any){
  await pool.query('INSERT INTO settings(id,value) VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET value=excluded.value',
    [id,JSON.stringify(value)]);
}
async function adopt(value=ready,stop=stopped){
  await setting('worker-readiness:'+value.service,value);
  const inserted=await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('WORKER_SERVICE_UPDATED',NULL,$1) RETURNING id",
    [JSON.stringify({service:value.service,candidate:value.candidate,pid:value.pid,
      quietCounts:{runs:0,jobs:0,cycles:0,planning:0},quietAt:new Date(Date.now()-3000).toISOString(),
      stopped:stop,readiness:value})]);
  fixtureEvents.push(String(inserted.rows[0].id));
}
function drain(phase='adopted'){
  return {owner:'worker-rollout',draining:true,workerRollout:{...expected,identity:workerReleaseIdentity(expected),
    mode:'per_service',services:{'agentos-workers':{phase,proof:{unit:'agentos-workers',active:true,oldPidGone:true,
      pid:ready.pid,cwd:expected.candidate,digest:expected.digest,checkedAt:new Date().toISOString(),readiness:ready,stopped}}}}};
}
const check=(release=expected)=>tx(async db=>{await lock(db);await assertObjectiveReviewConsumerReady(db,release);});
const unavailable=(promise:Promise<unknown>)=>expect(promise).rejects.toMatchObject({code:'REVIEW_CONSUMER_NOT_READY',status:503});

it('rejects API-only schema availability without an adopted live consumer',async()=>{
  await unavailable(check());
  await setting('worker-readiness:agentos-workers',ready);
  await unavailable(check());
});
it('does not substitute a fresh backlog owner for the actual worker review consumer',async()=>{
  const backlog={...ready,service:'agentos-backlog',locks:[{key:4280066,backendPid:ready.locks[0].backendPid}]};
  await owner!.query('SELECT pg_advisory_lock(4280066)');
  await adopt(backlog,{...stopped,unit:'agentos-backlog'});
  await unavailable(check());
});
it('admits the exact adopted worker release with both live owners, without requiring unrelated services',async()=>{
  await adopt();
  await expect(check()).resolves.toBeUndefined();
  expect((await pool.query("SELECT id FROM settings WHERE id LIKE 'worker-readiness:%'")).rows.map(row=>row.id))
    .toEqual(['worker-readiness:agentos-workers']);
});
it('accepts stable historical adoption while the same startup and owner connections remain live',async()=>{
  ready.readyAt=new Date(Date.now()-86400000).toISOString();
  stopped.checkedAt=new Date(Date.now()-86401000).toISOString();
  await adopt();
  await expect(check()).resolves.toBeUndefined();
});
it.each(['candidate','digest'] as const)('rejects a previous worker %s after the API changes release',async key=>{
  const old={...ready,[key]:key==='candidate'?'/opt/agentos/releases/managed-22222222-2222-4222-8222-222222222222':'b'.repeat(64)};
  await adopt(old);await unavailable(check());
});
it('rejects development or malformed expected release identities',async()=>{
  await adopt();
  await unavailable(check({...expected,candidate:'/tmp/development-source'}));
  await unavailable(check({...expected,digest:'not-a-digest'}));
});
it('rejects changed readiness even if an older adoption receipt remains',async()=>{
  await adopt();await setting('worker-readiness:agentos-workers',{...ready,startupId:randomUUID()});
  await unavailable(check());
});
it('compares readiness structurally after PostgreSQL JSONB reorders object keys',async()=>{
  await adopt();
  const reordered=Object.fromEntries(Object.entries(ready).reverse());
  await setting('worker-readiness:agentos-workers',reordered);
  await expect(check()).resolves.toBeUndefined();
});
it.each(['empty','observer-only','continuation-only','wrong-backend'] as const)('rejects %s ownership proof',async kind=>{
  if(kind==='empty')ready.locks=[];
  if(kind==='observer-only')ready.locks=ready.locks.filter((entry:any)=>entry.key===4280027);
  if(kind==='continuation-only')ready.locks=ready.locks.filter((entry:any)=>entry.key===4280050);
  if(kind==='wrong-backend')ready.locks=ready.locks.map((entry:any)=>({...entry,backendPid:2147483647}));
  await adopt();await unavailable(check());
});
it('rejects an owner lost after a valid adoption without trusting the saved ready flag',async()=>{
  await adopt();await expect(check()).resolves.toBeUndefined();
  await owner!.query('SELECT pg_advisory_unlock(4280027)');
  await unavailable(check());
});
it.each(['null-holder','out-of-range-key','out-of-range-backend'] as const)('rejects malformed %s without leaking a SQL error',async kind=>{
  if(kind==='null-holder')ready.locks=[null];
  if(kind==='out-of-range-key')ready.locks.push({key:2147483648,backendPid:ready.locks[0].backendPid});
  if(kind==='out-of-range-backend')ready.locks[0].backendPid=2147483648;
  await adopt();await unavailable(check());
});
it('maps malformed locks in partial-rollout readiness to the same unavailable contract',async()=>{
  ready.locks=[null];await adopt();await setting('maintenance',drain());await unavailable(check());
});
it.each(['oldPidGone','cgroupEmpty'] as const)('requires the prior worker stop proof: %s',async field=>{
  await adopt(ready,{...stopped,[field]:false});await unavailable(check());
});
it('rejects readiness whose PID is still the prior worker PID',async()=>{
  await adopt(ready,{...stopped,previousPid:ready.pid});await unavailable(check());
});
it('allows an adopted worker lane during a partial rollout while unrelated lanes still drain',async()=>{
  await adopt();await setting('maintenance',drain());
  await expect(check()).resolves.toBeUndefined();
});
it.each(['draining','restarting','awaiting_readiness','failed'] as const)('rejects worker lane phase %s during rollout',async phase=>{
  await adopt();await setting('maintenance',drain(phase));await unavailable(check());
});
it('rejects an unrelated maintenance owner even with previously adopted workers',async()=>{
  await adopt();await setting('maintenance',{draining:true,owner:'other-maintenance'});await unavailable(check());
});
it('rejects a different release or startup in the active rollout proof',async()=>{
  await adopt();const value=drain();value.workerRollout.services['agentos-workers'].proof.readiness={...ready,startupId:randomUUID()};
  await setting('maintenance',value);await unavailable(check());
});
it('does not set readiness or manufacture adoption as a side effect of admission',async()=>{
  await adopt();
  const before=(await pool.query("SELECT value FROM settings WHERE id='worker-readiness:agentos-workers'")).rows[0].value;
  const events=(await pool.query("SELECT count(*)::int n FROM events WHERE kind='WORKER_SERVICE_UPDATED'")).rows[0].n;
  await check();
  expect((await pool.query("SELECT value FROM settings WHERE id='worker-readiness:agentos-workers'")).rows[0].value).toEqual(before);
  expect((await pool.query("SELECT count(*)::int n FROM events WHERE kind='WORKER_SERVICE_UPDATED'")).rows[0].n).toBe(events);
});
