import {afterAll,beforeAll,beforeEach,expect,it} from 'vitest';
import {randomUUID} from 'node:crypto';
import {pool,migrate} from '../server/db.js';
import {seedRegistry} from '../server/registry.js';
import {installManagement} from '../server/management.js';
import {registerWorkerValidationPolicy,readWorkerValidationIdentity,verifyWorkerValidationRegistration} from '../server/worker-validation.js';

const policy={version:1,enabled:true,project:'agentos',profile:'agentos.validate',stages:['work'],required:true,
  requiredDatabaseTests:[{file:'tests/worker-validation-registration-pg.test.ts',name:'future admission uses the registered policy'}]};
let goal:string;
beforeAll(async()=>{await migrate();await installManagement();});
beforeEach(async()=>{
  await pool.query(`DELETE FROM settings WHERE id LIKE 'worker-validation:%' OR id LIKE 'worker-validation-handoff:%' OR id LIKE 'candidate-validation:%'
    OR id LIKE 'management:%' OR id LIKE 'workforce:%' OR id='autonomy:agentos'`);
  await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');await seedRegistry();goal=randomUUID();
  await pool.query(`INSERT INTO goals(id,title,description,project,state,idempotency_key)
    VALUES($1::uuid,'Future validation','Preconfigure only','agentos','failed',$1::text)`,[goal]);
  await pool.query("INSERT INTO objectives(id,request,request_hash,mode,status) VALUES($1,'{}','fixture','execute','failed')",[goal]);
  await pool.query("INSERT INTO settings(id,value) VALUES('autonomy:agentos','{\"enabled\":true}')");
});
afterAll(async()=>{await pool.end();});
const register=()=>registerWorkerValidationPolicy(goal,policy,{channel:'operator-session'});
const snapshot=async(run:string)=>(await pool.query('SELECT instruction_snapshot FROM runs WHERE id=$1',[run])).rows[0].instruction_snapshot;
async function state(value:string){
  await pool.query('UPDATE goals SET state=$2 WHERE id=$1',[goal,value]);
  await pool.query('UPDATE objectives SET status=$2 WHERE id=$1',[goal,value]);
}
async function admit(runState='running'){
  const task=randomUUID(),run=randomUUID();
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,attempt_count)
    VALUES($1::uuid,$2,$1::text,'Fixture work','Frozen admission','qa.integration','running','work','[]',
      '{"kind":"harness","workspace":"agentos/ofuture"}',1)`,[task,goal]);
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'harness',1,$4)",
    [run,task,randomUUID(),runState]);
  return {run,task};
}

it.each(['planning','replanning','failed','planned'])('registers future policy in %s without activating or dispatching',async(value)=>{
  await state(value);const before=(await pool.query('SELECT g.state,o.status,o.mode,o.revision FROM goals g JOIN objectives o ON o.id=g.id WHERE g.id=$1',[goal])).rows[0];
  const registration=await register();
  expect(registration.policy).toEqual(policy);
  expect((await pool.query('SELECT g.state,o.status,o.mode,o.revision FROM goals g JOIN objectives o ON o.id=g.id WHERE g.id=$1',[goal])).rows[0]).toEqual(before);
  expect((await pool.query('SELECT count(*)::int n FROM tasks WHERE goal_id=$1',[goal])).rows[0].n).toBe(0);
  expect((await pool.query("SELECT kind FROM events WHERE entity_id=$1",[goal])).rows).toEqual([{kind:'WORKER_VALIDATION_POLICY_REGISTERED'}]);
});

it('future admission uses the registered policy while an old snapshot stays immutable',async()=>{
  await state('active');const old=await admit();const original=await snapshot(old.run);expect(original.workerValidation).toBeNull();
  await pool.query("UPDATE runs SET state='cancelled',finished_at=now() WHERE id=$1",[old.run]);
  await pool.query("UPDATE tasks SET state='cancelled' WHERE id=$1",[old.task]);await state('failed');
  // A canonical cancellation can leave the objective's display status executing.
  // The owning goal state, not that stale projection, controls registration.
  await pool.query("UPDATE objectives SET status='executing' WHERE id=$1",[goal]);
  const registration=await register();expect(await snapshot(old.run)).toEqual(original);
  expect((await pool.query('SELECT state FROM goals WHERE id=$1',[goal])).rows[0].state).toBe('failed');
  await state('active');const next=await admit();
  expect((await snapshot(next.run)).workerValidation).toEqual(registration);
  expect(await verifyWorkerValidationRegistration(pool,await readWorkerValidationIdentity(pool,next.run))).toEqual(registration);
  expect(await snapshot(old.run)).toEqual(original);
  await state('failed');
  await expect(verifyWorkerValidationRegistration(pool,await readWorkerValidationIdentity(pool,next.run))).rejects.toThrow('VALIDATION_RUNTIME_UNAUTHORIZED');
});

it.each(['paused','cancelled','completed'])('refuses future registration on %s without an execution exception',async(value)=>{
  await state(value);await expect(register()).rejects.toThrow('VALIDATION_RUNTIME_UNAUTHORIZED');
});
it.each(['queued','running','cancelling','reconciling'])('refuses nonactive registration while a %s run owns work',async(value)=>{
  await admit(value);await expect(register()).rejects.toThrow('VALIDATION_RUNTIME_UNAUTHORIZED');
});
it('refuses residual leases even when a run row is terminal',async()=>{
  const {run}=await admit('failed');await pool.query('INSERT INTO leases(resource,run_id) VALUES($1,$2)',['workspace:agentos/ofuture',run]);
  await expect(register()).rejects.toThrow('VALIDATION_RUNTIME_UNAUTHORIZED');
});
it('refuses an unresolved handoff even before it has reserved a validator job',async()=>{
  const {run}=await admit('submitted');
  await pool.query('INSERT INTO settings(id,value) VALUES($1,$2)',
    ['worker-validation-handoff:'+run,JSON.stringify({runId:run,state:'pending',code:'STOP_UNCONFIRMED'})]);
  await expect(register()).rejects.toThrow('VALIDATION_RUNTIME_UNAUTHORIZED');
});
it('preserves an admitted policy after submission even before the handoff row exists',async()=>{
  await state('active');const original=await register(),{run,task}=await admit();
  await pool.query("UPDATE runs SET state='submitted',finished_at=now() WHERE id=$1",[run]);
  await pool.query("UPDATE tasks SET state='submitted' WHERE id=$1",[task]);await state('failed');
  expect((await pool.query('SELECT 1 FROM settings WHERE id=$1',['worker-validation-handoff:'+run])).rowCount).toBe(0);
  await expect(register()).rejects.toThrow('VALIDATION_RUNTIME_UNAUTHORIZED');
  expect((await pool.query('SELECT value FROM settings WHERE id=$1',['worker-validation:'+goal])).rows[0].value).toEqual(original);
  expect((await snapshot(run)).workerValidation).toEqual(original);
  expect((await pool.query("SELECT count(*)::int n FROM events WHERE kind='WORKER_VALIDATION_POLICY_REGISTERED' AND entity_id=$1",[goal])).rows[0].n).toBe(1);
});
it.each(['proposed','freezing','frozen','running','uncertain'])('preserves a %s validator owner before future policy replacement',async(value)=>{
  await pool.query('INSERT INTO settings(id,value) VALUES($1,$2)',['candidate-validation:'+randomUUID(),JSON.stringify({goalId:goal,state:value})]);
  await expect(register()).rejects.toThrow('VALIDATION_RUNTIME_UNAUTHORIZED');
});
it.each(['management','workforce'])('refuses nonactive registration owned by %s',async(kind)=>{
  await pool.query('INSERT INTO settings(id,value) VALUES($1,\'{"enabled":true}\')',[kind+':'+goal]);
  await expect(register()).rejects.toThrow('VALIDATION_RUNTIME_UNAUTHORIZED');
});
it.each([false,'false'])('refuses explicitly disabled autonomy encoded as %j',async(enabled)=>{
  await pool.query("UPDATE settings SET value=$1 WHERE id='autonomy:agentos'",[JSON.stringify({enabled})]);
  await expect(register()).rejects.toThrow('VALIDATION_RUNTIME_UNAUTHORIZED');
});
it('inherits the existing enabled autonomy default when no project override exists',async()=>{
  await pool.query("DELETE FROM settings WHERE id='autonomy:agentos'");
  expect((await register()).policy).toEqual(policy);
  expect((await pool.query('SELECT state FROM goals WHERE id=$1',[goal])).rows[0].state).toBe('failed');
});
it.each(['plan','missing'])('requires an existing execute objective: %s',async(mode)=>{
  if(mode==='missing')await pool.query('DELETE FROM objectives WHERE id=$1',[goal]);
  else await pool.query("UPDATE objectives SET mode='plan' WHERE id=$1",[goal]);
  await expect(register()).rejects.toThrow('VALIDATION_RUNTIME_UNAUTHORIZED');
});
