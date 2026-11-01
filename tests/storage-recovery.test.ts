import {beforeAll,beforeEach,afterAll,it,expect,vi} from 'vitest';
import {randomUUID} from 'node:crypto';
import {pool,tx,lock,event} from '../server/db.js';
import {seedRegistry} from '../server/registry.js';
import {prepareSchema} from '../server/schema-readiness.js';
import {prepareBacklogSchema} from '../server/backlog-schema-readiness.js';
import {reconcileStorageRecovery,validStorageObservation,recordStorageFailure} from '../server/storage-recovery.js';
import {recordOpenCodeNotStarted,hasStoragePreflightReceipt} from '../server/not-started.js';
import {prepareOpenCodeWorkspace} from '../bridge/opencode-preflight.mjs';
import {stoppedRunProof} from '../bridge/stopped-run-proof.mjs';
// Keep bounded NOWAIT conflict recovery and readiness receipts in the
// canonical preparers; direct nested DDL can collide with a transient reader.
beforeAll(async()=>{await prepareSchema();await prepareBacklogSchema();});
beforeEach(async()=>{await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');await pool.query("DELETE FROM settings WHERE id='storage:agentos-workspaces' OR id='autonomy:storage-test'");await seedRegistry();});
afterAll(async()=>{await pool.query("DELETE FROM settings WHERE id='storage:agentos-workspaces' OR id='autonomy:storage-test'");await pool.end();});
const available=()=>({version:1,resource:'agentos-workspaces',freeBytes:8*1024**3,reservedBytes:0,availableBytes:8*1024**3,headroomBytes:2*1024**3,available:true,checkedAt:new Date().toISOString()});
async function fixture(){
 const goal=randomUUID(),task=randomUUID(),run=randomUUID(),fence=randomUUID();
 await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1::uuid,'Deliver','Preserve criteria','storage-test',$1::text)",[goal]);
 await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,criteria,state,execution,attempt_count,max_attempts)
  VALUES($1,$2,'work','Work','Do the original work','engineering.backend.node',$3,'failed',$4,3,3)`,
  [task,goal,JSON.stringify([{id:'passed',label:'Real acceptance',type:'json_field',name:'verification.json',field:'passed',equals:true}]),JSON.stringify({kind:'harness',workspace:'storage-test/o-work',sourceWorkspace:'storage-test/source'})]);
 await pool.query("INSERT INTO runs(id,task_id,attempt,state,fence,runtime,agent_version,error_code) VALUES($1,$2,3,'failed',$3,'harness',1,'DISK_CAPACITY')",[run,task,fence]);
 await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('HARNESS_NOT_STARTED',$1,$2)",[task,JSON.stringify({runId:run,confirmed:true,code:'DISK_CAPACITY'})]);
 const proof={runId:run,workspace:'storage-test/o-work',checkedAt:new Date().toISOString(),candidate:{candidateDigest:'a'.repeat(64)},process:{unit:'agentos-run-'+run,stopped:true,mainPid:0,populated:false,activeState:'inactive'}};
 const inspect=vi.fn().mockImplementation(async()=>({...proof,checkedAt:new Date().toISOString()}));
 const request=vi.fn().mockImplementation(async(method:string)=>method==='worker.storage'?available():{workspace:'storage-test/o-work',sourceWorkspace:'storage-test/source'});
 return {goal,task,run,fence,proof,inspect,request};
}
async function state(id:string){return (await pool.query('SELECT state,attempt_count,max_attempts,criteria,execution FROM tasks WHERE id=$1',[id])).rows[0];}
it('requeues only the proven unstarted attempt after actual capacity and stop checks, once',async()=>{
 const f=await fixture(),before=await state(f.task);await recordStorageFailure(pool,f.run);
 expect(await reconcileStorageRecovery(f.inspect,f.request)).toEqual([f.task]);
 expect(await state(f.task)).toMatchObject({state:'ready',attempt_count:3,max_attempts:4,criteria:before.criteria});
 expect((await pool.query('SELECT state FROM runs WHERE id=$1',[f.run])).rows[0].state).toBe('failed');
 expect(await reconcileStorageRecovery(f.inspect,f.request)).toEqual([]);
 expect((await pool.query("SELECT 1 FROM events WHERE kind='STORAGE_PREFLIGHT_REQUEUED'")).rowCount).toBe(1);
 expect((await pool.query('SELECT 1 FROM acceptances WHERE task_id=$1',[f.task])).rowCount).toBe(0);
});
it.each(['DISK_CAPACITY','CONTEXT_TOO_LARGE'])('uses the real native preflight receipt field without trusting a different failure: %s',async(errorCode)=>{
 const f=await fixture();const workerId='opencode-native-123';
 await pool.query("DELETE FROM events WHERE entity_id=$1",[f.task]);
 await pool.query("UPDATE tasks SET state='running',execution=jsonb_set(execution,'{kind}','\"opencode\"') WHERE id=$1",[f.task]);
 await pool.query("UPDATE runs SET state='running',runtime='opencode',worker_id=$2,error_code=NULL WHERE id=$1",[f.run,workerId]);
 await pool.query("INSERT INTO opencode_jobs(run_id,state) VALUES($1,'running')",[f.run]);
 expect(await tx(async db=>{await lock(db);return recordOpenCodeNotStarted(db,{runId:f.run,fence:f.fence,workerId,remoteStartAttempted:false,cancelRequested:false,errorCode});})).toBe(true);
 const receipt=(await pool.query("SELECT payload FROM events WHERE kind='OPENCODE_NOT_STARTED' AND entity_id=$1",[f.task])).rows[0].payload;
 expect(receipt).toMatchObject({confirmed:true,errorCode});expect(receipt).not.toHaveProperty('code');
 expect(await hasStoragePreflightReceipt(pool,f.run,f.task)).toBe(errorCode==='DISK_CAPACITY');
 await pool.query("UPDATE tasks SET state='failed' WHERE id=$1",[f.task]);
 await pool.query("UPDATE runs SET state='failed',error_code='DISK_CAPACITY' WHERE id=$1",[f.run]);
 await pool.query("UPDATE opencode_jobs SET state='finished' WHERE run_id=$1",[f.run]);
 f.inspect.mockImplementation(async()=>({...f.proof,checkedAt:new Date().toISOString(),runtime:'opencode',notStarted:true,state:{runId:f.run,runtime:'opencode',workspace:f.proof.workspace,state:'not_started',code:'DISK_CAPACITY'}}));
 expect(await reconcileStorageRecovery(f.inspect,f.request)).toEqual(errorCode==='DISK_CAPACITY'?[f.task]:[]);
 expect((await state(f.task)).state).toBe(errorCode==='DISK_CAPACITY'?'ready':'failed');
 if(errorCode==='DISK_CAPACITY')expect(f.inspect).toHaveBeenCalledWith(f.run,true);
 else expect(f.inspect).not.toHaveBeenCalled();
 expect((await pool.query('SELECT 1 FROM acceptances WHERE task_id=$1',[f.task])).rowCount).toBe(0);
});
it('keeps a full volume blocked without preparing another copy or launching inspection',async()=>{
 const f=await fixture();f.request.mockResolvedValue({...available(),freeBytes:0,availableBytes:0,available:false});
 expect(await reconcileStorageRecovery(f.inspect,f.request)).toEqual([]);
 expect(f.inspect).not.toHaveBeenCalled();expect(f.request).toHaveBeenCalledTimes(1);
 expect((await state(f.task)).state).toBe('failed');
});
it('recovers the remote preparation producer only after its durable receipt and fresh double stop inspection',async()=>{
 const f=await fixture();
 await pool.query("DELETE FROM events WHERE entity_id=$1",[f.task]);
 await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{kind}','\"opencode\"') WHERE id=$1",[f.task]);
 await pool.query("UPDATE runs SET runtime='opencode' WHERE id=$1",[f.run]);
 await event(pool,'OPENCODE_LAUNCH_ATTEMPTED',f.task,{runId:f.run});
 let durable:any;
 await expect(prepareOpenCodeWorkspace({runId:f.run,workspace:f.proof.workspace,sourceWorkspace:'storage-test/source'},{
  signal:new AbortController().signal,
  persist:async receipt=>{durable=receipt;},
  emit:(kind,receipt)=>event(pool,kind,f.task,receipt),
  operation:async()=>{throw Object.assign(Error('DISK_CAPACITY'),{code:'DISK_CAPACITY'});}
 })).rejects.toMatchObject({code:'DISK_CAPACITY'});
 expect(await hasStoragePreflightReceipt(pool,f.run,f.task)).toBe(true);
 const inspectUnit=vi.fn().mockImplementation(async()=>({...f.proof.process,checkedAt:new Date().toISOString()}));
 f.inspect.mockImplementation(async()=>{
  const proof=await stoppedRunProof(f.run,f.proof.workspace,{
   readState:async()=>({runId:f.run,workspace:f.proof.workspace,runtime:'opencode',state:'failed',stopped:true,code:'DISK_CAPACITY',requests:0,toolCalls:0,notStarted:durable}),
   inspectUnit,inspectWorkspace:async()=>f.proof.candidate,
  });
  return {...proof,runtime:'opencode',notStarted:true,state:{runId:f.run,runtime:'opencode',workspace:f.proof.workspace,state:'not_started',code:'DISK_CAPACITY'}};
 });
 expect(await reconcileStorageRecovery(f.inspect,f.request)).toEqual([f.task]);
 expect(inspectUnit).toHaveBeenCalledTimes(2);
 expect((await state(f.task)).state).toBe('ready');
 expect((await pool.query('SELECT 1 FROM acceptances WHERE task_id=$1',[f.task])).rowCount).toBe(0);
});
it.each(['receipt_missing','runtime_started','cancelled','paused','dependency_pending'])('does not infer replay safety from the disk error: %s',async(mode)=>{
 const f=await fixture();
 if(mode==='receipt_missing')await pool.query("DELETE FROM events WHERE kind='HARNESS_NOT_STARTED'");
 if(mode==='runtime_started'||mode==='cancelled')await pool.query('INSERT INTO events(kind,entity_id,payload) VALUES($1,$2,$3)',[mode==='cancelled'?'CANCEL_REQUESTED':'RUNTIME_STARTED',f.task,JSON.stringify({runId:f.run})]);
 if(mode==='paused')await pool.query("UPDATE goals SET state='paused' WHERE id=$1",[f.goal]);
 if(mode==='dependency_pending'){
  const dep=randomUUID();await pool.query("INSERT INTO tasks(id,goal_id,key,title,description,agent_id,criteria,state) VALUES($1,$2,'dependency','Dependency','Preserve dependency','engineering.backend.node','[]','ready')",[dep,f.goal]);
  await pool.query('INSERT INTO dependencies VALUES($1,$2)',[f.task,dep]);
 }
 expect(await reconcileStorageRecovery(f.inspect,f.request)).toEqual([]);expect((await state(f.task)).state).toBe('failed');
});
it('does not revoke delegated semantic ownership or advance the task while support exists',async()=>{
 const f=await fixture(),id=randomUUID();
 await pool.query("INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,state,reason,evidence,support_task_ids) VALUES($1,$2,$3,$4,'delegated','Support in progress','[]',$5)",[id,f.goal,f.task,f.run,JSON.stringify([randomUUID()])]);
 expect(await reconcileStorageRecovery(f.inspect,f.request)).toEqual([]);
 expect((await pool.query('SELECT state FROM supervision_cases WHERE id=$1',[id])).rows[0].state).toBe('delegated');
 expect((await state(f.task)).state).toBe('failed');
});
it('retires only the unassigned case whose entire evidence is this resource failure',async()=>{
 const f=await fixture(),id=randomUUID();await pool.query("INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,state,reason,evidence) VALUES($1,$2,$3,$4,'open','Repeated disk preflight',$5)",[id,f.goal,f.task,f.run,JSON.stringify([{runId:f.run,code:'DISK_CAPACITY'}])]);
 expect(await reconcileStorageRecovery(f.inspect,f.request)).toEqual([f.task]);
 expect((await pool.query('SELECT state FROM supervision_cases WHERE id=$1',[id])).rows[0].state).toBe('superseded');
});
it('rejects stale stop receipts and contradictory resource observations',async()=>{
 const f=await fixture();f.inspect.mockResolvedValue({...f.proof,checkedAt:new Date(Date.now()-120000).toISOString()});
 expect(await reconcileStorageRecovery(f.inspect,f.request)).toEqual([]);expect((await state(f.task)).state).toBe('failed');
 expect(validStorageObservation({...available(),availableBytes:1})).toBe(false);
 expect(validStorageObservation({...available(),reservedBytes:-1})).toBe(false);
});
it('cannot requeue after a pause arrives during inspection',async()=>{
 const f=await fixture();f.inspect.mockImplementation(async()=>{await pool.query("UPDATE goals SET state='paused' WHERE id=$1",[f.goal]);return f.proof;});
 expect(await reconcileStorageRecovery(f.inspect,f.request)).toEqual([]);expect((await state(f.task)).state).toBe('failed');
});
