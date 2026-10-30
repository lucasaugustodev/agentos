import {beforeAll,beforeEach,afterAll,afterEach,it,expect,vi} from 'vitest';
import {randomUUID} from 'node:crypto';
import {mkdir,writeFile} from 'node:fs/promises';
import path from 'node:path';
import {pool,tx,event} from '../server/db.js';
import {config} from '../server/config.js';
import {seedRegistry} from '../server/registry.js';
import {hash} from '../server/memory.js';
import {prepareSchema} from '../server/schema-readiness.js';
import {prepareBacklogSchema} from '../server/backlog-schema-readiness.js';
import {recordSupervisionDecisions,applySupervisionDecisions,resumeSupervisedWork} from '../server/supervision.js';
import {writeArtifact,completeRun,validateTask,schedule} from '../server/service.js';
import {reconcileDisabledWorkforceLanes,applyDisabledLaneRevision} from '../server/workforce-disabled-lane.js';

beforeAll(async()=>{
  expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');
  // Nested installers use NOWAIT relation locks. Keep their bounded conflict
  // recovery and readiness receipts in the canonical outer preparer.
  await prepareSchema();await prepareBacklogSchema();
});
beforeEach(async()=>{
  await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');await seedRegistry();
  await pool.query("DELETE FROM settings WHERE id IN('disabled-workforce-lane-cursor','maintenance','storage:agentos-workspaces','supervised-resume-cursor')");
  await pool.query(`INSERT INTO settings VALUES('capacity','{"maxRuns":28,"browserSlots":2}') ON CONFLICT(id) DO UPDATE SET value=EXCLUDED.value`);
  await pool.query(`INSERT INTO settings VALUES('remote-capacity','{"maxRuns":26}') ON CONFLICT(id) DO UPDATE SET value=EXCLUDED.value`);
  config.bridgeHost='test-only';config.remoteExecution=true;
});
afterEach(()=>{vi.restoreAllMocks();config.bridgeHost='';config.remoteExecution=false;});
afterAll(async()=>{await pool.query("DELETE FROM settings WHERE id='disabled-workforce-lane-cursor'");await pool.end();});

async function artifact(runId:string,name:string,text:string){
  const id=randomUUID(),bytes=Buffer.from(text),digest=hash(bytes);
  await mkdir(path.join(config.dataDir,'artifacts'),{recursive:true});
  await writeFile(path.join(config.dataDir,'artifacts',id),bytes,{flag:'wx'});
  await pool.query("INSERT INTO artifacts(id,run_id,name,mime,digest,size) VALUES($1,$2,$3,'application/json',$4,$5)",[id,runId,name,digest,bytes.length]);
  return {id,name,digest,size:bytes.length,base64:bytes.toString('base64')};
}
async function fixture(mode='valid'){
  const goal=randomUUID(),task=randomUUID(),run=randomUUID(),fence=randomUUID(),source=randomUUID(),sourceRun=randomUUID();
  const project='disabled-'+goal,workspace=project+'/aux',muse='muse-'+goal,glm='glm-'+goal;
  for(const [id,provider,model] of [[muse,'muse','muse-spark-1.3'],[glm,'openai','glm-5.3-flash']])
    await pool.query(`INSERT INTO accounts(id,label,provider,model,models,enabled,max_concurrency,allow_unknown,bridge_id)
      VALUES($1,'Fixture',$2,$3,$4,true,12,true,'test-only')`,[id,provider,model,JSON.stringify([model])]);
  await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Restore evidence','Original acceptance',$2,$3)",[goal,project,goal]);
  const policy={enabled:true,museTarget:0,glmTarget:12,glmAccountId:glm,workerAccounts:[],glmHarnessEnabled:true,detachedHarnessEnabled:true};
  await pool.query('INSERT INTO settings VALUES($1,$2)',['workforce:'+goal,JSON.stringify(policy)]);
  await pool.query('INSERT INTO settings VALUES($1,$2)',['supervision:'+goal,JSON.stringify({enabled:true})]);
  await pool.query('INSERT INTO settings VALUES($1,$2)',['autonomy:'+project,JSON.stringify({enabled:true})]);
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,attempt_count)
    VALUES($1,$2,'source','Accepted source','Frozen source','qa.backend','accepted','integrate','[]',$3,1)`,
    [source,goal,JSON.stringify({kind:'harness',workspace:project+'/source',baseWorkspace:project+'/base'})]);
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'harness',1,'submitted')",[sourceRun,source,randomUUID()]);
  const input=await artifact(sourceRun,'candidate.json',JSON.stringify({baseDigest:hash('base'),candidateDigest:hash('candidate')}));
  await pool.query("INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) VALUES($1,$2,$3,true,'[]','fixture')",[randomUUID(),source,sourceRun]);
  const execution={kind:'harness',profile:'code',managed:true,workKind:'diagnostic',protocolDelivery:'backlog-v1',readOnlySource:true,
    workspace,sourceWorkspace:project+'/source',baseWorkspace:project+'/base',workforceLane:'muse',inputs:[{id:input.id,name:input.name,digest:input.digest,base64:input.base64}]};
  const criteria=[{id:'report',type:'artifact',name:'report.json',label:'Original evidence'}];
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution,
    attempt_count,account_id,model,allowed_accounts) VALUES($1,$2,'aux','Auxiliary','Exact assigned work','qa.backend',
    'waiting_resource','work',$3,$4,1,$5,'muse-spark-1.3',$6)`,[task,goal,JSON.stringify(criteria),JSON.stringify(execution),muse,JSON.stringify([muse])]);
  // This fixture intentionally represents a pre-snapshot run. The production
  // trigger is restored in finally; no runtime code can create this legacy state.
  if(mode==='legacy')await pool.query('ALTER TABLE runs DISABLE TRIGGER agentos_instruction_snapshot');
  try{await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,finished_at,account_id,model) VALUES($1,$2,1,$3,'harness',1,'submitted',now(),$4,'muse-spark-1.3')",[run,task,fence,muse]);}
  finally{if(mode==='legacy')await pool.query('ALTER TABLE runs ENABLE TRIGGER agentos_instruction_snapshot');}
  const snapshot=(await pool.query('SELECT instruction_snapshot FROM runs WHERE id=$1',[run])).rows[0].instruction_snapshot;
  const contextId=randomUUID();
  await pool.query("INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,$3,'Frozen')",[contextId,run,JSON.stringify({executionInputs:snapshot?.executionInputs??[],dependencyArtifacts:[],taskInstructions:{taskId:task,goalId:goal,revisionId:snapshot?.revisionId??null,descriptionHash:hash('Exact assigned work')}})]);
  await pool.query('UPDATE runs SET context_id=$2 WHERE id=$1',[run,contextId]);
  const output=await artifact(run,'report.json','{"executed":true}');
  const acceptance=randomUUID();
  await pool.query("INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) VALUES($1,$2,$3,true,'[{\"id\":\"report\",\"passed\":true}]','fixture')",[acceptance,task,run]);
  if(mode==='changed')await pool.query(`UPDATE tasks SET execution=execution||'{"recoveryContext":{"instruction":"New semantic input"}}' WHERE id=$1`,[task]);
  const proof=()=>({runId:run,workspace,checkedAt:new Date().toISOString(),candidate:{patch:''},
    process:{unit:'agentos-run-'+run,stopped:true,activeState:'inactive',mainPid:0,populated:false}});
  return {goal,task,run,fence,project,workspace,muse,glm,source,sourceRun,output,acceptance,policy,proof};
}
async function history(f:Awaited<ReturnType<typeof fixture>>){
  return {run:(await pool.query('SELECT * FROM runs WHERE id=$1',[f.run])).rows[0],
    acceptance:(await pool.query('SELECT * FROM acceptances WHERE id=$1',[f.acceptance])).rows[0],
    contexts:(await pool.query('SELECT * FROM contexts WHERE run_id=$1 ORDER BY id',[f.run])).rows,
    artifacts:(await pool.query('SELECT * FROM artifacts WHERE run_id=$1 ORDER BY id',[f.run])).rows};
}
async function decide(f:Awaited<ReturnType<typeof fixture>>,caseId:string,lane:string|null='glm'){
  const id=randomUUID(),run=randomUUID(),fence=randomUUID();
  const action={caseId,kind:'revise',...(lane?{lane}:{}),diagnosis:'The legacy source no longer proves the current frozen contract.',
    instruction:'Recertify the assigned evidence and the original tests on the existing isolated workspace. Verify inputs and source before working; preserve every criterion and report real defects.'};
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,attempt_count)
    VALUES($1,$2,$3,'Supervisor','Review current case','qa.backend','running','coordinate',
      '[{"id":"decision","type":"artifact","name":"coordination.json"}]',1)`,[id,f.goal,'review-'+id]);
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'harness',1,'running')",[run,id,fence]);
  await writeArtifact(run,fence,'coordination.json',JSON.stringify({supervision:{actions:[action]}}),'application/json');
  await completeRun(run,fence,'Executable recertification',{});await validateTask(id);
  await recordSupervisionDecisions();
  expect((await pool.query('SELECT state,decision_run_id FROM supervision_cases WHERE id=$1',[caseId])).rows[0])
    .toEqual({state:'decided',decision_run_id:run});
  return {action,run};
}

it('restores only the accepted projection with full frozen/input/output/stop proof and is idempotent',async()=>{
  const f=await fixture(),before=await history(f),task=(await pool.query('SELECT * FROM tasks WHERE id=$1',[f.task])).rows[0];
  expect(await reconcileDisabledWorkforceLanes(async()=>f.proof())).toMatchObject({restored:1,requested:0});
  const after=(await pool.query('SELECT * FROM tasks WHERE id=$1',[f.task])).rows[0];
  expect(after).toEqual({...task,state:'accepted',reason:after.reason,updated_at:after.updated_at});
  expect(await history(f)).toEqual(before);
  expect(await reconcileDisabledWorkforceLanes(async()=>f.proof())).toMatchObject({examined:0});
  expect((await pool.query("SELECT 1 FROM events WHERE kind='ACCEPTED_AUXILIARY_PROJECTION_RESTORED'")).rowCount).toBe(1);
  expect((await pool.query('SELECT 1 FROM supervision_cases')).rowCount).toBe(0);
});

it.each(['legacy','changed','corrupt'])('routes %s evidence to one typed supervised recertification without accepting or rerunning it',async mode=>{
  const f=await fixture(mode),before=await history(f);
  if(mode==='corrupt')await writeFile(path.join(config.dataDir,'artifacts',f.output.id),'corrupt');
  const first=await reconcileDisabledWorkforceLanes(async()=>f.proof());
  expect(first).toMatchObject({restored:0,requested:1});
  const c=(await pool.query('SELECT * FROM supervision_cases WHERE task_id=$1',[f.task])).rows[0];
  expect(c).toMatchObject({state:'open',source_run_id:f.run});
  expect(c.evidence[0]).toMatchObject({kind:'WORKFORCE_LANE_DISABLED',requiredAction:{kind:'revise',lane:'glm'},
    proofFailure:mode==='legacy'?'LEGACY_SNAPSHOT_MISSING':mode==='changed'?'FROZEN_CONTRACT_CHANGED':'OUTPUT_BYTES_CHANGED'});
  const held=(await pool.query("SELECT payload FROM events WHERE kind='WORKFORCE_LANE_RECOVERY_REQUESTED' AND entity_id=$1",[f.task])).rows[0].payload;
  expect(held.originalContractHash).toBe(c.evidence[0].taskContractHash);
  expect(held.heldContractHash).not.toBe(held.originalContractHash);
  expect((await pool.query('SELECT state,attempt_count,account_id FROM tasks WHERE id=$1',[f.task])).rows[0])
    .toEqual({state:'recovery_blocked',attempt_count:1,account_id:f.muse});
  expect(await history(f)).toEqual(before);
  expect(await reconcileDisabledWorkforceLanes(async()=>f.proof())).toMatchObject({examined:0});
  expect((await pool.query('SELECT 1 FROM supervision_cases')).rowCount).toBe(1);
});

it.each(['cancel','lease','live','paused','autonomy','manual','enabled_lane','stale_stop','process_live','contract_race','new_run_race'])
('never restores or routes through %s',async mode=>{
  const f=await fixture();
  if(mode==='cancel')await event(pool,'CANCEL_REQUESTED',f.task,{runId:f.run});
  if(mode==='lease')await pool.query("INSERT INTO leases VALUES('remote-slot',$1)",[f.run]);
  if(mode==='live'){
    await pool.query(`UPDATE settings SET value=jsonb_set(value,'{museTarget}','6') WHERE id=$1`,['workforce:'+f.goal]);
    await pool.query("UPDATE runs SET state='reconciling' WHERE id=$1",[f.run]);
    await pool.query(`UPDATE settings SET value=jsonb_set(value,'{museTarget}','0') WHERE id=$1`,['workforce:'+f.goal]);
  }
  if(mode==='paused')await pool.query("UPDATE tasks SET state='paused' WHERE id=$1",[f.task]);
  if(mode==='autonomy')await pool.query("UPDATE settings SET value='{\"enabled\":false}' WHERE id=$1",['autonomy:'+f.project]);
  if(mode==='manual'){await pool.query("UPDATE tasks SET recovery='{\"mode\":\"manual\"}' WHERE id=$1",[f.task]);await pool.query('DELETE FROM settings WHERE id=$1',['autonomy:'+f.project]);}
  if(mode==='enabled_lane')await pool.query(`UPDATE settings SET value=jsonb_set(value,'{museTarget}','6') WHERE id=$1`,['workforce:'+f.goal]);
  const result=await reconcileDisabledWorkforceLanes(async()=>{
    if(mode==='contract_race')await pool.query("UPDATE tasks SET description='Changed during inspection' WHERE id=$1",[f.task]);
    if(mode==='new_run_race'){await pool.query('UPDATE tasks SET attempt_count=2 WHERE id=$1',[f.task]);await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,2,$3,'harness',1,'submitted')",[randomUUID(),f.task,randomUUID()]);}
    const proof=f.proof();if(mode==='stale_stop')proof.checkedAt=new Date(Date.now()-120000).toISOString();if(mode==='process_live')proof.process.mainPid=123;
    return proof;
  });
  expect(result.restored).toBe(0);expect(result.requested).toBe(0);
  expect((await pool.query('SELECT 1 FROM supervision_cases')).rowCount).toBe(0);
  expect((await pool.query('SELECT account_id FROM tasks WHERE id=$1',[f.task])).rows[0].account_id).toBe(f.muse);
});

it('uses an explicit accepted revise to move only the future Harness route, then admits one real new attempt',async()=>{
  const f=await fixture('changed'),before=await history(f);
  await reconcileDisabledWorkforceLanes(async()=>f.proof());
  const c=(await pool.query('SELECT * FROM supervision_cases WHERE task_id=$1',[f.task])).rows[0];
  const original=(await pool.query('SELECT * FROM tasks WHERE id=$1',[f.task])).rows[0];
  // A routine detector/scheduler update renews the hold clocks. It must not
  // invalidate authority for the future route or alter the acceptance proof.
  await pool.query("UPDATE tasks SET state='recovery_blocked' WHERE id=$1",[f.task]);
  await decide(f,c.id);await applySupervisionDecisions(async()=>f.proof());
  const applied=(await pool.query('SELECT state,reason FROM supervision_cases WHERE id=$1',[c.id])).rows[0];
  expect(applied,JSON.stringify(applied)).toMatchObject({state:'applied'});
  const revised=(await pool.query('SELECT * FROM tasks WHERE id=$1',[f.task])).rows[0];
  const {continuation,...originalExecution}=original.execution;
  expect(revised).toMatchObject({account_id:f.glm,model:'glm-5.3-flash',allowed_accounts:[f.glm],attempt_count:1,
    criteria:original.criteria,execution:{...originalExecution,workforceLane:'glm'}});
  expect(revised.execution.continuation).toBeUndefined();
  expect(await history(f)).toEqual(before);
  expect(await resumeSupervisedWork(async()=>f.proof())).toContain(f.task);
  await schedule('agentos-opencode');
  const latest=(await pool.query('SELECT * FROM runs WHERE task_id=$1 ORDER BY attempt DESC',[f.task])).rows;
  expect(latest).toHaveLength(2);expect(latest[0]).toMatchObject({attempt:2,state:'queued',account_id:f.glm,runtime:'harness'});
  expect(latest[0].instruction_snapshot.task).toMatchObject({criteria:original.criteria,allowed_accounts:[f.glm],execution:{kind:'harness',workforceLane:'glm',workspace:f.workspace,inputs:original.execution.inputs}});
  expect(latest[1].instruction_snapshot).toEqual(before.run.instruction_snapshot);
  expect((await pool.query('SELECT * FROM acceptances WHERE id=$1',[f.acceptance])).rows[0]).toEqual(before.acceptance);
  await applySupervisionDecisions(async()=>f.proof());
  expect((await pool.query("SELECT 1 FROM events WHERE kind='WORKFORCE_DISABLED_LANE_REVISED'")).rowCount).toBe(1);
});

it.each(['no_lane','account_disabled','quota','policy','cancel','changed','wrong_decision','goal_paused','missing_hold','hold_case','hold_run'])
('rejects a future lane revision with %s and preserves historical acceptance',async mode=>{
  const f=await fixture('changed');await reconcileDisabledWorkforceLanes(async()=>f.proof());
  const c=(await pool.query('SELECT * FROM supervision_cases WHERE task_id=$1',[f.task])).rows[0];
  const d=await decide(f,c.id,mode==='no_lane'?null:'glm'),before=await history(f);
  if(mode==='account_disabled')await pool.query('UPDATE accounts SET enabled=false WHERE id=$1',[f.glm]);
  if(mode==='quota')await pool.query(`UPDATE accounts SET quota='{"ordinaryUsageAllowed":false}' WHERE id=$1`,[f.glm]);
  if(mode==='policy')await pool.query(`UPDATE settings SET value=jsonb_set(value,'{glmHarnessEnabled}','false') WHERE id=$1`,['workforce:'+f.goal]);
  if(mode==='cancel')await event(pool,'CANCEL_REQUESTED',f.task,{runId:f.run});
  if(mode==='changed')await pool.query("UPDATE tasks SET description='New instruction' WHERE id=$1",[f.task]);
  // Public pause semantics are goal-scoped (app PATCH goal state / manager pause).
  if(mode==='goal_paused'){
    await pool.query("UPDATE goals SET state='paused' WHERE id=$1",[f.goal]);
    expect((await pool.query('SELECT state FROM goals WHERE id=$1',[f.goal])).rows[0].state).toBe('paused');
  }
  if(mode==='missing_hold')await pool.query("DELETE FROM events WHERE kind='WORKFORCE_LANE_RECOVERY_REQUESTED' AND entity_id=$1",[f.task]);
  if(mode==='wrong_decision')await pool.query('INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) SELECT $1,task_id,id,false,\'[]\',\'fixture\' FROM runs WHERE id=$2',[randomUUID(),d.run]);
  await expect(tx(async db=>{
    await db.query("UPDATE supervision_cases SET state='applied',applied_attempt=1 WHERE id=$1",[c.id]);
    if(mode==='hold_case'||mode==='hold_run')await db.query('UPDATE tasks SET execution=jsonb_set(execution,$2::text[],to_jsonb($3::text)) WHERE id=$1',
      [f.task,['continuation',mode==='hold_case'?'caseId':'runId'],randomUUID()]);
    return applyDisabledLaneRevision(db,c.id,d.action,f.proof());
  })).rejects.toThrow();
  expect((await pool.query('SELECT account_id FROM tasks WHERE id=$1',[f.task])).rows[0].account_id).toBe(f.muse);
  expect(await history(f)).toEqual(before);
});

it('rotates the bounded inspection past eight unconfirmed actors without retrying or discarding them',async()=>{
  const tasks:Awaited<ReturnType<typeof fixture>>[]=[];for(let i=0;i<9;i++)tasks.push(await fixture());
  tasks.sort((a,b)=>a.task.localeCompare(b.task));
  const inspect=vi.fn(async(runId:string)=>{
    if(runId!==tasks[8].run)throw Error('STOP_UNCONFIRMED');
    return tasks[8].proof();
  });
  expect(await reconcileDisabledWorkforceLanes(inspect)).toMatchObject({examined:8,restored:0});
  expect(inspect.mock.calls.some(c=>c[0]===tasks[8].run)).toBe(false);
  expect(await reconcileDisabledWorkforceLanes(inspect)).toMatchObject({examined:8,restored:1});
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1',[tasks[8].task])).rows[0].state).toBe('accepted');
  expect((await pool.query('SELECT 1 FROM supervision_cases')).rowCount).toBe(0);
});

it('concurrent reconciliation commits a single restoration receipt',async()=>{
  const f=await fixture(),inspect=async()=>f.proof();
  const results=await Promise.all([reconcileDisabledWorkforceLanes(inspect),reconcileDisabledWorkforceLanes(inspect)]);
  expect(results.reduce((sum,r)=>sum+r.restored,0)).toBe(1);
  expect((await pool.query("SELECT 1 FROM events WHERE kind='ACCEPTED_AUXILIARY_PROJECTION_RESTORED' AND entity_id=$1",[f.task])).rowCount).toBe(1);
});
