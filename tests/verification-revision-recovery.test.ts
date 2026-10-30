import {beforeAll,beforeEach,afterAll,it,expect,vi} from 'vitest';
import {randomUUID,createHash} from 'node:crypto';
import {mkdir,writeFile} from 'node:fs/promises';
import path from 'node:path';
import {pool,migrate,tx} from '../server/db.js';
import {config} from '../server/config.js';
import {seedRegistry} from '../server/registry.js';
import {installSupervision} from '../server/supervision.js';
import {installTaskExecutionMetadata} from '../server/task-execution-metadata.js';
import {currentSemanticDecision} from '../server/semantic-retry.js';
import {setSupervisionInstruction,reviseInstruction} from '../server/task-instructions.js';
import {reconcileVerificationRevisions} from '../server/verification-revision-recovery.js';
import {queueInterruptedVerificationRecheckInTransaction} from '../server/continuation.js';
import {verificationInterruptionCodes,interruptedVerificationReason} from '../server/verification-interruption.js';
const sha=(s:string|Buffer)=>createHash('sha256').update(s).digest('hex');
const currentTask=async(id:string)=>(await pool.query('SELECT * FROM tasks WHERE id=$1',[id])).rows[0];
beforeAll(async()=>{expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');await migrate();await installTaskExecutionMetadata();await installSupervision();});
beforeEach(async()=>{await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');await seedRegistry();});
afterAll(()=>pool.end());
async function artifact(run:string,name:string,value:any){
 const id=randomUUID(),bytes=Buffer.from(JSON.stringify(value));await mkdir(path.join(config.dataDir,'artifacts'),{recursive:true});
 await writeFile(path.join(config.dataDir,'artifacts',id),bytes);
 await pool.query('INSERT INTO artifacts(id,run_id,name,digest,size,mime) VALUES($1,$2,$3,$4,$5,\'application/json\')',[id,run,name,sha(bytes),bytes.length]);return id;
}
async function fixture(){
 const goal=randomUUID(),qa=randomUUID(),parent=randomUUID(),coordinator=randomUUID(),run=randomUUID(),parentRun=randomUUID(),review=randomUUID(),caseId=randomUUID();
 const prefix='verification-revision-'+goal,workspace=prefix+'/qa';
 await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key,state) VALUES($1::uuid,'Real QA loop','Keep original criteria',$2,$1::text,'active')",[goal,prefix]);
 await pool.query('INSERT INTO settings VALUES($1,$2)',['supervision:'+goal,JSON.stringify({enabled:true})]);
 const criteria=[{id:'passed',label:'Independent verification',type:'json_field',name:'verification.json',field:'passed',equals:true}];
 for(const [id,key,state,stage,execution] of [
  [parent,'implementation','accepted','integrate',{kind:'harness',workspace:prefix+'/implementation',baseWorkspace:prefix+'/base'}],
  [qa,'qa','submitted','verify',{kind:'harness',workspace,baseWorkspace:prefix+'/base',sourceWorkspace:prefix+'/implementation',readOnlySource:true,supervision:{caseId,sourceRunId:run}}],
  [coordinator,'coordinator','accepted','coordinate',null]] as const)
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,execution,criteria,attempt_count)
   VALUES($1,$2,$3,$3,'Original criteria; never claim the game is accepted','qa.backend',$4,$5,$6,$7,1)`,[id,goal,key,state,stage,execution?JSON.stringify(execution):null,JSON.stringify(criteria)]);
 for(const [rid,tid] of [[run,qa],[parentRun,parent],[review,coordinator]]){
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,finished_at) VALUES($1,$2,1,$3,'harness',1,'submitted',now())",[rid,tid,randomUUID()]);
  await pool.query("INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) VALUES($1,$2,$3,$4,$5,'canonical-fixture')",[randomUUID(),tid,rid,tid!==qa,JSON.stringify([{id:'passed',type:'json_field',passed:tid!==qa}])]);
 }
 await pool.query('INSERT INTO dependencies VALUES($1,$2)',[qa,parent]);
 const action={caseId,kind:'revise',diagnosis:'Recheck real browser evidence with writable scratch output.',instruction:'Keep source read-only, preserve all original gates, and publish actual verification.'};
 const decisionArtifact=await artifact(review,'coordination.json',{supervision:{actions:[action]}});
 await pool.query(`INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,state,reason,evidence,decision,decision_run_id,applied_attempt)
  VALUES($1,$2,$3,$4,'applied','Revised QA','[]',$5,$6,1)`,[caseId,goal,qa,run,JSON.stringify({actions:[action]}),review]);
 const digest=sha('Canonical unchanged implementation');
 await artifact(run,'candidate.json',{candidateDigest:digest,baseDigest:digest});await artifact(parentRun,'candidate.json',{candidateDigest:digest,baseDigest:digest});
 const fence=(await pool.query('SELECT fence FROM runs WHERE id=$1',[run])).rows[0].fence;
 const inspect=vi.fn(async()=>({runId:run,workspace,checkedAt:new Date().toISOString(),process:{unit:'agentos-run-'+run,stopped:true,mainPid:0,populated:false,activeState:'inactive'}}));
 return {goal,qa,parent,run,parentRun,caseId,review,decisionArtifact,workspace,fence,criteria,inspect};
}
async function interrupt(f:Awaited<ReturnType<typeof fixture>>,code='MODEL_OUTPUT_LIMIT'){
 await pool.query("UPDATE tasks SET state='failed' WHERE id=$1",[f.qa]);
 await pool.query("UPDATE runs SET state='failed',error_code=$2 WHERE id=$1",[f.run,code]);
 await pool.query('DELETE FROM acceptances WHERE run_id=$1',[f.run]);
 await pool.query("DELETE FROM artifacts WHERE run_id=$1 AND name='candidate.json'",[f.run]);
}
async function newVerificationInput(f:Awaited<ReturnType<typeof fixture>>){
 const refs=(await pool.query('SELECT id,name,digest,$2::uuid task_id FROM artifacts WHERE run_id=$1',[f.parentRun,f.parent])).rows;
 const context=randomUUID(),bytes=Buffer.from(JSON.stringify({counterexample:'New bounded browser reproduction for the failed criterion'}));
 await pool.query("INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,$3,'Original QA dependency evidence')",
  [context,f.run,JSON.stringify({dependencyArtifacts:refs})]);
 await pool.query('UPDATE runs SET context_id=$2 WHERE id=$1',[f.run,context]);
 await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{inputs}',$2::jsonb) WHERE id=$1",
  [f.qa,JSON.stringify([{id:randomUUID(),name:'counterexample.json',digest:sha(bytes),base64:bytes.toString('base64')}])]);
}
it('consumes an already applied context-capacity revision after generic recovery deferred it to planning',async()=>{
 const f=await fixture();await interrupt(f,'CONTEXT_TOO_LARGE');
 await pool.query("UPDATE tasks SET state='recovery_blocked',reason='Planner must divide the delivery' WHERE id=$1",[f.qa]);
 await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('REPLANNING_REQUIRED',$1,$2)",
  [f.qa,JSON.stringify({runId:f.run,reason:'context_capacity'})]);
 const before=(await pool.query('SELECT * FROM runs WHERE id=$1',[f.run])).rows[0];
 expect(await currentSemanticDecision(pool,await currentTask(f.qa),before,
  (await pool.query('SELECT * FROM supervision_cases WHERE id=$1',[f.caseId])).rows[0])).toBe(true);
 expect(await reconcileVerificationRevisions(f.inspect)).toEqual([{taskId:f.qa,status:'recheck_queued'}]);
 const after=await currentTask(f.qa);
 expect(after.attempt_count).toBe(1);expect(after.criteria).toEqual(f.criteria);
 expect(after.execution.readOnlySource).toBe(true);
 expect(after.execution.verificationRecheck).toMatchObject({sourceRunId:f.run,reason:interruptedVerificationReason});
 expect((await pool.query('SELECT * FROM runs WHERE id=$1',[f.run])).rows[0]).toEqual(before);
 expect((await pool.query('SELECT count(*)::int n FROM acceptances WHERE run_id=$1',[f.run])).rows[0].n).toBe(0);
});
it.each(verificationInterruptionCodes)('reconciles a physically stopped %s QA after its real accepted revision without inventing a candidate or result',async code=>{
 const f=await fixture();await interrupt(f,code);
 // A previous evaluation of this implementation is not a result of this
 // interrupted attempt. The new revision must actually complete independent QA.
 await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('SUPERVISION_QA_RECHECK_READY',$1,$2)",
  [f.qa,JSON.stringify({reason:'APPLIED_REVISION_RECONCILED',implementationRunId:f.parentRun})]);
 const before=(await pool.query('SELECT * FROM runs WHERE id=$1',[f.run])).rows[0];
 const out=await reconcileVerificationRevisions(f.inspect);expect(out).toEqual([{taskId:f.qa,status:'recheck_queued'}]);
 const task=(await pool.query('SELECT * FROM tasks WHERE id=$1',[f.qa])).rows[0];
 expect(task.state).toBe('blocked_dependencies');expect(task.attempt_count).toBe(1);
 expect(task.criteria).toEqual(f.criteria);expect(task.execution.readOnlySource).toBe(true);
 expect(task.execution.sourceWorkspace).toContain('/implementation');
 expect(task.execution.verificationRecheck).toMatchObject({sourceRunId:f.run,sourceAttempt:1,targetRunId:f.parentRun,reason:interruptedVerificationReason});
 expect((await pool.query('SELECT * FROM runs WHERE id=$1',[f.run])).rows[0]).toEqual(before);
 expect((await pool.query('SELECT count(*)::int n FROM acceptances WHERE run_id=$1',[f.run])).rows[0].n).toBe(0);
 expect((await pool.query('SELECT count(*)::int n FROM artifacts WHERE run_id=$1',[f.run])).rows[0].n).toBe(0);
 expect((await pool.query('SELECT state FROM tasks WHERE id=$1',[f.parent])).rows[0].state).toBe('accepted');
 expect(await reconcileVerificationRevisions(f.inspect)).toEqual([]);
});
it.each(['pause','cancel','live','pending-case','unbound-decision','stale-stop','wrong-workspace','parent-unaccepted','changed-candidate','ordinary-failure'])(
 'does not recover an interrupted readonly QA with %s evidence or ownership',async mode=>{
 const f=await fixture();await interrupt(f);
 if(mode==='pause')await pool.query("UPDATE goals SET state='paused' WHERE id=$1",[f.goal]);
 if(mode==='cancel')await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('CANCEL_REQUESTED',$1,$2)",[f.qa,JSON.stringify({runId:f.run})]);
 if(mode==='live')await pool.query("UPDATE runs SET state='running' WHERE id=$1",[f.run]);
 if(mode==='pending-case')await pool.query("INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,state,reason,evidence,request_key) VALUES($1,$2,$3,$4,'open','Other owner','[]','new-interruption-owner')",[randomUUID(),f.goal,f.qa,f.run]);
 if(mode==='unbound-decision')await pool.query('UPDATE artifacts SET digest=$2 WHERE id=$1',[f.decisionArtifact,'f'.repeat(64)]);
 if(mode==='parent-unaccepted')await pool.query("UPDATE tasks SET state='submitted' WHERE id=$1",[f.parent]);
 if(mode==='ordinary-failure')await pool.query("UPDATE runs SET error_code='ASSERTION_FAILED' WHERE id=$1",[f.run]);
 if(mode==='changed-candidate')await artifact(f.run,'candidate.json',{baseDigest:sha('old'),candidateDigest:sha('modified')});
 const inspect=mode==='stale-stop'?async()=>({...await f.inspect(),checkedAt:'2020-01-01T00:00:00Z'}):
  mode==='wrong-workspace'?async()=>({...await f.inspect(),workspace:'unrelated/workspace'}):f.inspect;
 const out=await reconcileVerificationRevisions(inspect);
 expect(out.some(x=>x.status==='recheck_queued')).toBe(false);
 expect((await pool.query("SELECT count(*)::int n FROM events WHERE entity_id=$1 AND kind='VERIFICATION_RECHECK_QUEUED'",[f.qa])).rows[0].n).toBe(0);
 expect((await pool.query('SELECT count(*)::int n FROM runs WHERE task_id=$1',[f.qa])).rows[0].n).toBe(1);
 expect((await pool.query('SELECT criteria FROM tasks WHERE id=$1',[f.qa])).rows[0].criteria).toEqual(f.criteria);
});
it('rejects a direct interrupted queue call without an exact current accepted semantic revision',async()=>{
 const f=await fixture();await interrupt(f);
 const request={version:1,sourceTaskId:f.qa,sourceRunId:f.run,sourceAttempt:1,targetTaskId:f.parent,targetRunId:f.parentRun};
 await expect(tx(db=>queueInterruptedVerificationRecheckInTransaction(db,request,{caseId:randomUUID(),proof:{}})))
  .rejects.toThrow('VERIFICATION_INTERRUPTION_DECISION_UNBOUND');
 expect((await pool.query('SELECT state FROM tasks WHERE id=$1',[f.qa])).rows[0].state).toBe('failed');
});
it.each(['release','cancel','pause'])('queues fairly behind a busy admission lock and rechecks %s ownership before acting',async mode=>{
 const f=await fixture();await interrupt(f);
 const holder=await pool.connect();let pending:Promise<any>|undefined;
 try{
  await holder.query('SELECT pg_advisory_lock(4280001)');
  pending=reconcileVerificationRevisions(f.inspect);
  // Observe a real blocked lock request, never a fixed delay that assumes the
  // callback already ran. The old try-lock path cannot produce this waiter.
  await vi.waitFor(async()=>{
   const waiting=(await pool.query(`SELECT count(*)::int n FROM pg_locks WHERE locktype='advisory'
    AND database=(SELECT oid FROM pg_database WHERE datname=current_database())
    AND classid=0 AND objid=4280001 AND objsubid=1 AND NOT granted`)).rows[0].n;
   expect(waiting).toBeGreaterThan(0);
  },{timeout:3000,interval:20});
  if(mode==='cancel')await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('CANCEL_REQUESTED',$1,$2)",[f.qa,JSON.stringify({runId:f.run})]);
  if(mode==='pause')await pool.query("UPDATE goals SET state='paused' WHERE id=$1",[f.goal]);
  await holder.query('SELECT pg_advisory_unlock(4280001)');
  const result=await pending;
  if(mode==='release')expect(result).toEqual([{taskId:f.qa,status:'recheck_queued'}]);
  else expect(result).toEqual([{taskId:f.qa,status:'owner_changed'}]);
  expect((await pool.query('SELECT criteria FROM tasks WHERE id=$1',[f.qa])).rows[0].criteria).toEqual(f.criteria);
 }finally{
  await holder.query('SELECT pg_advisory_unlock_all()');holder.release();await pending;
 }
});
it('times out a busy admission owner without changing QA, its accepted decision or previous attempt',async()=>{
 const f=await fixture();await interrupt(f);const holder=await pool.connect();
 const before=(await pool.query('SELECT state,execution,criteria FROM tasks WHERE id=$1',[f.qa])).rows[0];
 try{
  await holder.query('SELECT pg_advisory_lock(4280001)');
  expect(await reconcileVerificationRevisions(f.inspect)).toEqual([{taskId:f.qa,status:'admission_busy'}]);
  expect((await pool.query('SELECT state,execution,criteria FROM tasks WHERE id=$1',[f.qa])).rows[0]).toEqual(before);
  expect((await pool.query('SELECT state FROM supervision_cases WHERE id=$1',[f.caseId])).rows[0].state).toBe('applied');
  expect((await pool.query('SELECT count(*)::int n FROM runs WHERE task_id=$1',[f.qa])).rows[0].n).toBe(1);
 }finally{await holder.query('SELECT pg_advisory_unlock_all()');holder.release();}
});
it.each(['bound','tampered'])('recovers a later interrupted turn only when its previously consumed failed-source recheck is %s',async mode=>{
 const f=await fixture();await interrupt(f);
 expect(await reconcileVerificationRevisions(f.inspect)).toEqual([{taskId:f.qa,status:'recheck_queued'}]);
 const nextRun=randomUUID(),nextCase=randomUUID(),nextReview=randomUUID();
 await pool.query("UPDATE tasks SET state='failed',attempt_count=2 WHERE id=$1",[f.qa]);
 await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,error_code,finished_at) VALUES($1,$2,2,$3,'harness',1,'failed','INCOMPLETE_TURN',now())",[nextRun,f.qa,randomUUID()]);
 const coordinator=(await pool.query('SELECT task_id FROM runs WHERE id=$1',[f.review])).rows[0].task_id;
 await pool.query('UPDATE tasks SET attempt_count=2 WHERE id=$1',[coordinator]);
 await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,finished_at) VALUES($1,$2,2,$3,'harness',1,'submitted',now())",[nextReview,coordinator,randomUUID()]);
 await pool.query("INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) VALUES($1,$2,$3,true,'[]','canonical-fixture')",[randomUUID(),coordinator,nextReview]);
 const action={caseId:nextCase,kind:'revise',diagnosis:'The interrupted turn never published a completed independent result.',instruction:'Complete the original independent QA with bounded final output; retain every criterion and actual failure.'};
 await artifact(nextReview,'coordination.json',{supervision:{actions:[action]}});
 await pool.query("INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,state,reason,evidence,decision,decision_run_id,applied_attempt) VALUES($1,$2,$3,$4,'applied','Next interrupted round','[]',$5,$6,2)",[nextCase,f.goal,f.qa,nextRun,JSON.stringify({actions:[action]}),nextReview]);
 await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{supervision}',$2::jsonb) WHERE id=$1",[f.qa,JSON.stringify({caseId:nextCase,sourceRunId:nextRun})]);
 await tx(db=>setSupervisionInstruction(db,f.qa,nextCase,'revise',reviseInstruction(action),nextRun));
 if(mode==='tampered')await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{verificationRecheck,candidateDigest}',$2::jsonb) WHERE id=$1",[f.qa,JSON.stringify('f'.repeat(64))]);
 const task=(await pool.query('SELECT * FROM tasks WHERE id=$1',[f.qa])).rows[0];
 const inspect=vi.fn(async()=>({runId:nextRun,workspace:task.execution.workspace,checkedAt:new Date().toISOString(),process:{unit:'agentos-run-'+nextRun,stopped:true,mainPid:0,populated:false,activeState:'inactive'}}));
 const result=await reconcileVerificationRevisions(inspect);
 if(mode==='tampered'){expect(result).toEqual([]);expect(inspect).not.toHaveBeenCalled();return;}
 expect(result).toEqual([{taskId:f.qa,status:'recheck_queued'}]);
 const current=(await pool.query('SELECT execution FROM tasks WHERE id=$1',[f.qa])).rows[0].execution;
 expect(current.verificationRecheck).toMatchObject({sourceRunId:nextRun,sourceAttempt:2,targetRunId:f.parentRun,reason:interruptedVerificationReason});
 expect((await pool.query('SELECT count(*)::int n FROM runs WHERE task_id=$1',[f.qa])).rows[0].n).toBe(2);
 expect((await pool.query('SELECT count(*)::int n FROM acceptances WHERE task_id=$1',[f.qa])).rows[0].n).toBe(0);
});
it('turns a real accepted applied QA revision into one bound independent recheck and preserves the original attempt, artifacts, implementation and criteria',async()=>{
 const f=await fixture(),before=(await pool.query('SELECT instruction_snapshot FROM runs WHERE id=$1',[f.run])).rows[0];
 await newVerificationInput(f);
 const result=await reconcileVerificationRevisions(f.inspect);expect(result).toEqual([{taskId:f.qa,status:'recheck_queued'}]);
 const t=(await pool.query('SELECT state,execution,criteria,attempt_count,description FROM tasks WHERE id=$1',[f.qa])).rows[0];
 expect(t.state).toBe('blocked_dependencies');expect(t.execution.readOnlySource).toBe(true);expect(t.execution.workspace).toContain('/oqa-');
 expect(t.execution.verificationRecheck).toMatchObject({sourceRunId:f.run,targetRunId:f.parentRun,reason:'APPLIED_REVISION_RECONCILED'});
 expect(t.criteria).toEqual(f.criteria);expect(t.attempt_count).toBe(1);expect(t.description).toContain('QA_RECHECK_V1');
 expect((await pool.query('SELECT state FROM tasks WHERE id=$1',[f.parent])).rows[0].state).toBe('accepted');
 expect((await pool.query('SELECT instruction_snapshot FROM runs WHERE id=$1',[f.run])).rows[0]).toEqual(before);
 expect((await pool.query('SELECT passed FROM acceptances WHERE run_id=$1',[f.run])).rows[0].passed).toBe(false);
 expect(await reconcileVerificationRevisions(f.inspect)).toEqual([]);expect(f.inspect).toHaveBeenCalledTimes(1);
 expect((await pool.query("SELECT count(*)::int n FROM events WHERE entity_id=$1 AND kind='VERIFICATION_RECHECK_QUEUED'",[f.qa])).rows[0].n).toBe(1);
});
it('an applied textual revision alone routes unchanged material QA to one review instead of replaying it',async()=>{
 const f=await fixture();
 const result=await reconcileVerificationRevisions(f.inspect);
 expect(result[0]).toMatchObject({taskId:f.qa,status:'review'});
 expect((await pool.query('SELECT evidence FROM supervision_cases WHERE id=$1',[result[0].caseId])).rows[0].evidence[0].code)
  .toBe('VERIFICATION_NO_PROGRESS');
 expect(await reconcileVerificationRevisions(f.inspect)).toEqual([]);
 expect((await pool.query("SELECT 1 FROM events WHERE kind='VERIFICATION_RECHECK_QUEUED' AND entity_id=$1",[f.qa])).rowCount).toBe(0);
 expect((await pool.query('SELECT count(*)::int n FROM runs WHERE task_id=$1',[f.qa])).rows[0].n).toBe(1);
});
it.each(['pause','cancel','live','pending-case','accepted-result'])('does not disturb %s ownership or create a recheck',async mode=>{
 const f=await fixture();
 if(mode==='pause')await pool.query("UPDATE goals SET state='paused' WHERE id=$1",[f.goal]);
 if(mode==='cancel')await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('CANCEL_REQUESTED',$1,$2)",[f.qa,JSON.stringify({runId:f.run})]);
 if(mode==='live')await pool.query("UPDATE runs SET state='running' WHERE id=$1",[f.run]);
 if(mode==='pending-case')await pool.query("INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,state,reason,evidence,request_key) VALUES($1,$2,$3,$4,'open','New decision owns task','[]','new-phase')",[randomUUID(),f.goal,f.qa,f.run]);
 if(mode==='accepted-result')await pool.query('INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) VALUES($1,$2,$3,true,\'[]\',\'latest-result\')',[randomUUID(),f.qa,f.run]);
 const before=(await pool.query('SELECT state,execution,criteria FROM tasks WHERE id=$1',[f.qa])).rows[0];
 expect(await reconcileVerificationRevisions(f.inspect)).toEqual([]);expect(f.inspect).not.toHaveBeenCalled();
 expect((await pool.query('SELECT state,execution,criteria FROM tasks WHERE id=$1',[f.qa])).rows[0]).toEqual(before);
});
it('rechecks decision and run fences after stop inspection; a concurrent edit grants no recheck',async()=>{
 const f=await fixture();
 const inspect=async()=>{await pool.query('UPDATE artifacts SET digest=$2 WHERE id=$1',[f.decisionArtifact,'f'.repeat(64)]);return f.inspect();};
 expect(await reconcileVerificationRevisions(inspect)).toEqual([{taskId:f.qa,status:'owner_changed'}]);
 expect((await pool.query('SELECT execution FROM tasks WHERE id=$1',[f.qa])).rows[0].execution.verificationRecheck).toBeUndefined();
});
it('selects only the bound accepted implementation among dependencies with large inline inputs',async()=>{
 const f=await fixture();
 await newVerificationInput(f);
 const unrelated=randomUUID(),otherRun=randomUUID(),bytes=Buffer.alloc(2*1024*1024,42);
 await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,execution,criteria,attempt_count)
  VALUES($1,$2,'unrelated','Unrelated evidence','Keep unrelated input out of QA source selection','qa.backend','accepted','work',$3,'[]',1)`,
  [unrelated,f.goal,JSON.stringify({kind:'harness',workspace:'unrelated/source',inputs:[{id:randomUUID(),name:'unrelated.bin',digest:sha(bytes),base64:bytes.toString('base64')}]})]);
 await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,finished_at) VALUES($1,$2,1,$3,'harness',1,'submitted',now())",[otherRun,unrelated,randomUUID()]);
 await pool.query("INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) VALUES($1,$2,$3,true,'[]','canonical-fixture')",[randomUUID(),unrelated,otherRun]);
 await pool.query('INSERT INTO dependencies VALUES($1,$2)',[f.qa,unrelated]);
 expect(await reconcileVerificationRevisions(f.inspect)).toEqual([{taskId:f.qa,status:'recheck_queued'}]);
 expect((await pool.query('SELECT execution FROM tasks WHERE id=$1',[f.qa])).rows[0].execution.verificationRecheck.targetRunId).toBe(f.parentRun);
 expect((await pool.query('SELECT state FROM tasks WHERE id=$1',[unrelated])).rows[0].state).toBe('accepted');
});
it('requests review without rechecking when the implementation workspace no longer matches the QA binding',async()=>{
 const f=await fixture();
 await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{workspace}','\"changed/source\"') WHERE id=$1",[f.parent]);
 const result=await reconcileVerificationRevisions(f.inspect);
 expect(result[0]).toMatchObject({taskId:f.qa,status:'review'});
 expect((await pool.query('SELECT execution FROM tasks WHERE id=$1',[f.qa])).rows[0].execution.verificationRecheck).toBeUndefined();
 expect((await pool.query('SELECT evidence FROM supervision_cases WHERE id=$1',[result[0].caseId])).rows[0].evidence[0].code).toBe('VERIFICATION_IMPLEMENTATION_UNAVAILABLE');
 expect((await pool.query('SELECT passed FROM acceptances WHERE run_id=$1',[f.run])).rows[0].passed).toBe(false);
});
it('refuses stale stop evidence and asks supervision once when the same canonical implementation was already rechecked',async()=>{
 const f=await fixture();expect(await reconcileVerificationRevisions(async()=>({...await f.inspect(),checkedAt:'2020-01-01T00:00:00Z'}))).toEqual([]);
 await pool.query("INSERT INTO events(kind,entity_id,payload) VALUES('SUPERVISION_QA_RECHECK_READY',$1,$2)",[f.qa,JSON.stringify({reason:'APPLIED_REVISION_RECONCILED',implementationRunId:f.parentRun})]);
 const result=await reconcileVerificationRevisions(f.inspect);expect(result[0]).toMatchObject({taskId:f.qa,status:'review'});
 expect((await pool.query('SELECT state FROM tasks WHERE id=$1',[f.qa])).rows[0].state).toBe('recovery_blocked');
 expect((await pool.query('SELECT parent_case_id FROM supervision_cases WHERE id=$1',[result[0].caseId])).rows[0].parent_case_id).toBe(f.caseId);
 expect(await reconcileVerificationRevisions(f.inspect)).toEqual([]);
 expect((await pool.query('SELECT count(*)::int n FROM supervision_cases WHERE task_id=$1',[f.qa])).rows[0].n).toBe(2);
});

it.each(['bound','tampered'])('sends a second rejected QA round with an already-consumed recheck to supervision only when its binding is %s',async mode=>{
 const f=await fixture();await newVerificationInput(f);
 expect(await reconcileVerificationRevisions(f.inspect)).toEqual([{taskId:f.qa,status:'recheck_queued'}]);
 const nextRun=randomUUID(),nextReview=randomUUID(),nextCase=randomUUID();
 await pool.query("UPDATE tasks SET attempt_count=2,state='submitted' WHERE id=$1",[f.qa]);
 await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,finished_at) VALUES($1,$2,2,$3,'harness',1,'submitted',now())",[nextRun,f.qa,randomUUID()]);
 await pool.query("INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) VALUES($1,$2,$3,false,$4,'canonical-fixture')",[randomUUID(),f.qa,nextRun,JSON.stringify([{id:'passed',type:'json_field',passed:false}])]);
 const coordinator=(await pool.query('SELECT task_id FROM runs WHERE id=$1',[f.review])).rows[0].task_id;
 await pool.query('UPDATE tasks SET attempt_count=2 WHERE id=$1',[coordinator]);
 await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state,finished_at) VALUES($1,$2,2,$3,'harness',1,'submitted',now())",[nextReview,coordinator,randomUUID()]);
 await pool.query("INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) VALUES($1,$2,$3,true,'[]','canonical-fixture')",[randomUUID(),coordinator,nextReview]);
 const action={caseId:nextCase,kind:'revise',diagnosis:'Original QA still rejects this unchanged candidate.',instruction:'Repair the remaining native requirement; do not claim acceptance or repeat an unchanged candidate.'};
 await artifact(nextReview,'coordination.json',{supervision:{actions:[action]}});
 await pool.query(`INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,state,reason,evidence,decision,decision_run_id,applied_attempt)
  VALUES($1,$2,$3,$4,'applied','Second real QA round','[]',$5,$6,2)`,[nextCase,f.goal,f.qa,nextRun,JSON.stringify({actions:[action]}),nextReview]);
 await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{supervision}',$2::jsonb) WHERE id=$1",[f.qa,JSON.stringify({caseId:nextCase,sourceRunId:nextRun})]);
 await tx(db=>setSupervisionInstruction(db,f.qa,nextCase,'revise',reviseInstruction(action),nextRun));
 const task=(await pool.query('SELECT * FROM tasks WHERE id=$1',[f.qa])).rows[0],run=(await pool.query('SELECT * FROM runs WHERE id=$1',[nextRun])).rows[0],c=(await pool.query('SELECT * FROM supervision_cases WHERE id=$1',[nextCase])).rows[0];
 expect(await currentSemanticDecision(pool,task,run,c)).toBe(true);
 expect(run.instruction_snapshot.task.execution.verificationRecheck.sourceRunId).toBe(f.run);
 if(mode==='tampered')await pool.query("UPDATE tasks SET execution=jsonb_set(execution,'{verificationRecheck,candidateDigest}',$2::jsonb) WHERE id=$1",[f.qa,JSON.stringify('f'.repeat(64))]);
 const inspect=vi.fn(async()=>({runId:nextRun,workspace:task.execution.workspace,checkedAt:new Date().toISOString(),process:{unit:'agentos-run-'+nextRun,stopped:true,mainPid:0,populated:false,activeState:'inactive'}}));
 const result=await reconcileVerificationRevisions(inspect);
 if(mode==='tampered'){expect(result).toEqual([]);expect(inspect).not.toHaveBeenCalled();return;}
 expect(result[0]).toMatchObject({taskId:f.qa,status:'review'});
 expect((await pool.query('SELECT evidence FROM supervision_cases WHERE id=$1',[result[0].caseId])).rows[0].evidence[0].code).toBe('VERIFICATION_SAME_CANDIDATE_REPEATED');
 expect((await pool.query("SELECT count(*)::int n FROM events WHERE entity_id=$1 AND kind='VERIFICATION_RECHECK_QUEUED'",[f.qa])).rows[0].n).toBe(1);
 expect((await pool.query('SELECT passed FROM acceptances WHERE run_id=$1',[nextRun])).rows[0].passed).toBe(false);
});
