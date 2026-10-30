import {beforeAll,beforeEach,afterEach,afterAll,expect,it,vi} from 'vitest';
import {randomUUID} from 'node:crypto';
import {readModelPolicy} from '../bridge/model-policy.mjs';
import {pool,migrate,tx,event} from '../server/db.js';
import {seedRegistry} from '../server/registry.js';
import {objectiveSchema} from '../server/objective-contracts.js';
import {compileDelivery} from '../server/incremental-planning.js';
import {submitObjective,claimPlanning,applyPlannedResult,reconcilePlanning,PlanningEngine,feedbackObjective} from '../server/planner.js';
import {accountBridge} from '../server/bridge.js';
import {config} from '../server/config.js';
import {planningContractVersion} from '../server/planning-supervision.js';
import {planningPreparationFixture} from './fixtures/planning-preparation.js';
vi.mock('../bridge/model-policy.mjs',async original=>({...await original<any>(),readModelPolicy:vi.fn()}));
const policy={version:1,mode:'glm-only',accountId:'agentos-api-00000000-0000-4000-8000-000000000001',bridgeId:'agentos:api:00000000-0000-4000-8000-000000000001',model:'glm-5.3-flash',requestLimit:6,sessionLimit:12} as const;
let f:any;
let preparation:ReturnType<typeof planningPreparationFixture>;
const item=(id='req-1')=>({kind:'delivery' as const,requirementId:id,title:'Implementar e comprovar entrega',agentId:'engineering.backend.node',action:'Implementar integralmente o requisito preservando os controles e a candidata aceita.',verification:'Executar os controles originais e todos os negativos na mesma candidata acumulada.',
 sourceContract:{version:1 as const,required:[],preserve:[],noSourceReason:'Synthetic planner-recovery fixture exercises controller transitions without implementation source.'}});
beforeAll(()=>migrate());
beforeEach(async()=>{
 preparation=planningPreparationFixture();
 vi.mocked(readModelPolicy).mockReturnValue(policy);
 await pool.query('TRUNCATE goals,agents,accounts,memories,events CASCADE');await seedRegistry();
 await pool.query("UPDATE accounts SET enabled=false");
 await pool.query(`INSERT INTO accounts(id,label,provider,model,models,bridge_id,enabled,allow_unknown,max_concurrency) VALUES($1,'Synthetic GLM','openai',$2,$3,$4,true,true,12)`,[policy.accountId,policy.model,JSON.stringify([policy.model]),policy.bridgeId]);
 config.bridgeHost='fixture';config.remoteExecution=true;
 const input=objectiveSchema.parse({message:'Qualificar a recuperação do planner sem repetir trabalho aceito.',project:'test',workspaces:['test/base'],mode:'execute',sourceContractVersion:1,requirements:['Corrigir e verificar o primeiro módulo.','Qualificar o segundo módulo integralmente.'],idempotencyKey:randomUUID()});
 const {objectiveId:goal}=await submitObjective(input),initial=await claimPlanning();
 await applyPlannedResult(initial,compileDelivery(item(),input));
 await acceptAll(goal,1);await reconcilePlanning();const advance=await claimPlanning();
 await pool.query("UPDATE planning_runs SET state='failed',output=NULL,error='Subtarefas do mesmo requisito rejeitadas.',recovery_counts='{"+ '"invalid_plan":3'+"}',finished_at=now() WHERE id=$1",[advance.id]);
 await tx(db=>event(db,'PLANNING_BLOCKED',goal,{planningRunId:advance.id,code:'PLAN_INVALID',recovery:{category:'invalid_plan',count:3}}));
 await pool.query("UPDATE objectives SET status='waiting_external' WHERE id=$1",[goal]);
 await pool.query("INSERT INTO settings VALUES($1,'{\"mode\":\"supervised\"}')",['objective-review:'+goal]);
 f={goal,input,advance};
});
afterEach(()=>{vi.restoreAllMocks();config.bridgeHost='';config.remoteExecution=false;});
afterAll(()=>pool.end());
async function acceptAll(goal:string,revision:number){
 for(const t of (await pool.query('SELECT id,execution FROM tasks WHERE goal_id=$1 AND plan_revision=$2',[goal,revision])).rows){
  const run=randomUUID(),fence=randomUUID(),contextId=randomUUID();
  await pool.query("INSERT INTO runs(id,task_id,attempt,state,fence,runtime,agent_version) VALUES($1,$2,1,'submitted',$3,'harness',1)",[run,t.id,fence]);
  // These synthetic acceptances are controller fixtures, but their source owner
  // still needs the real frozen identity contract before preparing a new lot.
  await pool.query('INSERT INTO contexts(id,run_id,manifest,content) VALUES($1,$2,$3,$4)',[contextId,run,
    JSON.stringify({runIdentity:{goalId:goal,taskId:t.id,runId:run,runFence:fence},dependencyArtifacts:[]}),'Synthetic accepted source context']);
  await pool.query('UPDATE runs SET context_id=$2 WHERE id=$1',[run,contextId]);
  preparation.registerSource({goalId:goal,taskId:t.id,runId:run,fence,contextId,workspace:t.execution.workspace,sourceWorkspace:t.execution.sourceWorkspace});
  await pool.query("UPDATE tasks SET state='accepted',attempt_count=1 WHERE id=$1",[t.id]);
  await pool.query("INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) VALUES($1,$2,$3,true,'[]','synthetic-controller-test')",[randomUUID(),t.id,run]);
  await pool.query("INSERT INTO artifacts(id,run_id,name,mime,digest,size) VALUES($1,$2,'candidate.json','application/json',$3,2)",[randomUUID(),run,'a'.repeat(64)]);
 }
}
function bridge(generate:(params:any)=>Promise<any>){
 return vi.spyOn(accountBridge,'request').mockImplementation(async(method,p:any)=>{
  if(method==='memory.search')return {items:[]};if(method==='worker.workspaces')return [{id:'test/base'}];
  const inspection=preparation(method,p);if(inspection!==undefined)return inspection;
  if(method==='generate'){expect(p.accountId).toBe(policy.bridgeId);expect(p.model).toBe(policy.model);return generate(p);}
  throw Error('Unexpected RPC: '+method);
 });
}
const response=(value:any)=>({message:{content:[{type:'text',text:JSON.stringify({...value,
 requirementContracts:['req-1','req-2'].map(id=>({id,observables:[{id:'result',name:id+'-result.json',authority:'current_run',type:'artifact',description:'Synthetic supervisor fixture product evidence'}]}))})}]}});
it('reconciles the existing failed identity once, retaining attempts, source/QA proof and accepted tasks',async()=>{
 const before=(await pool.query('SELECT id,state,execution,criteria FROM tasks WHERE goal_id=$1 ORDER BY id',[f.goal])).rows;
 await reconcilePlanning();await reconcilePlanning();
 const row=(await pool.query('SELECT * FROM planning_runs WHERE id=$1',[f.advance.id])).rows[0];
 expect(row.state).toBe('queued');expect(row.attempt).toBe(f.advance.attempt);expect(row.fence).not.toBe(f.advance.fence);
 expect(row.feedback.incrementalAdvance).toEqual(f.advance.feedback.incrementalAdvance);
 expect(row.feedback.sourceOverrides).toEqual(f.advance.feedback.sourceOverrides);
 expect(row.feedback.planningRecovery.contractVersion).toBe(planningContractVersion);
 expect((await pool.query('SELECT id,state,execution,criteria FROM tasks WHERE goal_id=$1 ORDER BY id',[f.goal])).rows).toEqual(before);
 expect((await pool.query("SELECT 1 FROM events WHERE kind='PLANNING_REVIEW_QUEUED' AND entity_id=$1",[f.goal])).rowCount).toBe(1);
});
it('lets the GLM supervisor apply a corrected multi-task proposal, then verifies effect only after independent batch acceptance',async()=>{
 await reconcilePlanning();
 bridge(async p=>{
  const message=JSON.parse(p.messages[0].content);
  // A v1 subdelivery remains accepted, but cannot manufacture a v2 outcome.
  expect(message.eligible).toEqual(['req-1','req-2']);expect(message.completed).toEqual([]);
  expect(message.completionMigration.requirementCompletion).toBe('unproven_without_v2_outcomes');
  expect(message.planningSupervision.mode).toBe('supervised');
  return response({deliveries:[{...item('req-2'),key:'a',writePaths:['src/a.ts']},{...item('req-2'),key:'b',dependsOn:['a'],writePaths:['src/b.ts']}]});
 });
 const engine=new PlanningEngine();await engine.execute(await claimPlanning(),new AbortController().signal);
 expect((await pool.query('SELECT revision FROM objectives WHERE id=$1',[f.goal])).rows[0].revision).toBe(2);
 expect((await pool.query("SELECT 1 FROM events WHERE kind='PLANNING_REVIEW_APPLIED' AND entity_id=$1",[f.goal])).rowCount).toBe(1);
 expect((await pool.query("SELECT 1 FROM events WHERE kind='PLANNING_REVIEW_EFFECT_VERIFIED' AND entity_id=$1",[f.goal])).rowCount).toBe(0);
 await acceptAll(f.goal,2);await reconcilePlanning();await reconcilePlanning();
 expect((await pool.query("SELECT 1 FROM events WHERE kind='PLANNING_REVIEW_EFFECT_VERIFIED' AND entity_id=$1",[f.goal])).rowCount).toBe(1);
});
it.each(['pause','cancel','workforce','observe','uncertain','changed-proof','newer-plan'] as const)('does not reopen %s',async kind=>{
 if(kind==='pause'||kind==='cancel')await pool.query('UPDATE goals SET state=$2 WHERE id=$1',[f.goal,kind==='pause'?'paused':'cancelled']);
 if(kind==='workforce')await pool.query("INSERT INTO settings VALUES($1,'{\"enabled\":true}')",['workforce:'+f.goal]);
 if(kind==='observe')await pool.query("UPDATE settings SET value='{\"mode\":\"observe\"}' WHERE id=$1",['objective-review:'+f.goal]);
 if(kind==='uncertain')await tx(db=>event(db,'PLANNING_BLOCKED',f.goal,{planningRunId:f.advance.id,code:'STOP_UNCONFIRMED',recovery:{category:'external'}}));
 if(kind==='changed-proof')await pool.query("UPDATE artifacts SET digest=$1 WHERE id=$2",['b'.repeat(64),f.advance.feedback.incrementalAdvance.candidateArtifactId]);
 if(kind==='newer-plan')await pool.query("INSERT INTO planning_runs(id,objective_id,base_revision,trigger_key,fence,state) VALUES($1,$2,1,'newer',gen_random_uuid(),'failed')",[randomUUID(),f.goal]);
 await reconcilePlanning();expect((await pool.query('SELECT state FROM planning_runs WHERE id=$1',[f.advance.id])).rows[0].state).toBe('failed');
});
it('escalates one invalid supervised proposal to a complete serial fallback, without another queue or acceptance shortcut',async()=>{
 await reconcilePlanning();let calls=0;
 bridge(async p=>{
  const message=JSON.parse(p.messages[0].content);
  if(++calls===1)return response({...item('invented')});
  expect(message.planningSupervision.mode).toBe('serial');expect(message.schema.properties).not.toHaveProperty('deliveries');
  return response(item('req-2'));
 });
 const engine=new PlanningEngine();await engine.execute(await claimPlanning(),new AbortController().signal);
 expect((await pool.query('SELECT feedback FROM planning_runs WHERE id=$1',[f.advance.id])).rows[0].feedback.planningRecovery.mode).toBe('serial');
 await pool.query('UPDATE planning_runs SET next_attempt_at=now() WHERE id=$1',[f.advance.id]);
 await engine.execute(await claimPlanning(),new AbortController().signal);
 expect(calls).toBe(2);expect((await pool.query('SELECT state FROM planning_runs WHERE id=$1',[f.advance.id])).rows[0].state).toBe('completed');
 expect((await pool.query('SELECT state FROM tasks WHERE goal_id=$1 AND plan_revision=2',[f.goal])).rows.every(t=>t.state!=='accepted')).toBe(true);
});
it('does not loop indefinitely after the same contract also rejects a serial proposal',async()=>{
 await reconcilePlanning();bridge(async()=>response(item('invented')));const engine=new PlanningEngine();
 for(let i=0;i<2;i++){await engine.execute(await claimPlanning(),new AbortController().signal);await pool.query('UPDATE planning_runs SET next_attempt_at=now() WHERE id=$1',[f.advance.id]);}
 await reconcilePlanning();expect((await pool.query('SELECT state FROM planning_runs WHERE id=$1',[f.advance.id])).rows[0].state).toBe('failed');
 expect(await claimPlanning()).toBeNull();
});
it('discards a supervisory result after an explicit pause without changing the accepted candidate',async()=>{
 await reconcilePlanning();bridge(async()=>{await pool.query("UPDATE goals SET state='paused' WHERE id=$1",[f.goal]);return response(item('req-2'));});
 await new PlanningEngine().execute(await claimPlanning(),new AbortController().signal);
 expect((await pool.query('SELECT state FROM goals WHERE id=$1',[f.goal])).rows[0].state).toBe('paused');
 expect((await pool.query('SELECT revision FROM objectives WHERE id=$1',[f.goal])).rows[0].revision).toBe(1);
});
it('does not substitute another model while the authorized GLM account requires authentication',async()=>{
 await reconcilePlanning();await pool.query("UPDATE accounts SET quota_status='auth_required' WHERE id=$1",[policy.accountId]);
 const rpc=bridge(async()=>{throw Error('No provider call is permitted');});
 expect(await claimPlanning()).toBeNull();expect(rpc).not.toHaveBeenCalled();
 expect((await pool.query('SELECT state FROM planning_runs WHERE id=$1',[f.advance.id])).rows[0].state).toBe('waiting_quota');
});
it('discards a late result if the operator disables supervised recovery',async()=>{
 await reconcilePlanning();bridge(async()=>{await pool.query("UPDATE settings SET value='{\"mode\":\"off\"}' WHERE id=$1",['objective-review:'+f.goal]);return response(item('req-2'));});
 await new PlanningEngine().execute(await claimPlanning(),new AbortController().signal);
 expect((await pool.query('SELECT state FROM planning_runs WHERE id=$1',[f.advance.id])).rows[0].state).toBe('failed');
 expect((await pool.query('SELECT revision FROM objectives WHERE id=$1',[f.goal])).rows[0].revision).toBe(1);
});
it('routes newly arrived counterevidence to a fresh plan instead of reviving an obsolete verified advance',async()=>{
 await feedbackObjective(f.goal,'older-negative-bound-to-failed-plan','Older external counterexample still needs verification.');
 const old=(await pool.query("SELECT id FROM objective_feedback WHERE objective_id=$1 AND trigger_key='older-negative-bound-to-failed-plan'",[f.goal])).rows[0];
 await pool.query('UPDATE planning_runs SET feedback=feedback||$2::jsonb WHERE id=$1',[f.advance.id,JSON.stringify({feedbackIds:[old.id],reports:['Older external counterexample still needs verification.']})]);
 await feedbackObjective(f.goal,'new-negative-after-planner-failure','New external counterexample invalidates the proposed advance; preserve it and repair before qualification.');
 await reconcilePlanning();
 expect((await pool.query('SELECT state FROM planning_runs WHERE id=$1',[f.advance.id])).rows[0].state).toBe('failed');
 const fresh=await claimPlanning();expect(fresh.id).not.toBe(f.advance.id);
 expect(fresh.feedback.reports.join('\n')).toContain('New external counterexample');
 expect(fresh.feedback.reports.join('\n')).toContain('Older external counterexample');
 expect(fresh.feedback.incrementalAdvance).toBeUndefined();
});
it('rejects a supervisory result if new counterevidence arrived during generation, without retrying the stale context',async()=>{
 await reconcilePlanning();bridge(async()=>{
  await feedbackObjective(f.goal,'late-negative-during-review','Late external counterexample must be reproduced before advancing this candidate.');
  return response(item('req-2'));
 });
 await new PlanningEngine().execute(await claimPlanning(),new AbortController().signal);
 const stale=(await pool.query('SELECT state,error FROM planning_runs WHERE id=$1',[f.advance.id])).rows[0];
 expect(stale.state).toBe('failed');expect(stale.error).toContain('Nova evidência');
 expect((await pool.query('SELECT revision FROM objectives WHERE id=$1',[f.goal])).rows[0].revision).toBe(1);
 await reconcilePlanning();const fresh=await claimPlanning();expect(fresh.id).not.toBe(f.advance.id);
 expect(fresh.feedback.reports.join('\n')).toContain('Late external counterexample');
});
