import {beforeAll,beforeEach,afterAll,expect,it} from 'vitest';
import {randomUUID} from 'node:crypto';
import {pool,migrate,tx,lock} from '../server/db.js';
import {seedRegistry} from '../server/registry.js';
import {objectiveSchema,validatePlan} from '../server/objective-contracts.js';
import {compileDelivery,deliveryDecisionSchema,verifiedBatch} from '../server/incremental-planning.js';
import {submitObjective,claimPlanning,applyPlannedResult,reconcilePlanning} from '../server/planner.js';
import {refreshGoal} from '../server/service.js';
import {isAttemptLocalWriteScope} from '../server/attempt-local-write-scope.js';
beforeAll(()=>migrate());
beforeEach(async()=>{
 await pool.query('TRUNCATE goals,agents,accounts,memories,events CASCADE');await seedRegistry();
 await pool.query(`UPDATE accounts SET model='test-model',models='["test-model"]',bridge_id='test',max_concurrency=4,allow_unknown=true`);
});
afterAll(()=>pool.end());
const request=()=>objectiveSchema.parse({message:'Qualificar vinte cenários e preservar o restante do objetivo.',project:'test',workspaces:['test/base'],mode:'execute',planningStrategy:'incremental',sourceContractVersion:1,requirements:['Executar vinte cenários com três variantes, contando negativos e pendências.','Publicar a candidata validada preservando os critérios.'],idempotencyKey:randomUUID()});
const reportOnly={version:1 as const,required:[],preserve:[],noSourceReason:'This scope-only controller fixture publishes diagnostic evidence without claiming implementation source.'};
const part=(key:string,dependsOn:string[]=[])=>({key,dependsOn,requirementId:'req-1',title:'Implementar '+key,
 action:'Implementar a contribuição delimitada e registrar resultados reais dos controles.',
 verification:'Executar controles discriminantes preservando o denominador e todos os negativos.',
 agentId:'engineering.backend.node',writePaths:['qualification/'+key],sourceContract:{version:1 as const,
  required:[{path:'qualification/'+key,kind:'directory' as const}],preserve:[]}});
async function accept(id:string){
 const run=randomUUID();
 await pool.query("INSERT INTO runs(id,task_id,attempt,state,fence,runtime,agent_version) VALUES($1,$2,1,'submitted',$3,'harness',1)",[run,id,randomUUID()]);
 await pool.query("UPDATE tasks SET state='accepted',attempt_count=1 WHERE id=$1",[id]);
 await pool.query("INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) VALUES($1,$2,$3,true,'[]','synthetic-controller-test')",[randomUUID(),id,run]);
 await pool.query("INSERT INTO artifacts(id,run_id,name,mime,digest,size) VALUES($1,$2,'candidate.json','application/json',$3,2)",[randomUUID(),run,'a'.repeat(64)]);
}
it('keeps requirement coverage unique while several task identities contribute to it',async()=>{
 const input=request(),plan=compileDelivery({deliveries:[part('fixtures-a'),part('fixtures-b'),part('runner')]},input);
 validatePlan(plan,input,(await pool.query('SELECT * FROM agents')).rows,[]);
 expect(plan.incremental?.selectedRequirementIds).toEqual(['req-1']);
 expect(plan.incremental?.pendingRequirementIds).toEqual(['req-2']);
 expect(plan.tasks.filter(t=>t.stage==='work')).toHaveLength(3);
 expect(plan.tasks.filter(t=>t.stage==='work').every(t=>t.description.includes('contribuição delimitada'))).toBe(true);
 expect(plan.tasks.find(t=>t.stage==='verify')?.description).toContain(input.requirements[0]);
});
it('joins actual prerequisites before their consumer and binds each resulting workspace',async()=>{
 const input=request(),plan=compileDelivery({deliveries:[part('runner',['a','b']),part('a'),part('b'),part('report',['runner'])]},input);
 validatePlan(plan,input,(await pool.query('SELECT * FROM agents')).rows,[]);
 expect(plan.tasks.find(t=>t.key==='join-1')?.dependencies).toEqual(['deliver-2','deliver-3']);
 expect(plan.tasks.find(t=>t.key==='deliver-1')?.sourceTask).toBe('join-1');
 expect(plan.tasks.find(t=>t.key==='deliver-4')?.sourceTask).toBe('deliver-1');
 const {objectiveId}=await submitObjective(input);await applyPlannedResult(await claimPlanning(),plan);
 const rows=(await pool.query('SELECT key,state,execution FROM tasks WHERE goal_id=$1',[objectiveId])).rows;
 expect(rows.find(t=>t.key==='r1-deliver-1').state).toBe('blocked_dependencies');
 expect(rows.find(t=>t.key==='r1-deliver-1').execution.sourceWorkspace).toContain('r1-join-1');
 expect(rows.find(t=>t.key==='r1-deliver-4').execution.sourceWorkspace).toContain('r1-deliver-1');
});
it('rejects duplicate keys, cycles, unknown dependencies, QA as implementation and ineligible requirements with specific causes',()=>{
 const input=request();
 const bad=(deliveries:unknown[],violation:string)=>expect(()=>compileDelivery({deliveries},input)).toThrowError(expect.objectContaining({detail:expect.objectContaining({violation})}));
 bad([part('a'),part('a')],'DUPLICATE_TASK_KEY');
 bad([part('a',['b']),part('b',['a'])],'DEPENDENCY_CYCLE');
 bad([part('a',['missing'])],'UNKNOWN_DEPENDENCY');
 bad([{...part('a'),agentId:'qa.integration'}],'VALIDATOR_AS_WORKER');
 bad([{...part('a'),requirementId:'invented'}],'INELIGIBLE_REQUIREMENT');
 bad([part('a'),{...part('b'),writePaths:['qualification/a/nested'],sourceContract:reportOnly}],'WRITE_SCOPE_CONFLICT');
 bad([part('a',['b','b']),part('b')],'DUPLICATE_DEPENDENCY');
 expect(()=>compileDelivery({...part('a'),deliveries:[part('b')]},input)).toThrowError(expect.objectContaining({detail:expect.objectContaining({violation:'AMBIGUOUS_DECISION'})}));
 expect(()=>compileDelivery({deliveries:[part('a'),{...part('b'),writePaths:['qualification/./a']}]},input)).toThrow();
 expect(()=>compileDelivery({deliveries:[part('a'),{...part('b'),writePaths:['qualification//a']}]},input)).toThrow();
});
it('enforces write isolation for a single requirement even when the raw plan is tampered with',async()=>{
 const input=request(),plan=compileDelivery({deliveries:[part('a'),part('b')]},input);
 plan.tasks[1].writePaths=plan.tasks[0].writePaths;
 expect(()=>validatePlan(plan,input,[],[])).toThrow();
 const agents=(await pool.query('SELECT * FROM agents')).rows;
 expect(()=>validatePlan(plan,input,agents,[])).toThrowError(expect.objectContaining({detail:expect.objectContaining({violation:'SOURCE_OUTPUT_OUTSIDE_WRITE_SCOPE'})}));
 plan.tasks[1].sourceContract=reportOnly;
 expect(()=>validatePlan(plan,input,agents,[])).toThrowError(/disjuntos/);
});
it('keeps identically named local outputs in different workspaces without a false source conflict',async()=>{
 const input=request(),plan=compileDelivery({deliveries:[
  {...part('a'),writePaths:['qualification/a','evidence/req-4','data/local']},
  {...part('b'),writePaths:['qualification/b','evidence/req-4/qa-laudo.md','data/local']} ]},input);
 validatePlan(plan,input,(await pool.query('SELECT * FROM agents')).rows,[]);
 const {objectiveId}=await submitObjective(input);await applyPlannedResult(await claimPlanning(),plan);
 const workers=(await pool.query("SELECT execution FROM tasks WHERE goal_id=$1 AND stage='work'",[objectiveId])).rows;
 expect(workers).toHaveLength(2);
 expect(new Set(workers.map(w=>w.execution.workspace)).size).toBe(2);
 expect(new Set(workers.map(w=>w.execution.sourceWorkspace)).size).toBe(1);
 expect(workers.every(w=>w.execution.sourceWorkspace!==w.execution.workspace)).toBe(true);
});
it('allows explicit local-output-only contributions while keeping final integration and independent QA',async()=>{
 const input=request(),plan=compileDelivery({deliveries:[
  {...part('a'),writePaths:['evidence/review'],sourceContract:reportOnly},{...part('b'),writePaths:['evidence/review','data/scratch'],sourceContract:reportOnly} ]},input);
 validatePlan(plan,input,(await pool.query('SELECT * FROM agents')).rows,[]);
 expect(plan.tasks.filter(t=>t.stage==='work')).toHaveLength(2);
 expect(plan.tasks.find(t=>t.stage==='integrate')?.dependencies).toHaveLength(2);
 expect(plan.tasks.find(t=>t.stage==='verify')?.sourceTask).toBe('integrate');
});
it('never treats source folders with similar names, aliases or forbidden paths as local-output exemptions',async()=>{
 for(const value of ['src/evidence','src/data','database','evidence-source','evidence/../server','evidence/./server','evidence//x','/data','data\\x','data/.git/config','.'])
  expect(isAttemptLocalWriteScope(value)).toBe(false);
 const input=request();
 for(const source of ['server/shared','src/evidence','src/data','database','evidence-source'])
  expect(()=>compileDelivery({deliveries:[{...part('a'),writePaths:[source,'evidence/x'],sourceContract:reportOnly},
   {...part('b'),writePaths:[source+'/child','evidence/x'],sourceContract:reportOnly} ]},input)).toThrowError(expect.objectContaining({detail:expect.objectContaining({violation:'WRITE_SCOPE_CONFLICT'})}));
 for(const value of ['evidence/../server','evidence/./x','evidence//x'])
  expect(()=>compileDelivery({deliveries:[part('a'),{...part('b'),writePaths:[value]}]},input)).toThrow();
 const forbidden=compileDelivery({deliveries:[part('a'),{...part('b'),writePaths:['data/.git/config'],sourceContract:reportOnly}]},input);
 const agents=(await pool.query('SELECT * FROM agents')).rows;
 expect(()=>validatePlan(forbidden,input,agents,[])).toThrowError(expect.objectContaining({detail:expect.objectContaining({writePath:'data/.git/config'})}));
});
it('does not advance on partial accepts or the join candidate; requires final integration and QA of the whole batch',async()=>{
 const input=request(),plan=compileDelivery({deliveries:[part('a'),part('b'),part('runner',['a','b'])]},input);
 const {objectiveId}=await submitObjective(input);await applyPlannedResult(await claimPlanning(),plan);
 const tasks=(await pool.query('SELECT id,key,stage FROM tasks WHERE goal_id=$1 ORDER BY key',[objectiveId])).rows;
 for(const task of tasks.filter(t=>t.stage==='work'||t.key==='r1-join-3'))await accept(task.id);
 expect(await verifiedBatch(pool,objectiveId,1)).toBeNull();
 await reconcilePlanning();expect(await claimPlanning()).toBeNull();
 for(const task of tasks.filter(t=>t.key==='r1-integrate'||t.stage==='verify'))await accept(task.id);
 const proof=await verifiedBatch(pool,objectiveId,1);
 expect(proof?.sourceWorkspace).toContain('r1-integrate');expect(proof?.sourceWorkspace).not.toContain('join');
 await tx(async db=>{await lock(db);await refreshGoal(db,objectiveId);});
 expect((await pool.query('SELECT state FROM goals WHERE id=$1',[objectiveId])).rows[0].state).toBe('active');
 await reconcilePlanning();const next=await claimPlanning();
 expect(next.feedback.incrementalAdvance).toEqual(proof);
 expect(compileDelivery({...part('publish'),requirementId:'req-2',dependsOn:[]},input,plan,true).incremental?.completedRequirementIds).toEqual(['req-1']);
});
it('advertises only eligible requirement IDs and supports a serial supervisor fallback without a deliveries array',()=>{
 const agents=[{id:'engineering.backend.node',runtime:'harness'},{id:'qa.integration',runtime:'harness'}];
 const schema=deliveryDecisionSchema(agents,['req-2'],true);
 expect(schema.safeParse({deliveries:[part('a')]}).success).toBe(false);
 expect(schema.safeParse(part('a')).success).toBe(false);
 expect(schema.safeParse({...part('a'),kind:'delivery',requirementId:'req-2'}).success).toBe(true);
});
it('preserves every accepted partial branch as input while keeping a join out of the accumulated source after a consumer failure',async()=>{
 const input=request();const {objectiveId}=await submitObjective(input);
 const first=compileDelivery({...part('first'),requirementId:'req-1'},input);
 await applyPlannedResult(await claimPlanning(),first);
 for(const t of (await pool.query('SELECT id FROM tasks WHERE goal_id=$1',[objectiveId])).rows)await accept(t.id);
 await reconcilePlanning();const advancing=await claimPlanning();
 const second=compileDelivery({deliveries:['a','b','consumer'].map((key,i)=>({...part(key,i===2?['a','b']:[]),requirementId:'req-2'}))},input,first,true);
 await applyPlannedResult(advancing,second);
 const tasks=(await pool.query('SELECT id,key FROM tasks WHERE goal_id=$1 AND plan_revision=2',[objectiveId])).rows;
 for(const t of tasks.filter(t=>['r2-deliver-1','r2-deliver-2','r2-join-3'].includes(t.key)))await accept(t.id);
 const consumer=tasks.find(t=>t.key==='r2-deliver-3'),failed=randomUUID();
 await pool.query("INSERT INTO runs(id,task_id,attempt,state,fence,runtime,agent_version,summary) VALUES($1,$2,1,'failed',$3,'harness',1,'Consumer requires a narrower repair')",[failed,consumer.id,randomUUID()]);
 await pool.query("UPDATE tasks SET state='recovery_blocked',attempt_count=1 WHERE id=$1",[consumer.id]);
 await reconcilePlanning();const repair=await claimPlanning();
 expect(repair.feedback.sourceOverrides['test/base']).toContain('r1-deliver');
 expect(repair.feedback.sourceOverrides['test/base']).not.toContain('join');
 expect(repair.feedback.partialDeliveries.map((p:any)=>p.key).sort()).toEqual(['r2-deliver-1','r2-deliver-2','r2-join-3']);
 expect(repair.feedback.artifacts).toHaveLength(3);
 expect(repair.feedback.reports.join('\n')).toContain('Reuse their canonical patches');
});
