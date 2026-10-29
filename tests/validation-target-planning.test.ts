import {beforeAll,beforeEach,afterEach,afterAll,expect,it,vi} from 'vitest';
import {randomUUID} from 'node:crypto';
import {z} from 'zod';
import {pool,migrate} from '../server/db.js';
import {seedRegistry} from '../server/registry.js';
import {objectiveSchema,validatePlan,planSchema} from '../server/objective-contracts.js';
import {compileDelivery,deliveryDecisionSchema,verifiedBatch} from '../server/incremental-planning.js';
import {submitObjective,claimPlanning,applyPlannedResult} from '../server/planner.js';
import {schedule} from '../server/service.js';
import {executeRuntime} from '../server/runtime.js';
import {accountBridge} from '../server/bridge.js';
import {config} from '../server/config.js';

beforeAll(()=>migrate());
beforeEach(async()=>{
  config.remoteExecution=false;config.bridgeHost='';config.dshCommand='';
  await pool.query('TRUNCATE goals,agents,accounts,memories,events CASCADE');
  await seedRegistry();
  await pool.query(`UPDATE accounts SET model='test-model',models='["test-model"]',bridge_id='test',max_concurrency=4,allow_unknown=true`);
});
afterEach(()=>{vi.restoreAllMocks();config.remoteExecution=false;});
afterAll(()=>pool.end());

const request=()=>objectiveSchema.parse({message:'Produzir duas contribuições e validar a candidata certa.',project:'test',
  workspaces:['test/base'],mode:'execute',planningStrategy:'incremental',requirements:['Preservar os controles ao implementar a transformação.','Publicar o resultado final verificado.'],idempotencyKey:randomUUID()});
const delivery=(key:string,dependsOn:string[]=[])=>({kind:'delivery' as const,key,dependsOn,requirementId:'req-1',
  title:'Entregar '+key,action:'Produzir a contribuição delimitada preservando todos os controles existentes.',
  verification:'Executar os controles discriminantes e registrar comandos, resultados e limitações.',
  agentId:'engineering.backend.node',writePaths:['src/'+key]});
const validation=(key='check',target='producer',dependsOn:string[]=[])=>({...delivery(key,dependsOn),
  kind:'validation' as const,validationTargetKey:target,agentId:'qa.integration',writePaths:['evidence/check']});
const compile=(items:unknown[])=>compileDelivery({deliveries:items},request());
async function persist(items:unknown[]){
  const input=request(),plan=compileDelivery({deliveries:items},input);
  const agents=(await pool.query('SELECT * FROM agents')).rows;
  validatePlan(plan,input,agents,[]);
  const {objectiveId}=await submitObjective(input);
  await applyPlannedResult(await claimPlanning(),plan);
  return {objectiveId,plan,tasks:(await pool.query('SELECT * FROM tasks WHERE goal_id=$1 ORDER BY key',[objectiveId])).rows};
}
// Synthetic controller state only; these receipts are not live qualification.
async function accept(taskId:string){
  const runId=randomUUID();
  await pool.query("INSERT INTO runs(id,task_id,attempt,state,fence,runtime,agent_version) VALUES($1,$2,1,'submitted',$3,'harness',1)",[runId,taskId,randomUUID()]);
  await pool.query("UPDATE tasks SET state='accepted',attempt_count=1 WHERE id=$1",[taskId]);
  await pool.query("INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer) VALUES($1,$2,$3,true,'[]','synthetic-controller-test')",[randomUUID(),taskId,runId]);
  await pool.query("INSERT INTO artifacts(id,run_id,name,mime,digest,size) VALUES($1,$2,'candidate.json','application/json',$3,2)",[randomUUID(),runId,'a'.repeat(64)]);
  return runId;
}

it('requires semantic kind in every new generation while retaining legacy compiled plans',()=>{
  const agents=[{id:'engineering.backend.node',runtime:'harness'},{id:'qa.integration',runtime:'harness'}];
  const {kind,...legacy}=delivery('producer');
  for(const serial of [false,true]){
    const schema=deliveryDecisionSchema(agents,['req-1'],serial);
    expect(schema.safeParse(legacy).success).toBe(false);
    expect(schema.safeParse(delivery('producer')).success).toBe(true);
    expect(()=>JSON.stringify(z.toJSONSchema(schema))).not.toThrow();
  }
  const schema=deliveryDecisionSchema(agents,['req-1']);
  expect(schema.safeParse({deliveries:[legacy,validation()]}).success).toBe(false);
  expect(schema.safeParse({deliveries:[delivery('producer'),validation()]}).success).toBe(true);
  expect(schema.safeParse(validation()).success).toBe(false);
  const stored=compileDelivery(legacy,request());
  expect(stored.tasks.map(task=>task.stage)).toEqual(['work','verify']);
  expect(planSchema.parse(stored)).toEqual(stored);
});

it('counterexample: an untyped baseline QA is no longer generated as parallel writable work',async()=>{
  const {kind:ignored,...producer}=delivery('producer');
  const {kind:ignoredAgain,validationTargetKey:ignoredTarget,...oldCheck}=validation();
  const legacy=compile([producer,{...oldCheck,agentId:'qa.acceptance'}]);
  expect(legacy.tasks.find(task=>task.key==='deliver-2')).toMatchObject({stage:'work',dependencies:[]});
  expect(legacy.tasks.find(task=>task.key==='deliver-2')?.sourceTask).toBeUndefined();
  const {tasks}=await persist([delivery('producer'),validation()]);
  const worker=tasks.find(task=>task.key==='r1-deliver-1'),check=tasks.find(task=>task.key==='r1-validate-2');
  expect(worker.state).toBe('ready');
  expect(check).toMatchObject({state:'blocked_dependencies',stage:'verify',agent_id:'qa.integration',agent_role:'validator'});
  expect(check.execution).toMatchObject({sourceWorkspace:worker.execution.workspace,readOnlySource:true});
  expect(check.execution.sourceWorkspace).not.toBe(worker.execution.sourceWorkspace);
  expect(check.criteria).toEqual([{id:'verified',label:'Validação independente da candidata alvo',type:'json_field',name:'verification.json',field:'passed',equals:true}]);
  await schedule();
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1',[check.id])).rows[0].state).toBe('blocked_dependencies');
  expect((await pool.query('SELECT 1 FROM runs WHERE task_id=$1',[check.id])).rowCount).toBe(0);
  await accept(worker.id);
  await schedule();
  // The actual scheduler has released the dependency gate, but this fixture
  // deliberately has no configured executor and cannot launch a remote actor.
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1',[check.id])).rows[0].state).toBe('waiting_configuration');
});

it('forwards the declared producer workspace to prepare and keeps source read-only at execution',async()=>{
  const {tasks}=await persist([delivery('producer'),validation()]);
  const producer=tasks.find(task=>task.key==='r1-deliver-1'),check=tasks.find(task=>task.key==='r1-validate-2');
  const bridge=vi.spyOn(accountBridge,'request').mockResolvedValue({summary:'Synthetic runtime routing check',artifacts:[],evidence:{stopped:true}});
  config.remoteExecution=true;
  await executeRuntime({task:check,run:{id:randomUUID(),task_id:check.id,fence:randomUUID()},
    account:{bridge_id:'fixture',model:'test-model'},context:{content:'',manifest:{selected:[],dependencyArtifacts:[],executionInputs:[]}},
    signal:new AbortController().signal,onEvent:vi.fn()});
  expect(bridge.mock.calls[0]).toEqual(['worker.prepare',
    {sourceWorkspace:producer.execution.workspace,workspace:check.execution.workspace},expect.anything()]);
  expect(bridge.mock.calls[1][0]).toBe('worker.execute');
  expect(bridge.mock.calls[1][1]).toMatchObject({workspace:check.execution.workspace,readOnlySource:true,
    deliveryContract:{version:1,artifacts:[{name:'verification.json',path:'evidence/verification.json',mime:'application/json'}]}});
});

it('keeps deliveries parallel and extra evidence dependencies cannot change the validation target',async()=>{
  const {plan,tasks}=await persist([validation('check','producer',['reference']),delivery('producer'),delivery('reference')]);
  const check=plan.tasks.find(task=>task.key==='validate-1')!;
  expect(check.sourceTask).toBe('deliver-2');
  expect(check.dependencies).toEqual(['deliver-3','deliver-2']);
  expect(plan.tasks.some(task=>task.key.startsWith('join-'))).toBe(false);
  expect(tasks.filter(task=>task.stage==='work').map(task=>task.state)).toEqual(['ready','ready']);
  expect(plan.tasks.find(task=>task.key==='integrate')?.dependencies).toEqual(['deliver-3','deliver-2']);
  expect(plan.tasks.find(task=>task.key==='integrate')?.writePaths).toEqual(['src/producer','src/reference']);
  expect(plan.tasks.find(task=>task.key==='verify')).toMatchObject({sourceTask:'integrate',dependencies:['integrate','validate-1']});
});

it('validation gates never become source producers or join patch inputs for downstream delivery',async()=>{
  const {plan}=await persist([delivery('producer'),delivery('reference'),validation('check','producer'),
    delivery('consumer',['check','reference'])]);
  expect(plan.tasks.find(task=>task.key==='join-4')?.dependencies).toEqual(['deliver-1','deliver-2']);
  expect(plan.tasks.find(task=>task.key==='deliver-4')).toMatchObject({sourceTask:'join-4',dependencies:['join-4','validate-3']});
  const one=compile([delivery('producer'),validation(),delivery('consumer',['check'])]);
  expect(one.tasks.find(task=>task.key==='deliver-3')).toMatchObject({sourceTask:'deliver-1',dependencies:['deliver-1','validate-2']});
  const evidenceOnly=compile([delivery('producer'),delivery('reference'),delivery('other'),
    validation('check','producer',['reference']),delivery('consumer',['check','other'])]);
  expect(evidenceOnly.tasks.find(task=>task.key==='join-5')?.dependencies).toEqual(['deliver-1','deliver-3']);
});

it('rejects absent, unknown, validation targets, cycles, mismatched requirements and source writes',()=>{
  const bad=(items:unknown[],violation:string)=>expect(()=>compile(items)).toThrowError(expect.objectContaining({detail:expect.objectContaining({violation})}));
  bad([delivery('producer'),{...validation(),validationTargetKey:undefined}],'VALIDATION_TARGET_REQUIRED');
  bad([delivery('producer'),validation('check','missing')],'VALIDATION_TARGET_REQUIRED');
  bad([delivery('producer'),validation(),validation('other','check')],'VALIDATION_TARGET_REQUIRED');
  bad([validation()],'VALIDATION_TARGET_REQUIRED');
  bad([delivery('producer',['check']),validation()],'DEPENDENCY_CYCLE');
  bad([delivery('producer'),validation('check','producer',['check'])],'DEPENDENCY_CYCLE');
  bad([delivery('producer'),{...validation(),requirementId:'req-2'}],'VALIDATION_REQUIREMENT_MISMATCH');
  bad([delivery('producer'),{...validation(),writePaths:['src/producer']}],'VALIDATION_SOURCE_WRITE');
  bad([{...delivery('producer'),validationTargetKey:'other'},delivery('other')],'UNEXPECTED_VALIDATION_TARGET');
});

it('allows research products without requiring source changes or granting validation writes',async()=>{
  const {plan}=await persist([{...delivery('producer'),writePaths:undefined}, {...validation(),writePaths:['evidence','data']}]);
  expect(plan.tasks.find(task=>task.stage==='work')?.writePaths).toEqual([]);
  expect(plan.tasks.find(task=>task.key==='integrate')?.writePaths).toEqual([]);
  expect(plan.tasks.find(task=>task.key==='validate-2')?.writePaths).toEqual(['evidence','data']);
});

it('advances only through the final DAG sink after every intermediate acceptance',async()=>{
  const {objectiveId,tasks}=await persist([delivery('producer'),delivery('reference'),validation()]);
  const check=tasks.find(task=>task.key==='r1-validate-3'),final=tasks.find(task=>task.key==='r1-verify');
  for(const task of tasks.filter(task=>task.id!==check.id))await accept(task.id);
  expect(await verifiedBatch(pool,objectiveId,1)).toBeNull();
  const checkRun=await accept(check.id);
  const proof=await verifiedBatch(pool,objectiveId,1);
  expect(proof?.taskId).toBe(final.id);
  expect(proof?.runId).not.toBe(checkRun);
  expect(proof?.sourceWorkspace).toContain('r1-integrate');
  await pool.query('UPDATE acceptances SET passed=false WHERE run_id=$1',[checkRun]);
  expect(await verifiedBatch(pool,objectiveId,1)).toBeNull();
});

it('refuses ambiguous validation sinks or a cycle even when all reported results passed',async()=>{
  const {objectiveId,tasks}=await persist([delivery('producer'),validation()]);
  for(const task of tasks)await accept(task.id);
  const final=tasks.find(task=>task.key==='r1-verify'),check=tasks.find(task=>task.key==='r1-validate-2'),integrate=tasks.find(task=>task.key==='r1-integrate');
  expect(await verifiedBatch(pool,objectiveId,1)).not.toBeNull();
  await pool.query('DELETE FROM dependencies WHERE task_id=$1 AND depends_on=$2',[final.id,check.id]);
  expect(await verifiedBatch(pool,objectiveId,1)).toBeNull();
  await pool.query('INSERT INTO dependencies VALUES($1,$2)',[final.id,check.id]);
  await pool.query('INSERT INTO dependencies VALUES($1,$2)',[integrate.id,final.id]);
  expect(await verifiedBatch(pool,objectiveId,1)).toBeNull();
  await pool.query('DELETE FROM dependencies WHERE task_id=$1 AND depends_on=$2',[integrate.id,final.id]);
  const producer=tasks.find(task=>task.key==='r1-deliver-1');
  await pool.query('INSERT INTO dependencies VALUES($1,$2)',[producer.id,check.id]);
  expect(await verifiedBatch(pool,objectiveId,1)).toBeNull();
});
