import {beforeAll,beforeEach,afterAll,it,expect} from 'vitest';
import {randomUUID,createHash} from 'node:crypto';
import {mkdir,writeFile} from 'node:fs/promises';
import path from 'node:path';
import {pool,migrate} from '../server/db.js';
import {config} from '../server/config.js';
import {seedRegistry} from '../server/registry.js';
import {validateTask} from '../server/service.js';
import {readPendingValidations,readValidationSnapshot} from '../server/validation-identity.js';

const hash=(v:string|Buffer)=>createHash('sha256').update(v).digest('hex');
const winner={path:'src/winner.ts',sha256:hash('known-good')},feature={path:'src/feature.ts',sha256:hash('implemented')};
const digest=(files:any[])=>hash(JSON.stringify(files.map(f=>[f.path,f.sha256]).sort((a,b)=>a[0].localeCompare(b[0]))));
const contract={version:1,required:[{path:feature.path,kind:'file'}],preserve:[{path:winner.path,kind:'file'}]};
beforeAll(async()=>{expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');await migrate();await mkdir(path.join(config.dataDir,'artifacts'),{recursive:true});});
beforeEach(async()=>{await pool.query('TRUNCATE goals,agents,accounts,memories,events CASCADE');
  await pool.query("DELETE FROM settings WHERE id LIKE 'validation-receipt:%'");await seedRegistry();});
afterAll(()=>pool.end());
async function artifact(run:string,name:string,value:any){
  const id=randomUUID(),bytes=Buffer.from(JSON.stringify(value));await writeFile(path.join(config.dataDir,'artifacts',id),bytes,{flag:'wx'});
  await pool.query("INSERT INTO artifacts(id,run_id,name,mime,digest,size) VALUES($1,$2,$3,'application/json',$4,$5)",[id,run,name,hash(bytes),bytes.length]);return id;
}
async function fixture(options:{missing?:boolean;regression?:boolean;legacy?:boolean;duplicate?:boolean}={}){
  const goal=randomUUID(),task=randomUUID(),run=randomUUID(),fence=randomUUID();
  const execution={kind:'harness',workspace:'source-contract/otask',sourceWorkspace:'source-contract/base',...(!options.legacy?{sourceContract:contract}:{})};
  await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1::uuid,'Candidate','Source contract','source-contract',$1::text)",[goal]);
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria,attempt_count,stage,execution)
    VALUES($1,$2,'deliver','Deliver source','Deliver and retain canonical source','qa.backend','submitted',$3,1,'work',$4)`,[task,goal,
    JSON.stringify([{id:'report',type:'json_field',name:'delivery.json',field:'passed',equals:true,label:'Declared delivery'}]),JSON.stringify(execution)]);
  await pool.query("INSERT INTO runs(id,task_id,attempt,state,fence,runtime,agent_version) VALUES($1,$2,1,'submitted',$3,'harness',1)",[run,task,fence]);
  await artifact(run,'delivery.json',{passed:true});
  const files=[options.regression?{...winner,sha256:hash('regression')}:winner,...(!options.missing?[feature]:[])];
  const candidate={baseDigest:digest([winner]),candidateDigest:digest(files),baseFiles:[winner],files};
  await artifact(run,'candidate.json',candidate);if(options.duplicate)await artifact(run,'candidate.json',candidate);
  return {goal,task,run,execution};
}
it('refuses passed:true when the required source is not in the actual candidate and preserves the rejection',async()=>{
  const f=await fixture({missing:true});const result=await validateTask(f.task);
  expect(result.passed).toBe(false);expect(result.checks).toEqual(expect.arrayContaining([expect.objectContaining({id:'source-output',passed:false})]));
  expect((await pool.query('SELECT state FROM tasks WHERE id=$1',[f.task])).rows[0].state).toBe('submitted');
  expect(await readPendingValidations()).toEqual([]);expect(await validateTask(f.task)).toMatchObject({passed:false,duplicate:true});
});
it('accepts only when both the canonical source contract and original report criterion pass',async()=>{
  const f=await fixture();expect((await validateTask(f.task)).passed).toBe(true);
  expect(await validateTask(f.task)).toMatchObject({passed:true,duplicate:true});
  expect((await pool.query('SELECT count(*)::int n FROM acceptances WHERE task_id=$1',[f.task])).rows[0].n).toBe(1);
});
it('rejects a regression of a preserved source despite a passing delivery report',async()=>{
  const f=await fixture({regression:true});expect((await validateTask(f.task)).passed).toBe(false);
});
it('refuses ambiguous canonical artifacts instead of accepting the first one',async()=>{
  const f=await fixture({duplicate:true});expect((await validateTask(f.task)).passed).toBe(false);
});
it('binds validation identity to the immutable and current source contracts',async()=>{
  const f=await fixture(),before=await readValidationSnapshot(pool,f.task);
  expect(before!.validation_input.sourceOutput.frozen.contract).toEqual(contract);
  await pool.query('UPDATE tasks SET execution=$2 WHERE id=$1',[f.task,JSON.stringify({...f.execution,sourceContract:{...contract,preserve:[]}})]);
  const after=await readValidationSnapshot(pool,f.task);expect(after!.validation_key).not.toBe(before!.validation_key);
  expect((await validateTask(f.task)).checks).toEqual(expect.arrayContaining([expect.objectContaining({id:'source-output',passed:false,detail:'SOURCE_OUTPUT_FROZEN_CONTRACT_MISMATCH'})]));
});
it('cannot withdraw an admitted source contract to obtain legacy acceptance',async()=>{
  const f=await fixture({missing:true}),{sourceContract:ignored,...execution}=f.execution;
  await pool.query('UPDATE tasks SET execution=$2 WHERE id=$1',[f.task,JSON.stringify(execution)]);
  expect((await validateTask(f.task)).passed).toBe(false);
});
it('does not rewrite historical legacy validation semantics',async()=>{
  const f=await fixture({legacy:true,missing:true});const result=await validateTask(f.task);
  expect(result.passed).toBe(true);expect(result.checks.some(c=>c.id==='source-output')).toBe(false);
  expect((await readValidationSnapshot(pool,f.task))!.validation_input).not.toHaveProperty('sourceOutput');
});
