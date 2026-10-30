import {beforeAll,beforeEach,afterEach,afterAll,expect,it,vi} from 'vitest';
import {randomUUID} from 'node:crypto';
import {pool,migrate} from '../server/db.js';
import {seedRegistry} from '../server/registry.js';
import {config} from '../server/config.js';
import {objectiveSchema} from '../server/objective-contracts.js';
import {compileDelivery} from '../server/incremental-planning.js';
import {submitObjective,claimPlanning,applyPlannedResult,feedbackObjective,reconcilePlanning} from '../server/planner.js';
import {schedule} from '../server/service.js';

beforeAll(()=>migrate());
beforeEach(async()=>{
  config.remoteExecution=false;config.bridgeHost='';config.dshCommand='';
  await pool.query('TRUNCATE goals,agents,accounts,memories,events CASCADE');
  await seedRegistry();
  await pool.query(`UPDATE accounts SET provider='codex',model='test-model',models='["test-model"]',bridge_id='test',max_concurrency=4,allow_unknown=true`);
});
afterEach(()=>{vi.restoreAllMocks();config.remoteExecution=false;});
afterAll(()=>pool.end());
const request=()=>objectiveSchema.parse({message:'Deliver source and preserve the known original file.',project:'test',workspaces:['test/base'],mode:'execute',
  planningStrategy:'incremental',requirements:['A canonical kernel must exist and preserve the reference.'],idempotencyKey:randomUUID()});
const sourceContract={version:1 as const,required:[{path:'kernels/fused.py',kind:'file' as const}],
  preserve:[{path:'kernels/reference.py',kind:'file' as const,sha256:'a'.repeat(64)}]};
const decision=()=>({kind:'delivery' as const,key:'kernel',requirementId:'req-1',title:'Implement canonical kernel',
  action:'Implement the exact required canonical kernel without changing the reference implementation.',
  verification:'Verify the canonical source outputs and run independent discriminating correctness controls.',
  agentId:'engineering.backend.node',writePaths:['kernels/fused.py'],sourceContract});

it('activates new planning without changing an existing objective request and freezes contract plus scope at admission',async()=>{
  const input=request(),{objectiveId}=await submitObjective(input),planning=await claimPlanning();
  expect(planning.feedback.sourceContractVersion).toBe(1);
  expect(planning.request.sourceContractVersion).toBe(1);
  const persisted=(await pool.query('SELECT request FROM objectives WHERE id=$1',[objectiveId])).rows[0].request;
  expect(persisted).toEqual(input);expect(persisted.sourceContractVersion).toBeUndefined();
  const plan=compileDelivery(decision(),planning.request);
  await applyPlannedResult(planning,plan);
  const work=(await pool.query("SELECT * FROM tasks WHERE goal_id=$1 AND stage='work'",[objectiveId])).rows[0];
  expect(work.execution).toMatchObject({sourceContract,writePaths:['kernels/fused.py']});
  expect(work.description).toContain('code placed there cannot satisfy a required source output');
  expect(work.description).not.toContain('PostgreSQL e somente pelo request_validation');
  config.remoteExecution=true;config.bridgeHost='synthetic-source-contract-bridge';
  const runs=await schedule();expect(runs).toHaveLength(1);
  const admitted=(await pool.query('SELECT instruction_snapshot FROM runs WHERE id=$1',[runs[0]])).rows[0].instruction_snapshot;
  expect(admitted.task.execution.sourceContract).toEqual(sourceContract);
  expect(admitted.task.execution.writePaths).toEqual(['kernels/fused.py']);
});

it('rechecks current stored planning authority instead of accepting a caller that removes the version',async()=>{
  const input=request();await submitObjective(input);const run=await claimPlanning();
  const {sourceContract:ignored,...legacyDecision}=decision();
  const legacy=compileDelivery(legacyDecision,input);
  await expect(applyPlannedResult({...run,feedback:{},request:input},legacy)).rejects.toMatchObject({code:'PLAN_INVALID',detail:{violation:'SOURCE_CONTRACT_VERSION_REQUIRED'}});
  expect((await pool.query('SELECT id FROM tasks')).rowCount).toBe(0);
});

it('keeps an explicitly historical pending proposal legacy and adopts version one only for its next revision',async()=>{
  const input=request(),{objectiveId}=await submitObjective(input);
  // Recreate a pre-upgrade database row. Production never removes this field.
  await pool.query("UPDATE planning_runs SET feedback='{}' WHERE objective_id=$1",[objectiveId]);
  const run=await claimPlanning(),{sourceContract:ignored,...legacyDecision}=decision();
  const legacy=compileDelivery(legacyDecision,input);
  expect(run.request.sourceContractVersion).toBeUndefined();
  await applyPlannedResult(run,legacy);
  const old=(await pool.query('SELECT execution FROM tasks WHERE goal_id=$1',[objectiveId])).rows;
  expect(old.every(row=>!Object.hasOwn(row.execution,'sourceContract'))).toBe(true);
  await feedbackObjective(objectiveId,'new-source-contract-review','A new revision must declare its actual source obligations.');
  await reconcilePlanning();
  const next=await claimPlanning();expect(next.feedback.sourceContractVersion).toBe(1);
  expect(next.request.sourceContractVersion).toBe(1);
  expect((await pool.query('SELECT plan FROM plan_revisions WHERE objective_id=$1',[objectiveId])).rows[0].plan).toEqual(legacy);
  expect((await pool.query('SELECT request FROM objectives WHERE id=$1',[objectiveId])).rows[0].request).toEqual(input);
});
