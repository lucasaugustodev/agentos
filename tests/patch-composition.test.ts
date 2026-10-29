import {it,expect} from 'vitest';
import {randomUUID} from 'node:crypto';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import path from 'node:path';
import {patchComposition} from '../server/patch-composition.js';
import {bindCompositionInputs} from '../bridge/composition-inputs.mjs';
import {createHash,randomUUID as uuid} from 'node:crypto';

const canonical=(value:any):any=>Array.isArray(value)?value.map(canonical):value&&typeof value==='object'
  ?Object.fromEntries(Object.keys(value).sort().map(key=>[key,canonical(value[key])])):value;
const proofDigest=(value:any)=>createHash('sha256').update(JSON.stringify(canonical(value))).digest('hex');

it('composes only hash-bound accepted dependencies, excluding rejected recovery and operator patches',()=>{
  const taskId=randomUUID(),patch={id:randomUUID(),name:'implementation.patch',digest:'a'.repeat(64)},
    report={id:randomUUID(),name:'repair-report.json',digest:'b'.repeat(64)};
  const refs=[{...patch,task_id:taskId},{...report,task_id:taskId}];
  const task={stage:'integrate',execution:{sourceWorkspace:'test/source',workspace:'test/owned'}};
  const inputs=[patch,report];
  expect(patchComposition(task,{dependencyArtifacts:refs},inputs)?.patches).toHaveLength(1);
  expect(patchComposition(task,{dependencyArtifacts:[],recoveryArtifacts:refs},inputs)).toBeNull();
  expect(patchComposition({...task,execution:{...task.execution,readOnlySource:true}},{dependencyArtifacts:refs},inputs)).toBeNull();
  expect(patchComposition({...task,stage:'verify'},{dependencyArtifacts:refs},inputs)).toBeNull();
  expect(()=>patchComposition(task,{dependencyArtifacts:refs},[{...patch,digest:'c'.repeat(64)},report])).toThrow('COMPOSITION_INPUT_BINDING');
});

it('carries references rather than duplicate patch bytes and resolves every identity at the remote boundary',()=>{
  const id=randomUUID(),producer=randomUUID(),patch={id,name:'implementation.patch',digest:'a'.repeat(64),base64:'A'.repeat(1000000)},
    report={id:randomUUID(),name:'repair-report.json',digest:'b'.repeat(64),base64:'BB=='};
  const task={stage:'integrate',execution:{sourceWorkspace:'test/source'}},refs=[{...patch,task_id:producer},{...report,task_id:producer}];
  const plan=patchComposition(task,{dependencyArtifacts:refs},[patch,report]);
  expect(JSON.stringify(plan).length).toBeLessThan(600);
  expect(bindCompositionInputs(plan,[patch,report]).patches[0].patch).toEqual(patch);
  expect(()=>bindCompositionInputs(plan,[{...patch,digest:'c'.repeat(64)},report])).toThrow('COMPOSITION_INPUT_BINDING');
  expect(()=>bindCompositionInputs(plan,[patch,patch,report])).toThrow('COMPOSITION_INPUT_BINDING');
  expect(()=>bindCompositionInputs(plan,[report])).toThrow('COMPOSITION_INPUT_BINDING');
});

it('freezes only supervisor-selected active repairs against the exact prior candidate and retains superseded proof',()=>{
  const taskId=uuid(),goalId=uuid(),runId=uuid(),fence='fence-37',workspace='game/o-integrate',
    oldProducer=uuid(),activeProducer=uuid(),oldRun=uuid(),activeRun=uuid();
  const oldPatch={id:uuid(),name:'implementation.patch',digest:'1'.repeat(64),task_id:oldProducer,run_id:oldRun},
    oldReport={id:uuid(),name:'repair-report.json',digest:'2'.repeat(64),task_id:oldProducer,run_id:oldRun},
    patchBytes=Buffer.from('--- a/src/app.js\n+++ b/src/app.js\n@@ -1 +1 @@\n-old\n+new\n'),
    reportBytes=Buffer.from('{"fixVerified":true,"testsPassed":true}\n'),
    patch={id:uuid(),name:'implementation.patch',digest:createHash('sha256').update(patchBytes).digest('hex'),
      base64:patchBytes.toString('base64'),task_id:activeProducer,run_id:activeRun},
    report={id:uuid(),name:'repair-report.json',digest:createHash('sha256').update(reportBytes).digest('hex'),
      base64:reportBytes.toString('base64'),task_id:activeProducer,run_id:activeRun};
  const active={producerTaskId:activeProducer,producerRunId:activeRun,patchArtifactId:patch.id,patchDigest:patch.digest,
    reportArtifactId:report.id,reportDigest:report.digest};
  const superseded={producerTaskId:oldProducer,producerRunId:oldRun,patchArtifactId:oldPatch.id,patchDigest:oldPatch.digest,
    reportArtifactId:oldReport.id,reportDigest:oldReport.digest};
  const selection:any={version:1,authority:'supervisor-effective-source-manifest',consumerTaskId:taskId,
    consumerRunId:runId,consumerRunFence:fence,workspace,target:{consumerRunId:runId,workspace,
      candidateArtifactId:uuid(),candidateArtifactDigest:'5'.repeat(64),candidateDigest:'6'.repeat(64)},
    active:[active],superseded:[superseded],resolutionEventIds:['resolution-event']};
  selection.digest=proofDigest(selection);
  const binding={eventIds:['resolution-event'],targetCandidate:selection.target,
    original:{taskId:oldProducer,runId:oldRun},effectiveSource:{taskId:activeProducer,runId:activeRun,artifacts:[patch,report]}};
  const task={id:taskId,goal_id:goalId,stage:'integrate',execution:{sourceWorkspace:'game/source',workspace}};
  const manifest={runIdentity:{taskId,goalId,runId,runFence:fence},dependencyArtifacts:[patch,report],
    effectiveSources:{compositionSelection:selection,bindings:[binding],historicalArtifacts:[oldPatch,oldReport]}};
  const plan=patchComposition(task,manifest,[patch,report]);
  expect(plan).toMatchObject({version:2,expectedCandidateDigest:'6'.repeat(64),patches:[{
    producerTaskId:activeProducer,patch:{id:patch.id},report:{id:report.id}}],
    selection:{active:[active],superseded:[superseded]}});
  expect(bindCompositionInputs(plan,[patch,report]).patches[0].patch).toEqual(patch);
  expect(plan?.patches.some((entry:any)=>entry.producerTaskId===oldProducer)).toBe(false);
  expect(()=>patchComposition(task,{...manifest,runIdentity:{...manifest.runIdentity,runId:uuid()}},[patch,report]))
    .toThrow('COMPOSITION_SELECTION_INVALID');
  const foreign={...selection,consumerRunId:uuid()};
  const {digest:_foreignDigest,...foreignBody}=foreign;foreign.digest=proofDigest(foreignBody);
  expect(()=>patchComposition(task,{...manifest,effectiveSources:{...manifest.effectiveSources,compositionSelection:foreign}},[patch,report]))
    .toThrow('COMPOSITION_SELECTION_INVALID');
  expect(()=>patchComposition(task,{...manifest,dependencyArtifacts:[oldPatch,oldReport,patch,report]},[oldPatch,oldReport,patch,report]))
    .toThrow('COMPOSITION_SELECTION_SUPERSEDED_ACTIVE');
  const forged={...selection,active:[{...active,producerTaskId:uuid()}]};
  const {digest:_digest,...forgedBody}=forged;forged.digest=proofDigest(forgedBody);
  expect(()=>patchComposition(task,{...manifest,effectiveSources:{...manifest.effectiveSources,compositionSelection:forged}},[patch,report]))
    .toThrow('COMPOSITION_SELECTION_ACTIVE_SET');
});

it('rejects an omitted accepted dependency patch and keeps it active when explicitly listed',()=>{
  const taskId=uuid(),goalId=uuid(),runId=uuid(),fence='fence-active-set',workspace='game/o-integrate',
    resolved=uuid(),other=uuid(),resolvedRun=uuid(),otherRun=uuid();
  const mk=(task:string,run:string,name:string,digest:string)=>({id:uuid(),task_id:task,run_id:run,name,digest});
  const p1=mk(resolved,resolvedRun,'implementation.patch','1'.repeat(64)),r1=mk(resolved,resolvedRun,'repair-report.json','2'.repeat(64));
  const p2=mk(other,otherRun,'implementation.patch','3'.repeat(64)),r2=mk(other,otherRun,'repair-report.json','4'.repeat(64));
  const ref=(p:any,r:any)=>({producerTaskId:p.task_id,producerRunId:p.run_id,patchArtifactId:p.id,patchDigest:p.digest,
    reportArtifactId:r.id,reportDigest:r.digest});
  const target={consumerTaskId:taskId,consumerRunId:'stopped',workspace,candidateArtifactId:uuid(),
    candidateArtifactDigest:'5'.repeat(64),candidateDigest:'6'.repeat(64)};
  const binding={eventIds:['decision-event'],targetCandidate:target,original:{taskId:resolved,runId:uuid()},
    effectiveSource:{taskId:resolved,runId:resolvedRun,artifacts:[p1,r1]}};
  const make=(active:any[])=>{
    const proof:any={version:1,authority:'supervisor-effective-source-manifest',consumerTaskId:taskId,consumerRunId:runId,
      consumerRunFence:fence,workspace,target,active,superseded:[],resolutionEventIds:['decision-event']};
    proof.digest=proofDigest(proof);return proof;
  };
  const task={id:taskId,goal_id:goalId,stage:'integrate',execution:{sourceWorkspace:'game/source',workspace}};
  const refs=[p1,r1,p2,r2],inputs=refs.map(({id,name,digest})=>({id,name,digest}));
  const base={runIdentity:{taskId,goalId,runId,runFence:fence},dependencyArtifacts:refs,
    effectiveSources:{bindings:[binding],historicalArtifacts:[],compositionSelection:make([ref(p1,r1)])}};
  expect(()=>patchComposition(task,base,inputs)).toThrow('COMPOSITION_SELECTION_ACTIVE_SET');
  const manifest={...base,effectiveSources:{...base.effectiveSources,compositionSelection:make([ref(p1,r1),ref(p2,r2)])}};
  expect(patchComposition(task,manifest,inputs)?.patches.map((item:any)=>item.producerTaskId).sort()).toEqual([resolved,other].sort());
});

it('checks real Git patch application, conflicts, idempotency and isolation using server-only temporary fixtures',async()=>{
  const {stdout}=await promisify(execFile)('python3',['-B',path.resolve('tests/fixtures/workspace_compose_test.py')],{timeout:20000});
  expect(JSON.parse(stdout)).toMatchObject({passed:true,checks:15,productionTouched:false});
});
