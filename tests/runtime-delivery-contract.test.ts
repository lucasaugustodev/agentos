import { afterEach, expect, it, vi } from 'vitest';
import { randomUUID } from 'node:crypto';
import { executeRuntime } from '../server/runtime.js';
import { config } from '../server/config.js';
import { accountBridge } from '../server/bridge.js';
import { taskDeliveryContract } from '../bridge/task-delivery-contract.mjs';

afterEach(() => { vi.restoreAllMocks(); config.remoteExecution = false; config.bridgeHost = ''; });
const criteria = [
  { id: 'patch', type: 'artifact', name: 'change.diff', label: 'Integrated patch' },
  { id: 'manifest', type: 'artifact', name: 'manifest.json', label: 'Candidate manifest' },
  { id: 'passed', type: 'json_field', name: 'verification.json', field: 'passed', equals: true, label: 'Real integration checks' },
];
function input(extraCriteria: any[] = []): any {
  config.remoteExecution = true;
  const task = { id: randomUUID(), goal_id: randomUUID(), title: 'Treasures of Aztec integration', description: 'Implement and test only the task.',
    criteria: [...criteria, ...extraCriteria], execution: { kind: 'harness', workspace: 'test/isolated' } };
  return { task, run: { id: randomUUID(), task_id: task.id, fence: randomUUID() }, account: { bridge_id: 'agentos:muse:test', model: 'muse-spark-1.3' },
    context: { content: '', manifest: { selected: [], dependencyArtifacts: [], executionInputs: [] } },
    signal: new AbortController().signal, onEvent: vi.fn() };
}
function stoppedBridge() {
  return vi.spyOn(accountBridge, 'request').mockResolvedValue({ summary: 'Existing result', artifacts: [], evidence: { stopped: true } });
}
it('transmits all integration delivery names as a typed worker.execute contract', async () => {
  const i = input(); const request = stoppedBridge();
  await executeRuntime(i);
  const [method, params] = request.mock.calls[0] as any;
  expect(method).toBe('worker.execute');
  expect(params).toMatchObject({runId:i.run.id,runFence:i.run.fence});
  expect(params.deliveryContract).toEqual({ version: 1, artifacts: [
    { name: 'change.diff', path: 'evidence/change.diff', mime: 'text/plain' },
    { name: 'manifest.json', path: 'evidence/manifest.json', mime: 'application/json' },
    { name: 'verification.json', path: 'evidence/verification.json', mime: 'application/json' },
  ] });
  expect(JSON.parse(JSON.stringify(params)).deliveryContract).toEqual(params.deliveryContract);
  expect(params.prompt).toContain('Criteria: ' + JSON.stringify(criteria));
});

it('uses a worker inactivity window while preserving its signal and result contract', async () => {
  const i=input(),request=stoppedBridge();
  await executeRuntime(i);
  const options=request.mock.calls[0][2];
  expect(options).toMatchObject({signal:i.signal,idleTimeout:60*60*1000});
  expect(options?.timeout).toBeUndefined();
});
it('derives delivery from the immutable run snapshot rather than changed live criteria', async () => {
  const i = input(); const frozen = { ...i.task, criteria };
  i.run.instruction_snapshot = { schema: 1, task: frozen };
  i.task = { ...i.task, criteria: [{ type: 'artifact', name: 'unrelated.json' }] };
  const request = stoppedBridge();
  await executeRuntime(i);
  expect((request.mock.calls[0][1] as any).deliveryContract).toEqual(taskDeliveryContract(criteria));
});
it('transports the frozen requirement outcome report as a mandatory artifact without granting acceptance',async()=>{
  const gate={id:'outcomes',type:'requirement_outcomes',version:2,name:'requirement-outcomes.json',label:'Product outcomes',
    requirements:[{id:'product',observables:[{id:'result',name:'result.json',authority:'current_run',type:'artifact',description:'Actual product evidence'}],externalDependencies:[]}]};
  const i=input([gate]);i.run.instruction_snapshot={schema:1,task:structuredClone(i.task)};
  i.task.criteria=[];
  const request=stoppedBridge(),result=await executeRuntime(i);
  const contract=(request.mock.calls[0][1] as any).deliveryContract;
  expect(contract.artifacts.filter((a:any)=>a.name==='requirement-outcomes.json')).toEqual([
    {name:'requirement-outcomes.json',path:'evidence/requirement-outcomes.json',mime:'application/json'}]);
  expect(contract.artifacts.some((a:any)=>a.name==='candidate.json')).toBe(false);
  expect(result).toMatchObject({artifacts:[]});
  expect(()=>taskDeliveryContract([{...gate,name:'../requirement-outcomes.json'}])).toThrow('TASK_DELIVERY_CONTRACT');
});
it('leaves controller generated artifacts out of the worker contract without changing acceptance criteria', async () => {
  const i = input(['candidate.json', 'changes.patch', 'runtime-protocol-check.json'].map(name => ({ type: 'artifact', name })));
  const request = stoppedBridge(); await executeRuntime(i);
  const params = request.mock.calls[0][1] as any;
  expect(params.deliveryContract).toEqual(taskDeliveryContract(criteria));
  for (const name of ['candidate.json', 'changes.patch', 'runtime-protocol-check.json']) expect(params.prompt).toContain(name);
});
it('does not rewrite workspace, source isolation or the existing stopped-result contract', async () => {
  const i = input(); i.task.execution.readOnlySource = true; i.task.execution.workKind = 'repair';
  const request = stoppedBridge(); const result = await executeRuntime(i);
  expect(request.mock.calls[0][1]).toMatchObject({ workspace: 'test/isolated', readOnlySource: true, workKind: 'repair' });
  expect(result).toMatchObject({ summary: 'Existing result', artifacts: [] });
});
it('rejects unsafe delivery names before a worker execution request', async () => {
  const i = input([{ type: 'artifact', name: '../manifest.json' }]); const request = stoppedBridge();
  await expect(executeRuntime(i)).rejects.toMatchObject({ code: 'TASK_DELIVERY_CONTRACT' });
  expect(request).not.toHaveBeenCalled();
  expect(i.onEvent).toHaveBeenCalledWith('HARNESS_NOT_STARTED',expect.objectContaining({confirmed:true,phase:'preflight'}));
});
it('records a recoverable prelaunch failure for oversized operator inputs without invoking a remote actor',async()=>{
 const i=input(),request=stoppedBridge();
 const bytes=Buffer.alloc(8*1024*1024+1);
 const {createHash}=await import('node:crypto');
 i.task.execution.inputs=[{id:randomUUID(),name:'huge.log',digest:createHash('sha256').update(bytes).digest('hex'),base64:bytes.toString('base64')}];
 await expect(executeRuntime(i)).rejects.toMatchObject({code:'SUPERVISION_INPUTS_TOO_LARGE'});
 expect(request).not.toHaveBeenCalled();
 expect(i.onEvent).toHaveBeenCalledWith('HARNESS_NOT_STARTED',expect.objectContaining({confirmed:true,code:'SUPERVISION_INPUTS_TOO_LARGE'}));
});
