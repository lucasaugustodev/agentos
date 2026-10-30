import {expect,it,vi} from 'vitest';
import {randomUUID,createHash} from 'node:crypto';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import path from 'node:path';
import {inspectWorkspaceGeneration,inspectWorkspacePreparation,inspectWorkerSourceBinding,inspectWorkerRetrySourceBinding} from '../server/workspace-inspection.js';

const sha=(value:string|Buffer)=>createHash('sha256').update(value).digest('hex');
const canonical=(value:Record<string,unknown>)=>JSON.stringify(Object.fromEntries(Object.entries(value).sort(([a],[b])=>a<b?-1:a>b?1:0)));
const files=[{path:'src/main.py',sha256:sha('accepted source')}];
const inventory=(value=files)=>sha(JSON.stringify(value.map(f=>[f.path,f.sha256])));
function fixture(){
  const workspace='sample/otarget',source='sample/osource',parent='b'.repeat(64),identity=randomUUID();
  const requested={workspace,sourceWorkspace:source,preparationIdentity:identity,expectedSourceDigest:inventory(),expectedSourceSnapshotDigest:parent};
  const binding={version:1,preparationIdentity:identity,sourceWorkspace:source,targetWorkspace:workspace,expectedSourceDigest:inventory(),expectedSourceSnapshotDigest:parent};
  const body={...binding,bindingDigest:sha(canonical(binding)),observedSourceDigest:inventory(),sourceSnapshotDigest:parent,
    cloneBaseDigest:inventory(),baselineDigest:inventory(),createdAt:new Date().toISOString()};
  const receipt={...body,receiptDigest:sha(canonical(body))};
  const marker={workspace,sourceWorkspace:source,baseDigest:inventory(),sourceSnapshotDigest:parent,createdAt:body.createdAt,
    preparation:{version:1,preparationIdentity:identity,bindingDigest:receipt.bindingDigest,receiptDigest:receipt.receiptDigest}};
  const raw=Buffer.from(JSON.stringify(marker));
  const common={version:1,workspace,generation:{workspace,sourceWorkspace:source,baseDigest:inventory(),snapshotDigest:sha(raw),sourceSnapshotDigest:parent},
    markerBase64:raw.toString('base64'),candidate:{baseDigest:inventory(),candidateDigest:inventory(),files},checkedAt:new Date().toISOString()};
  const generated={...common,legacyParentGeneration:false};
  const prepared={...common,exists:true,preparationReceipt:receipt,destinationChanged:false};
  const runId=randomUUID(),fence=randomUUID();
  const sourced={...generated,runId,state:{runId,workspace,runtime:'harness',state:'completed',runFence:fence,stopped:true,
    artifacts:[{name:'candidate.json',digest:'c'.repeat(64),mime:'application/json'},{name:'changes.patch',digest:'d'.repeat(64),mime:'text/plain'}]},
    admission:{authority:'runtime_operation_policy',goalId:randomUUID(),taskId:randomUUID(),runId,runFence:fence,contextVersion:randomUUID(),
      workspace:'/workspace',sourceSnapshotDigest:common.generation.snapshotDigest,policyDigest:'e'.repeat(64)},
    process:{unit:`agentos-run-${runId}`,checkedAt:common.checkedAt,stopped:true,loadState:'not-found',activeState:'inactive',subState:'dead',mainPid:0,cgroupExists:false,populated:false}};
  return{requested,generated,prepared,sourced,runId};
}

it('exposes typed generation metadata using only the fixed readonly account bridge method',async()=>{
  const f=fixture(),request=vi.fn(async()=>f.generated);
  expect(await inspectWorkspaceGeneration(f.requested.workspace,request)).toEqual(f.generated);
  expect(request).toHaveBeenCalledWith('worker.inspectGeneration',{workspace:f.requested.workspace},{timeout:45000});
});

it('rejects malformed generation bytes inventories identity and unknown returned fields',async()=>{
  const f=fixture();
  for(const change of [(value:any)=>{value.workspace='sample/other';},(value:any)=>{value.markerBase64+='!';},
    (value:any)=>{value.candidate.candidateDigest='0'.repeat(64);},(value:any)=>{value.candidate.files.push(value.candidate.files[0]);},
    (value:any)=>{value.generation.snapshotDigest='0'.repeat(64);},(value:any)=>{value.legacyParentGeneration=true;},
    (value:any)=>{value.prompt='must not pass';},(value:any)=>{value.checkedAt=new Date(Date.now()-120000).toISOString();}]){
    const value=structuredClone(f.generated);change(value);
    await expect(inspectWorkspaceGeneration(f.requested.workspace,async()=>value)).rejects.toThrow('WORKSPACE_INSPECTION_RESPONSE_INVALID');
  }
});

it('distinguishes an exact prepared clone from retained retry edits without recopying',async()=>{
  const f=fixture(),request=vi.fn(async()=>f.prepared);
  expect(await inspectWorkspacePreparation(f.requested,request)).toEqual(f.prepared);
  const changed=structuredClone(f.prepared);changed.candidate.files=[{path:'src/main.py',sha256:sha('retry edits')}];
  changed.candidate.candidateDigest=inventory(changed.candidate.files);changed.destinationChanged=true;
  expect(await inspectWorkspacePreparation(f.requested,async()=>changed)).toEqual(changed);
  expect(request).toHaveBeenCalledWith('worker.inspectPreparation',f.requested,{timeout:45000});
});

it('accepts only explicit confirmed absence and propagates partial or uncertain outcomes without retry',async()=>{
  const f=fixture(),missing={version:1,exists:false,confirmed:true,workspace:f.requested.workspace,preparationIdentity:f.requested.preparationIdentity,checkedAt:new Date().toISOString()};
  expect(await inspectWorkspacePreparation(f.requested,async()=>missing)).toEqual(missing);
  await expect(inspectWorkspacePreparation(f.requested,async()=>({...missing,confirmed:false}))).rejects.toThrow('WORKSPACE_INSPECTION_RESPONSE_INVALID');
  for(const code of ['PREPARATION_PARTIAL_STATE','PREPARATION_EFFECT_UNCONFIRMED','STOP_UNCONFIRMED']){
    const error=Object.assign(Error(code),{code}),request=vi.fn(async()=>{throw error;});
    await expect(inspectWorkspacePreparation(f.requested,request)).rejects.toBe(error);
    expect(request).toHaveBeenCalledTimes(1);
  }
});

it('rejects preparation receipts that disagree with the request marker or sealed digest',async()=>{
  const f=fixture();
  for(const change of [(value:any)=>{value.preparationReceipt.preparationIdentity=randomUUID();},
    (value:any)=>{value.preparationReceipt.receiptDigest='0'.repeat(64);},(value:any)=>{value.preparationReceipt.bindingDigest='0'.repeat(64);},
    (value:any)=>{value.preparationReceipt.sourceWorkspace='sample/other';},(value:any)=>{value.destinationChanged=true;}]){
    const value=structuredClone(f.prepared);change(value);
    await expect(inspectWorkspacePreparation(f.requested,async()=>value)).rejects.toThrow('WORKSPACE_INSPECTION_RESPONSE_INVALID');
  }
});

it('typed source binding retains canonical runtime artifacts and the same physical stop identity',async()=>{
  const f=fixture(),request=vi.fn(async()=>f.sourced);
  expect(await inspectWorkerSourceBinding(f.runId,f.requested.workspace,request)).toEqual(f.sourced);
  expect(request).toHaveBeenCalledWith('worker.inspectSourceBinding',{runId:f.runId,workspace:f.requested.workspace},{timeout:45000});
});

it('rejects source binding fence physical stop and canonical artifact mismatches',async()=>{
  const f=fixture();
  for(const change of [(value:any)=>{value.state.runFence=randomUUID();},(value:any)=>{value.process.populated=true;},
    (value:any)=>{value.process.unit='agentos-run-other';},(value:any)=>{value.admission.sourceSnapshotDigest='0'.repeat(64);},
    (value:any)=>{value.state.artifacts[1]=value.state.artifacts[0];}]){
    const value=structuredClone(f.sourced);change(value);
    await expect(inspectWorkerSourceBinding(f.runId,f.requested.workspace,async()=>value)).rejects.toThrow('WORKSPACE_INSPECTION_RESPONSE_INVALID');
  }
});

it('retry binding projects terminal admission without accepting artifacts or claiming success',async()=>{
  const f=fixture();
  for(const terminal of ['completed','failed','cancelled']){
    const value:any=structuredClone(f.sourced);value.state.state=terminal;delete value.state.artifacts;
    const request=vi.fn(async()=>value);
    expect(await inspectWorkerRetrySourceBinding(f.runId,f.requested.workspace,request)).toEqual(value);
    expect(request).toHaveBeenCalledWith('worker.inspectRetrySourceBinding',{runId:f.runId,workspace:f.requested.workspace},{timeout:45000});
    await expect(inspectWorkerSourceBinding(f.runId,f.requested.workspace,async()=>value)).rejects.toThrow('WORKSPACE_INSPECTION_RESPONSE_INVALID');
  }
});

it('retry binding rejects live crossed or malformed identities without transport retries',async()=>{
  const f=fixture();
  for(const change of [(v:any)=>{v.state.state='running';},(v:any)=>{v.state.state='not_started';},
    (v:any)=>{v.admission.runId=randomUUID();},(v:any)=>{v.state.runFence=randomUUID();},
    (v:any)=>{v.process.populated=true;},(v:any)=>{v.admission.sourceSnapshotDigest='0'.repeat(64);},
    (v:any)=>{v.state.artifacts=[];},(v:any)=>{v.admission.contextVersion='invalid';}]){
    const value:any=structuredClone(f.sourced);value.state.state='failed';delete value.state.artifacts;change(value);
    await expect(inspectWorkerRetrySourceBinding(f.runId,f.requested.workspace,async()=>value)).rejects.toThrow('WORKSPACE_INSPECTION_RESPONSE_INVALID');
  }
  const error=Error('PREPARATION_RECEIPT_MISSING'),request=vi.fn(async()=>{throw error;});
  await expect(inspectWorkerRetrySourceBinding(f.runId,f.requested.workspace,request)).rejects.toBe(error);
  expect(request).toHaveBeenCalledTimes(1);
});

it('executes real readonly filesystem and bridge dispatcher adversarial fixtures',async()=>{
  const python=await promisify(execFile)('python3',['-B',path.resolve('tests/fixtures/workspace_readonly_inspection_test.py')],{timeout:60000,maxBuffer:1024*1024});
  expect(python.stdout).toBe('');expect(python.stderr).toContain('Ran 16 tests');expect(python.stderr).toContain('OK');
  const node=await promisify(execFile)(process.execPath,['--test','--test-reporter=tap',path.resolve('tests/workspace-inspection-bridge.test.mjs')],{timeout:30000,maxBuffer:1024*1024});
  expect(node.stdout).toContain('# tests 6');expect(node.stdout).toContain('# pass 6');expect(node.stdout).toContain('# fail 0');expect(node.stdout).toContain('# skipped 0');
},95000);
