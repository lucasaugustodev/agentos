import {createHash,randomUUID} from 'node:crypto';
import {expect,it} from 'vitest';
import {preparationHash,verifySourcePreparation,preparedBaselineBinding} from '../server/source-preparation-contract.js';

const sha=(v:string|Buffer)=>createHash('sha256').update(v).digest('hex');
const canonical=(v:any):any=>Array.isArray(v)?v.map(canonical):v&&typeof v==='object'
  ?Object.fromEntries(Object.keys(v).sort().map(k=>[k,canonical(v[k])])):v;
function fixture(){
  const request={workspace:'sample/onext',sourceWorkspace:'sample/oaccepted',preparationIdentity:randomUUID(),
    expectedSourceDigest:sha('candidate'),expectedSourceSnapshotDigest:sha('parent marker')};
  const binding={version:1,preparationIdentity:request.preparationIdentity,sourceWorkspace:request.sourceWorkspace,
    targetWorkspace:request.workspace,expectedSourceDigest:request.expectedSourceDigest,
    expectedSourceSnapshotDigest:request.expectedSourceSnapshotDigest};
  const body={...binding,bindingDigest:preparationHash(binding),observedSourceDigest:request.expectedSourceDigest,
    sourceSnapshotDigest:request.expectedSourceSnapshotDigest,cloneBaseDigest:request.expectedSourceDigest,
    baselineDigest:request.expectedSourceDigest,createdAt:'2026-01-01T00:00:00+00:00'};
  const receipt={...body,receiptDigest:preparationHash(body)};
  const marker={workspace:request.workspace,sourceWorkspace:request.sourceWorkspace,baseDigest:request.expectedSourceDigest,
    sourceSnapshotDigest:request.expectedSourceSnapshotDigest,createdAt:receipt.createdAt,
    preparation:{version:1,preparationIdentity:request.preparationIdentity,bindingDigest:receipt.bindingDigest,receiptDigest:receipt.receiptDigest}};
  const bytes=Buffer.from(JSON.stringify(canonical(marker)));
  const inspection:any={version:1,exists:true,workspace:request.workspace,preparationReceipt:receipt,markerBase64:bytes.toString('base64'),
    generation:{workspace:request.workspace,sourceWorkspace:request.sourceWorkspace,baseDigest:request.expectedSourceDigest,
      snapshotDigest:sha(bytes),sourceSnapshotDigest:request.expectedSourceSnapshotDigest},
    candidate:{baseDigest:request.expectedSourceDigest,candidateDigest:request.expectedSourceDigest},destinationChanged:false,checkedAt:1000};
  return {request,inspection,marker,bytes};
}
const verify=(f:ReturnType<typeof fixture>,requireUnchanged=true)=>verifySourcePreparation(f.request,f.inspection,{requireUnchanged,now:2000});

it('binds the prepared tree and receipt to the source generation, preserving final acceptance',()=>{
  const f=fixture(),v=verify(f);
  expect(v.targetSnapshotDigest).toBe(sha(f.bytes));expect(v.destinationChanged).toBe(false);
  expect(v).not.toHaveProperty('accepted');expect(v).not.toHaveProperty('requirements');
  const consumerId=randomUUID();
  expect(preparedBaselineBinding(v,f.request.workspace,'sample/oworker',consumerId)).toEqual({
    workspace:'sample/oworker',sourceWorkspace:f.request.workspace,preparationIdentity:consumerId,
    expectedSourceDigest:f.request.expectedSourceDigest,expectedSourceSnapshotDigest:sha(f.bytes)});
  expect(preparedBaselineBinding(v,'sample/another-parent','sample/oworker',consumerId)).toBeNull();
});

it('never treats a receipt alone as completed preparation',()=>{
  const f=fixture();f.inspection={preparationReceipt:f.inspection.preparationReceipt};
  expect(()=>verify(f)).toThrow('SOURCE_PREPARATION_PROOF_INVALID');
});

it.each(['preparationIdentity','sourceWorkspace','targetWorkspace','expectedSourceDigest','expectedSourceSnapshotDigest',
  'observedSourceDigest','cloneBaseDigest','baselineDigest','bindingDigest','receiptDigest'])(
  'rejects a changed %s in the protected receipt',key=>{
    const f=fixture();f.inspection.preparationReceipt[key]=sha('other');
    expect(()=>verify(f)).toThrow('SOURCE_PREPARATION_PROOF_INVALID');
  });

it('rejects a consistently rehashed receipt that retargets the durable request',()=>{
  const f=fixture();f.inspection.preparationReceipt.targetWorkspace='sample/other';
  const {receiptDigest,...body}=f.inspection.preparationReceipt;
  f.inspection.preparationReceipt.receiptDigest=preparationHash(body);
  expect(()=>verify(f)).toThrow('SOURCE_PREPARATION_PROOF_INVALID');
});

it('rejects regenerated marker bytes even when parsed JSON agrees',()=>{
  const f=fixture();f.inspection.markerBase64=Buffer.from(JSON.stringify(f.marker,null,2)).toString('base64');
  expect(()=>verify(f)).toThrow('SOURCE_PREPARATION_PROOF_INVALID');
});

it('rejects duplicate JSON keys rather than accepting the final parsed value',()=>{
  const f=fixture();const bytes=Buffer.from('{"workspace":"sample/untrusted",'+f.bytes.toString().slice(1));
  f.inspection.markerBase64=bytes.toString('base64');f.inspection.generation.snapshotDigest=sha(bytes);
  expect(()=>verify(f)).toThrow('SOURCE_PREPARATION_PROOF_INVALID');
});

it('rejects missing or altered generation and stale physical observation',()=>{
  for(const edit of [(x:any)=>delete x.generation,(x:any)=>x.generation.snapshotDigest=sha('new'),
    (x:any)=>x.checkedAt=-60000,(x:any)=>x.checkedAt=3000,(x:any)=>x.checkedAt='invalid',
    (x:any)=>x.destinationChanged=true]){
    const f=fixture();edit(f.inspection);expect(()=>verify(f)).toThrow('SOURCE_PREPARATION_PROOF_INVALID');
  }
});

it('rejects a modified first clone while retaining an explicitly authorized retry candidate',()=>{
  const f=fixture();f.inspection.candidate.candidateDigest=sha('work preserved');f.inspection.destinationChanged=true;
  expect(()=>verify(f)).toThrow('SOURCE_PREPARATION_PROOF_INVALID');
  const retained=verify(f,false);expect(retained.currentCandidateDigest).toBe(sha('work preserved'));
  expect(retained.targetSnapshotDigest).toBe(sha(f.bytes));expect(retained.destinationChanged).toBe(true);
  // The pure check grants no retry ownership or execution authority by itself.
  expect(retained).not.toHaveProperty('canExecute');
});

it('does not issue a self-copy or cross-project consumer binding',()=>{
  const f=fixture(),v=verify(f);
  expect(()=>preparedBaselineBinding(v,f.request.workspace,f.request.workspace,randomUUID())).toThrow();
  expect(()=>preparedBaselineBinding(v,f.request.workspace,'elsewhere/otask',randomUUID())).toThrow();
});
