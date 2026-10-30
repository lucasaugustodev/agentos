import {createHash} from 'node:crypto';
import {preparationHash} from '../../server/source-preparation-contract.js';
const sha=(v:string)=>createHash('sha256').update(v).digest('hex');
const canonical=(v:any):any=>Array.isArray(v)?v.map(canonical):v&&typeof v==='object'
  ?Object.fromEntries(Object.keys(v).sort().map(k=>[k,canonical(v[k])])):v;

/** Protected bridge fixtures for planner-engine tests; no filesystem or RPC. */
export function planningPreparationFixture(){
  const prepared=new Map<string,any>(),files=[{path:'source.txt',sha256:sha('fixture')}];
  const digest=sha(JSON.stringify(files.map(f=>[f.path,f.sha256])));
  return (method:string,params:any)=>{
    const checkedAt=new Date().toISOString();
    if(method==='worker.inspectPlanningSource'){
      const directoryIdentity={device:'1',inode:'5'};
      return {version:1,workspace:params.workspace,sourceGenerationKind:'unmarked',sourceDigest:digest,files,directoryIdentity,checkedAt,
        generationDigest:preparationHash({version:1,kind:'unmarked',workspace:params.workspace,sourceDigest:digest,...directoryIdentity})};
    }
    if(method==='worker.inspectPreparation')return prepared.has(params.preparationIdentity)
      ?{...prepared.get(params.preparationIdentity),checkedAt}
      :{version:1,exists:false,confirmed:true,workspace:params.workspace,preparationIdentity:params.preparationIdentity,checkedAt};
    if(method!=='worker.prepare')return undefined;
    const binding={version:1,preparationIdentity:params.preparationIdentity,sourceWorkspace:params.sourceWorkspace,
      targetWorkspace:params.workspace,expectedSourceDigest:params.expectedSourceDigest,expectedSourceSnapshotDigest:params.expectedSourceSnapshotDigest,
      ...(params.sourceGenerationKind?{sourceGenerationKind:params.sourceGenerationKind}:{})};
    const body={...binding,bindingDigest:preparationHash(binding),observedSourceDigest:digest,sourceSnapshotDigest:params.expectedSourceSnapshotDigest,
      cloneBaseDigest:digest,baselineDigest:digest,createdAt:checkedAt};
    const receipt={...body,receiptDigest:preparationHash(body)};
    const marker={workspace:params.workspace,sourceWorkspace:params.sourceWorkspace,baseDigest:digest,
      sourceSnapshotDigest:params.expectedSourceSnapshotDigest,createdAt:checkedAt,preparation:{version:1,
        preparationIdentity:params.preparationIdentity,bindingDigest:receipt.bindingDigest,receiptDigest:receipt.receiptDigest}};
    const raw=JSON.stringify(canonical(marker));
    prepared.set(params.preparationIdentity,{version:1,exists:true,workspace:params.workspace,preparationReceipt:receipt,
      markerBase64:Buffer.from(raw).toString('base64'),generation:{workspace:params.workspace,sourceWorkspace:params.sourceWorkspace,
        baseDigest:digest,sourceSnapshotDigest:params.expectedSourceSnapshotDigest,snapshotDigest:sha(raw)},
      candidate:{baseDigest:digest,candidateDigest:digest,files},destinationChanged:false,checkedAt});
    return {};
  };
}
