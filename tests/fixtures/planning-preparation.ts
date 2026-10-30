import {createHash} from 'node:crypto';
import {preparationHash} from '../../server/source-preparation-contract.js';
const sha=(v:string)=>createHash('sha256').update(v).digest('hex');
const canonical=(v:any):any=>Array.isArray(v)?v.map(canonical):v&&typeof v==='object'
  ?Object.fromEntries(Object.keys(v).sort().map(k=>[k,canonical(v[k])])):v;

/** Protected bridge fixtures for planner-engine tests; no filesystem or RPC. */
export function planningPreparationFixture(){
  const prepared=new Map<string,any>(),files=[{path:'source.txt',sha256:sha('fixture')}];
  const digest=sha(JSON.stringify(files.map(f=>[f.path,f.sha256])));
  const sources=new Map<string,any>();
  const registerSource=(identity:{goalId:string;taskId:string;runId:string;fence:string;contextId:string;workspace:string;sourceWorkspace:string})=>{
    const createdAt=new Date().toISOString(),sourceSnapshotDigest=sha('fixture-parent:'+identity.sourceWorkspace);
    const marker={workspace:identity.workspace,sourceWorkspace:identity.sourceWorkspace,baseDigest:digest,sourceSnapshotDigest,createdAt};
    const raw=JSON.stringify(canonical(marker));
    sources.set(identity.workspace,{identity,snapshot:{version:1,workspace:identity.workspace,
      generation:{workspace:identity.workspace,sourceWorkspace:identity.sourceWorkspace,baseDigest:digest,sourceSnapshotDigest,snapshotDigest:sha(raw)},
      markerBase64:Buffer.from(raw).toString('base64'),legacyParentGeneration:false,candidate:{baseDigest:digest,candidateDigest:digest,files}}});
  };
  const inspect=(method:string,params:any)=>{
    const checkedAt=new Date().toISOString();
    if(method==='worker.inspectPlanningSource'){
      const registered=sources.get(params.workspace);
      if(registered)return {version:1,workspace:params.workspace,sourceGenerationKind:'snapshot',sourceDigest:digest,
        generationDigest:registered.snapshot.generation.snapshotDigest,snapshot:{...registered.snapshot,checkedAt},checkedAt};
      if(params.sourceGenerationKind==='snapshot')throw Error('FIXTURE_SOURCE_NOT_REGISTERED');
      const directoryIdentity={device:'1',inode:'5'};
      return {version:1,workspace:params.workspace,sourceGenerationKind:'unmarked',sourceDigest:digest,files,directoryIdentity,checkedAt,
        generationDigest:preparationHash({version:1,kind:'unmarked',workspace:params.workspace,sourceDigest:digest,...directoryIdentity})};
    }
    if(method==='worker.inspectRetrySourceBinding'){
      const registered=sources.get(params.workspace),identity=registered?.identity;
      if(!identity||identity.runId!==params.runId)throw Error('FIXTURE_SOURCE_NOT_REGISTERED');
      return {...registered.snapshot,runId:identity.runId,
        state:{runId:identity.runId,workspace:identity.workspace,runtime:'harness',state:'completed',runFence:identity.fence,stopped:true},
        admission:{authority:'runtime_operation_policy',goalId:identity.goalId,taskId:identity.taskId,runId:identity.runId,
          runFence:identity.fence,contextVersion:identity.contextId,workspace:'/workspace',
          sourceSnapshotDigest:registered.snapshot.generation.snapshotDigest,policyDigest:sha('fixture-policy:'+identity.runId)},
        process:{unit:'agentos-run-'+identity.runId,checkedAt,stopped:true,loadState:'loaded',activeState:'inactive',subState:'dead',mainPid:0,
          cgroupExists:false,populated:false},checkedAt};
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
  return Object.assign(inspect,{registerSource});
}
