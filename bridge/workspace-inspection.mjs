import {workspaceOperation} from './workspaces.mjs';
import {inspectUnit} from './process-stop.mjs';
import {stoppedSourceBindingProof,stoppedRetrySourceBindingProof} from './stopped-run-proof.mjs';

const workspace=/^[a-z0-9_-]+\/[a-z0-9_-]+$/;
const uuid=/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/;
const hash=/^[0-9a-f]{64}$/;
const fail=()=>{throw Object.assign(Error('WORKSPACE_INSPECTION_REQUEST_INVALID'),{code:'WORKSPACE_INSPECTION_REQUEST_INVALID'});};

/** A closed request grammar. No caller-selected path, executable, policy or
 * action is forwarded to the privileged workspace manager. */
export async function workspaceInspection(method,params,io={}){
  const keys=method==='worker.inspectGeneration'?['workspace']:
    ['worker.inspectSourceBinding','worker.inspectRetrySourceBinding'].includes(method)?['workspace','runId']:
    method==='worker.inspectPreparation'?['workspace','sourceWorkspace','preparationIdentity','expectedSourceDigest','expectedSourceSnapshotDigest']:null;
  if(!keys||!params||typeof params!=='object'||Array.isArray(params)||Object.keys(params).length!==keys.length||
    keys.some(key=>typeof params[key]!=='string')||Object.keys(params).some(key=>!keys.includes(key))||!workspace.test(params.workspace))fail();
  const operation=io.operation??workspaceOperation;
  if(method==='worker.inspectGeneration')return operation({action:'inspect-generation',workspace:params.workspace});
  if(method==='worker.inspectPreparation'){
    if(!workspace.test(params.sourceWorkspace)||!uuid.test(params.preparationIdentity)||
      !hash.test(params.expectedSourceDigest)||!hash.test(params.expectedSourceSnapshotDigest))fail();
    return operation({action:'inspect-preparation',...params});
  }
  if(!uuid.test(params.runId))fail();
  const retry=method==='worker.inspectRetrySourceBinding';
  return (retry?stoppedRetrySourceBindingProof:stoppedSourceBindingProof)(params.runId,params.workspace,{
    inspectUnit:io.inspectUnit??inspectUnit,
    inspectSourceBinding:(runId,workspace)=>operation({action:retry?'inspect-retry-source-binding':'inspect',workspace,sourceBindingRunId:runId}),
  });
}
