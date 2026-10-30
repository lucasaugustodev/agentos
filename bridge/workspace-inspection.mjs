import {workspaceOperation} from './workspaces.mjs';
import {inspectUnit} from './process-stop.mjs';
import {stoppedSourceBindingProof,stoppedRetrySourceBindingProof} from './stopped-run-proof.mjs';

const workspace=/^[a-z0-9_-]+\/[a-z0-9_-]+$/;
const uuid=/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/;
const hash=/^[0-9a-f]{64}$/;
const fail=()=>{throw Object.assign(Error('WORKSPACE_INSPECTION_REQUEST_INVALID'),{code:'WORKSPACE_INSPECTION_REQUEST_INVALID'});};
const inspectionErrors=new Set(['WORKSPACE_INSPECTION_REQUEST_INVALID','SOURCE_INVENTORY_BOUND','SOURCE_TOO_LARGE','SOURCE_NOT_REGULAR',
  'PREPARATION_BINDING_INVALID','PREPARATION_BINDING_MISMATCH','PREPARATION_DIRECTORY_UNPROTECTED',
  'PREPARATION_RECEIPT_MISSING','PREPARATION_RECEIPT_INVALID','PREPARATION_RECEIPT_UNPROTECTED',
  'PREPARATION_BASELINE_CHANGED','PREPARATION_BASELINE_UNPROTECTED','PREPARATION_SOURCE_GENERATION_CHANGED',
  'PREPARATION_SOURCE_DIGEST_CHANGED','PREPARATION_TARGET_CHANGED','PREPARATION_CLONE_CHANGED',
  'PREPARATION_PARTIAL_STATE','PREPARATION_EFFECT_UNCONFIRMED','SOURCE_BINDING_OBSERVATION_CHANGED','SOURCE_BINDING_MARKER_INVALID']);
/** Preserve closed reconciliation causes, never arbitrary manager messages. */
export function workspaceInspectionError(error){
  return inspectionErrors.has(error?.code)?{code:error.code,
    message:'Executor isolado: '+error.code+'. Evidências preservadas para diagnóstico.'}:null;
}

/** A closed request grammar. No caller-selected path, executable, policy or
 * action is forwarded to the privileged workspace manager. */
export async function workspaceInspection(method,params,io={}){
  const keys=method==='worker.inspectGeneration'?['workspace']:
    method==='worker.inspectPlanningSource'?['workspace','sourceGenerationKind']:
    ['worker.inspectSourceBinding','worker.inspectRetrySourceBinding'].includes(method)?['workspace','runId']:
    method==='worker.inspectPreparation'?['workspace','sourceWorkspace','preparationIdentity','expectedSourceDigest','expectedSourceSnapshotDigest',
      ...(Object.hasOwn(params??{},'sourceGenerationKind')?['sourceGenerationKind']:[])]:null;
  if(!keys||!params||typeof params!=='object'||Array.isArray(params)||Object.keys(params).length!==keys.length||
    keys.some(key=>typeof params[key]!=='string')||Object.keys(params).some(key=>!keys.includes(key))||!workspace.test(params.workspace))fail();
  const operation=io.operation??workspaceOperation;
  if(method==='worker.inspectGeneration')return operation({action:'inspect-generation',workspace:params.workspace});
  if(method==='worker.inspectPlanningSource'){
    if(!['root','snapshot'].includes(params.sourceGenerationKind))fail();
    return operation({action:'inspect-planning-source',...params});
  }
  if(method==='worker.inspectPreparation'){
    if(!workspace.test(params.sourceWorkspace)||!uuid.test(params.preparationIdentity)||
      !hash.test(params.expectedSourceDigest)||!hash.test(params.expectedSourceSnapshotDigest)||
      (Object.hasOwn(params,'sourceGenerationKind')&&params.sourceGenerationKind!=='unmarked'))fail();
    return operation({action:'inspect-preparation',...params});
  }
  if(!uuid.test(params.runId))fail();
  const retry=method==='worker.inspectRetrySourceBinding';
  return (retry?stoppedRetrySourceBindingProof:stoppedSourceBindingProof)(params.runId,params.workspace,{
    inspectUnit:io.inspectUnit??inspectUnit,
    inspectSourceBinding:(runId,workspace)=>operation({action:retry?'inspect-retry-source-binding':'inspect',workspace,sourceBindingRunId:runId}),
  });
}
