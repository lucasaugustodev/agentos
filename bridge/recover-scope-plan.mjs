// Collect a typed plan from an ended native worker; no model or shell task runs.
import {readFile,realpath} from 'node:fs/promises';
import {createHash} from 'node:crypto';
import {pathToFileURL} from 'node:url';
import {collectArtifacts} from './artifact-collector.mjs';
import {inspectUnit} from './process-stop.mjs';
import {workspaceOperation} from './workspaces.mjs';
export async function recoverScopePlan(id,options={}){
  if(!/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(id))throw Error('INVALID_RUN');
  const inspect=options.inspect??inspectUnit,operation=options.operation??workspaceOperation;
  const state=JSON.parse(await readFile((options.runRoot??'/var/lib/agentos-bridge/worker-runs')+'/'+id+'/state.json','utf8'));
  if(state.runId!==id || state.runtime!=='opencode' || !/^[a-z0-9_-]+\/o[a-z0-9_-]+$/.test(state.workspace))throw Error('RUN_IDENTITY_MISMATCH');
  if(state.state!=='completed' && !(state.state==='failed' && /^(RESULT_FORMAT|ARTIFACT_PATH|ARTIFACT_NAME|ARTIFACT_NAME_COLLISION|ARTIFACT_MISSING|RESERVED_ARTIFACT_NAME)$/.test(state.code)))throw Error('DELIVERY_NOT_RECOVERABLE');
  if(!(await inspect('agentos-run-'+id)).stopped)throw Error('STOP_UNCONFIRMED');
  const workspace=(options.workspaceRoot??'/srv/agentos/workspaces')+'/'+state.workspace;
  if(await realpath(workspace)!==workspace)throw Error('INVALID_WORKSPACE');
  const sourceInventory=state.sourceContractVersion===1?{includeBaseFiles:true}:{};
  const before=await operation({action:'inspect',workspace:state.workspace,...sourceInventory});
  // The contract names one exact file. No guessing from prose or other files.
  const artifacts=await collectArtifacts({artifacts:[{name:'scope-plan.json',path:'evidence/scope-plan.json',mime:'application/json'}]},workspace);
  const candidate=await operation({action:'inspect',workspace:state.workspace,...sourceInventory});
  if(before.candidateDigest!==candidate.candidateDigest)throw Error('SOURCE_CHANGED_DURING_COLLECTION');
  for(const [name,content] of [['candidate.json',JSON.stringify({...candidate,patch:undefined})],['changes.patch',candidate.patch]]){
    const bytes=Buffer.from(content);artifacts.push({name,mime:name.endsWith('.json')?'application/json':'text/plain',
      digest:createHash('sha256').update(bytes).digest('hex'),base64:bytes.toString('base64')});
  }
  const after=await inspect('agentos-run-'+id);if(!after.stopped)throw Error('STOP_UNCONFIRMED');
  return {runId:id,workspace:state.workspace,checkedAt:after.checkedAt,process:after,candidate:{...candidate,patch:undefined},
    artifacts,summary:'Plano recuperado do arquivo exato; contrato e referências ainda serão conferidos.',collectorVersion:'scoped-plan-files-v1'};
}
// Releases are invoked through /opt/agentos/current, which is a symlink.
// Node resolves import.meta.url to the release, while argv retains that alias.
if(process.argv[1] && import.meta.url===pathToFileURL(await realpath(process.argv[1])).href){
 try{console.log(JSON.stringify(await recoverScopePlan(process.argv[2])));}
 catch(e){console.log(JSON.stringify({error:e.code??e.message}));process.exitCode=1;}
}
