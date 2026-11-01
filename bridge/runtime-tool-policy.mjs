import {open} from 'node:fs/promises';
import {constants} from 'node:fs';
import {createHash} from 'node:crypto';

const UUID=/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/;
const KEY=/^[a-z0-9_-]{1,60}$/;
const PROJECT=/^[a-z0-9_-]{1,80}$/;
const POLICY='/var/lib/agentos-bridge/supervisor-tool-policy.json';
const fail=()=>{throw Object.assign(Error('SUPERVISOR_TOOL_POLICY_INVALID'),{code:'SUPERVISOR_TOOL_POLICY_INVALID'});};
const worker=()=>({version:1,role:'worker',bashTimeoutMs:300000});
const keys=(value,expected)=>value&&typeof value==='object'&&!Array.isArray(value)&&
  Object.keys(value).sort().join(',')===expected.split(',').sort().join(',');
const canonical=value=>Array.isArray(value)?value.map(canonical):value&&typeof value==='object'?
  Object.fromEntries(Object.keys(value).sort().map(key=>[key,canonical(value[key])])):value;
export const toolPolicyDefinitionDigest=value=>createHash('sha256').update(JSON.stringify(canonical(value))).digest('hex');

/** Local operator configuration, never an agent-writable prompt or tool argument.
 * The SSH controller is the dispatch authority. Its cycle manifest is generated
 * from the persisted occurrence, step and attempt, not from occurrence.input. */
export function selectRuntimeToolPolicy(params,policy,policyDigest){
  if(policy===null)return worker();
  if(!keys(policy,'version,supervisors')||policy.version!==1||!Array.isArray(policy.supervisors)||
    policy.supervisors.length>32||!/^[0-9a-f]{64}$/.test(policyDigest??''))fail();
  const identities=new Set();
  for(const binding of policy.supervisors){
    if(!keys(binding,'project,cycleId,definitionVersion,step,definitionSHA256,definition')||!PROJECT.test(binding.project??'')||
      !UUID.test(binding.cycleId??'')||!Number.isSafeInteger(binding.definitionVersion)||binding.definitionVersion<1||
      !KEY.test(binding.step??''))fail();
    // Registration seals the API's immutable definition snapshot. The existing
    // dispatch carries its cycle/version tuple, not a definition hash. Do not
    // claim a live digest comparison that the old controller cannot supply.
    if(!/^[0-9a-f]{64}$/.test(binding.definitionSHA256??'')||
      toolPolicyDefinitionDigest(binding.definition)!==binding.definitionSHA256||
      binding.definition?.project!==binding.project||!Array.isArray(binding.definition.steps)||
      binding.definition.steps.filter(s=>s.key===binding.step&&s.kind==='agent'&&s.readOnly===true).length!==1)fail();
    const identity=JSON.stringify([binding.project,binding.cycleId,binding.definitionVersion,binding.step]);
    if(identities.has(identity))fail();
    identities.add(identity);
  }
  const manifest=params.context?.manifest;
  const binding=policy.supervisors.find(b=>manifest?.source==='cycle'&&manifest.cycleId===b.cycleId&&
    manifest.definitionVersion===b.definitionVersion&&manifest.step===b.step);
  if(!binding)return worker();
  const input=manifest.cycleInput;
  if(params.managed!==true||params.readOnlySource!==true||!UUID.test(params.runId??'')||
    !UUID.test(params.runFence??'')||!UUID.test(manifest.occurrenceId??'')||
    params.workspace!==binding.project+'/oc'+manifest.occurrenceId.replaceAll('-','').slice(0,18)+'-'+binding.step||
    !input||input.id!==params.runId||input.attemptId!==params.runId||input.runFence!==params.runFence||
    input.occurrenceId!==manifest.occurrenceId||input.name!=='cycle-input.json'||
    input.path!=='/home/agent/inputs/'+params.runId+'/cycle-input.json'||
    !/^[0-9a-f]{64}$/.test(input.digest??'')||input.algorithm!=='sha256'||input.encoding!=='utf-8'||
    !Number.isSafeInteger(input.size)||input.size<0||input.size>8*1024*1024||
    !params.inputs?.some(i=>i.id===input.id&&i.name===input.name&&i.digest===input.digest))
    return {...worker(),diagnostic:'SUPERVISOR_BINDING_UNPROVEN'};
  return {version:1,role:'supervisor',bashTimeoutMs:60000,policyDigest,
    binding:{project:binding.project,cycleId:binding.cycleId,definitionVersion:binding.definitionVersion,
      definitionSHA256:binding.definitionSHA256,step:binding.step,
      occurrenceId:manifest.occurrenceId,runId:params.runId,runFence:params.runFence}};
}

export async function runtimeToolPolicy(params,filename=POLICY){
  let file;
  try{file=await open(filename,constants.O_RDONLY|constants.O_NOFOLLOW);}
  catch(error){return error.code==='ENOENT'?worker():{...worker(),diagnostic:'SUPERVISOR_POLICY_UNAVAILABLE'};}
  try{
    const stat=await file.stat();
    if(!stat.isFile()||stat.uid!==0||(stat.mode&0o077)!==0||stat.size>262144)fail();
    const bytes=await file.readFile();
    if(bytes.length>262144)fail();
    let policy;try{policy=JSON.parse(bytes);}catch{fail();}
    return selectRuntimeToolPolicy(params,policy,createHash('sha256').update(bytes).digest('hex'));
  }catch{
    // An unproved restriction must not be reported as installed or prevent an
    // otherwise authorized legacy worker from executing.
    return {...worker(),diagnostic:'SUPERVISOR_POLICY_UNAVAILABLE'};
  }finally{await file.close();}
}

/** Only the owning bridge places this value in the fresh, cleared sandbox env. */
export function supervisorToolPatchArgs(profile){
  if(profile===undefined)return [];
  if(profile!=='supervisor-v1')fail();
  return ['--patch','/opt/agentos-bridge/supervisor.patch.yml'];
}
