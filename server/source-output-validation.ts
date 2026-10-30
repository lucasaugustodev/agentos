import {createHash} from 'node:crypto';
import {runtimeCandidateDigest} from './operation-progress.js';
import {normalizeSourceContract,type SourceContractEntry} from './source-contract-schema.js';

const hash=(value:string|Buffer)=>createHash('sha256').update(value).digest('hex');
type File={path:string;sha256:string};
export type SourceOutputCheck={path:string;kind:'required'|'preserve';passed:boolean;code:string};
export type SourceOutputProof={passed:boolean;code:string;checks:SourceOutputCheck[];candidateDigest?:string;contractDigest?:string};
const invalid=(code:string):SourceOutputProof=>({passed:false,code,checks:[]});

/** Inspect canonical runtime inventories, never the worker's passed flag or a
 * source tree embedded in an evidence artifact. No filesystem/provider I/O. */
export function verifySourceOutputCandidate(rawContract:unknown,bytes:Buffer,artifactDigest:string):SourceOutputProof {
  let contract;
  try{contract=normalizeSourceContract(rawContract);}catch{return invalid('SOURCE_OUTPUT_CONTRACT_INVALID');}
  const candidateDigest=runtimeCandidateDigest(bytes,artifactDigest);
  if(!candidateDigest)return invalid('SOURCE_OUTPUT_CANDIDATE_INVALID');
  const candidate=JSON.parse(bytes.toString('utf8'));
  // The complete before inventory comes from the protected runtime baseline.
  // A digest alone cannot prove which particular source files were retained.
  const beforeBytes=Buffer.from(JSON.stringify({baseDigest:candidate.baseDigest,candidateDigest:candidate.baseDigest,files:candidate.baseFiles}));
  if(!runtimeCandidateDigest(beforeBytes,hash(beforeBytes)))return invalid('SOURCE_OUTPUT_BASE_INVENTORY_INVALID');
  const before=new Map<string,string>(candidate.baseFiles.map((f:File)=>[f.path,f.sha256]));
  const after=new Map<string,string>(candidate.files.map((f:File)=>[f.path,f.sha256]));
  const matches=(files:Map<string,string>,entry:SourceContractEntry)=>entry.kind==='file'
    ? (files.has(entry.path)?[[entry.path,files.get(entry.path)!] as const]:[])
    : [...files.entries()].filter(([p])=>p.startsWith(entry.path+'/'));
  const checks:SourceOutputCheck[]=[];
  for(const entry of contract.required){
    const found=matches(after,entry);
    const code=!found.length?'SOURCE_OUTPUT_REQUIRED_MISSING':entry.sha256&&found[0][1]!==entry.sha256?'SOURCE_OUTPUT_REQUIRED_HASH_MISMATCH':'SOURCE_OUTPUT_PRESENT';
    checks.push({path:entry.path,kind:'required',passed:code==='SOURCE_OUTPUT_PRESENT',code});
  }
  for(const entry of contract.preserve){
    const original=matches(before,entry);
    const code=!original.length?'SOURCE_OUTPUT_PRESERVATION_BASE_MISSING'
      : entry.sha256&&original[0][1]!==entry.sha256?'SOURCE_OUTPUT_PRESERVATION_BASE_HASH_MISMATCH'
      : original.some(([p,digest])=>after.get(p)!==digest)?'SOURCE_OUTPUT_PRESERVATION_CHANGED':'SOURCE_OUTPUT_PRESERVED';
    checks.push({path:entry.path,kind:'preserve',passed:code==='SOURCE_OUTPUT_PRESERVED',code});
  }
  return {passed:checks.every(c=>c.passed),code:checks.every(c=>c.passed)?'SOURCE_OUTPUT_VERIFIED':'SOURCE_OUTPUT_REQUIREMENTS_FAILED',
    checks,candidateDigest,contractDigest:hash(JSON.stringify(contract))};
}

type Snapshot={run_id:string;artifacts:any[];validation_input:any};
type Reader=(id:string)=>Promise<{id?:string;run_id:string;name:string;digest:string;size:number;bytes:Buffer}>;
export async function sourceOutputValidation(snapshot:Snapshot,read:Reader){
  const authority=snapshot.validation_input.sourceOutput;
  if(authority===undefined)return null;
  const result=(proof:SourceOutputProof)=>({id:'source-output',label:'Arquivos e preservação da candidata',type:'source_contract',
    passed:proof.passed,detail:proof.passed?'Arquivos obrigatórios e preservação conferidos na candidata real; demais critérios continuam obrigatórios.'
      :proof.code+(proof.checks.length?': '+proof.checks.filter(c=>!c.passed).slice(0,16).map(c=>c.path+' ('+c.code+')').join('; '):'')});
  try{
    const current=authority.current,frozen=authority.frozen;
    if(authority.schema!==1||!current||!frozen||current.kind!=='harness'||frozen.kind!=='harness'||
      !current.workspace||!current.sourceWorkspace||current.workspace!==frozen.workspace||current.sourceWorkspace!==frozen.sourceWorkspace||
      !current.contract||!frozen.contract||
      JSON.stringify(normalizeSourceContract(current.contract))!==JSON.stringify(normalizeSourceContract(frozen.contract)))
      return result(invalid('SOURCE_OUTPUT_FROZEN_CONTRACT_MISMATCH'));
    const refs=snapshot.artifacts.filter(a=>a.name==='candidate.json');
    if(refs.length!==1)return result(invalid('SOURCE_OUTPUT_CANDIDATE_IDENTITY'));
    const ref=refs[0],artifact=await read(ref.id);
    if(artifact.run_id!==snapshot.run_id||ref.run_id!==snapshot.run_id||artifact.name!=='candidate.json'||
      artifact.digest!==ref.digest||artifact.size!==ref.size||artifact.bytes.length!==ref.size||
      (artifact.id!==undefined&&artifact.id!==ref.id))return result(invalid('SOURCE_OUTPUT_CANDIDATE_IDENTITY'));
    return result(verifySourceOutputCandidate(frozen.contract,artifact.bytes,ref.digest));
  }catch{return result(invalid('SOURCE_OUTPUT_EVIDENCE_INVALID'));}
}
