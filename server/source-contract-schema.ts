import {z} from 'zod';

/** Exact workspace-relative source paths. No aliases, globs or attempt-local
 * evidence are authority for the canonical candidate inventory. */
export const sourceContractPathSchema=z.string().min(1).max(300).refine(path=>{
  if(path!==path.trim()||path.startsWith('/')||path.endsWith('/')||path.includes('\\')||
    path.includes('//')||/[\u0000-\u001f\u007f:*?"<>|]/.test(path))return false;
  return path.split('/').every(part=>part!=='.'&&part!=='..'&&
    !['.git','node_modules','assets','evidence','data','.agentos-snapshot.json','.local-test','test-results','playwright-report','__pycache__','.ssh','.aws','.codex','.claude'].includes(part)&&
    !part.startsWith('.env'));
},'Use an exact canonical source path outside reserved and attempt-local directories.');

export const sourceContractEntrySchema=z.object({path:sourceContractPathSchema,
  kind:z.enum(['file','directory']),sha256:z.string().regex(/^[a-f0-9]{64}$/).optional()}).strict()
  .refine(entry=>!entry.sha256||entry.kind==='file','Only exact files may declare a SHA-256.')
  .refine(entry=>entry.kind!=='file'||(!entry.path.endsWith('.log')&&!entry.path.endsWith('.pyc')),'Runtime inventories exclude .log and .pyc files.');
export const sourceContractSchema=z.object({version:z.literal(1),
  required:z.array(sourceContractEntrySchema).max(128),
  preserve:z.array(sourceContractEntrySchema).max(128),
  noSourceReason:z.string().trim().min(20).max(1000).optional()}).strict().superRefine((contract,ctx)=>{
  if(!contract.required.length&&!contract.noSourceReason)
    ctx.addIssue({code:'custom',path:['noSourceReason'],message:'An empty source output list requires an explicit report-only justification.'});
  if(contract.required.length&&contract.noSourceReason)
    ctx.addIssue({code:'custom',path:['noSourceReason'],message:'A source-producing contract cannot also declare report-only output.'});
  for(const field of ['required','preserve'] as const){
    const seen=new Set<string>();
    contract[field].forEach((entry,index)=>{
      if(seen.has(entry.path))ctx.addIssue({code:'custom',path:[field,index,'path'],message:'Duplicate source contract path.'});
      seen.add(entry.path);
    });
  }
});
export type SourceContract=z.infer<typeof sourceContractSchema>;
export type SourceContractEntry=z.infer<typeof sourceContractEntrySchema>;

export function sourcePathWithin(path:string,scope:string){
  const prefix=scope.endsWith('/')?scope.slice(0,-1):scope;
  return path===prefix||path.startsWith(prefix+'/');
}

/** Stable ordering only: normalization never fixes aliases or expands scope. */
export function normalizeSourceContract(raw:unknown):SourceContract {
  const parsed=sourceContractSchema.parse(raw);
  const ordered=(items:SourceContractEntry[])=>[...items].sort((a,b)=>a.path.localeCompare(b.path)||a.kind.localeCompare(b.kind));
  return {...parsed,required:ordered(parsed.required),preserve:ordered(parsed.preserve)};
}

/** Preserve all contributor obligations; differing exact hashes are a planning
 * conflict, not permission to pick one contributor's result. */
export function combineSourceContracts(contracts:SourceContract[]):SourceContract {
  if(contracts.length===1)return normalizeSourceContract(contracts[0]);
  const combine=(field:'required'|'preserve')=>{
    const entries=new Map<string,SourceContractEntry>();
    for(const contract of contracts)for(const entry of contract[field]){
      const old=entries.get(entry.path);
      if(old&&(old.kind!==entry.kind||(old.sha256&&entry.sha256&&old.sha256!==entry.sha256)))
        throw Error('SOURCE_CONTRACT_CONFLICT');
      entries.set(entry.path,{...entry,...(old?.sha256?{sha256:old.sha256}:{})});
    }
    return [...entries.values()];
  };
  const required=combine('required');
  return normalizeSourceContract({version:1,required,preserve:combine('preserve'),
    ...(!required.length?{noSourceReason:'This candidate has only explicitly declared report-only contributions; source delivery is not claimed.'}:{})});
}

export function sourceContractInstruction(contract:SourceContract){
  return 'FROZEN SOURCE OUTPUT CONTRACT: '+JSON.stringify(contract)+
    '\nRequired source files belong in /workspace at the exact declared paths and within the existing write scope. evidence/ and data/ are attempt-local reports, fixtures or temporary state; code placed there cannot satisfy a required source output. A directory output needs at least one canonical source file. Preserve entries must already exist in the baseline and retain their original file bytes; an optional file SHA-256 is an additional exact obligation. To import a missing reference use required with its exact file hash, never preserve. Do not widen writePaths or reinterpret a report as source. Read-only verification checks these obligations without modifying source.';
}
