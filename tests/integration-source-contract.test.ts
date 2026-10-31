import {createHash} from 'node:crypto';
import {expect,it} from 'vitest';
import {compileDelivery} from '../server/incremental-planning.js';
import {objectiveSchema,validatePlan} from '../server/objective-contracts.js';
import {verifySourceOutputCandidate} from '../server/source-output-validation.js';
import {composeSourceContracts,type SourceContract} from '../server/source-contract-schema.js';

const hash=(value:string|Buffer)=>createHash('sha256').update(value).digest('hex');
const file=(path:string,sha256?:string)=>({path,kind:'file' as const,...(sha256?{sha256}:{})});
const directory=(path:string)=>({path,kind:'directory' as const});
const baseline={'src/a.ts':'old a','src/b.ts':'old b','src/keep.ts':'retained','src/legacy/old.ts':'legacy'};
const integrated={...baseline,'src/a.ts':'new a','src/b.ts':'new b'};
function candidate(before:Record<string,string>,after:Record<string,string>){
  const files=(values:Record<string,string>)=>Object.entries(values).sort(([a],[b])=>Buffer.compare(Buffer.from(a),Buffer.from(b)))
    .map(([path,content])=>({path,sha256:hash(content)}));
  const baseFiles=files(before),current=files(after),digest=(items:ReturnType<typeof files>)=>hash(JSON.stringify(items.map(f=>[f.path,f.sha256])));
  return Buffer.from(JSON.stringify({baseDigest:digest(baseFiles),candidateDigest:digest(current),baseFiles,files:current}));
}
const request=objectiveSchema.parse({message:'Compose the two bounded contributions without changing other source.',project:'test',mode:'execute',
  workspaces:['test/base'],requirements:['The integrated source contains both contributions and preserves unrelated files.'],
  planningStrategy:'incremental',sourceContractVersion:1,idempotencyKey:'integration-preserve-fixture'});
const agents=[{id:'engineering.backend.node',runtime:'harness'},{id:'engineering.integration',runtime:'harness'},{id:'qa.integration',runtime:'harness'}];
const producer=(key:string,other:string)=>({kind:'delivery' as const,key,requirementId:'req-1',title:'Implement source '+key,
  action:'Implement this bounded source contribution while preserving the other producer and unrelated source.',
  verification:'Verify the actual canonical inventory and the independent behavior of this contribution.',
  agentId:'engineering.backend.node',writePaths:[`src/${key}.ts`],sourceContract:{version:1 as const,
    required:[file(`src/${key}.ts`)],preserve:[file(`src/${other}.ts`),file('src/keep.ts',hash('retained')),directory('src/legacy')]}});
const proof=(contract:unknown,before:Record<string,string>,after:Record<string,string>)=>{
  const bytes=candidate(before,after);return verifySourceOutputCandidate(contract,bytes,hash(bytes));
};
const owner=(contract:SourceContract,writePaths:string[])=>({contract,writePaths});
const fixtureOwners=()=>[producer('a','b'),producer('b','a')].map(p=>owner(p.sourceContract,p.writePaths));

it('composes two exclusive producers over the parent inventory while preserving all unrelated source',()=>{
  const a=producer('a','b'),b=producer('b','a');
  const plan=compileDelivery({deliveries:[a,b]},request);
  expect(()=>validatePlan(plan,request,agents,[])).not.toThrow();
  const contract=plan.tasks.find(t=>t.key==='integrate')!.sourceContract!;
  // Each producer must retain its peer's original bytes in its own candidate.
  expect(proof(a.sourceContract,baseline,{...baseline,'src/a.ts':'new a'}).passed).toBe(true);
  expect(proof(a.sourceContract,baseline,integrated).passed).toBe(false);
  expect(proof(b.sourceContract,baseline,{...baseline,'src/b.ts':'new b'}).passed).toBe(true);
  // The integrator must then be able to apply both accepted deltas together.
  expect(proof(contract,baseline,integrated).passed).toBe(true);
  expect(contract.required).toEqual([file('src/a.ts'),file('src/b.ts')]);
  expect(contract.preserve).toEqual([file('src/keep.ts',hash('retained')),directory('src/legacy')]);
  expect(proof(contract,baseline,{...integrated,'src/keep.ts':'regressed'}).passed).toBe(false);
  expect(proof(contract,baseline,{...integrated,'src/legacy/old.ts':'regressed'}).passed).toBe(false);
  const missing={...integrated};delete (missing as Partial<typeof missing>)['src/b.ts'];
  expect(proof(contract,baseline,missing).passed).toBe(false);
  const qa=plan.tasks.find(t=>t.key==='verify')!;
  expect(qa.sourceContract).toEqual(contract);
  expect(proof(qa.sourceContract,integrated,integrated).passed).toBe(true);
  expect(a.sourceContract.preserve).toContainEqual(file('src/b.ts'));
  expect(b.sourceContract.preserve).toContainEqual(file('src/a.ts'));
});

it('uses the same ownership composition for dependency joins and final QA without weakening a producer validator',()=>{
  const a=producer('a','b'),b=producer('b','a');
  const consumer={...producer('c','a'),dependsOn:['a','b']};
  consumer.sourceContract.preserve.push(file('src/b.ts'));
  const validation={kind:'validation',key:'check-a',validationTargetKey:'a',requirementId:'req-1',
    title:'Validate producer a',action:'Inspect the exact producer candidate without modifying any source file.',
    verification:'Check the canonical producer inventory and preserve the independent evidence.',agentId:'qa.integration',writePaths:['evidence']};
  const plan=compileDelivery({deliveries:[a,b,consumer,validation]},request);
  expect(()=>validatePlan(plan,request,agents,[])).not.toThrow();
  const task=(key:string)=>plan.tasks.find(t=>t.key===key)!;
  expect(proof(task('join-3').sourceContract,baseline,integrated).passed).toBe(true);
  const after={...integrated,'src/c.ts':'new c'};
  expect(proof(task('integrate').sourceContract,baseline,after).passed).toBe(true);
  expect(task('verify').sourceContract).toEqual(task('integrate').sourceContract);
  expect(task('validate-4').sourceContract).toEqual(a.sourceContract);
  expect(proof(task('validate-4').sourceContract,baseline,integrated).passed).toBe(false);
});

it('rejects a plan that drops unrelated preservation or a declared integrated output',()=>{
  for(const field of ['preserve','required'] as const){
    const plan=compileDelivery({deliveries:[producer('a','b'),producer('b','a')]},request);
    const contract=plan.tasks.find(t=>t.key==='integrate')!.sourceContract!;
    contract[field]=contract[field].slice(1);
    expect(()=>validatePlan(plan,request,agents,[])).toThrowError(expect.objectContaining({
      detail:expect.objectContaining({violation:'SOURCE_CONTRACT_OBLIGATION_DROPPED'})}));
  }
});

it('keeps a producer own preservation even when peers also preserve that output',()=>{
  const owners=fixtureOwners();owners[0].contract.preserve.push(file('src/a.ts'));
  const contract=composeSourceContracts(owners);
  expect(contract.preserve).toContainEqual(file('src/a.ts'));
  expect(proof(contract,baseline,integrated).passed).toBe(false);
  expect(proof(contract,baseline,{...integrated,'src/a.ts':'old a'}).passed).toBe(true);
});

it('retains explicit preservation hashes and rejects conflicting exact reference hashes',()=>{
  const owners=fixtureOwners();owners[0].contract.preserve[0]=file('src/b.ts',hash('old b'));
  const contract=composeSourceContracts(owners);
  expect(contract.preserve).toContainEqual(file('src/b.ts',hash('old b')));
  expect(proof(contract,baseline,integrated).passed).toBe(false);
  owners[1].contract.required[0]=file('src/b.ts',hash('new b'));
  expect(()=>composeSourceContracts(owners)).toThrow();
});

it('does not transfer preservation merely because another contribution has a writable scope',()=>{
  const a=producer('a','b');
  const report:SourceContract={version:1,required:[],preserve:[],noSourceReason:'This contribution records observations without a declared source output.'};
  const contract=composeSourceContracts([owner(a.sourceContract,a.writePaths),owner(report,['src/b.ts','evidence/'])]);
  expect(contract.preserve).toContainEqual(file('src/b.ts'));
  expect(proof(contract,baseline,integrated).passed).toBe(false);
});

it.each(['same-scope','parent-scope','unowned-output','unsafe-scope'])('rejects %s ownership instead of discarding preservation',mode=>{
  const owners=fixtureOwners();
  if(mode==='same-scope')owners[1].writePaths.push('src/a.ts');
  if(mode==='parent-scope')owners[1].writePaths=['src/'];
  if(mode==='unowned-output')owners[1].writePaths=['src/elsewhere.ts'];
  if(mode==='unsafe-scope')owners[1].writePaths=['src//'];
  expect(()=>composeSourceContracts(owners)).toThrow('SOURCE_CONTRACT_CONFLICT');
});

it.each(['preserved-parent','required-parent','kind-mismatch'])('rejects %s directory ambiguity without weakening its other descendants',mode=>{
  const owners=fixtureOwners();
  if(mode==='preserved-parent')owners[0].contract.preserve=[directory('src')];
  if(mode==='required-parent'){
    owners[0].contract.preserve=[file('other/keep.ts')];
    owners[1].contract.required=[directory('other')];owners[1].writePaths=['other/'];
  }
  if(mode==='kind-mismatch')owners[0].contract.preserve[0]=directory('src/b.ts');
  expect(()=>composeSourceContracts(owners)).toThrow('SOURCE_CONTRACT_CONFLICT');
});

it('supports a whole declared directory with one owner while retaining unrelated directories',()=>{
  const a:SourceContract={version:1,required:[directory('src/a')],preserve:[directory('src/b'),directory('src/legacy')]};
  const b:SourceContract={version:1,required:[directory('src/b')],preserve:[directory('src/a'),directory('src/legacy')]};
  const contract=composeSourceContracts([owner(a,['src/a/']),owner(b,['src/b/'])]);
  const before={'src/a/old.ts':'a','src/b/old.ts':'b','src/legacy/old.ts':'retain'};
  const after={'src/a/new.ts':'new a','src/b/new.ts':'new b','src/legacy/old.ts':'retain'};
  expect(proof(contract,before,after).passed).toBe(true);
  expect(proof(contract,before,{...after,'src/legacy/old.ts':'regressed'}).passed).toBe(false);
});
