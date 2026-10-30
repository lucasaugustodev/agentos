import {expect,it} from 'vitest';
import {createHash} from 'node:crypto';
import {verifySourceOutputCandidate,sourceOutputValidation} from '../server/source-output-validation.js';

const hash=(value:string|Buffer)=>createHash('sha256').update(value).digest('hex');
const files=(values:Record<string,string>)=>Object.entries(values).map(([path,content])=>({path,sha256:hash(content)}));
const inventory=(values:ReturnType<typeof files>)=>hash(JSON.stringify(values.map(f=>[f.path,f.sha256]).sort((a,b)=>Buffer.compare(Buffer.from(a[0]),Buffer.from(b[0]))))
  .replace(/[\u0080-\uffff]/g,c=>'\\u'+c.charCodeAt(0).toString(16).padStart(4,'0')));
function candidate(before:Record<string,string>,after:Record<string,string>){
  const baseFiles=files(before),current=files(after);
  return Buffer.from(JSON.stringify({baseDigest:inventory(baseFiles),candidateDigest:inventory(current),baseFiles,files:current}));
}
const contract={version:1,required:[{path:'src/feature.ts',kind:'file'}],preserve:[{path:'src/winner.ts',kind:'file'}]};
const baseline={'src/winner.ts':'known-good\n','README.md':'keep\n'};
const output={...baseline,'src/feature.ts':'implemented\n'};
const check=(value:Buffer,raw:any=contract)=>verifySourceOutputCandidate(raw,value,hash(value));

it('verifies the canonical delivered files and retained baseline bytes',()=>{
  expect(check(candidate(baseline,output))).toMatchObject({passed:true,code:'SOURCE_OUTPUT_VERIFIED'});
});
it('rejects source left only in evidence even when a worker declares passed true',()=>{
  const value=candidate(baseline,baseline);
  expect(check(value)).toMatchObject({passed:false,checks:expect.arrayContaining([
    {path:'src/feature.ts',kind:'required',passed:false,code:'SOURCE_OUTPUT_REQUIRED_MISSING'}])});
});
it('does not accept a missing winner just because a new source file exists',()=>{
  expect(check(candidate(baseline,{'src/feature.ts':'implemented\n'})).passed).toBe(false);
});
it('rejects changed preserved files without accepting a model supplied replacement hash',()=>{
  expect(check(candidate(baseline,{...output,'src/winner.ts':'regression\n'}))).toMatchObject({passed:false,
    checks:expect.arrayContaining([expect.objectContaining({code:'SOURCE_OUTPUT_PRESERVATION_CHANGED'})])});
});
it('requires a preservation baseline instead of treating missing on both sides as preserved',()=>{
  expect(check(candidate({}, {'src/feature.ts':'ok'}))).toMatchObject({passed:false,
    checks:expect.arrayContaining([expect.objectContaining({code:'SOURCE_OUTPUT_PRESERVATION_BASE_MISSING'})])});
});
it('imports an exact reference through required sha256, without claiming it existed in the base',()=>{
  const value=candidate({}, {'src/winner.ts':'known-good\n'});
  expect(check(value,{version:1,required:[{path:'src/winner.ts',kind:'file',sha256:hash('known-good\n')}],preserve:[]}).passed).toBe(true);
  expect(check(value,{version:1,required:[{path:'src/winner.ts',kind:'file',sha256:hash('other')}],preserve:[]}).passed).toBe(false);
});
it('checks every preserved descendant and requires a real source file for directory outputs',()=>{
  const raw={version:1,required:[{path:'src/new',kind:'directory'}],preserve:[{path:'src/old',kind:'directory'}]};
  const before={'src/old/a.ts':'a','src/old/b.ts':'b'};
  expect(check(candidate(before,{...before,'src/new/a.ts':'new'}),raw).passed).toBe(true);
  expect(check(candidate(before,{'src/old/a.ts':'a','src/new/a.ts':'new'}),raw).passed).toBe(false);
  expect(check(candidate(before,{...before,'src/new-other/a.ts':'new'}),raw).passed).toBe(false);
});
it.each(['missing','wrong','duplicate'])('rejects a %s before inventory',mode=>{
  const raw=JSON.parse(candidate(baseline,output).toString());
  if(mode==='missing')delete raw.baseFiles;
  if(mode==='wrong')raw.baseFiles[0].sha256='f'.repeat(64);
  if(mode==='duplicate')raw.baseFiles.push(raw.baseFiles[0]);
  expect(check(Buffer.from(JSON.stringify(raw)))).toMatchObject({passed:false,code:'SOURCE_OUTPUT_BASE_INVENTORY_INVALID'});
});
it('rejects a corrupt candidate inventory independently of the contract checks',()=>{
  const raw=JSON.parse(candidate(baseline,output).toString());raw.files[0].sha256='f'.repeat(64);
  expect(check(Buffer.from(JSON.stringify(raw)))).toMatchObject({passed:false,code:'SOURCE_OUTPUT_CANDIDATE_INVALID'});
});
it('keeps report-only work explicit while still validating the candidate envelope',()=>{
  expect(check(candidate(baseline,baseline),{version:1,required:[],preserve:[],noSourceReason:'This task produces a diagnostic report without source changes.'}).passed).toBe(true);
  expect(check(candidate(baseline,baseline),{version:1,required:[],preserve:[]})).toMatchObject({passed:false,code:'SOURCE_OUTPUT_CONTRACT_INVALID'});
});

function snapshot(){
  const bytes=candidate(baseline,output),ref={id:'artifact',run_id:'run',name:'candidate.json',digest:hash(bytes),size:bytes.length};
  const execution={kind:'harness',workspace:'project/otarget',sourceWorkspace:'project/source',contract};
  return {task:{run_id:'run',artifacts:[ref],validation_input:{sourceOutput:{schema:1,current:structuredClone(execution),frozen:structuredClone(execution)}}},
    read:async()=>({...ref,bytes})};
}
it('leaves legacy validation unchanged',async()=>{
  expect(await sourceOutputValidation({run_id:'run',artifacts:[],validation_input:{}},async()=>{throw Error('must not read');})).toBeNull();
});
it.each(['contract','workspace','sourceWorkspace','kind'] as const)('refuses changed %s authority after admission',async(field)=>{
  const f=snapshot();(f.task.validation_input.sourceOutput.current as any)[field]=field==='contract'?null:'changed';
  expect(await sourceOutputValidation(f.task,f.read)).toMatchObject({passed:false,detail:'SOURCE_OUTPUT_FROZEN_CONTRACT_MISMATCH'});
});
it('rejects a duplicate or wrong-run candidate reference',async()=>{
  const f=snapshot();f.task.artifacts.push(f.task.artifacts[0]);
  expect(await sourceOutputValidation(f.task,f.read)).toMatchObject({passed:false,detail:'SOURCE_OUTPUT_CANDIDATE_IDENTITY'});
  f.task.artifacts.pop();f.task.artifacts[0].run_id='other';
  expect(await sourceOutputValidation(f.task,f.read)).toMatchObject({passed:false,detail:'SOURCE_OUTPUT_CANDIDATE_IDENTITY'});
});
