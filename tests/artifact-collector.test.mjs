import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, writeFile, rm, symlink } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { collectArtifacts, parseArtifactResult } from '../bridge/artifact-collector.mjs';
import { finalSessionResponse, verifyCompletedNativeResult, readCompletedNativeResult } from '../bridge/recover-artifacts.mjs';
import {recoverScopePlan} from '../bridge/recover-scope-plan.mjs';
import {randomUUID, createHash} from 'node:crypto';
import {spawnSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';

function completedNativeFixture() {
 const hash = value => createHash('sha256').update(value).digest('hex');
 const identity = { runId: randomUUID(), workspace: 'test/completed-native' };
 const candidate = { baseDigest: hash('base'), candidateDigest: hash('candidate'),
   files: [{ path: 'qa-protocol/real.test.mjs', sha256: hash('real test') }], patch: 'exact runtime patch\n' };
 const contents = {
   'task-specific-report.json': '{"passed":false,"limitations":["delivery remains pending"]}',
   'protocol-report.json': '{"suiteExecuted":true,"passed":false,"exitCode":1}',
   'candidate.json': JSON.stringify({ ...candidate, patch: undefined }),
   'changes.patch': candidate.patch,
 };
 const artifacts = Object.entries(contents).map(([name, value]) => ({ name, mime: 'text/plain',
   digest: hash(value), base64: Buffer.from(value).toString('base64') }));
 const state = { ...identity, runtime: 'opencode', state: 'completed', stopped: true, exitCode: 0,
   artifacts: artifacts.map(({ base64, ...metadata }) => metadata) };
 const result = { summary: 'Useful evidence, final delivery not approved.', artifacts,
   evidence: { runtime: 'opencode', requests: 17, toolCalls: 43, exitCode: 0, finishReason: 'stop', stopped: true } };
 return { state, result, candidate, identity };
}

test('completed native recovery uses persisted result.json without any response.txt and preserves every original byte', async () => {
 const f = completedNativeFixture(), root = await mkdtemp(path.join(tmpdir(), 'native-completed-result-'));
 try {
   await writeFile(path.join(root, 'result.json'), JSON.stringify(f.result));
   const recovered = await readCompletedNativeResult(root, f.state, f.candidate, f.identity);
   assert.deepEqual(recovered.artifacts, f.result.artifacts);
   assert.equal(recovered.summary, f.result.summary);
   assert.equal(JSON.parse(Buffer.from(recovered.artifacts[0].base64, 'base64').toString()).passed, false);
   assert.equal(recovered.artifacts.filter(a => a.name === 'candidate.json').length, 1);
 } finally { await rm(root, { recursive: true, force: true }); }
});

test('completed native recovery rejects changed bytes, digest substitution and noncanonical base64', () => {
 const f = completedNativeFixture();
 for (const mutate of [
   r => { r.artifacts[0].base64 = Buffer.from('forged acceptance').toString('base64'); },
   r => { r.artifacts[0].digest = '0'.repeat(64); },
   r => { r.artifacts[0].base64 += '\n'; },
 ]) {
   const result = structuredClone(f.result); mutate(result);
   assert.throws(() => verifyCompletedNativeResult(f.state, result, f.candidate, f.identity), /ARTIFACT_CORRUPT|COMPLETED_ARTIFACTS_CHANGED/);
 }
});

test('completed native recovery requires exact manifest equality, including missing, extra and duplicate outputs', () => {
 const f = completedNativeFixture();
 for (const mutate of [
   r => r.artifacts.pop(),
   r => r.artifacts.push({ ...r.artifacts[0], name: 'unreported.json' }),
   r => { r.artifacts[1] = { ...r.artifacts[0] }; },
 ]) {
   const result = structuredClone(f.result); mutate(result);
   assert.throws(() => verifyCompletedNativeResult(f.state, result, f.candidate, f.identity), /COMPLETED_ARTIFACTS_CHANGED/);
 }
 const state = structuredClone(f.state); state.artifacts[1] = { ...state.artifacts[0] };
 assert.throws(() => verifyCompletedNativeResult(state, f.result, f.candidate, f.identity), /COMPLETED_ARTIFACTS_CHANGED/);
});

test('completed native recovery binds the recorded candidate and patch to the current workspace', () => {
 const f = completedNativeFixture();
 for (const changes of [
   { candidateDigest: 'a'.repeat(64) }, { baseDigest: 'b'.repeat(64) },
   { files: [] }, { patch: 'new actor changed the patch' },
 ]) assert.throws(() => verifyCompletedNativeResult(f.state, f.result, { ...f.candidate, ...changes }, f.identity), /SOURCE_CHANGED_DURING_COLLECTION/);
});

test('completed native recovery refuses a foreign run, an unfinished process or an unsuccessful runtime', () => {
 const f = completedNativeFixture();
 for (const changes of [{ runId: randomUUID() }, { workspace: 'test/foreign' }, { state: 'failed' }, { stopped: false }, { exitCode: 1 }])
   assert.throws(() => verifyCompletedNativeResult({ ...f.state, ...changes }, f.result, f.candidate, f.identity), /NATIVE_COMPLETION_UNCONFIRMED/);
 for (const changes of [{ runtime: 'harness' }, { stopped: false }, { exitCode: 1 }, { finishReason: 'length' }])
   assert.throws(() => verifyCompletedNativeResult(f.state, { ...f.result, evidence: { ...f.result.evidence, ...changes } }, f.candidate, f.identity), /NATIVE_COMPLETION_UNCONFIRMED/);
 assert.throws(() => verifyCompletedNativeResult(f.state, { ...f.result, runId: randomUUID() }, f.candidate, f.identity), /NATIVE_COMPLETION_UNCONFIRMED/);
});

test('scope-plan collector CLI emits its typed result when invoked through a release symlink',async()=>{
 const root=await mkdtemp(path.join(tmpdir(),'scope-plan-cli-'));
 try{
  const alias=path.join(root,'current.mjs');
  await symlink(fileURLToPath(new URL('../bridge/recover-scope-plan.mjs',import.meta.url)),alias);
  const run=spawnSync(process.execPath,[alias,'invalid-run'],{encoding:'utf8',timeout:10000});
  assert.equal(run.status,1);
  assert.deepEqual(JSON.parse(run.stdout),{error:'INVALID_RUN'});
 }finally{await rm(root,{recursive:true,force:true})}
});

async function planFixture(){
 const root=await mkdtemp(path.join(tmpdir(),'scope-plan-collection-')),id=randomUUID(),workspace='test/o-plan-'+id;
 await mkdir(path.join(root,'runs',id),{recursive:true});await mkdir(path.join(root,'workspaces',workspace,'evidence'),{recursive:true});
 await writeFile(path.join(root,'runs',id,'state.json'),JSON.stringify({runId:id,runtime:'opencode',workspace,state:'failed',code:'RESULT_FORMAT'}));
 const bytes='{"version":1,"tasks":[],"blockers":[]}';
 await writeFile(path.join(root,'workspaces',workspace,'evidence','scope-plan.json'),bytes);
 const options={runRoot:path.join(root,'runs'),workspaceRoot:path.join(root,'workspaces'),
  inspect:async(unit)=>({unit,stopped:true,checkedAt:new Date().toISOString(),mainPid:0,populated:false,activeState:'inactive'}),
  operation:async()=>({candidateDigest:'a'.repeat(64),baseDigest:'b'.repeat(64),patch:''})};
 return {root,id,workspace,bytes,options};
}
test('recovers exact scope-plan bytes after RESULT_FORMAT without a test suite or final JSON envelope',async()=>{
 const f=await planFixture();try{
  const delivery=await recoverScopePlan(f.id,f.options);
  assert.equal(delivery.collectorVersion,'scoped-plan-files-v1');
  assert.equal(Buffer.from(delivery.artifacts.find(a=>a.name==='scope-plan.json').base64,'base64').toString(),f.bytes);
  assert.equal(delivery.artifacts.some(a=>a.name==='protocol-report.json'),false);
  assert.equal(delivery.artifacts.some(a=>a.name==='runtime-plan-check.json'),false);
 }finally{await rm(f.root,{recursive:true,force:true})}
});
test('plan collection rejects a live worker, changed candidate and file outside its workspace',async()=>{
 const f=await planFixture();try{
  await assert.rejects(recoverScopePlan(f.id,{...f.options,inspect:async()=>({stopped:false})}),/STOP_UNCONFIRMED/);
  let calls=0;await assert.rejects(recoverScopePlan(f.id,{...f.options,operation:async()=>({candidateDigest:String(++calls),patch:''})}),/SOURCE_CHANGED_DURING_COLLECTION/);
  if(process.platform!=='win32'){
   const exact=path.join(f.root,'workspaces',f.workspace,'evidence','scope-plan.json');
   await rm(exact);await writeFile(path.join(f.root,'outside.json'),f.bytes);await symlink(path.join(f.root,'outside.json'),exact);
   await assert.rejects(recoverScopePlan(f.id,f.options),{code:'ARTIFACT_PATH'});
  }
 }finally{await rm(f.root,{recursive:true,force:true})}
});

test('incident: descriptive labels become real filenames and preserve exact bytes', async () => {
  const root=await mkdtemp(path.join(tmpdir(),'artifact-labels-'));
  try {
    await mkdir(path.join(root,'evidence'));
    await writeFile(path.join(root,'evidence/change.diff'),'real patch');
    await writeFile(path.join(root,'evidence/verification.json'),'{"passed":false}');
    const result=await collectArtifacts({artifacts:[
      {name:'Patch de estado e matemática',path:'evidence/change.diff'},
      {name:'Verification JSON',path:'/workspace/evidence/verification.json'},
    ]},root);
    assert.deepEqual(result.map(a=>a.name),['change.diff','verification.json']);
    assert.equal(Buffer.from(result[1].base64,'base64').toString(),'{"passed":false}');
    const [alias]=await collectArtifacts({artifacts:[{name:'required.json',path:'evidence/verification.json'}]},root);
    assert.equal(alias.name,'required.json');
    await assert.rejects(collectArtifacts({artifacts:[{name:'Descritivo',path:'missing.json'}]},root),{code:'ARTIFACT_MISSING',index:0});
    for (const invalid of ['../secret','/etc/passwd','/workspace/../secret'])
      await assert.rejects(collectArtifacts({artifacts:[{name:'Relatório',path:invalid}]},root),{code:'ARTIFACT_PATH'});
    await assert.rejects(collectArtifacts({artifacts:[
      {name:'Relatório 1',path:'evidence/change.diff'},{name:'Relatório 2',path:'evidence/change.diff'},
    ]},root),{code:'ARTIFACT_NAME_COLLISION'});
    if (process.platform!=='win32') {
      await symlink('/etc/passwd',path.join(root,'escape'));
      await assert.rejects(collectArtifacts({artifacts:[{name:'Relatório',path:'escape'}]},root),{code:'ARTIFACT_PATH'});
    }
  } finally { await rm(root,{recursive:true,force:true}); }
});

test('a hidden provenance file has a stable legal name without changing paths or bytes',async()=>{
 const root=await mkdtemp(path.join(tmpdir(),'artifact-hidden-'));
 try {
  await mkdir(path.join(root,'data')); await writeFile(path.join(root,'data','.candidate-provenance.json'),'{"hashesVerified":115}');
  const result=await collectArtifacts({artifacts:[{name:'candidate materialization',path:'data/.candidate-provenance.json'}]},root);
  assert.equal(result[0].name,'candidate-provenance.json');
  assert.equal(Buffer.from(result[0].base64,'base64').toString(),'{"hashesVerified":115}');
  await assert.rejects(collectArtifacts({artifacts:[
   {name:'candidate materialization',path:'data/.candidate-provenance.json'},
   {name:'candidate-provenance.json',path:'data/.candidate-provenance.json'},
  ]},root),{code:'ARTIFACT_NAME_COLLISION'});
  await assert.rejects(collectArtifacts({artifacts:[{name:'candidate materialization',path:'../.candidate-provenance.json'}]},root),{code:'ARTIFACT_PATH'});
 }finally{await rm(root,{recursive:true,force:true})}
});

test('only the final completed turn supplies the recovered manifest',()=>{
  const message={type:'assistant/message',seq:34,data:{turn:1,message:{content:[{type:'text',text:'final'}]}}};
  const end={type:'turn/end',seq:36,data:{turn:1,reason:{kind:'completed'}}};
  assert.equal(finalSessionResponse([message,end]),'final');
  assert.throws(()=>finalSessionResponse([message]));
  assert.throws(()=>finalSessionResponse([message,{...end,data:{turn:2,reason:{kind:'completed'}}}]));
  assert.throws(()=>finalSessionResponse([message,{...end,data:{turn:1,reason:{kind:'aborted'}}}]));
});

test('Qwen delivery with surrounding prose recovers exact JSON and failed evidence', async()=>{
  const value={summary:'Native renderer remains blocked; no acceptance.',artifacts:[
    {name:'verification.json',path:'evidence/verification.json',mime:'application/json'},
  ]};
  const raw=JSON.stringify(value,null,2);
  for(const answer of [raw, '```json\n'+raw+'\n```',
    'Here is the delivery:\n\n```json\n'+raw+'\n```\n\nFinal notes: passed=false.',
    'Delivery:\r\n```JSON\r\n'+raw+'\r\n```\r\nNo approval.'])
    assert.deepEqual(parseArtifactResult(answer),value);
  const root=await mkdtemp(path.join(tmpdir(),'artifact-format-'));
  try{
    await mkdir(path.join(root,'evidence'));
    await writeFile(path.join(root,'evidence/verification.json'),'{"passed":false}');
    const artifacts=await collectArtifacts(parseArtifactResult('Delivery:\n```json\n'+raw+'\n```\nDone.'),root);
    assert.equal(Buffer.from(artifacts[0].base64,'base64').toString(),'{"passed":false}');
  }finally{await rm(root,{recursive:true,force:true})}
});

test('ambiguous, truncated or invalid fenced delivery stays rejected',()=>{
  const raw=JSON.stringify({summary:'result',artifacts:[]});
  for(const answer of [
    '```json\n'+raw+'\n```\n```json\n'+raw+'\n```',
    'Example: '+raw+'\n```json\n'+raw+'\n```',
    '```json\n'+raw+'\n```\nThe actual result: '+raw,
    '```json\n'+raw+'\n```\nThe actual result: {"summary":"unfinished',
    '```json\n'+raw,
    '```json\n{"summary": "incomplete"\n```',
    '```javascript\n'+raw+'\n```',
    'prose '+raw,
    '```json\n{"summary":"invalid contract"}\n```',
    '```json\n'+JSON.stringify({summary:'too many',artifacts:Array(31).fill({})})+'\n```',
  ])assert.throws(()=>parseArtifactResult(answer),{code:'RESULT_FORMAT'});
});
