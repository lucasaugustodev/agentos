import {afterEach,beforeEach,expect,it} from 'vitest';
import {mkdtemp,mkdir,readFile,rm,symlink,writeFile} from 'node:fs/promises';
import {createHash} from 'node:crypto';
import {tmpdir} from 'node:os';
import path from 'node:path';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';

const exec=promisify(execFile);
const guardPath=path.resolve('scripts/agentos-release-action.py');
const runnerPath=path.resolve('node_modules/vitest/vitest.mjs');
const hash=(raw:string|Buffer)=>createHash('sha256').update(raw).digest('hex');
let build:string;
const file='tests/required-pg.test.ts';
const requiredName='transaction contract preserves receipt identity';
const requirements=()=>({version:1,profile:'agentos.validate',candidateDigest:'a'.repeat(64),
  requiredDatabaseTests:[{file,name:requiredName}]});
const report=(status='passed')=>({success:status!=='failed',numTotalTests:2,
  numPassedTests:Number(status==='passed'),numFailedTests:Number(status==='failed'),
  testResults:[{name:path.join(build,file),status:status==='failed'?'failed':'passed',
    assertionResults:[{fullName:requiredName,status},{fullName:'optional provider integration',status:'pending'}]}]});
const bootstrap=`
import importlib.util,json,pathlib,sys
spec=importlib.util.spec_from_file_location('release_guard',sys.argv[1])
guard=importlib.util.module_from_spec(spec);spec.loader.exec_module(guard)
build=pathlib.Path(sys.argv[2]);parameters=json.loads(sys.argv[3])
try:
    result=guard.test_evidence(build,parameters)
    print(json.dumps({'result':result}))
except ValueError as error:
    print(json.dumps({'error':str(error)}))
`;
async function evidence(parameters:unknown={validationCoverage:requirements()}){
  const result=await exec('python3',['-B','-c',bootstrap,guardPath,build,JSON.stringify(parameters)],
    {timeout:10_000,maxBuffer:2*1024*1024});
  return JSON.parse(result.stdout);
}
async function put(value:unknown){
  const raw=JSON.stringify(value);
  await writeFile(path.join(build,'.local-test/vitest-results.json'),raw);
  return raw;
}

beforeEach(async()=>{
  build=await mkdtemp(path.join(tmpdir(),'agentos-required-tests-'));
  await mkdir(path.join(build,'.local-test'));
});
afterEach(async()=>{await rm(build,{recursive:true,force:true});});

it('binds each required test to the exact report and candidate while allowing unrelated skips',async()=>{
  const raw=await put(report());
  const value=(await evidence()).result;
  expect(value.passed).toBe(true);
  expect(value.coverage).toMatchObject({version:1,source:'registered_vitest_json',profile:'agentos.validate',
    candidateDigest:'a'.repeat(64),reportRoot:build,reportDigest:hash(raw),
    requiredTests:[{file,name:requiredName,status:'passed'}]});
  expect(value.coverage.obligationsDigest).toBe(hash(JSON.stringify(requirements().requiredDatabaseTests)));
  expect(value.artifacts.map((a:any)=>a.name)).toEqual(['vitest-results.json','validation-coverage.json']);
  expect(value.artifacts[0].content).toBe(raw);
});

it.each(['failed','pending','skipped','todo','disabled'])('keeps required case %s negative even with other passing tests',async(status)=>{
  const value=report(status);
  value.testResults[0].assertionResults.push({fullName:'unrelated passing control',status:'passed'});
  value.numTotalTests++;value.numPassedTests++;
  await put(value);
  const proof=(await evidence()).result;
  expect(proof.passed).toBe(false);
  expect(proof.coverage.requiredTests[0].status).toBe(status==='failed'?'failed':'skipped');
});

it('reports a missing required case rather than inferring database coverage from a suite name',async()=>{
  const value=report();value.testResults[0].assertionResults[0].fullName='some other behavior';
  await put(value);
  expect((await evidence()).result).toMatchObject({passed:false,
    coverage:{requiredTests:[{file,name:requiredName,status:'missing'}]}});
});

it.each(['case','file','alias','outside','count','contradiction'])('rejects ambiguous or inconsistent report %s',async(kind)=>{
  const value=report();
  if(kind==='case'){value.testResults[0].assertionResults.push(value.testResults[0].assertionResults[0]);value.numTotalTests++;value.numPassedTests++;}
  if(kind==='file')value.testResults.push(value.testResults[0]);
  if(kind==='alias')value.testResults[0].name=build+'/tests/../tests/required-pg.test.ts';
  if(kind==='outside')value.testResults[0].name='/another-build/tests/required-pg.test.ts';
  if(kind==='count')value.numPassedTests=800;
  if(kind==='contradiction')value.testResults[0].status='failed';
  await put(value);
  expect((await evidence()).error).toMatch(/^VALIDATION_REPORT_/);
});

it('requires a regular report file rather than following a symlink outside the validation directory',async()=>{
  const source=path.join(build,'unrelated.json');await writeFile(source,JSON.stringify(report()));
  await symlink(source,path.join(build,'.local-test/vitest-results.json'));
  expect((await evidence()).error).toBe('VALIDATION_REPORT_MISSING_OR_LINK');
});

it('keeps legacy validation evidence distinct from an authorized database coverage obligation',async()=>{
  await put(report());
  const proof=(await evidence({})).result;
  expect(proof.coverage).toBeNull();
  expect(proof.artifacts.map((a:any)=>a.name)).toEqual(['vitest-results.json']);
  const duplicate=requirements();duplicate.requiredDatabaseTests.push({...duplicate.requiredDatabaseTests[0]});
  expect((await evidence({validationCoverage:duplicate})).error).toBe('VALIDATION_COVERAGE_DUPLICATE_OBLIGATION');
});

it('consumes an actual pinned Vitest JSON report and distinguishes a skipped required control',async()=>{
  await mkdir(path.join(build,'tests'));
  await writeFile(path.join(build,file),`it(${JSON.stringify(requiredName)},()=>expect(2+2).toBe(4));
it.skip('optional provider integration',()=>{});`);
  const config=path.join(build,'vitest.config.mjs');
  await writeFile(config,"export default {test:{globals:true,include:['tests/*.test.ts'],maxWorkers:1,fileParallelism:false,watch:false}};");
  const result=await exec(process.execPath,[runnerPath,'run','--root',build,'--config',config,
    '--reporter=default','--reporter=json','--outputFile=.local-test/vitest-results.json'],
    {cwd:build,timeout:30_000,maxBuffer:1024*1024});
  const proof=(await evidence()).result;
  expect(proof.passed).toBe(true);
  const skipped=requirements();skipped.requiredDatabaseTests=[{file,name:'optional provider integration'}];
  expect((await evidence({validationCoverage:skipped})).result).toMatchObject({passed:false,
    coverage:{requiredTests:[{status:'skipped'}]}});
  // This is a reporter transport test. Its arithmetic control is not a database qualification.
  const reportPath=path.join(build,'.local-test/vitest-results.json');
  const code=`import importlib.util,json,pathlib,sys
spec=importlib.util.spec_from_file_location('guard',sys.argv[1]);guard=importlib.util.module_from_spec(spec);spec.loader.exec_module(guard)
text=pathlib.Path(sys.argv[2]).read_text()
print(json.dumps({'valid':guard.validation_complete(text,require_build=False,report_path=sys.argv[3]),'extra':guard.validation_complete(text+'\\nunhandled late failure',require_build=False,report_path=sys.argv[3])}))`;
  const log=path.join(build,'reporter.log');await writeFile(log,result.stdout+result.stderr);
  const completeness=JSON.parse((await exec('python3',['-B','-c',code,guardPath,log,reportPath],{timeout:10_000})).stdout);
  expect(completeness).toEqual({valid:true,extra:false});
  expect(hash(await readFile(reportPath))).toBe(proof.reportDigest);
});
