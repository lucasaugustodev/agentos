import {expect,it} from 'vitest';
import {z} from 'zod';
import {sourceContractSchema,sourceContractEntrySchema,normalizeSourceContract,combineSourceContracts} from '../server/source-contract-schema.js';
import {objectiveSchema,planSchema,planGenerationSchema,validatePlan,effectivePlanningRequest} from '../server/objective-contracts.js';
import {compileDelivery,deliveryDecisionSchema} from '../server/incremental-planning.js';

const agents=[{id:'engineering.backend.node',runtime:'harness'},{id:'engineering.integration',runtime:'harness'},{id:'qa.integration',runtime:'harness'}];
const request=(version:1|null=1)=>objectiveSchema.parse({message:'Deliver actual source kernels and an independent report.',project:'test',mode:'execute',
  workspaces:['test/base'],requirements:['The canonical source contains the required implementation.'],planningStrategy:'incremental',
  idempotencyKey:'source-contract-fixture',...(version?{sourceContractVersion:version}:{})});
const contract=(path='kernels/fused.py')=>({version:1 as const,required:[{path,kind:'file' as const}],preserve:[]});
const delivery=(key='producer',path='kernels/fused.py',dependsOn:string[]=[])=>({kind:'delivery' as const,key,requirementId:'req-1',dependsOn,
  title:'Deliver source '+key,action:'Implement the bounded canonical source contribution and preserve existing behavior.',
  verification:'Execute the independent discriminating checks and retain their actual evidence.',agentId:'engineering.backend.node',
  writePaths:[path],sourceContract:contract(path)});
const validation=(target='producer')=>({kind:'validation' as const,key:'check',validationTargetKey:target,requirementId:'req-1',dependsOn:[],
  title:'Validate exact candidate',action:'Inspect the canonical producer candidate without modifying the source tree.',
  verification:'Run the independent checks against the actual target candidate and preserve evidence.',agentId:'qa.integration',writePaths:['evidence/check']});
const violation=(fn:()=>unknown,code:string)=>{try{fn();throw Error('Expected planning rejection');}catch(error:any){expect(error.detail?.violation).toBe(code);}};

it('rejects the concrete tools/evidence-only scope for a required kernels output before dispatch',()=>{
  violation(()=>compileDelivery({...delivery(),writePaths:['tools','evidence']},request()),'SOURCE_OUTPUT_OUTSIDE_WRITE_SCOPE');
  const plan=compileDelivery(delivery(),request());
  plan.tasks[0].writePaths=['tools','evidence'];
  violation(()=>validatePlan(plan,request(),agents,[]),'SOURCE_OUTPUT_OUTSIDE_WRITE_SCOPE');
});

it('requires source declarations or an explicit report-only reason without inspecting task prose',()=>{
  const {sourceContract,...missing}=delivery();
  violation(()=>compileDelivery(missing,request()),'SOURCE_CONTRACT_REQUIRED');
  expect(sourceContractSchema.safeParse({version:1,required:[],preserve:[]}).success).toBe(false);
  const report={version:1 as const,required:[],preserve:[],noSourceReason:'This task only records diagnostic observations and does not promise implementation.'};
  const plan=compileDelivery({...missing,writePaths:['evidence'],sourceContract:report},request());
  expect(plan.tasks.every(task=>task.sourceContract?.noSourceReason===report.noSourceReason)).toBe(true);
  expect(()=>validatePlan(plan,request(),agents,[])).not.toThrow();
});

it('keeps legacy plans parseable and opts new model schemas into the explicit contract',()=>{
  const {sourceContract,...legacy}=delivery();
  const oldRequest=request(null),plan=compileDelivery(legacy,oldRequest);
  expect(plan.sourceContractVersion).toBeUndefined();
  expect(planSchema.parse(plan)).toEqual(plan);
  expect(()=>validatePlan(plan,oldRequest,agents,[])).not.toThrow();
  violation(()=>validatePlan(plan,request(),agents,[]),'SOURCE_CONTRACT_VERSION_REQUIRED');
  for(const serial of [false,true]){
    const schema=deliveryDecisionSchema(agents,['req-1'],serial,1);
    expect(schema.safeParse(legacy).success).toBe(false);
    expect(schema.safeParse(delivery()).success).toBe(true);
    expect(()=>z.toJSONSchema(schema)).not.toThrow();
  }
  expect(planGenerationSchema(1).safeParse(plan).success).toBe(false);
  expect(effectivePlanningRequest(oldRequest,{})).toEqual(oldRequest);
  expect(effectivePlanningRequest(oldRequest,{sourceContractVersion:1}).sourceContractVersion).toBe(1);
  expect(oldRequest.sourceContractVersion).toBeUndefined();
});

it('inherits all source obligations through joins, integration and the exact readonly validation target',()=>{
  const first=delivery('first','kernels/first.py'),second=delivery('second','kernels/second.py');
  const consumer=delivery('consumer','tools/runner.py',['first','second']);
  first.sourceContract.preserve=[];
  const plan=compileDelivery({deliveries:[first,second,consumer,validation('consumer')]},request());
  const required=(key:string)=>plan.tasks.find(task=>task.key===key)!.sourceContract!.required.map(entry=>entry.path);
  expect(required('join-3')).toEqual(['kernels/first.py','kernels/second.py']);
  expect(required('integrate')).toEqual(['kernels/first.py','kernels/second.py','tools/runner.py']);
  expect(required('verify')).toEqual(required('integrate'));
  expect(required('validate-4')).toEqual(['tools/runner.py']);
  expect(plan.tasks.find(task=>task.key==='validate-4')!.writePaths).toEqual(['evidence/check']);
  expect(()=>validatePlan(plan,request(),agents,[])).not.toThrow();
  plan.tasks.find(task=>task.key==='verify')!.sourceContract=contract('tools/runner.py');
  violation(()=>validatePlan(plan,request(),agents,[]),'SOURCE_CONTRACT_OBLIGATION_DROPPED');
});

it('does not normalize unsafe aliases or accept outputs excluded from the canonical runtime inventory',()=>{
  for(const path of ['evidence/kernel.py','data/kernel.py','src/../kernels/a.py','src//a.py','src/./a.py','/kernels/a.py','C:/kernels/a.py','src\\a.py',
    '.local-test/a','src/test-results/a','playwright-report/a','src/__pycache__/a','.env.local','src/a.log','src/a.pyc'])
    expect(sourceContractEntrySchema.safeParse({path,kind:'file'}).success,path).toBe(false);
  expect(sourceContractEntrySchema.safeParse({path:'rules/check.py',kind:'file'}).success).toBe(true);
  expect(sourceContractEntrySchema.safeParse({path:'src/archive.log',kind:'directory'}).success).toBe(true);
  expect(sourceContractEntrySchema.safeParse({path:'kernels',kind:'directory',sha256:'a'.repeat(64)}).success).toBe(false);
  expect(()=>normalizeSourceContract({version:1,required:[...contract().required,...contract().required],preserve:[]})).toThrow();
});

it('normalizes ordering while preserving exact reference hashes and conflicting obligations',()=>{
  const hash='a'.repeat(64),other='b'.repeat(64);
  const a=normalizeSourceContract({version:1,required:[{path:'kernels/reference.py',kind:'file',sha256:hash}],
    preserve:[{path:'src/original.py',kind:'file',sha256:other}]});
  const merged=combineSourceContracts([a,contract('src/new.py')]);
  expect(merged.required[0]).toEqual(a.required[0]);
  expect(merged.preserve).toEqual(a.preserve);
  expect(()=>combineSourceContracts([a,{...a,required:[{...a.required[0],sha256:other}]}])).toThrow('SOURCE_CONTRACT_CONFLICT');
});
