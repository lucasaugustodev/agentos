import {expect,it} from 'vitest';
import {createHash,randomUUID} from 'node:crypto';
import {verifyRequirementOutcomes,requirementOutcomeCriterion,acceptedRequirementOutcomes,
  resolveRequirementOutcomes,requirementContractsSchema} from '../server/requirement-outcomes.js';
import {projectWorkFrontier,objectiveFrontierNodes} from '../server/work-frontier.js';
import {deliveryFrontier} from '../server/incremental-planning.js';
import {objectiveSchema} from '../server/objective-contracts.js';

const sha=(v:string|Buffer)=>createHash('sha256').update(v).digest('hex');
const contracts=requirementContractsSchema.parse([{id:'delivery',externalDependencies:[{key:'external-window',kind:'external_event',name:'external-event.json',field:'available',equals:true}],observables:[{id:'actual-result',name:'measured-result.json',
  authority:'current_run',type:'json_field',field:'correct',equals:true,description:'Actual measured product correctness'}]}]);
const criterion=requirementOutcomeCriterion(contracts);
function fixture(status:'completed'|'partial'|'externally_blocked'='completed'){
  const run=randomUUID(),fence=randomUUID();
  const product=Buffer.from(JSON.stringify({correct:true}));
  const files=[{path:'src/product.txt',sha256:sha('product')}];
  const inventory=sha(JSON.stringify(files.map(f=>[f.path,f.sha256])));
  const candidate=Buffer.from(JSON.stringify({baseDigest:inventory,candidateDigest:inventory,baseFiles:files,files}));
  const report={version:2,outcomes:[{requirementId:'delivery',status,scope:status==='completed'?'full':'partial',
    observations:status==='completed'?[{observableId:'actual-result',name:'measured-result.json',digest:sha(product)}]:[],
    ...(status==='externally_blocked'?{dependency:{key:'external-window',kind:'external_event',reason:'The external service is not open yet.'}}:{})}]};
  const artifacts=[['candidate.json',candidate],['measured-result.json',product],['requirement-outcomes.json',Buffer.from(JSON.stringify(report))]]
    .map(([name,bytes])=>({id:randomUUID(),run_id:run,name:name as string,digest:sha(bytes),size:Buffer.byteLength(bytes),bytes:bytes as Buffer}));
  const snapshot={run_id:run,run_fence:fence,artifacts,validation_input:{contract:{stage:'verify',covers:['delivery']}}};
  const reader=async(id:string)=>artifacts.find(a=>a.id===id)!;
  const setReport=(value:any)=>{
    const a=artifacts.find(a=>a.name==='requirement-outcomes.json')!;a.bytes=Buffer.from(JSON.stringify(value));a.digest=sha(a.bytes);a.size=a.bytes.length;
  };
  const external=Buffer.from(JSON.stringify({available:true}));
  const inputs=status==='completed'?[{id:randomUUID(),name:'external-event.json',digest:sha(external),base64:external.toString('base64'),receiptDigest:'b'.repeat(64),requirementIds:['delivery']}]:[];
  return {snapshot,reader,artifacts,report,setReport,inputs};
}

it('retains an accepted offline gate while its external requirement remains uncompleted',async()=>{
  const f=fixture('externally_blocked'),proof=await verifyRequirementOutcomes(criterion,f.snapshot,f.reader,f.inputs);
  const criteria=[criterion],task={id:'verify',title:'Offline preparation',state:'accepted',stage:'verify',plan_revision:2,
    attempt_count:1,criteria,covers:['delivery']};
  const run={id:f.snapshot.run_id,fence:f.snapshot.run_fence,task_id:task.id,attempt:1,state:'submitted',accepted_criteria:criteria};
  const checks=[{id:criterion.id,type:criterion.type,passed:true,requirementOutcomes:proof}];
  const nodes=objectiveFrontierNodes([task],[run],[{run_id:run.id,passed:true,checks,created_at:'2026-01-01'}],[],f.artifacts);
  const current=projectWorkFrontier({kind:'objective',id:'generic',version:2},nodes,[{id:'delivery',description:'Real external delivery'}],2);
  expect(current.entries[0].state).toBe('accepted');expect(current.complete).toBe(false);
  expect(current.requirements[0].state).toBe('externally_blocked');expect(current.next).toBe('await_external_dependency');
  // Old receipts retain old semantics; adoption happens through a new plan.
  expect(projectWorkFrontier({kind:'objective',id:'legacy',version:1},nodes,[{id:'delivery',description:'Legacy scope'}]).complete).toBe(true);
});
it('requires every frozen observable and the actual hash for a full outcome',async()=>{
  const f=fixture();const proof=await verifyRequirementOutcomes(criterion,f.snapshot,f.reader,f.inputs);
  expect(proof.outcomes[0].status).toBe('completed');
  f.report.outcomes[0].observations=[];f.setReport(f.report);
  await expect(verifyRequirementOutcomes(criterion,f.snapshot,f.reader,f.inputs)).rejects.toThrow('REQUIREMENT_OBSERVABLE_MISSING');
});
it('does not hide another actionable blocker behind an accepted external requirement wait',async()=>{
  const f=fixture('externally_blocked'),proof=await verifyRequirementOutcomes(criterion,f.snapshot,f.reader,[]);
  const nodes=[{id:'verified',title:'Accepted gate',state:'accepted',scope:'product',stage:'verify',dependencies:[],
    covers:['delivery'],criteria:[criterion],accepted:true,live:false,requirementOutcomes:proof},
    {id:'failed',title:'Unresolved independent work',state:'failed',scope:'product',stage:'work',dependencies:[],
      covers:[],criteria:[{id:'actual',label:'Actual result'}],accepted:false,live:false}];
  const frontier=projectWorkFrontier({kind:'objective',id:'generic',version:2},nodes,[{id:'delivery',description:'External product'}],2);
  expect(frontier.requirements[0].state).toBe('externally_blocked');
  expect(frontier.next).toBe('supervise_blockers');expect(frontier.complete).toBe(false);
});
it.each(['missing','duplicate','foreign'])('rejects %s requirement coverage instead of completing by a passed flag',async(mode)=>{
  const f=fixture();const r:any=f.report;
  if(mode==='missing')r.outcomes=[];
  if(mode==='duplicate')r.outcomes.push({...r.outcomes[0]});
  if(mode==='foreign')r.outcomes[0].requirementId='other';
  f.setReport(r);await expect(verifyRequirementOutcomes(criterion,f.snapshot,f.reader,f.inputs)).rejects.toThrow();
});
it.each(['hash','body','run','duplicate'])('rejects changed %s canonical evidence',async(mode)=>{
  const f=fixture(),a=f.artifacts.find(a=>a.name==='measured-result.json')!;
  if(mode==='hash'){f.report.outcomes[0].observations[0].digest='f'.repeat(64);f.setReport(f.report);}
  if(mode==='body')a.bytes=Buffer.from('corrupt');
  if(mode==='run')a.run_id=randomUUID();
  if(mode==='duplicate')f.artifacts.push({...a,id:randomUUID()});
  await expect(verifyRequirementOutcomes(criterion,f.snapshot,f.reader,f.inputs)).rejects.toThrow();
});
it('accepts a truthful partial observation without claiming a complete product',async()=>{
  const f=fixture('partial'),proof=await verifyRequirementOutcomes(criterion,f.snapshot,f.reader,f.inputs);
  expect(resolveRequirementOutcomes([{id:'delivery',description:'Complete product'}],[{id:'qa',stage:'verify',covers:['delivery'],
    accepted:true,live:false,planRevision:2,requirementOutcomes:proof}])[0].state).toBe('partial');
});
it('does not accept partial scope or pending dependency disguised as completed',async()=>{
  for(const mutation of ['scope','dependency']){
    const f=fixture(),r:any=f.report;
    if(mutation==='scope')r.outcomes[0].scope='partial';
    else r.outcomes[0].dependency={key:'pending',kind:'external_input',reason:'Input is still unavailable'};
    f.setReport(r);await expect(verifyRequirementOutcomes(criterion,f.snapshot,f.reader,f.inputs)).rejects.toThrow();
  }
});
it('cannot replace authenticated operator input with a worker-authored lookalike',async()=>{
  const f=fixture(),c=structuredClone(criterion);c.requirements[0].observables[0].authority='objective_input';
  await expect(verifyRequirementOutcomes(c,f.snapshot,f.reader,f.inputs)).rejects.toThrow('REQUIREMENT_INPUT_IDENTITY');
  const source=f.artifacts.find(a=>a.name==='measured-result.json')!,inputId=randomUUID();
  (f.report.outcomes[0].observations[0] as any).inputId=inputId;f.setReport(f.report);
  const inputs=[{id:inputId,name:source.name,digest:source.digest,base64:source.bytes.toString('base64'),receiptDigest:'a'.repeat(64),requirementIds:['delivery']}];
  expect((await verifyRequirementOutcomes(c,f.snapshot,f.reader,[...f.inputs,...inputs])).outcomes[0].evidence[0]).toMatchObject({authority:'objective_input',id:inputId});
  await expect(verifyRequirementOutcomes(c,f.snapshot,f.reader,[...f.inputs,{...inputs[0],requirementIds:['unrelated']}])).rejects.toThrow('REQUIREMENT_INPUT_IDENTITY');
});
it('fences normalized outcomes to the exact current run and immutable evidence',async()=>{
  const f=fixture(),proof=await verifyRequirementOutcomes(criterion,f.snapshot,f.reader,f.inputs);
  const checks=[{id:criterion.id,type:criterion.type,passed:true,requirementOutcomes:proof}];
  expect(acceptedRequirementOutcomes([criterion],checks,f.snapshot.run_id,f.artifacts,f.snapshot.run_fence)).not.toBeNull();
  expect(acceptedRequirementOutcomes([criterion],checks,f.snapshot.run_id,f.artifacts,randomUUID())).toBeNull();
  expect(acceptedRequirementOutcomes([criterion],checks,randomUUID(),f.artifacts,f.snapshot.run_fence)).toBeNull();
  expect(acceptedRequirementOutcomes([criterion],checks,f.snapshot.run_id,[],f.snapshot.run_fence)).toBeNull();
});
it('a later pending verifier cannot inherit an earlier full outcome',async()=>{
  const f=fixture(),proof=await verifyRequirementOutcomes(criterion,f.snapshot,f.reader,f.inputs);
  const owners=[{id:'old',stage:'verify',covers:['delivery'],accepted:true,live:false,planRevision:2,requirementOutcomes:proof},
    {id:'new',stage:'verify',covers:['delivery'],accepted:false,live:true,planRevision:3}];
  expect(resolveRequirementOutcomes([{id:'delivery',description:'Product'}],owners)[0].state).toBe('pending');
});
it('uses the final verifier after an accepted intermediate validation without ignoring a missing final gate',async()=>{
  const f=fixture(),proof=await verifyRequirementOutcomes(criterion,f.snapshot,f.reader,f.inputs);
  const nodes=[{id:'producer',stage:'work',dependencies:[],covers:['delivery'],accepted:true,live:false,planRevision:2},
    {id:'intermediate',stage:'verify',dependencies:['producer'],covers:['delivery'],accepted:true,live:false,planRevision:2},
    {id:'integrate',stage:'integrate',dependencies:['producer','intermediate'],covers:['delivery'],accepted:true,live:false,planRevision:2},
    {id:'final',stage:'verify',dependencies:['integrate'],covers:['delivery'],accepted:true,live:false,planRevision:2,requirementOutcomes:proof}];
  const requirements=[{id:'delivery',description:'Complete integrated product'}];
  expect(resolveRequirementOutcomes(requirements,nodes)[0]).toMatchObject({state:'accepted',taskIds:['final']});
  expect(resolveRequirementOutcomes(requirements,nodes.map(n=>n.id==='final'?{...n,requirementOutcomes:null}:n))[0].state).toBe('pending');
});
it('refuses circular self-certification observables',()=>{
  for(const name of ['requirement-outcomes.json','verification.json','candidate.json'])
    expect(requirementContractsSchema.safeParse([{id:'delivery',observables:[{...contracts[0].observables[0],name}]}]).success).toBe(false);
});
it('rejects an invented external dependency and a block whose canonical input is already available',async()=>{
  const f=fixture('externally_blocked');(f.report.outcomes[0].dependency as any).key='invented';f.setReport(f.report);
  await expect(verifyRequirementOutcomes(criterion,f.snapshot,f.reader)).rejects.toThrow('REQUIREMENT_DEPENDENCY_UNDECLARED');
  f.report.outcomes[0].dependency!.key='external-window';f.setReport(f.report);
  await expect(verifyRequirementOutcomes(criterion,f.snapshot,f.reader,fixture().inputs)).rejects.toThrow('REQUIREMENT_DEPENDENCY_ALREADY_AVAILABLE');
});
it('cannot complete while a declared external prerequisite is still absent',async()=>{
  const f=fixture();await expect(verifyRequirementOutcomes(criterion,f.snapshot,f.reader,[])).rejects.toThrow('REQUIREMENT_DEPENDENCY_UNSATISFIED');
});
it('does not advance an externally blocked selected requirement into completed',()=>{
  const request=objectiveSchema.parse({message:'Deliver a generic product',project:'generic',mode:'execute',workspaces:['generic/base'],
    requirements:['First observable product','External product'],completionContractVersion:2,idempotencyKey:'generic-test'});
  const previous:any={completionContractVersion:2,requirements:[{id:'req-1',description:request.requirements[0]},{id:'req-2',description:request.requirements[1]}],
    incremental:{selectedRequirementId:'req-1',completedRequirementIds:[],pendingRequirementIds:['req-2']}};
  const resolution={version:2 as const,revision:1,completedRequirementIds:[],blockedRequirementIds:['req-1'],proofDigests:[]};
  expect(deliveryFrontier(request,previous,true,resolution)).toMatchObject({completed:[],eligible:['req-2']});
  expect(deliveryFrontier(request,previous,false,resolution).eligible).toEqual(['req-1','req-2']);
});
