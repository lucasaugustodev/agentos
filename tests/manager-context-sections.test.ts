import {createHash} from 'node:crypto';
import {expect,it} from 'vitest';
import {projectManagerReview,projectPersistedManagerDescription,managerContextVersion,packManagerRecords} from '../server/manager-context.js';
import {expandSharedManagerContext} from '../server/manager-context-sharing.js';
import {frozenMaterialDecision} from '../server/management-material-decision.js';
import {managerContextSectionNames,managerContextSectionDigest,isManagerContextSectionReference,
 prepareManagerContextSections} from '../server/manager-context-sections.js';

const id=(n:number)=>`00000000-0000-4000-8000-${String(n).padStart(12,'0')}`;
const sha=(text:string)=>createHash('sha256').update(text).digest('hex');
const canonical=(value:any):any=>Array.isArray(value)?value.map(canonical):value&&typeof value==='object'?
 Object.fromEntries(Object.keys(value).sort().map(key=>[key,canonical(value[key])])):value;
const digest=(value:any)=>sha(JSON.stringify(canonical(value)));
const noise=(seed:number,size:number)=>Array.from({length:Math.ceil(size/64)},(_,i)=>sha(`${seed}:${i}`)).join('').slice(0,size);
const prefix=`MANAGEMENT_V1\n${managerContextVersion}\nFixture context guide\n`;
const decoded=(description:string)=>expandSharedManagerContext(JSON.parse(description.slice(description.lastIndexOf('\n{"cases":')+1)));

function fixture(sectionSize=32000):any {
 const binding={goalId:id(1),caseId:id(2),consumerTaskId:id(3),consumerRunId:id(4),consumerAttempt:58,
  obligations:Array.from({length:29},(_,i)=>({obligationId:id(100+i),generation:sha(`generation:${i}`),
   handoffId:id(200+i),producerTaskId:id(300+i),producerRunId:id(400+i),producerManifestDigest:sha(`manifest:${i}`),
   repairArtifacts:[{id:id(500+i*2),name:'implementation.patch',digest:sha(`patch:${i}`),size:100+i},
    {id:id(501+i*2),name:'repair-report.json',digest:sha(`report:${i}`),size:200+i}]}))};
 const material={version:1,authority:'review_only',...binding,identityDigest:digest(binding),actionValidationRequired:true};
 const required={version:1,caseId:id(2),taskId:id(3),consumerRunId:id(4),consumerAttempt:58,
  identityDigest:material.identityDigest,materialReview:structuredClone(material),actionAddress:'supervision.actions'};
 const requests=Array.from({length:4},(_,i)=>({kind:'task_metadata',taskId:id(3),expectedAttempt:58,
  part:['decision','criteria','dependencies','outcome'][i],offset:i*256}));
 const results=requests.map((request,i)=>({request,status:'OK',digest:sha(`page:${i}`),
  totalCharacters:6000,text:noise(700+i,2300+i*100),nextOffset:request.offset+2300+i*100,truncated:true}));
 const tasks=[{id:id(3),state:'recovery_blocked',expectedAttempt:58,stage:'integrate',workKind:'integrate',
  instructionRevisionId:id(5),workspace:'fixture/consumer',baseWorkspace:'fixture/scope',
  criteria:[{id:'integration',type:'artifact',name:'integration.json'},{id:'qa',type:'artifact',name:'verification.json'}],
  dependencies:binding.obligations.map(o=>o.producerTaskId),covers:['integration','qa'],
  intent:{text:'Preserve unstructured requirements and independent validation of the same candidate.'}}];
 const operation:any={goalId:id(1),focus:{caseId:id(2),taskId:id(3),scope:'fixture/scope'},tasks,
  requiredDecision:required,previous:{receipts:[{code:'MANAGEMENT_MATERIAL_DECISION_REQUIRED',requiredDecision:structuredClone(required)}],
   followThrough:{code:'MANAGEMENT_MATERIAL_DECISION_REQUIRED',satisfied:false,requiredDecision:structuredClone(required)}},
  decisionFeedback:{version:1,entries:[{caseId:id(2),taskId:id(3),code:'ACTION_UNAPPLIED'}]},
  backlog:{status:'awaiting_supervision',receipts:[{code:'CAPACITY_WAIT',taskId:id(3)}]},
  otherTaskDirectory:{columns:['id','state'],rows:[[id(44),'running']],unknownDetail:{mustRemain:'inline'}},
  recoveredContext:{requests,results,selection:{code:'FOCUS_RESUMED',binding:{caseId:id(2),taskId:id(3)}}},
  retainedEvidence:{version:2,entries:[{request:requests[0],digest:results[0].digest,totalCharacters:6000,
   pages:[{offset:0,text:noise(900,3000)}],availableRanges:[{offset:0,characters:3000}],complete:false}]}};
 operation.workOpportunities={selection:{cursor:2},consumers:[],acceptedSources:[],existingCoverage:[],liveActors:[],futureObservation:noise(1,sectionSize)};
 operation.deferredWorkOpportunities={selection:{cursor:2},consumers:[],acceptedSources:[],existingCoverage:[],liveActors:[],futureObservation:noise(2,sectionSize)};
 operation.progress={version:1,delivery:{integrated:0,verified:0,total:5},stagnation:{detected:true,unchangedReviews:4},milestones:[{scope:'fixture/scope',detail:noise(3,sectionSize)}]};
 operation.frontier={version:1,counts:{pending:5},scopeSummary:[{scope:'fixture/scope',pending:5}],requirements:[{id:'complete',description:noise(4,sectionSize)}]};
 operation.sourceCatalog={columns:['id','state','title'],bases:['fixture/scope'],rows:[[id(50),'accepted',noise(5,sectionSize)]]};
 operation.deferredDeliveries=[{id:id(60),state:'staged',consumer_task_id:id(3),producer_task_id:id(300),futureReceipt:{exact:noise(6,sectionSize)}}];
 operation.deliveryConsumptions=binding.obligations.map((o,i)=>({id:o.obligationId,consumer_task_id:id(3),producer_task_id:o.producerTaskId,
  state:'awaiting_supervision',scope:'fixture/scope',receipt:{generation:o.generation,caseId:id(2),code:'SUPERVISION_REQUESTED',
   ...(i===0?{futureReceipt:{exact:noise(7,sectionSize)}}:{})}}));
 return {cases:[{id:id(2),taskId:id(3),scope:'fixture/scope',currentRun:{id:id(4),attempt:58,state:'failed'},
  evidence:[{kind:'UNKNOWN_CURRENT_AUTHORITY',constraint:{required:true}}],materialConsumptionReview:material}],
  originalDescription:'Deliver the original goal with all acceptance criteria.',operation};
}

it('stores exactly the seven complete operational values, including unknown nested data, without changing the source',()=>{
 const source=fixture(),before=structuredClone(source);
 const prepared=prepareManagerContextSections(source.operation,id(1),source.operation.focus);
 expect(prepared.blobs).toHaveLength(7);
 expect(prepared.blobs.map(b=>b.section).sort()).toEqual([...managerContextSectionNames].sort());
 for(const blob of prepared.blobs){
  expect(blob.text).toBe(JSON.stringify(source.operation[blob.section]));
  expect(blob.characters).toBe(blob.text.length);
  expect(blob.digest).toBe(sha(blob.section+'\n'+blob.text));
  expect(managerContextSectionDigest(blob.section,blob.text)).toBe(blob.digest);
  const ref=prepared.references[blob.section]!;
  expect(isManagerContextSectionReference(ref,blob.section,id(1))).toBe(true);
  expect(ref.recovery.request).toEqual({kind:'context_section',section:blob.section,digest:blob.digest,offset:0});
 }
 expect(source).toEqual(before);
});

it('fits an operationally large decision with all29 bindings, current contracts and all four original pages intact',()=>{
 const source=fixture(),before=structuredClone(source);
 const result=projectManagerReview(source,prefix,0,150000,{goalId:id(1)}),out=decoded(result.description);
 expect(result.withinBudget).toBe(true);expect(JSON.stringify(result.description).length).toBeLessThanOrEqual(150000);
 expect(result.sectionBlobs!.length).toBeGreaterThan(0);
 expect(out.cases).toEqual(source.cases);expect(out.originalDescription).toBe(source.originalDescription);
 for(const key of ['focus','tasks','requiredDecision','previous','decisionFeedback','backlog','otherTaskDirectory','recoveredContext','retainedEvidence'])
  expect(out.operation[key]).toEqual(source.operation[key]);
 const frozen=frozenMaterialDecision(result.description);
 expect(frozen?.invalid).toBe(false);
 if(!frozen||frozen.invalid)throw Error('INVALID_MATERIAL_FIXTURE');
 expect(frozen.binding.obligations).toEqual(source.cases[0].materialConsumptionReview.obligations);
 expect(frozen.binding.obligations).toHaveLength(29);
 const refs=managerContextSectionNames.filter(section=>isManagerContextSectionReference(out.operation[section],section,id(1)))
  .map(section=>out.operation[section]);
 expect(refs.length).toBe(result.sectionBlobs!.length);
 for(const ref of refs){
  const matches=result.sectionBlobs!.filter(blob=>blob.section===ref.section&&blob.digest===ref.digest);
  expect(matches).toHaveLength(1);expect(JSON.parse(matches[0].text)).toEqual(source.operation[ref.section]);
 }
 expect(source).toEqual(before);
});

it('reprojects a persisted reference without inventing a new blob or changing its source address',()=>{
 const source=fixture(),first=projectManagerReview(source,prefix,0,150000,{goalId:id(1)}),before=decoded(first.description);
 expect(first.withinBudget).toBe(true);
 const second=projectManagerReview(before,prefix,0,150000,{goalId:id(1)});
 expect(second.withinBudget).toBe(true);expect(second.sectionBlobs).toEqual([]);
 const after=decoded(second.description);
 for(const section of managerContextSectionNames)expect(after.operation[section]).toEqual(before.operation[section]);
 const persisted=decoded(projectPersistedManagerDescription(first.description));
 for(const section of managerContextSectionNames)expect(persisted.operation[section]).toEqual(before.operation[section]);
 expect(after.cases).toEqual(before.cases);expect(after.operation.recoveredContext).toEqual(before.operation.recoveredContext);
});

it.each(['extra','wrong-goal','wrong-section','invalid-digest','request-mismatch','false-complete'])(
 'treats a %s reference-shaped value as literal data instead of granting a reference',mode=>{
 const original=fixture(10),prepared=prepareManagerContextSections(original.operation,id(1),original.operation.focus);
 const literal:any=structuredClone(prepared.references.progress);
 if(mode==='extra')literal.futureAuthority={read:true};
 if(mode==='wrong-goal')literal.goalId=id(99);
 if(mode==='wrong-section')literal.section='workOpportunities';
 if(mode==='invalid-digest')literal.digest='not-a-digest';
 if(mode==='request-mismatch')literal.recovery.request.digest='f'.repeat(64);
 if(mode==='false-complete')literal.complete=true;
 expect(isManagerContextSectionReference(literal,'progress',id(1))).toBe(false);
 const source={...original.operation,progress:literal},output=prepareManagerContextSections(source,id(1),source.focus);
 const blob=output.blobs.find(entry=>entry.section==='progress');
 expect(blob).toBeDefined();expect(blob!.text).toBe(JSON.stringify(literal));
 expect(output.references.progress!.digest).toBe(sha('progress\n'+JSON.stringify(literal)));
});

it('preserves unknown top-level authority and refuses overflow even after every operational section is recoverable',()=>{
 const source=fixture();source.operation.futureConstraint={currentTask:id(3),exact:noise(1000,180000)};
 const result=projectManagerReview(source,prefix,0,150000,{goalId:id(1)}),out=decoded(result.description);
 expect(result.withinBudget).toBe(false);
 expect(out.operation.futureConstraint).toEqual(source.operation.futureConstraint);
 expect(out.cases).toEqual(source.cases);expect(out.operation.requiredDecision).toEqual(source.operation.requiredDecision);
 expect(result.sectionBlobs!.every(blob=>(managerContextSectionNames as readonly string[]).includes(blob.section))).toBe(true);
});

it('hashes transport JSON dates, null and literal marker text exactly and never recursively interprets stored JSON',()=>{
 const source=fixture(10);source.operation.progress={observedAt:new Date('2026-10-12T18:00:00.000Z'),value:null,
  literal:{$n:2},text:'^1',unicode:'😀\ud800\u0000',nested:{kind:'context_section',section:'progress',digest:'a'.repeat(64)}};
 const prepared=prepareManagerContextSections(source.operation,id(1),source.operation.focus);
 const blob=prepared.blobs.find(entry=>entry.section==='progress')!;
 expect(blob.text).toBe(JSON.stringify(source.operation.progress));
 expect(JSON.parse(blob.text).observedAt).toBe('2026-10-12T18:00:00.000Z');
 expect(JSON.parse(blob.text).literal).toEqual({$n:2});expect(JSON.parse(blob.text).unicode).toBe('😀\ud800\u0000');
 expect(managerContextSectionDigest('frontier',blob.text)).not.toBe(blob.digest);
});

it('makes canonical packed cross-section sources independently readable before replacing either parent view',()=>{
 const source=fixture(10),rows=[{taskId:id(70),detail:'first'},{taskId:id(71),detail:'second'}];
 source.operation.workOpportunities.acceptedSources=packManagerRecords(rows);
 source.operation.deferredWorkOpportunities.acceptedSources={$rowsOf:'operation.workOpportunities.acceptedSources',
  sha256:sha(JSON.stringify(rows)),rows:[1,{taskId:id(72),detail:'third'}]};
 source.operation.deferredWorkOpportunities.existingCoverage={$ref:'operation.workOpportunities.existingCoverage',
  sha256:sha(JSON.stringify([])),items:0};
 const before=structuredClone(source),prepared=prepareManagerContextSections(source.operation,id(1),source.operation.focus);
 const current=JSON.parse(prepared.blobs.find(blob=>blob.section==='workOpportunities')!.text);
 const deferred=JSON.parse(prepared.blobs.find(blob=>blob.section==='deferredWorkOpportunities')!.text);
 expect(current.acceptedSources).toEqual(source.operation.workOpportunities.acceptedSources);
 expect(current.acceptedSources.rows.map((row:any[])=>Object.fromEntries(current.acceptedSources.schemas[row[0]]
  .map((key:string,i:number)=>[key,row[i+1]])))).toEqual(rows);
 expect(deferred.acceptedSources).toEqual([rows[1],{taskId:id(72),detail:'third'}]);
 expect(deferred.existingCoverage).toEqual([]);expect(source).toEqual(before);
});

it.each(['hash','index','cycle','unknown-field'])('does not create a dangling section from a %s cross-reference',mode=>{
 const source=fixture(10),rows=[{taskId:id(70),detail:'first'}];
 source.operation.workOpportunities.acceptedSources=rows;
 const reference:any={$rowsOf:'operation.workOpportunities.acceptedSources',sha256:sha(JSON.stringify(rows)),rows:[0]};
 if(mode==='hash')reference.sha256='f'.repeat(64);
 if(mode==='index')reference.rows=[1];
 if(mode==='unknown-field')reference.futureConstraint=true;
 if(mode==='cycle'){reference.$rowsOf='operation.deferredWorkOpportunities.acceptedSources';reference.rows=[0];}
 source.operation.deferredWorkOpportunities.acceptedSources=reference;
 const prepared=prepareManagerContextSections(source.operation,id(1),source.operation.focus);
 expect(prepared.references.deferredWorkOpportunities).toBeUndefined();
 expect(prepared.blobs.some(blob=>blob.section==='deferredWorkOpportunities')).toBe(false);
 if(mode!=='cycle')expect(prepared.references.workOpportunities).toBeUndefined();
 const review=projectManagerReview(source,prefix,0,150000,{goalId:id(1)});
 const out=decoded(review.description),canonical=decoded(projectManagerReview(source,prefix,0,150000).description);
 expect(out.operation.deferredWorkOpportunities).toEqual(canonical.operation.deferredWorkOpportunities);
 expect(out.operation.deferredWorkOpportunities.acceptedSources).toEqual(reference);
 expect(isManagerContextSectionReference(out.operation.deferredWorkOpportunities)).toBe(false);
});

it('does not generate any reference or store requirement when the complete observation already fits',()=>{
 const source=fixture(0),result=projectManagerReview(source,prefix,0,150000,{goalId:id(1)}),out=decoded(result.description);
 expect(result.withinBudget).toBe(true);expect(result.sectionBlobs??[]).toEqual([]);
 expect(out.operation.contextSectionDirectory).toBeUndefined();
 const canonical=decoded(projectManagerReview(source,prefix,0,150000).description);
 for(const section of managerContextSectionNames)expect(out.operation[section]).toEqual(canonical.operation[section]);
 expect(out.cases).toEqual(canonical.cases);expect(out.operation.tasks).toEqual(canonical.operation.tasks);
});

it.each([{}, {version:1,goalId:id(1),sections:'a future literal constraint'},
 {version:99,futureConstraint:{taskId:id(3),instruction:'This unknown root must remain visible.'}}])(
 'does not overwrite an unrecognized contextSectionDirectory root with its own protocol [case %#]',directory=>{
 const source=fixture();source.operation.contextSectionDirectory=directory;const before=structuredClone(source);
 const result=projectManagerReview(source,prefix,0,150000,{goalId:id(1)}),out=decoded(result.description);
 expect(out.operation.contextSectionDirectory).toEqual(directory);
 expect(out.cases).toEqual(source.cases);expect(out.operation.requiredDecision).toEqual(source.operation.requiredDecision);
 expect(source).toEqual(before);
});

it('does not strand an inline unknown root that refers to an operational parent selected for paging',()=>{
 const source=fixture();source.operation.futureConstraint={source:{$ref:'operation.workOpportunities.acceptedSources',
  sha256:sha(JSON.stringify(source.operation.workOpportunities.acceptedSources)),items:0}};
 const prepared=prepareManagerContextSections(source.operation,id(1),source.operation.focus);
 expect(prepared.references.workOpportunities).toBeUndefined();
 const review=projectManagerReview(source,prefix,0,150000,{goalId:id(1)}),out=decoded(review.description);
 expect(out.operation.futureConstraint).toEqual(source.operation.futureConstraint);
 expect(isManagerContextSectionReference(out.operation.workOpportunities)).toBe(false);
 expect(out.operation.workOpportunities.acceptedSources).toEqual(source.operation.workOpportunities.acceptedSources);
});
