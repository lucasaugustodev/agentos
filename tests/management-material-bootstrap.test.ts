import {it,expect,vi} from 'vitest';
import {randomUUID} from 'node:crypto';
import type {DB} from '../server/db.js';
import {deliveryDigest} from '../server/delivery-supervision.js';
import {bootstrapRequiredMaterialDecision,frozenMaterialDecision,materialContextGapSchema,
  readMaterialDecisionOutcome} from '../server/management-material-decision.js';
import {compactSharedManagerContext,expandSharedManagerContext} from '../server/manager-context-sharing.js';

function fixture(count=1) {
  const goalId=randomUUID(),caseId=randomUUID(),taskId=randomUUID(),runId=randomUUID();
  const binding={goalId,caseId,consumerTaskId:taskId,consumerRunId:runId,consumerAttempt:58,
    obligations:Array.from({length:count},(_,i)=>({obligationId:randomUUID(),generation:deliveryDigest(['generation',i]),
      handoffId:randomUUID(),producerTaskId:randomUUID(),producerRunId:randomUUID(),
      producerManifestDigest:deliveryDigest(['manifest',i]),repairArtifacts:[
        {id:randomUUID(),name:'implementation.patch',digest:deliveryDigest(['patch',i]),size:12},
        {id:randomUUID(),name:'repair-report.json',digest:deliveryDigest(['report',i]),size:13}]}))};
  const hint={version:1,authority:'review_only',...binding,identityDigest:deliveryDigest(binding),actionValidationRequired:true};
  const row:any={id:caseId,goal_id:goalId,task_id:taskId,state:'open',decision_run_id:null,
    has_live_run:false,current_task_state:'failed',current_run_state:'failed',
    source_run_id:runId,current_run_id:runId,current_attempt:58,materialConsumptionReview:hint};
  const focus={caseId,taskId};
  const cases=[row];
  const bootstrap=(prior:any=null)=>bootstrapRequiredMaterialDecision(goalId,focus,cases,prior);
  const snapshot=()=>({cases:[{id:caseId,taskId,evidence:[],currentRun:{id:runId,attempt:58,state:'failed'},
    materialConsumptionReview:row.materialConsumptionReview}],operation:{focus,requiredDecision:bootstrap()}});
  return {goalId,caseId,taskId,runId,binding,hint,row,focus,cases,bootstrap,snapshot};
}
type Fixture=ReturnType<typeof fixture>;
function rehash(f:Fixture) {
  const h=f.row.materialConsumptionReview;
  h.identityDigest=deliveryDigest({goalId:h.goalId,caseId:h.caseId,consumerTaskId:h.consumerTaskId,
    consumerRunId:h.consumerRunId,consumerAttempt:h.consumerAttempt,obligations:h.obligations});
}

it('gives the first canonical material case the exact contract later enforced by the existing outcome guard',async()=>{
  const f=fixture(29),before=structuredClone(f.cases),required=f.bootstrap();
  const snapshot=f.snapshot();
  expect(required.materialReview).toEqual(f.hint);
  expect(required.contextGapContract.template).toEqual({caseId:f.caseId,consumerRunId:f.runId,
    identityDigest:f.hint.identityDigest,reason:'',requestIndices:[]});
  expect(materialContextGapSchema.safeParse(required.contextGapContract.template).success).toBe(false);
  expect(required.actionAddress).toBe('supervision.actions');
  expect(required).not.toHaveProperty('satisfied');expect(required).not.toHaveProperty('decisionRunId');
  expect(required).not.toHaveProperty('sourceTaskId');
  expect(frozenMaterialDecision('MANAGEMENT_V1\n'+JSON.stringify(snapshot))).toMatchObject({
    invalid:false,binding:f.binding,identityDigest:f.hint.identityDigest});
  // The bootstrap itself performs no I/O. The mock supplies only the existing
  // read-only currentEpisode observation, not an accepted source or action.
  const query=vi.fn().mockResolvedValue({rows:[{id:f.caseId,task_id:f.taskId,source_run_id:f.runId,
    case_state:'open',task_state:'failed',attempt_count:58,run_id:f.runId,attempt:58,
    run_state:'failed',live:false,accepted_decision:false}]});
  const db={query} as unknown as DB;
  const coordination:any={management:{actions:[]}};
  const description='MANAGEMENT_V1\n'+JSON.stringify(snapshot);
  const result:any=await readMaterialDecisionOutcome(db,f.goalId,randomUUID(),description,coordination);
  expect(result).toMatchObject({code:'MANAGEMENT_MATERIAL_DECISION_REQUIRED',satisfied:false});
  expect(result.requiredDecision).toEqual(required);
  coordination.supervision={actions:[{caseId:f.caseId,kind:'revise',diagnosis:'A proposed source is not an applied decision.',
    instruction:'Preserve exact criteria and use only a source accepted by the canonical applier.'}]};
  expect(await readMaterialDecisionOutcome(db,f.goalId,randomUUID(),description,coordination))
    .toMatchObject({code:'MANAGEMENT_MATERIAL_ACTION_UNAPPLIED',satisfied:false});
  expect(query).toHaveBeenCalledTimes(2);
  for(const [sql] of query.mock.calls)expect(sql.trim()).toMatch(/^SELECT /);
  expect(f.cases).toEqual(before);
});

it.each(['failed','submitted','recovery_blocked'])('accepts the canonical stopped consumer state %s',state=>{
  const f=fixture();f.row.current_task_state=state;
  expect(f.bootstrap()?.identityDigest).toBe(f.hint.identityDigest);
});

const mismatches:[string,(f:Fixture)=>void][]=[
  ['selected case identity',f=>{f.row.id=randomUUID();}],
  ['selected task identity',f=>{f.row.task_id=randomUUID();}],
  ['selected goal identity',f=>{f.row.goal_id=randomUUID();}],
  ['source run',f=>{f.row.source_run_id=randomUUID();}],
  ['current run',f=>{f.row.current_run_id=randomUUID();}],
  ['current attempt',f=>{f.row.current_attempt++;}],
  ['noninteger attempt',f=>{f.row.current_attempt=58.5;}],
  ['closed case',f=>{f.row.state='resolved';}],
  ['owned decision',f=>{f.row.decision_run_id=randomUUID();}],
  ['live actor',f=>{f.row.has_live_run=true;}],
  ['uncertain liveness',f=>{delete f.row.has_live_run;}],
  ['accepted consumer',f=>{f.row.current_task_state='accepted';}],
  ['live run',f=>{f.row.current_run_state='running';}],
  ['missing hint',f=>{delete f.row.materialConsumptionReview;}],
  ['hint authority',f=>{f.row.materialConsumptionReview.authority='accepted';}],
  ['hint version',f=>{f.row.materialConsumptionReview.version=2;}],
  ['action validation marker',f=>{f.row.materialConsumptionReview.actionValidationRequired=false;}],
  ['exact binding digest',f=>{f.row.materialConsumptionReview.identityDigest='a'.repeat(64);}],
  ['nested unknown binding field',f=>{f.row.materialConsumptionReview.obligations[0].unknownAuthority=true;rehash(f);}],
  ['missing repair reference',f=>{f.row.materialConsumptionReview.obligations[0].repairArtifacts.pop();rehash(f);}],
  ['no obligations',f=>{f.row.materialConsumptionReview.obligations=[];rehash(f);}],
  ...(['goalId','caseId','consumerTaskId','consumerRunId'] as const).map(field=>
    [`self-consistent but foreign hint ${field}`,(f:Fixture)=>{f.row.materialConsumptionReview[field]=randomUUID();rehash(f);}] as [string,(f:Fixture)=>void]),
  ['self-consistent but changed hint attempt',f=>{f.row.materialConsumptionReview.consumerAttempt=59;rehash(f);}],
  ['attempt zero',f=>{f.row.current_attempt=0;f.row.materialConsumptionReview.consumerAttempt=0;rehash(f);}],
];
it.each(mismatches)('does not bootstrap from %s',(_label,mutate)=>{
  const f=fixture();mutate(f);const before=structuredClone(f.cases);
  expect(f.bootstrap()).toBeNull();expect(f.cases).toEqual(before);
});

it('requires a unique explicitly selected same-goal case, never a global or projected hint',()=>{
  const f=fixture();
  expect(bootstrapRequiredMaterialDecision(randomUUID(),f.focus,f.cases)).toBeNull();
  expect(bootstrapRequiredMaterialDecision('invalid',f.focus,f.cases)).toBeNull();
  expect(bootstrapRequiredMaterialDecision(f.goalId,null,f.cases)).toBeNull();
  expect(bootstrapRequiredMaterialDecision(f.goalId,{caseId:f.caseId},f.cases)).toBeNull();
  expect(bootstrapRequiredMaterialDecision(f.goalId,f.focus,[])).toBeNull();
  expect(bootstrapRequiredMaterialDecision(f.goalId,f.focus,[f.row,structuredClone(f.row)])).toBeNull();
  expect(bootstrapRequiredMaterialDecision(f.goalId,f.focus,f.snapshot().cases)).toBeNull();
});

it.each([null,undefined])('bootstraps an absent prior value %s',prior=>{
  const f=fixture();expect(f.bootstrap(prior)?.identityDigest).toBe(f.hint.identityDigest);
});

it.each([
  {version:1,bindingState:'needs_revalidation',previousIdentityDigest:'f'.repeat(64),requests:[]},
  {version:1,bindingState:'control_metadata_revalidated',identityDigest:'e'.repeat(64)},
  {version:99,futureRequirement:{mustStay:true}},false,0,'unknown-prior',
])('preserves every non-null prior without replacing its revalidation or unknown contract [case %#]',prior=>{
  const f=fixture(),before=structuredClone(prior);
  expect(f.bootstrap(prior)).toBe(prior);expect(prior).toEqual(before);
  expect(bootstrapRequiredMaterialDecision('invalid',null,[],prior)).toBe(prior);
});

it.each([1,29,128])('keeps all %s obligations and exact codec roundtrip without another proportional copy',count=>{
  const f=fixture(count),snapshot=f.snapshot();
  const without={...snapshot,operation:{...snapshot.operation,requiredDecision:null}};
  const before=compactSharedManagerContext(without),after=compactSharedManagerContext(snapshot);
  expect(expandSharedManagerContext(after)).toEqual(snapshot);
  expect(snapshot.operation.requiredDecision.materialReview.obligations).toEqual(f.binding.obligations);
  const chars=(value:any)=>JSON.stringify('MANAGEMENT_V1\n'+JSON.stringify(value)).length;
  // Adding a duplicate binding changes which identities the exact codec shares.
  // Comparing two independently optimized encodings incorrectly charges that
  // changed representation as a second authority copy (5280/15797 in the
  // 29/128-source regression). The actual plain prior already contains the
  // complete binding; retain it plus a bounded fixed contract allowance.
  expect(chars(after)-chars(without)).toBeLessThan(5000);
  if(count>1) {
    expect(after.sharedContext?.nodeReferences).toBe(true);
    expect(chars(after)).toBeLessThan(chars(snapshot)*0.8);
  }
  expect(expandSharedManagerContext(before)).toEqual(without);
  expect(JSON.stringify(snapshot.operation.requiredDecision.contextGapContract).length).toBeLessThan(1200);
});
