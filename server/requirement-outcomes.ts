import {createHash} from 'node:crypto';
import {z} from 'zod';
import type {DB} from './db.js';

const id=z.string().regex(/^[a-z0-9_-]{1,50}$/);
const name=z.string().regex(/^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,100}$/);
const digest=z.string().regex(/^[a-f0-9]{64}$/);
const hash=(value:unknown)=>createHash('sha256').update(JSON.stringify(value)).digest('hex');
const authority=z.enum(['current_run','objective_input']);

/** Observables are frozen by planning, not chosen by the reporting worker.
 * These predicates prove the declared contract, not arbitrary natural-language
 * truth. External outcomes still require a controller-supplied receipt. */
export const requirementObservableSchema=z.discriminatedUnion('type',[
  z.object({id,name,authority,type:z.literal('artifact'),description:z.string().min(5).max(500)}).strict(),
  z.object({id,name,authority,type:z.literal('json_field'),description:z.string().min(5).max(500),
    field:z.string().regex(/^[a-zA-Z][a-zA-Z0-9_]{0,79}$/),equals:z.union([z.string().max(500),z.number().finite(),z.boolean()])}).strict(),
]);
export const externalDependencySchema=z.object({key:id,kind:z.enum(['external_input','external_event']),
  name,field:z.string().regex(/^[a-zA-Z][a-zA-Z0-9_]{0,79}$/),equals:z.union([z.string().max(500),z.number().finite(),z.boolean()])}).strict();
export const requirementContractSchema=z.object({id,observables:z.array(requirementObservableSchema).min(1).max(12),
  externalDependencies:z.array(externalDependencySchema).max(12).default([])}).strict()
  .refine(c=>new Set(c.observables.map(o=>o.id)).size===c.observables.length,'Duplicate observable')
  .refine(c=>new Set(c.externalDependencies.map(o=>o.key)).size===c.externalDependencies.length,'Duplicate external dependency')
  .refine(c=>c.observables.every(o=>!['requirement-outcomes.json','verification.json','candidate.json'].includes(o.name)),
    'An outcome cannot certify itself or substitute the runtime inventory for product evidence');
export const requirementContractsSchema=z.array(requirementContractSchema).min(1).max(30)
  .refine(cs=>new Set(cs.map(c=>c.id)).size===cs.length,'Duplicate requirement');
export type RequirementContract=z.infer<typeof requirementContractSchema>;
export const requirementOutcomesCriterionSchema=z.object({
  id:z.string().min(1),label:z.string().min(1),type:z.literal('requirement_outcomes'),
  version:z.literal(2),name:z.literal('requirement-outcomes.json'),requirements:requirementContractsSchema,
}).strict();
export type RequirementOutcomesCriterion=z.infer<typeof requirementOutcomesCriterionSchema>;

const observation=z.object({observableId:id,name,digest,inputId:z.uuid().optional()}).strict();
const outcome=z.object({requirementId:id,status:z.enum(['completed','partial','externally_blocked']),
  scope:z.enum(['full','partial']),observations:z.array(observation).max(12),
  dependency:z.object({key:id,kind:z.enum(['external_input','external_event']),reason:z.string().min(10).max(1000)}).strict().optional(),
}).strict().superRefine((v,ctx)=>{
  if(new Set(v.observations.map(o=>o.observableId)).size!==v.observations.length)
    ctx.addIssue({code:'custom',message:'Duplicate observation'});
  if(v.status==='completed'&&(v.scope!=='full'||v.dependency))
    ctx.addIssue({code:'custom',message:'Partial scope or pending dependency cannot complete a requirement'});
  if((v.status==='externally_blocked')!==!!v.dependency)
    ctx.addIssue({code:'custom',message:'Only an external block must carry a dependency'});
});
export const requirementOutcomeReportSchema=z.object({version:z.literal(2),outcomes:z.array(outcome).min(1).max(30)}).strict()
  .refine(v=>new Set(v.outcomes.map(o=>o.requirementId)).size===v.outcomes.length,'Duplicate requirement outcome');
export type RequirementOutcome=z.infer<typeof outcome>;
type Artifact={id:string;run_id:string;name:string;digest:string;size:number;bytes?:Buffer};
type Snapshot={run_id:string;run_fence:string;artifacts:Artifact[];validation_input:any};
type Reader=(id:string)=>Promise<Artifact&{bytes:Buffer}>;
export type OutcomeInput={id:string;name:string;digest:string;base64:string;receiptDigest:string;requirementIds:string[]};
export async function readOutcomeInputs(db:DB,goalId:string):Promise<OutcomeInput[]>{
  const {readObjectiveInputs}=await import('./objective-inputs.js');
  const inputs=await readObjectiveInputs(db,goalId);
  if(!inputs.length)return [];
  const receipts=(await db.query('SELECT id,receipt_digest FROM objective_inputs WHERE goal_id=$1 AND id=ANY($2::uuid[])',
    [goalId,inputs.map(i=>i.id)])).rows;
  return inputs.map(i=>({...i,receiptDigest:receipts.find(r=>r.id===i.id)?.receipt_digest}));
}
export type RequirementOutcomeProof={version:2;runId:string;fence:string;contractDigest:string;
  candidate:{id:string;digest:string};report:{id:string;digest:string};
  outcomes:Array<RequirementOutcome&{evidence:Array<{observableId:string;id:string;name:string;digest:string;
    authority:'current_run'|'objective_input';receiptDigest?:string}>;
    dependencyChecks:Array<{key:string;name:string;kind:string;ready:boolean;receipts:string[]}>}>};

/** Read only this run's canonical artifact identities. Never fetch worker paths,
 * trust a worker-provided UUID, or accept a matching filename with changed bytes. */
export async function verifyRequirementOutcomes(raw:unknown,snapshot:Snapshot,read:Reader,inputs:OutcomeInput[]=[]):Promise<RequirementOutcomeProof>{
  const criterion=requirementOutcomesCriterionSchema.parse(raw);
  if(snapshot.validation_input?.contract?.stage!=='verify'||
    JSON.stringify([...(snapshot.validation_input?.contract?.covers??[])].sort())!==JSON.stringify(criterion.requirements.map(r=>r.id).sort()))
    throw Error('REQUIREMENT_OUTCOME_TASK_SCOPE');
  const readOne=async(artifactName:string,expected?:string)=>{
    const refs=snapshot.artifacts.filter(a=>a.name===artifactName);
    if(refs.length!==1)throw Error('REQUIREMENT_EVIDENCE_IDENTITY');
    const ref=refs[0],actual=await read(ref.id);
    if(ref.run_id!==snapshot.run_id||actual.id!==ref.id||actual.run_id!==snapshot.run_id||actual.name!==artifactName||
      actual.digest!==ref.digest||actual.bytes.length!==ref.size||actual.bytes.length>1024*1024||
      createHash('sha256').update(actual.bytes).digest('hex')!==ref.digest||(expected&&expected!==ref.digest))
      throw Error('REQUIREMENT_EVIDENCE_HASH');
    return actual;
  };
  const reportArtifact=await readOne(criterion.name),candidate=await readOne('candidate.json');
  const {runtimeCandidateDigest}=await import('./operation-progress.js');
  if(!runtimeCandidateDigest(candidate.bytes,candidate.digest))throw Error('REQUIREMENT_CANDIDATE_INVALID');
  const report=requirementOutcomeReportSchema.parse(JSON.parse(reportArtifact.bytes.toString('utf8')));
  const expected=criterion.requirements.map(r=>r.id).sort();
  if(JSON.stringify(report.outcomes.map(r=>r.requirementId).sort())!==JSON.stringify(expected))
    throw Error('REQUIREMENT_OUTCOME_COVERAGE');
  const outcomes:RequirementOutcomeProof['outcomes']=[];
  for(const item of report.outcomes){
    const contract=criterion.requirements.find(r=>r.id===item.requirementId)!;
    const evidence:RequirementOutcomeProof['outcomes'][number]['evidence']=[];
    const dependencies=contract.externalDependencies.map(dependency=>{
      const candidates=inputs.filter(i=>i.name===dependency.name&&i.requirementIds.includes(item.requirementId));
      const ready=candidates.some(i=>{
        const bytes=Buffer.from(i.base64,'base64');
        if(!digest.safeParse(i.receiptDigest).success||bytes.length>65536||createHash('sha256').update(bytes).digest('hex')!==i.digest)
          throw Error('REQUIREMENT_INPUT_HASH');
        try{const value=JSON.parse(bytes.toString('utf8'));return value&&Object.hasOwn(value,dependency.field)&&value[dependency.field]===dependency.equals;}
        catch{return false;}
      });
      return {key:dependency.key,name:dependency.name,kind:dependency.kind,ready,receipts:candidates.map(i=>i.receiptDigest).sort()};
    });
    if(item.status==='externally_blocked'){
      const dependency=dependencies.find(d=>d.key===item.dependency!.key&&d.kind===item.dependency!.kind);
      if(!dependency)throw Error('REQUIREMENT_DEPENDENCY_UNDECLARED');
      if(dependency.ready)throw Error('REQUIREMENT_DEPENDENCY_ALREADY_AVAILABLE');
    }
    if(item.status==='completed'&&dependencies.some(d=>!d.ready))throw Error('REQUIREMENT_DEPENDENCY_UNSATISFIED');
    for(const observation of item.observations){
      const predicate=contract.observables.find(p=>p.id===observation.observableId);
      if(!predicate||predicate.name!==observation.name)throw Error('REQUIREMENT_OBSERVABLE_UNDECLARED');
      let artifact:{id:string;name:string;digest:string;bytes:Buffer},receiptDigest:string|undefined;
      if(predicate.authority==='objective_input'){
        const input=inputs.find(i=>i.id===observation.inputId&&i.name===observation.name&&i.digest===observation.digest&&
          i.requirementIds.includes(item.requirementId));
        if(!input||!digest.safeParse(input.receiptDigest).success)throw Error('REQUIREMENT_INPUT_IDENTITY');
        const bytes=Buffer.from(input.base64,'base64');
        if(bytes.length>65536||createHash('sha256').update(bytes).digest('hex')!==input.digest)throw Error('REQUIREMENT_INPUT_HASH');
        artifact={id:input.id,name:input.name,digest:input.digest,bytes};receiptDigest=input.receiptDigest;
      }else{
        if(observation.inputId)throw Error('REQUIREMENT_EVIDENCE_AUTHORITY');
        artifact=await readOne(observation.name,observation.digest);
      }
      if(predicate.type==='json_field'){
        const data=JSON.parse(artifact.bytes.toString('utf8'));
        if(!data||typeof data!=='object'||!Object.hasOwn(data,predicate.field)||data[predicate.field]!==predicate.equals)
          throw Error('REQUIREMENT_OBSERVABLE_UNSATISFIED');
      }
      evidence.push({observableId:predicate.id,id:artifact.id,name:artifact.name,digest:artifact.digest,authority:predicate.authority,
        ...(receiptDigest?{receiptDigest}:{})});
    }
    if(item.status==='completed'&&evidence.length!==contract.observables.length)
      throw Error('REQUIREMENT_OBSERVABLE_MISSING');
    outcomes.push({...item,evidence,dependencyChecks:dependencies});
  }
  return {version:2,runId:snapshot.run_id,fence:snapshot.run_fence,contractDigest:hash(criterion.requirements),
    candidate:{id:candidate.id,digest:candidate.digest},report:{id:reportArtifact.id,digest:reportArtifact.digest},outcomes};
}

/** Only controller-normalized checks from the current accepted attempt are
 * eligible. The adapter separately checks frozen criteria, run state and fence. */
export function acceptedRequirementOutcomes(criteria:any[],checks:any[],runId:string,artifacts:Artifact[]=[],fence?:string){
  const gates=criteria.filter(c=>c?.type==='requirement_outcomes');
  if(gates.length!==1)return null;
  const parsed=requirementOutcomesCriterionSchema.safeParse(gates[0]);if(!parsed.success)return null;
  const c=parsed.data,check=checks.find(x=>x.id===c.id&&x.type===c.type&&x.passed===true),proof=check?.requirementOutcomes;
  if(proof?.version!==2||proof.runId!==runId||!fence||proof.fence!==fence||proof.contractDigest!==hash(c.requirements)||
    !digest.safeParse(proof.candidate?.digest).success||!digest.safeParse(proof.report?.digest).success||
    !artifacts.some(a=>a.id===proof.candidate.id&&a.run_id===runId&&a.name==='candidate.json'&&a.digest===proof.candidate.digest)||
    !artifacts.some(a=>a.id===proof.report.id&&a.run_id===runId&&a.name===c.name&&a.digest===proof.report.digest))return null;
  if(!Array.isArray(proof.outcomes))return null;
  const report=requirementOutcomeReportSchema.safeParse({version:2,outcomes:proof.outcomes.map((o:any)=>{
    const {evidence,dependencyChecks,...report}=o;return report;})});
  if(!report.success||JSON.stringify(report.data.outcomes.map(o=>o.requirementId).sort())!==JSON.stringify(c.requirements.map(r=>r.id).sort()))return null;
  for(const item of proof.outcomes){
    const contract=c.requirements.find(r=>r.id===item.requirementId)!;
    if(!Array.isArray(item.dependencyChecks)||item.dependencyChecks.length!==contract.externalDependencies.length||
      new Set(item.dependencyChecks.map((d:any)=>d.key)).size!==item.dependencyChecks.length||
      item.dependencyChecks.some((d:any)=>!contract.externalDependencies.some(c=>c.key===d.key&&c.kind===d.kind&&c.name===d.name)||
        typeof d.ready!=='boolean'||!Array.isArray(d.receipts)||!d.receipts.every((r:unknown)=>digest.safeParse(r).success))||
      (item.status==='completed'&&item.dependencyChecks.some((d:any)=>!d.ready))||
      (item.status==='externally_blocked'&&!item.dependencyChecks.some((d:any)=>d.key===item.dependency?.key&&d.kind===item.dependency.kind&&!d.ready)))return null;
    if(!Array.isArray(item.evidence)||item.evidence.length!==item.observations.length||
      item.observations.some((o:any)=>!contract.observables.some(p=>p.id===o.observableId&&p.name===o.name)||
        !item.evidence.some((e:any)=>e.observableId===o.observableId&&e.name===o.name&&e.digest===o.digest&&
          (contract.observables.find(p=>p.id===o.observableId)?.authority==='objective_input'
            ?e.authority==='objective_input'&&e.id===o.inputId&&digest.safeParse(e.receiptDigest).success
            :e.authority==='current_run'&&!o.inputId&&artifacts.some(a=>a.id===e.id&&a.run_id===runId&&a.name===e.name&&a.digest===e.digest))))||
      (item.status==='completed'&&item.evidence.length!==contract.observables.length))return null;
  }
  return proof as RequirementOutcomeProof;
}

export type OutcomeOwner={id:string;stage?:string;covers?:string[];planRevision?:number;accepted:boolean;live:boolean;dependencies?:string[];
  requirementOutcomes?:RequirementOutcomeProof|null};
/** One resolver for progress, goal completion and incremental advancement.
 * A newer verifier's pending/partial result cannot inherit an older full PASS. */
export function resolveRequirementOutcomes(requirements:Array<{id:string;description:string}>,owners:OutcomeOwner[]){
  return requirements.map(requirement=>{
    const eligible=owners.filter(o=>o.stage==='verify'&&o.covers?.includes(requirement.id)&&
      !owners.some(consumer=>consumer.dependencies?.includes(o.id)));
    const revision=Math.max(-1,...eligible.map(o=>o.planRevision??0));
    const latest=eligible.filter(o=>(o.planRevision??0)===revision);
    const outcomes=latest.map(o=>o.accepted&&!o.live?o.requirementOutcomes?.outcomes.find(r=>r.requirementId===requirement.id):undefined);
    const state=!latest.length?'unplanned':outcomes.some(o=>!o)?'pending':
      outcomes.every(o=>o!.status==='completed')?'accepted':outcomes.some(o=>o!.status==='partial')?'partial':'externally_blocked';
    return {...requirement,state,taskIds:latest.map(o=>o.id),
      dependencies:outcomes.flatMap(o=>o?.dependency?[o.dependency]:[]),
      proofDigests:latest.flatMap(o=>o.accepted&&!o.live&&o.requirementOutcomes?[hash(o.requirementOutcomes)]:[])};
  });
}

export function requirementOutcomeCriterion(requirements:RequirementContract[]):RequirementOutcomesCriterion{
  return requirementOutcomesCriterionSchema.parse({id:'requirement-outcomes',label:'Resultados por requisito e evidências vinculadas',
    type:'requirement_outcomes',version:2,name:'requirement-outcomes.json',requirements});
}

export type RequirementResolution={version:2;revision:number;completedRequirementIds:string[];
  blockedRequirementIds:string[];proofDigests:string[]};
export async function readRequirementResolution(db:DB,goalId:string,revision:number):Promise<RequirementResolution>{
  const {readObjectiveFrontier}=await import('./work-frontier.js');
  const frontier=await readObjectiveFrontier(db,goalId);
  if(frontier.boundary.version!==revision)throw Error('REQUIREMENT_RESOLUTION_STALE');
  return {version:2,revision,completedRequirementIds:frontier.requirements.filter(r=>r.state==='accepted').map(r=>r.id).sort(),
    blockedRequirementIds:frontier.requirements.filter(r=>r.state==='externally_blocked').map(r=>r.id).sort(),
    proofDigests:[...new Set(frontier.requirements.flatMap(r=>'proofDigests' in r&&Array.isArray(r.proofDigests)
      ?r.proofDigests.filter((value):value is string=>typeof value==='string'):[]))].sort()};
}
