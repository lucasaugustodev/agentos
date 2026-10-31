import {z} from 'zod';
import type {ProposedPlan} from './objective-contracts.js';

const digest=z.string().regex(/^[a-f0-9]{64}$/);
export const candidateCiCriterionSchema=z.object({
  id:z.literal('accepted-candidate-ci'),label:z.string().min(1),type:z.literal('candidate_ci'),version:z.literal(1),
  name:z.literal('verification.json'),sourceTaskKey:z.string().regex(/^[a-z0-9_-]{1,80}$/),
  registrationId:z.uuid(),mapDigest:digest,policyRegistrationId:z.uuid(),policyDigest:digest,
  obligationsDigest:digest,localArtifacts:z.array(z.string().regex(/^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,100}$/)).min(1).max(20),
}).strict();

/** Only the controller calls this on an authenticated integration map. It does
 * not reinterpret a pre-existing verification attempt or its passed boolean. */
export function bindCandidateReadinessQaPlan(plan:ProposedPlan,registration:any):ProposedPlan{
  const reject=(code:string):never=>{throw Object.assign(new Error(code),{code});};
  if(plan.tasks.some(t=>t.criteria.some(c=>(c as any).type==='candidate_ci')))reject('READINESS_QA_PREDECLARED');
  const result=structuredClone(plan);
  const integrators=result.tasks.filter(t=>t.criteria.some(c=>c.type==='candidate_readiness'));
  if(!integrators.length)return result;
  if(integrators.length!==1)reject('READINESS_QA_SOURCE_UNMAPPED');
  const producer=integrators[0];
  const targets=result.tasks.filter(t=>t.stage==='verify'&&t.sourceTask===producer.key);
  if(!targets.length)return result;
  if(registration.policyStages?.includes('verify'))reject('READINESS_QA_POLICY_CONFLICT');
  if(!Array.isArray(registration.policyStages)||!registration.policyStages.includes('integrate'))reject('READINESS_QA_POLICY_UNBOUND');
  for(const task of targets){
    if(!task.dependencies.includes(producer.key)||task.criteria.length!==1||
      task.criteria[0].type!=='json_field'||task.criteria[0].name!=='verification.json'||
      task.criteria[0].field!=='passed'||task.criteria[0].equals!==true||
      JSON.stringify([...task.covers].sort())!==JSON.stringify([...producer.covers].sort()))reject('READINESS_QA_SCOPE_UNMAPPED');
    const localArtifacts=[...new Set<string>(registration.contract.obligations.filter((o:any)=>o.kind==='local').map((o:any)=>o.name))];
    const integrated:any=producer.criteria.find(c=>c.type==='candidate_readiness');
    const criterion=candidateCiCriterionSchema.parse({id:'accepted-candidate-ci',label:'CI canonica da mesma candidata e controles locais independentes',
      type:'candidate_ci',version:1,name:'verification.json',sourceTaskKey:producer.key,
      registrationId:registration.registrationId,mapDigest:registration.contractDigest,
      policyRegistrationId:registration.policyRegistrationId,policyDigest:registration.policyDigest,
      obligationsDigest:integrated.obligationsDigest,localArtifacts});
    task.criteria.push(criterion as any);
    task.description='Independently verify the exact accepted integrated candidate, read-only. '+
      'The controller supplies authenticated registered CI evidence for this exact source in the frozen context. '+
      'Inspect that evidence for the registered_ci obligations; do not provision a database, patch system tools, or claim you ran those checks locally. '+
      'Execute every local obligation independently and publish its own actual evidence artifact. '+
      'Publish evidence/verification.json with passed:true only when local checks and the supplied canonical CI evidence satisfy the original requirements; '+
      'use passed:false for any failure, missing evidence or source mismatch. A copied CI report is never authority. '+
      'Preserve the candidate; changes invalidate inherited CI. Original requirements: '+JSON.stringify(result.requirements)+
      '\nRegistered finite controls: '+JSON.stringify(registration.contract.obligations)+
      '\nOriginal verification scope (the registered CI execution wording above supersedes only unavailable duplicate database execution): '+task.description;
    if(task.description.length>20000)reject('READINESS_QA_CONTEXT_TOO_LARGE');
  }
  return result;
}
