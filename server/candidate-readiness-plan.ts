import {DomainError} from './contracts.js';
import type {ProposedPlan} from './objective-contracts.js';

/** Source joins prepare inputs for a later delivery. Only the unique integration
 * downstream of every source producer can carry the batch readiness contract.
 * Neither array order, task names nor matching requirement labels prove that. */
export function finalCandidateIntegration(plan:ProposedPlan){
  const reject=():never=>{throw new DomainError('READINESS_SCOPE_UNMAPPED','READINESS_SCOPE_UNMAPPED',409);};
  const byKey=new Map(plan.tasks.map(task=>[task.key,task]));
  if(byKey.size!==plan.tasks.length)reject();
  const visiting=new Set<string>(),ancestors=new Map<string,Set<string>>();
  function parents(key:string):Set<string>{
    const cached=ancestors.get(key);if(cached)return cached;
    const task=byKey.get(key);if(!task||visiting.has(key))reject();
    visiting.add(key);const result=new Set<string>();
    for(const dependency of task!.dependencies){
      result.add(dependency);for(const ancestor of parents(dependency))result.add(ancestor);
    }
    visiting.delete(key);ancestors.set(key,result);return result;
  }
  for(const task of plan.tasks)parents(task.key);
  const producers=plan.tasks.filter(task=>task.stage==='work'||task.stage==='integrate');
  const final=producers.filter(task=>task.stage==='integrate'&&
    producers.every(producer=>producer.key===task.key||parents(task.key).has(producer.key)));
  if(final.length!==1)reject();
  return final[0];
}
