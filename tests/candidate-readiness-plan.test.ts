import {expect,it} from 'vitest';
import {compileDelivery} from '../server/incremental-planning.js';
import {finalCandidateIntegration} from '../server/candidate-readiness-plan.js';

function fixture(){
  const request:any={mode:'execute',message:'Combine three source contributions and their wiring.',
    workspaces:['agentos/base'],requirements:Array.from({length:6},(_,i)=>`Preserve and verify original requirement ${i+1}.`)};
  const deliveries=Array.from({length:6},(_,i)=>({kind:'delivery',key:`part-${i+1}`,requirementId:`req-${i%3+1}`,
    title:`Implement source contribution ${i+1}`,agentId:'engineering.backend.node',writePaths:[`server/part-${i+1}.ts`],
    action:'Implement the assigned source and preserve its dependencies.',
    verification:'Run the required regression checks and publish their real results.',
    dependsOn:i===3?['part-1','part-2']:i===4?['part-2','part-4']:i===5?['part-3','part-4']:[]}));
  return compileDelivery({deliveries},request);
}

it('selects the accumulated integration in the real compiler DAG with three intermediate joins',()=>{
  const plan=fixture(),before=structuredClone(plan);
  expect(plan.tasks.filter(t=>t.stage==='integrate').map(t=>t.key)).toEqual(['join-4','join-5','join-6','integrate']);
  expect(finalCandidateIntegration(plan).key).toBe('integrate');
  expect(plan).toEqual(before);
  expect(plan.incremental).toMatchObject({selectedRequirementIds:['req-1','req-2','req-3'],pendingRequirementIds:['req-4','req-5','req-6']});
  expect(plan.requirements).toHaveLength(6);
});
it('selects by dependencies when a join has full batch coverage and the task array is reordered',()=>{
  const plan=fixture();
  expect(plan.tasks.find(t=>t.key==='join-6')?.covers.sort()).toEqual(['req-1','req-2','req-3']);
  plan.tasks.reverse();expect(finalCandidateIntegration(plan).key).toBe('integrate');
});
it.each(['missing-edge','foreign-source','duplicate-key','cycle','unknown-parent','no-final'])('rejects a non-accumulated or invalid DAG: %s',mode=>{
  const plan=fixture(),final=plan.tasks.find(t=>t.key==='integrate')!;
  if(mode==='missing-edge')final.dependencies=final.dependencies.filter(key=>key!=='deliver-6');
  if(mode==='foreign-source')plan.tasks.push({...structuredClone(plan.tasks.find(t=>t.stage==='work')!),key:'unmapped-source',dependencies:[],covers:['req-4']});
  if(mode==='duplicate-key')plan.tasks.push(structuredClone(final));
  if(mode==='cycle')plan.tasks.find(t=>t.key==='deliver-1')!.dependencies=['integrate'];
  if(mode==='unknown-parent')final.dependencies.push('missing-source');
  if(mode==='no-final')plan.tasks=plan.tasks.filter(t=>t.key!=='integrate'&&t.key!=='verify');
  expect(()=>finalCandidateIntegration(plan)).toThrow('READINESS_SCOPE_UNMAPPED');
});
