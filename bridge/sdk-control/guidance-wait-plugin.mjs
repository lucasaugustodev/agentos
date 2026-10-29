import {defineTool} from '/opt/deepseek-harness/app/node_modules/@deepseek-ai/dsh-tools/lib/index.js';
export const name='agentos-guidance-wait';
// The deadline is fixed per attempt, so once it has passed every call returns
// at once. Read as a bounded wait, an agent called it 444 times in 42 minutes
// (crenteflix, 2026-10-27), each call a paid model turn with no work done.
export const DEADLINE_NOTE='The guidance window for this attempt has closed: no supervisor guidance will arrive, and calling wait_for_guidance again returns this same result. Continue with the tools you have, or finish now with your report, real evidence and limitations (for example a dependency that needs network access).';
export const inject=['tools','systemPrompt','guidanceWaitBroker'];
export function apply(ctx){
 const broker=ctx.get('guidanceWaitBroker');if(broker?.available!==true)return;
 ctx.tools.register(defineTool({name:'wait_for_guidance',
  description:'Wait for an actually queued supervisor guidance notification until the fixed runtime deadline. This tool grants no authority and does not consume guidance; the SDK delivers guidance on its next step.',
  parameters:{},isConcurrencySafe:()=>false,
  output:{schema:{type:'object',additionalProperties:false,properties:{status:{type:'string',enum:['guidance_queued','deadline','runtime_closed'],required:true},actionId:{type:'string'},messageId:{type:'string'},note:{type:'string'}}},render:(_args,value)=>[{type:'text',text:JSON.stringify(value)}]},
  execute:async(_args,exec)=>{const value=await broker.wait(exec.agent,exec.signal);return value?.status==='deadline'?{...value,note:DEADLINE_NOTE}:value;}
 }));
 ctx.systemPrompt.section({name:'tool:wait_for_guidance',order:ctx.systemPrompt.getSectionOrder('TOOL_READ'),text:()=> 'When the task requires live supervisor guidance, call wait_for_guidance instead of repeated shell sleeps. guidance_queued only means the validated guidance is pending; read the actual next-step input before acting. deadline means no guidance will arrive in this attempt: do not call wait_for_guidance again; continue on your own or finish with your report and limitations. It is not task acceptance or a request to cancel.'});
 ctx.provide('guidanceWaitToolReady',{registered:true});
}
