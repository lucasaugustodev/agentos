import test from 'node:test';
import assert from 'node:assert/strict';
import {readCompletionsStream,fromCompletionsResponse,toCompletionsMessages,openAICompletionsFallback} from '../bridge/openai-completions-fallback.mjs';
import {completion,toGeneration} from '../bridge/runtime-gateway.mjs';
import {validateModelPolicy} from '../bridge/model-policy.mjs';
import {captureRuntimeHandoff,handoffNotes} from '../bridge/runtime-handoff.mjs';
const sse=(events)=>({async *[Symbol.asyncIterator](){for(const event of events)yield Buffer.from(`data: ${JSON.stringify(event)}\n\n`);}});
test('EOF and malformed tools never dispatch a partial response',async()=>{
  await assert.rejects(readCompletionsStream(sse([{choices:[{delta:{content:'partial'}}]}])),{code:'MODEL_INCOMPLETE_RESPONSE'});
  for(const args of ['{','[]','null','"text"']) assert.throws(()=>fromCompletionsResponse({choices:[{
    message:{tool_calls:[{id:'c',function:{name:'write',arguments:args}}]},finish_reason:'tool_calls'
  }]}),{code:'MODEL_INCOMPLETE_RESPONSE'});
  assert.throws(()=>fromCompletionsResponse({choices:[{message:{tool_calls:[{id:'c',function:{name:'write',arguments:'{'}}]},finish_reason:'length'}]}),{code:'MODEL_OUTPUT_LIMIT'});
  assert.throws(()=>validateModelPolicy({version:1,mode:'glm-onyl'}),{code:'MODEL_POLICY_INVALID'});
});
test('GLM reasoning survives both wire adapters and is absent from progress events',async()=>{
  const events=[];
  const result=await readCompletionsStream(sse([
    {choices:[{delta:{reasoning_content:'private reasoning'}}]},
    {choices:[{delta:{tool_calls:[{index:0,id:'c',function:{name:'read',arguments:'{"path":"x"}'}}]},finish_reason:'tool_calls'}]}
  ]),undefined,p=>events.push(p));
  const wire=completion(result,'glm-5.3-flash')[0].choices[0].delta;
  const generated=toGeneration({messages:[wire]},{model:'glm-5.3-flash'});
  const outbound=toCompletionsMessages('',generated.messages);
  assert.equal(outbound[0].reasoning_content,'private reasoning');
  assert.equal(outbound[0].tool_calls[0].function.arguments,'{"path":"x"}');
  assert.ok(!JSON.stringify(events).includes('private reasoning'));
  assert.equal(events.at(-1).reasoningCharacters,17);
});
test('an incomplete response with material never retries or dispatches a tool',async()=>{
  let calls=0;const retries=[];
  const provider=openAICompletionsFallback({random:()=>0,fetch:async()=>{
    calls++;
    const data=calls===1?[{choices:[{delta:{content:'partial'}}]}]:[{choices:[{delta:{content:'complete'},finish_reason:'stop'}]}];
    return {ok:true,headers:new Headers({'content-type':'text/event-stream'}),body:sse(data)};
  }});
  await assert.rejects(provider.streamSimple({id:'glm-5.3-flash',baseUrl:'https://provider.example/v1'},
    {messages:[]},{apiKey:'fixture',onRetry:r=>retries.push(r)}).result(),{code:'MODEL_INCOMPLETE_RESPONSE'});
  assert.equal(calls,1);assert.deepEqual(retries,[]);
});
test('trusted handoff binds unverified notes to the run and does not manufacture acceptance',async()=>{
  const identity={runId:'run',runFence:'fence',workspace:'test/work'};
  const notes={completed:'Edited module and ran failing test',remaining:'One defect remains',nextAction:'Fix the observed assertion'};
  const result=await captureRuntimeHandoff(identity,async()=>({text:JSON.stringify({...notes,passed:true,permission:'all'}),digest:'a'.repeat(64),size:100}));
  assert.equal(result.runFence,'fence');assert.equal(result.verified,false);
  assert.deepEqual(result.notes,notes);assert.equal(result.passed,undefined);
  assert.equal(handoffNotes({...notes,nextAction:'api_key=secret'}),null);
  assert.equal(await captureRuntimeHandoff(identity,async()=>{throw Error('symlink');}),null);
});
