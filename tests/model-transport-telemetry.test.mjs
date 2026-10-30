import test from 'node:test';
import assert from 'node:assert/strict';
import { openAICompletionsFallback } from '../bridge/openai-completions-fallback.mjs';
import { createModelTransportTelemetry, MAX_MODEL_TELEMETRY_BYTES } from '../bridge/model-transport-telemetry.mjs';
import { toGeneration } from '../bridge/runtime-gateway.mjs';
import { glmModelProfile } from '../bridge/glm-model-profile.mjs';
import { GLM_ACCOUNT, withAccountRequestGate, withGlmRequestSlot } from '../bridge/gated-generation.mjs';

const requestId='11111111-2222-4333-8444-555555555555';
const model={id:'glm-5.3-flash',baseUrl:'https://private-provider.invalid/v1',...glmModelProfile(GLM_ACCOUNT,'glm-5.3-flash')};
const bytes=value=>Buffer.from(typeof value==='string'?value:'data: '+JSON.stringify(value)+'\n\n');
const jsonReply=(usage,content='private returned text')=>new Response(JSON.stringify({choices:[{message:{content},finish_reason:'stop'}],...(usage===undefined?{}:{usage})}),{headers:{'content-type':'application/json'}});
const tick=()=>new Promise(resolve=>setImmediate(resolve));
const terminal=events=>events.filter(e=>e.kind==='MODEL_TRANSPORT_FINISHED');

test('effective serialized GLM wire and gate share identity despite DSH off/8192 defaults',async()=>{
  let time=0,wire,fetches=0,releases=0;const events=[];
  const emit=(kind,payload)=>events.push({kind,payload});
  const input=toGeneration({reasoning_effort:'off',max_tokens:8192,
    messages:[{role:'system',content:'private system instruction'},{role:'user',content:'private user input'}],
    tools:[{type:'function',function:{name:'private_tool_name',description:'private schema description',parameters:{type:'object'}}}]},
    {accountId:GLM_ACCOUNT,model:model.id,reasoning:'low'});
  const reply={ok:true,status:200,headers:new Headers({'content-type':'text/event-stream','secret':'private response header'}),body:{async *[Symbol.asyncIterator](){
    time=40;yield bytes(': private keepalive\n\n');
    time=50;yield bytes({choices:[{delta:{reasoning_content:'private reasoning'}}]});
    time=60;yield bytes({choices:[{delta:{tool_calls:[{index:0,id:'private_call',function:{name:'private_tool_name',arguments:'{"private_argument":"secret"}'}}]}}]});
    time=70;yield bytes({choices:[{delta:{},finish_reason:'tool_calls'}],usage:{prompt_tokens:100,completion_tokens:20,total_tokens:120,prompt_tokens_details:{cached_tokens:5},completion_tokens_details:{reasoning_tokens:7},private:'private usage field'}});
    time=75;yield bytes('data: [DONE]\n\n');time=80;
  }}};
  const provider=openAICompletionsFallback({telemetryNow:()=>time,fetch:async(_url,options)=>{
    fetches++;wire=JSON.parse(options.body);time=30;return reply;
  }});
  const result=await withAccountRequestGate({...input,runId:'fixture-run',requestId},undefined,signal=>provider.streamSimple(model,
    {systemPrompt:input.system,messages:input.messages,tools:input.tools},
    {apiKey:'private-api-key',signal,reasoning:input.reasoning,maxTokens:input.maxOutputTokens,
      onTelemetry:(kind,payload)=>emit(kind,{...payload,requestId})}).result(),emit,
    {telemetryNow:()=>time,acquire:async({onState})=>{
      time=10;onState({state:'queued',position:1,active:6});time=20;onState({state:'acquired',active:6});
      return {release:()=>{releases++;}};
    }});
  assert.equal(result.stopReason,'toolUse');assert.equal(fetches,1);assert.equal(releases,1);
  const sent=events.find(e=>e.kind==='MODEL_WIRE_REQUESTED').payload;
  assert.equal(wire.max_tokens,32768);assert.equal(wire.reasoning_effort,'low');
  assert.deepEqual(wire.thinking,{type:'enabled',clear_thinking:false});
  assert.deepEqual(sent.wire,{model:wire.model,stream:true,maxTokens:32768,reasoningEffort:'low',thinkingType:'enabled',clearThinking:false,
    requestBytes:Buffer.byteLength(JSON.stringify(wire)),messageCount:2,toolCount:1});
  assert.ok(events.every(e=>e.payload.requestId===requestId));
  assert.deepEqual(events.filter(e=>e.kind.startsWith('MODEL_REQUEST_')||e.kind==='MODEL_QUEUE_WAIT').map(e=>e.payload.gateElapsedMs),[10,20,80]);
  const receipt=terminal(events)[0].payload;
  assert.deepEqual(receipt.timings,{fetchStartMs:0,headersMs:10,firstBodyMs:20,firstSemanticMs:30,lastSemanticMs:40,bodyEndMs:60,finishedMs:60});
  assert.equal(receipt.outcome,'success');assert.equal(receipt.bodyFormat,'sse');
  assert.deepEqual(receipt.usage,{inputTokens:100,outputTokens:20,totalTokens:120,cachedInputTokens:5,reasoningTokens:7});
  assert.equal(receipt.counters.reasoningCharacters,'private reasoning'.length);
  assert.equal(receipt.counters.toolCharacters,'{"private_argument":"secret"}'.length);
  assert.equal(receipt.counters.characters,0);
  assert.ok(events.every(e=>Buffer.byteLength(JSON.stringify(e.payload))<MAX_MODEL_TELEMETRY_BYTES));
  const serialized=JSON.stringify(events);
  for(const secret of ['private-provider','private system','private user','private_tool_name','private schema','private reasoning','private_call','private_argument','secret','private-api-key','private response','private usage'])assert.ok(!serialized.includes(secret),secret);
});

test('JSON body timing does not fabricate semantic deltas and unavailable usage remains null',async()=>{
  const events=[];let time=0;
  const result=await openAICompletionsFallback({telemetryNow:()=>++time,fetch:async()=>jsonReply(undefined)})
    .streamSimple(model,{messages:[]},{onTelemetry:(kind,payload)=>events.push({kind,payload})}).result();
  assert.equal(result.content[0].text,'private returned text');
  const receipt=terminal(events)[0].payload;
  assert.equal(receipt.outcome,'success');assert.equal(receipt.bodyFormat,'json');assert.equal(receipt.usage,null);
  assert.equal(receipt.timings.firstSemanticMs,null);assert.equal(receipt.timings.lastSemanticMs,null);
  assert.ok(receipt.timings.firstBodyMs!==null&&receipt.timings.bodyEndMs!==null);
  assert.ok(!JSON.stringify(events).includes('private returned text'));
});

test('retry receipts preserve original attempt count and do not copy error bodies or headers',async()=>{
  let calls=0;const events=[];
  const fetch=async()=>++calls===1?new Response('{"private":"private error body"}',{status:429,headers:{'retry-after':'0','private':'private header'}}):jsonReply({prompt_tokens:2});
  await openAICompletionsFallback({fetch,random:()=>0}).streamSimple(model,{messages:[]},{onTelemetry:(kind,payload)=>events.push({kind,payload})}).result();
  assert.equal(calls,2);
  assert.deepEqual(terminal(events).map(e=>[e.payload.attempt,e.payload.outcome,e.payload.httpStatus]),[[1,'retry',429],[2,'success',200]]);
  assert.deepEqual(terminal(events)[1].payload.usage,{inputTokens:2,outputTokens:null,totalTokens:null,cachedInputTokens:null,reasoningTokens:null});
  assert.ok(!JSON.stringify(events).includes('private'));
});

test('terminal authentication error records only a typed outcome and never adds a retry',async()=>{
  let calls=0;const events=[];
  await assert.rejects(openAICompletionsFallback({fetch:async()=>{calls++;return new Response('private body',{status:401});}})
    .streamSimple(model,{messages:[]},{apiKey:'private-secret',onTelemetry:(kind,payload)=>events.push({kind,payload})}).result(),{code:'AUTH_REQUIRED'});
  assert.equal(calls,1);assert.equal(terminal(events).length,1);
  assert.equal(terminal(events)[0].payload.outcome,'error');assert.equal(terminal(events)[0].payload.code,'AUTH_REQUIRED');
  assert.equal(terminal(events)[0].payload.timings.firstBodyMs,null,'error-body classification is not model output streaming');
  assert.ok(!JSON.stringify(events).includes('private'));
});

function clock(){let time=0,seq=0;const timers=new Map();return {now:()=>time,pending:timers,
  timers:{setTimeout(fn,delay){const id=++seq;timers.set(id,{at:time+delay,fn});return id;},clearTimeout(id){timers.delete(id);}},
  advance(ms){time+=ms;for(const [id,t] of [...timers])if(t.at<=time){timers.delete(id);t.fn();}}};}
function streamed({signal,cancel}={}){
  const c=clock(),events=[];let control,calls=0,cancellations=0;
  const response=new Response(new ReadableStream({start(value){control=value;},cancel(reason){cancellations++;return cancel?.(reason);}}),{headers:{'content-type':'text/event-stream'}});
  const result=openAICompletionsFallback({fetch:async()=>{calls++;return response;},now:c.now,telemetryNow:c.now,timers:c.timers,idleTimeoutMs:100})
    .streamSimple(model,{messages:[]},{signal,onTelemetry:(kind,payload)=>events.push({kind,payload})}).result();
  const outcome=result.then(value=>({value}),error=>({error}));
  return {c,events,outcome,enqueue:value=>control.enqueue(bytes(value)),close:()=>control.close(),calls:()=>calls,cancellations:()=>cancellations};
}

test('keepalive bytes and usage are not first/last semantic delta; idle timeout still owns termination',async()=>{
  const f=streamed();await tick();f.c.advance(40);f.enqueue(': keepalive\n\n');await tick();
  f.c.advance(30);f.enqueue({usage:{completion_tokens:0},choices:[]});await tick();
  f.c.advance(31);const result=await f.outcome;
  assert.equal(result.error.code,'MODEL_IDLE_TIMEOUT');assert.equal(f.calls(),1);assert.equal(f.cancellations(),1);
  const receipt=terminal(f.events)[0].payload;
  assert.equal(receipt.outcome,'timeout');assert.equal(receipt.code,'MODEL_IDLE_TIMEOUT');
  assert.equal(receipt.timings.firstBodyMs,40);assert.equal(receipt.timings.firstSemanticMs,null);assert.equal(receipt.timings.lastSemanticMs,null);assert.equal(receipt.timings.bodyEndMs,null);
  assert.equal(receipt.usage.outputTokens,0);assert.equal(receipt.usage.inputTokens,null);
  assert.equal(f.c.pending.size,0);
});

test('cancellation receipt follows actual transport cleanup and cannot release a permit early',async()=>{
  const caller=new AbortController();let finishCancel,releases=0;
  const f=streamed({signal:caller.signal,cancel:()=>new Promise(resolve=>{finishCancel=resolve;})});
  const held=withGlmRequestSlot({runId:'cancel',requestId},()=>f.outcome,
    {acquire:async({onState})=>{onState({state:'acquired'});return {release:()=>releases++};}});
  await tick();f.c.advance(20);f.enqueue({choices:[{delta:{reasoning_content:'private reasoning'}}]});await tick();
  caller.abort();await tick();assert.equal(terminal(f.events).length,0);assert.equal(releases,0);
  f.c.advance(10);finishCancel();const result=await held;
  assert.equal(result.error.code,'CANCELLED');assert.equal(releases,1);
  const receipt=terminal(f.events)[0].payload;
  assert.equal(receipt.outcome,'cancelled');assert.equal(receipt.timings.firstSemanticMs,20);assert.equal(receipt.timings.lastSemanticMs,20);assert.equal(receipt.timings.bodyEndMs,null);assert.equal(receipt.timings.finishedMs,30);
  assert.ok(!JSON.stringify(f.events).includes('private reasoning'));
});

test('malformed body remains terminal without replay and retains only content-free timing',async()=>{
  const f=streamed();await tick();f.c.advance(5);f.enqueue('data: private malformed body\n\n');
  const result=await f.outcome;assert.equal(result.error.code,'MODEL_INCOMPLETE_RESPONSE');assert.equal(f.calls(),1);
  const receipt=terminal(f.events)[0].payload;
  assert.equal(receipt.outcome,'error');assert.equal(receipt.timings.firstBodyMs,5);assert.equal(receipt.timings.firstSemanticMs,null);
  assert.ok(!JSON.stringify(f.events).includes('private malformed'));
});

test('throwing or rejecting telemetry and broken clocks cannot change completion or gate ownership',async()=>{
  for(const observe of [()=>{throw Error('observer failed');},()=>Promise.reject(Error('observer failed'))]){
    let releases=0;
    const result=await withGlmRequestSlot({runId:'observer',requestId,emit:observe},()=>openAICompletionsFallback({telemetryNow:()=>{throw Error('clock failed');},fetch:async()=>jsonReply(undefined,'ok')})
      .streamSimple(model,{messages:[]},{onTelemetry:observe}).result(),
      {telemetryNow:()=>{throw Error('clock failed');},acquire:async({onState})=>{onState({state:'queued',position:1});onState({state:'acquired'});return {release:()=>releases++};}});
    assert.equal(result.content[0].text,'ok');assert.equal(releases,1);await tick();
  }
});

test('telemetry whitelist drops malformed/oversized strings and preserves unavailable counts',()=>{
  const events=[];let time=0;
  const observer=createModelTransportTelemetry(JSON.stringify({model:'private '.repeat(10000),stream:true,max_tokens:'private',reasoning_effort:'private',thinking:{type:'private',clear_thinking:'private'},messages:[],tools:[],headers:{authorization:'private'}}),
    (kind,payload)=>events.push({kind,payload}),()=>time);
  const attempt=observer.attempt(1);time=10;attempt.usage({prompt_tokens:-1,completion_tokens:'private',total_tokens:NaN,completion_tokens_details:{reasoning_tokens:Infinity}});attempt.finish('success');attempt.finish('error','AUTH_REQUIRED');
  assert.equal(terminal(events).length,1);
  assert.deepEqual(terminal(events)[0].payload.usage,{inputTokens:null,outputTokens:null,totalTokens:null,cachedInputTokens:null,reasoningTokens:null});
  assert.equal(events[0].payload.wire.model,null);assert.ok(!JSON.stringify(events).includes('private'));
  assert.ok(events.every(e=>Buffer.byteLength(JSON.stringify(e.payload))<MAX_MODEL_TELEMETRY_BYTES));
});
