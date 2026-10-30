import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,writeFile,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {validateModelPolicy,assertModelRoute} from '../bridge/model-policy.mjs';
import {operatorModelProfile,workerModelSettings} from '../bridge/operator-model-profile.mjs';
import {openAICompletionsFallback,toCompletionsMessages,fromCompletionsResponse,readCompletionsStream} from '../bridge/openai-completions-fallback.mjs';
import {toGeneration,completion} from '../bridge/runtime-gateway.mjs';
import {openCodeRoute,openCodeCompletionPayload} from '../bridge/opencode-executor.mjs';
import {withAccountRequestGate,GLM_ACCOUNT} from '../bridge/gated-generation.mjs';
import {acquireGlmRequestSlot} from '../bridge/glm-request-slot.mjs';
import {createGlmRequestGate} from '../bridge/glm-request-gate.mjs';

const policy={version:2,mode:'exclusive-model',accountId:'agentos-api-11111111-2222-4333-8444-555555555555',
  bridgeId:'agentos:api:11111111-2222-4333-8444-555555555555',model:'deepseek-v4-pro-0813',requestLimit:6,sessionLimit:12};
async function configured(t,value=policy){
  const dir=await mkdtemp(tmpdir()+'/agentos-deepseek-'),previous=process.env.AGENTOS_MODEL_POLICY_FILE;
  process.env.AGENTOS_MODEL_POLICY_FILE=dir+'/policy.json';
  if(value)await writeFile(process.env.AGENTOS_MODEL_POLICY_FILE,JSON.stringify(value));
  t.after(async()=>{if(previous===undefined)delete process.env.AGENTOS_MODEL_POLICY_FILE;else process.env.AGENTOS_MODEL_POLICY_FILE=previous;await rm(dir,{recursive:true,force:true});});
  return dir;
}
const reply=(message={content:'ok'})=>new Response(JSON.stringify({choices:[{message,finish_reason:message.tool_calls?'tool_calls':'stop'}]}),{headers:{'content-type':'application/json'}});
const tick=()=>new Promise(resolve=>setImmediate(resolve));
async function until(predicate){for(let n=0;n<1000;n++){if(await predicate())return;await new Promise(resolve=>setTimeout(resolve,5));}assert.fail('fixture did not converge');}

test('v2 selects exactly one operator-owned API pair; model names grant no profile',async t=>{
  await configured(t);
  assert.deepEqual(validateModelPolicy(policy),policy);
  for(const change of [{version:1},{mode:'glm-only'},{model:'deepseek-v4-pro'},{bridgeId:GLM_ACCOUNT},
    {accountId:'harness:openai-codex'},{requestLimit:7},{sessionLimit:13},{extra:true}])
    assert.throws(()=>validateModelPolicy({...policy,...change}),/MODEL_POLICY_INVALID/);
  assert.throws(()=>assertModelRoute(GLM_ACCOUNT,'glm-5.3-flash'),/MODEL_POLICY_GLM_ONLY/);
  assert.throws(()=>assertModelRoute('unverified',policy.model),/MODEL_POLICY_GLM_ONLY/);
  assert.deepEqual(operatorModelProfile('unverified',policy.model),{});
  assert.deepEqual(workerModelSettings('unverified',policy.model),{reasoning:'high',maxOutputTokens:8192});
  assert.deepEqual(workerModelSettings(policy.bridgeId,policy.model),{reasoning:'high',maxOutputTokens:32768});
  assert.deepEqual(openCodeRoute({accountId:policy.accountId,model:policy.model}),{accountId:policy.bridgeId,model:policy.model});
  assert.throws(()=>openCodeRoute({accountId:GLM_ACCOUNT,model:'glm-5.3-flash'}),/INVALID_ROUTE/);
});

for(const [effort,expected] of [['low','high'],['high','high'],['xhigh','max']])test(`DeepSeek actual serialized wire ${effort} and telemetry use its thinking contract`,async t=>{
  await configured(t);let wire;const events=[];
  const input=toGeneration({max_tokens:1,reasoning_effort:'off',messages:[{role:'user',content:'private input'}]},
    {accountId:policy.bridgeId,model:policy.model,reasoning:effort});
  const result=await openAICompletionsFallback({fetch:async(_url,options)=>{wire=JSON.parse(options.body);return reply();}})
    .streamSimple({id:policy.model,baseUrl:'https://fixture.invalid/v1',...operatorModelProfile(policy.bridgeId,policy.model)},
      {systemPrompt:'private system',messages:input.messages},
      {apiKey:'fixture-only',maxTokens:input.maxOutputTokens,reasoning:input.reasoning,onTelemetry:(kind,payload)=>events.push({kind,payload})}).result();
  assert.equal(result.stopReason,'stop');assert.equal(wire.max_tokens,32768);
  assert.equal(wire.reasoning_effort,expected);assert.deepEqual(wire.thinking,{type:'enabled'});
  const observed=events.find(e=>e.kind==='MODEL_WIRE_REQUESTED').payload.wire;
  assert.equal(observed.model,policy.model);assert.equal(observed.maxTokens,32768);
  assert.equal(observed.reasoningEffort,expected);assert.equal(observed.thinkingType,'enabled');
  assert.equal(observed.clearThinking,null);
  assert.ok(!JSON.stringify(events).includes('private input'));assert.ok(!JSON.stringify(events).includes('fixture-only'));
});

test('planner budget remains 8192 and explicit disabled thinking omits effort and Z.ai options',async t=>{
  await configured(t);let wire;
  await openAICompletionsFallback({fetch:async(_url,options)=>{wire=JSON.parse(options.body);return reply();}})
    .streamSimple({id:policy.model,baseUrl:'https://fixture.invalid/v1',...operatorModelProfile(policy.bridgeId,policy.model)},
      {messages:[]},{maxTokens:8192,reasoning:'off'}).result();
  assert.equal(wire.max_tokens,8192);assert.deepEqual(wire.thinking,{type:'disabled'});assert.equal(wire.reasoning_effort,undefined);
});

test('all assistant reasoning survives both directions and subsequent tool turns byte-for-byte',async t=>{
  await configured(t);
  const reason1=' reasoning Î±\n'.repeat(1200),reason2='tool reasoning Ã©\n'.repeat(1200);
  const normal=fromCompletionsResponse({choices:[{message:{content:'previous answer',reasoning_content:reason1},finish_reason:'stop'}]});
  const tool=fromCompletionsResponse({choices:[{message:{reasoning_content:reason2,tool_calls:[{id:'call1',function:{name:'read',arguments:'{"path":"fixture"}'}}]},finish_reason:'tool_calls'}]});
  const history=[completion(normal,policy.model)[0].choices[0].delta,completion(tool,policy.model)[0].choices[0].delta,
    {role:'tool',tool_call_id:'call1',content:'fixture bytes'}];
  const input=toGeneration({messages:history},{accountId:policy.bridgeId,model:policy.model});
  const rebuilt=toCompletionsMessages('',input.messages);
  assert.equal(rebuilt[0].reasoning_content,reason1);assert.equal(rebuilt[1].reasoning_content,reason2);
  assert.equal(rebuilt[1].tool_calls[0].id,'call1');assert.equal(rebuilt[2].tool_call_id,'call1');
  const payload={model:policy.model,messages:rebuilt,tools:[{type:'function',function:{name:'read'}}],max_completion_tokens:1,thinking:{type:'disabled',clear_thinking:true}};
  const scoped=openCodeCompletionPayload(payload,{accountId:policy.bridgeId,model:policy.model});
  assert.deepEqual(scoped.messages,rebuilt);assert.deepEqual(scoped.thinking,{type:'enabled'});
  assert.equal(scoped.max_tokens,32768);assert.equal(scoped.max_completion_tokens,undefined);assert.equal(scoped.reasoning_effort,'high');
});

test('explicit empty reasoning is preserved and absence is never fabricated',async()=>{
  const json=fromCompletionsResponse({choices:[{message:{content:'ok',reasoning_content:''},finish_reason:'stop'}]});
  const stream=await readCompletionsStream({async *[Symbol.asyncIterator](){yield Buffer.from('data: '+JSON.stringify({choices:[{delta:{content:'ok',reasoning_content:''},finish_reason:'stop'}]})+'\n\n');}});
  for(const message of [json,stream]){
    const wire=completion(message,policy.model)[0].choices[0].delta;
    const input=toGeneration({messages:[wire]},{accountId:'unconfigured',model:policy.model});
    assert.equal(toCompletionsMessages('',input.messages)[0].reasoning_content,'');
  }
  const absent=fromCompletionsResponse({choices:[{message:{content:'ok'},finish_reason:'stop'}]});
  assert.equal(Object.hasOwn(completion(absent,policy.model)[0].choices[0].delta,'reasoning_content'),false);
});

test('mixed valid calls share six permits and cancellation retains an owned permit until body unwinds',async t=>{
  const dir=await configured(t,null),socketPath=dir+'/gate.sock';
  const gate=await createGlmRequestGate({socketPath});t.after(()=>gate.close());
  const dependencies={acquire:args=>acquireGlmRequestSlot({...args,socketPath})};
  const releases=[],events=[],calls=[];let bodies=0;
  const invoke=(accountId,model,index,signal)=>withAccountRequestGate({accountId,model,runId:'mixed-'+index},signal,async()=>{
    bodies++;await new Promise(resolve=>{releases[index]=resolve;});bodies--;return index;
  },(kind,payload)=>events.push({kind,payload}),dependencies);
  for(let i=0;i<3;i++)calls.push(invoke(GLM_ACCOUNT,'glm-5.3-flash',i));
  await until(()=>bodies===3);
  // This fixture changes only admission policy; already-owned transport bodies
  // must still unwind. It is not an online installation/drain procedure.
  await writeFile(dir+'/policy.json',JSON.stringify(policy));
  for(let i=3;i<6;i++)calls.push(invoke(policy.bridgeId,policy.model,i));
  await until(()=>bodies===6);assert.equal((await gate.status()).active,6);
  const abort=new AbortController();
  calls.push(invoke(policy.bridgeId,policy.model,6,abort.signal).catch(e=>e));
  await until(async()=>(await gate.status()).queued===1);
  assert.equal(releases[6],undefined);releases[0]();await calls[0];
  await until(()=>typeof releases[6]==='function');abort.abort();await tick();
  assert.equal((await gate.status()).active,6);assert.equal(bodies,6);
  releases[6]();await calls[6];await until(async()=>(await gate.status()).active===5);
  for(let i=1;i<6;i++)releases[i]();await Promise.all(calls);
  await until(async()=>(await gate.status()).active===0);
  assert.ok(events.some(e=>e.kind==='MODEL_REQUEST_ADMITTED'&&e.payload.accountId===policy.bridgeId&&e.payload.model===policy.model));
  assert.equal((await gate.status()).maxObserved,6);
  let effects=0;
  assert.throws(()=>withAccountRequestGate({accountId:policy.bridgeId,model:'renamed-privileged'},undefined,()=>effects++,()=>{},dependencies),/MODEL_POLICY_GLM_ONLY/);
  assert.equal(effects,0);
});
