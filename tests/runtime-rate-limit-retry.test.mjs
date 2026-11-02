import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {withGlmRequestSlot} from '../bridge/gated-generation.mjs';
import {modelTransportFailure,markModelTransportFailure,markModelRateLimitFailure,isModelRateLimitFailure,generationObserver} from '../bridge/model-transport-failure.mjs';
import {openAICompletionsFallback} from '../bridge/openai-completions-fallback.mjs';
import {WORKER_MODEL_TIMEOUT_MS} from '../bridge/runtime-model-channel.mjs';

// Execute the exact production generation boundary without importing the
// runner's workspace/process adapters or starting any worker/provider process.
const source=await readFile(new URL('../bridge/runtime-runner.mjs',import.meta.url),'utf8');
const helperStart=source.indexOf('export function runtimeGenerationFailureCode(');
const helperEnd=source.indexOf('export async function runState(',helperStart);
assert.ok(helperStart>0&&helperEnd>helperStart,'one bounded helper region');
const helpers=source.slice(helperStart,helperEnd).replace(/^export /gm,'');
const start=source.indexOf('      const generationSignal=requestTimeout?');
const end=source.indexOf("      for (const k of ['input', 'output', 'cacheRead', 'cacheWrite', 'totalTokens'])",start);
assert.ok(start>0&&end>start&&source.indexOf('      const generationSignal=requestTimeout?',start+1)===-1);
const body=source.slice(start,end);
const AsyncFunction=Object.getPrototypeOf(async function(){}).constructor;
const failure=(code,status=429,retryAfterSeconds=33)=>markModelRateLimitFailure(Object.assign(Error(code),{code,status,retryAfterSeconds}));
const result={message:{content:'fixture'}};

function fixture(generate,{pending=0,complete=true,random=()=>0.5,initialElapsed=0,onWait}={}){
  let now=0,active=0,calls=0,maxActive=0,waits=[];
  const events=[],order=[],controller=new AbortController();
  const input={accountId:'fixture',model:'fixture',messages:[{role:'user',content:'immutable task'}]};
  const channel={signal:controller.signal,failureCode:e=>controller.signal.aborted?'CANCELLED':
    /quota|429|rate.limit/i.test(e?.message??'')?'QUOTA_EXCEEDED':e?.code??'MODEL_REQUEST_FAILED'};
  const deps={WORKER_MODEL_TIMEOUT_MS,performance:{now:()=>now},Math:{random},
    requestTimeout:null,channel,params:{runId:'fixture-run'},gatedRequest:true,input,
    requestId:'fixture-request',requestSequence:1,GLM_REQUEST_ADMITTED:Symbol('admitted'),
    transportToolTraceComplete:complete,pendingTransportTools:new Set(Array.from({length:pending},(_,i)=>i)),
    emit:(kind,payload)=>events.push({kind,...payload}),modelTransportFailure,isModelRateLimitFailure,generationObserver,
    generate:async generationInput=>{calls++;order.push('generate');now+=calls===1?initialElapsed:0;return generate(calls,generationInput,controller);},
    withAccountRequestGate:(params,signal,operation,emit)=>withGlmRequestSlot({runId:'fixture-run',signal,emit},operation,{
      acquire:async()=>{active++;maxActive=Math.max(maxActive,active);order.push('acquire');return {release(){active--;order.push('release');}};},
    }),
    delay:async(ms,unused,{signal})=>{assert.equal(active,0,'no gate permit during provider wait');waits.push(ms);order.push('wait');
      onWait?.({controller,signal});signal.throwIfAborted();now+=ms;},
  };
  // Helpers retain the real Math methods; only randomness is deterministic.
  deps.Math=Object.create(Math);deps.Math.random=random;
  const execute=new AsyncFunction(...Object.keys(deps),helpers+'\nlet generationAttempts=0;\n'+body+'\nreturn {result,generationAttempts};');
  return {run:()=>execute(...Object.values(deps)),events,order,waits,input,channel,
    get calls(){return calls;},get active(){return active;},get maxActive(){return maxActive;}};
}

test('real classified 429/Retry-After33 releases its permit, waits, then continues the same immutable generation',async()=>{
  let fetches=0;
  const provider=openAICompletionsFallback({fetch:async()=>{
    fetches++;
    return fetches===1?new Response('{"error":{"message":"private provider text"}}',{status:429,headers:{'retry-after':'33'}}):
      new Response(JSON.stringify({choices:[{message:{content:'done'},finish_reason:'stop'}]}),{headers:{'content-type':'application/json'}});
  }});
  const inputs=[];
  const f=fixture(async(n,input)=>{inputs.push(input);return {message:await provider.streamSimple(
    {id:'fixture',baseUrl:'https://provider.invalid/v1'},{messages:[]},{apiKey:'fixture'}).result()};});
  const output=await f.run();
  assert.equal(output.result.message.content[0].text,'done');assert.equal(output.generationAttempts,2);
  assert.equal(fetches,2);assert.equal(f.active,0);assert.equal(f.maxActive,1);assert.deepEqual(f.waits,[33500]);
  assert.deepEqual(f.order,['acquire','generate','release','wait','acquire','generate','release']);
  assert.deepEqual({...inputs[0],generationAttempt:2},inputs[1]);assert.strictEqual(inputs[0].messages,inputs[1].messages);
  assert.equal(f.events.filter(e=>e.kind==='MODEL_RATE_LIMIT_WAIT').length,1);
  assert.equal(JSON.stringify(f.events).includes('private provider text'),false);
});

test('a 600-second provider minimum is never shortened and remains outside the gate',async()=>{
  const f=fixture(n=>{if(n===1)throw failure('RATE_LIMIT',429,600);return result;});
  assert.equal((await f.run()).result,result);assert.deepEqual(f.waits,[600500]);assert.equal(f.active,0);
});

test('repeated rate limits stop at the720-second deadline without an early retry or spending transport attempts',async()=>{
  const f=fixture(()=>{throw failure('RATE_LIMIT',429,300);});
  await assert.rejects(f.run(),{code:'RATE_LIMIT',status:429});
  assert.equal(f.calls,3);assert.deepEqual(f.waits,[300500,300500]);
  assert.deepEqual(f.events.filter(e=>e.kind==='MODEL_RATE_LIMIT_WAIT').map(e=>e.generationAttempt),[1,2]);
});

test('missing Retry-After uses bounded exponential delay and jitter; zero still avoids a busy loop',async()=>{
  const f=fixture(n=>{if(n<4)throw failure('RATE_LIMIT',429,null);return result;});
  await f.run();assert.deepEqual(f.waits,[5500,10500,20500]);
  const zero=fixture(n=>{if(n===1)throw failure('RATE_LIMIT',429,0);return result;});
  await zero.run();assert.deepEqual(zero.waits,[1500]);
});

test('quota, authentication, malformed metadata and textual rate-limit lookalikes do not acquire retry authority',async()=>{
  for(const error of [failure('QUOTA_EXCEEDED'),failure('AUTH_REQUIRED',401),failure('RATE_LIMIT',503),
    failure('RATE_LIMIT',429,-1),failure('RATE_LIMIT',429,Infinity),failure('RATE_LIMIT',429,'33'),
    Object.assign(Error('RATE_LIMIT 429'),{status:429,retryAfterSeconds:33}),
    Object.assign(Error('RATE_LIMIT'),{code:'RATE_LIMIT',status:429,retryAfterSeconds:33}),failure('RATE_LIMIT',429,720)]){
    const f=fixture(()=>{throw error;});await assert.rejects(f.run(),e=>e===error);assert.equal(f.calls,1);assert.deepEqual(f.waits,[]);
  }
});

test('caller cancellation during the wait causes no subsequent provider request',async()=>{
  const f=fixture(()=>{throw failure('RATE_LIMIT');},{onWait:({controller})=>controller.abort(Object.assign(Error('CANCELLED'),{code:'CANCELLED'}))});
  await assert.rejects(f.run(),{code:'CANCELLED'});assert.equal(f.calls,1);assert.equal(f.active,0);
});

test('unfinished tools or incomplete trace never permit replay of the loop',async()=>{
  for(const options of [{pending:1},{complete:false}]){
    const f=fixture(()=>{throw failure('RATE_LIMIT');},options);await assert.rejects(f.run(),{code:'RATE_LIMIT'});
    assert.equal(f.calls,1);assert.deepEqual(f.waits,[]);
  }
});

test('an incomplete stream never gains retry authority and real transport retries retain their separate bound',async()=>{
  const partial=fixture(()=>{throw failure('MODEL_INCOMPLETE_RESPONSE',200,null);});
  await assert.rejects(partial.run(),{code:'MODEL_INCOMPLETE_RESPONSE'});assert.equal(partial.calls,1);
  const transient=fixture(n=>{if(n===1)throw failure('RATE_LIMIT');throw markModelTransportFailure(Error('socket'),'connection_interrupted');});
  await assert.rejects(transient.run(),/socket/);assert.equal(transient.calls,4);
  assert.deepEqual(transient.waits,[33500,750,1500]);
  const exhausted=transient.events.find(e=>e.kind==='MODEL_TRANSPORT_EXHAUSTED');
  assert.equal(exhausted.generationAttempt,4);assert.equal(exhausted.transportAttempt,3);
});

test('the fourth and tenth rate waits retain monotonic generation and observer identities',async()=>{
  const f=fixture(n=>{if(n<=10)throw failure('RATE_LIMIT',429,33);return result;});
  assert.equal((await f.run()).generationAttempts,11);
  const waits=f.events.filter(e=>e.kind==='MODEL_RATE_LIMIT_WAIT');
  assert.deepEqual(waits.map(e=>e.generationAttempt),Array.from({length:10},(_,i)=>i+1));
  for(const n of [4,10,11]){
    const rows=[];generationObserver((kind,p)=>rows.push(p),{requestId:'same-request',generationAttempt:n})('fixture',{});
    assert.equal(rows[0].generationAttempt,n);
  }
});

test('interleaved transport and rate failures exhaust three transport failures at physical generation five',async()=>{
  const f=fixture(n=>{if(n%2===0)throw failure('RATE_LIMIT');
    throw markModelTransportFailure(Error('socket'),'connection_interrupted');});
  await assert.rejects(f.run(),/socket/);assert.equal(f.calls,5);
  assert.deepEqual(f.waits,[750,33500,1500,33500]);
  const exhausted=f.events.find(e=>e.kind==='MODEL_TRANSPORT_EXHAUSTED');
  assert.equal(exhausted.generationAttempt,5);assert.equal(exhausted.transportAttempt,3);
});

test('the exact recovery consumer accepts legacy exhaustion and new mixed counters only with intact provenance',async()=>{
  const src=await readFile(new URL('../server/transport-task-recovery.ts',import.meta.url),'utf8');
  const begin=src.indexOf('export async function transportCause(db:DB,run:any){');
  const finish=src.indexOf('export async function hasTransportCause(',begin);
  assert.ok(begin>=0&&finish>begin);
  const definition=src.slice(begin,finish).replace('export async function transportCause(db:DB,run:any)',
    'async function transportCause(db,run)');
  const uuid=/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/;
  const consume=new Function('uuid',definition+';return transportCause;')(uuid);
  const run={state:'failed',runtime:'harness',context_id:'context',id:'run',fence:'fence',task_id:'task',error_code:'MODEL_REQUEST_FAILED'};
  const base={version:1,source:'runtime-generation-boundary',runId:'run',runFence:'fence',contextVersion:'context',
    requestId:'11111111-1111-4111-8111-111111111111',generationAttempt:3,reason:'provider_unavailable',
    code:'MODEL_REQUEST_FAILED',responseDelivered:false,traceComplete:true,pendingTools:0,cancelled:false};
  const invoke=payload=>consume({query:async()=>({rows:[{id:'event',payload:{...base,...payload}}]})},run);
  assert.ok(await invoke({}));assert.ok(await invoke({generationAttempt:13,transportAttempt:3}));
  for(const invalid of [{generationAttempt:4},{transportAttempt:2},{generationAttempt:4,transportAttempt:4},
    {generationAttempt:2,transportAttempt:3},{generationAttempt:3.5,transportAttempt:3},
    {generationAttempt:4,transportAttempt:null},{generationAttempt:4,transportAttempt:3,responseDelivered:true},
    {generationAttempt:4,transportAttempt:3,pendingTools:1},{generationAttempt:4,transportAttempt:3,runFence:'other'}])
    assert.equal(await invoke(invalid),null);
});

test('runner-local typed classification overrides a cached old channel while cancellation remains authoritative',()=>{
  const select=new Function('WORKER_MODEL_TIMEOUT_MS',helpers+';return runtimeGenerationFailureCode;')(WORKER_MODEL_TIMEOUT_MS);
  const old={failureCode:()=> 'QUOTA_EXCEEDED'};
  assert.equal(select(old,failure('RATE_LIMIT')),'RATE_LIMIT');assert.equal(select(old,failure('QUOTA_EXCEEDED')),'QUOTA_EXCEEDED');
  for(const code of ['CANCELLED','MODEL_TIMEOUT','MODEL_CLIENT_DISCONNECTED'])
    assert.equal(select({failureCode:()=>code},failure('RATE_LIMIT')),code);
});
