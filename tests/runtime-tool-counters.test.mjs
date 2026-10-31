import test from 'node:test';
import assert from 'node:assert/strict';
import {registerHooks} from 'node:module';
import {EventEmitter} from 'node:events';
import {PassThrough, Writable} from 'node:stream';
import {createInterface} from 'node:readline';
import {createHash, randomUUID} from 'node:crypto';
import {fileURLToPath} from 'node:url';
import path from 'node:path';
import * as termination from '../bridge/harness-termination.mjs';
import * as failures from '../bridge/runtime-tool-failure.mjs';
import * as transportFailures from '../bridge/model-transport-failure.mjs';
import * as modelChannel from '../bridge/runtime-model-channel.mjs';
import * as modelGateway from '../bridge/runtime-gateway.mjs';
import {openAICompletionsFallback} from '../bridge/openai-completions-fallback.mjs';

const runnerUrl=new URL('../bridge/runtime-runner.mjs',import.meta.url);
const forbidden=()=>{throw Error('UNEXPECTED_EXTERNAL_OPERATION');};
const call=id=>({type:'tool/call',data:{callId:id,name:'bash',arguments:'{"command":"fixture-only"}'}});
const result=id=>({type:'tool/result',data:{message:{source:{kind:'tool',callId:id},
  content:[{type:'tool-result',toolCallId:id,isError:false,content:[{type:'text',text:'fixture result'}]}]}}});
const counters=value=>({toolCalls:value.toolCalls,toolResults:value.toolResults,toolEvents:value.toolEvents});

// Run the actual runner and its readline/session handler. Only system, storage,
// model and collection boundaries are replaced; no tool-counting logic is copied.
async function runFixture(events,{reason={kind:'completed'},noise=[],generationFailures,unmarked=false,cancelBackoff=false,adapterFailure=false,lateHeaders=false,partialBody=false,providerWait,initialGuidance=false,compositionFixture=false,badPromptReceipt=false}={}){
  const runId=randomUUID(),receiptId=randomUUID(),emitted=[],saved=[],writes=new Map();
  const controller=new AbortController();
  let gatewayHandler,generations=0,gateActive=0,gateAdmissions=0,fetches=0;
  const wire=[];
  const providerBodies=[];
  const initialPrompts=[];
  const initialPrompt=initialGuidance?'Operator retry guidance: restore original tests; keep database coverage pending for integration CI.':'Counter fixture without a provider call.';
  const retryGuidance=initialGuidance?{bindingId:randomUUID(),requestId:randomUUID(),
    instructionDigest:createHash('sha256').update(initialPrompt).digest('hex'),promptDigest:createHash('sha256').update(initialPrompt).digest('hex')}:undefined;
  const child=new EventEmitter();child.stdout=new PassThrough();child.stderr=new PassThrough();
  const frame=value=>child.stdout.write(JSON.stringify(value)+'\n');
  const event=value=>frame({method:'session.event',params:{sessionId:runId,event:value}});
  child.stdin=new Writable({
    write(chunk,_encoding,done){
      const message=JSON.parse(chunk.toString());
      queueMicrotask(()=>{
        if(message.method==='initialize'){
          frame({id:message.id,result:{serverInfo:{name:'deepseek-harness-sdk-runtime'}}});return;
        }
        assert.equal(message.method,'session/prompt');
        initialPrompts.push(message.params.contentBlocks.map(b=>b.text).join('\n'));
        frame({id:message.id,result:badPromptReceipt==='null'?null:badPromptReceipt==='malformed'?{messageId:'-'.repeat(36)}:badPromptReceipt?{}:{messageId:receiptId}});
        frame({method:'session.status',params:{sessionId:runId,status:'running'}});
        event({type:'agent/inbox/spliced',data:{inserted:[{id:receiptId}]}});
        for(const value of noise){
          if(value==='malformed')child.stdout.write('not JSON\n');
          else if(value==='foreign')frame({method:'session.event',params:{sessionId:randomUUID(),event:call('foreign')}});
          else if(value==='rpc')frame({id:999,method:'session.event',params:{sessionId:runId,event:call('rpc')}});
        }
        for(const value of events)event(value);
        const finish=()=>{
          event({type:'assistant/message',data:{message:{content:[{type:'text',text:JSON.stringify({summary:'Fixture result',artifacts:[]})}]}}});
          event({type:'turn/end',data:{reason}});
          frame({method:'session.status',params:{sessionId:runId,status:'idle'}});
        };
        if(generationFailures!==undefined){
          const req=Object.assign((async function*(){yield JSON.stringify({messages:[{role:'user',content:'fixture'}]});})(),
            {method:'POST',url:'/v1/chat/completions'});
          const res=Object.assign(new EventEmitter(),{headersSent:false,writableEnded:false,destroyed:false,
            writeHead(){this.headersSent=true;},flushHeaders(){},write(s){wire.push(s);},
            end(s){wire.push(s);this.writableEnded=true;}});
          void gatewayHandler(req,res).then(()=>{if(wire.some(s=>s.includes('[DONE]')))finish();});
        } else finish();
      });
      done();
    },
    final(done){queueMicrotask(()=>child.emit('close',0));done();},
  });
  const io={
    mkdir:async()=>{},realpath:async value=>value,stat:compositionFixture?async()=>({isDirectory:()=>false}):forbidden,readdir:forbidden,
    readFile:async value=>{
      if(value==='/proc/meminfo')return 'MemAvailable: 104857600 kB\n';
      if(value instanceof URL&&value.pathname.endsWith('/artifact-collector.mjs'))return Buffer.from('fixture collector identity');
      throw Error('UNEXPECTED_READ');
    },
    writeFile:async(file,value)=>{writes.set(file,String(value));if(file.endsWith('/state.json'))saved.push(JSON.parse(value));},
  };
  const gateway=Object.assign(new EventEmitter(),{listen(_socket,ready){ready();},closeAllConnections(){},close(){}});
  const modules={
    './model-policy.mjs':{assertModelRoute(){},usesSharedRequestGate:()=>generationFailures!==undefined},
    './operator-model-profile.mjs':{workerModelSettings:()=>({reasoning:'high'})},
    './runtime-guidance.mjs':{createRuntimeGuidanceServer:forbidden},
    './sandbox-localhost.mjs':{prepareSandboxLocalhost:async()=>({mount:[]})},
    './repair-finalizer.mjs':{prepareRepairFinalization:async()=>null,finalizeRepairArtifacts:async()=>{},repairFinalizationInstruction:()=>''},
    'node:child_process':{spawn:()=>child},'node:http':{createServer:handler=>{gatewayHandler=handler;return gateway;}},
    'node:readline':{createInterface},'node:fs/promises':io,'node:crypto':{createHash,randomUUID},
    './gated-generation.mjs':{GLM_REQUEST_ADMITTED:Symbol(),withAccountRequestGate:async(input,signal,operation)=>{
      assert.equal(gateActive,0);signal.throwIfAborted();gateAdmissions++;gateActive++;
      try{return await operation(signal);}finally{gateActive--;}
    }},
    'node:path':{default:path},'node:url':{fileURLToPath},
    'node:timers/promises':{setTimeout:async(ms,_value,options)=>{
      if(ms===750||ms===1500&&options){if(cancelBackoff)controller.abort();options?.signal.throwIfAborted();return;}
      return new Promise(()=>{});
    }},
    './runtime-gateway.mjs':{toGeneration:()=>({accountId:'fixture',model:'fixture',messages:[]}),completion:modelGateway.completion},
    './runtime-model-channel.mjs':modelChannel,
    './workspaces.mjs':{workspaceOperation:compositionFixture?async request=>request.action==='compose'
      ?{complete:true,receipts:[{status:'applied'}]}:{baseDigest:'a'.repeat(64),candidateDigest:'b'.repeat(64),patch:'fixture patch'}:forbidden},
    './source-inputs.mjs':{materializeSourceInputs:()=>({inputs:[],telemetry:{materialized:false},instruction:''})},
    './artifact-input-bundle.mjs':{hydrateArtifactInputBundle:async value=>value,referencedInputLimitBytes:1024},
    './composition-inputs.mjs':{bindCompositionInputs:compositionFixture?value=>value:forbidden},
    './runtime-artifacts.mjs':{preserveRuntimeArtifactClaims:compositionFixture?()=>[]:forbidden,runtimeArtifactInstruction:''},
    './task-delivery-contract.mjs':{museTaskDeliveryInstruction:()=>''},
    './delivery-manifest.mjs':{deliveryContract:()=>({requiredArtifacts:[]}),extractPromptJsonFields:()=>[],
      prepareDeliveryManifest:async()=>{},collectDeliveryManifest:async()=>null,deliveryManifestInstructionFor:()=>''},
    './runtime-progress.mjs':{analyzePollTrace:()=>false},
    './inspect-runtime-progress.mjs':{inspectRuntimeProgress:forbidden,checkpointRuntimeProgress:forbidden},
    './runtime-handoff.mjs':{captureRuntimeHandoff:compositionFixture?async()=>null:forbidden,runtimeHandoffInstruction:''},
    './runtime-memory.mjs':{harnessMemory:()=>({memoryMiB:1024,minAvailableKiB:1,memoryMax:'MemoryMax=1G',slotMemory:'1024'})},
    './process-stop.mjs':{stopUnit:async(_unit,ended)=>{assert.equal(ended(),true);return {stopped:true};}},
    './unit-exit.mjs':{inspectUnitExit:async()=>null},
    './harness-termination.mjs':termination,'./runtime-tool-failure.mjs':failures,
    './model-transport-failure.mjs':transportFailures,
  };
  // Node's synchronous module hooks need no experimental VM flags. Hooks only
  // replace this fixture runner's imports, and are removed after each attempt.
  const fixtureId=randomUUID(),fixtureUrl=new URL(runnerUrl);
  fixtureUrl.searchParams.set('counter-fixture',fixtureId);
  const symbol=Symbol.for('agentos-counter-fixture:'+fixtureId),prefix='agentos-counter-fixture:'+fixtureId+'/';
  globalThis[symbol]=modules;
  const hooks=registerHooks({
    resolve(specifier,context,nextResolve){
      if(context.parentURL!==fixtureUrl.href)return nextResolve(specifier,context);
      if(/^\.\/artifact-collector\.mjs\?v=[a-f0-9]{64}$/.test(specifier))
        modules[specifier]={parseArtifactResult:JSON.parse,collectArtifacts:async value=>value.artifacts};
      assert.ok(modules[specifier],`Unexpected import ${specifier}`);
      return {url:prefix+encodeURIComponent(specifier),shortCircuit:true};
    },
    load(url,context,nextLoad){
      if(!url.startsWith(prefix))return nextLoad(url,context);
      const specifier=decodeURIComponent(url.slice(prefix.length)),values=modules[specifier];
      const get=name=>`globalThis[Symbol.for(${JSON.stringify(Symbol.keyFor(symbol))})][${JSON.stringify(specifier)}][${JSON.stringify(name)}]`;
      return {format:'module',shortCircuit:true,source:Object.keys(values).map(name=>name==='default'
        ?`export default ${get(name)};`:`export const ${name}=${get(name)};`).join('\n')};
    },
  });
  let value,error;
  try{
    const {runHarness}=await import(fixtureUrl.href);
    value=await runHarness({runId,workspace:'fixture/counters',accountId:'fixture',model:'fixture',
      ...(generationFailures!==undefined?{runFence:randomUUID(),contextVersion:randomUUID()}:{}),
      ...(compositionFixture?{managed:true,composition:{version:1},inputs:[]}:{}),
      prompt:initialPrompt,...(retryGuidance?{retryGuidance}:{}),context:{}},controller.signal,async input=>{
        if(initialGuidance)assert.ok(initialPrompts[0].includes(initialPrompt),'Guidance must be delivered in the first SDK prompt before generation');
        generations++;assert.equal(gateActive,1);
        assert.equal(input.generationAttempt,generations);
        assert.ok(wire.every(s=>s.startsWith(':')),'Only transport comments may precede a validated completion');
        if(adapterFailure){
          let clock=0;
          return openAICompletionsFallback({now:()=>clock,fetch:async(_url,request)=>{
            providerBodies.push(JSON.parse(request.body));
            fetches++;if(lateHeaders)clock=251485;
            if(partialBody){
              let part=0;
              return new Response(new ReadableStream({pull(c){
                if(part++===0)c.enqueue(new TextEncoder().encode('data: {"choices":[{"delta":{"content":"partial"}}]}\n\n'));
                else c.error(Object.assign(new TypeError('network reset'),{cause:{code:'UND_ERR_SOCKET'}}));
              }}),{headers:{'content-type':'text/event-stream'}});
            }
            if(adapterFailure==='once'&&generations>1)return new Response(JSON.stringify({choices:[{message:{content:'complete'},finish_reason:'stop'}]}),
              {headers:{'content-type':'application/json'}});
            return new Response('',{status:providerWait?503:502,headers:{'retry-after':providerWait==='date'
              ?new Date(clock+600000).toUTCString():providerWait??'0'}});
          }}).streamSimple({id:'fixture',baseUrl:'https://provider.invalid/v1'},
            {messages:[]},{apiKey:'fixture',onTelemetry:transportFailures.generationObserver(
              (kind,payload)=>emitted.push({kind,payload}),input)}).result().then(message=>({message}));
        }
        if(generations<=generationFailures){
          const e=Object.assign(Error('MODEL_INCOMPLETE_RESPONSE'),{code:'MODEL_INCOMPLETE_RESPONSE'});
          throw unmarked?e:transportFailures.markModelTransportFailure(e,'connection_interrupted');
        }
        return {message:{content:[{type:'text',text:'valid final response'}],stopReason:'stop'}};
      },
      (kind,payload)=>emitted.push({kind,payload}));
  }
  catch(e){error=e;}
  finally{hooks.deregister();delete globalThis[symbol];child.stdout.end();child.stderr.end();}
  assert.deepEqual(saved.slice(0,2).map(s=>s.state),['starting','running']);
  assert.equal(saved.at(-1).stopped,true,error?.stack);
  assert.equal(emitted.filter(e=>e.kind==='HARNESS_STOPPED').length,1);
  assert.equal(gateActive,0);
  return {value,error,saved,emitted,writes,wire,generations,gateAdmissions,fetches,providerBodies,initialPrompts,retryGuidance};
}

test('delivers bound retry guidance in the initial SDK prompt before generation without claiming compliance',async()=>{
  const f=await runFixture([],{initialGuidance:true,generationFailures:0});
  assert.equal(f.error,undefined);assert.equal(f.generations,1);assert.equal(f.initialPrompts.length,1);
  const delivered=f.emitted.filter(e=>e.kind==='TASK_RETRY_GUIDANCE_PROMPT_DELIVERED');
  assert.equal(delivered.length,1);assert.equal(delivered[0].payload.bindingId,f.retryGuidance.bindingId);
  assert.match(delivered[0].payload.boundary,/not evidence of comprehension, compliance or acceptance/);
  assert.equal(f.emitted.some(e=>e.kind==='TASK_RETRY_GUIDANCE_CONSUMED'),false);
  assert.equal(delivered[0].payload.deliveredPromptDigest,createHash('sha256').update(f.initialPrompts[0]).digest('hex'));
});

test('preserves guidance while legitimate managed composition enriches the initial SDK prompt',async()=>{
  const f=await runFixture([],{initialGuidance:true,compositionFixture:true,generationFailures:0});
  assert.equal(f.error,undefined);assert.equal(f.generations,1);
  assert.match(f.initialPrompts[0],/Operator retry guidance/);assert.match(f.initialPrompts[0],/RUNTIME PATCH COMPOSITION/);
  const delivered=f.emitted.find(e=>e.kind==='TASK_RETRY_GUIDANCE_PROMPT_DELIVERED');
  assert.ok(delivered);assert.equal(delivered.payload.deliveredPromptDigest,createHash('sha256').update(f.initialPrompts[0]).digest('hex'));
  assert.notEqual(delivered.payload.promptDigest,delivered.payload.deliveredPromptDigest);
});

test('does not claim initial guidance delivery when the SDK prompt receipt has no message identity',async()=>{
  for(const badPromptReceipt of [true,'null','malformed']){
    const f=await runFixture([],{initialGuidance:true,badPromptReceipt});
    assert.equal(f.error.code,'RETRY_GUIDANCE_PROMPT_UNCONFIRMED');
    assert.equal(f.emitted.some(e=>e.kind==='TASK_RETRY_GUIDANCE_PROMPT_DELIVERED'),false);
  }
});

test('51 completed tool calls remain 51 calls while the activity counter retains all 102 events',async()=>{
  const events=Array.from({length:51},(_,i)=>[call('call-'+i),result('call-'+i)]).flat();
  const f=await runFixture(events);assert.equal(f.error,undefined);
  assert.equal(f.saved.at(-1).state,'completed');
  assert.deepEqual(counters(f.saved.at(-1)),{toolCalls:51,toolResults:51,toolEvents:102});
  assert.deepEqual(counters(f.value.evidence),{toolCalls:51,toolResults:51,toolEvents:102});
  const activity=f.emitted.filter(e=>e.kind==='HARNESS_TOOL_EVENT');assert.equal(activity.length,102);
  assert.deepEqual(activity.map(e=>e.payload.toolEvents),Array.from({length:102},(_,i)=>i+1));
  assert.deepEqual(counters(activity.at(-1).payload),{toolCalls:51,toolResults:51,toolEvents:102});
});

test('parallel, orphan and progress events do not invent calls; foreign sessions and RPC frames are ignored',async()=>{
  const f=await runFixture([call('parallel-a'),call('parallel-b'),result('parallel-b'),result('orphan'),
    {type:'tool/progress',data:{message:'still working'}},{type:'assistant/message',data:{message:{content:[{type:'text',text:'tool/call'}]}}}],
    {noise:['malformed','foreign','rpc']});
  assert.equal(f.error,undefined);
  assert.deepEqual(counters(f.value.evidence),{toolCalls:2,toolResults:2,toolEvents:5});
  assert.deepEqual(f.emitted.filter(e=>e.kind==='HARNESS_TOOL_EVENT').map(e=>e.payload.type),
    ['tool/call','tool/call','tool/result','tool/result','tool/progress']);
});

test('a failed turn persists partial tool counters without treating missing results as completed calls',async()=>{
  const f=await runFixture([call('a'),call('b'),call('c'),result('b')],{reason:{kind:'max-tokens'}});
  assert.equal(f.value,undefined);assert.equal(f.error.code,'MODEL_OUTPUT_LIMIT');
  const state=f.saved.at(-1);assert.equal(state.state,'failed');assert.equal(state.code,'MODEL_OUTPUT_LIMIT');
  assert.deepEqual(counters(state),{toolCalls:3,toolResults:1,toolEvents:4});
  assert.deepEqual(counters(f.emitted.filter(e=>e.kind==='HARNESS_TOOL_EVENT').at(-1).payload),
    {toolCalls:3,toolResults:1,toolEvents:4});
});

test('retries only a buffered generation while retaining all earlier tools and releasing each gate',async()=>{
  const f=await runFixture([call('a'),result('a')],{generationFailures:1});
  assert.equal(f.error,undefined);assert.equal(f.generations,2);assert.equal(f.gateAdmissions,2);
  assert.equal(f.value.evidence.toolCalls,1);assert.equal(f.value.evidence.toolResults,1);
  assert.equal(f.emitted.filter(e=>e.kind==='MODEL_GENERATION_RETRY').length,1);
  assert.equal(f.emitted.filter(e=>e.kind==='RUNTIME_TRANSPORT_INTERRUPTED').length,0);
  assert.equal(f.wire.filter(s=>s.includes('[DONE]')).length,1);
});
test('exhausted generation retries preserve the ordinary terminal failure without delivering tool output',async()=>{
  const f=await runFixture([call('a'),result('a')],{generationFailures:99});
  assert.equal(f.error.code,'MODEL_INCOMPLETE_RESPONSE');assert.equal(f.generations,3);assert.equal(f.gateAdmissions,3);
  assert.equal(f.emitted.filter(e=>e.kind==='MODEL_ERROR').length,1);
  assert.equal(f.saved.at(-1).stopped,true);
  assert.equal(f.wire.some(s=>s.includes('[DONE]')),false);
});
test('neither a generic stream error nor an unsettled tool can authorize replay',async()=>{
  for(const options of [{unmarked:true},{}]){
    const f=await runFixture(options.unmarked?[]:[call('unfinished'),result('unrelated')],{generationFailures:99,...options});
    assert.equal(f.generations,1);assert.equal(f.error.code,'MODEL_INCOMPLETE_RESPONSE');
    assert.equal(f.emitted.filter(e=>e.kind==='RUNTIME_TRANSPORT_INTERRUPTED').length,0);
  }
});
test('caller cancellation during retry backoff prevents another admission and continuation proof',async()=>{
  const f=await runFixture([],{generationFailures:99,cancelBackoff:true});
  assert.equal(f.generations,1);assert.equal(f.gateAdmissions,1);assert.equal(f.error.code,'CANCELLED');
  assert.equal(f.emitted.filter(e=>e.kind==='RUNTIME_TRANSPORT_INTERRUPTED').length,0);
});
test('real adapter retries remain bounded at nine fast fetches or three late-502 fetches across three gated generations',async()=>{
  for(const lateHeaders of [false,true]){
    const f=await runFixture([],{generationFailures:99,adapterFailure:true,lateHeaders});
    assert.equal(f.error.code,'MODEL_REQUEST_FAILED');assert.equal(f.generations,3);assert.equal(f.gateAdmissions,3);
    assert.equal(f.fetches,lateHeaders?3:9);
    assert.equal(f.wire.some(s=>s.includes('[DONE]')),false);
  }
});
test('logical request plus generation and HTTP attempt preserve late failure then success without changing provider body',async()=>{
  const f=await runFixture([call('prior'),result('prior')],{generationFailures:1,adapterFailure:'once',lateHeaders:true});
  assert.equal(f.error,undefined);assert.equal(f.generations,2);assert.equal(f.fetches,2);
  const terminal=f.emitted.filter(e=>e.kind==='MODEL_TRANSPORT_FINISHED').map(e=>e.payload);
  assert.deepEqual(terminal.map(p=>[p.generationAttempt,p.attempt,p.outcome]),[[1,1,'error'],[2,1,'success']]);
  assert.equal(terminal[0].requestId,terminal[1].requestId);
  assert.deepEqual(f.providerBodies[0],f.providerBodies[1]);
  assert.equal(Object.hasOwn(f.providerBodies[0],'generationAttempt'),false);
  assert.equal(Object.hasOwn(f.providerBodies[0],'requestId'),false);
  assert.equal(f.value.evidence.toolCalls,1);assert.equal(f.gateAdmissions,2);
});
test('a partial provider body never authorizes outer generation retry even after a proven connection reset',async()=>{
  const f=await runFixture([],{generationFailures:99,adapterFailure:true,partialBody:true});
  assert.equal(f.error.code,'MODEL_INCOMPLETE_RESPONSE');assert.equal(f.generations,1);assert.equal(f.fetches,1);
  assert.equal(f.gateAdmissions,1);assert.equal(f.emitted.filter(e=>e.kind==='MODEL_GENERATION_RETRY').length,0);
  assert.equal(f.wire.some(s=>s.includes('[DONE]')),false);
});
test('the gateway never replaces a late 503 Retry-After with its shorter generation backoff',async()=>{
  for(const providerWait of ['600','date']){
    const f=await runFixture([],{generationFailures:99,adapterFailure:true,lateHeaders:true,providerWait});
    assert.equal(f.error.code,'MODEL_REQUEST_FAILED');assert.equal(f.generations,1);assert.equal(f.fetches,1);
    assert.equal(f.gateAdmissions,1);assert.equal(f.emitted.filter(e=>e.kind==='MODEL_GENERATION_RETRY').length,0);
  }
});
