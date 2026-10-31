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

const runnerUrl=new URL('../bridge/runtime-runner.mjs',import.meta.url);
const forbidden=()=>{throw Error('UNEXPECTED_EXTERNAL_OPERATION');};
const call=id=>({type:'tool/call',data:{callId:id,name:'bash',arguments:'{"command":"fixture-only"}'}});
const result=id=>({type:'tool/result',data:{message:{source:{kind:'tool',callId:id},
  content:[{type:'tool-result',toolCallId:id,isError:false,content:[{type:'text',text:'fixture result'}]}]}}});
const counters=value=>({toolCalls:value.toolCalls,toolResults:value.toolResults,toolEvents:value.toolEvents});

// Run the actual runner and its readline/session handler. Only system, storage,
// model and collection boundaries are replaced; no tool-counting logic is copied.
async function runFixture(events,{reason={kind:'completed'},noise=[]}={}){
  const runId=randomUUID(),receiptId=randomUUID(),emitted=[],saved=[],writes=new Map();
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
        frame({id:message.id,result:{messageId:receiptId}});
        frame({method:'session.status',params:{sessionId:runId,status:'running'}});
        event({type:'agent/inbox/spliced',data:{inserted:[{id:receiptId}]}});
        for(const value of noise){
          if(value==='malformed')child.stdout.write('not JSON\n');
          else if(value==='foreign')frame({method:'session.event',params:{sessionId:randomUUID(),event:call('foreign')}});
          else if(value==='rpc')frame({id:999,method:'session.event',params:{sessionId:runId,event:call('rpc')}});
        }
        for(const value of events)event(value);
        event({type:'assistant/message',data:{message:{content:[{type:'text',text:JSON.stringify({summary:'Fixture result',artifacts:[]})}]}}});
        event({type:'turn/end',data:{reason}});
        frame({method:'session.status',params:{sessionId:runId,status:'idle'}});
      });
      done();
    },
    final(done){queueMicrotask(()=>child.emit('close',0));done();},
  });
  const io={
    mkdir:async()=>{},realpath:async value=>value,stat:forbidden,readdir:forbidden,
    readFile:async value=>{
      if(value==='/proc/meminfo')return 'MemAvailable: 104857600 kB\n';
      if(value instanceof URL&&value.pathname.endsWith('/artifact-collector.mjs'))return Buffer.from('fixture collector identity');
      throw Error('UNEXPECTED_READ');
    },
    writeFile:async(file,value)=>{writes.set(file,String(value));if(file.endsWith('/state.json'))saved.push(JSON.parse(value));},
  };
  const gateway=Object.assign(new EventEmitter(),{listen(_socket,ready){ready();},closeAllConnections(){},close(){}});
  const modules={
    './model-policy.mjs':{assertModelRoute(){},usesSharedRequestGate:()=>false},
    './operator-model-profile.mjs':{workerModelSettings:forbidden},
    './runtime-guidance.mjs':{createRuntimeGuidanceServer:forbidden},
    './sandbox-localhost.mjs':{prepareSandboxLocalhost:async()=>({mount:[]})},
    './repair-finalizer.mjs':{prepareRepairFinalization:async()=>null,finalizeRepairArtifacts:async()=>{},repairFinalizationInstruction:()=>''},
    'node:child_process':{spawn:()=>child},'node:http':{createServer:()=>gateway},
    'node:readline':{createInterface},'node:fs/promises':io,'node:crypto':{createHash,randomUUID},
    './gated-generation.mjs':{GLM_REQUEST_ADMITTED:Symbol(),withAccountRequestGate:forbidden},
    'node:path':{default:path},'node:url':{fileURLToPath},
    'node:timers/promises':{setTimeout:()=>new Promise(()=>{})},
    './runtime-gateway.mjs':{toGeneration:forbidden,completion:forbidden},
    './runtime-model-channel.mjs':{bufferedModelChannel:forbidden,WORKER_MODEL_TIMEOUT_MS:60000},
    './workspaces.mjs':{workspaceOperation:forbidden},
    './source-inputs.mjs':{materializeSourceInputs:()=>({inputs:[],telemetry:{materialized:false},instruction:''})},
    './artifact-input-bundle.mjs':{hydrateArtifactInputBundle:async value=>value,referencedInputLimitBytes:1024},
    './composition-inputs.mjs':{bindCompositionInputs:forbidden},
    './runtime-artifacts.mjs':{preserveRuntimeArtifactClaims:forbidden,runtimeArtifactInstruction:''},
    './task-delivery-contract.mjs':{museTaskDeliveryInstruction:()=>''},
    './delivery-manifest.mjs':{deliveryContract:()=>({requiredArtifacts:[]}),extractPromptJsonFields:()=>[],
      prepareDeliveryManifest:async()=>{},collectDeliveryManifest:async()=>null,deliveryManifestInstructionFor:()=>''},
    './runtime-progress.mjs':{analyzePollTrace:()=>false},
    './inspect-runtime-progress.mjs':{inspectRuntimeProgress:forbidden,checkpointRuntimeProgress:forbidden},
    './runtime-handoff.mjs':{captureRuntimeHandoff:forbidden,runtimeHandoffInstruction:''},
    './runtime-memory.mjs':{harnessMemory:()=>({memoryMiB:1024,minAvailableKiB:1,memoryMax:'MemoryMax=1G',slotMemory:'1024'})},
    './process-stop.mjs':{stopUnit:async(_unit,ended)=>{assert.equal(ended(),true);return {stopped:true};}},
    './unit-exit.mjs':{inspectUnitExit:async()=>null},
    './harness-termination.mjs':termination,'./runtime-tool-failure.mjs':failures,
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
      prompt:'Counter fixture without a provider call.',context:{}},new AbortController().signal,forbidden,
      (kind,payload)=>emitted.push({kind,payload}));
  }
  catch(e){error=e;}
  finally{hooks.deregister();delete globalThis[symbol];child.stdout.end();child.stderr.end();}
  assert.deepEqual(saved.slice(0,2).map(s=>s.state),['starting','running']);
  assert.equal(saved.at(-1).stopped,true);
  assert.equal(emitted.filter(e=>e.kind==='HARNESS_STOPPED').length,1);
  return {value,error,saved,emitted,writes};
}

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
