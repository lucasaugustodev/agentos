import test from 'node:test';
import assert from 'node:assert/strict';
import {openAICompletionsFallback, readCompletionsStream} from '../bridge/openai-completions-fallback.mjs';
import {createModelTransportTelemetry, MAX_MODEL_TELEMETRY_BYTES} from '../bridge/model-transport-telemetry.mjs';
import {sanitizeProtocolTerminal} from '../bridge/protocol-terminal-diagnostic.mjs';

const canary='PRIVATE-CANARY-DO-NOT-PERSIST';
const model={id:'deepseek-v4-pro-0813',baseUrl:'https://provider.invalid/v1'};
const frame=value=>'data: '+JSON.stringify(value)+'\n\n';
const chunk=(delta,finish)=>frame({choices:[{delta,...(finish===undefined?{}:{finish_reason:finish})}]});
const tool=(overrides={})=>({index:0,id:canary+'-id',function:{name:canary+'-tool',arguments:JSON.stringify({key:canary})},...overrides});
const body=parts=>({async *[Symbol.asyncIterator](){for(const part of parts)yield Buffer.isBuffer(part)?part:Buffer.from(part);}});
async function run(parts,{format='sse',signal,observe}={}) {
  const events=[],retries=[];let calls=0;
  const provider=openAICompletionsFallback({fetch:async()=>{
    calls++;return {ok:true,status:200,headers:new Headers({'content-type':format==='sse'?'text/event-stream':'application/json','private':canary}),
      body:Array.isArray(parts)?body(parts):parts};
  }});
  let value,error;
  try {value=await provider.streamSimple(model,{systemPrompt:canary,messages:[]},{signal,apiKey:canary,
    onRetry:r=>retries.push(r),onTelemetry:(kind,payload)=>{events.push({kind,payload});return observe?.(kind,payload);}}).result();}
  catch(e){error=e;}
  const receipts=events.filter(e=>e.kind==='MODEL_TRANSPORT_FINISHED');
  assert.equal(calls,1,'observations never add a replay');assert.deepEqual(retries,[]);
  assert.equal(receipts.length,1);assert.equal(receipts[0].payload.version,1);
  assert.ok(events.every(e=>Buffer.byteLength(JSON.stringify(e.payload))<=MAX_MODEL_TELEMETRY_BYTES));
  assert.ok(!JSON.stringify(events).includes(canary),'only a closed content-free schema is emitted');
  return {value,error,events,receipt:receipts[0].payload,diagnostic:receipts[0].payload.protocolTerminal};
}
const expected=(overrides={})=>({version:1,format:'sse',eofObserved:true,doneObserved:false,
  finishReason:'stop',trailingBytes:0,toolCount:0,validation:'accepted',aborted:false,...overrides});

test('valid terminal responses still complete with or without DONE, including split UTF-8 tools',async()=>{
  for(const done of [false,true]){
    const text=chunk({content:canary,reasoning_content:canary},'stop')+(done?'data: [DONE]\n\n':'');
    const r=await run([text]);assert.equal(r.error,undefined);
    assert.equal(r.value.stopReason,'stop');assert.deepEqual(r.diagnostic,expected({doneObserved:done}));
  }
  const text=Buffer.from(chunk({tool_calls:[tool({function:{name:'read',arguments:JSON.stringify({path:canary+'-蛇'})}})]},'tool_calls'));
  const split=text.indexOf(Buffer.from('蛇'))+1;
  const r=await run([text.subarray(0,split),text.subarray(split)]);
  assert.equal(r.error,undefined);assert.equal(r.value.content[0].arguments.path,canary+'-蛇');
  assert.equal(r.value.stopReason,'toolUse');
  assert.deepEqual(r.diagnostic,expected({finishReason:'tool_calls',toolCount:1}));
});

const cases=[
  ['missing_finish',{},undefined,'MODEL_INCOMPLETE_RESPONSE','missing'],
  ['invalid_finish',{content:canary},canary,'MODEL_INCOMPLETE_RESPONSE','other'],
  ['finish_tool_mismatch',{content:canary},'tool_calls','MODEL_INCOMPLETE_RESPONSE','tool_calls'],
  ['finish_tool_mismatch',{tool_calls:[tool()]},'stop','MODEL_INCOMPLETE_RESPONSE','stop'],
  ['tool_id_missing',{tool_calls:[tool({id:undefined})]},'tool_calls','MODEL_INCOMPLETE_RESPONSE','tool_calls'],
  ['tool_id_duplicate',{tool_calls:[tool(),tool({index:1})]},'tool_calls','MODEL_INCOMPLETE_RESPONSE','tool_calls'],
  ['tool_name_missing',{tool_calls:[tool({function:{arguments:'{}'}})]},'tool_calls','MODEL_INCOMPLETE_RESPONSE','tool_calls'],
  ['invalid_tool_json',{tool_calls:[tool({function:{name:canary,arguments:'{"key":"'+canary}})]},'tool_calls','MODEL_INCOMPLETE_RESPONSE','tool_calls'],
  ['invalid_tool_shape',{tool_calls:[tool({function:{name:canary,arguments:'[]'}})]},'tool_calls','MODEL_INCOMPLETE_RESPONSE','tool_calls'],
  ['empty_message',{},'stop','MISSING_MODEL_RESPONSE','stop'],
  ['output_limit',{tool_calls:[tool({function:{arguments:'{'}})]},'length','MODEL_OUTPUT_LIMIT','length'],
];
for(const [reason,delta,finish,code,finishReason] of cases)test('strict terminal rejection: '+reason+' / '+finishReason,async()=>{
  const r=await run([chunk(delta,finish),'data: [DONE]\n\n']);
  assert.equal(r.value,undefined,'partial tools do not escape the adapter');assert.equal(r.error.code,code);
  assert.deepEqual(r.diagnostic,expected({validation:reason,finishReason,toolCount:delta.tool_calls?.length??0,doneObserved:true}));
});

test('a valid-looking frame without its newline remains rejected as an unterminated tail',async()=>{
  const tail=chunk({content:canary+'-蛇'},'stop').trimEnd();
  const r=await run([tail]);assert.equal(r.error.code,'MODEL_INCOMPLETE_RESPONSE');
  assert.deepEqual(r.diagnostic,expected({validation:'unterminated_tail',finishReason:'missing',trailingBytes:Buffer.byteLength(tail)}));
});

test('frame rejection and data after DONE cannot claim body EOF',async()=>{
  for(const [parts,validation,doneObserved] of [
    [['data: '+canary+'\n\n'],'malformed_frame',false],
    [[frame({error:{message:canary}})],'provider_error',false],
    [['data: [DONE]\n\n',chunk({content:canary},'stop')],'data_after_done',true],
  ]){
    const r=await run(parts);assert.equal(r.error.code,'MODEL_INCOMPLETE_RESPONSE');
    assert.deepEqual(r.diagnostic,expected({validation,doneObserved,eofObserved:false,finishReason:'missing',trailingBytes:1}));
  }
});

test('transport interruption after partial material records no EOF and never replays',async()=>{
  const partial={async *[Symbol.asyncIterator](){yield Buffer.from(chunk({tool_calls:[tool()]}));throw Error(canary);}};
  const r=await run(partial);assert.equal(r.error.code,'MODEL_INCOMPLETE_RESPONSE');
  assert.deepEqual(r.diagnostic,expected({validation:'interrupted',eofObserved:false,finishReason:'missing',toolCount:1}));
});

test('caller cancellation after a parsed terminal wins without claiming EOF or releasing partial tools',async()=>{
  const caller=new AbortController();
  const partial={async *[Symbol.asyncIterator](){yield Buffer.from(chunk({tool_calls:[tool()]},'tool_calls'));caller.abort();}};
  const r=await run(partial,{signal:caller.signal});assert.equal(r.error.code,'CANCELLED');
  assert.equal(r.value,undefined);assert.equal(r.receipt.outcome,'cancelled');
  assert.deepEqual(r.diagnostic,expected({validation:'interrupted',eofObserved:false,finishReason:'tool_calls',toolCount:1,aborted:true}));
});

test('JSON observations separate normal EOF, malformed JSON and the unchanged terminal validation',async()=>{
  const valid=JSON.stringify({choices:[{message:{content:canary},finish_reason:'stop'}]});
  const r=await run([valid],{format:'json'});assert.equal(r.value.content[0].text,canary);
  assert.deepEqual(r.diagnostic,expected({format:'json'}));
  const bad=await run(['{"private":"'+canary],{format:'json'});assert.equal(bad.error.code,'MODEL_INCOMPLETE_RESPONSE');
  assert.deepEqual(bad.diagnostic,expected({format:'json',validation:'malformed_json',finishReason:'missing'}));
  const invalid=await run([JSON.stringify({choices:[{message:{tool_calls:[tool()]},finish_reason:canary}]})],{format:'json'});
  assert.equal(invalid.error.code,'MODEL_INCOMPLETE_RESPONSE');
  assert.deepEqual(invalid.diagnostic,expected({format:'json',validation:'invalid_finish',finishReason:'other',toolCount:1}));
});

test('synchronous and asynchronous observers cannot change success or rejection',async()=>{
  for(const observe of [()=>{throw Error(canary);},()=>Promise.reject(Error(canary))]){
    const good=await run([chunk({content:'ok'},'stop')],{observe});assert.equal(good.value.content[0].text,'ok');
    const bad=await run([chunk({tool_calls:[tool()]})],{observe});assert.equal(bad.error.code,'MODEL_INCOMPLETE_RESPONSE');
    const telemetry={chunk(){},semantic(){},bodyEnd(){},protocolTerminal:observe};
    const direct=await readCompletionsStream(body([chunk({content:'ok'},'stop')]),undefined,undefined,undefined,undefined,undefined,telemetry);
    assert.equal(direct.content[0].text,'ok');
    await assert.rejects(readCompletionsStream(body([chunk({content:canary})]),undefined,undefined,undefined,undefined,undefined,telemetry),{code:'MODEL_INCOMPLETE_RESPONSE'});
  }
});

test('terminal whitelist rejects invalid enum/types and excludes arbitrary nested content',()=>{
  const safe=expected();
  for(const extra of [{validation:canary},{finishReason:canary},{format:canary},{version:2},
    {toolCount:NaN},{trailingBytes:Number.MAX_SAFE_INTEGER+1},{trailingBytes:-1},{aborted:canary}])
    assert.equal(sanitizeProtocolTerminal({...safe,...extra}),null);
  assert.deepEqual(sanitizeProtocolTerminal({...safe,args:{key:canary},raw:canary.repeat(10000)}),safe);
  const events=[],attempt=createModelTransportTelemetry('{}',(kind,payload)=>events.push({kind,payload})).attempt(1);
  attempt.protocolTerminal({...safe,secret:canary.repeat(10000)});attempt.finish('success');
  assert.deepEqual(events.at(-1).payload.protocolTerminal,safe);
  assert.ok(!JSON.stringify(events).includes(canary));
  assert.ok(Buffer.byteLength(JSON.stringify(events.at(-1).payload))<MAX_MODEL_TELEMETRY_BYTES);
});
