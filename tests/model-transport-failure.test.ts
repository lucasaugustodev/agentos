import {it,expect} from 'vitest';
// @ts-expect-error Runtime ESM has no generated declarations.
import {openAICompletionsFallback} from '../bridge/openai-completions-fallback.mjs';
// @ts-expect-error Runtime ESM has no generated declarations.
import {modelTransportFailure} from '../bridge/model-transport-failure.mjs';
const invoke=(fetch:any)=>openAICompletionsFallback({fetch,now:()=>Date.now()})
  .streamSimple({id:'fixture',baseUrl:'https://provider.invalid/v1'},{messages:[]},{apiKey:'fixture'}).result();
it('classifies a 502 arriving after 251 seconds when the existing 30-second transport retry budget is exhausted',async()=>{
  let now=0,requests=0;
  const e=await openAICompletionsFallback({now:()=>now,fetch:async()=>{
    requests++;now=251485;return new Response('',{status:502});
  }}).streamSimple({id:'fixture',baseUrl:'https://provider.invalid/v1'},{messages:[]},{apiKey:'fixture'}).result().catch((e:any)=>e);
  expect(requests).toBe(1);expect(e.code).toBe('MODEL_REQUEST_FAILED');expect(e.status).toBe(502);
  expect(modelTransportFailure(e)).toBe('provider_unavailable');
});
it.each([401,402,403,429,501])('does not grant generation replay for HTTP %s',async status=>{
  let now=0;
  const e=await openAICompletionsFallback({now:()=>now,fetch:async()=>{
    now=251485;return new Response('',{status});
  }}).streamSimple({id:'fixture',baseUrl:'https://provider.invalid/v1'},{messages:[]},{apiKey:'fixture'}).result().catch((e:any)=>e);
  expect(modelTransportFailure(e)).toBeNull();
});
it.each(['seconds','http-date'])('preserves a late 503 Retry-After %s and refuses outer retry instead of shortening the wait',async format=>{
  const base=Date.parse('2026-10-31T14:00:00Z');let now=base,requests=0;
  const e=await openAICompletionsFallback({now:()=>now,fetch:async()=>{
    requests++;now=base+251485;
    return new Response('',{status:503,headers:{'retry-after':format==='seconds'?'600':new Date(now+600000).toUTCString()}});
  }}).streamSimple({id:'fixture',baseUrl:'https://provider.invalid/v1'},{messages:[]},{apiKey:'fixture'}).result().catch((e:any)=>e);
  expect(requests).toBe(1);expect(e.status).toBe(503);expect(e.retryAfterSeconds).toBe(600);
  expect(modelTransportFailure(e)).toBeNull();
});
it('preserves the no-replay barrier after a partial provider body even for an explicit connection reset',async()=>{
  let requests=0;
  const e=await invoke(async()=>{
    requests++;let step=0;
    return new Response(new ReadableStream({pull(c){
      if(step++===0)c.enqueue(new TextEncoder().encode('data: {"choices":[{"delta":{"content":"partial"}}]}\n\n'));
      else c.error(Object.assign(new TypeError('transport failure'),{cause:{code:'UND_ERR_SOCKET'}}));
    }}),{headers:{'content-type':'text/event-stream'}});
  }).catch((e:any)=>e);
  expect(requests).toBe(1);expect(e.code).toBe('MODEL_INCOMPLETE_RESPONSE');
  expect(modelTransportFailure(e)).toBeNull();
});
it.each(['data: malformed\n\n','data: {"error":{"code":"ECONNRESET","message":"SSE stream ended without [DONE]"}}\n\n',
  'data: {"choices":[{"delta":{"content":"partial"}}]}\n\n'])
  ('does not infer transport retry authority from invalid protocol or provider text: %s',async body=>{
    const e=await invoke(async()=>new Response(body,{headers:{'content-type':'text/event-stream'}})).catch((e:any)=>e);
    expect(e.code).toBe('MODEL_INCOMPLETE_RESPONSE');expect(modelTransportFailure(e)).toBeNull();
  });
