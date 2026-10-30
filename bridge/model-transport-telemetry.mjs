// Observations of the serialized request and transport only. Never content,
// credentials, URLs, response headers or inferred provider compute timings.
export const MODEL_TELEMETRY_VERSION = 1;
export const MAX_MODEL_TELEMETRY_BYTES = 4096;
const number = value => Number.isSafeInteger(value) && value >= 0 ? value : null;
const token = value => typeof value === 'string' && /^[a-zA-Z0-9][a-zA-Z0-9_.:-]{0,127}$/.test(value) ? value : null;
const code = value => typeof value === 'string' && /^[A-Z_]{3,60}$/.test(value) ? value : null;
const noop = () => {};
const empty = {headers:noop,chunk:noop,semantic:noop,usage:noop,bodyEnd:noop,finish:noop};

export function createModelTransportTelemetry(serialized, observe,
  monotonicNow = () => performance.now()) {
  if (typeof observe !== 'function') return {attempt:()=>empty};
  const emit = (kind,payload) => {
    try {
      // A future added field cannot silently turn events into prompt storage.
      if (Buffer.byteLength(JSON.stringify(payload),'utf8') > MAX_MODEL_TELEMETRY_BYTES) return;
      Promise.resolve(observe(kind,payload)).catch(noop);
    } catch { /* Telemetry never affects response, retry or permit ownership. */ }
  };
  let wire;
  try {
    // Derive the whitelist from the actual serialized bytes, not DSH headers
    // or defaults that another adapter may have overridden.
    const body=JSON.parse(serialized);
    wire={model:token(body.model),stream:typeof body.stream==='boolean'?body.stream:null,
      maxTokens:number(body.max_tokens),reasoningEffort:['low','high','max','medium','minimal','off'].includes(body.reasoning_effort)?body.reasoning_effort:null,
      thinkingType:['enabled','disabled','adaptive'].includes(body.thinking?.type)?body.thinking.type:null,
      clearThinking:typeof body.thinking?.clear_thinking==='boolean'?body.thinking.clear_thinking:null,
      requestBytes:Buffer.byteLength(serialized,'utf8'),
      messageCount:Array.isArray(body.messages)?body.messages.length:null,
      toolCount:Array.isArray(body.tools)?body.tools.length:0};
  } catch { return {attempt:()=>empty}; }
  return {attempt(attempt) {
    if (!Number.isSafeInteger(attempt) || attempt<1 || attempt>3) return empty;
    let start,previous=0;
    try {start=monotonicNow();} catch {start=null;}
    if (!Number.isFinite(start)) start=null;
    const elapsed=()=>{
      try {
        const value=monotonicNow();
        if (start===null || !Number.isFinite(value)) return null;
        previous=Math.max(previous,value-start);
        return Math.round(previous*1000)/1000;
      } catch {return null;}
    };
    const identity={version:MODEL_TELEMETRY_VERSION,adapter:'openai-completions-fallback-v1',attempt};
    const times={fetchStartMs:start===null?null:0,headersMs:null,firstBodyMs:null,
      firstSemanticMs:null,lastSemanticMs:null,bodyEndMs:null,finishedMs:null};
    const counts={bodyBytes:0,chunks:0,characters:0,reasoningCharacters:0,toolCharacters:0,semanticCharacters:0};
    let reported=null,status=null,format=null,finished=false,bodySeen=false,semanticSeen=false;
    emit('MODEL_WIRE_REQUESTED',{...identity,wire:{...wire},timingOrigin:'fetch_start'});
    // All methods are observer-only and swallow clock/callback failures.
    const guard=operation=>(...args)=>{try{if(!finished)operation(...args);}catch{}};
    return {
      headers:guard((value,bodyFormat)=>{times.headersMs=elapsed();status=Number.isInteger(value)&&value>=100&&value<=599?value:null;format=['sse','json'].includes(bodyFormat)?bodyFormat:null;}),
      chunk:guard(bytes=>{
        if(number(bytes)===null)return;
        counts.chunks++;counts.bodyBytes+=bytes;
        if(bytes&&!bodySeen){bodySeen=true;times.firstBodyMs=elapsed();}
      }),
      semantic:guard(value=>{
        const keys=['characters','reasoningCharacters','toolCharacters','semanticCharacters'];
        if(!keys.every(key=>number(value?.[key])!==null&&value[key]>=counts[key]) || value.semanticCharacters<=counts.semanticCharacters)return;
        for(const key of keys)counts[key]=value[key];
        const time=elapsed();if(!semanticSeen){semanticSeen=true;times.firstSemanticMs=time;}times.lastSemanticMs=time;
      }),
      usage:guard(value=>{
        if(!value || typeof value!=='object')return;
        reported={inputTokens:number(value.prompt_tokens),outputTokens:number(value.completion_tokens),
          totalTokens:number(value.total_tokens),cachedInputTokens:number(value.prompt_tokens_details?.cached_tokens),
          reasoningTokens:number(value.completion_tokens_details?.reasoning_tokens)};
      }),
      bodyEnd:guard(()=>{times.bodyEndMs=elapsed();}),
      finish:guard((outcome,errorCode)=>{
        if(!['success','error','cancelled','timeout','retry'].includes(outcome))return;
        times.finishedMs=elapsed();finished=true;
        emit('MODEL_TRANSPORT_FINISHED',{...identity,outcome,code:code(errorCode),httpStatus:status,bodyFormat:format,
          timingOrigin:'fetch_start',timings:{...times},counters:{...counts},usage:reported});
      }),
    };
  }};
}
