import {acquireGlmRequestSlot,getGlmRequestGateStatus} from './glm-request-slot.mjs';
import {DEFAULT_GLM_GATE_SOCKET} from './glm-request-gate.mjs';

function requireQuiet(status,owned){
  if(status?.limit!==6||status.active!==owned||status.queued!==0||status.legacyReserved!==0||
      status.effectiveActive!==owned||status.reservationState!=='confirmed'||status.closed!==false)
    throw Object.assign(Error('MODEL_POLICY_GATE_BUSY'),{code:'MODEL_POLICY_GATE_BUSY'});
}
/** An operator-only installation barrier. It never invokes a provider. */
export async function withModelPolicyGateBarrier(token,operation,{socketPath=DEFAULT_GLM_GATE_SOCKET}={}){
  requireQuiet(await getGlmRequestGateStatus({socketPath}),0);
  const permits=[],lost=new AbortController();
  try{
    for(let index=0;index<6;index++){
      const queued=new AbortController(),timer=setTimeout(()=>queued.abort(),2000);
      try{permits.push(await acquireGlmRequestSlot({socketPath,runId:`policy-install-${token}-${index}`,
        signal:AbortSignal.any([lost.signal,queued.signal]),onLost:error=>lost.abort(error)}));}
      finally{clearTimeout(timer);}
    }
    const assertQuiet=async()=>{
      lost.signal.throwIfAborted();requireQuiet(await getGlmRequestGateStatus({socketPath}),6);lost.signal.throwIfAborted();
    };
    await assertQuiet();const value=await operation(assertQuiet);lost.signal.throwIfAborted();return value;
  }finally{for(const permit of permits)permit.release();}
}
