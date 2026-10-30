// Counts are per cause, not per invocation. Transport outages must never
// consume the opportunity to repair the first semantic plan error.
export function planningRecovery(code:string, invalid:boolean, cancelled:boolean, previous:Record<string,number>={}) {
  const category = code === 'STOP_UNCONFIRMED' ? 'external' : cancelled ? 'interruption' : invalid ? 'invalid_plan' :
    ['PROVIDER_TIMEOUT','MODEL_TIMEOUT','MODEL_IDLE_TIMEOUT','MODEL_REQUEST_FAILED','MODEL_INCOMPLETE_RESPONSE',
      'MODEL_OUTPUT_LIMIT','MISSING_MODEL_RESPONSE','RATE_LIMIT','SOURCE_BUSY','HOST_CAPACITY',
      'BRIDGE_DISCONNECTED','ACCOUNT_UNAVAILABLE','GLM_GATE_UNAVAILABLE',
      'SOURCE_PREPARATION_RECONCILE','SOURCE_PREPARATION_STALE','SOURCE_PREPARATION_BRANCH_BUSY'].includes(code) ? 'transient' : 'external';
  const count = (Number.isSafeInteger(previous[category]) ? previous[category] : 0)+1;
  const retry = category === 'transient' || category === 'interruption' || (category === 'invalid_plan' && count<3);
  return {category,count,retry,counts:{...previous,[category]:count},
    delaySeconds: category === 'invalid_plan' ? 10 : Math.min(600,10*2**Math.min(count-1,6))};
}
