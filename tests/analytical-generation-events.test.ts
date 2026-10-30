import {afterEach,expect,it,vi} from 'vitest';
import {randomUUID} from 'node:crypto';
import {executeRuntime,type RuntimeInput} from '../server/runtime.js';
import {accountBridge} from '../server/bridge.js';
import {generationTiming} from '../server/generation-timing.js';
import * as manager from '../server/manager-turn-generation.js';

afterEach(()=>vi.restoreAllMocks());
function pending(){
  let resolve!:()=>void;
  const promise=new Promise<void>(done=>{resolve=done;});
  return {promise,resolve};
}
function input():RuntimeInput {
  const task={id:randomUUID(),goal_id:randomUUID(),key:'analytic-review',title:'Review available evidence',
    description:'Report only what the supplied evidence establishes.',stage:'work',agent_id:'research.review',
    model:'glm-5.3-flash',execution:null,criteria:[{id:'report',type:'artifact',name:'report.json'}]};
  return {task,run:{id:randomUUID(),task_id:task.id,fence:randomUUID(),runtime:'analytical'},
    account:{bridge_id:'agentos:api:00000000-0000-4000-8000-000000000001',provider:'opencode',model:'glm-5.3-flash'},
    context:{content:'Evidence only.',manifest:{selected:[],dependencyArtifacts:[],executionInputs:[]}},
    signal:new AbortController().signal,onEvent:async()=>{}};
}
const usage={input:10,output:3,totalTokens:13};
const delivery={summary:'Evidence reviewed.',artifacts:[{name:'report.json',content:'{"observed":true}',mime:'application/json'}]};
const result=()=>({message:{role:'assistant',content:[{type:'text',text:JSON.stringify(delivery)}],usage}});
function events(requestId:string){
  return [
    {kind:'MODEL_REQUEST_ADMITTED',payload:{requestId,gateElapsedMs:5}},
    {kind:'MODEL_WIRE_REQUESTED',payload:{requestId,version:1,attempt:1,reasoningEffort:'low',thinkingType:'enabled'}},
    {kind:'MODEL_TRANSPORT_FINISHED',payload:{requestId,version:1,attempt:1,outcome:'success',usage:{input:null,output:null}}},
    {kind:'MODEL_REQUEST_RELEASED',payload:{requestId}},
  ];
}

it('flushes analytical wire and gate events in emission order before delivering the original result',async()=>{
  const i=input(),requestId=randomUUID(),stream=events(requestId),entered=pending(),release=pending();
  const recorded:Array<{kind:string;payload:Record<string,unknown>}>=[];
  i.onEvent=async(kind,payload)=>{
    if(!recorded.length){entered.resolve();await release.promise;}
    recorded.push({kind,payload});
  };
  const request=vi.spyOn(accountBridge,'request').mockImplementation(async(_method,_params,options)=>{
    for(const entry of stream)options?.onEvent?.(entry.kind,entry.payload);
    return result();
  });
  let settled=false;
  const running=executeRuntime(i).then(value=>{settled=true;return value;});
  await entered.promise;
  expect(settled).toBe(false);expect(recorded).toEqual([]);
  release.resolve();
  expect(await running).toEqual({...delivery,usage:{...usage,costSource:'catalog_estimate'}});
  expect(recorded).toEqual(stream);
  expect(recorded.every(entry=>entry.payload.requestId===requestId)).toBe(true);
  expect(request).toHaveBeenCalledTimes(1);
  expect(request.mock.calls[0][0]).toBe('generate');
  expect(request.mock.calls[0][2]).toMatchObject({signal:i.signal,timeout:generationTiming(i.account.bridge_id,i.task.model).timeout});
  expect(request.mock.calls[0][2]?.idleTimeout).toBeUndefined();
  // The event adapter forwards the bridge whitelist only; it adds no prompt,
  // account, generated content, URL, tool arguments or result to the payload.
  expect(JSON.stringify(recorded)).not.toContain(i.task.description);
  expect(JSON.stringify(recorded)).not.toContain(i.account.bridge_id);
  expect(JSON.stringify(recorded)).not.toContain(delivery.summary);
});

it.each(['PROVIDER_TIMEOUT','CANCELLED','STOP_UNCONFIRMED'])(
  'flushes final analytical observations without masking or retrying %s',async(code)=>{
    const i=input(),entered=pending(),release=pending(),recorded:string[]=[];
    const error=Object.assign(new Error('Original provider outcome'),{code});
    i.onEvent=async kind=>{entered.resolve();await release.promise;recorded.push(kind);};
    const request=vi.spyOn(accountBridge,'request').mockImplementation(async(_method,_params,options)=>{
      options?.onEvent?.('MODEL_TRANSPORT_FINISHED',{requestId:randomUUID(),outcome:'error',code});
      throw error;
    });
    let settled=false;
    const running=executeRuntime(i).then(()=>{settled=true;return null;},reason=>{settled=true;return reason;});
    await entered.promise;expect(settled).toBe(false);
    release.resolve();expect(await running).toBe(error);
    expect(recorded).toEqual(['MODEL_TRANSPORT_FINISHED']);expect(request).toHaveBeenCalledTimes(1);
  },
);

it('keeps later analytical observations after a rejected or synchronously throwing observer without changing generation',async()=>{
  const i=input(),stream=events(randomUUID()),recorded:string[]=[];
  i.onEvent=vi.fn((kind:string)=>{
    recorded.push(kind);
    if(kind==='MODEL_REQUEST_ADMITTED')throw Error('Observer synchronous failure');
    if(kind==='MODEL_WIRE_REQUESTED')return Promise.reject(Error('Observer asynchronous failure'));
    return Promise.resolve();
  });
  const request=vi.spyOn(accountBridge,'request').mockImplementation(async(_method,_params,options)=>{
    for(const entry of stream)options?.onEvent?.(entry.kind,entry.payload);
    return result();
  });
  expect(await executeRuntime(i)).toMatchObject(delivery);
  expect(recorded).toEqual(stream.map(entry=>entry.kind));expect(request).toHaveBeenCalledTimes(1);
});

it('retains separate manager notifications across two generations and never duplicates wire events',async()=>{
  const i=input(),caseId=randomUUID(),sourceTaskId=randomUUID(),ids=[randomUUID(),randomUUID()];
  i.task.stage='coordinate';i.task.criteria=[{id:'review',type:'artifact',name:'coordination.json'}];
  i.task.description='MANAGEMENT_V1\nFOCUSED_DECISION_V2\n'+JSON.stringify({cases:[{id:caseId,taskId:sourceTaskId}],
    operation:{focus:{caseId,taskId:sourceTaskId,scope:'review/source'}}});
  expect(manager.managerReadOwner(i.run,i.task,i.account)).not.toBeNull();
  const read=vi.spyOn(manager,'readManagerTurnContext').mockResolvedValue({results:[],dataOnly:true,actionAuthority:false} as any);
  const recorded:Array<{kind:string;payload:Record<string,unknown>}>=[];
  i.onEvent=async(kind,payload)=>{await Promise.resolve();recorded.push({kind,payload});};
  let calls=0;
  const request=vi.spyOn(accountBridge,'request').mockImplementation(async(_method,_params,options)=>{
    const round=calls++;
    for(const entry of events(ids[round]))options?.onEvent?.(entry.kind,entry.payload);
    if(round===0)return {message:{role:'assistant',content:[{type:'toolCall',id:randomUUID(),name:'read_manager_context',
      arguments:{requests:[{kind:'task_metadata',taskId:sourceTaskId,expectedAttempt:1,part:'criteria',offset:0}]}}],usage}};
    return result();
  });
  const delivered=await executeRuntime(i);
  expect(delivered.summary).toBe(delivery.summary);
  expect(delivered.usage).toMatchObject({generationCalls:2,input:20,output:6,totalTokens:26});
  expect(request).toHaveBeenCalledTimes(2);expect(read).toHaveBeenCalledTimes(1);
  expect(recorded.map(entry=>entry.kind)).toEqual([
    ...events(ids[0]).map(entry=>entry.kind),'MANAGER_GENERATION_COMPLETED','MANAGER_CONTEXT_READ',
    ...events(ids[1]).map(entry=>entry.kind),'MANAGER_GENERATION_COMPLETED',
  ]);
  expect(recorded.filter(entry=>entry.kind.startsWith('MODEL_'))).toEqual([...events(ids[0]),...events(ids[1])]);
  expect(recorded.filter(entry=>entry.kind.startsWith('MANAGER_')).every(entry=>entry.payload.actionApplied===false)).toBe(true);
  expect((request.mock.calls[1][1] as any).messages.some((entry:any)=>entry.role==='toolResult')).toBe(true);
});

it('does not turn analytical telemetry into delivery acceptance or a repair of malformed model output',async()=>{
  const i=input(),recorded:string[]=[];
  i.onEvent=async kind=>{recorded.push(kind);};
  const request=vi.spyOn(accountBridge,'request').mockImplementation(async(_method,_params,options)=>{
    for(const entry of events(randomUUID()))options?.onEvent?.(entry.kind,entry.payload);
    return {message:{content:[{type:'text',text:'not the delivery contract'}],usage}};
  });
  await expect(executeRuntime(i)).rejects.toMatchObject({code:'RESULT_FORMAT'});
  expect(recorded).toEqual(events('irrelevant').map(entry=>entry.kind));
  expect(request).toHaveBeenCalledTimes(1);
});
