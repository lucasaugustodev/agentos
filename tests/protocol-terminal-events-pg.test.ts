import {afterAll,afterEach,beforeAll,beforeEach,expect,it,vi} from 'vitest';
import {randomUUID} from 'node:crypto';
import {PassThrough} from 'node:stream';
import {EventEmitter} from 'node:events';

vi.mock('node:child_process',async original=>({...await original<typeof import('node:child_process')>(),spawn:vi.fn()}));
vi.mock('../server/recall.js',()=>({importRecall:vi.fn(async()=>undefined)}));
vi.mock('../server/context-rank.js',()=>({rankContext:vi.fn(async()=>({}))}));
import {spawn} from 'node:child_process';
import {pool,event} from '../server/db.js';
import {config} from '../server/config.js';
import {accountBridge} from '../server/bridge.js';
import {runCycleAgent} from '../server/cycles/runtime.js';
import {cycleEvent} from '../server/cycles/store.js';
import {clean} from '../server/cycles/integrations.js';
const adapterUrl=new URL('../bridge/openai-completions-fallback.mjs',import.meta.url).href;
const {openAICompletionsFallback}=await import(adapterUrl);
const canary='TERMINAL-CONTENT-CANARY-NEVER-PERSIST';
let child:ReturnType<typeof makeChild>|undefined;
function makeChild(){return Object.assign(new EventEmitter(),{stdin:new PassThrough(),stdout:new PassThrough(),stderr:new PassThrough(),killed:false});}

beforeAll(()=>{expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');});
beforeEach(async()=>{
  await pool.query('TRUNCATE cycles,events CASCADE');
  config.bridgeHost='fixture-no-network';
});
afterEach(()=>{child?.emit('close',0);accountBridge.close();child=undefined;config.bridgeHost='';vi.restoreAllMocks();});
afterAll(async()=>{await pool.query('TRUNCATE cycles,events CASCADE');await pool.end();});

it.each(['accepted','invalid_tool_json'])(
  'persists %s diagnostics from actual parser through bridge and cycle runtime without accepting delivery',async(validation)=>{
    const cycleId=randomUUID(),occurrenceId=randomUUID(),stepId=randomUUID(),attemptId=randomUUID(),requestId=randomUUID();
    const definition={name:'Transport fixture',project:'terminal-fixture',description:'Observer-only diagnostic'};
    const step={id:stepId,key:'verify',definition:{title:'Verify',instructions:'Inspect evidence',workspace:'fixture/source',dependsOn:[],criteria:[]}};
    const occurrence={id:occurrenceId,cycle_id:cycleId,version:1,input:{},definition};
    const attempt={id:attemptId,fence:randomUUID(),number:1,model:'deepseek-v4-pro-0813'};
    await pool.query("INSERT INTO cycles(id,name,project,definition,request_key) VALUES($1::uuid,'Fixture','terminal-fixture',$2,$1::text)",[cycleId,JSON.stringify(definition)]);
    await pool.query("INSERT INTO cycle_occurrences(id,cycle_id,version,title,source_key,definition) VALUES($1::uuid,$2,1,'Fixture',$1::text,$3)",[occurrenceId,cycleId,JSON.stringify(definition)]);
    await pool.query("INSERT INTO cycle_steps(id,occurrence_id,key,definition,position,state,attempt) VALUES($1,$2,'verify',$3,0,'running',1)",[stepId,occurrenceId,JSON.stringify(step.definition)]);
    await pool.query("INSERT INTO cycle_attempts(id,step_id,number,fence,owner,model) VALUES($1,$2,1,$3,'fixture',$4)",[attemptId,stepId,attempt.fence,attempt.model]);
    child=makeChild();vi.mocked(spawn).mockReturnValue(child as any);
    let sent='',executeResolve!:(value:any)=>void;
    const executing=new Promise<any>(resolve=>{executeResolve=resolve;});
    child.stdin.on('data',bytes=>{
      sent+=bytes.toString();let cut;
      while((cut=sent.indexOf('\n'))!==-1){
        const message=JSON.parse(sent.slice(0,cut));sent=sent.slice(cut+1);
        if(message.method==='worker.prepare')child!.stdout.write(JSON.stringify({id:message.id,result:{prepared:true}})+'\n');
        if(message.method==='worker.execute')executeResolve(message);
      }
    });
    const result=runCycleAgent(occurrence,step,attempt,{bridge_id:'agentos:api:00000000-0000-4000-8000-000000000042'},
      new AbortController().signal,async(kind,payload)=>{
        await cycleEvent(pool,cycleId,occurrenceId,kind,clean(payload),stepId,attemptId);
        // Ordinary worker event persistence uses this same unmodified serializer.
        await event(pool,kind,stepId,{runId:attemptId,...payload});
      }).then(value=>({value,error:undefined}),error=>({value:undefined,error}));
    const dispatch=await executing;
    const envelope=(kind:string,payload:Record<string,unknown>)=>child!.stdout.write(JSON.stringify({id:dispatch.id,event:{kind,payload:{...payload,requestId}}})+'\n');
    const tool={id:canary+'-id',index:0,function:{name:canary+'-name',arguments:validation==='accepted'?JSON.stringify({privateKey:canary}):'{"privateKey":"'+canary}};
    const wire='data: '+JSON.stringify({choices:[{delta:{reasoning_content:canary,tool_calls:[tool]},finish_reason:'tool_calls'}]})+'\n\n';
    let fetches=0;
    const provider=openAICompletionsFallback({fetch:async()=>{fetches++;return new Response(wire,{headers:{'content-type':'text/event-stream'}});}});
    let modelResult,modelError;
    try{modelResult=await provider.streamSimple({id:attempt.model,baseUrl:'https://provider.invalid/v1'},
      {systemPrompt:canary,messages:[]},{apiKey:canary,onTelemetry:envelope}).result();}
    catch(error){modelError=error;}
    expect(fetches).toBe(1);
    if(validation==='accepted'){
      expect(modelResult.stopReason).toBe('toolUse');
      child.stdout.write(JSON.stringify({id:dispatch.id,result:{summary:'Returned',artifacts:[],evidence:{stopped:true}}})+'\n');
      expect((await result).error).toBeUndefined();
    }else{
      expect(modelResult).toBeUndefined();expect(modelError).toMatchObject({code:'MODEL_INCOMPLETE_RESPONSE'});
      child.stdout.write(JSON.stringify({id:dispatch.id,error:{code:'MODEL_INCOMPLETE_RESPONSE',message:'Typed transport failure'}})+'\n');
      expect((await result).error).toMatchObject({code:'MODEL_INCOMPLETE_RESPONSE'});
    }
    const persisted=(await pool.query("SELECT kind,payload,attempt_id FROM cycle_events WHERE occurrence_id=$1 ORDER BY id",[occurrenceId])).rows;
    const terminal=persisted.filter(row=>row.kind==='MODEL_TRANSPORT_FINISHED');
    expect(terminal).toHaveLength(1);
    expect(terminal[0]).toMatchObject({attempt_id:attemptId,payload:{version:1,requestId,protocolTerminal:{version:1,
      format:'sse',validation,finishReason:'tool_calls',doneObserved:false,eofObserved:true,toolCount:1,trailingBytes:0,aborted:false}}});
    const generic=(await pool.query("SELECT payload FROM events WHERE entity_id=$1 AND kind='MODEL_TRANSPORT_FINISHED'",[stepId])).rows;
    expect(generic).toHaveLength(1);expect(generic[0].payload).toEqual({...terminal[0].payload,runId:attemptId});
    expect(JSON.stringify(persisted)).not.toContain(canary);expect(JSON.stringify(generic)).not.toContain(canary);
    expect(Buffer.byteLength(JSON.stringify(terminal[0].payload))).toBeLessThan(4096);
    expect((await pool.query('SELECT state FROM cycle_steps WHERE id=$1',[stepId])).rows[0].state).toBe('running');
    expect((await pool.query('SELECT state FROM cycle_attempts WHERE id=$1',[attemptId])).rows[0].state).toBe('running');
    expect(accountBridge.pending.size).toBe(0);
  },
);
