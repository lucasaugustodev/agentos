import { it, expect, vi, afterEach } from 'vitest';
import { PassThrough } from 'node:stream';
import { EventEmitter } from 'node:events';
vi.mock('node:child_process', async (importOriginal) => ({
  ...(await importOriginal<typeof import('node:child_process')>()),
  spawn: vi.fn(),
}));
import { spawn } from 'node:child_process';
import { AccountBridge } from '../server/bridge.js';
import { config } from '../server/config.js';
import { taskDeliveryContract } from '../bridge/task-delivery-contract.mjs';
function fixture() {
  config.bridgeHost = 'test-only';
  const child = Object.assign(new EventEmitter(), {
    stdin: new PassThrough(),
    stdout: new PassThrough(),
    stderr: new PassThrough(),
    killed: false,
  });
  vi.mocked(spawn).mockReturnValue(child as any);
  return { bridge: new AccountBridge(), child };
}
afterEach(() => {
  config.bridgeHost = '';
  vi.restoreAllMocks();
  vi.useRealTimers();
});
it('preserves the typed task delivery contract in the worker.execute SSH envelope', async () => {
  const { bridge, child } = fixture();
  const deliveryContract = taskDeliveryContract([
    { type: 'artifact', name: 'change.diff' },
    { type: 'artifact', name: 'manifest.json' },
    { type: 'json_field', name: 'verification.json' },
  ]);
  const result = bridge.request('worker.execute', { runId: 'fixture', deliveryContract });
  const sent = JSON.parse(child.stdin.read().toString());
  expect(sent).toMatchObject({ method: 'worker.execute', params: { runId: 'fixture', deliveryContract } });
  child.stdout.write(JSON.stringify({ id: sent.id, result: { evidence: { stopped: true } } }) + '\n');
  expect(await result).toEqual({ evidence: { stopped: true } });
  bridge.close();
});
it('keeps cancellation pending until the remote request finishes', async () => {
  const { bridge, child } = fixture();
  const controller = new AbortController();
  const result = bridge.request('generate', {}, { signal: controller.signal }).catch((e) => e);
  const id = [...bridge.pending.keys()][0];
  controller.abort();
  expect(bridge.pending.size).toBe(1);
  child.stdout.write(
    JSON.stringify({ id, error: { code: 'CANCELLED', message: 'Stopped' } }) + '\n',
  );
  expect(await result).toMatchObject({ code: 'CANCELLED' });
  expect(bridge.pending.size).toBe(0);
  bridge.close();
});
it('marks a broken connection as uncertain instead of claiming cancellation', async () => {
  const { bridge, child } = fixture();
  const result = bridge.request('generate').catch((e) => e);
  child.emit('close', 255);
  expect(await result).toMatchObject({ code: 'STOP_UNCONFIRMED' });
  expect(bridge.pending.size).toBe(0);
  bridge.close();
});
it('keeps worker events pending and never masks unconfirmed stop with a requested cancellation', async () => {
  const { bridge, child } = fixture();
  const c = new AbortController(),
    onEvent = vi.fn();
  const result = bridge
    .request('worker.execute', {}, { signal: c.signal, onEvent })
    .catch((e) => e);
  const id = [...bridge.pending.keys()][0];
  child.stdout.write(
    JSON.stringify({ id, event: { kind: 'HARNESS_STARTED', payload: { ready: true } } }) + '\n',
  );
  expect(onEvent).toHaveBeenCalledWith('HARNESS_STARTED', { ready: true });
  expect(bridge.pending.size).toBe(1);
  c.abort();
  child.stdout.write(
    JSON.stringify({ id, error: { code: 'STOP_UNCONFIRMED', message: 'Still uncertain' } }) + '\n',
  );
  expect(await result).toMatchObject({ code: 'STOP_UNCONFIRMED' });
  bridge.close();
});

it('keeps an active worker beyond the original deadline and clears its timer on confirmed completion', async () => {
  vi.useFakeTimers();
  const {bridge,child}=fixture();
  const result=bridge.request('worker.execute',{}, {idleTimeout:100});
  const {id}=JSON.parse(child.stdin.read().toString());
  await vi.advanceTimersByTimeAsync(60);
  child.stdout.write(JSON.stringify({id,event:{kind:'MODEL_RESPONSE',payload:{requests:1}}})+'\n');
  await vi.advanceTimersByTimeAsync(60);
  child.stdout.write(JSON.stringify({id,event:{kind:'HARNESS_TOOL_EVENT',payload:{toolEvents:1}}})+'\n');
  await vi.advanceTimersByTimeAsync(60);
  expect(bridge.pending.get(id)?.cancelError).toBeUndefined();
  expect(child.stdin.read()).toBeNull();
  child.stdout.write(JSON.stringify({id,result:{summary:'Actual result',evidence:{stopped:true}}})+'\n');
  expect(await result).toMatchObject({summary:'Actual result',evidence:{stopped:true}});
  expect(bridge.pending.size).toBe(0);
  expect(vi.getTimerCount()).toBe(0);
  bridge.close();
});

it('heartbeat and queued/request-start notifications do not extend inactivity or prove completion', async () => {
  vi.useFakeTimers();
  const {bridge,child}=fixture();
  const result=bridge.request('worker.execute',{}, {idleTimeout:100}).catch(e=>e);
  const {id}=JSON.parse(child.stdin.read().toString());
  for (const kind of ['HEARTBEAT','GLM_REQUEST_SLOT_QUEUED','MODEL_REQUEST']) {
    await vi.advanceTimersByTimeAsync(30);
    child.stdout.write(JSON.stringify({id,event:{kind,payload:{}}})+'\n');
  }
  await vi.advanceTimersByTimeAsync(10);
  expect(bridge.pending.get(id)?.cancelError?.code).toBe('RUNTIME_IDLE_TIMEOUT');
  expect(JSON.parse(child.stdin.read().toString())).toEqual({method:'cancel',params:{id}});
  expect(bridge.pending.size).toBe(1);
  child.stdout.write(JSON.stringify({id,error:{code:'STOP_UNCONFIRMED',message:'Still alive'}})+'\n');
  expect(await result).toMatchObject({code:'STOP_UNCONFIRMED'});
  bridge.close();
});

it('terminal protocol diagnostics are forwarded but cannot renew inactivity or complete a worker', async () => {
  vi.useFakeTimers();
  const {bridge,child}=fixture(),onEvent=vi.fn();
  const result=bridge.request('worker.execute',{}, {idleTimeout:100,onEvent}).catch(e=>e);
  const {id}=JSON.parse(child.stdin.read().toString());
  await vi.advanceTimersByTimeAsync(90);
  const payload={requestId:'10000000-0000-4000-8000-000000000001',version:1,
    protocolTerminal:{version:1,format:'sse',eofObserved:true,doneObserved:false,finishReason:'tool_calls',
      validation:'accepted',trailingBytes:0,toolCount:1,aborted:false}};
  child.stdout.write(JSON.stringify({id,event:{kind:'MODEL_TRANSPORT_FINISHED',payload}})+'\n');
  expect(onEvent).toHaveBeenCalledWith('MODEL_TRANSPORT_FINISHED',payload);
  expect(bridge.pending.size).toBe(1);
  await vi.advanceTimersByTimeAsync(10);
  expect(bridge.pending.get(id)?.cancelError?.code).toBe('RUNTIME_IDLE_TIMEOUT');
  child.stdout.write(JSON.stringify({id,error:{code:'STOP_UNCONFIRMED',message:'Still uncertain'}})+'\n');
  expect(await result).toMatchObject({code:'STOP_UNCONFIRMED'});
  bridge.close();
});

it('late progress cannot renew the remote stop-confirmation deadline', async () => {
  vi.useFakeTimers();
  const {bridge,child}=fixture();
  const result=bridge.request('worker.execute',{}, {idleTimeout:100}).catch(e=>e);
  const {id}=JSON.parse(child.stdin.read().toString());
  await vi.advanceTimersByTimeAsync(100+14999);
  child.stdout.write(JSON.stringify({id,event:{kind:'MODEL_RESPONSE',payload:{}}})+'\n');
  await vi.advanceTimersByTimeAsync(1);
  expect(await result).toMatchObject({code:'STOP_UNCONFIRMED'});
  expect(bridge.pending.size).toBe(0);
  bridge.close();
});

it('explicit cancellation still takes precedence over worker progress and requires remote confirmation', async () => {
  vi.useFakeTimers();
  const {bridge,child}=fixture(), controller=new AbortController();
  const result=bridge.request('worker.execute',{}, {idleTimeout:100,signal:controller.signal}).catch(e=>e);
  const {id}=JSON.parse(child.stdin.read().toString());
  controller.abort();
  child.stdout.write(JSON.stringify({id,event:{kind:'HARNESS_TOOL_EVENT',payload:{}}})+'\n');
  expect(bridge.pending.size).toBe(1);
  child.stdout.write(JSON.stringify({id,error:{code:'CANCELLED',message:'Stopped'}})+'\n');
  expect(await result).toMatchObject({code:'CANCELLED'});
  expect(vi.getTimerCount()).toBe(0);
  bridge.close();
});

it('ordinary model calls retain their absolute response timeout despite events', async () => {
  vi.useFakeTimers();
  const {bridge,child}=fixture();
  const result=bridge.request('generate',{}, {timeout:100}).catch(e=>e);
  const {id}=JSON.parse(child.stdin.read().toString());
  await vi.advanceTimersByTimeAsync(90);
  child.stdout.write(JSON.stringify({id,event:{kind:'MODEL_RESPONSE',payload:{}}})+'\n');
  await vi.advanceTimersByTimeAsync(10);
  expect(bridge.pending.get(id)?.cancelError?.code).toBe('PROVIDER_TIMEOUT');
  child.stdout.write(JSON.stringify({id,error:{code:'CANCELLED',message:'Stopped'}})+'\n');
  expect(await result).toMatchObject({code:'PROVIDER_TIMEOUT'});
  bridge.close();
});

const firstModelRequest = '44444444-4444-4444-8444-444444444444';
const secondModelRequest = '55555555-5555-4555-8555-555555555555';
function bridgeEvent(child: ReturnType<typeof fixture>['child'], id: string, kind: string, payload: unknown) {
  child.stdout.write(JSON.stringify({id,event:{kind,payload}})+'\n');
}
function semanticProgress(requestId: string, characters = 0, reasoningCharacters = 0, toolCharacters = 0) {
  return {requestId,characters,reasoningCharacters,toolCharacters,
    semanticCharacters:characters+reasoningCharacters+toolCharacters};
}

it('renews worker inactivity for identified semantic stream progress while never completing the task', async () => {
  vi.useFakeTimers();
  const {bridge,child}=fixture(), onEvent=vi.fn();
  const result=bridge.request('worker.execute',{}, {idleTimeout:100,onEvent});
  const {id}=JSON.parse(child.stdin.read().toString());
  bridgeEvent(child,id,'MODEL_REQUEST',{requestId:firstModelRequest,requests:1});
  for (const [characters,reasoningCharacters,toolCharacters] of [[1,0,0],[1,3,0],[1,3,4]]) {
    await vi.advanceTimersByTimeAsync(80);
    bridgeEvent(child,id,'MODEL_STREAM_PROGRESS',semanticProgress(firstModelRequest,characters,reasoningCharacters,toolCharacters));
  }
  expect(bridge.pending.get(id)?.cancelError).toBeUndefined();
  expect(bridge.pending.size).toBe(1);
  expect(child.stdin.read()).toBeNull();
  expect(onEvent).toHaveBeenCalledWith('MODEL_STREAM_PROGRESS',semanticProgress(firstModelRequest,1,3,4));
  bridgeEvent(child,id,'MODEL_RESPONSE',{requestId:firstModelRequest,requests:1});
  child.stdout.write(JSON.stringify({id,result:{evidence:{stopped:true}}})+'\n');
  expect(await result).toEqual({evidence:{stopped:true}});
  expect(vi.getTimerCount()).toBe(0);
  bridge.close();
});

it('cannot establish progress using an unannounced, malformed or out-of-sequence request identity', async () => {
  vi.useFakeTimers();
  const {bridge,child}=fixture();
  const result=bridge.request('worker.execute',{}, {idleTimeout:100}).catch(e=>e);
  const {id}=JSON.parse(child.stdin.read().toString());
  await vi.advanceTimersByTimeAsync(40);
  bridgeEvent(child,id,'MODEL_STREAM_PROGRESS',semanticProgress(firstModelRequest,2));
  bridgeEvent(child,id,'MODEL_REQUEST',{requestId:'arbitrary',requests:1});
  bridgeEvent(child,id,'MODEL_STREAM_PROGRESS',semanticProgress('arbitrary',3));
  bridgeEvent(child,id,'MODEL_REQUEST',{requestId:secondModelRequest,requests:2});
  bridgeEvent(child,id,'MODEL_STREAM_PROGRESS',semanticProgress(secondModelRequest,4));
  await vi.advanceTimersByTimeAsync(60);
  expect(bridge.pending.get(id)?.cancelError?.code).toBe('RUNTIME_IDLE_TIMEOUT');
  child.stdout.write(JSON.stringify({id,error:{code:'CANCELLED',message:'Stopped'}})+'\n');
  expect(await result).toMatchObject({code:'RUNTIME_IDLE_TIMEOUT'});
  bridge.close();
});

it.each([
  ['duplicate counters', {characters:2,reasoningCharacters:2,toolCharacters:2,semanticCharacters:6,chunks:99,elapsedMs:9000}],
  ['regressive text', {characters:1,reasoningCharacters:3,toolCharacters:3,semanticCharacters:7}],
  ['regressive reasoning', {characters:3,reasoningCharacters:1,toolCharacters:3,semanticCharacters:7}],
  ['regressive tool arguments', {characters:3,reasoningCharacters:3,toolCharacters:1,semanticCharacters:7}],
  ['regressive total', {characters:2,reasoningCharacters:2,toolCharacters:2,semanticCharacters:5}],
  ['inconsistent total', {characters:3,reasoningCharacters:3,toolCharacters:3,semanticCharacters:8}],
  ['fractional counter', {characters:2.5,reasoningCharacters:2,toolCharacters:2,semanticCharacters:7}],
  ['negative counter', {characters:-1,reasoningCharacters:3,toolCharacters:3,semanticCharacters:7}],
  ['string counter', {characters:'3',reasoningCharacters:3,toolCharacters:3,semanticCharacters:9}],
  ['unsafe counter', {characters:Number.MAX_SAFE_INTEGER+1,reasoningCharacters:3,toolCharacters:3,semanticCharacters:Number.MAX_SAFE_INTEGER+1}],
  ['missing counters', {characters:3,reasoningCharacters:3,semanticCharacters:9}],
  ['keepalive only', {chunks:99,elapsedMs:9000,done:true}],
] as const)('does not renew inactivity for %s', async (_name, invalid) => {
  vi.useFakeTimers();
  const {bridge,child}=fixture();
  const result=bridge.request('worker.execute',{}, {idleTimeout:100}).catch(e=>e);
  const {id}=JSON.parse(child.stdin.read().toString());
  bridgeEvent(child,id,'MODEL_REQUEST',{requestId:firstModelRequest,requests:1});
  await vi.advanceTimersByTimeAsync(50);
  bridgeEvent(child,id,'MODEL_STREAM_PROGRESS',semanticProgress(firstModelRequest,2,2,2));
  await vi.advanceTimersByTimeAsync(50);
  bridgeEvent(child,id,'MODEL_STREAM_PROGRESS',{requestId:firstModelRequest,...invalid});
  await vi.advanceTimersByTimeAsync(50);
  expect(bridge.pending.get(id)?.cancelError?.code).toBe('RUNTIME_IDLE_TIMEOUT');
  child.stdout.write(JSON.stringify({id,error:{code:'CANCELLED',message:'Stopped'}})+'\n');
  expect(await result).toMatchObject({code:'RUNTIME_IDLE_TIMEOUT'});
  bridge.close();
});

it('tracks each announced request independently and rejects delayed progress after its response', async () => {
  vi.useFakeTimers();
  const {bridge,child}=fixture();
  const result=bridge.request('worker.execute',{}, {idleTimeout:100}).catch(e=>e);
  const {id}=JSON.parse(child.stdin.read().toString());
  bridgeEvent(child,id,'MODEL_REQUEST',{requestId:firstModelRequest,requests:1});
  await vi.advanceTimersByTimeAsync(50);
  bridgeEvent(child,id,'MODEL_STREAM_PROGRESS',semanticProgress(firstModelRequest,100));
  bridgeEvent(child,id,'MODEL_RESPONSE',{requestId:firstModelRequest,requests:1});
  await vi.advanceTimersByTimeAsync(50);
  bridgeEvent(child,id,'MODEL_REQUEST',{requestId:secondModelRequest,requests:2});
  await vi.advanceTimersByTimeAsync(40);
  bridgeEvent(child,id,'MODEL_STREAM_PROGRESS',semanticProgress(secondModelRequest,1));
  await vi.advanceTimersByTimeAsync(60);
  bridgeEvent(child,id,'MODEL_STREAM_PROGRESS',semanticProgress(firstModelRequest,200));
  await vi.advanceTimersByTimeAsync(40);
  expect(bridge.pending.get(id)?.cancelError?.code).toBe('RUNTIME_IDLE_TIMEOUT');
  child.stdout.write(JSON.stringify({id,error:{code:'CANCELLED',message:'Stopped'}})+'\n');
  expect(await result).toMatchObject({code:'RUNTIME_IDLE_TIMEOUT'});
  bridge.close();
});

it('does not reset semantic counters when the same request is announced again or reused', async () => {
  vi.useFakeTimers();
  const {bridge,child}=fixture();
  const result=bridge.request('worker.execute',{}, {idleTimeout:100}).catch(e=>e);
  const {id}=JSON.parse(child.stdin.read().toString());
  bridgeEvent(child,id,'MODEL_REQUEST',{requestId:firstModelRequest,requests:1});
  await vi.advanceTimersByTimeAsync(20);
  bridgeEvent(child,id,'MODEL_STREAM_PROGRESS',semanticProgress(firstModelRequest,10));
  await vi.advanceTimersByTimeAsync(60);
  bridgeEvent(child,id,'MODEL_REQUEST',{requestId:firstModelRequest,requests:1});
  bridgeEvent(child,id,'MODEL_REQUEST',{requestId:firstModelRequest,requests:2});
  bridgeEvent(child,id,'MODEL_STREAM_PROGRESS',semanticProgress(firstModelRequest,5));
  await vi.advanceTimersByTimeAsync(40);
  expect(bridge.pending.get(id)?.cancelError?.code).toBe('RUNTIME_IDLE_TIMEOUT');
  child.stdout.write(JSON.stringify({id,error:{code:'CANCELLED',message:'Stopped'}})+'\n');
  expect(await result).toMatchObject({code:'RUNTIME_IDLE_TIMEOUT'});
  bridge.close();
});

it('does not renew liveness for repeated model responses or tool events', async () => {
  vi.useFakeTimers();
  const {bridge,child}=fixture();
  const result=bridge.request('worker.execute',{}, {idleTimeout:100}).catch(e=>e);
  const {id}=JSON.parse(child.stdin.read().toString());
  await vi.advanceTimersByTimeAsync(20);
  bridgeEvent(child,id,'MODEL_RESPONSE',{requests:1});
  bridgeEvent(child,id,'HARNESS_TOOL_EVENT',{toolEvents:2});
  await vi.advanceTimersByTimeAsync(60);
  bridgeEvent(child,id,'MODEL_RESPONSE',{requests:1});
  bridgeEvent(child,id,'HARNESS_TOOL_EVENT',{toolEvents:1});
  bridgeEvent(child,id,'HARNESS_TOOL_EVENT',{toolEvents:2});
  bridgeEvent(child,id,'HARNESS_TOOL_EVENT',null);
  await vi.advanceTimersByTimeAsync(40);
  expect(bridge.pending.get(id)?.cancelError?.code).toBe('RUNTIME_IDLE_TIMEOUT');
  child.stdout.write(JSON.stringify({id,error:{code:'CANCELLED',message:'Stopped'}})+'\n');
  expect(await result).toMatchObject({code:'RUNTIME_IDLE_TIMEOUT'});
  bridge.close();
});

it('closes failed request identities and cannot postpone a pending stop with late semantic progress', async () => {
  vi.useFakeTimers();
  const {bridge,child}=fixture();
  const result=bridge.request('worker.execute',{}, {idleTimeout:100}).catch(e=>e);
  const {id}=JSON.parse(child.stdin.read().toString());
  bridgeEvent(child,id,'MODEL_REQUEST',{requestId:firstModelRequest,requests:1});
  await vi.advanceTimersByTimeAsync(40);
  bridgeEvent(child,id,'MODEL_ERROR',{requestId:firstModelRequest,requests:1,code:'MODEL_IDLE_TIMEOUT'});
  bridgeEvent(child,id,'MODEL_STREAM_PROGRESS',semanticProgress(firstModelRequest,2));
  await vi.advanceTimersByTimeAsync(60);
  expect(bridge.pending.get(id)?.cancelError?.code).toBe('RUNTIME_IDLE_TIMEOUT');
  bridgeEvent(child,id,'MODEL_REQUEST',{requestId:secondModelRequest,requests:2});
  await vi.advanceTimersByTimeAsync(14999);
  bridgeEvent(child,id,'MODEL_STREAM_PROGRESS',semanticProgress(secondModelRequest,3));
  await vi.advanceTimersByTimeAsync(1);
  expect(await result).toMatchObject({code:'STOP_UNCONFIRMED'});
  expect(bridge.pending.size).toBe(0);
  bridge.close();
});

it('retires a completed request even when a later concurrent request responded first', async () => {
  vi.useFakeTimers();
  const {bridge,child}=fixture();
  const result=bridge.request('worker.execute',{}, {idleTimeout:100}).catch(e=>e);
  const {id}=JSON.parse(child.stdin.read().toString());
  bridgeEvent(child,id,'MODEL_REQUEST',{requestId:firstModelRequest,requests:1});
  bridgeEvent(child,id,'MODEL_REQUEST',{requestId:secondModelRequest,requests:2});
  await vi.advanceTimersByTimeAsync(20);
  bridgeEvent(child,id,'MODEL_STREAM_PROGRESS',semanticProgress(firstModelRequest,1));
  bridgeEvent(child,id,'MODEL_RESPONSE',{requestId:secondModelRequest,requests:2});
  await vi.advanceTimersByTimeAsync(30);
  bridgeEvent(child,id,'MODEL_RESPONSE',{requestId:firstModelRequest,requests:1});
  await vi.advanceTimersByTimeAsync(50);
  bridgeEvent(child,id,'MODEL_STREAM_PROGRESS',semanticProgress(firstModelRequest,2));
  await vi.advanceTimersByTimeAsync(20);
  expect(bridge.pending.get(id)?.cancelError?.code).toBe('RUNTIME_IDLE_TIMEOUT');
  child.stdout.write(JSON.stringify({id,error:{code:'CANCELLED',message:'Stopped'}})+'\n');
  expect(await result).toMatchObject({code:'RUNTIME_IDLE_TIMEOUT'});
  bridge.close();
});
