import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {createGlmRequestGate} from '../bridge/glm-request-gate.mjs';
import {acquireGlmRequestSlot} from '../bridge/glm-request-slot.mjs';
import {withModelPolicyGateBarrier} from '../bridge/model-policy-gate.mjs';
const sleep=ms=>new Promise(resolve=>setTimeout(resolve,ms));
async function until(check){for(let i=0;i<1000;i++){if(await check())return;await sleep(5);}assert.fail('gate did not converge');}
async function fixture(t){const dir=await mkdtemp(tmpdir()+'/agentos-policy-barrier-'),socketPath=dir+'/gate.sock';
  const gate=await createGlmRequestGate({socketPath});t.after(async()=>{await gate.close();await rm(dir,{recursive:true,force:true});});return {gate,socketPath};}
test('installation owns six permits beyond admission timeout and blocks any competing transport',async t=>{
  const {gate,socketPath}=await fixture(t);let invoked=false;
  await withModelPolicyGateBarrier('fixture',async check=>{
    assert.equal((await gate.status()).active,6);const controller=new AbortController();
    const queued=acquireGlmRequestSlot({socketPath,runId:'competitor',signal:controller.signal}).then(slot=>{slot.release();invoked=true;}).catch(()=>{});
    await until(async()=>(await gate.status()).queued===1);assert.equal(invoked,false);
    await assert.rejects(check(),{code:'MODEL_POLICY_GATE_BUSY'});controller.abort();await queued;
    await until(async()=>(await gate.status()).queued===0);await sleep(2100);await check();
    assert.equal((await gate.status()).active,6);
  },{socketPath});
  await until(async()=>(await gate.status()).active===0);assert.equal(invoked,false);
});
test('an already owned provider permit blocks installation without acquiring any administrative slot',async t=>{
  const {gate,socketPath}=await fixture(t),provider=await acquireGlmRequestSlot({socketPath,runId:'live-provider'});let applied=false;
  try{await assert.rejects(withModelPolicyGateBarrier('fixture',async()=>{applied=true;},{socketPath}),{code:'MODEL_POLICY_GATE_BUSY'});
    assert.equal(applied,false);assert.equal((await gate.status()).active,1);
  }finally{provider.release();}
});
test('a failed installation releases its six permits and never retries the operation',async t=>{
  const {gate,socketPath}=await fixture(t);let calls=0;
  await assert.rejects(withModelPolicyGateBarrier('fixture',async()=>{calls++;throw Error('fixture-failure');},{socketPath}),/fixture-failure/);
  await until(async()=>(await gate.status()).active===0);assert.equal(calls,1);
});
test('lost gate fails closed without replay or a successful installation claim',async t=>{
  const {gate,socketPath}=await fixture(t);let calls=0;
  await assert.rejects(withModelPolicyGateBarrier('fixture',async check=>{calls++;await gate.close();await sleep(10);await check();},{socketPath}));
  assert.equal(calls,1);
});
