import test from 'node:test';
import assert from 'node:assert/strict';
import {stoppedRunProof,stoppedSourceBindingProof} from '../bridge/stopped-run-proof.mjs';

const runId='bd8daedb-d7a8-42da-bad7-a2dcd1be0270',workspace='sample/ocandidate';
const fence='3c8920ad-d11e-4e8e-b1a3-aa7c6ee10465';
const stop=()=>({unit:`agentos-run-${runId}`,stopped:true,mainPid:0,populated:false,
  activeState:'inactive',checkedAt:new Date().toISOString()});
function fixture(){
  const calls=[];
  const value={version:1,runId,workspace,
    state:{runId,workspace,state:'completed',stopped:true,runFence:fence},
    admission:{runId,runFence:fence,sourceSnapshotDigest:'a'.repeat(64)},
    generation:{workspace,snapshotDigest:'a'.repeat(64),sourceSnapshotDigest:null},
    candidate:{candidateDigest:'b'.repeat(64)},legacyParentGeneration:true};
  const io={inspectUnit:async unit=>{calls.push(['unit',unit]);return stop();},
    inspectSourceBinding:async(run,source)=>{calls.push(['source',run,source]);return value;}};
  return{calls,value,io};
}

test('source binding requires physical stop before and after the protected metadata read',async()=>{
  const {calls,value,io}=fixture();
  const result=await stoppedSourceBindingProof(runId,workspace,io);
  assert.deepEqual(calls,[['unit',`agentos-run-${runId}`],['source',runId,workspace],['unit',`agentos-run-${runId}`]]);
  assert.equal(result.generation,value.generation);
  assert.equal(result.legacyParentGeneration,true);
  assert.equal(result.process.mainPid,0);
  assert.equal(result.checkedAt,result.process.checkedAt);
});

test('a live source unit prevents metadata inspection',async()=>{
  const {calls,io}=fixture();io.inspectUnit=async()=>({...stop(),stopped:false,mainPid:123,populated:true,activeState:'active'});
  await assert.rejects(stoppedSourceBindingProof(runId,workspace,io),/STOP_UNCONFIRMED/);
  assert.deepEqual(calls,[]);
});

test('a unit that becomes live during inspection invalidates the observation',async()=>{
  const {calls,io}=fixture();let count=0;
  io.inspectUnit=async()=>++count===1?stop():({...stop(),stopped:false,populated:true});
  await assert.rejects(stoppedSourceBindingProof(runId,workspace,io),/STOP_UNCONFIRMED/);
  assert.deepEqual(calls,[['source',runId,workspace]]);
});

test('a stopped boolean alone is insufficient without zero PID and empty cgroup',async()=>{
  for(const changed of [{mainPid:7},{populated:true},{activeState:'active'},{unit:'agentos-run-other'}]){
    const {calls,io}=fixture();io.inspectUnit=async()=>({...stop(),...changed});
    await assert.rejects(stoppedSourceBindingProof(runId,workspace,io),/STOP_UNCONFIRMED/);
    assert.deepEqual(calls,[]);
  }
});

test('mismatched run workspace fence or generation from inspection is rejected',async()=>{
  for(const change of [value=>{value.runId='other';},value=>{value.workspace='sample/other';},
    value=>{value.state.runFence='other';},value=>{value.state.stopped=false;},
    value=>{value.admission.sourceSnapshotDigest='c'.repeat(64);},
    value=>{value.generation.workspace='sample/other';}]){
    const {value,io}=fixture();change(value);
    await assert.rejects(stoppedSourceBindingProof(runId,workspace,io),/SOURCE_BINDING_INSPECTION_CONFLICT/);
  }
});

test('stale or future process observations never authorize a source read',async()=>{
  for(const offset of [-61_000,61_000]){
    const {calls,io}=fixture();io.inspectUnit=async()=>({...stop(),checkedAt:new Date(Date.now()+offset).toISOString()});
    await assert.rejects(stoppedSourceBindingProof(runId,workspace,io),/STOP_UNCONFIRMED/);
    assert.deepEqual(calls,[]);
  }
});

test('source binding accepts only run UUID and managed workspace IDs',async()=>{
  for(const [run,source] of [['../state',workspace],[runId,'../private'],[runId,'/etc/private'],[runId,undefined]]){
    const {calls,io}=fixture();
    await assert.rejects(stoppedSourceBindingProof(run,source,io),/SOURCE_BINDING_IDENTITY_INVALID/);
    assert.deepEqual(calls,[]);
  }
});

test('ordinary stopped-run recovery retains its existing response without admission metadata',async()=>{
  const result=await stoppedRunProof(runId,undefined,{
    readState:async()=>({runId,workspace,state:'failed'}),inspectUnit:async()=>stop(),
    inspectWorkspace:async()=>({candidateDigest:'a'.repeat(64),patch:'preserved patch'}),
  });
  assert.equal(result.state,'failed');
  assert.equal(result.candidate.patch,'preserved patch');
  assert.equal(result.admission,undefined);
});
