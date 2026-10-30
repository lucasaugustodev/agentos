import test from 'node:test';
import assert from 'node:assert/strict';
import {workspaceInspection} from '../bridge/workspace-inspection.mjs';

const runId='bd8daedb-d7a8-42da-bad7-a2dcd1be0270';
const bound={workspace:'sample/otarget',sourceWorkspace:'sample/osource',preparationIdentity:runId,
  expectedSourceDigest:'a'.repeat(64),expectedSourceSnapshotDigest:'b'.repeat(64)};

test('generation inspection forwards only the fixed readonly manager action',async()=>{
  const calls=[];
  const result=await workspaceInspection('worker.inspectGeneration',{workspace:bound.workspace},
    {operation:async params=>{calls.push(params);return {fixture:true};}});
  assert.deepEqual(calls,[{action:'inspect-generation',workspace:bound.workspace}]);
  assert.deepEqual(result,{fixture:true});
});

test('preparation inspection forwards the exact binding without calling prepare',async()=>{
  const calls=[];
  await workspaceInspection('worker.inspectPreparation',bound,{operation:async params=>calls.push(params)});
  assert.deepEqual(calls,[{action:'inspect-preparation',...bound}]);
});

test('unknown methods paths extra keys and incomplete bindings never reach the manager',async()=>{
  const calls=[];const io={operation:async value=>calls.push(value)};
  for(const [method,params] of [['worker.prepare',bound],['worker.inspectGeneration',{workspace:'../private'}],
    ['worker.inspectGeneration',{workspace:bound.workspace,action:'prepare'}],
    ['worker.inspectPreparation',{...bound,expectedSourceDigest:'bad'}],
    ['worker.inspectPreparation',{workspace:bound.workspace}],
    ['worker.inspectSourceBinding',{workspace:bound.workspace,runId:'../secret'}]])
    await assert.rejects(workspaceInspection(method,params,io),/WORKSPACE_INSPECTION_REQUEST_INVALID/);
  assert.deepEqual(calls,[]);
});

test('source binding through account bridge retains both physical stop probes',async()=>{
  const calls=[];
  const result=await workspaceInspection('worker.inspectSourceBinding',{runId,workspace:bound.workspace},{
    inspectUnit:async unit=>{calls.push('stop');return{unit,stopped:true,mainPid:0,populated:false,activeState:'inactive',checkedAt:new Date().toISOString()};},
    operation:async params=>{
      calls.push(params);return{version:1,runId,workspace:bound.workspace,
        state:{runId,workspace:bound.workspace,state:'completed',stopped:true,runFence:runId},
        admission:{runId,runFence:runId,sourceSnapshotDigest:bound.expectedSourceSnapshotDigest},
        generation:{workspace:bound.workspace,snapshotDigest:bound.expectedSourceSnapshotDigest}};
    },
  });
  assert.deepEqual(calls,['stop',{action:'inspect',workspace:bound.workspace,sourceBindingRunId:runId},'stop']);
  assert.equal(result.process.stopped,true);
});

test('source binding never inspects metadata while the physical run remains live',async()=>{
  const calls=[];
  await assert.rejects(workspaceInspection('worker.inspectSourceBinding',{runId,workspace:bound.workspace},{
    inspectUnit:async unit=>({unit,stopped:false,mainPid:5,populated:true,activeState:'active',checkedAt:new Date().toISOString()}),
    operation:async params=>calls.push(params),
  }),/STOP_UNCONFIRMED/);
  assert.deepEqual(calls,[]);
});
