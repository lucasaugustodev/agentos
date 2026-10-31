import {afterAll,afterEach,expect,it,vi} from 'vitest';
import {waitForCandidateValidationStop,requestConfirmedShutdown} from '../server/candidate-validation-lifecycle.js';
import {pool} from '../server/db.js';
afterEach(()=>vi.useRealTimers());
afterAll(()=>pool.end());

it('waits for both existing reconciliations and does not fabricate a new command',async()=>{
  let releaseOwner!:()=>void,releaseEvidence!:()=>void;
  const owner=new Promise<void>(r=>{releaseOwner=r;});
  const evidence=new Promise<void>(r=>{releaseEvidence=r;});
  let stopped=false;
  const stopping=waitForCandidateValidationStop([owner,evidence],1000).then(()=>{stopped=true;});
  releaseOwner();await Promise.resolve();expect(stopped).toBe(false);
  releaseEvidence();await stopping;expect(stopped).toBe(true);
});
it('bounds the wait without cancelling, settling or replacing the original owner',async()=>{
  vi.useFakeTimers();let release!:()=>void,settled=false;
  const owner=new Promise<void>(r=>{release=r;}).then(()=>{settled=true;});
  const stopping=waitForCandidateValidationStop([owner],25);
  const rejected=expect(stopping).rejects.toThrow('CANDIDATE_VALIDATION_STOP_UNCONFIRMED');
  await vi.advanceTimersByTimeAsync(25);await rejected;
  expect(settled).toBe(false);expect(vi.getTimerCount()).toBe(0);
  release();await owner;expect(settled).toBe(true);
});
it('a rejected reconciliation reports only a safe refusal',async()=>{
  await expect(waitForCandidateValidationStop([Promise.reject(Error('PRIVATE_REASON'))])).rejects.toThrow(/^CANDIDATE_VALIDATION_STOP_UNCONFIRMED$/);
});
it('handles refused signal shutdown without running the success exit path',async()=>{
  const exit=vi.fn(),unconfirmed=vi.fn();
  const close=async()=>{await waitForCandidateValidationStop([Promise.reject(Error('PRIVATE'))]);exit();};
  await expect(requestConfirmedShutdown(close,unconfirmed)).resolves.toBeUndefined();
  expect(unconfirmed).toHaveBeenCalledOnce();expect(exit).not.toHaveBeenCalled();
});
it('retains the ordinary successful shutdown path',async()=>{
  const exit=vi.fn(),unconfirmed=vi.fn();
  await requestConfirmedShutdown(async()=>{await waitForCandidateValidationStop([null]);exit();},unconfirmed);
  expect(exit).toHaveBeenCalledOnce();expect(unconfirmed).not.toHaveBeenCalled();
});
