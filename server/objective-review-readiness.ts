import {realpath} from 'node:fs/promises';
import {fileURLToPath} from 'node:url';
import path from 'node:path';
import {isDeepStrictEqual} from 'node:util';
import type {DB} from './db.js';
import {DomainError} from './contracts.js';
import {workerSourceDigest} from './worker-readiness.js';
import {workerReleaseIdentity,workerSchedulerMaintenance,workerRunAdmission,type WorkerRelease} from './worker-rollout.js';

const service='agentos-workers';
const notReady=()=>new DomainError('REVIEW_CONSUMER_NOT_READY',
  'O consumidor desta revisão ainda não adotou a mesma versão da API. Aguarde a atualização confirmada.',503);

/** Module identity, never caller input or process.cwd(). Schema installation by
 * an API process cannot claim that an old queue consumer understands new signals. */
export async function objectiveReviewControllerRelease():Promise<WorkerRelease>{
  try{
    const candidate=await realpath(fileURLToPath(new URL('../',import.meta.url)));
    const current=await realpath(path.resolve(candidate,'..','..','current'));
    if(current!==candidate)throw notReady();
    const release={candidate,digest:await workerSourceDigest(candidate)};
    workerReleaseIdentity(release);
    return release;
  }catch{throw notReady();}
}

/** Read-only admission under the scheduler lock. Reuses rollout's adoption
 * receipt and lane policy; requires the actual ObjectiveReviewEngine consumer,
 * not the separate backlog service. No readiness/capability is written here. */
export async function assertObjectiveReviewConsumerReady(db:DB,expected:WorkerRelease){
  try{await checkConsumerReady(db,expected);}catch{throw notReady();}
}
async function checkConsumerReady(db:DB,expected:WorkerRelease){
  try{workerReleaseIdentity(expected);}catch{throw notReady();}
  const maintenance=await workerSchedulerMaintenance(db);
  if(!workerRunAdmission(maintenance,{stage:'work'},'harness').allowed)throw notReady();
  const ready=(await db.query('SELECT value FROM settings WHERE id=$1',['worker-readiness:'+service])).rows[0]?.value;
  const adopted=(await db.query(`SELECT payload FROM events WHERE kind='WORKER_SERVICE_UPDATED'
    AND payload->>'service'=$1 ORDER BY id DESC LIMIT 1`,[service])).rows[0]?.payload;
  if(!ready||ready.service!==service||ready.candidate!==expected.candidate||ready.digest!==expected.digest||
    !Number.isSafeInteger(ready.pid)||ready.pid<=0||!Number.isFinite(Date.parse(ready.readyAt))||
    Date.parse(ready.readyAt)>Date.now()+5000||!/^[0-9a-f-]{36}$/.test(ready.startupId??'')||
    !Array.isArray(ready.locks)||ready.locks.length<2||ready.locks.length>8||
    ready.locks.some((holder:any)=>!Number.isSafeInteger(holder?.key)||holder.key<=0||holder.key>2147483647||
      !Number.isSafeInteger(holder?.backendPid)||holder.backendPid<=0||holder.backendPid>2147483647)||
    ![4280027,4280050].every(key=>ready.locks.some((holder:any)=>holder.key===key))||
    !adopted||adopted.candidate!==expected.candidate||adopted.pid!==ready.pid||
    !isDeepStrictEqual(adopted.readiness,ready)||adopted.stopped?.unit!==service||
    adopted.stopped.activeState!=='inactive'||adopted.stopped.mainPid!==0||
    adopted.stopped.oldPidGone!==true||adopted.stopped.cgroupEmpty!==true||
    !Number.isSafeInteger(adopted.stopped.previousPid)||adopted.stopped.previousPid<0||
    adopted.stopped.previousPid===ready.pid||
    !Number.isFinite(Date.parse(adopted.stopped.checkedAt))||Date.parse(adopted.stopped.checkedAt)>Date.parse(ready.readyAt))throw notReady();
  const owners=(await db.query(`SELECT count(*)::int n FROM
    jsonb_to_recordset($1::jsonb) AS holder(key int,"backendPid" int) JOIN pg_locks l
    ON l.locktype='advisory' AND l.classid=0 AND l.objsubid=1
      AND l.database=(SELECT oid FROM pg_database WHERE datname=current_database())
      AND l.objid=holder.key::oid AND l.pid=holder."backendPid" AND l.granted`,[JSON.stringify(ready.locks)])).rows[0]?.n;
  if(owners!==ready.locks.length)throw notReady();
}
