import {createHash} from 'node:crypto';
import {z} from 'zod';
import {tx,lock,event,type DB} from './db.js';
import {validateModelPolicy,type ModelPolicy} from '../bridge/model-policy.mjs';
import {workerReleaseIdentity} from './worker-rollout.js';
import {requireSchemaReady} from './schema-readiness.js';
import {requireBacklogSchemaReady} from './backlog-schema-readiness.js';

const sha=z.string().regex(/^[0-9a-f]{64}$/);
export const modelPolicyInstallationInput=z.object({version:z.literal(1),token:z.uuid(),
  candidate:z.string().regex(/^\/opt\/agentos\/releases\/managed-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/),digest:sha,
  expectedPolicyFileDigest:sha,expectedPreviousPolicyDigest:sha.nullable()}).strict();
export type ModelPolicyInstallationInput=z.infer<typeof modelPolicyInstallationInput>;
export type PolicyInstallationObservation={checkedAt:number;sourceDigest:string;fileDigest:string;
  policy:ModelPolicy;servicesStopped:true;gateQuiescent:true;publicationLockHeld:true;fileIdentity:string};
const failure=(code:string)=>Object.assign(Error(code),{code});
function requireValue(value:unknown,code:string):asserts value{if(!value)throw failure(code);}
function canonical(value:any):any{
  if(Array.isArray(value))return value.map(canonical);
  if(value&&typeof value==='object')return Object.fromEntries(Object.keys(value).sort().map(key=>[key,canonical(value[key])]));
  return value;
}
export const modelPolicyDigest=(value:unknown)=>createHash('sha256').update(JSON.stringify(canonical(value))).digest('hex');
const tables=['runs','cycle_attempts','planning_runs','chat_messages','workspace_preparations',
  'opencode_jobs','supervisor_jobs','agentos_queue.job','leases','settings'];
export async function modelPolicyQuiet(db:DB){
  const catalog=(await db.query('SELECT name,to_regclass(name)::text relation FROM unnest($1::text[]) name',[tables])).rows;
  requireValue(tables.every(name=>catalog.some(row=>row.name===name&&row.relation)),'MODEL_POLICY_QUIET_TABLES');
  const counts=(await db.query(`SELECT
    (SELECT count(*)::int FROM runs WHERE state IN('queued','running','cancelling','reconciling')) runs,
    (SELECT count(*)::int FROM cycle_attempts WHERE state IN('queued','running','cancelling','reconciling')) cycles,
    (SELECT count(*)::int FROM planning_runs WHERE state='running') planning,
    (SELECT count(*)::int FROM chat_messages WHERE state IN('queued','running','cancelling','reconciling')) chats,
    (SELECT count(*)::int FROM workspace_preparations WHERE state IS DISTINCT FROM 'prepared') preparations,
    (SELECT count(*)::int FROM settings WHERE id LIKE 'planning-source-preparation:%'
      AND COALESCE(value->>'state','') NOT IN('prepared','applied')) planning_preparations,
    (SELECT count(*)::int FROM opencode_jobs WHERE state IS DISTINCT FROM 'finished') native_jobs,
    (SELECT count(*)::int FROM supervisor_jobs WHERE state IS DISTINCT FROM 'completed') supervisor_jobs,
    (SELECT count(*)::int FROM agentos_queue.job WHERE state='active') queue_active,
    (SELECT count(*)::int FROM leases) leases`)).rows[0];
  requireValue(counts&&Object.values(counts).every(value=>Number.isSafeInteger(value)&&value===0),'MODEL_POLICY_NOT_QUIET');
  return counts;
}
function observation(value:PolicyInstallationObservation,input:ModelPolicyInstallationInput){
  requireValue(value&&Number.isSafeInteger(value.checkedAt)&&value.checkedAt<=Date.now()&&
    Date.now()-value.checkedAt<=30000,'MODEL_POLICY_OBSERVATION_STALE');
  requireValue(value.sourceDigest===input.digest&&value.fileDigest===input.expectedPolicyFileDigest&&
    typeof value.fileIdentity==='string'&&value.fileIdentity.length>0,'MODEL_POLICY_SOURCE_CHANGED');
  requireValue(value.servicesStopped===true&&value.gateQuiescent===true&&value.publicationLockHeld===true,'MODEL_POLICY_PHYSICAL_BUSY');
  validateModelPolicy(value.policy);
}
async function owner(db:DB,input:ModelPolicyInstallationInput){
  const value=(await db.query("SELECT value FROM settings WHERE id='maintenance' FOR UPDATE")).rows[0]?.value;
  const rollout=value?.workerRollout;
  requireValue(value?.draining===true&&value.owner==='worker-rollout'&&rollout?.mode==='global'&&
    rollout.phase==='restarting'&&rollout.token===input.token&&rollout.candidate===input.candidate&&
    rollout.digest===input.digest&&rollout.identity===workerReleaseIdentity(input),'MODEL_POLICY_DRAIN_OWNER');
  const guards=(await db.query(`SELECT count(*)::int n FROM pg_trigger
    WHERE tgname='agentos_worker_rollout_admission' AND tgenabled IN('O','A')
      AND tgrelid=ANY(ARRAY['runs'::regclass,'cycle_attempts'::regclass,'planning_runs'::regclass])
      AND obj_description(tgfoid,'pg_proc')='agentos:phased-rollout:v4'`)).rows[0];
  requireValue(guards?.n===3,'MODEL_POLICY_ADMISSION_REQUIRED');
}

/** Operator CLI only. The host observer is trusted code from this exact release,
 * never an RPC/prompt-supplied receipt. No file writes, DDL, restart or resume. */
export async function installOperatorModelPolicy(raw:unknown,observe:()=>Promise<PolicyInstallationObservation>){
  const input=modelPolicyInstallationInput.parse(raw),before=await observe();observation(before,input);
  const policy=before.policy,policyDigest=modelPolicyDigest(policy),operationDigest=modelPolicyDigest(input);
  return tx(async db=>{
    await db.query("SET LOCAL lock_timeout='2000ms'");await db.query("SET LOCAL statement_timeout='8000ms'");
    await lock(db);await owner(db,input);await requireSchemaReady(db);await requireBacklogSchemaReady(db);await modelPolicyQuiet(db);
    const key='model-policy-install:'+input.token;
    const prior=(await db.query('SELECT value FROM settings WHERE id=$1 FOR UPDATE',[key])).rows[0]?.value;
    const current=(await db.query("SELECT value FROM settings WHERE id='model-policy' FOR UPDATE")).rows[0]?.value??null;
    if(prior){
      requireValue(prior.version===1&&prior.kind==='model_policy_installation'&&prior.operationDigest===operationDigest&&
        prior.policyDigest===policyDigest&&prior.policyFileDigest===input.expectedPolicyFileDigest&&
        prior.previousPolicyDigest===input.expectedPreviousPolicyDigest&&prior.token===input.token&&
        prior.candidate===input.candidate&&prior.digest===input.digest&&prior.identity===workerReleaseIdentity(input)&&
        prior.accountId===policy.accountId&&prior.bridgeId===policy.bridgeId&&prior.model===policy.model&&
        prior.requestLimit===6&&prior.sessionLimit===12&&Number.isFinite(Date.parse(prior.installedAt))&&
        current!==null&&modelPolicyDigest(current)===policyDigest,'MODEL_POLICY_INSTALL_CONFLICT');
      requireValue((await db.query("SELECT 1 FROM events WHERE kind='MODEL_POLICY_INSTALLED' AND payload=$1::jsonb",[JSON.stringify(prior)])).rowCount===1,
        'MODEL_POLICY_INSTALL_CONFLICT');
    }else requireValue((current===null?null:modelPolicyDigest(current))===input.expectedPreviousPolicyDigest,'MODEL_POLICY_PREVIOUS_CHANGED');
    const account=(await db.query(`SELECT id,bridge_id,enabled,models,max_concurrency,connection_status FROM accounts
      WHERE id=$1 FOR SHARE`,[policy.accountId])).rows[0];
    requireValue(account?.enabled===true&&account.bridge_id===policy.bridgeId&&Array.isArray(account.models)&&
      account.models.includes(policy.model)&&account.max_concurrency===policy.sessionLimit&&
      account.connection_status==='connected','MODEL_POLICY_ACCOUNT_UNAVAILABLE');
    // Recapture while owning admission. Hashing must not turn an old physical
    // observation into fresh authority; the host timestamps the beginning.
    const final=await observe();observation(final,input);
    requireValue(final.fileIdentity===before.fileIdentity&&modelPolicyDigest(final.policy)===policyDigest,'MODEL_POLICY_FILE_CHANGED');
    await owner(db,input);const counts=await modelPolicyQuiet(db);
    if(prior)return {...prior,alreadyApplied:true};
    const receipt={version:1,kind:'model_policy_installation',operationDigest,policyDigest,
      policyFileDigest:final.fileDigest,previousPolicyDigest:input.expectedPreviousPolicyDigest,
      token:input.token,candidate:input.candidate,digest:input.digest,identity:workerReleaseIdentity(input),
      accountId:policy.accountId,bridgeId:policy.bridgeId,model:policy.model,requestLimit:6,sessionLimit:12,
      checkedAt:final.checkedAt,installedAt:new Date().toISOString(),counts};
    await db.query("INSERT INTO settings(id,value) VALUES('model-policy',$1) ON CONFLICT(id) DO UPDATE SET value=excluded.value",[JSON.stringify(policy)]);
    await db.query('INSERT INTO settings(id,value) VALUES($1,$2)',[key,JSON.stringify(receipt)]);
    await event(db,'MODEL_POLICY_INSTALLED',null,receipt);
    const committedObservation=await observe();observation(committedObservation,input);
    requireValue(committedObservation.fileIdentity===before.fileIdentity&&
      modelPolicyDigest(committedObservation.policy)===policyDigest,'MODEL_POLICY_FILE_CHANGED');
    return {...receipt,alreadyApplied:false};
  });
}
