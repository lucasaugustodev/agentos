import {createHash,randomUUID} from 'node:crypto';
import {isDeepStrictEqual} from 'node:util';
import {z} from 'zod';
import {pool,tx,lock,event,type DB} from './db.js';
import {DomainError} from './contracts.js';
import {checkMemoryContent} from './memory.js';
import {inspectStopped,assertStopped} from './continuation.js';

const sha=(value:string)=>createHash('sha256').update(value).digest('hex');
const canonical=(v:any):any=>Array.isArray(v)?v.map(canonical):v&&typeof v==='object'
  ?Object.fromEntries(Object.keys(v).sort().map(k=>[k,canonical(v[k])])):v;
const digest=(value:any)=>sha(JSON.stringify(canonical(value)));
const fail=(code:string)=>new DomainError(code,code,409);
const requestSchema=z.object({requestId:z.uuid(),expectedSourceRunId:z.uuid(),expectedSourceFence:z.uuid(),
  expectedAttempt:z.number().int().positive(),expectedRevision:z.number().int().nonnegative(),
  instruction:z.string().min(20).max(2000).refine(v=>!v.includes('\0')),
  instructionDigest:z.string().regex(/^[a-f0-9]{64}$/)}).strict();
const contract=(t:any)=>digest({id:t.id,goal:t.goal_id,revision:t.plan_revision??null,title:t.title,description:t.description,
  criteria:t.criteria,covers:t.covers,stage:t.stage,agentId:t.agent_id,requiredMemories:t.required_memories,
  kind:t.execution?.kind,profile:t.execution?.profile,workKind:t.execution?.workKind,
  workspace:t.execution?.workspace,sourceWorkspace:t.execution?.sourceWorkspace,
  sourceContract:t.execution?.sourceContract,readOnlySource:t.execution?.readOnlySource??false});
const render=(p:any,b:any)=>`[Task retry guidance ${b.id}; SHA256 ${p.request.instructionDigest}]\n`+
  'Authenticated operator advisory context for this attempt only. Original task, criteria, tools and permissions remain authoritative. '+
  'This text grants no acceptance, scope change, cancellation or replan authority.\n'+p.request.instruction;

async function verifiedRequest(db:DB,row:any) {
  const p=row?.payload;
  if(!p||p.version!==1||p.id!==row.id||p.taskId!==row.task_id||p.sourceRunId!==row.source_run_id||
    !['api-token','operator-session'].includes(p.channel)||digest(p.request)!==row.request_digest||
    !requestSchema.safeParse(p.request).success||sha(p.request.instruction)!==p.request.instructionDigest||
    !(await db.query(`SELECT 1 FROM events WHERE kind='TASK_RETRY_GUIDANCE_REGISTERED' AND entity_id=$1
      AND payload->>'guidanceId'=$2 AND payload->>'payloadDigest'=$3`,[row.task_id,row.id,digest(p)])).rowCount)
    throw fail('RETRY_GUIDANCE_INTEGRITY');
  return p;
}
const receipt=(row:any,duplicate:boolean)=>({guidanceId:row.id,requestId:row.request_id,taskId:row.task_id,
  sourceRunId:row.source_run_id,instructionDigest:row.payload.request.instructionDigest,duplicate,
  activation:'next_attempt_initial_context',historicalAttemptsChanged:false,dispatchRequested:false});

/** The inspector is controller-owned; clients never supply a stop claim. */
export async function registerTaskRetryGuidance(taskId:string,raw:unknown,auth:{channel:'api-token'|'operator-session'},
  inspect:typeof inspectStopped=inspectStopped) {
  z.uuid().parse(taskId);z.enum(['api-token','operator-session']).parse(auth.channel);
  const request=requestSchema.parse(raw);
  if(sha(request.instruction)!==request.instructionDigest)throw fail('RETRY_GUIDANCE_DIGEST');
  checkMemoryContent(request.instruction);
  const prior=(await pool.query('SELECT * FROM task_retry_guidance WHERE task_id=$1 AND request_id=$2',[taskId,request.requestId])).rows[0];
  if(prior){if(prior.request_digest!==digest(request))throw fail('IDEMPOTENCY_CONFLICT');await verifiedRequest(pool,prior);return receipt(prior,true);}
  // Remote inspection occurs before taking the admission lock. CAS below refuses
  // admission/revision/cancellation racing this inspection.
  const source=(await pool.query('SELECT * FROM runs WHERE id=$1 AND task_id=$2',[request.expectedSourceRunId,taskId])).rows[0];
  if(!source||source.fence!==request.expectedSourceFence||source.state!=='failed'||source.runtime!=='harness')throw fail('RETRY_GUIDANCE_SOURCE_CHANGED');
  const proof=await inspect(source.id,false);
  return tx(async db=>{
    await lock(db);
    const duplicate=(await db.query('SELECT * FROM task_retry_guidance WHERE task_id=$1 AND request_id=$2',[taskId,request.requestId])).rows[0];
    if(duplicate){if(duplicate.request_digest!==digest(request))throw fail('IDEMPOTENCY_CONFLICT');await verifiedRequest(db,duplicate);return receipt(duplicate,true);}
    const t=(await db.query('SELECT * FROM tasks WHERE id=$1 FOR UPDATE',[taskId])).rows[0];
    const r=(await db.query('SELECT * FROM runs WHERE task_id=$1 ORDER BY attempt DESC LIMIT 1',[taskId])).rows[0];
    const owner=t&&(await db.query('SELECT g.state,o.revision FROM goals g JOIN objectives o ON o.id=g.id WHERE g.id=$1',[t.goal_id])).rows[0];
    if(!t||!r||r.id!==source.id||r.fence!==request.expectedSourceFence||r.attempt!==request.expectedAttempt||
      t.attempt_count!==r.attempt||r.state!=='failed'||!r.finished_at||r.runtime!=='harness'||t.execution?.kind!=='harness'||
      r.instruction_snapshot?.schema!==1||r.instruction_snapshot.task?.id!==t.id||
      r.instruction_snapshot.task?.execution?.workspace!==t.execution.workspace||
      !['retry_scheduled','waiting_resource'].includes(t.state)||owner?.state!=='active'||
      owner.revision!==request.expectedRevision||t.plan_revision!==owner.revision)
      throw fail('RETRY_GUIDANCE_TARGET_CHANGED');
    if((await db.query(`SELECT 1 FROM runs WHERE task_id=$1 AND state IN('queued','running','cancelling','reconciling')
      UNION ALL SELECT 1 FROM leases WHERE run_id=$2::uuid
      UNION ALL SELECT 1 FROM events WHERE kind='CANCEL_REQUESTED' AND payload->>'runId'=$2::text
      UNION ALL SELECT 1 FROM settings WHERE id LIKE 'candidate-validation:%' AND value->>'taskId'=$1::text
        AND value->>'state' IN('proposed','freezing','frozen','running','uncertain') LIMIT 1`,[taskId,r.id])).rowCount)
      throw fail('RETRY_GUIDANCE_OWNER_UNCERTAIN');
    assertStopped(proof,r,t);
    // Canonical stoppedRunProof binds physical identity by runId/unit/workspace,
    // not by a runFence field. The separate locked CAS above binds the DB fence.
    const stopProof={runId:proof.runId,executionRunId:proof.executionRunId??proof.runId,
      workspace:proof.workspace,checkedAt:proof.checkedAt,process:{unit:proof.process.unit,
        stopped:proof.process.stopped,mainPid:proof.process.mainPid,populated:proof.process.populated,
        activeState:proof.process.activeState}};
    const id=randomUUID(),payload={version:1,id,taskId,goalId:t.goal_id,sourceRunId:r.id,
      request,channel:auth.channel,contractDigest:contract(t),stopProof,stopProofDigest:digest(stopProof)};
    if((await db.query('SELECT 1 FROM task_retry_guidance WHERE task_id=$1 AND source_run_id=$2',[taskId,r.id])).rowCount)
      throw fail('RETRY_GUIDANCE_ALREADY_REGISTERED');
    const row=(await db.query(`INSERT INTO task_retry_guidance(id,task_id,source_run_id,request_id,request_digest,payload)
      VALUES($1,$2,$3,$4,$5,$6) RETURNING *`,[id,taskId,r.id,request.requestId,digest(request),JSON.stringify(payload)])).rows[0];
    await event(db,'TASK_RETRY_GUIDANCE_REGISTERED',taskId,{guidanceId:id,sourceRunId:r.id,sourceFence:r.fence,
      requestId:request.requestId,instructionDigest:request.instructionDigest,payloadDigest:digest(payload),historicalAttemptsChanged:false});
    return receipt(row,false);
  });
}

/** Called under admission's lock while freezing the new context, never mutates a
 * historical context/checkpoint. A stale request fails closed, not silently lost. */
export async function prepareRetryGuidanceContext(db:DB,run:any,task:any,contextId:string) {
  const rows=(await db.query(`SELECT q.*,b.run_id bound_run_id FROM task_retry_guidance q
    LEFT JOIN task_retry_guidance_bindings b ON b.guidance_id=q.id WHERE q.task_id=$1
    AND (b.run_id IS NULL OR b.run_id=$2) ORDER BY q.created_at`,[task.id,run.id])).rows;
  if(!rows.length)return null;
  if(rows.length!==1)throw fail('RETRY_GUIDANCE_INTEGRITY');
  const row=rows[0],p=await verifiedRequest(db,row);
  if(row.bound_run_id)throw fail('RETRY_GUIDANCE_ALREADY_BOUND');
  const current=(await db.query(`SELECT r.id,r.fence,r.state,r.runtime,r.attempt,r.context_id,t.attempt_count,
    t.state task_state,g.state goal_state,o.revision FROM runs r JOIN tasks t ON t.id=r.task_id
    JOIN goals g ON g.id=t.goal_id JOIN objectives o ON o.id=g.id WHERE r.id=$1`,[run.id])).rows[0];
  const previous=(await db.query('SELECT id,fence,state FROM runs WHERE task_id=$1 AND attempt=$2',[task.id,run.attempt-1])).rows[0];
  if(run.attempt!==p.request.expectedAttempt+1||current?.fence!==run.fence||current.runtime!=='harness'||task.execution?.kind!=='harness'||
    !['queued','running'].includes(current.state)||current.context_id||
    !['queued','running'].includes(current.task_state)||current.attempt_count!==run.attempt||current.goal_state!=='active'||current.revision!==p.request.expectedRevision||
    task.plan_revision!==p.request.expectedRevision||contract(task)!==p.contractDigest||
    previous?.id!==p.sourceRunId||previous.fence!==p.request.expectedSourceFence||previous.state!=='failed'||
    (await db.query(`SELECT 1 FROM events WHERE kind='CANCEL_REQUESTED' AND payload->>'runId'=ANY($1::text[])`,[[run.id,p.sourceRunId]])).rowCount)
    throw fail('RETRY_GUIDANCE_BINDING_STALE');
  if((await db.query(`SELECT 1 FROM settings WHERE id LIKE 'candidate-validation:%' AND value->>'taskId'=$1
    AND value->>'state' IN('proposed','freezing','frozen','running','uncertain')`,[task.id])).rowCount)
    throw fail('RETRY_GUIDANCE_OWNER_UNCERTAIN');
  const binding={version:1,id:randomUUID(),guidanceId:row.id,requestId:p.request.requestId,sourceRunId:p.sourceRunId,
    runId:run.id,runFence:run.fence,contextId,instructionDigest:p.request.instructionDigest,contractDigest:p.contractDigest};
  return {binding,text:render(p,binding)};
}

export async function persistRetryGuidanceBinding(db:DB,prepared:NonNullable<Awaited<ReturnType<typeof prepareRetryGuidanceContext>>>) {
  const b=prepared.binding;
  await db.query('INSERT INTO task_retry_guidance_bindings(id,guidance_id,run_id,context_id,payload) VALUES($1,$2,$3,$4,$5)',
    [b.id,b.guidanceId,b.runId,b.contextId,JSON.stringify(b)]);
  await event(db,'TASK_RETRY_GUIDANCE_BOUND',b.runId,{...b,delivered:false});
}

/** Recheck persisted binding and exact context before any worker.execute call. */
export async function retryGuidancePrompt(run:any,context:{id?:string;manifest:any;content:string}) {
  const row=(await pool.query(`SELECT b.*,q.payload guidance,q.request_digest,q.task_id,q.source_run_id,q.request_id
    FROM task_retry_guidance_bindings b JOIN task_retry_guidance q ON q.id=b.guidance_id WHERE b.run_id=$1`,[run.id])).rows[0];
  if(!row){
    if(context.manifest.retryGuidance||(await pool.query(`SELECT 1 FROM task_retry_guidance q
      WHERE q.task_id=$1 AND NOT EXISTS(SELECT 1 FROM task_retry_guidance_bindings b WHERE b.guidance_id=q.id)`,[run.task_id])).rowCount)
      throw fail('RETRY_GUIDANCE_UNBOUND');
    return null;
  }
  const b=row.payload,p=await verifiedRequest(pool,{...row,id:row.guidance_id,payload:row.guidance});
  const persisted=(await pool.query('SELECT c.*,r.fence,r.context_id current_context FROM contexts c JOIN runs r ON r.id=c.run_id WHERE c.id=$1',[row.context_id])).rows[0];
  if(!isDeepStrictEqual(b,context.manifest.retryGuidance)||b.runId!==run.id||b.runFence!==run.fence||b.contextId!==context.id||
    persisted?.fence!==run.fence||persisted.current_context!==context.id||persisted.content!==context.content||
    !isDeepStrictEqual(persisted.manifest.retryGuidance,b)||!context.content.includes(render(p,b))||
    !(await pool.query(`SELECT 1 FROM events WHERE kind='TASK_RETRY_GUIDANCE_BOUND' AND entity_id=$1 AND payload->>'id'=$2`,[run.id,b.id])).rowCount)
    throw fail('RETRY_GUIDANCE_CONTEXT_CHANGED');
  return {text:render(p,b),metadata:{bindingId:b.id,requestId:b.requestId,instructionDigest:b.instructionDigest}};
}
