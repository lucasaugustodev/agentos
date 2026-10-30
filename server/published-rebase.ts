import { createHash, randomUUID } from 'node:crypto';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { z } from 'zod';
import { config } from './config.js';
import { event, lock, pool, tx, type DB } from './db.js';
import { assertStopped, inspectStopped } from './continuation.js';
import { readArtifact } from './service.js';
import { runtimeCandidateDigest } from './operation-progress.js';
import { acceptedSnapshotProof } from './deferred-handoffs.js';
import { frameworkProjectAuthorization } from './candidate-validation.js';
import { verifyAcceptedManagementAction } from './candidate-validation.js';

const sha = (v: string | Buffer) => createHash('sha256').update(v).digest('hex');
const canonical = (v: any): any => Array.isArray(v) ? v.map(canonical) : v && typeof v === 'object'
  ? Object.fromEntries(Object.keys(v).sort().map(k => [k, canonical(v[k])])) : v;
const digest = (v: any) => sha(JSON.stringify(canonical(v)));
const fail = (code: string) => Object.assign(new Error(code), { code });

export const publishedRebaseSchema = z.object({
  kind: z.literal('request_rebase'), taskId: z.uuid(), expectedAttempt: z.number().int().positive(),
  runId: z.uuid(), runFence: z.uuid(), contextVersion: z.uuid(), candidateArtifactId: z.uuid(),
  candidateArtifactDigest: z.string().regex(/^[a-f0-9]{64}$/), candidateDigest: z.string().regex(/^[a-f0-9]{64}$/),
  baseline: z.object({kind:z.literal('current_published_runtime'),expectedDigest:z.string().regex(/^[a-f0-9]{64}$/).optional()}).strict(),
  reason: z.string().min(20).max(2000),
}).strict();
type Request = z.infer<typeof publishedRebaseSchema>;

export async function inspectPublishedBaseline() {
  const script=fileURLToPath(new URL('../scripts/agentos-published-rebase.py',import.meta.url)).replaceAll('\\','/');
  if(!/^\/opt\/agentos\/releases\/managed-[0-9a-f-]{36}\/scripts\/agentos-published-rebase\.py$/.test(script)) throw fail('REBASE_BROKER_UNDEPLOYED');
  return new Promise<any>((resolve,reject)=>{
    const child=spawn('ssh',['-T','-o','BatchMode=yes','-o','ConnectTimeout=8',config.bridgeHost,'/usr/bin/python3',script,'inspect_baseline'],
      {windowsHide:true,stdio:['ignore','pipe','pipe']});
    let out='';const timer=setTimeout(()=>child.kill('SIGTERM'),45000);
    child.stdout.on('data',(b)=>{out+=b.toString();if(out.length>2*1024*1024)child.kill();});child.stderr.resume();
    child.once('error',()=>{clearTimeout(timer);reject(fail('REBASE_PUBLISHED_BASE_UNAVAILABLE'));});
    child.once('close',(code)=>{clearTimeout(timer);try{const value=JSON.parse(out);if(code!==0)throw fail(value?.error??'REBASE_PUBLISHED_BASE_UNAVAILABLE');resolve(value);}catch(e:any){reject(/^REBASE_[A-Z_]+$/.test(e?.message??'')?e:fail('REBASE_PUBLISHED_BASE_UNAVAILABLE'));}});
  });
}

/** Shared validation of the protected publication broker's response. */
export function validatePublishedBaseline(value:any){
  const b=value?.baseline;
  if(!b||b.version!==1||!/^\/opt\/agentos\/releases\/managed-[0-9a-f-]{36}$/.test(b.release??'')||
    !/^[a-f0-9]{64}$/.test(b.releaseDigest??'')||!/^[a-f0-9]{64}$/.test(b.sourceManifestDigest??'')||
    b.sourceWorkspace!=='agentos/src'+b.releaseDigest.slice(0,16)||
    !/^\/var\/lib\/agentos-build\//.test(b.publicationProofRef??'')||!/^\/var\/lib\/agentos-build\//.test(b.validationProofRef??'')||
    !/^[a-f0-9]{64}$/.test(b.publicationProofDigest??'')||!/^[a-f0-9]{64}$/.test(b.validationProofDigest??'')||
    !/^\/var\/lib\/agentos\/releases\//.test(b.registeredReceiptRef??'')||
    !/^[a-f0-9]{64}$/.test(b.registeredReceiptDigest??'')||!/^[a-f0-9]{64}$/.test(b.registeredActionDigest??'')||
    !b.sourceManifestHashes||digest(b.sourceManifestHashes)!==b.sourceManifestDigest||
    !b.generatedReleaseHashes||Object.entries(b.generatedReleaseHashes).some(([name,hash]:any)=>!name.startsWith('dist/')||!/^[a-f0-9]{64}$/.test(hash))||
    digest({...b.sourceManifestHashes,...b.generatedReleaseHashes})!==b.workspaceManifestDigest)
    throw fail('REBASE_PUBLISHED_BASE_UNAVAILABLE');
  return b;
}

export async function installPublishedRebase(db: DB = pool) {
  await db.query(`CREATE OR REPLACE FUNCTION agentos_published_rebase_owned(target uuid) RETURNS boolean
      LANGUAGE sql STABLE AS $$ SELECT EXISTS(SELECT 1 FROM settings s WHERE s.id LIKE 'published-rebase:%'
        AND s.value->>'taskId'=target::text AND s.value->>'state' IN('proposed','preparing','prepared','applying','uncertain')) $$;
    CREATE OR REPLACE FUNCTION agentos_published_rebase_guard() RETURNS trigger LANGUAGE plpgsql AS $$
    DECLARE authority jsonb; action_id text; expected jsonb;
    BEGIN
      PERFORM pg_advisory_xact_lock(4280001);
      IF TG_TABLE_NAME='runs' THEN
        IF agentos_published_rebase_owned(NEW.task_id) THEN RAISE EXCEPTION 'PUBLISHED_REBASE_OWNS_TASK'; END IF;
        RETURN NEW;
      ELSIF TG_TABLE_NAME='dependencies' THEN
        IF (TG_OP<>'DELETE' AND agentos_published_rebase_owned(NEW.task_id)) OR
          (TG_OP<>'INSERT' AND agentos_published_rebase_owned(OLD.task_id)) OR
          (TG_OP<>'DELETE' AND agentos_published_rebase_owned(NEW.depends_on)) OR
          (TG_OP<>'INSERT' AND agentos_published_rebase_owned(OLD.depends_on)) THEN RAISE EXCEPTION 'PUBLISHED_REBASE_OWNS_DEPENDENCIES'; END IF;
        IF TG_OP='DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;
      END IF;
      IF NOT agentos_published_rebase_owned(OLD.id) THEN RETURN NEW; END IF;
      SELECT value INTO authority FROM settings WHERE id LIKE 'published-rebase:%' AND value->>'taskId'=OLD.id::text
        AND value->>'state'='applying' FOR SHARE;
      IF authority IS NULL THEN RAISE EXCEPTION 'PUBLISHED_REBASE_OWNS_TASK'; END IF;
      action_id:=authority->>'actionId';
      expected:=jsonb_set(jsonb_set(OLD.execution-'continuation','{workspace}',to_jsonb(authority->>'targetWorkspace')),
        '{sourceWorkspace}',to_jsonb(authority->>'baselineWorkspace')) ||
        jsonb_build_object('publishedRebase',jsonb_build_object('version',1,'actionId',action_id,
          'requestDigest',authority->>'requestDigest','publishedBaselineDigest',authority->>'publishedBaselineDigest',
          'sourceManifestDigest',authority->>'sourceManifestDigest','workspaceManifestDigest',authority->>'workspaceManifestDigest',
          'preparedReceiptDigest',authority#>>'{prepared,receiptDigest}',
          'priorWorkspace',OLD.execution->>'workspace','baselineWorkspace',authority->>'baselineWorkspace',
          'workspace',authority->>'targetWorkspace'));
      IF NEW.goal_id<>OLD.goal_id OR NEW.description<>OLD.description OR NEW.criteria<>OLD.criteria OR
        NEW.agent_id<>OLD.agent_id OR NEW.resources<>OLD.resources OR NEW.required_memories<>OLD.required_memories OR
        NEW.allowed_accounts<>OLD.allowed_accounts OR NEW.execution-'continuation'<>expected OR
        (to_jsonb(NEW)-ARRAY['execution','state','reason','next_attempt_at','max_attempts','updated_at']) IS DISTINCT FROM
          (to_jsonb(OLD)-ARRAY['execution','state','reason','next_attempt_at','max_attempts','updated_at']) OR
        NEW.state IS DISTINCT FROM 'ready' OR NEW.reason IS DISTINCT FROM 'Rebase isolado preparado a partir da publicação verificada; critérios e dependências preservados.' OR
        NEW.next_attempt_at IS NOT NULL OR NEW.attempt_count<>OLD.attempt_count OR
        NEW.max_attempts<>GREATEST(OLD.max_attempts,OLD.attempt_count+1) OR OLD.execution->>'workspace'<>authority->>'currentWorkspace' OR
        authority->>'targetWorkspace'!~'^[a-z0-9_-]+/o[a-z0-9_-]+$' THEN
        RAISE EXCEPTION 'PUBLISHED_REBASE_CONTRACT_CHANGED';
      END IF;
      RETURN NEW;
    END $$;
    CREATE OR REPLACE FUNCTION agentos_published_rebase_settings_guard() RETURNS trigger LANGUAGE plpgsql AS $$
    DECLARE before_value jsonb; after_value jsonb; old_state text; new_state text;
    BEGIN
      IF TG_OP='DELETE' THEN
        IF OLD.id ~ '^published-rebase:[0-9a-f-]{36}$' THEN RAISE EXCEPTION 'PUBLISHED_REBASE_RECEIPT_IMMUTABLE'; END IF;
        RETURN OLD;
      END IF;
      IF TG_OP='UPDATE' AND OLD.id ~ '^published-rebase:[0-9a-f-]{36}$' AND NEW.id<>OLD.id THEN
        RAISE EXCEPTION 'PUBLISHED_REBASE_RECEIPT_IMMUTABLE';
      END IF;
      IF NEW.id !~ '^published-rebase:[0-9a-f-]{36}$' THEN RETURN NEW; END IF;
      IF TG_OP='INSERT' THEN
        IF NEW.id<>'published-rebase:'||(NEW.value->>'actionId') OR NEW.value->>'state'<>'proposed' OR
          NEW.value->>'actionId'!~'^[0-9a-f-]{36}$' OR NOT (NEW.value ? 'requestDigest') THEN
          RAISE EXCEPTION 'PUBLISHED_REBASE_RECEIPT_INVALID';
        END IF;
        RETURN NEW;
      END IF;
      before_value:=OLD.value;after_value:=NEW.value;old_state:=before_value->>'state';new_state:=after_value->>'state';
      IF (before_value-'state'-'startedAt'-'prepared'-'preparedAt'-'errorCode'-'updatedAt'-'appliedAt') IS DISTINCT FROM
         (after_value-'state'-'startedAt'-'prepared'-'preparedAt'-'errorCode'-'updatedAt'-'appliedAt') THEN
        RAISE EXCEPTION 'PUBLISHED_REBASE_RECEIPT_IMMUTABLE';
      END IF;
      IF old_state='prepared' AND before_value->'prepared' IS DISTINCT FROM after_value->'prepared' THEN
        RAISE EXCEPTION 'PUBLISHED_REBASE_PREPARED_RECEIPT_IMMUTABLE';
      END IF;
      IF NOT ((old_state='proposed' AND new_state IN('preparing','failed')) OR
        (old_state='preparing' AND new_state IN('prepared','uncertain','failed')) OR
        (old_state='prepared' AND new_state IN('applying','uncertain','failed')) OR
        (old_state='uncertain' AND new_state='prepared') OR
        (old_state='applying' AND new_state='applied')) THEN RAISE EXCEPTION 'PUBLISHED_REBASE_STATE_INVALID'; END IF;
      RETURN NEW;
    END $$;
    DROP TRIGGER IF EXISTS agentos_published_rebase_settings_guard ON settings;
    CREATE TRIGGER agentos_published_rebase_settings_guard BEFORE INSERT OR UPDATE OR DELETE ON settings FOR EACH ROW EXECUTE FUNCTION agentos_published_rebase_settings_guard();
    DROP TRIGGER IF EXISTS a_agentos_published_rebase_guard ON tasks;
    CREATE TRIGGER a_agentos_published_rebase_guard BEFORE UPDATE ON tasks FOR EACH ROW EXECUTE FUNCTION agentos_published_rebase_guard();
    DROP TRIGGER IF EXISTS a_agentos_published_rebase_guard ON runs;
    CREATE TRIGGER a_agentos_published_rebase_guard BEFORE INSERT ON runs FOR EACH ROW EXECUTE FUNCTION agentos_published_rebase_guard();
    DROP TRIGGER IF EXISTS a_agentos_published_rebase_guard ON dependencies;
    CREATE TRIGGER a_agentos_published_rebase_guard BEFORE INSERT OR UPDATE OR DELETE ON dependencies FOR EACH ROW EXECUTE FUNCTION agentos_published_rebase_guard();`);
}

async function currentDependencies(db: DB, task: any) {
  const rows = (await db.query('SELECT depends_on FROM dependencies WHERE task_id=$1 ORDER BY depends_on', [task.id])).rows;
  const proofs = [];
  for (const row of rows) {
    const source = (await db.query('SELECT * FROM tasks WHERE id=$1 AND goal_id=$2', [row.depends_on, task.goal_id])).rows[0];
    if (!source) throw fail('REBASE_DEPENDENCIES_CHANGED');
    proofs.push(await acceptedSnapshotProof(db, source));
  }
  return proofs;
}

async function stateBinding(db: DB, goalId: string, raw: Request) {
  const authorization = await frameworkProjectAuthorization(db, goalId);
  const task = (await db.query('SELECT * FROM tasks WHERE id=$1 AND goal_id=$2', [raw.taskId, goalId])).rows[0];
  const run = task && (await db.query('SELECT * FROM runs WHERE task_id=$1 ORDER BY attempt DESC LIMIT 1', [task.id])).rows[0];
  if (!task || !run || task.goal_id !== goalId || !['submitted','failed'].includes(run.state) ||
    ['accepted','cancelled','superseded','paused','queued','running','cancelling','reconciling'].includes(task.state) ||
    run.id !== raw.runId || run.fence !== raw.runFence || run.context_id !== raw.contextVersion ||
    run.attempt !== raw.expectedAttempt || task.attempt_count !== raw.expectedAttempt ||
    task.stage !== 'integrate' || task.execution?.kind !== 'harness' ||
    !/^[a-z0-9_-]+\/o[a-z0-9_-]+$/.test(task.execution?.workspace ?? '') ||
    task.execution.workspace.split('/')[0] !== authorization.project || run.instruction_snapshot?.task?.execution?.workspace !== task.execution.workspace)
    throw fail('REBASE_BINDING_STALE');
  if((await db.query("SELECT 1 FROM settings WHERE id LIKE 'candidate-validation:%' AND value->>'taskId'=$1 AND value->>'state' IN('proposed','freezing','frozen','running','uncertain')",[task.id])).rowCount)
    throw fail('REBASE_VALIDATION_OWNS_TASK');
  const dependencies = await currentDependencies(db, task);
  return { task, run, authorization, dependencies };
}

async function candidate(db: DB, req: Request, proof: any) {
  const refs = (await db.query("SELECT id,digest,size FROM artifacts WHERE run_id=$1 AND name='candidate.json' ORDER BY id LIMIT 2", [req.runId])).rows;
  if (refs.length !== 1 || refs[0].id !== req.candidateArtifactId || refs[0].digest !== req.candidateArtifactDigest) throw fail('REBASE_CANDIDATE_CHANGED');
  const artifact = await readArtifact(refs[0].id);
  if (artifact.bytes.length !== refs[0].size || sha(artifact.bytes) !== refs[0].digest ||
    runtimeCandidateDigest(artifact.bytes, refs[0].digest) !== req.candidateDigest || proof?.candidate?.candidateDigest !== req.candidateDigest)
    throw fail('REBASE_CANDIDATE_CHANGED');
  return { candidateSize: artifact.bytes.length, candidateArtifactSize: refs[0].size };
}

function requestDigest(value: any) { const copy = { ...value }; delete copy.requestDigest; delete copy.state;
  for(const key of ['startedAt','prepared','preparedAt','errorCode','updatedAt','appliedAt'])delete copy[key]; return digest(copy); }

async function acceptedManager(db: DB, goalId: string, managerRunId: string, index: number, raw: Request) {
  const verified = await verifyAcceptedManagementAction(db, goalId, managerRunId, index, raw);
  if (!verified || verified.actionDigest !== digest(raw)) throw fail('REBASE_MANAGER_ACTION_UNVERIFIED');
  return verified;
}

export async function requestPublishedRebase(goalId: string, managerRunId: string, index: number, raw: unknown,
  inspect = inspectStopped) {
  const parsed = publishedRebaseSchema.safeParse(raw);
  let proof: any, snapshot: any, baseline: any;
  if (parsed.success) {
    try {
      snapshot = await stateBinding(pool, goalId, parsed.data);
      proof = await inspect(snapshot.run.id, snapshot.task.execution?.kind === 'opencode');
      baseline = await inspectPublishedBaseline();
    } catch { /* exact refusal is recorded below */ }
  }
  return tx(async db => {
    await lock(db);
    const prior = (await db.query('SELECT receipt FROM management_actions WHERE run_id=$1 AND action_index=$2', [managerRunId,index])).rows[0];
    if (prior) return prior.receipt;
    let code='REBASE_FORMAT', actionId:string|null=null;
    try {
      if (!parsed.success || !Number.isSafeInteger(index) || index<0) throw fail(code);
      const req=parsed.data, current=await stateBinding(db,goalId,req);
      assertStopped(proof,current.run,current.task);
      const managerProof=await acceptedManager(db,goalId,managerRunId,index,req);
      const candidateProof=await candidate(db,req,proof);
      const criteriaDigest=digest(current.task.criteria), dependencyDigest=digest(current.dependencies);
      const {project,authorization}=current.authorization;
      const pinnedBaseline=validatePublishedBaseline(baseline);
      if(req.baseline.expectedDigest&&req.baseline.expectedDigest!==pinnedBaseline.releaseDigest)throw fail('REBASE_PUBLISHED_BASE_CHANGED');
      if(current.task.execution?.sourceWorkspace===pinnedBaseline.sourceWorkspace ||
        current.task.execution?.publishedRebase?.publishedBaselineDigest===pinnedBaseline.releaseDigest)
        throw fail('REBASE_ALREADY_CURRENT');
      actionId=randomUUID();
      const request:any={version:1,actionId,managerRunId,managerActionIndex:index,managerAction:req,managerProof,
        goalId,taskId:req.taskId,project,frameworkAuthorization:authorization,expectedAttempt:req.expectedAttempt,
        runId:req.runId,runFence:req.runFence,contextVersion:req.contextVersion,
        candidateArtifactId:req.candidateArtifactId,candidateArtifactDigest:req.candidateArtifactDigest,
        candidateDigest:req.candidateDigest,
        criteriaDigest,dependencyManifest:current.dependencies,dependencyIds:current.dependencies.map((d:any)=>d.taskId),dependencyDigest,
        currentWorkspace:current.task.execution.workspace,priorSourceWorkspace:current.task.execution.sourceWorkspace,
        maxAttemptsBefore:current.task.max_attempts,
        sourceWorkspace:pinnedBaseline.sourceWorkspace,publishedBaseline:pinnedBaseline,
        workspaceManifestDigest:pinnedBaseline.workspaceManifestDigest,
        taskState:current.task.state,
        baseWorkspace:current.task.execution.baseWorkspace,workKind:current.task.execution.workKind,
        publishedBaselineRelease:pinnedBaseline.release,publishedBaselineDigest:pinnedBaseline.releaseDigest,
        sourceManifestDigest:pinnedBaseline.sourceManifestDigest,publicationProofRef:pinnedBaseline.publicationProofRef,
        publicationProofDigest:pinnedBaseline.publicationProofDigest,validationProofRef:pinnedBaseline.validationProofRef,
        validationProofDigest:pinnedBaseline.validationProofDigest,reason:req.reason,runState:current.run.state,
        stopProofDigest:digest(proof),candidateArtifactSize:candidateProof.candidateArtifactSize,state:'proposed'};
      request.baselineWorkspace=project+'/pub'+pinnedBaseline.releaseDigest;
      request.targetWorkspace=project+'/o-rb-'+actionId.replaceAll('-','');
      request.requestDigest=requestDigest(request);
      const priorJob=(await db.query("SELECT value FROM settings WHERE id LIKE 'published-rebase:%' AND value->>'taskId'=$1 AND value->>'runId'=$2 AND value->>'candidateDigest'=$3 AND value->>'publishedBaselineDigest'=$4 ORDER BY id LIMIT 1",[req.taskId,req.runId,req.candidateDigest,pinnedBaseline.releaseDigest])).rows[0]?.value;
      if(priorJob){actionId=priorJob.actionId;code='REBASE_ALREADY_REQUESTED';}
      else {
        await db.query('INSERT INTO settings(id,value) VALUES($1,$2)', ['published-rebase:'+actionId,JSON.stringify(request)]);
        code='REBASE_PROPOSED';
      }
    } catch(error:any) { const message=error?.message??'';code=/^REBASE_[A-Z_]+$/.test(message)?message:
      message==='STOP_UNCONFIRMED'?'REBASE_STOP_UNCONFIRMED':
      message==='VALIDATION_PROJECT_UNREGISTERED'?'REBASE_PROJECT_UNREGISTERED':
      message.startsWith('VALIDATION_')?'REBASE_MANAGER_ACTION_UNVERIFIED':'REBASE_REJECTED'; }
    const receipt={kind:'request_rebase',index,actionId,taskId:parsed.success?parsed.data.taskId:null,code,acceptance:false};
    await db.query('INSERT INTO management_actions(run_id,action_index,receipt) VALUES($1,$2,$3)',[managerRunId,index,JSON.stringify(receipt)]);
    await event(db,'MANAGEMENT_ACTION',goalId,{runId:managerRunId,...receipt}); return receipt;
  });
}

async function dispatch(actionId:string, mode:'prepare'|'receipt'='prepare') {
  if(!/^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(actionId)) throw fail('REBASE_ID_INVALID');
  const script=fileURLToPath(new URL('../scripts/agentos-published-rebase.py',import.meta.url)).replaceAll('\\','/');
  if(!/^\/opt\/agentos\/releases\/managed-[0-9a-f-]{36}\/scripts\/agentos-published-rebase\.py$/.test(script)) throw fail('REBASE_BROKER_UNDEPLOYED');
  return new Promise<any>((resolve,reject)=>{
    const child=spawn('ssh',['-T','-o','BatchMode=yes','-o','ConnectTimeout=8',config.bridgeHost,'/usr/bin/python3',script,mode,actionId],{windowsHide:true,stdio:['ignore','pipe','pipe']});
    let out='';const timer=setTimeout(()=>child.kill('SIGTERM'),180000);
    child.stdout.on('data',(b)=>{out+=b.toString();if(out.length>64*1024)child.kill();});child.stderr.resume();
    child.once('error',()=>{clearTimeout(timer);reject(fail('REBASE_TRANSPORT_UNCERTAIN'));});
    child.once('close',(code)=>{clearTimeout(timer);try{const value=JSON.parse(out);if(code!==0)throw fail(value?.error??'REBASE_TRANSPORT_UNCERTAIN');resolve(value);}catch(e:any){reject(/^REBASE_[A-Z_]+$/.test(e?.message??'')?e:fail('REBASE_TRANSPORT_UNCERTAIN'));}});
  });
}

async function applyPrepared(actionId:string, prepared:any, inspect=inspectStopped) {
  const initial=(await pool.query("SELECT value FROM settings WHERE id=$1 AND value->>'state'='prepared'",['published-rebase:'+actionId])).rows[0]?.value;
  if(!initial)return false;
  const latestBaseline=await inspectPublishedBaseline();
  if(digest(latestBaseline?.baseline)!==digest(initial.publishedBaseline))throw fail('REBASE_PUBLISHED_BASE_CHANGED');
  const proof=await inspect(initial.runId,false);
  return tx(async db=>{
    await lock(db);
    const row=(await db.query('SELECT value FROM settings WHERE id=$1 FOR UPDATE',['published-rebase:'+actionId])).rows[0]?.value;
    if(!row||row.state!=='prepared'||row.requestDigest!==initial.requestDigest||prepared?.requestDigest!==row.requestDigest||prepared?.state!=='prepared'||
      prepared?.project!==row.project||prepared?.goalId!==row.goalId||prepared?.taskId!==row.taskId||prepared?.runId!==row.runId||
      prepared?.runFence!==row.runFence||prepared?.targetWorkspace!==row.targetWorkspace||prepared?.baselineWorkspace!==row.baselineWorkspace||
      prepared?.publishedBaselineDigest!==row.publishedBaselineDigest||prepared?.sourceManifestDigest!==row.sourceManifestDigest||
      prepared?.workspaceManifestDigest!==row.workspaceManifestDigest||
      prepared?.sourceChanged!==true||!/^[a-f0-9]{64}$/.test(prepared?.priorSourceManifestDigest??'')||
      !/^[a-f0-9]{64}$/.test(prepared?.priorWorkspaceManifestDigest??'')||
      !/^[a-f0-9]{64}$/.test(prepared?.sourceChangeDigest??''))return false;
    const goal=(await db.query('SELECT project,state FROM goals WHERE id=$1',[row.goalId])).rows[0];
    const auth=await frameworkProjectAuthorization(db,row.goalId);
    if(!goal||goal.state!=='active'||goal.project!==row.project||auth.project!==row.project||digest(auth.authorization)!==digest(row.frameworkAuthorization))throw fail('REBASE_AUTHORITY_CHANGED');
    const task=(await db.query('SELECT * FROM tasks WHERE id=$1 AND goal_id=$2 FOR UPDATE',[row.taskId,row.goalId])).rows[0];
    const run=task&&(await db.query('SELECT * FROM runs WHERE task_id=$1 ORDER BY attempt DESC LIMIT 1',[task.id])).rows[0];
    if(!task||!run||run.id!==row.runId||run.fence!==row.runFence||run.context_id!==row.contextVersion||task.state!==row.taskState||
      task.attempt_count!==row.expectedAttempt||run.attempt!==row.expectedAttempt||task.execution?.workspace!==row.currentWorkspace||
      task.max_attempts!==row.maxAttemptsBefore||
      run.state!==row.runState||digest(task.criteria)!==row.criteriaDigest)throw fail('REBASE_BINDING_STALE');
    assertStopped(proof,run,task);
    const accepted=await acceptedManager(db,row.goalId,row.managerRunId,row.managerActionIndex,row.managerAction);
    if(digest(accepted)!==digest(row.managerProof))throw fail('REBASE_MANAGER_ACTION_CHANGED');
    const deps=await currentDependencies(db,task);if(digest(deps)!==row.dependencyDigest)throw fail('REBASE_DEPENDENCIES_CHANGED');
    const candidateProof=await candidate(db,{runId:row.runId,candidateArtifactId:row.candidateArtifactId,
      candidateArtifactDigest:row.candidateArtifactDigest,candidateDigest:row.candidateDigest} as Request,proof);
    if(candidateProof.candidateArtifactSize!==row.candidateArtifactSize)throw fail('REBASE_CANDIDATE_CHANGED');
    if(!row.prepared||row.prepared.receiptDigest!==digest(prepared))throw fail('REBASE_PREPARED_RECEIPT_CHANGED');
    const state={...row,state:'applying'};
    await db.query('UPDATE settings SET value=$2 WHERE id=$1',['published-rebase:'+actionId,JSON.stringify(state)]);
    const execution={...task.execution,workspace:row.targetWorkspace,sourceWorkspace:row.baselineWorkspace,
      publishedRebase:{version:1,actionId,requestDigest:row.requestDigest,publishedBaselineDigest:row.publishedBaselineDigest,
        sourceManifestDigest:row.sourceManifestDigest,workspaceManifestDigest:row.workspaceManifestDigest,
        preparedReceiptDigest:row.prepared.receiptDigest,priorWorkspace:row.currentWorkspace,
        baselineWorkspace:row.baselineWorkspace,workspace:row.targetWorkspace}};
    const maxAttemptsAfter=Math.max(task.max_attempts,task.attempt_count+1);
    await db.query("UPDATE tasks SET execution=$2,state='ready',reason='Rebase isolado preparado a partir da publicação verificada; critérios e dependências preservados.',next_attempt_at=NULL,max_attempts=$3,updated_at=now() WHERE id=$1",
      [task.id,JSON.stringify(execution),maxAttemptsAfter]);
    await db.query('UPDATE settings SET value=$2 WHERE id=$1',['published-rebase:'+actionId,JSON.stringify({...state,state:'applied',appliedAt:new Date().toISOString()})]);
    await event(db,'PUBLISHED_REBASE_APPLIED',task.id,{actionId,requestDigest:row.requestDigest,priorWorkspace:row.currentWorkspace,
      baselineWorkspace:row.baselineWorkspace,workspace:row.targetWorkspace,publishedBaselineDigest:row.publishedBaselineDigest,
      workspaceManifestDigest:row.workspaceManifestDigest,candidateDigest:row.candidateDigest,
      criteriaPreserved:true,dependenciesPreserved:true,sourceChanged:prepared.sourceChanged,
      priorSourceManifestDigest:prepared.priorSourceManifestDigest,priorWorkspaceManifestDigest:prepared.priorWorkspaceManifestDigest,
      sourceChangeDigest:prepared.sourceChangeDigest,
      maxAttemptsBefore:task.max_attempts,maxAttemptsAfter,acceptance:false});
    return true;
  });
}

export async function readPublishedRebases(db:DB,goalId:string) {
  return (await db.query("SELECT id,value FROM settings WHERE id LIKE 'published-rebase:%' AND value->>'goalId'=$1 ORDER BY id",[goalId])).rows
    .map(row=>({actionId:row.value.actionId,state:row.value.state,taskId:row.value.taskId,runId:row.value.runId,
      targetWorkspace:row.value.targetWorkspace,code:row.value.code??null}));
}

export async function reconcilePublishedRebases() {
  const rows=(await pool.query("SELECT id,value FROM settings WHERE id LIKE 'published-rebase:%' AND value->>'state' IN('proposed','preparing','prepared','uncertain') ORDER BY id")).rows;
  for(const item of rows){
    const id=item.value.actionId;
    const client=await pool.connect();let owner=false;
    try {
      owner=!!(await client.query('SELECT pg_try_advisory_lock(hashtextextended($1,4280097)) owned',[id])).rows[0]?.owned;
      if(!owner)continue;
      const current=(await client.query('SELECT value FROM settings WHERE id=$1',[item.id])).rows[0]?.value;
      if(!current)continue;
      if(current.state==='preparing'){
        await client.query("UPDATE settings SET value=jsonb_set(value,'{state}','\"uncertain\"'::jsonb) WHERE id=$1 AND value->>'state'='preparing'",[item.id]);
        continue;
      }
      if(current.state==='proposed'){
        const claim=await tx(async db=>{await lock(db);const row=(await db.query('SELECT value FROM settings WHERE id=$1 FOR UPDATE',[item.id])).rows[0]?.value;
          if(!row||row.state!=='proposed')return false;await db.query('UPDATE settings SET value=$2 WHERE id=$1',[item.id,JSON.stringify({...row,state:'preparing',startedAt:new Date().toISOString()})]);return true;});
        if(!claim)continue;
      }
      let remotePrepared=current.state==='prepared',receiptVerified=false;
      try{
        const recovering=current.state==='uncertain'||current.state==='prepared';
        const result=await dispatch(id,recovering?'receipt':'prepare');
        receiptVerified=true;
        if(current.state==='uncertain'){
          const receipt={...result,receiptDigest:digest(result)};
          await tx(async db=>{await lock(db);const latest=(await db.query('SELECT value FROM settings WHERE id=$1 FOR UPDATE',[item.id])).rows[0]?.value;
            if(!latest||latest.state!=='uncertain'||latest.requestDigest!==current.requestDigest)throw fail('REBASE_OWNER_CHANGED');
            await db.query('UPDATE settings SET value=$2 WHERE id=$1',[item.id,JSON.stringify({...latest,state:'prepared',prepared:receipt,preparedAt:new Date().toISOString()})]);});
          remotePrepared=true;
        } else if(current.state==='prepared'&&current.prepared?.receiptDigest!==digest(result)) {
          throw fail('REBASE_PREPARED_RECEIPT_CHANGED');
        } else if(current.state!=='prepared'){
          const receipt={...result,receiptDigest:digest(result)};
          await tx(async db=>{await lock(db);const latest=(await db.query('SELECT value FROM settings WHERE id=$1 FOR UPDATE',[item.id])).rows[0]?.value;
            if(!latest||latest.state!=='preparing'||latest.requestDigest!==current.requestDigest)throw fail('REBASE_OWNER_CHANGED');
            await db.query('UPDATE settings SET value=$2 WHERE id=$1',[item.id,JSON.stringify({...latest,state:'prepared',prepared:receipt,preparedAt:new Date().toISOString()})]);});
          remotePrepared=true;
        }
        await applyPrepared(id,result);
      }catch(error:any){
        const knownNoop=error?.message==='REBASE_ALREADY_CURRENT';
        if(knownNoop){await pool.query("UPDATE settings SET value=jsonb_set(value,'{state}','\"failed\"'::jsonb) WHERE id=$1 AND value->>'state' IN('preparing','prepared')",[item.id]).catch(()=>{});}
        else if(!receiptVerified&&current.state!=='uncertain'){
          await pool.query("UPDATE settings SET value=jsonb_set(value,'{state}','\"uncertain\"'::jsonb) WHERE id=$1 AND value->>'state' IN('preparing','prepared')",[item.id]).catch(()=>{});
        } else if(receiptVerified){
          await pool.query("UPDATE settings SET value=jsonb_set(value,'{state}','\"failed\"'::jsonb) WHERE id=$1 AND value->>'state' IN('preparing','prepared')",[item.id]).catch(()=>{});
        }
      }
    } finally {
      if(owner)await client.query('SELECT pg_advisory_unlock(hashtextextended($1,4280097))',[id]).catch(()=>{});
      client.release();
    }
  }
}
