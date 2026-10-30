import type {DB} from './db.js';
import {DomainError} from './contracts.js';
import {preparationHash} from './source-preparation-contract.js';
import {inspectPlanningSource,inspectWorkerRetrySourceBinding,type WorkspaceInspectionRequest} from './workspace-inspection.js';

const failure=(code:string)=>new DomainError(code,'A origem explícita do planejamento mudou ou ainda possui execução sem encerramento confirmado.');
const same=(a:unknown,b:unknown)=>preparationHash(a)===preparationHash(b);
const projectWorkspace=/^[a-z0-9_-]+\/[a-z0-9_-]+$/;
const live=['queued','running','cancelling','reconciling'];
type Actor={taskId:string;goalId:string;runId:string;fence:string;contextId:string;attempt:number;taskAttempt:number;
  state:string;runtime:string;execution:unknown;frozenExecution:unknown;contextIdentity:unknown};
export type ExplicitSourceCapture={version:1;goalId:string;project:string;revision:number;baseWorkspace:string;sourceWorkspace:string;
  authority:'objective_root'|'published_root'|'task_source';requestDigest:string;actors:Actor[];databaseFingerprint:string};
export type ExplicitSourceProof={version:1;kind:'explicit_planning_source';scope:{goalId:string;project:string;revision:number;baseWorkspace:string};
  selectedSource:{workspace:string;candidateDigest:string;snapshotDigest:string;sourceGenerationKind:'snapshot'|'unmarked'};
  databaseFingerprint:string;proofDigest:string;acceptance:false;finalVerificationRequired:true};

/** Check only affected tasks and actual source owners. An unrelated queued task
 * is legitimate parallel work, not a reason to reject the plan. */
export async function assertPlanningSourcesIdle(db:DB,run:any,sources:string[]){
  const replace=run.feedback?.replaceTaskIds??[];
  if(!Array.isArray(replace)||replace.some((id:any)=>typeof id!=='string'))throw failure('STALE_PLAN');
  const row=(await db.query(`SELECT
    EXISTS(SELECT 1 FROM runs r JOIN tasks t ON t.id=r.task_id LEFT JOIN task_execution_metadata m ON m.task_id=t.id
      WHERE (t.id=ANY($1::uuid[]) OR m.workspace=ANY($2::text[]))
        AND (r.state=ANY($3::text[]) OR EXISTS(SELECT 1 FROM leases l WHERE l.run_id=r.id))) live,
    EXISTS(SELECT 1 FROM leases WHERE resource=ANY($4::text[])) leased,
    EXISTS(SELECT 1 FROM settings s WHERE s.id LIKE 'candidate-validation:%'
      AND s.value->>'state' IN('proposed','freezing','frozen','running','uncertain')
      AND (s.value->>'taskId'=ANY($5::text[]) OR s.value#>>'{request,workspace}'=ANY($2::text[]))) validating,
    EXISTS(SELECT 1 FROM runs r JOIN tasks t ON t.id=r.task_id LEFT JOIN task_execution_metadata m ON m.task_id=t.id
      LEFT JOIN settings h ON h.id='worker-validation-handoff:'||r.id
      WHERE (t.id=ANY($1::uuid[]) OR m.workspace=ANY($2::text[])) AND r.attempt=t.attempt_count
        AND r.state='submitted' AND NULLIF(r.instruction_snapshot->'workerValidation','null'::jsonb) IS NOT NULL
        AND COALESCE(h.value->>'state','pending')='pending') pending`,
    [replace,sources,live,sources.map(s=>'workspace:'+s),replace])).rows[0];
  if(row.live||row.leased||row.validating||row.pending)throw failure('SOURCE_PREPARATION_BRANCH_BUSY');
}

/** Explicit source selection authorizes repair input, never an accepted chain.
 * Initial and registered published roots are distinct from task workspaces;
 * task origins require current immutable admission plus physical stop proof. */
export async function captureExplicitSourceAuthority(db:DB,run:any,base:string):Promise<ExplicitSourceCapture>{
  const source=run.feedback?.sourceOverrides?.[base]??base;
  if(!projectWorkspace.test(base)||!projectWorkspace.test(source)||source.split('/')[0]!==base.split('/')[0])
    throw failure('SOURCE_PREPARATION_ORIGIN_INVALID');
  await assertPlanningSourcesIdle(db,run,[source]);
  const row=(await db.query(`SELECT g.project,o.revision,o.request->'workspaces' workspaces,
    encode(sha256(convert_to(o.request::text,'UTF8')),'hex') request_digest,
    (SELECT value->>'workspace' FROM settings WHERE id='agentos-source-workspace') published,
    NOT EXISTS(SELECT 1 FROM tasks t LEFT JOIN task_execution_metadata m ON m.task_id=t.id
      WHERE m.task_id IS NULL OR m.version<>1 OR m.goal_id<>t.goal_id) metadata_complete,
    EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid='public.tasks'::regclass AND tgname='agentos_execution_metadata_sync'
      AND tgenabled IN('O','A') AND NOT tgisinternal AND tgfoid=to_regprocedure('public.agentos_execution_metadata_sync()')) metadata_trigger,
    (SELECT count(*)::int FROM task_execution_metadata WHERE workspace=$2) actor_count,
    COALESCE((SELECT jsonb_agg(jsonb_build_object('taskId',t.id,'goalId',t.goal_id,'runId',r.id,'fence',r.fence,
      'contextId',r.context_id,'attempt',r.attempt,'taskAttempt',t.attempt_count,'state',r.state,'runtime',r.runtime,
      'execution',jsonb_build_object('kind',t.control->'kind','workspace',t.control->'workspace'),
      'frozenExecution',jsonb_build_object('kind',r.instruction_snapshot#>'{task,execution,kind}',
        'workspace',r.instruction_snapshot#>'{task,execution,workspace}'),
      'contextIdentity',c.manifest->'runIdentity') ORDER BY t.id)
      FROM (SELECT t.id,t.goal_id,t.attempt_count,m.control FROM tasks t JOIN task_execution_metadata m ON m.task_id=t.id
        WHERE m.workspace=$2 ORDER BY t.id LIMIT 65)t
      LEFT JOIN LATERAL(SELECT * FROM runs WHERE task_id=t.id ORDER BY attempt DESC LIMIT 1)r ON true
      LEFT JOIN contexts c ON c.id=r.context_id),'[]'::jsonb) actors
    FROM goals g JOIN objectives o ON o.id=g.id WHERE g.id=$1`,[run.objective_id,source])).rows[0];
  if(!row||!row.metadata_complete||!row.metadata_trigger||row.revision!==run.base_revision||row.project!==base.split('/')[0]||!row.workspaces?.includes(base)||
    row.actor_count>64||row.actor_count!==row.actors.length)throw failure('SOURCE_PREPARATION_AUTHORITY_CHANGED');
  let authority:ExplicitSourceCapture['authority'];
  if(row.actors.length){
    authority='task_source';
    for(const a of row.actors as Actor[]){
      if(a.goalId!==run.objective_id||!a.runId||!a.fence||!a.contextId||a.runtime!=='harness'||
        !['submitted','failed','cancelled'].includes(a.state)||a.attempt!==a.taskAttempt||
        !same(a.execution,{kind:'harness',workspace:source})||!same(a.execution,a.frozenExecution)||
        !same(a.contextIdentity,{runId:a.runId,runFence:a.fence,taskId:a.taskId,goalId:a.goalId}))
        throw failure('SOURCE_PREPARATION_AUTHORITY_CHANGED');
    }
  }else if(source===base)authority='objective_root';
  else if(row.project==='agentos'&&/^agentos\/src[0-9a-f]{16}$/.test(source)&&row.published===source)authority='published_root';
  else throw failure('SOURCE_PREPARATION_ORIGIN_UNREGISTERED');
  const body={version:1 as const,goalId:run.objective_id,project:row.project,revision:run.base_revision,baseWorkspace:base,
    sourceWorkspace:source,authority,requestDigest:row.request_digest,actors:row.actors as Actor[]};
  return {...body,databaseFingerprint:preparationHash(body)};
}

export async function recaptureExplicitSourceAuthority(db:DB,run:any,prior:ExplicitSourceCapture){
  const {databaseFingerprint,...body}=prior;
  if(preparationHash(body)!==databaseFingerprint)throw failure('SOURCE_PREPARATION_AUTHORITY_CHANGED');
  const current=await captureExplicitSourceAuthority(db,run,prior.baseWorkspace);
  if(current.databaseFingerprint!==databaseFingerprint)throw failure('SOURCE_PREPARATION_AUTHORITY_CHANGED');
}

export async function observeExplicitSource(capture:ExplicitSourceCapture,request:WorkspaceInspectionRequest){
  const source=await inspectPlanningSource(capture.sourceWorkspace,capture.authority==='task_source'?'snapshot':'root',request);
  const times=[Date.parse(source.checkedAt)];
  for(const actor of capture.actors){
    const stop=await inspectWorkerRetrySourceBinding(actor.runId,capture.sourceWorkspace,request),a=stop.admission;
    if(a.goalId!==actor.goalId||a.taskId!==actor.taskId||a.runId!==actor.runId||a.runFence!==actor.fence||a.contextVersion!==actor.contextId||
      stop.generation.snapshotDigest!==source.generationDigest||stop.candidate.candidateDigest!==source.sourceDigest)
      throw failure('SOURCE_PREPARATION_AUTHORITY_CHANGED');
    times.push(Date.parse(stop.checkedAt));
  }
  const observationCheckedAt=Math.min(...times),age=Date.now()-observationCheckedAt;
  if(!Number.isFinite(age)||age<0||age>60000)throw failure('SOURCE_PREPARATION_STALE');
  const body={version:1 as const,kind:'explicit_planning_source' as const,
    scope:{goalId:capture.goalId,project:capture.project,revision:capture.revision,baseWorkspace:capture.baseWorkspace},
    selectedSource:{workspace:capture.sourceWorkspace,candidateDigest:source.sourceDigest,snapshotDigest:source.generationDigest,
      sourceGenerationKind:source.sourceGenerationKind},databaseFingerprint:capture.databaseFingerprint,
    acceptance:false as const,finalVerificationRequired:true as const};
  return {proof:{...body,proofDigest:preparationHash(body)} as ExplicitSourceProof,observationCheckedAt};
}
