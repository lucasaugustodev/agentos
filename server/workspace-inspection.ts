import {createHash} from 'node:crypto';
import {z} from 'zod';
import {accountBridge} from './bridge.js';
import {runtimeCandidateDigest} from './operation-progress.js';

const hash=z.string().regex(/^[0-9a-f]{64}$/);
const uuid=z.string().regex(/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/);
const workspace=z.string().regex(/^[a-z0-9_-]+\/[a-z0-9_-]+$/);
const checkedAt=z.string().refine(value=>Number.isFinite(Date.parse(value)));
const generation=z.object({workspace,sourceWorkspace:workspace,baseDigest:hash,snapshotDigest:hash,sourceSnapshotDigest:hash.nullable()}).strict();
const candidate=z.object({baseDigest:hash,candidateDigest:hash,files:z.array(z.object({path:z.string().min(1).max(500),sha256:hash}).strict()).max(10000)}).strict();
const markerBase64=z.string().min(4).max(Math.ceil(128*1024/3)*4);
const binding=z.object({version:z.literal(1),preparationIdentity:uuid,sourceWorkspace:workspace,targetWorkspace:workspace,
  expectedSourceDigest:hash,expectedSourceSnapshotDigest:hash}).strict();
const receipt=binding.extend({bindingDigest:hash,observedSourceDigest:hash,sourceSnapshotDigest:hash,
  cloneBaseDigest:hash,baselineDigest:hash,createdAt:checkedAt,receiptDigest:hash}).strict();
const marker=z.object({workspace,sourceWorkspace:workspace,baseDigest:hash,createdAt:checkedAt,
  sourceSnapshotDigest:hash.optional(),preparation:z.object({version:z.literal(1),preparationIdentity:uuid,bindingDigest:hash,receiptDigest:hash}).strict().optional()}).strict();
const generationResponse=z.object({version:z.literal(1),workspace,generation,markerBase64,legacyParentGeneration:z.boolean(),candidate,checkedAt}).strict();
const preparationResponse=z.object({version:z.literal(1),exists:z.literal(true),workspace,preparationReceipt:receipt,
  generation,markerBase64,candidate,destinationChanged:z.boolean(),checkedAt}).strict();
const missingPreparation=z.object({version:z.literal(1),exists:z.literal(false),confirmed:z.literal(true),workspace,preparationIdentity:uuid,checkedAt}).strict();
const artifact=z.object({name:z.enum(['candidate.json','changes.patch']),digest:hash,mime:z.enum(['application/json','text/plain'])}).strict();
const sourceResponse=z.object({version:z.literal(1),runId:uuid,workspace,
  state:z.object({runId:uuid,workspace,runtime:z.literal('harness'),state:z.literal('completed'),runFence:uuid,stopped:z.literal(true),artifacts:z.array(artifact).length(2)}).strict(),
  admission:z.object({authority:z.literal('runtime_operation_policy'),goalId:uuid,taskId:uuid,runId:uuid,runFence:uuid,contextVersion:uuid,
    workspace:z.literal('/workspace'),sourceSnapshotDigest:hash,policyDigest:hash}).strict(),
  generation,markerBase64,legacyParentGeneration:z.boolean(),candidate,
  process:z.object({unit:z.string(),checkedAt,stopped:z.literal(true),loadState:z.enum(['loaded','not-found']),
    activeState:z.enum(['inactive','failed']),subState:z.string(),mainPid:z.literal(0),cgroupExists:z.boolean(),populated:z.literal(false)}).strict(),checkedAt}).strict();

export type WorkspaceGenerationInspection=z.infer<typeof generationResponse>;
export type WorkspacePreparationInspection=z.infer<typeof preparationResponse>|z.infer<typeof missingPreparation>;
export type WorkerSourceBindingInspection=z.infer<typeof sourceResponse>;
export type WorkspacePreparationRequest={workspace:string;sourceWorkspace:string;preparationIdentity:string;expectedSourceDigest:string;expectedSourceSnapshotDigest:string};
export type WorkspaceInspectionRequest=(method:string,params:unknown,options?:{timeout?:number;signal?:AbortSignal})=>Promise<unknown>;
const sha=(raw:Buffer|string)=>createHash('sha256').update(raw).digest('hex');
const flatDigest=(value:Record<string,unknown>)=>sha(JSON.stringify(Object.fromEntries(Object.entries(value).sort(([a],[b])=>a<b?-1:a>b?1:0))));
const fail=():never=>{throw Error('WORKSPACE_INSPECTION_RESPONSE_INVALID');};
function parse<T>(schema:z.ZodType<T>,value:unknown):T{const result=schema.safeParse(value);if(!result.success)return fail();return result.data;}
function current(at:string){const age=Date.now()-Date.parse(at);if(!Number.isFinite(age)||age< -5000||age>60000)fail();}
function verifyGeneration(value:{workspace:string;generation:z.infer<typeof generation>;markerBase64:string;candidate:z.infer<typeof candidate>;checkedAt:string}){
  const bytes=Buffer.from(value.markerBase64,'base64');
  if(bytes.length>128*1024||bytes.toString('base64')!==value.markerBase64||!Buffer.from(bytes.toString('utf8')).equals(bytes)||sha(bytes)!==value.generation.snapshotDigest)fail();
  let decoded:unknown;try{decoded=JSON.parse(bytes.toString('utf8'));}catch{return fail();}
  const m=parse(marker,decoded),g=value.generation;
  if(m.workspace!==value.workspace||g.workspace!==value.workspace||m.sourceWorkspace!==g.sourceWorkspace||m.baseDigest!==g.baseDigest||
    (m.sourceSnapshotDigest??null)!==g.sourceSnapshotDigest||value.candidate.baseDigest!==g.baseDigest)fail();
  const encoded=Buffer.from(JSON.stringify(value.candidate));
  if(runtimeCandidateDigest(encoded,sha(encoded))!==value.candidate.candidateDigest)fail();
  current(value.checkedAt);
  return m;
}

export async function inspectWorkspaceGeneration(value:string,request:WorkspaceInspectionRequest=accountBridge.request.bind(accountBridge)):Promise<WorkspaceGenerationInspection>{
  const requested=workspace.parse(value);
  const result=parse(generationResponse,await request('worker.inspectGeneration',{workspace:requested},{timeout:45000}));
  if(result.workspace!==requested||result.legacyParentGeneration!==(result.generation.sourceSnapshotDigest===null))fail();
  verifyGeneration(result);return result;
}

export async function inspectWorkspacePreparation(value:WorkspacePreparationRequest,request:WorkspaceInspectionRequest=accountBridge.request.bind(accountBridge)):Promise<WorkspacePreparationInspection>{
  const requested=z.object({workspace,sourceWorkspace:workspace,preparationIdentity:uuid,expectedSourceDigest:hash,expectedSourceSnapshotDigest:hash}).strict().parse(value);
  const result=parse(z.union([preparationResponse,missingPreparation]),await request('worker.inspectPreparation',requested,{timeout:45000}));
  if(result.workspace!==requested.workspace)fail();
  if(!result.exists){if(result.preparationIdentity!==requested.preparationIdentity)fail();current(result.checkedAt);return result;}
  const r=result.preparationReceipt;
  if(r.targetWorkspace!==requested.workspace||r.sourceWorkspace!==requested.sourceWorkspace||r.preparationIdentity!==requested.preparationIdentity||
    r.expectedSourceDigest!==requested.expectedSourceDigest||r.expectedSourceSnapshotDigest!==requested.expectedSourceSnapshotDigest||
    result.destinationChanged!==(result.candidate.candidateDigest!==requested.expectedSourceDigest))fail();
  const m=verifyGeneration(result),bound={version:1,preparationIdentity:r.preparationIdentity,sourceWorkspace:r.sourceWorkspace,targetWorkspace:r.targetWorkspace,
    expectedSourceDigest:r.expectedSourceDigest,expectedSourceSnapshotDigest:r.expectedSourceSnapshotDigest};
  const {receiptDigest,...body}=r;
  if(r.bindingDigest!==flatDigest(bound)||receiptDigest!==flatDigest(body)||
    [r.observedSourceDigest,r.cloneBaseDigest,r.baselineDigest,result.candidate.baseDigest].some(value=>value!==requested.expectedSourceDigest)||
    r.sourceSnapshotDigest!==requested.expectedSourceSnapshotDigest||result.generation.sourceSnapshotDigest!==requested.expectedSourceSnapshotDigest||
    m.preparation?.preparationIdentity!==requested.preparationIdentity||m.preparation.bindingDigest!==r.bindingDigest||m.preparation.receiptDigest!==receiptDigest)fail();
  return result;
}

export async function inspectWorkerSourceBinding(runId:string,value:string,request:WorkspaceInspectionRequest=accountBridge.request.bind(accountBridge)):Promise<WorkerSourceBindingInspection>{
  const requested=z.object({runId:uuid,workspace}).strict().parse({runId,workspace:value});
  const result=parse(sourceResponse,await request('worker.inspectSourceBinding',requested,{timeout:45000}));
  if(result.runId!==requested.runId||result.workspace!==requested.workspace||result.state.runId!==requested.runId||result.state.workspace!==requested.workspace||
    result.admission.runId!==requested.runId||result.admission.runFence!==result.state.runFence||
    result.admission.sourceSnapshotDigest!==result.generation.snapshotDigest||result.process.unit!==`agentos-run-${requested.runId}`||
    result.process.checkedAt!==result.checkedAt||result.legacyParentGeneration!==(result.generation.sourceSnapshotDigest===null))fail();
  for(const [name,mime] of [['candidate.json','application/json'],['changes.patch','text/plain']])
    if(result.state.artifacts.filter(a=>a.name===name&&a.mime===mime).length!==1)fail();
  verifyGeneration(result);return result;
}
