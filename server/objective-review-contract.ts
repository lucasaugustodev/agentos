import {z} from 'zod';
import {operationDigest} from './operations-contract.js';

const uuid=z.uuid(),digest=z.string().regex(/^[a-f0-9]{64}$/);
export const operatorReviewRequestSchema=z.object({
  requestKey:z.string().regex(/^[A-Za-z0-9][A-Za-z0-9._:-]{7,159}$/),
  taskId:uuid,runId:uuid,runFence:uuid,contextVersion:uuid,
  expectedAttempt:z.number().int().positive(),expectedRevision:z.number().int().nonnegative(),
  observationEventId:z.string().regex(/^[1-9][0-9]{0,18}$/)
    .refine(value=>/^[1-9][0-9]{0,18}$/.test(value)&&BigInt(value)<=9223372036854775807n),
  reason:z.string().trim().min(20).max(1500),
}).strict();
export type OperatorReviewRequest=z.infer<typeof operatorReviewRequestSchema>;
export type OperatorReviewAuth={channel:'api-token'|'operator-session'};
export const operatorReviewRequestDigest=(goalId:string,request:OperatorReviewRequest)=>
  operationDigest({version:1,source:'operator',goalId,request});

export const operatorReviewSignalSchema=z.object({
  version:z.literal(1),source:z.literal('operator'),pattern:z.literal('operator_review'),
  requestKey:operatorReviewRequestSchema.shape.requestKey,requestDigest:digest,
  expectedRevision:z.number().int().nonnegative(),reason:z.string().min(20).max(1500),
  observationEventId:operatorReviewRequestSchema.shape.observationEventId,
  evidenceDigest:digest,materialDigest:digest,lastSequence:z.number().int().min(0).max(2147483647),
}).strict();

/** A bounded persisted snapshot, not a loop classification. The observer only
 * exposes tool returns and metadata, not source bytes or model reasoning.
 * Identical reads, new tokens, call IDs and presence are not material progress.
 * Omitted/truncated text stays explicit evidence with limits, never fabricated. */
export function operatorReviewMaterial(observation:any,now=Date.now()):{digest:string;lastSequence:number}|null{
  if(!observation||typeof observation!=='object'||Array.isArray(observation)||
    !uuid.safeParse(observation.runId).success||typeof observation.observedAt!=='string'||
    !Number.isFinite(Date.parse(observation.observedAt))||observation.unavailable||observation.error)return null;
  const age=now-Date.parse(observation.observedAt);
  if(age< -5000||age>180000)return null;
  for(const key of ['actions','files','evidence'])if(!Array.isArray(observation[key])||observation[key].length>100||
    observation[key].some((value:any)=>!value||typeof value!=='object'||Array.isArray(value)))return null;
  if(observation.actions.some((action:any)=>!Number.isSafeInteger(action.sequence)||action.sequence<0||action.sequence>2147483647))return null;
  if(Buffer.byteLength(JSON.stringify(observation),'utf8')>192*1024)return null;
  const ordered=(values:any[])=>[...new Map(values.map(value=>[operationDigest(value),value])).entries()]
    .sort(([a],[b])=>a.localeCompare(b)).map(([,value])=>value);
  const actions=observation.actions.filter((a:any)=>['returned','error'].includes(a.state))
    .map(({id,sequence,startedAt,finishedAt,...returned}:any)=>returned);
  const material={runId:observation.runId,workspace:observation.workspace,runtimeState:observation.runtimeState,
    traceTruncated:observation.traceTruncated??false,actions:ordered(actions),
    files:ordered(observation.files),evidence:ordered(observation.evidence)};
  return {digest:operationDigest({version:1,material}),lastSequence:Math.max(0,...observation.actions.map((a:any)=>a.sequence))};
}
