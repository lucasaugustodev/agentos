import {createHash} from 'node:crypto';
import {z} from 'zod';

const hash=z.string().regex(/^[a-f0-9]{64}$/),workspace=z.string().regex(/^[a-z0-9_-]+\/[a-z0-9_-]+$/);
const canonical=(v:any):any=>Array.isArray(v)?v.map(canonical):v&&typeof v==='object'
  ?Object.fromEntries(Object.keys(v).sort().map(k=>[k,canonical(v[k])])):v;
export const preparationHash=(v:unknown)=>createHash('sha256').update(JSON.stringify(canonical(v))).digest('hex');
const sha=(b:Buffer)=>createHash('sha256').update(b).digest('hex');
const fail=()=>{throw Object.assign(new Error('SOURCE_PREPARATION_PROOF_INVALID'),{code:'SOURCE_PREPARATION_PROOF_INVALID'});};

export const sourcePreparationRequestSchema=z.object({workspace,sourceWorkspace:workspace,
  preparationIdentity:z.uuid(),expectedSourceDigest:hash,expectedSourceSnapshotDigest:hash}).strict();
export type SourcePreparationRequest=z.infer<typeof sourcePreparationRequestSchema>;
const receiptSchema=z.object({version:z.literal(1),preparationIdentity:z.uuid(),sourceWorkspace:workspace,targetWorkspace:workspace,
  expectedSourceDigest:hash,expectedSourceSnapshotDigest:hash,bindingDigest:hash,observedSourceDigest:hash,
  sourceSnapshotDigest:hash,cloneBaseDigest:hash,baselineDigest:hash,createdAt:z.string().min(1).max(100),receiptDigest:hash}).strict();
const markerSchema=z.object({workspace,sourceWorkspace:workspace,baseDigest:hash,sourceSnapshotDigest:hash,
  createdAt:z.string().min(1).max(100),preparation:z.object({version:z.literal(1),preparationIdentity:z.uuid(),bindingDigest:hash,receiptDigest:hash}).strict()}).strict();
export type SourcePreparationReceipt=z.infer<typeof receiptSchema>;
export type VerifiedSourcePreparation={version:1;request:SourcePreparationRequest;receipt:SourcePreparationReceipt;
  targetSnapshotDigest:string;currentCandidateDigest:string;destinationChanged:boolean;checkedAt:number};

/** Consistency check on a response from the protected controller inspector.
 * Self-consistent JSON from a model is not authority: callers must use the fixed
 * bridge operation, then bind this result to their durable controller intent.
 * The Python manager writes canonical marker bytes. Requiring those exact bytes
 * also rejects duplicate JSON keys without accepting JSON.parse's last value.
 * Receipt alone never proves that a destination/baseline exists.
 */
export function verifySourcePreparation(request:SourcePreparationRequest,inspection:any,
  options:{requireUnchanged:boolean;now?:number}):VerifiedSourcePreparation {
  const parsed=sourcePreparationRequestSchema.safeParse(request),receipt=receiptSchema.safeParse(inspection?.preparationReceipt);
  if(!parsed.success||!receipt.success||request.workspace===request.sourceWorkspace||
    request.workspace.split('/')[0]!==request.sourceWorkspace.split('/')[0])return fail();
  const expected={version:1,preparationIdentity:request.preparationIdentity,sourceWorkspace:request.sourceWorkspace,
    targetWorkspace:request.workspace,expectedSourceDigest:request.expectedSourceDigest,
    expectedSourceSnapshotDigest:request.expectedSourceSnapshotDigest};
  const r=receipt.data,{receiptDigest,...body}=r;
  if(Object.entries(expected).some(([k,v])=>r[k as keyof typeof r]!==v)||r.bindingDigest!==preparationHash(expected)||
    receiptDigest!==preparationHash(body)||r.sourceSnapshotDigest!==request.expectedSourceSnapshotDigest||
    [r.observedSourceDigest,r.cloneBaseDigest,r.baselineDigest].some(d=>d!==request.expectedSourceDigest))return fail();
  if(typeof inspection?.markerBase64!=='string'||inspection.markerBase64.length>175000)return fail();
  const raw=Buffer.from(inspection.markerBase64,'base64');
  if(raw.length>128*1024||raw.toString('base64')!==inspection.markerBase64)return fail();
  let marker:unknown;try{marker=JSON.parse(raw.toString('utf8'));}catch{return fail();}
  const m=markerSchema.safeParse(marker);
  if(!m.success||!Buffer.from(JSON.stringify(canonical(m.data))).equals(raw)||
    m.data.workspace!==request.workspace||m.data.sourceWorkspace!==request.sourceWorkspace||
    m.data.baseDigest!==request.expectedSourceDigest||m.data.sourceSnapshotDigest!==request.expectedSourceSnapshotDigest||
    m.data.createdAt!==r.createdAt||m.data.preparation.preparationIdentity!==request.preparationIdentity||
    m.data.preparation.bindingDigest!==r.bindingDigest||m.data.preparation.receiptDigest!==receiptDigest)return fail();
  const g=inspection.generation,c=inspection.candidate,now=options.now??Date.now();
  const checkedAt=typeof inspection.checkedAt==='number'?inspection.checkedAt:Date.parse(inspection.checkedAt);
  if(inspection.version!==1||inspection.exists!==true||inspection.workspace!==request.workspace||!Number.isFinite(now)||!Number.isFinite(checkedAt)||
    checkedAt>now||now-checkedAt>60000||g?.workspace!==request.workspace||g?.sourceWorkspace!==request.sourceWorkspace||
    g?.baseDigest!==request.expectedSourceDigest||g?.snapshotDigest!==sha(raw)||g?.sourceSnapshotDigest!==request.expectedSourceSnapshotDigest||
    c?.baseDigest!==request.expectedSourceDigest||!hash.safeParse(c?.candidateDigest).success)return fail();
  const destinationChanged=c.candidateDigest!==request.expectedSourceDigest;
  if(inspection.destinationChanged!==destinationChanged||(options.requireUnchanged&&destinationChanged))return fail();
  return {version:1,request:{...parsed.data},receipt:r,targetSnapshotDigest:sha(raw),
    currentCandidateDigest:c.candidateDigest,destinationChanged,checkedAt};
}

/** New task instructions capture this controller-owned binding at admission.
 * A descendant whose source is another task must bind that parent's generation
 * separately; an unrelated preparation must not be copied into its contract.
 */
export function preparedBaselineBinding(prepared:VerifiedSourcePreparation,sourceWorkspace:string,targetWorkspace:string,preparationIdentity:string){
  if(sourceWorkspace!==prepared.request.workspace)return null;
  if(sourceWorkspace===targetWorkspace||sourceWorkspace.split('/')[0]!==targetWorkspace.split('/')[0])return fail();
  return sourcePreparationRequestSchema.parse({workspace:targetWorkspace,sourceWorkspace,
    preparationIdentity,expectedSourceDigest:prepared.receipt.cloneBaseDigest,
    expectedSourceSnapshotDigest:prepared.targetSnapshotDigest});
}
