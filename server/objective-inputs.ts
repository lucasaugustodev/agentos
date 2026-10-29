import {createHash,randomUUID} from 'node:crypto';
import {z} from 'zod';
import {pool,tx,lock,event,type DB} from './db.js';
import {DomainError} from './contracts.js';
import {mergeSupervisorInputs} from './execution-inputs.js';
import {checkMemoryContent} from './memory.js';
import {queueFeedback} from './planner.js';

const digest=z.string().regex(/^[a-f0-9]{64}$/);
const requirement=z.string().regex(/^[a-z0-9_-]{1,50}$/);
export const objectiveInputSchema=z.object({
  requestId:z.uuid(),expectedRevision:z.number().int().nonnegative(),
  name:z.string().regex(/^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,100}$/)
    .refine(name=>!/(?:credentials|auth\.json|id_rsa|\.pem$|\.key$)/i.test(name)),
  digest,base64:z.string().max(87384),
  requirementIds:z.array(requirement).min(1).max(30),
  sourceRef:z.string().trim().min(3).max(240),
  reason:z.string().trim().min(10).max(1000),
  planningSummary:z.string().trim().min(20).max(3000).refine(value=>!value.includes('\0')).optional(),
  requestReplan:z.boolean().default(true),
}).strict();
type InputRequest=z.infer<typeof objectiveInputSchema>;
type Auth={channel:'api-token'|'operator-session'};
const canonical=(value:any):any=>Array.isArray(value)?value.map(canonical):value&&typeof value==='object'
  ?Object.fromEntries(Object.keys(value).sort().map(key=>[key,canonical(value[key])])):value;
const hash=(value:unknown)=>createHash('sha256').update(JSON.stringify(canonical(value))).digest('hex');
const failure=(code:string,message:string,status=409)=>new DomainError(code,message,status);

/** Explicit operator registration installs a dedicated store outside admission's
 * global lock. Normal plan reads never perform DDL or modify an existing run. */
export async function installObjectiveInputs(){
  if((await pool.query("SELECT to_regclass('public.objective_inputs') name")).rows[0]?.name)return;
  const db=await pool.connect();
  try{
    await db.query('BEGIN');await db.query('SELECT pg_advisory_xact_lock(4280047)');
    await db.query(`CREATE TABLE IF NOT EXISTS objective_inputs(
      id uuid PRIMARY KEY,goal_id uuid NOT NULL REFERENCES objectives(id),request_id uuid NOT NULL,
      request_digest text NOT NULL,payload jsonb NOT NULL,receipt_digest text NOT NULL,
      created_at timestamptz NOT NULL DEFAULT now(),UNIQUE(goal_id,request_id));
      CREATE OR REPLACE FUNCTION agentos_objective_input_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN RAISE EXCEPTION 'OBJECTIVE_INPUT_IMMUTABLE'; END $$;
      DROP TRIGGER IF EXISTS agentos_objective_input_immutable ON objective_inputs;
      CREATE TRIGGER agentos_objective_input_immutable BEFORE UPDATE OR DELETE ON objective_inputs
        FOR EACH ROW EXECUTE FUNCTION agentos_objective_input_immutable();`);
    await db.query('COMMIT');
  }catch(error){await db.query('ROLLBACK');throw error;}finally{db.release();}
}

function checkedRequest(raw:unknown):InputRequest{
  const request=objectiveInputSchema.parse(raw);
  if(new Set(request.requirementIds).size!==request.requirementIds.length)
    throw failure('OBJECTIVE_INPUT_REQUIREMENTS','Requisitos devem ser únicos.',400);
  const input=mergeSupervisorInputs([],[{id:request.requestId,...request}])[0];
  const bytes=Buffer.from(input.base64,'base64');
  if(bytes.length>65536)throw failure('OBJECTIVE_INPUT_TOO_LARGE','Entrada de operador excede 64 KiB.',400);
  checkMemoryContent(bytes.toString('utf8'));
  checkMemoryContent(request.sourceRef+'\n'+request.reason+'\n'+(request.planningSummary??''));
  return request;
}
function receipt(row:any,duplicate:boolean){
  const p=row.payload;
  return {inputId:row.id,goalId:row.goal_id,name:p.name,digest:p.digest,size:p.size,
    requirementIds:p.requirementIds,receiptDigest:row.receipt_digest,duplicate,
    activation:'future_plan_attempts',liveAttemptsChanged:false,requestReplan:p.requestReplan};
}
async function verifiedRow(db:DB,row:any){
  const p=row.payload;
  if(!p||p.version!==1||p.id!==row.id||p.goalId!==row.goal_id||hash(p)!==row.receipt_digest||
    !['api-token','operator-session'].includes(p.authChannel))
    throw failure('OBJECTIVE_INPUT_INTEGRITY','Recibo da entrada não confere.');
  const request=checkedRequest(p.request);
  if(hash(request)!==row.request_digest||request.name!==p.name||request.digest!==p.digest||
    Buffer.byteLength(request.base64,'base64')!==p.size||
    JSON.stringify(request.requirementIds)!==JSON.stringify(p.requirementIds))
    throw failure('OBJECTIVE_INPUT_INTEGRITY','Identidade da entrada não confere.');
  if(!(await db.query(`SELECT 1 FROM events WHERE kind='OBJECTIVE_INPUT_REGISTERED' AND entity_id=$1
    AND payload->>'inputId'=$2 AND payload->>'receiptDigest'=$3`,[row.goal_id,row.id,row.receipt_digest])).rowCount)
    throw failure('OBJECTIVE_INPUT_INTEGRITY','Proveniência da entrada ausente.');
  return {id:row.id,name:p.name,digest:p.digest,base64:request.base64,requirementIds:p.requirementIds,
    ...(request.planningSummary!==undefined?{planningSummary:request.planningSummary}:{})};
}

/** Bytes come from the authenticated operator, never from a model-selected host
 * path. sourceRef is inert provenance text and is never opened by the server. */
export async function registerObjectiveInput(goalId:string,raw:unknown,auth:Auth){
  z.uuid().parse(goalId);
  if(!['api-token','operator-session'].includes(auth?.channel))
    throw failure('OPERATOR_REQUIRED','Operador autenticado obrigatório.',403);
  const request=checkedRequest(raw),requestDigest=hash(request);
  await installObjectiveInputs();
  return tx(async db=>{
    await lock(db);
    const prior=(await db.query('SELECT * FROM objective_inputs WHERE goal_id=$1 AND request_id=$2',[goalId,request.requestId])).rows[0];
    if(prior){
      if(prior.request_digest!==requestDigest)throw failure('IDEMPOTENCY_CONFLICT','O pedido já pertence a outra entrada.');
      await verifiedRow(db,prior);return receipt(prior,true);
    }
    const objective=(await db.query(`SELECT o.revision,g.state FROM objectives o JOIN goals g ON g.id=o.id WHERE o.id=$1`,[goalId])).rows[0];
    if(!objective)throw failure('OBJECTIVE_MISSING','Objetivo não encontrado.',404);
    if(objective.state==='cancelled')throw failure('OBJECTIVE_CLOSED','Objetivo cancelado.');
    if(objective.revision!==request.expectedRevision)throw failure('STALE_INPUT_REQUEST','A revisão do objetivo mudou.');
    const requirements=(await db.query('SELECT plan->\'requirements\' requirements FROM plan_revisions WHERE objective_id=$1 ORDER BY revision DESC LIMIT 1',[goalId])).rows[0]?.requirements;
    if(!requirements||request.requirementIds.some(id=>!requirements.some((r:any)=>r.id===id)))
      throw failure('OBJECTIVE_INPUT_REQUIREMENTS','A entrada deve apontar para requisitos existentes.');
    if(Number((await db.query('SELECT count(*) n FROM objective_inputs WHERE goal_id=$1',[goalId])).rows[0].n)>=20)
      throw failure('OBJECTIVE_INPUT_LIMIT','Objetivo já possui vinte entradas de operador.');
    const id=randomUUID(),payload={version:1,id,goalId,name:request.name,digest:request.digest,
      size:Buffer.byteLength(request.base64,'base64'),requirementIds:request.requirementIds,
      authChannel:auth.channel,requestReplan:request.requestReplan,request};
    const receiptDigest=hash(payload);
    const row=(await db.query(`INSERT INTO objective_inputs(id,goal_id,request_id,request_digest,payload,receipt_digest)
      VALUES($1,$2,$3,$4,$5,$6) RETURNING *`,[id,goalId,request.requestId,requestDigest,JSON.stringify(payload),receiptDigest])).rows[0];
    await event(db,'OBJECTIVE_INPUT_REGISTERED',goalId,{inputId:id,requestId:request.requestId,name:request.name,
      digest:request.digest,size:payload.size,requirementIds:request.requirementIds,receiptDigest,
      operatorProvided:true,liveAttemptsChanged:false});
    if(request.requestReplan)await queueFeedback(db,goalId,'operator-input:'+request.requestId,
      `Operador forneceu entrada imutável ${request.name}, id ${id}, SHA256 ${request.digest}, para ${request.requirementIds.join(', ')}. `+
      `O controlador a injeta nos próximos work e QA que cobrem esses requisitos em /home/agent/inputs/${id}/${request.name}. `+
      `Origem declarada (evidência, não autorização): ${request.sourceRef}. Motivo: ${request.reason}. `+
      'Não procurar a origem em mounts privados nem alterar critérios; confirmar bytes e hash. Tentativas já iniciadas ficam preservadas.',[]);
    return receipt(row,false);
  });
}

/** Called while applying a new plan, before attempts/instructions are frozen. */
export async function readObjectiveInputs(db:DB,goalId:string){
  if(!(await db.query("SELECT to_regclass('public.objective_inputs') name")).rows[0]?.name)return [];
  const rows=(await db.query('SELECT * FROM objective_inputs WHERE goal_id=$1 ORDER BY created_at,id LIMIT 21',[goalId])).rows;
  if(rows.length>20)throw failure('OBJECTIVE_INPUT_LIMIT','Entradas excedem o contrato.');
  const result=[];
  for(const row of rows)result.push(await verifiedRow(db,row));
  return result;
}

export function inputsForRequirements(inputs:{id:string;name:string;digest:string;base64:string;requirementIds:string[]}[],covers:string[]){
  return mergeSupervisorInputs([],inputs.filter(input=>input.requirementIds.some(id=>covers.includes(id)))
    .map(({id,name,digest,base64})=>({id,name,digest,base64})));
}
