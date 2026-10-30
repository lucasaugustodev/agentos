import {type DB} from './db.js';
import {hash} from './memory.js';
import {readFile} from 'node:fs/promises';
import path from 'node:path';
import {config} from './config.js';

export async function verificationCandidate(db:DB,runId:string) {
  const refs=(await db.query("SELECT id,run_id,name,digest,size FROM artifacts WHERE run_id=$1 AND name='candidate.json'",[runId])).rows;
  if(refs.length!==1)throw Error('VERIFICATION_CANDIDATE_UNBOUND');
  const ref=refs[0];
  try{
    if(!Number.isSafeInteger(ref.size)||ref.size<0||ref.size>8*1024*1024)throw Error('INVALID_SIZE');
    const bytes=await readFile(path.join(config.dataDir,'artifacts',ref.id));
    if(bytes.length!==ref.size||hash(bytes)!==ref.digest)throw Error('INVALID_DIGEST');
    const candidate=JSON.parse(bytes.toString());
    if(!/^[0-9a-f]{64}$/.test(candidate?.candidateDigest)||!/^[0-9a-f]{64}$/.test(candidate?.baseDigest))throw Error('INVALID_CANDIDATE');
    return {ref,candidate};
  }catch{throw Error('VERIFICATION_CANDIDATE_INVALID');}
}

export async function materialVerificationFailure(db:DB,runId:string) {
  return Boolean((await db.query(`SELECT 1 FROM acceptances a
    WHERE a.id=(SELECT id FROM acceptances WHERE run_id=$1 ORDER BY created_at DESC,id DESC LIMIT 1)
      AND NOT a.passed AND EXISTS(SELECT 1 FROM jsonb_array_elements(a.checks) c
        WHERE c->>'passed'='false' AND c->>'type' IS DISTINCT FROM 'review')`,[runId])).rowCount);
}

/** Compare input content, not attempt IDs, artifact UUIDs or supervisor UUIDs.
 * Missing frozen context cannot prove progress and must remain supervised. */
export async function verificationInputsChanged(db:DB,task:any,run:any) {
  const manifest=(await db.query('SELECT manifest FROM contexts WHERE id=$1 AND run_id=$2',
    [run.context_id,run.id])).rows[0]?.manifest;
  const frozen=run.instruction_snapshot?.task;
  if(!frozen||!Array.isArray(manifest?.dependencyArtifacts))return false;
  const normalize=(refs:any[])=>[...new Set(refs.filter(a=>!['candidate.json','task-instructions-history.json'].includes(a.name))
    .map(a=>JSON.stringify([a.name,a.digest])))].sort();
  const current=(await db.query(`SELECT a.name,a.digest FROM dependencies d JOIN tasks t ON t.id=d.depends_on
    JOIN runs r ON r.task_id=t.id AND r.attempt=t.attempt_count JOIN artifacts a ON a.run_id=r.id
    WHERE d.task_id=$1 AND t.state='accepted' AND r.state='submitted'`,[task.id])).rows;
  const memories=async(ids:string[],historical=false)=> (await db.query('SELECT id,content FROM memories WHERE id=ANY($1::uuid[])',[ids]))
    .rows.map(m=>historical?(manifest.selected??[]).find((old:any)=>old.id===m.id)?.hash??hash(m.content):hash(m.content)).sort();
  const inputs=(t:any)=>normalize(t.execution?.inputs??[]);
  const before=[normalize(manifest.dependencyArtifacts),inputs(frozen),await memories(frozen.required_memories??[],true)];
  const after=[normalize(current),inputs(task),await memories(task.required_memories??[])];
  // Removing evidence is not a new input and must not reset a material failure.
  return after.some((group,index)=>group.some(value=>!before[index].includes(value)));
}

/** Scheduling a repaired dependency can make its old QA ready again without
 * going through continuation. Recheck the same content guard at admission. */
export async function assertVerificationAdmissionProgress(db:DB,task:any) {
  if(!task.execution?.readOnlySource||!task.attempt_count)return;
  // The scheduler intentionally selects a small execution projection. Hydrate
  // only a readonly retry, after ordinary dependency checks, under its lock.
  task=(await db.query('SELECT * FROM tasks WHERE id=$1',[task.id])).rows[0];
  if(!task?.execution?.readOnlySource)return;
  const run=(await db.query('SELECT * FROM runs WHERE task_id=$1 ORDER BY attempt DESC LIMIT 1',[task.id])).rows[0];
  if(!run||run.state!=='submitted'||run.attempt!==task.attempt_count||!await materialVerificationFailure(db,run.id))return;
  try{
    const target=(await db.query(`SELECT r.id FROM dependencies d JOIN tasks t ON t.id=d.depends_on
      JOIN runs r ON r.task_id=t.id AND r.attempt=t.attempt_count WHERE d.task_id=$1
      AND t.execution->>'workspace'=$2 AND t.execution->>'readOnlySource' IS DISTINCT FROM 'true'
      AND t.state='accepted' AND r.state='submitted'`,[task.id,task.execution.sourceWorkspace])).rows;
    if(target.length!==1)throw Error('VERIFICATION_CANDIDATE_UNBOUND');
    const before=await verificationCandidate(db,run.id),after=await verificationCandidate(db,target[0].id);
    if(before.candidate.baseDigest!==before.candidate.candidateDigest)throw Error('VERIFICATION_CANDIDATE_INVALID');
    if(before.candidate.candidateDigest===after.candidate.candidateDigest &&
      !await verificationInputsChanged(db,task,run))throw Error('VERIFICATION_NO_PROGRESS');
  }catch(error:any){throw Object.assign(error,{code:error.message});}
}
