import {beforeAll,afterAll,it,expect} from 'vitest';
import {randomUUID,createHash} from 'node:crypto';
import {redact,parseTrace} from '../bridge/observe-run.mjs';
import {pool,migrate,event} from '../server/db.js';
import {config} from '../server/config.js';
import {seedRegistry} from '../server/registry.js';
import {writeArtifact,readArtifact,completeRun} from '../server/service.js';

beforeAll(async()=>{
  expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');
  await migrate();
  await seedRegistry();
});
afterAll(async()=>{await pool.end();});
const digest=(bytes:Buffer)=>createHash('sha256').update(bytes).digest('hex');
async function fixture(){
  const goal=randomUUID(),task=randomUUID(),run=randomUUID(),fence=randomUUID();
  await pool.query(`INSERT INTO goals(id,title,description,project,idempotency_key)
    VALUES($1,'Observation Unicode','Display projection only','isolated',$2)`,[goal,'unicode-'+goal]);
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria,attempt_count)
    VALUES($1,$2,'unicode','Unicode','Real output still requires acceptance','qa.backend','running','[]',1)`,[task,goal]);
  await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state)
    VALUES($1,$2,1,$3,'opencode',1,'running')`,[run,task,fence]);
  return {task,run,fence};
}

it('makes only the display projection JSONB-compatible and preserves valid Unicode and control text',async()=>{
  const raw='start\0 middle \ud800 end \udfff valid 🐍\n\t\r ação';
  const display=redact(raw);
  expect(display).toBe('start␀ middle � end � valid 🐍\n\t\r ação');
  expect(raw).toContain('\0');
  expect((await pool.query('SELECT $1::jsonb AS value',[JSON.stringify({preview:display})])).rows[0].value)
    .toEqual({preview:display});
  await expect(pool.query('SELECT $1::jsonb',[JSON.stringify({preview:raw})])).rejects.toMatchObject({code:'22P05'});
});

it.each([
  'aa🐍'+'x'.repeat(20),
  'x'.repeat(20)+'🐍'+'z'.repeat(5),
])('normalizes a surrogate split by head or tail truncation after slicing [case %#]',async raw=>{
  const preview=redact(raw,9);
  expect(preview).toContain('trecho omitido');
  expect(preview).toContain('�');
  await expect(pool.query('SELECT $1::jsonb',[JSON.stringify({preview})])).resolves.toBeDefined();
});

it('preserves secret redaction and excludes reasoning while normalizing actual tool output',async()=>{
  expect(redact('sk-secretfixture1234\0password=never-expose\ud800')).not.toMatch(/secretfixture|never-expose/);
  const trace=[
    {type:'assistant/message',data:{message:{content:[{type:'text',text:'PRIVATE_REASONING\0'}]}}},
    {type:'tool/call',data:{callId:'a',name:'bash',arguments:JSON.stringify({command:'node test.mjs'})}},
    {type:'tool/result',data:{message:{content:[{type:'tool-result',toolCallId:'a',content:[{type:'text',text:'failure\0\ud800\nProcess exited with exit code 1'}]}]}}},
  ].map(row=>JSON.stringify(row)).join('\n');
  const result=parseTrace(trace);
  expect(result.actions[0]).toMatchObject({state:'error',exitCode:1});
  expect(result.actions[0].output).toContain('failure␀�');
  expect(JSON.stringify(result)).not.toContain('PRIVATE_REASONING');
  await expect(pool.query('SELECT $1::jsonb',[JSON.stringify(result)])).resolves.toBeDefined();
});

it('lets the actual event barrier complete before publishing unchanged artifact bytes and a submitted result',async()=>{
  const f=await fixture(),bytes=Buffer.from('real log\0retained exactly\n'),before=Buffer.from(bytes),expectedDigest=digest(bytes);
  const observation={evidence:[{name:'protocol-before-r24.log',preview:redact(bytes.toString('utf8'))}]};
  // Same awaited barrier as the native controller, using the real PostgreSQL event writer.
  const pendingEvents=event(pool,'EXECUTION_OBSERVED',f.task,{runId:f.run,observation});
  await pendingEvents;
  const artifact=await writeArtifact(f.run,f.fence,'protocol-before-r24.log',bytes,'text/plain');
  await completeRun(f.run,f.fence,'Real result; acceptance remains separate',{inputTokens:1});
  const stored=await readArtifact(artifact.id);
  expect(bytes).toEqual(before);
  expect(stored.bytes).toEqual(before);
  expect(stored.digest).toBe(expectedDigest);
  expect((await pool.query('SELECT state,summary FROM runs WHERE id=$1',[f.run])).rows[0])
    .toEqual({state:'submitted',summary:'Real result; acceptance remains separate'});
  expect((await pool.query('SELECT 1 FROM acceptances WHERE run_id=$1',[f.run])).rowCount).toBe(0);
  expect((await pool.query(`SELECT payload->'observation'->'evidence'->0->>'preview' preview
    FROM events WHERE entity_id=$1 AND kind='EXECUTION_OBSERVED'`,[f.task])).rows[0].preview).toContain('␀');
});

it('does not swallow an unrelated event database failure or run publication after a rejected barrier',async()=>{
  const f=await fixture();
  const publication=event(pool,null as unknown as string,f.task,{preview:redact('valid\0preview')})
    .then(async()=>{
      await writeArtifact(f.run,f.fence,'should-not-exist.log','output','text/plain');
      await completeRun(f.run,f.fence,'must not submit');
    });
  await expect(publication).rejects.toMatchObject({code:'23502'});
  expect((await pool.query('SELECT state FROM runs WHERE id=$1',[f.run])).rows[0].state).toBe('running');
  expect((await pool.query('SELECT 1 FROM artifacts WHERE run_id=$1',[f.run])).rowCount).toBe(0);
});

it('does not normalize authoritative result fields or bypass artifact validation',async()=>{
  const f=await fixture();
  await event(pool,'EXECUTION_OBSERVED',f.task,{preview:redact('valid\0preview')});
  await expect(writeArtifact(f.run,f.fence,'../invalid.log','output','text/plain')).rejects.toMatchObject({code:'ARTIFACT_INVALID'});
  await expect(completeRun(f.run,f.fence,'Raw result remains raw',{invalid:'\0'})).rejects.toMatchObject({code:'22P05'});
  expect((await pool.query('SELECT state FROM runs WHERE id=$1',[f.run])).rows[0].state).toBe('running');
  expect((await pool.query('SELECT 1 FROM artifacts WHERE run_id=$1',[f.run])).rowCount).toBe(0);
});
