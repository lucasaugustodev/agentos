import { randomUUID } from 'node:crypto';
import { beforeAll, afterAll, expect, it } from 'vitest';
import { pool, migrate, tx } from '../server/db.js';
import { installRuntimeLookupIndexes } from '../server/runtime-lookup-indexes.js';
import {seedRegistry} from '../server/registry.js';
import { config } from '../server/config.js';

beforeAll(async () => {
  expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');
  await migrate(); await seedRegistry();
});
afterAll(async () => { await pool.end(); });

it('serializes independent installers and leaves all runtime lookup indexes ready', async () => {
  await Promise.all([installRuntimeLookupIndexes(pool), installRuntimeLookupIndexes(pool)]);
  const rows = (await pool.query(`SELECT c.relname,i.indisvalid,i.indisready FROM pg_index i
    JOIN pg_class c ON c.oid=i.indexrelid WHERE c.relname=ANY($1::text[])`, [[
    'artifacts_run_name_idx','acceptances_run_created_idx','events_run_activity_idx','events_kind_created_idx','tasks_goal_scope_idx',
    'events_confirmed_run_kind_idx',
  ]])).rows;
  expect(rows).toHaveLength(6);
  expect(rows.every(x => x.indisvalid && x.indisready)).toBe(true);
});

it('preserves all recovery stop-query results and indexes present and absent runs with custom and generic plans', async () => {
  const entity = randomUUID();
  const ids = { stopped: randomUUID(), notStarted: randomUUID(), falseOnly: randomUUID(),
    missing: randomUUID(), otherKind: randomUUID(), absent: randomUUID(), malformed: 'not-a-uuid-' + randomUUID() };
  // These are the three recovery branches; the first two intentionally share
  // their SQL and return all matches, whereas the third only tests existence.
  const queries = [
    { name: 'context', sql: "SELECT 1 FROM events WHERE kind IN('HARNESS_STOPPED','HARNESS_NOT_STARTED') AND payload->>'runId'=$1 AND payload->>'confirmed'='true'" },
    { name: 'resource', sql: "SELECT 1 FROM events WHERE kind IN('HARNESS_STOPPED','HARNESS_NOT_STARTED') AND payload->>'runId'=$1 AND payload->>'confirmed'='true'" },
    { name: 'stopped', sql: "SELECT 1 FROM events WHERE kind='HARNESS_STOPPED' AND payload->>'runId'=$1 AND payload->>'confirmed'='true' LIMIT 1" },
  ];
  const client = await pool.connect();
  try {
    await pool.query(`INSERT INTO events(kind,entity_id,payload)
      SELECT CASE n%4 WHEN 0 THEN 'MODEL_RESPONSE' WHEN 1 THEN 'HARNESS_STOPPED'
        WHEN 2 THEN 'HARNESS_NOT_STARTED' ELSE 'HARNESS_STOPPED' END,$1,
        jsonb_build_object('runId',gen_random_uuid()::text,'confirmed',n%5<>0,
          'preservedEvidence',repeat(md5(n::text),8)) FROM generate_series(1,16384)n`,[entity]);
    for (const [kind, runId, confirmed] of [
      ['HARNESS_STOPPED', ids.stopped, true],
      ['HARNESS_STOPPED', ids.stopped, 'true'],
      ['HARNESS_NOT_STARTED', ids.notStarted, true],
      ['HARNESS_STOPPED', ids.falseOnly, false],
      ['HARNESS_STOPPED', ids.falseOnly, 'false'],
      ['HARNESS_STOPPED', ids.missing, undefined],
      ['MODEL_RESPONSE', ids.otherKind, true],
      ['HARNESS_STOPPED', ids.malformed, true],
    ] as const) {
      await pool.query('INSERT INTO events(kind,entity_id,payload) VALUES($1,$2,$3)',
        [kind,entity,JSON.stringify({runId,...(confirmed === undefined ? {} : {confirmed})})]);
    }
    await pool.query('ANALYZE events');
    const definition=(await pool.query(`SELECT i.indisvalid,i.indisready,i.indnkeyatts,
      pg_get_indexdef(i.indexrelid,1,true) first_key,pg_get_indexdef(i.indexrelid,2,true) second_key,
      pg_get_expr(i.indpred,i.indrelid) predicate FROM pg_index i
      WHERE i.indexrelid='public.events_confirmed_run_kind_idx'::regclass`)).rows[0];
    expect(definition).toMatchObject({indisvalid:true,indisready:true,indnkeyatts:2,second_key:'kind'});
    expect(definition.first_key).toContain("payload ->> 'runId'");
    expect(definition.predicate).toContain('HARNESS_STOPPED');
    expect(definition.predicate).toContain('HARNESS_NOT_STARTED');
    expect(definition.predicate).toContain("payload ->> 'confirmed'");
    expect(definition.predicate).toContain("'true'::text");

    for (const lookup of queries) {
      const expected = lookup.name === 'stopped' ? [1,0,0,0,0,0,1] : [2,1,0,0,0,0,1];
      const runIds = Object.values(ids);
      await client.query('BEGIN');
      try {
        // Compare actual multisets with the same SQL executed as a scan. This
        // does not drop the shared test index or weaken planner assertions.
        await client.query('SET LOCAL enable_indexscan=off');
        await client.query('SET LOCAL enable_indexonlyscan=off');
        await client.query('SET LOCAL enable_bitmapscan=off');
        const scanPlan=(await client.query('EXPLAIN (FORMAT JSON) '+lookup.sql,[ids.stopped])).rows[0]['QUERY PLAN'];
        expect(JSON.stringify(scanPlan)).toContain('Seq Scan');
        const scanRows=[];
        for (const run of runIds) scanRows.push((await client.query(lookup.sql,[run])).rows);
        expect(scanRows.map(rows=>rows.length)).toEqual(expected);
        await client.query('SET LOCAL enable_indexscan=on');
        await client.query('SET LOCAL enable_indexonlyscan=on');
        await client.query('SET LOCAL enable_bitmapscan=on');
        for (let i=0;i<runIds.length;i++) expect((await client.query(lookup.sql,[runIds[i]])).rows).toEqual(scanRows[i]);

        for (const mode of ['force_custom_plan','force_generic_plan']) {
          await client.query('SET LOCAL plan_cache_mode='+mode);
          const prepared='confirmed_stop_'+lookup.name;
          await client.query('PREPARE '+prepared+'(text) AS '+lookup.sql);
          try {
            for (const run of [ids.stopped,ids.absent]) {
              // UUID generated above, not arbitrary SQL input. EXECUTE has no
              // wire parameter substitution in its argument expression.
              expect(run).toMatch(/^[a-f0-9-]{36}$/);
              const plan=(await client.query(`EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) EXECUTE ${prepared}('${run}')`)).rows[0]['QUERY PLAN'];
              const text=JSON.stringify(plan);
              expect(text).toContain('events_confirmed_run_kind_idx');
              expect(text).toContain('Index Cond');
              expect(text).toContain('runId');
              expect(text).not.toContain('"Node Type":"Seq Scan"');
              const nodes: any[]=[];
              const visit=(node:any)=>{nodes.push(node);for(const child of node.Plans??[])visit(child);};
              visit(plan[0].Plan);
              expect(nodes.reduce((sum,node)=>sum+(node['Rows Removed by Filter']??0),0)).toBeLessThanOrEqual(2);
              expect(plan[0].Plan['Actual Rows']).toBe(run===ids.absent?0:lookup.name==='stopped'?1:2);
            }
          } finally { await client.query('DEALLOCATE '+prepared); }
        }
      } finally { await client.query('ROLLBACK'); }
    }
  } finally {
    client.release();
    await pool.query('DELETE FROM events WHERE entity_id=$1',[entity]);
    await pool.query('ANALYZE events');
  }
});

it('uses a workspace index to select a scope beside large immutable task contexts',async()=>{
  const goal=randomUUID();
  try{
    await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key) VALUES($1,'Scoped lookup','Scoped lookup','lookup-test',$2)",[goal,goal]);
    await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,criteria,execution)
      SELECT gen_random_uuid(),$1,'history-'||n,'History','Preserved','qa.backend','accepted','[]',
        jsonb_build_object('baseWorkspace','lookup-test/scope-'||n,'kind','harness','workKind','diagnostic',
          'contextSnapshot',repeat('immutable worker context ',10000)) FROM generate_series(1,256)n`,[goal]);
    await pool.query('ANALYZE tasks');
    const sql="SELECT id,execution->>'baseWorkspace' scope FROM tasks WHERE goal_id=$1 AND execution->>'baseWorkspace'=$2";
    const rows=(await pool.query(sql,[goal,'lookup-test/scope-17'])).rows;
    expect(rows).toHaveLength(1);
    expect(rows[0].scope).toBe('lookup-test/scope-17');
    const plan=(await pool.query('EXPLAIN (FORMAT JSON) '+sql,[goal,'lookup-test/scope-17'])).rows[0]['QUERY PLAN'];
    expect(JSON.stringify(plan)).toContain('tasks_goal_scope_idx');
    expect((await pool.query("SELECT execution->>'contextSnapshot' context FROM tasks WHERE id=$1",[rows[0].id])).rows[0].context)
      .toBe('immutable worker context '.repeat(10000));
  }finally{
    await pool.query('DELETE FROM tasks WHERE goal_id=$1',[goal]);
    await pool.query('DELETE FROM goals WHERE id=$1',[goal]);
    await pool.query('ANALYZE tasks');
  }
});

it('locates the latest activity for one run without including another run or heartbeat', async () => {
  const run = randomUUID(), other = randomUUID();
  const inserted: string[] = [];
  try {
    for (const [kind, runId, date] of [
      ['OPENCODE_TOOL_EVENT',run,'2026-01-01T00:00:01Z'],
      ['MODEL_RESPONSE',run,'2026-01-01T00:00:02Z'],
      ['MODEL_RESPONSE',other,'2026-01-01T00:00:03Z'],
      ['HEARTBEAT',run,'2026-01-01T00:00:04Z'],
    ]) {
      inserted.push((await pool.query('INSERT INTO events(kind,entity_id,payload,created_at) VALUES($1,$2,$3,$4) RETURNING id',
        [kind,run,JSON.stringify({runId}),date])).rows[0].id);
    }
    // Exercise the selective per-run lookup against a real activity history.
    // Four rows allow PostgreSQL to prefer the equally valid kind index;
    // requiring one physical plan on that empty fixture is nondeterministic.
    inserted.push(...(await pool.query(`INSERT INTO events(kind,entity_id,payload,created_at)
      SELECT 'MODEL_RESPONSE',$1,jsonb_build_object('runId',gen_random_uuid()::text),
        '2026-01-01T00:00:05Z'::timestamptz+g*interval '1 microsecond'
      FROM generate_series(1,4096)g RETURNING id`,[run])).rows.map(row=>row.id));
    await pool.query('ANALYZE events');
    await tx(async db => {
      const sql=`SELECT max(created_at) last_activity FROM events WHERE payload->>'runId'=$1
        AND kind IN('OPENCODE_TOOL_EVENT','HARNESS_TOOL_EVENT','ARTIFACT_CREATED','MODEL_RESPONSE')`;
      const result = (await db.query(sql,[run])).rows[0];
      expect(result.last_activity.toISOString()).toBe('2026-01-01T00:00:02.000Z');
      const explain = (await db.query('EXPLAIN (FORMAT JSON) '+sql,[run])).rows[0]['QUERY PLAN'];
      expect(JSON.stringify(explain)).toContain('events_run_activity_idx');
    });
  } finally {
    await pool.query('DELETE FROM events WHERE id=ANY($1::bigint[])',[inserted]);
    await pool.query('ANALYZE events');
  }
});

it('supports artifact and acceptance lookups by run using the installed indexes', async () => {
  await tx(async db => {
    await db.query('SET LOCAL enable_seqscan=off');
    const run = randomUUID();
    const artifact = (await db.query('EXPLAIN (FORMAT JSON) SELECT id,name,digest FROM artifacts WHERE run_id=$1 AND name=$2',
      [run,'implementation.patch'])).rows[0]['QUERY PLAN'];
    const acceptance = (await db.query('EXPLAIN (FORMAT JSON) SELECT passed FROM acceptances WHERE run_id=$1 ORDER BY created_at DESC LIMIT 1',
      [run])).rows[0]['QUERY PLAN'];
    expect(JSON.stringify(artifact)).toContain('artifacts_run_name_idx');
    expect(JSON.stringify(acceptance)).toContain('acceptances_run_created_idx');
  });
});
