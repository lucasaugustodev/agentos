import {afterAll,beforeAll,expect,it,vi} from 'vitest';
import {randomUUID} from 'node:crypto';
import {pool,migrate} from '../server/db.js';
import {config} from '../server/config.js';
import {installObjectiveReview} from '../server/objective-review.js';

beforeAll(async()=>{
  expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');
  await migrate();
});
afterAll(()=>pool.end());

// A single cold -> failed -> retry -> warm lifecycle uses this isolated file's
// module instance. No exported cache-reset hook and no production/module reset.
it('coalesces cold installation, rolls back failed DDL and retries once before caching success',async()=>{
  const client=await pool.connect();
  const schema='review_install_'+randomUUID().replaceAll('-','');
  const commands:string[]=[];
  let failFirstDdl=true,ddlCount=0,leased=false,releaseFailure!:()=>void,enteredDdl!:()=>void;
  const failureGate=new Promise<void>(resolve=>{releaseFailure=resolve;});
  const entered=new Promise<void>(resolve=>{enteredDdl=resolve;});
  const injected=new Error('SYNTHETIC_INSTALL_FAILURE_AFTER_DDL');
  const release=vi.fn(()=>{leased=false;});
  const wrapped={
    // Keep the real PostgreSQL connection owned by the fixture until cleanup;
    // the installer still must release its acquired handle after every attempt.
    release,
    query:async(text:string,...values:any[])=>{
      commands.push(text);
      if(text.startsWith('CREATE TABLE IF NOT EXISTS objective_review_signals')){
        ddlCount++;
        const result=await (client.query as any)(text,...values);
        if(failFirstDdl){
          failFirstDdl=false;enteredDdl();await failureGate;throw injected;
        }
        return result;
      }
      return (client.query as any)(text,...values);
    },
  };
  let restoreConnect:(()=>void)|undefined;
  const pending:Promise<unknown>[]=[];
  try{
    // Names are generated locally from a UUID; no external identifiers or data.
    await client.query('CREATE SCHEMA '+schema);
    await client.query('SET search_path TO '+schema);
    await client.query(`CREATE TABLE goals(id uuid PRIMARY KEY);
      CREATE TABLE tasks(id uuid PRIMARY KEY);
      CREATE TABLE runs(id uuid PRIMARY KEY);
      CREATE TABLE events(id bigserial PRIMARY KEY,kind text NOT NULL,entity_id text,payload jsonb NOT NULL);`);
    const connectSpy=vi.spyOn(pool,'connect').mockImplementation((async()=>{
      // A broken coalescer must fail the test, not interleave two transactions
      // on the fixture's one physical client and invalidate the counterproof.
      if(leased)throw new Error('DUPLICATE_SCHEMA_INSTALLATION');
      leased=true;return wrapped;
    }) as any);
    restoreConnect=()=>connectSpy.mockRestore();

    const first=installObjectiveReview(),concurrent=installObjectiveReview();
    const outcomes=Promise.allSettled([first,concurrent]);
    pending.push(outcomes);
    await Promise.race([entered,outcomes.then(()=>{throw new Error('INSTALL_DDL_NOT_REACHED');})]);
    expect(connectSpy).toHaveBeenCalledTimes(1);
    expect(ddlCount).toBe(1);
    releaseFailure();
    const failed=await outcomes;
    expect(failed).toEqual([{status:'rejected',reason:injected},{status:'rejected',reason:injected}]);
    expect(commands.filter(command=>command==='BEGIN')).toHaveLength(1);
    expect(commands.filter(command=>command==='ROLLBACK')).toHaveLength(1);
    expect(commands.filter(command=>command==='COMMIT')).toHaveLength(0);
    expect(release).toHaveBeenCalledTimes(1);
    expect((await client.query('SELECT to_regclass($1) relation',[schema+'.objective_review_signals'])).rows[0].relation).toBeNull();
    expect((await client.query('SELECT to_regprocedure($1) routine',[schema+'.agentos_objective_control_notify()'])).rows[0].routine).toBeNull();

    const retryCalls=[installObjectiveReview(),installObjectiveReview()];
    pending.push(...retryCalls);await Promise.all(retryCalls);
    expect(connectSpy).toHaveBeenCalledTimes(2);
    expect(ddlCount).toBe(2);
    expect(commands.filter(command=>command==='BEGIN')).toHaveLength(2);
    expect(commands.filter(command=>command==='ROLLBACK')).toHaveLength(1);
    expect(commands.filter(command=>command==='COMMIT')).toHaveLength(1);
    expect(release).toHaveBeenCalledTimes(2);
    expect((await client.query('SELECT to_regclass($1) relation',[schema+'.objective_review_signals'])).rows[0].relation).not.toBeNull();
    expect((await client.query(`SELECT count(*)::int n FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid
      JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1
      AND t.tgname IN('agentos_objective_control_notify','agentos_objective_review_history')`,[schema])).rows[0].n).toBe(2);

    const installedCommands=commands.length;
    const warmCalls=[installObjectiveReview(),installObjectiveReview(),installObjectiveReview()];
    pending.push(...warmCalls);await Promise.all(warmCalls);
    expect(connectSpy).toHaveBeenCalledTimes(2);
    expect(commands).toHaveLength(installedCommands);
    expect(release).toHaveBeenCalledTimes(2);
  }finally{
    releaseFailure();
    await Promise.allSettled(pending);
    restoreConnect?.();
    // A failing assertion still leaves only this fixture's isolated schema.
    await client.query('ROLLBACK');
    await client.query('RESET search_path');
    await client.query('DROP SCHEMA IF EXISTS '+schema+' CASCADE');
    client.release();
  }
});
