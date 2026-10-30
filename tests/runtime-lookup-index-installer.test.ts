import type { Pool } from 'pg';
import { expect, it } from 'vitest';
import { installRuntimeLookupIndexes } from '../server/runtime-lookup-indexes.js';

function installerClient(options: { invalid?: boolean; createError?: Error } = {}) {
  const statements: string[]=[];
  let released=false;
  const client={
    async query(sql:string,values?:string[]) {
      statements.push(sql);
      if(sql.includes('SELECT indisvalid')) {
        if(values?.[0]==='public.events_confirmed_run_kind_idx')
          return {rows:options.invalid?[{valid:false}]:[]};
        return {rows:[{valid:true}]};
      }
      if(sql.startsWith('CREATE INDEX')&&options.createError) throw options.createError;
      return {rows:[]};
    },
    release(){released=true;},
  };
  return {pool:{connect:async()=>client} as unknown as Pool,statements,released:()=>released};
}

it('adds the confirmed-run index as one concurrent statement outside a transaction',async()=>{
  const state=installerClient();
  await installRuntimeLookupIndexes(state.pool);
  expect(state.statements[0]).toBe('SELECT pg_advisory_lock(4280099)');
  const ddl=state.statements.filter(sql=>sql.startsWith('CREATE INDEX'));
  expect(ddl).toHaveLength(1);
  expect(ddl[0]).toContain('CREATE INDEX CONCURRENTLY IF NOT EXISTS events_confirmed_run_kind_idx');
  expect(ddl[0]).toContain("ON public.events((payload->>'runId'),kind)");
  expect(ddl[0]).toContain("kind IN('HARNESS_STOPPED','HARNESS_NOT_STARTED') AND payload->>'confirmed'='true'");
  expect(state.statements.some(sql=>/^(BEGIN|COMMIT|ROLLBACK)\b/.test(sql))).toBe(false);
  expect(state.statements.at(-1)).toBe('SELECT pg_advisory_unlock(4280099)');
  expect(state.released()).toBe(true);
});

it('does not skip or recreate an invalid confirmed-run index and releases ownership',async()=>{
  const state=installerClient({invalid:true});
  await expect(installRuntimeLookupIndexes(state.pool)).rejects.toThrow('RUNTIME_LOOKUP_INDEX_INVALID');
  expect(state.statements.some(sql=>sql.startsWith('CREATE INDEX'))).toBe(false);
  expect(state.statements.at(-1)).toBe('SELECT pg_advisory_unlock(4280099)');
  expect(state.released()).toBe(true);
});

it('propagates a concurrent-build timeout without retry or destructive reconciliation',async()=>{
  const error=Object.assign(new Error('statement timeout'),{code:'57014'});
  const state=installerClient({createError:error});
  await expect(installRuntimeLookupIndexes(state.pool)).rejects.toBe(error);
  expect(state.statements.filter(sql=>sql.startsWith('CREATE INDEX'))).toHaveLength(1);
  expect(state.statements.some(sql=>/\b(DROP|REINDEX)\b/.test(sql))).toBe(false);
  expect(state.statements.at(-1)).toBe('SELECT pg_advisory_unlock(4280099)');
  expect(state.released()).toBe(true);
});
