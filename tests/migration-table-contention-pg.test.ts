import {beforeAll,beforeEach,afterAll,expect,it} from 'vitest';
import {pool} from '../server/db.js';
import {config} from '../server/config.js';
import {prepareSchema,migrationReceiptKey} from '../server/schema-readiness.js';
import {prepareBacklogSchema,requireBacklogSchemaReady,backlogMigrationReceiptKey} from '../server/backlog-schema-readiness.js';
import {installSupervision} from '../server/supervision.js';
import {installVerificationSourceBinding} from '../server/verification-source-binding.js';

beforeAll(async()=>{
 expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');
 await prepareSchema();await prepareBacklogSchema();
});
beforeEach(async()=>{await prepareSchema();await prepareBacklogSchema();});
afterAll(()=>pool.end());
const receipt=async(key:string)=>(await pool.query('SELECT value FROM settings WHERE id=$1',[key])).rows[0]?.value;
const definitions=async()=>(await pool.query("SELECT oid::text,xmin::text,proname FROM pg_proc WHERE pronamespace='public'::regnamespace AND proname IN('agentos_supervision_hold','agentos_verification_binding_guard') ORDER BY oid")).rows;

it('reproduces the real opposite relation order as a PostgreSQL deadlock without killing either session',async()=>{
 const reader=await pool.connect(),installer=await pool.connect();
 try{
  await reader.query('BEGIN');await reader.query("SET LOCAL statement_timeout='8s'");
  await installer.query('BEGIN');await installer.query("SET LOCAL statement_timeout='8s'");
  await reader.query('LOCK TABLE tasks IN ACCESS SHARE MODE');
  await installer.query('LOCK TABLE supervision_cases IN ACCESS EXCLUSIVE MODE');
  const pending=installer.query('LOCK TABLE tasks IN ACCESS EXCLUSIVE MODE')
   .then(()=>({code:'ok'})).catch(error=>({code:error.code})).finally(()=>installer.query('ROLLBACK'));
  const read=reader.query('SELECT count(*) FROM supervision_cases')
   .then(()=>({code:'ok'})).catch(error=>({code:error.code})).finally(()=>reader.query('ROLLBACK'));
  const outcomes=await Promise.all([pending,read]);
  expect(outcomes.map(result=>result.code).sort()).toEqual(['40P01','ok']);
  expect((await reader.query('SELECT 1 ok')).rows[0].ok).toBe(1);
  expect((await installer.query('SELECT 1 ok')).rows[0].ok).toBe(1);
 }finally{
  await reader.query('ROLLBACK');await installer.query('ROLLBACK');reader.release();installer.release();
 }
});

it.each(['tasks','supervision_cases'])('refuses supervision DDL before changing definitions while %s has a live reader, then canonically reconciles backlog',async table=>{
 const reader=await pool.connect(),base=await receipt(migrationReceiptKey),before=await definitions();
 try{
  await reader.query('BEGIN');await reader.query("SET LOCAL statement_timeout='1s'");
  await reader.query(`LOCK TABLE ${table} IN ACCESS SHARE MODE`);
  await pool.query('DELETE FROM settings WHERE id=$1',[backlogMigrationReceiptKey]);
  await expect(prepareBacklogSchema({apply:installSupervision,retryDelaysMs:[]})).rejects.toMatchObject({code:'55P03'});
  expect(await definitions()).toEqual(before);
  expect((await receipt(backlogMigrationReceiptKey)).state).toBe('applying');
  expect(await receipt(migrationReceiptKey)).toEqual(base);
  await expect(requireBacklogSchemaReady()).rejects.toMatchObject({code:'BACKLOG_MIGRATION_REQUIRED'});
  // The failed installer released earlier locks; the SAME live reader proceeds.
  await expect(reader.query('SELECT count(*) FROM tasks t JOIN supervision_cases c ON c.task_id=t.id')).resolves.toBeDefined();
 }finally{await reader.query('ROLLBACK');reader.release();}
 await expect(prepareBacklogSchema()).resolves.toMatchObject({state:'complete',alreadyApplied:false});
 expect(await receipt(migrationReceiptKey)).toEqual(base);
 await expect(requireBacklogSchemaReady()).resolves.toMatchObject({version:1});
});

it('refuses the tasks/settings verification-binding inversion before function replacement and releases its partial lock set',async()=>{
 const reader=await pool.connect(),before=await definitions();
 try{
  await reader.query('BEGIN');await reader.query("SET LOCAL statement_timeout='1s'");
  await reader.query('LOCK TABLE tasks IN ACCESS SHARE MODE');
  await expect(installVerificationSourceBinding()).rejects.toMatchObject({code:'55P03'});
  expect(await definitions()).toEqual(before);
  await expect(reader.query('SELECT count(*) FROM settings')).resolves.toBeDefined();
 }finally{await reader.query('ROLLBACK');reader.release();}
 await installVerificationSourceBinding();
});

it('uses the existing bounded retry after the live reader finishes, preserving the completed base receipt',async()=>{
 const reader=await pool.connect(),base=await receipt(migrationReceiptKey);let calls=0;
 try{
  await reader.query('BEGIN');await reader.query('LOCK TABLE tasks IN ACCESS SHARE MODE');
  await pool.query('DELETE FROM settings WHERE id=$1',[backlogMigrationReceiptKey]);
  await expect(prepareBacklogSchema({retryDelaysMs:[1],apply:async()=>{
   calls++;
   try{await installSupervision();}
   catch(error){await reader.query('ROLLBACK');throw error;}
  }})).resolves.toMatchObject({state:'complete',alreadyApplied:false});
  expect(calls).toBe(2);expect(await receipt(migrationReceiptKey)).toEqual(base);
 }finally{await reader.query('ROLLBACK');reader.release();}
});
