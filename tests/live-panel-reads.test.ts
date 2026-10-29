import {beforeAll,beforeEach,afterAll,it,expect} from 'vitest';
import {randomUUID} from 'node:crypto';
import {migrate,pool} from '../server/db.js';
import {config} from '../server/config.js';
import {providerHealth} from '../server/provider-health-query.js';
import {autonomyLive} from '../server/autonomy-live.js';

beforeAll(async()=>{expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');await migrate();});
beforeEach(async()=>{
 await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');
 await pool.query("INSERT INTO agents(id,name,department,capabilities,runtime,version,description) VALUES('live-fixture','Fixture','test','[]','test',1,'Fixture')");
 await pool.query(`INSERT INTO accounts(id,label,provider,model,models,enabled,max_concurrency,allow_unknown,bridge_id) VALUES
  ('agentos-api-slow','Verboo','openai','glm-5.3-flash','["glm-5.3-flash"]',true,6,true,'agentos:api:slow'),
  ('agentos-claude-fast','Claude','claude','claude-opus-5-5','["claude-opus-5-5"]',true,6,true,'agentos:claude:fast')`);
});
afterAll(()=>pool.end());

async function fixture(){
 const goal=randomUUID(),task=randomUUID(),run=randomUUID(),failed=randomUUID();
 await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key,state) VALUES($1::uuid,'Live','d','live',$1::text,'active')",[goal]);
 await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria)
  VALUES($1,$2,'work','Tela de perfil','d','live-fixture','running','work','[]')`,[task,goal]);
 await pool.query(`INSERT INTO runs(id,task_id,attempt,fence,account_id,runtime,agent_version,state,model,started_at,created_at)
  VALUES($1,$3,1,$4,'agentos-api-slow','harness',1,'failed','glm-5.3-flash',now()-interval '2 hours',now()-interval '2 hours'),
        ($2,$3,2,$5,'agentos-api-slow','harness',1,'running','glm-5.3-flash',now()-interval '10 minutes',now()-interval '10 minutes')`,
  [failed,run,task,randomUUID(),randomUUID()]);
 await pool.query("UPDATE runs SET error_code='MODEL_TIMEOUT',finished_at=now()-interval '1 hour' WHERE id=$1",[failed]);
 return {goal,task,run};
}

it('attributes model events by run when the event names no account, and by bridge id',async()=>{
 const f=await fixture();
 await pool.query(`INSERT INTO events(kind,entity_id,payload) VALUES
  ('MODEL_RESPONSE',$1,jsonb_build_object('runId',$2::text,'tokens',10)),
  ('MODEL_RESPONSE',$1,jsonb_build_object('runId','not-a-uuid')),
  ('MODEL_RESPONSE',$1,jsonb_build_object('accountId','agentos:claude:fast','providerMs',900)),
  ('MODEL_ERROR',$1,jsonb_build_object('accountId','agentos-claude-fast','code','MODEL_TIMEOUT'))`,[f.task,f.run]);
 const health=await providerHealth(['agentos-api-slow','agentos-claude-fast']);
 expect(health['agentos-api-slow']).toMatchObject({state:'healthy',requests:1,failures:0});
 expect(health['agentos-claude-fast']).toMatchObject({state:'slow',requests:2,failures:1,timeouts:1,p50ProviderMs:900});
});

it('reports live autonomy from the database without writing',async()=>{
 const f=await fixture();
 const before=(await pool.query('SELECT count(*)::int n FROM events')).rows[0].n;
 const live=await autonomyLive();
 expect(live.active).toEqual([expect.objectContaining({id:f.run,title:'Tela de perfil',account_label:'Verboo'})]);
 expect(live.objectives).toEqual([expect.objectContaining({id:f.goal,running:1,attempts:2,failures:1,accepted_count:0})]);
 expect(live.accounts.find((a:any)=>a.id==='agentos-api-slow')).toMatchObject({attempts:2,failed:1,timeouts:1,running:1});
 expect(live.errors).toEqual([{code:'MODEL_TIMEOUT',n:1}]);
 expect(live.totals).toMatchObject({attempts:2,failed:1,succeeded:0});
 expect((await pool.query('SELECT count(*)::int n FROM events')).rows[0].n).toBe(before);
});
