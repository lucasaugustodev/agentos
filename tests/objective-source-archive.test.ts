import {beforeAll,beforeEach,afterAll,afterEach,it,expect} from 'vitest';
import {mkdir,writeFile,mkdtemp,symlink,rm} from 'node:fs/promises';
import {execFileSync} from 'node:child_process';
import path from 'node:path';
import os from 'node:os';
import {randomUUID,createHash} from 'node:crypto';
import {migrate,pool} from '../server/db.js';
import {config} from '../server/config.js';
import {objectiveSource,objectiveSourceArchive,objectiveSourceSummary} from '../server/objective-source-archive.js';

const sha=(b:Buffer|string)=>createHash('sha256').update(b).digest('hex');
const original={dataDir:config.dataDir,workspacesRoot:config.workspacesRoot};
let root='';
beforeAll(async()=>{expect(new URL(config.databaseUrl).pathname).toBe('/agentos_test');await migrate();});
beforeEach(async()=>{
 await pool.query('TRUNCATE goals,agents,accounts,events CASCADE');
 await pool.query("INSERT INTO agents(id,name,department,capabilities,runtime,version,description) VALUES('source-fixture','Fixture','test','[]','test',1,'Fixture')");
 root=await mkdtemp(path.join(os.tmpdir(),'agentos-source-'));
 Object.assign(config,{dataDir:root+'/data',workspacesRoot:root+'/workspaces'});
 await mkdir(root+'/data/artifacts',{recursive:true});
});
afterEach(async()=>{Object.assign(config,original);await rm(root,{recursive:true,force:true});});
afterAll(()=>pool.end());

const sources={'server.js':'console.log("ok")\n','public/index.html':'<h1>Mini</h1>\n','test/a.test.js':'// t\n'};
async function fixture({state='completed',workspace='mini/oabcr2-deliver'}={}){
 const goal=randomUUID();
 await pool.query("INSERT INTO goals(id,title,description,project,idempotency_key,state) VALUES($1::uuid,'Mini','d','mini fb',$1::text,$2)",[goal,state]);
 const dir=path.join(root,'workspaces',workspace);
 await mkdir(dir+'/public',{recursive:true});await mkdir(dir+'/test',{recursive:true});await mkdir(dir+'/evidence',{recursive:true});
 for(const [p,c] of Object.entries(sources))await writeFile(dir+'/'+p,c);
 await writeFile(dir+'/evidence/report.json','{}');
 const candidate={baseDigest:sha('base'),candidateDigest:sha('cand-'+goal),
  files:Object.entries(sources).map(([p,c])=>({path:p,sha256:sha(c)}))};
 const add=async(key:string,execution:any,at:string,cand:any)=>{
  const task=randomUUID(),run=randomUUID(),art=randomUUID(),bytes=Buffer.from(JSON.stringify(cand));
  await pool.query(`INSERT INTO tasks(id,goal_id,key,title,description,agent_id,state,stage,criteria,execution)
   VALUES($1,$2,$3,$3,'d','source-fixture','accepted','work','[]',$4)`,[task,goal,key,JSON.stringify(execution)]);
  await pool.query("INSERT INTO runs(id,task_id,attempt,fence,runtime,agent_version,state) VALUES($1,$2,1,$3,'harness',1,'submitted')",[run,task,randomUUID()]);
  await pool.query("INSERT INTO acceptances(id,task_id,run_id,passed,checks,reviewer,created_at) VALUES($1,$2,$3,true,'[]','test',$4)",[randomUUID(),task,run,at]);
  await writeFile(root+'/data/artifacts/'+art,bytes);
  await pool.query("INSERT INTO artifacts(id,run_id,name,mime,digest,size) VALUES($1,$2,'candidate.json','application/json',$3,$4)",[art,run,sha(bytes),bytes.length]);
  return {task,run};
 };
 await add('r1-deliver',{workspace:'mini/old-deliver'},'2026-10-27T10:00:00Z',{...candidate,files:[{path:'gone.js',sha256:sha('x')}]});
 const deliver=await add('r2-deliver',{workspace},'2026-10-27T12:00:00Z',candidate);
 await add('r2-verify',{workspace:'mini/oabcr2-verify',readOnlySource:true},'2026-10-27T13:00:00Z',candidate);
 return {goal,dir,candidate,deliver};
}

it('exports exactly the accepted source of the latest writable delivery, without evidence',async()=>{
 const f=await fixture();
 const before=(await pool.query('SELECT count(*)::int n FROM events')).rows[0].n;
 const summary=objectiveSourceSummary(await objectiveSource(f.goal));
 expect(summary).toMatchObject({taskKey:'r2-deliver',runId:f.deliver.run,fileCount:3,candidateDigest:f.candidate.candidateDigest,
  fileName:'mini-fb-'+f.candidate.candidateDigest.slice(0,12)+'.zip'});
 const zip=await objectiveSourceArchive(f.goal),file=root+'/out.zip';
 await writeFile(file,zip.bytes);
 const folder=summary.fileName.replace('.zip','');
 const listed=execFileSync('python3',['-I','-c',`import zipfile,sys,json
z=zipfile.ZipFile(sys.argv[1]);assert z.testzip() is None
print(json.dumps({n:z.read(n).decode() for n in z.namelist()}))`,file]).toString();
 const contents=JSON.parse(listed);
 expect(Object.keys(contents).sort()).toEqual([...Object.keys(sources).map(p=>folder+'/'+p),folder+'/AGENTOS-MANIFEST.json'].sort());
 for(const [p,c] of Object.entries(sources))expect(contents[folder+'/'+p]).toBe(c);
 expect(JSON.parse(contents[folder+'/AGENTOS-MANIFEST.json'])).toMatchObject({runId:f.deliver.run,candidateDigest:f.candidate.candidateDigest});
 expect((await pool.query('SELECT count(*)::int n FROM events')).rows[0].n).toBe(before);
});

it('refuses a file that no longer matches the accepted hash',async()=>{
 const f=await fixture();await writeFile(f.dir+'/server.js','tampered');
 await expect(objectiveSourceArchive(f.goal)).rejects.toMatchObject({code:'OBJECTIVE_SOURCE_CHANGED'});
});

it('refuses symlinked files and objectives that are not completed',async()=>{
 const f=await fixture();await rm(f.dir+'/server.js');await writeFile(root+'/outside.js',sources['server.js']);
 await symlink(root+'/outside.js',f.dir+'/server.js');
 await expect(objectiveSource(f.goal)).rejects.toMatchObject({code:'OBJECTIVE_SOURCE_CHANGED'});
 const running=await fixture({state:'active',workspace:'mini/other-deliver'});
 await expect(objectiveSource(running.goal)).rejects.toMatchObject({code:'OBJECTIVE_SOURCE_NOT_COMPLETED'});
});

it('refuses a workspace outside the workspaces root',async()=>{
 const f=await fixture({workspace:'mini/ok-deliver'});
 await pool.query("UPDATE tasks SET execution='{\"workspace\":\"../../etc\"}' WHERE id=$1",[f.deliver.task]);
 await expect(objectiveSource(f.goal)).rejects.toMatchObject({code:'OBJECTIVE_SOURCE_WORKSPACE_INVALID'});
});
