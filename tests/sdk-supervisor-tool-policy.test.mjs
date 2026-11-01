import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,mkdir,cp,appendFile,writeFile,readFile,rm} from 'node:fs/promises';
import path from 'node:path';
import {tmpdir} from 'node:os';
import {fileURLToPath} from 'node:url';
import {spawn} from 'node:child_process';
import {createInterface} from 'node:readline';
import {randomUUID} from 'node:crypto';

for(const role of ['supervisor','worker'])test('actual persistent bash deadline and session preservation: '+role,{timeout:45000},async t=>{
  const root=await mkdtemp(path.join(tmpdir(),'agentos-sdk-supervisor-timeout-'));
  const home=root+'/home',work=root+'/workspace',bridge=root+'/bridge';
  await mkdir(home);await mkdir(work);await mkdir(work+'/evidence');await mkdir(work+'/data');
  await cp(fileURLToPath(new URL('../bridge',import.meta.url)),bridge,{recursive:true});
  await cp(fileURLToPath(new URL('./fixtures/dsh-supervisor-tool-driver.mjs',import.meta.url)),bridge+'/tool-driver.mjs');
  await appendFile(bridge+'/worker.patch.yml','\n- insert:\n    - id: supervisor-tool-driver-fixture\n      name: /opt/agentos-bridge/tool-driver.mjs\n      inject: [llm, tools]\n');
  const patch=await readFile(bridge+'/supervisor.patch.yml','utf8');
  assert.match(patch,/timeoutMs: 60000\s*$/);
  // Shorten ONLY this disposable fixture. The production policy remains 60s.
  await writeFile(bridge+'/supervisor.patch.yml',patch.replace('timeoutMs: 60000','timeoutMs: 1000'));
  const original=await readFile('/opt/agentos-runtime/harness-node_modules/@deepseek-ai/dsh-sdk-minimal/cordis.patch.yml','utf8');
  assert.match(original,/- id: persistent-bash\s+name: '@deepseek-ai\/dsh-tool-bash-persistent'[\s\S]*?timeoutMs: 300000/);
  const identity={runId:randomUUID(),runFence:randomUUID(),role};
  const identityBytes=JSON.stringify(identity);
  await writeFile(root+'/identity.json',identityBytes);
  await writeFile(work+'/document.txt','immutable document');
  await writeFile(work+'/.agentos-snapshot.json','immutable snapshot');
  await writeFile(work+'/cpu-fixture.py',
    "import os,time,json\nfrom pathlib import Path\npid=os.getpid()\nPath('/workspace/data/cpu-identity.json').write_text(json.dumps({'pid':pid,'start':Path('/proc/'+str(pid)+'/stat').read_text().split()[21]}))\nprint('CPU_FIXTURE_STARTED',flush=True)\nend=time.monotonic()+1.8\nwhile "+(role==='supervisor'?'True':'time.monotonic()<end')+":\n x=sum(i*i for i in range(1000))\nprint('WORKER_COMPLETED_BEYOND_FIXTURE_SUPERVISOR_LIMIT',flush=True)\n");
  await writeFile(work+'/check-fixture.py',
    "import json\nfrom pathlib import Path\np=json.loads(Path('/workspace/data/cpu-identity.json').read_text())\ns=Path('/proc/'+str(p['pid'])+'/stat')\nassert not s.exists() or s.read_text().split()[21]!=p['start'], 'OLD_FOREGROUND_STILL_ALIVE'\nassert Path('/workspace/document.txt').read_text()=='immutable document'\nassert Path('/workspace/.agentos-snapshot.json').read_text()=='immutable snapshot'\nprint('SAME_SESSION_NEXT_COMMAND_SUCCEEDED',flush=True)\n");
  const nodeRoot=path.dirname(path.dirname(process.execPath));
  const args=['--unshare-all','--die-with-parent','--new-session','--clearenv',
    '--ro-bind','/usr','/usr','--symlink','usr/bin','/bin','--symlink','usr/lib','/lib','--symlink','usr/lib64','/lib64',
    '--proc','/proc','--dev','/dev','--tmpfs','/tmp','--dir','/etc','--dir','/run',
    '--ro-bind','/etc/ld.so.cache','/etc/ld.so.cache','--ro-bind',nodeRoot,nodeRoot,
    '--ro-bind','/opt/agentos-runtime','/opt/agentos-runtime',
    '--ro-bind','/opt/agentos-runtime/harness-node_modules','/opt/deepseek-harness/app/node_modules',
    '--ro-bind',bridge,'/opt/agentos-bridge','--bind',home,'/home/agent',
    '--ro-bind',work,'/workspace','--bind',work+'/evidence','/workspace/evidence','--bind',work+'/data','/workspace/data',
    '--ro-bind',root+'/identity.json','/run/fixture-identity.json',
    '--chdir','/workspace','--setenv','HOME','/home/agent','--setenv','PATH',nodeRoot+'/bin:/usr/bin:/bin',
    '--setenv','LANG','C.UTF-8',
    ...(role==='supervisor'?['--setenv','AGENTOS_SUPERVISOR_TOOL_PROFILE','supervisor-v1']:[]),
    '--',process.execPath,'/opt/agentos-bridge/sandbox-entry.mjs'];
  const child=spawn('/usr/bin/bwrap',args,{stdio:['pipe','pipe','pipe']});
  let stderr='',seq=0;const frames=[],pending=new Map();
  child.stderr.on('data',bytes=>{stderr=(stderr+bytes).slice(-16000);});
  createInterface({input:child.stdout}).on('line',line=>{
    let frame;try{frame=JSON.parse(line);}catch{return;}
    frames.push(frame);const resolve=pending.get(frame.id);if(resolve){pending.delete(frame.id);resolve(frame);}
  });
  const closed=new Promise(resolve=>child.once('close',code=>{
    for(const done of pending.values())done({error:{message:'FIXTURE_EXITED',code}});pending.clear();resolve(code);
  }));
  t.after(async()=>{child.stdin.end();if(child.exitCode===null)child.kill('SIGTERM');await closed;await rm(root,{recursive:true,force:true});});
  const rpc=(method,params)=>new Promise(resolve=>{const id=++seq;pending.set(id,resolve);child.stdin.write(JSON.stringify({jsonrpc:'2.0',id,method,params})+'\n');});
  const until=async(predicate,from)=>{
    for(let i=0;i<4000;i++){
      const frame=frames.slice(from).find(predicate);if(frame)return frame;
      if(child.exitCode!==null)throw Error('FIXTURE_EXITED '+stderr);
      await new Promise(resolve=>setTimeout(resolve,5));
    }
    throw Error('FIXTURE_BOUNDARY_TIMEOUT '+stderr);
  };
  const initialized=await rpc('initialize',{cwd:'/workspace',provider:'supervisor-tool-scripted-fixture',model:'scripted-no-provider',maxTokens:1024});
  assert.equal(initialized.result?.serverInfo?.name,'deepseek-harness-sdk-runtime',JSON.stringify({initialized,stderr}));
  for(let invocation=0;invocation<2;invocation++){
    const from=frames.length;
    const response=await rpc('session/prompt',{sessionId:identity.runId,contentBlocks:[{type:'text',text:'Execute the bounded fixture, then continue in this same session.'}]});
    assert.equal(response.error,undefined,JSON.stringify(response));
    await until(frame=>frame.method==='session.event'&&frame.params.event.type==='turn/end',from);
    const proof=JSON.parse(await readFile(work+'/evidence/sdk-timeout-proof.json','utf8'));
    assert.equal(proof.role,role);assert.equal(proof.runId,identity.runId);assert.equal(proof.runFence,identity.runFence);
    assert.equal(proof.modelSteps,3+invocation);assert.equal(proof.sessionPreserved,true);assert.equal(proof.nextCommandSucceeded,true);
  }
  const events=frames.filter(f=>f.method==='session.event');
  assert.ok(events.every(f=>f.params.sessionId===identity.runId));
  const calls=events.filter(f=>f.params.event.type==='tool/call');assert.equal(calls.length,2);
  const results=events.filter(f=>f.params.event.type==='tool/result');assert.equal(results.length,2);
  assert.equal(await readFile(root+'/identity.json','utf8'),identityBytes);
  assert.equal(await readFile(work+'/document.txt','utf8'),'immutable document');
  assert.equal(await readFile(work+'/.agentos-snapshot.json','utf8'),'immutable snapshot');
  assert.deepEqual((await rpc('shutdown')).result,{});assert.equal(await closed,0,stderr);
});
