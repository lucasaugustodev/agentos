import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFile,mkdtemp,writeFile,rm} from 'node:fs/promises';
import {spawn} from 'node:child_process';
import {PassThrough} from 'node:stream';
import {tmpdir} from 'node:os';
import path from 'node:path';
import {createHash} from 'node:crypto';

const hash=value=>createHash('sha256').update(value).digest('hex');
const source=await readFile(new URL('../bridge/opencode-sandbox.mjs',import.meta.url),'utf8');

async function executeFixture(prompt){
 const root=await mkdtemp(path.join(tmpdir(),'agentos-sandbox-stdin-'));
 const fixture=path.join(root,'fixture.mjs');
 const stdout=new PassThrough(),stderr=new PassThrough();
 let output='',child,exitResolve;
 stdout.on('data',chunk=>{output+=chunk;});
 const exited=new Promise(resolve=>{exitResolve=resolve;});
 try{
  await writeFile(fixture,"import {createHash} from 'node:crypto';let input='';for await(const chunk of process.stdin)input+=chunk;console.log(JSON.stringify({argv:process.argv.slice(2),bytes:Buffer.byteLength(input),digest:createHash('sha256').update(input).digest('hex')}));");
  const context=vm.createContext({process:{env:{},stdout,stderr,exit:exitResolve,cwd:()=>root}});
  const modules={
   'node:http':{default:{createServer:()=>({once(){},listen(_port,_host,resolve){resolve();},closeAllConnections(){},close(){}})}},
   'node:fs/promises':{readFile:async file=>{if(file==='/home/agent/prompt.txt')return prompt;if(file==='/home/agent/opencode.json')return '{}';throw Error('UNEXPECTED_READ');}},
   // The entrypoint probes for the isolated paths; a development host has none.
   'node:fs':{existsSync:()=>false},
   'node:path':{default:path},
   'node:child_process':{spawn:(_file,args,options)=>(child=spawn(process.execPath,[fixture,...args],{...options,cwd:root,env:process.env}))},
  };
  const module=new vm.SourceTextModule(source,{context});
  await module.link(specifier=>{
   const exports=modules[specifier];if(!exports)throw Error('UNEXPECTED_IMPORT');
   return new vm.SyntheticModule(Object.keys(exports),function(){for(const [name,value] of Object.entries(exports))this.setExport(name,value);},{context});
  });
  await module.evaluate();
  let timer;
  try{
   const code=await Promise.race([exited,new Promise((_,reject)=>{timer=setTimeout(()=>reject(Error('FIXTURE_STDIN_NOT_CLOSED')),5000);})]);
   assert.equal(code,0);
  }finally{clearTimeout(timer);}
  return JSON.parse(output);
 }finally{if(child?.exitCode===null)child.kill('SIGTERM');await rm(root,{recursive:true,force:true});}
}

test('a task filename in the prompt cannot match the model command line',async()=>{
 const marker='lucky-neko-authentic-stdin-regression';
 const prompt='Execute qa-protocol/'+marker+'.test.mjs\nPreserve all task context.';
 const result=await executeFixture(prompt);
 assert.equal(result.argv.some(argument=>argument.includes(marker)),false);
 assert.equal(result.digest,hash(prompt));
 assert.equal(result.bytes,Buffer.byteLength(prompt));
});

test('stdin transmits the entire Unicode context and reaches EOF under backpressure',async()=>{
 const prompt='Tarefa: memória, revisão e contexto único 🧪\n'.repeat(2500);
 const result=await executeFixture(prompt);
 assert.equal(result.argv.at(-1)==='AgentOS isolated task',true);
 assert.equal(result.digest,hash(prompt));
 assert.equal(result.bytes,Buffer.byteLength(prompt));
});
