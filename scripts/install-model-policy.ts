/** Root-only CLI under the existing publication lock/global drain. No process
 * control, schema migration, provider request, policy-file write or drain release. */
import {constants,fstatSync} from 'node:fs';
import {open,lstat,readFile,readdir,realpath,readlink} from 'node:fs/promises';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import path from 'node:path';
import {fileURLToPath,pathToFileURL} from 'node:url';
import {createHash} from 'node:crypto';
import {validateModelPolicy} from '../bridge/model-policy.mjs';
// @ts-ignore The request gate barrier is a host-only JavaScript module.
import {withModelPolicyGateBarrier} from '../bridge/model-policy-gate.mjs';
import {modelPolicyInstallationInput,installOperatorModelPolicy} from '../server/model-policy-installation.js';
import {workerSourceDigest} from '../server/worker-readiness.js';
import {pool} from '../server/db.js';

const publication='/var/lib/agentos/releases/publication.lock';
const policyFile='/etc/agentos/model-policy.json';
const units=['agentos-api','agentos-workers','agentos-opencode','agentos-backlog','agentos-cycles'];
const exec=promisify(execFile),sha=(bytes:Buffer|string)=>createHash('sha256').update(bytes).digest('hex');
function requireValue(value:unknown,code:string):asserts value{if(!value)throw Object.assign(Error(code),{code});}
function sameFile(a:any,b:any){return ['dev','ino','uid','gid','mode','nlink','size','mtimeNs','ctimeNs'].every(key=>a[key]===b[key]);}
export function assertProtectedPolicyMetadata(info:any){
  requireValue(info.isFile()&&info.uid===0n&&info.nlink===1n&&[0o600n,0o640n].includes(info.mode&0o7777n)&&
    info.size>0n&&info.size<=4096n,'MODEL_POLICY_FILE_PROTECTION');
}
async function protectedPolicy(){
  for(const directory of ['/etc','/etc/agentos']){
    const info=await lstat(directory);requireValue(info.isDirectory()&&!info.isSymbolicLink()&&info.uid===0&&(info.mode&0o022)===0,'MODEL_POLICY_FILE_PROTECTION');
  }
  const file=await open(policyFile,constants.O_RDONLY|constants.O_NOFOLLOW);
  try{
    const before=await file.stat({bigint:true});assertProtectedPolicyMetadata(before);
    const buffer=Buffer.alloc(4097);let used=0;
    while(used<buffer.length){const {bytesRead}=await file.read(buffer,used,buffer.length-used,null);if(!bytesRead)break;used+=bytesRead;}
    requireValue(used<=4096,'MODEL_POLICY_FILE_PROTECTION');
    const bytes=buffer.subarray(0,used),after=await file.stat({bigint:true}),named=await lstat(policyFile,{bigint:true});
    requireValue(sameFile(before,after)&&sameFile(after,named)&&BigInt(bytes.length)===after.size,'MODEL_POLICY_FILE_CHANGED');
    const policy=validateModelPolicy(JSON.parse(bytes.toString('utf8')));
    requireValue(Object.keys(policy).every(key=>['version','mode','accountId','bridgeId','model','requestLimit','sessionLimit'].includes(key)),'MODEL_POLICY_INVALID');
    return {policy,fileDigest:sha(bytes),fileIdentity:sha(JSON.stringify(Object.fromEntries(
      ['dev','ino','uid','gid','mode','nlink','size','mtimeNs','ctimeNs'].map(key=>[key,String((after as any)[key])]))))};
  }finally{await file.close();}
}
async function publicationOwned(fd:number){
  requireValue(Number.isInteger(fd)&&fd>=3&&fd<=1024,'MODEL_POLICY_PUBLICATION_LOCK');
  const named=await lstat(publication),opened=fstatSync(fd);
  requireValue(named.isFile()&&!named.isSymbolicLink()&&named.uid===0&&named.nlink===1&&(named.mode&0o022)===0&&
    named.dev===opened.dev&&named.ino===opened.ino&&await readlink('/proc/self/fd/'+fd)===publication,'MODEL_POLICY_PUBLICATION_LOCK');
  const info=await readFile('/proc/self/fdinfo/'+fd,'utf8');
  requireValue(/^lock:\s+\d+: FLOCK\s+ADVISORY\s+WRITE\s+/m.test(info),'MODEL_POLICY_PUBLICATION_LOCK');
}
async function emptyCgroup(directory:string,depth=0):Promise<boolean>{
  requireValue(depth<=16,'MODEL_POLICY_CGROUP_UNCERTAIN');
  let info;try{info=await lstat(directory);}catch(error:any){if(error.code==='ENOENT')return true;throw error;}
  requireValue(info.isDirectory()&&!info.isSymbolicLink(),'MODEL_POLICY_CGROUP_UNCERTAIN');
  if((await readFile(path.join(directory,'cgroup.procs'),'utf8')).trim())return false;
  const names=await readdir(directory,{withFileTypes:true});requireValue(names.length<=4096,'MODEL_POLICY_CGROUP_UNCERTAIN');
  for(const name of names)if(name.isDirectory()&&!await emptyCgroup(path.join(directory,name.name),depth+1))return false;
  return true;
}
async function servicesStopped(){
  for(const unit of units){
    const {stdout}=await exec('systemctl',['show',unit,'--property=MainPID,ActiveState,SubState,ControlGroup'],{timeout:5000,maxBuffer:16384});
    const fields=Object.fromEntries(stdout.trim().split('\n').map(line=>{const cut=line.indexOf('=');return [line.slice(0,cut),line.slice(cut+1)];}));
    const group='/system.slice/'+unit+'.service';
    requireValue(fields.MainPID==='0'&&fields.ActiveState==='inactive'&&fields.SubState==='dead'&&
      (!fields.ControlGroup||fields.ControlGroup===group)&&await emptyCgroup('/sys/fs/cgroup'+group),'MODEL_POLICY_SERVICES_ACTIVE');
  }
}
export async function modelPolicyInstallMain(args=process.argv.slice(2)){
  requireValue(process.getuid?.()===0,'MODEL_POLICY_OPERATOR_UID');
  requireValue(args.length===2&&args[0]==='--publication-lock-fd'&&/^\d{1,4}$/.test(args[1]),'MODEL_POLICY_ARGUMENTS');
  const fd=Number(args[1]);await publicationOwned(fd);
  requireValue(!process.env.AGENTOS_MODEL_POLICY_FILE||process.env.AGENTOS_MODEL_POLICY_FILE===policyFile,'MODEL_POLICY_CONFIG_PATH');
  requireValue(new URL(process.env.DATABASE_URL??'').pathname==='/agentos','MODEL_POLICY_DATABASE_SCOPE');
  let text='';for await(const part of process.stdin){text+=part;requireValue(Buffer.byteLength(text)<=4096,'MODEL_POLICY_INPUT_TOO_LARGE');}
  const input=modelPolicyInstallationInput.parse(JSON.parse(text));
  requireValue(await realpath(process.cwd())===input.candidate&&
    await realpath(fileURLToPath(new URL('../',import.meta.url)))===input.candidate,'MODEL_POLICY_SOURCE_CHANGED');
  await servicesStopped();
  const receipt=await withModelPolicyGateBarrier(input.token,async(assertGateQuiet:()=>Promise<void>)=>{
    const observe=async()=>{
      const checkedAt=Date.now();await assertGateQuiet();await publicationOwned(fd);
      const file=await protectedPolicy(),sourceDigest=await workerSourceDigest(input.candidate);
      await servicesStopped();await assertGateQuiet();
      return {checkedAt,sourceDigest,...file,servicesStopped:true as const,gateQuiescent:true as const,publicationLockHeld:true as const};
    };
    return installOperatorModelPolicy(input,observe);
  });
  console.log(JSON.stringify({ok:true,...receipt}));
}
if(process.argv[1]&&import.meta.url===pathToFileURL(process.argv[1]).href){
  modelPolicyInstallMain().catch(error=>{
    const code=/^[A-Z0-9_]{4,80}$/.test(error?.code??'')?error.code:'MODEL_POLICY_INSTALL_FAILED';
    console.error(JSON.stringify({ok:false,code}));process.exitCode=1;
  }).finally(()=>pool.end());
}
