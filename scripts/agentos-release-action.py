#!/usr/bin/env python3
"""Registered AgentOS action. Candidate checks run as a separate unprivileged user."""
import os, sys, json, pathlib, hashlib, subprocess, time, urllib.request, uuid, fcntl, re, stat, pwd, shlex, types

_LOADED_MODULE_CODE = sys._getframe().f_code
_REGISTRATION_LOCK = None
_EXECUTOR_SHA = None
REGISTRATION_LOCK_PATH = pathlib.Path('/opt/agentos-control/validator-registration.lock')

ORIGIN_MODULE_SHA = '742212cdc76a1404418ad9fa0b57215d377eea4433f77bc2cd29eed9e2a27e6f'
WORKSPACE_SKIP = {'.git','node_modules','evidence','data','.agentos-snapshot.json',
                  '.local-test','test-results','playwright-report','__pycache__'}
OUTPUT_ROOTS = {'dist','.local-test','test-results','playwright-report'}
CACHE_ROOTS = {'node_modules/.vite','node_modules/.vite-temp'}
MAX_FILE_BYTES = 32 * 1024 * 1024
MAX_TREE_BYTES = 128 * 1024 * 1024


def checked_tree(root, skip_directory=None, skip_file=None):
 """Read only regular, single-link files through no-follow directory descriptors.

 Parent directories are never re-resolved while descending. A candidate-created
 link, FIFO, device, hard link or concurrent change cannot become a root read.
 """
 root=pathlib.Path(root);result={};total=0
 root_fd=os.open(root,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
 def walk(directory,prefix):
  nonlocal total
  for name in sorted(os.listdir(directory)):
   relative=(prefix+'/'+name).lstrip('/')
   info=os.stat(name,dir_fd=directory,follow_symlinks=False)
   if stat.S_ISDIR(info.st_mode):
    if skip_directory and skip_directory(relative,name):continue
    fd=os.open(name,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=directory)
    try:
     opened=os.fstat(fd)
     if (opened.st_dev,opened.st_ino)!=(info.st_dev,info.st_ino):raise ValueError('VALIDATION_TREE_CHANGED')
     walk(fd,relative)
    finally:os.close(fd)
   else:
    # Ignored dependencies/runtime data are never read or copied, including
    # their expected external links. Every included entry must be regular.
    if skip_file and skip_file(relative,name):continue
    if not stat.S_ISREG(info.st_mode) or info.st_nlink!=1:raise ValueError('VALIDATION_TREE_NOT_REGULAR')
    fd=os.open(name,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK,dir_fd=directory)
    try:
     opened=os.fstat(fd)
     identity=lambda value:(value.st_dev,value.st_ino,value.st_mode,value.st_size,value.st_mtime_ns,value.st_ctime_ns,value.st_nlink)
     if identity(opened)!=identity(info) or not stat.S_ISREG(opened.st_mode) or opened.st_nlink!=1:raise ValueError('VALIDATION_TREE_CHANGED')
     if opened.st_size>MAX_FILE_BYTES:raise ValueError('VALIDATION_TREE_TOO_LARGE')
     chunks=[];remaining=opened.st_size+1
     while remaining:
      chunk=os.read(fd,min(remaining,1024*1024))
      if not chunk:break
      chunks.append(chunk);remaining-=len(chunk)
     raw=b''.join(chunks)
     if len(raw)!=opened.st_size or identity(os.fstat(fd))!=identity(opened):raise ValueError('VALIDATION_TREE_CHANGED')
     # Same-size writes can share a filesystem timestamp tick. Re-read the
     # same descriptor as well; metadata alone is not an input-byte check.
     os.lseek(fd,0,os.SEEK_SET)
     check=[];remaining=opened.st_size+1
     while remaining:
      chunk=os.read(fd,min(remaining,1024*1024))
      if not chunk:break
      check.append(chunk);remaining-=len(chunk)
     if b''.join(check)!=raw or identity(os.fstat(fd))!=identity(opened):raise ValueError('VALIDATION_TREE_CHANGED')
    finally:os.close(fd)
    total+=len(raw)
    if total>MAX_TREE_BYTES or len(result)>=20000:raise ValueError('VALIDATION_TREE_TOO_LARGE')
    result[relative]=raw
 try:walk(root_fd,'')
 finally:os.close(root_fd)
 return result


def workspace_source_snapshot(source):
 files=checked_tree(source,
  lambda relative,name:name in WORKSPACE_SKIP or name.startswith('.env'),
  lambda relative,name:name in WORKSPACE_SKIP or name.startswith('.env') or name.endswith(('.log','.pyc')))
 # Match workspace-manager's exact ASCII-escaped source digest. Assets are
 # nevertheless preserved and measured by the separate build input manifest.
 selected={name:raw for name,raw in files.items() if 'assets' not in pathlib.PurePosixPath(name).parts[:-1]}
 if any(len(raw)>8*1024*1024 for raw in selected.values()):raise ValueError('VALIDATION_SOURCE_TOO_LARGE')
 source_digest=hashlib.sha256(json.dumps([(name,hashlib.sha256(raw).hexdigest()) for name,raw in sorted(selected.items())],separators=(',',':')).encode()).hexdigest()
 inputs={name:raw for name,raw in files.items() if pathlib.PurePosixPath(name).parts[0] not in OUTPUT_ROOTS
         and not any(part in {'.local','.snapshot'} for part in pathlib.PurePosixPath(name).parts)}
 return {'files':inputs,'candidateDigest':source_digest,'sourceManifestDigest':manifest_digest(files),'inputDigest':manifest_digest(inputs)}


def manifest_digest(files):
 return canonical_digest({name:hashlib.sha256(raw).hexdigest() for name,raw in files.items()})


def build_snapshot(build, include_outputs=False):
 excluded={'.local-test','test-results','playwright-report','node_modules'}
 if not include_outputs:excluded.add('dist')
 return checked_tree(build,
  lambda relative,name:relative in excluded or name=='__pycache__',
  lambda relative,name:relative in excluded or name.endswith('.pyc'))


def require_same_inputs(build,expected):
 actual=build_snapshot(build)
 if actual!=expected:raise ValueError('VALIDATION_SOURCE_CHANGED_DURING_CI')
 return manifest_digest(actual)


def write_regular_files(destination,files):
 """Destination must be new and root-owned; copy captured bytes, never paths."""
 destination=pathlib.Path(destination)
 destination.mkdir(mode=0o700)
 for relative,raw in sorted(files.items()):
  name=pathlib.PurePosixPath(relative)
  if name.is_absolute() or any(part in ('','..','.') for part in name.parts) or name.as_posix()!=relative:raise ValueError('VALIDATION_COPY_PATH')
  target=destination.joinpath(*name.parts)
  target.parent.mkdir(parents=True,exist_ok=True)
  fd=os.open(target,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o444)
  with os.fdopen(fd,'wb') as handle:
   handle.write(raw)
   os.fchmod(handle.fileno(),0o444)
 for folder,dirs,_ in os.walk(destination,followlinks=False):
  pathlib.Path(folder).chmod(0o555)


def ensure_build_root():
 for path in BUILD_ROOT.parents:
  info=path.lstat()
  if not stat.S_ISDIR(info.st_mode) or info.st_uid!=0 or info.st_mode&0o022:raise ValueError('VALIDATION_BUILD_ROOT_UNPROTECTED')
 BUILD_ROOT.mkdir(mode=0o711,exist_ok=True)
 for path in [BUILD_ROOT,*BUILD_ROOT.parents]:
  info=path.lstat()
  if not stat.S_ISDIR(info.st_mode) or info.st_uid!=0 or info.st_mode&0o022:raise ValueError('VALIDATION_BUILD_ROOT_UNPROTECTED')
 # mkdir's mode is filtered by the launching operator's umask. Change only the
 # fixed, now-verified protected directory; no existing writable root is trusted.
 BUILD_ROOT.chmod(0o711)
 return BUILD_ROOT


def protected_tool(path,root_owned=True,forbidden_uid=None):
 """Root-owned dependency files; the fixed Node install may have vendor UID."""
 path=pathlib.Path(path).resolve(strict=True)
 for item in [path,*path.parents]:
  info=item.lstat()
  if stat.S_ISLNK(info.st_mode) or info.st_mode&0o022 or (root_owned and info.st_uid!=0) or (forbidden_uid is not None and info.st_uid==forbidden_uid):
   raise ValueError('VALIDATION_TOOL_UNPROTECTED')
 if not stat.S_ISREG(path.stat().st_mode):raise ValueError('VALIDATION_TOOL_NOT_REGULAR')
 return path


def trusted_runners(base,node_path,build_uid):
 bundle=pathlib.Path(base).joinpath('node_modules').resolve(strict=True)
 result={'node':protected_tool(node_path,root_owned=False,forbidden_uid=build_uid)}
 for name,relative in [('typescript','typescript/bin/tsc'),('vite','vite/bin/vite.js'),('vitest','vitest/vitest.mjs')]:
  path=protected_tool(bundle/relative)
  if not path.is_relative_to(bundle):raise ValueError('VALIDATION_TOOL_OUTSIDE_BUNDLE')
  result[name]=path
 return result


def runner_identity(runners):
 return {key:{'path':str(path),'sha256':hashlib.sha256(path.read_bytes()).hexdigest()}
         for key,path in runners.items()}


def runner_command(runners):
 node=shlex.quote(str(runners['node']))
 return ' && '.join([node+' '+shlex.quote(str(runners['typescript']))+' --noEmit',
  node+' '+shlex.quote(str(runners['vite']))+' build',
  node+' '+shlex.quote(str(runners['vitest']))+' run --reporter=default --reporter=json --outputFile=.local-test/vitest-results.json'])


def install_dependencies_and_outputs(build,base,uid,gid):
 modules=build/'node_modules';modules.mkdir(mode=0o755)
 bundle=pathlib.Path(base).joinpath('node_modules').resolve(strict=True)
 for dependency in bundle.iterdir():
  if dependency.name not in {'.vite','.vite-temp'}:
   target=dependency.resolve(strict=True)
   if not target.is_relative_to(bundle):raise ValueError('VALIDATION_DEPENDENCY_OUTSIDE_BUNDLE')
   info=target.stat()
   if info.st_uid!=0 or info.st_mode&0o022:raise ValueError('VALIDATION_DEPENDENCY_UNPROTECTED')
   os.symlink(str(target),modules/dependency.name)
 for relative in sorted(OUTPUT_ROOTS|CACHE_ROOTS):
  path=build/relative;path.mkdir(mode=0o700)
  os.chown(path,uid,gid)
 modules.chmod(0o555)
 return [str(build/relative) for relative in sorted(OUTPUT_ROOTS|CACHE_ROOTS)]


def validation_unit(attempt):
 return 'agentos-validation-'+str(uuid.UUID(attempt))+'.service'


def confirm_validation_stopped(unit):
 if not re.fullmatch(r'agentos-validation-[a-f0-9-]{36}\.service',unit):raise ValueError('VALIDATION_UNIT_IDENTITY')
 try:
  response=subprocess.run(['systemctl','show',unit,'--property=LoadState,ActiveState,MainPID,ControlGroup'],check=True,capture_output=True,text=True,timeout=5)
  state=dict(line.split('=',1) for line in response.stdout.splitlines() if '=' in line)
  group='/system.slice/'+unit
  if state.get('LoadState') not in ('loaded','not-found') or state.get('ActiveState') not in ('inactive','failed') or state.get('MainPID')!='0' or state.get('ControlGroup') not in ('',group):raise ValueError('VALIDATION_STOP_UNCONFIRMED')
  events=pathlib.Path('/sys/fs/cgroup'+group+'/cgroup.events')
  if events.exists():
   if not re.search(r'^populated 0$',events.read_text(),re.M):raise ValueError('VALIDATION_STOP_UNCONFIRMED')
  elif state.get('ControlGroup'):raise ValueError('VALIDATION_STOP_UNCONFIRMED')
  return {'unit':unit,'mainPID':0,'controlGroup':state['ControlGroup'],'populated':False,
          'activeState':state['ActiveState'],'loadState':state['LoadState']}
 except (OSError,subprocess.SubprocessError):raise ValueError('VALIDATION_STOP_UNCONFIRMED')


def stop_and_confirm(unit):
 try:subprocess.run(['systemctl','stop','--no-block',unit],capture_output=True,timeout=5)
 except (OSError,subprocess.SubprocessError):pass
 deadline=time.monotonic()+20
 while True:
  try:return confirm_validation_stopped(unit)
  except ValueError:
   if time.monotonic()>=deadline:raise ValueError('VALIDATION_STOP_UNCONFIRMED')
   time.sleep(0.25)


def execute_validation(args,unit):
 timed_out=False
 try:
  result=subprocess.run(args,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,timeout=1830)
  checked=result.stdout;failed=result.returncode!=0
 except subprocess.TimeoutExpired as error:
  checked=error.stdout or '';failed=True;timed_out=True
  if isinstance(checked,bytes):checked=checked.decode('utf-8','replace')
  proof=stop_and_confirm(unit)
 except (OSError,subprocess.SubprocessError):
  # A lost client/pipe cannot establish whether the service was dispatched.
  # Attempt cleanup before reporting even a non-timeout transport failure.
  stop_and_confirm(unit)
  raise ValueError('VALIDATION_CLIENT_FAILED_AFTER_STOP')
 else:
  try:proof=confirm_validation_stopped(unit)
  except ValueError:proof=stop_and_confirm(unit)
 return {'output':checked,'failed':failed,'timedOut':timed_out,'stopProof':proof}


def verify_loaded_executor(raw):
 # Python loaded the module before we could acquire the registration lock.
 # Bind that already-loaded code to the exact registered source read under lock.
 loaded=compile(raw,__file__,'exec',dont_inherit=True,optimize=sys.flags.optimize)
 if loaded!=_LOADED_MODULE_CODE:raise ValueError('VALIDATION_EXECUTOR_CHANGED_AT_STARTUP')
 return hashlib.sha256(raw).hexdigest()


def acquire_execution_authority():
 global _REGISTRATION_LOCK,_EXECUTOR_SHA
 if _REGISTRATION_LOCK is not None:raise ValueError('VALIDATION_AUTHORITY_ALREADY_ACQUIRED')
 if REGISTRATION_LOCK_PATH.is_symlink():raise ValueError('VALIDATION_REGISTRATION_LOCK_UNPROTECTED')
 path=protected_tool(REGISTRATION_LOCK_PATH)
 fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW)
 try:
  info=os.fstat(fd)
  if not stat.S_ISREG(info.st_mode) or info.st_uid!=0 or info.st_mode&0o022 or info.st_nlink!=1:raise ValueError('VALIDATION_REGISTRATION_LOCK_UNPROTECTED')
  fcntl.flock(fd,fcntl.LOCK_SH)
  # The registrar never replaces this lock inode. Check the path still names it.
  current=path.lstat()
  if (current.st_dev,current.st_ino)!=(info.st_dev,info.st_ino):raise ValueError('VALIDATION_REGISTRATION_LOCK_CHANGED')
  executor=protected_tool(pathlib.Path(__file__))
  _EXECUTOR_SHA=verify_loaded_executor(executor.read_bytes())
  _REGISTRATION_LOCK=fd
 except BaseException:
  os.close(fd)
  raise


def trusted_origin_module():
 acquire_execution_authority()
 path=pathlib.Path(__file__).resolve().with_name('candidate_origin.py')
 # Verify raw bytes before importing; the candidate's sys.path is never searched.
 if path.is_symlink():raise ValueError('VALIDATION_ORIGIN_BUNDLE_UNPROTECTED')
 path=protected_tool(path)
 raw=path.read_bytes()
 if hashlib.sha256(raw).hexdigest()!=ORIGIN_MODULE_SHA:raise ValueError('VALIDATION_ORIGIN_BUNDLE_CHANGED')
 module=types.ModuleType('registered_candidate_origin');module.__file__=str(path)
 # Execute the exact pinned bytes already read, never a second path lookup.
 exec(compile(raw,str(path),'exec'),module.__dict__)
 module.require_registered_bundle(pathlib.Path(__file__).resolve(),_EXECUTOR_SHA,ORIGIN_MODULE_SHA)
 return module

def validation_complete(output, require_build=True, report_path=None):
 text=re.sub(r'\x1b\[[0-?]*[ -/]*[@-~]','',output)
 if require_build and not re.search(r'\bbuilt in\s+\S+',text):return False
 for label in ('Test Files','Tests'):
  lines=re.findall(r'^\s*'+label+r'\s+(.+)$',text,flags=re.MULTILINE)
  if not lines:return False
  summary=lines[-1].strip();total=re.search(r'\((\d+)\)\s*$',summary)
  terms=re.findall(r'(\d+)\s+(passed|skipped|todo)',summary)
  if not total or not any(kind=='passed' and int(n)>0 for n,kind in terms):return False
  residue=re.sub(r'\d+\s+(?:passed|skipped|todo)|\(\d+\)|[|\s]','',summary)
  if residue or sum(int(n) for n,_ in terms)!=int(total.group(1)):return False
 durations=list(re.finditer(r'^\s*Duration\s+[^\n]+',text,flags=re.MULTILINE))
 if not durations:return False
 tail=text[durations[-1].end():].strip()
 if report_path is not None and tail=='JSON report written to '+str(report_path):tail=''
 return not tail

def coverage_requirements(raw):
 if raw is None:return None
 if not isinstance(raw,dict) or set(raw)!={'version','profile','candidateDigest','requiredDatabaseTests'}:raise ValueError('VALIDATION_COVERAGE_CONTRACT')
 if type(raw['version']) is not int or raw['version']!=1 or raw['profile']!='agentos.validate' or not isinstance(raw['candidateDigest'],str) or not re.fullmatch('[a-f0-9]{64}',raw['candidateDigest']):raise ValueError('VALIDATION_COVERAGE_CONTRACT')
 tests=raw['requiredDatabaseTests']
 if not isinstance(tests,list) or not 1<=len(tests)<=256:raise ValueError('VALIDATION_COVERAGE_OBLIGATIONS')
 seen=set()
 for test in tests:
  if not isinstance(test,dict) or set(test)!={'file','name'}:raise ValueError('VALIDATION_COVERAGE_OBLIGATIONS')
  file,name=test['file'],test['name']
  if not isinstance(file,str) or len(file)>300 or not re.fullmatch(r'tests/(?:[a-zA-Z0-9_-]+/)*[a-zA-Z0-9_-]+(?:\.[a-zA-Z0-9_-]+)*\.test\.[cm]?[jt]s',file):raise ValueError('VALIDATION_COVERAGE_TEST_PATH')
  if not isinstance(name,str) or not 1<=len(name)<=500 or '\0' in name:raise ValueError('VALIDATION_COVERAGE_TEST_NAME')
  key=(file,name)
  if key in seen:raise ValueError('VALIDATION_COVERAGE_DUPLICATE_OBLIGATION')
  seen.add(key)
 return raw

def canonical_digest(value):
 return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()).hexdigest()

def parse_test_report(raw,build):
 if len(raw)>8*1024*1024:raise ValueError('VALIDATION_REPORT_TOO_LARGE')
 try:report=json.loads(raw)
 except (ValueError,UnicodeDecodeError):raise ValueError('VALIDATION_REPORT_JSON')
 if not isinstance(report,dict) or type(report.get('success')) is not bool or not isinstance(report.get('testResults'),list):raise ValueError('VALIDATION_REPORT_SCHEMA')
 build=pathlib.Path(build).absolute()
 tests={};files=set();file_failed=False
 statuses={'passed':'passed','failed':'failed','pending':'skipped','skipped':'skipped','todo':'skipped','disabled':'skipped'}
 for suite in report['testResults']:
  if not isinstance(suite,dict) or not isinstance(suite.get('name'),str) or not isinstance(suite.get('assertionResults'),list) or suite.get('status') not in ('passed','failed'):raise ValueError('VALIDATION_REPORT_SUITE')
  path=pathlib.Path(suite['name'])
  try:relative=path.relative_to(build)
  except ValueError:raise ValueError('VALIDATION_REPORT_PATH')
  if not path.is_absolute() or str(build/relative)!=suite['name'] or any(p in ('','.','..') for p in relative.parts):raise ValueError('VALIDATION_REPORT_PATH')
  file=relative.as_posix()
  if file in files:raise ValueError('VALIDATION_REPORT_DUPLICATE_FILE')
  files.add(file);file_failed=file_failed or suite['status']=='failed'
  for case in suite['assertionResults']:
   if not isinstance(case,dict) or not isinstance(case.get('fullName'),str) or not case['fullName'] or case.get('status') not in statuses:raise ValueError('VALIDATION_REPORT_CASE')
   key=(file,case['fullName'])
   if key in tests:raise ValueError('VALIDATION_REPORT_DUPLICATE_CASE')
   tests[key]=statuses[case['status']]
 for field,count in [('numTotalTests',len(tests)),('numPassedTests',sum(s=='passed' for s in tests.values())),('numFailedTests',sum(s=='failed' for s in tests.values()))]:
  if type(report.get(field)) is not int or report[field]!=count:raise ValueError('VALIDATION_REPORT_COUNTS')
 if report['success'] and (file_failed or 'failed' in tests.values()):raise ValueError('VALIDATION_REPORT_CONTRADICTION')
 return report,tests

def test_evidence(build,parameters):
 requirements=coverage_requirements(parameters.get('validationCoverage'))
 path=build/'.local-test/vitest-results.json'
 if any(part.is_symlink() for part in [path,*path.parents]):raise ValueError('VALIDATION_REPORT_MISSING_OR_LINK')
 try:reports=checked_tree(path.parent,lambda relative,name:True,lambda relative,name:relative!='vitest-results.json')
 except (OSError,ValueError):raise ValueError('VALIDATION_REPORT_MISSING_OR_LINK')
 if 'vitest-results.json' not in reports:raise ValueError('VALIDATION_REPORT_MISSING_OR_LINK')
 raw=reports['vitest-results.json'];report,tests=parse_test_report(raw,build)
 report_digest=hashlib.sha256(raw).hexdigest()
 artifacts=[{'name':'vitest-results.json','mime':'application/json','content':raw.decode('utf-8')}]
 coverage=None
 if requirements:
  coverage={'version':1,'source':'registered_vitest_json','profile':requirements['profile'],
   'candidateDigest':requirements['candidateDigest'],'reportRoot':str(build),
   'reportDigest':report_digest,'obligationsDigest':canonical_digest(requirements['requiredDatabaseTests']),
   'requiredTests':[dict(item,status=tests.get((item['file'],item['name']),'missing')) for item in requirements['requiredDatabaseTests']]}
  content=json.dumps(coverage,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False)
  artifacts.append({'name':'validation-coverage.json','mime':'application/json','content':content})
 passed=report['success'] and bool(tests) and (coverage is None or all(item['status']=='passed' for item in coverage['requiredTests']))
 return {'passed':passed,'reportDigest':report_digest,'coverage':coverage,'artifacts':artifacts}

def evidence_identity(evidence):
 if _EXECUTOR_SHA is None:raise ValueError('VALIDATION_AUTHORITY_NOT_ACQUIRED')
 identity={'executorSha256':_EXECUTOR_SHA}
 if evidence:
  identity['testReportDigest']=evidence['reportDigest']
  if evidence['coverage']:
   identity['coverageDigest']=hashlib.sha256(next(a['content'] for a in evidence['artifacts'] if a['name']=='validation-coverage.json').encode()).hexdigest()
 return identity
ROOT=pathlib.Path('/opt/agentos')
STATE=pathlib.Path('/var/lib/agentos/releases')
BUILD_ROOT=pathlib.Path('/var/lib/agentos-validation-builds')
NODE='/opt/deepseek-harness/node/bin'
def digest(root):
 h=hashlib.sha256()
 for p in sorted(root.rglob('*')):
  # Keep this contract identical to workerSourceDigest: generated Python caches
  # are mutable runtime data; source, documentation and built assets are signed.
  if p.is_file() and not p.is_symlink() and not any(x in p.relative_to(root).parts for x in ['node_modules','.local-test','.git','__pycache__']):
   h.update(str(p.relative_to(root)).encode());h.update(p.read_bytes())
 return h.hexdigest()
def run(args,**kw):
 return subprocess.run(args,check=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,**kw).stdout

def publish_source(candidate,record):
 workspace='agentos/src'+record['digest'][:16]
 source=pathlib.Path('/srv/agentos/workspaces')/workspace
 files=build_snapshot(candidate,include_outputs=True)
 if not source.exists():write_regular_files(source,files)
 elif build_snapshot(source,include_outputs=True)!=files:raise ValueError('PUBLISHED_SOURCE_CHANGED')
 value=json.dumps({'workspace':workspace,'release':str(candidate)})
 run(['runuser','-u','postgres','--','/usr/lib/postgresql/17/bin/psql','-p','15482','-d','agentos','-v','ON_ERROR_STOP=1','-c',"INSERT INTO settings(id,value) VALUES('agentos-source-workspace','"+value.replace("'","''")+"') ON CONFLICT(id) DO UPDATE SET value=excluded.value"])

def publication_executor(origin):
 # Registration remains locked for this entire process. Preserve the validator
 # identity in the old receipt; this is the independently registered publisher.
 bound=origin.require_registered_bundle(pathlib.Path(__file__).resolve(),_EXECUTOR_SHA,ORIGIN_MODULE_SHA)
 pointer={key:bound[key] for key in ('version','registrationId','registrationDigest')}
 record,ref=origin.document(origin.Layout().control/'validator-registrations'/(pointer['registrationId']+'.json'))
 if ref['sha256']!=pointer['registrationDigest'] or not re.fullmatch('[a-f0-9]{40}',record.get('sourceCommit','')):
  raise ValueError('PUBLICATION_EXECUTOR_UNBOUND')
 return {'path':str(pathlib.Path(__file__).resolve()),'sha256':_EXECUTOR_SHA,
         'sourceCommit':record['sourceCommit'],'registration':pointer}

ROLLBACK_READINESS_JS = r'''
// Imported source is the exact previously published release, never a CLI that
// might silently ignore --check. No installer or application runtime is called.
const {pool}=await import('./server/db.ts');
let db;
try {
 db=await pool.connect();
 await db.query('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');
 await db.query("SET LOCAL statement_timeout='5s'");
 await db.query("SET LOCAL lock_timeout='1s'");
 const gates=[['base','schema-readiness','requireSchemaReady'],
  ['backlog','backlog-schema-readiness','requireBacklogSchemaReady'],
  ['taskExecution','task-execution-metadata','requireTaskExecutionMetadataReady'],
  ['runCriteria','run-criteria-metadata','requireRunCriteriaMetadataReady'],
  ['panel','task-panel-metadata','requireTaskPanelMetadataReady']];
 for (const [,file,name] of gates) await (await import('./server/'+file+'.ts'))[name](db);
 await db.query('ROLLBACK');
 console.log(JSON.stringify({version:1,ok:true,readOnly:true,gates:gates.map(x=>x[0])}));
} catch {
 if(db) await db.query('ROLLBACK').catch(()=>{});
 console.error('ROLLBACK_SCHEMA_NOT_READY');process.exitCode=1;
} finally {db?.release();await pool.end();}
'''

def require_rollback_readiness(previous,record):
 previous=pathlib.Path(previous)
 origin=record.get('candidateOrigin',{}).get('published',{})
 expected=origin.get('releaseDigest')
 if (previous.parent!=ROOT/'releases' or previous.is_symlink() or
     str(previous)!=record.get('base') or origin.get('release')!=str(previous) or
     not isinstance(expected,str) or not re.fullmatch('[a-f0-9]{64}',expected) or digest(previous)!=expected):
  raise ValueError('ROLLBACK_SOURCE_UNBOUND')
 bundle=(previous/'node_modules').resolve(strict=True)
 node=protected_tool(pathlib.Path(NODE)/'node',root_owned=False,forbidden_uid=pwd.getpwnam('agentos-build').pw_uid)
 loader=protected_tool(bundle/'tsx/dist/loader.mjs')
 environment=protected_tool(pathlib.Path('/etc/agentos/server.env'))
 if not loader.is_relative_to(bundle):raise ValueError('ROLLBACK_LOADER_UNBOUND')
 # PGOPTIONS also makes transactions opened by imports read-only. The only
 # child is Node; bounded queries and finally/pool.end close its connections.
 result=subprocess.run([str(node),'--env-file='+str(environment),'--import',str(loader),
  '--input-type=module','-e',ROLLBACK_READINESS_JS],cwd=previous,
  env={'PATH':NODE+':/usr/bin:/bin','HOME':'/nonexistent','NODE_OPTIONS':'','NODE_PATH':'',
       'PGAPPNAME':'agentos-rollback-readiness','PGOPTIONS':'-c default_transaction_read_only=on -c statement_timeout=5000 -c lock_timeout=1000 -c idle_in_transaction_session_timeout=10000'},
  stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,timeout=40)
 expected_result={'version':1,'ok':True,'readOnly':True,'gates':['base','backlog','taskExecution','runCriteria','panel']}
 if result.returncode or result.stderr or json.loads(result.stdout)!=expected_result:raise ValueError('ROLLBACK_SCHEMA_NOT_READY')
 if digest(previous)!=expected or (ROOT/'current').resolve()!=pathlib.Path(record['candidate']):raise ValueError('ROLLBACK_SOURCE_CHANGED')
 return {**expected_result,'source':str(previous),'digest':expected,'checkedAt':time.time()}

def persist_rollback_observation(receipt,record,value):
 path=receipt.with_name(receipt.stem+'.rollback.json')
 value={'version':1,'candidate':record['candidate'],'digest':record['digest'],'base':record['base'],
        'validationReceiptSha256':hashlib.sha256(receipt.read_bytes()).hexdigest(),
        'publisher':record['publicationExecutor'],'observedAt':time.time(),**value}
 temporary=path.with_name(path.name+'.'+uuid.uuid4().hex+'.tmp')
 fd=os.open(temporary,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
 with os.fdopen(fd,'w') as stream:
  json.dump(value,stream,sort_keys=True,separators=(',',':'));stream.flush();os.fsync(stream.fileno())
 # An occurrence gets one immutable rollback observation. A lost response or
 # returned pointer cannot erase the previous failed publication history.
 os.link(temporary,path,follow_symlinks=False);os.unlink(temporary)
 fd=os.open(path.parent,os.O_RDONLY|os.O_DIRECTORY)
 try:os.fsync(fd)
 finally:os.close(fd)
def main():
 data=json.load(sys.stdin); action=sys.argv[1]
 if action not in {'validate','publish'}:raise ValueError('INVALID_ACTION')
 origin_module=trusted_origin_module()
 occurrence=str(uuid.UUID(data['occurrenceId']));STATE.mkdir(parents=True,exist_ok=True)
 receipt=STATE/(occurrence+'.json')
 if action=='publish':
  # Shared with worker rollout; retain the lock through swap, health and pending write.
  publication=open(STATE/'publication.lock','a');fcntl.flock(publication,fcntl.LOCK_EX)
 if action=='validate':
  requirements=coverage_requirements(data.get('parameters',{}).get('validationCoverage'))
  validation_base=str((ROOT/'current').resolve())
  workspace=next((r['output']['workspace'] for r in reversed(data['results']) if r.get('output',{}).get('workspace')),data.get('parameters',{}).get('workspace'))
  if not workspace or not workspace.startswith('agentos/oc'):raise ValueError('INVALID_SOURCE_WORKSPACE')
  source=(pathlib.Path('/srv/agentos/workspaces')/workspace).resolve()
  if not source.is_relative_to('/srv/agentos/workspaces/agentos'):raise ValueError('INVALID_SOURCE_PATH')
  attempt=str(uuid.UUID(data['attemptId']))
  build=ensure_build_root()/attempt
  if build.exists():raise ValueError('BUILD_ALREADY_EXISTS_RECONCILE')
  snapshot=workspace_source_snapshot(source)
  if requirements and requirements['candidateDigest']!=snapshot['candidateDigest']:raise ValueError('VALIDATION_CANDIDATE_DIGEST_CHANGED')
  candidate_origin=origin_module.capture_origin(workspace,snapshot['candidateDigest'])
  write_regular_files(build,snapshot['files'])
  # Use exactly the reviewed dependency graph; dependency changes require a prepared dependency bundle.
  if (build/'package-lock.json').read_bytes()!=pathlib.Path(validation_base,'package-lock.json').read_bytes():raise ValueError('DEPENDENCY_BUNDLE_REQUIRED')
  build_user=pwd.getpwnam('agentos-build')
  runners=trusted_runners(validation_base,pathlib.Path(NODE)/'node',build_user.pw_uid)
  runners_before=runner_identity(runners)
  writable=install_dependencies_and_outputs(build,validation_base,build_user.pw_uid,build_user.pw_gid)
  require_same_inputs(build,snapshot['files'])
  db=pathlib.Path('/etc/agentos/test-database-url').read_text().strip()
  serial=open(BUILD_ROOT/'validation.lock','a');fcntl.flock(serial,fcntl.LOCK_EX)
  env={'PATH':NODE+':/usr/bin:/bin','HOME':str(BUILD_ROOT),'AGENTOS_TEST_DATABASE_URL':db,'DATABASE_URL':db,'CI':'1','PYTHONDONTWRITEBYTECODE':'1'}
  unit=validation_unit(attempt)
  args=['systemd-run','--quiet','--pipe','--wait','--collect','--service-type=exec','--unit='+unit,'--uid=agentos-build',
   '--property=NoNewPrivileges=yes','--property=ProtectSystem=strict','--property=ProtectHome=yes',
   '--property=ReadWritePaths='+' '.join(writable),'--property=PrivateTmp=yes','--property=MemoryMax=4G','--property=CPUQuota=200%',
   '--property=RuntimeMaxSec=1800','--property=TimeoutStopSec=15','--property=KillMode=control-group','--property=SendSIGKILL=yes',
   '--property=PrivateNetwork=yes','--working-directory='+str(build)]
  for k,v in env.items():args+=['--setenv='+k+'='+v]
  # Every interpreter/runner resolves outside build to the reviewed dependency bundle.
  args+=['/bin/sh','-c',runner_command(runners)]
  execution=execute_validation(args,unit)
  checked,failed=execution['output'],execution['failed']
  execution_identity={'unit':unit,'stopProof':execution['stopProof'],'runners':runners_before}
  # A client response/exit is not stop proof. execute_validation verifies the
  # named unit and its cgroup before any report, source or generated file read.
  if runner_identity(trusted_runners(validation_base,pathlib.Path(NODE)/'node',build_user.pw_uid))!=runners_before:raise ValueError('VALIDATION_RUNNER_CHANGED')
  if workspace_source_snapshot(source)!=snapshot:raise ValueError('VALIDATION_FROZEN_SOURCE_CHANGED')
  build_input_digest=require_same_inputs(build,snapshot['files'])
  evidence=None;coverage_error=None
  try:evidence=test_evidence(build,data.get('parameters',{}))
  except ValueError as e:coverage_error=str(e)
  if failed or coverage_error or not evidence['passed']:
   print(json.dumps({'ok':True,'summary':'Validação falhou; consulte o log e a cobertura obrigatória.','output':{'passed':False,'coverageError':coverage_error,'validationExecution':execution_identity,**evidence_identity(evidence)},'artifacts':[{'name':'verification.json','content':'{"passed":false}'},{'name':'validation.log','mime':'text/plain','content':checked[-100000:]}]+(evidence['artifacts'] if evidence else [])}));return
  if not validation_complete(checked,report_path=build/'.local-test/vitest-results.json'):raise ValueError('VALIDATION_INCOMPLETE')
  if str((ROOT/'current').resolve())!=validation_base:raise ValueError('BASE_CHANGED_REVALIDATE')
  files=build_snapshot(build,include_outputs=True)
  require_same_inputs(build,snapshot['files'])
  stop_proof=confirm_validation_stopped(unit)
  candidate=ROOT/'releases'/('managed-'+attempt)
  write_regular_files(candidate,files)
  if build_snapshot(candidate,include_outputs=True)!=files:raise ValueError('VALIDATION_RELEASE_COPY_CHANGED')
  os.symlink(str(pathlib.Path(validation_base,'node_modules').resolve()),candidate/'node_modules')
  record={'candidate':str(candidate),'digest':digest(candidate),'base':validation_base,'validatedAt':time.time(),
          'sourceCandidateDigest':snapshot['candidateDigest'],'sourceManifestDigest':snapshot['sourceManifestDigest'],
          'buildInputManifestDigest':build_input_digest,
          'candidateOrigin':candidate_origin,'validationExecution':{'unit':unit,'stopProof':stop_proof,'runners':runners_before},**evidence_identity(evidence)}
  receipt.write_text(json.dumps(record));os.chmod(receipt,0o600)
  print(json.dumps({'ok':True,'summary':'Candidata validada: build e suíte executados isoladamente.','output':record,
   'artifacts':[{'name':'verification.json','content':'{"passed":true}'},{'name':'validation.log','mime':'text/plain','content':checked[-100000:]}]+evidence['artifacts']}))
 elif action=='publish':
  record=json.loads(receipt.read_text());candidate=pathlib.Path(record['candidate']);previous=(ROOT/'current').resolve()
  if digest(candidate)!=record['digest']:raise ValueError('CANDIDATE_CHANGED')
  if previous==candidate:
   # A completed replay is read-only. An interrupted prior publication needs
   # reconciliation rather than new effects under a stale origin assertion.
   if not record.get('publishedAt'):raise ValueError('PUBLICATION_EFFECT_UNCONFIRMED')
   print(json.dumps({'ok':True,'summary':'Versão já publicada.','output':record}));return
  if str(previous)!=record['base']:raise ValueError('BASE_CHANGED_REVALIDATE')
  if receipt.with_name(receipt.stem+'.rollback.json').exists():raise ValueError('PUBLICATION_ROLLBACK_RECONCILE')
  origin_module.require_publish_origin(record)
  record['publicationExecutor']=publication_executor(origin_module)
  swap=ROOT/('current-'+occurrence);os.symlink(candidate,swap);os.replace(swap,ROOT/'current')
  try:
   run(['systemctl','restart','agentos-api'])
   for attempt in range(30):
    try:
     with urllib.request.urlopen('http://127.0.0.1:4280/api/session',timeout=3) as response:
      if response.status==200:break
    except Exception:
     if attempt==29:raise
     time.sleep(1)
  except Exception:
   try:readiness=require_rollback_readiness(previous,record)
   except Exception:
    persist_rollback_observation(receipt,record,{'state':'blocked','code':'ROLLBACK_READINESS_UNPROVEN'})
    raise ValueError('PUBLICATION_FAILED_ROLLBACK_BLOCKED_RECONCILE') from None
   persist_rollback_observation(receipt,record,{'state':'ready','readiness':readiness})
   os.symlink(previous,swap);os.replace(swap,ROOT/'current');run(['systemctl','restart','agentos-api']);raise
  publish_source(candidate,record)
  record.update(publishedAt=time.time(),previous=str(previous),workersPendingRestart=True);receipt.write_text(json.dumps(record))
  (STATE/'pending-workers.json').write_text(json.dumps(record))
  print(json.dumps({'ok':True,'summary':'Interface publicada e verificada. Executores ativos preservados; atualização deles aguarda janela sem execuções.','output':record}))
 else:raise ValueError('INVALID_ACTION')
if __name__=='__main__':
 try:main()
 except Exception as e:
  # No process environment or command arguments in errors.
  print(type(e).__name__+': '+(str(e) if isinstance(e,ValueError) else 'ACTION_FAILED'),file=sys.stderr);sys.exit(1)
