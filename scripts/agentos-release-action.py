#!/usr/bin/env python3
"""Registered AgentOS action. Candidate checks run as a separate unprivileged user."""
import os, sys, json, pathlib, hashlib, shutil, subprocess, time, urllib.request, uuid, fcntl, re

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
 if not isinstance(tests,list) or not 1<=len(tests)<=128:raise ValueError('VALIDATION_COVERAGE_OBLIGATIONS')
 seen=set()
 for test in tests:
  if not isinstance(test,dict) or set(test)!={'file','name'}:raise ValueError('VALIDATION_COVERAGE_OBLIGATIONS')
  file,name=test['file'],test['name']
  if not isinstance(file,str) or len(file)>300 or not re.fullmatch(r'tests/[A-Za-z0-9_./-]+\.[cm]?[jt]sx?',file) or any(p in ('','.','..') for p in file.split('/')):raise ValueError('VALIDATION_COVERAGE_TEST_PATH')
  if not isinstance(name,str) or not 1<=len(name)<=2000 or '\0' in name:raise ValueError('VALIDATION_COVERAGE_TEST_NAME')
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
 if any(part.is_symlink() for part in [path,*path.parents]) or not path.is_file():raise ValueError('VALIDATION_REPORT_MISSING_OR_LINK')
 if path.stat().st_size>8*1024*1024:raise ValueError('VALIDATION_REPORT_TOO_LARGE')
 raw=path.read_bytes();report,tests=parse_test_report(raw,build)
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
 identity={'executorSha256':hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest()}
 if evidence:
  identity['testReportDigest']=evidence['reportDigest']
  if evidence['coverage']:
   identity['coverageDigest']=hashlib.sha256(next(a['content'] for a in evidence['artifacts'] if a['name']=='validation-coverage.json').encode()).hexdigest()
 return identity
ROOT=pathlib.Path('/opt/agentos')
STATE=pathlib.Path('/var/lib/agentos/releases')
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
 if not source.exists():
  shutil.copytree(candidate,source,ignore=shutil.ignore_patterns('node_modules','.local-test','test-results','playwright-report','.git','__pycache__'))
 value=json.dumps({'workspace':workspace,'release':str(candidate)})
 run(['runuser','-u','postgres','--','/usr/lib/postgresql/17/bin/psql','-p','15482','-d','agentos','-v','ON_ERROR_STOP=1','-c',"INSERT INTO settings(id,value) VALUES('agentos-source-workspace','"+value.replace("'","''")+"') ON CONFLICT(id) DO UPDATE SET value=excluded.value"])
def main():
 data=json.load(sys.stdin); action=sys.argv[1]
 occurrence=str(uuid.UUID(data['occurrenceId']));STATE.mkdir(parents=True,exist_ok=True)
 receipt=STATE/(occurrence+'.json')
 if action=='publish':
  # Shared with worker rollout; retain the lock through swap, health and pending write.
  publication=open(STATE/'publication.lock','a');fcntl.flock(publication,fcntl.LOCK_EX)
 if action=='validate':
  coverage_requirements(data.get('parameters',{}).get('validationCoverage'))
  validation_base=str((ROOT/'current').resolve())
  workspace=next((r['output']['workspace'] for r in reversed(data['results']) if r.get('output',{}).get('workspace')),data.get('parameters',{}).get('workspace'))
  if not workspace or not workspace.startswith('agentos/oc'):raise ValueError('INVALID_SOURCE_WORKSPACE')
  source=(pathlib.Path('/srv/agentos/workspaces')/workspace).resolve()
  if not source.is_relative_to('/srv/agentos/workspaces/agentos'):raise ValueError('INVALID_SOURCE_PATH')
  attempt=str(uuid.UUID(data['attemptId']))
  build=pathlib.Path('/var/lib/agentos-build')/attempt
  if build.exists():raise ValueError('BUILD_ALREADY_EXISTS_RECONCILE')
  build.mkdir(parents=True)
  for base,dirs,files in os.walk(source,followlinks=False):
   dirs[:]=[x for x in dirs if x not in ['node_modules','.git','.snapshot','evidence','data','dist','.local','.local-test','test-results','playwright-report','__pycache__']]
   for name in files:
    p=pathlib.Path(base)/name;rel=p.relative_to(source)
    if name.startswith('.env') or name=='__pycache__' or p.is_symlink():continue
    target=build/rel;target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(p,target)
  # Use exactly the reviewed dependency graph; dependency changes require a prepared dependency bundle.
  if (build/'package-lock.json').read_bytes()!=(ROOT/'current/package-lock.json').read_bytes():raise ValueError('DEPENDENCY_BUNDLE_REQUIRED')
  modules=build/'node_modules';modules.mkdir()
  for dependency in (ROOT/'current/node_modules').iterdir():
   if dependency.name not in ['.vite','.vite-temp']:
    os.symlink(str(dependency.resolve()),modules/dependency.name)
  for cache in ['.vite','.vite-temp']:(modules/cache).mkdir()
  (build/'.local-test').mkdir()
  run(['chown','-hR','agentos-build:agentos-build',str(build)])
  db=pathlib.Path('/etc/agentos/test-database-url').read_text().strip()
  serial=open('/var/lib/agentos-build/validation.lock','a');fcntl.flock(serial,fcntl.LOCK_EX)
  env={'PATH':NODE+':/usr/bin:/bin','HOME':'/var/lib/agentos-build','AGENTOS_TEST_DATABASE_URL':db,'DATABASE_URL':db,'CI':'1'}
  args=['systemd-run','--quiet','--pipe','--wait','--collect','--uid=agentos-build',
   '--property=NoNewPrivileges=yes','--property=ProtectSystem=strict','--property=ProtectHome=yes',
   '--property=ReadWritePaths='+str(build),'--property=PrivateTmp=yes','--property=MemoryMax=4G','--property=CPUQuota=200%',
   '--property=PrivateNetwork=yes','--working-directory='+str(build)]
  for k,v in env.items():args+=['--setenv='+k+'='+v]
  # Use the pinned runner, not a candidate's replacement package.json test command.
  args+=['/bin/sh','-c','node node_modules/typescript/bin/tsc --noEmit && node node_modules/vite/bin/vite.js build && node node_modules/vitest/vitest.mjs run --reporter=default --reporter=json --outputFile=.local-test/vitest-results.json']
  failed=False
  try:checked=run(args,timeout=1800)
  except subprocess.CalledProcessError as e:checked=e.stdout;failed=True
  evidence=None;coverage_error=None
  try:evidence=test_evidence(build,data.get('parameters',{}))
  except ValueError as e:coverage_error=str(e)
  if failed or coverage_error or not evidence['passed']:
   print(json.dumps({'ok':True,'summary':'Validação falhou; consulte o log e a cobertura obrigatória.','output':{'passed':False,'coverageError':coverage_error,**evidence_identity(evidence)},'artifacts':[{'name':'verification.json','content':'{"passed":false}'},{'name':'validation.log','mime':'text/plain','content':checked[-100000:]}]+(evidence['artifacts'] if evidence else [])}));return
  if not validation_complete(checked,report_path=build/'.local-test/vitest-results.json'):raise ValueError('VALIDATION_INCOMPLETE')
  if str((ROOT/'current').resolve())!=validation_base:raise ValueError('BASE_CHANGED_REVALIDATE')
  candidate=ROOT/'releases'/('managed-'+attempt)
  shutil.copytree(build,candidate,ignore=shutil.ignore_patterns('node_modules','.local-test','test-results','playwright-report','__pycache__'))
  os.symlink(str((ROOT/'current/node_modules').resolve()),candidate/'node_modules')
  record={'candidate':str(candidate),'digest':digest(candidate),'base':validation_base,'validatedAt':time.time(),**evidence_identity(evidence)}
  receipt.write_text(json.dumps(record));os.chmod(receipt,0o600)
  print(json.dumps({'ok':True,'summary':'Candidata validada: build e suíte executados isoladamente.','output':record,
   'artifacts':[{'name':'verification.json','content':'{"passed":true}'},{'name':'validation.log','mime':'text/plain','content':checked[-100000:]}]+evidence['artifacts']}))
 elif action=='publish':
  record=json.loads(receipt.read_text());candidate=pathlib.Path(record['candidate']);previous=(ROOT/'current').resolve()
  if previous==candidate:
   publish_source(candidate,record)
   print(json.dumps({'ok':True,'summary':'Versão já publicada.','output':record}));return
  if str(previous)!=record['base']:raise ValueError('BASE_CHANGED_REVALIDATE')
  if digest(candidate)!=record['digest']:raise ValueError('CANDIDATE_CHANGED')
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
