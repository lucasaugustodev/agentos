#!/usr/bin/env python3
"""Registered AgentOS action. Candidate checks run as a separate unprivileged user."""
import os, sys, json, pathlib, hashlib, shutil, subprocess, time, urllib.request, uuid, fcntl, re

def validation_complete(output, require_build=True):
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
 return bool(durations and not text[durations[-1].end():].strip())
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
  run(['chown','-hR','agentos-build:agentos-build',str(build)])
  db=pathlib.Path('/etc/agentos/test-database-url').read_text().strip()
  serial=open('/var/lib/agentos-build/validation.lock','a');fcntl.flock(serial,fcntl.LOCK_EX)
  env={'PATH':NODE+':/usr/bin:/bin','HOME':'/var/lib/agentos-build','AGENTOS_TEST_DATABASE_URL':db,'DATABASE_URL':db,'CI':'1'}
  args=['systemd-run','--quiet','--pipe','--wait','--collect','--uid=agentos-build',
   '--property=NoNewPrivileges=yes','--property=ProtectSystem=strict','--property=ProtectHome=yes',
   '--property=ReadWritePaths='+str(build),'--property=PrivateTmp=yes','--property=MemoryMax=4G','--property=CPUQuota=200%',
   '--property=PrivateNetwork=yes','--working-directory='+str(build)]
  for k,v in env.items():args+=['--setenv='+k+'='+v]
  args+=['/bin/sh','-c','npm run build && npm test']
  try:checked=run(args,timeout=1800)
  except subprocess.CalledProcessError as e:
   print(json.dumps({'ok':True,'summary':'Validação falhou; consulte o log.','output':{'passed':False},'artifacts':[{'name':'verification.json','content':'{"passed":false}'},{'name':'validation.log','mime':'text/plain','content':e.stdout[-100000:]}]}));return
  if not validation_complete(checked):raise ValueError('VALIDATION_INCOMPLETE')
  if str((ROOT/'current').resolve())!=validation_base:raise ValueError('BASE_CHANGED_REVALIDATE')
  candidate=ROOT/'releases'/('managed-'+attempt)
  shutil.copytree(build,candidate,ignore=shutil.ignore_patterns('node_modules','.local-test','test-results','playwright-report','__pycache__'))
  os.symlink(str((ROOT/'current/node_modules').resolve()),candidate/'node_modules')
  record={'candidate':str(candidate),'digest':digest(candidate),'base':validation_base,'validatedAt':time.time()}
  receipt.write_text(json.dumps(record));os.chmod(receipt,0o600)
  print(json.dumps({'ok':True,'summary':'Candidata validada: build e suíte executados isoladamente.','output':record,
   'artifacts':[{'name':'verification.json','content':'{"passed":true}'},{'name':'validation.log','mime':'text/plain','content':checked[-100000:]}]}))
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
