"""Own AgentOS snapshots only. Never copies source Git config, credentials or links."""
import contextlib, datetime, difflib, fcntl, hashlib, json, os, pathlib, re, shutil, subprocess, sys, uuid, stat
import importlib.util
_storage_spec=importlib.util.spec_from_file_location('agentos_storage_capacity',pathlib.Path(__file__).with_name('storage-capacity.py'))
storage_capacity=importlib.util.module_from_spec(_storage_spec);_storage_spec.loader.exec_module(storage_capacity)
ROOT=pathlib.Path('/srv/agentos/workspaces')
DATA=pathlib.Path('/var/lib/agentos-bridge')
SKIP={'.git','node_modules','evidence','data','.agentos-snapshot.json','.local-test','test-results','playwright-report','__pycache__'}
def digest_key(value): return hashlib.sha256(value.encode()).hexdigest()
def workspace(value):
    if not re.fullmatch(r'[a-z0-9_-]+/[a-z0-9_-]+',value): raise ValueError('INVALID_WORKSPACE')
    p=ROOT/value
    if p.exists() and p.resolve()!=p: raise ValueError('INVALID_WORKSPACE')
    if p.parent.resolve()!=p.parent or not p.parent.is_dir(): raise ValueError('INVALID_PROJECT')
    return p
@contextlib.contextmanager
def locks(names,shared=()):
    (DATA/'locks').mkdir(parents=True,exist_ok=True)
    handles=[]
    try:
        legacy=open(DATA/'worker.lock','a'); handles.append(legacy)
        fcntl.flock(legacy,fcntl.LOCK_SH|fcntl.LOCK_NB)
        for name in sorted(set(names)):
            h=open(DATA/'locks'/('workspace-'+digest_key(name)), 'a'); handles.append(h)
            fcntl.flock(h,(fcntl.LOCK_SH if name in shared else fcntl.LOCK_EX)|fcntl.LOCK_NB)
        yield
    except BlockingIOError: raise ValueError('SOURCE_BUSY')
    finally:
        for h in handles: h.close()
def ignored(directory,names):
    result=[]
    for name in names:
        p=pathlib.Path(directory)/name
        if name in SKIP or name.startswith('.env') or name in {'.ssh','.aws','.codex','.claude'}: result.append(name)
        elif p.is_symlink(): raise ValueError('SOURCE_SYMLINK')
    return result
def sources(root):
    out={}
    for folder,dirs,files in os.walk(root,followlinks=False):
        dirs[:]=[d for d in sorted(dirs) if d not in SKIP and d!='assets' and not d.startswith('.env')]
        for d in dirs:
            if (pathlib.Path(folder)/d).is_symlink(): raise ValueError('SOURCE_SYMLINK')
        for name in sorted(files):
            if name in SKIP or name.startswith('.env') or name.endswith('.log') or name.endswith('.pyc'): continue
            p=pathlib.Path(folder)/name
            fd=os.open(p,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
            with os.fdopen(fd,'rb') as f:
                if not stat.S_ISREG(os.fstat(f.fileno()).st_mode): raise ValueError('SOURCE_NOT_REGULAR')
                b=f.read(8*1024*1024+1)
            if len(b)>8*1024*1024: raise ValueError('SOURCE_TOO_LARGE')
            out[str(p.relative_to(root))]=b
    return out
def source_hash(files):
    return digest_key(json.dumps([(k,hashlib.sha256(v).hexdigest()) for k,v in sorted(files.items())],separators=(',',':')))

def copy_reservation(source):
    copied=0;baseline=0
    for folder,dirs,files in os.walk(source,followlinks=False):
        excluded=set(ignored(folder,dirs+files))
        dirs[:]=[d for d in dirs if d not in excluded]
        for name in files:
            if name in excluded: continue
            p=pathlib.Path(folder)/name;info=p.stat(follow_symlinks=False)
            if not stat.S_ISREG(info.st_mode): raise ValueError('SOURCE_NOT_REGULAR')
            amount=max(4096,info.st_size);copied+=amount
            if 'assets' not in p.relative_to(source).parts and not name.endswith('.log'): baseline+=amount
    return copied+2*baseline+16*1024*1024
def source_snapshot_identity(source_id,source):
    marker=source/'.agentos-snapshot.json'
    try: info=marker.lstat()
    except FileNotFoundError: return None
    if not stat.S_ISREG(info.st_mode) or info.st_uid!=os.geteuid() or info.st_mode&0o022:
        raise ValueError('SOURCE_SNAPSHOT_UNPROTECTED')
    fd=os.open(marker,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    with os.fdopen(fd,'rb') as handle:
        raw=handle.read(128*1024+1)
    if len(raw)>128*1024 or json.loads(raw).get('workspace')!=source_id:
        raise ValueError('SOURCE_SNAPSHOT_INVALID')
    return hashlib.sha256(raw).hexdigest()

def make_new_clone_editable(staging):
    """Normalize only the newly copied tree, before protected metadata exists.

    copytree preserves immutable release modes. Workers must be able to edit
    their clone without DAC_OVERRIDE; executable and private-file modes remain
    intact. Source, existing workspaces, baselines and markers are never passed
    here. Dependencies and protected metadata are installed after this step.
    """
    for folder,dirs,files in os.walk(staging,followlinks=False):
        directory=pathlib.Path(folder);info=directory.lstat()
        if not stat.S_ISDIR(info.st_mode): raise ValueError('SOURCE_NOT_REGULAR')
        directory.chmod(stat.S_IMODE(info.st_mode)|0o700)
        for name in dirs:
            if not stat.S_ISDIR((directory/name).lstat().st_mode): raise ValueError('SOURCE_SYMLINK')
        for name in files:
            path=directory/name;info=path.lstat()
            if not stat.S_ISREG(info.st_mode): raise ValueError('SOURCE_NOT_REGULAR')
            path.chmod(stat.S_IMODE(info.st_mode)|0o600)

def prepare(params):
    target_id,source_id=params['workspace'],params['sourceWorkspace']
    target,source=workspace(target_id),workspace(source_id)
    if target==source or target.parent!=source.parent or not target.name.startswith('o'): raise ValueError('INVALID_TARGET')
    with locks([target_id,source_id],shared=[source_id]):
        marker=target/'.agentos-snapshot.json'
        if target.exists():
            state=json.loads(marker.read_text())
            if state['sourceWorkspace']!=source_id or state.get('workspace')!=target_id: raise ValueError('SNAPSHOT_CONFLICT')
            return state
        if not source.is_dir(): raise ValueError('SOURCE_MISSING')
        parent_snapshot=source_snapshot_identity(source_id,source)
        with storage_capacity.reserve(ROOT,DATA,copy_reservation(source)):
            staging=target.parent/('.prepare-'+uuid.uuid4().hex)
            baseline_staging=DATA/'workspace-baselines'/('.prepare-'+uuid.uuid4().hex)
            try:
                shutil.copytree(source,staging,ignore=ignored)
                make_new_clone_editable(staging)
                project_deps=pathlib.Path('/opt/agentos-project-deps')/target_id.split('/')[0]/'node_modules'
                (staging/'node_modules').symlink_to(str(project_deps) if project_deps.is_dir() else '/opt/agentos-runtime/node_modules',target_is_directory=True)
                (staging/'evidence').mkdir(exist_ok=True); (staging/'data').mkdir(exist_ok=True)
                ignore=staging/'.gitignore'
                existing=ignore.read_text() if ignore.exists() else ''
                needed=[x for x in ['assets/','evidence/','data/','node_modules/','.agentos-snapshot.json'] if x not in existing.splitlines()]
                if needed: ignore.write_text(existing+'\n'+'\n'.join(needed)+'\n')
                files=sources(staging); baseline=DATA/'workspace-baselines'/digest_key(target_id)
                baseline_staging.mkdir(parents=True,exist_ok=False)
                for name,b in files.items():
                    dest=baseline_staging/name; dest.parent.mkdir(parents=True,exist_ok=True); dest.write_bytes(b)
                env={**os.environ,'GIT_CONFIG_NOSYSTEM':'1','GIT_CONFIG_GLOBAL':'/dev/null','GIT_AUTHOR_NAME':'AgentOS','GIT_AUTHOR_EMAIL':'agentos@localhost','GIT_COMMITTER_NAME':'AgentOS','GIT_COMMITTER_EMAIL':'agentos@localhost'}
                for args in [['init','-q'],['-c','core.hooksPath=/dev/null','add','.'],['-c','core.hooksPath=/dev/null','commit','-qm','Isolated objective baseline','--allow-empty']]:
                    subprocess.run(['git',*args],cwd=staging,env=env,check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
                state={'workspace':target_id,'sourceWorkspace':source_id,'baseDigest':source_hash(files),'createdAt':datetime.datetime.now(datetime.timezone.utc).isoformat()}
                if source_snapshot_identity(source_id,source)!=parent_snapshot: raise ValueError('SOURCE_SNAPSHOT_CHANGED')
                if parent_snapshot is not None: state['sourceSnapshotDigest']=parent_snapshot
                (staging/'.agentos-snapshot.json').write_text(json.dumps(state))
                # Preserve a previous failed preparation's baseline separately;
                # stale files must never silently join the new source manifest.
                if baseline.exists(): baseline.rename(baseline.parent/('orphan-'+digest_key(target_id)+'-'+uuid.uuid4().hex))
                baseline_staging.rename(baseline)
                staging.rename(target)
                return state
            finally:
                # Only this invocation's unpublished staging tree, while holding
                # its source/target locks. Existing workspaces/evidence are kept.
                if staging.exists():
                    if staging.is_symlink() or staging.resolve().parent!=target.parent.resolve() or not re.fullmatch(r'\.prepare-[0-9a-f]{32}',staging.name): raise ValueError('INVALID_STAGING')
                    shutil.rmtree(staging)
                if baseline_staging.exists():
                    if baseline_staging.is_symlink() or baseline_staging.resolve().parent!=(DATA/'workspace-baselines').resolve(): raise ValueError('INVALID_STAGING')
                    shutil.rmtree(baseline_staging)
def inspect(params):
    target_id=params['workspace']; root=workspace(target_id)
    with locks([target_id]):
        baseline=DATA/'workspace-baselines'/digest_key(target_id)
        before,after=sources(baseline),sources(root)
        patch=[]
        for name in sorted(set(before)|set(after)):
            if before.get(name)==after.get(name): continue
            try: a=before.get(name,b'').decode('utf8').splitlines(keepends=True); b=after.get(name,b'').decode('utf8').splitlines(keepends=True)
            except UnicodeDecodeError: raise ValueError('PATCH_BINARY_SOURCE')
            for line in difflib.unified_diff(a,b,fromfile='a/'+name if name in before else '/dev/null',tofile='b/'+name if name in after else '/dev/null'):
                patch.append(line if line.endswith('\n') else line+'\n\\ No newline at end of file\n')
        return {'baseDigest':source_hash(before),'candidateDigest':source_hash(after),'files':[{'path':k,'sha256':hashlib.sha256(v).hexdigest()} for k,v in sorted(after.items())],'patch':''.join(patch)}
if __name__=='__main__':
    try:
        params=json.load(sys.stdin)
        action=params.get('action','prepare')
        if action=='compose':
            from workspace_compose import compose
            result=compose(params,workspace,locks,sources,source_hash,DATA)
        elif action=='prepare': result=prepare(params)
        elif action=='inspect': result=inspect(params)
        elif action=='capacity': result=storage_capacity.capacity(ROOT,DATA)
        else: raise ValueError('INVALID_ACTION')
        print(json.dumps(result))
    except Exception as e:
        print(json.dumps({'error':str(e) if isinstance(e,ValueError) else type(e).__name__}));sys.exit(1)
