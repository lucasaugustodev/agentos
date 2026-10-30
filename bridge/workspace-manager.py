"""Own AgentOS snapshots only. Never copies source Git config, credentials or links."""
import base64, contextlib, datetime, difflib, fcntl, hashlib, json, os, pathlib, re, shutil, subprocess, sys, uuid, stat
import importlib.util
_storage_spec=importlib.util.spec_from_file_location('agentos_storage_capacity',pathlib.Path(__file__).with_name('storage-capacity.py'))
storage_capacity=importlib.util.module_from_spec(_storage_spec);_storage_spec.loader.exec_module(storage_capacity)
ROOT=pathlib.Path('/srv/agentos/workspaces')
DATA=pathlib.Path('/var/lib/agentos-bridge')
# Production requires protected ancestors through the filesystem root. Isolated
# fixtures replace this anchor with their own temporary, owned directory.
PROTECTION_ROOT=pathlib.Path('/')
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

def preparation_binding(params):
    keys=('expectedSourceDigest','expectedSourceSnapshotDigest','preparationIdentity')
    if not any(key in params for key in keys): return None
    if not all(isinstance(params.get(key),str) for key in keys):
        raise ValueError('PREPARATION_BINDING_INVALID')
    if not all(re.fullmatch(r'[0-9a-f]{64}',params[key]) for key in keys[:2]):
        raise ValueError('PREPARATION_BINDING_INVALID')
    try: valid=str(uuid.UUID(params['preparationIdentity']))==params['preparationIdentity']
    except ValueError: valid=False
    if not valid: raise ValueError('PREPARATION_BINDING_INVALID')
    return {'version':1,'preparationIdentity':params['preparationIdentity'],
            'sourceWorkspace':params['sourceWorkspace'],'targetWorkspace':params['workspace'],
            'expectedSourceDigest':params['expectedSourceDigest'],
            'expectedSourceSnapshotDigest':params['expectedSourceSnapshotDigest']}

def preparation_digest(value):
    return digest_key(json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False))

def protected_preparation_directory(path,create=False):
    path=pathlib.Path(path)
    if path!=path.absolute() or path.resolve()!=path or not path.is_relative_to(PROTECTION_ROOT):
        raise ValueError('PREPARATION_DIRECTORY_UNPROTECTED')
    if create and not path.exists():
        protected_preparation_directory(path.parent)
        path.mkdir(mode=0o700)
    current=path
    while True:
        info=current.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid!=os.geteuid() or info.st_mode&0o022:
            raise ValueError('PREPARATION_DIRECTORY_UNPROTECTED')
        if current==PROTECTION_ROOT: break
        current=current.parent
    return path

def read_preparation_json(path,immutable=False,required_mode=None,include_raw=False):
    try: fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    except FileNotFoundError: raise ValueError('PREPARATION_RECEIPT_MISSING')
    except OSError: raise ValueError('PREPARATION_RECEIPT_UNPROTECTED')
    with os.fdopen(fd,'rb') as handle:
        before=os.fstat(handle.fileno())
        if (not stat.S_ISREG(before.st_mode) or before.st_uid!=os.geteuid() or before.st_nlink!=1
                or before.st_mode&(0o222 if immutable else 0o022)
                or (required_mode is not None and stat.S_IMODE(before.st_mode)!=required_mode)):
            raise ValueError('PREPARATION_RECEIPT_UNPROTECTED')
        raw=handle.read(128*1024+1)
        after=os.fstat(handle.fileno())
    identity=lambda info:(info.st_dev,info.st_ino,info.st_mode,info.st_uid,info.st_gid,
                          info.st_nlink,info.st_size,info.st_mtime_ns,info.st_ctime_ns)
    if identity(before)!=identity(after) or len(raw)>128*1024:
        raise ValueError('PREPARATION_RECEIPT_CHANGED')
    def unique_object(pairs):
        value={}
        for key,item in pairs:
            if key in value: raise ValueError('DUPLICATE_JSON_KEY')
            value[key]=item
        return value
    try: value=json.loads(raw,object_pairs_hook=unique_object)
    except (ValueError,UnicodeError): raise ValueError('PREPARATION_RECEIPT_INVALID')
    if not isinstance(value,dict): raise ValueError('PREPARATION_RECEIPT_INVALID')
    result=(value,hashlib.sha256(raw).hexdigest())
    return (*result,raw) if include_raw else result

def write_preparation_json(path,value):
    raw=json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()
    fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o444)
    with os.fdopen(fd,'wb') as handle:
        os.fchmod(handle.fileno(),0o444) # Explicit despite a restrictive umask.
        handle.write(raw);handle.flush();os.fsync(handle.fileno())

@contextlib.contextmanager
def preparation_locks(names,shared=()):
    directory=protected_preparation_directory(DATA/'locks',create=True)
    handles=[]
    try:
        requests=[(DATA/'worker.lock',True)]+[(directory/('workspace-'+digest_key(name)),name in shared)
                                             for name in sorted(set(names))]
        for path,is_shared in requests:
            try: fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_NOFOLLOW|os.O_NONBLOCK,0o600)
            except OSError: raise ValueError('PREPARATION_LOCK_UNPROTECTED')
            handle=os.fdopen(fd,'a');handles.append(handle);info=os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_uid!=os.geteuid() or info.st_nlink!=1 or info.st_mode&0o022:
                raise ValueError('PREPARATION_LOCK_UNPROTECTED')
            fcntl.flock(handle,(fcntl.LOCK_SH if is_shared else fcntl.LOCK_EX)|fcntl.LOCK_NB)
        yield
    except BlockingIOError: raise ValueError('SOURCE_BUSY')
    finally:
        for handle in handles: handle.close()

def flush_preparation_tree(root,names):
    directories={root}
    for name in names:
        path=root/name
        fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
        try:
            if not stat.S_ISREG(os.fstat(fd).st_mode): raise ValueError('SOURCE_NOT_REGULAR')
            os.fsync(fd)
        finally: os.close(fd)
        current=path.parent
        while current!=root:
            directories.add(current);current=current.parent
    for directory in sorted(directories,key=lambda path:len(path.parts),reverse=True):
        fd=os.open(directory,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        try: os.fsync(fd)
        finally: os.close(fd)

def preparation_source_matches(source,binding):
    marker,generation=read_preparation_json(source/'.agentos-snapshot.json')
    if marker.get('workspace')!=binding['sourceWorkspace'] or generation!=binding['expectedSourceSnapshotDigest']:
        raise ValueError('PREPARATION_SOURCE_GENERATION_CHANGED')
    if source_hash(sources(source))!=binding['expectedSourceDigest']:
        raise ValueError('PREPARATION_SOURCE_DIGEST_CHANGED')

def preparation_baseline_matches(baseline,expected):
    protected_preparation_directory(baseline)
    for folder,dirs,files in os.walk(baseline,followlinks=False):
        protected_preparation_directory(pathlib.Path(folder))
        for name in dirs:
            protected_preparation_directory(pathlib.Path(folder)/name)
        for name in files:
            info=(pathlib.Path(folder)/name).lstat()
            if not stat.S_ISREG(info.st_mode) or info.st_uid!=os.geteuid() or info.st_mode&0o022 or info.st_nlink!=1:
                raise ValueError('PREPARATION_BASELINE_UNPROTECTED')
    if source_hash(sources(baseline))!=expected:
        raise ValueError('PREPARATION_BASELINE_CHANGED')

def replay_bound_preparation(target,baseline,receipt_path,binding):
    state,_=read_preparation_json(target/'.agentos-snapshot.json',immutable=True)
    receipt,_=read_preparation_json(receipt_path,immutable=True)
    body={key:value for key,value in receipt.items() if key!='receiptDigest'}
    expected_keys=set(binding)|{'bindingDigest','observedSourceDigest','sourceSnapshotDigest',
                               'cloneBaseDigest','baselineDigest','createdAt','receiptDigest'}
    expected=binding['expectedSourceDigest'];generation=binding['expectedSourceSnapshotDigest']
    if (type(receipt.get('version')) is not int or set(receipt)!=expected_keys or any(receipt.get(key)!=value for key,value in binding.items())
            or receipt.get('bindingDigest')!=preparation_digest(binding)
            or receipt.get('receiptDigest')!=preparation_digest(body)
            or any(receipt.get(key)!=expected for key in ['observedSourceDigest','cloneBaseDigest','baselineDigest'])
            or receipt.get('sourceSnapshotDigest')!=generation
            or not isinstance(receipt.get('createdAt'),str)
            or state!={'workspace':binding['targetWorkspace'],'sourceWorkspace':binding['sourceWorkspace'],
                       'baseDigest':expected,'sourceSnapshotDigest':generation,'createdAt':receipt['createdAt'],
                       'preparation':{'version':1,'preparationIdentity':binding['preparationIdentity'],
                                      'bindingDigest':receipt['bindingDigest'],'receiptDigest':receipt['receiptDigest']}}):
        raise ValueError('PREPARATION_BINDING_MISMATCH')
    preparation_baseline_matches(baseline,expected)
    if source_hash(sources(target))!=expected:
        raise ValueError('PREPARATION_TARGET_CHANGED')
    return {**state,'preparationReceipt':receipt}

def prepare_bound(params,binding):
    target_id,source_id=params['workspace'],params['sourceWorkspace']
    target,source=workspace(target_id),workspace(source_id)
    if target==source or target.is_symlink() or target.parent!=source.parent or not target.name.startswith('o'):
        raise ValueError('INVALID_TARGET')
    protected_preparation_directory(DATA)
    receipts=protected_preparation_directory(DATA/'workspace-preparations',create=True)
    baselines=protected_preparation_directory(DATA/'workspace-baselines',create=True)
    receipt_path=receipts/(binding['preparationIdentity']+'.json')
    baseline=baselines/digest_key(target_id)
    with preparation_locks([target_id,source_id,'preparation:'+binding['preparationIdentity']],shared=[source_id]):
        if not source.is_dir(): raise ValueError('SOURCE_MISSING')
        preparation_source_matches(source,binding)
        if target.exists():
            result=replay_bound_preparation(target,baseline,receipt_path,binding)
            preparation_source_matches(source,binding)
            return result
        if receipt_path.exists():
            previous,_=read_preparation_json(receipt_path,immutable=True)
            if any(previous.get(key)!=value for key,value in binding.items()):
                raise ValueError('PREPARATION_BINDING_MISMATCH')
        if baseline.exists() or baseline.is_symlink() or receipt_path.exists() or receipt_path.is_symlink():
            raise ValueError('PREPARATION_PARTIAL_STATE')
        with storage_capacity.reserve(ROOT,DATA,copy_reservation(source)):
            staging=target.parent/('.prepare-'+uuid.uuid4().hex)
            baseline_staging=baselines/('.prepare-'+uuid.uuid4().hex)
            published=False
            try:
                shutil.copytree(source,staging,ignore=ignored)
                make_new_clone_editable(staging)
                ignore=staging/'.gitignore'
                existing=ignore.read_text() if ignore.exists() else ''
                if any(name not in existing.splitlines() for name in ['assets/','evidence/','data/','node_modules/','.agentos-snapshot.json']):
                    raise ValueError('PREPARATION_SOURCE_NORMALIZATION')
                project_deps=pathlib.Path('/opt/agentos-project-deps')/target_id.split('/')[0]/'node_modules'
                (staging/'node_modules').symlink_to(str(project_deps) if project_deps.is_dir() else '/opt/agentos-runtime/node_modules',target_is_directory=True)
                (staging/'evidence').mkdir(exist_ok=True);(staging/'data').mkdir(exist_ok=True)
                files=sources(staging);expected=binding['expectedSourceDigest']
                if source_hash(files)!=expected: raise ValueError('PREPARATION_CLONE_CHANGED')
                preparation_source_matches(source,binding)
                baseline_staging.mkdir(mode=0o700)
                for name,raw in files.items():
                    dest=baseline_staging/name;parent=baseline_staging
                    for part in pathlib.Path(name).parts[:-1]:
                        parent=parent/part;parent.mkdir(mode=0o700,exist_ok=True)
                    fd=os.open(dest,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o444)
                    with os.fdopen(fd,'wb') as handle:
                        os.fchmod(handle.fileno(),0o444);handle.write(raw)
                env={**os.environ,'GIT_CONFIG_NOSYSTEM':'1','GIT_CONFIG_GLOBAL':'/dev/null','GIT_AUTHOR_NAME':'AgentOS','GIT_AUTHOR_EMAIL':'agentos@localhost','GIT_COMMITTER_NAME':'AgentOS','GIT_COMMITTER_EMAIL':'agentos@localhost'}
                for args in [['init','-q'],['-c','core.hooksPath=/dev/null','add','.'],['-c','core.hooksPath=/dev/null','commit','-qm','Isolated objective baseline','--allow-empty']]:
                    subprocess.run(['git',*args],cwd=staging,env=env,check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
                if source_hash(sources(staging))!=expected: raise ValueError('PREPARATION_CLONE_CHANGED')
                preparation_source_matches(source,binding)
                created=datetime.datetime.now(datetime.timezone.utc).isoformat()
                receipt={**binding,'bindingDigest':preparation_digest(binding),'observedSourceDigest':expected,
                         'sourceSnapshotDigest':binding['expectedSourceSnapshotDigest'],
                         'cloneBaseDigest':expected,'baselineDigest':expected,'createdAt':created}
                receipt['receiptDigest']=preparation_digest(receipt)
                state={'workspace':target_id,'sourceWorkspace':source_id,'baseDigest':expected,
                       'sourceSnapshotDigest':binding['expectedSourceSnapshotDigest'],'createdAt':created,
                       'preparation':{'version':1,'preparationIdentity':binding['preparationIdentity'],
                                      'bindingDigest':receipt['bindingDigest'],'receiptDigest':receipt['receiptDigest']}}
                write_preparation_json(staging/'.agentos-snapshot.json',state)
                preparation_baseline_matches(baseline_staging,expected)
                flush_preparation_tree(staging,[*files,'.agentos-snapshot.json'])
                flush_preparation_tree(baseline_staging,files)
                if source_hash(sources(staging))!=expected: raise ValueError('PREPARATION_CLONE_CHANGED')
                preparation_source_matches(source,binding)
                # Reserve the identity durably before either destination rename.
                # A receipt alone is not completion: replay also requires the
                # exact protected marker, baseline and destination bytes.
                published=True
                write_preparation_json(receipt_path,receipt)
                flush_preparation_tree(receipts,[])
                baseline_staging.rename(baseline)
                flush_preparation_tree(baselines,[])
                staging.rename(target)
                flush_preparation_tree(target.parent,[])
                return {**state,'preparationReceipt':receipt}
            except Exception:
                if published: raise ValueError('PREPARATION_EFFECT_UNCONFIRMED')
                raise
            finally:
                # Only unpublished staging paths created by this invocation.
                # Published partial state is never deleted or silently replaced.
                for path,parent in [(staging,target.parent),(baseline_staging,baselines)]:
                    if path.exists():
                        if path.is_symlink() or path.resolve().parent!=parent.resolve() or not re.fullmatch(r'\.prepare-[0-9a-f]{32}',path.name):
                            raise ValueError('INVALID_STAGING')
                        shutil.rmtree(path)

def prepare(params):
    binding=preparation_binding(params)
    if binding is not None: return prepare_bound(params,binding)
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
def source_binding_marker(marker,workspace_id):
    required={'workspace','sourceWorkspace','baseDigest','createdAt'}
    allowed=required|{'sourceSnapshotDigest','preparation'}
    valid_hash=lambda value:isinstance(value,str) and re.fullmatch(r'[0-9a-f]{64}',value) is not None
    if (not required.issubset(marker) or not set(marker).issubset(allowed) or marker['workspace']!=workspace_id
            or not isinstance(marker['sourceWorkspace'],str)
            or not re.fullmatch(r'[a-z0-9_-]+/[a-z0-9_-]+',marker['sourceWorkspace'])
            or marker['sourceWorkspace'].split('/')[0]!=workspace_id.split('/')[0]
            or not valid_hash(marker['baseDigest']) or not isinstance(marker['createdAt'],str)
            or not re.fullmatch(r'\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,6})?(?:Z|\+00:00)',marker['createdAt'])
            or ('sourceSnapshotDigest' in marker and not valid_hash(marker['sourceSnapshotDigest']))):
        raise ValueError('SOURCE_BINDING_MARKER_INVALID')
    if 'preparation' in marker:
        value=marker['preparation']
        if (not isinstance(value,dict) or set(value)!={'version','preparationIdentity','bindingDigest','receiptDigest'}
                or type(value['version']) is not int or value['version']!=1
                or not source_binding_uuid(value['preparationIdentity'])
                or not all(valid_hash(value[key]) for key in ['bindingDigest','receiptDigest'])):
            raise ValueError('SOURCE_BINDING_MARKER_INVALID')

def source_binding_uuid(value):
    return isinstance(value,str) and re.fullmatch(r'[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}',value) is not None

def inspect_source_binding(params):
    run_id=params['sourceBindingRunId'];workspace_id=params['workspace']
    if not source_binding_uuid(run_id): raise ValueError('SOURCE_BINDING_IDENTITY_INVALID')
    target=workspace(workspace_id)
    protected_preparation_directory(DATA)
    run_directory=protected_preparation_directory(DATA/'worker-runs'/run_id)
    protected_preparation_directory(target)
    with preparation_locks([workspace_id]):
        state,state_digest=read_preparation_json(run_directory/'state.json',required_mode=0o600)
        policy,policy_digest=read_preparation_json(run_directory/'operation-policy.json',required_mode=0o600)
        marker,marker_digest,marker_raw=read_preparation_json(target/'.agentos-snapshot.json',include_raw=True)
        source_binding_marker(marker,workspace_id)
        if (state.get('runId')!=run_id or state.get('workspace')!=workspace_id or state.get('runtime')!='harness'
                or state.get('state')!='completed' or state.get('stopped') is not True):
            raise ValueError('SOURCE_BINDING_RUN_NOT_COMPLETED')
        if (type(policy.get('version')) is not int or policy.get('version')!=1
                or policy.get('runId')!=run_id or policy.get('workspace')!='/workspace'
                or not all(source_binding_uuid(policy.get(key)) for key in ['goalId','taskId','runId','runFence','contextVersion'])
                or state.get('runFence')!=policy['runFence']):
            raise ValueError('SOURCE_BINDING_POLICY_IDENTITY')
        if policy.get('sourceSnapshotDigest')!=marker_digest:
            raise ValueError('SOURCE_BINDING_GENERATION_CHANGED')
        artifacts=state.get('artifacts');canonical=[]
        if not isinstance(artifacts,list) or len(artifacts)>512: raise ValueError('SOURCE_BINDING_RUNTIME_ARTIFACTS')
        for name,mime in [('candidate.json','application/json'),('changes.patch','text/plain')]:
            selected=[item for item in artifacts if isinstance(item,dict) and item.get('name')==name]
            if (len(selected)!=1 or selected[0].get('mime')!=mime or not isinstance(selected[0].get('digest'),str)
                    or not re.fullmatch(r'[0-9a-f]{64}',selected[0]['digest'])):
                raise ValueError('SOURCE_BINDING_RUNTIME_ARTIFACTS')
            canonical.append({'name':name,'digest':selected[0]['digest'],'mime':mime})
        baseline=DATA/'workspace-baselines'/digest_key(workspace_id)
        preparation_baseline_matches(baseline,marker['baseDigest'])
        files=sources(target)
        candidate={'baseDigest':marker['baseDigest'],'candidateDigest':source_hash(files),
                   'files':[{'path':name,'sha256':hashlib.sha256(raw).hexdigest()} for name,raw in sorted(files.items())]}
        # Recheck each fixed protected file and the inventory before releasing
        # the workspace lock. No caller-selected path, state body or prompt is
        # exported. The outer inspector still probes the physical unit twice.
        for path,expected,mode in [(run_directory/'state.json',state_digest,0o600),
                                    (run_directory/'operation-policy.json',policy_digest,0o600),
                                    (target/'.agentos-snapshot.json',marker_digest,None)]:
            if read_preparation_json(path,required_mode=mode)[1]!=expected:
                raise ValueError('SOURCE_BINDING_OBSERVATION_CHANGED')
        if source_hash(sources(target))!=candidate['candidateDigest']:
            raise ValueError('SOURCE_BINDING_OBSERVATION_CHANGED')
        preparation_baseline_matches(baseline,marker['baseDigest'])
        return {'version':1,'runId':run_id,'workspace':workspace_id,
                'state':{'runId':run_id,'workspace':workspace_id,'runtime':'harness','state':'completed',
                         'runFence':policy['runFence'],'stopped':True,'artifacts':canonical},
                'admission':{'authority':'runtime_operation_policy',
                             **{key:policy[key] for key in ['goalId','taskId','runId','runFence','contextVersion','workspace','sourceSnapshotDigest']},
                             'policyDigest':policy_digest},
                'generation':{'workspace':workspace_id,'sourceWorkspace':marker['sourceWorkspace'],
                              'baseDigest':marker['baseDigest'],'snapshotDigest':marker_digest,
                              'sourceSnapshotDigest':marker.get('sourceSnapshotDigest')},
                'markerBase64':base64.b64encode(marker_raw).decode('ascii'),
                'legacyParentGeneration':'sourceSnapshotDigest' not in marker,'candidate':candidate}

def inspect(params):
    if 'sourceBindingRunId' in params: return inspect_source_binding(params)
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
