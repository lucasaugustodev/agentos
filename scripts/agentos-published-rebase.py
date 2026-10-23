#!/usr/bin/env python3
"""Prepare a fresh workspace from the protected active AgentOS publication.

The only caller argument is an action UUID. Authority and every workspace
identity are loaded from a committed settings receipt using fixed read-only SQL.
This helper never changes a task, run, criterion, dependency, or acceptance.
"""
import hashlib, importlib.util, json, os, pathlib, re, stat, subprocess, sys, uuid

sys.dont_write_bytecode = True
STATE = pathlib.Path('/var/lib/agentos-published-rebase')
PSQL = ['/usr/bin/runuser', '-u', 'postgres', '--', '/usr/lib/postgresql/17/bin/psql',
        '-p', '15482', '-d', 'agentos', '-t', '-A', '-v', 'ON_ERROR_STOP=1', '-c']
ACTIVE = pathlib.Path('/opt/agentos/current')

def canonical(value): return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode('utf-8')
def sha(value): return hashlib.sha256(value).hexdigest()
def fail(code): raise ValueError(code)

def read_authority(action_id):
    if str(uuid.UUID(action_id)) != action_id: fail('REBASE_ID_INVALID')
    # action_id is a canonical UUID, so interpolation cannot alter this fixed query.
    sql = """SELECT jsonb_build_object(
      'request',s.value,
      'registry',CASE WHEN g.project='agentos' THEN NULL ELSE fp.value END,
      'goal',jsonb_build_object('id',g.id,'project',g.project,'state',g.state),
      'task',jsonb_build_object('id',t.id,'goalId',t.goal_id,'state',t.state,'attempt',t.attempt_count,
        'stage',t.stage,'workspace',t.execution->>'workspace','sourceWorkspace',t.execution->>'sourceWorkspace',
        'baseWorkspace',t.execution->>'baseWorkspace','workKind',t.execution->>'workKind','maxAttempts',t.max_attempts,
        'criteria',t.criteria),
      'run',jsonb_build_object('id',r.id,'fence',r.fence,'contextId',r.context_id,'attempt',r.attempt,
        'state',r.state,'workspace',r.instruction_snapshot#>>'{task,execution,workspace}'),
      'managerAccepted',EXISTS(SELECT 1 FROM runs mr JOIN tasks mt ON mt.id=mr.task_id
        JOIN acceptances a ON a.run_id=mr.id AND a.task_id=mt.id
        WHERE mr.id=(s.value->>'managerRunId')::uuid AND mt.goal_id=g.id AND mt.stage='coordinate'
          AND mr.state='submitted' AND a.id=(SELECT ac.id FROM acceptances ac WHERE ac.run_id=mr.id AND ac.task_id=mt.id ORDER BY ac.created_at DESC,ac.id DESC LIMIT 1) AND a.passed=true),
      'dependencies',COALESCE((SELECT jsonb_agg(jsonb_build_object('taskId',d.depends_on,'goalId',dt.goal_id,
        'state',dt.state,'attempt',dt.attempt_count,'workspace',dt.execution->>'workspace',
        'runId',dr.id,'fence',dr.fence,'artifacts',COALESCE((SELECT jsonb_agg(jsonb_build_object('id',a.id,'name',a.name,'digest',a.digest,'size',a.size) ORDER BY a.id)
          FROM artifacts a WHERE a.run_id=dr.id),'[]'::jsonb)) ORDER BY d.depends_on)
        FROM dependencies d JOIN tasks dt ON dt.id=d.depends_on
        LEFT JOIN LATERAL (SELECT r.id,r.fence FROM runs r WHERE r.task_id=dt.id AND r.attempt=dt.attempt_count AND r.state='submitted'
          AND (SELECT ac.passed FROM acceptances ac WHERE ac.run_id=r.id ORDER BY ac.created_at DESC,ac.id DESC LIMIT 1)=true) dr ON true
        WHERE d.task_id=t.id),'[]'::jsonb))::text
      FROM settings s JOIN goals g ON g.id=(s.value->>'goalId')::uuid
      JOIN tasks t ON t.id=(s.value->>'taskId')::uuid AND t.goal_id=g.id
      JOIN runs r ON r.id=(s.value->>'runId')::uuid AND r.task_id=t.id
      LEFT JOIN settings fp ON fp.id='framework-project:'||g.project
      WHERE s.id='published-rebase:""" + action_id + "'"
    env = dict(os.environ)
    env['PGOPTIONS'] = '-c default_transaction_read_only=on'
    result = subprocess.run(PSQL + [sql], env=env, capture_output=True, text=True, timeout=15, check=False)
    if result.returncode or not result.stdout.strip(): fail('REBASE_AUTHORITY_UNAVAILABLE')
    try: value = json.loads(result.stdout)
    except Exception: fail('REBASE_AUTHORITY_INVALID')
    if not isinstance(value, dict) or set(value) != {'request','registry','goal','task','run','managerAccepted','dependencies'}:
        fail('REBASE_AUTHORITY_INVALID')
    return value

def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    if not spec or not spec.loader: fail('REBASE_HELPER_UNAVAILABLE')
    module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module); return module

def protected_release_file(root, name):
    relative=pathlib.PurePosixPath(name)
    if relative.is_absolute() or '\\' in name or any(part in ('','.','..') for part in relative.parts): fail('REBASE_PUBLISHED_FILE_INVALID')
    current=root
    for index,part in enumerate(relative.parts):
        current=current/part; info=current.lstat()
        if current.is_symlink() or info.st_uid!=0 or info.st_mode&0o022: fail('REBASE_PUBLISHED_FILE_INVALID')
        if index<len(relative.parts)-1 and not stat.S_ISDIR(info.st_mode): fail('REBASE_PUBLISHED_FILE_INVALID')
        if index==len(relative.parts)-1 and not stat.S_ISREG(info.st_mode): fail('REBASE_PUBLISHED_FILE_INVALID')
    return current

def protected_manifest():
    broker = load_module('published_rebase_candidate_validation', pathlib.Path(__file__).with_name('agentos-candidate-validation.py'))
    result = broker.published_source_manifest()
    if not result.get('sourceDrift', {}).get('known') or any(result['sourceDrift'].get(k + 'Count', 1) for k in ['missing','changed','extra']):
        fail('REBASE_PUBLISHED_SOURCE_DRIFT')
    files = result.get('sourceHashes')
    docs = result.get('docsGitByteHashes', {})
    if not isinstance(files, dict) or not isinstance(docs, dict) or not files: fail('REBASE_PUBLISHED_MANIFEST_INVALID')
    files = {**files, **docs}
    for name, digest in files.items():
        p = pathlib.PurePosixPath(name)
        if p.is_absolute() or '\\' in name or any(part in ('', '.', '..', '.git', 'node_modules') or part.startswith('.env') for part in p.parts): fail('REBASE_PUBLISHED_MANIFEST_INVALID')
        if not re.fullmatch('[a-f0-9]{64}', digest): fail('REBASE_PUBLISHED_MANIFEST_INVALID')
        protected_release_file(pathlib.Path(result['publishedBaseline']).resolve(strict=True),name)
    return broker, result, files

def protected_release_physical_files(root):
    """Hash the complete sealed tree, including files filtered by workspace-manager."""
    root=pathlib.Path(root).resolve(strict=True)
    root_info=root.lstat()
    if root.is_symlink() or not stat.S_ISDIR(root_info.st_mode) or root_info.st_uid!=0 or root_info.st_mode&0o022:
        fail('REBASE_PUBLISHED_OUTPUT_INVALID')
    output={}; total=0; count=0
    for folder,dirs,names in os.walk(root,followlinks=False):
        dirs[:]=[name for name in sorted(dirs) if name not in ('node_modules','.git','.local-test')]
        names.sort()
        for directory in dirs:
            item=pathlib.Path(folder)/directory; meta=item.lstat()
            if item.is_symlink() or not stat.S_ISDIR(meta.st_mode) or meta.st_uid!=0 or meta.st_mode&0o022:
                fail('REBASE_PUBLISHED_OUTPUT_INVALID')
        for name in names:
            item=pathlib.Path(folder)/name; meta=item.lstat()
            if item.is_symlink() or not stat.S_ISREG(meta.st_mode) or meta.st_uid!=0 or meta.st_mode&0o022:
                fail('REBASE_PUBLISHED_OUTPUT_INVALID')
            relative=item.relative_to(root).as_posix(); path=pathlib.PurePosixPath(relative)
            if ('\\' in relative or path.is_absolute() or any(part in ('','.','..') for part in path.parts) or
                any(part.startswith('.env') for part in path.parts)):
                fail('REBASE_PUBLISHED_OUTPUT_INVALID')
            count+=1; total+=meta.st_size
            if count>4096 or total>512*1024*1024 or meta.st_size>32*1024*1024:
                fail('REBASE_PUBLISHED_OUTPUT_TOO_LARGE')
            with open(item,'rb') as handle:
                data=handle.read(32*1024*1024+1)
            if len(data)!=meta.st_size or len(data)>32*1024*1024: fail('REBASE_PUBLISHED_OUTPUT_INVALID')
            output[relative]=sha(data)
    return output

def protected_release_outputs(published):
    """Return sealed release files outside the source-only manifest."""
    root=pathlib.Path(published['publishedBaseline']).resolve(strict=True)
    return protected_release_physical_files(root)

def complete_workspace_manifest(published, source_files):
    physical=protected_release_outputs(published)
    if any(physical.get(name)!=digest for name,digest in source_files.items()): fail('REBASE_PUBLISHED_SOURCE_DRIFT')
    generated={name:digest for name,digest in physical.items() if name not in source_files}
    if set(source_files).intersection(generated): fail('REBASE_PUBLISHED_MANIFEST_INVALID')
    # The worker baseline copies only the source manifest. Sealed build products
    # and bytecode are evidence of the publication, never baseline inputs.
    return generated,dict(source_files),sha(canonical(source_files))

def registered_physical_files(root):
    """Read every physical alias file; manager.sources intentionally filters assets/logs."""
    return protected_release_physical_files(root)

def registered_inventory_matches(actual, source_files, generated_files):
    expected={**source_files,**generated_files}
    return actual == expected

def inspect_baseline():
    _, published, files = protected_manifest()
    generated, complete, workspace_digest=complete_workspace_manifest(published,files)
    manager = load_module('published_rebase_baseline_workspace_manager', ACTIVE / 'bridge/workspace-manager.py')
    registered = 'agentos/src' + published['publishedBaselineDigest'][:16]
    actual = registered_physical_files(manager.workspace(registered))
    if not registered_inventory_matches(actual,files,generated):
        fail('REBASE_REGISTERED_SOURCE_DRIFT')
    baseline = {'version':1,'release':str(pathlib.Path(published['publishedBaseline']).resolve()),
        'releaseDigest':published['publishedBaselineDigest'],'sourceWorkspace':registered,
        'sourceManifestHashes':files,'sourceManifestDigest':published['sourceManifestDigest'],
        'generatedReleaseHashes':{},'sealedReleaseExtraHashes':generated,'workspaceManifestDigest':workspace_digest,
        'publicationProofRef':published['publicationProofRef'],'publicationProofDigest':published['publicationProofDigest'],
        'validationProofRef':published['validationProofRef'],'validationProofDigest':published['validationProofDigest'],
        'registeredReceiptRef':published['registeredReceiptPath'],'registeredReceiptDigest':published['registeredReceiptSha'],
        'registeredActionDigest':published['registeredActionSha']}
    if not re.fullmatch(r'/opt/agentos/releases/managed-[0-9a-f-]{36}', baseline['release']): fail('REBASE_PUBLISHED_BASE_INVALID')
    return {'baseline':baseline}

def verify_authority(value, action_id, published, files):
    req, goal, task, run = value['request'], value['goal'], value['task'], value['run']
    project = goal.get('project')
    if req.get('version') != 1 or req.get('actionId') != action_id or req.get('project') != project or req.get('goalId') != goal.get('id'):
        fail('REBASE_AUTHORITY_INVALID')
    auth = req.get('frameworkAuthorization', {})
    if auth.get('project') != project or auth.get('goalId') != goal.get('id'): fail('REBASE_PROJECT_UNREGISTERED')
    if project == 'agentos':
        if auth != {'kind':'registered_agentos_source','project':'agentos','goalId':goal['id']} or value.get('registry') is not None:
            fail('REBASE_PROJECT_UNREGISTERED')
    else:
        registry = value.get('registry')
        if (not isinstance(registry, dict) or auth.get('kind') != 'registered_framework_project' or
            auth.get('registryId') != 'framework-project:' + project or auth.get('registry') != registry or
            auth.get('registryDigest') != sha(canonical(registry)) or
            registry != {'version':1,'enabled':True,'project':project,'role':'framework',
                         'sourceKind':'agentos_published_runtime','goalIds':registry.get('goalIds')} or
            goal['id'] not in registry.get('goalIds', [])):
            fail('REBASE_PROJECT_UNREGISTERED')
    if goal.get('state') != 'active' or value.get('managerAccepted') is not True: fail('REBASE_AUTHORITY_STALE')
    if task.get('id') != req.get('taskId') or task.get('goalId') != goal.get('id') or task.get('attempt') != req.get('expectedAttempt'):
        fail('REBASE_BINDING_STALE')
    if task.get('maxAttempts') != req.get('maxAttemptsBefore'): fail('REBASE_BINDING_STALE')
    if (task.get('state') != req.get('taskState') or task.get('stage')!='integrate' or
        task.get('state') in ('accepted','cancelled','superseded','queued','running','cancelling','reconciling')):
        fail('REBASE_BINDING_STALE')
    if run.get('id') != req.get('runId') or run.get('fence') != req.get('runFence') or run.get('contextId') != req.get('contextVersion') or run.get('attempt') != task.get('attempt'):
        fail('REBASE_BINDING_STALE')
    if (run.get('state') != req.get('runState') or run.get('state') not in ('submitted','failed') or
        task.get('workspace') != req.get('currentWorkspace') or task.get('sourceWorkspace') != req.get('priorSourceWorkspace') or
        task.get('baseWorkspace') != req.get('baseWorkspace') or task.get('workKind') != req.get('workKind') or
        not re.fullmatch(re.escape(project) + r'/o[a-z0-9_-]+', task.get('workspace') or '')):
        fail('REBASE_BINDING_STALE')
    if req.get('criteriaDigest') != sha(canonical(task.get('criteria'))): fail('REBASE_CRITERIA_CHANGED')
    deps = value.get('dependencies')
    dependency_ids = sorted(d.get('taskId','') for d in deps if isinstance(d,dict)) if isinstance(deps,list) else []
    if (not isinstance(deps,list) or dependency_ids != sorted(req.get('dependencyIds',[])) or
        any(d.get('goalId') != goal.get('id') or d.get('state') != 'accepted' or not d.get('runId') for d in deps)):
        fail('REBASE_DEPENDENCIES_CHANGED')
    if req.get('publishedBaselineDigest') != published.get('publishedBaselineDigest') or req.get('sourceManifestDigest') != published.get('sourceManifestDigest'):
        fail('REBASE_PUBLISHED_BASE_CHANGED')
    expected = {**published.get('sourceHashes', {}), **published.get('docsGitByteHashes', {})}
    if expected != files: fail('REBASE_PUBLISHED_MANIFEST_INVALID')
    generated, complete, workspace_digest=complete_workspace_manifest(published,files)
    pinned=req.get('publishedBaseline')
    actual={'version':1,'release':str(pathlib.Path(published['publishedBaseline']).resolve()),
        'releaseDigest':published['publishedBaselineDigest'],'sourceWorkspace':'agentos/src'+published['publishedBaselineDigest'][:16],
        'sourceManifestHashes':files,'sourceManifestDigest':published['sourceManifestDigest'],
        'generatedReleaseHashes':{},'sealedReleaseExtraHashes':generated,'workspaceManifestDigest':workspace_digest,
        'publicationProofRef':published['publicationProofRef'],'publicationProofDigest':published['publicationProofDigest'],
        'validationProofRef':published['validationProofRef'],'validationProofDigest':published['validationProofDigest'],
        'registeredReceiptRef':published['registeredReceiptPath'],'registeredReceiptDigest':published['registeredReceiptSha'],
        'registeredActionDigest':published['registeredActionSha']}
    if pinned != actual or req.get('sourceWorkspace') != actual['sourceWorkspace'] or req.get('workspaceManifestDigest') != workspace_digest:
        fail('REBASE_PUBLISHED_BASE_CHANGED')
    baseline = project + '/pub' + published['publishedBaselineDigest']
    target = project + '/o-rb-' + action_id.replace('-', '')
    if req.get('baselineWorkspace') != baseline or req.get('targetWorkspace') != target:
        fail('REBASE_WORKSPACE_BINDING_INVALID')
    if not re.fullmatch('[a-f0-9]{64}', req.get('requestDigest','')): fail('REBASE_AUTHORITY_INVALID')
    payload = dict(req); pinned = payload.pop('requestDigest'); payload.pop('state', None)
    for field in ('startedAt','prepared','preparedAt','errorCode','updatedAt','appliedAt'): payload.pop(field, None)
    if sha(canonical(payload)) != pinned: fail('REBASE_AUTHORITY_DIGEST')
    return req

def atomic_json(file, value):
    raw = canonical(value); temp = file.with_name(file.name + '.tmp-' + uuid.uuid4().hex)
    with open(temp, 'xb') as f:
        os.fchmod(f.fileno(), 0o600); f.write(raw); f.flush(); os.fsync(f.fileno())
    os.replace(temp, file)
    fd = os.open(file.parent, os.O_DIRECTORY)
    try: os.fsync(fd)
    finally: os.close(fd)

def source_change_proof(prior_source, prior_workspace_manifest, published, source_files, workspace_digest):
    if not prior_workspace_manifest: fail('REBASE_SOURCE_WORKSPACE_INVALID')
    physical=protected_release_outputs(published)
    generated={name:digest for name,digest in physical.items() if name not in source_files}
    prior_source_manifest={name:digest for name,digest in prior_workspace_manifest.items()
        if name not in generated}
    # The request has no immutable old-publication manifest binding for this
    # prior workspace. A prior-only path could be stale/untrusted output, so
    # refuse it; supporting legitimate upstream deletions needs that proof.
    if set(prior_source_manifest) - set(source_files): fail('REBASE_PRIOR_SOURCE_MANIFEST_INVALID')
    if prior_source_manifest == source_files: fail('REBASE_ALREADY_CURRENT')
    prior_source_digest = sha(canonical(prior_source_manifest))
    prior_workspace_digest = sha(canonical(prior_workspace_manifest))
    change_digest = sha(canonical({'priorSourceWorkspace':prior_source,'priorSourceManifestDigest':prior_source_digest,
        'priorWorkspaceManifestDigest':prior_workspace_digest,'publishedBaselineDigest':published['publishedBaselineDigest'],
        'sourceManifestDigest':published['sourceManifestDigest'],'workspaceManifestDigest':workspace_digest}))
    return {'sourceChanged':True,'priorSourceManifestDigest':prior_source_digest,
        'priorWorkspaceManifestDigest':prior_workspace_digest,'sourceChangeDigest':change_digest}

def validate_prior_source_workspace(project, value):
    if not isinstance(value,str) or not re.fullmatch(re.escape(project) + r'/[a-z0-9_-]+',value):
        fail('REBASE_SOURCE_WORKSPACE_INVALID')
    return value

def safe_baseline(req, files, published, manager):
    release = pathlib.Path(published['publishedBaseline']).resolve(strict=True)
    baseline_id = req['baselineWorkspace']; baseline = manager.workspace(baseline_id)
    marker = baseline / '.agentos-snapshot.json'
    expected_marker = {'kind':'published_rebase_baseline','version':1,'workspace':baseline_id,
        'publishedBaselineDigest':published['publishedBaselineDigest'],'sourceManifestDigest':published['sourceManifestDigest'],
        'workspaceManifestDigest':req['workspaceManifestDigest'],
        'publicationProofDigest':published['publicationProofDigest'],'validationProofDigest':published['validationProofDigest']}
    if baseline.exists():
        info=baseline.lstat()
        marker_info=marker.lstat() if marker.exists() else None
        if (baseline.is_symlink() or info.st_uid!=0 or info.st_mode&0o222 or
            marker_info is None or not stat.S_ISREG(marker_info.st_mode) or marker_info.st_uid!=0 or marker_info.st_mode&0o222 or
            json.loads(marker.read_bytes()) != expected_marker): fail('REBASE_BASELINE_CONFLICT')
        actual = manager.sources(baseline)
        if set(actual) != set(files) or any(sha(actual[k]) != files[k] for k in files): fail('REBASE_BASELINE_CHANGED')
        for name in files:
            item=(baseline/name).lstat()
            if not stat.S_ISREG(item.st_mode) or item.st_uid!=0 or item.st_mode&0o222: fail('REBASE_BASELINE_CHANGED')
        return baseline
    baseline.parent.mkdir(mode=0o750, parents=False, exist_ok=True)
    staging = baseline.parent / ('.rebase-' + uuid.uuid4().hex)
    try:
        staging.mkdir(mode=0o700)
        total = 0
        for name, digest in sorted(files.items()):
            src = protected_release_file(release,name)
            info = src.lstat()
            if not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o022: fail('REBASE_PUBLISHED_FILE_INVALID')
            data = src.read_bytes()
            if sha(data) != digest: fail('REBASE_PUBLISHED_SOURCE_DRIFT')
            target = staging / name; target.parent.mkdir(parents=True, exist_ok=True)
            with open(target, 'xb') as f:
                f.write(data); f.flush(); os.fsync(f.fileno())
            os.chmod(target, 0o444); total += len(data)
        if total > 256 * 1024 * 1024: fail('REBASE_PUBLISHED_SOURCE_TOO_LARGE')
        (staging / '.agentos-snapshot.json').write_bytes(canonical(expected_marker))
        os.chmod(staging / '.agentos-snapshot.json', 0o444)
        for folder, dirs, _ in os.walk(staging, topdown=False):
            for directory in dirs: os.chmod(pathlib.Path(folder) / directory, 0o555)
            os.chmod(folder, 0o555)
        staging.rename(baseline)
    finally:
        if staging.exists():
            for folder, dirs, names in os.walk(staging):
                os.chmod(folder, 0o700)
                for name in names: os.chmod(pathlib.Path(folder) / name, 0o600)
            import shutil; shutil.rmtree(staging)
    return baseline

def prepare(action_id):
    if os.geteuid() != 0: fail('REBASE_PRIVILEGE_REQUIRED')
    authority = read_authority(action_id)
    broker, published, files = protected_manifest()
    generated, copy_files, workspace_digest=complete_workspace_manifest(published,files)
    req = verify_authority(authority, action_id, published, files)
    if inspect_baseline().get('baseline') != req.get('publishedBaseline'): fail('REBASE_REGISTERED_SOURCE_DRIFT')
    if not STATE.exists(): STATE.mkdir(mode=0o700)
    if STATE.is_symlink() or STATE.stat().st_uid != 0 or STATE.stat().st_mode & 0o077: fail('REBASE_STATE_UNSAFE')
    state = STATE / action_id
    if state.exists() and (state.is_symlink() or state.stat().st_uid != 0 or state.stat().st_mode & 0o077): fail('REBASE_STATE_UNSAFE')
    state.mkdir(mode=0o700, parents=True, exist_ok=True)
    request_file = state / 'request.json'
    if request_file.exists():
        info=request_file.lstat()
        if request_file.is_symlink() or info.st_uid != 0 or info.st_mode&0o077 or request_file.read_bytes() != canonical(req): fail('REBASE_REQUEST_RECEIPT_CONFLICT')
    else: atomic_json(request_file, req)
    manager = load_module('published_rebase_workspace_manager', ACTIVE / 'bridge/workspace-manager.py')
    if not re.fullmatch(re.escape(req['project']) + r'/pub[a-f0-9]{64}', req['baselineWorkspace']): fail('REBASE_WORKSPACE_BINDING_INVALID')
    # Baseline creation is serialized with ordinary worker snapshots. The actual
    # task snapshot is then created by the existing capacity-reserving manager.
    prior_source=validate_prior_source_workspace(req['project'],req.get('priorSourceWorkspace'))
    candidate_run = {'runId':req['runId'],'workspace':req['currentWorkspace'],'taskId':req['taskId'],
        'goalId':req['goalId'],'runFence':req['runFence'],'contextVersion':req['contextVersion']}
    def recheck_sources():
        with manager.locks([req['currentWorkspace'],prior_source]):
            if manager.source_hash(manager.sources(manager.workspace(req['currentWorkspace']))) != req['candidateDigest']:
                fail('REBASE_CANDIDATE_CHANGED')
            prior={name:sha(data) for name,data in sorted(manager.sources(manager.workspace(prior_source)).items())}
            check=source_change_proof(prior_source,prior,published,files,workspace_digest)
            if check != source_change: fail('REBASE_SOURCE_CHANGED_DURING_PREPARE')
            broker.stopped(candidate_run, manager)
    with manager.locks([req['baselineWorkspace'], req['currentWorkspace'], req['targetWorkspace'], prior_source]):
        current = manager.workspace(req['currentWorkspace'])
        if manager.source_hash(manager.sources(current)) != req['candidateDigest']: fail('REBASE_CANDIDATE_CHANGED')
        prior_root=manager.workspace(prior_source)
        prior_manifest={name:sha(data) for name,data in sorted(manager.sources(prior_root).items())}
        source_change=source_change_proof(prior_source,prior_manifest,published,files,workspace_digest)
        prior_manifest_digest=source_change['priorSourceManifestDigest']
        source_change_digest=source_change['sourceChangeDigest']
        broker.stopped(candidate_run, manager)
        needed = sum(max(4096, (ACTIVE / name).lstat().st_size) for name in copy_files) + 16 * 1024 * 1024
        with manager.storage_capacity.reserve(manager.ROOT, manager.DATA, needed):
            baseline = safe_baseline(req, copy_files, published, manager)
    if manager.workspace(req['targetWorkspace']).exists():
        receipt_file = state / 'prepared.json'
        if not receipt_file.exists(): fail('REBASE_TARGET_UNOWNED')
        receipt_info=receipt_file.lstat()
        if receipt_file.is_symlink() or receipt_info.st_uid!=0 or receipt_info.st_mode&0o077: fail('REBASE_PREPARED_RECEIPT_UNSAFE')
        receipt = json.loads(receipt_file.read_bytes())
        if (receipt.get('targetWorkspace') != req['targetWorkspace'] or receipt.get('requestDigest') != req['requestDigest'] or
            receipt.get('priorSourceManifestDigest') != prior_manifest_digest or receipt.get('sourceChangeDigest') != source_change_digest or
            receipt.get('sourceChanged') is not True):
            fail('REBASE_TARGET_UNOWNED')
        snapshot=manager.prepare({'workspace':req['targetWorkspace'],'sourceWorkspace':req['baselineWorkspace']})
        observed=manager.inspect({'workspace':req['targetWorkspace']})
        if (snapshot.get('baseDigest')!=receipt.get('targetBaseDigest') or observed.get('baseDigest')!=snapshot.get('baseDigest') or
            observed.get('candidateDigest')!=snapshot.get('baseDigest')): fail('REBASE_TARGET_CHANGED')
        recheck_sources()
        return receipt
    broker.stopped(candidate_run, manager)
    target_state = manager.prepare({'workspace':req['targetWorkspace'],'sourceWorkspace':req['baselineWorkspace']})
    observed=manager.inspect({'workspace':req['targetWorkspace']})
    if observed.get('baseDigest')!=target_state.get('baseDigest') or observed.get('candidateDigest')!=target_state.get('baseDigest'):
        fail('REBASE_TARGET_CHANGED')
    recheck_sources()
    after=inspect_baseline().get('baseline')
    if after != req.get('publishedBaseline'):
        fail('REBASE_PUBLISHED_BASE_CHANGED')
    result = {'version':1,'actionId':action_id,'requestDigest':req['requestDigest'],'project':req['project'],
        'goalId':req['goalId'],'taskId':req['taskId'],'runId':req['runId'],'runFence':req['runFence'],
        'publishedBaselineDigest':published['publishedBaselineDigest'],'sourceManifestDigest':published['sourceManifestDigest'],
        'workspaceManifestDigest':workspace_digest,
        'publicationProofDigest':published['publicationProofDigest'],'validationProofDigest':published['validationProofDigest'],
        'baselineWorkspace':req['baselineWorkspace'],'targetWorkspace':req['targetWorkspace'],
        **source_change,
        'targetBaseDigest':target_state.get('baseDigest'),'state':'prepared'}
    atomic_json(state / 'prepared.json', result)
    return result

def read_prepared_receipt(action_id):
    """Recover a lost prepare reply using read-only validation; never rerun prepare."""
    if os.geteuid() != 0: fail('REBASE_PRIVILEGE_REQUIRED')
    authority = read_authority(action_id)
    broker, published, files = protected_manifest()
    req = verify_authority(authority, action_id, published, files)
    state = STATE / action_id
    if state.is_symlink() or not state.is_dir(): fail('REBASE_PREPARED_RECEIPT_MISSING')
    state_info = state.lstat()
    if state_info.st_uid != 0 or state_info.st_mode & 0o077: fail('REBASE_STATE_UNSAFE')
    request_file = state / 'request.json'
    prepared_file = state / 'prepared.json'
    for path, limit in ((request_file, 2 * 1024 * 1024), (prepared_file, 128 * 1024)):
        info = path.lstat()
        if (path.is_symlink() or not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or
            info.st_mode & 0o077 or info.st_size > limit):
            fail('REBASE_PREPARED_RECEIPT_UNSAFE')
    try:
        recorded_request = json.loads(request_file.read_bytes())
        receipt = json.loads(prepared_file.read_bytes())
    except Exception:
        fail('REBASE_PREPARED_RECEIPT_INVALID')
    def request_hash(value):
        payload = dict(value); pinned = payload.pop('requestDigest', None)
        for field in ('state','startedAt','prepared','preparedAt','errorCode','updatedAt','appliedAt'):
            payload.pop(field, None)
        return pinned, sha(canonical(payload))
    if request_hash(recorded_request) != request_hash(req) or request_hash(req)[0] != req.get('requestDigest'):
        fail('REBASE_REQUEST_RECEIPT_CONFLICT')
    if (not isinstance(receipt, dict) or receipt.get('version') != 1 or receipt.get('actionId') != action_id or
        receipt.get('requestDigest') != req.get('requestDigest') or receipt.get('project') != req.get('project') or
        receipt.get('goalId') != req.get('goalId') or receipt.get('taskId') != req.get('taskId') or
        receipt.get('runId') != req.get('runId') or receipt.get('runFence') != req.get('runFence') or
        receipt.get('publishedBaselineDigest') != req.get('publishedBaselineDigest') or
        receipt.get('sourceManifestDigest') != req.get('sourceManifestDigest') or
        receipt.get('workspaceManifestDigest') != req.get('workspaceManifestDigest') or
        receipt.get('baselineWorkspace') != req.get('baselineWorkspace') or
        receipt.get('targetWorkspace') != req.get('targetWorkspace') or receipt.get('sourceChanged') is not True):
        fail('REBASE_PREPARED_RECEIPT_INVALID')
    for name in ('priorSourceManifestDigest','priorWorkspaceManifestDigest','sourceChangeDigest','targetBaseDigest'):
        if not re.fullmatch('[a-f0-9]{64}', receipt.get(name, '')): fail('REBASE_PREPARED_RECEIPT_INVALID')
    _, current_published, current_files = protected_manifest()
    if inspect_baseline().get('baseline') != req.get('publishedBaseline'):
        fail('REBASE_PUBLISHED_BASE_CHANGED')
    generated, complete, workspace_digest = complete_workspace_manifest(current_published, current_files)
    manager = load_module('published_rebase_receipt_workspace_manager', ACTIVE / 'bridge/workspace-manager.py')
    baseline_root = manager.workspace(req['baselineWorkspace'])
    baseline_sources = manager.sources(baseline_root)
    if set(baseline_sources) != set(complete) or any(sha(baseline_sources[name]) != complete[name] for name in complete):
        fail('REBASE_BASELINE_CHANGED')
    prior_source = validate_prior_source_workspace(req['project'], req.get('priorSourceWorkspace'))
    prior = {name:sha(data) for name,data in sorted(manager.sources(manager.workspace(prior_source)).items())}
    source_change = source_change_proof(prior_source, prior, current_published, current_files, workspace_digest)
    if any(receipt.get(key) != value for key,value in source_change.items()):
        fail('REBASE_SOURCE_CHANGED_DURING_PREPARE')
    observed = manager.inspect({'workspace':req['targetWorkspace']})
    if (observed.get('baseDigest') != receipt.get('targetBaseDigest') or
        observed.get('candidateDigest') != receipt.get('targetBaseDigest')):
        fail('REBASE_TARGET_CHANGED')
    return receipt

if __name__ == '__main__':
    try:
        if len(sys.argv)==2 and sys.argv[1]=='inspect_baseline': result=inspect_baseline()
        elif len(sys.argv) == 3 and sys.argv[1] == 'prepare': result = prepare(sys.argv[2])
        elif len(sys.argv) == 3 and sys.argv[1] == 'receipt': result = read_prepared_receipt(sys.argv[2])
        else: fail('REBASE_MODE_INVALID')
        print(json.dumps(result, sort_keys=True, separators=(',', ':')))
    except Exception as exc:
        code = str(exc) if isinstance(exc, ValueError) else type(exc).__name__
        print(json.dumps({'error':code}, sort_keys=True, separators=(',', ':')))
        sys.exit(1)
