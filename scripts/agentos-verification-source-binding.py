#!/usr/bin/env python3
"""Read-only physical source proof for a never-started, read-only QA binding.

The only CLI identity is a task UUID. The committed settings receipt supplies
all workspace and artifact identities; this helper never prepares a workspace,
changes a task, or creates acceptance evidence.
"""
import hashlib, importlib.util, json, os, pathlib, re, stat, subprocess, sys, uuid

sys.dont_write_bytecode = True
PSQL = ['/usr/bin/runuser', '-u', 'postgres', '--', '/usr/lib/postgresql/17/bin/psql',
        '-p', '15482', '-d', 'agentos', '-t', '-A', '-v', 'ON_ERROR_STOP=1', '-c']
ACTIVE = pathlib.Path('/opt/agentos/current')
STATE_ROOT = pathlib.Path('/var/lib/agentos-verification-source-binding')

def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode('utf-8')

def sha(value): return hashlib.sha256(value).hexdigest()
def fail(code): raise ValueError(code)

def request_digest(value):
    payload = dict(value)
    pinned = payload.pop('requestDigest', None)
    for key in ('state', 'claimedAt', 'prepared', 'preparedAt', 'errorCode', 'updatedAt', 'appliedAt', 'binding'):
        payload.pop(key, None)
    return pinned, sha(canonical(payload))

def read_database(task_id):
    if str(uuid.UUID(task_id)) != task_id: fail('VERIFICATION_BINDING_ID_INVALID')
    sql = """SELECT jsonb_build_object(
      'request',b.value,
      'goal',jsonb_build_object('id',g.id,'project',g.project,'state',g.state),
      'registry',fp.value,
      'task',jsonb_build_object('id',q.id,'goalId',q.goal_id,'state',q.state,'attempt',q.attempt_count,
        'maxAttempts',q.max_attempts,'stage',q.stage,'execution',q.execution,'criteria',q.criteria,
        'instructionRevisionId',q.instruction_revision_id,'resources',q.resources),
      'dependencies',COALESCE((SELECT jsonb_agg(jsonb_build_object('taskId',d.depends_on,'goalId',p.goal_id,
        'state',p.state,'stage',p.stage,'execution',p.execution) ORDER BY d.depends_on)
        FROM dependencies d JOIN tasks p ON p.id=d.depends_on WHERE d.task_id=q.id),'[]'::jsonb),
      'parent',jsonb_build_object('id',p.id,'goalId',p.goal_id,'state',p.state,'attempt',p.attempt_count,
        'stage',p.stage,'execution',p.execution,'runId',r.id,'fence',r.fence,'contextId',r.context_id,
        'runAttempt',r.attempt,'runState',r.state,'acceptancePassed',a.passed,
        'candidateArtifacts',COALESCE((SELECT jsonb_agg(jsonb_build_object('id',x.id,'name',x.name,'digest',x.digest,'size',x.size) ORDER BY x.id)
          FROM artifacts x WHERE x.run_id=r.id AND x.name='candidate.json'),'[]'::jsonb),
        'rebase',rb.value),
      'taskRuns',(SELECT count(*) FROM runs x WHERE x.task_id=q.id),
      'taskBusy',EXISTS(SELECT 1 FROM runs x WHERE x.task_id=q.id AND x.state IN('queued','running','cancelling','reconciling')) OR
        EXISTS(SELECT 1 FROM leases l JOIN runs x ON x.id=l.run_id WHERE x.task_id=q.id),
      'parentBusy',EXISTS(SELECT 1 FROM runs x WHERE x.task_id=p.id AND x.state IN('queued','running','cancelling','reconciling')) OR
        EXISTS(SELECT 1 FROM leases l JOIN runs x ON x.id=l.run_id WHERE x.task_id=p.id),
      'cancelled',EXISTS(SELECT 1 FROM events e WHERE e.entity_id=q.id::text AND e.kind='CANCEL_REQUESTED') OR
        EXISTS(SELECT 1 FROM events e WHERE e.entity_id=p.id::text AND e.kind='CANCEL_REQUESTED'),
      'pendingCase',EXISTS(SELECT 1 FROM supervision_cases c WHERE c.task_id=q.id AND c.state IN('open','decided','delegated','waiting_evidence'))
      )::text
      FROM settings b JOIN tasks q ON q.id=(b.value->>'taskId')::uuid
      JOIN goals g ON g.id=q.goal_id
      LEFT JOIN settings fp ON fp.id='framework-project:'||g.project
      LEFT JOIN tasks p ON p.id=(b.value#>>'{parent,taskId}')::uuid AND p.goal_id=g.id
      LEFT JOIN LATERAL(SELECT x.* FROM runs x WHERE x.task_id=p.id AND x.attempt=p.attempt_count ORDER BY x.attempt DESC LIMIT 1)r ON true
      LEFT JOIN LATERAL(SELECT ac.passed FROM acceptances ac WHERE ac.run_id=r.id AND ac.task_id=p.id ORDER BY ac.created_at DESC,ac.id DESC LIMIT 1)a ON true
      LEFT JOIN settings rb ON rb.id='published-rebase:'||(p.execution#>>'{publishedRebase,actionId}')
      WHERE b.id='verification-source-binding:""" + task_id + "'"
    env = dict(os.environ); env['PGOPTIONS'] = '-c default_transaction_read_only=on'
    result = subprocess.run(PSQL + [sql], env=env, capture_output=True, text=True, timeout=15, check=False)
    if result.returncode or not result.stdout.strip(): fail('VERIFICATION_BINDING_AUTHORITY_UNAVAILABLE')
    try: value = json.loads(result.stdout)
    except Exception: fail('VERIFICATION_BINDING_AUTHORITY_INVALID')
    if not isinstance(value, dict): fail('VERIFICATION_BINDING_AUTHORITY_INVALID')
    return value

def verify_database(value, task_id):
    request, goal, task, parent = (value.get(k) for k in ('request', 'goal', 'task', 'parent'))
    if not all(isinstance(x, dict) for x in (request, goal, task, parent)): fail('VERIFICATION_BINDING_AUTHORITY_INVALID')
    pinned, actual = request_digest(request)
    if pinned != actual or request.get('version') != 1 or request.get('actionId') is None or request.get('taskId') != task_id or \
       request.get('state') not in ('preparing', 'uncertain', 'prepared'):
        fail('VERIFICATION_BINDING_REQUEST_DIGEST')
    project = goal.get('project'); auth = request.get('authorization')
    if request.get('goalId') != goal.get('id') or request.get('project') != project or task.get('goalId') != goal.get('id'):
        fail('VERIFICATION_BINDING_AUTHORITY_CHANGED')
    if project == 'agentos':
        if auth != {'kind': 'registered_agentos_source', 'project': 'agentos', 'goalId': goal['id']}:
            fail('VERIFICATION_BINDING_PROJECT_UNREGISTERED')
    else:
        registry = value.get('registry')
        if (not isinstance(registry, dict) or auth != {'kind': 'registered_framework_project', 'project': project,
            'goalId': goal['id'], 'registryId': 'framework-project:' + project, 'registry': registry,
            'registryDigest': sha(canonical(registry))} or registry != {'version': 1, 'enabled': True, 'project': project,
            'role': 'framework', 'sourceKind': 'agentos_published_runtime', 'goalIds': registry.get('goalIds')} or
            goal['id'] not in registry.get('goalIds', [])):
            fail('VERIFICATION_BINDING_PROJECT_UNREGISTERED')
    expected = request.get('expectedTask', {})
    execution = task.get('execution', {})
    if (goal.get('state') != 'active' or task.get('state') not in ('blocked_dependencies','ready','waiting_resource') or
        task.get('attempt') != 0 or value.get('taskRuns') != 0 or value.get('taskBusy') is not False or
        value.get('pendingCase') is not False or value.get('cancelled') is not False or
        task.get('stage') != 'verify' or execution.get('kind') != 'harness' or execution.get('workKind') != 'verification' or
        execution.get('readOnlySource') is not True or execution.get('workspace') != request.get('originalWorkspace') or
        execution.get('sourceWorkspace') != request.get('originalSourceWorkspace') or
        execution.get('baseWorkspace') != request.get('baseWorkspace') or expected.get('attempt') != 0 or
        expected.get('workspace') != execution.get('workspace') or expected.get('sourceWorkspace') != execution.get('sourceWorkspace') or
        expected.get('baseWorkspace') != execution.get('baseWorkspace') or sha(canonical(task.get('criteria'))) != expected.get('criteriaDigest')):
        fail('VERIFICATION_BINDING_TASK_CHANGED')
    if not re.fullmatch(re.escape(project) + r'/o-vb-[0-9a-f]{32}', request.get('targetWorkspace', '')) or \
       request.get('sourceWorkspace') != request.get('parent', {}).get('workspace') or \
       request.get('targetWorkspace') == request.get('originalWorkspace'):
        fail('VERIFICATION_BINDING_WORKSPACE_INVALID')
    deps = value.get('dependencies')
    if not isinstance(deps, list) or any(d.get('state') != 'accepted' or d.get('goalId') != goal['id'] for d in deps) or \
       sorted(d.get('taskId') for d in deps) != sorted(expected.get('dependencies', [])) or \
       sha(canonical(sorted(d.get('taskId') for d in deps))) != expected.get('dependenciesDigest'):
        fail('VERIFICATION_BINDING_DEPENDENCIES_CHANGED')
    parent_req = request.get('parent', {}); parent_execution = parent.get('execution', {})
    rebase = parent_execution.get('publishedRebase', {}); receipt = parent.get('rebase')
    parents = [d for d in deps if d.get('taskId') == parent_req.get('taskId') and d.get('stage') == 'integrate' and
        d.get('execution', {}).get('baseWorkspace') == request.get('baseWorkspace') and
        d.get('execution', {}).get('scope', d.get('execution', {}).get('baseWorkspace')) ==
            execution.get('scope', execution.get('baseWorkspace')) and
        d.get('execution', {}).get('publishedRebase', {}).get('priorWorkspace') == request.get('originalSourceWorkspace')]
    artifacts = parent.get('candidateArtifacts')
    if (len(parents) != 1 or parent.get('id') != parent_req.get('taskId') or parent.get('goalId') != goal['id'] or
        parent.get('stage') != 'integrate' or parent.get('state') != 'accepted' or parent.get('parentBusy') is not False or
        parent.get('cancelled') is True or parent.get('runState') != 'submitted' or parent.get('acceptancePassed') is not True or
        parent.get('runId') != parent_req.get('runId') or parent.get('fence') != parent_req.get('fence') or
        parent.get('runAttempt') != parent_req.get('attempt') or parent.get('attempt') != parent_req.get('attempt') or
        parent_execution.get('workspace') != parent_req.get('workspace') or parent_execution.get('sourceWorkspace') != parent_req.get('baselineWorkspace') or
        parent_execution.get('baseWorkspace') != request.get('baseWorkspace') or parent_execution.get('publishedRebase', {}).get('actionId') != parent_req.get('rebaseActionId') or
        parent_execution.get('publishedRebase', {}).get('requestDigest') != parent_req.get('rebaseRequestDigest') or
        parent_execution.get('publishedRebase', {}).get('preparedReceiptDigest') != parent_req.get('rebaseReceiptDigest') or
        parent_execution.get('publishedRebase', {}).get('priorWorkspace') != request.get('originalSourceWorkspace') or
        parent_execution.get('publishedRebase', {}).get('workspace') != parent_execution.get('workspace') or
        parent_execution.get('publishedRebase', {}).get('baselineWorkspace') != parent_req.get('baselineWorkspace') or
        not isinstance(receipt, dict) or receipt.get('state') != 'applied' or receipt.get('actionId') != parent_req.get('rebaseActionId') or
        receipt.get('taskId') != parent.get('id') or receipt.get('goalId') != goal.get('id') or
        receipt.get('requestDigest') != parent_req.get('rebaseRequestDigest') or receipt.get('targetWorkspace') != parent_execution.get('workspace') or
        receipt.get('currentWorkspace') != parent_req.get('priorWorkspace') or
        receipt.get('prepared', {}).get('sourceChanged') is not True or
        receipt.get('prepared', {}).get('receiptDigest') != parent_req.get('rebaseReceiptDigest') or
        not isinstance(artifacts, list) or len(artifacts) != 1 or artifacts[0].get('name') != 'candidate.json' or
        artifacts[0].get('id') != parent_req.get('candidateArtifactId') or artifacts[0].get('digest') != parent_req.get('candidateArtifactDigest') or
        artifacts[0].get('size') != parent_req.get('candidateArtifactSize') or
        parent_execution.get('publishedRebase', {}).get('workspace') != parent_execution.get('workspace')):
        fail('VERIFICATION_BINDING_PARENT_CHANGED')
    return request

def secure_file(path, limit):
    info = path.lstat()
    if path.is_symlink() or not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o077 or info.st_size > limit:
        fail('VERIFICATION_BINDING_RECEIPT_UNSAFE')
    return path.read_bytes()

def write_json(path, value):
    raw = canonical(value); temp = path.with_name(path.name + '.tmp-' + uuid.uuid4().hex)
    fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'wb') as stream:
        stream.write(raw); stream.flush(); os.fsync(stream.fileno())
    os.replace(temp, path)
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try: os.fsync(directory)
    finally: os.close(directory)

def state_directory(task_id, create=False):
    if os.geteuid() != 0: fail('VERIFICATION_BINDING_PRIVILEGE_REQUIRED')
    if not STATE_ROOT.exists():
        if not create: fail('VERIFICATION_BINDING_RECEIPT_MISSING')
        STATE_ROOT.mkdir(mode=0o700)
    info = STATE_ROOT.lstat()
    if STATE_ROOT.is_symlink() or not stat.S_ISDIR(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o077:
        fail('VERIFICATION_BINDING_RECEIPT_UNSAFE')
    directory = STATE_ROOT / task_id
    if create and not directory.exists(): directory.mkdir(mode=0o700)
    info = directory.lstat()
    if directory.is_symlink() or not stat.S_ISDIR(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o077:
        fail('VERIFICATION_BINDING_RECEIPT_UNSAFE')
    return directory

def physical_proof(request, workspace_root='/srv/agentos/workspaces', manager_path=None):
    root = pathlib.Path(workspace_root)
    project, source, target = request['project'], request['sourceWorkspace'], request['targetWorkspace']
    if not re.fullmatch(re.escape(project) + r'/[a-z0-9_-]+', source): fail('VERIFICATION_BINDING_WORKSPACE_INVALID')
    source_path = root / source
    target_path = root / target
    if source_path.is_symlink() or not source_path.is_dir() or target_path.exists() or target_path.is_symlink():
        fail('VERIFICATION_BINDING_WORKSPACE_STATE_CHANGED')
    marker_path = source_path / '.agentos-snapshot.json'
    marker_info = marker_path.lstat()
    if marker_path.is_symlink() or not stat.S_ISREG(marker_info.st_mode) or marker_info.st_size > 4096:
        fail('VERIFICATION_BINDING_SNAPSHOT_INVALID')
    try: marker = json.loads(marker_path.read_bytes())
    except Exception: fail('VERIFICATION_BINDING_SNAPSHOT_INVALID')
    if marker.get('workspace') != source or marker.get('sourceWorkspace') != request['parent']['baselineWorkspace']:
        fail('VERIFICATION_BINDING_SNAPSHOT_CHANGED')
    manager_path = manager_path or ACTIVE / 'bridge/workspace-manager.py'
    spec = importlib.util.spec_from_file_location('verification_binding_workspace_manager', manager_path)
    if not spec or not spec.loader: fail('VERIFICATION_BINDING_HELPER_UNAVAILABLE')
    manager = importlib.util.module_from_spec(spec); spec.loader.exec_module(manager)
    # Keep production defaults, while allowing the focused fixture to exercise
    # the real scanner against a temporary workspace tree.
    manager.ROOT = root
    manager.DATA = root.parent / '.verification-binding-test-data'
    observed = manager.inspect({'workspace': source})
    if observed.get('candidateDigest') != request['parent']['candidateDigest']:
        fail('VERIFICATION_BINDING_SOURCE_CHANGED')
    return {'version': 1, 'actionId': request['actionId'], 'requestDigest': request['requestDigest'],
        'taskId': request['taskId'], 'goalId': request['goalId'], 'project': project,
        'parentTaskId': request['parent']['taskId'], 'parentRunId': request['parent']['runId'],
        'parentFence': request['parent']['fence'], 'parentAttempt': request['parent']['attempt'],
        'candidateArtifactId': request['parent']['candidateArtifactId'],
        'candidateArtifactDigest': request['parent']['candidateArtifactDigest'],
        'candidateDigest': request['parent']['candidateDigest'], 'sourceWorkspace': source,
        'targetWorkspace': target, 'baseWorkspace': request['baseWorkspace'],
        'sourceManifestDigest': observed['candidateDigest'], 'sourceChanged': True,
        'rebaseActionId': request['parent']['rebaseActionId'],
        'rebaseReceiptDigest': request['parent']['rebaseReceiptDigest']}

def assert_unprepared(proof_path):
    if proof_path.exists(): fail('VERIFICATION_BINDING_PREPARE_ALREADY_RECORDED')

def verify_receipt_values(recorded_request, stored, request, fresh):
    if recorded_request != request or stored.get('requestDigest') != request['requestDigest']:
        fail('VERIFICATION_BINDING_RECEIPT_CHANGED')
    if fresh != stored: fail('VERIFICATION_BINDING_SOURCE_CHANGED')
    return stored

def prepare_receipt_files(request, directory, proof_fn=physical_proof):
    request_file, proof_file = directory / 'request.json', directory / 'prepared.json'
    assert_unprepared(proof_file)
    if request_file.exists() and secure_file(request_file, 256 * 1024) != canonical(request):
        fail('VERIFICATION_BINDING_REQUEST_RECEIPT_CHANGED')
    proof = proof_fn(request)
    assert_unprepared(proof_file)
    if not request_file.exists(): write_json(request_file, request)
    write_json(proof_file, proof)
    return proof

def prepare(task_id):
    database = read_database(task_id)
    request = verify_database(database, task_id)
    directory = state_directory(task_id, create=True)
    return prepare_receipt_files(request, directory)

def receipt(task_id):
    database = read_database(task_id)
    request = verify_database(database, task_id)
    directory = state_directory(task_id)
    recorded_request = json.loads(secure_file(directory / 'request.json', 256 * 1024))
    stored = json.loads(secure_file(directory / 'prepared.json', 128 * 1024))
    fresh = physical_proof(request)
    return verify_receipt_values(recorded_request, stored, request, fresh)

def main(argv):
    if len(argv) != 3 or argv[1] not in ('prepare', 'receipt'):
        fail('VERIFICATION_BINDING_COMMAND_INVALID')
    task_id = argv[2]
    value = prepare(task_id) if argv[1] == 'prepare' else receipt(task_id)
    print(json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False))

if __name__ == '__main__':
    try: main(sys.argv)
    except Exception as error:
        code = 'VERIFICATION_BINDING_SOURCE_BUSY' if str(error) == 'SOURCE_BUSY' else (
            str(error) if isinstance(error, ValueError) else 'VERIFICATION_BINDING_INTERNAL_ERROR')
        print(json.dumps({'error': code}, separators=(',', ':'))); sys.exit(1)
