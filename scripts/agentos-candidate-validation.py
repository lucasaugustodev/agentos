#!/usr/bin/env python3
"""Validate an owned, frozen candidate. Never publishes or edits a worker tree.

The controller admits the accepted Sol decision. This root-side broker verifies
the physical identity and serializes immutable receipts before calling the
existing, hash-pinned registered validation action.
"""
import base64, contextlib, hashlib, importlib.util, json, os, pathlib, re, shutil, subprocess, sys, uuid
sys.dont_write_bytecode = True
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import validator_provenance as provenance

REGISTERED_HASH = 'e66fcf513aaa513ff2216d87556fb5dba771597dc933ca4b4b38573d8842d5b5'
REGISTERED = pathlib.Path('/opt/agentos-control/agentos-release-action.py')
PROFILES = pathlib.Path('/etc/agentos/actions.json')
STATE = pathlib.Path('/var/lib/agentos-bridge/candidate-validations')

def sha(value): return hashlib.sha256(value).hexdigest()
def canonical(value): return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode('utf-8')
def atomic_bytes(file, value):
    temporary = file.with_name(file.name + '.tmp-' + uuid.uuid4().hex)
    with open(temporary, 'xb') as handle:
        os.chmod(temporary, 0o600); handle.write(value); handle.flush(); os.fsync(handle.fileno())
    os.replace(temporary, file)
    directory = os.open(file.parent, os.O_DIRECTORY)
    try: os.fsync(directory)
    finally: os.close(directory)
def atomic(file, value): atomic_bytes(file, canonical(value))

def load_workspace_manager():
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent / 'bridge'))
    spec = importlib.util.spec_from_file_location('validation_workspaces', pathlib.Path(__file__).resolve().parent.parent / 'bridge/workspace-manager.py')
    module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    return module

def identity(request):
    runtime_authority = request.get('authority') == 'runtime_artifact'
    if request.get('authority') not in (None, 'runtime_artifact'):
        raise ValueError('VALIDATION_AUTHORITY_INVALID')
    for name in ['actionId', 'goalId', 'taskId', 'runId', 'runFence', 'contextVersion'] + ([] if runtime_authority else ['managerRunId']):
        if str(uuid.UUID(request[name])) != request[name]: raise ValueError('VALIDATION_IDENTITY_INVALID')
    if not isinstance(request.get('expectedAttempt'), int) or request['expectedAttempt'] < 1:
        raise ValueError('VALIDATION_IDENTITY_INVALID')
    if runtime_authority:
        runtime_validation_authority(request)
    else:
        if 'runtimeAuthority' in request: raise ValueError('VALIDATION_AUTHORITY_AMBIGUOUS')
        decision = request.get('decisionProof', {})
        if (set(decision) != {'coordinationArtifactId','coordinationArtifactDigest','coordinationSize','actionIndex','actionDigest'} or
            str(uuid.UUID(decision.get('coordinationArtifactId',''))) != decision['coordinationArtifactId'] or
            not re.fullmatch('[a-f0-9]{64}',decision.get('coordinationArtifactDigest','')) or
            not re.fullmatch('[a-f0-9]{64}',decision.get('actionDigest','')) or
            type(decision.get('coordinationSize')) is not int or not 0<decision['coordinationSize']<=8*1024*1024 or
            type(decision.get('actionIndex')) is not int or decision['actionIndex']<0): raise ValueError('VALIDATION_DECISION_UNBOUND')
    project = request.get('project', '')
    authorization = request.get('frameworkAuthorization', {})
    if not re.fullmatch(r'[a-z0-9_-]+', project) or authorization.get('project') != project or authorization.get('goalId') != request['goalId']:
        raise ValueError('VALIDATION_PROJECT_UNREGISTERED')
    if authorization.get('kind') == 'registered_agentos_source':
        if project != 'agentos' or set(authorization) != {'kind','project','goalId'}: raise ValueError('VALIDATION_PROJECT_UNREGISTERED')
    elif authorization.get('kind') == 'registered_framework_project':
        registry = authorization.get('registry', {})
        if (set(authorization) != {'kind','project','goalId','registryId','registry','registryDigest'} or
            set(registry) != {'version','enabled','project','role','sourceKind','goalIds'} or
            type(registry.get('version')) is not int or registry['version'] != 1 or registry.get('enabled') is not True or
            registry.get('project') != project or registry.get('role') != 'framework' or registry.get('sourceKind') != 'agentos_published_runtime' or
            not isinstance(registry.get('goalIds'),list) or not 0<len(registry['goalIds'])<=256 or request['goalId'] not in registry['goalIds'] or
            authorization.get('registryId') != 'framework-project:' + project or authorization.get('registryDigest') != sha(canonical(registry))):
            raise ValueError('VALIDATION_PROJECT_UNREGISTERED')
        if any(str(uuid.UUID(value)) != value for value in registry['goalIds']): raise ValueError('VALIDATION_PROJECT_UNREGISTERED')
    else: raise ValueError('VALIDATION_PROJECT_UNREGISTERED')
    if not re.fullmatch(re.escape(project) + r'/o[a-z0-9_-]+', request.get('workspace', '')):
        raise ValueError('VALIDATION_WORKSPACE_INVALID')
    for name in ['candidateDigest', 'candidateArtifactDigest', 'contractDigest']:
        if not re.fullmatch('[a-f0-9]{64}', request.get(name, '')): raise ValueError('VALIDATION_IDENTITY_INVALID')
    raw = base64.b64decode(request['candidateBase64'], validate=True)
    if len(raw) > 8*1024*1024 or len(raw) != request['candidateSize'] or sha(raw) != request['candidateArtifactDigest']:
        raise ValueError('VALIDATION_ARTIFACT_CHANGED')
    candidate = json.loads(raw)
    if candidate.get('candidateDigest') != request['candidateDigest']:
        raise ValueError('VALIDATION_ARTIFACT_CHANGED')
    return candidate, raw

def runtime_validation_authority(request):
    """Independent structural/hash checks; controller checks persisted policy,
    authenticated registration event, current admission snapshot and ownership.
    The intent is evidence, never a command or a grant of host access.
    """
    if 'managerRunId' in request or 'decisionProof' in request:
        raise ValueError('VALIDATION_AUTHORITY_AMBIGUOUS')
    a = request.get('runtimeAuthority', {})
    if (set(a) != {'version','kind','profile','policyRegistration','intent','snapshotDigest'} or
        a.get('version') != 1 or a.get('kind') != 'runtime_artifact' or a.get('profile') != 'agentos.validate'):
        raise ValueError('VALIDATION_RUNTIME_AUTHORITY_INVALID')
    r = a.get('policyRegistration', {})
    if (set(r) != {'version','id','registrationId','goalId','policy','policyDigest','channel'} or
        r.get('version') != 1 or r.get('goalId') != request.get('goalId') or
        r.get('id') != 'worker-validation:' + request['goalId'] or
        str(uuid.UUID(r.get('registrationId',''))) != r['registrationId'] or
        r.get('channel') not in ['api-token','operator-session']):
        raise ValueError('VALIDATION_RUNTIME_POLICY_INVALID')
    policy = r.get('policy', {})
    if (set(policy) != {'version','enabled','project','profile','stages','required','requiredDatabaseTests'} or
        policy.get('version') != 1 or policy.get('enabled') is not True or policy.get('required') is not True or
        policy.get('project') != request.get('project') or policy.get('profile') != 'agentos.validate' or
        not isinstance(policy.get('stages'), list) or not policy['stages'] or
        any(stage not in ['work','integrate','verify'] for stage in policy['stages']) or
        len(set(policy['stages'])) != len(policy['stages']) or
        r.get('policyDigest') != sha(canonical(policy)) or a.get('snapshotDigest') != sha(canonical(r))):
        raise ValueError('VALIDATION_RUNTIME_POLICY_INVALID')
    required = policy.get('requiredDatabaseTests')
    if not isinstance(required, list) or not 1 <= len(required) <= 128:
        raise ValueError('VALIDATION_RUNTIME_COVERAGE_INVALID')
    seen = set()
    for item in required:
        if (not isinstance(item, dict) or set(item) != {'file','name'} or
            not isinstance(item.get('file'), str) or not isinstance(item.get('name'), str) or
            len(item['file']) > 300 or
            not re.fullmatch(r'tests/(?:[A-Za-z0-9_-]+/)*[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+)*\.test\.[cm]?[jt]s', item['file']) or
            any(part in ('','.','..') for part in item['file'].split('/')) or
            not 1 <= len(item['name']) <= 500 or '\0' in item['name'] or
            (item['file'],item['name']) in seen):
            raise ValueError('VALIDATION_RUNTIME_COVERAGE_INVALID')
        seen.add((item['file'],item['name']))
    intent = a.get('intent', {})
    if (set(intent) != {'artifactId','digest','size','base64'} or
        str(uuid.UUID(intent.get('artifactId',''))) != intent['artifactId'] or
        type(intent.get('size')) is not int or not 0 < intent['size'] <= 4096):
        raise ValueError('VALIDATION_RUNTIME_INTENT_INVALID')
    raw = base64.b64decode(intent['base64'], validate=True)
    if (base64.b64encode(raw).decode() != intent['base64'] or len(raw) != intent['size'] or
        sha(raw) != intent['digest']):
        raise ValueError('VALIDATION_RUNTIME_INTENT_CHANGED')
    body = json.loads(raw.decode('utf-8'))
    if (not isinstance(body, dict) or set(body) != {'version','profile','reason'} or body.get('version') != 1 or
        body.get('profile') != 'agentos.validate' or not isinstance(body.get('reason'), str) or
        not 20 <= len(body['reason']) <= 2000):
        raise ValueError('VALIDATION_RUNTIME_INTENT_INVALID')

def stopped(request, manager):
    state_file = manager.DATA / 'worker-runs' / request['runId'] / 'state.json'
    state = json.loads(state_file.read_bytes())
    if state.get('runId') != request['runId'] or state.get('workspace') != request['workspace'] or state.get('stopped') is not True:
        raise ValueError('VALIDATION_PHYSICAL_IDENTITY_CHANGED')
    # Missing historical optional metadata cannot contradict the authoritative
    # controller binding; present fields must agree with its exact attempt.
    for name in ['taskId', 'goalId', 'runFence', 'contextVersion']:
        if name in state and state[name] != request[name]: raise ValueError('VALIDATION_PHYSICAL_IDENTITY_CHANGED')
    unit = 'agentos-run-' + request['runId'] + '.service'
    result = subprocess.run(['systemctl', 'show', unit, '--property=LoadState,ActiveState,MainPID,ControlGroup'], check=True, capture_output=True, text=True, timeout=5)
    values = dict(line.split('=', 1) for line in result.stdout.splitlines() if '=' in line)
    cgroup = '/system.slice/' + unit
    if values.get('LoadState') not in ['loaded', 'not-found'] or values.get('ActiveState') not in ['inactive', 'failed'] or values.get('MainPID') != '0' or values.get('ControlGroup') not in ['', cgroup]:
        raise ValueError('STOP_UNCONFIRMED')
    events = pathlib.Path('/sys/fs/cgroup' + cgroup + '/cgroup.events')
    if events.exists() and not re.search(r'^populated 0$', events.read_text(), re.M): raise ValueError('STOP_UNCONFIRMED')

def freeze(request, directory, manager, physical=stopped):
    candidate, raw = identity(request)
    frozen_id = 'agentos/ocv' + request['actionId'].replace('-', '')
    source, target = manager.workspace(request['workspace']), manager.workspace(frozen_id)
    with manager.locks([request['workspace'], frozen_id]):
        physical(request, manager)
        before = manager.sources(source)
        if manager.source_hash(before) != request['candidateDigest']: raise ValueError('VALIDATION_CANDIDATE_CHANGED')
        # Canonical list is evidence, never a list of paths to read or execute.
        actual = [{'path': name, 'sha256': sha(value)} for name, value in sorted(before.items())]
        if candidate.get('files') != actual: raise ValueError('VALIDATION_ARTIFACT_CHANGED')
        if sum(len(value) for value in before.values()) > 128*1024*1024: raise ValueError('VALIDATION_SOURCE_TOO_LARGE')
        if target.exists():
            if manager.source_hash(manager.sources(target)) != request['candidateDigest']: raise ValueError('VALIDATION_FROZEN_CHANGED')
        else:
            staging = target.parent / ('.validation-' + uuid.uuid4().hex)
            try:
                staging.mkdir(mode=0o700)
                for name, value in before.items():
                    dest = staging / name; dest.parent.mkdir(parents=True, exist_ok=True); dest.write_bytes(value)
                if manager.source_hash(manager.sources(staging)) != request['candidateDigest']: raise ValueError('VALIDATION_CLONE_CHANGED')
                if manager.source_hash(manager.sources(source)) != request['candidateDigest']: raise ValueError('VALIDATION_CANDIDATE_CHANGED')
                physical(request, manager)
                staging.rename(target)
            finally:
                if staging.exists(): shutil.rmtree(staging)
        atomic(directory / 'frozen.json', {k: v for k, v in request.items() if k != 'candidateBase64'} | {'frozenWorkspace': frozen_id})
        candidate_file = directory / 'candidate.json'
        if candidate_file.exists() and candidate_file.read_bytes() != raw: raise ValueError('VALIDATION_ARTIFACT_CHANGED')
        atomic_bytes(candidate_file, raw)
        return frozen_id

def registered_profile():
    profile = json.loads(PROFILES.read_bytes()).get('agentos.validate', {})
    if profile.get('command') != '/usr/bin/python3' or profile.get('args') != [str(REGISTERED), 'validate'] or profile.get('effect') != 'read' or 'agentos' not in profile.get('projects', []):
        raise ValueError('VALIDATION_PROFILE_CHANGED')
    execution_registration()
    return profile

def execution_registration():
    layout = provenance.Layout()
    actual = sha(provenance.read(REGISTERED))
    if actual == REGISTERED_HASH and not layout.registry.exists():
        return {'registeredActionSha': REGISTERED_HASH}
    identity = provenance.active_registration(layout)
    record = provenance.registration_record(layout, identity)
    return {'registeredActionSha': record['executorSha256'], 'executionRegistration': identity}

def verified_execution_registration(receipt):
    """Historical CI retains its executor identity after a reviewed upgrade."""
    actual = receipt.get('registeredActionSha')
    if actual == REGISTERED_HASH and 'executionRegistration' not in receipt:
        return actual
    if actual not in provenance.REVIEWED_EXECUTORS:
        raise ValueError('VALIDATION_EXECUTOR_UNREVIEWED')
    registration = provenance.registration_record(provenance.Layout(), receipt.get('executionRegistration', {}))
    if registration['executorSha256'] != actual:
        raise ValueError('VALIDATION_EXECUTOR_UNREVIEWED')
    return actual

def diagnostic(output):
    log = next((item.get('content', '') for item in output.get('artifacts', []) if item.get('name') == 'validation.log'), '')
    log = re.sub(r'\b(?:postgres(?:ql)?|https?)://[^\s\"\'<>]+', '[private-url]', str(log))
    log = re.sub(r'(?i)(?:password|secret|api[_-]?key|access[_-]?token|DATABASE_URL)\s*[:=]\s*\S+', '[redacted]', log)
    log = re.sub(r'\b(?:sk|rk|ghp)-[\w.-]{16,}', '[redacted]', log)
    return log[-6000:]

def legacy_published_baseline(candidate_files=None, include_manifest=False):
    """Read protected release provenance, never assume a numbered release.

    Hashes are useful only after both the publication sidecar and the actual
    active release agree. Missing provenance is explicit, never guessed.
    """
    current = pathlib.Path('/opt/agentos/current').resolve()
    result = {'publishedBaseline': str(current), 'publishedBaselineDigest': None,
              'sourceDrift': {'known': False, 'code': 'PUBLISHED_SOURCE_MANIFEST_UNAVAILABLE'}}
    def protected(file):
        info = file.stat(follow_symlinks=False)
        return not file.is_symlink() and info.st_uid == 0 and info.st_mode & 0o022 == 0
    published_action = current/'scripts/agentos-release-action.py'
    if not published_action.exists() or not protected(published_action) or sha(published_action.read_bytes()) != REGISTERED_HASH: return result
    # The registered script executes main at import. Use its documented digest
    # algorithm locally instead, preserving its exclude rules byte for byte.
    h = hashlib.sha256()
    for file in sorted(current.rglob('*')):
        if file.is_file() and not file.is_symlink() and not any(part in ['node_modules','.local-test','.git','__pycache__'] for part in file.relative_to(current).parts):
            h.update(str(file.relative_to(current)).encode()); h.update(file.read_bytes())
    actual_release_digest = h.hexdigest()
    for publication in sorted(pathlib.Path('/var/lib/agentos-build').glob('*/framework-general-publication-proof.json')):
        if not protected(publication): continue
        value = json.loads(publication.read_bytes()); released = value.get('published', {}).get('output', {})
        if released.get('candidate') != str(current) or released.get('digest') != actual_release_digest: continue
        proof = pathlib.Path(value.get('validationProof', ''))
        if not proof.is_relative_to('/var/lib/agentos-build') or not protected(proof): continue
        raw = proof.read_bytes()
        if sha(raw) != value.get('validationProofSha256'): continue
        validated = json.loads(raw)
        if validated.get('release', {}).get('candidate') != str(current) or validated.get('release', {}).get('digest') != actual_release_digest: continue
        if validated.get('registeredActionSha256') != REGISTERED_HASH: continue
        receipt = pathlib.Path(validated.get('receipt', ''))
        if not receipt.is_relative_to('/var/lib/agentos/releases') or not protected(receipt): continue
        record = json.loads(receipt.read_bytes())
        if record.get('candidate') != str(current) or record.get('digest') != actual_release_digest or not record.get('publishedAt'): continue
        expected = validated.get('sourceHashes', {}) | validated.get('docsGitByteHashes', {})
        if not expected or any(not re.fullmatch('[a-f0-9]{64}', digest) for digest in expected.values()): continue
        for name, digest in expected.items():
            relative = pathlib.PurePosixPath(name)
            if relative.is_absolute() or any(part in ['..','.git','node_modules'] or part.startswith('.env') for part in relative.parts): raise ValueError('PUBLISHED_SOURCE_MANIFEST_INVALID')
            file = current / name
            if not protected(file) or sha(file.read_bytes()) != digest: raise ValueError('PUBLISHED_SOURCE_MANIFEST_CHANGED')
        compared_files = candidate_files if candidate_files is not None else {name:(current/name).read_bytes() for name in expected}
        missing = sorted(name for name in expected if name not in compared_files)
        changed = sorted(name for name in expected if name in compared_files and sha(compared_files[name]) != expected[name])
        extras = sorted(name for name in compared_files if name not in expected)
        result.update(publishedBaselineDigest=actual_release_digest,publicationProofRef=str(publication),publicationProofDigest=sha(publication.read_bytes()),validationProofRef=str(proof),validationProofDigest=sha(raw),
            registeredReceiptPath=str(receipt),registeredReceiptSha=sha(receipt.read_bytes()),registeredActionSha=REGISTERED_HASH,
            publishedActionPath=str(published_action),publishedActionSha=REGISTERED_HASH,
            registeredControlReady=REGISTERED.exists() and protected(REGISTERED) and sha(REGISTERED.read_bytes())==REGISTERED_HASH,
            sourceDrift={'known':True,'publishedSourceFiles':len(expected),'missingCount':len(missing),'changedCount':len(changed),'extraCount':len(extras),'missing':missing,'changed':changed,'extra':extras})
        if include_manifest:
            result.update(sourceHashes=expected,docsGitByteHashes=validated.get('docsGitByteHashes',{}),sourceManifestDigest=sha(canonical(expected)))
        break
    if pathlib.Path('/opt/agentos/current').resolve() != current: raise ValueError('PUBLISHED_BASE_CHANGED')
    return result

def published_baseline(candidate_files=None, include_manifest=False):
    proof = provenance.publication(provenance.Layout())
    if proof is None:
        return legacy_published_baseline(candidate_files, include_manifest)
    current = pathlib.Path(proof['release']['candidate'])
    expected = proof['sourceHashes']
    compared = candidate_files if candidate_files is not None else {name: provenance.read(current/name, 8*1024*1024) for name in expected}
    missing = sorted(name for name in expected if name not in compared)
    changed = sorted(name for name in expected if name in compared and sha(compared[name]) != expected[name])
    extras = sorted(name for name in compared if name not in expected)
    try:
        active = execution_registration()
    except (OSError, ValueError):
        active = {'registeredActionSha': None}
    result = {'publishedBaseline':str(current),'publishedBaselineDigest':proof['release']['digest'],
        'provenanceVersion':2,'provenanceKind':proof['kind'],
        'publicationProofRef':proof['publicationRef']['path'],'publicationProofDigest':proof['publicationRef']['sha256'],
        'validationProofRef':proof['validationRef']['path'],'validationProofDigest':proof['validationRef']['sha256'],
        'registeredReceiptPath':proof['registeredReceipt']['path'],'registeredReceiptSha':proof['registeredReceipt']['sha256'],
        'validationExecutorSha256':proof['validationExecutorSha256'],
        'publishedActionPath':str(current/'scripts/agentos-release-action.py'),'publishedActionSha':proof['publishedActionSha256'],
        **active,'registeredControlReady':active['registeredActionSha'] in provenance.REVIEWED_EXECUTORS,
        'sourceDrift':{'known':True,'publishedSourceFiles':len(expected),'missingCount':len(missing),
            'changedCount':len(changed),'extraCount':len(extras),'missing':missing,'changed':changed,'extra':extras}}
    if include_manifest:
        result.update(sourceHashes=expected,docsGitByteHashes={},sourceManifestDigest=sha(canonical(expected)))
    return result

def published_source_manifest():
    """Importable read-only provenance for the separate reviewed rebase broker.
    Every exposed path/hash is present and verified in the protected source
    manifest; generated release outputs are outside this source-file manifest.
    """
    value = published_baseline(include_manifest=True)
    if not value.get('sourceDrift',{}).get('known') or not value.get('sourceHashes'):
        raise ValueError('PUBLISHED_SOURCE_MANIFEST_UNAVAILABLE')
    value.update(sourceWorkspace='agentos/src'+value['publishedBaselineDigest'][:16],registeredSourceWorkspace='agentos/src'+value['publishedBaselineDigest'][:16])
    return value

def inspect_published_baseline():
    """Fixed, read-only baseline tuple consumed by the rebase inspector."""
    return published_source_manifest()

def execute(request, directory, frozen_id, executor=None, baseline=None):
    if executor is not None:  # Isolated tests supply a process, never the CLI.
        return execute_bound(request, directory, frozen_id, executor, baseline)
    with provenance.execution_lock(provenance.Layout()):
        profile = registered_profile()
        identity = execution_registration()
        return execute_bound(request, directory, frozen_id, profile, (baseline or {}) | identity)

def execute_bound(request, directory, frozen_id, executor=None, baseline=None):
    # This started marker is fsynced BEFORE crossing the command boundary.
    # A killed owner is uncertain and cannot authorize a second invocation.
    file = directory / 'receipt.json'
    started = {'version': 1, 'actionId': request['actionId'], 'requestDigest': sha(canonical(request)), 'state': 'running', 'candidateDigest': request['candidateDigest'], 'validatedCandidateDigest': request['candidateDigest'], 'frozenWorkspace': frozen_id, **(baseline or {})}
    atomic(file, started)
    try:
        profile = registered_profile() if executor is None else executor
        parameters = {'workspace': frozen_id}
        if request.get('authority') == 'runtime_artifact':
            runtime_validation_authority(request)
            parameters['validationCoverage'] = {'version':1,'profile':'agentos.validate',
                'candidateDigest':request['candidateDigest'],
                'requiredDatabaseTests':request['runtimeAuthority']['policyRegistration']['policy']['requiredDatabaseTests']}
        payload = {'occurrenceId': request['actionId'], 'attemptId': request['actionId'], 'parameters': parameters, 'results': []}
        completed = subprocess.run([profile['command'], *profile['args']], input=json.dumps(payload), capture_output=True, text=True, timeout=1900)
        if completed.returncode != 0:
            refusal = re.search(r'^ValueError: ([A-Z_]+)$', completed.stderr.strip())
            result = {'state': 'failed', 'code': 'VALIDATION_COMMAND_FAILED', 'exitCode': completed.returncode, 'passed': False,
                      **({'executorCode': refusal.group(1)} if refusal else {})}
        else:
            output = json.loads(completed.stdout)
            passed = output.get('ok') is True and output.get('output', {}).get('passed') is not False and bool(re.fullmatch('[a-f0-9]{64}', output.get('output', {}).get('digest', '')))
            baseline_changed = bool(baseline and output.get('output', {}).get('base') and output['output']['base'] != baseline.get('publishedBaseline'))
            if baseline_changed: passed = False
            # Keep actual result/log bytes, root-owned. Management receives a
            # digest/ref, never credentials or arbitrary raw command stderr.
            atomic_bytes(directory / 'result.json', canonical(output))
            result = {'state': 'completed', 'code': 'VALIDATION_BASE_CHANGED' if baseline_changed else ('VALIDATION_PASSED' if passed else 'VALIDATION_CHECKS_FAILED'), 'passed': passed, 'validatedReleaseDigest': output.get('output', {}).get('digest'), 'resultDigest': sha(canonical(output)), 'resultRef': str(directory / 'result.json'), 'diagnostic': diagnostic(output)}
    except subprocess.TimeoutExpired:
        result = {'state': 'uncertain', 'code': 'VALIDATION_COMMAND_TIMEOUT', 'passed': False}
    except ValueError as error:
        safe_code = str(error) if re.fullmatch('[A-Z_]+',str(error)) else 'VALIDATION_RESULT_FORMAT'
        result = {'state': 'failed', 'code': 'VALIDATION_EXECUTOR_FAILED', 'executorCode': safe_code, 'passed': False}
    except (KeyError, OSError):
        result = {'state': 'failed', 'code': 'VALIDATION_EXECUTOR_FAILED', 'passed': False}
    atomic(file, started | result)
    return started | result

def process(request, mode):
    if mode not in ['freeze', 'execute', 'receipt']: raise ValueError('VALIDATION_MODE_INVALID')
    STATE.mkdir(parents=True, exist_ok=True, mode=0o700)
    directory = STATE / request['actionId']; directory.mkdir(exist_ok=True, mode=0o700)
    with open(directory / 'operation.lock', 'a') as handle:
        import fcntl
        if mode == 'receipt':
            file = directory / 'receipt.json'
            frozen = directory / 'frozen.json'
            if file.exists(): result = json.loads(file.read_bytes())
            elif frozen.exists():
                value = json.loads(frozen.read_bytes())
                if {k:v for k,v in value.items() if k != 'frozenWorkspace'} != {k:v for k,v in request.items() if k != 'candidateBase64'}:
                    raise ValueError('VALIDATION_IDEMPOTENCY_CONFLICT')
                result = {'state': 'frozen', 'actionId': request['actionId'], 'requestDigest': sha(canonical(request)), 'candidateDigest': request['candidateDigest'], 'frozenWorkspace': value['frozenWorkspace']}
            else: result = {'state': 'missing', 'actionId': request['actionId']}
            print(json.dumps(result)); return
        fcntl.flock(handle, fcntl.LOCK_EX)
        intent = directory / 'request.json'
        if intent.exists() and intent.read_bytes() != canonical(request): raise ValueError('VALIDATION_IDEMPOTENCY_CONFLICT')
        if not intent.exists(): atomic(intent, request)
        receipt = directory / 'receipt.json'
        if receipt.exists():
            previous = json.loads(receipt.read_bytes())
            if previous['state'] == 'running': previous = previous | {'state': 'uncertain', 'code': 'VALIDATION_OWNER_INTERRUPTED', 'passed': False}
            print(json.dumps(previous)); return
        if mode == 'freeze':
            frozen = freeze(request, directory, load_workspace_manager())
            print(json.dumps({'state': 'frozen', 'actionId': request['actionId'], 'requestDigest': sha(canonical(request)), 'candidateDigest': request['candidateDigest'], 'frozenWorkspace': frozen})); return
        frozen = json.loads((directory / 'frozen.json').read_bytes())
        manager = load_workspace_manager()
        with manager.locks([frozen['frozenWorkspace']]):
            files = manager.sources(manager.workspace(frozen['frozenWorkspace']))
            if manager.source_hash(files) != request['candidateDigest']: raise ValueError('VALIDATION_FROZEN_CHANGED')
            baseline = published_baseline(files)
            print(json.dumps(execute(request, directory, frozen['frozenWorkspace'], baseline=baseline)))

def main():
    request = json.load(sys.stdin); identity(request)
    mode = sys.argv[1]
    try: process(request, mode)
    except Exception as error:
        # A refused clone has no validation command effect. If a started
        # receipt exists, retain it instead of relabelling an ambiguous effect.
        file = STATE / request['actionId'] / 'receipt.json'
        if file.exists(): result = json.loads(file.read_bytes())
        else:
            safe_code = str(error) if isinstance(error, ValueError) and re.fullmatch('[A-Z_]+', str(error)) else 'VALIDATION_BROKER_FAILED'
            result = {'version':1,'actionId':request['actionId'],'candidateDigest':request['candidateDigest'],
                'requestDigest':sha(canonical(request)),'state':'rejected' if isinstance(error,ValueError) else 'uncertain',
                'code':safe_code,'passed':False}
            if file.parent.exists(): atomic(file,result)
        print(json.dumps(result))

if __name__ == '__main__':
    try: main()
    except Exception as error:
        # Unexpected failures might have crossed a command boundary. The
        # controller keeps uncertain ownership rather than admitting a retry.
        print(json.dumps({'state': 'uncertain', 'code': str(error) if isinstance(error, ValueError) else 'VALIDATION_BROKER_FAILED', 'passed': False}))
