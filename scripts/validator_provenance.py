"""Versioned, root-owned validator registration and verifiable publication receipts.

This module never runs candidate code. The two executor identities below are
reviewed public source, not values supplied by a worker or an editable allowlist.
Operator receipts and native broker receipts are distinct proof formats; neither
is rewritten into a historical framework-general sidecar.
"""
import contextlib
import fcntl
import hashlib
import json
import math
import os
import pathlib
import re
import stat
import time
import uuid

LEGACY_SHA = 'e66fcf513aaa513ff2216d87556fb5dba771597dc933ca4b4b38573d8842d5b5'
REVIEWED_SHA = '02a92158a4084a1aeb3b4c55998b6740d90428f80acb06a225730a1423ba9c26'
REVIEWED_COMMIT = 'd52ef6b603e637bf301ac1ab0360a72dfd6912a6'
# A later executor requires a reviewed code change adding its exact public
# source identity here. Neither CLI input nor a writable registry can add one.
HISTORICAL_EXECUTORS = {REVIEWED_SHA: REVIEWED_COMMIT}
# Only complete reviewed bundles are admissible for new execution. Historical
# scripts remain identifiable without becoming eligible for re-registration.
REVIEWED_BUNDLES = {
    'cf4351977a994457dd0b74da8449c05597479ba4410e3015e499d7359d094383': {
        'sourceCommit':'d65e4801eef3412b7ff234fbf81c618acd512373',
        'files':{
            'agentos-release-action.py':'cf4351977a994457dd0b74da8449c05597479ba4410e3015e499d7359d094383',
            'candidate_origin.py':'742212cdc76a1404418ad9fa0b57215d377eea4433f77bc2cd29eed9e2a27e6f',
        },
    },
    'ed727294a54b6df325befc93b3c700350baf56713083806781ceafeb952064c1': {
        'sourceCommit':'dc442efa9e61eda58b8e10c81277b77ea02cc6c0',
        'files':{
            'agentos-release-action.py':'ed727294a54b6df325befc93b3c700350baf56713083806781ceafeb952064c1',
            'candidate_origin.py':'742212cdc76a1404418ad9fa0b57215d377eea4433f77bc2cd29eed9e2a27e6f',
        },
    },
    '39843bdbde81e84b5093ed6667484378d1f4444c66aa33424b52d28b45b0cc44': {
        'sourceCommit':'084e0459a7d045c45c22b8159c60dd5742df2acb',
        'files':{
            'agentos-release-action.py':'39843bdbde81e84b5093ed6667484378d1f4444c66aa33424b52d28b45b0cc44',
            'candidate_origin.py':'742212cdc76a1404418ad9fa0b57215d377eea4433f77bc2cd29eed9e2a27e6f',
        },
    },
}
REVIEWED_EXECUTORS = {**HISTORICAL_EXECUTORS,
                      **{key:value['sourceCommit'] for key,value in REVIEWED_BUNDLES.items()}}
# Exact legacy bytes with only __pycache__ excluded from digest. It is a
# permitted migration preimage, never an executor authorized for new CI.
PARTIAL_PREIMAGE = '783eee1dac521a54a351032c73f3335b7a322ab6419c50bd6ce4fc8fec69dbd3'
DIGEST_SKIP = {'.git', 'node_modules', '.local-test', '__pycache__'}
SOURCE_SKIP = DIGEST_SKIP | {'evidence', 'data', 'assets', '.agentos-snapshot.json',
                            'test-results', 'playwright-report'}
OWNER_UID = 0
PROTECTION_ROOT = pathlib.Path('/')
MAX_JSON = 16 * 1024 * 1024


class Layout:
    def __init__(self, root=pathlib.Path('/')):
        self.root = pathlib.Path(root)
        self.active = self.root / 'opt/agentos/current'
        self.releases = self.root / 'opt/agentos/releases'
        self.receipts = self.root / 'var/lib/agentos/releases'
        self.builds = self.root / 'var/lib/agentos-build'
        self.validation_builds = self.root / 'var/lib/agentos-validation-builds'
        self.operator_records = self.root / 'var/lib/agentos-operator-releases'
        self.broker = self.root / 'var/lib/agentos-bridge/candidate-validations'
        self.control = self.root / 'opt/agentos-control/agentos-release-action.py'
        self.registry = self.root / 'opt/agentos-control/validator-registrations'


def fail(code):
    raise ValueError(code)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()


def protected(path, directory=False):
    path = pathlib.Path(path)
    if not path.is_absolute() or not path.is_relative_to(PROTECTION_ROOT):
        fail('VALIDATOR_PROVENANCE_PATH')
    # All ancestors as well as the leaf must be protected. Test fixtures set
    # OWNER_UID and use a private root; production always requires root.
    for p in [path, *path.parents]:
        info = p.lstat()
        if (stat.S_ISLNK(info.st_mode) or info.st_uid != OWNER_UID or info.st_mode & 0o022):
            fail('VALIDATOR_PROVENANCE_UNPROTECTED')
        if p == path:
            if not (stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode)):
                fail('VALIDATOR_PROVENANCE_NOT_REGULAR')
        if p == PROTECTION_ROOT:
            break
    return path


def read(path, limit=MAX_JSON):
    path = protected(path)
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_uid != OWNER_UID or info.st_mode & 0o022:
            fail('VALIDATOR_PROVENANCE_UNPROTECTED')
        raw = stream.read(limit + 1)
    if len(raw) > limit:
        fail('VALIDATOR_PROVENANCE_TOO_LARGE')
    return raw


def document(path):
    raw = read(path)
    value = json.loads(raw)
    if not isinstance(value, dict):
        fail('VALIDATOR_PROVENANCE_FORMAT')
    return value, {'path': str(path), 'sha256': sha(raw)}


def release_path(layout, value):
    path = pathlib.Path(value)
    if path.parent != layout.releases or not re.fullmatch(r'managed-[0-9a-f-]{36}', path.name):
        fail('VALIDATOR_RELEASE_PATH')
    uuid.UUID(path.name[8:])
    protected(path, directory=True)
    return path


def tree_digest(root):
    digest = hashlib.sha256()
    for file in sorted(pathlib.Path(root).rglob('*')):
        if any(p in DIGEST_SKIP for p in file.relative_to(root).parts):
            continue
        if file.is_symlink():
            continue  # Same digest contract as workerSourceDigest/release guard.
        if file.is_file():
            digest.update(file.relative_to(root).as_posix().encode())
            digest.update(read(file, 32 * 1024 * 1024))
    return digest.hexdigest()


def source_manifest(root):
    result = {}
    for folder, dirs, files in os.walk(root, followlinks=False):
        dirs[:] = sorted(d for d in dirs if d not in SOURCE_SKIP and not d.startswith('.env'))
        for d in dirs:
            protected(pathlib.Path(folder) / d, directory=True)
        for name in sorted(files):
            if name in SOURCE_SKIP or name.startswith('.env') or name.endswith(('.log', '.pyc')):
                continue
            file = pathlib.Path(folder) / name
            result[file.relative_to(root).as_posix()] = sha(read(file, 8 * 1024 * 1024))
    if not result or len(result) > 4096:
        fail('VALIDATOR_SOURCE_MANIFEST_INVALID')
    return result


def complete_output(value, report_path=None):
    if value.get('ok') is not True or value.get('output', {}).get('passed') is False:
        fail('VALIDATOR_VALIDATION_FAILED')
    logs = [x.get('content') for x in value.get('artifacts', []) if x.get('name') == 'validation.log']
    if len(logs) != 1 or not isinstance(logs[0], str):
        fail('VALIDATOR_VALIDATION_LOG_MISSING')
    text = re.sub(r'\x1b\[[0-?]*[ -/]*[@-~]', '', logs[0])
    # Match the registered guard's completion contract, not a naked passed flag.
    # A separate task criterion may demand zero skipped mandatory DB cases.
    # The reviewed executor retains only the last 100000 characters after
    # validating the complete build+suite output. A truncated suffix is not a
    # complete build log; the native protected receipt binds that guard result.
    if not re.search(r'\bbuilt in\s+\S+', text) and len(logs[0]) != 100000:
        fail('VALIDATOR_VALIDATION_INCOMPLETE')
    for label in ['Test Files', 'Tests']:
        matches = re.findall(r'^\s*' + label + r'\s+(.+)$', text, re.M)
        summary = matches[-1].strip() if matches else ''
        total = re.search(r'\((\d+)\)\s*$', summary)
        terms = re.findall(r'(\d+)\s+(passed|skipped|todo)', summary)
        residue = re.sub(r'\d+\s+(?:passed|skipped|todo)|\(\d+\)|[|\s]', '', summary)
        if not total or residue or sum(int(n) for n, _ in terms) != int(total.group(1)) or not any(int(n) > 0 and kind == 'passed' for n, kind in terms):
            fail('VALIDATOR_VALIDATION_INCOMPLETE')
    durations = list(re.finditer(r'^\s*Duration\s+[^\n]+', text, re.M))
    tail = text[durations[-1].end():].strip() if durations else ''
    if report_path is not None and tail == 'JSON report written to ' + str(report_path):
        tail = ''
    if not durations or tail:
        fail('VALIDATOR_VALIDATION_INCOMPLETE')


def executor_artifacts(value, executor):
    output = value.get('output', {})
    if executor not in (LEGACY_SHA, REVIEWED_SHA) or 'executorSha256' in output:
        if output.get('executorSha256') != executor:
            fail('VALIDATOR_VALIDATION_EXECUTOR_CHANGED')
        for key, name in [('testReportDigest', 'vitest-results.json'), ('coverageDigest', 'validation-coverage.json')]:
            if key == 'coverageDigest' and key not in output:
                continue
            artifacts = [x for x in value.get('artifacts', []) if x.get('name') == name]
            if len(artifacts) != 1 or not isinstance(artifacts[0].get('content'), str):
                fail('VALIDATOR_VALIDATION_ARTIFACT_MISSING')
            raw = artifacts[0]['content'].encode()
            if len(raw) > 8 * 1024 * 1024 or sha(raw) != output.get(key):
                fail('VALIDATOR_VALIDATION_ARTIFACT_CHANGED')


def record_matches(layout, current, record, validated):
    for key in set(validated) | {'candidate', 'digest', 'base', 'validatedAt'}:
        if record.get(key) != validated.get(key):
            fail('VALIDATOR_PUBLICATION_RECEIPT_MISMATCH')
    if record.get('candidate') != str(current) or record.get('previous') != record.get('base'):
        fail('VALIDATOR_PUBLICATION_RECEIPT_MISMATCH')
    for key in ['validatedAt', 'publishedAt']:
        if type(record.get(key)) not in (int, float) or not math.isfinite(record[key]) or record[key] <= 0:
            fail('VALIDATOR_PUBLICATION_TIME_INVALID')
    if record['publishedAt'] < record['validatedAt'] or record['publishedAt'] > time.time() + 5:
        fail('VALIDATOR_PUBLICATION_TIME_INVALID')
    if tree_digest(current) != record.get('digest'):
        fail('VALIDATOR_PUBLISHED_BYTES_CHANGED')
    return release_path(layout, record['base'])


def registered_executor_at(layout, declared, timestamp):
    if not isinstance(declared, dict) or set(declared) != {'path','sha256','sourceCommit','registration'}:
        fail('VALIDATOR_OPERATOR_EXECUTOR_UNBOUND')
    executor = declared['sha256']
    if (executor not in REVIEWED_BUNDLES or declared['sourceCommit'] != REVIEWED_EXECUTORS.get(executor) or
            pathlib.Path(declared['path']) != layout.control):
        fail('VALIDATOR_OPERATOR_EXECUTOR_UNBOUND')
    registered = registration_record(layout, declared['registration'])
    installed = registered.get('installedAt')
    if (registered['executorSha256'] != executor or registered.get('version') != 2 or
            type(installed) not in (int,float) or not math.isfinite(installed) or installed <= 0 or installed > timestamp):
        fail('VALIDATOR_OPERATOR_EXECUTOR_UNBOUND')
    # A historically superseded registration is valid only before its successor
    # was installed. Inspect immutable records, not today's mutable pointer.
    rid = declared['registration']['registrationId']
    for path in layout.registry.glob('*.json'):
        if path.name == 'current.json' or path.name.endswith('.intent.json') or path.stem == rid:
            continue
        other, ref = document(path)
        if other.get('state') != 'installed' or other.get('kind') != 'reviewed_validator_transition':
            continue
        installed_after = other.get('installedAt')
        if (type(installed_after) in (int,float) and math.isfinite(installed_after) and
                installed < installed_after <= timestamp):
            registration_record(layout, {'version':1,'registrationId':other.get('registrationId'),
                                         'registrationDigest':ref['sha256']})
            fail('VALIDATOR_OPERATOR_EXECUTOR_SUPERSEDED')
    return executor


def operator_proof(layout, current, directory):
    published, p = document(directory / 'publish.receipt.json')
    if published.get('output', {}).get('candidate') != str(current):
        return None
    validated, v = document(directory / 'validate.receipt.json')
    vi, vir = document(directory / 'validate.intent.json')
    pi, pir = document(directory / 'publish.intent.json')
    inp, ir = document(directory / 'release-input.json')
    if (vi.get('action') != 'validate' or pi.get('action') != 'publish' or
            vi.get('input') != inp or pi.get('input') != inp or vi.get('base') != pi.get('base')):
        fail('VALIDATOR_OPERATOR_INTENT_CHANGED')
    occurrence, attempt = str(uuid.UUID(inp['occurrenceId'])), str(uuid.UUID(inp['attemptId']))
    if current.name != 'managed-' + attempt:
        fail('VALIDATOR_OPERATOR_INTENT_CHANGED')
    record, r = document(layout.receipts / (occurrence + '.json'))
    if published.get('ok') is not True or published.get('output') != record:
        fail('VALIDATOR_PUBLICATION_RECEIPT_MISMATCH')
    base = record_matches(layout, current, record, validated.get('output', {}))
    if str(base) != vi.get('base'):
        fail('VALIDATOR_OPERATOR_INTENT_CHANGED')
    declared = vi.get('executor')
    publisher = pi.get('executor')
    modern = 'executorSha256' in validated.get('output', {})
    if modern or declared is not None or pi.get('executor') is not None:
        executor = registered_executor_at(layout, declared, record['validatedAt'])
        registered_executor_at(layout, publisher, record['publishedAt'])
        if validated.get('output',{}).get('executorSha256') != executor:
            fail('VALIDATOR_OPERATOR_EXECUTOR_UNBOUND')
        # Old equal-executor receipts remain byte-for-byte historical evidence.
        # A transition requires the publisher's own captured active authority;
        # neither validation output nor its executor field is rewritten.
        captured = record.get('publicationExecutor')
        if ((publisher != declared and captured != publisher) or
                (captured is not None and captured != publisher) or
                'publicationExecutor' in validated.get('output',{})):
            fail('VALIDATOR_OPERATOR_EXECUTOR_UNBOUND')
    else:
        # Only legacy receipts without modern identity use the historical base.
        executor = sha(read(base / 'scripts/agentos-release-action.py'))
        if executor != LEGACY_SHA and executor not in HISTORICAL_EXECUTORS:
            fail('VALIDATOR_HISTORICAL_EXECUTOR_UNREVIEWED')
    complete_output(validated, (layout.validation_builds if executor in REVIEWED_BUNDLES else layout.builds) / attempt / '.local-test/vitest-results.json')
    executor_artifacts(validated, executor)
    for action, timestamp in [('validate', record['validatedAt']), ('publish', record['publishedAt'])]:
        if (directory / (action + '.intent.json')).stat().st_mtime > timestamp + 2 or (directory / (action + '.receipt.json')).stat().st_mtime + 2 < timestamp:
            fail('VALIDATOR_OPERATOR_TIME_UNBOUND')
    return {'version': 2, 'kind': 'operator_guard_receipts', 'release': record,
            'validationExecutorSha256': executor, 'publicationRef': p, 'validationRef': v,
            'registeredReceipt': r, 'evidence': [p, v, vir, pir, ir, r]}


def registration_record(layout, identity):
    if set(identity) != {'version', 'registrationId', 'registrationDigest'} or identity.get('version') != 1:
        fail('VALIDATOR_REGISTRATION_REFERENCE_INVALID')
    rid = str(uuid.UUID(identity['registrationId']))
    record, ref = document(layout.registry / (rid + '.json'))
    if ref['sha256'] != identity['registrationDigest']:
        fail('VALIDATOR_REGISTRATION_CHANGED')
    if (record.get('version') not in (1,2) or record.get('kind') != 'reviewed_validator_transition' or
            record.get('registrationId') != rid or record.get('executorSha256') not in REVIEWED_EXECUTORS or
            record.get('sourceCommit') != REVIEWED_EXECUTORS.get(record.get('executorSha256')) or record.get('state') != 'installed' or
            record.get('previousExecutorSha256') not in {LEGACY_SHA, PARTIAL_PREIMAGE, *REVIEWED_EXECUTORS}):
        fail('VALIDATOR_REGISTRATION_INVALID')
    evidence = record.get('publicationEvidence', {}).get('evidence', [])
    if not evidence:
        fail('VALIDATOR_REGISTRATION_EVIDENCE_MISSING')
    if record['version'] == 1:
        if record['executorSha256'] not in HISTORICAL_EXECUTORS:
            fail('VALIDATOR_REGISTRATION_INVALID')
        for ref in evidence:
            if sha(read(ref['path'])) != ref['sha256']:
                fail('VALIDATOR_REGISTRATION_EVIDENCE_CHANGED')
    else:
        bundle = REVIEWED_BUNDLES.get(record['executorSha256'])
        if (not bundle or record.get('bundleManifest') != bundle['files'] or
                record.get('bundleDigest') != sha(canonical(bundle['files'])) or
                record.get('sourceKind') not in ('published_runtime','reviewed_public_source')):
            fail('VALIDATOR_REGISTRATION_BUNDLE_INVALID')
        validate_archives(layout, rid, record.get('evidenceArchive'), evidence)
        source_evidence = record.get('sourceEvidence', [])
        if (len(source_evidence) != len(bundle['files']) or
                {ref.get('name'):ref.get('sha256') for ref in source_evidence} != bundle['files']):
            fail('VALIDATOR_REGISTRATION_SOURCE_CHANGED')
        validate_archives(layout, rid, record.get('sourceArchive'), source_evidence)
    intent, intent_ref = document(layout.registry / (rid + '.intent.json'))
    if (intent_ref['sha256'] != record.get('intentSha256') or intent.get('registrationId') != rid or
            intent.get('expectedRegisteredSha256') != record['previousExecutorSha256'] or
            intent.get('expectedReleaseDigest') != record['publicationEvidence'].get('release', {}).get('digest') or
            intent.get('expectedPublicationProofDigest') != sha(canonical(record['publicationEvidence']))):
        fail('VALIDATOR_REGISTRATION_INTENT_CHANGED')
    if record.get('sourceKind') == 'reviewed_public_source' and (
            intent.get('executorSha256') != record['executorSha256'] or intent.get('sourceCommit') != record['sourceCommit']):
        fail('VALIDATOR_REGISTRATION_INTENT_CHANGED')
    return record


def validate_archives(layout, rid, archives, originals):
    if not isinstance(archives,list) or len(archives) != len(originals):
        fail('VALIDATOR_REGISTRATION_ARCHIVE_MISSING')
    expected = {(item['path'],item['sha256']) for item in originals}
    actual = set()
    for item in archives:
        if set(item) != {'path','sha256','archivedPath'}:
            fail('VALIDATOR_REGISTRATION_ARCHIVE_INVALID')
        path = pathlib.Path(item['archivedPath'])
        if path != layout.registry/(rid+'.evidence')/(item['sha256']+'.blob'):
            fail('VALIDATOR_REGISTRATION_ARCHIVE_INVALID')
        if sha(read(path)) != item['sha256']:
            fail('VALIDATOR_REGISTRATION_EVIDENCE_CHANGED')
        actual.add((item['path'],item['sha256']))
    if actual != expected or len(actual) != len(archives):
        fail('VALIDATOR_REGISTRATION_ARCHIVE_INVALID')


def preserve_evidence(layout, rid, refs):
    folder = layout.registry/(rid+'.evidence')
    folder.mkdir(mode=0o700, exist_ok=True)
    protected(folder, directory=True)
    archived = []
    for ref in refs:
        raw = read(ref['path'])
        if sha(raw) != ref['sha256']:
            fail('VALIDATOR_REGISTRATION_EVIDENCE_CHANGED')
        dest = folder/(ref['sha256']+'.blob')
        if dest.exists():
            if read(dest) != raw:
                fail('VALIDATOR_REGISTRATION_EVIDENCE_CHANGED')
        else:
            atomic(dest, raw)
        archived.append({'path':ref['path'],'sha256':ref['sha256'],'archivedPath':str(dest)})
    return archived


def verify_installed_bytes(layout, record):
    manifest = record.get('bundleManifest', {'agentos-release-action.py':record['executorSha256']})
    for name, expected in manifest.items():
        if sha(read(layout.control.parent / name)) != expected:
            fail('VALIDATION_EXECUTOR_CHANGED')


def active_registration(layout, required_executor=None, for_execution=False):
    pointer, _ = document(layout.registry / 'current.json')
    record = registration_record(layout, pointer)
    if ((required_executor is not None and record['executorSha256'] != required_executor) or
            (for_execution and record['executorSha256'] not in REVIEWED_BUNDLES)):
        fail('VALIDATOR_EXECUTOR_HISTORICAL_ONLY')
    verify_installed_bytes(layout, record)
    return pointer


def broker_proof(layout, current, directory):
    result, result_ref = document(directory / 'result.json')
    if result.get('output', {}).get('candidate') != str(current):
        return None
    receipt, receipt_ref = document(directory / 'receipt.json')
    request, request_ref = document(directory / 'request.json')
    action = str(uuid.UUID(directory.name))
    if (receipt.get('actionId') != action or request.get('actionId') != action or
            receipt.get('state') != 'completed' or receipt.get('passed') is not True or
            receipt.get('registeredActionSha') not in REVIEWED_EXECUTORS or
            receipt.get('resultDigest') != result_ref['sha256'] or
            receipt.get('requestDigest') != sha(canonical(request)) or
            receipt.get('resultRef') != str(directory / 'result.json')):
        fail('VALIDATOR_BROKER_RECEIPT_UNBOUND')
    registration = registration_record(layout, receipt.get('executionRegistration', {}))
    if registration['executorSha256'] != receipt['registeredActionSha']:
        fail('VALIDATOR_BROKER_RECEIPT_UNBOUND')
    complete_output(result, (layout.validation_builds if receipt['registeredActionSha'] in REVIEWED_BUNDLES else layout.builds) / action / '.local-test/vitest-results.json')
    executor_artifacts(result, receipt['registeredActionSha'])
    record, rr = document(layout.receipts / (action + '.json'))
    record_matches(layout, current, record, result.get('output', {}))
    if 'publicationExecutor' in record:
        registered_executor_at(layout, record['publicationExecutor'], record['publishedAt'])
    return {'version': 2, 'kind': 'registered_broker_receipts', 'release': record,
            'validationExecutorSha256': receipt['registeredActionSha'], 'publicationRef': rr,
            'validationRef': result_ref, 'registeredReceipt': rr,
            'evidence': [rr, result_ref, receipt_ref, request_ref]}


def publication(layout):
    current = release_path(layout, layout.active.resolve(strict=True))
    action_sha = sha(read(current / 'scripts/agentos-release-action.py'))
    if action_sha not in REVIEWED_EXECUTORS:
        return None  # Legacy publication parser retains its existing contract.
    if action_sha in REVIEWED_BUNDLES:
        for name, expected in REVIEWED_BUNDLES[action_sha]['files'].items():
            if sha(read(current/'scripts'/name)) != expected:
                fail('VALIDATOR_PUBLISHED_BUNDLE_CHANGED')
    found = []
    operator_paths = [*layout.builds.glob('*/publish.receipt.json'), *layout.operator_records.glob('*/publish.receipt.json')]
    for path in sorted(operator_paths):
        try:
            advertised, _ = document(path)
        except (OSError, ValueError):
            continue  # An unreadable/unprotected file is never proof.
        if advertised.get('output', {}).get('candidate') != str(current):
            continue
        # Unrelated protected receipts are not authority for this release.
        value = operator_proof(layout, current, path.parent)
        if value:
            found.append(value)
    for path in sorted(layout.broker.glob('*/result.json')):
        try:
            advertised, _ = document(path)
        except (OSError, ValueError):
            continue
        if advertised.get('output', {}).get('candidate') != str(current):
            continue
        value = broker_proof(layout, current, path.parent)
        if value:
            found.append(value)
    if not found:
        fail('PUBLISHED_SOURCE_MANIFEST_UNAVAILABLE')
    identities = {(x['release']['digest'], x['validationExecutorSha256']) for x in found}
    if len(identities) != 1:
        fail('VALIDATOR_PUBLICATION_AMBIGUOUS')
    proof = found[0]
    proof.update(publishedActionSha256=action_sha, sourceHashes=source_manifest(current))
    if layout.active.resolve(strict=True) != current:
        fail('PUBLISHED_BASE_CHANGED')
    return proof


def atomic(path, raw):
    temporary = path.with_name(path.name + '.tmp-' + uuid.uuid4().hex)
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'wb') as f:
        f.write(raw); f.flush(); os.fsync(f.fileno())
    os.replace(temporary, path)
    fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


@contextlib.contextmanager
def locks(layout):
    with contextlib.ExitStack() as stack:
        for path in [layout.control.parent / 'validator-registration.lock',
                     layout.receipts / 'publication.lock', layout.validation_builds / 'validation.lock']:
            protected(path.parent, directory=True)
            fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
            handle = stack.enter_context(os.fdopen(fd, 'a'))
            protected(path)
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                fail('VALIDATOR_REGISTRATION_BUSY')
        yield


@contextlib.contextmanager
def execution_lock(layout):
    path = layout.control.parent / 'validator-registration.lock'
    protected(path.parent, directory=True)
    fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'a') as handle:
        protected(path)
        fcntl.flock(handle, fcntl.LOCK_SH)
        yield


def ensure_validation_build_root(layout):
    path = layout.validation_builds
    protected(path.parent, directory=True)
    path.mkdir(mode=0o711, exist_ok=True)
    protected(path, directory=True)
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        info = os.fstat(fd)
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != OWNER_UID or info.st_mode & 0o022:
            fail('VALIDATOR_PROVENANCE_UNPROTECTED')
        # mkdir's mode is masked by the launching operator's umask. Only this
        # fixed, protected CI root gains traversal; no ancestor is chmodded.
        os.fchmod(fd, 0o711)
    finally:
        os.close(fd)


def register(layout, request, after_replace=None):
    """Explicit reviewed transition; no automatic trust-on-first-use or reload.

    Crash between replacement and pointer publication fails closed. Replaying
    the SAME immutable intent reconciles that transition without running CI.
    """
    if os.geteuid() != OWNER_UID:
        fail('VALIDATOR_REGISTRATION_PRIVILEGE_REQUIRED')
    basic = {'registrationId', 'expectedRegisteredSha256', 'expectedReleaseDigest', 'expectedPublicationProofDigest'}
    reviewed_source = set(request) == basic | {'executorSha256','sourceCommit'}
    if not reviewed_source and set(request) != basic:
        fail('VALIDATOR_REGISTRATION_REQUEST_INVALID')
    rid = str(uuid.UUID(request['registrationId']))
    if request['expectedRegisteredSha256'] not in {LEGACY_SHA, PARTIAL_PREIMAGE, *REVIEWED_EXECUTORS}:
        fail('VALIDATOR_REGISTRATION_PREIMAGE_UNREVIEWED')
    protected(layout.control.parent, directory=True)
    ensure_validation_build_root(layout)
    with locks(layout):
        for path in layout.broker.glob('*/receipt.json'):
            prior, _ = document(path)
            if prior.get('state') in ('running', 'uncertain'):
                fail('VALIDATOR_REGISTRATION_EXECUTION_UNCERTAIN')
        intent_path = layout.registry / (rid + '.intent.json')
        record_path = layout.registry / (rid + '.json')
        if record_path.exists():
            # Lost replies reconcile the installed transition, even after a
            # later publication. They never roll a newer pointer backwards.
            intent, _ = document(intent_path)
            if intent != request:
                fail('VALIDATOR_REGISTRATION_IDEMPOTENCY_CONFLICT')
            record, ref = document(record_path)
            pointer = {'version':1,'registrationId':rid,'registrationDigest':ref['sha256']}
            registration_record(layout, pointer)
            if (layout.registry/'current.json').exists():
                current, _ = document(layout.registry/'current.json')
                if current != pointer:
                    fail('VALIDATOR_REGISTRATION_SUPERSEDED')
            verify_installed_bytes(layout, record)
            atomic(layout.registry/'current.json', canonical(pointer))
            return pointer
        proof = publication(layout)
        if (not proof or proof['release']['digest'] != request['expectedReleaseDigest'] or
                sha(canonical(proof)) != request['expectedPublicationProofDigest']):
            fail('VALIDATOR_REGISTRATION_PROOF_CHANGED')
        if reviewed_source:
            target_sha = request.get('executorSha256')
            bundle = REVIEWED_BUNDLES.get(target_sha)
            if not bundle or request.get('sourceCommit') != bundle['sourceCommit']:
                fail('VALIDATOR_REGISTRATION_SOURCE_UNREVIEWED')
            source = layout.control.parent/'reviewed-validator-bundles'/target_sha
            source_kind = 'reviewed_public_source'
        else:
            source = pathlib.Path(proof['release']['candidate'])/'scripts'
            target_sha = sha(read(source/'agentos-release-action.py'))
            bundle = REVIEWED_BUNDLES.get(target_sha)
            source_kind = 'published_runtime'
        if not bundle:
            fail('VALIDATOR_REGISTRATION_SOURCE_UNREVIEWED')
        if (set(bundle['files']) != {'agentos-release-action.py','candidate_origin.py'} or
                bundle['files']['agentos-release-action.py'] != target_sha):
            fail('VALIDATOR_REGISTRATION_BUNDLE_INVALID')
        source_bytes = {name:read(source/name) for name in bundle['files']}
        if {name:sha(raw) for name,raw in source_bytes.items()} != bundle['files']:
            fail('VALIDATOR_REGISTRATION_SOURCE_CHANGED')
        source_evidence = [{'name':name,'path':str(source/name),'sha256':bundle['files'][name]} for name in sorted(bundle['files'])]
        existing = intent_path.exists()
        if existing:
            intent, _ = document(intent_path)
            if intent != request:
                fail('VALIDATOR_REGISTRATION_IDEMPOTENCY_CONFLICT')
        else:
            if (layout.registry / 'current.json').exists():
                active_registration(layout)
            if sha(read(layout.control)) != request['expectedRegisteredSha256']:
                fail('VALIDATOR_REGISTRATION_PREIMAGE_CHANGED')
            layout.registry.mkdir(mode=0o700, exist_ok=True)
            protected(layout.registry, directory=True)
            atomic(intent_path, canonical(request))
        actual = sha(read(layout.control))
        if actual not in (request['expectedRegisteredSha256'], target_sha):
            fail('VALIDATOR_REGISTRATION_PREIMAGE_CHANGED')
        evidence_archive = preserve_evidence(layout, rid, proof['evidence'])
        source_archive = preserve_evidence(layout, rid, source_evidence)
        # Install dependency before the entrypoint. Incomplete installation
        # fails the guard's literal module hash or its installed-bundle check.
        for name in ['candidate_origin.py','agentos-release-action.py']:
            destination = layout.control.parent/name
            if not destination.exists() or read(destination) != source_bytes[name]:
                atomic(destination, source_bytes[name])
        if after_replace:
            after_replace()  # Test-only interruption point, never CLI supplied.
        record = {'version': 2, 'kind': 'reviewed_validator_transition', 'state': 'installed',
                  'registrationId': rid, 'installedAt': time.time(), 'sourceCommit': REVIEWED_EXECUTORS[target_sha],
                  'previousExecutorSha256': request['expectedRegisteredSha256'],
                  'executorSha256': target_sha, 'publicationEvidence': proof,
                  'sourceKind':source_kind,'sourceEvidence':source_evidence,
                  'bundleManifest':bundle['files'],'bundleDigest':sha(canonical(bundle['files'])),
                  'evidenceArchive':evidence_archive,'sourceArchive':source_archive,
                  'intentSha256': sha(canonical(request))}
        atomic(record_path, canonical(record))
        pointer = {'version': 1, 'registrationId': rid, 'registrationDigest': sha(canonical(record))}
        atomic(layout.registry / 'current.json', canonical(pointer))
        return pointer


def main():
    import sys
    layout = Layout()
    if sys.argv[1:] == ['inspect']:
        proof = publication(layout)
        print(json.dumps({'publication': proof, 'publicationProofDigest': sha(canonical(proof)),
                          'registeredExecutorSha256': sha(read(layout.control)),
                          'admissibleExecutors': {key:value['sourceCommit'] for key,value in REVIEWED_BUNDLES.items()}}))
    elif sys.argv[1:] == ['register']:
        print(json.dumps(register(layout, json.load(sys.stdin))))
    else:
        fail('VALIDATOR_REGISTRATION_MODE_INVALID')


if __name__ == '__main__':
    main()
