"""Read-only publication eligibility from controller-owned preparation evidence.

This module is part of the reviewed privileged executor bundle. It imports only
the standard library, never source from a candidate. The guard computes the
candidate digest from actual input bytes; a model cannot supply this proof.
"""
import hashlib
import json
import math
import os
import pathlib
import re
import stat
import uuid

OWNER_UID = 0
PROTECTION_ROOT = pathlib.Path('/')
MAX_FILE = 8 * 1024 * 1024
MAX_TOTAL = 128 * 1024 * 1024
MAX_DEPTH = 32
SKIP = {'.git', 'node_modules', 'evidence', 'data', '.agentos-snapshot.json',
        '.local-test', 'test-results', 'playwright-report', '__pycache__'}
DIGEST_SKIP = {'.git', 'node_modules', '.local-test', '__pycache__'}


class Layout:
    def __init__(self, root=pathlib.Path('/')):
        root = pathlib.Path(root)
        self.active = root / 'opt/agentos/current'
        self.releases = root / 'opt/agentos/releases'
        self.receipts = root / 'var/lib/agentos/releases'
        self.workspaces = root / 'srv/agentos/workspaces'
        self.data = root / 'var/lib/agentos-bridge'
        self.control = root / 'opt/agentos-control'


def fail(code):
    raise ValueError('CANDIDATE_ORIGIN_' + code)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'),
                      ensure_ascii=False, allow_nan=False).encode('utf-8')


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def protected(path, directory=False):
    path = pathlib.Path(path)
    if not path.is_absolute() or not path.is_relative_to(PROTECTION_ROOT):
        fail('PATH_INVALID')
    for item in [path, *path.parents]:
        info = item.lstat()
        if info.st_uid != OWNER_UID or info.st_mode & 0o022 or stat.S_ISLNK(info.st_mode):
            fail('UNPROTECTED')
        if item == path and not (stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode)):
            fail('NOT_REGULAR')
        if item == PROTECTION_ROOT:
            break
    return path


def read(path, limit=MAX_FILE):
    path = protected(path)
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_uid != OWNER_UID or info.st_mode & 0o022:
            fail('UNPROTECTED')
        raw = stream.read(limit + 1)
    if len(raw) > limit:
        fail('TOO_LARGE')
    return raw


def document(path, limit=MAX_FILE):
    raw = read(path, limit)
    value = json.loads(raw)
    if not isinstance(value, dict):
        fail('FORMAT')
    return value, {'path': str(path), 'sha256': sha(raw)}


def workspace_path(layout, value):
    if not isinstance(value, str) or not re.fullmatch(r'agentos/[a-z0-9_-]+', value):
        fail('WORKSPACE_INVALID')
    return layout.workspaces / value


def source_bytes(root):
    """Same source selection and digest bytes as workspace-manager.sources."""
    protected(root, directory=True)
    result = {}
    total = 0
    for folder, dirs, files in os.walk(root, followlinks=False):
        dirs[:] = sorted(d for d in dirs if d not in SKIP and d != 'assets' and not d.startswith('.env'))
        for name in dirs:
            protected(pathlib.Path(folder) / name, directory=True)
        for name in sorted(files):
            if name in SKIP or name.startswith('.env') or name.endswith(('.log', '.pyc')):
                continue
            file = pathlib.Path(folder) / name
            raw = read(file)
            total += len(raw)
            result[file.relative_to(root).as_posix()] = raw
            if total > MAX_TOTAL or len(result) > 4096:
                fail('TOO_LARGE')
    if not result:
        fail('EMPTY_SOURCE')
    return result


def source_digest(files):
    # workspace-manager intentionally uses JSON's default ASCII escaping here.
    return sha(json.dumps([(name, sha(raw)) for name, raw in sorted(files.items())],
                          separators=(',', ':')).encode())


def prepared_files(files):
    result = dict(files)
    raw = result.get('.gitignore', b'')
    existing = raw.decode('utf-8').replace('\r\n', '\n').replace('\r', '\n')
    needed = [p for p in ['assets/', 'evidence/', 'data/', 'node_modules/', '.agentos-snapshot.json']
              if p not in existing.splitlines()]
    if needed:
        result['.gitignore'] = (existing + '\n' + '\n'.join(needed) + '\n').encode()
    return result


def release_digest(root):
    h = hashlib.sha256()
    for file in sorted(root.rglob('*')):
        if any(p in DIGEST_SKIP for p in file.relative_to(root).parts) or file.is_symlink():
            continue
        if file.is_file():
            h.update(file.relative_to(root).as_posix().encode())
            h.update(read(file, 32 * 1024 * 1024))
    return h.hexdigest()


def published_base(layout):
    current = layout.active.resolve(strict=True)
    if current.parent != layout.releases or not re.fullmatch(r'managed-[0-9a-f-]{36}', current.name):
        fail('RELEASE_INVALID')
    uuid.UUID(current.name[8:])
    protected(current, directory=True)
    digest = release_digest(current)
    receipts = []
    for path in sorted(layout.receipts.glob('*.json')):
        if not re.fullmatch(r'[0-9a-f-]{36}\.json', path.name):
            continue  # pending-workers.json is rollout state, not another publication.
        uuid.UUID(path.stem)
        record, ref = document(path)
        if record.get('candidate') != str(current):
            continue
        if (record.get('digest') != digest or record.get('previous') != record.get('base') or
                any(type(record.get(k)) not in (int, float) or not math.isfinite(record[k]) or record[k] <= 0
                    for k in ['validatedAt', 'publishedAt']) or record['publishedAt'] < record['validatedAt']):
            fail('PUBLICATION_UNBOUND')
        receipts.append(ref)
    if len(receipts) != 1:
        fail('PUBLICATION_UNBOUND')
    files = source_bytes(current)
    return {'release': str(current), 'releaseDigest': digest,
            'sourceManifestDigest': sha(canonical({k: sha(v) for k, v in files.items()})),
            'publicationReceipt': receipts[0]}, prepared_files(files)


def original_workspace(layout, workspace, candidate_digest):
    """A frozen CI copy inherits identity only from its root-owned broker receipt."""
    workspace_path(layout, workspace)
    if not re.fullmatch(r'agentos/ocv[0-9a-f]{32}', workspace):
        return workspace, []
    action = str(uuid.UUID(workspace.split('/ocv')[1]))
    directory = layout.data / 'candidate-validations' / action
    request, request_ref = document(directory / 'request.json', 16 * 1024 * 1024)
    frozen, frozen_ref = document(directory / 'frozen.json', 16 * 1024 * 1024)
    candidate_raw = read(directory / 'candidate.json')
    candidate = json.loads(candidate_raw)
    if (request.get('actionId') != action or frozen.get('frozenWorkspace') != workspace or
            {k: v for k, v in frozen.items() if k != 'frozenWorkspace'} !=
            {k: v for k, v in request.items() if k != 'candidateBase64'} or
            request.get('candidateDigest') != candidate_digest or candidate.get('candidateDigest') != candidate_digest or
            len(candidate_raw) != request.get('candidateSize') or sha(candidate_raw) != request.get('candidateArtifactDigest')):
        fail('FROZEN_BINDING_CHANGED')
    original = request.get('workspace')
    workspace_path(layout, original)
    if original == workspace:
        fail('ANCESTRY_CYCLE')
    return original, [request_ref, frozen_ref, {'path': str(directory / 'candidate.json'), 'sha256': sha(candidate_raw)}]


def trace_origin(layout, workspace, expected):
    seen = set()
    chain = []
    while len(chain) < MAX_DEPTH:
        if workspace in seen:
            fail('ANCESTRY_CYCLE')
        seen.add(workspace)
        root = workspace_path(layout, workspace)
        marker, marker_ref = document(root / '.agentos-snapshot.json')
        baseline = layout.data / 'workspace-baselines' / sha(workspace.encode())
        files = source_bytes(baseline)
        digest = source_digest(files)
        if marker.get('workspace') != workspace or marker.get('baseDigest') != digest:
            fail('PREPARATION_CHANGED')
        source = marker.get('sourceWorkspace')
        workspace_path(layout, source)
        chain.append({'workspace': workspace, 'sourceWorkspace': source,
                      'snapshot': marker_ref, 'baseline': str(baseline), 'baseDigest': digest})
        if files == expected:
            return chain
        if not source.split('/')[1].startswith('o'):
            fail('REBASE_REQUIRED')
        # Names are reusable after cleanup. Only the exact parent generation
        # captured by prepare may transfer ancestry to this copied consumer.
        parent_snapshot = marker.get('sourceSnapshotDigest')
        if not isinstance(parent_snapshot, str) or not re.fullmatch('[a-f0-9]{64}', parent_snapshot):
            fail('PARENT_GENERATION_UNBOUND')
        if sha(read(workspace_path(layout, source) / '.agentos-snapshot.json')) != parent_snapshot:
            fail('PARENT_GENERATION_CHANGED')
        workspace = source
    fail('ANCESTRY_LIMIT')


def capture_origin(workspace, candidate_digest, layout=None):
    """Called by the registered guard with its measured source input digest.

    Negative provenance does not invalidate diagnostic CI. It can never authorize
    publication, and contains no guessed historical release or fabricated proof.
    """
    layout = layout or Layout()
    if not re.fullmatch('[0-9a-f]{64}', candidate_digest):
        fail('CANDIDATE_DIGEST_INVALID')
    base = {'version': 1, 'kind': 'controller_preparation', 'workspace': workspace,
            'candidateDigest': candidate_digest}
    try:
        published, expected = published_base(layout)
        original, refs = original_workspace(layout, workspace, candidate_digest)
        chain = trace_origin(layout, original, expected)
        if layout.active.resolve(strict=True) != pathlib.Path(published['release']):
            fail('PUBLICATION_CHANGED')
        proof = {**base, 'publishEligible': True, 'code': 'CURRENT_PUBLISHED_SOURCE',
                 'published': published, 'originalWorkspace': original,
                 'frozenBinding': refs, 'preparationChain': chain}
        return {**proof, 'proofDigest': sha(canonical(proof))}
    except (OSError, ValueError, KeyError, TypeError) as error:
        code = str(error)
        if not re.fullmatch(r'CANDIDATE_ORIGIN_[A-Z_]+', code):
            code = 'CANDIDATE_ORIGIN_UNPROVEN'
        return {**base, 'publishEligible': False, 'code': code}


def require_publish_origin(record, layout=None):
    """Run under the publication lock, before the first publication effect."""
    proof = record.get('candidateOrigin')
    if not isinstance(proof, dict) or proof.get('publishEligible') is not True:
        fail('REBASE_REQUIRED')
    digest = record.get('sourceCandidateDigest')
    if proof.get('candidateDigest') != digest or record.get('base') != proof.get('published', {}).get('release'):
        fail('RECEIPT_UNBOUND')
    now = capture_origin(proof.get('workspace'), digest, layout)
    if now != proof:
        fail('PUBLICATION_CHANGED')
    return proof


def require_registered_bundle(executor_path, executor_sha, module_sha, layout=None):
    """The guard calls this after its literal module hash check, before effects.

    Cycles invoke the guard directly. They must also refuse an interrupted or
    unregistered multi-file transition, not only the validation broker.
    """
    layout = layout or Layout()
    executor_path = pathlib.Path(executor_path)
    if executor_path != layout.control / 'agentos-release-action.py':
        fail('EXECUTOR_PATH')
    manifest = {'agentos-release-action.py':executor_sha, 'candidate_origin.py':module_sha}
    if any(not isinstance(digest, str) or not re.fullmatch('[a-f0-9]{64}', digest) for digest in manifest.values()):
        fail('BUNDLE_DIGEST')
    directory = layout.control / 'validator-registrations'
    pointer, _ = document(directory / 'current.json')
    if set(pointer) != {'version','registrationId','registrationDigest'} or type(pointer.get('version')) is not int or pointer['version'] != 1:
        fail('BUNDLE_REFERENCE')
    rid = str(uuid.UUID(pointer['registrationId']))
    record, ref = document(directory / (rid + '.json'))
    if (ref['sha256'] != pointer['registrationDigest'] or type(record.get('version')) is not int or record['version'] != 2 or
            record.get('kind') != 'reviewed_validator_transition' or record.get('state') != 'installed' or
            record.get('registrationId') != rid or record.get('executorSha256') != executor_sha or
            record.get('bundleManifest') != manifest or record.get('bundleDigest') != sha(canonical(manifest))):
        fail('BUNDLE_UNREGISTERED')
    intent, intent_ref = document(directory / (rid + '.intent.json'))
    if (intent_ref['sha256'] != record.get('intentSha256') or intent.get('registrationId') != rid or
            intent.get('expectedRegisteredSha256') != record.get('previousExecutorSha256') or
            intent.get('expectedReleaseDigest') != record.get('publicationEvidence', {}).get('release', {}).get('digest') or
            intent.get('expectedPublicationProofDigest') != sha(canonical(record.get('publicationEvidence')))):
        fail('BUNDLE_INTENT_CHANGED')
    for name, expected in manifest.items():
        if sha(read(layout.control / name)) != expected:
            fail('BUNDLE_BYTES_CHANGED')
    return {**pointer, 'bundleDigest':record['bundleDigest']}
