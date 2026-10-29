"""Produce repair packaging from frozen bytes; never execute worker code or tests."""
import base64, difflib, hashlib, json, os, pathlib, re, stat, sys

MAX = 8 * 1024 * 1024
FORBIDDEN = {'.', '..', '.git', 'assets', 'node_modules', 'evidence', 'data', 'rules', '.agentos-snapshot.json', '.ssh', '.aws', '.codex', '.claude'}

def fail(code):
    raise ValueError('REPAIR_FINALIZATION_' + code)

def sha(data):
    return hashlib.sha256(data).hexdigest()

def unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result: fail('DUPLICATE_JSON_KEY')
        result[key] = value
    return result

def safe(name):
    return isinstance(name, str) and len(name) <= 512 and re.fullmatch(r'[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+)*', name) and not any(p in FORBIDDEN or p.startswith('.env') for p in name.split('/'))

def read(root, name, missing=False):
    if root.resolve() != root or not root.is_dir(): fail('ROOT')
    current = root
    for component in name.split('/'):
        current = current / component
        try:
            mode = current.lstat().st_mode
        except FileNotFoundError:
            if missing: return None
            fail('FILE_MISSING')
        if stat.S_ISLNK(mode): fail('SYMLINK')
    fd = os.open(current, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as handle:
        before = os.fstat(handle.fileno())
        if not stat.S_ISREG(before.st_mode) or before.st_size > MAX: fail('FILE_TYPE_SIZE')
        if sys.platform.startswith('linux') and \
           pathlib.Path('/proc/self/fd/' + str(handle.fileno())).resolve() != current: fail('PATH_CHANGED')
        data = handle.read(MAX + 1)
        after = os.fstat(handle.fileno())
        actual = current.lstat()
        attrs = ('st_dev', 'st_ino', 'st_size', 'st_mtime_ns', 'st_ctime_ns')
        if len(data) != before.st_size or any(getattr(before, a) != getattr(other, a) for other in (after, actual) for a in attrs): fail('FILE_CHANGED')
        if current.resolve() != current: fail('PATH_CHANGED')
        return data

def finalize(params):
    if params.get('stopped') is not True: fail('STOP_REQUIRED')
    root = pathlib.Path(params['root'])
    candidate = root / 'data' / 'repair-candidate'
    if not root.is_absolute() or candidate.resolve() != candidate: fail('ROOT')
    binding = params['snapshot']['binding']
    if binding != params['binding']: fail('IDENTITY')
    raw = base64.b64decode(params['report'], validate=True)
    if len(raw) > MAX: fail('REPORT_SIZE')
    report = json.loads(raw.decode('utf-8'), object_pairs_hook=unique)
    if not isinstance(report, dict) or type(report.get('fixVerified')) is not bool: fail('REPORT')
    selected = report.get('changedFiles')
    if not isinstance(selected, list) or not 1 <= len(selected) <= 128 or not all(safe(p) for p in selected) or len(set(selected)) != len(selected): fail('SELECTION')
    removals = report.get('removedFiles', [])
    if not isinstance(removals, list) or not all(isinstance(p, str) and p in selected for p in removals) or len(set(removals)) != len(removals): fail('REMOVALS')
    files = params['snapshot']['files']
    if not isinstance(files, dict): fail('SNAPSHOT')
    before_hashes = {}
    # Existing claims cannot silently select an alternative effective source.
    for entry in report.get('sourceHashes', []):
        if not isinstance(entry, dict) or not safe(entry.get('path')) or entry['path'] in before_hashes: fail('SOURCE_CLAIMS')
        before_hashes[entry['path']] = entry.get('before')
    changes, patch, total = [], [], 0
    for name in sorted(selected):
        before = read(root, name, missing=True)
        before_hash = None if before is None else sha(before)
        if before_hash != files.get(name): fail('BASE_CHANGED')
        if name in before_hashes and before_hashes[name] != before_hash: fail('EFFECTIVE_SOURCE_MISMATCH')
        after = read(candidate, name, missing=True)
        if name in removals:
            if before is None or after is not None: fail('REMOVAL_NOT_CONFIRMED')
        elif after is None:
            fail('CANDIDATE_MISSING')
        if before == after: continue
        for content in (before, after):
            if content is not None:
                total += len(content)
                if total > 16 * MAX or b'\0' in content: fail('INPUT_SIZE_ENCODING')
        a = (before or b'').decode('utf-8').splitlines(keepends=True)
        b = (after or b'').decode('utf-8').splitlines(keepends=True)
        # Empty-file adds/deletes cannot be represented by the verifier's hunk subset.
        lines = list(difflib.unified_diff(a, b, fromfile='a/' + name if before is not None else '/dev/null', tofile='b/' + name if after is not None else '/dev/null'))
        if not lines: fail('EMPTY_FILE_CHANGE')
        patch.extend(line if line.endswith('\n') else line + '\n\\ No newline at end of file\n' for line in lines)
        changes.append({'path': name, 'before': before_hash, 'after': None if after is None else sha(after)})
    if not changes: fail('NO_CHANGE')
    patch_bytes = ''.join(patch).encode('utf-8')
    if len(patch_bytes) > MAX: fail('PATCH_SIZE')
    # Re-read selected files to reject mutation during the bounded collection.
    for item in changes:
        for target, expected in ((root, item['before']), (candidate, item['after'])):
            data = read(target, item['path'], missing=True)
            if (None if data is None else sha(data)) != expected: fail('FILE_CHANGED')
    provenance = {'version': 1, 'binding': binding, 'sourceSnapshotDigest': params['snapshot']['sourceSnapshotDigest'],
                  'workerReportDigest': sha(raw), 'patchDigest': sha(patch_bytes), 'stopped': True,
                  'applicabilityVerified': False, 'qaVerified': False}
    generated = {**report, 'changedFiles': [x['path'] for x in changes], 'sourceHashes': changes, 'runtimeFinalization': provenance}
    encoded = json.dumps(generated, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
    if len(encoded) > MAX: fail('REPORT_SIZE')
    return {'patch': base64.b64encode(patch_bytes).decode(), 'report': base64.b64encode(encoded).decode(), 'provenance': provenance}

if __name__ == '__main__':
    try:
        request = sys.stdin.buffer.read(24 * MAX + 1)
        if len(request) > 24 * MAX: fail('REQUEST_SIZE')
        result = finalize(json.loads(request, object_pairs_hook=unique))
        print(json.dumps({'ok': True, **result}))
    except Exception as error:
        code = str(error) if isinstance(error, ValueError) and str(error).startswith('REPAIR_FINALIZATION_') else 'REPAIR_FINALIZATION_INPUT_INVALID'
        print(json.dumps({'ok': False, 'code': code}))
