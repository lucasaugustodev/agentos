#!/usr/bin/python3
"""Registered, default-off CPU observation. Never an acceptance/official action.

Only this launcher is privileged. Every supplied checker/candidate byte runs
under a non-root systemd user and bubblewrap. No shell or arbitrary argv API.
"""
import base64
import ctypes
import datetime
import fcntl
import hashlib
import json
import math
import os
import pathlib
import pwd
import re
import selectors
import secrets
import signal
import stat
import subprocess
import sys
import time

_LOADED_CODE = sys._getframe().f_code
UUID = re.compile(r'^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$')
SHA = re.compile(r'^[a-f0-9]{64}$')
PROFILES = {'python-cpu-argv-v1', 'python-cpu-candidate-base-v1'}
MAX_REQUEST = 8 * 1024 * 1024
_CANCELLED = False


def require(condition, code):
    if not condition:
        raise ValueError(code)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True, allow_nan=False).encode()


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def strict_json(raw):
    def pairs(rows):
        out = {}
        for key, value in rows:
            require(key not in out, 'JSON_DUPLICATE')
            out[key] = value
        return out
    def invalid(_):
        raise ValueError('JSON_NONFINITE')
    def floating(value):
        number = float(value)
        require(math.isfinite(number), 'JSON_NONFINITE')
        return number
    return json.loads(raw.decode('utf-8'), object_pairs_hook=pairs, parse_constant=invalid, parse_float=floating)


def utc_now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat().replace('+00:00', 'Z')


def protected(path, *, directory=False):
    p = pathlib.Path(path)
    require(p.is_absolute() and str(p) == path and '..' not in p.parts, 'PROTECTED_PATH')
    for item in [p, *p.parents]:
        info = item.lstat()
        require(not stat.S_ISLNK(info.st_mode) and info.st_uid == 0 and not info.st_mode & 0o022,
                'PROTECTED_OWNER_MODE')
        require(stat.S_ISDIR(info.st_mode) if item != p or directory else
                stat.S_ISREG(info.st_mode) and info.st_nlink == 1, 'PROTECTED_TYPE')
    return p


def read_protected(path, limit):
    p = protected(str(path))
    fd = os.open(p, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        info = os.fstat(fd)
        require(stat.S_ISREG(info.st_mode) and info.st_uid == 0 and info.st_nlink == 1
                and not info.st_mode & 0o022 and info.st_size <= limit, 'PROTECTED_FILE')
        with os.fdopen(fd, 'rb', closefd=False) as file:
            raw = file.read(limit + 1)
        require(len(raw) == info.st_size and len(raw) <= limit, 'PROTECTED_SIZE')
        return raw
    finally:
        os.close(fd)


def sync_dir(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def rename_exclusive(source, target):
    # Linux-only registered executor. No replace or exists+rename fallback:
    # unsupported filesystem/kernel must fail closed before publication.
    libc = ctypes.CDLL(None, use_errno=True)
    operation = libc.renameat2
    operation.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
    operation.restype = ctypes.c_int
    if operation(-100, os.fsencode(source), -100, os.fsencode(target), 1) != 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error), str(target))


def immutable(path, raw, mode=0o600):
    temporary = path.parent / ('.' + path.name + '.publish-' + secrets.token_hex(12))
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, mode)
    try:
        os.fchmod(fd, mode)
        with os.fdopen(fd, 'wb', closefd=False) as file:
            file.write(raw)
            file.flush()
            os.fsync(file.fileno())
    finally:
        os.close(fd)
    # All bytes are durable before the final name is atomically and exclusively
    # visible. SIGKILL can leave a staging file, never a partially named receipt.
    rename_exclusive(temporary, path)
    sync_dir(path.parent)


def registered(path, expected):
    require(os.geteuid() == 0, 'REGISTERED_LAUNCHER_REQUIRED')
    raw = read_protected(path, 16384)
    require(sha(raw) == expected, 'REGISTRATION_CHANGED')
    r = strict_json(raw)
    require(type(r) is dict and set(r) == {'version', 'enabled', 'executorPath', 'executorSHA256',
        'stateDirectory', 'user', 'projects', 'profiles', 'limits'}, 'REGISTRATION_SCHEMA')
    require(type(r['version']) is int and r['version'] == 1 and r['enabled'] is True, 'DISABLED')
    source = read_protected(r['executorPath'], 1024 * 1024)
    require(pathlib.Path(r['executorPath']) == pathlib.Path(__file__) and sha(source) == r['executorSHA256']
            and compile(source, __file__, 'exec', dont_inherit=True, optimize=sys.flags.optimize) == _LOADED_CODE,
            'EXECUTOR_CHANGED')
    require(type(r['projects']) is list and 0 < len(r['projects']) <= 30 and all(
        type(x) is str and re.fullmatch(r'[a-z0-9_-]{1,80}', x) for x in r['projects']), 'REGISTRATION_PROJECTS')
    require(type(r['profiles']) is list and 0 < len(r['profiles']) <= 2 and len(set(r['profiles'])) == len(r['profiles'])
            and set(r['profiles']) <= PROFILES, 'REGISTRATION_PROFILES')
    require(type(r['user']) is str and re.fullmatch(r'[a-z_][a-z0-9_-]{0,31}', r['user']), 'REGISTRATION_USER')
    user = pwd.getpwnam(r['user'])
    require(user.pw_uid != 0 and user.pw_gid != 0, 'NONROOT_REQUIRED')
    limits = r['limits']
    require(type(limits) is dict and set(limits) == {'memoryMiB', 'timeoutSeconds', 'maxOutputBytes'}
            and all(type(v) is int for v in limits.values()) and 64 <= limits['memoryMiB'] <= 512
            and 1 <= limits['timeoutSeconds'] <= 300 and 1024 <= limits['maxOutputBytes'] <= 262144,
            'REGISTRATION_LIMITS')
    state = protected(r['stateDirectory'], directory=True)
    require(state != pathlib.Path('/'), 'REGISTRATION_STATE')
    # The dedicated staging path is traversable, while intents/receipts remain
    # root-only files. Reject private parent paths before dispatch instead of
    # changing their permissions or launching a predictably broken sandbox.
    require(all(item.stat().st_mode & 0o001 for item in [state, *state.parents]), 'STATE_NOT_TRAVERSABLE')
    return r, user


def request_valid(request, registration, registration_sha):
    require(type(request) is dict and set(request) == {'version', 'requestId', 'occurrenceId', 'runFence',
        'definitionVersion', 'project', 'source', 'artifacts', 'profile', 'baseSource',
        'registrationSHA256', 'executorSHA256'}, 'REQUEST_SCHEMA')
    require(type(request['version']) is int and request['version'] == 1
            and type(request['definitionVersion']) is int and 1 <= request['definitionVersion'] <= 2**31 - 1
            and all(type(request[k]) is str and UUID.fullmatch(request[k]) for k in ('requestId', 'occurrenceId', 'runFence')),
            'REQUEST_IDENTITY')
    require(request['registrationSHA256'] == registration_sha and request['executorSHA256'] == registration['executorSHA256']
            and request['project'] in registration['projects'] and request['profile'] in registration['profiles'], 'REQUEST_SCOPE')
    source = request['source']
    require(type(source) is dict and set(source) == {'stepId', 'key', 'attemptId', 'attemptNumber', 'runFence'}
            and all(type(source[k]) is str and UUID.fullmatch(source[k]) for k in ('stepId', 'attemptId', 'runFence'))
            and type(source['attemptNumber']) is int and source['attemptNumber'] == 1
            and type(source['key']) is str and re.fullmatch(r'[a-z0-9_-]{1,60}', source['key']), 'SOURCE_IDENTITY')
    names = ['candidate.py', 'cpu-checks.py', 'cpu-result.json']
    if request['profile'] == 'python-cpu-candidate-base-v1':
        names += ['implementation.patch', 'base-candidate.py']
    require(type(request['artifacts']) is list and len(request['artifacts']) == len(names), 'INPUT_SET')
    files = {}
    for ref, name in zip(request['artifacts'], names):
        require(type(ref) is dict and set(ref) == {'id', 'name', 'digest', 'size', 'base64'}
                and type(ref['id']) is str and UUID.fullmatch(ref['id']) and ref['name'] == name
                and type(ref['digest']) is str and SHA.fullmatch(ref['digest'])
                and type(ref['size']) is int and 0 < ref['size'] <= 1024 * 1024 and type(ref['base64']) is str,
                'INPUT_IDENTITY')
        raw = base64.b64decode(ref['base64'], validate=True)
        require(base64.b64encode(raw).decode() == ref['base64'] and len(raw) == ref['size'] and sha(raw) == ref['digest'], 'INPUT_BYTES')
        files[name] = raw
    require(len({r['id'] for r in request['artifacts']}) == len(names), 'INPUT_IDS')
    base = request['baseSource']
    if len(names) == 3:
        require(base is None, 'BASE_SCOPE')
    else:
        require(type(base) is dict and set(base) == {'artifact', 'source', 'inputSHA256'}
                and type(base['inputSHA256']) is str and SHA.fullmatch(base['inputSHA256']), 'BASE_SCOPE')
        ref = request['artifacts'][-1]
        require(base['artifact'] == {'id': ref['id'], 'name': 'candidate.py', 'digest': ref['digest'], 'size': ref['size']}, 'BASE_BINDING')
        actor = base['source']
        require(type(actor) is dict and set(actor) == {'occurrenceId', 'attemptId', 'runFence'}
                and all(type(x) is str and UUID.fullmatch(x) for x in actor.values()), 'BASE_IDENTITY')
    return files


def argv_for(profile):
    if profile == 'python-cpu-argv-v1':
        return ['/usr/bin/python3', '-I', '-B', '/inputs/cpu-checks.py', '--candidate', '/inputs/candidate.py', '--output', '/output/cpu-replay-result.json']
    require(profile == 'python-cpu-candidate-base-v1', 'PROFILE')
    return ['/usr/bin/python3', '-I', '-B', '/output/cpu-checks.py', '/output/candidate.py', '/inputs/base-candidate.py']


def command(args, timeout=10):
    return subprocess.run(args, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          env={'PATH': '/usr/sbin:/usr/bin:/bin', 'LANG': 'C.UTF-8'}, timeout=timeout, check=False)


def unit_state(unit):
    result = command(['/usr/bin/systemctl', 'show', unit, '--property=LoadState,ActiveState,SubState,MainPID,ControlGroup,ExecMainCode,ExecMainStatus,ExecMainStartTimestampMonotonic,Result,MemoryPeak'])
    require(result.returncode in (0, 1) and len(result.stdout) <= 16384, 'UNIT_INSPECTION_UNAVAILABLE')
    data = dict(line.split('=', 1) for line in result.stdout.decode().splitlines() if '=' in line)
    require(data.get('LoadState') in ('loaded', 'not-found'), 'UNIT_INSPECTION_UNAVAILABLE')
    return data


def stopped(unit, allow_exited=False):
    data = unit_state(unit)
    group = data.get('ControlGroup', '')
    terminal_state = data.get('ActiveState') in ('inactive', 'failed') or (
        allow_exited and data.get('ActiveState') == 'active' and data.get('SubState') == 'exited')
    require(terminal_state and data.get('MainPID') == '0'
            and group in ('', '/system.slice/' + unit), 'CPU_REPLAY_UNCONFIRMED')
    if group:
        events = pathlib.Path('/sys/fs/cgroup' + group, 'cgroup.events')
        require(events.is_file() and re.search(r'^populated 0$', events.read_text(), re.M), 'CPU_REPLAY_UNCONFIRMED')
    return {'stopped': True, 'unit': unit, 'mainPID': 0, 'populated': False, 'controlGroup': group,
            'activeState': data['ActiveState'], 'loadState': data['LoadState'], 'observedAt': utc_now()}


def stop_unit(unit):
    command(['/usr/bin/systemctl', 'stop', unit], timeout=8)
    deadline = time.monotonic() + 8
    while True:
        try:
            proof = stopped(unit)
            time.sleep(0.05)
            stopped(unit)
            return proof
        except ValueError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.1)


def encoded(raw):
    return {'digest': sha(raw), 'size': len(raw), 'base64': base64.b64encode(raw).decode()}


def metric(data, key):
    value = data.get(key, '')
    return int(value) if value.isdigit() and 0 <= int(value) < 2**53 else None


def fresh_receipt(directory, request, registration_sha):
    if not (directory / 'receipt.json').exists() and (directory / 'incomplete.json').exists():
        terminal = strict_json(read_protected(directory / 'incomplete.json', 16384))
        require(terminal['requestSHA256'] == sha(canonical(request)) and terminal['requestId'] == request['requestId']
                and terminal['runFence'] == request['runFence'] and terminal['registrationSHA256'] == registration_sha,
                'INTENT_CHANGED')
        stopped(terminal['unit'], allow_exited=True)
        return {'ok': False, 'code': 'CPU_REPLAY_RESULT_LOST', 'terminal': terminal}
    intent = strict_json(read_protected(directory / 'intent.json', MAX_REQUEST + 65536))
    require(intent['requestSHA256'] == sha(canonical(request)) and intent['registrationSHA256'] == registration_sha
            and intent['request'] == request, 'INTENT_CHANGED')
    receipt_path = directory / 'receipt.json'
    if not receipt_path.exists():
        return {'ok': False, 'code': 'CPU_REPLAY_UNCONFIRMED'}
    receipt = strict_json(read_protected(receipt_path, 2 * 1024 * 1024))
    require(receipt['requestSHA256'] == intent['requestSHA256'] and receipt['requestId'] == request['requestId']
            and receipt['runFence'] == request['runFence'] and receipt['registrationSHA256'] == registration_sha,
            'RECEIPT_CHANGED')
    stopped(receipt['unit'])
    return {'ok': True, 'receipt': receipt}


def recover_incomplete(state, directory):
    """Caller holds this request's lock. Never launch or terminate a process.
    A terminal tombstone frees other IDs, but never invents missing outputs."""
    protected(str(directory), directory=True)
    unit = 'agentos-cpu-replay-' + directory.name + '.service'
    if (directory / 'receipt.json').exists():
        return True
    if (directory / 'incomplete.json').exists():
        terminal = strict_json(read_protected(directory / 'incomplete.json', 16384))
        require(terminal.get('unit') == unit and terminal.get('requestId') == directory.name, 'INCOMPLETE_CHANGED')
        stopped(unit, allow_exited=True)
        return True
    intent_path = directory / 'intent.json'
    if intent_path.exists():
        intent = strict_json(read_protected(intent_path, MAX_REQUEST + 65536))
        request = intent['request']
        require(intent['unit'] == unit and request['requestId'] == directory.name and
                intent['requestSHA256'] == sha(canonical(request)) and
                request['registrationSHA256'] == intent['registrationSHA256'], 'INTENT_CHANGED')
        identity = {k: request[k] for k in ('requestId', 'occurrenceId', 'runFence', 'registrationSHA256', 'executorSHA256')}
        identity['requestSHA256'] = intent['requestSHA256']
    else:
        admission = strict_json(read_protected(state / 'admissions' / (directory.name + '.json'), 4096))
        p = admission['proof']
        require(p['requestId'] == directory.name and p['dispatched'] is False and p['version'] == 1, 'ADMISSION_CHANGED')
        identity = {k: p[k] for k in ('requestId', 'occurrenceId', 'runFence', 'registrationSHA256', 'executorSHA256', 'requestSHA256')}
        # Directory absence alone is not a proof. No-intent recovery also
        # requires that the derived unit never remains registered/running.
        require(unit_state(unit)['LoadState'] == 'not-found', 'CPU_REPLAY_UNCONFIRMED')
    proof = stopped(unit, allow_exited=True)
    time.sleep(0.05)
    stopped(unit, allow_exited=True)
    terminal = {**identity, 'version': 1, 'kind': 'cpu-replay-incomplete', 'unit': unit,
        'stopProof': proof, 'observedAt': utc_now(), 'dispatchIntentPresent': intent_path.exists(),
        'outputsUnavailable': True, 'executionOutcomeKnown': False, 'acceptance': False}
    immutable(directory / 'incomplete.json', canonical(terminal))
    # Release only this bounded output mount after the physical proof. No data
    # is substituted into stdout/result and no receipt is rewritten.
    if os.path.ismount(directory / 'output'):
        require(command(['/usr/bin/umount', str(directory / 'output')]).returncode == 0, 'OUTPUT_UNMOUNT_UNCONFIRMED')
    return True


def execute(directory, request, registration, registration_sha, user, files):
    require(not _CANCELLED, 'CPU_REPLAY_CANCELLED')
    directory.mkdir(mode=0o711)
    directory.chmod(0o711)
    sync_dir(directory.parent)
    inputs = directory / 'inputs'
    inputs.mkdir(mode=0o755)
    inputs.chmod(0o755)
    output = directory / 'output'
    output.mkdir(mode=0o700)
    for name, raw in files.items():
        immutable(inputs / name, raw, 0o444)
    unit = 'agentos-cpu-replay-' + request['requestId'] + '.service'
    require(unit_state(unit)['LoadState'] == 'not-found', 'UNIT_ALREADY_EXISTS')
    # No unit dispatch before this fsynced immutable ownership record.
    immutable(directory / 'intent.json', canonical({'version': 1, 'request': request,
        'requestSHA256': sha(canonical(request)), 'registrationSHA256': registration_sha, 'unit': unit, 'createdAt': utc_now()}))
    mounted = False
    dispatched = False
    terminal = False
    selector = selectors.DefaultSelector()
    fds = []
    capture = {'stdout': bytearray(), 'stderr': bytearray()}
    exceeded = False
    timed_out = False
    start = utc_now()
    last = {}
    try:
        mount = command(['/usr/bin/mount', '-t', 'tmpfs', '-o',
            f'size=16M,nosuid,nodev,noexec,uid={user.pw_uid},gid={user.pw_gid},mode=0700', 'tmpfs', str(output)])
        require(mount.returncode == 0 and os.path.ismount(output), 'OUTPUT_MOUNT_FAILED')
        mounted = True
        for stream in ('stdout', 'stderr'):
            fifo = directory / (stream + '.fifo')
            os.mkfifo(fifo, 0o600)
            fd = os.open(fifo, os.O_RDONLY | os.O_NONBLOCK)
            fds.append(fd)
            selector.register(fd, selectors.EVENT_READ, stream)
        mounts = ['/usr/bin/bwrap', '--unshare-all', '--die-with-parent', '--new-session',
            '--ro-bind', '/usr', '/usr', '--ro-bind', '/lib', '/lib', '--ro-bind-try', '/lib64', '/lib64',
            '--proc', '/proc', '--dev', '/dev', '--tmpfs', '/tmp', '--dir', '/inputs', '--bind', str(output), '/output']
        for name in files:
            mounts += ['--ro-bind', str(inputs / name), '/inputs/' + name]
        if request['profile'] == 'python-cpu-candidate-base-v1':
            for name in ('cpu-checks.py', 'candidate.py', 'implementation.patch'):
                mounts += ['--ro-bind', str(inputs / name), '/output/' + name]
        mounts += ['--chdir', '/output', '--clearenv', '--setenv', 'PATH', '/usr/bin:/bin', '--setenv', 'LANG', 'C.UTF-8',
                   '--', *argv_for(request['profile'])]
        properties = [f'User={user.pw_uid}', f'Group={user.pw_gid}', 'Type=exec', 'RemainAfterExit=yes',
            'NoNewPrivileges=yes', 'PrivateNetwork=yes', 'PrivateTmp=yes', 'ProtectHome=yes', 'ProtectSystem=strict',
            'ReadWritePaths=' + str(output), 'KillMode=control-group', 'TimeoutStopSec=2', 'TasksMax=32',
            'CPUQuota=100%', 'MemoryAccounting=yes', f'MemoryMax={registration["limits"]["memoryMiB"]}M',
            'MemorySwapMax=0', 'LimitFSIZE=2097152', 'UMask=0077',
            'RuntimeMaxSec=' + str(registration['limits']['timeoutSeconds']),
            'StandardInput=null', 'StandardOutput=file:' + str(directory / 'stdout.fifo'),
            'StandardError=file:' + str(directory / 'stderr.fifo')]
        args = ['/usr/bin/systemd-run', '--quiet', '--unit=' + unit]
        for prop in properties:
            args += ['--property=' + prop]
        # Set before invocation: a lost client reply does not mean no dispatch.
        require(not _CANCELLED, 'CPU_REPLAY_CANCELLED')
        dispatched = True
        result = command([*args, '--', *mounts], timeout=15)
        require(result.returncode == 0, 'CPU_REPLAY_DISPATCH_UNCONFIRMED')
        deadline = time.monotonic() + registration['limits']['timeoutSeconds'] + 5
        while True:
            for key, _ in selector.select(0.1):
                raw = os.read(key.fd, 65536)
                if raw:
                    remaining = registration['limits']['maxOutputBytes'] - len(capture[key.data])
                    capture[key.data] += raw[:max(0, remaining)]
                    exceeded |= len(raw) > remaining
            last = unit_state(unit)
            if exceeded or _CANCELLED or time.monotonic() > deadline:
                timed_out = time.monotonic() > deadline
                break
            if last.get('ActiveState') == 'failed' or last.get('SubState') == 'exited':
                break
            require(last.get('LoadState') != 'not-found', 'CPU_REPLAY_UNCONFIRMED')
        require(metric(last, 'ExecMainStartTimestampMonotonic') not in (None, 0), 'CPU_REPLAY_START_UNCONFIRMED')
        memory_peak = metric(last, 'MemoryPeak')
        oom = last.get('Result') == 'oom-kill'
        timed_out |= last.get('Result') == 'timeout'
        # Preserve execution status and memory while the retained unit exists.
        status_before_stop = dict(last)
        proof = stop_unit(unit)
        terminal = True
        # Drain remaining bounded pipe bytes only after physical stop.
        for fd, stream in zip(fds, ('stdout', 'stderr')):
            while True:
                try:
                    raw = os.read(fd, 65536)
                except BlockingIOError:
                    break
                if not raw:
                    break
                remaining = registration['limits']['maxOutputBytes'] - len(capture[stream])
                capture[stream] += raw[:max(0, remaining)]
                exceeded |= len(raw) > remaining
        outputs = {name: encoded(bytes(raw)) for name, raw in capture.items()}
        generated = output / ('cpu-result.json' if request['profile'] == 'python-cpu-candidate-base-v1' else 'cpu-replay-result.json')
        outputs['result'] = None
        if generated.exists() or generated.is_symlink():
            fd = os.open(generated, os.O_RDONLY | os.O_NOFOLLOW)
            try:
                info = os.fstat(fd)
                require(stat.S_ISREG(info.st_mode) and info.st_uid == user.pw_uid and info.st_nlink == 1, 'OUTPUT_TYPE')
                if info.st_size <= registration['limits']['maxOutputBytes']:
                    with os.fdopen(fd, 'rb', closefd=False) as file:
                        raw = file.read(registration['limits']['maxOutputBytes'] + 1)
                    require(len(raw) == info.st_size, 'OUTPUT_CHANGED')
                    outputs['result'] = encoded(raw)
                else:
                    exceeded = True
            finally:
                os.close(fd)
        code, status = metric(status_before_stop, 'ExecMainCode'), metric(status_before_stop, 'ExecMainStatus')
        receipt = {k: request[k] for k in ('requestId', 'occurrenceId', 'runFence', 'project', 'source', 'profile', 'baseSource')}
        receipt.update({'version': 1, 'kind': 'cpu-replay-observation', 'requestSHA256': sha(canonical(request)),
            'registrationSHA256': registration_sha, 'executorSHA256': registration['executorSHA256'],
            'observationOnly': True, 'acceptance': False, 'argv': argv_for(request['profile']),
            'startedAt': start, 'finishedAt': utc_now(), 'exitCode': status if code == 1 else None,
            'signal': status if code in (2, 3) else None, 'timedOut': timed_out, 'oomKilled': oom,
            'cancelled': _CANCELLED, 'outputLimitExceeded': exceeded, 'outputsComplete': not exceeded,
            'memoryPeakBytes': memory_peak, 'rssPeakBytes': None, 'unit': unit, 'stopProof': proof,
            'unitStatusBeforeStop': status_before_stop, 'outputs': outputs})
        immutable(directory / 'receipt.json', canonical(receipt))
        return {'ok': True, 'receipt': receipt}
    finally:
        if dispatched and not terminal:
            try:
                stop_unit(unit)
                terminal = True
            except Exception:
                pass  # Uncertainty is durable; never make another invocation.
        selector.close()
        for fd in fds:
            os.close(fd)
        if mounted and (terminal or not dispatched):
            require(command(['/usr/bin/umount', str(output)]).returncode == 0, 'OUTPUT_UNMOUNT_UNCONFIRMED')


def waiting(request, registration_sha):
    # Only returned while holding this request's lock, after checking that no
    # execution directory/intent exists. Another request being busy is NOT proof.
    return {'ok': False, 'code': 'CPU_REPLAY_WAITING', 'admission': {
        'version': 1, 'requestId': request['requestId'], 'occurrenceId': request['occurrenceId'],
        'runFence': request['runFence'], 'requestSHA256': sha(canonical(request)),
        'registrationSHA256': registration_sha, 'executorSHA256': request['executorSHA256'],
        'dispatched': False}}


def cancellation(state, request, registration_sha, create=False):
    folder = state / 'cancellations'
    target = folder / (request['requestId'] + '.json')
    unit = 'agentos-cpu-replay-' + request['requestId'] + '.service'
    expected = {'ok': False, 'code': 'CPU_REPLAY_CANCELLED_NO_DISPATCH', 'cancellation': {
        **waiting(request, registration_sha)['admission'], 'cancelled': True, 'unit': unit}}
    if target.exists():
        value = strict_json(read_protected(target, 4096))
        require(value == expected, 'CANCELLATION_CHANGED')
        require(unit_state(unit)['LoadState'] == 'not-found', 'CPU_REPLAY_UNCONFIRMED')
        return value
    if not create:
        return None
    # The caller holds the own request lock. No unit/intent can race this
    # durable tombstone; execute checks it under that very same lock.
    require(not (state / request['requestId']).exists() and unit_state(unit)['LoadState'] == 'not-found',
            'CPU_REPLAY_UNCONFIRMED')
    folder.mkdir(mode=0o700, exist_ok=True)
    protected(str(folder), directory=True)
    immutable(target, canonical(expected))
    return expected


def lock_file(path):
    fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    info = os.fstat(fd)
    try:
        require(stat.S_ISREG(info.st_mode) and info.st_uid == 0 and info.st_nlink == 1
                and not info.st_mode & 0o077, 'LOCK_UNPROTECTED')
    except Exception:
        os.close(fd)
        raise
    return fd


def admission_record(state, request, registration_sha):
    folder = state / 'admissions'
    folder.mkdir(mode=0o700, exist_ok=True)
    protected(str(folder), directory=True)
    target = folder / (request['requestId'] + '.json')
    now = time.time()
    proof = waiting(request, registration_sha)['admission']
    created = now
    if target.exists():
        previous = strict_json(read_protected(target, 4096))
        require(set(previous) == {'proof', 'createdEpoch', 'updatedEpoch'} and previous['proof'] == proof,
                'ADMISSION_CHANGED')
        require(type(previous['createdEpoch']) in (int, float) and
                0 < previous['createdEpoch'] <= previous['updatedEpoch'] <= now, 'ADMISSION_CLOCK')
        created = previous['createdEpoch']
    record = {'proof': proof, 'createdEpoch': created, 'updatedEpoch': now}
    temp = folder / (request['requestId'] + '.next-' + str(os.getpid()))
    immutable(temp, canonical(record))
    os.replace(temp, target)
    sync_dir(folder)
    return record


def admission_head(state, now):
    rows = []
    paths = list((state / 'admissions').glob('*.json'))
    require(len(paths) <= 4096, 'ADMISSION_SIZE')
    for file in paths:
        r = strict_json(read_protected(file, 4096))
        require(set(r) == {'proof', 'createdEpoch', 'updatedEpoch'} and
                type(r['proof']) is dict and r['proof'].get('requestId') == file.stem and UUID.fullmatch(file.stem), 'ADMISSION_SCHEMA')
        require(type(r['createdEpoch']) in (int, float) and type(r['updatedEpoch']) in (int, float) and
                0 < r['createdEpoch'] <= r['updatedEpoch'] <= now + 1, 'ADMISSION_CLOCK')
        # A paused/dead controller cannot leave an immortal head in the queue.
        # Its original ordering remains if the same request returns later.
        if not (state / file.stem).exists() and now - r['updatedEpoch'] <= 60:
            if not (state / 'cancellations' / (file.stem + '.json')).exists():
                rows.append((r['createdEpoch'], file.stem))
    return min(rows)[1] if rows else None


def main(argv):
    require(len(argv) == 4 and argv[1] in ('execute', 'inspect', 'cancel') and SHA.fullmatch(argv[3]), 'CLI_SCOPE')
    registration, user = registered(argv[2], argv[3])
    raw = sys.stdin.buffer.read(MAX_REQUEST + 1)
    require(len(raw) <= MAX_REQUEST, 'REQUEST_SIZE')
    request = strict_json(raw)
    files = request_valid(request, registration, argv[3])
    state = pathlib.Path(registration['stateDirectory'])
    directory = state / request['requestId']
    # Immutable receipts can be inspected while ANY other request is running.
    # The own directory, even an incomplete one, always defeats no-dispatch.
    if directory.exists() and (directory / 'receipt.json').exists():
        protected(str(directory), directory=True)
        return fresh_receipt(directory, request, argv[3])
    own_fd = lock_file(state / (request['requestId'] + '.lock'))
    try:
        try:
            fcntl.flock(own_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return {'ok': False, 'code': 'CPU_REPLAY_UNCONFIRMED'}
        if directory.exists():
            protected(str(directory), directory=True)
            recover_incomplete(state, directory)
            return fresh_receipt(directory, request, argv[3])
        cancelled = cancellation(state, request, argv[3], create=argv[1] == 'cancel')
        if cancelled:
            return cancelled
        if argv[1] == 'inspect':
            return waiting(request, argv[3])
        admission_record(state, request, argv[3])
        lock_fd = lock_file(state / 'execution.lock')
        try:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return waiting(request, argv[3])
            if admission_head(state, time.time()) != request['requestId']:
                return waiting(request, argv[3])
            for prior in state.iterdir():
                if prior.is_dir() and UUID.fullmatch(prior.name) and not (prior / 'receipt.json').exists():
                    prior_fd = lock_file(state / (prior.name + '.lock'))
                    try:
                        try:
                            fcntl.flock(prior_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                            recover_incomplete(state, prior)
                        except (BlockingIOError, ValueError, OSError):
                            return waiting(request, argv[3])
                    finally:
                        os.close(prior_fd)
            require(not _CANCELLED, 'CPU_REPLAY_CANCELLED')
            return execute(directory, request, registration, argv[3], user, files)
        finally:
            os.close(lock_fd)
    finally:
        os.close(own_fd)


def cancelled(_signum, _frame):
    global _CANCELLED
    _CANCELLED = True


if __name__ == '__main__':
    os.umask(0o077)
    signal.signal(signal.SIGTERM, cancelled)
    signal.signal(signal.SIGINT, cancelled)
    try:
        answer = main(sys.argv)
    except Exception as error:
        code = str(error) if isinstance(error, ValueError) and re.fullmatch(r'[A-Z0-9_]+', str(error)) else 'CPU_REPLAY_UNCONFIRMED'
        answer = {'ok': False, 'code': code}
    sys.stdout.buffer.write(canonical(answer) + b'\n')
