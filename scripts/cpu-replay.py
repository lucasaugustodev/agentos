#!/usr/bin/python3
"""Registered, default-off CPU observation. Never an acceptance/official action.

Only this launcher is privileged. Every supplied checker/candidate byte runs
under a non-root systemd user and bubblewrap. No shell or arbitrary argv API.
"""
import base64
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


def immutable(path, raw, mode=0o600):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, mode)
    try:
        os.fchmod(fd, mode)
        with os.fdopen(fd, 'wb', closefd=False) as file:
            file.write(raw)
            file.flush()
            os.fsync(file.fileno())
    finally:
        os.close(fd)
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


def stopped(unit):
    data = unit_state(unit)
    group = data.get('ControlGroup', '')
    require(data.get('ActiveState') in ('inactive', 'failed') and data.get('MainPID') == '0'
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


def main(argv):
    require(len(argv) == 4 and argv[1] in ('execute', 'inspect') and SHA.fullmatch(argv[3]), 'CLI_SCOPE')
    registration, user = registered(argv[2], argv[3])
    raw = sys.stdin.buffer.read(MAX_REQUEST + 1)
    require(len(raw) <= MAX_REQUEST, 'REQUEST_SIZE')
    request = strict_json(raw)
    files = request_valid(request, registration, argv[3])
    state = pathlib.Path(registration['stateDirectory'])
    lock_path = state / 'execution.lock'
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        info = os.fstat(lock_fd)
        require(stat.S_ISREG(info.st_mode) and info.st_uid == 0 and info.st_nlink == 1 and not info.st_mode & 0o077, 'LOCK_UNPROTECTED')
        # A busy initial single-tool executor declines conservatively. The core
        # keeps uncertain ownership rather than assuming absence of an effect.
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return {'ok': False, 'code': 'CPU_REPLAY_BUSY'}
        directory = state / request['requestId']
        if directory.exists():
            protected(str(directory), directory=True)
            return fresh_receipt(directory, request, argv[3])
        if argv[1] == 'inspect':
            return {'ok': False, 'code': 'CPU_REPLAY_UNCONFIRMED'}
        for prior in state.iterdir():
            if prior.is_dir() and UUID.fullmatch(prior.name) and not (prior / 'receipt.json').exists():
                return {'ok': False, 'code': 'CPU_REPLAY_UNCONFIRMED'}
        return execute(directory, request, registration, argv[3], user, files)
    finally:
        os.close(lock_fd)


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
