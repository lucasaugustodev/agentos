#!/usr/bin/env python3
"""Versioned operator recovery for a recorded, blocked publication rollback.

This does not replace the registered publisher. Registration binds one reviewed
transition. A live/uncertain API and an unrecorded publication phase are outside
v1. No native actor is signalled, and uncertain effects are only observed.
"""
import ast
import contextlib
import fcntl
import hashlib
import json
import os
import pathlib
import re
import stat
import subprocess
import sys
import time
import types
import urllib.request
import uuid

ROOT = pathlib.Path('/opt/agentos')
CONTROL = pathlib.Path('/opt/agentos-control')
STATE = pathlib.Path('/var/lib/agentos/releases')
EXECUTOR_SHA = 'd57894cd6a39d65940d753d8118829a69ccfd599e183bf05324bca73444171f2'
ORIGIN_SHA = '742212cdc76a1404418ad9fa0b57215d377eea4433f77bc2cd29eed9e2a27e6f'
NODE_SHA = '7fde7b8afa198da66257f42ee2001d874c7355631e6d1579a5fb5ef1f246df4c'
NODE = '/opt/deepseek-harness/node/bin/node'
ENV = pathlib.Path('/etc/agentos/server.env')
SERVICES = ('agentos-workers', 'agentos-opencode', 'agentos-backlog', 'agentos-cycles')
SHA = re.compile(r'^[a-f0-9]{64}$')
LOADED = sys._getframe().f_code


def fail(code):
    raise ValueError(code)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True).encode()


def protected(path, directory=False):
    path = pathlib.Path(path)
    if not path.is_absolute():
        fail('RECOVERY_PATH')
    for item in [path, *path.parents]:
        s = item.lstat()
        if stat.S_ISLNK(s.st_mode) or s.st_uid != 0 or s.st_mode & 0o022:
            fail('RECOVERY_UNPROTECTED')
        if item != path or directory:
            if not stat.S_ISDIR(s.st_mode):
                fail('RECOVERY_DIRECTORY')
        elif not stat.S_ISREG(s.st_mode) or s.st_nlink != 1:
            fail('RECOVERY_FILE')
    return path


def read(path):
    path = protected(path)
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        if os.fstat(fd).st_size > 16 * 1024 * 1024:
            fail('RECOVERY_DOCUMENT_TOO_LARGE')
        with os.fdopen(fd, 'rb', closefd=False) as stream:
            return stream.read()
    finally:
        os.close(fd)


def document(path):
    return json.loads(read(path))


def fresh(path, value):
    path = pathlib.Path(path)
    protected(path.parent, directory=True)
    raw = canonical(value)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'wb') as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    sync_directory(path.parent)


def sync_directory(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def durable_directory(path):
    """Repair interrupted mkdir durability before recording or dispatching work."""
    path = pathlib.Path(path)
    if not path.is_relative_to(STATE):
        fail('RECOVERY_STATE_PATH')
    parent = protected(STATE, directory=True)
    for name in path.relative_to(STATE).parts:
        child = parent / name
        if not child.exists():
            child.mkdir(mode=0o700)
        protected(child, directory=True)
        # An existing directory may be left by a crash before its first fsync.
        sync_directory(parent)
        sync_directory(child)
        parent = child


def parse_request(value):
    if set(value) != {'occurrenceId', 'operationId', 'validationReceiptSha256', 'blockedReceiptSha256'}:
        fail('RECOVERY_INPUT')
    for key in ('occurrenceId', 'operationId'):
        if str(uuid.UUID(value[key])) != value[key]:
            fail('RECOVERY_IDENTITY')
    if not all(SHA.fullmatch(value[key]) for key in ('validationReceiptSha256', 'blockedReceiptSha256')):
        fail('RECOVERY_DIGEST')
    return value


def reconcile(io, request):
    """All effect boundaries are explicit; retries inspect the same durable stage."""
    request = parse_request(request)
    binding = io.bind(request)
    intent = io.get('intent')
    if intent is None:
        io.before_schema(binding)
        intent = {'version': 1, 'request': request, 'binding': binding}
        io.put('intent', intent)
    if intent != {'version': 1, 'request': request, 'binding': binding}:
        fail('RECOVERY_INTENT_CHANGED')
    completed = io.get('complete')
    if completed is not None:
        if (completed.get('version') != 1 or completed.get('binding') != binding or
                completed.get('readinessSha256') != sha(canonical(io.get('readiness'))) or
                completed.get('schemaResultSha256') != sha(canonical(io.get('schema-result')))):
            fail('RECOVERY_COMPLETION_RECEIPT_UNBOUND')
        return io.verify_complete(binding)

    pointer = io.pointer()
    if pointer == binding['candidate']:
        io.before_schema(binding)
        schema_intent = io.get('schema-intent')
        if schema_intent is None:
            io.put('schema-intent', {'version': 1, 'binding': binding, 'request': request})
            io.launch_schema(request)
            return {'state': 'schema_pending'}
        if schema_intent != {'version': 1, 'binding': binding, 'request': request}:
            fail('SCHEMA_INTENT_CHANGED')
        schema = io.schema_result(binding)
        if schema is None:
            return {'state': 'schema_pending'}
        io.before_schema(binding)
        proof = io.get('readiness')
        if proof is None:
            proof = {'version': 1, 'binding': binding,
                     'schemaResultSha256': sha(canonical(schema)),
                     'proof': io.require_rollback_readiness(binding)}
            io.put('readiness', proof)
        io.check_readiness_record(proof, binding, schema)
        swap = {'version': 1, 'binding': binding, 'readinessSha256': sha(canonical(proof))}
        if io.get('swap-intent') is None:
            io.put('swap-intent', swap)
        elif io.get('swap-intent') != swap:
            fail('ROLLBACK_SWAP_INTENT_CHANGED')
        io.swap(binding)
    elif pointer == binding['base']:
        # The old guard correctly requires current==candidate. Never forge its
        # input after swap: prove the saved pre-swap gate and read current base.
        io.verify_saved_swap(binding)
    else:
        fail('RECOVERY_POINTER_CHANGED')

    io.after_swap(binding)
    if io.get('api-start-intent') is None:
        io.require_api_stopped()
        io.put('api-start-intent', {'version': 1, 'binding': binding})
        io.start_api()
        return {'state': 'api_pending'}
    if io.get('api-start-intent') != {'version': 1, 'binding': binding}:
        fail('API_START_INTENT_CHANGED')
    api = io.api_result(binding)
    if api is None:
        return {'state': 'api_pending'}
    completed = {'version': 1, 'binding': binding, 'api': api,
                 'readinessSha256': sha(canonical(io.get('readiness'))),
                 'schemaResultSha256': sha(canonical(io.get('schema-result')))}
    io.put('complete', completed)
    return {'state': 'completed', 'receipt': completed}


OBSERVATION_JS = r'''
const {pool}=await import('./server/db.ts');let c;
try{
 c=await pool.connect();await c.query('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');
 const rows=(await c.query(`SELECT json_build_object(
 'chats',(SELECT count(*)::int FROM chat_messages WHERE role='assistant' AND state IN('queued','running','cancelling')),
 'maintenance',(SELECT value FROM settings WHERE id='maintenance'),
 'source',(SELECT value FROM settings WHERE id='agentos-source-workspace'),
 'readiness',COALESCE((SELECT json_object_agg(id,value) FROM settings WHERE id LIKE 'worker-readiness:%'),'{}'::json),
 'scopePolicies',COALESCE((SELECT json_agg(json_build_object('id',id,'value',value) ORDER BY id) FROM settings
   WHERE id LIKE 'worker-validation:%' AND value#>>'{policy,applicability}'='candidate_readiness'),'[]'::json),
 'scopeEvents',COALESCE((SELECT json_agg(json_build_object('id',id,'kind',kind,'entityId',entity_id,'payload',payload) ORDER BY id) FROM events
   WHERE kind='WORKER_VALIDATION_SCOPE_ADMISSION_AUTHORIZED' OR
   (kind='WORKER_VALIDATION_POLICY_REGISTERED' AND payload#>>'{policy,applicability}'='candidate_readiness')),'[]'::json),
 'scopeRuns',COALESCE((SELECT json_agg(json_build_object('id',id,'scope',instruction_snapshot->'workerValidationScope') ORDER BY id)
   FROM runs WHERE instruction_snapshot ? 'workerValidationScope'),'[]'::json)) data`)).rows[0].data;
 await c.query('ROLLBACK');console.log(JSON.stringify(rows));
}catch{if(c)await c.query('ROLLBACK').catch(()=>{});console.error('RECOVERY_DB_OBSERVATION_FAILED');process.exitCode=1;}
finally{c?.release();await pool.end();}
'''


class Runtime:
    def __init__(self, registration, guard, request):
        self.registration, self.guard, self.request = registration, guard, request
        self.parent = STATE / 'rollback-reconciliations' / request['occurrenceId']
        self.directory = self.parent / request['operationId']
        self.record = None

    def get(self, name):
        path = self.directory / (name + '.json')
        return document(path) if path.exists() else None

    def put(self, name, value):
        fresh(self.directory / (name + '.json'), value)

    def bind(self, request):
        receipt = STATE / (request['occurrenceId'] + '.json')
        blocked = receipt.with_name(receipt.stem + '.rollback.json')
        if not blocked.exists():
            fail('PUBLICATION_PHASE_UNPROVEN')
        raw, raw_blocked = read(receipt), read(blocked)
        if sha(raw) != request['validationReceiptSha256'] or sha(raw_blocked) != request['blockedReceiptSha256']:
            fail('RECOVERY_RECEIPT_CHANGED')
        record, observation = json.loads(raw), json.loads(raw_blocked)
        transition = self.registration['transition']
        expected = {'occurrenceId': request['occurrenceId'], 'validationReceiptSha256': sha(raw),
                    'candidate': record['candidate'], 'digest': record['digest'], 'base': record['base'],
                    'baseDigest': record['candidateOrigin']['published']['releaseDigest']}
        if transition != expected or record.get('publishedAt') or record.get('workersPendingRestart'):
            fail('RECOVERY_TRANSITION_UNBOUND')
        if observation.get('state') != 'blocked' or observation.get('code') != 'ROLLBACK_READINESS_UNPROVEN' or any(
                observation.get(key) != value for key, value in {
                    'version': 1, 'candidate': record['candidate'], 'digest': record['digest'],
                    'base': record['base'], 'validationReceiptSha256': sha(raw)}.items()):
            fail('PUBLICATION_PHASE_UNPROVEN')
        publisher = observation.get('publisher', {})
        if publisher != self.guard._RECOVERY_PUBLISHER:
            fail('RECOVERY_PUBLISHER_UNBOUND')
        for key, digest in [('candidate', record['digest']), ('base', expected['baseDigest'])]:
            path = pathlib.Path(record[key])
            if path.parent != ROOT / 'releases' or not re.fullmatch(r'managed-[a-f0-9-]{36}', path.name) or path.is_symlink():
                fail('RECOVERY_RELEASE_PATH')
            if self.guard.digest(path) != digest:
                fail('RECOVERY_SOURCE_CHANGED')
        if (STATE / 'pending-workers.json').exists():
            fail('RECOVERY_ADOPTION_PENDING')
        self.record = record
        durable_directory(self.parent)
        owner = self.parent / 'owner.json'
        identity = {'operationId': request['operationId'], 'request': request,
                    'registrationSha256': self.registration['_sha256']}
        if owner.exists():
            if document(owner) != identity:
                fail('RECOVERY_OWNER_CHANGED')
        else:
            fresh(owner, identity)
        durable_directory(self.directory)
        return {**expected, 'request': request, 'registrationSha256': self.registration['_sha256'],
                'environmentSha256': sha(read(ENV)), 'toolSha256': self.registration['toolSha256']}

    def pointer(self):
        return str((ROOT / 'current').resolve(strict=True))

    def service(self, name):
        p = subprocess.run(['systemctl', 'show', name, '--property=ActiveState,MainPID,ControlGroup,InvocationID'],
                           capture_output=True, text=True, check=True, timeout=10)
        return dict(line.split('=', 1) for line in p.stdout.splitlines() if '=' in line)

    def stopped(self, name):
        v = self.service(name)
        if v.get('ActiveState') not in ('inactive', 'failed') or v.get('MainPID') != '0':
            return False
        group = v.get('ControlGroup')
        if group and group != '/system.slice/' + name.removesuffix('.service') + '.service':
            fail('RECOVERY_CGROUP_UNBOUND')
        if group:
            root = pathlib.Path('/sys/fs/cgroup' + group)
            if any(p.read_text().strip() for p in root.rglob('cgroup.procs')):
                return False
        return True

    def require_api_stopped(self):
        if not self.stopped('agentos-api.service'):
            fail('API_STOP_REQUIRED')

    def node(self, source, code):
        loader = pathlib.Path(source, 'node_modules/tsx/dist/loader.mjs').resolve(strict=True)
        self.guard.protected_tool(loader)
        p = subprocess.run([NODE, '--env-file=' + str(ENV), '--import', str(loader), '--input-type=module', '-e', code],
                           cwd=source, capture_output=True, text=True, timeout=40,
                           env={'PATH': str(pathlib.Path(NODE).parent) + ':/usr/bin:/bin', 'HOME': '/nonexistent',
                                'PGOPTIONS': '-c default_transaction_read_only=on -c statement_timeout=5000 -c lock_timeout=1000 -c idle_in_transaction_session_timeout=10000'})
        if p.returncode or p.stderr:
            fail('RECOVERY_READINESS_UNPROVEN')
        return json.loads(p.stdout)

    def compatibility(self, binding, allow_chats=False):
        observed = self.node(binding['base'], OBSERVATION_JS)
        expected = self.registration['baselineObservation']
        for key in ('scopePolicies', 'scopeEvents', 'scopeRuns'):
            if observed.get(key) != expected.get(key):
                fail('NEW_SCOPE_EFFECT_UNPROVEN')
        if not allow_chats and observed.get('chats') != 0:
            fail('API_CHAT_STOP_UNCONFIRMED')
        if observed.get('maintenance', {}) and observed['maintenance'].get('draining'):
            fail('RECOVERY_FOREIGN_MAINTENANCE')
        if (observed.get('source') != expected.get('source') or
                observed.get('source', {}).get('release') != binding['base']):
            fail('PUBLISHED_SOURCE_EFFECT_UNPROVEN')
        for name in SERVICES:
            ready = observed['readiness'].get('worker-readiness:' + name, {})
            service = self.service(name)
            pid = int(service.get('MainPID', '0'))
            if (service.get('ActiveState') != 'active' or pid <= 0 or ready.get('pid') != pid or
                    ready.get('candidate') != binding['base'] or ready.get('digest') != binding['baseDigest'] or
                    str(pathlib.Path('/proc', str(pid), 'cwd').resolve(strict=True)) != binding['base']):
                fail('RECOVERY_CONTROLLER_ADOPTION_UNPROVEN')
        return observed

    def before_schema(self, binding):
        if self.pointer() != binding['candidate']:
            fail('RECOVERY_POINTER_CHANGED')
        self.require_api_stopped()
        self.compatibility(binding)

    def launch_schema(self, request):
        unit = 'agentos-rollback-schema-' + request['operationId'] + '.service'
        argv = ['systemd-run', '--quiet', '--no-block', '--unit=' + unit,
                '--property=Type=exec', '--property=Restart=no', '--property=KillMode=control-group',
                '--property=RuntimeMaxSec=900', '--property=TimeoutStopSec=15',
                '/usr/bin/python3', '-B', str(pathlib.Path(__file__).resolve()), 'schema-worker',
                request['occurrenceId'], request['operationId']]
        p = subprocess.run(argv, capture_output=True, text=True, timeout=15)
        self.put('schema-launch-result', {'exitCode': p.returncode, 'unit': unit})
        if p.returncode:
            fail('SCHEMA_START_UNCONFIRMED')

    def schema_result(self, binding):
        result = self.get('schema-result')
        unit = 'agentos-rollback-schema-' + self.request['operationId'] + '.service'
        if not self.stopped(unit):
            return None
        if result is None:
            fail('SCHEMA_EFFECT_UNCONFIRMED')
        if result.get('binding') != binding or result.get('exitCode') != 0:
            fail('SCHEMA_RECOVERY_FAILED')
        execution = self.get('schema-execution-intent')
        if not execution or execution.get('binding') != binding or result.get('executionIntentSha256') != sha(canonical(execution)):
            fail('SCHEMA_EXECUTION_UNBOUND')
        raw = read(self.directory / 'schema.stdout.json')
        if sha(raw) != result.get('stdoutSha256') or sha(read(self.directory / 'schema.stderr.txt')) != result.get('stderrSha256'):
            fail('SCHEMA_RECEIPT_CHANGED')
        data = json.loads(raw)
        if data.get('ok') is not True or data.get('state') != 'complete' or data.get('backlog', {}).get('state') != 'complete':
            fail('SCHEMA_RECOVERY_INCOMPLETE')
        return result

    def require_rollback_readiness(self, binding):
        return self.guard.require_rollback_readiness(binding['base'], self.record)

    def check_readiness_record(self, proof, binding, schema):
        if (proof.get('binding') != binding or proof.get('schemaResultSha256') != sha(canonical(schema)) or
                proof.get('proof', {}).get('ok') is not True or proof['proof'].get('source') != binding['base'] or
                proof['proof'].get('digest') != binding['baseDigest'] or
                proof['proof'].get('gates') != ['base', 'backlog', 'taskExecution', 'runCriteria', 'panel']):
            fail('ROLLBACK_READINESS_RECEIPT_UNBOUND')

    def swap(self, binding):
        self.before_schema(binding)
        # Recheck the unchanged canonical guard immediately before the effect.
        self.require_rollback_readiness(binding)
        target = ROOT / ('recovery-' + self.request['operationId'])
        if target.exists() or target.is_symlink():
            if not target.is_symlink() or os.readlink(target) != binding['base']:
                fail('ROLLBACK_SWAP_UNCONFIRMED')
        else:
            os.symlink(binding['base'], target)
        os.replace(target, ROOT / 'current')
        sync_directory(ROOT)

    def verify_saved_swap(self, binding):
        proof, swap = self.get('readiness'), self.get('swap-intent')
        schema = self.schema_result(binding)
        if not proof or not swap or schema is None:
            fail('ROLLBACK_SWAP_UNPROVEN')
        self.check_readiness_record(proof, binding, schema)
        if swap != {'version': 1, 'binding': binding, 'readinessSha256': sha(canonical(proof))}:
            fail('ROLLBACK_SWAP_UNPROVEN')
        if self.get('api-start-intent') is None:
            # A response/fsync may have failed after rename. Complete that
            # durability barrier before allowing an API start on replay.
            sync_directory(ROOT)

    def after_swap(self, binding):
        if self.pointer() != binding['base']:
            fail('RECOVERY_POINTER_CHANGED')
        self.compatibility(binding, allow_chats=self.get('api-start-intent') is not None)
        expected = {'version': 1, 'ok': True, 'readOnly': True,
                    'gates': ['base', 'backlog', 'taskExecution', 'runCriteria', 'panel']}
        if self.node(binding['base'], self.guard.ROLLBACK_READINESS_JS) != expected:
            fail('RECOVERY_BASE_NOT_READY')

    def start_api(self):
        p = subprocess.run(['systemctl', 'start', '--no-block', 'agentos-api.service'],
                           capture_output=True, text=True, timeout=15)
        self.put('api-start-result', {'exitCode': p.returncode})
        if p.returncode:
            fail('API_START_UNCONFIRMED')

    def api_result(self, binding):
        state = self.service('agentos-api.service')
        if state.get('ActiveState') in ('activating', 'deactivating'):
            return None
        pid = int(state.get('MainPID', '0'))
        if state.get('ActiveState') != 'active' or pid <= 0:
            fail('API_START_UNCONFIRMED')
        if str(pathlib.Path('/proc', str(pid), 'cwd').resolve(strict=True)) != binding['base']:
            fail('API_SOURCE_UNBOUND')
        environment = pathlib.Path('/proc', str(pid), 'environ').read_bytes().split(b'\0')
        if b'AGENTOS_ROLE=api' not in environment:
            fail('API_ROLE_UNBOUND')
        try:
            with urllib.request.urlopen('http://127.0.0.1:4280/api/session', timeout=3) as response:
                if response.status != 200:
                    return None
        except Exception:
            return None
        after = self.service('agentos-api.service')
        if any(after.get(key) != state.get(key) for key in ('MainPID', 'ActiveState', 'InvocationID')):
            return None
        return {'pid': pid, 'source': binding['base'], 'digest': binding['baseDigest'],
                'invocationId': state.get('InvocationID'), 'healthy': True}

    def verify_complete(self, binding):
        self.verify_saved_swap(binding)
        self.after_swap(binding)
        receipt = self.get('complete')
        if (receipt.get('binding') != binding or receipt.get('api', {}).get('source') != binding['base'] or
                receipt['api'].get('digest') != binding['baseDigest'] or receipt['api'].get('healthy') is not True or
                self.api_result(binding) is None):
            fail('RECOVERY_COMPLETION_UNPROVEN')
        return {'state': 'completed', 'readOnlyReplay': True, 'receipt': receipt}


def load_authority():
    path = pathlib.Path(__file__).resolve()
    if (os.geteuid() != 0 or path.parent.parent != CONTROL / 'rollback-tools' or
            not re.fullmatch('[a-f0-9]{40}', path.parent.name)):
        fail('RECOVERY_INSTALLATION_UNREGISTERED')
    raw = read(path)
    if compile(raw, str(path), 'exec', dont_inherit=True, optimize=sys.flags.optimize) != LOADED:
        fail('RECOVERY_CODE_CHANGED')
    registration_raw = read(path.with_name('registration.json'))
    registration = json.loads(registration_raw)
    if (registration.get('version') != 1 or registration.get('operation') != 'agentos.rollback-reconcile' or
            registration.get('sourceCommit') != path.parent.name or registration.get('toolSha256') != sha(raw) or
            registration.get('executorSha256') != EXECUTOR_SHA or registration.get('originSha256') != ORIGIN_SHA):
        fail('RECOVERY_REGISTRATION_UNBOUND')
    qualification = registration.get('qualification', {})
    qraw = read(pathlib.Path(qualification.get('path', '')))
    q = json.loads(qraw)
    if (sha(qraw) != qualification.get('sha256') or q.get('qualified') is not True or
            q.get('toolSha256') != sha(raw) or q.get('sourceCommit') != path.parent.name or
            q.get('suite') != 'agentos.rollback-reconcile.v1' or
            not isinstance(q.get('tests'), int) or q['tests'] < 20 or q.get('passed') != q['tests'] or
            q.get('skipped') != 0 or q.get('failed') != 0):
        fail('RECOVERY_QUALIFICATION_UNBOUND')
    if set(registration.get('baselineObservation', {})) != {'source', 'scopePolicies', 'scopeEvents', 'scopeRuns'}:
        fail('RECOVERY_BASELINE_REQUIRED')
    executor = CONTROL / 'agentos-release-action.py'
    raw = read(executor)
    if sha(raw) != EXECUTOR_SHA:
        fail('RECOVERY_EXECUTOR_CHANGED')
    guard = types.ModuleType('registered_recovery_guard')
    guard.__file__ = str(executor)
    exec(compile(raw, str(executor), 'exec'), guard.__dict__)
    origin = guard.trusted_origin_module()  # Retain its shared registration lock.
    guard._RECOVERY_PUBLISHER = guard.publication_executor(origin)
    if sha(guard.protected_tool(pathlib.Path(NODE).resolve()).read_bytes()) != NODE_SHA:
        fail('RECOVERY_NODE_CHANGED')
    registration['_sha256'] = sha(registration_raw)
    return registration, guard


def schema_worker(io, request):
    expected = '/system.slice/agentos-rollback-schema-' + request['operationId'] + '.service'
    if not any(line.endswith(':' + expected) for line in pathlib.Path('/proc/self/cgroup').read_text().splitlines()):
        fail('SCHEMA_WORKER_UNIT_UNBOUND')
    binding = io.bind(request)
    intent = io.get('schema-intent')
    if (intent != {'version': 1, 'binding': binding, 'request': request} or
            io.get('schema-result') is not None or io.get('schema-execution-intent') is not None):
        fail('SCHEMA_WORKER_INTENT_UNBOUND')
    io.before_schema(binding)
    base = pathlib.Path(binding['base'])
    command = [NODE, '--env-file=' + str(ENV), '--import', str((base / 'node_modules/tsx/dist/loader.mjs').resolve()),
               str(base / 'scripts/prepare-schema.ts')]
    for file in [ENV, base / 'scripts/prepare-schema.ts', pathlib.Path(command[3])]:
        io.guard.protected_tool(file)
    execution = {'version': 1, 'binding': binding, 'command': command,
                 'unit': expected, 'schemaIntentSha256': sha(canonical(intent))}
    io.put('schema-execution-intent', execution)
    p = subprocess.run(command, cwd=base, capture_output=True, timeout=780,
                       env={'PATH': str(pathlib.Path(NODE).parent) + ':/usr/bin:/bin', 'HOME': '/nonexistent'})
    for name, raw in [('schema.stdout.json', p.stdout), ('schema.stderr.txt', p.stderr)]:
        path = io.directory / name
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, 'wb') as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
    io.put('schema-result', {'version': 1, 'binding': binding, 'exitCode': p.returncode,
                           'executionIntentSha256': sha(canonical(execution)),
                           'stdoutSha256': sha(p.stdout), 'stderrSha256': sha(p.stderr)})


def main():
    os.umask(0o077)
    registration, guard = load_authority()
    action = sys.argv[1] if len(sys.argv) > 1 else ''
    if action == 'schema-worker' and len(sys.argv) == 4:
        occurrence, operation = str(uuid.UUID(sys.argv[2])), str(uuid.UUID(sys.argv[3]))
        request = document(STATE / 'rollback-reconciliations' / occurrence / operation / 'intent.json')['request']
        if request['occurrenceId'] != occurrence or request['operationId'] != operation:
            fail('SCHEMA_WORKER_OWNER_CHANGED')
    elif action == 'reconcile' and len(sys.argv) == 2:
        request = parse_request(json.load(sys.stdin))
    else:
        fail('RECOVERY_ACTION')
    path = protected(STATE / 'publication.lock')
    descriptor = os.open(path, os.O_RDWR | os.O_NOFOLLOW)
    with os.fdopen(descriptor, 'a') as publication:
        fcntl.flock(publication, fcntl.LOCK_EX)
        before, after = os.fstat(publication.fileno()), path.lstat()
        if (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino):
            fail('RECOVERY_PUBLICATION_LOCK_CHANGED')
        io = Runtime(registration, guard, request)
        result = schema_worker(io, request) if action == 'schema-worker' else reconcile(io, request)
        print(json.dumps(result or {'state': 'schema_executed'}))


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        code = str(error) if isinstance(error, ValueError) and re.fullmatch('[A-Z0-9_]+', str(error)) else 'RECOVERY_RECONCILE_REQUIRED'
        print(json.dumps({'ok': False, 'code': code}), file=sys.stderr)
        sys.exit(1)
