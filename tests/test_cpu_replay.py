"""Pure protocol tests; optional explicit isolated sandbox qualification.

Default invocation runs no checker. --sandbox-root requires a dedicated,
root-owned development directory and runs only synthetic non-root fixtures.
"""
import base64
import hashlib
import importlib.util
import io
import json
import os
import pathlib
import subprocess
import sys
import unittest
import tempfile
import uuid
from unittest.mock import patch

SOURCE = pathlib.Path(__file__).resolve().parents[1] / 'scripts' / 'cpu-replay.py'
spec = importlib.util.spec_from_file_location('cpu_replay', SOURCE)
M = importlib.util.module_from_spec(spec)
spec.loader.exec_module(M)


def reference(name, raw):
    return {'id': str(uuid.uuid4()), 'name': name, 'size': len(raw),
            'digest': M.sha(raw), 'base64': base64.b64encode(raw).decode()}


def registration(state):
    return {'version': 1, 'enabled': True, 'executorPath': str(SOURCE), 'executorSHA256': M.sha(SOURCE.read_bytes()),
        'stateDirectory': str(state), 'user': 'agentos-build', 'projects': ['fixture'],
        'profiles': sorted(M.PROFILES), 'limits': {'memoryMiB': 64, 'timeoutSeconds': 5, 'maxOutputBytes': 4096}}


def request(reg, reg_sha, checker=b'print("fixture")\n', profile='python-cpu-argv-v1'):
    values = [reference('candidate.py', b'synthetic candidate\n'), reference('cpu-checks.py', checker),
              reference('cpu-result.json', b'{"passed":true}\n')]
    r = {'version': 1, 'requestId': str(uuid.uuid4()), 'occurrenceId': str(uuid.uuid4()),
        'runFence': str(uuid.uuid4()), 'definitionVersion': 1, 'project': 'fixture', 'profile': profile,
        'registrationSHA256': reg_sha, 'executorSHA256': reg['executorSHA256'],
        'source': {'stepId': str(uuid.uuid4()), 'key': 'produce', 'attemptId': str(uuid.uuid4()),
                   'attemptNumber': 1, 'runFence': str(uuid.uuid4())}, 'artifacts': values, 'baseSource': None}
    if profile == 'python-cpu-candidate-base-v1':
        values += [reference('implementation.patch', b'synthetic patch\n'), reference('base-candidate.py', b'synthetic base\n')]
        a = values[-1]
        r['baseSource'] = {'artifact': {'id': a['id'], 'name': 'candidate.py', 'digest': a['digest'], 'size': a['size']},
            'source': {'occurrenceId': str(uuid.uuid4()), 'attemptId': str(uuid.uuid4()), 'runFence': str(uuid.uuid4())},
            'inputSHA256': 'd' * 64}
    return r


class Protocol(unittest.TestCase):
    def setUp(self):
        self.reg = registration('/isolated/state')
        self.sha = M.sha(M.canonical(self.reg))
        self.r = request(self.reg, self.sha)

    def test_both_fixed_profiles(self):
        self.assertEqual(len(M.request_valid(self.r, self.reg, self.sha)), 3)
        r = request(self.reg, self.sha, profile='python-cpu-candidate-base-v1')
        self.assertEqual(len(M.request_valid(r, self.reg, self.sha)), 5)
        self.assertEqual(M.argv_for(r['profile'])[-2:], ['/output/candidate.py', '/inputs/base-candidate.py'])

    def test_no_shell_or_extra_input(self):
        for key, value in [('argv', ['sh', '-c', 'false;echo success']), ('environment', {'KEY': 'value'})]:
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, 'REQUEST_SCHEMA'):
                M.request_valid({**self.r, key: value}, self.reg, self.sha)

    def test_pin_and_scope(self):
        for key, value in [('registrationSHA256', 'f' * 64), ('executorSHA256', 'f' * 64),
                           ('project', 'production'), ('profile', 'bash')]:
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, 'REQUEST_SCOPE'):
                M.request_valid({**self.r, key: value}, self.reg, self.sha)

    def test_artifact_tamper(self):
        for key, value in [('size', True), ('size', 2**53), ('base64', base64.b64encode(b'changed').decode()),
                           ('digest', 'f' * 64), ('name', '../candidate.py')]:
            r = json.loads(json.dumps(self.r)); r['artifacts'][0][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                M.request_valid(r, self.reg, self.sha)

    def test_base_cannot_be_rebound(self):
        r = request(self.reg, self.sha, profile='python-cpu-candidate-base-v1')
        r['baseSource']['artifact']['digest'] = 'e' * 64
        with self.assertRaisesRegex(ValueError, 'BASE_BINDING'):
            M.request_valid(r, self.reg, self.sha)

    def test_duplicate_keys_and_nonfinite_rejected(self):
        for raw in [b'{"x":1,"x":2}', b'{"x":NaN}', b'{"x":Infinity}']:
            with self.subTest(raw=raw), self.assertRaises(ValueError): M.strict_json(raw)

    def test_completed_inspection_does_not_dispatch(self):
        receipt = {'requestId': self.r['requestId'], 'runFence': self.r['runFence'], 'registrationSHA256': self.sha,
                   'requestSHA256': M.sha(M.canonical(self.r)), 'unit': 'fixed'}
        intent = {'request': self.r, 'requestSHA256': receipt['requestSHA256'], 'registrationSHA256': self.sha}
        with patch.object(M, 'read_protected', side_effect=[M.canonical(intent), M.canonical(receipt)]), \
             patch.object(pathlib.Path, 'exists', return_value=True), patch.object(M, 'stopped') as stopped, \
             patch.object(M, 'execute') as execute:
            out = M.fresh_receipt(pathlib.Path('/isolated/request'), self.r, self.sha)
            self.assertEqual(out, {'ok': True, 'receipt': receipt}); stopped.assert_called_once_with('fixed'); execute.assert_not_called()

    def test_api_verify_is_readonly_even_when_another_request_owns_execution(self):
        receipt = {'requestId': self.r['requestId'], 'unit': 'fixed'}
        with patch.object(M, 'registered', return_value=(self.reg, object())), \
             patch.object(M, 'protected', return_value=pathlib.Path('/isolated/state')), \
             patch.object(sys, 'stdin', type('Input', (), {'buffer': io.BytesIO(M.canonical(self.r))})()), \
             patch.object(pathlib.Path, 'exists', return_value=True), \
             patch.object(M, 'fresh_receipt', return_value={'ok': True, 'receipt': receipt}), \
             patch.object(M, 'stopped', return_value={'stopped': True, 'unit': 'fixed', 'mainPID': 0, 'populated': False}), \
             patch.object(M, 'lock_file') as lock, patch.object(M, 'execute') as execute, \
             patch.object(M, 'recover_incomplete') as recover, patch.object(M, 'immutable') as write:
            result = M.main(['launcher', 'verify', '/isolated/registration', self.sha])
            self.assertTrue(result['ok'])
            self.assertEqual(result['physicalInspection']['receiptSHA256'], M.sha(M.canonical(receipt)))
            lock.assert_not_called(); execute.assert_not_called(); recover.assert_not_called(); write.assert_not_called()

    def test_api_verify_missing_receipt_never_creates_tombstone_or_admission(self):
        with patch.object(M, 'registered', return_value=(self.reg, object())), \
             patch.object(M, 'protected', return_value=pathlib.Path('/isolated/state')), \
             patch.object(sys, 'stdin', type('Input', (), {'buffer': io.BytesIO(M.canonical(self.r))})()), \
             patch.object(pathlib.Path, 'exists', return_value=False), patch.object(M, 'lock_file') as lock, \
             patch.object(M, 'recover_incomplete') as recover, patch.object(M, 'immutable') as write:
            with self.assertRaisesRegex(ValueError, 'CPU_REPLAY_UNCONFIRMED'):
                M.main(['launcher', 'verify', '/isolated/registration', self.sha])
            lock.assert_not_called(); recover.assert_not_called(); write.assert_not_called()

    def test_lost_receipt_is_uncertain_never_rerun(self):
        intent = {'request': self.r, 'requestSHA256': M.sha(M.canonical(self.r)), 'registrationSHA256': self.sha}
        with patch.object(M, 'read_protected', return_value=M.canonical(intent)), \
             patch.object(pathlib.Path, 'exists', return_value=False), patch.object(M, 'execute') as execute:
            self.assertEqual(M.fresh_receipt(pathlib.Path('/isolated/request'), self.r, self.sha),
                             {'ok': False, 'code': 'CPU_REPLAY_UNCONFIRMED'}); execute.assert_not_called()

    def test_new_fence_does_not_adopt_old_intent(self):
        intent = {'request': self.r, 'requestSHA256': M.sha(M.canonical(self.r)), 'registrationSHA256': self.sha}
        changed = {**self.r, 'runFence': str(uuid.uuid4())}
        with patch.object(M, 'read_protected', return_value=M.canonical(intent)), self.assertRaisesRegex(ValueError, 'INTENT_CHANGED'):
            M.fresh_receipt(pathlib.Path('/isolated/request'), changed, self.sha)

    def test_symlink_is_not_protected(self):
        with self.assertRaises((ValueError, OSError)): M.protected('/proc/self/exe')

    def test_waiting_proof_binds_request_and_never_claims_a_completed_execution(self):
        out = M.waiting(self.r, self.sha)
        self.assertEqual(out['code'], 'CPU_REPLAY_WAITING')
        self.assertEqual(set(out['admission']), {'version', 'requestId', 'occurrenceId', 'runFence',
            'requestSHA256', 'registrationSHA256', 'executorSHA256', 'dispatched'})
        self.assertIs(out['admission']['dispatched'], False)
        self.assertEqual(out['admission']['requestSHA256'], M.sha(M.canonical(self.r)))

    def test_queue_keeps_original_order_but_an_abandoned_head_expires(self):
        with tempfile.TemporaryDirectory() as root:
            state = pathlib.Path(root); folder = state / 'admissions'; folder.mkdir()
            first, second = str(uuid.uuid4()), str(uuid.uuid4())
            for request_id, created, updated in [(first, 10, 100), (second, 20, 150)]:
                (folder / (request_id + '.json')).write_bytes(M.canonical({'proof': {'requestId': request_id},
                    'createdEpoch': created, 'updatedEpoch': updated}))
            with patch.object(M, 'read_protected', side_effect=lambda p, limit: p.read_bytes()):
                self.assertEqual(M.admission_head(state, 155), first)
                self.assertEqual(M.admission_head(state, 161), second)
                (state / second).mkdir()
                self.assertIsNone(M.admission_head(state, 161))

    def test_queue_rejects_future_clock_or_misbound_identity(self):
        with tempfile.TemporaryDirectory() as root:
            state = pathlib.Path(root); folder = state / 'admissions'; folder.mkdir()
            request_id = str(uuid.uuid4()); target = folder / (request_id + '.json')
            with patch.object(M, 'read_protected', side_effect=lambda p, limit: p.read_bytes()):
                for proof, timestamp in [({'requestId': request_id}, 200), ({'requestId': str(uuid.uuid4())}, 90)]:
                    target.write_bytes(M.canonical({'proof': proof, 'createdEpoch': 10, 'updatedEpoch': timestamp}))
                    with self.assertRaises(ValueError): M.admission_head(state, 100)

    def test_atomic_publication_never_replaces_an_existing_receipt(self):
        with tempfile.TemporaryDirectory() as root:
            target = pathlib.Path(root) / 'receipt.json'
            M.immutable(target, b'original')
            with self.assertRaises(FileExistsError): M.immutable(target, b'replacement')
            self.assertEqual(target.read_bytes(), b'original')

    def test_publication_failure_has_no_partial_final_name(self):
        with tempfile.TemporaryDirectory() as root:
            target = pathlib.Path(root) / 'intent.json'
            with patch.object(M, 'rename_exclusive', side_effect=OSError('unsupported filesystem')), self.assertRaises(OSError):
                M.immutable(target, b'complete staged bytes')
            self.assertFalse(target.exists())
            temporary = list(pathlib.Path(root).glob('.intent.json.publish-*'))
            self.assertEqual(len(temporary), 1)
            self.assertEqual(temporary[0].read_bytes(), b'complete staged bytes')


def sandbox_suite(root):
    root = pathlib.Path(root)
    if os.geteuid() != 0 or not str(root).startswith('/var/lib/agentos-cpu-replay-development-'):
        raise ValueError('EXPLICIT_ISOLATED_SANDBOX_ROOT_REQUIRED')
    root.mkdir(mode=0o711, parents=True, exist_ok=False); root.chmod(0o711)
    M.protected(str(root), directory=True)
    caller = os.getpid(); proof = []

    def run(name, code, timeout=5):
        folder = root / name; folder.mkdir(mode=0o711); folder.chmod(0o711)
        state = folder / 'state'; state.mkdir(mode=0o711); state.chmod(0o711)
        reg = registration(state); reg['limits']['timeoutSeconds'] = timeout
        reg_path = folder / 'registration.json'; M.immutable(reg_path, M.canonical(reg))
        reg_sha = M.sha(reg_path.read_bytes()); req = request(reg, reg_sha, code.encode())
        req_path = folder / 'request.json'; M.immutable(req_path, M.canonical(req))
        args = ['/usr/bin/python3', '-I', '-B', str(SOURCE), 'execute', str(reg_path), reg_sha]
        result = subprocess.run(args, input=M.canonical(req), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            env={'PATH': '/usr/bin:/bin', 'LANG': 'C.UTF-8'}, timeout=timeout + 45)
        out = M.strict_json(result.stdout)
        M.immutable(folder / 'response.json', result.stdout)
        if not out.get('ok'): raise AssertionError((name, out))
        rec = out['receipt']
        assert rec['stopProof']['stopped'] and rec['stopProof']['mainPID'] == 0 and rec['stopProof']['populated'] is False
        assert rec['observationOnly'] and rec['acceptance'] is False and os.getpid() == caller
        inspected = subprocess.run([*args[:4], 'inspect', *args[5:]], input=M.canonical(req),
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, env={'PATH': '/usr/bin:/bin'}, timeout=15)
        assert M.strict_json(inspected.stdout) == out
        # An execute replay must inspect the same completed receipt, not spawn.
        repeated = subprocess.run(args, input=M.canonical(req), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            env={'PATH': '/usr/bin:/bin'}, timeout=15)
        assert M.strict_json(repeated.stdout) == out
        proof.append({'case': name, 'requestId': req['requestId'], 'receiptSHA256': M.sha(M.canonical(rec)),
            'exitCode': rec['exitCode'], 'signal': rec['signal'], 'oomKilled': rec['oomKilled'],
            'timedOut': rec['timedOut'], 'memoryPeakBytes': rec['memoryPeakBytes'], 'controllerAlive': True,
            'stopConfirmed': True, 'idempotent': True})
        return rec

    success = run('success', '''import os,sys,json,hashlib
assert os.getuid()!=0
blocked=False
try: open('/inputs/candidate.py','ab').write(b'forbidden')
except OSError: blocked=True
assert blocked
open(sys.argv[-1],'w').write(json.dumps({'uid':os.getuid(),'readonly':blocked,'networkNamespace':os.readlink('/proc/self/ns/net')}))
''')
    payload = json.loads(base64.b64decode(success['outputs']['result']['base64']))
    assert payload['uid'] != 0 and payload['readonly'] is True
    assert payload['networkNamespace'] != os.readlink('/proc/self/ns/net') and success['exitCode'] == 0
    failed = run('nonzero', 'import sys\nsys.exit(7)\n'); assert failed['exitCode'] == 7
    timeout = run('timeout-descendant', "import subprocess,time\nsubprocess.Popen(['/usr/bin/python3','-c','import time;time.sleep(60)'])\ntime.sleep(60)\n", 1)
    assert timeout['timedOut'] is True
    oom = run('oom', 'x=[]\nwhile True: x.append(bytearray(4*1024*1024))\n')
    assert oom['oomKilled'] is True
    flood = run('bounded-output', "import sys\nsys.stdout.write('x'*(1024*1024));sys.stdout.flush()\n")
    assert flood['outputLimitExceeded'] is True and flood['outputsComplete'] is False
    assert flood['outputs']['stdout']['size'] <= 4096
    M.immutable(root / 'qualification.json', M.canonical({'sourceSHA256': M.sha(SOURCE.read_bytes()),
        'syntheticOnly': True, 'productionChanges': False, 'cases': proof}))
    print(json.dumps({'passed': len(proof), 'proof': str(root / 'qualification.json'),
                      'sha256': M.sha((root / 'qualification.json').read_bytes())}))


if __name__ == '__main__':
    if len(sys.argv) == 3 and sys.argv[1] == '--sandbox-root':
        sandbox_suite(sys.argv[2])
    elif sys.argv[1:] == ['--protocol-json']:
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(Protocol)
        names = [test.id().rsplit('.', 1)[-1] for test in suite]
        result = unittest.TextTestRunner(stream=io.StringIO()).run(suite)
        print(json.dumps({'tests': names, 'testsRun': result.testsRun, 'failures': len(result.failures),
            'errors': len(result.errors), 'skipped': len(result.skipped), 'successful': result.wasSuccessful()}))
        sys.exit(0 if result.wasSuccessful() else 1)
    else:
        unittest.main()
