"""Server-only diagnostics: no official requests, production state or credentials."""
import copy
import importlib.util
import json
import pathlib
import subprocess
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location('official_diagnostic_base',
    pathlib.Path(__file__).with_name('continuity_watchdog_test.py'))
B = importlib.util.module_from_spec(spec); spec.loader.exec_module(B); W = B.W


class OfficialIdentityDiagnosticTests(unittest.TestCase):
    def setUp(self):
        self.f = B.WatchTests(); self.f.setUp(); self.addCleanup(self.f.tearDown)
        self.job = self.f.add_gpu_job(problem='muse_decode_layer')
        self.client = object.__new__(W.Client)

    def cli(self, value, *, code=0, stderr=b'', stdout=None):
        raw = W.canonical(value) if stdout is None else stdout
        result = subprocess.CompletedProcess([], code, raw, stderr)
        with mock.patch.object(W.subprocess, 'run', return_value=result) as run:
            try:
                return self.client.official(self.job)
            finally:
                run.assert_called_once_with(['/opt/openkernelchallenge/bin/okc', 'status', self.job['id'], '--json'],
                    capture_output=True, timeout=60, check=False)

    def failed(self, function):
        with self.assertRaises(ValueError) as caught:
            function()
        error = caught.exception
        self.assertIs(type(error), ValueError)
        self.assertEqual(str(error), 'OFFICIAL_IDENTITY')
        fields = W.official_error_fields(error, self.f.config)
        self.assertLessEqual(len(W.canonical(fields['officialObservationFailure'])), 2048)
        return fields['officialObservationFailure']

    def test_cli_missing_identity_keeps_exact_value_error_and_raw_hash(self):
        value = {'status': 'queued', 'error': 'not a job response'}
        diag = self.failed(lambda: self.cli(value))
        self.assertEqual(diag['origin'], 'cli')
        self.assertFalse(diag['returnedId']['present'])
        self.assertFalse(diag['returnedBoard']['present'])
        self.assertEqual(diag['expectedId']['value'], self.job['id'])
        self.assertEqual(diag['responseSHA256'], W.digest(W.canonical(value)))
        self.assertEqual(diag['cliExitCode'], 0)
        self.assertIsNone(diag['httpStatus'])
        self.assertEqual(diag['configCanonicalSHA256'], W.digest(self.f.config))

    def test_hostile_response_keys_values_and_stderr_never_reach_diagnostics(self):
        secret = 'private-token-never-print'
        value = {'id': 'https://user:' + secret + '@invalid.test/path', 'board': secret,
                 'status': secret, secret: {'nested': secret}, 'result': {'log_tail': secret}}
        diag = self.failed(lambda: self.cli(value, stderr=('Error: ' + secret).encode()))
        encoded = W.canonical(diag).decode()
        self.assertNotIn(secret, encoded)
        self.assertNotIn('https://', encoded)
        self.assertNotIn('log_tail', encoded)
        self.assertEqual(diag['returnedId']['sha256'], W.digest(value['id'].encode()))
        self.assertEqual(diag['stderrSHA256'], W.digest(('Error: ' + secret).encode()))
        self.assertEqual(diag['knownFieldsPresent'], ['id', 'board', 'status', 'result'])

    def test_metadata_bounded_with_large_and_nontext_fields(self):
        value = {'id': ['secret'] * 10000, 'board': {'private': 'secret'},
                 'status': False, 'result': {'huge': 'x' * 1000000}}
        diag = self.failed(lambda: self.cli(value))
        self.assertEqual(diag['returnedId'], {'present': True, 'type': 'array'})
        self.assertEqual(diag['returnedBoard'], {'present': True, 'type': 'object'})
        self.assertNotIn('secret', W.canonical(diag).decode())
        self.assertLess(len(W.canonical(diag)), 1500)

    def test_cli_bad_board_and_bad_id_remain_rejected(self):
        for change in [{'id': 'f' * 16}, {'board': 'muse_attention_decode'}]:
            with self.subTest(change=change):
                value = {'id': self.job['id'], 'board': self.job['problem'], **change}
                self.failed(lambda: self.cli(value))

    def test_success_returns_exact_value_without_diagnostic_or_extra_request(self):
        value = {'id': self.job['id'], 'board': self.job['problem'], 'status': 'running', 'result': None}
        self.assertEqual(self.cli(value), value)

    def test_transport_and_parse_errors_keep_existing_classification(self):
        with self.assertRaisesRegex(ValueError, 'OFFICIAL_OBSERVATION_UNAVAILABLE') as caught:
            self.cli({}, code=1, stderr=b'private transport detail')
        self.assertEqual(W.official_error_fields(caught.exception, self.f.config), {})
        with self.assertRaises(json.JSONDecodeError):
            self.cli(None, stdout=b'not-json')
        with self.assertRaises(AttributeError):
            self.cli([])  # Preserve the preexisting non-object error too.

    def test_cache_identity_failure_never_queries_or_mutates_cache_or_ledger(self):
        bad = {'id': self.job['id'], 'board': 'muse_attention_decode', 'status': 'done'}
        self.f.w.state['official'][self.job['id']] = copy.deepcopy(bad)
        before = copy.deepcopy(self.f.w.state)
        ledger = self.f.root / 'untouched-ledger'; ledger.write_bytes(b'owned uncertain effect')
        with mock.patch.object(self.f.client, 'official', side_effect=AssertionError('no lookup')):
            diag = self.failed(self.f.w.observe)
        self.assertEqual(diag['origin'], 'cache')
        self.assertEqual(diag['responseEncoding'], 'canonical-json')
        self.assertEqual(self.f.w.state, before)
        self.assertEqual(ledger.read_bytes(), b'owned uncertain effect')
        self.assertEqual(self.f.client.posts, [])

    def test_fake_client_failure_remains_fail_closed_and_no_receipt_is_published(self):
        with mock.patch.object(self.f.client, 'official', return_value={'id': self.job['id']}):
            diag = self.failed(self.f.w.tick)
        self.assertEqual(diag['origin'], 'client')
        self.assertNotIn(self.job['id'], self.f.w.state['official'])
        self.assertFalse((self.f.w.directory / ('official-' + self.job['id'] + '.json')).exists())
        self.assertEqual(self.f.client.posts, [])

    def test_diagnostic_failure_does_not_replace_or_weaken_original_error(self):
        with mock.patch.object(W, 'canonical', side_effect=RuntimeError('diagnostic unavailable')):
            with self.assertRaisesRegex(ValueError, '^OFFICIAL_IDENTITY$') as caught:
                W.official_identity_guard({'id': '0' * 16}, self.job, 'cache')
        self.assertIs(type(caught.exception), ValueError)
        self.assertEqual(W.official_error_fields(caught.exception, self.f.config), {})

    def test_untrusted_exception_attribute_is_not_propagated(self):
        error = ValueError('OFFICIAL_IDENTITY')
        error._official_observation = b'{"body":"secret"}'
        self.assertEqual(W.official_error_fields(error, self.f.config), {})

    def test_blocked_status_publishes_only_safe_diagnostic_and_not_acceptance(self):
        with self.assertRaises(ValueError) as caught:
            self.cli({'id': self.job['id'], 'status': 'queued'})
        result = self.f.w.status('observation_or_dispatch_blocked', code=str(caught.exception),
            automaticPostRetry=False, **W.official_error_fields(caught.exception, self.f.config))
        latest = W.read_json(self.f.w.directory / 'latest.json')
        self.assertEqual(latest, result)
        self.assertEqual(latest['code'], 'OFFICIAL_IDENTITY')
        self.assertIs(latest['automaticPostRetry'], False)
        self.assertIn('officialObservationFailure', latest)
        self.assertNotIn('acceptance', latest)
        self.assertEqual(self.f.client.posts, [])


if __name__ == '__main__':
    unittest.main()
