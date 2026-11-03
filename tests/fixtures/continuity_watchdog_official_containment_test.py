"""Server-only per-job observation containment; no network, model or CHECK."""
import copy
import importlib.util
import json
import pathlib
import subprocess
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location('official_containment_base',
    pathlib.Path(__file__).with_name('continuity_watchdog_test.py'))
B = importlib.util.module_from_spec(spec); spec.loader.exec_module(B); W = B.W


class OfficialContainmentTests(unittest.TestCase):
    def setUp(self):
        self.f = B.WatchTests(); self.f.setUp(); self.addCleanup(self.f.tearDown)
        self.original = self.f.client.official
        self.bad = self.f.add_gpu_job(problem='muse_decode_layer')
        self.good = self.f.add_gpu_job(problem='muse_prefill_layer', source='b' * 64, status='done')
        self.calls = []

    def failure(self, job, value=None, origin='cli', **options):
        value = {'status': 'response unavailable'} if value is None else value
        W.official_identity_guard(value, job, origin, stdout=W.canonical(value),
            stderr=b'', returncode=options.get('returncode', 0), started=0)

    def observe(self, bad_ids=None, value=None):
        bad_ids = {self.bad['id']} if bad_ids is None else bad_ids
        def lookup(job):
            self.calls.append(job['id'])
            if job['id'] in bad_ids: self.failure(job, value)
            return self.original(job)
        with mock.patch.object(self.f.client, 'official', side_effect=lookup):
            return self.f.w.observe()

    def test_invalid_first_job_does_not_discard_later_terminal_result(self):
        observed = self.observe()
        self.assertEqual(self.calls, [self.bad['id'], self.good['id']])
        rows = {r['id']: r for r in observed['officialJobs']}
        self.assertEqual(rows[self.bad['id']]['status'], 'observation_unavailable')
        self.assertEqual(rows[self.bad['id']]['result'], {})
        self.assertIsNone(rows[self.bad['id']]['caseCounts'])
        self.assertNotIn('receiptSHA256', rows[self.bad['id']])
        self.assertNotIn('receiptPath', rows[self.bad['id']])
        self.assertEqual(rows[self.good['id']]['status'], 'done')
        self.f.reload()
        self.assertIn(self.good['id'], self.f.w.state['official'])
        self.assertNotIn(self.bad['id'], self.f.w.state['official'])
        self.assertFalse((self.f.w.directory / ('official-' + self.bad['id'] + '.json')).exists())
        self.assertEqual(self.f.client.posts, [])

    def test_all_unavailable_jobs_keep_exact_source_owners_and_capacity(self):
        observed = self.observe({self.bad['id'], self.good['id']})
        self.assertCountEqual(observed['pendingOfficialJobs'], [self.bad['id'], self.good['id']])
        self.assertEqual(observed['pipelineUsed'], 2)
        self.assertEqual(observed['availableCandidateSlots'], 0)
        self.assertEqual(observed['eligibleProblems'], [])
        owners = {r['candidateSHA256']: r for r in observed['pipelineOwners']}
        for job in [self.bad, self.good]:
            self.assertEqual(owners[job['sourceSHA256']]['jobIds'], [job['id']])
            self.assertEqual(owners[job['sourceSHA256']]['stages'], ['gpu'])
        self.assertEqual(self.f.w.state['official'], {})
        self.assertEqual(self.f.client.posts, [])

    def test_future_valid_response_recovers_same_job_without_resubmission(self):
        before = self.observe()
        self.assertEqual(before['pipelineUsed'], 1)
        self.f.client.official_states[self.bad['id']] = 'done'
        after = self.observe(set())
        self.assertEqual(after['pipelineUsed'], 0)
        self.assertEqual(after['pendingOfficialJobs'], [])
        self.assertEqual(self.f.w.state['official'][self.bad['id']]['id'], self.bad['id'])
        self.assertNotEqual(before['fingerprint'], after['fingerprint'])
        self.assertEqual(self.f.client.posts, [])

    def test_pending_unknown_job_never_triggers_a_replacement_post(self):
        def lookup(job):
            if job['id'] == self.bad['id']: self.failure(job)
            return self.original(job)
        with mock.patch.object(self.f.client, 'official', side_effect=lookup):
            result = self.f.w.tick()
        self.assertEqual(result['state'], 'work_active')
        self.assertEqual(result['officialJobs'], [self.bad['id']])
        self.assertEqual(self.f.client.posts, [])
        self.assertEqual(list(self.f.w.directory.glob('intent-*.json')), [])

    def test_real_client_guard_remains_exact_and_only_lookup_is_contained(self):
        client = object.__new__(W.Client)
        for bad in [{'status': 'pending'}, {'id': 'f' * 16, 'board': self.bad['problem']},
                    {'id': self.bad['id'], 'board': 'muse_attention_decode'}]:
            with self.subTest(bad=bad), mock.patch.object(W.subprocess, 'run',
                    return_value=subprocess.CompletedProcess([], 0, W.canonical(bad), b'')):
                with self.assertRaisesRegex(ValueError, '^OFFICIAL_IDENTITY$'):
                    client.official(self.bad)
                with mock.patch.object(self.f.client, 'official', side_effect=client.official):
                    result = self.f.w.observe()
                self.assertEqual(result['pipelineUsed'], 2)
                self.assertEqual(self.f.w.state['official'], {})
        self.assertEqual(self.f.client.posts, [])

    def test_non_cli_identity_or_other_error_remains_fail_closed(self):
        for origin, code in [('client', 0), ('cache', 0), ('cli', 1)]:
            with self.subTest(origin=origin, code=code):
                with mock.patch.object(self.f.client, 'official',
                        side_effect=lambda job: self.failure(job, origin=origin, returncode=code)):
                    with self.assertRaisesRegex(ValueError, '^OFFICIAL_IDENTITY$'):
                        self.f.w.observe()
        for error in [ValueError('OFFICIAL_IDENTITY'), ValueError('OFFICIAL_OBSERVATION_UNAVAILABLE'),
                      json.JSONDecodeError('bad', 'x', 0), TimeoutError('expired')]:
            with mock.patch.object(self.f.client, 'official', side_effect=error):
                with self.assertRaises(type(error)): self.f.w.observe()
        self.assertEqual(self.f.client.posts, [])

    def test_cli_diagnostic_for_different_job_is_not_a_containment_token(self):
        with mock.patch.object(self.f.client, 'official', side_effect=lambda job: self.failure(self.good)):
            with self.assertRaisesRegex(ValueError, '^OFFICIAL_IDENTITY$'): self.f.w.observe()
        self.assertEqual(self.f.client.posts, [])

    def test_corrupt_local_terminal_cache_still_aborts_without_lookup(self):
        self.f.w.state['official'][self.bad['id']] = {
            'id': self.bad['id'], 'board': 'muse_attention_decode', 'status': 'done'}
        before = copy.deepcopy(self.f.w.state)
        with mock.patch.object(self.f.client, 'official', side_effect=AssertionError('no CLI')):
            with self.assertRaisesRegex(ValueError, '^OFFICIAL_IDENTITY$'): self.f.w.observe()
        self.assertEqual(self.f.w.state, before)
        self.assertEqual(self.f.client.posts, [])

    def test_repeated_unavailable_observation_does_not_refresh_semantic_readset(self):
        first = self.observe(value={'status': 'response one'})
        second = self.observe(value={'status': 'response two', 'error': 'different diagnostic'})
        self.assertEqual(first['fingerprint'], second['fingerprint'])
        self.assertEqual(W.action_evidence(first, self.f.config), W.action_evidence(second, self.f.config))
        self.assertEqual(first['pipelineOwners'], second['pipelineOwners'])
        self.assertEqual(self.f.client.posts, [])

    def test_recovery_only_invalidates_selected_source_official_proof(self):
        self.f.config['officialJobs'] = []
        pending, original = self.f.prepared_source_action()
        selected = next(r for r in original['catalog'] if r['id'] == pending['approvedPlan']['experiments'][0]['sourceArtifactId'])
        job = {'id': 'e' * 16, 'problem': selected['problem'], 'sourceSHA256': selected['digest'],
               'status': 'observation_unavailable', 'observationCode': 'OFFICIAL_IDENTITY', 'result': {}, 'caseCounts': None}
        original['officialJobs'].append(job)
        pending['actionEvidence'] = W.action_evidence(original, self.f.config)
        fresh = copy.deepcopy(original)
        self.assertTrue(W.unchanged_action_scope(pending, fresh, self.f.config))
        fresh['officialJobs'][-1].update(status='done', result={'passed': False})
        self.assertFalse(W.unchanged_action_scope(pending, fresh, self.f.config))
        fresh = copy.deepcopy(original)
        fresh['officialJobs'].append({**job, 'id': 'd' * 16, 'sourceSHA256': 'f' * 64, 'status': 'done', 'result': {'passed': True}})
        self.assertTrue(W.unchanged_action_scope(pending, fresh, self.f.config))

    def test_diagnostics_are_bounded_not_raw_and_clear_on_next_valid_observation(self):
        secret = 'sensitive-response-never-publish'
        self.observe(value={'status': secret, secret: secret})
        latest = self.f.w.status('observed')
        self.assertNotIn(secret, W.canonical(latest).decode())
        self.assertEqual(len(latest['officialObservationFailures']), 1)
        self.assertLessEqual(len(W.canonical(latest['officialObservationFailures'][0])), 2048)
        self.observe(set())
        self.assertNotIn('officialObservationFailures', self.f.w.status('observed'))

    def test_existing_terminal_bytes_and_unknown_intents_are_not_rewritten(self):
        terminal = self.original(self.good); terminal.update(observedAt=5, finished_at=4)
        self.f.w.state['official'][self.good['id']] = terminal
        ledger = self.f.w.directory / 'unresolved-effect.json'; ledger.write_bytes(b'owned unknown effect')
        before = W.canonical(terminal)
        self.observe()
        self.assertEqual(W.canonical(self.f.w.state['official'][self.good['id']]), before)
        self.assertEqual(ledger.read_bytes(), b'owned unknown effect')
        self.assertNotIn(self.good['id'], self.calls)
        self.assertEqual(list(self.f.w.directory.glob('intent-*.json')), [])
        self.assertEqual(self.f.client.posts, [])


if __name__ == '__main__':
    unittest.main()
