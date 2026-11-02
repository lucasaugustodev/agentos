"""Terminal scan evidence survives partial sweeps, never stale or changed bindings."""
import copy
import importlib.util
import pathlib
import unittest

spec = importlib.util.spec_from_file_location('terminal_diagnostics_base',
    pathlib.Path(__file__).with_name('continuity_watchdog_test.py'))
B = importlib.util.module_from_spec(spec); spec.loader.exec_module(B); W = B.W


class TerminalDiagnosticsTests(unittest.TestCase):
    def setUp(self):
        self.f = B.WatchTests(); self.f.setUp(); self.addCleanup(self.f.tearDown)
        self.row = self.f.repairable_operation()
        self.detail = self.f.client.occurrences[self.row['occurrenceId']]
        self.policy = W.read_json(self.f.config['autoCheckPolicyPath'])
        self.policy['adapterSHA256'] = 'a' * 64
        W.atomic(self.f.config['autoCheckPolicyPath'], self.policy)
        self.status = W.read_json(self.f.config['autoCheckStatusPath'])
        self.status['policySHA256'] = W.digest(self.policy)
        self.row['diagnosticProof'] = {'version': 1, 'verifyKind': 'native_identity', 'observedAt': 9900, 'verifiedAt': 10000,
            'policySHA256': W.digest(self.policy), 'adapterSHA256': 'a' * 64,
            'detailSHA256': W.digest(self.detail), 'dispatchSHA256': 'b' * 64}
        self.write()

    def write(self, row=None, rows=None):
        self.status['outcomes'] = rows if rows is not None else [self.row if row is None else row]
        W.atomic(self.f.config['autoCheckStatusPath'], self.status)

    def test_three_reverified_partial_sweeps_preserve_repair_identity_and_dispatch(self):
        initial = self.f.w.observe()
        expected = W.action_evidence(initial, self.f.config, version=2)
        self.assertEqual(len(initial['autoCheckDiagnostics']['repairableEvidence']), 1)
        for now in [10090, 10180, 10270]:
            self.f.w.clock = lambda n=now: n
            self.status['observedAt'] = now
            self.row['diagnosticProof']['verifiedAt'] = now
            self.write()
            current = self.f.w.observe()
            self.assertEqual(current['autoCheckDiagnostics'], initial['autoCheckDiagnostics'])
            self.assertEqual(W.action_evidence(current, self.f.config, version=2), expected)
            self.assertEqual(self.row['diagnosticProof']['observedAt'], 9900)
            self.assertEqual(current['pipelineUsed'], 0)
        self.assertEqual(self.f.submit_repair_plan()['state'], 'experiments_dispatched')
        latest = self.f.w.observe()
        self.assertEqual(len(latest['active']), 1)
        self.assertEqual(latest['pipelineUsed'], 1)
        self.assertEqual(self.f.w.state['experiments'][-1]['repairOf'], self.row['occurrenceId'])

    def test_new_top_level_time_cannot_rejuvenate_stale_or_expired_row(self):
        for now, verified in [(10400, 10000), (13501, 13501)]:
            self.f.w.clock = lambda n=now: n
            self.status['observedAt'] = now
            self.row['diagnosticProof']['verifiedAt'] = verified
            self.write()
            got = self.f.w.observe()
            self.assertEqual(got['autoCheckDiagnostics']['repairableEvidence'], [])
            self.assertEqual(got['pipelineUsed'], 1)

    def test_invalid_proofs_preserve_owner_and_never_authorize_repair(self):
        original = copy.deepcopy(self.row)
        changes = [{'version': 2}, {'version': True}, {'verifyKind': 'full_audit'}, {'observedAt': True}, {'verifiedAt': float('inf')},
            {'observedAt': -1}, {'observedAt': 10001}, {'verifiedAt': 10006}, {'verifiedAt': 10004},
            {'verifiedAt': 9899}, {'policySHA256': 'c' * 64}, {'adapterSHA256': 'c' * 64},
            {'detailSHA256': 'c' * 64}, {'dispatchSHA256': 'bad'}]
        # Nonfinite numbers are rejected at the helper before JSON publication.
        for change in changes:
            row = copy.deepcopy(original); row['diagnosticProof'].update(change)
            with self.subTest(change=change):
                self.assertFalse(W.terminal_diagnostic_current(row, self.detail, self.policy,
                    W.digest(self.policy), 10000, 360))
                if change.get('verifiedAt') == float('inf'): continue
                self.write(row)
                got = self.f.w.observe()
                self.assertFalse(got['autoCheckDiagnostics']['repairableEvidence'])
                self.assertEqual(got['pipelineUsed'], 1)

    def test_native_identity_changes_invalidate_cached_diagnostic(self):
        original = copy.deepcopy(self.detail)
        for target in ['input', 'attempt', 'artifact', 'step']:
            current = copy.deepcopy(original)
            if target == 'input': current['occurrence']['input']['newInstruction'] = 'changed'
            elif target == 'attempt': current['attempts'][0]['fence_token'] = 'new-fence'
            elif target == 'artifact': current['artifacts'][0]['digest'] = 'd' * 64
            else: current['steps'][0]['attempt'] += 1
            with self.subTest(target=target):
                self.assertFalse(W.terminal_diagnostic_current(self.row, current, self.policy,
                    W.digest(self.policy), 10000, 360))
        self.assertTrue(W.terminal_diagnostic_current(self.row, original, self.policy,
            W.digest(self.policy), 10000, 360))

    def test_proof_cannot_bless_quota_unknown_effect_or_missing_explicit_no_post(self):
        for change in [{'state': 'quota_wait'}, {'state': 'account_wait'}, {'state': 'uncertain_owned'},
                       {'postAttempted': True}, {'postAttempted': None}, {'postAttempted': 0},
                       {'jobId': '0123456789abcdef'}, {'diagnosticProof': None}]:
            with self.subTest(change=change):
                self.write({**self.row, **change})
                self.assertFalse(self.f.w.observe()['autoCheckDiagnostics']['repairableEvidence'])
        missing_job = dict(self.row); missing_job.pop('jobId')
        self.write(missing_job)
        self.assertFalse(self.f.w.observe()['autoCheckDiagnostics']['repairableEvidence'])

    def test_identical_metadata_never_substitutes_for_verifying_artifact_bytes(self):
        artifacts = {a['name']: a for a in self.detail['artifacts']}
        for name in ['qa-verification.json', 'cpu-result.json', 'cpu-checks.py',
                     'candidate.py', 'implementation.patch', 'verification.json']:
            artifact = artifacts[name]
            original = self.f.client.artifacts[artifact['id']]
            self.f.client.artifacts[artifact['id']] = b'tampered bytes without metadata update'
            with self.subTest(name=name):
                self.assertTrue(W.terminal_diagnostic_current(self.row, self.detail, self.policy,
                    W.digest(self.policy), 10000, 360))
                got = self.f.w.observe()
                self.assertFalse(got['autoCheckDiagnostics']['repairableEvidence'])
                self.assertFalse(got['active'])
            self.f.client.artifacts[artifact['id']] = original

    def test_duplicate_owner_fails_closed_and_legacy_fresh_row_retains_contract(self):
        self.write(rows=[self.row, copy.deepcopy(self.row)])
        with self.assertRaisesRegex(ValueError, 'AUTO_CHECK_DUPLICATE_OWNER'):
            self.f.w.observe()
        legacy = dict(self.row); legacy.pop('diagnosticProof')
        self.write(legacy)
        self.assertEqual(len(self.f.w.observe()['autoCheckDiagnostics']['repairableEvidence']), 1)


if __name__ == '__main__': unittest.main()
