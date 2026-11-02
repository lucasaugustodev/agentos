"""A proven official quota wait reserves WIP without disabling independent CPU work."""
import copy
import importlib.util
import pathlib
import unittest

spec = importlib.util.spec_from_file_location('quota_cpu_base',
    pathlib.Path(__file__).with_name('continuity_watchdog_test.py'))
B = importlib.util.module_from_spec(spec)
spec.loader.exec_module(B)
W = B.W


class QuotaCPUAdmissionTests(unittest.TestCase):
    def setUp(self):
        self.f = B.WatchTests(); self.f.setUp(); self.addCleanup(self.f.tearDown)
        self.f.parallel_capacity()
        self.f.config.update(maxPendingCandidates=24, maxPipelinesPerProblem=8)
        self.observed = self.f.w.observe()
        self.source = self.observed['catalog'][0]
        self.row = {'problem': self.source['problem'], 'occurrenceId': self.source['occurrenceId'],
            'candidateSHA256': self.source['digest'], 'state': 'quota_wait', 'code': 'CHECK_QUOTA',
            'postAttempted': False, 'jobId': None}

    def admission(self, rows=None, pending=None, active=None, catalog=None, policy=None):
        return W.pipeline_admission(24, active or [], self.observed['catalog'] if catalog is None else catalog,
            pending or set(), [], {'outcomes': [self.row] if rows is None else rows},
            set(self.observed['confirmedSourceArtifactIds']), capacity_policy=self.f.config if policy is None else policy)

    def test_verified_quota_keeps_owner_but_permits_seven_independent_cpu_slots(self):
        got = self.admission(pending={self.source['digest']})
        self.assertEqual(got['pipelineUsed'], 1)
        self.assertEqual(got['waitingPipelineUsed'], 1)
        self.assertEqual(got['nativePipelineUsed'], 0)
        self.assertEqual(got['boardAvailableCandidateSlots'][self.source['problem']], 7)
        self.assertEqual(got['uncertainProblems'], [])
        self.assertIn(self.source['problem'], got['eligibleProblems'])
        self.assertEqual(got['pipelineOwners'][0]['stages'], ['quota_wait', 'unmeasured'])
        self.assertEqual(got['pipelineOwners'][0]['occurrenceIds'], [self.source['occurrenceId']])

    def test_incomplete_untyped_or_possible_effect_status_still_blocks_board(self):
        variants = [{'postAttempted': None}, {'postAttempted': True}, {'postAttempted': 0},
            {'code': 'LOCAL_RATE_WAIT'}, {'code': None}, {'jobId': '0123456789abcdef'},
            {'state': 'uncertain_owned'}, {'state': 'check_running'},
            {'candidateSHA256': 'a' * 64}, {'candidateSHA256': None}, {'occurrenceId': B.uid()}]
        rows = [{**self.row, **v} for v in variants]
        missing_post = dict(self.row); missing_post.pop('postAttempted'); rows.append(missing_post)
        missing_job = dict(self.row); missing_job.pop('jobId'); rows.append(missing_job)
        for row in rows:
            with self.subTest(row=row):
                got = self.admission([row])
                self.assertEqual(got['pipelineUsed'], 1)
                self.assertEqual(got['boardAvailableCandidateSlots'][self.source['problem']], 0)
                self.assertEqual(got['uncertainProblems'], [self.source['problem']])

    def test_unknown_effect_dominates_proven_quota_even_for_same_source(self):
        unknown = {**self.row, 'state': 'uncertain_owned', 'postAttempted': None}
        got = self.admission([self.row, unknown])
        self.assertEqual(got['pipelineUsed'], 1)
        self.assertEqual(got['uncertainProblems'], [self.source['problem']])
        self.assertEqual(got['boardAvailableCandidateSlots'][self.source['problem']], 0)

    def test_unknown_native_actor_cannot_be_hidden_by_quota_observation(self):
        got = self.admission(active=[{'problem': self.source['problem'], 'occurrenceId': B.uid(),
                                      'ownershipUncertain': True}])
        self.assertEqual(got['pipelineUsed'], 2)
        self.assertEqual(got['boardAvailableCandidateSlots'][self.source['problem']], 0)
        self.assertEqual(got['uncertainProblems'], [self.source['problem']])

    def test_eight_waiting_candidates_still_fill_board_and_twenty_four_fill_global_cap(self):
        catalog, rows = [], []
        for board in W.PROBLEMS:
            for n in range(8):
                source = {'id': B.uid(), 'problem': board, 'occurrenceId': B.uid(),
                          'digest': W.digest((board + str(n)).encode())}
                catalog.append(source)
                rows.append({**self.row, 'problem': board, 'occurrenceId': source['occurrenceId'],
                             'candidateSHA256': source['digest']})
        got = self.admission(rows, catalog=catalog)
        self.assertEqual(got['pipelineUsed'], 24)
        self.assertEqual(got['availableCandidateSlots'], 0)
        self.assertEqual(got['boardAvailableCandidateSlots'], {p: 0 for p in W.PROBLEMS})
        self.assertEqual(got['uncertainProblems'], [])
        self.assertEqual(got['eligibleProblems'], [])

    def test_legacy_serial_capacity_contract_stays_closed_for_owned_board(self):
        policy = dict(self.f.config); policy.pop('pipelineCapacityVersion')
        got = self.admission(policy=policy)
        self.assertEqual(got['pipelineUsed'], 1)
        self.assertEqual(got['eligibleProblems'], [])
        self.assertNotIn('uncertainProblems', got)

    def test_authenticated_status_reaches_planner_and_dispatch_without_repairing_quota_candidate(self):
        status = self.f.repairable_operation()
        quota = {**status, 'state': 'quota_wait', 'code': 'CHECK_QUOTA', 'postAttempted': False, 'jobId': None}
        self.f.install_diagnostic_fixture([quota])
        observed = self.f.w.observe()
        self.assertEqual(observed['autoCheckDiagnostics']['repairableEvidence'], [])
        self.assertIs(observed['autoCheckDiagnostics']['outcomes'][0]['postAttempted'], False)
        self.assertEqual(observed['pipelineUsed'], 1)
        self.assertEqual(observed['boardAvailableCandidateSlots']['muse_decode_layer'], 7)
        self.assertEqual(self.f.w.tick()['state'], 'supervisor_dispatched')
        self.f.complete_review(self.f.parallel_plan({'muse_decode_layer': 2}))
        outcome = self.f.w.tick()
        self.assertEqual(outcome['state'], 'experiments_dispatched')
        self.assertEqual(len(outcome['dispatched']), 2)
        latest = self.f.w.observe()
        self.assertEqual(latest['pipelineUsed'], 3)
        self.assertEqual(latest['nativePipelineUsed'], 2)
        self.assertEqual(latest['waitingPipelineUsed'], 1)
        self.assertTrue(any(row['candidateSHA256'] == quota['candidateSHA256'] and 'quota_wait' in row['stages']
                            for row in latest['pipelineOwners']))
        self.assertFalse(any(r.get('repairOf') == status['occurrenceId'] for r in self.f.w.state['experiments']))

    def test_marker_does_not_survive_other_status_or_wrong_policy(self):
        status = self.f.repairable_operation()
        incomplete = {**status, 'state': 'quota_wait', 'code': 'CHECK_QUOTA'}
        incomplete.pop('jobId')
        self.f.install_diagnostic_fixture([incomplete])
        observed = self.f.w.observe()
        self.assertNotIn('postAttempted', observed['autoCheckDiagnostics']['outcomes'][0])
        self.assertIn('muse_decode_layer', observed['uncertainProblems'])
        for changed in [{'state': 'uncertain_owned'}, {'code': 'OTHER'}, {'jobId': '0123456789abcdef'}]:
            row = {**status, 'state': 'quota_wait', 'code': 'CHECK_QUOTA', **changed}
            self.f.install_diagnostic_fixture([row])
            observed = self.f.w.observe()
            self.assertNotIn('postAttempted', observed['autoCheckDiagnostics']['outcomes'][0])
        self.f.install_diagnostic_fixture([{**status, 'state': 'quota_wait', 'code': 'CHECK_QUOTA'}])
        raw = W.read_json(self.f.config['autoCheckStatusPath']); raw['policySHA256'] = '0' * 64
        W.atomic(self.f.config['autoCheckStatusPath'], raw)
        with self.assertRaisesRegex(ValueError, 'AUTO_CHECK_POLICY_BINDING'):
            self.f.w.observe()


if __name__ == '__main__':
    unittest.main()
