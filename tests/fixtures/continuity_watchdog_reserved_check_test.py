"""An authenticated single unknown CHECK owns its source, not unrelated CPU work."""
import copy
import importlib.util
import pathlib
import unittest

spec = importlib.util.spec_from_file_location('reserved_check_base',
    pathlib.Path(__file__).with_name('continuity_watchdog_test.py'))
B = importlib.util.module_from_spec(spec); spec.loader.exec_module(B); W = B.W


class ReservedCheckCPUTests(unittest.TestCase):
    def setUp(self):
        self.f = B.WatchTests(); self.f.setUp(); self.addCleanup(self.f.tearDown)
        self.row = self.f.repairable_operation()
        self.f.enable_mixed_plan_contract()
        self.row.update(state='uncertain_owned', code='UNCERTAIN_RECONCILE_NO_RESEND', postAttempted=None)
        self.f.install_diagnostic_fixture([self.row])
        self.detail = self.f.client.occurrences[self.row['occurrenceId']]
        self.policy = W.read_json(self.f.config['autoCheckPolicyPath'])
        self.pin = {'problem': self.row['problem'], 'sourceSHA256': self.row['candidateSHA256'],
            'occurrenceId': self.row['occurrenceId'], 'intentEpoch': 9990, 'ledgerSHA256': 'b' * 64,
            'originPolicy': {'path': str(self.f.root / 'old-policy.json'), 'sha256': 'c' * 64}}
        self.policy['uncertainCheckReservations'] = {'version': 1, 'intents': [self.pin]}
        self.row['uncertainCheckReservation'] = {'version': 1, 'slots': 1, 'verifiedAt': 10000,
            'detailSHA256': W.digest(self.detail), 'policySHA256': W.digest(self.policy),
            'ownSourceUnretryable': True, 'effectReconciled': False, 'intent': copy.deepcopy(self.pin)}
        self.write()

    def write(self, rows=None):
        W.atomic(self.f.config['autoCheckPolicyPath'], self.policy)
        status = W.read_json(self.f.config['autoCheckStatusPath'])
        status.update(policySHA256=W.digest(self.policy), outcomes=rows if rows is not None else [self.row])
        W.atomic(self.f.config['autoCheckStatusPath'], status)

    def test_verified_reservation_keeps_wip_and_releases_other_sources_only(self):
        got = self.f.w.observe(); board = self.row['problem']
        self.assertEqual(got['pipelineUsed'], 1)
        self.assertEqual(got['waitingPipelineUsed'], 1)
        self.assertEqual(got['boardAvailableCandidateSlots'][board], 7)
        self.assertIn(board, got['eligibleProblems'])
        self.assertEqual(got['uncertainProblems'], [])
        self.assertEqual(got['autoCheckDiagnostics']['repairableEvidence'], [])
        self.assertEqual(got['pipelineOwners'][0]['stages'], ['uncertain_owned', 'unmeasured'])
        own_ids = {a['id'] for a in got['catalog'] if a['digest'] == self.pin['sourceSHA256']}
        self.assertTrue(own_ids)
        self.assertFalse(own_ids.intersection(got['confirmedSourceArtifactIds']))
        item = got['autoCheckDiagnostics']['outcomes'][0]
        self.assertEqual(item['verifiedCheckReservation'], self.pin)
        self.assertNotIn('postAttempted', item)  # Never reinterpret uncertainty as no effect.

    def test_default_absent_policy_or_marker_retains_original_board_veto(self):
        for missing in ['policy', 'marker']:
            savedp, savedr = copy.deepcopy(self.policy), copy.deepcopy(self.row)
            if missing == 'policy': self.policy.pop('uncertainCheckReservations')
            else: self.row.pop('uncertainCheckReservation')
            self.write(); got = self.f.w.observe()
            self.assertEqual(got['pipelineUsed'], 1)
            self.assertEqual(got['boardAvailableCandidateSlots'][self.row['problem']], 0)
            self.assertNotIn('reservedOfficialSources', got)
            self.policy, self.row = savedp, savedr

    def test_disabled_policy_and_inactive_controller_never_relax_ownership(self):
        for enabled, controller in [(False, 'active'), (True, 'inspection_only')]:
            self.policy['enabled'] = enabled
            self.row['uncertainCheckReservation']['policySHA256'] = W.digest(self.policy)
            self.write()
            status = W.read_json(self.f.config['autoCheckStatusPath']); status['controllerState'] = controller
            W.atomic(self.f.config['autoCheckStatusPath'], status)
            got = self.f.w.observe()
            self.assertEqual(got['pipelineUsed'], 1)
            self.assertEqual(got['boardAvailableCandidateSlots'][self.row['problem']], 0)
            self.assertNotIn('verifiedCheckReservation', got['autoCheckDiagnostics']['outcomes'][0])

    def test_invalid_proof_and_multiplicity_never_relax_veto(self):
        original = copy.deepcopy(self.row)
        changes = [{'version': True}, {'slots': True}, {'slots': 2}, {'verifiedAt': True},
            {'verifiedAt': 9639}, {'verifiedAt': 10001}, {'policySHA256': '0' * 64},
            {'detailSHA256': '0' * 64}, {'ownSourceUnretryable': False}, {'effectReconciled': True},
            {'intent': {**self.pin, 'ledgerSHA256': 'd' * 64}}, {'extra': True}]
        for change in changes:
            with self.subTest(change=change):
                self.row = copy.deepcopy(original); self.row['uncertainCheckReservation'].update(change)
                self.write(); got = self.f.w.observe()
                self.assertEqual(got['boardAvailableCandidateSlots'][self.row['problem']], 0)
                self.assertEqual(got['pipelineUsed'], 1)

    def test_post_null_is_required_and_never_made_false(self):
        original = copy.deepcopy(self.row)
        for changes in [{'postAttempted': False}, {'postAttempted': True}, {'jobId': 'a' * 16},
                        {'state': 'account_wait'}, {'code': 'OTHER'}]:
            self.row = {**copy.deepcopy(original), **changes}; self.write()
            self.assertNotIn('verifiedCheckReservation', self.f.w.observe()['autoCheckDiagnostics']['outcomes'][0])
        for key in ['postAttempted', 'jobId']:
            self.row = copy.deepcopy(original); self.row.pop(key); self.write()
            self.assertNotIn('verifiedCheckReservation', self.f.w.observe()['autoCheckDiagnostics']['outcomes'][0])

    def test_current_attempt_inventory_and_qa_binding_cannot_borrow_old_proof(self):
        original = copy.deepcopy(self.detail)
        for change in ['fence', 'artifact', 'step', 'qa']:
            current = copy.deepcopy(original)
            if change == 'fence': current['attempts'][0]['fence'] = B.uid()
            elif change == 'artifact': current['artifacts'][0]['digest'] = 'e' * 64
            elif change == 'step': current['steps'][0]['attempt'] += 1
            else: self.row['qaSHA256'] = 'e' * 64
            self.f.client.occurrences[self.row['occurrenceId']] = current
            self.write(); got = self.f.w.observe()
            self.assertNotIn('verifiedCheckReservation', got['autoCheckDiagnostics']['outcomes'][0])

    def test_same_source_alias_is_still_reserved_and_cannot_be_delegated(self):
        alias = copy.deepcopy(self.detail); aid = B.uid(); alias['occurrence']['id'] = aid
        self.f.client.occurrences[aid] = alias
        original_record = next(r for r in self.f.w.state['experiments'] if r['occurrenceId'] == self.row['occurrenceId'])
        self.f.w.state['experiments'].append({**original_record, 'occurrenceId': aid, 'experimentId': 'source-alias'})
        row = {**copy.deepcopy(self.row), 'occurrenceId': aid, 'code': 'SOURCE_PREVIOUS_PROVENANCE'}
        row['uncertainCheckReservation']['detailSHA256'] = W.digest(alias)
        self.write([self.row, row]); got = self.f.w.observe()
        self.assertEqual(got['pipelineUsed'], 1)
        self.assertEqual(got['uncertainProblems'], [])
        self.assertEqual(set(got['pipelineOwners'][0]['occurrenceIds']), {aid, self.row['occurrenceId']})
        self.assertTrue(all(a['id'] not in got['confirmedSourceArtifactIds'] for a in got['catalog']
            if a['digest'] == self.pin['sourceSHA256']))
        self.assertEqual(len(got['autoCheckDiagnostics']['repairableEvidence']), 0)

    def test_any_other_unknown_still_vetoes_board_even_with_valid_reservation(self):
        observed = self.f.w.observe(); good = observed['autoCheckDiagnostics']['outcomes'][0]
        bad = {**good, 'occurrenceId': B.uid(), 'candidateSHA256': 'f' * 64}
        bad.pop('verifiedCheckReservation')
        result = W.pipeline_admission(24, [], observed['catalog'], set(), [], {'outcomes': [good, bad]},
            set(observed['confirmedSourceArtifactIds']), capacity_policy=self.f.config)
        self.assertEqual(result['pipelineUsed'], 2)
        self.assertEqual(result['boardAvailableCandidateSlots'][self.row['problem']], 0)

    def test_four_prefill_owners_keep_four_slots_not_eight_and_global_cap_stays_hard(self):
        observed = self.f.w.observe(); good = observed['autoCheckDiagnostics']['outcomes'][0]
        board = 'muse_prefill_layer'; good = copy.deepcopy(good); good['problem'] = board
        good['verifiedCheckReservation']['problem'] = board
        catalog = [{**a, 'problem': board} for a in observed['catalog']]
        rows = [good]
        for n in range(3):
            oid, sha = B.uid(), W.digest(str(n).encode())
            catalog.append({'id': B.uid(), 'problem': board, 'occurrenceId': oid, 'digest': sha})
            rows.append({'problem': board, 'occurrenceId': oid, 'candidateSHA256': sha,
                'state': 'account_wait', 'code': 'ACCOUNT_HAS_UNCERTAIN_EFFECT', 'postAttempted': False, 'jobId': None})
        result = W.pipeline_admission(24, [], catalog, set(), [], {'outcomes': rows},
            {a['id'] for a in catalog}, capacity_policy=self.f.config)
        self.assertEqual(result['pipelineUsed'], 4)
        self.assertEqual(result['boardAvailableCandidateSlots'][board], 4)
        result = W.pipeline_admission(4, [], catalog, set(), [], {'outcomes': rows},
            {a['id'] for a in catalog}, capacity_policy=self.f.config)
        self.assertEqual(result['availableCandidateSlots'], 0)
        self.assertEqual(result['eligibleProblems'], [])

    def test_reverification_time_is_not_semantic_progress_or_new_authority(self):
        before = self.f.w.observe()
        for now in [10060, 10120]:
            self.f.w.clock = lambda n=now: n
            self.row['uncertainCheckReservation']['verifiedAt'] = now
            self.write(); current = self.f.w.observe()
            self.assertEqual(current['autoCheckDiagnostics'], before['autoCheckDiagnostics'])
            self.assertEqual(W.action_evidence(current, self.f.config, version=2),
                             W.action_evidence(before, self.f.config, version=2))

    def test_plan_dispatches_other_source_but_never_original_or_alias(self):
        self.assertEqual(self.f.w.tick()['state'], 'supervisor_dispatched')
        plan = self.f.mixed_plan({self.row['problem']: 1})
        observed = self.f.w.observe()
        plan['experiments'][0]['sourceArtifactId'] = next(a['id'] for a in observed['catalog']
            if a['problem'] == self.row['problem'] and a['id'] in observed['confirmedSourceArtifactIds'])
        forbidden = copy.deepcopy(plan)
        forbidden['experiments'][0]['sourceArtifactId'] = next(a['id'] for a in observed['catalog']
            if a['digest'] == self.pin['sourceSHA256'])
        with self.assertRaisesRegex(ValueError, 'SOURCE_OFFICIAL_EFFECT_RESERVED'):
            W.validate_plan(forbidden, observed, self.f.config, expected_fingerprint=plan['evidenceFingerprint'],
                source_input=self.f.w.supervision_input(self.f.w.state['supervisions'][-1]))
        self.f.complete_review(plan)
        result = self.f.w.tick()
        self.assertEqual(result['state'], 'experiments_dispatched')
        self.assertEqual(len(result['dispatched']), 1)
        final = self.f.w.observe()
        self.assertEqual(final['pipelineUsed'], 2)
        self.assertTrue(any(r['candidateSHA256'] == self.pin['sourceSHA256'] for r in final['pipelineOwners']))
        self.assertFalse(any(r.get('repairOf') == self.row['occurrenceId'] for r in self.f.w.state['experiments']))


if __name__ == '__main__': unittest.main()
