"""Isolated-server evidence repair tests; conflicting QA is never acceptance."""
import base64
import copy
import importlib.util
import json
import pathlib
import unittest

spec = importlib.util.spec_from_file_location('base_historical_binding_fixture',
    pathlib.Path(__file__).with_name('continuity_watchdog_test.py'))
B = importlib.util.module_from_spec(spec); spec.loader.exec_module(B); W = B.W


class HistoricalQABindingTests(unittest.TestCase):
    def setUp(self):
        self.f = B.WatchTests(); self.f.setUp(); self.addCleanup(self.f.tearDown)
        self.status = self.f.repairable_operation()
        self.prepare(self.status)

    def prepare(self, status):
        self.status = status
        self.detail = self.f.client.occurrences[status['occurrenceId']]
        self.artifacts = {a['name']: a for a in self.detail['artifacts']}
        self.base = self.detail['occurrence']['input']['experiment']['sourceArtifact']
        self.cpu = {'passed': True, 'candidateSHA256': status['candidateSHA256'],
            'checkerSHA256': self.artifacts['cpu-checks.py']['digest'],
            'cases': [{'name': 'actual-case', 'passed': True}],
            'negativeControls': [{'name': 'mutated-output', 'detected': True}]}
        self.store('cpu-result.json', self.cpu)
        self.verification = {'passed': True, 'scopePreserved': True, 'gpuValidated': False,
            'officialActionsPerformed': False, 'sourceSHA256': self.base['digest'],
            'candidateSHA256': status['candidateSHA256']}
        self.store('verification.json', self.verification)
        self.report = {**self.verification, 'cpuReplayPassed': True, 'approvedForOfficialReview': True,
            'canonicalPrecisionPreserved': True, 'evaluatorIndependent': True,
            'upstreamAttributionPreserved': True, 'blockingFindings': [], 'findings': [],
            'limitations': ['CPU-only; GPU correctness and performance unmeasured.'],
            'cpuEvidence': {'checkerSHA256': 'a' * 64, 'resultSHA256': 'b' * 64,
                'cases': copy.deepcopy(self.cpu['cases']),
                'negativeControls': copy.deepcopy(self.cpu['negativeControls'])}}
        self.store('qa-verification.json', self.report)
        self.refresh()

    def store(self, name, value=None, raw=None):
        artifact = self.artifacts[name]
        raw = W.canonical(value) if raw is None else raw
        self.f.client.artifacts[artifact['id']] = raw
        artifact.update(size=len(raw), digest=W.digest(raw))
        if name == 'qa-verification.json': self.status['qaSHA256'] = artifact['digest']
        return raw

    def refresh(self): self.f.install_diagnostic_fixture([self.status])
    def observe(self): self.refresh(); return self.f.w.observe()
    def rejected(self):
        observation = self.observe()
        self.assertEqual(observation['autoCheckDiagnostics']['repairableEvidence'], [])
        return observation

    def test_exact_producer_can_explain_two_stale_qa_hashes_without_rewriting_report(self):
        before = copy.deepcopy(self.detail); raw = self.f.client.artifacts[self.artifacts['qa-verification.json']['id']]
        observation = self.observe(); rows = observation['autoCheckDiagnostics']['repairableEvidence']
        self.assertEqual(len(rows), 1)
        diagnostic = rows[0]['repairQADiagnostic']; conflict = diagnostic['evidenceBindingConflict']
        self.assertEqual(conflict['reportedCpuEvidence'], {
            'checkerSHA256': 'a' * 64, 'resultSHA256': 'b' * 64})
        self.assertFalse(conflict['currentAcceptance']); self.assertFalse(conflict['originalReportBindingValid'])
        self.assertEqual(set(conflict['conflictingFields']), {'checkerSHA256', 'resultSHA256'})
        self.assertEqual(conflict['actualProducerBindings'], {
            'checkerSHA256': self.artifacts['cpu-checks.py']['digest'],
            'resultSHA256': self.artifacts['cpu-result.json']['digest']})
        self.assertEqual(conflict['originalQAArtifact']['digest'], W.digest(raw))
        self.assertEqual(diagnostic['report'], self.report)
        self.assertEqual(self.f.client.artifacts[self.artifacts['qa-verification.json']['id']], raw)
        self.assertEqual(self.detail, before)
        self.assertNotIn(self.status['candidateSHA256'], observation['pendingOfficialCandidateSHA256'])

    def test_one_stale_hash_with_both_valid_refs_is_explicit_and_matching_report_is_legacy(self):
        self.report['cpuEvidence']['checkerSHA256'] = self.artifacts['cpu-checks.py']['digest']
        self.store('qa-verification.json', self.report)
        row = self.observe()['autoCheckDiagnostics']['repairableEvidence'][0]
        self.assertEqual(row['repairQADiagnostic']['evidenceBindingConflict']['conflictingFields'], ['resultSHA256'])
        self.report['cpuEvidence']['resultSHA256'] = self.artifacts['cpu-result.json']['digest']
        self.store('qa-verification.json', self.report)
        row = self.observe()['autoCheckDiagnostics']['repairableEvidence'][0]
        self.assertNotIn('evidenceBindingConflict', row['repairQADiagnostic'])
        self.f.submit_repair_plan()
        current = self.f.client.occurrences[self.f.w.state['experiments'][-1]['occurrenceId']]
        self.assertTrue(all('HISTORICAL QA BINDING CONFLICT' not in s['instructions'] for s in current['occurrence']['definition']['steps']))

    def test_new_repair_requires_fresh_cpu_and_separate_qa_with_original_candidate_and_patch(self):
        raw = self.f.client.artifacts[self.artifacts['qa-verification.json']['id']]
        result = self.f.submit_repair_plan(); self.assertEqual(result['state'], 'experiments_dispatched')
        current = self.f.client.occurrences[self.f.w.state['experiments'][-1]['occurrenceId']]['occurrence']
        self.assertFalse(current['input']['officialActionsAllowed'])
        packet = current['input']['experiment']; target = packet['repairTarget']
        self.assertEqual(target['candidateSHA256'], self.status['candidateSHA256'])
        self.assertEqual(target['canonicalPatch']['sha256'], self.artifacts['implementation.patch']['digest'])
        self.assertEqual([s['key'] for s in current['definition']['steps']], ['produce', 'independent-qa'])
        self.assertEqual(current['definition']['steps'][1]['dependsOn'], ['produce'])
        for step in current['definition']['steps']:
            self.assertIn('HISTORICAL QA BINDING CONFLICT', step['instructions'])
            self.assertIn('NEW evidence', step['instructions'])
            self.assertTrue(any(c.get('id') == 'immutable-repair-candidate' for c in step['criteria']))
        self.assertEqual(self.f.client.artifacts[self.artifacts['qa-verification.json']['id']], raw)

    def test_projected_original_retains_conflict_marker_and_exact_full_qa_material(self):
        self.report['largeContext'] = 'X' * 80000
        raw = self.store('qa-verification.json', self.report); self.refresh()
        row = self.f.w.observe()['autoCheckDiagnostics']['repairableEvidence'][0]
        diagnostic = row['repairQADiagnostic']
        self.assertFalse(diagnostic['reportProjection']['complete'])
        self.assertLessEqual(len(W.canonical(diagnostic)), W.REPAIR_QA_CONTEXT_LIMIT)
        self.assertEqual(diagnostic['evidenceBindingConflict']['originalQAArtifact']['digest'], W.digest(raw))
        self.f.submit_repair_plan()
        current = self.f.client.occurrences[self.f.w.state['experiments'][-1]['occurrenceId']]['occurrence']
        original = next(m for m in current['input']['experiment']['materials'] if m['name'] == 'prior-qa-original.json')
        self.assertEqual(base64.b64decode(original['base64']), raw)
        self.assertEqual(original['sha256'], W.digest(raw))

    def test_wrong_or_missing_producer_cpu_hash_never_becomes_historical_qa_repair(self):
        for field in ['candidateSHA256', 'checkerSHA256', 'sourceSHA256']:
            for value in [None, 'f' * 64, True]:
                cpu = copy.deepcopy(self.cpu)
                if value is None: cpu.pop(field, None)
                else: cpu[field] = value
                if field == 'sourceSHA256' and value is None: continue
                self.store('cpu-result.json', cpu)
                with self.subTest(field=field, value=value): self.rejected()
        self.store('cpu-result.json', raw=b'{"passed":true,"value":Infinity}')
        self.rejected()

    def test_malformed_or_missing_qa_hashes_remain_unrepairable(self):
        for field in ['checkerSHA256', 'resultSHA256']:
            for value in [None, True, 123, 'bad-hash', ['a' * 64]]:
                report = copy.deepcopy(self.report)
                if value is None: report['cpuEvidence'].pop(field)
                else: report['cpuEvidence'][field] = value
                self.store('qa-verification.json', report)
                with self.subTest(field=field, value=value): self.rejected()

    def test_false_or_untyped_semantic_claims_and_blockers_prevent_repair(self):
        for field in ['passed', 'scopePreserved', 'cpuReplayPassed', 'canonicalPrecisionPreserved',
                      'evaluatorIndependent', 'upstreamAttributionPreserved', 'approvedForOfficialReview']:
            for value in [False, None, 1, 'true']:
                report = copy.deepcopy(self.report); report[field] = value
                self.store('qa-verification.json', report)
                with self.subTest(field=field, value=value): self.rejected()
        for field, value in [('gpuValidated', True), ('officialActionsPerformed', True),
                             ('performanceGainProven', True),
                             ('blockingFindings', [{'id': 'numerical-defect'}]), ('blockingFindings', None)]:
            report = copy.deepcopy(self.report); report[field] = value
            self.store('qa-verification.json', report); self.rejected()

    def test_verification_base_candidate_or_pass_and_raw_integrity_must_be_exact(self):
        for field, value in [('sourceSHA256', 'f' * 64), ('candidateSHA256', 'f' * 64),
                             ('passed', False), ('scopePreserved', None), ('gpuValidated', True),
                             ('performanceGainProven', True)]:
            report = copy.deepcopy(self.verification); report[field] = value
            self.store('verification.json', report)
            with self.subTest(field=field): self.rejected()
        self.store('verification.json', self.verification)
        self.f.client.artifacts[self.artifacts['cpu-checks.py']['id']] += b'# changed after identity capture'
        self.rejected()

    def test_wrong_patch_source_qa_lineage_or_missing_manifest_remains_held(self):
        patch = self.f.client.artifacts[self.artifacts['implementation.patch']['id']]
        self.store('implementation.patch', raw=patch.replace(b'fixed candidate', b'different candidate'))
        self.rejected(); self.store('implementation.patch', raw=patch)
        report = copy.deepcopy(self.report); report['sourceSHA256'] = 'f' * 64
        self.store('qa-verification.json', report); self.rejected()
        self.store('qa-verification.json', self.report)
        for attempt in self.detail['attempts']:
            original = copy.deepcopy(attempt)
            for change in ['fence', 'manifest', 'stopped']:
                attempt.clear(); attempt.update(copy.deepcopy(original))
                if change == 'fence': attempt['fence'] = B.uid()
                elif change == 'manifest': attempt['context']['manifest'] = {}
                else: attempt['receipt']['stopped'] = False
                with self.subTest(attempt=attempt['id'], change=change): self.rejected()
            attempt.clear(); attempt.update(original)

    def test_quota_uncertain_and_attempt_cap_are_not_bypassed(self):
        for state in ['quota_wait', 'uncertain_owned', 'paused', 'controller_blocked']:
            self.f.install_diagnostic_fixture([{**self.status, 'state': state}])
            posts = len(self.f.client.posts)
            self.assertFalse(self.f.w.observe()['autoCheckDiagnostics']['repairableEvidence'])
            self.assertEqual(len(self.f.client.posts), posts)
        self.refresh()
        rows = [self.status]
        for _ in range(2):
            self.f.submit_repair_plan()
            status = self.f.complete_repair_source(self.f.w.state['experiments'][-1]['occurrenceId'])
            self.prepare(status); rows.append(status); self.f.install_diagnostic_fixture(rows)
        observation = self.f.w.observe()
        self.assertFalse(observation['autoCheckDiagnostics']['repairableEvidence'])
        self.assertTrue(any(r['classification'] == 'repair_exhausted'
            for r in observation['autoCheckDiagnostics']['outcomes']), observation['autoCheckDiagnostics']['outcomes'])


if __name__ == '__main__': unittest.main()
