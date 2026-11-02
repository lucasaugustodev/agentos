"""Isolated-server regression tests: authenticated QA context is not acceptance."""
import base64
import copy
import importlib.util
import json
import pathlib
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location('base_qa_projection_fixture',
    pathlib.Path(__file__).with_name('continuity_watchdog_test.py'))
B = importlib.util.module_from_spec(spec)
spec.loader.exec_module(B)
W = B.W


class QAProjectionTests(unittest.TestCase):
    def setUp(self):
        self.f = B.WatchTests()
        self.f.setUp()
        self.addCleanup(self.f.tearDown)
        self.status, self.detail, self.qa, self.latest, self.report = self.f.prior_qa_diagnostic_fixture()

    def store(self, report=None, raw=None):
        raw = W.canonical(report) if raw is None else raw
        self.f.client.artifacts[self.qa['id']] = raw
        self.qa.update(size=len(raw), digest=W.digest(raw))
        self.status['qaSHA256'] = self.qa['digest']
        self.f.install_diagnostic_fixture([self.status])
        return raw

    def diagnostic(self):
        return self.f.w.repair_qa_diagnostic(self.detail, self.qa, self.status['problem'],
            self.latest, self.status['candidateSHA256'])

    def large(self):
        self.report['cpuEvidence']['cases'] = [
            {'name': 'case-' + str(i), 'passed': True, 'detail': 'D' * 2000, 'ordinal': i}
            for i in range(40)]
        self.report['cpuEvidence']['checkerSHA256'] = next(a['digest'] for a in self.detail['artifacts'] if a['name'] == 'cpu-checks.py')
        self.report['cpuEvidence']['resultSHA256'] = next(a['digest'] for a in self.detail['artifacts'] if a['name'] == 'cpu-result.json')
        self.report['cpuEvidence']['command'] = 'python3 cpu-checks.py'
        self.report['extra/large~field'] = 'Z' * 40000
        return self.store(self.report)

    def test_pretty_raw_over_64k_preserves_complete_small_report_and_exact_bytes(self):
        raw = self.store(raw=b' ' * 75000 + W.canonical(self.report))
        result = self.diagnostic()
        self.assertEqual(result['report'], self.report)
        self.assertNotIn('reportProjection', result)
        self.assertEqual(result['artifact']['digest'], W.digest(raw))
        self.assertEqual(result['artifact']['size'], len(raw))
        self.assertLessEqual(len(W.canonical(result)), W.REPAIR_QA_CONTEXT_LIMIT)
        context, full = self.f.w.qa_context(self.detail, self.qa, self.status['problem'], self.latest)
        self.assertEqual(context['bindingState'], 'verified')
        self.assertEqual(full, self.report)
        self.assertLessEqual(len(W.canonical(context)), W.QA_CONTEXT_LIMIT)
        self.assertEqual(self.f.client.artifacts[self.qa['id']], raw)

    def test_projection_is_deterministic_and_preserves_whole_values_order_and_original_hash(self):
        raw = self.large(); before = copy.deepcopy(self.report)
        result = self.diagnostic(); projection = result['reportProjection']
        self.assertEqual(result, self.diagnostic())
        self.assertEqual(self.report, before)
        self.assertLessEqual(len(W.canonical(result)), W.REPAIR_QA_CONTEXT_LIMIT)
        self.assertFalse(result['currentAcceptance']); self.assertFalse(projection['complete'])
        self.assertEqual(projection['originalArtifact']['digest'], W.digest(raw))
        self.assertEqual(projection['reportSHA256'], W.digest(self.report))
        self.assertEqual(projection['reportBytes'], len(W.canonical(self.report)))
        self.assertEqual(projection['selectedReportSHA256'], W.digest(W.request_numbers_v1(result['report'])))
        for key in ['passed', 'candidateSHA256', 'sourceSHA256', 'findings', 'limitations']:
            self.assertEqual(result['report'][key], self.report[key])
        self.assertNotIn('cpuEvidence', result['report'])
        self.assertEqual(projection['arrayCounts']['/cpuEvidence/cases'], 40)
        sections = {row['pointer']: row['value'] for row in projection['sections']}
        self.assertNotIn('/cpuEvidence/cases', sections)
        self.assertEqual(sections['/cpuEvidence/negativeControls'], self.report['cpuEvidence']['negativeControls'])
        refs = {row['pointer']: row for row in projection['omittedFieldReferences']}
        self.assertEqual(refs['/cpuEvidence']['sha256'], W.digest(self.report['cpuEvidence']))
        self.assertEqual(refs['/extra~1large~0field']['bytes'], len(W.canonical(self.report['extra/large~field'])))

    def test_projected_repair_worker_receives_complete_original_separately_and_keeps_independent_qa(self):
        raw = self.large(); original_detail = copy.deepcopy(self.detail)
        self.assertEqual(self.f.submit_repair_plan()['state'], 'experiments_dispatched')
        new = self.f.client.occurrences[self.f.w.state['experiments'][-1]['occurrenceId']]
        packet = new['occurrence']['input']['experiment']
        material = next(m for m in packet['materials'] if m['name'] == 'prior-qa-original.json')
        self.assertEqual(base64.b64decode(material['base64']), raw)
        self.assertEqual(material['sha256'], W.digest(raw))
        self.assertEqual(material['artifactId'], self.qa['id'])
        self.assertEqual(material['attemptId'], self.qa['attempt_id'])
        self.assertEqual(material['bytes'], len(raw))
        self.assertEqual(packet['repairQADiagnostic']['reportProjection']['originalArtifact']['digest'], material['sha256'])
        self.assertNotEqual(W.digest(packet['repairQADiagnostic']['report']), material['sha256'])
        self.assertEqual(self.detail, original_detail)
        self.assertEqual(packet['repairTarget']['candidateSHA256'], self.status['candidateSHA256'])
        steps = new['occurrence']['definition']['steps']
        self.assertEqual([s['key'] for s in steps], ['produce', 'independent-qa'])
        self.assertIn('at most 1048576 bytes', steps[1]['instructions'])
        self.assertNotIn('at most 32768 bytes', steps[1]['instructions'])
        for step in steps:
            self.assertIn('prior-qa-original.json', step['instructions'])
            self.assertIn('finalizer', step['instructions'])
        self.assertEqual(new['occurrence']['definition']['allowedActions'], [])

    def test_native_to_unmeasured_large_qa_retains_reservation_without_adapter_status(self):
        raw = self.large()
        self.f.install_diagnostic_fixture([])
        observation = self.f.w.observe()
        owner = [o for o in observation['pipelineOwners'] if self.status['occurrenceId'] in o['occurrenceIds']]
        self.assertEqual(len(owner), 1)
        self.assertEqual(owner[0]['stages'], ['unmeasured'])
        self.assertIn(self.status['candidateSHA256'], observation['pendingOfficialCandidateSHA256'])
        self.assertEqual(observation['pipelineUsed'], 1)
        self.assertEqual(self.f.client.artifacts[self.qa['id']], raw)
        context = next(a['qaEvidence'] for o in observation['outcomes']
            for a in o['artifacts'] if a['id'] == self.qa['id'])
        self.assertEqual(context['bindingState'], 'verified')

    def test_projection_hash_domains_survive_api_integral_float_roundtrip(self):
        self.large()
        self.report['findings'][0]['metrics'] = {'integral': 1.0, 'signedZero': -0.0, 'fraction': 0.125, 'boolean': True}
        self.report['cpuEvidence']['negativeControls'][0]['note']['missing'] = 16.0
        raw = self.store(self.report)
        before = self.diagnostic()
        # Independent simulation of JSON API number representation: integer
        # number spellings are returned as JSON integers, booleans stay booleans.
        def api_number(text):
            value = float(text)
            return int(value) if value.is_integer() else value
        after = json.loads(json.dumps(before, ensure_ascii=False), parse_float=api_number)
        self.assertNotEqual(W.digest(before['report']), W.digest(after['report']))
        projection = after['reportProjection']
        self.assertEqual(projection['selectedReportCanonicalization'], 'json-numbers-v1')
        self.assertEqual(projection['selectedReportSHA256'], W.digest(after['report']))
        self.assertIs(after['report']['findings'][0]['metrics']['boolean'], True)
        self.assertEqual(projection['originalArtifact']['digest'], W.digest(raw))
        self.assertEqual(projection['reportHashDomain'], 'original_strict_json_canonical')
        self.assertEqual(projection['omittedFieldHashDomain'], 'original_strict_json_canonical')
        self.assertEqual(projection['reportSHA256'], W.digest(json.loads(raw)))
        self.assertEqual(projection['reportBytes'], len(W.canonical(json.loads(raw))))
        for section in projection['sections']:
            original = self.report
            for part in section['pointer'].split('/')[1:]:
                original = original[part.replace('~1', '/').replace('~0', '~')]
            self.assertEqual(section['valueCanonicalization'], 'json-numbers-v1')
            self.assertEqual(section['valueSHA256'], W.digest(section['value']))
            self.assertEqual(section['value'], W.request_numbers_v1(original))
        for ref in projection['omittedFieldReferences']:
            original = self.report
            for part in ref['pointer'].split('/')[1:]:
                original = original[part.replace('~1', '/').replace('~0', '~')]
            self.assertEqual(ref['sha256'], W.digest(original))
            self.assertEqual(ref['bytes'], len(W.canonical(original)))
        normalized = W.request_numbers_v1(before)
        self.assertEqual(W.canonical(after), W.canonical(normalized))
        tampered = copy.deepcopy(after); tampered['report']['findings'][0]['metrics']['fraction'] = 0.25
        self.assertNotEqual(W.digest(tampered['report']), projection['selectedReportSHA256'])

    def test_exact_one_mib_artifact_admitted_but_over_bound_rejected_before_fetch(self):
        core = W.canonical(self.report)
        self.store(raw=core + b' ' * (W.QA_ARTIFACT_LIMIT - len(core)))
        self.assertEqual(self.diagnostic()['report'], self.report)
        self.qa['size'] += 1
        with mock.patch.object(self.f.client, 'request', side_effect=AssertionError('must not fetch oversized artifact')):
            with self.assertRaisesRegex(ValueError, 'REPAIR_QA_DIAGNOSTIC_SIZE'): self.diagnostic()
            context, full = self.f.w.qa_context(self.detail, self.qa, self.status['problem'], self.latest)
            self.assertEqual(context['code'], 'QA_CONTEXT_SIZE_EXCEEDED')
            self.assertIsNone(full)

    def test_strict_raw_parser_rejects_duplicate_nonfinite_and_overflow_without_context_leak(self):
        for raw, code in [(b'{"secret":"do-not-inject","x":{"n":1,"n":2}}', 'QA_REPORT_DUPLICATE_KEY'),
                          (b'{"secret":"do-not-inject","n":NaN}', 'QA_REPORT_NONFINITE'),
                          (b'{"secret":"do-not-inject","n":1e999}', 'QA_REPORT_NONFINITE'),
                          (b'[]', 'QA_CONTEXT_SCHEMA')]:
            with self.subTest(code=code):
                self.store(raw=raw)
                context, report = self.f.w.qa_context(self.detail, self.qa, self.status['problem'], self.latest)
                self.assertEqual(context['bindingState'], 'unavailable')
                self.assertEqual(context['code'], code); self.assertIsNone(report)
                self.assertNotIn('do-not-inject', W.canonical(context).decode())
                with self.assertRaisesRegex(ValueError, code): self.diagnostic()

    def test_projected_report_never_bypasses_candidate_source_fence_or_manifest_guards(self):
        self.large()
        for change, code in [('candidate', 'QA_CONTEXT_CANDIDATE_BINDING'),
                             ('source', 'QA_CONTEXT_SOURCE_BINDING'), ('fence', 'REPAIR_QA_MANIFEST_BINDING'),
                             ('stop', 'QA_CONTEXT_STOP_UNCONFIRMED')]:
            original = copy.deepcopy(self.detail); report = copy.deepcopy(self.report)
            with self.subTest(change=change):
                if change in ['candidate', 'source']:
                    report[change + 'SHA256'] = 'f' * 64
                else:
                    attempt = next(a for a in self.detail['attempts'] if a['id'] == self.qa['attempt_id'])
                    if change == 'fence': attempt['fence'] = B.uid()
                    else: attempt['receipt']['stopped'] = False
                self.store(report)
                with self.assertRaisesRegex(ValueError, code): self.diagnostic()
                self.detail.clear(); self.detail.update(original)
                self.qa = next(a for a in self.detail['artifacts'] if a['id'] == self.qa['id'])

    def test_blockers_are_complete_and_semantic_failure_cannot_be_repaired_as_metadata(self):
        self.large()
        self.report['passed'] = False
        self.report['blockingFindings'] = [{'id': 'wrong-arithmetic', 'detail': 'Semantic mismatch.'}]
        self.store(self.report)
        observation = self.f.w.observe()
        self.assertEqual(observation['autoCheckDiagnostics']['outcomes'][0]['classification'], 'new_hypothesis_required')
        self.assertEqual(observation['autoCheckDiagnostics']['repairableEvidence'], [])
        context, _ = self.f.w.qa_context(self.detail, self.qa, self.status['problem'], self.latest)
        self.assertEqual(context['blockingFindings'], self.report['blockingFindings'])
        self.report['blockingFindings'][0]['detail'] = 'x' * 40000
        self.store(self.report)
        context, full = self.f.w.qa_context(self.detail, self.qa, self.status['problem'], self.latest)
        self.assertEqual(context['code'], 'QA_CONTEXT_BLOCKERS_SIZE_EXCEEDED')
        self.assertEqual(context['bindingState'], 'unavailable'); self.assertEqual(full, self.report)
        self.assertNotIn('blockingFindings', context)

    def test_presentation_failure_keeps_internal_ownership_but_cannot_authorize_repair(self):
        self.large()
        self.report['blockingFindings'] = [{'id': 'too-large-context', 'detail': 'B' * 40000}]
        self.store(self.report)
        self.f.install_diagnostic_fixture([])
        observation = self.f.w.observe()
        owners = [o for o in observation['pipelineOwners'] if self.status['occurrenceId'] in o['occurrenceIds']]
        self.assertEqual(len(owners), 1)
        self.assertEqual(owners[0]['stages'], ['unmeasured'])
        self.assertIn(self.status['candidateSHA256'], observation['pendingOfficialCandidateSHA256'])
        posts = len(self.f.client.posts)
        with self.assertRaisesRegex(ValueError, 'QA_CONTEXT_BLOCKERS_SIZE_EXCEEDED'): self.diagnostic()
        self.assertEqual(len(self.f.client.posts), posts)
        self.assertEqual(observation['autoCheckDiagnostics']['repairableEvidence'], [])

    def test_omitted_field_mutation_changes_authenticated_diagnostic_and_scope(self):
        self.large(); old = self.diagnostic()
        old_observation = self.f.w.observe()
        self.report['extra/large~field'] += 'changed'
        self.store(self.report); new = self.diagnostic(); new_observation = self.f.w.observe()
        self.assertEqual(old['report'], new['report'])
        self.assertNotEqual(old['reportProjection']['reportSHA256'], new['reportProjection']['reportSHA256'])
        self.assertNotEqual(old['artifact']['digest'], new['artifact']['digest'])
        source = next(a for a in old_observation['catalog'] if a['occurrenceId'] == self.status['occurrenceId'])
        self.assertNotEqual(W.source_action_evidence(old_observation, self.f.config)['sources'][source['id']],
                            W.source_action_evidence(new_observation, self.f.config)['sources'][source['id']])

    def test_original_material_corruption_prevents_any_repair_post(self):
        self.large()
        self.assertEqual(self.f.w.tick()['state'], 'supervisor_dispatched')
        supervisor = self.f.w.state['supervisions'][-1]
        plan = self.f.plan(); plan['decision'] = 'repair_evidence'
        observation = self.f.w.observe()
        plan['experiments'][0]['sourceArtifactId'] = observation['autoCheckDiagnostics']['repairableEvidence'][0]['candidateArtifactId']
        supervisor['approvedPlan'] = plan
        posts = len(self.f.client.posts)
        self.f.client.artifacts[self.qa['id']] += b' '
        with self.assertRaisesRegex(ValueError, 'ARTIFACT_CORRUPT'):
            self.f.w.dispatch_experiment(plan['experiments'][0], observation, supervisor)
        self.assertEqual(len(self.f.client.posts), posts)


if __name__ == '__main__':
    unittest.main()
