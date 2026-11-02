"""Server-only deterministic qualification of the periodic supervisor controller."""
import copy
import difflib
import importlib.util
import json
import pathlib
import tempfile
import unittest
from unittest import mock
import uuid

SOURCE = pathlib.Path(__file__).parents[2] / 'scripts' / 'continuity-watchdog.py'
spec = importlib.util.spec_from_file_location('watchdog', SOURCE)
W = importlib.util.module_from_spec(spec)
spec.loader.exec_module(W)


def uid():
    return str(uuid.uuid4())


class FakeClient:
    def __init__(self, config):
        self.config = config
        self.cycles = {config['controlCycleId']: {'id': config['controlCycleId'], 'state': 'active',
            'definition': W.supervisor_definition(config), 'request_key': 'control-key'}}
        self.occurrences, self.artifacts, self.posts, self.gets = {}, {}, [], []
        self.timeout_after_commit_number = None
        self.timeout_before_commit_number = None
        self.official_states = {}

    def make_occurrence(self, cycle_id, body):
        oid = uid()
        occurrence = {'id': oid, 'cycle_id': cycle_id, 'state': 'queued', 'source_key': 'manual:' + body['requestKey'],
                      'input': body['input'], 'definition': self.cycles[cycle_id]['definition'], 'finished_at': None}
        self.occurrences[oid] = {'occurrence': occurrence, 'attempts': [], 'steps': [], 'artifacts': []}
        return occurrence

    def complete(self, oid, artifacts, state='completed', stopped=True):
        d = self.occurrences[oid]
        d['occurrence'].update({'state': state, 'finished_at': '2026-11-01T18:00:00Z'})
        sid, aid = uid(), uid()
        key = 'independent-qa' if 'qa-verification.json' in artifacts else 'produce'
        definition = next((s for s in d['occurrence'].get('definition', {}).get('steps', []) if s.get('key') == key), {'kind': 'agent'})
        d['steps'] = [{'id': sid, 'key': key, 'attempt': 1, 'state': state, 'definition': definition}]
        d['attempts'] = [{'id': aid, 'step_id': sid, 'number': 1, 'state': 'completed' if state == 'completed' else 'failed',
                         'finished_at': '2026-11-01T18:00:00Z', 'fence': uid(), 'receipt': {'stopped': stopped}}]
        d['artifacts'] = []
        for name, value in artifacts.items():
            raw = value if isinstance(value, bytes) else W.canonical(value)
            artifact_id = uid(); self.artifacts[artifact_id] = raw
            d['artifacts'].append({'id': artifact_id, 'name': name, 'digest': W.digest(raw), 'size': len(raw), 'attempt_id': aid})

    def request(self, method, route, body=None, binary=False):
        if method == 'POST':
            self.posts.append((route, copy.deepcopy(body)))
            number = len(self.posts)
            if number == self.timeout_before_commit_number:
                raise TimeoutError('transport observation expired')
            if route == '/api/cycles':
                result = {'id': uid(), 'request_key': body['requestKey'], 'definition': body['definition'], 'state': 'draft'}
                self.cycles[result['id']] = result
            else:
                result = self.make_occurrence(route.split('/')[3], body)
            if number == self.timeout_after_commit_number:
                raise TimeoutError('response lost after committed request')
            return copy.deepcopy(result)
        self.gets.append(route)
        if route == '/api/cycles':
            return {'cycles': list(self.cycles.values())}
        if route.startswith('/api/cycle-artifacts/'):
            return self.artifacts[route.split('/')[-1]]
        if route.startswith('/api/cycle-runs/'):
            return copy.deepcopy(self.occurrences[route.split('/')[-1]])
        cycle = self.cycles[route.split('/')[-1]]
        return {'cycle': copy.deepcopy(cycle), 'occurrences': [{'id': d['occurrence']['id']} for d in self.occurrences.values()
                    if d['occurrence']['cycle_id'] == cycle['id']]}

    def official(self, job):
        # Exact identity vocabulary observed in official okc status JSON 1717/b8d8.
        return {'id': job['id'], 'board': job['problem'], 'status': self.official_states.get(job['id'], 'done'),
                'result': {'passed': True, 'speedup': 1.211222, 'log_tail': 'real tests complete'}}


class WatchTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name)
        self.config = {'version': 1, 'enabled': True, 'watchId': 'openkernel-continuity-test',
            'workspace': 'openkernel/isolated-test', 'model': 'deepseek-v4-pro-0813',
            'accountId': 'agentos-api-' + uid(), 'maxPendingCandidates': 2, 'intervalSeconds': 120,
            'seedOccurrences': [], 'officialJobs': [], 'controlCycleId': uid(), 'problemMaterials': {}}
        self.config['controlDefinitionSHA256'] = W.digest(W.supervisor_definition(self.config))
        self.client = FakeClient(self.config)
        source_cycle = uid(); self.client.cycles[source_cycle] = {'id': source_cycle, 'state': 'draft', 'definition': {}}
        seed = self.client.make_occurrence(source_cycle, {'requestKey': 'seed-work', 'input': {}})
        self.seed = seed['id']
        self.config['seedOccurrences'] = [{'id': self.seed, 'problem': 'muse_decode_layer'}]
        self.client.complete(self.seed, {'candidate.py': b'# candidate source\n', 'qa-verification.json': {
            'passed': False, 'candidateSHA256': W.digest(b'# candidate source\n'), 'approvedForOfficialReview': False, 'gpuValidated': False}})
        refs = []
        for name in ['canonical-TASK.md', 'canonical-AGENTS.md']:
            p = self.root / name; p.write_bytes(b'Canonical scope and precision contract.\n')
            refs.append({'name': name, 'path': str(p), 'sha256': W.digest(p.read_bytes())})
        self.config['problemMaterials']['muse_decode_layer'] = refs
        self.w = W.Watchdog(self.config, self.root / 'state', self.client, clock=lambda: 10000)

    def tearDown(self):
        self.tmp.cleanup()

    def plan(self, n=1):
        review = self.w.state['supervisions'][-1]
        sources = {}
        for source in review['input']['catalog']:
            if source['problem'] in review['input']['eligibleProblems']:
                sources.setdefault(source['problem'], source)
        chosen = sorted(sources.values(), key=lambda r: ['muse_decode_layer', 'muse_prefill_layer', 'muse_attention_decode'].index(r['problem']))
        if not chosen: chosen = self.w.observe()['catalog'][:1]  # Deliberately invalid plans for diagnostic tests.
        return {'version': 1, 'evidenceFingerprint': review['fingerprint'], 'decision': 'experiments',
            'reason': 'Measured bottleneck requires a discriminating CPU experiment.', 'experiments': [
                {'id': 'experiment-' + str(i), 'problem': chosen[i % len(chosen)]['problem'], 'sourceArtifactId': chosen[i % len(chosen)]['id'],
                 'hypothesis': 'A different tile size may reduce launch overhead while preserving arithmetic.',
                 'instructions': 'Change only tile configuration and add a dispatch truth table with a negative control.'} for i in range(n)]}

    def add_seed(self, problem='muse_prefill_layer', raw=b'# independent prefill source\n'):
        cycle = uid(); self.client.cycles[cycle] = {'id': cycle, 'state': 'draft', 'definition': {}}
        row = self.client.make_occurrence(cycle, {'requestKey': 'seed-' + problem, 'input': {}})
        self.client.complete(row['id'], {'candidate.py': raw})
        self.config['seedOccurrences'].append({'id': row['id'], 'problem': problem})
        self.config['problemMaterials'][problem] = self.config['problemMaterials']['muse_decode_layer']
        return row['id']

    def add_gpu_job(self, problem='muse_prefill_layer', source='a' * 64, status='running'):
        job = {'id': uuid.uuid4().hex[:16], 'problem': problem, 'sourceSHA256': source}
        self.config['officialJobs'].append(job); self.client.official_states[job['id']] = status
        return job

    def complete_review(self, plan=None):
        self.client.complete(self.w.state['supervisions'][-1]['occurrenceId'], {'continuation-plan.json': self.plan() if plan is None else plan})

    def reload(self):
        self.w = W.Watchdog(self.config, self.root / 'state', self.client, clock=lambda: 10060)

    def register_completed_job(self):
        self.w.tick(); self.complete_review(); self.w.tick()
        row = self.w.state['experiments'][-1]
        source = self.client.occurrences[row['occurrenceId']]['occurrence']['input']['experiment']['sourceArtifact']
        self.client.complete(row['occurrenceId'], {'candidate.py': b'# reviewed new candidate\n', 'qa-verification.json': {
            'passed': True, 'candidateSHA256': W.digest(b'# reviewed new candidate\n'), 'sourceSHA256': source['digest'],
            'approvedForOfficialReview': True, 'cpuReplayPassed': True, 'scopePreserved': True,
            'gpuValidated': False, 'officialActionsPerformed': False}})
        qa = next(a for a in self.client.occurrences[row['occurrenceId']]['artifacts'] if a['name'] == 'qa-verification.json')
        receipt = self.root / 'registered-receipt.json'
        job = {'id': '0123456789abcdef', 'problem': row['problem'], 'sourceSHA256': W.digest(b'# reviewed new candidate\n'),
               'occurrenceId': row['occurrenceId'], 'qaArtifactId': qa['id'], 'qaSHA256': qa['digest'],
               'policySHA256': 'a' * 64, 'auditSHA256': 'b' * 64}
        transport = {'state': 'job_recorded', 'mode': 'check', 'identity': {**{k: job[k] for k in
            ['problem', 'occurrenceId', 'policySHA256', 'auditSHA256']}, 'sourceSha256': job['sourceSHA256']},
            'receipt': {'job_id': job['id']}}
        job['dispatchReceiptSHA256'] = 'c' * 64
        job['transportReceiptSHA256'] = W.digest(transport)
        W.atomic(receipt, {'version': 1, 'mode': 'check', **job, 'transportReceipt': transport})
        job.update(receiptPath=str(receipt), receiptSHA256=W.digest(receipt.read_bytes()))
        path = self.root / 'registry.json'; W.atomic(path, {'version': 1, 'watchId': self.config['watchId'], 'jobs': [job]})
        self.config['officialRegistryPath'] = str(path)
        return job, path

    def test_registered_official_job_releases_pending_and_wakes_next_supervisor(self):
        job, _ = self.register_completed_job()
        self.assertEqual(self.w.observe()['pendingOfficialCandidateSHA256'], [])
        self.assertEqual(self.w.observe()['officialJobs'][0]['id'], job['id'])
        self.assertEqual(self.w.tick()['state'], 'supervisor_dispatched')

    def test_registry_rejects_unregistered_owner_wrong_source_and_receipt(self):
        job, path = self.register_completed_job()
        for field, value, error in [('occurrenceId', uid(), 'REGISTERED_JOB_OWNER'),
                                  ('sourceSHA256', 'f' * 64, 'REGISTERED_JOB_ARTIFACTS'),
                                  ('receiptSHA256', 'f' * 64, 'REGISTERED_JOB_RECEIPT')]:
            W.atomic(path, {'version': 1, 'watchId': self.config['watchId'], 'jobs': [{**job, field: value}]})
            with self.assertRaisesRegex(ValueError, error): self.w.observe()

    def test_registry_rejects_failed_qa_and_old_attempt_artifacts(self):
        job, _ = self.register_completed_job()
        d = self.client.occurrences[job['occurrenceId']]
        d['steps'][0]['attempt'] += 1
        with self.assertRaisesRegex(ValueError, 'REGISTERED_JOB_ARTIFACTS'): self.w.observe()
        d['steps'][0]['attempt'] -= 1
        qa = next(a for a in d['artifacts'] if a['id'] == job['qaArtifactId'])
        review = json.loads(self.client.artifacts[qa['id']]); review['passed'] = False
        raw = W.canonical(review); self.client.artifacts[qa['id']] = raw; qa.update(digest=W.digest(raw), size=len(raw))
        job['qaSHA256'] = qa['digest']
        W.atomic(self.config['officialRegistryPath'], {'version': 1, 'watchId': self.config['watchId'], 'jobs': [job]})
        with self.assertRaisesRegex(ValueError, 'REGISTERED_JOB_QA'): self.w.observe()

    def test_matching_receipt_hash_cannot_hide_wrong_job_or_transport_binding(self):
        job, registry_path = self.register_completed_job()
        original = json.loads(pathlib.Path(job['receiptPath']).read_bytes())
        for kind in ['job', 'transport']:
            receipt = copy.deepcopy(original)
            if kind == 'job': receipt['id'] = 'f' * 16
            else: receipt['transportReceipt']['identity']['sourceSha256'] = 'f' * 64
            W.atomic(job['receiptPath'], receipt); job['receiptSHA256'] = W.digest(pathlib.Path(job['receiptPath']).read_bytes())
            W.atomic(registry_path, {'version': 1, 'watchId': self.config['watchId'], 'jobs': [job]})
            with self.assertRaisesRegex(ValueError, 'REGISTERED_RECEIPT_BINDING|REGISTERED_TRANSPORT_BINDING'):
                self.w.observe()

    def test_actual_official_record_vocabulary_counts_board_specific_cases(self):
        raw = {'result': {'records': [{'event': 'test', 'status': 'pass'} for _ in range(5)] +
                      [{'event': 'benchmark', 'status': 'pass'} for _ in range(8)] +
                      [{'event': 'profile', 'status': 'pass'} for _ in range(8)]}}
        self.assertEqual(W.case_counts(raw), {'tests': {'total': 5, 'passed': 5}, 'benchmarks': {'total': 8, 'passed': 8}})

    def test_compaction_preserves_unknown_source_incumbent_and_failure_evidence(self):
        path = self.root / 'comparisons.json'
        W.atomic(path, {'incumbents': [{'id': '0123456789abcdef', 'sourceSHA256': None, 'sourceBindingKnown': False,
                                       'problem': 'muse_decode_layer', 'speedup': 1.271882}]})
        self.config['comparisonContext'] = {'path': str(path), 'sha256': W.digest(path.read_bytes())}
        self.config['officialJobs'] = [{'id': '1123456789abcdef', 'problem': 'muse_decode_layer', 'sourceSHA256': 'a' * 64}]
        self.w.tick(); inp = self.w.state['supervisions'][-1]['input']
        self.assertIsNone(inp['comparisonContext']['incumbents'][0]['sourceSHA256'])
        self.assertNotIn('logTail', inp['officialJobs'][0])
        self.assertIn('receiptSHA256', inp['officialJobs'][0])
        self.assertFalse(any(a['digest'] == 'a' * 64 for a in inp['catalog']))
        self.assertLess(len(W.canonical(inp)), 20000)

    def complete_outcome_report(self, negative=True, cpu_passed=True):
        self.w.tick(); self.complete_review(); self.w.tick()
        oid = self.w.state['experiments'][-1]['occurrenceId']
        source = self.client.occurrences[oid]['occurrence']['input']['experiment']['sourceArtifact']['digest']
        candidate, checker = b'# a source-bound candidate\n', b'# a source-bound checker\n'
        verification = {'sourceSHA256': source, 'candidateSHA256': W.digest(candidate), 'passed': True,
            'hypothesisOutcome': 'falsified-before-implementation' if negative else 'implemented-awaiting-qa',
            'hypothesisSupported': not negative, 'semanticChangeVsSource': not negative,
            'officialCheckOrScoreRequested': False, 'decisionLocators': ['The base already contains the routed-pair operation.']}
        cpu = {'passed': cpu_passed, 'candidateSHA256': W.digest(candidate), 'checkerSHA256': W.digest(checker),
            'cases': [{'name': 'pair-scheduling', 'passed': cpu_passed}],
            'negativeControls': [{'name': 'routing-mutant', 'detected': True}]}
        self.client.complete(oid, {'candidate.py': candidate, 'cpu-checks.py': checker,
                                  'verification.json': verification, 'cpu-result.json': cpu})
        self.client.occurrences[oid]['steps'][0]['key'] = 'produce'
        return oid

    def replace_outcome_json(self, oid, name, mutate):
        row = next(a for a in self.client.occurrences[oid]['artifacts'] if a['name'] == name)
        value = json.loads(self.client.artifacts[row['id']]); mutate(value)
        raw = W.canonical(value); self.client.artifacts[row['id']] = raw; row.update(digest=W.digest(raw), size=len(raw))

    def test_negative_report_survives_real_input_compaction_without_becoming_acceptance(self):
        oid = self.complete_outcome_report()
        self.assertEqual(self.w.tick()['state'], 'supervisor_dispatched')
        row = next(r for r in self.w.state['supervisions'][-1]['input']['outcomes'] if r['occurrenceId'] == oid)
        evidence = row['executionEvidence']
        self.assertEqual(evidence['classification'], 'negative_result_reported')
        self.assertEqual(evidence['hypothesisOutcome'], 'falsified-before-implementation')
        self.assertFalse(evidence['semanticChangeVsSource'])
        self.assertTrue(evidence['cpu']['reportedPassed'])
        self.assertEqual(evidence['authority'], 'producer_report_only_not_qa_or_performance_acceptance')
        self.assertFalse(any(a['name'] == 'qa-verification.json' for a in row['artifacts']))

    def test_reported_negative_does_not_release_live_qa_owner(self):
        oid = self.complete_outcome_report(); d = self.client.occurrences[oid]
        sid, aid = uid(), uid(); d['occurrence']['state'] = 'running'
        d['steps'].append({'id': sid, 'key': 'independent-qa', 'state': 'running', 'attempt': 1})
        d['attempts'].append({'id': aid, 'step_id': sid, 'number': 1, 'state': 'running', 'receipt': {'stopped': False}})
        observation = self.w.observe()
        self.assertIn('muse_decode_layer', observation['blockedProblems'])
        self.assertTrue(any(aid in row['attemptIds'] for row in observation['active']))
        self.assertEqual(self.w.tick()['state'], 'work_active')
        self.assertEqual(d['attempts'][-1]['state'], 'running')

    def test_cpu_failure_is_distinct_from_reported_hypothesis_falsification(self):
        oid = self.complete_outcome_report(negative=False, cpu_passed=False)
        evidence = self.w.execution_evidence(self.client.occurrences[oid])
        self.assertEqual(evidence['classification'], 'cpu_check_failed_reported')
        self.assertEqual(evidence['cpu']['failedCases'], ['pair-scheduling'])
        self.assertEqual(evidence['cpu']['failedCaseCount'], 1)

    def failed_producer_diagnostics(self):
        oid = self.complete_outcome_report(negative=False, cpu_passed=False)
        detail = self.client.occurrences[oid]; attempt = detail['attempts'][0]
        detail['occurrence']['state'] = 'attention'; attempt['state'] = 'failed'
        attempt['context'] = {'manifest': {'cycleId': detail['occurrence']['cycle_id'],
            'occurrenceId': oid, 'step': 'produce', 'cycleInput': {'attemptId': attempt['id'],
            'occurrenceId': oid, 'runFence': attempt['fence'], 'digest': 'c' * 64}}}
        self.replace_outcome_json(oid, 'cpu-result.json', lambda value: value.update(
            cases=[{'name': 'cost-falsifier', 'passed': False,
                    'detail': 'CPU estimate: candidate 7000 us vs base 4000 us; not a GPU measurement.'}],
            details={'cost': {'model': {'memory_efficiency': 0.9, 'weight_passes': 8},
                              'candidate_us': 7000, 'base_us': 4000, 'estimated': True}},
            limitations=['No GPU or candidate execution; estimates depend on assumed bandwidth.'],
            gpuValidated=False, officialActionsPerformed=False))
        self.replace_outcome_json(oid, 'verification.json', lambda value: value.update(
            whyPassedIsFalse='The stated cost falsifier fired.',
            missingEvidence='Independent GPU measurement is absent.',
            contractRisk='The producer reports a storage-format concern, not an authoritative ruling.'))
        return oid

    def test_failed_producer_diagnostics_reach_diagnostic_only_without_authority(self):
        oid = self.failed_producer_diagnostics(); observation = self.w.observe()
        observation.update(diagnosticOnly=True, availableCandidateSlots=0, eligibleProblems=[])
        self.w.submit_supervisor(observation)
        inp = self.w.state['supervisions'][-1]['input']
        evidence = next(r for r in inp['outcomes'] if r['occurrenceId'] == oid)['executionEvidence']
        diagnostic = evidence['producerDiagnostics']
        sections = {(s['artifact'], s['pointer']): s['value'] for s in diagnostic['sections']}
        self.assertIn('7000 us', sections['cpu-result.json', '/cases/0']['detail'])
        self.assertEqual(sections['cpu-result.json', '/details/cost']['model']['weight_passes'], 8)
        self.assertIn('No GPU', sections['cpu-result.json', '/limitations'][0])
        self.assertEqual(diagnostic['authority'], 'producer_report_only_not_qa_or_performance_acceptance')
        self.assertFalse(diagnostic['currentAcceptance']); self.assertFalse(diagnostic['officialAuthorization'])
        self.assertEqual(diagnostic['runFence'], self.client.occurrences[oid]['attempts'][0]['fence'])
        self.assertEqual(inp['sourceCapsules']['sources'], [])
        self.assertNotIn('REPORT.txt', json.dumps(diagnostic))

    def test_failed_producer_diagnostic_fence_and_emitter_mismatch_withhold_text(self):
        oid = self.failed_producer_diagnostics(); original = copy.deepcopy(self.client.occurrences[oid])
        self.add_seed()
        for change in ['fence', 'attempt', 'occurrence', 'cycle', 'step', 'live']:
            with self.subTest(change=change):
                d = copy.deepcopy(original); self.client.occurrences[oid] = d
                attempt = d['attempts'][0]; manifest = attempt['context']['manifest']
                if change == 'fence': manifest['cycleInput']['runFence'] = uid()
                if change == 'attempt': manifest['cycleInput']['attemptId'] = uid()
                if change == 'occurrence': manifest['occurrenceId'] = uid()
                if change == 'cycle': manifest['cycleId'] = uid()
                if change == 'step': manifest['step'] = 'independent-qa'
                if change == 'live': attempt.update(state='running', finished_at=None)
                evidence = self.w.execution_evidence(d)['producerDiagnostics']
                self.assertEqual(evidence['bindingState'], 'unavailable')
                self.assertNotIn('7000', json.dumps(evidence))
                self.assertIn('muse_prefill_layer', self.w.observe()['eligibleProblems'])

    def test_failed_producer_diagnostics_hash_source_and_checker_mismatch_do_not_enter(self):
        oid = self.failed_producer_diagnostics(); d = self.client.occurrences[oid]
        original, blobs = copy.deepcopy(d), copy.deepcopy(self.client.artifacts)
        for change in ['hash', 'sourceSHA256', 'candidateSHA256', 'checkerSHA256', 'attempt']:
            self.client.occurrences[oid] = d = copy.deepcopy(original); self.client.artifacts = copy.deepcopy(blobs)
            if change == 'hash':
                row = next(a for a in d['artifacts'] if a['name'] == 'cpu-result.json')
                self.client.artifacts[row['id']] = b'{"detail":"unbound injected report"}'
            elif change == 'attempt':
                next(a for a in d['artifacts'] if a['name'] == 'cpu-result.json')['attempt_id'] = uid()
            else:
                self.replace_outcome_json(oid, 'cpu-result.json', lambda value: value.update({change: 'f' * 64}))
            evidence = self.w.execution_evidence(d)
            self.assertEqual(evidence['bindingState'], 'invalid')
            self.assertNotIn('producerDiagnostics', evidence)
            self.assertNotIn('7000', json.dumps(evidence))

    def test_failed_diagnostic_sections_are_whole_and_omissions_are_visible(self):
        oid = self.failed_producer_diagnostics()
        self.replace_outcome_json(oid, 'cpu-result.json', lambda value: value.update(
            details={'oversized': '界' * 10000, 'small': {'assumption': 'reported, not inferred'}}))
        diagnostic = self.w.execution_evidence(self.client.occurrences[oid])['producerDiagnostics']
        self.assertLessEqual(len(W.canonical(diagnostic)), W.PRODUCER_DIAGNOSTIC_LIMIT)
        self.assertGreater(diagnostic['omittedSectionCount'], 0)
        self.assertIn('PRODUCER_DIAGNOSTIC_BUDGET', diagnostic['omissionReasons'])
        self.assertNotIn('界', json.dumps(diagnostic, ensure_ascii=False))
        self.assertTrue(any(s['pointer'] == '/details/small' for s in diagnostic['sections']))

    def test_failed_diagnostic_total_budget_keeps_latest_and_marks_omission(self):
        oid = self.failed_producer_diagnostics(); evidence = self.w.execution_evidence(self.client.occurrences[oid])
        rows = []
        for i in range(12):
            e = copy.deepcopy(evidence); e['producerDiagnostics']['sections'] = [
                {'artifact': 'cpu-result.json', 'pointer': '/details', 'value': 'x' * 14000}]
            e['producerDiagnostics']['finishedAt'] = f'2026-11-01T18:{i:02}:00Z'
            rows.append({'occurrenceId': str(i), 'problem': 'muse_decode_layer', 'state': 'attention',
                'finishedAt': None, 'failures': [], 'artifacts': [], 'executionEvidence': e})
        compact = W.compact_outcomes(rows)
        diagnostics = [r['executionEvidence']['producerDiagnostics'] for r in compact]
        self.assertLessEqual(sum(len(W.canonical(d)) for d in diagnostics),
                             W.SUPERVISOR_DIAGNOSTIC_LIMIT)
        self.assertNotIn('sections', diagnostics[0]); self.assertIn('sections', diagnostics[-1])
        self.assertEqual(diagnostics[0]['code'], 'SUPERVISOR_DIAGNOSTIC_BUDGET')
        self.assertIn('sections', rows[0]['executionEvidence']['producerDiagnostics'])  # No mutation.

    def test_three_hundred_diagnostics_include_all_metadata_in_global_budget(self):
        oid = self.failed_producer_diagnostics(); evidence = self.w.execution_evidence(self.client.occurrences[oid])
        rows = []
        for i in range(300):
            e = copy.deepcopy(evidence); e['producerDiagnostics']['sections'] = [
                {'artifact': 'cpu-result.json', 'pointer': '/details', 'value': 'x' * 14000}]
            rows.append({'occurrenceId': uid(), 'problem': 'muse_decode_layer', 'state': 'attention',
                'finishedAt': None, 'failures': [], 'artifacts': [], 'executionEvidence': e})
        original = W.canonical(rows)
        compact = W.compact_outcomes(rows)
        self.assertLessEqual(sum(len(W.canonical(r['executionEvidence']['producerDiagnostics']))
            for r in compact if 'producerDiagnostics' in r['executionEvidence']), W.SUPERVISOR_DIAGNOSTIC_LIMIT)
        summary = {}; compact = W.compact_outcomes(rows, summary)
        self.assertLessEqual(len(W.canonical(summary)) + sum(len(W.canonical(r['executionEvidence']['producerDiagnostics']))
            for r in compact if 'producerDiagnostics' in r['executionEvidence']), W.SUPERVISOR_DIAGNOSTIC_LIMIT)
        self.assertGreater(summary['omittedEnvelopeCount'], 0)
        self.assertGreater(summary['omittedMarkerCount'], 0)
        self.assertLessEqual(len(summary['references']), 8)
        self.assertEqual(summary['omittedReferenceCount'], summary['omittedEnvelopeCount'] - len(summary['references']))
        self.assertEqual(W.canonical(rows), original)
        self.assertEqual([r['executionEvidence']['artifacts'] for r in compact],
                         [r['executionEvidence']['artifacts'] for r in rows])

    def test_failed_diagnostic_does_not_invent_absent_model_or_contract(self):
        oid = self.failed_producer_diagnostics()
        self.replace_outcome_json(oid, 'cpu-result.json', lambda value: [value.pop(k, None)
            for k in ['details', 'limitations', 'gpuValidated', 'officialActionsPerformed']])
        self.replace_outcome_json(oid, 'verification.json', lambda value: [value.pop(k, None)
            for k in ['whyPassedIsFalse', 'missingEvidence', 'contractRisk']])
        diagnostic = self.w.execution_evidence(self.client.occurrences[oid])['producerDiagnostics']
        self.assertEqual([s['pointer'] for s in diagnostic['sections']], ['/cases/0'])
        self.assertNotIn('model', json.dumps(diagnostic))
        self.assertNotIn('contractRisk', json.dumps(diagnostic))

    def test_pending_supervisor_replay_keeps_old_diagnostics_exactly(self):
        oid = self.failed_producer_diagnostics(); observation = self.w.observe()
        self.client.timeout_after_commit_number = len(self.client.posts) + 1
        with self.assertRaises(TimeoutError): self.w.submit_supervisor(observation)
        before = copy.deepcopy(self.w.state['reviewRequests'])
        posted = copy.deepcopy(self.client.posts[-1]); count = len(self.client.posts)
        changed = copy.deepcopy(observation)
        next(r for r in changed['outcomes'] if r['occurrenceId'] == oid)['executionEvidence']['producerDiagnostics'] = {
            'bindingState': 'unavailable', 'code': 'FUTURE_READ_FAILED'}
        self.w.submit_supervisor(changed)  # Same durable intent: GET reconciliation only.
        self.assertEqual(self.w.state['reviewRequests'], before)
        self.assertEqual(len(self.client.posts), count)
        self.assertEqual(self.w.state['supervisions'][-1]['input'], posted[1]['input'])

    def test_failed_report_io_error_is_local_and_does_not_stop_other_board(self):
        oid = self.failed_producer_diagnostics(); self.add_seed()
        row = next(a for a in self.client.occurrences[oid]['artifacts'] if a['name'] == 'cpu-result.json')
        original_request = self.client.request
        def unavailable(method, route, *args, **kwargs):
            if route == '/api/cycle-artifacts/' + row['id']:
                raise OSError('private transport detail must not be copied')
            return original_request(method, route, *args, **kwargs)
        self.client.request = unavailable
        observation = self.w.observe()
        evidence = next(r for r in observation['outcomes'] if r['occurrenceId'] == oid)['executionEvidence']
        self.assertEqual(evidence['code'], 'OUTCOME_ARTIFACT_UNAVAILABLE')
        self.assertNotIn('private transport', json.dumps(observation))
        self.assertIn('muse_prefill_layer', observation['eligibleProblems'])

    def test_outcome_source_candidate_checker_and_attempt_mismatch_are_local_diagnostics(self):
        oid = self.complete_outcome_report(); self.add_seed()
        original = copy.deepcopy(self.client.occurrences[oid]); raw = copy.deepcopy(self.client.artifacts)
        for name, key in [('verification.json', 'sourceSHA256'), ('verification.json', 'candidateSHA256'),
                          ('cpu-result.json', 'candidateSHA256'), ('cpu-result.json', 'checkerSHA256')]:
            with self.subTest(name=name, key=key):
                self.client.occurrences[oid] = copy.deepcopy(original); self.client.artifacts = copy.deepcopy(raw)
                self.replace_outcome_json(oid, name, lambda value: value.update({key: 'f' * 64}))
                observation = self.w.observe(); evidence = next(r for r in observation['outcomes'] if r['occurrenceId'] == oid)['executionEvidence']
                self.assertEqual(evidence['bindingState'], 'invalid'); self.assertNotIn('decisionLocators', evidence)
                self.assertIn('muse_prefill_layer', observation['eligibleProblems'])
        self.client.occurrences[oid] = copy.deepcopy(original); self.client.artifacts = copy.deepcopy(raw)
        next(a for a in self.client.occurrences[oid]['artifacts'] if a['name'] == 'cpu-result.json')['attempt_id'] = uid()
        self.assertEqual(self.w.execution_evidence(self.client.occurrences[oid])['code'], 'OUTCOME_EVIDENCE_SET')

    def test_generated_hash_guidance_binds_source_bytes_not_task_input(self):
        control = copy.deepcopy(W.supervisor_definition(self.config))
        oid = self.complete_outcome_report(negative=False)
        detail = self.client.occurrences[oid]
        packet = detail['occurrence']['input']['experiment']
        base = next(m for m in packet['materials'] if m['name'] == 'base-candidate.py')
        raw = W.base64.b64decode(base['base64'], validate=True)
        source_hash = W.digest(raw)
        task_input_hash = W.digest(W.canonical(detail['occurrence']['input']))
        self.assertEqual(source_hash, base['sha256'])
        self.assertEqual(source_hash, packet['sourceArtifact']['digest'])
        self.assertEqual(raw, self.client.artifacts[packet['sourceArtifact']['id']])
        self.assertNotEqual(source_hash, task_input_hash)
        steps = detail['occurrence']['definition']['steps']
        self.assertEqual([s['key'] for s in steps], ['produce', 'independent-qa'])
        for step in steps:
            self.assertIn('cycleInputSHA256', step['instructions'])
            self.assertIn('input.experiment.sourceArtifact.digest', step['instructions'])
            self.assertTrue(step['readOnly'])
        self.assertIn('Independently rehash', steps[1]['instructions'])
        self.assertIn('Preserve every', steps[1]['instructions'])
        self.assertNotIn('cases [{name,passed}]', steps[1]['instructions'])
        self.assertIn('fresh isolated diagnostic state', steps[0]['instructions'])
        self.assertEqual(W.supervisor_definition(self.config), control)
        self.assertEqual(self.w.execution_evidence(detail)['bindingState'], 'verified')
        self.replace_outcome_json(oid, 'verification.json', lambda value: value.update(
            sourceSHA256=task_input_hash, cycleInputSHA256=task_input_hash))
        rejected = self.w.execution_evidence(detail)
        self.assertEqual(rejected['bindingState'], 'invalid')
        self.assertEqual(rejected['code'], 'OUTCOME_VERIFICATION_BINDING')
        self.assertEqual(rejected['authority'], 'no_report_content_admitted')
        self.assertNotIn('cpu', rejected)

    def test_corrupt_or_malformed_outcome_never_injects_unbound_text(self):
        oid = self.complete_outcome_report(); d = self.client.occurrences[oid]
        row = next(a for a in d['artifacts'] if a['name'] == 'verification.json')
        self.client.artifacts[row['id']] = b'{"hypothesisOutcome":"UNBOUND INSTRUCTION"}'
        evidence = self.w.execution_evidence(d)
        self.assertEqual(evidence['code'], 'ARTIFACT_CORRUPT')
        self.assertNotIn('UNBOUND', W.canonical(evidence).decode())
        for raw in [b'[]', b'{"passed":true,"passed":false}', b'{"value":NaN}', b'[' * 2000 + b']' * 2000]:
            self.client.artifacts[row['id']] = raw; row.update(digest=W.digest(raw), size=len(raw))
            self.assertEqual(self.w.execution_evidence(d)['bindingState'], 'invalid')

    def test_outcome_context_is_bounded_and_keeps_failure_counts(self):
        oid = self.complete_outcome_report(negative=False)
        self.replace_outcome_json(oid, 'verification.json', lambda value: value.update(decisionLocators=['x' * 1000] * 40))
        self.replace_outcome_json(oid, 'cpu-result.json', lambda value: value.update(
            passed=False, cases=[{'name': 'failed-' + str(i), 'passed': False} for i in range(50)]))
        evidence = self.w.execution_evidence(self.client.occurrences[oid])
        self.assertEqual(len(evidence['decisionLocators']), 4)
        self.assertEqual(max(map(len, evidence['decisionLocators'])), 240)
        self.assertEqual(evidence['cpu']['failedCaseCount'], 50)
        self.assertEqual(len(evidence['cpu']['failedCases']), 12)
        self.assertLess(len(W.canonical(evidence)), 3000)

    def test_authenticated_noop_rejection_isolates_only_candidate_and_never_repairs(self):
        status = self.repairable_operation()
        self.install_diagnostic_fixture([{**status, 'code': 'NO_CANDIDATE_CHANGE'}])
        self.assertEqual(self.w.observe()['autoCheckDiagnostics']['outcomes'][0]['classification'], 'new_hypothesis_required')
        self.install_diagnostic_fixture([{**status, 'code': 'NO_EXECUTABLE_CHANGE'}])
        observation = self.w.observe()
        self.assertEqual(observation['autoCheckDiagnostics']['outcomes'][0]['classification'], 'new_hypothesis_required')
        self.assertEqual(observation['autoCheckDiagnostics']['repairableEvidence'], [])
        self.assertEqual(observation['pendingOfficialCandidateSHA256'], [])
        self.assertEqual(self.w.tick()['state'], 'supervisor_dispatched')
        inp = self.w.state['supervisions'][-1]['input']
        self.assertFalse(any(a['digest'] == status['candidateSHA256'] for a in inp['catalog']))
        self.assertTrue(inp['catalog']); self.assertIn('muse_decode_layer', inp['eligibleProblems'])
        self.assertEqual(inp['isolatedCandidates'][0]['code'], 'NO_EXECUTABLE_CHANGE')

    def test_disabled_policy_noop_claim_does_not_isolate_candidate(self):
        status_row = self.repairable_operation()
        self.install_diagnostic_fixture([{**status_row, 'code': 'NO_EXECUTABLE_CHANGE'}])
        policy = W.read_json(self.config['autoCheckPolicyPath']); policy['enabled'] = False
        W.atomic(self.config['autoCheckPolicyPath'], policy)
        status = W.read_json(self.config['autoCheckStatusPath']); status['policySHA256'] = W.digest(policy)
        W.atomic(self.config['autoCheckStatusPath'], status)
        self.assertEqual(self.w.observe()['autoCheckDiagnostics']['outcomes'][0]['classification'], 'diagnostic_only')

    def test_unchanged_candidate_rejection_preserves_distinct_original_source_identity(self):
        status = self.repairable_operation(); oid = status['occurrenceId']
        detail = self.client.occurrences[oid]
        base = detail['occurrence']['input']['experiment']['sourceArtifact']
        row = next(a for a in detail['artifacts'] if a['name'] == 'candidate.py')
        source_raw = self.client.artifacts[base['id']]
        self.client.artifacts[row['id']] = source_raw; row.update(digest=base['digest'], size=len(source_raw))
        self.replace_outcome_json(oid, 'qa-verification.json', lambda value: value.update(candidateSHA256=base['digest']))
        qa = next(a for a in detail['artifacts'] if a['name'] == 'qa-verification.json')
        self.install_diagnostic_fixture([{**status, 'candidateSHA256': base['digest'],
            'qaSHA256': qa['digest'], 'code': 'NO_CANDIDATE_CHANGE'}])
        self.assertEqual(self.w.tick()['state'], 'supervisor_dispatched')
        review = self.w.state['supervisions'][-1]; inp = review['input']
        self.assertTrue(any(a['id'] == base['id'] for a in inp['catalog']))
        self.assertFalse(any(a['id'] == row['id'] for a in inp['catalog']))
        valid = self.plan(); valid['experiments'][0]['sourceArtifactId'] = base['id']
        W.validate_plan(valid, {'fingerprint': review['fingerprint'], **inp}, self.config)
        invalid = copy.deepcopy(valid); invalid['experiments'][0]['sourceArtifactId'] = row['id']
        with self.assertRaisesRegex(ValueError, 'SOURCE_SCOPE'):
            W.validate_plan(invalid, {'fingerprint': review['fingerprint'], **inp}, self.config)

    def test_protected_registry_rejects_writable_and_symlink_paths(self):
        path = self.root / 'unsafe.json'; path.write_bytes(b'{}'); path.chmod(0o666)
        with self.assertRaisesRegex(ValueError, 'PROTECTED_OWNER_MODE'): W.protected_bytes(path, 32)
        path.chmod(0o600); link = self.root / 'link.json'; link.symlink_to(path)
        with self.assertRaisesRegex(ValueError, 'PROTECTED_PATH'): W.protected_bytes(link, 32)

    def complete_repair_source(self, oid):
        detail = self.client.occurrences[oid]
        base = detail['occurrence']['input']['experiment']['sourceArtifact']
        original = self.client.artifacts[base['id']]
        candidate = b'# fixed candidate source\n'
        patch = ''.join(difflib.unified_diff(original.decode().splitlines(True), candidate.decode().splitlines(True),
                                            fromfile='base-candidate.py', tofile='candidate.py')).encode()
        self.client.complete(oid, {'candidate.py': candidate, 'implementation.patch': patch,
            'cpu-checks.py': b'# independently replay this checker\n', 'cpu-result.json': {'passed': True},
            'verification.json': {'passed': True, 'sourceSHA256': base['digest'], 'candidateSHA256': W.digest(candidate)},
            'qa-verification.json': {'passed': True, 'sourceSHA256': base['digest'], 'candidateSHA256': W.digest(candidate),
                'cpuReplayPassed': True, 'scopePreserved': True, 'approvedForOfficialReview': True,
                'gpuValidated': False, 'officialActionsPerformed': False}})
        detail = self.client.occurrences[oid]
        qa = next(a for a in detail['artifacts'] if a['name'] == 'qa-verification.json')
        defs = {s['key']: s for s in detail['occurrence']['definition']['steps']}
        detail['steps'][0].update(key='produce', definition=defs['produce'])
        producer = detail['attempts'][0]
        producer['context'] = {'manifest': {'cycleId': detail['occurrence']['cycle_id'],
            'occurrenceId': oid, 'step': 'produce', 'cycleInput': {
                'attemptId': producer['id'], 'occurrenceId': oid, 'runFence': producer['fence'], 'digest': 'b' * 64}}}
        sid, aid = uid(), uid()
        detail['steps'].append({'id': sid, 'key': 'independent-qa', 'attempt': 1, 'state': 'completed',
                                'definition': defs['independent-qa']})
        detail['attempts'].append({'id': aid, 'step_id': sid, 'number': 1, 'state': 'completed',
            'finished_at': '2026-11-01T18:00:00Z', 'fence': uid(), 'receipt': {'stopped': True}})
        attempt = detail['attempts'][-1]
        attempt['context'] = {'manifest': {'cycleId': detail['occurrence']['cycle_id'],
            'occurrenceId': oid, 'step': 'independent-qa', 'cycleInput': {
                'attemptId': aid, 'occurrenceId': oid, 'runFence': attempt['fence'], 'digest': 'a' * 64}}}
        qa['attempt_id'] = aid
        return {'occurrenceId': oid, 'problem': 'muse_decode_layer', 'candidateSHA256': W.digest(candidate),
                'qaArtifactId': qa['id'], 'qaSHA256': qa['digest'], 'state': 'rejected_repairable',
                'jobId': None, 'postAttempted': False, 'code': 'CPU_RESULT_BINDING'}

    def install_diagnostic_fixture(self, rows, observed=10000):
        self.config['autoCheckStatusPath'] = str(self.root / 'auto-check-status.json')
        self.config['autoCheckPolicyPath'] = str(self.root / 'auto-check-policy.json')
        config_path = self.root / 'watcher-config.json'; W.atomic(config_path, self.config)
        code_path = self.root / 'watcher-code.py'; code_path.write_bytes(SOURCE.read_bytes())
        policy = {'version': 1, 'enabled': True, 'authority': 'operator-delegated-diagnostic-check',
            'allowedActions': ['check'], 'automaticPostRetry': False, 'watchId': self.config['watchId'],
            'controlCycleId': self.config['controlCycleId'], 'workspace': self.config['workspace'],
            'model': self.config['model'], 'accountId': self.config['accountId'], 'boards': {'muse_decode_layer': {}},
            'expiresAt': 20000, 'watcherConfig': {'path': str(config_path), 'sha256': W.digest(config_path.read_bytes())},
            'watcherCode': {'path': str(code_path), 'sha256': W.digest(code_path.read_bytes())}}
        W.atomic(self.config['autoCheckPolicyPath'], policy)
        W.atomic(self.config['autoCheckStatusPath'], {'version': 1, 'watchId': self.config['watchId'],
            'observedAt': observed, 'mode': 'check', 'policySHA256': W.digest(policy), 'policyExpiresAt': 20000,
            'controllerState': 'active', 'outcomes': rows})

    def repairable_operation(self):
        self.w.tick(); self.complete_review(); self.w.tick()
        target = self.w.state['experiments'][-1]['occurrenceId']
        status = self.complete_repair_source(target); self.install_diagnostic_fixture([status])
        return status

    def submit_repair_plan(self):
        self.assertEqual(self.w.tick()['state'], 'supervisor_dispatched')
        review = self.w.state['supervisions'][-1]
        target = review['input']['autoCheckDiagnostics']['repairableEvidence'][0]
        plan = self.plan(); plan['decision'] = 'repair_evidence'
        plan['experiments'][0]['sourceArtifactId'] = target['candidateArtifactId']
        self.complete_review(plan)
        return self.w.tick()

    def test_rejected_evidence_dispatches_immutable_repair_then_independent_qa(self):
        status = self.repairable_operation()
        observation = self.w.observe()
        self.assertEqual(observation['pendingOfficialCandidateSHA256'], [])
        result = self.submit_repair_plan(); self.assertEqual(result['state'], 'experiments_dispatched')
        record = self.w.state['experiments'][-1]
        self.assertEqual(record['repairCandidateSHA256'], status['candidateSHA256'])
        detail = self.client.occurrences[record['occurrenceId']]
        packet = detail['occurrence']['input']['experiment']
        target = packet['repairTarget']
        self.assertEqual(target['occurrenceId'], status['occurrenceId'])
        self.assertEqual(target['candidateSHA256'], status['candidateSHA256'])
        original = self.client.occurrences[status['occurrenceId']]['occurrence']['input']['experiment']['sourceArtifact']
        self.assertEqual(packet['sourceArtifact']['digest'], original['digest'])
        self.assertEqual(len({a['attemptId'] for a in target['materials']}), 1)
        self.assertEqual([s['key'] for s in detail['occurrence']['definition']['steps']], ['produce', 'independent-qa'])
        self.assertEqual(detail['occurrence']['definition']['allowedActions'], [])

    def test_qa_cpu_result_cannot_replace_or_duplicate_producer_evidence(self):
        status = self.repairable_operation()
        detail = self.client.occurrences[status['occurrenceId']]
        qa = next(a for a in detail['artifacts'] if a['name'] == 'qa-verification.json')
        original = next(a for a in detail['artifacts'] if a['name'] == 'cpu-result.json')
        raw = W.canonical({'passed': True, 'independentQARecheck': True})
        extra = {**original, 'id': uid(), 'attempt_id': qa['attempt_id'], 'digest': W.digest(raw), 'size': len(raw)}
        detail['artifacts'].append(extra); self.client.artifacts[extra['id']] = raw
        result = self.w.observe()['autoCheckDiagnostics']['repairableEvidence'][0]
        selected = result['producerArtifacts']
        self.assertEqual(len(selected), 5)
        self.assertEqual(next(a['id'] for a in selected if a['name'] == 'cpu-result.json'), original['id'])
        self.assertNotIn(extra['id'], {a['id'] for a in selected})
        self.assertEqual(self.submit_repair_plan()['state'], 'experiments_dispatched')

    def test_producer_envelope_failures_are_local_and_cannot_borrow_qa_evidence(self):
        status = self.repairable_operation()
        self.add_seed('muse_prefill_layer')
        self.install_diagnostic_fixture([status])
        original = copy.deepcopy(self.client.occurrences[status['occurrenceId']])
        posts = len(self.client.posts)
        cases = ['missing', 'duplicate', 'wrong-role', 'definition', 'obsolete', 'duplicate-attempt',
                 'fence', 'manifest-attempt', 'manifest-cycle', 'manifest-step', 'stop']
        for mutation in cases:
            with self.subTest(mutation=mutation):
                detail = copy.deepcopy(original); self.client.occurrences[status['occurrenceId']] = detail
                producer = detail['attempts'][0]; qa = next(a for a in detail['artifacts'] if a['name'] == 'qa-verification.json')
                row = next(a for a in detail['artifacts'] if a['name'] == 'cpu-result.json')
                if mutation == 'missing': detail['artifacts'].remove(row)
                elif mutation == 'duplicate': detail['artifacts'].append({**row, 'id': uid()})
                elif mutation == 'wrong-role': row['attempt_id'] = qa['attempt_id']
                elif mutation == 'definition': detail['steps'][0]['definition']['kind'] = 'action'
                elif mutation == 'obsolete': detail['steps'][0]['attempt'] += 1
                elif mutation == 'duplicate-attempt': detail['attempts'].append({**producer, 'id': uid()})
                elif mutation == 'fence': producer['fence'] = uid()
                elif mutation == 'manifest-attempt': producer['context']['manifest']['cycleInput']['attemptId'] = uid()
                elif mutation == 'manifest-cycle': producer['context']['manifest']['cycleId'] = uid()
                elif mutation == 'manifest-step': producer['context']['manifest']['step'] = 'independent-qa'
                else: producer['receipt']['stopped'] = False
                observation = self.w.observe()
                diagnostics = observation['autoCheckDiagnostics']
                self.assertFalse(diagnostics['repairableEvidence'])
                self.assertEqual(diagnostics['outcomes'][0]['classification'], 'repair_context_unavailable')
                self.assertIn('muse_prefill_layer', observation['eligibleProblems'])
                self.assertEqual(len(self.client.posts), posts)

    def test_bad_producer_envelope_does_not_prevent_pending_other_board_plan_consumption(self):
        status = self.repairable_operation(); self.add_seed('muse_prefill_layer')
        self.install_diagnostic_fixture([status])
        self.assertEqual(self.w.tick()['state'], 'supervisor_dispatched')
        plan = self.plan(); plan['experiments'] = [dict(plan['experiments'][0], problem='muse_prefill_layer',
            sourceArtifactId=next(a['id'] for a in self.w.state['supervisions'][-1]['input']['catalog']
                                  if a['problem'] == 'muse_prefill_layer'))]
        self.complete_review(plan)
        detail = self.client.occurrences[status['occurrenceId']]
        detail['artifacts'].append(copy.deepcopy(next(a for a in detail['artifacts'] if a['name'] == 'cpu-result.json')))
        self.assertEqual(self.w.tick()['state'], 'experiments_dispatched')
        self.assertEqual(self.w.state['experiments'][-1]['problem'], 'muse_prefill_layer')
        self.assertNotIn('repairOf', self.w.state['experiments'][-1])

    def test_producer_null_receipt_uses_only_exact_fenced_native_stop_proof(self):
        status = self.repairable_operation()
        detail = self.client.occurrences[status['occurrenceId']]; producer = detail['attempts'][0]
        producer['receipt'] = None
        detail['events'] = [{'kind': 'HARNESS_STOPPED', 'attempt_id': producer['id'], 'payload': {
            'runFence': producer['fence'], 'confirmed': True, 'unit': 'agentos-run-' + producer['id'],
            'proof': {'stopped': True, 'mainPid': 0, 'populated': False, 'activeState': 'inactive'}}}]
        self.assertEqual(len(self.w.observe()['autoCheckDiagnostics']['repairableEvidence']), 1)
        detail['events'][0]['payload']['runFence'] = uid()
        diagnostics = self.w.observe()['autoCheckDiagnostics']
        self.assertEqual(diagnostics['repairableEvidence'], [])
        self.assertEqual(diagnostics['outcomes'][0]['repairContextCode'], 'REPAIR_BOUNDARY_UNCONFIRMED')

    def test_oversized_producer_cpu_report_is_local_without_weakening_limit(self):
        status = self.repairable_operation(); self.add_seed('muse_prefill_layer')
        self.install_diagnostic_fixture([status])
        detail = self.client.occurrences[status['occurrenceId']]
        row = next(a for a in detail['artifacts'] if a['name'] == 'cpu-result.json')
        raw = W.canonical({'passed': True, 'details': 'x' * 131072})
        self.client.artifacts[row['id']] = raw; row.update(digest=W.digest(raw), size=len(raw))
        observation = self.w.observe(); diagnostics = observation['autoCheckDiagnostics']
        self.assertEqual(diagnostics['repairableEvidence'], [])
        self.assertEqual(diagnostics['outcomes'][0]['repairContextCode'], 'REPAIR_CPU_LOGICAL_SIZE')
        self.assertIn('muse_prefill_layer', observation['eligibleProblems'])

    def replace_repair_cpu(self, status, raw):
        detail = self.client.occurrences[status['occurrenceId']]
        row = next(a for a in detail['artifacts'] if a['name'] == 'cpu-result.json')
        self.client.artifacts[row['id']] = raw; row.update(digest=W.digest(raw), size=len(raw))
        return row

    def test_formatting_heavy_repair_preserves_all_original_material_bytes(self):
        status = self.repairable_operation(); detail = self.client.occurrences[status['occurrenceId']]
        checker = next(a for a in detail['artifacts'] if a['name'] == 'cpu-checks.py')
        report = {'passed': True, 'candidateSHA256': status['candidateSHA256'], 'checkerSHA256': checker['digest'],
            'cases': [{'name': 'coverage', 'passed': True, 'problems': []}],
            'negativeControls': [{'name': 'mutant', 'detected': True, 'note': {'missing': 16}}],
            'details': {'complete': ['all values', True, 19, 0.125, None]}, 'limitations': ['CPU only.']}
        raw = json.dumps(report, indent=2).encode() + b' ' * 140000
        cpu = self.replace_repair_cpu(status, raw)
        before = copy.deepcopy(detail); original_artifacts = dict(self.client.artifacts)
        self.assertGreater(len(raw), 131072); self.assertLess(len(raw), 262144)
        with self.assertRaisesRegex(ValueError, 'ARTIFACT_SIZE'):
            self.w.verified_artifact(cpu, 131072)  # Generic and execution-evidence caps stay unchanged.
        outcome = self.w.execution_evidence(detail)
        self.assertEqual(outcome['code'], 'ARTIFACT_SIZE')
        self.assertEqual(len(self.w.observe()['autoCheckDiagnostics']['repairableEvidence']), 1)
        self.assertEqual(self.w.repair_cpu_report(cpu), report)
        self.assertEqual(self.submit_repair_plan()['state'], 'experiments_dispatched')
        new = self.client.occurrences[self.w.state['experiments'][-1]['occurrenceId']]['occurrence']
        target = new['input']['experiment']['repairTarget']
        for material in target['materials']:
            source = next(a for a in before['artifacts'] if a['id'] == material['artifactId'])
            self.assertEqual(W.base64.b64decode(material['base64']), original_artifacts[source['id']])
            self.assertEqual(material['sha256'], source['digest']); self.assertEqual(material['bytes'], source['size'])
        self.assertEqual(json.loads(W.base64.b64decode(next(m for m in target['materials'] if m['name'] == 'cpu-result.json')['base64'])), report)
        self.assertEqual(detail, before)
        self.assertFalse(new['input']['officialActionsAllowed'])
        self.assertFalse(new['input']['experiment']['repairQADiagnostic']['currentAcceptance'])
        producer, qa = new['definition']['steps']
        self.assertIn('Rehash the edited checker and new result', producer['instructions'])
        self.assertIn('Preserve every field, ordered array', producer['instructions'])
        self.assertIn('COMPLETE cases and negativeControls', qa['instructions'])
        self.assertIn('32768 bytes', qa['instructions'])

    def test_repair_cpu_raw_and_logical_limits_are_independent_and_inclusive(self):
        status = self.repairable_operation()
        logical = b'{"passed":true,"padding":"' + b'x' * (131072 - len(b'{"passed":true,"padding":""}')) + b'"}'
        row = self.replace_repair_cpu(status, logical + b' ' * (262144 - len(logical)))
        self.assertEqual(len(W.canonical(self.w.repair_cpu_report(row))), 131072)
        for raw, code in [(logical + b' ' * (262145 - len(logical)), 'ARTIFACT_SIZE'),
                          (logical.replace(b'"padding":"', b'"padding":"x'), 'REPAIR_CPU_LOGICAL_SIZE')]:
            row = self.replace_repair_cpu(status, raw)
            with self.assertRaisesRegex(ValueError, code): self.w.repair_cpu_report(row)

    def test_repair_cpu_rejects_duplicate_nonfinite_and_corrupt_bytes_locally(self):
        status = self.repairable_operation(); self.add_seed('muse_prefill_layer')
        self.install_diagnostic_fixture([status]); posts = len(self.client.posts)
        for raw, code in [(b'{"passed":false,"passed":true}', 'REPAIR_CPU_DUPLICATE_JSON_KEY'),
                (b'{"passed":true,"nested":{"x":1,"x":2}}', 'REPAIR_CPU_DUPLICATE_JSON_KEY'),
                *[(b'{"passed":true,"value":' + n + b'}', 'REPAIR_CPU_NONFINITE_JSON')
                  for n in [b'NaN', b'Infinity', b'-Infinity', b'1e999']]]:
            with self.subTest(raw=raw):
                self.replace_repair_cpu(status, raw)
                observation = self.w.observe(); diagnostics = observation['autoCheckDiagnostics']
                self.assertEqual(diagnostics['repairableEvidence'], [])
                self.assertEqual(diagnostics['outcomes'][0]['repairContextCode'], code)
                self.assertIn('muse_prefill_layer', observation['eligibleProblems'])
                self.assertEqual(len(self.client.posts), posts)
        for change in ['hash', 'size']:
            row = self.replace_repair_cpu(status, b'{"passed":true}')
            row.update({'digest': 'f' * 64} if change == 'hash' else {'size': row['size'] + 1})
            with self.assertRaisesRegex(ValueError, 'ARTIFACT_CORRUPT'): self.w.repair_cpu_report(row)
        self.assertEqual(self.w.tick()['state'], 'supervisor_dispatched')
        self.complete_review(self.plan())
        self.assertEqual(self.w.tick()['state'], 'experiments_dispatched')
        self.assertEqual(self.w.state['experiments'][-1]['problem'], 'muse_prefill_layer')

    def test_repair_cpu_cannot_hide_candidate_checker_or_qa_hash_conflict(self):
        status = self.repairable_operation(); self.add_seed('muse_prefill_layer')
        self.install_diagnostic_fixture([status])
        for field in ['candidateSHA256', 'checkerSHA256']:
            self.replace_repair_cpu(status, W.canonical({'passed': True, field: 'f' * 64}))
            observation = self.w.observe(); diagnostics = observation['autoCheckDiagnostics']
            self.assertFalse(diagnostics['repairableEvidence'])
            self.assertEqual(diagnostics['outcomes'][0]['repairContextCode'], 'REPAIR_EVIDENCE_HASH_CONFLICT')
            self.assertIn('muse_prefill_layer', observation['eligibleProblems'])
        self.replace_repair_cpu(status, b'{"passed":true}')
        qa = next(a for a in self.client.occurrences[status['occurrenceId']]['artifacts'] if a['name'] == 'qa-verification.json')
        original = json.loads(self.client.artifacts[qa['id']])
        for field in ['checkerSHA256', 'resultSHA256']:
            report = {**original, 'cpuEvidence': {field: 'f' * 64}}
            raw = W.canonical(report); self.client.artifacts[qa['id']] = raw
            qa.update(digest=W.digest(raw), size=len(raw)); status['qaSHA256'] = qa['digest']
            self.install_diagnostic_fixture([status])
            diagnostics = self.w.observe()['autoCheckDiagnostics']
            self.assertEqual(diagnostics['outcomes'][0]['repairContextCode'], 'REPAIR_EVIDENCE_HASH_CONFLICT')
            self.assertFalse(diagnostics['repairableEvidence'])

    def test_untrusted_native_json_shapes_are_local_and_other_board_continues(self):
        status = self.repairable_operation(); self.add_seed('muse_prefill_layer')
        original = copy.deepcopy(self.client.occurrences[status['occurrenceId']])
        artifacts = dict(self.client.artifacts)
        for target, value, code in [('qa', None, 'REPAIR_QA_REPORT_SCHEMA'), ('qa', [], 'REPAIR_QA_REPORT_SCHEMA'),
                ('cpu', None, 'REPAIR_CPU_REPORT_SCHEMA'), ('cpu', [], 'REPAIR_CPU_REPORT_SCHEMA'),
                ('evidence', [], 'REPAIR_CPU_EVIDENCE_SCHEMA'), ('evidence', 'invalid', 'REPAIR_CPU_EVIDENCE_SCHEMA'),
                ('evidence', None, 'REPAIR_CPU_EVIDENCE_SCHEMA')]:
            with self.subTest(target=target, value=value):
                detail = copy.deepcopy(original); self.client.occurrences[status['occurrenceId']] = detail
                self.client.artifacts = dict(artifacts)
                name = 'cpu-result.json' if target == 'cpu' else 'qa-verification.json'
                row = next(a for a in detail['artifacts'] if a['name'] == name)
                if target == 'evidence':
                    report = json.loads(self.client.artifacts[row['id']]); report['cpuEvidence'] = value
                else: report = value
                raw = W.canonical(report); self.client.artifacts[row['id']] = raw
                row.update(digest=W.digest(raw), size=len(raw))
                bound = {**status, **({'qaSHA256': row['digest']} if target != 'cpu' else {})}
                self.install_diagnostic_fixture([bound])
                observation = self.w.observe(); diagnostics = observation['autoCheckDiagnostics']
                self.assertEqual(diagnostics['repairableEvidence'], [])
                self.assertEqual(diagnostics['outcomes'][0]['repairContextCode'], code)
                self.assertIn('muse_prefill_layer', observation['eligibleProblems'])

    def test_repair_hash_guidance_keeps_original_base_and_immutable_candidate_distinct(self):
        status = self.repairable_operation()
        self.assertEqual(self.submit_repair_plan()['state'], 'experiments_dispatched')
        oid = self.w.state['experiments'][-1]['occurrenceId']
        occurrence = self.client.occurrences[oid]['occurrence']
        packet = occurrence['input']['experiment']
        base = next(m for m in packet['materials'] if m['name'] == 'base-candidate.py')
        target = next(m for m in packet['repairTarget']['materials'] if m['name'] == 'candidate.py')
        self.assertEqual(W.digest(W.base64.b64decode(base['base64'])), packet['sourceArtifact']['digest'])
        self.assertEqual(W.digest(W.base64.b64decode(target['base64'])), status['candidateSHA256'])
        self.assertNotEqual(base['sha256'], target['sha256'])
        for step in occurrence['definition']['steps']:
            self.assertIn('cycleInputSHA256', step['instructions'])
            criteria = [c for c in step['criteria'] if c['id'] == 'immutable-repair-candidate']
            self.assertEqual(len(criteria), 1)
            self.assertEqual(criteria[0]['equals'], target['sha256'])

    def repair_capacity_fixture(self):
        self.config['maxPendingCandidates'] = 3
        status = self.repairable_operation()
        self.add_seed(problem='muse_prefill_layer')
        self.add_gpu_job(problem='muse_attention_decode')
        self.install_diagnostic_fixture([status])
        self.assertEqual(self.submit_repair_plan()['state'], 'experiments_dispatched')
        record = self.w.state['experiments'][-1]
        return status, record, self.client.occurrences[record['occurrenceId']]

    def test_linked_queued_running_and_uncertain_repair_use_one_slot_and_release_other_board(self):
        status, record, detail = self.repair_capacity_fixture()
        queued = copy.deepcopy(detail)
        for state in ['queued', 'running', 'uncertain']:
            with self.subTest(state=state):
                detail.clear(); detail.update(copy.deepcopy(queued))
                if state != 'queued':
                    sid, aid = uid(), uid()
                    detail['occurrence']['state'] = 'running' if state == 'running' else 'attention'
                    detail['steps'] = [{'id': sid, 'key': 'produce', 'attempt': 1, 'state': 'running'}]
                    detail['attempts'] = [{'id': aid, 'step_id': sid, 'number': 1, 'fence': uid(),
                        'state': 'running' if state == 'running' else 'failed', 'receipt': {'stopped': False}}]
                observation = self.w.observe()
                self.assertEqual(observation['pipelineUsed'], 2)
                self.assertEqual(observation['availableCandidateSlots'], 1)
                self.assertEqual(observation['eligibleProblems'], ['muse_prefill_layer'])
                owners = [x for x in observation['pipelineOwners'] if x['problem'] == 'muse_decode_layer']
                self.assertEqual(len(owners), 1)
                self.assertEqual(owners[0]['occurrenceIds'], [record['occurrenceId']])
                self.assertEqual(owners[0]['candidateSHA256'], status['candidateSHA256'])
                self.assertNotIn(status['candidateSHA256'], observation['pendingOfficialCandidateSHA256'])
        self.assertEqual(self.w.tick()['state'], 'supervisor_dispatched')
        valid = self.plan(); self.assertEqual(valid['experiments'][0]['problem'], 'muse_prefill_layer')
        invalid = copy.deepcopy(valid)
        invalid['experiments'][0].update(problem='muse_decode_layer', sourceArtifactId=
            next(a['id'] for a in self.client.occurrences[status['occurrenceId']]['artifacts'] if a['name'] == 'candidate.py'))
        with self.assertRaisesRegex(ValueError, 'BOARD_PIPELINE_OWNED'):
            W.validate_plan(invalid, self.w.observe(), self.config)
        self.complete_review(valid)
        result = self.w.tick()
        self.assertEqual(result['state'], 'experiments_dispatched')
        self.assertEqual(result['dispatched'][0]['problem'], 'muse_prefill_layer')
        self.assertEqual(len([r for r in self.w.state['experiments'] if r.get('repairOf') == status['occurrenceId']]), 1)

    def test_stopped_failed_repair_does_not_revive_rejected_parent_and_wakes_supervisor(self):
        status, record, detail = self.repair_capacity_fixture()
        self.client.complete(record['occurrenceId'], {'candidate.py': b'# fixed candidate source\n'}, state='attention')
        observation = self.w.observe()
        self.assertEqual(observation['pipelineUsed'], 1)  # Independent official GPU job only.
        self.assertNotIn(status['candidateSHA256'], observation['pendingOfficialCandidateSHA256'])
        self.assertIn('muse_decode_layer', observation['eligibleProblems'])
        self.assertEqual(self.w.tick()['state'], 'supervisor_dispatched')
        outcome = next(x for x in self.w.state['supervisions'][-1]['input']['outcomes'] if x['occurrenceId'] == record['occurrenceId'])
        self.assertEqual(outcome['state'], 'attention')
        self.assertTrue(outcome['failures'])

    def test_completed_approved_repair_keeps_exactly_one_unmeasured_reservation(self):
        status, record, detail = self.repair_capacity_fixture()
        self.complete_repair_source(record['occurrenceId'])
        observation = self.w.observe()
        self.assertEqual(observation['pipelineUsed'], 2)
        owners = [x for x in observation['pipelineOwners'] if x['problem'] == 'muse_decode_layer']
        self.assertEqual(len(owners), 1)
        self.assertEqual(owners[0]['stages'], ['unmeasured'])
        self.assertEqual(owners[0]['occurrenceIds'], [record['occurrenceId']])
        self.assertEqual(observation['pendingOfficialCandidateSHA256'], [status['candidateSHA256']])
        self.assertNotIn('muse_decode_layer', observation['eligibleProblems'])
        self.assertIn('muse_prefill_layer', observation['eligibleProblems'])

    def test_wrong_repair_input_parent_hash_or_artifact_cannot_release_capacity(self):
        status, record, detail = self.repair_capacity_fixture()
        original = copy.deepcopy(detail['occurrence']['input'])
        for field in ['parent', 'candidate', 'base', 'artifact', 'patch']:
            with self.subTest(field=field):
                detail['occurrence']['input'] = copy.deepcopy(original)
                packet = detail['occurrence']['input']['experiment']; target = packet['repairTarget']
                if field == 'parent': target['occurrenceId'] = uid()
                elif field == 'candidate': target['candidateSHA256'] = 'f' * 64
                elif field == 'base': packet['sourceArtifact']['digest'] = 'f' * 64
                elif field == 'patch': target['canonicalPatch']['sha256'] = 'f' * 64
                else: next(a for a in target['materials'] if a['name'] == 'candidate.py')['artifactId'] = uid()
                observation = self.w.observe()
                self.assertEqual(observation['pipelineUsed'], 3)
                self.assertEqual(observation['availableCandidateSlots'], 0)
                self.assertEqual(observation['eligibleProblems'], [])
                self.assertIn(status['candidateSHA256'], observation['pendingOfficialCandidateSHA256'])

    def test_two_actual_repair_actors_are_not_hidden_by_equal_candidate_hash(self):
        status, record, detail = self.repair_capacity_fixture()
        other = self.client.make_occurrence(record['cycleId'], {'requestKey': 'second-real-repair',
            'input': copy.deepcopy(detail['occurrence']['input'])})
        self.w.state['experiments'].append({**record, 'occurrenceId': other['id']})
        observation = self.w.observe()
        self.assertEqual(observation['pipelineUsed'], 3)
        owners = [x for x in observation['pipelineOwners'] if x['problem'] == 'muse_decode_layer']
        self.assertEqual(len(owners), 2)
        self.assertEqual({x['occurrenceIds'][0] for x in owners}, {record['occurrenceId'], other['id']})
        self.assertEqual(observation['eligibleProblems'], [])

    def prior_qa_diagnostic_fixture(self):
        status = self.repairable_operation()
        detail = self.client.occurrences[status['occurrenceId']]
        qa = next(a for a in detail['artifacts'] if a['name'] == 'qa-verification.json')
        value = json.loads(self.client.artifacts[qa['id']])
        value.update(findings=[{'id': 'details-contamination', 'text': 'Mutants overwrite original-case diagnostics.'}],
            limitations=['CPU-only; GPU correctness and speed unmeasured.'],
            cpuEvidence={'cases': [{'name': 'coverage', 'passed': True, 'problems': []}],
                         'negativeControls': [{'name': 'mutant', 'detected': True, 'note': {'missing': 16}}]})
        raw = W.canonical(value); self.client.artifacts[qa['id']] = raw
        qa.update(digest=W.digest(raw), size=len(raw)); status['qaSHA256'] = qa['digest']
        self.install_diagnostic_fixture([status])
        latest = {a['id'] for a in detail['attempts']}
        return status, detail, qa, latest, value

    def test_repair_delivers_complete_prior_qa_as_separate_diagnostic_not_acceptance(self):
        status, original, qa, _, report = self.prior_qa_diagnostic_fixture()
        before = copy.deepcopy(original)
        self.assertEqual(self.submit_repair_plan()['state'], 'experiments_dispatched')
        review = self.w.state['supervisions'][-1]['input']
        self.assertNotIn('repairQADiagnostic', review['autoCheckDiagnostics']['repairableEvidence'][0])
        detail = self.client.occurrences[self.w.state['experiments'][-1]['occurrenceId']]
        packet = detail['occurrence']['input']['experiment']; diagnostic = packet['repairQADiagnostic']
        self.assertEqual(diagnostic['report'], report)
        self.assertEqual(diagnostic['artifact'], {k: qa[k] for k in ['id', 'name', 'digest', 'size']})
        self.assertEqual(diagnostic['candidateSHA256'], status['candidateSHA256'])
        self.assertEqual(diagnostic['attemptId'], qa['attempt_id'])
        attempt = next(a for a in original['attempts'] if a['id'] == qa['attempt_id'])
        self.assertEqual(diagnostic['runFence'], attempt['fence'])
        self.assertEqual(diagnostic['authority'], 'prior_qa_diagnostic_only_not_current_acceptance')
        self.assertIs(diagnostic['currentAcceptance'], False)
        self.assertEqual({a['name'] for a in packet['materials']},
                         {'base-candidate.py', 'canonical-TASK.md', 'canonical-AGENTS.md'})
        self.assertEqual(len(packet['repairTarget']['materials']), 5)
        self.assertNotIn('repairQADiagnostic', packet['repairTarget'])
        self.assertEqual(original, before)
        for step in detail['occurrence']['definition']['steps']:
            self.assertIn('repairQADiagnostic', step['instructions'])
            self.assertIn('sha256sum ABS_CHECKER ABS_CANDIDATE && python3 ABS_CHECKER', step['instructions'])
        self.assertEqual([s['key'] for s in detail['occurrence']['definition']['steps']], ['produce', 'independent-qa'])
        self.assertEqual(detail['occurrence']['definition']['allowedActions'], [])

    def test_prior_qa_diagnostic_rejects_artifact_hash_and_size_before_dispatch(self):
        status, detail, qa, latest, _ = self.prior_qa_diagnostic_fixture()
        for field, value, code in [('digest', 'f' * 64, 'ARTIFACT_CORRUPT'),
                                   ('size', 32769, 'REPAIR_QA_DIAGNOSTIC_SIZE')]:
            row = {**qa, field: value}; posts = len(self.client.posts)
            with self.assertRaisesRegex(ValueError, code):
                self.w.repair_qa_diagnostic(detail, row, status['problem'], latest, status['candidateSHA256'])
            self.assertEqual(len(self.client.posts), posts)

    def test_prior_qa_diagnostic_rejects_candidate_attempt_and_fence_substitution(self):
        status, detail, qa, latest, _ = self.prior_qa_diagnostic_fixture()
        for mutation, code in [('candidate', 'REPAIR_QA_CANDIDATE_BINDING'),
                               ('attempt', 'QA_CONTEXT_ATTEMPT'), ('fence', 'REPAIR_QA_MANIFEST_BINDING'),
                               ('manifest-attempt', 'REPAIR_QA_MANIFEST_BINDING'),
                               ('manifest-occurrence', 'REPAIR_QA_MANIFEST_BINDING'),
                               ('manifest-step', 'REPAIR_QA_MANIFEST_BINDING')]:
            with self.subTest(mutation=mutation):
                d, row, wanted = copy.deepcopy(detail), dict(qa), status['candidateSHA256']
                a = next(a for a in d['attempts'] if a['id'] == row['attempt_id'])
                m = a['context']['manifest']
                if mutation == 'candidate': wanted = 'f' * 64
                elif mutation == 'attempt': row['attempt_id'] = uid()
                elif mutation == 'fence': a['fence'] = uid()
                elif mutation == 'manifest-attempt': m['cycleInput']['attemptId'] = uid()
                elif mutation == 'manifest-occurrence': m['occurrenceId'] = uid()
                else: m['step'] = 'produce'
                with self.assertRaisesRegex(ValueError, code):
                    self.w.repair_qa_diagnostic(d, row, status['problem'], latest, wanted)

    def test_prior_qa_diagnostic_cannot_promote_unstopped_or_obsolete_qa_attempt(self):
        status, detail, qa, latest, _ = self.prior_qa_diagnostic_fixture()
        for mutation, code in [('stop', 'QA_CONTEXT_STOP_UNCONFIRMED'), ('obsolete', 'QA_CONTEXT_STEP_BINDING')]:
            d = copy.deepcopy(detail); a = next(a for a in d['attempts'] if a['id'] == qa['attempt_id'])
            if mutation == 'stop': a['receipt']['stopped'] = False
            else: next(s for s in d['steps'] if s['id'] == a['step_id'])['attempt'] += 1
            with self.assertRaisesRegex(ValueError, code):
                self.w.repair_qa_diagnostic(d, qa, status['problem'], latest, status['candidateSHA256'])

    def test_invalid_prior_qa_manifest_is_local_and_another_board_keeps_dispatching(self):
        status, detail, qa, _, _ = self.prior_qa_diagnostic_fixture()
        self.add_seed('muse_prefill_layer')
        attempt = next(a for a in detail['attempts'] if a['id'] == qa['attempt_id'])
        attempt['context']['manifest']['cycleInput']['runFence'] = uid()
        self.install_diagnostic_fixture([status])
        observation = self.w.observe()
        diagnostics = observation['autoCheckDiagnostics']
        self.assertEqual(diagnostics['repairableEvidence'], [])
        row = next(r for r in diagnostics['outcomes'] if r['occurrenceId'] == status['occurrenceId'])
        self.assertEqual(row['classification'], 'repair_context_unavailable')
        self.assertEqual(row['repairContextCode'], 'REPAIR_QA_MANIFEST_BINDING')
        self.assertEqual(self.w.tick()['state'], 'supervisor_dispatched')
        inp = self.w.state['supervisions'][-1]['input']
        self.assertIn('muse_prefill_layer', inp['eligibleProblems'])
        self.assertNotIn('repairQADiagnostic', json.dumps(inp))
        self.assertNotIn('"missing": 16', json.dumps(inp))  # Full old CPU control report stays withheld.
        plan = self.plan(); self.assertEqual(plan['experiments'][0]['problem'], 'muse_prefill_layer')
        self.complete_review(plan)
        self.assertEqual(self.w.tick()['state'], 'experiments_dispatched')
        self.assertEqual(self.w.state['experiments'][-1]['problem'], 'muse_prefill_layer')
        self.assertNotIn('repairOf', self.w.state['experiments'][-1])

    def test_existing_native_definition_and_input_are_reconciled_without_reformulation(self):
        legacy = W.definition(self.config, 'Already dispatched experiment', [
            W.native_step(self.config, 'produce', 'Legacy producer', 'Persisted earlier contract.', ['candidate.py'])])
        create_body = {'requestKey': 'legacy-create-key', 'definition': legacy}
        self.client.timeout_after_commit_number = 1
        with self.assertRaises(TimeoutError): self.w.post_once('create', 'legacy-create-key', create_body)
        created = self.w.reconcile_existing_post('create', 'legacy-create-key')
        self.assertEqual(created['definition'], legacy)
        run_body = {'requestKey': 'legacy-run-key', 'input': {'version': 1, 'immutableLegacyContext': 'exact bytes'}}
        self.client.timeout_after_commit_number = 2
        with self.assertRaises(TimeoutError): self.w.post_once('run', 'legacy-run-key', run_body, created['id'])
        recovered = self.w.reconcile_existing_post('run', 'legacy-run-key', created['id'])
        self.assertEqual(recovered['input'], run_body['input'])
        self.assertEqual(recovered['definition'], legacy)
        altered = copy.deepcopy(create_body)
        altered['definition']['steps'][0]['instructions'] += ' New hash guidance.'
        with self.assertRaisesRegex(ValueError, 'POST_IDENTITY_CHANGED'):
            self.w.post_once('create', 'legacy-create-key', altered)
        self.assertEqual(len(self.client.posts), 2)

    def test_two_failed_repairs_isolate_candidate_without_blocking_other_work(self):
        status = self.repairable_operation(); rows = [status]
        for _ in range(2):
            self.submit_repair_plan()
            row = self.complete_repair_source(self.w.state['experiments'][-1]['occurrenceId'])
            rows.append(row); self.install_diagnostic_fixture(rows)
        result = self.w.tick()
        self.assertEqual(result['state'], 'supervisor_dispatched')
        review = self.w.state['supervisions'][-1]
        self.assertTrue(review['input']['isolatedCandidates'])
        self.assertFalse(any(a['digest'] == status['candidateSHA256'] for a in review['input']['catalog']))
        invalid = self.plan(); invalid['decision'] = 'repair_evidence'
        with self.assertRaisesRegex(ValueError, 'PLAN_CAPACITY'):
            W.validate_plan(invalid, {'fingerprint': review['fingerprint'], **review['input']}, self.config)
        self.complete_review(self.plan())
        self.assertEqual(self.w.tick()['state'], 'experiments_dispatched')
        self.assertEqual(len([r for r in self.w.state['experiments'] if r.get('repairOf')]), 2)

    def test_diagnostic_hash_or_source_lineage_conflict_cannot_authorize_repair(self):
        status = self.repairable_operation()
        self.install_diagnostic_fixture([{**status, 'candidateSHA256': 'f' * 64}])
        row = self.w.observe()['autoCheckDiagnostics']['outcomes'][0]
        self.assertEqual((row['classification'], row['repairContextCode']),
                         ('repair_context_unavailable', 'REPAIR_ARTIFACT_BINDING'))
        self.install_diagnostic_fixture([status])
        detail = self.client.occurrences[status['occurrenceId']]
        qa = next(a for a in detail['artifacts'] if a['id'] == status['qaArtifactId'])
        value = json.loads(self.client.artifacts[qa['id']]); value['sourceSHA256'] = 'f' * 64
        raw = W.canonical(value); self.client.artifacts[qa['id']] = raw; qa.update(digest=W.digest(raw), size=len(raw))
        self.install_diagnostic_fixture([{**status, 'qaSHA256': qa['digest']}])
        row = self.w.observe()['autoCheckDiagnostics']['outcomes'][0]
        self.assertEqual((row['classification'], row['repairContextCode']),
                         ('repair_context_unavailable', 'REPAIR_SOURCE_LINEAGE'))

    def test_controller_quota_uncertain_and_stale_diagnostics_never_authorize_repair(self):
        status = self.repairable_operation()
        for state in ['controller_blocked', 'observation_wait', 'quota_wait', 'uncertain_owned', 'paused']:
            self.install_diagnostic_fixture([{**status, 'state': state}])
            observation = self.w.observe()
            self.assertEqual(observation['autoCheckDiagnostics']['repairableEvidence'], [])
            self.assertIn(status['candidateSHA256'], observation['pendingOfficialCandidateSHA256'])
        self.install_diagnostic_fixture([status], observed=9000)
        self.assertEqual(self.w.observe()['autoCheckDiagnostics']['controllerState'], 'observation_stale')

    def test_disabled_or_nonactive_inspection_policy_is_diagnostic_only(self):
        status_row = self.repairable_operation()
        policy = W.read_json(self.config['autoCheckPolicyPath']); policy['enabled'] = False
        W.atomic(self.config['autoCheckPolicyPath'], policy)
        status = W.read_json(self.config['autoCheckStatusPath']); status['policySHA256'] = W.digest(policy)
        W.atomic(self.config['autoCheckStatusPath'], status)
        observation = self.w.observe()
        self.assertFalse(observation['autoCheckDiagnostics']['repairAuthority'])
        self.assertEqual(observation['autoCheckDiagnostics']['repairableEvidence'], [])
        self.assertIn(status_row['candidateSHA256'], observation['pendingOfficialCandidateSHA256'])
        policy['enabled'] = True; W.atomic(self.config['autoCheckPolicyPath'], policy)
        status.update(policySHA256=W.digest(policy), controllerState='inspection_only_policy_disabled')
        W.atomic(self.config['autoCheckStatusPath'], status)
        self.assertFalse(self.w.observe()['autoCheckDiagnostics']['repairAuthority'])

    def test_semantic_failure_requires_new_hypothesis_not_evidence_repair(self):
        status = self.repairable_operation(); detail = self.client.occurrences[status['occurrenceId']]
        qa = next(a for a in detail['artifacts'] if a['id'] == status['qaArtifactId'])
        value = json.loads(self.client.artifacts[qa['id']]); value['canonicalPrecisionPreserved'] = False
        raw = W.canonical(value); self.client.artifacts[qa['id']] = raw; qa.update(digest=W.digest(raw), size=len(raw))
        self.install_diagnostic_fixture([{**status, 'qaSHA256': qa['digest'], 'code': 'QA_ATTESTATION_MISSING_canonicalPrecisionPreserved'}])
        diagnostic = self.w.observe()['autoCheckDiagnostics']
        self.assertEqual(diagnostic['repairableEvidence'], [])
        self.assertEqual(diagnostic['outcomes'][0]['classification'], 'new_hypothesis_required')

    def test_material_age_does_not_reset_when_a_supervisor_is_dispatched(self):
        detail = self.client.occurrences[self.seed]
        detail['occurrence'].update(state='running', created_at='1970-01-01T02:30:00Z')
        detail['attempts'][0].update(state='running', finished_at=None)
        before = self.w.observe()['idleSeconds']; self.w.tick()
        self.assertEqual(self.w.observe()['idleSeconds'], before)
        self.assertEqual(before, 1000)
        self.assertEqual(self.w.observe()['controllerDispatchAgeSeconds'], 0)

    def test_header_normalization_preserves_original_hunks_and_exact_candidate(self):
        base, candidate = b'old\nkept\n', b'new\nkept\n'
        patch = ''.join(difflib.unified_diff(base.decode().splitlines(True), candidate.decode().splitlines(True),
            fromfile='a/kernels/muse_decode_layer/submission.py', tofile='b/kernels/muse_decode_layer/submission.py')).encode()
        canonical, proof = W.normalized_repair_patch(base, candidate, patch, 'muse_decode_layer')
        self.assertEqual(canonical.splitlines(True)[2:], patch.splitlines(True)[2:])
        self.assertEqual(canonical.splitlines(True)[:2], [b'--- base-candidate.py\n', b'+++ candidate.py\n'])
        self.assertEqual(proof['originalPatchSHA256'], W.digest(patch))
        self.assertEqual(proof['candidateSHA256'], W.digest(candidate))
        self.assertTrue(proof['appliedExactly'])
        for bad in [patch.replace(b'muse_decode_layer', b'muse_prefill_layer'), patch[:-len(b' kept\n')],
                    patch + b'--- other\n+++ stolen\n@@ -1 +1 @@\n-old\n+new\n']:
            with self.assertRaisesRegex(ValueError, 'REPAIR_PATCH_'):
                W.normalized_repair_patch(base, candidate, bad, 'muse_decode_layer')
        with self.assertRaisesRegex(ValueError, 'REPAIR_PATCH_CANDIDATE_MISMATCH'):
            W.normalized_repair_patch(base, b'wrong\n', patch, 'muse_decode_layer')

    def test_actual_next_dispatch_requires_updated_control_definition_and_version(self):
        cycle = self.client.cycles[self.config['controlCycleId']]
        cycle.update(version=1, definition={**W.supervisor_definition(self.config), 'steps': [
            {**W.supervisor_definition(self.config)['steps'][0], 'instructions': 'Old supervisor contract'}]})
        old = self.client.make_occurrence(cycle['id'], {'requestKey': 'old-snapshot', 'input': {}})
        self.config['controlVersion'] = 2
        with self.assertRaisesRegex(ValueError, 'CONTROL_CHANGED'): self.w.tick()
        self.assertEqual(self.client.posts, [])
        cycle.update(version=2, definition=W.supervisor_definition(self.config))
        self.assertEqual(self.w.tick()['state'], 'supervisor_dispatched')
        new = self.client.occurrences[self.w.state['supervisions'][-1]['occurrenceId']]['occurrence']
        self.assertIn('repair_evidence', new['definition']['steps'][0]['instructions'])
        self.assertEqual(old['definition']['steps'][0]['instructions'], 'Old supervisor contract')

    def test_status_policy_or_config_hash_conflict_cannot_dispatch_repair(self):
        self.repairable_operation()
        status = W.read_json(self.config['autoCheckStatusPath']); status['policySHA256'] = 'f' * 64
        W.atomic(self.config['autoCheckStatusPath'], status)
        with self.assertRaisesRegex(ValueError, 'AUTO_CHECK_POLICY_BINDING'): self.w.observe()

    def test_repaired_candidate_or_patch_change_is_not_accepted_as_evidence_repair(self):
        self.repairable_operation(); self.submit_repair_plan()
        oid = self.w.state['experiments'][-1]['occurrenceId']
        self.complete_repair_source(oid)
        candidate = next(a for a in self.client.occurrences[oid]['artifacts'] if a['name'] == 'candidate.py')
        candidate['digest'] = 'f' * 64
        with self.assertRaisesRegex(ValueError, 'REPAIR_IMMUTABLE_ARTIFACT_CHANGED'): self.w.observe()

    def test_recurrent_supervisor_dispatch_completion_new_supervisor(self):
        self.add_seed()
        self.assertEqual(self.w.tick()['state'], 'supervisor_dispatched')
        self.complete_review(self.plan(2))
        result = self.w.tick()
        self.assertEqual(result['state'], 'experiments_dispatched')
        self.assertEqual(len(result['dispatched']), 2)
        for row in result['dispatched']:
            definition = self.client.cycles[row['cycleId']]['definition']
            self.assertEqual(definition['allowedActions'], [])
            self.assertEqual([s['readOnly'] for s in definition['steps']], [True, True])
            self.assertEqual(definition['steps'][1]['dependsOn'], ['produce'])
            self.client.complete(row['occurrenceId'], {'candidate.py': ('# new ' + row['experimentId']).encode(),
                'qa-verification.json': {'passed': False, 'gpuValidated': False}})
        self.reload()
        self.assertEqual(self.w.tick()['state'], 'supervisor_dispatched')
        self.assertEqual(len(self.w.state['supervisions']), 2)

    def test_restart_during_partial_dispatch_reconciles_without_duplicate(self):
        self.add_seed()
        self.w.tick(); self.complete_review(self.plan(2))
        self.client.timeout_after_commit_number = 5  # supervisor + first(create/run) + second(create/run)
        with self.assertRaises(TimeoutError): self.w.tick()
        self.assertEqual(self.w.state['supervisions'][-1]['state'], 'dispatching')
        self.reload()
        self.assertEqual(self.w.tick()['state'], 'experiments_dispatched')
        self.assertEqual(len(self.client.posts), 5)
        self.assertEqual(len(self.w.state['experiments']), 2)

    def test_prefill_gpu_does_not_block_decode_cpu_or_duplicate_prefill(self):
        self.add_seed(); self.add_gpu_job()
        observation = self.w.observe()
        self.assertEqual(observation['pipelineUsed'], 1)
        self.assertEqual(observation['availableCandidateSlots'], 1)
        self.assertEqual(observation['eligibleProblems'], ['muse_decode_layer'])
        self.assertEqual(self.w.tick()['state'], 'supervisor_dispatched')
        bad = self.plan(); source = next(r for r in observation['catalog'] if r['problem'] == 'muse_prefill_layer')
        bad['experiments'][0].update(problem='muse_prefill_layer', sourceArtifactId=source['id'])
        with self.assertRaisesRegex(ValueError, 'BOARD_PIPELINE_OWNED'):
            W.validate_plan(bad, observation, self.config)
        self.complete_review(); result = self.w.tick()
        self.assertEqual(result['state'], 'experiments_dispatched')
        self.assertEqual([r['problem'] for r in result['dispatched']], ['muse_decode_layer'])
        self.assertEqual(self.w.observe()['pipelineUsed'], 2)
        self.assertEqual(self.w.observe()['availableCandidateSlots'], 0)
        self.assertEqual(self.w.tick()['state'], 'work_active')

    def test_cpu_qa_and_gpu_share_total_capacity(self):
        prefill = self.add_seed(); self.add_seed('muse_attention_decode', b'# attention source\n')
        detail = self.client.occurrences[prefill]
        detail['occurrence']['state'] = 'running'
        detail['attempts'][0].update(state='running', receipt=None)
        self.add_gpu_job(problem='muse_attention_decode')
        observation = self.w.observe()
        self.assertEqual(observation['pipelineUsed'], 2)
        self.assertEqual(observation['availableCandidateSlots'], 0)
        self.assertEqual(observation['eligibleProblems'], [])
        self.assertNotIn(detail['artifacts'][0]['id'], observation['confirmedSourceArtifactIds'])
        self.assertEqual(self.w.tick()['state'], 'work_active')

    def test_live_other_board_allows_one_terminal_source_experiment(self):
        prefill = self.add_seed(); d = self.client.occurrences[prefill]
        d['occurrence']['state'] = 'running'; d['attempts'][0].update(state='running', receipt=None)
        self.assertEqual(self.w.tick()['state'], 'supervisor_dispatched')
        self.assertFalse(self.w.state['supervisions'][-1]['input']['diagnosticOnly'])
        self.complete_review(); result = self.w.tick()
        self.assertEqual(result['dispatched'][0]['problem'], 'muse_decode_layer')
        self.assertEqual(d['occurrence']['state'], 'running')

    def test_same_board_two_experiments_rejected(self):
        self.w.tick(); plan = self.plan(2)
        with self.assertRaisesRegex(ValueError, 'PLAN_DUPLICATE_BOARD'):
            W.validate_plan(plan, self.w.observe(), self.config)

    def parallel_capacity(self):
        before = W.digest(W.supervisor_definition(self.config))
        self.config.update(pipelineCapacityVersion=2, maxPendingCandidates=12, maxPipelinesPerProblem=4)
        W.validate_config(self.config)
        self.assertEqual(W.digest(W.supervisor_definition(self.config)), before)

    def parallel_plan(self, counts):
        review = self.w.state['supervisions'][-1]
        plan = self.plan(); plan['experiments'] = []
        for problem, count in counts.items():
            source = next(a for a in review['input']['catalog'] if a['problem'] == problem)
            for index in range(count):
                plan['experiments'].append({'id': 'parallel-' + str(len(plan['experiments'])),
                    'problem': problem, 'sourceArtifactId': source['id'],
                    'hypothesis': 'Independent bounded routing strategy number ' + str(index) + ' may change launch overhead.',
                    'instructions': 'Implement the stated isolated strategy only, with a discriminating CPU falsifier and negative control.'})
        return plan

    def test_parallel_optin_twelve_four_preserves_control_and_legacy_bounds(self):
        original = copy.deepcopy(self.config); self.parallel_capacity()
        for changes, code in [({'maxPendingCandidates': 25}, 'CANDIDATE_CAPACITY'),
                              ({'maxPipelinesPerProblem': 9}, 'BOARD_PIPELINE_CAPACITY'),
                              ({'maxPendingCandidates': True}, 'CANDIDATE_CAPACITY'),
                              ({'pipelineCapacityVersion': 3}, 'PIPELINE_CAPACITY_VERSION')]:
            with self.subTest(changes=changes), self.assertRaisesRegex(ValueError, code):
                W.validate_config({**self.config, **changes})
        with self.assertRaisesRegex(ValueError, 'CANDIDATE_CAPACITY'):
            W.validate_config({**original, 'maxPendingCandidates': 12})

    def test_parallel_twelve_distinct_runs_are_isolated_and_hard_bounded(self):
        self.parallel_capacity(); self.add_seed(); self.add_seed('muse_attention_decode', b'# attention\n')
        self.w.tick(); plan = self.parallel_plan({p: 4 for p in W.PROBLEMS})
        self.complete_review(plan); result = self.w.tick()
        self.assertEqual(result['state'], 'experiments_dispatched')
        self.assertEqual(len(result['dispatched']), 12)
        self.assertEqual(len({r['cycleId'] for r in result['dispatched']}), 12)
        self.assertEqual(len({r['occurrenceId'] for r in result['dispatched']}), 12)
        for record in result['dispatched']:
            detail = self.client.occurrences[record['occurrenceId']]
            steps = detail['occurrence']['definition']['steps']
            self.assertEqual([s['readOnly'] for s in steps], [True, True])
            self.assertEqual(steps[1]['dependsOn'], ['produce'])
            self.assertEqual(detail['occurrence']['definition']['allowedActions'], [])
        observed = self.w.observe()
        self.assertEqual(observed['pipelineUsed'], 12)
        self.assertEqual(observed['nativePipelineUsed'], 12)
        self.assertEqual(observed['waitingPipelineUsed'], 0)
        self.assertEqual(observed['boardAvailableCandidateSlots'], {p: 0 for p in W.PROBLEMS})
        self.assertEqual(observed['availableCandidateSlots'], 0)
        self.assertEqual(self.w.tick()['state'], 'work_active')
        self.assertEqual(len(self.client.posts), 25)

    def test_parallel_fifth_same_board_and_thirteenth_total_rejected(self):
        self.parallel_capacity(); self.add_seed(); self.add_seed('muse_attention_decode', b'# attention\n'); self.w.tick()
        with self.assertRaisesRegex(ValueError, 'PLAN_BOARD_CAPACITY'):
            W.validate_plan(self.parallel_plan({'muse_decode_layer': 5}), self.w.observe(), self.config)
        with self.assertRaisesRegex(ValueError, 'PLAN_CAPACITY'):
            W.validate_plan(self.parallel_plan({'muse_decode_layer': 5, 'muse_prefill_layer': 4, 'muse_attention_decode': 4}),
                            self.w.observe(), self.config)

    def test_parallel_twenty_four_queued_fronts_eight_per_board_preserve_hard_ownership(self):
        self.parallel_capacity(); self.config.update(maxPendingCandidates=24, maxPipelinesPerProblem=8)
        W.validate_config(self.config)
        self.add_seed(); self.add_seed('muse_attention_decode', b'# attention\n')
        self.w.tick(); inp = self.w.state['supervisions'][-1]['input']
        self.assertIn('up to 8 independent hypotheses', inp['interpretationRules'][-1])
        self.assertIn('24 committed pipelines', inp['interpretationRules'][-1])
        self.assertIn('not a promise', inp['interpretationRules'][-1])
        self.complete_review(self.parallel_plan({p: 8 for p in W.PROBLEMS}))
        result = self.w.tick()
        self.assertEqual(result['state'], 'experiments_dispatched')
        self.assertEqual(len({r['occurrenceId'] for r in result['dispatched']}), 24)
        self.assertEqual(len({r['cycleId'] for r in result['dispatched']}), 24)
        self.assertTrue(all(self.client.occurrences[r['occurrenceId']]['occurrence']['state'] == 'queued'
                            for r in result['dispatched']))
        observed = self.w.observe()
        self.assertEqual(observed['pipelineUsed'], 24)
        self.assertEqual(observed['pipelineUsedByProblem'], {p: 8 for p in W.PROBLEMS})
        self.assertEqual(observed['availableCandidateSlots'], 0)
        self.assertEqual(self.w.tick()['state'], 'work_active')
        self.assertEqual(len(self.client.posts), 49)

    def test_parallel_ninth_per_board_and_twenty_fifth_total_rejected(self):
        self.parallel_capacity(); self.config.update(maxPendingCandidates=24, maxPipelinesPerProblem=8)
        self.add_seed(); self.add_seed('muse_attention_decode', b'# attention\n'); self.w.tick()
        with self.assertRaisesRegex(ValueError, 'PLAN_BOARD_CAPACITY'):
            W.validate_plan(self.parallel_plan({'muse_decode_layer': 9}), self.w.observe(), self.config)
        with self.assertRaisesRegex(ValueError, 'PLAN_CAPACITY'):
            W.validate_plan(self.parallel_plan({'muse_decode_layer': 9, 'muse_prefill_layer': 8, 'muse_attention_decode': 8}),
                            self.w.observe(), self.config)

    def test_parallel_normalized_duplicate_rejected_despite_renaming(self):
        self.parallel_capacity(); self.w.tick(); plan = self.parallel_plan({'muse_decode_layer': 2})
        plan['experiments'][1]['hypothesis'] = '  ' + plan['experiments'][0]['hypothesis'].upper().replace(' ', '  ') + ' '
        plan['experiments'][1]['instructions'] += ' Different instruction wording cannot create a new hypothesis.'
        with self.assertRaisesRegex(ValueError, 'PLAN_DUPLICATE_HYPOTHESIS'):
            W.validate_plan(plan, self.w.observe(), self.config)
        self.assertEqual(len(self.client.posts), 1)

    def test_parallel_owned_hypothesis_remains_reserved_native_and_gpu_wait(self):
        self.parallel_capacity(); self.w.tick(); plan = self.parallel_plan({'muse_decode_layer': 1})
        self.complete_review(plan); self.w.tick(); record = self.w.state['experiments'][-1]
        for stage in ['native', 'gpu']:
            if stage == 'gpu':
                candidate = b'# measured later\n'
                self.client.complete(record['occurrenceId'], {'candidate.py': candidate, 'qa-verification.json': {
                    'passed': True, 'candidateSHA256': W.digest(candidate), 'approvedForOfficialReview': True, 'gpuValidated': False}})
                self.add_gpu_job('muse_decode_layer', W.digest(candidate), 'queued')
            observed = self.w.observe()
            self.assertEqual(observed['pipelineUsed'], 1)
            self.assertEqual(observed['boardAvailableCandidateSlots']['muse_decode_layer'], 3)
            self.assertEqual(len(observed['hypothesisReservations']), 1)
            duplicate = copy.deepcopy(plan); duplicate['evidenceFingerprint'] = observed['fingerprint']
            duplicate['experiments'][0]['id'] = 'renamed-owned-work'
            with self.subTest(stage=stage), self.assertRaisesRegex(ValueError, 'PLAN_DUPLICATE_HYPOTHESIS'):
                W.validate_plan(duplicate, observed, self.config)
        self.assertEqual(observed['nativePipelineUsed'], 0)
        self.assertEqual(observed['waitingPipelineUsed'], 1)

    def test_parallel_three_gpu_waits_leave_nine_committed_slots(self):
        self.parallel_capacity(); self.add_seed(); self.add_seed('muse_attention_decode', b'# attention\n')
        for problem in W.PROBLEMS: self.add_gpu_job(problem)
        observed = self.w.observe()
        self.assertEqual(observed['availableCandidateSlots'], 9)
        self.assertEqual(observed['boardAvailableCandidateSlots'], {p: 3 for p in W.PROBLEMS})
        self.assertEqual(observed['waitingPipelineUsed'], 3)
        self.w.tick(); self.complete_review(self.parallel_plan({p: 3 for p in W.PROBLEMS})); self.w.tick()
        observed = self.w.observe()
        self.assertEqual(observed['pipelineUsed'], 12)
        self.assertEqual(observed['nativePipelineUsed'], 9)
        self.assertEqual(observed['waitingPipelineUsed'], 3)
        self.assertEqual(observed['eligibleProblems'], [])

    def test_parallel_unknown_native_owner_blocks_only_its_board(self):
        self.parallel_capacity(); self.add_seed()
        detail = self.client.occurrences[self.seed]
        detail['occurrence']['state'] = 'running'; detail['attempts'][0].update(state='running', receipt=None)
        observed = self.w.observe()
        self.assertEqual(observed['pipelineUsed'], 1)
        self.assertEqual(observed['uncertainProblems'], ['muse_decode_layer'])
        self.assertEqual(observed['boardAvailableCandidateSlots']['muse_decode_layer'], 0)
        self.assertEqual(observed['eligibleProblems'], ['muse_prefill_layer'])

    def test_parallel_partial_same_board_lost_ack_recovers_then_starts_remainder_once(self):
        self.parallel_capacity(); self.w.tick(); self.complete_review(self.parallel_plan({'muse_decode_layer': 4}))
        self.client.timeout_after_commit_number = 7
        with self.assertRaises(TimeoutError): self.w.tick()
        self.assertEqual(len(self.w.state['experiments']), 2)
        self.reload(); self.assertEqual(self.w.tick()['state'], 'experiments_dispatched')
        self.assertEqual(len(self.w.state['experiments']), 4)
        self.assertEqual(len(self.client.posts), 9)
        run_keys = [body['requestKey'] for route, body in self.client.posts if route.endswith('/run')]
        self.assertEqual(len(run_keys), 5)
        self.assertEqual(len(run_keys), len(set(run_keys)))
        self.assertEqual(self.w.observe()['pipelineUsedByProblem']['muse_decode_layer'], 4)

    def test_parallel_partial_unknown_does_not_start_remaining_branches(self):
        self.parallel_capacity(); self.w.tick(); self.complete_review(self.parallel_plan({'muse_decode_layer': 4}))
        self.client.timeout_before_commit_number = 7
        with self.assertRaises(TimeoutError): self.w.tick()
        self.reload()
        with self.assertRaisesRegex(ValueError, 'POST_UNCERTAIN_RECONCILE_ONLY'): self.w.tick()
        self.assertEqual(len(self.client.posts), 7)
        reservation = self.w.state['supervisions'][-1]['pipelineReservation']
        self.assertEqual(reservation, {'externalUsed': 0, 'acknowledged': 2, 'remaining': 2, 'total': 4})

    def test_parallel_partial_new_owner_reconciles_without_overfilling_board(self):
        self.parallel_capacity(); self.w.tick(); self.complete_review(self.parallel_plan({'muse_decode_layer': 4}))
        self.client.timeout_after_commit_number = 7
        with self.assertRaises(TimeoutError): self.w.tick()
        external = self.add_seed('muse_decode_layer', b'# independently owned source\n')
        self.client.occurrences[external]['occurrence']['state'] = 'running'
        self.client.occurrences[external]['attempts'][0].update(state='running', receipt=None)
        self.reload(); result = self.w.tick()
        self.assertEqual(result['state'], 'changed_plan_reconciled')
        self.assertEqual(len(self.client.posts), 7)
        self.assertEqual(len(self.w.state['experiments']), 3)
        self.assertEqual(len(result['unstartedRemainder']), 1)
        self.assertEqual(self.w.observe()['pipelineUsedByProblem']['muse_decode_layer'], 4)

    def test_parallel_input_exposes_productive_capacity_without_changing_control(self):
        self.w.tick(); legacy_rules = self.w.state['supervisions'][-1]['input']['interpretationRules']
        self.parallel_capacity(); self.w.submit_supervisor(self.w.observe()); inp = self.w.state['supervisions'][-1]['input']
        self.assertEqual(inp['boardAvailableCandidateSlots']['muse_decode_layer'], 4)
        self.assertEqual(inp['pipelinePerProblemLimit'], 4)
        self.assertIn('SAME eligible problem', inp['interpretationRules'][0])
        self.assertIn('confirmedSourceArtifactIds', inp['interpretationRules'][0])
        self.assertIn('quota waits and official GPU work', inp['interpretationRules'][0])
        self.assertIn('No new official authority', inp['interpretationRules'][1])
        self.assertIn('owned EXPERIMENT cannot be replaced or duplicated', inp['interpretationRules'][1])
        self.assertEqual(inp['interpretationRules'][2:-1], legacy_rules[2:])
        self.assertIn('never invent filler sessions', inp['interpretationRules'][-1])
        self.assertEqual(self.config['controlDefinitionSHA256'], W.digest(W.supervisor_definition(self.config)))

    def test_parallel_repair_candidate_cannot_be_duplicated_with_new_hypothesis(self):
        status = self.repairable_operation(); self.parallel_capacity(); self.install_diagnostic_fixture([status])
        self.w.tick(); observed = self.w.observe()
        plan = self.parallel_plan({'muse_decode_layer': 2}); plan['decision'] = 'repair_evidence'
        target = observed['autoCheckDiagnostics']['repairableEvidence'][0]
        for exp in plan['experiments']: exp['sourceArtifactId'] = target['candidateArtifactId']
        observed['autoCheckDiagnostics']['repairableEvidence'].append(copy.deepcopy(target))
        with self.assertRaisesRegex(ValueError, 'PLAN_DUPLICATE_REPAIR'):
            W.validate_plan(plan, observed, self.config)

    def test_parallel_repair_rejects_identical_candidate_already_in_ordinary_qa(self):
        status = self.repairable_operation(); self.parallel_capacity(); self.install_diagnostic_fixture([status])
        self.w.tick(); observed = self.w.observe()
        target = observed['autoCheckDiagnostics']['repairableEvidence'][0]
        plan = self.parallel_plan({'muse_decode_layer': 1}); plan['decision'] = 'repair_evidence'
        plan['experiments'][0]['sourceArtifactId'] = target['candidateArtifactId']
        source = next(a for a in observed['catalog'] if a['id'] == target['candidateArtifactId'])
        other = uid(); observed['catalog'].append({**source, 'id': uid(), 'occurrenceId': other})
        observed['pipelineOwners'].append({'problem': 'muse_decode_layer', 'ownerKey': 'occurrence:' + other,
            'stages': ['native'], 'occurrenceIds': [other], 'jobIds': [], 'candidateSHA256': None})
        with self.assertRaisesRegex(ValueError, 'PLAN_DUPLICATE_REPAIR'):
            W.validate_plan(plan, observed, self.config)

    def test_parallel_linked_repair_deduplicates_parent_but_not_two_native_actors(self):
        status, record, detail = self.repair_capacity_fixture()
        self.parallel_capacity(); self.install_diagnostic_fixture([status])
        observed = self.w.observe()
        self.assertEqual(observed['pipelineUsed'], 2)
        self.assertEqual(observed['pipelineUsedByProblem']['muse_decode_layer'], 1)
        other = self.client.make_occurrence(record['cycleId'], {'requestKey': 'second-owned-repair',
            'input': copy.deepcopy(detail['occurrence']['input'])})
        self.w.state['experiments'].append({**record, 'occurrenceId': other['id']})
        observed = self.w.observe()
        self.assertEqual(observed['pipelineUsed'], 3)
        self.assertEqual(observed['pipelineUsedByProblem']['muse_decode_layer'], 2)

    def test_parallel_partial_three_boards_including_gpu_reservations_stay_bounded(self):
        self.parallel_capacity(); self.add_seed(); self.add_seed('muse_attention_decode', b'# attention\n')
        for problem in W.PROBLEMS: self.add_gpu_job(problem)
        self.w.tick(); self.complete_review(self.parallel_plan({p: 3 for p in W.PROBLEMS}))
        self.client.timeout_after_commit_number = 17
        with self.assertRaises(TimeoutError): self.w.tick()
        self.assertEqual(self.w.state['supervisions'][-1]['pipelineReservation']['total'], 12)
        self.reload(); self.assertEqual(self.w.tick()['state'], 'experiments_dispatched')
        self.assertEqual(len(self.client.posts), 19)
        self.assertEqual(self.w.observe()['pipelineUsed'], 12)
        self.assertEqual(self.w.observe()['pipelineUsedByProblem'], {p: 4 for p in W.PROBLEMS})

    def test_parallel_quota_or_uncertain_owner_keeps_board_reserved(self):
        self.parallel_capacity(); observed = self.w.observe()
        source = observed['catalog'][0]
        for state in ['quota_wait', 'uncertain_owned', 'check_running']:
            diagnostics = {'outcomes': [{'problem': source['problem'], 'occurrenceId': source['occurrenceId'],
                                        'candidateSHA256': source['digest'], 'state': state}]}
            admission = W.pipeline_admission(12, [], observed['catalog'], set(), [], diagnostics,
                set(observed['confirmedSourceArtifactIds']), capacity_policy=self.config)
            with self.subTest(state=state):
                self.assertEqual(admission['pipelineUsed'], 1)
                self.assertEqual(admission['boardAvailableCandidateSlots'][source['problem']], 0)
                self.assertEqual(admission['uncertainProblems'], [source['problem']])

    def prepared_source_action(self):
        self.parallel_capacity()
        pending, observed = self.prepared_action()
        self.assertEqual(pending['actionEvidence']['version'], 2)
        return pending, observed

    def test_selected_source_plan_survives_other_producer_to_qa_same_board(self):
        self.parallel_capacity(); self.w.tick()
        self.complete_review(self.parallel_plan({'muse_decode_layer': 1})); self.w.tick()
        first = self.w.state['experiments'][-1]
        self.w.tick(); plan = self.parallel_plan({'muse_decode_layer': 1})
        plan['experiments'][0]['hypothesis'] = 'A second distinct normalization schedule may reduce independent launch overhead.'
        pending = self.w.state['supervisions'][-1]
        self.complete_review(plan)
        self.client.complete(first['occurrenceId'], {'candidate.py': b'# new descendant, not the selected base\n'})
        detail = self.client.occurrences[first['occurrenceId']]; sid, aid = uid(), uid()
        detail['occurrence']['state'] = 'running'
        detail['steps'].append({'id': sid, 'key': 'independent-qa', 'attempt': 1, 'state': 'running'})
        detail['attempts'].append({'id': aid, 'step_id': sid, 'number': 1, 'state': 'running', 'receipt': None})
        before = len(self.client.posts); result = self.w.tick()
        self.assertEqual(result['state'], 'experiments_dispatched')
        self.assertEqual(len(self.client.posts) - before, 2)
        self.assertEqual(pending['state'], 'consumed')
        self.assertTrue(pending['acceptedAcrossIndependentChange'])
        self.assertEqual(detail['attempts'][-1]['state'], 'running')

    def test_source_scope_child_report_parent_hash_does_not_mean_same_candidate(self):
        pending, original = self.prepared_source_action()
        selected = next(a for a in original['catalog'] if a['id'] == pending['approvedPlan']['experiments'][0]['sourceArtifactId'])
        fresh = copy.deepcopy(original)
        child = {'occurrenceId': uid(), 'problem': selected['problem'], 'state': 'running', 'finishedAt': None,
                 'failures': [], 'artifacts': [], 'executionEvidence': {'bindingState': 'verified',
                    'sourceSHA256': selected['digest'], 'candidateSHA256': 'c' * 64, 'classification': 'cpu_check_failed_reported'}}
        fresh['outcomes'].append(child)
        self.assertTrue(W.unchanged_action_scope(pending, fresh, self.config))
        child['executionEvidence']['candidateSHA256'] = selected['digest']
        self.assertFalse(W.unchanged_action_scope(pending, fresh, self.config))

    def test_source_scope_matching_bytes_qa_from_other_origin_invalidates(self):
        pending, original = self.prepared_source_action()
        selected = next(a for a in original['catalog'] if a['id'] == pending['approvedPlan']['experiments'][0]['sourceArtifactId'])
        fresh = copy.deepcopy(original)
        qa = {'id': uid(), 'name': 'qa-verification.json', 'digest': 'd' * 64, 'size': 100,
              'qaEvidence': {'bindingState': 'verified', 'candidateSHA256': selected['digest'],
                             'blockingFindings': [{'id': 'new-blocker', 'detail': 'Exact bytes fail the independent check.'}]}}
        fresh['outcomes'].append({'occurrenceId': uid(), 'problem': selected['problem'], 'state': 'completed',
                                 'finishedAt': None, 'failures': [], 'artifacts': [qa]})
        self.assertFalse(W.unchanged_action_scope(pending, fresh, self.config))
        qa['qaEvidence']['candidateSHA256'] = 'a' * 64
        self.assertTrue(W.unchanged_action_scope(pending, fresh, self.config))
        qa['qaEvidence'] = {'bindingState': 'unavailable', 'code': 'QA_CONTEXT_SOURCE_BINDING'}
        self.assertTrue(W.unchanged_action_scope(pending, fresh, self.config))

    def test_source_scope_selected_origin_unknown_qa_and_boundary_fail_closed(self):
        pending, original = self.prepared_source_action()
        selected = next(a for a in original['catalog'] if a['id'] == pending['approvedPlan']['experiments'][0]['sourceArtifactId'])
        for change in ['qa', 'boundary', 'descriptor', 'duplicate']:
            fresh = copy.deepcopy(original)
            if change == 'qa':
                outcome = next(o for o in fresh['outcomes'] if o['occurrenceId'] == selected['occurrenceId'])
                outcome['artifacts'].append({'id': uid(), 'name': 'qa-verification.json', 'digest': 'd' * 64, 'size': 100,
                    'qaEvidence': {'bindingState': 'unavailable', 'code': 'QA_CONTEXT_SOURCE_BINDING'}})
            elif change == 'boundary': fresh['confirmedSourceArtifactIds'].remove(selected['id'])
            elif change == 'descriptor': next(a for a in fresh['catalog'] if a['id'] == selected['id'])['size'] += 1
            else: fresh['catalog'].append(copy.deepcopy(selected))
            with self.subTest(change=change): self.assertFalse(W.unchanged_action_scope(pending, fresh, self.config))

    def test_source_scope_official_result_only_matches_exact_candidate(self):
        pending, original = self.prepared_source_action()
        selected = next(a for a in original['catalog'] if a['id'] == pending['approvedPlan']['experiments'][0]['sourceArtifactId'])
        fresh = copy.deepcopy(original)
        result = {'id': 'f' * 16, 'problem': selected['problem'], 'sourceSHA256': 'b' * 64, 'status': 'done',
                  'result': {'passed': False}, 'caseCounts': {}}
        fresh['officialJobs'].append(result)
        self.assertTrue(W.unchanged_action_scope(pending, fresh, self.config))
        result['sourceSHA256'] = selected['digest']
        self.assertFalse(W.unchanged_action_scope(pending, fresh, self.config))

    def test_source_scope_external_history_and_canonical_pins_remain_relevant(self):
        pending, original = self.prepared_source_action()
        fresh = copy.deepcopy(original)
        fresh['sourceExternalEvidence'] = [{'problem': 'muse_decode_layer', 'bindingState': 'verified',
                                          'candidateSHA256': 'b' * 64, 'findings': ['Historical defect requires revalidation.']}]
        self.assertFalse(W.unchanged_action_scope(pending, fresh, self.config))
        config = copy.deepcopy(self.config)
        config['problemMaterials']['muse_decode_layer'][0]['sha256'] = 'a' * 64
        self.assertFalse(W.unchanged_action_scope(pending, original, config))

    def test_source_scope_same_bytes_do_not_bypass_fresh_capacity_or_duplicate_admission(self):
        pending, original = self.prepared_source_action()
        source = next(a for a in original['catalog'] if a['id'] == pending['approvedPlan']['experiments'][0]['sourceArtifactId'])
        for change, code in [('capacity', 'PLAN_CAPACITY'), ('board', 'PLAN_BOARD_CAPACITY'),
                             ('hypothesis', 'PLAN_DUPLICATE_HYPOTHESIS')]:
            fresh = copy.deepcopy(original); fresh['fingerprint'] = 'f' * 64
            if change == 'capacity': fresh['availableCandidateSlots'] = 0
            elif change == 'board': fresh['boardAvailableCandidateSlots']['muse_decode_layer'] = 0
            else: fresh['hypothesisReservations'].append({'key': W.hypothesis_identity(source['problem'], source['digest'],
                                                                                     pending['approvedPlan']['experiments'][0]['hypothesis'])})
            self.assertTrue(W.unchanged_action_scope(pending, fresh, self.config))
            result = self.w.consume_plan(copy.deepcopy(pending), fresh)
            with self.subTest(change=change):
                self.assertEqual(result['state'], 'stale_plan_superseded')
                self.assertEqual(result['code'], code)
                self.assertEqual(len(self.client.posts), 1)

    def test_source_scope_repair_target_changes_cannot_hide_behind_same_candidate(self):
        status = self.repairable_operation(); self.parallel_capacity(); self.install_diagnostic_fixture([status])
        self.w.tick(); pending = self.w.state['supervisions'][-1]; original = self.w.observe()
        plan = self.parallel_plan({'muse_decode_layer': 1}); plan['decision'] = 'repair_evidence'
        plan['experiments'][0]['sourceArtifactId'] = original['autoCheckDiagnostics']['repairableEvidence'][0]['candidateArtifactId']
        pending['approvedPlan'] = plan
        for field in ['qaSHA256', 'producerArtifacts', 'repairQADiagnostic', 'canonicalPatch']:
            fresh = copy.deepcopy(original); target = fresh['autoCheckDiagnostics']['repairableEvidence'][0]
            if field == 'qaSHA256': target[field] = 'a' * 64
            elif field == 'producerArtifacts': target[field][0]['digest'] = 'b' * 64
            elif field == 'canonicalPatch': target[field]['sha256'] = 'c' * 64
            else: target[field]['report']['findings'] = ['Changed relevant repair finding.']
            with self.subTest(field=field): self.assertFalse(W.unchanged_action_scope(pending, fresh, self.config))

    def test_source_scope_legacy_board_proof_never_upgraded_in_place(self):
        pending, original = self.prepared_source_action()
        legacy = copy.deepcopy(pending); legacy['actionEvidence'] = W.action_evidence(original, self.config, version=1)
        fresh = copy.deepcopy(original)
        fresh['active'].append({'problem': 'muse_decode_layer', 'occurrenceId': uid(), 'state': 'running', 'attemptIds': []})
        self.assertTrue(W.unchanged_action_scope(pending, fresh, self.config))
        self.assertFalse(W.unchanged_action_scope(legacy, fresh, self.config))
        self.assertEqual(legacy['actionEvidence']['version'], 1)

    def test_source_scope_stale_controller_authority_still_invalidates(self):
        pending, original = self.prepared_source_action()
        fresh = copy.deepcopy(original)
        fresh['autoCheckDiagnostics'].update(controllerState='observation_stale', repairAuthority=None)
        self.assertFalse(W.unchanged_action_scope(pending, fresh, self.config))
        self.assertEqual(self.w.consume_plan(pending, fresh)['state'], 'stale_plan_superseded')
        self.assertEqual(len(self.client.posts), 1)

    def test_source_scope_pin_change_with_uncertain_run_reconciles_by_get_only(self):
        self.parallel_capacity(); self.w.tick(); self.complete_review(self.parallel_plan({'muse_decode_layer': 4}))
        self.client.timeout_before_commit_number = 7
        with self.assertRaises(TimeoutError): self.w.tick()
        self.config['problemMaterials']['muse_decode_layer'][0]['sha256'] = 'f' * 64
        before_gets = len(self.client.gets)
        self.reload(); result = self.w.tick()
        self.assertEqual(result['state'], 'owned_dispatch_uncertain')
        self.assertGreater(len(self.client.gets), before_gets)
        self.assertEqual(len(self.client.posts), 7)
        self.assertEqual(self.w.state['supervisions'][-1]['state'], 'dispatching')

    def test_source_scope_unpinned_material_drift_preserves_existing_fail_closed_limit(self):
        # Known preexisting dispatch-body limitation, not a new recovery guarantee:
        # unchanged pins do not preflight actual files in a source read-set hash.
        self.parallel_capacity(); self.w.tick(); self.complete_review(self.parallel_plan({'muse_decode_layer': 4}))
        self.client.timeout_before_commit_number = 7
        with self.assertRaises(TimeoutError): self.w.tick()
        pathlib.Path(self.config['problemMaterials']['muse_decode_layer'][0]['path']).write_bytes(b'Changed unpinned material\n')
        self.reload()
        with self.assertRaisesRegex(ValueError, 'MATERIAL_CHANGED'): self.w.tick()
        self.assertEqual(len(self.client.posts), 7)
        self.assertEqual(self.w.state['supervisions'][-1]['state'], 'dispatching')

    def source_inventory_fixture(self):
        pending, original = self.prepared_source_action()
        source = next(a for a in original['catalog'] if a['id'] == pending['approvedPlan']['experiments'][0]['sourceArtifactId'])
        row = next(o for o in original['outcomes'] if o['occurrenceId'] == source['occurrenceId'])
        row['failures'] = [{'attemptId': uid(), 'code': 'A'}, {'attemptId': uid(), 'code': 'B'}]
        row['executionEvidence'] = {'bindingState': 'verified', 'candidateSHA256': source['digest'],
            'artifacts': [{'id': uid(), 'digest': 'a' * 64}, {'id': uid(), 'digest': 'b' * 64}],
            'producerDiagnostics': {'sections': [{'pointer': '/cases/0'}, {'pointer': '/cases/1'}]}}
        row['artifacts'].append({'id': uid(), 'name': 'qa-verification.json', 'digest': 'd' * 64, 'size': 100,
            'qaEvidence': {'bindingState': 'verified', 'candidateSHA256': source['digest'],
                          'blockingFindings': [{'id': 'F1'}, {'id': 'F2'}]}})
        original['autoCheckDiagnostics']['repairableEvidence'] = [{
            'problem': source['problem'], 'occurrenceId': source['occurrenceId'], 'candidateArtifactId': source['id'],
            'producerArtifacts': [{'id': uid(), 'digest': 'c' * 64}, {'id': uid(), 'digest': 'e' * 64}],
            'repairQADiagnostic': {'report': {'cases': [{'name': 'first'}, {'name': 'second'}]}}}]
        return original, source

    def test_source_scope_normalizes_only_contract_inventories(self):
        original, source = self.source_inventory_fixture(); fresh = copy.deepcopy(original)
        row = next(o for o in fresh['outcomes'] if o['occurrenceId'] == source['occurrenceId'])
        row['failures'].reverse(); row['artifacts'].reverse(); row['executionEvidence']['artifacts'].reverse()
        fresh['autoCheckDiagnostics']['repairableEvidence'][0]['producerArtifacts'].reverse()
        before = copy.deepcopy(original)
        self.assertEqual(W.action_evidence(original, self.config), W.action_evidence(fresh, self.config))
        self.assertEqual(original, before)

    def test_source_scope_keeps_sequences_and_duplicate_inventory_significant(self):
        original, source = self.source_inventory_fixture()
        for target in ['findings', 'diagnostic_sections', 'repair_cases', 'duplicate_artifact', 'duplicate_failure']:
            fresh = copy.deepcopy(original)
            row = next(o for o in fresh['outcomes'] if o['occurrenceId'] == source['occurrenceId'])
            if target == 'findings': row['artifacts'][-1]['qaEvidence']['blockingFindings'].reverse()
            elif target == 'diagnostic_sections': row['executionEvidence']['producerDiagnostics']['sections'].reverse()
            elif target == 'repair_cases': fresh['autoCheckDiagnostics']['repairableEvidence'][0]['repairQADiagnostic']['report']['cases'].reverse()
            elif target == 'duplicate_artifact': row['executionEvidence']['artifacts'].append(copy.deepcopy(row['executionEvidence']['artifacts'][0]))
            else: row['failures'].append(copy.deepcopy(row['failures'][0]))
            with self.subTest(target=target):
                self.assertNotEqual(W.action_evidence(original, self.config), W.action_evidence(fresh, self.config))

    def test_partial_dispatch_second_unknown_never_reposts(self):
        self.add_seed(); self.w.tick(); self.complete_review(self.plan(2))
        self.client.timeout_before_commit_number = 5
        with self.assertRaises(TimeoutError): self.w.tick()
        self.assertEqual(len(self.w.state['experiments']), 1)
        self.reload()
        with self.assertRaisesRegex(ValueError, 'POST_UNCERTAIN_RECONCILE_ONLY'): self.w.tick()
        self.assertEqual(len(self.client.posts), 5)
        self.assertEqual(self.w.state['supervisions'][-1]['state'], 'dispatching')
        self.assertEqual(self.w.state['supervisions'][-1]['pipelineReservation'],
                         {'externalUsed': 0, 'acknowledged': 1, 'remaining': 1, 'total': 2})

    def test_partial_dispatch_changed_evidence_preserves_owned_intent(self):
        self.add_seed(); self.w.tick(); self.complete_review(self.plan(2))
        self.client.timeout_before_commit_number = 5
        with self.assertRaises(TimeoutError): self.w.tick()
        self.client.complete(self.seed, {'candidate.py': b'# changed after first ACK\n'})
        self.reload(); result = self.w.tick()
        self.assertEqual(result['state'], 'owned_dispatch_uncertain')
        self.assertEqual(result['uncertainEffects'][0]['stage'], 'run')
        self.assertEqual(self.w.state['supervisions'][-1]['state'], 'dispatching')
        self.assertEqual(len(self.client.posts), 5)
        self.assertTrue(self.w.state['supervisions'][-1]['reconcileOnly'])
        self.assertEqual(self.w.tick()['state'], 'owned_dispatch_uncertain')
        self.assertEqual(len(self.client.posts), 5)

    def test_changed_evidence_recovers_lost_run_response_only_by_get(self):
        self.add_seed(); self.w.tick(); self.complete_review(self.plan(2))
        self.client.timeout_after_commit_number = 5
        with self.assertRaises(TimeoutError): self.w.tick()
        self.client.complete(self.seed, {'candidate.py': b'# changed evidence after first ACK\n'})
        before_gets = len(self.client.gets)
        self.reload(); result = self.w.tick()
        self.assertEqual(result['state'], 'changed_plan_reconciled')
        self.assertEqual(len(result['dispatched']), 2)
        self.assertEqual(len(self.w.state['experiments']), 2)
        self.assertEqual(self.w.state['supervisions'][-1]['state'], 'consumed')
        self.assertGreater(len(self.client.gets), before_gets)
        self.assertEqual(len(self.client.posts), 5)
        self.assertEqual(self.w.tick()['state'], 'work_active')
        self.assertEqual(len(self.client.posts), 5)

    def test_changed_evidence_create_ack_run_unattempted_is_closed(self):
        self.w.tick(); self.complete_review()
        original = self.w.post_once
        def stop_before_run(kind, *args, **kwargs):
            if kind == 'run': raise RuntimeError('interrupted before run intent')
            return original(kind, *args, **kwargs)
        with mock.patch.object(self.w, 'post_once', side_effect=stop_before_run):
            with self.assertRaisesRegex(RuntimeError, 'before run intent'): self.w.tick()
        self.assertEqual(len(self.client.posts), 2)  # Supervisor run and experiment create only.
        self.client.complete(self.seed, {'candidate.py': b'# changed before run attempt\n'})
        self.reload(); result = self.w.tick()
        self.assertEqual(result['state'], 'changed_plan_reconciled')
        self.assertEqual(result['dispatched'], [])
        self.assertEqual(result['unstartedRemainder'][0]['reason'], 'RUN_NOT_ATTEMPTED')
        self.assertEqual(self.w.state['supervisions'][-1]['state'], 'superseded')
        self.assertEqual(len(self.client.posts), 2)
        self.assertEqual(self.w.tick()['state'], 'supervisor_dispatched')

    def test_changed_evidence_committed_create_response_lost_never_runs_it(self):
        self.add_seed(); self.w.tick(); self.complete_review(self.plan(2))
        self.client.timeout_after_commit_number = 4
        with self.assertRaises(TimeoutError): self.w.tick()
        self.client.complete(self.seed, {'candidate.py': b'# changed after second create\n'})
        self.reload(); result = self.w.tick()
        self.assertEqual(result['state'], 'changed_plan_reconciled')
        self.assertEqual(len(result['dispatched']), 1)
        self.assertEqual(result['unstartedRemainder'][0]['reason'], 'RUN_NOT_ATTEMPTED')
        self.assertEqual(len(self.client.posts), 4)
        self.assertEqual(self.w.state['supervisions'][-1]['state'], 'consumed')

    def test_changed_evidence_unattempted_second_action_never_starts(self):
        self.add_seed(); self.w.tick(); self.complete_review(self.plan(2))
        original = self.w.dispatch_experiment
        def stop_second(exp, *args, **kwargs):
            if exp['id'] == 'experiment-1': raise RuntimeError('no second create intent')
            return original(exp, *args, **kwargs)
        with mock.patch.object(self.w, 'dispatch_experiment', side_effect=stop_second):
            with self.assertRaises(RuntimeError): self.w.tick()
        self.client.complete(self.seed, {'candidate.py': b'# changed before second create\n'})
        self.reload(); result = self.w.tick()
        self.assertEqual(result['state'], 'changed_plan_reconciled')
        self.assertEqual(len(result['dispatched']), 1)
        self.assertEqual(result['unstartedRemainder'][0]['reason'], 'CREATE_NOT_ATTEMPTED')
        self.assertEqual(len(self.client.posts), 3)

    def test_changed_evidence_unknown_create_keeps_precise_ownership(self):
        self.w.tick(); self.complete_review()
        self.client.timeout_before_commit_number = 2
        with self.assertRaises(TimeoutError): self.w.tick()
        self.client.complete(self.seed, {'candidate.py': b'# changed while create uncertain\n'})
        self.reload(); result = self.w.tick()
        self.assertEqual(result['state'], 'owned_dispatch_uncertain')
        self.assertEqual(result['uncertainEffects'][0]['stage'], 'create')
        self.assertEqual(result['acknowledgedExperiments'], [])
        self.assertEqual(len(self.client.posts), 2)

    def test_reconcile_receipt_cannot_hide_changed_run_body(self):
        self.w.tick(); self.complete_review(); self.client.timeout_after_commit_number = 3
        with self.assertRaises(TimeoutError): self.w.tick()
        self.client.complete(self.seed, {'candidate.py': b'# changed parent evidence\n'})
        attempted = next(d for d in self.client.occurrences.values()
                         if d['occurrence'].get('input', {}).get('experiment'))
        attempted['occurrence']['input']['experiment']['hypothesis'] = 'different unapproved hypothesis'
        self.reload()
        with self.assertRaisesRegex(ValueError, 'RECONCILE_RECEIPT_CHANGED'): self.w.tick()
        self.assertEqual(self.w.state['supervisions'][-1]['state'], 'dispatching')
        self.assertEqual(len(self.client.posts), 3)

    def test_reconcile_only_latch_survives_evidence_returning_to_old_fingerprint(self):
        self.w.tick(); self.complete_review(); original_seed = copy.deepcopy(self.client.occurrences[self.seed])
        self.client.timeout_before_commit_number = 2
        with self.assertRaises(TimeoutError): self.w.tick()
        self.client.complete(self.seed, {'candidate.py': b'# temporary evidence change\n'})
        self.reload(); self.assertEqual(self.w.tick()['state'], 'owned_dispatch_uncertain')
        # The original POST completes late. Expose its result to GET, without a new POST.
        body = self.client.posts[1][1]; cycle_id = uid()
        self.client.cycles[cycle_id] = {'id': cycle_id, 'request_key': body['requestKey'],
                                       'definition': body['definition'], 'state': 'draft'}
        self.client.occurrences[self.seed] = original_seed
        self.reload()
        self.assertEqual(self.w.observe()['fingerprint'], self.w.state['supervisions'][-1]['fingerprint'])
        result = self.w.tick()
        self.assertEqual(result['state'], 'changed_plan_reconciled')
        self.assertEqual(result['unstartedRemainder'][0]['reason'], 'RUN_NOT_ATTEMPTED')
        self.assertEqual(len(self.client.posts), 2)
        self.assertEqual(self.w.state['experiments'], [])

    def test_changed_evidence_repair_run_recovery_preserves_repair_lineage(self):
        status = self.repairable_operation()
        self.assertEqual(self.w.tick()['state'], 'supervisor_dispatched')
        review = self.w.state['supervisions'][-1]
        target = review['input']['autoCheckDiagnostics']['repairableEvidence'][0]
        plan = self.plan(); plan['decision'] = 'repair_evidence'
        plan['experiments'][0]['sourceArtifactId'] = target['candidateArtifactId']
        self.complete_review(plan); self.client.timeout_after_commit_number = len(self.client.posts) + 2
        with self.assertRaises(TimeoutError): self.w.tick()
        posts = len(self.client.posts)
        self.client.complete(self.seed, {'candidate.py': b'# changed unrelated seed after repair dispatch\n'})
        self.reload(); result = self.w.tick()
        self.assertEqual(result['state'], 'changed_plan_reconciled')
        self.assertEqual(result['dispatched'][0]['repairOf'], status['occurrenceId'])
        self.assertEqual(result['dispatched'][0]['repairCandidateSHA256'], status['candidateSHA256'])
        self.assertEqual(result['dispatched'][0]['repairAttempt'], 1)
        self.assertEqual(len(self.client.posts), posts)

    def test_official_terminal_wins_over_adapter_running_label(self):
        source = 'a' * 64
        diagnostics = {'outcomes': [{'problem': 'muse_prefill_layer', 'candidateSHA256': source,
            'occurrenceId': uid(), 'state': 'check_running', 'jobId': '0123456789abcdef'}]}
        jobs = [{'problem': 'muse_prefill_layer', 'sourceSHA256': source, 'id': '0123456789abcdef', 'status': 'done'}]
        admission = W.pipeline_admission(2, [], [], set(), jobs, diagnostics, set())
        self.assertEqual(admission['pipelineUsed'], 0)
        jobs[0]['status'] = 'running'
        self.assertEqual(W.pipeline_admission(2, [], [], set(), jobs, diagnostics, set())['pipelineUsed'], 1)

    def test_quota_or_uncertain_source_occupies_one_slot_not_two(self):
        source = 'b' * 64; occurrence = uid(); artifact = uid()
        catalog = [{'id': artifact, 'problem': 'muse_prefill_layer', 'digest': source, 'occurrenceId': occurrence}]
        for state in ['quota_wait', 'uncertain_owned']:
            diagnostics = {'outcomes': [{'problem': 'muse_prefill_layer', 'candidateSHA256': source,
                'occurrenceId': occurrence, 'state': state, 'jobId': None}]}
            admission = W.pipeline_admission(2, [], catalog, {source}, [], diagnostics, {artifact})
            self.assertEqual(admission['pipelineUsed'], 1)
            self.assertEqual(admission['eligibleProblems'], [])
            self.assertEqual(admission['blockedProblems'], ['muse_prefill_layer'])

    def test_zero_capacity_still_allows_stale_owner_diagnosis(self):
        self.config['maxPendingCandidates'] = 1
        d = self.client.occurrences[self.seed]
        d['occurrence'].update(state='running', created_at='1970-01-01T00:01:00Z')
        d['attempts'][0].update(state='running', receipt=None)
        self.assertEqual(self.w.tick()['state'], 'supervisor_dispatched')
        plan = self.plan(); plan.update(decision='observe_active', experiments=[],
            reason='Preserve existing native actor and inspect its missing progress: ' + self.seed)
        self.complete_review(plan)
        self.assertEqual(self.w.tick()['state'], 'active_owner_preserved')
        self.assertEqual(len(self.client.posts), 1)

    def test_ambiguous_missing_receipt_never_resends(self):
        self.client.timeout_before_commit_number = 1
        with self.assertRaises(TimeoutError): self.w.tick()
        self.reload()
        with self.assertRaisesRegex(ValueError, 'POST_UNCERTAIN_RECONCILE_ONLY'): self.w.tick()
        self.assertEqual(len(self.client.posts), 1)

    def test_supervisor_response_lost_reconciles_stable_body_after_time_change(self):
        self.client.timeout_after_commit_number = 1
        with self.assertRaises(TimeoutError): self.w.tick()
        self.reload()
        self.assertEqual(self.w.tick()['state'], 'supervisor_dispatched')
        self.assertEqual(len(self.client.posts), 1)

    def test_paused_control_blocks_all_post(self):
        self.client.cycles[self.config['controlCycleId']]['state'] = 'paused'
        self.assertEqual(self.w.tick()['state'], 'paused_by_control_cycle')
        self.assertFalse(self.client.posts)

    def test_live_or_unconfirmed_executor_never_restarted(self):
        for state, stopped in [('running', False), ('completed', False), ('reconciling', False)]:
            self.client.occurrences[self.seed]['occurrence']['state'] = state
            self.client.occurrences[self.seed]['attempts'][0]['receipt']['stopped'] = stopped
            self.assertEqual(self.w.tick()['state'], 'work_active')
        self.assertFalse(self.client.posts)

    def test_unknown_attempt_state_blocks_instead_of_terminal_guess(self):
        self.client.occurrences[self.seed]['attempts'][0]['state'] = 'unknown'
        with self.assertRaisesRegex(ValueError, 'ATTEMPT_STATE_UNKNOWN'): self.w.tick()
        self.assertFalse(self.client.posts)

    def test_invalid_plan_gets_bounded_corrective_supervisor(self):
        self.w.tick(); self.complete_review({})
        result = self.w.tick()
        self.assertEqual(result['state'], 'supervisor_dispatched')
        self.assertEqual(result['continuation']['steps'][0]['state'], 'supervisor_plan_rejected')
        self.assertEqual(self.w.state['supervisions'][-1]['input']['priorReviewErrors'], ['PLAN_FIELDS'])
        self.complete_review({})
        self.w.tick()
        self.assertEqual(self.w.tick()['state'], 'supervisor_failure_needs_repair')
        self.assertEqual(len(self.client.posts), 2)

    def test_stale_plan_superseded_not_applied(self):
        self.w.tick(); self.complete_review()
        self.client.complete(self.seed, {'candidate.py': b'# changed material'})
        result = self.w.tick()
        self.assertEqual(result['state'], 'supervisor_dispatched')
        self.assertEqual(result['continuation']['steps'][0]['state'], 'stale_plan_superseded')
        self.assertEqual(self.w.state['supervisions'][-2]['state'], 'superseded')
        self.assertFalse(self.w.state['experiments'])

    def test_terminal_drain_stale_opens_exactly_one_fresh_review_in_same_tick(self):
        self.w.tick(); self.complete_review(); old = self.w.state['supervisions'][-1]
        self.client.complete(self.seed, {'candidate.py': b'# materially changed source\n'})
        before_posts = len(self.client.posts); before_gets = len(self.client.gets)
        result = self.w.tick()
        self.assertEqual(result['state'], 'supervisor_dispatched')
        self.assertEqual(result['continuation']['administrativeTransitions'], 1)
        self.assertEqual(result['continuation']['steps'][0]['state'], 'stale_plan_superseded')
        self.assertEqual(result['continuation']['nextAction'], 'await_supervisor_result')
        self.assertEqual(old['state'], 'superseded')
        self.assertEqual(len(self.client.posts), before_posts + 1)
        self.assertFalse(self.w.state['experiments'])
        self.assertGreaterEqual(self.client.gets[before_gets:].count('/api/cycles/' + self.config['controlCycleId']), 2)
        self.assertGreaterEqual(self.client.gets[before_gets:].count('/api/cycle-runs/' + self.seed), 2)
        self.assertEqual(self.w.tick()['state'], 'supervisor_running')
        self.assertEqual(len(self.client.posts), before_posts + 1)

    def test_terminal_drain_native_failure_retries_only_within_existing_limit(self):
        self.w.tick(); oid = self.w.state['supervisions'][-1]['occurrenceId']
        self.client.complete(oid, {}, state='failed')
        result = self.w.tick()
        self.assertEqual(result['state'], 'supervisor_dispatched')
        self.assertEqual(result['continuation']['steps'][0]['state'], 'supervisor_failed')
        self.assertEqual(self.w.state['supervisions'][-1]['input']['priorReviewErrors'], ['NATIVE_SUPERVISOR_FAILED'])
        self.client.complete(self.w.state['supervisions'][-1]['occurrenceId'], {}, state='failed')
        self.assertEqual(self.w.tick()['state'], 'supervisor_failure_needs_repair')
        self.assertEqual(len(self.client.posts), 2)

    def test_terminal_drain_pause_between_transitions_prevents_new_review(self):
        self.w.tick(); self.complete_review({}); original = self.w.status
        def pause_after_rejection(state, **facts):
            result = original(state, **facts)
            if state == 'supervisor_plan_rejected':
                self.client.cycles[self.config['controlCycleId']]['state'] = 'paused'
            return result
        with mock.patch.object(self.w, 'status', side_effect=pause_after_rejection): result = self.w.tick()
        self.assertEqual(result['state'], 'paused_by_control_cycle')
        self.assertEqual(result['continuation']['administrativeTransitions'], 1)
        self.assertEqual(result['continuation']['nextAction'], 'await_control_resume')
        self.assertEqual(len(self.client.posts), 1)

    def test_terminal_drain_reobserves_owner_before_retrying(self):
        self.w.tick(); self.complete_review({}); original = self.w.status
        def owner_after_rejection(state, **facts):
            result = original(state, **facts)
            if state == 'supervisor_plan_rejected':
                d = self.client.occurrences[self.seed]
                d['occurrence']['state'] = 'running'; d['attempts'][0].update(state='running', receipt=None)
            return result
        with mock.patch.object(self.w, 'status', side_effect=owner_after_rejection): result = self.w.tick()
        self.assertEqual(result['state'], 'work_active')
        self.assertEqual(result['continuation']['nextAction'], 'observe_owned_work')
        self.assertEqual(len(self.client.posts), 1)

    def test_terminal_drain_lost_retry_response_stops_and_reconciles_get_only(self):
        self.w.tick(); self.complete_review({}); self.client.timeout_after_commit_number = 2
        with self.assertRaises(TimeoutError): self.w.tick()
        requests = copy.deepcopy(self.w.state['reviewRequests'])
        self.assertEqual(len(self.client.posts), 2)
        self.reload(); self.assertEqual(self.w.tick()['state'], 'supervisor_dispatched')
        self.assertEqual(len(self.client.posts), 2)
        self.assertEqual(self.w.state['reviewRequests'], requests)
        self.assertEqual(self.w.tick()['state'], 'supervisor_running')

    def test_terminal_drain_uncertain_retry_never_resends(self):
        self.w.tick(); self.complete_review({}); self.client.timeout_before_commit_number = 2
        with self.assertRaises(TimeoutError): self.w.tick()
        self.reload()
        with self.assertRaisesRegex(ValueError, 'POST_UNCERTAIN_RECONCILE_ONLY'): self.w.tick()
        self.assertEqual(len(self.client.posts), 2)

    def test_terminal_drain_limit_preserves_recoverable_persisted_state(self):
        self.w.tick(); self.complete_review({})
        with mock.patch.object(W, 'MAX_ADMINISTRATIVE_TRANSITIONS', 1): result = self.w.tick()
        self.assertEqual(result['state'], 'administrative_transition_limit')
        self.assertEqual(result['continuation']['administrativeTransitions'], 1)
        self.assertEqual(result['continuation']['nextAction'], 'resume_controller_on_next_tick')
        self.assertEqual(self.w.state['supervisions'][-1]['state'], 'failed')
        self.assertEqual(len(self.client.posts), 1)
        self.reload(); self.assertEqual(self.w.tick()['state'], 'supervisor_dispatched')
        self.assertEqual(len(self.client.posts), 2)

    def test_terminal_drain_known_prior_fingerprint_cannot_loop_or_repost(self):
        original_seed = copy.deepcopy(self.client.occurrences[self.seed])
        self.w.tick(); self.complete_review(); first = copy.deepcopy(self.w.state['supervisions'][-1])
        self.client.complete(self.seed, {'candidate.py': b'# source B\n'})
        self.assertEqual(self.w.tick()['state'], 'supervisor_dispatched')
        self.complete_review(); self.client.occurrences[self.seed] = original_seed
        posts, records = len(self.client.posts), len(self.w.state['supervisions'])
        result = self.w.tick()
        self.assertEqual(result['state'], 'supervisor_dispatched')
        self.assertEqual(result['continuation']['administrativeTransitions'], 1)
        self.assertEqual(len(self.client.posts), posts)
        self.assertEqual(len(self.w.state['supervisions']), records + 1)
        self.assertFalse(self.w.state['experiments'])
        # Existing request-key semantics can acknowledge the historical review.
        # This patch stops there; changing that identity/recovery is separate work.
        self.assertEqual(result['occurrenceId'], first['occurrenceId'])
        self.assertEqual(self.w.state['supervisions'][-1]['input'], first['input'])

    def prefill_plan_while_decode_qa_runs(self):
        prefill = self.add_seed()
        d = self.client.occurrences[self.seed]
        d['occurrence']['state'] = 'running'; d['attempts'][0].update(state='running', receipt=None)
        self.assertEqual(self.w.tick()['state'], 'supervisor_dispatched')
        self.assertEqual(self.w.state['supervisions'][-1]['input']['availableCandidateSlots'], 1)
        self.complete_review()
        return prefill

    def test_prefill_plan_survives_other_board_qa_failure_and_dispatches_once(self):
        self.prefill_plan_while_decode_qa_runs()
        self.client.complete(self.seed, {'candidate.py': b'# rejected decode\n', 'qa-verification.json': {
            'passed': False, 'approvedForOfficialReview': False, 'findings': ['prepare has an unbound name']}}, state='attention')
        result = self.w.tick()
        self.assertEqual(result['state'], 'experiments_dispatched')
        self.assertEqual([r['problem'] for r in result['dispatched']], ['muse_prefill_layer'])
        record = self.w.state['supervisions'][-1]
        self.assertEqual(record['acceptedAcrossIndependentChange']['freshAdmission']['externalUsed'], 0)
        self.assertNotEqual(record['acceptedAcrossIndependentChange']['from'], record['acceptedAcrossIndependentChange']['to'])
        self.assertEqual(len(self.client.posts), 3)
        self.reload(); self.w.tick()
        self.assertEqual(sum('/run' in path and body['input'].get('experiment', {}).get('problem') == 'muse_prefill_layer'
                             for path, body in self.client.posts), 1)

    def test_legacy_plan_without_controller_proof_stays_stale(self):
        self.prefill_plan_while_decode_qa_runs()
        record = self.w.state['supervisions'][-1]
        record.pop('actionEvidence'); record['input'].pop('actionEvidence'); self.w.save()
        self.client.complete(self.seed, {'candidate.py': b'# rejected decode\n'}, state='attention')
        result = self.w.tick()
        self.assertEqual(result['state'], 'supervisor_dispatched')
        self.assertEqual(record['state'], 'superseded')
        self.assertFalse(self.w.state['experiments'])
        self.assertEqual(len(self.client.posts), 2)  # A fresh supervisor, never the stale action.

    def test_model_cannot_supply_action_evidence(self):
        self.w.tick(); plan = self.plan(); plan['actionEvidence'] = self.w.state['supervisions'][-1]['actionEvidence']
        self.complete_review(plan)
        result = self.w.tick()
        self.assertEqual(result['state'], 'supervisor_dispatched')
        self.assertEqual(result['continuation']['steps'][0]['code'], 'PLAN_FIELDS')
        self.assertFalse(self.w.state['experiments'])
        self.assertEqual(len(self.client.posts), 2)

    def test_same_board_failure_or_source_change_invalidates_plan(self):
        prefill = self.prefill_plan_while_decode_qa_runs()
        old = self.w.state['supervisions'][-1]
        self.client.complete(prefill, {'candidate.py': b'# new prefill source\n',
            'qa-verification.json': {'passed': False, 'findings': ['precision changed']}}, state='attention')
        result = self.w.tick()
        self.assertEqual(result['state'], 'supervisor_dispatched')
        self.assertEqual(old['state'], 'superseded')
        self.assertFalse(self.w.state['experiments'])
        self.assertEqual(len(self.client.posts), 2)

    def test_same_board_new_official_measurement_invalidates_plan(self):
        self.add_gpu_job(problem='muse_decode_layer', status='done')
        pending, obs = self.prepared_action()
        obs['officialJobs'][0]['result']['speedup'] = 0.9
        obs['fingerprint'] = W.digest(obs['officialJobs'])
        self.assertEqual(self.w.consume_plan(pending, obs)['state'], 'stale_plan_superseded')
        self.assertEqual(len(self.client.posts), 1)

    def prepared_action(self):
        self.w.tick(); self.complete_review()
        pending = self.w.state['supervisions'][-1]
        pending.update(approvedPlan=self.plan(), state='dispatching'); self.w.save()
        return pending, self.w.observe(exclude_supervisor=pending['occurrenceId'])

    def test_fresh_global_capacity_rejects_scoped_plan_without_post(self):
        pending, obs = self.prepared_action()
        obs.update(fingerprint='f' * 64, pipelineUsed=2, availableCandidateSlots=0, eligibleProblems=[])
        result = self.w.consume_plan(pending, obs)
        self.assertEqual(result['state'], 'stale_plan_superseded')
        self.assertEqual(result['code'], 'PLAN_CAPACITY')
        self.assertEqual(pending['errorCode'], 'PLAN_ADMISSION_CHANGED')
        self.assertEqual(len(self.client.posts), 1)

    def test_fresh_eligible_problem_rechecked_even_with_equal_scoped_proof(self):
        pending, obs = self.prepared_action(); obs['eligibleProblems'] = []
        result = self.w.consume_plan(pending, obs)
        self.assertEqual(result['code'], 'BOARD_PIPELINE_OWNED')
        self.assertEqual(len(self.client.posts), 1)

    def test_owner_source_boundary_and_isolation_change_invalidate_scope(self):
        pending, original = self.prepared_action()
        variants = []
        owner = copy.deepcopy(original); owner['pipelineOwners'].append({'problem': 'muse_decode_layer', 'ownerKey': 'new-owner'})
        variants.append(owner)
        boundary = copy.deepcopy(original); boundary['confirmedSourceArtifactIds'] = []; variants.append(boundary)
        isolated = copy.deepcopy(original); isolated['autoCheckDiagnostics']['outcomes'].append({
            'problem': 'muse_decode_layer', 'state': 'rejected_repairable', 'code': 'NO_EXECUTABLE_CHANGE',
            'classification': 'new_hypothesis_required', 'occurrenceId': self.seed,
            'candidateSHA256': original['catalog'][0]['digest']}); variants.append(isolated)
        for obs in variants:
            self.assertFalse(W.unchanged_action_scope(pending, obs, self.config))
        self.assertEqual(len(self.client.posts), 1)

    def test_policy_and_canonical_authority_change_reject_without_post(self):
        pending, obs = self.prepared_action(); obs['autoCheckDiagnostics']['policySHA256'] = 'a' * 64
        self.assertEqual(self.w.consume_plan(pending, obs)['state'], 'stale_plan_superseded')
        self.assertEqual(len(self.client.posts), 1)
        config = copy.deepcopy(self.config); config['problemMaterials']['muse_decode_layer'][0]['sha256'] = 'b' * 64
        self.assertFalse(W.unchanged_action_scope(pending, self.w.observe(), config))

    def test_pause_after_plan_never_dispatches(self):
        self.prefill_plan_while_decode_qa_runs()
        self.client.complete(self.seed, {'candidate.py': b'# stopped decode\n'}, state='attention')
        self.client.cycles[self.config['controlCycleId']]['state'] = 'paused'
        self.assertEqual(self.w.tick()['state'], 'paused_by_control_cycle')
        self.assertEqual(len(self.client.posts), 1)

    def bind_source_qa_blockers(self, blockers):
        detail = self.client.occurrences[self.seed]
        row = next(a for a in detail['artifacts'] if a['name'] == 'qa-verification.json')
        qa = json.loads(self.client.artifacts[row['id']])
        qa['blockingFindings'] = blockers; qa['findings'] = blockers
        raw = W.canonical(qa); self.client.artifacts[row['id']] = raw
        row.update(digest=W.digest(raw), size=len(raw))
        return row

    def test_complete_qa_blockers_reach_supervisor_and_selected_worker(self):
        blockers = [{'id': 'F01', 'severity': 'blocking', 'detail': 'First defect. ' + 'A' * 700},
                    {'id': 'F02', 'severity': 'blocking', 'detail': 'Second distinct checker defect. ' + 'B' * 900}]
        qa = self.bind_source_qa_blockers(blockers)
        self.w.tick()
        review = self.w.state['supervisions'][-1]['input']
        artifact = next(a for o in review['outcomes'] for a in o['artifacts'] if a['id'] == qa['id'])
        self.assertEqual(artifact['qaEvidence']['blockingFindings'], blockers)
        self.assertEqual(artifact['qaEvidence']['artifact']['digest'], qa['digest'])
        self.assertEqual(artifact['qaEvidence']['bindingState'], 'verified')
        self.assertTrue(artifact['review']['findingsTruncated'])
        self.complete_review(); self.w.tick()
        experiment = self.w.state['experiments'][-1]
        inp = self.client.occurrences[experiment['occurrenceId']]['occurrence']['input']['experiment']
        self.assertEqual(inp['sourceQAEvidence'][0]['blockingFindings'], blockers)
        self.assertEqual(inp['sourceQAEvidence'][0]['candidateSHA256'], inp['sourceArtifact']['digest'])
        self.assertEqual({r['name'] for r in inp['materials']}, {'base-candidate.py', 'canonical-TASK.md', 'canonical-AGENTS.md'})

    def test_source_qa_tampering_and_cross_candidate_text_never_injected(self):
        row = self.bind_source_qa_blockers([{'id': 'F01', 'detail': 'untrusted-cross-candidate-text'}])
        raw = self.client.artifacts[row['id']]
        self.client.artifacts[row['id']] = raw + b' '
        observed = self.w.observe()
        artifact = next(a for o in observed['outcomes'] for a in o['artifacts'] if a['id'] == row['id'])
        self.assertEqual(artifact['qaEvidence']['bindingState'], 'unavailable')
        self.assertNotIn('untrusted-cross-candidate-text', W.canonical(W.compact_outcomes(observed['outcomes'])).decode())
        qa = json.loads(raw); qa['candidateSHA256'] = 'f' * 64
        raw = W.canonical(qa); self.client.artifacts[row['id']] = raw; row.update(digest=W.digest(raw), size=len(raw))
        observed = self.w.observe()
        artifact = next(a for o in observed['outcomes'] for a in o['artifacts'] if a['id'] == row['id'])
        self.assertEqual(artifact['qaEvidence']['code'], 'QA_CONTEXT_CANDIDATE_BINDING')
        self.assertNotIn('untrusted-cross-candidate-text', W.canonical(W.compact_outcomes(observed['outcomes'])).decode())

    def test_source_qa_oversize_is_explicit_without_truncation_or_global_failure(self):
        for size, code in [(34000, 'QA_CONTEXT_BLOCKERS_SIZE_EXCEEDED'), (70000, 'QA_CONTEXT_SIZE_EXCEEDED')]:
            row = self.bind_source_qa_blockers([{'id': 'F01', 'detail': 'X' * size}])
            value = json.loads(self.client.artifacts[row['id']]); value['findings'] = []
            raw = W.canonical(value); self.client.artifacts[row['id']] = raw; row.update(digest=W.digest(raw), size=len(raw))
            observed = self.w.observe()
            context = next(a['qaEvidence'] for o in observed['outcomes'] for a in o['artifacts'] if a['id'] == row['id'])
            with self.subTest(size=size):
                self.assertEqual(context['bindingState'], 'unavailable')
                self.assertEqual(context['code'], code)
                self.assertNotIn('blockingFindings', context)
                self.assertTrue(observed['catalog'])

    def test_source_qa_old_attempt_and_wrong_parent_rejected(self):
        row = self.bind_source_qa_blockers([{'id': 'F01', 'detail': 'bound to exact source'}])
        detail = self.client.occurrences[self.seed]
        context, review = self.w.qa_context(detail, row, 'muse_decode_layer', set())
        self.assertEqual(context['code'], 'QA_CONTEXT_ATTEMPT'); self.assertIsNone(review)
        detail['occurrence']['input'] = {'experiment': {'sourceArtifact': {'digest': 'a' * 64}}}
        context, review = self.w.qa_context(detail, row, 'muse_decode_layer', {row['attempt_id']})
        self.assertEqual(context['code'], 'QA_CONTEXT_SOURCE_BINDING'); self.assertIsNone(review)

    def test_source_qa_legacy_strings_keep_full_text_and_explicit_identity(self):
        blockers = ['First legacy blocker. ' + 'A' * 800, 'Second legacy blocker. ' + 'B' * 900]
        row = self.bind_source_qa_blockers(blockers)
        self.w.tick(); value = next(a['qaEvidence'] for o in self.w.state['supervisions'][-1]['input']['outcomes']
                                   for a in o['artifacts'] if a['id'] == row['id'])
        self.assertEqual([r['text'] for r in value['blockingFindings']], blockers)
        self.assertEqual(value['emitterProvenance'], 'legacy_report_independence_not_established')
        self.assertTrue(all(r['identityMethod'].endswith('not_semantic_id') for r in value['blockingFindings']))
        self.assertEqual(value['runFence'], self.client.occurrences[self.seed]['attempts'][0]['fence'])
        self.bind_source_qa_blockers(['x' * 900 for _ in range(32)])
        observed = self.w.observe()
        value = next(a['qaEvidence'] for o in observed['outcomes'] for a in o['artifacts'] if a['name'] == 'qa-verification.json')
        self.assertEqual(value['code'], 'QA_CONTEXT_BLOCKERS_SIZE_EXCEEDED')

    def registered_qa_detail(self):
        self.register_completed_job()
        detail = self.client.occurrences[self.w.state['experiments'][-1]['occurrenceId']]
        artifact = next(a for a in detail['artifacts'] if a['name'] == 'qa-verification.json')
        return detail, artifact

    def test_registered_source_qa_rejects_producer_forgery_and_definition_change(self):
        detail, artifact = self.registered_qa_detail()
        latest = {artifact['attempt_id']}
        value, review = self.w.qa_context(detail, artifact, 'muse_decode_layer', latest)
        self.assertEqual(value['emitterProvenance'], 'registered_independent_qa_step')
        detail['steps'][0]['key'] = 'produce'
        value, review = self.w.qa_context(detail, artifact, 'muse_decode_layer', latest)
        self.assertEqual(value['code'], 'QA_CONTEXT_NOT_INDEPENDENT_QA_STEP'); self.assertIsNone(review)
        detail['steps'][0]['key'] = 'independent-qa'
        detail['steps'][0]['definition'] = copy.deepcopy(detail['steps'][0]['definition'])
        detail['steps'][0]['definition']['instructions'] = 'Different, unauthorized QA role.'
        value, review = self.w.qa_context(detail, artifact, 'muse_decode_layer', latest)
        self.assertEqual(value['code'], 'QA_CONTEXT_DEFINITION_BINDING'); self.assertIsNone(review)

    def test_registered_source_qa_binds_fence_and_latest_step_attempt(self):
        detail, artifact = self.registered_qa_detail()
        attempt = detail['attempts'][0]; attempt['receipt'] = None
        detail['events'] = [{'kind': 'HARNESS_STOPPED', 'attempt_id': attempt['id'], 'payload': {
            'runFence': uid(), 'confirmed': True, 'unit': 'agentos-run-' + attempt['id'],
            'proof': {'stopped': True, 'mainPid': 0, 'populated': False, 'activeState': 'inactive'}}}]
        value, review = self.w.qa_context(detail, artifact, 'muse_decode_layer', {attempt['id']})
        self.assertEqual(value['code'], 'QA_CONTEXT_STOP_UNCONFIRMED'); self.assertIsNone(review)
        detail['events'][0]['payload']['runFence'] = attempt['fence']
        value, review = self.w.qa_context(detail, artifact, 'muse_decode_layer', {attempt['id']})
        self.assertEqual(value['bindingState'], 'verified')
        detail['steps'][0]['attempt'] = 2
        value, review = self.w.qa_context(detail, artifact, 'muse_decode_layer', {attempt['id']})
        self.assertEqual(value['code'], 'QA_CONTEXT_STEP_BINDING'); self.assertIsNone(review)

    def install_source_report(self):
        detail = self.client.occurrences[self.seed]
        source = next(a for a in detail['artifacts'] if a['name'] == 'candidate.py')
        source_bytes = self.client.artifacts[source['id']]
        candidate = self.root / 'reported-candidate.py'; candidate.write_bytes(source_bytes)
        base = 'a' * 64
        detail['occurrence']['input'] = {'experiment': {'sourceArtifact': {'digest': base}}}
        self.replace_outcome_json(self.seed, 'qa-verification.json', lambda q: q.update(sourceSHA256=base))
        primary = self.root / 'primary-source.txt'; primary.write_bytes(b'Historical precision implementation.\n')
        report = {'version': 1, 'occurrenceId': self.seed, 'candidateSHA256': source['digest'], 'sourceSHA256': base,
            'nextWork': {'scope': 'DO_NOT_COPY_THIS_INSTRUCTION'}, 'qualityAccepted': True,
            'findings': [{'id': 'NUMERIC_BOUNDARY', 'fact': 'A source-specific numerical boundary must be checked.',
                          'severity': 'source_confirmed_not_gpu_tested', 'nextWork': 'DO_NOT_COPY_NESTED_INSTRUCTION'}],
            'evidence': {'candidate': {'path': str(candidate), 'sha256': W.digest(source_bytes), 'bytes': len(source_bytes)},
                         'primary': {'path': str(primary), 'sha256': W.digest(primary.read_bytes()), 'bytes': primary.stat().st_size}}}
        path = self.root / 'operator-report.json'; W.atomic(path, report)
        material = self.config['problemMaterials']['muse_decode_layer'][0]
        ref = {'path': str(path), 'sha256': W.digest(path.read_bytes()), 'kind': 'operator-reviewed-source-report',
            'problem': 'muse_decode_layer', 'occurrenceId': self.seed, 'candidateArtifactId': source['id'],
            'candidateSHA256': source['digest'], 'sourceSHA256': base,
            'rules': [{'kind': 'official_contract', 'classification': 'active_material', 'name': material['name'],
                       'sha256': material['sha256']}, {'kind': 'primary_source', 'classification': 'historical_provenance',
                       'refId': 'primary', 'sha256': report['evidence']['primary']['sha256']}]}
        self.config['sourceEvidenceRefs'] = [ref]
        return ref, report

    def test_operator_report_reaches_planner_and_worker_without_authority(self):
        ref, report = self.install_source_report()
        self.w.tick(); value = self.w.state['supervisions'][-1]['input']['sourceExternalEvidence'][0]
        self.assertEqual(value['bindingState'], 'verified')
        self.assertEqual(value['relationship'], 'exact_source_report')
        self.assertFalse(value['qualityAccepted']); self.assertFalse(value['gpuValidated'])
        self.assertNotIn('DO_NOT_COPY', W.canonical(value).decode())
        self.complete_review(); self.w.tick()
        detail = self.client.occurrences[self.w.state['experiments'][-1]['occurrenceId']]
        worker = detail['occurrence']['input']['experiment']['sourceExternalEvidence'][0]
        self.assertEqual(worker['relationship'], 'exact_source')
        self.assertEqual(worker['findings'], value['findings'])
        self.assertEqual(worker['rules'], value['rules'])
        self.assertEqual(worker['reportSHA256'], value['reportSHA256'])
        self.assertFalse(worker['currentSourceDefectsRevalidated'])

    def test_operator_report_other_source_is_history_and_other_board_omitted(self):
        self.install_source_report(); obs = self.w.observe()
        original = obs['catalog'][0]
        other = {**original, 'digest': 'b' * 64}
        values = self.w.external_evidence_for_source(obs['sourceExternalEvidence'], other)
        self.assertEqual(values[0]['relationship'], 'board_history')
        self.assertFalse(values[0]['currentSourceDefectsRevalidated'])
        self.assertEqual(self.w.external_evidence_for_source(obs['sourceExternalEvidence'], {**other, 'problem': 'muse_prefill_layer'}), [])

    def test_operator_report_wrong_hash_parent_occurrence_and_board_are_local_diagnostics(self):
        ref, report = self.install_source_report()
        original = copy.deepcopy(ref)
        for key, value in [('sha256', 'f' * 64), ('sourceSHA256', 'b' * 64), ('occurrenceId', uid()),
                           ('candidateArtifactId', uid()), ('problem', 'muse_prefill_layer')]:
            self.config['sourceEvidenceRefs'] = [{**original, key: value}]
            obs = self.w.observe(); result = obs['sourceExternalEvidence'][0]
            with self.subTest(key=key):
                self.assertEqual(result['bindingState'], 'unavailable')
                self.assertNotIn('findings', result); self.assertTrue(obs['catalog'])

    def test_operator_report_rule_hash_is_not_active_authority_without_byte_match(self):
        ref, report = self.install_source_report()
        ref['rules'][0]['sha256'] = 'b' * 64
        self.assertEqual(self.w.observe()['sourceExternalEvidence'][0]['code'], 'EXTERNAL_CONTEXT_ACTIVE_RULE_BINDING')
        material = self.config['problemMaterials']['muse_decode_layer'][0]
        ref['rules'][0]['sha256'] = material['sha256']
        pathlib.Path(material['path']).write_bytes(b'Changed current rule.\n')
        self.assertEqual(self.w.observe()['sourceExternalEvidence'][0]['code'], 'EXTERNAL_CONTEXT_ACTIVE_RULE_BYTES')

    def test_operator_report_tampered_historical_reference_not_injected(self):
        ref, report = self.install_source_report()
        pathlib.Path(report['evidence']['primary']['path']).write_bytes(b'Unverified text.\n')
        value = self.w.observe()['sourceExternalEvidence'][0]
        self.assertEqual(value['bindingState'], 'unavailable'); self.assertNotIn('findings', value)

    def test_operator_report_total_context_limit_and_oversize_are_explicit(self):
        ref, report = self.install_source_report()
        report['findings'][0]['fact'] = 'A' * 15000
        W.atomic(ref['path'], report); ref['sha256'] = W.digest(pathlib.Path(ref['path']).read_bytes())
        self.config['sourceEvidenceRefs'] = [copy.deepcopy(ref) for _ in range(8)]
        obs = self.w.observe(); rows = obs['sourceExternalEvidence']
        self.assertTrue(any(r.get('code') == 'EXTERNAL_CONTEXT_TOTAL_BYTES_EXCEEDED' for r in rows))
        self.assertLessEqual(sum(len(W.canonical(r)) for r in rows if r['bindingState'] == 'verified'), 65536)
        self.assertTrue(obs['catalog'])
        report['findings'][0]['fact'] = 'A' * 30000
        W.atomic(ref['path'], report); ref['sha256'] = W.digest(pathlib.Path(ref['path']).read_bytes())
        self.config['sourceEvidenceRefs'] = [ref]
        self.assertEqual(self.w.observe()['sourceExternalEvidence'][0]['bindingState'], 'unavailable')

    def test_operator_report_change_invalidates_only_its_board_read_set(self):
        self.install_source_report(); obs = self.w.observe(); changed = copy.deepcopy(obs)
        changed['sourceExternalEvidence'][0]['findings'][0]['fact'] = 'Updated source-specific finding.'
        before = W.action_evidence(obs, self.config); after = W.action_evidence(changed, self.config)
        self.assertNotEqual(before['boards']['muse_decode_layer'], after['boards']['muse_decode_layer'])
        self.assertEqual(before['boards']['muse_prefill_layer'], after['boards']['muse_prefill_layer'])

    def late_candidate_binding(self):
        obs = self.w.observe()
        source = next(r for r in obs['catalog'] if r['name'] == 'candidate.py')
        row = {'occurrenceId': self.seed, 'problem': 'muse_decode_layer', 'state': 'await_native',
               'code': 'NATIVE_NOT_COMPLETED', 'candidateSHA256': None, 'qaArtifactId': None, 'qaSHA256': None, 'jobId': None}
        obs['autoCheckDiagnostics']['outcomes'] = [row]
        changed = copy.deepcopy(obs); changed['autoCheckDiagnostics']['outcomes'][0]['candidateSHA256'] = source['digest']
        return obs, changed

    def test_late_adapter_binding_same_native_source_preserves_action_evidence(self):
        before, after = self.late_candidate_binding()
        snapshot = copy.deepcopy(before)
        self.assertEqual(W.action_evidence(before, self.config), W.action_evidence(after, self.config))
        self.assertEqual(before, snapshot)

    def test_late_adapter_wrong_hash_or_other_occurrence_is_material(self):
        before, after = self.late_candidate_binding()
        after['autoCheckDiagnostics']['outcomes'][0]['candidateSHA256'] = 'f' * 64
        self.assertNotEqual(W.action_evidence(before, self.config), W.action_evidence(after, self.config))
        before, after = self.late_candidate_binding()
        unrelated = uid()
        for obs in (before, after): obs['autoCheckDiagnostics']['outcomes'][0]['occurrenceId'] = unrelated
        self.assertNotEqual(W.action_evidence(before, self.config), W.action_evidence(after, self.config))

    def test_late_adapter_binding_ambiguous_or_absent_native_source_remains_strict(self):
        for mode in ['ambiguous', 'absent', 'wrong_scope', 'duplicate_outcome']:
            before, after = self.late_candidate_binding()
            for obs in (before, after):
                outcome = next(o for o in obs['outcomes'] if o['occurrenceId'] == self.seed)
                source = next(r for r in outcome['artifacts'] if r['name'] == 'candidate.py')
                if mode == 'ambiguous': outcome['artifacts'].append({**source, 'id': 'ambiguous', 'digest': 'a' * 64})
                elif mode == 'absent': outcome['artifacts'] = [r for r in outcome['artifacts'] if r['name'] != 'candidate.py']
                elif mode == 'wrong_scope': source['occurrenceId'] = 'unrelated'
                else: obs['outcomes'].append(copy.deepcopy(outcome))
            with self.subTest(mode=mode):
                self.assertNotEqual(W.action_evidence(before, self.config), W.action_evidence(after, self.config))

    def test_late_adapter_binding_does_not_hide_status_qa_or_authority_changes(self):
        for change in [{'state': 'controller_blocked'}, {'code': 'OTHER'}, {'qaArtifactId': 'new-qa'}, {'qaSHA256': 'e' * 64}]:
            before, after = self.late_candidate_binding(); after['autoCheckDiagnostics']['outcomes'][0].update(change)
            with self.subTest(change=change):
                self.assertNotEqual(W.action_evidence(before, self.config), W.action_evidence(after, self.config))
        before, after = self.late_candidate_binding(); after['autoCheckDiagnostics']['policySHA256'] = 'd' * 64
        self.assertNotEqual(W.action_evidence(before, self.config), W.action_evidence(after, self.config))

    def test_late_adapter_binding_duplicate_identical_bytes_is_not_ambiguity(self):
        before, after = self.late_candidate_binding()
        for obs in (before, after):
            outcome = next(o for o in obs['outcomes'] if o['occurrenceId'] == self.seed)
            source = next(r for r in outcome['artifacts'] if r['name'] == 'candidate.py')
            outcome['artifacts'].append({**source, 'id': 'qa-copy'})
        self.assertEqual(W.action_evidence(before, self.config), W.action_evidence(after, self.config))

    def test_scoped_fingerprint_ignores_observation_clocks_and_activity(self):
        pending, obs = self.prepared_action(); changed = copy.deepcopy(obs)
        changed['outcomes'][0]['progress'] = {'at': '2099-01-01T00:00:00Z', 'kind': 'heartbeat'}
        changed['outcomes'][0]['finishedAt'] = '2099-01-01T00:00:00Z'
        changed.update(idleSeconds=200, controllerDispatchAgeSeconds=999, fingerprint='f' * 64)
        changed['autoCheckDiagnostics']['observedAt'] = 999999
        self.assertEqual(W.action_evidence(obs, self.config), W.action_evidence(changed, self.config))
        self.assertTrue(W.unchanged_action_scope(pending, changed, self.config))

    def test_artifact_inventory_permutation_keeps_global_and_action_evidence(self):
        self.add_seed(); before = self.w.observe()
        self.w.tick(); self.complete_review()
        for detail in self.client.occurrences.values():
            detail['artifacts'].reverse()
        after = self.w.observe()
        self.assertEqual(before['fingerprint'], after['fingerprint'])
        self.assertEqual(W.action_evidence(before, self.config), W.action_evidence(after, self.config))
        result = self.w.tick()
        self.assertEqual(result['state'], 'experiments_dispatched')
        self.assertEqual(len(result['dispatched']), 1)
        self.assertEqual(len(self.client.posts), 3)
        self.reload(); self.w.tick()
        self.assertEqual(len(self.w.state['experiments']), 1)

    def test_artifact_identity_content_and_qa_change_still_invalidate_hash(self):
        before = self.w.observe(); original = copy.deepcopy(self.client.occurrences[self.seed]['artifacts'])
        for field, value in [('id', uid()), ('size', 999)]:
            after = copy.deepcopy(before); after['outcomes'][0]['artifacts'][0][field] = value
            self.assertNotEqual(W.action_evidence(before, self.config), W.action_evidence(after, self.config))
        qa = next(r for r in original if r['name'] == 'qa-verification.json')
        review = json.loads(self.client.artifacts[qa['id']]); review['passed'] = True
        self.client.complete(self.seed, {'candidate.py': b'# candidate source\n', 'qa-verification.json': review})
        self.assertNotEqual(before['fingerprint'], self.w.observe()['fingerprint'])

    def test_semantic_sequences_and_duplicate_inventory_are_not_erased(self):
        first = {'outcomes': [{'occurrenceId': 'one', 'problem': 'muse_decode_layer',
            'artifacts': [{'id': 'candidate', 'digest': 'a'}, {'id': 'qa', 'digest': 'b'}],
            'failures': [{'attemptId': 'two', 'code': 'X'}, {'attemptId': 'one', 'code': 'Y'}],
            'executionEvidence': {'artifacts': [{'id': 'result'}, {'id': 'checker'}],
                'cpu': {'failedCases': [{'name': 'first'}, {'name': 'second'}]}}}],
            'instructions': ['prepare', 'run'], 'patchHunks': ['hunk1', 'hunk2']}
        permuted = copy.deepcopy(first)
        for key in ['artifacts', 'failures']: permuted['outcomes'][0][key].reverse()
        permuted['outcomes'][0]['executionEvidence']['artifacts'].reverse()
        self.assertEqual(W.evidence_material(first), W.evidence_material(permuted))
        for change in ['cases', 'instructions', 'patch', 'duplicate']:
            altered = copy.deepcopy(first)
            if change == 'cases': altered['outcomes'][0]['executionEvidence']['cpu']['failedCases'].reverse()
            elif change == 'instructions': altered['instructions'].reverse()
            elif change == 'patch': altered['patchHunks'].reverse()
            else: altered['outcomes'][0]['artifacts'].append(copy.deepcopy(altered['outcomes'][0]['artifacts'][0]))
            self.assertNotEqual(W.evidence_material(first), W.evidence_material(altered))

    def test_known_job_owner_and_diagnostic_inventories_are_order_independent(self):
        first = {'catalog': [{'id': 'b'}, {'id': 'a'}], 'officialJobs': [{'id': 'j2'}, {'id': 'j1'}],
            'pipelineOwners': [{'problem': 'prefill', 'ownerKey': 'two', 'stages': ['gpu', 'native'],
                'occurrenceIds': ['o2', 'o1'], 'jobIds': ['j2', 'j1']}, {'problem': 'decode', 'ownerKey': 'one'}],
            'autoCheckDiagnostics': {'outcomes': [{'occurrenceId': 'o2'}, {'occurrenceId': 'o1'}],
                'repairableEvidence': [{'occurrenceId': 'o2', 'producerArtifacts': [{'id': 'b'}, {'id': 'a'}]},
                                       {'occurrenceId': 'o1', 'producerArtifacts': []}]},
            'active': [{'occurrenceId': 'o2', 'attemptIds': ['a2', 'a1']}, {'occurrenceId': 'o1', 'attemptIds': []}]}
        shuffled = copy.deepcopy(first)
        for key in ['catalog', 'officialJobs', 'pipelineOwners', 'active']: shuffled[key].reverse()
        owner = next(r for r in shuffled['pipelineOwners'] if r['ownerKey'] == 'two')
        for key in ['stages', 'occurrenceIds', 'jobIds']: owner[key].reverse()
        for key in ['outcomes', 'repairableEvidence']: shuffled['autoCheckDiagnostics'][key].reverse()
        for row in shuffled['autoCheckDiagnostics']['repairableEvidence']: row['producerArtifacts'].reverse()
        for row in shuffled['active']: row['attemptIds'].reverse()
        self.assertEqual(W.digest(W.evidence_material(first)), W.digest(W.evidence_material(shuffled)))
        for key in ['catalog', 'officialJobs']:
            changed = copy.deepcopy(first); changed[key][0]['id'] = 'different'
            self.assertNotEqual(W.evidence_material(first), W.evidence_material(changed))

    def test_partial_ack_other_board_change_recovers_unknown_run_by_get(self):
        self.add_seed(); attention = self.add_seed('muse_attention_decode', b'# independent attention\n')
        self.w.tick(); self.complete_review(self.plan(2)); self.client.timeout_after_commit_number = 5
        with self.assertRaises(TimeoutError): self.w.tick()
        self.client.complete(attention, {'candidate.py': b'# unrelated attention outcome\n'})
        self.reload(); result = self.w.tick()
        self.assertEqual(result['state'], 'experiments_dispatched')
        self.assertEqual(len(self.client.posts), 5)
        self.assertEqual(len(self.w.state['experiments']), 2)
        self.assertEqual(self.w.state['supervisions'][-1]['pipelineReservation']['acknowledged'], 1)

    def test_partial_ack_other_board_change_keeps_uncertain_run_get_only(self):
        self.add_seed(); attention = self.add_seed('muse_attention_decode', b'# independent attention\n')
        self.w.tick(); self.complete_review(self.plan(2)); self.client.timeout_before_commit_number = 5
        with self.assertRaises(TimeoutError): self.w.tick()
        self.client.complete(attention, {'candidate.py': b'# unrelated attention outcome\n'})
        self.reload()
        with self.assertRaisesRegex(ValueError, 'POST_UNCERTAIN_RECONCILE_ONLY'): self.w.tick()
        self.assertEqual(len(self.client.posts), 5)
        self.assertEqual(len(self.w.state['experiments']), 1)
        self.assertEqual(self.w.state['supervisions'][-1]['state'], 'dispatching')

    def test_empty_plan_and_unverified_wait_and_cross_scope_rejected(self):
        self.w.tick(); obs = self.w.observe()
        for edit, code in [(lambda p: p.update(experiments=[]), 'PLAN_CAPACITY'),
            (lambda p: p.update(decision='await_external', experiments=[]), 'WAIT_DEPENDENCY_UNPROVEN'),
            (lambda p: p['experiments'][0].update(problem='muse_moe'), 'EXPERIMENT_SCOPE'),
            (lambda p: p['experiments'][0].update(sourceArtifactId=uid()), 'SOURCE_SCOPE'),
            (lambda p: p['experiments'][0].update(model='other'), 'EXPERIMENT_FIELDS')]:
            p = self.plan(); edit(p)
            with self.assertRaisesRegex(ValueError, code): W.validate_plan(p, obs, self.config)

    def test_failed_qa_does_not_exhaust_official_capacity(self):
        obs = self.w.observe()
        self.assertEqual(obs['availableCandidateSlots'], 2)
        qa = next(a for a in obs['outcomes'][0]['artifacts'] if a['name'] == 'qa-verification.json')
        self.assertEqual(qa['review']['passed'], False)

    def test_actual_official_identity_and_nested_summary(self):
        self.config['officialJobs'] = [{'id': '1717a286eb2949c0', 'problem': 'muse_decode_layer', 'sourceSHA256': 'a' * 64}]
        obs = self.w.observe()
        self.assertEqual(obs['officialJobs'][0]['result']['speedup'], 1.211222)
        self.assertNotIn('logTail', obs['officialJobs'][0])
        receipt = json.loads(pathlib.Path(obs['officialJobs'][0]['receiptPath']).read_bytes())
        self.assertEqual(receipt['result']['log_tail'], 'real tests complete')

    def test_corrupted_candidate_rejected_before_any_experiment_post(self):
        self.w.tick(); self.complete_review()
        candidate = self.w.state['supervisions'][-1]['input']['catalog'][0]
        self.client.artifacts[candidate['id']] = b'corrupt'
        with self.assertRaisesRegex(ValueError, 'ARTIFACT_CORRUPT'): self.w.tick()
        self.assertEqual(len(self.client.posts), 1)

    def test_capsules_supply_actual_selected_source_not_workspace_baseline(self):
        raw = b'\n' * 950 + b'def kernel_fn(batch, length):\n    if length == 8 and batch in (16, 32):\n        return "already_gated"\n'
        self.client.complete(self.seed, {'candidate.py': raw})
        self.w.tick()
        inp = self.w.state['supervisions'][-1]['input']; cap = inp['sourceCapsules']['sources'][0]
        self.assertEqual(W.base64.b64decode(cap['base64']), raw)
        self.assertEqual(cap['sha256'], W.digest(raw))
        self.assertEqual(cap['index']['entries'][0]['startLine'], 951)
        self.assertEqual(cap['index']['entries'][0]['name'], 'kernel_fn')
        self.assertNotEqual(cap['sha256'], W.digest(b'\n' * 345 + b'# old baseline\n'))
        self.assertIn(cap['artifactIds'][0], inp['confirmedSourceArtifactIds'])
        self.complete_review(); self.assertEqual(self.w.tick()['state'], 'experiments_dispatched')
        material = self.client.posts[-1][1]['input']['experiment']['materials'][0]
        self.assertEqual(W.base64.b64decode(material['base64']), raw)

    def test_capsule_aliases_deduplicate_bytes_but_preserve_identity(self):
        self.add_seed(problem='muse_decode_layer', raw=b'# candidate source\n')
        self.w.tick(); inp = self.w.state['supervisions'][-1]['input']
        caps = inp['sourceCapsules']['sources']
        self.assertEqual(len(caps), 1); self.assertEqual(len(caps[0]['artifactIds']), 2)
        self.assertEqual(len(inp['catalog']), 2)
        self.assertEqual(inp['sourceCapsules']['rawBytes'], len(b'# candidate source\n'))

    def test_same_bytes_different_boards_have_distinct_capsules(self):
        self.add_seed(raw=b'# candidate source\n')
        self.w.tick(); caps = self.w.state['supervisions'][-1]['input']['sourceCapsules']['sources']
        self.assertEqual(len(caps), 2)
        self.assertEqual({c['problem'] for c in caps}, {'muse_decode_layer', 'muse_prefill_layer'})

    def test_capsules_only_include_eligible_terminal_sources(self):
        self.prefill_plan_while_decode_qa_runs()
        inp = self.w.state['supervisions'][-1]['input']
        self.assertEqual({c['problem'] for c in inp['sourceCapsules']['sources']}, {'muse_prefill_layer'})
        self.assertTrue(all(a['problem'] == 'muse_prefill_layer' for a in inp['catalog']))
        self.assertEqual(inp['sourceCapsules']['omitted'][0]['reason'], 'BOARD_NOT_ELIGIBLE')

    def test_capsule_corrupt_or_missing_source_omitted_without_dispatch(self):
        row = self.client.occurrences[self.seed]['artifacts'][0]
        for corruption in [b'wrong-source', None]:
            with self.subTest(corruption=corruption):
                if corruption is None: self.client.artifacts.pop(row['id'])
                else: self.client.artifacts[row['id']] = corruption
                result = self.w.tick()
                self.assertEqual(result['state'], 'supervisor_sources_unavailable')
                self.assertEqual(result['newPostRequests'], 0)
        self.assertEqual(len(self.client.posts), 0)

    def test_capsule_missing_or_wrong_bytes_cannot_authorize_plan(self):
        self.w.tick(); original = self.w.state['supervisions'][-1]['input']; plan = self.plan()
        for mutation, code in [
            (lambda x: x.pop('sourceCapsules'), 'SOURCE_CAPSULES_MISSING'),
            (lambda x: x['sourceCapsules'].update(sources=[]), 'SOURCE_NOT_PROVIDED'),
            (lambda x: x['sourceCapsules']['sources'][0].update(sha256='0' * 64), 'SOURCE_CAPSULE_BINDING'),
            (lambda x: x['sourceCapsules']['sources'][0].update(problem='muse_prefill_layer'), 'SOURCE_CAPSULE_BINDING'),
            (lambda x: x['sourceCapsules']['sources'][0].update(base64=W.base64.b64encode(b'other').decode()), 'SOURCE_CAPSULE_BYTES')]:
            inp = copy.deepcopy(original); mutation(inp)
            with self.subTest(code=code), self.assertRaisesRegex(ValueError, code):
                W.validate_plan(plan, {'fingerprint': plan['evidenceFingerprint'], **inp}, self.config)

    def test_capsule_exact_current_catalog_hash_revalidated_before_dispatch(self):
        self.w.tick(); self.complete_review()
        pending = self.w.state['supervisions'][-1]
        plan = self.plan(); pending.update(approvedPlan=plan, state='dispatching')
        obs = self.w.observe()
        obs['catalog'][0]['digest'] = '0' * 64
        with mock.patch.object(W, 'unchanged_action_scope', return_value=True):
            result = self.w.consume_plan(pending, obs)
        self.assertEqual(result['code'], 'SOURCE_CAPSULE_BINDING')
        self.assertEqual(len(self.client.posts), 1)

    def test_capsule_file_and_total_raw_budgets_are_explicit(self):
        self.client.complete(self.seed, {'candidate.py': b'#' + b'x' * W.SOURCE_FILE_LIMIT})
        self.assertEqual(self.w.tick()['omitted'][0]['reason'], 'SOURCE_FILE_LIMIT')
        self.client.complete(self.seed, {'candidate.py': b'#' + b'a' * (W.SOURCE_FILE_LIMIT - 1)})
        for letter in b'bcde': self.add_seed(problem='muse_decode_layer', raw=b'#' + bytes([letter]) * (W.SOURCE_FILE_LIMIT - 1))
        self.w.tick(); inp = self.w.state['supervisions'][-1]['input']; caps = inp['sourceCapsules']
        self.assertEqual(caps['rawBytes'], W.SOURCE_RAW_LIMIT)
        self.assertEqual(len(caps['sources']), 4)
        self.assertEqual(caps['omittedCount'], 1)
        self.assertEqual(caps['omitted'][0]['reason'], 'SOURCE_RAW_BUDGET')
        self.assertLessEqual(len(W.canonical(inp)), W.SUPERVISOR_INPUT_LIMIT)

    def test_complete_input_budget_counts_context_not_only_sources(self):
        obs = self.w.observe(); inp = {'catalog': copy.deepcopy(obs['catalog']), 'context': 'x' * (W.SUPERVISOR_INPUT_LIMIT - 1000)}
        raw = b'#' + b'a' * 2000
        row = obs['catalog'][0]; row.update(digest=W.digest(raw), size=len(raw)); inp['catalog'] = copy.deepcopy(obs['catalog'])
        self.client.artifacts[row['id']] = raw
        packet = self.w.supply_source_capsules(inp, obs)
        self.assertFalse(packet['sources']); self.assertEqual(packet['omitted'][0]['reason'], 'SOURCE_INPUT_BUDGET')
        self.assertLessEqual(len(W.canonical(inp)), W.SUPERVISOR_INPUT_LIMIT)
        inp['context'] = 'x' * W.SUPERVISOR_INPUT_LIMIT
        with self.assertRaisesRegex(ValueError, 'SUPERVISOR_INPUT_SIZE'):
            self.w.supply_source_capsules(inp, obs)

    def test_capsule_priority_uses_available_measured_source_parent_and_latest(self):
        parent = self.client.occurrences[self.seed]['artifacts'][0]
        best_oid = self.add_seed(problem='muse_decode_layer', raw=b'# measured best\n')
        best = self.client.occurrences[best_oid]['artifacts'][0]
        recent_oid = self.add_seed(problem='muse_decode_layer', raw=b'# later available\n')
        self.client.occurrences[recent_oid]['occurrence']['finished_at'] = '2026-11-01T19:00:00Z'
        obs = self.w.observe()
        obs['officialJobs'] = [
            {'id': 'known', 'problem': 'muse_decode_layer', 'sourceSHA256': best['digest'], 'status': 'done', 'result': {'passed': True, 'speedup': 1.2}},
            {'id': 'unknown-incumbent', 'problem': 'muse_decode_layer', 'sourceSHA256': 'f' * 64, 'status': 'done', 'result': {'passed': True, 'speedup': 9.9}}]
        next(o for o in obs['outcomes'] if o['occurrenceId'] == best_oid)['executionEvidence'] = {
            'bindingState': 'verified', 'candidateSHA256': best['digest'], 'sourceSHA256': parent['digest']}
        inp = {'catalog': copy.deepcopy(obs['catalog'])}; self.w.supply_source_capsules(inp, obs)
        self.assertEqual([c['selectionReason'] for c in inp['sourceCapsules']['sources']],
                         ['best_measured_available', 'known_parent', 'latest_available'])
        self.assertNotIn('f' * 64, [c['sha256'] for c in inp['sourceCapsules']['sources']])

    def test_latest_attention_uses_verified_material_time_and_supplies_its_parent(self):
        recent = self.add_seed(problem='muse_decode_layer', raw=b'# latest failed candidate\n')
        detail = self.client.occurrences[recent]
        detail['occurrence'].update(state='attention', finished_at=None)
        detail['attempts'][0].update(state='failed')
        detail['artifacts'][0]['created_at'] = '2026-11-01T19:30:00Z'
        parent = self.client.occurrences[self.seed]['artifacts'][0]
        obs = self.w.observe(); result = next(o for o in obs['outcomes'] if o['occurrenceId'] == recent)
        result['executionEvidence'] = {'bindingState': 'verified', 'candidateSHA256': detail['artifacts'][0]['digest'],
                                      'sourceSHA256': parent['digest']}
        inp = {'catalog': copy.deepcopy(obs['catalog'])}; self.w.supply_source_capsules(inp, obs)
        self.assertEqual([c['selectionReason'] for c in inp['sourceCapsules']['sources']], ['latest_available', 'latest_parent'])
        self.assertIn(detail['artifacts'][0]['id'], inp['sourceCapsules']['sources'][0]['artifactIds'])

    def test_legacy_pending_review_keeps_original_input_and_contract(self):
        self.w.tick(); row = self.w.state['supervisions'][-1]
        row['input'].pop('sourceAccessVersion'); row['input'].pop('sourceCapsules')
        self.complete_review(); self.assertEqual(self.w.tick()['state'], 'experiments_dispatched')
        self.assertEqual(len(self.client.posts), 3)

    def test_source_index_does_not_execute_or_claim_semantics(self):
        raw = b'raise RuntimeError("must not execute")\n\ndef selected():\n    return 1\n'
        index = W.source_index(raw)
        self.assertEqual(index['kind'], 'python_ast_locations_only')
        self.assertEqual(index['entries'][0]['startLine'], 3)
        self.assertEqual(W.source_index(b'def broken(:')['kind'], 'python_ast_unavailable')

    def test_capsule_inventory_is_stable_under_catalog_permutation(self):
        self.add_seed(problem='muse_decode_layer', raw=b'# second source\n')
        obs = self.w.observe(); first = {'catalog': copy.deepcopy(obs['catalog'])}
        self.w.supply_source_capsules(first, obs)
        shuffled = copy.deepcopy(obs); shuffled['catalog'].reverse(); shuffled['outcomes'].reverse()
        second = {'catalog': copy.deepcopy(shuffled['catalog'])}; self.w.supply_source_capsules(second, shuffled)
        self.assertEqual(first['sourceCapsules'], second['sourceCapsules'])

    def test_pending_request_retry_never_fetches_or_reformulates_source_capsules(self):
        self.client.timeout_after_commit_number = 1
        with self.assertRaises(TimeoutError): self.w.tick()
        original = copy.deepcopy(self.client.posts[0][1]); self.reload()
        source = self.client.occurrences[self.seed]['artifacts'][0]
        self.client.artifacts[source['id']] = b'changed after intent'
        self.assertEqual(self.w.tick()['state'], 'supervisor_dispatched')
        self.assertEqual(len(self.client.posts), 1)
        self.assertEqual(self.w.state['supervisions'][-1]['input'], original['input'])

    def test_fresh_heartbeat_does_not_hide_stale_progress_diagnostic_only(self):
        d = self.client.occurrences[self.seed]
        d['occurrence'].update(state='running', created_at='1970-01-01T00:01:00Z')
        d['attempts'][0].update(state='running', heartbeat_at='1970-01-01T02:46:40Z', receipt=None)
        result = self.w.tick()
        self.assertEqual(result['state'], 'supervisor_dispatched')
        self.assertEqual(result['trigger'], 'stale_progress_diagnostic')
        bad = self.plan()
        with self.assertRaisesRegex(ValueError, 'LIVE_OWNER_DIAGNOSIS_ONLY'):
            W.validate_plan(bad, {'fingerprint': bad['evidenceFingerprint'], **self.w.state['supervisions'][-1]['input']}, self.config)
        good = self.plan(); good.update(decision='observe_active', experiments=[],
            reason='Long tool or model call may be legitimate; preserve active owner ' + self.seed)
        self.complete_review(good)
        self.assertEqual(self.w.tick()['state'], 'active_owner_preserved')
        self.assertEqual(len(self.client.posts), 1)
        self.assertEqual(self.w.tick()['state'], 'awaiting_changed_evidence')

    def test_tool_activity_is_not_material_progress_or_permission_to_restart(self):
        d = self.client.occurrences[self.seed]
        d['occurrence'].update(state='running', created_at='1970-01-01T00:01:00Z')
        d['attempts'][0].update(state='running', receipt=None)
        d['events'] = [{'id': 99, 'kind': 'HARNESS_TOOL_EVENT', 'payload': {'type': 'tool/result'},
                        'created_at': '1970-01-01T02:46:39Z'}]
        self.assertEqual(self.w.tick()['trigger'], 'stale_progress_diagnostic')
        record = self.w.state['supervisions'][-1]
        self.assertEqual(record['input']['active'][0]['activity']['kind'], 'HARNESS_TOOL_EVENT')
        self.assertTrue(record['input']['diagnosticOnly'])

    def test_explicit_review_gate_does_not_grant_retry_authority(self):
        d = self.client.occurrences[self.seed]
        d['occurrence'].update(state='attention', created_at='1970-01-01T00:01:00Z')
        d['steps'][0].update(state='attention', definition={'kind': 'review'})
        self.assertFalse(W.confirmed_boundary(d))
        self.assertEqual(self.w.tick()['trigger'], 'stale_progress_diagnostic')

    def test_failed_delivery_can_use_exact_fenced_native_stop_event(self):
        d = self.client.occurrences[self.seed]
        d['occurrence']['state'] = 'attention'
        a = d['attempts'][0]; a.update(state='failed', receipt=None)
        d['events'] = [{'id': 100, 'kind': 'HARNESS_STOPPED', 'attempt_id': a['id'],
            'created_at': '2026-11-01T17:59:59Z', 'payload': {'confirmed': True, 'runFence': a['fence'],
            'unit': 'agentos-run-' + a['id'], 'proof': {'stopped': True, 'mainPid': 0, 'populated': False, 'activeState': 'inactive'}}}]
        self.assertTrue(W.confirmed_boundary(d))
        d['events'][0]['payload']['runFence'] = uid()
        self.assertFalse(W.confirmed_boundary(d))


    def enable_mixed_plan_contract(self):
        self.parallel_capacity()
        self.config.update(planVersion=2, maxPendingCandidates=24, maxPipelinesPerProblem=8)
        self.config['controlDefinitionSHA256'] = W.digest(W.supervisor_definition(self.config))
        self.client.cycles[self.config['controlCycleId']]['definition'] = W.supervisor_definition(self.config)
        W.validate_config(self.config)

    def mixed_plan(self, counts, repair=None):
        plan = self.parallel_plan(counts)
        plan.update(version=2, decision='actions')
        for exp in plan['experiments']:
            exp['kind'] = 'experiment'
        if repair:
            exp = next(e for e in plan['experiments'] if e['problem'] == repair['problem'])
            exp.update(kind='repair_evidence', sourceArtifactId=repair['candidateArtifactId'])
        inp = self.w.state['supervisions'][-1]['input']
        plan['capacityReview'] = [{'problem': p, 'availableSlots': n, 'plannedActions': counts.get(p, 0),
            'reason': 'Selected distinct source-bound CPU strategies; remaining capacity awaits additional discriminating evidence.'}
            for p, n in inp['boardAvailableCandidateSlots'].items()]
        return plan

    def prepare_mixed_actions(self, counts=None):
        status = self.repairable_operation()
        self.enable_mixed_plan_contract()
        self.add_seed(); self.add_seed('muse_attention_decode', b'# attention\n')
        self.install_diagnostic_fixture([status])
        self.w.tick()
        inp = self.w.state['supervisions'][-1]['input']
        repair = inp['autoCheckDiagnostics']['repairableEvidence'][0]
        return self.mixed_plan(counts or {'muse_prefill_layer': 1, 'muse_decode_layer': 1}, repair), repair

    def test_mixed_contract_is_explicit_optin_and_legacy_definition_is_unchanged(self):
        original = W.supervisor_definition(self.config)
        self.enable_mixed_plan_contract()
        self.assertNotEqual(original, W.supervisor_definition(self.config))
        self.assertEqual(original, W.supervisor_definition({**self.config, 'planVersion': 1}))
        text = W.supervisor_definition(self.config)['steps'][0]['instructions']
        self.assertIn('Mix immutable evidence repairs with new', text)
        self.assertIn('including the SAME board', text)
        self.assertNotIn('otherwise []', text)
        for value in [True, 0, 3]:
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, 'PLAN_VERSION_CONFIG'):
                W.validate_config({**self.config, 'planVersion': value})

    def test_mixed_twenty_four_actions_dispatch_repair_and_independent_work_together(self):
        plan, repair = self.prepare_mixed_actions({p: 8 for p in W.PROBLEMS})
        self.complete_review(plan)
        result = self.w.tick()
        self.assertEqual(result['state'], 'experiments_dispatched')
        self.assertEqual(len(result['dispatched']), 24)
        repairs = [r for r in result['dispatched'] if r.get('repairCandidateSHA256')]
        self.assertEqual(len(repairs), 1)
        self.assertEqual(repairs[0]['repairCandidateSHA256'], repair['candidateSHA256'])
        for r in result['dispatched']:
            definition = self.client.occurrences[r['occurrenceId']]['occurrence']['definition']
            self.assertEqual([s['key'] for s in definition['steps']], ['produce', 'independent-qa'])
            self.assertEqual(definition['allowedActions'], [])
        self.assertEqual(self.w.observe()['pipelineUsed'], 24)

    def test_mixed_action_cannot_label_an_unauthorized_source_as_repair(self):
        plan, _ = self.prepare_mixed_actions()
        plan['experiments'][0]['kind'] = 'repair_evidence'
        self.complete_review(plan)
        posts = len(self.client.posts)
        result = self.w._tick_once()
        self.assertEqual(result['code'], 'REPAIR_SCOPE_NOT_AUTHORIZED')
        self.assertEqual(len(self.client.posts), posts)

    def test_mixed_kind_and_capacity_report_are_strict(self):
        plan, _ = self.prepare_mixed_actions()
        inp = self.w.state['supervisions'][-1]['input']
        observation = {'fingerprint': plan['evidenceFingerprint'], **inp}
        changes = ['kind', 'missing_kind', 'missing_board', 'duplicate_board', 'invented_slots', 'invented_actions', 'bool_count', 'short_reason']
        for change in changes:
            bad = copy.deepcopy(plan)
            if change == 'kind': bad['experiments'][0]['kind'] = 'shell'
            elif change == 'missing_kind': del bad['experiments'][0]['kind']
            elif change == 'missing_board': bad['capacityReview'].pop()
            elif change == 'duplicate_board': bad['capacityReview'][-1] = bad['capacityReview'][0]
            elif change == 'invented_slots': bad['capacityReview'][0]['availableSlots'] += 1
            elif change == 'invented_actions': bad['capacityReview'][0]['plannedActions'] += 1
            elif change == 'bool_count': bad['capacityReview'][0]['plannedActions'] = True
            else: bad['capacityReview'][0]['reason'] = 'idle'
            with self.subTest(change=change), self.assertRaises(ValueError):
                W.validate_plan(bad, observation, self.config)

    def test_mixed_contract_does_not_accept_legacy_plan_under_new_prompt(self):
        self.enable_mixed_plan_contract(); self.w.tick()
        self.complete_review(self.plan())
        posts = len(self.client.posts)
        self.assertEqual(self.w._tick_once()['code'], 'PLAN_FIELDS')
        self.assertEqual(len(self.client.posts), posts)

    def test_mixed_coverage_uses_immutable_review_but_admission_uses_fresh_capacity(self):
        plan, _ = self.prepare_mixed_actions()
        original = self.w.state['supervisions'][-1]['input']
        observation = self.w.observe()
        observation['boardAvailableCandidateSlots']['muse_prefill_layer'] -= 1
        self.assertEqual(W.validate_plan(plan, observation, self.config,
                         expected_fingerprint=plan['evidenceFingerprint'], source_input=original), plan)
        observation['boardAvailableCandidateSlots']['muse_prefill_layer'] = 0
        with self.assertRaisesRegex(ValueError, 'PLAN_BOARD_CAPACITY'):
            W.validate_plan(plan, observation, self.config, expected_fingerprint=plan['evidenceFingerprint'], source_input=original)

    def test_mixed_repair_lost_ack_reconciles_without_duplicate_dispatch(self):
        plan, repair = self.prepare_mixed_actions()
        self.complete_review(plan)
        self.client.timeout_after_commit_number = len(self.client.posts) + 4
        with self.assertRaises(TimeoutError): self.w.tick()
        posts = len(self.client.posts)
        self.reload()
        self.assertEqual(self.w.tick()['state'], 'experiments_dispatched')
        self.assertEqual(len(self.client.posts), posts)
        owned = self.w.state['experiments'][-2:]
        self.assertEqual(len({r['occurrenceId'] for r in owned}), 2)
        self.assertEqual(owned[-1]['repairCandidateSHA256'], repair['candidateSHA256'])

    def test_mixed_changed_authority_with_lost_repair_ack_is_get_only(self):
        plan, repair = self.prepare_mixed_actions()
        self.complete_review(plan)
        self.client.timeout_after_commit_number = len(self.client.posts) + 4
        with self.assertRaises(TimeoutError): self.w.tick()
        posts = len(self.client.posts)
        self.config['maxPendingCandidates'] = 23
        self.install_diagnostic_fixture([repair])
        self.reload()
        self.assertEqual(self.w.tick()['state'], 'changed_plan_reconciled')
        self.assertEqual(len(self.client.posts), posts)
        self.assertEqual(self.w.state['experiments'][-1]['repairCandidateSHA256'], repair['candidateSHA256'])

    def test_mixed_source_scope_checks_repair_target_for_its_own_action(self):
        plan, _ = self.prepare_mixed_actions()
        pending = self.w.state['supervisions'][-1]
        pending['approvedPlan'] = plan
        fresh = self.w.observe()
        self.assertTrue(W.unchanged_action_scope(pending, fresh, self.config))
        fresh['autoCheckDiagnostics']['repairableEvidence'][0]['qaSHA256'] = 'f' * 64
        self.assertFalse(W.unchanged_action_scope(pending, fresh, self.config))


if __name__ == '__main__':
    unittest.main(verbosity=2)
