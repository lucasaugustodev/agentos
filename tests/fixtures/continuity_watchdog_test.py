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
        d['steps'] = [{'id': sid, 'attempt': 1, 'state': state, 'definition': {'kind': 'agent'}}]
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
        sid, aid = uid(), uid()
        detail['steps'].append({'id': sid, 'attempt': 1, 'state': 'completed', 'definition': {'kind': 'agent'}})
        detail['attempts'].append({'id': aid, 'step_id': sid, 'number': 1, 'state': 'completed',
            'finished_at': '2026-11-01T18:00:00Z', 'fence': uid(), 'receipt': {'stopped': True}})
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
        with self.assertRaisesRegex(ValueError, 'REPAIR_ARTIFACT_BINDING'): self.w.observe()
        self.install_diagnostic_fixture([status])
        detail = self.client.occurrences[status['occurrenceId']]
        qa = next(a for a in detail['artifacts'] if a['id'] == status['qaArtifactId'])
        value = json.loads(self.client.artifacts[qa['id']]); value['sourceSHA256'] = 'f' * 64
        raw = W.canonical(value); self.client.artifacts[qa['id']] = raw; qa.update(digest=W.digest(raw), size=len(raw))
        self.install_diagnostic_fixture([{**status, 'qaSHA256': qa['digest']}])
        with self.assertRaisesRegex(ValueError, 'REPAIR_SOURCE_LINEAGE'): self.w.observe()

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
        self.assertEqual(self.w.tick()['state'], 'supervisor_plan_rejected')
        self.assertEqual(self.w.tick()['state'], 'supervisor_dispatched')
        self.assertEqual(self.w.state['supervisions'][-1]['input']['priorReviewErrors'], ['PLAN_FIELDS'])
        self.complete_review({})
        self.w.tick()
        self.assertEqual(self.w.tick()['state'], 'supervisor_failure_needs_repair')
        self.assertEqual(len(self.client.posts), 2)

    def test_stale_plan_superseded_not_applied(self):
        self.w.tick(); self.complete_review()
        self.client.complete(self.seed, {'candidate.py': b'# changed material'})
        self.assertEqual(self.w.tick()['state'], 'stale_plan_superseded')
        self.assertEqual(self.w.tick()['state'], 'supervisor_dispatched')

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
        self.assertEqual(self.w.tick()['state'], 'stale_plan_superseded')
        self.assertEqual(len(self.client.posts), 1)

    def test_model_cannot_supply_action_evidence(self):
        self.w.tick(); plan = self.plan(); plan['actionEvidence'] = self.w.state['supervisions'][-1]['actionEvidence']
        self.complete_review(plan)
        self.assertEqual(self.w.tick()['code'], 'PLAN_FIELDS')
        self.assertEqual(len(self.client.posts), 1)

    def test_same_board_failure_or_source_change_invalidates_plan(self):
        prefill = self.prefill_plan_while_decode_qa_runs()
        self.client.complete(prefill, {'candidate.py': b'# new prefill source\n',
            'qa-verification.json': {'passed': False, 'findings': ['precision changed']}}, state='attention')
        self.assertEqual(self.w.tick()['state'], 'stale_plan_superseded')
        self.assertEqual(len(self.client.posts), 1)

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


if __name__ == '__main__':
    unittest.main(verbosity=2)
