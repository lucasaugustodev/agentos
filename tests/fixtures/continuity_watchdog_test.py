"""Server-only deterministic qualification of the periodic supervisor controller."""
import copy
import importlib.util
import json
import pathlib
import tempfile
import unittest
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
        self.occurrences, self.artifacts, self.posts = {}, {}, []
        self.timeout_after_commit_number = None
        self.timeout_before_commit_number = None

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
        return {'id': job['id'], 'board': job['problem'], 'status': 'done',
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
        source = review['input']['catalog'][0]
        return {'version': 1, 'evidenceFingerprint': review['fingerprint'], 'decision': 'experiments',
            'reason': 'Measured bottleneck requires a discriminating CPU experiment.', 'experiments': [
                {'id': 'experiment-' + str(i), 'problem': 'muse_decode_layer', 'sourceArtifactId': source['id'],
                 'hypothesis': 'A different tile size may reduce launch overhead while preserving arithmetic.',
                 'instructions': 'Change only tile configuration and add a dispatch truth table with a negative control.'} for i in range(n)]}

    def complete_review(self, plan=None):
        self.client.complete(self.w.state['supervisions'][-1]['occurrenceId'], {'continuation-plan.json': self.plan() if plan is None else plan})

    def reload(self):
        self.w = W.Watchdog(self.config, self.root / 'state', self.client, clock=lambda: 10060)

    def test_recurrent_supervisor_dispatch_completion_new_supervisor(self):
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
        self.w.tick(); self.complete_review(self.plan(2))
        self.client.timeout_after_commit_number = 5  # supervisor + first(create/run) + second(create/run)
        with self.assertRaises(TimeoutError): self.w.tick()
        self.assertEqual(self.w.state['supervisions'][-1]['state'], 'dispatching')
        self.reload()
        self.assertEqual(self.w.tick()['state'], 'experiments_dispatched')
        self.assertEqual(len(self.client.posts), 5)
        self.assertEqual(len(self.w.state['experiments']), 2)

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
        self.assertEqual(obs['outcomes'][0]['artifacts'][1]['review']['passed'], False)

    def test_actual_official_identity_and_nested_summary(self):
        self.config['officialJobs'] = [{'id': '1717a286eb2949c0', 'problem': 'muse_decode_layer', 'sourceSHA256': 'a' * 64}]
        obs = self.w.observe()
        self.assertEqual(obs['officialJobs'][0]['result']['speedup'], 1.211222)
        self.assertEqual(obs['officialJobs'][0]['logTail'], 'real tests complete')

    def test_corrupted_candidate_rejected_before_any_experiment_post(self):
        self.w.tick(); self.complete_review()
        candidate = self.w.state['supervisions'][-1]['input']['catalog'][0]
        self.client.artifacts[candidate['id']] = b'corrupt'
        with self.assertRaisesRegex(ValueError, 'ARTIFACT_CORRUPT'): self.w.tick()
        self.assertEqual(len(self.client.posts), 1)

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
