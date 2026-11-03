"""Opt-in native dependency evidence applies only to new QA step definitions."""
import copy
import importlib.util
import json
import pathlib
import subprocess
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location('native_qa_v2_base',
    pathlib.Path(__file__).with_name('continuity_watchdog_test.py'))
B = importlib.util.module_from_spec(spec); spec.loader.exec_module(B); W = B.W


class NativeQAV2Tests(unittest.TestCase):
    def setUp(self):
        self.f = B.WatchTests(); self.f.setUp(); self.addCleanup(self.f.tearDown)
        self.supervisor = {'occurrenceId': B.uid(), 'approvedPlan': {'version': 1, 'decision': 'experiments'}}

    def example(self, repair=False):
        if repair:
            self.f.repairable_operation()
        observation = self.f.w.observe()
        source = (observation['autoCheckDiagnostics']['repairableEvidence'][0]['candidateArtifactId']
                  if repair else observation['catalog'][0]['id'])
        experiment = {'id': 'qa-contract-example', 'problem': 'muse_decode_layer', 'sourceArtifactId': source,
            'hypothesis': 'Preserve candidate arithmetic and use independently bound replay evidence.',
            'instructions': 'Execute discriminating CPU checks and independent QA without any official action.'}
        self.supervisor['approvedPlan']['decision'] = 'repair_evidence' if repair else 'experiments'
        return experiment, observation

    def capture(self, experiment, observation):
        requests = []
        def post(kind, key, body, cycle=None):
            requests.append((kind, key, copy.deepcopy(body), cycle))
            if kind == 'create': return {'id': '11111111-1111-4111-8111-111111111111'}
            return {'id': '22222222-2222-4222-8222-222222222222', 'cycle_id': cycle, 'input': copy.deepcopy(body['input'])}
        old_state = copy.deepcopy(self.f.w.state)
        with mock.patch.object(self.f.w, 'post_once', post):
            self.f.w.dispatch_experiment(experiment, observation, self.supervisor)
        self.f.w.state = old_state
        return requests

    def test_default_and_explicit_v1_definition_and_input_are_byte_identical(self):
        experiment, observation = self.example()
        absent = self.capture(experiment, observation)
        self.f.config['nativeQAContractVersion'] = 1
        explicit = self.capture(experiment, observation)
        self.assertEqual(W.canonical(absent), W.canonical(explicit))
        steps = absent[0][2]['definition']['steps']
        self.assertEqual([s['maxAttempts'] for s in steps], [1, 1])
        self.assertTrue(all('dependencyEvidence' not in s for s in steps))
        self.assertTrue(all('nativeQAContractVersion' not in s['parameters'] for s in steps))

    def test_invalid_versions_are_rejected_before_dispatch(self):
        for value in [None, False, True, 0, 4, '2', 2.0, {}, []]:
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, 'NATIVE_QA_CONTRACT_VERSION'):
                W.validate_config({**self.f.config, 'nativeQAContractVersion': value})
        for value in [1, 2]: W.validate_config({**self.f.config, 'nativeQAContractVersion': value})
        self.assertEqual(self.f.client.posts, [])

    def test_v2_only_changes_qa_marker_bound_fields_and_retry_limit(self):
        experiment, observation = self.example()
        legacy = self.capture(experiment, observation)
        self.f.config['nativeQAContractVersion'] = 2
        new = self.capture(experiment, observation)
        old_steps = legacy[0][2]['definition']['steps']; steps = new[0][2]['definition']['steps']
        self.assertEqual(steps[0], old_steps[0]); self.assertEqual(steps[0]['maxAttempts'], 1)
        qa = copy.deepcopy(steps[1]); self.assertEqual(qa.pop('maxAttempts'), 3)
        self.assertEqual(qa['parameters'].pop('nativeQAContractVersion'), 2)
        self.assertEqual(qa.pop('dependencyEvidence'), {'version': 1, 'sourceStep': 'produce', 'bindings': [
            {'reportArtifact': 'qa-verification.json', 'jsonPointer': '/cpuEvidence/checkerSHA256', 'dependencyArtifact': 'cpu-checks.py'},
            {'reportArtifact': 'qa-verification.json', 'jsonPointer': '/cpuEvidence/resultSHA256', 'dependencyArtifact': 'cpu-result.json'}]})
        qa['maxAttempts'] = 1
        self.assertEqual(qa, old_steps[1])
        self.assertEqual(new[1], legacy[1])
        self.assertEqual(new[0][2]['definition']['allowedActions'], [])

    def test_qa_contract_alone_keeps_supervisor_control_but_repair_first_changes_it(self):
        config = copy.deepcopy(self.f.config)
        original = W.canonical(W.supervisor_definition(config))
        config['nativeQAContractVersion'] = 2
        self.assertEqual(W.canonical(W.supervisor_definition(config)), original)
        self.f.enable_mixed_plan_contract()
        config = {**self.f.config, 'plannerMode': 'board-lanes-v1', 'targetNativeWorkers': 8, 'readyLowWatermark': 2}
        normal = W.canonical(W.supervisor_definition(config))
        config['nativeQAContractVersion'] = 2
        self.assertEqual(W.canonical(W.supervisor_definition(config)), normal)
        config['repairFirstPlanning'] = True
        self.assertNotEqual(W.canonical(W.supervisor_definition(config)), normal)

    def test_repair_v2_preserves_exact_materials_and_immutable_candidate_criteria(self):
        experiment, observation = self.example(repair=True)
        legacy = self.capture(experiment, observation)
        self.f.config['nativeQAContractVersion'] = 2
        new = self.capture(experiment, observation)
        self.assertEqual(new[1], legacy[1])
        for prior, current in zip(legacy[0][2]['definition']['steps'], new[0][2]['definition']['steps']):
            self.assertEqual(prior['criteria'], current['criteria'])
            self.assertTrue(any(c['id'] == 'immutable-repair-candidate' for c in current['criteria']))
        self.assertEqual(new[0][2]['definition']['steps'][1]['dependencyEvidence']['sourceStep'], 'produce')

    def test_new_v2_occurrence_does_not_rewrite_old_run_or_durable_receipts(self):
        experiment, observation = self.example()
        old = self.f.w.dispatch_experiment(experiment, observation, self.supervisor)
        old_detail = W.canonical(self.f.client.occurrences[old['occurrenceId']])
        old_intents = {p.name:p.read_bytes() for p in self.f.w.directory.glob('intent-*.json')}
        self.f.config['nativeQAContractVersion'] = 2
        new = self.f.w.dispatch_experiment({**experiment, 'id': 'new-v2'}, observation, self.supervisor)
        self.assertEqual(W.canonical(self.f.client.occurrences[old['occurrenceId']]), old_detail)
        self.assertEqual({n:(self.f.w.directory/n).read_bytes() for n in old_intents}, old_intents)
        self.assertEqual(self.f.client.occurrences[new['occurrenceId']]['occurrence']['definition']['steps'][1]['maxAttempts'], 3)
        self.assertEqual(self.f.client.occurrences[old['occurrenceId']]['occurrence']['definition']['steps'][1]['maxAttempts'], 1)

    def test_lost_legacy_create_ack_reconciles_by_get_after_v2_optin(self):
        experiment, observation = self.example()
        self.f.client.timeout_after_commit_number = 1
        with self.assertRaises(TimeoutError): self.f.w.dispatch_experiment(experiment, observation, self.supervisor)
        marker = next(self.f.w.directory.glob('intent-*.json')); before = W.read_json(marker)
        self.f.config['nativeQAContractVersion'] = 2
        result = self.f.w.reconcile_existing_post('create', before['requestKey'])
        self.assertEqual(len(self.f.client.posts), 1)
        self.assertEqual(result['definition']['steps'][1]['maxAttempts'], 1)
        self.assertEqual(W.read_json(marker)['requestDigest'], before['requestDigest'])

    def test_v2_requests_survive_actual_node_json_roundtrip_without_hash_or_type_drift(self):
        experiment, observation = self.example(); self.f.config['nativeQAContractVersion'] = 2
        requests = self.capture(experiment, observation)
        for kind, key, body, cycle in requests:
            normalized = W.request_numbers_v1(body)
            result = subprocess.run(['node', '-e', 'let s="";process.stdin.on("data",b=>s+=b);process.stdin.on("end",()=>process.stdout.write(JSON.stringify(JSON.parse(s))));'],
                input=W.canonical(normalized), capture_output=True, check=True, timeout=10)
            returned = json.loads(result.stdout)
            self.assertEqual(W.digest(returned), W.digest(normalized))
            self.assertEqual(returned, normalized)
        definition = requests[0][2]['definition']
        self.assertIs(type(definition['steps'][1]['parameters']['nativeQAContractVersion']), int)
        self.assertIs(type(definition['steps'][1]['dependencyEvidence']['version']), int)


if __name__ == '__main__': unittest.main()
