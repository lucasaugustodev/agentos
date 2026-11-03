"""New NativeQA v3 definitions keep immutable legacy dispatch contracts.

Golden definitions were independently captured from cf9120f on the isolated
development server. Only the synthetic random account ID was normalized.
"""
import copy
import base64
import datetime
import difflib
import importlib.util
import pathlib
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location('native_qa_v3_legacy_fixture',
    pathlib.Path(__file__).with_name('continuity_watchdog_native_qa_v2_test.py'))
V2 = importlib.util.module_from_spec(spec); spec.loader.exec_module(V2)
B, W = V2.B, V2.W

LEGACY_DEFINITION_SHA256 = {
    (False, 1): '470a1703d1a91dc618c2e976b2bc5ee3122bd0bae5caf7483d07b916fa4da8da',
    (False, 2): '66ce9249db51d7c64ec5ebcf5fc5df423c13a07e023375bf75dc84866dab3a58',
    (True, 1): 'b69de6fefaf288131ee9bc810ed4d46df720aeff7cbe491b1e73d195c097a79f',
    (True, 2): 'ae8b5dbdaccfc84abb91e28c986b81a2ed292f62313b9e4b250c06010434f7f3',
}


class NativeQAV3Tests(unittest.TestCase):
    def setUp(self):
        self.legacy = V2.NativeQAV2Tests(); self.legacy.setUp()
        self.addCleanup(self.legacy.doCleanups)
        self.f = self.legacy.f

    def pair(self, repair=False):
        self.f.config['nativeQAContractVersion'] = 2
        exp, observation = self.legacy.example(repair)
        before = self.legacy.capture(exp, observation)
        self.f.config['nativeQAContractVersion'] = 3
        after = self.legacy.capture(exp, observation)
        return exp, observation, before, after

    def test_legacy_definitions_match_independent_prechange_bytes(self):
        for repair in [False, True]:
            for version in [None, 1, 2]:
                with self.subTest(repair=repair, version=version):
                    f = V2.NativeQAV2Tests(); f.setUp()
                    try:
                        if version is not None: f.f.config['nativeQAContractVersion'] = version
                        exp, obs = f.example(repair)
                        definition = copy.deepcopy(f.capture(exp, obs)[0][2]['definition'])
                        definition['allowedAccounts'] = ['synthetic-account']
                        for step in definition['steps']: step['accountId'] = 'synthetic-account'
                        self.assertEqual(W.digest(definition), LEGACY_DEFINITION_SHA256[(repair, version or 1)])
                    finally:
                        f.doCleanups()

    def test_v3_is_strict_opt_in_without_changing_supervisor_definition(self):
        original = W.canonical(W.supervisor_definition(self.f.config))
        for version in [1, 2, 3]:
            config = {**self.f.config, 'nativeQAContractVersion': version}
            W.validate_config(config)
            self.assertEqual(W.canonical(W.supervisor_definition(config)), original)
        for invalid in [None, True, False, 0, 4, '3', 3.0, {}, []]:
            with self.subTest(invalid=invalid), self.assertRaisesRegex(ValueError, 'NATIVE_QA_CONTRACT_VERSION'):
                W.validate_config({**self.f.config, 'nativeQAContractVersion': invalid})
        self.assertEqual(self.f.client.posts, [])

    def test_v3_declares_one_qa_attempt_and_controller_replay_contract(self):
        _, _, before, after = self.pair()
        old = before[0][2]['definition']; definition = after[0][2]['definition']
        producer, qa = definition['steps']
        self.assertEqual(definition['allowedActions'], ['native.cpu-replay.observe'])
        self.assertEqual([producer['maxAttempts'], qa['maxAttempts']], [1, 1])
        self.assertEqual(qa['dependsOn'], ['produce'])
        self.assertEqual(qa['parameters'], {'nativeQAContractVersion': 3,
            'cpuReplayObservationVersion': 1, 'cpuReplayProfile': 'python-cpu-candidate-base-v1'})
        self.assertEqual(qa['dependencyEvidence'], {'version': 2, 'kind': 'controller-cpu-replay',
            'sourceStep': 'produce', 'reportArtifact': 'qa-verification.json'})
        self.assertEqual(producer['parameters'], old['steps'][0]['parameters'])
        self.assertNotIn('dependencyEvidence', producer)
        # The producer's ABI instructions change, while its required outputs,
        # scope criteria, account, workspace and existing permissions do not.
        p, q = copy.deepcopy(producer), copy.deepcopy(old['steps'][0])
        self.assertNotEqual(p.pop('instructions'), q.pop('instructions'))
        self.assertEqual(p, q)

    def test_semantic_report_does_not_require_model_copied_technical_attestations(self):
        _, _, _, after = self.pair()
        definition = after[0][2]['definition']; producer, qa = definition['steps']
        self.assertNotIn('sha256sum ABS_CHECKER ABS_CANDIDATE && python3', producer['instructions'])
        self.assertNotIn('sha256sum ABS_CHECKER ABS_CANDIDATE && python3', qa['instructions'])
        self.assertNotIn('cpuEvidence containing', qa['instructions'])
        self.assertNotIn('COMPLETE cases and negativeControls arrays', qa['instructions'])
        fields = {c['field']: c['equals'] for c in qa['criteria'] if c.get('type') == 'json_field'}
        self.assertEqual(fields.get('version'), 3)
        self.assertEqual(fields.get('decision'), 'approve_for_check')
        self.assertFalse(set(fields) & {'passed', 'cpuReplayPassed', 'approvedForOfficialReview',
            'candidateSHA256', 'sourceSHA256', 'cpuEvidence.checkerSHA256', 'cpuEvidence.resultSHA256'})
        self.assertEqual(fields.get('gpuValidated'), False)
        self.assertEqual(fields.get('performanceGainProven'), False)
        self.assertEqual(fields.get('officialActionsPerformed'), False)

    def test_v3_preserves_exact_input_materials_and_repair_candidate_constraints(self):
        for repair in [False, True]:
            with self.subTest(repair=repair):
                _, _, before, after = self.pair(repair)
                self.assertEqual(W.canonical(before[1]), W.canonical(after[1]))
                if repair:
                    for old, new in zip(before[0][2]['definition']['steps'][:1], after[0][2]['definition']['steps'][:1]):
                        old_immutable = [c for c in old['criteria'] if c['id'] == 'immutable-repair-candidate']
                        self.assertTrue(old_immutable)
                        self.assertEqual(old_immutable, [c for c in new['criteria'] if c['id'] == 'immutable-repair-candidate'])
                    self.assertNotIn('copy its full cases/negativeControls', after[0][2]['definition']['steps'][1]['instructions'])

    def test_new_v3_does_not_rewrite_old_definition_attempt_policy_or_intents(self):
        exp, obs = self.legacy.example()
        self.f.config['nativeQAContractVersion'] = 2
        old = self.f.w.dispatch_experiment(exp, obs, self.legacy.supervisor)
        old_detail = W.canonical(self.f.client.occurrences[old['occurrenceId']])
        old_intents = {p.name: p.read_bytes() for p in self.f.w.directory.glob('intent-*.json')}
        self.f.config['nativeQAContractVersion'] = 3
        new = self.f.w.dispatch_experiment({**exp, 'id': 'new-v3'}, obs, self.legacy.supervisor)
        self.assertEqual(W.canonical(self.f.client.occurrences[old['occurrenceId']]), old_detail)
        self.assertEqual({n: (self.f.w.directory/n).read_bytes() for n in old_intents}, old_intents)
        self.assertEqual(self.f.client.occurrences[old['occurrenceId']]['occurrence']['definition']['steps'][1]['maxAttempts'], 3)
        self.assertEqual(self.f.client.occurrences[new['occurrenceId']]['occurrence']['definition']['steps'][1]['maxAttempts'], 1)

    def test_lost_v2_create_ack_reconciles_unchanged_after_v3_opt_in(self):
        exp, obs = self.legacy.example(); self.f.config['nativeQAContractVersion'] = 2
        self.f.client.timeout_after_commit_number = 1
        with self.assertRaises(TimeoutError): self.f.w.dispatch_experiment(exp, obs, self.legacy.supervisor)
        marker = next(self.f.w.directory.glob('intent-*.json')); before = W.read_json(marker)
        self.f.config['nativeQAContractVersion'] = 3
        result = self.f.w.reconcile_existing_post('create', before['requestKey'])
        self.assertEqual(len(self.f.client.posts), 1)
        self.assertEqual(result['definition']['steps'][1]['parameters']['nativeQAContractVersion'], 2)
        self.assertEqual(W.read_json(marker)['requestDigest'], before['requestDigest'])


class NativeQAV3EvidenceTests(unittest.TestCase):
    # Synthetic protocol fixture; the separate core/launcher replay qualifies
    # interoperability. This fixture never invokes a provider or executor.
    def setUp(self):
        NativeQAV3Tests.setUp(self)
        self.f.config['nativeQAContractVersion'] = 3
        exp, obs = self.legacy.example()
        sent = self.legacy.capture(exp, obs)
        definition = sent[0][2]['definition']; inp = sent[1][2]['input']
        oid, pid, qid, psid, qsid = [B.uid() for _ in range(5)]
        self.detail = {'occurrence': {'id': oid, 'cycle_id': B.uid(), 'version': 1, 'state': 'completed',
            'definition': definition, 'input': inp, 'finished_at': '1970-01-01T02:46:40Z'},
            'steps': [{'id': sid, 'key': key, 'attempt': 1, 'state': 'completed', 'definition': definition['steps'][i]}
                for i, (sid, key) in enumerate([(psid, 'produce'), (qsid, 'independent-qa')])],
            'attempts': [{'id': aid, 'step_id': sid, 'number': 1, 'state': 'completed', 'fence': B.uid(),
                'finished_at': '1970-01-01T02:46:40Z', 'receipt': {'stopped': True}, 'context': {}}
                for aid, sid in [(pid, psid), (qid, qsid)]], 'artifacts': [], 'events': []}
        producer, qa = self.detail['attempts']; self.qa = qa
        self.source = {'stepId': psid, 'key': 'produce', 'attemptId': pid, 'attemptNumber': 1, 'runFence': producer['fence']}
        self.refs = []
        base_raw = self.f.client.artifacts[inp['experiment']['sourceArtifact']['id']]
        patch_raw = ''.join(difflib.unified_diff(base_raw.decode().splitlines(True), 'candidate = 2\n'.splitlines(True),
            fromfile='base-candidate.py', tofile='candidate.py')).encode()
        for name, raw in [('candidate.py', b'candidate = 2\n'), ('cpu-checks.py', b'print("checks")\n'),
            ('cpu-result.json', b'{"passed":true}'), ('implementation.patch', patch_raw)]:
            self.refs.append(self.artifact(name, raw, pid))
        verification = self.artifact('verification.json', W.canonical({'passed': True, 'scopePreserved': True,
            'sourceSHA256': W.digest(base_raw), 'candidateSHA256': self.refs[0]['digest'],
            'gpuValidated': False, 'officialActionsPerformed': False}), pid)
        self.report = {'version': 3, 'decision': 'approve_for_check', 'scopePreserved': True,
            'canonicalPrecisionPreserved': True, 'evaluatorIndependent': True, 'upstreamAttributionPreserved': True,
            'checkerReview': {'casesMeaningful': True, 'negativeControlsDiscriminating': True,
                'noTautologicalPass': True, 'rationale': 'Independent semantic examination.'},
            'blockingFindings': [], 'findings': [], 'limitations': ['CPU only.'],
            'gpuValidated': False, 'performanceGainProven': False, 'officialActionsPerformed': False}
        self.qa_ref = self.artifact('qa-verification.json', W.canonical(self.report), qid)
        executor = self.f.root/'executor.py'; executor.write_bytes(b'# synthetic protected executor identity\n')
        reg = self.f.root/'registration.json'
        registration = {'version': 1, 'enabled': True, 'executorPath': str(executor), 'executorSHA256': W.digest(executor.read_bytes()),
            'projects': [definition['project']], 'profiles': ['python-cpu-candidate-base-v1'], 'limits': {'maxOutputBytes': 262144}}
        W.atomic(reg, registration)
        self.registration = {'path': str(reg), 'digest': W.digest(reg.read_bytes()), 'executorPath': str(executor),
            'executorSHA256': registration['executorSHA256'], 'profile': 'python-cpu-candidate-base-v1'}
        snapshot = {'version': 1, 'contractSHA256': W.digest([2, 'controller-cpu-replay', 'produce', 'qa-verification.json']),
            'occurrenceId': oid, 'consumer': {'stepId': qsid, 'key': 'independent-qa', 'attemptId': qid,
                'attemptNumber': 1, 'runFence': qa['fence']}, 'source': self.source,
            'artifacts': sorted(self.refs + [verification], key=lambda r: (r['name'], r['id']))}
        base = {k: inp['experiment']['sourceArtifact'][k] for k in ['id', 'name', 'digest', 'size']}
        request = {'version': 1, 'requestId': qid, 'occurrenceId': oid, 'runFence': qa['fence'], 'definitionVersion': 1,
            'project': definition['project'], 'source': self.source, 'profile': self.registration['profile'],
            'baseSource': {'artifact': base, 'source': {'occurrenceId': self.f.seed, 'attemptId': B.uid(), 'runFence': B.uid()},
                'inputSHA256': W.digest(inp)}, 'registrationSHA256': self.registration['digest'],
            'executorSHA256': self.registration['executorSHA256'], 'artifacts': self.refs + [{**base, 'name': 'base-candidate.py'}]}
        request_sha = W.digest({**request, 'artifacts': [{**r, 'base64': base64.b64encode(self.f.client.artifacts[r['id']]).decode()} for r in request['artifacts']]})
        unit = 'agentos-cpu-replay-' + qid + '.service'; stop = {'stopped': True, 'mainPID': 0, 'populated': False, 'unit': unit}
        cpu = {'passed': True, 'candidateSHA256': self.refs[0]['digest'], 'checkerSHA256': self.refs[1]['digest'],
            'cases': [{'name': 'case', 'passed': True}], 'negativeControls': [{'name': 'control', 'detected': True}]}
        def output(raw): return {'base64': base64.b64encode(raw).decode(), 'size': len(raw), 'digest': W.digest(raw)}
        receipt = {k: request[k] for k in ['requestId', 'occurrenceId', 'runFence', 'source', 'project', 'profile', 'baseSource',
            'registrationSHA256', 'executorSHA256']}
        receipt.update(version=1, kind='cpu-replay-observation', observationOnly=True, acceptance=False, requestSHA256=request_sha,
            unit=unit, stopProof=stop, argv=['/usr/bin/python3', '-I', '-B', '/output/cpu-checks.py', '/output/candidate.py', '/inputs/base-candidate.py'],
            exitCode=0, signal=None, outputsComplete=True, timedOut=False, oomKilled=False, cancelled=False, outputLimitExceeded=False,
            outputs={'stdout': output(b''), 'stderr': output(b''), 'result': output(W.canonical(cpu))})
        receipt_sha = W.digest(receipt); method = 'cpu-replay-ascii-integers-v1'
        handoff = {'version': 1, 'contractVersion': 3, 'requestId': qid, 'requestSHA256': request_sha, 'receiptSHA256': receipt_sha,
            'receiptCanonicalization': method, 'registration': self.registration, 'receipt': {'id': qid, 'name': 'cpu-replay-observation.json',
                'digest': receipt_sha, 'size': len(W.canonical(receipt)), 'path': '/home/agent/inputs/'+qid+'/cpu-replay-observation.json'}}
        validation = {'version': 1, 'contractVersion': 3, 'status': 'matched', 'code': None, 'snapshotContractSHA256': snapshot['contractSHA256'],
            'source': self.source, 'requestSHA256': request_sha, 'receiptSHA256': receipt_sha, 'receiptCanonicalization': method,
            'handoff': handoff, 'semanticReport': self.qa_ref, 'semanticDecision': self.report['decision'],
            'candidateExecutedByController': True, 'gpuValidated': False, 'performanceGainProven': False}
        qa['context'] = {'cpuReplayPreparation': {'phase': 'qa_dispatching', 'snapshot': snapshot,
            'registration': self.registration, 'requestSHA256': request_sha}, 'manifest': {'dependencyEvidence': snapshot, 'nativeQA3': handoff},
            'nativeQA3Validation': validation}
        for a, key in [(producer, 'produce'), (qa, 'independent-qa')]:
            a['context'].setdefault('manifest', {}).update(cycleId=self.detail['occurrence']['cycle_id'], occurrenceId=oid, step=key,
                cycleInput={'attemptId': a['id'], 'occurrenceId': oid, 'runFence': a['fence'], 'digest': W.digest(inp)})
        self.evidence = {'version': 1, 'kind': 'native-qa3-controller-evidence', 'state': 'evidence', 'occurrenceId': oid,
            'attemptId': qid, 'runFence': qa['fence'], 'definitionVersion': 1, 'registration': self.registration,
            'request': request, 'requestSHA256': request_sha, 'requestCanonicalization': method, 'snapshot': snapshot,
            'action': {'key': oid+':'+qid+':cpu-replay-v1', 'state': 'completed', 'inputHash': request_sha},
            'modelDispatched': True, 'receipt': receipt, 'receiptSHA256': receipt_sha, 'receiptCanonicalization': method,
            'physicalConfirmed': True, 'physicalInspection': {'version': 1, 'observedAt': '1970-01-01T02:46:40Z', 'receiptSHA256': receipt_sha, 'stopProof': stop},
            'handoff': handoff, 'semanticReport': self.qa_ref, 'validation': validation}
        config = self.f.root/'qa3-config.json'; policy = self.f.root/'qa3-policy.json'
        self.f.config['autoCheckPolicyPath'] = str(policy); W.atomic(config, self.f.config)
        W.atomic(policy, {'watchId': self.f.config['watchId'], 'controlCycleId': self.f.config['controlCycleId'],
            'watcherCode': {'path': str(B.SOURCE), 'sha256': W.digest(B.SOURCE.read_bytes())},
            'watcherConfig': {'path': str(config), 'sha256': W.digest(config.read_bytes())},
            'nativeQA3Registrations': {'version': 1, 'registrations': [self.registration]}})
        old = self.f.client.request
        def request(method, route, body=None, binary=False):
            if route.endswith('/cpu-replay'):
                self.assertEqual(method, 'GET'); self.f.client.gets.append(route); return copy.deepcopy(self.evidence)
            return old(method, route, body, binary)
        self.f.client.request = request

    def artifact(self, name, raw, aid):
        ref = {'id': B.uid(), 'name': name, 'size': len(raw), 'digest': W.digest(raw)}
        self.f.client.artifacts[ref['id']] = raw
        self.detail['artifacts'].append({**ref, 'attempt_id': aid})
        return ref

    def read(self): return self.f.w.native_qa3_evidence(self.detail, self.qa)

    def install_owned(self):
        oid = self.detail['occurrence']['id']
        self.f.client.occurrences[oid] = self.detail
        self.f.w.state['experiments'].append({'occurrenceId': oid, 'problem': 'muse_decode_layer',
            'supervisorOccurrenceId': B.uid()})
        return oid

    def refresh_config_pin(self):
        path = pathlib.Path(self.f.config['autoCheckPolicyPath']); p = W.read_json(path)
        W.atomic(pathlib.Path(p['watcherConfig']['path']), self.f.config)
        p['watcherConfig']['sha256'] = W.digest(pathlib.Path(p['watcherConfig']['path']).read_bytes()); W.atomic(path, p)

    def test_observe_keeps_unmeasured_qa3_owner_and_preserves_raw_semantics(self):
        oid = self.install_owned(); obs = self.f.w.observe()
        self.assertIn(self.refs[0]['digest'], obs['pendingOfficialCandidateSHA256'])
        self.assertFalse(any(a['occurrenceId'] == oid for a in obs['active']))
        outcome = next(o for o in obs['outcomes'] if o['occurrenceId'] == oid)
        self.assertTrue(outcome['controllerEvidence']['qaApproval'])
        qa = next(a for a in outcome['artifacts'] if a['name'] == 'qa-verification.json')
        self.assertEqual(qa['semanticReview']['decision'], self.report['decision']); self.assertNotIn('review', qa)
        self.assertNotIn('findings', qa['semanticReview'])
        self.evidence['receiptSHA256'] = 'e'*64
        uncertain = self.f.w.observe()
        self.assertTrue(any(a['occurrenceId'] == oid and a['state'] == 'boundary_unconfirmed_or_review_owned' for a in uncertain['active']))
        self.assertNotIn(self.refs[0]['id'], uncertain['confirmedSourceArtifactIds'])

    def test_cpu_failure_reaches_supervisor_without_model_stopped_or_qa_artifact(self):
        self.technical_failure(); oid = self.install_owned(); obs = self.f.w.observe()
        outcome = next(o for o in W.compact_outcomes(obs['outcomes']) if o['occurrenceId'] == oid)
        self.assertEqual(outcome['controllerEvidence']['code'], 'NATIVE_QA3_CPU_EXECUTION_FAILED')
        self.assertFalse(outcome['controllerEvidence']['modelDispatched'])
        self.assertFalse(any(a['name'] == 'qa-verification.json' for a in outcome['artifacts']))
        self.assertFalse(any(a['occurrenceId'] == oid for a in obs['active']))
        self.assertIn(self.refs[0]['id'], obs['confirmedSourceArtifactIds'])
        self.assertNotIn('stopped', self.qa['receipt'])

    def test_registered_qa3_job_requires_original_semantics_and_transport_receipt(self):
        oid = self.install_owned(); receipt = self.f.root/'registered-qa3.json'
        job = {'id': '0123456789abcdef', 'problem': 'muse_decode_layer', 'sourceSHA256': self.refs[0]['digest'],
            'occurrenceId': oid, 'qaArtifactId': self.qa_ref['id'], 'qaSHA256': self.qa_ref['digest'],
            'policySHA256': 'a'*64, 'auditSHA256': 'b'*64, 'dispatchReceiptSHA256': 'c'*64}
        transport = {'state': 'job_recorded', 'mode': 'check', 'identity': {**{k: job[k] for k in
            ['problem', 'occurrenceId', 'policySHA256', 'auditSHA256']}, 'sourceSha256': job['sourceSHA256']},
            'receipt': {'job_id': job['id']}}
        job['transportReceiptSHA256'] = W.digest(transport)
        W.atomic(receipt, {'version': 1, 'mode': 'check', **job, 'transportReceipt': transport})
        job.update(receiptPath=str(receipt), receiptSHA256=W.digest(receipt.read_bytes()))
        registry = self.f.root/'qa3-registry.json'; W.atomic(registry, {'version': 1, 'watchId': self.f.config['watchId'], 'jobs': [job]})
        self.f.config['officialRegistryPath'] = str(registry); self.refresh_config_pin()
        self.assertEqual(self.f.w.registered_jobs(), [{k: job[k] for k in ['id', 'problem', 'sourceSHA256']}])
        job['receiptSHA256'] = 'f'*64; W.atomic(registry, {'version': 1, 'watchId': self.f.config['watchId'], 'jobs': [job]})
        with self.assertRaisesRegex(ValueError, 'REGISTERED_JOB_RECEIPT'): self.f.w.registered_jobs()
        self.assertEqual(self.f.client.posts, [])

    def test_invalid_report_repair_keeps_original_bytes_and_exact_controller_bindings(self):
        oid = self.install_owned(); raw = b'{"version":3,"version":3}'
        self.replace_report(raw, 'NATIVE_QA3_REPORT_INVALID', None)
        self.qa['state'] = 'failed'; self.detail['steps'][1]['state'] = 'attention'; self.detail['occurrence']['state'] = 'attention'
        row = {'occurrenceId': oid, 'problem': 'muse_decode_layer', 'candidateSHA256': self.refs[0]['digest'],
            'qaArtifactId': self.qa_ref['id'], 'qaSHA256': self.qa_ref['digest'], 'state': 'rejected_repairable',
            'jobId': None, 'postAttempted': False, 'code': 'QA_V3_REPORT_INVALID'}
        self.f.install_diagnostic_fixture([row]); p = W.read_json(self.f.config['autoCheckPolicyPath'])
        p['nativeQA3Registrations'] = {'version': 1, 'registrations': [self.registration]}; W.atomic(self.f.config['autoCheckPolicyPath'], p)
        status = W.read_json(self.f.config['autoCheckStatusPath']); status['policySHA256'] = W.digest(p); W.atomic(self.f.config['autoCheckStatusPath'], status)
        observation = self.f.w.observe(); repairs = observation['autoCheckDiagnostics']['repairableEvidence']
        self.assertEqual(len(repairs), 1, observation['autoCheckDiagnostics'])
        diagnostic = repairs[0]['repairQADiagnostic']
        self.assertIsNone(diagnostic['report']); self.assertEqual(diagnostic['reportState'], 'invalid_original_json_or_schema')
        self.assertEqual(diagnostic['originalArtifact'], self.qa_ref)
        self.assertEqual(diagnostic['controllerEvidence'], {'version': 1, 'contractVersion': 3,
            'requestSHA256': self.evidence['requestSHA256'], 'receiptSHA256': self.evidence['receiptSHA256'],
            'snapshotSHA256': W.digest(self.evidence['snapshot']), 'validationSHA256': W.digest(self.evidence['validation']),
            'registration': self.registration})
        self.legacy.supervisor['approvedPlan']['decision'] = 'repair_evidence'
        exp = {'id': 'repair-invalid-report', 'problem': 'muse_decode_layer', 'sourceArtifactId': self.refs[0]['id'],
            'hypothesis': 'Correct malformed review evidence while preserving all candidate bytes.',
            'instructions': 'Run a fresh independent controlled CPU replay and semantic QA without rewriting historical evidence.'}
        requests = self.legacy.capture(exp, observation)
        materials = requests[1][2]['input']['experiment']['materials']
        original = next(m for m in materials if m['name'] == 'prior-qa-original.json')
        self.assertEqual(base64.b64decode(original['base64']), raw); self.assertEqual(original['sha256'], W.digest(raw))
        self.assertEqual(self.f.client.posts, [])

    def test_original_report_and_fresh_controller_proof_are_separate(self):
        view, report = self.read()
        self.assertTrue(view['qaApproval']); self.assertEqual(report, self.report)
        self.assertNotIn('candidateSHA256', report); self.assertNotIn('physicalInspection', view)
        before = W.digest(view); self.evidence['physicalInspection']['observedAt'] = '1970-01-01T02:46:39Z'
        self.assertEqual(W.digest(self.read()[0]), before); self.assertEqual(self.f.client.posts, [])

    def test_tampered_artifacts_policy_fence_and_physical_proof_fail_closed(self):
        for key, bad in [('runFence', B.uid()), ('requestSHA256', 'f'*64), ('receiptSHA256', 'f'*64)]:
            old = self.evidence[key]; self.evidence[key] = bad
            with self.subTest(key=key), self.assertRaises(ValueError): self.read()
            self.evidence[key] = old
        self.evidence['physicalInspection']['observedAt'] = '1970-01-01T02:46:24Z'
        with self.assertRaisesRegex(ValueError, 'PHYSICAL_STOP'): self.read()
        self.evidence['physicalInspection']['observedAt'] = '1970-01-01T02:46:40Z'
        self.f.client.artifacts[self.refs[1]['id']] = b'tampered'
        with self.assertRaises(ValueError): self.read()
        self.assertEqual(self.f.client.posts, [])

    def test_base_source_actor_or_input_hash_change_breaks_original_request_digest(self):
        for change in ['actor', 'input']:
            original = copy.deepcopy(self.evidence['request']['baseSource'])
            if change == 'actor': self.evidence['request']['baseSource']['source']['attemptId'] = B.uid()
            else: self.evidence['request']['baseSource']['inputSHA256'] = 'f'*64
            with self.subTest(change=change), self.assertRaisesRegex(ValueError, 'REQUEST_DIGEST'): self.read()
            self.evidence['request']['baseSource'] = original
        self.assertEqual(self.f.client.posts, [])

    def test_semantic_rejection_is_new_hypothesis_without_rewriting_report(self):
        self.report['decision'] = 'reject'; self.report['blockingFindings'] = ['Hypothesis is false.']
        self.replace_report(W.canonical(self.report), 'NATIVE_QA3_SEMANTIC_REJECT', 'reject')
        view, report = self.read()
        self.assertEqual(view['classification'], 'new_hypothesis_required'); self.assertFalse(view['qaApproval'])
        self.assertEqual(report, self.report); self.assertEqual(self.f.client.posts, [])

    def replace_report(self, raw, code, decision):
        self.f.client.artifacts[self.qa_ref['id']] = raw
        self.qa_ref.update(digest=W.digest(raw), size=len(raw))
        next(a for a in self.detail['artifacts'] if a['id'] == self.qa_ref['id']).update(self.qa_ref)
        self.evidence['validation'].update(status='rejected', code=code, semanticDecision=decision)

    def test_invalid_original_json_remains_raw_and_never_becomes_valid_qa(self):
        for raw in [b'{bad', b'{"version":3,"version":3}', b'{"unknown":Infinity}', W.canonical({**self.report, 'unexpected': 1})]:
            with self.subTest(raw=raw):
                self.replace_report(raw, 'NATIVE_QA3_REPORT_INVALID', None)
                view, report = self.read()
                self.assertIsNone(report); self.assertFalse(view['reportSchemaValid']); self.assertFalse(view['qaApproval'])
                self.assertEqual(self.f.client.artifacts[self.qa_ref['id']], raw)

    def technical_failure(self):
        e = self.evidence; q = self.qa; r = e['receipt']
        r.update(exitCode=137, oomKilled=True)
        e['receiptSHA256'] = W.digest(r); e['physicalInspection']['receiptSHA256'] = e['receiptSHA256']
        terminal = {'kind': 'cpu-preparation-terminal', 'modelDispatched': False,
            'cpu': {'requestId': q['id'], 'requestSHA256': e['requestSHA256'],
                'receiptSHA256': e['receiptSHA256'], 'stopProof': r['stopProof']}}
        failure = {'version': 1, 'status': 'rejected', 'code': 'NATIVE_QA3_CPU_EXECUTION_FAILED', 'modelDispatched': False,
            'snapshotContractSHA256': e['snapshot']['contractSHA256'], 'source': self.source,
            'requestSHA256': e['requestSHA256'], 'receiptSHA256': e['receiptSHA256'],
            'receiptCanonicalization': e['receiptCanonicalization'], 'terminal': terminal}
        q.update(state='failed', receipt=terminal, error_code=failure['code'])
        self.detail['steps'][1]['state'] = 'attention'; self.detail['occurrence']['state'] = 'attention'
        q['context']['cpuReplayPreparation']['phase'] = 'cpu_complete'
        q['context']['nativeQA3PreparationFailure'] = failure
        q['context']['manifest'].pop('nativeQA3'); q['context'].pop('nativeQA3Validation')
        e.update(modelDispatched=False, preparationFailure=failure, diagnostic=failure, semanticReport=None, validation=None, handoff=None)
        self.detail['artifacts'] = [a for a in self.detail['artifacts'] if a['attempt_id'] != q['id']]

    def test_complete_cpu_failure_has_own_stop_boundary_without_model_or_fake_report(self):
        self.technical_failure(); before = W.canonical(self.qa['receipt'])
        view, report = self.read()
        self.assertEqual(view['classification'], 'cpu_preparation_failed'); self.assertIsNone(report)
        self.assertFalse(view['modelDispatched']); self.assertTrue(view['physicalConfirmed'])
        self.assertFalse(W.confirmed_boundary(self.detail)); self.assertTrue(self.f.w.native_boundary(self.detail, view))
        self.assertNotIn('stopped', self.qa['receipt']); self.assertEqual(W.canonical(self.qa['receipt']), before)
        self.assertFalse(view['qaApproval']); self.assertEqual(self.f.client.posts, [])

    def test_unconfirmed_cpu_preparation_never_releases_ownership_or_claims_qa(self):
        self.technical_failure(); e = self.evidence
        e.update(state='diagnostic', physicalConfirmed=False, receipt=None, receiptSHA256=None, physicalInspection=None)
        e['action']['state'] = 'pending'; e['diagnostic'] = {'code': 'CPU_REPLAY_UNCONFIRMED'}
        view, report = self.read()
        self.assertEqual(view['classification'], 'cpu_preparation_unconfirmed'); self.assertIsNone(report)
        self.assertFalse(self.f.w.native_boundary(self.detail, view)); self.assertEqual(self.f.client.posts, [])

    def test_missing_controller_authority_keeps_completed_candidate_owned(self):
        self.assertTrue(W.confirmed_boundary(self.detail))
        self.evidence['runFence'] = B.uid()
        view = self.f.w.native_qa3_terminal(self.detail)
        self.assertEqual(view['bindingState'], 'unavailable')
        self.assertFalse(self.f.w.native_boundary(self.detail, view))

    def test_preparation_receipt_cannot_relabel_cpu_result_or_identity(self):
        self.technical_failure()
        failure = self.qa['context']['nativeQA3PreparationFailure']
        failure['code'] = 'NATIVE_QA3_CPU_RESULT_INVALID'
        with self.assertRaisesRegex(ValueError, 'PREPARATION_FAILURE'): self.read()
        failure['code'] = 'NATIVE_QA3_CPU_EXECUTION_FAILED'
        failure['terminal']['cpu']['requestId'] = B.uid()
        with self.assertRaisesRegex(ValueError, 'PREPARATION_FAILURE'): self.read()

    def test_nonobject_cpu_result_is_typed_failure_not_unhandled_reader_error(self):
        self.technical_failure(); e = self.evidence; failure = self.qa['context']['nativeQA3PreparationFailure']
        for value in [[1], 'not an object', 42]:
            raw = W.canonical(value); e['receipt']['outputs']['result'] = {'base64': base64.b64encode(raw).decode(), 'size': len(raw), 'digest': W.digest(raw)}
            e['receipt'].update(exitCode=0, oomKilled=False); sha = W.digest(e['receipt'])
            e['receiptSHA256'] = e['physicalInspection']['receiptSHA256'] = failure['receiptSHA256'] = failure['terminal']['cpu']['receiptSHA256'] = sha
            failure['code'] = 'NATIVE_QA3_CPU_RESULT_INVALID'
            view, report = self.read()
            self.assertEqual(view['code'], failure['code']); self.assertIsNone(report)

    def test_semantic_and_cpu_diagnostics_bind_source_readset_but_fresh_clock_does_not(self):
        view, _ = self.read(); self.detail['occurrence']['state'] = 'completed'
        source = {**self.refs[0], 'problem': 'muse_decode_layer', 'occurrenceId': self.detail['occurrence']['id']}
        obs = self.f.w.observe(); obs['catalog'].append(source); obs['confirmedSourceArtifactIds'].append(source['id'])
        obs['outcomes'].append({'occurrenceId': source['occurrenceId'], 'problem': source['problem'], 'state': 'completed',
            'failures': [], 'artifacts': [], 'controllerEvidence': view})
        before = W.source_action_evidence(obs, self.f.config)['sources'][source['id']]['experimentSHA256']
        obs['outcomes'][-1]['controllerEvidence'] = {**view, 'classification': 'new_hypothesis_required', 'qaApproval': False}
        self.assertNotEqual(W.source_action_evidence(obs, self.f.config)['sources'][source['id']]['experimentSHA256'], before)

    def test_lost_v3_create_ack_stays_v3_after_current_flag_changes(self):
        exp, obs = self.legacy.example(); self.f.config['nativeQAContractVersion'] = 3
        self.f.client.timeout_after_commit_number = 1
        with self.assertRaises(TimeoutError): self.f.w.dispatch_experiment(exp, obs, self.legacy.supervisor)
        marker = next(self.f.w.directory.glob('intent-*.json')); before = W.read_json(marker)
        self.f.config['nativeQAContractVersion'] = 2
        result = self.f.w.reconcile_existing_post('create', before['requestKey'])
        self.assertEqual(len(self.f.client.posts), 1)
        self.assertEqual(result['definition']['steps'][1]['parameters']['nativeQAContractVersion'], 3)
        self.assertEqual(W.read_json(marker)['requestDigest'], before['requestDigest'])

    def test_unknown_v3_create_remains_owned_without_retry_or_reinterpretation(self):
        exp, obs = self.legacy.example(); self.f.config['nativeQAContractVersion'] = 3
        self.f.client.timeout_before_commit_number = 1
        with self.assertRaises(TimeoutError): self.f.w.dispatch_experiment(exp, obs, self.legacy.supervisor)
        marker = next(self.f.w.directory.glob('intent-*.json')); before = marker.read_bytes(); data = W.read_json(marker)
        self.f.config['nativeQAContractVersion'] = 1
        with self.assertRaisesRegex(ValueError, 'UNCERTAIN'):
            self.f.w.reconcile_existing_post('create', data['requestKey'])
        self.assertEqual(marker.read_bytes(), before)
        self.assertEqual(len(self.f.client.posts), 1)


if __name__ == '__main__': unittest.main()
