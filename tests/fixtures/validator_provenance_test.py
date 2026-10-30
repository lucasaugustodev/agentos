import base64
import copy
import hashlib
import importlib.util
import json
import os
import pathlib
import shutil
import tempfile
import time
import unittest
import uuid
from unittest.mock import patch

ROOT = pathlib.Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('provenance_broker', ROOT/'scripts/agentos-candidate-validation.py')
broker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(broker)
p = broker.provenance
from validation_coverage import verified_coverage
NATIVE_BUNDLES = copy.deepcopy(p.REVIEWED_BUNDLES)


class ValidatorProvenanceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.temp.name)
        self.layout = p.Layout(self.root)
        self.owner = patch.object(p, 'OWNER_UID', os.getuid()); self.owner.start()
        self.protection = patch.object(p, 'PROTECTION_ROOT', self.root); self.protection.start()
        self.reviewed = (ROOT/'tests/fixtures/release-action-d52ef6b.txt').read_bytes()
        self.assertEqual(p.sha(self.reviewed), p.REVIEWED_SHA)
        self.helper = (ROOT/'scripts/candidate_origin.py').read_bytes()
        # Test-only reviewed fixture bundle; production never admits legacy02a.
        self.bundles = patch.object(p,'REVIEWED_BUNDLES',{p.REVIEWED_SHA:{'sourceCommit':p.REVIEWED_COMMIT,
            'files':{'agentos-release-action.py':p.REVIEWED_SHA,'candidate_origin.py':p.sha(self.helper)}}})
        self.bundles.start()
        self.legacy = self.reviewed.replace(
            b'  # Keep this contract identical to workerSourceDigest: generated Python caches\n'
            b'  # are mutable runtime data; source, documentation and built assets are signed.\n', b'')
        self.legacy = self.legacy.replace(b",'__pycache__'", b'').replace(b" or name=='__pycache__'", b'')
        self.assertEqual(p.sha(self.legacy), p.LEGACY_SHA)
        self.partial = self.legacy.replace(b"['node_modules','.local-test','.git']", b"['node_modules','.local-test','.git','__pycache__']")
        self.assertEqual(p.sha(self.partial), p.PARTIAL_PREIMAGE)
        for folder in [self.layout.releases, self.layout.receipts, self.layout.builds,
                       self.layout.broker, self.layout.control.parent]:
            folder.mkdir(parents=True, exist_ok=True)
        self.base = self.layout.releases/('managed-'+str(uuid.uuid4()))
        self.put(self.base/'scripts/agentos-release-action.py', self.legacy)
        self.put(self.layout.control, self.partial)
        self.occurrence = str(uuid.uuid4()); self.attempt = str(uuid.uuid4())
        self.current = self.layout.releases/('managed-'+self.attempt)
        self.put(self.current/'scripts/agentos-release-action.py', self.reviewed)
        self.put(self.current/'scripts/candidate_origin.py', self.helper)
        self.put(self.current/'server/fixture.ts', b'export const checked = true;\n')
        self.put(self.current/'docs/contract.md', b'Preserve mandatory tests.\n')
        self.put(self.current/'dist/app.js', b'console.log("tested")\n')
        self.layout.active.symlink_to(self.current)
        self.directory = self.layout.builds/'operator-proof'
        self.directory.mkdir()
        self.input = {'occurrenceId': self.occurrence, 'attemptId': self.attempt,
                      'parameters': {'workspace': 'agentos/ocfixture'}, 'results': []}
        now = time.time()-5
        self.validated = {'candidate':str(self.current),'digest':p.tree_digest(self.current),
                          'base':str(self.base),'validatedAt':now}
        self.record = self.validated | {'previous':str(self.base),'publishedAt':now+1,'workersPendingRestart':True}
        self.output = {'ok':True,'output':self.validated,'artifacts':[{'name':'validation.log',
            'content':'built in 1s\n Test Files 2 passed (2)\n Tests 4 passed (4)\n Duration 1s\n'}]}
        self.save_operator()

    def tearDown(self):
        self.bundles.stop(); self.protection.stop(); self.owner.stop(); self.temp.cleanup()

    def put(self, path, raw):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw); path.chmod(0o600)

    def js(self, path, value):
        self.put(path, p.canonical(value))

    def save_operator(self):
        self.js(self.directory/'release-input.json', self.input)
        for action in ['validate','publish']:
            file = self.directory/(action+'.intent.json')
            self.js(file, {'action':action,'input':self.input,'base':str(self.base),
                          **({'executor':getattr(self,'publisher_declaration',self.executor_declaration) if action=='publish' else self.executor_declaration} if hasattr(self,'executor_declaration') else {})})
            os.utime(file, (time.time()-20, time.time()-20))
        self.js(self.directory/'validate.receipt.json', self.output)
        self.js(self.directory/'publish.receipt.json', {'ok':True,'output':self.record})
        self.js(self.layout.receipts/(self.occurrence+'.json'), self.record)

    def request(self):
        proof = p.publication(self.layout)
        return {'registrationId':str(uuid.uuid4()),'expectedRegisteredSha256':p.sha(self.layout.control.read_bytes()),
                'expectedReleaseDigest':self.record['digest'],'expectedPublicationProofDigest':p.sha(p.canonical(proof))}

    def register(self, request=None, after_replace=None):
        return p.register(self.layout, request or self.request(), after_replace)

    def test_operator_receipts_are_a_distinct_verified_format_not_fake_legacy_sidecars(self):
        self.assertIn(p.REVIEWED_SHA,p.HISTORICAL_EXECUTORS)
        before = {x.name:x.read_bytes() for x in self.directory.iterdir()}
        proof = p.publication(self.layout)
        self.assertEqual(proof['kind'], 'operator_guard_receipts')
        self.assertEqual(proof['validationExecutorSha256'], p.LEGACY_SHA)
        self.assertEqual(proof['publishedActionSha256'], p.REVIEWED_SHA)
        self.assertEqual(before, {x.name:x.read_bytes() for x in self.directory.iterdir()})
        self.assertFalse(list(self.root.rglob('framework-general-*.json')))

    def test_explicit_registration_keeps_history_and_replays_without_new_transition(self):
        request = self.request(); before = {x.name:x.read_bytes() for x in self.directory.iterdir()}
        first = self.register(request); second = self.register(request)
        self.assertEqual(first, second); self.assertEqual(self.layout.control.read_bytes(), self.reviewed)
        record = p.registration_record(self.layout, first)
        self.assertEqual(record['previousExecutorSha256'], p.PARTIAL_PREIMAGE)
        self.assertEqual(record['publicationEvidence']['validationExecutorSha256'], p.LEGACY_SHA)
        self.assertEqual(before, {x.name:x.read_bytes() for x in self.directory.iterdir()})
        self.assertEqual(p.active_registration(self.layout), first)

    def test_registered_bytes_without_receipt_are_not_trusted(self):
        self.put(self.layout.control, self.reviewed)
        with self.assertRaises(FileNotFoundError): p.active_registration(self.layout)

    def test_crash_after_replacement_fails_closed_and_same_intent_reconciles(self):
        request = self.request()
        def crash(): raise RuntimeError('simulated process interruption')
        with self.assertRaises(RuntimeError): self.register(request, crash)
        self.assertEqual(self.layout.control.read_bytes(), self.reviewed)
        with self.assertRaises(FileNotFoundError): p.active_registration(self.layout)
        recovered = self.register(request)
        self.assertEqual(p.active_registration(self.layout), recovered)

    def test_changed_preimage_or_proof_never_updates_control(self):
        request = self.request(); request['expectedPublicationProofDigest'] = '0'*64
        with self.assertRaisesRegex(ValueError,'PROOF_CHANGED'): self.register(request)
        self.assertEqual(self.layout.control.read_bytes(), self.partial)
        request = self.request(); self.put(self.layout.control,b'not reviewed')
        with self.assertRaisesRegex(ValueError,'PREIMAGE_CHANGED'): self.register(request)
        self.assertEqual(self.layout.control.read_bytes(),b'not reviewed')

    def test_changed_registered_file_after_registration_is_refused(self):
        self.register(); self.put(self.layout.control,self.reviewed+b'\n')
        with self.assertRaisesRegex(ValueError,'EXECUTOR_CHANGED'): p.active_registration(self.layout)

    def test_missing_false_or_mismatched_validation_is_never_publication_proof(self):
        self.output['ok'] = False; self.save_operator()
        with self.assertRaisesRegex(ValueError,'VALIDATION_FAILED'): p.publication(self.layout)
        self.output['ok'] = True; self.output['output'] = self.validated | {'digest':'0'*64}; self.save_operator()
        with self.assertRaisesRegex(ValueError,'RECEIPT_MISMATCH'): p.publication(self.layout)

    def test_times_and_base_are_bound(self):
        self.record['publishedAt'] = self.record['validatedAt']-1; self.save_operator()
        with self.assertRaisesRegex(ValueError,'TIME_INVALID'): p.publication(self.layout)
        self.record['publishedAt'] = time.time()-1; self.record['previous'] = str(self.current); self.save_operator()
        with self.assertRaisesRegex(ValueError,'RECEIPT_MISMATCH'): p.publication(self.layout)

    def test_generated_cache_does_not_change_proof_but_source_dist_and_docs_do(self):
        before = p.publication(self.layout)
        self.put(self.current/'bridge/__pycache__/generated.pyc',b'mutable')
        self.assertEqual(before, p.publication(self.layout))
        for relative in ['server/fixture.ts','dist/app.js','docs/contract.md']:
            file = self.current/relative; raw = file.read_bytes(); self.put(file,raw+b'changed')
            with self.assertRaisesRegex(ValueError,'PUBLISHED_BYTES_CHANGED'): p.publication(self.layout)
            self.put(file,raw)

    def test_unknown_executor_and_symlink_receipt_are_not_authority(self):
        self.put(self.base/'scripts/agentos-release-action.py',b'fake validator')
        with self.assertRaisesRegex(ValueError,'HISTORICAL_EXECUTOR_UNREVIEWED'): p.publication(self.layout)
        self.put(self.base/'scripts/agentos-release-action.py',self.legacy)
        target = self.directory/'validate.receipt.json'; target.unlink(); target.symlink_to(self.directory/'publish.receipt.json')
        with self.assertRaisesRegex(ValueError,'UNPROTECTED'): p.publication(self.layout)

    def test_service_writable_registration_or_receipt_is_rejected(self):
        identity = self.register(); (self.layout.registry/'current.json').chmod(0o666)
        with self.assertRaisesRegex(ValueError,'UNPROTECTED'): p.active_registration(self.layout)
        (self.layout.registry/'current.json').chmod(0o600)
        (self.layout.registry/(identity['registrationId']+'.json')).chmod(0o666)
        with self.assertRaisesRegex(ValueError,'UNPROTECTED'): p.active_registration(self.layout)

    def test_uncertain_execution_prevents_registration(self):
        self.js(self.layout.broker/str(uuid.uuid4())/'receipt.json',{'state':'uncertain'})
        with self.assertRaisesRegex(ValueError,'EXECUTION_UNCERTAIN'): self.register()
        self.assertEqual(self.layout.control.read_bytes(),self.partial)

    def test_future_native_publication_reuses_registration_without_release_specific_ids(self):
        identity = self.register(); old_current = self.current
        action = str(uuid.uuid4()); next_release = self.layout.releases/('managed-'+action)
        self.put(next_release/'scripts/agentos-release-action.py',self.reviewed)
        self.put(next_release/'scripts/candidate_origin.py',self.helper)
        self.put(next_release/'server/next.ts',b'new reviewed source')
        self.layout.active.unlink(); self.layout.active.symlink_to(next_release)
        validated = {'candidate':str(next_release),'digest':p.tree_digest(next_release),
                     'base':str(old_current),'validatedAt':time.time()-3}
        record = validated | {'publishedAt':time.time()-1,'previous':str(old_current)}
        directory = self.layout.broker/action
        request = {'actionId':action,'candidateDigest':'a'*64}
        output = copy.deepcopy(self.output); output['output'] = validated
        self.js(directory/'request.json',request); self.js(directory/'result.json',output)
        self.js(directory/'receipt.json',{'actionId':action,'state':'completed','passed':True,
            'registeredActionSha':p.REVIEWED_SHA,'executionRegistration':identity,
            'requestDigest':p.sha(p.canonical(request)),'resultDigest':p.sha(p.canonical(output)),
            'resultRef':str(directory/'result.json')})
        self.js(self.layout.receipts/(action+'.json'),record)
        proof = p.publication(self.layout)
        self.assertEqual(proof['kind'],'registered_broker_receipts')
        self.assertEqual(proof['release']['candidate'],str(next_release))
        self.assertEqual(proof['validationExecutorSha256'],p.REVIEWED_SHA)
        self.assertEqual(p.active_registration(self.layout),identity)

    def test_historical_ci_executor_identity_is_not_rewritten(self):
        with patch.object(p,'Layout',return_value=self.layout):
            self.assertEqual(broker.verified_execution_registration({'registeredActionSha':p.LEGACY_SHA}),p.LEGACY_SHA)
            identity = self.register()
            new = {'registeredActionSha':p.REVIEWED_SHA,'executionRegistration':identity}
            self.assertEqual(broker.verified_execution_registration(new),p.REVIEWED_SHA)
            with self.assertRaisesRegex(ValueError,'UNREVIEWED'):
                broker.verified_execution_registration({'registeredActionSha':p.PARTIAL_PREIMAGE})

    def test_generic_summary_does_not_manufacture_database_coverage(self):
        proof = p.publication(self.layout)
        self.assertNotIn('requiredChecks',proof); self.assertNotIn('coverage',proof)

    def test_json_report_footer_is_bound_to_the_actual_build(self):
        output = copy.deepcopy(self.output)
        report = self.layout.builds/self.attempt/'.local-test/vitest-results.json'
        output['artifacts'][0]['content'] += 'JSON report written to '+str(report)+'\n'
        p.complete_output(output,report)
        with self.assertRaisesRegex(ValueError,'VALIDATION_INCOMPLETE'):
            p.complete_output(output,self.layout.builds/'foreign/.local-test/vitest-results.json')

    def test_reviewed_coverage_executor_requires_its_real_report_hash(self):
        executor = '39843bdbde81e84b5093ed6667484378d1f4444c66aa33424b52d28b45b0cc44'
        output = copy.deepcopy(self.output)
        with self.assertRaisesRegex(ValueError,'EXECUTOR_CHANGED'): p.executor_artifacts(output,executor)
        raw = '{"testResults":[]}'
        output['output'] = dict(output['output'],executorSha256=executor,testReportDigest=p.sha(raw.encode()))
        output['artifacts'].append({'name':'vitest-results.json','content':raw})
        p.executor_artifacts(output,executor)
        output['artifacts'][-1]['content'] += 'changed'
        with self.assertRaisesRegex(ValueError,'ARTIFACT_CHANGED'): p.executor_artifacts(output,executor)

    def runtime_request(self):
        request = {key:str(uuid.uuid4()) for key in ['actionId','goalId','taskId','runId','runFence','contextVersion']}
        raw = p.canonical({'candidateDigest':'a'*64,'files':[]})
        request.update(authority='runtime_artifact',expectedAttempt=1,project='agentos',workspace='agentos/ofixture',
                       candidateDigest='a'*64,candidateArtifactDigest=p.sha(raw),candidateSize=len(raw),
                       candidateBase64=base64.b64encode(raw).decode(),contractDigest='b'*64,
                       frameworkAuthorization={'kind':'registered_agentos_source','project':'agentos','goalId':request['goalId']})
        policy = {'version':1,'enabled':True,'project':'agentos','profile':'agentos.validate','stages':['work','integrate','verify'],'required':True,
                  'requiredDatabaseTests':[{'file':'tests/exact-pg.test.ts','name':'durable input survives a restart'}]}
        registration = {'version':1,'id':'worker-validation:'+request['goalId'],'registrationId':str(uuid.uuid4()),
                        'goalId':request['goalId'],'policy':policy,'policyDigest':p.sha(p.canonical(policy)),'channel':'api-token'}
        intent = p.canonical({'version':1,'profile':'agentos.validate','reason':'Run the required isolated database checks.'})
        request['runtimeAuthority'] = {'version':1,'kind':'runtime_artifact','profile':'agentos.validate',
            'policyRegistration':registration,'snapshotDigest':p.sha(p.canonical(registration)),
            'intent':{'artifactId':str(uuid.uuid4()),'digest':p.sha(intent),'size':len(intent),'base64':base64.b64encode(intent).decode()}}
        return request

    def test_runtime_artifact_authority_does_not_forge_a_manager(self):
        request = self.runtime_request()
        candidate, _ = broker.identity(request)
        self.assertEqual(candidate['candidateDigest'],request['candidateDigest'])
        self.assertNotIn('managerRunId',request)
        request['managerRunId'] = str(uuid.uuid4())
        with self.assertRaisesRegex(ValueError,'AUTHORITY_AMBIGUOUS'): broker.identity(request)

    def test_changed_policy_snapshot_intent_and_foreign_goal_are_rejected(self):
        for change in ['snapshot','intent','goal']:
            request = self.runtime_request(); authority = request['runtimeAuthority']
            if change == 'snapshot': authority['snapshotDigest'] = '0'*64
            if change == 'intent': authority['intent']['digest'] = '0'*64
            if change == 'goal': authority['policyRegistration']['goalId'] = str(uuid.uuid4())
            with self.assertRaises(ValueError): broker.identity(request)

    def test_worker_cannot_supply_arbitrary_command_or_coverage_path(self):
        request = self.runtime_request(); a = request['runtimeAuthority']
        raw = p.canonical({'version':1,'profile':'agentos.validate','reason':'Validate the immutable candidate safely.','command':'anything'})
        a['intent'].update(digest=p.sha(raw),size=len(raw),base64=base64.b64encode(raw).decode())
        with self.assertRaisesRegex(ValueError,'INTENT_INVALID'): broker.identity(request)
        request = self.runtime_request(); a = request['runtimeAuthority']; r = a['policyRegistration']
        r['policy']['requiredDatabaseTests'][0]['file'] = 'tests/../../private.ts'
        r['policyDigest'] = p.sha(p.canonical(r['policy'])); a['snapshotDigest'] = p.sha(p.canonical(r))
        with self.assertRaisesRegex(ValueError,'COVERAGE_INVALID'): broker.identity(request)

    def coverage_output(self, request, status='passed', unrelated_status='skipped'):
        required = request['runtimeAuthority']['policyRegistration']['policy']['requiredDatabaseTests']
        root = '/var/lib/agentos-validation-builds/'+request['actionId']
        report = {'testResults':[{'name':root+'/'+required[0]['file'],'assertionResults':[
            {'fullName':required[0]['name'],'status':status},
            {'fullName':'optional browser scenario','status':unrelated_status}]}]}
        report_bytes = p.canonical(report)
        derived = 'skipped' if status in ['pending','todo','disabled','skipped'] else status
        coverage = {'version':1,'source':'registered_vitest_json','profile':'agentos.validate',
            'candidateDigest':request['candidateDigest'],'obligationsDigest':p.sha(p.canonical(required)),
            'reportDigest':p.sha(report_bytes),'reportRoot':root,
            'requiredTests':[required[0] | {'status':derived}]}
        coverage_bytes = p.canonical(coverage)
        return {'ok':True,'output':{'executorSha256':p.REVIEWED_SHA,
            'testReportDigest':p.sha(report_bytes),'coverageDigest':p.sha(coverage_bytes)},
            'artifacts':[{'name':'vitest-results.json','content':report_bytes.decode()},
                         {'name':'validation-coverage.json','content':coverage_bytes.decode()}]}

    def replace_report(self, output, report):
        raw = p.canonical(report)
        output['artifacts'][0]['content'] = raw.decode()
        output['output']['testReportDigest'] = p.sha(raw)
        coverage = json.loads(output['artifacts'][1]['content']); coverage['reportDigest'] = p.sha(raw)
        raw_coverage = p.canonical(coverage)
        output['artifacts'][1]['content'] = raw_coverage.decode()
        output['output']['coverageDigest'] = p.sha(raw_coverage)

    def test_exact_database_obligation_is_verified_without_rejecting_unrelated_skips(self):
        request = self.runtime_request(); output = self.coverage_output(request)
        result = verified_coverage(request,output,p.REVIEWED_SHA)
        self.assertEqual(result['requiredTests'][0]['status'],'passed')
        self.assertEqual(result['obligationsDigest'],p.sha(p.canonical(request['runtimeAuthority']['policyRegistration']['policy']['requiredDatabaseTests'])))

    def test_negative_or_skipped_required_case_remains_negative(self):
        request = self.runtime_request()
        for status in ['failed','skipped','pending','todo','disabled']:
            result = verified_coverage(request,self.coverage_output(request,status),p.REVIEWED_SHA)
            self.assertEqual(result['requiredTests'][0]['status'],'failed' if status=='failed' else 'skipped')

    def test_changed_report_digest_or_candidate_never_proves_coverage(self):
        request = self.runtime_request(); output = self.coverage_output(request)
        output['artifacts'][0]['content'] += ' '
        with self.assertRaisesRegex(ValueError,'RECEIPT_UNBOUND'): verified_coverage(request,output,p.REVIEWED_SHA)
        output = self.coverage_output(request); request['candidateDigest'] = '0'*64
        with self.assertRaisesRegex(ValueError,'IDENTITY_CHANGED'): verified_coverage(request,output,p.REVIEWED_SHA)

    def test_forged_passed_status_duplicate_case_and_foreign_root_are_rejected(self):
        request = self.runtime_request()
        for mutation in ['failed','duplicate','foreign_root']:
            output = self.coverage_output(request); report = json.loads(output['artifacts'][0]['content'])
            suite = report['testResults'][0]
            if mutation == 'failed': suite['assertionResults'][0]['status'] = 'failed'
            if mutation == 'duplicate': suite['assertionResults'].append(copy.deepcopy(suite['assertionResults'][0]))
            if mutation == 'foreign_root': suite['name'] = '/other/tests/exact-pg.test.ts'
            self.replace_report(output,report)
            with self.assertRaises(ValueError): verified_coverage(request,output,p.REVIEWED_SHA)

    def test_old_output_missing_coverage_does_not_gain_an_attestation(self):
        request = self.runtime_request()
        self.assertIsNone(verified_coverage(request,{'artifacts':[]},p.REVIEWED_SHA))
        self.assertIsNone(verified_coverage({},self.coverage_output(request),p.REVIEWED_SHA))

    def staged_request(self):
        folder=self.layout.control.parent/'reviewed-validator-bundles'/p.REVIEWED_SHA
        self.put(folder/'agentos-release-action.py',self.reviewed)
        self.put(folder/'candidate_origin.py',self.helper)
        return self.request() | {'executorSha256':p.REVIEWED_SHA,'sourceCommit':p.REVIEWED_COMMIT}

    def test_reviewed_public_source_registration_precedes_its_publication_honestly(self):
        request=self.staged_request()
        before=self.layout.active.resolve()
        pointer=self.register(request)
        record=p.registration_record(self.layout,pointer)
        self.assertEqual(record['sourceKind'],'reviewed_public_source')
        self.assertEqual(record['publicationEvidence']['validationExecutorSha256'],p.LEGACY_SHA)
        self.assertEqual(self.layout.active.resolve(),before)
        self.assertEqual(record['bundleManifest'],p.REVIEWED_BUNDLES[p.REVIEWED_SHA]['files'])
        self.assertEqual(p.active_registration(self.layout,for_execution=True),pointer)

    def test_current_capacity_bundle_registers_exact_bytes_without_rewriting_publication_or_replaying_ci(self):
        guard=(ROOT/'scripts/agentos-release-action.py').read_bytes();identity=p.sha(guard)
        bundle=NATIVE_BUNDLES[identity]
        self.assertEqual(bundle['files'],{'agentos-release-action.py':identity,'candidate_origin.py':p.sha(self.helper)})
        self.assertEqual(bundle['sourceCommit'],'d65e4801eef3412b7ff234fbf81c618acd512373')
        folder=self.layout.control.parent/'reviewed-validator-bundles'/identity
        self.put(folder/'agentos-release-action.py',guard);self.put(folder/'candidate_origin.py',self.helper)
        before={x.name:x.read_bytes() for x in self.directory.iterdir()};active=self.layout.active.resolve()
        with patch.dict(p.REVIEWED_BUNDLES,{identity:bundle}):
            request=self.request()|{'executorSha256':identity,'sourceCommit':bundle['sourceCommit']}
            pointer=self.register(request)
            self.assertEqual(self.layout.control.read_bytes(),guard)
            self.assertEqual(p.active_registration(self.layout,for_execution=True),pointer)
            self.assertEqual(self.register(request),pointer)
            record=p.registration_record(self.layout,pointer)
            self.assertEqual(record['sourceKind'],'reviewed_public_source')
            self.assertEqual(record['bundleManifest'],bundle['files'])
        self.assertEqual(self.layout.active.resolve(),active)
        self.assertEqual({x.name:x.read_bytes() for x in self.directory.iterdir()},before)

    def test_unreviewed_hash_wrong_commit_and_free_source_path_are_rejected(self):
        for field,value in [('executorSha256','f'*64),('sourceCommit','f'*40),('sourcePath','/tmp/untrusted')]:
            request=self.staged_request();request[field]=value
            with self.assertRaises(ValueError):self.register(request)
            self.assertEqual(self.layout.control.read_bytes(),self.partial)

    def test_changed_staged_module_or_wrong_base_cannot_register(self):
        request=self.staged_request()
        helper=self.layout.control.parent/'reviewed-validator-bundles'/p.REVIEWED_SHA/'candidate_origin.py'
        self.put(helper,self.helper+b'\n')
        with self.assertRaisesRegex(ValueError,'SOURCE_CHANGED'):self.register(request)
        request=self.staged_request();request['expectedReleaseDigest']='0'*64
        with self.assertRaisesRegex(ValueError,'PROOF_CHANGED'):self.register(request)

    def test_legacy_versions_are_historical_only_without_new_admissible_bundle(self):
        request=self.request()
        with patch.object(p,'REVIEWED_BUNDLES',{}):
            with self.assertRaisesRegex(ValueError,'SOURCE_UNREVIEWED'):self.register(request)
        self.assertEqual(self.layout.control.read_bytes(),self.partial)

    def test_registered_module_tampering_is_rejected(self):
        self.register(self.staged_request())
        self.put(self.layout.control.parent/'candidate_origin.py',self.helper+b'\n')
        with self.assertRaisesRegex(ValueError,'EXECUTOR_CHANGED'):
            p.active_registration(self.layout,for_execution=True)

    def test_archived_evidence_survives_source_cleanup_and_lost_reply(self):
        request=self.staged_request();pointer=self.register(request)
        for directory in [self.directory,self.layout.control.parent/'reviewed-validator-bundles'/p.REVIEWED_SHA]:
            self.assertTrue(directory.resolve().is_relative_to(self.root))
            shutil.rmtree(directory)
        self.assertEqual(p.active_registration(self.layout),pointer)
        self.assertEqual(self.register(request),pointer)
        record=p.registration_record(self.layout,pointer)
        archive=pathlib.Path(record['evidenceArchive'][0]['archivedPath'])
        self.put(archive,archive.read_bytes()+b'changed')
        with self.assertRaisesRegex(ValueError,'EVIDENCE_CHANGED'):p.active_registration(self.layout)

    def test_wrong_executor_declaration_cannot_replace_historical_inference(self):
        self.output['output']['executorSha256']=p.REVIEWED_SHA
        self.record['executorSha256']=p.REVIEWED_SHA
        self.save_operator()
        with self.assertRaisesRegex(ValueError,'OPERATOR_EXECUTOR_UNBOUND'):p.publication(self.layout)

    def test_modern_operator_receipt_binds_actual_registered_executor_not_old_base(self):
        pointer=self.register(self.staged_request())
        report='{"testResults":[]}'
        metadata={'executorSha256':p.REVIEWED_SHA,'testReportDigest':p.sha(report.encode())}
        self.output['artifacts'].append({'name':'vitest-results.json','content':report})
        self.validated.update(metadata,validatedAt=time.time()+0.1)
        self.record.update(metadata,validatedAt=self.validated['validatedAt'],publishedAt=time.time()+0.2)
        self.executor_declaration={'path':str(self.layout.control),'sha256':p.REVIEWED_SHA,
                                  'sourceCommit':p.REVIEWED_COMMIT,'registration':pointer}
        self.save_operator()
        proof=p.publication(self.layout)
        self.assertEqual(proof['validationExecutorSha256'],p.REVIEWED_SHA)
        self.assertEqual(p.sha((self.base/'scripts/agentos-release-action.py').read_bytes()),p.LEGACY_SHA)
        self.executor_declaration['sha256']='f'*64;self.save_operator()
        with self.assertRaisesRegex(ValueError,'OPERATOR_EXECUTOR_UNBOUND'):p.publication(self.layout)

    def test_operator_records_can_be_preserved_exactly_under_protected_root(self):
        destination=self.layout.operator_records/'preserved'
        shutil.copytree(self.directory,destination,copy_function=shutil.copy2)
        before={item.name:item.read_bytes() for item in self.directory.iterdir()}
        self.assertTrue(self.directory.resolve().is_relative_to(self.root));shutil.rmtree(self.directory)
        proof=p.publication(self.layout)
        self.assertEqual(proof['kind'],'operator_guard_receipts')
        self.assertEqual(before,{item.name:item.read_bytes() for item in destination.iterdir()})
        self.assertTrue(proof['publicationRef']['path'].startswith(str(destination)))

    def test_protected_operator_root_never_admits_symlink_receipts_or_ancestors(self):
        destination=self.layout.operator_records/'preserved'
        shutil.copytree(self.directory,destination,copy_function=shutil.copy2)
        self.assertTrue(self.directory.resolve().is_relative_to(self.root));shutil.rmtree(self.directory)
        self.assertEqual(p.publication(self.layout)['kind'],'operator_guard_receipts')
        receipt=destination/'validate.receipt.json';saved=destination/'saved-validation.json'
        receipt.rename(saved);receipt.symlink_to(saved)
        with self.assertRaisesRegex(ValueError,'UNPROTECTED'):p.publication(self.layout)
        receipt.unlink();saved.rename(receipt)
        moved=self.root/'outside-operator';destination.rename(moved)
        destination.symlink_to(moved,target_is_directory=True)
        with self.assertRaisesRegex(ValueError,'UNPROTECTED'):p.read(destination/'publish.receipt.json')
        with self.assertRaisesRegex(ValueError,'PUBLISHED_SOURCE_MANIFEST_UNAVAILABLE'):p.publication(self.layout)

    def mixed_executors(self):
        first=self.register(self.staged_request())
        report='{"testResults":[]}'
        self.output['artifacts'].append({'name':'vitest-results.json','content':report})
        self.validated.update(executorSha256=p.REVIEWED_SHA,testReportDigest=p.sha(report.encode()),validatedAt=time.time(),
                              sourceCandidateDigest='a'*64,sourceManifestDigest='b'*64,buildInputManifestDigest='c'*64)
        self.record.update(self.validated,publishedAt=time.time())
        self.executor_declaration={'path':str(self.layout.control),'sha256':p.REVIEWED_SHA,
                                  'sourceCommit':p.REVIEWED_COMMIT,'registration':first}
        self.save_operator()
        newer=self.reviewed+b'\n# separately reviewed publisher fixture\n';identity=p.sha(newer);commit='b'*40
        self.addCleanup(patch.stopall)
        patch.dict(p.REVIEWED_EXECUTORS,{identity:commit}).start()
        patch.dict(p.REVIEWED_BUNDLES,{identity:{'sourceCommit':commit,
          'files':{'agentos-release-action.py':identity,'candidate_origin.py':p.sha(self.helper)}}}).start()
        folder=self.layout.control.parent/'reviewed-validator-bundles'/identity
        self.put(folder/'agentos-release-action.py',newer);self.put(folder/'candidate_origin.py',self.helper)
        request=self.request()|{'executorSha256':identity,'sourceCommit':commit}
        second=self.register(request)
        self.publisher_declaration={'path':str(self.layout.control),'sha256':identity,
                                    'sourceCommit':commit,'registration':second}
        self.record.update(publicationExecutor=copy.deepcopy(self.publisher_declaration),publishedAt=time.time())
        self.save_operator()

    def test_distinct_registered_publisher_preserves_original_validation_bytes(self):
        self.mixed_executors()
        before=(self.directory/'validate.receipt.json').read_bytes()
        proof=p.publication(self.layout)
        self.assertEqual(proof['validationExecutorSha256'],p.REVIEWED_SHA)
        self.assertNotEqual(self.record['publicationExecutor']['sha256'],p.REVIEWED_SHA)
        self.assertEqual(before,(self.directory/'validate.receipt.json').read_bytes())

    def test_distinct_publisher_requires_its_exact_captured_active_registration(self):
        self.mixed_executors()
        original=copy.deepcopy(self.record['publicationExecutor'])
        for value in [None,self.executor_declaration,{**original,'sha256':'f'*64},
                      {**original,'registration':self.executor_declaration['registration']}]:
            with self.subTest(value=value):
                self.record['publicationExecutor']=value;self.save_operator()
                with self.assertRaisesRegex(ValueError,'EXECUTOR_UNBOUND'):p.publication(self.layout)

    def test_unreviewed_publisher_bundle_or_changed_registration_fails(self):
        self.mixed_executors()
        identity=self.publisher_declaration['sha256']
        with patch.dict(p.REVIEWED_BUNDLES,{},clear=True):
            with self.assertRaises(ValueError):p.publication(self.layout)
        rid=self.publisher_declaration['registration']['registrationId']
        path=self.layout.registry/(rid+'.json');raw=path.read_bytes()
        self.put(path,raw+b' ')
        with self.assertRaisesRegex(ValueError,'REGISTRATION_CHANGED'):p.publication(self.layout)

    def test_publisher_cannot_precede_its_registration_or_use_superseded_authority(self):
        self.mixed_executors()
        self.record['publishedAt']=self.validated['validatedAt'];self.save_operator()
        with self.assertRaisesRegex(ValueError,'EXECUTOR_UNBOUND'):p.publication(self.layout)
        self.record['publishedAt']=time.time();self.publisher_declaration=self.executor_declaration
        self.record['publicationExecutor']=self.executor_declaration;self.save_operator()
        with self.assertRaisesRegex(ValueError,'EXECUTOR_SUPERSEDED'):p.publication(self.layout)

    def test_publisher_transition_never_rebinds_candidate_source_or_test_evidence(self):
        self.mixed_executors()
        original=copy.deepcopy(self.record)
        for key in ['digest','base','candidate','executorSha256','testReportDigest','validatedAt',
                    'sourceCandidateDigest','sourceManifestDigest','buildInputManifestDigest']:
            with self.subTest(key=key):
                self.record=copy.deepcopy(original);self.record[key]='changed';self.save_operator()
                with self.assertRaises(ValueError):p.publication(self.layout)
        self.record=original;self.save_operator()
        path=self.directory/'publish.intent.json';value=json.loads(path.read_bytes())
        value['input']['parameters']['workspace']='agentos/ocother';self.js(path,value)
        with self.assertRaisesRegex(ValueError,'INTENT_CHANGED'):p.publication(self.layout)

    def test_validation_root_is_traversable_under_restrictive_umask(self):
        request=self.staged_request();before=self.layout.validation_builds.parent.stat().st_mode
        old=os.umask(0o077)
        try:self.register(request)
        finally:os.umask(old)
        self.assertEqual(self.layout.validation_builds.stat().st_mode & 0o777,0o711)
        self.assertEqual(self.layout.validation_builds.parent.stat().st_mode,before)

    def test_unprotected_validation_root_is_not_chmodded_into_trust(self):
        request=self.staged_request();self.layout.validation_builds.mkdir(mode=0o777)
        self.layout.validation_builds.chmod(0o777)
        with self.assertRaisesRegex(ValueError,'UNPROTECTED'):self.register(request)
        self.assertEqual(self.layout.validation_builds.stat().st_mode & 0o777,0o777)
        self.assertEqual(self.layout.control.read_bytes(),self.partial)

    def test_validation_root_symlink_never_changes_target_permissions(self):
        request=self.staged_request();target=self.root/'unrelated';target.mkdir(mode=0o700)
        self.layout.validation_builds.symlink_to(target,target_is_directory=True)
        before=target.stat().st_mode
        with self.assertRaisesRegex(ValueError,'UNPROTECTED'):self.register(request)
        self.assertEqual(target.stat().st_mode,before)


if __name__ == '__main__':
    unittest.main()
