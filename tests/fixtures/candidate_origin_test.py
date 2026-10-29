import base64
import contextlib
import copy
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


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


origin = module('origin_fixture', ROOT/'scripts/candidate_origin.py')
manager = module('origin_workspace_manager', ROOT/'bridge/workspace-manager.py')


class CandidateOriginTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.temp.name)
        self.layout = origin.Layout(self.root)
        self.stack = contextlib.ExitStack()
        for target, name, value in [(origin, 'OWNER_UID', os.getuid()),
                                    (origin, 'PROTECTION_ROOT', self.root),
                                    (manager, 'ROOT', self.layout.workspaces),
                                    (manager, 'DATA', self.layout.data)]:
            self.stack.enter_context(patch.object(target, name, value))
        self.stack.enter_context(patch.object(manager.storage_capacity, 'reserve', side_effect=lambda *a: contextlib.nullcontext()))
        for path in [self.layout.releases, self.layout.receipts, self.layout.workspaces/'agentos', self.layout.data]:
            path.mkdir(parents=True)
        self.files = {'.gitignore': b'dist/\n', 'server/main.ts': b'export const version = 1;\n',
                      'scripts/guard.py': b'# reviewed source\n', 'dist/main.js': b'console.log(1);\n'}
        self.publish(self.files)

    def tearDown(self):
        self.stack.close()
        self.temp.cleanup()

    def put(self, path, raw):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
        path.chmod(0o600)

    def js(self, path, value):
        self.put(path, origin.canonical(value))

    def publish(self, files):
        release = self.layout.releases/('managed-'+str(uuid.uuid4()))
        for name, raw in files.items():
            self.put(release/name, raw)
        prior = str(self.layout.active.resolve()) if self.layout.active.exists() else str(release)
        if self.layout.active.is_symlink():
            self.layout.active.unlink()
        self.layout.active.symlink_to(release)
        record = {'candidate':str(release),'digest':origin.release_digest(release),'base':prior,
                  'previous':prior,'validatedAt':time.time()-2,'publishedAt':time.time()-1}
        self.js(self.layout.receipts/(str(uuid.uuid4())+'.json'), record)
        self.js(self.layout.receipts/'pending-workers.json', record)
        self.current = release
        self.digest = record['digest']
        self.source = 'agentos/src'+self.digest[:16]
        shutil.copytree(release, self.layout.workspaces/self.source)
        return record

    def prepare(self, key='ocfixture', source=None):
        workspace = 'agentos/'+key
        manager.prepare({'workspace':workspace,'sourceWorkspace':source or self.source})
        return workspace

    def capture(self, workspace):
        digest = manager.source_hash(manager.sources(manager.workspace(workspace)))
        return origin.capture_origin(workspace, digest, self.layout)

    def record(self, proof):
        return {'base':str(self.current),'sourceCandidateDigest':proof['candidateDigest'],'candidateOrigin':proof}

    def test_real_prepare_with_current_release_and_reviewed_change_can_publish(self):
        workspace = self.prepare()
        self.put(manager.workspace(workspace)/'server/main.ts', b'export const version = 2;\n')
        proof = self.capture(workspace)
        self.assertTrue(proof['publishEligible'])
        self.assertEqual(proof['published']['releaseDigest'], self.digest)
        self.assertEqual(origin.require_publish_origin(self.record(proof), self.layout), proof)

    def test_old_candidate_validated_after_new_publication_remains_diagnostic(self):
        workspace = self.prepare()
        newer = self.files | {'server/security.ts':b'export const preserved = true;\n'}
        self.publish(newer)
        proof = self.capture(workspace)
        self.assertFalse(proof['publishEligible'])
        self.assertEqual(proof['code'], 'CANDIDATE_ORIGIN_REBASE_REQUIRED')
        with self.assertRaisesRegex(ValueError, 'REBASE_REQUIRED'):
            origin.require_publish_origin(self.record(proof), self.layout)
        self.assertNotIn('published', proof)  # Never invent a historical origin.

    def test_publication_changed_after_valid_ci_invalidates_origin(self):
        workspace = self.prepare()
        record = self.record(self.capture(workspace))
        self.publish(self.files | {'server/new.ts':b'export const added = true;\n'})
        with self.assertRaisesRegex(ValueError, 'PUBLICATION_CHANGED'):
            origin.require_publish_origin(record, self.layout)

    def test_explicit_new_prepare_rebase_preserves_current_then_applies_changes(self):
        prior = self.prepare('ocprior')
        self.put(manager.workspace(prior)/'server/main.ts', b'export const version = 2;\n')
        self.publish(self.files | {'server/security.ts':b'export const preserved = true;\n'})
        rebased = self.prepare('ocrebased')
        self.put(manager.workspace(rebased)/'server/main.ts', b'export const version = 2;\n')
        self.assertFalse(self.capture(prior)['publishEligible'])
        proof = self.capture(rebased)
        self.assertTrue(proof['publishEligible'])
        self.assertTrue((manager.workspace(rebased)/'server/security.ts').exists())
        origin.require_publish_origin(self.record(proof), self.layout)

    def test_accepted_source_chain_retains_origin_without_discarding_patch(self):
        producer = self.prepare('ocproducer')
        self.put(manager.workspace(producer)/'server/main.ts', b'export const version = 2;\n')
        consumer = self.prepare('occonsumer', producer)
        proof = self.capture(consumer)
        self.assertTrue(proof['publishEligible'])
        self.assertEqual([p['workspace'] for p in proof['preparationChain']], [consumer, producer])
        self.assertEqual((manager.workspace(consumer)/'server/main.ts').read_bytes(), b'export const version = 2;\n')

    def test_mutable_worker_claim_is_not_origin_authority(self):
        workspace = self.prepare()
        self.publish(self.files | {'server/fix.ts':b'// new fix\n'})
        self.js(manager.workspace(workspace)/'origin.json', {'sourceBaseDigest':self.digest,'publishEligible':True})
        self.assertFalse(self.capture(workspace)['publishEligible'])

    def test_baseline_hash_change_is_rejected(self):
        workspace = self.prepare()
        path = self.layout.data/'workspace-baselines'/origin.sha(workspace.encode())/'server/main.ts'
        self.put(path, b'// forged baseline\n')
        self.assertEqual(self.capture(workspace)['code'], 'CANDIDATE_ORIGIN_PREPARATION_CHANGED')

    def test_snapshot_rewrite_after_ci_invalidates_receipt(self):
        workspace = self.prepare()
        record = self.record(self.capture(workspace))
        path = manager.workspace(workspace)/'.agentos-snapshot.json'
        value = json.loads(path.read_bytes())
        value['createdAt'] = 'changed by an operator after CI'
        self.js(path, value)
        with self.assertRaisesRegex(ValueError, 'PUBLICATION_CHANGED'):
            origin.require_publish_origin(record, self.layout)

    def test_ancestry_cycle_is_bounded(self):
        workspace = self.prepare()
        self.publish(self.files | {'server/new.ts':b'// new\n'})
        path = manager.workspace(workspace)/'.agentos-snapshot.json'
        value = json.loads(path.read_bytes());value['sourceWorkspace'] = workspace
        self.js(path, value)
        self.assertEqual(self.capture(workspace)['code'], 'CANDIDATE_ORIGIN_PARENT_GENERATION_UNBOUND')

    def test_writable_controller_baseline_is_not_authority(self):
        workspace = self.prepare()
        path = self.layout.data/'workspace-baselines'/origin.sha(workspace.encode())/'server/main.ts'
        path.chmod(0o666)
        self.assertEqual(self.capture(workspace)['code'], 'CANDIDATE_ORIGIN_UNPROTECTED')

    def test_baseline_symlink_is_rejected(self):
        workspace = self.prepare()
        path = self.layout.data/'workspace-baselines'/origin.sha(workspace.encode())/'server/main.ts'
        path.unlink();path.symlink_to(self.current/'server/main.ts')
        self.assertFalse(self.capture(workspace)['publishEligible'])

    def freeze(self, workspace):
        action = str(uuid.uuid4())
        frozen_id = 'agentos/ocv'+action.replace('-', '')
        files = manager.sources(manager.workspace(workspace))
        digest = manager.source_hash(files)
        for name, raw in files.items():
            self.put(self.layout.workspaces/frozen_id/name, raw)
        candidate = origin.canonical({'candidateDigest':digest,'files':[{'path':k,'sha256':origin.sha(v)} for k,v in sorted(files.items())]})
        request = {'actionId':action,'workspace':workspace,'candidateDigest':digest,
                   'candidateArtifactDigest':origin.sha(candidate),'candidateSize':len(candidate),
                   'candidateBase64':base64.b64encode(candidate).decode()}
        directory = self.layout.data/'candidate-validations'/action
        self.js(directory/'request.json', request)
        self.js(directory/'frozen.json', {k:v for k,v in request.items() if k!='candidateBase64'} | {'frozenWorkspace':frozen_id})
        self.put(directory/'candidate.json', candidate)
        return frozen_id, directory

    def test_frozen_broker_candidate_inherits_only_bound_original_origin(self):
        workspace = self.prepare()
        frozen, _ = self.freeze(workspace)
        proof = self.capture(frozen)
        self.assertTrue(proof['publishEligible'])
        self.assertEqual(proof['originalWorkspace'], workspace)
        self.assertEqual(len(proof['frozenBinding']), 3)
        origin.require_publish_origin(self.record(proof), self.layout)

    def test_frozen_origin_cannot_be_retargeted(self):
        workspace = self.prepare()
        frozen, directory = self.freeze(workspace)
        value = json.loads((directory/'frozen.json').read_bytes())
        value['workspace'] = 'agentos/ocother'
        self.js(directory/'frozen.json', value)
        self.assertEqual(self.capture(frozen)['code'], 'CANDIDATE_ORIGIN_FROZEN_BINDING_CHANGED')

    def test_historical_validation_has_no_retroactive_origin(self):
        with self.assertRaisesRegex(ValueError, 'REBASE_REQUIRED'):
            origin.require_publish_origin({'base':str(self.current),'digest':'a'*64}, self.layout)

    def test_candidate_identity_must_match_ci_receipt(self):
        workspace = self.prepare()
        record = self.record(self.capture(workspace))
        record['sourceCandidateDigest'] = 'b'*64
        with self.assertRaisesRegex(ValueError, 'RECEIPT_UNBOUND'):
            origin.require_publish_origin(record, self.layout)

    def test_claimed_source_base_hash_without_preparation_is_not_enough(self):
        unmanaged = 'agentos/ocunmanaged'
        shutil.copytree(self.current, self.layout.workspaces/unmanaged)
        self.assertFalse(self.capture(unmanaged)['publishEligible'])

    def test_source_digest_matches_real_manager_unicode_and_filters(self):
        workspace = self.prepare()
        root = manager.workspace(workspace)
        self.put(root/'server/café.ts', 'export const café = true;\n'.encode())
        self.put(root/'evidence/report.json', b'{}')
        self.put(root/'cache.pyc', b'generated')
        self.assertEqual(origin.source_digest(origin.source_bytes(root)), manager.source_hash(manager.sources(root)))

    def test_changed_native_publication_receipt_blocks_origin(self):
        workspace = self.prepare()
        receipt = next(p for p in self.layout.receipts.glob('*.json') if p.name!='pending-workers.json')
        value = json.loads(receipt.read_bytes());value['digest'] = 'c'*64
        self.js(receipt, value)
        self.assertEqual(self.capture(workspace)['code'], 'CANDIDATE_ORIGIN_PUBLICATION_UNBOUND')

    def bundle(self):
        guard = self.layout.control/'agentos-release-action.py'
        self.put(guard, b'# reviewed fixture guard\n')
        helper = self.layout.control/'candidate_origin.py'
        self.put(helper, b'# reviewed fixture origin\n')
        hashes = {guard.name:origin.sha(guard.read_bytes()),helper.name:origin.sha(helper.read_bytes())}
        rid = str(uuid.uuid4());directory = self.layout.control/'validator-registrations'
        evidence = {'release':{'digest':self.digest}}
        intent = {'registrationId':rid,'expectedRegisteredSha256':'a'*64,
                  'expectedReleaseDigest':self.digest,'expectedPublicationProofDigest':origin.sha(origin.canonical(evidence))}
        self.js(directory/(rid+'.intent.json'), intent)
        record = {'version':2,'kind':'reviewed_validator_transition','state':'installed','registrationId':rid,
                  'executorSha256':hashes[guard.name],'previousExecutorSha256':'a'*64,
                  'bundleManifest':hashes,'bundleDigest':origin.sha(origin.canonical(hashes)),
                  'intentSha256':origin.sha(origin.canonical(intent)),'publicationEvidence':evidence}
        self.js(directory/(rid+'.json'),record)
        pointer = {'version':1,'registrationId':rid,'registrationDigest':origin.sha(origin.canonical(record))}
        self.js(directory/'current.json',pointer)
        return guard, helper, hashes, directory, rid, record

    def check_bundle(self, bundle):
        guard, _, hashes, _, _, _ = bundle
        return origin.require_registered_bundle(guard,hashes['agentos-release-action.py'],hashes['candidate_origin.py'],self.layout)

    def test_installed_bundle_is_verified_before_direct_cycle_execution(self):
        bundle = self.bundle()
        result = self.check_bundle(bundle)
        self.assertEqual(result['registrationId'],bundle[4])
        self.assertEqual(result['bundleDigest'],bundle[5]['bundleDigest'])

    def test_partial_bundle_installation_without_pointer_is_closed(self):
        bundle = self.bundle();(bundle[3]/'current.json').unlink()
        with self.assertRaises(OSError):
            self.check_bundle(bundle)

    def test_changed_bundle_helper_is_rejected(self):
        bundle = self.bundle();self.put(bundle[1],b'# changed helper\n')
        with self.assertRaisesRegex(ValueError,'BUNDLE_BYTES_CHANGED'):
            self.check_bundle(bundle)

    def test_changed_bundle_intent_is_rejected(self):
        bundle = self.bundle();path=bundle[3]/(bundle[4]+'.intent.json')
        value=json.loads(path.read_bytes());value['expectedRegisteredSha256']='b'*64;self.js(path,value)
        with self.assertRaisesRegex(ValueError,'BUNDLE_INTENT_CHANGED'):
            self.check_bundle(bundle)

    def test_service_writable_bundle_record_is_rejected(self):
        bundle = self.bundle();(bundle[3]/(bundle[4]+'.json')).chmod(0o666)
        with self.assertRaisesRegex(ValueError,'UNPROTECTED'):
            self.check_bundle(bundle)

    def test_unregistered_same_named_executor_path_is_rejected(self):
        bundle = self.bundle()
        with self.assertRaisesRegex(ValueError,'EXECUTOR_PATH'):
            origin.require_registered_bundle(self.current/'agentos-release-action.py',bundle[2]['agentos-release-action.py'],
                                             bundle[2]['candidate_origin.py'],self.layout)

    def test_reused_parent_workspace_id_cannot_upgrade_old_child_origin(self):
        producer = self.prepare('ocproducer')
        self.put(manager.workspace(producer)/'server/main.ts', b'export const version = 2;\n')
        consumer = self.prepare('occonsumer', producer)
        original_child = manager.sources(manager.workspace(consumer))
        self.publish(self.files | {'server/security.ts':b'export const requiredFix = true;\n'})
        shutil.rmtree(manager.workspace(producer))
        self.prepare('ocproducer')  # Same ID, new controller generation on release B.
        proof = self.capture(consumer)
        self.assertFalse(proof['publishEligible'])
        self.assertEqual(proof['code'],'CANDIDATE_ORIGIN_PARENT_GENERATION_CHANGED')
        self.assertEqual(manager.sources(manager.workspace(consumer)),original_child)
        self.assertNotIn('server/security.ts',original_child)

    def test_legacy_chained_snapshot_without_parent_pin_requires_rebase(self):
        producer=self.prepare('ocproducer')
        self.put(manager.workspace(producer)/'server/main.ts', b'export const version = 2;\n')
        consumer=self.prepare('occonsumer',producer)
        path=manager.workspace(consumer)/'.agentos-snapshot.json'
        value=json.loads(path.read_bytes());value.pop('sourceSnapshotDigest');self.js(path,value)
        proof=self.capture(consumer)
        self.assertFalse(proof['publishEligible'])
        self.assertEqual(proof['code'],'CANDIDATE_ORIGIN_PARENT_GENERATION_UNBOUND')

    def test_existing_prepare_does_not_retrofit_parent_generation(self):
        producer=self.prepare('ocproducer')
        consumer=self.prepare('occonsumer',producer)
        path=manager.workspace(consumer)/'.agentos-snapshot.json'
        value=json.loads(path.read_bytes());value.pop('sourceSnapshotDigest');self.js(path,value)
        before=path.read_bytes()
        replay=manager.prepare({'workspace':consumer,'sourceWorkspace':producer})
        self.assertNotIn('sourceSnapshotDigest',replay)
        self.assertEqual(path.read_bytes(),before)

    def test_direct_exact_current_source_remains_valid_without_parent_pin(self):
        producer=self.prepare('ocproducer')
        consumer=self.prepare('occonsumer',producer)
        path=manager.workspace(consumer)/'.agentos-snapshot.json'
        value=json.loads(path.read_bytes());value.pop('sourceSnapshotDigest');self.js(path,value)
        self.assertTrue(self.capture(consumer)['publishEligible'])


if __name__ == '__main__':
    unittest.main()
