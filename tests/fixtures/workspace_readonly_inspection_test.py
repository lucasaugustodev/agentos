"""Generation and prepared-clone inspection with real files and lock ownership."""
import base64
import fcntl
import json
import pathlib
import unittest
import uuid
from unittest.mock import patch
import workspace_prepare_binding_test as preparation_fixture
manager=preparation_fixture.manager


class ReadonlyWorkspaceInspectionTests(unittest.TestCase):
    setUp=preparation_fixture.BoundPreparationTests.setUp
    tearDown=preparation_fixture.BoundPreparationTests.tearDown
    marker_digest=preparation_fixture.BoundPreparationTests.marker_digest
    replace_json=preparation_fixture.BoundPreparationTests.replace_json

    def test_base_generation_needs_no_run_policy_or_parent_traversal(self):
        self.assertFalse((self.data/'worker-runs').exists())
        value=manager.inspect_generation({'workspace':'sample/osource'})
        self.assertEqual(value['workspace'],'sample/osource')
        self.assertEqual(value['candidate']['candidateDigest'],manager.source_hash(manager.sources(self.source)))
        self.assertEqual(value['generation']['snapshotDigest'],self.marker_digest())
        self.assertEqual(base64.b64decode(value['markerBase64']),(self.source/'.agentos-snapshot.json').read_bytes())
        self.assertTrue(value['legacyParentGeneration'])
        self.assertIsNone(value['generation']['sourceSnapshotDigest'])
        self.assertNotIn('policy',value);self.assertNotIn('admission',value);self.assertNotIn('process',value)
        self.assertNotIn('patch',value['candidate'])

    def test_base_inspection_has_no_source_or_marker_writes(self):
        before=manager.sources(self.source);marker=(self.source/'.agentos-snapshot.json').read_bytes()
        manager.inspect_generation({'workspace':'sample/osource'})
        self.assertEqual(manager.sources(self.source),before)
        self.assertEqual((self.source/'.agentos-snapshot.json').read_bytes(),marker)

    def test_base_inspection_rejects_unsafe_marker_and_baseline(self):
        marker=self.source/'.agentos-snapshot.json';value=json.loads(marker.read_text())
        self.replace_json(marker,{**value,'prompt':'PRIVATE'})
        with self.assertRaisesRegex(ValueError,'SOURCE_BINDING_MARKER_INVALID'):
            manager.inspect_generation({'workspace':'sample/osource'})
        self.replace_json(marker,value)
        baseline=self.data/'workspace-baselines'/manager.digest_key('sample/osource')
        (baseline/'src/main.py').write_text('changed\n')
        with self.assertRaisesRegex(ValueError,'PREPARATION_BASELINE_CHANGED'):
            manager.inspect_generation({'workspace':'sample/osource'})

    def test_base_inspection_refuses_live_workspace_lock(self):
        file=self.data/'locks'/('workspace-'+manager.digest_key('sample/osource'))
        with file.open('a') as handle:
            fcntl.flock(handle,fcntl.LOCK_EX|fcntl.LOCK_NB)
            with self.assertRaisesRegex(ValueError,'SOURCE_BUSY'):manager.inspect_generation({'workspace':'sample/osource'})

    def test_base_inspection_detects_source_change_during_inventory(self):
        original=manager.sources;changed=False
        def inventory(root):
            nonlocal changed
            result=original(root)
            if root==self.source and not changed:
                changed=True;(root/'src/main.py').write_text('changed during inspection\n')
            return result
        with patch.object(manager,'sources',side_effect=inventory):
            with self.assertRaisesRegex(ValueError,'SOURCE_BINDING_OBSERVATION_CHANGED'):
                manager.inspect_generation({'workspace':'sample/osource'})

    def test_absence_is_confirmed_only_without_destination_baseline_or_receipt(self):
        value=manager.inspect_preparation(self.params)
        self.assertEqual(value['exists'],False)
        self.assertIs(value['confirmed'],True)
        self.assertEqual(value['preparationIdentity'],self.params['preparationIdentity'])
        self.assertFalse(self.target.exists());self.assertFalse(self.receipt.exists())

    def test_absence_cannot_be_claimed_through_unprotected_baseline_parent(self):
        (self.data/'workspace-baselines').chmod(0o777)
        with self.assertRaisesRegex(ValueError,'PREPARATION_DIRECTORY_UNPROTECTED'):
            manager.inspect_preparation(self.params)

    def test_prepared_clone_returns_exact_receipt_and_marker(self):
        prepared=manager.prepare(self.params);value=manager.inspect_preparation(self.params)
        self.assertTrue(value['exists']);self.assertFalse(value['destinationChanged'])
        self.assertEqual(value['preparationReceipt'],prepared['preparationReceipt'])
        self.assertEqual(base64.b64decode(value['markerBase64']),(self.target/'.agentos-snapshot.json').read_bytes())
        self.assertEqual(value['candidate']['candidateDigest'],self.params['expectedSourceDigest'])

    def test_modified_clone_is_observed_without_recopy_or_live_origin_requirement(self):
        manager.prepare(self.params)
        (self.target/'src/main.py').write_text('retained retry work\n')
        self.source.rename(self.root/'source-no-longer-in-project')
        value=manager.inspect_preparation(self.params)
        self.assertTrue(value['exists']);self.assertTrue(value['destinationChanged'])
        self.assertEqual(value['candidate']['baseDigest'],self.params['expectedSourceDigest'])
        self.assertEqual((self.target/'src/main.py').read_text(),'retained retry work\n')

    def test_receipt_only_partial_write_reserves_both_uuid_and_destination(self):
        original=manager.write_preparation_json
        def injected(path,value):
            if path==self.receipt:
                path.write_text('{');path.chmod(0o444)
                raise OSError('partial receipt')
            return original(path,value)
        with patch.object(manager,'write_preparation_json',side_effect=injected):
            with self.assertRaisesRegex(ValueError,'PREPARATION_EFFECT_UNCONFIRMED'):manager.prepare(self.params)
        with self.assertRaisesRegex(ValueError,'PREPARATION_PARTIAL_STATE'):manager.inspect_preparation(self.params)
        for params in [{**self.params,'preparationIdentity':str(uuid.uuid4())},{**self.params,'workspace':'sample/oother'}]:
            with self.assertRaisesRegex(ValueError,'PREPARATION_BINDING_MISMATCH'):manager.prepare(params)
            with self.assertRaisesRegex(ValueError,'PREPARATION_BINDING_MISMATCH'):manager.inspect_preparation(params)
        self.assertEqual(self.receipt.read_text(),'{')
        self.assertFalse(self.target.exists())

    def test_orphan_baseline_and_destination_are_partial_not_absent(self):
        self.baseline.mkdir()
        with self.assertRaisesRegex(ValueError,'PREPARATION_PARTIAL_STATE'):manager.inspect_preparation(self.params)
        self.baseline.rmdir();self.target.mkdir()
        with self.assertRaisesRegex(ValueError,'PREPARATION_PARTIAL_STATE'):manager.inspect_preparation(self.params)

    def test_receipt_tampering_or_wrong_binding_is_never_not_found(self):
        manager.prepare(self.params)
        receipt=json.loads(self.receipt.read_text());receipt['baselineDigest']='0'*64
        self.replace_json(self.receipt,receipt)
        with self.assertRaisesRegex(ValueError,'PREPARATION_BINDING_MISMATCH'):manager.inspect_preparation(self.params)
        with self.assertRaisesRegex(ValueError,'PREPARATION_BINDING_MISMATCH'):
            manager.inspect_preparation({**self.params,'preparationIdentity':str(uuid.uuid4())})

    def test_readonly_preparation_requires_complete_binding(self):
        for name in ['expectedSourceDigest','expectedSourceSnapshotDigest','preparationIdentity']:
            value=dict(self.params);del value[name]
            with self.assertRaisesRegex(ValueError,'PREPARATION_BINDING_INVALID'):manager.inspect_preparation(value)

    def test_readonly_preparation_rejects_active_destination_owner(self):
        manager.prepare(self.params)
        file=self.data/'locks'/('workspace-'+manager.digest_key(self.params['workspace']))
        with file.open('a') as handle:
            fcntl.flock(handle,fcntl.LOCK_EX|fcntl.LOCK_NB)
            with self.assertRaisesRegex(ValueError,'SOURCE_BUSY'):manager.inspect_preparation(self.params)

    def test_readonly_preparation_detects_concurrent_changes(self):
        manager.prepare(self.params);original=manager.sources;changed=False
        def inventory(root):
            nonlocal changed
            result=original(root)
            if root==self.target and not changed:
                changed=True;(root/'src/main.py').write_text('new during inspection\n')
            return result
        with patch.object(manager,'sources',side_effect=inventory):
            with self.assertRaisesRegex(ValueError,'SOURCE_BINDING_OBSERVATION_CHANGED'):manager.inspect_preparation(self.params)

    def test_prepublication_receipt_layout_is_not_silently_migrated(self):
        self.receipt.parent.mkdir(mode=0o700)
        old=self.receipt.parent/(self.params['preparationIdentity']+'.json')
        old.write_text('{}');old.chmod(0o444)
        with self.assertRaisesRegex(ValueError,'PREPARATION_RECEIPT_LAYOUT_UNSUPPORTED'):manager.inspect_preparation(self.params)
        self.assertEqual(old.read_text(),'{}')


if __name__=='__main__':unittest.main()
