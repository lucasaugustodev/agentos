"""Protected, sanitized source inspection. Temporary files only; no processes run."""
import base64
import fcntl
import hashlib
import json
import pathlib
import unittest
import uuid
from unittest.mock import patch
import workspace_prepare_binding_test as preparation_fixture
manager=preparation_fixture.manager


class SourceBindingInspectionTests(unittest.TestCase):
    tearDown=preparation_fixture.BoundPreparationTests.tearDown
    marker_digest=preparation_fixture.BoundPreparationTests.marker_digest
    replace_json=preparation_fixture.BoundPreparationTests.replace_json

    def setUp(self):
        preparation_fixture.BoundPreparationTests.setUp(self)
        manager.prepare(self.params)
        self.run_id=str(uuid.uuid4());self.fence=str(uuid.uuid4())
        self.run_dir=self.data/'worker-runs'/self.run_id
        self.run_dir.mkdir(parents=True,mode=0o700)
        self.inspection={'action':'inspect','workspace':self.params['workspace'],'sourceBindingRunId':self.run_id}
        self.policy={'version':1,'goalId':str(uuid.uuid4()),'taskId':str(uuid.uuid4()),'runId':self.run_id,
                     'runFence':self.fence,'contextVersion':str(uuid.uuid4()),'workspace':'/workspace',
                     'sourceSnapshotDigest':self.hash((self.target/'.agentos-snapshot.json').read_bytes()),
                     'maxDeadlineMs':3600000,'unrelated':'PRIVATE_POLICY_MUST_NOT_BE_EXPORTED'}
        self.state={'runId':self.run_id,'runFence':self.fence,'workspace':self.params['workspace'],
                    'runtime':'harness','state':'completed','stopped':True,
                    'summary':'PRIVATE_STATE_MUST_NOT_BE_EXPORTED','artifacts':self.artifacts(self.params['workspace'])}
        self.put(self.run_dir/'operation-policy.json',self.policy)
        self.put(self.run_dir/'state.json',self.state)

    @staticmethod
    def hash(raw):return hashlib.sha256(raw).hexdigest()

    def artifacts(self,workspace):
        value=manager.inspect({'workspace':workspace})
        return [{'name':'candidate.json','mime':'application/json','digest':self.hash(json.dumps({k:v for k,v in value.items() if k!='patch'}).encode())},
                {'name':'changes.patch','mime':'text/plain','digest':self.hash(value['patch'].encode())},
                {'name':'other.txt','mime':'text/plain','digest':'f'*64,'private':'PRIVATE_ARTIFACT_MUST_NOT_BE_EXPORTED'}]

    def put(self,path,value,mode=0o600):
        if path.exists() or path.is_symlink():path.unlink()
        path.write_text(json.dumps(value));path.chmod(mode)

    def inspect(self):return manager.inspect(self.inspection)

    def rejected(self,code):
        with self.assertRaisesRegex(ValueError,'^'+code+'$'):self.inspect()

    def test_returns_sanitized_runtime_binding_and_exact_marker_only(self):
        value=self.inspect();encoded=json.dumps(value)
        self.assertEqual(value['runId'],self.run_id)
        self.assertEqual(value['state']['artifacts'],self.state['artifacts'][:2])
        self.assertEqual(value['state']['runFence'],self.fence)
        self.assertEqual(value['admission']['contextVersion'],self.policy['contextVersion'])
        self.assertEqual(value['admission']['policyDigest'],self.hash((self.run_dir/'operation-policy.json').read_bytes()))
        raw=(self.target/'.agentos-snapshot.json').read_bytes()
        self.assertEqual(base64.b64decode(value['markerBase64']),raw)
        self.assertEqual(value['generation']['snapshotDigest'],self.hash(raw))
        self.assertEqual(value['generation']['sourceSnapshotDigest'],self.params['expectedSourceSnapshotDigest'])
        self.assertFalse(value['legacyParentGeneration'])
        self.assertEqual(value['candidate']['candidateDigest'],manager.source_hash(manager.sources(self.target)))
        self.assertNotIn('patch',value['candidate'])
        self.assertNotIn('PRIVATE_',encoded)
        self.assertNotIn('maxDeadlineMs',encoded)
        self.assertNotIn('accepted candidate',encoded)

    def test_legacy_missing_parent_generation_is_explicit_not_reconstructed(self):
        self.inspection['workspace']='sample/osource'
        self.state['workspace']='sample/osource';self.state['artifacts']=self.artifacts('sample/osource')
        self.policy['sourceSnapshotDigest']=self.marker_digest()
        self.put(self.run_dir/'state.json',self.state);self.put(self.run_dir/'operation-policy.json',self.policy)
        value=self.inspect()
        self.assertTrue(value['legacyParentGeneration'])
        self.assertIsNone(value['generation']['sourceSnapshotDigest'])
        self.assertNotIn('sourceSnapshotDigest',json.loads(base64.b64decode(value['markerBase64'])))

    def test_run_identifier_cannot_select_arbitrary_paths(self):
        for run in ['../secret','/etc/secret',self.run_id.upper(),None]:
            self.inspection['sourceBindingRunId']=run
            self.rejected('SOURCE_BINDING_IDENTITY_INVALID')

    def test_workspace_symlink_is_rejected(self):
        other=self.target.with_name('osaved');self.target.rename(other);self.target.symlink_to(other,target_is_directory=True)
        self.rejected('INVALID_WORKSPACE')

    def test_policy_symlink_is_not_followed(self):
        outside=self.root/'private-policy';outside.write_text('PRIVATE_NOT_JSON')
        file=self.run_dir/'operation-policy.json';file.unlink();file.symlink_to(outside)
        self.rejected('PREPARATION_RECEIPT_UNPROTECTED')
        self.assertEqual(outside.read_text(),'PRIVATE_NOT_JSON')

    def test_marker_symlink_is_not_followed(self):
        outside=self.root/'private-marker';outside.write_text('PRIVATE_NOT_JSON')
        file=self.target/'.agentos-snapshot.json';file.unlink();file.symlink_to(outside)
        self.rejected('PREPARATION_RECEIPT_UNPROTECTED')

    def test_run_directory_symlink_is_rejected(self):
        other=self.run_dir.with_name('saved');self.run_dir.rename(other);self.run_dir.symlink_to(other,target_is_directory=True)
        self.rejected('PREPARATION_DIRECTORY_UNPROTECTED')

    def test_data_and_workspace_ancestors_must_be_controller_protected(self):
        for folder in [self.data/'worker-runs',self.target.parent,self.root]:
            folder.chmod(0o777)
            try:self.rejected('PREPARATION_DIRECTORY_UNPROTECTED')
            finally:folder.chmod(0o700)

    def test_policy_and_state_require_runtime_private_modes(self):
        for file in [self.run_dir/'operation-policy.json',self.run_dir/'state.json']:
            file.chmod(0o644)
            try:self.rejected('PREPARATION_RECEIPT_UNPROTECTED')
            finally:file.chmod(0o600)

    def test_policy_identity_cannot_disagree_with_run_fence_or_workspace(self):
        for key,value in [('runId',str(uuid.uuid4())),('runFence',str(uuid.uuid4())),('workspace','sample/other'),
                          ('goalId','invalid'),('taskId','invalid'),('contextVersion','invalid'),('version',True)]:
            self.put(self.run_dir/'operation-policy.json',{**self.policy,key:value})
            self.rejected('SOURCE_BINDING_POLICY_IDENTITY')

    def test_state_must_be_completed_stopped_and_match_requested_identity(self):
        for key,value in [('runId',str(uuid.uuid4())),('workspace','sample/other'),('runtime','opencode'),
                          ('state','running'),('stopped',False),('stopped',1)]:
            self.put(self.run_dir/'state.json',{**self.state,key:value})
            self.rejected('SOURCE_BINDING_RUN_NOT_COMPLETED')

    def test_policy_must_name_exact_current_marker_generation(self):
        self.put(self.run_dir/'operation-policy.json',{**self.policy,'sourceSnapshotDigest':'0'*64})
        self.rejected('SOURCE_BINDING_GENERATION_CHANGED')

    def test_marker_replacement_cannot_inherit_old_run_authority(self):
        marker=self.target/'.agentos-snapshot.json';value=json.loads(marker.read_text())
        value['createdAt']='2020-01-01T00:00:00+00:00';self.put(marker,value,0o444)
        self.rejected('SOURCE_BINDING_GENERATION_CHANGED')

    def test_marker_schema_is_closed_before_any_raw_bytes_are_returned(self):
        marker=self.target/'.agentos-snapshot.json';value=json.loads(marker.read_text())
        for key,item in [('prompt','PRIVATE_MARKER_MUST_NOT_BE_EXPORTED'),('workspace','sample/other'),
                         ('sourceWorkspace','other/project'),('createdAt','PRIVATE_NOT_A_TIMESTAMP'),
                         ('sourceSnapshotDigest',None),('baseDigest','bad'),('preparation',{'version':True})]:
            self.put(marker,{**value,key:item},0o444)
            self.policy['sourceSnapshotDigest']=self.hash(marker.read_bytes())
            self.put(self.run_dir/'operation-policy.json',self.policy)
            self.rejected('SOURCE_BINDING_MARKER_INVALID')

    def test_duplicate_marker_json_keys_are_rejected(self):
        marker=self.target/'.agentos-snapshot.json';raw=marker.read_text()
        marker.unlink();marker.write_text(raw[:-1]+',"workspace":"sample/otarget"}');marker.chmod(0o444)
        self.rejected('PREPARATION_RECEIPT_INVALID')

    def test_required_runtime_artifacts_are_unambiguous_and_not_worker_claims(self):
        artifacts=self.state['artifacts']
        variants=[artifacts[1:],artifacts+[artifacts[0]],
                  [{**artifacts[0],'name':'worker-candidate.json'},*artifacts[1:]],
                  [{**artifacts[0],'digest':'bad'},*artifacts[1:]],
                  [{**artifacts[0],'mime':'text/plain'},*artifacts[1:]]]
        for value in variants:
            self.put(self.run_dir/'state.json',{**self.state,'artifacts':value})
            self.rejected('SOURCE_BINDING_RUNTIME_ARTIFACTS')

    def test_missing_policy_does_not_fabricate_admission_from_state(self):
        (self.run_dir/'operation-policy.json').unlink()
        self.rejected('PREPARATION_RECEIPT_MISSING')

    def test_modified_baseline_is_rejected(self):
        file=self.baseline/'src/main.py';file.chmod(0o644);file.write_text('changed baseline\n')
        self.rejected('PREPARATION_BASELINE_CHANGED')

    def test_live_workspace_owner_lock_prevents_inspection(self):
        lock=self.data/'locks'/('workspace-'+manager.digest_key(self.inspection['workspace']))
        with lock.open('a') as handle:
            fcntl.flock(handle,fcntl.LOCK_EX|fcntl.LOCK_NB)
            self.rejected('SOURCE_BUSY')

    def test_source_change_during_inventory_is_rejected(self):
        original=manager.sources;changed=False
        def inventory(root):
            nonlocal changed
            value=original(root)
            if root==self.target and not changed:
                changed=True;(root/'src/main.py').write_text('changed during inspection\n')
            return value
        with patch.object(manager,'sources',side_effect=inventory):
            self.rejected('SOURCE_BINDING_OBSERVATION_CHANGED')

    def test_context_change_during_inventory_is_rejected(self):
        original=manager.sources;changed=False
        def inventory(root):
            nonlocal changed
            value=original(root)
            if root==self.target and not changed:
                changed=True;self.put(self.run_dir/'operation-policy.json',{**self.policy,'contextVersion':str(uuid.uuid4())})
            return value
        with patch.object(manager,'sources',side_effect=inventory):
            self.rejected('SOURCE_BINDING_OBSERVATION_CHANGED')

    def test_state_change_during_inventory_is_rejected(self):
        original=manager.sources;changed=False
        def inventory(root):
            nonlocal changed
            value=original(root)
            if root==self.target and not changed:
                changed=True;self.put(self.run_dir/'state.json',{**self.state,'runFence':str(uuid.uuid4())})
            return value
        with patch.object(manager,'sources',side_effect=inventory):
            self.rejected('SOURCE_BINDING_OBSERVATION_CHANGED')

    def test_normal_inspection_keeps_original_contract(self):
        value=manager.inspect({'workspace':self.params['workspace']})
        self.assertEqual(set(value),{'baseDigest','candidateDigest','files','patch'})
        self.assertNotIn('admission',value)


if __name__=='__main__':unittest.main()
