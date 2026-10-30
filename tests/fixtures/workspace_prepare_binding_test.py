"""Bound preparation transitions, using real temporary files, locks and Git.

Run on the isolated Linux development server. No service, database, production
workspace, provider or credential is needed. Only storage reservation is mocked.
"""
import contextlib
import fcntl
import hashlib
import importlib.util
import json
import os
import pathlib
import stat
import tempfile
import unittest
import uuid
from unittest.mock import patch

ROOT = pathlib.Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('bound_workspace_manager', ROOT/'bridge/workspace-manager.py')
manager = importlib.util.module_from_spec(spec)
spec.loader.exec_module(manager)


class BoundPreparationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.temp.name)
        self.workspaces = self.root/'workspaces'
        self.data = self.root/'controller'
        (self.workspaces/'sample').mkdir(parents=True)
        self.data.mkdir()
        self.stack = contextlib.ExitStack()
        for name,value in [('ROOT',self.workspaces),('DATA',self.data),('PROTECTION_ROOT',self.root)]:
            self.stack.enter_context(patch.object(manager,name,value))
        self.stack.enter_context(patch.object(manager.storage_capacity,'reserve',side_effect=lambda *a:contextlib.nullcontext()))
        raw = self.workspaces/'sample/raw'
        (raw/'src/nested').mkdir(parents=True)
        (raw/'.gitignore').write_text('assets/\nevidence/\ndata/\nnode_modules/\n.agentos-snapshot.json\n')
        (raw/'src/main.py').write_text('original\n')
        (raw/'src/nested/second.py').write_text('second\n')
        # Produce a real legacy generation. Never fabricate historical ancestry.
        manager.prepare({'workspace':'sample/osource','sourceWorkspace':'sample/raw'})
        self.source = self.workspaces/'sample/osource'
        (self.source/'src/main.py').write_text('accepted candidate\n')
        self.params = {'workspace':'sample/otarget','sourceWorkspace':'sample/osource',
                       'expectedSourceDigest':manager.source_hash(manager.sources(self.source)),
                       'expectedSourceSnapshotDigest':self.marker_digest(),
                       'preparationIdentity':str(uuid.uuid4())}
        self.target = self.workspaces/self.params['workspace']
        self.baseline = self.data/'workspace-baselines'/manager.digest_key(self.params['workspace'])
        self.receipt = self.data/'workspace-preparations'/(self.params['preparationIdentity']+'--'+manager.digest_key(self.params['workspace'])+'.json')

    def tearDown(self):
        self.stack.close()
        for directory,_,_ in os.walk(self.root,followlinks=False):
            pathlib.Path(directory).chmod(0o700)
        self.temp.cleanup()

    def marker_digest(self):
        return hashlib.sha256((self.source/'.agentos-snapshot.json').read_bytes()).hexdigest()

    def replace_json(self,path,value,mode=0o444):
        path.unlink()
        path.write_text(json.dumps(value))
        path.chmod(mode)

    def rejected(self,code,params=None):
        with self.assertRaisesRegex(ValueError,'^'+code+'$'):
            manager.prepare(params or self.params)

    def no_destination(self):
        self.assertFalse(self.target.exists())
        self.assertFalse(self.baseline.exists())
        self.assertFalse(self.receipt.exists())
        self.assertEqual(list(self.target.parent.glob('.prepare-*')),[])
        self.assertEqual(list(self.baseline.parent.glob('.prepare-*')),[])

    def test_verified_copy_seals_receipt_and_replays_exactly(self):
        before = manager.sources(self.source)
        generation = self.marker_digest()
        result = manager.prepare(self.params)
        receipt = result['preparationReceipt']
        self.assertEqual(manager.sources(self.source),before)
        self.assertEqual(self.marker_digest(),generation)
        self.assertEqual(manager.sources(self.target),before)
        self.assertEqual(manager.sources(self.baseline),before)
        self.assertEqual(receipt['targetWorkspace'],self.params['workspace'])
        self.assertEqual(receipt['sourceWorkspace'],self.params['sourceWorkspace'])
        self.assertEqual(receipt['sourceSnapshotDigest'],generation)
        for key in ['observedSourceDigest','cloneBaseDigest','baselineDigest']:
            self.assertEqual(receipt[key],self.params['expectedSourceDigest'])
        self.assertEqual(receipt['bindingDigest'],manager.preparation_digest(manager.preparation_binding(self.params)))
        self.assertEqual(receipt['receiptDigest'],manager.preparation_digest({k:v for k,v in receipt.items() if k!='receiptDigest'}))
        self.assertEqual(json.loads(self.receipt.read_text()),receipt)
        for path in [self.receipt,self.target/'.agentos-snapshot.json',self.baseline/'src/main.py']:
            self.assertEqual(stat.S_IMODE(path.stat().st_mode),0o444)
        self.assertEqual(manager.prepare(self.params),result)

    def test_binding_fields_are_all_or_nothing_and_canonical(self):
        for key in ['expectedSourceDigest','expectedSourceSnapshotDigest','preparationIdentity']:
            params = dict(self.params);del params[key]
            self.rejected('PREPARATION_BINDING_INVALID',params)
        for key,value in [('expectedSourceDigest','A'*64),('expectedSourceSnapshotDigest','bad'),
                          ('preparationIdentity',self.params['preparationIdentity'].replace('-','')),
                          ('preparationIdentity',None),('expectedSourceDigest',True)]:
            self.rejected('PREPARATION_BINDING_INVALID',{**self.params,key:value})
        self.no_destination()

    def test_stale_content_refuses_copy(self):
        self.rejected('PREPARATION_SOURCE_DIGEST_CHANGED',{**self.params,'expectedSourceDigest':'0'*64})
        self.no_destination()

    def test_stale_generation_refuses_copy(self):
        self.rejected('PREPARATION_SOURCE_GENERATION_CHANGED',{**self.params,'expectedSourceSnapshotDigest':'0'*64})
        self.no_destination()

    def test_missing_generation_is_not_invented(self):
        (self.source/'.agentos-snapshot.json').unlink()
        self.rejected('PREPARATION_RECEIPT_MISSING')
        self.no_destination()

    def test_source_marker_wrong_workspace_or_unprotected_refuses(self):
        marker = self.source/'.agentos-snapshot.json'
        value = json.loads(marker.read_text());value['workspace']='sample/other'
        self.replace_json(marker,value)
        self.rejected('PREPARATION_SOURCE_GENERATION_CHANGED',{**self.params,'expectedSourceSnapshotDigest':self.marker_digest()})
        marker.chmod(0o666)
        self.rejected('PREPARATION_RECEIPT_UNPROTECTED')
        self.no_destination()

    def test_bound_clone_never_normalizes_source_gitignore(self):
        (self.source/'.gitignore').write_text('assets/\n')
        params={**self.params,'expectedSourceDigest':manager.source_hash(manager.sources(self.source))}
        manager.prepare(params)
        self.assertEqual((self.source/'.gitignore').read_text(),'assets/\n')
        self.assertEqual(manager.sources(self.source),manager.sources(self.target))
        self.assertIn('node_modules',(self.target/'.git/info/exclude').read_text().splitlines())

    def test_source_mutation_during_copy_is_rejected(self):
        original = manager.shutil.copytree
        def changed(source,target,*args,**kwargs):
            result=original(source,target,*args,**kwargs)
            if pathlib.Path(source)==self.source:(self.source/'src/main.py').write_text('changed during copy\n')
            return result
        with patch.object(manager.shutil,'copytree',side_effect=changed):
            self.rejected('PREPARATION_SOURCE_DIGEST_CHANGED')
        self.no_destination()

    def test_clone_mutation_during_copy_is_rejected(self):
        original = manager.shutil.copytree
        def changed(source,target,*args,**kwargs):
            result=original(source,target,*args,**kwargs)
            if pathlib.Path(source)==self.source:(pathlib.Path(target)/'src/main.py').write_text('changed clone\n')
            return result
        with patch.object(manager.shutil,'copytree',side_effect=changed):
            self.rejected('PREPARATION_CLONE_CHANGED')
        self.no_destination()

    def test_generation_replacement_during_copy_is_rejected(self):
        original = manager.shutil.copytree
        def changed(source,target,*args,**kwargs):
            result=original(source,target,*args,**kwargs)
            if pathlib.Path(source)==self.source:
                marker=self.source/'.agentos-snapshot.json'
                state=json.loads(marker.read_text());state['createdAt']='replacement generation'
                self.replace_json(marker,state)
            return result
        with patch.object(manager.shutil,'copytree',side_effect=changed):
            self.rejected('PREPARATION_SOURCE_GENERATION_CHANGED')
        self.no_destination()

    def test_source_change_after_git_is_rejected_before_publication(self):
        original = manager.subprocess.run
        def changed(args,**kwargs):
            result=original(args,**kwargs)
            if 'commit' in args:(self.source/'src/main.py').write_text('changed after git\n')
            return result
        with patch.object(manager.subprocess,'run',side_effect=changed):
            self.rejected('PREPARATION_SOURCE_DIGEST_CHANGED')
        self.no_destination()

    def test_source_change_during_final_flush_is_rejected(self):
        original = manager.flush_preparation_tree
        def changed(root,names):
            result=original(root,names)
            if root.parent==self.baseline.parent:(self.source/'src/main.py').write_text('changed during flush\n')
            return result
        with patch.object(manager,'flush_preparation_tree',side_effect=changed):
            self.rejected('PREPARATION_SOURCE_DIGEST_CHANGED')
        self.no_destination()

    def test_identity_cannot_be_rebound_to_another_destination(self):
        manager.prepare(self.params)
        original=self.receipt.read_bytes()
        self.rejected('PREPARATION_BINDING_MISMATCH',{**self.params,'workspace':'sample/oother'})
        self.assertFalse((self.workspaces/'sample/oother').exists())
        self.assertEqual(self.receipt.read_bytes(),original)

    def test_destination_cannot_be_rebound_to_another_identity(self):
        manager.prepare(self.params)
        original=(self.target/'.agentos-snapshot.json').read_bytes()
        self.rejected('PREPARATION_BINDING_MISMATCH',{**self.params,'preparationIdentity':str(uuid.uuid4())})
        self.assertEqual((self.target/'.agentos-snapshot.json').read_bytes(),original)

    def test_modified_destination_refuses_replay_without_resetting_it(self):
        manager.prepare(self.params)
        (self.target/'src/main.py').write_text('work after preparation\n')
        self.rejected('PREPARATION_TARGET_CHANGED')
        self.assertEqual((self.target/'src/main.py').read_text(),'work after preparation\n')

    def test_modified_baseline_refuses_replay(self):
        manager.prepare(self.params)
        changed=self.baseline/'src/main.py';changed.chmod(0o644);changed.write_text('changed\n')
        self.rejected('PREPARATION_BASELINE_CHANGED')

    def test_missing_and_corrupted_receipt_refuse_replay(self):
        manager.prepare(self.params)
        receipt=json.loads(self.receipt.read_text())
        self.receipt.unlink()
        self.rejected('PREPARATION_RECEIPT_MISSING')
        receipt['cloneBaseDigest']='0'*64
        manager.write_preparation_json(self.receipt,receipt)
        self.rejected('PREPARATION_BINDING_MISMATCH')

    def test_receipt_and_marker_must_be_sealed_and_single_link(self):
        manager.prepare(self.params)
        self.receipt.chmod(0o644)
        self.rejected('PREPARATION_RECEIPT_UNPROTECTED')
        self.receipt.chmod(0o444)
        os.link(self.receipt,self.root/'linked-receipt')
        self.rejected('PREPARATION_RECEIPT_UNPROTECTED')
        (self.root/'linked-receipt').unlink()
        marker=self.target/'.agentos-snapshot.json';marker.chmod(0o644)
        self.rejected('PREPARATION_RECEIPT_UNPROTECTED')

    def test_receipt_symlink_and_dangling_destination_are_rejected(self):
        self.target.symlink_to(self.root/'missing',target_is_directory=True)
        self.rejected('INVALID_TARGET')
        self.target.unlink()
        self.receipt.parent.mkdir(mode=0o700)
        self.receipt.symlink_to(self.root/'missing-receipt')
        self.rejected('PREPARATION_PARTIAL_STATE')
        self.assertFalse(self.target.exists())

    def test_protection_includes_data_ancestors_and_anchor(self):
        for directory in [self.data,self.root]:
            old=stat.S_IMODE(directory.stat().st_mode);directory.chmod(0o777)
            try:self.rejected('PREPARATION_DIRECTORY_UNPROTECTED')
            finally:directory.chmod(old)
        actual=self.root/'actual-controller';self.data.rename(actual);self.data.symlink_to(actual,target_is_directory=True)
        self.rejected('PREPARATION_DIRECTORY_UNPROTECTED')
        self.no_destination()

    def test_protected_baseline_subdirectory_is_checked_on_replay(self):
        manager.prepare(self.params)
        (self.baseline/'src').chmod(0o777)
        self.rejected('PREPARATION_DIRECTORY_UNPROTECTED')

    def test_lock_directory_must_be_protected(self):
        (self.data/'locks').chmod(0o777)
        self.rejected('PREPARATION_DIRECTORY_UNPROTECTED')
        self.no_destination()

    def test_lock_symlinks_hardlinks_and_shared_write_are_rejected(self):
        lock=self.data/'worker.lock';lock.unlink()
        other=self.root/'other-lock';other.touch()
        lock.symlink_to(other)
        self.rejected('PREPARATION_LOCK_UNPROTECTED')
        lock.unlink();os.link(other,lock)
        self.rejected('PREPARATION_LOCK_UNPROTECTED')
        lock.unlink();lock.touch();lock.chmod(0o666)
        self.rejected('PREPARATION_LOCK_UNPROTECTED')
        self.no_destination()

    def test_locks_cover_source_destination_and_preparation_identity(self):
        for name in [self.params['sourceWorkspace'],self.params['workspace'],'preparation:'+self.params['preparationIdentity']]:
            lock=self.data/'locks'/('workspace-'+manager.digest_key(name))
            with lock.open('a') as handle:
                fcntl.flock(handle,fcntl.LOCK_EX|fcntl.LOCK_NB)
                self.rejected('SOURCE_BUSY')
        self.no_destination()
        source_lock=self.data/'locks'/('workspace-'+manager.digest_key(self.params['sourceWorkspace']))
        with source_lock.open('a') as handle:
            fcntl.flock(handle,fcntl.LOCK_SH|fcntl.LOCK_NB)
            self.assertEqual(manager.prepare(self.params)['baseDigest'],self.params['expectedSourceDigest'])

    def test_restrictive_umask_preserves_sealed_readable_receipts_and_baseline(self):
        previous=os.umask(0o077)
        try:manager.prepare(self.params)
        finally:os.umask(previous)
        for path in [self.receipt,self.target/'.agentos-snapshot.json',self.baseline/'src/main.py',self.baseline/'src/nested/second.py']:
            self.assertEqual(stat.S_IMODE(path.stat().st_mode),0o444)
        self.assertEqual(manager.prepare(self.params)['baseDigest'],self.params['expectedSourceDigest'])

    def test_permissive_umask_does_not_make_new_baseline_directories_writable_by_others(self):
        previous=os.umask(0o002)
        try:manager.prepare(self.params)
        finally:os.umask(previous)
        for path in [self.receipt.parent,self.baseline,self.baseline/'src',self.baseline/'src/nested']:
            self.assertEqual(stat.S_IMODE(path.stat().st_mode),0o700)

    def test_orphan_baseline_refuses_copy_and_is_preserved(self):
        self.baseline.mkdir()
        (self.baseline/'proof').write_text('preserve me\n')
        self.rejected('PREPARATION_PARTIAL_STATE')
        self.assertEqual((self.baseline/'proof').read_text(),'preserve me\n')
        self.assertFalse(self.target.exists())

    def test_failed_destination_rename_keeps_identity_and_requires_reconciliation(self):
        original=pathlib.Path.rename
        def changed(path,target):
            if pathlib.Path(target)==self.target:raise OSError('injected destination failure')
            return original(path,target)
        with patch.object(pathlib.Path,'rename',autospec=True,side_effect=changed):
            self.rejected('PREPARATION_EFFECT_UNCONFIRMED')
        self.assertTrue(self.baseline.is_dir())
        self.assertTrue(self.receipt.is_file())
        self.assertFalse(self.target.exists())
        self.rejected('PREPARATION_PARTIAL_STATE')
        self.rejected('PREPARATION_BINDING_MISMATCH',{**self.params,'workspace':'sample/oother'})
        self.assertFalse((self.workspaces/'sample/oother').exists())

    def test_receipt_write_failure_never_reports_success_or_replaces_partial_bytes(self):
        original=manager.write_preparation_json
        def changed(path,value):
            if path==self.receipt:
                path.write_text('{');path.chmod(0o444)
                raise OSError('injected receipt failure')
            return original(path,value)
        with patch.object(manager,'write_preparation_json',side_effect=changed):
            self.rejected('PREPARATION_EFFECT_UNCONFIRMED')
        self.assertEqual(self.receipt.read_text(),'{')
        self.assertFalse(self.target.exists())
        self.assertFalse(self.baseline.exists())
        self.rejected('PREPARATION_RECEIPT_INVALID')
        self.assertEqual(self.receipt.read_text(),'{')

    def test_legacy_callers_keep_normalization_and_existing_destination_behavior(self):
        raw=self.workspaces/'sample/raw'
        (raw/'.gitignore').write_text('dist/\n')
        params={'workspace':'sample/olegacy','sourceWorkspace':'sample/raw'}
        result=manager.prepare(params)
        self.assertNotIn('preparationReceipt',result)
        self.assertNotIn('sourceSnapshotDigest',result)
        target=self.workspaces/params['workspace']
        self.assertIn('node_modules/',(target/'.gitignore').read_text().splitlines())
        (target/'src/main.py').write_text('legacy work preserved\n')
        self.assertEqual(manager.prepare(params),result)
        self.assertEqual((target/'src/main.py').read_text(),'legacy work preserved\n')

    def test_explicit_unmarked_root_preserves_bytes_without_normalizing_gitignore(self):
        raw=self.workspaces/'sample/raw';(raw/'.gitignore').write_text('dist/\n')
        before=manager.sources(raw)
        observed=manager.inspect_planning_source({'workspace':'sample/raw','sourceGenerationKind':'root'})
        params={**self.params,'sourceWorkspace':'sample/raw','sourceGenerationKind':'unmarked',
                'expectedSourceDigest':observed['sourceDigest'],'expectedSourceSnapshotDigest':observed['generationDigest']}
        result=manager.prepare(params)
        self.assertEqual(manager.sources(raw),before)
        self.assertFalse((raw/'.agentos-snapshot.json').exists())
        self.assertEqual(manager.sources(self.target),before)
        self.assertEqual(result['preparationReceipt']['sourceGenerationKind'],'unmarked')
        self.assertEqual(manager.prepare(params),result)
        self.assertEqual(manager.inspect_preparation(params)['preparationReceipt'],result['preparationReceipt'])
        with self.assertRaisesRegex(ValueError,'PREPARATION_BINDING_MISMATCH'):
            manager.inspect_preparation({k:v for k,v in params.items() if k!='sourceGenerationKind'})
        child={'workspace':'sample/ochild','sourceWorkspace':params['workspace'],'preparationIdentity':str(uuid.uuid4()),
               'expectedSourceDigest':observed['sourceDigest'],
               'expectedSourceSnapshotDigest':hashlib.sha256((self.target/'.agentos-snapshot.json').read_bytes()).hexdigest()}
        manager.prepare(child)
        self.assertEqual(manager.sources(self.workspaces/'sample/ochild'),before)

    def test_unmarked_inspection_is_bounded_and_cannot_observe_a_live_root(self):
        raw=self.workspaces/'sample/raw'
        with self.assertRaisesRegex(ValueError,'SOURCE_INVENTORY_BOUND'):manager.sources(raw,max_files=1)
        with self.assertRaisesRegex(ValueError,'SOURCE_INVENTORY_BOUND'):manager.sources(raw,max_bytes=1)
        file=self.data/'locks'/('workspace-'+manager.digest_key('sample/raw'))
        with file.open('a') as handle:
            fcntl.flock(handle,fcntl.LOCK_EX|fcntl.LOCK_NB)
            with self.assertRaisesRegex(ValueError,'SOURCE_BUSY'):
                manager.inspect_planning_source({'workspace':'sample/raw','sourceGenerationKind':'root'})

    def test_explicit_unmarked_never_downgrades_existing_or_unsafe_marker(self):
        with self.assertRaisesRegex(ValueError,'PREPARATION_SOURCE_GENERATION_CHANGED'):
            manager.prepare({**self.params,'sourceGenerationKind':'unmarked'})
        marker=self.source/'.agentos-snapshot.json';marker.chmod(0o666)
        with self.assertRaisesRegex(ValueError,'PREPARATION_RECEIPT_UNPROTECTED'):
            manager.inspect_planning_source({'workspace':'sample/osource','sourceGenerationKind':'root'})
        self.no_destination()

    def test_explicit_root_generation_changes_when_same_bytes_get_another_directory(self):
        raw=self.workspaces/'sample/raw'
        observed=manager.inspect_planning_source({'workspace':'sample/raw','sourceGenerationKind':'root'})
        prior=raw.with_name('prior-root');raw.rename(prior)
        import shutil
        shutil.copytree(prior,raw)
        params={**self.params,'sourceWorkspace':'sample/raw','sourceGenerationKind':'unmarked',
                'expectedSourceDigest':observed['sourceDigest'],'expectedSourceSnapshotDigest':observed['generationDigest']}
        with self.assertRaisesRegex(ValueError,'PREPARATION_SOURCE_GENERATION_CHANGED'):manager.prepare(params)
        self.no_destination()


if __name__=='__main__':unittest.main()
