"""Canonical prepare/copy permissions, with real namespace/capability workers.

Run only in the isolated Linux validation sandbox. No production directories,
services, database, account or source tree are writable or needed.
"""
import contextlib
import importlib.util
import json
import os
import pathlib
import stat
import subprocess
import tempfile
import unittest
from unittest.mock import patch

ROOT = pathlib.Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('editable_workspace_manager', ROOT/'bridge/workspace-manager.py')
manager = importlib.util.module_from_spec(spec)
spec.loader.exec_module(manager)

WORKER = '''
import errno,json,pathlib,os
root=pathlib.Path('/workspace');result={}
status=dict(line.split(':',1) for line in pathlib.Path('/proc/self/status').read_text().splitlines() if ':' in line)
result['identity']={key:status.get(key,'').strip() for key in ['Uid','Gid','CapEff','CapBnd','NoNewPrivs']}
for name,operation in [('edit',lambda:(root/'server/main.py').write_text('changed\\n')),
                       ('create',lambda:(root/'server/new.py').write_text('new\\n')),
                       ('rename',lambda:(root/'server/main.py').rename(root/'server/renamed.py')),
                       ('marker',lambda:(root/'.agentos-snapshot.json').write_text('changed'))]:
    try:operation();result[name]='ok'
    except OSError as error:result[name]=error.errno
result['sourceVisible']=pathlib.Path('/source').exists()
result['baselineVisible']=pathlib.Path('/baseline').exists()
print(json.dumps(result))
'''


class EditableWorkspaceModesTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.temp.name)
        self.workspaces = self.root/'workspaces'
        self.data = self.root/'controller'
        (self.workspaces/'sample').mkdir(parents=True)
        self.data.mkdir()
        self.stack = contextlib.ExitStack()
        for name,value in [('ROOT',self.workspaces),('DATA',self.data)]:
            self.stack.enter_context(patch.object(manager,name,value))
        self.stack.enter_context(patch.object(manager.storage_capacity,'reserve',side_effect=lambda *a:contextlib.nullcontext()))
        self.source = self.workspaces/'sample/srcsealed'
        for name,raw,mode in [('.gitignore',b'dist/\n',0o444),('server/main.py',b'original\n',0o444),
                              ('scripts/run.sh',b'#!/bin/sh\nexit 0\n',0o555),('private/config.txt',b'fixture\n',0o400)]:
            path=self.source/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(raw);path.chmod(mode)
        for directory,dirs,_ in os.walk(self.source):pathlib.Path(directory).chmod(0o555)
        self.sealed = self.capture(self.source)

    def tearDown(self):
        self.stack.close()
        # Only temporary fixture directories; do not follow dependency symlinks.
        for directory,dirs,_ in os.walk(self.root,followlinks=False):pathlib.Path(directory).chmod(0o700)
        self.temp.cleanup()

    def capture(self, root):
        return {str(path.relative_to(root)):{'mode':stat.S_IMODE(path.lstat().st_mode),
                'content':path.read_bytes().hex() if path.is_file() else None}
                for path in [root,*sorted(root.rglob('*'))] if not path.is_symlink()}

    def prepare(self, suffix):
        ident='sample/o'+suffix
        result=manager.prepare({'workspace':ident,'sourceWorkspace':'sample/srcsealed'})
        target=manager.workspace(ident)
        self.assertEqual(self.capture(self.source),self.sealed)
        baseline=self.data/'workspace-baselines'/manager.digest_key(ident)
        self.assertEqual(manager.source_hash(manager.sources(baseline)),result['baseDigest'])
        return target,baseline,result

    def test_new_clone_normalizes_owner_write_only_and_preserves_executability(self):
        target,baseline,state=self.prepare('modes')
        self.assertEqual(stat.S_IMODE((target/'server/main.py').stat().st_mode),0o644)
        self.assertEqual(stat.S_IMODE((target/'scripts/run.sh').stat().st_mode),0o755)
        self.assertEqual(stat.S_IMODE((target/'private/config.txt').stat().st_mode),0o600)
        self.assertEqual(stat.S_IMODE((target/'server').stat().st_mode),0o755)
        self.assertEqual(stat.S_IMODE(target.stat().st_mode),0o755)
        marker=target/'.agentos-snapshot.json';marker_before=marker.read_bytes()
        baseline_before=self.capture(baseline)
        # A repeated admission never chmods an already existing workspace.
        (target/'server/main.py').chmod(0o444)
        self.assertEqual(manager.prepare({'workspace':'sample/omodes','sourceWorkspace':'sample/srcsealed'}),state)
        self.assertEqual(stat.S_IMODE((target/'server/main.py').stat().st_mode),0o444)
        self.assertEqual(marker.read_bytes(),marker_before)
        self.assertEqual(self.capture(baseline),baseline_before)

    def test_real_worker_can_edit_clone_but_cannot_write_marker_or_see_baseline(self):
        for drop_caps in [False,True]:
            with self.subTest(drop_caps=drop_caps):
                target,baseline,state=self.prepare('caps'+str(int(drop_caps)))
                baseline_before=self.capture(baseline);marker_before=(target/'.agentos-snapshot.json').read_bytes()
                args=['/usr/bin/bwrap','--unshare-all','--die-with-parent','--new-session',
                      '--ro-bind','/usr','/usr','--symlink','usr/bin','/bin','--symlink','usr/sbin','/sbin',
                      '--symlink','usr/lib','/lib','--symlink','usr/lib64','/lib64','--proc','/proc','--dev','/dev',
                      '--tmpfs','/tmp','--bind',str(target),'/workspace',
                      '--ro-bind',str(target/'.agentos-snapshot.json'),'/workspace/.agentos-snapshot.json',
                      '--chdir','/workspace','--clearenv','--setenv','PATH','/usr/bin:/bin']
                if drop_caps:args+=['--cap-drop','ALL']
                result=subprocess.run(args+['--','/usr/bin/python3','-B','-c',WORKER],check=True,capture_output=True,text=True,timeout=30)
                observed=json.loads(result.stdout)
                self.assertEqual([observed[key] for key in ['edit','create','rename']],['ok']*3,observed)
                self.assertEqual(observed['marker'],30,observed) # EROFS from protected marker bind
                self.assertFalse(observed['sourceVisible']);self.assertFalse(observed['baselineVisible'])
                if drop_caps:self.assertEqual(int(observed['identity']['CapEff'],16),0)
                self.assertEqual((target/'.agentos-snapshot.json').read_bytes(),marker_before)
                self.assertEqual(self.capture(baseline),baseline_before)
                self.assertEqual(self.capture(self.source),self.sealed)


if __name__=='__main__':unittest.main(verbosity=2)
