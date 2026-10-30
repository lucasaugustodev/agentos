#!/usr/bin/env python3
"""Two-user counterexample for the isolated RootDirectory test harness only.

The root preparation phase has no capabilities or production mounts. A separate
agentos-build unit then proves actual access to root-created source bytes.
"""
import importlib.util
import json
import os
from pathlib import Path
import stat
import sys

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('ownership_guard', ROOT / 'scripts/agentos-release-action.py')
guard = importlib.util.module_from_spec(spec)
spec.loader.exec_module(guard)
BASE = Path('/ownership')
PAYLOAD = b'read-only root-owned source under umask077\n'


def prepare():
    assert os.getuid() == 0
    status = dict(line.split(':', 1) for line in Path('/proc/self/status').read_text().splitlines() if ':' in line)
    assert status['NoNewPrivs'].strip() == '1' and int(status['CapEff'].strip(), 16) == 0
    assert BASE.is_dir() and BASE.stat().st_uid == 0
    os.umask(0o077)
    guard.BUILD_ROOT = BASE / 'builds'
    assert guard.ensure_build_root() == guard.BUILD_ROOT
    assert stat.S_IMODE(guard.BUILD_ROOT.stat().st_mode) == 0o711
    guard.write_regular_files(guard.BUILD_ROOT / 'candidate', {'nested/source.py': PAYLOAD})
    source = guard.BUILD_ROOT / 'candidate/nested/source.py'
    assert source.stat().st_uid == 0 and stat.S_IMODE(source.stat().st_mode) == 0o444
    assert stat.S_IMODE(source.parent.stat().st_mode) == 0o555
    unsafe = BASE / 'unsafe'
    unsafe.mkdir()
    unsafe.chmod(0o777)
    guard.BUILD_ROOT = unsafe / 'refused'
    try:
        guard.ensure_build_root()
    except ValueError as error:
        assert str(error) == 'VALIDATION_BUILD_ROOT_UNPROTECTED'
    else:
        raise AssertionError('Writable ancestor accepted')
    assert not guard.BUILD_ROOT.exists()
    target = BASE / 'link-target'
    target.mkdir(mode=0o700)
    link = BASE / 'link'
    link.symlink_to(target)
    guard.BUILD_ROOT = link
    try:
        guard.ensure_build_root()
    except ValueError as error:
        assert str(error) == 'VALIDATION_BUILD_ROOT_UNPROTECTED'
    else:
        raise AssertionError('Symlink root accepted')
    assert stat.S_IMODE(target.stat().st_mode) == 0o700
    BASE.chmod(0o711)
    print(json.dumps({'preparedByUid': 0, 'umask': '077', 'buildRootMode': '0711',
                      'sourceMode': '0444', 'unsafeAncestorRejected': True,
                      'symlinkRejectedWithoutChmod': True, 'capabilities': 0}))


def verify():
    assert os.getuid() == 989 and os.getgid() == 984
    builds = BASE / 'builds'
    source = builds / 'candidate/nested/source.py'
    assert source.read_bytes() == PAYLOAD
    assert builds.stat().st_uid == 0 and source.stat().st_uid == 0
    for operation in [lambda: source.write_bytes(b'changed'), lambda: (builds / 'untrusted').mkdir()]:
        try:
            operation()
        except PermissionError:
            pass
        else:
            raise AssertionError('Unprivileged mutation succeeded')
    print(json.dumps({'readerUid': os.getuid(), 'ownerUid': source.stat().st_uid,
                      'sourceReadable': True, 'sourceImmutable': True,
                      'rootRejectsNewUnprivilegedEntries': True}))


if __name__ == '__main__':
    assert sys.argv[1:] in (['prepare'], ['verify'])
    prepare() if sys.argv[1] == 'prepare' else verify()
