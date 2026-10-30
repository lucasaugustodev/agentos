#!/usr/bin/env python3
"""Execution-boundary counterexamples. Run only inside the isolated server jail.

The trusted fixture toolchain uses Python as a benign fake Node interpreter.
It demonstrates path selection, not execution of the production Node bundle.
No systemd service, production database, registration or publication is invoked.
"""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import tempfile
import unittest
import sys
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('execution_guard', ROOT / 'scripts/agentos-release-action.py')
guard = importlib.util.module_from_spec(spec)
spec.loader.exec_module(guard)
UNIT = 'agentos-validation-12345678-1234-4234-8234-123456789abc.service'


class BoundaryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)

    def tearDown(self):
        for root, dirs, files in os.walk(self.root, followlinks=False):
            Path(root).chmod(0o700)
        self.temp.cleanup()

    def source(self):
        result = self.root / 'source'
        result.mkdir()
        (result / 'code.py').write_text('original source\n')
        return result

    def toolchain(self):
        base = self.root / 'fake-release'
        base.mkdir()
        (base / 'node_modules').symlink_to(ROOT / 'tests/fixtures/validator-trusted-toolchain/bundle')
        return base

    def test_regular_snapshot_records_exact_bytes(self):
        source = self.source()
        self.assertEqual(guard.checked_tree(source), {'code.py': b'original source\n'})

    def test_file_link_is_never_read(self):
        source = self.source()
        (source / 'escape').symlink_to('/etc/passwd')
        with self.assertRaisesRegex(ValueError, 'NOT_REGULAR'):
            guard.checked_tree(source)

    def test_directory_link_is_never_descended(self):
        source = self.source()
        (source / 'escape').symlink_to('/usr')
        with self.assertRaisesRegex(ValueError, 'NOT_REGULAR'):
            guard.checked_tree(source)

    def test_root_link_is_never_followed(self):
        source = self.source()
        link = self.root / 'linked-root'
        link.symlink_to(source)
        with self.assertRaises(OSError):
            guard.checked_tree(link)

    def test_hard_link_is_rejected(self):
        source = self.source()
        os.link(source / 'code.py', source / 'alias.py')
        with self.assertRaisesRegex(ValueError, 'NOT_REGULAR'):
            guard.checked_tree(source)

    def test_fifo_is_rejected_without_blocking(self):
        source = self.source()
        os.mkfifo(source / 'fifo')
        with self.assertRaisesRegex(ValueError, 'NOT_REGULAR'):
            guard.checked_tree(source)

    def test_mutation_during_read_is_rejected(self):
        source = self.source()
        original = os.read
        def mutate(fd, count):
            result = original(fd, count)
            (source / 'code.py').write_text('modified source\n')
            return result
        with patch.object(guard.os, 'read', side_effect=mutate):
            with self.assertRaisesRegex(ValueError, 'TREE_CHANGED'):
                guard.checked_tree(source)

    def test_ignored_dependency_link_is_not_copied(self):
        source = self.source()
        (source / 'node_modules').symlink_to('/usr')
        self.assertEqual(guard.build_snapshot(source), {'code.py': b'original source\n'})

    def test_source_digest_matches_real_manager_including_prior_dist(self):
        source = self.source()
        for relative in ['dist/old.js', 'assets/binary.dat', 'nested/assets/a.dat', 'nested/unicode-ç.py',
                         'evidence/report.json', 'ignored.log', '.local/tmp.txt', '.snapshot/tmp.txt']:
            path = source / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(relative)
        import sys
        sys.path.insert(0, str(ROOT / 'bridge'))
        try:
            manager_spec = importlib.util.spec_from_file_location('real_workspace_manager', ROOT / 'bridge/workspace-manager.py')
            manager = importlib.util.module_from_spec(manager_spec)
            manager_spec.loader.exec_module(manager)
            expected = manager.source_hash(manager.sources(source))
        finally:
            sys.path.pop(0)
        snapshot = guard.workspace_source_snapshot(source)
        self.assertEqual(snapshot['candidateDigest'], expected)
        self.assertNotIn('dist/old.js', snapshot['files'])
        self.assertIn('assets/binary.dat', snapshot['files'])
        self.assertNotEqual(snapshot['sourceManifestDigest'], snapshot['inputDigest'])
        before = snapshot['candidateDigest']
        (source / 'dist/old.js').write_text('changed prior generated source inventory')
        self.assertNotEqual(guard.workspace_source_snapshot(source)['candidateDigest'], before)

    def test_build_generated_outputs_do_not_change_input_digest(self):
        source = self.source()
        expected = guard.build_snapshot(source)
        for relative in ['dist/app.js', '.local-test/report.json', 'test-results/a', 'playwright-report/a']:
            path = source / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text('generated')
        self.assertEqual(guard.require_same_inputs(source, expected), guard.manifest_digest(expected))
        self.assertEqual(guard.build_snapshot(source, True)['dist/app.js'], b'generated')

    def test_modified_added_and_deleted_inputs_fail(self):
        source = self.source()
        expected = guard.build_snapshot(source)
        (source / 'code.py').write_text('changed')
        with self.assertRaisesRegex(ValueError, 'SOURCE_CHANGED'):
            guard.require_same_inputs(source, expected)
        (source / 'code.py').write_bytes(expected['code.py'])
        (source / 'extra.py').write_text('extra')
        with self.assertRaisesRegex(ValueError, 'SOURCE_CHANGED'):
            guard.require_same_inputs(source, expected)
        (source / 'extra.py').unlink()
        (source / 'code.py').unlink()
        with self.assertRaisesRegex(ValueError, 'SOURCE_CHANGED'):
            guard.require_same_inputs(source, expected)

    def test_generated_dist_symlink_is_rejected_before_copy(self):
        source = self.source()
        (source / 'dist').mkdir()
        (source / 'dist/secret').symlink_to('/etc/passwd')
        with self.assertRaisesRegex(ValueError, 'NOT_REGULAR'):
            guard.build_snapshot(source, True)

    def test_generated_dist_directory_link_is_rejected(self):
        source = self.source()
        (source / 'dist').symlink_to('/usr')
        with self.assertRaisesRegex(ValueError, 'NOT_REGULAR'):
            guard.build_snapshot(source, True)

    def test_safe_copy_uses_captured_bytes_and_readonly_inputs(self):
        source = self.source()
        captured = guard.checked_tree(source)
        (source / 'code.py').unlink()
        (source / 'code.py').symlink_to('/etc/passwd')
        target = self.root / 'copy'
        guard.write_regular_files(target, captured)
        self.assertEqual(guard.checked_tree(target), captured)
        self.assertEqual(stat.S_IMODE(target.stat().st_mode), 0o555)
        self.assertEqual(stat.S_IMODE((target / 'code.py').stat().st_mode), 0o444)

    def test_restrictive_operator_umask_preserves_readable_immutable_files(self):
        target = self.root / 'umask-copy'
        previous = os.umask(0o077)
        try:
            guard.write_regular_files(target, {'nested/source.py': b'code'})
        finally:
            os.umask(previous)
        self.assertEqual(stat.S_IMODE(target.stat().st_mode), 0o555)
        self.assertEqual(stat.S_IMODE((target / 'nested').stat().st_mode), 0o555)
        self.assertEqual(stat.S_IMODE((target / 'nested/source.py').stat().st_mode), 0o444)

    def test_safe_copy_refuses_existing_destination(self):
        target = self.root / 'copy'
        target.symlink_to('/usr')
        with self.assertRaises(FileExistsError):
            guard.write_regular_files(target, {'file': b'data'})

    def test_safe_copy_refuses_noncanonical_relative_paths(self):
        for index, path in enumerate(['../outside', '/outside', 'a/../outside', 'a/./file', 'a//file']):
            with self.subTest(path=path):
                with self.assertRaisesRegex(ValueError, 'COPY_PATH'):
                    guard.write_regular_files(self.root / str(index), {path: b'data'})

    def test_mutable_runner_is_rejected(self):
        runner = self.root / 'node'
        runner.write_text('fake node')
        with self.assertRaisesRegex(ValueError, 'TOOL_UNPROTECTED'):
            guard.protected_tool(runner, root_owned=False, forbidden_uid=os.getuid())

    def test_trusted_fake_runner_ignores_build_local_runner_replacement(self):
        base = self.toolchain()
        runners = guard.trusted_runners(base, '/usr/bin/python3', os.getuid())
        self.assertTrue(all(path.is_absolute() for path in runners.values()))
        build = self.root / 'mutable-build'
        build.mkdir()
        before = guard.runner_identity(runners)
        result = subprocess.run(['/bin/sh', '-c', guard.runner_command(runners)], cwd=build,
                                capture_output=True, text=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn('TRUSTED_FAKE_VITEST', result.stdout)
        self.assertTrue((build / 'node_modules/vitest/vitest.mjs').exists())
        self.assertEqual(before, guard.runner_identity(runners))

    def test_only_declared_outputs_and_caches_are_writable(self):
        base = self.toolchain()
        build = self.root / 'build'
        guard.write_regular_files(build, {'source.py': b'code'})
        # The real installer is root; this fixture is deliberately unprivileged.
        build.chmod(0o755)
        writable = guard.install_dependencies_and_outputs(build, base, os.getuid(), os.getgid())
        build.chmod(0o555)
        self.assertEqual(set(writable), {str(build / item) for item in guard.OUTPUT_ROOTS | guard.CACHE_ROOTS})
        self.assertEqual(stat.S_IMODE((build / 'node_modules').stat().st_mode), 0o555)
        self.assertEqual(stat.S_IMODE((build / 'source.py').stat().st_mode), 0o444)
        for path in writable:
            self.assertEqual(stat.S_IMODE(Path(path).stat().st_mode), 0o700)
        self.assertTrue((build / 'node_modules/vitest').is_symlink())

    def test_build_root_owned_by_build_user_is_rejected(self):
        root = self.root / 'build-root'
        with patch.object(guard, 'BUILD_ROOT', root):
            with self.assertRaisesRegex(ValueError, 'BUILD_ROOT_UNPROTECTED'):
                guard.ensure_build_root()

    def state(self, **changes):
        state = {'LoadState': 'loaded', 'ActiveState': 'inactive', 'MainPID': '0', 'ControlGroup': ''}
        state.update(changes)
        return subprocess.CompletedProcess([], 0, stdout='\n'.join(k + '=' + v for k, v in state.items()))

    def test_stopped_unit_and_absent_cgroup_are_confirmed(self):
        with patch.object(guard.subprocess, 'run', return_value=self.state()), patch.object(Path, 'exists', return_value=False):
            proof = guard.confirm_validation_stopped(UNIT)
        self.assertFalse(proof['populated'])
        self.assertEqual(proof['mainPID'], 0)

    def test_live_pid_active_unit_and_foreign_cgroup_are_rejected(self):
        for changes in [{'MainPID': '13'}, {'ActiveState': 'active'}, {'ControlGroup': '/other'}, {'LoadState': 'error'}]:
            with self.subTest(changes=changes), patch.object(guard.subprocess, 'run', return_value=self.state(**changes)):
                with self.assertRaisesRegex(ValueError, 'STOP_UNCONFIRMED'):
                    guard.confirm_validation_stopped(UNIT)

    def test_populated_cgroup_is_rejected_even_after_main_exit(self):
        with patch.object(guard.subprocess, 'run', return_value=self.state()), patch.object(Path, 'exists', return_value=True), patch.object(Path, 'read_text', return_value='populated 1\nfrozen 0\n'):
            with self.assertRaisesRegex(ValueError, 'STOP_UNCONFIRMED'):
                guard.confirm_validation_stopped(UNIT)

    def test_inaccessible_named_cgroup_remains_uncertain(self):
        with patch.object(guard.subprocess, 'run', return_value=self.state(ControlGroup='/system.slice/' + UNIT)), patch.object(Path, 'exists', return_value=False):
            with self.assertRaisesRegex(ValueError, 'STOP_UNCONFIRMED'):
                guard.confirm_validation_stopped(UNIT)

    def test_empty_named_cgroup_can_be_confirmed(self):
        with patch.object(guard.subprocess, 'run', return_value=self.state(ControlGroup='/system.slice/' + UNIT)), patch.object(Path, 'exists', return_value=True), patch.object(Path, 'read_text', return_value='populated 0\nfrozen 0\n'):
            self.assertFalse(guard.confirm_validation_stopped(UNIT)['populated'])

    def test_timeout_requests_cleanup_before_returning_failure(self):
        proof = {'mainPID': 0, 'populated': False}
        with patch.object(guard.subprocess, 'run', side_effect=subprocess.TimeoutExpired(['fake'], 1, output=b'partial')), patch.object(guard, 'stop_and_confirm', return_value=proof) as stop:
            result = guard.execute_validation(['fake'], UNIT)
        stop.assert_called_once_with(UNIT)
        self.assertTrue(result['failed'])
        self.assertTrue(result['timedOut'])
        self.assertEqual(result['stopProof'], proof)

    def test_timeout_without_physical_stop_is_uncertain(self):
        with patch.object(guard.subprocess, 'run', side_effect=subprocess.TimeoutExpired(['fake'], 1)), patch.object(guard, 'stop_and_confirm', side_effect=ValueError('VALIDATION_STOP_UNCONFIRMED')):
            with self.assertRaisesRegex(ValueError, 'STOP_UNCONFIRMED'):
                guard.execute_validation(['fake'], UNIT)

    def test_successful_client_cannot_bypass_stop_proof(self):
        with patch.object(guard.subprocess, 'run', return_value=subprocess.CompletedProcess([], 0, 'passed')), patch.object(guard, 'confirm_validation_stopped', side_effect=ValueError('VALIDATION_STOP_UNCONFIRMED')), patch.object(guard, 'stop_and_confirm', side_effect=ValueError('VALIDATION_STOP_UNCONFIRMED')):
            with self.assertRaisesRegex(ValueError, 'STOP_UNCONFIRMED'):
                guard.execute_validation(['fake'], UNIT)

    def test_transport_error_also_requires_cleanup(self):
        with patch.object(guard.subprocess, 'run', side_effect=OSError('broken pipe')), patch.object(guard, 'stop_and_confirm', return_value={}) as stop:
            with self.assertRaisesRegex(ValueError, 'CLIENT_FAILED_AFTER_STOP'):
                guard.execute_validation(['fake'], UNIT)
        stop.assert_called_once_with(UNIT)

    def test_report_symlink_and_hard_link_are_rejected(self):
        build = self.source()
        directory = build / '.local-test'
        directory.mkdir()
        path = directory / 'vitest-results.json'
        path.symlink_to('/etc/passwd')
        with self.assertRaisesRegex(ValueError, 'REPORT_MISSING_OR_LINK'):
            guard.test_evidence(build, {})
        path.unlink()
        other = self.root / 'other.json'
        other.write_text('{}')
        os.link(other, path)
        with self.assertRaisesRegex(ValueError, 'REPORT_MISSING_OR_LINK'):
            guard.test_evidence(build, {})

    def test_required_test_skipped_is_not_passed(self):
        build = self.source()
        directory = build / '.local-test'
        directory.mkdir()
        file = 'tests/database.test.ts'
        report = {'success': True, 'numTotalTests': 2, 'numPassedTests': 1, 'numFailedTests': 0,
                  'testResults': [{'name': str(build / file), 'status': 'passed',
                    'assertionResults': [{'fullName': 'required', 'status': 'pending'},
                                         {'fullName': 'other', 'status': 'passed'}]}]}
        (directory / 'vitest-results.json').write_text(json.dumps(report))
        requirements = {'version': 1, 'profile': 'agentos.validate', 'candidateDigest': 'a' * 64,
                        'requiredDatabaseTests': [{'file': file, 'name': 'required'}]}
        evidence = guard.test_evidence(build, {'validationCoverage': requirements})
        self.assertFalse(evidence['passed'])
        self.assertEqual(evidence['coverage']['requiredTests'][0]['status'], 'skipped')

    def test_loaded_code_matches_current_exact_source(self):
        raw = Path(guard.__file__).read_bytes()
        self.assertEqual(guard.verify_loaded_executor(raw), hashlib.sha256(raw).hexdigest())

    def test_changed_code_between_startup_and_lock_is_rejected(self):
        raw = Path(guard.__file__).read_bytes()
        changed = raw.replace(b'MAX_TREE_BYTES = 128', b'MAX_TREE_BYTES = 129')
        self.assertNotEqual(changed, raw)
        with self.assertRaisesRegex(ValueError, 'EXECUTOR_CHANGED_AT_STARTUP'):
            guard.verify_loaded_executor(changed)

    def test_registration_exclusive_lock_waits_for_guard_terminal(self):
        lock = ROOT / 'tests/fixtures/validator-registration-lock.txt'
        command = [sys.executable, '-B', '-c',
                   'import os,fcntl,sys\nfd=os.open(sys.argv[1],os.O_RDONLY)\n'
                   'try:fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)\n'
                   'except BlockingIOError:sys.exit(2)\n'
                   'os.close(fd)\n', str(lock)]
        with patch.object(guard, 'REGISTRATION_LOCK_PATH', lock), patch.object(guard, '_REGISTRATION_LOCK', None), patch.object(guard, '_EXECUTOR_SHA', None):
            guard.acquire_execution_authority()
            try:
                self.assertEqual(subprocess.run(command, timeout=10).returncode, 2)
                measured = hashlib.sha256(Path(guard.__file__).read_bytes()).hexdigest()
                self.assertEqual(guard.evidence_identity(None), {'executorSha256': measured})
            finally:
                os.close(guard._REGISTRATION_LOCK)
            self.assertEqual(subprocess.run(command, timeout=10).returncode, 0)

    def test_coverage_policy_name_has_same_500_limit(self):
        raw = {'version': 1, 'profile': 'agentos.validate', 'candidateDigest': 'a' * 64,
               'requiredDatabaseTests': [{'file': 'tests/a.test.ts', 'name': 'x' * 501}]}
        with self.assertRaisesRegex(ValueError, 'TEST_NAME'):
            guard.coverage_requirements(raw)

    def test_coverage_policy_accepts_only_registered_test_path_grammar(self):
        for path in ['tests/helper.ts', 'tests/a.test.tsx', 'tests/../a.test.ts', 'tests/.hidden/a.test.ts']:
            raw = {'version': 1, 'profile': 'agentos.validate', 'candidateDigest': 'a' * 64,
                   'requiredDatabaseTests': [{'file': path, 'name': 'required'}]}
            with self.subTest(path=path), self.assertRaisesRegex(ValueError, 'TEST_PATH'):
                guard.coverage_requirements(raw)


if __name__ == '__main__':
    unittest.main()
