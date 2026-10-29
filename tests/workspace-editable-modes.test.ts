import { expect, it } from 'vitest';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import path from 'node:path';

it('makes only a newly prepared clone editable while preserving sealed source and executable modes', async () => {
  // Portable CI covers the real prepare transition. The same fixture's second
  // test exercises real nested bwrap workers (default and cap-drop ALL) in the
  // explicitly provisioned namespace validation sandbox, not a production host.
  const { stdout, stderr } = await promisify(execFile)('python3', [
    '-B', path.resolve('tests/fixtures/workspace_editable_modes_test.py'),
    'EditableWorkspaceModesTests.test_new_clone_normalizes_owner_write_only_and_preserves_executability',
  ], { timeout: 30_000, maxBuffer: 1024 * 1024 });
  expect(stdout).toBe('');
  expect(stderr).toContain('Ran 1 test');
  expect(stderr).toContain('OK');
});
