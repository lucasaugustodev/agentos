import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { chmod, mkdtemp, mkdir, readFile, readdir, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';

vi.mock('../server/db.js', () => ({ pool: { query: vi.fn() } }));
vi.mock('../server/schema-readiness.js', () => ({ prepareSchema: vi.fn(async () => undefined) }));
vi.mock('../server/run-criteria-metadata.js', () => ({
  installRunCriteriaMetadata: vi.fn(async () => undefined),
}));
import { workerSourceDigest } from '../server/worker-readiness.js';

const exec = promisify(execFile);
const guardPath = path.resolve('scripts/agentos-release-action.py');
// Import the real guard without running main or producing bytecode in the source
// tree. All publication fixtures and cache mutations stay in a temporary tree.
const bootstrap = `
import contextlib, importlib.util, io, json, pathlib, sys, uuid
spec = importlib.util.spec_from_file_location('release_guard', sys.argv[1])
guard = importlib.util.module_from_spec(spec)
spec.loader.exec_module(guard)
candidate = pathlib.Path(sys.argv[2])
`;
let temporary: string;
let candidate: string;
async function put(relative: string, content: string) {
  const file = path.join(candidate, relative);
  await mkdir(path.dirname(file), { recursive: true });
  await writeFile(file, content);
}
async function python(body: string, ...args: string[]) {
  const result = await exec(
    'python3',
    ['-B', '-c', bootstrap + body, guardPath, candidate, ...args],
    { timeout: 10_000, maxBuffer: 128 * 1024 },
  );
  return JSON.parse(result.stdout);
}
const publisher = `
control = candidate.parent / 'control'
control.mkdir()
before = control / 'before'
before.mkdir()
(control / 'current').symlink_to(before)
guard.ROOT = control
guard.STATE = control / 'receipts'
guard.STATE.mkdir()
occurrence = str(uuid.uuid4())
receipt = guard.STATE / (occurrence + '.json')
receipt.write_text(json.dumps({'base': str(before), 'candidate': str(candidate), 'digest': sys.argv[3]}))
effects = []
# This suite exercises the real publish ordering and Python/Node digest policy.
# Registration/root-owned ancestry are isolated at this explicit boundary;
# candidate-origin and validator-execution-boundary fixtures test those real
# privileged contracts separately. Never create/read a production lock here.
authority_checks = []
origin_error = sys.argv[4] if len(sys.argv) > 4 else None
class FixtureOrigin:
    def require_publish_origin(self, record):
        authority_checks.append('publication_origin')
        assert record['base'] == str(before) and record['candidate'] == str(candidate)
        if origin_error: raise ValueError(origin_error)
def fixture_authority():
    authority_checks.append('registered_bundle')
    return FixtureOrigin()
guard.trusted_origin_module = fixture_authority
def fixture_publication_executor(origin):
    assert isinstance(origin, FixtureOrigin)
    assert (control / 'current').resolve() == before
    authority_checks.append('publication_executor')
    return {'fixture': 'registered publisher'}
guard.publication_executor = fixture_publication_executor
guard.run = lambda args, **kw: effects.append(args) or ''
guard.publish_source = lambda *_args: effects.append('publish_source')
class Healthy:
    status = 200
    def __enter__(self): return self
    def __exit__(self, *_args): pass
guard.urllib.request.urlopen = lambda *_args, **_kw: Healthy()
guard.sys.argv = ['guard', 'publish']
guard.sys.stdin = io.StringIO(json.dumps({'occurrenceId': occurrence}))
code = None
with contextlib.redirect_stdout(io.StringIO()):
    try: guard.main()
    except ValueError as error: code = str(error)
print(json.dumps({'error': code, 'effects': effects,
    'authorityChecks': authority_checks,
    'current': str((control / 'current').resolve()), 'before': str(before),
    'pending': (guard.STATE / 'pending-workers.json').exists()}))
`;

beforeEach(async () => {
  temporary = await mkdtemp(path.join(tmpdir(), 'agentos-release-parity-'));
  candidate = path.join(temporary, 'candidate');
  await mkdir(candidate);
  for (const [file, content] of [
    ['server/runtime.ts', 'export const runtime = 1;\n'],
    ['bridge/workspace_compose.py', 'value = 1\n'],
    ['docs/contract.md', 'Immutable acceptance criteria.\n'],
    ['dist/app.js', 'console.log("reviewed bundle");\n'],
    ['scripts/guard.py', 'print("reviewed executable")\n'],
  ])
    await put(file, content);
});
afterEach(async () => {
  // Real copy helpers seal directories read-only. Restore only owned fixture
  // directories for cleanup, without following the test's symlinks.
  async function writableDirectories(directory: string) {
    await chmod(directory, 0o700);
    for (const item of await readdir(directory, { withFileTypes: true }))
      if (item.isDirectory()) await writableDirectories(path.join(directory, item.name));
  }
  await writableDirectories(temporary);
  await rm(temporary, { recursive: true, force: true });
});

it('keeps Python publication and Node adoption digests equal when generated caches change', async () => {
  const baseline = await workerSourceDigest(candidate);
  expect(await python('print(json.dumps(guard.digest(candidate)))')).toBe(baseline);
  for (const directory of [
    'node_modules',
    '.local-test',
    '.git',
    '__pycache__',
    'bridge/__pycache__',
  ])
    await put(directory + '/mutable.pyc', 'generated bytes');
  await symlink(
    path.join(candidate, 'docs/contract.md'),
    path.join(candidate, 'ignored-file-link'),
  );
  await symlink(path.join(candidate, 'docs'), path.join(candidate, 'ignored-directory-link'));
  expect(await workerSourceDigest(candidate)).toBe(baseline);
  expect(await python('print(json.dumps(guard.digest(candidate)))')).toBe(baseline);
  await put('bridge/__pycache__/mutable.pyc', 'regenerated bytecode with different bytes');
  expect(await workerSourceDigest(candidate)).toBe(baseline);
  expect(await python('print(json.dumps(guard.digest(candidate)))')).toBe(baseline);
  const result = await python(publisher, baseline);
  expect(result).toMatchObject({ error: null, current: candidate, pending: true });
  expect(result.effects).toEqual([['systemctl', 'restart', 'agentos-api'], 'publish_source']);
  expect(result.authorityChecks).toEqual(['registered_bundle', 'publication_origin', 'publication_executor']);
});

it.each([
  'server/runtime.ts',
  'bridge/workspace_compose.py',
  'docs/contract.md',
  'dist/app.js',
  'scripts/guard.py',
])('still rejects an unreviewed change to %s before publication', async (file) => {
  const baseline = await workerSourceDigest(candidate);
  expect(await python('print(json.dumps(guard.digest(candidate)))')).toBe(baseline);
  await put(
    file,
    (await readFile(path.join(candidate, file), 'utf8')) + '\nchanged after validation\n',
  );
  const changed = await workerSourceDigest(candidate);
  expect(changed).not.toBe(baseline);
  expect(await python('print(json.dumps(guard.digest(candidate)))')).toBe(changed);
  const result = await python(publisher, baseline);
  expect(result.error).toBe('CANDIDATE_CHANGED');
  expect(result.current).toBe(result.before);
  expect(result.effects).toEqual([]);
  expect(result.authorityChecks).toEqual(['registered_bundle']);
  expect(result.pending).toBe(false);
});

it('keeps the real publication path behind the origin gate after digest verification', async () => {
  const baseline = await workerSourceDigest(candidate);
  const result = await python(publisher, baseline, 'CANDIDATE_ORIGIN_FIXTURE_DENIED');
  expect(result.error).toBe('CANDIDATE_ORIGIN_FIXTURE_DENIED');
  expect(result.current).toBe(result.before);
  expect(result.effects).toEqual([]);
  expect(result.pending).toBe(false);
  expect(result.authorityChecks).toEqual(['registered_bundle', 'publication_origin']);
});

it('excludes caches from actual validation-copy and release/source-copy policies without dropping source', async () => {
  await put('bridge/__pycache__/workspace_compose.cpython-312.pyc', 'generated before validation');
  await put('__pycache__/guard.pyc', 'generated');
  // Exercise the actual byte-snapshot/copy helpers, not extracted AST fragments.
  // CI itself is outside this policy test; its generated dist is represented by
  // different bytes so the release cannot accidentally retain the input build.
  const result = await python(`
snapshot = guard.workspace_source_snapshot(candidate)
build = candidate.parent / 'build'
guard.write_regular_files(build, snapshot['files'])
guard.require_same_inputs(build, snapshot['files'])
validation_files = sorted(guard.build_snapshot(build))
# The real validator grants only output paths to the unprivileged CI process.
# This temporary fixture owns its build root and models that output creation.
build.chmod(0o755)
(build / 'dist').mkdir()
(build / 'dist/app.js').write_text('console.log("rebuilt fixture output");\\n')
(build / '__pycache__').mkdir()
(build / '__pycache__/after-ci.pyc').write_text('generated after CI')
guard.require_same_inputs(build, snapshot['files'])
copies = {}
source = build
for label in ['release', 'published_source']:
    target = candidate.parent / label
    files = guard.build_snapshot(source, include_outputs=True)
    guard.write_regular_files(target, files)
    assert guard.build_snapshot(target, include_outputs=True) == files
    copies[label] = {'files': sorted(files), 'dist': (target / 'dist/app.js').read_text()}
    source = target
print(json.dumps({'validation': validation_files, **copies}))
`);
  for (const files of [result.validation, result.release.files, result.published_source.files] as string[][]) {
    expect(files.some((file) => file.includes('__pycache__'))).toBe(false);
    expect(files).toContain('bridge/workspace_compose.py');
    expect(files).toContain('server/runtime.ts');
    expect(files).toContain('docs/contract.md');
    expect(files).toContain('scripts/guard.py');
  }
  // Validation intentionally rebuilds dist, whereas the final release must keep it.
  expect(result.validation).not.toContain('dist/app.js');
  expect(result.release.files).toContain('dist/app.js');
  expect(result.published_source.files).toContain('dist/app.js');
  expect(result.release.dist).toBe('console.log("rebuilt fixture output");\n');
  expect(result.published_source.dist).toBe(result.release.dist);
});
