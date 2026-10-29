// Lists the source paths the canonical patch cannot carry, and what to do with
// each. workspace-manager.py inspect() diffs every changed file as UTF-8 text on
// BOTH sides, so a binary that is added, modified OR deleted relative to the
// snapshot base rejects the whole delivery; an untouched binary passes.
// Runs inside the sandbox (/opt/agentos-bridge is mounted read-only there).
import { execFileSync } from 'node:child_process';
import { readFileSync, readdirSync } from 'node:fs';
import path from 'node:path';

const root = process.argv[2] ?? '/workspace';
// Same exclusions as workspace-manager.py sources().
const SKIP = new Set(['.git', 'node_modules', 'evidence', 'data', 'assets', '.agentos-snapshot.json', '.local-test', 'test-results', 'playwright-report']);
const skipped = (rel) =>
  rel.split('/').some((part) => SKIP.has(part) || part.startsWith('.env')) || rel.endsWith('.log');
const text = (bytes) => {
  try {
    new TextDecoder('utf-8', { fatal: true }).decode(bytes);
    return true;
  } catch {
    return false;
  }
};
const git = (...args) => execFileSync('git', ['-C', root, '-c', 'core.quotePath=false', '-c', 'safe.directory=*', ...args], { maxBuffer: 1 << 28 });

// The snapshot base is the workspace's first commit, made when it was prepared.
let base = null;
try {
  base = git('rev-list', '--max-parents=0', 'HEAD').toString().trim().split('\n')[0] || null;
} catch {}
const baseFiles = new Set(base ? git('ls-tree', '-r', '-z', '--name-only', base).toString().split('\0').filter(Boolean) : []);
const atBase = (rel) => (baseFiles.has(rel) ? git('show', `${base}:${rel}`) : null);

const current = new Map();
(function walk(dir) {
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    const rel = path.relative(root, full).split(path.sep).join('/');
    if (skipped(rel)) continue;
    if (entry.isDirectory()) walk(full);
    else if (entry.isFile()) current.set(rel, readFileSync(full));
  }
})(root);

const problems = [];
for (const [rel, bytes] of current) {
  const before = atBase(rel);
  if (before && Buffer.compare(before, bytes) === 0) continue;
  if (!text(bytes))
    problems.push(before ? `RESTORE  ${rel}  (binary changed; run: git -C ${root} checkout ${base} -- '${rel}')` : `MOVE     ${rel}  (new binary; move it under data/ and point the code there, or delete it)`);
  else if (before && !text(before))
    problems.push(`RESTORE  ${rel}  (was binary in the base; run: git -C ${root} checkout ${base} -- '${rel}')`);
}
for (const rel of baseFiles)
  if (!skipped(rel) && !current.has(rel) && !text(atBase(rel)))
    problems.push(`RESTORE  ${rel}  (binary from the base was deleted; run: git -C ${root} checkout ${base} -- '${rel}')`);

if (problems.length) {
  console.log(problems.sort().join('\n'));
  process.exitCode = 1;
} else console.log('OK: no binary change in the source patch.');
