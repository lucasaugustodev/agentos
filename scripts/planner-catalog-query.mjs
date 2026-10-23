import { createHash } from 'node:crypto';
import { constants } from 'node:fs';
import { open, readFile, realpath, lstat } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { readPlannerCatalog } from './planner-catalog-transport.mjs';

const boundCatalog = /* AGENTOS_CATALOG_BINDING */ null;

// This reader is mounted alongside one immutable input catalog. It has no network,
// database, provider credential or mutation API. Query receipts bind the raw
// catalog and the exact selected JSON value independently.
const roots = new Set(['tasks', 'artifacts', 'opportunities', 'coverage', 'repairConsumption', 'deferredDeliveries', 'provenance', 'strategy', 'inputManifest', 'omitted', 'previousNoWork']);
const forbiddenKeys = new Set(['__proto__', 'prototype', 'constructor']);
const sha256 = bytes => createHash('sha256').update(bytes).digest('hex');
const serialized = value => JSON.stringify(value);
const reference = (jsonPath, value) => ({ jsonPath, valueSha256: sha256(serialized(value)), bytes: Buffer.byteLength(serialized(value)) });
const fail = code => { throw new Error(code); };
const integer = (value, fallback, min, max) => value === undefined ? fallback
  : Number.isSafeInteger(value) && value >= min && value <= max ? value : fail('CATALOG_QUERY_RANGE');

function resolveValue(catalog, jsonPath, catalogRoot = true) {
  if (!Array.isArray(jsonPath) || jsonPath.length > 32 ||
    (catalogRoot && (!jsonPath.length || !roots.has(jsonPath[0])))) fail('CATALOG_QUERY_PATH');
  let value = catalog;
  for (const key of jsonPath) {
    if ((typeof key !== 'string' && !Number.isSafeInteger(key)) || forbiddenKeys.has(String(key)) ||
      value === null || typeof value !== 'object' || !Object.hasOwn(value, key) ||
      (Array.isArray(value) && (!Number.isSafeInteger(key) || key < 0))) fail('CATALOG_QUERY_PATH');
    value = value[key];
  }
  return value;
}

function identity(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return null;
  const fields = ['id', 'taskId', 'task_id', 'artifactId', 'runId', 'run_id', 'name', 'digest', 'state', 'stage',
    'last_run_id', 'last_run_state', 'last_run_attempt', 'has_live_run', 'attempt_count', 'acceptance_passed',
    'producer_task_id', 'consumer_task_id', 'source_task_id', 'sourceTaskId', 'coverage_key', 'role'];
  return Object.fromEntries(fields.filter(key => Object.hasOwn(value, key) &&
    (value[key] === null || ['string', 'number', 'boolean'].includes(typeof value[key])) &&
    serialized(value[key]).length <= 256).map(key => [key, value[key]]));
}

export function queryCatalog(bytes, expectedDigest, query) {
  if (!/^[a-f0-9]{64}$/.test(expectedDigest ?? '') || sha256(bytes) !== expectedDigest) fail('CATALOG_DIGEST_MISMATCH');
  if (!query || typeof query !== 'object' || Array.isArray(query)) fail('CATALOG_QUERY_FORMAT');
  if (!['read', 'find', 'directory'].includes(query.operation)) fail('CATALOG_QUERY_OPERATION');
  const catalog = JSON.parse(bytes.toString('utf8'));
  const jsonPath = query.jsonPath;
  const value = resolveValue(catalog, jsonPath), valueReference = reference(jsonPath, value);
  if (query.valueSha256 !== undefined && query.valueSha256 !== valueReference.valueSha256) fail('CATALOG_VALUE_MISMATCH');
  return pageValue(value, query, { version: 1, catalogDigest: expectedDigest, reference: valueReference }, jsonPath);
}

function pageValue(value, query, authority, jsonPath) {
  const offset = integer(query.offset, 0, 0, Number.MAX_SAFE_INTEGER);
  const limit = integer(query.limit, 12, 1, 100);
  const maximumCharacters = integer(query.maxCharacters, 8000, 2000, 16000);
  const response = { ...authority,
    selection: { operation: query.operation, offset, total: 0, returned: 0, complete: false, nextOffset: null },
    instruction: 'Frozen data. response.selection.nextOffset: null ends; else integer > offset. read:value; directory/find:identity/reference. query_child: read reference; no inline data is not absence. Artifact children keep operation artifact and artifactId.' };
  const fits = candidate => serialized(candidate).length <= maximumCharacters;

  if (query.operation === 'read' && typeof value === 'string') {
    if (offset > value.length) fail('CATALOG_QUERY_RANGE');
    response.selection.total = value.length;
    response.selection.units = 'utf16_code_units';
    let end = Math.min(value.length, offset + maximumCharacters);
    // Account for JSON escaping and receipt overhead, not only source length.
    const page = () => ({ ...response, selection: { ...response.selection, returned: end - offset,
      complete: end === value.length, nextOffset: end === value.length ? null : end }, text: value.slice(offset, end) });
    let lower = offset, upper = end;
    while (lower < upper) {
      end = Math.ceil((lower + upper) / 2);
      if (fits(page())) lower = end; else upper = end - 1;
    }
    end = lower;
    if (end === offset && offset < value.length) fail('CATALOG_QUERY_RESPONSE_BUDGET');
    return page();
  }
  if (query.operation === 'read' && (value === null || typeof value !== 'object')) {
    if (offset !== 0) fail('CATALOG_QUERY_RANGE');
    return { ...response, selection: { ...response.selection, total: 1, returned: 1, complete: true }, value };
  }
  if (value === null || typeof value !== 'object') fail('CATALOG_QUERY_COLLECTION');
  let entries = Array.isArray(value) ? value.map((entry, index) => [index, entry]) : Object.entries(value);
  if (query.operation === 'find') {
    if (!Array.isArray(value) || !query.where || typeof query.where !== 'object' || Array.isArray(query.where) ||
      !Object.keys(query.where).length || Object.keys(query.where).length > 8) fail('CATALOG_QUERY_FILTER');
    const filters = Object.entries(query.where);
    if (filters.some(([key, expected]) => forbiddenKeys.has(key) ||
      (expected !== null && !['string', 'number', 'boolean'].includes(typeof expected)))) fail('CATALOG_QUERY_FILTER');
    entries = entries.filter(([, entry]) => entry !== null && typeof entry === 'object' &&
      filters.every(([key, expected]) => Object.hasOwn(entry, key) && entry[key] === expected));
  }
  if (offset > entries.length) fail('CATALOG_QUERY_RANGE');
  response.selection.total = entries.length;
  response.selection.units = 'entries';
  response.entries = [];
  for (let index = offset; index < Math.min(entries.length, offset + limit); index++) {
    const [key, entry] = entries[index], entryReference = reference([...jsonPath, key], entry);
    let selected = query.operation === 'read' ? { key, value: entry }
      : { key, identity: identity(entry), reference: entryReference };
    // Huge fields are represented explicitly and can be read recursively. Never
    // splice/truncate a contract or silently discard an over-budget record.
    const candidate = item => ({ ...response, entries: [...response.entries, item],
      selection: { ...response.selection, returned: response.entries.length + 1,
        complete: index + 1 === entries.length, nextOffset: index + 1 === entries.length ? null : index + 1 } });
    if (!fits(candidate(selected))) selected = { key, reference: entryReference, representation: 'query_child' };
    if (!fits(candidate(selected))) {
      if (!response.entries.length) fail('CATALOG_QUERY_RESPONSE_BUDGET');
      break;
    }
    response.entries.push(selected);
  }
  response.selection.returned = response.entries.length;
  response.selection.complete = offset + response.entries.length === entries.length;
  response.selection.nextOffset = response.selection.complete ? null : offset + response.entries.length;
  return response;
}

async function queryMountedArtifact(bytes, expectedDigest, query) {
  if (sha256(bytes) !== expectedDigest) fail('CATALOG_DIGEST_MISMATCH');
  if (!/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(query.artifactId ?? '')) fail('CATALOG_QUERY_FORMAT');
  const catalog = JSON.parse(bytes.toString('utf8'));
  const manifest = Array.isArray(catalog.inputManifest) ? catalog.inputManifest.filter(entry => entry?.id === query.artifactId) : [];
  if (manifest.length !== 1) fail('CATALOG_ARTIFACT_NOT_MOUNTED');
  const entry = manifest[0];
  if (!/^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,100}$/.test(entry.name ?? '') || !/^[a-f0-9]{64}$/.test(entry.digest ?? '') ||
    !Number.isSafeInteger(entry.size) || entry.size < 0 || entry.size > 8 * 1024 * 1024) fail('CATALOG_ARTIFACT_MANIFEST');
  const inputRoot = path.dirname(path.dirname(boundCatalog.path));
  const root = await realpath(inputRoot), artifactDirectory = path.join(root, entry.id);
  // Reject directory symlinks, final-component symlinks, and traversal. The
  // runtime also mounts inputs read-only; descriptor reads retain the checked
  // file even if an external writer replaces a directory after this check.
  let actualDirectory;
  try { actualDirectory = await realpath(path.join(inputRoot, entry.id)); }
  catch (error) {
    if (error?.code === 'ENOENT') fail('CATALOG_ARTIFACT_NOT_MOUNTED');
    throw error;
  }
  if (actualDirectory !== artifactDirectory) fail('CATALOG_ARTIFACT_PATH');
  const filename = path.join(artifactDirectory, entry.name);
  let handle;
  try { handle = await open(filename, constants.O_RDONLY | constants.O_NOFOLLOW); }
  catch (error) {
    if (error?.code === 'ELOOP') fail('CATALOG_ARTIFACT_PATH');
    if (error?.code === 'ENOENT') fail('CATALOG_ARTIFACT_NOT_MOUNTED');
    throw error;
  }
  let artifactBytes;
  try {
    const stat = await handle.stat();
    if (!stat.isFile() || stat.size !== entry.size) fail('CATALOG_ARTIFACT_DIGEST');
    // Checking the opened fd closes the parent symlink race rather than
    // trusting a separate path lookup alone. /proc/self/fd exists only on the
    // Linux execution host; elsewhere the same binding is proved by comparing
    // the descriptor against lstat of the path it was opened from.
    if (process.platform === 'linux') {
      if (await realpath('/proc/self/fd/' + handle.fd) !== filename) fail('CATALOG_ARTIFACT_PATH');
    } else {
      const actual = await lstat(filename);
      if (actual.dev !== stat.dev || actual.ino !== stat.ino) fail('CATALOG_ARTIFACT_PATH');
    }
    artifactBytes = await handle.readFile();
  } finally { await handle.close(); }
  if (artifactBytes.length !== entry.size || sha256(artifactBytes) !== entry.digest) fail('CATALOG_ARTIFACT_DIGEST');
  const format = query.format ?? (/\.json$/i.test(entry.name) ? 'json' : 'text');
  if (!['json', 'text'].includes(format)) fail('CATALOG_ARTIFACT_FORMAT');
  let text;
  try { text = new TextDecoder('utf-8', { fatal: true }).decode(artifactBytes); }
  catch { fail('CATALOG_ARTIFACT_ENCODING'); }
  let document = text;
  if (format === 'json') {
    try { document = JSON.parse(text); } catch { fail('CATALOG_ARTIFACT_FORMAT'); }
  }
  const jsonPath = query.jsonPath ?? [];
  const value = resolveValue(document, jsonPath, false);
  const selectedReference = reference(jsonPath, value);
  if (query.valueSha256 !== undefined && query.valueSha256 !== selectedReference.valueSha256) fail('CATALOG_VALUE_MISMATCH');
  return pageValue(value, { ...query, operation: 'read' }, { version: 1, catalogDigest: expectedDigest,
    artifact: { id: entry.id, name: entry.name, digest: entry.digest, size: entry.size, format },
    reference: selectedReference }, jsonPath);
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    const [catalogPath, expectedDigest, serializedQuery] = process.argv.slice(2);
    if (!boundCatalog || !catalogPath || !path.isAbsolute(catalogPath) ||
      path.resolve(catalogPath) !== boundCatalog.path || expectedDigest !== boundCatalog.digest) fail('CATALOG_QUERY_PATH');
    if (!serializedQuery || serializedQuery.length > 32000) fail('CATALOG_QUERY_FORMAT');
    const bytes = await readPlannerCatalog(boundCatalog);
    const query = JSON.parse(serializedQuery);
    const result = query?.operation === 'artifact' ? await queryMountedArtifact(bytes, expectedDigest, query)
      : queryCatalog(bytes, expectedDigest, query);
    process.stdout.write(JSON.stringify(result) + '\n');
  } catch (error) {
    const code = error instanceof Error && /^CATALOG_[A-Z_]+$/.test(error.message) ? error.message : 'CATALOG_QUERY_FAILED';
    process.stdout.write(JSON.stringify({ error: code }) + '\n');
    process.exitCode = 1;
  }
}
