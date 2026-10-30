import { readFile, realpath, stat } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import path from 'node:path';
import { artifactPath } from './artifact-path.mjs';

const validName = /^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,100}$/;
const fail = (code, reason, index) => Object.assign(new Error(code), { code, reason, index });

export function parseArtifactResult(text) {
  let value;
  const input = text.trim();
  try { value = JSON.parse(input); }
  catch {
    // A completed answer may surround its explicit JSON delivery with prose.
    // Extract only one complete fenced block; never repair JSON or guess among
    // multiple examples. Artifact paths and acceptance remain validated below.
    const fences = [...input.matchAll(/^[\t ]*```([^\r\n]*)$/gm)];
    if (fences.length !== 2 || !/^(?:json)?\s*$/i.test(fences[0][1]) || fences[1][1].trim())
      throw fail('RESULT_FORMAT', 'invalid_json');
    // Another object outside the envelope may be an example or an unfinished
    // final answer. Never certify the fenced draft by guessing which one wins.
    const outside = input.slice(0, fences[0].index) + input.slice(fences[1].index + fences[1][0].length);
    if (outside.includes('{')) throw fail('RESULT_FORMAT', 'ambiguous_envelope');
    const body = input.slice(fences[0].index + fences[0][0].length, fences[1].index).trim();
    try { value = JSON.parse(body); }
    catch { throw fail('RESULT_FORMAT', 'invalid_json'); }
  }
  if (!value || typeof value.summary !== 'string' || !value.summary.trim() ||
      !Array.isArray(value.artifacts) || value.artifacts.length > 30)
    throw fail('RESULT_FORMAT', 'invalid_contract');
  return value;
}

export async function collectArtifacts(value, workspace) {
  const root = await realpath(workspace), result = [];
  let total = 0;
  for (const [index, item] of value.artifacts.entries()) {
    let relative;
    try { relative = artifactPath(item?.path); }
    catch { throw fail('ARTIFACT_PATH', 'unsafe_path', index); }
    // Models sometimes use a display label in `name`. The real filename is
    // unambiguous in that case. Keep valid explicit aliases for task contracts.
    const basename = path.posix.basename(relative);
    const fallback = /^\.[a-zA-Z0-9][a-zA-Z0-9_.-]{0,100}$/.test(basename) ? basename.slice(1) : basename;
    const name = typeof item.name === 'string' && validName.test(item.name)
      ? item.name : fallback;
    if (!validName.test(name)) throw fail('ARTIFACT_NAME', 'invalid_filename', index);
    if (result.some(a => a.name === name)) throw fail('ARTIFACT_NAME_COLLISION', 'duplicate_name', index);
    let file, info;
    try { file = await realpath(path.resolve(root, relative)); info = await stat(file); }
    catch (e) { if (e.code === 'ENOENT') throw fail('ARTIFACT_MISSING', 'file_not_found', index); throw e; }
    if (!file.startsWith(root + path.sep) || !info.isFile())
      throw fail('ARTIFACT_PATH', 'outside_workspace_or_not_file', index);
    total += info.size;
    if (info.size > 8 * 1024 * 1024 || total > 24 * 1024 * 1024)
      throw fail('ARTIFACT_TOO_LARGE', 'size_limit', index);
    const bytes = await readFile(file);
    result.push({ name, mime: item.mime ?? 'application/octet-stream',
      digest: createHash('sha256').update(bytes).digest('hex'), base64: bytes.toString('base64') });
  }
  return result;
}
