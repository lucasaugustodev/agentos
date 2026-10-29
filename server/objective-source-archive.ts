import { lstat, readFile, realpath } from 'node:fs/promises';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { crc32, deflateRawSync } from 'node:zlib';
import { pool } from './db.js';
import { config } from './config.js';
import { DomainError } from './contracts.js';
import { readArtifact } from './service.js';

/** Read-only export of the source a completed objective was accepted with.
 * Authority is the accepted run's candidate.json (digest-checked artifact);
 * every workspace file must still match the accepted sha256, otherwise the
 * download is refused. No writes, locks or queue access. */
const digestPattern = /^[a-f0-9]{64}$/;
const maxFiles = 5000, maxBytes = 128 * 1024 * 1024;
const sha = (b: Buffer) => createHash('sha256').update(b).digest('hex');
const fail = (code: string, message: string, status = 409): never => {
  throw new DomainError(code, message, status);
};

async function acceptedSource(objectiveId: string) {
  const goal = (await pool.query('SELECT id,project,state FROM goals WHERE id=$1', [objectiveId])).rows[0];
  if (!goal) fail('NOT_FOUND', 'Objetivo não encontrado.', 404);
  if (goal.state !== 'completed')
    fail('OBJECTIVE_SOURCE_NOT_COMPLETED', 'O download fica disponível quando o objetivo for concluído.');
  const row = (await pool.query(
    `SELECT t.id task_id,t.key task_key,t.execution->>'workspace' workspace,r.id run_id,
       a.id artifact_id,ac.created_at accepted_at
     FROM tasks t JOIN runs r ON r.task_id=t.id
     JOIN acceptances ac ON ac.run_id=r.id AND ac.task_id=t.id AND ac.passed
     JOIN artifacts a ON a.run_id=r.id AND a.name='candidate.json'
     WHERE t.goal_id=$1 AND t.state='accepted' AND t.execution->>'workspace' IS NOT NULL
       AND coalesce(t.execution->>'readOnlySource','false')<>'true'
     ORDER BY ac.created_at DESC,ac.id DESC LIMIT 1`,
    [objectiveId],
  )).rows[0];
  if (!row) fail('OBJECTIVE_SOURCE_MISSING', 'Nenhuma entrega aceita com código-fonte registrado.', 404);
  return { goal, ...row };
}

function candidateFiles(bytes: Buffer) {
  let candidate: any;
  try {
    candidate = JSON.parse(bytes.toString('utf8'));
  } catch {
    candidate = null;
  }
  if (!candidate || !digestPattern.test(candidate.candidateDigest ?? '') || !Array.isArray(candidate.files))
    fail('OBJECTIVE_SOURCE_CANDIDATE_INVALID', 'O registro da entrega aceita está inválido.');
  if (!candidate.files.length || candidate.files.length > maxFiles)
    fail('OBJECTIVE_SOURCE_TOO_LARGE', 'A entrega aceita excede o limite de arquivos do download.', 413);
  const seen = new Set<string>();
  for (const f of candidate.files) {
    if (!f || typeof f.path !== 'string' || !f.path || f.path.includes('\\') || f.path.startsWith('/') ||
      f.path.split('/').some((p: string) => !p || p === '.' || p === '..') ||
      !digestPattern.test(f.sha256 ?? '') || seen.has(f.path))
      fail('OBJECTIVE_SOURCE_CANDIDATE_INVALID', 'O registro da entrega aceita está inválido.');
    seen.add(f.path);
  }
  return { candidateDigest: candidate.candidateDigest as string,
    files: candidate.files.map((f: any) => ({ path: f.path as string, sha256: f.sha256 as string })) };
}

export async function objectiveSource(objectiveId: string) {
  const source = await acceptedSource(objectiveId);
  const candidate = candidateFiles((await readArtifact(source.artifact_id)).bytes);
  const root = await realpath(config.workspacesRoot).catch(() =>
    fail('OBJECTIVE_SOURCE_UNAVAILABLE', 'O diretório de workspaces não está disponível.', 503));
  const dir = path.resolve(root, source.workspace);
  if (!/^[A-Za-z0-9._-]+\/[A-Za-z0-9._-]+$/.test(source.workspace) || !dir.startsWith(root + path.sep))
    fail('OBJECTIVE_SOURCE_WORKSPACE_INVALID', 'O workspace da entrega aceita é inválido.');
  const files: { path: string; sha256: string; bytes: Buffer }[] = [];
  let total = 0;
  for (const f of candidate.files) {
    const file = path.join(dir, f.path);
    const info = await lstat(file).catch(() => null);
    if (!info?.isFile())
      fail('OBJECTIVE_SOURCE_CHANGED', `Arquivo da entrega aceita ausente: ${f.path}.`);
    if ((await realpath(file)) !== file)
      fail('OBJECTIVE_SOURCE_CHANGED', `Arquivo da entrega aceita fora do workspace: ${f.path}.`);
    total += info!.size;
    if (total > maxBytes)
      fail('OBJECTIVE_SOURCE_TOO_LARGE', 'A entrega aceita excede o limite de tamanho do download.', 413);
    const bytes = await readFile(file);
    if (sha(bytes) !== f.sha256)
      fail('OBJECTIVE_SOURCE_CHANGED', `O arquivo ${f.path} não confere com a versão aceita.`);
    files.push({ ...f, bytes });
  }
  const project = String(source.goal.project).replace(/[^A-Za-z0-9._-]+/g, '-') || 'sistema';
  return {
    objectiveId,
    project,
    fileName: `${project}-${candidate.candidateDigest.slice(0, 12)}.zip`,
    taskId: source.task_id,
    taskKey: source.task_key,
    runId: source.run_id,
    acceptedAt: new Date(source.accepted_at).toISOString(),
    candidateDigest: candidate.candidateDigest,
    fileCount: files.length,
    totalBytes: total,
    files,
  };
}

export const objectiveSourceSummary = ({ files, ...rest }: Awaited<ReturnType<typeof objectiveSource>>) => rest;

function dosTime(at: Date) {
  const year = Math.max(1980, at.getFullYear());
  return {
    time: (at.getHours() << 11) | (at.getMinutes() << 5) | Math.floor(at.getSeconds() / 2),
    date: ((year - 1980) << 9) | ((at.getMonth() + 1) << 5) | at.getDate(),
  };
}

/** Minimal zip writer (deflate, UTF-8 names); no dependency. */
export function zipFiles(entries: { name: string; bytes: Buffer }[], at: Date) {
  const { time, date } = dosTime(at), locals: Buffer[] = [], central: Buffer[] = [];
  let offset = 0;
  for (const e of entries) {
    const name = Buffer.from(e.name, 'utf8'), data = deflateRawSync(e.bytes), crc = crc32(e.bytes);
    const local = Buffer.alloc(30);
    local.writeUInt32LE(0x04034b50, 0); local.writeUInt16LE(20, 4); local.writeUInt16LE(0x0800, 6);
    local.writeUInt16LE(8, 8); local.writeUInt16LE(time, 10); local.writeUInt16LE(date, 12);
    local.writeUInt32LE(crc, 14); local.writeUInt32LE(data.length, 18); local.writeUInt32LE(e.bytes.length, 22);
    local.writeUInt16LE(name.length, 26);
    const head = Buffer.alloc(46);
    head.writeUInt32LE(0x02014b50, 0); head.writeUInt16LE(0x0314, 4); head.writeUInt16LE(20, 6);
    head.writeUInt16LE(0x0800, 8); head.writeUInt16LE(8, 10); head.writeUInt16LE(time, 12);
    head.writeUInt16LE(date, 14); head.writeUInt32LE(crc, 16); head.writeUInt32LE(data.length, 20);
    head.writeUInt32LE(e.bytes.length, 24); head.writeUInt16LE(name.length, 28);
    head.writeUInt32LE((0o100644 << 16) >>> 0, 38); head.writeUInt32LE(offset, 42);
    locals.push(local, name, data);
    central.push(head, name);
    offset += local.length + name.length + data.length;
  }
  const dir = Buffer.concat(central), end = Buffer.alloc(22);
  end.writeUInt32LE(0x06054b50, 0); end.writeUInt16LE(entries.length, 8); end.writeUInt16LE(entries.length, 10);
  end.writeUInt32LE(dir.length, 12); end.writeUInt32LE(offset, 16);
  return Buffer.concat([...locals, dir, end]);
}

export async function objectiveSourceArchive(objectiveId: string) {
  const source = await objectiveSource(objectiveId), folder = source.fileName.replace(/\.zip$/, '');
  const manifest = {
    ...objectiveSourceSummary(source),
    files: source.files.map((f) => ({ path: f.path, sha256: f.sha256, size: f.bytes.length })),
  };
  const bytes = zipFiles(
    [
      ...source.files.map((f) => ({ name: `${folder}/${f.path}`, bytes: f.bytes })),
      { name: `${folder}/AGENTOS-MANIFEST.json`, bytes: Buffer.from(JSON.stringify(manifest, null, 2) + '\n') },
    ],
    new Date(source.acceptedAt),
  );
  return { fileName: source.fileName, bytes, digest: sha(bytes) };
}
