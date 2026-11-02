import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { isDeepStrictEqual } from 'node:util';
import { config } from '../config.js';
import { pool, tx, lock, type DB } from '../db.js';
import { hash } from '../memory.js';
import { DomainError } from '../contracts.js';
import type { CycleStep } from './contracts.js';

type ArtifactRef = { id: string; name: string; digest: string; size: number };
type ActorRef = { stepId: string; key: string; attemptId: string; attemptNumber: number; runFence: string };
export type DependencySnapshot = {
  version: 1; contractSHA256: string; occurrenceId: string;
  consumer: ActorRef; source: ActorRef; artifacts: ArtifactRef[];
};
export class DependencyBindingError extends DomainError {
  constructor(public feedback: any) {
    super('DEPENDENCY_ARTIFACT_BINDING', 'Report hashes do not match the frozen dependency artifacts. Rerun only this validation against the supplied current artifacts.');
  }
}
const invalid = (code = 'DEPENDENCY_EVIDENCE_INVALID'): never => {
  throw new DomainError(code, 'Dependency evidence is missing, changed or invalid; no automatic acceptance or producer retry.');
};
export const nativeQA2 = (step: CycleStep) => step.parameters.nativeQAContractVersion === 2;

// Contract hashing is independent of jsonb key order.
function contractHash(step: CycleStep) {
  const c = step.dependencyEvidence!;
  return hash(JSON.stringify([c.version, c.sourceStep,
    c.bindings.map(b => [b.reportArtifact, b.jsonPointer, b.dependencyArtifact])]));
}
async function sourceIdentity(db: DB, occurrenceId: string, sourceKey: string) {
  const row = (await db.query(
    `SELECT s.id,s.key,s.state,s.attempt,r.id attempt_id,r.state attempt_state,r.number,r.fence,r.receipt
     FROM cycle_steps s JOIN cycle_attempts r ON r.step_id=s.id AND r.number=s.attempt
     WHERE s.occurrence_id=$1 AND s.key=$2 FOR SHARE OF s,r`, [occurrenceId, sourceKey])).rows[0];
  if (!row || row.state !== 'completed' || row.attempt_state !== 'completed' || row.receipt?.stopped !== true)
    return invalid('DEPENDENCY_SOURCE_CHANGED');
  const artifacts: ArtifactRef[] = (await db.query(
    'SELECT id,name,digest,size FROM cycle_artifacts WHERE attempt_id=$1 ORDER BY name,id FOR SHARE', [row.attempt_id])).rows;
  if (!artifacts.length || artifacts.length > 256 || artifacts.some(a =>
      !/^[a-f0-9]{64}$/.test(a.digest) || !Number.isSafeInteger(a.size) || a.size < 0 || a.size > 24 * 1024 * 1024))
    return invalid();
  return {
    source: { stepId: row.id, key: row.key, attemptId: row.attempt_id, attemptNumber: row.number, runFence: row.fence },
    artifacts,
  };
}
export async function dependencyArtifactBytes(ref: ArtifactRef) {
  const row = (await pool.query('SELECT id,name,digest,size FROM cycle_artifacts WHERE id=$1', [ref.id])).rows[0];
  if (!isDeepStrictEqual(row, ref)) return invalid('DEPENDENCY_ARTIFACT_CHANGED');
  const bytes = await readFile(path.join(config.dataDir, 'cycle-artifacts', ref.digest));
  if (bytes.length !== ref.size || hash(bytes) !== ref.digest) return invalid('DEPENDENCY_ARTIFACT_CHANGED');
  return bytes;
}
export async function freezeDependencyEvidence(o: any, s: any, a: any): Promise<DependencySnapshot> {
  return tx(async db => {
    await lock(db);
    const identity = await sourceIdentity(db, o.id, s.definition.dependencyEvidence.sourceStep);
    for (const b of s.definition.dependencyEvidence.bindings)
      if (!identity.artifacts.some(f => f.name === b.dependencyArtifact)) return invalid();
    const snapshot: DependencySnapshot = {
      version: 1, contractSHA256: contractHash(s.definition), occurrenceId: o.id,
      consumer: { stepId: s.id, key: s.key, attemptId: a.id, attemptNumber: a.number, runFence: a.fence },
      ...identity,
    };
    // A retry cannot quietly bind a different producer than the failed QA.
    if (a.number > 1) {
      const prior = (await db.query('SELECT state,error_code,receipt,context FROM cycle_attempts WHERE step_id=$1 AND number=$2', [s.id, a.number - 1])).rows[0];
      const frozen = prior?.context?.manifest?.dependencyEvidence;
      if (!prior || prior.state !== 'failed' || prior.error_code !== 'DEPENDENCY_ARTIFACT_BINDING' || prior.receipt?.stopped !== true ||
          !frozen || frozen.contractSHA256 !== snapshot.contractSHA256 ||
          !isDeepStrictEqual(frozen.source, snapshot.source) || !isDeepStrictEqual(frozen.artifacts, snapshot.artifacts))
        return invalid('DEPENDENCY_SOURCE_CHANGED');
    }
    return snapshot;
  });
}
export async function assertDependencySnapshot(db: DB, o: any, s: any, a: any, snapshot: DependencySnapshot) {
  if (!snapshot || snapshot.version !== 1 || snapshot.contractSHA256 !== contractHash(s.definition) || snapshot.occurrenceId !== o.id ||
      !isDeepStrictEqual(snapshot.consumer, { stepId: s.id, key: s.key, attemptId: a.id, attemptNumber: a.number, runFence: a.fence }))
    return invalid();
  const own = (await db.query(`SELECT s.state,s.attempt,r.state attempt_state,r.fence
    FROM cycle_steps s JOIN cycle_attempts r ON r.step_id=s.id AND r.id=$2
    WHERE s.id=$1 FOR SHARE OF s,r`, [s.id, a.id])).rows[0];
  if (!own || own.state !== 'running' || own.attempt !== a.number || own.attempt_state !== 'running' || own.fence !== a.fence)
    return invalid('DEPENDENCY_SOURCE_CHANGED');
  const current = await sourceIdentity(db, o.id, s.definition.dependencyEvidence.sourceStep);
  if (!isDeepStrictEqual(current.source, snapshot.source) || !isDeepStrictEqual(current.artifacts, snapshot.artifacts))
    return invalid('DEPENDENCY_SOURCE_CHANGED');
}

// Bounded, unambiguous JSON: JSON.parse alone accepts duplicate keys and 1e999.
function reportJSON(bytes: Buffer): any {
  if (!bytes.length || bytes.length > 1024 * 1024) return invalid('DEPENDENCY_REPORT_INVALID');
  const text = bytes.toString('utf8');
  if (!Buffer.from(text).equals(bytes)) return invalid('DEPENDENCY_REPORT_INVALID');
  let at = 0;
  const fail = (): never => invalid('DEPENDENCY_REPORT_INVALID');
  const ws = () => { while (/[\x20\t\r\n]/.test(text[at] ?? '\0')) at++; };
  const string = () => {
    const start = at++;
    while (at < text.length) {
      if (text[at] === '\\') { at += 2; continue; }
      if (text[at++] === '"') return JSON.parse(text.slice(start, at));
    }
    return fail();
  };
  const parse = (depth: number): any => {
    if (depth > 64) return fail();
    ws();
    if (text[at] === '"') return string();
    if (text[at] === '{') {
      at++; ws(); const result = Object.create(null), seen = new Set<string>();
      if (text[at] === '}') { at++; return result; }
      for (;;) {
        ws(); if (text[at] !== '"') return fail();
        const key = string(); if (seen.has(key)) return fail(); seen.add(key);
        ws(); if (text[at++] !== ':') return fail();
        result[key] = parse(depth + 1); ws();
        if (text[at] === '}') { at++; return result; }
        if (text[at++] !== ',') return fail();
      }
    }
    if (text[at] === '[') {
      at++; ws(); const result: any[] = [];
      if (text[at] === ']') { at++; return result; }
      for (;;) {
        result.push(parse(depth + 1)); ws();
        if (text[at] === ']') { at++; return result; }
        if (text[at++] !== ',') return fail();
      }
    }
    const token = /(?:true|false|null|-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?)/y;
    token.lastIndex = at; const found = token.exec(text);
    if (!found) return fail();
    at = token.lastIndex; const value = JSON.parse(found[0]);
    if (typeof value === 'number' && !Number.isFinite(value)) return fail();
    return value;
  };
  try { const value = parse(0); ws(); if (at !== text.length) return fail(); return value; }
  catch { return fail(); }
}

export async function checkDependencyEvidence(o: any, s: any, a: any) {
  const context = (await pool.query('SELECT context FROM cycle_attempts WHERE id=$1', [a.id])).rows[0]?.context;
  const snapshot: DependencySnapshot = context?.manifest?.dependencyEvidence;
  await tx(async db => { await lock(db); await assertDependencySnapshot(db, o, s, a, snapshot); });
  // Revalidate the full delivered inventory, not just the two referenced hashes.
  for (const ref of snapshot.artifacts) await dependencyArtifactBytes(ref);
  const reports = new Map<string, { ref: ArtifactRef; value: any }>();
  const bindings = [];
  for (const binding of s.definition.dependencyEvidence.bindings) {
    if (!reports.has(binding.reportArtifact)) {
      const ref = (await pool.query('SELECT id,name,digest,size FROM cycle_artifacts WHERE attempt_id=$1 AND name=$2', [a.id, binding.reportArtifact])).rows[0];
      if (!ref || ref.size < 1 || ref.size > 1024 * 1024) return invalid('DEPENDENCY_REPORT_INVALID');
      reports.set(binding.reportArtifact, { ref, value: reportJSON(await dependencyArtifactBytes(ref)) });
    }
    const report = reports.get(binding.reportArtifact)!;
    let received = report.value;
    for (const part of binding.jsonPointer.slice(1).split('/').map((k: string) => k.replace(/~1/g, '/').replace(/~0/g, '~'))) {
      if (!received || typeof received !== 'object' || !Object.hasOwn(received, part)) return invalid('DEPENDENCY_REPORT_INVALID');
      received = received[part];
    }
    // Only a well-formed but incorrect SHA is repairable. Invalid reports fail closed.
    if (typeof received !== 'string' || !/^[a-f0-9]{64}$/.test(received)) return invalid('DEPENDENCY_REPORT_INVALID');
    const dependencyArtifact = snapshot.artifacts.find(f => f.name === binding.dependencyArtifact)!;
    bindings.push({ reportArtifact: report.ref, jsonPointer: binding.jsonPointer, dependencyArtifact, expected: dependencyArtifact.digest, received });
  }
  const feedback = { version: 1, status: bindings.every(b => b.expected === b.received) ? 'matched' : 'mismatch',
    source: snapshot.source, bindings };
  await pool.query("UPDATE cycle_attempts SET context=jsonb_set(context,'{dependencyEvidenceValidation}',$3::jsonb) WHERE id=$1 AND fence=$2 AND state='running'", [a.id, a.fence, JSON.stringify(feedback)]);
  if (feedback.status === 'mismatch') throw new DependencyBindingError(feedback);
  return snapshot;
}
