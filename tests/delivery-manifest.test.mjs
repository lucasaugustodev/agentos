import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, writeFile, readFile, rm, symlink, truncate } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { randomUUID, createHash } from 'node:crypto';
import { spawnSync } from 'node:child_process';
import { deliveryContract, extractPromptJsonFields, deliveryManifestInstructionFor, prepareDeliveryManifest, sealDeliveryManifest, collectDeliveryManifest } from '../bridge/delivery-manifest.mjs';
import { recoverArtifacts } from '../bridge/recover-artifacts.mjs';
import { parseResult } from '../bridge/opencode-executor.mjs';
import { prepareOpenCodeWorkspace } from '../bridge/opencode-preflight.mjs';
import { stoppedRunProof } from '../bridge/stopped-run-proof.mjs';

test('remote preflight never emits evidence for cancellation, another error or failed persistence', async () => {
  for (const scenario of ['cancelled','other-error','persist-failed']) {
    const controller = new AbortController(); if (scenario === 'cancelled') controller.abort();
    const events = [];
    await assert.rejects(prepareOpenCodeWorkspace({ runId: randomUUID(), workspace: 'test/o-work', sourceWorkspace: 'test/source' }, {
      signal: controller.signal, emit: (...args) => events.push(args),
      persist: async () => { if (scenario === 'persist-failed') throw Error('WRITE_FAILED'); },
      operation: async () => { throw Object.assign(Error('preflight'), { code: scenario === 'other-error' ? 'SOURCE_BUSY' : 'DISK_CAPACITY' }); },
    }));
    assert.deepEqual(events, []);
  }
});

test('remote preflight proof rejects model activity, wrong ownership and cancelled or incomplete state', async () => {
  const runId = randomUUID(), workspace = 'test/o-work'; let receipt;
  await assert.rejects(prepareOpenCodeWorkspace({ runId, workspace, sourceWorkspace: 'test/source' }, {
    signal: new AbortController().signal, emit: () => {}, persist: async value => { receipt = value; },
    operation: async () => { throw Object.assign(Error('DISK_CAPACITY'), { code: 'DISK_CAPACITY' }); },
  }));
  const state = { runId, workspace, runtime: 'opencode', state: 'failed', stopped: true, code: 'DISK_CAPACITY', requests: 0, toolCalls: 0, notStarted: receipt };
  for (const changes of [{ requests: 1 }, { toolCalls: 1 }, { stopped: false }, { state: 'starting' }, { state: 'cancelled' }, { notStarted: { ...receipt, runId: randomUUID() } }, { notStarted: { ...receipt, workerStartAttempted: true } }]) {
    let inspected = false;
    await assert.rejects(stoppedRunProof(runId, workspace, { readState: async () => ({ ...state, ...changes }), inspectUnit: async () => { inspected = true; }, inspectWorkspace: async () => ({}) }), /NOT_STARTED_RECEIPT_CONFLICT/);
    assert.equal(inspected, false);
  }
});

const hash = bytes => createHash('sha256').update(bytes).digest('hex');
async function fixture() {
  const root = await mkdtemp(path.join(tmpdir(), 'agentos-manifest-'));
  const contract = deliveryContract(randomUUID()), workspaceId = `test/o-${contract.runId}`;
  const runRoot = path.join(root, 'runs'), workspaceRoot = path.join(root, 'workspaces');
  const directory = path.join(runRoot, contract.runId), home = path.join(directory, 'home');
  const workspace = path.join(workspaceRoot, workspaceId);
  await mkdir(home, { recursive: true }); await mkdir(path.join(workspace, 'evidence'), { recursive: true });
  await prepareDeliveryManifest(home, contract);
  const report = '{"passed":false,"limitations":["real regression still fails"]}';
  await writeFile(path.join(workspace, 'evidence', 'verification.json'), report);
  const draft = { summary: 'Real output, no acceptance claimed.', artifacts: [{ name: 'verification.json', path: 'evidence/verification.json', mime: 'application/json' }] };
  const manifest = await sealDeliveryManifest(workspace, draft, contract);
  await writeFile(path.join(home, 'delivery.json'), JSON.stringify(manifest));
  const state = { runId: contract.runId, workspace: workspaceId, runtime: 'harness', state: 'failed',
    code: 'RESULT_FORMAT', stopped: true, turnReason: { kind: 'completed' }, deliveryContract: contract };
  const saveState = () => writeFile(path.join(directory, 'state.json'), JSON.stringify(state));
  await saveState();
  const candidate = { baseDigest: hash('base'), candidateDigest: hash('candidate'), files: [], patch: '' };
  const options = { runRoot, workspaceRoot,
    inspect: async unit => ({ unit, stopped: true, mainPid: 0, populated: false, activeState: 'inactive', checkedAt: new Date().toISOString() }),
    operation: async () => candidate };
  return { root, contract, workspace, home, draft, manifest, state, saveState, candidate, options,
    close: () => rm(root, { recursive: true, force: true }) };
}

test('generic packaging binds hashes to this run while preserving a genuinely failed business report', async () => {
  const f = await fixture();
  try {
    const value = await collectDeliveryManifest(f.workspace, f.home, f.contract);
    assert.equal(value.runId, f.contract.runId);
    assert.equal(JSON.parse(Buffer.from(value.artifacts[0].base64, 'base64')).passed, false);
    assert.equal(value.artifacts[0].digest, f.manifest.artifacts[0].digest);
    assert.match(value.manifestDigest, /^[a-f0-9]{64}$/);
  } finally { await f.close(); }
});

test('current-attempt envelope cannot be replaced with another run or nonce', async () => {
  const f = await fixture();
  try {
    for (const fields of [{ runId: randomUUID() }, { nonce: randomUUID() }]) {
      await writeFile(path.join(f.home, 'delivery.json'), JSON.stringify({ ...f.manifest, ...fields }));
      await assert.rejects(collectDeliveryManifest(f.workspace, f.home, f.contract), /RESULT_FORMAT/);
    }
  } finally { await f.close(); }
});

test('bytes changed after sealing require explicit resealing in this attempt', async () => {
  const f = await fixture();
  try {
    await writeFile(path.join(f.workspace, 'evidence', 'verification.json'), '{"passed":true}');
    await assert.rejects(collectDeliveryManifest(f.workspace, f.home, f.contract), /ARTIFACT_CORRUPT/);
  } finally { await f.close(); }
});

test('an absent manifest preserves legacy parsing, while a missing declared file has a typed packaging failure', async () => {
  const f = await fixture();
  try {
    await rm(path.join(f.home, 'delivery.json'));
    assert.equal(await collectDeliveryManifest(f.workspace, f.home, f.contract), null);
    await writeFile(path.join(f.home, 'delivery.json'), JSON.stringify(f.manifest));
    await rm(path.join(f.workspace, 'evidence', 'verification.json'));
    await assert.rejects(collectDeliveryManifest(f.workspace, f.home, f.contract), { code: 'ARTIFACT_MISSING' });
  } finally { await f.close(); }
});

test('draft validator checks every required artifact name without inventing a passing value', async () => {
  const f = await fixture();
  try {
    const contract = deliveryContract(randomUUID(), ['verification.json', 'real-test.log']);
    await assert.rejects(sealDeliveryManifest(f.workspace, f.draft, contract), /ARTIFACT_MISSING/);
    await writeFile(path.join(f.workspace, 'evidence', 'real-test.log'), 'FAIL: actual invariant remains false');
    const manifest = await sealDeliveryManifest(f.workspace, { ...f.draft,
      artifacts: [...f.draft.artifacts, { name: 'real-test.log', path: 'evidence/real-test.log' }] }, contract);
    assert.equal(manifest.artifacts.length, 2);
    assert.equal(JSON.parse(await readFile(path.join(f.workspace, 'evidence', 'verification.json'))).passed, false);
  } finally { await f.close(); }
});

test('rejects path traversal and both leaf and ancestor symlinks, including links inside the workspace', async () => {
  const f = await fixture();
  try {
    await symlink(path.join(f.workspace, 'evidence'), path.join(f.workspace, 'linked'));
    await symlink(path.join(f.workspace, 'evidence', 'verification.json'), path.join(f.workspace, 'evidence', 'linked.json'));
    for (const p of ['../outside.json', '/etc/passwd', 'linked/verification.json', 'evidence/linked.json'])
      await assert.rejects(sealDeliveryManifest(f.workspace, { ...f.draft, artifacts: [{ name: 'report.json', path: p }] }, f.contract), /ARTIFACT_PATH/);
    await rm(path.join(f.home, 'delivery.json'));
    await symlink(path.join(f.home, 'delivery-contract.json'), path.join(f.home, 'delivery.json'));
    await assert.rejects(collectDeliveryManifest(f.workspace, f.home, f.contract), /ARTIFACT_PATH/);
  } finally { await f.close(); }
});

test('rejects directories, duplicate names, reserved runtime artifacts and excessive byte sizes', async () => {
  const f = await fixture();
  try {
    for (const artifacts of [
      [{ name: 'directory', path: 'evidence' }], [f.draft.artifacts[0], f.draft.artifacts[0]],
      [{ ...f.draft.artifacts[0], name: 'candidate.json' }],
    ]) await assert.rejects(sealDeliveryManifest(f.workspace, { ...f.draft, artifacts }, f.contract), /ARTIFACT_PATH/);
    await truncate(path.join(f.workspace, 'evidence', 'verification.json'), 8 * 1024 * 1024 + 1);
    await assert.rejects(sealDeliveryManifest(f.workspace, f.draft, f.contract), /ARTIFACT_TOO_LARGE/);
  } finally { await f.close(); }
});

test('validator CLI seals draft hashes and identities without overwriting evidence', async () => {
  const f = await fixture();
  try {
    const report = await readFile(path.join(f.workspace, 'evidence', 'verification.json'));
    const draftFile = path.join(f.workspace, 'evidence', 'delivery-draft.json');
    await writeFile(draftFile, JSON.stringify(f.draft));
    const result = spawnSync(process.execPath, [path.join(f.home, 'delivery-validator.mjs'), f.workspace,
      path.join(f.home, 'delivery-contract.json'), draftFile, path.join(f.home, 'delivery.json')], { encoding: 'utf8' });
    assert.equal(result.status, 0, result.stderr + result.stdout);
    assert.equal(JSON.parse(result.stdout).code, 'DELIVERY_MANIFEST_VALIDATED');
    assert.deepEqual(await readFile(path.join(f.workspace, 'evidence', 'verification.json')), report);
    assert.equal((await collectDeliveryManifest(f.workspace, f.home, f.contract)).artifacts.length, 1);
  } finally { await f.close(); }
});

for (const runtime of ['harness', 'opencode', 'muse']) test(`stopped ${runtime} integration recovers bound manifest without protocol files or a response message`, async () => {
  const f = await fixture();
  try {
    f.state.runtime = runtime;
    if (runtime === 'opencode') Object.assign(f.state, { exitCode: 0, finishReason: 'stop' });
    if (runtime === 'muse') Object.assign(f.state, { exitCode: 0, finishReason: 'completed' });
    await f.saveState();
    const result = await recoverArtifacts(f.contract.runId, false, undefined, false, f.options);
    assert.equal(result.collectorVersion, 'artifact-manifest-v1');
    assert.equal(result.manifest.runId, f.contract.runId);
    assert.deepEqual(result.artifacts.map(a => a.name), ['verification.json', 'candidate.json', 'changes.patch']);
    assert.equal(JSON.parse(Buffer.from(result.artifacts[0].base64, 'base64')).passed, false);
  } finally { await f.close(); }
});

test('recovery retains the opted-in baseline inventory required by source acceptance', async () => {
  const f=await fixture();
  try {
    f.state.sourceContractVersion=1;await f.saveState();
    const baseFiles=[{path:'src/winner.ts',sha256:hash('known-good')}];let inspections=0;
    const result=await recoverArtifacts(f.contract.runId,false,undefined,false,{...f.options,
      operation:async params=>{assert.equal(params.includeBaseFiles,true);inspections++;return {...f.candidate,baseFiles};}});
    assert.equal(inspections,2);
    const candidate=JSON.parse(Buffer.from(result.artifacts.find(a=>a.name==='candidate.json').base64,'base64'));
    assert.deepEqual(candidate.baseFiles,baseFiles);
  } finally {await f.close();}
});

test('generic collection refuses incomplete turns, cancellations and changed candidates', async () => {
  const f = await fixture();
  try {
    f.state.turnReason.kind = 'cancelled'; await f.saveState();
    await assert.rejects(recoverArtifacts(f.contract.runId, false, undefined, false, f.options), /DELIVERY_NOT_FINISHED/);
    f.state.turnReason.kind = 'completed'; f.state.state = 'cancelled'; await f.saveState();
    await assert.rejects(recoverArtifacts(f.contract.runId, false, undefined, false, f.options), /DELIVERY_NOT_RECOVERABLE/);
    f.state.state = 'failed'; await f.saveState();
    let reads = 0;
    await assert.rejects(recoverArtifacts(f.contract.runId, false, undefined, false, { ...f.options,
      operation: async () => ({ ...f.candidate, candidateDigest: hash(String(reads++)) }) }), /SOURCE_CHANGED_DURING_COLLECTION/);
  } finally { await f.close(); }
});

test('collector requires fresh stop before and after collection and preserves source-busy', async () => {
  const f = await fixture();
  try {
    for (const failAt of [0, 1]) {
      let calls = 0;
      await assert.rejects(recoverArtifacts(f.contract.runId, false, undefined, false, { ...f.options,
        inspect: async unit => ({ ...await f.options.inspect(unit), stopped: calls++ !== failAt }) }), /STOP_UNCONFIRMED/);
    }
    await assert.rejects(recoverArtifacts(f.contract.runId, false, undefined, false, { ...f.options,
      operation: async () => { throw Error('SOURCE_BUSY'); } }), /SOURCE_BUSY/);
  } finally { await f.close(); }
});

test('strict final-message parser accepts one explicit fenced envelope but never rescues an earlier draft', () => {
  const json = JSON.stringify({ summary: 'actual final', artifacts: [] });
  assert.equal(parseResult(['Completed.\n```json\n' + json + '\n```\nEvidence attached.']).summary, 'actual final');
  assert.throws(() => parseResult([json, 'The final result is not structured']), /RESULT_FORMAT/);
  assert.throws(() => parseResult(['```json\n' + json + '\n```\n```json\n' + json + '\n```']), /RESULT_FORMAT/);
});

test('validator rejects delivery-draft self-reference, manifest envelope confusion and missing required json_field while allowing passed:false', async () => {
  const f = await fixture();
  try {
    const prompt = 'Task title\n\nCriteria: [{"id":"delivery","type":"json_field","name":"delivery.json","field":"passed","equals":true}]';
    const fields = extractPromptJsonFields(prompt);
    assert.deepEqual(fields, [{ name: 'delivery.json', field: 'passed', boolean: true }]);
    const contract = deliveryContract(randomUUID(), ['delivery.json'], fields);
    assert.match(deliveryManifestInstructionFor(contract), /evidence\/delivery\.json/);
    const draftFile = path.join(f.workspace, 'evidence', 'delivery-draft.json');
    await writeFile(draftFile, JSON.stringify({ summary: 'Draft', artifacts: [{ name: 'delivery.json', path: 'evidence/delivery-draft.json', mime: 'application/json' }] }));
    await assert.rejects(
      sealDeliveryManifest(f.workspace, JSON.parse(await readFile(draftFile, 'utf8')), contract),
      { code: 'ARTIFACT_PATH', reason: 'delivery_draft_is_not_a_task_artifact' },
    );
    await writeFile(path.join(f.workspace, 'evidence', 'delivery.json'), JSON.stringify({ summary: 'Wrong envelope', artifacts: [] }));
    await assert.rejects(
      sealDeliveryManifest(f.workspace, { summary: 'Wrong envelope', artifacts: [{ name: 'delivery.json', path: 'evidence/delivery.json', mime: 'application/json' }] }, contract),
      { code: 'RESULT_FORMAT', reason: 'artifact_contains_manifest_envelope_instead_of_criterion_report:delivery.json' },
    );
    await writeFile(path.join(f.workspace, 'evidence', 'delivery.json'), JSON.stringify({ note: 'missing passed' }));
    await assert.rejects(
      sealDeliveryManifest(f.workspace, { summary: 'Missing field', artifacts: [{ name: 'delivery.json', path: 'evidence/delivery.json', mime: 'application/json' }] }, contract),
      { code: 'RESULT_FORMAT', reason: 'artifact_json_field_missing:delivery.json:passed' },
    );
    for (const passed of [false, true]) {
      await writeFile(path.join(f.workspace, 'evidence', 'delivery.json'), JSON.stringify({ passed, checks: 38 }));
      const sealed = await sealDeliveryManifest(f.workspace, { summary: 'Valid report', artifacts: [{ name: 'delivery.json', path: 'evidence/delivery.json', mime: 'application/json' }] }, contract);
      assert.equal(sealed.artifacts[0].name, 'delivery.json');
    }
  } finally { await f.close(); }
});
