/** A controller preflight can have no record; a remote preparation preflight
 * must have its exact durable receipt. Both still inspect the unit twice. */
export async function stoppedRunProof(runId, notStartedWorkspace, io) {
  let state;
  try {
    state = await io.readState(runId);
    if (notStartedWorkspace) {
      const receipt = state.notStarted;
      if (!(state.runId === runId && state.workspace === notStartedWorkspace &&
        state.runtime === 'opencode' && state.state === 'failed' && state.stopped === true &&
        state.code === 'DISK_CAPACITY' && state.requests === 0 && state.toolCalls === 0 &&
        receipt?.version === 1 && receipt.runId === runId && receipt.workspace === notStartedWorkspace &&
        receipt.runtime === 'opencode' && receipt.confirmed === true &&
        receipt.phase === 'remote_workspace_prepare' && receipt.errorCode === 'DISK_CAPACITY' &&
        receipt.remoteStartAttempted === true && receipt.workerStartAttempted === false && receipt.cancelRequested === false))
        throw Error('NOT_STARTED_RECEIPT_CONFLICT');
      state = { ...state, state: 'not_started' };
    }
  } catch (e) {
    if (e.code !== 'ENOENT' || !notStartedWorkspace) throw e;
    state = { runId, workspace: notStartedWorkspace, state: 'not_started' };
  }
  if (state.runId !== runId) throw Error('RUN_IDENTITY_MISMATCH');
  const unit = `agentos-run-${runId}`;
  const before = await io.inspectUnit(unit);
  if (!before.stopped) throw Error('STOP_UNCONFIRMED');
  const candidate = await io.inspectWorkspace(state.workspace);
  const after = await io.inspectUnit(unit);
  if (!after.stopped) throw Error('STOP_UNCONFIRMED');
  return {
    runId,
    workspace: state.workspace,
    state: state.state,
    candidate,
    process: after,
    checkedAt: after.checkedAt,
  };
}

/** Opt-in, sanitized source admission evidence. It never treats a policy or a
 * remembered stop receipt as physical termination. The legacy recovery helper
 * above retains its original response and not-started behavior. */
async function sourceAdmissionProof(runId, workspace, io, terminalStates) {
  if (!/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(runId ?? '') ||
      !/^[a-z0-9_-]+\/[a-z0-9_-]+$/.test(workspace ?? '')) throw Error('SOURCE_BINDING_IDENTITY_INVALID');
  const unit = `agentos-run-${runId}`;
  const confirm = proof => {
    const age = Date.now() - Date.parse(proof?.checkedAt);
    if (proof?.unit !== unit || proof.stopped !== true || proof.mainPid !== 0 ||
        proof.populated !== false || !['inactive', 'failed'].includes(proof.activeState) ||
        !Number.isFinite(age) || age < 0 || age > 60_000) throw Error('STOP_UNCONFIRMED');
  };
  confirm(await io.inspectUnit(unit));
  const binding = await io.inspectSourceBinding(runId, workspace);
  if (binding?.version !== 1 || binding.runId !== runId || binding.workspace !== workspace ||
      binding.state?.runId !== runId || binding.state?.workspace !== workspace ||
      !terminalStates.includes(binding.state?.state) || binding.state?.stopped !== true ||
      binding.admission?.runId !== runId || binding.admission?.runFence !== binding.state?.runFence ||
      binding.generation?.workspace !== workspace ||
      binding.admission?.sourceSnapshotDigest !== binding.generation?.snapshotDigest)
    throw Error('SOURCE_BINDING_INSPECTION_CONFLICT');
  const after = await io.inspectUnit(unit);
  confirm(after);
  return {...binding, process: after, checkedAt: after.checkedAt};
}

export function stoppedSourceBindingProof(runId, workspace, io) {
  return sourceAdmissionProof(runId, workspace, io, ['completed']);
}

/** Retry authority only: no artifact completeness or acceptance claim. Failed
 * and cancelled attempts still require the same protected admission and stop. */
export function stoppedRetrySourceBindingProof(runId, workspace, io) {
  return sourceAdmissionProof(runId, workspace, io, ['completed','failed','cancelled']);
}
