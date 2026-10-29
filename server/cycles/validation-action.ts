import { execFile } from 'node:child_process';
import { readFile } from 'node:fs/promises';
import { promisify } from 'node:util';
import { DomainError } from '../contracts.js';
import type { ActionProfile } from './integrations.js';

export const registeredValidationExecutorSha = '39843bdbde81e84b5093ed6667484378d1f4444c66aa33424b52d28b45b0cc44';

// This reserved action is a registered executor contract, independent of the
// profile's generic "read" classification. Other read profiles keep their rules.
export function isRegisteredValidation(action: string, profile: ActionProfile): boolean {
  if (action !== 'agentos.validate') return false;
  if (profile.command !== '/usr/bin/python3' || profile.effect !== 'read' ||
      JSON.stringify(profile.args) !== JSON.stringify(['/opt/agentos-control/agentos-release-action.py', 'validate']) ||
      !profile.projects.includes('agentos'))
    throw new DomainError('ACTION_UNAVAILABLE', 'O perfil de validação registrada foi alterado.');
  return true;
}

function uncertain(): never {
  throw new DomainError('STOP_UNCONFIRMED', 'Validação sem término confirmado. Reconcilie a execução antes de repetir.');
}

export async function confirmRegisteredValidationStop(attemptId: string, result: any) {
  if (!/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(attemptId)) uncertain();
  const unit = `agentos-validation-${attemptId}.service`, group = `/system.slice/${unit}`;
  const output = result?.output, execution = output?.validationExecution, proof = execution?.stopProof;
  if (result?.ok !== true || output?.executorSha256 !== registeredValidationExecutorSha ||
      !(output?.passed === false || /^[a-f0-9]{64}$/.test(output?.digest ?? '')) ||
      execution?.unit !== unit || proof?.unit !== unit || proof?.mainPID !== 0 || proof?.populated !== false ||
      !['loaded', 'not-found'].includes(proof?.loadState) || !['inactive', 'failed'].includes(proof?.activeState) ||
      !['', group].includes(proof?.controlGroup)) uncertain();
  try {
    const { stdout } = await promisify(execFile)('/usr/bin/systemctl',
      ['show', unit, '--property=LoadState,ActiveState,MainPID,ControlGroup'],
      { timeout: 5000, maxBuffer: 16384, encoding: 'utf8' });
    const state = Object.fromEntries(stdout.split('\n').filter(line => line.includes('=')).map(line => {
      const index = line.indexOf('='); return [line.slice(0, index), line.slice(index + 1)];
    }));
    if (!['loaded', 'not-found'].includes(state.LoadState) || !['inactive', 'failed'].includes(state.ActiveState) ||
        state.MainPID !== '0' || !['', group].includes(state.ControlGroup)) uncertain();
    try {
      const events = await readFile(`/sys/fs/cgroup${group}/cgroup.events`, 'utf8');
      if (!/^populated 0$/m.test(events)) uncertain();
    } catch (error: any) {
      if (error?.code !== 'ENOENT' || state.ControlGroup !== '') uncertain();
    }
    return { unit, mainPID: 0, populated: false, controlGroup: state.ControlGroup,
      activeState: state.ActiveState, loadState: state.LoadState };
  } catch {
    uncertain();
  }
}
