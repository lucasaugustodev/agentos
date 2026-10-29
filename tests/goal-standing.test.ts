import { describe, expect, it } from 'vitest';
import { goalStanding } from '../web/src/goal-standing.js';

const none = { active: 0, attention: 0, ready: 0 };

describe('goal standing', () => {
  it('does not call a parked objective "em andamento"', () => {
    // The exact case that misled: goal active, objective waiting, nothing running.
    const standing = goalStanding(
      { state: 'active', objective_status: 'waiting_external', objective_reason: 'Falta hipótese.' },
      none,
    );
    expect(standing.tone).toBe('blocked');
    expect(standing.label).toBe('Parado: precisa de você');
    expect(standing.detail).toBe('Falta hipótese.');
  });

  it('falls back to a plain sentence when the objective gave no reason', () => {
    const standing = goalStanding({ state: 'active', objective_status: 'waiting_external' }, none);
    expect(standing.detail).toContain('aguarda');
  });

  it('says it is planning when no step is running yet', () => {
    const standing = goalStanding({ state: 'active', objective_status: 'planning' }, none);
    expect(standing.tone).toBe('planning');
    expect(standing.label).toBe('Planejando');
  });

  it('live work outranks the objective status', () => {
    const standing = goalStanding(
      { state: 'active', objective_status: 'waiting_external' },
      { ...none, active: 2 },
    );
    expect(standing.tone).toBe('running');
    expect(standing.detail).toBe('2 etapas em execução agora.');
  });

  it('counts one step in the singular', () => {
    expect(goalStanding({ state: 'active' }, { ...none, active: 1 }).detail).toBe(
      '1 etapa em execução agora.',
    );
  });

  it('separates waiting on a person from waiting on a dispatch', () => {
    expect(goalStanding({ state: 'active' }, { ...none, attention: 1 }).tone).toBe('blocked');
    expect(goalStanding({ state: 'active' }, { ...none, ready: 3 }).tone).toBe('idle');
    expect(goalStanding({ state: 'active' }, { ...none, ready: 3 }).detail).toContain('3 etapas');
  });

  it('reports nothing active rather than implying progress', () => {
    const standing = goalStanding({ state: 'active' }, none);
    expect(standing.tone).toBe('idle');
    expect(standing.label).toBe('Sem etapa ativa');
  });

  it('terminal and merged states keep their own wording', () => {
    expect(goalStanding({ state: 'completed' }, none).tone).toBe('done');
    expect(goalStanding({ state: 'cancelled' }, none).tone).toBe('cancelled');
    expect(goalStanding({ state: 'paused' }, none).tone).toBe('paused');
    expect(goalStanding({ state: 'active', merged_into: 'x' }, none).tone).toBe('merged');
  });

  it('a cancelled goal is cancelled even with a stale objective status', () => {
    expect(goalStanding({ state: 'cancelled', objective_status: 'planning' }, none).label).toBe(
      'Cancelado',
    );
  });
});
