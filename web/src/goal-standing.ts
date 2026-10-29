// What is actually happening to a goal right now, in one phrase.
//
// A goal row stays 'active' while its objective waits for a person, while it
// replans, and while it genuinely runs. Showing "Em andamento" for all three is
// what made a parked objective look busy, so the objective's own status and the
// live run count decide the label instead.

type Row = Record<string, any>;

export type GoalStanding = {
  /** Drives colour and grouping; 'blocked' and 'idle' are not failures. */
  tone: 'running' | 'planning' | 'blocked' | 'idle' | 'paused' | 'done' | 'cancelled' | 'merged';
  label: string;
  detail: string;
};

export function goalStanding(
  goal: Row,
  counts: { active: number; attention: number; ready: number } = {
    active: 0,
    attention: 0,
    ready: 0,
  },
): GoalStanding {
  if (goal.merged_into)
    return {
      tone: 'merged',
      label: 'Consolidado',
      detail: 'Tarefas e evidências reunidas na operação principal.',
    };
  if (goal.state === 'cancelled')
    return { tone: 'cancelled', label: 'Cancelado', detail: 'Objetivo encerrado por cancelamento.' };
  if (goal.state === 'completed')
    return { tone: 'done', label: 'Concluído', detail: 'Todas as etapas foram aceitas.' };
  if (goal.state === 'paused')
    return { tone: 'paused', label: 'Pausado', detail: 'Novos despachos estão suspensos.' };

  // Work in flight outranks every status: something is running right now.
  if (counts.active > 0)
    return {
      tone: 'running',
      label: 'Em execução',
      detail: `${counts.active} ${counts.active === 1 ? 'etapa em execução' : 'etapas em execução'} agora.`,
    };

  const status = goal.objective_status;
  if (status === 'waiting_external')
    return {
      tone: 'blocked',
      label: 'Parado: precisa de você',
      // The objective's own reason is the honest detail; it says what is missing.
      detail: goal.objective_reason ?? 'O objetivo aguarda uma informação nova para seguir.',
    };
  if (status === 'planning' || status === 'replanning')
    return {
      tone: 'planning',
      label: status === 'planning' ? 'Planejando' : 'Replanejando',
      detail: 'Nenhuma etapa em execução: o plano está sendo montado.',
    };
  if (counts.attention > 0)
    return {
      tone: 'blocked',
      label: 'Parado: precisa de você',
      detail: `${counts.attention} ${counts.attention === 1 ? 'etapa aguarda' : 'etapas aguardam'} sua decisão.`,
    };
  if (counts.ready > 0)
    return {
      tone: 'idle',
      label: 'Aguardando despacho',
      detail: `${counts.ready} ${counts.ready === 1 ? 'etapa elegível' : 'etapas elegíveis'} na fila.`,
    };
  return {
    tone: 'idle',
    label: 'Sem etapa ativa',
    detail: 'Nenhuma etapa em execução nem elegível agora.',
  };
}
