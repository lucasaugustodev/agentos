import { duration, MODEL_REQUEST_BUDGET_MS } from './duration.js';

type Row = Record<string, any>;
export function modelActivity(run: Row, events: Row[] = []) {
  const relevant = events
    .filter(
      (e) =>
        e.payload?.runId === run.id &&
        ['MODEL_REQUEST', 'MODEL_RESPONSE', 'MODEL_ERROR', 'MUSE_TOOL_EVENT'].includes(e.kind),
    )
    .sort(
      (a, b) => Date.parse(b.created_at) - Date.parse(a.created_at) || Number(b.id) - Number(a.id),
    );
  const request = relevant.find((e) => e.kind === 'MODEL_REQUEST');
  const latestModel = relevant.find((e) => e.kind !== 'MUSE_TOOL_EVENT');
  const museTool = relevant.find((e) => e.kind === 'MUSE_TOOL_EVENT');
  return {
    pending: latestModel?.kind === 'MODEL_REQUEST' && ['running', 'cancelling'].includes(run.state),
    requestedAt: request?.created_at,
    lastActivityAt: relevant[0]?.created_at,
    latestKind: relevant[0]?.kind,
    museTool,
    contextBytes: request?.payload.contextBytes,
    requests: request?.payload.requests,
  };
}
export function observedProblems(observation: Row | undefined, run: Row) {
  return (observation?.evidence ?? [])
    .filter(
      (file: Row) =>
        /\.log$/.test(file.name) &&
        file.preview &&
        (!run.started_at ||
          new Date(file.modifiedAt).getTime() >= new Date(run.started_at).getTime()),
    )
    .flatMap((file: Row) => {
      const line = file.preview
        .split('\n')
        .find((text: string) =>
          /^(?:BROWSER_FAILURE|\s*(?:Error|TypeError|ReferenceError|AssertionError|TimeoutError):|\s*(?:FAIL|FAILED)\s)/.test(
            text,
          ),
        );
      return line
        ? [
            {
              title: line.startsWith('BROWSER_FAILURE')
                ? 'Falha registrada no teste de navegador'
                : 'Falha registrada no teste ou programa',
              line,
              source: file.name,
              at: file.modifiedAt,
            },
          ]
        : [];
    })
    .slice(0, 3);
}
export function executionStatus(
  observation: Row | undefined,
  run: Row,
  now = Date.now(),
  model?: ReturnType<typeof modelActivity>,
  budgetMs = MODEL_REQUEST_BUDGET_MS,
) {
  if (!['running', 'cancelling', 'reconciling'].includes(run.state))
    return {
      level: 'ended',
      title: 'Tentativa encerrada',
      detail: 'Confira o resultado e os critérios de aceite abaixo.',
    };
  if (!observation || observation.unavailable)
    return {
      level: 'unknown',
      title: 'Sem observação do executor',
      detail: 'Não é possível confirmar a atividade remota agora.',
    };
  if (now - new Date(observation.observedAt).getTime() > 90000)
    return {
      level: 'unknown',
      title: 'Observação desatualizada',
      detail:
        'O último retrato remoto tem mais de 90 segundos. O estado da tarefa não confirma atividade atual.',
    };
  if (!observation.process?.available)
    return {
      level: 'unknown',
      title: 'Processo remoto sem confirmação',
      detail: 'Há registros de ferramentas, mas a consulta ao processo remoto não foi confirmada.',
    };
  if (observation.process?.available && observation.process.activeState !== 'active')
    return {
      level: 'warning',
      title: 'Processo remoto não está ativo',
      detail: 'O controlador ainda não confirmou o término desta tentativa.',
    };
  const current = observation.actions?.findLast((a: Row) => a.state === 'running');
  const last = observation.actions?.at(-1);
  if (!current && model?.pending && model.latestKind !== 'MUSE_TOOL_EVENT') {
    // Named against the request's own budget, and always with the elapsed time:
    // a fixed "over 5 minutes" hides both how long it really waited and how
    // close it is to being abandoned, which is the whole question while it hangs.
    const waited = now - Date.parse(model.requestedAt);
    const spent = waited / budgetMs;
    return {
      level: spent >= 0.5 ? 'warning' : 'active',
      title:
        spent >= 0.5
          ? `Modelo sem resposta há ${duration(waited)} de ${duration(budgetMs)}`
          : `Aguardando o modelo há ${duration(waited)}`,
      detail: `A chamada ao modelo começou há ${duration(waited)} e esta requisição é abandonada em ${duration(budgetMs)}. Nenhuma ferramenta está executando neste intervalo.`,
    };
  }
  const lastAt =
    current?.startedAt ??
    [last?.finishedAt, model?.lastActivityAt, run.started_at]
      .filter(Boolean)
      .sort((a, b) => Date.parse(b) - Date.parse(a))[0];
  if (lastAt && now - new Date(lastAt).getTime() > 300000)
    return {
      level: 'warning',
      title: 'Sem novo retorno há mais de 5 minutos',
      detail:
        'Pode ser uma operação longa ou uma trava. Processo ativo sozinho não confirma progresso.',
    };
  if (current)
    return {
      level: 'active',
      title: 'Comando em execução',
      detail: 'A ferramenta foi iniciada e ainda não devolveu o resultado final.',
    };
  if (model?.latestKind === 'MUSE_TOOL_EVENT')
    return {
      level: 'active',
      title: 'Atividade de ferramenta do Muse',
      detail: `Última operação registrada: ${model.museTool?.payload?.operation ?? 'ferramenta'}. O resultado final da tarefa ainda não foi entregue.`,
    };
  return {
    level: 'active',
    title: 'Entre chamadas de ferramentas',
    detail:
      'Consulte a última resposta e a atividade do modelo; o executor continua sob observação.',
  };
}
export function historicalEstimate(task: Row, run: Row, data: Row, now = Date.now()) {
  const samples = new Map<string, number>();
  for (const previous of data.runs ?? []) {
    const candidate = data.tasks.find((t: Row) => t.id === previous.task_id);
    const goal = candidate && data.goals.find((g: Row) => g.id === candidate.goal_id);
    if (
      !candidate ||
      !goal ||
      goal.is_demo ||
      candidate.state !== 'accepted' ||
      candidate.id === task.id ||
      candidate.agent_id !== task.agent_id ||
      candidate.execution?.kind !== task.execution?.kind ||
      previous.model !== run.model ||
      !previous.started_at ||
      !previous.finished_at
    )
      continue;
    // Use the final attempt of an accepted task; not its earlier failures.
    if (data.runs.some((r: Row) => r.task_id === candidate.id && r.attempt > previous.attempt))
      continue;
    const duration =
      new Date(previous.finished_at).getTime() - new Date(previous.started_at).getTime();
    if (duration > 0) samples.set(candidate.id, duration);
  }
  if (samples.size < 3)
    return {
      title: 'Sem previsão confiável',
      detail: `${samples.size} etapas comparáveis concluídas; precisamos de pelo menos 3 para formar uma faixa histórica.`,
    };
  const times = [...samples.values()].sort((a, b) => a - b);
  const elapsed = run.started_at ? now - new Date(run.started_at).getTime() : 0;
  const low = times[Math.floor((times.length - 1) * 0.5)];
  const high = times[Math.ceil((times.length - 1) * 0.9)];
  if (elapsed > high)
    return {
      title: 'Além da faixa histórica',
      detail: `A tentativa já ultrapassou a referência de ${Math.ceil(high / 60000)} min. É preciso avaliar o bloqueio antes de prever o término.`,
    };
  return {
    title: `${Math.max(1, Math.ceil((low - elapsed) / 60000))}–${Math.max(1, Math.ceil((high - elapsed) / 60000))} min restantes`,
    detail: `Referência P50–P90 de ${samples.size} etapas aceitas com o mesmo agente, modelo e tipo de execução. Não é garantia nem previsão do objetivo inteiro.`,
  };
}
export function actionSummary(action: Row) {
  const command = action.command ?? '';
  if (/browser|playwright|chromium/i.test(command)) return 'Comando com teste de navegador';
  if (/math[-_]|million|1000000|rtp/i.test(command)) return 'Verificação de matemática / RTP';
  if (/validate|\btest\b|pytest|vitest/i.test(command)) return 'Execução de testes e validações';
  if (/write|write_text|writeFile|apply_patch|open\([^\n]*['"]w|git apply/.test(command))
    return 'Alteração de arquivos';
  if (/\bcat\b|\brg\b|\bsed\b|\bhead\b|\bls\b/.test(command))
    return 'Inspeção de código e arquivos';
  return `Ferramenta ${action.tool ?? 'do agente'}`;
}
