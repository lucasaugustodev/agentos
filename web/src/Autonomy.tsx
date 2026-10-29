import React, { useEffect, useState } from 'react';
import { elapsed } from './run-route';
import './live-routes.css';

const duration = (s?: number | null) =>
  s == null ? '—' : s < 3600 ? Math.round(s / 60) + ' min' : (s / 3600).toFixed(1).replace('.', ',') + ' h';
const statusLabel: Record<string, string> = {
  active: 'Em andamento',
  planned: 'Planejado',
  replanning: 'Replanejando',
  paused: 'Pausado',
  completed: 'Concluído',
  failed: 'Falhou',
};

export function Autonomy({ api }: { api: (url: string) => Promise<any> }) {
  const [data, setData] = useState<any>(null),
    [error, setError] = useState('');
  useEffect(() => {
    let stopped = false;
    const refresh = async () => {
      try {
        const d = await api('/autonomy');
        if (!stopped) {
          setData(d);
          setError('');
        }
      } catch {
        if (!stopped) setError('Não foi possível consultar os indicadores de autonomia.');
      }
    };
    void refresh();
    const timer = setInterval(refresh, 15000);
    return () => {
      stopped = true;
      clearInterval(timer);
    };
  }, [api]);
  if (error && !data) return <section role="alert">{error}</section>;
  if (!data) return <p>Consultando indicadores…</p>;
  const t = data.totals,
    rate = t.attempts ? Math.round((t.succeeded / t.attempts) * 100) : null;
  return (
    <section className="autonomy-panel">
      <h2>Autonomia da operação</h2>
      <p>
        Últimas {data.windowHours} h · atualizado {new Date(data.observedAt).toLocaleTimeString('pt-BR')}
        {error && ' · última atualização falhou'}
      </p>
      <div className="autonomy-cards">
        <article>
          <strong>{data.active.length}</strong>
          <span>Tarefas executando agora</span>
        </article>
        <article>
          <strong>
            {t.succeeded} / {t.attempts}
          </strong>
          <span>Tentativas concluídas{rate !== null ? ` · ${rate}%` : ''}</span>
        </article>
        <article>
          <strong>{t.failed}</strong>
          <span>Tentativas com falha (recuperação automática)</span>
        </article>
        <article>
          <strong>{t.interventions}</strong>
          <span>Intervenções suas · {t.attention} itens pedindo atenção</span>
        </article>
      </div>
      <h3>Objetivos</h3>
      <ul className="autonomy-list">
        {data.objectives.map((o: any) => (
          <li key={o.id}>
            <strong>{o.title}</strong>
            <span>
              {statusLabel[o.state] ?? o.state} · {o.accepted_count}/{o.task_count} etapas aceitas ·{' '}
              {o.running} executando · {o.failures}/{o.attempts} falhas
              {o.last_accepted_at ? ` · último aceite há ${elapsed(o.last_accepted_at)}` : ''}
              {o.attention_count ? ` · ${o.attention_count} pedem atenção` : ''}
            </span>
          </li>
        ))}
      </ul>
      <h3>Contas</h3>
      <ul className="autonomy-list">
        {data.accounts.map((a: any) => (
          <li key={a.id}>
            <strong>
              {a.label} · {a.model}
            </strong>
            <span>
              {a.running}/{a.max_concurrency} vagas em uso · {a.succeeded} concluídas · {a.failed} falhas
              {a.timeouts ? ` (${a.timeouts} por tempo esgotado)` : ''} · média de {duration(a.avg_success_seconds)} por
              tentativa concluída
            </span>
          </li>
        ))}
      </ul>
      {!!data.errors.length && (
        <>
          <h3>Falhas mais comuns</h3>
          <ul className="autonomy-list">
            {data.errors.map((e: any) => (
              <li key={e.code}>
                <strong>{e.code}</strong>
                <span>{e.n} tentativas</span>
              </li>
            ))}
          </ul>
        </>
      )}
      <details>
        <summary>Ver execuções ativas</summary>
        <ul className="autonomy-list">
          {data.active.map((r: any) => (
            <li key={r.id}>
              <strong>{r.title}</strong>
              <span>
                {r.stage} · {r.model} · {r.account_label ?? 'conta automática'} ·{' '}
                {r.state === 'running' ? 'há ' + elapsed(r.started_at) : r.state}
                {r.heartbeat_at ? ' · sinal há ' + elapsed(r.heartbeat_at) : ''}
              </span>
            </li>
          ))}
        </ul>
      </details>
    </section>
  );
}
