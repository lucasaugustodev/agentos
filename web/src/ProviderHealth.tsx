// Whether the provider is answering, stated in the words an operator needs:
// "connected" is what the bridge believes, which is not the same question.
import { duration, sinceLabel } from './duration.js';

export type ProviderHealthRow = {
  state: 'idle' | 'healthy' | 'slow' | 'failing' | 'down';
  requests: number;
  failures: number;
  timeouts: number;
  consecutiveFailures: number;
  lastSuccessAt: number | null;
  lastFailureAt: number | null;
  lastFailureCode: string | null;
  p50ProviderMs: number | null;
  p95ProviderMs: number | null;
  maxProviderMs: number | null;
  maxQueuedMs: number | null;
};

const labels: Record<ProviderHealthRow['state'], string> = {
  idle: 'Sem chamadas',
  healthy: 'Respondendo',
  slow: 'Respondendo devagar',
  failing: 'Falhando',
  down: 'Sem resposta',
};

export function ProviderHealthBadge({
  health,
  windowMinutes,
  now,
}: {
  health?: ProviderHealthRow;
  windowMinutes: number;
  now: number;
}) {
  if (!health) return null;
  const detail: string[] = [];
  if (health.state === 'idle') detail.push('nenhuma chamada em ' + windowMinutes + ' min');
  else {
    detail.push('última resposta ' + sinceLabel(health.lastSuccessAt, now));
    if (health.p95ProviderMs !== null) detail.push('p95 ' + duration(health.p95ProviderMs));
    if (health.timeouts) detail.push(health.timeouts + ' por tempo esgotado');
    if (health.consecutiveFailures)
      detail.push(health.consecutiveFailures + ' falhas seguidas');
    if (health.lastFailureCode && health.state !== 'healthy')
      detail.push(health.lastFailureCode);
  }
  return (
    <div className={'provider-health ' + health.state} title={detail.join(' · ')}>
      <span className="provider-health-dot" aria-hidden="true" />
      <div>
        <strong>{labels[health.state]}</strong>
        <small>{detail.join(' · ')}</small>
        {health.maxQueuedMs ? (
          // Shown apart on purpose: time spent waiting for a permit is ours, not
          // the provider's, and blaming it on the provider sends you debugging
          // the wrong system.
          <small className="provider-health-queue">
            espera por permissão até {duration(health.maxQueuedMs)}
          </small>
        ) : null}
      </div>
    </div>
  );
}
