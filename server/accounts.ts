import { readModelPolicy, assertModelRoute } from '../bridge/model-policy.mjs';
import { pool, tx, event } from './db.js';
import { accountBridge } from './bridge.js';
import { config } from './config.js';
import { DomainError } from './contracts.js';

export function isNativeOnlyAccount(account: any) {
  return (account.family ?? account.provider) === 'muse';
}

/** Native subscriptions execute isolated worker tasks, not generic generate calls. */
export function supportsAccountExecution(account: any, execution?: { kind?: string }) {
  return !isNativeOnlyAccount(account) || execution?.kind === 'harness';
}

export function quotaFor(account: any, model: string) {
  const q = account.quota ?? {},
    family = account.family ?? account.provider;
  const label =
    family === 'agy'
      ? /claude|gpt/i.test(model)
        ? 'Claude e GPT'
        : 'Gemini'
      : ({ codex: 'codex', claude: 'Claude', kimi: 'Kimi', qwen: 'Qwen', muse: 'Muse' } as any)[
          family
        ];
  const group = q.groups?.find((g: any) => g.label === label),
    windows = (group?.windows ?? []).filter(
      (w: any) => !w.disabled && Number.isFinite(w.remainingPercent),
    );
  const remaining = windows.length
    ? Math.min(...windows.map((w: any) => w.remainingPercent))
    : null;
  const blocked =
    account.connection_status === 'reconnect_required' ||
    account.authStatus === 'reconnect_required';
  const feedback=account.execution_feedback?.[model];
  const executionHold=feedback?.code==='QUOTA_EXCEEDED' && Date.parse(feedback.blockedUntil)>Date.now();
  return {
    status: blocked
      ? 'auth_required'
      : executionHold || q.ordinaryUsageAllowed === false || (remaining !== null && remaining <= 0)
        ? 'exhausted'
        : q.status === 'ok' && remaining !== null
          ? 'available'
          : 'unknown',
    remaining,
    ...(executionHold?{blockedUntil:feedback.blockedUntil,source:'execution_feedback'}:{}),
  };
}
let syncing: Promise<any> | null = null,
  lastSync = 0;
export async function syncAccounts(force = false) {
  if (!config.bridgeHost) return { connected: false, count: 0 };
  if (syncing) return syncing;
  if (!force && Date.now() - lastSync < 30000) return { connected: true, cached: true };
  syncing = (async () => {
    const state = await accountBridge.request('state');
    await tx(async (db) => {
      const ids = [];
      for (const account of state.accounts) {
        const id = account.id.replace(/:/g, '-'),
          q = quotaFor(account, account.defaultModel);
        ids.push(id);
        await db.query(
          `INSERT INTO accounts(id,label,provider,model,enabled,max_concurrency,quota_status,remaining_percent,observed_at,allow_unknown,source,bridge_id,identity,models,quota,connection_status) VALUES($1,$2,$3,$4,$5,4,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15) ON CONFLICT(id) DO UPDATE SET label=excluded.label,provider=excluded.provider,model=excluded.model,enabled=excluded.enabled,quota_status=excluded.quota_status,remaining_percent=excluded.remaining_percent,observed_at=excluded.observed_at,allow_unknown=excluded.allow_unknown,source=excluded.source,bridge_id=excluded.bridge_id,identity=excluded.identity,models=excluded.models,quota=excluded.quota,connection_status=excluded.connection_status`,
          [
            id,
            account.label,
            account.family,
            account.defaultModel,
            account.connected,
            q.status,
            q.remaining,
            account.quota.updated ? new Date(account.quota.updated) : new Date(),
            q.status === 'unknown' && account.authStatus === 'connected',
            account.source,
            account.id,
            JSON.stringify(account.identity),
            JSON.stringify(account.models),
            JSON.stringify(account.quota),
            account.authStatus,
          ],
        );
      }
      await db.query(
        "UPDATE accounts SET enabled=false,connection_status='disconnected' WHERE source IN('harness','agentos') AND NOT(id=ANY($1::text[]))",
        [ids],
      );
    });
    lastSync = Date.now();
    return { connected: true, count: state.accounts.length, checkedAt: state.checkedAt };
  })().finally(() => {
    syncing = null;
  });
  return syncing;
}
export async function chooseChatAccount(accountId?: string, model?: string) {
  await syncAccounts();
  const accounts = (
    await pool.query(
      "SELECT * FROM accounts WHERE bridge_id IS NOT NULL AND enabled=true ORDER BY CASE provider WHEN 'codex' THEN 0 WHEN 'kimi' THEN 1 WHEN 'qwen' THEN 2 WHEN 'agy' THEN 3 ELSE 4 END,id",
    )
  ).rows;
  const exclusive = readModelPolicy();
  if (exclusive) {
    if (accountId && accountId !== 'auto') assertModelRoute(accountId, model || exclusive.model, exclusive);
    if (model) assertModelRoute(exclusive.accountId, model, exclusive);
    accountId = exclusive.accountId; model = exclusive.model;
  }
  const candidates =
    accountId && accountId !== 'auto'
      ? accounts.filter((a) => a.id === accountId)
      : accounts.filter((a) => !isNativeOnlyAccount(a));
  if (!candidates.length)
    throw new DomainError(
      'ACCOUNT_MISSING',
      'Nenhuma conta conectada está disponível. Use Conectar conta.',
    );
  for (const account of candidates) {
    if (isNativeOnlyAccount(account))
      throw new DomainError(
        'NATIVE_RUNTIME_ONLY',
        'Muse Code está disponível para agentes nas tarefas. Escolha outra conta para o chat.',
      );
    const selectedModel = model || account.model;
    if (!account.models.includes(selectedModel)) {
      if (accountId !== 'auto' && accountId)
        throw new DomainError('MODEL_NOT_AVAILABLE', 'O modelo não consta nesta conta.');
      continue;
    }
    const q = quotaFor(account, selectedModel);
    if (q.status === 'auth_required' || q.status === 'exhausted') continue;
    const n = Number(
      (
        await pool.query(
          "SELECT (SELECT count(*) FROM cycle_attempts WHERE account_id=$1 AND state IN('running','cancelling','reconciling'))+(SELECT count(*) FROM chat_messages WHERE account_id=$1 AND state IN('queued','running','cancelling'))+(SELECT count(*) FROM runs WHERE account_id=$1 AND state IN('queued','running','cancelling','reconciling'))+(SELECT count(*) FROM planning_runs WHERE account_id=$1 AND state='running' AND output IS NULL) n",
          [account.id],
        )
      ).rows[0].n,
    );
    if (n >= (exclusive?.version===2 ? Math.min(account.max_concurrency,exclusive.sessionLimit) : account.max_concurrency)) continue;
    return { account, model: selectedModel };
  }
  throw new DomainError(
    'ACCOUNT_UNAVAILABLE',
    'A conta está sem cota, precisa reconectar ou está com as vagas ocupadas. Escolha outra conta.',
  );
}
export async function refreshConnectionState() {
  return syncAccounts(true);
}
