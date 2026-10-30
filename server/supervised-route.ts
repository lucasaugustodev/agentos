import { chooseWorkforceRoute, workforceCapacityReason } from './workforce-policy.js';
import {readModelPolicy,modelRouteAllowed} from '../bridge/model-policy.mjs';

export type SupervisedRoute = { accountId: string; model: string };

const solModel = /^gpt-[\d.]+-sol(?:$|[-:])/i;
const lunaModel = /^gpt-[\d.]+-luna(?:$|[-:])/i;
const glmModel = /^glm[-.]/i;

/** Validate an explicit supervisor route using the same account and workforce policy as admission. */
export function resolveSupervisedRoute(task: any, route: SupervisedRoute, policy: any, accounts: any[], activeRuns: any[], now = Date.now()) {
  const exclusive=readModelPolicy();
  if (exclusive?.version===2 && task?.id &&
      ((task.account_id!=null&&task.account_id!==exclusive.accountId)||
       (task.model!=null&&task.model!==exclusive.model)))
    return {ok:false as const,code:'SUPERVISION_ROUTE_UNAVAILABLE'};
  if (!route || typeof route.accountId !== 'string' || typeof route.model !== 'string' ||
      task?.stage === 'coordinate' || task?.execution?.kind !== 'harness' ||
      task?.execution?.readOnlySource === true || !['work', 'integrate'].includes(task?.stage) ||
      policy?.enabled !== true || !Array.isArray(task?.allowed_accounts) ||
      !task.allowed_accounts.includes(route.accountId)) return { ok: false as const, code: 'SUPERVISION_ROUTE_UNAVAILABLE' };

  const account = accounts.find((candidate) => candidate.id === route.accountId);
  if (!account || !account.enabled || !account.bridge_id || !(account.models ?? []).includes(route.model) ||
      solModel.test(route.model)) return { ok: false as const, code: 'SUPERVISION_ROUTE_UNAVAILABLE' };

  // Derive the lane only from the requested account/model identity. It is not
  // an additional model-controlled permission and remains subject to policy.
  let lane: string | undefined;
  if (exclusive?.version===2 && account.bridge_id===exclusive.bridgeId &&
      modelRouteAllowed(account.id,route.model,exclusive)) lane='glm';
  else if (glmModel.test(route.model) && account.id === policy.glmAccountId) lane = 'glm';
  else if (lunaModel.test(route.model) && account.provider === 'codex') lane = 'luna';
  else if (['kimi', 'qwen', 'muse'].includes(account.provider)) lane = account.provider;
  else if (account.provider === 'agy' && task.execution.kind !== 'harness') lane = 'agy';
  if (!lane) return { ok: false as const, code: 'SUPERVISION_ROUTE_UNAVAILABLE' };

  const candidate = {
    ...task,
    account_id: route.accountId,
    model: route.model,
    execution: { ...task.execution, workforceLane: lane },
  };
  const selected = chooseWorkforceRoute(candidate, [account], policy, now);
  if (!selected || selected.accountId !== route.accountId || selected.model !== route.model)
    return { ok: false as const, code: 'SUPERVISION_ROUTE_UNAVAILABLE' };

  const capacity = workforceCapacityReason(candidate, activeRuns, policy);
  if (capacity) return { ok: false as const, code: 'SUPERVISION_ROUTE_CAPACITY' };
  const accountActive = activeRuns.filter((run) => run.account_id === route.accountId &&
    ['queued', 'running', 'cancelling', 'reconciling'].includes(run.state)).length;
  if (Number.isFinite(account.max_concurrency) && accountActive >= account.max_concurrency)
    return { ok: false as const, code: 'SUPERVISION_ROUTE_CAPACITY' };

  return { ok: true as const, route: selected, lane };
}
