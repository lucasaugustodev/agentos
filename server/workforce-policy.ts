import { readModelPolicy, modelRouteAllowed } from '../bridge/model-policy.mjs';
import {exclusiveModelPolicySql} from './model-policy-sql.js';
import type { DB } from './db.js';
import { quotaFor, supportsAccountExecution } from './accounts.js';

export type WorkforcePolicy = {
  enabled: boolean;
  glmAccountId: string;
  glmTarget: number;
  /** Scoped planners share the GLM session pool; this is an additional ceiling. */
  plannerMaxActive?: number;
  /** Provider request ceiling; separate from the number of live sessions. */
  glmRequestLimit?: number;
  /** Explicit operator authorization for GLM in the existing Harness workspace. */
  glmHarnessEnabled?: boolean;
  detachedHarnessEnabled?: boolean;
  deliveryFirstEnabled?: boolean;
  workerAccounts: string[];
  lunaTarget?: number;
  lunaAccounts?: string[];
  lunaFallback?: boolean;
  /** Requested Muse sessions; admission also respects the account concurrency. */
  museTarget?: number;
  coordinatorAccountId: string;
  coordinatorModel: string;
};

const isSol = (model: string) => /^gpt-[\d.]+-sol(?:$|[-:])/i.test(model);
const isLuna = (model: string) => /^gpt-[\d.]+-luna(?:$|[-:])/i.test(model);
const activeStates = new Set(['queued', 'running', 'cancelling', 'reconciling']);

/** Preflight for schedulers; the SQL admission guard remains the concurrency authority. */
export function workforceCapacityReason(
  task: any,
  activeRuns: any[],
  policy: WorkforcePolicy,
): string | null {
  const exclusive = readModelPolicy();
  if (exclusive && !modelRouteAllowed(task.account_id, task.model, exclusive)) return 'MODEL_POLICY_GLM_ONLY';
  if (!policy?.enabled) return null;
  const active = activeRuns.filter(
    (run) =>
      activeStates.has(run.state) &&
      (!task.goal_id || !run.goal_id || run.goal_id === task.goal_id),
  );
  if (policy.museTarget !== undefined && /^muse-/i.test(task.model ?? '') &&
      active.filter(run => /^muse-/i.test(run.model ?? '')).length >= policy.museTarget)
    return 'WORKFORCE_MUSE_CAPACITY';
  if ((exclusive && task.account_id === exclusive.accountId) || /^glm[-.]/i.test(task.model ?? '') || task.execution?.kind === 'opencode' ||
      (task.execution?.kind === 'harness' && task.execution?.workforceLane === 'glm')) {
    const nativeCount = active.filter(
      (run) => (exclusive && run.account_id === exclusive.accountId) || run.runtime === 'opencode' || run.execution?.kind === 'opencode' ||
        /^glm[-.]/i.test(run.model ?? '') ||
        (run.execution?.kind === 'harness' && run.execution?.workforceLane === 'glm'),
    ).length;
    if (nativeCount >= Math.max(1, policy.glmTarget ?? 6)) return 'WORKFORCE_GLM_CAPACITY';
  }
  if (task.stage === 'coordinate' && active.some(run => run.stage === 'coordinate'))
    return 'WORKFORCE_COORDINATOR_BUSY';
  if (isSol(task.model ?? '')) {
    if (task.stage !== 'coordinate') return 'WORKFORCE_SOL_RESERVED';
    if (active.some((run) => isSol(run.model ?? ''))) return 'WORKFORCE_COORDINATOR_BUSY';
  }
  if (
    policy.lunaTarget !== undefined &&
    (task.execution?.workforceLane === 'luna' || isLuna(task.model ?? '')) &&
    active.filter((run) => isLuna(run.model ?? '')).length >= Math.max(0, policy.lunaTarget)
  )
    return 'WORKFORCE_LUNA_CAPACITY';
  return null;
}

/** Route only within the user's workforce; quota is evaluated for the actual model. */
export function chooseWorkforceRoute(
  task: any,
  accounts: any[],
  policy: WorkforcePolicy,
  now = Date.now(),
): { accountId: string; model: string } | null {
  if (!policy?.enabled) return null;
  const exclusive = readModelPolicy();
  if (exclusive) {
    const a = accounts.find(a => a.enabled && a.id === exclusive.accountId && a.bridge_id === exclusive.bridgeId);
    if (!a || !supportsAccountExecution(a, task.execution) || !a.models?.includes(exclusive.model)) return null;
    if (task.allowed_accounts?.length && !task.allowed_accounts.includes(a.id)) return null;
    const q = quotaFor(a, exclusive.model);
    if (['exhausted', 'auth_required'].includes(q.status) || (q.status === 'unknown' && !a.allow_unknown)) return null;
    if (task.execution?.continuationRoutes?.some((r: any) => r.accountId === a.id && r.model === exclusive.model && Date.parse(r.until) > now)) return null;
    return { accountId: a.id, model: exclusive.model };
  }
  const coordinator = task.stage === 'coordinate';
  const native = task.execution?.kind === 'opencode';
  const lane = task.execution?.workforceLane ?? (native ? 'glm' : undefined);
  const glmHarness = lane === 'glm' && task.execution?.kind === 'harness' &&
    policy.glmHarnessEnabled === true;
  if (lane === 'muse' && task.execution?.kind !== 'harness') return null;
  const cooldowns = task.execution?.continuationRoutes ?? [];
  const lunaFallback =
    policy.lunaFallback === true &&
    task.execution?.kind === 'harness' &&
    (lane === undefined || lane === 'kimi' || lane === 'qwen');
  const permittedLuna = (account: any) =>
    (policy.lunaTarget ?? 0) > 0 &&
    account.provider === 'codex' &&
    !!policy.lunaAccounts?.includes(account.id);
  const candidates = accounts.filter((account) => {
    if (!account.enabled || !account.bridge_id) return false;
    if (!supportsAccountExecution(account, task.execution)) return false;
    if (task.allowed_accounts?.length && !task.allowed_accounts.includes(account.id)) return false;
    if (coordinator) return account.provider === 'codex' ||
      (account.id === policy.coordinatorAccountId && account.id === policy.glmAccountId && policy.glmHarnessEnabled === true);
    if (lane === 'luna') return permittedLuna(account);
    if (lunaFallback && permittedLuna(account))
      return !!task.allowed_accounts?.includes(account.id);
    if (!policy.workerAccounts.includes(account.id) && account.id !== policy.glmAccountId)
      return false;
    if (lane === 'glm' || native) return (native || glmHarness) && account.id === policy.glmAccountId;
    if (account.id === policy.glmAccountId || account.provider === 'codex') return false;
    if (lane === 'muse') return account.provider === 'muse';
    if (lane === 'agy') return task.execution?.kind !== 'harness' && account.provider === 'agy';
    if (lane === 'kimi' || lane === 'qwen') return ['kimi', 'qwen'].includes(account.provider);
    return ['kimi', 'qwen', 'muse'].includes(account.provider);
  });
  candidates.sort((a, b) => {
    const rank = (account: any) =>
      coordinator
        ? account.id === policy.coordinatorAccountId
          ? 0
          : 1
        : lunaFallback && account.provider === 'codex'
          ? 10
          : lane === 'kimi' || lane === 'qwen'
            ? account.provider === lane
              ? 0
              : 1
            : account.id === task.account_id
              ? 0
              : account.provider === 'qwen'
                ? 1
                : 2;
    return rank(a) - rank(b) || a.id.localeCompare(b.id);
  });
  for (const account of candidates) {
    const models = coordinator
      ? [policy.coordinatorModel]
      : lane === 'luna' || (lunaFallback && account.provider === 'codex')
        ? ['gpt-6-luna']
        : [...new Set([task.model, account.model, ...(account.models ?? [])].filter(Boolean))];
    for (const model of models) {
      if (!(account.models ?? []).includes(model) || (!coordinator && isSol(model))) continue;
      if ((native || glmHarness) && !/^glm[-.]/i.test(model)) continue;
      if (
        cooldowns.some(
          (route: any) =>
            route.accountId === account.id &&
            route.model === model &&
            Date.parse(route.until) > now,
        )
      )
        continue;
      const quota = quotaFor(account, model);
      if (['exhausted', 'auth_required'].includes(quota.status)) continue;
      if (quota.status === 'unknown' && !account.allow_unknown) continue;
      return { accountId: account.id, model };
    }
  }
  return null;
}

/** Durable policy covers the existing scheduler and native queue without restarting live runs. */
export async function installWorkforcePolicy(db: DB) {
  await db.query(`
CREATE TABLE IF NOT EXISTS opencode_jobs (
 run_id uuid PRIMARY KEY REFERENCES runs(id), state text NOT NULL DEFAULT 'queued',
 created_at timestamptz NOT NULL DEFAULT now(), started_at timestamptz, finished_at timestamptz
);
CREATE OR REPLACE FUNCTION agentos_workforce_resources() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF EXISTS(SELECT 1 FROM settings WHERE id='workforce:'||NEW.goal_id AND value->>'enabled'='true') THEN
  SELECT COALESCE(jsonb_agg(resource),'[]'::jsonb) INTO NEW.resources
  FROM jsonb_array_elements_text(COALESCE(NEW.resources,'[]'::jsonb)) resource
  WHERE resource NOT LIKE 'workstream:%';
  IF NEW.stage='coordinate' AND NOT NEW.resources ? ('coordination:'||NEW.goal_id) THEN
   NEW.resources := NEW.resources || jsonb_build_array('coordination:'||NEW.goal_id);
  END IF;
 END IF;
 RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS z_agentos_workforce_resources ON tasks;
CREATE TRIGGER z_agentos_workforce_resources BEFORE INSERT OR UPDATE OF execution,resources,goal_id,stage ON tasks
 FOR EACH ROW EXECUTE FUNCTION agentos_workforce_resources();

CREATE OR REPLACE FUNCTION agentos_workforce_admission() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE target_goal uuid; target_stage text; target_model text; target_policy jsonb; target_kind text; global_policy jsonb;
BEGIN
 IF NEW.state NOT IN ('queued','running','cancelling','reconciling') THEN RETURN NEW; END IF;
 SELECT t.goal_id,t.stage,COALESCE(NEW.model,t.model,a.model),s.value,t.execution->>'kind'
 INTO target_goal,target_stage,target_model,target_policy,target_kind
 FROM tasks t LEFT JOIN accounts a ON a.id=NEW.account_id
 LEFT JOIN settings s ON s.id='workforce:'||t.goal_id WHERE t.id=NEW.task_id;
 SELECT value INTO global_policy FROM settings WHERE id='model-policy';
 IF global_policy->>'version'='2' AND NOT COALESCE(${exclusiveModelPolicySql('global_policy')},false) THEN
   RAISE EXCEPTION 'MODEL_POLICY_INVALID';
 END IF;
 IF (${exclusiveModelPolicySql('global_policy')}) AND
    (TG_OP='INSERT' OR OLD.state NOT IN('queued','running','cancelling','reconciling') OR
     OLD.account_id IS DISTINCT FROM NEW.account_id OR OLD.model IS DISTINCT FROM target_model) AND
    (NEW.account_id IS DISTINCT FROM global_policy->>'accountId' OR target_model IS DISTINCT FROM global_policy->>'model') THEN
   RAISE EXCEPTION 'MODEL_POLICY_GLM_ONLY';
 END IF;
 IF target_policy->>'enabled' IS DISTINCT FROM 'true' THEN RETURN NEW; END IF;
 NEW.model := target_model;
 IF target_model ~* '^muse-' AND target_policy ? 'museTarget' THEN
  PERFORM pg_advisory_xact_lock(4280001);
  IF (SELECT count(*) FROM runs r JOIN tasks t ON t.id=r.task_id
   WHERE t.goal_id=target_goal AND r.id<>NEW.id AND r.state IN ('queued','running','cancelling','reconciling')
   AND r.model ~* '^muse-') >= GREATEST(0,(target_policy->>'museTarget')::int) THEN
   RAISE EXCEPTION 'WORKFORCE_MUSE_CAPACITY: Muse session target is full';
  END IF;
 END IF;
 IF target_model ~* '^gpt-[0-9.]+-luna($|[-:])' AND target_policy ? 'lunaTarget' THEN
  IF NOT COALESCE(target_policy->'lunaAccounts' ? NEW.account_id,false) THEN
   RAISE EXCEPTION 'WORKFORCE_LUNA_ACCOUNT: account is not authorized for Luna workers';
  END IF;
  PERFORM pg_advisory_xact_lock(4280001);
  IF (SELECT count(*) FROM runs r JOIN tasks t ON t.id=r.task_id LEFT JOIN accounts a ON a.id=r.account_id
   WHERE t.goal_id=target_goal AND r.id<>NEW.id
    AND r.state IN ('queued','running','cancelling','reconciling')
    AND COALESCE(r.model,t.model,a.model) ~* '^gpt-[0-9.]+-luna($|[-:])') >= GREATEST(0,COALESCE((target_policy->>'lunaTarget')::int,0)) THEN
   RAISE EXCEPTION 'WORKFORCE_LUNA_CAPACITY: Luna worker target is full';
  END IF;
 END IF;
 IF target_kind='opencode' OR NEW.runtime='opencode' OR target_model ~* '^glm[-.]'
    OR (NEW.account_id=global_policy->>'accountId' AND ${exclusiveModelPolicySql('global_policy')}) THEN
  PERFORM pg_advisory_xact_lock(4280001);
  IF NEW.state IN('queued','running')
   AND (TG_OP='INSERT' OR OLD.state NOT IN('queued','running','cancelling','reconciling'))
   AND EXISTS(SELECT 1 FROM dependencies d JOIN tasks p ON p.id=d.depends_on
    WHERE d.task_id=NEW.task_id AND p.state<>'accepted') THEN
   RAISE EXCEPTION 'DEPENDENCIES_PENDING';
  END IF;
  IF (SELECT count(*) FROM runs r JOIN tasks t ON t.id=r.task_id
   WHERE t.goal_id=target_goal AND r.id<>NEW.id
    AND r.state IN ('queued','running','cancelling','reconciling')
    AND (t.execution->>'kind'='opencode' OR r.runtime='opencode' OR COALESCE(r.model,t.model) ~* '^glm[-.]'
      OR r.account_id=global_policy->>'accountId')) >= GREATEST(1,COALESCE((target_policy->>'glmTarget')::int,6)) THEN
   RAISE EXCEPTION 'WORKFORCE_GLM_CAPACITY: native worker target is full';
  END IF;
 END IF;
 IF target_stage='coordinate' THEN
  PERFORM pg_advisory_xact_lock(4280001);
  IF EXISTS(SELECT 1 FROM runs r JOIN tasks t ON t.id=r.task_id
   WHERE t.goal_id=target_goal AND t.stage='coordinate' AND r.id<>NEW.id
   AND r.state IN('queued','running','cancelling','reconciling')) THEN
   RAISE EXCEPTION 'WORKFORCE_COORDINATOR_BUSY: only one active coordinator per goal';
  END IF;
 END IF;
 IF target_model ~* '^gpt-[0-9.]+-sol($|[-:])' THEN
  IF target_stage IS DISTINCT FROM 'coordinate' THEN
   RAISE EXCEPTION 'WORKFORCE_SOL_RESERVED: Sol is reserved for coordination';
  END IF;
  PERFORM pg_advisory_xact_lock(4280001);
  IF EXISTS(SELECT 1 FROM runs r JOIN tasks t ON t.id=r.task_id LEFT JOIN accounts a ON a.id=r.account_id
   WHERE t.goal_id=target_goal AND r.id<>NEW.id
    AND r.state IN ('queued','running','cancelling','reconciling')
    AND COALESCE(r.model,t.model,a.model) ~* '^gpt-[0-9.]+-sol($|[-:])') THEN
   RAISE EXCEPTION 'WORKFORCE_COORDINATOR_BUSY: only one active Sol coordinator per goal';
  END IF;
 END IF;
 RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS agentos_workforce_admission ON runs;
CREATE TRIGGER agentos_workforce_admission BEFORE INSERT OR UPDATE OF state,model,account_id ON runs
 FOR EACH ROW EXECUTE FUNCTION agentos_workforce_admission();

CREATE OR REPLACE FUNCTION agentos_workforce_native_dispatch() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF EXISTS(SELECT 1 FROM runs r JOIN tasks t ON t.id=r.task_id
  JOIN settings s ON s.id='workforce:'||t.goal_id
  WHERE r.id=NEW.run_id AND s.value->>'enabled'='true' AND
    (t.execution->>'kind'='opencode' OR (t.execution->>'kind'='harness'
      AND s.value->>'detachedHarnessEnabled'='true'))) THEN
  UPDATE runs r SET runtime=CASE WHEN t.execution->>'kind'='opencode' THEN 'opencode' ELSE r.runtime END
    FROM tasks t WHERE r.id=NEW.run_id AND t.id=r.task_id;
  INSERT INTO opencode_jobs(run_id) VALUES(NEW.run_id) ON CONFLICT(run_id) DO NOTHING;
  RETURN NULL;
 END IF;
 RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS agentos_workforce_native_dispatch ON outbox;
CREATE TRIGGER agentos_workforce_native_dispatch BEFORE INSERT ON outbox
 FOR EACH ROW EXECUTE FUNCTION agentos_workforce_native_dispatch();

CREATE OR REPLACE FUNCTION agentos_workstream_admission() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE project_name text; base text; target_goal uuid;
BEGIN
 IF NEW.state NOT IN ('queued','running','cancelling','reconciling') THEN RETURN NEW; END IF;
 IF TG_OP='UPDATE' AND OLD.state IN ('queued','running','cancelling','reconciling') THEN RETURN NEW; END IF;
 SELECT g.project,COALESCE(t.execution->>'baseWorkspace',t.execution->>'workspace'),t.goal_id
 INTO project_name,base,target_goal FROM tasks t JOIN goals g ON g.id=t.goal_id WHERE t.id=NEW.task_id;
 IF EXISTS(SELECT 1 FROM settings WHERE id='workforce:'||target_goal AND value->>'enabled'='true') THEN RETURN NEW; END IF;
 IF base IS NOT NULL AND EXISTS(SELECT 1 FROM settings WHERE id='exclusive-workstreams:'||project_name AND value->>'enabled'='true') THEN
  PERFORM pg_advisory_xact_lock(4280001);
  IF EXISTS(SELECT 1 FROM runs r JOIN tasks t ON t.id=r.task_id JOIN goals g ON g.id=t.goal_id
   WHERE r.id<>NEW.id AND r.state IN ('queued','running','cancelling','reconciling') AND g.project=project_name
    AND COALESCE(t.execution->>'baseWorkspace',t.execution->>'workspace')=base) THEN
   RAISE EXCEPTION 'WORKSTREAM_BUSY: %',base;
  END IF;
 END IF;
 RETURN NEW;
END $$;
`);
}
