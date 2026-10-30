/** Role authority is bound to the configured coordinator, not a model family.
 * Historical Codex authority remains valid only in the legacy profile. */
import {exclusiveModelPolicySql} from './model-policy-sql.js';
export function managerAuthoritySql(accountAlias: 'a'|'ac'='a') {
  return `((r.model ~ '^gpt-[0-9.]+-sol$' AND ${accountAlias}.provider='codex'
    AND NOT EXISTS(SELECT 1 FROM settings policy WHERE policy.id='model-policy'))
    OR EXISTS(SELECT 1 FROM settings policy JOIN settings workforce
      ON workforce.id='workforce:'||t.goal_id
      WHERE policy.id='model-policy' AND ${exclusiveModelPolicySql('policy.value')}
      AND policy.value->>'accountId'=r.account_id
      AND policy.value->>'model'=r.model
      AND policy.value->>'bridgeId'=${accountAlias}.bridge_id
      AND workforce.value->>'enabled'='true'
      AND workforce.value->>'coordinatorAccountId'=r.account_id
      AND workforce.value->>'coordinatorModel'=r.model))`;
}
