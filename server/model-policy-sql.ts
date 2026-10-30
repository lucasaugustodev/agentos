/** Closed policy shapes used by SQL authority. Values remain operator-owned. */
export function exclusiveModelPolicySql(value: string) {
  return `((${value}->>'version'='1' AND ${value}->>'mode'='glm-only'
      AND ${value}->>'model'='glm-5.3-flash') OR
    (${value}->>'version'='2' AND ${value}->>'mode'='exclusive-model'
      AND ${value}->>'model'='deepseek-v4-pro-0813'
      AND ${value}->>'accountId' ~ '^agentos-api-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      AND ${value}->>'bridgeId'='agentos:api:'||substring(${value}->>'accountId' FROM 13)
      AND ${value}->>'requestLimit'='6' AND ${value}->>'sessionLimit'='12'
      AND (${value} - ARRAY['version','mode','accountId','bridgeId','model','requestLimit','sessionLimit'])='{}'::jsonb))`;
}
