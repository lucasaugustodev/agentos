import { z } from 'zod';
import { DomainError, criterionSchema, validateDag } from '../contracts.js';
import { checkMemoryContent } from '../memory.js';

const artifactName = z.string().regex(/^[\w][\w.-]{0,150}$/);
export const dependencyEvidenceSchema = z.object({
  version: z.literal(1),
  sourceStep: z.string().regex(/^[a-z0-9_-]{1,60}$/),
  bindings: z.array(z.object({
    reportArtifact: artifactName,
    jsonPointer: z.string().min(2).max(500).regex(/^(?:\/(?:[^~\/]|~[01])*)+$/),
    dependencyArtifact: artifactName,
  }).strict()).min(1).max(30),
}).strict().superRefine((value, ctx) => {
  const seen = new Set<string>();
  for (const binding of value.bindings) {
    const key = JSON.stringify([binding.reportArtifact, binding.jsonPointer]);
    if (seen.has(key)) ctx.addIssue({ code: 'custom', message: 'Duplicate dependency evidence binding.' });
    seen.add(key);
  }
});

export const stepSchema = z
  .object({
    key: z.string().regex(/^[a-z0-9_-]{1,60}$/),
    title: z.string().trim().min(3).max(150),
    kind: z.enum(['agent', 'action', 'wait', 'review']),
    instructions: z.string().max(20000).default(''),
    dependsOn: z.array(z.string()).max(30).default([]),
    accountId: z.string().optional(),
    model: z.string().max(150).optional(),
    workspace: z
      .string()
      .regex(/^[a-z0-9_-]+\/[a-z0-9_-]+$/)
      .optional(),
    readOnly: z.boolean().default(false),
    sourceStep: z.string().optional(),
    repairStep: z.string().optional(),
    dependencyEvidence: dependencyEvidenceSchema.optional(),
    action: z
      .string()
      .regex(/^[a-z0-9_.-]{1,100}$/)
      .optional(),
    parameters: z.record(z.string(), z.unknown()).default({}),
    seconds: z
      .number()
      .int()
      .min(1)
      .max(86400 * 7)
      .default(60),
    maxAttempts: z.number().int().min(1).max(8).default(3),
    criteria: z.array(criterionSchema).max(30).default([]),
  })
  .superRefine((s, ctx) => {
    if (s.kind === 'agent' && (!s.workspace || !s.criteria.length))
      ctx.addIssue({
        code: 'custom',
        message: 'Etapa de agente exige workspace e critérios de entrega.',
      });
    if (s.kind === 'action' && !s.action)
      ctx.addIssue({ code: 'custom', message: 'Selecione uma ação registrada.' });
    if (s.dependencyEvidence && (s.kind !== 'agent' || s.repairStep ||
        !s.dependsOn.includes(s.dependencyEvidence.sourceStep)))
      ctx.addIssue({ code: 'custom', message: 'Dependency evidence requires a direct agent dependency and consumer-only retries.' });
    if (s.parameters.nativeQAContractVersion !== undefined && s.parameters.nativeQAContractVersion !== 1 &&
        (s.parameters.nativeQAContractVersion !== 2 || !s.dependencyEvidence || s.maxAttempts > 3))
      ctx.addIssue({ code: 'custom', message: 'NativeQA v2 requires dependency evidence and at most three QA attempts.' });
  });
export const cycleSchema = z
  .object({
    name: z.string().trim().min(3).max(150),
    description: z.string().max(5000).default(''),
    project: z.string().regex(/^[a-z0-9_-]{1,80}$/),
    trigger: z.object({
      kind: z.enum(['manual', 'interval', 'bitrix']),
      seconds: z
        .number()
        .int()
        .min(30)
        .max(86400 * 30)
        .default(60),
      groupId: z.number().int().positive().optional(),
      stageId: z.number().int().nonnegative().optional(),
      includeExisting: z.boolean().default(false),
    }),
    concurrency: z.number().int().min(1).max(10).default(1),
    steps: z.array(stepSchema).min(1).max(30),
    allowedActions: z.array(z.string()).max(30).default([]),
    allowedAccounts: z.array(z.string()).max(30).default([]),
  })
  .superRefine((c, ctx) => {
    if (c.trigger.kind === 'bitrix' && (!c.trigger.groupId || c.trigger.stageId === undefined))
      ctx.addIssue({ code: 'custom', message: 'Informe grupo e etapa do Bitrix.' });
    for (const s of c.steps) {
      if (s.dependencyEvidence) {
        const source = c.steps.find(p => p.key === s.dependencyEvidence!.sourceStep);
        if (!source || source.kind !== 'agent' ||
            (s.parameters.nativeQAContractVersion === 2 && source.maxAttempts !== 1))
          ctx.addIssue({ code: 'custom', message: 'NativeQA dependency must be an agent with one producer attempt.' });
      }
      if (
        s.repairStep &&
        (!s.dependsOn.includes(s.repairStep) ||
          !c.steps.some((p) => p.key === s.repairStep && p.kind === 'agent'))
      )
        ctx.addIssue({
          code: 'custom',
          message: 'A correção deve apontar para uma etapa de agente diretamente anterior.',
        });
      if (s.sourceStep && !s.dependsOn.includes(s.sourceStep))
        ctx.addIssue({
          code: 'custom',
          message: 'A origem do workspace deve ser uma dependência direta.',
        });
      if (s.workspace && s.workspace.split('/')[0] !== c.project)
        ctx.addIssue({ code: 'custom', message: 'Workspace fora do projeto do ciclo.' });
      if (s.action && !c.allowedActions.includes(s.action))
        ctx.addIssue({ code: 'custom', message: 'Ação fora do escopo autorizado: ' + s.action });
      if (s.accountId && c.allowedAccounts.length && !c.allowedAccounts.includes(s.accountId))
        ctx.addIssue({ code: 'custom', message: 'Conta fora do escopo autorizado.' });
    }
  });
export type CycleDefinition = z.infer<typeof cycleSchema>;
export type CycleStep = z.infer<typeof stepSchema>;
export function parseCycle(input: unknown): CycleDefinition {
  const value = cycleSchema.parse(input);
  validateDag(value.steps.map((s) => ({ key: s.key, dependencies: s.dependsOn })) as any);
  checkMemoryContent(JSON.stringify(value));
  return value;
}
export function conflict(
  message = 'Esta versão mudou. Atualize antes de aplicar sua alteração.',
): never {
  throw new DomainError('VERSION_CONFLICT', message);
}
export const liveCycleStates = [
  'queued',
  'running',
  'waiting',
  'recovering',
  'attention',
  'cancelling',
  'reconciling',
];
