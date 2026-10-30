import { z } from 'zod';
import {requirementOutcomesCriterionSchema} from './requirement-outcomes.js';
export const criterionSchema = z.discriminatedUnion('type', [
  z.object({
    id: z.string().min(1),
    label: z.string().min(1),
    type: z.literal('artifact'),
    name: z.string().min(1),
  }),
  z.object({
    id: z.string().min(1),
    label: z.string().min(1),
    type: z.literal('json_field'),
    name: z.string().min(1),
    field: z.string().min(1),
    equals: z.union([z.string(), z.number(), z.boolean()]),
  }),
  z.object({ id: z.string().min(1), label: z.string().min(1), type: z.literal('review') }),
]);
// Internal repair-delivery gate. Other consumers (notably cycles) keep their
// existing criterion union until they implement this deterministic validator.
export const repairSourceProofCriterionSchema = z.object({
  id: z.string().min(1), label: z.string().min(1), type: z.literal('repair_source_proof'),
  version: z.literal(1), name: z.literal('repair-report.json'), patch: z.literal('implementation.patch'),
});
export const taskCriterionSchema = z.discriminatedUnion('type', [
  ...criterionSchema.options, repairSourceProofCriterionSchema, requirementOutcomesCriterionSchema,
]);
export const taskSchema = z.object({
  key: z.string().regex(/^[a-z0-9_-]{1,80}$/),
  title: z.string().min(3).max(200),
  description: z.string().max(20000).default(''),
  agentId: z.string().min(1),
  accountId: z.string().nullable().optional(),
  model: z.string().min(1).max(150).optional(),
  recovery: z
    .object({
      mode: z.enum(['automatic', 'manual']),
      repairTaskKey: z
        .string()
        .regex(/^[a-z0-9_-]{1,80}$/)
        .optional(),
    })
    .optional(),
  execution: z
    .object({
      kind: z.literal('harness'),
      workspace: z.string().regex(/^[a-z0-9_-]+\/[a-z0-9_-]+$/),
    })
    .optional(),
  dependencies: z.array(z.string()).max(100).default([]),
  resources: z.array(z.string().min(1).max(100)).max(20).default([]),
  criteria: z.array(taskCriterionSchema).min(1).max(30),
  requiredMemories: z.array(z.uuid()).max(30).default([]),
  maxAttempts: z.number().int().min(1).max(10).default(3),
  priority: z.number().int().min(-100).max(100).default(0),
});
export const goalSchema = z.object({
  title: z.string().min(3).max(200),
  description: z.string().max(20000).default(''),
  project: z.string().regex(/^[a-z0-9_-]{1,80}$/),
  idempotencyKey: z.string().min(8).max(160),
  tasks: z.array(taskSchema).min(1).max(250),
});
export type GoalInput = z.infer<typeof goalSchema>;
export type Criterion = z.infer<typeof taskCriterionSchema>;
export class DomainError extends Error {
  constructor(
    public code: string,
    message: string,
    public status = 409,
  ) {
    super(message);
  }
}
export function validateDag(tasks: GoalInput['tasks']) {
  const byKey = new Map(tasks.map((t) => [t.key, t]));
  if (byKey.size !== tasks.length)
    throw new DomainError('DUPLICATE_TASK', 'As chaves das tarefas precisam ser únicas.', 400);
  const visiting = new Set<string>(),
    done = new Set<string>();
  function visit(key: string) {
    if (done.has(key)) return;
    if (visiting.has(key))
      throw new DomainError('CYCLIC_DAG', 'O plano contém dependências circulares.', 400);
    const task = byKey.get(key);
    if (!task) throw new DomainError('MISSING_DEPENDENCY', 'Dependência inexistente: ' + key, 400);
    visiting.add(key);
    for (const dep of task.dependencies) visit(dep);
    visiting.delete(key);
    done.add(key);
  }
  for (const key of byKey.keys()) visit(key);
  for (const task of tasks) {
    const target = task.recovery?.repairTaskKey;
    if (!target || target === task.key) continue;
    const ancestors = new Set<string>();
    function collect(key: string) {
      for (const dep of byKey.get(key)!.dependencies) {
        if (!ancestors.has(dep)) {
          ancestors.add(dep);
          collect(dep);
        }
      }
    }
    collect(task.key);
    if (
      !ancestors.has(target) ||
      byKey.get(target)?.execution?.workspace !== task.execution?.workspace
    )
      throw new DomainError(
        'RECOVERY_TARGET_INVALID',
        'A correção deve apontar para uma etapa anterior no mesmo workspace.',
        400,
      );
  }
}
