import { z } from 'zod';
import { taskSchema, criterionSchema, DomainError } from './contracts.js';
import {isAttemptLocalWriteScope} from './attempt-local-write-scope.js';
const artifactName = z.string().regex(/^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,100}$/);
const planCriterion = z.discriminatedUnion('type', [
  criterionSchema.options[0].extend({ name: artifactName }),
  criterionSchema.options[1].extend({ name: artifactName }),
]);
export const workspaceId = z
  .string()
  .regex(/^[a-z0-9_-]+\/[a-z0-9_-]+$/)
  .max(160);
export const objectiveSchema = z
  .object({
    message: z.string().trim().min(5).max(30000),
    // A long prompt makes a useless name. Callers that have one send it; the
    // fallback stays the opening of the message, as before.
    title: z.string().trim().min(3).max(120).optional(),
    project: z.string().regex(/^[a-z0-9_-]{1,80}$/),
    mode: z.enum(['plan', 'execute']),
    workspaces: z.array(workspaceId).min(1).max(10),
    constraints: z.array(z.string().min(1).max(2000)).max(30).default([]),
    requirements: z.array(z.string().min(1).max(2000)).max(30).default([]),
    requestedCapacity: z.number().int().min(1).max(32).default(2),
    plannerAccountId: z.string().optional(),
    plannerModel: z.string().max(150).optional(),
    allowedAccounts: z.array(z.string()).max(30).default([]),
    idempotencyKey: z.string().min(8).max(160),
    chatId: z.uuid().optional(),
    planningStrategy: z.enum(['complete', 'incremental']).optional(),
  })
  .refine((r) => r.workspaces.every((w) => w.split('/')[0] === r.project), {
    message: 'Os workspaces devem pertencer ao projeto do objetivo.',
    path: ['workspaces'],
  });
export type ObjectiveInput = z.infer<typeof objectiveSchema>;
export const proposedTaskSchema = taskSchema
  .pick({
    key: true,
    title: true,
    description: true,
    agentId: true,
    dependencies: true,
    criteria: true,
  })
  .extend({
    criteria: z.array(planCriterion).min(1).max(30),
    baseWorkspace: workspaceId,
    stage: z.enum(['work', 'integrate', 'verify']),
    covers: z.array(z.string()).min(1).max(30),
    writePaths: z.array(z.string().max(200)).max(40).default([]),
    browser: z.boolean().default(false),
    accountId: z.string().optional(),
    model: z.string().max(150).optional(),
    sourceTask: z.string().optional(),
    parentKey: z.string().optional(),
    agentRole: z.enum(['coordinator', 'specialist', 'worker', 'validator']).optional(),
  });
export const planSchema = z.object({
  title: z.string().min(3).max(200),
  summary: z.string().min(5).max(5000),
  requirements: z
    .array(
      z.object({
        id: z.string().regex(/^[a-z0-9_-]{1,50}$/),
        description: z.string().min(5).max(2000),
      }),
    )
    .min(1)
    .max(30),
  assumptions: z.array(z.string().max(2000)).max(20).default([]),
  approachChange: z.string().max(5000).default(''),
  tasks: z.array(proposedTaskSchema).min(1).max(80),
  incremental: z.object({
    version: z.literal(1),
    selectedRequirementId: z.string(),
    selectedRequirementIds: z.array(z.string()).min(1).max(6).optional(),
    completedRequirementIds: z.array(z.string()).max(30),
    pendingRequirementIds: z.array(z.string()).max(30),
  }).strict().optional(),
});
export type ProposedPlan = z.infer<typeof planSchema>;

export function validatePlan(
  plan: ProposedPlan,
  request: ObjectiveInput,
  agents: any[],
  accounts: any[],
  previousRequirements?: any[],
) {
  const byKey = new Map(plan.tasks.map((t) => [t.key, t]));
  const fail = (message: string, detail?: Record<string, unknown>): never => {
    throw Object.assign(new DomainError('PLAN_INVALID', message, 400), { detail });
  };
  if (byKey.size !== plan.tasks.length) fail('Chaves de tarefas duplicadas.');
  if (new Set(plan.requirements.map((r) => r.id)).size !== plan.requirements.length)
    fail('Requisitos duplicados.');
  if (
    previousRequirements &&
    JSON.stringify(plan.requirements) !== JSON.stringify(previousRequirements)
  )
    fail('A revisão precisa preservar os requisitos do objetivo.');
  const visiting = new Set<string>(),
    cached = new Map<string, Set<string>>();
  function ancestors(key: string): Set<string> {
    if (cached.has(key)) return cached.get(key)!;
    if (visiting.has(key)) fail('Dependências circulares.');
    const t = byKey.get(key);
    if (!t) fail('Dependência inexistente: ' + key);
    visiting.add(key);
    const all = new Set<string>();
    for (const d of t!.dependencies) {
      all.add(d);
      for (const a of ancestors(d)) all.add(a);
    }
    visiting.delete(key);
    cached.set(key, all);
    return all;
  }
  const ancestry = new Map(plan.tasks.map((t) => [t.key, ancestors(t.key)]));
  // Delegation is distinct from dependency order, but must also be a forest.
  for (const task of plan.tasks) {
    const seen = new Set([task.key]);
    let parent = task.parentKey;
    while (parent) {
      if (seen.has(parent)) fail('Hierarquia de delegação circular.');
      seen.add(parent);
      const node = byKey.get(parent);
      if (!node) fail('Pai de delegação inexistente: '+parent);
      parent = node!.parentKey;
    }
  }
  for (const t of plan.tasks) {
    if (/^base-\d+$/.test(t.key)) fail('Chave de tarefa reservada para o snapshot da base.');
    if (!request.workspaces.includes(t.baseWorkspace))
      fail('Workspace fora do escopo: ' + t.baseWorkspace);
    if (!agents.some((a) => a.id === t.agentId && a.runtime === 'harness'))
      fail('Especialização indisponível: ' + t.agentId);
    if (
      t.accountId &&
      (!accounts.some(
        (a) => a.id === t.accountId && a.enabled && a.bridge_id && a.provider !== 'agy',
      ) ||
        (request.allowedAccounts.length && !request.allowedAccounts.includes(t.accountId)))
    )
      fail('Conta fora das rotas autorizadas.');
    if (
      t.model &&
      !accounts.some((a) => (!t.accountId || a.id === t.accountId) && a.models.includes(t.model))
    )
      fail('Modelo indisponível.');
    if (t.covers.some((id) => !plan.requirements.some((r) => r.id === id)))
      fail('Requisito desconhecido.');
    if (
      t.sourceTask &&
      (!ancestry.get(t.key)!.has(t.sourceTask) ||
        byKey.get(t.sourceTask)?.baseWorkspace !== t.baseWorkspace)
    )
      fail('A origem da candidata precisa ser uma dependência do mesmo workspace.');
    const refusedPath = t.writePaths.find(
      (p) =>
        p.startsWith('/') ||
        p.includes('..') ||
        p.includes('\\') ||
        p.includes('//') || /(^|\/)\.(\/|$)/.test(p) ||
        /(^|\/)(\.git|node_modules|assets)(\/|$)/.test(p),
    );
    if (refusedPath !== undefined)
      fail('Escopo de escrita inválido.', { task: t.key, writePath: refusedPath });
    if (t.stage === 'integrate' && t.sourceTask)
      fail('Integração deve partir da base comum e aplicar os patches.');
    if (
      t.stage === 'verify' &&
      (!t.sourceTask || !t.criteria.some((c) => c.type === 'json_field' && c.equals === true))
    )
      fail('Validação exige candidata e resultado estruturado.');
  }
  const finalValidators = new Set<string>();
  for (const workspace of request.workspaces) {
    const tasks = plan.tasks.filter((t) => t.baseWorkspace === workspace);
    if (!tasks.length) continue;
    const workers = tasks.filter((t) => t.stage === 'work');
    const integrations = tasks.filter((t) => t.stage === 'integrate');
    const validators = tasks.filter((t) => t.stage === 'verify');
    if (
      workers.length > 1 &&
      !integrations.some((i) => workers.every((w) => ancestry.get(i.key)!.has(w.key)))
    )
      fail('Frentes paralelas precisam de integração comum.', {
        workspace,
        workers: workers.map((w) => w.key),
        integrations: integrations.map((i) => ({
          key: i.key,
          cobre: workers.filter((w) => ancestry.get(i.key)!.has(w.key)).map((w) => w.key),
        })),
      });
    const finals = validators.filter((v) =>
      tasks
        .filter((t) => t.stage !== 'verify')
        .every(
          (t) =>
            ancestry.get(v.key)!.has(t.key) &&
            (v.sourceTask === t.key || ancestry.get(v.sourceTask!)?.has(t.key)),
        ),
    );
    for (const v of finals) finalValidators.add(v.key);
    if (!finals.length)
      fail('Falta validação final da candidata integrada.', {
        workspace,
        aVerificar: tasks.filter((t) => t.stage !== 'verify').map((t) => t.key),
        validadores: validators.map((v) => ({
          key: v.key,
          sourceTask: v.sourceTask ?? null,
          alcanca: tasks
            .filter((t) => t.stage !== 'verify' && ancestry.get(v.key)!.has(t.key))
            .map((t) => t.key),
        })),
      });
  }
  const incremental = plan.incremental;
  if (incremental) {
    const selectedIds = incremental.selectedRequirementIds ?? [incremental.selectedRequirementId];
    const partition = [...incremental.completedRequirementIds, ...selectedIds, ...incremental.pendingRequirementIds];
    if (request.mode !== 'execute' || request.workspaces.length !== 1 ||
        selectedIds[0] !== incremental.selectedRequirementId ||
        partition.length !== plan.requirements.length || new Set(partition).size !== partition.length ||
        partition.some(id => !plan.requirements.some(r => r.id === id)))
      fail('Partição incremental inválida ou escopo não suportado.');
    if (plan.tasks.some(t => t.stage !== 'verify' && t.covers.some(id => !selectedIds.includes(id))))
      fail('O lote deve implementar somente seu requisito selecionado.');
    const work = plan.tasks.filter(t => t.stage === 'work');
    if(selectedIds.some(id=>!work.some(w=>w.covers.includes(id))))
      fail('Requisito selecionado sem subtarefa de implementação.',{violation:'MISSING_REQUIREMENT_WORK'});
    if (work.length > 1) {
      const seenPaths: { key: string; path: string }[] = [];
      for (const w of work) {
        if(w.covers.length!==1)
          fail('Cada subtarefa deve identificar seu requisito.',{violation:'TASK_REQUIREMENT_BINDING',task:w.key});
        if(w.dependencies.length&&!w.sourceTask)
          fail('Subtarefa dependente precisa da candidata produzida por suas dependências.',{violation:'DEPENDENCY_SOURCE_REQUIRED',task:w.key});
        if (!w.writePaths.length)
          fail('Entregas paralelas no mesmo lote exigem writePaths explícitos.', { task: w.key });
        for (const rawPath of w.writePaths) {
          if(isAttemptLocalWriteScope(rawPath))continue;
          const norm = rawPath.replace(/\/+$/, '');
          const conflict = seenPaths.find(
            other => other.key !== w.key && (other.path === norm || other.path.startsWith(norm + '/') || norm.startsWith(other.path + '/')),
          );
          if (conflict)
            fail('Entregas paralelas no mesmo lote exigem writePaths disjuntos.', { task: w.key, conflictTask: conflict.key, writePath: rawPath });
          seenPaths.push({ key: w.key, path: norm });
        }
      }
    }
  }
  const selectedIds = incremental ? (incremental.selectedRequirementIds ?? [incremental.selectedRequirementId]) : [];
  const requiredNow = incremental?.pendingRequirementIds.length
    ? plan.requirements.filter(r => selectedIds.includes(r.id))
    : plan.requirements;
  for (const req of requiredNow)
    if (!plan.tasks.some((t) => finalValidators.has(t.key) && t.covers.includes(req.id)))
      fail('Requisito sem validação final: ' + req.id);
}
