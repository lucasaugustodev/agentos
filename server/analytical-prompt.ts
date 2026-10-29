export const analyticalPromptCharacterLimit = 180000;

export function promptCheckpoint(checkpoint: any) {
  if (checkpoint?.kind !== 'server_migration') return checkpoint ?? null;
  const { proof, ...rest } = checkpoint;
  return {
    ...rest,
    proof: proof ? {
      runId: proof.runId, workspace: proof.workspace, checkedAt: proof.checkedAt,
      process: proof.process, candidateDigest: proof.candidate?.candidateDigest,
      baseDigest: proof.candidate?.baseDigest,
    } : undefined,
  };
}

/** Shared by assembly and execution: JSON escaping is part of the budget. */
export function analyticalPrompt(run: any, task: any, context: {content: string; manifest: any}, dependencies: any[]) {
  const payload: any = {
    role: task.agent_id,
    task: task.title,
    description: task.description,
    instructionManifest: context.manifest.taskInstructions ?? null,
    executionInputs: context.manifest.executionInputs ?? [],
    criteria: task.criteria,
    checkpoint: promptCheckpoint(run.checkpoint),
    context: context.content,
    dependencies,
  };
  const agentRole = task.agent_role ?? run?.agent_role ?? null;
  const agentLevel = task.agent_level ?? run?.agent_level ?? null;
  if (agentRole !== null) payload.agentRole = agentRole;
  if (agentLevel !== null) payload.level = agentLevel;
  if (task.execution?.writePaths?.length) payload.writePaths = task.execution.writePaths;
  return JSON.stringify(payload);
}

/** Only optional memory can be excluded. All non-memory inputs are immutable. */
export function fitManagerOptionalMemory<T extends {id: string; required: boolean; content: string}>(
  memories: T[], measure: (selected: T[]) => number,
) {
  const selected = memories.filter(m => m.required);
  const requiredPayloadCharacters = measure(selected);
  const omitted: Array<{id: string; characters: number; reason: string}> = [];
  for (const memory of memories.filter(m => !m.required)) {
    if (measure([...selected, memory]) <= analyticalPromptCharacterLimit) selected.push(memory);
    else omitted.push({id: memory.id, characters: memory.content.length, reason: 'serialized_transport_budget'});
  }
  return {selected, omitted, requiredPayloadCharacters, payloadCharacters: measure(selected),
    maximumCharacters: analyticalPromptCharacterLimit,
    requiredFits: requiredPayloadCharacters <= analyticalPromptCharacterLimit};
}
