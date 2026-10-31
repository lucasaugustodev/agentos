import { z } from 'zod';
import { randomUUID } from 'node:crypto';
import { isDeepStrictEqual } from 'node:util';
import { DomainError } from './contracts.js';
import { event, type DB } from './db.js';
import { planSchema,bindRequirementOutcomeContracts, type ObjectiveInput, type ProposedPlan } from './objective-contracts.js';
import {requirementContractsSchema,readRequirementResolution,type RequirementResolution} from './requirement-outcomes.js';
import {isAttemptLocalWriteScope} from './attempt-local-write-scope.js';
import {sourceContractSchema,normalizeSourceContract,combineSourceContracts,composeSourceContracts,SourceCompositionScopeError,sourcePathWithin,type SourceContract} from './source-contract-schema.js';

// Semantic choices only. IDs, dependencies, runtime envelopes and receipt
// formats are controller-owned. Inspired by Ruflo's action/precondition split;
// no Ruflo runtime or second scheduler is installed.
const writePathItemSchema = z
  .string()
  .min(1)
  .max(200)
  .regex(/^(?![/\\])(?!.*\.\.)(?!.*\\)(?!\.git(?:\/|$))(?!node_modules(?:\/|$))(?!assets(?:\/|$))[a-zA-Z0-9._/-]+$/)
  .refine(p=>!p.includes('//')&&!p.split('/').includes('.'),'Use caminhos relativos sem aliases.');

const singleDeliveryFields = {
  // Optional only while parsing stored/legacy decisions. New model responses
  // use deliveryDecisionSchema, which requires the semantic kind explicitly.
  kind: z.enum(['delivery', 'validation']).optional(),
  validationTargetKey: z.string().regex(/^[a-z][a-z0-9_-]{0,39}$/).optional(),
  key: z.string().regex(/^[a-z][a-z0-9_-]{0,39}$/).optional(),
  dependsOn: z.array(z.string().regex(/^[a-z][a-z0-9_-]{0,39}$/)).max(6).optional(),
  requirementId: z.string(),
  title: z.string().min(3).max(120),
  action: z.string().min(25).max(1600),
  verification: z.string().min(25).max(1200),
  agentId: z.string(),
  writePaths: z.array(writePathItemSchema).max(20).optional(),
  sourceContract: sourceContractSchema.optional(),
};

export const nextDeliveryItemSchema = z.object(singleDeliveryFields).strict();

export const nextDeliverySchema = z.object({
  requirementContracts:requirementContractsSchema.optional(),
  kind: singleDeliveryFields.kind,
  validationTargetKey: singleDeliveryFields.validationTargetKey,
  key: singleDeliveryFields.key,
  dependsOn: singleDeliveryFields.dependsOn,
  requirementId: z.string().optional(),
  title: z.string().min(3).max(120).optional(),
  action: z.string().min(25).max(1600).optional(),
  verification: z.string().min(25).max(1200).optional(),
  agentId: z.string().optional(),
  writePaths: z.array(writePathItemSchema).max(20).optional(),
  sourceContract: singleDeliveryFields.sourceContract,
  deliveries: z.array(nextDeliveryItemSchema).min(1).max(6).optional(),
}).strict();

export function deliveryDecisionSchema(agents: {id:string;runtime:string}[],eligible?:string[],serial=false,sourceContractVersion?:1,completionVersion?:2) {
  const ids = [...new Set(agents.filter(a=>a.runtime==='harness'&&a.id!=='qa.integration').map(a=>a.id))];
  if (!ids.length) throw new DomainError('SCOPE_MISSING','Nenhum agente de execução disponível.');
  const agentEnum = z.enum(ids as [string,...string[]]);
  const requirementId=eligible?.length?z.enum(eligible as [string,...string[]]):z.string();
  const delivery=nextDeliveryItemSchema.extend({kind:z.literal('delivery'),
    validationTargetKey:z.never().optional(),agentId:agentEnum,requirementId,
    sourceContract:sourceContractVersion===1?sourceContractSchema:singleDeliveryFields.sourceContract});
  const contracts=completionVersion===2?{requirementContracts:requirementContractsSchema}:{};
  if(serial)return delivery.extend(contracts);
  const validation=nextDeliveryItemSchema.extend({kind:z.literal('validation'),
    validationTargetKey:singleDeliveryFields.validationTargetKey.unwrap(),
    agentId:z.literal('qa.integration'),requirementId,sourceContract:z.never().optional()});
  const item=z.discriminatedUnion('kind',[delivery,validation]);
  return z.union([delivery.extend(contracts),z.object({...contracts,deliveries:z.array(item).min(1).max(6)}).strict()]);
}

function invalid(violation:string,message:string,detail:Record<string,unknown>={}):never{
  throw Object.assign(new DomainError('PLAN_INVALID',message),{detail:{violation,...detail}});
}

export function selectedRequirementIds(incremental?: ProposedPlan['incremental']): string[] {
  if (!incremental) return [];
  return incremental.selectedRequirementIds?.length ? incremental.selectedRequirementIds : [incremental.selectedRequirementId];
}

export function incrementalEligible(request: ObjectiveInput, previous?: ProposedPlan, glmOnly = false) {
  return !!previous?.incremental || (request.planningStrategy !== 'complete' &&
    (request.planningStrategy === 'incremental' || glmOnly) && request.mode === 'execute' &&
    request.workspaces.length === 1 && request.requirements.length > 0 &&
    request.requirements.every(r => r.length >= 5) && !previous);
}

export function deliveryFrontier(request: ObjectiveInput, previous?: ProposedPlan, advance = false,resolution?:RequirementResolution) {
  const requirements = previous?.requirements ?? request.requirements.map((description, i) => ({id: `req-${i+1}`, description}));
  if(request.completionContractVersion===2||previous?.completionContractVersion===2){
    const completed=previous?.completionContractVersion===2?(resolution?.completedRequirementIds??[]):[];
    const blocked=advance&&previous?.completionContractVersion===2?(resolution?.blockedRequirementIds??[]):[];
    return {requirements,completed,eligible:requirements.map(r=>r.id).filter(id=>!completed.includes(id)&&!blocked.includes(id))};
  }
  const prior = previous?.incremental;
  const priorSelected = selectedRequirementIds(prior);
  const completed = prior ? [...prior.completedRequirementIds, ...(advance ? priorSelected : [])] : [];
  const eligible = prior ? (advance ? prior.pendingRequirementIds : priorSelected) : requirements.map(r => r.id);
  return { requirements, completed, eligible };
}

export function compileDelivery(raw: unknown, request: ObjectiveInput, previous?: ProposedPlan, advance = false, reports: string[] = [],resolution?:RequirementResolution) {
  const parsed = nextDeliverySchema.parse(raw);
  if(parsed.deliveries&&Object.keys(singleDeliveryFields).some(key=>(parsed as any)[key]!==undefined))
    invalid('AMBIGUOUS_DECISION','Use uma entrega única ou deliveries; não misture os dois formatos.');
  const {requirementContracts:requestedContracts,...single}=parsed;
  const items = parsed.deliveries?.length ? parsed.deliveries : [nextDeliveryItemSchema.parse(single)];
  const sourceContractVersion=request.sourceContractVersion??previous?.sourceContractVersion;
  if(!sourceContractVersion&&items.some(item=>item.sourceContract))
    invalid('SOURCE_CONTRACT_VERSION_REQUIRED','Um contrato de fonte exige planejamento com a versão explícita.');
  for(const item of items){
    if(item.kind==='validation'){
      if(item.sourceContract)invalid('VALIDATION_SOURCE_CONTRACT','O controlador herda o contrato da candidata alvo; o validador não o redefine.');
      continue;
    }
    if(sourceContractVersion===1){
      if(!item.sourceContract)invalid('SOURCE_CONTRACT_REQUIRED','Declare os arquivos de fonte obrigatórios ou uma justificativa de trabalho somente em evidências.',{key:item.key});
      item.sourceContract=normalizeSourceContract(item.sourceContract);
      for(const output of item.sourceContract.required)if(!item.writePaths?.some(scope=>sourcePathWithin(output.path,scope)))
        invalid('SOURCE_OUTPUT_OUTSIDE_WRITE_SCOPE','O produto de fonte precisa caber no escopo explícito, sem ampliar writePaths.',{key:item.key,path:output.path});
    }
  }
  const combined=(contracts:Array<SourceContract|undefined>)=>{
    if(sourceContractVersion!==1)return {};
    try{return {sourceContract:combineSourceContracts(contracts as SourceContract[])};}
    catch{invalid('SOURCE_CONTRACT_CONFLICT','As contribuições têm obrigações de fonte conflitantes.');}
  };
  const composed=(contributors:Array<{sourceContract?:SourceContract;writePaths?:string[]}>)=>{
    if(sourceContractVersion!==1)return {};
    try{return {sourceContract:composeSourceContracts(contributors.map(item=>({contract:item.sourceContract!,writePaths:item.writePaths??[]})))};}
    catch(error){
      if(error instanceof SourceCompositionScopeError)
        invalid(error.violation,error.violation==='WRITE_SCOPE_CONFLICT'
          ?'Subtarefas precisam de escopos de escrita disjuntos.':'Escopo de escrita inválido.',
          {writePath:error.writePath,contributorIndex:error.contributorIndex,conflictContributorIndex:error.conflictContributorIndex});
      invalid('SOURCE_CONTRACT_CONFLICT','As contribuições têm obrigações de fonte conflitantes.');
    }
  };
  const { requirements, completed, eligible } = deliveryFrontier(request, previous, advance,resolution);
  const finish=(plan:ProposedPlan)=>bindDeliveryFeedback(bindRequirementOutcomeContracts({...plan,
    ...(requestedContracts?{requirementContracts:requestedContracts}:{})},request,previous),reports);
  const selectedIds = [...new Set(items.filter(i=>i.kind!=='validation').map(i => i.requirementId))];
  if (items.some(i=>!eligible.includes(i.requirementId)))
    invalid('INELIGIBLE_REQUIREMENT','Selecione somente requisitos elegíveis.',{eligible,received:items.map(i=>i.requirementId)});
  if(items.some(i=>i.kind!=='validation'&&i.agentId==='qa.integration'))
    invalid('VALIDATOR_AS_WORKER','O QA independente é criado pelo controlador; selecione um implementador.');
  const keyed=items.map((item,index)=>({...item,kind:item.kind??'delivery',partKey:item.key??`part-${index+1}`,index}));
  const byKey=new Map(keyed.map(item=>[item.partKey,item]));
  if(byKey.size!==keyed.length)invalid('DUPLICATE_TASK_KEY','Cada subtarefa precisa de uma chave única.');
  for(const item of keyed){
    if(new Set(item.dependsOn??[]).size!==(item.dependsOn??[]).length)
      invalid('DUPLICATE_DEPENDENCY','Não repita uma dependência.',{key:item.partKey});
    if(item.kind==='delivery'){
      if(item.validationTargetKey)invalid('UNEXPECTED_VALIDATION_TARGET','Somente uma validação pode declarar candidata alvo.',{key:item.partKey});
      continue;
    }
    const target=item.validationTargetKey&&byKey.get(item.validationTargetKey);
    if(!target||target.kind!=='delivery')
      invalid('VALIDATION_TARGET_REQUIRED','A validação exige uma entrega produtora deste mesmo lote.',{key:item.partKey,target:item.validationTargetKey});
    if(item.requirementId!==target.requirementId)
      invalid('VALIDATION_REQUIREMENT_MISMATCH','A validação deve cobrir o requisito de sua candidata alvo.',{key:item.partKey,target:target.partKey});
    if(item.writePaths?.some(path=>!isAttemptLocalWriteScope(path)))
      invalid('VALIDATION_SOURCE_WRITE','A validação não pode escrever na fonte; use apenas evidence/ ou data/.',{key:item.partKey});
    // The model may omit the dependency but may never omit candidate identity.
    item.dependsOn=[...new Set([...(item.dependsOn??[]),target.partKey])];
  }
  const visiting=new Set<string>(),ancestors=new Map<string,Set<string>>(),ordered:typeof keyed=[];
  function visit(key:string):Set<string>{
    if(ancestors.has(key))return ancestors.get(key)!;
    if(visiting.has(key))invalid('DEPENDENCY_CYCLE','Dependências entre subtarefas não podem formar ciclos.',{key});
    const item=byKey.get(key);
    if(!item)invalid('UNKNOWN_DEPENDENCY','A dependência precisa nomear uma subtarefa deste lote.',{key});
    visiting.add(key);const all=new Set<string>();
    for(const dependency of item.dependsOn??[]){all.add(dependency);for(const parent of visit(dependency))all.add(parent);}
    visiting.delete(key);ancestors.set(key,all);ordered.push(item);return all;
  }
  for(const item of keyed)visit(item.partKey);
  const pending = requirements.map(r => r.id).filter(id => !selectedIds.includes(id) && !completed.includes(id));
  const covers = pending.length ? selectedIds : requirements.map(r => r.id);
  if (items.length === 1) {
    const decision = items[0];
    const selected = requirements.find(r => r.id === decision.requirementId)!;
    const plan = planSchema.parse({
      ...(sourceContractVersion?{sourceContractVersion}:{}),
      title: previous?.title ?? request.message.slice(0, 120),
      summary: `Lote verificável: ${decision.title}. ${pending.length} requisito(s) ainda pendente(s).`,
      requirements,
      assumptions: ['Aceite de lote parcial não encerra o objetivo. Cada lote preserva a candidata anterior verificada.'],
      approachChange: previous ? decision.action : '',
      incremental: {version: 1, selectedRequirementId: decision.requirementId, completedRequirementIds: completed, pendingRequirementIds: pending},
      tasks: [{
        key: 'deliver', title: decision.title, agentId: decision.agentId, stage: 'work',
        baseWorkspace: request.workspaces[0], covers: [decision.requirementId],
        ...(decision.writePaths?.length ? { writePaths: decision.writePaths } : {}),
        ...combined([decision.sourceContract]),
        description: `${decision.action}\nEntregue integralmente este requisito: ${selected.description}\nReproduza o defeito antes de corrigir quando aplicável; execute os controles: ${decision.verification}\nPreserve funcionalidades e mudanças dos lotes anteriores. Trabalhe de forma incremental e verificavel: escreva primeiro um esqueleto minimo dos arquivos de entrega e sele evidencias a cada etapa concluida, em vez de auditar a arvore inteira antes da primeira escrita. Limite a exploracao aos arquivos do contrato e das dependencias diretas. Nao crie bancos, containers ou dados de runtime locais (nada de pgdata, docker ou DATABASE_URL inventada): request_validation e uma acao exclusiva do manager, nunca uma ferramenta do worker. Siga a capacidade de CI capturada na admissao: com politica registrada, entregue validation-intent.json e encerre para o controlador executar CI; sem politica, registre a limitacao sem inventar testes ou infraestrutura. Codigo exigido como produto pertence aos caminhos de fonte declarados, nunca apenas em evidence/ ou data/. Registre comandos, resultados e limitações em evidence/delivery.json. O campo passed só pode ser true após verificar a entrega; não basta produzir um plano ou relatório.`,
        criteria: [{id:'delivery',label:'Entrega implementada e controles executados',type:'json_field',name:'delivery.json',field:'passed',equals:true}],
      }, {
        key:'verify',title: pending.length ? 'Verificar lote e regressões' : 'Verificar entrega completa',agentId:'qa.integration',stage:'verify',
        baseWorkspace:request.workspaces[0],sourceTask:'deliver',dependencies:['deliver'],covers,
        ...combined([decision.sourceContract]),
        description:`Verifique independentemente a candidata imutável e repita os controles: ${decision.verification}\n${pending.length ? 'Este aceite cobre apenas o lote selecionado, não encerra o objetivo. Verifique também regressões das entregas anteriores.' : 'Esta é a verificação final: comprove TODOS os requisitos do objetivo na mesma candidata acumulada, incluindo os lotes anteriores.'}\nInspecione evidências reais, registre comandos e resultados em evidence/verification.json. passed deve ser false se houver falhas, critérios não verificados ou evidência ausente. Não repare a candidata.`,
        criteria:[{id:'verified',label:'Verificação independente da candidata',type:'json_field',name:'verification.json',field:'passed',equals:true}],
      }],
    });
    return finish(plan);
  }
  const seenPaths: { idx: number; path: string }[] = [];
  const deliveries=keyed.filter(item=>item.kind==='delivery');
  for (const item of deliveries) {
    const idx=item.index;
    if (deliveries.length>1&&!item.writePaths?.length)
      invalid('WRITE_SCOPE_REQUIRED','Subtarefas no mesmo lote exigem writePaths explícitos.',{key:item.key??`part-${idx+1}`});
    for (const rawPath of item.writePaths??[]) {
      if(isAttemptLocalWriteScope(rawPath))continue;
      const norm = rawPath.replace(/\/+$/, '');
      if (seenPaths.some(other => other.idx !== idx && (other.path === norm || other.path.startsWith(norm + '/') || norm.startsWith(other.path + '/'))))
        invalid('WRITE_SCOPE_CONFLICT','Subtarefas precisam de escopos de escrita disjuntos; agrupe escrita compartilhada na mesma tarefa.',{path:rawPath,key:item.key??`part-${idx+1}`});
      seenPaths.push({ idx, path: norm });
    }
  }
  const taskKey=(key:string)=>`${byKey.get(key)!.kind==='validation'?'validate':'deliver'}-${byKey.get(key)!.index+1}`;
  function sourceParents(item:typeof keyed[number]){
    return [...new Set((item.dependsOn??[]).map(key=>{
      const parent=byKey.get(key)!;return parent.kind==='validation'?parent.validationTargetKey!:key;
    }))];
  }
  function sourceAncestors(item:typeof keyed[number]):Set<string>{
    const found=new Set<string>();
    for(const key of sourceParents(item)){
      found.add(key);
      for(const parent of sourceAncestors(byKey.get(key)!))found.add(parent);
    }
    return found;
  }
  const joins:z.input<typeof planSchema>['tasks']=[];
  const validationTasks:z.input<typeof planSchema>['tasks']=ordered.filter(item=>item.kind==='validation').map(item=>({
    key:taskKey(item.partKey),title:item.title,agentId:'qa.integration',agentRole:'validator',stage:'verify',
    baseWorkspace:request.workspaces[0],sourceTask:taskKey(item.validationTargetKey!),
    dependencies:item.dependsOn!.map(taskKey),covers:[item.requirementId],writePaths:item.writePaths??[],
    ...combined([byKey.get(item.validationTargetKey!)!.sourceContract]),
    description:`${item.action}\nValide somente a candidata aceita de ${taskKey(item.validationTargetKey!)}. Outras dependências fornecem evidências, não substituem a candidata alvo.\nExecute os controles: ${item.verification}\nA fonte é somente leitura. Grave relatórios e scripts em evidence/ e estado temporário em data/. Registre comandos, resultados e limitações em evidence/verification.json. passed deve ser false para qualquer falha, critério não verificado ou evidência ausente. Não repare a candidata. Este aceite não substitui a verificação final da entrega acumulada.`,
    criteria:[{id:'verified',label:'Validação independente da candidata alvo',type:'json_field',name:'verification.json',field:'passed',equals:true}],
  }));
  const workTasks = ordered.filter(item=>item.kind==='delivery').map((item) => {
    const idx=item.index;
    const selected = requirements.find(r => r.id === item.requirementId)!;
    const validationGates=(item.dependsOn??[]).filter(key=>byKey.get(key)!.kind==='validation').map(taskKey);
    // Validation workspaces are not source producers. A delivery waiting for a
    // validation inherits its original producer, never the validator's copy.
    const dependencies=sourceParents(item).map(taskKey);
    let sourceTask=dependencies[0];
    if(dependencies.length>1){
      sourceTask=`join-${idx+1}`;
      const sourceKeys=sourceAncestors(item);
      const parents=ordered.filter(p=>p.kind==='delivery'&&sourceKeys.has(p.partKey));
      joins.push({key:sourceTask,title:`Preparar insumos de ${item.title}`.slice(0,200),
        agentId:'engineering.integration',stage:'integrate',baseWorkspace:request.workspaces[0],
        dependencies:parents.map(p=>taskKey(p.partKey)),covers:[...new Set(parents.map(p=>p.requirementId))],
        writePaths:[...new Set(parents.flatMap(p=>p.writePaths??[]))],
        ...composed(parents),
        description:`Integre os patches canônicos das dependências ${parents.map(p=>taskKey(p.partKey)).join(', ')} na ordem indicada sobre a base comum. Preserve todas as pré-imagens e os lotes anteriores. Esta preparação fornece insumos à subtarefa ${item.partKey}; não conclui nenhum requisito. Execute os controles das dependências: ${parents.map(p=>p.verification).join(' | ')}. Registre evidence/delivery.json, passed:true somente se todos passarem.`,
        criteria:[{id:'joined',label:'Insumos integrados e verificados',type:'json_field',name:'delivery.json',field:'passed',equals:true}]});
    }
    const shared=deliveries.filter(i=>i.requirementId===item.requirementId).length>1;
    return {
      key: `deliver-${idx + 1}`,
      title: item.title,
      agentId: item.agentId,
      stage: 'work' as const,
      ...(sourceTask?{dependencies:[...new Set([sourceTask,...validationGates])],sourceTask}:{dependencies:[]}),
      baseWorkspace: request.workspaces[0],
      covers: [item.requirementId],
      writePaths: item.writePaths??[],
      ...combined([item.sourceContract]),
      description: `${item.action}\n${shared?'Entregue a contribuição delimitada desta subtarefa; a integração e o QA deverão comprovar o requisito inteiro':'Entregue integralmente este requisito'}: ${selected.description}\nIdentidade da subtarefa: ${item.partKey}. Dependências: ${(item.dependsOn??[]).join(', ')||'nenhuma'}.\nEscopo exclusivo de escrita desta subtarefa: ${(item.writePaths??[]).join(', ')}\nReproduza o defeito antes de corrigir quando aplicável; execute os controles: ${item.verification}\nPreserve funcionalidades e mudanças dos lotes anteriores. Trabalhe de forma incremental e verificavel: escreva primeiro um esqueleto minimo dos arquivos de entrega e sele evidencias a cada etapa concluida, em vez de auditar a arvore inteira antes da primeira escrita. Limite a exploracao aos arquivos do contrato e das dependencias diretas. Nao crie bancos, containers ou dados de runtime locais (nada de pgdata, docker ou DATABASE_URL inventada): request_validation e uma acao exclusiva do manager, nunca uma ferramenta do worker. Siga a capacidade de CI capturada na admissao: com politica registrada, entregue validation-intent.json e encerre para o controlador executar CI; sem politica, registre a limitacao sem inventar testes ou infraestrutura. Codigo exigido como produto pertence aos caminhos de fonte declarados, nunca apenas em evidence/ ou data/. Registre comandos, resultados e limitações em evidence/delivery.json. O campo passed só pode ser true após verificar a entrega; não basta produzir um plano ou relatório.`,
      criteria: [{id:'delivery',label:'Entrega implementada e controles executados',type:'json_field' as const,name:'delivery.json',field:'passed',equals:true}],
    };
  });
  const combinedVerification = items.map(i => `[${i.requirementId}] ${i.verification}`).join(' | ');
  const combinedWritePaths = [...new Set(deliveries.flatMap(i => i.writePaths ?? []))];
  const plan = planSchema.parse({
    ...(sourceContractVersion?{sourceContractVersion}:{}),
    title: previous?.title ?? request.message.slice(0, 120),
    summary: `Janela paralela verificável (${items.length} entregas): ${items.map(i => i.title).join('; ')}. ${pending.length} requisito(s) ainda pendente(s).`,
    requirements,
    assumptions: ['Aceite de lote parcial não encerra o objetivo. Entregas paralelas têm escopos de fonte disjuntos, integração explícita e QA independente. evidence/ e data/ pertencem a cada workspace isolado; consumidores precisam de artefatos publicados, não de diretórios locais implícitos.'],
    approachChange: previous ? items.map(i => i.action).join(' | ').slice(0, 4000) : '',
    incremental: {
      version: 1,
      selectedRequirementId: selectedIds[0],
      selectedRequirementIds: selectedIds,
      completedRequirementIds: completed,
      pendingRequirementIds: pending,
    },
    tasks: [
      ...joins,...workTasks,...validationTasks,
      {
        key: 'integrate',
        title: 'Integrar entregas paralelas do lote',
        agentId: 'engineering.integration',
        stage: 'integrate',
        baseWorkspace: request.workspaces[0],
        dependencies: workTasks.map(w => w.key),
        covers: selectedIds,
        writePaths: combinedWritePaths,
        ...composed(deliveries),
        description: `Aplique os patches canônicos (changes.patch em /home/agent/inputs) das tarefas aceitas deste lote (${workTasks.map(w => w.key).join(', ')}) sobre a base comum na ordem indicada sem omitir nenhuma alteração. Cada patch de uma subtarefa dependente é o delta da sua própria fonte já integrada; não substitua a base por uma branch parcial.\nRequisitos completos que o conjunto deve satisfazer: ${JSON.stringify(requirements.filter(r=>selectedIds.includes(r.id)))}\nExecute os controles combinados: ${combinedVerification}\nPreserve todas as entregas dos lotes anteriores e registre o resultado real da integração em evidence/delivery.json com passed:true somente após comprovar os requisitos inteiros e todos os controles combinados.`,
        criteria: [{id:'integrated',label:'Patches integrados e controles combinados executados',type:'json_field',name:'delivery.json',field:'passed',equals:true}],
      },
      {
        key: 'verify',
        title: pending.length ? 'Verificar lote integrado e regressões' : 'Verificar entrega completa',
        agentId: 'qa.integration',
        stage: 'verify',
        baseWorkspace: request.workspaces[0],
        sourceTask: 'integrate',
        dependencies: ['integrate',...validationTasks.map(task=>task.key)],
        ...composed(deliveries),
        covers,
        description: `Verifique independentemente a candidata integrada imutável e repita os controles combinados: ${combinedVerification}\nComprove os requisitos COMPLETOS: ${JSON.stringify(requirements.filter(r=>selectedIds.includes(r.id)))}. Aceites parciais das subtarefas não comprovam a entrega conjunta.\n${pending.length ? 'Este aceite cobre apenas a janela selecionada, não encerra o objetivo. Verifique também regressões das entregas anteriores.' : 'Esta é a verificação final: comprove TODOS os requisitos do objetivo na mesma candidata acumulada, incluindo os lotes anteriores.'}\nInspecione evidências reais, registre comandos e resultados em evidence/verification.json. passed deve ser false se houver falhas, critérios não verificados ou evidência ausente. Não repare a candidata.`,
        criteria: [{id:'verified',label:'Verificação independente da candidata',type:'json_field',name:'verification.json',field:'passed',equals:true}],
      },
    ],
  });
  return finish(plan);
}

function deliveryFeedbackText(reports: string[] = []) {
  if (!reports.length) return '';
  return '\nREPORTED EVIDENCE FOR THIS REVISION (unverified claims; preserve scope and original criteria):\n' +
    reports.map((report,i)=>`[${i+1}] ${report}`).join('\n') +
    '\nReproduce each applicable counterexample. Implementation must repair confirmed defects; independent QA must rerun the reported cases against this same candidate and report any unresolved case as passed:false. Prior passing reports do not settle these new observations.';
}

export function bindDeliveryFeedback(plan: ProposedPlan, reports: string[] = []) {
  if (!plan.incremental) return plan;
  // Do not ask the planner to reproduce counterexamples in its small response.
  // Both actors receive the exact persisted feedback, independently of the LLM
  // summary. It is an assertion to verify, never an acceptance or new authority.
  const feedback = deliveryFeedbackText(reports);
  const bound = structuredClone(plan);
  if (feedback) {
    const missing = bound.tasks.filter(t=>!t.description.endsWith(feedback));
    if (missing.some(t=>t.description.length+feedback.length>20000))
      throw new DomainError('REQUIRED_CONTEXT','O feedback integral excede o contexto da tarefa; preserve-o em entradas referenciadas antes de executar.');
    for (const task of missing) task.description += feedback;
  }
  return planSchema.parse(bound);
}

/** Called inside the existing scheduler transaction/lock. Acceptance is not
 * inferred from a model response; current run, acceptance and immutable
 * candidate metadata are required before handing the source to the next lot. */
export async function verifiedBatch(db: DB, goalId: string, revision: number) {
  const tasks = (await db.query(`SELECT t.id,t.key,t.state,t.stage,t.execution,t.attempt_count,
    r.id run_id,r.state run_state,
    (SELECT passed FROM acceptances WHERE run_id=r.id ORDER BY created_at DESC,id DESC LIMIT 1) passed,
    (SELECT id FROM acceptances WHERE run_id=r.id ORDER BY created_at DESC,id DESC LIMIT 1) acceptance_id,
    a.id candidate_artifact_id,a.digest candidate_digest,
    ARRAY(SELECT d.depends_on FROM dependencies d WHERE d.task_id=t.id) dependency_ids
    FROM tasks t LEFT JOIN runs r ON r.task_id=t.id AND r.attempt=t.attempt_count
    LEFT JOIN LATERAL(SELECT id,digest FROM artifacts WHERE run_id=r.id AND name='candidate.json' ORDER BY created_at DESC,id DESC LIMIT 1)a ON true
    WHERE t.goal_id=$1 AND t.plan_revision=$2 AND t.stage IN('work','integrate','verify') AND t.state<>'superseded'`, [goalId,revision])).rows;
  const verifiers = tasks.filter(t => t.stage === 'verify');
  const integrations = tasks.filter(t => t.stage === 'integrate');
  const workers = tasks.filter(t => t.stage === 'work');
  const byId=new Map(tasks.map(task=>[task.id,task]));
  function ancestors(id:string){
    const found=new Set<string>(),pending=[...(byId.get(id)?.dependency_ids??[])];
    while(pending.length){
      const parent=pending.pop()!;
      if(found.has(parent))continue;
      found.add(parent);pending.push(...(byId.get(parent)?.dependency_ids??[]));
    }
    return found;
  }
  if(tasks.some(task=>task.dependency_ids.some((id:string)=>!byId.has(id))||ancestors(task.id).has(task.id)))return null;
  // A targeted intermediate check cannot advance the batch. The final check
  // must follow every task, including every validation gate, and inspect the
  // accumulated source producer rather than any validator's unchanged copy.
  const finals=verifiers.filter(candidate=>{
    const parents=ancestors(candidate.id);
    const sources=tasks.filter(task=>task.execution?.workspace===candidate.execution?.sourceWorkspace&&task.stage!=='verify');
    if(sources.length!==1||parents.has(candidate.id)||!parents.has(sources[0].id))return false;
    const sourceParents=ancestors(sources[0].id);
    return tasks.every(task=>task.id===candidate.id||parents.has(task.id))&&
      [...workers,...integrations].every(task=>task.id===sources[0].id||sourceParents.has(task.id));
  });
  const verifier=finals[0];
  const sourceProducer=tasks.find(task=>task.execution?.workspace===verifier?.execution?.sourceWorkspace&&task.stage!=='verify');
  if (!workers.length || finals.length!==1 || !sourceProducer ||
      tasks.some(t => t.state !== 'accepted' || t.run_state !== 'submitted' || t.passed !== true) ||
      !verifier.execution?.sourceWorkspace || tasks.some(t => !t.candidate_artifact_id)) return null;
  if ((await db.query(`SELECT 1 FROM runs r JOIN tasks t ON t.id=r.task_id WHERE t.goal_id=$1
      AND (r.state IN('queued','running','cancelling','reconciling') OR EXISTS(SELECT 1 FROM leases l WHERE l.run_id=r.id)) LIMIT 1`,[goalId])).rowCount) return null;
  return { taskId:verifier.id, runId:verifier.run_id, sourceWorkspace:verifier.execution.sourceWorkspace,
    candidateArtifactId:verifier.candidate_artifact_id,candidateDigest:verifier.candidate_digest,
    verificationAcceptanceId:verifier.acceptance_id,workRunId:sourceProducer.run_id };
}

export async function queueNextDelivery(db: DB, objective: any, previous: ProposedPlan) {
  if (!previous.incremental || (!previous.incremental.pendingRequirementIds.length&&previous.completionContractVersion!==2)) return false;
  if(previous.completionContractVersion===2&&!(await db.query(`SELECT 1 FROM objectives o JOIN goals g ON g.id=o.id
    WHERE o.id=$1 AND o.revision=$2 AND o.mode='execute' AND g.state='active'`,[objective.id,objective.revision])).rowCount)return false;
  if ((await db.query("SELECT 1 FROM planning_runs WHERE objective_id=$1 AND state IN('queued','running','waiting_quota')",[objective.id])).rowCount) return false;
  const proof = await verifiedBatch(db, objective.id, objective.revision);
  if (!proof) return false;
  const resolution=previous.completionContractVersion===2?await readRequirementResolution(db,objective.id,objective.revision):undefined;
  if(resolution&&!deliveryFrontier(objective.request,previous,true,resolution).eligible.length)return false;
  const id = randomUUID();
  const inserted = await db.query(`INSERT INTO planning_runs(id,objective_id,base_revision,trigger_key,fence,feedback)
    VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING`,[id,objective.id,objective.revision,
    `incremental:${objective.revision}:${proof.runId}`,randomUUID(),JSON.stringify({sourceContractVersion:1,incrementalAdvance:proof,
      ...(resolution?{completionContractVersion:2,requirementResolution:resolution}:{}),
      sourceOverrides:{[objective.request.workspaces[0]]:proof.sourceWorkspace}})]);
  if (!inserted.rowCount) return false;
  await db.query("UPDATE objectives SET status='replanning',reason=NULL,updated_at=now() WHERE id=$1",[objective.id]);
  await db.query("UPDATE goals SET state='replanning' WHERE id=$1 AND state='active'",[objective.id]);
  await event(db,'INCREMENTAL_DELIVERY_QUEUED',objective.id,{planningRunId:id,revision:objective.revision,proof,
    remaining:previous.incremental.pendingRequirementIds});
  return true;
}

export async function assertIncrementalAuthority(db: DB, run: any, plan: ProposedPlan, previous?: ProposedPlan) {
  if (!plan.incremental) {
    if (previous?.incremental) throw new DomainError('PLAN_INVALID','Uma revisão não pode descartar requisitos incrementais pendentes.');
    return;
  }
  const feedback = deliveryFeedbackText(run.feedback?.reports);
  if (feedback && plan.tasks.some(t=>!t.description.endsWith(feedback)))
    throw new DomainError('REQUIRED_CONTEXT','A revisão não pode omitir o feedback persistido dos executores e do QA.');
  const advance = !!run.feedback?.incrementalAdvance;
  const resolution=run.feedback?.requirementResolution;
  if(previous?.completionContractVersion===2){
    const current=await readRequirementResolution(db,run.objective_id,run.base_revision);
    if(!resolution||!isDeepStrictEqual(current,resolution))throw new DomainError('STALE_PLAN','Resultados por requisito mudaram desde o planejamento.');
  }
  const { completed, eligible } = deliveryFrontier(run.request, previous, advance,resolution);
  const selectedIds = selectedRequirementIds(plan.incremental);
  const expectedPending = plan.requirements.map(r => r.id).filter(id => !completed.includes(id) && !selectedIds.includes(id));
  if (!selectedIds.length || new Set(selectedIds).size !== selectedIds.length ||
      selectedIds.some(id => !eligible.includes(id)) ||
      JSON.stringify(completed) !== JSON.stringify(plan.incremental.completedRequirementIds) ||
      JSON.stringify(expectedPending) !== JSON.stringify(plan.incremental.pendingRequirementIds))
    throw new DomainError('PLAN_INVALID','Cobertura incremental não corresponde ao estado persistido.');
  if (advance) {
    const proof = await verifiedBatch(db,run.objective_id,run.base_revision);
    if (!proof || !isDeepStrictEqual(proof,run.feedback.incrementalAdvance) ||
        run.feedback.sourceOverrides?.[run.request.workspaces[0]] !== proof.sourceWorkspace)
      throw new DomainError('STALE_PLAN','A candidata verificada ou seu aceite mudou.');
  }
}
