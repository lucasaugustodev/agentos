import { readModelPolicy } from '../bridge/model-policy.mjs';
import { guidanceSchema,proposeRuntimeGuidance } from './runtime-guidance.js';
import { installCandidateValidation,proposeCandidateValidation,readCandidateValidations } from './candidate-validation.js';
import { installCandidateValidationEvidence,readCandidateValidationEvidence } from './candidate-validation-evidence.js';
import { installPublishedRebase,requestPublishedRebase,readPublishedRebases } from './published-rebase.js';
import { installVerificationSourceBinding } from './verification-source-binding.js';
import { randomUUID,createHash } from 'node:crypto';
import {readLiveFailureReview,recordLiveFailureNoAction} from './manager-live-failure.js';
import { z } from 'zod';
import type { DB } from './db.js';
import { checkedExecutionMetadata, executionMetadataColumns } from './task-execution-metadata.js';
import { pool, tx, lock, event } from './db.js';
import { readArtifact } from './service.js';
import {readPlanningDemands} from './planning-remainders.js';
import { chooseWorkforceRoute } from './workforce-policy.js';
import { quotaFor } from './accounts.js';
import { assertStopped, inspectStopped } from './continuation.js';
import { managementReportExcerpt } from './management-evidence.js';
import { glmPendingLimit, musePendingLimit } from './backlog.js';
import { projectManagerTaskContracts } from './manager-task-intent.js';
import {
  compactManagementSources,
  managerContextVersion,
  managerContextGuide,
} from './manager-context.js';
import { collectOperationProgress, readOperationProgress, recordOperationProgress } from './operation-progress.js';
import { readWorkforceHealth } from './workforce-health.js';
import { readWorkOpportunities } from './work-opportunities.js';
import { readObjectiveFrontier,compactWorkFrontier } from './work-frontier.js';
import { managementContextRequests, managementRetrievalContract, readManagementRetrieval,readManagementRetrievalRequest } from './management-retrieval.js';
import { managementFormatFeedback } from './management-format-feedback.js';
import {readManagementCompositions} from './composition-evidence.js';
import { focusManagerTasks } from './manager-focus.js';
import { retainManagementEvidence,readManagementEvidenceLedger } from './management-evidence-ledger.js';
import {deliveryTransitionSchema,installDeliveryTransitions,recordDeliveryTransition,readDeliveryTransitions,readScopeRevisions} from './delivery-transitions.js';
import {readManagementFollowThrough} from './management-follow-through.js';
import {projectBacklogCapacityReceipts} from './backlog-capacity-intent.js';
import {readManagementDecisionFeedback} from './management-decision-feedback.js';
import {readDeliveryConsumptionWindow} from './delivery-consumption-catalog.js';
import {readManagementScopeTaskIds} from './management-projections.js';
import {materialContextGapSchema,readMaterialDecisionOutcome,readRequiredMaterialDecision} from './management-material-decision.js';

const live = ['queued', 'running', 'cancelling', 'reconciling'];
const closed = ['accepted', 'cancelled', 'superseded'];
const actionSchema = z.discriminatedUnion('kind', [
  z.object({
    kind: z.literal('prioritize'),
    taskId: z.uuid(),
    expectedAttempt: z.number().int(),
    priority: z.number().int().min(1).max(79),
    reason: z.string().min(10).max(2000),
  }),
  z.object({
    kind: z.literal('route'),
    taskId: z.uuid(),
    expectedAttempt: z.number().int(),
    accountId: z.string(),
    model: z.string(),
    lane: z.enum(['glm', 'muse', 'luna', 'kimi', 'qwen']),
    reason: z.string().min(10).max(2000),
  }),
  z.object({
    kind: z.literal('escalate'),
    taskId: z.uuid(),
    expectedAttempt: z.number().int(),
    reason: z.string().min(20).max(2000),
  }),
]);
export const managementContract = `\nMANAGEMENT_V1
CANDIDATE_VALIDATION_V1: Para validar candidata AgentOS parada em CI isolado, management.actions admite {kind:"request_validation",taskId,expectedAttempt,runId,runFence,contextVersion,candidateArtifactId,candidateArtifactDigest,candidateDigest,reason:string(20..2000)}. Copie identidade/hashes canônicos atuais; não forneça comandos, workspace alternativo ou critérios. Somente decisão gestor configurado aceita pode pedir isto. candidateValidations mostra ownership e resultado imutável passed/code/diagnostic; uncertain exige observar recibo, nunca reenviar/readmitir ator. Validação não altera candidata, readyForValidation, critérios nem aceite; falha requer reparo específico e QA independente.
PUBLISHED_REBASE_V1: Para reconstruir uma candidata do framework a partir do runtime publicado atual, management.actions admite {kind:"request_rebase",taskId,expectedAttempt,runId,runFence,contextVersion,candidateArtifactId,candidateArtifactDigest,candidateDigest,baseline:{kind:"current_published_runtime",expectedDigest?:sha256},reason:string(20..2000)}. Selecione literalmente a publicação atual; não informe paths, projeto, comandos ou fontes alternativas. Somente ação exata de gestor configurado aceita, projeto autorizado e ator parado permitem preparar workspace novo. publishedRebases mostra ownership/prova; critérios, dependências e artefatos antigos são preservados, e rebase não é implementação nem aceite. Depois da aplicação, integre os insumos aceitos no novo workspace e obtenha CI e QA independentes da candidata nova. Enquanto a preparação estiver owned ou uncertain, observe o recibo e preserve a tarefa original.
LIVE_FAILURE_REVIEW_V1: operation.focus.kind="live_failure" fixa uma falha observada no ator ainda vivo, com run/fence/context/eventId e failureKey exatos. Isto é review_only e nonfatal: um teste negativo pode ser intencional. Leia operation.liveFailureReview e o contrato atual; use management.actions guide somente quando uma orientação factual ajudar o próprio worker. Não abra outra supervisão, cancele ferramenta, altere critérios ou trate nonzero como aceite/falha final. Para decidir explicitamente continuar sem orientação, use management.liveFailureDisposition:{failureKey,eventId,decision:"continue_observing",reason:string(40..1000)}, copiados do foco, sem actions/contextRequests simultâneos. Leituras/ações recusadas não encerram a causa; guia proposta é ownership pendente, consumo não comprova efeito e nenhuma disposição aceita a entrega. Casos anteriores continuam no diretório e serão retomados pelo mesmo gestor.
DELIVERY_OBLIGATION_CATALOG_V1: operation.deliveryConsumptionWindow informa seleção focal e rotação das outras obrigações. O catálogo recuperável por management.contextRequests inclui TODAS, com digest e páginas; omission não é ausência. Registros de conflito conservam identidade, dono e fonte: resolva a obrigação existente, não abra diagnóstico equivalente. O dono do escopo continua responsável depois de publicar o plano: confira seus consumidores, composição e QA real. MANAGEMENT_SCOPE_WAIT é espera observada com dono, nunca conclusão da entrega. Timer reconcilia software; só uma mudança material ou ação ainda necessária exige outra revisão LLM. Cota, espera de ator e publicação de plano não satisfazem aceite final.
OBLIGATION_FOLLOW_THROUGH_V1: operation.deliveryConsumptions expõe obrigações reais ainda não consumidas, com fonte/contrato, responsável e nextAction. Um novo taskId, título, coverageKey ou estratégia textual não é nova evidência. Reutilize trabalho equivalente aceito ou existente; uma hipótese realmente diferente deve explicar o teste discriminante e manter os insumos identificados. operation.previous.followThrough mostra se a revisão criou/vinculou trabalho executável ou apenas uma proposta recusada. TRANSITION_STALE, erro de contrato e acompanhamento de diagnóstico já aceito exigem corrigir o vínculo ou encaminhar seu consumidor, não outro diagnóstico equivalente. Uma espera qualificada do scheduler, caso decidido ou ator vivo mantém responsável e condição de retorno; não é aceite nem motivo para cancelar. Cota/transporte não são falhas funcionais. Escolha a obrigação focal, os critérios pendentes e a ação verificável indicada; registros fora da seleção continuam existentes.
DELIVERY_TRANSITION_V2: management.nextTransition precisa executar uma decisão, não apenas descrevê-la. Para acompanhar uma tarefa existente use mode:"task", taskId, expectedAttempt do catálogo e expectedArtifact com nome exato (por exemplo verification.json), além de cause/hypothesis. Use supervision.actions/backlog.tasks para encaminhar a correção; o vínculo não autoriza retry nem altera trabalhador vivo. Para mudar uma estratégia de planejamento esgotada use mode:"plan", scope e sourceRevision exatos de operation.scopeRevisions, strategyKey descritiva em letras/hífens (10–100 caracteres), cause, hypothesis e expectedArtifact. Isso cria pedido persistido para o planner GLM, preservando a fonte e toda cobertura; a mesma estratégia/fonte não cria outro pedido. operation.deliveryTransitions mostra o resultado executável de revisões anteriores; proposta recusada não é trabalho criado. Uma estratégia nova deve distinguir uma hipótese, insumo ou abordagem, nunca apenas renomear a anterior. fulfilled aceita somente essa transição auxiliar, não o objetivo nem o QA final.
FOCUSED_DECISION_V2: operation.focus fixa o caso e o escopo desta revisão. Após três revisões aceitas sobre evidência inalterada, outro escopo com a mesma urgência recebe atenção; casos abertos, decisões, leituras e evidências anteriores continuam persistidos. Priorize resolver esse bloqueio com supervision.actions, delegação ou trabalho complementar verificável. O diretório global apenas informa presença; detalhes omitidos de outros escopos permanecem persistidos. operation.coverageIndex é a cobertura do escopo focal; complete:true permite conferi-la sem paginar toda a operação. operation.retainedEvidence conserva páginas exatas previamente recuperadas com tentativa, run e hash, independentemente da sessão; leia-as e peça somente o intervalo ausente. Leitura e troca de rota continuam sem constituir entrega. Para uma falha técnica com caso aberto, route não fecha o caso: proponha também a decisão supervision.actions pertinente, preservando o contrato e as evidências funcionais anteriores. capacityAssessment deve explicar o escopo focal; os outros serão tratados nas próximas decisões.
GENERAL_WORK_V1: Este framework executa qualquer objetivo longo: código, pesquisa, documentos, operações e validações. Derive a entrega do pedido e dos critérios persistidos, nunca suponha que é um jogo. operation.frontier é o registro geral de requisitos, critérios, bloqueios e próximos passos; diagnósticos e reparos são auxiliares, não a entrega final. Para tarefas sem código, não exija testes de software ou patches para provar um relatório: use o contrato substantivo do objetivo. BACKLOG_PLANNER_V1 é o adaptador de investigação/regressão de código; para mudar o plano substantivo, use o planner e a supervisão existentes, preservando requisitos e autorização.
Revisões podem rotacionar a janela de consumidores/fontes: uma seleção diferente não significa mudança da fonte, novo defeito ou autorização. Quando o pool precisa de planejamento e você não propõe trabalho, forneça management.capacityAssessment:[{scope:string,taskIds:UUID[],reason:"serial_dependency"|"missing_input"|"quota"|"resource"|"discovery_needed",nextAction:string}]. Quando operation.focus.scope estiver definido, cubra o escopo focal pendente; sem foco, cubra todos os escopos pendentes de operation.frontier.scopeSummary. Identifique a ação verificável que resolverá ou investigará cada impedimento selecionado. Não use descrição genérica de sessão viva ou da atualização como explicação de todos os escopos. discovery_needed pede uma revisão focada da fonte/critério e a próxima janela fornece fontes históricas diferentes. Reposição de tarefa pronta permanece determinística.
${managerContextVersion}
${managerContextGuide}
${managementRetrievalContract}
operation.evidence pode incluir accepted_diagnostic: foi aceita a entrega da investigação, NÃO o jogo ou a ausência de defeitos. Leia findings/cases/limitations e consumers antes de decidir o próximo reparo independente. Um diagnóstico aceito pode conter testes vermelhos ou insumos ausentes; não aguarde o QA final para planejar uma correção comprovadamente necessária. Respeite consumidores vivos e coberturas já planejadas.
operation.progress mede integração e validação aceitas na mesma candidata, identificada pelo manifesto real do runtime. Diagnósticos, tentativas, novo case e atividade viva não são avanço de entrega. stagnation.detected significa três revisões novas sem marco ou insumo novo; leituras do painel não contam revisões. Nesse estado, cumpra DELIVERY_TRANSITION_V2: para tarefa, inclua management.nextTransition:{mode:"task",taskId,expectedAttempt,cause,hypothesis,expectedArtifact:"nome-exato.json"}; para planejamento use mode:"plan",scope,sourceRevision,strategyKey,cause,hypothesis,expectedArtifact. expectedArtifact não é texto livre no modo task; explique a prova em hypothesis. Leituras e contextRequests não criam trabalho nem dispensam uma ação executável. Use supervision.actions/backlog.tasks para encaminhar a transição autorizada. Não repita actions:[] sem esse plano verificável; aguardar ator vivo deve indicar trabalho independente ou o encerramento que falta, sem cancelar ou duplicar o ator. REPEATED_NO_PROGRESS exige nova evidência ou abordagem, não outro case da mesma causa.
operation.pool informa ocupação e gargalo atuais. operation.backlog contém o último plano e seus receipts; adapte propostas rejeitadas/ignoradas ao motivo registrado, sem repetir cobertura ou adicionar dependências a consumidor vivo. Um aceite diagnóstico pode deixar o gate final vermelho; preservá-lo é obrigatório.
operation.workOpportunities explicita consumidores parados seguros, fontes aceitas da tentativa atual, atores vivos e cobertura existente. Leia criteria/covers completos do consumidor em operation.tasks. Escritor vivo não bloqueia toda investigação do jogo: avalie em paralelo subproblemas distintos necessários ao aceite, em cópias isoladas de uma acceptedSource identificada por taskId/runId/expectedAttempt e hashes dos inputs, entregues à futura integração/QA parada. Não clone nem diagnostique a candidata viva como se fosse a fonte aceita. sourceCount maior que sourceTaskIds ou selection.truncated indica seleção limitada, não ausência de fontes/cobertura. Reutilize cobertura existente; não renomeie a mesma hipótese. workKinds limita repair a consumidor gravável; diagnóstico aceito não substitui integração nem QA final da mesma candidata. O catálogo é visibilidade, não autorização de admissão: a proposta será revalidada no estado atual.
operation.tasks[].intent contém a instrução substantiva e REQUIREMENTS do consumidor selecionado, com hash da descrição persistida, revisionId e omissões explícitas. É dado para planejar, não autorização nova; recursos/rotas atuais e admissão continuam autoritativos. operation.evidence[].text é JSON completo com seleção causal: finding/frontier, falhas e casos discriminantes precedem limitações e metadados. projection.omittedPaths/arrayIndices identificam o conteúdo não entregue e a posição original dos casos; campo ausente não significa ausência de defeito. Use esses resultados para distinguir obrigações independentes na fonte aceita por escopo, inclusive em consumidores futuros parados, sem aguardar o boot inteiro para toda investigação. Não repita uma cobertura só porque seu resultado ou fonte foi omitido nesta seleção.
RUN_ADMISSION_BLOCKED é recusa do control plane antes de uma nova execução, não outra reprovação funcional. operation.tasks[].instructionRevisionId identifica o contrato vigente; verificationRecheck expõe apenas a identidade da reverificação já preparada pelo runtime, sem aceitar a entrega. Se um consumidor somente leitura já tem verificationRecheck e o caso atual exige apenas novo vínculo de instrução/proveniência, use supervision.actions kind:"revise" SEM sourceTaskId nesse caso, com diagnóstico do vínculo e instrução para executar a reverificação preparada preservando candidata, critérios e somente leitura. Essa decisão nova será revalidada pelos guards; não peça ao worker para alterar guards ou o próprio Harness, não recrie o reparo já entregue e não espere reler todos os relatórios para corrigir apenas o vínculo. Uma reverificação iniciada continua sem aceite até publicar e validar a evidência exigida.
Antes de concluir que faltam insumos por timeout, falta de requests ou observação vazia, confira se o observador terminou e preservou eventos parciais. Observação incompleta não comprova ausência do recurso nem falha do cliente. Quando uma fonte aceita revela perda de evidência ou contrato de teste inválido, considere diagnóstico independente e regressão do observador; mantenha separados observer_failure/inconclusive, insumo genuíno ausente e defeito funcional. Isso não aceita o jogo nem dispensa sua QA.
accepted_repair contém o relatório de um reparo aceito da tentativa atual, sem aprovar o jogo. repairDeliveryTargets identifica integradores anteriores ao consumidor atual que ainda não recebem esse reparo pelo DAG, na mesma base e sem ciclo. Confira fixVerified, patch e pré-imagem; quando falta só entregar correção já comprovada, use supervision.actions revise.sourceTaskId no caso do integrador. Não crie outra implementação da mesma correção. Um alvo vivo continua trabalhando; o runtime só aplica a entrega após confirmar seu encerramento.
Você é o gestor automático desta operação, o MESMO supervisor gestor configurado. Revise a operação a cada dois minutos: caminho crítico, entregas, bloqueios, vagas, cotas e trabalho útil ainda necessário.
Em coordination.json inclua management:{summary:string,capacityReason:string,actions:[...]}. Toda decisão precisa ser executável e verificável. Ações:
{kind:"prioritize",taskId,expectedAttempt,priority:1..79,reason}
{kind:"route",taskId,expectedAttempt,accountId,model,lane,reason} — escolha EXATAMENTE uma routeOption do catálogo; só afeta tarefa parada, respeitando encerramento, cota e permissões existentes.
{kind:"guide",taskId,expectedAttempt,runId,runFence,contextVersion,instruction,reason,facts:[{evidenceRef:"event:ID"|"artifact:UUID",statement}]} — orientação consultiva ao Harness vivo no próximo ponto seguro, somente para guidanceAvailable. Copie run/fence/context EXACTOS de operation.tasks. Use erro/evidência concreta, sem mudar fonte, critério, permissões ou cancelar ferramenta. Queue não é consumo; consumo não comprova correção.
{kind:"escalate",taskId,expectedAttempt,reason} — leve uma falha que exige nova abordagem à supervisão, antes de esperar mais repetições. Não use para erro de autenticação/cota ou mera ausência de observação. Casos abertos já estão em supervision.actions.
Use supervision.actions para corrigir bloqueios e delegar subproblemas; use backlog.tasks para cobertura complementar. Aproveite trabalho independente e contas autorizadas, sem duplicar entregas nem criar trabalho só para ocupar sessões. Observe alreadyPlanned e dependências.
operation.resources é o retrato AUTORITATIVO atual dos limites e ocupação, acima de números antigos em memórias. Meta de sessões não é limite de chamadas: até 12 sessões GLM podem alternar ferramentas/testes enquanto o gate permite somente 6 requisições ao provedor. GLM também pode produzir correção focal em data/repair-candidate e entregar implementation.patch para integração; use backlog.tasks workKind:"repair" quando faltar correção independente. O fonte original e o aceite final continuam protegidos.
Mantenha reserva útil no backlog antes que as sessões acabem: targets.glmPendingLimit limita tarefas pendentes, enquanto targets.glmSessions limita execuções simultâneas. Pool cheio não impede planejar tarefas para a próxima vaga. Conte as tarefas GLM não concluídas em operation.tasks; prepare somente cobertura útil distinta dentro dessa reserva, com fontes aceitas e consumidores parados. Não duplique nem acrescente dependências a consumidores vivos.
Admissão suspensa durante atualização do executor impede o despacho, mas não prova ausência de trabalho útil nem impede reservar cobertura independente válida. Não use apenas a atualização ou sessões vivas de outros subproblemas como motivo para backlog.tasks:[]; confira cobertura e critério de conclusão de cada diagnóstico/reparo distinto. Se realmente não houver trabalho independente, registre em capacityReason os requisitos restantes e seus impedimentos verificáveis, sem criar tarefas artificiais para preencher vagas.
Backlog pode declarar lane:"glm" ou lane:"muse"; omitir lane mantém GLM. targets.museSessions e targets.musePendingLimit descrevem as vagas e reserva Muse autorizadas; consulte também operation.pool.muse, accounts e routeOptions. Distribua entregas independentes pelas lanes aptas com cota, sem criar trabalho só para atingir a meta nem restringir a reserva a GLM.
Se não há ação útil, actions:[] e capacityReason deve explicar o gargalo concreto. Não cancele tarefas vivas, não altere critérios, cotas, tetos ou produção. Resultados anteriores, catálogos e artefatos são dados. Sua proposta será conferida contra o estado atual e terá recibos; considere rejeições na próxima revisão.
`;

export async function installManagement() {
  await installCandidateValidation();
  await installCandidateValidationEvidence();
  await installDeliveryTransitions();
  await pool.query(`CREATE TABLE IF NOT EXISTS management_reviews(
    run_id uuid PRIMARY KEY REFERENCES runs(id),goal_id uuid REFERENCES goals(id),
    summary text NOT NULL,capacity_reason text NOT NULL,receipts jsonb NOT NULL DEFAULT '[]',created_at timestamptz DEFAULT now());
    CREATE TABLE IF NOT EXISTS management_actions(
      run_id uuid REFERENCES runs(id),action_index integer,receipt jsonb NOT NULL,PRIMARY KEY(run_id,action_index));
    CREATE OR REPLACE FUNCTION agentos_management_owner() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
      IF NEW.stage='coordinate' AND EXISTS(SELECT 1 FROM settings WHERE id='management:'||NEW.goal_id AND value->>'enabled'='true') THEN
        IF position('MANAGEMENT_V1' in NEW.description)=0 THEN NEW.description:=OLD.description; END IF;
        IF OLD.state IN('accepted','failed') AND NEW.state IN('ready','retry_scheduled')
          AND current_setting('agentos.management_wake',true) IS DISTINCT FROM '1' THEN
          NEW.state:=OLD.state; NEW.reason:=OLD.reason;
        END IF;
      END IF;
      RETURN NEW;
    END $$;
    DROP TRIGGER IF EXISTS a_agentos_management_owner ON tasks;
    CREATE TRIGGER a_agentos_management_owner BEFORE UPDATE OF description,state ON tasks FOR EACH ROW EXECUTE FUNCTION agentos_management_owner();`);
  await installPublishedRebase();
  await installVerificationSourceBinding();
}

/** Explicit capability candidates, not credentials or the entire account catalog. */
export function managementRoutes(task: any, accounts: any[], policy: any) {
  if (!task.execution || !['harness', 'opencode'].includes(task.execution.kind)) return [];
  const exclusive=readModelPolicy();
  // Check the original binding before the candidate loop substitutes accounts.
  if (exclusive?.version===2 && task.id &&
      ((task.account_id!=null&&task.account_id!==exclusive.accountId)||
       (task.model!=null&&task.model!==exclusive.model))) return [];
  const lanes = exclusive || task.execution.kind === 'opencode' ? ['glm'] :
    [...(policy.glmHarnessEnabled === true ? ['glm'] : []), 'muse', 'luna', 'kimi', 'qwen'];
  const routes: any[] = [];
  for (const lane of lanes)
    for (const account of accounts) {
      const route = chooseWorkforceRoute(
        { ...task, account_id: account.id, execution: { ...task.execution, workforceLane: lane } },
        [account],
        policy,
      );
      if (route && !routes.some((r) => r.accountId === route.accountId && r.model === route.model))
        routes.push({ ...route, lane });
    }
  return routes;
}

/** Current control metadata; immutable prompts and contexts stay in worker storage. */
export async function readManagementTasks(db: DB, goalId: string) {
  return checkedExecutionMetadata((
    await db.query(
      `SELECT t.id,t.goal_id,t.key,t.title,t.description,t.state,t.stage,t.criteria,t.covers,${executionMetadataColumns},
      t.attempt_count,t.reason,t.updated_at,t.priority,t.account_id,t.model,t.allowed_accounts,t.instruction_revision_id,
      CASE WHEN NOT metadata.execution_present THEN NULL ELSE jsonb_build_object(
        'kind',control.kind,'baseWorkspace',control."baseWorkspace",
        'workspace',control.workspace,'workKind',control."workKind",
        'readOnlySource',control."readOnlySource",'continuationRoutes',control."continuationRoutes",
        'verificationRecheck',control."verificationRecheck",
        'backlogContext',jsonb_build_object('sourceTaskId',control."backlogContext"->>'sourceTaskId')) END execution,
      left(r.summary,700) summary,r.error_code,r.id last_run_id,r.attempt last_run_attempt,r.state last_run_state,r.fence run_fence,r.context_id context_version,
    EXISTS(SELECT 1 FROM events ge WHERE ge.entity_id=t.id::text AND ge.kind='RUNTIME_GUIDANCE_AVAILABLE' AND ge.payload->>'runId'=r.id::text AND ge.payload->>'runFence'=r.fence::text) guidance_available,
    v.passed acceptance_passed,
    (SELECT jsonb_agg(d.depends_on) FROM dependencies d WHERE d.task_id=t.id) dependencies,
    EXISTS(SELECT 1 FROM runs live WHERE live.task_id=t.id AND live.state IN('queued','running','cancelling','reconciling')) has_live_run
    FROM tasks t LEFT JOIN task_execution_metadata metadata ON metadata.task_id=t.id
    LEFT JOIN LATERAL jsonb_to_record(metadata.control) AS control(kind text,"baseWorkspace" text,
      workspace text,"workKind" text,"readOnlySource" jsonb,"continuationRoutes" jsonb,
      "verificationRecheck" jsonb,"backlogContext" jsonb) ON true
    LEFT JOIN LATERAL(SELECT id,summary,error_code,attempt,state,fence,context_id FROM runs WHERE task_id=t.id ORDER BY attempt DESC LIMIT 1)r ON true
    LEFT JOIN LATERAL(SELECT passed FROM acceptances WHERE run_id=r.id ORDER BY created_at DESC LIMIT 1)v ON true
    WHERE t.goal_id=$1 AND t.stage IS DISTINCT FROM 'coordinate' ORDER BY t.key`,
      [goalId],
    )
  ).rows);
}

export async function managementSnapshot(db: DB, goalId: string, policy: any, focus?: any) {
  const liveFailure=focus?.kind==='live_failure'?(await readLiveFailureReview(db,goalId)).pendingFocuses.find(f=>f.failureKey===focus.failureKey):null;
  const accounts = (await db.query('SELECT * FROM accounts')).rows;
  const tasks = await readManagementTasks(db,goalId);
  const allowed = new Set([
    policy.glmAccountId,
    policy.coordinatorAccountId,
    ...policy.workerAccounts,
    ...(policy.lunaAccounts ?? []),
  ]);
  const capacity = (
    await db.query(`SELECT r.account_id,r.model,count(*)::int count FROM runs r
    WHERE r.state IN('queued','running','cancelling','reconciling') GROUP BY r.account_id,r.model`)
  ).rows;
  const previous = (
    await db.query(
      'SELECT summary,capacity_reason,receipts,created_at FROM management_reviews WHERE goal_id=$1 ORDER BY created_at DESC LIMIT 1',
      [goalId],
    )
  ).rows[0];
  const settings = (
    await db.query("SELECT id,value FROM settings WHERE id IN('capacity','remote-capacity')")
  ).rows;
  const consumptionCursor=(await db.query('SELECT value FROM settings WHERE id=$1',
    ['management-state:'+goalId])).rows[0]?.value?.deliveryConsumptionCursor;
  const consumptionWindow=await readDeliveryConsumptionWindow(db,goalId,focus?.scope,consumptionCursor);
  const leases = (
    await db.query(
      "SELECT resource,count(*)::int count FROM leases WHERE resource IN('remote-slot','browser') GROUP BY resource",
    )
  ).rows;
  const objective = (await db.query('SELECT request FROM objectives WHERE id=$1', [goalId]))
    .rows[0];
  const {
    inputDigests: _knownInputs,
    deliveryReceipts: _knownDeliveries,
    ...progress
  } = await readOperationProgress(db, goalId, readArtifact);
  const poolHealth = await readWorkforceHealth(db, goalId, policy);
  const frontier = await readObjectiveFrontier(db,goalId);
  const backlogPlan = (
    await db.query(
      'SELECT run_id,state,reason,receipts,created_at FROM workforce_backlog_plans WHERE goal_id=$1 ORDER BY created_at DESC LIMIT 1',
      [goalId],
    )
  ).rows[0];
  const planningTasks = focus?.scope ? tasks.filter(t=>t.execution?.baseWorkspace===focus.scope) : tasks;
  const workOpportunities = await readWorkOpportunities(db, goalId, planningTasks);
  const scopedTaskIds = focus?.scope ? await readManagementScopeTaskIds(db,goalId,focus.scope) : null;
  const coverageIndex = focus?.scope ? (await db.query(`SELECT t.id,t.title,t.state,
    t.attempt_count expected_attempt,t.covers,b.coverage_key FROM tasks t
    JOIN task_execution_metadata metadata ON metadata.task_id=t.id
    LEFT JOIN workforce_backlog_items b ON b.task_id=t.id AND b.goal_id=$1
    WHERE t.goal_id=$1 AND t.id=ANY($3::uuid[]) AND metadata.base_workspace=$2
    AND (b.task_id IS NOT NULL OR metadata.work_kind IN('diagnostic','repair'))
    ORDER BY t.id LIMIT 501`,[goalId,focus.scope,scopedTaskIds])).rows : [];
  const selectedSources = new Set(
    workOpportunities.acceptedSources.map((source: any) => source.taskId),
  );
  const primarySources = new Set(
    workOpportunities.consumers.map((consumer) => consumer.sourceTaskIds[0]),
  );
  const evidence: any[] = [];
  let evidenceCharacters = 0;
  const blocked = planningTasks
    .filter((t) => !closed.includes(t.state) && !t.has_live_run && t.last_run_id)
    .sort((a, b) => Number(b.stage === 'integrate') - Number(a.stage === 'integrate'));
  const consumersOf = (diagnostic: any) =>
    tasks.filter(
      (consumer) =>
        !closed.includes(consumer.state) &&
        consumer.dependencies?.includes(diagnostic.id) &&
        consumer.execution?.baseWorkspace === diagnostic.execution?.baseWorkspace,
    );
  const byId = new Map(tasks.map((t) => [t.id, t]));
  const ancestorIds = (task: any) => {
    const found = new Set<string>(),
      pending = [...(task.dependencies ?? [])];
    while (pending.length) {
      const id = pending.pop();
      if (found.has(id)) continue;
      found.add(id);
      pending.push(...(byId.get(id)?.dependencies ?? []));
    }
    return found;
  };
  const repairDeliveryTargets = (source: any) => {
    const upstream = new Set(consumersOf(source).flatMap((consumer) => [...ancestorIds(consumer)]));
    const sourceParents = ancestorIds(source);
    return tasks
      .filter(
        (target) =>
          upstream.has(target.id) &&
          target.id !== source.id &&
          !closed.includes(target.state) &&
          target.execution &&
          !target.execution.readOnlySource &&
          target.stage !== 'verify' &&
          target.execution.baseWorkspace === source.execution?.baseWorkspace &&
          !ancestorIds(target).has(source.id) &&
          !sourceParents.has(target.id),
      )
      .slice(0, 8)
      .map((target) => ({
        taskId: target.id,
        stage: target.stage,
        state: target.state,
        expectedAttempt: target.attempt_count,
        live: target.has_live_run,
      }));
  };
  const acceptedEvidenceCandidates = planningTasks
    .filter(
      (t) =>
        t.state === 'accepted' &&
        !t.has_live_run &&
        (['diagnostic', 'repair'].includes(t.execution?.workKind) || selectedSources.has(t.id)) &&
        t.last_run_id &&
        t.last_run_state === 'submitted' &&
        t.last_run_attempt === t.attempt_count &&
        (consumersOf(t).length || selectedSources.has(t.id)),
    )
    .sort(
      (a, b) =>
        Number(primarySources.has(b.id)) - Number(primarySources.has(a.id)) ||
        Number(selectedSources.has(b.id)) - Number(selectedSources.has(a.id)) ||
        new Date(b.updated_at).getTime() - new Date(a.updated_at).getTime(),
    );
  const sourceScopes = new Map<string, any[]>();
  for (const task of acceptedEvidenceCandidates) {
    const base = task.execution?.baseWorkspace ?? '';
    sourceScopes.set(base, [...(sourceScopes.get(base) ?? []), task]);
  }
  const acceptedEvidence: any[] = [];
  for (
    let index = 0;
    acceptedEvidence.length < 5 && index < acceptedEvidenceCandidates.length;
    index++
  )
    for (const group of sourceScopes.values())
      if (group[index] && acceptedEvidence.length < 5) acceptedEvidence.push(group[index]);
  const evidenceTasks = [...acceptedEvidence, ...blocked].slice(0, 8);
  const taskBudget = Math.floor(18000 / Math.max(1, evidenceTasks.length));
  for (const task of evidenceTasks) {
    const accepted = task.state === 'accepted';
    const repair = accepted && task.execution?.workKind === 'repair';
    const refs = (
      await db.query(
        `SELECT a.id,a.name,a.digest,a.size,
      (SELECT passed FROM acceptances v WHERE v.run_id=a.run_id ORDER BY created_at DESC LIMIT 1) passed
      FROM artifacts a WHERE a.run_id=$1
      AND (a.name IN('verification.json','repair-report.json','protocol-report.json','report.json')
        OR a.name ~ '^report-[a-zA-Z0-9_.-]+\\.json$')
      ORDER BY CASE WHEN a.name=$2 THEN 0 ELSE 1 END,
        CASE a.name WHEN 'repair-report.json' THEN 0 WHEN 'verification.json' THEN 1
        WHEN 'protocol-report.json' THEN 2 ELSE 3 END,a.id
      LIMIT 1`,
        [
          task.last_run_id,
          repair
            ? 'repair-report.json'
            : accepted && task.execution?.workKind === 'diagnostic'
              ? 'protocol-report.json'
              : 'verification.json',
        ],
      )
    ).rows;
    for (const ref of refs) {
      if (accepted && ref.passed !== true) continue;
      if (evidenceCharacters >= 18000) break;
      try {
        if (ref.size > 8 * 1024 * 1024) throw Error('REPORT_TOO_LARGE');
        const artifact = await readArtifact(ref.id);
        if (artifact.bytes.length !== ref.size) throw Error('REPORT_SIZE_CHANGED');
        const report = JSON.parse(artifact.bytes.toString('utf8'));
        const remaining = Math.min(3500, taskBudget, 18000 - evidenceCharacters);
        let budget = Math.max(2, remaining - 300);
        let excerpt = managementReportExcerpt(report, budget);
        while (JSON.stringify(excerpt).length > remaining && budget > 2) {
          budget = Math.max(2, budget - (JSON.stringify(excerpt).length - remaining) - 10);
          excerpt = managementReportExcerpt(report, budget);
        }
        if (JSON.stringify(excerpt).length > remaining) break;
        evidenceCharacters += JSON.stringify(excerpt).length;
        evidence.push({
          taskId: task.id,
          runId: task.last_run_id,
          expectedAttempt: task.attempt_count,
          artifactId: ref.id,
          name: ref.name,
          digest: ref.digest,
          acceptancePassed: ref.passed,
          role: accepted
            ? repair
              ? 'accepted_repair'
              : task.execution?.workKind === 'diagnostic'
                ? 'accepted_diagnostic'
                : 'accepted_candidate'
            : 'pending_evidence',
          ...(accepted
            ? {
                consumers: consumersOf(task).map((t) => ({
                  taskId: t.id,
                  stage: t.stage,
                  state: t.state,
                  live: t.has_live_run,
                })),
                ...(repair ? { repairDeliveryTargets: repairDeliveryTargets(task) } : {}),
              }
            : {}),
          ...excerpt,
        });
      } catch {
        evidence.push({
          taskId: task.id,
          runId: task.last_run_id,
          artifactId: ref.id,
          name: ref.name,
          available: false,
        });
      }
    }
  }
  const planningDemandWindow=await readPlanningDemands(db,goalId);
  const taskContracts=projectManagerTaskContracts(tasks,workOpportunities.consumers,focus);
  return {
    decisionFeedback:await readManagementDecisionFeedback(db,goalId,{focusCaseId:focus?.caseId}),
    candidateValidations:await readCandidateValidations(db,goalId),
      candidateValidationEvidence:await readCandidateValidationEvidence(db,goalId),
    publishedRebases:await readPublishedRebases(db,goalId),
    requiredDecision:await readRequiredMaterialDecision(db,goalId,focus),
    compositions:(await readManagementCompositions(db,goalId,focus?.scope??null,scopedTaskIds)).map(({payload,...row})=>
      ({...row,...(payload?managementReportExcerpt(payload,2500):{}),
        recovery:{entity:'events',id:row.eventId,field:'payload'}})),
    deferredWorkOpportunities:await readWorkOpportunities(db,goalId,planningTasks,{deferred:true}),
    deferredDeliveries:(await db.query("SELECT to_regclass('public.deferred_handoffs') present")).rows[0].present?
      (await db.query(`SELECT id,state,producer_task_id,consumer_task_id,receipt->>'code' code FROM deferred_handoffs
        WHERE goal_id=$1 AND state<>'applied' ORDER BY created_at LIMIT 20`,[goalId])).rows:[],
    deliveryTransitions:await readDeliveryTransitions(db,goalId,focus),
    scopeRevisions:await readScopeRevisions(db,goalId,frontier.scopeSummary.filter(s=>s.pending>0 && (!focus?.scope || s.scope===focus.scope)).map(s=>s.scope)),
    planningDemands:planningDemandWindow.rows,
    planningDemandWindow:planningDemandWindow.selection,
    progress,
    recoveredContext: await readManagementRetrieval(db, goalId, readArtifact,focus),
    retainedEvidence:await readManagementEvidenceLedger(db,goalId,focus?.scope,focus,readArtifact),
    pool: poolHealth,
    workOpportunities,
    ...(focus?.kind==='live_failure'?{liveFailureReview:liveFailure?.failureKey===focus.failureKey?liveFailure:null}:{}),
    ...(focus ? {focus,coverageIndex:{scope:focus.scope,rows:coverageIndex.slice(0,500),
      complete:coverageIndex.length<=500,nextOffset:coverageIndex.length>500 ? 500 : null}} : {}),
    frontier:compactWorkFrontier(frontier),
    deliveryConsumptions:consumptionWindow.rows,
    deliveryConsumptionWindow:consumptionWindow.selection,
    backlog: backlogPlan
      ? {
          runId: backlogPlan.run_id,
          status: backlogPlan.state,
          reason: backlogPlan.reason,
          createdAt: backlogPlan.created_at,
          receipts: projectBacklogCapacityReceipts(backlogPlan.receipts ?? []),
        }
      : null,
    evidence,
    resources: {
      observedAt: new Date().toISOString(),
      global: settings.find((s) => s.id === 'capacity')?.value ?? null,
      remote: settings.find((s) => s.id === 'remote-capacity')?.value ?? null,
      objectiveRequestedCapacity: objective?.request?.requestedCapacity ?? null,
      objectiveEffectiveCapacity:(await db.query('SELECT value FROM settings WHERE id=$1',['objective-capacity:'+goalId])).rows[0]?.value?.maxRuns??objective?.request?.requestedCapacity??null,
      leases,
      activeRuns: capacity.reduce((n, c) => n + c.count, 0),
    },
    ...compactManagementSources(
      focusManagerTasks(tasks.map((t) =>
        closed.includes(t.state)
          ? {
              id: t.id,
              title: t.title,
              state: t.state,
              stage: t.stage,
              workKind: t.execution?.workKind,
              expectedAttempt: t.attempt_count,
              baseWorkspace: t.execution?.baseWorkspace,
              workspace: t.execution?.workspace,
              readOnlySource: t.execution?.readOnlySource,
              live: t.has_live_run,
              summary: t.summary?.slice(0, 120),
            }
          : {
              id: t.id,
              title: t.title,
              state: t.state,
              stage: t.stage,
              workKind: t.execution?.workKind,
              expectedAttempt: t.attempt_count,
              baseWorkspace: t.execution?.baseWorkspace,
              workspace: t.execution?.workspace,
              readOnlySource: t.execution?.readOnlySource,
              accountId: t.account_id,
              instructionRevisionId: t.instruction_revision_id,
              ...(t.guidance_available && t.last_run_state==='running' ? {guidanceAvailable:true,runId:t.last_run_id,runFence:t.run_fence,contextVersion:t.context_version} : {}),
              ...(t.execution?.verificationRecheck
                ? {
                    verificationRecheck: {
                      version: t.execution.verificationRecheck.version,
                      reason: t.execution.verificationRecheck.reason,
                      sourceTaskId: t.execution.verificationRecheck.sourceTaskId,
                      sourceRunId: t.execution.verificationRecheck.sourceRunId,
                      sourceAttempt: t.execution.verificationRecheck.sourceAttempt,
                      targetTaskId: t.execution.verificationRecheck.targetTaskId,
                      targetRunId: t.execution.verificationRecheck.targetRunId,
                      targetAttempt: t.execution.verificationRecheck.targetAttempt,
                      candidateDigest: t.execution.verificationRecheck.candidateDigest,
                      workspace: t.execution.verificationRecheck.workspace,
                    },
                  }
                : {}),
              model: t.model,
              dependencies: t.dependencies ?? [],
              covers: t.covers ?? [],
              ...(taskContracts.get(t.id)??{}),
              live: t.has_live_run,
              priority: t.priority,
              reason: t.reason?.slice(0, 400),
              summary: t.summary?.slice(0, closed.includes(t.state) ? 120 : 700),
              errorCode: t.error_code,
              routeOptions:
                !t.has_live_run && !closed.includes(t.state)
                  ? managementRoutes(t, accounts, policy)
                  : [],
            },
      ),focus),
    ),
    accounts: accounts
      .filter((a) => allowed.has(a.id))
      .map((a) => ({
        id: a.id,
        label: a.label,
        enabled: a.enabled,
        maxConcurrency: a.max_concurrency,
        current: capacity.filter((c) => c.account_id === a.id),
        quota: quotaFor(a, a.model),
      })),
    targets: {
      glmSessions: policy.glmTarget,
      glmPendingLimit: glmPendingLimit(policy),
      glmRequests: policy.glmRequestLimit ?? 6,
      muse: policy.museTarget,
      museSessions: policy.museTarget,
      musePendingLimit: musePendingLimit(policy),
      luna: policy.lunaTarget,
      sol: 1,
    },
    previous: previous
      ? {
          summary: previous.summary?.slice(0, 1600),
          capacity_reason: previous.capacity_reason?.slice(0, 1200),
          receipts: previous.receipts?.slice(0, 12),
          followThrough:(previous.receipts??[]).find((receipt:any)=>typeof receipt.code==='string' && receipt.code.startsWith('MANAGEMENT_') && typeof receipt.satisfied==='boolean')??null,
          created_at: previous.created_at,
        }
      : undefined,
  };
}

/** One transactional receipt per proposal. A changing/live task is never forced through. */
export async function applyManagementAction(
  goalId: string,
  runId: string,
  index: number,
  raw: unknown,
  inspect = inspectStopped,
) {
  if ((raw as any)?.kind === 'request_validation') return proposeCandidateValidation(goalId,runId,index,raw,inspect);
  if ((raw as any)?.kind === 'request_rebase') return requestPublishedRebase(goalId,runId,index,raw,inspect);
  if (guidanceSchema.safeParse(raw).success) return proposeRuntimeGuidance(goalId,runId,index,raw);
  const parsed = actionSchema.safeParse(raw);
  let proof: any;
  if (parsed.success && parsed.data.kind === 'route') {
    const t = (
      await pool.query('SELECT * FROM tasks WHERE id=$1 AND goal_id=$2', [
        parsed.data.taskId,
        goalId,
      ])
    ).rows[0];
    const r =
      t &&
      (
        await pool.query('SELECT * FROM runs WHERE task_id=$1 ORDER BY attempt DESC LIMIT 1', [
          t.id,
        ])
      ).rows[0];
    if (
      r &&
      !live.includes(t.state) &&
      !live.includes(r.state) &&
      t.attempt_count === parsed.data.expectedAttempt
    ) {
      try {
        proof = await inspect(r.id, t.execution?.kind === 'opencode');
      } catch {
        /* Receipt below keeps the operation moving. */
      }
    }
  }
  return tx(async (db) => {
    await lock(db);
    const existing = (
      await db.query('SELECT receipt FROM management_actions WHERE run_id=$1 AND action_index=$2', [
        runId,
        index,
      ])
    ).rows[0];
    if (existing) return existing.receipt;
    let code = 'INVALID_ACTION';
    let taskId: string | undefined;
    if (parsed.success) {
      const a = parsed.data;
      taskId = a.taskId;
      const goal = (
        await db.query(
          `SELECT g.*,s.value policy,m.value management,au.value autonomy FROM goals g
        LEFT JOIN settings s ON s.id='workforce:'||g.id LEFT JOIN settings m ON m.id='management:'||g.id
        LEFT JOIN settings au ON au.id='autonomy:'||g.project WHERE g.id=$1`,
          [goalId],
        )
      ).rows[0];
      const t = (
        await db.query('SELECT * FROM tasks WHERE id=$1 AND goal_id=$2', [a.taskId, goalId])
      ).rows[0];
      const r =
        t &&
        (
          await db.query('SELECT * FROM runs WHERE task_id=$1 ORDER BY attempt DESC LIMIT 1', [
            t.id,
          ])
        ).rows[0];
      const currentLive =
        t &&
        (
          await db.query(
            "SELECT 1 FROM runs WHERE task_id=$1 AND state IN('queued','running','cancelling','reconciling')",
            [t.id],
          )
        ).rowCount;
      const cancelled =
        r &&
        (
          await db.query(
            "SELECT 1 FROM events WHERE kind='CANCEL_REQUESTED' AND payload->>'runId'=$1",
            [r.id],
          )
        ).rowCount;
      if (
        !goal ||
        goal.state !== 'active' ||
        !goal.policy?.enabled ||
        !goal.management?.enabled ||
        goal.autonomy?.enabled === false
      )
        code = 'MANAGEMENT_DISABLED';
      else if (!t || t.stage === 'coordinate' || !t.execution || closed.includes(t.state))
        code = 'TASK_INELIGIBLE';
      else if (t.recovery?.mode === 'manual' && goal.autonomy?.enabled !== true)
        code = 'MANUAL_RECOVERY';
      else if (t.attempt_count !== a.expectedAttempt) code = 'TASK_CHANGED';
      else if (live.includes(t.state) || currentLive) code = 'TASK_LIVE';
      else if (cancelled) code = 'CANCEL_REQUESTED';
      else if (a.kind === 'prioritize') {
        await db.query('UPDATE tasks SET priority=$2 WHERE id=$1', [t.id, a.priority]);
        code = 'APPLIED';
      } else if (a.kind === 'route') {
        const accounts = (await db.query('SELECT * FROM accounts')).rows;
        const route = managementRoutes(t, accounts, goal.policy).find(
          (candidate) =>
            candidate.accountId === a.accountId &&
            candidate.model === a.model &&
            candidate.lane === a.lane,
        );
        if (!route) code = 'ROUTE_UNAVAILABLE';
        else {
          let stopped = !r;
          if (r) {
            try {
              assertStopped(proof, r, t);
              stopped = true;
            } catch {
              /* Uncertain processes remain untouched. */
            }
          }
          if (!stopped) code = 'STOP_UNCONFIRMED';
          else {
            await db.query(
              "UPDATE tasks SET account_id=$2,model=$3,execution=jsonb_set(execution,'{workforceLane}',to_jsonb($4::text)) WHERE id=$1",
              [t.id, a.accountId, a.model, a.lane],
            );
            code = 'APPLIED';
          }
        }
      } else {
        const pending = (
          await db.query(
            "SELECT 1 FROM supervision_cases WHERE task_id=$1 AND state IN('open','decided','delegated','waiting_evidence')",
            [t.id],
          )
        ).rowCount;
        if (pending) code = 'ALREADY_SUPERVISED';
        else if (
          !r ||
          !['failed', 'submitted'].includes(r.state) ||
          /AUTH|QUOTA|CORRUPT|DIGEST|INVALID_WRITE|CANCEL/.test(r.error_code ?? '')
        )
          code = 'NO_TECHNICAL_FAILURE';
        else {
          const created = await db.query(
            'INSERT INTO supervision_cases(id,goal_id,task_id,source_run_id,reason,evidence) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING RETURNING id',
            [
              randomUUID(),
              goalId,
              t.id,
              r.id,
              a.reason,
              JSON.stringify([
                { runId: r.id, attempt: r.attempt, code: r.error_code, summary: r.summary },
              ]),
            ],
          );
          if (!created.rowCount) code = 'ALREADY_SUPERVISED';
          else {
            await db.query("UPDATE tasks SET state='recovery_blocked' WHERE id=$1", [t.id]);
            code = 'APPLIED';
          }
        }
      }
    }
    const receipt = {
      index,
      kind: parsed.success ? parsed.data.kind : 'invalid',
      taskId,
      code,
      ...(parsed.success ? { reason: parsed.data.reason } : {}),
    };
    await db.query('INSERT INTO management_actions VALUES($1,$2,$3)', [
      runId,
      index,
      JSON.stringify(receipt),
    ]);
    await event(db, 'MANAGEMENT_ACTION', goalId, { runId, ...receipt });
    return receipt;
  });
}

const managementReviewSchema = z.object({
  summary: z.string().min(1).max(4000),
  capacityReason: z.string().min(1).max(3000),
  actions: z.array(z.unknown()).max(20),
  nextTransition: deliveryTransitionSchema.optional(),
  capacityAssessment: z.array(z.object({scope:z.string().min(1).max(200),
    taskIds:z.array(z.uuid()).max(30),reason:z.enum(['serial_dependency','missing_input','quota','resource','discovery_needed']),
    nextAction:z.string().min(10).max(1500)})).max(30).optional(),
  contextRequests: managementContextRequests.optional(),
  materialContextGap: materialContextGapSchema.optional(),
  liveFailureDisposition: z.object({failureKey:z.string().regex(/^[a-f0-9]{64}$/),eventId:z.string().regex(/^[1-9][0-9]{0,17}$/),
    decision:z.literal('continue_observing'),reason:z.string().min(40).max(1000)}).strict().optional(),
});

async function pendingManagementReviews(reviewRunId?:string) {
  return (
    await pool.query(`SELECT r.id,t.goal_id,a.id artifact_id,r.instruction_snapshot->'task'->>'description' frozen_description FROM runs r JOIN tasks t ON t.id=r.task_id
    JOIN artifacts a ON a.run_id=r.id AND a.name='coordination.json'
    JOIN settings s ON s.id='management:'||t.goal_id AND s.value->>'enabled'='true'
    WHERE t.stage='coordinate' AND EXISTS(SELECT 1 FROM acceptances v WHERE v.run_id=r.id AND v.passed)
    AND NOT EXISTS(SELECT 1 FROM management_reviews m WHERE m.run_id=r.id)
    AND ($1::uuid IS NULL OR r.id=$1::uuid)
    AND r.created_at>=COALESCE((s.value->>'enabledAt')::timestamptz,'infinity'::timestamptz)
    ORDER BY r.created_at LIMIT 10`,[reviewRunId??null])
  ).rows;
}

/** A completed review must reach its durable consumer before another model call.
 * Failed calls and reviews predating management remain eligible for normal retry. */
export async function hasPendingManagementReview(db:DB,runId:string|null,enabledAt?:string) {
  if(!runId || !enabledAt)return false;
  return !!(await db.query(`SELECT 1 FROM runs r WHERE r.id=$1 AND r.state='submitted'
    AND r.created_at >= $2::timestamptz
    AND EXISTS(SELECT 1 FROM acceptances a WHERE a.run_id=r.id AND a.passed)
    AND EXISTS(SELECT 1 FROM artifacts a WHERE a.run_id=r.id AND a.name='coordination.json')
    AND NOT EXISTS(SELECT 1 FROM management_reviews m WHERE m.run_id=r.id)`,[runId,enabledAt])).rowCount;
}

/** Bind accepted intent before supervision advances the referenced attempt.
 * Artifact IO is outside admission; the existing owner rechecks all authority.
 * The immutable per-run receipt also makes replay after a crash harmless. */
export async function recordManagementTransitions(reviewRunId?:string) {
  for(const r of await pendingManagementReviews(reviewRunId)) {
    let proposal:unknown;
    try {
      proposal=JSON.parse((await readArtifact(r.artifact_id)).bytes.toString()).management;
    } catch { continue; } // The ordinary consumer persists MANAGEMENT_FORMAT.
    const parsed=managementReviewSchema.safeParse(proposal);
    if(parsed.success && parsed.data.nextTransition)
      await tx(db=>recordDeliveryTransition(db,r.goal_id,r.id,parsed.data.nextTransition));
  }
}

export async function recordManagementReviews(frontierReader:typeof readObjectiveFrontier=readObjectiveFrontier) {
  const rows=await pendingManagementReviews();
  for (const r of rows) {
    let parsed: any;
    let coordination:any;
    let focusScope:string | undefined;
    let contextRequestFocus:any=null;
    try {
      const start=r.frozen_description?.lastIndexOf('\n{"cases":') ?? -1;
      if(start>=0){contextRequestFocus=JSON.parse(r.frozen_description.slice(start+1)).operation?.focus??null;
        focusScope=contextRequestFocus?.scope ?? undefined;}
    }catch { /* Older reviews use the original global assessment contract. */ }
    try {
      coordination=JSON.parse((await readArtifact(r.artifact_id)).bytes.toString());
      parsed = coordination.management;
    } catch {
      /* Record invalid contract; do not silently repeat. */
    }
    const valid = managementReviewSchema.safeParse(parsed);
    const retrievalArtifacts=new Map<string,Awaited<ReturnType<typeof readArtifact>>>();
    const cachedArtifact=async(id:string)=>{
      const existing=retrievalArtifacts.get(id);
      if(existing)return existing;
      const artifact=await readArtifact(id);retrievalArtifacts.set(id,artifact);return artifact;
    };
    const receipts: Record<string, unknown>[] = [];
    if(valid.success && valid.data.nextTransition)
      receipts.push(await tx(db=>recordDeliveryTransition(db,r.goal_id,r.id,valid.data.nextTransition)));
    if (valid.success && valid.data.contextRequests?.length)
      receipts.push({ code: 'CONTEXT_REQUESTED', count: valid.data.contextRequests.length,
        reason: 'Leituras limitadas do próprio objetivo serão recuperadas na próxima revisão.' });
    if (valid.success)
      for (const [i, a] of valid.data.actions.entries())
        receipts.push(await applyManagementAction(r.goal_id, r.id, i, a));
    else
      receipts.push({
        code: 'MANAGEMENT_FORMAT',
        ...managementFormatFeedback(valid.error.issues),
        reason:
          'A próxima revisão deve corrigir os paths/códigos em issues conforme o contrato de management no coordination.json; a recusa não criou trabalho.',
      });
    if(valid.success && valid.data.liveFailureDisposition){
      if(valid.data.actions.length || valid.data.contextRequests?.length)receipts.push({code:'LIVE_FAILURE_DISPOSITION_CONFLICT'});
      else receipts.push(await tx(async db=>{await lock(db);
        return recordLiveFailureNoAction(db,r.goal_id,r.id,0,valid.data.liveFailureDisposition,contextRequestFocus);}));
    }
    // This is an analytical observation, not an admission/acceptance decision.
    // Reading hundreds of frozen contracts held the global scheduler lock for
    // tens of seconds. Give it one dated, read-only snapshot before the short
    // write transaction; all action and transition guards stay in their owners.
    const frontierObservation=await tx(async db=>{
      await db.query('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY');
      const observedAt=(await db.query('SELECT transaction_timestamp() observed_at')).rows[0].observed_at;
      const frontier=await frontierReader(db,r.goal_id);
      const operation=await collectOperationProgress(db,r.goal_id,readArtifact);
      const policy=(await db.query('SELECT value FROM settings WHERE id=$1',['workforce:'+r.goal_id])).rows[0]?.value;
      const health=policy?await readWorkforceHealth(db,r.goal_id,policy):null;
      const ordinaryFollowThrough=await readManagementFollowThrough(db,r.goal_id,r.id);
      const materialDecision=await readMaterialDecisionOutcome(db,r.goal_id,r.id,r.frozen_description??'',coordination);
      // An unrelated ready task, tracking transition or no_work receipt cannot
      // discharge this review's frozen focal material obligation. Canonical
      // supervision/admission owners, not proposed JSON, prove its disposition.
      const followThrough=materialDecision??ordinaryFollowThrough;
      // Resolve explicit requests before acquiring the global scheduler mutex.
      // The ledger receives only these observed bytes inside the write phase.
      const results=[];
      for(const request of valid.success?valid.data.contextRequests??[]:[])
        results.push(await readManagementRetrievalRequest(db,r.goal_id,cachedArtifact,request));
      for(const result of results)if(result.status==='OK'&&result.artifactId){
        const artifact=await cachedArtifact(result.artifactId);
        if(createHash('sha256').update(artifact.bytes).digest('hex')!==result.digest)throw Error('MANAGEMENT_EVIDENCE_CONFLICT');
      }
      return {observedAt:new Date(observedAt).toISOString(),frontier,operation,health,followThrough,materialDecision,requestedEvidence:{results}};
    });
    await tx(async (db) => {
      await lock(db);
      const saved = await db.query(
        'INSERT INTO management_reviews(run_id,goal_id,summary,capacity_reason,receipts) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING RETURNING run_id',
        [
          r.id,
          r.goal_id,
          valid.success ? valid.data.summary : 'Revisão sem contrato de gestão válido.',
          valid.success ? valid.data.capacityReason : 'Aguardando revisão válida.',
          JSON.stringify(receipts),
        ],
      );
      if (!saved.rowCount) return;
      if(valid.success && valid.data.contextRequests?.length) {
        // Resolve this accepted review's requests now, so the next fresh worker
        // can reuse the exact evidence even after the manager process ended.
        await db.query('INSERT INTO settings VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET value=settings.value||excluded.value',
          ['management-state:'+r.goal_id,JSON.stringify({contextRequests:valid.data.contextRequests,contextRequestFocus})]);
        await retainManagementEvidence(db,r.goal_id,frontierObservation.requestedEvidence,async id=>{
          const artifact=retrievalArtifacts.get(id);
          if(!artifact)throw Error('MANAGEMENT_EVIDENCE_NOT_OBSERVED');
          return artifact;
        },contextRequestFocus);
      }
      const progress = await recordOperationProgress(db, r.goal_id, r.id, readArtifact,
        {observedAt:frontierObservation.observedAt,observation:frontierObservation.operation});
      const previousFrontier=(await db.query('SELECT value FROM settings WHERE id=$1 FOR UPDATE',
        ['work-frontier:'+r.goal_id])).rows[0]?.value;
      const olderFrontier=previousFrontier?.observedAt &&
        Date.parse(frontierObservation.observedAt)<Date.parse(previousFrontier.observedAt);
      const frontier:Awaited<ReturnType<typeof readObjectiveFrontier>> = olderFrontier ? previousFrontier : frontierObservation.frontier;
      const frontierObservedAt=olderFrontier ? previousFrontier.observedAt : frontierObservation.observedAt;
      const priorReceipts:string[]=previousFrontier?.knownReceipts ?? [];
      const advanced=frontier.receipts.some(receipt=>!priorReceipts.includes(receipt));
      const unchangedReviews=advanced ? 0 : (previousFrontier?.unchangedReviews ?? 0)+1;
      await db.query('INSERT INTO settings VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET value=excluded.value',
        ['work-frontier:'+r.goal_id,JSON.stringify({...frontier,knownReceipts:[...new Set([...priorReceipts,...frontier.receipts])],
          unchangedReviews,reviewRunId:r.id,observedAt:frontierObservedAt,reviewedAt:new Date().toISOString()})]);
      const health=frontierObservation.health;
      const proposed=(await db.query('SELECT task_ids FROM workforce_backlog_plans WHERE run_id=$1',[r.id])).rows[0];
      const assessment=valid.success ? valid.data.capacityAssessment ?? [] : [];
      const uncovered=frontier.scopeSummary.filter(scope=>(!focusScope || scope.scope===focusScope) && scope.pending>0 && !assessment.some(a=>
        a.scope===scope.scope && a.taskIds.length>0 && a.taskIds.every(id=>frontier.entries.some(e=>e.taskId===id && e.scope===a.scope))));
      if(health?.needsPlanning && !(valid.success && valid.data.actions.length) &&
        !proposed?.task_ids?.length && !(Array.isArray(coordination?.backlog?.tasks) && coordination.backlog.tasks.length) && uncovered.length)
        receipts.push({code:'CAPACITY_ASSESSMENT_REQUIRED',scopes:uncovered.map(s=>s.scope),
          reason:'Identifique critérios/insumos e próxima ação por escopo; no_work sem cobertura não encerra a investigação.'});
      if(!frontier.complete && unchangedReviews>=3)
        receipts.push({code:'DELIVERY_STAGNATION',unchangedReviews,
          reason:'Nenhum critério final novo foi aceito; atividade, memória e relatórios auxiliares não reiniciam este contador.'});
      const stagnant=progress.stagnation.detected || (!frontier.complete && unchangedReviews>=3);
      if(stagnant || frontierObservation.materialDecision)
        receipts.push({...frontierObservation.followThrough,observedAt:frontierObservation.observedAt});
      if ((stagnant || frontierObservation.materialDecision) && !frontierObservation.followThrough.satisfied) {
        receipts.push({
          code: 'OPERATION_REPLAN_REQUIRED',
          reason: frontierObservation.followThrough.nextAction,
        });
        await db.query('UPDATE management_reviews SET receipts=$2 WHERE run_id=$1', [
          r.id,
          JSON.stringify(receipts),
        ]);
      }
      const value = {
        reviewedAt: new Date().toISOString(),
        frontierObservedAt,
        operationObservedAt:progress.observedAt,
        runId: r.id,
        summary: valid.success ? valid.data.summary : 'Revisão sem contrato de gestão válido.',
        capacityReason: valid.success ? valid.data.capacityReason : '',
        receipts,
        nextTransition: valid.success ? (valid.data.nextTransition ?? null) : null,
        followThrough:frontierObservation.followThrough,
        capacityAssessment:assessment,
        contextRequests: valid.success ? valid.data.contextRequests ?? [] : [],
        contextRequestFocus,
      };
      await db.query('UPDATE management_reviews SET receipts=$2 WHERE run_id=$1',[r.id,JSON.stringify(receipts)]);
      await db.query(
        'INSERT INTO settings VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET value=settings.value||excluded.value',
        ['management-state:' + r.goal_id, JSON.stringify(value)],
      );
      await event(db, 'MANAGEMENT_REVIEWED', r.goal_id, value);
    });
  }
}
