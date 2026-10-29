import { managerAuthoritySql } from './manager-authority.js';
import { createHash, randomUUID } from 'node:crypto';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { z } from 'zod';
import { pool, tx, lock, event, type DB } from './db.js';
import { config } from './config.js';
import { readArtifact } from './service.js';
import {
  frameworkProjectAuthorization,
  verifyAcceptedManagementAction,
} from './candidate-validation.js';
import { assertStopped } from './continuation.js';
import { mergeSupervisorInputs } from './execution-inputs.js';
import { actionSchema } from './supervision-actions.js';
import {verifyWorkerValidationAuthority,requiredDatabaseTestSchema} from './worker-validation.js';
import {readDeliverySourceIdentity,sourceIdentityFrozen} from './delivery-source-resolution.js';

const sha = (value: Buffer | string) => createHash('sha256').update(value).digest('hex');
const canonical = (v: any): any =>
  Array.isArray(v)
    ? v.map(canonical)
    : v && typeof v === 'object'
      ? Object.fromEntries(
          Object.keys(v)
            .sort()
            .map((k) => [k, canonical(v[k])]),
        )
      : v;
const digest = (v: any) => sha(JSON.stringify(canonical(v)));
const registeredShas = ['e66fcf513aaa513ff2216d87556fb5dba771597dc933ca4b4b38573d8842d5b5',
  '02a92158a4084a1aeb3b4c55998b6740d90428f80acb06a225730a1423ba9c26',
  '640557b086101b38566fbb9dd1361015f9e22ac31e5526caa43d7b176c1a6abe'] as const;
const key = (id: string) => 'candidate-validation-evidence:' + id;
const uuid = z.uuid(),
  hash = z.string().regex(/^[a-f0-9]{64}$/);
const inputSchema = z
  .object({
    name: z.enum(['ci-receipt.json', 'validation.log']),
    mime: z.enum(['application/json', 'text/plain']),
    digest: hash,
    size: z
      .number()
      .int()
      .min(0)
      .max(8 * 1024 * 1024),
    base64: z.string().max(12 * 1024 * 1024),
  })
  .strict();
const exportSchema = z
  .object({
    version: z.literal(1),
    actionId: uuid,
    requestDigest: hash,
    resultDigest: hash,
    receiptDigest: hash,
    authority: z
      .object({
        version: z.literal(1),
        kind: z.literal('registered_candidate_validation'),
        actionId: uuid,
        requestDigest: hash,
        resultDigest: hash,
        receiptDigest: hash,
        producer: z
          .object({
            goalId: uuid,
            taskId: uuid,
            runId: uuid,
            runFence: uuid,
            contextVersion: uuid,
            expectedAttempt: z.number().int().positive(),
            candidateDigest: hash,
            contractDigest: hash,
          })
          .strict(),
        outcome: z
          .object({
            passed: z.boolean(),
            code: z.enum([
              'VALIDATION_PASSED',
              'VALIDATION_CHECKS_FAILED',
              'VALIDATION_BASE_CHANGED',
            ]),
            state: z.literal('completed'),
          })
          .strict(),
        registeredActionSha: z.enum(registeredShas),
        coverage:z.object({version:z.literal(1),source:z.literal('registered_vitest_json'),
          profile:z.literal('agentos.validate'),candidateDigest:hash,obligationsDigest:hash,
          reportDigest:hash,requiredTests:z.array(requiredDatabaseTestSchema.extend({
            status:z.enum(['passed','failed','skipped','missing'])}).strict()).min(1).max(128)}).strict().optional(),
        registeredCommand: z.tuple([
          z.literal('/usr/bin/python3'),
          z.literal('/opt/agentos-control/agentos-release-action.py'),
          z.literal('validate'),
        ]),
        publishedBaselineDigest: hash,
        publicationProofDigest: hash,
        validationProofDigest: hash,
        registeredReceiptSha: hash,
        validatedReleaseDigest: hash.nullable(),
        log: z
          .object({
            rawDigest: hash,
            rawBytes: z
              .number()
              .int()
              .min(0)
              .max(8 * 1024 * 1024),
            digest: hash,
            bytes: z
              .number()
              .int()
              .min(0)
              .max(8 * 1024 * 1024),
            sanitized: z.literal(true),
            completeRegisteredArtifact: z.literal(true),
            source: z.literal('registered_action_validation.log'),
            commandStdoutMayBeTruncatedByRegisteredAction: z.literal(true),
          })
          .strict(),
        acceptance: z.literal(false),
      })
      .strict(),
    inputs: z.array(inputSchema).length(2),
  })
  .strict();
type Export = z.infer<typeof exportSchema>;
type Dispatch = (request: any) => Promise<any>;
const proposal = (v: any) =>
  Object.fromEntries(
    [
      'kind',
      'taskId',
      'expectedAttempt',
      'runId',
      'runFence',
      'contextVersion',
      'candidateArtifactId',
      'candidateArtifactDigest',
      'candidateDigest',
      'reason',
    ].map((k) => [k, v[k]]),
  );
const failure = (code: string) => {
  throw Error(code);
};

/** Only controller-origin evidence can be delivered. No historical run gains artifacts. */
export async function installCandidateValidationEvidence(db: DB = pool) {
  await db.query(`CREATE OR REPLACE FUNCTION agentos_ci_evidence_immutable() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
    IF TG_OP<>'INSERT' AND OLD.id LIKE 'candidate-validation-evidence:%' THEN
      IF TG_OP='DELETE' OR NEW.id<>OLD.id OR (OLD.value->>'state'='published' AND NEW.value IS DISTINCT FROM OLD.value) OR
        (NEW.value-'state'-'claimId'-'claimedAt'-'nextProbeAt'-'updatedAt'-'code'-'body'-'scopeDigest'-'criteriaDigest'-'contextDependencyDigest') IS DISTINCT FROM
        (OLD.value-'state'-'claimId'-'claimedAt'-'nextProbeAt'-'updatedAt'-'code'-'body'-'scopeDigest'-'criteriaDigest'-'contextDependencyDigest') OR
        (OLD.value ? 'scopeDigest' AND NEW.value->'scopeDigest' IS DISTINCT FROM OLD.value->'scopeDigest') OR
        (OLD.value ? 'criteriaDigest' AND NEW.value->'criteriaDigest' IS DISTINCT FROM OLD.value->'criteriaDigest') OR
        (OLD.value ? 'contextDependencyDigest' AND NEW.value->'contextDependencyDigest' IS DISTINCT FROM OLD.value->'contextDependencyDigest') OR
        (OLD.value ? 'body' AND NEW.value->'body' IS DISTINCT FROM OLD.value->'body')
        THEN RAISE EXCEPTION 'CI_EVIDENCE_IMMUTABLE'; END IF;
    END IF; IF TG_OP='DELETE' THEN RETURN OLD; END IF; RETURN NEW; END $$;
    DROP TRIGGER IF EXISTS agentos_ci_evidence_immutable ON settings;
    CREATE TRIGGER agentos_ci_evidence_immutable BEFORE UPDATE OR DELETE ON settings FOR EACH ROW EXECUTE FUNCTION agentos_ci_evidence_immutable();`);
}

const scopeExecutionOmissions=[
    'workspace',
    'sourceWorkspace',
    'snapshot',
    'baseDigest',
    'inputs',
    'supervision',
    'supervisionHandoff',
    'repairDeliveryContext',
    'revisionFeedback',
    'continuation',
    'publishedRebase',
    'verificationRecheck',
    'candidateValidationEvidence',
    'workforceLane',
  ];
const scopeTaskFields=['id','goal_id','stage','criteria','covers','agent_id','resources','required_memories','allowed_accounts'];
function scope(task: any) {
  const execution = { ...task.execution };
  for (const name of scopeExecutionOmissions)
    delete execution[name];
  return digest({
    id: task.id,
    goalId: task.goal_id,
    stage: task.stage,
    criteria: task.criteria,
    covers: task.covers,
    execution,
    agentId: task.agent_id,
    resources: task.resources,
    requiredMemories: task.required_memories,
    allowedAccounts: task.allowed_accounts,
  });
}
async function source(db: DB, actionId: string,mode:'current'|'historical_runtime_evidence'='current') {
  uuid.parse(actionId);
  const v = (
    await db.query('SELECT value FROM settings WHERE id=$1', ['candidate-validation:' + actionId])
  ).rows[0]?.value;
  if (
    !v ||
    v.actionId !== actionId ||
    v.state !== 'completed' ||
    !v.result?.resultDigest ||
    !['VALIDATION_PASSED', 'VALIDATION_CHECKS_FAILED', 'VALIDATION_BASE_CHANGED'].includes(
      v.result?.code,
    )
  )
    failure('CI_EVIDENCE_NOT_COMPLETED');
  if(v.authority==='runtime_artifact') await verifyWorkerValidationAuthority(db,v,
    mode==='historical_runtime_evidence'?'historical_evidence':'current');
  else await verifyAcceptedManagementAction(db, v.goalId, v.managerRunId, v.index, proposal(v));
  if(mode!=='historical_runtime_evidence' || v.authority!=='runtime_artifact') {
    const authorization = await frameworkProjectAuthorization(db, v.goalId);
    if (digest(authorization.authorization) !== digest(v.request.frameworkAuthorization))
      failure('CI_EVIDENCE_AUTHORIZATION_CHANGED');
  }
  const r = (
    await db.query(
      `SELECT r.id,r.task_id,r.fence,r.context_id,r.attempt,r.state,r.finished_at,t.goal_id,
        jsonb_typeof(r.instruction_snapshot->'task')='object' frozen_task_present,
        (SELECT COALESCE(jsonb_object_agg(k,val),'{}'::jsonb)
          FROM jsonb_each(r.instruction_snapshot->'task') e(k,val) WHERE k=ANY($3::text[]))
        ||CASE WHEN (r.instruction_snapshot->'task') ? 'execution' THEN jsonb_build_object('execution',
          (r.instruction_snapshot#>'{task,execution}')-$2::text[]) ELSE '{}'::jsonb END scope_task
        FROM runs r JOIN tasks t ON t.id=r.task_id WHERE r.id=$1`,
      [v.runId,scopeExecutionOmissions,scopeTaskFields],
    )
  ).rows[0];
  if (
    !r ||
    r.goal_id !== v.goalId ||
    r.task_id !== v.taskId ||
    r.fence !== v.runFence ||
    r.context_id !== v.contextVersion ||
    !r.frozen_task_present ||
    r.attempt !== v.expectedAttempt ||
    !['submitted', 'failed'].includes(r.state) ||
    !r.finished_at
  )
    failure('CI_EVIDENCE_PRODUCER_CHANGED');
  const candidate = await readArtifact(v.candidateArtifactId);
  if (
    candidate.run_id !== r.id ||
    candidate.name !== 'candidate.json' ||
    candidate.digest !== v.candidateArtifactDigest ||
    candidate.size !== v.request.candidateSize ||
    candidate.bytes.toString('base64') !== v.request.candidateBase64
  )
    failure('CI_EVIDENCE_CANDIDATE_CHANGED');
  const context = (
    await db.query("SELECT manifest->'dependencyArtifacts' dependencies FROM contexts WHERE id=$1 AND run_id=$2", [r.context_id, r.id])
  ).rows[0];
  if (!context) failure('CI_EVIDENCE_PRODUCER_CHANGED');
  return {
    v,
    run: r,
    scopeDigest: scope(r.scope_task),
    criteriaDigest: digest(r.scope_task.criteria),
    contextDependencyDigest: digest(context.dependencies ?? []),
  };
}

async function fetchEvidence(request: any) {
  const script = fileURLToPath(
    new URL('../scripts/agentos-candidate-validation-evidence.py', import.meta.url),
  );
  if (
    !config.bridgeHost ||
    !/^\/opt\/agentos\/releases\/managed-[0-9a-f-]{36}\/scripts\/agentos-candidate-validation-evidence.py$/.test(
      script,
    )
  )
    failure('CI_EVIDENCE_UNDEPLOYED');
  return new Promise<any>((resolve, reject) => {
    const child = spawn(
      'ssh',
      [
        '-T',
        '-o',
        'BatchMode=yes',
        '-o',
        'ConnectTimeout=8',
        config.bridgeHost,
        '/usr/bin/python3',
        script,
        'evidence',
      ],
      { windowsHide: true, stdio: ['pipe', 'pipe', 'pipe'] },
    );
    let stdout = '';
    const timer = setTimeout(() => child.kill('SIGTERM'), 45000);
    child.stdin.on('error', () => {});
    child.stdin.end(JSON.stringify(request));
    child.stderr.resume();
    child.stdout.on('data', (b) => {
      stdout += b.toString();
      if (stdout.length > 24 * 1024 * 1024) child.kill('SIGTERM');
    });
    child.once('error', () => {
      clearTimeout(timer);
      reject(Error('CI_EVIDENCE_TRANSPORT_FAILED'));
    });
    child.once('close', (code) => {
      clearTimeout(timer);
      try {
        if (code !== 0) throw Error();
        resolve(JSON.parse(stdout));
      } catch {
        reject(Error('CI_EVIDENCE_TRANSPORT_FAILED'));
      }
    });
  });
}

function checkedExport(raw: any, current: Awaited<ReturnType<typeof source>>) {
  const value = exportSchema.parse(raw),
    v = current.v,
    authority = value.authority;
  const expectedProducer = Object.fromEntries(
    [
      'goalId',
      'taskId',
      'runId',
      'runFence',
      'contextVersion',
      'expectedAttempt',
      'candidateDigest',
    ].map((k) => [k, v[k]]),
  );
  expectedProducer.contractDigest = v.request.contractDigest;
  if (
    value.actionId !== v.actionId ||
    value.requestDigest !== digest(v.request) ||
    value.resultDigest !== v.result.resultDigest ||
    authority.actionId !== value.actionId ||
    authority.requestDigest !== value.requestDigest ||
    authority.resultDigest !== value.resultDigest ||
    authority.receiptDigest !== value.receiptDigest ||
    digest(authority.producer) !== digest(expectedProducer) ||
    authority.outcome.passed !== v.result.passed ||
    authority.outcome.code !== v.result.code ||
    authority.publishedBaselineDigest !== v.result.publishedBaselineDigest ||
    authority.registeredReceiptSha !== v.result.registeredReceiptSha
  )
    failure('CI_EVIDENCE_RESULT_UNBOUND');
  const names = new Set(value.inputs.map((i) => i.name));
  if (names.size !== 2) failure('CI_EVIDENCE_INPUTS_INVALID');
  for (const input of value.inputs) {
    const bytes = Buffer.from(input.base64, 'base64');
    if (
      bytes.toString('base64') !== input.base64 ||
      bytes.length !== input.size ||
      sha(bytes) !== input.digest
    )
      failure('CI_EVIDENCE_BYTES_CHANGED');
  }
  const proof = value.inputs.find((i) => i.name === 'ci-receipt.json')!,
    log = value.inputs.find((i) => i.name === 'validation.log')!;
  if (
    proof.mime !== 'application/json' ||
    log.mime !== 'text/plain' ||
    Buffer.from(proof.base64, 'base64').toString() !== JSON.stringify(canonical(authority)) ||
    log.digest !== authority.log.digest ||
    log.size !== authority.log.bytes
  )
    failure('CI_EVIDENCE_BYTES_CHANGED');
  return value;
}

/** Controller-only evidence can gate this exact current attempt. Historical
 * artifacts and acceptances remain untouched; exported receipts are not worker reports. */
export async function readRuntimeCandidateValidationEvidence(db:DB,actionId:string) {
  const job=(await db.query('SELECT value FROM settings WHERE id=$1',[key(actionId)])).rows[0]?.value;
  if(job?.state!=='published') return null;
  const current=await source(db,actionId),v=current.v;
  if(v.authority!=='runtime_artifact' || v.result.stale===true) failure('CI_EVIDENCE_AUTHORIZATION_CHANGED');
  const target=await readDeliverySourceIdentity(db,v.taskId);
  if(!target || !sourceIdentityFrozen(target) || target.live || target.leased || target.cancelled ||
    target.run_id!==v.runId || target.fence!==v.runFence || target.contract_digest!==v.request.contractDigest)
    failure('CI_EVIDENCE_PRODUCER_CHANGED');
  const body=checkedExport({...job.body,inputs:job.body.inputs.map(({id,source,...input}:any)=>input)},current);
  if(job.requestDigest!==body.requestDigest || job.resultDigest!==body.resultDigest ||
    job.scopeDigest!==current.scopeDigest || job.criteriaDigest!==current.criteriaDigest ||
    job.contextDependencyDigest!==current.contextDependencyDigest) failure('CI_EVIDENCE_PUBLICATION_CHANGED');
  return body;
}

/** Feed a future repair through existing immutable execution inputs. This never
 * adds artifacts to the producer or impersonates a manager's revise action. */
export async function runtimeValidationRepairInputs(db:DB,goalId:string,taskIds:string[],covers:string[]) {
  if(!taskIds.length || !covers.length) return [];
  const rows=(await db.query(`SELECT e.value FROM settings e JOIN tasks t ON t.id::text=e.value->>'taskId'
    JOIN settings v ON v.id='candidate-validation:'||(e.value->>'actionId')
    WHERE e.id LIKE 'candidate-validation-evidence:%' AND e.value->>'state'='published'
      AND t.goal_id=$1 AND t.id=ANY($2::uuid[]) AND t.covers ?| $3::text[]
      AND v.value->>'authority'='runtime_artifact'
      AND EXISTS(SELECT 1 FROM acceptances a WHERE a.run_id::text=v.value->>'runId' AND NOT a.passed)
    ORDER BY e.value->>'updatedAt' DESC,e.id LIMIT 1`,[goalId,taskIds,covers])).rows;
  if(!rows.length) return [];
  const job=rows[0].value,current=await source(db,job.actionId,'historical_runtime_evidence');
  const body=checkedExport({...job.body,inputs:job.body.inputs.map(({id,source,...input}:any)=>input)},current);
  if(job.requestDigest!==body.requestDigest || job.resultDigest!==body.resultDigest ||
    job.criteriaDigest!==current.criteriaDigest || job.scopeDigest!==current.scopeDigest)
    failure('CI_EVIDENCE_PUBLICATION_CHANGED');
  // This small immutable-derived input keeps historical proof useful after
  // revocation without representing it as authority for the next candidate.
  const note=Buffer.from(JSON.stringify({version:1,kind:'historical_candidate_ci',diagnosticOnly:true,
    authorizesExecution:false,authorizesAcceptance:false,actionId:job.actionId,producer:body.authority.producer,
    originalPolicyRegistrationId:current.v.request.runtimeAuthority.policyRegistration.registrationId,
    originalPolicyDigest:current.v.request.runtimeAuthority.policyRegistration.policyDigest,receiptDigest:body.receiptDigest}));
  const noteHash=sha('ci-diagnostic-scope:'+job.actionId),id=noteHash.slice(0,8)+'-'+noteHash.slice(8,12)+
    '-5'+noteHash.slice(13,16)+'-8'+noteHash.slice(17,20)+'-'+noteHash.slice(20,32);
  return [...job.body.inputs,{id,name:'ci-diagnostic-scope.json',mime:'application/json',digest:sha(note),
    size:note.length,base64:note.toString('base64'),source:'registered_external_ci_diagnostic'}];
}

/** Durable publication is retryable because the RPC is strictly read-only.
 * IDs are allocated once before it. A lost DB reply reuses the same bytes/IDs. */
export async function reconcileCandidateValidationEvidence(dispatch: Dispatch = fetchEvidence) {
  const rows = (
    await pool.query(`SELECT s.value FROM settings s LEFT JOIN settings j ON j.id='candidate-validation-evidence:'||(s.value->>'actionId')
    WHERE s.id LIKE 'candidate-validation:%' AND s.value->>'state'='completed' AND s.value#>>'{result,resultDigest}' IS NOT NULL
    AND j.value->>'state' IS DISTINCT FROM 'published' ORDER BY COALESCE(j.value->>'updatedAt',''),s.id LIMIT 20`)
  ).rows;
  for (const row of rows) {
    let claim: any;
    try {
      const current = await source(pool, row.value.actionId);
      claim = await tx(async (db) => {
        await lock(db);
        const previous = (
          await db.query('SELECT value FROM settings WHERE id=$1 FOR UPDATE', [
            key(current.v.actionId),
          ])
        ).rows[0]?.value;
        if (
          previous?.state === 'published' ||
          (previous?.state === 'reading' && Date.now() - Date.parse(previous.claimedAt) < 60000) ||
          (previous?.nextProbeAt && Date.now() < Date.parse(previous.nextProbeAt))
        )
          return null;
        const identity = {
          version: 1,
          actionId: current.v.actionId,
          goalId: current.v.goalId,
          taskId: current.v.taskId,
          requestDigest: digest(current.v.request),
          resultDigest: current.v.result.resultDigest,
          scopeDigest: current.scopeDigest,
          criteriaDigest: current.criteriaDigest,
          contextDependencyDigest: current.contextDependencyDigest,
          inputIds: previous?.inputIds ?? [randomUUID(), randomUUID()],
        };
        if (
          previous &&
          Object.entries(identity).some(
            ([k, v]) => previous[k] !== undefined && digest(previous[k]) !== digest(v),
          )
        )
          failure('CI_EVIDENCE_PUBLICATION_CHANGED');
        const next = {
          ...identity,
          state: 'reading',
          claimId: randomUUID(),
          claimedAt: new Date().toISOString(),
        };
        await db.query(
          'INSERT INTO settings(id,value) VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET value=excluded.value',
          [key(next.actionId), JSON.stringify(next)],
        );
        return next;
      });
      if (!claim) continue;
      const exported = checkedExport(await dispatch(current.v.request), current);
      await tx(async (db) => {
        await lock(db);
        const fresh = await source(db, claim.actionId);
        const job = (
          await db.query('SELECT value FROM settings WHERE id=$1 FOR UPDATE', [key(claim.actionId)])
        ).rows[0]?.value;
        if (job?.state === 'published' || job?.claimId !== claim.claimId) return;
        checkedExport(exported, fresh);
        const body = {
          ...exported,
          inputs: exported.inputs.map((input, index) => ({
            ...input,
            id: job.inputIds[index],
            source: 'registered_external_ci',
          })),
        };
        await db.query('UPDATE settings SET value=$2 WHERE id=$1', [
          key(job.actionId),
          JSON.stringify({ ...job, state: 'published', body, updatedAt: new Date().toISOString() }),
        ]);
        await event(db, 'CANDIDATE_VALIDATION_EVIDENCE_PUBLISHED', job.taskId, {
          actionId: job.actionId,
          goalId: job.goalId,
          resultDigest: job.resultDigest,
          inputRefs: body.inputs.map(({ base64, ...ref }) => ref),
          acceptance: false,
        });
      });
    } catch (error: any) {
      if (!claim) {
        const observed = row.value;
        if (
          !uuid.safeParse(observed?.actionId).success ||
          !uuid.safeParse(observed?.taskId).success ||
          !uuid.safeParse(observed?.goalId).success ||
          !hash.safeParse(observed?.result?.resultDigest).success
        )
          continue;
        await tx(async (db) => {
          await lock(db);
          const id = key(observed.actionId);
          const job = (await db.query('SELECT value FROM settings WHERE id=$1 FOR UPDATE', [id]))
            .rows[0]?.value;
          if (job?.state === 'published' || job?.state === 'reading') return;
          const next = {
            ...(job ?? {
              version: 1,
              actionId: observed.actionId,
              goalId: observed.goalId,
              taskId: observed.taskId,
              requestDigest: digest(observed.request),
              resultDigest: observed.result.resultDigest,
              inputIds: [randomUUID(), randomUUID()],
            }),
            state: 'reconciling',
            code: /^[A-Z_]+$/.test(error.message) ? error.message : 'CI_EVIDENCE_UNCONFIRMED',
            nextProbeAt: new Date(Date.now() + 15000).toISOString(),
            updatedAt: new Date().toISOString(),
          };
          await db.query(
            'INSERT INTO settings(id,value) VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET value=excluded.value',
            [id, JSON.stringify(next)],
          );
        });
        continue;
      }

      await tx(async (db) => {
        await lock(db);
        const job = (
          await db.query('SELECT value FROM settings WHERE id=$1 FOR UPDATE', [key(claim.actionId)])
        ).rows[0]?.value;
        if (!job || job.state === 'published' || job.claimId !== claim.claimId) return;
        await db.query('UPDATE settings SET value=$2 WHERE id=$1', [
          key(job.actionId),
          JSON.stringify({
            ...job,
            state: 'reconciling',
            code: /^[A-Z_]+$/.test(error.message) ? error.message : 'CI_EVIDENCE_UNCONFIRMED',
            nextProbeAt: new Date(Date.now() + 15000).toISOString(),
            updatedAt: new Date().toISOString(),
          }),
        ]);
      });
    }
  }
}

export async function readCandidateValidationEvidence(db: DB, goalId: string) {
  return (
    await db.query(
      "SELECT value FROM settings WHERE id LIKE 'candidate-validation-evidence:%' AND value->>'goalId'=$1 ORDER BY id LIMIT 20",
      [goalId],
    )
  ).rows.map(({ value: v }) => {
    const { body, ...metadata } = v;
    return {
      ...metadata,
      ...(body
        ? {
            authority: body.authority,
            inputRefs: body.inputs.map(({ base64, ...ref }: any) => ref),
          }
        : {}),
    };
  });
}

async function acceptedRevision(db: DB, c: any, a: any) {
  const persisted = (await db.query('SELECT * FROM supervision_cases WHERE id=$1', [a.caseId]))
    .rows[0];
  if (
    !persisted ||
    persisted.id !== c.id ||
    persisted.goal_id !== c.goal_id ||
    persisted.task_id !== c.task_id ||
    persisted.source_run_id !== c.source_run_id ||
    persisted.decision_run_id !== c.decision_run_id ||
    persisted.state !== 'decided' ||
    digest(persisted.decision) !== digest(c.decision)
  )
    failure('CI_EVIDENCE_DECISION_UNBOUND');
  if (
    a.kind !== 'revise' ||
    !a.validationActionId ||
    a.sourceTaskId ||
    a.initialAdmission ||
    a.deliveryResolution ||
    a.lane ||
    a.workKind
  )
    failure('CI_EVIDENCE_REVISION_INVALID');
  const manager = (
    await db.query(
      `SELECT r.id FROM runs r JOIN tasks t ON t.id=r.task_id JOIN accounts ac ON ac.id=r.account_id
    WHERE r.id=$1 AND t.goal_id=$2 AND t.stage='coordinate' AND r.state='submitted' AND r.runtime='harness' AND ${managerAuthoritySql('ac')}
    AND (SELECT passed FROM acceptances v WHERE v.run_id=r.id AND v.task_id=t.id ORDER BY created_at DESC,id DESC LIMIT 1)=true`,
      [c.decision_run_id, c.goal_id],
    )
  ).rowCount;
  if (!manager) failure('CI_EVIDENCE_SOL_REQUIRED');
  const refs = (
    await db.query("SELECT id FROM artifacts WHERE run_id=$1 AND name='coordination.json'", [
      c.decision_run_id,
    ])
  ).rows;
  if (refs.length !== 1) failure('CI_EVIDENCE_DECISION_UNBOUND');
  const artifact = await readArtifact(refs[0].id);
  if (artifact.bytes.length !== artifact.size) failure('CI_EVIDENCE_DECISION_UNBOUND');
  const actions = JSON.parse(artifact.bytes.toString()).supervision?.actions;
  if (
    !Array.isArray(actions) ||
    !actions.some((raw: any) => {
      const parsed = actionSchema.safeParse(raw);
      return parsed.success && digest(parsed.data) === digest(a);
    })
  )
    failure('CI_EVIDENCE_DECISION_UNBOUND');
  return {
    artifactId: artifact.id,
    artifactDigest: artifact.digest,
    actionDigest: digest(a),
    decisionRunId: c.decision_run_id,
  };
}

export async function prepareCandidateValidationEvidence(
  db: DB,
  c: any,
  a: any,
  t: any,
  r: any,
  proof: any,
) {
  const decisionProof = await acceptedRevision(db, c, a),
    current = await source(db, a.validationActionId);
  const job = (
    await db.query('SELECT value FROM settings WHERE id=$1', [key(a.validationActionId)])
  ).rows[0]?.value;
  if (!job || job.state !== 'published' || !job.body) failure('CI_EVIDENCE_NOT_PUBLISHED');
  const body = checkedExport(
    { ...job.body, inputs: job.body.inputs.map(({ id, source, ...ref }: any) => ref) },
    current,
  );
  const latest = (
    await db.query('SELECT * FROM runs WHERE task_id=$1 ORDER BY attempt DESC LIMIT 1', [t.id])
  ).rows[0];
  const actualTask = (
    await db.query('SELECT * FROM tasks WHERE id=$1 AND goal_id=$2', [t.id, t.goal_id])
  ).rows[0];
  if (
    !actualTask ||
    digest(actualTask) !== digest(t) ||
    latest?.fence !== r.fence ||
    latest?.context_id !== r.context_id ||
    latest?.state !== r.state ||
    latest?.attempt !== r.attempt ||
    digest(latest?.instruction_snapshot) !== digest(r.instruction_snapshot)
  )
    failure('CI_EVIDENCE_CONSUMER_STALE');
  if (
    t.id !== current.v.taskId ||
    t.goal_id !== current.v.goalId ||
    c.task_id !== t.id ||
    c.goal_id !== t.goal_id ||
    c.source_run_id !== r.id ||
    latest?.id !== r.id ||
    t.attempt_count !== r.attempt ||
    !['submitted', 'failed'].includes(r.state) ||
    !r.finished_at ||
    [
      'accepted',
      'cancelled',
      'superseded',
      'paused',
      'running',
      'queued',
      'cancelling',
      'reconciling',
    ].includes(t.state) ||
    t.stage === 'verify' ||
    t.execution?.readOnlySource
  )
    failure('CI_EVIDENCE_CONSUMER_STALE');
  assertStopped(proof, r, t);
  if (
    proof.workspace !== t.execution?.workspace ||
    proof.candidate?.candidateDigest !== body.authority.producer.candidateDigest
  )
    failure('CI_EVIDENCE_SOURCE_CHANGED');
  if (scope(t) !== job.scopeDigest || digest(t.criteria) !== job.criteriaDigest)
    failure('CI_EVIDENCE_CONTRACT_CHANGED');
  if (
    (
      await db.query(
        "SELECT 1 FROM runs WHERE task_id=$1 AND state IN('queued','running','cancelling','reconciling') UNION ALL SELECT 1 FROM leases l JOIN runs rr ON rr.id=l.run_id WHERE rr.task_id=$1",
        [t.id],
      )
    ).rowCount ||
    (
      await db.query(
        "SELECT 1 FROM events WHERE kind='CANCEL_REQUESTED' AND (payload->>'runId'=$1 OR entity_id=ANY($2::text[]))",
        [r.id, [r.id, t.id]],
      )
    ).rowCount
  )
    failure('CI_EVIDENCE_CONSUMER_STALE');
  const consumerContext = (
    await db.query('SELECT manifest FROM contexts WHERE id=$1 AND run_id=$2', [r.context_id, r.id])
  ).rows[0];
  if (!consumerContext) failure('CI_EVIDENCE_CONSUMER_STALE');
  if (digest(consumerContext.manifest.dependencyArtifacts ?? []) !== job.contextDependencyDigest)
    failure('CI_EVIDENCE_CONTRACT_CHANGED');
  const inputs = mergeSupervisorInputs(t.execution?.inputs ?? [], job.body.inputs);
  return {
    actionId: a.validationActionId,
    resultDigest: job.resultDigest,
    producer: body.authority.producer,
    outcome: body.authority.outcome,
    decisionProof,
    consumer: {
      taskId: t.id,
      runId: r.id,
      runFence: r.fence,
      contextVersion: r.context_id,
      expectedAttempt: r.attempt,
      taskDigest: digest(t),
    },
    inputs,
    inputRefs: job.body.inputs.map(({ base64, ...ref }: any) => ref),
    acceptance: false,
  };
}

/** Called again under the existing supervision CAS lock, immediately before input freeze. */
export async function bindCandidateValidationEvidence(
  db: DB,
  c: any,
  a: any,
  t: any,
  r: any,
  proof: any,
  prepared: any,
) {
  if (prepared?.consumer?.taskDigest !== digest(t)) failure('CI_EVIDENCE_CONSUMER_STALE');
  const fresh = await prepareCandidateValidationEvidence(db, c, a, t, r, proof);
  if (digest(fresh) !== digest(prepared)) failure('CI_EVIDENCE_DELIVERY_CHANGED');
  return fresh;
}
