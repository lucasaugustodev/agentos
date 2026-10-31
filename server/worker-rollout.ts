import { randomUUID, createHash } from 'node:crypto';
import { PgBoss } from 'pg-boss';
import { pool, tx, lock, event, type DB } from './db.js';
import { config } from './config.js';
import { DomainError } from './contracts.js';
import type { WorkerReadiness } from './worker-readiness.js';
import { isDeepStrictEqual } from 'node:util';
import { independentNativeSupervisorSql } from './native-supervision.js';
import { activeCandidateValidationCount } from './candidate-validation-lifecycle.js';

export type WorkerRelease = { candidate: string; digest: string; [key: string]: unknown };
export type PendingWorkerRelease = { record: WorkerRelease; version: string };
const owner = 'worker-rollout';
const active = "'queued','running','cancelling','reconciling'";
export const workerServices = [
  'agentos-opencode',
  'agentos-backlog',
  'agentos-cycles',
  'agentos-workers',
] as const;
export type WorkerService = (typeof workerServices)[number];
export type WorkerServiceStopProof = {
  unit: WorkerService;
  previousPid: number;
  activeState: 'inactive';
  mainPid: 0;
  oldPidGone: true;
  cgroupEmpty: true;
  checkedAt: string;
};
export type WorkerServiceProof = {
  unit: WorkerService;
  active: true;
  pid: number;
  cwd: string;
  digest: string;
  checkedAt: string;
  oldPidGone: true;
  readiness: WorkerReadiness;
  stopped: WorkerServiceStopProof;
};
export function workerRunService(task: any, runtime?: string): WorkerService {
  if (runtime === 'opencode' || task.execution?.kind === 'opencode' ||
    (task.execution?.kind === 'harness' && task.workforceEnabled && task.detachedHarnessEnabled))
    return 'agentos-opencode';
  if (task.stage === 'coordinate' && task.execution == null && task.supervisionEnabled)
    return 'agentos-backlog';
  return 'agentos-workers';
}
export function validWorkerServiceProof(
  service: WorkerService,
  record: WorkerRelease,
  proof: any,
  restartAt: string,
  previousPid = 0,
  now = Date.now(),
) {
  const ready = proof?.readiness,
    checkedAt = Date.parse(proof?.checkedAt),
    readyAt = Date.parse(ready?.readyAt);
  const stopped = proof?.stopped;
  const ownerKey = {
    'agentos-opencode': 4280040,
    'agentos-backlog': 4280066,
    'agentos-cycles': 4280088,
    'agentos-workers': 0,
  }[service];
  return (
    proof?.unit === service &&
    proof.active === true &&
    proof.oldPidGone === true &&
    Number.isSafeInteger(proof.pid) &&
    proof.pid > 0 &&
    proof.pid !== previousPid &&
    stopped?.unit === service &&
    stopped.previousPid === previousPid &&
    stopped.activeState === 'inactive' &&
    stopped.mainPid === 0 &&
    stopped.oldPidGone === true &&
    stopped.cgroupEmpty === true &&
    Date.parse(stopped.checkedAt) >= Date.parse(restartAt) &&
    Date.parse(stopped.checkedAt) <= readyAt &&
    proof.cwd === record.candidate &&
    proof.digest === record.digest &&
    checkedAt <= now + 5000 &&
    checkedAt >= now - 30000 &&
    ready?.service === service &&
    ready.pid === proof.pid &&
    ready.candidate === record.candidate &&
    ready.digest === record.digest &&
    /^[0-9a-f-]{36}$/.test(ready.startupId) &&
    Array.isArray(ready.locks) &&
    (!ownerKey ||
      ready.locks.some(
        (lock: any) =>
          lock.key === ownerKey && Number.isSafeInteger(lock.backendPid) && lock.backendPid > 0,
      )) &&
    readyAt >= Date.parse(restartAt) &&
    readyAt <= checkedAt + 5000
  );
}
/** Same lane policy as the database trigger. Snapshot read under scheduler lock. */
export function workerRunAdmission(value: any, task: any, runtime?: string) {
  if (!value?.draining) return { allowed: true, service: workerRunService(task, runtime) };
  const service = workerRunService(task, runtime),
    rollout = value.workerRollout;
  const phase = rollout?.services?.[service],
    proof = phase?.proof;
  let owned = false;
  try {
    owned =
      value.owner === owner &&
      rollout?.mode === 'per_service' &&
      rollout.identity === workerReleaseIdentity(rollout);
  } catch {
    /* malformed reservation is closed */
  }
  const readiness = value.currentReadiness?.[service];
  const allowed =
    owned &&
    phase?.phase === 'adopted' &&
    proof?.unit === service &&
    proof.cwd === rollout.candidate &&
    proof.digest === rollout.digest &&
    proof.active === true &&
    isDeepStrictEqual(readiness, proof.readiness) &&
    readiness?.pid === proof.pid &&
    value.instructionSnapshotEnabled &&
    (service !== 'agentos-opencode' || (task.workforceEnabled && value.nativeDispatchEnabled)) &&
    (service !== 'agentos-backlog' || value.supervisorDispatchEnabled);
  return { allowed: !!allowed, service };
}
export async function workerSchedulerMaintenance(db: DB) {
  const value = (await db.query("SELECT value FROM settings WHERE id='maintenance'")).rows[0]
    ?.value;
  if (!value?.draining || value.owner !== owner || value.workerRollout?.mode !== 'per_service')
    return value;
  const ready = (await db.query("SELECT id,value FROM settings WHERE id LIKE 'worker-readiness:%'"))
    .rows;
  const owners = (
    await db.query(
      "SELECT objid::int key,pid FROM pg_locks WHERE locktype='advisory' AND classid=0 AND objsubid=1 AND database=(SELECT oid FROM pg_database WHERE datname=current_database()) AND granted",
    )
  ).rows;
  const triggers = (
    await db.query(
      "SELECT tgname FROM pg_trigger WHERE tgenabled IN('O','A') AND ((tgrelid='runs'::regclass AND tgname='agentos_instruction_snapshot') OR (tgrelid='outbox'::regclass AND tgname IN('agentos_supervisor_dispatch','agentos_workforce_native_dispatch')))",
    )
  ).rows.map((r) => r.tgname);
  return {
    ...value,
    currentReadiness: Object.fromEntries(
      ready
        .filter(
          (r) =>
            Array.isArray(r.value.locks) &&
            r.value.locks.every((lock: any) =>
              owners.some((o) => o.key === lock.key && o.pid === lock.backendPid),
            ),
        )
        .map((r) => [r.id.slice('worker-readiness:'.length), r.value]),
    ),
    instructionSnapshotEnabled: triggers.includes('agentos_instruction_snapshot'),
    nativeDispatchEnabled: triggers.includes('agentos_workforce_native_dispatch'),
    supervisorDispatchEnabled: triggers.includes('agentos_supervisor_dispatch'),
  };
}

export function workerReleaseIdentity(record: WorkerRelease) {
  if (
    !/^\/opt\/agentos\/releases\/managed-[0-9a-f-]{36}$/.test(record.candidate) ||
    !/^[0-9a-f]{64}$/.test(record.digest)
  )
    throw new DomainError('ROLLOUT_RECORD_INVALID', 'Registro de atualização inválido.');
  return createHash('sha256')
    .update(record.candidate + '\n' + record.digest)
    .digest('hex');
}
async function maintenance(db: DB) {
  return (await db.query("SELECT value FROM settings WHERE id='maintenance' FOR UPDATE")).rows[0]
    ?.value;
}
function owns(value: any, token: string) {
  return value?.draining === true && value.owner === owner && value.workerRollout?.token === token;
}
async function saveMaintenance(db: DB, value: any) {
  await db.query(
    "INSERT INTO settings(id,value) VALUES('maintenance',$1) ON CONFLICT(id) DO UPDATE SET value=excluded.value",
    [JSON.stringify(value)],
  );
}
/** Preserve the older transfer guard as an enabled trigger, sharing the same
 * strict lane predicate instead of allowing it to veto every adopted lane. */
async function updateLegacyMaintenanceAdmission() {
  await pool.query(`DO $migration$ DECLARE source text; prior text; BEGIN
    IF to_regprocedure('agentos_maintenance_admission()') IS NOT NULL THEN
      SELECT prosrc INTO source FROM pg_proc WHERE oid='agentos_worker_rollout_admission()'::regprocedure;
      SELECT prosrc INTO prior FROM pg_proc WHERE oid='agentos_maintenance_admission()'::regprocedure;
      IF prior IS DISTINCT FROM source THEN
        EXECUTE format('CREATE OR REPLACE FUNCTION agentos_maintenance_admission() RETURNS trigger LANGUAGE plpgsql AS %L', source);
      END IF;
    END IF;
  END $migration$`);
}

/** Commit DDL before taking the scheduler lock; reversing those locks can deadlock
 * an already-admitted writer. A distinct function preserves other maintenance gates. */
export async function installWorkerRolloutAdmission() {
  const installed = (
    await pool.query(
      "SELECT count(*)::int n FROM pg_trigger WHERE tgname='agentos_worker_rollout_admission' AND tgenabled IN('O','A') AND tgrelid=ANY(ARRAY['runs'::regclass,'cycle_attempts'::regclass,'planning_runs'::regclass]) AND obj_description(tgfoid,'pg_proc')='agentos:phased-rollout:v4'",
    )
  ).rows[0].n;
  if (installed === 3) {
    await updateLegacyMaintenanceAdmission();
    return;
  }
  await pool.query(`CREATE OR REPLACE FUNCTION agentos_worker_rollout_admission() RETURNS trigger LANGUAGE plpgsql AS $$
    DECLARE m jsonb; rollout jsonb; adopted jsonb; readiness jsonb; lane text='agentos-workers'; task tasks; gate boolean=false;
    BEGIN
    PERFORM pg_advisory_xact_lock(4280001);
    SELECT value INTO m FROM settings WHERE id='maintenance';
    IF m->>'draining'='true' THEN
      IF TG_OP='INSERT' THEN
        gate:=TG_TABLE_NAME<>'planning_runs' OR NEW.state='running';
      ELSIF TG_TABLE_NAME='planning_runs' THEN
        gate:=NEW.state='running' AND OLD.state<>'running';
      ELSE gate:=NEW.state IN(${active}) AND OLD.state NOT IN(${active}); END IF;
      IF NOT gate THEN RETURN NEW; END IF;
      IF TG_TABLE_NAME='cycle_attempts' THEN lane:='agentos-cycles';
      ELSIF TG_TABLE_NAME='runs' THEN
        SELECT * INTO STRICT task FROM tasks WHERE id=NEW.task_id;
        IF NEW.runtime='opencode' OR task.execution->>'kind'='opencode' OR
          (NEW.runtime='harness' AND task.execution->>'kind'='harness' AND
            EXISTS(SELECT 1 FROM settings WHERE id='workforce:'||task.goal_id
              AND value->>'enabled'='true' AND value->>'detachedHarnessEnabled'='true')) THEN
          lane:='agentos-opencode';
          IF NOT (task.execution->>'kind'='opencode' OR
            (NEW.runtime='harness' AND task.execution->>'kind'='harness' AND
              EXISTS(SELECT 1 FROM settings WHERE id='workforce:'||task.goal_id
                AND value->>'detachedHarnessEnabled'='true'))) OR
            NOT EXISTS(SELECT 1 FROM settings WHERE id='workforce:'||task.goal_id AND value->>'enabled'='true') OR
            NOT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid='outbox'::regclass AND tgname='agentos_workforce_native_dispatch' AND tgenabled IN('O','A'))
          THEN RAISE EXCEPTION 'MAINTENANCE_DRAIN'; END IF;
        ELSIF task.stage='coordinate' AND task.execution IS NULL AND
          EXISTS(SELECT 1 FROM settings WHERE id='supervision:'||task.goal_id AND value->>'enabled'='true') THEN
          lane:='agentos-backlog';
          IF NOT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid='outbox'::regclass AND tgname='agentos_supervisor_dispatch' AND tgenabled IN('O','A'))
            THEN RAISE EXCEPTION 'MAINTENANCE_DRAIN'; END IF;
        END IF;
        IF NOT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid='runs'::regclass AND tgname='agentos_instruction_snapshot' AND tgenabled IN('O','A'))
          OR NEW.instruction_snapshot IS NULL THEN RAISE EXCEPTION 'MAINTENANCE_DRAIN'; END IF;
      END IF;
      rollout:=m->'workerRollout'; adopted:=rollout->'services'->lane;
      SELECT value INTO readiness FROM settings WHERE id='worker-readiness:'||lane;
      IF m->>'owner' IS DISTINCT FROM 'worker-rollout' OR rollout->>'mode' IS DISTINCT FROM 'per_service' OR
        rollout->>'identity' IS DISTINCT FROM encode(sha256(convert_to((rollout->>'candidate')||chr(10)||(rollout->>'digest'),'UTF8')),'hex') OR
        adopted->>'phase' IS DISTINCT FROM 'adopted' OR adopted->'proof'->>'unit' IS DISTINCT FROM lane OR
        adopted->'proof'->>'active' IS DISTINCT FROM 'true' OR adopted->'proof'->>'oldPidGone' IS DISTINCT FROM 'true' OR
        adopted->'proof'->>'cwd' IS DISTINCT FROM rollout->>'candidate' OR
        adopted->'proof'->>'digest' IS DISTINCT FROM rollout->>'digest' OR
        readiness IS NULL OR readiness IS DISTINCT FROM adopted->'proof'->'readiness' OR
        readiness->>'pid' IS DISTINCT FROM adopted->'proof'->>'pid'
      THEN RAISE EXCEPTION 'MAINTENANCE_DRAIN'; END IF;
      IF jsonb_typeof(readiness->'locks') IS DISTINCT FROM 'array' THEN RAISE EXCEPTION 'MAINTENANCE_DRAIN'; END IF;
      IF EXISTS(SELECT 1 FROM jsonb_to_recordset(readiness->'locks') AS holder(key int,"backendPid" int)
        WHERE NOT EXISTS(SELECT 1 FROM pg_locks l WHERE l.locktype='advisory' AND l.classid=0 AND l.objsubid=1 AND l.database=(SELECT oid FROM pg_database WHERE datname=current_database()) AND l.objid=holder.key::oid AND l.pid=holder."backendPid" AND l.granted))
      THEN RAISE EXCEPTION 'MAINTENANCE_DRAIN'; END IF;
    END IF; RETURN NEW; END $$;
    COMMENT ON FUNCTION agentos_worker_rollout_admission() IS 'agentos:phased-rollout:v4';
    DROP TRIGGER IF EXISTS agentos_worker_rollout_admission ON runs;
    CREATE TRIGGER agentos_worker_rollout_admission BEFORE INSERT OR UPDATE OF state ON runs FOR EACH ROW EXECUTE FUNCTION agentos_worker_rollout_admission();
    DROP TRIGGER IF EXISTS agentos_worker_rollout_admission ON cycle_attempts;
    CREATE TRIGGER agentos_worker_rollout_admission BEFORE INSERT OR UPDATE OF state ON cycle_attempts FOR EACH ROW EXECUTE FUNCTION agentos_worker_rollout_admission();
    DROP TRIGGER IF EXISTS agentos_worker_rollout_admission ON planning_runs;
    CREATE TRIGGER agentos_worker_rollout_admission BEFORE INSERT OR UPDATE OF state ON planning_runs FOR EACH ROW EXECUTE FUNCTION agentos_worker_rollout_admission();`);
  await updateLegacyMaintenanceAdmission();
}

/** Admission and the global quiet check use the scheduler's transaction lock. */
export async function claimWorkerDrain(
  db: DB,
  record: WorkerRelease,
  mode: 'global' | 'per_service' = 'global',
) {
  await lock(db);
  const value = await maintenance(db);
  if ((value?.draining && value.owner !== owner) || (value?.owner && value.owner !== owner))
    return { status: 'foreign_maintenance' as const };
  const identity = workerReleaseIdentity(record),
    prior = value?.workerRollout;
  const same = value?.draining && value.owner === owner && prior?.identity === identity;
  const reservation = same
    ? {
        ...prior,
        mode: prior.mode === 'per_service' ? prior.mode : mode,
        services: prior.services ?? {},
      }
    : {
        token: randomUUID(),
        identity,
        candidate: record.candidate,
        digest: record.digest,
        previousMaintenance:
          prior && Object.hasOwn(prior, 'previousMaintenance')
            ? prior.previousMaintenance
            : value?.draining && value.owner === owner
              ? { ...value, draining: false }
              : (value ?? null),
        startedAt: new Date().toISOString(),
        phase: 'draining',
        mode,
        services: {},
      };
  await saveMaintenance(db, {
    ...value,
    draining: true,
    owner,
    reason:
      reservation.mode === 'per_service'
        ? 'Atualização por serviço: executores adotados continuam; executores antigos drenam.'
        : 'Atualização pendente: novas tentativas suspensas; execuções admitidas continuam.',
    workerRollout: reservation,
  });
  if (!same)
    await event(db, 'WORKER_ROLLOUT_DRAIN_STARTED', null, {
      candidate: record.candidate,
      token: reservation.token,
    });
  return { status: 'draining' as const, token: reservation.token as string };
}

export async function admitWorkerRestart(db: DB, token: string) {
  await lock(db);
  const value = await maintenance(db);
  if (!owns(value, token)) return { ready: false, lostOwnership: true };
  const counts = (
    await db.query(`SELECT
    (SELECT count(*) FROM runs WHERE state IN(${active}))::int runs,
    (SELECT count(*) FROM cycle_attempts WHERE state IN(${active}))::int cycles,
    (SELECT count(*) FROM planning_runs WHERE state='running')::int planning`)
  ).rows[0];
  counts.candidate_validations = await activeCandidateValidationCount(db);
  if (Object.values(counts).some((n) => Number(n) > 0)) return { ready: false, counts };
  await saveMaintenance(db, {
    ...value,
    workerRollout: { ...value.workerRollout, phase: 'restarting' },
  });
  return { ready: true, counts };
}

export async function admitWorkerServiceRestart(db: DB, token: string, service: WorkerService) {
  await lock(db);
  const value = await maintenance(db);
  if (!owns(value, token) || value.workerRollout.mode !== 'per_service')
    return { ready: false, lostOwnership: true };
  const prior = value.workerRollout.services?.[service];
  if (prior?.phase === 'adopted') return { ready: true, adopted: true, phase: prior };
  const tables = (
    await db.query(
      "SELECT to_regclass('opencode_jobs') native,to_regclass('supervisor_jobs') supervisor,to_regclass('agentos_queue.job') queue",
    )
  ).rows[0];
  const native = tables.native
    ? 'EXISTS(SELECT 1 FROM opencode_jobs j WHERE j.run_id=r.id)'
    : 'false';
  const independentlySupervised = tables.native
    ? `EXISTS(SELECT 1 FROM opencode_jobs j WHERE j.run_id=r.id AND j.state='running' AND ${independentNativeSupervisorSql()})`
    : 'false';
  const supervisor = tables.supervisor
    ? 'EXISTS(SELECT 1 FROM supervisor_jobs j WHERE j.run_id=r.id)'
    : 'false';
  const rows = (
    await db.query(`SELECT count(*)::int n, CASE
    WHEN r.runtime='opencode' OR ${native} OR (t.execution->>'kind'='opencode' AND
      EXISTS(SELECT 1 FROM settings WHERE id='workforce:'||t.goal_id AND value->>'enabled'='true')) THEN 'agentos-opencode'
    WHEN ${supervisor} OR (t.stage='coordinate' AND t.execution IS NULL AND
      EXISTS(SELECT 1 FROM settings WHERE id='supervision:'||t.goal_id AND value->>'enabled'='true')) THEN 'agentos-backlog'
    ELSE 'agentos-workers' END lane FROM runs r JOIN tasks t ON t.id=r.task_id WHERE r.state IN(${active})
      AND NOT (${independentlySupervised}) GROUP BY lane`)
  ).rows;
  const counts = {
    runs: rows.find((row) => row.lane === service)?.n ?? 0,
    jobs: 0,
    cycles: 0,
    planning: 0,
    candidate_validations: 0,
  };
  if (service === 'agentos-opencode' && tables.native)
    counts.jobs = (
      await db.query(`SELECT count(*)::int n FROM opencode_jobs j JOIN runs r ON r.id=j.run_id
        WHERE j.state IN('queued','launching','running','orphaned')
        AND NOT (j.state='running' AND ${independentNativeSupervisorSql()})`)
    ).rows[0].n;
  if (service === 'agentos-backlog' && tables.supervisor)
    counts.jobs = (
      await db.query(
        "SELECT count(*)::int n FROM supervisor_jobs WHERE state IN('queued','running')",
      )
    ).rows[0].n;
  if (service === 'agentos-workers' && tables.queue)
    counts.jobs = (
      await db.query(
        "SELECT count(*)::int n FROM agentos_queue.job WHERE name='runs' AND state='active'",
      )
    ).rows[0].n;
  if (service === 'agentos-cycles' || service === 'agentos-workers')
    counts.cycles = (
      await db.query(`SELECT count(*)::int n FROM cycle_attempts WHERE state IN(${active})`)
    ).rows[0].n;
  if (service === 'agentos-workers')
    counts.planning = (
      await db.query("SELECT count(*)::int n FROM planning_runs WHERE state='running'")
    ).rows[0].n;
  if (service === 'agentos-workers')
    counts.candidate_validations = await activeCandidateValidationCount(db);
  if (Object.values(counts).some((n) => n > 0)) return { ready: false, counts };
  const phase =
    prior?.phase === 'restarting' || prior?.phase === 'awaiting_readiness'
      ? prior
      : {
          phase: 'restarting',
          restartAt: new Date().toISOString(),
          quietCounts: counts,
          quietAt: new Date().toISOString(),
        };
  await saveMaintenance(db, {
    ...value,
    workerRollout: {
      ...value.workerRollout,
      services: { ...value.workerRollout.services, [service]: phase },
    },
  });
  return { ready: true, counts, phase };
}

/** Persist the completed stop/start boundary before waiting for startup. A slow
 * initialization or expired observation must not restart that same process. */
export async function recordWorkerServiceStart(db: DB, token: string, service: WorkerService,
  restartAt: string, started: {previousPid:number;stopped:WorkerServiceStopProof}) {
  await lock(db);
  const value=await maintenance(db),phase=value?.workerRollout?.services?.[service];
  if(!owns(value,token)||value.workerRollout.mode!=='per_service'||phase?.phase!=='restarting'||
    phase.restartAt!==restartAt)return false;
  const stopped=started.stopped;
  if(stopped.unit!==service||stopped.previousPid!==started.previousPid||
    !Number.isSafeInteger(started.previousPid)||started.previousPid<0||
    stopped.activeState!=='inactive'||stopped.mainPid!==0||stopped.oldPidGone!==true||
    stopped.cgroupEmpty!==true||!Number.isFinite(Date.parse(stopped.checkedAt))||
    Date.parse(stopped.checkedAt)<Date.parse(restartAt)||Date.parse(stopped.checkedAt)>Date.now()+5000)
    throw new DomainError('ROLLOUT_SERVICE_STOP_PROOF','Encerramento anterior sem prova válida.');
  await saveMaintenance(db,{...value,workerRollout:{...value.workerRollout,
    services:{...value.workerRollout.services,[service]:{...phase,phase:'awaiting_readiness',
      previousPid:started.previousPid,stopped,startedAt:new Date().toISOString()}}}});
  return true;
}

export async function adoptWorkerService(
  db: DB,
  token: string,
  record: WorkerRelease,
  service: WorkerService,
  proof: WorkerServiceProof,
  previousPid = 0,
) {
  await lock(db);
  const value = await maintenance(db),
    rollout = value?.workerRollout;
  if (
    !owns(value, token) ||
    rollout.mode !== 'per_service' ||
    rollout.identity !== workerReleaseIdentity(record)
  )
    return false;
  const phase = rollout.services?.[service];
  if (!phase || !validWorkerServiceProof(service, record, proof, phase.restartAt, previousPid))
    throw new DomainError(
      'ROLLOUT_SERVICE_PROOF',
      'Prontidão do executor novo não foi comprovada.',
    );
  const readiness = (
    await db.query('SELECT value FROM settings WHERE id=$1', ['worker-readiness:' + service])
  ).rows[0]?.value;
  if (JSON.stringify(readiness) !== JSON.stringify(proof.readiness)) {
    // jsonb key order is canonical in PostgreSQL, not in a process-created object.
    if (
      !(
        await db.query('SELECT $1::jsonb=$2::jsonb same', [
          JSON.stringify(readiness ?? null),
          JSON.stringify(proof.readiness),
        ])
      ).rows[0].same
    )
      throw new DomainError('ROLLOUT_SERVICE_PROOF', 'Recibo de prontidão mudou.');
  }
  const owners = (
    await db.query(
      `SELECT count(*)::int n FROM
    jsonb_to_recordset($1::jsonb) AS holder(key int,"backendPid" int) JOIN pg_locks l
    ON l.locktype='advisory' AND l.classid=0 AND l.objsubid=1 AND l.database=(SELECT oid FROM pg_database WHERE datname=current_database()) AND l.objid=holder.key::oid AND l.pid=holder."backendPid" AND l.granted`,
      [JSON.stringify(proof.readiness.locks)],
    )
  ).rows[0].n;
  if (owners !== proof.readiness.locks.length)
    throw new DomainError('ROLLOUT_SERVICE_PROOF', 'Dono da fila perdeu seu lock.');
  await saveMaintenance(db, {
    ...value,
    workerRollout: {
      ...rollout,
      services: {
        ...rollout.services,
        [service]: { ...phase, phase: 'adopted', previousPid, stopped: proof.stopped, proof },
      },
    },
  });
  if (phase.phase !== 'adopted' || phase.proof?.readiness?.startupId !== proof.readiness.startupId)
    await event(db, 'WORKER_SERVICE_UPDATED', null, {
      service,
      candidate: record.candidate,
      pid: proof.pid,
      quietCounts: phase.quietCounts,
      quietAt: phase.quietAt,
      stopped: proof.stopped,
      readiness: proof.readiness,
    });
  return true;
}

export async function finishWorkerDrain(db: DB, token: string, record: WorkerRelease) {
  await lock(db);
  const value = await maintenance(db);
  if (!owns(value, token) || value.workerRollout.identity !== workerReleaseIdentity(record))
    return false;
  if (
    value.workerRollout.mode === 'per_service' &&
    workerServices.some((service) => value.workerRollout.services?.[service]?.phase !== 'adopted')
  )
    return false;
  const previous = value.workerRollout.previousMaintenance;
  if (previous === null) await db.query("DELETE FROM settings WHERE id='maintenance'");
  else await saveMaintenance(db, previous);
  await event(db, 'WORKERS_UPDATED', null, { candidate: record.candidate, token });
  return true;
}
async function recordFailure(token: string, code: string, service?: WorkerService) {
  await tx(async (db) => {
    await lock(db);
    const value = await maintenance(db);
    if (!owns(value, token)) return;
    await saveMaintenance(db, {
      ...value,
      workerRollout: {
        ...value.workerRollout,
        phase: 'failed',
        errorCode: code,
        ...(service
          ? {
              services: {
                ...value.workerRollout.services,
                [service]: {
                  ...value.workerRollout.services?.[service],
                  phase:
                    value.workerRollout.services?.[service]?.phase === 'adopted'
                      ? 'adopted'
                      : value.workerRollout.services?.[service]?.stopped && code!=='ROLLOUT_SERVICE_START_FAILED'
                        ? 'awaiting_readiness'
                      : 'failed',
                  errorCode: code,
                },
              },
            }
          : {}),
      },
    });
    await event(db, 'WORKER_ROLLOUT_FAILED', null, {
      candidate: value.workerRollout.candidate,
      token,
      code,
      ...(service ? { service } : {}),
    });
  });
}

/** Old workers can fail schedule() before publishing their pre-drain outbox.
 * Deliver only already-admitted runs through the same transactional pg-boss path. */
export async function dispatchAdmittedWorkerRuns() {
  if (
    !(
      await pool.query(
        "SELECT 1 FROM outbox o JOIN runs r ON r.id=o.run_id WHERE o.sent_at IS NULL AND r.state='queued' LIMIT 1",
      )
    ).rowCount
  )
    return 0;
  const boss = new PgBoss({ connectionString: config.databaseUrl, schema: 'agentos_queue' });
  boss.on('error', () => {});
  try {
    await boss.start();
    return await tx(async (db) => {
      await lock(db);
      const rows = (
        await db.query(
          "SELECT o.* FROM outbox o JOIN runs r ON r.id=o.run_id WHERE o.sent_at IS NULL AND r.state='queued' ORDER BY o.id FOR UPDATE OF o",
        )
      ).rows;
      for (const row of rows) {
        await boss.send(
          'runs',
          { runId: row.run_id },
          {
            db: { executeSql: (text: string, values?: any[]) => db.query(text, values) },
            singletonKey: row.run_id,
            retryLimit: 3,
            heartbeatSeconds: 30,
          },
        );
        await db.query('UPDATE outbox SET sent_at=now() WHERE id=$1', [row.id]);
      }
      return rows.length;
    });
  } finally {
    await boss.stop({ graceful: true });
  }
}

export type WorkerRolloutIO = {
  readPending(): Promise<PendingWorkerRelease | null>;
  readLast(): Promise<any | null>;
  verifyRelease(record: WorkerRelease): Promise<void>;
  dispatchAdmitted(): Promise<number>;
  restart(): Promise<void>;
  healthy(): Promise<void>;
  archive(expected: PendingWorkerRelease, completed: any): Promise<void>;
  restartService?(
    service: WorkerService,
  ): Promise<{ previousPid: number; stopped: WorkerServiceStopProof }>;
  serviceProof?(
    service: WorkerService,
    record: WorkerRelease,
    phase: any,
    options?: { once?: boolean },
  ): Promise<WorkerServiceProof>;
  scheduleAdmitted?(): Promise<unknown>;
};

async function collectAndAdoptWorkerService(io:WorkerRolloutIO,token:string,record:WorkerRelease,
  service:WorkerService,phase:any,previousPid:number){
  // Wait for initialization outside the scheduler lock. Then collect a new,
  // single observation after acquiring it, so queue delay cannot expire proof.
  await io.serviceProof!(service,record,phase);
  return tx(async db=>{
    await lock(db);
    const proof=await io.serviceProof!(service,record,phase,{once:true});
    return adoptWorkerService(db,token,record,service,proof,previousPid);
  });
}

async function reconcilePhasedWorkerRollout(
  io: WorkerRolloutIO,
  pending: PendingWorkerRelease,
  token: string,
) {
  const waiting: Record<string, any> = {},
    errors: Record<string, string> = {};
  for (const service of workerServices) {
    try {
      const quiet = await tx((db) => admitWorkerServiceRestart(db, token, service));
      if (quiet.lostOwnership) return { status: 'ownership_changed' };
      if (!quiet.ready) {
        waiting[service] = quiet.counts;
        continue;
      }
      await io.verifyRelease(pending.record);
      if ((await io.readPending())?.version !== pending.version)
        throw new DomainError('ROLLOUT_PENDING_CHANGED', 'Atualização pendente mudou.');
      let phase = quiet.phase,
        previousPid = phase?.previousPid ?? 0;
      if (!quiet.adopted && !phase?.stopped) {
        if (!(await tx((db) => admitWorkerServiceRestart(db, token, service))).ready) {
          waiting[service] = { changed: true };
          continue;
        }
        // Gate stays closed through stop/start/init; this never starts a second owner.
        const stopped = await io.restartService!(service);
        previousPid = stopped.previousPid;
        if(!(await tx(db=>recordWorkerServiceStart(db,token,service,phase.restartAt,stopped))))
          return {status:'ownership_changed'};
        phase = { ...phase, phase:'awaiting_readiness',previousPid, stopped: stopped.stopped };
      }
      if (!(await collectAndAdoptWorkerService(io,token,pending.record,service,phase,previousPid)))
        return { status: 'ownership_changed' };
    } catch (error: any) {
      const code = error instanceof DomainError ? error.code : 'ROLLOUT_SERVICE_RETRY_REQUIRED';
      await recordFailure(token, code, service);
      errors[service] = code;
    }
  }
  const value = (await pool.query("SELECT value FROM settings WHERE id='maintenance'")).rows[0]
    ?.value;
  if (!owns(value, token)) return { status: 'ownership_changed' };
  const allAdopted = workerServices.every(
    (service) => value.workerRollout.services?.[service]?.phase === 'adopted',
  );
  if (!allAdopted || Object.keys(errors).length) {
    await io.scheduleAdmitted?.();
    await io.dispatchAdmitted();
    return {
      status: Object.keys(errors).length ? 'partial_failed' : 'partial',
      waiting,
      errors,
      adopted: workerServices.filter(
        (service) => value.workerRollout.services?.[service]?.phase === 'adopted',
      ),
    };
  }
  const completed = {
    ...pending.record,
    workersPendingRestart: false,
    workersUpdatedAt: new Date().toISOString(),
    rolloutToken: token,
    workerServices: value.workerRollout.services,
  };
  await io.archive(pending, completed);
  const resumed = await tx((db) => finishWorkerDrain(db, token, pending.record));
  return { status: resumed ? 'updated' : 'ownership_changed', candidate: pending.record.candidate };
}
/** Caller holds the publication filesystem lock throughout this single tick. */
export async function reconcileWorkerRollout(io: WorkerRolloutIO) {
  const pending = await io.readPending(),
    last = await io.readLast();
  const value = (await pool.query("SELECT value FROM settings WHERE id='maintenance'")).rows[0]
    ?.value;
  if (
    value?.owner === owner &&
    value.draining &&
    last?.rolloutToken === value.workerRollout?.token &&
    last.workersPendingRestart === false &&
    (!pending || workerReleaseIdentity(pending.record) === workerReleaseIdentity(last))
  ) {
    await io.verifyRelease(last);
    if (io.serviceProof && io.restartService) {
      for (const service of workerServices) {
        const phase = last.workerServices?.[service];
        if (phase?.phase !== 'adopted')
          throw new DomainError('ROLLOUT_SERVICE_PROOF', 'Recibo completo sem prova por serviço.');
        if (!(await collectAndAdoptWorkerService(io,last.rolloutToken,last,service,phase,phase.previousPid)))
          return { status: 'ownership_changed' };
      }
    }
    await io.healthy();
    if (pending) await io.archive(pending, last);
    const resumed = await tx((db) => finishWorkerDrain(db, last.rolloutToken, last));
    return { status: resumed ? 'updated' : 'ownership_changed' };
  }
  if (!pending)
    return { status: value?.owner === owner && value.draining ? 'missing_pending_record' : 'idle' };
  workerReleaseIdentity(pending.record);
  await io.verifyRelease(pending.record);
  await installWorkerRolloutAdmission();
  const phased = !!io.restartService && !!io.serviceProof;
  const drain = await tx((db) =>
    claimWorkerDrain(db, pending.record, phased ? 'per_service' : 'global'),
  );
  if (drain.status === 'foreign_maintenance') return drain;
  try {
    await io.dispatchAdmitted();
    if (phased) return await reconcilePhasedWorkerRollout(io, pending, drain.token);
    const quiet = await tx((db) => admitWorkerRestart(db, drain.token));
    if (!quiet.ready)
      return {
        status: quiet.lostOwnership ? 'ownership_changed' : 'draining',
        counts: quiet.counts,
      };
    await io.verifyRelease(pending.record);
    if ((await io.readPending())?.version !== pending.version)
      throw new DomainError('ROLLOUT_PENDING_CHANGED', 'Atualização pendente mudou.');
    if (!(await tx((db) => admitWorkerRestart(db, drain.token))).ready)
      return { status: 'ownership_changed' };
    await io.restart();
    await io.healthy();
    const completed = {
      ...pending.record,
      workersPendingRestart: false,
      workersUpdatedAt: new Date().toISOString(),
      rolloutToken: drain.token,
    };
    await io.archive(pending, completed);
    const resumed = await tx((db) => finishWorkerDrain(db, drain.token, pending.record));
    return {
      status: resumed ? 'updated' : 'ownership_changed',
      candidate: pending.record.candidate,
    };
  } catch (error: any) {
    const code = error instanceof DomainError ? error.code : 'ROLLOUT_RETRY_REQUIRED';
    await recordFailure(drain.token, code);
    return { status: 'failed', code };
  }
}
