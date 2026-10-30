import { randomUUID } from 'node:crypto';
import type { PoolClient } from 'pg';
import { pool, tx, lock } from '../db.js';
import { quotaFor, supportsAccountExecution } from '../accounts.js';
import { DomainError } from '../contracts.js';
import { createOccurrence, cycleEvent } from './store.js';
import { listBitrix, clean } from './integrations.js';
import { runCycleAction, runCycleAgent, saveCycleArtifacts, readCycleArtifact } from './runtime.js';
import { inspectStopped } from '../continuation.js';
import { collectStoppedArtifacts } from '../artifact-recovery.js';

const terminal = ['completed', 'cancelled'];
export class CycleEngine {
  ownerId = 'cycles-' + randomUUID();
  owner?: PoolClient;
  timer?: ReturnType<typeof setInterval>;
  active = new Map<string, AbortController>();
  busy = false;
  stopped = false;
  async start() {
    this.timer = setInterval(() => void this.tick(), 2000);
    await this.tick();
  }
  async tick() {
    if (this.busy || this.stopped) return;
    this.busy = true;
    try {
      if (!this.owner) {
        const c = await pool.connect();
        if (!(await c.query('SELECT pg_try_advisory_lock(4280088) owned')).rows[0].owned) {
          c.release();
          return;
        }
        this.owner = c;
        c.on('error', () => {
          this.stopped = true;
          for (const controller of this.active.values()) controller.abort();
        });
      }
      await this.owner.query('SELECT 1');
      await pool.query(
        "INSERT INTO settings(id,value) VALUES('cycle-engine',$1) ON CONFLICT(id) DO UPDATE SET value=excluded.value",
        [
          JSON.stringify({
            owner: this.ownerId,
            checkedAt: new Date().toISOString(),
            active: this.active.size,
          }),
        ],
      );
      await this.poll();
      await this.recover();
      await this.reconcile();
      for (let i = 0; i < 4; i++) {
        const job = await this.claim();
        if (!job) break;
        void this.execute(job);
      }
    } catch (e: any) {
      await pool
        .query(
          "INSERT INTO settings(id,value) VALUES('cycle-engine-error',$1) ON CONFLICT(id) DO UPDATE SET value=excluded.value",
          [JSON.stringify({ code: e.code ?? 'CYCLE_ERROR', at: new Date().toISOString() })],
        )
        .catch(() => {});
    } finally {
      this.busy = false;
    }
  }
  async poll() {
    const due = (
      await pool.query(
        "SELECT * FROM cycles WHERE state='active' AND (next_check_at IS NULL OR next_check_at<=now()) ORDER BY next_check_at NULLS FIRST LIMIT 3",
      )
    ).rows;
    for (const cycle of due) {
      const trigger = cycle.definition.trigger;
      if (trigger.kind === 'manual') {
        await pool.query("UPDATE cycles SET next_check_at=now()+interval '1 hour' WHERE id=$1", [
          cycle.id,
        ]);
        continue;
      }
      try {
        const items =
          trigger.kind === 'bitrix' ? await listBitrix(trigger.groupId, trigger.stageId) : [];
        await tx(async (db) => {
          await lock(db);
          const fresh = (await db.query('SELECT * FROM cycles WHERE id=$1', [cycle.id])).rows[0];
          if (fresh.state !== 'active' || fresh.version !== cycle.version) return;
          const cursor = fresh.cursor ?? {};
          if (trigger.kind === 'interval')
            await createOccurrence(
              db,
              fresh,
              'interval:' + Math.floor(Date.now() / (trigger.seconds * 1000)),
              { scheduledAt: new Date().toISOString() },
            );
          else {
            const seen = new Set<string>(cursor.seen ?? []);
            for (const item of items) {
              const identity = item.id + ':' + item.revision;
              if (!seen.has(identity) && (cursor.initialized || trigger.includeExisting))
                await createOccurrence(
                  db,
                  fresh,
                  'bitrix:' + identity,
                  {
                    cardId: item.id,
                    stageId: item.stageId,
                    groupId: trigger.groupId,
                    revision: item.revision,
                  },
                  item.title,
                );
              seen.add(identity);
            }
            cursor.seen = [...seen];
            cursor.initialized = true;
          }
          await db.query(
            "UPDATE cycles SET checked_at=now(),last_success_at=now(),last_error=NULL,next_check_at=now()+($2::int*interval '1 second'),cursor=$3 WHERE id=$1",
            [cycle.id, trigger.seconds, JSON.stringify(cursor)],
          );
        });
      } catch (e: any) {
        await pool.query(
          "UPDATE cycles SET checked_at=now(),last_error=$2,next_check_at=now()+interval '60 seconds' WHERE id=$1",
          [cycle.id, e.code ?? 'CONNECTOR_UNAVAILABLE'],
        );
      }
    }
  }
  async recover() {
    await tx(async (db) => {
      await lock(db);
      const stale = (
        await db.query(
          "UPDATE cycle_attempts SET state='reconciling',error_code='OWNER_LOST' WHERE state='running' AND owner<>$1 AND heartbeat_at<now()-interval '90 seconds' RETURNING *",
          [this.ownerId],
        )
      ).rows;
      for (const a of stale) {
        const s = (
          await db.query(
            "UPDATE cycle_steps SET state='reconciling',reason='Executor perdeu comunicação; término precisa ser confirmado.' WHERE id=$1 RETURNING *",
            [a.step_id],
          )
        ).rows[0];
        await db.query("UPDATE cycle_occurrences SET state='reconciling',reason=$2 WHERE id=$1", [
          s.occurrence_id,
          s.reason,
        ]);
      }
      const waits = (
        await db.query(
          "SELECT s.*,o.cycle_id,o.state occurrence_state FROM cycle_steps s JOIN cycle_occurrences o ON o.id=s.occurrence_id WHERE s.state='waiting' AND s.next_at<=now() AND o.state NOT IN('paused','cancelled','cancelling')",
        )
      ).rows;
      for (const s of waits) {
        await db.query("UPDATE cycle_steps SET state='completed',output=$2 WHERE id=$1", [
          s.id,
          JSON.stringify({
            summary: 'Período de espera concluído.',
            completedAt: new Date().toISOString(),
          }),
        ]);
        await cycleEvent(db, s.cycle_id, s.occurrence_id, 'STEP_COMPLETED', {}, s.id);
      }
      const occurrences = (
        await db.query(
          "SELECT * FROM cycle_occurrences WHERE state IN('queued','running','waiting','recovering','cancelling')",
        )
      ).rows;
      for (const o of occurrences) {
        const steps = (await db.query('SELECT * FROM cycle_steps WHERE occurrence_id=$1', [o.id]))
          .rows;
        if (o.state === 'cancelling') {
          const live = (
            await db.query(
              "SELECT 1 FROM cycle_attempts a JOIN cycle_steps s ON s.id=a.step_id WHERE s.occurrence_id=$1 AND a.state IN('running','cancelling','reconciling')",
              [o.id],
            )
          ).rowCount;
          if (!live) {
            await db.query(
              "UPDATE cycle_steps SET state='cancelled' WHERE occurrence_id=$1 AND state<>'completed'",
              [o.id],
            );
            await db.query(
              "UPDATE cycle_occurrences SET state='cancelled',finished_at=now() WHERE id=$1",
              [o.id],
            );
            await cycleEvent(db, o.cycle_id, o.id, 'CANCELLED');
          }
        } else if (steps.length && steps.every((s) => s.state === 'completed')) {
          await db.query(
            "UPDATE cycle_occurrences SET state='completed',finished_at=now(),reason=NULL WHERE id=$1",
            [o.id],
          );
          await cycleEvent(db, o.cycle_id, o.id, 'COMPLETED');
        }
      }
    });
  }
  async claim(): Promise<any | null> {
    return tx(async (db) => {
      await lock(db);
      if (
        (
          await db.query(
            "SELECT 1 FROM settings WHERE id='maintenance' AND value->>'draining'='true'",
          )
        ).rowCount
      )
        return null;
      const candidates = (
        await db.query(`SELECT s.*,o.cycle_id,o.definition occurrence_definition,o.input,o.version,o.title occurrence_title FROM cycle_steps s JOIN cycle_occurrences o ON o.id=s.occurrence_id
        WHERE s.state IN('queued','retry') AND (s.next_at IS NULL OR s.next_at<=now()) AND o.state IN('queued','running','waiting','recovering') ORDER BY o.created_at,s.position LIMIT 100`)
      ).rows;
      for (const s of candidates) {
        const others = (
          await db.query('SELECT key,state FROM cycle_steps WHERE occurrence_id=$1', [
            s.occurrence_id,
          ])
        ).rows;
        if (
          s.definition.dependsOn.some(
            (key: string) => !others.some((p) => p.key === key && p.state === 'completed'),
          )
        )
          continue;
        const occupied = Number(
          (
            await db.query(
              "SELECT count(*) FROM cycle_attempts a JOIN cycle_steps s ON s.id=a.step_id JOIN cycle_occurrences o ON o.id=s.occurrence_id WHERE o.cycle_id=$1 AND a.state IN('running','cancelling','reconciling')",
              [s.cycle_id],
            )
          ).rows[0].count,
        );
        if (occupied >= s.occurrence_definition.concurrency) continue;
        if (s.definition.kind === 'wait') {
          await db.query(
            "UPDATE cycle_steps SET state='waiting',next_at=now()+($2::int*interval '1 second') WHERE id=$1",
            [s.id, s.definition.seconds],
          );
          await db.query(
            "UPDATE cycle_occurrences SET state='waiting',reason='Aguardando período definido.' WHERE id=$1",
            [s.occurrence_id],
          );
          await cycleEvent(
            db,
            s.cycle_id,
            s.occurrence_id,
            'WAIT_STARTED',
            { seconds: s.definition.seconds },
            s.id,
          );
          continue;
        }
        if (s.definition.kind === 'review') {
          await db.query("UPDATE cycle_steps SET state='attention',reason=$2 WHERE id=$1", [
            s.id,
            s.definition.instructions || 'Revisão explícita solicitada pela definição deste ciclo.',
          ]);
          await db.query(
            "UPDATE cycle_occurrences SET state='attention',reason='Uma etapa solicita sua decisão.' WHERE id=$1",
            [s.occurrence_id],
          );
          await cycleEvent(db, s.cycle_id, s.occurrence_id, 'REVIEW_REQUESTED', {}, s.id);
          continue;
        }
        let account: any = null,
          model: string | null = null;
        if (s.definition.kind === 'agent') {
          const capacity = (await db.query("SELECT value FROM settings WHERE id='capacity'"))
            .rows[0]?.value ?? { maxRuns: 4 };
          const total = Number(
            (
              await db.query(
                "SELECT (SELECT count(*) FROM runs WHERE state IN('queued','running','cancelling','reconciling'))+(SELECT count(*) FROM cycle_attempts WHERE state IN('running','cancelling','reconciling') AND account_id IS NOT NULL) n",
              )
            ).rows[0].n,
          );
          if (total >= capacity.maxRuns) continue;
          const accounts = (
            await db.query(
              'SELECT * FROM accounts WHERE enabled=true AND bridge_id IS NOT NULL ORDER BY id',
            )
          ).rows;
          for (const candidate of accounts) {
            if (s.definition.accountId && s.definition.accountId !== candidate.id) continue;
            if (
              s.occurrence_definition.allowedAccounts.length &&
              !s.occurrence_definition.allowedAccounts.includes(candidate.id)
            )
              continue;
            if (!supportsAccountExecution(candidate, { kind: 'harness' })) continue;
            const m =
              s.definition.model ??
              (candidate.provider === 'codex' ? 'gpt-6-luna' : candidate.model);
            if (!candidate.models.includes(m)) continue;
            const quota = quotaFor(candidate, m);
            if (
              ['exhausted', 'auth_required'].includes(quota.status) ||
              (quota.status === 'unknown' && !candidate.allow_unknown)
            )
              continue;
            const n = Number(
              (
                await db.query(
                  "SELECT (SELECT count(*) FROM runs WHERE account_id=$1 AND state IN('queued','running','cancelling','reconciling'))+(SELECT count(*) FROM cycle_attempts WHERE account_id=$1 AND state IN('running','cancelling','reconciling'))+(SELECT count(*) FROM chat_messages WHERE account_id=$1 AND state IN('queued','running','cancelling'))+(SELECT count(*) FROM planning_runs WHERE account_id=$1 AND state='running' AND output IS NULL) n",
                  [candidate.id],
                )
              ).rows[0].n,
            );
            if (n < candidate.max_concurrency) {
              account = candidate;
              model = m;
              break;
            }
          }
          if (!account) {
            await db.query(
              "UPDATE cycle_steps SET reason='Aguardando conta ou capacidade disponível.' WHERE id=$1",
              [s.id],
            );
            continue;
          }
        }
        const id = randomUUID(),
          fence = randomUUID();
        const attempt = (
          await db.query(
            'INSERT INTO cycle_attempts(id,step_id,number,fence,owner,account_id,model) VALUES($1,$2,$3,$4,$5,$6,$7) RETURNING *',
            [id, s.id, s.attempt + 1, fence, this.ownerId, account?.id ?? null, model],
          )
        ).rows[0];
        await db.query(
          "UPDATE cycle_steps SET state='running',attempt=attempt+1,reason=NULL,next_at=NULL WHERE id=$1",
          [s.id],
        );
        await db.query(
          "UPDATE cycle_occurrences SET state='running',reason=NULL,updated_at=now() WHERE id=$1",
          [s.occurrence_id],
        );
        await cycleEvent(
          db,
          s.cycle_id,
          s.occurrence_id,
          'STEP_STARTED',
          { account: account?.label, model },
          s.id,
          id,
        );
        return {
          s,
          a: attempt,
          account,
          o: {
            id: s.occurrence_id,
            cycle_id: s.cycle_id,
            definition: s.occurrence_definition,
            input: s.input,
            version: s.version,
            title: s.occurrence_title,
          },
        };
      }
      return null;
    });
  }
  async reconcile() {
    const rows = (
      await pool.query(
        `SELECT a.*,s.occurrence_id,s.key,s.definition,o.cycle_id,o.definition cycle_definition,o.state occurrence_state FROM cycle_attempts a JOIN cycle_steps s ON s.id=a.step_id JOIN cycle_occurrences o ON o.id=s.occurrence_id WHERE a.state='reconciling' AND a.account_id IS NOT NULL AND a.heartbeat_at<now()-interval '15 seconds' ORDER BY a.heartbeat_at LIMIT 2`,
      )
    ).rows;
    for (const a of rows) {
      if (this.active.has(a.id)) continue;
      await pool.query('UPDATE cycle_attempts SET heartbeat_at=now() WHERE id=$1', [a.id]);
      try {
        const proof = await inspectStopped(a.id, false),
          p = proof.process;
        const workspace =
          a.cycle_definition.project +
          '/oc' +
          a.occurrence_id.replaceAll('-', '').slice(0, 18) +
          '-' +
          a.key;
        const age = Date.now() - Date.parse(proof.checkedAt);
        if (
          proof.runId !== a.id ||
          proof.workspace !== workspace ||
          p?.unit !== 'agentos-run-' + a.id ||
          p.stopped !== true ||
          p.mainPid !== 0 ||
          p.populated !== false ||
          !['inactive', 'failed'].includes(p.activeState) ||
          !Number.isFinite(age) ||
          age < -5000 ||
          age > 60000
        )
          continue;
        try {
          const delivery = await collectStoppedArtifacts(a.id, a.definition.readOnly);
          await saveCycleArtifacts(a.id, delivery.artifacts ?? []);
        } catch {}
        await tx(async (db) => {
          await lock(db);
          const changed = await db.query(
            "UPDATE cycle_attempts SET state='failed',finished_at=now(),error_code='STOP_CONFIRMED',receipt=$2,fence=$3 WHERE id=$1 AND state='reconciling' RETURNING id",
            [a.id, JSON.stringify(proof), randomUUID()],
          );
          if (!changed.rowCount) return;
          const cancelling = ['cancelling', 'cancelled'].includes(a.occurrence_state),
            retry = a.number < a.definition.maxAttempts;
          await db.query(
            "UPDATE cycle_steps SET state=$2,reason=$3,next_at=now()+interval '15 seconds' WHERE id=$1",
            [
              a.step_id,
              cancelling ? 'cancelled' : retry ? 'retry' : 'attention',
              'Executor encerrado; arquivos preservados.',
            ],
          );
          if (!cancelling)
            await db.query(
              "UPDATE cycle_occurrences SET state=$2,reason=$3 WHERE id=$1 AND state<>'paused'",
              [
                a.occurrence_id,
                retry ? 'recovering' : 'attention',
                'Encerramento confirmado; retomada usa os arquivos preservados.',
              ],
            );
          await cycleEvent(
            db,
            a.cycle_id,
            a.occurrence_id,
            'STOP_CONFIRMED',
            { retry: !cancelling && retry },
            a.step_id,
            a.id,
          );
        });
      } catch {}
    }
  }
  async execute({ o, s, a, account }: any) {
    const controller = new AbortController();
    this.active.set(a.id, controller);
    const timer = setInterval(
      () =>
        void pool
          .query(
            "UPDATE cycle_attempts SET heartbeat_at=now() WHERE id=$1 AND fence=$2 AND state='running' RETURNING state",
            [a.id, a.fence],
          )
          .then(async (changed) => {
            if (!changed.rowCount) controller.abort();
            const state = (
              await pool.query('SELECT state FROM cycle_occurrences WHERE id=$1', [o.id])
            ).rows[0]?.state;
            if (state === 'cancelling') controller.abort();
          })
          .catch(() => controller.abort()),
      3000,
    );
    const emit = async (kind: string, payload: any) => {
      await cycleEvent(pool, o.cycle_id, o.id, kind, clean(payload), s.id, a.id);
    };
    try {
      const result =
        s.definition.kind === 'agent'
          ? await runCycleAgent(o, s, a, account, controller.signal, emit)
          : await runCycleAction(o, s, a, controller.signal, emit);
      await saveCycleArtifacts(a.id, result.artifacts ?? []);
      if (s.definition.action === 'agentos.validate' && result.output?.passed === false)
        throw new DomainError('VALIDATION_CHECKS_FAILED', 'A validação terminou e registrou testes ou cobertura obrigatória sem aprovação.');
      for (const criterion of s.definition.criteria) {
        const artifact = (
          await pool.query('SELECT id FROM cycle_artifacts WHERE attempt_id=$1 AND name=$2', [
            a.id,
            criterion.name,
          ])
        ).rows[0];
        if (criterion.type === 'review')
          throw new DomainError('DELIVERY_INVALID', 'Use uma etapa de revisão explícita.');
        if (!artifact)
          throw new DomainError('DELIVERY_INVALID', 'Evidência ausente: ' + criterion.name);
        if (criterion.type === 'json_field') {
          let value = JSON.parse((await readCycleArtifact(artifact.id)).bytes.toString());
          for (const key of criterion.field.split('.')) value = value?.[key];
          if (value !== criterion.equals)
            throw new DomainError('DELIVERY_INVALID', 'Critério não atendido: ' + criterion.label);
        }
      }
      await tx(async (db) => {
        await lock(db);
        const changed = await db.query(
          "UPDATE cycle_attempts SET state='completed',finished_at=now(),summary=$3,usage=$4,receipt=$5 WHERE id=$1 AND fence=$2 AND state='running' RETURNING id",
          [
            a.id,
            a.fence,
            result.summary ?? 'Ação concluída.',
            JSON.stringify(result.usage ?? null),
            JSON.stringify(result.evidence ?? result.receipt ?? null),
          ],
        );
        if (!changed.rowCount) return;
        await db.query("UPDATE cycle_steps SET state='completed',output=$2 WHERE id=$1", [
          s.id,
          JSON.stringify(
            clean(
              result.output ?? {
                summary: result.summary ?? result.message,
                receipt: result.receipt,
              },
            ),
          ),
        ]);
        await cycleEvent(db, o.cycle_id, o.id, 'STEP_COMPLETED', {}, s.id, a.id);
      });
    } catch (e: any) {
      const code = e.code ?? 'EXECUTION_ERROR';
      const uncertain = code === 'STOP_UNCONFIRMED';
      const retry =
        !uncertain &&
        !controller.signal.aborted &&
        a.number < s.definition.maxAttempts &&
        !['APPROVAL_REQUIRED', 'ACTION_SCOPE', 'ACTION_UNAVAILABLE', 'BITRIX_AUTH'].includes(code);
      await tx(async (db) => {
        await lock(db);
        const changed = await db.query(
          "UPDATE cycle_attempts SET state=$3,finished_at=now(),error_code=$4,summary=$5 WHERE id=$1 AND fence=$2 AND state='running' RETURNING id",
          [
            a.id,
            a.fence,
            uncertain ? 'reconciling' : controller.signal.aborted ? 'cancelled' : 'failed',
            code,
            clean(e.code ? e.message : 'A execução não produziu um resultado válido.'),
          ],
        );
        if (!changed.rowCount) return;
        if (retry && s.definition.repairStep) {
          const files = (
            await db.query('SELECT id,name FROM cycle_artifacts WHERE attempt_id=$1', [a.id])
          ).rows;
          await db.query(
            "UPDATE cycle_steps SET state='queued',reason='Corrigir validação que falhou.',output=COALESCE(output,'{}')||$3::jsonb WHERE occurrence_id=$1 AND key=$2",
            [
              o.id,
              s.definition.repairStep,
              JSON.stringify({ repair: { step: s.key, error: code, artifacts: files } }),
            ],
          );
        }
        await db.query(
          "UPDATE cycle_steps SET state=$2,reason=$3,next_at=now()+($4::int*interval '1 second') WHERE id=$1",
          [
            s.id,
            uncertain
              ? 'reconciling'
              : controller.signal.aborted
                ? 'cancelled'
                : retry
                  ? 'retry'
                  : 'attention',
            e.code ? clean(e.message) : 'Consulte as evidências da tentativa.',
            Math.min(300, 15 * 2 ** (a.number - 1)),
          ],
        );
        await db.query(
          "UPDATE cycle_occurrences SET state=$2,reason=$3,updated_at=now() WHERE id=$1 AND state NOT IN('cancelling','paused')",
          [
            o.id,
            uncertain ? 'reconciling' : retry ? 'recovering' : 'attention',
            uncertain
              ? 'Confirmando término do executor.'
              : retry
                ? 'Nova tentativa agendada; arquivos preservados.'
                : 'Uma etapa precisa de atenção.',
          ],
        );
        await cycleEvent(
          db,
          o.cycle_id,
          o.id,
          retry ? 'RETRY_SCHEDULED' : uncertain ? 'RECONCILING' : 'ATTENTION',
          { code },
          s.id,
          a.id,
        );
      });
    } finally {
      clearInterval(timer);
      this.active.delete(a.id);
      void this.tick();
    }
  }
  async close() {
    this.stopped = true;
    clearInterval(this.timer);
    for (const c of this.active.values()) c.abort();
    while (this.active.size || this.busy) await new Promise((r) => setTimeout(r, 100));
    if (this.owner) {
      await this.owner.query('SELECT pg_advisory_unlock(4280088)');
      this.owner.release();
      this.owner = undefined;
    }
  }
}
export async function resolveCycleStep(id: string, decision: 'approve' | 'retry', note: string) {
  return tx(async (db) => {
    await lock(db);
    const s = (
      await db.query(
        'SELECT s.*,o.cycle_id FROM cycle_steps s JOIN cycle_occurrences o ON o.id=s.occurrence_id WHERE s.id=$1',
        [id],
      )
    ).rows[0];
    if (!s || s.state !== 'attention')
      throw new DomainError('STEP_STATE', 'Etapa não está aguardando decisão.');
    if (decision === 'approve' && s.definition.kind !== 'review')
      throw new DomainError(
        'STEP_STATE',
        'Apenas etapas de revisão podem ser aprovadas manualmente.',
      );
    if (decision === 'retry' && s.definition.kind === 'review')
      throw new DomainError('STEP_STATE', 'Esta etapa exige decisão de revisão.');
    await db.query(
      'UPDATE cycle_steps SET state=$2,next_at=NULL,reason=NULL,output=$3 WHERE id=$1',
      [
        id,
        decision === 'approve' ? 'completed' : 'queued',
        JSON.stringify({ reviewedBy: 'operator', note, at: new Date().toISOString() }),
      ],
    );
    await db.query("UPDATE cycle_occurrences SET state='queued',reason=NULL WHERE id=$1", [
      s.occurrence_id,
    ]);
    await cycleEvent(db, s.cycle_id, s.occurrence_id, 'OPERATOR_DECISION', { decision, note }, id);
    return { ok: true };
  });
}
