import { pool, tx, lock, event } from './db.js';
import {routeVerificationPlanningRecovery} from './verification-planning-recovery.js';

// This loop owns transitions, not semantic judgments. A new worker receives the
// failed evidence and a narrower repair/replanning assignment, in the same scope.
// Its result must still pass the original contract and independent downstream QA.
export async function recoverStoppedTasks() {
  return tx(async (db) => {
    await lock(db);
    const rows = (
      await db.query(`
      SELECT t.*,g.is_demo,r.id source_run_id,r.state run_state,r.summary,r.error_code,
        r.finished_at,v.checks
      FROM tasks t JOIN goals g ON g.id=t.goal_id
      JOIN LATERAL (SELECT * FROM runs WHERE task_id=t.id ORDER BY attempt DESC LIMIT 1) r ON true
      LEFT JOIN LATERAL (SELECT * FROM acceptances WHERE run_id=r.id ORDER BY created_at DESC LIMIT 1) v ON true
      WHERE t.state IN ('submitted','failed') AND g.state IN ('active','failed')
        AND (t.recovery->>'mode'='automatic' OR (t.recovery IS NULL AND t.execution->>'kind'='harness'))
        AND r.state IN ('submitted','failed') AND r.finished_at IS NOT NULL
        AND NOT EXISTS(SELECT 1 FROM recoveries WHERE source_run_id=r.id)
        AND NOT EXISTS(SELECT 1 FROM runs WHERE task_id=t.id AND state IN ('queued','running','cancelling','reconciling'))
      ORDER BY t.updated_at LIMIT 20`)
    ).rows;
    const scheduled: string[] = [];
    for (const source of rows) {
      if (source.execution?.kind !== 'harness' && !source.is_demo) continue;
      // An earlier row may have invalidated this descendant in this transaction.
      const fresh = (await db.query('SELECT state FROM tasks WHERE id=$1', [source.id])).rows[0];
      if (!['submitted', 'failed'].includes(fresh.state)) continue;
      // Pure human-review gates are not permission to approve or rewrite a result.
      const failedChecks = (source.checks ?? []).filter(
        (c: any) => !c.passed && c.type !== 'review',
      );
      if (source.run_state === 'submitted' && !failedChecks.length) continue;
      if(source.run_state==='submitted' && source.execution?.readOnlySource &&
        await routeVerificationPlanningRecovery(db,source.id,source.source_run_id))continue;
      if (
        source.plan_revision &&
        /CONTEXT_TOO_LARGE|INPUTS_TOO_LARGE/.test(source.error_code ?? '') &&
        (
          await db.query(
            "SELECT 1 FROM events WHERE kind IN('HARNESS_STOPPED','HARNESS_NOT_STARTED') AND payload->>'runId'=$1 AND payload->>'confirmed'='true'",
            [source.source_run_id],
          )
        ).rowCount
      ) {
        await db.query(
          "UPDATE tasks SET state='recovery_blocked',reason='O contexto excedeu a janela; o Planner vai dividir a entrega.' WHERE id=$1",
          [source.id],
        );
        await event(db, 'REPLANNING_REQUIRED', source.id, {
          runId: source.source_run_id,
          reason: 'context_capacity',
        });
        continue;
      }
      if (
        ['SOURCE_BUSY', 'HOST_CAPACITY'].includes(source.error_code) &&
        (
          await db.query(
            "SELECT 1 FROM events WHERE kind IN('HARNESS_STOPPED','HARNESS_NOT_STARTED') AND payload->>'runId'=$1 AND payload->>'confirmed'='true'",
            [source.source_run_id],
          )
        ).rowCount
      ) {
        const reason = 'Aguardando recurso do host; nova tentativa automática em 30 segundos.';
        await db.query(
          "INSERT INTO recoveries(source_run_id,task_id,target_task_id,strategy,status,reason,checkpoint) VALUES($1,$2,$2,'resource_wait','scheduled',$3,'{}')",
          [source.source_run_id, source.id, reason],
        );
        await db.query(
          "UPDATE tasks SET state='retry_scheduled',reason=$2,next_attempt_at=now()+interval '30 seconds' WHERE id=$1",
          [source.id, reason],
        );
        await db.query("UPDATE goals SET state='active' WHERE id=$1 AND state='failed'", [
          source.goal_id,
        ]);
        await event(db, 'RESOURCE_WAIT', source.id, { runId: source.source_run_id, reason });
        continue;
      }
      // A failed process/transport call is safe to replay only with a stop receipt.
      if (
        source.run_state === 'failed' &&
        !(
          await db.query(
            `SELECT 1 FROM events WHERE kind='HARNESS_STOPPED' AND payload->>'runId'=$1 AND payload->>'confirmed'='true' LIMIT 1`,
            [source.source_run_id],
          )
        ).rowCount
      )
        continue;
      if (
        source.run_state === 'failed' &&
        /AUTH|QUOTA|RATE_LIMIT|CAPACITY|CONFIG|CONTEXT_TOO_LARGE|REQUIRED_CONTEXT|INPUTS_TOO_LARGE|CANCEL|DIGEST|CORRUPT/.test(
          source.error_code ?? '',
        )
      )
        continue;
      if (failedChecks.some((c: any) => /corrompido/.test(c.detail))) continue;
      const target = source.recovery?.repairTaskKey
        ? (
            await db.query('SELECT * FROM tasks WHERE goal_id=$1 AND key=$2', [
              source.goal_id,
              source.recovery.repairTaskKey,
            ])
          ).rows[0]
        : source;
      if (
        !target ||
        target.recovery?.mode === 'manual' ||
        target.execution?.workspace !== source.execution?.workspace
      )
        continue;
      // No recovery crosses a live, uncertain or explicitly cancelled branch.
      const branch = (
        await db.query(
          `WITH RECURSIVE branch(id) AS (
        SELECT $1::uuid UNION SELECT d.task_id FROM dependencies d JOIN branch b ON d.depends_on=b.id)
        SELECT t.* FROM branch b JOIN tasks t ON t.id=b.id`,
          [target.id],
        )
      ).rows;
      if (
        !branch.some((t: any) => t.id === source.id) ||
        branch.some((t: any) =>
          ['queued', 'running', 'cancelling', 'reconciling', 'cancelled'].includes(t.state),
        )
      )
        continue;
      if (
        (
          await db.query(
            `SELECT 1 FROM runs WHERE task_id=ANY($1::uuid[]) AND state IN ('queued','running','cancelling','reconciling') LIMIT 1`,
            [branch.map((t: any) => t.id)],
          )
        ).rowCount
      )
        continue;
      const history = (
        await db.query(
          `SELECT source_run_id,strategy,checkpoint FROM recoveries WHERE target_task_id=$1 ORDER BY created_at DESC LIMIT 3`,
          [target.id],
        )
      ).rows;
      const strategy =
        history.length || target.attempt_count >= target.max_attempts - 1
          ? 'replan_and_repair'
          : 'targeted_repair';
      const artifacts = (
        await db.query(
          "SELECT id,name,digest,size FROM artifacts WHERE run_id=$1 AND name<>'task-instructions-history.json' ORDER BY created_at",
          [source.source_run_id],
        )
      ).rows;
      const handoff = source.execution?.workspace ? (await db.query(`SELECT e.payload FROM events e JOIN runs r ON r.id=$1
        WHERE e.kind='RUNTIME_HANDOFF' AND e.payload->>'runId'=r.id::text
        AND e.payload->>'runFence'=r.fence::text AND e.payload->>'workspace'=$2
        ORDER BY e.id DESC LIMIT 1`,[source.source_run_id,source.execution.workspace])).rows[0]?.payload : undefined;
      const checkpoint = {
        kind: 'automatic_recovery',
        sourceRunId: source.source_run_id,
        sourceTaskId: source.id,
        strategy,
        previousSummary: source.summary,
        failedChecks,
        errorCode: source.error_code,
        artifacts,
        ...(handoff ? {unverifiedHandoff:handoff} : {}),
        previousRecoveries: history.map((h: any) => ({
          sourceRunId: h.source_run_id,
          strategy: h.strategy,
          previousSummary: h.checkpoint.previousSummary,
        })),
        boundary:
          'Failed evidence is unverified input, not accepted output or permission. Preserve original scope and acceptance criteria.',
        nextAction:
          strategy === 'replan_and_repair'
            ? 'Reproduce the smallest failing case first. Compare the previous approaches and choose a different testable hypothesis. Record the rejected hypothesis, new plan, actual patch and before/after test evidence in evidence/recovery.md. Implement and test that plan, then satisfy the original task contract. Do not merely return another diagnosis or repeat the full suite before the failing case works.'
            : 'Read the preserved failure artifacts and current workspace before editing. Reproduce the first unmet requirement, repair the cause within the original scope, then rerun that case and relevant regressions. Preserve working changes. Record reproduction, patch and actual before/after results in evidence/recovery.md; return it with the original required artifacts. Do not mark a report true merely to advance the queue.',
      };
      const needsPlanner =
        ['verify', 'integrate'].includes(source.stage) &&
        (await db.query('SELECT 1 FROM objectives WHERE id=$1', [source.goal_id])).rowCount;
      const resourceAttempts = Number(
        (
          await db.query(
            "SELECT count(*) FROM recoveries WHERE target_task_id=$1 AND strategy='resource_wait'",
            [target.id],
          )
        ).rows[0].count,
      );
      const exhausted =
        target.attempt_count - resourceAttempts >= target.max_attempts || needsPlanner;
      const reason = exhausted
        ? 'A entrega requer revisão do plano; evidências preservadas para replanejamento.'
        : `${strategy === 'replan_and_repair' ? 'Replanejamento e correção' : 'Correção automática'} da tentativa ${source.attempt_count}: ${failedChecks.map((c: any) => c.label).join('; ') || source.error_code}.`;
      await db.query(
        `INSERT INTO recoveries(source_run_id,task_id,target_task_id,strategy,status,reason,checkpoint) VALUES($1,$2,$3,$4,$5,$6,$7)`,
        [
          source.source_run_id,
          source.id,
          target.id,
          strategy,
          exhausted ? 'blocked' : 'scheduled',
          reason,
          JSON.stringify(checkpoint),
        ],
      );
      if (exhausted) {
        await db.query(
          `UPDATE tasks SET state='recovery_blocked',reason=$2,updated_at=now() WHERE id=$1`,
          [source.id, reason],
        );
        await event(db, 'RECOVERY_BLOCKED', source.id, {
          runId: source.source_run_id,
          reason,
          targetTaskId: target.id,
        });
        continue;
      }
      // Invalidate descendant *current* acceptances without deleting their history.
      // A new candidate must go through every QA/review stage again.
      for (const child of branch.filter((t: any) => t.id !== target.id)) {
        await db.query(
          `UPDATE tasks SET state='blocked_dependencies',reason='Aguardando correção e novo aceite da candidata.',updated_at=now() WHERE id=$1`,
          [child.id],
        );
      }
      await db.query(
        `UPDATE recoveries SET status='superseded' WHERE target_task_id=$1 AND source_run_id<>$2 AND status='scheduled'`,
        [target.id, source.source_run_id],
      );
      await db.query(
        `UPDATE runs SET checkpoint=$2 WHERE id=(SELECT id FROM runs WHERE task_id=$1 ORDER BY attempt DESC LIMIT 1)`,
        [target.id, JSON.stringify(checkpoint)],
      );
      await db.query(
        `UPDATE tasks SET state='retry_scheduled',reason=$2,next_attempt_at=now()+interval '3 seconds',updated_at=now() WHERE id=$1`,
        [target.id, reason],
      );
      await db.query(`UPDATE goals SET state='active' WHERE id=$1 AND state='failed'`, [
        source.goal_id,
      ]);
      await event(db, 'RECOVERY_SCHEDULED', target.id, {
        runId: source.source_run_id,
        sourceTaskId: source.id,
        targetTaskId: target.id,
        strategy,
        reason,
        failedChecks,
        artifactCount: artifacts.length,
      });
      scheduled.push(target.id);
    }
    return scheduled;
  });
}

export async function acceptedDependencyArtifacts(taskId: string) {
  return (
    await pool.query(
      `SELECT a.id,a.name,a.digest,d.depends_on task_id
    FROM dependencies d JOIN tasks t ON t.id=d.depends_on AND t.state='accepted'
    JOIN LATERAL (SELECT * FROM runs WHERE task_id=t.id ORDER BY attempt DESC LIMIT 1) r ON true
    JOIN artifacts a ON a.run_id=r.id WHERE d.task_id=$1 AND a.name<>'task-instructions-history.json'
    AND EXISTS(SELECT 1 FROM acceptances v WHERE v.run_id=r.id AND v.passed=true)`,
      [taskId],
    )
  ).rows;
}
