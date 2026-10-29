import { pool } from './db.js';

/** Live autonomy read model: how much the operation advances without the
 * operator, computed from the database on each request. Read-only. */
const attentionStates = ['blocked_failed', 'waiting_external', 'recovery_blocked', 'waiting_configuration'];
const liveStates = ['queued', 'running', 'cancelling', 'reconciling'];

export async function autonomyLive(windowHours = 24) {
  const db = await pool.connect();
  try {
    await db.query('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');
    const objectives = (await db.query(
      `SELECT g.id,g.title,g.project,g.state,o.status objective_status,g.created_at,
         count(t.id) FILTER(WHERE t.state<>'superseded')::int task_count,
         count(t.id) FILTER(WHERE t.state='accepted')::int accepted_count,
         count(t.id) FILTER(WHERE t.state=ANY($1))::int attention_count,
         (SELECT max(a.created_at) FROM acceptances a JOIN tasks x ON x.id=a.task_id
           WHERE x.goal_id=g.id AND a.passed) last_accepted_at,
         (SELECT count(*)::int FROM runs r JOIN tasks x ON x.id=r.task_id
           WHERE x.goal_id=g.id AND r.state=ANY($2)) running,
         (SELECT count(*)::int FROM runs r JOIN tasks x ON x.id=r.task_id
           WHERE x.goal_id=g.id AND r.created_at>now()-make_interval(hours=>$3)) attempts,
         (SELECT count(*)::int FROM runs r JOIN tasks x ON x.id=r.task_id
           WHERE x.goal_id=g.id AND r.state='failed' AND r.created_at>now()-make_interval(hours=>$3)) failures,
         (SELECT count(*)::int FROM objective_feedback f
           WHERE f.objective_id=g.id AND f.created_at>now()-make_interval(hours=>$3)) interventions
       FROM goals g LEFT JOIN objectives o ON o.id=g.id LEFT JOIN tasks t ON t.goal_id=g.id
       WHERE NOT g.is_demo AND g.merged_into IS NULL AND g.state<>'cancelled'
       GROUP BY g.id,o.status ORDER BY g.created_at DESC LIMIT 20`,
      [attentionStates, liveStates, windowHours],
    )).rows;
    const accounts = (await db.query(
      `SELECT a.id,a.label,a.model,a.max_concurrency,
         count(r.id)::int attempts,
         count(r.id) FILTER(WHERE r.state='submitted')::int succeeded,
         count(r.id) FILTER(WHERE r.state='failed')::int failed,
         count(r.id) FILTER(WHERE r.error_code='MODEL_TIMEOUT')::int timeouts,
         count(r.id) FILTER(WHERE r.state=ANY($2))::int running,
         round(avg(extract(epoch FROM r.finished_at-r.started_at)) FILTER(WHERE r.state='submitted'))::int avg_success_seconds
       FROM accounts a LEFT JOIN runs r ON r.account_id=a.id AND (r.created_at>now()-make_interval(hours=>$1) OR r.state=ANY($2))
       WHERE a.enabled AND a.provider<>'local' GROUP BY a.id ORDER BY a.id`,
      [windowHours, liveStates],
    )).rows;
    const errors = (await db.query(
      `SELECT error_code code,count(*)::int n FROM runs
       WHERE state='failed' AND error_code IS NOT NULL AND created_at>now()-make_interval(hours=>$1)
       GROUP BY 1 ORDER BY 2 DESC,1 LIMIT 6`,
      [windowHours],
    )).rows;
    const active = (await db.query(
      `SELECT r.id,r.state,r.model,r.started_at,r.heartbeat_at,t.id task_id,t.title,t.stage,t.goal_id,a.label account_label
       FROM runs r JOIN tasks t ON t.id=r.task_id LEFT JOIN accounts a ON a.id=r.account_id
       WHERE r.state=ANY($1) ORDER BY r.started_at NULLS LAST`,
      [liveStates],
    )).rows;
    await db.query('COMMIT');
    const attempts = accounts.reduce((n, a) => n + a.attempts, 0);
    const failed = accounts.reduce((n, a) => n + a.failed, 0);
    return {
      observedAt: new Date().toISOString(),
      windowHours,
      objectives,
      accounts,
      errors,
      active,
      totals: {
        attempts,
        failed,
        succeeded: accounts.reduce((n, a) => n + a.succeeded, 0),
        interventions: objectives.reduce((n, o) => n + o.interventions, 0),
        attention: objectives.reduce((n, o) => n + o.attention_count, 0),
      },
    };
  } catch (error) {
    await db.query('ROLLBACK').catch(() => {});
    throw error;
  } finally {
    db.release();
  }
}
