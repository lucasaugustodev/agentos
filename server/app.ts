import Fastify from 'fastify';
import fastifyStatic from '@fastify/static';
import { existsSync } from 'node:fs';
import path from 'node:path';
import { randomBytes, timingSafeEqual } from 'node:crypto';
import { z, ZodError } from 'zod';
import { config } from './config.js';
import { pool, tx, event, lock } from './db.js';
import {
  createGoal,
  createDemo,
  snapshot,
  goalDetail,
  cancelTask,
  validateTask,
  readArtifact,
  delegateTasks,
  retryTask,
} from './service.js';
import { saveMemory } from './memory.js';
import { searchAgents } from './registry.js';
import { DomainError } from './contracts.js';
import { recallStatus } from './recall.js';
import { syncAccounts } from './accounts.js';
import { providerHealth, providerHealthWindowMinutes } from './provider-health-query.js';
import { accountBridge } from './bridge.js';
import { chatEngine, listChats, newChat, chatDetail } from './chat.js';
import type { WorkerEngine } from './worker.js';
import { submitObjective, objectiveDetail, feedbackObjective } from './planner.js';
import {registerObjectiveInput} from './objective-inputs.js';
import {objectiveSource,objectiveSourceSummary,objectiveSourceArchive} from './objective-source-archive.js';
import {autonomyLive} from './autonomy-live.js';
import {configureObjectiveReview,objectiveReviews} from './objective-review.js';
import {reconcileInheritedCancellation} from './objective-state-repair.js';
import { cycleRoutes } from './cycles/routes.js';
import { trustedTailnet } from './tailnet.js';
import { panelState, panelGoal, panelTask, panelCollection } from './panel.js';
import { plannerPanel } from './planner-panel.js';
import {requestOperatorRecovery} from './operator-recovery.js';
import {requestOperatorOpenCodeRecovery} from './operator-opencode-recovery.js';
import {authorizeOperationsRequest} from './operations-contract.js';
import {registerOperationsApi} from './operations-api.js';
const equal = (a: string, b: string) => {
  const x = Buffer.from(a),
    y = Buffer.from(b);
  return x.length === y.length && timingSafeEqual(x, y);
};
export function makeApp(engine?: WorkerEngine) {
  const app = Fastify({ logger: false, bodyLimit: 3 * 1024 * 1024, forceCloseConnections: true });
  cycleRoutes(app);
  const sessionKey = randomBytes(32).toString('hex');
  app.setErrorHandler((error: any, _req, reply) => {
    if (error instanceof ZodError)
      return reply.status(400).send({
        error: 'VALIDATION',
        message: error.issues.map((i) => i.path.join('.') + ': ' + i.message).join('; '),
      });
    if (error instanceof DomainError)
      return reply.status(error.status).send({ error: error.code, message: error.message });
    return reply.status(500).send({
      error: 'INTERNAL',
      message: 'A operação falhou. Confira os dados e tente novamente.',
    });
  });
  app.addHook('onRequest', async (req, reply) => {
    reply.header('X-Content-Type-Options', 'nosniff').header('Referrer-Policy', 'same-origin');
    if (!req.url.startsWith('/api/')) return;
    reply.header('Cache-Control', 'no-store');
    const origin = req.headers.origin;
    if (origin && origin !== config.origin)
      return reply.status(403).send({ message: 'Origem não autorizada.' });
    if (req.headers['sec-fetch-site'] === 'cross-site')
      return reply.status(403).send({ message: 'Solicitação externa bloqueada.' });
    if (authorizeOperationsRequest(req)) return;
    const bearer = (req.headers.authorization ?? '').replace(/^Bearer /, '');
    if (bearer && equal(bearer, config.apiToken)) return;
    const host = (req.headers.host ?? '').split(':')[0];
    const tailnet = await trustedTailnet(req);
    if (!tailnet && !['127.0.0.1', 'localhost', '['].includes(host))
      return reply.status(403).send({ message: 'Host não autorizado.' });
    if (req.url === '/api/session' && req.method === 'GET') return;
    const cookie =
      (req.headers.cookie ?? '')
        .split(';')
        .map((x) => x.trim())
        .find((x) => x.startsWith('agentos_session='))
        ?.slice(16) ?? '';
    if (!equal(cookie, sessionKey))
      return reply.status(401).send({ message: 'Inicie uma sessão local.' });
  });
  registerOperationsApi(app);
  app.get('/api/autonomy', async () => autonomyLive());
  app.get('/api/session', async (_req, reply) => {
    reply.header(
      'Set-Cookie',
      `agentos_session=${sessionKey}; HttpOnly; SameSite=Strict; Path=/; Max-Age=43200${config.tailnetHost ? '; Secure' : ''}`,
    );
    return { application: 'agentos', authenticated: true, environment: config.tailnetHost ? 'private-server' : 'development' };
  });
  app.get('/api/state', async () => ({
    ...(await snapshot()),
    recall: await recallStatus(),
    engine: engine
      ? {
          lastTick: engine.lastTick,
          error: engine.lastError,
          workerId: engine.workerId,
          workerSlots: config.workerSlots,
          active: engine.active.size,
        }
      : null,
  }));
  app.get('/api/panel/state', async () => {
    const state=await panelState();
    return {...state,recall:await recallStatus(),
      engine:config.role==='api' ? state.engine : engine ? {lastTick:engine.lastTick,error:engine.lastError,
        workerId:engine.workerId,workerSlots:config.workerSlots,active:engine.active.size} : state.engine};
  });
  app.get('/api/panel/goals/:id', async req => panelGoal(z.uuid().parse((req.params as any).id)));
  app.get('/api/panel/goals/:id/planners', async req => plannerPanel(z.uuid().parse((req.params as any).id)));
  app.get('/api/panel/tasks/:id', async req => {
    const query=z.object({before:z.coerce.number().int().positive().optional(),
      limit:z.coerce.number().int().min(1).max(50).default(20)}).parse(req.query);
    return panelTask(z.uuid().parse((req.params as any).id),query.before,query.limit);
  });
  app.get('/api/panel/collections/:kind', async req => {
    const kind=z.enum(['artifacts','memories','contexts','runs']).parse((req.params as any).kind);
    const query=z.object({before:z.uuid().optional(),goalId:z.uuid().optional(),limit:z.coerce.number().int().min(1).max(100).default(50)}).parse(req.query);
    return panelCollection(kind,query.before,query.limit,query.goalId);
  });
  app.get('/api/panel/contexts/:id', async req => {
    const row=(await pool.query('SELECT * FROM contexts WHERE id=$1',[z.uuid().parse((req.params as any).id)])).rows[0];
    if(!row) throw new DomainError('NOT_FOUND','Contexto não encontrado.',404);
    return row;
  });
  app.get('/api/agents', async (req) => {
    const q = req.query as any;
    return searchAgents(q.q ?? '', q.capabilities?.split(',') ?? []);
  });
  app.post('/api/goals', async (req) => createGoal(req.body));
  app.get('/api/workspaces', async () => accountBridge.request('worker.workspaces'));
  app.post('/api/objectives', async (req) => submitObjective(req.body));
  app.post('/api/objectives/:id/inputs',async req=>{
    const bearer=(req.headers.authorization??'').replace(/^Bearer /,'');
    return registerObjectiveInput(z.uuid().parse((req.params as any).id),req.body,
      {channel:bearer&&equal(bearer,config.apiToken)?'api-token':'operator-session'});
  });
  app.get('/api/objectives/:id/reviews',async req=>objectiveReviews(z.uuid().parse((req.params as any).id)));
  app.post('/api/objectives/:id/review-policy',async req=>configureObjectiveReview(z.uuid().parse((req.params as any).id),req.body));
  app.get('/api/objectives/:id', async (req) =>
    objectiveDetail(z.uuid().parse((req.params as any).id)),
  );
  app.get('/api/objectives/:id/source', async (req) =>
    objectiveSourceSummary(await objectiveSource(z.uuid().parse((req.params as any).id))),
  );
  app.get('/api/objectives/:id/source.zip', async (req, reply) => {
    const zip = await objectiveSourceArchive(z.uuid().parse((req.params as any).id));
    return reply
      .header('Content-Type', 'application/zip')
      .header('X-Content-Type-Options', 'nosniff')
      .header('Cache-Control', 'no-store')
      .header('X-Archive-SHA256', zip.digest)
      .header('Content-Disposition', `attachment; filename="${zip.fileName}"`)
      .send(zip.bytes);
  });
  app.get('/api/objectives/:id/plan', async (req) =>
    objectiveDetail(z.uuid().parse((req.params as any).id)),
  );
  app.get('/api/objectives/:id/progress', async (req) => ({
    ...(await objectiveDetail(z.uuid().parse((req.params as any).id))),
    ...(await goalDetail((req.params as any).id)),
  }));
  app.post('/api/objectives/:id/feedback', async (req) => {
    const b = z
      .object({
        idempotencyKey: z.string().min(8).max(160),
        description: z.string().min(5).max(5000),
      })
      .parse(req.body);
    return feedbackObjective(
      z.uuid().parse((req.params as any).id),
      b.idempotencyKey,
      b.description,
    );
  });
  app.get('/api/chats', async () => listChats());
  app.post('/api/chats', async (req) => {
    const b = z
      .object({
        title: z.string().min(1).max(100).default('Nova conversa'),
        project: z
          .string()
          .regex(/^[a-z0-9_-]{1,80}$/)
          .default('chat'),
      })
      .parse(req.body ?? {});
    return newChat(b.title, b.project);
  });
  app.get('/api/chats/:id', async (req) => chatDetail(z.uuid().parse((req.params as any).id)));
  app.post('/api/chats/:id/messages', async (req) =>
    chatEngine.send(
      z.uuid().parse((req.params as any).id),
      z
        .object({
          message: z.string().trim().min(1).max(30000),
          requestKey: z.string().min(8).max(160),
          accountId: z.string().optional(),
          model: z.string().max(150).optional(),
        })
        .parse(req.body),
    ),
  );
  app.post('/api/chats/:id/cancel', async (req) =>
    chatEngine.cancel(z.uuid().parse((req.params as any).id)),
  );
  app.get('/api/connections', async () => {
    let error = null;
    try {
      await syncAccounts();
    } catch (e: any) {
      error = e.message;
    }
    const accounts = (
      await pool.query('SELECT * FROM accounts WHERE bridge_id IS NOT NULL ORDER BY source,label')
    ).rows;
    return {
      configured: !!config.bridgeHost,
      error,
      accounts,
      // Whether the provider is actually answering, as opposed to whether the
      // bridge believes it is connected. The two disagree exactly when it matters.
      health: await providerHealth(accounts.map((a: any) => a.id)),
      healthWindowMinutes: providerHealthWindowMinutes,
    };
  });
  app.post('/api/connections/refresh', async () => syncAccounts(true));
  app.post('/api/connections/login', async (req) =>
    accountBridge.request(
      'login.start',
      z
        .object({
          family: z.enum(['codex', 'claude', 'kimi', 'agy', 'muse']),
          label: z.string().min(1).max(100),
        })
        .parse(req.body),
    ),
  );
  app.get('/api/connections/login/active', async () => accountBridge.request('login.active'));
  app.get('/api/connections/login/:id', async (req) => {
    const result = await accountBridge.request('login.state', {
      id: z.uuid().parse((req.params as any).id),
    });
    if (result.status === 'completed') await syncAccounts(true);
    return result;
  });
  app.post('/api/connections/login/:id/answer', async (req) => {
    const b = z.object({ value: z.string().min(1).max(16000) }).parse(req.body);
    return accountBridge.request('login.answer', {
      id: z.uuid().parse((req.params as any).id),
      value: b.value,
    });
  });
  app.post('/api/connections/login/:id/cancel', async (req) =>
    accountBridge.request('login.cancel', { id: z.uuid().parse((req.params as any).id) }),
  );
  app.post('/api/connections/api-key', async (req) => {
    const b = z
      .object({
        family: z.enum(['qwen', 'openai']),
        label: z.string().min(1).max(100),
        baseUrl: z.url(),
        model: z.string().min(1).max(150),
        key: z.string().min(10).max(4096),
      })
      .parse(req.body);
    const result = await accountBridge.request('account.addKey', b);
    await syncAccounts(true);
    return result;
  });
  app.post('/api/demo', async () => createDemo());
  app.get('/api/goals/:id', async (req) => goalDetail(z.uuid().parse((req.params as any).id)));
  app.post('/api/goals/:id/tasks', async (req) =>
    delegateTasks(z.uuid().parse((req.params as any).id), (req.body as any)?.tasks),
  );
  app.post('/api/goals/:id/state', async (req) => {
    const id = z.uuid().parse((req.params as any).id),
      body = z.object({ state: z.enum(['active', 'paused']) }).parse(req.body);
    return tx(async (db) => {
      await lock(db);
      if(body.state==='active')await reconcileInheritedCancellation(db,id);
      const changed = await db.query(
        "UPDATE goals SET state=$2 WHERE id=$1 AND state IN('active','paused') RETURNING id,state",
        [id, body.state],
      );
      if (!changed.rowCount)
        throw new DomainError('GOAL_NOT_ACTIVE', 'Objetivo encerrado ou inexistente.');
      await event(db, body.state === 'paused' ? 'GOAL_PAUSED' : 'GOAL_RESUMED', id, {});
      return changed.rows[0];
    });
  });
  app.post('/api/tasks/:id/cancel', async (req) =>
    cancelTask(z.uuid().parse((req.params as any).id)),
  );
  app.post('/api/tasks/:id/interrupt-for-recovery',async(req,reply)=>{
    const bearer=(req.headers.authorization??'').replace(/^Bearer /,'');
    const request=(req.body as any)?.runtime==='opencode'?requestOperatorOpenCodeRecovery:requestOperatorRecovery;
    const result=await request(z.uuid().parse((req.params as any).id),req.body,
      {channel:bearer&&equal(bearer,config.apiToken)?'api-token':'operator-session'});
    return reply.status(result.duplicate?200:202).send(result);
  });
  app.post('/api/tasks/:id/review', async (req) => {
    const id = z.uuid().parse((req.params as any).id);
    z.object({ reviewed: z.literal(true) }).parse(req.body);
    return validateTask(id, 'local-operator');
  });
  app.post('/api/tasks/:id/retry', async (req) => {
    const id = z.uuid().parse((req.params as any).id),
      body = z
        .object({
          checkpoint: z.string().min(10).max(10000),
          effectsReviewed: z.literal(true),
          accountId: z.string().nullable().optional(),
          model: z.string().min(1).max(150).optional(),
        })
        .parse(req.body);
    return retryTask(id, body.checkpoint, { accountId: body.accountId, model: body.model });
  });
  app.get('/api/artifacts/:id', async (req, reply) => {
    const a = await readArtifact(z.uuid().parse((req.params as any).id));
    return reply
      .header(
        'Content-Type',
        ['image/png', 'image/jpeg', 'application/json', 'text/plain'].includes(a.mime)
          ? a.mime
          : 'application/octet-stream',
      )
      .header('X-Content-Type-Options', 'nosniff')
      .header('Content-Disposition', `attachment; filename="${a.name}"`)
      .send(a.bytes);
  });
  app.post('/api/memories', async (req) =>
    saveMemory(
      z
        .object({
          project: z.string().regex(/^[a-z0-9_-]{1,80}$/),
          title: z.string().min(3).max(200),
          content: z.string().min(3).max(30000),
          kind: z.enum(['decision', 'observation', 'evidence']),
          verification: z.enum(['confirmed', 'unverified']),
          source: z.string().min(1).max(200),
          supersedes: z.uuid().optional(),
        })
        .parse(req.body),
    ),
  );
  app.patch('/api/capacity', async (req) => {
    const value = z
      .object({
        maxRuns: z.number().int().min(1).max(100),
        browserSlots: z.number().int().min(0).max(32),
      })
      .parse(req.body);
    await tx(async (db) => {
      await db.query("UPDATE settings SET value=$1 WHERE id='capacity'", [JSON.stringify(value)]);
      await event(db, 'CAPACITY_CHANGED', null, value);
    });
    return value;
  });
  app.post('/api/accounts', async (req) => {
    const a = z
      .object({
        id: z.string().regex(/^[a-z0-9_-]{1,60}$/),
        label: z.string().min(1).max(100),
        provider: z.string().min(1).max(100),
        model: z.string().min(1).max(100),
        enabled: z.boolean().default(false),
        maxConcurrency: z.number().int().min(1).max(32).default(1),
        quotaStatus: z
          .enum(['unknown', 'available', 'exhausted', 'auth_required'])
          .default('unknown'),
        remainingPercent: z.number().min(0).max(100).nullable().default(null),
        allowUnknown: z.boolean().default(false),
      })
      .parse(req.body);
    if (a.id === 'local-validation' || a.provider === 'local')
      throw new DomainError('RESERVED_ACCOUNT', 'Conta reservada ao executor técnico.', 400);
    await tx(async (db) => {
      await db.query(
        `INSERT INTO accounts(id,label,provider,model,enabled,max_concurrency,quota_status,remaining_percent,allow_unknown,observed_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,now()) ON CONFLICT(id) DO UPDATE SET label=excluded.label,provider=excluded.provider,model=excluded.model,enabled=excluded.enabled,max_concurrency=excluded.max_concurrency,quota_status=excluded.quota_status,remaining_percent=excluded.remaining_percent,allow_unknown=excluded.allow_unknown,observed_at=excluded.observed_at`,
        [
          a.id,
          a.label,
          a.provider,
          a.model,
          a.enabled,
          a.maxConcurrency,
          a.quotaStatus,
          a.remainingPercent,
          a.allowUnknown,
        ],
      );
      await event(db, 'ACCOUNT_ROUTE_UPDATED', a.id, { provider: a.provider, model: a.model });
    });
    return { saved: true };
  });
  app.get('/api/events', async (req, reply) => {
    let cursor = Number(req.headers['last-event-id'] ?? (req.query as any).after ?? 0);
    if (!Number.isSafeInteger(cursor) || cursor < 0)
      return reply.status(400).send({ message: 'Cursor inválido.' });
    reply.hijack();
    reply.raw.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-store',
      Connection: 'keep-alive',
      'X-Accel-Buffering': 'no',
    });
    reply.raw.write(': connected\n\n');
    let busy = false;
    const flush = async () => {
      if (busy || reply.raw.destroyed) return;
      busy = true;
      try {
        const rows = (
          await pool.query('SELECT * FROM events WHERE id>$1 ORDER BY id LIMIT 250', [cursor])
        ).rows;
        for (const e of rows) {
          reply.raw.write(`id: ${e.id}\ndata: ${JSON.stringify(e)}\n\n`);
          cursor = Number(e.id);
        }
        if (!rows.length) reply.raw.write(': heartbeat\n\n');
      } catch {
        reply.raw.end();
      } finally {
        busy = false;
      }
    };
    const timer = setInterval(() => void flush(), 1500);
    reply.raw.on('close', () => clearInterval(timer));
    await flush();
  });
  const dist = path.resolve('dist');
  if (existsSync(dist)) {
    app.register(fastifyStatic, { root: dist });
    app.setNotFoundHandler((req, reply) =>
      req.url.startsWith('/api/')
        ? reply.status(404).send({ message: 'Endpoint não encontrado.' })
        : reply.sendFile('index.html'),
    );
  }
  return app;
}
