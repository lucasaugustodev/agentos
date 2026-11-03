import type { FastifyInstance } from 'fastify';
import { z } from 'zod';
import { pool } from '../db.js';
import {
  createCycle,
  updateCycle,
  setCycleState,
  listCycles,
  cycleDetail,
  occurrenceDetail,
  runCycle,
  commandOccurrence,
} from './store.js';
import { resolveCycleStep } from './engine.js';
import { availableActions } from './integrations.js';
import { readCycleArtifact } from './runtime.js';
import { nativeQA3Evidence } from './native-qa3.js';
const key = z.string().min(8).max(160);
export function cycleRoutes(app: FastifyInstance) {
  app.get('/api/cycles', async () => ({
    cycles: await listCycles(),
    health:
      (await pool.query("SELECT value FROM settings WHERE id='cycle-engine'")).rows[0]?.value ??
      null,
  }));
  app.get('/api/cycles/actions', async () => availableActions());
  app.post('/api/cycles', async (req) => {
    const b = z.object({ definition: z.unknown(), requestKey: key }).parse(req.body);
    return createCycle(b.definition, b.requestKey);
  });
  app.get('/api/cycles/:id', async (req) => cycleDetail(z.uuid().parse((req.params as any).id)));
  app.put('/api/cycles/:id', async (req) => {
    const b = z.object({ version: z.number().int(), definition: z.unknown() }).parse(req.body);
    return updateCycle(z.uuid().parse((req.params as any).id), b.version, b.definition);
  });
  app.post('/api/cycles/:id/state', async (req) => {
    const b = z
      .object({ version: z.number().int(), state: z.enum(['active', 'paused']) })
      .parse(req.body);
    return setCycleState(z.uuid().parse((req.params as any).id), b.version, b.state);
  });
  app.post('/api/cycles/:id/run', async (req) => {
    const b = z
      .object({ requestKey: key, input: z.record(z.string(), z.unknown()).default({}) })
      .parse(req.body);
    return runCycle(z.uuid().parse((req.params as any).id), b.requestKey, b.input);
  });
  app.get('/api/cycle-runs/:id', async (req) =>
    occurrenceDetail(z.uuid().parse((req.params as any).id)),
  );
  app.get('/api/cycle-runs/:id/attempts/:attemptId/cpu-replay', async (req) => {
    const params = z.object({ id: z.uuid(), attemptId: z.uuid() }).parse(req.params);
    return nativeQA3Evidence(params.id, params.attemptId);
  });
  app.post('/api/cycle-runs/:id/command', async (req) =>
    commandOccurrence(
      z.uuid().parse((req.params as any).id),
      z.object({ command: z.enum(['pause', 'resume', 'cancel']) }).parse(req.body).command,
    ),
  );
  app.post('/api/cycle-steps/:id/resolve', async (req) => {
    const b = z
      .object({ decision: z.enum(['approve', 'retry']), note: z.string().min(5).max(5000) })
      .parse(req.body);
    return resolveCycleStep(z.uuid().parse((req.params as any).id), b.decision, b.note);
  });
  app.get('/api/cycle-artifacts/:id', async (req, reply) => {
    const a = await readCycleArtifact(z.uuid().parse((req.params as any).id));
    return reply
      .type(a.mime)
      .header('Content-Disposition', `attachment; filename="${a.name}"`)
      .send(a.bytes);
  });
}
