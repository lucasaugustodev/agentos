import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { createGlmRequestGate } from '../bridge/glm-request-gate.mjs';
import { acquireGlmRequestSlot } from '../bridge/glm-request-slot.mjs';
import {
  GLM_ACCOUNT,
  GLM_REQUEST_ADMITTED,
  glmRequestBudgetMs,
  inGlmRequestLane,
  requestGatedModel,
  withAccountRequestGate,
  withGlmRequestSlot,
} from '../bridge/gated-generation.mjs';
import { glmModelProfile } from '../bridge/glm-model-profile.mjs';

const deferred = () => {
  let resolve;
  const promise = new Promise((done) => {
    resolve = done;
  });
  return { promise, resolve };
};
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
async function until(predicate) {
  const deadline = Date.now() + 3000;
  while (!(await predicate())) {
    if (Date.now() > deadline) assert.fail('Timed out waiting for request state');
    await sleep(5);
  }
}
async function setup(t) {
  const directory = await mkdtemp(path.join(tmpdir(), 'glm-generation-'));
  const socketPath =
    process.platform === 'win32'
      ? `\\\\.\\pipe\\agentos-generation-${randomUUID()}`
      : path.join(directory, 'gate.sock');
  const gate = await createGlmRequestGate({ socketPath, maxConcurrent: 1, refreshMs: 15 });
  t.after(async () => {
    await gate.close();
    await rm(directory, { recursive: true, force: true });
  });
  return { gate, socketPath };
}

test('a delayed SSE body retains its permit after headers and partial data', async (t) => {
  const { gate, socketPath } = await setup(t);
  const headers = deferred();
  let stream,
    fetches = 0;
  const first = requestGatedModel(
    { runId: 'stream', socketPath },
    'https://unused.invalid',
    {},
    async (response) => {
      headers.resolve();
      return response.text();
    },
    {
      fetch: async () => {
        fetches++;
        return new Response(
          new ReadableStream({
            start(controller) {
              stream = controller;
              controller.enqueue(new TextEncoder().encode('data: first\n\n'));
            },
          }),
          { headers: { 'content-type': 'text/event-stream' } },
        );
      },
    },
  );
  await headers.promise;
  const second = requestGatedModel(
    { runId: 'successor', socketPath },
    'https://unused.invalid',
    {},
    (response) => response.text(),
    {
      fetch: async () => {
        fetches++;
        return new Response('second');
      },
    },
  );
  await until(async () => (await gate.status()).queued === 1);
  assert.equal(fetches, 1);
  assert.equal((await gate.status()).active, 1);
  stream.enqueue(new TextEncoder().encode('data: [DONE]\n\n'));
  stream.close();
  assert.equal(await first, 'data: first\n\ndata: [DONE]\n\n');
  assert.equal(await second, 'second');
  assert.equal(fetches, 2);
  await until(async () => (await gate.status()).active === 0);
});

test('a queued cancellation never contacts the model or consumes a permit', async (t) => {
  const { gate, socketPath } = await setup(t);
  const holder = await acquireGlmRequestSlot({ runId: 'holder', socketPath });
  const controller = new AbortController();
  let fetches = 0;
  const work = requestGatedModel(
    { runId: 'cancelled', socketPath, signal: controller.signal },
    'https://unused.invalid',
    {},
    (response) => response.text(),
    {
      fetch: async () => {
        fetches++;
        return new Response('unexpected');
      },
    },
  );
  const rejected = assert.rejects(work, { code: 'GLM_REQUEST_ABORTED' });
  await until(async () => (await gate.status()).queued === 1);
  controller.abort();
  await rejected;
  assert.equal(fetches, 0);
  assert.equal((await gate.status()).active, 1);
  holder.release();
});

test('provider timeout starts after admission, not while waiting in the queue', async (t) => {
  const { gate, socketPath } = await setup(t);
  const holder = await acquireGlmRequestSlot({ runId: 'holder', socketPath });
  let called = false;
  const work = withGlmRequestSlot(
    { runId: 'waiting', socketPath, requestTimeoutMs: 15 },
    async (signal) => {
      signal.throwIfAborted();
      called = true;
      return 'done';
    },
  );
  await until(async () => (await gate.status()).queued === 1);
  await sleep(50);
  assert.equal(called, false);
  holder.release();
  assert.equal(await work, 'done');
});

test('active cancellation holds capacity until the provider finishes aborting', async (t) => {
  const { gate, socketPath } = await setup(t);
  const started = deferred(),
    aborted = deferred(),
    drained = deferred();
  const controller = new AbortController();
  const work = withGlmRequestSlot(
    { runId: 'active', socketPath, signal: controller.signal },
    async (signal) => {
      signal.addEventListener('abort', () => aborted.resolve(), { once: true });
      started.resolve();
      await drained.promise;
      signal.throwIfAborted();
    },
  );
  const rejected = assert.rejects(work, { name: 'AbortError' });
  await started.promise;
  controller.abort();
  await aborted.promise;
  await sleep(25);
  assert.equal((await gate.status()).active, 1);
  drained.resolve();
  await rejected;
  await until(async () => (await gate.status()).active === 0);
});

test('gate connection loss aborts the upstream call and fails closed', async (t) => {
  const { gate, socketPath } = await setup(t);
  const started = deferred();
  let aborted = false;
  const work = withGlmRequestSlot(
    { runId: 'lost', socketPath },
    (signal) =>
      new Promise((resolve, reject) => {
        signal.addEventListener(
          'abort',
          () => {
            aborted = true;
            reject(signal.reason);
          },
          { once: true },
        );
        started.resolve();
      }),
  );
  const rejected = assert.rejects(work, { code: 'GLM_GATE_LOST' });
  await started.promise;
  await gate.close();
  await rejected;
  assert.equal(aborted, true);
});

test('an unavailable gate cannot fall back to a direct request', async () => {
  let fetched = false;
  await assert.rejects(
    requestGatedModel(
      { runId: 'unavailable' },
      'https://unused.invalid',
      {},
      (response) => response.text(),
      {
        acquire: async () => {
          throw Object.assign(new Error('GLM_GATE_UNAVAILABLE'), { code: 'GLM_GATE_UNAVAILABLE' });
        },
        fetch: async () => {
          fetched = true;
          return new Response('unexpected');
        },
      },
    ),
    { code: 'GLM_GATE_UNAVAILABLE' },
  );
  assert.equal(fetched, false);
});

test('generic GLM generations share the gate while unrelated accounts remain untouched', async (t) => {
  const { gate, socketPath } = await setup(t);
  const done = deferred(),
    admitted = deferred();
  const generic = withAccountRequestGate(
    { accountId: GLM_ACCOUNT, runId: 'generic' },
    undefined,
    async () => {
      admitted.resolve();
      return done.promise;
    },
    undefined,
    { acquire: (options) => acquireGlmRequestSlot({ ...options, socketPath }) },
  );
  await admitted.promise;
  let nativeCalled = false;
  const native = withGlmRequestSlot({ runId: 'native', socketPath }, async () => {
    nativeCalled = true;
    return 'native';
  });
  await until(async () => (await gate.status()).queued === 1);
  const originalSignal = new AbortController().signal;
  const other = await withAccountRequestGate(
    { accountId: 'harness:qwen-cloud' },
    originalSignal,
    (signal) => {
      assert.equal(signal, originalSignal);
      return 'other';
    },
    undefined,
    { acquire: () => assert.fail('Must not gate a different account') },
  );
  assert.equal(other, 'other');
  assert.equal(nativeCalled, false);
  done.resolve('generic');
  assert.equal(await generic, 'generic');
  assert.equal(await native, 'native');
});

test('queue events are deduplicated and omit untrusted or secret fields', async () => {
  const events = [];
  let releases = 0;
  await withGlmRequestSlot(
    { runId: 'observed', emit: (kind, payload) => events.push({ kind, payload }) },
    async () => 'done',
    {
      acquire: async ({ onState }) => {
        const queued = {
          state: 'queued',
          position: 2,
          active: 6,
          legacyReserved: 0,
          secret: 'must-not-escape',
        };
        onState(queued);
        onState(queued);
        onState({ ...queued, position: 1 });
        onState({ state: 'acquired', active: 6 });
        return {
          release: () => {
            releases++;
          },
        };
      },
    },
  );
  assert.deepEqual(
    events.map((event) => event.kind),
    ['MODEL_QUEUE_WAIT', 'MODEL_QUEUE_WAIT', 'MODEL_REQUEST_ADMITTED', 'MODEL_REQUEST_RELEASED'],
  );
  assert.equal(JSON.stringify(events).includes('must-not-escape'), false);
  assert.equal(releases, 1);
});

test('only the private in-process admission identity avoids a second gate', async () => {
  let acquisitions = 0,
    releases = 0;
  const dependencies = {
    acquire: async ({ onState }) => {
      acquisitions++;
      onState({ state: 'acquired' });
      return {
        release: () => {
          releases++;
        },
      };
    },
  };
  const params = { accountId: GLM_ACCOUNT, runId: 'harness' };
  const result = await withAccountRequestGate(
    params,
    undefined,
    (signal) =>
      withAccountRequestGate(
        params,
        signal,
        async () => 'generated',
        undefined,
        dependencies,
        GLM_REQUEST_ADMITTED,
      ),
    undefined,
    dependencies,
  );
  assert.equal(result, 'generated');
  assert.equal(acquisitions, 1);
  assert.equal(releases, 1);
  // Neither a JSON field nor a same-description symbol is valid proof.
  for (const admission of [
    'agentos.glm.request.admitted',
    Symbol('agentos.glm.request.admitted'),
  ]) {
    await withAccountRequestGate(
      { ...params, admission: true },
      undefined,
      async () => 'direct',
      undefined,
      dependencies,
      admission,
    );
  }
  assert.equal(acquisitions, 3);
  assert.equal(releases, 3);
});

test('an admitted provider timeout retains its reason and releases the permit', async (t) => {
  const { gate, socketPath } = await setup(t);
  await assert.rejects(
    withGlmRequestSlot(
      { runId: 'timeout', socketPath, requestTimeoutMs: 15 },
      (signal) =>
        new Promise((resolve, reject) => {
          signal.addEventListener('abort', () => reject(new Error('provider translated abort')), {
            once: true,
          });
        }),
    ),
    { name: 'TimeoutError' },
  );
  await until(async () => (await gate.status()).active === 0);
});

test('an installation account running the verified model keeps the GLM lane and its contract', () => {
  const own = 'agentos:api:cb017dea-b631-49ae-bd6b-36655265c9d6';
  assert.equal(inGlmRequestLane({ accountId: own, model: 'glm-5.3-flash' }), true);
  // The profile carries the endpoint's thinking contract, not generic defaults.
  const profile = glmModelProfile(own, 'glm-5.3-flash');
  assert.equal(profile.reasoning, true);
  assert.equal(profile.compat.thinkingFormat, 'zai');
  // Another model on the same account is not the verified GLM endpoint.
  assert.deepEqual(glmModelProfile(own, 'deepseek-v4.1-flash'), {});
  assert.deepEqual(glmModelProfile('harness:qwen-cloud', 'qwen-max'), {});
});

test('the provider budget is configured within bounds, never silently shortened', () => {
  const original = process.env.AGENTOS_GLM_REQUEST_TIMEOUT_MS;
  try {
    delete process.env.AGENTOS_GLM_REQUEST_TIMEOUT_MS;
    assert.equal(glmRequestBudgetMs(), 720000);
    process.env.AGENTOS_GLM_REQUEST_TIMEOUT_MS = '1500000';
    assert.equal(glmRequestBudgetMs(), 1500000);
    for (const refused of ['30000', '7200000', 'soon', '']) {
      process.env.AGENTOS_GLM_REQUEST_TIMEOUT_MS = refused;
      assert.equal(glmRequestBudgetMs(), 720000);
    }
  } finally {
    if (original === undefined) delete process.env.AGENTOS_GLM_REQUEST_TIMEOUT_MS;
    else process.env.AGENTOS_GLM_REQUEST_TIMEOUT_MS = original;
  }
});
