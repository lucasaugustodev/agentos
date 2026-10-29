import { strict as assert } from 'node:assert';
import test from 'node:test';
import { modelRequestTiming } from '../bridge/model-request-timing.mjs';

// A fake clock keeps the invariant observable: real elapsed time cannot show
// that queue wait was excluded from the provider measurement.
const clock = (start = 1000) => {
  let at = start;
  return { now: () => at, advance: (ms) => (at += ms) };
};

test('reports nothing before the request is handed to the gate', () => {
  const t = modelRequestTiming({ now: clock().now });
  assert.deepEqual(t.sample(), {});
});

test('separates queue wait from provider time', () => {
  const c = clock();
  const t = modelRequestTiming({ now: c.now });
  t.start();
  c.advance(30_000);
  t.observe('MODEL_REQUEST_ADMITTED');
  c.advance(5_000);
  assert.deepEqual(t.sample(), { durationMs: 35_000, queuedMs: 30_000, providerMs: 5_000 });
});

test('a request that never got a permit reports no provider time', () => {
  const c = clock();
  const t = modelRequestTiming({ now: c.now });
  t.start();
  c.advance(720_000);
  // Null, not zero: the provider was never asked, so it owes nothing.
  assert.deepEqual(t.sample(), { durationMs: 720_000, queuedMs: 720_000, providerMs: null });
});

test('an ungated request never queues', () => {
  const c = clock();
  const t = modelRequestTiming({ gated: false, now: c.now });
  t.start();
  c.advance(12_000);
  assert.deepEqual(t.sample(), { durationMs: 12_000, queuedMs: 0, providerMs: 12_000 });
});

test('ignores unrelated gate events and a repeated admission', () => {
  const c = clock();
  const t = modelRequestTiming({ now: c.now });
  t.start();
  c.advance(2_000);
  t.observe('MODEL_QUEUE_WAIT');
  t.observe('MODEL_REQUEST_ADMITTED');
  c.advance(1_000);
  // A released/re-admitted permit must not restate the provider's start.
  t.observe('MODEL_REQUEST_ADMITTED');
  c.advance(1_000);
  assert.deepEqual(t.sample(), { durationMs: 4_000, queuedMs: 2_000, providerMs: 2_000 });
});

test('admission before a request is ignored', () => {
  const c = clock();
  const t = modelRequestTiming({ now: c.now });
  t.observe('MODEL_REQUEST_ADMITTED');
  t.start();
  c.advance(3_000);
  assert.deepEqual(t.sample(), { durationMs: 3_000, queuedMs: 3_000, providerMs: null });
});
