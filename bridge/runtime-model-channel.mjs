// Buffer provider output until validated, while keeping the SDK's SSE idle
// watchdog alive. Comments carry transport liveness only, never model progress.
import { messagesError } from './runtime-gateway.mjs';
export const WORKER_MODEL_TIMEOUT_MS = 720_000;
const failure = (code) => Object.assign(new Error(code), { code });

// The pinned runtime speaks Messages; older adapters speak completions. Both
// wires share this buffering and liveness; only the terminal framing differs.
export function bufferedModelChannel(res, callerSignal, { heartbeatMs = 15_000, wire = 'completions' } = {}) {
  const messages = wire === 'messages';
  const controller = new AbortController();
  let timer;
  const clear = () => { clearInterval(timer); timer = undefined; };
  const cancelled = () => { clear(); controller.abort(failure('CANCELLED')); };
  const disconnected = () => {
    clear();
    if (!res.writableEnded) controller.abort(failure('MODEL_CLIENT_DISCONNECTED'));
  };
  callerSignal.addEventListener('abort', cancelled, { once: true });
  res.on('close', disconnected);
  if (callerSignal.aborted) cancelled();
  const writable = () => !res.destroyed && !res.writableEnded;
  return {
    signal: controller.signal,
    start() {
      controller.signal.throwIfAborted();
      res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-cache' });
      res.flushHeaders();
      const heartbeat = () => {
        // Skip a heartbeat on backpressure; never grow a queue for a slow reader.
        if (writable() && !res.writableNeedDrain) res.write(': agentos transport keepalive\n\n');
      };
      heartbeat();
      timer = setInterval(heartbeat, heartbeatMs);
      timer.unref?.();
    },
    finish(frames) {
      controller.signal.throwIfAborted();
      clear();
      if (messages) {
        // Messages has no [DONE] sentinel: message_stop terminates the stream.
        for (const event of frames)
          res.write('event: ' + event.type + '\ndata: ' + JSON.stringify(event) + '\n\n');
        res.end();
        return;
      }
      for (const frame of frames) res.write('data: ' + JSON.stringify(frame) + '\n\n');
      res.end('data: [DONE]\n\n');
    },
    failureCode(error) {
      // User cancellation always wins a race with timeout/disconnection.
      if (callerSignal.aborted) return 'CANCELLED';
      if (error?.name === 'TimeoutError') return 'MODEL_TIMEOUT';
      if (controller.signal.reason?.code === 'MODEL_CLIENT_DISCONNECTED')
        return 'MODEL_CLIENT_DISCONNECTED';
      if (/quota|429|rate.limit/i.test(error?.message ?? '')) return 'QUOTA_EXCEEDED';
      if (/auth|credential|401|403/i.test(error?.message ?? '')) return 'AUTH_REQUIRED';
      return error?.code ?? 'MODEL_REQUEST_FAILED';
    },
    fail(code) {
      clear();
      if (!writable()) return;
      if (messages) {
        const body = JSON.stringify(messagesError(code));
        if (res.headersSent) res.end('event: error\ndata: ' + body + '\n\n');
        else {
          res.writeHead(400, { 'content-type': 'application/json' });
          res.end(body);
        }
        return;
      }
      const payload = JSON.stringify({ error: { message: code, type: 'agentos_gateway' } });
      // Headers may already be sent for keepalives. A protocol error frame is
      // terminal; no [DONE] or completion may follow this failed response.
      if (res.headersSent) res.end('data: ' + payload + '\n\n');
      else {
        res.writeHead(400, { 'content-type': 'application/json' });
        res.end(payload);
      }
    },
    dispose() {
      clear();
      callerSignal.removeEventListener('abort', cancelled);
      res.removeListener('close', disconnected);
    },
  };
}
