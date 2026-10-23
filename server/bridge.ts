import { assertModelRoute } from '../bridge/model-policy.mjs';
import { spawn, type ChildProcessWithoutNullStreams } from 'node:child_process';
import { createInterface } from 'node:readline';
import { randomUUID } from 'node:crypto';
import { config } from './config.js';
import { DomainError } from './contracts.js';
import { bridgeScript, localBridgeScript } from './bridge-path.js';

export class AccountBridge {
  child?: ChildProcessWithoutNullStreams;
  pending = new Map<
    string,
    {
      resolve: (v: any) => void;
      reject: (e: Error) => void;
      timer: ReturnType<typeof setTimeout>;
      cleanup: () => void;
      child: ChildProcessWithoutNullStreams;
      method: string;
      cancelError?: DomainError;
      onEvent?: (kind: string, payload: Record<string, unknown>) => void;
      onProgress?: (kind: string) => void;
    }
  >();
  connect() {
    if (this.child && !this.child.killed) return this.child;
    const child = config.bridgeHost
      ? spawn(
          'ssh',
          [
            '-T',
            '-o',
            'BatchMode=yes',
            '-o',
            'ConnectTimeout=8',
            '-o',
            'ServerAliveInterval=15',
            '-o',
            'ServerAliveCountMax=2',
            config.bridgeHost,
            (process.env.AGENTOS_GLM_ONLY === '1' ? 'env AGENTOS_GLM_ONLY=1 ' : '') +
            (process.env.AGENTOS_NODE_BIN || '/opt/deepseek-harness/node/bin/node') + ' ' + bridgeScript('server.mjs'),
          ],
          { windowsHide: true, stdio: ['pipe', 'pipe', 'pipe'] },
        )
      : spawn(
          process.execPath,
          [localBridgeScript('server.mjs')],
          {
            windowsHide: true,
            stdio: ['pipe', 'pipe', 'pipe'],
            env: {
              ...process.env,
              AGENTOS_BRIDGE_DATA: config.bridgeDataDir,
              AGENTOS_WORKSPACES_ROOT: config.workspacesRoot,
              // A bridge on this host owns its own request gate.
              AGENTOS_LOCAL_GLM_GATE: '1',
            },
          },
        );
    this.child = child;
    // A local bridge reports its own startup failures to this operator's log.
    // A remote one keeps its stderr off this host.
    if (config.bridgeHost) child.stderr.resume();
    else child.stderr.pipe(process.stderr);
    child.stdin.on('error', () => {});
    const fail = () => {
      if (this.child === child) this.child = undefined;
      for (const [id, p] of this.pending) {
        if (p.child !== child) continue;
        clearTimeout(p.timer);
        p.cleanup();
        p.reject(
          new DomainError(
            ['generate', 'worker.execute'].includes(p.method)
              ? 'STOP_UNCONFIRMED'
              : 'BRIDGE_DISCONNECTED',
            'A conexão privada foi interrompida. O término remoto não foi confirmado; o envio não será repetido automaticamente.',
          ),
        );
        this.pending.delete(id);
      }
    };
    child.on('error', fail);
    child.on('close', fail);
    const lines = createInterface({ input: child.stdout });
    lines.on('line', (line) => {
      let message: any;
      try {
        message = JSON.parse(line);
      } catch {
        return;
      }
      const p = this.pending.get(message.id);
      if (!p) return;
      if (message.event) {
        p.onProgress?.(message.event.kind);
        p.onEvent?.(message.event.kind, message.event.payload);
        return;
      }
      this.pending.delete(message.id);
      clearTimeout(p.timer);
      p.cleanup();
      message.error?.code === 'STOP_UNCONFIRMED'
        ? p.reject(new DomainError('STOP_UNCONFIRMED', message.error.message))
        : p.cancelError
          ? p.reject(p.cancelError)
          : message.error
            ? p.reject(
                new DomainError(
                  message.error.code ?? 'BRIDGE_ERROR',
                  message.error.message ?? 'Falha na conta.',
                ),
              )
            : p.resolve(message.result);
    });
    return child;
  }
  request(
    method: string,
    params: unknown = {},
    options: {
      signal?: AbortSignal;
      timeout?: number;
      idleTimeout?: number;
      onEvent?: (kind: string, payload: Record<string, unknown>) => void;
    } = {},
  ) {
    if (['generate', 'worker.execute'].includes(method)) {
      const route = params as { accountId: string; model: string };
      try { assertModelRoute(route.accountId, route.model); }
      catch (error: any) {
        if (method === 'worker.execute') {
          try { options.onEvent?.('HARNESS_NOT_STARTED', {confirmed:true,phase:'model_policy',code:error.code??'MODEL_POLICY_GLM_ONLY'}); } catch {}
        }
        return Promise.reject(error);
      }
    }
    if (options.idleTimeout !== undefined &&
      (method !== 'worker.execute' || !Number.isFinite(options.idleTimeout) || options.idleTimeout <= 0))
      return Promise.reject(new DomainError('INVALID_BRIDGE_TIMEOUT', 'Prazo de inatividade inválido.'));
    if (options.signal?.aborted)
      return Promise.reject(new DomainError('CANCELLED', 'Pedido cancelado antes do envio.'));
    const child = this.connect(),
      id = randomUUID();
    return new Promise<any>((resolve, reject) => {
      const stop = (code: string, message: string) => {
        const p = this.pending.get(id);
        if (p && !p.cancelError) {
          p.cancelError = new DomainError(code, message);
          clearTimeout(p.timer);
          child.stdin.write(JSON.stringify({ method: 'cancel', params: { id } }) + '\n');
          p.timer = setTimeout(() => {
            this.pending.delete(id);
            p.cleanup();
            reject(
              new DomainError(
                'STOP_UNCONFIRMED',
                'O término remoto não foi confirmado. Confira a execução antes de reenviar.',
              ),
            );
          }, 15000);
        }
      };
      const abort = () => stop('CANCELLED', 'Pedido interrompido com término remoto confirmado.');
      const timer = setTimeout(() => {
        if (options.idleTimeout !== undefined)
          stop('RUNTIME_IDLE_TIMEOUT', 'A execução ficou sem resposta do modelo ou atividade de ferramentas; encerramento solicitado.');
        else
          stop('PROVIDER_TIMEOUT', 'O provedor excedeu o tempo de resposta; execução interrompida.');
      }, options.idleTimeout ?? options.timeout ?? 25000);
      // Liveness is separate from acceptance. Heartbeats, queue presence and
      // request-start notifications do not renew a worker's inactivity window.
      const onProgress = options.idleTimeout === undefined ? undefined : (kind: string) => {
        const p = this.pending.get(id);
        if (p && !p.cancelError && ['MODEL_RESPONSE', 'HARNESS_TOOL_EVENT'].includes(kind))
          p.timer.refresh();
      };
      const cleanup = () => options.signal?.removeEventListener('abort', abort);
      this.pending.set(id, {
        resolve,
        reject,
        timer,
        cleanup,
        child,
        method,
        onEvent: options.onEvent,
        onProgress,
      });
      options.signal?.addEventListener('abort', abort, { once: true });
      if (options.signal?.aborted) {
        abort();
        return;
      }
      child.stdin.write(JSON.stringify({ id, method, params }) + '\n');
    });
  }
  close() {
    this.child?.stdin.end();
    this.child = undefined;
  }
}
export const accountBridge = new AccountBridge();
