import React, { useEffect, useRef, useState } from 'react';
import {
  ArrowUp,
  Check,
  ChevronRight,
  ExternalLink,
  MessageSquare,
  Plus,
  RefreshCw,
  Square,
  Wallet,
  X,
  Loader2,
  Link2,
  Sparkles,
} from 'lucide-react';
type Row = Record<string, any>;
async function api(url: string, method = 'GET', body?: unknown) {
  const r = await fetch('/api' + url, {
    method,
    headers: body ? { 'Content-Type': 'application/json' } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  });
  const result = await r.json();
  if (!r.ok) throw Error(result.message ?? 'Não foi possível concluir.');
  return result;
}
const familyLabels: Row = {
  codex: 'Codex',
  claude: 'Claude',
  kimi: 'Kimi',
  agy: 'Antigravity',
  muse: 'Muse Code',
  qwen: 'Qwen Cloud',
  openai: 'API compatível',
};
import { ProviderHealthBadge } from './ProviderHealth.js';
function useConnections() {
  const [data, setData] = useState<Row>({ accounts: [] }),
    [error, setError] = useState('');
  async function refresh(force = false) {
    try {
      if (force) await api('/connections/refresh', 'POST', {});
      setData(await api('/connections'));
      setError('');
    } catch (e: any) {
      setError(e.message);
    }
  }
  useEffect(() => {
    void refresh();
    const t = setInterval(() => void refresh(), 30000);
    const changed = () => void refresh();
    window.addEventListener('agentos-connections-changed', changed);
    return () => {
      clearInterval(t);
      window.removeEventListener('agentos-connections-changed', changed);
    };
  }, []);
  return { data, error, refresh };
}
export function SimpleChat({ onConnect, onGoals }: { onConnect: () => void; onGoals: () => void }) {
  const [chats, setChats] = useState<Row[]>([]),
    [selected, setSelected] = useState<string | null>(null),
    [detail, setDetail] = useState<Row | null>(null),
    [draft, setDraft] = useState(() => sessionStorage.getItem('agentos-manager-draft') ?? ''),
    [accountId, setAccountId] = useState('auto'),
    [model, setModel] = useState(''),
    [error, setError] = useState(''),
    [sending, setSending] = useState(false);
  const [intent, setIntent] = useState('chat'),
    [workspaces, setWorkspaces] = useState<Row[]>([]),
    [workspace, setWorkspace] = useState(''),
    [capacity, setCapacity] = useState(2);
  const { data: connections } = useConnections(),
    bottom = useRef<HTMLDivElement>(null);
  const accounts = (connections.accounts ?? []).filter((a: Row) => a.provider !== 'muse'),
    account = accounts.find((a: Row) => a.id === accountId);
  const active = detail?.messages?.some((m: Row) =>
    ['queued', 'running', 'cancelling'].includes(m.state),
  );
  async function load() {
    try {
      setChats(await api('/chats'));
    } catch (e: any) {
      setError(e.message);
    }
  }
  useEffect(() => {
    sessionStorage.removeItem('agentos-manager-draft');
    void load();
    void api('/workspaces')
      .then((rows) => setWorkspaces(rows.filter((w: Row) => !/^o[a-f0-9]{12}r\d+-/.test(w.name))))
      .catch(() => {});
  }, []);
  useEffect(() => {
    if (!selected) {
      setDetail(null);
      return;
    }
    let disposed = false;
    const refresh = () =>
      api('/chats/' + selected)
        .then((d) => {
          if (!disposed) setDetail(d);
        })
        .catch((e) => setError(e.message));
    void refresh();
    const t = setInterval(refresh, 1500);
    return () => {
      disposed = true;
      clearInterval(t);
    };
  }, [selected]);
  useEffect(() => {
    bottom.current?.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
  }, [detail?.messages?.length, active]);
  async function send() {
    if (!draft.trim() || sending || active) return;
    setSending(true);
    setError('');
    try {
      let id = selected;
      if (!id) {
        const chat = await api('/chats', 'POST', {});
        id = chat.id;
        setSelected(id);
      }
      if (intent !== 'chat') {
        if (!workspace) throw Error('Selecione o workspace da tarefa.');
        await api('/objectives', 'POST', {
          message: draft,
          project: workspace.split('/')[0],
          mode: intent,
          workspaces: [workspace],
          requestedCapacity: capacity,
          plannerAccountId: accountId,
          ...(model ? { plannerModel: model } : {}),
          idempotencyKey: crypto.randomUUID(),
          chatId: id,
        });
      } else
        await api('/chats/' + id + '/messages', 'POST', {
          message: draft,
          requestKey: crypto.randomUUID(),
          accountId,
          ...(model ? { model } : {}),
        });
      setDraft('');
      setDetail(await api('/chats/' + id));
      await load();
    } catch (e: any) {
      setError(e.message);
    } finally {
      setSending(false);
    }
  }
  return (
    <section className="chat-shell">
      <aside className="chat-history">
        <button
          className="button full"
          onClick={() => {
            setSelected(null);
            setDetail(null);
            setDraft('');
            setError('');
          }}
        >
          <Plus size={16} />
          Nova conversa
        </button>
        <small>CONVERSAS</small>
        <div className="chat-history-list">
          {chats.map((c) => (
            <button
              key={c.id}
              className={selected === c.id ? 'selected' : ''}
              onClick={() => {
                setSelected(c.id);
                setError('');
              }}
            >
              <MessageSquare size={14} />
              <span>{c.title}</span>
            </button>
          ))}
          {!chats.length && <p>Suas conversas ficam salvas aqui.</p>}
        </div>
        <button className="text-button" onClick={onGoals}>
          Ver objetivos e tarefas
          <ChevronRight size={13} />
        </button>
      </aside>
      <div className="chat-main">
        <header className="chat-heading">
          <div>
            <span className="chat-logo">
              <Sparkles size={19} />
            </span>
            <div>
              <strong>{detail?.chat?.title ?? 'Gestor do AgentOS'}</strong>
              <small>Converse, ajuste operações, crie ciclos e evolua o aplicativo</small>
            </div>
          </div>
          <button className="button" onClick={onConnect}>
            <Plus size={14} />
            Conectar conta
          </button>
        </header>
        <div className="chat-messages">
          {!detail?.messages?.length && (
            <div className="chat-welcome">
              <div className="welcome-symbol">
                <MessageSquare size={31} />
              </div>
              <h1>O que vamos fazer?</h1>
              <p>
                Descreva a tarefa do seu jeito. Peça um plano, delegue análises ou acompanhe o que
                já está em andamento.
              </p>
              <div className="chat-suggestions">
                {[
                  'Me ajude a criar um ciclo',
                  'Como estão os objetivos em andamento?',
                  'Quais contas e agentes posso usar?',
                ].map((s) => (
                  <button key={s} onClick={() => setDraft(s)}>
                    {s}
                    <ChevronRight size={14} />
                  </button>
                ))}
              </div>
            </div>
          )}
          {detail?.messages?.map((m: Row) => (
            <article key={m.id} className={'chat-message ' + m.role}>
              <span className="message-avatar">
                {m.role === 'user' ? 'LC' : <Sparkles size={15} />}
              </span>
              <div className="message-body">
                <div className="message-author">
                  {m.role === 'user' ? 'Você' : 'AgentOS'}
                  {m.model && (
                    <small>
                      {m.model} ·{' '}
                      {accounts.find((a: Row) => a.id === m.account_id)?.label ?? m.account_id}
                    </small>
                  )}
                </div>
                {m.content && <div className="message-text">{m.content}</div>}
                {m.content?.match(/#goal=([a-f0-9-]{36})/) && (
                  <a
                    className="button small"
                    href={'#goal=' + m.content.match(/#goal=([a-f0-9-]{36})/)[1]}
                  >
                    Acompanhar plano e execução
                  </a>
                )}
                {m.content?.match(/#cycle=([a-f0-9-]{36})/) && (
                  <a
                    className="button small"
                    href={m.content.slice(m.content.indexOf('#cycle=')).split(/[\s)]/)[0]}
                  >
                    Acompanhar ciclo
                  </a>
                )}
                {['queued', 'running', 'cancelling'].includes(m.state) && (
                  <div className="message-working">
                    <Loader2 size={14} />
                    {m.state === 'cancelling'
                      ? 'Interrompendo…'
                      : 'Trabalhando na sua solicitação…'}
                  </div>
                )}
                {m.error && <div className="message-error">{m.error}</div>}
              </div>
            </article>
          ))}
          <div ref={bottom} />
        </div>
        <footer className="chat-compose">
          <div className="objective-compose">
            <select
              aria-label="Tipo de pedido"
              value={intent}
              onChange={(e) => setIntent(e.target.value)}
            >
              <option value="chat">Conversar</option>
              <option value="execute">Planejar e executar tarefa</option>
              <option value="plan">Somente planejar</option>
            </select>
            {intent !== 'chat' && (
              <>
                <select
                  aria-label="Workspace da tarefa"
                  value={workspace}
                  onChange={(e) => setWorkspace(e.target.value)}
                >
                  <option value="">Selecione o workspace</option>
                  {workspaces.map((w) => (
                    <option key={w.id} value={w.id}>
                      {w.id}
                    </option>
                  ))}
                </select>
                <label>
                  Até{' '}
                  <input
                    aria-label="Frentes simultâneas"
                    type="number"
                    min={1}
                    max={32}
                    value={capacity}
                    onChange={(e) => setCapacity(Number(e.target.value))}
                  />{' '}
                  frentes
                </label>
                <small>
                  O plano e o andamento ficam salvos, mesmo depois de fechar esta conversa.
                </small>
              </>
            )}
          </div>
          {error && (
            <div className="message-error" role="alert">
              {error}
            </div>
          )}
          <div className="compose-box">
            <textarea
              aria-label="Mensagem"
              value={draft}
              onChange={(e) => setDraft(e.target.value)}
              placeholder="Descreva sua tarefa ou pergunte sobre o progresso…"
              rows={3}
              onKeyDown={(e) => {
                if (e.key === 'Enter' && !e.shiftKey) {
                  e.preventDefault();
                  void send();
                }
              }}
            />
            <div className="compose-toolbar">
              <div className="chat-selects">
                <Wallet size={14} />
                <select
                  aria-label="Conta do chat"
                  value={accountId}
                  onChange={(e) => {
                    setAccountId(e.target.value);
                    setModel('');
                  }}
                >
                  <option value="auto">Conta automática</option>
                  {accounts.map((a: Row) => (
                    <option key={a.id} value={a.id}>
                      {a.label}
                      {a.quota_status === 'exhausted'
                        ? ' · sem cota'
                        : a.connection_status === 'reconnect_required'
                          ? ' · reconectar'
                          : ''}
                    </option>
                  ))}
                </select>
                {account && (
                  <select
                    aria-label="Modelo do chat"
                    value={model || account.model}
                    onChange={(e) => setModel(e.target.value)}
                  >
                    {account.models.map((m: string) => (
                      <option key={m} value={m}>
                        {m}
                      </option>
                    ))}
                  </select>
                )}
              </div>
              {active ? (
                <button
                  className="send-button stop"
                  aria-label="Interromper resposta"
                  onClick={() =>
                    void api('/chats/' + selected + '/cancel', 'POST', {}).catch((e) =>
                      setError(e.message),
                    )
                  }
                >
                  <Square size={15} />
                </button>
              ) : (
                <button
                  className="send-button"
                  aria-label="Enviar mensagem"
                  disabled={!draft.trim() || sending || !accounts.length}
                  onClick={() => void send()}
                >
                  {sending ? <Loader2 size={17} /> : <ArrowUp size={20} />}
                </button>
              )}
            </div>
          </div>
          <p>
            {accounts.length
              ? `${accounts.length} contas disponíveis no catálogo · tarefas e memórias ficam neste AgentOS`
              : 'Conectando ao catálogo de contas do servidor…'}
            <span>Enter envia · Shift+Enter nova linha</span>
          </p>
        </footer>
      </div>
    </section>
  );
}
export function ConnectionsPanel({ onConnect }: { onConnect: () => void }) {
  const { data, error, refresh } = useConnections();
  const [busy, setBusy] = useState(false);
  // "last answer 12s ago" has to keep counting, or a dead provider reads as fresh.
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const t = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(t);
  }, []);
  return (
    <>
      <div className="connections-intro">
        <div>
          <Link2 size={22} />
          <div>
            <h2>Suas contas, no mesmo lugar</h2>
            <p>
              As conexões herdadas usam as credenciais que já estão no servidor. Novas contas são
              adicionadas ao AgentOS.
            </p>
          </div>
        </div>
        <button
          className="button"
          disabled={busy}
          onClick={async () => {
            setBusy(true);
            try {
              await refresh(true);
            } finally {
              setBusy(false);
            }
          }}
        >
          <RefreshCw size={14} />
          Atualizar
        </button>
      </div>
      {(error || data.error) && (
        <div className="message-error" role="alert">
          {error || data.error}
        </div>
      )}
      <div className="connected-grid">
        {data.accounts.map((a: Row) => (
          <article className="connected-card" key={a.id}>
            <div className="connected-top">
              <span className={'provider-mark ' + a.provider}>
                {familyLabels[a.provider]?.slice(0, 1) ?? 'A'}
              </span>
              <span className="source-tag">
                {a.source === 'harness' ? 'Herdada do Harness' : 'Adicionada no AgentOS'}
              </span>
            </div>
            <h3>{a.label}</h3>
            <p className="account-identity">{a.identity?.email ?? familyLabels[a.provider]}</p>
            <ProviderHealthBadge
              health={data.health?.[a.id]}
              windowMinutes={data.healthWindowMinutes ?? 30}
              now={now}
            />
            <span
              className={
                'connection-state ' +
                (a.connection_status === 'reconnect_required' ? 'warning' : '')
              }
            >
              <i />
              {a.connection_status === 'reconnect_required'
                ? 'Precisa reconectar'
                : a.enabled
                  ? 'Conectada'
                  : 'Desconectada'}
            </span>
            <div className="account-models">
              {a.models.length} modelos <span>{a.model}</span>
            </div>
            {a.provider === 'muse' && (
              <p>
                Disponível para agentes nas tarefas. Use outra conta para o chat e o planejamento.
              </p>
            )}
            <div className="account-quota-groups">
              {a.quota?.groups?.length ? (
                a.quota.groups.map((g: Row) => (
                  <div key={g.label}>
                    <strong>{g.label}</strong>
                    {g.windows?.map((w: Row, i: number) => (
                      <div className="quota-window" key={i}>
                        <span>{w.label}</span>
                        <b>{Number(w.remainingPercent).toFixed(0)}%</b>
                        <div className="progress-track">
                          <i
                            style={{ width: Math.max(0, Math.min(100, w.remainingPercent)) + '%' }}
                          />
                        </div>
                      </div>
                    ))}
                  </div>
                ))
              ) : (
                <p>Cota não informada pelo provedor.</p>
              )}
            </div>
            {a.connection_status === 'reconnect_required' && (
              <button className="button full" onClick={onConnect}>
                Conectar novamente em outra rota
              </button>
            )}
          </article>
        ))}
        <button className="add-connection-card" onClick={onConnect}>
          <Plus size={25} />
          <strong>Adicionar outra conta</strong>
          <span>Codex, Claude, Kimi, AGY, Muse Code ou API</span>
        </button>
      </div>
    </>
  );
}
function safeUrl(value: string) {
  try {
    const u = new URL(value);
    return u.protocol === 'https:' &&
      [
        'auth.openai.com',
        'chatgpt.com',
        'claude.ai',
        'platform.claude.com',
        'auth.kimi.com',
        'kimi.com',
        'www.kimi.com',
        'accounts.google.com',
        'account.qwencloud.com',
        'auth.meta.com',
      ].includes(u.hostname)
      ? u.href
      : null;
  } catch {
    return null;
  }
}
export function ConnectAccountDialog({ onClose }: { onClose: () => void }) {
  const [family, setFamily] = useState('codex'),
    [label, setLabel] = useState(''),
    [baseUrl, setBaseUrl] = useState('https://token-plan.maas.qwencloudapi.com/compatible-mode/v1'),
    [model, setModel] = useState('deepseek-v4-flash-0731'),
    [key, setKey] = useState(''),
    [attempt, setAttempt] = useState<Row | null>(null),
    [answer, setAnswer] = useState(''),
    [busy, setBusy] = useState(false),
    [error, setError] = useState('');
  const apiFamily = ['qwen', 'openai'].includes(family);
  useEffect(() => {
    let closed = false;
    void api('/connections/login/active')
      .then((pending) => {
        if (!closed && pending?.id) {
          setAttempt(pending);
          setFamily(pending.family);
          setLabel(pending.label);
        }
      })
      .catch((e) => {
        if (!closed) setError(e.message);
      });
    return () => {
      closed = true;
    };
  }, []);
  useEffect(() => {
    if (attempt?.status === 'completed')
      window.dispatchEvent(new Event('agentos-connections-changed'));
  }, [attempt?.status]);
  async function close() {
    try {
      if (attempt?.id && attempt.status === 'pending')
        await api('/connections/login/' + attempt.id + '/cancel', 'POST', {});
      onClose();
    } catch (e: any) {
      setError(e.message);
    }
  }
  useEffect(() => {
    if (!attempt?.id || attempt.status !== 'pending') return;
    const t = setInterval(
      () =>
        api('/connections/login/' + attempt.id)
          .then(setAttempt)
          .catch((e) => setError(e.message)),
      1500,
    );
    return () => clearInterval(t);
  }, [attempt?.id, attempt?.status]);
  async function start(e: React.FormEvent) {
    e.preventDefault();
    setError('');
    setBusy(true);
    try {
      if (apiFamily) {
        await api('/connections/api-key', 'POST', {
          family,
          label: label || familyLabels[family] + ' adicional',
          baseUrl,
          model,
          key,
        });
        setKey('');
        setAttempt({ status: 'completed' });
      } else
        setAttempt(
          await api('/connections/login', 'POST', {
            family,
            label: label || familyLabels[family] + ' adicional',
          }),
        );
    } catch (e: any) {
      setError(e.message);
    } finally {
      setBusy(false);
    }
  }
  return (
    <div className="modal-backdrop">
      <section
        className="modal connect-modal"
        role="dialog"
        aria-modal="true"
        aria-label="Conectar conta"
      >
        <div className="modal-heading">
          <h2>Conectar outra conta</h2>
          <button
            className="icon-btn"
            disabled={busy}
            onClick={() => void close()}
            aria-label="Fechar conexão"
          >
            <X size={20} />
          </button>
        </div>
        {error && (
          <div className="message-error" role="alert">
            {error}
          </div>
        )}
        {!attempt ? (
          <form className="form" onSubmit={start}>
            <p className="connect-note">
              Adicione quantas contas precisar. As contas atuais permanecem conectadas.
            </p>
            <label>
              Serviço
              <select
                aria-label="Serviço"
                value={family}
                onChange={(e) => {
                  setFamily(e.target.value);
                  if (e.target.value === 'openai') {
                    setBaseUrl('');
                    setModel('');
                  }
                  if (e.target.value === 'qwen') {
                    setBaseUrl('https://token-plan.maas.qwencloudapi.com/compatible-mode/v1');
                    setModel('deepseek-v4-flash-0731');
                  }
                }}
              >
                {Object.entries(familyLabels).map(([id, name]) => (
                  <option key={id} value={id}>
                    {String(name)}
                  </option>
                ))}
              </select>
            </label>
            {family === 'muse' && (
              <p className="connect-note">
                Conecte sua conta Muse Code para executar agentes nas tarefas. O chat e o
                planejamento usam as outras contas conectadas.
              </p>
            )}
            <label>
              Nome da conta
              <input
                value={label}
                onChange={(e) => setLabel(e.target.value)}
                placeholder={familyLabels[family] + ' adicional'}
              />
            </label>
            {apiFamily && (
              <>
                <label>
                  Endpoint da API
                  <input
                    type="url"
                    required
                    value={baseUrl}
                    onChange={(e) => setBaseUrl(e.target.value)}
                    placeholder="https://api.exemplo.com/v1"
                  />
                </label>
                <label>
                  Modelo
                  <input required value={model} onChange={(e) => setModel(e.target.value)} />
                </label>
                <label>
                  Chave da API
                  <input
                    type="password"
                    required
                    autoComplete="new-password"
                    value={key}
                    onChange={(e) => setKey(e.target.value)}
                  />
                </label>
                <small>A chave será guardada no servidor de contas, fora da conversa.</small>
              </>
            )}
            <button className="button primary" disabled={busy}>
              {busy ? <Loader2 size={15} /> : <Plus size={15} />}{' '}
              {apiFamily ? 'Salvar conexão' : 'Iniciar login'}
            </button>
          </form>
        ) : attempt.status === 'completed' ? (
          <div className="connection-complete">
            <span>
              <Check size={30} />
            </span>
            <h3>Conta conectada</h3>
            <p>
              {family === 'muse'
                ? 'Muse Code já pode executar agentes nas tarefas. Use outra conta para conversar no chat.'
                : 'Ela já pode ser selecionada no chat e usada nas tarefas.'}
            </p>
            <button className="button primary" onClick={onClose}>
              Concluir
            </button>
            <button
              className="text-button"
              onClick={() => {
                setAttempt(null);
                setLabel('');
              }}
            >
              Adicionar mais uma conta
            </button>
          </div>
        ) : (
          <div className="login-progress">
            <p>
              {attempt.status === 'pending'
                ? 'Conclua a autorização da sua conta.'
                : 'O login não foi concluído.'}
            </p>
            {attempt.notices?.map((n: Row, i: number) => {
              const url = safeUrl(n.url ?? n.verificationUri ?? '');
              return (
                <div className="login-notice" key={i}>
                  {(n.instructions || n.message) && <p>{n.instructions ?? n.message}</p>}
                  {n.userCode && <code className="login-code">{n.userCode}</code>}
                  {url && (
                    <a className="button primary" href={url} target="_blank" rel="noreferrer">
                      Abrir página de autorização
                      <ExternalLink size={13} />
                    </a>
                  )}
                  {n.links?.map(
                    (l: Row) =>
                      safeUrl(l.url) && (
                        <a key={l.url} href={safeUrl(l.url)!} target="_blank" rel="noreferrer">
                          {l.label ?? 'Autorizar conta'}
                        </a>
                      ),
                  )}
                </div>
              );
            })}
            {attempt.prompt && (
              <form
                className="form"
                onSubmit={async (e) => {
                  e.preventDefault();
                  setBusy(true);
                  setError('');
                  try {
                    setAttempt(
                      await api('/connections/login/' + attempt.id + '/answer', 'POST', {
                        value:
                          answer ||
                          (attempt.prompt.type === 'select' ? attempt.prompt.options?.[0]?.id : ''),
                      }),
                    );
                    setAnswer('');
                  } catch (e: any) {
                    setError(e.message);
                  } finally {
                    setBusy(false);
                  }
                }}
              >
                <label>
                  {attempt.prompt.message}
                  {attempt.prompt.type === 'select' ? (
                    <select
                      value={answer || attempt.prompt.options?.[0]?.id}
                      onChange={(e) => setAnswer(e.target.value)}
                    >
                      {attempt.prompt.options?.map((o: Row) => (
                        <option key={o.id} value={o.id}>
                          {o.label}
                        </option>
                      ))}
                    </select>
                  ) : (
                    <input
                      type={attempt.prompt.type === 'secret' ? 'password' : 'text'}
                      required
                      value={answer}
                      onChange={(e) => setAnswer(e.target.value)}
                      autoComplete="off"
                    />
                  )}
                </label>
                <button className="button primary" disabled={busy}>
                  Enviar autorização
                </button>
              </form>
            )}
            {attempt.error && (
              <div className="message-error">{attempt.error.message ?? attempt.error}</div>
            )}
            {attempt.status === 'pending' ? (
              <button
                className="text-button"
                onClick={async () => {
                  await api('/connections/login/' + attempt.id + '/cancel', 'POST', {});
                  setAttempt(null);
                }}
              >
                Cancelar este login
              </button>
            ) : (
              <button className="button" onClick={() => setAttempt(null)}>
                Tentar novamente
              </button>
            )}
          </div>
        )}
      </section>
    </div>
  );
}
