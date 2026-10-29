type Row = Record<string, any>;

/** "model · account" for a run, as the operator knows the account. */
export const runRoute = (run: Row, accounts: Row[]) => {
  const account = accounts.find((a) => a.id === run.account_id);
  return `${run.model ?? account?.model ?? 'modelo a definir'} · ${account?.label ?? run.account_id ?? 'conta automática'}`;
};

/** Short elapsed time since an ISO timestamp, e.g. "42min" or "3h". */
export const elapsed = (at?: string | null) => {
  if (!at) return '—';
  const n = Math.max(0, Math.floor((Date.now() - Date.parse(at)) / 1000));
  return n < 60 ? n + 's' : n < 3600 ? Math.floor(n / 60) + 'min' : Math.floor(n / 3600) + 'h' + String(Math.floor((n % 3600) / 60)).padStart(2, '0');
};
