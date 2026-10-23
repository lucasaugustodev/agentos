// Read-only observer for AgentOS-owned runs. Never reads assistant reasoning,
// process arguments/environment, production sessions or credential stores.
import { readFile, realpath, stat, lstat, readdir, open } from 'node:fs/promises';
import { constants } from 'node:fs';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { pathToFileURL } from 'node:url';
const exec = promisify(execFile);
const DATA = '/var/lib/agentos-bridge/worker-runs';
const ROOT = '/srv/agentos/workspaces';
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const sensitive = /(?:\.env\b|credentials?\.(?:json|ya?ml)|auth\.json|id_rsa|private[_-]?key)/i;
export function redact(text, limit = 2000) {
  let value = String(text ?? '')
    .replace(/-----BEGIN [^-]*PRIVATE KEY-----[\s\S]*?(?:-----END [^-]*PRIVATE KEY-----|$)/g, '[CHAVE OMITIDA]')
    .replace(/\b(?:sk-[A-Za-z0-9_.-]{8,}|eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+)\b/g, '[SEGREDO OMITIDO]')
    .replace(/(Bearer\s+)[A-Za-z0-9_.~+\/-]+/gi, '$1[OMITIDO]')
    .replace(/((?:api[_-]?key|access[_-]?token|refresh[_-]?token|password|secret|authorization)["']?\s*[:=]\s*["']?)[^\s,"'};]+/gi, '$1[OMITIDO]')
    .replace(/(https?:\/\/)[^\s/@:]+:[^\s/@]+@/gi, '$1[OMITIDO]@')
    .replace(/\x1b\[[0-9;]*m/g, '');
  if (value.length > limit) value = value.slice(0, Math.floor(limit / 3)) + '\n[… trecho omitido …]\n' + value.slice(-Math.floor(limit * 2 / 3));
  // Display projection only: PostgreSQL JSONB rejects NUL and lone UTF-16 surrogates.
  // Normalize after truncation too; never change original artifact bytes or digests.
  return value.replace(/\u0000/g, '\u2400').toWellFormed();
}
const iso = time => { const d = new Date(time); return Number.isNaN(+d) ? null : d.toISOString(); };
export function changedFiles(diff, untracked) {
  const files = [];
  for (const line of diff.trim().split('\n').filter(Boolean)) {
    const [added, removed, name] = line.split('\t');
    if (name && !sensitive.test(name)) files.push({path:redact(name,180),added,removed,status:'modified'});
  }
  for (const name of untracked.split('\0').filter(Boolean)) {
    if (sensitive.test(name) || files.some(file=>file.path===name)) continue;
    files.push({path:redact(name,180),added:null,removed:null,status:'new'});
  }
  return files.slice(0,30);
}
export function parseTrace(text) {
  const calls = new Map();
  let count = 0;
  for (const line of text.split('\n')) {
    let row;
    try { row = JSON.parse(line); } catch { continue; }
    const data = row.data;
    if (!data || !['tool/call', 'tool/result'].includes(row.type)) continue;
    if (row.type === 'tool/call' && typeof data.callId === 'string') {
      let args;
      try { args = JSON.parse(data.arguments); } catch { args = {}; }
      const raw = typeof args.command === 'string' ? args.command : JSON.stringify(args);
      const privateCommand = sensitive.test(raw);
      calls.set(data.callId, {
        id: data.callId, sequence: row.seq, tool: redact(data.name, 70),
        command: privateCommand ? '[Comando envolvendo credenciais omitido]' : redact(raw, 1800),
        privateCommand, startedAt: iso(row.time), finishedAt: null, output: '', exitCode: null, state: 'running',
      });
      count++;
    }
    if (row.type === 'tool/result') {
      // Only explicit tool-result text blocks. No generic recursion into messages.
      for (const result of data.message?.content ?? []) {
        if (result.type !== 'tool-result') continue;
        const call = calls.get(result.toolCallId);
        if (!call) continue;
        const output = (result.content ?? []).filter(b => b.type === 'text').map(b => b.text).join('\n');
        const exit = output.match(/(?:finished with|Process exited with) exit code\s+(-?\d+)/i) ?? output.match(/exit code\s+(-?\d+)/i);
        call.output = call.privateCommand ? '[Saída omitida por conter dados de credenciais]' : redact(output, 2600);
        call.exitCode = exit ? Number(exit[1]) : null;
        call.finishedAt = iso(row.time);
        call.state = result.isError || (call.exitCode !== null && call.exitCode !== 0) ? 'error' : 'returned';
      }
    }
  }
  const actions = [...calls.values()].map(({ privateCommand, ...call }) => call);
  return { actions: actions.slice(-12), toolCalls: count };
}
async function contained(file, root) {
  const resolved = await realpath(file);
  if (resolved !== file || !resolved.startsWith(root + '/')) throw Error('UNSAFE_OBSERVATION_PATH');
  return resolved;
}
async function tail(file, maxBytes) {
  const handle = await open(file, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
  try {
    // Validate the opened descriptor too: a worker may rename a writable path
    // after realpath but before open. Never read a descriptor outside that path.
    // /proc/self/fd exists only on Linux; elsewhere the descriptor identity is
    // proved by comparing it against lstat of the same path.
    if (process.platform === 'linux' && (await realpath(`/proc/self/fd/${handle.fd}`)) !== file)
      throw Error('UNSAFE_OBSERVATION_PATH');
    const info = await handle.stat();
    const actual = await lstat(file);
    if (!info.isFile() || actual.dev !== info.dev || actual.ino !== info.ino)
      throw Error('UNSAFE_OBSERVATION_PATH');
    const bytes = Buffer.alloc(Math.min(info.size, maxBytes));
    await handle.read(bytes, 0, bytes.length, Math.max(0, info.size - bytes.length));
    return { text: bytes.toString('utf8'), truncated: info.size > maxBytes, modifiedAt: info.mtime.toISOString() };
  } finally { await handle.close(); }
}
async function command(file, args, cwd) {
  return (await exec(file, args, { cwd, timeout: 5000, maxBuffer: 512000, env: { PATH: '/usr/bin:/bin', LANG: 'C.UTF-8', GIT_OPTIONAL_LOCKS: '0', GIT_CONFIG_NOSYSTEM: '1', GIT_CONFIG_GLOBAL: '/dev/null' } })).stdout;
}
export async function observeRun(runId) {
  if (!UUID.test(runId)) throw Error('INVALID_RUN');
  const dir = `${DATA}/${runId}`;
  const state = JSON.parse(await readFile(await contained(`${dir}/state.json`, DATA), 'utf8'));
  if (!/^[a-z0-9_-]+\/[a-z0-9_-]+$/.test(state.workspace)) throw Error('INVALID_WORKSPACE');
  const workspace = await contained(`${ROOT}/${state.workspace}`, ROOT);
  const traceFile = `${dir}/home/sessions/--workspace--/${runId}/session.v3.jsonl`;
  let trace = { actions: [], toolCalls: 0 }, traceTruncated = false, traceModifiedAt = null;
  try {
    const log = await tail(await contained(traceFile, dir), 8 * 1024 * 1024);
    trace = parseTrace(log.text); traceTruncated = log.truncated; traceModifiedAt = log.modifiedAt;
  } catch (e) { if (e.code !== 'ENOENT') throw e; }
  const unit = 'agentos-run-' + runId;
  let process = { available: false, activeState: 'unknown' };
  try {
    const properties = await command('systemctl', ['show', unit, '-p', 'ActiveState,SubState,MemoryCurrent,MemoryPeak,MemoryMax,CPUUsageNSec,TasksCurrent,ControlGroup']);
    const props = Object.fromEntries(properties.trim().split('\n').map(line => { const i = line.indexOf('='); return [line.slice(0, i), line.slice(i + 1)]; }));
    const numeric = key => /^\d+$/.test(props[key] ?? '') && Number(props[key]) < Number.MAX_SAFE_INTEGER ? Number(props[key]) : null;
    const names = [];
    if (/^\/system\.slice\/agentos-run-[a-f0-9-]+\.service$/.test(props.ControlGroup ?? '')) {
      try {
        const pids = (await readFile(`/sys/fs/cgroup${props.ControlGroup}/cgroup.procs`, 'utf8')).trim().split('\n');
        for (const pid of pids.slice(0, 80)) if (/^\d+$/.test(pid)) {
          try { names.push(redact((await readFile(`/proc/${pid}/comm`, 'utf8')).trim(), 80)); } catch {}
        }
      } catch {}
    }
    process = { available: true, activeState: props.ActiveState, subState: props.SubState, memoryBytes: numeric('MemoryCurrent'), memoryPeakBytes: numeric('MemoryPeak'), memoryLimitBytes: numeric('MemoryMax'), cpuNs: numeric('CPUUsageNSec'), processes: numeric('TasksCurrent'), processNames: [...new Set(names)] };
  } catch {}
  const files = [], evidence = [];
  try {
    await contained(`${workspace}/.git`, workspace);
    const args = ['--no-optional-locks', `--git-dir=${workspace}/.git`, `--work-tree=${workspace}`, '-c', 'core.fsmonitor=false', '-c', 'core.hooksPath=/dev/null'];
    const changes = await command('git', [...args, 'diff', '--no-ext-diff', '--no-textconv', '--numstat', 'HEAD'], workspace);
    const untracked = await command('git', [...args, 'ls-files', '--others', '--exclude-standard', '-z'], workspace);
    files.push(...changedFiles(changes, untracked));
  } catch {}
  try {
    const root = await contained(`${workspace}/evidence`, workspace);
    const entries = [];
    for (const file of (await readdir(root)).slice(0, 200)) {
      if (!/^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,100}\.(log|json|png|jpg|txt|tap|md)$/.test(file) || sensitive.test(file)) continue;
      const path = `${root}/${file}`;
      try {
        await contained(path, workspace);
        const info = await stat(path);
        if (info.isFile()) entries.push({ path, name: file, size: info.size, modifiedAt: info.mtime.toISOString() });
      } catch {}
    }
    entries.sort((a, b) => b.modifiedAt.localeCompare(a.modifiedAt));
    for (const entry of entries.slice(0, 8)) {
      const { path, ...meta } = entry;
      const preview = /\.(log|json|txt|tap|md)$/.test(entry.name) ? redact((await tail(path, 12000)).text, 2200) : null;
      evidence.push({ ...meta, preview });
    }
  } catch {}
  return { runId, workspace: state.workspace, runtimeState: state.state, observedAt: new Date().toISOString(), traceModifiedAt, traceTruncated, ...trace, process, files, evidence };
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try { console.log(JSON.stringify(await observeRun(process.argv[2]))); }
  catch (e) { console.log(JSON.stringify({ error: ['INVALID_RUN', 'INVALID_WORKSPACE', 'UNSAFE_OBSERVATION_PATH'].includes(e.message) ? e.message : 'OBSERVATION_UNAVAILABLE' })); process.exitCode = 1; }
}
