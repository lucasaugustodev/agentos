import { writeFile } from 'node:fs/promises';
import path from 'node:path';

// The agent runs as an ordinary user inside its own user namespace. Measured
// on 2026-10-27: as uid 0 with no /etc/passwd, PostgreSQL's initdb and postgres
// refuse to start at all, and an agent spent twelve attempts trying to work
// around it. Host root maps to this uid, so the workspace, home, sockets and
// policy files the runner owns stay readable and writable inside.
export const SANDBOX_UID = 1000;

const PASSWD =
  'root:x:0:0:root:/root:/usr/sbin/nologin\n' +
  `agent:x:${SANDBOX_UID}:${SANDBOX_UID}:agent:/home/agent:/bin/bash\n` +
  'nobody:x:65534:65534:nobody:/nonexistent:/usr/sbin/nologin\n';
const GROUP = `root:x:0:\nagent:x:${SANDBOX_UID}:\nnogroup:x:65534:\n`;

/**
 * Write passwd/group into the run directory (outside the agent's writable home)
 * and return the bwrap arguments that switch identity and mount them read-only.
 */
export async function prepareSandboxIdentity(runDir) {
  if (typeof runDir !== 'string' || !path.isAbsolute(runDir)) throw new TypeError('runDir must be an absolute path');
  const passwd = path.join(runDir, 'passwd'), group = path.join(runDir, 'group');
  await writeFile(passwd, PASSWD, { flag: 'wx', mode: 0o444 });
  await writeFile(group, GROUP, { flag: 'wx', mode: 0o444 });
  return Object.freeze([
    '--unshare-user', '--uid', String(SANDBOX_UID), '--gid', String(SANDBOX_UID),
    '--ro-bind', passwd, '/etc/passwd',
    '--ro-bind', group, '/etc/group',
  ]);
}
