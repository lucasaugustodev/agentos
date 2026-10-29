import { fileURLToPath } from 'node:url';

/** A published controller launches its own immutable bridge, including helpers.
 * Existing actors keep their previous release; developer checkouts use the
 * installed compatibility bridge and never send workstation paths over SSH. */
export function bridgeScript(name: string, sourceUrl = import.meta.url) {
  if (!/^[a-z0-9][a-z0-9_.-]*\.(mjs|py)$/.test(name)) throw Error('INVALID_BRIDGE_SCRIPT');
  const source = fileURLToPath(sourceUrl).replaceAll('\\', '/');
  const release = source.match(/^(\/opt\/agentos\/releases\/managed-[0-9a-f-]{36})\/server\/bridge-path\.(ts|js)$/);
  return (release ? release[1] + '/bridge/' : '/opt/agentos-bridge/') + name;
}

/** The script inside this checkout, for a bridge started on this same host.
 * bridgeScript() names the provisioned path on the remote execution host and
 * is not a file here; passing it to a local spawn starts nothing. */
export function localBridgeScript(name: string, sourceUrl = import.meta.url) {
  if (!/^[a-z0-9][a-z0-9_.-]*\.(mjs|py)$/.test(name)) throw Error('INVALID_BRIDGE_SCRIPT');
  return fileURLToPath(new URL('../bridge/' + name, sourceUrl));
}
