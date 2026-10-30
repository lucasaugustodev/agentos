import {expect,it} from 'vitest';
import {createHash} from 'node:crypto';
import {validatePublishedBaseline} from '../server/published-rebase.js';
const sha=(value:string)=>createHash('sha256').update(value).digest('hex');
function baseline(){
 const files={'server/file.ts':sha('source')};
 return {version:1,release:'/opt/agentos/releases/managed-11111111-1111-4111-8111-111111111111',
  releaseDigest:'a'.repeat(64),sourceWorkspace:'agentos/src'+'a'.repeat(16),sourceManifestHashes:files,
  sourceManifestDigest:sha(JSON.stringify(files)),generatedReleaseHashes:{},workspaceManifestDigest:sha(JSON.stringify(files)),
  publicationProofRef:'/var/lib/agentos-operator-releases/release-fixture/publish.receipt.json',publicationProofDigest:'b'.repeat(64),
  validationProofRef:'/var/lib/agentos-operator-releases/release-fixture/validate.receipt.json',validationProofDigest:'c'.repeat(64),
  registeredReceiptRef:'/var/lib/agentos/releases/receipt.json',registeredReceiptDigest:'d'.repeat(64),registeredActionDigest:'e'.repeat(64)};
}
it('accepts both verified legacy sidecars and the exact operator receipt pair',()=>{
 const b=baseline();expect(validatePublishedBaseline({baseline:b})).toBe(b);
 b.publicationProofRef='/var/lib/agentos-build/fixture/framework-general-publication-proof.json';
 b.validationProofRef='/var/lib/agentos-build/fixture/framework-general-validation-proof.json';
 expect(validatePublishedBaseline({baseline:b})).toBe(b);
});
it.each([
 ['operator-traversal','/var/lib/agentos-operator-releases/../untrusted/publish.receipt.json'],
 ['operator-encoded-traversal','/var/lib/agentos-operator-releases/%2e%2e/publish.receipt.json'],
 ['operator-alias','/var/lib/agentos-operator-releases//release-fixture/publish.receipt.json'],
 ['operator-backslash','/var/lib/agentos-operator-releases/release-fixture\\publish.receipt.json'],
 ['untrusted-root','/tmp/agentos-operator-releases/release-fixture/publish.receipt.json'],
 ['wrong-receipt','/var/lib/agentos-operator-releases/release-fixture/arbitrary.json'],
 ['legacy-traversal','/var/lib/agentos-build/fixture/../arbitrary.json'],
])('rejects noncanonical publication reference %s',(_name,ref)=>{
 const b=baseline();b.publicationProofRef=ref;
 expect(()=>validatePublishedBaseline({baseline:b})).toThrow('REBASE_PUBLISHED_BASE_UNAVAILABLE');
});
it.each([
 '/var/lib/agentos-operator-releases/other-release/validate.receipt.json',
 '/var/lib/agentos-operator-releases/release-fixture/../release-fixture/validate.receipt.json',
 '/var/lib/agentos-build/fixture/validation.json',
])('rejects a validation receipt outside the same operator pair: %s',ref=>{
 const b=baseline();b.validationProofRef=ref;
 expect(()=>validatePublishedBaseline({baseline:b})).toThrow('REBASE_PUBLISHED_BASE_UNAVAILABLE');
});
it.each(['registeredReceiptDigest','registeredActionDigest','publicationProofDigest','validationProofDigest','sourceManifestDigest','workspaceManifestDigest'] as const)
 ('retains digest and registered authority checks for operator receipts: %s',key=>{
  const b=baseline();b[key]='unverified';expect(()=>validatePublishedBaseline({baseline:b})).toThrow('REBASE_PUBLISHED_BASE_UNAVAILABLE');
 });
