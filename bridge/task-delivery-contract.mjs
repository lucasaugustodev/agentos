// Output instructions only. This contract neither grants tools/permissions nor
// collects files, rewrites source snapshots, or decides task acceptance.
const validName = /^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,100}$/;
const controllerNames = new Set(['candidate.json', 'changes.patch', 'runtime-protocol-check.json', 'runtime-plan-check.json']);
const failure = () => Object.assign(new Error('TASK_DELIVERY_CONTRACT'), { code: 'TASK_DELIVERY_CONTRACT' });
const mimeFor = (name) => name.endsWith('.json') ? 'application/json' :
  /\.(?:md|txt|log|diff|patch|mjs|js|ts|py|yaml|yml)$/.test(name) ? 'text/plain' : 'application/octet-stream';

export function taskDeliveryContract(criteria = []) {
  if (!Array.isArray(criteria) || criteria.length > 30) throw failure();
  const artifacts = [];
  for (const criterion of criteria) {
    if (!criterion || !['artifact', 'json_field', 'requirement_outcomes'].includes(criterion.type)) continue;
    const name = criterion.name;
    if (typeof name !== 'string' || !validName.test(name)) throw failure();
    if (controllerNames.has(name) || artifacts.some((item) => item.name === name)) continue;
    artifacts.push({ name, path: 'evidence/' + name, mime: mimeFor(name) });
  }
  return { version: 1, artifacts };
}

export function validateTaskDeliveryContract(value) {
  if (!value || value.version !== 1 || !Array.isArray(value.artifacts) || value.artifacts.length > 30)
    throw failure();
  const names = new Set();
  for (const item of value.artifacts) {
    if (!item || typeof item.name !== 'string' || !validName.test(item.name) ||
      controllerNames.has(item.name) || names.has(item.name) || item.path !== 'evidence/' + item.name ||
      item.mime !== mimeFor(item.name)) throw failure();
    names.add(item.name);
  }
  return value;
}

export function museTaskDeliveryInstruction(value) {
  // Existing callers without the new optional field retain their output shape.
  if (value === undefined) return '';
  const contract = validateTaskDeliveryContract(value);
  return '\n\nTASK_DELIVERY_CONTRACT_V1\n' +
    'The final JSON artifacts array MUST include EVERY entry below with exactly its name, relative path and mime:\n' +
    JSON.stringify(contract.artifacts) + '\n' +
    'Create or update these deliverable files under evidence/, never at the root of /workspace. ' +
    'A file that exists on disk but is omitted from the final artifacts array is NOT submitted. ' +
    'The report.json example illustrates the JSON shape; it does not replace the required entries above. ' +
    'You may also submit real supplemental evidence. Preserve actual failed or inconclusive test results and limitations; ' +
    'do not fabricate files, test execution or passed:true to satisfy a criterion. ' +
    'Do not list candidate.json, changes.patch or runtime-protocol-check.json: the controller produces those reserved artifacts. ' +
    'Preserve existing source files, including legacy root reports; the output directory instruction is not permission to delete or relocate them. ' +
    'Return only JSON {"summary":"actual work, tests and limitations","artifacts":[{"name":"exact required filename","path":"evidence/exact required filename","mime":"the required MIME type"}]}. ' +
    'Independent acceptance remains with the controller.';
}
