"""Independent verification of registered Vitest evidence, never worker claims."""
import hashlib
import json
import pathlib
import uuid

MAX_REPORT = 8 * 1024 * 1024


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def fail(code):
    raise ValueError('CI_EVIDENCE_COVERAGE_' + code)


def relative_file(value, root):
    if not isinstance(value, str) or not value or '\\' in value or '\0' in value:
        fail('PATH_INVALID')
    parts = value.split('/')
    if value.startswith('/'):
        parts = parts[1:]
    if any(p in ('', '.', '..') for p in parts):
        fail('PATH_INVALID')
    path = pathlib.PurePosixPath(value)
    if path.is_absolute():
        if not path.is_relative_to(root):
            fail('PATH_INVALID')
        path = path.relative_to(root)
    if not path.parts or path.as_posix() != '/'.join(path.parts):
        fail('PATH_INVALID')
    return path.as_posix()


def verified_coverage(request, output, executor_sha):
    if request.get('authority') != 'runtime_artifact':
        return None  # No invented requirements for historical manager requests.
    required = request['runtimeAuthority']['policyRegistration']['policy']['requiredDatabaseTests']
    artifacts = output.get('artifacts', [])
    reports = [a for a in artifacts if a.get('name') == 'vitest-results.json']
    coverages = [a for a in artifacts if a.get('name') == 'validation-coverage.json']
    if not coverages:
        return None  # Old registered executors provide no discriminant coverage.
    if len(reports) != 1 or len(coverages) != 1:
        fail('ARTIFACT_IDENTITY')
    raw = []
    for artifact in [reports[0], coverages[0]]:
        if not isinstance(artifact.get('content'), str):
            fail('ARTIFACT_FORMAT')
        value = artifact['content'].encode('utf-8')
        if not value or len(value) > MAX_REPORT:
            fail('ARTIFACT_SIZE')
        raw.append(value)
    report_bytes, coverage_bytes = raw
    report, coverage = json.loads(report_bytes), json.loads(coverage_bytes)
    recorded = output.get('output', {})
    if (recorded.get('testReportDigest') != sha(report_bytes) or
            recorded.get('coverageDigest') != sha(coverage_bytes) or
            recorded.get('executorSha256') != executor_sha):
        fail('RECEIPT_UNBOUND')
    action = str(uuid.UUID(request['actionId']))
    # Structured coverage is issued only by the hardened registered executor.
    # Legacy releases did not attest required DB cases; no old path is upgraded.
    root = pathlib.PurePosixPath('/var/lib/agentos-validation-builds') / action
    if (coverage.get('version') != 1 or coverage.get('profile') != 'agentos.validate' or
            coverage.get('source') != 'registered_vitest_json' or
            coverage.get('candidateDigest') != request['candidateDigest'] or
            coverage.get('obligationsDigest') != sha(canonical(required)) or
            coverage.get('reportDigest') != sha(report_bytes) or
            coverage.get('reportRoot') != str(root)):
        fail('IDENTITY_CHANGED')
    if not isinstance(report, dict) or not isinstance(report.get('testResults'), list):
        fail('REPORT_FORMAT')
    found = {}
    for suite in report['testResults']:
        if not isinstance(suite, dict) or not isinstance(suite.get('assertionResults'), list):
            fail('REPORT_FORMAT')
        file = relative_file(suite.get('name'), root)
        for assertion in suite['assertionResults']:
            if not isinstance(assertion, dict) or not isinstance(assertion.get('fullName'), str):
                fail('REPORT_FORMAT')
            key = (file, assertion['fullName'])
            if not key[1] or key in found:
                fail('DUPLICATE_CASE')
            status = assertion.get('status')
            if status in ('pending', 'todo', 'skipped', 'disabled'):
                status = 'skipped'
            if status not in ('passed', 'failed', 'skipped'):
                fail('STATUS_INVALID')
            found[key] = status
    derived = [item | {'status':found.get((item['file'], item['name']), 'missing')} for item in required]
    if coverage.get('requiredTests') != derived:
        fail('STATUS_MISMATCH')
    return {'version':1,'source':'registered_vitest_json','profile':'agentos.validate',
            'candidateDigest':request['candidateDigest'],'obligationsDigest':sha(canonical(required)),
            'reportDigest':sha(report_bytes),'requiredTests':derived}
