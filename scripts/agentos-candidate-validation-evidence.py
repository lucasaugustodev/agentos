#!/usr/bin/env python3
"""Read verified private CI receipts; export only typed provenance and scrubbed log.

This command never executes validation, publishes a release, or writes evidence.
The controller supplies the already admitted original request, never a path.
"""
import base64, contextlib, hashlib, importlib.util, json, os, pathlib, re, stat, sys
sys.dont_write_bytecode = True

STATE = pathlib.Path('/var/lib/agentos-bridge/candidate-validations')
MAX_RESULT = 16 * 1024 * 1024
spec = importlib.util.spec_from_file_location('ci_broker', pathlib.Path(__file__).with_name('agentos-candidate-validation.py'))
broker = importlib.util.module_from_spec(spec); spec.loader.exec_module(broker)
from validation_coverage import verified_coverage
sha = broker.sha
canonical = broker.canonical

def protected_read(path, limit):
    for parent in [*reversed(path.parents), path]:
        info = parent.lstat()
        if stat.S_ISLNK(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o022:
            raise ValueError('CI_EVIDENCE_STORE_UNPROTECTED')
        if parent != path and not stat.S_ISDIR(info.st_mode):
            raise ValueError('CI_EVIDENCE_STORE_UNPROTECTED')
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077 or info.st_size > limit:
        raise ValueError('CI_EVIDENCE_FILE_INVALID')
    with open(path, 'rb') as handle:
        opened = os.fstat(handle.fileno())
        if (opened.st_dev, opened.st_ino) != (info.st_dev, info.st_ino) or opened.st_uid != 0 or opened.st_mode & 0o077:
            raise ValueError('CI_EVIDENCE_FILE_CHANGED')
        data = handle.read(limit + 1)
        if len(data) != info.st_size or len(data) > limit:
            raise ValueError('CI_EVIDENCE_FILE_CHANGED')
        return data

def scrub_log(log):
    log = re.sub(r'\b(?:postgres(?:ql)?|https?)://[^\s\"\'<>]+', '[private-url]', log)
    log = re.sub(r'(?i)(?:password|secret|api[_-]?key|access[_-]?token|DATABASE_URL)\s*[:=]\s*\S+', '[redacted]', log)
    log = re.sub(r'''(?i)(["']?(?:password|secret|client_secret|api[_-]?key|access[_-]?token|DATABASE_URL|authorization|proxy-authorization|cookie|set-cookie|x-api-key|x-auth-token)["']?\s*[:=]\s*)(?:"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*'|[^\s,}\]]+)''', r'\1[redacted]', log)
    log = re.sub(r'\b(?:sk|rk|ghp|github_pat)-[\w.-]{16,}', '[redacted]', log)
    log = re.sub(r'(?i)\bBearer\s+[A-Za-z0-9._~+/-]{12,}', 'Bearer [redacted]', log)
    log = re.sub(r'(?i)\bBasic\s+[A-Za-z0-9+/=]{12,}', 'Basic [redacted]', log)
    log = re.sub(r'(?im)^\s*(?:Authorization|Proxy-Authorization|Cookie|Set-Cookie|X-API-Key|X-Auth-Token)\s*:.*$', '[redacted-header]', log)
    log = re.sub(r'-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----', '[redacted-private-key]', log, flags=re.S)
    return log

def verified_export(request):
    if os.geteuid() != 0:
        raise ValueError('CI_EVIDENCE_PRIVILEGE_REQUIRED')
    broker.identity(request)
    directory = STATE / request['actionId']
    for private in [STATE, directory]:
        info = private.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o077 or stat.S_ISLNK(info.st_mode):
            raise ValueError('CI_EVIDENCE_STORE_UNPROTECTED')
    # No caller-provided resultRef, source path, shell, or command is used.
    intent = protected_read(directory / 'request.json', MAX_RESULT)
    if intent != canonical(request):
        raise ValueError('CI_EVIDENCE_REQUEST_CHANGED')
    receipt_bytes = protected_read(directory / 'receipt.json', MAX_RESULT)
    receipt = json.loads(receipt_bytes)
    for name in ['publishedBaselineDigest','publicationProofDigest','validationProofDigest','registeredReceiptSha']:
        if not re.fullmatch('[a-f0-9]{64}', receipt.get(name, '')):
            raise ValueError('CI_EVIDENCE_BASELINE_UNPROVEN')
    if (receipt.get('state') != 'completed' or type(receipt.get('passed')) is not bool or
        receipt.get('actionId') != request['actionId'] or
        receipt.get('candidateDigest') != request['candidateDigest'] or
        receipt.get('validatedCandidateDigest') != request['candidateDigest'] or
        receipt.get('frozenWorkspace') != 'agentos/ocv' + request['actionId'].replace('-', '') or
        receipt.get('requestDigest') != sha(intent)):
        raise ValueError('CI_EVIDENCE_RECEIPT_UNBOUND')
    executor_sha = broker.verified_execution_registration(receipt)
    result_bytes = protected_read(directory / 'result.json', MAX_RESULT)
    if sha(result_bytes) != receipt.get('resultDigest'):
        raise ValueError('CI_EVIDENCE_RESULT_CHANGED')
    output = json.loads(result_bytes)
    matches = [a for a in output.get('artifacts', []) if a.get('name') == 'validation.log']
    if len(matches) != 1 or not isinstance(matches[0].get('content'), str):
        raise ValueError('CI_EVIDENCE_LOG_UNAVAILABLE')
    raw = matches[0]['content'].encode('utf-8')
    if len(raw) > 8 * 1024 * 1024:
        raise ValueError('CI_EVIDENCE_LOG_TOO_LARGE')
    verified = output.get('ok') is True and output.get('output', {}).get('passed') is not False and bool(re.fullmatch('[a-f0-9]{64}', output.get('output', {}).get('digest', '')))
    baseline_changed = bool(output.get('output', {}).get('base') and output['output']['base'] != receipt.get('publishedBaseline'))
    if receipt['passed'] != (verified and not baseline_changed):
        raise ValueError('CI_EVIDENCE_OUTCOME_CHANGED')
    if receipt.get('resultRef') != str(directory / 'result.json'):
        raise ValueError('CI_EVIDENCE_RESULT_REF_INVALID')
    profile = broker.registered_profile()
    clean = scrub_log(matches[0]['content']).encode('utf-8')
    authority = {
        'version': 1, 'kind': 'registered_candidate_validation', 'actionId': request['actionId'],
        'requestDigest': sha(intent), 'receiptDigest': sha(receipt_bytes), 'resultDigest': sha(result_bytes),
        'producer': {k: request[k] for k in ['goalId','taskId','runId','runFence','contextVersion','expectedAttempt','candidateDigest','contractDigest']},
        'outcome': {'passed': receipt['passed'], 'code': receipt.get('code'), 'state': receipt['state']},
        'registeredActionSha': executor_sha,
        'registeredCommand': [profile['command'], *profile['args']],
        'publishedBaselineDigest': receipt.get('publishedBaselineDigest'),
        'publicationProofDigest': receipt.get('publicationProofDigest'),
        'validationProofDigest': receipt.get('validationProofDigest'),
        'registeredReceiptSha': receipt.get('registeredReceiptSha'),
        'validatedReleaseDigest': receipt.get('validatedReleaseDigest'),
        'log': {'rawDigest': sha(raw), 'rawBytes': len(raw), 'digest': sha(clean), 'bytes': len(clean),
                'sanitized': True, 'completeRegisteredArtifact': True,
                'source': 'registered_action_validation.log', 'commandStdoutMayBeTruncatedByRegisteredAction': True},
        'acceptance': False,
    }
    coverage = verified_coverage(request, output, executor_sha)
    if coverage is not None:
        authority['coverage'] = coverage
    proof = canonical(authority)
    return {'version':1, 'actionId':request['actionId'], 'requestDigest':sha(intent),
            'resultDigest':sha(result_bytes), 'receiptDigest':sha(receipt_bytes), 'authority':authority,
            'inputs':[{'name':'ci-receipt.json','mime':'application/json','digest':sha(proof),'size':len(proof),'base64':base64.b64encode(proof).decode()},
                      {'name':'validation.log','mime':'text/plain','digest':sha(clean),'size':len(clean),'base64':base64.b64encode(clean).decode()}]}

def main():
    if sys.argv[1:] != ['evidence']:
        raise ValueError('CI_EVIDENCE_MODE_INVALID')
    request = json.load(sys.stdin)
    print(json.dumps(verified_export(request), ensure_ascii=False))

if __name__ == '__main__':
    try: main()
    except Exception as error:
        print(json.dumps({'state':'blocked','code':str(error) if isinstance(error,ValueError) and re.fullmatch('[A-Z_]+',str(error)) else 'CI_EVIDENCE_UNAVAILABLE','acceptance':False}))
