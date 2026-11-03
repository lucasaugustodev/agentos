#!/usr/bin/env python3
"""Root-scoped native producer/QA -> diagnostic CHECK, never SCORE.

No policy is enabled by this module. A deployment must pin this file, qualified
runtime/transport helpers and an explicitly operator-approved policy outside all
worker workspaces. The old per-candidate manual-review lane is left unchanged.
Untrusted candidate/checker code is inspected and frozen, never run here.
"""
import argparse
import ast
import base64
import copy
import datetime as dt
import errno
import fcntl
import hashlib
import importlib.util
import json
import math
import os
import pathlib
import re
import selectors
import shlex
import stat
import subprocess
import time

SHA = re.compile(r"[a-f0-9]{64}")
UUID = re.compile(r"[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}")
JOB = re.compile(r"[a-f0-9]{16,64}")
BOARDS = {"muse_decode_layer", "muse_prefill_layer", "muse_attention_decode"}
PRODUCER_FILES = {"candidate.py", "implementation.patch", "cpu-checks.py", "cpu-result.json", "verification.json"}
QA_FILES = {"qa-verification.json", "review.txt"}
WATCHER_STATE_MAX_BYTES = 64 * 1024 * 1024
REQUEST_CANONICALIZATION = "json-numbers-v1"
MAX_SAFE_INTEGER = 2 ** 53 - 1
TERMINAL_DIAGNOSTIC_TTL = 3600
TERMINAL_DIAGNOSTIC_LIMIT = 128
TERMINAL_DIAGNOSTIC_MAX_BYTES = 1024 * 1024
JOBS_OBSERVATION_MAX_BYTES = 1024 * 1024
REQUIRED_ARTIFACT_MAX_BYTES = 1024 * 1024
OPTIONAL_ARTIFACT_MAX_BYTES = 8 * 1024 * 1024
ATTEMPT_ARTIFACT_MAX_BYTES = 24 * 1024 * 1024
QA3_CANONICALIZATION = "cpu-replay-ascii-integers-v1"
QA3_PROFILE = "python-cpu-candidate-base-v1"
JOBS_OBSERVATION_MAX_IDS = 200


def require(ok, code):
    if not ok:
        raise ValueError(code)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(value if isinstance(value, bytes) else canonical(value)).hexdigest()


def qa3_canonical(value):
    """Explicit cross-language receipt domain; raw CPU JSON is NOT in it."""
    def check(item):
        if type(item) is dict:
            for key, child in item.items():
                check(key); check(child)
        elif type(item) is list:
            for child in item: check(child)
        elif type(item) is str:
            require(all(32 <= ord(c) <= 126 for c in item), "NATIVE_QA3_CANONICAL")
        elif type(item) is int:
            require(abs(item) <= MAX_SAFE_INTEGER, "NATIVE_QA3_CANONICAL")
        else:
            require(item is None or type(item) is bool, "NATIVE_QA3_CANONICAL")
    check(value)
    return canonical(value)


def qa3_semantic(report, *, approval=False):
    flags = {"scopePreserved", "canonicalPrecisionPreserved", "evaluatorIndependent", "upstreamAttributionPreserved"}
    review_flags = {"casesMeaningful", "negativeControlsDiscriminating", "noTautologicalPass"}
    lists = {"blockingFindings", "findings", "limitations"}
    false_flags = {"gpuValidated", "performanceGainProven", "officialActionsPerformed"}
    require(type(report) is dict and set(report) == flags | lists | false_flags | {"version", "decision", "checkerReview"}
            and type(report["version"]) is int and report["version"] == 3
            and report["decision"] in {"approve_for_check", "reject"}
            and all(type(report[f]) is bool for f in flags)
            and all(report[f] is False for f in false_flags), "NATIVE_QA3_REPORT_INVALID")
    review = report["checkerReview"]
    valid_text = lambda value: type(value) is str and 0 < len(value.strip()) <= 8000
    require(type(review) is dict and set(review) == review_flags | {"rationale"}
            and all(type(review[f]) is bool for f in review_flags) and valid_text(review["rationale"])
            and all(type(report[f]) is list and len(report[f]) <= 100 and all(valid_text(x) for x in report[f]) for f in lists)
            and bool(report["limitations"]), "NATIVE_QA3_REPORT_INVALID")
    approved = (report["decision"] == "approve_for_check" and all(report[f] for f in flags)
                and all(review[f] for f in review_flags) and not report["blockingFindings"])
    if approval: require(approved, "QA_V3_SEMANTIC_REJECT")
    return approved


def qa3_cpu_result(receipt, candidate_sha, checker_sha):
    require(receipt.get("exitCode") == 0 and type(receipt.get("exitCode")) is int
            and receipt.get("signal") is None and all(receipt.get(k) is False for k in
                ("timedOut", "oomKilled", "cancelled", "outputLimitExceeded"))
            and receipt.get("outputsComplete") is True and receipt.get("outputs", {}).get("result"),
            "CPU_V3_EXECUTION_FAILED")
    cpu = strict_json(base64.b64decode(receipt["outputs"]["result"]["base64"], validate=True))
    require(type(cpu) is dict and cpu.get("passed") is True and cpu.get("candidateSHA256") == candidate_sha
            and cpu.get("checkerSHA256") == checker_sha, "CPU_V3_RESULT_INVALID")
    for name, flag in (("cases", "passed"), ("negativeControls", "detected")):
        rows = cpu.get(name)
        require(type(rows) is list and 0 < len(rows) <= 10000
                and all(type(r) is dict and type(r.get("name")) is str and r["name"].strip() and r.get(flag) is True for r in rows)
                and len({r["name"] for r in rows}) == len(rows), "CPU_V3_RESULT_INVALID")
    return cpu


def kernel_code_structure(raw):
    """Inspect syntax only; never run a candidate to decide CHECK admission.

    Comments, formatting and declared docstrings are not a new kernel experiment.
    This comparison does not claim general semantic equivalence: all executable
    expressions, constants, decorators and ordinary string literals are retained.
    """
    tree = ast.parse(raw)
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            if (node.body and isinstance(node.body[0], ast.Expr)
                    and isinstance(node.body[0].value, ast.Constant)
                    and isinstance(node.body[0].value.value, str)):
                node.body = node.body[1:]
    return ast.dump(tree, include_attributes=False)


def strict_json(raw):
    def pairs(rows):
        value = {}
        for key, item in rows:
            require(key not in value, "JSON_DUPLICATE_KEY")
            value[key] = item
        return value
    def bad_constant(_):
        raise ValueError("JSON_NONFINITE")
    def finite_float(text):
        value = float(text)
        require(math.isfinite(value), "JSON_NONFINITE")
        return value
    return json.loads(raw, object_pairs_hook=pairs, parse_constant=bad_constant, parse_float=finite_float)


def request_numbers_v1(value):
    """Versioned JSON-number representation shared with the native POST writer.

    Booleans stay booleans; signed/integral floating zero becomes integer zero.
    Reject nonfinite and unsafe numeric values rather than rounding them through
    the API's JavaScript number representation. Legacy receipts are never edited.
    """
    if type(value) in (int, float):
        require(abs(value) <= MAX_SAFE_INTEGER and math.isfinite(value), "REQUEST_NUMBER_UNSAFE")
        return int(value) if type(value) is float and value.is_integer() else value
    if type(value) is dict:
        require(all(type(key) is str for key in value), "REQUEST_JSON_TYPE")
        return {key: request_numbers_v1(item) for key, item in value.items()}
    if type(value) is list:
        return [request_numbers_v1(item) for item in value]
    require(value is None or type(value) in (bool, str), "REQUEST_JSON_TYPE")
    return value


def same_json_numeric_representation(left, right):
    """Only a finite safe integral float/int representation may differ."""
    if type(left) is not type(right):
        return (type(left) in (int, float) and type(right) in (int, float)
                and abs(left) <= MAX_SAFE_INTEGER and abs(right) <= MAX_SAFE_INTEGER
                and math.isfinite(left) and math.isfinite(right)
                and left == right)
    if type(left) is dict:
        return left.keys() == right.keys() and all(same_json_numeric_representation(left[k], right[k]) for k in left)
    if type(left) is list:
        return len(left) == len(right) and all(same_json_numeric_representation(a, b) for a, b in zip(left, right))
    if type(left) is float:
        return math.isfinite(left) and math.isfinite(right) and left == right
    return left == right


class PreflightQuotaError(ValueError):
    """Internal provenance: quota refused before any new submission intent."""


class AccountPreflightWait(ValueError):
    """Another account effect is unresolved; this candidate has no new intent."""


class ReservedUncertainEffect(ValueError):
    """Internal proof of a reviewed reservation, never proof of no effect."""


class NativeDiagnosticError(ValueError):
    """A native audit rejected before this call could create a POST intent."""
    def __init__(self, error):
        super().__init__(str(error))
        self.evidence_errors = getattr(error, "evidence_errors", [])


class EvidenceJSONError(ValueError):
    """Authenticated worker evidence is rejected, with bounded repair references."""
    def __init__(self, stage, artifact):
        super().__init__("QA_EVIDENCE_JSON_NONFINITE" if stage == "independent-qa" else "CPU_EVIDENCE_JSON_NONFINITE")
        self.evidence_errors = [{"stage": stage, "code": "JSON_NONFINITE",
                                 "artifact": {key: artifact[key] for key in ("id", "name", "digest", "size")}}]


def protected(path, immutable=False, maximum=16 * 1024 * 1024):
    """Root ownership, no writable ancestors/symlinks, bounded regular file."""
    path = pathlib.Path(path)
    require(path.is_absolute() and path.resolve() == path, "PROTECTED_PATH")
    for part in (path, *path.parents):
        info = part.lstat()
        require(info.st_uid == 0 and not info.st_mode & 0o022 and not stat.S_ISLNK(info.st_mode), "PROTECTED_OWNER_MODE")
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as stream:
        info = os.fstat(stream.fileno())
        require(stat.S_ISREG(info.st_mode) and info.st_size <= maximum, "PROTECTED_FILE")
        require(not immutable or not info.st_mode & 0o222, "PROTECTED_NOT_IMMUTABLE")
        raw = stream.read(maximum + 1)
    require(len(raw) <= maximum, "PROTECTED_SIZE")
    return raw


def pinned(ref, immutable=True):
    require(isinstance(ref, dict) and SHA.fullmatch(ref.get("sha256", "")), "PIN_REQUIRED")
    raw = protected(ref["path"], immutable=immutable)
    require(digest(raw) == ref["sha256"], "PIN_CHANGED")
    return raw


def watcher_state(policy):
    """Bounded recovery for accumulated controller history, never a generic limit.

    Keep every input and intent intact. This is headroom for recovery, not a
    retention strategy; immutable external payloads need a separate migration.
    """
    raw = protected(pathlib.Path(policy["watcherStateDirectory"]) / "state.json",
                    maximum=WATCHER_STATE_MAX_BYTES)
    state = strict_json(raw)
    require(isinstance(state, dict), "WATCH_STATE_SCHEMA")
    require(state.get("watchId") == policy["watchId"], "WATCH_STATE_SCOPE")
    require(isinstance(state.get("experiments", []), list)
            and all(isinstance(row, dict) for row in state.get("experiments", [])), "WATCH_STATE_SCHEMA")
    return state


def load_helper(ref, name):
    pinned(ref)
    spec = importlib.util.spec_from_file_location(name, ref["path"])
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    require(digest(protected(ref["path"], immutable=True)) == ref["sha256"], "HELPER_CHANGED")
    return module


def validate_policy(policy, now, adapter_sha, allow_disabled=False):
    require(policy.get("version") == 1 and (policy.get("enabled") is True or (allow_disabled and policy.get("enabled") is False)), "POLICY_DISABLED")
    require(policy.get("authority") == "operator-delegated-diagnostic-check", "POLICY_AUTHORITY")
    require(policy.get("allowedActions") == ["check"] and policy.get("automaticPostRetry") is False, "POLICY_ACTION_SCOPE")
    require(policy.get("adapterSHA256") == adapter_sha, "ADAPTER_NOT_PINNED")
    require(type(policy.get("notBefore")) in (int, float) and type(policy.get("expiresAt")) in (int, float)
            and policy["notBefore"] <= now < policy["expiresAt"]
            and 0 < policy["expiresAt"] - policy["notBefore"] <= 7 * 86400, "POLICY_EXPIRED_OR_UNBOUNDED")
    require(isinstance(policy.get("watchId"), str) and 1 <= len(policy["watchId"]) <= 100, "WATCH_ID")
    require(UUID.fullmatch(policy.get("controlCycleId", "")), "CONTROL_ID")
    require(policy.get("project") == "openkernel" and isinstance(policy.get("workspace"), str)
            and policy["workspace"].startswith("openkernel/"), "WORKSPACE_SCOPE")
    require(policy.get("boards") and set(policy["boards"]).issubset(BOARDS), "BOARD_SCOPE")
    require(policy.get("quotaPolicy") == "official-account-limits", "POLICY_QUOTA_SCOPE")
    require(type(policy.get("minimumChecksRemaining")) is int and policy["minimumChecksRemaining"] >= 0, "QUOTA_RESERVE")
    reservation_policy(policy)
    jobs_observation_policy(policy)
    qa3 = policy.get("nativeQA3Registrations")
    if qa3 is not None:
        require(type(qa3) is dict and set(qa3) == {"version", "registrations"}
                and type(qa3.get("version")) is int and qa3["version"] == 1
                and type(qa3.get("registrations")) is list and 0 < len(qa3["registrations"]) <= 32,
                "NATIVE_QA3_REGISTRATION_POLICY")
        seen = set()
        for ref in qa3["registrations"]:
            require(type(ref) is dict and set(ref) == {"path", "digest", "executorPath", "executorSHA256", "profile"}
                    and all(type(ref[k]) is str and ref[k].startswith("/") and pathlib.PurePosixPath(ref[k]).is_absolute()
                            and ".." not in pathlib.PurePosixPath(ref[k]).parts for k in ("path", "executorPath"))
                    and SHA.fullmatch(ref.get("digest", "")) and SHA.fullmatch(ref.get("executorSHA256", ""))
                    and ref.get("profile") == QA3_PROFILE and ref["path"] not in seen, "NATIVE_QA3_REGISTRATION_POLICY")
            seen.add(ref["path"])
    for key in ("accountId", "model", "stateDirectory", "legacyLedger", "accountLock", "watcherStateDirectory", "registryPath", "artifactDirectory", "workerRunsDirectory"):
        require(isinstance(policy.get(key), str) and policy[key], "POLICY_FIELD_" + key)
    origins = policy.get("nativeSourceOrigins")
    if origins is not None:
        require(isinstance(origins, dict) and type(origins.get("enabled")) is bool
                and type(origins.get("maxDepth")) is int and 1 <= origins["maxDepth"] <= 8
                and origins.get("registryDirectory") == str(pathlib.Path(policy["stateDirectory"]) / "source-origins")
                and pathlib.Path(origins["registryDirectory"]).is_absolute(), "SOURCE_ORIGIN_POLICY_SCOPE")
    require(SHA.fullmatch(policy.get("rulesEvidence", {}).get("llmsSha256", ""))
            and isinstance(policy["rulesEvidence"].get("version"), str), "RULES_BINDING")
    for board in policy["boards"]:
        contract = policy["boards"][board]
        files = contract.get("officialFiles", {})
        require(isinstance(files, dict) and {"TASK.md", "AGENTS.md"}.issubset(files)
                and all(SHA.fullmatch(h) for h in files.values()), "OFFICIAL_FILES_BINDING")
        require(isinstance(contract.get("materials"), dict)
                and {"canonical-TASK.md", "canonical-AGENTS.md"}.issubset(contract["materials"]), "MATERIAL_BINDING")
        require(contract["materials"]["canonical-TASK.md"] == files["TASK.md"]
                and contract["materials"]["canonical-AGENTS.md"] == files["AGENTS.md"], "MATERIAL_CANONICAL_MISMATCH")
        require(contract.get("seedSources") and all(SHA.fullmatch(h) for h in contract["seedSources"]), "PROVENANCE_ROOTS_REQUIRED")
        for seed in contract["seedSources"].values():
            require(isinstance(seed.get("provenance"), list) and bool(seed["provenance"])
                    and isinstance(seed.get("attribution"), str) and bool(seed["attribution"].strip()), "PROVENANCE_ATTRIBUTION_REQUIRED")
    return policy


def reservation_policy(policy):
    value = policy.get("uncertainCheckReservations")
    if value is None: return None
    require(type(value) is dict and set(value) == {"version", "intents"}
            and type(value.get("version")) is int and value["version"] == 1
            and type(value.get("intents")) is list and 1 <= len(value["intents"]) <= 16,
            "UNCERTAIN_RESERVATION_POLICY")
    seen = set()
    for row in value["intents"]:
        require(type(row) is dict and set(row) == {"problem", "sourceSHA256", "occurrenceId", "intentEpoch", "ledgerSHA256", "originPolicy"}
                and row.get("problem") in policy["boards"] and SHA.fullmatch(row.get("sourceSHA256", ""))
                and UUID.fullmatch(row.get("occurrenceId", "")) and SHA.fullmatch(row.get("ledgerSHA256", ""))
                and type(row.get("intentEpoch")) in (int, float) and math.isfinite(row["intentEpoch"]) and row["intentEpoch"] > 0,
                "UNCERTAIN_RESERVATION_POLICY")
        ref = row["originPolicy"]
        require(type(ref) is dict and set(ref) == {"path", "sha256"} and type(ref["path"]) is str
                and pathlib.Path(ref["path"]).is_absolute() and SHA.fullmatch(ref.get("sha256", "")), "UNCERTAIN_RESERVATION_POLICY")
        key = (row["problem"], row["sourceSHA256"])
        require(key not in seen, "UNCERTAIN_RESERVATION_POLICY")
        seen.add(key)
    return value


def jobs_observation_policy(policy):
    value = policy.get("jobsObservation")
    if value is None: return None
    require(type(value) is dict and set(value) == {"version", "indexPath", "bootstrap"}
            and type(value["version"]) is int and value["version"] == 1,
            "JOBS_OBSERVATION_POLICY")
    path = pathlib.Path(value["indexPath"])
    require(path.is_absolute() and path.parent == pathlib.Path(policy["stateDirectory"])
            and re.fullmatch(r"jobs-observation-v1-[a-z0-9]{8,64}\.json", path.name),
            "JOBS_OBSERVATION_POLICY")
    ref = value["bootstrap"]
    require(type(ref) is dict and set(ref) == {"path", "sha256"}
            and type(ref["path"]) is str and pathlib.Path(ref["path"]).is_absolute()
            and SHA.fullmatch(ref.get("sha256", "")), "JOBS_OBSERVATION_POLICY")
    return value


def jobs_window_identity(response, now, maximum=50):
    """Identity only: result/status changes must not rewrite historical jobs."""
    require(type(response) is list or type(response) is dict and set(response) == {"jobs"}, "JOBS_WINDOW_SCHEMA")
    rows = response if type(response) is list else response["jobs"]
    require(type(rows) is list and len(rows) <= maximum, "JOBS_WINDOW_SIZE")
    result = []
    for row in rows:
        require(type(row) is dict and JOB.fullmatch(row.get("job_id", ""))
                and type(row.get("problem")) is str and 1 <= len(row["problem"]) <= 100
                and row.get("mode") in {"check", "run"}
                and type(row.get("submitted_at")) in (int, float)
                and math.isfinite(row["submitted_at"]) and 0 < row["submitted_at"] <= now + 5,
                "JOBS_WINDOW_IDENTITY")
        result.append({key: row[key] for key in ("job_id", "problem", "mode", "submitted_at")})
    require(len({row["job_id"] for row in result}) == len(result)
            and [row["submitted_at"] for row in result] == sorted((row["submitted_at"] for row in result), reverse=True),
            "JOBS_WINDOW_ORDER")
    return result


def jobs_window_complete(window, day):
    return len(window) < 50 or window[-1]["submitted_at"] < day


def jobs_head_overlap(previous_window, previous_jobs, current_window):
    """A repeated head and exact remaining prefix establish continuity."""
    if not previous_window:
        require(len(current_window) < 50, "JOBS_WINDOW_HEAD_MISSING")
        return copy.deepcopy(current_window)
    matches = [i for i, row in enumerate(current_window) if row["job_id"] == previous_window[0]["job_id"]]
    require(len(matches) == 1, "JOBS_WINDOW_HEAD_MISSING")
    offset = matches[0]
    overlap = current_window[offset:]
    require(canonical(overlap) == canonical(previous_window[:len(overlap)])
            and len(overlap) == min(len(previous_window), 50 - offset), "JOBS_WINDOW_OVERLAP_CHANGED")
    known = {row["job_id"] for row in previous_jobs}
    require(all(row["job_id"] not in known for row in current_window[:offset]), "JOBS_WINDOW_REUSED_ID")
    combined = copy.deepcopy(current_window[:offset]) + copy.deepcopy(previous_jobs)
    require(len(combined) <= JOBS_OBSERVATION_MAX_IDS, "JOBS_OBSERVATION_ID_LIMIT")
    return jobs_window_identity(combined, max(row["submitted_at"] for row in combined), JOBS_OBSERVATION_MAX_IDS)


def source_screen(raw):
    """Conservative diagnostic-lane gate, not a proof of semantic legality."""
    tree = ast.parse(raw.decode("utf-8"))
    denied = {"os", "sys", "subprocess", "socket", "requests", "urllib", "http", "pathlib", "inspect", "importlib", "pickle", "marshal", "ctypes",
              "base64", "binascii", "codecs", "zlib", "gzip", "bz2", "lzma", "builtins"}
    dynamic = {"exec", "eval", "compile", "__import__", "open", "getattr", "setattr", "globals", "locals", "__builtins__"}
    loaders = {"load_library", "load_binary", "load_cubin", "cuModuleLoad", "cuModuleLoadData", "cuModuleLoadDataEx",
               "cuLibraryLoadData", "cuLibraryLoadFromFile", "module_from_buffer", "fromhex", "b64decode", "b85decode", "a85decode", "decompress"}
    for node in ast.walk(tree):
        modules = ([n.name for n in node.names] if isinstance(node, ast.Import)
                   else [node.module or ""] if isinstance(node, ast.ImportFrom) else [])
        require(not any(m.split(".")[0] in denied or re.search(r"(^|\.)(reference|checks|evaluator|runner|static_checks)(\.|$)", m) for m in modules), "SOURCE_REQUIRES_MANUAL_REVIEW")
        if isinstance(node, ast.ImportFrom):
            require(not any(n.name in dynamic | loaders for n in node.names), "SOURCE_REQUIRES_MANUAL_REVIEW")
        if isinstance(node, ast.Name):
            # Also catch aliases such as f = exec; the later call's name alone
            # cannot establish that submitted code remains inspectable source.
            require(node.id not in dynamic | loaders, "SOURCE_REQUIRES_MANUAL_REVIEW")
        if isinstance(node, ast.Attribute):
            require(node.attr not in {"environ", "getenv", "system", "popen", "__dict__", "__globals__", "__subclasses__"} | dynamic | loaders,
                    "SOURCE_REQUIRES_MANUAL_REVIEW")


def replay_commands(raw, checker_sha, candidate_sha):
    """Require actual bash call/result pair and pinned files, not a report claim.

    QA reports the exact command it ran. Commands must execute cpu-checks.py and
    print both content hashes in the successful tool result. We do not execute
    shell text or candidate code, and do not consider echo-only calls evidence.
    """
    calls, results = {}, {}
    for line in raw.splitlines():
        row = strict_json(line)
        data = row.get("data", {})
        if row.get("type") == "tool/call" and data.get("name") == "bash":
            arguments = data.get("arguments")
            args = strict_json(arguments) if isinstance(arguments, str) else arguments
            command = args.get("command", "") if isinstance(args, dict) else ""
            calls[data.get("callId")] = command
        elif row.get("type") == "tool/result":
            message = data.get("message", {})
            key = message.get("source", {}).get("callId")
            results[key] = canonical(message).decode()
    accepted = []
    for key, command in calls.items():
        # Shell parsing is only a classifier; the independent QA must inspect
        # checker semantics, discriminants and negative controls separately.
        try:
            lexer = shlex.shlex(command, posix=True, punctuation_chars=";&|()<>")
            lexer.whitespace_split = True
            tokens = list(lexer)
        except ValueError:
            continue
        # One static workspace prefix is equivalent for the two absolute
        # artifact operands and the identical checker invocation. Do not infer
        # other cwd changes, relative operands or compound shell semantics.
        if tokens[:3] == ["cd", "/workspace", "&&"]:
            tokens = tokens[3:]
            if len(tokens) < 6 or not all(
                    pathlib.PurePosixPath(value).is_absolute()
                    and not value.startswith("//") and ".." not in pathlib.PurePosixPath(value).parts
                    for value in tokens[1:3]):
                continue
        # A narrow direct-command protocol, not arbitrary shell interpretation:
        # sha256sum CHECKER CANDIDATE && python3 CHECKER [arguments].
        # Reject echo/python-looking strings, false &&, nested shells, redirects,
        # substitutions and additional statements even if their transcript says 0.
        executes = (len(tokens) >= 6 and tokens[0] == "sha256sum" and tokens[3] == "&&"
            and pathlib.PurePosixPath(tokens[1]).name == "cpu-checks.py"
            and pathlib.PurePosixPath(tokens[2]).name == "candidate.py"
            and bool(re.fullmatch(r"(?:.*/)?python(?:3(?:\.\d+)?)?", tokens[4]))
            and tokens[5] == tokens[1]
            and all(t not in {";", "&&", "||", "|", "&", "(", ")", "<", ">"} for t in tokens[4:])
            and all(c not in command for c in ("$", "`", "\n", "\r")))
        result = results.get(key, "")
        if executes and "[Command finished with exit code 0]" in result and checker_sha in result and candidate_sha in result:
            accepted.append({"callId": key, "commandSHA256": digest(command.encode()), "resultSHA256": digest(result.encode())})
    require(accepted, "CPU_REPLAY_EXECUTION_UNPROVEN")
    return accepted


def semantic_qa(verification, qa, cpu, source_sha, candidate_sha, checker_sha, result_sha):
    for row in (verification, qa):
        require(row.get("passed") is True and row.get("scopePreserved") is True
                and row.get("sourceSHA256") == source_sha and row.get("candidateSHA256") == candidate_sha,
                "QA_SOURCE_OR_SCOPE_MISMATCH")
        require(row.get("gpuValidated") is False and row.get("officialActionsPerformed") is False
                and row.get("performanceGainProven", False) is False, "QA_OVERCLAIM")
    require(qa.get("approvedForOfficialReview") is True and qa.get("cpuReplayPassed") is True, "QA_NOT_APPROVED")
    for key in ("canonicalPrecisionPreserved", "evaluatorIndependent", "upstreamAttributionPreserved"):
        require(qa.get(key) is True, "QA_ATTESTATION_MISSING_" + key)
    require(qa.get("blockingFindings") == [] and isinstance(qa.get("findings"), list), "QA_BLOCKING_FINDINGS")
    require(isinstance(qa.get("limitations"), list) and bool(qa["limitations"]), "QA_LIMITATIONS_MISSING")
    evidence = qa.get("cpuEvidence", {})
    require(evidence.get("checkerSHA256") == checker_sha and evidence.get("resultSHA256") == result_sha, "QA_TEST_BINDING")
    require(cpu.get("passed") is True and cpu.get("candidateSHA256") == candidate_sha
            and cpu.get("checkerSHA256") == checker_sha, "CPU_RESULT_BINDING")
    for name, flag in (("cases", "passed"), ("negativeControls", "detected")):
        original, rows = cpu.get(name), evidence.get(name)
        for report in (original, rows):
            require(isinstance(report, list) and bool(report) and all(isinstance(r, dict)
                    and isinstance(r.get("name"), str) and r["name"].strip() and r.get(flag) is True for r in report)
                    and len({r["name"] for r in report}) == len(report), "CPU_MEANINGFUL_" + name)
        # QA is instructed to report name/flag rows. A textual diagnostic may
        # be omitted. Structured detail is evidence: preserve it in full with
        # exact JSON types, just like every other field, identity and flag.
        require(canonical([{k: v for k, v in r.items() if k != "detail"} for r in original]) ==
                canonical([{k: v for k, v in r.items() if k != "detail"} for r in rows]), "CPU_CASES_NOT_REPLAYED")
        for source, review in zip(original, rows):
            if isinstance(source.get("detail"), str):
                require("detail" not in review or canonical(review["detail"]) == canonical(source["detail"]),
                        "CPU_CASES_NOT_REPLAYED")
            else:
                require(("detail" in source) == ("detail" in review) and
                        ("detail" not in source or canonical(review["detail"]) == canonical(source["detail"])),
                        "CPU_CASES_NOT_REPLAYED")


def apply_single_file_patch(base, patch):
    """Pure unified-patch application; no shell, filesystem writes or paths."""
    lines = patch.decode("utf-8").splitlines(keepends=True)
    require(len(lines) >= 3 and lines[:2] == ["--- base-candidate.py\n", "+++ candidate.py\n"], "PATCH_HEADER_SCOPE")
    original = base.decode("utf-8").splitlines(keepends=True)
    output, cursor, i, hunks = [], 0, 2, 0
    while i < len(lines):
        match = re.fullmatch(r"@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@[^\n]*\n", lines[i])
        require(match is not None, "PATCH_HUNK_SCHEMA")
        start, old_count, new_start, new_count = (int(match[1]), int(match[2] or 1), int(match[3]), int(match[4] or 1))
        target = start - 1 if old_count else start
        require(cursor <= target <= len(original), "PATCH_HUNK_RANGE")
        output.extend(original[cursor:target]); cursor = target
        require(new_start == len(output) + (1 if new_count else 0), "PATCH_NEW_RANGE")
        removed = added = 0
        i += 1; hunks += 1
        while i < len(lines) and not lines[i].startswith("@@ "):
            line = lines[i]
            require(line[0] in " +-", "PATCH_BODY_SCHEMA")
            if line[0] in " -":
                require(cursor < len(original) and original[cursor] == line[1:], "PATCH_BASE_MISMATCH")
                cursor += 1; removed += 1
            if line[0] in " +":
                output.append(line[1:]); added += 1
            i += 1
        require(removed == old_count and added == new_count, "PATCH_HUNK_COUNT")
    require(hunks > 0, "PATCH_EMPTY")
    output.extend(original[cursor:])
    return "".join(output).encode()


class NativeAudit:
    def __init__(self, policy, runtime, transport_helper):
        self.p, self.runtime, self.io = policy, runtime, transport_helper
        self.artifacts = pathlib.Path(policy["artifactDirectory"])
        self.runs = pathlib.Path(policy["workerRunsDirectory"])

    def artifact_limit(self, row, *, optional=False):
        # Acceptance evidence and source aliases never inherit the attachment
        # allowance, even when their name is optional in the other step.
        canonical_names = PRODUCER_FILES | QA_FILES | {"base-candidate.py"}
        return (OPTIONAL_ARTIFACT_MAX_BYTES if optional is True and row.get("name") not in canonical_names
                else REQUIRED_ARTIFACT_MAX_BYTES)

    def artifact_budget(self, rows):
        # Match the native bridge's per-delivery bounds before allocating any
        # attachment bytes. Count aliases separately, as the bridge does.
        require(all(type(r.get("size")) is int and 0 <= r["size"] <= OPTIONAL_ARTIFACT_MAX_BYTES for r in rows),
                "ARTIFACT_SIZE")
        require(sum(r["size"] for r in rows) <= ATTEMPT_ARTIFACT_MAX_BYTES, "ARTIFACT_TOTAL_SIZE")

    def artifact(self, row, *, allow_empty=False, optional=False):
        require(UUID.fullmatch(row.get("id", "")) and SHA.fullmatch(row.get("digest", "")), "ARTIFACT_IDENTITY")
        # Only the trusted audit loop can allow empty optional attachments.
        # Required acceptance evidence and source lookups remain nonempty.
        # Empty content still has to match its real size and SHA256 below.
        maximum = self.artifact_limit(row, optional=optional)
        require(type(row.get("size")) is int and (0 < row["size"] <= maximum
                or row["size"] == 0 and allow_empty is True), "ARTIFACT_SIZE")
        raw = self.io.read_regular(self.artifacts / row["digest"], maximum)
        require(len(raw) == row["size"] and digest(raw) == row["digest"], "ARTIFACT_BYTES_CHANGED")
        return raw

    def qa_contract(self, definition):
        specs = definition.get("steps", [])
        require(len(specs) == 2 and [s.get("key") for s in specs] == ["produce", "independent-qa"], "NATIVE_DAG_CHANGED")
        qa = specs[1]
        parameters = qa.get("parameters", {})
        require(type(parameters) is dict and type(specs[0].get("parameters", {})) is dict, "NATIVE_QA_CONTRACT")
        version = parameters.get("nativeQAContractVersion")
        if version is None and "dependencyEvidence" not in qa:
            return None
        if type(version) is int and version == 3:
            contract = {"version": 2, "kind": "controller-cpu-replay", "sourceStep": "produce",
                        "reportArtifact": "qa-verification.json"}
            require(parameters == {"nativeQAContractVersion": 3, "cpuReplayObservationVersion": 1,
                                    "cpuReplayProfile": QA3_PROFILE}
                    and canonical(qa.get("dependencyEvidence")) == canonical(contract)
                    and type(qa.get("maxAttempts")) is int and qa["maxAttempts"] == 1
                    and type(specs[0].get("maxAttempts")) is int and specs[0]["maxAttempts"] == 1
                    and not specs[0].get("dependencyEvidence") and not qa.get("repairStep")
                    and "nativeQAContractVersion" not in specs[0].get("parameters", {})
                    and "cpuReplayObservationVersion" not in specs[0].get("parameters", {})
                    and definition.get("allowedActions") == ["native.cpu-replay.observe"], "NATIVE_QA_CONTRACT")
            return contract
        bindings = [
            {"reportArtifact": "qa-verification.json", "jsonPointer": "/cpuEvidence/checkerSHA256", "dependencyArtifact": "cpu-checks.py"},
            {"reportArtifact": "qa-verification.json", "jsonPointer": "/cpuEvidence/resultSHA256", "dependencyArtifact": "cpu-result.json"}]
        contract = {"version": 1, "sourceStep": "produce", "bindings": bindings}
        require(type(version) is int and version == 2
                and canonical(qa.get("dependencyEvidence")) == canonical(contract)
                and type(qa.get("maxAttempts")) is int and 1 <= qa["maxAttempts"] <= 3
                and type(specs[0].get("maxAttempts")) is int and specs[0]["maxAttempts"] == 1
                and not specs[0].get("dependencyEvidence")
                and "nativeQAContractVersion" not in specs[0].get("parameters", {})
                and not qa.get("repairStep"), "NATIVE_QA_CONTRACT")
        return contract

    def qa_attempt_chain(self, detail, *, allow_failed=False):
        """V2 opt-in only: validate every stopped QA and its frozen producer.

        A previous mismatch is evidence for a bounded consumer retry, never QA
        acceptance. Legacy executions retain their single-attempt contract.
        """
        occurrence = detail["occurrence"]
        contract = self.qa_contract(occurrence["definition"])
        if contract and contract.get("version") == 2:
            return self.qa3_attempt_chain(detail, allow_failed=allow_failed)
        require(contract is not None, "NATIVE_QA_CONTRACT")
        specs = occurrence["definition"]["steps"]
        steps = {s["key"]: s for s in detail["steps"]}
        require(len(detail["steps"]) == len(steps) == 2
                and all(steps[s["key"]]["definition"] == s for s in specs), "NATIVE_QA_STEPS")
        producer_rows = [a for a in detail["attempts"] if a["step_id"] == steps["produce"]["id"]]
        qa_rows = [a for a in detail["attempts"] if a["step_id"] == steps["independent-qa"]["id"]]
        require(len(producer_rows) == 1 and len(detail["attempts"]) == 1 + len(qa_rows), "NATIVE_QA_ATTEMPT_SET")
        producer = producer_rows[0]
        require(type(producer.get("number")) is int and producer["number"] == 1, "NATIVE_QA_PRODUCER")
        if not qa_rows:
            require(allow_failed and producer["state"] == "failed" and steps["independent-qa"]["state"] == "queued"
                    and occurrence["state"] in {"failed", "attention"}, "NATIVE_QA_ATTEMPT_SET")
            return {"produce": producer}, []
        require(1 <= len(qa_rows) <= specs[1]["maxAttempts"]
                and all(type(a.get("number")) is int for a in qa_rows), "NATIVE_QA_ATTEMPT_SET")
        qa_rows = sorted(qa_rows, key=lambda a: a["number"])
        require([a["number"] for a in qa_rows] == list(range(1, len(qa_rows) + 1))
                and type(steps["independent-qa"].get("attempt")) is int
                and steps["independent-qa"]["attempt"] == len(qa_rows), "NATIVE_QA_ATTEMPT_SET")
        actors = [producer, *qa_rows]
        for actor in actors:
            self.artifact_budget([r for r in detail["artifacts"] if r["attempt_id"] == actor["id"]])
        require(len({a["id"] for a in actors}) == len(actors)
                and len({a["fence"] for a in actors}) == len(actors)
                and all(UUID.fullmatch(a["id"]) and UUID.fullmatch(a["fence"])
                        and a.get("finished_at") and a.get("receipt", {}).get("stopped") is True
                        and a.get("model") == self.p["model"] and a.get("account_id") == self.p["accountId"] for a in actors),
                "NATIVE_QA_ACTOR_BINDING")
        require(producer["state"] == steps["produce"]["state"] == "completed"
                and type(steps["produce"].get("attempt")) is int and steps["produce"]["attempt"] == 1,
                "NATIVE_QA_PRODUCER")
        source = {"stepId": steps["produce"]["id"], "key": "produce", "attemptId": producer["id"],
                  "attemptNumber": 1, "runFence": producer["fence"]}
        source_rows = sorted([r for r in detail["artifacts"] if r["attempt_id"] == producer["id"]], key=lambda r: (r["name"], r["id"]))
        require(0 < len(source_rows) <= 256 and len({r["name"] for r in source_rows}) == len(source_rows), "NATIVE_QA_INVENTORY")
        require(all(type(r.get("name")) is str and re.fullmatch(r"[\w][\w.-]{0,160}", r["name"])
                    for r in source_rows), "ARTIFACT_NAME")
        source_refs = [{k: r[k] for k in ("id", "name", "digest", "size")} for r in source_rows]
        for row in source_rows:
            self.artifact(row, allow_empty=row["name"] not in PRODUCER_FILES, optional=row["name"] not in PRODUCER_FILES)
        contract_sha = digest([1, "produce", [[b["reportArtifact"], b["jsonPointer"], b["dependencyArtifact"]] for b in contract["bindings"]]])
        history = []
        for index, attempt in enumerate(qa_rows):
            last = index == len(qa_rows) - 1
            completed = last and attempt["state"] == "completed"
            require(completed or (not last or allow_failed) and attempt["state"] == "failed"
                    and attempt.get("error_code") == "DEPENDENCY_ARTIFACT_BINDING", "NATIVE_QA_RETRY_CAUSE")
            if last:
                require(steps["independent-qa"]["state"] == "completed" if completed
                        else steps["independent-qa"]["state"] in {"failed", "attention"}, "NATIVE_QA_STEP_STATE")
            manifest = attempt["context"]["manifest"]
            consumer = {"stepId": steps["independent-qa"]["id"], "key": "independent-qa", "attemptId": attempt["id"],
                        "attemptNumber": attempt["number"], "runFence": attempt["fence"]}
            expected = {"version": 1, "contractSHA256": contract_sha, "occurrenceId": occurrence["id"],
                        "consumer": consumer, "source": source, "artifacts": source_refs}
            require(canonical(manifest.get("dependencyEvidence")) == canonical(expected), "NATIVE_QA_DEPENDENCY_SNAPSHOT")
            for row in source_rows:
                handed = self.io.read_regular(self.runs / attempt["id"] / "home/inputs" / row["id"] / row["name"],
                                              self.artifact_limit(row, optional=row["name"] not in PRODUCER_FILES))
                require(len(handed) == row["size"] and digest(handed) == row["digest"], "NATIVE_QA_DEPENDENCY_HANDOFF")
            raw = self.io.read_regular(self.runs / attempt["id"] / "home/inputs" / attempt["id"] / "cycle-input.json", 2 * 1024 * 1024)
            binding = manifest["cycleInput"]
            require(manifest.get("cycleId") == occurrence["cycle_id"] and manifest.get("occurrenceId") == occurrence["id"]
                    and manifest.get("step") == "independent-qa" and binding["attemptId"] == attempt["id"]
                    and binding["runFence"] == attempt["fence"] and binding["digest"] == digest(raw)
                    and binding["size"] == len(raw) and strict_json(raw) == occurrence["input"], "NATIVE_QA_INPUT_BINDING")
            reports = [r for r in detail["artifacts"] if r["attempt_id"] == attempt["id"] and r["name"] == "qa-verification.json"]
            require(len(reports) == 1, "NATIVE_QA_REPORT_SET")
            report_ref = {k: reports[0][k] for k in ("id", "name", "digest", "size")}
            report = strict_json(self.artifact(reports[0])); bindings = []
            require(type(report) is dict and type(report.get("cpuEvidence")) is dict, "NATIVE_QA_REPORT_BINDING")
            for b in contract["bindings"]:
                matches = [r for r in source_refs if r["name"] == b["dependencyArtifact"]]
                require(len(matches) == 1, "NATIVE_QA_INVENTORY")
                received = report.get("cpuEvidence", {}).get(b["jsonPointer"].split("/")[-1])
                require(type(received) is str and SHA.fullmatch(received), "NATIVE_QA_REPORT_BINDING")
                bindings.append({"reportArtifact": report_ref, "jsonPointer": b["jsonPointer"], "dependencyArtifact": matches[0],
                                 "expected": matches[0]["digest"], "received": received})
            status = "matched" if all(b["expected"] == b["received"] for b in bindings) else "mismatch"
            validation = {"version": 1, "status": status, "source": source, "bindings": bindings}
            require(canonical(attempt["context"].get("dependencyEvidenceValidation")) == canonical(validation)
                    and status == ("matched" if completed else "mismatch"), "NATIVE_QA_GATE_PROOF")
            self.runtime.stopped(attempt["id"])
            history.append({"attemptId": attempt["id"], "number": attempt["number"], "fence": attempt["fence"],
                            "state": attempt["state"], "report": report_ref, "snapshotSHA256": digest(expected),
                            "gateProofSHA256": digest(validation)})
        return {"produce": producer, "independent-qa": qa_rows[-1]}, history

    def qa3_verify(self, binding, request):
        """Only the pinned launcher's read-only mode; never execute/inspect.

        Limit pipe allocation and time even if a registered executable fails.
        No candidate/checker command can be supplied by the model or API.
        """
        child = subprocess.Popen(["/usr/bin/python3", "-I", "-B", binding["executorPath"], "verify",
                                  binding["path"], binding["digest"]], stdin=subprocess.PIPE,
                                 stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                                 env={"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8", "PYTHONDONTWRITEBYTECODE": "1"})
        request_raw, output, sent = qa3_canonical(request), bytearray(), 0
        deadline = time.monotonic() + 15
        try:
            with selectors.DefaultSelector() as selector:
                os.set_blocking(child.stdin.fileno(), False); os.set_blocking(child.stdout.fileno(), False)
                selector.register(child.stdin, selectors.EVENT_WRITE); selector.register(child.stdout, selectors.EVENT_READ)
                while selector.get_map():
                    require(time.monotonic() < deadline, "NATIVE_QA3_VERIFY_TIMEOUT")
                    for key, _ in selector.select(min(0.2, max(0, deadline - time.monotonic()))):
                        if key.fileobj is child.stdin:
                            sent += os.write(child.stdin.fileno(), request_raw[sent:sent + 65536])
                            if sent == len(request_raw): selector.unregister(child.stdin); child.stdin.close()
                        else:
                            raw = os.read(child.stdout.fileno(), 65536)
                            if not raw: selector.unregister(child.stdout)
                            else:
                                output.extend(raw)
                                require(len(output) <= 2 * 1024 * 1024, "NATIVE_QA3_VERIFY_SIZE")
            require(child.wait(timeout=max(0.01, deadline - time.monotonic())) == 0, "NATIVE_QA3_VERIFY_FAILED")
            return strict_json(bytes(output))
        finally:
            if child.poll() is None: child.kill(); child.wait()
            if not child.stdin.closed: child.stdin.close()
            child.stdout.close()

    def qa3_evidence(self, detail, producer, attempt, snapshot):
        """Bind the original native request, protected replay and model handoff.

        This proves origin even for semantic rejection. CHECK still requires
        completed independent QA and separately validated CPU/semantic results.
        Fresh physical timestamps are verified, then excluded from stable proof.
        """
        o = detail["occurrence"]
        envelope = self.runtime.api("GET", "/api/cycle-runs/" + o["id"] + "/attempts/" + attempt["id"] + "/cpu-replay")
        require(type(envelope) is dict and len(canonical(envelope)) <= 2 * 1024 * 1024
                and type(envelope.get("version")) is int and envelope["version"] == 1
                and envelope.get("kind") == "native-qa3-controller-evidence"
                and envelope.get("occurrenceId") == o["id"] and envelope.get("attemptId") == attempt["id"]
                and envelope.get("runFence") == attempt["fence"] and envelope.get("definitionVersion") == o["version"]
                and canonical(envelope.get("snapshot")) == canonical(snapshot), "NATIVE_QA3_ENVELOPE_BINDING")
        require(envelope.get("state") == "evidence" and envelope.get("physicalConfirmed") is True,
                "CPU_V3_EXECUTION_UNCONFIRMED")
        require(type(envelope.get("modelDispatched")) is bool, "NATIVE_QA3_MODEL_BOUNDARY")
        model_dispatched = envelope["modelDispatched"]
        binding = envelope.get("registration")
        allowed = self.p.get("nativeQA3Registrations", {})
        require(type(binding) is dict and set(binding) == {"path", "digest", "executorPath", "executorSHA256", "profile"}
                and type(allowed.get("version")) is int and allowed["version"] == 1
                and type(allowed.get("registrations")) is list and 0 < len(allowed["registrations"]) <= 32
                and any(canonical(binding) == canonical(x) for x in allowed["registrations"])
                and binding["profile"] == QA3_PROFILE and SHA.fullmatch(binding["digest"])
                and SHA.fullmatch(binding["executorSHA256"]), "NATIVE_QA3_REGISTRATION")
        raw_registration = protected(pathlib.Path(binding["path"]), immutable=True, maximum=16384)
        registration = strict_json(raw_registration)
        require(digest(raw_registration) == binding["digest"] and registration.get("version") == 1
                and registration.get("enabled") is True and registration.get("executorPath") == binding["executorPath"]
                and registration.get("executorSHA256") == binding["executorSHA256"]
                and o["definition"]["project"] in registration.get("projects", [])
                and QA3_PROFILE in registration.get("profiles", [])
                and digest(protected(pathlib.Path(binding["executorPath"]), immutable=True, maximum=1024 * 1024)) == binding["executorSHA256"],
                "NATIVE_QA3_REGISTRATION_CHANGED")
        rows = {r["name"]: r for r in detail["artifacts"] if r["attempt_id"] == producer["id"]}
        artifact_ref = lambda row: {k: row[k] for k in ("id", "name", "digest", "size")}
        inputs = []
        for name in ("candidate.py", "cpu-checks.py", "cpu-result.json", "implementation.patch"):
            row = rows[name]; raw = self.artifact(row)
            inputs.append({**artifact_ref(row), "base64": base64.b64encode(raw).decode()})
        experiment = o["input"]["experiment"]
        base_ref = experiment["sourceArtifact"]
        bases = [r for r in experiment["materials"] if r["name"] == "base-candidate.py"]
        require(len(bases) == 1, "NATIVE_QA3_BASE_BINDING")
        base_raw = base64.b64decode(bases[0]["base64"], validate=True)
        require(base_raw == self.artifact(base_ref) and digest(base_raw) == bases[0]["sha256"]
                and len(base_raw) == bases[0]["bytes"], "NATIVE_QA3_BASE_BINDING")
        origin = self.runtime.api("GET", "/api/cycle-runs/" + base_ref["occurrenceId"])
        base_rows = [r for r in origin["artifacts"] if all(r.get(k) == base_ref.get(k) for k in ("id", "name", "digest", "size"))]
        require(len(base_rows) == 1 and origin["occurrence"]["id"] == base_ref["occurrenceId"]
                and origin["occurrence"]["definition"].get("project") == o["definition"]["project"], "NATIVE_QA3_BASE_BINDING")
        base_actors = [a for a in origin["attempts"] if a["id"] == base_rows[0]["attempt_id"]]
        require(len(base_actors) == 1 and base_actors[0]["state"] == "completed"
                and base_actors[0].get("receipt", {}).get("stopped") is True, "NATIVE_QA3_BASE_BINDING")
        base_actor = base_actors[0]
        self.runtime.stopped(base_actor["id"])
        base_steps = [s for s in origin["steps"] if s["id"] == base_actor["step_id"]]
        require(len(base_steps) == 1, "NATIVE_QA3_BASE_BINDING")
        if base_steps[0]["key"] != "produce":
            require(base_steps[0]["key"] == "independent-qa" and self.qa_contract(origin["occurrence"]["definition"]),
                    "NATIVE_QA3_BASE_ALIAS_UNPROVEN")
            base_selected, _ = self.qa_attempt_chain(origin)
            require(base_selected["independent-qa"]["id"] == base_actor["id"], "NATIVE_QA3_BASE_ALIAS_UNPROVEN")
            originals = [r for r in origin["artifacts"] if r["attempt_id"] == base_selected["produce"]["id"] and r["name"] == "candidate.py"]
            require(len(originals) == 1 and self.artifact(originals[0]) == base_raw, "NATIVE_QA3_BASE_ALIAS_UNPROVEN")
        # Before model dispatch no QA handoff exists. The producer's frozen
        # task input carries the same original JSON.stringify bytes; it must
        # match the current authenticated occurrence input, never be invented.
        input_actor = attempt if model_dispatched else producer
        input_raw = self.io.read_regular(self.runs / input_actor["id"] / "home/inputs" / input_actor["id"] / "cycle-input.json", 2 * 1024 * 1024)
        input_binding = input_actor["context"]["manifest"]["cycleInput"]
        require(input_binding.get("attemptId") == input_actor["id"] and input_binding.get("runFence") == input_actor["fence"]
                and input_binding.get("digest") == digest(input_raw) and input_binding.get("size") == len(input_raw)
                and canonical(strict_json(input_raw)) == canonical(o["input"]), "NATIVE_QA3_INPUT_BINDING")
        base_source = {"artifact": artifact_ref(base_ref), "source": {"occurrenceId": base_ref["occurrenceId"],
                       "attemptId": base_actor["id"], "runFence": base_actor["fence"]}, "inputSHA256": digest(input_raw)}
        inputs.append({**artifact_ref(base_ref), "name": "base-candidate.py", "base64": base64.b64encode(base_raw).decode()})
        request = {"version": 1, "requestId": attempt["id"], "occurrenceId": o["id"], "runFence": attempt["fence"],
                   "definitionVersion": o["version"], "project": o["definition"]["project"], "source": snapshot["source"],
                   "artifacts": inputs, "profile": QA3_PROFILE, "baseSource": base_source,
                   "registrationSHA256": binding["digest"], "executorSHA256": binding["executorSHA256"]}
        request_sha = digest(qa3_canonical(request))
        metadata = {**request, "artifacts": [{k: v for k, v in row.items() if k != "base64"} for row in inputs]}
        preparation = attempt["context"].get("cpuReplayPreparation", {})
        require(canonical(envelope.get("request")) == canonical(metadata)
                and envelope.get("requestSHA256") == request_sha
                and envelope.get("requestCanonicalization") == QA3_CANONICALIZATION
                and canonical(preparation.get("registration")) == canonical(binding)
                and preparation.get("requestSHA256") == request_sha
                and canonical(preparation.get("snapshot")) == canonical(snapshot)
                and envelope.get("action") == {"key": o["id"] + ":" + attempt["id"] + ":cpu-replay-v1",
                                               "state": "completed", "inputHash": request_sha}, "NATIVE_QA3_REQUEST_CHANGED")
        receipt = envelope["receipt"]
        require(type(receipt) is dict and envelope.get("receiptCanonicalization") == QA3_CANONICALIZATION
                and digest(qa3_canonical(receipt)) == envelope.get("receiptSHA256"), "NATIVE_QA3_RECEIPT_CHANGED")
        for key in ("requestId", "occurrenceId", "runFence", "project", "profile", "source", "baseSource"):
            require(canonical(receipt.get(key)) == canonical(request[key]), "NATIVE_QA3_RECEIPT_BINDING")
        require(type(receipt.get("version")) is int and receipt["version"] == 1
                and receipt.get("kind") == "cpu-replay-observation" and receipt.get("observationOnly") is True
                and receipt.get("acceptance") is False and receipt.get("requestSHA256") == request_sha
                and receipt.get("registrationSHA256") == binding["digest"] and receipt.get("executorSHA256") == binding["executorSHA256"]
                and receipt.get("unit") == "agentos-cpu-replay-" + attempt["id"] + ".service"
                and receipt.get("argv") == ["/usr/bin/python3", "-I", "-B", "/output/cpu-checks.py", "/output/candidate.py", "/inputs/base-candidate.py"],
                "NATIVE_QA3_RECEIPT_BINDING")
        output_limit = registration.get("limits", {}).get("maxOutputBytes")
        require(type(output_limit) is int and 1024 <= output_limit <= 262144
                and type(receipt.get("outputs")) is dict and set(receipt["outputs"]) == {"stdout", "stderr", "result"}, "NATIVE_QA3_OUTPUTS")
        for name, output in receipt["outputs"].items():
            if name == "result" and output is None: continue
            require(type(output) is dict and set(output) == {"base64", "digest", "size"}
                    and type(output["size"]) is int and 0 <= output["size"] <= output_limit, "NATIVE_QA3_OUTPUTS")
            raw = base64.b64decode(output["base64"], validate=True)
            require(base64.b64encode(raw).decode() == output["base64"] and len(raw) == output["size"]
                    and digest(raw) == output["digest"], "NATIVE_QA3_OUTPUTS")
        verified = self.qa3_verify(binding, request)
        require(verified.get("ok") is True and canonical(verified.get("receipt")) == canonical(receipt), "NATIVE_QA3_PROTECTED_RECEIPT")
        for inspection in (envelope.get("physicalInspection"), verified.get("physicalInspection")):
            require(type(inspection) is dict and type(inspection.get("version")) is int and inspection["version"] == 1
                    and inspection.get("receiptSHA256") == envelope["receiptSHA256"], "NATIVE_QA3_PHYSICAL_PROOF")
            when = dt.datetime.fromisoformat(inspection["observedAt"].replace("Z", "+00:00")).timestamp()
            require(time.time() - 15 <= when <= time.time() + 1, "NATIVE_QA3_PHYSICAL_STALE")
            stop = inspection.get("stopProof", {})
            require(stop.get("stopped") is True and type(stop.get("mainPID")) is int and stop["mainPID"] == 0
                    and stop.get("populated") is False and stop.get("unit") == receipt["unit"], "NATIVE_QA3_PHYSICAL_PROOF")
        raw_receipt = qa3_canonical(receipt)
        if not model_dispatched:
            require(envelope.get("handoff") is None and envelope.get("semanticReport") is None
                    and envelope.get("validation") is None and not attempt["context"].get("manifest", {}).get("nativeQA3")
                    and preparation.get("phase") != "qa_dispatching"
                    and not [r for r in detail["artifacts"] if r["attempt_id"] == attempt["id"]], "NATIVE_QA3_NO_MODEL_BINDING")
            reason = None
            try: qa3_cpu_result(receipt, rows["candidate.py"]["digest"], rows["cpu-checks.py"]["digest"])
            except (ValueError, TypeError, KeyError) as error:
                reason = "NATIVE_QA3_CPU_EXECUTION_FAILED" if str(error) == "CPU_V3_EXECUTION_FAILED" else "NATIVE_QA3_CPU_RESULT_INVALID"
            require(reason is not None, "NATIVE_QA3_NO_MODEL_BINDING")
            terminal = attempt.get("receipt")
            require(type(terminal) is dict and set(terminal) == {"kind", "modelDispatched", "cpu"}
                    and terminal["kind"] == "cpu-preparation-terminal" and terminal["modelDispatched"] is False
                    and type(terminal["cpu"]) is dict and set(terminal["cpu"]) == {"requestId", "requestSHA256", "receiptSHA256", "stopProof"}
                    and terminal["cpu"]["requestId"] == attempt["id"] and terminal["cpu"]["requestSHA256"] == request_sha
                    and terminal["cpu"]["receiptSHA256"] == digest(raw_receipt), "NATIVE_QA3_PREPARATION_PROOF")
            stopped = terminal["cpu"]["stopProof"]
            require(stopped.get("stopped") is True and type(stopped.get("mainPID")) is int and stopped["mainPID"] == 0
                    and stopped.get("populated") is False and stopped.get("unit") == receipt["unit"], "NATIVE_QA3_PREPARATION_PROOF")
            failure = {"version": 1, "status": "rejected", "code": reason, "modelDispatched": False,
                       "snapshotContractSHA256": snapshot["contractSHA256"], "source": snapshot["source"],
                       "requestSHA256": request_sha, "receiptSHA256": digest(raw_receipt),
                       "receiptCanonicalization": QA3_CANONICALIZATION, "terminal": terminal}
            require(canonical(envelope.get("preparationFailure")) == canonical(failure)
                    and canonical(envelope.get("diagnostic")) == canonical(failure)
                    and canonical(attempt["context"].get("nativeQA3PreparationFailure")) == canonical(failure)
                    and attempt.get("error_code") == reason, "NATIVE_QA3_PREPARATION_PROOF")
            return {"request": metadata, "requestSHA256": request_sha, "receipt": receipt, "receiptSHA256": digest(raw_receipt),
                    "registration": binding, "snapshot": snapshot, "handoff": None, "validation": None,
                    "modelDispatched": False, "preparationFailure": failure, "semanticReportSchemaValid": False}
        require(envelope.get("diagnostic") is None and envelope.get("preparationFailure") is None,
                "NATIVE_QA3_MODEL_BOUNDARY")
        handoff = {"version": 1, "contractVersion": 3, "requestId": attempt["id"], "requestSHA256": request_sha,
                   "receiptSHA256": digest(raw_receipt), "receiptCanonicalization": QA3_CANONICALIZATION, "registration": binding,
                   "receipt": {"id": attempt["id"], "name": "cpu-replay-observation.json", "digest": digest(raw_receipt),
                               "size": len(raw_receipt), "path": "/home/agent/inputs/" + attempt["id"] + "/cpu-replay-observation.json"}}
        require(envelope.get("modelDispatched") is True and preparation.get("phase") == "qa_dispatching"
                and canonical(envelope.get("handoff")) == canonical(handoff)
                and canonical(attempt["context"]["manifest"].get("nativeQA3")) == canonical(handoff)
                and self.io.read_regular(self.runs / attempt["id"] / "home/inputs" / attempt["id"] / "cpu-replay-observation.json", 2 * 1024 * 1024) == raw_receipt,
                "NATIVE_QA3_HANDOFF_CHANGED")
        reports = [r for r in detail["artifacts"] if r["attempt_id"] == attempt["id"] and r["name"] == "qa-verification.json"]
        require(len(reports) == 1 and canonical(envelope.get("semanticReport")) == canonical(artifact_ref(reports[0])), "NATIVE_QA3_REPORT_BINDING")
        raw_report = self.artifact(reports[0]); semantic = None
        try:
            parsed = strict_json(raw_report); approved = qa3_semantic(parsed); semantic = parsed
            reason = None if approved else "NATIVE_QA3_SEMANTIC_REJECT"
            try: qa3_cpu_result(receipt, rows["candidate.py"]["digest"], rows["cpu-checks.py"]["digest"])
            except ValueError as error:
                reason = "NATIVE_QA3_CPU_EXECUTION_FAILED" if str(error) == "CPU_V3_EXECUTION_FAILED" else "NATIVE_QA3_CPU_RESULT_INVALID"
        except (ValueError, TypeError, KeyError):
            reason = "NATIVE_QA3_REPORT_INVALID"
        validation = {"version": 1, "contractVersion": 3, "status": "rejected" if reason else "matched", "code": reason,
                      "snapshotContractSHA256": snapshot["contractSHA256"], "source": snapshot["source"],
                      "requestSHA256": request_sha, "receiptSHA256": digest(raw_receipt), "receiptCanonicalization": QA3_CANONICALIZATION,
                      "handoff": handoff, "semanticReport": artifact_ref(reports[0]), "semanticDecision": semantic["decision"] if semantic else None,
                      "candidateExecutedByController": True, "gpuValidated": False, "performanceGainProven": False}
        require(canonical(envelope.get("validation")) == canonical(validation)
                and canonical(attempt["context"].get("nativeQA3Validation")) == canonical(validation), "NATIVE_QA3_GATE_PROOF")
        return {"request": metadata, "requestSHA256": request_sha, "receipt": receipt, "receiptSHA256": digest(raw_receipt),
                "registration": binding, "snapshot": snapshot, "handoff": handoff, "validation": validation,
                "semanticReportSchemaValid": semantic is not None, "modelDispatched": True, "preparationFailure": None}

    def qa3_attempt_chain(self, detail, *, allow_failed=False):
        oid = detail["occurrence"]["id"]
        ancestors = getattr(self, "_qa3_ancestry", ())
        require(oid not in ancestors and len(ancestors) < 8, "NATIVE_QA3_ANCESTRY")
        self._qa3_ancestry = (*ancestors, oid)
        try:
            return self._qa3_attempt_chain(detail, allow_failed=allow_failed)
        finally:
            self._qa3_ancestry = ancestors

    def _qa3_attempt_chain(self, detail, *, allow_failed=False):
        o = detail["occurrence"]; specs = o["definition"]["steps"]
        steps = {s["key"]: s for s in detail["steps"]}
        require(len(detail["steps"]) == len(steps) == 2 and all(steps[s["key"]]["definition"] == s for s in specs), "NATIVE_QA_STEPS")
        selected = {}
        for key in ("produce", "independent-qa"):
            actors = [a for a in detail["attempts"] if a["step_id"] == steps[key]["id"]]
            require(len(actors) == 1 and type(actors[0].get("number")) is int and actors[0]["number"] == 1
                    and type(steps[key].get("attempt")) is int and steps[key]["attempt"] == 1, "NATIVE_QA_ATTEMPT_SET")
            a = actors[0]; selected[key] = a
            no_model = (allow_failed and key == "independent-qa" and a.get("receipt", {}).get("kind") == "cpu-preparation-terminal"
                        and a["receipt"].get("modelDispatched") is False)
            require(UUID.fullmatch(a["id"]) and UUID.fullmatch(a["fence"]) and a.get("finished_at")
                    and (a.get("receipt", {}).get("stopped") is True or no_model) and a.get("model") == self.p["model"]
                    and a.get("account_id") == self.p["accountId"]
                    and (a["state"] == steps[key]["state"] == "completed" or allow_failed and key == "independent-qa"
                         and a["state"] == "failed" and steps[key]["state"] in {"failed", "attention"}), "NATIVE_QA_ACTOR_BINDING")
            self.artifact_budget([r for r in detail["artifacts"] if r["attempt_id"] == a["id"]])
            if not no_model: self.runtime.stopped(a["id"])
        producer, qa = selected["produce"], selected["independent-qa"]
        require(len(detail["attempts"]) == 2 and producer["id"] != qa["id"] and producer["fence"] != qa["fence"], "NATIVE_QA_ATTEMPT_SET")
        source = {"stepId": steps["produce"]["id"], "key": "produce", "attemptId": producer["id"], "attemptNumber": 1, "runFence": producer["fence"]}
        refs = sorted([{k: r[k] for k in ("id", "name", "digest", "size")} for r in detail["artifacts"] if r["attempt_id"] == producer["id"]], key=lambda r: (r["name"], r["id"]))
        require(0 < len(refs) <= 256 and len({r["name"] for r in refs}) == len(refs), "NATIVE_QA_INVENTORY")
        snapshot = {"version": 1, "contractSHA256": digest([2, "controller-cpu-replay", "produce", "qa-verification.json"]),
                    "occurrenceId": o["id"], "consumer": {"stepId": steps["independent-qa"]["id"], "key": "independent-qa",
                    "attemptId": qa["id"], "attemptNumber": 1, "runFence": qa["fence"]}, "source": source, "artifacts": refs}
        no_model = qa.get("receipt", {}).get("kind") == "cpu-preparation-terminal"
        persisted_snapshot = (qa["context"].get("cpuReplayPreparation", {}).get("snapshot") if no_model
                              else qa["context"]["manifest"].get("dependencyEvidence"))
        require(canonical(persisted_snapshot) == canonical(snapshot), "NATIVE_QA_DEPENDENCY_SNAPSHOT")
        for row in refs:
            require(re.fullmatch(r"[\w][\w.-]{0,160}", row["name"]), "ARTIFACT_NAME")
            raw = self.artifact(row, optional=row["name"] not in PRODUCER_FILES, allow_empty=row["name"] not in PRODUCER_FILES)
            if not no_model:
                require(self.io.read_regular(self.runs / qa["id"] / "home/inputs" / row["id"] / row["name"], self.artifact_limit(row, optional=row["name"] not in PRODUCER_FILES)) == raw, "NATIVE_QA_DEPENDENCY_HANDOFF")
        evidence = self.qa3_evidence(detail, producer, qa, snapshot)
        if not allow_failed: require(evidence["validation"] and evidence["validation"]["status"] == "matched", "QA_V3_REJECTED")
        self._qa3_evidence = evidence
        return selected, [{"attemptId": qa["id"], "number": 1, "fence": qa["fence"], "state": qa["state"],
                          "report": evidence["validation"]["semanticReport"] if evidence["validation"] else None, "snapshotSHA256": digest(snapshot),
                          "gateProofSHA256": digest(evidence["validation"] or evidence["preparationFailure"]), "receiptSHA256": evidence["receiptSHA256"]}]

    def diagnostic_binding(self, occurrence_id):
        detail = self.runtime.api("GET", "/api/cycle-runs/" + occurrence_id)
        v2 = self.qa_contract(detail["occurrence"]["definition"])
        result = {"candidateSHA256": None, "qaArtifactId": None, "qaSHA256": None}
        for step_key, name in (("produce", "candidate.py"), ("independent-qa", "qa-verification.json")):
            steps = [s for s in detail["steps"] if s["key"] == step_key]
            ids = {a["id"] for a in detail["attempts"] if len(steps) == 1 and a["step_id"] == steps[0]["id"]
                   and (not v2 or step_key != "independent-qa" or type(a.get("number")) is int
                        and type(steps[0].get("attempt")) is int and a["number"] == steps[0]["attempt"])}
            rows = [r for r in detail["artifacts"] if r["attempt_id"] in ids and r["name"] == name]
            if len(rows) == 1:
                self.artifact(rows[0])
                if name == "candidate.py": result["candidateSHA256"] = rows[0]["digest"]
                else: result.update(qaArtifactId=rows[0]["id"], qaSHA256=rows[0]["digest"])
        # This is only a cache read-set, never an audit/QA acceptance. Snapshot
        # again after a rejection so a moving attempt cannot enter the cache.
        self._diagnostic_identity = None
        try:
            self._diagnostic_identity = self.terminal_identity(occurrence_id, detail)
        except (ValueError, KeyError, TypeError, OSError):
            pass
        return result

    def terminal_identity(self, occurrence_id, detail=None):
        """Authenticated complete native detail and both dispatch receipts.

        Legacy attempts must be completed. QA3 also permits an authenticated
        terminal rejection, with current receipt and physical-stop verification.
        Hash the full response: changed steps, definitions, manifests, artifact
        inventories or attempt identity invalidate the historical diagnosis.
        Raw evidence is still verified by the consumer before native repair;
        this method does not grant CHECK or acceptance authority.
        """
        if detail is None:
            detail = self.runtime.api("GET", "/api/cycle-runs/" + occurrence_id)
        occurrence = detail["occurrence"]
        specs = occurrence["definition"].get("steps", [])
        if any((s.get("parameters") or {}).get("nativeQAContractVersion") == 3 for s in specs):
            require((self.qa_contract(occurrence["definition"]) or {}).get("version") == 2, "NATIVE_QA_CONTRACT")
            # A rejected QA3 report or a physically stopped CPU preparation is
            # terminal too. Reverify the protected receipt, all raw handoffs and
            # actor bindings; this extends diagnostic retention, never CHECK.
            require(occurrence["id"] == occurrence_id and occurrence["state"] in
                    {"completed", "attention", "failed"}, "CACHE_NATIVE_NOT_TERMINAL")
            selected, _ = self.qa3_attempt_chain(detail, allow_failed=True)
            evidence = self._qa3_evidence
            candidates = [r for r in detail["artifacts"] if r["attempt_id"] == selected["produce"]["id"]
                          and r["name"] == "candidate.py"]
            reports = [r for r in detail["artifacts"] if r["attempt_id"] == selected["independent-qa"]["id"]
                       and r["name"] == "qa-verification.json"]
            require(len(candidates) == 1 and len(reports) == (1 if evidence["modelDispatched"] else 0),
                    "CACHE_NATIVE_ARTIFACT")
            binding = {"candidateSHA256": candidates[0]["digest"],
                       "qaArtifactId": reports[0]["id"] if reports else None,
                       "qaSHA256": reports[0]["digest"] if reports else None}
            _, receipts = self.dispatch(occurrence)
            return {"detailSHA256": digest(detail), "dispatchSHA256": digest({k: digest(v) for k, v in receipts.items()}),
                    "binding": binding}
        require(occurrence["id"] == occurrence_id and occurrence["state"] == "completed", "CACHE_NATIVE_NOT_TERMINAL")
        v2 = self.qa_contract(occurrence["definition"])
        selected, _ = self.qa_attempt_chain(detail) if v2 else ({}, [])
        require(len(detail["steps"]) == 2 and (v2 or len(detail["attempts"]) == 2), "CACHE_NATIVE_ATTEMPTS")
        binding = {}
        for key, name in (("produce", "candidate.py"), ("independent-qa", "qa-verification.json")):
            steps = [s for s in detail["steps"] if s["key"] == key]
            require(len(steps) == 1 and steps[0]["state"] == "completed", "CACHE_NATIVE_STEP")
            attempts = [selected[key]] if v2 else [a for a in detail["attempts"] if a["step_id"] == steps[0]["id"]]
            require(len(attempts) == 1, "CACHE_NATIVE_ATTEMPTS")
            attempt = attempts[0]
            require(UUID.fullmatch(attempt["id"]) and UUID.fullmatch(attempt["fence"])
                    and attempt["number"] == (steps[0]["attempt"] if v2 else 1) and attempt["state"] == "completed"
                    and attempt.get("finished_at") and attempt.get("receipt", {}).get("stopped") is True,
                    "CACHE_NATIVE_ATTEMPT_BOUNDARY")
            manifest = attempt["context"]["manifest"]["cycleInput"]
            require(manifest["attemptId"] == attempt["id"] and manifest["runFence"] == attempt["fence"]
                    and SHA.fullmatch(manifest["digest"]), "CACHE_NATIVE_MANIFEST")
            rows = [r for r in detail["artifacts"] if r["attempt_id"] == attempt["id"] and r["name"] == name]
            require(len(rows) == 1 and UUID.fullmatch(rows[0]["id"]) and SHA.fullmatch(rows[0]["digest"]),
                    "CACHE_NATIVE_ARTIFACT")
            if key == "produce": binding["candidateSHA256"] = rows[0]["digest"]
            else: binding.update(qaArtifactId=rows[0]["id"], qaSHA256=rows[0]["digest"])
        _, receipts = self.dispatch(occurrence)
        return {"detailSHA256": digest(detail), "dispatchSHA256": digest({k: digest(v) for k, v in receipts.items()}),
                "binding": binding}

    def request_digest(self, receipt, body, *, allow_legacy_diagnostic=False):
        version = receipt.get("requestCanonicalization")
        require(version in (None, REQUEST_CANONICALIZATION), "DISPATCH_CANONICALIZATION")
        if version == REQUEST_CANONICALIZATION:
            require(canonical(body) == canonical(request_numbers_v1(body)), "DISPATCH_NUMBER_REPRESENTATION")
        if receipt["requestDigest"] == digest(body):
            return
        require(version is None and allow_legacy_diagnostic, "DISPATCH_DIGEST")
        # An old request embedded a complete authenticated QA report before the
        # JavaScript API normalized 1.0/0.0 to 1/0. Reconstruct that *exact* body
        # from its pinned source bytes; never rewrite a receipt or normalize its
        # stored hash. Projected reports have a different contract and cannot use
        # this legacy recovery path.
        experiment = body.get("input", {}).get("experiment", {})
        diagnostic = experiment.get("repairQADiagnostic")
        require(type(diagnostic) is dict and "reportProjection" not in diagnostic
                and type(diagnostic.get("version")) is int and diagnostic["version"] == 1
                and diagnostic.get("authority") == "prior_qa_diagnostic_only_not_current_acceptance"
                and diagnostic.get("bindingState") == "verified"
                and diagnostic.get("currentAcceptance") is False
                and type(diagnostic.get("report")) is dict, "DISPATCH_DIGEST")
        ref = diagnostic.get("artifact")
        require(type(ref) is dict and ref.get("name") == "qa-verification.json", "DISPATCH_DIGEST")
        original = strict_json(self.artifact(ref))
        require(type(original) is dict
                and same_json_numeric_representation(original, diagnostic["report"]), "DISPATCH_DIGEST")
        reconstructed = copy.deepcopy(body)
        reconstructed["input"]["experiment"]["repairQADiagnostic"]["report"] = original
        require(receipt["requestDigest"] == digest(reconstructed), "DISPATCH_DIGEST")

    def evidence_json(self, stage, name, files, artifacts):
        raw = files[stage][name]
        try:
            return strict_json(raw)
        except ValueError as error:
            if str(error) != "JSON_NONFINITE":
                raise
            rows = [row for row in artifacts[stage] if row["name"] == name]
            require(len(rows) == 1 and rows[0]["digest"] == digest(raw)
                    and rows[0]["size"] == len(raw), "ARTIFACT_BYTES_CHANGED")
            raise EvidenceJSONError(stage, rows[0]) from error

    def material_set(self, experiment, materials, problem, dispatch, candidate_sha):
        """Keep canonical inputs closed; authenticate one historical QA attachment.

        A projected repair may carry the original report for worker inspection.
        It is diagnostic data, never canonical source or current QA acceptance.
        The new candidate still passes every ordinary audit below this check.
        """
        expected = {"base-candidate.py", *self.p["boards"][problem]["materials"]}
        if set(materials) == expected:
            return
        require(set(materials) == expected | {"prior-qa-original.json"}, "MATERIAL_SET_CHANGED")
        target, diagnostic = experiment.get("repairTarget"), experiment.get("repairQADiagnostic")
        require(type(target) is dict and type(diagnostic) is dict
                and UUID.fullmatch(target.get("occurrenceId", ""))
                and dispatch.get("repairOf") == target["occurrenceId"]
                and dispatch.get("repairCandidateSHA256") == target.get("candidateSHA256") == candidate_sha,
                "REPAIR_DIAGNOSTIC_SCOPE")
        require(type(diagnostic.get("version")) is int and diagnostic["version"] == 1
                and diagnostic.get("authority") == "prior_qa_diagnostic_only_not_current_acceptance"
                and diagnostic.get("currentAcceptance") is False and diagnostic.get("bindingState") == "verified"
                and diagnostic.get("problem") == problem and diagnostic.get("occurrenceId") == target["occurrenceId"]
                and diagnostic.get("stepKey") == "independent-qa", "REPAIR_DIAGNOSTIC_BINDING")
        if diagnostic.get("nativeQAContractVersion") == 3:
            self.qa3_repair_material(experiment, materials, problem, target, diagnostic, candidate_sha)
            return
        projection, ref = diagnostic.get("reportProjection"), diagnostic.get("artifact")
        require(type(projection) is dict and type(projection.get("version")) is int and projection["version"] == 1
                and projection.get("kind") == "bounded_historical_qa" and projection.get("complete") is False
                and type(ref) is dict and projection.get("originalArtifact") == ref
                and ref.get("name") == "qa-verification.json" and ref.get("id") == target.get("qaArtifactId")
                and ref.get("digest") == target.get("qaSHA256"), "REPAIR_DIAGNOSTIC_PROJECTION")
        attached = [row for row in experiment["materials"] if row["name"] == "prior-qa-original.json"]
        require(len(attached) == 1 and attached[0].get("artifactId") == ref.get("id")
                and attached[0].get("attemptId") == diagnostic.get("attemptId")
                and type(attached[0].get("bytes")) is int and attached[0]["bytes"] == ref.get("size")
                and attached[0].get("sha256") == ref.get("digest"), "REPAIR_DIAGNOSTIC_MATERIAL_BINDING")
        raw = self.artifact(ref)
        require(raw == materials["prior-qa-original.json"], "REPAIR_DIAGNOSTIC_BYTES")
        original = strict_json(raw)
        base_sha = digest(materials["base-candidate.py"])
        require(type(original) is dict and original.get("candidateSHA256") == diagnostic.get("candidateSHA256") == candidate_sha
                and (original.get("sourceSHA256") or original.get("baseSourceSHA256")) == diagnostic.get("sourceSHA256") == base_sha,
                "REPAIR_DIAGNOSTIC_SOURCE_BINDING")
        require(projection.get("reportSHA256") == digest(original)
                and type(projection.get("reportBytes")) is int and projection["reportBytes"] == len(canonical(original))
                and projection.get("reportHashDomain") == "original_strict_json_canonical"
                and projection.get("selectedReportCanonicalization") == REQUEST_CANONICALIZATION
                and type(diagnostic.get("report")) is dict
                and set(diagnostic["report"]).issubset(original)
                and canonical(diagnostic["report"]) == canonical(request_numbers_v1({k: original[k] for k in diagnostic["report"]}))
                and projection.get("selectedReportSHA256") == digest(diagnostic["report"]), "REPAIR_DIAGNOSTIC_PROJECTION_BINDING")
        prior = self.runtime.api("GET", "/api/cycle-runs/" + target["occurrenceId"])
        prior_dispatch, _ = self.dispatch(prior["occurrence"])
        require(prior_dispatch["problem"] == problem
                and prior["occurrence"]["input"]["experiment"]["sourceArtifact"]["digest"] == base_sha,
                "REPAIR_DIAGNOSTIC_OWNER_BINDING")
        prior_attempt_ids = {a["id"] for a in prior["attempts"]}
        candidates = [row for row in prior["artifacts"] if row["name"] == "candidate.py" and row["attempt_id"] in prior_attempt_ids]
        require(bool(candidates) and {row["digest"] for row in candidates} == {candidate_sha}, "REPAIR_DIAGNOSTIC_CANDIDATE_BINDING")
        for row in candidates:
            self.artifact(row)
        rows = [row for row in prior["artifacts"] if row["id"] == ref["id"]]
        attempts = [a for a in prior["attempts"] if a["id"] == diagnostic.get("attemptId")]
        require(len(rows) == len(attempts) == 1 and rows[0].get("attempt_id") == diagnostic["attemptId"]
                and all(rows[0].get(k) == ref.get(k) for k in ("id", "name", "digest", "size")), "REPAIR_DIAGNOSTIC_ARTIFACT_OWNER")
        attempt = attempts[0]
        steps = [s for s in prior["steps"] if s["id"] == attempt["step_id"]]
        require(len(steps) == 1 and steps[0]["id"] == diagnostic.get("stepId")
                and steps[0]["key"] == "independent-qa" and steps[0].get("attempt") == attempt["number"]
                and steps[0].get("definition") in prior["occurrence"]["definition"]["steps"]
                and steps[0]["definition"].get("key") == "independent-qa"
                and steps[0]["definition"].get("kind") == "agent"
                and attempt["state"] in {"completed", "failed"} and attempt.get("finished_at")
                and attempt.get("fence") == diagnostic.get("runFence"), "REPAIR_DIAGNOSTIC_ATTEMPT_BINDING")
        self.runtime.stopped(attempt["id"])
        manifest = (attempt.get("context") or {}).get("manifest") or {}
        binding = manifest.get("cycleInput") or {}
        require(manifest.get("cycleId") == prior["occurrence"]["cycle_id"]
                and manifest.get("occurrenceId") == target["occurrenceId"] and manifest.get("step") == "independent-qa"
                and binding.get("attemptId") == attempt["id"] and binding.get("occurrenceId") == target["occurrenceId"]
                and binding.get("runFence") == attempt["fence"]
                and binding.get("digest") == diagnostic.get("cycleInputSHA256"), "REPAIR_DIAGNOSTIC_MANIFEST_BINDING")
        prior_raw = self.io.read_regular(self.runs / attempt["id"] / "home/inputs" / attempt["id"] / "cycle-input.json", 2 * 1024 * 1024)
        require(digest(prior_raw) == binding["digest"] and len(prior_raw) == binding.get("size")
                and strict_json(prior_raw) == prior["occurrence"]["input"], "REPAIR_DIAGNOSTIC_INPUT_BINDING")

    def qa3_repair_material(self, experiment, materials, problem, target, diagnostic, candidate_sha):
        """Preserve an invalid original QA artifact without parsing it as truth.

        Only the controller's authenticated REPORT_INVALID gate authorizes this
        bounded diagnostic form. The repaired child still needs a new replay,
        semantic QA and all ordinary CHECK gates. No old receipt is reused.
        """
        ref = diagnostic.get("artifact")
        require(type(diagnostic.get("nativeQAContractVersion")) is int
                and diagnostic["nativeQAContractVersion"] == 3 and diagnostic.get("report") is None
                and "report" in diagnostic and diagnostic.get("reportState") == "invalid_original_json_or_schema"
                and "reportProjection" not in diagnostic and type(ref) is dict
                and diagnostic.get("originalArtifact") == ref and ref.get("id") == target.get("qaArtifactId")
                and ref.get("digest") == target.get("qaSHA256") and ref.get("name") == "qa-verification.json",
                "REPAIR_QA3_DIAGNOSTIC")
        attachments = [r for r in experiment["materials"] if r["name"] == "prior-qa-original.json"]
        require(len(attachments) == 1 and attachments[0].get("artifactId") == ref["id"]
                and attachments[0].get("attemptId") == diagnostic.get("attemptId")
                and attachments[0].get("sha256") == ref["digest"] and type(attachments[0].get("bytes")) is int
                and attachments[0]["bytes"] == ref["size"] and self.artifact(ref) == materials["prior-qa-original.json"],
                "REPAIR_QA3_ORIGINAL_BYTES")
        prior = self.runtime.api("GET", "/api/cycle-runs/" + target["occurrenceId"])
        prior_dispatch, _ = self.dispatch(prior["occurrence"])
        require(prior["occurrence"]["id"] == target["occurrenceId"] and prior_dispatch["problem"] == problem
                and self.qa_contract(prior["occurrence"]["definition"]).get("version") == 2,
                "REPAIR_QA3_OWNER_BINDING")
        actors, _ = self.qa3_attempt_chain(prior, allow_failed=True)
        evidence = self._qa3_evidence; qa = actors["independent-qa"]
        require(evidence["modelDispatched"] is True and evidence["semanticReportSchemaValid"] is False
                and evidence["validation"]["status"] == "rejected" and evidence["validation"]["code"] == "NATIVE_QA3_REPORT_INVALID"
                and evidence["validation"]["semanticReport"] == ref
                and qa["id"] == diagnostic.get("attemptId") and qa["fence"] == diagnostic.get("runFence")
                and qa["step_id"] == diagnostic.get("stepId"), "REPAIR_QA3_GATE_BINDING")
        expected = {"version": 1, "contractVersion": 3, "requestSHA256": evidence["requestSHA256"],
                    "receiptSHA256": evidence["receiptSHA256"], "snapshotSHA256": digest(evidence["snapshot"]),
                    "validationSHA256": digest(evidence["validation"]), "registration": evidence["registration"]}
        require(canonical(diagnostic.get("controllerEvidence")) == canonical(expected), "REPAIR_QA3_CONTROLLER_BINDING")
        candidates = [r for r in prior["artifacts"] if r["attempt_id"] == actors["produce"]["id"] and r["name"] == "candidate.py"]
        base = prior["occurrence"]["input"]["experiment"]["sourceArtifact"]
        require(len(candidates) == 1 and candidates[0]["digest"] == candidate_sha == diagnostic.get("candidateSHA256")
                and self.artifact(candidates[0]) and base["digest"] == diagnostic.get("sourceSHA256") == digest(materials["base-candidate.py"])
                and self.artifact(base) == materials["base-candidate.py"]
                and qa["context"]["manifest"]["cycleInput"]["digest"] == diagnostic.get("cycleInputSHA256"),
                "REPAIR_QA3_SOURCE_BINDING")

    def dispatch(self, occurrence):
        directory = pathlib.Path(self.p["watcherStateDirectory"])
        state = watcher_state(self.p)
        entries = [e for e in state.get("experiments", []) if e.get("occurrenceId") == occurrence["id"]]
        require(len(entries) == 1 and entries[0]["cycleId"] == occurrence["cycle_id"], "NOT_WATCHDOG_DISPATCHED")
        source_key = occurrence.get("source_key", "")
        require(source_key.startswith("manual:" + self.p["watchId"] + "-experiment-") and source_key.endswith("-run"), "DISPATCH_SOURCE_KEY")
        key = source_key[len("manual:"):]
        run_raw = protected(directory / ("intent-" + digest(key) + ".json"))
        create_raw = protected(directory / ("intent-" + digest(key[:-4]) + ".json"))
        run, create = strict_json(run_raw), strict_json(create_raw)
        require(run.get("state") == create.get("state") == "recorded" and run.get("kind") == "run" and create.get("kind") == "create", "DISPATCH_RECEIPT_UNCONFIRMED")
        require(run["result"]["id"] == occurrence["id"] and run["result"]["cycle_id"] == occurrence["cycle_id"]
                and run["result"]["input"] == occurrence["input"] and run["requestKey"] == key
                and create["result"]["id"] == occurrence["cycle_id"]
                and create["result"]["definition"] == occurrence["definition"], "DISPATCH_CHANGED")
        expected_run = {"requestKey": key, "input": occurrence["input"]}
        expected_create = {"requestKey": key[:-4], "definition": occurrence["definition"]}
        self.request_digest(create, expected_create)
        self.request_digest(run, expected_run, allow_legacy_diagnostic=True)
        require(entries[0]["supervisorOccurrenceId"] == occurrence["input"]["supervisorOccurrenceId"], "SUPERVISOR_LINEAGE")
        return entries[0], {"run": run_raw, "create": create_raw}

    def audit(self, occurrence_id):
        self._completed_audit_detail_sha = None
        require(UUID.fullmatch(occurrence_id), "OCCURRENCE_ID")
        detail = self.runtime.api("GET", "/api/cycle-runs/" + occurrence_id)
        occurrence = detail["occurrence"]
        if (occurrence.get("id") == occurrence_id and occurrence.get("state") in {"failed", "attention"}
                and occurrence.get("definition", {}).get("steps", [{}])[-1].get("parameters", {}).get("nativeQAContractVersion") == 3):
            # A terminal typed rejection must reach supervision instead of
            # looking like a still-running native task. Re-prove its full
            # source provenance first; this branch can never return acceptance.
            selected, _ = self.qa_attempt_chain(detail, allow_failed=True)
            evidence = self._qa3_evidence
            rows = [r for r in detail["artifacts"] if r["attempt_id"] == selected["produce"]["id"] and r["name"] == "candidate.py"]
            require(len(rows) == 1, "NATIVE_QA_INVENTORY")
            problem = occurrence["input"]["experiment"]["problem"]
            self.source_origin(problem, rows[0]["digest"], {**rows[0], "occurrenceId": occurrence_id, "problem": problem}, ())
            code = (evidence["validation"] or evidence["preparationFailure"])["code"]
            require(code is not None, "NATIVE_QA3_TERMINAL_UNBOUND")
            raise ValueError({"NATIVE_QA3_SEMANTIC_REJECT": "QA_V3_SEMANTIC_REJECT",
                              "NATIVE_QA3_REPORT_INVALID": "QA_V3_REPORT_INVALID",
                              "NATIVE_QA3_CPU_EXECUTION_FAILED": "CPU_V3_EXECUTION_FAILED",
                              "NATIVE_QA3_CPU_RESULT_INVALID": "CPU_V3_RESULT_INVALID"}[code])
        require(occurrence["id"] == occurrence_id and occurrence["state"] == "completed", "NATIVE_NOT_COMPLETED")
        dispatch, receipts = self.dispatch(occurrence)
        problem = dispatch["problem"]
        require(problem in self.p["boards"], "BOARD_NOT_AUTHORIZED")
        inp, definition = occurrence["input"], occurrence["definition"]
        require(inp.get("watchId") == self.p["watchId"] and all(inp.get(k) is False
                for k in ("officialActionsAllowed", "productionActionsAllowed", "sourceWriteAllowed")), "WORKER_CAPABILITY_CHANGED")
        contract_v3 = definition.get("steps", [{}, {}])[-1].get("parameters", {}).get("nativeQAContractVersion") == 3
        require(definition.get("project") == "openkernel" and definition.get("allowedActions") == (["native.cpu-replay.observe"] if contract_v3 else [])
                and definition.get("allowedAccounts") == [self.p["accountId"]], "NATIVE_DEFINITION_SCOPE")
        specs = definition["steps"]
        v2 = self.qa_contract(definition)
        require(len(specs) == 2 and [s["key"] for s in specs] == ["produce", "independent-qa"]
                and specs[0].get("dependsOn") == [] and specs[1].get("dependsOn") == ["produce"], "NATIVE_DAG_CHANGED")
        for s in specs:
            require(s.get("kind") == "agent" and s.get("readOnly") is True and s.get("workspace") == self.p["workspace"]
                    and s.get("model") == self.p["model"] and s.get("accountId") == self.p["accountId"]
                    and s.get("maxAttempts") == (specs[1]["maxAttempts"] if v2 and s["key"] == "independent-qa" else 1)
                    and not s.get("sourceStep"), "NATIVE_STEP_SCOPE")
        selected, qa_history = self.qa_attempt_chain(detail) if v2 else ({}, [])
        controller_evidence = self._qa3_evidence if contract_v3 else None
        require(len(detail["steps"]) == 2 and (v2 or len(detail["attempts"]) == 2), "NATIVE_ATTEMPT_SET")
        frozen, identities, files, artifacts = {}, {}, {}, {}
        for key, required in (("produce", PRODUCER_FILES), ("independent-qa", QA_FILES)):
            steps = [s for s in detail["steps"] if s["key"] == key]
            require(len(steps) == 1 and steps[0]["state"] == "completed", "STEP_NOT_COMPLETED")
            require(steps[0].get("definition") == next(s for s in specs if s["key"] == key), "STEP_DEFINITION_CHANGED")
            attempts = [selected[key]] if v2 else [a for a in detail["attempts"] if a["step_id"] == steps[0]["id"]]
            require(len(attempts) == 1, "ATTEMPT_SET")
            a = attempts[0]
            require(UUID.fullmatch(a["id"]) and UUID.fullmatch(a["fence"]) and a["number"] == (steps[0]["attempt"] if v2 else 1)
                    and a["state"] == "completed" and a.get("finished_at") and a.get("receipt", {}).get("stopped") is True,
                    "ATTEMPT_NOT_STOPPED")
            require(a.get("model") == self.p["model"] and a.get("account_id") == self.p["accountId"], "ATTEMPT_MODEL_SCOPE")
            self.runtime.stopped(a["id"])
            home = self.runs / a["id"] / "home"
            raw = self.io.read_regular(home / "inputs" / a["id"] / "cycle-input.json", 2 * 1024 * 1024)
            manifest = a["context"]["manifest"]["cycleInput"]
            require(manifest["attemptId"] == a["id"] and manifest["runFence"] == a["fence"]
                    and manifest["digest"] == digest(raw) and manifest["size"] == len(raw)
                    and strict_json(raw) == inp, "NATIVE_INPUT_CHANGED")
            rows = [r for r in detail["artifacts"] if r["attempt_id"] == a["id"]]
            require(len({r["name"] for r in rows}) == len(rows) and required.issubset({r["name"] for r in rows}), "NATIVE_ARTIFACT_SET")
            self.artifact_budget(rows)
            files[key], artifacts[key] = {}, rows
            for row in rows:
                require(re.fullmatch(r"[\w][\w.-]{0,160}", row["name"]), "ARTIFACT_NAME")
                data = self.artifact(row, allow_empty=row["name"] not in required, optional=row["name"] not in required)
                files[key][row["name"]] = data
                frozen[key + "--" + row["name"]] = data
            transcript = self.io.read_regular(home / "sessions" / "--workspace--" / a["id"] / "session.v3.jsonl", 24 * 1024 * 1024)
            identities[key] = {"attemptId": a["id"], "fence": a["fence"], "finishedAt": a["finished_at"],
                               "inputSHA256": digest(raw), "transcriptSHA256": digest(transcript)}
            files[key]["transcript"] = transcript
        require(identities["produce"]["attemptId"] != identities["independent-qa"]["attemptId"], "QA_NOT_INDEPENDENT")
        qa_home = self.runs / identities["independent-qa"]["attemptId"] / "home"
        for row in artifacts["produce"]:
            raw = self.io.read_regular(qa_home / "inputs" / row["id"] / row["name"],
                                      self.artifact_limit(row, optional=row["name"] not in PRODUCER_FILES))
            require(raw == files["produce"][row["name"]], "QA_PREDECESSOR_CHANGED")
        experiment = inp["experiment"]
        require(experiment["problem"] == problem and isinstance(experiment["hypothesis"], str), "EXPERIMENT_SCOPE")
        materials = {}
        for row in experiment["materials"]:
            require(row["name"] not in materials and re.fullmatch(r"[\w][\w.-]{0,160}", row["name"]), "MATERIAL_NAME")
            raw = base64.b64decode(row["base64"], validate=True)
            require(len(raw) == row["bytes"] and digest(raw) == row["sha256"], "MATERIAL_CHANGED")
            materials[row["name"]] = raw
            frozen["material--" + row["name"]] = raw
        contract = self.p["boards"][problem]
        self.material_set(experiment, materials, problem, dispatch, digest(files["produce"]["candidate.py"]))
        for name, sha in contract["materials"].items():
            require(digest(materials[name]) == sha, "CANONICAL_MATERIAL_CHANGED")
        base_sha = digest(materials["base-candidate.py"])
        source_artifact = experiment["sourceArtifact"]
        require(source_artifact["digest"] == base_sha and self.artifact(source_artifact) == materials["base-candidate.py"], "BASE_SOURCE_CHANGED")
        provenance = self.source_provenance(problem, base_sha, source_artifact)
        for i, ref in enumerate(provenance):
            frozen["provenance--" + str(i) + ".json"] = pinned(ref)
        producer = files["produce"]
        candidate_sha, checker_sha, result_sha = (digest(producer[n]) for n in ("candidate.py", "cpu-checks.py", "cpu-result.json"))
        require(candidate_sha != base_sha and bool(producer["implementation.patch"].strip()), "NO_CANDIDATE_CHANGE")
        require(apply_single_file_patch(materials["base-candidate.py"], producer["implementation.patch"]) == producer["candidate.py"], "PATCH_CANDIDATE_MISMATCH")
        source_screen(producer["candidate.py"])
        require(kernel_code_structure(materials["base-candidate.py"]) != kernel_code_structure(producer["candidate.py"]),
                "NO_EXECUTABLE_CHANGE")
        qa = self.evidence_json("independent-qa", "qa-verification.json", files, artifacts)
        if contract_v3:
            verification = self.evidence_json("produce", "verification.json", files, artifacts)
            require(verification.get("passed") is True and verification.get("scopePreserved") is True
                    and verification.get("sourceSHA256") == base_sha and verification.get("candidateSHA256") == candidate_sha
                    and all(verification.get(k) is False for k in ("gpuValidated", "officialActionsPerformed"))
                    and verification.get("performanceGainProven", False) is False, "PRODUCER_V3_VERIFICATION")
            original_cpu = self.evidence_json("produce", "cpu-result.json", files, artifacts)
            require(type(original_cpu) is dict and type(original_cpu.get("passed")) is bool
                    and original_cpu.get("candidateSHA256") == candidate_sha and original_cpu.get("checkerSHA256") == checker_sha,
                    "CPU_V3_PRODUCER_BINDING")
            qa3_semantic(qa, approval=True)
            qa3_cpu_result(controller_evidence["receipt"], candidate_sha, checker_sha)
            commands = {"kind": "controller-cpu-replay", "requestSHA256": controller_evidence["requestSHA256"],
                        "receiptSHA256": controller_evidence["receiptSHA256"], "registration": controller_evidence["registration"]}
            frozen["controller-cpu-evidence.json"] = canonical(controller_evidence)
        else:
            semantic_qa(self.evidence_json("produce", "verification.json", files, artifacts), qa,
                        self.evidence_json("produce", "cpu-result.json", files, artifacts),
                        base_sha, candidate_sha, checker_sha, result_sha)
            commands = {key: replay_commands(files[key]["transcript"], checker_sha, candidate_sha) for key in files}
        qa_row = next(r for r in artifacts["independent-qa"] if r["name"] == "qa-verification.json")
        for name, raw in receipts.items():
            frozen["dispatch--" + name + ".json"] = raw
        report = {"version": 1, "authority": "delegated-policy-and-independent-native-QA", "operatorReviewedCandidate": False,
                  "watchId": self.p["watchId"], "occurrenceId": occurrence_id, "cycleId": occurrence["cycle_id"],
                  "problem": problem, "sourceSHA256": candidate_sha, "baseSHA256": base_sha, "identities": identities,
                  "qaArtifactId": qa_row["id"], "qaSHA256": qa_row["digest"], "dispatchReceiptSHA256": digest(receipts["run"]),
                  "cpuExecutionEvidence": commands, "hypothesis": experiment["hypothesis"], "provenance": provenance,
                  "gpuValidated": False, "performanceGainProven": False, "candidateExecutedByController": bool(contract_v3),
                  "files": {name: digest(raw) for name, raw in sorted(frozen.items())},
                  **({"nativeQAContractVersion": 3 if contract_v3 else 2, "qaAttemptHistory": qa_history} if v2 else {})}
        self._completed_audit_detail_sha = digest(detail)
        return report, frozen, producer["candidate.py"]

    def source_provenance(self, problem, source_sha, source_artifact=None, ancestry=()):
        seed = self.p["boards"][problem]["seedSources"].get(source_sha)
        if seed:
            return seed["provenance"]
        # Completed automatic derivatives retain the entire frozen ancestry.
        path = pathlib.Path(self.p["stateDirectory"]) / (problem + "-" + source_sha + "-audit.json")
        try:
            raw = protected(path, immutable=True)
        except FileNotFoundError:
            if self.p.get("nativeSourceOrigins", {}).get("enabled") is not True:
                raise
            return self.source_origin(problem, source_sha, source_artifact, ancestry)
        audit = strict_json(raw)
        require(audit.get("watchId") == self.p["watchId"] and audit.get("problem") == problem
                and audit.get("sourceSHA256") == source_sha, "BASE_PROVENANCE_SCOPE")
        return [{"path": str(path), "sha256": digest(raw)}] + audit["provenance"]

    def source_origin(self, problem, source_sha, source_artifact, ancestry):
        """Prove where source bytes came from; never promote a failed QA.

        A rejected experiment can be corrected from its own source. Its origin
        receipt is separate from an acceptance audit and cannot authorize CHECK.
        Every hop is reread, including the current stopped boundary and immutable
        dispatch/input/material bindings. No worker code executes in this lane.
        """
        try:
            return self._source_origin(problem, source_sha, source_artifact, ancestry)
        except FileNotFoundError as error:
            raise ValueError("SOURCE_ORIGIN_READ_UNAVAILABLE") from error
        except (KeyError, TypeError, RecursionError) as error:
            raise ValueError("SOURCE_ORIGIN_INCONSISTENT_SCHEMA") from error
        except ValueError as error:
            if str(error).startswith("SOURCE_ORIGIN_"):
                raise
            raise ValueError("SOURCE_ORIGIN_INCONSISTENT:" + str(error)[:100]) from error

    def _source_origin(self, problem, source_sha, source_artifact, ancestry):
        options = self.p["nativeSourceOrigins"]
        require(len(ancestry) < options["maxDepth"], "SOURCE_ORIGIN_DEPTH")
        require(isinstance(source_artifact, dict) and UUID.fullmatch(source_artifact.get("occurrenceId", ""))
                and source_artifact.get("problem") == problem and source_artifact.get("name") == "candidate.py"
                and source_artifact.get("digest") == source_sha, "SOURCE_ORIGIN_ABSENT")
        occurrence_id = source_artifact["occurrenceId"]
        require(occurrence_id not in ancestry, "SOURCE_ORIGIN_CYCLE")
        detail = self.runtime.api("GET", "/api/cycle-runs/" + occurrence_id)
        occurrence = detail["occurrence"]
        require(occurrence.get("id") == occurrence_id and occurrence.get("state") in {"completed", "attention", "failed"},
                "SOURCE_ORIGIN_OWNER_UNCONFIRMED")
        dispatch, receipts = self.dispatch(occurrence)
        inp, definition = occurrence["input"], occurrence["definition"]
        require(dispatch["problem"] == problem and inp.get("watchId") == self.p["watchId"]
                and all(inp.get(k) is False for k in ("officialActionsAllowed", "productionActionsAllowed", "sourceWriteAllowed")),
                "SOURCE_ORIGIN_CAPABILITY_SCOPE")
        contract_v3 = definition.get("steps", [{}, {}])[-1].get("parameters", {}).get("nativeQAContractVersion") == 3
        require(definition.get("project") == "openkernel" and definition.get("allowedActions") == (["native.cpu-replay.observe"] if contract_v3 else [])
                and definition.get("allowedAccounts") == [self.p["accountId"]], "SOURCE_ORIGIN_DEFINITION_SCOPE")
        specs = definition["steps"]
        v2 = self.qa_contract(definition)
        require(len(specs) == 2 and [s["key"] for s in specs] == ["produce", "independent-qa"]
                and specs[0].get("dependsOn") == [] and specs[1].get("dependsOn") == ["produce"], "SOURCE_ORIGIN_DAG")
        for spec in specs:
            require(spec.get("kind") == "agent" and spec.get("readOnly") is True
                    and spec.get("workspace") == self.p["workspace"] and spec.get("model") == self.p["model"]
                    and spec.get("accountId") == self.p["accountId"]
                    and spec.get("maxAttempts") == (specs[1]["maxAttempts"] if v2 and spec["key"] == "independent-qa" else 1)
                    and not spec.get("sourceStep"), "SOURCE_ORIGIN_EXECUTOR_SCOPE")
        selected, qa_history = self.qa_attempt_chain(detail, allow_failed=True) if v2 else ({}, [])
        controller_evidence = self._qa3_evidence if contract_v3 else None
        require(len(detail["steps"]) == 2 and (v2 or len(detail["attempts"]) in {1, 2}), "SOURCE_ORIGIN_ATTEMPT_SET")
        identities, attempts, frozen = {}, {}, {}
        for spec in specs:
            key = spec["key"]
            steps = [s for s in detail["steps"] if s["key"] == key]
            require(len(steps) == 1 and steps[0].get("definition") == spec, "SOURCE_ORIGIN_STEP_BINDING")
            rows = ([selected[key]] if key in selected else []) if v2 else [a for a in detail["attempts"] if a["step_id"] == steps[0]["id"]]
            if key == "independent-qa" and not rows:
                # The native engine leaves the downstream step queued when its
                # only producer fails. No attempted/unknown QA may be treated as
                # not_run, and a completed producer must wait for its QA.
                require(len(detail["attempts"]) == 1 and attempts["produce"]["state"] == "failed"
                        and steps[0]["state"] == "queued" and occurrence["state"] in {"attention", "failed"},
                        "SOURCE_ORIGIN_QA_NOT_RUN_UNPROVEN")
                identities[key] = {"state": "not_run", "stepId": steps[0]["id"], "stepState": steps[0]["state"], "attemptCount": 0}
                continue
            require(len(rows) == 1, "SOURCE_ORIGIN_ATTEMPT_SET")
            a = rows[0]
            self.artifact_budget([r for r in detail["artifacts"] if r["attempt_id"] == a["id"]])
            if contract_v3 and key == "independent-qa" and controller_evidence["modelDispatched"] is False:
                # The controller proved this CPU tool stopped. No QA model was
                # launched, so there is no harness stop or model artifact to
                # invent. Only the authenticated producer can supply a source.
                require(a["state"] == "failed" and controller_evidence["preparationFailure"] is not None,
                        "SOURCE_ORIGIN_PREPARATION_UNCONFIRMED")
                identities[key] = {"attemptId": a["id"], "fence": a["fence"], "state": "cpu_preparation_failed",
                                   "modelDispatched": False, "cpuReceiptSHA256": controller_evidence["receiptSHA256"]}
                frozen["controller-cpu-evidence.json"] = canonical(controller_evidence)
                continue
            require(UUID.fullmatch(a["id"]) and UUID.fullmatch(a["fence"]) and a["number"] == (steps[0]["attempt"] if v2 else 1)
                    and a.get("finished_at") and a.get("model") == self.p["model"]
                    and a.get("account_id") == self.p["accountId"], "SOURCE_ORIGIN_ATTEMPT_BINDING")
            require((key == "produce" and (a["state"] == steps[0]["state"] == "completed"
                        or a["state"] == "failed" and steps[0]["state"] in {"failed", "attention"}))
                    or (key == "independent-qa" and a["state"] in {"completed", "failed"}
                        and steps[0]["state"] in {"completed", "failed", "attention"}), "SOURCE_ORIGIN_OWNER_UNCONFIRMED")
            stopped = (a.get("receipt") or {}).get("stopped") is True
            stop_events = [e for e in detail.get("events", []) if e.get("kind") == "HARNESS_STOPPED"
                           and e.get("attempt_id") == a["id"] and e.get("occurrence_id") == occurrence_id
                           and e.get("payload", {}).get("runFence") == a["fence"]
                           and e["payload"].get("confirmed") is True
                           and e["payload"].get("unit") == "agentos-run-" + a["id"]
                           and e["payload"].get("proof", {}).get("stopped") is True
                           and e["payload"]["proof"].get("mainPid") == 0
                           and e["payload"]["proof"].get("populated") is False
                           and e["payload"]["proof"].get("activeState") == "inactive"]
            require(stopped or bool(stop_events), "SOURCE_ORIGIN_OWNER_UNCONFIRMED")
            self.runtime.stopped(a["id"])
            raw = self.io.read_regular(self.runs / a["id"] / "home/inputs" / a["id"] / "cycle-input.json", 2 * 1024 * 1024)
            manifest = a["context"]["manifest"]
            binding = manifest["cycleInput"]
            require(manifest.get("cycleId") == occurrence["cycle_id"] and manifest.get("occurrenceId") == occurrence_id
                    and manifest.get("step") == key and binding["attemptId"] == a["id"] and binding["runFence"] == a["fence"]
                    and binding["digest"] == digest(raw) and binding["size"] == len(raw) and strict_json(raw) == inp,
                    "SOURCE_ORIGIN_INPUT_CHANGED")
            attempts[key] = a
            identities[key] = {"attemptId": a["id"], "fence": a["fence"], "state": a["state"], "finishedAt": a["finished_at"],
                               "inputSHA256": digest(raw), "manifestSHA256": digest(manifest), "stopConfirmed": True}
            frozen[key + "-input.json"] = raw
            frozen[key + "-identity.json"] = canonical({**identities[key], "accountId": a["account_id"],
                "model": a["model"], "manifest": manifest, "receiptStopped": stopped, "stopEvents": stop_events})
        qa_attempt = attempts.get("independent-qa")
        require(not qa_attempt or attempts["produce"]["state"] == "completed"
                and attempts["produce"]["id"] != qa_attempt["id"], "SOURCE_ORIGIN_QA_IDENTITY")
        def artifact_for(key, name):
            matches = [r for r in detail["artifacts"] if r["attempt_id"] == attempts[key]["id"] and r["name"] == name]
            require(len(matches) == 1, "SOURCE_ORIGIN_ARTIFACT_SET")
            row = matches[0]
            return row, self.artifact(row)
        producer_row, candidate = artifact_for("produce", "candidate.py")
        require(digest(candidate) == source_sha, "SOURCE_ORIGIN_CANDIDATE_CHANGED")
        selected = [r for r in detail["artifacts"] if r["id"] == source_artifact["id"]]
        require(len(selected) == 1 and all(selected[0].get(k) == source_artifact.get(k) for k in ("id", "name", "digest", "size"))
                and selected[0]["attempt_id"] in {a["id"] for a in attempts.values()}
                and self.artifact(selected[0]) == candidate, "SOURCE_ORIGIN_ARTIFACT_ALIAS")
        # A QA-published copy is a transport alias, not independent source. The
        # producer artifact and exact handed-off bytes must both be established.
        if qa_attempt:
            qa_home = self.runs / qa_attempt["id"] / "home"
            handed = self.io.read_regular(qa_home / "inputs" / producer_row["id"] / "candidate.py", 1024 * 1024)
            require(handed == candidate, "SOURCE_ORIGIN_HANDOFF_CHANGED")
            qa_row, qa_raw = artifact_for("independent-qa", "qa-verification.json")
            qa = strict_json(qa_raw) if not contract_v3 or controller_evidence["semanticReportSchemaValid"] else None
        else:
            require(source_artifact["id"] == producer_row["id"], "SOURCE_ORIGIN_QA_ALIAS_WITHOUT_ATTEMPT")
        patch_row, patch = artifact_for("produce", "implementation.patch")
        experiment, materials = inp["experiment"], {}
        require(experiment.get("problem") == problem, "SOURCE_ORIGIN_BOARD_CHANGED")
        for row in experiment["materials"]:
            require(row["name"] not in materials, "SOURCE_ORIGIN_MATERIAL_DUPLICATE")
            raw = base64.b64decode(row["base64"], validate=True)
            require(len(raw) == row["bytes"] and digest(raw) == row["sha256"], "SOURCE_ORIGIN_MATERIAL_CHANGED")
            materials[row["name"]] = raw
        contract = self.p["boards"][problem]
        self.material_set(experiment, materials, problem, dispatch, source_sha)
        require(all(digest(materials[n]) == sha for n, sha in contract["materials"].items()), "SOURCE_ORIGIN_CANONICAL_CHANGED")
        base, parent = materials["base-candidate.py"], experiment["sourceArtifact"]
        base_sha = digest(base)
        require(parent["digest"] == base_sha and self.artifact(parent) == base, "SOURCE_ORIGIN_BASE_CHANGED")
        require(apply_single_file_patch(base, patch) == candidate, "SOURCE_ORIGIN_PATCH_CHANGED")
        if qa_attempt:
            if contract_v3:
                approved = qa3_semantic(qa) if qa is not None else None
                qa_evidence = {"artifactId": qa_row["id"], "sha256": qa_row["digest"], "state": qa_attempt["state"],
                               "semanticDecision": qa["decision"] if qa else None, "reportedPassed": None,
                               "reportSchemaValid": qa is not None,
                               "reportedApprovedForOfficialReview": approved, "controllerValidation": controller_evidence["validation"]}
                frozen["controller-cpu-evidence.json"] = canonical(controller_evidence)
            else:
                require(qa.get("sourceSHA256") == base_sha and qa.get("candidateSHA256") == source_sha
                        and type(qa.get("passed")) is bool and type(qa.get("approvedForOfficialReview")) is bool, "SOURCE_ORIGIN_QA_BINDING")
                qa_evidence = {"artifactId": qa_row["id"], "sha256": qa_row["digest"], "state": qa_attempt["state"],
                               "reportedPassed": qa["passed"], "reportedApprovedForOfficialReview": qa["approvedForOfficialReview"]}
            frozen["qa-verification.json"] = qa_raw
        else:
            qa_evidence = {"artifactId": None, "sha256": None, "state": "not_run",
                           "reportedPassed": None, "reportedApprovedForOfficialReview": None}
            if contract_v3:
                qa_evidence.update(state="cpu_preparation_failed", modelDispatched=False,
                                   preparationFailure=controller_evidence["preparationFailure"])
        producer_reports = []
        for name in ("verification.json", "cpu-result.json"):
            rows = [r for r in detail["artifacts"] if r["attempt_id"] == attempts["produce"]["id"] and r["name"] == name]
            require(len(rows) <= 1, "SOURCE_ORIGIN_REPORT_SET")
            if not rows:
                producer_reports.append({"name": name, "state": "not_published", "reportedPassed": None})
                continue
            row, raw = rows[0], self.artifact(rows[0])
            value = strict_json(raw)
            require(value.get("candidateSHA256") == source_sha and type(value.get("passed")) is bool,
                    "SOURCE_ORIGIN_REPORT_BINDING")
            if name == "verification.json":
                require(value.get("sourceSHA256") == base_sha, "SOURCE_ORIGIN_REPORT_BINDING")
            else:
                checker_row, _ = artifact_for("produce", "cpu-checks.py")
                require(value.get("checkerSHA256") == checker_row["digest"], "SOURCE_ORIGIN_REPORT_BINDING")
            producer_reports.append({"name": name, "state": "producer_report_only", "artifactId": row["id"],
                                     "sha256": row["digest"], "reportedPassed": value["passed"]})
            frozen[name] = raw
        provenance = self.source_provenance(problem, base_sha, parent, (*ancestry, occurrence_id))
        for i, ref in enumerate(provenance): frozen["ancestor-" + str(i) + ".json"] = pinned(ref)
        frozen.update({"candidate.py": candidate, "base-candidate.py": base, "implementation.patch": patch,
                       "dispatch-run.json": receipts["run"], "dispatch-create.json": receipts["create"]})
        report = {"version": 2, "authority": "authorized-native-source-origin", "qualityAccepted": False,
                  "gpuValidated": False, "candidateExecutedByController": bool(contract_v3), "watchId": self.p["watchId"],
                  "problem": problem, "sourceSHA256": source_sha, "baseSHA256": base_sha, "occurrenceId": occurrence_id,
                  "cycleId": occurrence["cycle_id"], "occurrenceState": occurrence["state"], "selectedArtifactId": source_artifact["id"],
                  "producerArtifactId": producer_row["id"], "selectedArtifactAttemptId": selected[0]["attempt_id"],
                  "identities": identities, "patchSHA256": patch_row["digest"], "dispatchReceiptSHA256": digest(receipts["run"]),
                  "qa": qa_evidence, "producerReports": producer_reports,
                  "provenance": provenance, "files": {k: digest(v) for k, v in sorted(frozen.items())},
                  **({"nativeQAContractVersion": 3 if contract_v3 else 2, "qaAttemptHistory": qa_history} if v2 else {})}
        directory = pathlib.Path(options["registryDirectory"])
        require(directory == pathlib.Path(self.p["stateDirectory"]) / "source-origins" and directory.resolve() == directory,
                "SOURCE_ORIGIN_REGISTRY_SCOPE")
        def protected_directory(path):
            require(path.is_dir() and path.resolve() == path, "SOURCE_ORIGIN_REGISTRY_SCOPE")
            for part in (path, *path.parents):
                info = part.lstat()
                require(info.st_uid == 0 and not info.st_mode & 0o022 and not stat.S_ISLNK(info.st_mode), "SOURCE_ORIGIN_REGISTRY_PROTECTION")
        protected_directory(directory.parent)
        directory.mkdir(mode=0o700, exist_ok=True)
        protected_directory(directory)
        # Preserve already frozen v1 origin receipts and their ancestry paths.
        directory = directory / "v2"
        directory.mkdir(mode=0o700, exist_ok=True)
        protected_directory(directory)
        folder = directory / (problem + "-" + source_sha + "-" + source_artifact["id"])
        folder.mkdir(mode=0o700, exist_ok=True)
        protected_directory(folder)
        for name, raw in frozen.items(): self.io.immutable_snapshot(folder / name, raw)
        path, raw = folder / "origin.json", canonical(report)
        self.io.immutable_snapshot(path, raw)
        require(protected(path, immutable=True) == raw, "SOURCE_ORIGIN_REGISTRY_CHANGED")
        return [{"path": str(path), "sha256": digest(raw)}, *provenance]


class OfficialLane:
    """Uses the pinned qualified CLI transport and the existing account ledger."""
    def __init__(self, policy, policy_sha, io, audit, official, clock=time.time, control_active=lambda: True):
        self.p, self.policy_sha, self.io, self.audit, self.official, self.clock = policy, policy_sha, io, audit, official, clock
        self.state = pathlib.Path(policy["stateDirectory"])
        self.legacy = pathlib.Path(policy["legacyLedger"])
        self.control_active = control_active

    def receipt_document(self, value, audit):
        return {"version": 1, "id": value["receipt"]["job_id"], "mode": "check", "problem": audit["problem"],
                "sourceSHA256": audit["sourceSHA256"], "occurrenceId": audit["occurrenceId"],
                "qaArtifactId": audit["qaArtifactId"], "qaSHA256": audit["qaSHA256"],
                "dispatchReceiptSHA256": audit["dispatchReceiptSHA256"], "policySHA256": self.policy_sha,
                "auditSHA256": digest(audit), "transportReceiptSHA256": digest(value), "transportReceipt": value}

    def registered_occurrences(self):
        path = pathlib.Path(self.p["registryPath"])
        if not path.exists(): return {}
        registry = strict_json(protected(path))
        require(registry.get("version") == 1 and registry.get("watchId") == self.p["watchId"], "REGISTRY_SCOPE")
        result = {}
        fields = ("id", "problem", "sourceSHA256", "occurrenceId", "qaArtifactId", "qaSHA256", "policySHA256",
                  "auditSHA256", "dispatchReceiptSHA256", "transportReceiptSHA256")
        for row in registry["jobs"]:
            require(row["problem"] in self.p["boards"] and JOB.fullmatch(row["id"]) and UUID.fullmatch(row["occurrenceId"]), "REGISTRY_SCOPE")
            raw = pinned({"path": row["receiptPath"], "sha256": row["receiptSHA256"]})
            receipt = strict_json(raw)
            require(all(receipt.get(k) == row.get(k) for k in fields) and receipt.get("mode") == "check", "REGISTRY_RECEIPT_BINDING")
            transport = receipt["transportReceipt"]
            require(digest(transport) == row["transportReceiptSHA256"] and transport.get("state") == "job_recorded"
                    and transport.get("mode") == "check" and transport.get("receipt", {}).get("job_id") == row["id"], "REGISTRY_TRANSPORT_BINDING")
            identity = transport["identity"]
            require(identity.get("problem") == row["problem"] and identity.get("sourceSha256") == row["sourceSHA256"]
                    and identity.get("occurrenceId") == row["occurrenceId"] and identity.get("policySHA256") == row["policySHA256"]
                    and identity.get("auditSHA256") == row["auditSHA256"], "REGISTRY_IDENTITY_BINDING")
            require(row["occurrenceId"] not in result, "REGISTRY_DUPLICATE_OCCURRENCE")
            result[row["occurrenceId"]] = row
        return result

    def records(self):
        rows = []
        for directory in (self.state, self.legacy):
            for path in directory.glob("*.json"):
                if path.name.endswith(("-check.json", "-scored.json")):
                    # Legacy files are compatibility vetoes/deduplication, not
                    # authority. Keep the qualified helper's regular/no-symlink
                    # reader; its historical parent need not be root-owned.
                    raw = self.io.read_regular(path, 1024 * 1024) if directory == self.legacy else protected(path)
                    rows.append(strict_json(raw))
        return rows

    def uncertain_reservations(self, previous):
        """An explicitly pinned, single POST stays unknown and consumes a slot.

        This is not effect reconciliation. An unlisted or changed unknown still
        blocks the account. Both the native authority and its legacy mirror must
        retain the exact reviewed bytes; a legacy-only record grants nothing.
        """
        unknown = [r for r in previous if r.get("state") != "job_recorded"]
        if not unknown: return []
        try:
            options = reservation_policy(self.p)
            require(options is not None, "RESERVATION_DISABLED")
            pins = {(r["problem"], r["sourceSHA256"]): r for r in options["intents"]}
            result = {}
            for row in unknown:
                identity = row.get("identity", {})
                key = (identity.get("problem"), identity.get("sourceSha256"))
                require(key in pins, "RESERVATION_UNKNOWN_NOT_PINNED")
                pin = pins[key]
                require(row.get("state") == "submission_outcome_unknown" and row.get("mode") == "check"
                        and row.get("automaticPostRetry") is False
                        and identity.get("origin") == "native-diagnostic-check-policy-v1"
                        and identity.get("operatorReviewedCandidate") is False
                        and identity.get("occurrenceId") == pin["occurrenceId"]
                        and type(row.get("intentEpoch")) in (int, float) and row["intentEpoch"] == pin["intentEpoch"]
                        and row["intentEpoch"] <= self.clock() + 5
                        and identity.get("policySHA256") == pin["originPolicy"]["sha256"]
                        and SHA.fullmatch(identity.get("auditSHA256", "")), "RESERVATION_INTENT_IDENTITY")
                filename = key[0] + "-" + key[1] + "-check.json"
                native = protected(self.state / filename)
                mirror = self.io.read_regular(self.legacy / filename, 1024 * 1024)
                require(native == mirror and digest(native) == pin["ledgerSHA256"]
                        and canonical(strict_json(native)) == canonical(row), "RESERVATION_MIRROR_PIN")
                old = strict_json(pinned(pin["originPolicy"]))
                validate_policy(old, row["intentEpoch"], old.get("adapterSHA256"))
                require(all(old.get(k) == self.p.get(k) for k in ("accountId", "model", "project", "workspace",
                        "stateDirectory", "legacyLedger", "accountLock", "artifactDirectory"))
                        and type(old.get("transportHelper")) is dict
                        and old["transportHelper"] == self.p.get("transportHelper")
                        and key[0] in old["boards"], "RESERVATION_ACCOUNT_SCOPE")
                stem = key[0] + "-" + key[1]
                audit_raw = protected(self.state / (stem + "-audit.json"), immutable=True)
                audit = strict_json(audit_raw)
                require(digest(audit_raw) == identity["auditSHA256"] and audit.get("problem") == key[0]
                        and audit.get("sourceSHA256") == key[1] and audit.get("occurrenceId") == pin["occurrenceId"],
                        "RESERVATION_AUDIT_BINDING")
                require(digest(protected(self.state / (stem + ".py"), immutable=True)) == key[1], "RESERVATION_SOURCE_BINDING")
                stdout = strict_json(protected(self.state / (stem + "-evidence") / "stdout.json", immutable=True))
                require(type(row.get("exitCode")) is int and row["exitCode"] == 1
                        and canonical(stdout) == canonical(row.get("receipt"))
                        and type(stdout) is dict and set(stdout) == {"error", "status"}
                        and type(stdout["status"]) is int and stdout["status"] == 429
                        and type(stdout["error"]) is str and bool(stdout["error"]), "RESERVATION_SINGLE_POST_RECEIPT")
                result[key] = {"problem": key[0], "sourceSHA256": key[1], "occurrenceId": pin["occurrenceId"],
                               "intentEpoch": pin["intentEpoch"], "ledgerSHA256": pin["ledgerSHA256"], "slots": 1}
            return [result[key] for key in sorted(result)]
        except (ValueError, KeyError, TypeError, OSError) as error:
            waiting = AccountPreflightWait("ACCOUNT_HAS_UNCERTAIN_EFFECT")
            waiting.reservation_reason = str(error)[:120]
            raise waiting from error

    def reservation_read_set(self, checks):
        if "uncertainCheckReservations" not in checks: return
        rows = self.records()
        reservations = self.uncertain_reservations(rows)
        if (digest(sorted(digest(r) for r in rows)) != checks["ledgerReadSetSHA256"]
                or reservations != checks["uncertainCheckReservations"]):
            raise AccountPreflightWait("ACCOUNT_HAS_UNCERTAIN_EFFECT")
        now = self.clock()
        if not (0 <= now - checks["reservationReadStartedAt"] <= 60
                and int(now // 86400) * 86400 == checks["quotaObservation"]["utcDayStart"]):
            raise PreflightQuotaError("CHECK_QUOTA_SYNC_PENDING")

    def reservation_jobs_window(self, response, now):
        # The current endpoint is the newest 50 jobs, not a full archive. A
        # saturated window entirely inside this UTC day cannot prove coverage;
        # preserve uncertainty rather than treating a missing row as rejection.
        if type(response) is dict and set(response) != {"jobs"}:
            raise PreflightQuotaError("CHECK_QUOTA_SYNC_PENDING")
        jobs = response if type(response) is list else response.get("jobs") if type(response) is dict else None
        if not (type(jobs) is list and len(jobs) <= 50 and all(type(j) is dict
                and JOB.fullmatch(j.get("job_id", "")) and type(j.get("problem")) is str
                and j.get("mode") in {"check", "run"} and type(j.get("submitted_at")) in (int, float)
                and math.isfinite(j["submitted_at"]) and 0 < j["submitted_at"] <= now + 5 for j in jobs)):
            raise PreflightQuotaError("CHECK_QUOTA_SYNC_PENDING")
        stamps = [j["submitted_at"] for j in jobs]
        if (len({j["job_id"] for j in jobs}) != len(jobs) or stamps != sorted(stamps, reverse=True)
                or len(jobs) == 50 and stamps[-1] >= int(now // 86400) * 86400):
            raise PreflightQuotaError("CHECK_QUOTA_SYNC_PENDING")
        return jobs

    def own_uncertainty_error(self, code, occurrence_id, audit):
        """Attach CPU-only provenance; the existing uncertain effect is intact.

        Called only after a successful native audit, inside the account lock.
        The marker does not authorize CHECK, reconcile an effect, or release WIP.
        """
        error = ValueError(code)
        try:
            options = reservation_policy(self.p)
            require(options is not None, "RESERVATION_DISABLED")
            reservations = self.uncertain_reservations(self.records())
            selected = [r for r in reservations if r["problem"] == audit["problem"]
                        and r["sourceSHA256"] == audit["sourceSHA256"]]
            require(len(selected) == 1, "RESERVATION_OWN_SOURCE")
            detail_sha = getattr(self.audit, "_completed_audit_detail_sha", None)
            require(type(detail_sha) is str and SHA.fullmatch(detail_sha), "RESERVATION_NATIVE_AUDIT")
            detail = self.audit.runtime.api("GET", "/api/cycle-runs/" + occurrence_id)
            require(digest(detail) == detail_sha and detail["occurrence"]["id"] == occurrence_id,
                    "RESERVATION_NATIVE_DETAIL_CHANGED")
            require(self.uncertain_reservations(self.records()) == reservations, "RESERVATION_SET_CHANGED")
            pin = next(r for r in options["intents"] if r["problem"] == selected[0]["problem"]
                       and r["sourceSHA256"] == selected[0]["sourceSHA256"])
            now = self.clock()
            require(type(now) in (int, float) and math.isfinite(now), "RESERVATION_TIME")
            error = ReservedUncertainEffect(code)
            error.uncertain_check_reservation = {"version": 1, "slots": 1, "verifiedAt": now,
                "detailSHA256": detail_sha, "policySHA256": self.policy_sha, "ownSourceUnretryable": True,
                "effectReconciled": False, "intent": copy.deepcopy(pin)}
        except (ValueError, KeyError, TypeError, OSError, StopIteration):
            pass  # Missing proof preserves the original, fully blocked outcome.
        return error

    def jobs_authority(self, team, day):
        options = jobs_observation_policy(self.p)
        scope = {key: self.p[key] for key in ("project", "accountId", "model", "stateDirectory",
                 "legacyLedger", "accountLock", "transportHelper")}
        return {"policySHA256": self.policy_sha, "adapterSHA256": self.p["adapterSHA256"],
                "transportScopeSHA256": digest(scope), "teamSHA256": digest(team), "utcDayStart": day,
                "indexPath": options["indexPath"], "bootstrap": options["bootstrap"]}

    def jobs_report(self, ref, authority, reanchor=False):
        raw = pinned(ref)
        require(len(raw) <= JOBS_OBSERVATION_MAX_BYTES, "JOBS_OBSERVATION_BYTES")
        report = strict_json(raw)
        require(type(report) is dict and report.get("kind") == "okc-jobs-observation-v1"
                and type(report.get("version")) is int and report["version"] == 1,
                "JOBS_OBSERVATION_REPORT")
        old = report["authority"]
        if reanchor:
            # Only an explicitly pinned bootstrap may cross a policy/code
            # boundary. Never adopt an incompatible existing mutable index.
            require(all(old[key] == authority[key] for key in ("transportScopeSHA256", "teamSHA256", "utcDayStart")),
                    "JOBS_OBSERVATION_REANCHOR_SCOPE")
        else:
            require(canonical(old) == canonical(authority), "JOBS_OBSERVATION_AUTHORITY")
        start, end = report["readStartedAt"], report["observedAt"]
        require(type(start) in (int, float) and type(end) in (int, float)
                and math.isfinite(start) and math.isfinite(end) and 0 <= end - start <= 60
                and int(start // 86400) * 86400 == int(end // 86400) * 86400 == authority["utcDayStart"],
                "JOBS_OBSERVATION_TIME")
        window = jobs_window_identity(report["response"], end)
        jobs = jobs_window_identity(report["jobs"], end, JOBS_OBSERVATION_MAX_IDS)
        require(canonical(window) == canonical(report["window"])
                and canonical(window) == canonical(jobs[:len(window)])
                and digest(report["meBefore"]["team"]) == digest(report["meAfter"]["team"]) == authority["teamSHA256"],
                "JOBS_OBSERVATION_BINDING")
        if report["coverage"] == "utc_baseline":
            require(report["previousReport"] is None and jobs_window_complete(window, authority["utcDayStart"])
                    and canonical(jobs) == canonical(window), "JOBS_OBSERVATION_BASELINE")
        else:
            require(report["coverage"] == "head_overlap" and type(report["previousReport"]) is dict
                    and SHA.fullmatch(report["previousReport"].get("sha256", "")), "JOBS_OBSERVATION_CHAIN")
        return report

    def jobs_bootstrap(self, authority):
        ref = jobs_observation_policy(self.p)["bootstrap"]
        raw = pinned(ref)
        require(len(raw) <= JOBS_OBSERVATION_MAX_BYTES, "JOBS_OBSERVATION_BYTES")
        seed = strict_json(raw)
        if seed.get("kind") == "okc-jobs-observation-v1":
            report = self.jobs_report(ref, authority, reanchor=True)
            require(report["observedAt"] <= self.clock(), "JOBS_OBSERVATION_FUTURE")
            return report["window"], report["jobs"], ref
        require(type(seed.get("version")) is int and seed["version"] == 1 and type(seed["startEpoch"]) in (int, float)
                and type(seed["endEpoch"]) in (int, float) and math.isfinite(seed["startEpoch"])
                and math.isfinite(seed["endEpoch"]) and 0 <= seed["endEpoch"] - seed["startEpoch"] <= 60
                and int(seed["startEpoch"] // 86400) * 86400 == authority["utcDayStart"]
                and int(seed["endEpoch"] // 86400) * 86400 == authority["utcDayStart"]
                and seed["endEpoch"] <= self.clock()
                and seed["postTransport"] == self.p["transportHelper"] and type(seed.get("POSTs")) is int and seed["POSTs"] == 0,
                "JOBS_OBSERVATION_BOOTSTRAP")
        captured_policy_raw = pinned(seed["refs"]["policy"])
        require(digest(captured_policy_raw) == seed["policySHA256"], "JOBS_OBSERVATION_BOOTSTRAP_POLICY")
        captured_policy = strict_json(captured_policy_raw)
        scope = {key: captured_policy[key] for key in ("project", "accountId", "model", "stateDirectory",
                 "legacyLedger", "accountLock", "transportHelper")}
        require(digest(scope) == authority["transportScopeSHA256"], "JOBS_OBSERVATION_BOOTSTRAP_SCOPE")
        before = strict_json(pinned(seed["refs"]["meBefore"]))
        after = strict_json(pinned(seed["refs"]["meAfter"]))
        require(digest(before["team"]) == digest(after["team"]) == seed["teamSHA256"] == authority["teamSHA256"],
                "JOBS_OBSERVATION_BOOTSTRAP_ACCOUNT")
        window = jobs_window_identity(strict_json(pinned(seed["refs"]["jobs"])), seed["endEpoch"])
        require(jobs_window_complete(window, authority["utcDayStart"]), "JOBS_OBSERVATION_BOOTSTRAP_COVERAGE")
        return window, window, ref

    def observe_jobs(self, response, me_before, read_started):
        """GET-only evidence, separate from the unchanged single-POST helper."""
        options = jobs_observation_policy(self.p)
        try:
            pinned(options["bootstrap"])
            me_after = self.official.fetch("me")
            now = self.clock(); day = int(now // 86400) * 86400
            require(type(me_before.get("team")) is str and 1 <= len(me_before["team"]) <= 200
                    and me_after.get("team") == me_before["team"], "JOBS_OBSERVATION_ACCOUNT")
            require(0 <= now - read_started <= 60 and int(read_started // 86400) * 86400 == day,
                    "JOBS_OBSERVATION_TIME")
            authority = self.jobs_authority(me_before["team"], day)
            window = jobs_window_identity(response, now)
            path = pathlib.Path(options["indexPath"])
            before_raw = protected(path, maximum=JOBS_OBSERVATION_MAX_BYTES) if path.exists() else None
            previous_ref, coverage = None, "utc_baseline"
            if before_raw is not None:
                pointer = strict_json(before_raw)
                require(type(pointer) is dict and set(pointer) == {"version", "report"}
                        and type(pointer["version"]) is int and pointer["version"] == 1, "JOBS_OBSERVATION_INDEX")
                old_raw = pinned(pointer["report"]); old = strict_json(old_raw)
                old_authority = old["authority"]
                # UTC rollover may begin a new proven baseline, but cannot
                # carry the previous day's index across account/code changes.
                same_scope = {k:v for k,v in old_authority.items() if k != "utcDayStart"}
                require(canonical(same_scope) == canonical({k:v for k,v in authority.items() if k != "utcDayStart"}),
                        "JOBS_OBSERVATION_AUTHORITY")
                if old_authority["utcDayStart"] == day:
                    old = self.jobs_report(pointer["report"], authority)
                    require(old["observedAt"] <= now, "JOBS_OBSERVATION_FUTURE")
                    jobs = jobs_head_overlap(old["window"], old["jobs"], window)
                    previous_ref, coverage = pointer["report"], "head_overlap"
                else:
                    require(old_authority["utcDayStart"] < day and jobs_window_complete(window, day), "JOBS_OBSERVATION_ROLLOVER")
                    jobs = window
            elif jobs_window_complete(window, day):
                jobs = window
            else:
                old_window, old_jobs, previous_ref = self.jobs_bootstrap(authority)
                jobs = jobs_head_overlap(old_window, old_jobs, window); coverage = "head_overlap"
            report = {"version": 1, "kind": "okc-jobs-observation-v1", "authority": authority,
                      "readStartedAt": read_started, "observedAt": now, "coverage": coverage,
                      "previousReport": previous_ref, "window": window, "jobs": jobs,
                      "meBefore": me_before, "meAfter": me_after, "response": response}
            raw = canonical(report)
            require(len(raw) <= JOBS_OBSERVATION_MAX_BYTES, "JOBS_OBSERVATION_BYTES")
            directory = self.state / "jobs-observation-reports"
            directory.mkdir(mode=0o700, exist_ok=True)
            require(directory.resolve() == directory and all(part.lstat().st_uid == 0
                    and not part.lstat().st_mode & 0o022 and not stat.S_ISLNK(part.lstat().st_mode)
                    for part in (directory, *directory.parents)), "JOBS_OBSERVATION_DIRECTORY")
            ref = {"path": str(directory / (digest(raw) + ".json")), "sha256": digest(raw)}
            current_raw = protected(path, maximum=JOBS_OBSERVATION_MAX_BYTES) if path.exists() else None
            require(current_raw == before_raw, "JOBS_OBSERVATION_CAS")
            self.io.immutable_snapshot(pathlib.Path(ref["path"]), raw)
            require((protected(path, maximum=JOBS_OBSERVATION_MAX_BYTES) if path.exists() else None) == before_raw,
                    "JOBS_OBSERVATION_CAS")
            self.io.atomic_record(path, {"version": 1, "report": ref})
            committed = protected(path)
            require(canonical(strict_json(committed)) == canonical({"version": 1, "report": ref}), "JOBS_OBSERVATION_CAS")
            return jobs, {"version": 1, "indexPath": str(path), "indexSHA256": digest(committed),
                          "report": ref, "teamSHA256": authority["teamSHA256"], "observedAt": now,
                          "readStartedAt": read_started, "utcDayStart": day}
        except (ValueError, KeyError, TypeError, OSError) as error:
            waiting = PreflightQuotaError("CHECK_QUOTA_SYNC_PENDING")
            waiting.observation_reason = str(error)[:120]
            raise waiting from error

    def jobs_observation_read_set(self, checks):
        proof = checks.get("jobsObservation")
        if proof is None: return
        try:
            now = self.clock(); options = jobs_observation_policy(self.p)
            require(proof["indexPath"] == options["indexPath"]
                    and digest(protected(proof["indexPath"])) == proof["indexSHA256"]
                    and canonical(strict_json(protected(proof["indexPath"]))["report"]) == canonical(proof["report"])
                    and 0 <= now - proof["readStartedAt"] <= 60
                    and proof["observedAt"] <= now
                    and int(now // 86400) * 86400 == proof["utcDayStart"], "JOBS_OBSERVATION_STALE_OR_CHANGED")
            raw = pinned(proof["report"]); report = strict_json(raw)
            pinned(options["bootstrap"])
            self.jobs_report(proof["report"], self.jobs_authority(report["meBefore"]["team"], proof["utcDayStart"]))
        except (ValueError, KeyError, TypeError, OSError) as error:
            waiting = PreflightQuotaError("CHECK_QUOTA_SYNC_PENDING")
            waiting.observation_reason = str(error)[:120]
            raise waiting from error

    def quota_observation(self, previous, jobs, problem, quota, now):
        """A lagging quota GET cannot erase CHECKs acknowledged by this lane.

        New intents retain this observation before POST. Existing records are
        never rewritten; a known local job missing from the GET waits for sync.
        """
        day = int(now // 86400) * 86400
        remote = set()
        for row in jobs:
            if row.get("problem") == problem and row.get("mode") == "check" and row["submitted_at"] >= day:
                require(JOB.fullmatch(row.get("job_id", "")) and row["submitted_at"] <= now + 5, "CHECK_JOBS_SCHEMA")
                remote.add(row["job_id"])
        local = {}
        for row in previous:
            if row.get("mode") != "check" or row.get("identity", {}).get("problem") != problem:
                continue
            epoch = row.get("intentEpoch")
            require(type(epoch) in (int, float) and math.isfinite(epoch) and epoch <= now + 5, "CHECK_QUOTA_LOCAL_SCHEMA")
            if epoch < day: continue
            job = row.get("receipt", {}).get("job_id")
            require(row.get("state") == "job_recorded" and JOB.fullmatch(job or ""), "CHECK_QUOTA_LOCAL_SCHEMA")
            # Native and legacy mirrors count once; contradictory mirrors wait.
            require(job not in local or canonical(local[job]) == canonical(row), "CHECK_QUOTA_LOCAL_CONFLICT")
            local[job] = row
        if set(local) - remote:
            raise PreflightQuotaError("CHECK_QUOTA_SYNC_PENDING")
        known = remote | set(local)
        consumed = max(quota["used"], len(known))
        for row in local.values():
            observation = row.get("quotaObservation")
            if observation is None: continue  # Legacy record; no invented baseline.
            require(type(observation) is dict and observation.get("version") == 1
                    and observation.get("problem") == problem and observation.get("utcDayStart") == day
                    and type(observation.get("checkedAt")) in (int, float)
                    and math.isfinite(observation["checkedAt"]) and day <= observation["checkedAt"] <= row["intentEpoch"]
                    and type(observation.get("used")) is int and observation["used"] >= 0
                    and type(observation.get("knownJobIds")) is list
                    and all(type(j) is str and JOB.fullmatch(j) for j in observation["knownJobIds"])
                    and len(set(observation["knownJobIds"])) == len(observation["knownJobIds"]), "CHECK_QUOTA_OBSERVATION_SCHEMA")
            # The observation predates this POST, so this job cannot already be
            # in it. Later jobs outside its membership add to the proven floor.
            require(row["receipt"]["job_id"] not in observation["knownJobIds"], "CHECK_QUOTA_OBSERVATION_SCHEMA")
            consumed = max(consumed, observation["used"] + len(known - set(observation["knownJobIds"])))
        if min(quota["left"], quota["per_day"] - consumed) <= self.p["minimumChecksRemaining"]:
            raise PreflightQuotaError("CHECK_QUOTA_SYNC_PENDING")
        return {"version": 1, "problem": problem, "utcDayStart": day, "checkedAt": now,
                "used": consumed, "knownJobIds": sorted(known)}

    def preflight(self, problem):
        # NativeAudit has already run. An account wait must not suppress CPU
        # evidence auditing, but needs no official GETs until reconciled.
        reservation_started = self.clock()
        self.uncertain_reservations(self.records())
        live = self.official.fetch("problems/" + problem)
        require(live.get("name") == problem, "PROBLEM_CHANGED")
        current = {**live["files"], "AGENTS.md": live["agents_md"], "TASK.md": live["task_md"]}
        require({n: digest(v.encode()) for n, v in current.items()} == self.p["boards"][problem]["officialFiles"], "OFFICIAL_CONTRACT_CHANGED")
        rules = self.official.fetch("llms.txt", public=True)
        metadata = self.official.fetch("rules")
        account_before = self.official.fetch("me")
        accepted = account_before.get("rules", {})
        require(digest(rules.encode()) == self.p["rulesEvidence"]["llmsSha256"]
                and metadata.get("version") == self.p["rulesEvidence"]["version"], "RULES_CHANGED")
        require(accepted.get("version") == metadata["version"] and type(accepted.get("accepted_at")) in (float, int)
                and math.isfinite(accepted["accepted_at"]) and accepted["accepted_at"] > 0, "HUMAN_TERMS_REQUIRED")
        quota = self.official.fetch("problems")
        require(quota.get("tracks", {}).get("kernels", {}).get("open") is True, "TRACK_CLOSED")
        rows = [r for r in quota.get("problems", []) if r.get("name") == problem]
        require(len(rows) == 1, "PROBLEM_UNAVAILABLE")
        q = rows[0]["limits"]["checks"]
        previous = self.records()
        reservations = self.uncertain_reservations(previous)
        require(all(type(q.get(k)) is int and q[k] >= 0 for k in ("left", "used", "per_day")), "CHECK_QUOTA_SCHEMA")
        if not (q["left"] > self.p["minimumChecksRemaining"] and q["used"] < q["per_day"]):
            raise PreflightQuotaError("CHECK_QUOTA")
        rate = re.search(r"Submissions are rate-limited \((\d+) a minute per token,\s*(\d+) a minute per account\)", rules)
        require(rate is not None, "RATE_POLICY_UNKNOWN")
        maximum = min(map(int, rate.groups()))
        require(0 < maximum <= 1000, "RATE_POLICY_INVALID")
        now = self.clock()
        require(sum(r.get("intentEpoch", 0) > now - 60 for r in previous) < maximum, "LOCAL_RATE_WAIT")
        jobs = self.official.fetch("jobs")
        jobs_proof = None
        if jobs_observation_policy(self.p) is not None:
            jobs, jobs_proof = self.observe_jobs(jobs, account_before, reservation_started)
            now = self.clock()
        else:
            jobs = (self.reservation_jobs_window(jobs, now) if reservation_policy(self.p) is not None
                    else jobs if isinstance(jobs, list) else jobs.get("jobs"))
        require(isinstance(jobs, list) and all(type(j.get("submitted_at")) in (int, float)
                and math.isfinite(j["submitted_at"]) for j in jobs), "JOBS_TIME_SCHEMA")
        require(sum(j["submitted_at"] > now - 60 for j in jobs) < maximum, "EXTERNAL_RATE_WAIT")
        observation = self.quota_observation([r for r in previous if r.get("state") == "job_recorded"], jobs, problem, q, now)
        # Keep used/floor free of reservations. Otherwise each acknowledged job
        # would capitalize the same uncertainty and charge it repeatedly.
        reserved = sum(r["slots"] for r in reservations if r["problem"] == problem)
        if min(q["left"], q["per_day"] - observation["used"]) - reserved <= self.p["minimumChecksRemaining"]:
            raise PreflightQuotaError("CHECK_QUOTA_SYNC_PENDING")
        result = {"checkedAt": now, "quota": q, "quotaObservation": observation,
                  "rules": self.p["rulesEvidence"], "effectiveRate": maximum}
        if jobs_proof is not None:
            result["jobsObservation"] = jobs_proof
            self.jobs_observation_read_set(result)
        if reservation_policy(self.p) is not None:
            if int(reservation_started // 86400) * 86400 != observation["utcDayStart"]:
                raise PreflightQuotaError("CHECK_QUOTA_SYNC_PENDING")
            result.update(uncertainCheckReservations=reservations, reservationReadStartedAt=reservation_started,
                          ledgerReadSetSHA256=digest(sorted(digest(r) for r in previous)))
            self.reservation_read_set(result)
        return result

    def register(self, receipt_path, value, audit):
        job = value.get("receipt", {}).get("job_id")
        require(value.get("state") == "job_recorded" and JOB.fullmatch(job or ""), "JOB_NOT_ACKNOWLEDGED")
        path = pathlib.Path(self.p["registryPath"])
        registry = strict_json(protected(path)) if path.exists() else {"version": 1, "watchId": self.p["watchId"], "jobs": []}
        require(registry.get("version") == 1 and registry.get("watchId") == self.p["watchId"] and isinstance(registry.get("jobs"), list), "REGISTRY_SCOPE")
        row = {"id": job, "problem": audit["problem"], "sourceSHA256": audit["sourceSHA256"],
               "occurrenceId": audit["occurrenceId"], "qaArtifactId": audit["qaArtifactId"], "qaSHA256": audit["qaSHA256"],
               "receiptPath": str(receipt_path), "receiptSHA256": digest(self.receipt_document(value, audit)),
               "policySHA256": self.policy_sha, "auditSHA256": digest(audit), "dispatchReceiptSHA256": audit["dispatchReceiptSHA256"]}
        row["transportReceiptSHA256"] = digest(value)
        prior = [r for r in registry["jobs"] if r.get("id") == job or (r.get("problem"), r.get("sourceSHA256")) == (row["problem"], row["sourceSHA256"])]
        require(not prior or prior == [row], "REGISTRY_CONFLICT")
        if not prior:
            registry["jobs"].append(row)
            self.io.atomic_record(path, registry)
        return row

    def run(self, occurrence_id, mode):
        require(mode in ("inspect", "check"), "CHECK_ONLY")
        require(mode != "check" or self.p["enabled"] is True, "POLICY_DISABLED")
        lock_path = pathlib.Path(self.p["accountLock"])
        protected(lock_path)
        fd = os.open(lock_path, os.O_RDWR | os.O_NOFOLLOW)
        with os.fdopen(fd, "r+") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            require(self.control_active(), "CONTROL_PAUSED")
            try:
                audit, files, source = self.audit.audit(occurrence_id)
            except ValueError as error:
                raise NativeDiagnosticError(error) from error
            problem, sha = audit["problem"], audit["sourceSHA256"]
            key = problem + "-" + sha
            intent, mirror = self.state / (key + "-check.json"), self.legacy / (key + "-check.json")
            if intent.exists():
                prior = strict_json(protected(intent))
                if (prior.get("identity", {}).get("occurrenceId") != occurrence_id
                        or prior["identity"].get("sourceSha256") != sha):
                    raise self.own_uncertainty_error("SOURCE_PREVIOUS_PROVENANCE", occurrence_id, audit)
                if prior.get("state") != "job_recorded":
                    raise self.own_uncertainty_error("UNCERTAIN_RECONCILE_NO_RESEND", occurrence_id, audit)
                frozen_receipt = self.state / (key + "-receipt.json")
                self.io.immutable_snapshot(frozen_receipt, canonical(self.receipt_document(prior, audit)))
                registered = self.register(frozen_receipt, prior, audit) if mode == "check" else None
                return {"state": "existing_job", "jobId": prior["receipt"]["job_id"], "postAttempted": False, "registered": registered}
            require(not mirror.exists(), "SOURCE_ALREADY_IN_SHARED_LEDGER")
            checks = self.preflight(problem)
            if mode == "inspect":
                return {"state": "eligible", "audit": audit, "preflight": checks, "postAttempted": False}
            fresh, fresh_files, fresh_source = self.audit.audit(occurrence_id)
            require(fresh == audit and fresh_files == files and fresh_source == source, "AUDIT_CHANGED_DURING_PREFLIGHT")
            require(self.clock() < self.p["expiresAt"], "POLICY_EXPIRED")
            require(self.control_active(), "CONTROL_PAUSED")
            self.reservation_read_set(checks)
            self.jobs_observation_read_set(checks)
            if int(self.clock() // 86400) * 86400 != checks["quotaObservation"]["utcDayStart"]:
                raise PreflightQuotaError("CHECK_QUOTA_SYNC_PENDING")
            snapshot = self.state / (key + ".py")
            self.io.immutable_snapshot(snapshot, source)
            self.io.immutable_snapshot(self.state / (key + "-audit.json"), canonical(audit))
            folder = self.state / (key + "-evidence")
            folder.mkdir(mode=0o700, exist_ok=True)
            for name, raw in files.items():
                self.io.immutable_snapshot(folder / name, raw)
            self.io.immutable_snapshot(folder / ("preflight-" + digest(checks) + ".json"), canonical(checks))
            identity = {"origin": "native-diagnostic-check-policy-v1", "problem": problem, "sourceSha256": sha,
                        "occurrenceId": occurrence_id, "policySHA256": self.policy_sha, "auditSHA256": digest(audit),
                        "operatorReviewedCandidate": False}
            intent_epoch = self.clock()
            self.reservation_read_set(checks)
            self.jobs_observation_read_set(checks)
            if int(intent_epoch // 86400) * 86400 != checks["quotaObservation"]["utcDayStart"]:
                raise PreflightQuotaError("CHECK_QUOTA_SYNC_PENDING")
            value = {"createdAt": dt.datetime.now(dt.timezone.utc).isoformat(), "intentEpoch": intent_epoch,
                     "mode": "check", "identity": identity, "state": "submission_intent", "automaticPostRetry": False,
                     "quotaObservation": checks["quotaObservation"]}
            if "uncertainCheckReservations" in checks:
                value["uncertainCheckReservations"] = checks["uncertainCheckReservations"]
            if "jobsObservation" in checks:
                value["jobsObservation"] = checks["jobsObservation"]
            self.io.immutable_snapshot(intent, canonical(value))
            self.io.immutable_snapshot(mirror, canonical(value))
            # The single effect boundary. All failures after this point are
            # uncertain until a confirmed job receipt is available; never resend.
            try:
                result = self.official.check(problem, snapshot)
                self.io.immutable_snapshot(folder / "stdout.json", result.stdout.encode())
                self.io.immutable_snapshot(folder / "stderr.txt", result.stderr.encode())
                value["exitCode"] = result.returncode
                receipt = strict_json(result.stdout)
                require(isinstance(receipt, dict), "CHECK_RECEIPT_SCHEMA")
                value["receipt"] = receipt
                value["state"] = "job_recorded" if JOB.fullmatch(receipt.get("job_id", "")) else "submission_outcome_unknown"
            except (ValueError, OSError, subprocess.TimeoutExpired, TypeError):
                value["state"] = "submission_outcome_unknown"
            self.io.atomic_record(intent, value)
            self.io.atomic_record(mirror, value)
            require(digest(self.io.read_regular(snapshot, immutable=True)) == sha, "SNAPSHOT_CHANGED_NO_RESEND")
            row = None
            if value["state"] == "job_recorded":
                receipt_path = self.state / (key + "-receipt.json")
                self.io.immutable_snapshot(receipt_path, canonical(self.receipt_document(value, audit)))
                row = self.register(receipt_path, value, audit)
            return {"state": value["state"], "jobId": (value.get("receipt") or {}).get("job_id"), "postAttempted": True,
                    "automaticPostRetry": False, "registryEntry": row}


def official_client(helper, boards):
    class Official(helper.Official):
        def fetch(self, path, public=False):
            if path in {"problems/" + b for b in boards}:
                argv = ["curl", "--fail", "--silent", "--show-error", "--max-time", "40", "--noproxy", "*",
                        "--proto", "=https", "--max-redirs", "0", "--header", "@-", "https://openkernelchallenge.dev/api/v1/" + path]
                result = subprocess.run(argv, input="Authorization: Bearer " + self.token + "\n", capture_output=True, text=True, timeout=45)
                require(result.returncode == 0 and len(result.stdout) <= 8 * 1024 * 1024, "OFFICIAL_GET_FAILED")
                return strict_json(result.stdout)
            return super().fetch(path, public)
    return Official()


def scan_selection(state, registered, boards, cursor=None):
    """Bound work, not history: fairly inspect unresolved owners before known jobs."""
    rows, seen = [], set()
    for row in state.get("experiments", []):
        occurrence = row.get("occurrenceId", "")
        if not UUID.fullmatch(occurrence) or row.get("problem") not in boards or occurrence in seen:
            continue
        seen.add(occurrence); rows.append(row)
    positions = {r["occurrenceId"]: i for i, r in enumerate(rows)}
    pending = [r for r in rows if r["occurrenceId"] not in registered]
    # The cursor is an ordering hint, never a receipt or authority. Keeping it
    # in the atomic status makes a crash retry the same bounded read work.
    after = positions.get(cursor, -1)
    pending = [r for r in pending if positions[r["occurrenceId"]] > after] + [
        r for r in pending if positions[r["occurrenceId"]] <= after]
    selected = pending[:100]
    selected += [r for r in reversed(rows) if r["occurrenceId"] in registered][:100 - len(selected)]
    return selected, {"version": 1, "candidateLimit": 100,
        "unregisteredCandidates": len(pending), "registeredCandidates": len(rows) - len(pending),
        "selectedCandidates": len(selected), "lastUnregisteredOccurrenceId": cursor,
        "order": "unregistered_round_robin_then_registered"}


def terminal_diagnostic_code(code):
    return isinstance(code, str) and (code.startswith(("CPU_", "QA_", "PATCH_"))
        or code in {"NO_CANDIDATE_CHANGE", "NO_EXECUTABLE_CHANGE"})


def diagnostic_effect(lane, row, registered):
    """Current authenticated effects always dominate historical rejection."""
    occurrence, problem, source = row["occurrenceId"], row["problem"], row["candidateSHA256"]
    matching = [job for oid, job in registered.items() if oid == occurrence
                or job["problem"] == problem and job["sourceSHA256"] == source]
    require(len({job["id"] for job in matching}) <= 1, "DIAGNOSTIC_REGISTERED_SOURCE_CONFLICT")
    job = matching[0] if matching else None
    if job:
        return {"occurrenceId": occurrence, "problem": problem, "candidateSHA256": job["sourceSHA256"],
                "qaArtifactId": job["qaArtifactId"], "qaSHA256": job["qaSHA256"], "state": "check_running",
                "jobId": job["id"], "postAttempted": False, "proposedRemediation": "observe_same_official_job"}
    effects = []
    for directory in (lane.state, lane.legacy):
        path = directory / (problem + "-" + source + "-check.json")
        if not path.exists():
            continue
        raw = lane.io.read_regular(path, 1024 * 1024) if directory == lane.legacy else protected(path)
        value = strict_json(raw)
        effects.append(value)
    if not effects:
        return None
    jobs = {(value.get("receipt") or {}).get("job_id") for value in effects}
    known = (all(value.get("state") == "job_recorded" for value in effects)
             and len(jobs) == 1 and JOB.fullmatch(next(iter(jobs)) or ""))
    return {k: row[k] for k in ["occurrenceId", "problem", "candidateSHA256", "qaArtifactId", "qaSHA256"]} | {
        "state": "check_running" if known else "uncertain_owned", "jobId": next(iter(jobs)) if known else None,
        "postAttempted": None, "proposedRemediation": "observe_same_official_job" if known else
        "reconcile_existing_intent_never_post_again"}


def diagnostic_cache_entry(row, lane, now):
    """Validate a cache record, without assigning fresh observation authority."""
    require(type(row) is dict, "DIAGNOSTIC_CACHE_SCHEMA")
    proof = row.get("diagnosticProof", {})
    require(row.get("state") == "rejected_repairable" and terminal_diagnostic_code(row.get("code"))
            and "jobId" in row and row["jobId"] is None and row.get("postAttempted") is False,
            "DIAGNOSTIC_CACHE_SCOPE")
    require(type(proof) is dict and type(proof.get("version")) is int and proof["version"] == 1
            and proof.get("verifyKind") == "native_identity"
            and proof.get("policySHA256") == lane.policy_sha
            and proof.get("adapterSHA256") == lane.p["adapterSHA256"], "DIAGNOSTIC_CACHE_AUTHORITY")
    require(all(type(proof.get(key)) in (int, float) and math.isfinite(proof[key])
                for key in ("observedAt", "verifiedAt"))
            and proof["observedAt"] <= proof["verifiedAt"] <= now + 5
            and 0 <= now - proof["observedAt"] <= TERMINAL_DIAGNOSTIC_TTL,
            "DIAGNOSTIC_CACHE_EXPIRED")
    require(all(isinstance(proof.get(key), str) and SHA.fullmatch(proof[key])
                for key in ("detailSHA256", "dispatchSHA256"))
            and isinstance(row.get("candidateSHA256"), str) and SHA.fullmatch(row["candidateSHA256"])
            and UUID.fullmatch(row.get("occurrenceId", ""))
            and (isinstance(row.get("qaSHA256"), str) and SHA.fullmatch(row["qaSHA256"])
                 and isinstance(row.get("qaArtifactId"), str) and UUID.fullmatch(row["qaArtifactId"])
                 or row.get("code") in {"CPU_V3_EXECUTION_FAILED", "CPU_V3_RESULT_INVALID"}
                 and "qaSHA256" in row and row["qaSHA256"] is None
                 and "qaArtifactId" in row and row["qaArtifactId"] is None)
            and row.get("problem") in lane.p["boards"], "DIAGNOSTIC_CACHE_BINDING")
    require(len(canonical(row)) <= 2048, "DIAGNOSTIC_CACHE_ENTRY_SIZE")
    return proof


def scan(lane, status_path, mode):
    """One bounded scan; terminal GET and supervisor continuation remain watcher-owned."""
    require(mode in ("inspect", "check"), "CHECK_ONLY")
    state = watcher_state(lane.p)
    outcomes = []
    registered = lane.registered_occurrences()
    cursor = None
    if pathlib.Path(status_path).exists():
        prior = strict_json(protected(status_path, maximum=1024 * 1024))
        if (prior.get("version") == 1 and prior.get("watchId") == lane.p["watchId"]
                and prior.get("policySHA256") == lane.policy_sha and prior.get("scanSelection") is not None):
            selection = prior["scanSelection"]
            require(isinstance(selection, dict) and selection.get("version") == 1
                    and (selection.get("lastUnregisteredOccurrenceId") is None
                         or isinstance(selection.get("lastUnregisteredOccurrenceId"), str)
                         and UUID.fullmatch(selection["lastUnregisteredOccurrenceId"])), "SCAN_CURSOR_SCOPE")
            cursor = selection.get("lastUnregisteredOccurrenceId")
    selected, selection = scan_selection(state, registered, lane.p["boards"], cursor)
    started = time.monotonic()
    budget_exhausted = False
    cache_path = lane.state / "terminal-diagnostics-v1.json"
    cache, verified, unavailable = {}, set(), []
    if cache_path.exists():
        document = strict_json(protected(cache_path, maximum=TERMINAL_DIAGNOSTIC_MAX_BYTES))
        require(type(document) is dict and type(document.get("version")) is int
                and document["version"] == 1 and document.get("watchId") == lane.p["watchId"]
                and isinstance(document.get("entries"), dict)
                and len(document["entries"]) <= TERMINAL_DIAGNOSTIC_LIMIT, "DIAGNOSTIC_CACHE_FILE")
        if (document.get("policySHA256") == lane.policy_sha
                and document.get("adapterSHA256") == lane.p["adapterSHA256"]):
            cache = document["entries"]
    owned = {r["occurrenceId"]: r for r in state.get("experiments", []) if r.get("problem") in lane.p["boards"]}
    # Revalidate historical diagnoses first. Cheap native GET/receipt identity
    # checks avoid repeating deep audits and official calls for rejected work.
    # Unverified records stay in the private index, never in fresh outcomes.
    for occurrence, cached in list(cache.items()):
        if time.monotonic() - started >= 45:
            budget_exhausted = True
            break
        try:
            proof = diagnostic_cache_entry(cached, lane, lane.clock())
            require(occurrence in owned and cached["occurrenceId"] == occurrence
                    and owned[occurrence]["problem"] == cached["problem"], "DIAGNOSTIC_CACHE_OWNER")
            effect = diagnostic_effect(lane, cached, registered)
            if effect:
                outcomes.append(effect); verified.add(occurrence); del cache[occurrence]
                continue
            current = lane.audit.terminal_identity(occurrence)
            require(all(current[key] == proof[key] for key in ["detailSHA256", "dispatchSHA256"])
                    and all(cached[key] == value for key, value in current["binding"].items()),
                    "DIAGNOSTIC_CACHE_IDENTITY_CHANGED")
            effect = diagnostic_effect(lane, cached, registered)
            if effect:
                outcomes.append(effect); verified.add(occurrence); del cache[occurrence]
                continue
            current_row = copy.deepcopy(cached)
            current_row["diagnosticProof"]["verifiedAt"] = lane.clock()
            cache[occurrence] = current_row; outcomes.append(current_row); verified.add(occurrence)
        except (ValueError, KeyError, TypeError, OSError, subprocess.TimeoutExpired) as error:
            del cache[occurrence]
            unavailable.append({"occurrenceId": occurrence, "code": str(error)[:120]})
            # Do not re-label the old diagnosis as current. The owner remains
            # conservative until a new ordinary audit supplies a fresh result.
            if occurrence in owned:
                cached = cached if type(cached) is dict else {}
                outcomes.append({"occurrenceId": occurrence, "problem": owned[occurrence]["problem"],
                    "candidateSHA256": cached.get("candidateSHA256"), "qaArtifactId": cached.get("qaArtifactId"),
                    "qaSHA256": cached.get("qaSHA256"), "state": "observation_wait", "jobId": None,
                    "postAttempted": None, "code": "CACHED_DIAGNOSTIC_UNAVAILABLE",
                    "proposedRemediation": "reaudit_native_identity_preserve_owner"})
    for row in selected:
        occurrence = row.get("occurrenceId", "")
        if not UUID.fullmatch(occurrence) or row.get("problem") not in lane.p["boards"]:
            continue
        if occurrence in verified:
            if occurrence not in registered:
                selection["lastUnregisteredOccurrenceId"] = occurrence
            continue
        if occurrence in registered:
            job = registered[occurrence]
            outcomes.append({"occurrenceId": occurrence, "problem": job["problem"], "candidateSHA256": job["sourceSHA256"],
                             "qaArtifactId": job["qaArtifactId"], "qaSHA256": job["qaSHA256"], "state": "check_running",
                             "jobId": job["id"], "postAttempted": False, "proposedRemediation": "observe_same_official_job"})
            continue
        # Never start another network/audit/POST sequence near the scan deadline.
        # Once an intent is created, finish that one bounded transport call.
        if time.monotonic() - started >= 45:
            budget_exhausted = True
            break
        binding = {"candidateSHA256": None, "qaArtifactId": None, "qaSHA256": None}
        try:
            binding = lane.audit.diagnostic_binding(occurrence)
            result = lane.run(occurrence, mode)
            state = ("check_running" if result["state"] in ("job_recorded", "existing_job") else
                     "uncertain_owned" if result["state"] == "submission_outcome_unknown" else result["state"])
            outcomes.append({"occurrenceId": occurrence, "problem": row["problem"], **binding, "state": state,
                             "jobId": result.get("jobId"), "postAttempted": result["postAttempted"],
                             "proposedRemediation": ("observe_same_official_job" if state == "check_running" else
                                "reconcile_existing_intent_never_post_again" if state == "uncertain_owned" else "await_controller_check")})
        except (ValueError, KeyError, OSError, TypeError, subprocess.TimeoutExpired) as error:
            code = str(error) if isinstance(error, ValueError) else type(error).__name__
            status, remediation, job_id = "controller_blocked", "repair_controller_without_replacing_candidate", None
            if isinstance(error, OSError) and getattr(error, "errno", None) not in {errno.EROFS, errno.EACCES, errno.EPERM, errno.ENOSPC, errno.EDQUOT}:
                status, remediation = "observation_wait", "retry_read_only_observation_preserve_owner"
            elif isinstance(error, subprocess.TimeoutExpired):
                status, remediation = "observation_wait", "retry_read_only_observation_preserve_owner"
            elif code == "SOURCE_ORIGIN_READ_UNAVAILABLE":
                status, remediation = "observation_wait", "retry_source_origin_observation_preserve_owner"
            elif code == "SOURCE_ORIGIN_OWNER_UNCONFIRMED":
                status, remediation = "await_native", "preserve_source_native_owner"
            elif code.startswith("SOURCE_ORIGIN_"):
                status, remediation = "controller_blocked", "reconcile_source_origin_without_replacing_candidate"
            elif isinstance(error, ValueError) and (code.startswith(("CPU_", "QA_", "PATCH_"))
                    or code in {"NO_CANDIDATE_CHANGE", "NO_EXECUTABLE_CHANGE", "SOURCE_REQUIRES_MANUAL_REVIEW"}):
                status, remediation = "rejected_repairable", "supervisor_review_evidence_and_plan_native_correction"
                if code in {"NO_CANDIDATE_CHANGE", "NO_EXECUTABLE_CHANGE"}:
                    remediation = "supervisor_new_hypothesis"
            if code == "NATIVE_NOT_COMPLETED": status, remediation = "await_native", "preserve_active_native_owner"
            elif code in {"CHECK_QUOTA", "CHECK_QUOTA_SYNC_PENDING", "POLICY_DAILY_BUDGET", "LOCAL_RATE_WAIT", "EXTERNAL_RATE_WAIT", "TRACK_CLOSED"}:
                status, remediation = "quota_wait", "await_official_quota_or_rate_window"
            elif "UNCERTAIN" in code or code in {"SOURCE_ALREADY_IN_SHARED_LEDGER", "SOURCE_PREVIOUS_PROVENANCE"}:
                status, remediation = "uncertain_owned", "reconcile_existing_intent_never_post_again"
            elif code == "CONTROL_PAUSED": status, remediation = "paused", "respect_operator_pause"
            if isinstance(error, AccountPreflightWait) and code == "ACCOUNT_HAS_UNCERTAIN_EFFECT":
                status, remediation = "account_wait", "preserve_candidate_wait_for_account_reconciliation_without_post"
            # Any durable effect dominates a later local/registry error. Do not
            # tell a supervisor to start replacement work for an owned POST.
            if binding["candidateSHA256"]:
                key = row["problem"] + "-" + binding["candidateSHA256"] + "-check.json"
                for directory in (lane.state, lane.legacy):
                    path = directory / key
                    if path.exists():
                        raw = lane.io.read_regular(path, 1024 * 1024) if directory == lane.legacy else protected(path)
                        prior = strict_json(raw)
                        job_id = (prior.get("receipt") or {}).get("job_id")
                        if prior.get("state") == "job_recorded" and JOB.fullmatch(job_id or ""):
                            status, remediation = "check_running", "observe_same_official_job"
                        else:
                            status, remediation = "uncertain_owned", "reconcile_existing_intent_never_post_again"
                        break
            outcome = {"occurrenceId": occurrence, "problem": row["problem"],
                             **binding, "state": status, "code": code[:120], "jobId": job_id,
                             "postAttempted": (False if job_id is None and (
                                 isinstance(error, PreflightQuotaError) and code in {"CHECK_QUOTA", "CHECK_QUOTA_SYNC_PENDING"} and status == "quota_wait"
                                 or isinstance(error, AccountPreflightWait) and code == "ACCOUNT_HAS_UNCERTAIN_EFFECT"
                                 and status == "account_wait") else None),
                             "proposedRemediation": remediation,
                             **({"accountWaitReason": error.reservation_reason} if getattr(error, "reservation_reason", None) else {}),
                             **({"jobsObservationReason": error.observation_reason} if getattr(error, "observation_reason", None) else {}),
                             **({"evidenceErrors": error.evidence_errors} if getattr(error, "evidence_errors", None) else {})}
            reservation = getattr(error, "uncertain_check_reservation", None)
            if (isinstance(error, ReservedUncertainEffect) and status == "uncertain_owned"
                    and code in {"UNCERTAIN_RECONCILE_NO_RESEND", "SOURCE_PREVIOUS_PROVENANCE"}
                    and job_id is None and outcome["postAttempted"] is None and type(reservation) is dict
                    and reservation["intent"]["problem"] == row["problem"]
                    and reservation["intent"]["sourceSHA256"] == binding["candidateSHA256"]):
                outcome["uncertainCheckReservation"] = reservation
            if (isinstance(error, NativeDiagnosticError) and status == "rejected_repairable"
                    and job_id is None and terminal_diagnostic_code(code)
                    and getattr(lane.audit, "_diagnostic_identity", None)):
                try:
                    effect = diagnostic_effect(lane, outcome, registered)
                    if effect:
                        outcome = effect
                    else:
                        old = lane.audit._diagnostic_identity
                        current = lane.audit.terminal_identity(occurrence)
                        require(current == old and all(outcome[k] == v for k, v in current["binding"].items()),
                                "DIAGNOSTIC_CHANGED_DURING_AUDIT")
                        effect = diagnostic_effect(lane, outcome, registered)
                        if effect:
                            outcome = effect
                        else:
                            now = lane.clock()
                            outcome.update(postAttempted=False, diagnosticProof={"version": 1, "verifyKind": "native_identity",
                                "observedAt": now, "verifiedAt": now, "policySHA256": lane.policy_sha,
                                "adapterSHA256": lane.p["adapterSHA256"],
                                **{key: current[key] for key in ("detailSHA256", "dispatchSHA256")}})
                            diagnostic_cache_entry(outcome, lane, now)
                            if occurrence in cache or len(cache) < TERMINAL_DIAGNOSTIC_LIMIT:
                                cache[occurrence] = copy.deepcopy(outcome)
                except (ValueError, KeyError, TypeError, OSError, subprocess.TimeoutExpired):
                    outcome.pop("diagnosticProof", None)
                    outcome["postAttempted"] = None
            outcomes.append(outcome)
        selection["lastUnregisteredOccurrenceId"] = occurrence
    outcomes = list({row["occurrenceId"]: row for row in outcomes}.values())
    cache_document = {"version": 1, "watchId": lane.p["watchId"], "policySHA256": lane.policy_sha,
                      "adapterSHA256": lane.p["adapterSHA256"], "entries": cache}
    require(len(canonical(cache_document)) <= TERMINAL_DIAGNOSTIC_MAX_BYTES, "DIAGNOSTIC_CACHE_SIZE")
    # Cache-before-status crash is safe: it only stores historical rejections,
    # and every later use repeats identity/effect verification before exposure.
    lane.io.atomic_record(cache_path, cache_document)
    doc = {"version": 1, "watchId": lane.p["watchId"], "observedAt": lane.clock(), "mode": mode,
           "policySHA256": lane.policy_sha, "policyExpiresAt": lane.p["expiresAt"],
           "controllerState": "active" if lane.p["enabled"] else "inspection_only_policy_disabled",
           "scanBudgetExhausted": budget_exhausted, "scanSelection": selection, "outcomes": outcomes,
           "terminalDiagnostics": {"entries": len(cache), "verified": len(verified), "unavailable": unavailable,
                                   "originalObservationTTLSeconds": TERMINAL_DIAGNOSTIC_TTL}}
    require(len(canonical(doc)) <= 262144, "DIAGNOSTIC_STATUS_SIZE")
    lane.io.atomic_record(pathlib.Path(status_path), doc)
    return doc


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["inspect", "check", "scan-inspect", "scan-check"])
    parser.add_argument("--policy", required=True)
    parser.add_argument("--policy-sha256", required=True)
    parser.add_argument("--occurrence")
    args = parser.parse_args()
    os.umask(0o077)
    policy_raw = pinned({"path": args.policy, "sha256": args.policy_sha256})
    raw_policy = strict_json(policy_raw)
    adapter_sha = digest(protected(pathlib.Path(__file__), immutable=True))
    try:
        policy = validate_policy(raw_policy, time.time(), adapter_sha, allow_disabled=args.mode in ("inspect", "scan-inspect"))
    except ValueError as error:
        if str(error) != "POLICY_EXPIRED_OR_UNBOUNDED":
            raise
        # Only publish expiration for an otherwise valid, pinned authority file.
        # This cannot renew authority or permit a CHECK. CPU supervision is not
        # paused; watcher consumes this explicit diagnostic instead of silence.
        policy = validate_policy(raw_policy, raw_policy["notBefore"], adapter_sha, allow_disabled=True)
        if time.time() >= policy["expiresAt"]:
            io = load_helper(policy["transportHelper"], "qualified_official_transport")
            io.protected(pathlib.Path(policy["statusPath"]).parent)
            status = {"version": 1, "watchId": policy["watchId"], "observedAt": time.time(), "mode": args.mode,
                      "policySHA256": args.policy_sha256, "policyExpiresAt": policy["expiresAt"],
                      "controllerState": "policy_expired", "outcomes": []}
            io.atomic_record(pathlib.Path(policy["statusPath"]), status)
            print(json.dumps(status)); return
        raise
    pinned(policy["watcherCode"])
    config = strict_json(pinned(policy["watcherConfig"], immutable=False))
    require(config.get("enabled") is True and config.get("watchId") == policy["watchId"]
            and config.get("controlCycleId") == policy["controlCycleId"], "WATCHER_SCOPE_CHANGED")
    io = load_helper(policy["transportHelper"], "qualified_official_transport")
    runtime = load_helper(policy["runtimeHelper"], "qualified_native_runtime")
    watcher = load_helper(policy["watcherCode"], "qualified_continuity_watcher")
    client = watcher.Client()
    def control_active():
        control = client.request("GET", "/api/cycles/" + policy["controlCycleId"])
        return (control["cycle"]["state"] == "active" and
                digest(control["cycle"]["definition"]) == config["controlDefinitionSHA256"])
    require(control_active(), "CONTROL_PAUSED")
    for name in ("stateDirectory", "legacyLedger", "watcherStateDirectory"):
        path = pathlib.Path(policy[name])
        require(path.is_dir() and path.resolve() == path, "STATE_DIRECTORY")
        if name != "legacyLedger": io.protected(path)
    io.protected(pathlib.Path(policy["registryPath"]).parent)
    io.protected(pathlib.Path(policy["statusPath"]).parent)
    audit = NativeAudit(policy, runtime, io)
    lane = OfficialLane(policy, args.policy_sha256, io, audit, official_client(io, policy["boards"]), control_active=control_active)
    if args.mode.startswith("scan-"):
        result = scan(lane, policy["statusPath"], args.mode[5:])
    else:
        require(args.occurrence is not None, "OCCURRENCE_REQUIRED")
        result = lane.run(args.occurrence, args.mode)
    print(json.dumps(result, ensure_ascii=False, allow_nan=False))


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, OSError, TypeError, subprocess.TimeoutExpired) as error:
        # Do not print provider output, credentials, source text or stack locals.
        print(json.dumps({"state": "blocked", "code": type(error).__name__ + ":" + str(error)[:160],
                          "automaticPostRetry": False, "instruction": "Inspect durable intent; never resend uncertain POST."}))
        raise SystemExit(1)
