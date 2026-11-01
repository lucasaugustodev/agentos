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


def require(ok, code):
    if not ok:
        raise ValueError(code)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(value if isinstance(value, bytes) else canonical(value)).hexdigest()


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
    return json.loads(raw, object_pairs_hook=pairs, parse_constant=bad_constant)


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


def source_screen(raw):
    """Conservative diagnostic-lane gate, not a proof of semantic legality."""
    tree = ast.parse(raw.decode("utf-8"))
    denied = {"os", "sys", "subprocess", "socket", "requests", "urllib", "http", "pathlib", "inspect", "importlib", "pickle", "marshal", "ctypes"}
    for node in ast.walk(tree):
        modules = ([n.name for n in node.names] if isinstance(node, ast.Import)
                   else [node.module or ""] if isinstance(node, ast.ImportFrom) else [])
        require(not any(m.split(".")[0] in denied or re.search(r"(^|\.)(reference|checks|evaluator|runner|static_checks)(\.|$)", m) for m in modules), "SOURCE_REQUIRES_MANUAL_REVIEW")
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            require(node.func.id not in {"exec", "eval", "compile", "__import__", "open", "getattr", "setattr", "globals", "locals"}, "SOURCE_REQUIRES_MANUAL_REVIEW")
        if isinstance(node, ast.Attribute):
            require(node.attr not in {"environ", "getenv", "system", "popen", "__dict__", "__globals__", "__subclasses__"}, "SOURCE_REQUIRES_MANUAL_REVIEW")


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
            require(all("detail" not in r or isinstance(r["detail"], str) for r in report), "CPU_CASES_NOT_REPLAYED")
        # QA is instructed to report name/flag rows. A textual diagnostic may
        # be omitted; all other fields, identities, order and flags stay exact.
        require(canonical([{k: v for k, v in r.items() if k != "detail"} for r in original]) ==
                canonical([{k: v for k, v in r.items() if k != "detail"} for r in rows]), "CPU_CASES_NOT_REPLAYED")
        require(all("detail" not in review or review["detail"] == source.get("detail")
                    for source, review in zip(original, rows)), "CPU_CASES_NOT_REPLAYED")


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

    def artifact(self, row, *, allow_empty=False):
        require(UUID.fullmatch(row.get("id", "")) and SHA.fullmatch(row.get("digest", "")), "ARTIFACT_IDENTITY")
        # Only the trusted audit loop can allow empty optional attachments.
        # Required acceptance evidence and source lookups remain nonempty.
        # Empty content still has to match its real size and SHA256 below.
        require(type(row.get("size")) is int and (0 < row["size"] <= 1024 * 1024
                or row["size"] == 0 and allow_empty is True), "ARTIFACT_SIZE")
        raw = self.io.read_regular(self.artifacts / row["digest"], 1024 * 1024)
        require(len(raw) == row["size"] and digest(raw) == row["digest"], "ARTIFACT_BYTES_CHANGED")
        return raw

    def diagnostic_binding(self, occurrence_id):
        detail = self.runtime.api("GET", "/api/cycle-runs/" + occurrence_id)
        result = {"candidateSHA256": None, "qaArtifactId": None, "qaSHA256": None}
        for step_key, name in (("produce", "candidate.py"), ("independent-qa", "qa-verification.json")):
            steps = [s for s in detail["steps"] if s["key"] == step_key]
            ids = {a["id"] for a in detail["attempts"] if len(steps) == 1 and a["step_id"] == steps[0]["id"]}
            rows = [r for r in detail["artifacts"] if r["attempt_id"] in ids and r["name"] == name]
            if len(rows) == 1:
                self.artifact(rows[0])
                if name == "candidate.py": result["candidateSHA256"] = rows[0]["digest"]
                else: result.update(qaArtifactId=rows[0]["id"], qaSHA256=rows[0]["digest"])
        return result

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
        require(run["requestDigest"] == digest(expected_run) and create["requestDigest"] == digest(expected_create), "DISPATCH_DIGEST")
        require(entries[0]["supervisorOccurrenceId"] == occurrence["input"]["supervisorOccurrenceId"], "SUPERVISOR_LINEAGE")
        return entries[0], {"run": run_raw, "create": create_raw}

    def audit(self, occurrence_id):
        require(UUID.fullmatch(occurrence_id), "OCCURRENCE_ID")
        detail = self.runtime.api("GET", "/api/cycle-runs/" + occurrence_id)
        occurrence = detail["occurrence"]
        require(occurrence["id"] == occurrence_id and occurrence["state"] == "completed", "NATIVE_NOT_COMPLETED")
        dispatch, receipts = self.dispatch(occurrence)
        problem = dispatch["problem"]
        require(problem in self.p["boards"], "BOARD_NOT_AUTHORIZED")
        inp, definition = occurrence["input"], occurrence["definition"]
        require(inp.get("watchId") == self.p["watchId"] and all(inp.get(k) is False
                for k in ("officialActionsAllowed", "productionActionsAllowed", "sourceWriteAllowed")), "WORKER_CAPABILITY_CHANGED")
        require(definition.get("project") == "openkernel" and definition.get("allowedActions") == []
                and definition.get("allowedAccounts") == [self.p["accountId"]], "NATIVE_DEFINITION_SCOPE")
        specs = definition["steps"]
        require(len(specs) == 2 and [s["key"] for s in specs] == ["produce", "independent-qa"]
                and specs[0].get("dependsOn") == [] and specs[1].get("dependsOn") == ["produce"], "NATIVE_DAG_CHANGED")
        for s in specs:
            require(s.get("kind") == "agent" and s.get("readOnly") is True and s.get("workspace") == self.p["workspace"]
                    and s.get("model") == self.p["model"] and s.get("accountId") == self.p["accountId"]
                    and s.get("maxAttempts") == 1 and not s.get("sourceStep"), "NATIVE_STEP_SCOPE")
        require(len(detail["attempts"]) == len(detail["steps"]) == 2, "NATIVE_ATTEMPT_SET")
        frozen, identities, files, artifacts = {}, {}, {}, {}
        for key, required in (("produce", PRODUCER_FILES), ("independent-qa", QA_FILES)):
            steps = [s for s in detail["steps"] if s["key"] == key]
            require(len(steps) == 1 and steps[0]["state"] == "completed", "STEP_NOT_COMPLETED")
            require(steps[0].get("definition") == next(s for s in specs if s["key"] == key), "STEP_DEFINITION_CHANGED")
            attempts = [a for a in detail["attempts"] if a["step_id"] == steps[0]["id"]]
            require(len(attempts) == 1, "ATTEMPT_SET")
            a = attempts[0]
            require(UUID.fullmatch(a["id"]) and UUID.fullmatch(a["fence"]) and a["number"] == 1
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
            files[key], artifacts[key] = {}, rows
            for row in rows:
                require(re.fullmatch(r"[\w][\w.-]{0,160}", row["name"]), "ARTIFACT_NAME")
                data = self.artifact(row, allow_empty=row["name"] not in required)
                files[key][row["name"]] = data
                frozen[key + "--" + row["name"]] = data
            transcript = self.io.read_regular(home / "sessions" / "--workspace--" / a["id"] / "session.v3.jsonl", 24 * 1024 * 1024)
            identities[key] = {"attemptId": a["id"], "fence": a["fence"], "finishedAt": a["finished_at"],
                               "inputSHA256": digest(raw), "transcriptSHA256": digest(transcript)}
            files[key]["transcript"] = transcript
        require(identities["produce"]["attemptId"] != identities["independent-qa"]["attemptId"], "QA_NOT_INDEPENDENT")
        qa_home = self.runs / identities["independent-qa"]["attemptId"] / "home"
        for row in artifacts["produce"]:
            raw = self.io.read_regular(qa_home / "inputs" / row["id"] / row["name"], 1024 * 1024)
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
        require(set(materials) == {"base-candidate.py", *contract["materials"]}, "MATERIAL_SET_CHANGED")
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
        qa = strict_json(files["independent-qa"]["qa-verification.json"])
        semantic_qa(strict_json(producer["verification.json"]), qa, strict_json(producer["cpu-result.json"]), base_sha, candidate_sha, checker_sha, result_sha)
        commands = {key: replay_commands(files[key]["transcript"], checker_sha, candidate_sha) for key in files}
        qa_row = next(r for r in artifacts["independent-qa"] if r["name"] == "qa-verification.json")
        for name, raw in receipts.items():
            frozen["dispatch--" + name + ".json"] = raw
        report = {"version": 1, "authority": "delegated-policy-and-independent-native-QA", "operatorReviewedCandidate": False,
                  "watchId": self.p["watchId"], "occurrenceId": occurrence_id, "cycleId": occurrence["cycle_id"],
                  "problem": problem, "sourceSHA256": candidate_sha, "baseSHA256": base_sha, "identities": identities,
                  "qaArtifactId": qa_row["id"], "qaSHA256": qa_row["digest"], "dispatchReceiptSHA256": digest(receipts["run"]),
                  "cpuExecutionEvidence": commands, "hypothesis": experiment["hypothesis"], "provenance": provenance,
                  "gpuValidated": False, "performanceGainProven": False, "candidateExecutedByController": False,
                  "files": {name: digest(raw) for name, raw in sorted(frozen.items())}}
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
        require(definition.get("project") == "openkernel" and definition.get("allowedActions") == []
                and definition.get("allowedAccounts") == [self.p["accountId"]], "SOURCE_ORIGIN_DEFINITION_SCOPE")
        specs = definition["steps"]
        require(len(specs) == 2 and [s["key"] for s in specs] == ["produce", "independent-qa"]
                and specs[0].get("dependsOn") == [] and specs[1].get("dependsOn") == ["produce"], "SOURCE_ORIGIN_DAG")
        for spec in specs:
            require(spec.get("kind") == "agent" and spec.get("readOnly") is True
                    and spec.get("workspace") == self.p["workspace"] and spec.get("model") == self.p["model"]
                    and spec.get("accountId") == self.p["accountId"] and spec.get("maxAttempts") == 1
                    and not spec.get("sourceStep"), "SOURCE_ORIGIN_EXECUTOR_SCOPE")
        require(len(detail["steps"]) == 2 and len(detail["attempts"]) in {1, 2}, "SOURCE_ORIGIN_ATTEMPT_SET")
        identities, attempts, frozen = {}, {}, {}
        for spec in specs:
            key = spec["key"]
            steps = [s for s in detail["steps"] if s["key"] == key]
            require(len(steps) == 1 and steps[0].get("definition") == spec, "SOURCE_ORIGIN_STEP_BINDING")
            rows = [a for a in detail["attempts"] if a["step_id"] == steps[0]["id"]]
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
            require(UUID.fullmatch(a["id"]) and UUID.fullmatch(a["fence"]) and a["number"] == 1
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
            qa = strict_json(qa_raw)
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
        require(set(materials) == {"base-candidate.py", *contract["materials"]}
                and all(digest(materials[n]) == sha for n, sha in contract["materials"].items()), "SOURCE_ORIGIN_CANONICAL_CHANGED")
        base, parent = materials["base-candidate.py"], experiment["sourceArtifact"]
        base_sha = digest(base)
        require(parent["digest"] == base_sha and self.artifact(parent) == base, "SOURCE_ORIGIN_BASE_CHANGED")
        require(apply_single_file_patch(base, patch) == candidate, "SOURCE_ORIGIN_PATCH_CHANGED")
        if qa_attempt:
            require(qa.get("sourceSHA256") == base_sha and qa.get("candidateSHA256") == source_sha
                    and type(qa.get("passed")) is bool and type(qa.get("approvedForOfficialReview")) is bool, "SOURCE_ORIGIN_QA_BINDING")
            qa_evidence = {"artifactId": qa_row["id"], "sha256": qa_row["digest"], "state": qa_attempt["state"],
                           "reportedPassed": qa["passed"], "reportedApprovedForOfficialReview": qa["approvedForOfficialReview"]}
            frozen["qa-verification.json"] = qa_raw
        else:
            qa_evidence = {"artifactId": None, "sha256": None, "state": "not_run",
                           "reportedPassed": None, "reportedApprovedForOfficialReview": None}
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
                  "gpuValidated": False, "candidateExecutedByController": False, "watchId": self.p["watchId"],
                  "problem": problem, "sourceSHA256": source_sha, "baseSHA256": base_sha, "occurrenceId": occurrence_id,
                  "cycleId": occurrence["cycle_id"], "occurrenceState": occurrence["state"], "selectedArtifactId": source_artifact["id"],
                  "producerArtifactId": producer_row["id"], "selectedArtifactAttemptId": selected[0]["attempt_id"],
                  "identities": identities, "patchSHA256": patch_row["digest"], "dispatchReceiptSHA256": digest(receipts["run"]),
                  "qa": qa_evidence, "producerReports": producer_reports,
                  "provenance": provenance, "files": {k: digest(v) for k, v in sorted(frozen.items())}}
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

    def preflight(self, problem):
        live = self.official.fetch("problems/" + problem)
        require(live.get("name") == problem, "PROBLEM_CHANGED")
        current = {**live["files"], "AGENTS.md": live["agents_md"], "TASK.md": live["task_md"]}
        require({n: digest(v.encode()) for n, v in current.items()} == self.p["boards"][problem]["officialFiles"], "OFFICIAL_CONTRACT_CHANGED")
        rules = self.official.fetch("llms.txt", public=True)
        metadata = self.official.fetch("rules")
        accepted = self.official.fetch("me").get("rules", {})
        require(digest(rules.encode()) == self.p["rulesEvidence"]["llmsSha256"]
                and metadata.get("version") == self.p["rulesEvidence"]["version"], "RULES_CHANGED")
        require(accepted.get("version") == metadata["version"] and type(accepted.get("accepted_at")) in (float, int)
                and math.isfinite(accepted["accepted_at"]) and accepted["accepted_at"] > 0, "HUMAN_TERMS_REQUIRED")
        quota = self.official.fetch("problems")
        require(quota.get("tracks", {}).get("kernels", {}).get("open") is True, "TRACK_CLOSED")
        rows = [r for r in quota.get("problems", []) if r.get("name") == problem]
        require(len(rows) == 1, "PROBLEM_UNAVAILABLE")
        q = rows[0]["limits"]["checks"]
        require(all(type(q.get(k)) is int and q[k] >= 0 for k in ("left", "used", "per_day"))
                and q["left"] > self.p["minimumChecksRemaining"] and q["used"] < q["per_day"], "CHECK_QUOTA")
        rate = re.search(r"Submissions are rate-limited \((\d+) a minute per token,\s*(\d+) a minute per account\)", rules)
        require(rate is not None, "RATE_POLICY_UNKNOWN")
        maximum = min(map(int, rate.groups()))
        require(0 < maximum <= 1000, "RATE_POLICY_INVALID")
        previous = self.records()
        require(all(r.get("state") == "job_recorded" for r in previous), "ACCOUNT_HAS_UNCERTAIN_EFFECT")
        now = self.clock()
        require(sum(r.get("intentEpoch", 0) > now - 60 for r in previous) < maximum, "LOCAL_RATE_WAIT")
        jobs = self.official.fetch("jobs")
        jobs = jobs if isinstance(jobs, list) else jobs.get("jobs")
        require(isinstance(jobs, list) and all(type(j.get("submitted_at")) in (int, float)
                and math.isfinite(j["submitted_at"]) for j in jobs), "JOBS_TIME_SCHEMA")
        require(sum(j["submitted_at"] > now - 60 for j in jobs) < maximum, "EXTERNAL_RATE_WAIT")
        return {"checkedAt": now, "quota": q, "rules": self.p["rulesEvidence"], "effectiveRate": maximum}

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
            audit, files, source = self.audit.audit(occurrence_id)
            problem, sha = audit["problem"], audit["sourceSHA256"]
            key = problem + "-" + sha
            intent, mirror = self.state / (key + "-check.json"), self.legacy / (key + "-check.json")
            if intent.exists():
                prior = strict_json(protected(intent))
                require(prior.get("identity", {}).get("occurrenceId") == occurrence_id
                        and prior["identity"].get("sourceSha256") == sha, "SOURCE_PREVIOUS_PROVENANCE")
                require(prior.get("state") == "job_recorded", "UNCERTAIN_RECONCILE_NO_RESEND")
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
            value = {"createdAt": dt.datetime.now(dt.timezone.utc).isoformat(), "intentEpoch": self.clock(),
                     "mode": "check", "identity": identity, "state": "submission_intent", "automaticPostRetry": False}
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


def scan(lane, status_path, mode):
    """One bounded scan; terminal GET and supervisor continuation remain watcher-owned."""
    require(mode in ("inspect", "check"), "CHECK_ONLY")
    state = watcher_state(lane.p)
    outcomes = []
    registered = lane.registered_occurrences()
    started = time.monotonic()
    budget_exhausted = False
    for row in reversed(state.get("experiments", [])[-100:]):
        occurrence = row.get("occurrenceId", "")
        if not UUID.fullmatch(occurrence) or row.get("problem") not in lane.p["boards"]:
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
            elif code in {"CHECK_QUOTA", "POLICY_DAILY_BUDGET", "LOCAL_RATE_WAIT", "EXTERNAL_RATE_WAIT", "TRACK_CLOSED"}:
                status, remediation = "quota_wait", "await_official_quota_or_rate_window"
            elif "UNCERTAIN" in code or code in {"SOURCE_ALREADY_IN_SHARED_LEDGER", "SOURCE_PREVIOUS_PROVENANCE"}:
                status, remediation = "uncertain_owned", "reconcile_existing_intent_never_post_again"
            elif code == "CONTROL_PAUSED": status, remediation = "paused", "respect_operator_pause"
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
            outcomes.append({"occurrenceId": occurrence, "problem": row["problem"],
                             **binding, "state": status, "code": code[:120], "jobId": job_id,
                             "postAttempted": None, "proposedRemediation": remediation})
    doc = {"version": 1, "watchId": lane.p["watchId"], "observedAt": lane.clock(), "mode": mode,
           "policySHA256": lane.policy_sha, "policyExpiresAt": lane.p["expiresAt"],
           "controllerState": "active" if lane.p["enabled"] else "inspection_only_policy_disabled",
           "scanBudgetExhausted": budget_exhausted, "outcomes": outcomes}
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
