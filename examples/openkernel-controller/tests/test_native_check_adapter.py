"""Server-only, isolated fixtures; no provider/network/production reads."""
import base64
import copy
import difflib
import errno
import importlib.util
import json
import os
import pathlib
import shutil
import subprocess
import tempfile
import types
import unittest
from unittest import mock

SPEC = importlib.util.spec_from_file_location("native_adapter", pathlib.Path(__file__).parents[1] / "tools/native_check_adapter.py")
A = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(A)

BOARD = "muse_decode_layer"
WATCH = "example-test-watch"
OCC = "11111111-1111-1111-1111-111111111111"
CYCLE = "22222222-2222-2222-2222-222222222222"
SUPERVISOR = "33333333-3333-3333-3333-333333333333"
IDS = ["44444444-4444-4444-4444-444444444444", "55555555-5555-5555-5555-555555555555"]
NOW = 1800000000
RULES = "Submissions are rate-limited (6 a minute per token, 10 a minute per account)"


class IO:
    @staticmethod
    def read_regular(path, limit=16 * 1024 * 1024, immutable=False):
        return A.protected(path, immutable=immutable, maximum=limit)

    @staticmethod
    def immutable_snapshot(path, raw):
        path = pathlib.Path(path)
        if path.exists():
            A.require(path.read_bytes() == raw, "IMMUTABLE_CONFLICT")
            return
        with path.open("xb") as stream:
            stream.write(raw); stream.flush(); os.fsync(stream.fileno())
        path.chmod(0o400)

    @staticmethod
    def atomic_record(path, value):
        path = pathlib.Path(path)
        tmp = path.with_name(path.name + ".new")
        with tmp.open("wb") as stream:
            stream.write(A.canonical(value)); stream.flush(); os.fsync(stream.fileno())
        tmp.chmod(0o600)
        os.replace(tmp, path)


class FakeOfficial:
    def __init__(self, files):
        self.files, self.posts, self.error, self.response = files, 0, None, {"job_id": "1234567890abcdef"}
        self.accepted = True
        self.left, self.jobs = 10, []

    def fetch(self, path, public=False):
        if path == "problems/" + BOARD:
            return {"name": BOARD, "files": {k: v for k, v in self.files.items() if k not in {"TASK.md", "AGENTS.md"}},
                    "agents_md": self.files["AGENTS.md"], "task_md": self.files["TASK.md"]}
        if path == "problems":
            return {"tracks": {"kernels": {"open": True}}, "problems": [{"name": BOARD, "limits": {"checks": {"left": self.left, "used": 10 - self.left, "per_day": 10}}}]}
        if path == "llms.txt": return RULES
        if path == "rules": return {"version": "test-rules-v1"}
        if path == "me": return {"rules": {"version": "test-rules-v1", "accepted_at": 1 if self.accepted else None}}
        if path == "jobs": return self.jobs
        raise AssertionError("unapproved route")

    def check(self, problem, snapshot):
        assert problem == BOARD and snapshot.is_file()
        self.posts += 1
        if self.error: raise self.error
        return types.SimpleNamespace(stdout=json.dumps(self.response), stderr="", returncode=0)


class Fixture(unittest.TestCase):
    def setUp(self):
        # A deliberately provisioned server-only directory. Never /tmp (the
        # production reader correctly rejects world-writable ancestors).
        root = pathlib.Path(os.environ["OPENKERNEL_ADAPTER_TEST_ROOT"])
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.root = pathlib.Path(tempfile.mkdtemp(prefix="case-", dir=root))
        self.paths = {}
        for key in ("stateDirectory", "legacyLedger", "watcherStateDirectory", "artifactDirectory", "workerRunsDirectory"):
            path = self.root / key; path.mkdir(mode=0o700); self.paths[key] = str(path)
        self.paths["accountLock"] = str(self.root / "account.lock")
        pathlib.Path(self.paths["accountLock"]).touch(mode=0o600)
        self.paths["registryPath"] = str(self.root / "registry.json")
        self.official_files = {"TASK.md": "task", "AGENTS.md": "agents", "baseline.py": "def run(): pass\n"}
        self.source = b"def kernel(x):\n    return x\n"
        self.candidate = b"def kernel(x):\n    return x + 1\n"
        self.checker = b"# Isolated contract fixture, never executed by controller.\n"
        self.patch = "".join(difflib.unified_diff(self.source.decode().splitlines(keepends=True), self.candidate.decode().splitlines(keepends=True), fromfile="base-candidate.py", tofile="candidate.py")).encode()
        self.provenance = self.root / "provenance.json"
        IO.immutable_snapshot(self.provenance, A.canonical({"attribution": "synthetic fixture", "sourceSHA256": A.digest(self.source)}))
        self.p = {"version": 1, "enabled": True, "authority": "operator-delegated-diagnostic-check", "allowedActions": ["check"],
                  "automaticPostRetry": False, "adapterSHA256": "a" * 64, "notBefore": NOW - 10, "expiresAt": NOW + 3600,
                  "watchId": WATCH, "controlCycleId": CYCLE, "project": "openkernel", "workspace": "openkernel/isolated-fixture",
                  "accountId": "test-account", "model": "test-model", "quotaPolicy": "official-account-limits", "minimumChecksRemaining": 1,
                  "rulesEvidence": {"llmsSha256": A.digest(RULES.encode()), "version": "test-rules-v1"}, **self.paths,
                  "boards": {BOARD: {"officialFiles": {k: A.digest(v.encode()) for k, v in self.official_files.items()},
                                     "materials": {"canonical-TASK.md": A.digest(b"task"), "canonical-AGENTS.md": A.digest(b"agents")},
                                     "seedSources": {A.digest(self.source): {"attribution": "synthetic fixture", "provenance": [{"path": str(self.provenance), "sha256": A.digest(self.provenance.read_bytes())}]}}}}}
        self.cpu = {"passed": True, "candidateSHA256": A.digest(self.candidate), "checkerSHA256": A.digest(self.checker),
                    "cases": [{"name": "distinguishing arithmetic", "passed": True}], "negativeControls": [{"name": "mutated mapping", "detected": True}]}
        self.verify = {"passed": True, "scopePreserved": True, "sourceSHA256": A.digest(self.source), "candidateSHA256": A.digest(self.candidate),
                       "gpuValidated": False, "officialActionsPerformed": False}
        self.qa = {**self.verify, "approvedForOfficialReview": True, "cpuReplayPassed": True,
                   "canonicalPrecisionPreserved": True, "evaluatorIndependent": True, "upstreamAttributionPreserved": True,
                   "blockingFindings": [], "findings": [], "limitations": ["CPU-only fixture"],
                   "cpuEvidence": {"checkerSHA256": A.digest(self.checker), "resultSHA256": A.digest(A.canonical(self.cpu)),
                                   "cases": self.cpu["cases"], "negativeControls": self.cpu["negativeControls"]}}
        self.detail = self.native_fixture()
        self.runtime = types.SimpleNamespace(api=lambda method, path: copy.deepcopy(self.detail), stopped=mock.Mock())
        self.audit = A.NativeAudit(self.p, self.runtime, IO)
        self.official = FakeOfficial(self.official_files)
        self.lane = A.OfficialLane(self.p, "b" * 64, IO, self.audit, self.official, clock=lambda: NOW)

    def tearDown(self):
        shutil.rmtree(self.root)

    def put(self, path, raw):
        path = pathlib.Path(path); path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        path.write_bytes(raw); path.chmod(0o600)

    def artifact(self, name, raw, attempt=IDS[0]):
        idx = len(getattr(self, "rows", [])) + 1
        row = {"id": f"66666666-6666-6666-6666-{idx:012d}", "attempt_id": attempt, "name": name, "digest": A.digest(raw), "size": len(raw)}
        self.put(pathlib.Path(self.paths["artifactDirectory"]) / row["digest"], raw)
        self.rows.append(row)
        return row

    def transcript(self):
        call = {"type": "tool/call", "data": {"callId": "actual-tool-call", "name": "bash", "arguments": json.dumps({"command": "sha256sum /data/cpu-checks.py /data/candidate.py && python3 /data/cpu-checks.py --candidate /data/candidate.py"})}}
        text = A.digest(self.checker) + " cpu-checks.py\n" + A.digest(self.candidate) + " candidate.py\n[Command finished with exit code 0]"
        result = {"type": "tool/result", "data": {"message": {"source": {"kind": "tool", "callId": "actual-tool-call"}, "content": [{"type": "tool-result", "content": [{"type": "text", "text": text}]}]}}}
        return A.canonical(call) + b"\n" + A.canonical(result) + b"\n"

    def native_fixture(self):
        self.rows = []
        source_row = self.artifact("candidate.py", self.source, "77777777-7777-7777-7777-777777777777")
        raw_materials = {"base-candidate.py": self.source, "canonical-TASK.md": b"task", "canonical-AGENTS.md": b"agents"}
        inp = {"version": 1, "watchId": WATCH, "supervisorOccurrenceId": SUPERVISOR,
               "officialActionsAllowed": False, "productionActionsAllowed": False, "sourceWriteAllowed": False,
               "experiment": {"problem": BOARD, "sourceArtifact": source_row, "hypothesis": "Synthetic distinguishing arithmetic fixture",
                              "materials": [{"name": k, "bytes": len(v), "sha256": A.digest(v), "base64": base64.b64encode(v).decode()} for k, v in raw_materials.items()]}}
        specs = [{"key": key, "kind": "agent", "readOnly": True, "workspace": self.p["workspace"], "accountId": self.p["accountId"], "model": self.p["model"],
                  "maxAttempts": 1, "dependsOn": [] if i == 0 else ["produce"]} for i, key in enumerate(["produce", "independent-qa"])]
        definition = {"project": "openkernel", "allowedActions": [], "allowedAccounts": [self.p["accountId"]], "steps": specs}
        key = WATCH + "-experiment-" + "c" * 32 + "-run"
        occ = {"id": OCC, "cycle_id": CYCLE, "state": "completed", "source_key": "manual:" + key, "input": inp, "definition": definition}
        directory = pathlib.Path(self.paths["watcherStateDirectory"])
        entry = {"occurrenceId": OCC, "cycleId": CYCLE, "problem": BOARD, "supervisorOccurrenceId": SUPERVISOR}
        self.put(directory / "state.json", A.canonical({"watchId": WATCH, "experiments": [entry]}))
        for kind, request_key, result, body in [("run", key, occ, {"requestKey": key, "input": inp}),
                ("create", key[:-4], {"id": CYCLE, "definition": definition}, {"requestKey": key[:-4], "definition": definition})]:
            receipt = {"state": "recorded", "kind": kind, "requestKey": request_key, "result": result, "requestDigest": A.digest(body)}
            self.put(directory / ("intent-" + A.digest(request_key) + ".json"), A.canonical(receipt))
        for name, raw in {"candidate.py": self.candidate, "implementation.patch": self.patch, "cpu-checks.py": self.checker,
                          "cpu-result.json": A.canonical(self.cpu), "verification.json": A.canonical(self.verify)}.items():
            self.artifact(name, raw)
        self.artifact("qa-verification.json", A.canonical(self.qa), IDS[1]); self.artifact("review.txt", b"Independent fixture review.\n", IDS[1])
        attempts, steps = [], []
        for i, attempt_id in enumerate(IDS):
            fence = f"88888888-8888-8888-8888-{i:012d}"
            home = pathlib.Path(self.paths["workerRunsDirectory"]) / attempt_id / "home"
            raw = A.canonical(inp)
            self.put(home / "inputs" / attempt_id / "cycle-input.json", raw)
            self.put(home / "sessions" / "--workspace--" / attempt_id / "session.v3.jsonl", self.transcript())
            attempts.append({"id": attempt_id, "fence": fence, "number": 1, "step_id": "step-" + str(i), "state": "completed",
                             "finished_at": "2026-01-01T00:00:00Z", "receipt": {"stopped": True}, "model": self.p["model"], "account_id": self.p["accountId"],
                             "context": {"manifest": {"cycleInput": {"attemptId": attempt_id, "runFence": fence, "digest": A.digest(raw), "size": len(raw)}}}})
            steps.append({"id": "step-" + str(i), "key": specs[i]["key"], "state": "completed", "definition": specs[i]})
        qa_home = pathlib.Path(self.paths["workerRunsDirectory"]) / IDS[1] / "home"
        for row in self.rows:
            if row["attempt_id"] == IDS[0]:
                self.put(qa_home / "inputs" / row["id"] / row["name"], (pathlib.Path(self.paths["artifactDirectory"]) / row["digest"]).read_bytes())
        return {"occurrence": occ, "steps": steps, "attempts": attempts, "artifacts": [r for r in self.rows if r["attempt_id"] in IDS]}

    def test_native_full_audit_inspect_no_effect(self):
        result = self.lane.run(OCC, "inspect")
        self.assertEqual(result["state"], "eligible")
        self.assertFalse(result["audit"]["operatorReviewedCandidate"])
        self.assertEqual(self.official.posts, 0)
        self.assertEqual(len(list(pathlib.Path(self.paths["stateDirectory"]).iterdir())), 0)
        self.assertEqual(self.runtime.stopped.call_count, 2)

    def test_documentation_only_candidate_cannot_consume_check_quota(self):
        self.candidate = b'"""The proposed optimization was already present."""\n' + self.source
        self.patch = "".join(difflib.unified_diff(self.source.decode().splitlines(keepends=True),
            self.candidate.decode().splitlines(keepends=True), fromfile="base-candidate.py", tofile="candidate.py")).encode()
        self.cpu["candidateSHA256"] = A.digest(self.candidate)
        self.verify["candidateSHA256"] = A.digest(self.candidate)
        self.qa["candidateSHA256"] = A.digest(self.candidate)
        self.qa["cpuEvidence"]["resultSHA256"] = A.digest(A.canonical(self.cpu))
        self.detail = self.native_fixture()
        # Even a mistakenly positive independent report cannot promote a new
        # source hash whose only difference is documentation.
        self.assertTrue(self.qa["approvedForOfficialReview"])
        with self.assertRaisesRegex(ValueError, "NO_EXECUTABLE_CHANGE"):
            self.lane.run(OCC, "check")
        row = A.scan(self.lane, self.root / "status.json", "check")["outcomes"][0]
        self.assertEqual(row["state"], "rejected_repairable")
        self.assertEqual(row["code"], "NO_EXECUTABLE_CHANGE")
        self.assertEqual(row["proposedRemediation"], "supervisor_new_hypothesis")
        self.assertEqual(row["candidateSHA256"], A.digest(self.candidate))
        self.assertEqual(self.official.posts, 0)
        self.assertEqual(list(pathlib.Path(self.paths["stateDirectory"]).iterdir()), [])

    def test_kernel_structure_ignores_only_comments_formatting_and_declared_docs(self):
        before = b'"""module old"""\nclass C:\n    """class old"""\n    async def run(self, x):\n        """method old"""\n        return x + 1\n'
        after = b'# A different comment\n"""module new"""\n\nclass C:\n    """class new"""\n    async def run(self,x):\n        """method new"""\n        return x+1  # same arithmetic\n'
        self.assertNotEqual(A.digest(before), A.digest(after))
        self.assertEqual(A.kernel_code_structure(before), A.kernel_code_structure(after))

    def test_kernel_structure_preserves_runtime_literals_and_launch_config(self):
        for before, after in [
            (b'def kernel(x):\n    return x + 1\n', b'def kernel(x):\n    return x + 2\n'),
            (b'def kernel():\n    return "old"\n', b'def kernel():\n    return "new"\n'),
            (b'@jit(num_warps=4)\ndef kernel(x):\n    return x\n',
             b'@jit(num_warps=8)\ndef kernel(x):\n    return x\n')]:
            with self.subTest(before=before):
                self.assertNotEqual(A.kernel_code_structure(before), A.kernel_code_structure(after))

    def test_one_post_registry_and_no_resend(self):
        first = self.lane.run(OCC, "check"); second = self.lane.run(OCC, "check")
        self.assertEqual(first["state"], "job_recorded"); self.assertEqual(second["state"], "existing_job")
        self.assertEqual(self.official.posts, 1)
        registry = A.strict_json(pathlib.Path(self.paths["registryPath"]).read_bytes())
        self.assertEqual(len(registry["jobs"]), 1)
        row = registry["jobs"][0]
        self.assertEqual(A.digest(pathlib.Path(row["receiptPath"]).read_bytes()), row["receiptSHA256"])

    def test_timeout_never_retries(self):
        self.official.error = subprocess.TimeoutExpired("redacted", 90)
        self.assertEqual(self.lane.run(OCC, "check")["state"], "submission_outcome_unknown")
        with self.assertRaisesRegex(ValueError, "UNCERTAIN"):
            self.lane.run(OCC, "check")
        self.assertEqual(self.official.posts, 1)

    def test_bad_receipt_never_retries(self):
        self.official.response = {"message": "queued without identity"}
        self.assertEqual(self.lane.run(OCC, "check")["state"], "submission_outcome_unknown")
        with self.assertRaises(ValueError): self.lane.run(OCC, "check")
        self.assertEqual(self.official.posts, 1)

    def test_uncertain_other_board_blocks_account(self):
        self.put(pathlib.Path(self.paths["legacyLedger"]) / "other-scored.json", A.canonical({"state": "submission_intent", "intentEpoch": NOW}))
        with self.assertRaisesRegex(ValueError, "ACCOUNT_HAS_UNCERTAIN_EFFECT"): self.lane.run(OCC, "check")
        self.assertEqual(self.official.posts, 0)

    def test_legacy_duplicate_blocks(self):
        self.put(pathlib.Path(self.paths["legacyLedger"]) / (BOARD + "-" + A.digest(self.candidate) + "-check.json"), b"{}")
        with self.assertRaisesRegex(ValueError, "SOURCE_ALREADY"): self.lane.run(OCC, "check")
        self.assertEqual(self.official.posts, 0)

    def test_missing_acceptance_quota_rate_contract_blocks(self):
        self.official.accepted = False
        with self.assertRaisesRegex(ValueError, "HUMAN_TERMS"): self.lane.run(OCC, "check")
        self.official.accepted = True; self.official.left = 1
        with self.assertRaisesRegex(ValueError, "CHECK_QUOTA"): self.lane.run(OCC, "check")
        self.official.left = 10; self.official.jobs = [{"submitted_at": NOW}] * 6
        with self.assertRaisesRegex(ValueError, "EXTERNAL_RATE"): self.lane.run(OCC, "check")
        self.official.jobs = []; self.official.files["TASK.md"] = "changed"
        with self.assertRaisesRegex(ValueError, "OFFICIAL_CONTRACT"): self.lane.run(OCC, "check")
        self.assertEqual(self.official.posts, 0)

    def test_completed_without_physical_stop_blocks(self):
        self.runtime.stopped.side_effect = ValueError("STILL_RUNNING")
        with self.assertRaisesRegex(ValueError, "STILL_RUNNING"): self.lane.run(OCC, "check")
        self.assertEqual(self.official.posts, 0)

    def test_not_completed_or_fence_change_blocks(self):
        self.detail["occurrence"]["state"] = "running"
        with self.assertRaisesRegex(ValueError, "NATIVE_NOT_COMPLETED"): self.audit.audit(OCC)
        self.detail["occurrence"]["state"] = "completed"
        self.detail["attempts"][0]["fence"] = IDS[1]
        with self.assertRaisesRegex(ValueError, "NATIVE_INPUT_CHANGED"): self.audit.audit(OCC)

    def test_extra_attempt_blocks(self):
        self.detail["attempts"].append(copy.deepcopy(self.detail["attempts"][0]))
        with self.assertRaisesRegex(ValueError, "NATIVE_ATTEMPT_SET"): self.audit.audit(OCC)

    def test_native_input_forged_or_not_dispatch_blocks(self):
        self.detail["occurrence"]["input"]["officialActionsAllowed"] = True
        with self.assertRaisesRegex(ValueError, "DISPATCH_CHANGED"): self.audit.audit(OCC)
        self.detail["occurrence"]["input"]["officialActionsAllowed"] = False
        self.put(pathlib.Path(self.paths["watcherStateDirectory"]) / "state.json", A.canonical({"watchId": WATCH, "experiments": []}))
        with self.assertRaisesRegex(ValueError, "NOT_WATCHDOG_DISPATCHED"): self.audit.audit(OCC)

    def test_qa_actual_handoff_modified_blocks(self):
        row = next(r for r in self.detail["artifacts"] if r["name"] == "candidate.py")
        path = pathlib.Path(self.paths["workerRunsDirectory"]) / IDS[1] / "home/inputs" / row["id"] / row["name"]
        self.put(path, self.source)
        with self.assertRaisesRegex(ValueError, "QA_PREDECESSOR_CHANGED"): self.audit.audit(OCC)

    def test_corrupt_artifact_blocks(self):
        path = pathlib.Path(self.paths["artifactDirectory"]) / A.digest(self.candidate)
        self.put(path, self.source)
        with self.assertRaisesRegex(ValueError, "ARTIFACT_BYTES_CHANGED"): self.audit.audit(OCC)

    def test_optional_empty_workspace_patches_preserve_full_audit(self):
        for attempt in IDS:
            for name in ("changes.patch", "extra.log"):
                row = self.artifact(name, b"", attempt)
                self.detail["artifacts"].append(row)
                if attempt == IDS[0]:
                    path = pathlib.Path(self.paths["workerRunsDirectory"]) / IDS[1] / "home/inputs" / row["id"] / row["name"]
                    self.put(path, b"")
        result = self.lane.run(OCC, "inspect")
        self.assertEqual(result["state"], "eligible")
        self.assertEqual(self.official.posts, 0)

    def test_empty_mandatory_or_arbitrary_artifact_rejected(self):
        for name in (*A.PRODUCER_FILES, *A.QA_FILES, "other.patch"):
            with self.subTest(name=name):
                row = self.artifact(name, b"")
                with self.assertRaisesRegex(ValueError, "ARTIFACT_SIZE"): self.audit.artifact(row)

    def test_empty_workspace_patch_requires_real_empty_digest(self):
        row = self.artifact("changes.patch", b"")
        row["digest"] = A.digest(self.candidate)
        with self.assertRaisesRegex(ValueError, "ARTIFACT_BYTES_CHANGED"): self.audit.artifact(row, allow_empty=True)

    def test_empty_workspace_patch_bytes_still_verified(self):
        row = self.artifact("changes.patch", b"")
        self.put(pathlib.Path(self.paths["artifactDirectory"]) / row["digest"], b"tampered")
        with self.assertRaisesRegex(ValueError, "ARTIFACT_BYTES_CHANGED"): self.audit.artifact(row, allow_empty=True)

    def test_audit_never_marks_required_evidence_optional(self):
        for attempt, required in ((IDS[0], A.PRODUCER_FILES), (IDS[1], A.QA_FILES)):
            for name in required:
                row = next(r for r in self.detail["artifacts"] if r["attempt_id"] == attempt and r["name"] == name)
                original = copy.deepcopy(row)
                row.update(size=0, digest=A.digest(b""))
                self.put(pathlib.Path(self.paths["artifactDirectory"]) / A.digest(b""), b"")
                with self.subTest(name=name), self.assertRaisesRegex(ValueError, "ARTIFACT_SIZE"):
                    self.audit.audit(OCC)
                row.clear(); row.update(original)

    def test_source_derivative_without_provenance_blocks(self):
        self.p["boards"][BOARD]["seedSources"] = {}
        with self.assertRaises(OSError): self.audit.audit(OCC)

    def test_semantics_no_precision_trade_or_false_approval(self):
        for change in ({"canonicalPrecisionPreserved": False}, {"evaluatorIndependent": False}, {"upstreamAttributionPreserved": False},
                       {"approvedForOfficialReview": False}, {"blockingFindings": ["wrong rounding"]}, {"gpuValidated": True},
                       {"candidateSHA256": "d" * 64}, {"cpuReplayPassed": False}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                A.semantic_qa(self.verify, {**self.qa, **change}, self.cpu, A.digest(self.source), A.digest(self.candidate), A.digest(self.checker), A.digest(A.canonical(self.cpu)))

    def test_empty_cases_or_missing_negative_controls_blocks(self):
        for field in ("cases", "negativeControls"):
            qa = copy.deepcopy(self.qa); qa["cpuEvidence"][field] = []
            with self.assertRaisesRegex(ValueError, "CPU_MEANINGFUL"):
                A.semantic_qa(self.verify, qa, self.cpu, A.digest(self.source), A.digest(self.candidate), A.digest(self.checker), A.digest(A.canonical(self.cpu)))

    def test_echo_not_actual_replay_and_wrong_hash(self):
        for raw in (self.transcript().replace(b"python3", b"echo"), self.transcript().replace(b"python3", b"echo python3"),
                    self.transcript().replace(b"python3", b"false && python3"), self.transcript().replace(b"python3", b"bash -c python3"),
                    self.transcript().replace(A.digest(self.checker).encode(), b"f" * 64), self.transcript().replace(b"exit code 0", b"exit code 1")):
            with self.assertRaisesRegex(ValueError, "CPU_REPLAY_EXECUTION_UNPROVEN"):
                A.replay_commands(raw, A.digest(self.checker), A.digest(self.candidate))

    def test_patch_exact_and_adverse(self):
        self.assertEqual(A.apply_single_file_patch(self.source, self.patch), self.candidate)
        for patch in (self.patch.replace(b"base-candidate.py", b"../../etc/passwd"), self.patch.replace(b"    return x\n", b"    return y\n"), self.patch + self.patch):
            with self.assertRaises(ValueError): A.apply_single_file_patch(self.source, patch)

    def test_source_evaluator_and_env_escape_denied(self):
        A.source_screen(b"import torch\nimport triton\ndef forward(x): return x\n")
        for raw in (b"import os", b"from harness.reference import run", b"eval('x')", b"x.__globals__", b"getattr(x, 'eval')"):
            with self.assertRaisesRegex(ValueError, "SOURCE_REQUIRES_MANUAL_REVIEW"): A.source_screen(raw)

    def test_policy_action_expiry_and_hash_scope(self):
        A.validate_policy(self.p, NOW, "a" * 64)
        for change in ({"enabled": False}, {"allowedActions": ["check", "scored"]}, {"automaticPostRetry": True}, {"expiresAt": NOW},
                       {"adapterSHA256": "f" * 64}, {"quotaPolicy": "unbounded"}, {"authority": "worker-approved"}):
            with self.subTest(change=change), self.assertRaises(ValueError): A.validate_policy({**self.p, **change}, NOW, "a" * 64)
        with self.assertRaisesRegex(ValueError, "CHECK_ONLY"): self.lane.run(OCC, "scored")
        self.assertEqual(self.official.posts, 0)

    def test_filesystem_guards_and_strict_json(self):
        link = self.root / "symlink"; link.symlink_to(self.provenance)
        with self.assertRaisesRegex(ValueError, "PROTECTED_PATH"): A.protected(link)
        self.provenance.chmod(0o666)
        with self.assertRaisesRegex(ValueError, "PROTECTED_OWNER_MODE"): A.protected(self.provenance)
        for raw in (b'{"passed":true,"passed":false}', b'{"value":NaN}'):
            with self.assertRaises(ValueError): A.strict_json(raw)

    def test_audit_change_between_preflight_and_submit_blocks(self):
        actual = self.audit.audit; calls = [0]
        def changing(occ):
            result = actual(occ); calls[0] += 1
            if calls[0] == 2: result[0]["hypothesis"] = "changed after review"
            return result
        self.audit.audit = changing
        with self.assertRaisesRegex(ValueError, "AUDIT_CHANGED"): self.lane.run(OCC, "check")
        self.assertEqual(self.official.posts, 0)

    def test_crash_after_durable_intent_never_sends_again(self):
        original = IO.immutable_snapshot
        def failing(path, raw):
            if pathlib.Path(path).parent == pathlib.Path(self.paths["legacyLedger"]): raise OSError("mirror disk failure")
            return original(path, raw)
        with mock.patch.object(IO, "immutable_snapshot", side_effect=failing), self.assertRaises(OSError):
            self.lane.run(OCC, "check")
        with self.assertRaisesRegex(ValueError, "UNCERTAIN"): self.lane.run(OCC, "check")
        self.assertEqual(self.official.posts, 0)

    def test_registry_write_failure_recovers_without_post(self):
        original = IO.atomic_record
        def failing(path, value):
            if str(path) == self.paths["registryPath"]: raise OSError("registry disk failure")
            return original(path, value)
        with mock.patch.object(IO, "atomic_record", side_effect=failing), self.assertRaises(OSError):
            self.lane.run(OCC, "check")
        self.assertEqual(self.lane.run(OCC, "check")["state"], "existing_job")
        self.assertEqual(self.official.posts, 1)

    def test_pause_during_preflight_blocks(self):
        answers = iter([True, False]); self.lane.control_active = lambda: next(answers)
        with self.assertRaisesRegex(ValueError, "CONTROL_PAUSED"): self.lane.run(OCC, "check")
        self.assertEqual(self.official.posts, 0)

    def test_scan_exposes_active_then_blocked_then_ready_without_post(self):
        status = self.root / "status.json"
        self.detail["occurrence"]["state"] = "running"
        self.assertEqual(A.scan(self.lane, status, "inspect")["outcomes"][0]["state"], "await_native")
        self.detail["occurrence"]["state"] = "completed"
        self.detail["attempts"][0]["receipt"]["stopped"] = False
        row = A.scan(self.lane, status, "inspect")["outcomes"][0]
        self.assertEqual(row["state"], "controller_blocked")
        self.assertEqual(row["candidateSHA256"], A.digest(self.candidate))
        self.detail["attempts"][0]["receipt"]["stopped"] = True
        self.assertEqual(A.scan(self.lane, status, "inspect")["outcomes"][0]["state"], "eligible")
        self.assertEqual(self.official.posts, 0)

    def test_scan_quota_and_uncertain_keep_ownership(self):
        status = self.root / "status.json"
        self.official.left = 0
        self.assertEqual(A.scan(self.lane, status, "check")["outcomes"][0]["state"], "quota_wait")
        self.official.left = 10; self.official.error = subprocess.TimeoutExpired("redacted", 90)
        first = A.scan(self.lane, status, "check")["outcomes"][0]
        self.assertEqual(first["state"], "uncertain_owned")
        second = A.scan(self.lane, status, "check")["outcomes"][0]
        self.assertEqual(second["state"], "uncertain_owned")
        self.assertEqual(self.official.posts, 1)

    def test_scan_registered_receipt_skips_native_reaudit(self):
        self.lane.run(OCC, "check")
        self.runtime.api = mock.Mock(side_effect=AssertionError("registered work should not be reread"))
        status = A.scan(self.lane, self.root / "status.json", "check")
        self.assertEqual(status["outcomes"][0]["jobId"], "1234567890abcdef")
        self.assertFalse(status["outcomes"][0]["postAttempted"])
        self.assertEqual(self.official.posts, 1)

    def test_registered_receipt_semantics_cannot_be_replaced_with_hash_only(self):
        self.lane.run(OCC, "check")
        path = pathlib.Path(self.paths["registryPath"])
        registry = A.strict_json(path.read_bytes()); row = registry["jobs"][0]
        receipt = A.strict_json(pathlib.Path(row["receiptPath"]).read_bytes())
        receipt["transportReceipt"]["receipt"]["job_id"] = "f" * 16
        replacement = self.root / "replacement-receipt.json"
        IO.immutable_snapshot(replacement, A.canonical(receipt))
        row.update(receiptPath=str(replacement), receiptSHA256=A.digest(receipt))
        IO.atomic_record(path, registry)
        with self.assertRaisesRegex(ValueError, "REGISTRY_TRANSPORT_BINDING"):
            self.lane.registered_occurrences()

    def test_scan_deadline_does_not_start_new_post(self):
        with mock.patch.object(A.time, "monotonic", side_effect=[1, 47]):
            status = A.scan(self.lane, self.root / "status.json", "check")
        self.assertTrue(status["scanBudgetExhausted"])
        self.assertEqual(self.official.posts, 0)

    def test_readonly_account_lock_is_controller_failure_not_kernel_repair(self):
        original = A.os.open
        def readonly_lock(path, flags, *args, **kwargs):
            if pathlib.Path(path) == pathlib.Path(self.paths["accountLock"]) and flags & os.O_RDWR:
                raise OSError(errno.EROFS, "Read-only file system")
            return original(path, flags, *args, **kwargs)
        with mock.patch.object(A.os, "open", side_effect=readonly_lock):
            row = A.scan(self.lane, self.root / "status.json", "check")["outcomes"][0]
        self.assertEqual(row["state"], "controller_blocked")
        self.assertEqual(row["proposedRemediation"], "repair_controller_without_replacing_candidate")
        self.assertEqual(row["candidateSHA256"], A.digest(self.candidate))
        self.assertEqual(self.official.posts, 0)

    def test_observation_timeout_preserves_owner(self):
        self.runtime.api = mock.Mock(side_effect=TimeoutError("read timed out"))
        row = A.scan(self.lane, self.root / "status.json", "check")["outcomes"][0]
        self.assertEqual(row["state"], "observation_wait")
        self.assertEqual(self.official.posts, 0)

    def test_semantic_rejection_remains_repairable(self):
        self.audit.audit = mock.Mock(side_effect=ValueError("QA_ATTESTATION_MISSING_canonicalPrecisionPreserved"))
        row = A.scan(self.lane, self.root / "status.json", "check")["outcomes"][0]
        self.assertEqual(row["state"], "rejected_repairable")
        self.assertEqual(row["candidateSHA256"], A.digest(self.candidate))
        self.assertEqual(self.official.posts, 0)

    def test_acknowledged_post_dominates_later_registry_controller_failure(self):
        original = IO.atomic_record
        def failing(path, value):
            if str(path) == self.paths["registryPath"]: raise OSError(errno.EROFS, "registry readonly")
            return original(path, value)
        with mock.patch.object(IO, "atomic_record", side_effect=failing):
            row = A.scan(self.lane, self.root / "status.json", "check")["outcomes"][0]
        self.assertEqual(row["state"], "check_running")
        self.assertEqual(row["jobId"], "1234567890abcdef")
        self.assertEqual(self.official.posts, 1)


if __name__ == "__main__":
    unittest.main()
