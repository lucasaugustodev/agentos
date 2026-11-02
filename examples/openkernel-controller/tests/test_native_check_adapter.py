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

    def dispatch_diagnostic(self, report, *, version=None, projection=None):
        raw = A.canonical(report)
        ref = self.artifact("qa-verification.json", raw, "77777777-7777-7777-7777-777777777777")
        diagnostic = {"version": 1, "authority": "prior_qa_diagnostic_only_not_current_acceptance",
                      "bindingState": "verified", "currentAcceptance": False,
                      "artifact": {k: ref[k] for k in ("id", "name", "digest", "size")}, "report": report}
        if projection is not None: diagnostic["reportProjection"] = projection
        occurrence = self.detail["occurrence"]
        occurrence["input"]["experiment"]["repairQADiagnostic"] = diagnostic
        key = occurrence["source_key"][len("manual:"):]
        original_body = copy.deepcopy({"requestKey": key, "input": occurrence["input"]})
        occurrence["input"] = A.request_numbers_v1(occurrence["input"])
        body = {"requestKey": key, "input": occurrence["input"]}
        receipt = {"state": "recorded", "kind": "run", "requestKey": key,
                   "result": copy.deepcopy(occurrence), "requestDigest": A.digest(body if version else original_body)}
        if version is not None: receipt["requestCanonicalization"] = version
        path = pathlib.Path(self.paths["watcherStateDirectory"]) / ("intent-" + A.digest(key) + ".json")
        self.put(path, A.canonical(receipt))
        for attempt in self.detail["attempts"]:
            data = A.canonical(occurrence["input"])
            self.put(pathlib.Path(self.paths["workerRunsDirectory"]) / attempt["id"] / "home" / "inputs" / attempt["id"] / "cycle-input.json", data)
            attempt["context"]["manifest"]["cycleInput"].update(digest=A.digest(data), size=len(data))
        return path, ref

    def test_legacy_integral_float_diagnostic_recovers_exact_hash_without_receipt_write(self):
        path, ref = self.dispatch_diagnostic({"nested": [1.0, 0.0, -0.0, 7244800000.0, 1.25, True]})
        original = path.read_bytes()
        self.assertEqual(self.lane.run(OCC, "inspect")["state"], "eligible")
        self.assertEqual(path.read_bytes(), original)
        self.assertEqual(self.official.posts, 0)
        self.assertEqual(len(list(pathlib.Path(self.paths["stateDirectory"]).iterdir())), 0)

    def test_legacy_recovery_requires_original_full_body_digest(self):
        path, ref = self.dispatch_diagnostic({"value": 1.0})
        receipt = A.strict_json(path.read_bytes()); receipt["requestDigest"] = "0" * 64
        self.put(path, A.canonical(receipt))
        with self.assertRaisesRegex(ValueError, "DISPATCH_DIGEST"):
            self.audit.dispatch(self.detail["occurrence"])

    def test_legacy_numeric_recovery_never_coerces_boolean_or_different_structure(self):
        for value in [True, "1", 2, [1], {"nested": 1}]:
            with self.subTest(value=value):
                path, ref = self.dispatch_diagnostic({"value": 1.0})
                self.detail["occurrence"]["input"]["experiment"]["repairQADiagnostic"]["report"]["value"] = value
                receipt = A.strict_json(path.read_bytes()); receipt["result"] = copy.deepcopy(self.detail["occurrence"])
                self.put(path, A.canonical(receipt))
                with self.assertRaisesRegex(ValueError, "DISPATCH_DIGEST"):
                    self.audit.dispatch(self.detail["occurrence"])

    def test_projected_legacy_diagnostic_cannot_be_hydrated(self):
        self.dispatch_diagnostic({"value": 1.0}, projection={"version": 1, "complete": False})
        with self.assertRaisesRegex(ValueError, "DISPATCH_DIGEST"):
            self.audit.dispatch(self.detail["occurrence"])

    def test_legacy_diagnostic_artifact_bytes_are_reverified(self):
        path, ref = self.dispatch_diagnostic({"value": 1.0})
        self.put(pathlib.Path(self.paths["artifactDirectory"]) / ref["digest"], b'{"value":2.0}')
        with self.assertRaisesRegex(ValueError, "ARTIFACT_BYTES_CHANGED"):
            self.audit.dispatch(self.detail["occurrence"])

    def test_versioned_numbers_keep_full_native_audit_and_qa(self):
        path, ref = self.dispatch_diagnostic({"value": [1.0, -0.0, False, 1e-8]}, version=A.REQUEST_CANONICALIZATION,
                                             projection={"version": 1, "complete": False})
        self.assertEqual(self.lane.run(OCC, "inspect")["state"], "eligible")
        self.detail["attempts"][1]["receipt"]["stopped"] = False
        with self.assertRaisesRegex(ValueError, "ATTEMPT_NOT_STOPPED"):
            self.lane.run(OCC, "inspect")
        self.assertEqual(self.official.posts, 0)

    def test_unknown_request_canonicalization_is_rejected_even_with_matching_digest(self):
        self.dispatch_diagnostic({"value": 1.0}, version="future-unreviewed")
        with self.assertRaisesRegex(ValueError, "DISPATCH_CANONICALIZATION"):
            self.audit.dispatch(self.detail["occurrence"])

    def test_versioned_number_contract_rejects_unsafe_values_and_preserves_types(self):
        self.assertEqual(A.canonical(A.request_numbers_v1({"v": [True, False, None, "1", 1.0, -0.0, 1e-8]})),
                         b'{"v":[true,false,null,"1",1,0,1e-08]}')
        for value in [2 ** 53, -(2 ** 53), float(2 ** 53), 10 ** 1000, float('inf'), float('-inf'), float('nan')]:
            with self.subTest(type=type(value).__name__):
                with self.assertRaisesRegex(ValueError, "REQUEST_NUMBER_UNSAFE"):
                    A.request_numbers_v1({"v": value})
        self.assertEqual(A.request_numbers_v1(A.MAX_SAFE_INTEGER), A.MAX_SAFE_INTEGER)
        self.assertFalse(A.same_json_numeric_representation(True, 1))
        self.assertFalse(A.same_json_numeric_representation(2 ** 53, float(2 ** 53)))

    def replace_evidence(self, name, raw, attempt):
        row = next(r for r in self.detail["artifacts"] if r["name"] == name and r["attempt_id"] == attempt)
        row.update(digest=A.digest(raw), size=len(raw))
        self.put(pathlib.Path(self.paths["artifactDirectory"]) / row["digest"], raw)
        if attempt == IDS[0]:
            self.put(pathlib.Path(self.paths["workerRunsDirectory"]) / IDS[1] / "home" / "inputs" / row["id"] / name, raw)
        return row

    def test_cpu_nonfinite_evidence_is_rejected_with_authenticated_repair_reference(self):
        raw = json.dumps({**self.cpu, "diagnostics": {"end": float('inf')}}).encode()
        ref = self.replace_evidence("cpu-result.json", raw, IDS[0])
        row = A.scan(self.lane, self.root / "status.json", "check")["outcomes"][0]
        self.assertEqual((row["state"], row["code"]), ("rejected_repairable", "CPU_EVIDENCE_JSON_NONFINITE"))
        self.assertEqual(row["candidateSHA256"], A.digest(self.candidate))
        self.assertEqual(row["evidenceErrors"], [{"stage": "produce", "code": "JSON_NONFINITE",
                          "artifact": {k: ref[k] for k in ("id", "name", "digest", "size")}}])
        self.assertEqual(self.official.posts, 0)

    def test_qa_nonfinite_evidence_is_rejected_without_acceptance(self):
        self.replace_evidence("qa-verification.json", json.dumps({**self.qa, "diagnostics": float('nan')}).encode(), IDS[1])
        row = A.scan(self.lane, self.root / "status.json", "check")["outcomes"][0]
        self.assertEqual((row["state"], row["code"]), ("rejected_repairable", "QA_EVIDENCE_JSON_NONFINITE"))
        self.assertEqual(self.official.posts, 0)

    def test_nonfinite_outside_authenticated_evidence_remains_controller_blocked(self):
        self.audit.audit = mock.Mock(side_effect=ValueError("JSON_NONFINITE"))
        row = A.scan(self.lane, self.root / "status.json", "check")["outcomes"][0]
        self.assertEqual(row["state"], "controller_blocked")
        self.assertNotIn("evidenceErrors", row)
        with self.assertRaisesRegex(ValueError, "JSON_NONFINITE"): A.strict_json(b'{"v":1e999}')

    def test_nonfinite_evidence_does_not_hide_artifact_corruption(self):
        ref = next(r for r in self.detail["artifacts"] if r["name"] == "cpu-result.json")
        self.put(pathlib.Path(self.paths["artifactDirectory"]) / ref["digest"], b'{"v":Infinity}')
        row = A.scan(self.lane, self.root / "status.json", "check")["outcomes"][0]
        self.assertEqual((row["state"], row["code"]), ("controller_blocked", "ARTIFACT_BYTES_CHANGED"))
        self.assertNotIn("evidenceErrors", row)

    def projected_cpu_evidence(self):
        # Sanitized shape of a real 23-case/9-control report: the producer added
        # diagnostic detail, while QA followed the instructed name/flag schema.
        self.cpu['cases'] = [{'name': f'case-{i:02d}', 'passed': True,
                              'detail': 'Bounded arithmetic diagnostic'} for i in range(23)]
        self.cpu['negativeControls'] = [{'name': f'control-{i:02d}', 'detected': True,
                                         'detail': 'Mutated input rejected'} for i in range(9)]
        evidence = self.qa['cpuEvidence']
        evidence['resultSHA256'] = A.digest(A.canonical(self.cpu))
        for name, flag in [('cases', 'passed'), ('negativeControls', 'detected')]:
            evidence[name] = [{'name': row['name'], flag: row[flag]} for row in self.cpu[name]]

    def semantic_check(self):
        A.semantic_qa(self.verify, self.qa, self.cpu, A.digest(self.source), A.digest(self.candidate),
                      A.digest(self.checker), A.digest(A.canonical(self.cpu)))

    def test_projected_cpu_diagnostics_pass_actual_native_audit_without_post(self):
        self.projected_cpu_evidence()
        self.detail = self.native_fixture()
        self.assertEqual(self.lane.run(OCC, 'inspect')['state'], 'eligible')
        self.assertEqual(self.runtime.stopped.call_count, 2)
        self.assertEqual(self.official.posts, 0)
        self.assertEqual(list(pathlib.Path(self.paths['stateDirectory']).iterdir()), [])

    def test_identical_optional_diagnostics_remain_valid(self):
        self.projected_cpu_evidence()
        self.qa['cpuEvidence']['cases'] = copy.deepcopy(self.cpu['cases'])
        self.qa['cpuEvidence']['negativeControls'] = copy.deepcopy(self.cpu['negativeControls'])
        self.semantic_check()

    def test_case_and_control_projection_rejects_omission_addition_reorder_and_name_drift(self):
        for name in ['cases', 'negativeControls']:
            for mutation in ['omit', 'add', 'reorder', 'rename']:
                with self.subTest(name=name, mutation=mutation):
                    self.projected_cpu_evidence()
                    rows = self.qa['cpuEvidence'][name]
                    if mutation == 'omit': rows.pop()
                    elif mutation == 'add': rows.append({**rows[0], 'name': 'unexpected'})
                    elif mutation == 'reorder': rows.reverse()
                    else: rows[0]['name'] += '-different'
                    with self.assertRaisesRegex(ValueError, 'CPU_CASES_NOT_REPLAYED'):
                        self.semantic_check()

    def test_duplicate_case_or_control_identity_is_rejected_even_when_both_reports_match(self):
        for name in ['cases', 'negativeControls']:
            with self.subTest(name=name):
                self.projected_cpu_evidence()
                self.cpu[name][1]['name'] = self.cpu[name][0]['name']
                self.qa['cpuEvidence'][name][1]['name'] = self.cpu[name][0]['name']
                self.qa['cpuEvidence']['resultSHA256'] = A.digest(A.canonical(self.cpu))
                with self.assertRaisesRegex(ValueError, 'CPU_MEANINGFUL_'):
                    self.semantic_check()

    def test_projected_flags_are_boolean_and_true_in_both_reports(self):
        for name, flag in [('cases', 'passed'), ('negativeControls', 'detected')]:
            for value in [False, 1, 'true', None]:
                for which in ['producer', 'qa']:
                    with self.subTest(name=name, value=value, which=which):
                        self.projected_cpu_evidence()
                        rows = self.cpu[name] if which == 'producer' else self.qa['cpuEvidence'][name]
                        rows[0][flag] = value
                        self.qa['cpuEvidence']['resultSHA256'] = A.digest(A.canonical(self.cpu))
                        with self.assertRaisesRegex(ValueError, 'CPU_MEANINGFUL_'):
                            self.semantic_check()

    def test_projection_never_relaxes_checker_result_or_candidate_hash(self):
        for where, field in [('qa', 'checkerSHA256'), ('qa', 'resultSHA256'),
                              ('cpu', 'checkerSHA256'), ('cpu', 'candidateSHA256')]:
            with self.subTest(where=where, field=field):
                self.projected_cpu_evidence()
                target = self.qa['cpuEvidence'] if where == 'qa' else self.cpu
                target[field] = 'f' * 64
                with self.assertRaisesRegex(ValueError, 'QA_TEST_BINDING|CPU_RESULT_BINDING'):
                    self.semantic_check()

    def test_optional_detail_cannot_conflict_or_carry_ambiguous_structured_claims(self):
        for name in ['cases', 'negativeControls']:
            for detail in ['contradictory diagnostic', {'passed': False}, [], None, True]:
                with self.subTest(name=name, detail=detail):
                    self.projected_cpu_evidence()
                    self.qa['cpuEvidence'][name][0]['detail'] = detail
                    with self.assertRaisesRegex(ValueError, 'CPU_CASES_NOT_REPLAYED'):
                        self.semantic_check()
            self.projected_cpu_evidence()
            self.cpu[name][0]['detail'] = {'blocking': True}
            self.qa['cpuEvidence']['resultSHA256'] = A.digest(A.canonical(self.cpu))
            with self.assertRaisesRegex(ValueError, 'CPU_CASES_NOT_REPLAYED'):
                self.semantic_check()

    def test_only_text_detail_may_be_omitted_no_other_extra_field(self):
        for name in ['cases', 'negativeControls']:
            for which in ['producer', 'qa']:
                with self.subTest(name=name, which=which):
                    self.projected_cpu_evidence()
                    rows = self.cpu[name] if which == 'producer' else self.qa['cpuEvidence'][name]
                    rows[0]['additionalRequirement'] = 'must not disappear'
                    self.qa['cpuEvidence']['resultSHA256'] = A.digest(A.canonical(self.cpu))
                    with self.assertRaisesRegex(ValueError, 'CPU_CASES_NOT_REPLAYED'):
                        self.semantic_check()

    def test_projected_report_does_not_replace_actual_replay_call_result_pair(self):
        self.projected_cpu_evidence()
        self.detail = self.native_fixture()
        path = pathlib.Path(self.paths['workerRunsDirectory']) / IDS[1] / 'home/sessions/--workspace--' / IDS[1] / 'session.v3.jsonl'
        self.put(path, A.canonical({'type': 'assistant', 'data': {'text': 'CPU replay passed'}}) + b'\n')
        with self.assertRaisesRegex(ValueError, 'CPU_REPLAY_EXECUTION_UNPROVEN'):
            self.lane.run(OCC, 'inspect')
        self.assertEqual(self.official.posts, 0)

    def test_additional_fields_keep_exact_json_types_despite_python_equality(self):
        for name in ['cases', 'negativeControls']:
            for original, claimed in [(1, True), (True, 1), ({'count': 1}, {'count': True}), (1, 1.0)]:
                with self.subTest(name=name, original=original, claimed=claimed):
                    self.projected_cpu_evidence()
                    self.cpu[name][0]['extra'] = original
                    self.qa['cpuEvidence'][name][0]['extra'] = claimed
                    self.qa['cpuEvidence']['resultSHA256'] = A.digest(A.canonical(self.cpu))
                    with self.assertRaisesRegex(ValueError, 'CPU_CASES_NOT_REPLAYED'):
                        self.semantic_check()

    def test_projected_report_cannot_bypass_unconfirmed_stop(self):
        self.projected_cpu_evidence()
        self.detail = self.native_fixture()
        self.runtime.stopped.side_effect = ValueError('STOP_UNCONFIRMED')
        with self.assertRaisesRegex(ValueError, 'STOP_UNCONFIRMED'):
            self.lane.run(OCC, 'inspect')
        self.assertEqual(self.official.posts, 0)

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

    def origin_fixture(self, failed_qa=True, alias=False):
        self.p["nativeSourceOrigins"] = {"enabled": True, "maxDepth": 8,
            "registryDirectory": str(pathlib.Path(self.paths["stateDirectory"]) / "source-origins")}
        for a, key in zip(self.detail["attempts"], ("produce", "independent-qa")):
            a["context"]["manifest"].update(cycleId=CYCLE, occurrenceId=OCC, step=key)
        if failed_qa:
            self.detail["occurrence"]["state"] = "attention"
            self.detail["steps"][1]["state"] = "attention"
            a = self.detail["attempts"][1]
            a["state"] = "failed"; a["receipt"] = None
            self.detail["events"] = [{"kind": "HARNESS_STOPPED", "attempt_id": a["id"], "occurrence_id": OCC,
                "payload": {"runFence": a["fence"], "confirmed": True, "unit": "agentos-run-" + a["id"],
                "proof": {"stopped": True, "mainPid": 0, "populated": False, "activeState": "inactive"}}}]
            self.qa.update(passed=False, approvedForOfficialReview=False, blockingFindings=["synthetic NameError"])
            qa_row = next(r for r in self.detail["artifacts"] if r["name"] == "qa-verification.json")
            raw = A.canonical(self.qa); qa_row.update(digest=A.digest(raw), size=len(raw))
            self.put(pathlib.Path(self.paths["artifactDirectory"]) / qa_row["digest"], raw)
        row = next(r for r in self.detail["artifacts"] if r["name"] == "candidate.py" and r["attempt_id"] == IDS[0])
        if alias:
            row = self.artifact("candidate.py", self.candidate, IDS[1])
            self.detail["artifacts"].append(row)
        return {**row, "occurrenceId": OCC, "problem": BOARD}

    def origin(self, source):
        return self.audit.source_provenance(BOARD, source["digest"], source)

    def test_failed_source_origin_keeps_quality_false_and_qa_state(self):
        source = self.origin_fixture()
        refs = self.origin(source)
        report = A.strict_json(A.pinned(refs[0]))
        self.assertEqual(report["authority"], "authorized-native-source-origin")
        self.assertFalse(report["qualityAccepted"])
        self.assertFalse(report["qa"]["reportedPassed"])
        self.assertFalse(report["qa"]["reportedApprovedForOfficialReview"])
        self.assertEqual(report["qa"]["state"], "failed")
        self.assertEqual(report["baseSHA256"], A.digest(self.source))
        self.assertEqual(self.origin(source), refs)
        self.assertEqual(self.official.posts, 0)
        self.assertFalse(list(pathlib.Path(self.paths["stateDirectory"]).glob("*-audit.json")))

    def test_qa_copy_resolves_exact_producer_and_handoff(self):
        source = self.origin_fixture(alias=True)
        report = A.strict_json(A.pinned(self.origin(source)[0]))
        self.assertEqual(report["selectedArtifactId"], source["id"])
        self.assertNotEqual(report["selectedArtifactId"], report["producerArtifactId"])
        self.assertEqual(report["identities"]["produce"]["attemptId"], IDS[0])
        path = pathlib.Path(self.paths["workerRunsDirectory"]) / IDS[1] / "home/inputs" / report["producerArtifactId"] / "candidate.py"
        self.put(path, b"wrong bytes")
        with self.assertRaisesRegex(ValueError, "SOURCE_ORIGIN_HANDOFF_CHANGED"): self.origin(source)

    def test_source_origin_rejects_arbitrary_alias_same_hash(self):
        source = self.origin_fixture()
        source["id"] = "99999999-9999-9999-9999-999999999999"
        with self.assertRaisesRegex(ValueError, "SOURCE_ORIGIN_ARTIFACT_ALIAS"): self.origin(source)

    def test_source_origin_no_live_or_uncertain_parent_reuse(self):
        source = self.origin_fixture()
        for mutation in ("live", "wrong_fence", "populated"):
            detail = copy.deepcopy(self.detail)
            if mutation == "live": self.detail["attempts"][1]["state"] = "running"
            elif mutation == "wrong_fence": self.detail["events"][0]["payload"]["runFence"] = IDS[0]
            else: self.detail["events"][0]["payload"]["proof"]["populated"] = True
            with self.subTest(mutation=mutation), self.assertRaisesRegex(ValueError, "SOURCE_ORIGIN_OWNER_UNCONFIRMED"):
                self.origin(source)
            self.detail = detail
        self.runtime.stopped.side_effect = ValueError("ACTUAL_UNIT_STILL_RUNNING")
        with self.assertRaisesRegex(ValueError, "SOURCE_ORIGIN_INCONSISTENT:ACTUAL_UNIT"): self.origin(source)

    def test_source_origin_wrong_attempt_model_and_manifest_rejected(self):
        source = self.origin_fixture()
        for key in ("account_id", "model", "number"):
            original = self.detail["attempts"][0][key]
            self.detail["attempts"][0][key] = 2 if key == "number" else "wrong"
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, "SOURCE_ORIGIN_ATTEMPT_BINDING"): self.origin(source)
            self.detail["attempts"][0][key] = original
        self.detail["attempts"][0]["context"]["manifest"]["cycleInput"]["digest"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "SOURCE_ORIGIN_INPUT_CHANGED"): self.origin(source)

    def test_source_origin_dispatch_binding_cannot_be_skipped(self):
        source = self.origin_fixture()
        directory = pathlib.Path(self.paths["watcherStateDirectory"])
        state = A.strict_json((directory / "state.json").read_bytes())
        state["experiments"][0]["supervisorOccurrenceId"] = IDS[0]
        self.put(directory / "state.json", A.canonical(state))
        with self.assertRaisesRegex(ValueError, "SOURCE_ORIGIN_INCONSISTENT:SUPERVISOR_LINEAGE"): self.origin(source)

    def test_source_origin_patch_replay_and_qa_hash_binding(self):
        source = self.origin_fixture()
        patch = next(r for r in self.detail["artifacts"] if r["name"] == "implementation.patch")
        raw = self.patch.replace(b"return x + 1", b"return x + 2")
        patch.update(digest=A.digest(raw), size=len(raw)); self.put(pathlib.Path(self.paths["artifactDirectory"]) / patch["digest"], raw)
        with self.assertRaisesRegex(ValueError, "SOURCE_ORIGIN_PATCH_CHANGED"): self.origin(source)
        patch.update(digest=A.digest(self.patch), size=len(self.patch))
        qa = next(r for r in self.detail["artifacts"] if r["name"] == "qa-verification.json")
        raw = A.canonical({**self.qa, "candidateSHA256": "0" * 64})
        qa.update(digest=A.digest(raw), size=len(raw)); self.put(pathlib.Path(self.paths["artifactDirectory"]) / qa["digest"], raw)
        with self.assertRaisesRegex(ValueError, "SOURCE_ORIGIN_QA_BINDING"): self.origin(source)

    def test_source_origin_depth_cycle_and_optional_policy(self):
        source = self.origin_fixture()
        with self.assertRaisesRegex(ValueError, "SOURCE_ORIGIN_CYCLE"):
            self.audit.source_provenance(BOARD, source["digest"], source, (OCC,))
        with self.assertRaisesRegex(ValueError, "SOURCE_ORIGIN_DEPTH"):
            self.audit.source_provenance(BOARD, source["digest"], source, tuple(str(n) for n in range(8)))
        self.p["nativeSourceOrigins"]["enabled"] = False
        with self.assertRaises(FileNotFoundError): self.origin(source)
        for change in ({"maxDepth": 9}, {"maxDepth": True}, {"registryDirectory": "/tmp/origins"}):
            p = copy.deepcopy(self.p); p["nativeSourceOrigins"].update(change)
            with self.assertRaisesRegex(ValueError, "SOURCE_ORIGIN_POLICY_SCOPE"): A.validate_policy(p, NOW, "a" * 64)

    def test_source_origin_missing_reads_are_not_kernel_repairs(self):
        source = self.origin_fixture()
        home = pathlib.Path(self.paths["workerRunsDirectory"]) / IDS[0] / "home/inputs" / IDS[0] / "cycle-input.json"
        home.unlink()
        with self.assertRaisesRegex(ValueError, "SOURCE_ORIGIN_READ_UNAVAILABLE"): self.origin(source)
        self.detail["occurrence"]["state"] = "completed"
        self.lane.audit.audit = mock.Mock(side_effect=ValueError("SOURCE_ORIGIN_READ_UNAVAILABLE"))
        row = A.scan(self.lane, self.root / "status.json", "inspect")["outcomes"][0]
        self.assertEqual(row["state"], "observation_wait")
        self.lane.audit.audit.side_effect = ValueError("SOURCE_ORIGIN_PATCH_CHANGED")
        row = A.scan(self.lane, self.root / "status.json", "inspect")["outcomes"][0]
        self.assertEqual(row["state"], "controller_blocked")
        self.assertEqual(self.official.posts, 0)

    def test_source_origin_error_cannot_disown_external_post(self):
        self.lane.run(OCC, "check")
        pathlib.Path(self.paths["registryPath"]).unlink()
        self.audit.audit = mock.Mock(side_effect=ValueError("SOURCE_ORIGIN_PATCH_CHANGED"))
        row = A.scan(self.lane, self.root / "status.json", "check")["outcomes"][0]
        self.assertEqual(row["state"], "check_running")
        self.assertEqual(self.official.posts, 1)

    def test_source_origin_does_not_relax_final_candidate_qa(self):
        self.origin_fixture(failed_qa=False)
        self.qa["approvedForOfficialReview"] = False
        row = next(r for r in self.detail["artifacts"] if r["name"] == "qa-verification.json")
        raw = A.canonical(self.qa); row.update(size=len(raw), digest=A.digest(raw))
        self.put(pathlib.Path(self.paths["artifactDirectory"]) / row["digest"], raw)
        with self.assertRaisesRegex(ValueError, "QA_NOT_APPROVED"): self.audit.audit(OCC)
        self.assertEqual(self.official.posts, 0)

    def derived_origin_fixture(self):
        """A real two-occurrence fixture: rejected parent -> corrected child."""
        self.origin_fixture()
        parent_id = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
        mapping = {OCC: parent_id, CYCLE: "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                   IDS[0]: "cccccccc-cccc-cccc-cccc-cccccccccccc", IDS[1]: "dddddddd-dddd-dddd-dddd-dddddddddddd"}
        for i, row in enumerate(self.detail["artifacts"]): mapping[row["id"]] = f"eeeeeeee-eeee-eeee-eeee-{i:012d}"
        raw = A.canonical(self.detail).decode()
        for old, new in mapping.items(): raw = raw.replace(old, new)
        parent = A.strict_json(raw)
        for old, new in zip(self.detail["attempts"], parent["attempts"]):
            home = pathlib.Path(self.paths["workerRunsDirectory"]) / new["id"] / "home"
            input_raw = A.canonical(parent["occurrence"]["input"])
            self.put(home / "inputs" / new["id"] / "cycle-input.json", input_raw)
            new["context"]["manifest"]["cycleInput"].update(digest=A.digest(input_raw), size=len(input_raw))
        parent_qa_home = pathlib.Path(self.paths["workerRunsDirectory"]) / mapping[IDS[1]] / "home"
        for row in parent["artifacts"]:
            if row["attempt_id"] == mapping[IDS[0]]:
                self.put(parent_qa_home / "inputs" / row["id"] / row["name"], (pathlib.Path(self.paths["artifactDirectory"]) / row["digest"]).read_bytes())
        parent_key = WATCH + "-experiment-" + "e" * 32 + "-run"
        parent["occurrence"]["source_key"] = "manual:" + parent_key
        directory = pathlib.Path(self.paths["watcherStateDirectory"])
        def receipts(detail, key):
            o = detail["occurrence"]
            for kind, k, result, body in (("run", key, o, {"requestKey": key, "input": o["input"]}),
                    ("create", key[:-4], {"id": o["cycle_id"], "definition": o["definition"]}, {"requestKey": key[:-4], "definition": o["definition"]})):
                self.put(directory / ("intent-" + A.digest(k) + ".json"), A.canonical({"state": "recorded", "kind": kind,
                    "requestKey": k, "requestDigest": A.digest(body), "result": result}))
        receipts(parent, parent_key)
        self.source = self.candidate; self.candidate = b"def kernel(x):\n    return x + 2\n"
        self.patch = "".join(difflib.unified_diff(self.source.decode().splitlines(keepends=True), self.candidate.decode().splitlines(keepends=True), fromfile="base-candidate.py", tofile="candidate.py")).encode()
        self.cpu["candidateSHA256"] = A.digest(self.candidate)
        self.verify.update(sourceSHA256=A.digest(self.source), candidateSHA256=A.digest(self.candidate))
        self.qa.update(self.verify); self.qa.update(passed=True, approvedForOfficialReview=True, blockingFindings=[])
        self.qa["cpuEvidence"]["resultSHA256"] = A.digest(A.canonical(self.cpu))
        self.detail = self.native_fixture()
        source = next(r for r in parent["artifacts"] if r["name"] == "candidate.py" and r["attempt_id"] == mapping[IDS[0]])
        self.detail["occurrence"]["input"]["experiment"]["sourceArtifact"] = {**source, "occurrenceId": parent_id, "problem": BOARD}
        for a, key in zip(self.detail["attempts"], ("produce", "independent-qa")):
            raw = A.canonical(self.detail["occurrence"]["input"])
            self.put(pathlib.Path(self.paths["workerRunsDirectory"]) / a["id"] / "home/inputs" / a["id"] / "cycle-input.json", raw)
            a["context"]["manifest"].update(cycleId=CYCLE, occurrenceId=OCC, step=key)
            a["context"]["manifest"]["cycleInput"].update(digest=A.digest(raw), size=len(raw))
        receipts(self.detail, self.detail["occurrence"]["source_key"][7:])
        state = A.strict_json((directory / "state.json").read_bytes())
        state["experiments"].append({"occurrenceId": parent_id, "cycleId": parent["occurrence"]["cycle_id"], "problem": BOARD, "supervisorOccurrenceId": SUPERVISOR})
        self.put(directory / "state.json", A.canonical(state))
        self.runtime.api = lambda method, path: copy.deepcopy(parent if path.endswith(parent_id) else self.detail)
        return parent

    def test_corrected_child_passes_own_qa_without_accepting_parent(self):
        self.derived_origin_fixture()
        report, _, candidate = self.audit.audit(OCC)
        parent = A.strict_json(A.pinned(report["provenance"][0]))
        self.assertEqual(candidate, self.candidate)
        self.assertFalse(parent["qualityAccepted"])
        self.assertFalse(parent["qa"]["reportedPassed"])
        self.assertEqual(report["authority"], "delegated-policy-and-independent-native-QA")
        self.assertEqual(self.official.posts, 0)

    def test_source_origin_walks_two_hops_to_seed_and_rechecks_parent(self):
        parent = self.derived_origin_fixture()
        row = next(r for r in self.detail["artifacts"] if r["name"] == "candidate.py")
        source = {**row, "occurrenceId": OCC, "problem": BOARD}
        refs = self.origin(source)
        self.assertEqual(len(refs), 3)
        self.assertEqual(A.strict_json(A.pinned(refs[0]))["baseSHA256"], A.digest(self.source))
        self.assertFalse(A.strict_json(A.pinned(refs[1]))["qa"]["reportedPassed"])
        parent["attempts"][1]["state"] = "running"
        with self.assertRaisesRegex(ValueError, "SOURCE_ORIGIN_OWNER_UNCONFIRMED"): self.origin(source)

    def test_source_origin_registry_protection_before_writing(self):
        source = self.origin_fixture()
        directory = pathlib.Path(self.p["nativeSourceOrigins"]["registryDirectory"])
        outside = self.root / "wrong-origin-directory"; outside.mkdir(mode=0o700)
        directory.symlink_to(outside)
        with self.assertRaisesRegex(ValueError, "SOURCE_ORIGIN_REGISTRY_SCOPE"): self.origin(source)
        self.assertEqual(list(outside.iterdir()), [])

    def failed_producer_without_qa(self, detail):
        producer = detail["attempts"][0]
        producer["state"] = "failed"; producer["receipt"] = None
        detail["attempts"] = [producer]
        detail["occurrence"]["state"] = "attention"
        detail["steps"][0]["state"] = "attention"; detail["steps"][1]["state"] = "queued"
        detail["artifacts"] = [r for r in detail["artifacts"] if r["attempt_id"] == producer["id"]]
        detail["events"] = [{"kind": "HARNESS_STOPPED", "attempt_id": producer["id"], "occurrence_id": detail["occurrence"]["id"],
            "payload": {"runFence": producer["fence"], "confirmed": True, "unit": "agentos-run-" + producer["id"],
            "proof": {"stopped": True, "mainPid": 0, "populated": False, "activeState": "inactive"}}}]
        for row in detail["artifacts"]:
            if row["name"] in {"verification.json", "cpu-result.json"}:
                value = A.strict_json((pathlib.Path(self.paths["artifactDirectory"]) / row["digest"]).read_bytes())
                value["passed"] = False
                raw = A.canonical(value); row.update(digest=A.digest(raw), size=len(raw))
                self.put(pathlib.Path(self.paths["artifactDirectory"]) / row["digest"], raw)

    def test_failed_producer_origin_records_qa_not_run_and_negative_cpu(self):
        source = self.origin_fixture()
        self.failed_producer_without_qa(self.detail)
        report = A.strict_json(A.pinned(self.origin(source)[0]))
        self.assertEqual(report["identities"]["produce"]["state"], "failed")
        self.assertEqual(report["qa"], {"state": "not_run", "artifactId": None, "sha256": None,
            "reportedPassed": None, "reportedApprovedForOfficialReview": None})
        self.assertEqual([r["reportedPassed"] for r in report["producerReports"]], [False, False])
        self.assertFalse(report["qualityAccepted"])
        self.assertEqual(self.runtime.stopped.call_count, 1)
        self.assertEqual(self.official.posts, 0)

    def test_failed_producer_missing_optional_reports_stays_unaccepted(self):
        source = self.origin_fixture(); self.failed_producer_without_qa(self.detail)
        self.detail["artifacts"] = [r for r in self.detail["artifacts"] if r["name"] not in {"verification.json", "cpu-result.json", "cpu-checks.py"}]
        report = A.strict_json(A.pinned(self.origin(source)[0]))
        self.assertEqual([r["state"] for r in report["producerReports"]], ["not_published", "not_published"])
        self.assertFalse(report["qualityAccepted"])
        self.detail["artifacts"] = [r for r in self.detail["artifacts"] if r["name"] != "implementation.patch"]
        with self.assertRaisesRegex(ValueError, "SOURCE_ORIGIN_ARTIFACT_SET"): self.origin(source)

    def test_failed_producer_cannot_hide_qa_attempt_or_claim_qa_alias(self):
        source = self.origin_fixture(alias=True)
        hidden_qa = copy.deepcopy(self.detail["attempts"][1]); hidden_qa["state"] = "running"
        self.failed_producer_without_qa(self.detail)
        with self.assertRaisesRegex(ValueError, "SOURCE_ORIGIN_ARTIFACT_ALIAS"): self.origin(source)
        producer_row = next(r for r in self.detail["artifacts"] if r["name"] == "candidate.py")
        source = {**producer_row, "occurrenceId": OCC, "problem": BOARD}
        self.detail["attempts"].append(hidden_qa)
        with self.assertRaisesRegex(ValueError, "SOURCE_ORIGIN_OWNER_UNCONFIRMED"): self.origin(source)
        self.detail["attempts"].pop()
        self.detail["steps"][1]["state"] = "running"
        with self.assertRaisesRegex(ValueError, "SOURCE_ORIGIN_QA_NOT_RUN_UNPROVEN"): self.origin(source)

    def test_failed_producer_needs_physical_stop_and_report_bindings(self):
        source = self.origin_fixture(); self.failed_producer_without_qa(self.detail)
        self.detail["events"][0]["payload"]["runFence"] = IDS[1]
        with self.assertRaisesRegex(ValueError, "SOURCE_ORIGIN_OWNER_UNCONFIRMED"): self.origin(source)
        self.detail["events"][0]["payload"]["runFence"] = self.detail["attempts"][0]["fence"]
        row = next(r for r in self.detail["artifacts"] if r["name"] == "cpu-result.json")
        value = A.strict_json((pathlib.Path(self.paths["artifactDirectory"]) / row["digest"]).read_bytes()); value["candidateSHA256"] = "0" * 64
        raw = A.canonical(value); row.update(digest=A.digest(raw), size=len(raw));self.put(pathlib.Path(self.paths["artifactDirectory"]) / row["digest"], raw)
        with self.assertRaisesRegex(ValueError, "SOURCE_ORIGIN_REPORT_BINDING"): self.origin(source)

    def test_completed_producer_must_wait_for_qa_instead_of_not_run(self):
        source = self.origin_fixture(); self.failed_producer_without_qa(self.detail)
        self.detail["attempts"][0]["state"] = self.detail["steps"][0]["state"] = "completed"
        with self.assertRaisesRegex(ValueError, "SOURCE_ORIGIN_QA_NOT_RUN_UNPROVEN"): self.origin(source)

    def test_corrected_child_of_failed_producer_needs_own_full_qa(self):
        parent = self.derived_origin_fixture(); self.failed_producer_without_qa(parent)
        report, _, _ = self.audit.audit(OCC)
        origin = A.strict_json(A.pinned(report["provenance"][0]))
        self.assertFalse(origin["qualityAccepted"])
        self.assertEqual(origin["qa"]["state"], "not_run")
        self.assertEqual(report["authority"], "delegated-policy-and-independent-native-QA")
        self.detail["attempts"][1]["receipt"]["stopped"] = False
        with self.assertRaisesRegex(ValueError, "ATTEMPT_NOT_STOPPED"): self.audit.audit(OCC)
        self.assertEqual(self.official.posts, 0)

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

    def padded_watcher_state(self, size):
        path = pathlib.Path(self.paths["watcherStateDirectory"]) / "state.json"
        state = A.strict_json(path.read_bytes())
        state["supervisions"] = [{"input": {"preservedContext": ""}}]
        overhead = len(A.canonical(state))
        state["supervisions"][0]["input"]["preservedContext"] = "x" * (size - overhead)
        raw = A.canonical(state)
        self.assertEqual(len(raw), size)
        self.put(path, raw)
        return path, A.digest(raw)

    def test_large_state_scan_and_dispatch_preserve_history_and_do_not_post(self):
        path, before = self.padded_watcher_state(17 * 1024 * 1024)
        with self.assertRaisesRegex(ValueError, "PROTECTED_FILE"):
            A.protected(path)  # Generic policy/helper/intent ceiling stays 16 MiB.
        entry, receipts = self.audit.dispatch(self.detail["occurrence"])
        self.assertEqual(entry["occurrenceId"], OCC)
        self.assertEqual(set(receipts), {"run", "create"})
        result = A.scan(self.lane, self.root / "status.json", "inspect")
        self.assertEqual(result["outcomes"][0]["state"], "eligible")
        self.assertEqual(A.digest(path.read_bytes()), before)
        self.assertEqual(self.official.posts, 0)

    def test_watcher_state_accepts_exact_64_mib(self):
        path, before = self.padded_watcher_state(64 * 1024 * 1024)
        state = A.watcher_state(self.p)
        self.assertEqual(state["watchId"], WATCH)
        self.assertEqual(state["experiments"][0]["occurrenceId"], OCC)
        self.assertEqual(A.digest(path.read_bytes()), before)
        self.assertEqual(self.official.posts, 0)

    def test_watcher_state_rejects_64_mib_plus_one_before_parse(self):
        path = pathlib.Path(self.paths["watcherStateDirectory"]) / "state.json"
        with path.open("wb") as stream:
            stream.truncate(64 * 1024 * 1024 + 1)
        with mock.patch.object(A, "strict_json", side_effect=AssertionError("must bound before parse")):
            with self.assertRaisesRegex(ValueError, "PROTECTED_FILE"):
                A.watcher_state(self.p)
        self.assertEqual(self.official.posts, 0)

    def test_large_state_preserves_dispatch_and_qa_authorities(self):
        self.padded_watcher_state(17 * 1024 * 1024)
        wrong = copy.deepcopy(self.detail["occurrence"])
        wrong["source_key"] += "-changed"
        with self.assertRaisesRegex(ValueError, "DISPATCH_SOURCE_KEY"):
            self.audit.dispatch(wrong)
        qa = next(row for row in self.detail["artifacts"] if row["name"] == "qa-verification.json")
        path = pathlib.Path(self.paths["artifactDirectory"]) / qa["digest"]
        path.write_bytes(path.read_bytes() + b" ")
        result = A.scan(self.lane, self.root / "status.json", "inspect")
        self.assertEqual(result["outcomes"][0]["code"], "ARTIFACT_BYTES_CHANGED")
        self.assertEqual(self.official.posts, 0)

    def test_watcher_state_preserves_file_and_parent_modes(self):
        path = pathlib.Path(self.paths["watcherStateDirectory"]) / "state.json"
        for target in [path, path.parent]:
            before = target.stat().st_mode & 0o777
            try:
                target.chmod(before | 0o020)
                with self.assertRaisesRegex(ValueError, "PROTECTED_OWNER_MODE"):
                    A.watcher_state(self.p)
            finally:
                target.chmod(before)

    def test_watcher_state_preserves_owner_and_symlink_guards(self):
        path = pathlib.Path(self.paths["watcherStateDirectory"]) / "state.json"
        try:
            os.chown(path, 65534, -1)
            with self.assertRaisesRegex(ValueError, "PROTECTED_OWNER_MODE"):
                A.watcher_state(self.p)
        finally:
            os.chown(path, 0, -1)
        moved = path.with_name("saved-state.json")
        path.rename(moved); path.symlink_to(moved)
        with self.assertRaisesRegex(ValueError, "PROTECTED_PATH"):
            A.watcher_state(self.p)

    def test_watcher_state_preserves_strict_json_and_scope(self):
        path = pathlib.Path(self.paths["watcherStateDirectory"]) / "state.json"
        for raw, error in [
            (b'{"watchId":"a","watchId":"b"}', "JSON_DUPLICATE_KEY"),
            (b'{"value":NaN}', "JSON_NONFINITE"),
            (b'[]', "WATCH_STATE_SCHEMA"),
            (A.canonical({"watchId": "other", "experiments": []}), "WATCH_STATE_SCOPE"),
            (A.canonical({"watchId": WATCH, "experiments": {}}), "WATCH_STATE_SCHEMA"),
            (A.canonical({"watchId": WATCH, "experiments": [None]}), "WATCH_STATE_SCHEMA"),
        ]:
            with self.subTest(error=error):
                self.put(path, raw)
                with self.assertRaisesRegex(ValueError, error):
                    A.scan(self.lane, self.root / "status.json", "inspect")
        self.assertEqual(self.official.posts, 0)

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


class ScanSelectionTests(unittest.TestCase):
    def setUp(self):
        self.fixture = Fixture(); self.fixture.setUp(); self.addCleanup(self.fixture.tearDown)
        self.root, self.p = self.fixture.root, self.fixture.p
        self.status = self.root / "fair-status.json"
        self.rows = [{"occurrenceId": "aaaaaaaa-aaaa-aaaa-aaaa-" + str(i).zfill(12), "problem": BOARD}
                     for i in range(1, 206)]
        self.calls, self.registered = [], {}
        def run(oid, mode):
            self.calls.append(oid)
            return {"state": "eligible", "postAttempted": False}
        self.lane = types.SimpleNamespace(p=self.p, policy_sha="b"*64, io=IO,
            registered_occurrences=lambda: self.registered,
            audit=types.SimpleNamespace(diagnostic_binding=lambda oid: {
                "candidateSHA256": None, "qaArtifactId": None, "qaSHA256": None}),
            run=run, clock=lambda: NOW, state=pathlib.Path(self.p["stateDirectory"]),
            legacy=pathlib.Path(self.p["legacyLedger"]))

    def state(self, rows):
        path = pathlib.Path(self.p["watcherStateDirectory"]) / "state.json"
        IO.atomic_record(path, {"watchId": WATCH, "experiments": rows})
        return path

    def job(self, row):
        return {"id": "0123456789abcdef", "problem": BOARD, "sourceSHA256": "c"*64,
                "qaArtifactId": IDS[0], "qaSHA256": "d"*64}

    def test_old_unregistered_owners_are_not_dropped_by_last_hundred(self):
        rows = self.rows[:119]
        self.registered.update({r["occurrenceId"]:self.job(r) for r in rows[19:49]})
        path = self.state(rows); before = path.read_bytes()
        result = A.scan(self.lane, self.status, "inspect")
        self.assertEqual(result["scanSelection"]["unregisteredCandidates"], 89)
        self.assertEqual(len(self.calls), 89); self.assertEqual(len(result["outcomes"]), 100)
        self.assertTrue({rows[i]["occurrenceId"] for i in [1,5,16]}.issubset(self.calls))
        self.assertEqual(path.read_bytes(), before)
        self.assertFalse(any(r["postAttempted"] for r in result["outcomes"]))

    def test_round_robin_reaches_every_pending_owner_without_raising_limit(self):
        self.state(self.rows)
        for _ in range(3):
            result = A.scan(self.lane, self.status, "inspect")
            self.assertEqual(len(result["outcomes"]), 100)
            self.assertEqual(result["scanSelection"]["candidateLimit"], 100)
        self.assertEqual(set(self.calls), {r["occurrenceId"] for r in self.rows})
        self.assertEqual(self.calls[:100], [r["occurrenceId"] for r in self.rows[:100]])
        self.assertEqual(self.calls[100:200], [r["occurrenceId"] for r in self.rows[100:200]])

    def test_cursor_survives_last_owner_becoming_registered_and_new_append(self):
        self.state(self.rows[:101]); A.scan(self.lane, self.status, "inspect")
        self.registered[self.rows[99]["occurrenceId"]] = self.job(self.rows[99])
        self.state(self.rows[:103]); self.calls.clear()
        A.scan(self.lane, self.status, "inspect")
        self.assertEqual(self.calls[:3], [r["occurrenceId"] for r in self.rows[100:103]])
        self.assertNotIn(self.rows[99]["occurrenceId"], self.calls)

    def test_deadline_cursor_advances_only_through_finished_inspections(self):
        self.state(self.rows[:3])
        with mock.patch.object(A.time, "monotonic", side_effect=[0, 1, 46]):
            result = A.scan(self.lane, self.status, "inspect")
        self.assertTrue(result["scanBudgetExhausted"])
        self.assertEqual(result["scanSelection"]["lastUnregisteredOccurrenceId"], self.rows[0]["occurrenceId"])
        self.calls.clear(); A.scan(self.lane, self.status, "inspect")
        self.assertEqual(self.calls[0], self.rows[1]["occurrenceId"])

    def test_cursor_is_policy_scoped_and_malformed_hint_fails_before_audit(self):
        self.state(self.rows)
        A.scan(self.lane, self.status, "inspect")
        previous = A.strict_json(self.status.read_bytes()); previous["policySHA256"] = "e"*64
        IO.atomic_record(self.status, previous); self.calls.clear()
        A.scan(self.lane, self.status, "inspect")
        self.assertEqual(self.calls[0], self.rows[0]["occurrenceId"])
        previous = A.strict_json(self.status.read_bytes())
        previous["scanSelection"]["lastUnregisteredOccurrenceId"] = {"not": "an identity"}
        IO.atomic_record(self.status, previous); self.calls.clear()
        with self.assertRaisesRegex(ValueError, "SCAN_CURSOR_SCOPE"):
            A.scan(self.lane, self.status, "inspect")
        self.assertEqual(self.calls, [])

    def test_status_write_crash_keeps_prior_cursor_and_repeats_only_read_inspection(self):
        self.state(self.rows)
        A.scan(self.lane, self.status, "inspect")
        before = self.status.read_bytes(); self.calls.clear()
        with mock.patch.object(IO, "atomic_record", side_effect=OSError("isolated failure")):
            with self.assertRaises(OSError): A.scan(self.lane, self.status, "inspect")
        failed = list(self.calls); self.assertEqual(self.status.read_bytes(), before)
        self.calls.clear(); A.scan(self.lane, self.status, "inspect")
        self.assertEqual(self.calls, failed)

    def test_registered_rows_are_filler_and_never_reaudited(self):
        self.state(self.rows[:3]); self.registered.update({r["occurrenceId"]:self.job(r) for r in self.rows[1:3]})
        result = A.scan(self.lane, self.status, "inspect")
        self.assertEqual(self.calls, [self.rows[0]["occurrenceId"]])
        self.assertEqual(sum(r["state"] == "check_running" for r in result["outcomes"]), 2)
        self.assertTrue(all(r.get("jobId") == "0123456789abcdef" for r in result["outcomes"] if r["state"] == "check_running"))

    def test_unknown_result_keeps_ownership_and_rotates_without_resubmission(self):
        self.state(self.rows[:2])
        def unknown(oid, mode):
            self.calls.append(oid)
            raise ValueError("UNCERTAIN_RECONCILE_NO_RESEND")
        self.lane.run = unknown
        first = A.scan(self.lane, self.status, "inspect")
        second = A.scan(self.lane, self.status, "inspect")
        self.assertTrue(all(r["state"] == "uncertain_owned" and r["postAttempted"] is None
                            for d in [first, second] for r in d["outcomes"]))
        self.assertEqual(len(self.calls), 4)

    def test_real_preflight_quota_has_no_post_marker_but_untyped_error_does_not(self):
        self.fixture.official.left = 0
        first = A.scan(self.fixture.lane, self.status, "check")["outcomes"][0]
        self.assertEqual((first["state"], first["code"], first["postAttempted"]),
                         ("quota_wait", "CHECK_QUOTA", False))
        self.assertEqual(self.fixture.official.posts, 0)
        self.fixture.lane.run = mock.Mock(side_effect=ValueError("CHECK_QUOTA"))
        second = A.scan(self.fixture.lane, self.status, "check")["outcomes"][0]
        self.assertIsNone(second["postAttempted"])

    def test_global_unknown_effect_dominates_other_candidate_quota_failure(self):
        self.fixture.official.left = 0
        path = pathlib.Path(self.p["stateDirectory"]) / (BOARD + "-" + "f"*64 + "-check.json")
        IO.atomic_record(path, {"state":"submission_outcome_unknown", "identity":{"sourceSha256":"f"*64}})
        before = path.read_bytes()
        row = A.scan(self.fixture.lane, self.status, "check")["outcomes"][0]
        self.assertEqual(row["state"], "uncertain_owned")
        self.assertEqual(row["code"], "ACCOUNT_HAS_UNCERTAIN_EFFECT")
        self.assertIsNone(row["postAttempted"])
        self.assertEqual(path.read_bytes(), before); self.assertEqual(self.fixture.official.posts, 0)

    def test_local_durable_effect_overrides_even_a_typed_quota_exception(self):
        source = A.digest(self.fixture.candidate)
        path = pathlib.Path(self.p["stateDirectory"]) / (BOARD + "-" + source + "-check.json")
        IO.atomic_record(path, {"state":"submission_outcome_unknown", "identity":{"sourceSha256":source}})
        self.fixture.lane.run = mock.Mock(side_effect=A.PreflightQuotaError("CHECK_QUOTA"))
        row = A.scan(self.fixture.lane, self.status, "check")["outcomes"][0]
        self.assertEqual(row["state"], "uncertain_owned"); self.assertIsNone(row["postAttempted"])
        self.assertEqual(self.fixture.official.posts, 0)

    def test_malformed_quota_is_not_authenticated_quota_exhaustion(self):
        original = self.fixture.official.fetch
        def invalid(path, public=False):
            result = original(path, public)
            if path == "problems": result["problems"][0]["limits"]["checks"]["left"] = True
            return result
        self.fixture.official.fetch = invalid
        row = A.scan(self.fixture.lane, self.status, "check")["outcomes"][0]
        self.assertEqual(row["code"], "CHECK_QUOTA_SCHEMA")
        self.assertEqual(row["state"], "controller_blocked"); self.assertIsNone(row["postAttempted"])
        self.assertEqual(self.fixture.official.posts, 0)


if __name__ == "__main__":
    unittest.main()
