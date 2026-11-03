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

    def projected_repair_fixture(self):
        prior_id = '99999999-9999-4999-8999-999999999999'
        prior_cycle = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'
        prior_attempt = 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb'
        prior_fence = 'cccccccc-cccc-4ccc-8ccc-cccccccccccc'
        prior_input = copy.deepcopy(self.detail['occurrence']['input'])
        prior_raw = A.canonical(prior_input)
        original = {**copy.deepcopy(self.qa), 'historicalMeasurement': 1.0}
        raw = A.canonical(original)
        ref = self.artifact('qa-verification.json', raw, prior_attempt)
        candidate = self.artifact('candidate.py', self.candidate, prior_attempt)
        projection_ref = {k: ref[k] for k in ['id','name','digest','size']}
        spec = copy.deepcopy(self.detail['occurrence']['definition']['steps'][1])
        old_occ = {'id':prior_id,'cycle_id':prior_cycle,'state':'completed',
            'source_key':'manual:'+WATCH+'-experiment-prior-run','input':prior_input,
            'definition':copy.deepcopy(self.detail['occurrence']['definition'])}
        old_attempt = {'id':prior_attempt,'number':1,'step_id':'prior-qa','fence':prior_fence,'state':'completed',
            'finished_at':'2026-01-01T00:00:00Z','context':{'manifest':{'cycleId':prior_cycle,'occurrenceId':prior_id,
            'step':'independent-qa','cycleInput':{'attemptId':prior_attempt,'occurrenceId':prior_id,
            'runFence':prior_fence,'digest':A.digest(prior_raw),'size':len(prior_raw)}}}}
        self.prior_detail = {'occurrence':old_occ,'steps':[{'id':'prior-qa','key':'independent-qa','attempt':1,
            'definition':spec}],'attempts':[old_attempt],'artifacts':[ref,candidate]}
        self.put(pathlib.Path(self.paths['workerRunsDirectory'])/prior_attempt/'home/inputs'/prior_attempt/'cycle-input.json',prior_raw)
        directory=pathlib.Path(self.paths['watcherStateDirectory'])
        state=A.strict_json((directory/'state.json').read_bytes())
        state['experiments'][0].update(repairOf=prior_id,repairCandidateSHA256=A.digest(self.candidate))
        state['experiments'].append({'occurrenceId':prior_id,'cycleId':prior_cycle,'problem':BOARD,
                                    'supervisorOccurrenceId':SUPERVISOR})
        self.put(directory/'state.json',A.canonical(state))
        key=old_occ['source_key'][len('manual:'):]
        for kind,rkey,result,body in [('run',key,old_occ,{'requestKey':key,'input':prior_input}),
                ('create',key[:-4],{'id':prior_cycle,'definition':old_occ['definition']},
                 {'requestKey':key[:-4],'definition':old_occ['definition']})]:
            self.put(directory/('intent-'+A.digest(rkey)+'.json'),A.canonical({'state':'recorded','kind':kind,
                'requestKey':rkey,'requestDigest':A.digest(body),'result':result}))
        selected=A.request_numbers_v1({k:original[k] for k in ['candidateSHA256','sourceSHA256','passed','historicalMeasurement']})
        diagnostic={'version':1,'authority':'prior_qa_diagnostic_only_not_current_acceptance','currentAcceptance':False,
            'bindingState':'verified','problem':BOARD,'occurrenceId':prior_id,'stepKey':'independent-qa',
            'stepId':'prior-qa','attemptId':prior_attempt,'runFence':prior_fence,'cycleInputSHA256':A.digest(prior_raw),
            'artifact':projection_ref,'candidateSHA256':A.digest(self.candidate),'sourceSHA256':A.digest(self.source),
            'report':selected,'reportProjection':{'version':1,'kind':'bounded_historical_qa','complete':False,
                'originalArtifact':projection_ref,'reportSHA256':A.digest(original),'reportBytes':len(A.canonical(original)),
                'reportHashDomain':'original_strict_json_canonical','selectedReportCanonicalization':'json-numbers-v1',
                'selectedReportSHA256':A.digest(selected)}}
        experiment=self.detail['occurrence']['input']['experiment']
        experiment.update(repairQADiagnostic=diagnostic,repairTarget={'occurrenceId':prior_id,
            'candidateSHA256':A.digest(self.candidate),'qaArtifactId':ref['id'],'qaSHA256':ref['digest']})
        experiment['materials'].append({'name':'prior-qa-original.json','artifactId':ref['id'],'attemptId':prior_attempt,
            'sha256':ref['digest'],'bytes':ref['size'],'base64':base64.b64encode(raw).decode()})
        self.runtime.api=lambda method,path:copy.deepcopy(self.prior_detail if path.endswith(prior_id) else self.detail)
        self.refresh_current_input()
        return experiment,ref

    def refresh_current_input(self):
        occurrence=self.detail['occurrence'];key=occurrence['source_key'][len('manual:'):]
        raw=A.canonical(occurrence['input'])
        self.put(pathlib.Path(self.paths['watcherStateDirectory'])/('intent-'+A.digest(key)+'.json'),
            A.canonical({'state':'recorded','kind':'run','requestKey':key,'result':occurrence,
                         'requestCanonicalization':'json-numbers-v1','requestDigest':A.digest({'requestKey':key,'input':occurrence['input']})}))
        for attempt in self.detail['attempts']:
            self.put(pathlib.Path(self.paths['workerRunsDirectory'])/attempt['id']/'home/inputs'/attempt['id']/'cycle-input.json',raw)
            attempt['context']['manifest']['cycleInput'].update(digest=A.digest(raw),size=len(raw))

    def test_projected_historical_material_full_audit_preserves_canonical_and_numeric_domains(self):
        experiment,ref=self.projected_repair_fixture()
        self.assertEqual(experiment['repairQADiagnostic']['report']['historicalMeasurement'],1)
        self.assertIs(type(experiment['repairQADiagnostic']['report']['historicalMeasurement']),int)
        self.assertEqual(self.lane.run(OCC,'inspect')['state'],'eligible')
        self.assertEqual(self.official.posts,0)
        self.assertEqual(self.runtime.stopped.call_count,3)

    def test_historical_material_never_allows_arbitrary_extras_or_normal_experiment(self):
        experiment,_=self.projected_repair_fixture()
        original=copy.deepcopy(experiment)
        for mutation in ['extra','normal','unprojected']:
            with self.subTest(mutation=mutation):
                experiment.clear();experiment.update(copy.deepcopy(original))
                if mutation=='extra':experiment['materials'].append({'name':'unexpected.bin','sha256':A.digest(b'x'),'bytes':1,'base64':'eA=='})
                elif mutation=='normal':experiment.pop('repairTarget')
                else:experiment['repairQADiagnostic'].pop('reportProjection')
                self.refresh_current_input()
                with self.assertRaisesRegex(ValueError,'MATERIAL_SET_CHANGED|REPAIR_DIAGNOSTIC_'):
                    self.audit.audit(OCC)

    def test_historical_material_binds_artifact_attempt_projection_candidate_and_source(self):
        experiment,_=self.projected_repair_fixture();original=copy.deepcopy(experiment)
        mutations=[('material-artifact',lambda e:e['materials'][-1].update(artifactId=OCC)),
            ('material-attempt',lambda e:e['materials'][-1].update(attemptId=IDS[0])),
            ('material-size-bool',lambda e:e['materials'][-1].update(bytes=True)),
            ('original-artifact',lambda e:e['repairQADiagnostic']['reportProjection'].update(originalArtifact={})),
            ('projection-hash',lambda e:e['repairQADiagnostic']['reportProjection'].update(selectedReportSHA256='f'*64)),
            ('original-hash',lambda e:e['repairQADiagnostic']['reportProjection'].update(reportSHA256='f'*64)),
            ('selected-value',lambda e:e['repairQADiagnostic']['report'].update(historicalMeasurement=2)),
            ('source',lambda e:e['repairQADiagnostic'].update(sourceSHA256='f'*64)),
            ('candidate',lambda e:e['repairTarget'].update(candidateSHA256='f'*64)),
            ('scope',lambda e:e['repairQADiagnostic'].update(problem='other-board')),
            ('fence',lambda e:e['repairQADiagnostic'].update(runFence=IDS[0])),
            ('input-binding',lambda e:e['repairQADiagnostic'].update(cycleInputSHA256='f'*64))]
        for name,mutate in mutations:
            with self.subTest(name=name):
                experiment.clear();experiment.update(copy.deepcopy(original));mutate(experiment);self.refresh_current_input()
                with self.assertRaisesRegex(ValueError,'MATERIAL_CHANGED|REPAIR_DIAGNOSTIC_'):
                    self.audit.audit(OCC)

    def test_historical_material_checks_raw_bytes_and_prior_registered_emitter(self):
        experiment,ref=self.projected_repair_fixture()
        path=pathlib.Path(self.paths['artifactDirectory'])/ref['digest'];raw=path.read_bytes()
        self.put(path,raw+b' ')
        with self.assertRaisesRegex(ValueError,'ARTIFACT_BYTES_CHANGED'):self.audit.audit(OCC)
        self.put(path,raw)
        self.prior_detail['artifacts'][0]['attempt_id']=IDS[0]
        with self.assertRaisesRegex(ValueError,'REPAIR_DIAGNOSTIC_ARTIFACT_OWNER'):self.audit.audit(OCC)

    def test_historical_material_never_relaxes_current_qa_or_canonical_hashes(self):
        experiment,_=self.projected_repair_fixture()
        self.replace_evidence('qa-verification.json',A.canonical({**self.qa,'passed':False}),IDS[1])
        with self.assertRaisesRegex(ValueError,'QA_SOURCE_OR_SCOPE_MISMATCH'):self.audit.audit(OCC)
        self.replace_evidence('qa-verification.json',A.canonical(self.qa),IDS[1])
        material=next(x for x in experiment['materials'] if x['name']=='canonical-TASK.md')
        material.update(base64=base64.b64encode(b'changed').decode(),bytes=7,sha256=A.digest(b'changed'))
        self.refresh_current_input()
        with self.assertRaisesRegex(ValueError,'CANONICAL_MATERIAL_CHANGED'):self.audit.audit(OCC)

    def test_source_origin_applies_same_diagnostic_material_guard(self):
        experiment,_=self.projected_repair_fixture()
        source=self.origin_fixture(failed_qa=False)
        self.assertFalse(A.strict_json(A.pinned(self.origin(source)[0]))['qualityAccepted'])
        experiment['repairQADiagnostic']['reportProjection']['complete']=True
        self.refresh_current_input()
        with self.assertRaisesRegex(ValueError,'REPAIR_DIAGNOSTIC_PROJECTION'):
            self.origin(source)

    def test_source_screen_requires_review_for_encoded_payloads_and_binary_loaders(self):
        snippets=[
            'import base64 as b\npayload = b.b64decode("AA==")',
            'import zlib as z\npayload = z.decompress(b"bytes")',
            'import codecs as c\npayload = c.decode("00", "hex")',
            'from torch.ops import load_library as f\nf("kernel.so")',
            'torch.ops.load_library("kernel.so")',
            'cuda.cuModuleLoadData(b"prebuilt cubin")',
            'from cuda import cuModuleLoadData as f\nf(b"prebuilt cubin")',
            'cuda.cuLibraryLoadFromFile("kernel.so")',
            'module_from_buffer(b"prebuilt cubin")',
            'loader(bytes.fromhex("00ab"))',
        ]
        for snippet in snippets:
            with self.subTest(snippet=snippet):
                with self.assertRaisesRegex(ValueError,'SOURCE_REQUIRES_MANUAL_REVIEW'):
                    A.source_screen(snippet.encode())

    def test_source_screen_checks_aliased_dynamic_execution_without_running_code(self):
        for snippet in ['f = exec\nf(payload)','f = eval\nf(payload)',
                        'from builtins import exec as f\nf(payload)',
                        'f = __builtins__["exec"]\nf(payload)',
                        'other.compile(payload, "kernel", "exec")']:
            with self.subTest(snippet=snippet):
                with self.assertRaisesRegex(ValueError,'SOURCE_REQUIRES_MANUAL_REVIEW'):
                    A.source_screen(snippet.encode())

    def test_source_screen_preserves_readable_jit_and_installed_official_helpers(self):
        for snippet in [
            'import triton\nimport triton.language as tl\n@triton.jit\ndef kernel(x):\n    value = tl.load(x)\n',
            'from torch.utils.cpp_extension import load_inline\nmodule = load_inline(name="readable", cpp_sources="", cuda_sources="__global__ void kernel() {}")',
            'import nvrtc\nprogram = nvrtc.nvrtcCreateProgram("__global__ void kernel() {}", "kernel.cu")\ncompiled = nvrtc.nvrtcCompileProgram(program, [])',
            'from sglang.srt.layers.quantization.marlin_utils import marlin_make_workspace\ndef prepare(device):\n    return marlin_make_workspace(device)\n',
        ]:
            with self.subTest(snippet=snippet):A.source_screen(snippet.encode())

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

    def structured_cpu_evidence(self):
        self.projected_cpu_evidence()
        for name in ('cases', 'negativeControls'):
            for index, row in enumerate(self.cpu[name]):
                row['detail'] = {'observed': {'count': index + 1, 'exact': True},
                                 'shape': [2, 4], 'error': 0.0, 'notes': ['bounded', None]}
            self.qa['cpuEvidence'][name] = copy.deepcopy(self.cpu[name])
        self.qa['cpuEvidence']['resultSHA256'] = A.digest(A.canonical(self.cpu))

    def test_identical_structured_details_pass_full_native_qa1_and_qa2_without_post(self):
        self.structured_cpu_evidence()
        self.detail = self.native_fixture()
        self.assertEqual(self.lane.run(OCC, 'inspect')['state'], 'eligible')
        helper = NativeQAV2Tests(); helper.f = self; helper.d = self.detail; helper.a = self.audit
        helper.optin(1)
        self.assertEqual(self.lane.run(OCC, 'inspect')['state'], 'eligible')
        self.assertEqual(self.official.posts, 0)

    def test_structured_detail_cannot_be_omitted_changed_summarized_or_reordered(self):
        for name in ('cases', 'negativeControls'):
            for mutation in ('omit', 'nested', 'drop-field', 'summary', 'array-order'):
                with self.subTest(name=name, mutation=mutation):
                    self.structured_cpu_evidence()
                    row = self.qa['cpuEvidence'][name][0]
                    if mutation == 'omit': del row['detail']
                    elif mutation == 'nested': row['detail']['observed']['count'] += 1
                    elif mutation == 'drop-field': del row['detail']['observed']['exact']
                    elif mutation == 'summary': row['detail'] = 'all checks passed'
                    else: row['detail']['shape'].reverse()
                    with self.assertRaisesRegex(ValueError, 'CPU_CASES_NOT_REPLAYED'):
                        self.semantic_check()

    def test_structured_detail_preserves_exact_nested_json_types(self):
        for original, claimed in [(1, True), (True, 1), (1, 1.0), (None, False),
                                  ([1, 2], [1, True]), ({'count': 1}, {'count': True})]:
            with self.subTest(original=original, claimed=claimed):
                self.structured_cpu_evidence()
                self.cpu['cases'][0]['detail'] = {'value': original}
                self.qa['cpuEvidence']['cases'][0]['detail'] = {'value': claimed}
                self.qa['cpuEvidence']['resultSHA256'] = A.digest(A.canonical(self.cpu))
                with self.assertRaisesRegex(ValueError, 'CPU_CASES_NOT_REPLAYED'):
                    self.semantic_check()

    def test_nontext_details_are_exact_json_data_not_optional_text(self):
        for detail in ({}, [], None, True, 0, 0.0, ['one', {'two': 2}]):
            with self.subTest(detail=detail):
                self.structured_cpu_evidence()
                self.cpu['cases'][0]['detail'] = copy.deepcopy(detail)
                self.qa['cpuEvidence']['cases'][0]['detail'] = copy.deepcopy(detail)
                self.qa['cpuEvidence']['resultSHA256'] = A.digest(A.canonical(self.cpu))
                self.semantic_check()
                del self.qa['cpuEvidence']['cases'][0]['detail']
                with self.assertRaisesRegex(ValueError, 'CPU_CASES_NOT_REPLAYED'):
                    self.semantic_check()

    def test_detail_cannot_be_added_when_source_had_no_detail(self):
        for detail in ('new claim', {'count': 1}, [], None, True):
            with self.subTest(detail=detail):
                self.structured_cpu_evidence()
                del self.cpu['cases'][0]['detail']
                self.qa['cpuEvidence']['cases'][0]['detail'] = detail
                self.qa['cpuEvidence']['resultSHA256'] = A.digest(A.canonical(self.cpu))
                with self.assertRaisesRegex(ValueError, 'CPU_CASES_NOT_REPLAYED'):
                    self.semantic_check()

    def test_structured_details_do_not_relax_true_boolean_case_or_control_flags(self):
        for name, flag in [('cases', 'passed'), ('negativeControls', 'detected')]:
            for value in (False, 1, 'true', None):
                with self.subTest(name=name, value=value):
                    self.structured_cpu_evidence()
                    self.cpu[name][0][flag] = value
                    self.qa['cpuEvidence'][name][0][flag] = value
                    self.qa['cpuEvidence']['resultSHA256'] = A.digest(A.canonical(self.cpu))
                    with self.assertRaisesRegex(ValueError, 'CPU_MEANINGFUL_'):
                        self.semantic_check()

    def test_structured_details_do_not_replace_actual_execution_evidence(self):
        self.structured_cpu_evidence()
        self.detail = self.native_fixture()
        path = pathlib.Path(self.paths['workerRunsDirectory']) / IDS[1] / 'home/sessions/--workspace--' / IDS[1] / 'session.v3.jsonl'
        self.put(path, A.canonical({'type': 'assistant', 'data': {'text': 'CPU replay passed'}}) + b'\n')
        with self.assertRaisesRegex(ValueError, 'CPU_REPLAY_EXECUTION_UNPROVEN'):
            self.lane.run(OCC, 'inspect')
        self.assertEqual(self.official.posts, 0)

    def test_structured_details_cannot_hide_scope_or_hash_mismatch(self):
        for field, value in [('scopePreserved', False), ('candidateSHA256', 'f' * 64)]:
            with self.subTest(field=field):
                self.structured_cpu_evidence()
                self.qa[field] = value
                with self.assertRaisesRegex(ValueError, 'QA_SOURCE_OR_SCOPE_MISMATCH'):
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
        self.assertEqual([p.name for p in pathlib.Path(self.paths["stateDirectory"]).iterdir()],
                         ["terminal-diagnostics-v1.json"])

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

    def replay_with_command(self, command):
        rows = [A.strict_json(line) for line in self.transcript().splitlines()]
        rows[0]['data']['arguments'] = json.dumps({'command': command})
        return b'\n'.join(A.canonical(row) for row in rows)

    def test_static_workspace_cd_prefix_keeps_real_pinned_replay(self):
        command = 'sha256sum /workspace/evidence/cpu-checks.py /workspace/evidence/candidate.py && python3 /workspace/evidence/cpu-checks.py --candidate /workspace/evidence/candidate.py'
        for prefix in ('', 'cd /workspace && ', 'cd "/workspace" && '):
            raw = self.replay_with_command(prefix + command)
            proof = A.replay_commands(raw, A.digest(self.checker), A.digest(self.candidate))
            self.assertEqual(proof[0]['commandSHA256'], A.digest((prefix + command).encode()))

    def test_workspace_prefix_rejects_other_cwds_and_shell_control(self):
        command = 'sha256sum /workspace/evidence/cpu-checks.py /workspace/evidence/candidate.py && python3 /workspace/evidence/cpu-checks.py'
        for prefix in ('cd /tmp && ', 'cd /workspace; ', 'cd /workspace || ', 'cd /workspace && cd /tmp && ',
                       'cd "$PWD" && ', 'cd /workspace && echo ', 'cd /workspace && false && ',
                       'cd /workspace &&\n', 'cd /workspace && $(true) ', 'cd /workspace && `true` '):
            with self.subTest(prefix=prefix), self.assertRaisesRegex(ValueError, 'CPU_REPLAY_EXECUTION_UNPROVEN'):
                A.replay_commands(self.replay_with_command(prefix + command), A.digest(self.checker), A.digest(self.candidate))

    def test_workspace_prefix_requires_absolute_unambiguous_artifacts(self):
        for checker, candidate in [('cpu-checks.py','/workspace/candidate.py'),('/workspace/cpu-checks.py','candidate.py'),
                                   ('/workspace/../tmp/cpu-checks.py','/workspace/candidate.py'),
                                   ('//workspace/cpu-checks.py','/workspace/candidate.py')]:
            command = f'cd /workspace && sha256sum {checker} {candidate} && python3 {checker}'
            with self.subTest(checker=checker,candidate=candidate), self.assertRaisesRegex(ValueError, 'CPU_REPLAY_EXECUTION_UNPROVEN'):
                A.replay_commands(self.replay_with_command(command), A.digest(self.checker), A.digest(self.candidate))

    def test_workspace_prefix_cannot_replace_execution_or_tool_witness(self):
        command = 'cd /workspace && sha256sum /workspace/cpu-checks.py /workspace/candidate.py && python3 /workspace/cpu-checks.py'
        raw = self.replay_with_command(command)
        for bad in (raw.replace(b'python3', b'echo python3'), raw.replace(b'python3', b'bash -c python3'),
                    raw.replace(A.digest(self.checker).encode(),b'f'*64), raw.replace(b'exit code 0',b'exit code 1'),
                    raw.splitlines()[0], raw.splitlines()[1]):
            with self.assertRaisesRegex(ValueError, 'CPU_REPLAY_EXECUTION_UNPROVEN'):
                A.replay_commands(bad, A.digest(self.checker), A.digest(self.candidate))

    def test_workspace_prefix_rejects_trailing_commands_redirection_and_substitution(self):
        command = 'cd /workspace && sha256sum /workspace/cpu-checks.py /workspace/candidate.py && python3 /workspace/cpu-checks.py'
        for suffix in ('; true', ' && true', ' || true', ' | cat', ' > /tmp/out', ' $(true)', ' `true`', '\ntrue'):
            with self.subTest(suffix=suffix), self.assertRaisesRegex(ValueError, 'CPU_REPLAY_EXECUTION_UNPROVEN'):
                A.replay_commands(self.replay_with_command(command+suffix), A.digest(self.checker), A.digest(self.candidate))

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


class TerminalDiagnosticCacheTests(unittest.TestCase):
    def setUp(self):
        self.f = Fixture(); self.f.setUp(); self.addCleanup(self.f.tearDown)
        self.status = self.f.root / "cached-status.json"
        self.cache = pathlib.Path(self.f.p["stateDirectory"]) / "terminal-diagnostics-v1.json"
        self.now = NOW
        self.f.lane.clock = lambda: self.now
        qa = copy.deepcopy(self.f.qa); qa["cpuEvidence"]["resultSHA256"] = "f" * 64
        self.f.replace_evidence("qa-verification.json", A.canonical(qa), IDS[1])

    def scan(self):
        return A.scan(self.f.lane, self.status, "inspect")

    def cache_only(self):
        with mock.patch.object(A, "scan_selection", return_value=([], {"version":1, "lastUnregisteredOccurrenceId":None})):
            return self.scan()

    def test_terminal_rejection_is_reverified_without_repeating_audit_or_original_age(self):
        first = self.scan()["outcomes"][0]
        self.assertEqual(first["code"], "QA_TEST_BINDING")
        self.assertFalse(first["postAttempted"])
        self.now += 120
        with mock.patch.object(self.f.audit, "audit", side_effect=AssertionError("no repeat full audit")):
            second = self.scan()["outcomes"][0]
        self.assertEqual(second["diagnosticProof"]["observedAt"], NOW)
        self.assertEqual(second["diagnosticProof"]["verifiedAt"], NOW + 120)
        self.assertEqual(second["diagnosticProof"]["verifyKind"], "native_identity")
        self.assertEqual(self.f.official.posts, 0)

    def test_any_complete_detail_change_invalidates_cached_authority(self):
        changes = [lambda d: d["attempts"][0].update(fence=IDS[1]),
                   lambda d: d["steps"][0].update(state="running"),
                   lambda d: d["attempts"][1]["context"]["manifest"]["cycleInput"].update(digest="e" * 64),
                   lambda d: d["artifacts"][0].update(digest="d" * 64),
                   lambda d: d["attempts"][0].update(receipt={"stopped": False})]
        original = copy.deepcopy(self.f.detail)
        for change in changes:
            with self.subTest(change=change):
                self.f.detail = copy.deepcopy(original)
                self.scan(); change(self.f.detail)
                row = self.cache_only()["outcomes"][0]
                self.assertEqual(row["state"], "observation_wait")
                self.assertNotIn("diagnosticProof", row)
                self.assertEqual(A.strict_json(self.cache.read_bytes())["entries"], {})

    def test_dispatch_receipt_change_invalidates_even_when_api_detail_unchanged(self):
        self.scan()
        directory = pathlib.Path(self.f.p["watcherStateDirectory"])
        path = next(p for p in directory.glob("intent-*.json") if A.strict_json(p.read_bytes())["kind"] == "create")
        value = A.strict_json(path.read_bytes()); value["requestDigest"] = "0" * 64
        IO.atomic_record(path, value)
        self.assertEqual(self.cache_only()["outcomes"][0]["state"], "observation_wait")

    def test_ttl_is_from_original_observation_not_latest_verification(self):
        self.scan(); self.now += 3500; self.scan()
        self.now += 101
        row = self.cache_only()["outcomes"][0]
        self.assertNotIn("diagnosticProof", row)
        self.assertEqual(row["state"], "observation_wait")

    def test_policy_or_code_change_cannot_reuse_historical_record(self):
        for field in ("policy", "code", "materials"):
            with self.subTest(field=field):
                self.scan(); self.now += 1
                if field == "policy": self.f.lane.policy_sha = "e" * 64
                elif field == "code": self.f.p["adapterSHA256"] = "f" * 64
                else:
                    self.f.p["boards"][BOARD]["materials"]["canonical-TASK.md"] = "0" * 64
                    self.f.lane.policy_sha = A.digest(self.f.p)
                self.assertEqual(self.cache_only()["outcomes"], [])

    def test_unknown_and_acknowledged_effects_dominate_cache(self):
        self.scan()
        key = BOARD + "-" + A.digest(self.f.candidate) + "-check.json"
        path = pathlib.Path(self.f.p["legacyLedger"]) / key
        IO.atomic_record(path, {"state": "submission_outcome_unknown"})
        row = self.cache_only()["outcomes"][0]
        self.assertEqual((row["state"], row["postAttempted"]), ("uncertain_owned", None))
        self.assertNotIn("diagnosticProof", row)
        path.unlink(); self.scan()
        IO.atomic_record(path, {"state": "job_recorded", "receipt": {"job_id": "0123456789abcdef"}})
        row = self.cache_only()["outcomes"][0]
        self.assertEqual((row["state"], row["jobId"]), ("check_running", "0123456789abcdef"))

    def test_registry_dominates_and_both_ledgers_are_checked_before_cache(self):
        self.scan()
        self.f.lane.registered_occurrences = lambda: {OCC: {"problem": BOARD, "id": "0123456789abcdef",
            "sourceSHA256": A.digest(self.f.candidate), "qaArtifactId": IDS[1], "qaSHA256": "b" * 64}}
        self.assertEqual(self.cache_only()["outcomes"][0]["state"], "check_running")
        self.f.lane.registered_occurrences = lambda: {}
        self.scan()
        key = BOARD + "-" + A.digest(self.f.candidate) + "-check.json"
        IO.atomic_record(pathlib.Path(self.f.p["stateDirectory"]) / key,
                         {"state": "job_recorded", "receipt": {"job_id": "0123456789abcdef"}})
        IO.atomic_record(pathlib.Path(self.f.p["legacyLedger"]) / key, {"state": "submission_outcome_unknown"})
        self.assertEqual(self.cache_only()["outcomes"][0]["state"], "uncertain_owned")

    def test_nonterminal_and_provenance_failures_are_never_cached(self):
        for code in ["CHECK_QUOTA", "SOURCE_ORIGIN_QA_BINDING", "DISPATCH_DIGEST", "NATIVE_NOT_COMPLETED"]:
            with self.subTest(code=code), mock.patch.object(self.f.audit, "audit", side_effect=ValueError(code)):
                row = self.scan()["outcomes"][0]
                self.assertNotIn("diagnosticProof", row)
                self.assertEqual(A.strict_json(self.cache.read_bytes())["entries"], {})

    def test_same_source_effect_of_another_occurrence_dominates_cached_rejection(self):
        self.scan()
        self.f.lane.registered_occurrences = lambda: {SUPERVISOR: {"problem":BOARD,"id":"0123456789abcdef",
            "sourceSHA256":A.digest(self.f.candidate),"qaArtifactId":IDS[1],"qaSHA256":"b"*64}}
        key=BOARD+"-"+A.digest(self.f.candidate)+"-check.json"
        IO.atomic_record(pathlib.Path(self.f.p["legacyLedger"])/key,
            {"state":"job_recorded","receipt":{"job_id":"0123456789abcdef"},"identity":{"occurrenceId":SUPERVISOR}})
        row=self.cache_only()["outcomes"][0]
        self.assertEqual((row["state"],row["jobId"]),("check_running","0123456789abcdef"))
        self.assertNotIn("diagnosticProof",row)

    def test_registered_source_alias_without_ledger_dominates_before_native_get(self):
        self.scan()
        self.f.lane.registered_occurrences = lambda: {SUPERVISOR: {"problem":BOARD,"id":"0123456789abcdef",
            "sourceSHA256":A.digest(self.f.candidate),"qaArtifactId":IDS[1],"qaSHA256":"b"*64}}
        with mock.patch.object(self.f.runtime,"api",side_effect=AssertionError("registered alias needs no native read")):
            row=self.cache_only()["outcomes"][0]
        self.assertEqual((row["state"],row["jobId"]),("check_running","0123456789abcdef"))
        self.assertNotIn("diagnosticProof",row)

    def test_effect_created_during_identity_get_dominates_before_verified_time(self):
        self.scan(); self.now+=120
        original=self.f.audit.terminal_identity
        def racing(oid,detail=None):
            value=original(oid,detail)
            key=BOARD+"-"+A.digest(self.f.candidate)+"-check.json"
            IO.atomic_record(pathlib.Path(self.f.p["legacyLedger"])/key,{"state":"submission_outcome_unknown"})
            return value
        with mock.patch.object(self.f.audit,"terminal_identity",side_effect=racing):
            row=self.cache_only()["outcomes"][0]
        self.assertEqual(row["state"],"uncertain_owned")
        self.assertNotIn("diagnosticProof",row)
        self.assertEqual(A.strict_json(self.cache.read_bytes())["entries"],{})

    def test_invalid_proof_times_and_injected_effect_authority_are_rejected(self):
        row = self.scan()["outcomes"][0]
        for field, value in [("observedAt", True), ("verifiedAt", float("nan")),
                             ("verifiedAt", float("inf")), ("verifiedAt", NOW + 6),
                             ("observedAt", NOW + 1)]:
            changed = copy.deepcopy(row); changed["diagnosticProof"][field] = value
            with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                A.diagnostic_cache_entry(changed, self.f.lane, NOW)
        for state in ["eligible", "quota_wait", "uncertain_owned", "check_running"]:
            changed = {**row, "state": state}
            with self.subTest(state=state), self.assertRaisesRegex(ValueError, "DIAGNOSTIC_CACHE_SCOPE"):
                A.diagnostic_cache_entry(changed, self.f.lane, NOW)

    def test_cache_before_status_crash_reloads_without_new_effect(self):
        original = IO.atomic_record
        def crash(path, value):
            if pathlib.Path(path) == self.status: raise OSError("isolated status crash")
            return original(path, value)
        with mock.patch.object(IO, "atomic_record", side_effect=crash), self.assertRaises(OSError):
            self.scan()
        self.assertTrue(self.cache.is_file()); self.assertFalse(self.status.exists())
        with mock.patch.object(self.f.audit, "audit", side_effect=AssertionError("cache reload")):
            self.assertEqual(self.scan()["outcomes"][0]["code"], "QA_TEST_BINDING")
        self.assertEqual(self.f.official.posts, 0)

    def test_cache_count_and_file_bytes_are_bounded(self):
        self.scan(); before = self.status.read_bytes()
        value = A.strict_json(self.cache.read_bytes()); original = next(iter(value["entries"].values()))
        value["entries"] = {str(i):original for i in range(129)}; IO.atomic_record(self.cache, value)
        with self.assertRaisesRegex(ValueError, "DIAGNOSTIC_CACHE_FILE"): self.scan()
        self.assertEqual(self.status.read_bytes(), before)
        self.cache.write_bytes(b" " * (A.TERMINAL_DIAGNOSTIC_MAX_BYTES + 1))
        with self.assertRaisesRegex(ValueError, "PROTECTED_FILE"): self.scan()
        self.assertEqual(self.status.read_bytes(), before)

    def test_three_partial_sweeps_accumulate_verified_rejections_without_fake_freshness(self):
        rows = [{"occurrenceId": "aaaaaaaa-aaaa-aaaa-aaaa-" + str(i).zfill(12), "problem": BOARD} for i in range(3)]
        IO.atomic_record(pathlib.Path(self.f.p["watcherStateDirectory"]) / "state.json", {"watchId":WATCH, "experiments":rows})
        elapsed = [0]; calls = []
        def identity(oid):
            return {"detailSHA256":A.digest(oid), "dispatchSHA256":"e"*64,
                    "binding":{"candidateSHA256":A.digest(oid), "qaArtifactId":IDS[1], "qaSHA256":"f"*64}}
        audit = types.SimpleNamespace(terminal_identity=identity)
        def binding(oid):
            audit._diagnostic_identity = identity(oid)
            return identity(oid)["binding"]
        audit.diagnostic_binding = binding
        def reject(oid, mode):
            calls.append(oid); elapsed[0] += 46
            raise A.NativeDiagnosticError(ValueError("QA_TEST_BINDING"))
        lane = types.SimpleNamespace(p=self.f.p, policy_sha="b"*64, io=IO, audit=audit, run=reject,
            clock=lambda:self.now, registered_occurrences=lambda:{}, state=pathlib.Path(self.f.p["stateDirectory"]),
            legacy=pathlib.Path(self.f.p["legacyLedger"]))
        first_observed = {}
        for n in range(1,4):
            elapsed[0] = 0; self.now += 120
            with mock.patch.object(A.time, "monotonic", side_effect=lambda:elapsed[0]):
                result = A.scan(lane, self.status, "inspect")
            self.assertEqual(len(result["outcomes"]), n)
            for row in result["outcomes"]:
                first_observed.setdefault(row["occurrenceId"], row["diagnosticProof"]["observedAt"])
                self.assertEqual(row["diagnosticProof"]["observedAt"], first_observed[row["occurrenceId"]])
                self.assertEqual(row["diagnosticProof"]["verifiedAt"], self.now)
            self.assertLess(len(A.canonical(result)), 262144)
        self.assertEqual(len(calls), 3)


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
        self.assertEqual(row["state"], "account_wait")
        self.assertEqual(row["code"], "ACCOUNT_HAS_UNCERTAIN_EFFECT")
        self.assertIs(row["postAttempted"], False)
        self.assertEqual(path.read_bytes(), before); self.assertEqual(self.fixture.official.posts, 0)

    def test_untyped_account_wait_is_not_a_no_post_proof(self):
        self.fixture.lane.run = mock.Mock(side_effect=ValueError("ACCOUNT_HAS_UNCERTAIN_EFFECT"))
        row = A.scan(self.fixture.lane, self.status, "check")["outcomes"][0]
        self.assertEqual(row["state"], "uncertain_owned"); self.assertIsNone(row["postAttempted"])
        self.assertEqual(self.fixture.official.posts, 0)

    def test_own_durable_effect_dominates_typed_account_wait(self):
        source = A.digest(self.fixture.candidate)
        path = pathlib.Path(self.p["stateDirectory"]) / (BOARD + "-" + source + "-check.json")
        IO.atomic_record(path, {"state":"submission_outcome_unknown", "identity":{"sourceSha256":source}})
        before = path.read_bytes()
        self.fixture.lane.run = mock.Mock(side_effect=A.AccountPreflightWait("ACCOUNT_HAS_UNCERTAIN_EFFECT"))
        row = A.scan(self.fixture.lane, self.status, "check")["outcomes"][0]
        self.assertEqual(row["state"], "uncertain_owned"); self.assertIsNone(row["postAttempted"])
        self.assertEqual(path.read_bytes(), before); self.assertEqual(self.fixture.official.posts, 0)

    def test_account_wait_preserves_exact_candidate_binding_without_creating_intent(self):
        foreign = pathlib.Path(self.p["stateDirectory"]) / (BOARD + "-" + "f"*64 + "-check.json")
        IO.atomic_record(foreign, {"state":"submission_outcome_unknown", "identity":{"sourceSha256":"f"*64}})
        before = {str(p):p.read_bytes() for p in pathlib.Path(self.p["stateDirectory"]).rglob('*') if p.is_file()}
        row = A.scan(self.fixture.lane, self.status, "check")["outcomes"][0]
        self.assertEqual((row["state"], row["postAttempted"], row["jobId"]), ("account_wait", False, None))
        self.assertEqual(row["candidateSHA256"], A.digest(self.fixture.candidate))
        self.assertTrue(row["qaArtifactId"] and row["qaSHA256"])
        after = {str(p):p.read_bytes() for p in pathlib.Path(self.p["stateDirectory"]).rglob('*') if p.is_file()}
        self.assertEqual({p:after[p] for p in before}, before)
        added = set(after) - set(before)
        self.assertEqual({pathlib.Path(p).name for p in added}, {"terminal-diagnostics-v1.json"})
        self.assertEqual(A.strict_json(after[next(iter(added))])["entries"], {})
        self.assertEqual(self.fixture.official.posts, 0)

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


class NativeQAV2Tests(unittest.TestCase):
    def setUp(self):
        self.f=Fixture();self.f.setUp();self.addCleanup(self.f.tearDown)
        self.d=self.f.detail;self.a=self.f.audit

    def optin(self, count=2, failed=False):
        self.source=self.f.origin_fixture(failed_qa=False)
        specs=self.d['occurrence']['definition']['steps'];qa_spec=specs[1]
        qa_spec.update(maxAttempts=3,parameters={'nativeQAContractVersion':2},dependencyEvidence={
            'version':1,'sourceStep':'produce','bindings':[
                {'reportArtifact':'qa-verification.json','jsonPointer':'/cpuEvidence/checkerSHA256','dependencyArtifact':'cpu-checks.py'},
                {'reportArtifact':'qa-verification.json','jsonPointer':'/cpuEvidence/resultSHA256','dependencyArtifact':'cpu-result.json'}]})
        self.d['steps'][0]['attempt']=1;self.d['steps'][1]['attempt']=count
        producer,last=self.d['attempts'];last['number']=count
        priors=[]
        for number in range(1,count):
            prior=copy.deepcopy(last);prior.update(id=f'99999999-9999-4999-8999-{number:012d}',fence=f'aaaaaaaa-aaaa-4aaa-8aaa-{number:012d}',
                number=number,state='failed',error_code='DEPENDENCY_ARTIFACT_BINDING')
            prior['context']['manifest']['cycleInput'].update(attemptId=prior['id'],runFence=prior['fence'])
            bad=copy.deepcopy(self.f.qa);bad['cpuEvidence']['checkerSHA256']='f'*64
            self.d['artifacts'].append(self.f.artifact('qa-verification.json',A.canonical(bad),prior['id']))
            priors.append(prior)
        self.d['attempts']=[producer,*priors,last]
        if failed:
            last.update(state='failed',error_code='DEPENDENCY_ARTIFACT_BINDING')
            self.d['steps'][1]['state']='attention';self.d['occurrence']['state']='attention'
            row=next(r for r in self.d['artifacts'] if r['attempt_id']==last['id'] and r['name']=='qa-verification.json')
            bad=copy.deepcopy(self.f.qa);bad['cpuEvidence']['resultSHA256']='f'*64;raw=A.canonical(bad)
            row.update(digest=A.digest(raw),size=len(raw));self.f.put(pathlib.Path(self.f.paths['artifactDirectory'])/row['digest'],raw)
        self.f.refresh_current_input()
        key=self.d['occurrence']['source_key'][len('manual:'):-4]
        self.f.put(pathlib.Path(self.f.paths['watcherStateDirectory'])/('intent-'+A.digest(key)+'.json'),A.canonical({
            'state':'recorded','kind':'create','requestKey':key,'result':{'id':CYCLE,'definition':self.d['occurrence']['definition']},
            'requestDigest':A.digest({'requestKey':key,'definition':self.d['occurrence']['definition']})}))
        source={'stepId':self.d['steps'][0]['id'],'key':'produce','attemptId':producer['id'],'attemptNumber':1,'runFence':producer['fence']}
        refs=sorted([{k:r[k] for k in ['id','name','digest','size']} for r in self.d['artifacts'] if r['attempt_id']==producer['id']],key=lambda r:(r['name'],r['id']))
        bindings=qa_spec['dependencyEvidence']['bindings'];contract_sha=A.digest([1,'produce',[[b['reportArtifact'],b['jsonPointer'],b['dependencyArtifact']] for b in bindings]])
        for attempt in self.d['attempts'][1:]:
            for ref in refs:
                self.f.put(pathlib.Path(self.f.paths['workerRunsDirectory'])/attempt['id']/'home/inputs'/ref['id']/ref['name'],
                    (pathlib.Path(self.f.paths['artifactDirectory'])/ref['digest']).read_bytes())
            consumer={'stepId':self.d['steps'][1]['id'],'key':'independent-qa','attemptId':attempt['id'],'attemptNumber':attempt['number'],'runFence':attempt['fence']}
            attempt['context']['manifest']['dependencyEvidence']={'version':1,'contractSHA256':contract_sha,'occurrenceId':OCC,
                'consumer':consumer,'source':copy.deepcopy(source),'artifacts':copy.deepcopy(refs)}
            report_row=next(r for r in self.d['artifacts'] if r['attempt_id']==attempt['id'] and r['name']=='qa-verification.json')
            report=A.strict_json((pathlib.Path(self.f.paths['artifactDirectory'])/report_row['digest']).read_bytes())
            result=[]
            for b in bindings:
                dep=next(r for r in refs if r['name']==b['dependencyArtifact'])
                result.append({'reportArtifact':{k:report_row[k] for k in ['id','name','digest','size']},'jsonPointer':b['jsonPointer'],
                    'dependencyArtifact':copy.deepcopy(dep),'expected':dep['digest'],'received':report['cpuEvidence'][b['jsonPointer'].split('/')[-1]]})
            attempt['context']['dependencyEvidenceValidation']={'version':1,'status':'matched' if attempt['state']=='completed' else 'mismatch',
                'source':copy.deepcopy(source),'bindings':result}
        return last

    def test_v2_first_attempt_and_bounded_three_attempts_pass_full_audit(self):
        self.optin(3)
        result,_,_=self.a.audit(OCC)
        self.assertEqual(result['nativeQAContractVersion'],2)
        self.assertEqual([r['state'] for r in result['qaAttemptHistory']],['failed','failed','completed'])
        self.assertEqual(result['identities']['independent-qa']['attemptId'],IDS[1])
        self.assertEqual(self.f.official.posts,0)

    def test_v2_single_qa_still_requires_positive_controller_gate(self):
        self.optin(1);self.a.audit(OCC)
        self.d['attempts'][-1]['context'].pop('dependencyEvidenceValidation')
        with self.assertRaisesRegex(ValueError,'NATIVE_QA_GATE_PROOF'):self.a.audit(OCC)

    def test_legacy_retry_never_adopted_as_v2(self):
        self.optin(2);self.d['occurrence']['definition']['steps'][1].pop('parameters');self.d['occurrence']['definition']['steps'][1].pop('dependencyEvidence')
        with self.assertRaises(ValueError):self.a.audit(OCC)

    def test_v2_contract_rejects_other_bindings_versions_and_producer_retry(self):
        self.optin();definition=self.d['occurrence']['definition'];original=copy.deepcopy(definition)
        changes=[lambda d:d['steps'][1]['parameters'].update(nativeQAContractVersion=True),
            lambda d:d['steps'][1]['dependencyEvidence']['bindings'][0].update(jsonPointer='/historical/checkerSHA256'),
            lambda d:d['steps'][1].update(maxAttempts=4),lambda d:d['steps'][0].update(maxAttempts=2),
            lambda d:d['steps'][1].update(repairStep='produce')]
        for change in changes:
            value=copy.deepcopy(original);change(value)
            with self.assertRaisesRegex(ValueError,'NATIVE_QA_CONTRACT'):self.a.qa_contract(value)

    def test_v2_rejects_attempt_gaps_mixed_fences_old_selection_and_bad_retry(self):
        self.optin(3);original=copy.deepcopy(self.d)
        changes=[lambda d:d['attempts'][1].update(number=2),lambda d:d['attempts'][1].update(number=True),
            lambda d:d['steps'][1].update(attempt=2),lambda d:d['attempts'][1].update(error_code='HARNESS_SANDBOX_OOM'),
            lambda d:d['attempts'][1]['receipt'].update(stopped=False),lambda d:d['attempts'][2].update(fence=d['attempts'][1]['fence']),
            lambda d:d['attempts'][-1]['context']['manifest']['dependencyEvidence']['consumer'].update(attemptId=d['attempts'][1]['id']),
            lambda d:d['attempts'][-1]['context']['manifest']['dependencyEvidence']['source'].update(runFence=IDS[1]),
            lambda d:d['attempts'][-1]['context']['manifest']['dependencyEvidence']['artifacts'][0].update(digest='f'*64),
            lambda d:d['attempts'][1]['context']['dependencyEvidenceValidation'].update(status='matched')]
        for change in changes:
            self.d.clear();self.d.update(copy.deepcopy(original));change(self.d)
            with self.assertRaises(ValueError):self.a.qa_attempt_chain(self.d)

    def test_v2_requires_physical_stop_for_failed_qa(self):
        self.optin();prior=self.d['attempts'][1]['id']
        self.f.runtime.stopped.side_effect=lambda aid: (_ for _ in ()).throw(ValueError('STILL_RUNNING')) if aid==prior else None
        with self.assertRaisesRegex(ValueError,'STILL_RUNNING'):self.a.audit(OCC)

    def test_v2_raw_failed_report_cannot_be_replaced_by_forged_gate_proof(self):
        self.optin();prior=self.d['attempts'][1]
        row=next(r for r in self.d['artifacts'] if r['attempt_id']==prior['id'])
        self.f.put(pathlib.Path(self.f.paths['artifactDirectory'])/row['digest'],b'{}')
        with self.assertRaisesRegex(ValueError,'ARTIFACT_BYTES_CHANGED'):self.a.audit(OCC)

    def test_v2_prior_qa_handoff_must_match_frozen_producer_bytes(self):
        self.optin();prior=self.d['attempts'][1];ref=prior['context']['manifest']['dependencyEvidence']['artifacts'][0]
        self.f.put(pathlib.Path(self.f.paths['workerRunsDirectory'])/prior['id']/'home/inputs'/ref['id']/ref['name'],b'changed')
        with self.assertRaisesRegex(ValueError,'NATIVE_QA_DEPENDENCY_HANDOFF'):self.a.audit(OCC)

    def test_v2_inventory_rejects_path_escape_before_reading_handoff(self):
        self.optin();row=next(r for r in self.d['artifacts'] if r['attempt_id']==IDS[0])
        row['name']='../../outside'
        with self.assertRaisesRegex(ValueError,'ARTIFACT_NAME'):self.a.qa_attempt_chain(self.d)

    def test_v2_invalid_report_shape_is_not_retry_authority(self):
        self.optin();prior=self.d['attempts'][1];row=next(r for r in self.d['artifacts'] if r['attempt_id']==prior['id'])
        raw=A.canonical({'cpuEvidence':1});row.update(digest=A.digest(raw),size=len(raw))
        self.f.put(pathlib.Path(self.f.paths['artifactDirectory'])/row['digest'],raw)
        with self.assertRaisesRegex(ValueError,'NATIVE_QA_REPORT_BINDING'):self.a.audit(OCC)

    def test_v2_diagnostic_and_cache_use_only_last_qa_and_invalidate_history_change(self):
        self.optin(3);binding=self.a.diagnostic_binding(OCC);identity=self.a.terminal_identity(OCC)
        last=next(r for r in self.d['artifacts'] if r['attempt_id']==IDS[1] and r['name']=='qa-verification.json')
        self.assertEqual(binding['qaArtifactId'],last['id']);self.assertEqual(identity['binding'],binding)
        self.d['attempts'][1]['context']['manifest']['dependencyEvidence']['consumer']['runFence']=IDS[1]
        with self.assertRaisesRegex(ValueError,'NATIVE_QA_DEPENDENCY_SNAPSHOT'):self.a.terminal_identity(OCC)

    def test_failed_final_v2_is_origin_only_not_acceptance_or_terminal_cache(self):
        self.optin(3,failed=True)
        refs=self.f.origin(self.source);report=A.strict_json(A.pinned(refs[0]))
        self.assertFalse(report['qualityAccepted']);self.assertEqual(report['qa']['state'],'failed')
        self.assertEqual(len(report['qaAttemptHistory']),3)
        with self.assertRaisesRegex(ValueError,'NATIVE_NOT_COMPLETED'):self.a.audit(OCC)
        with self.assertRaisesRegex(ValueError,'CACHE_NATIVE_NOT_TERMINAL'):self.a.terminal_identity(OCC)

    def test_other_final_failure_has_no_v2_origin_authority(self):
        self.optin(2,failed=True);self.d['attempts'][-1]['error_code']='DELIVERY_INVALID'
        with self.assertRaisesRegex(ValueError,'NATIVE_QA_RETRY_CAUSE'):self.f.origin(self.source)

    def test_v2_successful_gate_does_not_replace_semantic_qa_or_replay(self):
        self.optin();row=next(r for r in self.d['artifacts'] if r['attempt_id']==IDS[1] and r['name']=='qa-verification.json')
        raw=self.a.artifact(row);report=A.strict_json(raw);report['canonicalPrecisionPreserved']=False;raw=A.canonical(report)
        row.update(digest=A.digest(raw),size=len(raw));self.f.put(pathlib.Path(self.f.paths['artifactDirectory'])/row['digest'],raw)
        for b in self.d['attempts'][-1]['context']['dependencyEvidenceValidation']['bindings']:
            b['reportArtifact']={k:row[k] for k in ['id','name','digest','size']}
        with self.assertRaisesRegex(ValueError,'QA_ATTESTATION_MISSING'):self.a.audit(OCC)


class QuotaSynchronizationTests(unittest.TestCase):
    def setUp(self):
        self.f = Fixture(); self.f.setUp(); self.addCleanup(self.f.tearDown)
        self.lane, self.p, self.official = self.f.lane, self.f.p, self.f.official
        self.p["minimumChecksRemaining"] = 0
        self.day = int(NOW // 86400) * 86400
        self.job = "abcdef0123456789"

    def record(self, used=9, known=None, mirror=True):
        row = {"state":"job_recorded", "mode":"check", "intentEpoch":NOW-120,
               "identity":{"problem":BOARD,"sourceSha256":"f"*64}, "receipt":{"job_id":self.job},
               "quotaObservation":{"version":1,"problem":BOARD,"utcDayStart":self.day,
                   "checkedAt":NOW-121,"used":used,"knownJobIds":known or []}}
        for key in ["stateDirectory", "legacyLedger"] if mirror else ["stateDirectory"]:
            IO.atomic_record(pathlib.Path(self.p[key]) / (BOARD+"-"+"f"*64+"-check.json"),row)
        return row

    def visible(self, job=None):
        return {"job_id":job or self.job,"problem":BOARD,"mode":"check","submitted_at":NOW-120}

    def test_last_acknowledged_check_blocks_stale_left_even_when_job_is_visible(self):
        self.record(); self.official.left=1; self.official.jobs=[self.visible()]
        before={str(p):p.read_bytes() for key in ['stateDirectory','legacyLedger'] for p in pathlib.Path(self.p[key]).glob('*')}
        with self.assertRaisesRegex(A.PreflightQuotaError,"CHECK_QUOTA_SYNC_PENDING"):
            self.lane.preflight(BOARD)
        self.assertEqual(before,{p:pathlib.Path(p).read_bytes() for p in before})
        self.assertEqual(self.official.posts,0)

    def test_missing_acknowledged_job_waits_for_get_visibility_before_new_post(self):
        self.record(used=2);self.official.left=8;self.official.jobs=[]
        with self.assertRaisesRegex(A.PreflightQuotaError,"CHECK_QUOTA_SYNC_PENDING"):
            self.lane.preflight(BOARD)
        self.assertEqual(self.official.posts,0)

    def test_remote_consumed_count_overrides_stale_quota_without_local_history(self):
        self.official.left=1
        self.official.jobs=[self.visible(format(i,'016x')) for i in range(10)]
        with self.assertRaisesRegex(A.PreflightQuotaError,"CHECK_QUOTA_SYNC_PENDING"):
            self.lane.preflight(BOARD)
        self.assertEqual(self.official.posts,0)

    def test_acknowledged_job_and_mirror_count_once_when_consumption_is_reflected(self):
        self.record(used=2);self.official.left=7;self.official.jobs=[self.visible()]
        result=self.lane.preflight(BOARD)
        self.assertEqual(result['quotaObservation']['used'],3)
        self.assertEqual(result['quotaObservation']['knownJobIds'],[self.job])

    def test_new_intent_contains_observation_before_sole_post(self):
        original=self.official.check
        def inspect_intent(problem,snapshot):
            path=pathlib.Path(self.p['stateDirectory'])/(BOARD+'-'+A.digest(self.f.candidate)+'-check.json')
            value=A.strict_json(path.read_bytes())
            self.assertEqual(value['state'],'submission_intent')
            self.assertEqual(value['quotaObservation'],{'version':1,'problem':BOARD,'utcDayStart':self.day,'checkedAt':NOW,'used':0,'knownJobIds':[]})
            return original(problem,snapshot)
        self.official.check=inspect_intent
        self.assertEqual(self.lane.run(OCC,'check')['state'],'job_recorded')
        self.assertEqual(self.official.posts,1)

    def test_midnight_during_second_audit_waits_before_any_snapshot(self):
        clock=[self.day+86400-1]
        self.lane.clock=lambda:clock[0];self.p['expiresAt']=clock[0]+3600
        original=self.lane.audit.audit;calls=[]
        def crosses(occurrence_id):
            result=original(occurrence_id);calls.append(occurrence_id)
            if len(calls)==2:clock[0]+=2
            return result
        self.lane.audit.audit=crosses
        with self.assertRaisesRegex(A.PreflightQuotaError,'CHECK_QUOTA_SYNC_PENDING'):
            self.lane.run(OCC,'check')
        self.assertEqual(list(pathlib.Path(self.p['stateDirectory']).iterdir()),[])
        self.assertEqual(self.official.posts,0)

    def test_midnight_during_freeze_never_creates_intent_and_next_tick_repreflights(self):
        clock=[self.day+86400-1]
        self.lane.clock=lambda:clock[0];self.p['expiresAt']=clock[0]+3600
        original=IO.immutable_snapshot
        def crosses(path,raw):
            result=original(path,raw)
            if pathlib.Path(path).name.startswith('preflight-'):clock[0]+=2
            return result
        with mock.patch.object(IO,'immutable_snapshot',side_effect=crosses):
            with self.assertRaisesRegex(A.PreflightQuotaError,'CHECK_QUOTA_SYNC_PENDING'):
                self.lane.run(OCC,'check')
        for key in ['stateDirectory','legacyLedger']:
            self.assertEqual(list(pathlib.Path(self.p[key]).glob('*-check.json')),[])
        self.assertEqual(self.official.posts,0)
        self.assertEqual(self.lane.run(OCC,'check')['state'],'job_recorded')
        path=pathlib.Path(self.p['stateDirectory'])/(BOARD+'-'+A.digest(self.f.candidate)+'-check.json')
        intent=A.strict_json(path.read_bytes())
        self.assertEqual(intent['quotaObservation']['utcDayStart'],int(intent['intentEpoch']//86400)*86400)
        self.assertEqual(self.official.posts,1)

    def test_account_wait_audits_candidate_but_performs_no_official_get_or_post(self):
        path=pathlib.Path(self.p['stateDirectory'])/(BOARD+'-'+'f'*64+'-check.json')
        IO.atomic_record(path,{'state':'submission_outcome_unknown','identity':{'sourceSha256':'f'*64}})
        original=self.lane.audit.audit;calls=[]
        def observed(occurrence_id):
            calls.append(occurrence_id);return original(occurrence_id)
        self.lane.audit.audit=observed
        self.official.fetch=mock.Mock(side_effect=AssertionError('no official network before reconciliation'))
        with self.assertRaisesRegex(A.AccountPreflightWait,'ACCOUNT_HAS_UNCERTAIN_EFFECT'):
            self.lane.run(OCC,'check')
        self.assertEqual(calls,[OCC]);self.official.fetch.assert_not_called()
        self.assertEqual(self.official.posts,0)

    def test_yesterday_and_other_board_do_not_consume_today_scope(self):
        record=self.record(used=9);record['intentEpoch']=self.day-1
        self.official.jobs=[{**self.visible(),'submitted_at':self.day-1},
                            {**self.visible('1234567890abcdef'),'problem':'muse_prefill_layer'}]
        result=self.lane.quota_observation([record],self.official.jobs,BOARD,{'used':0,'left':10,'per_day':10},NOW)
        self.assertEqual(result['used'],0);self.assertEqual(result['knownJobIds'],[])

    def test_malformed_observation_is_not_quota_wait_no_post_proof(self):
        row=self.record(used=True);self.official.left=7;self.official.jobs=[self.visible()]
        with self.assertRaisesRegex(ValueError,'CHECK_QUOTA_OBSERVATION_SCHEMA'):
            self.lane.preflight(BOARD)
        self.assertEqual(self.official.posts,0)

    def test_sync_wait_scan_has_explicit_false_and_binding_but_no_new_intent(self):
        self.record();self.official.left=1;self.official.jobs=[self.visible()]
        result=A.scan(self.lane,self.f.root/'sync-status.json','check')['outcomes'][0]
        self.assertEqual((result['state'],result['code'],result['postAttempted'],result['jobId']),('quota_wait','CHECK_QUOTA_SYNC_PENDING',False,None))
        self.assertEqual(result['candidateSHA256'],A.digest(self.f.candidate))
        path=pathlib.Path(self.p['stateDirectory'])/(BOARD+'-'+A.digest(self.f.candidate)+'-check.json')
        self.assertFalse(path.exists());self.assertEqual(self.official.posts,0)

    def test_untyped_sync_and_own_uncertainty_never_get_false_marker(self):
        self.lane.run=mock.Mock(side_effect=ValueError('CHECK_QUOTA_SYNC_PENDING'))
        row=A.scan(self.lane,self.f.root/'sync-status.json','check')['outcomes'][0]
        self.assertIsNone(row['postAttempted'])
        path=pathlib.Path(self.p['stateDirectory'])/(BOARD+'-'+A.digest(self.f.candidate)+'-check.json')
        IO.atomic_record(path,{'state':'submission_outcome_unknown','identity':{'sourceSha256':A.digest(self.f.candidate)}})
        self.lane.run=mock.Mock(side_effect=A.PreflightQuotaError('CHECK_QUOTA_SYNC_PENDING'))
        row=A.scan(self.lane,self.f.root/'sync-status.json','check')['outcomes'][0]
        self.assertEqual(row['state'],'uncertain_owned');self.assertIsNone(row['postAttempted'])
        self.assertEqual(self.official.posts,0)


class UncertainCheckReservationTests(unittest.TestCase):
    def setUp(self):
        self.f=Fixture();self.f.setUp();self.addCleanup(self.f.tearDown)
        self.p,self.lane,self.official=self.f.p,self.f.lane,self.f.official
        self.p['minimumChecksRemaining']=0
        self.p['transportHelper']={'path':str(self.f.root/'transport.py'),'sha256':'d'*64}
        self.now=NOW;self.lane.clock=lambda:self.now

    def reserve(self, board=BOARD, own=False, epoch=None, old_change=None):
        if board not in self.p['boards']:self.p['boards'][board]=copy.deepcopy(self.p['boards'][BOARD])
        raw=self.f.candidate if own else b'old reviewed candidate\n';sha=A.digest(raw)
        occurrence=OCC if own else SUPERVISOR;epoch=self.now-120 if epoch is None else epoch
        old=copy.deepcopy(self.p);old.pop('uncertainCheckReservations',None)
        old.update(notBefore=epoch-100,expiresAt=epoch+3600)
        if old_change:old_change(old)
        old_path=self.f.root/('old-policy-'+str(len(list(self.f.root.glob('old-policy-*'))))+'.json')
        old_raw=A.canonical(old);IO.immutable_snapshot(old_path,old_raw)
        audit={'problem':board,'sourceSHA256':sha,'occurrenceId':occurrence}
        audit_raw=A.canonical(audit);receipt={'error':'daily CHECK cap reached; effect remains unresolved','status':429}
        row={'state':'submission_outcome_unknown','mode':'check','automaticPostRetry':False,'intentEpoch':epoch,'exitCode':1,
             'identity':{'origin':'native-diagnostic-check-policy-v1','operatorReviewedCandidate':False,'problem':board,
                'sourceSha256':sha,'occurrenceId':occurrence,'policySHA256':A.digest(old_raw),'auditSHA256':A.digest(audit_raw)},'receipt':receipt}
        stem=board+'-'+sha
        self.native=pathlib.Path(self.p['stateDirectory'])/(stem+'-check.json');self.mirror=pathlib.Path(self.p['legacyLedger'])/self.native.name
        IO.atomic_record(self.native,row);IO.atomic_record(self.mirror,row)
        IO.immutable_snapshot(self.native.parent/(stem+'-audit.json'),audit_raw)
        IO.immutable_snapshot(self.native.parent/(stem+'.py'),raw)
        folder=self.native.parent/(stem+'-evidence');folder.mkdir(mode=0o700)
        IO.immutable_snapshot(folder/'stdout.json',A.canonical(receipt))
        pin={'problem':board,'sourceSHA256':sha,'occurrenceId':occurrence,'intentEpoch':epoch,
             'ledgerSHA256':A.digest(self.native.read_bytes()),'originPolicy':{'path':str(old_path),'sha256':A.digest(old_raw)}}
        self.p['uncertainCheckReservations']={'version':1,'intents':[pin]}
        return row,pin

    def test_default_account_block_and_strict_optin_schema(self):
        self.reserve();flag=self.p.pop('uncertainCheckReservations')
        with mock.patch.object(self.official,'fetch',side_effect=AssertionError('no GET')):
            with self.assertRaises(A.AccountPreflightWait):self.lane.preflight(BOARD)
        for bad in [True,{'version':True,'intents':flag['intents']},{'version':1,'intents':[]},
                    {'version':1,'intents':flag['intents']*2},{**flag,'allowAll':True}]:
            self.p['uncertainCheckReservations']=bad
            with self.assertRaisesRegex(ValueError,'UNCERTAIN_RESERVATION_POLICY'):A.validate_policy(self.p,NOW,'a'*64)

    def test_mirrors_reserve_once_keep_floor_clean_and_never_rewrite(self):
        self.reserve();before=(self.native.read_bytes(),self.mirror.read_bytes())
        result=self.lane.preflight(BOARD)
        self.assertEqual(result['quotaObservation']['used'],0)
        self.assertEqual(len(result['uncertainCheckReservations']),1)
        self.assertEqual(result['uncertainCheckReservations'][0]['slots'],1)
        self.assertEqual(before,(self.native.read_bytes(),self.mirror.read_bytes()))
        self.assertEqual(self.official.posts,0)

    def test_remaining_zero_or_one_blocks_before_post_but_two_can_inspect(self):
        self.reserve()
        for left,code in [(0,'CHECK_QUOTA'),(1,'CHECK_QUOTA_SYNC_PENDING')]:
            self.official.left=left
            with self.assertRaisesRegex(A.PreflightQuotaError,code):self.lane.run(OCC,'check')
            self.assertEqual(self.official.posts,0)
        self.official.left=2
        self.assertEqual(self.lane.run(OCC,'inspect')['state'],'eligible')

    def test_other_board_has_no_reserved_charge(self):
        self.reserve('muse_prefill_layer');self.official.left=1
        result=self.lane.preflight(BOARD)
        self.assertEqual(result['quotaObservation']['used'],9)
        self.assertEqual(result['uncertainCheckReservations'][0]['problem'],'muse_prefill_layer')

    def test_reservation_preserves_existing_minimum_margin(self):
        self.reserve();self.p['minimumChecksRemaining']=2;self.official.left=3
        with self.assertRaises(A.PreflightQuotaError):self.lane.run(OCC,'check')
        self.official.left=4;self.assertEqual(self.lane.run(OCC,'inspect')['state'],'eligible')
        self.assertEqual(self.official.posts,0)

    def test_unknown_survives_midnight_without_yesterdays_floor(self):
        day=int(NOW//86400)*86400;self.reserve(epoch=day-120)
        result=self.lane.preflight(BOARD)
        self.assertEqual(result['quotaObservation']['used'],0)
        self.assertEqual(result['uncertainCheckReservations'][0]['intentEpoch'],day-120)

    def test_two_acknowledgements_do_not_capitalize_the_same_reservation(self):
        self.reserve();first=self.lane.preflight(BOARD)
        for number in [1,2]:
            job=f'{number:016x}';row={'state':'job_recorded','mode':'check','intentEpoch':NOW-1,
                'identity':{'problem':BOARD,'sourceSha256':str(number)*64},'receipt':{'job_id':job},
                'quotaObservation':first['quotaObservation']}
            # The first observation predates the first synthetic ACK. The next
            # preserves its current floor without adding the uncertainty to used.
            row['quotaObservation']['checkedAt']=NOW-2
            for key in ['stateDirectory','legacyLedger']:
                IO.atomic_record(pathlib.Path(self.p[key])/(BOARD+'-'+str(number)*64+'-check.json'),row)
            self.official.jobs.append({'job_id':job,'problem':BOARD,'mode':'check','submitted_at':NOW-1})
            first=self.lane.preflight(BOARD)
            self.assertEqual(first['quotaObservation']['used'],number)
            self.assertEqual(len(first['uncertainCheckReservations']),1)

    def test_own_candidate_remains_unresendable_across_days(self):
        self.reserve(own=True,epoch=NOW-86400)
        with self.assertRaisesRegex(ValueError,'UNCERTAIN_RECONCILE_NO_RESEND'):self.lane.run(OCC,'check')
        row=A.scan(self.lane,self.f.root/'status.json','check')['outcomes'][0]
        self.assertEqual(row['state'],'uncertain_owned');self.assertIsNone(row['postAttempted'])
        self.assertEqual(self.official.posts,0)

    def test_same_source_from_another_occurrence_is_also_unresendable(self):
        row,pin=self.reserve(own=True);row['identity']['occurrenceId']=SUPERVISOR
        IO.atomic_record(self.native,row);IO.atomic_record(self.mirror,row)
        with self.assertRaisesRegex(ValueError,'SOURCE_PREVIOUS_PROVENANCE'):self.lane.run(OCC,'check')
        self.assertEqual(self.official.posts,0)

    def test_unlisted_unknown_and_missing_or_changed_mirror_block_globally(self):
        row,_=self.reserve();original=self.mirror.read_bytes()
        for change in ['missing','changed','unlisted']:
            if change=='missing':self.mirror.unlink()
            elif change=='changed':IO.atomic_record(self.mirror,{**row,'automaticPostRetry':True})
            else:
                self.mirror.write_bytes(original)
                IO.atomic_record(pathlib.Path(self.p['legacyLedger'])/(BOARD+'-'+'e'*64+'-check.json'),{'state':'submission_outcome_unknown'})
            with self.assertRaises(A.AccountPreflightWait):self.lane.preflight(BOARD)
        self.assertEqual(self.official.posts,0)

    def test_native_authority_cannot_be_replaced_by_legacy_only(self):
        self.reserve();self.native.unlink()
        with self.assertRaises(A.AccountPreflightWait):self.lane.preflight(BOARD)

    def test_only_one_reconciled_mirror_keeps_global_block(self):
        row,_=self.reserve();IO.atomic_record(self.native,{**row,'state':'job_recorded','receipt':{'job_id':'abcdef0123456789'}})
        with self.assertRaises(A.AccountPreflightWait):self.lane.preflight(BOARD)

    def test_tampered_historical_proofs_are_not_reservation_authority(self):
        row,pin=self.reserve();stem=self.native.name[:-11]
        paths=[pathlib.Path(pin['originPolicy']['path']),self.native.parent/(stem+'-audit.json'),
               self.native.parent/(stem+'.py'),self.native.parent/(stem+'-evidence')/'stdout.json']
        for path in paths:
            old=path.read_bytes();path.chmod(0o600);path.write_bytes(b'{}');path.chmod(0o400)
            with self.subTest(path=path),self.assertRaises(A.AccountPreflightWait):self.lane.preflight(BOARD)
            path.chmod(0o600);path.write_bytes(old);path.chmod(0o400)

    def test_origin_account_transport_mode_and_pin_are_not_assumed(self):
        for change in [lambda p:p.update(accountId='another'),lambda p:p.update(model='another'),
                       lambda p:p.update(allowedActions=['score']),lambda p:p.update(transportHelper={'path':'/elsewhere','sha256':'e'*64})]:
            with self.subTest(change=change):
                fixture=UncertainCheckReservationTests();fixture.setUp()
                try:
                    fixture.reserve(old_change=change)
                    with self.assertRaises(A.AccountPreflightWait):fixture.lane.preflight(BOARD)
                finally:fixture.doCleanups()

    def test_forged_multiplicity_and_receipt_do_not_become_reservations(self):
        row,pin=self.reserve()
        for field,value in [('automaticPostRetry',True),('mode','scored'),('state','submission_intent'),('exitCode',0)]:
            bad={**row,field:value};IO.atomic_record(self.native,bad);IO.atomic_record(self.mirror,bad)
            pin['ledgerSHA256']=A.digest(self.native.read_bytes())
            with self.assertRaises(A.AccountPreflightWait):self.lane.preflight(BOARD)

    def test_later_known_receipt_dominates_old_pin_without_rewrite(self):
        row,_=self.reserve();job='abcdef0123456789'
        known={**row,'state':'job_recorded','receipt':{'job_id':job}}
        IO.atomic_record(self.native,known);IO.atomic_record(self.mirror,known)
        self.official.jobs=[{'job_id':job,'problem':BOARD,'mode':'check','submitted_at':row['intentEpoch']}]
        result=self.lane.preflight(BOARD)
        self.assertEqual(result['uncertainCheckReservations'],[])
        self.assertEqual(result['quotaObservation']['used'],1)

    def test_new_unknown_during_second_audit_blocks_before_candidate_freeze(self):
        self.reserve();original=self.lane.audit.audit;calls=[]
        def changed(oid):
            result=original(oid);calls.append(oid)
            if len(calls)==2:IO.atomic_record(pathlib.Path(self.p['legacyLedger'])/(BOARD+'-'+'f'*64+'-check.json'),{'state':'submission_intent'})
            return result
        self.lane.audit.audit=changed
        with self.assertRaises(A.AccountPreflightWait):self.lane.run(OCC,'check')
        self.assertFalse((pathlib.Path(self.p['stateDirectory'])/(BOARD+'-'+A.digest(self.f.candidate)+'.py')).exists())
        self.assertEqual(self.official.posts,0)

    def test_midnight_or_stale_get_requires_fresh_observation(self):
        self.reserve();result=self.lane.preflight(BOARD)
        self.now+=61
        with self.assertRaisesRegex(A.PreflightQuotaError,'CHECK_QUOTA_SYNC_PENDING'):self.lane.reservation_read_set(result)
        self.now=(int(NOW//86400)+1)*86400
        with self.assertRaisesRegex(A.PreflightQuotaError,'CHECK_QUOTA_SYNC_PENDING'):self.lane.reservation_read_set(result)

    def test_job_window_rejects_duplicate_conflicting_or_truncated_observation(self):
        self.reserve();row={'job_id':'abcdef0123456789','problem':BOARD,'mode':'check','submitted_at':NOW-1}
        cases=[{'jobs':[row],'has_more':True},[row,row],[row,{**row,'problem':'muse_prefill_layer'}],
               [row,{**row,'job_id':'abcdef0123456790','submitted_at':NOW}],
               [{**row,'job_id':f'{i:016x}'} for i in range(50)]]
        for response in cases:
            with self.subTest(response=response),self.assertRaises(A.PreflightQuotaError):self.lane.reservation_jobs_window(response,NOW)
        self.assertEqual(self.lane.reservation_jobs_window({'jobs':[row]},NOW),[row])

    def test_midnight_during_official_gets_is_not_a_valid_quota_observation(self):
        self.reserve();self.now=(int(NOW//86400)+1)*86400-1;self.p['expiresAt']=self.now+3600
        original=self.official.fetch
        def cross(route,public=False):
            value=original(route,public)
            if route=='problems':self.now+=2
            return value
        self.official.fetch=cross
        with self.assertRaises(A.PreflightQuotaError):self.lane.preflight(BOARD)
        self.assertEqual(self.official.posts,0)

    def test_new_unrelated_intent_preserves_reservation_separate_from_floor(self):
        self.reserve();before=self.native.read_bytes()
        result=self.lane.run(OCC,'check');self.assertEqual(result['state'],'job_recorded')
        path=pathlib.Path(self.p['stateDirectory'])/(BOARD+'-'+A.digest(self.f.candidate)+'-check.json')
        value=A.strict_json(path.read_bytes())
        self.assertEqual(value['quotaObservation']['used'],0)
        self.assertEqual(len(value['uncertainCheckReservations']),1)
        self.assertEqual(self.native.read_bytes(),before);self.assertEqual(self.official.posts,1)

    def test_owned_marker_is_cpu_only_and_preserves_original_uncertainty(self):
        self.reserve(own=True);before=self.native.read_bytes()
        outcome=A.scan(self.lane,self.f.root/'proof-status.json','check')['outcomes'][0]
        self.assertEqual((outcome['state'],outcome['code'],outcome['postAttempted'],outcome['jobId']),
            ('uncertain_owned','UNCERTAIN_RECONCILE_NO_RESEND',None,None))
        proof=outcome['uncertainCheckReservation']
        self.assertEqual(set(proof),{'version','slots','verifiedAt','detailSHA256','policySHA256','ownSourceUnretryable','effectReconciled','intent'})
        self.assertEqual(proof['detailSHA256'],A.digest(self.f.detail));self.assertEqual(proof['policySHA256'],self.lane.policy_sha)
        self.assertTrue(proof['ownSourceUnretryable']);self.assertFalse(proof['effectReconciled'])
        self.assertEqual(proof['intent'],self.p['uncertainCheckReservations']['intents'][0])
        self.assertEqual(self.native.read_bytes(),before);self.assertEqual(self.official.posts,0)

    def test_alias_marker_binds_current_detail_and_original_intent(self):
        self.reserve(own=True);row=A.strict_json(self.native.read_bytes());pin=self.p['uncertainCheckReservations']['intents'][0]
        row['identity']['occurrenceId']=SUPERVISOR;pin['occurrenceId']=SUPERVISOR
        stem=self.native.name[:-11];audit_path=self.native.parent/(stem+'-audit.json');audit=A.strict_json(audit_path.read_bytes())
        audit['occurrenceId']=SUPERVISOR;raw=A.canonical(audit);audit_path.chmod(0o600);audit_path.write_bytes(raw);audit_path.chmod(0o400)
        row['identity']['auditSHA256']=A.digest(raw);IO.atomic_record(self.native,row);IO.atomic_record(self.mirror,row)
        pin['ledgerSHA256']=A.digest(self.native.read_bytes())
        outcome=A.scan(self.lane,self.f.root/'alias-status.json','check')['outcomes'][0]
        self.assertEqual(outcome['code'],'SOURCE_PREVIOUS_PROVENANCE');self.assertIsNone(outcome['postAttempted'])
        self.assertEqual(outcome['uncertainCheckReservation']['intent']['occurrenceId'],SUPERVISOR)
        self.assertEqual(outcome['uncertainCheckReservation']['detailSHA256'],A.digest(self.f.detail));self.assertEqual(self.official.posts,0)

    def test_changed_native_detail_or_new_unknown_suppresses_marker(self):
        self.reserve(own=True);original=self.f.runtime.api;calls=[]
        def changed(method,path):
            value=original(method,path);calls.append(path)
            if getattr(self.f.audit,'_completed_audit_detail_sha',None):value['occurrence']['state']='attention'
            return value
        self.f.runtime.api=changed
        outcome=A.scan(self.lane,self.f.root/'changed-status.json','check')['outcomes'][0]
        self.assertEqual(outcome['state'],'uncertain_owned');self.assertNotIn('uncertainCheckReservation',outcome)
        self.f.runtime.api=original
        IO.atomic_record(pathlib.Path(self.p['legacyLedger'])/(BOARD+'-'+'e'*64+'-check.json'),{'state':'submission_intent'})
        outcome=A.scan(self.lane,self.f.root/'unknown-status.json','check')['outcomes'][0]
        self.assertEqual(outcome['state'],'uncertain_owned');self.assertNotIn('uncertainCheckReservation',outcome)
        self.assertEqual(self.official.posts,0)

    def test_failed_audit_or_untyped_marker_cannot_authorize_cpu_reservation(self):
        self.reserve(own=True)
        self.lane.audit.audit=mock.Mock(side_effect=ValueError('QA_TEST_BINDING'))
        outcome=A.scan(self.lane,self.f.root/'bad-audit.json','check')['outcomes'][0]
        self.assertNotIn('uncertainCheckReservation',outcome)
        forged=ValueError('UNCERTAIN_RECONCILE_NO_RESEND');forged.uncertain_check_reservation={'version':1}
        self.lane.run=mock.Mock(side_effect=forged)
        outcome=A.scan(self.lane,self.f.root/'forged-marker.json','check')['outcomes'][0]
        self.assertEqual(outcome['state'],'uncertain_owned');self.assertNotIn('uncertainCheckReservation',outcome)
        self.assertIsNone(outcome['postAttempted']);self.assertEqual(self.official.posts,0)


class JobsObservationTests(unittest.TestCase):
    def setUp(self):
        self.f=Fixture();self.f.setUp();self.addCleanup(self.f.tearDown)
        self.p,self.lane,self.official=self.f.p,self.f.lane,self.f.official
        self.p['transportHelper']={'path':str(self.f.root/'transport.py'),'sha256':'d'*64}
        self.p['minimumChecksRemaining']=0
        self.now=NOW;self.day=int(NOW//86400)*86400;self.team='isolated-team'
        self.lane.clock=lambda:self.now
        self.original_fetch=self.official.fetch
        self.official.fetch=self.fetch
        self.baseline=[self.row(i+1,NOW-1000-i if i<43 else self.day-100-i) for i in range(50)]
        self.seed=self.capture(self.baseline)
        self.p['jobsObservation']={'version':1,'indexPath':str(pathlib.Path(self.p['stateDirectory'])/'jobs-observation-v1-testcase.json'),'bootstrap':self.seed}
        self.index=pathlib.Path(self.p['jobsObservation']['indexPath'])

    def row(self,i,stamp=None,mode='check'):
        return {'job_id':f'{i:016x}','problem':BOARD,'mode':mode,'submitted_at':NOW-100+i if stamp is None else stamp}

    def fetch(self,route,public=False):
        if route=='me':return {'team':self.team,'rules':{'version':'test-rules-v1','accepted_at':1}}
        if route=='problems':return {'tracks':{'kernels':{'open':True}},'problems':[{'name':BOARD,'limits':{'checks':{'per_day':200,'used':0,'left':200}}}]}
        return self.original_fetch(route,public)

    def put(self,name,value):
        path=self.f.root/name;raw=A.canonical(value);IO.immutable_snapshot(path,raw)
        return {'path':str(path),'sha256':A.digest(raw)}

    def capture(self,jobs):
        before=self.fetch('me');policy_raw=A.canonical(self.p)
        refs={'meBefore':self.put('bootstrap-me-before.json',before),'meAfter':self.put('bootstrap-me-after.json',before),
              'jobs':self.put('bootstrap-jobs.json',{'jobs':jobs}),'policy':self.put('bootstrap-policy.json',self.p)}
        return self.put('bootstrap-capture.json',{'version':1,'startEpoch':self.now-2,'endEpoch':self.now-1,
            'refs':refs,'postTransport':self.p['transportHelper'],'policySHA256':A.digest(policy_raw),
            'teamSHA256':A.digest(self.team),'POSTs':0})

    def observe(self,rows=None):
        return self.lane.observe_jobs({'jobs':self.baseline if rows is None else rows},self.fetch('me'),self.now-1)

    def append(self,count):
        return [self.row(100+i,NOW-100-i) for i in range(count)]+self.baseline

    def test_real_window50_to51_to60_preserves_all_ids(self):
        jobs,proof=self.observe();self.assertEqual(len(jobs),50)
        extended=self.append(1);jobs,proof=self.observe(extended[:50]);self.assertEqual(len(jobs),51)
        later=[self.row(200+i,NOW-50-i) for i in range(9)]+extended
        jobs,proof=self.observe(later[:50]);self.assertEqual(len(jobs),60)
        self.assertEqual(jobs,later);self.lane.jobs_observation_read_set({'jobsObservation':proof})
        self.assertEqual(self.official.posts,0)

    def test_bootstrap_can_anchor_current_full_day_window(self):
        rows=self.append(10);self.assertTrue(all(r['submitted_at']>=self.day for r in rows[:50]))
        jobs,proof=self.observe(rows[:50]);self.assertEqual(len(jobs),60)
        self.assertEqual(A.strict_json(A.pinned(proof['report']))['previousReport'],self.seed)

    def test_more_than50_new_jobs_or_missing_head_is_a_gap(self):
        self.observe();before=self.index.read_bytes()
        for rows in [[self.row(500+i,NOW-100-i) for i in range(50)],self.baseline[1:]]:
            with self.assertRaises(A.PreflightQuotaError):self.observe(rows)
            self.assertEqual(self.index.read_bytes(),before)
        self.assertEqual(self.official.posts,0)

    def test_overlap_reorder_deletion_identity_change_and_reused_id_fail_closed(self):
        jobs,proof=self.observe();original=self.index.read_bytes()
        reused=self.append(12);self.observe(reused[:50]);original=self.index.read_bytes()
        cases=[reused[1:51],copy.deepcopy(reused[:50]),copy.deepcopy(reused[:50]),
            [copy.deepcopy(self.baseline[-1])]+reused[:49]]
        cases[1][2]['mode']='run';cases[2][2],cases[2][3]=cases[2][3],cases[2][2]
        cases[3][0]['submitted_at']=self.now-1
        for rows in cases:
            with self.assertRaises(A.PreflightQuotaError):self.observe(rows)
            self.assertEqual(self.index.read_bytes(),original)

    def test_mutable_status_changes_do_not_change_job_identity(self):
        self.observe();changed=[{**r,'status':'done','speedup':1.5} for r in self.baseline]
        jobs,_=self.observe(changed);self.assertEqual(jobs,self.baseline)

    def test_token_rotation_same_team_is_ok_other_team_is_not(self):
        self.observe();self.official.token='different-secret-not-observed'
        self.observe();before=self.index.read_bytes();self.team='different-team'
        with self.assertRaises(A.PreflightQuotaError):self.observe()
        self.assertEqual(self.index.read_bytes(),before)

    def test_team_changes_between_authenticated_gets_do_not_publish(self):
        before=self.fetch('me');self.team='other-team'
        with self.assertRaises(A.PreflightQuotaError):self.lane.observe_jobs({'jobs':self.baseline},before,self.now-1)
        self.assertFalse(self.index.exists())

    def test_utc_rollover_requires_current_complete_window_not_old_day_floor(self):
        self.observe();self.now=self.day+86400+30
        jobs,proof=self.observe(self.baseline)
        self.assertEqual(proof['utcDayStart'],self.day+86400)
        self.assertEqual(A.strict_json(A.pinned(proof['report']))['coverage'],'utc_baseline')
        self.assertEqual(self.lane.quota_observation([],jobs,BOARD,{'used':0,'left':10,'per_day':10},self.now)['used'],0)

    def test_utc_crossing_or_slow_reads_cannot_publish_or_create_intent(self):
        for start in [self.now-61,self.day-1]:
            with self.assertRaises(A.PreflightQuotaError):self.lane.observe_jobs({'jobs':self.baseline},self.fetch('me'),start)
        self.assertFalse(self.index.exists());self.assertEqual(self.official.posts,0)

    def test_read_set_rejects_stale_changed_index_namespace_and_policy(self):
        jobs,proof=self.observe();checks={'jobsObservation':proof};raw=self.index.read_bytes()
        self.now+=61
        with self.assertRaises(A.PreflightQuotaError):self.lane.jobs_observation_read_set(checks)
        self.now=NOW
        IO.atomic_record(self.index,{'version':1,'report':self.seed})
        with self.assertRaises(A.PreflightQuotaError):self.lane.jobs_observation_read_set(checks)
        self.index.write_bytes(raw);self.lane.policy_sha='c'*64
        with self.assertRaises(A.PreflightQuotaError):self.lane.jobs_observation_read_set(checks)

    def test_existing_index_cannot_silently_cross_code_or_policy(self):
        self.observe();before=self.index.read_bytes();self.p['adapterSHA256']='c'*64
        with self.assertRaises(A.PreflightQuotaError):self.observe()
        self.assertEqual(self.index.read_bytes(),before)

    def test_explicit_pinned_reanchor_new_namespace_preserves_complete_chain(self):
        self.observe();rows=self.append(12);jobs,proof=self.observe(rows[:50]);before=self.index.read_bytes()
        old_index=self.index;self.lane.policy_sha='e'*64;self.p['adapterSHA256']='f'*64
        self.p['jobsObservation']={'version':1,'indexPath':str(old_index.with_name('jobs-observation-v1-reanchor.json')),'bootstrap':proof['report']}
        self.index=pathlib.Path(self.p['jobsObservation']['indexPath'])
        new,ref=self.observe(rows[:50]);self.assertEqual(new,jobs)
        self.assertEqual(old_index.read_bytes(),before);self.lane.jobs_observation_read_set({'jobsObservation':ref})

    def test_default_full50_guard_remains_unchanged(self):
        self.p.pop('jobsObservation')
        with self.assertRaises(A.PreflightQuotaError):self.lane.reservation_jobs_window(self.append(12)[:50],NOW)

    def test_retained_acknowledged_job_outside_window_still_counts_once(self):
        self.observe();rows=self.append(12);jobs,_=self.observe(rows[:50]);old=self.baseline[42]
        record={'state':'job_recorded','mode':'check','intentEpoch':old['submitted_at'],
                'identity':{'problem':BOARD,'sourceSha256':'f'*64},'receipt':{'job_id':old['job_id']}}
        self.assertNotIn(old['job_id'],[r['job_id'] for r in rows[:50]])
        q=self.lane.quota_observation([record,record],jobs,BOARD,{'used':0,'left':200,'per_day':200},NOW)
        self.assertEqual(len(q['knownJobIds']),55);self.assertEqual(q['used'],55)

    def test_run_jobs_participate_in_rate_but_not_check_quota(self):
        rows=[self.row(1000+i,NOW-1-i,'run') for i in range(6)]
        self.official.jobs=rows
        with self.assertRaisesRegex(ValueError,'EXTERNAL_RATE_WAIT'):self.lane.preflight(BOARD)
        self.assertEqual(self.lane.quota_observation([],rows,BOARD,{'used':0,'left':10,'per_day':10},NOW)['used'],0)
        self.assertEqual(self.official.posts,0)

    def test_preintent_guard_is_after_audit_and_preserves_single_post_boundary(self):
        self.official.jobs=self.baseline;original=self.lane.audit.audit;calls=[]
        def late(oid):
            result=original(oid);calls.append(oid)
            if len(calls)==2:self.now+=61
            return result
        self.lane.audit.audit=late
        with self.assertRaises(A.PreflightQuotaError):self.lane.run(OCC,'check')
        self.assertEqual(calls,[OCC,OCC]);self.assertEqual(self.official.posts,0)
        self.assertEqual(list(pathlib.Path(self.p['stateDirectory']).glob('*-check.json')),[])

    def test_new_intent_pins_get_observation_but_transport_authority_is_unchanged(self):
        self.official.jobs=self.baseline;transport=copy.deepcopy(self.p['transportHelper'])
        self.assertEqual(self.lane.run(OCC,'check')['state'],'job_recorded')
        intent=A.strict_json((pathlib.Path(self.p['stateDirectory'])/(BOARD+'-'+A.digest(self.f.candidate)+'-check.json')).read_bytes())
        self.assertIn('jobsObservation',intent);self.assertEqual(self.p['transportHelper'],transport)
        self.assertEqual(self.official.posts,1)

    def test_duplicate_ids_envelope_and_unsafe_time_are_not_coverage(self):
        for rows in [[self.baseline[0],self.baseline[0]],{**self.baseline[0],'submitted_at':float('inf')}]:
            with self.assertRaises(A.PreflightQuotaError):self.observe(rows if isinstance(rows,list) else [rows])
        with self.assertRaises(ValueError):A.jobs_window_identity({'jobs':self.baseline,'next':'cursor'},NOW)

    def test_identity_limit_and_report_bytes_are_bounded(self):
        self.observe();previous=self.baseline
        for batch in range(3):
            current=[self.row(2000+batch*49+i,NOW-500+batch*60-i) for i in range(49)]+previous
            self.observe(current[:50]);previous=current
        before=self.index.read_bytes();current=[self.row(3000+i,NOW-100-i) for i in range(4)]+previous
        with self.assertRaises(A.PreflightQuotaError):self.observe(current[:50])
        self.assertEqual(self.index.read_bytes(),before)
        huge=[{**r,'ignoredMutable': 'x'*22000} for r in previous[:50]]
        with self.assertRaises(A.PreflightQuotaError):self.observe(huge)

    def test_index_cas_failure_cannot_publish_an_observation(self):
        self.observe();original=IO.immutable_snapshot
        def changed(path,raw):
            original(path,raw)
            if pathlib.Path(path).parent.name=='jobs-observation-reports':IO.atomic_record(self.index,{'version':1,'report':self.seed})
        with mock.patch.object(IO,'immutable_snapshot',side_effect=changed),self.assertRaises(A.PreflightQuotaError):self.observe()
        self.assertEqual(self.official.posts,0)

    def test_clock_rollback_cannot_rejuvenate_an_existing_observation(self):
        self.observe();before=self.index.read_bytes();self.now-=1
        with self.assertRaises(A.PreflightQuotaError):self.observe()
        self.assertEqual(self.index.read_bytes(),before)

    def test_bootstrap_pin_is_rechecked_before_intent(self):
        _,proof=self.observe();path=pathlib.Path(self.seed['path']);path.chmod(0o600);path.write_bytes(b'{}');path.chmod(0o400)
        with self.assertRaises(A.PreflightQuotaError):self.lane.jobs_observation_read_set({'jobsObservation':proof})
        self.assertEqual(self.official.posts,0)

    def qualifier(self):
        spec=importlib.util.spec_from_file_location('get_chain_qualifier',pathlib.Path(__file__).parents[1]/'tools/qualify_jobs_observation.py')
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        return module

    def qualify(self,ref):
        policy=copy.deepcopy(self.p);policy['jobsObservation']['bootstrap']=ref
        policy['adapterSHA256']=A.digest(pathlib.Path(A.__file__).read_bytes())
        return self.qualifier().qualify(A,policy,'e'*64,ref)

    def test_qualifier_reconstructs_complete_chain_and_explicit_reanchor(self):
        self.observe();_,head=self.observe(self.append(12)[:50])
        proof=self.qualify(head['report']);self.assertEqual(proof['jobCount'],62)
        self.assertEqual(len(proof['references']),2);self.assertTrue(proof['admissionRequiresNewFreshGETs'])
        old=head['report'];self.p['jobsObservation']={'version':1,'indexPath':str(self.index.with_name('jobs-observation-v1-newscope.json')),'bootstrap':old}
        self.lane.policy_sha='e'*64;self.p['adapterSHA256']='f'*64
        _,new=self.observe(self.append(12)[:50]);self.assertEqual(self.qualify(new['report'])['jobCount'],62)

    def test_qualifier_rejects_omitted_historical_id_outside_current_window(self):
        self.observe();_,head=self.observe(self.append(12)[:50]);value=A.strict_json(A.pinned(head['report']))
        value['jobs'].pop();bad=self.put('bad-omitted-history.json',value)
        with self.assertRaisesRegex(ValueError,'QUALIFICATION_UNION_CHANGED'):self.qualify(bad)

    def test_qualifier_rejects_dangling_cycle_and_implicit_authority_handover(self):
        self.observe();_,head=self.observe(self.append(12)[:50]);original=A.strict_json(A.pinned(head['report']))
        value=copy.deepcopy(original);value['previousReport']={'path':head['report']['path'],'sha256':'0'*64}
        bad=self.put('bad-chain-cycle.json',value)
        with self.assertRaises(ValueError):self.qualify(bad)
        value=copy.deepcopy(original);value['authority']['policySHA256']='0'*64
        bad=self.put('bad-implicit-authority.json',value)
        with self.assertRaisesRegex(ValueError,'QUALIFICATION_REANCHOR_NOT_EXPLICIT'):self.qualify(bad)

    def test_qualifier_bootstrap_capture_preserves_authenticated_raw_pins(self):
        proof=self.qualify(self.seed);self.assertEqual(proof['jobCount'],50)
        self.assertEqual(proof['GETs'],0);self.assertEqual(proof['POSTs'],0)
        capture=A.strict_json(A.pinned(self.seed));path=pathlib.Path(capture['refs']['jobs']['path'])
        path.chmod(0o600);path.write_bytes(b'{"jobs":[]}');path.chmod(0o400)
        with self.assertRaisesRegex(ValueError,'PIN_CHANGED'):self.qualify(self.seed)

    def test_qualifier_requires_policy_pin_for_the_exact_adapter(self):
        with self.assertRaisesRegex(ValueError,'QUALIFICATION_ADAPTER_POLICY_PIN'):
            self.qualifier().qualify(A,self.p,'e'*64,self.seed)


class OptionalArtifactBoundsTests(unittest.TestCase):
    def setUp(self):
        self.f = Fixture(); self.f.setUp(); self.addCleanup(self.f.tearDown)
        self.raw = b'{"emulation":[]}' + b' ' * (1153946 - len(b'{"emulation":[]}'))

    def attach(self, name="emulation.json", raw=None, attempt=IDS[0]):
        raw = self.raw if raw is None else raw
        row = self.f.artifact(name, raw, attempt); self.f.detail["artifacts"].append(row)
        if attempt == IDS[0]:
            self.f.put(self.handoff(row), raw)
        return row

    def handoff(self, row, attempt=IDS[1]):
        return pathlib.Path(self.f.paths["workerRunsDirectory"]) / attempt / "home/inputs" / row["id"] / row["name"]

    def v2(self, count=3, failed=False):
        helper = NativeQAV2Tests(); helper.f = self.f; helper.d = self.f.detail; helper.a = self.f.audit
        helper.optin(count, failed)
        return helper

    def test_qa1_large_optional_preserves_full_bytes_hashes_and_frozen_snapshot(self):
        row = self.attach(); self.attach("qa-extra.json", self.raw, IDS[1])
        audit, files, _ = self.f.audit.audit(OCC)
        self.assertEqual(files["produce--emulation.json"], self.raw)
        self.assertEqual(audit["files"]["produce--emulation.json"], row["digest"])
        result = self.f.lane.run(OCC, "check")  # FakeOfficial only.
        self.assertEqual(result["state"], "job_recorded")
        frozen = pathlib.Path(self.f.paths["stateDirectory"]) / (BOARD + "-" + A.digest(self.f.candidate) + "-evidence/produce--emulation.json")
        self.assertEqual(frozen.read_bytes(), self.raw)
        self.assertEqual(self.f.official.posts, 1)

    def test_qa2_three_attempts_verify_large_optional_in_every_handoff(self):
        row = self.attach(); self.v2()
        audit, files, _ = self.f.audit.audit(OCC)
        self.assertEqual(len(audit["qaAttemptHistory"]), 3)
        self.assertEqual(files["produce--emulation.json"], self.raw)
        prior = self.f.detail["attempts"][1]
        self.f.put(self.handoff(row, prior["id"]), b'!' + self.raw[1:])
        with self.assertRaisesRegex(ValueError, "NATIVE_QA_DEPENDENCY_HANDOFF"):
            self.f.audit.audit(OCC)
        self.assertEqual(self.f.official.posts, 0)

    def test_required_and_canonical_alias_names_stay_at_one_mib(self):
        for name in A.PRODUCER_FILES | A.QA_FILES | {"base-candidate.py"}:
            row = self.f.artifact(name, self.raw)
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, "ARTIFACT_SIZE"):
                self.f.audit.artifact(row, optional=True, allow_empty=True)
        row = self.f.artifact("renamed-source.py", self.raw)
        with self.assertRaisesRegex(ValueError, "ARTIFACT_SIZE"):
            self.f.audit.artifact(row)  # Source lookups never request optional access.

    def test_required_large_report_fails_actual_qa1_and_qa2_audit(self):
        row = next(r for r in self.f.detail["artifacts"] if r["name"] == "qa-verification.json")
        original = copy.deepcopy(row)
        for version in (1, 2):
            if version == 2:
                row.clear(); row.update(original); self.v2(1)
            row.update(size=len(self.raw), digest=A.digest(self.raw))
            self.f.put(pathlib.Path(self.f.paths["artifactDirectory"]) / row["digest"], self.raw)
            with self.subTest(version=version), self.assertRaisesRegex(ValueError, "ARTIFACT_SIZE"):
                self.f.audit.audit(OCC)

    def test_optional_file_limit_and_metadata_types_are_checked_before_read(self):
        row = self.f.artifact("large.log", b'x' * A.OPTIONAL_ARTIFACT_MAX_BYTES)
        self.assertEqual(len(self.f.audit.artifact(row, optional=True)), A.OPTIONAL_ARTIFACT_MAX_BYTES)
        for size in (A.OPTIONAL_ARTIFACT_MAX_BYTES + 1, -1, True, 1.0):
            with self.subTest(size=size), mock.patch.object(IO, "read_regular", side_effect=AssertionError("must reject metadata first")):
                with self.assertRaisesRegex(ValueError, "ARTIFACT_SIZE"):
                    self.f.audit.artifact(dict(row, size=size), optional=True)
        with self.assertRaisesRegex(ValueError, "ARTIFACT_SIZE"):
            self.f.audit.artifact(row, optional=1)

    def test_total_limit_counts_duplicate_content_and_rejects_before_attachment_reads(self):
        sizes = [{"size": A.OPTIONAL_ARTIFACT_MAX_BYTES}] * 3
        self.f.audit.artifact_budget(sizes)
        with self.assertRaisesRegex(ValueError, "ARTIFACT_TOTAL_SIZE"):
            self.f.audit.artifact_budget(sizes + [{"size": 1}])
        for i in range(3):
            row = self.attach("bulk-" + str(i), b'x')
            row["size"] = A.OPTIONAL_ARTIFACT_MAX_BYTES
        with mock.patch.object(self.f.audit, "artifact", side_effect=AssertionError("inventory must be checked first")):
            with self.assertRaisesRegex(ValueError, "ARTIFACT_TOTAL_SIZE"):
                self.f.audit.audit(OCC)

    def test_v2_all_prior_qa_inventories_are_bounded_before_source_reads(self):
        self.v2(3)
        prior = self.f.detail["attempts"][1]
        for i in range(3):
            row = self.attach("prior-extra-" + str(i), b'x', prior["id"])
            row["size"] = A.OPTIONAL_ARTIFACT_MAX_BYTES
        with mock.patch.object(self.f.audit, "artifact", side_effect=AssertionError("budget first")):
            with self.assertRaisesRegex(ValueError, "ARTIFACT_TOTAL_SIZE"):
                self.f.audit.qa_attempt_chain(self.f.detail)

    def test_exact_24_mib_producer_inventory_passes_and_one_more_byte_fails(self):
        used = sum(r["size"] for r in self.f.detail["artifacts"] if r["attempt_id"] == IDS[0])
        remaining = A.ATTEMPT_ARTIFACT_MAX_BYTES - used
        for index in range(3):
            size = min(remaining, A.OPTIONAL_ARTIFACT_MAX_BYTES)
            self.attach("bounded-" + str(index) + ".log", bytes([65 + index]) * size)
            remaining -= size
        self.assertEqual(remaining, 0)
        audit, files, _ = self.f.audit.audit(OCC)
        self.assertEqual(sum(len(raw) for name, raw in files.items() if name.startswith("produce--")), A.ATTEMPT_ARTIFACT_MAX_BYTES)
        self.assertEqual(len(audit["files"]), len(files))
        self.attach("one-more-byte.log", b'x')
        with self.assertRaisesRegex(ValueError, "ARTIFACT_TOTAL_SIZE"):
            self.f.audit.audit(OCC)
        self.assertEqual(self.f.official.posts, 0)

    def test_large_optional_corruption_and_handoff_mismatch_are_not_skipped(self):
        row = self.attach(); path = pathlib.Path(self.f.paths["artifactDirectory"]) / row["digest"]
        self.f.put(path, b'!' + self.raw[1:])
        with self.assertRaisesRegex(ValueError, "ARTIFACT_BYTES_CHANGED"):
            self.f.audit.audit(OCC)
        self.f.put(path, self.raw); self.f.put(self.handoff(row), b'!' + self.raw[1:])
        with self.assertRaisesRegex(ValueError, "QA_PREDECESSOR_CHANGED"):
            self.f.audit.audit(OCC)

    def test_optional_artifact_and_handoff_symlinks_are_rejected(self):
        row = self.attach(); original = pathlib.Path(self.f.paths["artifactDirectory"]) / row["digest"]
        other = self.f.root / "same-bytes.json"; self.f.put(other, self.raw)
        original.unlink(); original.symlink_to(other)
        with self.assertRaises(ValueError): self.f.audit.audit(OCC)
        original.unlink(); self.f.put(original, self.raw)
        handed = self.handoff(row); handed.unlink(); handed.symlink_to(other)
        with self.assertRaises(ValueError): self.f.audit.audit(OCC)
        self.assertEqual(self.f.official.posts, 0)

    def test_optional_change_during_preflight_cannot_reach_effect_boundary(self):
        row = self.attach(); original_preflight = self.f.lane.preflight
        def change(problem):
            result = original_preflight(problem)
            changed = b'!' + self.raw[1:]
            row.update(digest=A.digest(changed))
            self.f.put(pathlib.Path(self.f.paths["artifactDirectory"]) / row["digest"], changed)
            self.f.put(self.handoff(row), changed)
            return result
        with mock.patch.object(self.f.lane, "preflight", side_effect=change):
            with self.assertRaisesRegex(ValueError, "AUDIT_CHANGED_DURING_PREFLIGHT"):
                self.f.lane.run(OCC, "check")
        self.assertEqual(self.f.official.posts, 0)
        self.assertFalse(list(pathlib.Path(self.f.paths["stateDirectory"]).glob("*-check.json")))

    def test_failed_v2_source_keeps_large_optional_inventory_without_acceptance(self):
        self.attach(); helper = self.v2(2, failed=True)
        refs = self.f.origin(helper.source)
        report = A.strict_json(A.pinned(refs[0]))
        self.assertFalse(report["qualityAccepted"])
        self.assertEqual(report["qa"]["state"], "failed")
        self.assertEqual(self.f.official.posts, 0)

    def test_source_alias_with_different_name_does_not_inherit_optional_bound(self):
        source = self.f.origin_fixture(alias=True)
        row = next(r for r in self.f.detail["artifacts"] if r["id"] == source["id"])
        row.update(name="copied-source.py", size=len(self.raw), digest=A.digest(self.raw))
        source.update({k:row[k] for k in ("name", "size", "digest")})
        self.f.put(pathlib.Path(self.f.paths["artifactDirectory"]) / row["digest"], self.raw)
        with self.assertRaisesRegex(ValueError, "SOURCE_ORIGIN_ABSENT"): self.f.origin(source)
        self.assertEqual(self.f.official.posts, 0)


class NativeQA3Tests(unittest.TestCase):
    def setUp(self):
        self.f = Fixture(); self.f.setUp(); self.addCleanup(self.f.tearDown)
        self.d, self.a = self.f.detail, self.f.audit
        self.f.origin_fixture(failed_qa=False)
        o = self.d['occurrence']; o['version'] = 1
        o['definition']['allowedActions'] = ['native.cpu-replay.observe']
        o['definition']['steps'][1].update(parameters={'nativeQAContractVersion':3,
            'cpuReplayObservationVersion':1,'cpuReplayProfile':A.QA3_PROFILE}, dependencyEvidence={
            'version':2,'kind':'controller-cpu-replay','sourceStep':'produce','reportArtifact':'qa-verification.json'})
        for s in self.d['steps']: s['attempt'] = 1
        base = o['input']['experiment']['sourceArtifact']
        base.update(occurrenceId='aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',problem=BOARD)
        self.origin_detail = {'occurrence':{'id':base['occurrenceId'],'definition':{'project':'openkernel'}},
            'artifacts':[base], 'steps':[{'id':'base-step','key':'produce'}],
            'attempts':[{'id':base['attempt_id'],'step_id':'base-step','state':'completed','receipt':{'stopped':True},
                         'fence':'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb'}]}
        self.f.refresh_current_input()
        key=o['source_key'][len('manual:'):-4]
        self.f.put(pathlib.Path(self.f.paths['watcherStateDirectory'])/('intent-'+A.digest(key)+'.json'), A.canonical({
            'state':'recorded','kind':'create','requestKey':key,'result':{'id':CYCLE,'definition':o['definition']},
            'requestDigest':A.digest({'requestKey':key,'definition':o['definition']})}))
        self.semantic = {'version':3,'decision':'approve_for_check','scopePreserved':True,'canonicalPrecisionPreserved':True,
            'evaluatorIndependent':True,'upstreamAttributionPreserved':True,'checkerReview':{'casesMeaningful':True,
            'negativeControlsDiscriminating':True,'noTautologicalPass':True,'rationale':'Independent discriminating arithmetic review.'},
            'blockingFindings':[],'findings':[],'limitations':['CPU-only fixture, GPU performance unmeasured.'],
            'gpuValidated':False,'performanceGainProven':False,'officialActionsPerformed':False}
        executor=self.f.root/'readonly-verifier.py';self.f.put(executor,b'# Fake process is never executed by this fixture.\n');executor.chmod(0o400)
        registration={'version':1,'enabled':True,'executorPath':str(executor),'executorSHA256':A.digest(executor.read_bytes()),
            'stateDirectory':str(self.f.root/'replays'),'user':'nobody','projects':['openkernel'],'profiles':[A.QA3_PROFILE],
            'limits':{'memoryMiB':128,'timeoutSeconds':30,'maxOutputBytes':262144}}
        file=self.f.root/'registration.json';self.f.put(file,A.canonical(registration));file.chmod(0o400)
        self.binding={'path':str(file),'digest':A.digest(file.read_bytes()),'executorPath':str(executor),
            'executorSHA256':registration['executorSHA256'],'profile':A.QA3_PROFILE}
        self.f.p['nativeQA3Registrations']={'version':1,'registrations':[self.binding]}
        self.cpu=copy.deepcopy(self.f.cpu)
        self.make_evidence()
        self.a.qa3_verify=mock.Mock(side_effect=lambda binding,request:copy.deepcopy(self.verified))
        self.f.runtime.api=self.route
        # No copied shell command is required in either actor's transcript.
        for actor in self.d['attempts']:
            self.f.put(pathlib.Path(self.f.paths['workerRunsDirectory'])/actor['id']/'home/sessions/--workspace--'/actor['id']/'session.v3.jsonl',b'{}\n')

    def route(self, method, path):
        self.assertEqual(method,'GET')
        if path.endswith('/cpu-replay'): return copy.deepcopy(self.evidence)
        if path.endswith(self.origin_detail['occurrence']['id']): return copy.deepcopy(self.origin_detail)
        if path.endswith(OCC): return copy.deepcopy(self.d)
        self.fail('Unexpected native route')

    def make_evidence(self):
        producer, qa=self.d['attempts'];o=self.d['occurrence'];ref=lambda row:{k:row[k] for k in ('id','name','digest','size')}
        qa_row=next(r for r in self.d['artifacts'] if r['name']=='qa-verification.json')
        raw=A.canonical(self.semantic);qa_row.update(digest=A.digest(raw),size=len(raw));self.f.put(pathlib.Path(self.f.paths['artifactDirectory'])/qa_row['digest'],raw)
        refs=sorted([ref(r) for r in self.d['artifacts'] if r['attempt_id']==producer['id']],key=lambda r:(r['name'],r['id']))
        source={'stepId':self.d['steps'][0]['id'],'key':'produce','attemptId':producer['id'],'attemptNumber':1,'runFence':producer['fence']}
        snapshot={'version':1,'contractSHA256':A.digest([2,'controller-cpu-replay','produce','qa-verification.json']),
            'occurrenceId':OCC,'source':source,'artifacts':refs,'consumer':{'stepId':self.d['steps'][1]['id'],'key':'independent-qa',
            'attemptId':qa['id'],'attemptNumber':1,'runFence':qa['fence']}}
        qa['context']['manifest']['dependencyEvidence']=copy.deepcopy(snapshot)
        inputs=[]
        for name in ('candidate.py','cpu-checks.py','cpu-result.json','implementation.patch'):
            row=next(r for r in refs if r['name']==name)
            inputs.append({**row,'base64':base64.b64encode((pathlib.Path(self.f.paths['artifactDirectory'])/row['digest']).read_bytes()).decode()})
        base=o['input']['experiment']['sourceArtifact'];base_actor=self.origin_detail['attempts'][0]
        inputs.append({**ref(base),'name':'base-candidate.py','base64':base64.b64encode(self.f.source).decode()})
        base_source={'artifact':ref(base),'source':{'occurrenceId':base['occurrenceId'],'attemptId':base_actor['id'],'runFence':base_actor['fence']},
            'inputSHA256':qa['context']['manifest']['cycleInput']['digest']}
        self.request={'version':1,'requestId':qa['id'],'occurrenceId':OCC,'runFence':qa['fence'],'definitionVersion':1,
            'project':'openkernel','source':source,'artifacts':inputs,'profile':A.QA3_PROFILE,'baseSource':base_source,
            'registrationSHA256':self.binding['digest'],'executorSHA256':self.binding['executorSHA256']}
        request_sha=A.digest(A.qa3_canonical(self.request));out=lambda raw:{'digest':A.digest(raw),'size':len(raw),'base64':base64.b64encode(raw).decode()}
        self.receipt={k:self.request[k] for k in ('requestId','occurrenceId','runFence','project','source','profile','baseSource')}
        unit='agentos-cpu-replay-'+qa['id']+'.service';stop={'stopped':True,'mainPID':0,'populated':False,'unit':unit}
        self.receipt.update(version=1,kind='cpu-replay-observation',requestSHA256=request_sha,registrationSHA256=self.binding['digest'],
            executorSHA256=self.binding['executorSHA256'],observationOnly=True,acceptance=False,
            argv=['/usr/bin/python3','-I','-B','/output/cpu-checks.py','/output/candidate.py','/inputs/base-candidate.py'],
            startedAt='2026-01-01T00:00:00Z',finishedAt='2026-01-01T00:00:01Z',exitCode=0,signal=None,timedOut=False,
            oomKilled=False,cancelled=False,outputLimitExceeded=False,outputsComplete=True,memoryPeakBytes=100,rssPeakBytes=None,
            unit=unit,stopProof=stop,outputs={'stdout':out(b''),'stderr':out(b''),'result':out(A.canonical(self.cpu))})
        self.finish_evidence(snapshot,ref(qa_row))

    def finish_evidence(self,snapshot=None,report_ref=None):
        qa=self.d['attempts'][1]
        snapshot=snapshot or qa['context']['manifest']['dependencyEvidence']
        report_ref=report_ref or self.evidence['semanticReport']
        request_sha=self.receipt['requestSHA256'];raw=A.qa3_canonical(self.receipt);receipt_sha=A.digest(raw)
        handoff={'version':1,'contractVersion':3,'requestId':qa['id'],'requestSHA256':request_sha,'receiptSHA256':receipt_sha,
            'receiptCanonicalization':A.QA3_CANONICALIZATION,'registration':self.binding,'receipt':{'id':qa['id'],
            'name':'cpu-replay-observation.json','digest':receipt_sha,'size':len(raw),'path':'/home/agent/inputs/'+qa['id']+'/cpu-replay-observation.json'}}
        reason=None if A.qa3_semantic(self.semantic) else 'NATIVE_QA3_SEMANTIC_REJECT'
        validation={'version':1,'contractVersion':3,'status':'rejected' if reason else 'matched','code':reason,
            'snapshotContractSHA256':snapshot['contractSHA256'],'source':snapshot['source'],'requestSHA256':request_sha,
            'receiptSHA256':receipt_sha,'receiptCanonicalization':A.QA3_CANONICALIZATION,'handoff':handoff,
            'semanticReport':report_ref,'semanticDecision':self.semantic['decision'],'candidateExecutedByController':True,
            'gpuValidated':False,'performanceGainProven':False}
        qa['context'].update(cpuReplayPreparation={'version':1,'registration':self.binding,'requestSHA256':request_sha,
            'snapshot':snapshot,'phase':'qa_dispatching'},nativeQA3Validation=copy.deepcopy(validation))
        qa['context']['manifest']['nativeQA3']=copy.deepcopy(handoff)
        self.f.put(pathlib.Path(self.f.paths['workerRunsDirectory'])/qa['id']/'home/inputs'/qa['id']/'cpu-replay-observation.json',raw)
        physical={'version':1,'observedAt':A.dt.datetime.now(A.dt.timezone.utc).isoformat(),'receiptSHA256':receipt_sha,'stopProof':self.receipt['stopProof']}
        self.evidence={'version':1,'kind':'native-qa3-controller-evidence','state':'evidence','occurrenceId':OCC,'attemptId':qa['id'],
            'runFence':qa['fence'],'definitionVersion':1,'request':{**self.request,'artifacts':[{k:v for k,v in r.items() if k!='base64'} for r in self.request['artifacts']]},
            'requestSHA256':request_sha,'requestCanonicalization':A.QA3_CANONICALIZATION,'registration':self.binding,'snapshot':snapshot,
            'action':{'key':OCC+':'+qa['id']+':cpu-replay-v1','state':'completed','inputHash':request_sha},'modelDispatched':True,
            'semanticReport':report_ref,'validation':validation,'receipt':self.receipt,'receiptSHA256':receipt_sha,
            'receiptCanonicalization':A.QA3_CANONICALIZATION,'physicalConfirmed':True,'diagnostic':None,'handoff':handoff,'physicalInspection':physical}
        self.verified={'ok':True,'receipt':copy.deepcopy(self.receipt),'physicalInspection':copy.deepcopy(physical)}

    def test_controlled_cpu_and_semantic_report_pass_full_audit_without_manual_replay(self):
        report,files,_=self.a.audit(OCC)
        self.assertEqual(report['nativeQAContractVersion'],3);self.assertTrue(report['candidateExecutedByController'])
        self.assertEqual(report['cpuExecutionEvidence']['kind'],'controller-cpu-replay')
        self.assertIn('controller-cpu-evidence.json',files);self.assertEqual(self.f.official.posts,0)
        self.assertNotIn('cpuEvidence',self.semantic);self.assertNotIn('candidateSHA256',self.semantic)

    def test_source_origin_semantic_rejection_is_preserved_never_accepted(self):
        self.semantic.update(decision='reject',blockingFindings=['The base already implements this hypothesis.'])
        self.d['attempts'][1].update(state='failed',error_code='NATIVE_QA3_SEMANTIC_REJECT')
        self.d['steps'][1]['state']='attention';self.d['occurrence']['state']='attention';self.make_evidence()
        source={**next(r for r in self.d['artifacts'] if r['name']=='candidate.py'),'occurrenceId':OCC,'problem':BOARD}
        report=A.strict_json(A.pinned(self.f.origin(source)[0]))
        self.assertFalse(report['qualityAccepted']);self.assertEqual(report['qa']['semanticDecision'],'reject')
        self.assertEqual(report['qa']['controllerValidation']['code'],'NATIVE_QA3_SEMANTIC_REJECT')
        with self.assertRaises(ValueError):self.a.audit(OCC)
        self.assertEqual(self.f.official.posts,0)

    def test_missing_registration_or_shadow_contract_cannot_upgrade(self):
        self.f.p.pop('nativeQA3Registrations')
        with self.assertRaisesRegex(ValueError,'NATIVE_QA3_REGISTRATION'):self.a.audit(OCC)

    def test_receipt_api_or_protected_copy_tampering_fails(self):
        self.verified['receipt']['exitCode']=1
        with self.assertRaisesRegex(ValueError,'NATIVE_QA3_PROTECTED_RECEIPT'):self.a.audit(OCC)

    def test_fresh_physical_stop_is_required_independently(self):
        for field,value in [('mainPID',99),('populated',True),('stopped',False)]:
            old=self.verified['physicalInspection']['stopProof'][field];self.verified['physicalInspection']['stopProof'][field]=value
            with self.subTest(field=field),self.assertRaisesRegex(ValueError,'NATIVE_QA3_PHYSICAL_PROOF'):self.a.audit(OCC)
            self.verified['physicalInspection']['stopProof'][field]=old
        self.verified['physicalInspection']['observedAt']='2020-01-01T00:00:00Z'
        with self.assertRaisesRegex(ValueError,'NATIVE_QA3_PHYSICAL_STALE'):self.a.audit(OCC)

    def test_fresh_physical_timestamp_does_not_change_stable_audit(self):
        first=self.a.audit(OCC)
        value=A.dt.datetime.now(A.dt.timezone.utc).isoformat()
        self.evidence['physicalInspection']['observedAt']=value;self.verified['physicalInspection']['observedAt']=value
        self.assertEqual(first,self.a.audit(OCC))

    def test_raw_controller_handoff_must_match(self):
        qa=self.d['attempts'][1]
        self.f.put(pathlib.Path(self.f.paths['workerRunsDirectory'])/qa['id']/'home/inputs'/qa['id']/'cpu-replay-observation.json',b'{}')
        with self.assertRaisesRegex(ValueError,'NATIVE_QA3_HANDOFF_CHANGED'):self.a.audit(OCC)

    def test_snapshot_fence_and_current_artifact_identity_are_required(self):
        self.evidence['snapshot']=copy.deepcopy(self.evidence['snapshot']);self.evidence['snapshot']['source']['runFence']=IDS[1]
        with self.assertRaisesRegex(ValueError,'NATIVE_QA3_ENVELOPE_BINDING'):self.a.audit(OCC)

    def test_cpu_result_changed_raw_bytes_never_accepts_report_claim(self):
        self.receipt['outputs']['result']['base64']=base64.b64encode(b'{}').decode();self.finish_evidence()
        with self.assertRaisesRegex(ValueError,'NATIVE_QA3_OUTPUTS'):self.a.audit(OCC)

    def test_unknown_semantic_fields_and_false_official_claims_rejected(self):
        for change in ({'candidateSHA256':'a'*64},{'gpuValidated':True},{'version':True},{'limitations':[]}):
            report={**self.semantic,**change}
            with self.subTest(change=change),self.assertRaises(ValueError):A.qa3_semantic(report,approval=True)

    def test_cpu_nonfinite_duplicate_negative_flags_and_empty_controls_rejected(self):
        for raw in (b'{"passed":true,"passed":true}',b'{"passed":NaN}',A.canonical({**self.cpu,'passed':False}),
                    A.canonical({**self.cpu,'negativeControls':[]})):
            receipt=copy.deepcopy(self.receipt);receipt['outputs']['result']['base64']=base64.b64encode(raw).decode()
            with self.subTest(raw=raw[:30]),self.assertRaises(ValueError):A.qa3_cpu_result(receipt,A.digest(self.f.candidate),A.digest(self.f.checker))

    def test_new_contract_keeps_one_producer_one_qa_no_implicit_retry(self):
        self.d['occurrence']['definition']['steps'][1]['maxAttempts']=2
        with self.assertRaisesRegex(ValueError,'DISPATCH_CHANGED'):self.a.audit(OCC)
        with self.assertRaisesRegex(ValueError,'NATIVE_QA_CONTRACT'):self.a.qa_contract(self.d['occurrence']['definition'])

    def rejected_report(self, raw):
        qa=self.d['attempts'][1];row=next(r for r in self.d['artifacts'] if r['name']=='qa-verification.json')
        row.update(digest=A.digest(raw),size=len(raw));self.f.put(pathlib.Path(self.f.paths['artifactDirectory'])/row['digest'],raw)
        ref={k:row[k] for k in ('id','name','digest','size')}
        self.evidence['semanticReport']=ref
        self.evidence['validation'].update(status='rejected',code='NATIVE_QA3_REPORT_INVALID',semanticDecision=None,semanticReport=ref)
        qa['context']['nativeQA3Validation']=copy.deepcopy(self.evidence['validation'])
        qa.update(state='failed',error_code='NATIVE_QA3_REPORT_INVALID')
        self.d['steps'][1]['state']='attention';self.d['occurrence']['state']='attention'

    def test_malformed_original_report_keeps_origin_and_typed_repair_diagnosis(self):
        self.rejected_report(b'{"version":3,')
        source={**next(r for r in self.d['artifacts'] if r['name']=='candidate.py'),'occurrenceId':OCC,'problem':BOARD}
        report=A.strict_json(A.pinned(self.f.origin(source)[0]))
        self.assertFalse(report['qualityAccepted']);self.assertFalse(report['qa']['reportSchemaValid'])
        self.assertIsNone(report['qa']['semanticDecision'])
        self.assertEqual(report['qa']['controllerValidation']['code'],'NATIVE_QA3_REPORT_INVALID')
        with self.assertRaisesRegex(ValueError,'QA_V3_REPORT_INVALID'):self.a.audit(OCC)
        self.assertEqual(self.f.official.posts,0)

    def test_unknown_report_field_does_not_become_approval_or_cpu_failure(self):
        self.rejected_report(A.canonical({**self.semantic,'modelInventedReceipt':True}))
        with self.assertRaisesRegex(ValueError,'QA_V3_REPORT_INVALID'):self.a.audit(OCC)
        self.assertEqual(self.evidence['receipt']['exitCode'],0)
        self.assertEqual(self.f.official.posts,0)

    def prepare_negative_cpu(self, kind='exit'):
        if kind=='exit':self.receipt['exitCode']=1
        elif kind=='oom':self.receipt.update(exitCode=None,signal=9,oomKilled=True)
        elif kind=='negative':
            raw=A.canonical({**self.cpu,'passed':False});self.receipt['outputs']['result']={
                'digest':A.digest(raw),'size':len(raw),'base64':base64.b64encode(raw).decode()}
        self.finish_evidence()
        qa=self.d['attempts'][1];sha=self.evidence['receiptSHA256']
        terminal={'kind':'cpu-preparation-terminal','modelDispatched':False,'cpu':{'requestId':qa['id'],
            'requestSHA256':self.evidence['requestSHA256'],'receiptSHA256':sha,'stopProof':copy.deepcopy(self.receipt['stopProof'])}}
        code='NATIVE_QA3_CPU_RESULT_INVALID' if kind=='negative' else 'NATIVE_QA3_CPU_EXECUTION_FAILED'
        failure={'version':1,'status':'rejected','code':code,'modelDispatched':False,
            'snapshotContractSHA256':self.evidence['snapshot']['contractSHA256'],'source':self.evidence['snapshot']['source'],
            'requestSHA256':self.evidence['requestSHA256'],'receiptSHA256':sha,'receiptCanonicalization':A.QA3_CANONICALIZATION,'terminal':terminal}
        self.evidence.update(modelDispatched=False,handoff=None,semanticReport=None,validation=None,diagnostic=failure,preparationFailure=failure)
        qa['context'].pop('manifest');qa['context'].pop('nativeQA3Validation');qa['context']['nativeQA3PreparationFailure']=copy.deepcopy(failure)
        qa['context']['cpuReplayPreparation']['phase']='preparing';qa.update(state='failed',error_code=code,receipt=terminal)
        self.d['steps'][1]['state']='attention';self.d['occurrence']['state']='attention'
        self.d['artifacts']=[r for r in self.d['artifacts'] if r['attempt_id']!=qa['id']]
        shutil.rmtree(pathlib.Path(self.f.paths['workerRunsDirectory'])/qa['id'])

    def test_failed_cpu_without_qa_model_keeps_producer_origin_and_no_harness_stop_claim(self):
        self.prepare_negative_cpu()
        source={**next(r for r in self.d['artifacts'] if r['name']=='candidate.py'),'occurrenceId':OCC,'problem':BOARD}
        report=A.strict_json(A.pinned(self.f.origin(source)[0]))
        self.assertEqual(report['qa']['state'],'cpu_preparation_failed');self.assertFalse(report['qa']['modelDispatched'])
        self.assertFalse(report['qualityAccepted']);self.assertIsNone(report['qa']['artifactId'])
        self.assertNotIn(mock.call(IDS[1]),self.f.runtime.stopped.call_args_list)
        with self.assertRaisesRegex(ValueError,'CPU_V3_EXECUTION_FAILED'):self.a.audit(OCC)
        self.assertEqual(self.f.official.posts,0)

    def test_negative_case_without_qa_has_distinct_result_diagnostic(self):
        self.prepare_negative_cpu('negative')
        with self.assertRaisesRegex(ValueError,'CPU_V3_RESULT_INVALID'):self.a.audit(OCC)
        self.assertEqual(self.f.official.posts,0)

    def test_oom_terminal_requires_real_current_cpu_stop_not_fake_qa_stop(self):
        self.prepare_negative_cpu('oom');self.verified['physicalInspection']['stopProof']['populated']=True
        with self.assertRaisesRegex(ValueError,'NATIVE_QA3_PHYSICAL_PROOF'):self.a.audit(OCC)
        self.assertNotIn(mock.call(IDS[1]),self.f.runtime.stopped.call_args_list)

    def test_incomplete_or_cancelled_receipt_never_becomes_executed_source(self):
        self.prepare_negative_cpu()
        self.evidence.update(state='diagnostic',receipt=None,receiptSHA256=None,physicalConfirmed=False,physicalInspection=None)
        with self.assertRaisesRegex(ValueError,'CPU_V3_EXECUTION_UNCONFIRMED'):self.a.audit(OCC)
        self.assertEqual(self.f.official.posts,0)

    def test_qa_copy_is_alias_only_with_exact_v3_handoff(self):
        row=self.f.artifact('candidate.py',self.f.candidate,IDS[1]);self.d['artifacts'].append(row)
        ref={**row,'occurrenceId':OCC,'problem':BOARD}
        report=A.strict_json(A.pinned(self.f.origin(ref)[0]))
        self.assertEqual(report['selectedArtifactId'],row['id']);self.assertNotEqual(report['producerArtifactId'],row['id'])
        self.assertFalse(report['qualityAccepted'])
        producer=next(r for r in self.d['artifacts'] if r['name']=='candidate.py' and r['attempt_id']==IDS[0])
        self.f.put(pathlib.Path(self.f.paths['workerRunsDirectory'])/IDS[1]/'home/inputs'/producer['id']/'candidate.py',b'changed')
        with self.assertRaises(ValueError):self.f.origin(ref)

    def test_registration_scope_cannot_self_authorize_new_profile_or_duplicate_entry(self):
        for value in ({'version':True,'registrations':[self.binding]}, {'version':1,'registrations':[self.binding,self.binding]},
                      {'version':1,'registrations':[{**self.binding,'profile':'model-command'}]}):
            policy={**self.f.p,'nativeQA3Registrations':value}
            with self.subTest(value=value),self.assertRaisesRegex(ValueError,'NATIVE_QA3_REGISTRATION_POLICY'):
                A.validate_policy(policy,NOW,'a'*64)

    def historical_v3_repair(self):
        raw=b'{"version":3,';self.rejected_report(raw)
        qa=self.d['attempts'][1];ref=self.evidence['semanticReport'];ev=self.evidence
        target={'occurrenceId':OCC,'candidateSHA256':A.digest(self.f.candidate),'qaArtifactId':ref['id'],'qaSHA256':ref['digest']}
        diagnostic={'version':1,'authority':'prior_qa_diagnostic_only_not_current_acceptance','currentAcceptance':False,
            'bindingState':'verified','problem':BOARD,'occurrenceId':OCC,'stepKey':'independent-qa','stepId':qa['step_id'],
            'attemptId':qa['id'],'runFence':qa['fence'],'cycleInputSHA256':qa['context']['manifest']['cycleInput']['digest'],
            'artifact':ref,'originalArtifact':ref,'candidateSHA256':A.digest(self.f.candidate),'sourceSHA256':A.digest(self.f.source),
            'nativeQAContractVersion':3,'report':None,'reportState':'invalid_original_json_or_schema',
            'controllerEvidence':{'version':1,'contractVersion':3,'requestSHA256':ev['requestSHA256'],
                'receiptSHA256':ev['receiptSHA256'],'snapshotSHA256':A.digest(ev['snapshot']),
                'validationSHA256':A.digest(ev['validation']),'registration':ev['registration']}}
        exp=copy.deepcopy(self.d['occurrence']['input']['experiment']);exp.update(repairTarget=target,repairQADiagnostic=diagnostic)
        exp['materials'].append({'name':'prior-qa-original.json','artifactId':ref['id'],'attemptId':qa['id'],
            'sha256':ref['digest'],'bytes':len(raw),'base64':base64.b64encode(raw).decode()})
        materials={r['name']:base64.b64decode(r['base64']) for r in exp['materials']}
        dispatch={'repairOf':OCC,'repairCandidateSHA256':A.digest(self.f.candidate)}
        return exp,materials,dispatch

    def test_raw_only_historical_report_repair_preserves_original_and_requires_gate(self):
        exp,materials,dispatch=self.historical_v3_repair()
        self.a.material_set(exp,materials,BOARD,dispatch,A.digest(self.f.candidate))
        self.assertIsNone(exp['repairQADiagnostic']['report'])
        self.assertEqual(materials['prior-qa-original.json'],b'{"version":3,')
        self.assertEqual(self.f.official.posts,0)
        with self.assertRaisesRegex(ValueError,'QA_V3_REPORT_INVALID'):self.a.audit(OCC)

    def test_raw_only_repair_never_copies_or_rebinds_previous_controller_proof(self):
        exp,materials,dispatch=self.historical_v3_repair()
        exp['repairQADiagnostic']['controllerEvidence']['receiptSHA256']='a'*64
        with self.assertRaisesRegex(ValueError,'REPAIR_QA3_CONTROLLER_BINDING'):
            self.a.material_set(exp,materials,BOARD,dispatch,A.digest(self.f.candidate))
        self.assertEqual(self.f.official.posts,0)

    def test_raw_only_repair_source_and_attachments_cannot_change(self):
        exp,materials,dispatch=self.historical_v3_repair();materials['prior-qa-original.json']=b'{}'
        with self.assertRaisesRegex(ValueError,'REPAIR_QA3_ORIGINAL_BYTES'):
            self.a.material_set(exp,materials,BOARD,dispatch,A.digest(self.f.candidate))
        self.assertEqual(self.f.official.posts,0)

    def terminal_scan(self, cache_only=False):
        if cache_only:
            with mock.patch.object(A,'scan_selection',return_value=([],{'version':1,'lastUnregisteredOccurrenceId':None})):
                return A.scan(self.f.lane,self.f.root/'qa3-status.json','inspect')
        return A.scan(self.f.lane,self.f.root/'qa3-status.json','inspect')

    def test_rejected_v3_report_survives_three_partial_sweeps_without_age_reset(self):
        self.rejected_report(b'{"version":3,')
        first=self.terminal_scan()['outcomes'][0]
        self.assertEqual(first['code'],'QA_V3_REPORT_INVALID');self.assertFalse(first['postAttempted'])
        self.assertIn('diagnosticProof',first)
        for age in [120,240]:
            self.f.lane.clock=lambda age=age:NOW+age
            with mock.patch.object(self.a,'audit',side_effect=AssertionError('no repeated acceptance audit')):
                row=self.terminal_scan(True)['outcomes'][0]
            self.assertEqual(row['diagnosticProof']['observedAt'],NOW)
            self.assertEqual(row['diagnosticProof']['verifiedAt'],NOW+age)
            self.assertEqual(row['qaSHA256'],first['qaSHA256'])
        self.assertEqual(self.f.official.posts,0)

    def test_stopped_cpu_failure_cache_never_fabricates_qa_artifact(self):
        self.prepare_negative_cpu('oom')
        row=self.terminal_scan()['outcomes'][0]
        self.assertEqual(row['code'],'CPU_V3_EXECUTION_FAILED');self.assertIn('diagnosticProof',row)
        self.assertIsNone(row['qaArtifactId']);self.assertIsNone(row['qaSHA256'])
        again=self.terminal_scan(True)['outcomes'][0]
        self.assertEqual(again['diagnosticProof']['observedAt'],row['diagnosticProof']['observedAt'])
        for code in ['QA_V3_REPORT_INVALID','CPU_REPLAY_EXECUTION_UNPROVEN']:
            with self.assertRaisesRegex(ValueError,'DIAGNOSTIC_CACHE_BINDING'):
                A.diagnostic_cache_entry({**row,'code':code},self.f.lane,NOW)
        self.assertNotIn(mock.call(IDS[1]),self.f.runtime.stopped.call_args_list)
        self.assertEqual(self.f.official.posts,0)

    def test_v3_cache_rechecks_raw_handoff_and_current_physical_stop(self):
        self.rejected_report(b'{"version":3,');self.terminal_scan()
        row=next(r for r in self.d['artifacts'] if r['name']=='cpu-checks.py')
        path=pathlib.Path(self.f.paths['workerRunsDirectory'])/IDS[1]/'home/inputs'/row['id']/row['name']
        raw=path.read_bytes();self.f.put(path,b'changed')
        result=self.terminal_scan(True)['outcomes'][0]
        self.assertEqual(result['state'],'observation_wait');self.assertNotIn('diagnosticProof',result)
        self.f.put(path,raw);self.terminal_scan()
        self.verified['physicalInspection']['observedAt']='2020-01-01T00:00:00Z'
        self.assertEqual(self.terminal_scan(True)['outcomes'][0]['state'],'observation_wait')
        self.assertEqual(self.f.official.posts,0)

    def test_v3_cache_changed_actor_fence_or_original_bytes_never_renews_proof(self):
        self.rejected_report(b'{"version":3,');self.terminal_scan()
        fence=self.d['attempts'][0]['fence'];self.d['attempts'][0]['fence']=IDS[1]
        self.assertEqual(self.terminal_scan(True)['outcomes'][0]['state'],'observation_wait')
        self.d['attempts'][0]['fence']=fence;self.terminal_scan()
        row=next(r for r in self.d['artifacts'] if r['name']=='qa-verification.json')
        self.f.put(pathlib.Path(self.f.paths['artifactDirectory'])/row['digest'],b'bad')
        self.assertEqual(self.terminal_scan(True)['outcomes'][0]['state'],'observation_wait')
        self.assertEqual(self.f.official.posts,0)

    def test_v3_cache_expiry_and_uncertain_effect_keep_owner_without_resend(self):
        self.rejected_report(b'{"version":3,');self.terminal_scan()
        self.f.lane.clock=lambda:NOW+3601
        row=self.terminal_scan(True)['outcomes'][0]
        self.assertEqual(row['state'],'observation_wait');self.assertNotIn('diagnosticProof',row)
        self.f.lane.clock=lambda:NOW;self.terminal_scan()
        path=pathlib.Path(self.f.p['stateDirectory'])/(BOARD+'-'+A.digest(self.f.candidate)+'-check.json')
        self.f.put(path,A.canonical({'state':'unknown','receipt':{},'identity':{'occurrenceId':OCC}}))
        row=self.terminal_scan(True)['outcomes'][0]
        self.assertEqual(row['state'],'uncertain_owned');self.assertNotIn('diagnosticProof',row)
        self.assertEqual(self.f.official.posts,0)

    def test_unconfirmed_cpu_receipt_cannot_enter_terminal_cache(self):
        self.prepare_negative_cpu()
        self.evidence.update(state='diagnostic',receipt=None,receiptSHA256=None,physicalConfirmed=False,physicalInspection=None)
        row=self.terminal_scan()['outcomes'][0]
        self.assertNotIn('diagnosticProof',row);self.assertIsNone(row['postAttempted'])
        self.assertEqual(self.f.official.posts,0)


if __name__ == "__main__":
    unittest.main()
