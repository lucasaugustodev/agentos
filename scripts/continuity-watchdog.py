#!/usr/bin/env python3
"""Periodic, evidence-driven continuation for an explicitly registered cycle.

The native cycle is the visible pause switch and LLM supervisor. This process
only observes, validates bounded plans, and dispatches native CPU experiments.
It never changes goals, retries a run, accepts a delivery, or submits a GPU job.
"""
import argparse
import base64
import datetime as dt
import fcntl
import hashlib
import json
import os
import pathlib
import re
import stat
import subprocess
import sys
import time
import urllib.error
import urllib.request

PROBLEMS = {"muse_decode_layer", "muse_prefill_layer", "muse_attention_decode"}
LIVE = {"queued", "running", "waiting", "recovering", "cancelling", "reconciling"}
TERMINAL = {"completed", "failed", "cancelled", "attention"}
UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")


def require(ok, code):
    if not ok:
        raise ValueError(code)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(value if isinstance(value, bytes) else canonical(value)).hexdigest()


def now_iso():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def atomic(path, value, exclusive=False):
    path = pathlib.Path(path)
    raw = canonical(value)
    if exclusive:
        with path.open("xb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
    else:
        temp = path.with_suffix(path.suffix + ".new")
        with temp.open("wb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, path)
    fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def read_json(path):
    return json.loads(pathlib.Path(path).read_bytes())


def confirmed_boundary(detail):
    """A terminal label alone is not proof the native executor stopped."""
    if detail["occurrence"]["state"] not in TERMINAL:
        return False
    if any(s["state"] == "attention" and s["definition"]["kind"] == "review" for s in detail["steps"]):
        return False  # Explicit review gates remain owned by their original controller.
    for attempt in detail["attempts"]:
        if attempt["state"] not in {"completed", "failed"} or not attempt.get("finished_at"):
            return False
        require(UUID.fullmatch(attempt.get("fence", "")), "ATTEMPT_FENCE_MISSING")
        proof_events = [e for e in detail.get("events", []) if e["kind"] == "HARNESS_STOPPED"
            and e.get("attempt_id") == attempt["id"] and e.get("payload", {}).get("runFence") == attempt["fence"]]
        event_stopped = any(e["payload"].get("confirmed") is True and
            e["payload"].get("unit") == "agentos-run-" + attempt["id"] and
            e["payload"].get("proof", {}).get("stopped") is True and
            e["payload"].get("proof", {}).get("mainPid") == 0 and
            e["payload"].get("proof", {}).get("populated") is False and
            e["payload"].get("proof", {}).get("activeState") == "inactive" for e in proof_events)
        if (attempt.get("receipt") or {}).get("stopped") is not True and not event_stopped:
            return False
    return bool(detail["attempts"])


def criterion(name):
    return {"id": name.replace(".", "-"), "label": name, "type": "artifact", "name": name}


COMMON = """You are executing one bounded CPU-only OpenKernel experiment, not controlling infrastructure.
Preserve canonical TASK/AGENTS rules, all numerical boundaries, all inputs/keys/experts,
and upstream attribution. No reference/evaluator imports, evaluator detection,
cached outputs, network, credentials, GPU submissions, production or source workspace writes.
Read supplied immutable materials and use data/ and evidence/ only. Candidate artifacts
are proposals, never acceptance, GPU correctness, performance or leaderboard proof.
Do not make API calls or start other agents. The controller owns subsequent dispatch.
Use the actual input source and record its SHA256; fail clearly when an input is missing.
"""

SUPERVISOR = """You are the periodic OpenKernel supervisor. The controller has observed that
the registered work has reached a confirmed boundary. Inspect cycle-input.json:
it contains actual outcomes, official measurements, immutable artifact references,
pending official candidates and the schema below. Choose the next small experiment
from evidence. Prefer the dominant measured bottleneck; do not repeat a failed
hypothesis without an explicit discriminating change. Decode, Prefill and Attention
only; MoE/BF16 standalone boards belong to another operator.

Publish evidence/continuation-plan.json and evidence/supervisor-summary.txt.
The JSON must have exactly these fields:
{ "version":1, "evidenceFingerprint":"<input evidenceFingerprint>",
  "decision":"experiments" or "await_external" or "observe_active", "reason":"20..1200 characters",
  "experiments":[{ "id":"short-lowercase-id", "problem":"muse_decode_layer" or
    "muse_prefill_layer" or "muse_attention_decode", "sourceArtifactId":"UUID from catalog",
    "hypothesis":"20..1200 characters", "instructions":"50..12000 characters" }] }
Use 1..availableCandidateSlots experiments when decision=experiments, otherwise [].
Every source must be a candidate.py artifact from that same problem in the input catalog.
Instructions must describe implementation and meaningful discriminating CPU checks,
including what result would falsify the hypothesis, without pretending to run a GPU.
If an official measurement is required, await_external and include the full SHA256
of a candidate in pendingOfficialCandidateSHA256 in your reason. No other external
dependency is presently supported: do not invent permission or approval requirements.
When diagnosticOnly=true, use observe_active, experiments=[] and quote the exact
active occurrence UUID in the reason. Diagnose only: a long tool/model call may be
legitimate; heartbeat proves presence, not progress. Never stop/retry/replace that actor.
Do not generate work just to fill slots. The deterministic controller creates producer
and independent QA tasks after validating this plan. No arbitrary shell/URL/action
or account/model/workspace field is accepted in the plan.
Your summary must state trigger, observed idle duration, next action or explicit blocker.
"""


def native_step(config, key, title, instructions, names, depends=None):
    return {"key": key, "title": title, "kind": "agent", "instructions": instructions,
            "dependsOn": depends or [], "accountId": config["accountId"], "model": config["model"],
            "workspace": config["workspace"], "readOnly": True, "maxAttempts": 1,
            "parameters": {}, "seconds": 60, "criteria": [criterion(n) for n in names]}


def definition(config, name, steps):
    return {"name": name, "description": "Periodic evidence-driven CPU work; official GPU actions remain separately guarded.",
            "project": "openkernel", "trigger": {"kind": "manual", "seconds": 120, "includeExisting": False},
            "concurrency": 1, "allowedActions": [], "allowedAccounts": [config["accountId"]], "steps": steps}


def supervisor_definition(config):
    return definition(config, "Supervisão contínua — OpenKernel", [native_step(config, "supervise",
        "Conferir progresso e decidir a próxima experiência", COMMON + SUPERVISOR,
        ["continuation-plan.json", "supervisor-summary.txt"])])


def validate_config(config, bound=True):
    require(config.get("version") == 1 and config.get("enabled") is True, "WATCH_NOT_ENABLED")
    require(re.fullmatch(r"[a-z0-9-]{8,80}", config.get("watchId", "")), "WATCH_ID")
    require(re.fullmatch(r"openkernel/[a-z0-9_-]+", config.get("workspace", "")), "WORKSPACE_SCOPE")
    require(config.get("model") == "deepseek-v4-pro-0813", "MODEL_SCOPE")
    require(re.fullmatch(r"agentos-api-[a-f0-9-]{36}", config.get("accountId", "")), "ACCOUNT_SCOPE")
    require(config.get("maxPendingCandidates") in [1, 2, 3], "CANDIDATE_CAPACITY")
    require(120 <= config.get("intervalSeconds", 0) <= 600, "WATCH_INTERVAL")
    require(type(config.get("seedOccurrences")) is list and 1 <= len(config["seedOccurrences"]) <= 20, "SEED_SCOPE")
    for row in config["seedOccurrences"]:
        require(UUID.fullmatch(row.get("id", "")) and row.get("problem") in PROBLEMS, "SEED_SCOPE")
    for row in config.get("officialJobs", []):
        require(re.fullmatch(r"[a-f0-9]{16,64}", row.get("id", "")) and row.get("problem") in PROBLEMS
                and re.fullmatch(r"[a-f0-9]{64}", row.get("sourceSHA256", "")), "JOB_SCOPE")
    if bound:
        require(UUID.fullmatch(config.get("controlCycleId", "")), "CONTROL_CYCLE_ID")
        require(config.get("controlDefinitionSHA256") == digest(supervisor_definition(config)), "CONTROL_DEFINITION")
    return config


def validate_plan(plan, observation, config):
    require(type(plan) is dict and set(plan) == {"version", "evidenceFingerprint", "decision", "reason", "experiments"}, "PLAN_FIELDS")
    require(plan["version"] == 1 and plan["evidenceFingerprint"] == observation["fingerprint"], "PLAN_EVIDENCE")
    require(plan["decision"] in ["experiments", "await_external", "observe_active"] and type(plan["reason"]) is str
            and 20 <= len(plan["reason"]) <= 1200, "PLAN_DECISION")
    experiments = plan["experiments"]
    require(type(experiments) is list, "PLAN_EXPERIMENTS")
    capacity = min(config["maxPendingCandidates"], observation["availableCandidateSlots"])
    require((plan["decision"] in ["await_external", "observe_active"] and not experiments) or
            (plan["decision"] == "experiments" and 1 <= len(experiments) <= capacity), "PLAN_CAPACITY")
    if observation.get("diagnosticOnly"):
        require(plan["decision"] == "observe_active" and any(r["occurrenceId"] in plan["reason"]
                for r in observation.get("active", [])), "LIVE_OWNER_DIAGNOSIS_ONLY")
    else:
        require(plan["decision"] != "observe_active", "ACTIVE_DEPENDENCY_UNPROVEN")
    if plan["decision"] == "await_external":
        require(any(sha in plan["reason"] for sha in observation.get("pendingOfficialCandidateSHA256", [])),
                "WAIT_DEPENDENCY_UNPROVEN")
    seen = set()
    catalog = {r["id"]: r for r in observation["catalog"]}
    for exp in experiments:
        require(type(exp) is dict and set(exp) == {"id", "problem", "sourceArtifactId", "hypothesis", "instructions"}, "EXPERIMENT_FIELDS")
        require(re.fullmatch(r"[a-z][a-z0-9-]{2,40}", exp.get("id", "")) and exp["id"] not in seen, "EXPERIMENT_ID")
        seen.add(exp["id"])
        require(exp["problem"] in PROBLEMS, "EXPERIMENT_SCOPE")
        source = catalog.get(exp["sourceArtifactId"])
        require(source and source["name"] == "candidate.py" and source["problem"] == exp["problem"], "SOURCE_SCOPE")
        require(type(exp["hypothesis"]) is str and 20 <= len(exp["hypothesis"]) <= 1200, "HYPOTHESIS")
        require(type(exp["instructions"]) is str and 50 <= len(exp["instructions"]) <= 12000, "INSTRUCTIONS")
    return plan


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise ValueError("API_REDIRECT_FORBIDDEN")


class Client:
    def __init__(self):
        self.base = "http://127.0.0.1:4280"
        self.token = os.environ["AGENTOS_API_TOKEN"]
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())

    def request(self, method, route, body=None, binary=False):
        require(route.startswith("/api/") and ".." not in route, "API_SCOPE")
        request = urllib.request.Request(self.base + route, data=None if body is None else canonical(body),
            headers={"Authorization": "Bearer " + self.token, "Content-Type": "application/json"}, method=method)
        with self.opener.open(request, timeout=25) as response:
            raw = response.read(32 * 1024 * 1024 + 1)
        require(len(raw) <= 32 * 1024 * 1024, "API_RESPONSE_LIMIT")
        return raw if binary else json.loads(raw)

    def official(self, job):
        result = subprocess.run(["/opt/openkernelchallenge/bin/okc", "status", job["id"], "--json"],
            capture_output=True, timeout=60, check=False)
        require(result.returncode == 0 and len(result.stdout) <= 4 * 1024 * 1024, "OFFICIAL_OBSERVATION_UNAVAILABLE")
        value = json.loads(result.stdout)
        require(value.get("id") == job["id"] and value.get("board") == job["problem"], "OFFICIAL_IDENTITY")
        # Full reply remains a protected evidence file. Workers receive bounded measurements, never auth.
        return value


class Watchdog:
    def __init__(self, config, directory, client, clock=time.time):
        self.config = validate_config(config)
        self.directory = pathlib.Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.client, self.clock = client, clock
        self.path = self.directory / "state.json"
        self.state = read_json(self.path) if self.path.exists() else {"version": 1, "watchId": config["watchId"],
            "createdAt": clock(), "supervisions": [], "experiments": [], "outcomes": {}, "official": {}}
        require(self.state["watchId"] == config["watchId"], "STATE_SCOPE")

    def save(self):
        atomic(self.path, self.state)

    def status(self, state, **facts):
        value = {"observedAt": now_iso(), "state": state, "watchId": self.config["watchId"],
                 "controlCycleId": self.config["controlCycleId"], "officialPostAuthority": False, **facts}
        atomic(self.directory / "latest.json", value)
        return value

    def control(self):
        value = self.client.request("GET", "/api/cycles/" + self.config["controlCycleId"])
        require(value["cycle"]["id"] == self.config["controlCycleId"] and
                digest(value["cycle"]["definition"]) == self.config["controlDefinitionSHA256"], "CONTROL_CHANGED")
        return value

    def find_post(self, kind, cycle_id, key):
        if kind == "create":
            rows = self.client.request("GET", "/api/cycles")["cycles"]
            matches = [row for row in rows if row.get("request_key") == key]
        else:
            rows = self.client.request("GET", "/api/cycles/" + cycle_id)["occurrences"]
            matches = []
            for row in rows:
                occurrence = self.client.request("GET", "/api/cycle-runs/" + row["id"])["occurrence"]
                if occurrence.get("source_key") == "manual:" + key:
                    matches.append(occurrence)
        require(len(matches) <= 1, "DUPLICATE_CANONICAL_IDENTITY")
        return matches[0] if matches else None

    def post_once(self, kind, key, body, cycle_id=None):
        marker = self.directory / ("intent-" + digest(key) + ".json")
        if marker.exists():
            prior = read_json(marker)
            require(prior["requestDigest"] == digest(body), "POST_IDENTITY_CHANGED")
            result = prior.get("result") or self.find_post(kind, cycle_id, key)
            require(result is not None, "POST_UNCERTAIN_RECONCILE_ONLY")
        else:
            require(self.control()["cycle"]["state"] == "active", "CONTROL_NOT_ACTIVE")
            prior = {"kind": kind, "requestKey": key, "requestDigest": digest(body), "at": now_iso(),
                     "state": "intent", "cycleId": cycle_id, "automaticRetry": False}
            atomic(marker, prior, exclusive=True)
            route = "/api/cycles" if kind == "create" else "/api/cycles/" + cycle_id + "/run"
            # A crash/timeout leaves the durable intent. Next tick only reconciles GET.
            result = self.client.request("POST", route, body)
        if kind == "create":
            require(result.get("request_key") == key and result.get("definition") == body["definition"], "CREATE_RECEIPT")
        else:
            require(result.get("cycle_id") == cycle_id and result.get("source_key") == "manual:" + key
                    and result.get("input") == body["input"], "RUN_RECEIPT")
        require(UUID.fullmatch(result.get("id", "")), "RECEIPT_ID")
        atomic(marker, {**prior, "state": "recorded", "result": result})
        return result

    def verified_artifact(self, row, limit=262144):
        require(UUID.fullmatch(row.get("id", "")) and re.fullmatch(r"[a-f0-9]{64}", row.get("digest", "")), "ARTIFACT_IDENTITY")
        require(type(row.get("size")) is int and 0 <= row["size"] <= limit, "ARTIFACT_SIZE")
        raw = self.client.request("GET", "/api/cycle-artifacts/" + row["id"], binary=True)
        require(len(raw) == row["size"] and digest(raw) == row["digest"], "ARTIFACT_CORRUPT")
        return raw

    def observe(self, exclude_supervisor=None):
        records = [*self.config["seedOccurrences"], *[r for r in self.state["experiments"]
                   if r["supervisorOccurrenceId"] != exclude_supervisor]]
        outcomes, catalog, active = [], [], []
        tested = {job["sourceSHA256"] for job in self.config.get("officialJobs", [])}
        pending = set()
        for record in records:
            oid = record.get("occurrenceId", record.get("id"))
            detail = self.client.request("GET", "/api/cycle-runs/" + oid)
            occurrence = detail["occurrence"]
            require(occurrence.get("id") == oid, "OCCURRENCE_IDENTITY")
            require(all(a["state"] in {"running", "completed", "failed", "cancelling", "reconciling"}
                        for a in detail["attempts"]), "ATTEMPT_STATE_UNKNOWN")
            live_attempts = [a for a in detail["attempts"] if a["state"] in LIVE]
            progress = [e for e in detail.get("events", []) if e["kind"] in {"STEP_COMPLETED", "DELIVERY_MANIFEST_COLLECTED"}]
            activities = [e for e in detail.get("events", []) if e["kind"] == "MODEL_RESPONSE" or
                e["kind"] == "HARNESS_TOOL_EVENT" and e.get("payload", {}).get("type") == "tool/result"]
            activity = max(activities, key=lambda e: str(e["created_at"])) if activities else None
            latest = max(progress, key=lambda e: str(e["created_at"])) if progress else None
            progress_at = latest["created_at"] if latest else occurrence.get("created_at")
            material_artifacts = [a for a in detail["artifacts"] if a.get("created_at") and
                a["name"] in {"candidate.py", "implementation.patch", "cpu-result.json", "qa-verification.json"}]
            latest_artifact = max(material_artifacts, key=lambda a: a["created_at"]) if material_artifacts else None
            if latest_artifact and (not progress_at or latest_artifact["created_at"] > progress_at):
                progress_at = latest_artifact["created_at"]
                latest = {"id": "artifact:" + latest_artifact["id"], "kind": "ARTIFACT_CREATED", "created_at": progress_at}
            progress_epoch = dt.datetime.fromisoformat(progress_at.replace("Z", "+00:00")).timestamp() if progress_at else self.clock()
            progress_ref = {"eventId": str(latest["id"]) if latest else None, "at": progress_at,
                            "kind": latest["kind"] if latest else "occurrence_created"}
            if occurrence["state"] in LIVE or live_attempts:
                active.append({"occurrenceId": oid, "state": occurrence["state"],
                               "attemptIds": [a["id"] for a in live_attempts], "progress": progress_ref,
                               "activity": {"kind": activity["kind"], "at": activity["created_at"]} if activity else None,
                               "stale": self.clock() - progress_epoch >= 600})
            elif not confirmed_boundary(detail):
                active.append({"occurrenceId": oid, "state": "boundary_unconfirmed_or_review_owned", "attemptIds": [],
                               "progress": progress_ref, "stale": self.clock() - progress_epoch >= 600})
            artifacts, qa_reviews = [], []
            latest_attempts = {a["id"] for a in detail["attempts"] if any(
                s["id"] == a["step_id"] and s["attempt"] == a["number"] for s in detail["steps"])}
            for row in detail["artifacts"]:
                if row["attempt_id"] not in latest_attempts:
                    continue
                if row["name"] in ["candidate.py", "cpu-result.json", "verification.json", "qa-verification.json", "review.txt"]:
                    item = {k: row[k] for k in ["id", "name", "digest", "size"]}
                    item.update({"problem": record["problem"], "occurrenceId": oid})
                    artifacts.append(item)
                    if row["name"] == "candidate.py":
                        catalog.append(item)
                    if row["name"] == "qa-verification.json":
                        review = json.loads(self.verified_artifact(item, 32768))
                        qa_reviews.append({k: review.get(k) for k in ["passed", "verificationPassed", "candidateSHA256",
                            "sourceSHA256", "baseSourceSHA256", "cpuReplayPassed", "scopePreserved", "approvedForOfficialReview",
                            "approvedForRootCheckReview", "gpuValidated", "performanceGainProven", "findings", "limitations"]})
                        item["review"] = qa_reviews[-1]
            for candidate in [a for a in artifacts if a["name"] == "candidate.py"]:
                eligible = any(q.get("passed") is True and q.get("candidateSHA256") == candidate["digest"] and
                    (q.get("approvedForOfficialReview") is True or q.get("approvedForRootCheckReview") is True)
                    and q.get("gpuValidated") is False for q in qa_reviews)
                if occurrence["state"] == "completed" and eligible and candidate["digest"] not in tested:
                    pending.add(candidate["digest"])
            outcomes.append({"occurrenceId": oid, "problem": record["problem"], "state": occurrence["state"],
                "artifacts": artifacts, "finishedAt": occurrence.get("finished_at"),
                "progress": progress_ref,
                "failures": [{"attemptId": a["id"], "code": a.get("error_code")} for a in detail["attempts"] if a["state"] == "failed"]})
        jobs, pending_jobs = [], []
        for job in self.config.get("officialJobs", []):
            cached = self.state["official"].get(job["id"])
            raw = cached or self.client.official(job)
            require(raw.get("id") == job["id"] and raw.get("board") == job["problem"], "OFFICIAL_IDENTITY")
            if raw.get("status") not in ["done", "failed"]:
                pending_jobs.append(job["id"])
            else:
                self.state["official"][job["id"]] = raw
                atomic(self.directory / ("official-" + job["id"] + ".json"), raw)
            result = raw.get("result") or {}
            jobs.append({**{k: job[k] for k in ["id", "problem", "sourceSHA256"]}, "status": raw.get("status"),
                         "result": {k: result.get(k) for k in ["passed", "speedup", "speedup_ci", "score_us",
                             "baseline_us", "checks", "flags", "error", "status"]},
                         "logTail": str(result.get("log_tail", ""))[-12000:], "receiptSHA256": digest(raw)})
            if job.get("analysisEvidence"):
                ref = job["analysisEvidence"]; data = pathlib.Path(ref["path"]).read_bytes()
                require(len(data) <= 16384 and digest(data) == ref["sha256"], "ANALYSIS_EVIDENCE_CHANGED")
                jobs[-1]["analysisEvidence"] = json.loads(data)
        slots = max(0, self.config["maxPendingCandidates"] - len(pending))
        material = {"outcomes": outcomes, "officialJobs": jobs, "availableCandidateSlots": slots,
                    "pendingOfficialCandidateSHA256": sorted(pending)}
        fingerprint = digest(material)
        self.save()
        return {**material, "catalog": catalog, "fingerprint": fingerprint, "active": active,
                "pendingOfficialJobs": pending_jobs, "diagnosticOnly": bool(active),
                "idleSeconds": max(0, int(self.clock() - self.state.get("lastDispatchAt", self.state["createdAt"])))}

    def submit_supervisor(self, observation):
        failures = [r for r in self.state["supervisions"] if r["fingerprint"] == observation["fingerprint"] and r["state"] == "failed"]
        generation = len(failures) + 1
        key = self.config["watchId"] + "-review-" + observation["fingerprint"][:32] + "-" + str(generation)
        body = {"requestKey": key, "input": {"version": 1, "watchId": self.config["watchId"],
            "evidenceFingerprint": observation["fingerprint"], "trigger": "stale_progress_diagnostic" if observation["diagnosticOnly"] else "registered_work_reached_boundary",
            "observedIdleSeconds": observation["idleSeconds"], "priorReviewErrors": [r.get("errorCode", "NATIVE_SUPERVISOR_FAILED") for r in failures], **{k: observation[k] for k in
            ["outcomes", "catalog", "officialJobs", "availableCandidateSlots", "pendingOfficialCandidateSHA256", "active", "diagnosticOnly"]}}}
        body["input"]["priorDispatches"] = self.state["experiments"][-10:]
        requests = self.state.setdefault("reviewRequests", {})
        if key not in requests:
            requests[key] = body
            self.save()
        body = requests[key]
        row = self.post_once("run", key, body, self.config["controlCycleId"])
        record = {"occurrenceId": row["id"], "fingerprint": observation["fingerprint"], "input": body["input"],
                  "createdAt": self.clock(), "state": "dispatched"}
        self.state["supervisions"].append(record)
        self.state["lastDispatchAt"] = self.clock()
        self.save()
        return self.status("supervisor_dispatched", occurrenceId=row["id"], trigger=body["input"]["trigger"], idleSeconds=observation["idleSeconds"])

    def material_packet(self, experiment, observation):
        source = next(row for row in observation["catalog"] if row["id"] == experiment["sourceArtifactId"])
        raw = self.verified_artifact(source)
        materials = [{"name": "base-candidate.py", "sha256": digest(raw), "bytes": len(raw),
                      "base64": base64.b64encode(raw).decode()}]
        for ref in self.config.get("problemMaterials", {}).get(experiment["problem"], []):
            path = pathlib.Path(ref["path"])
            require(path.is_absolute() and path.resolve() == path and path.is_file(), "MATERIAL_PATH")
            data = path.read_bytes()
            require(len(data) <= 262144 and digest(data) == ref["sha256"], "MATERIAL_CHANGED")
            materials.append({"name": ref["name"], "sha256": digest(data), "bytes": len(data),
                              "base64": base64.b64encode(data).decode()})
        require({"canonical-TASK.md", "canonical-AGENTS.md"}.issubset({m["name"] for m in materials}), "CANONICAL_MATERIALS_MISSING")
        return {"problem": experiment["problem"], "sourceArtifact": source, "materials": materials,
                "hypothesis": experiment["hypothesis"], "instructions": experiment["instructions"]}

    def dispatch_experiment(self, experiment, observation, supervisor):
        producer = COMMON + """Read input.experiment. Implement exactly its bounded hypothesis against
base-candidate.py. Publish candidate.py, implementation.patch, cpu-checks.py,
cpu-result.json and verification.json. verification must state sourceSHA256,
candidateSHA256, passed, scopePreserved, gpuValidated=false, officialActionsPerformed=false.
CPU tests must exercise the distinguishing arithmetic/indexing/dispatch behavior,
include a negative control, and report limitations. Do not import CUDA candidates on CPU.
"""
        qa = COMMON + """Independently read the actual predecessor candidate, patch and checks.
Verify source binding, constrained scope, canonical precision/rules and replay meaningful CPU tests.
Publish qa-verification.json and review.txt. JSON must contain passed, candidateSHA256,
sourceSHA256, cpuReplayPassed, scopePreserved, approvedForOfficialReview, gpuValidated=false,
officialActionsPerformed=false, findings (array). Fail candidly if wrong or missing evidence;
never claim GPU validity or speed from CPU/static checks. Your review cannot submit anything.
"""
        steps = [native_step(self.config, "produce", "Implementar experiência: " + experiment["id"], producer,
                 ["candidate.py", "implementation.patch", "cpu-checks.py", "cpu-result.json", "verification.json"]),
                 native_step(self.config, "independent-qa", "Revisar e reproduzir a experiência", qa,
                 ["qa-verification.json", "review.txt"], ["produce"])]
        for step, file, fields in [(steps[0], "verification.json", {"passed": True, "scopePreserved": True,
                "gpuValidated": False, "officialActionsPerformed": False}),
                (steps[1], "qa-verification.json", {"passed": True, "cpuReplayPassed": True, "scopePreserved": True,
                "approvedForOfficialReview": True, "gpuValidated": False, "officialActionsPerformed": False})]:
            step["criteria"].extend({"id": field, "label": field, "type": "json_field", "name": file,
                                     "field": field, "equals": value} for field, value in fields.items())
        d = definition(self.config, "OpenKernel — " + experiment["id"], steps)
        key = self.config["watchId"] + "-experiment-" + digest([supervisor["occurrenceId"], experiment])[:32]
        packet = self.material_packet(experiment, observation)
        created = self.post_once("create", key, {"requestKey": key, "definition": d})
        body = {"requestKey": key + "-run", "input": {"version": 1, "watchId": self.config["watchId"],
            "supervisorOccurrenceId": supervisor["occurrenceId"], "evidenceFingerprint": observation["fingerprint"],
            "officialActionsAllowed": False, "productionActionsAllowed": False, "sourceWriteAllowed": False,
            "experiment": packet}}
        row = self.post_once("run", key + "-run", body, created["id"])
        record = {"occurrenceId": row["id"], "cycleId": created["id"], "problem": experiment["problem"],
                  "supervisorOccurrenceId": supervisor["occurrenceId"], "experimentId": experiment["id"]}
        if not any(r["occurrenceId"] == row["id"] for r in self.state["experiments"]):
            self.state["experiments"].append(record)
        self.state["lastDispatchAt"] = self.clock()
        self.save()
        return record

    def consume_plan(self, pending, observation):
        if observation["fingerprint"] != pending["fingerprint"]:
            pending.update({"state": "superseded", "errorCode": "PLAN_OBSERVATION_CHANGED"}); self.save()
            return self.status("stale_plan_superseded", occurrenceId=pending["occurrenceId"])
        if (observation["active"] and pending["approvedPlan"]["decision"] != "observe_active") or observation["pendingOfficialJobs"]:
            return self.status("waiting_active_evidence", active=observation["active"], officialJobs=observation["pendingOfficialJobs"])
        plan = pending["approvedPlan"]
        dispatched = [self.dispatch_experiment(exp, observation, pending) for exp in plan["experiments"]]
        pending.update({"state": "consumed", "decision": plan["decision"], "reason": plan["reason"]})
        self.state["outcomes"][pending["fingerprint"]] = {"supervisorOccurrenceId": pending["occurrenceId"],
            "decision": plan["decision"], "reason": plan["reason"], "dispatched": dispatched}
        self.save()
        return self.status("experiments_dispatched" if dispatched else "active_owner_preserved" if plan["decision"] == "observe_active" else "awaiting_external", dispatched=dispatched,
                           supervisorOccurrenceId=pending["occurrenceId"], reason=plan["reason"])

    def tick(self):
        if self.control()["cycle"]["state"] != "active":
            return self.status("paused_by_control_cycle")
        pending = next((r for r in reversed(self.state["supervisions"]) if r["state"] in ["dispatched", "dispatching"]), None)
        observation = self.observe(exclude_supervisor=pending["occurrenceId"] if pending else None)
        if pending and pending["state"] == "dispatching":
            return self.consume_plan(pending, observation)
        if pending:
            detail = self.client.request("GET", "/api/cycle-runs/" + pending["occurrenceId"])
            if not confirmed_boundary(detail):
                return self.status("supervisor_running", occurrenceId=pending["occurrenceId"],
                    attempts=[{"id": a["id"], "state": a["state"]} for a in detail["attempts"]])
            if detail["occurrence"]["state"] != "completed":
                pending.update({"state": "failed", "errorCode": "NATIVE_SUPERVISOR_FAILED"}); self.save()
                return self.status("supervisor_failed", occurrenceId=pending["occurrenceId"], reason="Native supervisor did not complete; preserved for diagnosis.")
            try:
                rows = [a for a in detail["artifacts"] if a["name"] == "continuation-plan.json"]
                require(len(rows) == 1, "PLAN_ARTIFACT_MISSING_OR_AMBIGUOUS")
                plan = json.loads(self.verified_artifact(rows[0], 65536))
                original = {"fingerprint": pending["fingerprint"], **pending["input"]}
                validate_plan(plan, original, self.config)
            except (ValueError, KeyError, TypeError) as error:
                code = str(error) if re.fullmatch(r"[A-Z0-9_]+", str(error)) else "PLAN_FORMAT_INVALID"
                pending.update({"state": "failed", "errorCode": code}); self.save()
                return self.status("supervisor_plan_rejected", occurrenceId=pending["occurrenceId"], code=code)
            # Persist the complete approved action set BEFORE any create/run POST.
            # Own successful dispatches do not invalidate the remaining cursor.
            pending.update({"state": "dispatching", "approvedPlan": plan, "planArtifactId": rows[0]["id"]}); self.save()
            return self.consume_plan(pending, observation)
        if (observation["active"] and not any(r["stale"] for r in observation["active"])) or observation["pendingOfficialJobs"]:
            return self.status("work_active", active=observation["active"], officialJobs=observation["pendingOfficialJobs"])
        previous = self.state["outcomes"].get(observation["fingerprint"])
        if previous:
            return self.status("awaiting_changed_evidence", **previous)
        failed = [s for s in self.state["supervisions"] if s["fingerprint"] == observation["fingerprint"] and s["state"] == "failed"]
        if len(failed) >= 2:
            return self.status("supervisor_failure_needs_repair", occurrenceId=failed[-1]["occurrenceId"])
        return self.submit_supervisor(observation)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["template", "tick"])
    parser.add_argument("config", type=pathlib.Path)
    parser.add_argument("--state-dir", type=pathlib.Path)
    args = parser.parse_args()
    for item in [args.config, pathlib.Path(__file__).absolute()]:
        require(item.is_absolute() and item.resolve() == item, "PROTECTED_PATH")
        for parent in [item, *item.parents]:
            metadata = parent.lstat()
            require(metadata.st_uid == 0 and not metadata.st_mode & 0o022 and not stat.S_ISLNK(metadata.st_mode), "PROTECTED_OWNER_MODE")
    config = read_json(args.config)
    if args.command == "template":
        validate_config(config, bound=False)
        print(json.dumps({"requestKey": config["watchId"] + "-control", "definition": supervisor_definition(config)}, ensure_ascii=False))
        return
    require(args.state_dir is not None, "STATE_DIRECTORY_REQUIRED")
    os.umask(0o077)
    args.state_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    with (args.state_dir / "watch.lock").open("a+") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print(json.dumps({"state": "observer_already_running"})); return
        watcher = Watchdog(config, args.state_dir, Client())
        try:
            print(json.dumps(watcher.tick(), ensure_ascii=False))
        except Exception as error:
            code = str(error) if type(error) is ValueError and re.fullmatch(r"[A-Z0-9_]+", str(error)) else type(error).__name__
            print(json.dumps(watcher.status("observation_or_dispatch_blocked", code=code, automaticPostRetry=False)))
            raise SystemExit(1)


if __name__ == "__main__":
    main()
