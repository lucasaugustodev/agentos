#!/usr/bin/env python3
"""Prepare-only proof of a pinned GET observation chain; never send a request.

An operator may use the qualified anchor as the explicit bootstrap of a new
namespace. Admission still requires fresh authenticated GETs and strict overlap.
This program cannot publish policy, rewrite an index or qualify any CHECK result.
"""
import argparse
import importlib.util
import json
import os
import pathlib


def qualify(adapter, policy, policy_sha, anchor):
    A = adapter
    A.require(policy.get("adapterSHA256") == A.digest(pathlib.Path(A.__file__).read_bytes()), "QUALIFICATION_ADAPTER_POLICY_PIN")
    A.jobs_observation_policy(policy)
    A.require(policy["jobsObservation"]["bootstrap"] == anchor, "QUALIFICATION_BOOTSTRAP_SCOPE")
    lane = A.OfficialLane(policy, policy_sha, None, None, None)
    seen, refs, total = set(), [], [0]

    def read(ref):
        key = (ref["path"], ref["sha256"])
        A.require(key not in seen and len(seen) < 256, "QUALIFICATION_CHAIN_CYCLE_OR_LIMIT")
        raw = A.pinned(ref)
        A.require(len(raw) <= A.JOBS_OBSERVATION_MAX_BYTES, "QUALIFICATION_REPORT_SIZE")
        total[0] += len(raw)
        A.require(total[0] <= 16 * 1024 * 1024, "QUALIFICATION_TOTAL_SIZE")
        seen.add(key); refs.append({**ref, "bytes": len(raw)})
        return A.strict_json(raw)

    def walk(ref):
        value = read(ref)
        if value.get("kind") != "okc-jobs-observation-v1":
            # A reviewed authenticated capture is a leaf only if it reaches
            # before the UTC boundary or the actual short recent-jobs window.
            before = A.strict_json(A.pinned(value["refs"]["meBefore"]))
            day = int(value["endEpoch"] // 86400) * 86400
            authority = lane.jobs_authority(before["team"], day)
            scoped = dict(policy)
            scoped["jobsObservation"] = {**policy["jobsObservation"], "bootstrap": ref}
            verifier = A.OfficialLane(scoped, policy_sha, None, None, None, clock=lambda: value["endEpoch"])
            window, jobs, _ = verifier.jobs_bootstrap(authority)
            return {"window": window, "jobs": jobs, "observedAt": value["endEpoch"],
                    "authority": authority, "capture": True}
        authority = value["authority"]
        report = lane.jobs_report(ref, authority)
        target = lane.jobs_authority(report["meBefore"]["team"], authority["utcDayStart"])
        A.require(all(authority[k] == target[k] for k in ("teamSHA256", "transportScopeSHA256", "utcDayStart")),
                  "QUALIFICATION_AUTHORITY_SCOPE")
        if report["coverage"] == "head_overlap":
            prior = walk(report["previousReport"])
            A.require(prior["observedAt"] <= report["observedAt"], "QUALIFICATION_TIME_ORDER")
            A.require(all(prior["authority"][k] == authority[k] for k in ("teamSHA256", "transportScopeSHA256", "utcDayStart")),
                      "QUALIFICATION_CHAIN_SCOPE")
            if prior.get("capture") or A.canonical(prior["authority"]) != A.canonical(authority):
                A.require(report["previousReport"] == authority["bootstrap"], "QUALIFICATION_REANCHOR_NOT_EXPLICIT")
            jobs = A.jobs_head_overlap(prior["window"], prior["jobs"], report["window"])
            A.require(A.canonical(jobs) == A.canonical(report["jobs"]), "QUALIFICATION_UNION_CHANGED")
        return report

    head = walk(anchor)
    return {"version": 1, "passed": True, "scope": "GET-only observation-chain qualification",
            "policySHA256": policy_sha, "bootstrap": anchor, "sourceSHA256": A.digest(pathlib.Path(A.__file__).read_bytes()),
            "teamSHA256": head["authority"]["teamSHA256"], "utcDayStart": head["authority"]["utcDayStart"],
            "observedAt": head["observedAt"], "jobCount": len(head["jobs"]), "windowCount": len(head["window"]),
            "references": refs, "verifiedReportBytes": total[0], "admissionRequiresNewFreshGETs": True,
            "GETs": 0, "POSTs": 0, "productionWrites": 0, "officialAcceptance": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ("adapter", "adapter-sha256", "policy", "policy-sha256", "output"):
        parser.add_argument("--" + key, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    source = pathlib.Path(args.adapter)
    import hashlib
    if hashlib.sha256(source.read_bytes()).hexdigest() != args.adapter_sha256:
        raise ValueError("ADAPTER_PIN")
    spec = importlib.util.spec_from_file_location("qualified_readonly_adapter", source)
    A = importlib.util.module_from_spec(spec); spec.loader.exec_module(A)
    A.pinned({"path": str(source), "sha256": args.adapter_sha256})
    raw = A.pinned({"path": args.policy, "sha256": args.policy_sha256})
    policy = A.strict_json(raw)
    target = pathlib.Path(args.output)
    A.require(target.is_absolute() and target.resolve() == target
              and not target.is_relative_to(pathlib.Path(policy["stateDirectory"]))
              and not target.is_relative_to(pathlib.Path(policy["legacyLedger"])), "OUTPUT_SCOPE")
    proof = qualify(A, policy, args.policy_sha256, policy["jobsObservation"]["bootstrap"])
    raw = A.canonical(proof)
    fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o400)
    with os.fdopen(fd, "wb") as stream:
        stream.write(raw); stream.flush(); os.fsync(stream.fileno())
    print(json.dumps({"path": str(target), "sha256": A.digest(raw), "passed": True, "jobCount": proof["jobCount"]}))


if __name__ == "__main__":
    main()
