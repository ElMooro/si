#!/usr/bin/env python3
"""Preference pairs from the owned model's own bursts (2026-09-22).

Each trace burst (SageMaker job jh-burst-*) samples several completions per task; factory_trace_verify.py judged every
sample in a network-less runner and recorded the passes (factory/bursts/<burst>/verdicts/) and the failures
(factory/bursts/<burst>/attempts/) -- but only kept the TEXT of the passes. The failing text still exists in the burst's
own output (output/model.tar.gz -> traces.jsonl). This joins them back:

  a task with at least one passing and one failing sample  ->  factory/curriculum/code/pairs/<task>-<burst8>.json
  {schema factory-pref-pair.v1, task_id, burst, rejected (the failing sample's extracted code), rejected_sha256,
   rejected_sample, rejected_reason, cases, chosen_sample, chosen_solution_sha256}

The chosen side is never copied here: Gear B's collector keeps the verified, receipted row for the task and
attach_pairs() adds the failing attempt as `rejected`. Create-if-absent; idempotent; real data only.

  python3 scripts/factory_pairs.py [--bursts 40] [--dry-run]
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import re
import sys
import tarfile
from datetime import datetime, timezone
from typing import Dict, List, Tuple

PRIVATE = os.environ.get("AI_PRIVATE_BUCKET", "justhodl-ai-857687956942")
JOBS_PREFIX = "factory/bursts/jobs/"
PAIRS_PREFIX = "factory/curriculum/code/pairs/"


def sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from factory_trace_verify import extract_code  # noqa: E402  -- the exact rule the verifier applied to the passing samples


def build_pairs(traces: Dict[Tuple[str, str], str], failed: List[dict], passed: List[dict]) -> List[dict]:
    """Pure: pair each task that has a pass and a fail. traces maps (task_id, sample) -> raw completion."""
    pass_by_task: Dict[str, dict] = {}
    for v in passed:
        pass_by_task.setdefault(str(v.get("task_id")), v)
    out, seen = [], set()
    for a in failed:
        tid = str(a.get("task_id") or "")
        if not tid or tid not in pass_by_task or tid in seen:
            continue
        if isinstance(a.get("solution"), str) and a["solution"].strip():
            code = a["solution"]                                   # attempt records written after 2026-09-22 keep the extracted code
        else:
            raw = traces.get((tid, str(a.get("sample"))))
            if raw is None:
                continue
            code = extract_code(raw)
        chosen = pass_by_task[tid]
        if not code.strip() or sha(code.encode()) == str(chosen.get("solution_sha256") or ""):
            continue
        seen.add(tid)
        out.append({"schema_version": "factory-pref-pair.v1", "task_id": tid, "rejected": code, "rejected_sha256": sha(code.encode()),
                    "rejected_sample": a.get("sample"), "rejected_reason": a.get("reason"), "cases": a.get("cases"),
                    "chosen_sample": chosen.get("sample"), "chosen_solution_sha256": chosen.get("solution_sha256")})
    return out


def _list(s3, prefix: str, limit: int = 50000) -> List[str]:
    keys, token = [], None
    while True:
        kw = {"Bucket": PRIVATE, "Prefix": prefix, "MaxKeys": 1000}
        if token:
            kw["ContinuationToken"] = token
        page = s3.list_objects_v2(**kw)
        keys += [o["Key"] for o in page.get("Contents", [])]
        token = page.get("NextContinuationToken")
        if not token or len(keys) >= limit:
            return keys


def _json(s3, key: str):
    return json.loads(s3.get_object(Bucket=PRIVATE, Key=key)["Body"].read())


def burst_traces(s3, burst: str, rec: dict) -> Dict[Tuple[str, str], str]:
    out_uri = str(rec.get("out_uri") or "")
    key = out_uri.split(PRIVATE + "/", 1)[1] + burst + "/output/model.tar.gz"
    raw = s3.get_object(Bucket=PRIVATE, Key=key)["Body"].read()
    traces: Dict[Tuple[str, str], str] = {}
    with tarfile.open(fileobj=io.BytesIO(raw), mode="r:gz") as tar:
        member = next((m for m in tar.getmembers() if m.name.endswith("traces.jsonl")), None)
        if member is None:
            return traces
        for line in tar.extractfile(member).read().decode("utf-8", "replace").splitlines():
            if not line.strip():
                continue
            try:
                t = json.loads(line)
            except Exception:  # noqa: BLE001
                continue
            if t.get("task_id") is not None and isinstance(t.get("completion"), str):
                traces[(str(t["task_id"]), str(t.get("sample")))] = t["completion"]
    return traces


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bursts", type=int, default=60)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args(argv)
    import boto3
    s3 = boto3.client("s3", region_name="us-east-1")
    report = {"bursts_seen": 0, "bursts_used": 0, "traces": 0, "failed": 0, "passed": 0, "pairs": 0, "written": 0, "exists": 0, "errors": []}
    job_keys = sorted(k for k in _list(s3, JOBS_PREFIX) if k.endswith(".json"))[-args.bursts:]
    for jk in job_keys:
        burst = jk.rsplit("/", 1)[-1][:-5]
        report["bursts_seen"] += 1
        failed_keys = [k for k in _list(s3, "factory/bursts/%s/attempts/" % burst) if k.endswith(".json")]
        passed_keys = [k for k in _list(s3, "factory/bursts/%s/verdicts/" % burst) if k.endswith(".json")]
        if not failed_keys or not passed_keys:
            continue
        try:
            rec = _json(s3, jk)
            traces = burst_traces(s3, burst, rec)
        except Exception as exc:  # noqa: BLE001
            report["errors"].append("%s: %s" % (burst, str(exc)[:120]))
            continue
        failed = [_json(s3, k) for k in failed_keys]
        passed = [_json(s3, k) for k in passed_keys]
        pairs = build_pairs(traces, failed, passed)
        report["bursts_used"] += 1; report["traces"] += len(traces); report["failed"] += len(failed); report["passed"] += len(passed); report["pairs"] += len(pairs)
        for p in pairs:
            p["burst"] = burst; p["at"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            key = "%s%s-%s.json" % (PAIRS_PREFIX, re.sub(r"[^A-Za-z0-9_.-]", "_", p["task_id"])[:120], sha(burst.encode())[:8])
            if args.dry_run:
                continue
            try:
                s3.put_object(Bucket=PRIVATE, Key=key, Body=json.dumps(p, indent=1).encode(), ContentType="application/json", IfNoneMatch="*")
                report["written"] += 1
            except Exception as exc:  # noqa: BLE001
                if "PreconditionFailed" in str(exc) or "412" in str(exc):
                    report["exists"] += 1
                else:
                    raise
    print(json.dumps(report))
    return 0 if report["pairs"] or report["exists"] else 2


if __name__ == "__main__":
    sys.exit(main())
