#!/usr/bin/env python3
"""Turn an owned burst's traces into verified training rows (keep-only-passes), on the runner.

  join    --traces traces.jsonl --candidates candidates.jsonl --out joined.jsonl
          traces (from generate.py) never carry tests; candidates (from factory_oss_curriculum.py fetch) carry the
          tests keyed by task_id. The join writes {task_id, solution=<extracted code>, tests, timeout_s, ...} for
          factory_code_verify.py, which runs each in the network-less container. Holdout ids are refused here too.
  write   --in verified.jsonl --burst JOB --run-id ID
          verified passes -> factory/curriculum/code/verified/<sha>.json as factory-curriculum-row.v1 with
          kind=self_trace, license=own, verified_by=owner_runner (what gear_b.curate consumes); one verdict per
          candidate -> factory/bursts/<JOB>/verdicts/<sha>.json (immutable) and a burst summary.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time

PRIVATE = os.environ.get("FACTORY_PRIVATE_BUCKET", "justhodl-ai-857687956942")
REGION = os.environ.get("AWS_REGION", "us-east-1")
VERIFIED_PREFIX = "factory/curriculum/code/verified/"
FENCE = re.compile(r"```(?:python|py)?\s*\n(.*?)```", re.S)


def sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def extract_code(text: str) -> str:
    """The first fenced python block if any, else the text up to a trailing prose paragraph."""
    m = FENCE.search(text or "")
    code = m.group(1) if m else (text or "")
    return code.replace("\r\n", "\n").rstrip() + "\n"


def cmd_join(args) -> int:
    tests = {}
    with open(args.candidates, encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            row = json.loads(line)
            if row.get("task_id") and isinstance(row.get("tests"), str):
                tests[str(row["task_id"])] = row
    holdout = set()
    if args.holdout_ids and os.path.exists(args.holdout_ids):
        holdout = set(json.load(open(args.holdout_ids)).get("ids") or json.load(open(args.holdout_ids)) or [])
    stats = {"traces": 0, "joined": 0, "no_tests": 0, "holdout_refused": 0, "empty": 0}
    with open(args.traces, encoding="utf-8") as fin, open(args.out, "w", encoding="utf-8") as fout:
        for line in fin:
            if not line.strip():
                continue
            stats["traces"] += 1
            tr = json.loads(line)
            tid = str(tr.get("task_id") or "")
            if tid in holdout:
                stats["holdout_refused"] += 1; continue
            base = tests.get(tid)
            if not base:
                stats["no_tests"] += 1; continue
            code = extract_code(str(tr.get("completion") or ""))
            if len(code.strip()) < 8:
                stats["empty"] += 1; continue
            fout.write(json.dumps({"task_id": tid, "sample": tr.get("sample"), "family": base.get("family") or tr.get("family"),
                                   "prompt": base.get("prompt"), "solution": code, "tests": base["tests"], "timeout_s": base.get("timeout_s", 8),
                                   "source_sha": base.get("source_sha"), "license": "own", "citation": base.get("citation"),
                                   "source_url": base.get("source_url"), "completion_sha256": tr.get("completion_sha256") or sha(code.encode()),
                                   "adapter_generation": tr.get("adapter_generation"), "mode": tr.get("mode")}, sort_keys=True) + "\n")
            stats["joined"] += 1
    print(json.dumps(stats)); return 0


def cmd_write(args) -> int:
    import boto3
    s3 = boto3.client("s3", region_name=REGION)
    written, exists, verdicts, seen_tasks = 0, 0, 0, set()
    with open(args.inp, encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            row = json.loads(line)
            if "_report" in row:
                report = row["_report"]; continue
            if row.get("passed") is not True:
                continue
            vid = sha(json.dumps({"task_id": row["task_id"], "completion_sha256": row.get("completion_sha256")}, sort_keys=True).encode())[:32]
            verdict = {"schema_version": "factory-burst-verdict.v1", "burst": args.burst, "task_id": row["task_id"], "sample": row.get("sample"),
                       "passed": True, "completion_sha256": row.get("completion_sha256"), "verify_elapsed_s": row.get("verify_elapsed_s"),
                       "verify_isolation": row.get("verify_isolation"), "checker": "factory-trace-verify", "verified_by": "owner_runner",
                       "run_id": args.run_id, "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
            try:
                s3.put_object(Bucket=PRIVATE, Key="factory/bursts/%s/verdicts/%s.json" % (args.burst, vid), Body=json.dumps(verdict, sort_keys=True).encode(),
                              ContentType="application/json", IfNoneMatch="*"); verdicts += 1
            except Exception as exc:  # noqa: BLE001
                if "PreconditionFailed" not in str(exc) and "412" not in str(exc):
                    raise
            if row["task_id"] in seen_tasks and not args.all_samples:
                continue          # one kept row per task per burst unless asked: diversity comes from tasks, not duplicates
            seen_tasks.add(row["task_id"])
            doc = {"schema_version": "factory-curriculum-row.v1", "task_id": row["task_id"], "kind": "self_trace", "family": row.get("family") or "code",
                   "license": "own", "source_url": "s3://%s/factory/bursts/%s/verdicts/%s.json" % (PRIVATE, args.burst, vid), "citation": row.get("citation"),
                   "source_sha": row.get("source_sha"), "prompt": row["prompt"], "solution": row["solution"],
                   "tests_sha256": sha(str(row["tests"]).encode()), "passed": True, "verified_by": "owner_runner",
                   "checker": "factory-trace-verify:network-less-container", "run_id": args.run_id, "burst": args.burst,
                   "adapter_generation": row.get("adapter_generation"), "verify_elapsed_s": row.get("verify_elapsed_s")}
            key = VERIFIED_PREFIX + sha(json.dumps({"task_id": row["task_id"], "completion_sha256": row.get("completion_sha256")}, sort_keys=True).encode())[:32] + ".json"
            try:
                s3.put_object(Bucket=PRIVATE, Key=key, Body=json.dumps(doc, sort_keys=True).encode(), ContentType="application/json", IfNoneMatch="*"); written += 1
            except Exception as exc:  # noqa: BLE001
                if "PreconditionFailed" in str(exc) or "412" in str(exc):
                    exists += 1
                else:
                    raise
    summary = {"schema_version": "factory-burst-summary.v1", "burst": args.burst, "run_id": args.run_id, "rows_written": written, "rows_existing": exists,
               "verdicts": verdicts, "report": locals().get("report"), "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    s3.put_object(Bucket=PRIVATE, Key="factory/bursts/%s/summary-%s.json" % (args.burst, args.run_id), Body=json.dumps(summary, sort_keys=True).encode(), ContentType="application/json")
    print(json.dumps(summary)); return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    j = sub.add_parser("join"); j.add_argument("--traces", required=True); j.add_argument("--candidates", required=True); j.add_argument("--out", required=True); j.add_argument("--holdout-ids")
    w = sub.add_parser("write"); w.add_argument("--in", dest="inp", required=True); w.add_argument("--burst", required=True); w.add_argument("--run-id", required=True); w.add_argument("--all-samples", action="store_true")
    args = ap.parse_args(argv)
    return cmd_join(args) if args.cmd == "join" else cmd_write(args)


if __name__ == "__main__":
    sys.exit(main())
