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
    if args.task_cards and os.path.exists(args.task_cards):
        for line in open(args.task_cards, encoding="utf-8"):
            if not line.strip():
                continue
            card = json.loads(line)
            if card.get("id") and isinstance(card.get("tests"), str) and card["tests"].strip():
                tests[str(card["id"])] = {"task_id": card["id"], "prompt": card.get("text"), "tests": card["tests"], "timeout_s": 12, "family": "owner-task",
                                          "source_sha": sha(json.dumps(card, sort_keys=True).encode()), "source_url": "factory/queue/tasks/%s.json" % card["id"], "citation": "owner task card"}
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
    """Every attempt becomes a durable record; only supervisor-judged passes with training permission become rows.

    One result contract end to end (audit A05/A06/A07/A08): the verifier's checker id, judge level and case count travel
    unchanged into the verdict and the curriculum row; failures (from the sidecar) are stored as attempt records;
    owner-task results replay idempotently and conflict loudly; an owner task trains only with explicit consent.
    """
    import boto3
    s3 = boto3.client("s3", region_name=REGION)
    counts = {"rows_written": 0, "rows_existing": 0, "verdicts": 0, "failures_recorded": 0, "owner_results": 0, "owner_replays": 0,
              "skipped_not_trainable": 0, "skipped_not_supervisor_judge": 0, "skipped_duplicate_task": 0}
    report = None
    seen_tasks = set()

    def put_absent(key, doc):
        """create-if-absent -> 'written' | 'exists'"""
        try:
            s3.put_object(Bucket=PRIVATE, Key=key, Body=json.dumps(doc, sort_keys=True).encode(), ContentType="application/json", IfNoneMatch="*")
            return "written"
        except Exception as exc:  # noqa: BLE001
            if "PreconditionFailed" in str(exc) or "412" in str(exc):
                return "exists"
            raise

    sidecar = args.inp + ".failures.jsonl"
    paths = [args.inp] + ([sidecar] if os.path.exists(sidecar) else [])
    for path in paths:
        with open(path, encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                row = json.loads(line)
                if "_report" in row:
                    report = row["_report"]; continue
                at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                checker = str(row.get("checker") or "")
                if row.get("passed") is not True:
                    fid = sha(json.dumps({"task_id": row.get("task_id"), "sample": row.get("sample"), "burst": args.burst}, sort_keys=True).encode())[:32]
                    if put_absent("factory/bursts/%s/attempts/%s.json" % (args.burst, fid),
                                  {"schema_version": "factory-burst-attempt.v1", "burst": args.burst, "task_id": row.get("task_id"), "sample": row.get("sample"), "passed": False,
                                   "reason": row.get("reason"), "cases": row.get("cases"), "judge": row.get("judge"), "checker": checker, "run_id": args.run_id, "at": at,
                                   "solution": str(row.get("solution") or "")[:20000], "solution_sha256": sha(str(row.get("solution") or "").encode())}) == "written":
                        counts["failures_recorded"] += 1
                    continue
                vid = sha(json.dumps({"task_id": row["task_id"], "completion_sha256": row.get("completion_sha256")}, sort_keys=True).encode())[:32]
                verdict = {"schema_version": "factory-burst-verdict.v1", "burst": args.burst, "task_id": row["task_id"], "sample": row.get("sample"), "passed": True,
                           "completion_sha256": row.get("completion_sha256"), "solution_sha256": sha(str(row.get("solution") or "").encode()),
                           "tests_sha256": sha(str(row.get("tests") or "").encode()), "cases": row.get("cases"), "judge": row.get("judge"), "checker": checker,
                           "verify_elapsed_s": row.get("verify_elapsed_s"), "verify_isolation": row.get("verify_isolation"), "writer": "factory-trace-verify",
                           "verified_by": "owner_runner", "run_id": args.run_id, "at": at}
                if put_absent("factory/bursts/%s/verdicts/%s.json" % (args.burst, vid), verdict) == "written":
                    counts["verdicts"] += 1
                if str(row.get("family")) == "owner-task":
                    result_key = "factory/queue/tasks/%s-result-%s.json" % (row["task_id"], vid[:8])
                    result = {"schema_version": "factory-task-result.v1", "task": row["task_id"], "burst": args.burst, "passed": True, "solution": row["solution"],
                              "solution_sha256": verdict["solution_sha256"], "cases": row.get("cases"), "judge": row.get("judge"), "checker": checker,
                              "verified_by": "owner_runner", "run_id": args.run_id, "at": at}
                    state = put_absent(result_key, result)
                    if state == "exists":
                        prior = json.loads(s3.get_object(Bucket=PRIVATE, Key=result_key)["Body"].read())
                        if prior.get("solution_sha256") != result["solution_sha256"]:
                            raise RuntimeError("owner-task result conflict for %s: existing content differs" % result_key)
                        counts["owner_replays"] += 1
                    else:
                        counts["owner_results"] += 1
                    if row.get("trainable") is not True:
                        counts["skipped_not_trainable"] += 1
                        continue
                if not checker.startswith("factory-code-verify:v") or row.get("judge") != "supervisor":
                    counts["skipped_not_supervisor_judge"] += 1
                    continue
                if row["task_id"] in seen_tasks and not args.all_samples:
                    counts["skipped_duplicate_task"] += 1
                    continue
                seen_tasks.add(row["task_id"])
                doc = {"schema_version": "factory-curriculum-row.v1", "task_id": row["task_id"], "kind": "self_trace", "family": row.get("family") or "code",
                       "license": "own" if str(row.get("family")) != "owner-task" else "owner-consented", "source_url": "s3://%s/factory/bursts/%s/verdicts/%s.json" % (PRIVATE, args.burst, vid),
                       "citation": row.get("citation"), "source_sha": row.get("source_sha"), "prompt": row["prompt"], "solution": row["solution"],
                       "solution_sha256": verdict["solution_sha256"], "tests_sha256": verdict["tests_sha256"], "passed": True, "verified_by": "owner_runner",
                       "checker": checker, "judge": row.get("judge"), "cases": row.get("cases"), "writer": "factory-trace-verify", "run_id": args.run_id,
                       "burst": args.burst, "adapter_generation": row.get("adapter_generation"), "verify_elapsed_s": row.get("verify_elapsed_s"),
                       "trainable": True, "receipt": "factory/bursts/%s/verdicts/%s.json" % (args.burst, vid)}
                key = VERIFIED_PREFIX + vid + ".json"
                counts["rows_written" if put_absent(key, doc) == "written" else "rows_existing"] += 1
    summary = {"schema_version": "factory-burst-summary.v1", "burst": args.burst, "run_id": args.run_id, **counts, "report": report,
               "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    s3.put_object(Bucket=PRIVATE, Key="factory/bursts/%s/summary-%s.json" % (args.burst, args.run_id), Body=json.dumps(summary, sort_keys=True).encode(), ContentType="application/json")
    print(json.dumps(summary)); return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    j = sub.add_parser("join"); j.add_argument("--traces", required=True); j.add_argument("--candidates", required=True); j.add_argument("--out", required=True); j.add_argument("--holdout-ids"); j.add_argument("--task-cards")
    w = sub.add_parser("write"); w.add_argument("--in", dest="inp", required=True); w.add_argument("--burst", required=True); w.add_argument("--run-id", required=True); w.add_argument("--all-samples", action="store_true")
    args = ap.parse_args(argv)
    return cmd_join(args) if args.cmd == "join" else cmd_write(args)


if __name__ == "__main__":
    sys.exit(main())
