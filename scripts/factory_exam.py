#!/usr/bin/env python3
"""The frozen holdout exam (HumanEval, private copy) -- the ONLY number that may be called learning (2026-09-14).

  tasks   --out prompts.jsonl        prompts-only task file for generate.py --mode exam (never the tests)
  grade   --completions exam.jsonl --tests holdout-tests.jsonl --prompts holdout-prompts.jsonl --generation gen-0 --run-id R
          joins completion -> prompt + completion (HumanEval convention), tests -> `#deep` suite ending in check(entry_point),
          runs the v4 verifier, and writes the promotion-shaped evaluation:
            {"evaluation_id": "humaneval-frozen-<sha of the tests file>", "independent": true, "held_out": true,
             "n": 164, "score": pass_rate, "critical_failures": <harness errors>, "generation": ..., ...}
          to factory/exams/code/results/<generation>-<run_id>.json; gen-0 also pins factory/exams/code/results/base.json.
"""
from __future__ import annotations

import argparse
import hashlib
import re
import textwrap
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

PRIVATE = os.environ.get("FACTORY_PRIVATE_BUCKET", "justhodl-ai-857687956942")
REGION = os.environ.get("AWS_REGION", "us-east-1")
RESULTS_PREFIX = "factory/exams/code/results/"
HERE = Path(__file__).resolve().parent


def sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def read_jsonl(path):
    rows = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def cmd_tasks(args):
    prompts = read_jsonl(args.prompts)
    with open(args.out, "w", encoding="utf-8") as f:
        for r in prompts:
            f.write(json.dumps({"task_id": r["task_id"], "prompt": r["prompt"], "family": "humaneval", "holdout": True}, sort_keys=True) + "\n")
    print(json.dumps({"tasks": len(prompts), "out": args.out}))
    return 0


FENCE = re.compile(r"```(?:python|py)?\s*\n(.*?)```", re.S)


def join_solution(prompt, completion, entry_point):
    """HumanEval convention is prompt + body continuation; an instruct model may instead answer in prose with a fenced
    block, or restate the whole function. Take, in order: (1) prompt + completion if it compiles; (2) prompt + fenced/plain
    code indented as a body; (3) the fenced/plain code alone when it defines the entry point (plus the prompt's imports).
    Nothing here changes what the model wrote; it only chooses which byte range is the program."""
    text = completion.replace("\r\n", "\n")
    code = (FENCE.search(text).group(1) if FENCE.search(text) else text).rstrip() + "\n"
    imports = "\n".join(l for l in prompt.splitlines() if l.startswith(("import ", "from "))) + "\n"
    defines = re.search(r"^\s*def\s+%s\s*\(" % re.escape(entry_point), code, flags=re.M) is not None
    candidates = []
    if not defines:
        candidates.append(prompt + text)
        candidates.append(prompt + textwrap.indent(code, "    ") if not code.startswith((" ", "\t")) else prompt + code)
    else:
        candidates.append(imports + code)
    candidates.append(prompt + code)
    for cand in candidates:
        try:
            compile(cand, "candidate.py", "exec")
            return cand
        except SyntaxError:
            continue
    return candidates[0]


def cmd_grade(args):
    prompts = {r["task_id"]: r for r in read_jsonl(args.prompts)}
    tests_raw = Path(args.tests).read_bytes()
    tests = {r["task_id"]: r for r in read_jsonl(args.tests)}
    completions = read_jsonl(args.completions)
    evaluation_id = "humaneval-frozen-" + sha(tests_raw)[:16]
    by_task = {}
    for c in completions:
        by_task.setdefault(str(c.get("task_id")), []).append(c)
    candidates = []
    for tid, t in tests.items():
        p = prompts.get(tid)
        comps = by_task.get(tid) or []
        if not p or not comps:
            continue
        comp = comps[0]                                   # exam is greedy, one sample per task
        solution = join_solution(p["prompt"], str(comp.get("completion") or ""), p["entry_point"])
        suite = "#deep\n" + str(t["tests"]) + "\n\ncheck(%s)\n" % p["entry_point"]
        candidates.append({"task_id": tid, "solution": solution, "tests": suite, "timeout_s": 12})
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        (tmp / "cands.jsonl").write_text("\n".join(json.dumps(c) for c in candidates) + "\n", encoding="utf-8")
        verify = HERE / "factory_code_verify.py"
        proc = subprocess.run([sys.executable, str(verify), str(tmp / "cands.jsonl"), str(tmp / "out.jsonl")], capture_output=True, text=True, timeout=3600)
        out = read_jsonl(tmp / "out.jsonl")
        fails = read_jsonl(tmp / "out.jsonl.failures.jsonl") if (tmp / "out.jsonl.failures.jsonl").exists() else []
    report = out[-1]["_report"] if out and "_report" in out[-1] else {}
    passed_ids = sorted(r["task_id"] for r in out if r.get("passed"))
    harness_errors = sum(1 for f in fails if str(f.get("reason", "")).startswith(("protocol_violation", "refused_unsupported_suite", "tests_unparseable")))
    n = len(tests)
    result = {"schema_version": "factory-exam-result.v1", "evaluation_id": evaluation_id, "independent": True, "held_out": True,
              "generation": args.generation, "n": n, "attempted": len(candidates), "passed": len(passed_ids), "score": round(len(passed_ids) / n, 4) if n else 0.0,
              "pass_rate": round(len(passed_ids) / n, 4) if n else 0.0, "critical_failures": harness_errors, "missing_completions": n - len(candidates),
              "checker": report.get("checker"), "partial_judge_passes": report.get("partial_judge"), "tests_sha256": sha(tests_raw),
              "prompts_sha256": sha(Path(args.prompts).read_bytes()), "completions_sha256": sha(Path(args.completions).read_bytes()),
              "burst": args.burst, "run_id": args.run_id, "verifier_stderr": (proc.stderr or "")[-300:], "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
              "passed_task_ids": passed_ids, "failure_reasons": {f["task_id"]: f.get("reason") for f in fails}}
    print(json.dumps({k: result[k] for k in ("evaluation_id", "generation", "n", "attempted", "passed", "score", "critical_failures", "missing_completions")}))
    if args.out:
        Path(args.out).write_text(json.dumps(result, sort_keys=True) + "\n", encoding="utf-8")
    if args.write:
        upload(result)
    return 0


def upload(result):
    """Write the evaluation objects (runner host, credentials; grading itself happened in the container)."""
    import boto3
    s3 = boto3.client("s3", region_name=REGION)
    body = json.dumps(result, sort_keys=True).encode("utf-8")
    args = argparse.Namespace(generation=result["generation"], run_id=result["run_id"])
    if True:
        s3.put_object(Bucket=PRIVATE, Key="%s%s-%s.json" % (RESULTS_PREFIX, args.generation, args.run_id), Body=body, ContentType="application/json")
        if args.generation in ("gen-0", "base"):
            existing = None
            try:
                existing = json.loads(s3.get_object(Bucket=PRIVATE, Key=RESULTS_PREFIX + "base.json")["Body"].read())
            except Exception:  # noqa: BLE001
                existing = None
            trusted = int(result.get("critical_failures") or 0) == 0 and int(result.get("missing_completions") or 0) == 0
            if existing is None or (trusted and int(existing.get("critical_failures") or 0) > 0):
                s3.put_object(Bucket=PRIVATE, Key=RESULTS_PREFIX + "base.json", Body=body, ContentType="application/json")
                print("base exam pinned (%s)" % ("replaced an untrusted base" if existing else "first"))
            else:
                print("base exam kept; this run recorded as gen-0-%s (trusted=%s)" % (args.run_id, trusted))


def cmd_upload(args):
    upload(json.loads(Path(args.result).read_text(encoding="utf-8")))
    return 0


def main(argv=None):
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    t = sub.add_parser("tasks"); t.add_argument("--prompts", required=True); t.add_argument("--out", required=True)
    g = sub.add_parser("grade"); g.add_argument("--completions", required=True); g.add_argument("--tests", required=True); g.add_argument("--prompts", required=True)
    g.add_argument("--generation", required=True); g.add_argument("--run-id", required=True); g.add_argument("--burst", default=None); g.add_argument("--write", action="store_true")
    g.add_argument("--out", default=None)
    u = sub.add_parser("upload"); u.add_argument("--result", required=True)
    args = ap.parse_args(argv)
    return {"tasks": cmd_tasks, "grade": cmd_grade, "upload": cmd_upload}[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
