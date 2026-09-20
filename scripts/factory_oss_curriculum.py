"""Outside-world coding curriculum for the factory student (Claude Ship 2, 2026-09-13).

Doctrine: JustHodl is the exam hall, not the school. The student learns to code from
public, permissively licensed task sets whose tests are the checker -- never from the si
repo, never from Khalid, never from a paid model. Every row carries its license and source
URL; only rows the isolated runner executed to PASS become training rows (verified_by=owner_runner).

Sources (allow-list; add one only with its license line):
  mbpp      google-research/google-research mbpp/mbpp.jsonl        CC-BY-4.0   train+validation ids only
  apps      codeparrot/apps (Hugging Face)                           MIT         train split, introductory first
  humaneval openai/human-eval data/HumanEval.jsonl.gz                MIT         EXAM ONLY -- never in the lake

Subcommands (all run on the GitHub runner; `fetch`/`exam` need the network, `write` needs AWS):
  fetch  --sources mbpp,apps --max N --out candidates.jsonl
  exam   --ids-out humaneval-ids.json [--write]      freeze HumanEval prompts as the private code exam
  write  --in verified.jsonl --run-id ID              write verified rows to factory/curriculum/code/verified/
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import re
import subprocess
import sys
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
PRIVATE = "justhodl-ai-857687956942"
VERIFIED_PREFIX = "factory/curriculum/code/verified/"
EXAM_PROMPTS_KEY = "factory/exams/code/holdout-prompts.jsonl"
EXAM_TESTS_KEY = "factory/exams/code/holdout-tests.jsonl"
UA = {"User-Agent": "JustHodl-factory-curriculum (+https://justhodl.ai)"}

SOURCES = {
    "mbpp": {"url": "https://raw.githubusercontent.com/google-research/google-research/master/mbpp/mbpp.jsonl",
             "license": "CC-BY-4.0", "citation": "Austin et al. 2021, Program Synthesis with Large Language Models (MBPP)",
             "kind": "public_benchmark_train"},
    "apps": {"hf": "codeparrot/apps", "license": "MIT", "citation": "Hendrycks et al. 2021, Measuring Coding Challenge Competence with APPS",
             "kind": "public_benchmark_train"},
    "humaneval": {"url": "https://raw.githubusercontent.com/openai/human-eval/master/data/HumanEval.jsonl.gz",
                  "license": "MIT", "citation": "Chen et al. 2021, Evaluating Large Language Models Trained on Code (HumanEval)",
                  "kind": "exam_only"},
}
MBPP_TRAIN_IDS = range(601, 975)       # official train split
MBPP_VALIDATION_IDS = range(511, 601)  # official validation split (used for training here; test ids 11-510 stay out)


def clean_text(value) -> str:
    """Training targets are LF-only with no trailing whitespace: 463/464 MBPP reference solutions ship
    CRLF, which would otherwise be learned verbatim (2026-09-13 audit)."""
    text = str(value or "").replace("\r\n", "\n").replace("\r", "\n")
    return "\n".join(line.rstrip() for line in text.split("\n")).strip("\n") + "\n"


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def fetch_bytes(url: str, limit: int = 64 * 1024 * 1024) -> bytes:
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=60) as resp:
        return resp.read(limit)


def git_sha() -> str:
    try:
        return subprocess.run(["git", "rev-parse", "HEAD"], cwd=REPO, text=True, capture_output=True, timeout=10).stdout.strip()[:12]
    except Exception:  # noqa: BLE001
        return "unknown"


# ------------------------------------------------------------------ fetchers
def mbpp_rows(raw: bytes):
    src = SOURCES["mbpp"]
    for line in raw.decode("utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        tid = int(row.get("task_id") or 0)
        if tid not in MBPP_TRAIN_IDS and tid not in MBPP_VALIDATION_IDS:
            continue
        tests = "\n".join(row.get("test_list") or [])
        setup = row.get("test_setup_code") or ""
        prompt = "%s\nYour code should pass these tests:\n%s" % (str(row.get("text") or "").strip(), tests)
        yield {"task_id": "mbpp-%d" % tid, "kind": src["kind"], "family": "mbpp", "license": src["license"], "source_url": src["url"],
               "citation": src["citation"], "source_sha": sha(line.encode("utf-8")), "prompt": prompt,
               "solution": clean_text(row.get("code")), "tests": clean_text(setup + "\n" + tests), "timeout_s": 8}


APPS_DIFFICULTIES = ("introductory", "interview")     # 2026-09-19: interview problems join the supply (harder tasks teach more)
APPS_SOLUTIONS_PER_TASK = 4                            # try several reference solutions: APPS keeps many, and the first is often Python 2


def looks_python2(src: str) -> bool:
    return bool(re.search(r"^\s*print\s+[^(]", src, re.M) or "raw_input(" in src or re.search(r"\bxrange\(", src) or re.search(r"except\s+\w+\s*,\s*\w+:", src))


def apps_rows(max_rows: int):
    """APPS train split through the Hugging Face datasets library (runner only). Yields up to APPS_SOLUTIONS_PER_TASK
    candidate rows per problem (same task_id; the verifier judges each, the write step keeps the first that passes)."""
    from datasets import load_dataset  # type: ignore

    src = SOURCES["apps"]
    try:
        ds = load_dataset(src["hf"], split="train", trust_remote_code=False)
    except Exception as first:  # noqa: BLE001 -- script-backed hub datasets are refused by datasets>=3; use the parquet conversion
        try:
            ds = load_dataset("parquet", data_files="hf://datasets/%s@refs/convert/parquet/all/train/*.parquet" % src["hf"], split="train")
        except Exception as second:  # noqa: BLE001
            print(json.dumps({"apps": "unavailable", "direct": str(first)[:160], "parquet": str(second)[:160]}), file=sys.stderr)
            return
    n = 0
    for row in ds:
        if n >= max_rows:
            break
        difficulty = str(row.get("difficulty"))
        if difficulty not in APPS_DIFFICULTIES:
            continue
        try:
            solutions = json.loads(row.get("solutions") or "[]")
            io_pairs = json.loads(row.get("input_output") or "{}")
        except Exception:  # noqa: BLE001
            continue
        if not solutions or not io_pairs.get("inputs"):
            continue
        # stdin/stdout tasks: the test harness feeds each input and compares stdout
        tests = _stdio_tests(io_pairs)
        if not tests:
            continue
        pid = str(row.get("problem_id"))
        candidates = [str(sol) for sol in solutions if not looks_python2(str(sol))][:APPS_SOLUTIONS_PER_TASK]
        if not candidates:
            continue
        for i, sol in enumerate(candidates):
            yield {"task_id": "apps-%s" % pid, "kind": src["kind"], "family": "apps-" + difficulty, "license": src["license"],
                   "source_url": "https://huggingface.co/datasets/%s" % src["hf"], "citation": src["citation"],
                   "source_sha": sha(("apps:" + pid + "\n" + str(row.get("question") or "") + "\n" + tests).encode("utf-8")), "prompt": str(row.get("question") or "").strip(),
                   "solution": _as_function(sol), "tests": tests, "timeout_s": 12, "candidate": i}
        n += 1


def _as_function(program: str) -> str:
    """A stdin/stdout program stays a program. The test preamble (see _stdio_tests) executes the candidate source
    per case with redirected stdio, so nothing is re-indented and `if __name__ == "__main__":` guards work."""
    return program.replace("\r\n", "\n").rstrip() + "\n"


# First line marks a stdio task for the verifier runner: it must NOT import the candidate as a module (a stdin
# program would block on input()); the preamble runs the candidate source per case instead.
STDIO_PREAMBLE = """#stdio
import sys as __sys, io as __io
def __run(stdin_text):
    _in, _out = __sys.stdin, __sys.stdout
    __sys.stdin, __sys.stdout = __io.StringIO(stdin_text), __io.StringIO()
    try:
        exec(compile(__src, "candidate.py", "exec"), {"__name__": "__main__"})
    except SystemExit:
        pass
    finally:
        _text = __sys.stdout.getvalue()
        __sys.stdin, __sys.stdout = _in, _out
    return _text
"""


def _stdio_tests(io_pairs: dict) -> str:
    inputs, outputs = io_pairs.get("inputs") or [], io_pairs.get("outputs") or []
    if len(inputs) != len(outputs) or not inputs:
        return ""
    lines = [STDIO_PREAMBLE.rstrip("\n")]
    for i, (inp, out) in enumerate(zip(inputs[:8], outputs[:8])):
        if not isinstance(inp, str) or not isinstance(out, str):
            return ""
        lines.append("assert __run(%r).split() == %r.split(), 'case %d'" % (inp, out, i))
    return "\n".join(lines)


def humaneval_rows(raw: bytes):
    src = SOURCES["humaneval"]
    text = gzip.GzipFile(fileobj=io.BytesIO(raw)).read().decode("utf-8")
    for line in text.splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        yield {"task_id": str(row["task_id"]), "prompt": row["prompt"], "entry_point": row["entry_point"],
               "tests": row["test"] + "\ncheck(%s)\n" % row["entry_point"], "canonical_solution": row.get("canonical_solution"),
               "license": src["license"], "source_url": src["url"], "citation": src["citation"], "source_sha": sha(line.encode("utf-8"))}


# ------------------------------------------------------------------ commands
def cmd_fetch(args):
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    n = {"mbpp": 0, "apps": 0}
    with out.open("w", encoding="utf-8") as f:
        for name in [s.strip() for s in args.sources.split(",") if s.strip()]:
            if name == "mbpp":
                for row in mbpp_rows(fetch_bytes(SOURCES["mbpp"]["url"])):
                    f.write(json.dumps(row, sort_keys=True) + "\n")
                    n["mbpp"] += 1
            elif name == "apps":
                for row in apps_rows(args.max):
                    f.write(json.dumps(row, sort_keys=True) + "\n")
                    n["apps"] += 1
            elif name == "humaneval":
                raise SystemExit("humaneval is exam-only; use the `exam` subcommand")
            else:
                raise SystemExit("unknown source %s" % name)
    print(json.dumps({"candidates": n, "out": str(out)}))


def cmd_exam(args):
    rows = list(humaneval_rows(fetch_bytes(SOURCES["humaneval"]["url"])))
    ids = sorted(r["task_id"] for r in rows)
    Path(args.ids_out).write_text(json.dumps({"task_ids": ids, "source_shas": sorted(r["source_sha"] for r in rows),
                                              "license": SOURCES["humaneval"]["license"], "source_url": SOURCES["humaneval"]["url"]}, indent=1))
    print(json.dumps({"exam_tasks": len(ids), "ids_out": args.ids_out}))
    if not args.write:
        return
    import boto3

    s3 = boto3.client("s3", region_name="us-east-1")
    prompts = "\n".join(json.dumps({k: r[k] for k in ("task_id", "prompt", "entry_point", "license", "source_url", "citation")}, sort_keys=True) for r in rows) + "\n"
    tests = "\n".join(json.dumps({k: r[k] for k in ("task_id", "tests", "canonical_solution", "source_sha")}, sort_keys=True) for r in rows) + "\n"
    for key, body in ((EXAM_PROMPTS_KEY, prompts), (EXAM_TESTS_KEY, tests)):
        try:
            s3.put_object(Bucket=PRIVATE, Key=key, Body=body.encode("utf-8"), ContentType="application/jsonl", IfNoneMatch="*")
            print("written", key)
        except Exception as exc:  # noqa: BLE001
            code = getattr(exc, "response", {}).get("Error", {}).get("Code")
            if code in ("PreconditionFailed", "412"):
                print("exists", key)
            else:
                raise


def cmd_write(args):
    import boto3

    s3 = boto3.client("s3", region_name="us-east-1")
    checker = "factory-code-exam:%s" % git_sha()
    written = exists = skipped = 0
    for line in Path(args.infile).read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        if "_report" in row or row.get("passed") is not True:
            skipped += 1
            continue
        doc = {"schema_version": "factory-curriculum-row.v1", "task_id": row["task_id"], "kind": row["kind"], "family": row["family"],
               "license": row["license"], "source_url": row["source_url"], "citation": row.get("citation"), "source_sha": row["source_sha"],
               "prompt": row["prompt"], "solution": row["solution"], "tests_sha256": sha(str(row["tests"]).encode("utf-8")),
               "passed": True, "verified_by": "owner_runner", "checker": checker, "run_id": args.run_id, "verify_elapsed_s": row.get("verify_elapsed_s")}
        key = VERIFIED_PREFIX + sha(json.dumps({"task_id": row["task_id"], "source_sha": row["source_sha"]}, sort_keys=True).encode("utf-8"))[:32] + ".json"
        try:
            s3.put_object(Bucket=PRIVATE, Key=key, Body=json.dumps(doc, sort_keys=True).encode("utf-8"), ContentType="application/json", IfNoneMatch="*")
            written += 1
        except Exception as exc:  # noqa: BLE001
            code = getattr(exc, "response", {}).get("Error", {}).get("Code")
            if code in ("PreconditionFailed", "412"):
                exists += 1
            else:
                raise
    print(json.dumps({"written": written, "exists": exists, "skipped": skipped, "checker": checker}))


def main(argv=None):
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    f = sub.add_parser("fetch")
    f.add_argument("--sources", default="mbpp")
    f.add_argument("--max", type=int, default=800)
    f.add_argument("--out", required=True)
    e = sub.add_parser("exam")
    e.add_argument("--ids-out", required=True)
    e.add_argument("--write", action="store_true")
    w = sub.add_parser("write")
    w.add_argument("--in", dest="infile", required=True)
    w.add_argument("--run-id", default="local")
    args = ap.parse_args(argv)
    {"fetch": cmd_fetch, "exam": cmd_exam, "write": cmd_write}[args.cmd](args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
