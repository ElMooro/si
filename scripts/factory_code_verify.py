"""Isolated code verifier (Claude Ship 2). Runs INSIDE `docker run --network none` with no AWS
credentials, no repo on the path, no pip. Stdlib only.

Input : candidates.jsonl -- one {task_id, prompt, solution, tests, entry_point?, timeout_s?} per line
Output: verified.jsonl   -- the same rows with passed=true, plus a run report on the last line

A row passes only if its solution executes the tests without error inside the timeout. Any
exception, timeout, or non-zero exit is a fail and the row is dropped (never trained on).
Nothing here reads the network; the container has none. The writer job stamps verified_by.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time

RUNNER = r'''
import sys, json
src = open(sys.argv[1], encoding="utf-8").read()
tests = open(sys.argv[2], encoding="utf-8").read()
ns = {"__name__": "__candidate__"}
exec(compile(src, "candidate.py", "exec"), ns)
exec(compile(tests, "tests.py", "exec"), ns)
print("PASS")
'''


def run_one(row: dict, workdir: str, default_timeout: float = 8.0) -> dict:
    src = os.path.join(workdir, "candidate.py")
    tst = os.path.join(workdir, "tests.py")
    with open(src, "w", encoding="utf-8") as f:
        f.write(str(row["solution"]))
    with open(tst, "w", encoding="utf-8") as f:
        f.write(str(row["tests"]))
    runner = os.path.join(workdir, "runner.py")
    with open(runner, "w", encoding="utf-8") as f:
        f.write(RUNNER)
    timeout = float(row.get("timeout_s") or default_timeout)
    t0 = time.monotonic()
    try:
        proc = subprocess.run([sys.executable, "-I", "-S", runner, src, tst], capture_output=True, text=True, timeout=timeout, cwd=workdir,
                              env={"PYTHONHASHSEED": "0", "PATH": "/usr/bin:/bin", "PYTHONDONTWRITEBYTECODE": "1"})
        ok = proc.returncode == 0 and proc.stdout.strip().endswith("PASS")
        return {"passed": ok, "elapsed_s": round(time.monotonic() - t0, 3), "stderr": proc.stderr[-300:] if not ok else ""}
    except subprocess.TimeoutExpired:
        return {"passed": False, "elapsed_s": timeout, "stderr": "timeout"}
    except Exception as exc:  # noqa: BLE001
        return {"passed": False, "elapsed_s": round(time.monotonic() - t0, 3), "stderr": str(exc)[:300]}


def main(argv=None):
    argv = argv or sys.argv[1:]
    if len(argv) != 2:
        print("usage: factory_code_verify.py candidates.jsonl verified.jsonl", file=sys.stderr)
        return 2
    src_path, out_path = argv
    report = {"seen": 0, "passed": 0, "failed": 0, "timeouts": 0, "malformed": 0}
    with open(src_path, encoding="utf-8") as fin, open(out_path, "w", encoding="utf-8") as fout, tempfile.TemporaryDirectory() as tmp:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            report["seen"] += 1
            try:
                row = json.loads(line)
                assert isinstance(row.get("solution"), str) and isinstance(row.get("tests"), str) and row.get("task_id")
            except Exception:  # noqa: BLE001
                report["malformed"] += 1
                continue
            res = run_one(row, tmp)
            if res["passed"]:
                report["passed"] += 1
                fout.write(json.dumps(dict(row, passed=True, verify_elapsed_s=res["elapsed_s"]), sort_keys=True) + "\n")
            else:
                report["failed"] += 1
                if res["stderr"] == "timeout":
                    report["timeouts"] += 1
        fout.write(json.dumps({"_report": report}, sort_keys=True) + "\n")
    print(json.dumps(report))
    return 0


if __name__ == "__main__":
    sys.exit(main())
