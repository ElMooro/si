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
import shutil
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


def _drop_privileges():
    """uid/gid for the candidate process: `nobody` when we are root (the python:3.12-slim container), else None.
    A candidate that runs as nobody cannot touch the verifier's files or the mounted /work results."""
    if os.name != "posix" or os.geteuid() != 0:
        return None
    try:
        import pwd
        entry = pwd.getpwnam("nobody")
        return entry.pw_uid, entry.pw_gid
    except (ImportError, KeyError):
        return None


def run_one(row: dict, workdir: str, default_timeout: float = 8.0) -> dict:
    ids = _drop_privileges()
    # Each candidate gets its own scratch directory: readable by everyone, writable only by the verifier,
    # plus a world-writable tmp for the candidate's own files. Nothing under /work is writable by `nobody`.
    scratch = tempfile.mkdtemp(prefix="cand-", dir=workdir)
    os.chmod(scratch, 0o755)
    sandbox_tmp = os.path.join(scratch, "tmp")
    os.mkdir(sandbox_tmp)
    os.chmod(sandbox_tmp, 0o1777)
    src = os.path.join(scratch, "candidate.py")
    tst = os.path.join(scratch, "tests.py")
    runner = os.path.join(scratch, "runner.py")
    for path, text in ((src, str(row["solution"])), (tst, str(row["tests"])), (runner, RUNNER)):
        with open(path, "w", encoding="utf-8") as f:
            f.write(text)
        os.chmod(path, 0o644)
    timeout = float(row.get("timeout_s") or default_timeout)
    env = {"PYTHONHASHSEED": "0", "PATH": "/usr/bin:/bin", "PYTHONDONTWRITEBYTECODE": "1", "HOME": sandbox_tmp, "TMPDIR": sandbox_tmp}
    extra = {"user": ids[0], "group": ids[1], "extra_groups": []} if ids else {}
    t0 = time.monotonic()
    try:
        proc = subprocess.run([sys.executable, "-I", "-S", runner, src, tst], capture_output=True, text=True, timeout=timeout,
                              cwd=sandbox_tmp, env=env, **extra)
        ok = proc.returncode == 0 and proc.stdout.strip().endswith("PASS")
        return {"passed": ok, "elapsed_s": round(time.monotonic() - t0, 3), "stderr": proc.stderr[-300:] if not ok else "",
                "isolation": "unprivileged" if ids else "same-user"}
    except subprocess.TimeoutExpired:
        return {"passed": False, "elapsed_s": timeout, "stderr": "timeout", "isolation": "unprivileged" if ids else "same-user"}
    except Exception as exc:  # noqa: BLE001
        return {"passed": False, "elapsed_s": round(time.monotonic() - t0, 3), "stderr": str(exc)[:300], "isolation": "unprivileged" if ids else "same-user"}
    finally:
        shutil.rmtree(scratch, ignore_errors=True)


def main(argv=None):
    argv = argv or sys.argv[1:]
    if len(argv) != 2:
        print("usage: factory_code_verify.py candidates.jsonl verified.jsonl", file=sys.stderr)
        return 2
    src_path, out_path = argv
    report = {"seen": 0, "passed": 0, "failed": 0, "timeouts": 0, "malformed": 0}
    with open(src_path, encoding="utf-8") as fin, open(out_path, "w", encoding="utf-8") as fout, tempfile.TemporaryDirectory() as tmp:
        os.chmod(tmp, 0o755)  # traversable by the unprivileged candidate; only its own scratch tmp is writable
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
                fout.write(json.dumps(dict(row, passed=True, verify_elapsed_s=res["elapsed_s"], verify_isolation=res["isolation"]), sort_keys=True) + "\n")
            else:
                report["failed"] += 1
                if res["stderr"] == "timeout":
                    report["timeouts"] += 1
        fout.write(json.dumps({"_report": report}, sort_keys=True) + "\n")
    print(json.dumps(report))
    return 0


if __name__ == "__main__":
    sys.exit(main())
