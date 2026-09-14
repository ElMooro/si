#!/usr/bin/env python3
"""Independent code verifier (v2, 2026-09-14; audit F01/F02/F21).

The judge never shares authority with the candidate:

  * function tasks (MBPP-style `assert f(x) == y` suites): the SUPERVISOR parses every assert into a
    left-hand expression and a literal expected value. The candidate process receives only the
    expressions, evaluates them and returns reprs over stdout as JSON; the supervisor compares against
    expected values it alone holds. A candidate cannot forge a verdict because it never sees one.
    Asserts that are not `expr == literal` are evaluated in the candidate process as a bare boolean and
    the row is tagged `judge: partial` so downstream policy can weigh it.
  * stdio tasks (APPS-style, tests beginning with `#stdio`): one real subprocess per case, bytes in,
    bytes out, exit status preserved, output compared by the supervisor (whitespace-token rule).
  * empty, absent or zero-case suites are REFUSED (F02).

Per candidate: unprivileged user (`nobody` when root), isolated scratch, timeouts, output bounded.
Every output row carries passed, cases, judge, checker; failures are kept as rows too (F10).

Usage: factory_code_verify.py candidates.jsonl verified.jsonl
"""
from __future__ import annotations

import ast
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time

CHECKER = "factory-code-verify:v2-supervisor-judge"
STDIO_MARK = "#stdio"
MAX_OUTPUT = 200_000

# The candidate process evaluates expressions it is handed and reports reprs; it never learns what is expected.
EVAL_RUNNER = r'''
import sys, json, io
src = open(sys.argv[1], encoding="utf-8").read()
exprs = json.load(open(sys.argv[2], encoding="utf-8"))
ns = {"__name__": "__candidate__"}
out = []
_stdout = sys.stdout
sys.stdout = io.StringIO()
try:
    exec(compile(src, "candidate.py", "exec"), ns)
    for e in exprs:
        try:
            v = eval(compile(e["expr"], "expr", "eval"), ns)
            out.append({"i": e["i"], "repr": repr(v)[:4000]})
        except BaseException as exc:
            out.append({"i": e["i"], "error": type(exc).__name__})
except BaseException as exc:
    out.append({"i": -1, "error": "load:" + type(exc).__name__})
sys.stdout = _stdout
sys.stdout.write("\n" + json.dumps(out) + "\n")
sys.stdout.flush()
'''


def _drop_privileges():
    if os.name != "posix" or os.geteuid() != 0:
        return None
    try:
        import pwd
        entry = pwd.getpwnam("nobody")
        return entry.pw_uid, entry.pw_gid
    except (ImportError, KeyError):
        return None


def parse_asserts(tests: str):
    """[{i, expr, expected|None, truthy}] from a test suite; non-assert statements become a preamble."""
    try:
        tree = ast.parse(tests)
    except SyntaxError:
        return None, ""
    cases, preamble = [], []
    for node in tree.body:
        if isinstance(node, ast.Assert):
            test = node.test
            if isinstance(test, ast.Compare) and len(test.ops) == 1 and isinstance(test.ops[0], ast.Eq):
                left, right = test.left, test.comparators[0]
                try:
                    expected = ast.literal_eval(right)
                    cases.append({"i": len(cases), "expr": ast.unparse(left), "expected": repr(expected), "truthy": False})
                    continue
                except (ValueError, SyntaxError):
                    pass
            cases.append({"i": len(cases), "expr": ast.unparse(test), "expected": None, "truthy": True})
        else:
            preamble.append(ast.unparse(node))
    return cases, "\n".join(preamble)


# Static screen for FUNCTION tasks only (stdio programs may legitimately exit): a solution that reaches for
# process control or the raw output descriptor has no honest reason to, and it is the only remaining route to
# forge the runner's result line before the runner writes it (documented residual; hidden tests close it fully).
FUNCTION_TASK_FORBIDDEN = ("os._exit", "os.write(", "__stdout__", "sys.exit(", "os.kill", "signal.", "os.dup", "os.close(", "subprocess", "ctypes", "gc.get_", "sys._getframe", "inspect.")


def judge_function_task(row, scratch, env, extra, timeout):
    banned = [tok for tok in FUNCTION_TASK_FORBIDDEN if tok in str(row["solution"])]
    if banned:
        return {"passed": False, "cases": 0, "judge": "static", "stderr": "refused_forbidden_token:" + ",".join(banned)[:100]}
    cases, preamble = parse_asserts(str(row["tests"]))
    if cases is None:
        return {"passed": False, "cases": 0, "judge": "supervisor", "stderr": "tests_unparseable"}
    if not cases:
        return {"passed": False, "cases": 0, "judge": "supervisor", "stderr": "refused_zero_case_suite"}
    src = os.path.join(scratch, "candidate.py")
    exprs = os.path.join(scratch, "exprs.json")
    runner = os.path.join(scratch, "runner.py")
    program = (preamble + "\n\n" if preamble else "") + str(row["solution"])
    for path, text in ((src, program), (exprs, json.dumps([{"i": c["i"], "expr": c["expr"]} for c in cases])), (runner, EVAL_RUNNER)):
        with open(path, "w", encoding="utf-8") as f:
            f.write(text)
        os.chmod(path, 0o644)
    t0 = time.monotonic()
    try:
        proc = subprocess.run([sys.executable, "-I", "-S", runner, src, exprs], capture_output=True, text=True, timeout=timeout,
                              cwd=os.path.join(scratch, "tmp"), env=env, **extra)
    except subprocess.TimeoutExpired:
        return {"passed": False, "cases": len(cases), "judge": "supervisor", "stderr": "timeout", "elapsed_s": timeout}
    elapsed = round(time.monotonic() - t0, 3)
    last = (proc.stdout or "")[-MAX_OUTPUT:].strip().splitlines()
    try:
        reported = json.loads(last[-1]) if last else []
    except ValueError:
        reported = []
    if not isinstance(reported, list):
        reported = []
    by_i = {r.get("i"): r for r in reported if isinstance(r, dict)}
    if -1 in by_i:
        return {"passed": False, "cases": len(cases), "judge": "supervisor", "stderr": "candidate_failed_to_load", "elapsed_s": elapsed}
    partial = False
    for c in cases:
        got = by_i.get(c["i"])
        if not got or "error" in got:
            return {"passed": False, "cases": len(cases), "judge": "supervisor", "stderr": "case %d %s" % (c["i"], (got or {}).get("error", "missing")), "elapsed_s": elapsed}
        if c["truthy"]:
            partial = True
            if got.get("repr") != "True":
                return {"passed": False, "cases": len(cases), "judge": "partial", "stderr": "case %d not truthy" % c["i"], "elapsed_s": elapsed}
        elif got.get("repr") != c["expected"]:
            return {"passed": False, "cases": len(cases), "judge": "supervisor", "stderr": "case %d mismatch" % c["i"], "elapsed_s": elapsed}
    return {"passed": proc.returncode == 0, "cases": len(cases), "judge": "partial" if partial else "supervisor",
            "stderr": "" if proc.returncode == 0 else "exit %s" % proc.returncode, "elapsed_s": elapsed}


def parse_stdio(tests: str):
    """Cases from the stdio suite: `assert __run(<input>).split() == <output>.split()` lines."""
    cases = []
    try:
        tree = ast.parse(tests)
    except SyntaxError:
        return None
    for node in tree.body:
        if not isinstance(node, ast.Assert) or not isinstance(node.test, ast.Compare):
            continue
        left, right = node.test.left, node.test.comparators[0]
        try:
            inp = ast.literal_eval(left.func.value.args[0])
            out = ast.literal_eval(right.func.value)
        except Exception:  # noqa: BLE001
            continue
        if isinstance(inp, str) and isinstance(out, str):
            cases.append({"i": len(cases), "input": inp, "expected": out.split()})
    return cases


def judge_stdio_task(row, scratch, env, extra, timeout):
    cases = parse_stdio(str(row["tests"]))
    if cases is None:
        return {"passed": False, "cases": 0, "judge": "supervisor", "stderr": "tests_unparseable"}
    if not cases:
        return {"passed": False, "cases": 0, "judge": "supervisor", "stderr": "refused_zero_case_suite"}
    src = os.path.join(scratch, "candidate.py")
    with open(src, "w", encoding="utf-8") as f:
        f.write(str(row["solution"]))
    os.chmod(src, 0o644)
    t0 = time.monotonic()
    for c in cases:
        try:
            proc = subprocess.run([sys.executable, "-I", "-S", src], input=c["input"].encode("utf-8"), capture_output=True,
                                  timeout=timeout, cwd=os.path.join(scratch, "tmp"), env=env, **extra)
        except subprocess.TimeoutExpired:
            return {"passed": False, "cases": len(cases), "judge": "supervisor", "stderr": "timeout case %d" % c["i"], "elapsed_s": timeout}
        if proc.returncode != 0:
            return {"passed": False, "cases": len(cases), "judge": "supervisor", "stderr": "case %d exit %s" % (c["i"], proc.returncode), "elapsed_s": round(time.monotonic() - t0, 3)}
        got = proc.stdout[:MAX_OUTPUT].decode("utf-8", "replace").split()
        if got != c["expected"]:
            return {"passed": False, "cases": len(cases), "judge": "supervisor", "stderr": "case %d mismatch" % c["i"], "elapsed_s": round(time.monotonic() - t0, 3)}
    return {"passed": True, "cases": len(cases), "judge": "supervisor", "stderr": "", "elapsed_s": round(time.monotonic() - t0, 3)}


def run_one(row: dict, workdir: str, default_timeout: float = 8.0) -> dict:
    ids = _drop_privileges()
    tests = str(row.get("tests") or "")
    if not tests.strip():
        return {"passed": False, "cases": 0, "judge": "supervisor", "stderr": "refused_empty_suite", "elapsed_s": 0.0, "isolation": "static", "checker": CHECKER}
    scratch = tempfile.mkdtemp(prefix="cand-", dir=workdir)
    os.chmod(scratch, 0o755)
    sandbox_tmp = os.path.join(scratch, "tmp")
    os.mkdir(sandbox_tmp)
    os.chmod(sandbox_tmp, 0o1777)
    timeout = float(row.get("timeout_s") or default_timeout)
    env = {"PYTHONHASHSEED": "0", "PATH": "/usr/bin:/bin", "PYTHONDONTWRITEBYTECODE": "1", "HOME": sandbox_tmp, "TMPDIR": sandbox_tmp}
    extra = {"user": ids[0], "group": ids[1], "extra_groups": []} if ids else {}
    try:
        res = judge_stdio_task(row, scratch, env, extra, timeout) if tests.lstrip().startswith(STDIO_MARK) else judge_function_task(row, scratch, env, extra, timeout)
    except Exception as exc:  # noqa: BLE001
        res = {"passed": False, "cases": 0, "judge": "supervisor", "stderr": type(exc).__name__ + ":" + str(exc)[:200], "elapsed_s": 0.0}
    finally:
        shutil.rmtree(scratch, ignore_errors=True)
    res["isolation"] = "unprivileged" if ids else "same-user"
    res["checker"] = CHECKER
    return res


def main(argv=None):
    argv = argv or sys.argv[1:]
    if len(argv) != 2:
        print("usage: factory_code_verify.py candidates.jsonl verified.jsonl", file=sys.stderr)
        return 2
    src_path, out_path = argv
    report = {"seen": 0, "passed": 0, "failed": 0, "timeouts": 0, "malformed": 0, "refused_suites": 0, "refused_static": 0, "partial_judge": 0, "checker": CHECKER}
    failures_path = out_path + ".failures.jsonl"       # passes only in the main file (consumers rely on it); failures kept beside it (F10)
    with open(src_path, encoding="utf-8") as fin, open(out_path, "w", encoding="utf-8") as fout, open(failures_path, "w", encoding="utf-8") as ffail, \
            tempfile.TemporaryDirectory() as tmp:
        os.chmod(tmp, 0o755)
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
                if res.get("judge") == "partial":
                    report["partial_judge"] += 1
                fout.write(json.dumps(dict(row, passed=True, cases=res["cases"], judge=res["judge"], checker=CHECKER,
                                           verify_elapsed_s=res.get("elapsed_s"), verify_isolation=res["isolation"]), sort_keys=True) + "\n")
            else:
                report["failed"] += 1
                if str(res.get("stderr", "")).startswith("timeout"):
                    report["timeouts"] += 1
                reason = str(res.get("stderr", ""))
                if reason.startswith(("refused_empty_suite", "refused_zero_case_suite")):
                    report["refused_suites"] += 1
                elif reason.startswith("refused_forbidden_token"):
                    report["refused_static"] += 1
                ffail.write(json.dumps({"task_id": row["task_id"], "sample": row.get("sample"), "passed": False, "reason": str(res.get("stderr", ""))[:200],
                                        "cases": res["cases"], "judge": res["judge"], "checker": CHECKER, "_failure": True}, sort_keys=True) + "\n")
        fout.write(json.dumps({"_report": report}, sort_keys=True) + "\n")
    print(json.dumps(report))
    return 0


if __name__ == "__main__":
    sys.exit(main())
