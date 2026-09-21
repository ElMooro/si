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

CHECKER = "factory-code-verify:v4-supervisor-judge"
STDIO_MARK = "#stdio"
# Static screen for FUNCTION tasks only (stdio programs may legitimately exit): a solution reaching for process control,
# the raw descriptor, frames/tracebacks or the runner module has no honest reason to. In-process authority cannot be
# made airtight; this narrows the residual and hidden tests close it (documented in OWNED.md).
FUNCTION_TASK_FORBIDDEN = ("os._exit", "os.write(", "__stdout__", "sys.exit(", "os.kill", "signal.", "os.dup", "os.close(", "subprocess", "ctypes", "gc.get_",
                           "sys._getframe", "inspect.", "sys.exc_info", "__traceback__", "tb_frame", "f_back", "sys.settrace", "sys.setprofile", "__main__",
                           "__builtins__", "builtins.", "importlib", "os.fork", "os.exec")
MAX_OUTPUT = 200_000

# The candidate process evaluates expressions it is handed and reports reprs; it never learns what is expected.
EVAL_RUNNER = r"""
import sys, os, json, io
# Everything the reporter needs is bound HERE, before any candidate code runs, and referenced through closure cells /
# default arguments -- never through module globals or builtins at call time (audit B03: a candidate rebinding
# __main__._enc or builtins.repr must not change what is reported).
def _make_reporter(_write=os.write, _dumps=json.dumps, _type=type, _str=str, _repr=repr, _sorted=sorted, _len=len, _int=int,
                   _bool=bool, _float=float, _list=list, _tuple=tuple, _set=set, _frozenset=frozenset, _dict=dict, _isinstance=isinstance,
                   _BaseException=BaseException, _TypeError=TypeError, _ValueError=ValueError):
    STR_MAX, SEQ_MAX, DEPTH_MAX = 20000, 5000, 12
    def enc(v, depth=0):
        t = _type(v)
        if depth > DEPTH_MAX: raise _TypeError("depth")
        if t is _bool: return {"t": "bool", "v": v}
        if t is _int: return {"t": "int", "v": _str(v)}
        if t is _float: return {"t": "float", "v": _repr(v)}
        if t is _str:
            if _len(v) > STR_MAX: raise _ValueError("oversize_str")          # B04: never truncate; oversize is unsupported
            return {"t": "str", "v": v}
        if v is None: return {"t": "none"}
        if t is _list or t is _tuple:
            if _len(v) > SEQ_MAX: raise _ValueError("oversize_seq")
            return {"t": "list" if t is _list else "tuple", "v": [enc(x, depth + 1) for x in v]}
        if t is _set or t is _frozenset:
            if _len(v) > SEQ_MAX: raise _ValueError("oversize_seq")
            return {"t": "set", "v": [enc(x, depth + 1) for x in _sorted(v, key=_repr)]}
        if t is _dict:
            if _len(v) > SEQ_MAX: raise _ValueError("oversize_seq")
            return {"t": "dict", "v": [[enc(k, depth + 1), enc(x, depth + 1)] for k, x in _list(v.items())]}
        raise _TypeError(t.__name__)
    def line(obj):
        _write(1, ("\n" + _dumps(obj) + "\n").encode("utf-8"))
    def report(i, value):
        try:
            line({"i": i, "val": enc(value)})
        except _BaseException as exc:
            line({"i": i, "error": "unsupported:" + _type(exc).__name__})
    return report, line
__report, _line = _make_reporter()
_exit = os._exit
_src = open(sys.argv[1], encoding="utf-8").read()
_suite = open(sys.argv[2], encoding="utf-8").read()
_cand = {"__name__": "__candidate__"}
sys.stdout = io.StringIO()                                     # candidate prints never reach the descriptor
try:
    exec(compile(_src, "candidate.py", "exec"), _cand)
except BaseException as exc:
    _line({"i": -1, "error": "load:" + type(exc).__name__}); _exit(0)
_ns = dict(_cand)
_ns["__report"] = __report
try:
    exec(compile(_suite, "suite.py", "exec"), _ns)
except BaseException as exc:
    _line({"i": -2, "error": "suite:" + type(exc).__name__})
_line({"i": -3, "done": True})
_exit(0)
"""


def _drop_privileges():
    if os.name != "posix" or os.geteuid() != 0:
        return None
    try:
        import pwd
        entry = pwd.getpwnam("nobody")
        return entry.pw_uid, entry.pw_gid
    except (ImportError, KeyError):
        return None


class Unsupported(ValueError):
    pass


DEEP_MARK = "#deep"      # exam suites: asserts live inside `def check(candidate)`; the suite is transformed at every depth


def _rewrite_assert(node, cases):
    """One assert -> the __report call statement; appends the case descriptor. Raises Unsupported for other comparisons."""
    test = node.test
    if isinstance(test, ast.Compare) and len(test.ops) == 1 and isinstance(test.ops[0], ast.Eq):
        try:
            expected = ast.literal_eval(test.comparators[0])
            cases.append({"i": len(cases), "kind": "eq", "expected": expected, "src": ast.unparse(test.left)})
            return ast.parse("__report(%d, (%s))" % (cases[-1]["i"], ast.unparse(test.left))).body[0]
        except (ValueError, SyntaxError):
            raise Unsupported("assert with non-literal right-hand side: " + ast.unparse(test)[:80])
    if isinstance(test, ast.Compare):
        raise Unsupported("unsupported comparison operator: " + ast.unparse(test)[:80])
    if isinstance(test, ast.UnaryOp) and isinstance(test.op, ast.Not):
        cases.append({"i": len(cases), "kind": "falsy", "src": ast.unparse(test.operand)})
        return ast.parse("__report(%d, (%s))" % (cases[-1]["i"], ast.unparse(test.operand))).body[0]
    cases.append({"i": len(cases), "kind": "truthy", "src": ast.unparse(test)})
    return ast.parse("__report(%d, (%s))" % (cases[-1]["i"], ast.unparse(test))).body[0]


def transform_suite_deep(tests: str):
    """Exam mode: every assert at any depth becomes a report, in source order; non-literal comparisons are reported as
    truthy/falsy values (judge=partial for that case). The suite must end by calling its check function itself."""
    tree = ast.parse(tests)
    cases = []

    class Rewriter(ast.NodeTransformer):
        def visit_Assert(self, node):
            try:
                return _rewrite_assert(node, cases)
            except Unsupported:
                # exam suites use abs()/sorted()/set() comparisons: report the whole test expression's truthiness
                cases.append({"i": len(cases), "kind": "truthy", "src": ast.unparse(node.test), "partial": True})
                return ast.parse("__report(%d, (%s))" % (cases[-1]["i"], ast.unparse(node.test))).body[0]
    new_tree = Rewriter().visit(tree)
    ast.fix_missing_locations(new_tree)
    return cases, ast.unparse(new_tree) + "\n"


def transform_suite(tests: str):
    """(cases, program): every `assert` becomes `__report(i, <value>)` IN PLACE so stateful order is preserved.
    `assert expr == <literal>` compares typed values in the supervisor; `assert expr` / `assert not expr` compare
    truthiness of a typed value; anything else is unsupported and the whole suite is refused (A04)."""
    if tests.lstrip().startswith(DEEP_MARK):
        return transform_suite_deep(tests)
    tree = ast.parse(tests)
    cases = []
    body = []
    allowed_stmt = (ast.Import, ast.ImportFrom, ast.Assign, ast.AugAssign, ast.Expr, ast.FunctionDef, ast.ClassDef, ast.For, ast.If, ast.With, ast.Try, ast.AnnAssign)
    for node in tree.body:
        if isinstance(node, ast.Assert):
            test = node.test
            if isinstance(test, ast.Compare) and len(test.ops) == 1 and isinstance(test.ops[0], ast.Eq):
                try:
                    expected = ast.literal_eval(test.comparators[0])
                    cases.append({"i": len(cases), "kind": "eq", "expected": expected, "src": ast.unparse(test.left)})
                    body.append("__report(%d, (%s))" % (cases[-1]["i"], ast.unparse(test.left)))
                    continue
                except (ValueError, SyntaxError):
                    raise Unsupported("assert with non-literal right-hand side: " + ast.unparse(test)[:80])
            if isinstance(test, ast.Compare):
                raise Unsupported("unsupported comparison operator: " + ast.unparse(test)[:80])
            if isinstance(test, ast.UnaryOp) and isinstance(test.op, ast.Not):
                cases.append({"i": len(cases), "kind": "falsy", "src": ast.unparse(test.operand)})
                body.append("__report(%d, (%s))" % (cases[-1]["i"], ast.unparse(test.operand)))
                continue
            cases.append({"i": len(cases), "kind": "truthy", "src": ast.unparse(test)})
            body.append("__report(%d, (%s))" % (cases[-1]["i"], ast.unparse(test)))
        elif isinstance(node, allowed_stmt):
            body.append(ast.unparse(node))
        else:
            raise Unsupported("unsupported statement in suite: " + type(node).__name__)
    return cases, "\n".join(body) + "\n"


def decode(enc):
    """Typed value from the child; only exact builtin types are accepted (A03: no repr, no custom __eq__/__repr__)."""
    t = enc.get("t")
    v = enc.get("v")
    if t == "bool": return bool(v)
    if t == "int": return int(v)
    if t == "float": return float(v)
    if t == "str": return str(v)
    if t == "none": return None
    if t == "list": return [decode(x) for x in v]
    if t == "tuple": return tuple(decode(x) for x in v)
    if t == "set": return frozenset(decode(x) for x in v)
    if t == "dict": return {decode(k) if isinstance(decode(k), (int, float, str, bool, tuple, type(None))) else str(decode(k)): decode(x) for k, x in v}
    raise ValueError("unknown type tag")


def _norm(v):
    """Compare sets/frozensets and tuples/lists the way Python equality does after decoding."""
    if isinstance(v, (set, frozenset)):
        return frozenset(_norm(x) for x in v)
    if isinstance(v, (list, tuple)):
        return type(v)(_norm(x) for x in v)
    if isinstance(v, dict):
        return {k: _norm(x) for k, x in v.items()}
    return v


def judge_function_task(row, scratch, env, extra, timeout):
    banned = [tok for tok in FUNCTION_TASK_FORBIDDEN if tok in str(row["solution"])]
    if banned:
        return {"passed": False, "cases": 0, "judge": "static", "stderr": "refused_forbidden_token:" + ",".join(banned)[:100]}
    try:
        cases, program = transform_suite(str(row["tests"]))
    except SyntaxError:
        return {"passed": False, "cases": 0, "judge": "supervisor", "stderr": "tests_unparseable"}
    except Unsupported as exc:
        return {"passed": False, "cases": 0, "judge": "supervisor", "stderr": "refused_unsupported_suite:" + str(exc)[:120]}
    if not cases:
        return {"passed": False, "cases": 0, "judge": "supervisor", "stderr": "refused_zero_case_suite"}
    src = os.path.join(scratch, "candidate.py")
    suite = os.path.join(scratch, "suite.py")
    runner = os.path.join(scratch, "runner.py")
    for path, text in ((src, str(row["solution"])), (suite, program), (runner, EVAL_RUNNER)):
        with open(path, "w", encoding="utf-8") as f:
            f.write(text)
        os.chmod(path, 0o644)
    t0 = time.monotonic()
    try:
        proc = subprocess.run([sys.executable, "-I", "-S", runner, src, suite], capture_output=True, timeout=timeout,
                              cwd=os.path.join(scratch, "tmp"), env=env, **extra)
    except subprocess.TimeoutExpired:
        return {"passed": False, "cases": len(cases), "judge": "supervisor", "stderr": "timeout", "elapsed_s": timeout}
    elapsed = round(time.monotonic() - t0, 3)
    lines = [ln for ln in proc.stdout[:MAX_OUTPUT].decode("utf-8", "replace").splitlines() if ln.strip()]
    records = []
    for ln in lines:
        try:
            rec = json.loads(ln)
        except ValueError:
            return {"passed": False, "cases": len(cases), "judge": "supervisor", "stderr": "protocol_violation:non_json_line", "elapsed_s": elapsed}
        if not isinstance(rec, dict) or "i" not in rec:
            return {"passed": False, "cases": len(cases), "judge": "supervisor", "stderr": "protocol_violation:shape", "elapsed_s": elapsed}
        records.append(rec)
    if proc.returncode != 0:
        return {"passed": False, "cases": len(cases), "judge": "supervisor", "stderr": "exit %s" % proc.returncode, "elapsed_s": elapsed}
    if any(r.get("i") == -1 for r in records):
        return {"passed": False, "cases": len(cases), "judge": "supervisor", "stderr": "candidate_failed_to_load", "elapsed_s": elapsed}
    if any(r.get("i") == -2 for r in records):
        return {"passed": False, "cases": len(cases), "judge": "supervisor", "stderr": "suite_raised", "elapsed_s": elapsed}
    if not records or records[-1].get("i") != -3 or records[-1].get("done") is not True:
        return {"passed": False, "cases": len(cases), "judge": "supervisor", "stderr": "protocol_violation:no_terminal_marker", "elapsed_s": elapsed}
    body = records[:-1]
    deep = str(row["tests"]).lstrip().startswith(DEEP_MARK)
    if deep:
        # exam suites report from inside loops: one assert -> many records. Every record must map to a known case, every
        # case must have reported at least once, and every report must pass. Expected values stay in the supervisor.
        valid = {c["i"]: c for c in cases}
        if not body or any(r.get("i") not in valid for r in body) or set(r["i"] for r in body) != set(valid):
            return {"passed": False, "cases": len(cases), "judge": "supervisor", "stderr": "protocol_violation:case_coverage", "elapsed_s": elapsed}
        pairs = [(valid[r["i"]], r) for r in body]
    else:
        if [r.get("i") for r in body] != [c["i"] for c in cases]:      # exactly one record per case, in suite order, nothing else
            return {"passed": False, "cases": len(cases), "judge": "supervisor", "stderr": "protocol_violation:case_sequence", "elapsed_s": elapsed}
        pairs = list(zip(cases, body))
    for c, rec in pairs:
        if "error" in rec:
            return {"passed": False, "cases": len(cases), "judge": "supervisor", "stderr": "case %d %s" % (c["i"], rec["error"])[:120], "elapsed_s": elapsed}
        try:
            got = decode(rec.get("val") or {})
        except (ValueError, TypeError, AttributeError):
            return {"passed": False, "cases": len(cases), "judge": "supervisor", "stderr": "case %d undecodable" % c["i"], "elapsed_s": elapsed}
        if c["kind"] == "eq":
            if _norm(got) != _norm(c["expected"]):
                return {"passed": False, "cases": len(cases), "judge": "supervisor", "stderr": "case %d mismatch" % c["i"], "elapsed_s": elapsed}
        elif c["kind"] == "truthy" and not bool(got):
            return {"passed": False, "cases": len(cases), "judge": "supervisor", "stderr": "case %d not truthy" % c["i"], "elapsed_s": elapsed}
        elif c["kind"] == "falsy" and bool(got):
            return {"passed": False, "cases": len(cases), "judge": "supervisor", "stderr": "case %d not falsy" % c["i"], "elapsed_s": elapsed}
    judge = "partial" if any(c.get("partial") for c in cases) else "supervisor"
    return {"passed": True, "cases": len(body) if deep else len(cases), "judge": judge, "stderr": "", "elapsed_s": elapsed}


def parse_stdio(tests: str):
    """Cases from the stdio suite: `assert __run(<input>).split() == <output>.split()` lines."""
    cases = []
    try:
        tree = ast.parse(tests)
    except SyntaxError:
        return None
    for node in tree.body:
        if isinstance(node, (ast.Import, ast.ImportFrom, ast.FunctionDef)):
            continue                     # the preamble (imports + the __run helper) is ours
        ok = False
        if isinstance(node, ast.Assert) and isinstance(node.test, ast.Compare) and len(node.test.ops) == 1 and isinstance(node.test.ops[0], ast.Eq):
            left, right = node.test.left, node.test.comparators[0]
            try:
                inp = ast.literal_eval(left.func.value.args[0])
                out = ast.literal_eval(right.func.value)
                if isinstance(inp, str) and isinstance(out, str):
                    cases.append({"i": len(cases), "input": inp, "expected": out.split()}); ok = True
            except Exception:  # noqa: BLE001
                ok = False
        if not ok:
            return None                  # unsupported statement -> the suite is refused, never silently thinned (A04)
    return cases


def judge_stdio_task(row, scratch, env, extra, timeout):
    cases = parse_stdio(str(row["tests"]))
    if cases is None:
        return {"passed": False, "cases": 0, "judge": "supervisor", "stderr": "refused_unsupported_suite:stdio"}
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
            try:
                row = json.loads(line)
            except Exception:  # noqa: BLE001
                report["seen"] += 1; report["malformed"] += 1
                continue
            if isinstance(row, dict) and "_fetch" in row:
                fout.write(line + "\n")           # loader provenance from the fetch step: passed through to the run summary
                continue
            report["seen"] += 1
            try:
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
                if reason.startswith(("refused_empty_suite", "refused_zero_case_suite", "refused_unsupported_suite")):
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
