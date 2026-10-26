"""Bank-fusion SDK contract gate (2026-10-26, wave 2): every engine that imports the bank SDKs (bank_enrich, fmp_bank,
polygon_bankfirst) may only call attributes the real modules have, with keyword arguments their real signatures accept.

Why: the per-engine offline tests stub the SDK with a FakeBook/FakeBank, so a call to a method that does not exist - or a
keyword the real function does not take - passes the engine's own tests and fails in the Lambda (caught by a bare
`except Exception` it becomes a silent empty universe).  The pead-detector / starmine fakes also returned ROWS from
`prescreen()` where the real one returns SYMBOLS; that shape mismatch is why `prescreen_rows()` exists - a fake must
mirror the real return shape (see the FakeBook pattern in aws/lambdas/justhodl-insider-radar/tests/run_tests.py).
"""
import ast
import importlib
import inspect
import os
import sys
import types

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "aws", "shared"))
SDKS = ("bank_enrich", "fmp_bank", "polygon_bankfirst")


def _real(name):
    import boto3
    saved = boto3.client
    boto3.client = lambda *a, **k: types.SimpleNamespace()
    try:
        return importlib.import_module(name)
    finally:
        boto3.client = saved


def _aliases(tree):
    """{alias: sdk} for `import sdk as alias` / `import sdk` / `from sdk import name` (name -> 'sdk.name')."""
    out, direct = {}, {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for a in node.names:
                if a.name in SDKS:
                    out[a.asname or a.name] = a.name
        elif isinstance(node, ast.ImportFrom) and node.module in SDKS:
            for a in node.names:
                direct[a.asname or a.name] = (node.module, a.name)
    return out, direct


def _check_call(mod, attr, node, problems, where):
    fn = getattr(mod, attr, None)
    if fn is None:
        problems.append("%s: %s.%s does not exist in the real module" % (where, mod.__name__, attr))
        return
    if not callable(fn):
        return
    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):
        return
    params = sig.parameters
    accepts_kwargs = any(p.kind == p.VAR_KEYWORD for p in params.values())
    for kw in node.keywords:
        if kw.arg is not None and not accepts_kwargs and kw.arg not in params:
            problems.append("%s: %s.%s() has no keyword '%s' (real signature %s)" % (where, mod.__name__, attr, kw.arg, sig))
    positional = [p for p in params.values() if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)]
    accepts_varargs = any(p.kind == p.VAR_POSITIONAL for p in params.values())
    if not accepts_varargs and len(node.args) > len(positional):
        problems.append("%s: %s.%s() takes %d positional argument(s), called with %d" % (where, mod.__name__, attr, len(positional), len(node.args)))


def test_engines_call_only_real_bank_sdk_attributes_with_real_keywords():
    mods = {name: _real(name) for name in SDKS}
    problems, checked = [], 0
    base = os.path.join(ROOT, "aws", "lambdas")
    for fn in sorted(os.listdir(base)):
        path = os.path.join(base, fn, "source", "lambda_function.py")
        if not os.path.isfile(path):
            continue
        try:
            src = open(path, encoding="utf-8", errors="replace").read()
        except OSError:
            continue
        if not any(("import %s" % s) in src for s in SDKS):
            continue
        try:
            tree = ast.parse(src)
        except SyntaxError as e:
            problems.append("%s: does not parse (%s)" % (fn, e))
            continue
        aliases, direct = _aliases(tree)
        if not aliases and not direct:
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            f = node.func
            if isinstance(f, ast.Attribute) and isinstance(f.value, ast.Name) and f.value.id in aliases:
                _check_call(mods[aliases[f.value.id]], f.attr, node, problems, "%s:%d" % (fn, node.lineno))
                checked += 1
            elif isinstance(f, ast.Name) and f.id in direct:
                sdk, name = direct[f.id]
                _check_call(mods[sdk], name, node, problems, "%s:%d" % (fn, node.lineno))
                checked += 1
    assert checked > 50, "expected to check the fused engines' SDK calls, checked %d" % checked
    assert not problems, "bank SDK contract violations:\n  " + "\n  ".join(problems)


if __name__ == "__main__":
    test_engines_call_only_real_bank_sdk_attributes_with_real_keywords()
    print("ok bank_sdk_contracts")
