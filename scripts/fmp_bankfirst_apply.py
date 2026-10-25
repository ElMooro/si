#!/usr/bin/env python3
"""Apply the bank-first FMP shim to engines: insert one guarded install block after the module docstring and any
`from __future__` imports (so it precedes `from urllib.request import urlopen` bindings).  Idempotent; prints the engines
changed.  Usage: python3 scripts/fmp_bankfirst_apply.py <engine> [<engine> ...]  (or --list-file FILE)"""
import ast
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BLOCK = '''try:  # bank-first FMP (aws/shared/fmp_bankfirst): /stable answered from the warehouse banks when fresh, else live
    from fmp_bankfirst import install as _fmp_bankfirst_install
    _fmp_bankfirst_install()
except Exception:  # noqa: BLE001 - the shim never blocks the engine
    pass
'''
MARK = "from fmp_bankfirst import install"


def apply(engine):
    p = ROOT / "aws" / "lambdas" / engine / "source" / "lambda_function.py"
    src = p.read_text(encoding="utf-8")
    if MARK in src:
        return False
    tree = ast.parse(src)
    insert_line = 0
    body = tree.body
    i = 0
    if body and isinstance(body[0], ast.Expr) and isinstance(getattr(body[0], "value", None), ast.Constant) and isinstance(body[0].value.value, str):
        insert_line = body[0].end_lineno
        i = 1
    while i < len(body) and isinstance(body[i], ast.ImportFrom) and body[i].module == "__future__":
        insert_line = body[i].end_lineno
        i += 1
    lines = src.splitlines(keepends=True)
    new = lines[:insert_line] + ["\n" if insert_line and not lines[insert_line - 1].endswith("\n\n") else ""] + [BLOCK] + lines[insert_line:]
    out = "".join(new)
    ast.parse(out)
    p.write_text(out, encoding="utf-8")
    return True


def main():
    args = sys.argv[1:]
    if args and args[0] == "--list-file":
        args = json.loads(Path(args[1]).read_text())
    changed = [e for e in args if apply(e)]
    print(json.dumps({"changed": changed, "already": [e for e in args if e not in changed]}))


if __name__ == "__main__":
    main()
