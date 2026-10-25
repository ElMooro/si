#!/usr/bin/env python3
"""Apply the bank-first FMP shim to engines: insert one guarded install block after the module docstring and any
`from __future__` imports (so it precedes `from urllib.request import urlopen` bindings).  Idempotent; prints the engines
changed.  Usage: python3 scripts/fmp_bankfirst_apply.py [--append] <engine> [<engine> ...]  (or --list-file FILE)

--append puts the block at the END of the module instead: for engines whose tests pin the complete predecessor bytes
as a prefix (the repo's append-below-the-predecessor convention).  Only valid when the module calls
`urllib.request.urlopen(...)` by attribute (a `from urllib.request import urlopen` name would already be bound).
--polygon installs the Polygon shim (aws/shared/polygon_bankfirst: /v2/aggs daily bars from the per-ticker bank) instead
of the FMP one; an engine that calls both providers carries both blocks (each shim passes the other's host through)."""
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
POLY_BLOCK = '''try:  # bank-first Polygon (aws/shared/polygon_bankfirst): /v2/aggs daily bars answered from the warehouse when complete, else live
    from polygon_bankfirst import install as _polygon_bankfirst_install
    _polygon_bankfirst_install()
except Exception:  # noqa: BLE001 - the shim never blocks the engine
    pass
'''
POLY_MARK = "from polygon_bankfirst import install"
POLY_NEVER = {"justhodl-polygon-full", "justhodl-polygon-eod", "justhodl-polygon-bank", "justhodl-tv-bars", "justhodl-provider-catalog", "justhodl-provider-coverage"}
# The banks and the provider probes must always hit the live provider: a bank answering its own requests from itself
# would freeze the warehouse at whatever it last held.  Mirrors fmp_bankfirst.NEVER_INSTALL_IN.
NEVER = {"justhodl-fmp-bank", "justhodl-fmp-walker", "justhodl-fmp-ratios", "justhodl-provider-catalog", "justhodl-provider-coverage", "justhodl-provider-bank", "justhodl-polygon-bank", "justhodl-eia-bank"}


def apply(engine, append=False, polygon=False):
    never, mark, block = (POLY_NEVER, POLY_MARK, POLY_BLOCK) if polygon else (NEVER, MARK, BLOCK)
    if engine in never:
        raise SystemExit("refusing to install the bank-first shim in %s: banks and provider probes stay live-only" % engine)
    p = ROOT / "aws" / "lambdas" / engine / "source" / "lambda_function.py"
    src = p.read_text(encoding="utf-8")
    if mark in src:
        return False
    if append:
        import re
        if re.search(r"^\s*from urllib\.request import .*\burlopen\b", src, re.M):
            raise SystemExit("%s binds urlopen by name at import; an appended install would not intercept it" % engine)
        out = src + ("" if src.endswith("\n") else "\n") + "\n\n" + block
        ast.parse(out)
        p.write_text(out, encoding="utf-8")
        return True
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
    new = lines[:insert_line] + ["\n" if insert_line and not lines[insert_line - 1].endswith("\n\n") else ""] + [block] + lines[insert_line:]
    out = "".join(new)
    ast.parse(out)
    p.write_text(out, encoding="utf-8")
    return True


def main():
    args = sys.argv[1:]
    append = "--append" in args
    polygon = "--polygon" in args
    args = [a for a in args if a not in ("--append", "--polygon")]
    if args and args[0] == "--list-file":
        args = json.loads(Path(args[1]).read_text())
    changed = [e for e in args if apply(e, append, polygon)]
    print(json.dumps({"changed": changed, "already": [e for e in args if e not in changed]}))


if __name__ == "__main__":
    main()
