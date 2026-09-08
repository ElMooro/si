#!/usr/bin/env python3
"""Regenerate data/engine-manifest.json from the repo (ops 3436, extended 3886).
Engine -> written data/ keys (put_object/OUT_KEY greps) + description from
config.json. Runs on every ops run via run-ops.yml, so ask-desk and
strategist always reason over a fresh fleet map.

2026-07-25 extension (ops 3886): the original two patterns only caught a
literal inline `Key="data/....json"` at the call site, or a constant named
EXACTLY `OUT_KEY`. Measured coverage was 50.3% of the fleet (366/727) before
this fix — investigating a semi-sector flow/price divergence needed
justhodl-rebalance-radar and justhodl-earnings-tracker specifically, and
BOTH were invisible here (rebalance-radar defines `BUCKET, OUT = "...",
"data/rebalance-radar.json"` — a tuple-unpack to a variable simply not named
OUT_KEY). Added: tuple-unpack assignment, `VAR = os.environ.get("ENV",
"data/....json")` (a very common configurable-with-fallback idiom), and
broadened the prefix set beyond `data/` alone to the fleet's other real
top-level prefixes (etf-flows/, screener/, sentiment/, macro/, config/).
Also fixed a latent false-positive: the original WRITE_PAT matched a bare
`Key="data/....json"` ANYWHERE in a file, including inside get_object (READ)
calls — catalyst-calendar's read of earnings-tracker.json was getting
misattributed as a WRITE. Now every match must fall inside a windowed
search around an actual put_object(/.put_json( call site. Measured result:
79.1% coverage (575/727), 233 engines newly discoverable — verified against
the exact files that motivated this fix plus a fleet-wide before/after count.

KNOWN RESIDUAL GAP (documented, not fixed here): engines that write via a
generically-named wrapper function taking the key as a parameter — e.g.
justhodl-etf-fund-flows' `_write_json(key, obj)` called as
`_write_json("etf-flows/daily.json", ...)` — are still invisible, since the
literal never appears near the text `put_object(` at all. Closing that
gap needs real call-graph tracing (which call sites pass which literal to
which wrapper), not a regex extension, and is out of scope for this pass.
"""
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LAMBDAS = ROOT / "aws" / "lambdas"

PREFIXES = r'(?:data|etf-flows|screener|sentiment|macro|config)'
KEY_LIT = re.compile(rf'["\']({PREFIXES}/[a-zA-Z0-9_\-/{{}}\.]+?\.json)["\']')
VAR_DIRECT = re.compile(rf'^\s*([A-Za-z_]\w*)\s*=\s*["\']({PREFIXES}/[a-zA-Z0-9_\-/\.]+?\.json)["\']', re.M)
VAR_TUPLE = re.compile(
    rf'^\s*\w+\s*,\s*([A-Za-z_]\w*)\s*=\s*["\'][^"\']*["\']\s*,\s*["\']({PREFIXES}/[a-zA-Z0-9_\-/\.]+?\.json)["\']', re.M)
VAR_ENVGET = re.compile(
    rf'^\s*([A-Za-z_]\w*)\s*=\s*os\.environ\.get\([^,]+,\s*["\']({PREFIXES}/[a-zA-Z0-9_\-/\.]+?\.json)["\']', re.M)


# ── audit 2026-09-08 (section C-5): AST-based ownership ───────────────────────────────────────
# The 300-character window after a put call listed any key literal nearby as an output (a read of
# another engine's feed right after a write became an "output"), truncated at 16 keys, and could not
# see constant-prefix f-strings (justhodl-etf-fund-flows: OUTPUT_PREFIX = "etf-flows/" + f"{OUTPUT_PREFIX}daily.json"
# scanned as keys:[]). This resolver binds the ACTUAL Key= argument of each write call (and of locally
# defined wrappers around one) and resolves constants, f-strings, concatenations and formats. Dynamic
# segments become a "*" so the key FAMILY is still declared. Reads (get_object) are recorded separately.
import ast

WRITE_ATTRS = {"put_object", "put_json", "upload_fileobj", "upload_file", "write_json", "save_json", "put"}
READ_ATTRS = {"get_object", "download_fileobj", "get_json", "read_json", "head_object"}
# any top-level bucket folder (data/, etf-flows/, air/, portfolio/, calibration/, risk/, ...) -- the old allowlist hid air/hkia-cargo-levels.json
KEY_RE = re.compile(r'^[a-z0-9][a-z0-9_\-]*/')


class _Resolver:
    def __init__(self, tree):
        self.consts = {}
        for node in tree.body:
            if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
                v = self.resolve(node.value)
                if v is not None:
                    self.consts[node.targets[0].id] = v
            elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name) and node.value is not None:
                v = self.resolve(node.value)
                if v is not None:
                    self.consts[node.target.id] = v

    def resolve(self, node):
        """Return a string (with '*' for unresolvable segments) or None."""
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        if isinstance(node, ast.Name):
            return self.consts.get(node.id, "*")
        if isinstance(node, ast.JoinedStr):
            out = []
            for v in node.values:
                if isinstance(v, ast.Constant):
                    out.append(str(v.value))
                elif isinstance(v, ast.FormattedValue):
                    r = self.resolve(v.value) if isinstance(v.value, (ast.Name, ast.Constant, ast.Attribute)) else None
                    out.append(r if (r is not None and r != "*") else "*")
                else:
                    out.append("*")
            return "".join(out)
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            l, r = self.resolve(node.left), self.resolve(node.right)
            if l is None or r is None:
                return None
            return l + r
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Mod):
            l = self.resolve(node.left)
            return re.sub(r'%(?:\([^)]*\))?[-+ 0#]*\d*(?:\.\d+)?[sdifrxX%]', '*', l) if l else None   # %s %d %.2f %(name)s -> *
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "format":
            base = self.resolve(node.func.value)
            return re.sub(r'\\{[^}]*\\}', '*', base) if base else None
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in ("str",) and node.args:
            return "*"
        if isinstance(node, ast.Attribute):
            return "*"
        if isinstance(node, ast.Subscript):
            return "*"
        return None


def _normalise_key(k):
    if not k or not isinstance(k, str):
        return None
    k = re.sub(r'\*+', '*', k.strip())
    if not KEY_RE.match(k):
        return None
    if not (k.endswith(".json") or k.endswith(".json.gz") or k.endswith("*")):
        return None
    return k


def _key_from_call(call, res, wrapper_key_pos=None):
    """The Key of a write/read call: Key= keyword, else a positional arg (wrappers) that resolves to a key."""
    for kw in call.keywords:
        if kw.arg == "Key":
            return _normalise_key(res.resolve(kw.value))
    if wrapper_key_pos is not None and len(call.args) > wrapper_key_pos:
        return _normalise_key(res.resolve(call.args[wrapper_key_pos]))
    for a in call.args:
        k = _normalise_key(res.resolve(a))
        if k:
            return k
    return None


def ast_keys(code):
    """(writes, reads, ok) from the AST. ok=False when the file does not parse (caller falls back to regex)."""
    try:
        tree = ast.parse(code)
    except SyntaxError:
        return [], [], False
    res = _Resolver(tree)
    # wrappers: locally defined functions whose body contains a write attribute call; the key is the first
    # parameter that is passed as Key= (or the first positional) inside the body
    wrappers = {}
    for node in tree.body:
        if isinstance(node, ast.FunctionDef):
            pos = None
            for sub in ast.walk(node):
                if isinstance(sub, ast.Call) and isinstance(sub.func, ast.Attribute) and sub.func.attr in WRITE_ATTRS:
                    params = [a.arg for a in node.args.args]
                    for kw in sub.keywords:
                        if kw.arg == "Key" and isinstance(kw.value, ast.Name) and kw.value.id in params:
                            pos = params.index(kw.value.id)
                    if pos is None and params:
                        pos = 0
                    break
            if pos is not None:
                wrappers[node.name] = pos
    writes, reads = set(), set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if isinstance(node.func, ast.Attribute) and node.func.attr in WRITE_ATTRS:
            k = _key_from_call(node, res)
            if k:
                writes.add(k)
        elif isinstance(node.func, ast.Attribute) and node.func.attr in READ_ATTRS:
            k = _key_from_call(node, res)
            if k:
                reads.add(k)
        elif isinstance(node.func, ast.Name) and node.func.id in wrappers:
            k = _key_from_call(node, res, wrappers[node.func.id])
            if k:
                writes.add(k)
    return sorted(writes), sorted(reads), True


def confirmed_write_keys(code):
    """Real data/*.json (and sibling-prefix) keys this engine WRITES —
    windowed around actual put_object(/.put_json( call sites, so a read
    (get_object) of another engine's output is never misattributed."""
    varname_def = {}
    for pat in (VAR_DIRECT, VAR_TUPLE, VAR_ENVGET):
        for m in pat.finditer(code):
            varname_def[m.group(1)] = m.group(2)
    keys = set()
    # ops 5200: close the documented residual gap — a locally defined wrapper whose body
    # calls put_object (def _put_json(key, obj) / def save(key, data) / def publish(...))
    # is a write site too; every call of that wrapper gets the same key window.
    wrappers = []
    for dm in re.finditer(r'^\s*def\s+([A-Za-z_]\w*)\s*\(', code, re.M):
        body = code[dm.end(): dm.end() + 900]
        nxt = re.search(r'^\s*def\s+', body[1:], re.M)
        if nxt:
            body = body[: nxt.start() + 1]
        if 'put_object(' in body or '.put_json(' in body:
            wrappers.append(dm.group(1))
    sites = [m.end() for m in re.finditer(r'put_object\(|\.put_json\(', code)]
    for w in wrappers:
        for m in re.finditer(rf'(?<![\w.])(?<!def ){re.escape(w)}\(', code):
            sites.append(m.end())
    for end in sites:
        window = code[end: end + 300]
        for lit in KEY_LIT.findall(window):
            if "{" not in lit:
                keys.add(lit)
        for varname, key in varname_def.items():
            if re.search(rf'\b{re.escape(varname)}\b', window):
                keys.add(key)
    return sorted(keys)[:16]


def main():
    engines = []
    for d in sorted(LAMBDAS.iterdir()):
        src = d / "source" / "lambda_function.py"
        if not src.exists():
            continue
        try:
            code = src.read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue
        writes, reads, ok = ast_keys(code)
        # multi-file engines: sibling modules under source/ can own writes too
        for extra in sorted((d / "source").glob("*.py")):
            if extra.name == "lambda_function.py" or extra.name.startswith("_"):
                continue
            try:
                w2, r2, ok2 = ast_keys(extra.read_text(encoding="utf-8", errors="replace"))
                writes = sorted(set(writes) | set(w2)); reads = sorted(set(reads) | set(r2))
            except Exception:
                pass
        legacy = confirmed_write_keys(code)
        if ok:
            keys = writes
            method = "ast"
        else:
            keys = legacy
            method = "regex-window(fallback: parse error)"
        keys = keys[:64]
        desc = ""
        cfg = d / "config.json"
        if cfg.exists():
            try:
                desc = (json.loads(cfg.read_text()).get("description") or "")[:140]
            except Exception:
                pass
        if not desc:
            head = code.split('"""', 2)
            desc = (head[1].strip().splitlines()[0][:140] if len(head) > 2 else "")
        engines.append({"engine": d.name, "keys": keys, "n_keys": len(keys), "reads": [r for r in reads if r not in keys][:64],
                        "key_patterns": [k for k in keys if k.endswith("*")], "method": method,
                        "legacy_regex_keys": [k for k in legacy if k not in keys][:16], "description": desc})
    doc = {"generated_at": datetime.now(timezone.utc).isoformat(),
           "source": "scripts/gen_engine_manifest.py (AST Key= binding per write call + wrappers, audit 2026-09-08 C-5; regex window only as parse-failure fallback)",
           "n_engines": len(engines), "engines": engines}
    out = ROOT / "engine-manifest.json"
    out.write_text(json.dumps(doc, separators=(",", ":")))
    n_with_keys = sum(1 for e in engines if e["keys"])
    print(f"[manifest] {len(engines)} engines -> {out} "
          f"({n_with_keys} with keys found, {100*n_with_keys/max(len(engines),1):.1f}%)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
