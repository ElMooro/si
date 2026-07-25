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


def confirmed_write_keys(code):
    """Real data/*.json (and sibling-prefix) keys this engine WRITES —
    windowed around actual put_object(/.put_json( call sites, so a read
    (get_object) of another engine's output is never misattributed."""
    varname_def = {}
    for pat in (VAR_DIRECT, VAR_TUPLE, VAR_ENVGET):
        for m in pat.finditer(code):
            varname_def[m.group(1)] = m.group(2)
    keys = set()
    for m in re.finditer(r'put_object\(|\.put_json\(', code):
        window = code[m.end(): m.end() + 300]
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
        keys = confirmed_write_keys(code)
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
        engines.append({"engine": d.name, "keys": keys, "description": desc})
    doc = {"generated_at": datetime.now(timezone.utc).isoformat(),
           "source": "scripts/gen_engine_manifest.py (repo grep, per ops run)",
           "n_engines": len(engines), "engines": engines}
    out = ROOT / "engine-manifest.json"
    out.write_text(json.dumps(doc, separators=(",", ":")))
    n_with_keys = sum(1 for e in engines if e["keys"])
    print(f"[manifest] {len(engines)} engines -> {out} "
          f"({n_with_keys} with keys found, {100*n_with_keys/max(len(engines),1):.1f}%)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
