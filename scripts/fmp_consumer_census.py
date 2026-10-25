#!/usr/bin/env python3
"""Census of the engines that call FMP directly (the consumer-conversion lane's worklist).

Khalid 2026-10-25: "100% of the data pulled and used by my system".  The banks (justhodl-fmp-bank, justhodl-fmp-walker)
hold the data; aws/shared/fmp_bank.py serves it to anything on fmp_stable.fmp_get / equity_enrich.fmp.  Only 6 engines
use those clients; this script finds the rest and groups them by call shape so the conversion can be staged per family
(each family = one patch pattern + one test), never as a blind fleet rewrite.

Output: docs/audit/<date>/fmp-consumer-census.json + .md (read-only on the repo).
    python3 scripts/fmp_consumer_census.py [--out docs/audit/2026-10-25]
"""
from __future__ import annotations

import argparse
import ast
import json
import re
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LAMBDAS = ROOT / "aws" / "lambdas"
HOST_RE = re.compile(r"financialmodelingprep\.com/(stable|api/v3|api/v4)/([A-Za-z0-9_\-/{}]+)")
SHARED_CLIENTS = {"fmp_stable", "equity_enrich", "fmp_book", "fmp_bank", "fmp_analyst", "fmp_etf"}


def endpoints_in(text):
    out = Counter()
    for m in HOST_RE.finditer(text):
        ep = m.group(2).split("?")[0].strip("/")
        ep = re.sub(r"\{[^}]*\}.*$", "{..}", ep)                 # f-string tails
        out[(m.group(1), ep)] += 1
    return out


def http_shape(tree):
    """Which HTTP mechanism builds the FMP request: urllib / requests / a helper name."""
    names = Counter()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            f = node.func
            n = None
            if isinstance(f, ast.Attribute):
                base = f.value.id if isinstance(f.value, ast.Name) else (f.value.attr if isinstance(f.value, ast.Attribute) else None)
                n = "%s.%s" % (base, f.attr) if base else f.attr
            elif isinstance(f, ast.Name):
                n = f.id
            if n and any(k in n for k in ("urlopen", "requests.get", "requests.Session", "http_get", "fetch", "_get", "fmp", "get_json", "http_json", "_http")):
                names[n] += 1
    return names


def helper_functions(tree, src):
    """Functions whose body builds an FMP URL - the natural seam for a bank-first shim."""
    helpers = []
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            seg = ast.get_source_segment(src, node) or ""
            if "financialmodelingprep.com" in seg:
                args = [a.arg for a in node.args.args]
                helpers.append({"name": node.name, "args": args, "lineno": node.lineno, "endpoints": len(endpoints_in(seg))})
    return helpers


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="docs/audit/%s" % date.today().isoformat())
    a = ap.parse_args()
    out_dir = ROOT / a.out
    out_dir.mkdir(parents=True, exist_ok=True)
    engines = {}
    families = defaultdict(list)
    ep_total = Counter()
    for d in sorted(LAMBDAS.iterdir()):
        src_dir = d / "source"
        if d.name.startswith("_") or not src_dir.is_dir():
            continue
        files = list(src_dir.rglob("*.py"))
        text = "\n".join(f.read_text(encoding="utf-8", errors="replace") for f in files)
        if "financialmodelingprep.com" not in text:
            continue
        imports = set()
        helpers = []
        shapes = Counter()
        legacy = Counter()
        for f in files:
            s = f.read_text(encoding="utf-8", errors="replace")
            try:
                tree = ast.parse(s)
            except SyntaxError:
                continue
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    imports.update(n.name.split(".")[0] for n in node.names)
                elif isinstance(node, ast.ImportFrom) and node.module:
                    imports.add(node.module.split(".")[0])
            helpers += [dict(h, file=f.name) for h in helper_functions(tree, s)]
            shapes.update(http_shape(tree))
        eps = endpoints_in(text)
        ep_total.update(eps)
        legacy = {k: v for k, v in eps.items() if k[0] != "stable"}
        uses_shared = sorted(imports & SHARED_CLIENTS)
        has_tests = (d / "tests").is_dir()
        if uses_shared:
            family = "shared-client"
        elif len(helpers) == 1:
            family = "one-helper"
        elif len(helpers) > 1:
            family = "many-helpers"
        else:
            family = "inline-urls"
        engines[d.name] = {"family": family, "endpoints": {"%s/%s" % k: v for k, v in sorted(eps.items())}, "n_endpoints": len(eps), "legacy_api_v3_v4": {"%s/%s" % k: v for k, v in legacy.items()},
                           "helpers": helpers, "http_shapes": dict(shapes.most_common(5)), "uses_shared_client": uses_shared, "has_tests": has_tests,
                           "config_env_keys": sorted((json.loads((d / "config.json").read_text()).get("env") or {}).keys()) if (d / "config.json").exists() else []}
        families[family].append(d.name)
    summary = {"generated": date.today().isoformat(), "engines": len(engines), "families": {k: len(v) for k, v in families.items()},
               "top_endpoints": [{"endpoint": "%s/%s" % k, "engines": v} for k, v in ep_total.most_common(60)],
               "legacy_callers": sorted(n for n, e in engines.items() if e["legacy_api_v3_v4"]),
               "one_helper_engines": sorted(families.get("one-helper", [])), "inline_url_engines": sorted(families.get("inline-urls", [])),
               "plan": [
                   "shared-client (done): fmp_stable / equity_enrich -> fmp_book.try_harvest -> fmp_bank (bulk bank, then walker bank) before any live call.",
                   "one-helper: the engine has exactly one function that builds FMP URLs - wrap it: try fmp_bank.try_bank(path, qs) first, fall through to the existing code. One patch pattern, one test per engine, deploy in batches of ~20 behind the deploy gate.",
                   "many-helpers: same wrap per helper; review each engine's helpers (list above) before patching.",
                   "inline-urls: URLs built inline at call sites - route through a new module-level helper first, then the wrap. Review per engine.",
                   "legacy api/v3-v4 callers: switch to the /stable path while wrapping (legacy answers 403 since 2025-08-31).",
               ]}
    (out_dir / "fmp-consumer-census.json").write_text(json.dumps({"summary": summary, "engines": engines}, indent=1, sort_keys=True), encoding="utf-8")
    lines = ["# FMP consumer census — %s" % summary["generated"], "",
             "%d engines call FMP directly. Families: %s." % (len(engines), ", ".join("%s %d" % (k, v) for k, v in sorted(summary["families"].items()))), "",
             "## Conversion plan", ""] + ["%d. %s" % (i + 1, p) for i, p in enumerate(summary["plan"])] + ["", "## Top endpoints (engines calling)", "", "| endpoint | engines |", "|---|---|"]
    lines += ["| `%s` | %d |" % (x["endpoint"], x["engines"]) for x in summary["top_endpoints"]]
    lines += ["", "## Engines by family", ""]
    for fam in ("shared-client", "one-helper", "many-helpers", "inline-urls"):
        lines += ["### %s (%d)" % (fam, len(families.get(fam, []))), ""]
        for n in sorted(families.get(fam, [])):
            e = engines[n]
            lines.append("- `%s` — %d endpoints%s%s; helpers: %s" % (n, e["n_endpoints"], "; LEGACY api/v3-v4: %s" % ", ".join(e["legacy_api_v3_v4"]) if e["legacy_api_v3_v4"] else "", "" if e["has_tests"] else "; no tests dir", ", ".join("%s(%s)" % (h["name"], ",".join(h["args"])) for h in e["helpers"]) or "-"))
        lines.append("")
    (out_dir / "fmp-consumer-census.md").write_text("\n".join(lines), encoding="utf-8")
    print(json.dumps({"engines": len(engines), "families": summary["families"], "legacy_callers": len(summary["legacy_callers"]), "out": str(out_dir)}))


if __name__ == "__main__":
    main()
