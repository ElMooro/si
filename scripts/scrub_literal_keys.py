#!/usr/bin/env python3
"""scrub_literal_keys.py -- audit 2026-09-08 INST-06 fleet sweep.

Provider credentials were pasted as literals across the fleet (the audit found
5 sites; the tree held ~350 Lambda sources, 75 config.json env blocks and ~250
historical ops scripts). This tool makes every consumer read managed
configuration so the keys can finally be ROTATED without breaking engines.

The literals never live in this file. Pass them at run time:
    --literal fmp=<value> --literal polygon=<value> ...      (local)
    --from-ssm                                              (runner: reads /justhodl/<prov>/api-key)

Targets:
    lambdas   aws/lambdas/*/source/*.py  -> managed_secret((ENV,...), ("/justhodl/<prov>/api-key",))
              aws/lambdas/*/config.json  -> env entries whose VALUE is a literal are removed
                                            (deploy-lambdas merges config env over the live env, so
                                             the live function keeps its value; nothing is lost)
    ops       aws/ops/{ran,historical,audit,reports}/** -> literal replaced by REDACTED_<PROV>_KEY
              (aws/ops/pending is NEVER touched: run-ops executes any pending script in a push)
    shared    aws/shared/*.py -> same rewrite as lambdas

--batch N limits the number of Lambda directories rewritten in one run (deterministic
order, already-clean dirs skipped) so each push redeploys a bounded wave.
--dry-run reports without writing. Exit code 0; the JSON report is printed last.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SSM_NAME = {"fmp": "/justhodl/fmp/api-key", "polygon": "/justhodl/polygon/api-key",
            "fred": "/justhodl/fred/api-key", "cmc": "/justhodl/cmc/api-key",
            "telegram": "/justhodl/telegram/bot_token", "anthropic": "/justhodl/anthropic/api_key",
            "newsapi": "/justhodl/newsapi/api-key", "census": "/justhodl/census/api-key"}
# env names the fleet has used for each provider (first = canonical)
ENV_NAMES = {"fmp": ("FMP_KEY", "FMP_API_KEY"), "polygon": ("POLYGON_API_KEY", "POLYGON_KEY", "POLY_KEY"),
             "fred": ("FRED_API_KEY", "FRED_KEY"), "cmc": ("CMC_KEY", "COINMARKETCAP_API_KEY"),
             "telegram": ("TELEGRAM_BOT_TOKEN", "TELEGRAM_TOKEN"), "anthropic": ("ANTHROPIC_API_KEY", "ANTHROPIC_KEY"),
             "newsapi": ("NEWSAPI_KEY", "NEWS_API_KEY", "NEWS_KEY"), "census": ("CENSUS_API_KEY", "CENSUS_KEY")}
IMPORT_LINE = "from managed_secret import managed_secret  # audit 2026-09-08 INST-06: no literal credentials"


def load_literals(args) -> dict:
    lits = {}
    for item in args.literal or []:
        prov, _, val = item.partition("=")
        if prov in SSM_NAME and val:
            lits[val] = prov
    if args.from_ssm:
        import boto3
        ssm = boto3.client("ssm", region_name="us-east-1")
        for prov, name in SSM_NAME.items():
            try:
                lits[ssm.get_parameter(Name=name, WithDecryption=True)["Parameter"]["Value"]] = prov
            except Exception as e:
                print(f"[scrub] {name} not readable: {str(e)[:80]}", file=sys.stderr)
    return lits


def rewrite_python(text: str, lits: dict) -> tuple[str, int, list]:
    """Rewrite literal credential assignments; returns (new_text, n_rewrites, manual_leftovers)."""
    n = 0
    lines = text.split("\n")
    out = []
    for ln in lines:
        new = ln
        for val, prov in lits.items():
            if val not in ln:
                continue
            ssm = SSM_NAME[prov]
            # 1. VAR = os.environ.get("X", "<lit>")   /  os.environ.get('X', '<lit>')
            m = re.match(r'^(\s*)([A-Za-z_]\w*)\s*=\s*os\.environ\.get\(\s*["\']([A-Za-z_]\w*)["\']\s*,\s*["\']' + re.escape(val) + r'["\']\s*\)\s*(#.*)?$', ln)
            if m:
                ind, var, env = m.group(1), m.group(2), m.group(3)
                envs = tuple(dict.fromkeys((env,) + ENV_NAMES[prov]))
                new = f'{ind}{var} = managed_secret({envs!r}, ("{ssm}",))'
                break
            # 2. VAR = os.environ.get("A") or os.environ.get("B") or "<lit>"   (single line)
            m = re.match(r'^(\s*)([A-Za-z_]\w*)\s*=\s*\(?((?:os\.environ\.get\(\s*["\'][A-Za-z_]\w*["\'][^)]*\)\s*or\s*)+)["\']' + re.escape(val) + r'["\']\)?\s*(#.*)?$', ln)
            if m:
                ind, var = m.group(1), m.group(2)
                envs = tuple(dict.fromkeys(tuple(re.findall(r'os\.environ\.get\(\s*["\']([A-Za-z_]\w*)["\']', m.group(3))) + ENV_NAMES[prov]))
                new = f'{ind}{var} = managed_secret({envs!r}, ("{ssm}",))'
                break
            # 3. VAR = "<lit>"
            m = re.match(r'^(\s*)([A-Za-z_]\w*)\s*=\s*["\']' + re.escape(val) + r'["\']\s*(#.*)?$', ln)
            if m:
                ind, var = m.group(1), m.group(2)
                envs = tuple(dict.fromkeys((var,) + ENV_NAMES[prov]))
                new = f'{ind}{var} = managed_secret({envs!r}, ("{ssm}",))'
                break
            # 4. continuation line:   or "<lit>")   -> or managed_secret(...))
            m = re.match(r'^(\s*)or\s+["\']' + re.escape(val) + r'["\']\)?\s*(#.*)?$', ln)
            if m:
                closing = ")" if ln.rstrip().endswith(")") else ""
                new = f'{m.group(1)}or managed_secret({ENV_NAMES[prov]!r}, ("{ssm}",)){closing}'
                break
            # 5. any inline  os.environ.get("X", "<lit>")  expression (dict values, attributes, locals, aliased os)
            m5 = re.search(r'(?:_?os)\.environ\.get\(\s*["\']([A-Za-z_]\w*)["\']\s*,\s*["\']' + re.escape(val) + r'["\']\s*\)', ln)
            if m5:
                envs = tuple(dict.fromkeys((m5.group(1),) + ENV_NAMES[prov]))
                new = ln[:m5.start()] + f'managed_secret({envs!r}, ("{ssm}",))' + ln[m5.end():]
                break
            # 6. bare string literal continuation / return / attribute:   "<lit>")  |  return "<lit>"  |  self.x = '<lit>'
            m6 = re.search(r'["\']' + re.escape(val) + r'["\']', ln)
            if m6 and not re.search(r'[?&]api_key=', ln):
                new = ln[:m6.start()] + f'managed_secret({ENV_NAMES[prov]!r}, ("{ssm}",))' + ln[m6.end():]
                break
            # 7. key embedded in a URL string:  &api_key=<lit>&  -> &api_key={managed_secret(...)}&  (needs an f-string)
            m7 = re.search(r'api_key=' + re.escape(val), ln)
            if m7:
                repl = "api_key={managed_secret(" + repr(ENV_NAMES[prov]) + ", ('" + ssm + "',))}"
                cand = ln[:m7.start()] + repl + ln[m7.end():]
                # promote the enclosing plain string to an f-string when it is not one already
                q = cand.rfind('"', 0, m7.start()); q2 = cand.rfind("'", 0, m7.start()); qi = max(q, q2)
                if qi >= 0 and not (qi > 0 and cand[qi - 1] in "fF"):
                    cand = cand[:qi] + "f" + cand[qi:]
                new = cand
                break
        if new != ln:
            n += 1
        out.append(new)
    text2 = "\n".join(out)
    manual = [i + 1 for i, ln in enumerate(out) for val in lits if val in ln]
    if n and "from managed_secret import managed_secret" not in text2:
        text2 = insert_import(text2)
    return text2, n, manual


def insert_import(text2: str) -> str:
    """Insert IMPORT_LINE after the last TOP-LEVEL import statement that ends before the
    first managed_secret( use (AST-anchored, so multi-line imports and try-blocks are safe).
    Falls back to right after the module docstring / __future__ imports."""
    import ast
    ls = text2.split("\n")
    first_use = next((i for i, ln in enumerate(ls) if "managed_secret(" in ln), len(ls))
    anchor = None
    try:
        tree = ast.parse(text2)
        for node in tree.body:
            if isinstance(node, (ast.Import, ast.ImportFrom)) and node.end_lineno - 1 < first_use:
                if not (isinstance(node, ast.ImportFrom) and node.module == "__future__"):
                    anchor = node.end_lineno            # insert after this line (1-based end -> 0-based index)
        if anchor is None:
            anchor = 0
            for node in tree.body:
                if isinstance(node, ast.Expr) and isinstance(getattr(node, "value", None), ast.Constant) and isinstance(node.value.value, str):
                    anchor = node.end_lineno            # docstring
                elif isinstance(node, ast.ImportFrom) and node.module == "__future__":
                    anchor = node.end_lineno
                else:
                    break
    except SyntaxError:
        anchor = 0
    if anchor > first_use:
        anchor = 0
    ls.insert(anchor, IMPORT_LINE)
    return "\n".join(ls)


def scrub_config(path: Path, lits: dict, dry: bool) -> int:
    try:
        d = json.loads(path.read_text())
    except Exception:
        return 0
    total = 0
    for blk in ("environment", "env"):
        env = d.get(blk)
        if not isinstance(env, dict):
            continue
        hit = [k for k, v in env.items() if isinstance(v, str) and v in lits]
        for k in hit:
            env.pop(k)
        total += len(hit)
    if total and not dry:
        path.write_text(json.dumps(d, indent=2) + "\n")
    return total


def redact_text(path: Path, lits: dict, dry: bool) -> int:
    try:
        s = path.read_text()
    except Exception:
        return 0
    n = 0
    for val, prov in lits.items():
        c = s.count(val)
        if c:
            s = s.replace(val, f"REDACTED_{prov.upper()}_KEY")
            n += c
    if n and not dry:
        path.write_text(s)
    return n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--literal", action="append")
    ap.add_argument("--from-ssm", action="store_true")
    ap.add_argument("--targets", default="lambdas", help="comma list of lambdas,ops,shared")
    ap.add_argument("--batch", type=int, default=0, help="max Lambda dirs rewritten this run (0 = all)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    lits = load_literals(args)
    if not lits:
        print("no literals supplied", file=sys.stderr); sys.exit(2)
    targets = set(t.strip() for t in args.targets.split(","))
    rep = {"dry_run": args.dry_run, "lambdas": [], "lambdas_remaining": 0, "shared": [], "ops_files": 0, "ops_hits": 0, "manual": []}

    if "lambdas" in targets:
        dirs = sorted(p for p in (ROOT / "aws" / "lambdas").iterdir() if p.is_dir())
        done = 0
        for d in dirs:
            files = list((d / "source").glob("*.py")) if (d / "source").is_dir() else []
            cfg = d / "config.json"
            dirty = any(val in f.read_text(errors="ignore") for f in files for val in lits) or (cfg.exists() and any(val in cfg.read_text(errors="ignore") for val in lits))
            if not dirty:
                continue
            if args.batch and done >= args.batch:
                rep["lambdas_remaining"] += 1
                continue
            entry = {"lambda": d.name, "rewrites": 0, "config_env_removed": 0, "manual_lines": {}}
            for f in files:
                t2, n, manual = rewrite_python(f.read_text(), lits)
                if n and not args.dry_run:
                    f.write_text(t2)
                entry["rewrites"] += n
                if manual:
                    entry["manual_lines"][f.name] = manual
                    rep["manual"].append(f"{d.name}/source/{f.name}:{manual}")
            if cfg.exists():
                entry["config_env_removed"] = scrub_config(cfg, lits, args.dry_run)
            rep["lambdas"].append(entry)
            done += 1

    if "shared" in targets:
        for f in sorted((ROOT / "aws" / "shared").glob("*.py")):
            t2, n, manual = rewrite_python(f.read_text(), lits)
            if n:
                if not args.dry_run:
                    f.write_text(t2)
                rep["shared"].append({"file": f.name, "rewrites": n, "manual": manual})
            if manual:
                rep["manual"].append(f"shared/{f.name}:{manual}")

    if "ops" in targets:
        for sub in ("ran", "historical", "audit", "reports"):
            base = ROOT / "aws" / "ops" / sub
            if not base.exists():
                continue
            for f in base.rglob("*"):
                if f.is_file() and f.suffix in (".py", ".md", ".txt", ".json", ".log", ".sh"):
                    n = redact_text(f, lits, args.dry_run)
                    if n:
                        rep["ops_files"] += 1; rep["ops_hits"] += n

    rep["lambdas_rewritten"] = len(rep["lambdas"])
    rep["totals"] = {"rewrites": sum(e.get("rewrites", 0) for e in rep["lambdas"]), "config_env_removed": sum(e.get("config_env_removed", 0) for e in rep["lambdas"])}
    rep["lambda_names"] = [e["lambda"] for e in rep["lambdas"]]
    rep["lambdas"] = rep["lambdas"][:40]   # bounded detail; totals/names above are complete
    print(json.dumps(rep, indent=1))


if __name__ == "__main__":
    main()
