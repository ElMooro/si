"""
ops 1021 — FULL SYSTEM AUDIT + SYSTEM_CATALOG.md generator.

Produces two artifacts:
  1. aws/ops/reports/1021.json    — full machine-readable snapshot
  2. SYSTEM_CATALOG.md (repo root) — human/Claude-readable summary

WHY: Khalid wants a single source-of-truth document so future sessions don't
rebuild things that already exist (lesson from this session — 4 of 4 Phase D
candidates pre-existed). The catalog also serves as a continuous-audit
artifact: bugs surface in 1021.json scorecards; doctrine sits in the .md.

WHAT IT CATALOGS:

  A. Lambda inventory (paginated list_functions)
     - Family grouping (signals / risk / sentiment / data / api / ai / pro_pack)
     - Per-Lambda: name, runtime, memory, timeout, last_modified, code_size,
       env_keys, state
     - Static code scan (parse source code on disk):
        * S3 output keys produced (regex data/*.json)
        * FMP endpoints (flag deprecated /api/v3 + /api/v4)
        * Polygon endpoints (flag known-dead /stocks/v1/short-interest)
        * FRED series IDs (catalog)
        * Hardcoded API keys (security flag)

  B. EventBridge schedule registry
     - Both EventBridge Rules (legacy) and EventBridge Scheduler (new)
     - Per-schedule: name, expression, state, target Lambda
     - Flag: Lambdas with no schedule + orphaned schedules

  C. S3 inventory (list_objects_v2 prefix=data/)
     - Per-key: size, last_modified, age_hours
     - Stale flag: age > 36h (configurable per family)

  D. Page registry (parse all *.html in repo root)
     - Per-page: title, S3 keys consumed (regex S3 + data/*.json),
       nav crumbs out (to other pages)
     - Cross-reference: which page reads which S3 key

  E. Cross-graph (the master map)
     - Lambda → S3 key → consuming pages
     - Orphan Lambdas (output not consumed by any page)
     - Dangling pages (consume S3 keys nothing produces)
     - Stale consumers (page reads from stale S3 key)

  F. Bug scan
     - FMP /api/v3+v4 references (dead since 2025-08-31)
     - Polygon /stocks/v1/short-interest references (data dead post-2018)
     - Stale model strings (anything not claude-haiku-4-5-20251001)
     - Hardcoded API keys outside KHALID_KEYS allowlist
"""
import json
import os
import re
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

REPO_ROOT = Path(__file__).resolve().parents[3]
REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

cfg = Config(read_timeout=60, connect_timeout=10, retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION)
sch = boto3.client("scheduler", region_name=REGION)
events = boto3.client("events", region_name=REGION)

# ---------- Family classifier ----------
def classify_family(name):
    n = (name or "").lower().replace("justhodl-", "")
    if any(k in n for k in ("ai-chat", "ai-brief", "ai-router",
                              "morning-intelligence", "alpha-")):
        return "ai_layer"
    if any(k in n for k in ("api", "auth", "rate-limit")):
        return "api_public"
    if any(k in n for k in ("risk", "hedge", "vol-", "tail", "cro",
                              "crisis", "eurodollar", "canary",
                              "systemic")):
        return "risk_capstones"
    if any(k in n for k in ("sentiment", "social", "twitter", "reddit",
                              "news")):
        return "sentiment"
    if any(k in n for k in ("portfolio", "position")):
        return "portfolio"
    if any(k in n for k in ("13f", "activist", "sec-", "insider",
                              "form4", "pead", "earnings-")):
        return "smart_money"
    if any(k in n for k in ("short", "squeeze", "finra")):
        return "short_squeeze"
    if any(k in n for k in ("magic-formula", "gf-value", "starmine",
                              "beneish", "predictability", "smart-beta",
                              "eva-spread", "bond-vol", "ipo-pipeline")):
        return "pro_pack_v3"
    if any(k in n for k in ("bagger", "coffee-can", "hiring",
                              "global-liquidity", "insider-aggregate")):
        return "bagger_pack"
    if any(k in n for k in ("crypto", "dex", "btc", "eth")):
        return "crypto"
    if any(k in n for k in ("fred", "treasury", "fed-", "macro",
                              "liquidity")):
        return "macro_data"
    if any(k in n for k in ("signal-board", "best-ideas", "master-ranker",
                              "khalid-index", "alpha-score", "asymmetric")):
        return "fusion_meta"
    if any(k in n for k in ("daily-report", "telegram", "email")):
        return "delivery"
    if any(k in n for k in ("calibrator", "signal-logger",
                              "outcome-checker", "learning")):
        return "learning_loop"
    return "other"


# ---------- A. Lambda inventory ----------
def list_all_lambdas():
    """Paginated list_functions — much faster than per-Lambda get_function."""
    out = []
    paginator = lam.get_paginator("list_functions")
    for page in paginator.paginate():
        for fn in page.get("Functions", []):
            env = (fn.get("Environment") or {}).get("Variables", {}) or {}
            out.append({
                "name": fn.get("FunctionName"),
                "runtime": fn.get("Runtime"),
                "memory_mb": fn.get("MemorySize"),
                "timeout_s": fn.get("Timeout"),
                "code_size": fn.get("CodeSize"),
                "last_modified": fn.get("LastModified"),
                "env_keys": sorted(env.keys()),
                "description": (fn.get("Description") or "")[:200],
                "family": classify_family(fn.get("FunctionName")),
            })
    return out


# ---------- Static code scan ----------
S3_KEY_RE = re.compile(r"data/[a-z0-9_-]+\.json")
FMP_V3_RE = re.compile(r"financialmodelingprep\.com/api/v[34]")
FMP_STABLE_RE = re.compile(r"financialmodelingprep\.com/stable/([a-z0-9_/-]+)")
POLY_DEAD_RE = re.compile(r"api\.polygon\.io/stocks/v1/short-interest")
POLY_ANY_RE = re.compile(r"api\.polygon\.io/[a-z0-9_/-]+")
FRED_SERIES_RE = re.compile(r"series_id=([A-Z0-9]+)")
HARDCODED_KEY_RE = re.compile(
    r"(?:apikey|api_key|apiKey|token|TOKEN)\s*[=:]\s*['\"]([A-Za-z0-9_-]{20,})['\"]")
MODEL_RE = re.compile(r"claude-(haiku|sonnet|opus)-[0-9a-z-]+")


def scan_lambda_code(lambda_name):
    """Read lambda source from disk, extract endpoints + outputs + flags."""
    out = {
        "source_exists": False,
        "lines": 0,
        "s3_outputs": [],
        "fmp_stable_endpoints": [],
        "fmp_v3_v4_DEAD": False,
        "polygon_short_interest_DEAD": False,
        "polygon_endpoints": [],
        "fred_series": [],
        "model_strings": [],
        "hardcoded_keys_count": 0,
    }
    src = REPO_ROOT / "aws" / "lambdas" / lambda_name / "source" / \
        "lambda_function.py"
    if not src.exists():
        return out
    try:
        text = src.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return out
    out["source_exists"] = True
    out["lines"] = text.count("\n")
    out["s3_outputs"] = sorted(set(S3_KEY_RE.findall(text)))
    out["fmp_v3_v4_DEAD"] = bool(FMP_V3_RE.search(text))
    out["polygon_short_interest_DEAD"] = bool(POLY_DEAD_RE.search(text))
    out["fmp_stable_endpoints"] = sorted(set(
        m.group(1).split("?")[0].rstrip("/")
        for m in FMP_STABLE_RE.finditer(text)))[:15]
    out["polygon_endpoints"] = sorted(set(
        m.group(0) for m in POLY_ANY_RE.finditer(text)))[:10]
    out["fred_series"] = sorted(set(FRED_SERIES_RE.findall(text)))[:30]
    out["model_strings"] = sorted(set(
        m.group(0) for m in MODEL_RE.finditer(text)))
    # Hardcoded keys: count matches that don't look like env-var defaults
    hc = HARDCODED_KEY_RE.findall(text)
    out["hardcoded_keys_count"] = sum(
        1 for k in hc if k not in ("YOUR_API_KEY", "REPLACE_ME"))
    return out


# ---------- B. EventBridge schedule registry ----------
def list_all_schedules():
    """Combine EventBridge Scheduler (new) + EventBridge Rules (legacy)."""
    schedules = []
    # New Scheduler
    try:
        paginator = sch.get_paginator("list_schedules")
        for page in paginator.paginate():
            for s in page.get("Schedules", []):
                # Need full describe to get target
                try:
                    full = sch.get_schedule(
                        GroupName=s.get("GroupName", "default"),
                        Name=s["Name"])
                    target = (full.get("Target") or {}).get("Arn", "")
                    target_fn = (target.split(":")[-1]
                                  if target.startswith("arn:aws:lambda")
                                  else "")
                except Exception:
                    target_fn = ""
                schedules.append({
                    "api": "scheduler",
                    "name": s.get("Name"),
                    "group": s.get("GroupName"),
                    "state": s.get("State"),
                    "expression": s.get("ScheduleExpression"),
                    "target_lambda": target_fn,
                })
    except Exception as e:
        print(f"scheduler list err: {e}")
    # Legacy Rules
    try:
        paginator = events.get_paginator("list_rules")
        for page in paginator.paginate():
            for rule in page.get("Rules", []):
                if not rule.get("ScheduleExpression"):
                    continue  # event-pattern rules, not schedules
                try:
                    targets = events.list_targets_by_rule(
                        Rule=rule["Name"]).get("Targets", [])
                    target_fn = ""
                    for t in targets:
                        arn = t.get("Arn", "")
                        if arn.startswith("arn:aws:lambda"):
                            target_fn = arn.split(":")[-1]
                            break
                except Exception:
                    target_fn = ""
                schedules.append({
                    "api": "events",
                    "name": rule.get("Name"),
                    "state": rule.get("State"),
                    "expression": rule.get("ScheduleExpression"),
                    "target_lambda": target_fn,
                })
    except Exception as e:
        print(f"events list err: {e}")
    return schedules


# ---------- C. S3 inventory ----------
def list_all_s3_data():
    """list_objects_v2 paginated under data/ prefix."""
    out = []
    paginator = s3.get_paginator("list_objects_v2")
    now = datetime.now(timezone.utc)
    for page in paginator.paginate(Bucket=BUCKET, Prefix="data/"):
        for obj in page.get("Contents", []):
            lm = obj["LastModified"]
            age_h = round((now - lm).total_seconds() / 3600, 1)
            out.append({
                "key": obj["Key"],
                "size_bytes": obj["Size"],
                "last_modified": lm.isoformat(),
                "age_hours": age_h,
            })
    return out


# ---------- D. Page registry ----------
HTML_S3_REFS_RE = re.compile(r'["\'](data/[a-z0-9_-]+\.json)["\']')
HTML_HREF_RE = re.compile(r'href=["\']/?([a-z0-9_-]+\.html)["\']')
HTML_TITLE_RE = re.compile(r"<title>([^<]+)</title>", re.IGNORECASE)


def scan_pages():
    out = []
    for p in sorted(REPO_ROOT.glob("*.html")):
        try:
            text = p.read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue
        title_m = HTML_TITLE_RE.search(text)
        out.append({
            "page": p.name,
            "lines": text.count("\n"),
            "title": (title_m.group(1) if title_m else "").strip()[:120],
            "s3_keys_consumed": sorted(set(HTML_S3_REFS_RE.findall(text))),
            "linked_pages": sorted(set(HTML_HREF_RE.findall(text))),
        })
    return out


# ---------- E. Cross-graph ----------
def build_cross_graph(lambdas_scanned, s3_inventory, pages):
    s3_by_key = {o["key"]: o for o in s3_inventory}
    lambda_producers = {}  # key -> [lambda_names]
    for ln in lambdas_scanned:
        for k in ln["scan"]["s3_outputs"]:
            lambda_producers.setdefault(k, []).append(ln["name"])

    page_consumers = {}  # key -> [page_names]
    for p in pages:
        for k in p["s3_keys_consumed"]:
            page_consumers.setdefault(k, []).append(p["page"])

    # Combine to per-key view
    keys = set(lambda_producers) | set(page_consumers) | set(s3_by_key)
    graph = {}
    for k in sorted(keys):
        s3o = s3_by_key.get(k)
        graph[k] = {
            "producers": lambda_producers.get(k, []),
            "consumers": page_consumers.get(k, []),
            "s3_present": s3o is not None,
            "size_bytes": (s3o or {}).get("size_bytes"),
            "age_hours": (s3o or {}).get("age_hours"),
            "last_modified": (s3o or {}).get("last_modified"),
        }
    # Orphan flags
    orphan_lambdas = [k for k, v in graph.items()
                       if v["producers"] and not v["consumers"]
                       and v["s3_present"]]
    dangling_pages = [k for k, v in graph.items()
                       if v["consumers"] and not v["producers"]]
    ghost_s3 = [k for k, v in graph.items()
                 if v["s3_present"]
                 and not v["producers"] and not v["consumers"]]
    return graph, orphan_lambdas, dangling_pages, ghost_s3


# ---------- F. Bug scan ----------
def collect_bugs(lambdas_scanned, s3_inventory, cross_graph):
    bugs = {
        "lambdas_using_dead_fmp_v3_v4": [],
        "lambdas_using_dead_polygon_si": [],
        "lambdas_with_stale_model_strings": [],
        "lambdas_with_hardcoded_keys": [],
        "stale_s3_keys_24h_plus": [],
        "stale_s3_keys_72h_plus": [],
        "dangling_page_consumers": [],
        "pages_reading_missing_data": [],
    }
    expected_model = "claude-haiku-4-5-20251001"
    for ln in lambdas_scanned:
        s = ln["scan"]
        if s["fmp_v3_v4_DEAD"]:
            bugs["lambdas_using_dead_fmp_v3_v4"].append(ln["name"])
        if s["polygon_short_interest_DEAD"]:
            bugs["lambdas_using_dead_polygon_si"].append(ln["name"])
        bad_models = [m for m in s["model_strings"]
                       if m != expected_model
                       and not m.startswith("claude-sonnet-4-")
                       and not m.startswith("claude-opus-4-")]
        if bad_models:
            bugs["lambdas_with_stale_model_strings"].append(
                {"name": ln["name"], "models": bad_models})
        if s["hardcoded_keys_count"] > 0:
            bugs["lambdas_with_hardcoded_keys"].append(
                {"name": ln["name"], "count": s["hardcoded_keys_count"]})

    now = datetime.now(timezone.utc)
    for o in s3_inventory:
        if o["age_hours"] > 72:
            bugs["stale_s3_keys_72h_plus"].append(
                {"key": o["key"], "age_h": o["age_hours"]})
        elif o["age_hours"] > 24:
            bugs["stale_s3_keys_24h_plus"].append(
                {"key": o["key"], "age_h": o["age_hours"]})

    for k, v in cross_graph.items():
        if v["consumers"] and not v["producers"]:
            bugs["dangling_page_consumers"].append({
                "key": k, "consumers": v["consumers"]})
        if v["consumers"] and not v["s3_present"]:
            bugs["pages_reading_missing_data"].append({
                "key": k, "consumers": v["consumers"]})
    # Trim
    for k in bugs:
        if isinstance(bugs[k], list) and len(bugs[k]) > 25:
            bugs[k] = bugs[k][:25] + [f"...+{len(bugs[k])-25} more"]
    return bugs


# ---------- Markdown catalog generator ----------
def write_catalog_md(snapshot, out_path):
    s = snapshot
    md = []
    md.append("# JustHodl.AI — System Catalog")
    md.append("")
    md.append("**Authoritative inventory of every Lambda, schedule, S3 "
               "output, and HTML page on the platform.**")
    md.append("")
    md.append(f"*Auto-generated by `ops 1021` on {s['generated_at']}. "
               "Re-run that op to refresh.*")
    md.append("")
    md.append("> **Claude — read this file at the start of every session "
               "before proposing builds.** Nearly every \"obvious next "
               "feature\" already exists. Last session shipped 3 redundant "
               "engines before the CRITICAL BUILD RULE audit caught it. "
               "Check `lambdas_by_family` below, search `s3_outputs`, look "
               "at the cross-graph — *then* propose work.")
    md.append("")
    md.append("---")
    md.append("")
    md.append("## Top-line numbers")
    md.append("")
    md.append(f"- **Lambdas:** {s['n_lambdas']}")
    md.append(f"- **Schedules:** {s['n_schedules']}")
    md.append(f"- **S3 keys under data/:** {s['n_s3_keys']}")
    md.append(f"- **HTML pages:** {s['n_pages']}")
    md.append(f"- **Lambda → S3 → page edges:** {len(s['cross_graph'])}")
    md.append("")

    md.append("## Hard constants (use these EXACTLY)")
    md.append("")
    md.append("```")
    md.append("AWS account:           857687956942")
    md.append("Region:                us-east-1")
    md.append("S3 bucket:             justhodl-dashboard-live")
    md.append("Lambda role:           arn:aws:iam::857687956942:role/"
               "lambda-execution-role")
    md.append("Scheduler role:        arn:aws:iam::857687956942:role/"
               "justhodl-scheduler-role")
    md.append("Python runtime:        python3.12")
    md.append("Anthropic model:       claude-haiku-4-5-20251001 "
               "(claude-3-haiku-20240307 retired)")
    md.append("Repo:                  ElMooro/si")
    md.append("Workdir:               ~/work/si")
    md.append("Lambda description:    ≤250 chars")
    md.append("```")
    md.append("")

    md.append("## Lambda families (avoid rebuilding within these)")
    md.append("")
    fam = s["lambdas_by_family"]
    for fname in sorted(fam):
        names = fam[fname]
        md.append(f"### {fname} ({len(names)})")
        md.append("")
        for n in names[:80]:
            md.append(f"  - `{n}`")
        if len(names) > 80:
            md.append(f"  - ... +{len(names)-80} more")
        md.append("")

    md.append("## Bug scan")
    md.append("")
    bugs = s["bugs"]
    for category, items in bugs.items():
        n = len(items) if isinstance(items, list) else 0
        if n == 0:
            md.append(f"- ✅ **{category}**: clean")
        else:
            md.append(f"- 🔴 **{category}** ({n})")
            for item in items[:8]:
                md.append(f"    - `{item}`")
    md.append("")

    md.append("## Known dead / deprecated upstream feeds")
    md.append("")
    md.append("- **FMP `/api/v3` and `/api/v4`** — `403 Forbidden` since "
               "2025-08-31. Use `/stable/` only. The bug scan above flags "
               "any Lambda still on legacy paths.")
    md.append("- **Polygon `/stocks/v1/short-interest`** — endpoint largely "
               "abandoned post-2018 (130/157 tickers stuck at 2018-05-15 "
               "even with `order=desc` per ops 1014). Replacement at "
               "`aws/shared/finra_si.py` ready; waiting on Khalid FINRA "
               "Gateway registration (see `KHALID_ACTIONS.md`).")
    md.append("- **CBOE put-call ratio feed** — dead since session 5; "
               "replaced by Sentiment Extreme Composite v2.0 in T5.")
    md.append("")

    md.append("## FMP `/stable/` endpoint registry (in use across platform)")
    md.append("")
    all_eps = set()
    for ln in s["lambdas_scanned"]:
        for ep in ln["scan"]["fmp_stable_endpoints"]:
            all_eps.add(ep)
    for ep in sorted(all_eps):
        md.append(f"  - `/stable/{ep}`")
    md.append("")
    md.append("**Field gotchas (do NOT guess field names — verified):**")
    md.append("")
    md.append("- PE ratio (TTM): `priceToEarningsRatioTTM` in "
               "`/stable/ratios-ttm` — NOT in `/stable/quote` or "
               "`/stable/key-metrics-ttm`")
    md.append("- PB ratio (TTM): `priceToBookRatioTTM` in "
               "`/stable/ratios-ttm`")
    md.append("- ROIC (TTM): `returnOnInvestedCapitalTTM` in "
               "`/stable/key-metrics-ttm` (NOT `roicTTM`)")
    md.append("- Gross margin (TTM): `grossProfitMarginTTM` in "
               "`/stable/ratios-ttm`")
    md.append("- Effective tax (TTM): `effectiveTaxRateTTM` in "
               "`/stable/ratios-ttm`")
    md.append("- Invested capital (TTM): `investedCapitalTTM` in "
               "`/stable/key-metrics-ttm`")
    md.append("- Enterprise value (TTM): `enterpriseValueTTM` in "
               "`/stable/key-metrics-ttm`")
    md.append("- Beta: `beta` in `/stable/profile`")
    md.append("- `/stable/quote` has NO PE field. Has: price, marketCap, "
               "dayHigh, dayLow, exchange, name, open, previousClose, "
               "priceAvg200, priceAvg50, volume, yearHigh, yearLow.")
    md.append("")

    md.append("## Cross-graph orphans (Lambdas producing data nothing reads)")
    md.append("")
    md.append(f"Count: **{len(s['orphan_lambdas_s3_keys'])}**. "
               "These are candidates for either page-building or deletion.")
    for k in s["orphan_lambdas_s3_keys"][:30]:
        md.append(f"  - `{k}`")
    md.append("")

    md.append("## Cross-graph dangling consumers (pages reading from "
               "missing Lambdas)")
    md.append("")
    md.append(f"Count: **{len(s['dangling_pages_keys'])}**. These are bugs "
               "— the page is reading an S3 key nothing produces.")
    for k in s["dangling_pages_keys"][:30]:
        md.append(f"  - `{k}`")
    md.append("")

    md.append("## PROTECTED items (NEVER delete or modify "
               "without explicit approval)")
    md.append("")
    md.append("- `justhodl-stock-screener` Lambda + `screener/data.json` + "
               "`screener.html` — Khalid's flagship S&P 500 screener")
    md.append("- `index.html` homepage — top-level platform entry point; "
               "audit homepage navigation before adding new pages")
    md.append("")

    md.append("## Doctrine reminders")
    md.append("")
    md.append("- **Audit before building.** Check `lambdas_by_family` "
               "above. Pattern from May 2026 session: 4 of 4 Phase D "
               "candidates (squeeze, EVA, 13F, PEAD) already existed; only "
               "EVA was a true gap.")
    md.append("- **Ship loop:** `cd ~/work/si; edit; git add X; git -c "
               "user.email=raafouis@gmail.com -c user.name=Khalid commit "
               "-m msg; git push`. Sandbox blocks *.amazonaws.com — always "
               "ops-script. GH Actions diff = HEAD^ HEAD so split unrelated "
               "changes across commits. Python zipfile (not zip cmd). "
               "Always `\\n` line endings.")
    md.append("- **Audit doctrine:** after meaningful push, write "
               "`ops/pending/NNN_X_verify.py` = temp Lambda → fetch live "
               "URL → scan markers → return JSON → invoke → write "
               "`ops/reports/NNN.json` → delete temp Lambda. Commit/push, "
               "sleep 100-200, git pull, parse report. Claude proves work "
               "end-to-end without Khalid running anything.")
    md.append("- **Lambda invocations from sandbox:** never run aws CLI or "
               "boto3 directly — those calls go through *.amazonaws.com "
               "which is firewalled. Always use the ops-script pattern.")
    md.append("- **Memory entries are limited to 500 chars and 30 total.** "
               "Read SYSTEM_CATALOG.md instead of using memory for "
               "detailed system facts.")
    md.append("")

    out_path.write_text("\n".join(md), encoding="utf-8")


def main():
    started = time.time()
    report = {"started_at": datetime.now(timezone.utc).isoformat()}

    print("[ops 1021] phase A: enumerating Lambdas…")
    lambdas = list_all_lambdas()
    report["n_lambdas"] = len(lambdas)
    print(f"  found {len(lambdas)} Lambdas")

    print("[ops 1021] phase A2: scanning source code…")
    lambdas_scanned = []
    for ln in lambdas:
        scan = scan_lambda_code(ln["name"])
        lambdas_scanned.append({**ln, "scan": scan})

    print("[ops 1021] phase B: enumerating schedules…")
    schedules = list_all_schedules()
    report["n_schedules"] = len(schedules)
    print(f"  found {len(schedules)} schedules")

    print("[ops 1021] phase C: listing S3 data keys…")
    s3_inventory = list_all_s3_data()
    report["n_s3_keys"] = len(s3_inventory)
    print(f"  found {len(s3_inventory)} S3 keys")

    print("[ops 1021] phase D: scanning HTML pages…")
    pages = scan_pages()
    report["n_pages"] = len(pages)
    print(f"  found {len(pages)} pages")

    print("[ops 1021] phase E: building cross-graph…")
    cross_graph, orphan_keys, dangling_keys, ghost_keys = \
        build_cross_graph(lambdas_scanned, s3_inventory, pages)
    report["cross_graph"] = cross_graph
    report["orphan_lambdas_s3_keys"] = orphan_keys
    report["dangling_pages_keys"] = dangling_keys
    report["ghost_s3_keys"] = ghost_keys

    print("[ops 1021] phase F: bug scan…")
    bugs = collect_bugs(lambdas_scanned, s3_inventory, cross_graph)
    report["bugs"] = bugs

    # Group lambdas by family for the catalog
    by_family = {}
    for ln in lambdas_scanned:
        by_family.setdefault(ln["family"], []).append(ln["name"])
    for k in by_family:
        by_family[k].sort()
    report["lambdas_by_family"] = by_family
    report["lambdas_scanned"] = lambdas_scanned
    report["schedules"] = schedules
    report["s3_inventory_top_50_by_age"] = sorted(
        s3_inventory, key=lambda x: x["age_hours"], reverse=True)[:50]
    report["pages"] = pages

    # Scorecard summary
    report["scorecard_summary"] = {
        "n_lambdas": report["n_lambdas"],
        "n_schedules": report["n_schedules"],
        "n_s3_keys": report["n_s3_keys"],
        "n_pages": report["n_pages"],
        "n_fmp_v3_v4_bugs": len(bugs["lambdas_using_dead_fmp_v3_v4"]),
        "n_polygon_si_dead_bugs": len(bugs["lambdas_using_dead_polygon_si"]),
        "n_stale_model_string_bugs": len(
            bugs["lambdas_with_stale_model_strings"]),
        "n_hardcoded_key_bugs": len(bugs["lambdas_with_hardcoded_keys"]),
        "n_stale_s3_72h_plus": len(bugs["stale_s3_keys_72h_plus"]),
        "n_orphan_lambdas": len(orphan_keys),
        "n_dangling_page_keys": len(dangling_keys),
    }

    report["generated_at"] = datetime.now(timezone.utc).isoformat()
    report["duration_seconds"] = round(time.time() - started, 1)

    # Write JSON
    out_json = REPO_ROOT / "aws" / "ops" / "reports" / "1021.json"
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(report, indent=2, default=str))

    # Write catalog markdown
    catalog_path = REPO_ROOT / "SYSTEM_CATALOG.md"
    write_catalog_md(report, catalog_path)

    print(f"[ops 1021] complete in {report['duration_seconds']}s")
    print(f"  json:    {out_json.relative_to(REPO_ROOT)}")
    print(f"  catalog: {catalog_path.relative_to(REPO_ROOT)}")
    print(json.dumps(report["scorecard_summary"], indent=2))


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print(traceback.format_exc())
        sys.exit(1)
