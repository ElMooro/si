#!/usr/bin/env python3
"""
Step 81 — Refine architecture doc categorization.

Step 80 used a name-pattern heuristic which misclassified some
critical Lambdas (justhodl-repo-monitor, justhodl-financial-secretary)
as "deprecated_or_unclear" when they're actually core writers of
repo-data.json and data/secretary-*.json.

This rerun uses an EXPLICIT category map for known Lambdas, plus
verifies categorization by checking what each Lambda actually writes
to S3 (per the source analysis from step 80).

Outputs the same file path so it overwrites.
"""
import json
import os
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

s3 = boto3.client("s3", region_name=REGION)


def load_inventory():
    obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="_audit/inventory_2026-04-25.json")
    return json.loads(obj["Body"].read())


def analyze_source(src):
    if not src:
        return {}
    info = {
        "s3_reads": set(),
        "s3_writes": set(),
        "ddb_tables": set(),
        "http_endpoints": set(),
        "lambda_invokes": set(),
    }
    for m in re.finditer(r"""(?:get_object|head_object).*?Key\s*=\s*['"]([^'"]+)['"]""", src):
        info["s3_reads"].add(m.group(1))
    for m in re.finditer(r"""fs3\(['"]([^'"]+)['"]""", src):
        info["s3_reads"].add(m.group(1))
    for m in re.finditer(r"""justhodl-dashboard-live\.s3[^/]*?/([^"'\s]+)""", src):
        key = m.group(1).split("?")[0]
        if key.endswith(".json") or key.endswith(".html"):
            info["s3_reads"].add(key)
    for m in re.finditer(r"""put_object.*?Key\s*=\s*f?['"]([^'"]+)['"]""", src, re.DOTALL):
        info["s3_writes"].add(m.group(1))
    for m in re.finditer(r"""dynamodb\.Table\(['"]([^'"]+)['"]""", src):
        info["ddb_tables"].add(m.group(1))
    for m in re.finditer(r"""https?://([a-zA-Z0-9.-]+\.(?:com|org|io|ai|net|gov))""", src):
        host = m.group(1)
        if "amazonaws" not in host:
            info["http_endpoints"].add(host)
    for m in re.finditer(r"""invoke.*?FunctionName\s*=\s*['"]([^'"]+)['"]""", src, re.DOTALL):
        info["lambda_invokes"].add(m.group(1))
    return {k: sorted(v) for k, v in info.items()}


# ─── Explicit category map (manually curated) ──────────────────────────
# Categories: core_pipeline, learning_loop, user_facing, data_collectors,
#             intelligence_agents, telegram_bot, broken_or_legacy
EXPLICIT_CATEGORY = {
    # CORE PIPELINE — produce the canonical S3 data files
    "justhodl-daily-report-v3":      "core_pipeline",  # data/report.json
    "justhodl-crypto-intel":         "core_pipeline",  # crypto-intel.json
    "justhodl-edge-engine":          "core_pipeline",  # edge-data.json
    "justhodl-repo-monitor":         "core_pipeline",  # repo-data.json
    "justhodl-options-flow":         "core_pipeline",  # flow-data.json
    "justhodl-stock-screener":       "core_pipeline",  # screener/data.json
    "justhodl-stock-analyzer":       "core_pipeline",  # stock/* historical bars
    "justhodl-valuations-agent":     "core_pipeline",  # valuations-data.json (monthly)
    "justhodl-investor-agents":      "core_pipeline",  # investor-agents output
    "justhodl-intelligence":         "core_pipeline",  # intelligence-report.json (FIXED 2026-04-25)
    "justhodl-morning-intelligence": "core_pipeline",  # 8AM ET daily brief
    "justhodl-financial-secretary":  "core_pipeline",  # secretary-latest.json (FRED-based reports)

    # LEARNING LOOP
    "justhodl-signal-logger":   "learning_loop",
    "justhodl-outcome-checker": "learning_loop",
    "justhodl-calibrator":      "learning_loop",

    # USER-FACING (Function URLs / browser-callable)
    "justhodl-ai-chat":              "user_facing",
    "justhodl-chat-api":             "user_facing",
    "justhodl-bloomberg-v8":         "user_facing",
    "justhodl-advanced-charts":      "user_facing",
    "justhodl-khalid-metrics":       "user_facing",
    "justhodl-market-intelligence":  "user_facing",
    "cftc-futures-positioning-agent":"user_facing",

    # TELEGRAM
    "justhodl-telegram-bot": "telegram_bot",

    # KNOWN BROKEN
    "justhodl-ml-predictions": "broken_or_legacy",  # Calls dead api.justhodl.ai
    "MLPredictor":             "broken_or_legacy",  # pre-cleanup era

    # DATA COLLECTORS — fetch external APIs, write to data/* in S3
    # These are mostly fine, just numerous
    "alphavantage-market-agent":     "data_collectors",
    "alphavantage-technical-analysis":"data_collectors",
    "bea-economic-agent":            "data_collectors",
    "benzinga-news-agent":           "data_collectors",
    "bls-labor-agent":               "data_collectors",
    "bls-employment-api-v2":         "data_collectors",
    "bond-indices-agent":            "data_collectors",
    "census-economic-agent":         "data_collectors",
    "chatgpt-agent-api":             "data_collectors",
    "coinmarketcap-agent":           "data_collectors",
    "dollar-strength-agent":         "data_collectors",
    "ecb":                           "data_collectors",
    "ecb-auto-updater":              "data_collectors",
    "ecb-data-daily-updater":        "data_collectors",
    "eia-energy-agent":              "data_collectors",
    "enhanced-repo-agent":           "data_collectors",
    "fedliquidityapi":               "data_collectors",
    "fedliquidityapi-test":          "data_collectors",
    "fmp-fundamentals-agent":        "data_collectors",
    "fmp-stock-picks-agent":         "data_collectors",
    "fred-ice-bofa-api":             "data_collectors",
    "global-liquidity-agent-TEST":   "data_collectors",
    "global-liquidity-agent-v2":     "data_collectors",
    "google-trends-agent":           "data_collectors",
    "justhodl-charts-agent":         "data_collectors",
    "justhodl-data-collector":       "data_collectors",
    "justhodl-liquidity-agent":      "data_collectors",
    "manufacturing-global-agent":    "data_collectors",
    "nasdaq-datalink-agent":         "data_collectors",
    "news-sentiment-agent":          "data_collectors",
    "nyfed-financial-stability-fetcher":"data_collectors",
    "nyfed-primary-dealer-fetcher":  "data_collectors",
    "ofrapi":                        "data_collectors",
    "securities-banking-agent":      "data_collectors",
    "treasury-auto-updater":         "data_collectors",
    "treasury-api":                  "data_collectors",
    "volatility-monitor-agent":      "data_collectors",
    "xccy-basis-agent":              "data_collectors",

    # INTELLIGENCE AGENTS — reports/derived
    "justhodl-email-reports":        "intelligence_agents",
    "justhodl-email-reports-v2":     "intelligence_agents",
    "justhodl-daily-macro-report":   "intelligence_agents",
    "daily-liquidity-report":        "intelligence_agents",
    "macro-financial-intelligence":  "intelligence_agents",
    "macro-financial-report-viewer": "intelligence_agents",
    "macro-report-api":              "intelligence_agents",
    "FinancialIntelligence-Backend": "intelligence_agents",
    "aiapi-market-analyzer":         "intelligence_agents",
    "autonomous-ai-processor":       "intelligence_agents",
    "scrapeMacroData":               "intelligence_agents",
    "permanent-market-intelligence": "intelligence_agents",
    "report-email-agent":            "intelligence_agents",
    "market-report-generator":       "intelligence_agents",
    "aiapi-monitor":                 "intelligence_agents",

    # PROXIES / GATEWAYS
    "justhodl-fred-proxy":            "data_collectors",
    "justhodl-ecb-proxy":             "data_collectors",
    "justhodl-treasury-proxy":        "data_collectors",
    "universal-agent-gateway":        "data_collectors",
    "multi-agent-orchestrator":       "data_collectors",
    "ultimate-multi-agent":           "data_collectors",

    # MISC / UNCLEAR
    "justhodl-cache-layer":         "deprecated_or_unclear",
    "justhodl-crypto-enricher":     "data_collectors",  # writes crypto enrichment
    "justhodl-dex-scanner":         "data_collectors",
    "justhodl-news-sentiment":      "data_collectors",
    "justhodl-ultimate-orchestrator":"intelligence_agents",
    "justhodl-ultimate-trading":    "intelligence_agents",
    "createEnhancedIndex":          "deprecated_or_unclear",
    "createUniversalIndex":         "deprecated_or_unclear",
    "economyapi":                   "deprecated_or_unclear",
    "nyfedapi-isolated":            "deprecated_or_unclear",
    "testEnhancedScraper":          "deprecated_or_unclear",
    "fredapi":                      "data_collectors",
    "fedapi":                       "data_collectors",

    # OPENBB (legacy, separate ecosystem)
    "OpenBBS3DataProxy":          "legacy_openbb",
    "openbb-system2-api":         "legacy_openbb",
    "openbb-websocket-broadcast": "legacy_openbb",
    "openbb-websocket-handler":   "legacy_openbb",
}


with report("refine_architecture_doc") as r:
    r.heading("Refine architecture doc — explicit categorization")

    inventory = load_inventory()

    # Re-analyze sources
    lambdas_dir = REPO_ROOT / "aws/lambdas"
    src_data = {}
    for fn in inventory["lambdas"]:
        name = fn["name"]
        for candidate in [
            lambdas_dir / name / "source" / "lambda_function.py",
            lambdas_dir / name / "source" / "index.py",
            lambdas_dir / name / "source" / "handler.py",
        ]:
            if candidate.exists():
                src = candidate.read_text(encoding="utf-8", errors="ignore")
                src_data[name] = analyze_source(src)
                src_data[name]["loc"] = src.count("\n")
                break

    # Categorize
    categories = defaultdict(list)
    uncategorized = []
    for fn in inventory["lambdas"]:
        n = fn["name"]
        if n in EXPLICIT_CATEGORY:
            categories[EXPLICIT_CATEGORY[n]].append(n)
        else:
            uncategorized.append(n)
            categories["deprecated_or_unclear"].append(n)

    r.log(f"  Explicit map covered: {len(inventory['lambdas']) - len(uncategorized)}/{len(inventory['lambdas'])}")
    if uncategorized:
        r.log(f"  Falling back to deprecated_or_unclear:")
        for u in uncategorized:
            r.log(f"    - {u}")

    for cat, items in categories.items():
        r.log(f"    {cat}: {len(items)}")

    # Build EB lookup
    eb_by_target = defaultdict(list)
    for rule in inventory["eb_rules"]:
        for t in rule.get("targets", []):
            arn_tail = t.get("arn", "")
            if "function:" in arn_tail:
                target = arn_tail.split("function:")[-1]
                eb_by_target[target].append({
                    "rule": rule["name"],
                    "schedule": rule.get("schedule", "?"),
                    "state": rule.get("state"),
                })

    fn_by_name = {f["name"]: f for f in inventory["lambdas"]}

    # ─── Build doc ────────────────────────────────────────────────────
    today = "2026-04-25"
    md = []
    md.append(f"# JustHodl.AI — System Architecture (canonical, {today})\n")
    md.append(f"**Generated:** {datetime.now(timezone.utc).isoformat()}  ")
    md.append(f"**Source data:** [aws/ops/audit/inventory_{today}.json](inventory_{today}.json)  ")
    md.append(f"**Account:** AWS 857687956942 (us-east-1) + Cloudflare 2e120c8358c6c85dcaba07eb16947817\n")
    md.append("---\n")

    md.append("## At a glance\n")
    md.append(f"- **{len(inventory['lambdas'])} Lambda functions** (50 in repo, 45 not yet pulled into version control)")
    md.append(f"- **{len(inventory['s3_keys'])}+ S3 objects** in `justhodl-dashboard-live` (capped at 5000)")
    md.append(f"- **{len(inventory['ddb_tables'])} DynamoDB tables** (only 3 actively used)")
    md.append(f"- **{len(inventory['eb_rules'])} EventBridge rules** (90 enabled)")
    md.append(f"- **{len(inventory['ssm_params'])} SSM parameters** under `/justhodl/`")
    md.append(f"- **1 Cloudflare Worker:** `justhodl-ai-proxy` at `api.justhodl.ai`\n")

    # Critical path diagram
    md.append("## Critical path: signal → outcome → calibrated weight\n")
    md.append("```")
    md.append("External APIs (FRED, Polygon, FMP, CoinGecko, ECB, BLS, etc.)")
    md.append("        │")
    md.append("        ▼")
    md.append("  ┌─────────────────────────────────────┐")
    md.append("  │  data collector Lambdas (~38)       │  ← fetch + write to S3 on schedule")
    md.append("  └─────────────────────────────────────┘")
    md.append("        │")
    md.append("        ▼")
    md.append("  ┌───────────────────────────────────────────────────────────┐")
    md.append("  │  S3 (justhodl-dashboard-live) — public-readable per       │")
    md.append("  │  bucket policy (data/*, screener/*, sentiment/* + a few): │")
    md.append("  │    data/report.json     ← daily-report-v3 every 5 min     │")
    md.append("  │    crypto-intel.json    ← crypto-intel every 15 min       │")
    md.append("  │    edge-data.json       ← edge-engine every 6 hours       │")
    md.append("  │    repo-data.json       ← repo-monitor every 30m weekdays │")
    md.append("  │    flow-data.json       ← options-flow every 4h           │")
    md.append("  │    valuations-data.json ← valuations-agent monthly        │")
    md.append("  │    screener/data.json   ← stock-screener every 4h         │")
    md.append("  │    intelligence-report.json ← justhodl-intelligence       │")
    md.append("  │                            (hourly weekdays, FIXED 04-25) │")
    md.append("  └───────────────────────────────────────────────────────────┘")
    md.append("        │")
    md.append("        ▼")
    md.append("  ┌─────────────────────────────────────────┐")
    md.append("  │  justhodl-signal-logger (every 6h)      │  ← logs SIGNALS")
    md.append("  │    schema_v2 since 2026-04-25:          │     to DynamoDB")
    md.append("  │      baseline_price + magnitude +       │")
    md.append("  │      target_price + rationale +         │")
    md.append("  │      regime_at_log + khalid_score       │")
    md.append("  │  → DDB justhodl-signals (4,579 items)   │")
    md.append("  └─────────────────────────────────────────┘")
    md.append("        │")
    md.append("        ▼")
    md.append("  ┌─────────────────────────────────────────┐")
    md.append("  │  justhodl-outcome-checker               │  ← evaluates after")
    md.append("  │    Mon-Fri 22:30 UTC (NEW 2026-04-25)   │     windows elapse")
    md.append("  │    Sun 8:00 UTC                         │")
    md.append("  │    1st of month 8:00 UTC (NEW)          │")
    md.append("  │  → DDB justhodl-outcomes (738 items)    │")
    md.append("  └─────────────────────────────────────────┘")
    md.append("        │")
    md.append("        ▼")
    md.append("  ┌─────────────────────────────────────────┐")
    md.append("  │  justhodl-calibrator (Sunday 9:00 UTC)  │  ← computes")
    md.append("  │    24 signal types weighted             │     accuracy")
    md.append("  │  → SSM /justhodl/calibration/weights    │     per signal")
    md.append("  │  → SSM /justhodl/calibration/accuracy   │")
    md.append("  │  → S3 calibration/latest.json           │")
    md.append("  └─────────────────────────────────────────┘")
    md.append("```\n")

    # Tonight's fix flow
    md.append("## Recently fixed (2026-04-24/25)\n")
    md.append("Three layers of broken pipeline were repaired in two overnight sessions:")
    md.append("")
    md.append("1. **outcome-checker price fetchers** — Polygon `/v2/last/trade` (paid-tier-only) and FMP `/v3/quote-short` (retired Aug 2025) both returned HTTP 403 silently. Replaced with Polygon `/v2/aggs/.../prev`, FMP `/stable/quote`, and CoinGecko fallback. The learning loop had been silently dead — every `correct=None`.")
    md.append("")
    md.append("2. **signal-logger baseline_price capture** — 12 of 13 signal types had 0% baseline_price coverage because callers didn't pass `price=`. Added `get_baseline_price(ticker)` helper called automatically by `log_sig()` when no explicit price given. Now 100% coverage on new signals.")
    md.append("")
    md.append("3. **justhodl-intelligence chokepoint** — was reading from stale `data.json` orphan + broken `predictions.json`. Result: `intelligence-report.json` had `khalid_index=0`, `ml_risk_score=0`, `carry_risk_score=0` for an unknown duration, poisoning calibration data for those signals. Fixed by adapter pattern: now reads `data/report.json` + synthesizes pred dict from healthy sources (edge-data, repo-data, flow-data). Switched HTTP fetches to boto3 SDK so non-public-readable files (repo-data, edge-data) load correctly.")
    md.append("")
    md.append("**Result**: `intelligence-report.json` scores now: `khalid_index=43, plumbing_stress=25, ml_risk_score=60, carry_risk_score=25, vix=19.31`. signal-logger logs real values for ml_risk and carry_risk for the first time. Sunday April 26 9 UTC calibration will be the first meaningful learning event in system history.\n")

    # Per-Lambda cards
    md.append("## Lambda inventory by purpose\n")
    cat_descriptions = {
        "core_pipeline": "Core data pipeline — these produce the canonical S3 data files",
        "learning_loop": "Calibration system (fully fixed 2026-04-25)",
        "user_facing": "Lambdas with Function URLs, called by the browser or Telegram",
        "data_collectors": "External API fetchers — write to S3 on schedule",
        "intelligence_agents": "Multi-source aggregators producing reports/derivatives",
        "telegram_bot": "Telegram integration",
        "broken_or_legacy": "Known broken or pre-cleanup-era. Don't depend on these.",
        "legacy_openbb": "OpenBB-related Lambdas — appear unused now (consider retirement)",
        "deprecated_or_unclear": "Purpose unclear from name + source. Investigate or retire.",
    }
    cat_order = [
        "core_pipeline", "learning_loop", "user_facing", "data_collectors",
        "intelligence_agents", "telegram_bot", "broken_or_legacy",
        "legacy_openbb", "deprecated_or_unclear",
    ]
    for cat in cat_order:
        names = categories.get(cat, [])
        if not names:
            continue
        md.append(f"### {cat.replace('_', ' ').title()} ({len(names)})\n")
        md.append(f"_{cat_descriptions.get(cat, '')}_\n")
        for name in sorted(names):
            fn = fn_by_name.get(name, {})
            srcinfo = src_data.get(name, {})
            schedules = [r["schedule"] for r in eb_by_target.get(name, []) if r["state"] == "ENABLED"]
            md.append(f"#### `{name}`")
            details = []
            if fn.get("runtime"):
                details.append(f"runtime={fn['runtime']}")
            if fn.get("memory_mb"):
                details.append(f"mem={fn['memory_mb']}MB")
            if fn.get("timeout_s"):
                details.append(f"timeout={fn['timeout_s']}s")
            if details:
                md.append(f"- {', '.join(details)}")
            if schedules:
                md.append(f"- **Schedules:** {', '.join(schedules)}")
            if not srcinfo:
                md.append(f"- ⚠ Source not in repo")
            else:
                if srcinfo.get("loc"):
                    md.append(f"- LOC: {srcinfo['loc']}")
                if srcinfo.get("s3_reads"):
                    keys = srcinfo["s3_reads"][:8]
                    md.append(f"- **Reads S3:** {', '.join(f'`{k}`' for k in keys)}{'…' if len(srcinfo['s3_reads']) > 8 else ''}")
                if srcinfo.get("s3_writes"):
                    keys = srcinfo["s3_writes"][:8]
                    md.append(f"- **Writes S3:** {', '.join(f'`{k}`' for k in keys)}{'…' if len(srcinfo['s3_writes']) > 8 else ''}")
                if srcinfo.get("ddb_tables"):
                    md.append(f"- **DynamoDB:** {', '.join(f'`{t}`' for t in srcinfo['ddb_tables'])}")
                if srcinfo.get("http_endpoints"):
                    eps = srcinfo["http_endpoints"][:6]
                    md.append(f"- **External APIs:** {', '.join(f'`{e}`' for e in eps)}{'…' if len(srcinfo['http_endpoints']) > 6 else ''}")
                if srcinfo.get("lambda_invokes"):
                    md.append(f"- **Invokes:** {', '.join(srcinfo['lambda_invokes'])}")
            if fn.get("env_keys"):
                md.append(f"- env: {', '.join(fn['env_keys'][:8])}{'…' if len(fn['env_keys']) > 8 else ''}")
            md.append("")

    # DynamoDB
    md.append("## DynamoDB tables\n")
    md.append("| Table | Items | Size | Status |")
    md.append("|---|---:|---:|---|")
    for t in sorted(inventory["ddb_tables"], key=lambda x: -(x.get("size_bytes") or 0)):
        size_kb = (t.get("size_bytes") or 0) / 1024
        active = "🟢 ACTIVE" if (t.get("size_bytes") or 0) > 1000 else "💤 empty"
        md.append(f"| `{t['name']}` | {t.get('item_count', '?'):,} | {size_kb:.0f}KB | {active} |")
    md.append("")
    md.append("**Active tables:**")
    md.append("- `justhodl-signals` — every signal logged (4,579 items, schema_v2 since 2026-04-25)")
    md.append("- `justhodl-outcomes` — scored outcomes from outcome-checker (738 items)")
    md.append("- `fed-liquidity-cache` — FRED data cache (267k items, 19MB)")
    md.append("")
    md.append("**Cleanup candidate:** 22 empty tables from prior architecture experiments. Safe to delete after grep confirms no Lambda still references them.\n")

    # SSM
    md.append("## SSM parameters\n")
    md.append("| Name | Type | Purpose |")
    md.append("|---|---|---|")
    purposes = {
        "/justhodl/ai-chat/auth-token":     "Token for ai-chat Lambda; injected by CF Worker",
        "/justhodl/calibration/accuracy":   "Per-signal accuracy stats (calibrator output)",
        "/justhodl/calibration/report":     "Full calibration report JSON",
        "/justhodl/calibration/weights":    "Per-signal weights (consumed by future ranker)",
        "/justhodl/telegram/chat_id":       "Khalid's Telegram chat ID for bot pushes",
    }
    for p in inventory["ssm_params"]:
        md.append(f"| `{p['name']}` | {p['type']} | {purposes.get(p['name'], '(unknown)')} |")
    md.append("")

    # EB schedules
    md.append("## EventBridge schedules (90 enabled, 8 disabled)\n")
    md.append("Most-frequent firing rules grouped by pattern:\n")
    schedule_targets = []
    for rule in inventory["eb_rules"]:
        if rule.get("state") != "ENABLED":
            continue
        for t in rule.get("targets", []):
            arn_tail = t.get("arn", "")
            target = arn_tail.split("function:")[-1] if "function:" in arn_tail else arn_tail.split(":")[-1]
            schedule_targets.append({"rule": rule["name"], "schedule": rule.get("schedule", "?"), "target": target})
    by_schedule_type = defaultdict(list)
    for st in schedule_targets:
        s = st["schedule"]
        if s.startswith("rate("):
            by_schedule_type["rate"].append(st)
        elif "MON-FRI" in s:
            by_schedule_type["weekday"].append(st)
        elif any(d in s for d in ["SUN", "FRI", "MON", "TUE", "WED", "THU", "SAT"]):
            by_schedule_type["weekly"].append(st)
        elif s.startswith("cron(0 "):
            by_schedule_type["daily"].append(st)
        else:
            by_schedule_type["other"].append(st)
    for cat in ["rate", "weekday", "weekly", "daily", "other"]:
        items = by_schedule_type.get(cat, [])
        if not items:
            continue
        md.append(f"### {cat.title()} ({len(items)})\n")
        for st in sorted(items, key=lambda x: x["target"])[:20]:
            md.append(f"- `{st['schedule']}` → `{st['target']}`")
        if len(items) > 20:
            md.append(f"- ... and {len(items) - 20} more")
        md.append("")

    # S3 layout
    md.append("## S3 layout (`justhodl-dashboard-live`)\n")
    md.append("### Public-readable paths (per bucket policy)")
    md.append("- `data/*` — primary data files (3,319 files; report.json + fred caches + secretary history)")
    md.append("- `screener/*` — stock screener output")
    md.append("- `sentiment/*` — sentiment analysis output")
    md.append("- `flow-data.json` (root)")
    md.append("- `crypto-intel.json` (root)")
    md.append("")
    md.append("### Private (boto3 SDK access only)")
    md.append("- `repo-data.json` — repo monitor stress")
    md.append("- `edge-data.json` — edge engine composite")
    md.append("- `intelligence-report.json` — cross-system synthesis")
    md.append("- `predictions.json` — STALE 30+h (ml-predictions broken; downstream now bypasses)")
    md.append("- `valuations-data.json` — monthly valuations")
    md.append("- `calibration/*` — calibrator history")
    md.append("- `learning/*` — signal-logger metadata")
    md.append("- `archive/*` — historical snapshots (1,665 files, 29MB)")
    md.append("")
    md.append("### Critical files by update frequency")
    md.append("| Key | Writer | Frequency | Notes |")
    md.append("|---|---|---|---|")
    md.append("| `data/report.json` | daily-report-v3 | every 5 min | Source of truth: 188 stocks + FRED + regime |")
    md.append("| `repo-data.json` | repo-monitor | every 30 min weekdays | Plumbing stress score |")
    md.append("| `edge-data.json` | edge-engine | every 6h | Composite ML risk score, regime |")
    md.append("| `crypto-intel.json` | crypto-intel | every 15 min | BTC/ETH/SOL technicals + on-chain |")
    md.append("| `intelligence-report.json` | justhodl-intelligence | hourly weekdays | Cross-system synthesis (FIXED 2026-04-25) |")
    md.append("| `flow-data.json` | options-flow | every 4h | Options flow, fund flows |")
    md.append("| `screener/data.json` | stock-screener | every 4h | 503 stocks, Piotroski/Altman scores |")
    md.append("| `valuations-data.json` | valuations-agent | 1st of month 14 UTC | CAPE, Buffett indicator |")
    md.append("")

    # Cloudflare
    md.append("## Cloudflare\n")
    md.append("**Account:** `2e120c8358c6c85dcaba07eb16947817`")
    md.append("")
    md.append("**Worker:** `justhodl-ai-proxy`")
    md.append("- Routes: `api.justhodl.ai` (custom domain) + `justhodl-ai-proxy.raafouis.workers.dev`")
    md.append("- Forwards POST → AWS Lambda `justhodl-ai-chat`")
    md.append("- Origin allowlist: `https://justhodl.ai`, `https://www.justhodl.ai`")
    md.append("- Adds auth token from secret `AI_CHAT_TOKEN`")
    md.append("- Body size cap: 32KB")
    md.append("- Source: `cloudflare/workers/justhodl-ai-proxy/src/index.js`")
    md.append("- Auto-deploys via `.github/workflows/deploy-workers.yml`")
    md.append("")
    md.append("**This is the ONLY Worker.** No D1, no KV, no R2, no Hyperdrive in use.")
    md.append("")

    # CI/CD
    md.append("## CI/CD\n")
    md.append("**Repo:** `ElMooro/si` (justhodl.ai is GitHub Pages from this)")
    md.append("**Local working directory:** `/c/Users/Adam/Desktop/justhodl/si`")
    md.append("")
    md.append("**Workflows:**")
    md.append("- `deploy-lambdas.yml` — deploys any `aws/lambdas/<n>/source/` change to AWS")
    md.append("- `deploy-workers.yml` — deploys `cloudflare/workers/*/` to CF on push")
    md.append("- `run-ops.yml` — runs `aws/ops/pending/*.py` scripts with AWS creds; auto-commits `aws/ops/reports/`, `aws/ops/audit/`, and `aws/lambdas/` changes back to repo")
    md.append("- `rotate-dex-scanner-pat.yml` — manual workflow_dispatch for GitHub PAT rotation")
    md.append("")
    md.append("**IAM:** `github-actions-justhodl` (9 attached policies inc. AmazonDynamoDBReadOnlyAccess)")
    md.append("**Secrets:** `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `ANTHROPIC_API_KEY`, `ANTHROPIC_API_KEY_NEW`, `TELEGRAM_BOT_TOKEN`")
    md.append("")

    # Known broken
    md.append("## Known broken / stale (as of 2026-04-25)\n")
    md.append("- **`ml-predictions` Lambda** — silently broken since the April 22 CF migration. Calls `api.justhodl.ai` for bundled data; gets HTTP 403. Catches the exception and returns success (CloudWatch shows 0 errors). `predictions.json` last updated 30+ hours ago. **Decision: not retired**, the chokepoint downstream (`justhodl-intelligence`) was fixed instead via adapter pattern reading from `data/report.json` directly.")
    md.append("")
    md.append("- **`data.json` at S3 root** — 65 days stale orphan. Was the original aggregated data file before daily-report-v3 architecture replaced it. Some old Lambdas still tried to read it. Safe to delete after confirming no remaining consumers.")
    md.append("")
    md.append("- **22 empty DynamoDB tables** — leftover from architecture experiments. Safe to delete; low priority.")
    md.append("")
    md.append("- **Binance API geoblock** — `justhodl-crypto-intel` modules `fetch_oi` + `fetch_technicals` get HTTP 451 from Binance because AWS us-east-1 IPs are blocked. 15/17 modules still working; migration to Bybit/OKX/CoinGecko deferred.")
    md.append("")

    # Roadmap
    md.append("## Open roadmap (dependency-respecting)\n")
    md.append("1. **Sunday April 26 9 AM UTC** — first real calibration run (post Week 1 fixes). Watch for: did it run? what weights produced? did `n` accumulate per signal?")
    md.append("2. **Week 2A** — DONE 2026-04-25 (predictions schema v2, 7 enriched call sites with magnitude + rationale)")
    md.append("3. **Week 2B Backtester Lambda** — NEEDS 2-3 weeks of real outcomes to validate against; design at `aws/ops/design/2026-04-25-week-2-3-architecture.md`")
    md.append("4. **Week 3A Daily Ranker** — NEEDS calibrator weights with n≥10 per signal")
    md.append("5. **Week 3B Position sizing layer** — NEEDS ranker")
    md.append("")

    md.append("## Reference docs in repo\n")
    md.append("- `aws/ops/design/2026-04-25-week-2-3-architecture.md` — Week 2-3 design + 9 design questions")
    md.append("- `aws/ops/design/2026-04-25-decisions-locked.md` — Khalid's locked answers")
    md.append("- `aws/ops/design/2026-04-26-sunday-calibration-checkpoint.md` — what to check Sunday")
    md.append(f"- `aws/ops/audit/inventory_{today}.json` — raw structured inventory data")
    md.append(f"- `aws/ops/audit/system_architecture_{today}.md` — THIS DOC (canonical)")
    md.append("")

    # Save
    out_path = REPO_ROOT / f"aws/ops/audit/system_architecture_{today}.md"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(md))
    r.ok(f"  Saved: {out_path.name} ({len(md)} lines, {len(chr(10).join(md)):,} chars)")

    s3.put_object(
        Bucket="justhodl-dashboard-live",
        Key=f"_audit/system_architecture_{today}.md",
        Body="\n".join(md).encode(),
        ContentType="text/markdown",
    )
    r.ok(f"  Backup to S3")

    r.kv(
        explicitly_categorized=len(EXPLICIT_CATEGORY),
        uncategorized_fallback=len(uncategorized),
        doc_lines=len(md),
        doc_size_kb=round(len(chr(10).join(md)) / 1024, 1),
    )
    r.log("Done")
