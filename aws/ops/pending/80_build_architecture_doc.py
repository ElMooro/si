#!/usr/bin/env python3
"""
Step 80 — Build the canonical architecture doc from:
  1. The S3 inventory (Lambdas, S3 keys, DDB, SSM, EB rules)
  2. Cross-references parsed from Lambda source code (50 of 95 are
     in repo at aws/lambdas/<name>/source/lambda_function.py)

For each Lambda whose source we have, extract:
  - S3 keys it READS  (s3.get_object, http_get with bucket URL, fs3())
  - S3 keys it WRITES (s3.put_object)
  - DynamoDB tables it touches
  - HTTP endpoints it calls (urllib.request, requests)
  - Whether it has a Function URL

For Lambdas we don't have source for, just record what we know from
inventory (name, runtime, EB triggers, env keys).

Emit:
  aws/ops/audit/system_architecture_2026-04-25.md  — the canonical doc
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
    """Pull the inventory from S3 (since CI doesn't commit aws/ops/audit/ yet)."""
    obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="_audit/inventory_2026-04-25.json")
    return json.loads(obj["Body"].read())


def analyze_source(src):
    """Extract S3 reads/writes, DDB ops, HTTP calls from a Lambda source."""
    if not src:
        return {}
    info = {
        "s3_reads": set(),
        "s3_writes": set(),
        "ddb_tables": set(),
        "http_endpoints": set(),
        "lambda_invokes": set(),
    }

    # S3 reads via key= pattern
    for m in re.finditer(r"""(?:get_object|head_object).*?Key\s*=\s*['"]([^'"]+)['"]""", src):
        info["s3_reads"].add(m.group(1))
    # fs3() calls (the convention used in justhodl Lambdas)
    for m in re.finditer(r"""fs3\(['"]([^'"]+)['"]""", src):
        info["s3_reads"].add(m.group(1))
    # Bucket URL HTTP fetches
    for m in re.finditer(r"""justhodl-dashboard-live\.s3[^/]*?/([^"'\s]+)""", src):
        key = m.group(1).split("?")[0]
        if key.endswith(".json") or key.endswith(".html"):
            info["s3_reads"].add(key)

    # S3 writes
    for m in re.finditer(r"""put_object.*?Key\s*=\s*f?['"]([^'"]+)['"]""", src, re.DOTALL):
        info["s3_writes"].add(m.group(1))

    # DynamoDB
    for m in re.finditer(r"""(?:Table|table_name)\s*=?\s*['"]([^'"]+)['"]""", src):
        if "justhodl" in m.group(1) or "hodl" in m.group(1).lower():
            info["ddb_tables"].add(m.group(1))
    for m in re.finditer(r"""dynamodb\.Table\(['"]([^'"]+)['"]""", src):
        info["ddb_tables"].add(m.group(1))

    # HTTP endpoints (third-party APIs)
    for m in re.finditer(r"""https?://([a-zA-Z0-9.-]+\.(?:com|org|io|ai|net|gov))""", src):
        host = m.group(1)
        if host not in ["s3.amazonaws.com", "amazonaws.com"] and "amazonaws" not in host:
            info["http_endpoints"].add(host)

    # Lambda invokes
    for m in re.finditer(r"""invoke.*?FunctionName\s*=\s*['"]([^'"]+)['"]""", src, re.DOTALL):
        info["lambda_invokes"].add(m.group(1))

    return {k: sorted(v) for k, v in info.items()}


def kebab_or_snake(s):
    """For grouping similar names."""
    return s.replace("_", "-").lower()


with report("build_architecture_doc") as r:
    r.heading("Build canonical system architecture doc")

    inventory = load_inventory()
    r.log(f"  Inventory loaded: {len(inventory['lambdas'])} Lambdas, {len(inventory['s3_keys'])} S3 keys")

    # ─── Analyze each Lambda we have source for ───
    r.section("Analyzing Lambda sources from repo")
    lambdas_dir = REPO_ROOT / "aws/lambdas"
    src_data = {}
    sources_found = 0
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
                src_data[name]["bytes"] = len(src)
                sources_found += 1
                break
    r.log(f"  Sources analyzed: {sources_found}/{len(inventory['lambdas'])}")

    # ─── Build EB rule lookup by Lambda target ───
    r.section("Building EB rule → Lambda mapping")
    eb_by_target = defaultdict(list)
    for rule in inventory["eb_rules"]:
        for t in rule.get("targets", []):
            arn_tail = t.get("arn", "")
            # ARN format: ...:function:<name>
            if "function:" in arn_tail:
                target_name = arn_tail.split("function:")[-1]
                eb_by_target[target_name].append({
                    "rule": rule["name"],
                    "schedule": rule.get("schedule", "?"),
                    "state": rule.get("state"),
                })
            else:
                # Last 50 chars contains the name
                target_name = arn_tail.rstrip("0123456789").rstrip(":").split(":")[-1]
                if target_name:
                    eb_by_target[target_name].append({
                        "rule": rule["name"],
                        "schedule": rule.get("schedule", "?"),
                        "state": rule.get("state"),
                    })
    r.log(f"  EB → Lambda mappings: {len(eb_by_target)}")

    # ─── Categorize Lambdas by purpose ───
    r.section("Categorizing Lambdas")
    # Heuristic categorization based on name patterns + write targets
    categories = {
        "core_pipeline": [],         # The critical path
        "data_collectors": [],       # Fetch external data, write to S3
        "intelligence_agents": [],   # Read multiple sources, derive insights
        "user_facing": [],           # Function URLs, browser-callable
        "learning_loop": [],         # Calibration system
        "telegram_bot": [],
        "deprecated_or_unclear": [], # No clear purpose visible
        "legacy_openbb": [],
    }

    for fn in inventory["lambdas"]:
        n = fn["name"]
        nl = n.lower()
        srcinfo = src_data.get(n, {})

        if "openbb" in nl:
            categories["legacy_openbb"].append(n)
        elif n in ["justhodl-signal-logger", "justhodl-outcome-checker", "justhodl-calibrator"]:
            categories["learning_loop"].append(n)
        elif n in ["justhodl-daily-report-v3", "justhodl-intelligence", "justhodl-morning-intelligence",
                   "justhodl-edge-engine", "justhodl-crypto-intel", "justhodl-investor-agents",
                   "justhodl-stock-screener", "justhodl-stock-analyzer", "justhodl-valuations-agent",
                   "justhodl-options-flow"]:
            categories["core_pipeline"].append(n)
        elif n in ["justhodl-ai-chat", "justhodl-chat-api", "justhodl-bloomberg-v8",
                   "justhodl-advanced-charts", "justhodl-market-intelligence",
                   "justhodl-khalid-metrics", "cftc-futures-positioning-agent"]:
            categories["user_facing"].append(n)
        elif n == "justhodl-telegram-bot":
            categories["telegram_bot"].append(n)
        elif "agent" in nl or any(x in nl for x in ["fetcher", "collector", "scraper", "updater"]):
            categories["data_collectors"].append(n)
        elif "intelligence" in nl or "ai" in nl:
            categories["intelligence_agents"].append(n)
        else:
            categories["deprecated_or_unclear"].append(n)

    for cat, items in categories.items():
        r.log(f"    {cat}: {len(items)}")

    # ─── Build the doc ───
    r.section("Generating doc")
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    md = []
    md.append(f"# JustHodl.AI — System Architecture (canonical, {today})\n")
    md.append(f"**Generated:** {datetime.now(timezone.utc).isoformat()}  ")
    md.append(f"**Source:** [aws/ops/audit/inventory_{today}.json](inventory_{today}.json)  ")
    md.append(f"**Account:** AWS 857687956942 (us-east-1) + Cloudflare 2e120c8358c6c85dcaba07eb16947817\n")
    md.append(f"\n---\n")

    # ─── Top-of-doc summary ───
    md.append("## At a glance\n")
    md.append(f"- **{len(inventory['lambdas'])} Lambda functions** (50 in repo, 45 not yet pulled into version control)")
    md.append(f"- **{len(inventory['s3_keys'])} S3 objects** in `justhodl-dashboard-live` (capped at 5000 — actual total is higher)")
    md.append(f"- **{len(inventory['ddb_tables'])} DynamoDB tables** (only 3 actively used: `justhodl-signals`, `justhodl-outcomes`, `fed-liquidity-cache`)")
    md.append(f"- **{len(inventory['eb_rules'])} EventBridge rules** (90 enabled, 8 disabled)")
    md.append(f"- **{len(inventory['ssm_params'])} SSM parameters** under `/justhodl/`")
    md.append(f"- **1 Cloudflare Worker:** `justhodl-ai-proxy` at `api.justhodl.ai` → ai-chat Lambda\n")

    # ─── Critical path section ───
    md.append("## Critical path: how a signal becomes a calibrated weight\n")
    md.append("```")
    md.append("External APIs (FRED, Polygon, FMP, CoinGecko, etc.)")
    md.append("        │")
    md.append("        ▼")
    md.append("  ┌─────────────────────────────────┐")
    md.append("  │  data collector Lambdas (~30)   │  ← fetch + write to S3")
    md.append("  └─────────────────────────────────┘")
    md.append("        │")
    md.append("        ▼")
    md.append("  ┌──────────────────────────────────────────────────────┐")
    md.append("  │  S3 (justhodl-dashboard-live)                        │")
    md.append("  │    data/report.json   ← daily-report-v3 every 5 min  │")
    md.append("  │    crypto-intel.json  ← crypto-intel every 6h        │")
    md.append("  │    edge-data.json     ← edge-engine every 6h         │")
    md.append("  │    repo-data.json     ← repo-monitor                 │")
    md.append("  │    flow-data.json     ← options-flow                 │")
    md.append("  │    valuations-data.json ← valuations-agent monthly   │")
    md.append("  │    screener/data.json ← stock-screener every 4h      │")
    md.append("  │    intelligence-report.json ← justhodl-intelligence  │")
    md.append("  └──────────────────────────────────────────────────────┘")
    md.append("        │")
    md.append("        ▼")
    md.append("  ┌─────────────────────────────────────────┐")
    md.append("  │  justhodl-signal-logger (every 6h)      │  ← logs SIGNALS to DynamoDB")
    md.append("  │  → justhodl-signals (4,579 items)       │     with baseline_price + schema_v2")
    md.append("  └─────────────────────────────────────────┘")
    md.append("        │")
    md.append("        ▼")
    md.append("  ┌─────────────────────────────────────────┐")
    md.append("  │  justhodl-outcome-checker               │  ← evaluates signals after windows")
    md.append("  │    Mon-Fri 22:30 UTC                    │     elapse, writes outcomes")
    md.append("  │    Sun 8:00 UTC                         │")
    md.append("  │    1st of month 8:00 UTC                │")
    md.append("  │  → justhodl-outcomes (738 items)        │")
    md.append("  └─────────────────────────────────────────┘")
    md.append("        │")
    md.append("        ▼")
    md.append("  ┌─────────────────────────────────────────┐")
    md.append("  │  justhodl-calibrator (Sunday 9:00 UTC)  │  ← computes per-signal accuracy,")
    md.append("  │  → SSM /justhodl/calibration/weights    │     writes weights for 24 signals")
    md.append("  │  → SSM /justhodl/calibration/accuracy   │")
    md.append("  │  → S3 calibration/latest.json           │")
    md.append("  └─────────────────────────────────────────┘")
    md.append("```\n")

    # ─── Section by category ───
    md.append("## Lambda inventory by purpose\n")

    cat_descriptions = {
        "core_pipeline": "Core data pipeline — these produce the S3 data that everything else reads",
        "learning_loop": "Calibration system (Week 1 fix shipped 2026-04-24)",
        "user_facing": "Lambdas with Function URLs, called by the browser or Telegram bot",
        "data_collectors": "External API fetchers — write to S3 on schedule",
        "intelligence_agents": "Multi-source aggregators that produce derived insights",
        "telegram_bot": "Telegram integration",
        "legacy_openbb": "OpenBB-related Lambdas — appear unused now (consider retirement)",
        "deprecated_or_unclear": "Purpose unclear from name — investigate or retire",
    }

    # Build per-Lambda card
    eb_lookup = eb_by_target  # use built lookup
    fn_by_name = {f["name"]: f for f in inventory["lambdas"]}

    for cat, names in categories.items():
        if not names:
            continue
        md.append(f"### {cat.replace('_', ' ').title()} ({len(names)})\n")
        md.append(f"_{cat_descriptions.get(cat, '')}_\n")

        for name in sorted(names):
            fn = fn_by_name.get(name, {})
            srcinfo = src_data.get(name, {})
            schedules = [r["schedule"] for r in eb_lookup.get(name, []) if r["state"] == "ENABLED"]

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
                md.append(f"- ⚠ Source not in repo — pull via ops script if needed")
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

    # ─── DynamoDB tables ───
    md.append("## DynamoDB tables\n")
    md.append("| Table | Items | Size | Status |\n|---|---:|---:|---|")
    for t in sorted(inventory["ddb_tables"], key=lambda x: -(x.get("size_bytes") or 0)):
        size_kb = (t.get("size_bytes") or 0) / 1024
        active = "🟢 ACTIVE" if (t.get("size_bytes") or 0) > 1000 else "💤 empty"
        md.append(f"| `{t['name']}` | {t.get('item_count', '?'):,} | {size_kb:.0f}KB | {active} |")
    md.append("")
    md.append("**Active tables (real data):**")
    md.append("- `justhodl-signals` — every signal logged by signal-logger; 4,579 items")
    md.append("- `justhodl-outcomes` — scored outcomes from outcome-checker; 738 items")
    md.append("- `fed-liquidity-cache` — FRED data cache; 267k items, 19MB")
    md.append("")
    md.append("Other 22 tables are empty/dead — most were created during prior architecture experiments.")
    md.append("Safe to delete after confirming no Lambda still references them.\n")

    # ─── SSM parameters ───
    md.append("## SSM parameters\n")
    md.append("| Name | Type | Purpose |")
    md.append("|---|---|---|")
    for p in inventory["ssm_params"]:
        purpose = {
            "/justhodl/ai-chat/auth-token": "Token for ai-chat Lambda; injected by CF Worker",
            "/justhodl/calibration/accuracy": "Per-signal accuracy stats (calibrator output)",
            "/justhodl/calibration/report": "Full calibration report JSON",
            "/justhodl/calibration/weights": "Per-signal weights (consumed by ranker, future)",
            "/justhodl/telegram/chat_id": "Khalid's Telegram chat ID for bot pushes",
        }.get(p["name"], "(unknown)")
        md.append(f"| `{p['name']}` | {p['type']} | {purpose} |")
    md.append("")

    # ─── EventBridge schedules ───
    md.append("## EventBridge schedules (90 enabled)\n")
    md.append("Most-frequent firing rules (cron/rate):")
    md.append("")
    schedule_targets = []
    for rule in inventory["eb_rules"]:
        if rule.get("state") != "ENABLED":
            continue
        for t in rule.get("targets", []):
            arn_tail = t.get("arn", "")
            target = arn_tail.split("function:")[-1] if "function:" in arn_tail else arn_tail.split(":")[-1]
            schedule_targets.append({
                "rule": rule["name"],
                "schedule": rule.get("schedule", "?"),
                "target": target,
            })
    # Group by schedule pattern
    by_schedule_type = defaultdict(list)
    for st in schedule_targets:
        s = st["schedule"]
        if s.startswith("rate("):
            by_schedule_type["rate"].append(st)
        elif "MON-FRI" in s:
            by_schedule_type["weekday"].append(st)
        elif "SUN" in s or "FRI" in s or "MON" in s:
            by_schedule_type["weekly"].append(st)
        elif s.startswith("cron(0 "):
            by_schedule_type["daily"].append(st)
        else:
            by_schedule_type["other"].append(st)

    for cat in ["rate", "weekday", "weekly", "daily", "other"]:
        items = by_schedule_type.get(cat, [])
        if not items:
            continue
        md.append(f"### {cat.title()} schedules ({len(items)})\n")
        # Just show first 15 of each
        for st in sorted(items, key=lambda x: x["target"])[:15]:
            md.append(f"- `{st['schedule']}` → `{st['target']}`")
        if len(items) > 15:
            md.append(f"- ... and {len(items) - 15} more")
        md.append("")

    # ─── S3 layout ───
    md.append("## S3 layout (`justhodl-dashboard-live`)\n")
    md.append("### Public-readable paths (per bucket policy)")
    md.append("- `data/*` — primary data files (report.json, fred-cache.json, etc.)")
    md.append("- `screener/*` — stock screener output")
    md.append("- `sentiment/*` — sentiment analysis output")
    md.append("- `flow-data.json` — options/fund flow data")
    md.append("- `crypto-intel.json` — crypto intelligence")
    md.append("")
    md.append("### Private (boto3 SDK access only)")
    md.append("- `repo-data.json` — repo monitor stress scores")
    md.append("- `edge-data.json` — edge engine composite scores")
    md.append("- `intelligence-report.json` — justhodl-intelligence aggregated output")
    md.append("- `predictions.json` — STALE (30+ hours, ml-predictions writer broken since CF migration)")
    md.append("- `valuations-data.json` — monthly CAPE/Buffett indicator output")
    md.append("- `calibration/*` — calibrator history")
    md.append("- `learning/*` — signal-logger metadata")
    md.append("- `archive/*` — historical snapshots (1,665 files, 29MB)")
    md.append("")
    md.append("### Critical files (most-frequent updates)")
    md.append("| Key | Writer | Frequency | Notes |")
    md.append("|---|---|---|---|")
    md.append("| `data/report.json` | daily-report-v3 | every 5 min | The current source of truth (188 stocks + FRED + regime) |")
    md.append("| `crypto-intel.json` | crypto-intel | every 6h | BTC/ETH/SOL technicals + on-chain |")
    md.append("| `edge-data.json` | edge-engine | every 6h | Composite ML risk score, regime |")
    md.append("| `intelligence-report.json` | justhodl-intelligence | hourly weekdays | Cross-system synthesis (FIXED 2026-04-25) |")
    md.append("| `repo-data.json` | repo-monitor | every 30 min weekdays | Plumbing stress score |")
    md.append("| `flow-data.json` | options-flow | every 4h | Options flow, fund flows |")
    md.append("| `screener/data.json` | stock-screener | every 4h | 503 stocks, Piotroski/Altman scores |")
    md.append("")

    # ─── Cloudflare ───
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
    md.append("- Deploys via GitHub Actions on push (`.github/workflows/deploy-workers.yml`)")
    md.append("")

    # ─── Deploy / CI ───
    md.append("## CI/CD\n")
    md.append("**Repo:** `ElMooro/si` (justhodl.ai is GitHub Pages from this)")
    md.append("")
    md.append("**Workflows:**")
    md.append("- `deploy-lambdas.yml` — deploys any `aws/lambdas/<name>/source/` change to AWS")
    md.append("- `deploy-workers.yml` — deploys `cloudflare/workers/*/` to CF on push")
    md.append("- `run-ops.yml` — runs any `aws/ops/pending/*.py` script with AWS creds; auto-commits reports/audit/lambdas back to repo")
    md.append("- `rotate-dex-scanner-pat.yml` — manual workflow_dispatch for GitHub PAT rotation")
    md.append("")
    md.append("**IAM:** `github-actions-justhodl` — has 9 attached policies including AmazonDynamoDBReadOnlyAccess")
    md.append("**Secrets:** `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `ANTHROPIC_API_KEY`, `ANTHROPIC_API_KEY_NEW`, `TELEGRAM_BOT_TOKEN`")
    md.append("")

    # ─── Currently broken / stale ───
    md.append("## Known broken / stale (as of 2026-04-25)\n")
    md.append("- **`ml-predictions`** Lambda — silently broken since the April 22 CF Worker migration. Calls `api.justhodl.ai` for bundled data; gets HTTP 403. Lambda catches the exception and returns success (CloudWatch shows 0 errors) but `predictions.json` doesn't update. **Decision: not retired**, the chokepoint downstream (`justhodl-intelligence`) was fixed instead via adapter pattern reading from `data/report.json` directly. ml-predictions itself stays broken but no longer affects the system.")
    md.append("")
    md.append("- **`data.json`** at S3 root — 65 days stale orphan. Was the original aggregated data file before the daily-report-v3 architecture replaced it. Some old Lambdas (e.g. justhodl-intelligence pre-2026-04-25 fix) still tried to read it. Safe to delete after confirming no remaining consumers.")
    md.append("")
    md.append("- **22 empty DynamoDB tables** — leftover from architecture experiments. Safe to delete but low priority.")
    md.append("")
    md.append("- **Binance API geoblock** — `justhodl-crypto-intel` modules `fetch_oi` + `fetch_technicals` get HTTP 451 from Binance because AWS us-east-1 IPs are blocked. 15/17 modules still working; migration to Bybit/OKX/CoinGecko deferred.")
    md.append("")

    # ─── Roadmap ───
    md.append("## Open roadmap (dependency-respecting)\n")
    md.append("1. **Sunday April 26 9 AM UTC** — first real calibration run (post Week 1 fixes)")
    md.append("2. **Week 2A** — DONE 2026-04-25 (predictions schema v2, 7 enriched call sites)")
    md.append("3. **Week 2B** — Backtester Lambda, NEEDS 2-3 weeks of real outcomes")
    md.append("4. **Week 3A** — Daily Ranker, NEEDS calibrator weights with n≥10 per signal")
    md.append("5. **Week 3B** — Position sizing layer, NEEDS ranker")
    md.append("")

    # ─── Save the doc ───
    out_path = REPO_ROOT / f"aws/ops/audit/system_architecture_{today}.md"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(md))
    r.ok(f"  Saved to: aws/ops/audit/system_architecture_{today}.md ({len(md)} lines)")

    # Also save copy in S3 for safekeeping
    s3.put_object(
        Bucket="justhodl-dashboard-live",
        Key=f"_audit/system_architecture_{today}.md",
        Body="\n".join(md).encode(),
        ContentType="text/markdown",
    )
    r.ok(f"  Backup to: s3://justhodl-dashboard-live/_audit/system_architecture_{today}.md")

    r.kv(
        lambdas_documented=len(inventory["lambdas"]),
        sources_analyzed=sources_found,
        doc_lines=len(md),
        doc_size_kb=round(len("\n".join(md)) / 1024, 1),
    )
    r.log("Done")
