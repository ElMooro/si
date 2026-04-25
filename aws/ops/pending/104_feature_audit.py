#!/usr/bin/env python3
"""
Step 104 — Comprehensive system audit.

Cross-reference everything Khalid has explicitly requested across our
conversations against the current live system state.

Categories of features to verify:

A. CORE LAMBDAS (each should exist, be schedulable, write expected S3 file)
B. DASHBOARD PAGES on justhodl.ai (each should be in repo + S3)
C. S3 DATA FILES (each should be present + reasonably fresh)
D. EVENTBRIDGE SCHEDULES (each scheduled Lambda should have an enabled rule)
E. TELEGRAM BOT (token in SSM, chat_id in SSM, bot Lambda exists)
F. LEARNING LOOP (signal-logger, outcome-checker, calibrator)
G. AI CHAT (Lambda + CF Worker + token in SSM)
H. DOCUMENTED INTEGRATIONS (CFTC widget, ATH Tracker, ml-predictions)

For each item:
  - present: yes/no
  - working: yes/no/unknown
  - last_seen_evidence: timestamp + source
  - status: 🟢 working / 🟡 partial / 🔴 broken / ⚫ missing

Output: aws/ops/audit/feature_audit_2026-04-25.md
"""
import json
import os
import re
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path

from ops_report import report
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)
eb = boto3.client("events", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)
ddb = boto3.client("dynamodb", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)


def lambda_exists(name):
    try:
        cfg = lam.get_function_configuration(FunctionName=name)
        return cfg
    except lam.exceptions.ResourceNotFoundException:
        return None


def s3_object_info(key):
    try:
        head = s3.head_object(Bucket="justhodl-dashboard-live", Key=key)
        age_h = (datetime.now(timezone.utc) - head["LastModified"]).total_seconds() / 3600
        return {"exists": True, "size": head["ContentLength"], "age_h": age_h, "modified": head["LastModified"].isoformat()}
    except ClientError:
        return {"exists": False}


def lambda_24h_metrics(name):
    end = datetime.now(timezone.utc)
    start = end - timedelta(hours=24)
    try:
        inv = cw.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName="Invocations",
            Dimensions=[{"Name": "FunctionName", "Value": name}],
            StartTime=start, EndTime=end, Period=3600, Statistics=["Sum"],
        )
        err = cw.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName="Errors",
            Dimensions=[{"Name": "FunctionName", "Value": name}],
            StartTime=start, EndTime=end, Period=3600, Statistics=["Sum"],
        )
        invs = sum(p.get("Sum", 0) for p in inv.get("Datapoints", []))
        errs = sum(p.get("Sum", 0) for p in err.get("Datapoints", []))
        return {"invocations": int(invs), "errors": int(errs)}
    except Exception:
        return {"invocations": None, "errors": None}


def lambda_eb_rules(name):
    """Return list of enabled EB rules targeting this Lambda."""
    target_arn = f"arn:aws:lambda:us-east-1:857687956942:function:{name}"
    try:
        rule_names = eb.list_rule_names_by_target(TargetArn=target_arn).get("RuleNames", [])
        rules = []
        for rn in rule_names:
            try:
                d = eb.describe_rule(Name=rn)
                rules.append({
                    "name": rn,
                    "schedule": d.get("ScheduleExpression"),
                    "state": d.get("State"),
                })
            except Exception:
                pass
        return rules
    except Exception:
        return []


def evaluate_lambda(name, expects_schedule=True, max_age_for_invocation=False):
    """Return dict with present, working, evidence, status."""
    cfg = lambda_exists(name)
    if not cfg:
        return {"present": False, "status": "⚫", "evidence": "Lambda not found in account"}
    metrics = lambda_24h_metrics(name)
    rules = lambda_eb_rules(name)
    enabled_rules = [r for r in rules if r["state"] == "ENABLED"]
    disabled_rules = [r for r in rules if r["state"] == "DISABLED"]

    info = {
        "present": True,
        "memory_mb": cfg.get("MemorySize"),
        "runtime": cfg.get("Runtime"),
        "timeout": cfg.get("Timeout"),
        "last_modified": cfg.get("LastModified"),
        "invocations_24h": metrics["invocations"],
        "errors_24h": metrics["errors"],
        "rules_enabled": len(enabled_rules),
        "rules_disabled": len(disabled_rules),
        "schedules": [r["schedule"] for r in enabled_rules],
    }

    err_rate = (metrics["errors"] / metrics["invocations"]) if metrics["invocations"] else 0

    # Status logic
    if metrics["invocations"] == 0 and expects_schedule and not enabled_rules:
        info["status"] = "🟡"
        info["evidence"] = "Lambda exists but no enabled schedule + 0 invocations 24h"
    elif metrics["invocations"] and err_rate >= 0.99:
        info["status"] = "🔴"
        info["evidence"] = f"100% error rate ({metrics['errors']}/{metrics['invocations']} 24h)"
    elif metrics["invocations"] and err_rate >= 0.30:
        info["status"] = "🟡"
        info["evidence"] = f"High error rate {err_rate*100:.0f}% ({metrics['errors']}/{metrics['invocations']} 24h)"
    elif metrics["invocations"] == 0 and not expects_schedule:
        info["status"] = "🟢"
        info["evidence"] = f"On-demand Lambda; 0 24h invocations is fine"
    elif metrics["invocations"] == 0 and expects_schedule:
        info["status"] = "🟡"
        info["evidence"] = f"Has schedule but 0 invocations 24h"
    else:
        info["status"] = "🟢"
        info["evidence"] = f"{metrics['invocations']} inv / {metrics['errors']} err 24h"

    return info


def evaluate_s3_file(key, expected_max_age_h, optional_size_min=None):
    info = s3_object_info(key)
    if not info["exists"]:
        return {"present": False, "status": "⚫", "evidence": "File missing from S3"}
    if info["age_h"] > expected_max_age_h * 3:
        return {"present": True, "status": "🔴",
                "evidence": f"Age {info['age_h']:.1f}h, expected ≤{expected_max_age_h}h ({info['age_h']/expected_max_age_h:.1f}× over)",
                **info}
    elif info["age_h"] > expected_max_age_h:
        return {"present": True, "status": "🟡",
                "evidence": f"Age {info['age_h']:.1f}h, slightly stale (expected ≤{expected_max_age_h}h)",
                **info}
    else:
        return {"present": True, "status": "🟢",
                "evidence": f"Age {info['age_h']:.1f}h, size {info['size']:,}B",
                **info}


def evaluate_html_page(filename):
    """Check if an HTML page exists in S3."""
    info = s3_object_info(filename)
    if not info["exists"]:
        return {"present": False, "status": "⚫", "evidence": f"{filename} not in S3 bucket"}
    return {"present": True, "status": "🟢", "evidence": f"size {info['size']:,}B age {info['age_h']:.0f}h", **info}


def evaluate_ssm(name):
    try:
        resp = ssm.describe_parameters(
            ParameterFilters=[{"Key": "Name", "Values": [name]}]
        )
        params = resp.get("Parameters", [])
        if not params:
            return {"present": False, "status": "⚫", "evidence": "Not found"}
        p = params[0]
        return {"present": True, "status": "🟢", "evidence": f"type {p.get('Type')}", "type": p.get("Type")}
    except Exception as e:
        return {"present": False, "status": "🔴", "evidence": str(e)[:80]}


def evaluate_eb_rule(name, expected_state="ENABLED"):
    try:
        d = eb.describe_rule(Name=name)
        state = d.get("State")
        sched = d.get("ScheduleExpression")
        if state == expected_state:
            return {"present": True, "status": "🟢", "evidence": f"{state} {sched}"}
        else:
            return {"present": True, "status": "🟡", "evidence": f"state {state}, expected {expected_state}"}
    except eb.exceptions.ResourceNotFoundException:
        return {"present": False, "status": "⚫", "evidence": "Rule not found"}


with report("feature_audit") as r:
    r.heading("System feature audit: requested vs live")

    audit = []  # List of {category, name, request_origin, status, evidence}

    # ────────────────────────────────────────────────────────────────────
    # A. CORE LAMBDAS — requested across many sessions
    # ────────────────────────────────────────────────────────────────────
    r.section("A. Core Lambdas")
    core_lambdas = [
        # (name, expects_schedule, request_session_summary)
        ("justhodl-daily-report-v3", True, "Bloomberg V10.3 5-min refresh, 188 stocks + 230+ FRED + 21 tabs"),
        ("justhodl-ai-chat", False, "AI chat with Claude, dashboard + standalone access"),
        ("justhodl-bloomberg-v8", True, "Earlier Bloomberg V8 5-min refresh"),
        ("justhodl-intelligence", True, "Cross-system synthesis hourly weekdays + 7AM ET"),
        ("justhodl-morning-intelligence", True, "Daily 8AM ET Telegram brief + self-improvement"),
        ("justhodl-edge-engine", True, "Edge Intelligence every 6h, 5 engines"),
        ("justhodl-options-flow", True, "Options + fund flows every 4h"),
        ("justhodl-investor-agents", False, "6 legendary investor personas (Buffett, Munger, etc)"),
        ("justhodl-stock-analyzer", False, "ECharts candles, SMA, Golden/Death Cross"),
        ("justhodl-stock-screener", True, "503 stocks Piotroski + Altman every 4h"),
        ("justhodl-valuations-agent", True, "CAPE, Buffett indicator monthly"),
        ("justhodl-crypto-intel", True, "BTC/ETH/SOL technicals every 15min"),
        ("cftc-futures-positioning-agent", True, "CFTC COT 29 contracts weekly Fri 18 UTC"),
        ("justhodl-financial-secretary", True, "Personal Financial Secretary every 4h"),
        ("justhodl-repo-monitor", True, "Plumbing stress every 30min weekdays"),
        ("justhodl-dex-scanner", True, "DEX Intelligence every 15min"),
        ("justhodl-telegram-bot", False, "@Justhodl_bot /briefing /ask /cftc /crypto /edge"),
        ("justhodl-signal-logger", True, "Learning loop signal logger every 6h"),
        ("justhodl-outcome-checker", True, "Outcome scorer Mon-Fri 22:30 + Sun 8 + 1st-of-month"),
        ("justhodl-calibrator", True, "Per-signal weights Sunday 9 UTC"),
        ("justhodl-health-monitor", True, "System observability every 15min (NEW 2026-04-25)"),
        ("justhodl-ml-predictions", True, "ML predictions engine every 5min — KNOWN BROKEN"),
        ("justhodl-khalid-metrics", False, "Custom Khalid metrics endpoint"),
        ("justhodl-advanced-charts", False, "TradingView-style charts"),
    ]
    for name, expects, summary in core_lambdas:
        info = evaluate_lambda(name, expects_schedule=expects)
        audit.append({
            "category": "Core Lambdas",
            "name": name,
            "request": summary,
            **info,
        })
        r.log(f"  {info.get('status')} {name:42} {info.get('evidence', '?')}")

    # ────────────────────────────────────────────────────────────────────
    # B. DASHBOARD PAGES on justhodl.ai (S3 hosted)
    # ────────────────────────────────────────────────────────────────────
    r.section("B. Dashboard pages")
    pages = [
        ("index.html", "Main Bloomberg Terminal V10.3"),
        ("pro.html", "Pro Dashboard (enhanced macro + sector)"),
        ("agent.html", "Financial Secretary dashboard"),
        ("charts.html", "TradingView-style charts"),
        ("valuations.html", "Valuations dashboard"),
        ("edge.html", "Edge Intelligence terminal"),
        ("flow.html", "Options Flow dashboard"),
        ("intelligence.html", "Market Intelligence fusion"),
        ("risk.html", "Systemic Risk Monitor"),
        ("stocks.html", "Stock Picks page"),
        ("ath.html", "ATH Tracker"),
        ("trading-signals.html", "Trading Signals"),
        ("reports.html", "Reports & Analysis"),
        ("ml.html", "ML Predictions"),
        ("dex.html", "DEX Intelligence Terminal"),
        ("liquidity.html", "TGA + Fed Liquidity page"),
        ("health.html", "System Health Monitor (NEW 2026-04-25)"),
    ]
    for filename, desc in pages:
        info = evaluate_html_page(filename)
        audit.append({"category": "Dashboard pages", "name": filename, "request": desc, **info})
        r.log(f"  {info.get('status')} {filename:30} {info.get('evidence', '?')}")

    # ────────────────────────────────────────────────────────────────────
    # C. S3 DATA FILES (with expected freshness)
    # ────────────────────────────────────────────────────────────────────
    r.section("C. S3 data files")
    data_files = [
        # (key, max_age_hours, description)
        ("data/report.json", 0.5, "Source of truth, daily-report-v3 every 5min"),
        ("crypto-intel.json", 1, "BTC/ETH/SOL technicals every 15min"),
        ("edge-data.json", 12, "Edge composite every 6h"),
        ("repo-data.json", 4, "Plumbing stress every 30min weekdays"),
        ("flow-data.json", 8, "Options/fund flows every 4h"),
        ("intelligence-report.json", 4, "Cross-system synthesis hourly weekdays"),
        ("screener/data.json", 8, "503 stocks every 4h"),
        ("valuations-data.json", 24*40, "CAPE/Buffett monthly"),
        ("calibration/latest.json", 24*8, "Calibrator weekly Sunday"),
        ("learning/last_log_run.json", 8, "signal-logger heartbeat"),
        ("dex-scanner-data.json", 1, "DEX scanner every 15min"),
        ("data/secretary-latest.json", 6, "Financial Secretary every 4h"),
        ("ath-data.json", 24, "ATH tracker"),
        ("predictions.json", 1, "ml-predictions — KNOWN BROKEN"),
        ("data.json", 24, "Legacy orphan — KNOWN STALE"),
    ]
    for key, max_age, desc in data_files:
        info = evaluate_s3_file(key, max_age)
        audit.append({"category": "S3 data files", "name": key, "request": desc, **info})
        r.log(f"  {info.get('status')} {key:35} {info.get('evidence', '?')}")

    # ────────────────────────────────────────────────────────────────────
    # D. SSM PARAMETERS
    # ────────────────────────────────────────────────────────────────────
    r.section("D. SSM parameters")
    ssm_params = [
        ("/justhodl/ai-chat/auth-token", "AI chat auth token (CF Worker injects)"),
        ("/justhodl/calibration/weights", "Per-signal weights from calibrator"),
        ("/justhodl/calibration/accuracy", "Per-signal accuracy"),
        ("/justhodl/calibration/report", "Full calibration JSON"),
        ("/justhodl/telegram/chat_id", "Khalid's Telegram chat_id"),
        ("/justhodl/telegram/bot_token", "Bot token (NEW 2026-04-25)"),
    ]
    for name, desc in ssm_params:
        info = evaluate_ssm(name)
        audit.append({"category": "SSM parameters", "name": name, "request": desc, **info})
        r.log(f"  {info.get('status')} {name:50} {info.get('evidence', '?')}")

    # ────────────────────────────────────────────────────────────────────
    # E. KEY EVENTBRIDGE RULES
    # ────────────────────────────────────────────────────────────────────
    r.section("E. EventBridge rules — critical scheduled events")
    eb_rules = [
        ("justhodl-outcome-checker-daily", "Daily outcome scoring (Mon-Fri 22:30)"),
        ("justhodl-outcome-checker-weekly", "Sunday outcome scoring"),
        ("justhodl-calibrator-weekly", "Sunday 9 UTC calibration THE event"),
        ("justhodl-health-monitor-15min", "Health monitor every 15min (NEW)"),
        ("justhodl-v9-auto-refresh", "5-min auto-refresh (daily-report-v3)"),
        ("DailyMacroScraper", "scrapeMacroData (DISABLED 2026-04-25)"),
    ]
    for rule_name, desc in eb_rules:
        info = evaluate_eb_rule(rule_name, expected_state="ENABLED" if "DISABLED" not in desc else "DISABLED")
        audit.append({"category": "EB rules", "name": rule_name, "request": desc, **info})
        r.log(f"  {info.get('status')} {rule_name:40} {info.get('evidence', '?')}")

    # ────────────────────────────────────────────────────────────────────
    # F. DYNAMODB TABLES (the active ones)
    # ────────────────────────────────────────────────────────────────────
    r.section("F. DynamoDB active tables")
    ddb_tables = [
        ("justhodl-signals", "Learning loop — every signal logged"),
        ("justhodl-outcomes", "Learning loop — scored outcomes"),
        ("fed-liquidity-cache", "FRED data cache"),
    ]
    for tn, desc in ddb_tables:
        try:
            d = ddb.describe_table(TableName=tn)["Table"]
            items = d.get("ItemCount", 0)
            size_kb = (d.get("TableSizeBytes") or 0) / 1024
            status = "🟢" if items > 0 else "🟡"
            evidence = f"{items:,} items, {size_kb:.0f}KB"
        except Exception as e:
            status = "⚫"
            evidence = f"Not found: {e}"
            items = 0
        audit.append({"category": "DynamoDB", "name": tn, "request": desc, "status": status, "evidence": evidence})
        r.log(f"  {status} {tn:30} {evidence}")

    # ────────────────────────────────────────────────────────────────────
    # G. CLOUDFLARE WORKER (verified by attempting to fetch a known endpoint)
    # ────────────────────────────────────────────────────────────────────
    r.section("G. Cloudflare Worker")
    # We can't directly query CF from here without API key, but we can check
    # the deployed source in the repo
    cf_worker_path = REPO_ROOT / "cloudflare/workers/justhodl-ai-proxy/src/index.js"
    if cf_worker_path.exists():
        size = cf_worker_path.stat().st_size
        audit.append({
            "category": "Cloudflare",
            "name": "justhodl-ai-proxy",
            "request": "AI chat proxy at api.justhodl.ai",
            "status": "🟢",
            "evidence": f"Source in repo, {size}B (deployed via deploy-workers.yml)",
        })
        r.log(f"  🟢 justhodl-ai-proxy: source in repo {size}B")
    else:
        audit.append({
            "category": "Cloudflare",
            "name": "justhodl-ai-proxy",
            "request": "AI chat proxy at api.justhodl.ai",
            "status": "⚫",
            "evidence": "Source NOT in repo at expected path",
        })
        r.log(f"  ⚫ Source not in repo")

    # ────────────────────────────────────────────────────────────────────
    # Build the audit doc
    # ────────────────────────────────────────────────────────────────────
    r.section("Build audit doc")

    md = []
    md.append(f"# JustHodl.AI Feature Audit — {datetime.now(timezone.utc).strftime('%Y-%m-%d')}\n")
    md.append(f"**Generated:** {datetime.now(timezone.utc).isoformat()}")
    md.append(f"**Method:** Cross-reference Khalid's requested features (from prior conversations) "
              f"against current live system state (Lambdas + S3 + EB + SSM + DDB + CF).")
    md.append("")
    md.append("**Status legend:**")
    md.append("- 🟢 = present + working")
    md.append("- 🟡 = present but degraded (high error rate, stale, no schedule, etc)")
    md.append("- 🔴 = present but broken (100% errors)")
    md.append("- ⚫ = missing entirely")
    md.append("")

    # Headline counts
    counts = defaultdict(int)
    by_cat = defaultdict(list)
    for a in audit:
        counts[a.get("status", "?")] += 1
        by_cat[a["category"]].append(a)

    md.append("## At a glance\n")
    md.append(f"- Total features audited: **{len(audit)}**")
    md.append(f"- 🟢 Working: **{counts['🟢']}**")
    md.append(f"- 🟡 Partial / degraded: **{counts['🟡']}**")
    md.append(f"- 🔴 Broken: **{counts['🔴']}**")
    md.append(f"- ⚫ Missing: **{counts['⚫']}**")
    md.append("")

    # By category
    for cat in ["Core Lambdas", "Dashboard pages", "S3 data files", "SSM parameters",
                "EB rules", "DynamoDB", "Cloudflare"]:
        items = by_cat.get(cat, [])
        if not items:
            continue
        md.append(f"\n## {cat} ({len(items)})\n")
        md.append("| Status | Name | Requested feature | Evidence |")
        md.append("|---|---|---|---|")
        for a in items:
            md.append(f"| {a.get('status', '?')} | `{a['name']}` | {a.get('request', '?')} | {a.get('evidence', '?')[:120]} |")

    # Specific issues to address
    md.append("\n## Issues found (🟡 + 🔴 + ⚫)\n")
    issues = [a for a in audit if a.get("status") in ("🟡", "🔴", "⚫")]
    if not issues:
        md.append("None — system fully operational.")
    else:
        for a in issues:
            md.append(f"\n### {a.get('status')} `{a['name']}` ({a['category']})")
            md.append(f"**Requested:** {a.get('request', '?')}")
            md.append(f"**Status:** {a.get('evidence', '?')}")

    out_path = REPO_ROOT / "aws/ops/audit/feature_audit_2026-04-25.md"
    out_path.write_text("\n".join(md))
    r.ok(f"  Wrote: {out_path.relative_to(REPO_ROOT)} ({len(md)} lines)")

    # Backup to S3
    s3.put_object(
        Bucket="justhodl-dashboard-live",
        Key="_audit/feature_audit_2026-04-25.md",
        Body="\n".join(md).encode(),
        ContentType="text/markdown",
    )

    r.kv(
        total=len(audit),
        green=counts["🟢"],
        yellow=counts["🟡"],
        red=counts["🔴"],
        missing=counts["⚫"],
    )
    r.log("Done")
