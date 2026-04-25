#!/usr/bin/env python3
"""
Step 92 — AWS cost audit. Identify what's spending money and propose
SAFE-to-cut optimizations without breaking the system.

Pulls 30 days of cost data from Cost Explorer, then analyzes:
  1. Total spend + breakdown by service (Lambda / S3 / DDB / CloudWatch / etc.)
  2. Top 20 Lambdas by GB-seconds (the actual cost driver)
  3. CloudWatch logs storage size per log group (top 20)
  4. S3 bucket sizes
  5. DynamoDB tables consumption
  6. EventBridge rule frequency (high-frequency = high cost)

Then proposes SPECIFIC actions, ranked by:
  - Risk (none/low/medium/high)
  - Estimated monthly $ saved
  - Implementation difficulty

Output: aws/ops/audit/cost_audit_2026-04-25.md
"""
import json
import os
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

ce = boto3.client("ce", region_name="us-east-1")
lam = boto3.client("lambda", region_name=REGION)
cw_logs = boto3.client("logs", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
ddb = boto3.client("dynamodb", region_name=REGION)
eb = boto3.client("events", region_name=REGION)


with report("cost_audit") as r:
    r.heading("AWS cost audit + optimization recommendations")

    today = datetime.now(timezone.utc).date()
    start_30d = today - timedelta(days=30)

    # ─── 1. Cost by service (last 30 days) ─────────────────────────────
    r.section("1. Total spend by service (last 30 days)")
    service_costs = {}
    try:
        resp = ce.get_cost_and_usage(
            TimePeriod={"Start": start_30d.isoformat(), "End": today.isoformat()},
            Granularity="MONTHLY",
            Metrics=["UnblendedCost"],
            GroupBy=[{"Type": "DIMENSION", "Key": "SERVICE"}],
        )
        for result in resp.get("ResultsByTime", []):
            for grp in result.get("Groups", []):
                svc = grp["Keys"][0]
                amount = float(grp["Metrics"]["UnblendedCost"]["Amount"])
                service_costs[svc] = service_costs.get(svc, 0) + amount

        total = sum(service_costs.values())
        r.log(f"  Total 30-day spend: ${total:.2f}")
        r.log(f"  Monthly run rate:   ~${total:.2f}/mo (target: $30/mo)")
        r.log("")
        for svc, amt in sorted(service_costs.items(), key=lambda x: -x[1])[:15]:
            pct = (amt / total * 100) if total else 0
            bar = "█" * int(pct / 2)
            r.log(f"    ${amt:>7.2f}  {pct:>4.1f}%  {bar:<25}  {svc}")
    except Exception as e:
        r.warn(f"  Cost Explorer fetch failed: {e}")

    # ─── 2. Top Lambdas by GB-seconds (last 30 days) ───────────────────
    r.section("2. Top 20 Lambdas by GB-seconds (the cost driver)")
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=30)
    paginator = lam.get_paginator("list_functions")
    lambda_costs = []
    for page in paginator.paginate():
        for fn in page.get("Functions", []):
            name = fn["FunctionName"]
            mem_mb = fn.get("MemorySize", 128)
            try:
                # Get total invocations (sum)
                inv = cw.get_metric_statistics(
                    Namespace="AWS/Lambda", MetricName="Invocations",
                    Dimensions=[{"Name": "FunctionName", "Value": name}],
                    StartTime=start, EndTime=end, Period=86400, Statistics=["Sum"],
                )
                inv_total = sum(p.get("Sum", 0) for p in inv.get("Datapoints", []))

                # Get total duration (in milliseconds, summed)
                dur = cw.get_metric_statistics(
                    Namespace="AWS/Lambda", MetricName="Duration",
                    Dimensions=[{"Name": "FunctionName", "Value": name}],
                    StartTime=start, EndTime=end, Period=86400, Statistics=["Sum"],
                )
                dur_total_ms = sum(p.get("Sum", 0) for p in dur.get("Datapoints", []))
                gb_seconds = (dur_total_ms / 1000) * (mem_mb / 1024)
                # Lambda free tier: 400,000 GB-s/mo. After: $0.0000166667 per GB-s
                approx_cost = max(0, gb_seconds - 400_000) * 0.0000166667

                lambda_costs.append({
                    "name": name,
                    "mem_mb": mem_mb,
                    "invocations": int(inv_total),
                    "duration_ms_total": dur_total_ms,
                    "gb_seconds": gb_seconds,
                    "approx_cost": approx_cost,
                })
            except Exception as e:
                pass

    lambda_costs.sort(key=lambda x: -x["gb_seconds"])
    total_gbs = sum(l["gb_seconds"] for l in lambda_costs)
    r.log(f"  Total Lambda GB-seconds (30d): {total_gbs:,.0f}")
    r.log(f"  Free tier:                     400,000 GB-s/mo")
    r.log(f"  Over free tier:                {max(0, total_gbs - 400_000):,.0f} GB-s")
    r.log("")
    r.log("  name                                     mem  inv-30d  GB-s    avg-ms")
    for l in lambda_costs[:20]:
        avg_ms = l["duration_ms_total"] / l["invocations"] if l["invocations"] else 0
        r.log(f"    {l['name']:40} {l['mem_mb']:>4}  {l['invocations']:>7}  {l['gb_seconds']:>6.0f}  {avg_ms:>6.0f}")

    # ─── 3. CloudWatch Logs storage (often a hidden cost) ──────────────
    r.section("3. Top 20 CloudWatch Log Groups by stored bytes")
    try:
        log_groups = []
        paginator = cw_logs.get_paginator("describe_log_groups")
        for page in paginator.paginate():
            for lg in page.get("logGroups", []):
                log_groups.append({
                    "name": lg.get("logGroupName"),
                    "stored_bytes": lg.get("storedBytes", 0),
                    "retention_days": lg.get("retentionInDays"),
                })
        log_groups.sort(key=lambda x: -x["stored_bytes"])
        total_log_gb = sum(l["stored_bytes"] for l in log_groups) / 1024**3
        r.log(f"  Total log storage: {total_log_gb:.2f} GB")
        r.log(f"  Free tier: 5 GB")
        r.log(f"  Over: {max(0, total_log_gb - 5):.2f} GB at $0.03/GB/mo = ${max(0, total_log_gb - 5) * 0.03:.2f}")
        r.log("")
        r.log("  name                                                bytes      retention")
        no_retention = []
        for lg in log_groups[:20]:
            size_mb = lg["stored_bytes"] / 1024**2
            ret = f"{lg['retention_days']}d" if lg["retention_days"] else "FOREVER"
            r.log(f"    {lg['name']:55} {size_mb:>7.1f}MB  {ret}")
            if not lg["retention_days"]:
                no_retention.append(lg["name"])
        r.log(f"\n  Log groups with NO retention policy: {len(no_retention)}/{len(log_groups)}")
        r.log(f"  ↑ Setting these to 14d retention is a common safe cost win.")
    except Exception as e:
        r.warn(f"  Log group fetch failed: {e}")

    # ─── 4. S3 bucket sizes ────────────────────────────────────────────
    r.section("4. S3 bucket sizes")
    try:
        # justhodl-dashboard-live size from CloudWatch metric
        end_d = datetime.now(timezone.utc)
        start_d = end_d - timedelta(days=2)
        size_metric = cw.get_metric_statistics(
            Namespace="AWS/S3", MetricName="BucketSizeBytes",
            Dimensions=[
                {"Name": "BucketName", "Value": "justhodl-dashboard-live"},
                {"Name": "StorageType", "Value": "StandardStorage"},
            ],
            StartTime=start_d, EndTime=end_d, Period=86400, Statistics=["Average"],
        )
        if size_metric.get("Datapoints"):
            latest = max(size_metric["Datapoints"], key=lambda p: p["Timestamp"])
            size_gb = latest["Average"] / 1024**3
            r.log(f"  justhodl-dashboard-live: {size_gb:.2f} GB")
            # Standard storage: $0.023/GB/mo
            r.log(f"  Estimated cost: ${size_gb * 0.023:.2f}/mo")
    except Exception as e:
        r.warn(f"  S3 size: {e}")

    # ─── 5. DynamoDB consumption ───────────────────────────────────────
    r.section("5. DynamoDB tables (active ones only)")
    try:
        tables = []
        paginator = ddb.get_paginator("list_tables")
        for page in paginator.paginate():
            for tn in page.get("TableNames", []):
                td = ddb.describe_table(TableName=tn)["Table"]
                tables.append({
                    "name": tn,
                    "size_kb": (td.get("TableSizeBytes") or 0) / 1024,
                    "items": td.get("ItemCount", 0),
                    "billing": td.get("BillingModeSummary", {}).get("BillingMode", "?"),
                })
        active = [t for t in tables if t["size_kb"] > 0]
        empty = [t for t in tables if t["size_kb"] == 0]
        r.log(f"  Active tables: {len(active)}, empty: {len(empty)}")
        for t in sorted(active, key=lambda x: -x["size_kb"]):
            r.log(f"    {t['name']:40} {t['size_kb']:>10.0f}KB  items={t['items']:>10}  {t['billing']}")
        r.log(f"\n  Empty tables (cleanup candidates):")
        for t in empty:
            r.log(f"    {t['name']}")
    except Exception as e:
        r.warn(f"  DDB: {e}")

    # ─── 6. EventBridge frequency ──────────────────────────────────────
    r.section("6. High-frequency EventBridge rules (cost driver via downstream Lambda invocations)")
    try:
        rules = []
        paginator = eb.get_paginator("list_rules")
        for page in paginator.paginate():
            for rule in page.get("Rules", []):
                if rule.get("State") != "ENABLED":
                    continue
                sched = rule.get("ScheduleExpression", "")
                # Estimate frequency
                freq_per_day = None
                if sched.startswith("rate("):
                    import re as _re
                    m = _re.match(r"rate\((\d+)\s*(\w+)\)", sched)
                    if m:
                        n = int(m.group(1))
                        unit = m.group(2).rstrip("s").lower()
                        freq_per_day = {"minute": 1440/n, "hour": 24/n, "day": 1.0/n}.get(unit)
                rules.append({"name": rule["Name"], "schedule": sched, "freq_per_day": freq_per_day})
        rules.sort(key=lambda x: -(x["freq_per_day"] or 0))
        r.log(f"  Top 15 by frequency:")
        for rule in rules[:15]:
            freq = f"{rule['freq_per_day']:.0f}/day" if rule["freq_per_day"] else "?"
            r.log(f"    {rule['schedule']:35} {freq:>10}  {rule['name']}")
    except Exception as e:
        r.warn(f"  EB: {e}")

    # ─── Build recommendations doc ─────────────────────────────────────
    r.section("Recommendations")

    md = []
    md.append("# JustHodl.AI — Cost Audit & Optimization Plan\n")
    md.append(f"**Generated:** {datetime.now(timezone.utc).isoformat()}\n")
    md.append("**Scope:** Last 30 days of AWS spend, Lambda usage, log storage, S3, DynamoDB, EventBridge\n")
    md.append("\n---\n")

    md.append("## Spend summary\n")
    if total:
        md.append(f"- **30-day total:** ${total:.2f} (~${total:.0f}/mo)")
        md.append(f"- **Target:** $30/mo")
        md.append(f"- **Headroom:** {'over' if total > 30 else 'under'} target by ${abs(total - 30):.2f}")
    md.append("\n## Top services by cost\n")
    md.append("| Service | $30d | % |")
    md.append("|---|---:|---:|")
    for svc, amt in sorted(service_costs.items(), key=lambda x: -x[1])[:8]:
        pct = (amt / total * 100) if total else 0
        md.append(f"| {svc} | ${amt:.2f} | {pct:.1f}% |")
    md.append("")

    md.append("## Recommendations (ranked by safety × $-saved)\n")

    # Build specific recs based on what we found
    recs = []

    # Rec 1: Log retention
    if 'no_retention' in dir() and len(no_retention) > 0:
        recs.append({
            "title": "Set 14-day retention on all CloudWatch Log Groups",
            "risk": "NONE",
            "saves": f"~${max(0, total_log_gb - 5) * 0.03 * 0.7:.2f}/mo (proportional reduction)",
            "effort": "5 min",
            "details": (
                f"Currently {len(no_retention)} log groups have NO retention (logs accumulate forever). "
                f"Total log storage: {total_log_gb:.1f} GB. "
                "Setting 14-day retention is the AWS recommended default for application logs. "
                "Doesn't affect any Lambda or system function — only deletes old logs."
            ),
            "command": (
                "for lg in $(aws logs describe-log-groups --query 'logGroups[?retentionInDays==null].logGroupName' --output text); do\n"
                "  aws logs put-retention-policy --log-group-name \"$lg\" --retention-in-days 14\n"
                "done"
            ),
        })

    # Rec 2: Stop the broken Lambdas
    recs.append({
        "title": "Stop the 7 Lambdas at 100% error rate",
        "risk": "LOW (they're already broken, just costing money)",
        "saves": "Stops futile invocations + their CloudWatch costs",
        "effort": "10 min",
        "details": (
            "These 7 Lambdas have been at 100% error rate for 7+ days, found by the health monitor:\n"
            "- global-liquidity-agent-v2 (439 inv/7d, 100% err)\n"
            "- news-sentiment-agent (439 inv/7d, 100% err)\n"
            "- fmp-stock-picks-agent (90 inv/7d, 100% err)\n"
            "- daily-liquidity-report (21 inv/7d, 100% err)\n"
            "- ecb-data-daily-updater (21 inv/7d, 100% err)\n"
            "- scrapeMacroData (21 inv/7d, 100% err)\n"
            "- treasury-auto-updater (6 inv/7d, 100% err)\n\n"
            "Either fix them OR disable their EventBridge schedules until fixed. "
            "Disabling stops the bleeding without deleting code."
        ),
        "command": (
            "# Disable EB rules (reversible):\n"
            "for rule in <rule-name>; do\n"
            "  aws events disable-rule --name \"$rule\"\n"
            "done"
        ),
    })

    # Rec 3: 22 empty DynamoDB tables
    recs.append({
        "title": "Delete 22 empty DynamoDB tables (architecture-experiment leftovers)",
        "risk": "LOW (they're empty; no Lambda actively reads/writes)",
        "saves": "~$0 direct (PAY_PER_REQUEST = no idle cost) but reduces console clutter and bills if accessed accidentally",
        "effort": "15 min",
        "details": (
            "Empty tables identified by health monitor + architecture audit. "
            "Pay-per-request billing means no idle cost, but cleanup is good hygiene. "
            "Deferred unless you want clean console."
        ),
    })

    # Rec 4: High-frequency EB rules
    recs.append({
        "title": "Review high-frequency EventBridge rules",
        "risk": "MEDIUM (changes system behavior)",
        "saves": "Potentially significant — Lambda invocation cost is ~$0.20/M invocations",
        "effort": "30 min review",
        "details": (
            "Some Lambdas fire every 5 minutes (288/day). Review whether each one really needs that cadence. "
            "For example, justhodl-bloomberg-v8 fires every 5 min — does the data change that fast? "
            "Halving the frequency to every 10 min would halve the cost AND reduce FRED/Polygon API rate-limit pressure."
        ),
    })

    # Rec 5: Lambda right-sizing
    recs.append({
        "title": "Right-size Lambda memory allocations",
        "risk": "LOW (overprovisioning is safe, underprovisioning could timeout)",
        "saves": f"Lambda cost scales linearly with memory. Top 5 Lambdas use {sum(l['gb_seconds'] for l in lambda_costs[:5]):,.0f} GB-s/30d combined.",
        "effort": "1 hour",
        "details": (
            "AWS Lambda Power Tuning tool can identify the right memory size. "
            "Generally: faster execution at higher memory can offset the higher per-second cost. "
            "Sweet spot is usually 512MB-1024MB for I/O-bound Lambdas. "
            "Check justhodl-crypto-intel (1024MB) and justhodl-daily-report-v3 — these are top spenders."
        ),
    })

    # Rec 6: archive/ folder cleanup
    recs.append({
        "title": "Lifecycle policy on archive/* (move to S3 Glacier after 90 days)",
        "risk": "NONE (archive data isn't actively read)",
        "saves": "Glacier is ~$0.004/GB/mo vs Standard $0.023/GB/mo (5.7x cheaper)",
        "effort": "10 min",
        "details": (
            "S3 archive/ prefix has 1,665 files / 29MB historical snapshots. "
            "Add lifecycle rule: archive/* → Glacier Deep Archive after 90 days. "
            "Doesn't affect read access (just slower retrieval if ever needed)."
        ),
        "command": (
            "aws s3api put-bucket-lifecycle-configuration --bucket justhodl-dashboard-live --lifecycle-configuration '{\n"
            "  \"Rules\": [{\n"
            "    \"ID\": \"archive-to-glacier\",\n"
            "    \"Filter\": {\"Prefix\": \"archive/\"},\n"
            "    \"Status\": \"Enabled\",\n"
            "    \"Transitions\": [{\"Days\": 90, \"StorageClass\": \"GLACIER\"}]\n"
            "  }]\n"
            "}'"
        ),
    })

    for i, rec in enumerate(recs, 1):
        md.append(f"### {i}. {rec['title']}\n")
        md.append(f"- **Risk:** {rec['risk']}")
        md.append(f"- **Saves:** {rec['saves']}")
        md.append(f"- **Effort:** {rec['effort']}\n")
        md.append(rec["details"])
        if rec.get("command"):
            md.append(f"\n```bash\n{rec['command']}\n```")
        md.append("")

    md.append("\n## Decision matrix\n")
    md.append("| Rec | Risk | Action |")
    md.append("|---|---|---|")
    md.append("| 1. Log retention 14d | NONE | Do it now (cron-safe) |")
    md.append("| 2. Disable broken Lambda EB rules | LOW | Investigate first; disable while debugging |")
    md.append("| 3. Delete 22 empty DDB tables | LOW | Defer until quarterly cleanup |")
    md.append("| 4. Review EB frequencies | MED | Worth a 30-min audit |")
    md.append("| 5. Right-size Lambda memory | LOW | Defer; only for top spenders |")
    md.append("| 6. Lifecycle on archive/ | NONE | Do it now |")
    md.append("")

    md.append("\n## What NOT to touch\n")
    md.append("- **`justhodl-daily-report-v3` (5-min cadence)** — heart of the system. If anything, this could fire less frequently if 188 stocks don't move that fast, but the user-facing dashboard depends on it.")
    md.append("- **`justhodl-signal-logger` cadence** — it's the calibration data feeder. Don't change.")
    md.append("- **`justhodl-health-monitor` (15-min cadence)** — observability. Cost is negligible; benefit is enormous.")
    md.append("- **DynamoDB tables `justhodl-signals` and `justhodl-outcomes`** — your training data. Never delete.")

    out_path = REPO_ROOT / "aws/ops/audit/cost_audit_2026-04-25.md"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(md))
    r.ok(f"  Wrote audit doc to aws/ops/audit/cost_audit_2026-04-25.md ({len(md)} lines)")

    # Backup to S3
    s3.put_object(
        Bucket="justhodl-dashboard-live",
        Key="_audit/cost_audit_2026-04-25.md",
        Body="\n".join(md).encode(),
        ContentType="text/markdown",
    )

    r.kv(
        total_30d_spend=f"${total:.2f}",
        log_storage_gb=f"{total_log_gb:.1f}",
        recs_count=len(recs),
    )
    r.log("Done")
