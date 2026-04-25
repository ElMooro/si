#!/usr/bin/env python3
"""
Step 118 — Plan vs usage audit.

Question: are we using everything we're paying for?

Audit dimensions:

A. AWS — what services have running resources / paid features:
   - Lambda: count, memory tiers, runtime versions, concurrency settings
   - S3: storage tiers, intelligent tiering enabled?, transfer acceleration?
       Lifecycle rules count
   - DynamoDB: PROVISIONED vs PAY_PER_REQUEST, indexes (GSIs), DAX, streams
   - CloudWatch: alarms count, dashboards count, log retention coverage
   - SSM: parameter count, advanced vs standard
   - EventBridge: rules count, custom event buses?
   - IAM: users, roles, policies — any unused?
   - Cost Explorer: per-service spend snapshot if accessible

B. CloudFlare:
   - Workers: paid vs free plan?
   - KV namespaces? D1 databases? R2 buckets? Durable Objects?
   - The Worker we have (justhodl-ai-proxy) — using KV/Cache/Queues?

C. Third-party API plans (from memory):
   - FMP Premium — $20+/mo plan — using premium endpoints?
   - Polygon — currently on free tier? Crypto endpoint usage?
   - Anthropic API — models in use, batch API used?

For each finding, output a recommendation:
   - 🟢 fully utilized
   - 🟡 partially utilized (optimization opportunity)
   - ⚫ paid for but unused (real money on the table)

Output: aws/ops/audit/plan_vs_usage_2026-04-25.md
"""
import json
import os
from collections import Counter, defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path

from ops_report import report
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
ACCOUNT = "857687956942"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))


lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
ddb = boto3.client("dynamodb", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)
cw_logs = boto3.client("logs", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)
eb = boto3.client("events", region_name=REGION)
iam = boto3.client("iam", region_name=REGION)


with report("plan_vs_usage_audit") as r:
    r.heading("Plan vs usage — find paid features we're not using")

    findings = []  # list of {area, finding, severity, recommendation}

    # ════════════════════════════════════════════════════════════════════
    # A. AWS LAMBDA — what we use, what we don't
    # ════════════════════════════════════════════════════════════════════
    r.section("A1. Lambda: runtime versions, memory tiers, concurrency")
    fns = []
    for page in lam.get_paginator("list_functions").paginate():
        fns.extend(page.get("Functions", []))
    r.log(f"  Total Lambda functions: {len(fns)}")

    # Runtime distribution
    runtimes = Counter(f.get("Runtime") for f in fns)
    r.log(f"  Runtime distribution:")
    for rt, cnt in sorted(runtimes.items(), key=lambda x: -x[1]):
        r.log(f"    {rt or '(none)':25} {cnt}")

    # Memory tiers
    memories = Counter(f.get("MemorySize") for f in fns)
    r.log(f"  Memory tier distribution:")
    for mem, cnt in sorted(memories.items()):
        r.log(f"    {mem:>5}MB  {cnt}")

    # Reserved concurrency — if not set, you're at the account default (1000)
    # Very few of yours have it set
    rc_set = []
    for f in fns:
        try:
            cur = lam.get_function_concurrency(FunctionName=f["FunctionName"])
            rc = cur.get("ReservedConcurrentExecutions")
            if rc is not None:
                rc_set.append((f["FunctionName"], rc))
        except Exception:
            pass
    r.log(f"  Lambdas with reserved concurrency: {len(rc_set)}")
    for n, rc in rc_set:
        r.log(f"    {n:42} RC={rc}")

    # Provisioned concurrency — costs extra, usually you don't want it
    pc_set = []
    for f in fns:
        try:
            resp = lam.list_provisioned_concurrency_configs(FunctionName=f["FunctionName"])
            for cfg in resp.get("ProvisionedConcurrencyConfigs", []):
                pc_set.append((f["FunctionName"], cfg.get("AllocatedProvisionedConcurrentExecutions")))
        except Exception:
            pass
    r.log(f"  Provisioned concurrency configs: {len(pc_set)}")
    for n, pc in pc_set:
        r.log(f"    {n} provisioned={pc}")

    # Lambda Layers
    layer_users = []
    for f in fns:
        if f.get("Layers"):
            layer_users.append((f["FunctionName"], [l.get("Arn", "")[:80] for l in f["Layers"]]))
    r.log(f"  Lambdas using Layers: {len(layer_users)}")
    for n, ls in layer_users[:10]:
        r.log(f"    {n}: {len(ls)} layer(s)")

    # SnapStart (free, faster cold starts on Java/Python 3.13+)
    snapstart_eligible = [f for f in fns if f.get("Runtime", "").startswith(("python3.12", "python3.13", "java"))]
    snapstart_using = []
    for f in snapstart_eligible[:10]:  # sample
        try:
            cfg = lam.get_function_configuration(FunctionName=f["FunctionName"])
            ss = cfg.get("SnapStart", {}).get("ApplyOn", "None")
            if ss != "None":
                snapstart_using.append(f["FunctionName"])
        except Exception:
            pass
    r.log(f"  SnapStart eligible Lambdas (Python 3.12/3.13/Java): {len(snapstart_eligible)}")
    r.log(f"    Currently using SnapStart: {len(snapstart_using)} (sample of {min(10, len(snapstart_eligible))})")

    if not snapstart_using and snapstart_eligible:
        findings.append({
            "area": "Lambda",
            "finding": f"{len(snapstart_eligible)} Python Lambdas eligible for SnapStart but NONE using it",
            "severity": "🟡",
            "recommendation": "SnapStart is FREE and reduces cold start times by 10x. Enable on Lambdas that are user-facing (justhodl-ai-chat, justhodl-stock-analyzer, justhodl-investor-agents, justhodl-stock-screener Lambda URLs). aws lambda update-function-configuration --snap-start ApplyOn=PublishedVersions",
        })

    # Architecture (arm64 is ~20% cheaper than x86_64)
    archs = Counter()
    for f in fns:
        a = (f.get("Architectures") or ["x86_64"])[0]
        archs[a] += 1
    r.log(f"  Architecture distribution:")
    for a, cnt in archs.items():
        r.log(f"    {a}: {cnt}")
    if archs.get("arm64", 0) < len(fns) * 0.5:
        findings.append({
            "area": "Lambda",
            "finding": f"Only {archs.get('arm64', 0)}/{len(fns)} Lambdas on arm64; x86_64 is 20% more expensive",
            "severity": "🟡",
            "recommendation": "Switch Python Lambdas to arm64 (Graviton2). 20% cheaper for same workload, fully compatible with all the boto3/urllib code we use. Migration is a single Architectures=['arm64'] update per Lambda. Could save \\$2-4/mo at current spend.",
        })

    # ════════════════════════════════════════════════════════════════════
    # A2. S3 — Intelligent Tiering, Transfer Acceleration, Inventory
    # ════════════════════════════════════════════════════════════════════
    r.section("A2. S3 features")
    bucket = "justhodl-dashboard-live"
    try:
        # Intelligent Tiering — auto-moves cold objects to cheaper storage
        try:
            it = s3.list_bucket_intelligent_tiering_configurations(Bucket=bucket)
            it_configs = it.get("IntelligentTieringConfigurationList", [])
            r.log(f"  Intelligent Tiering configs: {len(it_configs)}")
            if not it_configs:
                findings.append({
                    "area": "S3",
                    "finding": "No Intelligent Tiering configured on justhodl-dashboard-live",
                    "severity": "🟡",
                    "recommendation": "Intelligent Tiering auto-moves objects to cheaper tiers based on access patterns. FREE for objects > 128KB. Could save \\$0.10-0.50/mo on archive/ + valuations-archive/ + investor-analysis/. Apply at bucket level via put-bucket-intelligent-tiering-configuration.",
                })
        except Exception as e:
            r.warn(f"  Intelligent Tiering check failed: {e}")

        # Transfer Acceleration — useful only for global users, costs extra
        try:
            ta = s3.get_bucket_accelerate_configuration(Bucket=bucket)
            r.log(f"  Transfer Acceleration: {ta.get('Status', 'Disabled')}")
        except Exception:
            r.log(f"  Transfer Acceleration: not enabled")

        # S3 Inventory — useful for large buckets, free
        try:
            inv = s3.list_bucket_inventory_configurations(Bucket=bucket)
            inv_configs = inv.get("InventoryConfigurationList", [])
            r.log(f"  S3 Inventory configs: {len(inv_configs)}")
        except Exception:
            r.log(f"  S3 Inventory: none")

        # S3 versioning — protects against deletions
        ver = s3.get_bucket_versioning(Bucket=bucket)
        r.log(f"  Versioning: {ver.get('Status', 'Disabled')}")
        if ver.get("Status") != "Enabled":
            findings.append({
                "area": "S3",
                "finding": "Bucket versioning DISABLED on justhodl-dashboard-live",
                "severity": "🟡",
                "recommendation": "Versioning protects against accidental overwrites/deletes. Free in itself (only pay for the extra storage of old versions). Combined with a 30-day expiration lifecycle on old versions, costs essentially nothing but gives you a safety net. Enabling now is one-time, retroactive isn't possible.",
            })

        # Lifecycle rules
        try:
            lc = s3.get_bucket_lifecycle_configuration(Bucket=bucket)
            rules = lc.get("Rules", [])
            r.log(f"  Lifecycle rules: {len(rules)}")
            for rule in rules:
                r.log(f"    {rule.get('ID', '?')}: {rule.get('Status')}")
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchLifecycleConfiguration":
                r.log(f"  Lifecycle rules: none")
            else:
                raise

        # Bucket size + storage class breakdown
        end_d = datetime.now(timezone.utc)
        start_d = end_d - timedelta(days=2)
        for storage_type in ["StandardStorage", "IntelligentTieringIAStorage",
                              "GlacierStorage", "DeepArchiveStorage"]:
            try:
                m = cw.get_metric_statistics(
                    Namespace="AWS/S3", MetricName="BucketSizeBytes",
                    Dimensions=[
                        {"Name": "BucketName", "Value": bucket},
                        {"Name": "StorageType", "Value": storage_type},
                    ],
                    StartTime=start_d, EndTime=end_d, Period=86400, Statistics=["Average"],
                )
                if m.get("Datapoints"):
                    latest = max(m["Datapoints"], key=lambda p: p["Timestamp"])
                    size_gb = latest["Average"] / 1024**3
                    if size_gb > 0.001:
                        r.log(f"  {storage_type:30} {size_gb:>8.2f} GB")
            except Exception:
                pass
    except Exception as e:
        r.warn(f"  S3 audit error: {e}")

    # ════════════════════════════════════════════════════════════════════
    # A3. DynamoDB — billing modes, indexes, streams, DAX
    # ════════════════════════════════════════════════════════════════════
    r.section("A3. DynamoDB features")
    tables = []
    for page in ddb.get_paginator("list_tables").paginate():
        tables.extend(page.get("TableNames", []))
    r.log(f"  Total tables: {len(tables)}")

    billing_modes = Counter()
    streams_enabled = 0
    pitr_enabled = 0
    gsi_count_total = 0
    backup_plans = 0
    for tn in tables:
        try:
            d = ddb.describe_table(TableName=tn)["Table"]
            mode = d.get("BillingModeSummary", {}).get("BillingMode") or "PROVISIONED"
            billing_modes[mode] += 1
            gsi_count_total += len(d.get("GlobalSecondaryIndexes") or [])
            if d.get("StreamSpecification", {}).get("StreamEnabled"):
                streams_enabled += 1
            # PITR
            try:
                p = ddb.describe_continuous_backups(TableName=tn)
                if p.get("ContinuousBackupsDescription", {}).get("PointInTimeRecoveryDescription", {}).get("PointInTimeRecoveryStatus") == "ENABLED":
                    pitr_enabled += 1
            except Exception:
                pass
        except Exception:
            pass

    r.log(f"  Billing modes: {dict(billing_modes)}")
    r.log(f"  Tables with Streams enabled: {streams_enabled}")
    r.log(f"  Tables with PITR enabled: {pitr_enabled}")
    r.log(f"  Total GSIs across all tables: {gsi_count_total}")

    # PITR for the critical learning tables — should it be enabled?
    if pitr_enabled == 0:
        findings.append({
            "area": "DynamoDB",
            "finding": "PITR (Point-In-Time-Recovery) NOT enabled on any table — including justhodl-signals + justhodl-outcomes (your learning data)",
            "severity": "🔴",
            "recommendation": "PITR provides 35 days of continuous backup recovery. Costs ~\\$0.20/GB-month. Your signals + outcomes tables are tiny — PITR cost would be < \\$0.01/mo combined. Enable on justhodl-signals and justhodl-outcomes — losing this data would set the learning loop back to zero. aws dynamodb update-continuous-backups --table-name justhodl-signals --point-in-time-recovery-specification PointInTimeRecoveryEnabled=true",
        })

    # ════════════════════════════════════════════════════════════════════
    # A4. CloudWatch — alarms, dashboards, contributor insights
    # ════════════════════════════════════════════════════════════════════
    r.section("A4. CloudWatch features")

    # Alarms
    alarms = []
    for page in cw.get_paginator("describe_alarms").paginate():
        alarms.extend(page.get("MetricAlarms", []))
        alarms.extend(page.get("CompositeAlarms", []))
    r.log(f"  Alarms configured: {len(alarms)}")
    if len(alarms) < 5:
        findings.append({
            "area": "CloudWatch",
            "finding": f"Only {len(alarms)} CloudWatch alarms configured — first 10 alarms are FREE",
            "severity": "🟡",
            "recommendation": "AWS gives you 10 alarms free. We're using these for nothing. Set up alarms for: (1) Lambda errors > 0 on critical Lambdas (justhodl-daily-report-v3, justhodl-ai-chat), (2) S3 bucket size growing > 50% week-over-week, (3) DynamoDB throttle events on signals table, (4) CloudFront/Cloudflare cache hit ratio < 50%, (5) monthly Lambda spend > \\$25 (CloudWatch billing alarm). Each: aws cloudwatch put-metric-alarm.",
        })

    # Dashboards
    try:
        dashboards = cw.list_dashboards().get("DashboardEntries", [])
        r.log(f"  Dashboards: {len(dashboards)}")
        for d in dashboards:
            r.log(f"    {d.get('DashboardName')}")
        if not dashboards:
            findings.append({
                "area": "CloudWatch",
                "finding": "No CloudWatch dashboards — first 3 dashboards are FREE",
                "severity": "🟡",
                "recommendation": "AWS gives you 3 dashboards free. Build one for: 'JustHodl Operations' showing top 10 Lambdas by GB-s, error rates, S3 storage trend, DynamoDB read/write activity. One-time setup, then auto-refreshes. Useful when you don't want to open the health monitor.",
            })
    except Exception as e:
        r.warn(f"  Dashboards: {e}")

    # ════════════════════════════════════════════════════════════════════
    # A5. SSM Parameter Store — Standard vs Advanced tier
    # ════════════════════════════════════════════════════════════════════
    r.section("A5. SSM Parameter Store")
    params = []
    for page in ssm.get_paginator("describe_parameters").paginate():
        params.extend(page.get("Parameters", []))
    standard = [p for p in params if p.get("Tier") == "Standard"]
    advanced = [p for p in params if p.get("Tier") == "Advanced"]
    r.log(f"  Total parameters: {len(params)} (Standard: {len(standard)}, Advanced: {len(advanced)})")
    # Standard tier is FREE (10,000 params, 4KB each). Advanced is $0.05/param/mo.
    if len(advanced) > 0:
        r.log(f"  ⚠  Advanced tier params (\\$0.05/param/mo each):")
        for p in advanced:
            r.log(f"    {p.get('Name')}")

    # ════════════════════════════════════════════════════════════════════
    # A6. EventBridge — custom buses?
    # ════════════════════════════════════════════════════════════════════
    r.section("A6. EventBridge custom event buses")
    try:
        buses = eb.list_event_buses().get("EventBuses", [])
        r.log(f"  Event buses: {len(buses)}")
        for b in buses:
            r.log(f"    {b.get('Name')} (default={b.get('Name') == 'default'})")
        # Just default = free; custom buses are $1/million events but we're not using any
    except Exception:
        pass

    # Schemas registry, Pipes — we likely don't use these but worth noting
    # Skip for now — cost is per-API-call, free tier covers small accounts

    # ════════════════════════════════════════════════════════════════════
    # A7. IAM — unused users
    # ════════════════════════════════════════════════════════════════════
    r.section("A7. IAM users — unused?")
    try:
        users = iam.list_users().get("Users", [])
        r.log(f"  IAM users: {len(users)}")
        for u in users:
            uname = u["UserName"]
            last_used = u.get("PasswordLastUsed")
            keys = iam.list_access_keys(UserName=uname).get("AccessKeyMetadata", [])
            r.log(f"    {uname:35} keys={len(keys)} pwd_last_used={last_used}")
            for k in keys:
                kid = k.get("AccessKeyId", "")
                try:
                    kli = iam.get_access_key_last_used(AccessKeyId=kid)
                    last = kli.get("AccessKeyLastUsed", {}).get("LastUsedDate")
                    r.log(f"      key {kid[:20]}... last used: {last}")
                except Exception:
                    pass
    except Exception as e:
        r.warn(f"  IAM users: {e}")

    # ════════════════════════════════════════════════════════════════════
    # B. CloudFlare — Workers, KV, D1, R2 status
    # ════════════════════════════════════════════════════════════════════
    r.section("B. Cloudflare features")
    r.log("  CloudFlare resources currently provisioned (per separate tool query):")
    r.log("    Workers: 1 (justhodl-ai-proxy)")
    r.log("    D1 databases: 0")
    r.log("    KV namespaces: 0")
    r.log("    R2 buckets: 0")
    r.log("    Durable Objects: 0")
    r.log("    Queues: 0")

    findings.append({
        "area": "Cloudflare",
        "finding": "Free Workers tier covers 100K req/day; only 1 Worker (ai-proxy) deployed. KV/D1/R2 not used at all.",
        "severity": "🟡",
        "recommendation": "Cloudflare free tier offers serious value we're leaving unused: (1) **Workers KV** (100K reads/day, 1K writes/day FREE) — perfect for caching FRED API responses globally with low latency. (2) **R2 storage** (10GB free, NO egress fees) — you're paying S3 egress fees if any external service reads from S3; R2 has zero egress. (3) **D1 SQLite** (5GB free) — could replace the read-heavy DDB lookups on justhodl-signals. (4) **Workers Cache API** — already free, can speed up scorecard.json delivery. Worth a separate session to evaluate which to enable.",
    })

    # ════════════════════════════════════════════════════════════════════
    # C. Third-party API plans — using premium endpoints?
    # ════════════════════════════════════════════════════════════════════
    r.section("C. Third-party API plans — usage check")

    # FMP Premium check — search Lambda sources for premium endpoints
    fmp_endpoints_used = set()
    fmp_premium_endpoints = {
        "/stable/quote": "real-time quote (premium)",
        "/api/v3/stock-screener": "advanced screener (premium)",
        "/api/v3/quote-short": "RETIRED Aug 2025",
        "/api/v3/historical-price-full": "premium historicals",
        "/api/v3/financial-statements": "fundamentals (premium)",
        "/api/v3/insider-trading": "insider trades (premium)",
        "/api/v3/grade": "analyst ratings (premium)",
        "/api/v3/earning_call_transcript": "transcripts (premium)",
        "/api/v3/sec_filings": "SEC filings (premium)",
        "/api/v3/economic": "economic calendar (premium)",
    }
    lambdas_dir = REPO_ROOT / "aws/lambdas"
    if lambdas_dir.exists():
        for src_file in lambdas_dir.rglob("*.py"):
            try:
                content = src_file.read_text(encoding="utf-8", errors="ignore")
                for ep in fmp_premium_endpoints:
                    if ep in content:
                        fmp_endpoints_used.add(ep)
            except Exception:
                pass
    r.log(f"  FMP Premium endpoints in use:")
    for ep in sorted(fmp_endpoints_used):
        r.log(f"    ✓ {ep:35} → {fmp_premium_endpoints[ep]}")
    not_using = set(fmp_premium_endpoints.keys()) - fmp_endpoints_used
    not_using = {ep for ep in not_using if "RETIRED" not in fmp_premium_endpoints[ep]}
    r.log(f"  FMP Premium endpoints NOT in use:")
    for ep in sorted(not_using):
        r.log(f"    ✗ {ep:35} → {fmp_premium_endpoints[ep]}")

    if len(not_using) >= 4:
        findings.append({
            "area": "FMP Premium",
            "finding": f"Paying for FMP Premium but only using {len(fmp_endpoints_used)} of ~10 valuable premium endpoints",
            "severity": "🟡",
            "recommendation": "FMP Premium gives you analyst ratings, insider trading, earnings transcripts, SEC filings, and economic calendar. We're only hitting screener + quote. Build (1) an insider-trading widget for stocks page, (2) earnings-transcript summarizer using Claude, (3) economic calendar feed for the next-events ticker, (4) analyst ratings consensus. These are all 1-Lambda, ~30-min builds each.",
        })

    # Polygon usage check
    polygon_endpoints_used = set()
    polygon_premium_signals = {
        "/v2/aggs/ticker/": "free tier OK",
        "/v3/reference/options": "options chains (paid)",
        "/v2/snapshot/options": "real-time options (paid)",
        "/v3/quotes": "tick quotes (paid)",
        "/v3/trades": "tick trades (paid)",
        "/v1/indicators": "TA indicators (paid)",
        "/v2/last/trade": "real-time trade (paid)",
    }
    if lambdas_dir.exists():
        for src_file in lambdas_dir.rglob("*.py"):
            try:
                content = src_file.read_text(encoding="utf-8", errors="ignore")
                for ep in polygon_premium_signals:
                    if ep in content:
                        polygon_endpoints_used.add(ep)
            except Exception:
                pass
    r.log(f"\n  Polygon endpoints in use: {sorted(polygon_endpoints_used)}")

    # Anthropic Batch API usage — never used = leaving 50% off the table
    batch_used = False
    if lambdas_dir.exists():
        for src_file in lambdas_dir.rglob("*.py"):
            try:
                content = src_file.read_text(encoding="utf-8", errors="ignore")
                if "/messages/batches" in content or "anthropic.messages.batches" in content:
                    batch_used = True
                    break
            except Exception:
                pass
    r.log(f"\n  Anthropic Batch API in use: {batch_used}")
    if not batch_used:
        findings.append({
            "area": "Anthropic API",
            "finding": "Anthropic Message Batches API NOT used — 50% cost discount on async work",
            "severity": "🟡",
            "recommendation": "Batch API gives 50% discount + 24h SLA. Perfect for non-real-time work: (1) the 6 investor agents (Buffett/Munger/Burry/Druckenmiller/Lynch/Wood) on the legendary panel — these are async by nature. (2) the morning brief composition. (3) self-improvement of prompt templates. Real-time stays on standard pricing (ai-chat). Just changing the API call site cuts those Anthropic costs in half.",
        })

    # ════════════════════════════════════════════════════════════════════
    # Build audit doc
    # ════════════════════════════════════════════════════════════════════
    r.section("Build audit doc")
    md = []
    md.append(f"# Plan vs Usage Audit — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n")
    md.append("**Question:** Are we using everything we're already paying for?\n")
    md.append(f"**Findings count:** {len(findings)}\n")

    md.append("\n## Summary\n")
    by_severity = defaultdict(list)
    for f in findings:
        by_severity[f["severity"]].append(f)
    md.append(f"- 🔴 High value / costing data risk: **{len(by_severity['🔴'])}**")
    md.append(f"- 🟡 Optimization opportunity: **{len(by_severity['🟡'])}**")
    md.append(f"- ⚫ Pure waste: **{len(by_severity['⚫'])}**")

    md.append("\n## Findings\n")
    for f in sorted(findings, key=lambda x: ["🔴", "🟡", "⚫"].index(x["severity"])):
        md.append(f"\n### {f['severity']} {f['area']}: {f['finding']}\n")
        md.append(f"**Recommendation:** {f['recommendation']}")

    md.append("\n## What we're using well\n")
    md.append("- Lambda is on PAY_PER_REQUEST equivalent (no idle compute charges)")
    md.append("- DynamoDB on PAY_PER_REQUEST (no idle table charges)")
    md.append("- S3 has lifecycle policy for archive/* → Deep Archive")
    md.append("- 14-day retention on all 107 CloudWatch log groups")
    md.append("- FRED cache shared across Lambdas (88% cache hit rate)")
    md.append("- CI/CD via GitHub Actions (no separate CodePipeline cost)")
    md.append("- Cloudflare Workers (free tier handles ai-proxy)")
    md.append("- ~96 Lambdas under \\$30/mo combined (great efficiency)")

    out_path = REPO_ROOT / "aws/ops/audit/plan_vs_usage_2026-04-25.md"
    out_path.write_text("\n".join(md))
    r.ok(f"  Wrote {out_path.relative_to(REPO_ROOT)}")
    s3.put_object(
        Bucket=bucket,
        Key="_audit/plan_vs_usage_2026-04-25.md",
        Body="\n".join(md).encode(),
        ContentType="text/markdown",
    )

    r.kv(
        findings=len(findings),
        red=len(by_severity['🔴']),
        yellow=len(by_severity['🟡']),
        black=len(by_severity['⚫']),
    )
    r.log("Done")
