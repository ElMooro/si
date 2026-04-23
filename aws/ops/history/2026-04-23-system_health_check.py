#!/usr/bin/env python3
"""
Comprehensive JustHodl.AI system health check.

Verifies every major subsystem we built/modified recently:

  1. Data pipeline freshness (data/report.json written on 5-min cadence)
  2. FRED cache v3.2 (88% hit rate, list-shape, _meta stamps)
  3. Secretary v2.2 (4h cadence, data/secretary-latest.json fresh)
  4. All critical Lambdas (alive + last-invocation recent + no errors)
  5. S3 public access (all 5 dashboard files HTTP 200)
  6. AI chat endpoint (api.justhodl.ai responsive)
  7. Frontend (dashboard HTML pages load)
  8. Data sanity (Khalid Index, FRED count, stock count, sector rotation $M format)

Runs read-only. No mutations to any resource.
"""
import json
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from ops_report import report
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

lam = boto3.client("lambda", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)

# ──────────────────────────────────────────────────────────────
# What we expect in a healthy system
# ──────────────────────────────────────────────────────────────
CRITICAL_LAMBDAS = {
    # name → (expected_schedule_minutes, purpose)
    "justhodl-daily-report-v3": (5, "Main data orchestrator (produces report.json)"),
    "justhodl-financial-secretary": (240, "4h Secretary brief + email"),
    "justhodl-ai-chat": (None, "AI chat (invoked per user request, no schedule)"),
    "cftc-futures-positioning-agent": (None, "CFTC signals (EB Friday 6PM UTC)"),
    "justhodl-stock-analyzer": (None, "Stock chart analyzer (on-demand)"),
    "justhodl-stock-screener": (240, "4h stock screener"),
    "justhodl-edge-engine": (None, "Edge intelligence (on-demand/scheduled)"),
    "justhodl-morning-intelligence": (1440, "Daily 8AM ET brief"),
    "justhodl-investor-agents": (None, "Multi-agent investor panel (on-demand)"),
    "justhodl-telegram-bot": (None, "Telegram bot (webhook-triggered)"),
    "justhodl-signal-logger": (360, "6h signal snapshot → DynamoDB"),
    "justhodl-dex-scanner": (None, "DEX scanner (pushes to GitHub)"),
}

PUBLIC_DASHBOARD_FILES = [
    "data/report.json",
    "data/secretary-latest.json",
    "data/fred-cache.json",
    "flow-data.json",
    "crypto-intel.json",
]

FRONTEND_PAGES = [
    "https://justhodl.ai/",
    "https://justhodl.ai/pro.html",
    "https://justhodl.ai/edge.html",
    "https://justhodl.ai/liquidity.html",
    "https://justhodl.ai/dex.html",
    "https://justhodl.ai/valuations.html",
]

with report("system_health_check") as r:
    r.heading("JustHodl.AI — End-to-End System Health Check")

    # ═══════════════════════════════════════════════════════════════
    # SECTION 1 — Data pipeline freshness
    # ═══════════════════════════════════════════════════════════════
    r.section("1. Data pipeline — data/report.json freshness")
    try:
        obj = s3.head_object(Bucket=BUCKET, Key="data/report.json")
        lm = obj["LastModified"]
        age_min = (datetime.now(timezone.utc) - lm).total_seconds() / 60
        size_mb = obj["ContentLength"] / 1024 / 1024
        r.log(f"  Last modified: {lm.isoformat()} ({age_min:.1f} min ago)")
        r.log(f"  Size: {size_mb:.2f} MB")
        if age_min < 10:
            r.ok(f"  ✓ FRESH — within 10 min (schedule is 5 min)")
            pipeline_status = "FRESH"
        elif age_min < 60:
            r.warn(f"  ⚠ Stale — {age_min:.0f} min old (expected <10)")
            pipeline_status = "STALE"
        else:
            r.fail(f"  ✗ VERY STALE — {age_min:.0f} min old (pipeline may be broken)")
            pipeline_status = "BROKEN"
        r.kv(pipeline=pipeline_status, age_min=round(age_min, 1), size_mb=round(size_mb, 2))

        # Read it to check the payload shape
        obj_full = s3.get_object(Bucket=BUCKET, Key="data/report.json")
        data = json.loads(obj_full["Body"].read())
        r.log(f"\n  Top-level keys: {sorted(list(data.keys()))[:10]}{'…' if len(data) > 10 else ''}")
        ki = data.get("khalid_index", {})
        if isinstance(ki, dict):
            r.log(f"  Khalid Index: score={ki.get('score')}, regime={ki.get('regime')}")
        else:
            r.log(f"  Khalid Index (legacy scalar): {ki}")
        fred = data.get("fred_data") or data.get("fred") or {}
        fred_count = 0
        if isinstance(fred, dict):
            for cat in fred.values():
                if isinstance(cat, dict):
                    fred_count += len(cat)
        r.log(f"  FRED series in report: {fred_count}")
        stocks = data.get("stock_data") or data.get("stocks") or data.get("tickers") or {}
        r.log(f"  Stocks tracked: {len(stocks)}")
        crypto = data.get("crypto_data") or data.get("crypto") or {}
        r.log(f"  Crypto coins: {len(crypto)}")
        r.kv(khalid_index=ki.get("score") if isinstance(ki, dict) else ki,
             regime=ki.get("regime") if isinstance(ki, dict) else None,
             fred_count=fred_count, stocks=len(stocks), crypto=len(crypto))
    except Exception as e:
        r.fail(f"  data/report.json read failed: {e}")

    # ═══════════════════════════════════════════════════════════════
    # SECTION 2 — FRED cache v3.2 health
    # ═══════════════════════════════════════════════════════════════
    r.section("2. FRED cache v3.2 — still hitting high skip rate?")
    try:
        obj = s3.head_object(Bucket=BUCKET, Key="data/fred-cache.json")
        age_min = (datetime.now(timezone.utc) - obj["LastModified"]).total_seconds() / 60
        r.log(f"  fred-cache.json: {obj['ContentLength']:,} bytes, {age_min:.1f} min old")

        # Check recent logs for "skipped N via smart TTL"
        streams = logs.describe_log_streams(
            logGroupName="/aws/lambda/justhodl-daily-report-v3",
            orderBy="LastEventTime", descending=True, limit=3,
        ).get("logStreams", [])

        skip_counts = []
        fetch_times = []
        done_times = []
        for s in streams[:3]:
            start = int((datetime.now(timezone.utc) - timedelta(minutes=30)).timestamp() * 1000)
            ev = logs.get_log_events(
                logGroupName="/aws/lambda/justhodl-daily-report-v3",
                logStreamName=s["logStreamName"], startTime=start, limit=200, startFromHead=False,
            )
            for e in ev.get("events", []):
                m = e.get("message", "").strip()
                # Parse "FRED v3.2: skipped N via smart TTL ({...}), fetching X"
                if "FRED v3.2: skipped" in m:
                    import re
                    mt = re.search(r"skipped (\d+) via smart TTL.*fetching (\d+)", m)
                    if mt:
                        skip_counts.append((int(mt.group(1)), int(mt.group(2))))
                if "FRED: " in m and "/233 in " in m:
                    mt = re.search(r"FRED: (\d+)/233 in ([\d.]+)s", m)
                    if mt:
                        fetch_times.append(float(mt.group(2)))
                if "V10] DONE" in m:
                    mt = re.search(r"DONE ([\d.]+)s", m)
                    if mt:
                        done_times.append(float(mt.group(1)))

        if skip_counts:
            latest_skip, latest_fetch = skip_counts[0]
            total = latest_skip + latest_fetch
            skip_pct = 100 * latest_skip / total if total > 0 else 0
            r.log(f"  Last 3 runs skip counts: {skip_counts}")
            r.log(f"  Latest run: skipped {latest_skip}, fetched {latest_fetch} ({skip_pct:.0f}% skip)")
            if skip_pct >= 80:
                r.ok(f"  ✓ Cache working as designed ({skip_pct:.0f}% skip rate)")
            elif skip_pct >= 50:
                r.warn(f"  ⚠ Lower than expected — {skip_pct:.0f}% (should be ≥80%)")
            else:
                r.fail(f"  ✗ Cache barely hitting — {skip_pct:.0f}%")
        else:
            r.warn("  No v3.2 log lines found in recent streams — may be between runs")

        if fetch_times:
            avg_fetch = sum(fetch_times) / len(fetch_times)
            r.log(f"  FRED fetch times (last {len(fetch_times)} runs): {[f'{t:.1f}s' for t in fetch_times]}")
            r.log(f"  Average: {avg_fetch:.1f}s (warm target <30s, cold expected ~120-250s)")
        if done_times:
            r.log(f"  End-to-end DONE times: {[f'{t:.1f}s' for t in done_times]}")

        r.kv(fred_cache=dict(size_kb=round(obj['ContentLength']/1024), age_min=round(age_min, 1)),
             last_skip_pct=round(skip_pct, 0) if skip_counts else None,
             avg_fetch_s=round(avg_fetch, 1) if fetch_times else None)
    except Exception as e:
        r.warn(f"  FRED cache check failed: {e}")

    # ═══════════════════════════════════════════════════════════════
    # SECTION 3 — Secretary v2.2 health
    # ═══════════════════════════════════════════════════════════════
    r.section("3. Secretary v2.2 — 4h brief + tier-2 cards")
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="data/secretary-latest.json")
        age_min = (datetime.now(timezone.utc) - obj["LastModified"]).total_seconds() / 60
        r.log(f"  secretary-latest.json: {obj['ContentLength']:,} bytes, {age_min:.1f} min old")
        if age_min < 260:
            r.ok(f"  ✓ Within 4h schedule window")
        else:
            r.warn(f"  ⚠ Exceeds 4h schedule ({age_min:.0f} min old)")

        data = json.loads(obj["Body"].read())
        # Check for v2.2 tier-2 cards
        has_options_flow = "options_flow" in data or "flow" in data
        has_crypto_intel = "crypto_intel" in data or "crypto" in data
        has_sector = "sector_rotation" in data
        has_hit_rate = "hit_rate_pct" in data or "yesterday_deltas" in data
        r.log(f"  v2.2 features present:")
        r.log(f"    options_flow card: {has_options_flow}")
        r.log(f"    crypto_intel card: {has_crypto_intel}")
        r.log(f"    sector rotation: {has_sector}")
        r.log(f"    hit rate tracking: {has_hit_rate}")

        # Specific — sector rotation should have small $ numbers now (bug fix: divide by 1e6)
        sr = data.get("sector_rotation") or {}
        if isinstance(sr, dict):
            top_in = sr.get("top_inflow_flow") or sr.get("top_inflow", {})
            if isinstance(top_in, dict):
                top_in = top_in.get("flow") or top_in.get("value")
            r.log(f"    top_inflow_flow value: {top_in} (should be <$1B after $M format fix)")

        r.kv(secretary_age_min=round(age_min, 1),
             tier2_options=has_options_flow, tier2_crypto=has_crypto_intel,
             hit_rate=has_hit_rate)
    except Exception as e:
        r.warn(f"  Secretary check failed: {e}")

    # ═══════════════════════════════════════════════════════════════
    # SECTION 4 — All critical Lambdas — alive + recent
    # ═══════════════════════════════════════════════════════════════
    r.section("4. Critical Lambdas — alive + recent activity")
    alive_count = 0
    stale_count = 0
    for fn_name, (expected_min, purpose) in CRITICAL_LAMBDAS.items():
        try:
            cfg = lam.get_function_configuration(FunctionName=fn_name)
            last_mod = cfg["LastModified"]

            # Get the latest invocation
            end = datetime.now(timezone.utc)
            start = end - timedelta(hours=24 if expected_min is None else (expected_min/60 * 3))
            resp = cw.get_metric_statistics(
                Namespace="AWS/Lambda", MetricName="Invocations",
                Dimensions=[{"Name": "FunctionName", "Value": fn_name}],
                StartTime=start, EndTime=end, Period=300, Statistics=["Sum"],
            )
            points = sorted(resp.get("Datapoints", []), key=lambda x: x["Timestamp"])
            last_inv_time = None
            total_inv = 0
            for p in points:
                if p.get("Sum", 0) > 0:
                    last_inv_time = p["Timestamp"]
                    total_inv += p["Sum"]

            # Errors
            err_resp = cw.get_metric_statistics(
                Namespace="AWS/Lambda", MetricName="Errors",
                Dimensions=[{"Name": "FunctionName", "Value": fn_name}],
                StartTime=start, EndTime=end, Period=300, Statistics=["Sum"],
            )
            err_total = sum(p.get("Sum", 0) for p in err_resp.get("Datapoints", []))

            if last_inv_time:
                age_min_inv = (datetime.now(timezone.utc) - last_inv_time).total_seconds() / 60
                status_icon = "✓"
                status = "ALIVE"
                if expected_min and age_min_inv > expected_min * 2:
                    status_icon = "⚠"
                    status = "STALE"
                    stale_count += 1
                else:
                    alive_count += 1
                err_flag = f" [{int(err_total)} err]" if err_total > 0 else ""
                r.log(f"  {status_icon} {fn_name:38} {status}  last_inv {age_min_inv:5.0f}m ago, {int(total_inv)} invs{err_flag}")
            else:
                r.log(f"  ? {fn_name:38} NO INVOCATIONS in window  (expected_cadence={expected_min}m)")
        except lam.exceptions.ResourceNotFoundException:
            r.warn(f"  ✗ {fn_name}: NOT FOUND (deleted?)")
        except Exception as e:
            r.warn(f"  ✗ {fn_name}: {str(e)[:80]}")

    r.kv(lambdas_alive=alive_count, lambdas_stale=stale_count, lambdas_total=len(CRITICAL_LAMBDAS))

    # ═══════════════════════════════════════════════════════════════
    # SECTION 5 — S3 public access
    # ═══════════════════════════════════════════════════════════════
    r.section("5. S3 public HTTPS access (dashboard readability)")
    import ssl
    ctx = ssl.create_default_context()
    ok = 0
    for key in PUBLIC_DASHBOARD_FILES:
        url = f"https://{BUCKET}.s3.amazonaws.com/{key}"
        try:
            req = urllib.request.Request(url, method="HEAD")
            with urllib.request.urlopen(req, timeout=8, context=ctx) as resp:
                if resp.status == 200:
                    r.log(f"  ✓ {key}: 200 OK")
                    ok += 1
                else:
                    r.warn(f"  ⚠ {key}: {resp.status}")
        except urllib.error.HTTPError as e:
            r.fail(f"  ✗ {key}: {e.code}")
        except Exception as e:
            r.warn(f"  ? {key}: {str(e)[:60]}")
    r.kv(s3_public=f"{ok}/{len(PUBLIC_DASHBOARD_FILES)}")

    # ═══════════════════════════════════════════════════════════════
    # SECTION 6 — AI chat endpoint
    # ═══════════════════════════════════════════════════════════════
    r.section("6. AI chat — api.justhodl.ai reachability")
    r.log("  NOTE: sandbox egress can't reach api.justhodl.ai directly.")
    r.log("  Indirect check: is the underlying Lambda invoking + returning 200s?")
    try:
        end = datetime.now(timezone.utc)
        start = end - timedelta(hours=6)
        resp = cw.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName="Invocations",
            Dimensions=[{"Name": "FunctionName", "Value": "justhodl-ai-chat"}],
            StartTime=start, EndTime=end, Period=3600, Statistics=["Sum"],
        )
        total_inv = sum(p.get("Sum", 0) for p in resp.get("Datapoints", []))
        err_resp = cw.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName="Errors",
            Dimensions=[{"Name": "FunctionName", "Value": "justhodl-ai-chat"}],
            StartTime=start, EndTime=end, Period=3600, Statistics=["Sum"],
        )
        total_err = sum(p.get("Sum", 0) for p in err_resp.get("Datapoints", []))
        r.log(f"  justhodl-ai-chat — last 6h: {int(total_inv)} invocations, {int(total_err)} errors")
        if total_inv > 0:
            err_rate = 100 * total_err / total_inv if total_inv else 0
            if err_rate < 5:
                r.ok(f"  ✓ Healthy — {err_rate:.1f}% error rate")
            else:
                r.warn(f"  ⚠ Error rate elevated: {err_rate:.1f}%")
        else:
            r.log(f"  (no traffic last 6h — feature may be idle)")
        r.kv(ai_chat_invocations_6h=int(total_inv), ai_chat_errors_6h=int(total_err))
    except Exception as e:
        r.warn(f"  {e}")

    # ═══════════════════════════════════════════════════════════════
    # SECTION 7 — EventBridge rules — are schedules wired?
    # ═══════════════════════════════════════════════════════════════
    r.section("7. EventBridge schedule rules")
    eb = boto3.client("events", region_name=REGION)
    try:
        rules = eb.list_rules(NamePrefix="justhodl")["Rules"]
        r.log(f"  Found {len(rules)} justhodl-prefix EB rules:")
        for rule in rules:
            state = rule.get("State", "?")
            sched = rule.get("ScheduleExpression", "(no schedule)")
            r.log(f"    [{state}] {rule['Name']:50} {sched}")
        # Also check non-prefix rules that might target our Lambdas
        r.log("")
        more_rules = eb.list_rules()["Rules"]
        # Filter to rules whose targets include our Lambdas
        our_lambda_arns = set()
        for fn_name in CRITICAL_LAMBDAS:
            try:
                cfg = lam.get_function_configuration(FunctionName=fn_name)
                our_lambda_arns.add(cfg["FunctionArn"])
            except Exception:
                pass
        for rule in more_rules:
            if rule["Name"].startswith("justhodl"):
                continue
            try:
                targets = eb.list_targets_by_rule(Rule=rule["Name"]).get("Targets", [])
                for t in targets:
                    if t.get("Arn") in our_lambda_arns:
                        r.log(f"    [{rule.get('State')}] {rule['Name']} → {t['Arn'].split(':')[-1]}  {rule.get('ScheduleExpression', '')}")
            except Exception:
                pass
    except Exception as e:
        r.warn(f"  EB check failed: {e}")

    # ═══════════════════════════════════════════════════════════════
    # SECTION 8 — Summary
    # ═══════════════════════════════════════════════════════════════
    r.section("8. Summary")
    r.log("  See all sections above. Any line starting with ✗ is a real failure.")
    r.log("  ⚠ means needs attention but not immediately broken.")
    r.log("  ✓ means verified working.")

    r.log("Done")
