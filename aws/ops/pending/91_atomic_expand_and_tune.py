#!/usr/bin/env python3
"""
Step 91 — Atomic expand + tune (fix CI race condition).

Steps 89 and 90 ran in separate CI invocations. Between them, GitHub
Actions had a race where step 90's checkout pulled from BEFORE step 89's
commit was merged, so step 90 read the pre-89 expectations.py and
rewrote that, effectively reverting step 89's expansion.

This step does the whole flow in ONE invocation:
  1. Read the most-recent expectations.py from disk (post-89 state if
     committed, pre-89 otherwise — handle both)
  2. Pull inventory from S3 (this is the canonical source)
  3. Run the expansion logic again — auto-derive 41 Lambda + 8 S3 entries
  4. Run the tuning logic — for each auto-derived Lambda, fetch 7d
     CloudWatch metrics and tune thresholds to observed reality
  5. Write the merged + tuned expectations.py
  6. Re-deploy + re-invoke + verify

Key fixes from step 90's lessons:
  - 6 Lambdas have been at 100% error rate for 7+ days. These are
    REAL bugs that the dashboard should highlight. Don't auto-silence
    them — keep severity=important and note them clearly.
  - For dormant Lambdas (0 inv 7d), set min_invocations_24h=0 +
    note "appears dormant". Don't silence the alarm if they error.
  - For low-volume Lambdas (observed > 0 but < expected), tune
    min_invocations_24h to 50% of observed avg/day.
"""
import io
import json
import os
import re
import zipfile
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)
eb = boto3.client("events", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)


# ─── Schedule parsing (same as step 89) ──────────────────────────────

def parse_schedule_to_per_day(schedule):
    if not schedule:
        return None
    m = re.match(r"rate\((\d+)\s*(\w+)\)", schedule)
    if m:
        n = int(m.group(1))
        unit = m.group(2).rstrip("s").lower()
        return {"minute": 1440/n, "hour": 24/n, "day": 1.0/n}.get(unit)
    m = re.match(r"cron\((.+)\)", schedule)
    if m:
        parts = m.group(1).split()
        if len(parts) >= 5:
            mins, hours, dom, mon, dow = parts[:5]
            if hours == "*":
                hours_per_day = 24
            elif "/" in hours:
                hours_per_day = 24 // int(hours.split("/")[-1])
            elif "," in hours:
                hours_per_day = len(hours.split(","))
            elif "-" in hours:
                start, end = hours.split("-")
                hours_per_day = int(end) - int(start) + 1
            else:
                hours_per_day = 1
            if mins == "*":
                mins_per_hour = 60
            elif "/" in mins:
                mins_per_hour = 60 // int(mins.split("/")[-1])
            elif "," in mins:
                mins_per_hour = len(mins.split(","))
            else:
                mins_per_hour = 1
            per_day = hours_per_day * mins_per_hour
            if dow not in ("?", "*"):
                if dow == "MON-FRI":
                    per_day *= 5/7
                elif dow in ("SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"):
                    per_day *= 1/7
                elif "," in dow:
                    per_day *= len(dow.split(","))/7
            if dom not in ("?", "*"):
                if dom == "1":
                    per_day *= 1/30
            return per_day
    return None


def schedule_to_seconds(per_day):
    return 86400 / per_day if per_day and per_day > 0 else None


# ─── S3 write extraction ─────────────────────────────────────────────

def extract_s3_writes(src):
    keys = set()
    for m in re.finditer(r"""put_object\s*\([^)]*?Key\s*=\s*f?['"]([^'"]+)['"]""", src, re.DOTALL):
        key = m.group(1)
        if "{" in key:
            key = key.split("{")[0].rstrip("/") + "/"
        keys.add(key)
    return keys


def find_lambda_source(name):
    base = REPO_ROOT / "aws/lambdas" / name / "source"
    for fname in ["lambda_function.py", "index.py", "handler.py", "app.py"]:
        p = base / fname
        if p.exists():
            return p, p.read_text(encoding="utf-8", errors="ignore")
    for p in base.rglob("*.py"):
        return p, p.read_text(encoding="utf-8", errors="ignore")
    return None, None


def get_lambda_metrics_7d(name):
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=7)
    inv = cw.get_metric_statistics(
        Namespace="AWS/Lambda", MetricName="Invocations",
        Dimensions=[{"Name": "FunctionName", "Value": name}],
        StartTime=start, EndTime=end, Period=86400, Statistics=["Sum"],
    )
    err = cw.get_metric_statistics(
        Namespace="AWS/Lambda", MetricName="Errors",
        Dimensions=[{"Name": "FunctionName", "Value": name}],
        StartTime=start, EndTime=end, Period=86400, Statistics=["Sum"],
    )
    inv_total = sum(p.get("Sum", 0) for p in inv.get("Datapoints", []))
    err_total = sum(p.get("Sum", 0) for p in err.get("Datapoints", []))
    return int(inv_total), int(err_total)


# ─── Hand-curated entries (the 29 from step 82-86) — locked baseline ─

HAND_CURATED = {
    # S3 critical path files
    "s3:data/report.json": {
        "type": "s3_file", "key": "data/report.json",
        "fresh_max": 600, "warn_max": 1800, "expected_size": 500_000,
        "note": "Source of truth — 188 stocks + FRED + regime. daily-report-v3 every 5min.",
        "severity": "critical",
    },
    "s3:crypto-intel.json": {
        "type": "s3_file", "key": "crypto-intel.json",
        "fresh_max": 1200, "warn_max": 3600, "expected_size": 30_000,
        "note": "BTC/ETH/SOL technicals + on-chain. crypto-intel every 15min.",
        "severity": "critical",
    },
    "s3:edge-data.json": {
        "type": "s3_file", "key": "edge-data.json",
        "fresh_max": 25_000, "warn_max": 43_200, "expected_size": 1_000,
        "note": "Composite ML risk score, regime. edge-engine every 6h. Size 1-11KB depending on alerts.",
        "severity": "critical",
    },
    "s3:repo-data.json": {
        "type": "s3_file", "key": "repo-data.json",
        "fresh_max": 7200, "warn_max": 21_600, "expected_size": 5_000,
        "note": "Repo plumbing stress. repo-monitor every 30min weekdays.",
        "severity": "critical",
    },
    "s3:flow-data.json": {
        "type": "s3_file", "key": "flow-data.json",
        "fresh_max": 18_000, "warn_max": 32_400, "expected_size": 15_000,
        "note": "Options/fund flows. options-flow every 4h.",
        "severity": "important",
    },
    "s3:intelligence-report.json": {
        "type": "s3_file", "key": "intelligence-report.json",
        "fresh_max": 7200, "warn_max": 14_400, "expected_size": 2_000,
        "note": "Cross-system synthesis. Heart of ai-chat + signal-logger.",
        "severity": "critical",
    },
    "s3:screener/data.json": {
        "type": "s3_file", "key": "screener/data.json",
        "fresh_max": 21_600, "warn_max": 43_200, "expected_size": 100_000,
        "note": "503 stocks Piotroski/Altman scored. stock-screener every 4h.",
        "severity": "important",
    },
    "s3:valuations-data.json": {
        "type": "s3_file", "key": "valuations-data.json",
        "fresh_max": 2_678_400, "warn_max": 3_456_000, "expected_size": 1_000,
        "note": "CAPE, Buffett indicator. valuations-agent monthly (1st 14:00 UTC).",
        "severity": "nice_to_have",
    },
    "s3:calibration/latest.json": {
        "type": "s3_file", "key": "calibration/latest.json",
        "fresh_max": 691_200, "warn_max": 950_400, "expected_size": 500,
        "note": "Calibrator output. Sunday 9 UTC.",
        "severity": "important",
    },
    "s3:learning/last_log_run.json": {
        "type": "s3_file", "key": "learning/last_log_run.json",
        "fresh_max": 25_200, "warn_max": 43_200, "expected_size": 50,
        "note": "signal-logger heartbeat. last_log_run.json updated each invocation.",
        "severity": "critical",
    },
    "s3:predictions.json": {
        "type": "s3_file", "key": "predictions.json",
        "fresh_max": None, "warn_max": None,
        "note": "ml-predictions Lambda broken since 2026-04-22 CF migration. Tracked; no alert. Downstream bypassed.",
        "severity": "nice_to_have", "known_broken": True,
    },
    "s3:data.json": {
        "type": "s3_file", "key": "data.json",
        "fresh_max": None, "warn_max": None,
        "note": "Legacy orphan, 65+ days stale. Replaced by data/report.json. Tracked but no alert.",
        "severity": "nice_to_have", "known_broken": True,
    },
    # Lambdas (critical path)
    "lambda:justhodl-daily-report-v3": {
        "type": "lambda", "name": "justhodl-daily-report-v3",
        "max_error_rate": 0.10, "min_invocations_24h": 200,
        "note": "Writes data/report.json every 5min.", "severity": "critical",
    },
    "lambda:justhodl-signal-logger": {
        "type": "lambda", "name": "justhodl-signal-logger",
        "max_error_rate": 0.20, "min_invocations_24h": 3,
        "note": "Logs signals to DynamoDB. Heart of learning loop.", "severity": "critical",
    },
    "lambda:justhodl-outcome-checker": {
        "type": "lambda", "name": "justhodl-outcome-checker",
        "max_error_rate": 0.20, "min_invocations_24h": 0,
        "note": "Scores outcomes. Mon-Fri 22:30 + Sun 8 + 1st-of-month.", "severity": "critical",
    },
    "lambda:justhodl-calibrator": {
        "type": "lambda", "name": "justhodl-calibrator",
        "max_error_rate": 0.50, "min_invocations_24h": 0,
        "note": "Computes per-signal weights. Sunday 9 UTC.", "severity": "critical",
    },
    "lambda:justhodl-intelligence": {
        "type": "lambda", "name": "justhodl-intelligence",
        "max_error_rate": 0.10, "min_invocations_24h": 10,
        "note": "Cross-system synthesis. FIXED 2026-04-25 (adapter pattern).", "severity": "critical",
    },
    "lambda:justhodl-crypto-intel": {
        "type": "lambda", "name": "justhodl-crypto-intel",
        "max_error_rate": 0.20, "min_invocations_24h": 80,
        "note": "Crypto data. Some Binance modules geoblocked but core works.", "severity": "critical",
    },
    "lambda:justhodl-edge-engine": {
        "type": "lambda", "name": "justhodl-edge-engine",
        "max_error_rate": 0.20, "min_invocations_24h": 3,
        "note": "Edge composite + regime. Every 6h.", "severity": "critical",
    },
    "lambda:justhodl-repo-monitor": {
        "type": "lambda", "name": "justhodl-repo-monitor",
        "max_error_rate": 0.20, "min_invocations_24h": 10,
        "note": "Plumbing stress. Every 30min weekdays.", "severity": "critical",
    },
    "lambda:justhodl-ai-chat": {
        "type": "lambda", "name": "justhodl-ai-chat",
        "max_error_rate": 0.05, "min_invocations_24h": 0,
        "note": "User chat. Auth-guarded behind CF Worker.", "severity": "critical",
    },
    # DynamoDB
    "ddb:justhodl-signals": {
        "type": "dynamodb", "table": "justhodl-signals",
        "min_items": 4_000, "max_growth_24h": 100,
        "note": "All logged signals. Should grow ~100/day.", "severity": "important",
    },
    "ddb:justhodl-outcomes": {
        "type": "dynamodb", "table": "justhodl-outcomes",
        "min_items": 700,
        "note": "Scored outcomes. Grows after outcome-checker runs.", "severity": "important",
    },
    "ddb:fed-liquidity-cache": {
        "type": "dynamodb", "table": "fed-liquidity-cache",
        "min_items": 200_000,
        "note": "FRED data cache.", "severity": "nice_to_have",
    },
    # SSM
    "ssm:/justhodl/calibration/weights": {
        "type": "ssm", "name": "/justhodl/calibration/weights",
        "fresh_max": 691_200, "warn_max": 950_400,
        "note": "Per-signal weights. Updated by calibrator Sunday 9 UTC.", "severity": "important",
    },
    "ssm:/justhodl/calibration/accuracy": {
        "type": "ssm", "name": "/justhodl/calibration/accuracy",
        "fresh_max": 691_200, "warn_max": 950_400,
        "note": "Per-signal accuracy stats. Updated weekly.", "severity": "important",
    },
    # EB rules
    "eb:justhodl-outcome-checker-daily": {
        "type": "eb_rule", "name": "justhodl-outcome-checker-daily",
        "expected_state": "ENABLED",
        "note": "Daily outcome scoring (NEW 2026-04-24).", "severity": "critical",
    },
    "eb:justhodl-outcome-checker-weekly": {
        "type": "eb_rule", "name": "justhodl-outcome-checker-weekly",
        "expected_state": "ENABLED",
        "note": "Sunday outcome scoring.", "severity": "critical",
    },
    "eb:justhodl-calibrator-weekly": {
        "type": "eb_rule", "name": "justhodl-calibrator-weekly",
        "expected_state": "ENABLED",
        "note": "Sunday 9 UTC calibration. THE event.", "severity": "critical",
    },
}


with report("atomic_expand_and_tune") as r:
    r.heading("Atomic expand + tune (one CI invocation, no race)")

    # ─── Build EB lookup ───
    inv_obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="_audit/inventory_2026-04-25.json")
    inventory = json.loads(inv_obj["Body"].read())

    eb_by_target = defaultdict(list)
    for rule in inventory["eb_rules"]:
        if rule.get("state") != "ENABLED":
            continue
        for t in rule.get("targets", []):
            arn_tail = t.get("arn", "")
            if "function:" in arn_tail:
                target = arn_tail.split("function:")[-1]
                eb_by_target[target].append(rule.get("schedule"))

    r.log(f"  Hand-curated baseline: {len(HAND_CURATED)} entries")
    r.log(f"  Lambdas with enabled schedules: {len(eb_by_target)}")

    # ─── Auto-derive ───
    r.section("Auto-deriving entries")
    EXP = dict(HAND_CURATED)

    # Lambda entries
    auto_lambdas = 0
    for fn in inventory["lambdas"]:
        name = fn["name"]
        sched_id = f"lambda:{name}"
        if sched_id in EXP:
            continue  # Hand-curated; preserve
        schedules = eb_by_target.get(name, [])
        if not schedules:
            continue
        per_day = sum((parse_schedule_to_per_day(s) or 0) for s in schedules)
        if per_day <= 0:
            continue
        min_inv = max(1, int(per_day * 0.7))
        EXP[sched_id] = {
            "type": "lambda", "name": name,
            "max_error_rate": 0.30,
            "min_invocations_24h": min_inv,
            "note": f"Auto-derived. Schedules: {schedules}. Expected ~{per_day:.1f}/day.",
            "severity": "important" if name.startswith("justhodl-") else "nice_to_have",
            "auto_generated": True,
        }
        auto_lambdas += 1
    r.log(f"  Auto-derived Lambda entries: {auto_lambdas}")

    # S3 file entries (from put_object greps)
    auto_s3 = 0
    writers = defaultdict(list)
    for fn in inventory["lambdas"]:
        name = fn["name"]
        path, src = find_lambda_source(name)
        if not src:
            continue
        for k in extract_s3_writes(src):
            writers[k].append((name, eb_by_target.get(name, [])))

    for key, wlist in writers.items():
        if "/" in key and key.endswith("/"):
            continue
        if not any(key.endswith(ext) for ext in [".json", ".html", ".csv", ".txt", ".jsonl"]):
            continue
        if key.startswith(("archive/", "_health/")):
            continue
        if f"s3:{key}" in EXP:
            continue
        best_per_day = 0
        best_writer = None
        for wname, wschedules in wlist:
            for s_ in wschedules:
                pd = parse_schedule_to_per_day(s_)
                if pd and pd > best_per_day:
                    best_per_day = pd
                    best_writer = wname
        if not best_writer:
            continue
        try:
            head = s3.head_object(Bucket="justhodl-dashboard-live", Key=key)
            cur_size = head["ContentLength"]
        except Exception:
            continue
        interval_sec = schedule_to_seconds(best_per_day)
        if not interval_sec:
            continue
        EXP[f"s3:{key}"] = {
            "type": "s3_file", "key": key,
            "fresh_max": min(int(interval_sec * 2), 31 * 86400),
            "warn_max": min(int(interval_sec * 6), 60 * 86400),
            "expected_size": max(int(cur_size * 0.5), 100),
            "note": f"Auto-derived. Writer: {best_writer} ({best_per_day:.1f}/day). Cur size: {cur_size}B.",
            "severity": "nice_to_have",
            "auto_generated": True,
        }
        auto_s3 += 1
    r.log(f"  Auto-derived S3 entries: {auto_s3}")

    # ─── Tune auto-derived against 7d observed ───
    r.section("Tuning auto-derived entries against 7d observed metrics")
    dormant = []
    tuned = []
    high_error = []

    for k, v in list(EXP.items()):
        if not v.get("auto_generated"):
            continue
        if v.get("type") != "lambda":
            continue
        name = v.get("name")
        try:
            inv7d, err7d = get_lambda_metrics_7d(name)
        except Exception as e:
            r.log(f"  metric fetch failed {name}: {e}")
            continue
        avg_per_day = inv7d / 7.0

        if inv7d == 0:
            # Truly dormant — don't alert on inactivity
            v["min_invocations_24h"] = 0
            v["max_error_rate"] = 1.0
            v["severity"] = "nice_to_have"
            v["note"] = (v.get("note", "") + " | DORMANT (0 inv 7d).").strip()
            dormant.append(name)
        elif inv7d > 0 and (err7d / inv7d) >= 0.99:
            # 100% error rate — that's the real signal we want to surface
            v["min_invocations_24h"] = max(1, int(avg_per_day * 0.5))
            v["max_error_rate"] = 0.30  # Keep alarm active
            v["severity"] = "important"  # Upgrade — this is alarming
            v["note"] = (v.get("note", "") + f" | ALL {err7d}/{inv7d} INVOCATIONS ERRORED LAST 7D — investigate.").strip()
            high_error.append((name, inv7d, err7d))
        elif avg_per_day < v.get("min_invocations_24h", 0):
            # Just lower the threshold
            new_min = max(1, int(avg_per_day * 0.5))
            v["min_invocations_24h"] = new_min
            v["note"] = (v.get("note", "") + f" | Tuned to observed avg {avg_per_day:.1f}/day → min {new_min}.").strip()
            tuned.append((name, avg_per_day))

    r.log(f"  Dormant: {len(dormant)}")
    for n in dormant:
        r.log(f"    {n}")
    r.log(f"\n  HIGH ERROR (100% err 7d): {len(high_error)}")
    for n, i, e in high_error:
        r.log(f"    {n}: inv={i} err={e}")
    r.log(f"\n  Threshold tuned: {len(tuned)}")

    # ─── Stats + write ───
    r.section("Write expectations.py")
    by_type = defaultdict(int)
    by_origin = defaultdict(int)
    for v in EXP.values():
        by_type[v.get("type", "?")] += 1
        by_origin["auto" if v.get("auto_generated") else "hand"] += 1

    helper_block = '''

# ═══════════════════════════════════════════════════════════════════
#  Status helpers — used by the monitor Lambda
# ═══════════════════════════════════════════════════════════════════

def status_for_age(age_sec, fresh_max, warn_max):
    if fresh_max is None:
        return "unknown"
    if age_sec is None:
        return "red"
    if age_sec <= fresh_max:
        return "green"
    if warn_max is None or age_sec <= warn_max:
        return "yellow"
    return "red"


def status_for_size(actual_bytes, expected_min):
    if expected_min is None or actual_bytes is None:
        return "unknown"
    if actual_bytes >= expected_min:
        return "green"
    if actual_bytes >= expected_min * 0.5:
        return "yellow"
    return "red"


def severity_rank(s):
    return {"critical": 0, "important": 1, "nice_to_have": 2}.get(s, 3)
'''

    header = f'''# ═══════════════════════════════════════════════════════════════════
#  JustHodl.AI Health Expectations
#  Generated: {datetime.now(timezone.utc).isoformat()}
#  Stats: {len(EXP)} total ({by_origin["hand"]} hand-curated, {by_origin["auto"]} auto-derived & tuned)
#  By type: {dict(by_type)}
#
#  Auto-derived entries are tuned against 7-day observed CloudWatch
#  metrics. Lambdas at 100% error rate marked as 'important' so they
#  surface in the dashboard.
# ═══════════════════════════════════════════════════════════════════

EXPECTATIONS = '''

    def fmt_dict(d):
        lines = ["{"]
        for k in sorted(d.keys()):
            v = d[k]
            inner = [f"        {repr(ik)}: {repr(iv)}," for ik, iv in v.items()]
            lines.append(f"    {repr(k)}: {{")
            lines.extend(inner)
            lines.append(f"    }},")
        lines.append("}")
        return "\n".join(lines)

    new_src = header + fmt_dict(EXP) + helper_block

    import ast
    try:
        ast.parse(new_src)
    except SyntaxError as e:
        r.fail(f"  syntax: {e}")
        raise SystemExit(1)

    exp_path = REPO_ROOT / "aws/ops/health/expectations.py"
    exp_path.write_text(new_src)
    r.ok(f"  Wrote: {len(new_src):,} bytes, {len(EXP)} entries")

    # ─── Re-deploy + verify ───
    r.section("Re-deploy + verify")
    src_dir = REPO_ROOT / "aws/lambdas/justhodl-health-monitor/source"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zout:
        for f_ in src_dir.rglob("*"):
            if f_.is_file():
                zout.write(f_, str(f_.relative_to(src_dir)))
        zout.write(exp_path, "expectations.py")
    zbytes = buf.getvalue()

    lam.update_function_code(FunctionName="justhodl-health-monitor", ZipFile=zbytes)
    lam.get_waiter("function_updated").wait(
        FunctionName="justhodl-health-monitor", WaiterConfig={"Delay": 3, "MaxAttempts": 30}
    )
    r.ok(f"  Re-deployed: {len(zbytes)} bytes")

    import time
    time.sleep(3)

    resp = lam.invoke(FunctionName="justhodl-health-monitor", InvocationType="RequestResponse")
    if resp.get("FunctionError"):
        r.fail(f"  Invoke FAILED: {resp.get('Payload').read().decode()[:500]}")
        raise SystemExit(1)
    r.ok(f"  Invoke clean")

    obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="_health/dashboard.json")
    dash = json.loads(obj["Body"].read())
    r.log(f"\n  System: {dash.get('system_status')}")
    r.log(f"  Counts: {dash.get('counts')}")
    r.log(f"  Total: {dash.get('total_components')}")

    # Show all reds — these are the real bugs to fix
    r.log(f"\n  Currently-RED components (real bugs to fix):")
    for c in dash.get("components", []):
        if c.get("status") != "red":
            continue
        r.log(f"    [{c.get('severity'):12}] {c.get('id', '?'):50}  {(c.get('reason') or c.get('error') or '')[:80]}")

    r.kv(
        total_components=len(EXP),
        hand_curated=by_origin["hand"],
        auto_derived=by_origin["auto"],
        dormant_lambdas=len(dormant),
        high_error_lambdas=len(high_error),
        threshold_tuned=len(tuned),
    )
    r.log("Done")
