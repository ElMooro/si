#!/usr/bin/env python3
"""
Step 117 — Signal-logger cadence investigation.

Goal: figure out whether justhodl-signal-logger is firing on schedule
and capturing the right signals at the right rate. Earlier observation
(step 108): 186 khalid_index signals all logged today, suggesting
either:
  a) logger only started capturing khalid_index recently, OR
  b) cadence is different from documented "every 6h", OR
  c) logger is firing way more frequently than expected

Diagnostic plan:
  1. EB rule(s) targeting signal-logger: name, schedule, state
  2. CloudWatch invocation history (last 30 days, daily breakdown)
  3. Read source: what does it actually log? Find every put_item
     to justhodl-signals
  4. Per-signal-type cadence: for each of the 15 signal_types in
     DDB, compute logged_at distribution — first/last/median gap
     between consecutive logs
  5. Recent activity: timestamps of last 10 logs per signal_type
  6. Cross-check: does the actual cadence match the EB schedule?
  7. Memory/preferences claim "every 6h" — verify against reality

Output: a clear "logger is healthy / logger has issue X" conclusion
plus a doc that updates our memory if needed.
"""
import json
import os
import re
from collections import defaultdict, Counter
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from pathlib import Path
from statistics import median

from ops_report import report
import boto3

REGION = "us-east-1"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

eb = boto3.client("events", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)
ddb = boto3.resource("dynamodb", region_name=REGION)


def d2f(o):
    if isinstance(o, Decimal): return float(o)
    if isinstance(o, dict):    return {k: d2f(v) for k, v in o.items()}
    if isinstance(o, list):    return [d2f(v) for v in o]
    return o


with report("diagnose_signal_logger_cadence") as r:
    r.heading("Signal-logger cadence investigation")

    name = "justhodl-signal-logger"

    # ─── 1. EB rules targeting signal-logger ────────────────────────────
    r.section("1. EventBridge rules for signal-logger")
    target_arn = f"arn:aws:lambda:us-east-1:857687956942:function:{name}"
    rules = []
    try:
        rule_names = eb.list_rule_names_by_target(TargetArn=target_arn).get("RuleNames", [])
        for rn in rule_names:
            d = eb.describe_rule(Name=rn)
            rules.append({
                "name": rn,
                "schedule": d.get("ScheduleExpression"),
                "state": d.get("State"),
            })
        r.log(f"  Rules targeting {name}: {len(rules)}")
        for rule in rules:
            r.log(f"    {rule['name']:50} state={rule['state']:10} schedule={rule['schedule']}")
    except Exception as e:
        r.fail(f"  EB list failed: {e}")

    if not rules:
        r.warn("  ⚠  NO EB rules — Lambda exists but has no schedule!")
        r.warn("  This would explain why khalid_index signals are bursty.")

    # ─── 2. CloudWatch invocation history (30 days) ─────────────────────
    r.section("2. CloudWatch invocation history (last 30 days)")
    end = datetime.now(timezone.utc)
    start_30 = end - timedelta(days=30)
    try:
        inv = cw.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName="Invocations",
            Dimensions=[{"Name": "FunctionName", "Value": name}],
            StartTime=start_30, EndTime=end, Period=86400, Statistics=["Sum"],
        )
        err = cw.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName="Errors",
            Dimensions=[{"Name": "FunctionName", "Value": name}],
            StartTime=start_30, EndTime=end, Period=86400, Statistics=["Sum"],
        )
        inv_total = sum(p.get("Sum", 0) for p in inv.get("Datapoints", []))
        err_total = sum(p.get("Sum", 0) for p in err.get("Datapoints", []))
        r.log(f"  30-day total: {int(inv_total)} invocations, {int(err_total)} errors")

        # Daily breakdown
        daily_inv = {p["Timestamp"].date(): int(p.get("Sum", 0)) for p in inv.get("Datapoints", [])}
        daily_err = {p["Timestamp"].date(): int(p.get("Sum", 0)) for p in err.get("Datapoints", [])}
        all_days = sorted(set(list(daily_inv.keys()) + list(daily_err.keys())))
        r.log(f"  Daily breakdown:")
        for day in all_days[-15:]:  # Last 15 days
            invs = daily_inv.get(day, 0)
            errs = daily_err.get(day, 0)
            bar = "█" * min(invs, 30)
            r.log(f"    {day}: inv={invs:>3} err={errs} {bar}")

        # If logger ran every 6h, we'd expect 4 inv/day. Deviation suggests issue.
        recent_avg = (sum(daily_inv.get(d, 0) for d in all_days[-7:]) / 7) if all_days else 0
        r.log(f"\n  Recent 7-day avg: {recent_avg:.1f} invocations/day")
        if 0 < recent_avg < 1:
            r.warn(f"  ⚠  Less than 1/day — logger appears mostly inactive")
        elif 3 <= recent_avg <= 5:
            r.ok(f"  Matches expected 4/day (every 6h)")
        elif recent_avg > 10:
            r.warn(f"  ⚠  Way more than expected — running far more often than 6h")
        else:
            r.warn(f"  ⚠  Cadence unexpected (expected 4/day for 6h schedule)")
    except Exception as e:
        r.fail(f"  CW metrics failed: {e}")

    # ─── 3. Most recent log stream — what does the logger output? ──────
    r.section("3. Most recent log stream — what does the logger do?")
    try:
        streams = logs.describe_log_streams(
            logGroupName=f"/aws/lambda/{name}",
            orderBy="LastEventTime", descending=True, limit=2,
        ).get("logStreams", [])
        for s in streams[:1]:
            last_ts = datetime.fromtimestamp(s.get("lastEventTimestamp", 0)/1000, tz=timezone.utc) if s.get("lastEventTimestamp") else None
            r.log(f"  Stream: {s['logStreamName']}")
            r.log(f"  Last event: {last_ts}")
            ev = logs.get_log_events(
                logGroupName=f"/aws/lambda/{name}",
                logStreamName=s["logStreamName"],
                limit=80, startFromHead=False,
            )
            r.log(f"  Last 80 log lines:")
            for e in ev.get("events", [])[-80:]:
                msg = e["message"].rstrip()
                r.log(f"    {msg[:240]}")
    except Exception as e:
        r.fail(f"  Log fetch failed: {e}")

    # ─── 4. Source: what signal types does it write? ────────────────────
    r.section("4. Source code — what signal types does logger write?")
    src_dir = REPO_ROOT / "aws/lambdas/justhodl-signal-logger/source"
    if src_dir.exists():
        for p in src_dir.rglob("*.py"):
            content = p.read_text(encoding="utf-8", errors="ignore")
            r.log(f"  {p.relative_to(REPO_ROOT)} — {content.count(chr(10))} LOC")

            # Find signal_type literal strings
            sig_types = set()
            for m in re.finditer(r"""['"]signal_type['"]\s*:\s*['"]([a-zA-Z_]+)['"]""", content):
                sig_types.add(m.group(1))
            # Also find signal_type variable assignments
            for m in re.finditer(r"""signal_type\s*=\s*['"]([a-zA-Z_]+)['"]""", content):
                sig_types.add(m.group(1))
            r.log(f"  Hardcoded signal_types: {sorted(sig_types)}")

            # Find every put_item or batch_write
            n_put = len(re.findall(r"\bput_item\s*\(", content))
            n_batch = len(re.findall(r"\bbatch_writer\b", content))
            r.log(f"  put_item calls: {n_put}, batch_writer calls: {n_batch}")
    else:
        r.warn(f"  Source not in repo at {src_dir}")

    # ─── 5. DDB cadence analysis: per signal_type, gap distribution ─────
    r.section("5. Signal-logger output cadence (from DDB timestamps)")
    sigs_table = ddb.Table("justhodl-signals")
    by_type_times = defaultdict(list)

    kwargs = {"ProjectionExpression": "signal_type, logged_at"}
    n = 0
    while True:
        resp = sigs_table.scan(**kwargs)
        for item in resp.get("Items", []):
            st = item.get("signal_type")
            ts = item.get("logged_at")
            if st and ts:
                try:
                    dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
                    by_type_times[st].append(dt)
                except Exception:
                    pass
            n += 1
        if "LastEvaluatedKey" not in resp:
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
        if n > 8000:
            break

    r.log(f"  Total signals scanned: {n}")
    r.log(f"\n  Per-signal-type cadence:")
    r.log(f"  {'signal_type':28} {'count':>6}  {'first':>11}  {'last':>11}  {'median_gap':>12}  {'expected':>10}")

    # Expected cadence per signal type. The signal-logger likely fires
    # all of these at once on its EB schedule. So all should show similar
    # cadence (= the EB schedule frequency).
    EXPECTED_GAPS = {
        # Most signals: the whole logger fires together, so cadence = EB schedule
        # If "every 6h" → median gap ~6h. If "every 1h" → median gap ~1h.
        # Rather than guess, let's compute and compare against rules[0]
    }
    expected_str = "?"
    if rules:
        sched = rules[0].get("schedule") or ""
        if sched.startswith("rate("):
            m = re.match(r"rate\((\d+)\s*(\w+)\)", sched)
            if m:
                n_unit, unit = int(m.group(1)), m.group(2).rstrip("s").lower()
                expected_str = f"{n_unit} {unit}"

    cadence_findings = []
    for st in sorted(by_type_times.keys(), key=lambda x: -len(by_type_times[x])):
        times = sorted(by_type_times[st])
        cnt = len(times)
        if cnt < 2:
            continue
        first = times[0]
        last = times[-1]
        gaps_seconds = [(times[i+1] - times[i]).total_seconds() for i in range(len(times)-1)]
        med_gap = median(gaps_seconds)
        # Format gap
        if med_gap < 90:
            gap_str = f"{med_gap:.0f}s"
        elif med_gap < 5400:
            gap_str = f"{med_gap/60:.0f}m"
        elif med_gap < 86400:
            gap_str = f"{med_gap/3600:.1f}h"
        else:
            gap_str = f"{med_gap/86400:.1f}d"

        first_str = first.strftime("%m-%d %H:%M")
        last_str = last.strftime("%m-%d %H:%M")
        r.log(f"  {st:28} {cnt:>6}  {first_str:>11}  {last_str:>11}  {gap_str:>12}  {expected_str:>10}")
        cadence_findings.append({
            "signal_type": st, "count": cnt, "first": first.isoformat(),
            "last": last.isoformat(), "median_gap_s": med_gap, "median_gap_str": gap_str,
        })

    # ─── 6. Comparison to expected ───────────────────────────────────────
    r.section("6. Cadence verdict")
    if not rules:
        r.fail("  ⚠  No EB rule — logger has no schedule")
    elif rules:
        sched = rules[0].get("schedule") or ""
        m = re.match(r"rate\((\d+)\s*(\w+)\)", sched)
        if m:
            n_unit, unit = int(m.group(1)), m.group(2).rstrip("s").lower()
            unit_seconds = {"minute": 60, "hour": 3600, "day": 86400}.get(unit)
            expected_seconds = n_unit * unit_seconds if unit_seconds else None
            if expected_seconds:
                r.log(f"  EB schedule: {sched} → expected median gap ~{expected_seconds}s")
                # Check each cadence finding
                tolerance = 0.3  # 30% tolerance
                aligned = []
                misaligned = []
                for c in cadence_findings:
                    ratio = c["median_gap_s"] / expected_seconds
                    if 1 - tolerance <= ratio <= 1 + tolerance:
                        aligned.append(c["signal_type"])
                    else:
                        misaligned.append((c["signal_type"], c["median_gap_str"], f"{ratio:.1f}x"))
                r.log(f"\n  Aligned with EB schedule ({len(aligned)}):")
                for st in aligned:
                    r.log(f"    ✅ {st}")
                if misaligned:
                    r.log(f"\n  Misaligned ({len(misaligned)}):")
                    for st, gap, ratio in misaligned:
                        r.log(f"    ⚠  {st:28} gap={gap:>10} ({ratio} of expected)")

    # ─── 7. Burst analysis: are most signals from a single hour? ────────
    r.section("7. Burst analysis — recency of last-24h signals")
    cutoff_24h = datetime.now(timezone.utc) - timedelta(hours=24)
    recent = []
    for st, times in by_type_times.items():
        for t in times:
            if t >= cutoff_24h:
                recent.append((t, st))
    recent.sort()
    by_hour = Counter()
    for t, _ in recent:
        by_hour[t.strftime("%Y-%m-%d %H")] += 1
    r.log(f"  Last 24h: {len(recent)} signal logs total")
    r.log(f"  By hour:")
    for hr in sorted(by_hour.keys()):
        r.log(f"    {hr}: {by_hour[hr]:>4} signals  {'█'*min(by_hour[hr]//5, 50)}")

    # ─── 8. Save audit doc ──────────────────────────────────────────────
    r.section("8. Save audit doc")
    doc_path = REPO_ROOT / "aws/ops/audit/signal_logger_cadence_2026-04-25.md"
    md = []
    md.append(f"# Signal-Logger Cadence Audit — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n")
    md.append("## EventBridge schedule\n")
    if rules:
        for rule in rules:
            md.append(f"- `{rule['name']}` — `{rule['schedule']}` ({rule['state']})")
    else:
        md.append("- **None** — Lambda exists with no schedule")
    md.append("\n## CloudWatch invocations (last 7 days)\n")
    if 'recent_avg' in dir():
        md.append(f"- Recent 7-day average: **{recent_avg:.1f} invocations/day**")
    md.append("\n## Per-signal-type cadence\n")
    md.append("| signal_type | count | first | last | median gap |")
    md.append("|---|---:|---|---|---|")
    for c in cadence_findings:
        md.append(f"| `{c['signal_type']}` | {c['count']} | {c['first'][:16]} | {c['last'][:16]} | {c['median_gap_str']} |")
    md.append("\n## Last-24h burst by hour\n")
    md.append("| Hour (UTC) | Signals |")
    md.append("|---|---:|")
    for hr in sorted(by_hour.keys()):
        md.append(f"| {hr} | {by_hour[hr]} |")
    doc_path.write_text("\n".join(md))
    r.ok(f"  Wrote {doc_path.relative_to(REPO_ROOT)}")

    r.kv(
        eb_rules=len(rules),
        signal_types=len(cadence_findings),
        recent_avg_per_day=f"{recent_avg:.1f}" if 'recent_avg' in dir() else "?",
        last_24h_logs=len(recent),
    )
    r.log("Done")
