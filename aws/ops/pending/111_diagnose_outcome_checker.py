#!/usr/bin/env python3
"""
Step 111 — Diagnose outcome-checker.

The reports/scorecard.json shows scored=0 out of 4,307 outcomes.
That's a critical issue: it means the entire learning loop isn't
actually learning. Calibrator weights are meaningless because
they're computed from unscored outcomes.

Possible causes:
  1. Outcome-checker Lambda is erroring silently
  2. Outcome-checker EB rules are disabled
  3. Outcome timestamps haven't reached their check date yet
     (would explain "no scoring" if all signals are recent)
  4. Outcome-checker is running but writing 'correct=null' due to
     some scoring bug

Diagnostic plan:
  1. List EB rules + state for justhodl-outcome-checker
  2. Last 24h invocations + errors from CloudWatch
  3. Read most recent log stream — see what it's actually doing
  4. Sample outcomes:
     a. How many have check_timestamp in the past?  (i.e. should
        have been scoreable)
     b. What's the earliest unscored signal's logged_at + check_at?
  5. Does outcome-checker even have a recent successful run?

Output: aws/ops/audit/outcome_checker_diagnosis_2026-04-25.md
"""
import json
import os
from collections import Counter
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from pathlib import Path

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


with report("diagnose_outcome_checker") as r:
    r.heading("Diagnose outcome-checker — why are 4,307 outcomes unscored?")

    # ─── 1. EB rules targeting outcome-checker ──────────────────────────
    r.section("1. EventBridge rules for outcome-checker")
    name = "justhodl-outcome-checker"
    target_arn = f"arn:aws:lambda:us-east-1:857687956942:function:{name}"
    try:
        rule_names = eb.list_rule_names_by_target(TargetArn=target_arn).get("RuleNames", [])
        r.log(f"  Rules targeting {name}: {len(rule_names)}")
        for rn in rule_names:
            d = eb.describe_rule(Name=rn)
            r.log(f"    {rn:50} state={d.get('State'):10} schedule={d.get('ScheduleExpression')}")
    except Exception as e:
        r.fail(f"  Couldn't list EB rules: {e}")

    # ─── 2. Lambda invocation metrics ──────────────────────────────────
    r.section("2. CloudWatch metrics (last 7 days)")
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
    r.log(f"  Last 7d: {int(inv_total)} invocations, {int(err_total)} errors")

    # Daily breakdown
    r.log(f"  Daily breakdown:")
    daily_inv = {p["Timestamp"].date(): int(p.get("Sum", 0)) for p in inv.get("Datapoints", [])}
    daily_err = {p["Timestamp"].date(): int(p.get("Sum", 0)) for p in err.get("Datapoints", [])}
    all_days = sorted(set(list(daily_inv.keys()) + list(daily_err.keys())))
    for day in all_days[-7:]:
        invs = daily_inv.get(day, 0)
        errs = daily_err.get(day, 0)
        r.log(f"    {day}: inv={invs} err={errs}")

    # ─── 3. Most recent log stream ──────────────────────────────────────
    r.section("3. Most recent CloudWatch log stream")
    try:
        streams = logs.describe_log_streams(
            logGroupName=f"/aws/lambda/{name}",
            orderBy="LastEventTime", descending=True, limit=2,
        ).get("logStreams", [])
        for s in streams[:1]:
            r.log(f"  Stream: {s['logStreamName']}")
            r.log(f"  Last event: {datetime.fromtimestamp(s.get('lastEventTimestamp', 0)/1000, tz=timezone.utc) if s.get('lastEventTimestamp') else '?'}")
            ev = logs.get_log_events(
                logGroupName=f"/aws/lambda/{name}",
                logStreamName=s["logStreamName"],
                limit=40, startFromHead=False,
            )
            r.log(f"  Last 40 log lines:")
            for e in ev.get("events", [])[-40:]:
                msg = e["message"].rstrip()
                r.log(f"    {msg[:250]}")
    except Exception as e:
        r.fail(f"  Couldn't read logs: {e}")

    # ─── 4. Outcomes deep-dive ──────────────────────────────────────────
    r.section("4. Outcomes table inspection")
    t = ddb.Table("justhodl-outcomes")

    # Sample 5 outcomes — what do unscored vs scored look like?
    scan = t.scan(Limit=10)
    r.log(f"  10 sampled outcomes:")
    for item in scan.get("Items", [])[:10]:
        i = d2f(item)
        r.log(f"    signal_type={i.get('signal_type'):25} window={i.get('window_key'):8} "
              f"correct={i.get('correct')!r:6} outcome={str(i.get('outcome'))[:40]} "
              f"checked_at={str(i.get('checked_at'))[:19]}")

    # Aggregate stats
    r.log(f"\n  Full scan for stats…")
    stats = Counter()
    by_window = Counter()
    correct_values = Counter()
    sample_unscored = []
    n = 0
    kwargs = {}
    while True:
        resp = t.scan(**kwargs)
        for item in resp.get("Items", []):
            i = d2f(item)
            n += 1
            wk = i.get("window_key", "?")
            by_window[wk] += 1
            cv = i.get("correct")
            correct_values[str(cv)] += 1
            if cv is None and len(sample_unscored) < 3:
                sample_unscored.append(i)
        if "LastEvaluatedKey" not in resp:
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
        if n > 5000:
            break

    r.log(f"  Total outcomes scanned: {n}")
    r.log(f"  Distribution of 'correct' values:")
    for v, cnt in correct_values.most_common():
        r.log(f"    {v:15} {cnt}")
    r.log(f"  Distribution of window_key:")
    for v, cnt in by_window.most_common(10):
        r.log(f"    {v:15} {cnt}")

    # ─── 5. Are unscored outcomes "due"? ────────────────────────────────
    r.section("5. Are unscored outcomes overdue for scoring?")
    # Cross-reference outcomes with their parent signals to find check_timestamps
    if sample_unscored:
        sigs_table = ddb.Table("justhodl-signals")
        for unscored in sample_unscored[:3]:
            sid = unscored.get("signal_id")
            r.log(f"\n  Unscored outcome {unscored.get('outcome_id', '?')[:50]}")
            r.log(f"    signal_id: {sid}")
            r.log(f"    window_key: {unscored.get('window_key')}")
            r.log(f"    logged_at: {unscored.get('logged_at')}")
            r.log(f"    checked_at: {unscored.get('checked_at')}")
            try:
                sig_resp = sigs_table.get_item(Key={"signal_id": sid})
                if sig_resp.get("Item"):
                    sig = d2f(sig_resp["Item"])
                    cts = sig.get("check_timestamps", {})
                    r.log(f"    parent signal check_timestamps: {cts}")
                    # Is the relevant check_timestamp in the past?
                    wk = unscored.get("window_key", "")
                    matching_key = None
                    for k in cts:
                        # window_key might be "day_30" or just "30"
                        if k.endswith(wk) or wk.endswith(k.replace("day_", "")):
                            matching_key = k
                            break
                    if matching_key:
                        check_at = cts[matching_key]
                        try:
                            check_dt = datetime.fromisoformat(str(check_at).replace("Z", "+00:00"))
                            now = datetime.now(timezone.utc)
                            if check_dt <= now:
                                overdue_days = (now - check_dt).days
                                r.log(f"    ⚠  OVERDUE by {overdue_days} days! check_timestamp was {check_at}")
                            else:
                                until = (check_dt - now).days
                                r.log(f"    Not yet due (in {until} days)")
                        except Exception as e:
                            r.log(f"    Couldn't parse check_at: {e}")
            except Exception as e:
                r.warn(f"    Couldn't fetch parent signal: {e}")

    # ─── 6. Final summary ───────────────────────────────────────────────
    r.section("6. Summary")
    r.log(f"  - Outcomes total: {n}")
    r.log(f"  - Scored (correct in [True, False]): {correct_values.get('True', 0) + correct_values.get('False', 0)}")
    r.log(f"  - Unscored (correct=None): {correct_values.get('None', 0)}")
    r.log(f"  - Last-7d inv: {int(inv_total)}, errors: {int(err_total)}")

    r.kv(
        outcomes_total=n,
        outcomes_scored=correct_values.get("True", 0) + correct_values.get("False", 0),
        outcomes_unscored=correct_values.get("None", 0),
        last_7d_invocations=int(inv_total),
        last_7d_errors=int(err_total),
    )
    r.log("Done")
