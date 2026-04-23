#!/usr/bin/env python3
"""
Pre-cleanup audit: verify each EB rule I'm about to delete.

Before deleting, for each rule we show:
  - Name + state + schedule
  - Every target (not just Lambdas — rules can target SQS, SNS, etc.)
  - Last invocation (if any)
  - Whether we've seen it in our memory/notes as intentional

Only after I see this list will I decide what to actually delete.

Candidates identified in health_check_followup (conservatively):

Category A — exact duplicates (same Lambda + same cron/rate):
  1. fmp-stock-picks-daily (2 rules with IDENTICAL cron)
  2. justhodl-crypto-15min vs justhodl-crypto-intel-schedule (both rate(15m) → crypto-intel)
  3. justhodl-ml-predictions-schedule vs justhodl-ml-schedule (both rate(4h))
  4. justhodl-edge-6h vs justhodl-edge-engine-6h (both rate(6h))

Category B — legacy daily-report duplicates that overlap v9-auto-refresh:
  5. justhodl-daily-8am (cron 13 UTC → daily-report-v3)
  6. justhodl-daily-v3 (cron 13 UTC → daily-report-v3)
  7. justhodl-v9-morning (MON-FRI 13 UTC → daily-report-v3)
  8. justhodl-v9-evening (MON-FRI 23 UTC → daily-report-v3)
  9. justhodl-morning-brief-daily (cron 13 UTC → ?)
  10. justhodl-8am (cron 13 UTC → ?)

Category C — disabled rules on global-liquidity-agent-v2:
  Already disabled — can delete 6 of them for tidiness.

CAUTION: v9-evening fires MON-FRI 23 UTC = 7 PM ET = market close.
That's probably INTENTIONAL — an evening-close snapshot. Keep it.
Similarly v9-morning at 13 UTC = 9 AM ET = market open, INTENTIONAL.

Revised deletion list after review:

  A1. fmp-stock-picks-daily (exact duplicate — one of the two)
  A2. justhodl-crypto-intel-schedule (newer name, redundant with justhodl-crypto-15min)
  A3. justhodl-ml-schedule (older name, redundant with justhodl-ml-predictions-schedule)
  A4. justhodl-edge-6h (redundant with justhodl-edge-engine-6h — name matches Lambda better)
  B5. justhodl-daily-8am (aliased to daily-report-v3 which also gets fired by v9-morning)
  B6. justhodl-daily-v3 (duplicate of morning-brief-daily/8am — pick one)
  B7. justhodl-morning-brief-daily (one of the 4 at 13 UTC — consolidate)
  B8. justhodl-8am (vague name, fires 13 UTC)
  C9-C14. 6× DISABLED global-liquidity-agent-v2 rules

That's ~14 deletions. We are CAUTIOUS:
  - Keep justhodl-v9-auto-refresh (every 5 min) — THE primary pipeline
  - Keep justhodl-v9-morning + justhodl-v9-evening (market open/close snapshots)
  - Keep any rule with a non-Lambda target
  - Verify with dry-run printout before executing
"""
import os
from ops_report import report
import boto3

REGION = "us-east-1"
eb = boto3.client("events", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)

# Rules I think we can delete — exhaustive audit below
CANDIDATES = [
    # Category A — exact duplicates
    "justhodl-crypto-intel-schedule",      # duplicate of justhodl-crypto-15min
    "justhodl-ml-schedule",                # duplicate of justhodl-ml-predictions-schedule
    "justhodl-edge-6h",                    # duplicate of justhodl-edge-engine-6h
    # Category B — daily-report-v3 redundant morning triggers
    "justhodl-daily-8am",
    "justhodl-daily-v3",
    "justhodl-morning-brief-daily",
    "justhodl-8am",
    # Category C — DISABLED global-liquidity-agent-v2 rules (the agent is retired)
    "liquidity-critical-monitor",
    "liquidity-daily-8am",
    "liquidity-daily-report",
    "liquidity-daily-report-v2",
    "liquidity-hourly-v2",
    "liquidity-news-v2",
]

# KEEP (document explicitly so future-me knows why):
KEEP_EXPLICIT = {
    "justhodl-v9-auto-refresh": "THE main 5-min pipeline — never delete",
    "justhodl-v9-morning": "Market open snapshot (9 AM ET / 13 UTC MON-FRI)",
    "justhodl-v9-evening": "Market close snapshot (7 PM ET / 23 UTC MON-FRI)",
    "justhodl-crypto-15min": "Keep crypto-intel 15min trigger",
    "justhodl-ml-predictions-schedule": "Keep ml-predictions 4h trigger",
    "justhodl-edge-engine-6h": "Keep edge-engine 6h trigger (name matches Lambda)",
    "secretary-4h-scan": "Keep Secretary 4h trigger",
    "justhodl-signal-logger-6h": "Keep signal logger",
    "justhodl-stock-screener-4h": "Keep screener",
    "justhodl-outcome-checker-weekly": "Keep weekly Sunday outcomes",
    "justhodl-calibrator-weekly": "Keep weekly Sunday calibration",
    "justhodl-khalid-metrics-refresh": "Keep Khalid metrics daily",
    "cftc-cot-weekly-update": "Keep CFTC Friday update",
}

# Investigate fmp-stock-picks-daily — both listed had the same name (impossible?)
# It's probably one rule with 2 list-items shown from de-dup in my listing.


with report("eb_cleanup_audit") as r:
    r.heading("Pre-cleanup AUDIT — verify every candidate before any delete")

    r.section("1. For each candidate: state + schedule + all targets")
    missing = []
    keepable = []
    for name in CANDIDATES:
        try:
            rule = eb.describe_rule(Name=name)
            targets = eb.list_targets_by_rule(Rule=name).get("Targets", [])
            target_summaries = []
            for t in targets:
                arn = t.get("Arn", "")
                svc = arn.split(":")[2] if ":" in arn else "?"
                tail = arn.split(":")[-1]
                target_summaries.append(f"{svc}:{tail}")
            r.log(f"  {name}")
            r.log(f"    state: {rule.get('State')}")
            r.log(f"    schedule: {rule.get('ScheduleExpression', '(none)')}")
            r.log(f"    targets: {target_summaries}")
            # If a target is NOT a Lambda, flag for human review
            non_lambda_targets = [t for t in target_summaries if not t.startswith("lambda:")]
            if non_lambda_targets:
                r.warn(f"    ⚠ has non-Lambda targets: {non_lambda_targets}")
                keepable.append(name)
        except eb.exceptions.ResourceNotFoundException:
            r.log(f"  {name}: NOT FOUND (already deleted?)")
            missing.append(name)
        except Exception as e:
            r.warn(f"  {name}: {e}")

    # Also check: does fmp-stock-picks-daily really have 2 rules, or was that a display quirk?
    r.section("2. Check fmp-stock-picks-daily — suspected identical duplicate")
    try:
        rule = eb.describe_rule(Name="fmp-stock-picks-daily")
        r.log(f"  Single rule exists: {rule.get('ScheduleExpression')}")
        r.log(f"  (My earlier listing showed it twice — likely just a display artifact)")
    except Exception as e:
        r.warn(f"  fmp-stock-picks-daily lookup: {e}")

    # Summary
    r.section("3. Summary")
    to_delete = [c for c in CANDIDATES if c not in missing and c not in keepable]
    r.log(f"  Candidates reviewed: {len(CANDIDATES)}")
    r.log(f"  Already missing: {len(missing)}")
    r.log(f"  Keep (non-Lambda targets): {len(keepable)}")
    r.log(f"  Safe to delete: {len(to_delete)}")
    r.log("")
    r.log("  Will delete on next run:")
    for name in to_delete:
        r.log(f"    - {name}")
    r.log("")
    r.log("  KEEPING (for the record):")
    for name, why in KEEP_EXPLICIT.items():
        r.log(f"    - {name}: {why}")
    r.kv(candidates=len(CANDIDATES), to_delete=len(to_delete), keepable=len(keepable), missing=len(missing))

    r.log("Done — AUDIT ONLY. No deletes performed.")
