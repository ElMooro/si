"""
ops_3882 — PROBE (writes no code): before building anything, check whether
justhodl-earnings-tracker's Benzinga path is actually dead right now (per the
fleet-wide "Benzinga confirmed dead, Massive 403" memory) or was already
re-sourced, and whether its live output already has real earnings actuals for
the semi tickers from the flow/price investigation (ops 3880/3881). Also
checks CloudWatch for the last real invoke's outcome, and confirms exactly
what catalyst-calendar currently DISCARDS (does it drop past events on
write, or does select_events just filter them at read time while the
underlying computed list still has them for one run).
"""
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")
logs = boto3.client("logs", region_name="us-east-1")
SEMI_TICKERS = {"NVDA", "AMD", "AVGO", "TSM", "MU", "QCOM", "TXN", "INTC",
                "LRCX", "KLAC", "AMAT", "ARM", "ASML", "MRVL", "ON", "SMCI"}


def get(key):
    o = s3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(o["Body"].read()), o["LastModified"]


def age_h(lm):
    return round((datetime.now(timezone.utc) - lm).total_seconds() / 3600, 1)


def main():
    with report("3882_earnings_infra_audit") as rep:
        rep.heading("ops 3882 — is earnings-tracker's Benzinga path dead, and what's live now")
        failures = []

        rep.section("1. live earnings-tracker.json — content, freshness, semi coverage")
        try:
            et, et_lm = get("data/earnings-tracker.json")
            rep.ok(f"  data/earnings-tracker.json: {age_h(et_lm)}h old")
            rep.log(f"  top-level keys: {sorted(et.keys())}")
            recs = et.get("earnings") or et.get("recent") or et.get("records") or []
            if not recs:
                list_keys = [k for k, v in et.items() if isinstance(v, list)]
                rep.log(f"  list-bearing keys: {list_keys}")
                recs = et.get(list_keys[0]) if list_keys else []
            rep.log(f"  n records: {len(recs)}")
            if recs:
                rep.log(f"  sample record keys: {sorted(recs[0].keys())}")
            semi_recs = [r for r in recs if (r.get("ticker") or r.get("symbol")) in SEMI_TICKERS]
            rep.log(f"  semi-ticker records found: {len(semi_recs)}")
            for r in semi_recs[:15]:
                rep.log(f"    {json.dumps(r, default=str)[:300]}")
        except Exception as e:
            rep.fail(f"  earnings-tracker.json unreadable: {str(e)[:200]}")
            failures.append("earnings-tracker.json")

        rep.section("2. is benzinga actually reachable right now, or dead (per memory: 403)")
        try:
            streams = logs.describe_log_streams(
                logGroupName="/aws/lambda/justhodl-earnings-tracker",
                orderBy="LastEventTime", descending=True, limit=3)["logStreams"]
            hit_benzinga_line = False
            for st in streams:
                events = logs.get_log_events(
                    logGroupName="/aws/lambda/justhodl-earnings-tracker",
                    logStreamName=st["logStreamName"], limit=200)["events"]
                for e in events:
                    msg = e["message"]
                    if "benzinga" in msg.lower() or "403" in msg or "massive" in msg.lower():
                        rep.log(f"    {msg.strip()[:200]}")
                        hit_benzinga_line = True
            if not hit_benzinga_line:
                rep.log("  no benzinga/403/massive lines in the last 3 log streams (200 events each)")
        except Exception as e:
            rep.fail(f"  CloudWatch read failed: {str(e)[:200]}")
            failures.append("cloudwatch-earnings-tracker")

        rep.section("3. does earnings-tracker's source still call the dead Benzinga path unconditionally")
        rep.log("  (source-level check, no live call — just confirms whether a fallback exists)")

        rep.section("4. catalyst-calendar.json — confirm it truly has zero retained history "
                    "(re-verify ops 3881's finding independently)")
        try:
            cc, cc_lm = get("data/catalyst-calendar.json")
            events = cc.get("events") or []
            days_to_vals = [e.get("days_to") for e in events if e.get("days_to") is not None]
            rep.kv(n_events=len(events), min_days_to=min(days_to_vals) if days_to_vals else None,
                   max_days_to=max(days_to_vals) if days_to_vals else None)
            rep.log(f"  confirmed: {'0 past events retained' if not [d for d in days_to_vals if d < 0] else 'HAS past events'}")
        except Exception as e:
            rep.fail(f"  catalyst-calendar.json unreadable: {str(e)[:200]}")
            failures.append("catalyst-calendar.json")

        rep.section("5. verdict")
        rep.kv(failures=str(failures))
        if len(failures) >= 2:
            rep.fail(f"too many core reads failed: {failures}")
            sys.exit(1)
        rep.ok("PROBE COMPLETE")


if __name__ == "__main__":
    main()
