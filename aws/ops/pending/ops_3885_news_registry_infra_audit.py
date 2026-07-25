"""
ops_3885 — PROBE (writes no code): before building any new news/registry
infrastructure, verify what already exists is actually ALIVE, not just
declared. Config audit already found real discrepancies: news-wire's
docstring says "rate(15 minutes)" but the deployed cron is daily; news-
sentiment and feed-catalog show schedule=None in config.json entirely.

This checks, with real S3 timestamps and real content:
  1. data/news-wire.json — freshness, size, does it carry real semi-sector
     headlines from the actual window this investigation cares about
  2. sentiment/data.json (news-sentiment's output) — freshness, coverage
  3. data/feed-registry.json — freshness, does it actually list feeds fleet-
     wide the way its docstring claims
  4. data/feed-catalog.json — freshness, and the acid test: does it already
     know about rebalance-radar and earnings-tracker's REAL schema (if it
     had been checked first, would ops 3880/3882/3883's stumbles have been
     avoided?)
  5. engine-manifest.json generated_at — static snapshot or live-refreshed
  6. CloudWatch — did these lambdas actually invoke successfully recently,
     or is scheduler=None meaning genuinely dormant
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
SEMI_TICKERS = {"NVDA","AMD","AVGO","TSM","MU","QCOM","TXN","INTC","LRCX","KLAC",
                "AMAT","ARM","ASML","MRVL","ON","SMCI"}


def get(key):
    o = s3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(o["Body"].read()), o["LastModified"], o["ContentLength"]


def age_h(lm):
    return round((datetime.now(timezone.utc) - lm).total_seconds() / 3600, 1)


def check_invokes(fn_name, rep, needle=None):
    try:
        streams = logs.describe_log_streams(
            logGroupName=f"/aws/lambda/{fn_name}",
            orderBy="LastEventTime", descending=True, limit=3)["logStreams"]
        if not streams:
            rep.fail(f"  {fn_name}: NO log streams at all — likely never invoked")
            return
        last_event_ms = streams[0].get("lastEventTimestamp")
        if last_event_ms:
            last_dt = datetime.fromtimestamp(last_event_ms / 1000, tz=timezone.utc)
            rep.log(f"  {fn_name}: last log event {age_h(last_dt)}h ago")
        for st in streams[:1]:
            events = logs.get_log_events(
                logGroupName=f"/aws/lambda/{fn_name}",
                logStreamName=st["logStreamName"], limit=40)["events"]
            for e in events[-8:]:
                rep.log(f"    {e['message'].strip()[:160]}")
    except Exception as e:
        rep.fail(f"  {fn_name}: CloudWatch read failed: {str(e)[:150]}")


def main():
    with report("3885_news_registry_infra_audit") as rep:
        rep.heading("ops 3885 — is existing news/registry infra alive, or declared-but-dormant")
        failures = []

        rep.section("1. data/news-wire.json — live? real semi headlines?")
        try:
            nw, nw_lm, nw_sz = get("data/news-wire.json")
            rep.ok(f"  {nw_sz:,} bytes, {age_h(nw_lm)}h old")
            rep.log(f"  top-level keys: {sorted(nw.keys())}")
            items = nw.get("items") or nw.get("headlines") or nw.get("news") or []
            if not items:
                lk = [k for k, v in nw.items() if isinstance(v, list)]
                rep.log(f"  list-bearing keys: {lk}")
                items = nw.get(lk[0]) if lk else []
            rep.log(f"  n items: {len(items)}")
            if items:
                rep.log(f"  sample item keys: {sorted(items[0].keys())}")
            semi_items = [it for it in items if (it.get("tickers") or it.get("ticker") or []) and
                          any(t in SEMI_TICKERS for t in (it.get("tickers") or [it.get("ticker")]))]
            rep.kv(n_semi_items=len(semi_items))
            for it in semi_items[:10]:
                rep.log(f"    {json.dumps(it, default=str)[:350]}")
        except Exception as e:
            rep.fail(f"  news-wire.json unreadable: {str(e)[:200]}")
            failures.append("news-wire.json")

        rep.section("2. sentiment/data.json — is news-sentiment (schedule=None) actually producing anything")
        try:
            ns, ns_lm, ns_sz = get("sentiment/data.json")
            rep.ok(f"  {ns_sz:,} bytes, {age_h(ns_lm)}h old")
            rep.log(f"  top-level keys: {sorted(ns.keys()) if isinstance(ns, dict) else type(ns)}")
        except Exception as e:
            rep.fail(f"  sentiment/data.json unreadable: {str(e)[:200]}")
            failures.append("sentiment/data.json")

        rep.section("3. data/feed-registry.json — fleet freshness ledger, live?")
        try:
            fr, fr_lm, fr_sz = get("data/feed-registry.json")
            rep.ok(f"  {fr_sz:,} bytes, {age_h(fr_lm)}h old")
            rep.log(f"  top-level keys: {sorted(fr.keys())}")
            feeds = fr.get("feeds") or []
            rep.kv(n_feeds_listed=len(feeds) if isinstance(feeds, list) else len(feeds) if isinstance(feeds, dict) else 0)
        except Exception as e:
            rep.fail(f"  feed-registry.json unreadable: {str(e)[:200]}")
            failures.append("feed-registry.json")

        rep.section("4. data/feed-catalog.json — THE ACID TEST: would it have surfaced rebalance-radar "
                    "and earnings-tracker's real schema?")
        try:
            fc, fc_lm, fc_sz = get("data/feed-catalog.json")
            rep.ok(f"  {fc_sz:,} bytes, {age_h(fc_lm)}h old")
            rep.log(f"  top-level keys: {sorted(fc.keys())}")
            catalog = fc.get("feeds") or fc.get("catalog") or {}
            rep.kv(n_entries=len(catalog) if hasattr(catalog, "__len__") else 0)
            rr_entry = None
            et_entry = None
            if isinstance(catalog, dict):
                rr_entry = catalog.get("data/rebalance-radar.json")
                et_entry = catalog.get("data/earnings-tracker.json")
            elif isinstance(catalog, list):
                rr_entry = next((e for e in catalog if e.get("key") == "data/rebalance-radar.json"), None)
                et_entry = next((e for e in catalog if e.get("key") == "data/earnings-tracker.json"), None)
            rep.log(f"  rebalance-radar.json entry: {json.dumps(rr_entry, default=str)[:800]}")
            rep.log(f"  earnings-tracker.json entry: {json.dumps(et_entry, default=str)[:800]}")
        except Exception as e:
            rep.fail(f"  feed-catalog.json unreadable: {str(e)[:200]}")
            failures.append("feed-catalog.json")

        rep.section("5. CloudWatch — actual recent invokes for all 4 (engine-manifest.json checked "
                    "separately via bash since it's a repo file, not S3)")
        for fn in ("justhodl-news-wire", "justhodl-news-sentiment",
                   "justhodl-feed-registry", "justhodl-feed-catalog"):
            check_invokes(fn, rep)

        rep.section("6. verdict")
        rep.kv(failures=str(failures))
        if len(failures) >= 3:
            rep.fail(f"most core reads failed: {failures}")
            sys.exit(1)
        rep.ok("PROBE COMPLETE")


if __name__ == "__main__":
    main()
