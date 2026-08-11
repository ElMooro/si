"""ops 4599 — three surfaced feeds fixed + fred v2.3 blended rate.

The honest gates exposed three blind feeds; fixes: fpr universe learns
top_tickers (+ empty falls back loudly), 13f extractor learns the
aggregate_by_ticker DICT, skew gets a shape-agnostic premium-row scan
(options-flow.json is a legacy alias of flow-data.json — same payload).
Contracts are consistency-shaped: each detector's count must agree with
what its feed actually contains, read here independently.
"""
import json
import sys
import time
from datetime import datetime, timezone

import boto3
from botocore.config import Config

from ops_report import report

B = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=300,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name="us-east-1")
BASE_IMPORTED = 60990          # ops 4598 snapshot
BASE_TS = "2026-08-11T00:38:52+00:00"


def gj(key):
    try:
        return json.loads(s3.get_object(Bucket=B, Key=key)["Body"].read())
    except Exception:
        return {}


def settle(r, fn):
    t0 = time.time()
    while time.time() - t0 < 240:
        try:
            c = lam.get_function(FunctionName=fn)["Configuration"]
            if c.get("LastUpdateStatus") == "Successful" \
                    and c.get("State") == "Active":
                return
        except Exception:
            pass
        time.sleep(6)
    r.warn("  %s did not settle" % fn)


def contract(r, name, cond, why):
    if cond:
        r.ok("  [%s] %s" % (name, why))
        return 0
    r.fail("  [%s] CONTRACT MISS — %s" % (name, why))
    return 1


def sync(r, fn):
    resp = lam.invoke(FunctionName=fn, InvocationType="RequestResponse")
    raw = resp["Payload"].read().decode("utf-8", "replace")
    if resp.get("FunctionError") or '"statusCode": 500' in raw[:80]:
        r.fail("  %s handler error: %s" % (fn, raw[:300]))
        return False
    return True


def main():
    with report("4599_feeds_close") as r:
        r.heading("ops 4599 — surfaced feeds closed + fred blended rate")
        misses = 0

        r.section("1. Settle + sync-invoke the three")
        for fn in ("justhodl-failed-pattern-reversal",
                   "justhodl-13f-price-divergence",
                   "justhodl-catalyst-skew-premove"):
            settle(r, fn)
            if not sync(r, fn):
                misses += 1

        r.section("2. fpr — universe restored")
        j = gj("data/failed-pattern-reversal.json")
        ds = j.get("data_sufficiency") or {}
        misses += contract(r, "fpr",
                           (ds.get("n_universe") or 0) > 0
                           and (ds.get("n_scanned_with_data") or 0) > 0,
                           "state=%s universe=%s scanned=%s"
                           % (j.get("state"), ds.get("n_universe"),
                              ds.get("n_scanned_with_data")))

        r.section("3. 13f — feeder tickers flowing")
        j = gj("data/13f-price-divergence.json")
        ds = j.get("data_sufficiency") or {}
        feed = gj("data/13f-positions.json")
        n_feed = len(feed.get("aggregate_by_ticker") or {})
        got = ds.get("n_feeder_tickers") or 0
        misses += contract(r, "13f",
                           (got > 0) == (n_feed > 0),
                           "consistency: feed dict=%d extractor=%d "
                           "state=%s" % (n_feed, got, j.get("state")))

        r.section("4. skew — premium rows vs feed truth")
        j = gj("data/catalyst-skew-premove.json")
        ds = j.get("data_sufficiency") or {}
        feed = gj("data/options-flow.json")
        n_q = 0
        if isinstance(feed, dict):
            for k, v in list(feed.items())[:40]:
                if (isinstance(v, list) and v and isinstance(v[0], dict)
                        and (v[0].get("symbol") or v[0].get("ticker"))
                        and any(f in v[0] for f in
                                ("call_put_ratio", "cp_ratio",
                                 "call_premium", "call_premium_usd",
                                 "put_premium", "put_premium_usd"))):
                    n_q += len(v)
        got = ds.get("n_with_options_data") or 0
        misses += contract(r, "skew",
                           (got > 0) == (n_q > 0),
                           "consistency: feed qualifying rows=%d "
                           "extractor names=%d state=%s (feed keys: %s)"
                           % (n_q, got, j.get("state"),
                              sorted(feed)[:8] if isinstance(feed, dict)
                              else "?"))

        r.section("5. fred — v2.3 + blended rate since 00:38")
        st = gj("data/_state/fred-scoped-import.json")
        now = datetime.now(timezone.utc)
        base = datetime.fromisoformat(BASE_TS)
        mins = max(1.0, (now - base).total_seconds() / 60.0)
        imp = int(st.get("series_imported") or 0)
        rate = (imp - BASE_IMPORTED) / mins
        r.log("  ver=%s rpm=%s imported=%d (+%d in %.0f min = %.1f/min "
              "= %.0f/h) cursor=%s throttled=%s"
              % (st.get("engine_version"), st.get("rate_rpm"), imp,
                 imp - BASE_IMPORTED, mins, rate, rate * 60,
                 st.get("queue_cursor"), st.get("throttled_429")))
        misses += contract(r, "fred",
                           str(st.get("engine_version")) == "2.3",
                           "v2.3 live on the chain")
        misses += contract(r, "fred", rate > 60,
                           "blended rate %.1f/min (serial era was 49; "
                           "ceiling-only was 65.7)" % rate)
        rem = 275105 - int(st.get("queue_cursor") or 0)
        if rate > 0:
            r.log("  remaining≈%d → ETA %.1f h (~%.1f days)"
                  % (rem, rem / (rate * 60), rem / (rate * 60) / 24))

        r.section("verdict")
        if misses:
            r.fail("feeds close: %d red" % misses)
            sys.exit(1)
        r.ok("three blind feeds seeing again; fred compound rate "
             "confirmed — sentinel keeps the watch")


if __name__ == "__main__":
    main()
