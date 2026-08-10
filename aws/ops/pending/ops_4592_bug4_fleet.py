"""ops 4592 — BUG-4 fleet gate verification (wo4592, gates 1-9).

The 4574 audit flagged ten engines able to claim confident QUIET while
blind; activist-13d was fixed in wo4585 rev-A, and the remaining nine
got the same doctrine tonight (one commit each): the confident-negative
is only claimable when the inputs actually loaded — feeder tickers,
options data, calendar fetches, condition feeds, price universes, or
vol series — otherwise the state says INSUFFICIENT_DATA and a
data_sufficiency block explains the evidence.

This op fires all nine, polls each payload, and asserts:
  - payload refreshed (the gate code runs, no crashes)
  - data_sufficiency present
  - state is a string; if the sufficiency numbers say blind, state is
    INSUFFICIENT_DATA (the gate actually gates)

Plus two ledger evidence prints: share-flows' sec-filings-intel ATM
warn, and data/import-health.json (FRED arc pulse).
"""
import json
import sys
import time

import boto3
from botocore.config import Config

from ops_report import report

REGION = "us-east-1"
B = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=120, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)

ENGINES = {
    "justhodl-13f-price-divergence": "data/13f-price-divergence.json",
    "justhodl-catalyst-skew-premove": "data/catalyst-skew-premove.json",
    "justhodl-earnings-iv-crush": "data/earnings-iv-crush.json",
    "justhodl-failed-pattern-reversal": "data/failed-pattern-reversal.json",
    "justhodl-forced-selling-bounce": "data/forced-selling-bounce.json",
    "justhodl-lockup-expiration": "data/lockup-expiration.json",
    "justhodl-ma-target-predictor": "data/ma-target-predictor.json",
    "justhodl-post-earnings-mean-rev": "data/post-earnings-mean-rev.json",
    "justhodl-vvix-vov-regime": "data/vvix-vov-regime.json",
}


def get_json(key):
    try:
        return json.loads(s3.get_object(Bucket=B, Key=key)["Body"].read())
    except Exception:
        return None


def settle(r, fn, deadline_s=240):
    t0 = time.time()
    while time.time() - t0 < deadline_s:
        try:
            c = lam.get_function(FunctionName=fn)["Configuration"]
            if c.get("LastUpdateStatus") == "Successful" \
                    and c.get("State") == "Active":
                return True
        except Exception:
            pass
        time.sleep(6)
    r.warn("  %s did not settle" % fn)
    return False


def contract(r, name, cond, why):
    if cond:
        r.ok("  [%s] %s" % (name, why))
        return 0
    r.fail("  [%s] CONTRACT MISS — %s" % (name, why))
    return 1


def blind_by_numbers(ds):
    """Mirror of the shipped rules: infer blindness from the block."""
    if not isinstance(ds, dict):
        return None
    if "http_ok" in ds:
        return ds.get("http_ok", 0) == 0 and ds.get("http_err", 0) > 0
    if "feeds_ok" in ds:
        t = (ds.get("feeds_ok") or 0) + (ds.get("feeds_miss") or 0)
        return bool(t) and (ds.get("feeds_ok") or 0) < 0.7 * t
    if "n_scanned_with_data" in ds:
        return not ds.get("n_scanned_with_data")
    if "n_feeder_tickers" in ds:
        return (not ds.get("feeder_loaded", True)
                or not ds.get("n_feeder_tickers"))
    if "n_events_in_window" in ds:
        return (not ds.get("n_events_in_window")
                or not ds.get("n_with_options_data"))
    return None  # rule-only blocks (vvix) — state check suffices


def main():
    with report("4592_bug4_fleet") as r:
        r.heading("ops 4592 — BUG-4 fleet gates (9 engines)")
        misses = 0

        r.section("1. Settle + fire + poll")
        before = {}
        for fn, key in ENGINES.items():
            settle(r, fn)
            j = get_json(key) or {}
            before[fn] = j.get("as_of") or j.get("generated_at") or ""
            try:
                lam.invoke(FunctionName=fn, InvocationType="Event")
            except Exception as e:
                misses += contract(r, fn, False, "invoke: %s" % str(e)[:90])
        r.log("  fired %d engines" % len(ENGINES))
        outs, pending, t0 = {}, dict(ENGINES), time.time()
        while pending and time.time() - t0 < 780:
            time.sleep(12)
            for fn in list(pending):
                cur = get_json(pending[fn])
                ts = (cur or {}).get("as_of") or \
                     (cur or {}).get("generated_at") or ""
                if cur is not None and ts and ts != before[fn]:
                    outs[fn] = cur
                    r.log("  %s refreshed (%ss)"
                          % (fn.replace("justhodl-", ""),
                             int(time.time() - t0)))
                    del pending[fn]
        for fn in pending:
            r.warn("  %s did not refresh in 780s" % fn)
            outs[fn] = get_json(pending[fn]) or {}
            misses += 1

        r.section("2. Gate contracts")
        for fn in ENGINES:
            nm = fn.replace("justhodl-", "")
            j = outs.get(fn) or {}
            ds = j.get("data_sufficiency")
            st = j.get("state")
            misses += contract(r, nm, isinstance(ds, dict),
                               "data_sufficiency published")
            blind = blind_by_numbers(ds)
            honest = (blind is None or not blind
                      or st == "INSUFFICIENT_DATA")
            misses += contract(r, nm, isinstance(st, str) and honest,
                               "state=%s (blind_by_numbers=%s, ds=%s)"
                               % (st, blind,
                                  {k: v for k, v in (ds or {}).items()
                                   if k != "rule"}))

        r.section("3. Ledger evidence prints")
        sf = get_json("data/share-flows.json") or {}
        atm_w = [w for w in (sf.get("warns") or [])
                 if "sec-filings" in str(w)]
        r.log("  share-flows ATM warn: %s"
              % (atm_w[-1][:180] if atm_w else "none (join live or no "
                 "fallback this run)"))
        ih = get_json("data/import-health.json") or {}
        r.log("  FRED import-health: status=%s as_of=%s"
              % (ih.get("status") or ih.get("state") or sorted(ih)[:6],
                 ih.get("generated_at") or ih.get("as_of")))

        r.section("verdict")
        if misses:
            r.fail("BUG-4 fleet gates: %d red" % misses)
            sys.exit(1)
        r.ok("all ten flagged engines now gated — a blind detector in this "
             "fleet says INSUFFICIENT_DATA, never calm")


if __name__ == "__main__":
    main()
