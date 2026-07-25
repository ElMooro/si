"""
ops_3872 — redeploy the market_cap unit-normalization fix (ops 3871 caught it
live: finviz-universe.json's market_cap is in MILLIONS, universe.json's is
raw DOLLARS; the engine was picking whichever donor resolved without
converting, corrupting flow_pct_mcap_21d and every downstream cross-sectional
z / quadrant call for the ~63% of stocks sourced from finviz).

The negative gate here is the interesting one: it's not enough that the run
completes — flow_pct_mcap_21d must land in a PLAUSIBLE range across the WHOLE
universe now, not just on the 5 hand-picked mega-caps ops 3871 checked. A
single-digit-percent monthly flow-vs-mcap ratio is normal institutional
positioning; anything in the thousands or millions of percent is the old bug
resurfacing on some other donor path.
"""
import io
import json
import statistics
import sys
import time
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

FN = "justhodl-etf-constituents"
BUCKET = "justhodl-dashboard-live"
KEY = "etf-flows/constituent-pressure.json"
MARKER = "x1e6 = $4.9T real"   # unique to the ops-3871-fix comment, not present pre-fix

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=890, retries={"max_attempts": 0}))


def snapshot():
    o = s3.get_object(Bucket=BUCKET, Key=KEY)
    return json.loads(o["Body"].read()), o["LastModified"]


def main():
    with report("3872_marketcap_unit_fix_redeploy") as rep:
        rep.heading("ops 3872 — redeploy market_cap unit fix, gate on PLAUSIBLE magnitude fleet-wide")

        rep.section("1. BEFORE — capture the corrupted state for a before/after diff")
        before, blm = snapshot()
        bper = before.get("per_stock_exposure") or {}
        b_pcts = [abs(r["flow_pct_mcap_21d"]) for r in bper.values()
                  if r.get("flow_pct_mcap_21d") is not None]
        b_extreme = sum(1 for p in b_pcts if p > 100)
        rep.kv(before_s3=blm.isoformat(), before_n_stocks=len(bper),
               before_n_extreme_pct=b_extreme,
               before_max_pct=max(b_pcts) if b_pcts else None,
               before_median_pct=statistics.median(b_pcts) if b_pcts else None)
        rep.log(f"  BEFORE: {b_extreme}/{len(b_pcts)} stocks show |flow_pct_mcap_21d| > 100% "
                f"— this is the bug signature, should collapse toward 0 after the fix")

        rep.section("2. ZIP-SETTLE BY MARKER")
        settled = False
        for attempt in range(1, 31):
            try:
                loc = lam.get_function(FunctionName=FN)["Code"]["Location"]
                blob = urllib.request.urlopen(loc, timeout=60).read()
                with zipfile.ZipFile(io.BytesIO(blob)) as z:
                    src = z.read("lambda_function.py").decode("utf-8", "ignore")
                if MARKER in src:
                    rep.ok(f"  new artifact live on attempt {attempt} ({len(blob):,} zip bytes)")
                    settled = True
                    break
                rep.log(f"  attempt {attempt}: fix marker not yet in the deployed zip")
            except Exception as e:
                rep.log(f"  attempt {attempt}: {str(e)[:100]}")
            time.sleep(20)
        if not settled:
            rep.fail("  deploy never landed")
            sys.exit(1)

        cfg = lam.get_function_configuration(FunctionName=FN)
        for _ in range(30):
            if cfg.get("State") == "Active" and cfg.get("LastUpdateStatus") != "InProgress":
                break
            time.sleep(10)
            cfg = lam.get_function_configuration(FunctionName=FN)
        rep.ok(f"  State={cfg.get('State')} LastUpdateStatus={cfg.get('LastUpdateStatus')}")

        rep.section("3. invoke")
        lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
        after = None
        for attempt in range(1, 43):
            time.sleep(20)
            try:
                doc, lm = snapshot()
            except Exception:
                continue
            if lm > blm:
                after = doc
                rep.ok(f"  artifact rewritten on attempt {attempt} ({lm.isoformat()})")
                break
        if after is None:
            rep.fail("  constituent-pressure.json never rewrote")
            sys.exit(1)

        rep.section("4. THE NEGATIVE GATE — plausible magnitude fleet-wide, not just 5 names")
        per = after.get("per_stock_exposure") or {}
        pcts = [abs(r["flow_pct_mcap_21d"]) for r in per.values()
                if r.get("flow_pct_mcap_21d") is not None]
        n_extreme = sum(1 for p in pcts if p > 100)
        n_reasonable = sum(1 for p in pcts if p <= 50)
        med = statistics.median(pcts) if pcts else None
        mx = max(pcts) if pcts else None

        # known mega-caps: re-verify market_cap lands in the trillions, not millions
        mega = {"AAPL", "MSFT", "NVDA", "GOOGL", "AMZN"}
        mega_ok = []
        for tk in mega:
            r = per.get(tk)
            if r and r.get("market_cap"):
                mega_ok.append(1e12 <= r["market_cap"] <= 1e13)
                rep.log(f"  {tk}: market_cap=${r['market_cap']/1e12:.2f}T")

        checks = [
            ("stock universe intact", len(per) >= 2000),
            ("all 5 known mega-caps land in $1T-$10T range",
             len(mega_ok) == 5 and all(mega_ok)),
            ("median |flow_pct_mcap_21d| is single-digit-to-low-double-digit percent",
             med is not None and med < 20),
            ("extreme-percentage count collapsed vs BEFORE",
             n_extreme < b_extreme * 0.1 if b_extreme else n_extreme < 10),
            ("max |flow_pct_mcap_21d| is not in the thousands (bug signature)",
             mx is not None and mx < 500),
            ("majority of stocks now show a reasonable (<=50%) monthly flow-vs-mcap ratio",
             n_reasonable >= len(pcts) * 0.85),
        ]
        for label, ok in checks:
            (rep.ok if ok else rep.fail)(f"  {label}")

        rep.kv(after_n_stocks=len(per), after_n_extreme_pct=n_extreme,
               after_n_reasonable=n_reasonable, after_median_pct=med, after_max_pct=mx,
               before_extreme=b_extreme, reduction_ratio=(
                   round(n_extreme / b_extreme, 4) if b_extreme else None))

        rep.section("5. quadrant distribution — should this differ meaningfully from ops 3870's")
        quad = after.get("quadrant_counts") or {}
        rep.log(f"  ops 3870 (corrupted units): STEALTH_ACCUMULATION=30 DISTRIBUTION_RALLY=30 "
                f"TREND_CONFIRMED=14 CAPITULATION=59 NEUTRAL=2114")
        rep.log(f"  ops 3872 (fixed units):     {quad}")
        rep.kv(quadrant_counts=str(quad))

        failed = [l for l, ok in checks if not ok]
        if failed:
            rep.fail(f"FAILED {len(failed)}: {failed}")
            sys.exit(1)
        rep.ok(f"PASS_ALL — units fixed fleet-wide: median {med}%, max {mx}%, "
               f"extreme count {b_extreme}->{n_extreme}, quadrant {quad}")


if __name__ == "__main__":
    main()
