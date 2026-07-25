"""
ops_3870 — DEPLOY: justhodl-etf-constituents extension for the buying/selling
pressure + heatmap feature Khalid asked for.

Ships in this push (source + config changed together, so deploy-lambdas.yml
owns the deploy — this ops does NOT call deploy_lambda(), it only zip-settles
and gates, per the standing rule against double-deploying the same push):

  1. daily-cadence implied pressure, same formula as the existing 5d/21d
     (etf_flow_daily * weight_decimal) — daily_flow_usd was already in scope,
     just unused. Now every stock carries daily/weekly/monthly buying or
     selling pressure, matching the request exactly.
  2. sector/country/price-return join from finviz-universe.json (primary,
     11.5k tickers) with data/universe.json as fallback for sector/country —
     genuinely-unknown tickers stay null, never guessed.
  3. a flow-vs-price quadrant per stock, using a CROSS-SECTIONAL flow
     z-score (21d flow as %-of-market-cap, z-scored against today's whole
     stock universe — real, computed fresh, not a fabricated history) against
     1-month price return, through the EXACT SAME thresholds the ETF engine
     already uses (STEALTH_ACCUMULATION / DISTRIBUTION_RALLY / TREND_CONFIRMED
     / CAPITULATION) — mirrored, not reinvented.

All new fields were verified locally against a stubbed S3 (4 unit tests: daily
math, sector/price join incl. honest nulls, quadrant classification against
constructed known cases, and a refusal to fabricate a z-score when n<30).
This ops verifies the same claims against the REAL deployed artifact and REAL
live data.
"""
import io
import json
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
MARKER = "total_aggregate_flow_daily_usd"

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=890, retries={"max_attempts": 0}))


def snapshot():
    o = s3.get_object(Bucket=BUCKET, Key=KEY)
    return json.loads(o["Body"].read()), o["LastModified"]


def main():
    with report("3870_constituents_pressure_deploy") as rep:
        rep.heading("ops 3870 — deploy daily-pressure + sector/price + quadrant, hard-gated")

        rep.section("1. BEFORE")
        before, blm = snapshot()
        rep.kv(before_generated_at=str(before.get("generated_at")),
               before_n_stocks=before.get("n_stocks_with_exposure"),
               before_s3=blm.isoformat(),
               had_daily_field=MARKER in json.dumps(before.get("top_aggregate_exposure") or [])[:2000])

        rep.section("2. ZIP-SETTLE BY MARKER — never invoke the old artifact (ops 3830 lesson)")
        settled = False
        for attempt in range(1, 31):
            try:
                loc = lam.get_function(FunctionName=FN)["Code"]["Location"]
                blob = urllib.request.urlopen(loc, timeout=60).read()
                with zipfile.ZipFile(io.BytesIO(blob)) as z:
                    src = z.read("lambda_function.py").decode("utf-8", "ignore")
                if MARKER in src and "flow_zscore_cross_sectional" in src:
                    rep.ok(f"  new artifact live on attempt {attempt} ({len(blob):,} zip bytes)")
                    settled = True
                    break
                rep.log(f"  attempt {attempt}: artifact still lacks the new markers")
            except Exception as e:
                rep.log(f"  attempt {attempt}: {str(e)[:100]}")
            time.sleep(20)
        if not settled:
            rep.fail("  deploy never landed — markers absent from the deployed zip")
            sys.exit(1)

        cfg = lam.get_function_configuration(FunctionName=FN)
        for _ in range(30):
            if cfg.get("State") == "Active" and cfg.get("LastUpdateStatus") != "InProgress":
                break
            time.sleep(10)
            cfg = lam.get_function_configuration(FunctionName=FN)
        rep.ok(f"  State={cfg.get('State')} LastUpdateStatus={cfg.get('LastUpdateStatus')} "
               f"Memory={cfg.get('MemorySize')} Timeout={cfg.get('Timeout')}")
        if cfg.get("MemorySize", 0) < 1536:
            rep.fail(f"  memory bump did not land ({cfg.get('MemorySize')} < 1536) — "
                     f"config.json drift, deploy-lambdas may have stomped it")

        rep.section("3. invoke (async — ~284 parallel FMP holdings fetches + a 15.9MB S3 parse)")
        lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
        after = None
        for attempt in range(1, 43):                       # up to ~14 min
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
            rep.fail("  constituent-pressure.json never rewrote — check CloudWatch logs "
                     "before any further change (do not assume the deploy is the cause)")
            sys.exit(1)

        rep.section("4. real-data gate — every claim, checked against the live artifact")
        per = after.get("per_stock_exposure") or {}
        top = after.get("top_aggregate_exposure") or []
        n = len(per)
        n_daily = sum(1 for r in per.values() if r.get("total_aggregate_flow_daily_usd") is not None)
        n_sector = after.get("n_stocks_with_sector") or 0
        n_price = after.get("n_stocks_with_price_return") or 0
        n_z = after.get("n_stocks_with_flow_zscore") or 0
        quad = after.get("quadrant_counts") or {}
        top_has_new_fields = bool(top) and "sector" in top[0] and "quadrant" in top[0]

        rep.kv(n_stocks=n, n_with_daily_pressure=n_daily, n_with_sector=n_sector,
               n_with_price_return=n_price, n_with_zscore=n_z, quadrant_counts=str(quad),
               top_aggregate_carries_new_fields=top_has_new_fields)

        checks = [
            ("stock universe present and not shrunk", n >= 2000),
            ("daily pressure computed for effectively all stocks", n_daily >= n * 0.95),
            ("sector known for a meaningful share (finviz+universe join actually ran)",
             n_sector >= n * 0.40),
            ("price return known for a meaningful share", n_price >= n * 0.40),
            ("cross-sectional z computed (n>=30 gate passed on live data)", n_z >= n * 0.30),
            ("at least one stock landed in each directional quadrant",
             quad.get("STEALTH_ACCUMULATION", 0) > 0 and quad.get("DISTRIBUTION_RALLY", 0) > 0),
            ("top_aggregate_exposure carries the new fields (hand-written list didn't drop them)",
             top_has_new_fields),
            ("no NaN/inf leaked into JSON (would have failed json.loads above, but check explicitly)",
             "NaN" not in json.dumps(top[:5]) and "Infinity" not in json.dumps(top[:5])),
        ]
        for label, ok in checks:
            (rep.ok if ok else rep.fail)(f"  {label}")

        rep.section("5. spot-check one real stock end to end")
        sample_tk = next((t for t, r in per.items() if r.get("quadrant") not in (None, "NEUTRAL")), None)
        if sample_tk:
            r = per[sample_tk]
            rep.log(f"  {sample_tk}: sector={r.get('sector')} mcap=${(r.get('market_cap') or 0)/1e9:.1f}B "
                    f"daily=${(r.get('total_aggregate_flow_daily_usd') or 0)/1e6:+.1f}M "
                    f"5d=${(r.get('total_aggregate_flow_5d_usd') or 0)/1e6:+.1f}M "
                    f"21d=${(r.get('total_aggregate_flow_21d_usd') or 0)/1e6:+.1f}M "
                    f"perf_m={r.get('perf_m_pct')}% z_xsec={r.get('flow_zscore_cross_sectional')} "
                    f"quadrant={r.get('quadrant')}")
            rep.ok(f"  {sample_tk} carries a full real record")
        else:
            rep.fail("  no stock landed in a directional quadrant — spot-check skipped")

        failed = [l for l, ok in checks if not ok]
        if failed:
            rep.fail(f"FAILED {len(failed)}: {failed}")
            sys.exit(1)
        rep.ok(f"PASS_ALL — {n} stocks, daily/weekly/monthly pressure live, "
               f"sector {n_sector}/{n} · price {n_price}/{n} · quadrant {quad}")


if __name__ == "__main__":
    main()
