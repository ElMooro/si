"""
ops_3871 — PROBE: what UNITS is market_cap actually in, and do the live
top_aggregate_exposure rows look sane. WRITES NO CODE.

ops 3870's own spot-check printed "MU mcap=$0.0B" — obviously wrong for a
name that size, but MU's sector/price/quadrant all resolved correctly, so
the ENGINE's join worked; the bug is in how I DISPLAYED the number in my own
report (assumed raw dollars, divided by 1e9). If I carry that same wrong
assumption into the page, market cap renders ~1000x too small for everyone.
Settling this with 5 known mega-caps before writing any page code that
displays it.
"""
import json
import sys
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")

KNOWN = {  # real approx market caps, dollars, July 2026 ballpark — order-of-magnitude check only
    "AAPL": 3.4e12, "MSFT": 3.2e12, "NVDA": 5.0e12, "GOOGL": 2.2e12, "AMZN": 2.3e12,
}


def get(key):
    return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())


def main():
    with report("3871_marketcap_units_probe") as rep:
        rep.heading("ops 3871 — PROBE: market_cap units + sanity-check live rows")

        cp = get("etf-flows/constituent-pressure.json")
        per = cp.get("per_stock_exposure") or {}
        if not per:
            rep.fail("  per_stock_exposure empty — engine output regressed since ops 3870")
            sys.exit(1)

        rep.section("1. market_cap units — 5 known mega-caps, order of magnitude")
        ratios = []
        for tk, real_usd in KNOWN.items():
            r = per.get(tk)
            if not r or r.get("market_cap") is None:
                rep.fail(f"  {tk}: no market_cap in live data")
                continue
            raw = r["market_cap"]
            ratio = real_usd / raw if raw else None
            ratios.append(ratio)
            rep.log(f"  {tk}: raw={raw!r}  real_usd~{real_usd:.2e}  real/raw ratio={ratio}")
        if not ratios:
            rep.fail("  no known mega-cap resolved — cannot determine units")
            sys.exit(1)
        avg_ratio = sum(ratios) / len(ratios)
        if 0.5e6 <= avg_ratio <= 2e6:
            unit = "MILLIONS (raw value x 1e6 = dollars)"
        elif 0.5 <= avg_ratio <= 2:
            unit = "RAW DOLLARS (no conversion needed)"
        elif 0.5e3 <= avg_ratio <= 2e3:
            unit = "THOUSANDS"
        else:
            unit = f"UNCLEAR — ratio {avg_ratio:.3e}, inspect manually"
        rep.kv(avg_real_over_raw_ratio=f"{avg_ratio:.3e}", determined_unit=unit)
        (rep.ok if "UNCLEAR" not in unit else rep.fail)(f"  DETERMINED UNIT: {unit}")

        rep.section("2. does the SAME unit convention hold in universe.json's market_cap")
        try:
            uni = get("data/universe.json")
            umap = {s["symbol"]: s for s in (uni.get("stocks") or []) if s.get("symbol")}
            for tk in list(KNOWN)[:3]:
                u = umap.get(tk)
                if u and u.get("market_cap"):
                    rep.log(f"  {tk} universe.json market_cap raw={u['market_cap']!r} "
                            f"(finviz raw was {per.get(tk, {}).get('market_cap')!r})")
        except Exception as e:
            rep.log(f"  universe.json cross-check skipped: {str(e)[:100]}")

        rep.section("3. sanity-check real top_aggregate_exposure rows (what the page will render)")
        top = cp.get("top_aggregate_exposure") or []
        if len(top) < 10:
            rep.fail(f"  only {len(top)} rows in top_aggregate_exposure — expected ~100")
            sys.exit(1)
        for r in top[:8]:
            mc = r.get("market_cap")
            mc_disp = f"${mc/1e6:,.0f}M" if (mc and "MILLIONS" in unit) else (
                f"${mc/1e9:,.1f}B" if mc else "—")
            rep.log(f"  {r.get('stock'):<6} {r.get('name','')[:28]:<28} "
                    f"sector={str(r.get('sector')):<22} mcap={mc_disp:<12} "
                    f"daily=${(r.get('total_aggregate_flow_daily_usd') or 0)/1e6:+8.1f}M "
                    f"5d=${(r.get('total_aggregate_flow_5d_usd') or 0)/1e6:+8.1f}M "
                    f"21d=${(r.get('total_aggregate_flow_21d_usd') or 0)/1e6:+8.1f}M "
                    f"quadrant={r.get('quadrant')}")

        rep.section("4. plausibility — top movers should be large, liquid, real names")
        top_stocks = {r.get("stock") for r in top[:20]}
        implausible = [t for t in top_stocks if t and (len(t) > 5 or not t.isalpha())]
        rep.kv(top_20_tickers=str(sorted(top_stocks)))
        (rep.fail if implausible else rep.ok)(
            f"  {'SUSPICIOUS tickers in top 20: ' + str(implausible) if implausible else 'top 20 tickers all look like real equity symbols'}")

        rep.ok("PROBE COMPLETE")


if __name__ == "__main__":
    main()
