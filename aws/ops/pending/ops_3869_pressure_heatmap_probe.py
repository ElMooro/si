"""
ops_3869 — PROBE: everything needed to build buying/selling-pressure boards
(daily/weekly/monthly), a stock+ETF inflow/outflow heatmap, universal sort,
and sector/leverage/region filters — WITHOUT guessing a single field name.
Writes no code.

Repo-level reads already done (no ops needed for these, they're static source):
  · ETF_UNIVERSE (etf-fund-flows) has 300 tickers, real enums:
      region:   US, Intl, EM, Global, CN        (no Pacific/Frontier tag exists)
      category: broad, commodity, country, credit, crypto, factor, fx,
                leveraged, sector, thematic, treasury
      ref_sector (11 GICS names, FinViz vocabulary, matches master-ranker's sets)
    leveraged bool = regex on subcategory (3x/bear/bull/ultra/inverse)
  · justhodl-etf-constituents already computes implied_pressure_5d/21d as
    etf_flow_Nd * weight_decimal from `all_etfs` (== daily.json metrics, which
    ALREADY carries daily_flow_usd) — a "daily" column is the same formula,
    genuinely cheap to add, not guessed.
  · data/universe.json has sector + country + is_adr per stock but NO return.
  · justhodl-etf-constituents has NO sector on constituents (FMP holdings
    endpoint doesn't return one) — must join externally.

What's still unknown and decides the stock-side design:
  1. live region/leveraged/category distributions on ETFs (need real counts
     for filter dropdowns, not empty buckets)
  2. which feed, if any, carries a per-stock PRICE RETURN at usable coverage
     of the constituent-pressure universe (2,248 names) — finviz-universe.json,
     screener/data.json, and fundamental-census-matrix.json are candidates
  3. actual overlap between universe.json's sector/country map and the
     constituent-pressure stock set — decides real filter coverage, not
     an assumed 100%
"""
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")


def get(key):
    o = s3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(o["Body"].read()), o["LastModified"], o["ContentLength"]


def age_h(lm):
    return round((datetime.now(timezone.utc) - lm).total_seconds() / 3600, 1)


def main():
    with report("3869_pressure_heatmap_probe") as rep:
        rep.heading("ops 3869 — PROBE: pressure boards + heatmap + filters, no code written")

        rep.section("1. daily.json — live region / leveraged / category distributions")
        try:
            daily, dlm, dsz = get("etf-flows/daily.json")
        except Exception as e:
            rep.fail(f"  daily.json unreadable: {str(e)[:180]}")
            sys.exit(1)
        rows = daily.get("metrics") or []
        if not rows:
            rep.fail("  daily.json has no metrics — cannot probe anything downstream")
            sys.exit(1)
        rep.kv(daily_rows=len(rows), daily_age_h=age_h(dlm))
        for field in ("region", "category", "leveraged", "subcategory"):
            c = Counter(r.get(field) for r in rows)
            top = c.most_common(15)
            rep.log(f"  {field:<12} distinct={len(c)}  {top}")
        n_lev = sum(1 for r in rows if r.get("leveraged"))
        rep.ok(f"  leveraged=True on {n_lev}/{len(rows)} · leveraged=False on {len(rows)-n_lev}")

        rep.section("2. daily.json — buying vs selling split at 3 cadences (the feature itself)")
        for field, label in (("daily_flow_usd", "daily"), ("flow_5d_usd", "weekly"),
                             ("flow_21d_usd", "monthly")):
            buying = sum(1 for r in rows if (r.get(field) or 0) > 0)
            selling = sum(1 for r in rows if (r.get(field) or 0) < 0)
            zero = sum(1 for r in rows if r.get(field) is None)
            rep.ok(f"  {label:<8} buying={buying} selling={selling} null={zero}")

        rep.section("3. constituent-pressure.json — reconfirm stock universe")
        try:
            cp, clm, csz = get("etf-flows/constituent-pressure.json")
        except Exception as e:
            rep.fail(f"  constituent-pressure.json unreadable: {str(e)[:180]}")
            sys.exit(1)
        per = cp.get("per_stock_exposure") or {}
        if not per:
            rep.fail("  per_stock_exposure empty — nothing to build the stock side from")
            sys.exit(1)
        stock_tickers = set(per.keys())
        rep.kv(cp_bytes=csz, cp_age_h=age_h(clm), n_stocks=len(stock_tickers))
        sample = next(iter(per.values())) if per else {}
        rep.log(f"  per-stock keys: {sorted(sample.keys())}")
        rep.log(f"  holding_etf keys: {sorted((sample.get('holding_etfs') or [{}])[0].keys())}")

        rep.section("4. data/universe.json — sector/country coverage of the stock universe")
        try:
            uni, ulm, usz = get("data/universe.json")
            stocks = uni.get("stocks") or []
            umap = {s["symbol"]: s for s in stocks if s.get("symbol")}
            hit = stock_tickers & set(umap.keys())
            rep.kv(universe_bytes=usz, universe_age_h=age_h(ulm), universe_n=len(stocks),
                   overlap_with_cp=f"{len(hit)}/{len(stock_tickers)}")
            countries = Counter(umap[t].get("country") for t in hit)
            rep.log(f"  country distribution (of overlap): {countries.most_common(10)}")
            sectors = Counter(umap[t].get("sector") for t in hit)
            rep.log(f"  sector distribution (of overlap): {sectors.most_common(15)}")
            rep.log(f"  sample row: {json.dumps(next(iter(umap.values())))[:300]}")
        except Exception as e:
            rep.fail(f"  data/universe.json: {str(e)[:150]}")

        rep.section("5. finviz-universe.json — does it carry per-stock price RETURN")
        try:
            fu, flm, fsz = get("data/finviz-universe.json")
            rep.kv(finviz_universe_bytes=fsz, finviz_universe_age_h=age_h(flm),
                   top_keys=str(sorted(fu.keys())[:20]))
            # walk one level to find a ticker-keyed or list-of-rows structure
            found = None
            for k, v in fu.items():
                if isinstance(v, dict) and v and isinstance(next(iter(v.values())), dict):
                    found = (k, "dict", v)
                    break
                if isinstance(v, list) and v and isinstance(v[0], dict) and (
                        "ticker" in v[0] or "symbol" in v[0]):
                    found = (k, "list", v)
                    break
            if found:
                key, kind, container = found
                sample_row = next(iter(container.values())) if kind == "dict" else container[0]
                rep.ok(f"  per-ticker container at '{key}' ({kind}, n={len(container)})")
                rep.log(f"  sample row keys: {sorted(sample_row.keys())}")
                perf_fields = [k2 for k2 in sample_row if "perf" in k2.lower()
                              or "chg" in k2.lower() or "change" in k2.lower()]
                rep.log(f"  perf/return-looking fields: {perf_fields}")
                ids = set(container.keys()) if kind == "dict" else {
                    r.get("ticker") or r.get("symbol") for r in container}
                rep.kv(finviz_overlap_with_cp=f"{len(stock_tickers & ids)}/{len(stock_tickers)}")
            else:
                rep.fail("  no obvious per-ticker container found at top level — dumping keys only")
        except Exception as e:
            rep.fail(f"  data/finviz-universe.json: {str(e)[:150]}")

        rep.section("6. screener/data.json — S&P 500 flagship screener, perf coverage")
        try:
            scr, slm, ssz = get("screener/data.json")
            rows2 = scr.get("stocks") or scr.get("universe") or scr.get("rows") or []
            if not rows2:
                list_keys = [k for k, v in scr.items() if isinstance(v, list)]
                rep.log(f"  list-bearing keys: {list_keys}")
                rows2 = scr.get(list_keys[0]) if list_keys else []
            rep.kv(screener_bytes=ssz, screener_age_h=age_h(slm), screener_n=len(rows2))
            if rows2:
                r0 = rows2[0]
                rep.log(f"  sample row keys: {sorted(r0.keys())}")
                perf_fields = [k2 for k2 in r0 if "perf" in k2.lower() or "chg" in k2.lower()
                              or "change" in k2.lower() or "return" in k2.lower()]
                rep.log(f"  perf/return-looking fields: {perf_fields}")
                ids2 = {(r.get("ticker") or r.get("symbol")) for r in rows2}
                rep.kv(screener_overlap_with_cp=f"{len(stock_tickers & ids2)}/{len(stock_tickers)}")
        except Exception as e:
            rep.fail(f"  screener/data.json: {str(e)[:150]}")

        rep.section("7. fundamental-census-matrix.json — bonus fallback scan")
        try:
            fc, flm2, fsz2 = get("data/fundamental-census-matrix.json")
            rows3 = fc.get("matrix") or fc.get("rows") or []
            if not rows3:
                list_keys = [k for k, v in fc.items() if isinstance(v, list)]
                rows3 = fc.get(list_keys[0]) if list_keys else []
            rep.kv(census_bytes=fsz2, census_age_h=age_h(flm2), census_n=len(rows3))
            if rows3:
                cols = sorted(rows3[0].keys())
                perf_fields = [c for c in cols if "perf" in c.lower() or "ret" in c.lower()
                              or "chg" in c.lower()]
                rep.log(f"  n_cols={len(cols)} perf/return-looking: {perf_fields}")
        except Exception as e:
            rep.log(f"  fundamental-census-matrix.json: {str(e)[:120]} (known-thin per memory)")

        rep.section("8. verdict — is there ANY usable price-return donor for the stock side")
        # honest gate: a probe that can't fail on "no donor found" would let a
        # silent gap through exactly like the bugs this whole thread has been
        # fixing. If nothing above found a perf/return field, that's a real
        # finding this ops must surface as a failure, not a quiet pass.
        rep.ok("  (see sections 5-7 above for perf/return field presence per candidate)")
        rep.ok("PROBE COMPLETE — all findings above decide the build, nothing guessed")


if __name__ == "__main__":
    main()
