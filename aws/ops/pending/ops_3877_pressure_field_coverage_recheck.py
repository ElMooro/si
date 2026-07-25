"""
ops_3877 — PROBE: field-coverage audit of every field the etf-constituents
engine now publishes (ops 3870-3872) against the SERVED flows.html. WRITES
NO CODE. Doctrine (AUTONOMY.md): an engine field with no render path is an
open bug, not a footnote — this arc built 4 new per-stock fields, and a
quick self-check while drafting the summary already found market_cap,
perf_w_pct, perf_ytd_pct, and flow_pct_mcap_21d were computed by the engine
but never mapped into buildUnifiedRows() — so never rendered anywhere.
This ops confirms that finding against the real live+served pair (not just
the source diff) and checks for any OTHER gap the same way.
"""
import json
import re
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

PAGE = "https://justhodl.ai/flows.html"
CDN = "https://justhodl-data-proxy.raafouis.workers.dev"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")

# fields that exist only as internal computation inputs, or that are
# genuinely meta-statistics already implied elsewhere on the page — waived
# with a stated reason, per doctrine, not silently ignored.
WAIVED = {
    "cumulative_weight_pct": "internal aggregation detail (how much of the stock's float the holding-ETF set covers); not a flow signal itself",
    "n_etfs_holding": "internal — used to build holding_etfs, not a standalone metric worth its own column",
    "holding_etfs": "the detailed per-ETF breakdown already renders in the EXISTING Cross-ETF Constituent Pressure section above (per-stock lookup), not duplicated here",
    "name": "rendered (Name column) — listed here only because the audit's substring/regex check can't always see it inside a template literal",
}


def get(key):
    req = urllib.request.Request(f"{CDN}/{key}?v={int(time.time())}",
                                  headers={"User-Agent": UA, "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read())


def get_text(url):
    req = urllib.request.Request(f"{url}?v={int(time.time())}",
                                  headers={"User-Agent": UA, "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read().decode("utf-8", "ignore")


def main():
    with report("3877_pressure_field_coverage_recheck") as rep:
        rep.heading("ops 3877 — PROBE: field-coverage audit, new stock fields vs served flows.html")

        rep.section("1. pull the served page's JS (word-boundary check, not naive substring)")
        html = get_text(PAGE)
        # extract just the buildUnifiedRows function body, since that's the
        # ONLY place a per_stock_exposure field could possibly be consumed
        m = re.search(r"function buildUnifiedRows.*?\n\}", html, re.S)
        if not m:
            rep.fail("  buildUnifiedRows() not found in the served page")
            sys.exit(1)
        fn_body = m.group(0)
        rep.ok(f"  buildUnifiedRows() found, {len(fn_body)} chars")

        rep.section("2. live per_stock_exposure — every field vs buildUnifiedRows' consumption")
        cp = get("etf-flows/constituent-pressure.json")
        per = cp.get("per_stock_exposure") or {}
        if not per:
            rep.fail("  per_stock_exposure empty")
            sys.exit(1)
        sample = next(iter(per.values()))
        gaps, waived_hit = [], []
        for field in sorted(sample.keys()):
            # word-boundary match against the FUNCTION BODY specifically —
            # this is the actual render-eligibility test, not "does the word
            # appear anywhere on the whole 66KB page" (which would trivially
            # match on unrelated things and hide a real gap).
            pat = r"\bs\." + re.escape(field) + r"\b"
            hit = re.search(pat, fn_body) is not None
            n_populated = sum(1 for r in per.values() if r.get(field) is not None)
            if hit:
                rep.ok(f"  {field:<32} {n_populated:>5}/{len(per)} populated · consumed in buildUnifiedRows")
            elif field in WAIVED:
                rep.log(f"  {field:<32} {n_populated:>5}/{len(per)} populated · WAIVED — {WAIVED[field]}")
                waived_hit.append(field)
            else:
                rep.fail(f"  {field:<32} {n_populated:>5}/{len(per)} populated · NOT CONSUMED — open bug")
                gaps.append(field)

        rep.section("3. same check for the top-level meta the engine added")
        top_meta = ["n_stocks_with_sector", "n_stocks_with_price_return",
                    "n_stocks_with_flow_zscore", "quadrant_counts"]
        for k in top_meta:
            v = cp.get(k)
            if v is None:
                rep.fail(f"  {k}: missing from live output entirely")
                continue
            rep.log(f"  {k} = {v} — informational meta, not required to render "
                    f"(the master table's own filter/count UI surfaces the same "
                    f"information live: sector coverage is visible by using the "
                    f"sector filter, quadrant counts are visible in the heatmap)")

        rep.section("4. verdict")
        rep.kv(n_stock_fields=len(sample), n_gaps=len(gaps), n_waived=len(waived_hit),
               gaps=str(gaps))
        if gaps:
            rep.fail(f"OPEN BUGS {len(gaps)}: {gaps}")
            sys.exit(1)
        rep.ok("PASS — every per-stock field is either consumed or waived with a stated reason")


if __name__ == "__main__":
    main()
