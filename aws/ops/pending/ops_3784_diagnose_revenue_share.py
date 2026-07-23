#!/usr/bin/env python3
"""ops 3784 — diagnose implausible revenue shares (NVDA 0.21% of semis?).

3783 shipped green and every gate passed, including share-sums-to-100. But the
NUMBERS are not credible and a passing gate on an incredible number is exactly
the failure mode worth chasing:
    NVDA 0.21%  ASML 0.03%  AMD 0.03%  AVGO 0.06%  |  TSM 3.77%  MSFT 5.81%
NVDA cannot be 0.21% of semiconductor revenue. Two orders of magnitude of
spread between TSM and NVDA inside the same industry means the denominator or
the numerator is wrong for most names, not that the metric is interesting.

HYPOTHESES (this ops tests, does not assume):
 H1 UNIT MISMATCH — `revenue` from the income statement is in absolute USD for
    some names and the ledger-carried rows (scored on an EARLIER version) have
    revenue_ttm = None, so the industry total is built from a handful of freshly
    -scored names while most rows contribute nothing. A name scored today gets
    a share vs a tiny denominator; a name carried from the ledger gets None.
    -> would produce exactly this: a few large shares, many microscopic ones.
 H2 STALE LEDGER ROWS — 2,322 scored but only 1,426 with revenue_share: the
    896 without are pre-v4.1 ledger rows that never had revenue_ttm.
 H3 WRONG FIELD — income statement `revenue` occasionally quarterly not TTM.

The honest outcome may be that this metric CANNOT be published until the ledger
refills with revenue over the next few daily runs. If so, this ops gates the
field OFF rather than leaving a plausible-looking wrong number on the page —
a wrong percentage that looks right is worse than no percentage.
"""
import sys, json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report
import boto3

BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")
FAILED = []


def gate(rep, n, ok, d=""):
    (rep.ok if ok else rep.fail)(f"{n} :: {d}")
    if not ok:
        FAILED.append(n)
    return ok


def main():
    with report("3784_diagnose_revenue_share") as rep:
        rep.heading("ops 3784 — diagnose implausible revenue_share_pct")

        d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/chokepoint.json")["Body"].read())
        cap = d.get("capture_gap") or {}
        rows = cap.get("all_rows") or []
        semis = [r for r in rows if (r.get("industry") or "").startswith("Semiconductor")]

        rep.section("Semiconductors — raw revenue vs share")
        semis_rev = [r for r in semis if r.get("revenue_ttm")]
        rep.kv(semis_scored=len(semis), semis_with_revenue=len(semis_rev),
               semis_missing_revenue=len(semis) - len(semis_rev))
        tot = sum(r["revenue_ttm"] for r in semis_rev)
        rep.kv(industry_revenue_total=tot)
        for r in sorted(semis_rev, key=lambda x: -(x.get("revenue_ttm") or 0))[:12]:
            rep.log("  %-6s revenue_ttm=%-18s share=%-8s mcap=%s" % (
                r.get("ticker"), "{:,.0f}".format(r["revenue_ttm"]),
                ("%.2f%%" % r["revenue_share_pct"]) if r.get("revenue_share_pct") is not None else "—",
                "{:,.0f}".format(r.get("market_cap") or 0)))

        rep.section("H1/H2 — are missing-revenue rows carried ledger rows?")
        no_rev = [r for r in rows if not r.get("revenue_ttm")]
        rep.kv(total_rows=len(rows), rows_without_revenue=len(no_rev))
        gate(rep, "DIAG.coverage_gap", len(no_rev) > 0,
             "%d of %d rows carry NO revenue → they contribute 0 to the denominator"
             % (len(no_rev), len(rows)))

        rep.section("H3 — magnitude check on known names")
        # NVDA TTM revenue is ~$130-200B in this era; TSM ~$90-100B.
        for t, lo, hi in (("NVDA", 5e10, 4e11), ("TSM", 4e10, 2e11),
                          ("ASML", 2e10, 6e10), ("AMD", 1e10, 6e10),
                          ("AVGO", 3e10, 1e11), ("INTC", 3e10, 1e11)):
            r = next((x for x in rows if x.get("ticker") == t), None)
            if not r:
                rep.log("  %-5s not scored" % t); continue
            rv = r.get("revenue_ttm")
            if rv is None:
                rep.log("  %-5s revenue_ttm=None  (carried ledger row, pre-v4.1)" % t)
                continue
            ok = lo <= rv <= hi
            rep.log("  %-5s revenue_ttm=%-16s plausible=%s (expected %.0fB-%.0fB)" % (
                t, "{:,.0f}".format(rv), ok, lo / 1e9, hi / 1e9))

        rep.section("VERDICT")
        semis_missing = len(semis) - len(semis_rev)
        if semis_missing > len(semis) * 0.2:
            rep.warn("DENOMINATOR IS INCOMPLETE: %d of %d semis contribute no revenue, "
                     "so every published share is computed against a partial industry "
                     "total and is therefore WRONG — not merely imprecise." % (
                         semis_missing, len(semis)))
            rep.warn("revenue_share_pct must be SUPPRESSED until the ledger refills "
                     "with revenue on subsequent daily runs. A wrong percentage that "
                     "looks plausible is worse than no percentage.")
            rep.log("NEXT: gate the field to null unless the industry's revenue "
                    "coverage clears a floor (e.g. >=70% of scored names carry "
                    "revenue), and surface coverage on every row that does publish.")
        else:
            rep.ok("coverage acceptable — shares are computed against a near-complete total")

        if FAILED:
            rep.fail("FAILED: %s" % ", ".join(FAILED))
            sys.exit(1)
        rep.ok("PASS_ALL — diagnosis complete (no engine change in this ops)")


if __name__ == "__main__":
    main()
