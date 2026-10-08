#!/usr/bin/env python3
"""ops 3806 — verify 'how crucial to industry' is served on both pages.

Khalid clarified the real goal: not filling a column, but "determine how crucial
is that company to that industry". The supply-chain map could never answer that
— FMP's endpoint is dead, the curated map names ~185 symbols, and the leaderboard
is micro/nano caps that no supplier map covers at any price.

Worse, the audit found the gap was actively DISTORTING the answer:
criticality weights supply-chain centrality at 15%, and s_ctr = clamp(ctr/8) was
0 for ~94% of the ledger. Unmapped companies were being docked 15 points on the
exact dimension being asked about.

v4.4 fixes both: centrality's weight is redistributed when unavailable (not
scored as zero), and structural_importance answers the question from data that
exists for every name — revenue rank within industry, gross-margin premium over
the industry median, R&D premium over the median, market-cap concentration, with
supply-chain centrality as a bonus only. Coverage went ~180 -> 1,847 of 1,847.
"""
import sys, json, time, urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report
import boto3

BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")
UA = "JustHodl.AI ops-verify raafouis@gmail.com"
FAILED = []


def gate(rep, n, ok, d=""):
    (rep.ok if ok else rep.fail)(f"{n} :: {d}")
    if not ok:
        FAILED.append(n)
    return ok


def fetch(u, a=0):
    x = u + ("&" if "?" in u else "?") + "v=%d%d" % (int(time.time()), a)
    req = urllib.request.Request(x, headers={"User-Agent": UA, "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return r.read().decode("utf-8", "replace")


def main():
    with report("3806_verify_structural_importance") as rep:
        rep.heading("ops 3806 — 'how crucial to industry' on both surfaces")

        d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/chokepoint.json")["Body"].read())
        cap = d.get("capture_gap") or {}
        rows = cap.get("all_rows") or []
        lead = cap.get("top_undervalued_all_industries") or []
        mem = [m for b in (cap.get("by_industry") or []) for m in (b.get("members") or []) if m]
        st = cap.get("stats") or {}

        rep.section("Feed")
        gate(rep, "FEED.v44", str(d.get("version", "")).startswith("4.4"), "v%s" % d.get("version"))
        si_all = sum(1 for x in rows if x.get("structural_importance") is not None)
        dep_all = sum(1 for x in rows if x.get("dependency_pct") is not None)
        rep.kv(ledger=len(rows), structural_importance=si_all, dependency=dep_all,
               coverage_gain="%.0fx" % (si_all / max(dep_all, 1)))
        gate(rep, "FEED.coverage", si_all > dep_all * 5,
             "%d names vs %d via the supply-chain map" % (si_all, dep_all))
        gate(rep, "FEED.note", bool(cap.get("structural_note")), "method note shipped")

        rep.section("Copies must carry it (the recurring snapshot trap)")
        for label, arr in (("leaderboard", lead), ("members", mem)):
            n = sum(1 for x in arr if x.get("structural_importance") is not None)
            gate(rep, f"COPY.{label}", n > 0, "%d of %d rows" % (n, len(arr)))

        rep.section("Sanity — the score must discriminate, not flatline")
        vals = [x["structural_importance"] for x in rows if x.get("structural_importance") is not None]
        if vals:
            vs = sorted(vals)
            rep.kv(si_min=vs[0], si_p25=vs[len(vs) // 4], si_median=vs[len(vs) // 2],
                   si_p75=vs[3 * len(vs) // 4], si_max=vs[-1])
            gate(rep, "SANITY.spread", (vs[-1] - vs[0]) > 25,
                 "range %.1f — a real cross-section" % (vs[-1] - vs[0]))
            gate(rep, "SANITY.not_saturated", vs[len(vs) // 2] < 90,
                 "median %.1f (not everything is 'crucial')" % vs[len(vs) // 2])

        rep.section("Unmapped names now scored — the whole point")
        unmapped = [x for x in rows if x.get("structural_importance") is not None
                    and not x.get("centrality_mapped")]
        rep.kv(unmapped_but_scored=len(unmapped))
        gate(rep, "FIX.unmapped_scored", len(unmapped) > 100,
             "%d companies outside the curated map now have a crucialness score" % len(unmapped))
        for x in sorted(unmapped, key=lambda z: -(z.get("structural_importance") or 0))[:10]:
            rep.log("  %-6s %-30s SI=%5.1f  %s" % (
                x.get("ticker"), (x.get("industry") or "")[:30],
                x.get("structural_importance") or 0, (x.get("structural_basis") or "")[:46]))

        rep.section("Served pages")
        cg = ""
        for a in range(1, 10):
            try:
                cg = fetch("https://justhodl.ai/capture-gap.html", a)
            except Exception as e:
                rep.warn(str(e)[:80]); time.sleep(25); continue
            if "v11-ops3806" in cg:
                break
            time.sleep(25)
        gate(rep, "PAGE.stamp", "v11-ops3806" in cg, "capture-gap v11 (%d bytes)" % len(cg))
        gate(rep, "PAGE.si_fn", "function si(" in cg, "renderer present")
        gate(rep, "PAGE.col", "How crucial" in cg, "column labelled")
        gate(rep, "PAGE.gloss", "HOW CRUCIAL TO ITS INDUSTRY" in cg, "glossary entry present")

        try:
            wh = fetch("https://justhodl.ai/why.html")
            gate(rep, "WHY.tile", "CRUCIAL TO INDUSTRY" in wh, "tile on the research page")
        except Exception as e:
            rep.warn("why.html: %s" % str(e)[:90])

        rep.section("Additive")
        for k in ("capture_gap", "revenue_share_pct", "catchup_pct", "criticality"):
            gate(rep, f"ADDITIVE.{k}", any(x.get(k) is not None for x in rows), "preserved")

        rep.section("VERDICT")
        if FAILED:
            rep.fail("FAILED: %s" % ", ".join(FAILED))
            sys.exit(1)
        rep.ok("PASS_ALL — the question is now answerable for the whole ledger")


if __name__ == "__main__":
    main()
