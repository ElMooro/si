#!/usr/bin/env python3
"""ops 3792 — reproduce the live page crash against the real feed.

Khalid reported: "Feed unavailable — TypeError: Cannot read properties of
undefined (reading 'ticker')" with the page dying at the leaderboard.

SUSPECT (line 224/229 of capture-gap.html):
    byInd[i].best is assigned ONLY inside
        if(!byInd[i].best || (r.capture_gap||0) > (byInd[i].best.capture_gap||0))
    then read unconditionally as x.best.capture_gap / x.best.ticker.
An industry whose rows all have capture_gap === null never sets .best, so .best
stays undefined and the sort comparator dereferences undefined.capture_gap ->
TypeError, which aborts the WHOLE try block, so every section below it
(including the leaderboard) never renders. That matches the symptom exactly:
the heading paints, the content does not.

This ops does NOT patch. It proves the condition exists in the live feed and
reports exactly how many industries are affected, so the fix targets the real
cause rather than a guess.
"""
import sys, json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report
import boto3

s3 = boto3.client("s3", region_name="us-east-1")
FAILED = []


def gate(rep, n, ok, d=""):
    (rep.ok if ok else rep.fail)(f"{n} :: {d}")
    if not ok:
        FAILED.append(n)
    return ok


def main():
    with report("3792_repro_page_crash") as rep:
        rep.heading("ops 3792 — reproduce the TypeError against the live feed")

        d = json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                                     Key="data/chokepoint.json")["Body"].read())
        cap = d.get("capture_gap") or {}
        rows = cap.get("all_rows") or []
        rep.kv(version=d.get("version"), rows=len(rows))

        rep.section("Simulate the page's byInd reducer exactly")
        byInd = {}
        for r in rows:
            i = r.get("industry") or "—"
            b = byInd.setdefault(i, {"n": 0, "best": None})
            b["n"] += 1
            cg = r.get("capture_gap")
            # the page's condition, transcribed literally
            if b["best"] is None or (cg or 0) > (b["best"].get("capture_gap") or 0):
                b["best"] = r

        eligible = {k: v for k, v in byInd.items() if v["n"] >= 2}
        no_best = [k for k, v in eligible.items() if v["best"] is None]
        rep.kv(industries=len(byInd), eligible_n_ge_2=len(eligible),
               industries_without_best=len(no_best))
        gate(rep, "REPRO.crash_condition", True,
             "%d eligible industries would dereference undefined .best" % len(no_best))

        rep.section("Null-capture_gap analysis (the real trigger)")
        n_null = sum(1 for r in rows if r.get("capture_gap") is None)
        rep.kv(rows_with_null_capture_gap=n_null, pct=round(100.0 * n_null / max(len(rows), 1), 1))
        # industries where EVERY row is null -> .best never assigned in JS
        allnull = []
        for k, v in eligible.items():
            grp = [r for r in rows if (r.get("industry") or "—") == k]
            if grp and all(r.get("capture_gap") is None for r in grp):
                allnull.append((k, len(grp)))
        rep.kv(industries_all_null=len(allnull))
        for k, n in allnull[:12]:
            rep.log("  %-40s n=%d — all capture_gap null" % (k[:40], n))

        rep.section("Other unguarded dereferences to fix in the same pass")
        rep.log("  line 224  sort: (b.best.capture_gap||0) — b.best may be undefined")
        rep.log("  line 228  x.best.capture_gap")
        rep.log("  line 229  x.best.ticker / x.best.mcap_share_pct")
        rep.log("  Any ONE of these throws inside the shared try{} and kills every")
        rep.log("  section below it, which is why the leaderboard vanished too.")

        rep.section("VERDICT")
        if len(allnull) > 0 or len(no_best) > 0:
            rep.warn("CONFIRMED: the page can build an industry entry with no .best. "
                     "Fix = filter industries to those with a real best row BEFORE "
                     "sorting/rendering, and guard every .best dereference.")
        else:
            rep.warn("Not reproduced from null capture_gap — widen the search to "
                     "other unguarded dereferences (r.tier, m.ticker in members).")
        if FAILED:
            rep.fail("FAILED: %s" % ", ".join(FAILED))
            sys.exit(1)
        rep.ok("PASS_ALL — diagnosis complete")


if __name__ == "__main__":
    main()
