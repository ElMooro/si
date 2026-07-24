#!/usr/bin/env python3
"""ops 3794 — rev share + supply-chain dependency blank on the boards.

Khalid: "rev share of industry and supply chain dependency aren't working".

HYPOTHESIS (same class as the 3790 growth bug, one layer wider): the leaderboard
and by_industry members are COPIED dicts snapshotted at line ~879. Everything
computed after that point never reaches them. 3790 patched this by re-copying
fields — but only the SIX growth fields. revenue_share_pct, dependency_pct,
criticality_pctile and friends are computed in the v4.1 percentage block, which
ALSO runs after the snapshot, and were never added to that refresh list.

So all_rows (the Full Ledger) should show them, while the leaderboard and the
industry-member tables show "—" for every row. That is a precise, testable
prediction — this ops checks it before any code is written.

It also checks the OTHER possibility: that dependency_pct is simply sparse
(only 154 of 2,811 names had it, by design, because the supply-chain graph
covers few sectors). If the leaderboard's 50 names happen to fall outside that
coverage, "not working" would actually be correct-but-useless output, and the
honest fix is to say so on the page rather than fake coverage.
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
    with report("3794_diagnose_revshare_dep_blank") as rep:
        rep.heading("ops 3794 — why rev share + dependency are blank on the boards")

        d = json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                                     Key="data/chokepoint.json")["Body"].read())
        cap = d.get("capture_gap") or {}
        rows = cap.get("all_rows") or []
        lead = cap.get("top_undervalued_all_industries") or []
        bi = cap.get("by_industry") or []
        mem = [m for b in bi for m in (b.get("members") or []) if m]
        rep.kv(version=d.get("version"), all_rows=len(rows),
               leaderboard=len(lead), member_rows=len(mem))

        FIELDS = ("revenue_share_pct", "dependency_pct", "criticality_pctile",
                  "revenue_share_suppressed", "criticality_basis",
                  "revenue_currency", "revenue_coverage_pct")

        rep.section("Coverage in all_rows (the Full Ledger — should be fine)")
        for f in FIELDS:
            n = sum(1 for r in rows if r.get(f) is not None)
            rep.log("  %-28s %d / %d" % (f, n, len(rows)))

        rep.section("Coverage on the LEADERBOARD (copied dicts)")
        miss = []
        for f in FIELDS:
            n = sum(1 for r in lead if r.get(f) is not None)
            rep.log("  %-28s %d / %d" % (f, n, len(lead)))
            if n == 0:
                miss.append(f)

        rep.section("Coverage on INDUSTRY MEMBERS (copied dicts)")
        miss_m = []
        for f in ("revenue_share_pct", "dependency_pct", "criticality_pctile"):
            n = sum(1 for m in mem if m.get(f) is not None)
            rep.log("  %-28s %d / %d" % (f, n, len(mem)))
            if n == 0:
                miss_m.append(f)

        rep.section("Is the field even present as a KEY on the copies?")
        if lead:
            sample = lead[0]
            for f in FIELDS:
                rep.log("  leaderboard[0] has key %-28s %s" % (f, f in sample))

        rep.section("Sparsity check — is dependency_pct just rare by design?")
        dep_all = sum(1 for r in rows if r.get("dependency_pct") is not None)
        lead_t = {r.get("ticker") for r in lead}
        dep_lead_possible = sum(1 for r in rows
                                if r.get("ticker") in lead_t and r.get("dependency_pct") is not None)
        rep.kv(dependency_in_ledger=dep_all,
               dependency_available_for_leaderboard_names=dep_lead_possible)
        rs_all = sum(1 for r in rows if r.get("revenue_share_pct") is not None)
        rs_lead_possible = sum(1 for r in rows
                               if r.get("ticker") in lead_t and r.get("revenue_share_pct") is not None)
        rep.kv(revshare_in_ledger=rs_all,
               revshare_available_for_leaderboard_names=rs_lead_possible)

        rep.section("VERDICT")
        if miss:
            rep.warn("CONFIRMED SNAPSHOT BUG: %s are 0/%d on the leaderboard but "
                     "populated in all_rows. The 3790 refresh copies only the six "
                     "growth fields; the v4.1 percentage fields were never added "
                     "to that list." % (", ".join(miss), len(lead)))
            rep.log("FIX: extend the refresh to every field computed after the")
            rep.log("snapshot — or better, refresh from the source row wholesale")
            rep.log("so a future field can never be forgotten again.")
        if dep_lead_possible == 0 and dep_all > 0:
            rep.warn("SEPARATE ISSUE: dependency_pct exists for %d ledger names but "
                     "for ZERO leaderboard names — the supply-chain graph simply "
                     "does not cover the small/micro caps that dominate the board. "
                     "That is honest sparsity, and the page should say so rather "
                     "than showing a silent dash." % dep_all)
        gate(rep, "DIAG.explained", bool(miss) or dep_lead_possible == 0 or rs_lead_possible == 0,
             "root cause identified")
        if FAILED:
            rep.fail("FAILED: %s" % ", ".join(FAILED))
            sys.exit(1)
        rep.ok("PASS_ALL — diagnosis complete")


if __name__ == "__main__":
    main()
