"""ops 4709 — read back the FULL 4708 results: every closed id,
every miss, and specifically whether Khalid's named series
(BAMLEMPTPRVICRPISYTW) succeeded. Also flags the ~67 remaining
worklist items for a follow-up tranche.
"""
import json
import sys

import boto3

from ops_report import report

B = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")
TARGET = "BAMLEMPTPRVICRPISYTW"


def main():
    with report("4709_te_bulk_readback") as r:
        r.heading("ops 4709 — full readback of the 112-series close")
        cov = json.loads(s3.get_object(
            Bucket=B, Key="data/repo-coverage.json")["Body"].read())
        blk = cov.get("te_fred_mirror_bulk") or {}
        closed = blk.get("closed") or []
        missed = blk.get("no_te_data") or []
        if not closed and not missed:
            r.fail("  te_fred_mirror_bulk is empty or missing — "
                  "4708 may not have actually written results")
            sys.exit(1)
        closed_ids = {c["id"] for c in closed}
        missed_ids = {m["id"] for m in missed}

        r.section("Khalid's specific series")
        if TARGET in closed_ids:
            hit = next(c for c in closed if c["id"] == TARGET)
            r.ok("  %s: CLOSED — +%s rows, %s -> %s"
                % (TARGET, hit["added"], hit["first"], hit["last"]))
        elif TARGET in missed_ids:
            hit = next(m for m in missed if m["id"] == TARGET)
            r.log("  %s: NOT in TE's mirror — %s" % (TARGET,
                                                      hit["why"]))
        else:
            r.log("  %s: not found in either list (check worklist "
                 "inclusion)" % TARGET)

        r.section("Full closed list (%d)" % len(closed))
        by_family = {}
        for c in closed:
            fam = c["id"].split("-")[0][:8]
            by_family.setdefault(fam[:4], []).append(c["id"])
        for c in sorted(closed, key=lambda x: x["id"]):
            r.log("  %-24s +%s rows  %s -> %s"
                 % (c["id"], c["added"], c["first"], c["last"]))

        r.section("Full miss list (%d) — pattern check" % len(missed))
        em_misses = [m["id"] for m in missed if "EM" in m["id"]]
        non_em_misses = [m["id"] for m in missed
                         if "EM" not in m["id"]]
        r.log("  EM-family misses: %d" % len(em_misses))
        r.log("  non-EM misses: %d -> %s" % (len(non_em_misses),
                                             non_em_misses))

        r.section("Remaining worklist for a follow-up tranche")
        r.log("  %d series still need work: %s"
             % (len(missed_ids), sorted(missed_ids)[:30]))

        r.ok("readback complete")


if __name__ == "__main__":
    main()
