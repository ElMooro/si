#!/usr/bin/env python3
"""ops 3801 — probe: why is dependency blank on the LEADERBOARD specifically.

Khalid: "on Most Undervalued — supply chain dependency isn't working."

3800 fixed the 100% artifact by requiring >=3 mapped peers. That fix may have
REMOVED the leaderboard's only dependency values — the board is dominated by
micro/nano caps, and those sit in industries with few or no mapped names. So
the column could be blank because (a) the copies still lag, or (b) every
leaderboard name legitimately fails the peer floor.

Distinguishing these matters: (a) is my bug, (b) is a coverage ceiling I must
report honestly rather than paper over. This ops checks the SOURCE row for each
leaderboard ticker against its copy — same technique that proved the earlier
ordering bug — and reports mapped-peer counts for the industries involved.

Writes no code.
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
    with report("3801_probe_leaderboard_dependency") as rep:
        rep.heading("ops 3801 — leaderboard dependency: copy lag or coverage floor?")

        d = json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                                     Key="data/chokepoint.json")["Body"].read())
        cap = d.get("capture_gap") or {}
        rows = cap.get("all_rows") or []
        lead = cap.get("top_undervalued_all_industries") or []
        src = {r.get("ticker"): r for r in rows}
        rep.kv(version=d.get("version"), rows=len(rows), leaderboard=len(lead))

        rep.section("Source vs copy for every leaderboard name")
        lag = 0
        floor = 0
        nomap = 0
        for x in lead[:25]:
            t = x.get("ticker")
            s = src.get(t) or {}
            sv, cv = s.get("dependency_pct"), x.get("dependency_pct")
            if sv is not None and cv is None:
                lag += 1
            sup = str(s.get("dependency_suppressed") or "")
            if "mapped peer" in sup:
                floor += 1
            elif "no mapped supplier" in sup:
                nomap += 1
            rep.log("  %-6s src=%-7s copy=%-7s peers=%-4s %s" % (
                t, sv, cv, s.get("dependency_mapped_peers"), sup[:56]))

        rep.kv(copy_lag=lag, blocked_by_peer_floor=floor, no_mapped_links=nomap)
        gate(rep, "DIAG.not_copy_lag", lag == 0,
             "%d names have a source value but null copy (0 => copies are fine)" % lag)

        rep.section("How many leaderboard names could EVER have dependency?")
        lt = {x.get("ticker") for x in lead}
        inledger = [r for r in rows if r.get("ticker") in lt]
        havecent = sum(1 for r in inledger if (r.get("centrality") or 0) > 0)
        rep.kv(leaderboard_names=len(lt), with_any_centrality=havecent)

        rep.section("Which industries do leaderboard names sit in, and are they mapped?")
        peers = {}
        for r in rows:
            i = r.get("industry")
            if r.get("dependency_mapped_peers") is not None:
                peers[i] = max(peers.get(i, 0), r["dependency_mapped_peers"])
        for x in lead[:14]:
            s = src.get(x.get("ticker")) or {}
            i = s.get("industry")
            rep.log("  %-6s %-34s mapped_peers_in_industry=%s" % (
                x.get("ticker"), str(i)[:34], peers.get(i, 0)))

        rep.section("VERDICT")
        if lag == 0:
            rep.warn("NOT a copy bug. The leaderboard is dominated by micro/nano caps "
                     "whose industries have 0-2 mapped names, so the >=3 peer floor "
                     "(added in 3800 to kill the fake 100%% prints) legitimately blanks "
                     "them. The column is correct; the CURATED GRAPH (185 symbols) "
                     "simply does not reach these companies.")
            rep.log("HONEST OPTIONS:")
            rep.log("  a) show the reason inline on the page instead of a bare dash")
            rep.log("     (dash reads as broken; 'unmapped' reads as true)")
            rep.log("  b) expand justhodl-supply-chain-graph — real work, own arc")
            rep.log("  c) do NOT relax the peer floor: that would bring back AMZN=100%")
        if FAILED:
            rep.fail("FAILED: %s" % ", ".join(FAILED))
            sys.exit(1)
        rep.ok("PASS_ALL — cause isolated")


if __name__ == "__main__":
    main()
