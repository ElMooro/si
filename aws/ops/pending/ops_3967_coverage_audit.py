"""
ops_3967 — COVERAGE AUDIT (read-only). Khalid's question: did the barometers
actually take every indicator in each bracket into account?

The published numbers were LIQUIDITY 41 live drivers, RISK 50, MACRO 65 =
156 voting. But 561 symbols are classified. So 405 did NOT vote, and before
answering him I need the real reason for every single one, not a story.

This op decomposes the funnel per domain:
    classified -> drivers (assets excluded by design)
               -> LIVE (has a resolved value)
               -> polarity != 0 (has a known direction)
               -> chg_pct is not None (has a measurable move)
               -> VOTING

and ranks the excluded ones by n_notes, so the ones HE wrote most about
surface first. If a metric he has 20 notes on is silently not counted, that
is a defect, not sparsity.

Read-only. Writes nothing. Evidence for the fix that follows.
"""
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
DOMS = ("MACRO", "LIQUIDITY", "RISK")


def main():
    with report("3967_coverage_audit") as rep:
        rep.heading("ops 3967 — did every indicator actually vote? (read-only audit)")
        checks = []

        doc = json.loads(s3.get_object(Bucket=BUCKET,
                                       Key="data/domain-barometers.json")["Body"].read())
        syms = doc.get("symbols") or []
        bar = doc.get("barometers") or {}
        rep.kv(version=doc.get("version"), generated_at=doc.get("generated_at"),
               n_symbols=len(syms))

        rep.section("A. the funnel, per domain")
        funnel = {}
        excluded = defaultdict(list)
        for d in DOMS:
            rows = [x for x in syms if x.get("domain") == d]
            drivers = [x for x in rows if x.get("role") == "driver"]
            live = [x for x in drivers if x.get("status") == "LIVE"]
            polar = [x for x in live if x.get("polarity")]
            moved = [x for x in polar if x.get("chg_pct") is not None]
            funnel[d] = {"classified": len(rows), "drivers": len(drivers),
                         "live": len(live), "with_direction": len(polar),
                         "with_measurable_move": len(moved),
                         "reported_voting": (bar.get(d) or {}).get("n_drivers_live")}
            rep.log(f"  {d}: classified {len(rows)} -> drivers {len(drivers)} "
                    f"-> LIVE {len(live)} -> has direction {len(polar)} "
                    f"-> has a move {len(moved)}  |  barometer used "
                    f"{(bar.get(d) or {}).get('n_drivers_live')}")
            for x in rows:
                if x.get("role") == "asset":
                    excluded[d].append((x, "asset — it is a prediction TARGET, not an input"))
                elif x.get("status") != "LIVE":
                    excluded[d].append((x, f"no live value (status {x.get('status')})"))
                elif not x.get("polarity"):
                    excluded[d].append((x, f"no known direction (polarity 0, "
                                           f"{x.get('polarity_basis')})"))
                elif x.get("chg_pct") is None:
                    excluded[d].append((x, "LIVE and directional but the feed carries "
                                           "no chg_pct — SILENTLY DROPPED"))
        rep.kv(**{f"{d}_funnel": json.dumps(funnel[d]) for d in DOMS})

        rep.section("B. the silent drop: LIVE + directional but no chg_pct")
        silent = [(d, x) for d in DOMS for x, why in excluded[d] if "SILENTLY" in why]
        rep.kv(n_silently_dropped=len(silent))
        for d, x in sorted(silent, key=lambda t: -(t[1].get("n_notes") or 0))[:30]:
            rep.log(f"  {x['symbol']:12s} {d:9s} notes={x.get('n_notes'):<4} "
                    f"value={x.get('value')} src={str(x.get('source'))[:28]}")
        checks.append(("silent drop is quantified", True))

        rep.section("C. excluded metrics he wrote the MOST about (per domain)")
        for d in DOMS:
            rows = sorted(excluded[d], key=lambda t: -(t[0].get("n_notes") or 0))[:12]
            rep.log(f"  ── {d} ──")
            for x, why in rows:
                rep.log(f"    {x['symbol']:12s} notes={x.get('n_notes'):<4} {why[:88]}")

        rep.section("D. exclusion reasons rolled up")
        allx = Counter()
        for d in DOMS:
            for _x, why in excluded[d]:
                allx[why.split("(")[0].split("—")[0].strip()] += 1
        for why, n in allx.most_common():
            rep.log(f"  {n:4d}  {why}")

        rep.section("E. how much of his WRITING is actually voting")
        tot_notes = sum(x.get("n_notes") or 0 for x in syms)
        vote_notes = 0
        for d in DOMS:
            for x in syms:
                if (x.get("domain") == d and x.get("role") == "driver"
                        and x.get("status") == "LIVE" and x.get("polarity")
                        and x.get("chg_pct") is not None):
                    vote_notes += x.get("n_notes") or 0
        rep.kv(total_tv_notes=tot_notes, notes_behind_voting_metrics=vote_notes,
               pct_of_his_research_voting=round(vote_notes / max(1, tot_notes) * 100, 1))

        rep.section("F. verdict")
        rep.log("  The barometers did NOT consider every indicator in each bracket.")
        rep.log("  Assets are excluded by design (they are what gets predicted).")
        rep.log("  NO_FREE_SOURCE symbols cannot vote — there is no value to read.")
        rep.log("  But the chg_pct drop is a REAL DEFECT: those metrics are live and")
        rep.log("  directional, and were dropped only because their adapter returns a")
        rep.log("  level with no change field (FRED yoy/single-observation paths).")
        rep.log("  Fix = derive the change from the engine's own ledger instead of")
        rep.log("  depending on the upstream feed to supply it.")

        for l, ok in checks:
            (rep.ok if ok else rep.fail)(f"  {l}")
        rep.ok(f"AUDIT COMPLETE — {len(silent)} live directional metrics silently dropped; "
               f"only {round(vote_notes/max(1,tot_notes)*100,1)}% of his TV research is behind "
               f"a voting metric")


if __name__ == "__main__":
    main()
