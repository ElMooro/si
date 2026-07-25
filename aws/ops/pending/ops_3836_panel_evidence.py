"""
ops_3836 — is khalid_panel_multiplier dead, or correctly silent?

ops 3835's fleet audit found khalid_panel_multiplier at 0/50, 0/30, 0/9 across
every best-setups collection. Before calling that a bug, read the contract:

  aws/shared/wl_fusion.py multiplier() returns EXACTLY 1.0 unless
    (a) the theme has a proven_tilt AND n_proven > 0, and
    (b) pressure_pctile >= 80 (currently firing).
  Docstring: "ONLY from panels that have proven an edge. Unproven pressure
  returns exactly 1.0 (no effect)."

So 0% active is the DESIGNED behaviour of an evidence-gated tilt, and that is
the right architecture — unproven signal must not move conviction. The real
question is WHICH branch we are in:

  BRANCH A — n_proven == 0 everywhere. No panel has ever accumulated graded
             evidence. That is a LEARNING-LOOP problem: the grader is not
             feeding, and the multiplier can never fire no matter the tape.
  BRANCH B — proven panels exist but pressure_pctile < 80 today. Working as
             intended, simply quiet.

Diagnostic only. Writes no engine code and changes nothing — the correct
response to A and B are completely different, so guessing would be worse than
measuring.
"""
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")
FN = "justhodl-wl-fusion"
lam = boto3.client("lambda", region_name="us-east-1")


def main():
    with report("3836_panel_evidence") as rep:
        rep.heading("ops 3836 — khalid_panel_multiplier: dead or correctly silent?")

        rep.section("1. Is data/wl-fusion.json even being produced?")
        try:
            head = s3.head_object(Bucket=BUCKET, Key="data/wl-fusion.json")
            doc = json.loads(s3.get_object(
                Bucket=BUCKET, Key="data/wl-fusion.json")["Body"].read())
        except Exception as e:
            rep.fail(f"  FEED ABSENT: {str(e)[:120]}")
            rep.fail("  BRANCH A-extreme — wl_fusion.load() returns {} and the "
                     "multiplier is hard-wired to 1.0 forever")
            sys.exit(1)
        gen = doc.get("generated_at")
        rep.ok(f"  present · {head['ContentLength']:,} bytes · LastModified "
               f"{head['LastModified']} · generated_at {gen}")
        try:
            age = (datetime.now(timezone.utc) - datetime.fromisoformat(
                str(gen).replace("Z", "+00:00"))).total_seconds() / 3600
            (rep.ok if age < 48 else rep.warn)(f"  age {age:.1f}h")
        except Exception:
            rep.warn("  generated_at unparseable")

        rep.section("2. Per-theme evidence — the branch decider")
        themes = doc.get("themes") or {}
        if not themes:
            rep.fail(f"  no themes{{}} block. top-level keys: {list(doc)[:14]}")
            sys.exit(1)
        n_proven_total, firing, provable = 0, 0, 0
        rep.log(f"  {'theme':<16}{'n_proven':>9}{'proven_tilt':>13}"
                f"{'pressure_pctile':>17}  would_fire")
        for name, t in sorted(themes.items()):
            if not isinstance(t, dict):
                continue
            npv = t.get("n_proven") or 0
            tilt = t.get("proven_tilt")
            pct = t.get("pressure_pctile")
            n_proven_total += npv if isinstance(npv, int) else 0
            if npv and tilt:
                provable += 1
            fires = bool(npv and tilt and isinstance(pct, (int, float)) and pct >= 80)
            if fires:
                firing += 1
            rep.log(f"  {name[:15]:<16}{str(npv):>9}{str(tilt):>13}"
                    f"{str(pct):>17}  {'YES' if fires else 'no'}")

        rep.section("3. Verdict")
        if provable == 0:
            rep.warn("  BRANCH A — NO theme has both proven_tilt and n_proven>0.")
            rep.warn("  The multiplier CANNOT fire regardless of the tape. This is a")
            rep.warn("  learning-loop gap, not a market condition: panels are never")
            rep.warn("  graduating to proven. Next step is the grader that populates")
            rep.warn("  proven_tilt / n_proven, NOT the multiplier itself.")
        elif firing == 0:
            rep.ok(f"  BRANCH B — {provable} theme(s) ARE provable but none is at "
                   f"pressure_pctile>=80 today. Working as designed, simply quiet.")
        else:
            rep.warn(f"  {firing} theme(s) should be firing yet best-setups shows "
                     f"0/50 active — that IS a defect; the join or the call site is "
                     f"dropping it.")

        rep.section("4. Is the producer scheduled?")
        try:
            sch = boto3.client("scheduler", region_name="us-east-1")
            eb = boto3.client("events", region_name="us-east-1")
            trig = [s_["Name"] for s_ in sch.list_schedules(MaxResults=100)
                    .get("Schedules", []) if "wl-fusion" in s_["Name"]]
            trig += [r["Name"] for r in eb.list_rules(Limit=100).get("Rules", [])
                     if "wl-fusion" in r["Name"]]
            (rep.ok if trig else rep.warn)(
                f"  triggers: {trig or 'NONE — manual-only, same gap as risk-regime (ops 3833)'}")
        except Exception as e:
            rep.warn(f"  schedule check failed: {str(e)[:70]}")

        rep.kv(themes=len(themes), provable=provable, firing=firing,
               n_proven_total=n_proven_total,
               branch=("A — no proven evidence" if provable == 0
                       else "B — provable but quiet" if firing == 0
                       else "DEFECT — should fire but does not"))
        rep.ok("DIAGNOSIS COMPLETE — nothing changed, branch identified")


if __name__ == "__main__":
    main()
