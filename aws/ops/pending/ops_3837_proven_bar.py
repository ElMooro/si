"""
ops_3837 — which condition blocks every panel from graduating to PROVEN?

ops 3836 established BRANCH A: all 10 themes have n_proven=0, so
khalid_panel_multiplier is structurally inert. The chain is intact —
justhodl-wl-engines writes w13{t_stat,n_effective} + fdr_pass into
data/wl-engines.json, and wl-fusion reads it — so this is empirical, not a
plumbing break.

wl-fusion requires ALL THREE per engine:
    fdr_pass  AND  |w13.t_stat| >= 2  AND  w13.n_effective >= 6

Counts each condition INDEPENDENTLY so the binding constraint is visible:
  • n_effective the binder  -> needs more TIME, the loop is working correctly
  • fdr_pass the binder     -> multiple-testing correction is rejecting everything
  • t_stat the binder       -> effects are real-signed but too weak
These have completely different responses, and "make the tilt fire" is not one
of them. Nothing is loosened here.

Also arms a schedule for wl-fusion — ops 3836 found it manual-only, the same
gap risk-regime had in ops 3833. That part IS a fix, not a diagnosis.
"""
import json
import sys
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")
sch = boto3.client("scheduler", region_name="us-east-1")


def main():
    with report("3837_proven_bar") as rep:
        rep.heading("ops 3837 — which condition blocks PROVEN?")

        rep.section("1. Read the evidence feed")
        try:
            idx = json.loads(s3.get_object(
                Bucket=BUCKET, Key="data/wl-engines.json")["Body"].read())
        except Exception as e:
            rep.fail(f"  data/wl-engines.json unreadable: {str(e)[:120]}")
            sys.exit(1)
        engines = idx.get("engines") or []
        rep.ok(f"  {len(engines)} engines · generated_at {idx.get('generated_at')}")
        if not engines:
            rep.fail("  empty engines[] — the evidence producer emits nothing")
            sys.exit(1)

        rep.section("2. Condition-by-condition pass counts")
        has_w13 = fdr = tstat = neff = all3 = 0
        rows = []
        for e in engines:
            if not isinstance(e, dict):
                continue
            w = e.get("w13") or {}
            hw = bool(w)
            f = bool(e.get("fdr_pass"))
            t = w.get("t_stat")
            n = w.get("n_effective")
            tp = isinstance(t, (int, float)) and abs(t) >= 2
            np_ = isinstance(n, (int, float)) and n >= 6
            has_w13 += hw
            fdr += f
            tstat += tp
            neff += np_
            if f and tp and np_:
                all3 += 1
            rows.append((e.get("name") or e.get("engine_id") or "?",
                         e.get("theme"), f, t, n, tp and np_ and f))
        tot = len(engines)
        rep.log(f"  engines total ............ {tot}")
        rep.log(f"  carry a w13 block ........ {has_w13}")
        rep.log(f"  fdr_pass ................. {fdr}")
        rep.log(f"  |t_stat| >= 2 ............ {tstat}")
        rep.log(f"  n_effective >= 6 ......... {neff}")
        rep.log(f"  ALL THREE (= proven) ..... {all3}")

        binder = min((("w13 block missing", has_w13),
                      ("fdr_pass", fdr), ("|t_stat|>=2", tstat),
                      ("n_effective>=6", neff)), key=lambda x: x[1])
        rep.warn(f"  BINDING CONSTRAINT: {binder[0]} — only {binder[1]}/{tot} pass it")

        rep.section("3. Closest candidates (nearest to graduating)")
        scored = [r for r in rows if isinstance(r[3], (int, float))]
        scored.sort(key=lambda r: -abs(r[3]))
        rep.log(f"  {'engine':<26}{'theme':<12}{'fdr':>5}{'t_stat':>9}{'n_eff':>8}")
        for nm, th, f, t, n, ok in scored[:12]:
            rep.log(f"  {str(nm)[:25]:<26}{str(th)[:11]:<12}{str(f):>5}"
                    f"{(round(t,2) if isinstance(t,(int,float)) else '-'):>9}"
                    f"{(n if n is not None else '-'):>8}")

        rep.section("4. Verdict")
        if all3 > 0:
            rep.warn(f"  {all3} engine(s) DO meet the bar — yet wl-fusion reported "
                     f"n_proven=0. That is a JOIN DEFECT in wl-fusion, not an "
                     f"evidence gap. Inspect its proven[] filter.")
        elif binder[0] == "n_effective>=6":
            rep.ok("  Evidence gap is SAMPLE SIZE — the loop is working, panels "
                   "simply have not accumulated 6 effective observations. This "
                   "resolves with TIME, not code. Do not loosen the bar.")
        elif binder[0] == "fdr_pass":
            rep.warn("  FDR correction rejects every panel. Either the panels have "
                     "no real edge, or the test is applied over too wide a family. "
                     "Worth reviewing the correction scope — NOT worth disabling.")
        else:
            rep.warn(f"  Binding constraint is {binder[0]}; effects are too weak to "
                     f"clear the bar. Correct outcome: the tilt stays off.")

        rep.section("5. Arm a schedule for wl-fusion (ops 3836 found none)")
        try:
            sch.create_schedule(
                Name="wl-fusion-sched",
                ScheduleExpression="cron(35 22 * * ? *)",
                FlexibleTimeWindow={"Mode": "OFF"},
                Target={"Arn": "arn:aws:lambda:us-east-1:857687956942:function:justhodl-wl-fusion",
                        "RoleArn": "arn:aws:iam::857687956942:role/justhodl-scheduler-role",
                        "Input": "{}"})
            rep.ok("  Scheduler armed cron(35 22 * * ? *)")
        except sch.exceptions.ConflictException:
            rep.ok("  Scheduler already exists")
        except Exception as e:
            rep.warn(f"  could not arm: {str(e)[:90]}")

        rep.kv(engines=tot, fdr_pass=fdr, t_ok=tstat, n_ok=neff, proven=all3,
               binding=binder[0])
        rep.ok("DIAGNOSIS COMPLETE — bar not loosened, schedule armed")


if __name__ == "__main__":
    main()
