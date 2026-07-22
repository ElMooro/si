"""
ops 3724 — estimate-revisions v3.0.0: snapshot the MOVING consensus, not a constant

WHAT OPS 3723 PROVED
════════════════════
The ledger is healthy and the arrays are still empty, for a reason no amount of
plumbing repair could have fixed:

    n_keys=448  baselines usable=369 (82.4%)  zero=0
    obs span 2026-07-03 .. 2026-07-22 (20 distinct days), 407 keys at MAX_OBS=10
    keys with >=2 obs AND usable eps = 363
    move_distribution_vs_threshold = {'ZERO': 363}      <-- every single one

363 names, 20 days, and EXACTLY ZERO drift on all of them. That is not a quiet
tape. That is a constant being snapshotted over and over.

ROOT CAUSE
══════════
`estimated_eps` on the Benzinga/Massive EARNINGS CALENDAR is a scheduled-event
attribute: the consensus attached to a future report date, fixed when the event
is scheduled and effectively frozen until the company reports. The 3722 probe
sample shows the shape plainly:

    WSR  date_status="projected"  estimated_eps=0.28  previous_eps=0.26

`previous_eps` there is the PRIOR QUARTER'S ACTUAL, not a prior estimate. So the
engine has been diffing a constant against itself since inception. It would have
printed ZERO even with Benzinga in perfect health — which also explains why
n_tracked sat at exactly 436 through every intervention in ops 3717-3721.

This engine never measured revisions. It measured a scheduling field.

THE FIX
═══════
The genuinely moving consensus is ALREADY being fetched and then discarded:
fmp_estimate_profile() returns `fwd_eps_cur` from FMP /stable/analyst-estimates
(epsAvg), a rolling analyst mean that updates as estimates are revised. Today it
is used only for the fwd_eps_growth_pct slope.

    v2.x:  cur_eps = calendar.estimated_eps          (static -> always ZERO)
    v3.0:  cur_eps = fmp.fwd_eps_cur                 (moving -> real revisions)
           calendar.estimated_eps retained as `sched_eps` for context only

Ordering matters and is already satisfied: `fmp` is fully populated by the
ThreadPoolExecutor block BEFORE the row loop (verified at ops 3717 G0b —
fmp at char 746, loop at char 5093).

LEDGER MIGRATION
════════════════
Existing obs hold the static calendar value, so the first v3 run would diff a
moving number against a frozen one and manufacture fake revisions. The ledger
must therefore be RE-SEEDED: this ops stamps a schema marker into the state file
and the engine drops pre-v3 observations for a key on first sight, restarting
that key's history from the FMP series. Real directions appear from the SECOND
v3 run onward — honest accrual, no synthetic day-one signal.

COVERAGE NOTE (recorded, not hidden)
FMP_SEED_CAP=280 bounds how many names get an FMP profile per run, so v3 tracks
~280 names rather than 436. That is a REAL reduction in breadth in exchange for
a signal that actually exists. Raising the cap is a separate decision with a
rate-limit cost; flagged for Khalid rather than silently changed.
"""
import io
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

import boto3  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "lambdas" / "justhodl-estimate-revisions" / "source" / "lambda_function.py"
BUCKET = "justhodl-dashboard-live"
STATE_KEY = "estimate-revisions/state.json"

S3C = boto3.client("s3", region_name="us-east-1")

OLD = '''        # Benzinga (Massive) has been 403 NOT_AUTHORIZED since 2026-07-15, so
        # estimated_eps arrives empty and every direction collapsed to None.
        # Fall back to the FMP forward consensus we already fetch for this name.
        # Benzinga keeps precedence so entitlement restoration is automatic.
        _prof_early = fmp.get(tk) or {}
        cur_eps = _num(r.get("estimated_eps"))
        if cur_eps is None:
            cur_eps = _num(_prof_early.get("fwd_eps_cur"))
        cur_rev = _num(r.get("estimated_revenue"))'''

NEW = '''        # v3.0.0 — the calendar's estimated_eps is a SCHEDULED-EVENT field: it is
        # fixed when the earnings date is scheduled and does not move until the
        # company reports. Snapshotting it produced 363/363 keys with EXACTLY
        # zero drift over 20 days (ops 3723). It never measured revisions.
        # The moving consensus is FMP analyst-estimates epsAvg (fwd_eps_cur),
        # already fetched here and previously used only for the growth slope.
        _prof_early = fmp.get(tk) or {}
        sched_eps = _num(r.get("estimated_eps"))          # context only
        cur_eps = _num(_prof_early.get("fwd_eps_cur"))    # the revisable number
        cur_rev = _num(r.get("estimated_revenue"))'''

OLD_ROW = '''            "current_eps_est": cur_eps, "baseline_eps_est": obs[0][1] if obs else None,'''
NEW_ROW = '''            "current_eps_est": cur_eps, "baseline_eps_est": obs[0][1] if obs else None,
            "scheduled_eps_est": sched_eps,
            "consensus_source": "fmp_analyst_estimates_epsAvg",'''

OLD_REC = '''        rec = keys.get(key) or {"t": tk, "fp": fp, "fy": fy, "obs": []}
        obs = rec["obs"]'''
NEW_REC = '''        rec = keys.get(key) or {"t": tk, "fp": fp, "fy": fy, "obs": [], "sv": 3}
        # Pre-v3 observations hold the STATIC calendar estimate. Diffing the new
        # moving consensus against a frozen baseline would manufacture a fake
        # revision, so drop that history and re-seed the key from FMP. Real
        # directions therefore appear from the SECOND v3 run onward.
        if rec.get("sv") != 3:
            rec = {"t": tk, "fp": fp, "fy": fy, "obs": [], "sv": 3}
        obs = rec["obs"]'''


def main():
    with report("3724_consensus_v3") as rep:
        rep.heading("ops 3724 — estimate-revisions v3.0.0 (moving consensus)")
        fails = []
        out = {}

        def gate(n, ok, d):
            rep.log(f"{n} {bool(ok)}")
            print(f"  {n:34} {bool(ok)}  {d}")
            out[n] = {"ok": bool(ok), "detail": d}
            if not ok:
                fails.append(n)

        src = io.open(SRC, encoding="utf-8").read()

        # G0 — producer field must exist before we depend on it
        gate("G0_key_contract", '"fwd_eps_cur": eps_cur' in src,
             "fmp_estimate_profile() returns fwd_eps_cur (the moving epsAvg)")

        i_pool, i_loop = src.find("ThreadPoolExecutor"), src.find('key = f"{tk}|{fp}|{fy}"')
        gate("G0b_fmp_before_loop", 0 < i_pool < i_loop,
             f"fmp populated at {i_pool} before row loop at {i_loop}")

        if "consensus_source" in src and '"sv": 3' in src:
            gate("G1_patched", True, "already v3 (idempotent re-run)")
        else:
            ok = True
            for a, b, label in ((OLD, NEW, "cur_eps"), (OLD_ROW, NEW_ROW, "row"),
                                (OLD_REC, NEW_REC, "ledger")):
                if a not in src:
                    gate(f"G1_anchor_{label}", False, "anchor missing")
                    ok = False
                else:
                    src = src.replace(a, b, 1)
            if ok:
                src = src.replace('"version": "2.1.0"', '"version": "3.0.0"', 1)
                io.open(SRC, "w", encoding="utf-8").write(src)
                gate("G1_patched",
                     "consensus_source" in src and '"sv": 3' in src
                     and '"version": "3.0.0"' in src,
                     "cur_eps <- fmp fwd_eps_cur; sched_eps kept for context; "
                     "ledger re-seeds pre-v3 keys; version -> 3.0.0")

        try:
            import py_compile
            py_compile.compile(str(SRC), doraise=True)
            gate("G2_compiles", True, "engine source compiles")
        except Exception as e:  # noqa: BLE001
            gate("G2_compiles", False, f"compile error: {str(e)[:150]}")

        # record the pre-change ledger state so the re-seed is auditable
        try:
            st = json.loads(S3C.get_object(Bucket=BUCKET, Key=STATE_KEY)["Body"].read())
            ks = st.get("keys") or {}
            pre_v3 = sum(1 for v in ks.values() if v.get("sv") != 3)
            gate("G3_ledger_snapshot", True,
                 f"n_keys={len(ks)} pre_v3_keys_to_reseed={pre_v3} "
                 f"updated={st.get('updated')}")
        except Exception as e:  # noqa: BLE001
            gate("G3_ledger_snapshot", False, str(e)[:140])

        out["verdict"] = ("PASS_ALL" if not fails else "GAPS: " + ",".join(fails))
        print("\nVERDICT:", out["verdict"])
        print("NOTE: deploy-lambdas bundles aws/shared/**; live verification is "
              "ops 3725 AFTER settle. Directions appear on the SECOND v3 run — "
              "the first only re-seeds baselines.")
        rep.log("VERDICT: " + out["verdict"])
        for _k, _v in out.items():
            if isinstance(_v, dict):
                rep.kv(gate=_k, ok=_v.get("ok"), detail=str(_v.get("detail"))[:170])

        if fails:
            sys.exit(1)


if __name__ == "__main__":
    main()
