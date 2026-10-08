"""
ops 3717 — estimate-revisions: re-source cur_eps onto FMP (Benzinga 403-dead)

ROOT CAUSE, ISOLATED AT OPS 3716 (not guessed)
══════════════════════════════════════════════
The engine looks healthy and is NOT healthy:
    status=LIVE  n_tracked=436  n_fmp_enriched=256  n_with_history=436
    upward_revisions=0  downward_revisions=0        <-- emits nothing usable

Mechanism:
    line 133  cur_eps = _num(r.get("estimated_eps"))     # r = BENZINGA calendar row
    line 146  eps_rev_pct = (cur_eps - baseline) / |baseline| * 100
    line 174  direction = UP if eps_rev_pct > 0 else DOWN if < 0 else None
    line 190  up   = [s for s in signals if s["direction"] == "UP"]

Benzinga has been 403 NOT_AUTHORIZED across all three Massive keys since
2026-07-15, so `estimated_eps` is empty on every row. cur_eps is None ->
eps_rev_pct is None -> direction is None for all 436 names -> both arrays
empty. The snapshot ledger is FINE (n_with_history=436); only the current
observation is missing. This engine was MISSED in the ops 3311-3323 sweep that
re-sourced its peers onto aws/shared/fmp_analyst.py.

Downstream blast radius: readthrough has been reporting
`degraded=['fundamental sidecar missing: estimate-revisions']`, which is why
consensus observability collapsed to CONSENSUS_NOT_DUE_YET on 53 of 74 rows
and why ops 3707/3710 concluded "consensus is the binding constraint". It was
never a consensus problem. It was a dead field.

THE FIX (minimal, reversible, preserves Benzinga precedence)
═══════════════════════════════════════════════════════════
fmp_estimate_profile() ALREADY returns `fwd_eps_cur` (FMP /stable
analyst-estimates epsAvg) and is ALREADY called for the seeded names — today
it is used only for the growth slope. So the live consensus number is already
in memory and simply discarded.

    cur_eps = benzinga_estimated_eps  OR  fmp_profile["fwd_eps_cur"]

If Massive ever re-enables the Benzinga entitlement, the original field wins
again automatically. No API change, no new dependency, no new key.

Because the ledger already holds 436 names of history, revision deltas begin
accruing on the very next run, and direction/up/down populate from real
snapshot diffs rather than being synthesised.

NOTE the profile is fetched AFTER cur_eps today, so the fix also moves the
profile lookup ahead of the cur_eps assignment. Verified in-source: `fmp` is
fully populated before the row loop begins (ThreadPoolExecutor block at
line ~113), so `fmp.get(tk)` is safe at that point.
"""
import io
import json
import sys
import time
import urllib.request
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

import boto3  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "lambdas" / "justhodl-estimate-revisions" / "source" / "lambda_function.py"
FN = "justhodl-estimate-revisions"
BUCKET = "justhodl-dashboard-live"
KEY = "data/estimate-revisions.json"

LAM = boto3.client("lambda", region_name="us-east-1")
S3C = boto3.client("s3", region_name="us-east-1")

OLD = '''        cur_eps = _num(r.get("estimated_eps"))
        cur_rev = _num(r.get("estimated_revenue"))'''

NEW = '''        # Benzinga (Massive) has been 403 NOT_AUTHORIZED since 2026-07-15, so
        # estimated_eps arrives empty and every direction collapsed to None.
        # Fall back to the FMP forward consensus we already fetch for this name.
        # Benzinga keeps precedence so entitlement restoration is automatic.
        _prof_early = fmp.get(tk) or {}
        cur_eps = _num(r.get("estimated_eps"))
        if cur_eps is None:
            cur_eps = _num(_prof_early.get("fwd_eps_cur"))
        cur_rev = _num(r.get("estimated_revenue"))'''


def main():
    with report("3717_est_rev_fmp_resource") as rep:
        rep.heading("ops 3717 — estimate-revisions cur_eps re-sourced onto FMP")
        fails = []
        out = {}

        def gate(n, ok, d):
            rep.log(f"{n} {bool(ok)}")
            print(f"  {n:34} {bool(ok)}  {d}")
            out[n] = {"ok": bool(ok), "detail": d}
            if not ok:
                fails.append(n)

        src = io.open(SRC, encoding="utf-8").read()

        # G0 KEY CONTRACT — prove the producer field exists before relying on it
        gate("G0_key_contract", '"fwd_eps_cur": eps_cur' in src,
             "fmp_estimate_profile() returns fwd_eps_cur "
             f"(present={'\"fwd_eps_cur\": eps_cur' in src})")

        # G0b — prove `fmp` is populated before the row loop uses it
        i_pool = src.find("ThreadPoolExecutor")
        i_loop = src.find('cur_eps = _num(r.get("estimated_eps"))')
        gate("G0b_fmp_ready_before_loop", 0 < i_pool < i_loop,
             f"fmp populated at char {i_pool} before row loop at {i_loop}")

        if "fwd_eps_cur" in src and "_prof_early" in src:
            gate("G1_patch_applied", True, "already patched (idempotent re-run)")
        elif OLD not in src:
            gate("G1_patch_applied", False, "anchor not found — inspect source manually")
        else:
            src = src.replace(OLD, NEW, 1)
            src = src.replace('"version": "2.0.0"', '"version": "2.1.0"', 1)
            io.open(SRC, "w", encoding="utf-8").write(src)
            gate("G1_patch_applied", "_prof_early" in src,
                 "cur_eps falls back to FMP fwd_eps_cur; version -> 2.1.0")

        if not fails:
            # deploy-lambdas.yml handles the zip on this same push (it bundles
            # aws/shared/**); do NOT also call deploy_lambda() here.
            t0 = time.time()
            live = False
            while time.time() - t0 < 420:
                try:
                    loc = LAM.get_function(FunctionName=FN)["Code"]["Location"]
                    z = zipfile.ZipFile(
                        io.BytesIO(urllib.request.urlopen(loc, timeout=60).read()))
                    s = z.read("lambda_function.py").decode("utf-8", "ignore")
                    if "_prof_early" in s:
                        live = True
                        break
                except Exception as e:  # noqa: BLE001
                    print("   settle retry:", str(e)[:60])
                time.sleep(20)
            gate("G2_artifact_live", live,
                 f"patched zip proven live after {round(time.time()-t0)}s")

            if live:
                before = None
                try:
                    before = S3C.head_object(Bucket=BUCKET, Key=KEY)["LastModified"]
                except Exception:  # noqa: BLE001
                    pass
                LAM.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
                t0 = time.time()
                fresh = False
                while time.time() - t0 < 480:
                    time.sleep(15)
                    try:
                        h = S3C.head_object(Bucket=BUCKET, Key=KEY)
                        if before is None or h["LastModified"] > before:
                            fresh = True
                            break
                    except Exception:  # noqa: BLE001
                        pass
                gate("G3_refreshed", fresh, f"artifact refreshed in {round(time.time()-t0)}s")

                d = json.loads(S3C.get_object(Bucket=BUCKET, Key=KEY)["Body"].read())
                up, dn = d.get("upward_revisions") or [], d.get("downward_revisions") or []
                n_cur = sum(1 for k in ("n_tracked",) if d.get(k))
                gate("G4_directions_populated", bool(up or dn),
                     f"upward={len(up)} downward={len(dn)} n_tracked={d.get('n_tracked')} "
                     f"n_fmp_enriched={d.get('n_fmp_enriched')} version={d.get('version')} "
                     f"(n_cur={n_cur})")

                if up or dn:
                    print("\n  sample upward:")
                    for r_ in up[:6]:
                        print(f"    {r_.get('ticker'):6} eps_rev={r_.get('eps_rev_pct')}% "
                              f"d2e={r_.get('days_to_earnings')} "
                              f"growth={r_.get('fwd_eps_growth_pct')}")
                    print("  sample downward:")
                    for r_ in dn[:4]:
                        print(f"    {r_.get('ticker'):6} eps_rev={r_.get('eps_rev_pct')}%")

        out["verdict"] = ("PASS_ALL" if not fails else "GAPS: " + ",".join(fails))
        print("\nVERDICT:", out["verdict"])
        rep.log("VERDICT: " + out["verdict"])
        for _k, _v in out.items():
            if isinstance(_v, dict):
                rep.kv(gate=_k, ok=_v.get("ok"), detail=str(_v.get("detail"))[:170])

        if fails:
            sys.exit(1)


if __name__ == "__main__":
    main()
