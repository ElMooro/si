"""
ops 3721 — verify the FMP calendar shim reaches the Lambdas, end to end

CONTEXT
═══════
ops 3720 (PASS_ALL) added _fmp_calendar() to aws/shared/benzinga.py and made
fetch_calendar() degrade to it when Massive returns nothing. The probe proved
the source is real: FMP returned 4,000 calendar rows, 2,868 with epsEstimated.

BUT the commit that carried the shim into the repo was the run-ops AUTO-COMMIT,
which is tagged [skip-deploy]. So deploy-lambdas.yml may never have fired for
aws/shared/benzinga.py, and the Lambdas can still be running the old module.
This ops therefore:

  1. Reads the LIVE zip of each consumer and greps for _fmp_calendar.
     (Zip is the only truth — descriptions and LastModified both lie.)
  2. If the shim is absent, force-publishes via update_function_code with a zip
     built from source/ + aws/shared/**, mirroring what deploy-lambdas does.
     The ops-helper deploy_lambda() is NOT used: it bundles source/ ONLY and
     would break `from benzinga import fetch_calendar` outright.
  3. Invokes estimate-revisions and proves the revision arrays finally fill.

WHAT SUCCESS LOOKS LIKE
═══════════════════════
    upward_revisions + downward_revisions > 0
after being 0/0 since 2026-07-15 while the engine reported status=LIVE.

Note the first post-fix run may still show small arrays: eps_rev_pct needs a
baseline observation in the state ledger for the SAME fiscal key. The ledger
holds 436 names, but its stored keys were built from Benzinga fiscal_period /
fiscal_year, and FMP does not supply those fields — so some names will re-seed
under a new key and produce a direction only on the NEXT run. G4 therefore
passes on ANY non-empty array, and G5 records the re-seed count as evidence
rather than failing on it.
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
SHARED_DIR = ROOT / "shared"
BUCKET = "justhodl-dashboard-live"
ER_KEY = "data/estimate-revisions.json"

CONSUMERS = ["justhodl-estimate-revisions", "justhodl-earnings-tracker"]
TARGET = "justhodl-estimate-revisions"

LAM = boto3.client("lambda", region_name="us-east-1")
S3C = boto3.client("s3", region_name="us-east-1")


def live_src(fn, member="lambda_function.py"):
    loc = LAM.get_function(FunctionName=fn)["Code"]["Location"]
    z = zipfile.ZipFile(io.BytesIO(urllib.request.urlopen(loc, timeout=90).read()))
    names = z.namelist()
    body = {}
    for n in names:
        if n.endswith(".py"):
            try:
                body[n] = z.read(n).decode("utf-8", "ignore")
            except Exception:  # noqa: BLE001
                pass
    return names, body


def build_zip(fn):
    """source/ at zip root + aws/shared/*.py alongside — what deploy-lambdas does."""
    buf = io.BytesIO()
    src_dir = ROOT / "lambdas" / fn / "source"
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for p in sorted(src_dir.rglob("*")):
            if p.is_file():
                z.write(p, p.relative_to(src_dir).as_posix())
        for p in sorted(SHARED_DIR.glob("*.py")):
            z.write(p, p.name)
    return buf.getvalue()


def main():
    with report("3721_shim_live_verify") as rep:
        rep.heading("ops 3721 — FMP calendar shim: live verification")
        fails = []
        out = {}

        def gate(n, ok, d):
            rep.log(f"{n} {bool(ok)}")
            print(f"  {n:34} {bool(ok)}  {d}")
            out[n] = {"ok": bool(ok), "detail": d}
            if not ok:
                fails.append(n)

        # ── 1. is the shim in the live zips? ────────────────────────────────
        rep.section("1 — live zip inspection")
        state = {}
        for fn in CONSUMERS:
            try:
                names, body = live_src(fn)
                bz = body.get("benzinga.py", "")
                state[fn] = {
                    "has_benzinga_module": "benzinga.py" in names,
                    "has_shim": "_fmp_calendar" in bz,
                    "n_files": len(names),
                }
            except Exception as e:  # noqa: BLE001
                state[fn] = {"error": str(e)[:110]}
            print(f"    {fn}: {state[fn]}")

        need = [f for f in CONSUMERS if not state.get(f, {}).get("has_shim")]
        gate("G1_shim_state_known", all("error" not in state[f] for f in CONSUMERS),
             f"needs_republish={need} state={state}")

        # ── 2. force-publish where missing ──────────────────────────────────
        if need and not fails:
            rep.section("2 — republish (deploy-lambdas skipped the [skip-deploy] commit)")
            for fn in need:
                try:
                    zb = build_zip(fn)
                    LAM.update_function_code(FunctionName=fn, ZipFile=zb, Publish=False)
                    t0 = time.time()
                    while time.time() - t0 < 180:
                        c = LAM.get_function_configuration(FunctionName=fn)
                        if c.get("LastUpdateStatus") == "Successful":
                            break
                        time.sleep(5)
                    print(f"    republished {fn} ({len(zb)} bytes)")
                    rep.log(f"republished {fn} ({len(zb)} bytes)")
                except Exception as e:  # noqa: BLE001
                    gate(f"G2_republish_{fn[-14:]}", False, str(e)[:150])

            time.sleep(10)
            still = []
            for fn in need:
                try:
                    _, body = live_src(fn)
                    if "_fmp_calendar" not in body.get("benzinga.py", ""):
                        still.append(fn)
                except Exception:  # noqa: BLE001
                    still.append(fn)
            gate("G2_shim_live", not still, f"shim missing after republish: {still or 'none'}")
        else:
            gate("G2_shim_live", True, "shim already present in every consumer zip")

        # ── 3. invoke and prove the arrays fill ─────────────────────────────
        if not fails:
            rep.section("3 — invoke estimate-revisions")
            before_doc = {}
            before = None
            try:
                before = S3C.head_object(Bucket=BUCKET, Key=ER_KEY)["LastModified"]
                before_doc = json.loads(
                    S3C.get_object(Bucket=BUCKET, Key=ER_KEY)["Body"].read())
            except Exception:  # noqa: BLE001
                pass

            LAM.invoke(FunctionName=TARGET, InvocationType="Event", Payload=b"{}")
            t0 = time.time()
            fresh = False
            while time.time() - t0 < 600:
                time.sleep(15)
                try:
                    h = S3C.head_object(Bucket=BUCKET, Key=ER_KEY)
                    if before is None or h["LastModified"] > before:
                        fresh = True
                        break
                except Exception:  # noqa: BLE001
                    pass
            gate("G3_refreshed", fresh, f"artifact refreshed in {round(time.time()-t0)}s")

            d = json.loads(S3C.get_object(Bucket=BUCKET, Key=ER_KEY)["Body"].read())
            up = d.get("upward_revisions") or []
            dn = d.get("downward_revisions") or []
            gate("G4_directions_populate", bool(up or dn),
                 f"upward={len(up)} downward={len(dn)} "
                 f"(was {len(before_doc.get('upward_revisions') or [])}/"
                 f"{len(before_doc.get('downward_revisions') or [])}) "
                 f"n_tracked={d.get('n_tracked')} "
                 f"n_fmp_enriched={d.get('n_fmp_enriched')} "
                 f"n_with_history={d.get('n_with_history')}")

            gate("G5_calendar_alive", (d.get("n_tracked") or 0) > 0,
                 f"n_tracked={d.get('n_tracked')} "
                 f"n_with_history={d.get('n_with_history')} "
                 f"n_state_keys={d.get('n_state_keys')} "
                 "(re-seed under new fiscal keys is expected — FMP supplies no "
                 "fiscal_period/fiscal_year, so some names produce a direction "
                 "only on the NEXT run)")

            for label, arr in (("upward", up), ("downward", dn)):
                if arr:
                    print(f"\n  sample {label}:")
                    for r_ in arr[:6]:
                        print(f"    {str(r_.get('ticker')):6} "
                              f"eps_rev={r_.get('eps_rev_pct')}% "
                              f"d2e={r_.get('days_to_earnings')} "
                              f"growth={r_.get('fwd_eps_growth_pct')}")

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
