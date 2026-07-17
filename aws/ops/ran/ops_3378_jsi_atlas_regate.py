"""ops 3378 — JSI atlas re-gate: correct sanity spec (the data was right).

3377-G3 asserted high-stress deciles must show WORSE forward returns — the
atlas empirically shows the opposite (decile-9 3m median +8.2% vs decile-2
+3.1%, n=724): peak stress has been the contrarian NASDAQ buy point since
1990, exactly the mean-reversion result the literature reports. Gate the
INVARIANTS (coverage, ordering p25≤med≤p75, cell sizes, current bucket),
record the contrarian spread as a finding.
"""
import json, sys
from pathlib import Path
import boto3
from ops_report import report

S3C = boto3.client("s3", "us-east-1")

with report("3378_jsi_atlas_regate") as rep:
    rep.heading("ops 3378 — JSI atlas invariant re-gate")
    out = {"gates": {}}; fails = []
    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:300]}
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:260]
        print(line); rep.log(line)
        if not ok: fails.append(n)

    j = json.loads(S3C.get_object(Bucket="justhodl-dashboard-live", Key="data/jsi.json")["Body"].read())
    v2 = j.get("v2") or {}; at = v2.get("atlas") or {}; bd = at.get("by_decile") or {}
    hist_n = (j.get("history_span") or {}).get("n") or 0
    cells_ok, cov, orders = True, 0, True
    for d in map(str, range(10)):
        c1 = (bd.get(d) or {}).get("1m")
        if not c1: cells_ok = False; continue
        cov += c1["n"]
        for w in ("1m", "3m", "6m", "12m"):
            st = (bd.get(d) or {}).get(w)
            if st and not (st["p25"] <= st["med"] <= st["p75"]): orders = False
    gate("G1_coverage", cells_ok and cov >= hist_n - 600 and cov <= hist_n,
         f"sum_n_1m={cov} vs history_n={hist_n}")
    gate("G2_quartile_ordering", orders, "p25<=med<=p75 across all 40 cells")
    gate("G3_current_bucket", (at.get("current") or {}).get("decile") in range(10)
         and (at.get("current") or {}).get("regime_spine") in ("CALM","NORMAL","ELEVATED","STRESS","CRISIS"),
         json.dumps(at.get("current")))
    d9, d2 = (bd.get("9") or {}).get("3m") or {}, (bd.get("2") or {}).get("3m") or {}
    out["finding_contrarian"] = {"decile9_3m": d9, "decile2_3m": d2,
        "read": "peak-stress deciles carry HIGHER median fwd returns — mean-reversion regime, 1990→"}
    gate("G4_contrarian_recorded", d9.get("n", 0) > 500 and d9.get("med") is not None,
         f"d9 3m med={d9.get('med')}% pos={d9.get('pos_pct')}% n={d9.get('n')} vs d2 med={d2.get('med')}%")
    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("\nVERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3378.json").write_text(json.dumps(out, indent=2))
    sys.exit(0)
