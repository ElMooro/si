"""ops 3682 — 4-FACTOR + 5 NEW PAIRS + GAP FIXES. portwatch v1.4 (Taiwan
alias 'Taiwan Province of China' ordered BEFORE china substring; real Chile
port names from 3681 ref probe: Antofagasta/Valparaiso/San Vicente/Coronel/
Iquique/Quintero + Keelung/Taichung/Mailiao). boom-stage v1.3: 4th factor
(FRED utilization + inventories/sales -> RAMPING/IDLING/INVENTORY_BUILD/DRAW
with _refine() confirm-or-contradict notes + inventory tilt on the slowdown
dial) and 5 new pairs (AU-iron, ID-nickel, QA-lng, DE-machinery, CH-pharma
incl new DEFENSIVE canary class). Page v4: factor-4 lines + defensive board."""
import json, sys, time, urllib.request
from pathlib import Path
import boto3
from botocore.config import Config
from _lambda_deploy_helpers import deploy_lambda
from ops_report import report

LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=300, retries={"max_attempts": 1}))
S3C = boto3.client("s3", "us-east-1")
B = "justhodl-dashboard-live"

with report("3682_four_factor") as rep:
    rep.heading("ops 3682 — 4-factor + new pairs + TW/CL fixes")
    out = {"gates": {}}
    fails = []
    import traceback
    Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    Path("aws/ops/reports/3682.json").write_text(json.dumps({"verdict": "STARTED"}))
    try:
        def gate(n, ok, d):
            out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:900]}
            print(("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:860]); rep.log(n + " " + str(ok))
            if not ok:
                fails.append(n)

        def dep(fn, desc):
            cfg = LAM.get_function_configuration(FunctionName=fn)
            deploy_lambda(report=rep, function_name=fn,
                          source_dir=Path(__file__).resolve().parents[2] / "lambdas" / fn / "source",
                          env_vars=(cfg.get("Environment") or {}).get("Variables") or {},
                          timeout=max(cfg.get("Timeout", 300), 300),
                          memory=max(cfg.get("MemorySize", 512), 512),
                          description=desc[:200], create_function_url=False)

        # [A] portwatch v1.4 — TW + CL
        dep("justhodl-portwatch", "portwatch v1.4 TW alias + Chile ports")
        LAM.invoke(FunctionName="justhodl-portwatch",
                   InvocationType="RequestResponse", Payload=b"{}")
        pj = json.loads(S3C.get_object(Bucket=B, Key="data/portwatch.json")["Body"].read())
        ex = {e["country"]: e for e in (pj.get("exporters") or [])}
        ok1 = "Taiwan" in ex and "Chile" in ex and "China" in ex
        gate("G1_ports", ok1,
             f"nations={len(ex)} TW={ex.get('Taiwan')} CL={ex.get('Chile')} "
             f"CN_ports={(ex.get('China') or {}).get('n_ports')} "
             f"slowing={pj.get('exporters_slowing')}")

        # [B] boom-stage v1.3
        dep("justhodl-boom-stage", "boom-stage v1.3 4-factor + 5 pairs")
        r = LAM.invoke(FunctionName="justhodl-boom-stage",
                       InvocationType="RequestResponse", Payload=b"{}")
        pl = json.loads(r["Payload"].read() or b"{}")
        err = pl.get("errorMessage") if isinstance(pl, dict) else None
        j = json.loads(S3C.get_object(Bucket=B, Key="data/boom-stage.json")["Body"].read())
        pairs = j.get("pairs") or []
        M = j.get("macro") or {}
        f4n = sum(1 for p in pairs if p.get("factor4"))
        newp = [p["id"] for p in pairs if p["id"] in
                ("AU-iron", "ID-nickel", "QA-lng", "DE-machinery", "CH-pharma")]
        ok2 = (not err and j.get("ok") and len(pairs) >= 14 and f4n >= 6
               and len(newp) == 5 and M.get("regime"))
        gate("G2_engine", ok2,
             f"err={err} n_pairs={len(pairs)} factor4_on={f4n} new={newp} "
             f"regime={M.get('regime')} slow={M.get('slowdown_risk')} "
             f"infl={M.get('inflation_pressure')} tilt={M.get('inventory_tilt')} "
             f"f4reads={M.get('factor4_reads')} "
             f"stages={[(p['id'], p['stage'], (p.get('value') or {}).get('yoy_pct'), (p.get('volume') or {}).get('vs_baseline_pct')) for p in pairs]}")
        out["macro"] = M
        out["notes"] = {p["id"]: p.get("factor4_note")
                        for p in pairs if p.get("factor4_note")}

        # [C] page v4
        ok3 = False; det = ""; dl = time.time() + 420
        while time.time() < dl:
            try:
                h = urllib.request.urlopen(urllib.request.Request(
                    "https://justhodl.ai/boom-stage.html?cb=" + str(int(time.time())),
                    headers={"User-Agent": "Mozilla/5.0"}), timeout=30
                ).read().decode("utf-8", "replace")
                mk = {"f4": "4th factor" in h,
                      "defensive": "DEFENSIVE" in h,
                      "tilt": "inventory/utilization tilt" in h}
                det = str(mk)
                if all(mk.values()):
                    ok3 = True; break
            except Exception as e:
                det = str(e)[:140]
            time.sleep(20)
        gate("G3_page", ok3, det)
    except Exception:
        out["crash"] = traceback.format_exc()[-1200:]
        print("CRASH:", out["crash"][-400:])
    out["verdict"] = ("CRASH" if out.get("crash") else
                       ("PASS_ALL" if not fails else "GAPS: " + ",".join(fails)))
    print("VERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3682.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
