"""ops 3680 — GLOBAL CANARY MATRIX (Khalid: Chile/Peru copper, Finland pulp,
Saudi/UAE oil, Taiwan semis; engine must SAY whether it means slowdown or
inflation, in plain English). portwatch v1.3.4 (+CL/PE/FI/SA ports+nations),
boom-stage v1.2 (9 pairs, SUPPLY_SHOCK_PRICING stage, GROWTH/INFLATION canary
tags, macro dials + plain-English verdict), page v3 (regime hero + two dials +
split boards)."""
import json, sys, time, urllib.request
from pathlib import Path
import boto3
from botocore.config import Config
from _lambda_deploy_helpers import deploy_lambda
from ops_report import report

LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=300, retries={"max_attempts": 1}))
S3C = boto3.client("s3", "us-east-1")
B = "justhodl-dashboard-live"

with report("3680_canary_matrix") as rep:
    rep.heading("ops 3680 — global canary matrix")
    out = {"gates": {}}
    fails = []
    import traceback
    Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    Path("aws/ops/reports/3680.json").write_text(json.dumps({"verdict": "STARTED"}))
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
                          timeout=cfg.get("Timeout", 300), memory=cfg.get("MemorySize", 512),
                          description=desc[:200], create_function_url=False)

        # [A] portwatch v1.3.4 — canary nations
        dep("justhodl-portwatch", "portwatch v1.3.4 +CL/PE/FI/SA")
        LAM.invoke(FunctionName="justhodl-portwatch",
                   InvocationType="RequestResponse", Payload=b"{}")
        pj = json.loads(S3C.get_object(Bucket=B, Key="data/portwatch.json")["Body"].read())
        ex = {e["country"]: e for e in (pj.get("exporters") or [])}
        want = ["Chile", "Peru", "Finland", "Saudi Arabia", "Taiwan"]
        have = [w for w in want if w in ex]
        gate("G1_ports", len(have) >= 3,
             f"nations={len(ex)} new_have={have} "
             f"rows={[(w, ex[w]['n_ports'], ex[w]['avg_vs_baseline_pct'], ex[w]['verdict']) for w in have]} "
             f"ref_search={pj.get('ref_search')}")
        out["new_nations"] = have

        # [B] boom-stage v1.2
        dep("justhodl-boom-stage", "boom-stage v1.2 canary matrix")
        r = LAM.invoke(FunctionName="justhodl-boom-stage",
                       InvocationType="RequestResponse", Payload=b"{}")
        pl = json.loads(r["Payload"].read() or b"{}")
        err = pl.get("errorMessage") if isinstance(pl, dict) else None
        j = json.loads(S3C.get_object(Bucket=B, Key="data/boom-stage.json")["Body"].read())
        M = j.get("macro") or {}
        pairs = j.get("pairs") or []
        ok2 = (not err and j.get("ok") and len(pairs) >= 9
               and isinstance(M.get("slowdown_risk"), (int, float))
               and isinstance(M.get("inflation_pressure"), (int, float))
               and M.get("regime") and len(M.get("plain_english") or "") > 60
               and all(p.get("canary") for p in pairs))
        gate("G2_engine", ok2,
             f"err={err} n_pairs={len(pairs)} regime={M.get('regime')} "
             f"slow={M.get('slowdown_risk')}({M.get('slowdown_band')}) "
             f"infl={M.get('inflation_pressure')}({M.get('inflation_band')}) "
             f"stages={[(p['id'], p['stage'], (p.get('value') or {}).get('yoy_pct'), (p.get('volume') or {}).get('vs_baseline_pct')) for p in pairs]} "
             f"say={str(M.get('plain_english'))[:300]}")
        out["macro"] = M
        out["pairs"] = [(p["id"], p["stage"], (p.get("canary") or {}).get("type")) for p in pairs]

        # [C] page v3 served
        ok3 = False; det = ""; dl = time.time() + 480
        while time.time() < dl:
            try:
                h = urllib.request.urlopen(urllib.request.Request(
                    "https://justhodl.ai/boom-stage.html?cb=" + str(int(time.time())),
                    headers={"User-Agent": "Mozilla/5.0"}), timeout=30
                ).read().decode("utf-8", "replace")
                mk = {"regime": "GLOBAL REGIME" in h,
                      "dials": "GLOBAL SLOWDOWN RISK" in h and "INFLATION PRESSURE" in h,
                      "plain": "plain_english" in h,
                      "split": "GROWTH CANARIES" in h and "INFLATION CANARIES" in h,
                      "shock": "SUPPLY_SHOCK_PRICING" in h}
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
    Path("aws/ops/reports/3680.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
