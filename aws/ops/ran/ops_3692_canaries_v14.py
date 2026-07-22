"""ops 3692 — boom-stage v1.4: page-audit fixes + 4 NEW canaries.
FIXES: (1) doctrine string synced to real thresholds incl SUPPLY_SHOCK_PRICING
(2) dynamic canary reclass — a GROWTH pair printing SUPPLY_SHOCK behaves as
INFLATION (Chile: mining util RAMPING yet volume -29.7% = export bottleneck)
(3) commodity-specific 4th factors for AU/QA/DE/CH (4) NA pairs collapsed.
NEW CANARIES: [A] same-commodity DIVERGENCE (CL vs PE copper spread = local
supply disruption, highest-conviction read) [B] price-led vs volume-led
BREADTH (one number: real demand or just price?) [C] stage PERSISTENCE
(stage_days + LATE-EARLY warning >=30d) [D] CHOKEPOINT attribution (Hormuz
-78% explains UAE/SA; causal not correlational)."""
import json, sys, time, urllib.request
from pathlib import Path
import boto3
from botocore.config import Config
from _lambda_deploy_helpers import deploy_lambda
from ops_report import report

LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=300, retries={"max_attempts": 1}))
S3C = boto3.client("s3", "us-east-1")
B = "justhodl-dashboard-live"

with report("3692_canaries_v14") as rep:
    rep.heading("ops 3692 — divergence/breadth/persistence/chokepoint")
    out = {"gates": {}}
    fails = []
    import traceback
    Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    Path("aws/ops/reports/3692.json").write_text(json.dumps({"verdict": "STARTED"}))
    try:
        def gate(n, ok, d):
            out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:900]}
            print(("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:860]); rep.log(n + " " + str(ok))
            if not ok:
                fails.append(n)

        # chokepoint inventory (for CHOKE_MAP tuning next round)
        pw = json.loads(S3C.get_object(Bucket=B, Key="data/portwatch.json")["Body"].read())
        out["chokepoint_names"] = [c.get("name") for c in (pw.get("chokepoints") or [])]

        cfg = LAM.get_function_configuration(FunctionName="justhodl-boom-stage")
        deploy_lambda(report=rep, function_name="justhodl-boom-stage",
                      source_dir=Path(__file__).resolve().parents[2] / "lambdas" / "justhodl-boom-stage" / "source",
                      env_vars=(cfg.get("Environment") or {}).get("Variables") or {},
                      timeout=max(cfg.get("Timeout", 120), 180),
                      memory=max(cfg.get("MemorySize", 256), 512),
                      description="boom-stage v1.4 divergence+breadth+persistence"[:200],
                      create_function_url=False)
        r = LAM.invoke(FunctionName="justhodl-boom-stage",
                       InvocationType="RequestResponse", Payload=b"{}")
        pl = json.loads(r["Payload"].read() or b"{}")
        err = pl.get("errorMessage") if isinstance(pl, dict) else None
        j = json.loads(S3C.get_object(Bucket=B, Key="data/boom-stage.json")["Body"].read())
        pairs = j.get("pairs") or []
        bw = j.get("breadth") or {}
        dv = j.get("divergences") or []
        recl = [p["id"] for p in pairs if p.get("reclassified")]
        chk = [(p["id"], p.get("choke_cause")) for p in pairs if p.get("choke_cause")]
        ok1 = (not err and j.get("ok")
               and isinstance(bw.get("price_led_share"), (int, float))
               and "SUPPLY_SHOCK_PRICING v>=10" in (j.get("method") or "")
               and all("stage_days" in p for p in pairs))
        gate("G1_engine", ok1,
             f"err={err} pairs={len(pairs)} breadth={bw.get('price_led_share')}% "
             f"'{bw.get('read')}' divergences={[(d2['commodity'], d2['spread_pp']) for d2 in dv]} "
             f"reclassified={recl} choke_causes={chk[:3]} "
             f"regime={(j.get('macro') or {}).get('regime')} "
             f"slow={(j.get('macro') or {}).get('slowdown_risk')} "
             f"infl={(j.get('macro') or {}).get('inflation_pressure')}")
        out["divergences"] = dv
        out["breadth"] = bw
        out["reclassified"] = recl

        ok2 = False; det = ""; dl = time.time() + 420
        while time.time() < dl:
            try:
                h = urllib.request.urlopen(urllib.request.Request(
                    "https://justhodl.ai/boom-stage.html?cb=" + str(int(time.time())),
                    headers={"User-Agent": "Mozilla/5.0"}), timeout=30
                ).read().decode("utf-8", "replace")
                mk = {"diverge": "Divergence Radar" in h,
                      "breadth": "PRICE-LED SHARE" in h,
                      "choke": "choke_cause" in h,
                      "maturity": "p.maturity" in h,
                      "na_collapse": "AWAITING DATA" in h}
                det = str(mk)
                if all(mk.values()):
                    ok2 = True; break
            except Exception as e:
                det = str(e)[:140]
            time.sleep(20)
        gate("G2_page", ok2, det)
    except Exception:
        out["crash"] = traceback.format_exc()[-1200:]
        print("CRASH:", out["crash"][-400:])
    out["verdict"] = ("CRASH" if out.get("crash") else
                       ("PASS_ALL" if not fails else "GAPS: " + ",".join(fails)))
    print("VERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3692.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
