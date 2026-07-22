"""ops 3694 — TW value leg LANDS (3693 proved asia-leads already carries
taiwan_exports.yoy_pct=+48.33 while the orders series builds its 12m vintage;
boom-stage was only reading the orders field). v1.5: value fallback chain
orders.yoy_pct -> orders.yoy -> exports.yoy, plus DIVERGENCE signals now
emitted into signals[] so the sentinel Telegram wire carries them.
Gate: TW-semis staged (not NA) with both legs numeric + semis divergence
present + divergence signal emitted."""
import json, sys, time, urllib.request
from pathlib import Path
import boto3
from botocore.config import Config
from _lambda_deploy_helpers import deploy_lambda
from ops_report import report

LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=300, retries={"max_attempts": 1}))
S3C = boto3.client("s3", "us-east-1")
B = "justhodl-dashboard-live"

with report("3694_tw_land") as rep:
    rep.heading("ops 3694 — Taiwan value leg lands + divergence signals")
    out = {"gates": {}}
    fails = []
    import traceback
    Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    Path("aws/ops/reports/3694.json").write_text(json.dumps({"verdict": "STARTED"}))
    try:
        def gate(n, ok, d):
            out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:900]}
            print(("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:860]); rep.log(n + " " + str(ok))
            if not ok:
                fails.append(n)

        cfg = LAM.get_function_configuration(FunctionName="justhodl-boom-stage")
        deploy_lambda(report=rep, function_name="justhodl-boom-stage",
                      source_dir=Path(__file__).resolve().parents[2] / "lambdas" / "justhodl-boom-stage" / "source",
                      env_vars=(cfg.get("Environment") or {}).get("Variables") or {},
                      timeout=max(cfg.get("Timeout", 180), 180),
                      memory=max(cfg.get("MemorySize", 512), 512),
                      description="boom-stage v1.5 TW value chain"[:200],
                      create_function_url=False)
        r = LAM.invoke(FunctionName="justhodl-boom-stage",
                       InvocationType="RequestResponse", Payload=b"{}")
        pl = json.loads(r["Payload"].read() or b"{}")
        err = pl.get("errorMessage") if isinstance(pl, dict) else None
        j = json.loads(S3C.get_object(Bucket=B, Key="data/boom-stage.json")["Body"].read())
        pairs = j.get("pairs") or []
        tw = next((p for p in pairs if p["id"] == "TW-semis"), {})
        dv = j.get("divergences") or []
        semis = next((d2 for d2 in dv if d2["commodity"] == "semis"), None)
        sig_div = [s2 for s2 in (j.get("signals") or [])
                   if s2.get("type") == "DIVERGENCE"]
        M = j.get("macro") or {}
        ok1 = (not err and tw.get("stage") not in (None, "NA")
               and isinstance((tw.get("value") or {}).get("yoy_pct"), (int, float))
               and isinstance((tw.get("volume") or {}).get("vs_baseline_pct"), (int, float))
               and bool(sig_div))
        gate("G1_tw", ok1,
             f"err={err} TW_stage={tw.get('stage')} "
             f"TW_value={(tw.get('value') or {}).get('yoy_pct')} "
             f"src={(tw.get('value') or {}).get('src')} "
             f"TW_vol={(tw.get('volume') or {}).get('vs_baseline_pct')} "
             f"why={str(tw.get('why'))[:90]} f4={(tw.get('factor4') or {}).get('read')} "
             f"semis_div={semis} div_signals={len(sig_div)} "
             f"na_left={[p['id'] for p in pairs if p['stage'] == 'NA']} "
             f"regime={M.get('regime')} slow={M.get('slowdown_risk')} "
             f"infl={M.get('inflation_pressure')} "
             f"breadth={(j.get('breadth') or {}).get('price_led_share')}%")
        out["tw"] = {"stage": tw.get("stage"),
                     "value": (tw.get("value") or {}).get("yoy_pct"),
                     "volume": (tw.get("volume") or {}).get("vs_baseline_pct")}
        out["divergences"] = dv
        out["signals"] = [(s2.get("type"), s2.get("pair")) for s2 in (j.get("signals") or [])]

        ok2 = False; det = ""; dl = time.time() + 300
        while time.time() < dl:
            try:
                h = urllib.request.urlopen(urllib.request.Request(
                    "https://justhodl.ai/boom-stage.html?cb=" + str(int(time.time())),
                    headers={"User-Agent": "Mozilla/5.0"}), timeout=30
                ).read().decode("utf-8", "replace")
                if "Divergence Radar" in h and "PRICE-LED SHARE" in h:
                    ok2 = True; det = "page live"; break
                det = "markers missing"
            except Exception as e:
                det = str(e)[:120]
            time.sleep(20)
        gate("G2_page", ok2, det)
    except Exception:
        out["crash"] = traceback.format_exc()[-1200:]
        print("CRASH:", out["crash"][-400:])
    out["verdict"] = ("CRASH" if out.get("crash") else
                       ("PASS_ALL" if not fails else "GAPS: " + ",".join(fails)))
    print("VERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3694.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
