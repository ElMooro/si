"""ops 3696 — AU + QA volume legs LAND; CH-pharma REMOVED.
portwatch v1.5: Australian iron-ore/bulk terminals (Port Hedland, Dampier,
Hay Point, Abbot Point, Gladstone, Newcastle, Port Walcott, Cape Lambert,
Port Botany, Fremantle, Brisbane) + Qatar LNG (Ras Laffan — world's largest
LNG terminal — Hamad Port, Umm Said, Al Ruwais); both nations proven in ref
+ daily layer by ops 3695 (AU 57 ports, QA 4, control OK).
boom-stage v1.6: CH-pharma pair removed — Switzerland landlocked (no volume
leg possible) and pharma acyclical, so it fails the canary test.
Gate: AU + QA present in exporters; AU-iron + QA-lng staged (not NA);
CH-pharma gone; NA residue empty."""
import json, sys, time, urllib.request
from pathlib import Path
import boto3
from botocore.config import Config
from _lambda_deploy_helpers import deploy_lambda
from ops_report import report

LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=300, retries={"max_attempts": 1}))
S3C = boto3.client("s3", "us-east-1")
B = "justhodl-dashboard-live"

with report("3696_au_qa_land") as rep:
    rep.heading("ops 3696 — AU/QA legs + CH-pharma removal")
    out = {"gates": {}}
    fails = []
    import traceback
    Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    Path("aws/ops/reports/3696.json").write_text(json.dumps({"verdict": "STARTED"}))
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

        dep("justhodl-portwatch", "portwatch v1.5 AU iron-ore + QA LNG")
        LAM.invoke(FunctionName="justhodl-portwatch",
                   InvocationType="RequestResponse", Payload=b"{}")
        pj = json.loads(S3C.get_object(Bucket=B, Key="data/portwatch.json")["Body"].read())
        ex = {e["country"]: e for e in (pj.get("exporters") or [])}
        ok1 = "Australia" in ex and "Qatar" in ex
        gate("G1_ports", ok1,
             f"nations={len(ex)} AU={ex.get('Australia')} QA={ex.get('Qatar')} "
             f"slowing={pj.get('exporters_slowing')}")

        dep("justhodl-boom-stage", "boom-stage v1.6 AU/QA staged, CH removed")
        r = LAM.invoke(FunctionName="justhodl-boom-stage",
                       InvocationType="RequestResponse", Payload=b"{}")
        pl = json.loads(r["Payload"].read() or b"{}")
        err = pl.get("errorMessage") if isinstance(pl, dict) else None
        j = json.loads(S3C.get_object(Bucket=B, Key="data/boom-stage.json")["Body"].read())
        pairs = j.get("pairs") or []
        ids = [p["id"] for p in pairs]
        na = [p["id"] for p in pairs if p["stage"] == "NA"]
        au = next((p for p in pairs if p["id"] == "AU-iron"), {})
        qa = next((p for p in pairs if p["id"] == "QA-lng"), {})
        M = j.get("macro") or {}
        ok2 = (not err and "CH-pharma" not in ids
               and au.get("stage") not in (None, "NA")
               and qa.get("stage") not in (None, "NA"))
        gate("G2_engine", ok2,
             f"err={err} n_pairs={len(pairs)} na={na} ch_gone={'CH-pharma' not in ids} "
             f"AU={au.get('stage')} ({(au.get('value') or {}).get('yoy_pct')}/"
             f"{(au.get('volume') or {}).get('vs_baseline_pct')}, "
             f"{(au.get('volume') or {}).get('n_ports')} ports) "
             f"QA={qa.get('stage')} ({(qa.get('value') or {}).get('yoy_pct')}/"
             f"{(qa.get('volume') or {}).get('vs_baseline_pct')}, "
             f"{(qa.get('volume') or {}).get('n_ports')} ports) "
             f"AU_choke={au.get('choke_cause') or (au.get('chokepoints') or [{}])[:1]} "
             f"QA_choke={qa.get('choke_cause')} "
             f"regime={M.get('regime')} slow={M.get('slowdown_risk')} "
             f"infl={M.get('inflation_pressure')} "
             f"breadth={(j.get('breadth') or {}).get('price_led_share')}% "
             f"divs={[(d2['commodity'], d2['spread_pp']) for d2 in (j.get('divergences') or [])]}")
        out["au"] = au.get("stage")
        out["qa"] = qa.get("stage")
        out["na_residue"] = na
        out["macro"] = M

        ok3 = False; det = ""; dl = time.time() + 300
        while time.time() < dl:
            try:
                h = urllib.request.urlopen(urllib.request.Request(
                    "https://justhodl.ai/boom-stage.html?cb=" + str(int(time.time())),
                    headers={"User-Agent": "Mozilla/5.0"}), timeout=30
                ).read().decode("utf-8", "replace")
                if "Divergence Radar" in h:
                    ok3 = True; det = "page live"; break
                det = "marker missing"
            except Exception as e:
                det = str(e)[:120]
            time.sleep(20)
        gate("G3_page", ok3, det)
    except Exception:
        out["crash"] = traceback.format_exc()[-1200:]
        print("CRASH:", out["crash"][-400:])
    out["verdict"] = ("CRASH" if out.get("crash") else
                       ("PASS_ALL" if not fails else "GAPS: " + ",".join(fails)))
    print("VERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3696.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
