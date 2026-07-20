"""ops 3599 — barometer v2 visual: engine v1.1 emits 180-session rolling
z-history (fi/fx/eq/spill); page ships gauge + migration-flow map + spillover
ribbon. Gates: history real + consistent with headline; card served with all
three visual markers; render-truth on the ribbon math (H length, last spill)."""
import json, sys, time, urllib.request
from pathlib import Path
import boto3
from botocore.config import Config
from _lambda_deploy_helpers import deploy_lambda
from ops_report import report

ROOT = Path(__file__).resolve().parents[2]
LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=300, retries={"max_attempts": 0}))
S3C = boto3.client("s3", "us-east-1")
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-fifx-vol-migration"

with report("3599_fifx_visual") as rep:
    rep.heading("ops 3599 — vol barometer v2 (gauge + flow + ribbon)")
    out = {"gates": {}}
    fails = []

    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:480]}
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:440]
        print(line); rep.log(line)
        if not ok:
            fails.append(n)

    try:
        env = (LAM.get_function_configuration(FunctionName=FN)
               .get("Environment") or {}).get("Variables") or {}
        deploy_lambda(report=rep, function_name=FN,
                      source_dir=ROOT / "lambdas" / "justhodl-fifx-vol-migration" / "source",
                      env_vars=env, timeout=180, memory=512,
                      description="Vol migration barometer v1.1: legs + spillover + 180-session rolling z-history for the gauge/flow/ribbon visuals.",
                      create_function_url=False)
        r = LAM.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        pl = json.loads(r["Payload"].read() or b"{}")
        if isinstance(pl, dict) and pl.get("errorMessage"):
            gate("G1_history_real", False, "fn error: " + pl["errorMessage"][:240])
        else:
            j = json.loads(S3C.get_object(Bucket=BUCKET, Key="data/fifx-vol.json")["Body"].read())
            H = j.get("history") or []
            m = j.get("migration") or {}
            last = H[-1] if H else {}
            ok1 = (j.get("version") == "1.1.0" and len(H) >= 120
                   and all(isinstance(last.get(k), (int, float)) for k in ("eq", "spill"))
                   and isinstance(m.get("spillover"), (int, float))
                   and abs(last["spill"] - m["spillover"]) <= 0.35)
            gate("G1_history_real", ok1,
                 f"v{j.get('version')} hist={len(H)} first={H[0]['d'] if H else None} "
                 f"last={last.get('d')} spill_last={last.get('spill')} vs headline {m.get('spillover')} "
                 f"state={m.get('state')} fi_z={last.get('fi')} fx_z={last.get('fx')} eq_z={last.get('eq')}")
            out["hist_sample"] = H[-3:]
    except Exception as e:
        gate("G1_history_real", False, str(e)[:320])

    ok2 = False; dl = time.time() + 330
    while time.time() < dl:
        try:
            with urllib.request.urlopen(urllib.request.Request(
                    "https://justhodl.ai/signal-board.html",
                    headers={"User-Agent": "Mozilla/5.0 (ops)"}), timeout=30) as r:
                html = r.read().decode("utf-8", "replace")
            if all(k in html for k in ("MIGRATION GAUGE", "THE MIGRATION MAP",
                                       "SPILLOVER · LAST", "jhflow")):
                ok2 = True; break
        except Exception:
            pass
        time.sleep(15)
    gate("G2_card_served", ok2, "served: gauge + migration map + ribbon + flow animation")

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("\nVERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3599.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
