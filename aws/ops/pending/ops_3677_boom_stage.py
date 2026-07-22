"""ops 3677 — NEW justhodl-boom-stage (Khalid: use port activity to prove an
industry boom is early vs plateauing). Value-vs-volume staging across
KR-semis / TW-electronics / CN-broad from existing feeds. Deploy + Scheduler
12:30 + invoke (RequestResponse fine: S3-only, no external egress) + gates:
KR-semis == EARLY_PRICE_LED with both legs numeric; page section + live
chain-read served; MI zip."""
import io, json, sys, time, urllib.request, zipfile
from pathlib import Path
import boto3
from botocore.config import Config
from _lambda_deploy_helpers import deploy_lambda
from ops_report import report

ACCT = "857687956942"
LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=120, retries={"max_attempts": 1}))
SCH = boto3.client("scheduler", "us-east-1")
S3C = boto3.client("s3", "us-east-1")
B = "justhodl-dashboard-live"
ROLE = f"arn:aws:iam::{ACCT}:role/justhodl-scheduler-role"

with report("3677_boom_stage") as rep:
    rep.heading("ops 3677 — boom-stage engine + wires")
    out = {"gates": {}}
    fails = []
    import traceback
    Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    Path("aws/ops/reports/3677.json").write_text(json.dumps({"verdict": "STARTED"}))
    try:
        def gate(n, ok, d):
            out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:860]}
            print(("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:820]); rep.log(n + " " + str(ok))
            if not ok:
                fails.append(n)

        deploy_lambda(report=rep, function_name="justhodl-boom-stage",
                      source_dir=Path(__file__).resolve().parents[2] / "lambdas" / "justhodl-boom-stage" / "source",
                      env_vars={}, timeout=60, memory=256,
                      description="boom-stage v1.0: value-vs-volume boom lifecycle"[:200],
                      create_function_url=False)
        arn = LAM.get_function(FunctionName="justhodl-boom-stage")["Configuration"]["FunctionArn"]
        c = dict(Name="justhodl-boom-stage-daily",
                 ScheduleExpression="cron(30 12 * * ? *)",
                 FlexibleTimeWindow={"Mode": "OFF"},
                 Target={"Arn": arn, "RoleArn": ROLE, "Input": "{}"},
                 State="ENABLED")
        try:
            SCH.create_schedule(**c)
        except Exception:
            SCH.update_schedule(**c)
        try:
            LAM.add_permission(FunctionName="justhodl-boom-stage",
                               StatementId="sched-boomstage",
                               Action="lambda:InvokeFunction",
                               Principal="scheduler.amazonaws.com")
        except LAM.exceptions.ResourceConflictException:
            pass
        r = LAM.invoke(FunctionName="justhodl-boom-stage",
                       InvocationType="RequestResponse", Payload=b"{}")
        pl = json.loads(r["Payload"].read() or b"{}")
        err = pl.get("errorMessage") if isinstance(pl, dict) else None
        j = json.loads(S3C.get_object(Bucket=B, Key="data/boom-stage.json")["Body"].read())
        kr = next((p for p in (j.get("pairs") or []) if p["id"] == "KR-semis"), {})
        ok1 = (not err and j.get("ok")
               and kr.get("stage") == "EARLY_PRICE_LED"
               and isinstance((kr.get("value") or {}).get("yoy_pct"), (int, float))
               and isinstance((kr.get("volume") or {}).get("vs_baseline_pct"), (int, float)))
        gate("G1_engine", ok1,
             f"err={err} ok={j.get('ok')} headline={j.get('headline')} "
             f"stages={[(p['id'], p['stage'], (p.get('value') or {}).get('yoy_pct'), (p.get('volume') or {}).get('vs_baseline_pct')) for p in j.get('pairs') or []]}")
        out["stages"] = j.get("pairs")

        # MI zip
        cfg2 = LAM.get_function_configuration(FunctionName="justhodl-morning-intelligence")
        deploy_lambda(report=rep, function_name="justhodl-morning-intelligence",
                      source_dir=Path(__file__).resolve().parents[2] / "lambdas" / "justhodl-morning-intelligence" / "source",
                      env_vars=(cfg2.get("Environment") or {}).get("Variables") or {},
                      timeout=cfg2.get("Timeout", 300), memory=cfg2.get("MemorySize", 1024),
                      description=(cfg2.get("Description") or "MI")[:200],
                      create_function_url=False)
        loc = LAM.get_function(FunctionName="justhodl-morning-intelligence")["Code"]["Location"]
        zf = zipfile.ZipFile(io.BytesIO(urllib.request.urlopen(loc, timeout=60).read()))
        blob = b"".join(zf.read(n) for n in zf.namelist() if n.endswith(".py"))
        mi_ok = b"boom_stage" in blob

        ok2 = False; det = ""; dl = time.time() + 480
        while time.time() < dl:
            try:
                h = urllib.request.urlopen(urllib.request.Request(
                    "https://justhodl.ai/freight-pulse.html?cb=" + str(int(time.time())),
                    headers={"User-Agent": "Mozilla/5.0"}), timeout=30
                ).read().decode("utf-8", "replace")
                mk = {"section": "Boom-Stage Lens" in h,
                      "fetch": "boom-stage.json" in h,
                      "chain_live": "exporters_slowing.join" in h,
                      "mi": mi_ok}
                det = str(mk)
                if all(mk.values()):
                    ok2 = True; break
            except Exception as e:
                det = str(e)[:140]
            time.sleep(20)
        gate("G2_wires", ok2, det)
    except Exception:
        out["crash"] = traceback.format_exc()[-1200:]
        print("CRASH:", out["crash"][-400:])
    out["verdict"] = ("CRASH" if out.get("crash") else
                       ("PASS_ALL" if not fails else "GAPS: " + ",".join(fails)))
    print("VERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3677.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
