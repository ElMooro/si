"""ops 3691 — NEW justhodl-trade-nowcast: completes the trade layer.
Sources proven in 3689/3690: BDI (tradingeconomics, 2-pattern agreement),
FRED PPI Deep Sea Freight + Long-Distance Trucking + Import/Export price
indices, CPB World Trade Monitor (sitemap-discovered pages; 3406 locs).
Scheduler 12:50. Wires: freight-pulse.html Trade Rates section + MI.
VALUE-gated (>=3 live series + BDI level)."""
import io, json, sys, time, urllib.request, zipfile
from pathlib import Path
import boto3
from botocore.config import Config
from _lambda_deploy_helpers import deploy_lambda
from ops_report import report

ACCT = "857687956942"
LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=300, retries={"max_attempts": 1}))
SCH = boto3.client("scheduler", "us-east-1")
S3C = boto3.client("s3", "us-east-1")
B = "justhodl-dashboard-live"
ROLE = f"arn:aws:iam::{ACCT}:role/justhodl-scheduler-role"

with report("3691_trade_nowcast") as rep:
    rep.heading("ops 3691 — trade-nowcast engine + wires")
    out = {"gates": {}}
    fails = []
    import traceback
    Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    Path("aws/ops/reports/3691.json").write_text(json.dumps({"verdict": "STARTED"}))
    try:
        def gate(n, ok, d):
            out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:900]}
            print(("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:860]); rep.log(n + " " + str(ok))
            if not ok:
                fails.append(n)

        deploy_lambda(report=rep, function_name="justhodl-trade-nowcast",
                      source_dir=Path(__file__).resolve().parents[2] / "lambdas" / "justhodl-trade-nowcast" / "source",
                      env_vars={}, timeout=180, memory=512,
                      description="trade-nowcast v1.0: BDI + freight PPI + CPB WTM"[:200],
                      create_function_url=False)
        arn = LAM.get_function(FunctionName="justhodl-trade-nowcast")["Configuration"]["FunctionArn"]
        c = dict(Name="justhodl-trade-nowcast-daily",
                 ScheduleExpression="cron(50 12 * * ? *)",
                 FlexibleTimeWindow={"Mode": "OFF"},
                 Target={"Arn": arn, "RoleArn": ROLE, "Input": "{}"},
                 State="ENABLED")
        try:
            SCH.create_schedule(**c)
        except Exception:
            SCH.update_schedule(**c)
        try:
            LAM.add_permission(FunctionName="justhodl-trade-nowcast",
                               StatementId="sched-tradenow",
                               Action="lambda:InvokeFunction",
                               Principal="scheduler.amazonaws.com")
        except LAM.exceptions.ResourceConflictException:
            pass
        r = LAM.invoke(FunctionName="justhodl-trade-nowcast",
                       InvocationType="RequestResponse", Payload=b"{}")
        pl = json.loads(r["Payload"].read() or b"{}")
        err = pl.get("errorMessage") if isinstance(pl, dict) else None
        j = json.loads(S3C.get_object(Bucket=B, Key="data/trade-nowcast.json")["Body"].read())
        ser = j.get("series") or {}
        bdi = j.get("bdi") or {}
        ok1 = (not err and j.get("ok") and len(ser) >= 3
               and isinstance(bdi.get("level"), (int, float))
               and j.get("verdict") in ("RISING", "FALLING", "STABLE"))
        gate("G1_engine", ok1,
             f"err={err} n_series={len(ser)} comp={j.get('rate_pressure')} "
             f"verdict={j.get('verdict')} bdi={bdi} "
             f"cpb={json.dumps(j.get('cpb_wtm'))[:220]} "
             f"series={[(k, v.get('yoy_pct')) for k, v in ser.items()]} "
             f"plain={str(j.get('plain'))[:220]} errs={j.get('errors')}")
        out["nowcast"] = {"comp": j.get("rate_pressure"),
                          "verdict": j.get("verdict"), "bdi": bdi.get("level"),
                          "plain": j.get("plain")}

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
        mi_ok = b"trade_rates" in blob

        ok2 = False; det = ""; dl = time.time() + 420
        while time.time() < dl:
            try:
                h = urllib.request.urlopen(urllib.request.Request(
                    "https://justhodl.ai/freight-pulse.html?cb=" + str(int(time.time())),
                    headers={"User-Agent": "Mozilla/5.0"}), timeout=30
                ).read().decode("utf-8", "replace")
                mk = {"section": "Trade Rates" in h,
                      "fetch": "trade-nowcast.json" in h,
                      "bdi": "BALTIC DRY INDEX" in h,
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
    Path("aws/ops/reports/3691.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
