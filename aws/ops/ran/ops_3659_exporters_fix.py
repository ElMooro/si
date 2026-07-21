"""ops 3659 — FREIGHT+EXPORTERS CANARIES (Khalid directive): [A] NEW
justhodl-freight-pulse (FRED: DOT TSI, Cass shipments/expenditures, truck
tonnage, rail carloads/intermodal -> composite ACCEL/STABLE/DECEL + inflection
flags; Scheduler daily 11:50). [B] portwatch v1.2 exporters pulse (35 majors,
country aggregation, SLOWING/ACCELERATING verdicts). [C] serves: portwatch
page Export-Nations board + macro-leads US-Freight + Exporters rows; MI feed.
Full-body traceback capture."""
import json, sys, time, urllib.request
from pathlib import Path
import boto3
from botocore.config import Config
from _lambda_deploy_helpers import deploy_lambda
from ops_report import report

ACCT = "857687956942"
LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=300, retries={"max_attempts": 0}))
SCH = boto3.client("scheduler", "us-east-1")
S3C = boto3.client("s3", "us-east-1")
B = "justhodl-dashboard-live"
ROLE = f"arn:aws:iam::{ACCT}:role/justhodl-scheduler-role"

with report("3659_exporters_fix") as rep:
    rep.heading("ops 3659 — freight pulse + exporters pulse")
    out = {"gates": {}}
    fails = []
    import traceback
    Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    Path("aws/ops/reports/3659.json").write_text(json.dumps({"verdict": "STARTED"}))
    try:
        def gate(n, ok, d):
            out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:660]}
            print(("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:620]); rep.log(n + " " + str(ok))
            if not ok:
                fails.append(n)

        def dep(fn, tmo, mem, desc, env=None):
            try:
                cfg = LAM.get_function_configuration(FunctionName=fn)
                env = env if env is not None else ((cfg.get("Environment") or {}).get("Variables") or {})
                tmo = max(tmo, cfg.get("Timeout", 120)); mem = max(mem, cfg.get("MemorySize", 256))
            except Exception:
                env = env or {}
            deploy_lambda(report=rep, function_name=fn,
                          source_dir=Path(__file__).resolve().parents[2] / "lambdas" / fn / "source",
                          env_vars=env, timeout=tmo, memory=mem,
                          description=desc[:200], create_function_url=False)

        # [A] freight-pulse
        dep("justhodl-freight-pulse", 120, 512,
            "freight-pulse v1.0: US freight canary composite (FRED)", env={})
        arn = LAM.get_function(FunctionName="justhodl-freight-pulse")["Configuration"]["FunctionArn"]
        cfgs = dict(Name="justhodl-freight-pulse-daily",
                    ScheduleExpression="cron(50 11 * * ? *)",
                    FlexibleTimeWindow={"Mode": "OFF"},
                    Target={"Arn": arn, "RoleArn": ROLE, "Input": "{}"},
                    State="ENABLED")
        try:
            SCH.create_schedule(**cfgs)
        except Exception:
            SCH.update_schedule(**cfgs)
        try:
            LAM.add_permission(FunctionName="justhodl-freight-pulse",
                               StatementId="sched-freight",
                               Action="lambda:InvokeFunction",
                               Principal="scheduler.amazonaws.com")
        except LAM.exceptions.ResourceConflictException:
            pass
        r = LAM.invoke(FunctionName="justhodl-freight-pulse",
                       InvocationType="RequestResponse", Payload=b"{}")
        pl = json.loads(r["Payload"].read() or b"{}")
        err = pl.get("errorMessage") if isinstance(pl, dict) else None
        fj = json.loads(S3C.get_object(Bucket=B, Key="data/freight-pulse.json")["Body"].read())
        ok1 = (not err and fj.get("ok") and fj.get("n_live", 0) >= 4
               and fj.get("verdict") in ("ACCELERATING", "STABLE", "DECELERATING"))
        gate("G1_freight", ok1,
             f"err={err} live={fj.get('n_live')} comp={fj.get('composite')} "
             f"verdict={fj.get('verdict')} infl={fj.get('inflections')} "
             f"sample={[(k, v.get('yoy_pct'), v.get('m6_ann_pct')) for k, v in list((fj.get('series') or {}).items())[:4] if v.get('ok')]} "
             f"errs={fj.get('errors')}")
        out["freight"] = {"composite": fj.get("composite"), "verdict": fj.get("verdict"),
                           "inflections": fj.get("inflections")}

        # [B] portwatch v1.2 exporters
        dep("justhodl-portwatch", 300, 768, "portwatch v1.2 + exporters pulse")
        r = LAM.invoke(FunctionName="justhodl-portwatch",
                       InvocationType="RequestResponse", Payload=b"{}")
        pl = json.loads(r["Payload"].read() or b"{}")
        err = pl.get("errorMessage") if isinstance(pl, dict) else None
        pj = json.loads(S3C.get_object(Bucket=B, Key="data/portwatch.json")["Body"].read())
        ex = pj.get("exporters") or []
        ok2 = (not err and len(ex) >= 8
               and any(e["code"] == "CHN" for e in ex)
               and all(isinstance(e.get("avg_vs_baseline_pct"), (int, float)) for e in ex))
        gate("G2_exporters", ok2,
             f"err={err} ports_n={len(pj.get('ports') or [])} exporters_n={len(ex)} "
             f"slowing={pj.get('exporters_slowing')} "
             f"board={[(e['country'], e['n_ports'], e['avg_vs_baseline_pct'], e['verdict']) for e in ex[:8]]}")
        out["exporters"] = ex[:12]

        # [C] serves + MI
        dep("justhodl-morning-intelligence", 300, 1024, "MI + freight feed")
        import io, zipfile
        loc = LAM.get_function(FunctionName="justhodl-morning-intelligence")["Code"]["Location"]
        zf = zipfile.ZipFile(io.BytesIO(urllib.request.urlopen(loc, timeout=60).read()))
        blob = b"".join(zf.read(n) for n in zf.namelist() if n.endswith(".py"))
        mi_ok = b"freight-pulse" in blob
        ok3 = False; det3 = ""; dl = time.time() + 480
        while time.time() < dl:
            try:
                def get(u):
                    return urllib.request.urlopen(urllib.request.Request(
                        u + "?cb=" + str(int(time.time())),
                        headers={"User-Agent": "Mozilla/5.0"}), timeout=30
                    ).read().decode("utf-8", "replace")
                h1 = get("https://justhodl.ai/portwatch.html")
                h2 = get("https://justhodl.ai/macro-leads.html")
                mk = {"exp_board": "Export Nations Pulse" in h1,
                      "freight_row": "US Freight" in h2,
                      "exp_row": "Exporters" in h2,
                      "fp_fetch": "freight-pulse.json" in h2,
                      "mi_zip": mi_ok}
                det3 = str(mk)
                if all(mk.values()):
                    ok3 = True; break
            except Exception as e:
                det3 = str(e)[:140]
            time.sleep(20)
        gate("G3_serves", ok3, det3)
    except Exception:
        out["crash"] = traceback.format_exc()[-1200:]
        print("CRASH:", out["crash"][-400:])
    out["verdict"] = ("CRASH" if out.get("crash") else
                       ("PASS_ALL" if not fails else "GAPS: " + ",".join(fails)))
    print("VERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3659.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
