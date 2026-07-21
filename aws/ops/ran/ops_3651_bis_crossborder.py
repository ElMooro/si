"""ops 3651 — NEW justhodl-bis-crossborder (BIS WS_CBS_PUB foreign claims,
eurodollar extension; key shape reused as FACT from World Monitor's
documented lesson: LBS_D_PUB has no per-counterparty on public API, CBS
does). Creates fn + weekly Scheduler Mon 09:40 UTC (quarterly data), invokes,
gates sane totals + counterparties + offshore/EM-Asia aggregates; wires
eurodollar-plumbing payload block (deploy+invoke) + MI zip."""
import io, json, sys, urllib.request, zipfile
from pathlib import Path
import boto3
from botocore.config import Config
from _lambda_deploy_helpers import deploy_lambda
from ops_report import report

ROOT = Path(__file__).resolve().parents[2]
ACCT = "857687956942"
LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=600, retries={"max_attempts": 0}))
SCH = boto3.client("scheduler", "us-east-1")
S3C = boto3.client("s3", "us-east-1")
B = "justhodl-dashboard-live"
ROLE = f"arn:aws:iam::{ACCT}:role/justhodl-scheduler-role"

with report("3651_bis_crossborder") as rep:
    rep.heading("ops 3651 — BIS cross-border claims engine + plumbing wire")
    out = {"gates": {}}
    fails = []

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
                      source_dir=ROOT / "lambdas" / fn / "source",
                      env_vars=env, timeout=tmo, memory=mem,
                      description=desc[:200], create_function_url=False)

    try:
        dep("justhodl-bis-crossborder", 180, 512,
            "bis-crossborder v1.0: CBS foreign claims by counterparty + offshore/EM-Asia aggregates", env={})
        arn = LAM.get_function(FunctionName="justhodl-bis-crossborder")["Configuration"]["FunctionArn"]
        cfg = dict(Name="justhodl-bis-crossborder-weekly",
                   ScheduleExpression="cron(40 9 ? * MON *)",
                   FlexibleTimeWindow={"Mode": "OFF"},
                   Target={"Arn": arn, "RoleArn": ROLE, "Input": "{}"},
                   State="ENABLED")
        try:
            SCH.create_schedule(**cfg)
        except Exception:
            SCH.update_schedule(**cfg)
        try:
            LAM.add_permission(FunctionName="justhodl-bis-crossborder",
                               StatementId="sched-bis-cb",
                               Action="lambda:InvokeFunction",
                               Principal="scheduler.amazonaws.com")
        except LAM.exceptions.ResourceConflictException:
            pass
        r = LAM.invoke(FunctionName="justhodl-bis-crossborder",
                       InvocationType="RequestResponse", Payload=b"{}")
        pl = json.loads(r["Payload"].read() or b"{}")
        err = pl.get("errorMessage") if isinstance(pl, dict) else None
        j = json.loads(S3C.get_object(Bucket=B, Key="data/bis-crossborder.json")["Body"].read())
        tot = j.get("total") or {}
        bc = j.get("by_counterparty") or []
        ok1 = (not err and j.get("ok")
               and isinstance(tot.get("latest_tn"), (int, float))
               and 10 <= tot["latest_tn"] <= 80
               and len(bc) >= 6
               and sum(1 for r0 in bc if isinstance(r0.get("yoy_pct"), (int, float))) >= 5)
        gate("G1_bis", ok1,
             f"err={err} cps={j.get('counterparties_in_response')} "
             f"TOTAL={tot.get('latest_tn')}tn {tot.get('period')} yoy={tot.get('yoy_pct')}% "
             f"offshore={j.get('offshore_centres')} em_asia={j.get('em_asia')} "
             f"top={[(r0['code'], r0['latest_bn'], r0['yoy_pct']) for r0 in bc[:6]]} "
             f"errors={j.get('errors')}")
        out["snapshot"] = {"total": tot, "offshore": j.get("offshore_centres"),
                            "em_asia": j.get("em_asia"),
                            "china": next((r0 for r0 in bc if r0.get('code') == 'CN'), None)}
    except Exception as e:
        gate("G1_bis", False, str(e)[:380])

    try:
        dep("justhodl-eurodollar-plumbing", 300, 1024, "plumbing + bis_crossborder block")
        r = LAM.invoke(FunctionName="justhodl-eurodollar-plumbing",
                       InvocationType="RequestResponse", Payload=b"{}")
        pl = json.loads(r["Payload"].read() or b"{}")
        err = pl.get("errorMessage") if isinstance(pl, dict) else None
        pj = json.loads(S3C.get_object(Bucket=B, Key="data/eurodollar-plumbing.json")["Body"].read())
        bb = pj.get("bis_crossborder")
        ok2 = (not err) and isinstance(bb, dict) and \
              isinstance(bb.get("total_tn"), (int, float))
        gate("G2_plumbing", ok2,
             f"err={err} block={bb}")
    except Exception as e:
        gate("G2_plumbing", False, str(e)[:340])

    try:
        dep("justhodl-morning-intelligence", 300, 1024, "MI + bis_cb feed")
        loc = LAM.get_function(FunctionName="justhodl-morning-intelligence")["Code"]["Location"]
        zf = zipfile.ZipFile(io.BytesIO(urllib.request.urlopen(loc, timeout=60).read()))
        blob = b"".join(zf.read(n) for n in zf.namelist() if n.endswith(".py"))
        gate("G3_mi", b"bis_cb" in blob, f"zip bis_cb={b'bis_cb' in blob}")
    except Exception as e:
        gate("G3_mi", False, str(e)[:280])

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("VERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3651.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
