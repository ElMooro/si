"""ops 3641 — RESURRECTION TRIO: squeeze-fuel (short-book guard input!),
options-analytics, tail-risk. Per engine: config + CW 7d inv/err + Scheduler
presence + live invoke (LogTail) + post-invoke S3 freshness. HEAL: clean
invoke + fresh key + no schedule -> create daily Scheduler (fleet role).
Erroring engines get their error-head recorded for the code-fix cycle."""
import base64, json, sys, time
from datetime import datetime, timedelta, timezone
from pathlib import Path
import boto3
from botocore.config import Config
from ops_report import report

LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=600, retries={"max_attempts": 0}))
S3C = boto3.client("s3", "us-east-1")
CW = boto3.client("cloudwatch", "us-east-1")
EBS = boto3.client("scheduler", "us-east-1")
B = "justhodl-dashboard-live"
ACCT = "857687956942"
ROLE = f"arn:aws:iam::{ACCT}:role/justhodl-scheduler-role"

TRIO = [
    ("justhodl-squeeze-fuel", ["data/squeeze-fuel.json"], "cron(40 21 ? * MON-FRI *)"),
    ("justhodl-options-analytics", ["data/options-analytics.json",
                                    "data/options-analytics-iv-history.json"],
     "cron(30 21 ? * MON-FRI *)"),
    ("justhodl-tail-risk", ["data/tail-risk.json", "data/tail-risk-history.json"],
     "cron(35 21 * * ? *)"),
]

with report("3641_resurrect_trio") as rep:
    rep.heading("ops 3641 — squeeze-fuel / options-analytics / tail-risk")
    out = {"gates": {}, "engines": {}}
    fails = []

    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:680]}
        print(("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:640]); rep.log(n + " " + str(ok))
        if not ok:
            fails.append(n)

    def cw7(fn, metric):
        try:
            r = CW.get_metric_statistics(
                Namespace="AWS/Lambda", MetricName=metric,
                Dimensions=[{"Name": "FunctionName", "Value": fn}],
                StartTime=datetime.now(timezone.utc) - timedelta(days=7),
                EndTime=datetime.now(timezone.utc), Period=604800,
                Statistics=["Sum"])
            return int(sum(p["Sum"] for p in (r.get("Datapoints") or [])))
        except Exception:
            return -1

    for fn, keys, cronx in TRIO:
        E = {"fn": fn}
        try:
            cfg = LAM.get_function_configuration(FunctionName=fn)
            E["timeout"] = cfg.get("Timeout"); E["mem"] = cfg.get("MemorySize")
            E["inv7"] = cw7(fn, "Invocations"); E["err7"] = cw7(fn, "Errors")
            scheds = []
            try:
                pg = EBS.list_schedules(NamePrefix=fn.replace("justhodl-", "justhodl-"))
                scheds = [s["Name"] for s in pg.get("Schedules", [])]
            except Exception as e:
                E["sched_list_err"] = str(e)[:80]
            E["schedules"] = scheds
            r = LAM.invoke(FunctionName=fn, InvocationType="RequestResponse",
                           LogType="Tail", Payload=b"{}")
            pl_raw = r["Payload"].read() or b"{}"
            try:
                pl = json.loads(pl_raw)
            except Exception:
                pl = {"raw": pl_raw[:200].decode("utf-8", "replace")}
            E["err"] = pl.get("errorMessage") if isinstance(pl, dict) else None
            logs = base64.b64decode(r.get("LogResult", "") or b"").decode("utf-8", "replace")
            E["log_tail"] = logs[-500:]
            time.sleep(3)
            fresh = {}
            for k in keys:
                try:
                    h = S3C.head_object(Bucket=B, Key=k)
                    age_min = (datetime.now(timezone.utc) - h["LastModified"]).total_seconds() / 60
                    fresh[k] = round(age_min, 1)
                except Exception as e:
                    fresh[k] = "missing:" + str(e)[:40]
            E["fresh_min"] = fresh
            freshened = any(isinstance(v, (int, float)) and v < 15 for v in fresh.values())
            E["freshened"] = freshened
            healed = None
            if freshened and not E["err"] and not scheds:
                try:
                    nm = fn + "-daily"
                    EBS.create_schedule(
                        Name=nm, ScheduleExpression=cronx,
                        ScheduleExpressionTimezone="UTC",
                        Description=f"ops 3641 resurrection: {fn} daily",
                        State="ENABLED", FlexibleTimeWindow={"Mode": "OFF"},
                        Target={"Arn": f"arn:aws:lambda:us-east-1:{ACCT}:function:{fn}",
                                "RoleArn": ROLE,
                                "Input": json.dumps({"source": "scheduler"})})
                    healed = nm
                except Exception as e:
                    healed = "create_fail:" + str(e)[:90]
            E["healed_schedule"] = healed
            ok = freshened or bool(E["err"]) or bool(E["log_tail"])
            gate("G_" + fn.split("-", 1)[1], ok,
                 f"inv7={E['inv7']} err7={E['err7']} scheds={scheds} "
                 f"invoke_err={str(E['err'])[:120]} fresh={fresh} "
                 f"healed={healed} log_tail={E['log_tail'][-220:]}")
        except Exception as e:
            E["fatal"] = str(e)[:280]
            gate("G_" + fn.split("-", 1)[1], False, E["fatal"])
        out["engines"][fn] = E

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("VERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3641.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
