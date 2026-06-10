# ops 1539 — yahoo deep-SPX redeploy, recreate deleted dark-engine schedules, walk-forward artifact check
import json, os, time, zipfile, io, boto3
from botocore.config import Config
from botocore.exceptions import ClientError
cfg = Config(read_timeout=300, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
ev = boto3.client("events", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
ACC = "857687956942"
out = {"ops": 1539}

def retry_conflict(fn, tries=10, wait=8):
    for i in range(tries):
        try: return fn()
        except ClientError as e:
            if e.response["Error"]["Code"] in ("ResourceConflictException", "TooManyRequestsException"):
                time.sleep(wait); continue
            raise
    raise RuntimeError("exhausted")

def zip_src(src):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for r, _, fs in os.walk(src):
            for f in fs:
                if "__pycache__" not in r and not f.endswith(".pyc"):
                    zf.write(os.path.join(r, f), arcname=os.path.relpath(os.path.join(r, f), src))
    return buf.getvalue()

def rd(k):
    try: return json.loads(s3.get_object(Bucket=B, Key=k)["Body"].read())
    except Exception as e: return {"_err": str(e)[:70]}

# A) redeploy analogs v2.1 + backtester v1.1, async kick
for fn, src in (("justhodl-historical-analogs", "aws/lambdas/justhodl-historical-analogs/source"),
                ("justhodl-alert-backtester", "aws/lambdas/justhodl-alert-backtester/source")):
    retry_conflict(lambda: lam.update_function_code(FunctionName=fn, ZipFile=zip_src(src)))
    for _ in range(40):
        c = lam.get_function_configuration(FunctionName=fn)
        if c.get("LastUpdateStatus") in ("Successful", None): break
        time.sleep(3)
    lam.invoke(FunctionName=fn, InvocationType="Event", Payload=b"{}")
out["deployed"] = ["analogs v2.1", "backtester v1.1"]

# B) recreate schedules for engines whose rules were DELETED in cost-cuts
SCHED = [
    ("justhodl-opex-calendar",          "justhodl-opex-calendar-daily",          "cron(0 11 * * ? *)"),
    ("justhodl-vol-surface",            "justhodl-vol-surface-daily",            "cron(30 13 * * ? *)"),
    ("justhodl-stablecoin-flow",        "justhodl-stablecoin-flow-daily",        "cron(0 14 * * ? *)"),
    ("justhodl-breadth-thrust",         "justhodl-breadth-thrust-daily",         "cron(30 21 * * ? *)"),
    ("justhodl-rv-iv-scanner",          "justhodl-rv-iv-scanner-daily",          "cron(30 14 * * ? *)"),
    ("justhodl-insider-buys-enriched",  "justhodl-insider-buys-enriched-daily",  "cron(0 10 * * ? *)"),
    ("justhodl-buyback-scanner",        "justhodl-buyback-scanner-weekly",       "cron(0 12 ? * MON *)"),
    ("justhodl-russell-recon-frontrun", "justhodl-russell-recon-daily",          "cron(30 12 * * ? *)"),
    ("justhodl-fleet-freshness-monitor","justhodl-fleet-freshness-hourly",       "rate(1 hour)"),
    ("justhodl-fleet-monitor",          "justhodl-fleet-monitor-6h",             "rate(6 hours)"),
    ("justhodl-fleet-error-monitor",    "justhodl-fleet-error-6h",               "rate(6 hours)"),
]
created, sched_err = [], []
for fn, rule, expr in SCHED:
    try:
        lam.get_function_configuration(FunctionName=fn)
    except Exception:
        sched_err.append(f"{fn}: lambda missing"); continue
    try:
        ev.put_rule(Name=rule, ScheduleExpression=expr, State="ENABLED",
                    Description=f"ops1539 recreate: {fn}")
        try:
            lam.add_permission(FunctionName=fn, StatementId=f"eb-{rule}"[:90],
                               Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                               SourceArn=f"arn:aws:events:us-east-1:{ACC}:rule/{rule}")
        except ClientError as e:
            if e.response["Error"]["Code"] != "ResourceConflictException":
                raise
        ev.put_targets(Rule=rule, Targets=[{"Id": "1", "Arn": f"arn:aws:lambda:us-east-1:{ACC}:function:{fn}"}])
        created.append(f"{rule} → {fn} ({expr})")
        lam.invoke(FunctionName=fn, InvocationType="Event", Payload=b"{}")  # immediate refresh
    except Exception as e:
        sched_err.append(f"{rule}: {type(e).__name__} {str(e)[:60]}")
out["rules_created"] = created
out["sched_errors"] = sched_err

# C) walk-forward artifact: backtest/ prefix
try:
    r = s3.list_objects_v2(Bucket=B, Prefix="backtest/", MaxKeys=20)
    now = time.time()
    out["backtest_prefix"] = [{"key": o["Key"], "age_h": round((now - o["LastModified"].timestamp()) / 3600, 1),
                               "kb": round(o["Size"] / 1024, 1)} for o in r.get("Contents", [])]
    summ = rd("backtest/summary.json")
    out["wf_summary"] = ({k: summ[k] for k in list(summ.keys())[:16] if not isinstance(summ[k], (list, dict))}
                         | {"_keys": list(summ.keys())[:20]}) if isinstance(summ, dict) else summ
except Exception as e:
    out["backtest_prefix"] = str(e)[:90]

# D) settle + verify deep engines
time.sleep(120)
an = rd("data/historical-analogs.json")
out["analogs"] = {"version": an.get("version"), "n_dates": an.get("n_historical_dates_evaluated"),
                  "analog_dates": [a.get("date") for a in (an.get("analogs") or [])[:10]],
                  "fwd21": (an.get("forward_distribution") or {}).get("21d"),
                  "call": an.get("directional_call")}
bt = rd("data/alert-backtests.json")
out["alert_bt"] = {"version": bt.get("version"), "spy_span": bt.get("spy_span"), "n_rules": bt.get("n_rules"),
                   "rows": [(x.get("id"), x.get("n_fires"),
                             ((x.get("forward_spy") or {}).get("21d") or {}).get("n"),
                             ((x.get("forward_spy") or {}).get("21d") or {}).get("median_pct"),
                             ((x.get("forward_spy") or {}).get("21d") or {}).get("pct_negative"))
                            for x in (bt.get("rules") or [])]}
open("aws/ops/reports/1539_depth2.json", "w").write(json.dumps(out, indent=2, default=str))
print(json.dumps({"rules_created": len(created), "errs": sched_err[:3], "spy": out["alert_bt"].get("spy_span"),
                  "analog_dates": out["analogs"]["analog_dates"][:5],
                  "wf_keys": [b.get("key") for b in out["backtest_prefix"]] if isinstance(out["backtest_prefix"], list) else out["backtest_prefix"]}, default=str))
