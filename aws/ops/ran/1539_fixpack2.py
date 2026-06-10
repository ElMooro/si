# ops 1539 — deploy deep-SPX analogs+backtester, recreate 11 dead schedules, walk-forward status, sectors schema, brief freshness
import json, os, time, zipfile, io, boto3
from datetime import datetime, timezone
from botocore.config import Config
from botocore.exceptions import ClientError
cfg = Config(read_timeout=300, retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
ev = boto3.client("events", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
out = {"ops": 1539, "deployed": [], "rules": [], "invoked": [], "errors": []}

def zip_src(fn):
    buf = io.BytesIO(); src = f"aws/lambdas/{fn}/source"
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for r, _, fs in os.walk(src):
            for f in fs:
                if "__pycache__" not in r and not f.endswith(".pyc"):
                    zf.write(os.path.join(r, f), arcname=os.path.relpath(os.path.join(r, f), src))
    return buf.getvalue()

def retry_conflict(fn_call, tries=10, wait=8):
    for i in range(tries):
        try: return fn_call()
        except ClientError as e:
            if e.response["Error"]["Code"] in ("ResourceConflictException", "TooManyRequestsException"):
                time.sleep(wait); continue
            raise
    raise RuntimeError("conflict retries exhausted")

# A) deploy + async invoke the two deep-SPX lambdas
for fn in ("justhodl-historical-analogs", "justhodl-alert-backtester"):
    try:
        retry_conflict(lambda: lam.update_function_code(FunctionName=fn, ZipFile=zip_src(fn)))
        for _ in range(40):
            c = lam.get_function_configuration(FunctionName=fn)
            if c.get("LastUpdateStatus") in ("Successful", None): break
            time.sleep(3)
        retry_conflict(lambda: lam.invoke(FunctionName=fn, InvocationType="Event", Payload=b"{}"))
        out["deployed"].append(fn)
    except Exception as e:
        out["errors"].append(f"deploy {fn}: {str(e)[:90]}")

# B) recreate dead schedules (rule name = <fn>-sched) + kick one async run each
SCHED = [("justhodl-opex-calendar", "cron(0 11 * * ? *)"), ("justhodl-vol-surface", "cron(30 13 * * ? *)"),
         ("justhodl-stablecoin-flow", "cron(0 14 * * ? *)"), ("justhodl-breadth-thrust", "cron(30 21 * * ? *)"),
         ("justhodl-rv-iv-scanner", "cron(30 14 * * ? *)"), ("justhodl-insider-buys-enriched", "cron(0 10 * * ? *)"),
         ("justhodl-buyback-scanner", "cron(0 12 ? * MON *)"), ("justhodl-russell-recon-frontrun", "cron(30 12 * * ? *)"),
         ("justhodl-fleet-freshness-monitor", "rate(1 hour)"), ("justhodl-fleet-monitor", "rate(6 hours)"),
         ("justhodl-carry-surface", "cron(0 13 * * ? *)")]
for fn, expr in SCHED:
    try:
        conf = lam.get_function_configuration(FunctionName=fn)
        arn = conf["FunctionArn"]
        rule = f"{fn}-sched"
        ev.put_rule(Name=rule, ScheduleExpression=expr, State="ENABLED",
                    Description=f"recreated ops1539 (deleted in cost-cuts)")
        ev.put_targets(Rule=rule, Targets=[{"Id": "1", "Arn": arn}])
        try:
            lam.add_permission(FunctionName=fn, StatementId=f"eb-{rule}"[:100], Action="lambda:InvokeFunction",
                               Principal="events.amazonaws.com",
                               SourceArn=f"arn:aws:events:us-east-1:857687956942:rule/{rule}")
        except ClientError as e:
            if e.response["Error"]["Code"] != "ResourceConflictException": raise
        retry_conflict(lambda: lam.invoke(FunctionName=fn, InvocationType="Event", Payload=b"{}"))
        out["rules"].append({"fn": fn, "rule": rule, "expr": expr})
        out["invoked"].append(fn)
    except ClientError as e:
        out["errors"].append(f"rule {fn}: {e.response['Error']['Code']}")
    except Exception as e:
        out["errors"].append(f"rule {fn}: {str(e)[:80]}")

# C) walk-forward backtest prefix status
try:
    r = s3.list_objects_v2(Bucket=B, Prefix="backtest/")
    now = datetime.now(timezone.utc)
    out["backtest_prefix"] = [{"key": o["Key"], "age_h": round((now - o["LastModified"]).total_seconds()/3600, 1),
                               "kb": round(o["Size"]/1024, 1)} for o in r.get("Contents", [])][:20]
except Exception as e:
    out["backtest_prefix"] = str(e)[:90]

# D) sectors schema
try:
    sec = json.loads(s3.get_object(Bucket=B, Key="data/sector-rotation.json")["Body"].read())
    s0 = (sec.get("sectors") or [{}])[0]
    out["sector_schema"] = {"top_keys": sorted(sec.keys()), "sector0_keys": sorted(s0.keys()),
                            "sector0_sample": {k: s0[k] for k in list(s0)[:8]}}
except Exception as e:
    out["sector_schema"] = str(e)[:90]

# E) brief freshness for review-named pages
now = datetime.now(timezone.utc)
def age(key):
    try:
        h = s3.head_object(Bucket=B, Key=key)
        return round((now - h["LastModified"]).total_seconds()/3600, 1)
    except Exception:
        return None
out["freshness_h"] = {k: age(k) for k in [
    "data/carry-surface.json", "data/sector-rotation.json", "data/13f-intel.json", "data/13f.json",
    "data/deep-value.json", "data/eps-velocity.json", "data/volatility.json", "data/vol-surface.json",
    "data/repo-stress.json", "data/flow.json", "data/event-flow.json", "data/master-board.json",
    "data/best-setups.json", "data/screener-brief.json", "data/opex-calendar.json", "data/breadth-thrust.json",
    "data/stablecoin-flow.json", "data/rv-iv.json", "data/insider-buys-enriched.json", "data/buyback-scan.json",
    "data/russell-recon.json"]}

# F) wait for async runs, then verify deep-SPX results
time.sleep(115)
try:
    an = json.loads(s3.get_object(Bucket=B, Key="data/historical-analogs.json")["Body"].read())
    yrs = sorted({d[:4] for d in (an.get("analog_dates") or [])})
    out["analogs_verify"] = {"version": an.get("version"), "spy_span": an.get("spy_span") or an.get("data_span"),
                             "n_pool": an.get("n_pool_dates"), "analog_years": yrs,
                             "n_analogs": len(an.get("analog_dates") or [])}
except Exception as e:
    out["analogs_verify"] = str(e)[:90]
try:
    bt = json.loads(s3.get_object(Bucket=B, Key="data/alert-backtests.json")["Body"].read())
    rows = bt.get("rules") or bt.get("results") or []
    out["alertbt_verify"] = {"spy_span": bt.get("spy_span"), "n_rules": len(rows),
                             "sample": [{"rule": r.get("rule") or r.get("id"), "n_fires": r.get("n_fires"),
                                         "med_21d": (r.get("fwd_21d") or {}).get("median_pct")} for r in rows[:6]]}
except Exception as e:
    out["alertbt_verify"] = str(e)[:90]

open("aws/ops/reports/1539_fixpack2.json", "w").write(json.dumps(out, indent=2, default=str))
print(json.dumps({"deployed": out["deployed"], "rules_n": len(out["rules"]), "errors": out["errors"][:5],
                  "analogs": out.get("analogs_verify"), "alertbt": out.get("alertbt_verify")}, default=str)[:1400])
