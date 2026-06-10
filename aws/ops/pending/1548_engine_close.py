# ops 1548 — monitor v1.2.1, router target re-attach + regen, CREATE bottleneck-boom + verify, acceptance snapshot
import json, os, time, zipfile, io, boto3
from datetime import datetime, timezone
from botocore.config import Config
from botocore.exceptions import ClientError
cfg = Config(read_timeout=300, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
ev = boto3.client("events", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
ddb = boto3.client("dynamodb", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
ACC = "857687956942"
out = {"ops": 1548, "errors": []}


def retry_conflict(fn, tries=10, wait=8):
    for i in range(tries):
        try:
            return fn()
        except ClientError as e:
            if e.response["Error"]["Code"] in ("ResourceConflictException", "TooManyRequestsException"):
                time.sleep(wait); continue
            raise
    raise RuntimeError("retries exhausted")


def zip_src(src):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for r, _, fs in os.walk(src):
            for f in fs:
                if "__pycache__" not in r and not f.endswith(".pyc"):
                    zf.write(os.path.join(r, f), arcname=os.path.relpath(os.path.join(r, f), src))
    return buf.getvalue()


def wait_ready(fn):
    for _ in range(40):
        c = lam.get_function_configuration(FunctionName=fn)
        if c.get("LastUpdateStatus") in ("Successful", None) and c.get("State") in ("Active", None):
            return
        time.sleep(3)


# A) monitor v1.2.1
retry_conflict(lambda: lam.update_function_code(
    FunctionName="justhodl-fleet-freshness-monitor",
    ZipFile=zip_src("aws/lambdas/justhodl-fleet-freshness-monitor/source")))
wait_ready("justhodl-fleet-freshness-monitor")
out["monitor"] = "v1.2.1 deployed"

# B) router rules: re-attach detached targets, then regen everything
router_arn = lam.get_function(FunctionName="justhodl-ai-brief-router")["Configuration"]["FunctionArn"]
fixed = []
for rule in ("justhodl-alerts-digest-daily", "justhodl-alerts-digest-close-daily"):
    try:
        tg = ev.list_targets_by_rule(Rule=rule).get("Targets", [])
        if not tg:
            ev.put_targets(Rule=rule, Targets=[{"Id": "1", "Arn": router_arn}])
            try:
                lam.add_permission(FunctionName="justhodl-ai-brief-router",
                                   StatementId=f"{rule}-fix-{int(time.time())}",
                                   Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                                   SourceArn=f"arn:aws:events:us-east-1:{ACC}:rule/{rule}")
            except ClientError:
                pass
            fixed.append(rule)
        else:
            fixed.append(f"{rule}:had_{len(tg)}_targets")
    except Exception as e:
        out["errors"].append(f"{rule}: {str(e)[:80]}")
ab_arn = lam.get_function(FunctionName="justhodl-ai-brief")["Configuration"]["FunctionArn"]
try:
    tg = ev.list_targets_by_rule(Rule="justhodl-ai-brief-4h").get("Targets", [])
    if not tg:
        ev.put_targets(Rule="justhodl-ai-brief-4h", Targets=[{"Id": "1", "Arn": ab_arn}])
        fixed.append("justhodl-ai-brief-4h")
    else:
        fixed.append(f"justhodl-ai-brief-4h:had_{len(tg)}_targets")
except Exception as e:
    out["errors"].append(f"ai-brief-4h targets: {str(e)[:80]}")
out["router_targets"] = fixed
retry_conflict(lambda: lam.invoke(FunctionName="justhodl-ai-brief-router", InvocationType="Event", Payload=b"{}"))

# C) CREATE bottleneck-boom
role = lam.get_function(FunctionName="justhodl-historical-analogs")["Configuration"]["Role"]
code = zip_src("aws/lambdas/justhodl-bottleneck-boom/source")
try:
    lam.get_function(FunctionName="justhodl-bottleneck-boom")
    retry_conflict(lambda: lam.update_function_code(FunctionName="justhodl-bottleneck-boom", ZipFile=code))
    out["bottleneck_fn"] = "updated"
except ClientError:
    retry_conflict(lambda: lam.create_function(
        FunctionName="justhodl-bottleneck-boom", Runtime="python3.12", Role=role,
        Handler="lambda_function.lambda_handler", Code={"ZipFile": code},
        Timeout=300, MemorySize=512,
        Environment={"Variables": {"FRED_KEY": "2f057499936072679d8843d7fce99989",
                                   "FMP_KEY": "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb",
                                   "SIGNALS_TABLE": "justhodl-signals"}},
        Description="Supply-bottleneck boom detector: Census M3 backlog pressure x company capture; closed-loop logged"))
    out["bottleneck_fn"] = "created"
wait_ready("justhodl-bottleneck-boom")
try:
    arn = lam.get_function(FunctionName="justhodl-bottleneck-boom")["Configuration"]["FunctionArn"]
    ev.put_rule(Name="justhodl-bottleneck-boom-daily", ScheduleExpression="cron(45 12 * * ? *)", State="ENABLED")
    ev.put_targets(Rule="justhodl-bottleneck-boom-daily", Targets=[{"Id": "1", "Arn": arn}])
    try:
        lam.add_permission(FunctionName="justhodl-bottleneck-boom", StatementId=f"bnb-{int(time.time())}",
                           Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                           SourceArn=f"arn:aws:events:us-east-1:{ACC}:rule/justhodl-bottleneck-boom-daily")
    except ClientError:
        pass
    out["bottleneck_rule"] = "cron(45 12 * * ? *)"
except Exception as e:
    out["errors"].append(f"bnb rule: {str(e)[:80]}")

r = retry_conflict(lambda: lam.invoke(FunctionName="justhodl-bottleneck-boom",
                                      InvocationType="RequestResponse", Payload=b"{}"))
out["bnb_fn_error"] = r.get("FunctionError", "NONE")
if out["bnb_fn_error"] != "NONE":
    out["bnb_payload"] = r["Payload"].read().decode()[:400]
time.sleep(2)
try:
    bb = json.loads(s3.get_object(Bucket=B, Key="data/bottleneck-boom.json")["Body"].read())
    out["bnb_verify"] = {"scored_n": bb.get("scored_n"), "universe_source": bb.get("universe_source"),
                         "signals_logged": bb.get("signals_logged"), "top_calls": bb.get("top_calls"),
                         "fred_used": bb.get("fred_used"), "fred_failed": bb.get("fred_failed"),
                         "pressure": bb.get("industry_pressure"),
                         "rank1": (bb.get("ranks") or [{}])[0], "duration_s": bb.get("duration_s")}
    tc = (bb.get("top_calls") or [None])[0]
    if tc:
        d0 = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        it = ddb.get_item(TableName="justhodl-signals",
                          Key={"signal_id": {"S": f"bottleneck-boom#{tc}#{d0}"}}).get("Item")
        out["ddb_signal"] = {"found": bool(it),
                             "fields": sorted(it.keys()) if it else None,
                             "status": (it or {}).get("status", {}).get("S"),
                             "direction": (it or {}).get("predicted_direction", {}).get("S")}
except Exception as e:
    out["bnb_verify"] = str(e)[:140]

# D) regen wait + acceptance snapshot
time.sleep(110)
def age_h(key):
    try:
        h = s3.head_object(Bucket=B, Key=key)
        return round((datetime.now(timezone.utc) - h["LastModified"]).total_seconds() / 3600, 1)
    except Exception:
        return None
out["acceptance_ages"] = {k: age_h(k) for k in (
    "data/carry-surface.json", "data/vol-surface.json", "data/sector-rotation.json",
    "data/eurodollar-stress.json", "data/auction-crisis.json", "data/ai-brief.json",
    "data/_skill/frontrun-skill-index.json", "data/bottleneck-boom.json",
    "data/alert-backtests.json", "data/historical-analogs.json")}
try:
    sr = json.loads(s3.get_object(Bucket=B, Key="data/sector-rotation.json")["Body"].read())
    out["sector_table_rows"] = len(sr.get("sectors") or sr.get("table") or sr.get("ranks") or [])
except Exception as e:
    out["sector_table_rows"] = str(e)[:60]
open("aws/ops/reports/1548_close.json", "w").write(json.dumps(out, indent=2, default=str))
print(json.dumps({"bnb": out.get("bnb_verify", {}).get("scored_n") if isinstance(out.get("bnb_verify"), dict) else out.get("bnb_verify"),
                  "top": (out.get("bnb_verify") or {}).get("top_calls") if isinstance(out.get("bnb_verify"), dict) else None,
                  "ddb": out.get("ddb_signal"), "targets": out["router_targets"],
                  "ages": out["acceptance_ages"]}, default=str)[:800])
