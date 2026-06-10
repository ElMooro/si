# ops 1547 — remediation: monitor v1.2 hard-recreate, freshness manifest, dead rules revived, invocations, verify
import json, os, time, zipfile, io, boto3
from datetime import datetime, timezone
from botocore.config import Config
from botocore.exceptions import ClientError
cfg = Config(read_timeout=300, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
ev = boto3.client("events", region_name="us-east-1", config=cfg)
logs = boto3.client("logs", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
ACC = "857687956942"
out = {"ops": 1547, "errors": [], "rules": [], "invoked": []}


def retry_conflict(fn, tries=10, wait=8):
    for i in range(tries):
        try:
            return fn()
        except ClientError as e:
            if e.response["Error"]["Code"] in ("ResourceConflictException", "TooManyRequestsException"):
                time.sleep(wait); continue
            raise
    raise RuntimeError("retries exhausted")


def deploy(fn, src):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for r, _, fs in os.walk(src):
            for f in fs:
                if "__pycache__" not in r and not f.endswith(".pyc"):
                    zf.write(os.path.join(r, f), arcname=os.path.relpath(os.path.join(r, f), src))
    retry_conflict(lambda: lam.update_function_code(FunctionName=fn, ZipFile=buf.getvalue()))
    for _ in range(40):
        c = lam.get_function_configuration(FunctionName=fn)
        if c.get("LastUpdateStatus") in ("Successful", None):
            return
        time.sleep(3)


def ensure_rule(fn, rule, expr):
    try:
        arn = lam.get_function(FunctionName=fn)["Configuration"]["FunctionArn"]
        ev.put_rule(Name=rule, ScheduleExpression=expr, State="ENABLED")
        ev.put_targets(Rule=rule, Targets=[{"Id": "1", "Arn": arn}])
        try:
            lam.add_permission(FunctionName=fn, StatementId=f"{rule}-{int(time.time())}",
                               Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                               SourceArn=f"arn:aws:events:us-east-1:{ACC}:rule/{rule}")
        except ClientError as e:
            if e.response["Error"]["Code"] != "ResourceConflictException":
                raise
        out["rules"].append({"fn": fn, "rule": rule, "expr": expr})
    except Exception as e:
        out["errors"].append(f"rule {rule}: {str(e)[:90]}")


# 0) discover skill-chain lambdas
names = []
pg = lam.get_paginator("list_functions")
for page in pg.paginate():
    for f in page["Functions"]:
        n = f["FunctionName"]
        if any(t in n for t in ("skill", "aggregat", "outcome", "calibrat")):
            names.append(n)
out["skill_chain_fns"] = sorted(names)
skill_fn = next((n for n in names if "skill" in n and "aggregat" in n), None) or \
           next((n for n in names if "skill" in n), None)

# 1) monitor v1.2 deploy + hard rule + manifest
deploy("justhodl-fleet-freshness-monitor", "aws/lambdas/justhodl-fleet-freshness-monitor/source")
ensure_rule("justhodl-fleet-freshness-monitor", "justhodl-freshness-30m", "rate(30 minutes)")
manifest = {
    "rules": [{"prefix": "data/", "default_max_age_h": 26.0}],
    "exclude_prefixes": ["data/archive/", "data/_archive/", "data/ecb-hist/", "data/vintage/",
                         "data/carry-surface/", "data/crisis-knowledge-base", "data/_skill/archive/"],
    "key_overrides": {
        "data/sector-rotation.json": 7, "data/eurodollar-stress.json": 3, "data/ai-brief.json": 6,
        "data/auction-crisis.json": 30, "data/vol-surface.json": 30, "data/carry-surface.json": 30,
        "data/_skill/frontrun-skill-index.json": 30, "data/alert-backtests.json": 30,
        "data/historical-analogs.json": 30, "data/ecb-derived.json": 8, "data/global-tide.json": 30,
        "data/apex-fusion.json": 5, "data/spx-history-deep.json": 2000, "data/bottleneck-boom.json": 30
    },
    "updated_at": datetime.now(timezone.utc).isoformat(), "updated_by": "ops-1547"
}
s3.put_object(Bucket=B, Key="data/_freshness-manifest.json", Body=json.dumps(manifest, indent=1).encode(),
              ContentType="application/json")
out["manifest_written"] = len(manifest["key_overrides"])

# 2) revive dead schedules
ensure_rule("justhodl-sector-rotation", "justhodl-sector-rotation-6h", "cron(15 1/6 * * ? *)")
ensure_rule("justhodl-auction-crisis-detector", "justhodl-auction-crisis-active", "cron(0/15 14-22 ? * MON-FRI *)")
ensure_rule("justhodl-auction-crisis-detector", "justhodl-auction-crisis-backstop", "cron(5 13 * * ? *)")
ensure_rule("justhodl-eurodollar-stress", "justhodl-eurodollar-stress-2h", "cron(10 0/2 * * ? *)")
if skill_fn:
    ensure_rule(skill_fn, "justhodl-skill-aggregator-daily", "cron(30 10 * * ? *)")

# 3) enable disabled ai-brief
try:
    ev.enable_rule(Name="justhodl-ai-brief-4h")
    out["rules"].append({"fn": "justhodl-ai-brief", "rule": "justhodl-ai-brief-4h", "expr": "ENABLED (existing)"})
except Exception as e:
    out["errors"].append(f"enable ai-brief-4h: {str(e)[:80]}")

# 4) invoke the dead now
for fn in ["justhodl-auction-crisis-detector", "justhodl-eurodollar-stress", "justhodl-ai-brief"] + ([skill_fn] if skill_fn else []):
    try:
        retry_conflict(lambda f=fn: lam.invoke(FunctionName=f, InvocationType="Event", Payload=b"{}"))
        out["invoked"].append(fn)
    except Exception as e:
        out["errors"].append(f"invoke {fn}: {str(e)[:80]}")

# 5) monitor smoke test: invoke sync + capture its summary lines
t0 = int(time.time() * 1000)
r = retry_conflict(lambda: lam.invoke(FunctionName="justhodl-fleet-freshness-monitor", InvocationType="RequestResponse", Payload=b"{}"))
out["monitor_invoke_err"] = r.get("FunctionError", "NONE")
time.sleep(5)
try:
    lr = logs.filter_log_events(logGroupName="/aws/lambda/justhodl-fleet-freshness-monitor", startTime=t0, limit=30)
    out["monitor_run_lines"] = [e["message"].strip()[:140] for e in lr.get("events", [])
                                 if any(k in e["message"] for k in ("freshness", "ESCALATION", "telegram", "alerts"))][:10]
except Exception as e:
    out["monitor_run_lines"] = str(e)[:80]

time.sleep(70)
def age_h(key):
    try:
        h = s3.head_object(Bucket=B, Key=key)
        return round((datetime.now(timezone.utc) - h["LastModified"]).total_seconds() / 3600, 1)
    except Exception:
        return None
out["ages_after"] = {k: age_h(k) for k in ("data/sector-rotation.json", "data/auction-crisis.json",
                                            "data/eurodollar-stress.json", "data/ai-brief.json",
                                            "data/_skill/frontrun-skill-index.json")}
open("aws/ops/reports/1547_remediate.json", "w").write(json.dumps(out, indent=2, default=str))
print(json.dumps({"rules": len(out["rules"]), "invoked": out["invoked"], "monitor": out["monitor_run_lines"][:3],
                  "ages_after": out["ages_after"], "skill_fns": out["skill_chain_fns"], "errors": out["errors"][:3]}, default=str)[:900])
