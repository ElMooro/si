"""ops 2792 — create justhodl-llm-cost-dashboard Lambda (brand-new dir no-ops on
GitHub deploy), wire hourly EventBridge, seed data/llm-cost.json, verify."""
import os, io, json, zipfile, time
from datetime import datetime, timezone
import boto3

REGION = "us-east-1"
FN = "justhodl-llm-cost-dashboard"
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"
SRC = "aws/lambdas/%s/source" % FN
R = {"ops": 2792, "ts": datetime.now(timezone.utc).isoformat(), "steps": {}}

lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)

# 1) build zip: source/*.py at root + aws/shared/*.py at root -------------------
buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
    for fn in os.listdir(SRC):
        if fn.endswith(".py"):
            z.write(os.path.join(SRC, fn), fn)
    for fn in os.listdir("aws/shared"):
        if fn.endswith(".py"):
            z.write(os.path.join("aws/shared", fn), fn)
zip_bytes = buf.getvalue()
R["steps"]["zip_bytes"] = len(zip_bytes)
print("zip built:", len(zip_bytes), "bytes")

# 2) create or update function -------------------------------------------------
cfg = dict(FunctionName=FN, Runtime="python3.12", Role=ROLE,
           Handler="lambda_function.lambda_handler", Timeout=120, MemorySize=256,
           Environment={"Variables": {"S3_BUCKET": "justhodl-dashboard-live"}})
try:
    lam.get_function(FunctionName=FN)
    lam.update_function_code(FunctionName=FN, ZipFile=zip_bytes)
    R["steps"]["fn"] = "updated"
    print("function: updated existing")
except lam.exceptions.ResourceNotFoundException:
    lam.create_function(Code={"ZipFile": zip_bytes}, **cfg)
    R["steps"]["fn"] = "created"
    print("function: created")
# wait active
for _ in range(30):
    try:
        if lam.get_function_configuration(FunctionName=FN)["State"] == "Active":
            break
    except Exception:
        pass
    time.sleep(2)

# 3) EventBridge hourly ---------------------------------------------------------
RULE = "justhodl-llm-cost-dashboard-hourly"
try:
    events.put_rule(Name=RULE, ScheduleExpression="cron(5 * * * ? *)", State="ENABLED",
                    Description="Hourly :05 — refresh LLM cost ledger aggregation")
    arn = lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]
    try:
        lam.add_permission(FunctionName=FN, StatementId="%s-invoke" % RULE,
                           Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                           SourceArn="arn:aws:events:%s:857687956942:rule/%s" % (REGION, RULE))
    except lam.exceptions.ResourceConflictException:
        pass
    events.put_targets(Rule=RULE, Targets=[{"Id": "1", "Arn": arn}])
    R["steps"]["schedule"] = "wired cron(5 * * * ? *)"
    print("schedule: wired")
except Exception as e:
    R["steps"]["schedule"] = "ERR " + str(e)[:120]
    print("schedule ERR", str(e)[:120])

# 4) seed data/llm-cost.json + verify -----------------------------------------
try:
    resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse")
    payload = json.loads(resp["Payload"].read().decode() or "{}")
    R["steps"]["seed_invoke"] = payload
    print("seed invoke:", json.dumps(payload)[:160])
except Exception as e:
    R["steps"]["seed_invoke"] = "ERR " + str(e)[:120]
    print("seed ERR", str(e)[:120])
try:
    o = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/llm-cost.json")
    d = json.loads(o["Body"].read())
    R["steps"]["output"] = {"mode": d.get("mode"), "budget": d.get("daily_budget_usd"),
                            "today_cost": d.get("today", {}).get("cost"),
                            "engines": len(d.get("per_engine", [])),
                            "cache_hit_rate_14d": d.get("trailing_14d", {}).get("cache_hit_rate")}
    print("output verified:", json.dumps(R["steps"]["output"]))
except Exception as e:
    R["steps"]["output"] = "ERR " + str(e)[:120]
    print("output ERR", str(e)[:120])

R["status"] = "DASHBOARD LAMBDA LIVE" if R["steps"].get("output") and isinstance(R["steps"]["output"], dict) else "CHECK"
os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/2792_dashboard_lambda.json", "w"), indent=1, default=str)
print("OPS 2792:", R["status"])
