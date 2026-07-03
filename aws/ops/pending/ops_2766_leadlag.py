"""ops 2766 — APAC LEAD-LAG engine live (backfill + cross-correlation).
Brand-new lambda dir => boto3 create_function (reuse apac-flows role + FMP_KEY),
EventBridge daily 10:15 UTC. Invoke, verify: Asian flow backfills (tw_semi/
kr_memory/hk_south) have real days, FMP US closes loaded, pairs carry computed r
with sample sizes. Report: 2766_leadlag.json.
"""
import os, io, json, time, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
REGION, BUCKET, ACCT, FN = "us-east-1", "justhodl-dashboard-live", "857687956942", "justhodl-apac-leadlag"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=300, retries={"max_attempts": 1}))
ev = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 2766, "ts": datetime.now(timezone.utc).isoformat()}
def zip_fn(fn):
    src = "aws/lambdas/%s/source" % fn; buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(src):
            if "__pycache__" in root: continue
            for f in files: z.write(os.path.join(root, f), os.path.relpath(os.path.join(root, f), src))
        for f in sorted(os.listdir("aws/shared")):
            if f.endswith(".py"): z.write(os.path.join("aws/shared", f), f)
    return buf.getvalue()
def wait_ok(fn, b=200):
    t0 = time.time()
    while time.time() - t0 < b:
        try:
            c = lam.get_function_configuration(FunctionName=fn)
            if c.get("State") == "Active" and c.get("LastUpdateStatus") in (None, "Successful"): return
        except ClientError: pass
        time.sleep(5)
print("settling 15s…"); time.sleep(15)
print("== 1/4 create/update lambda ==")
base = lam.get_function_configuration(FunctionName="justhodl-apac-flows")
role, runtime = base["Role"], base["Runtime"]
fmp = None
for donor in ("justhodl-apac-flows", "justhodl-fundamentals-engine", "justhodl-ma-tracker"):
    env = (lam.get_function_configuration(FunctionName=donor).get("Environment", {}) or {}).get("Variables", {}) or {}
    fmp = env.get("FMP_KEY") or env.get("FMP_API_KEY")
    if fmp: break
assert fmp, "no FMP key"
code = zip_fn(FN)
try:
    lam.get_function(FunctionName=FN); exists = True
except ClientError:
    exists = False
if exists:
    wait_ok(FN); lam.update_function_code(FunctionName=FN, ZipFile=code); wait_ok(FN)
    lam.update_function_configuration(FunctionName=FN, Timeout=300, MemorySize=512, Runtime=runtime,
                                      Handler="lambda_function.lambda_handler",
                                      Environment={"Variables": {"FMP_KEY": fmp, "FMP_API_KEY": fmp, "TZ": "UTC"}})
    wait_ok(FN); print("  updated")
else:
    for i in range(6):
        try:
            lam.create_function(FunctionName=FN, Runtime=runtime, Role=role, Handler="lambda_function.lambda_handler",
                                Code={"ZipFile": code}, Timeout=300, MemorySize=512, Publish=True,
                                Environment={"Variables": {"FMP_KEY": fmp, "FMP_API_KEY": fmp, "TZ": "UTC"}})
            break
        except ClientError as e:
            if "role" in str(e).lower() and i < 5: time.sleep(10); continue
            raise
    wait_ok(FN); print("  created")
R["function"] = "ready"
print("== 2/4 EventBridge daily 10:15 UTC ==")
rule = "justhodl-apac-leadlag-daily"
ev.put_rule(Name=rule, ScheduleExpression="cron(15 10 * * ? *)", State="ENABLED", Description="APAC lead-lag recompute")
arn = lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]
try:
    lam.add_permission(FunctionName=FN, StatementId="leadlag-eventbridge", Action="lambda:InvokeFunction",
                       Principal="events.amazonaws.com", SourceArn="arn:aws:events:%s:%s:rule/%s" % (REGION, ACCT, rule))
except ClientError as e:
    if "ResourceConflict" not in str(e): raise
ev.put_targets(Rule=rule, Targets=[{"Id": "leadlag", "Arn": arn}])
assert ev.describe_rule(Name=rule)["State"] == "ENABLED"
R["schedule"] = "cron(15 10 * * ? *)"
print("== 3/4 invoke + verify ==")
resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
pay = json.loads(resp["Payload"].read() or b"{}")
print("  invoke ->", json.dumps(pay, default=str)[:300])
assert not resp.get("FunctionError"), pay
d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/apac-leadlag.json")["Body"].read())
print("  series_days:", json.dumps(d.get("series_days")))
print("  US symbols loaded:", d.get("us_symbols_loaded"))
print("  status:", d.get("status"), "| proven leads:", len(d.get("proven_leads", [])))
for p in d.get("pairs", []):
    bh = p.get("best")
    print("   %-26s flow=%d overlap=%d best=%s" % (
        p["name"], p["flow_days"], p["overlap_days"],
        ("r=%.2f h=%dd n=%d sig=%s" % (bh["r"], bh["horizon"], bh["n"], bh["significant"]) if bh else "—")))
print("  READ:", d.get("read"))
assert sum(d.get("series_days", {}).values()) > 30, ("backfill too thin", d.get("series_days"))
assert len(d.get("us_symbols_loaded", [])) >= 3, ("FMP historical failed", d.get("us_symbols_loaded"))
assert any(p.get("best") for p in d.get("pairs", [])), "no correlations computed"
R["series_days"] = d.get("series_days"); R["us_loaded"] = d.get("us_symbols_loaded")
R["status"] = d.get("status"); R["proven_leads"] = d.get("proven_leads")
R["pairs"] = [{"name": p["name"], "flow_days": p["flow_days"], "overlap": p["overlap_days"], "best": p.get("best")} for p in d.get("pairs", [])]
R["read"] = d.get("read")
print("== 4/4 report ==")
os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/2766_leadlag.json", "w"), indent=1, ensure_ascii=False, default=str)
print("OPS 2766 COMPLETE — lead-lag engine live")
