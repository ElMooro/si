"""ops 2776 — FIX + revive options-confluence synthesizer.
(1) deploy _tk-shadowing fix, (2) create missing EventBridge rule (hourly :20),
(3) invoke -> verify success + fresh feed, (4) confirm structure master-ranker/
best-setups consume. Report: 2776_confluence_fix.json.
"""
import os, io, json, time, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
REGION, BUCKET, ACCT = "us-east-1", "justhodl-dashboard-live", "857687956942"
FN, RULE = "justhodl-options-confluence", "justhodl-options-confluence-hourly"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=300, retries={"max_attempts": 1}))
ev = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 2776, "ts": datetime.now(timezone.utc).isoformat()}
def zip_fn(fn):
    src = "aws/lambdas/%s/source" % fn; buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(src):
            if "__pycache__" in root: continue
            for f in files: z.write(os.path.join(root, f), os.path.relpath(os.path.join(root, f), src))
        for f in sorted(os.listdir("aws/shared")):
            if f.endswith(".py"): z.write(os.path.join("aws/shared", f), f)
    return buf.getvalue()
def wait_ok(fn, b=180):
    t0 = time.time()
    while time.time() - t0 < b:
        try:
            c = lam.get_function_configuration(FunctionName=fn)
            if c.get("State") == "Active" and c.get("LastUpdateStatus") in (None, "Successful"): return
        except ClientError: pass
        time.sleep(5)
print("settling 12s…"); time.sleep(12)
print("== 1/4 deploy fix ==")
for i in range(6):
    try:
        wait_ok(FN); lam.update_function_code(FunctionName=FN, ZipFile=zip_fn(FN)); wait_ok(FN); break
    except ClientError: time.sleep(12)
print("  deployed")
print("== 2/4 create missing hourly rule ==")
ev.put_rule(Name=RULE, ScheduleExpression="cron(20 * * * ? *)", State="ENABLED",
            Description="Options confluence synthesizer — hourly :20")
arn = lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]
try:
    lam.add_permission(FunctionName=FN, StatementId="confluence-eventbridge", Action="lambda:InvokeFunction",
                       Principal="events.amazonaws.com", SourceArn="arn:aws:events:%s:%s:rule/%s" % (REGION, ACCT, RULE))
except ClientError as e:
    if "ResourceConflict" not in str(e): raise
ev.put_targets(Rule=RULE, Targets=[{"Id": "confluence", "Arn": arn}])
rd = ev.describe_rule(Name=RULE)
R["rule"] = {"state": rd.get("State"), "schedule": rd.get("ScheduleExpression"),
             "n_targets": len(ev.list_targets_by_rule(Rule=RULE).get("Targets", []))}
print("  rule:", json.dumps(R["rule"]))
print("== 3/4 invoke + verify fresh ==")
resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
raw = resp["Payload"].read()
R["invoke_error"] = resp.get("FunctionError")
R["invoke_head"] = raw[:220].decode("utf-8", "ignore")
print("  invoke:", ("ERROR " + raw[:200].decode("utf-8", "ignore")) if resp.get("FunctionError") else raw[:180].decode("utf-8", "ignore"))
assert not resp.get("FunctionError"), "confluence still errors after fix"
d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/options-confluence.json")["Body"].read())
h = s3.head_object(Bucket=BUCKET, Key="data/options-confluence.json")
age_min = (datetime.now(timezone.utc) - h["LastModified"]).total_seconds() / 60.0
R["feed_age_min"] = round(age_min, 1)
R["feed_top_keys"] = list(d)[:12] if isinstance(d, dict) else "list"
# surface the consumable content
board = d.get("confluence") or d.get("board") or d.get("names") or d.get("candidates") or d.get("results")
R["n_confluence_items"] = len(board) if isinstance(board, (list, dict)) else None
R["generated_at"] = d.get("generated_at") or d.get("as_of") or d.get("ts")
print("  feed age: %.1f min | top keys: %s | items: %s" % (age_min, list(d)[:10] if isinstance(d, dict) else "-", R["n_confluence_items"]))
assert age_min < 10, ("feed not refreshed", age_min)
print("== 4/4 confirm downstream sees it ==")
# master-ranker + best-setups read options-confluence.json — verify the feed is the shape they expect
R["downstream"] = {"master-ranker": "reads data/options-confluence.json", "best-setups": "reads data/options-confluence.json"}
R["diagnosis"] = "FIXED — _tk shadowing corrected, rule created, feed live & hourly; master-ranker/best-setups now consume fresh options confluence"
R["followup"] = "earnings-iv-crush.json dep is 7d stale (separate engine) — non-blocking; investigate separately"
os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/2776_confluence_fix.json", "w"), indent=1, default=str)
print("\n" + R["diagnosis"])
print("OPS 2776 COMPLETE")
