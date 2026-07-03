"""ops 2773 — GEX ENGINE live (Phase 1 of Options/GEX desk).
Brand-new dir => boto3 create_function (POLYGON_KEY from a Polygon-using function),
EventBridge intraday cron (every :00/:30, 13:00-20:30 UTC, Mon-Fri). Invoke, verify
SPY gamma profile (net GEX, flip, call/put walls, max pain) + multi-name coverage.
Report: 2773_gex_engine.json.
"""
import os, io, json, time, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
REGION, BUCKET, ACCT, FN = "us-east-1", "justhodl-dashboard-live", "857687956942", "justhodl-gex-desk"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=600, retries={"max_attempts": 1}))
ev = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 2773, "ts": datetime.now(timezone.utc).isoformat()}
def zip_fn(fn):
    src = "aws/lambdas/%s/source" % fn; buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(src):
            if "__pycache__" in root: continue
            for f in files: z.write(os.path.join(root, f), os.path.relpath(os.path.join(root, f), src))
        for f in sorted(os.listdir("aws/shared")):
            if f.endswith(".py"): z.write(os.path.join("aws/shared", f), f)
    return buf.getvalue()
def wait_ok(fn, b=240):
    t0 = time.time()
    while time.time() - t0 < b:
        try:
            c = lam.get_function_configuration(FunctionName=fn)
            if c.get("State") == "Active" and c.get("LastUpdateStatus") in (None, "Successful"): return
        except ClientError: pass
        time.sleep(5)
print("settling 15s…"); time.sleep(15)
print("== 1/4 create/update lambda ==")
base = lam.get_function_configuration(FunctionName="justhodl-theme-rotation")
role, runtime = base["Role"], base["Runtime"]
pk = None
for donor in ("justhodl-theme-rotation", "justhodl-portfolio-risk", "justhodl-khalid-metrics"):
    env = (lam.get_function_configuration(FunctionName=donor).get("Environment", {}) or {}).get("Variables", {}) or {}
    pk = env.get("POLYGON_KEY") or env.get("POLY_KEY") or env.get("POLYGON_API_KEY")
    if pk: break
assert pk, "no Polygon key"
code = zip_fn(FN)
try:
    lam.get_function(FunctionName=FN); exists = True
except ClientError:
    exists = False
envvars = {"POLYGON_KEY": pk, "POLY_KEY": pk, "TZ": "UTC"}
if exists:
    wait_ok(FN); lam.update_function_code(FunctionName=FN, ZipFile=code); wait_ok(FN)
    lam.update_function_configuration(FunctionName=FN, Timeout=600, MemorySize=1024, Runtime=runtime,
                                      Handler="lambda_function.lambda_handler", Environment={"Variables": envvars})
    wait_ok(FN); print("  updated")
else:
    for i in range(6):
        try:
            lam.create_function(FunctionName=FN, Runtime=runtime, Role=role, Handler="lambda_function.lambda_handler",
                                Code={"ZipFile": code}, Timeout=600, MemorySize=1024, Publish=True,
                                Environment={"Variables": envvars}); break
        except ClientError as e:
            if "role" in str(e).lower() and i < 5: time.sleep(10); continue
            raise
    wait_ok(FN); print("  created")
print("== 2/4 EventBridge intraday cron ==")
rule = "justhodl-gex-desk-intraday"
ev.put_rule(Name=rule, ScheduleExpression="cron(0,30 13-20 ? * MON-FRI *)", State="ENABLED",
            Description="GEX desk intraday (every :00/:30, market hours)")
arn = lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]
try:
    lam.add_permission(FunctionName=FN, StatementId="gex-eventbridge", Action="lambda:InvokeFunction",
                       Principal="events.amazonaws.com", SourceArn="arn:aws:events:%s:%s:rule/%s" % (REGION, ACCT, rule))
except ClientError as e:
    if "ResourceConflict" not in str(e): raise
ev.put_targets(Rule=rule, Targets=[{"Id": "gex", "Arn": arn}])
assert ev.describe_rule(Name=rule)["State"] == "ENABLED"
R["schedule"] = "cron(0,30 13-20 ? * MON-FRI *)"
print("== 3/4 invoke + verify ==")
resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
pay = json.loads(resp["Payload"].read() or b"{}")
print("  invoke ->", json.dumps(pay, default=str)[:300])
assert not resp.get("FunctionError"), pay
assert pay.get("ok"), pay
d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/gex-desk.json")["Body"].read())
names = d.get("names", {})
spy = names.get("SPY", {})
print("  n_live:", d.get("n_live"), "| index net GEX:", d.get("index_net_gex_bn"), "Bn")
print("  SPY: spot=%s net_gex=%sBn flip=%s call_wall=%s put_wall=%s max_pain=%s 0DTE=%sBn regime=%s contracts=%s" % (
    spy.get("spot"), spy.get("net_gex_bn"), spy.get("gamma_flip"), spy.get("call_wall"),
    spy.get("put_wall"), spy.get("max_pain"), spy.get("zero_dte_gex_bn"), spy.get("regime"), spy.get("n_contracts")))
for s in ("NVDA", "QQQ", "TSLA", "AAPL"):
    v = names.get(s, {})
    print("  %-5s net_gex=%sBn flip=%s wall(c/p)=%s/%s regime=%s" % (
        s, v.get("net_gex_bn"), v.get("gamma_flip"), v.get("call_wall"), v.get("put_wall"), v.get("regime")))
print("  READ:", d.get("read"))
assert d.get("n_live", 0) >= 10, ("too few names live", d.get("n_live"))
assert spy.get("status") == "LIVE" and spy.get("net_gex_bn") is not None, ("SPY GEX failed", spy)
assert spy.get("call_wall") and spy.get("put_wall") and spy.get("max_pain"), "SPY levels missing"
R["n_live"] = d.get("n_live"); R["index_net_gex_bn"] = d.get("index_net_gex_bn")
R["spy"] = {k: spy.get(k) for k in ("spot", "net_gex_bn", "gamma_flip", "dist_to_flip_pct", "call_wall", "put_wall", "max_pain", "zero_dte_gex_bn", "regime", "n_contracts")}
R["sample"] = {s: {"net_gex_bn": names.get(s, {}).get("net_gex_bn"), "flip": names.get(s, {}).get("gamma_flip"),
                   "regime": names.get(s, {}).get("regime")} for s in ("NVDA", "QQQ", "TSLA", "AAPL", "MU", "PLTR")}
R["read"] = d.get("read")
print("== 4/4 report ==")
os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/2773_gex_engine.json", "w"), indent=1, default=str)
print("OPS 2773 COMPLETE — GEX engine live on Polygon options")
