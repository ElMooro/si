"""ops 2551 — create fiat-peg-monitor Lambda + verify all three builds:
(1) credit-stress ICE BofA YTW, (2) tic-flows Bahamas/Caribbean, (3) fiat-peg board."""
import boto3, json, io, zipfile, time
from botocore.config import Config

lam = boto3.client("lambda", "us-east-1", config=Config(read_timeout=200, retries={"max_attempts": 0}))
events = boto3.client("events", "us-east-1")
s3 = boto3.client("s3", "us-east-1")
B = "justhodl-dashboard-live"

def rd(k):
    try: return json.loads(s3.get_object(Bucket=B, Key=k)["Body"].read())
    except Exception as e: return {"_err": str(e)[:60]}

# ── create fiat-peg-monitor ──
FN = "justhodl-fiat-peg-monitor"
SRC = "aws/lambdas/justhodl-fiat-peg-monitor/source/lambda_function.py"
buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
    z.writestr("lambda_function.py", open(SRC).read())
code = buf.getvalue()
ENV = {"Variables": {"FRED_KEY": "2f057499936072679d8843d7fce99989",
                     "POLYGON_KEY": "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"}}
try:
    lam.get_function(FunctionName=FN)
    lam.update_function_code(FunctionName=FN, ZipFile=code); print("updated fiat-peg code")
except lam.exceptions.ResourceNotFoundException:
    lam.create_function(FunctionName=FN, Runtime="python3.12",
        Role="arn:aws:iam::857687956942:role/lambda-execution-role",
        Handler="lambda_function.lambda_handler", Code={"ZipFile": code},
        Timeout=60, MemorySize=256, Environment=ENV,
        Description="Currency-peg break / devaluation-pressure board")
    print("created fiat-peg")
for _ in range(24):
    if lam.get_function_configuration(FunctionName=FN).get("LastUpdateStatus") == "Successful": break
    time.sleep(5)
RULE = "justhodl-fiat-peg-monitor-4h"
events.put_rule(Name=RULE, ScheduleExpression="rate(4 hours)", State="ENABLED")
arn = lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]
try:
    lam.add_permission(FunctionName=FN, StatementId="fiatpeg-evt", Action="lambda:InvokeFunction",
                       Principal="events.amazonaws.com",
                       SourceArn=f"arn:aws:events:us-east-1:857687956942:rule/{RULE}")
except lam.exceptions.ResourceConflictException: pass
events.put_targets(Rule=RULE, Targets=[{"Id": "fp", "Arn": arn}])
print("scheduled rate(4h)")

# ── (3) invoke + verify fiat-peg ──
r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
print("\n[fiat-peg] err:", r.get("FunctionError")); time.sleep(2)
fp = rd("data/fiat-peg-monitor.json")
print("  headline:", fp.get("headline"))
for p in (fp.get("pegs") or []):
    print(f"    {p.get('ccy')}: spot {p.get('spot')} · pressure {p.get('pressure')} · {p.get('regime')}")

# ── (1) invoke + verify credit-stress YTW ──
r = lam.invoke(FunctionName="justhodl-credit-stress", InvocationType="RequestResponse", Payload=b"{}")
print("\n[credit-stress] err:", r.get("FunctionError")); time.sleep(2)
cs = rd("data/credit-stress.json")
print("  current_yields_pct:", cs.get("current_yields_pct"))
print("  ig_yield_curve_pct:", cs.get("ig_yield_curve_pct"))

# ── (2) invoke + verify tic-flows Bahamas/Caribbean ──
r = lam.invoke(FunctionName="justhodl-tic-flows", InvocationType="RequestResponse", Payload=b"{}")
print("\n[tic-flows] err:", r.get("FunctionError")); time.sleep(2)
tc = rd("data/tic-flows.json")
ind = tc.get("individual") or {}
print("  holders now:", list(ind))
for k in ("bahamas", "caribbean_banking"):
    print(f"    {k}:", ind.get(k))
print("\nDONE 2551")
