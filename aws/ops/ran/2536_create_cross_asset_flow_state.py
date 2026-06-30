"""ops 2536 — create justhodl-cross-asset-flow-state as a live Lambda.

Brand-new function dir => deploy-lambdas.yml silently no-ops, so we create it
here via standalone boto3 (proven pattern from ops 2534). No env vars (no LLM,
pure S3-feed synthesis). Schedule rate(2 hours). Invoke once and print the feed.
"""
import boto3, json, io, zipfile, time
from botocore.config import Config

lam = boto3.client("lambda", "us-east-1", config=Config(read_timeout=200, retries={"max_attempts": 0}))
events = boto3.client("events", "us-east-1")
s3 = boto3.client("s3", "us-east-1")

FN = "justhodl-cross-asset-flow-state"
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"
SRC = "aws/lambdas/justhodl-cross-asset-flow-state/source/lambda_function.py"

buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
    z.writestr("lambda_function.py", open(SRC).read())
code = buf.getvalue()

try:
    lam.get_function(FunctionName=FN); exists = True
except lam.exceptions.ResourceNotFoundException:
    exists = False

if exists:
    lam.update_function_code(FunctionName=FN, ZipFile=code)
    print("updated existing function")
else:
    lam.create_function(
        FunctionName=FN, Runtime="python3.12", Role=ROLE,
        Handler="lambda_function.lambda_handler", Code={"ZipFile": code},
        Timeout=60, MemorySize=256,
        Description="Unified cross-asset flow synthesizer -> data/cross-asset-flow-state.json")
    print("created function")

for _ in range(24):
    if lam.get_function_configuration(FunctionName=FN).get("LastUpdateStatus") == "Successful":
        break
    time.sleep(5)

# schedule rate(2 hours)
RULE = "justhodl-cross-asset-flow-state-2h"
events.put_rule(Name=RULE, ScheduleExpression="rate(2 hours)", State="ENABLED",
                Description="Synthesize cross-asset flow every 2h")
arn = lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]
try:
    lam.add_permission(FunctionName=FN, StatementId="xaflow-evt",
                       Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                       SourceArn=f"arn:aws:events:us-east-1:857687956942:rule/{RULE}")
except lam.exceptions.ResourceConflictException:
    pass
events.put_targets(Rule=RULE, Targets=[{"Id": "xa-flow", "Arn": arn}])
print("scheduled rate(2 hours)")

# invoke now
r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
print("invoke err:", r.get("FunctionError"))
print("invoke payload:", r["Payload"].read().decode()[:300])
time.sleep(3)

out = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/cross-asset-flow-state.json")["Body"].read())
print("\n=== cross-asset-flow-state.json ===")
print("headline:", out.get("headline"))
print("risk_regime_score:", out.get("risk_regime_score"), "| regime:", out.get("regime"), "| posture:", out.get("posture"))
print("asset_class_rotation keys:", list((out.get("asset_class_rotation") or {}).keys())[:8])
print("hard_assets_and_dollar:", json.dumps(out.get("hard_assets_and_dollar"))[:200])
print("dollar_fx_carry:", json.dumps(out.get("dollar_fx_carry"))[:200])
print("foreign_flows:", json.dumps(out.get("foreign_flows"))[:200])
dp = out.get("dark_pool") or {}
print("dark_pool acc/dist counts:", dp.get("accumulation_count"), "/", dp.get("distribution_count"),
      "| top_distribution:", [x.get("ticker") if isinstance(x, dict) else x for x in (dp.get("top_distribution") or [])][:5])
print("sources:", out.get("sources"))
print("DONE 2536")
