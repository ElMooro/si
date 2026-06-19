"""ops 1941 — Create justhodl-risk-regime (new fn) and verify.

deploy-lambdas.yml silently no-ops on brand-new function dirs, so create via
boto3: create_function (bundling massive.py), EventBridge daily rule, seed run
(no transition alert), then a real invoke + verify the composite RORO score.
"""
import io, json, time, zipfile, os
import boto3

REGION = "us-east-1"
ACCOUNT = "857687956942"
ROLE = f"arn:aws:iam::{ACCOUNT}:role/lambda-execution-role"
FN = "justhodl-risk-regime"
ROOT = os.getcwd()

lam = boto3.client("lambda", REGION)
events = boto3.client("events", REGION)
s3 = boto3.client("s3", REGION)
BUCKET = "justhodl-dashboard-live"

def zb(files):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for arc, p in files.items():
            z.write(p, arc)
    buf.seek(0); return buf.read()

cfg = json.load(open(f"{ROOT}/aws/lambdas/{FN}/config.json"))
pkg = zb({
    "lambda_function.py": f"{ROOT}/aws/lambdas/{FN}/source/lambda_function.py",
    "massive.py": f"{ROOT}/aws/shared/massive.py",
})

# create or update
exists = True
try:
    lam.get_function(FunctionName=FN)
except lam.exceptions.ResourceNotFoundException:
    exists = False

if not exists:
    lam.create_function(
        FunctionName=FN, Runtime="python3.12", Role=ROLE,
        Handler="lambda_function.lambda_handler", Code={"ZipFile": pkg},
        Timeout=cfg["timeout"], MemorySize=cfg["memory"],
        Environment={"Variables": cfg.get("environment", {})},
        Description="Authoritative cross-asset RORO synthesizer (Massive FX+options + FRED VIX/credit)",
        Publish=False)
    print("created", FN)
else:
    for i in range(24):
        try:
            lam.update_function_code(FunctionName=FN, ZipFile=pkg, Publish=False); break
        except lam.exceptions.ResourceConflictException:
            time.sleep(5)
    lam.update_function_configuration(FunctionName=FN, Timeout=cfg["timeout"],
        MemorySize=cfg["memory"], Environment={"Variables": cfg.get("environment", {})})
    print("updated", FN)

# wait active
for _ in range(40):
    c = lam.get_function_configuration(FunctionName=FN)
    if c["State"] == "Active" and c.get("LastUpdateStatus") != "InProgress":
        break
    time.sleep(3)

# schedule
sch = cfg["schedule"]
events.put_rule(Name=sch["name"], ScheduleExpression=sch["expression"],
                State="ENABLED", Description=sch["description"])
rule_arn = events.describe_rule(Name=sch["name"])["Arn"]
try:
    lam.add_permission(FunctionName=FN, StatementId=f"{sch['name']}-invoke",
                       Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                       SourceArn=rule_arn)
except lam.exceptions.ResourceConflictException:
    pass
fn_arn = lam.get_function_configuration(FunctionName=FN)["FunctionArn"]
events.put_targets(Rule=sch["name"], Targets=[{"Id": "1", "Arn": fn_arn}])
print("scheduled:", sch["expression"])

# seed run (suppress transition alert), then verify
lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
           Payload=json.dumps({"seed": True}).encode())
time.sleep(2)
r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse")
print("invoke:", str(json.loads(r["Payload"].read()))[:300])

d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/risk-regime.json")["Body"].read())
print("\n=== RISK REGIME ===")
print("score:", d["risk_regime_score"], "| regime:", d["risk_regime"])
print("posture:", d["posture"])
print("blocks_used:", d["blocks_used"])
print("components:")
for b, m in d["components"].items():
    print(f"  {b}: {json.dumps({k:v for k,v in m.items() if k!='tells'})[:160]}")
print("tells:")
for t in d["tells"]:
    print("  •", t)
print("\nDONE 1941")
