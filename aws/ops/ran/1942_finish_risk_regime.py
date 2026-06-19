"""ops 1942 — Finish justhodl-risk-regime setup (1941 aborted on a config-update
ResourceConflict while the create was still settling). Idempotent: wait active,
set config with retry, ensure schedule, redeploy code+massive.py to be safe, seed,
verify composite RORO score.
"""
import io, json, time, zipfile, os
import boto3

REGION = "us-east-1"; ACCOUNT = "857687956942"
FN = "justhodl-risk-regime"; ROOT = os.getcwd()
lam = boto3.client("lambda", REGION); events = boto3.client("events", REGION)
s3 = boto3.client("s3", REGION); BUCKET = "justhodl-dashboard-live"

def zb(files):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for arc, p in files.items():
            z.write(p, arc)
    buf.seek(0); return buf.read()

def wait_active():
    for _ in range(50):
        c = lam.get_function_configuration(FunctionName=FN)
        if c["State"] == "Active" and c.get("LastUpdateStatus") != "InProgress":
            return c
        time.sleep(3)
    return lam.get_function_configuration(FunctionName=FN)

def retry(fn, *a, **k):
    for i in range(24):
        try:
            return fn(*a, **k)
        except lam.exceptions.ResourceConflictException:
            time.sleep(5)
    raise RuntimeError("conflict timeout")

cfg = json.load(open(f"{ROOT}/aws/lambdas/{FN}/config.json"))
wait_active()
# redeploy code (idempotent) bundling massive.py
retry(lam.update_function_code, FunctionName=FN, ZipFile=zb({
    "lambda_function.py": f"{ROOT}/aws/lambdas/{FN}/source/lambda_function.py",
    "massive.py": f"{ROOT}/aws/shared/massive.py"}), Publish=False)
wait_active()
retry(lam.update_function_configuration, FunctionName=FN, Timeout=cfg["timeout"],
      MemorySize=cfg["memory"], Environment={"Variables": cfg.get("environment", {})})
wait_active()
print("config set: timeout", cfg["timeout"], "mem", cfg["memory"], "env", list(cfg.get("environment", {}).keys()))

# schedule (idempotent)
sch = cfg["schedule"]
events.put_rule(Name=sch["name"], ScheduleExpression=sch["expression"], State="ENABLED",
                Description=sch["description"])
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

# seed (suppress alert) then verify
lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=json.dumps({"seed": True}).encode())
time.sleep(2)
r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse")
print("invoke:", str(json.loads(r["Payload"].read()))[:300])

d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/risk-regime.json")["Body"].read())
print("\n=== RISK REGIME ===")
print("score:", d["risk_regime_score"], "| regime:", d["risk_regime"], "| posture:", d["posture"])
print("blocks_used:", d["blocks_used"])
for b, m in d["components"].items():
    print(f"  {b}: {json.dumps({k:v for k,v in m.items() if k!='tells'})[:170]}")
print("tells:")
for t in d["tells"]:
    print("  •", t)
print("\nDONE 1942")
