"""ops 1943 — fix risk-regime EventBridge schedule (needs ? for day-of-month when
day-of-week set) + seed + verify composite RORO."""
import json, time, boto3
REGION = "us-east-1"; FN = "justhodl-risk-regime"
lam = boto3.client("lambda", REGION); events = boto3.client("events", REGION)
s3 = boto3.client("s3", REGION); BUCKET = "justhodl-dashboard-live"
cfg = json.load(open(f"aws/lambdas/{FN}/config.json")); sch = cfg["schedule"]
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
print("tells:", d["tells"])
print("DONE 1943")
