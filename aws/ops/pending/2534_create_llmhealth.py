import boto3, json, io, zipfile, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=200,retries={"max_attempts":0}))
events=boto3.client("events","us-east-1"); s3=boto3.client("s3","us-east-1")
FN="justhodl-llm-health"; ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"
# env: inherit ANTHROPIC_KEY from brain-sync
bs_env=lam.get_function_configuration(FunctionName="justhodl-brain-sync").get("Environment",{}).get("Variables",{})
akey=bs_env.get("ANTHROPIC_KEY") or bs_env.get("ANTHROPIC_API_KEY","")
print("ANTHROPIC_KEY inherited:", bool(akey))
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    z.writestr("lambda_function.py", open("aws/lambdas/justhodl-llm-health/source/lambda_function.py").read())
code=buf.getvalue()
try:
    lam.get_function(FunctionName=FN); exists=True
except lam.exceptions.ResourceNotFoundException: exists=False
if exists:
    lam.update_function_code(FunctionName=FN, ZipFile=code)
    print("updated existing")
else:
    lam.create_function(FunctionName=FN, Runtime="python3.12", Role=ROLE,
        Handler="lambda_function.lambda_handler", Code={"ZipFile":code},
        Timeout=60, MemorySize=256,
        Environment={"Variables":{"ANTHROPIC_KEY":akey}},
        Description="LLM provider + AI-output health monitor")
    print("created function")
for _ in range(24):
    if lam.get_function_configuration(FunctionName=FN).get("LastUpdateStatus")=="Successful": break
    time.sleep(5)
# schedule rate(6 hours)
RULE="justhodl-llm-health-6h"
events.put_rule(Name=RULE, ScheduleExpression="rate(6 hours)", State="ENABLED",
                Description="LLM health every 6h")
arn=lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]
try:
    lam.add_permission(FunctionName=FN, StatementId="llmhealth-evt",
        Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
        SourceArn=f"arn:aws:events:us-east-1:857687956942:rule/{RULE}")
except lam.exceptions.ResourceConflictException: pass
events.put_targets(Rule=RULE, Targets=[{"Id":"llm-health","Arn":arn}])
print("scheduled rate(6 hours)")
# invoke now
r=lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
print("invoke err:", r.get("FunctionError")); time.sleep(3)
h=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/llm-health.json")["Body"].read())
print("\n=== llm-health.json ===")
print("status:", h.get("status"))
print("headline:", h.get("headline"))
print("redundancy:", h.get("redundancy"))
for p in h.get("providers",[]): print("  ", p.get("provider"), "ok=",p.get("ok"), p.get("status") or "", (p.get("error") or "")[:70])
print("billing_action_needed:", h.get("billing_action_needed"))
print("ai_output_checks:", h.get("ai_output_checks"))
print("DONE 2534")
