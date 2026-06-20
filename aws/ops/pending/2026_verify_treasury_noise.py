"""ops 2026: wait for treasury-noise Active (created by deploy-lambdas), ensure schedule, invoke, verify."""
import boto3, json, time
REGION="us-east-1"; FN="justhodl-treasury-noise"; B="justhodl-dashboard-live"
lam=boto3.client("lambda",REGION); events=boto3.client("events",REGION); s3=boto3.client("s3",REGION)
for _ in range(40):
    try:
        c=lam.get_function(FunctionName=FN)["Configuration"]
        if c.get("State")=="Active" and c.get("LastUpdateStatus")=="Successful": break
    except Exception as e: print("waiting…",str(e)[:50])
    time.sleep(4)
print("state:",c.get("State"),c.get("LastUpdateStatus"),"mem",c.get("MemorySize"),"timeout",c.get("Timeout"))
arn=c["FunctionArn"]
# ensure schedule exists
rule="justhodl-treasury-noise-daily"
rarn=events.put_rule(Name=rule,ScheduleExpression="cron(30 13 ? * TUE-SAT *)",State="ENABLED",Description="daily treasury-noise")["RuleArn"]
try: lam.add_permission(FunctionName=FN,StatementId="evt-treasury-noise",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=rarn)
except lam.exceptions.ResourceConflictException: pass
events.put_targets(Rule=rule,Targets=[{"Id":"1","Arn":arn}]); print("scheduled")
print("invoking…")
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse")
print("status:",r["StatusCode"]," payload:",r["Payload"].read().decode()[:450])
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/treasury-noise.json")["Body"].read())
print("\nok:",d.get("ok"),"stress:",d.get("treasury_stress"),"regime:",d.get("regime"))
print("curve_noise:",d.get("curve_noise_bps"),"bps | pct:",d.get("curve_noise_pctile"),"| z:",d.get("curve_noise_z"))
print("bill-SOFR:",d.get("bill_sofr_spread_bps"),"bps | funding stress pct:",d.get("funding_stress_pctile"))
print("history pts:",d.get("history_points"),"| as_of:",d.get("as_of_date"))
print("highest noise days:",d.get("highest_noise_days"))
print("DONE 2026")
