import json, boto3, time
from datetime import datetime, timezone
lam=boto3.client("lambda",region_name="us-east-1"); ev=boto3.client("events",region_name="us-east-1")
s3=boto3.client("s3",region_name="us-east-1"); B="justhodl-dashboard-live"; FN="justhodl-retail-sentiment"
# schedule check
rules=ev.list_rule_names_by_target(TargetArn=lam.get_function_configuration(FunctionName=FN)["FunctionArn"]).get("RuleNames",[])
print("EventBridge rules targeting fn:", rules)
for r in rules:
    rd=ev.describe_rule(Name=r); print(f"  {r}: state={rd.get('State')} sched={rd.get('ScheduleExpression')}")
# invoke synchronously and capture tail log
print("\ninvoking RequestResponse (may take ~60-120s)...")
n0=datetime.now(timezone.utc)
try:
    resp=lam.invoke(FunctionName=FN, InvocationType="RequestResponse", LogType="Tail")
    import base64
    log=base64.b64decode(resp.get("LogResult","")).decode("utf-8","ignore")
    payload=resp["Payload"].read().decode("utf-8","ignore")
    print("status:",resp.get("StatusCode"),"funcError:",resp.get("FunctionError"))
    print("payload:",payload[:500])
    print("---- tail log ----")
    print("\n".join(log.splitlines()[-25:]))
except Exception as e:
    print("invoke err:",str(e)[:200])
# re-read feed freshness
time.sleep(3)
d=json.loads(s3.get_object(Bucket=B,Key="data/retail-sentiment.json")["Body"].read())
print("\nfeed generated_at now:", d.get("generated_at"), "| surges:", len((d.get("ranked") or {}).get("biggest_velocity_surges") or []))
print("updated_this_run:", d.get("generated_at","") >= n0.isoformat()[:10])
