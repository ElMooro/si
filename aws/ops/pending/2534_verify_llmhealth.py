import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=120,retries={"max_attempts":0}))
events=boto3.client("events","us-east-1"); s3=boto3.client("s3","us-east-1")
# schedule rule present?
rules=[r["Name"]+" "+str(r.get("ScheduleExpression")) for r in events.list_rules(NamePrefix="justhodl-llm-health").get("Rules",[])]
print("schedule rules:", rules or "NONE — needs scheduling")
# invoke
r=lam.invoke(FunctionName="justhodl-llm-health",InvocationType="RequestResponse",Payload=b"{}")
print("invoke err:",r.get("FunctionError")); time.sleep(3)
h=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/llm-health.json")["Body"].read())
print("status:",h.get("status"),"| redundancy:",h.get("redundancy"))
print("headline:",h.get("headline"))
print("billing_action_needed:",h.get("billing_action_needed"))
for p in h.get("providers",[]): print("  provider:",p.get("provider"),"ok=",p.get("ok"),p.get("error","")[:60] if not p.get("ok") else "")
print("degraded_outputs:",[c.get("feed") or c.get("name") for c in (h.get("degraded_outputs") or [])])
print("DONE 2534")
