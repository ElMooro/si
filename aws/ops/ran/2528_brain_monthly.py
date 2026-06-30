import boto3, json, time
from botocore.config import Config
events=boto3.client("events","us-east-1"); lam=boto3.client("lambda","us-east-1")
s3=boto3.client("s3","us-east-1"); FN="justhodl-brain-sync"
arn=lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]
NEW="justhodl-brain-sync-monthly"; OLD="justhodl-brain-sync-15min"
print("=== existing brain-sync rules ===")
for r in events.list_rules(NamePrefix="justhodl-brain-sync").get("Rules",[]):
    print("  ",r["Name"],r.get("ScheduleExpression"),r.get("State"))
# monthly rule + target + permission
events.put_rule(Name=NEW, ScheduleExpression="cron(0 9 1 * ? *)", State="ENABLED",
                Description="Brain monthly distillation (1st 09:00 UTC)")
try:
    lam.add_permission(FunctionName=FN, StatementId="brain-monthly-evt",
        Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
        SourceArn=f"arn:aws:events:us-east-1:857687956942:rule/{NEW}")
except lam.exceptions.ResourceConflictException: pass
events.put_targets(Rule=NEW, Targets=[{"Id":"brain-sync","Arn":arn}])
print("monthly rule ENABLED -> cron(0 9 1 * ? *)")
# retire the 15-min rule
try:
    t=events.list_targets_by_rule(Rule=OLD).get("Targets",[])
    if t: events.remove_targets(Rule=OLD, Ids=[x["Id"] for x in t])
    events.delete_rule(Name=OLD); print("old 15-min rule deleted")
except Exception as e: print("old rule cleanup:",str(e)[:80])
print("=== rules now ===")
for r in events.list_rules(NamePrefix="justhodl-brain-sync").get("Rules",[]):
    print("  ",r["Name"],r.get("ScheduleExpression"),r.get("State"))
# SEED NOW (force) — Anthropic back, directive empty
laml=boto3.client("lambda","us-east-1",config=Config(read_timeout=290,retries={"max_attempts":0}))
print("\nseeding directive now (force=true)...")
r=laml.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=json.dumps({"force":True}).encode())
print("seed invoke err:",r.get("FunctionError")); time.sleep(3)
br=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/brain.json")["Body"].read())
d=br.get("directive") or {}; rr=br.get("regime_read") or {}
print("directive populated:",bool(d))
if d:
    print("  risk_posture:",d.get("risk_posture"))
    print("  sector_tilts:",json.dumps(d.get("sector_tilts") or {})[:260])
    print("  themes:",(d.get("themes") or [])[:5])
    print("  watched_tickers:",(d.get("watched_tickers") or [])[:10])
print("regime_read:",rr.get("regime") if isinstance(rr,dict) else None)
print("last_distill_at:",br.get("last_distill_at"),"| cadence:",br.get("distill_cadence"))
print("DONE 2528")
