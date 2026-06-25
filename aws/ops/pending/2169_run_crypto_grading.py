import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=400,retries={"max_attempts":0}))
ev=boto3.client("events","us-east-1"); s3=boto3.client("s3","us-east-1")
ACC="857687956942"
# 1) run crypto-ma200 to LOG signals
for _ in range(30):
    c=lam.get_function(FunctionName="justhodl-crypto-ma200")["Configuration"]
    if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    time.sleep(3)
lam.invoke(FunctionName="justhodl-crypto-ma200",InvocationType="RequestResponse")
cm=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/crypto-ma200.json")["Body"].read())
print("crypto-ma200 signals_logged:",cm.get("signals_logged"),"| counts:",json.dumps(cm.get("counts",{})))
# 2) ensure crypto-scorecard scheduled
SC="justhodl-crypto-scorecard"
try: ev.describe_rule(Name="justhodl-crypto-scorecard-daily"); print("scorecard rule present")
except Exception:
    rule="justhodl-crypto-scorecard-daily"; arn=lam.get_function(FunctionName=SC)["Configuration"]["FunctionArn"]
    ev.put_rule(Name=rule,ScheduleExpression="cron(15 1 * * ? *)",State="ENABLED")
    try: lam.add_permission(FunctionName=SC,StatementId="ev-"+rule,Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=f"arn:aws:events:us-east-1:{ACC}:rule/{rule}")
    except Exception: pass
    ev.put_targets(Rule=rule,Targets=[{"Id":"1","Arn":arn}]); print("scorecard rule created")
# 3) run crypto-scorecard
for _ in range(30):
    c=lam.get_function(FunctionName=SC)["Configuration"]
    if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    time.sleep(3)
lam.invoke(FunctionName=SC,InvocationType="RequestResponse")
sc=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/crypto-scorecard.json")["Body"].read())
print("\ncrypto-scorecard:")
print("  n_signals:",sc.get("n_signals"),"graded_primary:",sc.get("n_graded_primary"),"pending:",sc.get("n_pending"))
print("  by_type:",json.dumps(sc.get("by_type",{})))
print("  note:",sc.get("note","")[:90])
print("DONE 2169")
