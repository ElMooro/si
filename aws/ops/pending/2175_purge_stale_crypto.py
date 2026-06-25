import boto3, json, time
from botocore.config import Config
ddb=boto3.resource("dynamodb","us-east-1"); t=ddb.Table("justhodl-signals")
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=300,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
# delete the stale signal logged during the buffer bug
t.delete_item(Key={"signal_id":"cma200-UP#TON#2025-11-03"})
print("purged stale signal cma200-UP#TON#2025-11-03")
# re-run scorecard on the clean ledger
for _ in range(30):
    c=lam.get_function(FunctionName="justhodl-crypto-scorecard")["Configuration"]
    if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    time.sleep(3)
lam.invoke(FunctionName="justhodl-crypto-scorecard",InvocationType="RequestResponse")
sc=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/crypto-scorecard.json")["Body"].read())
print("clean scorecard: signals",sc.get("n_signals"),"graded",sc.get("n_graded_primary"),"pending",sc.get("n_pending"),"by_type",json.dumps(sc.get("by_type",{})))
print("DONE 2175")
