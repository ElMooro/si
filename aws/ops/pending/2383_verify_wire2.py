import boto3, json, time
from botocore.config import Config
cfg=Config(read_timeout=180, connect_timeout=15, retries={"max_attempts":0})
lam=boto3.client("lambda","us-east-1",config=cfg); s3=boto3.client("s3","us-east-1"); ddb=boto3.resource("dynamodb","us-east-1")
# 1) read crypto-intel.json (already written by the prior/ scheduled run) for surface
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="crypto-intel.json")["Body"].read())
surf=((d.get("implied_vol") or {}).get("surface") or {})
print("crypto-intel implied_vol.surface:",json.dumps(surf) if surf else "MISSING")
# 2) invoke logger (longer client timeout) + scan ledger
r=lam.invoke(FunctionName="justhodl-signal-logger",InvocationType="RequestResponse",Payload=b"{}")
print("logger resp:",r["Payload"].read().decode()[:90])
time.sleep(3)
t=ddb.Table("justhodl-signals")
resp=t.scan(FilterExpression="signal_type = :s",ExpressionAttributeValues={":s":"crypto_options_rr"})
items=sorted(resp.get("Items",[]),key=lambda x:x.get("logged_epoch",0),reverse=True)[:2]
print("crypto_options_rr signals:",len(resp.get("Items",[])))
for it in items:
    print("  ",it.get("logged_at"),"|",it.get("predicted_direction"),"| val",it.get("signal_value"),"| baseline",it.get("baseline_price"),"| windows",it.get("check_windows"),"| status",it.get("status"))
print("DONE 2383")
