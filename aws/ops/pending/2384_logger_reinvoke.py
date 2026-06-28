import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=120,retries={"max_attempts":0}))
ddb=boto3.resource("dynamodb","us-east-1"); s3=boto3.client("s3","us-east-1")
cfg=lam.get_function_configuration(FunctionName="justhodl-signal-logger")
print("logger LastModified:",cfg["LastModified"])
# confirm the surface file is readable at the exact key the logger uses
try:
    co=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/crypto-options-surface.json")["Body"].read())
    print("surface file rr_25d:",((co.get("btc") or {}).get("headline_30d") or {}).get("rr_25d"))
except Exception as e: print("surface read err",str(e)[:60])
r=lam.invoke(FunctionName="justhodl-signal-logger",InvocationType="RequestResponse",Payload=b"{}")
print("resp:",r["Payload"].read().decode()[:90])
time.sleep(4)
t=ddb.Table("justhodl-signals")
resp=t.scan(FilterExpression="signal_type = :s",ExpressionAttributeValues={":s":"crypto_options_rr"})
items=sorted(resp.get("Items",[]),key=lambda x:x.get("logged_epoch",0),reverse=True)[:2]
print("crypto_options_rr found:",len(resp.get("Items",[])))
for it in items:
    print("  ",it.get("logged_at"),"|",it.get("predicted_direction"),"| val",it.get("signal_value"),"| baseline",it.get("baseline_price"),"| windows",it.get("check_windows"))
print("DONE 2384")
