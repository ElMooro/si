import boto3, json, time
lam=boto3.client("lambda","us-east-1"); ddb=boto3.resource("dynamodb","us-east-1"); s3=boto3.client("s3","us-east-1")
# diagnostic: confirm the block's inputs are present
try:
    cd=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/crypto-dvol.json")["Body"].read())
    print("dvol json btc:", json.dumps(cd.get("btc")))
except Exception as e: print("dvol read err:",str(e)[:80])
# confirm deployed logger code has the block
code=lam.get_function(FunctionName="justhodl-signal-logger")
print("logger last modified:", code["Configuration"]["LastModified"])
# re-invoke
r=lam.invoke(FunctionName="justhodl-signal-logger",InvocationType="RequestResponse",Payload=b"{}")
print("FunctionError:",r.get("FunctionError"),"| resp:",r["Payload"].read().decode()[:120])
time.sleep(3)
t=ddb.Table("justhodl-signals")
resp=t.scan(FilterExpression="signal_type = :s",ExpressionAttributeValues={":s":"crypto_dvol_buyfear"})
items=sorted(resp.get("Items",[]),key=lambda x:x.get("logged_epoch",0),reverse=True)[:3]
print("crypto_dvol_buyfear found:",len(resp.get("Items",[])))
for it in items:
    print("  ",it.get("logged_at"),"|",it.get("predicted_direction"),"| val",it.get("signal_value"),"| baseline",it.get("baseline_price"),"| windows",it.get("check_windows"),"| status",it.get("status"))
print("DONE 2370")
