import boto3, json, time
lam=boto3.client("lambda","us-east-1"); ddb=boto3.resource("dynamodb","us-east-1")
r=lam.invoke(FunctionName="justhodl-signal-logger",InvocationType="RequestResponse",Payload=b"{}")
print("logger FunctionError:",r.get("FunctionError"))
body=r["Payload"].read().decode()
print("resp head:",body[:140])
time.sleep(2)
# scan signals table for crypto_dvol_buyfear (recent)
t=ddb.Table("justhodl-signals")
resp=t.scan(FilterExpression="signal_type = :s",ExpressionAttributeValues={":s":"crypto_dvol_buyfear"})
items=sorted(resp.get("Items",[]),key=lambda x:x.get("logged_epoch",0),reverse=True)[:3]
print("crypto_dvol_buyfear signals found:",len(resp.get("Items",[])))
for it in items:
    print("  ",it.get("logged_at"),"| dir:",it.get("predicted_direction"),"| val:",it.get("signal_value"),"| baseline:",it.get("baseline_price"),"| windows:",it.get("check_windows"),"| status:",it.get("status"))
print("DONE 2369")
