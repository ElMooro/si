import boto3, json, time
lam=boto3.client("lambda","us-east-1"); ddb=boto3.resource("dynamodb","us-east-1"); s3=boto3.client("s3","us-east-1")
# confirm dvol file readable at the key the logger uses
try:
    cd=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/crypto-dvol.json")["Body"].read())
    print("dvol pctile readable:",(cd.get("btc") or {}).get("pctile_1y"),"regime",(cd.get("btc") or {}).get("regime"))
except Exception as e: print("dvol read err:",str(e)[:60])
# re-invoke logger
r=lam.invoke(FunctionName="justhodl-signal-logger",InvocationType="RequestResponse",Payload=b"{}")
print("logger err:",r.get("FunctionError"),"| resp:",r["Payload"].read().decode()[:120])
time.sleep(3)
t=ddb.Table("justhodl-signals")
resp=t.scan(FilterExpression="signal_type = :s",ExpressionAttributeValues={":s":"crypto_dvol_buyfear"})
items=sorted(resp.get("Items",[]),key=lambda x:x.get("logged_epoch",0),reverse=True)[:3]
print("crypto_dvol_buyfear count:",len(resp.get("Items",[])))
for it in items:
    print("  ",it.get("logged_at"),"dir",it.get("predicted_direction"),"val",it.get("signal_value"),"baseline",it.get("baseline_price"),"windows",it.get("check_windows"))
print("DONE 2370")
