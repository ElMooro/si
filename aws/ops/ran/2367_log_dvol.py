import boto3, json, time
lam=boto3.client("lambda","us-east-1"); ddb=boto3.resource("dynamodb","us-east-1")
r=lam.invoke(FunctionName="justhodl-signal-logger",InvocationType="RequestResponse",Payload=b"{}")
print("FunctionError:",r.get("FunctionError"))
print("resp:",r["Payload"].read().decode()[:200])
time.sleep(3)
# scan signals table for crypto_dvol (recent)
t=ddb.Table("justhodl-signals")
from boto3.dynamodb.conditions import Attr
resp=t.scan(FilterExpression=Attr("signal_type").eq("crypto_dvol"), Limit=400)
items=resp.get("Items",[])
print("crypto_dvol signals in ledger:",len(items))
if items:
    items.sort(key=lambda x:str(x.get("logged_at","")),reverse=True)
    s=items[0]
    print("  latest: dir",s.get("predicted_direction"),"| against",s.get("measure_against"),"| baseline",s.get("baseline_price"),"| conf",s.get("confidence"),"| value",s.get("signal_value"))
    print("  meta:",json.dumps({k:str(v) for k,v in (s.get("metadata") or {}).items()}))
print("DONE 2367")
