import boto3, json, time
lam=boto3.client("lambda","us-east-1"); ddb=boto3.resource("dynamodb","us-east-1")
from boto3.dynamodb.conditions import Attr
lam.invoke(FunctionName="justhodl-signal-logger",InvocationType="RequestResponse",Payload=b"{}")
time.sleep(3)
t=ddb.Table("justhodl-signals")
def count_type(st):
    n=0; latest=None; ek=None
    while True:
        kw={"FilterExpression":Attr("signal_type").eq(st)}
        if ek: kw["ExclusiveStartKey"]=ek
        r=t.scan(**kw)
        for it in r.get("Items",[]):
            n+=1
            if latest is None or str(it.get("logged_epoch",""))>str(latest.get("logged_epoch","")): latest=it
        ek=r.get("LastEvaluatedKey")
        if not ek: break
    return n, latest
for st in ["crypto_dvol","crypto_risk_score"]:
    n,latest=count_type(st)
    print(f"{st}: {n} in ledger")
    if latest: print("   latest dir",latest.get("predicted_direction"),"| baseline",latest.get("baseline_price"),"| epoch",latest.get("logged_epoch"),"| val",latest.get("signal_value"))
print("DONE 2370")
