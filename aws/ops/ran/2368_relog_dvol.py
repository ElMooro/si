import boto3, json, time
lam=boto3.client("lambda","us-east-1"); ddb=boto3.resource("dynamodb","us-east-1")
from boto3.dynamodb.conditions import Attr
# confirm deployed code contains crypto_dvol
code_has=False
try:
    import urllib.request, io, zipfile
    loc=lam.get_function(FunctionName="justhodl-signal-logger")["Code"]["Location"]
    z=zipfile.ZipFile(io.BytesIO(urllib.request.urlopen(loc,timeout=30).read()))
    src=z.read("lambda_function.py").decode("utf-8","ignore")
    code_has="crypto_dvol" in src
except Exception as e: print("code check err:",str(e)[:80])
print("deployed code has crypto_dvol:",code_has)
r=lam.invoke(FunctionName="justhodl-signal-logger",InvocationType="RequestResponse",Payload=b"{}")
print("resp:",r["Payload"].read().decode()[:120])
time.sleep(3)
t=ddb.Table("justhodl-signals")
resp=t.scan(FilterExpression=Attr("signal_type").eq("crypto_dvol"), Limit=400)
items=resp.get("Items",[])
print("crypto_dvol signals in ledger:",len(items))
if items:
    items.sort(key=lambda x:str(x.get("logged_epoch","")),reverse=True); s=items[0]
    print("  latest: dir",s.get("predicted_direction"),"| against",s.get("measure_against"),"| baseline",s.get("baseline_price"),"| conf",s.get("confidence"),"| val",s.get("signal_value"))
print("DONE 2368")
