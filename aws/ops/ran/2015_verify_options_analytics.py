"""ops 2015: re-invoke options-analytics (new pick logic) + harvester ingestion + page live."""
import boto3, json, time, urllib.request
REGION="us-east-1"; B="justhodl-dashboard-live"
lam=boto3.client("lambda",REGION); ddb=boto3.client("dynamodb",REGION)
r=lam.invoke(FunctionName="justhodl-options-analytics",InvocationType="RequestResponse")
print("options-analytics invoke:",r["StatusCode"],"|",r["Payload"].read().decode()[:260])
time.sleep(2)
d=json.loads(boto3.client("s3",REGION).get_object(Bucket=B,Key="data/options-analytics.json")["Body"].read())
print("picks now:",len(d.get("top_picks") or []),"→",[(p["ticker"],p["score"],p["signal"]) for p in d.get("top_picks",[])])
print("\ninvoking signal-harvester…")
try:
    rr=lam.invoke(FunctionName="justhodl-signal-harvester",InvocationType="RequestResponse")
    print(" harvester:",rr["StatusCode"],"|",rr["Payload"].read().decode()[:200])
except Exception as e: print(" harvester err:",str(e)[:160])
time.sleep(3)
n=0;samp=[]
for pg in ddb.get_paginator("scan").paginate(TableName="justhodl-signals",
        FilterExpression="signal_type = :t",ExpressionAttributeValues={":t":{"S":"eng:options-analytics"}},
        ProjectionExpression="signal_id,signal_value",Limit=200):
    for it in pg.get("Items",[]):
        n+=1
        if len(samp)<8: samp.append((it.get("signal_id",{}).get("S"),it.get("signal_value",{}).get("S")))
    if n>=200: break
print(f"\neng:options-analytics rows in justhodl-signals: {n}")
for s in samp: print("  ",s)
print("\npage check:")
for u in ("https://justhodl.ai/options-analytics.html",):
    try:
        with urllib.request.urlopen(urllib.request.Request(u+f"?t={int(time.time())}",headers={"User-Agent":"v"}),timeout=20) as resp:
            print(f"  {u} -> HTTP {resp.getcode()} bytes={len(resp.read())}")
    except Exception as e: print(f"  {u} -> {str(e)[:90]}")
print("DONE 2015")
