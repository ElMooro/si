"""ops 2009: trigger signal-harvester -> confirm eng:squeeze-fuel in DDB justhodl-signals; verify page live."""
import boto3, json, time, urllib.request
REGION="us-east-1"; B="justhodl-dashboard-live"
lam=boto3.client("lambda",REGION); ddb=boto3.client("dynamodb",REGION)

print("invoking signal-harvester…")
try:
    r=lam.invoke(FunctionName="justhodl-signal-harvester",InvocationType="RequestResponse")
    print(" status:",r["StatusCode"],"|",r["Payload"].read().decode()[:280])
except Exception as e:
    print(" harvester invoke err:",str(e)[:160])
time.sleep(3)

# scan justhodl-signals for eng:squeeze-fuel rows (today)
try:
    n=0; samp=[]
    pag=ddb.get_paginator("scan")
    for pg in pag.paginate(TableName="justhodl-signals",
            FilterExpression="signal_type = :t",
            ExpressionAttributeValues={":t":{"S":"eng:squeeze-fuel"}},
            ProjectionExpression="signal_id,signal_value",Limit=200):
        for it in pg.get("Items",[]):
            n+=1
            if len(samp)<8: samp.append((it.get("signal_id",{}).get("S"),it.get("signal_value",{}).get("S")))
        if n>=200: break
    print(f"\neng:squeeze-fuel rows in justhodl-signals: {n}")
    for s in samp: print("  ",s)
except Exception as e:
    print(" ddb scan err:",str(e)[:200])

# verify page live (Pages)
print("\npage check:")
for u in ("https://justhodl.ai/squeeze-fuel.html","https://justhodl.ai/data/squeeze-fuel.json"):
    try:
        req=urllib.request.Request(u+f"?t={int(time.time())}",headers={"User-Agent":"jh-verify"})
        with urllib.request.urlopen(req,timeout=20) as resp:
            body=resp.read(); print(f"  {u} -> HTTP {resp.getcode()} bytes={len(body)}")
    except Exception as e:
        print(f"  {u} -> {str(e)[:100]}")
print("DONE 2009")
