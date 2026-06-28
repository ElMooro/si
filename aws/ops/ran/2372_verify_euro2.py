import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
r=lam.invoke(FunctionName="justhodl-eurodollar-plumbing",InvocationType="RequestResponse",Payload=b"{}")
print("FunctionError:",r.get("FunctionError"))
time.sleep(3)
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/eurodollar-plumbing.json")["Body"].read())
found=None
for lname,metrics in (d.get("layers") or {}).items():
    if isinstance(metrics,list):
        for m in metrics:
            if isinstance(m,dict) and m.get("id")=="stablecoin_offshore_usd": found=(lname,m)
print("plumbing_health:",d.get("plumbing_health"),"| verdict:",d.get("verdict") or d.get("regime"))
if found:
    m=found[1]
    print("✓ stablecoin offshore-$ metric in '%s' layer:"%found[0])
    print("   ",m.get("label"),"=",m.get("value"),m.get("unit"),"["+str(m.get("status"))+"]")
    print("   ",(m.get("detail") or "")[:120])
else:
    print("✗ still not found — check CloudWatch")
print("DONE 2372")
