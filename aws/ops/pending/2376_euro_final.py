import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
cfg=lam.get_function(FunctionName="justhodl-eurodollar-plumbing")["Configuration"]
print("deployed LastModified:",cfg["LastModified"])
r=lam.invoke(FunctionName="justhodl-eurodollar-plumbing",InvocationType="RequestResponse",Payload=b"{}")
print("FunctionError:",r.get("FunctionError"))
time.sleep(4)
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/eurodollar-plumbing.json")["Body"].read())
found=None
for lname,metrics in (d.get("layers") or {}).items():
    if isinstance(metrics,list):
        for m in metrics:
            if isinstance(m,dict) and m.get("id")=="stablecoin_offshore_usd": found=(lname,m)
print("plumbing_health:",d.get("plumbing_health"),"| verdict:",d.get("verdict") or d.get("regime"))
if found:
    m=found[1]
    print("\u2713 stablecoin offshore-$ metric LIVE in '%s' layer:"%found[0])
    print("   ",m.get("label"),"=",m.get("value"),m.get("unit"),"["+str(m.get("status"))+"]")
    print("   ",(m.get("detail") or "")[:150])
    # show full fx layer ids for context
    print("   fx layer ids:",[x.get("id") for x in d["layers"]["fx"] if isinstance(x,dict)])
else:
    print("\u2717 still not found")
print("DONE 2376")
