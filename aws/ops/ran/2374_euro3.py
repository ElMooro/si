import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1"); logs=boto3.client("logs","us-east-1")
cfg=lam.get_function(FunctionName="justhodl-eurodollar-plumbing")["Configuration"]
print("deployed LastModified:",cfg["LastModified"],"| timeout:",cfg["Timeout"])
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
    m=found[1]; print("✓ in '%s':"%found[0],m.get("label"),"=",m.get("value"),m.get("unit"),"["+str(m.get("status"))+"]")
else:
    print("✗ not found; recent logs:")
    try:
        streams=logs.describe_log_streams(logGroupName="/aws/lambda/justhodl-eurodollar-plumbing",orderBy="LastEventTime",descending=True,limit=1)["logStreams"]
        for e in logs.get_log_events(logGroupName="/aws/lambda/justhodl-eurodollar-plumbing",logStreamName=streams[0]["logStreamName"],limit=40,startFromHead=False)["events"]:
            mm=e["message"].rstrip()
            if any(k in mm for k in ["stablecoin","offshore","llama","Error","Traceback"]): print("   ",mm[:160])
    except Exception as e: print("   log err",str(e)[:60])
print("DONE 2374")
