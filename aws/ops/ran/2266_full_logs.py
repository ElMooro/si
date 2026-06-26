import boto3, json, time
logs=boto3.client("logs","us-east-1"); lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
cfg=lam.get_function_configuration(FunctionName="justhodl-equity-research")
print("Lambda LastModified:", cfg.get("LastModified"), "| timeout:", cfg.get("Timeout"))
# fire fresh invoke
before=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="equity-research/LDOS.json")["Body"].read()).get("generated_at")
lam.invoke(FunctionName="justhodl-equity-research", InvocationType="Event",
           Payload=json.dumps({"ticker":"TXN","force_refresh":True,"_internal":"1"}).encode())  # cold ticker = forces full run, separate key
print("invoked TXN (cold) to capture a clean full run")
time.sleep(160)
grp="/aws/lambda/justhodl-equity-research"
start=int((time.time()-200)*1000)
streams=logs.describe_log_streams(logGroupName=grp,orderBy="LastEventTime",descending=True,limit=2)["logStreams"]
lines=[]
for st in streams:
    for e in logs.get_log_events(logGroupName=grp,logStreamName=st["logStreamName"],startTime=start,limit=200,startFromHead=True)["events"]:
        lines.append(e["message"].rstrip())
# print the tail of the run
for m in lines[-60:]:
    if m.strip(): print(m[:260])
# did TXN write?
try:
    txn=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="equity-research/TXN.json")["Body"].read())
    print("\nTXN written? gen=",txn.get("generated_at"),"| business_mix segs:",(txn.get('business_mix') or {}).get('segments'),"| price pts:",len(txn.get('price_history') or []),"| margins op:",[x for x in ((txn.get('margins') or {}).get('operating_trend') or []) if x.get('value') is not None][:1])
except Exception as e:
    print("\nTXN not written:",str(e)[:60])
print("DONE 2266")
