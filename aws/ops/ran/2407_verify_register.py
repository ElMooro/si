import boto3, json, time
lam=boto3.client("lambda","us-east-1")
r=lam.invoke(FunctionName="justhodl-signal-logger",InvocationType="RequestResponse",Payload=b"{}")
print("err:",r.get("FunctionError"))
body=r["Payload"].read().decode()
# find the new stypes in the response
for s in ["crypto_exchange_flow","crypto_cot_assetmgr","coinbase_premium"]:
    print(f"  {s}: {'PRESENT' if s in body else 'neutral/not-emitted this run'}")
# pull CloudWatch [LOG] lines as definitive proof
logs=boto3.client("logs","us-east-1")
time.sleep(5)
try:
    grp="/aws/lambda/justhodl-signal-logger"
    streams=logs.describe_log_streams(logGroupName=grp,orderBy="LastEventTime",descending=True,limit=1)["logStreams"]
    if streams:
        ev=logs.get_log_events(logGroupName=grp,logStreamName=streams[0]["logStreamName"],limit=100,startFromHead=False)["events"]
        hits=[e["message"].strip() for e in ev if "[LOG]" in e["message"] and ("crypto_exchange_flow" in e["message"] or "crypto_cot_assetmgr" in e["message"] or "coinbase_premium" in e["message"])]
        print("CloudWatch [LOG] proof lines:")
        for h in hits[-6:]: print("   ",h)
        if not hits: print("    (no new-stype [LOG] lines this run — values mid-range; will emit at extremes)")
except Exception as e: print("logs err:",str(e)[:80])
print("DONE 2407")
