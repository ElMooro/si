import boto3, json, time
logs=boto3.client("logs","us-east-1"); s3=boto3.client("s3","us-east-1")
lg="/aws/lambda/justhodl-cycle-clock"
try:
    streams=logs.describe_log_streams(logGroupName=lg,orderBy="LastEventTime",descending=True,limit=2)["logStreams"]
    for st in streams:
        ev=logs.get_log_events(logGroupName=lg,logStreamName=st["logStreamName"],limit=60,startFromHead=False)["events"]
        for e in ev:
            m=e["message"].rstrip()
            if any(k in m for k in ["AI","llm","Import","Error","Traceback","Module","cycle-clock","Task timed","GLM","reason"]):
                print(m[:240])
except Exception as e:
    print("logs err:", str(e)[:120])
print("\n=== yield-curve field shapes ===")
yc=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/yield-curve.json")["Body"].read())
print("top keys:", list(yc.keys())[:20])
print("real_yields:", json.dumps(yc.get("real_yields"))[:200])
print("inflation_expectations:", json.dumps(yc.get("inflation_expectations"))[:200])
print("decomposition:", json.dumps(yc.get("decomposition"))[:200])
print("\n=== eps-revision keys ===")
ep=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/eps-revision-velocity.json")["Body"].read())
print("top keys:", list(ep.keys())[:20])
for k in ("breadth_pct","net_revision_breadth","summary","headline","regime","net_up_pct"):
    if k in ep: print(f"  {k}:", json.dumps(ep[k])[:120])
print("DONE 2321")
