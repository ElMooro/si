"""ops 1997: capture the swallowed EARNINGS exception in catalyst-calendar via CloudWatch."""
import boto3, json, time

REGION="us-east-1"
FN="justhodl-catalyst-calendar"
lam=boto3.client("lambda",region_name=REGION)
logs=boto3.client("logs",region_name=REGION)

# 1) invoke synchronously to force a fresh run + fresh log stream
print("=== invoking",FN,"(sync) ===")
r=lam.invoke(FunctionName=FN, InvocationType="RequestResponse")
payload=r["Payload"].read().decode()
print("status:",r["StatusCode"])
print("payload[:800]:",payload[:800])

# 2) give logs a moment to land
time.sleep(8)

# 3) pull most-recent log stream and dump catalyst lines
lg=f"/aws/lambda/{FN}"
streams=logs.describe_log_streams(logGroupName=lg, orderBy="LastEventTime", descending=True, limit=2)["logStreams"]
for s in streams:
    sn=s["logStreamName"]
    print(f"\n===== STREAM {sn} =====")
    ev=logs.get_log_events(logGroupName=lg, logStreamName=sn, limit=120, startFromHead=False)["events"]
    for e in ev:
        m=e["message"].rstrip()
        if "catalyst" in m or "EARNINGS" in m or "Traceback" in m or "Error" in m or "Source" in m or "[" in m:
            print(m)
