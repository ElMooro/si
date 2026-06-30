"""ops 2538 — capture the exact master-allocator runtime error."""
import boto3, json, time
from botocore.config import Config

lam = boto3.client("lambda", "us-east-1", config=Config(read_timeout=200, retries={"max_attempts": 0}))
logs = boto3.client("logs", "us-east-1")
FN = "justhodl-master-allocator"

r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
print("FunctionError:", r.get("FunctionError"))
payload = r["Payload"].read().decode()
print("PAYLOAD:", payload[:1500])

# also size of brain.json (memory suspicion)
s3 = boto3.client("s3", "us-east-1")
try:
    h = s3.head_object(Bucket="justhodl-dashboard-live", Key="data/brain.json")
    print("\nbrain.json size:", round(h["ContentLength"] / 1e6, 2), "MB")
except Exception as e:
    print("brain head err:", str(e)[:80])

# memory config
cfg = lam.get_function_configuration(FunctionName=FN)
print("allocator memory:", cfg.get("MemorySize"), "MB | timeout:", cfg.get("Timeout"), "s")

# recent log errors
time.sleep(2)
try:
    lg = f"/aws/lambda/{FN}"
    streams = logs.describe_log_streams(logGroupName=lg, orderBy="LastEventTime", descending=True, limit=1)["logStreams"]
    if streams:
        ev = logs.get_log_events(logGroupName=lg, logStreamName=streams[0]["logStreamName"], limit=40, startFromHead=False)["events"]
        print("\n=== recent CloudWatch ===")
        for e in ev[-25:]:
            m = e["message"].rstrip()
            if any(k in m for k in ("Error", "Traceback", "line ", "Exception", "brain", "KeyError", "Task timed", "MemoryError", "raise", "  File")):
                print("  " + m[:200])
except Exception as e:
    print("logs err:", str(e)[:100])
print("DONE 2538")
