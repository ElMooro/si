#!/usr/bin/env python3
"""Step 264 — Tail event-study Lambda CloudWatch logs to find why n_events=0."""
import boto3, time, json, os
from datetime import datetime, timezone

REGION = "us-east-1"
LOG_GROUP = "/aws/lambda/justhodl-event-study"
REPORT_PATH = "aws/ops/reports/264_event_study_diag.json"

logs = boto3.client("logs", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)

# 1. Trigger a fresh invoke
print("[264] invoking event-study…")
inv = lam.invoke(FunctionName="justhodl-event-study", InvocationType="RequestResponse", Payload=b"{}")
inv_payload = json.loads(inv["Payload"].read())
print(f"  status={inv.get('StatusCode')}  err={inv.get('FunctionError')}")
print(f"  payload: {inv_payload}")
time.sleep(4)

# 2. Tail CloudWatch logs for the last 10 minutes
end = int(time.time() * 1000)
start = end - 10 * 60 * 1000
streams = logs.describe_log_streams(
    logGroupName=LOG_GROUP, orderBy="LastEventTime",
    descending=True, limit=3,
)["logStreams"]

events = []
for s in streams:
    ev = logs.get_log_events(
        logGroupName=LOG_GROUP, logStreamName=s["logStreamName"],
        startTime=start, endTime=end, limit=200,
    )["events"]
    for e in ev:
        msg = e["message"].strip()
        if any(k in msg for k in ["[event-study]", "[fred]", "ERROR", "Traceback", "obs", "fed_funds", "spx"]):
            events.append({"ts": e["timestamp"], "msg": msg[:400]})

events.sort(key=lambda x: x["ts"])

out = {
    "probed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    "fresh_invoke": {
        "status": inv.get("StatusCode"),
        "func_err": inv.get("FunctionError"),
        "payload": inv_payload,
    },
    "n_log_events": len(events),
    "log_tail": events[-50:],
}
os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
open(REPORT_PATH, "w").write(json.dumps(out, indent=2, default=str))
print(json.dumps(out, indent=2, default=str)[:5000])
