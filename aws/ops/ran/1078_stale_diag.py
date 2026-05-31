#!/usr/bin/env python3
"""1078 — pull CloudWatch logs for the 3 stale Lambdas to diagnose root cause."""
import json, os, pathlib
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/1078_stale_diag.json"
logs = boto3.client("logs", region_name="us-east-1")

TARGETS = [
    "cftc-futures-positioning-agent",
    "justhodl-edge-engine",
    "justhodl-options-flow",
]


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "lambdas": {}}
    
    end_time = int(datetime.now(timezone.utc).timestamp() * 1000)
    start_time = end_time - (30 * 60 * 1000)  # last 30 min
    
    for name in TARGETS:
        entry = {"events": [], "errors_only": []}
        log_group = f"/aws/lambda/{name}"
        try:
            streams = logs.describe_log_streams(
                logGroupName=log_group, orderBy="LastEventTime",
                descending=True, limit=5,
            )
            entry["stream_count"] = len(streams.get("logStreams", []))
            if streams.get("logStreams"):
                latest_stream = streams["logStreams"][0]
                entry["latest_stream"]    = latest_stream["logStreamName"]
                entry["latest_event_at"]  = latest_stream.get("lastEventTimestamp")
            
            for s in streams.get("logStreams", [])[:2]:
                try:
                    evt = logs.get_log_events(
                        logGroupName=log_group, logStreamName=s["logStreamName"],
                        startTime=start_time, limit=80,
                    )
                    for e in evt.get("events", []):
                        msg = e["message"].strip()
                        if msg.startswith(("START ", "END ", "REPORT", "INIT_START")):
                            # Capture REPORT lines specially (timing info)
                            if msg.startswith("REPORT"):
                                entry["events"].append(msg[:280])
                            continue
                        entry["events"].append(msg[:280])
                        # Capture errors specifically
                        if any(t in msg for t in ["error", "Error", "ERROR", "Traceback",
                                                     "Exception", "404", "403", "500",
                                                     "timeout", "Timeout", "TimedOut"]):
                            entry["errors_only"].append(msg[:280])
                except Exception:
                    pass
        except Exception as e:
            entry["err"] = str(e)[:200]
        
        out["lambdas"][name] = entry
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print(f"[1078] DONE")


if __name__ == "__main__":
    main()
