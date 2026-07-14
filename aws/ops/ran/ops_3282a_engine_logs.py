"""ops 3282a — read the 13f engine's own logs. Feed frozen at
23:57:49 (and that v4 write had zero option rows): every invoke since
dies pre-write. Print the last two invocation tails — Tracebacks,
'Task timed out', REPORT durations, and every [13f] progress marker —
so the next patch targets the actual failure, not a theory."""
import sys
from datetime import datetime, timezone

import boto3

from ops_report import report

LOGS = boto3.client("logs", region_name="us-east-1")
LAM = boto3.client("lambda", region_name="us-east-1")
GRP = "/aws/lambda/justhodl-13f-positions"

with report("3282a_engine_logs") as rep:
    cfg = LAM.get_function_configuration(
        FunctionName="justhodl-13f-positions")
    rep.kv(timeout_s=cfg.get("Timeout"), mem=cfg.get("MemorySize"),
           last_modified=str(cfg.get("LastModified"))[:19])
    streams = LOGS.describe_log_streams(
        logGroupName=GRP, orderBy="LastEventTime",
        descending=True, limit=3).get("logStreams") or []
    for st in streams[:2]:
        rep.section(f"stream {st['logStreamName'][-18:]}")
        evs = LOGS.get_log_events(
            logGroupName=GRP, logStreamName=st["logStreamName"],
            limit=100, startFromHead=False).get("events") or []
        interesting = []
        for e in evs:
            m = e["message"].rstrip()
            if any(k in m for k in ("Traceback", "Error", "error",
                                    "timed out", "[13f]", "REPORT",
                                    "  File ", "raise", "START")):
                ts = datetime.fromtimestamp(
                    e["timestamp"] / 1000,
                    tz=timezone.utc).strftime("%H:%M:%S")
                interesting.append(f"{ts} {m[:200]}")
        for line in interesting[-40:]:
            rep.log("  " + line)
    rep.kv(verdict="DIAGNOSED")
