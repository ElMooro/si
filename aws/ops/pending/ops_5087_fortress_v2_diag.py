"""ops_5087 -- diagnostic: why did justhodl-fortress v2.0.0 produce no payload
in 14 minutes (ops 5086 G4)? Dump the CloudWatch log tail of every recent
stream (timeouts, memory, tracebacks, [fortress] progress lines), and the
state of data/fortress.json / data/fortress-backtest.json on disk.
Read-only; no deploys.
"""
import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-fortress"
cfg = Config(retries={"max_attempts": 8, "mode": "adaptive"})
s3 = boto3.client("s3", region_name=REGION, config=cfg)
lam = boto3.client("lambda", region_name=REGION, config=cfg)
logs = boto3.client("logs", region_name=REGION, config=cfg)


def jget(k):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=k)["Body"].read())
    except Exception as e:  # noqa: BLE001
        return {"_err": str(e)[:160]}


with report("5087-fortress-v2-diag") as r:
    r.heading("ops 5087 -- justhodl-fortress v2 diagnostic (logs + disk)")
    r.section("function")
    c = lam.get_function_configuration(FunctionName=FN)
    r.kv(memory=c.get("MemorySize"), timeout=c.get("Timeout"), state=c.get("State"), last_update=c.get("LastUpdateStatus"),
         code_size=c.get("CodeSize"), version_env=(c.get("Environment") or {}).get("Variables", {}).get("FORTRESS_VERSION"))
    r.section("disk")
    p = jget("data/fortress.json")
    r.kv(fortress_as_of=p.get("as_of"), version=p.get("version"), session=p.get("session"), sessions_loaded=p.get("sessions_loaded"),
         n_scored=p.get("n_scored"), elapsed=(p.get("diagnostics") or {}).get("elapsed_s"), err=p.get("_err"))
    for line in ((p.get("diagnostics") or {}).get("log") or [])[-12:]:
        r.log("  diag " + str(line)[:200])
    bt = jget("data/fortress-backtest.json")
    r.kv(backtest_as_of=bt.get("as_of"), backtest_obs=bt.get("n_observations"), backtest_err=bt.get("_err"))
    r.section("cloudwatch: last streams")
    grp = "/aws/lambda/" + FN
    streams = logs.describe_log_streams(logGroupName=grp, orderBy="LastEventTime", descending=True, limit=5).get("logStreams", [])
    since = int((datetime.now(timezone.utc) - timedelta(hours=2)).timestamp() * 1000)
    for st in streams:
        r.log("stream %s first=%s last=%s" % (st["logStreamName"][-24:],
                                              datetime.fromtimestamp((st.get("firstEventTimestamp") or 0) / 1000, tz=timezone.utc).isoformat(timespec="seconds"),
                                              datetime.fromtimestamp((st.get("lastEventTimestamp") or 0) / 1000, tz=timezone.utc).isoformat(timespec="seconds")))
        evs = []
        tok = None
        for _ in range(6):
            kw = {"logGroupName": grp, "logStreamName": st["logStreamName"], "startTime": since, "limit": 500, "startFromHead": True}
            if tok:
                kw["nextToken"] = tok
            res = logs.get_log_events(**kw)
            evs.extend(res.get("events", []))
            nt = res.get("nextForwardToken")
            if not nt or nt == tok or len(res.get("events", [])) == 0:
                break
            tok = nt
        keep = [e for e in evs if any(s in e.get("message", "") for s in (
            "[fortress]", "REPORT", "Task timed out", "Error", "Traceback", "MemoryError", "START RequestId", "END RequestId", "Runtime exited", "INIT_START"))]
        r.log("  %d events, %d kept" % (len(evs), len(keep)))
        for e in keep[:12] + (keep[-45:] if len(keep) > 57 else keep[12:]):
            ts = datetime.fromtimestamp(e["timestamp"] / 1000, tz=timezone.utc).strftime("%H:%M:%S")
            r.log("  %s %s" % (ts, e.get("message", "").rstrip().replace("\n", " | ")[:230]))
    r.section("verdict")
    r.log("see log tail above: look for 'Task timed out' (900s), 'Runtime exited' (OOM), or the last [fortress] progress line reached")
