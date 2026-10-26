"""ops 6660 - read-only: what did justhodl-bank-factors do on its first join?  (ops 6658 saw no state write in 17 min.)

Dumps the last 60 minutes of /aws/lambda/justhodl-bank-factors: every [bank-factors] progress line, any Traceback /
Error / Task timed out / Runtime.OutOfMemory line, and the REPORT lines (duration, max memory used) - plus the state
and manifest objects if any were written.  Nothing is written.
"""
from pathlib import Path
import json
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION, LIVE, FN = "us-east-1", "justhodl-dashboard-live", "justhodl-bank-factors"
CFG = Config(retries={"max_attempts": 8, "mode": "standard"}, connect_timeout=10, read_timeout=120)


def main():
    logs = boto3.client("logs", region_name=REGION, config=CFG)
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    with report("ops_6660_bank_factors_log_probe") as r:
        r.heading("ops 6660 - justhodl-bank-factors first-join log probe (read-only)")
        since = int((time.time() - 3600) * 1000)
        r.section("1. REPORT lines (duration / memory) and runtime errors")
        for pat in ('"REPORT RequestId"', '?Traceback ?"Task timed out" ?"Runtime.OutOfMemory" ?"Error" ?"errorMessage" ?"MemoryError" ?"Killed"'):
            try:
                ev = logs.filter_log_events(logGroupName="/aws/lambda/" + FN, startTime=since, filterPattern=pat, limit=60).get("events", [])
            except Exception as e:  # noqa: BLE001
                r.warn("filter %s: %s" % (pat[:30], type(e).__name__))
                continue
            r.log("pattern %s -> %d events" % (pat[:40], len(ev)))
            for e in ev[-25:]:
                r.log("   %s | %s" % (time.strftime("%H:%M:%S", time.gmtime(e["timestamp"] / 1000)), e.get("message", "").strip().replace("\n", " ⏎ ")[:600]))
        r.section("2. engine progress lines")
        try:
            ev = logs.filter_log_events(logGroupName="/aws/lambda/" + FN, startTime=since, filterPattern='"[bank-factors]"', limit=100).get("events", [])
            for e in ev[-60:]:
                r.log("   %s | %s" % (time.strftime("%H:%M:%S", time.gmtime(e["timestamp"] / 1000)), e.get("message", "").strip()[:400]))
            if not ev:
                r.log("   none - the engine never printed its first line (import failure or no invocation reached the code)")
        except Exception as e:  # noqa: BLE001
            r.warn("progress filter: %s" % type(e).__name__)
        r.section("3. raw tail (last 40 events, any stream)")
        try:
            streams = logs.describe_log_streams(logGroupName="/aws/lambda/" + FN, orderBy="LastEventTime", descending=True, limit=3).get("logStreams", [])
            for st in streams[:2]:
                ev = logs.get_log_events(logGroupName="/aws/lambda/" + FN, logStreamName=st["logStreamName"], limit=40, startFromHead=False).get("events", [])
                r.log("stream %s (%d events shown)" % (st["logStreamName"][-40:], len(ev)))
                for e in ev:
                    r.log("   %s | %s" % (time.strftime("%H:%M:%S", time.gmtime(e["timestamp"] / 1000)), e.get("message", "").strip().replace("\n", " ⏎ ")[:500]))
        except Exception as e:  # noqa: BLE001
            r.warn("tail: %s" % type(e).__name__)
        r.section("4. state / manifest objects")
        for key in ("data/warm/factor-book/_state/state.json", "data/warm/factor-book/manifest.json"):
            try:
                body = s3.get_object(Bucket=LIVE, Key=key)["Body"].read()
                d = json.loads(body)
                d.pop("hashes", None)
                r.log("%s: %s" % (key, json.dumps(d)[:1500]))
            except Exception as e:  # noqa: BLE001
                r.log("%s: absent (%s)" % (key, type(e).__name__))
        try:
            n = s3.list_objects_v2(Bucket=LIVE, Prefix="data/warm/factor-book/symbols/", MaxKeys=5).get("KeyCount", 0)
            r.log("symbols/ prefix: %s object(s) in the first page" % n)
        except Exception as e:  # noqa: BLE001
            r.log("symbols/ listing: %s" % type(e).__name__)
        r.ok("ops 6660 complete (read-only)")


if __name__ == "__main__":
    main()
    sys.exit(0)
