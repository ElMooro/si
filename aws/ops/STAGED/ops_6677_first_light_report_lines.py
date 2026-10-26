"""ops 6677 - read-only follow-up to ops 6676: every REPORT line since 16:00 UTC for the four fixed engines (did
analytics-snapshot run twice behind the probe's 333 s invoke? did signal-genealogy publish at 16:06 and then time out?)
plus signal-genealogy's own log lines in that window.  Nothing is written.  No key involved.
"""
from pathlib import Path
import os
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
CFG = Config(retries={"max_attempts": 8, "mode": "standard"}, connect_timeout=10, read_timeout=120)
FUNCTIONS = (os.environ.get("FUNCTIONS") or "justhodl-analytics-snapshot justhodl-earnings-whisper justhodl-event-flow-monitor justhodl-signal-genealogy").split()
SINCE = int(os.environ.get("SINCE_EPOCH") or (time.time() - 90 * 60))


def events(logs, fn, pattern, limit=400):
    out, token = [], None
    while True:
        kw = dict(logGroupName="/aws/lambda/" + fn, startTime=int(SINCE * 1000), filterPattern=pattern, limit=200)
        if token:
            kw["nextToken"] = token
        resp = logs.filter_log_events(**kw)
        out += resp.get("events", [])
        token = resp.get("nextToken")
        if not token or len(out) >= limit:
            break
    return sorted(out, key=lambda e: e["timestamp"])


def main():
    logs = boto3.client("logs", region_name=REGION, config=CFG)
    with report("ops_6677_first_light_report_lines") as r:
        r.heading("ops 6677 - REPORT lines since %s UTC for the four fixed engines (read-only)" % time.strftime("%H:%M", time.gmtime(SINCE)))
        for fn in FUNCTIONS:
            r.section(fn)
            try:
                for e in events(logs, fn, '"REPORT RequestId"'):
                    m = e.get("message", "")
                    rid = m.split("RequestId:")[1].split()[0] if "RequestId:" in m else "?"
                    parts = " ".join(p.strip() for p in m.split("\t") if p.strip().startswith(("Duration:", "Max Memory", "Status:", "Init Duration")))
                    r.log("%s | %s | %s" % (time.strftime("%H:%M:%S", time.gmtime(e["timestamp"] / 1000)), rid[:8], parts))
            except Exception as e:  # noqa: BLE001
                r.warn("%s: %s" % (fn, type(e).__name__))
        r.section("signal-genealogy: its own lines in the window")
        try:
            for e in events(logs, "justhodl-signal-genealogy", '?"[genealogy" ?Traceback ?"[ERROR]" ?"Task timed out" ?published ?"budget" ?"runtime" ?"chronology" ?"START RequestId" ?"END RequestId"', limit=120):
                r.log("%s | %s" % (time.strftime("%H:%M:%S", time.gmtime(e["timestamp"] / 1000)), e.get("message", "").strip().replace("\n", " ⏎ ")[:300]))
        except Exception as e:  # noqa: BLE001
            r.warn("genealogy lines: %s" % type(e).__name__)
        r.section("analytics-snapshot: its own lines in the window")
        try:
            for e in events(logs, "justhodl-analytics-snapshot", '?"[snapshot]" ?"Task timed out" ?Traceback', limit=80):
                r.log("%s | %s" % (time.strftime("%H:%M:%S", time.gmtime(e["timestamp"] / 1000)), e.get("message", "").strip().replace("\n", " ⏎ ")[:240]))
        except Exception as e:  # noqa: BLE001
            r.warn("snapshot lines: %s" % type(e).__name__)
        r.ok("ops 6677 complete (read-only)")


if __name__ == "__main__":
    main()
    sys.exit(0)
