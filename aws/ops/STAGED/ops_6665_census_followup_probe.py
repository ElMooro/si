"""ops 6665 - read-only: the engines the fleet error census (ops 6664) flagged, after the fleet fix lanes - justhodl-equity-research
(52 OOMs at 1 GB in 2 s: ten threads decoding the same bank table; lane X carries the one-loader-per-table fmp_bank),
justhodl-gap-metrics (240 s timeouts), justhodl-event-flow-monitor (120 s timeouts, not an FMP consumer), justhodl-eva-spread
(600 s timeout before its fusion).  For each (or the FUNCTIONS env list): the REPORT lines of the last 3 hours (duration /
memory / status), runtime errors, and the engines' own fusion / bank lines.  Also invokes equity-research once for AAPL
(its own output; the why.html desk) so a post-fix REPORT line exists.  Nothing else is written.
"""
from pathlib import Path
import os
import sys
import time

import json
import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
CFG = Config(retries={"max_attempts": 8, "mode": "standard"}, connect_timeout=10, read_timeout=120)
FUNCTIONS = (os.environ.get("FUNCTIONS") or "justhodl-equity-research justhodl-gap-metrics justhodl-event-flow-monitor justhodl-eva-spread").split()


def main():
    logs = boto3.client("logs", region_name=REGION, config=CFG)
    with report("ops_6665_census_followup_probe") as r:
        r.heading("ops 6665 - census follow-up: last-run logs after the fleet fix (read-only)")
        try:
            lam = boto3.client("lambda", region_name=REGION, config=CFG)
            cf = lam.get_function_configuration(FunctionName="justhodl-equity-research")
            r.log("equity-research deployed: modified %s, %s MB, %s s" % ((cf.get("LastModified") or "")[:19], cf.get("MemorySize"), cf.get("Timeout")))
            resp = lam.invoke(FunctionName="justhodl-equity-research", InvocationType="RequestResponse", Payload=json.dumps({"queryStringParameters": {"ticker": "AAPL", "refresh": "1"}, "rawQueryString": "ticker=AAPL&refresh=1"}).encode())
            body = resp["Payload"].read()
            r.log("equity-research AAPL invoke: status %s, %d bytes, FunctionError %s, head %s" % (resp.get("StatusCode"), len(body), resp.get("FunctionError"), body[:160]))
            time.sleep(20)
        except Exception as e:  # noqa: BLE001
            r.warn("equity-research invoke: %s %s" % (type(e).__name__, str(e)[:120]))
        since = int((time.time() - 180 * 60) * 1000)
        for fn in FUNCTIONS:
            r.section(fn)
            for label, pat in (("REPORT", '"REPORT RequestId"'), ("errors", '?Traceback ?"Task timed out" ?"Runtime.OutOfMemory" ?"[ERROR]" ?"errorMessage"'),
                               ("fusion lines", '?"factor book" ?"factor-book" ?"bank/" ?"universe" ?"hp universe" ?"budget" ?"bank_context"')):
                try:
                    ev = logs.filter_log_events(logGroupName="/aws/lambda/" + fn, startTime=since, filterPattern=pat, limit=40).get("events", [])
                except Exception as e:  # noqa: BLE001
                    r.warn("%s %s: %s" % (fn, label, type(e).__name__))
                    continue
                r.log("%s: %d events" % (label, len(ev)))
                for e in ev[-12:]:
                    r.log("   %s | %s" % (time.strftime("%H:%M:%S", time.gmtime(e["timestamp"] / 1000)), e.get("message", "").strip().replace("\n", " ⏎ ")[:420]))
        r.ok("ops 6665 complete (read-only)")


if __name__ == "__main__":
    main()
    sys.exit(0)
