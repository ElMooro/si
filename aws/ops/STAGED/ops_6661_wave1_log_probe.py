"""ops 6661 - read-only: what did the wave-1 engines that did not refresh in the first light (ops 6659) do?

For justhodl-stock-valuations, justhodl-insider-radar, justhodl-divcut-warning (and any names passed in FUNCTIONS env):
the REPORT lines of the last 90 minutes (duration / memory / status), runtime errors, and the engine's own print lines
mentioning the fusion ("factor book", "bank", "universe", "hp universe", "budget").  Nothing is written.
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
FUNCTIONS = (os.environ.get("FUNCTIONS") or "justhodl-stock-valuations justhodl-insider-radar justhodl-divcut-warning justhodl-deep-value-screener justhodl-magic-formula justhodl-beneish").split()


def main():
    logs = boto3.client("logs", region_name=REGION, config=CFG)
    with report("ops_6661_wave1_log_probe") as r:
        r.heading("ops 6661 - wave-1 engines: last-run logs (read-only)")
        since = int((time.time() - 90 * 60) * 1000)
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
        r.ok("ops 6661 complete (read-only)")


if __name__ == "__main__":
    main()
    sys.exit(0)
