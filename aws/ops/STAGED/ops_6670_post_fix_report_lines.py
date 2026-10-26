"""ops 6670 - read-only: the post-fix census (ops 6664, 05:04-11:05 UTC) still lists insider-radar (5 errors / 6 runs, 300 s), starmine
(3 / 3, 900 s) and equity-research (34 / 306, OOM) - which of those errors are BEFORE the fixes landed (insider-radar v1.2.2 at
06:30, the history book 08:04 / 09:14, lane AA pending) and which are after?  Every REPORT line of the last 6 hours for each,
timestamped, with status; plus the engines' own print lines.  Nothing is written.
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
FUNCTIONS = (os.environ.get("FUNCTIONS") or "justhodl-insider-radar justhodl-starmine justhodl-equity-research justhodl-insider-buyback-confluence").split()


def main():
    logs = boto3.client("logs", region_name=REGION, config=CFG)
    with report("ops_6670_post_fix_report_lines") as r:
        r.heading("ops 6670 - post-fix REPORT lines, timestamped (read-only)")
        since = int((time.time() - 6 * 3600) * 1000)
        for fn in FUNCTIONS:
            r.section(fn)
            for label, pat in (("REPORT", '"REPORT RequestId"'), ("engine lines", '?"[insider]" ?"[starmine]" ?"[research]" ?"ret60 basis" ?"bank/" ?"factor book" ?"budget" ?"[fmp-bankfirst]"')):
                ev, token = [], None
                try:
                    while True:
                        kw = dict(logGroupName="/aws/lambda/" + fn, startTime=since, filterPattern=pat, limit=200)
                        if token:
                            kw["nextToken"] = token
                        resp = logs.filter_log_events(**kw)
                        ev += resp.get("events", [])
                        token = resp.get("nextToken")
                        if not token or len(ev) >= 1200:
                            break
                except Exception as e:  # noqa: BLE001
                    r.warn("%s %s: %s" % (fn, label, type(e).__name__))
                    continue
                r.log("%s: %d events" % (label, len(ev)))
                if label == "REPORT":
                    bad = [e for e in ev if "Status: error" in e.get("message", "") or "Status: timeout" in e.get("message", "")]
                    ok = [e for e in ev if e not in bad]
                    r.log("   errors/timeouts %d, ok %d; last ok at %s" % (len(bad), len(ok), time.strftime("%H:%M:%S", time.gmtime(ok[-1]["timestamp"] / 1000)) if ok else None))
                    show = bad[-12:] + ok[-3:]
                else:
                    show = ev[-14:]
                for e in sorted(show, key=lambda x: x["timestamp"]):
                    m = e.get("message", "").strip().replace("\n", " ⏎ ")
                    if label == "REPORT":
                        m = " ".join(p.strip() for p in m.split("\t") if p.strip().startswith(("Duration:", "Max Memory", "Status:", "Error Type")))
                    r.log("   %s | %s" % (time.strftime("%H:%M:%S", time.gmtime(e["timestamp"] / 1000)), m[:300]))
        r.ok("ops 6670 complete (read-only)")


if __name__ == "__main__":
    main()
    sys.exit(0)
