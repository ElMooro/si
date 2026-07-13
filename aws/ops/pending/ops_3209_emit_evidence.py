"""ops 3209 — why did 24 firing panels emit 0 signals? Evidence, not
guesses: the runner prints exactly one of three markers per run
('trust-ledger signals emitted: N' / 'signal skip <id>: <err>' /
'signal emission unavailable: <err>'). This ops pulls those lines from the
last CloudWatch streams into the report. Also retries the checker invoke
that hit TooManyRequests."""
import sys
import time

import boto3

from ops_report import report

REGION = "us-east-1"
LOGS = boto3.client("logs", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)

with report("3209_emit_evidence") as rep:
    fails, warns = [], []
    rep.heading("ops 3209 — emission failure named from the logs")

    rep.section("1. Runner log markers")
    found = 0
    try:
        grp = "/aws/lambda/justhodl-wl-engines"
        for st in LOGS.describe_log_streams(
                logGroupName=grp, orderBy="LastEventTime",
                descending=True, limit=3).get("logStreams") or []:
            for e in LOGS.get_log_events(
                    logGroupName=grp,
                    logStreamName=st["logStreamName"],
                    limit=200, startFromHead=False).get("events") or []:
                m = e.get("message") or ""
                if any(k in m for k in ("trust-ledger", "signal skip",
                                        "emission unavailable",
                                        "Task timed out", "[ERROR]")):
                    rep.log("  " + m.strip()[:170])
                    found += 1
            if found >= 12:
                break
    except Exception as e:
        fails.append(f"logs: {str(e)[:80]}")
    if not found:
        warns.append("no emission markers in the last 3 streams — the "
                     "block may sit on a path the run never reached")

    rep.section("2. Checker retry (was TooManyRequests)")
    for attempt in range(3):
        try:
            r = LAM.invoke(FunctionName="justhodl-outcome-checker",
                           InvocationType="RequestResponse", Payload=b"{}")
            rep.kv(checker_status=r.get("StatusCode"),
                   function_error=r.get("FunctionError") or "none")
            break
        except Exception as e:
            if attempt == 2:
                warns.append(f"checker still throttled: {str(e)[:70]}")
            time.sleep(20)

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns), markers=found,
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
