"""ops 3225 — the discriminator: this run's trace weekly-counts + the
verbatim [series_source] failure for any FRED-class miss, plus the
runner's live env keys (names only). New FRED fetches fail in-runner and
succeed ops-side — the error text names which layer."""
import sys
import boto3
from ops_report import report

REGION = "us-east-1"
LOGS = boto3.client("logs", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)

with report("3225_fred_evidence") as rep:
    fails = []
    rep.heading("ops 3225 — FRED-class miss, named")
    env = (LAM.get_function_configuration(
        FunctionName="justhodl-wl-engines")
        .get("Environment") or {}).get("Variables") or {}
    rep.log("  env keys: " + ", ".join(sorted(env.keys()))[:150])
    shown = 0
    grp = "/aws/lambda/justhodl-wl-engines"
    for st in LOGS.describe_log_streams(
            logGroupName=grp, orderBy="LastEventTime",
            descending=True, limit=3).get("logStreams") or []:
        for e in LOGS.get_log_events(
                logGroupName=grp, logStreamName=st["logStreamName"],
                limit=400, startFromHead=False).get("events") or []:
            m = (e.get("message") or "").strip()
            if "[trace]" in m and ("weekly=" in m or "todo=" in m):
                rep.log("  " + m[:150]); shown += 1
            elif "[series_source]" in m and any(
                    k in m for k in ("FRED", "IRLTLT", "ECBDFR",
                                     "IR3TIB01GB")):
                rep.log("  ⚠ " + m[:160]); shown += 1
        if shown >= 10:
            break
    rep.kv(lines=shown)
    if not shown:
        fails.append("no discriminating lines found")
    rep.kv(n_fails=len(fails), verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
