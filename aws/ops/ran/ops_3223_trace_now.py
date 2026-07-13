"""ops 3223 — the race named: deploy_lambda awaits the CODE update but not
the CONFIG update, so 3221's invoke ran new code with the old env (no
WL_TRACE). The env has long since propagated — a plain invoke now traces.
Also harvests any [series_source]/pull-FAIL lines from the same run."""
import json
import sys
import time
from datetime import datetime, timezone

import boto3

from ops_report import report

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
LOGS = boto3.client("logs", region_name=REGION)
FN = "justhodl-wl-engines"


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


with report("3223_trace_now") as rep:
    fails, warns = [], []
    rep.heading("ops 3223 — traced run, env confirmed first")

    env = (LAM.get_function_configuration(FunctionName=FN)
           .get("Environment") or {}).get("Variables") or {}
    rep.kv(wl_trace_env="set" if env.get("WL_TRACE") else "MISSING")
    if not env.get("WL_TRACE"):
        fails.append("WL_TRACE still absent from live env")

    mark = datetime.now(timezone.utc).isoformat()
    if not fails:
        LAM.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
        idx2 = None
        for _ in range(60):
            time.sleep(10)
            d = s3_json("data/wl-engines.json") or {}
            if str(d.get("generated_at", "")) > mark:
                idx2 = d
                break
        if not idx2:
            warns.append("index not fresh in window")
        time.sleep(35)
        shown = 0
        grp = f"/aws/lambda/{FN}"
        try:
            for st in LOGS.describe_log_streams(
                    logGroupName=grp, orderBy="LastEventTime",
                    descending=True, limit=4).get("logStreams") or []:
                for e in LOGS.get_log_events(
                        logGroupName=grp,
                        logStreamName=st["logStreamName"],
                        limit=400, startFromHead=False
                        ).get("events") or []:
                    m = (e.get("message") or "").strip()
                    if any(k in m for k in ("[trace]", "pull FAIL",
                                            "[series_source]")):
                        rep.log("  " + m[:170])
                        shown += 1
                if shown >= 14:
                    break
        except Exception as e:
            warns.append(f"logs: {str(e)[:70]}")
        rep.kv(evidence_lines=shown)
        if not shown:
            fails.append("tracer silent even with env confirmed")
        if idx2:
            for nm in ("Europe Liquidity", "Global Deposit Rates"):
                e = next((x for x in (idx2.get("engines") or [])
                          if nm.lower() in str(x.get("name", "")).lower()),
                         None)
                if e:
                    rep.log(f"  {str(e.get('name'))[:38]:<38} "
                            f"{e.get('state')} "
                            f"resolved={e.get('members_resolved')}")
            act = sum(1 for x in (idx2.get("engines") or [])
                      if str(x.get("state")) == "ACTIVE")
            rep.kv(active_now=act)

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
