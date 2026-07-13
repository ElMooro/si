"""ops 3265 — master-ranker khalid_note: diagnose, guarantee, prove.

The 3264 window closed without a fresh feed. Decision tree:
  1. Feed generated_at NOW (did the invoked run finish late?).
  2. Log tail of the last run: the '[ranker] fusion overlays:
     ... khalid_notes=N' print proves the join executed; 'Task timed
     out' proves a timeout kill; a traceback proves a crash.
  3. Redeploy repo→live (env-preserving; timeout floored at 900 —
     a heavy ranker on a short fuse would explain everything).
  4. Invoke, poll long (≤12 min), prove khalid_note rows with a
     non-null sample.
"""
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3

from ops_report import report

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "shared"))
from _lambda_deploy_helpers import deploy_lambda  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
LOGS = boto3.client("logs", region_name=REGION)
FN = "justhodl-master-ranker"
KEY = "data/master-rank.json"
AWS_DIR = Path(__file__).resolve().parents[2]


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


def walk(o):
    if isinstance(o, dict):
        yield o
        for v in o.values():
            yield from walk(v)
    elif isinstance(o, list):
        for v in o:
            yield from walk(v)


def tail(want, limit=250):
    out = []
    try:
        grp = f"/aws/lambda/{FN}"
        for stm in LOGS.describe_log_streams(
                logGroupName=grp, orderBy="LastEventTime",
                descending=True, limit=2).get("logStreams") or []:
            for ev in LOGS.get_log_events(
                    logGroupName=grp, logStreamName=stm["logStreamName"],
                    limit=limit, startFromHead=False).get("events") or []:
                m = (ev.get("message") or "").strip()
                if any(k in m for k in want):
                    out.append(m[:150])
    except Exception:
        pass
    return out[-8:]


def field_rows(d):
    rows = [x for x in walk(d)
            if isinstance(x, dict) and "khalid_note" in x]
    return rows, [x for x in rows if x.get("khalid_note")]


with report("3265_ranker_field") as rep:
    fails, warns = [], []
    rep.heading("ops 3265 — ranker khalid_note: diagnose → redeploy → "
                "prove")

    rep.section("1. Current feed + last-run logs")
    d0 = s3_json(KEY) or {}
    rep.kv(feed_generated=str(d0.get("generated_at")
                              or d0.get("as_of"))[:19])
    r0, v0 = field_rows(d0)
    rep.kv(rows_with_field_now=len(r0), non_null_now=len(v0))
    for ln in tail(("fusion overlays", "Task timed out", "ERROR",
                    "Traceback", "DONE in")):
        rep.log("  " + ln)

    if r0:
        s = (v0 or r0)[0]
        rep.ok(f"already live: {len(r0)} rows carry khalid_note "
               f"({len(v0)} non-null) — e.g. "
               f"{s.get('ticker') or s.get('symbol')}")
    else:
        rep.section("2. Redeploy repo→live (env-preserving, "
                    "timeout≥900)")
        live_cfg = LAM.get_function_configuration(FunctionName=FN)
        env = (live_cfg.get("Environment") or {})\
            .get("Variables") or {}
        t_old = live_cfg.get("Timeout")
        m_old = live_cfg.get("MemorySize")
        rep.kv(live_timeout=t_old, live_memory=m_old)
        try:
            deploy_lambda(report=rep, function_name=FN,
                          source_dir=AWS_DIR / "lambdas" / FN
                          / "source",
                          env_vars=env, eb_rule_name=None,
                          eb_schedule=None,
                          timeout=max(int(t_old or 0), 900),
                          memory=int(m_old or 1536),
                          description=str(
                              live_cfg.get("Description") or "")[:250],
                          smoke=False)
            LAM.get_waiter("function_updated_v2").wait(
                FunctionName=FN,
                WaiterConfig={"Delay": 2, "MaxAttempts": 40})
        except Exception as e:
            fails.append(f"deploy: {str(e)[:90]}")

        if not fails:
            rep.section("3. Invoke + long poll")
            mark = datetime.now(timezone.utc).isoformat()
            LAM.invoke(FunctionName=FN, InvocationType="Event",
                       Payload=b"{}")
            fresh = None
            for _ in range(66):
                time.sleep(11)
                d = s3_json(KEY) or {}
                if str(d.get("generated_at", "")) > mark:
                    fresh = d
                    break
            if not fresh:
                for ln in tail(("Task timed out", "ERROR",
                                "Traceback", "fusion overlays",
                                "DONE in")):
                    rep.log("  post: " + ln)
                fails.append("feed not fresh after redeploy+invoke "
                             "(12 min) — logs above")
            else:
                r1, v1 = field_rows(fresh)
                for ln in tail(("fusion overlays",)):
                    rep.log("  " + ln)
                if r1:
                    s = (v1 or r1)[0]
                    kn = s.get("khalid_note") or {}
                    rep.ok(f"PROVEN: {len(r1)} rows carry khalid_note "
                           f"({len(v1)} non-null) — e.g. "
                           f"{s.get('ticker') or s.get('symbol')} "
                           f"stance={kn.get('stance')}")
                else:
                    fails.append("fresh feed still lacks khalid_note "
                                 "— join not reaching payload rows")

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
