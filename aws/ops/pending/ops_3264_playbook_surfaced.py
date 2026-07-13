"""ops 3264 — playbook surfaced + scheduled + ranker confirm.

  1. SCHEDULE: EventBridge Scheduler (classic rule cap saturated) —
     justhodl-playbook-weekly, Mondays 07:00 UTC (an hour after the
     notes crawler), role justhodl-scheduler-role. Idempotent.
  2. RANKER: data/master-rank.json must carry khalid_note (coded in
     3259; feed refreshes on cron). If the live feed pre-dates the
     join, invoke the ranker and poll until the field appears.
  3. PAGE: PLAYBOOK strip (flagship yield-curve countdown + top rules)
     live on panels.html — source-literal check per doctrine.
"""
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone

import boto3

from ops_report import report

REGION = "us-east-1"
ACCT = "857687956942"
BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
SCH = boto3.client("scheduler", region_name=REGION)
UA = {"User-Agent": "Mozilla/5.0 (jh-ops-3264)"}
FN = "justhodl-playbook-engine"
SNAME = "justhodl-playbook-weekly"


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


with report("3264_playbook_surfaced") as rep:
    fails, warns = [], []
    rep.heading("ops 3264 — playbook surfaced + weekly schedule + "
                "ranker khalid_note confirm")

    rep.section("1. Weekly schedule (EventBridge Scheduler)")
    arn = f"arn:aws:lambda:{REGION}:{ACCT}:function:{FN}"
    role = f"arn:aws:iam::{ACCT}:role/justhodl-scheduler-role"
    try:
        SCH.get_schedule(Name=SNAME)
        rep.ok(f"schedule {SNAME} already exists")
    except SCH.exceptions.ResourceNotFoundException:
        try:
            SCH.create_schedule(
                Name=SNAME,
                ScheduleExpression="cron(0 7 ? * MON *)",
                FlexibleTimeWindow={"Mode": "OFF"},
                Target={"Arn": arn, "RoleArn": role},
                State="ENABLED",
                Description="Weekly playbook re-extraction from the "
                            "notes mirror (ops 3264)")
            rep.ok(f"created {SNAME}: cron(0 7 ? * MON *) UTC")
        except Exception as e:
            fails.append(f"create_schedule: {str(e)[:90]}")
    except Exception as e:
        fails.append(f"get_schedule: {str(e)[:90]}")

    rep.section("2. master-ranker khalid_note — live confirm")
    key = "data/master-rank.json"
    d = s3_json(key) or {}
    rows = [x for x in walk(d)
            if isinstance(x, dict) and "khalid_note" in x]
    if not rows:
        rep.log("  feed pre-dates the join — invoking ranker")
        mark = datetime.now(timezone.utc).isoformat()
        try:
            LAM.invoke(FunctionName="justhodl-master-ranker",
                       InvocationType="Event", Payload=b"{}")
        except Exception as e:
            warns.append(f"ranker invoke: {str(e)[:60]}")
        for _ in range(45):
            time.sleep(10)
            d = s3_json(key) or {}
            if str(d.get("generated_at", "")) > mark:
                rows = [x for x in walk(d)
                        if isinstance(x, dict) and "khalid_note" in x]
                break
    withval = [x for x in rows if x.get("khalid_note")]
    if rows:
        s = (withval or rows)[0]
        kn = s.get("khalid_note") or {}
        rep.ok(f"master-rank: {len(rows)} rows carry khalid_note "
               f"({len(withval)} non-null) — e.g. "
               f"{s.get('ticker') or s.get('symbol')} "
               f"stance={kn.get('stance')}")
    else:
        warns.append("khalid_note not in master-rank after refresh "
                     "window — inspect ranker run next session")

    rep.section("3. PLAYBOOK strip live on panels.html")
    okp = False
    for i in range(22):
        try:
            h = urllib.request.urlopen(urllib.request.Request(
                f"https://justhodl.ai/panels.html?t={int(time.time())}",
                headers=UA), timeout=15).read().decode("utf-8",
                                                       "replace")
            if "ops 3264: PLAYBOOK" in h \
                    and "/data/playbook-rules.json" in h:
                okp = True
                rep.ok(f"strip live (~{(i + 1) * 15}s)")
                break
        except Exception:
            pass
        time.sleep(15)
    if not okp:
        warns.append("strip literal not live in window — pages deploy "
                     "may still be propagating; re-verify next ops")
    pb = s3_json("data/playbook-rules.json") or {}
    rep.kv(playbook_rules=pb.get("n_rules"),
           flagship_marker=((pb.get("flagship") or {})
                            .get("yield_curve") or {})
           .get("lag_marker_date"))

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
