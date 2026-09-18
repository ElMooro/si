"""ops 5708 -- the student's Monday wall: live rehearsal + schedules (Claude, 2026-09-18). Direct lane.

1. wall-prepare for real (bars from the warehouse, 6 owned forecasts submitted for the week of 2026-09-21)
2. wait for the owned answers, then wall-post with rehearse=true: every entry validated by the door against Monday
   09:31 ET, NOTHING written to the wall
3. two EventBridge Scheduler entries in America/New_York: wall-prepare Mon 09:05, wall-post Mon 09:31
   (create-if-absent; the scheduler role and the justhodl-ai target follow the existing justhodl-ai-* schedules)
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import boto3
from botocore.config import Config

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402

REGION, FN = "us-east-1", "justhodl-ai"


def invoke(lam, payload):
    resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=json.dumps(payload).encode())
    body = resp["Payload"].read()
    try:
        return json.loads(body)
    except Exception:  # noqa: BLE001
        return {"raw": body[:500].decode("utf-8", "replace")}


def main():
    cfg = Config(read_timeout=910, connect_timeout=10, retries={"max_attempts": 0})
    lam = boto3.client("lambda", region_name=REGION, config=cfg)
    sch = boto3.client("scheduler", region_name=REGION)
    with report("5708_wall_rehearsal") as r:
        r.heading("ops 5708 -- Monday wall rehearsal + schedules")
        r.section("1. Prepare (real bars, real owned submissions)")
        prep = invoke(lam, {"mode": "wall-prepare"})
        r.kv(ok=prep.get("ok"), week=prep.get("week"), last_session=prep.get("sessions"), symbols=json.dumps(prep.get("symbols"))[:700], error=prep.get("error") or prep.get("raw"))
        if not prep.get("ok"):
            r.fail("prepare failed"); sys.exit(1)
        r.section("2. Rehearse the post against Monday 09:31 ET (nothing written)")
        receipt = None
        for i in range(12):
            time.sleep(45)
            receipt = invoke(lam, {"mode": "wall-post", "body": {"rehearse": True}})
            owned = [e for e in (receipt.get("entries") or []) if e.get("agent") == "student-owned"]
            pending = [s for s in (receipt.get("skipped") or []) if "queued" in str(s.get("reason")) or "running" in str(s.get("reason"))]
            r.log("t+%2d min: entries=%d (owned %d) skipped=%d pending=%d" % ((i + 1) * 45 // 60, len(receipt.get("entries") or []), len(owned), len(receipt.get("skipped") or []), len(pending)))
            if owned and not pending:
                break
        for e in receipt.get("entries") or []:
            r.log("  %-14s %-4s %-5s %-10s crisis=%.2f %s" % (e["agent"], e["symbol"], e["direction"], e["regime"], e["crisis_probability"], e["status"]))
        for s_ in receipt.get("skipped") or []:
            r.warn("  skipped %s %s: %s" % (s_["agent"], s_["symbol"], s_["reason"]))
        n_ok = len([e for e in receipt.get("entries") or [] if e.get("status") == "rehearsed"])
        (r.ok if n_ok >= 6 else r.fail)("%d entries pass the door in rehearsal" % n_ok)
        r.section("3. Schedules (America/New_York)")
        existing = {s["Name"] for page in sch.get_paginator("list_schedules").paginate(NamePrefix="justhodl-ai-wall") for s in page.get("Schedules", [])}
        ref = next((s for page in sch.get_paginator("list_schedules").paginate(NamePrefix="justhodl-ai-") for s in page.get("Schedules", []) if not s["Name"].startswith("justhodl-ai-wall")), None)
        if not ref:
            r.fail("no existing justhodl-ai-* schedule to copy the role/target from"); sys.exit(1)
        refd = sch.get_schedule(Name=ref["Name"], GroupName=ref.get("GroupName", "default"))
        target = refd["Target"]
        for name, cron, mode in (("justhodl-ai-wall-prepare", "cron(5 9 ? * MON *)", "wall-prepare"), ("justhodl-ai-wall-post", "cron(31 9 ? * MON *)", "wall-post")):
            if name in existing:
                r.log("%s exists" % name); continue
            sch.create_schedule(Name=name, GroupName=refd.get("GroupName", "default"), ScheduleExpression=cron, ScheduleExpressionTimezone="America/New_York",
                                FlexibleTimeWindow={"Mode": "OFF"}, State="ENABLED",
                                Target={"Arn": target["Arn"], "RoleArn": target["RoleArn"], "Input": json.dumps({"mode": mode}), "RetryPolicy": {"MaximumRetryAttempts": 0}},
                                Description="student posts on the Monday wall: %s (v2.5.0)" % mode)
            r.ok("created %s %s" % (name, cron))
        if n_ok < 6:
            sys.exit(1)


if __name__ == "__main__":
    main()
