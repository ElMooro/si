"""ops 6593 - READ-ONLY: the 21 live triggers the schedule reconciler reports as UNDECLARED (alarm justhodl-schedule-drift,
21 at 07:31 UTC daily).  Full definitions in manifest shape (rules: describe_rule + targets; schedules: get_schedule),
so the repo copy of config/schedule-manifest.json can declare them and the S3 copy follow (ops 6594).  A target's
Input is kept only when it is short JSON with no key/token/secret-looking member; otherwise its length is recorded.
Also: is justhodl-katlin-permission-refresh (declared as a Scheduler schedule, reported as an undeclared events rule)
really two triggers for one job?
"""
from pathlib import Path
import json
import re
import sys

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
CFG = Config(retries={"max_attempts": 8, "mode": "standard"}, connect_timeout=10, read_timeout=120)
SECRETISH = re.compile(r"key|token|secret|password|authorization|bearer", re.I)


def c(name):
    return boto3.client(name, region_name=REGION, config=CFG)


def safe_input(text):
    if not text:
        return None
    if len(text) <= 400 and not SECRETISH.search(text):
        try:
            json.loads(text)
            return text
        except ValueError:
            pass
    return "<input %d chars, not recorded>" % len(text)


def main():
    s3, evb, sch = c("s3"), c("events"), c("scheduler")
    with report("ops_6593_undeclared_triggers") as r:
        r.heading("ops 6593 - the undeclared live triggers behind the schedule-drift alarm (read-only)")
        sd = json.loads(s3.get_object(Bucket=LIVE, Key="data/schedule-drift.json")["Body"].read())
        und = [d for d in (sd.get("drifts") or sd.get("findings") or []) if (d.get("drift") or d.get("cls")) == "UNDECLARED"]
        r.log("drift file generated %s, mode %s, %d drifts, %d UNDECLARED; keys look like: %s" % (sd.get("generated_at"), sd.get("mode"), len(sd.get("drifts") or []), len(und), [d.get("key") for d in und[:4]]))
        rules, scheds = [], []
        for d in und:
            k = d.get("key") or ""
            try:
                if "/" in k:
                    group, name = k.split("/", 1)
                    s_ = sch.get_schedule(Name=name, GroupName=group)
                    t = s_.get("Target") or {}
                    scheds.append({"expr": s_.get("ScheduleExpression"), "group": group, "kind": "scheduler", "name": name, "state": s_.get("State"),
                                   "timezone": s_.get("ScheduleExpressionTimezone") or "UTC",
                                   "targets": [{"arn": t.get("Arn"), "input": safe_input(t.get("Input")), "path": None}],
                                   "_description": (s_.get("Description") or "")[:120], "_created": str(s_.get("CreationDate") or "")[:19]})
                else:
                    rule = evb.describe_rule(Name=k)
                    tg = evb.list_targets_by_rule(Rule=k).get("Targets", [])
                    rules.append({"expr": rule.get("ScheduleExpression"), "kind": "events", "name": k, "state": rule.get("State"),
                                  "targets": [{"arn": t.get("Arn"), "id": t.get("Id"), "input": safe_input(t.get("Input")), "path": t.get("InputPath")} for t in tg],
                                  "_description": (rule.get("Description") or "")[:120], "_bus": rule.get("EventBusName")})
            except Exception as e:  # noqa: BLE001
                r.warn("%s: %s" % (k, type(e).__name__))
        r.section("rules (events)")
        for x in rules:
            r.log(json.dumps(x))
        r.section("schedules (scheduler)")
        for x in scheds:
            r.log(json.dumps(x))
        r.section("katlin-permission-refresh")
        try:
            s_ = sch.get_schedule(Name="justhodl-katlin-permission-refresh", GroupName="default")
            r.log("schedule: %s %s target %s input %s" % (s_.get("ScheduleExpression"), s_.get("State"), (s_.get("Target") or {}).get("Arn"), safe_input((s_.get("Target") or {}).get("Input"))))
        except Exception as e:  # noqa: BLE001
            r.log("schedule: %s" % type(e).__name__)
        kr = [x for x in rules if x["name"] == "justhodl-katlin-permission-refresh"]
        r.log("rule: %s" % (json.dumps(kr[0]) if kr else "none"))
        r.kv(undeclared=len(und), rules=len(rules), schedules=len(scheds))
        r.ok("definitions read")


if __name__ == "__main__":
    main()
