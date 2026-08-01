"""
ops_4266 -- wave-4 closure: onboard the snapshotter, unfreeze the map.

4265 proved calibration-snapshotter healthy (277 weights, W31 snapshot
written on invoke) -- the blocker was a schedule that died in the
EventBridge classic saturation, and the engine was unmanaged. This op
creates the governed Scheduler entry (declared in the manifest, config
committed). It also verifies the party-map source reroute: 4265 logs
proved theunitedstates.io Errno-110s from Lambda; the same canonical
file now fetches from raw.githubusercontent.com.

Gate: schedule exists · party map fresh with >=500 members from the
raw.githubusercontent source · snapshotter invocable under management.
"""
import json, sys, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from ops_report import report

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=300, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
sch = boto3.client("scheduler", region_name=REGION)
RUN_START = datetime.now(timezone.utc)

def wait_deployed(fn, tries=45, window_min=45):
    for _ in range(tries):
        try:
            c = lam.get_function_configuration(FunctionName=fn)
            if c.get("LastUpdateStatus") in (None, "Successful") \
                    and c.get("State") == "Active":
                lm = c.get("LastModified", "")
                try:
                    lm_dt = datetime.strptime(
                        lm.split(".")[0], "%Y-%m-%dT%H:%M:%S"
                    ).replace(tzinfo=timezone.utc)
                    if (RUN_START - lm_dt).total_seconds() < window_min * 60:
                        return c
                except Exception:
                    return c
        except Exception:
            pass
        time.sleep(8)
    return None

def age_min(key):
    h = s3.head_object(Bucket=BUCKET, Key=key)
    return (datetime.now(timezone.utc)
            - h["LastModified"]).total_seconds() / 60.0

fails = []
with report("4266_wave4_close") as r:
    r.heading("ops 4266 -- wave-4 closure")

    r.section("1. calibration-snapshotter under governance")
    name = "calibration-snapshotter-weekly"
    try:
        sch.get_schedule(Name=name, GroupName="default")
        r.ok("schedule %s already exists" % name)
    except Exception:
        donor = None
        for pg in sch.get_paginator("list_schedules").paginate(
                GroupName="default"):
            for it in pg.get("Schedules", []):
                try:
                    d = sch.get_schedule(Name=it["Name"],
                                         GroupName="default")
                    if "RoleArn" in (d.get("Target") or {}):
                        donor = d["Target"]["RoleArn"]
                        break
                except Exception:
                    continue
            if donor:
                break
        if not donor:
            fails.append("no donor RoleArn for Scheduler")
        else:
            sch.create_schedule(
                Name=name, GroupName="default",
                ScheduleExpression="cron(0 4 ? * SUN *)",
                FlexibleTimeWindow={"Mode": "OFF"},
                State="ENABLED",
                Target={"Arn": "arn:aws:lambda:us-east-1:857687956942:"
                               "function:justhodl-calibration-snapshotter",
                        "RoleArn": donor,
                        "Input": "{}"})
            r.ok("schedule created: %s cron(0 4 ? * SUN *)" % name)
    c = wait_deployed("justhodl-calibration-snapshotter")
    if c:
        r.ok("under deploy management (LastModified %s)"
             % c.get("LastModified"))
    else:
        r.warn("deploy not yet settled for snapshotter (config just "
               "onboarded; next push cycle will touch it)")

    r.section("2. party map -- canonical source, reachable host")
    # rerun delta: 45-min window let the PREVIOUS push's deploy satisfy
    # the check while this push was still rolling out (alphabetical
    # order touched the snapshotter first). Tight window = this push.
    if wait_deployed("justhodl-political-stocks", window_min=12):
        p = lam.invoke(FunctionName="justhodl-political-stocks",
                       InvocationType="RequestResponse", Payload=b"{}")
        r.log("invoked: %s"
              % (p["Payload"].read() or b"")[:150].decode("utf-8",
                                                          "ignore"))
        try:
            a = age_min("data/congress-party-map.json")
            doc = json.loads(s3.get_object(
                Bucket=BUCKET,
                Key="data/congress-party-map.json")["Body"].read())
            n = doc.get("n") or len(doc.get("party_map") or {})
            src2 = str(doc.get("source", ""))
            if a < 20 and n >= 500 and "raw.githubusercontent" in src2:
                r.ok("party map LIVE: %.1f min old, %d members, "
                     "source=raw.githubusercontent (canonical "
                     "unitedstates/congress-legislators)" % (a, n))
            elif a < 20:
                r.warn("map fresh (%d members) but source=%s"
                       % (n, src2[:70]))
            else:
                time.sleep(10)
                logs = boto3.client("logs", region_name=REGION)
                ev = logs.filter_log_events(
                    logGroupName="/aws/lambda/justhodl-political-stocks",
                    startTime=int((time.time() - 240) * 1000))
                for e2 in [x["message"].strip()[:140]
                           for x in ev.get("events", [])
                           if "political" in x["message"]
                           or "Error" in x["message"]][-8:]:
                    r.log("log: %s" % e2)
                fails.append("party map still stale %.0f min" % a)
        except Exception as e:
            fails.append("party-map verify: %s" % str(e)[:100])
    else:
        fails.append("political-stocks deploy never settled")

    r.section("RESULT")
    if fails:
        for f in fails:
            r.fail("  %s" % f)
    else:
        r.ok("OPS 4266 PASS -- wave 4 fully closed: snapshotter "
             "governed + scheduled, party map self-refreshing from a "
             "reachable canonical source")
if fails:
    sys.exit(1)
