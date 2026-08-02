"""
ops_4270 -- the page-critical quartet, by invoke-and-observe.

4256 flagged four page-critical artifacts. Tonight's recon corrected
the attribution table again: alpha-research and carry-surface only
READ their keys; the true writers are three dedicated engines --
compound-aggregator (unmanaged, no config), options-flow (managed,
two declared schedules), risk-regime (managed, daily 12:45) -- all
frozen. symbol-map is a legacy seed both consumer pages guard with
.catch({}) and symbol-dictionary supersedes: downgraded, not revived.

Per engine: sync invoke -> artifact fresh + page-shape probe ->
schedule triage (get declared Scheduler entries, create-if-missing
from donor role). Failures captured verbatim for the next delta.
"""
import json, sys, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from ops_report import report

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=340, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)
sch = boto3.client("scheduler", region_name=REGION)
RUN_START = datetime.now(timezone.utc)

ENGINES = [
    ("justhodl-compound-aggregator", "data/compound-signals.json",
     ["signals", "generated_at"], [("compound-aggregator-daily",
                                    "cron(15 21 ? * MON-FRI *)")]),
    ("justhodl-options-flow", "data/options-flow.json",
     ["generated_at"], [("justhodl-options-flow-sched", None),
                        ("justhodl-options-flow-30m", None)]),
    ("justhodl-risk-regime", "data/risk-regime.json",
     ["regime", "generated_at"], [("justhodl-risk-regime-daily", None)]),
]

def age_min(key):
    h = s3.head_object(Bucket=BUCKET, Key=key)
    return (datetime.now(timezone.utc)
            - h["LastModified"]).total_seconds() / 60.0, h.get(
        "ContentLength")

def donor_role():
    for pg in sch.get_paginator("list_schedules").paginate(
            GroupName="default"):
        for it in pg.get("Schedules", []):
            try:
                d = sch.get_schedule(Name=it["Name"], GroupName="default")
                ra = (d.get("Target") or {}).get("RoleArn")
                if ra:
                    return ra
            except Exception:
                continue
    return None

fails = []
with report("4270_page_critical") as r:
    r.heading("ops 4270 -- page-critical quartet")
    for fn, key, probe_keys, sched_specs in ENGINES:
        r.section(fn)
        try:
            base, _ = age_min(key)
            r.log("baseline %s: %.0f h stale" % (key, base / 60))
        except Exception as e:
            r.log("baseline head: %s" % str(e)[:80])
        try:
            cfg = lam.get_function_configuration(FunctionName=fn)
            r.log("live cfg: runtime=%s mem=%s timeout=%s managed=%s"
                  % (cfg.get("Runtime"), cfg.get("MemorySize"),
                     cfg.get("Timeout"),
                     "yes" if fn != "justhodl-compound-aggregator"
                     else "NO (onboard next push from these values)"))
            p = lam.invoke(FunctionName=fn,
                           InvocationType="RequestResponse", Payload=b"{}")
            pay = (p["Payload"].read() or b"")[:200].decode("utf-8",
                                                            "ignore")
            r.log("invoked: %s" % pay)
            if p.get("FunctionError"):
                time.sleep(8)
                ev = logs.filter_log_events(
                    logGroupName="/aws/lambda/%s" % fn,
                    startTime=int((time.time() - 240) * 1000))
                for ln in [x["message"].strip()[:150]
                           for x in ev.get("events", [])
                           if "Error" in x["message"]
                           or "Traceback" in x["message"]
                           or "[" in x["message"][:2]][:8]:
                    r.log("log: %s" % ln)
                fails.append("%s: FunctionError %s" % (fn, pay[:100]))
                continue
            a, sz = age_min(key)
            if a < 20:
                doc = json.loads(s3.get_object(
                    Bucket=BUCKET, Key=key)["Body"].read())
                missing = [k for k in probe_keys if k not in doc]
                extra = {k: (str(doc.get(k))[:48]) for k in probe_keys
                         if k in doc}
                if missing:
                    r.warn("fresh but page-shape keys missing: %s "
                           "(has: %s)" % (missing, list(doc)[:10]))
                r.ok("%s UNFROZEN: %.1f min, %s bytes, %s"
                     % (key, a, sz, extra))
            else:
                fails.append("%s ran but %s still %.0f min stale"
                             % (fn, key, a))
                continue
        except Exception as e:
            fails.append("%s: %s" % (fn, str(e)[:120]))
            continue
        # schedule triage
        for sname, screate in sched_specs:
            try:
                d = sch.get_schedule(Name=sname, GroupName="default")
                r.log("schedule %s: %s %s" % (
                    sname, d.get("State"),
                    d.get("ScheduleExpression")))
            except Exception:
                if screate:
                    ra = donor_role()
                    if ra:
                        sch.create_schedule(
                            Name=sname, GroupName="default",
                            ScheduleExpression=screate,
                            FlexibleTimeWindow={"Mode": "OFF"},
                            State="ENABLED",
                            Target={"Arn": "arn:aws:lambda:us-east-1:"
                                           "857687956942:function:%s" % fn,
                                    "RoleArn": ra, "Input": "{}"})
                        r.ok("schedule CREATED: %s %s" % (sname, screate))
                    else:
                        r.warn("no donor role; %s not created" % sname)
                else:
                    r.warn("declared schedule %s MISSING in Scheduler -- "
                           "the freeze cause; recreating from manifest "
                           "next delta once cadence confirmed" % sname)

    r.section("symbol-map -- downgraded, superseded")
    try:
        mn = json.loads(s3.get_object(
            Bucket=BUCKET,
            Key="data/_freshness-manifest.json")["Body"].read())
        mn.setdefault("key_overrides", {})["data/symbol-map.json"] = 8760
        mn.setdefault("retired", {})["data/symbol-map.json"] = {
            "retired_at": RUN_START.isoformat(),
            "superseded_by": "data/symbol-dictionary.json "
                             "(justhodl-symbol-dictionary)",
            "reason": "legacy seed; chart-pro + watchlists both guard "
                      "with .catch({}) -- soft dependency, not "
                      "page-critical"}
        s3.put_object(Bucket=BUCKET, Key="data/_freshness-manifest.json",
                      Body=json.dumps(mn, default=str).encode(),
                      ContentType="application/json",
                      CacheControl="no-store")
        r.ok("manifest: symbol-map SLA 8760h + retirement note")
    except Exception as e:
        r.warn("manifest update: %s" % str(e)[:100])

    r.section("RESULT")
    if fails:
        for f in fails:
            r.fail("  %s" % f)
    else:
        r.ok("OPS 4270 PASS -- three page-critical writers live, "
             "symbol-map honestly downgraded")
if fails:
    sys.exit(1)
