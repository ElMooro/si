"""ops 4601 — NY Fed priority #2 live (Khalid: FRED #1, NY Fed #2).

The deep family existed but never ran on cadence (schedule=None x3).
rev-A wired the docstring cadences; rev-B put nyfed on the health strip
behind fred. This op: FRED guard, create the three schedules, kick all
three engines, verify outputs move, force a sentinel sweep and assert
the nyfed pipeline sits at position 2.
"""
import json
import sys
import time
from datetime import datetime, timezone

import boto3
from botocore.config import Config

from ops_report import report

B = "justhodl-dashboard-live"
ACCT = "857687956942"
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=180,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name="us-east-1")
sch = boto3.client("scheduler", region_name="us-east-1")
SCHED_ROLE = "arn:aws:iam::857687956942:role/justhodl-scheduler-role"

SCHEDULES = {
    "justhodl-nyfed-full-history": ("justhodl-nyfed-full-history-nightly",
                                    "cron(40 4 * * ? *)"),
    "justhodl-nyfed-markets-full": ("justhodl-nyfed-markets-full-hourly",
                                    "rate(1 hour)"),
    "justhodl-nyfed-repo-deep": ("justhodl-nyfed-repo-deep-daily",
                                 "cron(22 5 * * ? *)"),
}


def gj(key):
    try:
        return json.loads(s3.get_object(Bucket=B, Key=key)["Body"].read())
    except Exception:
        return {}


def contract(r, name, cond, why):
    if cond:
        r.ok("  [%s] %s" % (name, why))
        return 0
    r.fail("  [%s] CONTRACT MISS — %s" % (name, why))
    return 1


def main():
    with report("4601_nyfed_p2") as r:
        r.heading("ops 4601 — NY Fed priority #2")
        misses = 0

        r.section("0. FRED untouched (guard)")
        st = gj("data/_state/fred-scoped-import.json")
        misses += contract(r, "fred-guard",
                           st.get("import_scope") == "full_catalog",
                           "scope=%s ver=%s rpm=%s imported=%s"
                           % (st.get("import_scope"),
                              st.get("engine_version"),
                              st.get("rate_rpm"),
                              st.get("series_imported")))

        r.section("1. Schedules")
        for fn, (name, expr) in SCHEDULES.items():
            arn = ("arn:aws:lambda:us-east-1:%s:function:%s"
                   % (ACCT, fn))
            try:
                sch.get_schedule(Name=name)
                r.log("  %s exists" % name)
            except Exception:
                sch.create_schedule(
                    Name=name, ScheduleExpression=expr,
                    FlexibleTimeWindow={"Mode": "OFF"},
                    Target={"Arn": arn, "RoleArn": SCHED_ROLE},
                    Description="NY Fed priority #2 (ops 4601)")
                r.ok("  created %s (%s)" % (name, expr))

        r.section("2. Kick all three + baselines")
        try:
            base_ls = s3.get_object(
                Bucket=B, Key="data/warm/nyfed/latest-summary.json"
            )["LastModified"]
        except Exception:
            base_ls = None
        base_pd = gj("data/warm/nyfed-markets/pd-state.json")
        r.log("  pd-state keys: %s" % sorted(base_pd)[:10])
        for fn in SCHEDULES:
            lam.invoke(FunctionName=fn, InvocationType="Event")
            r.log("  kicked %s" % fn)

        r.section("3. Poll outputs (engines 300-600s budgets)")
        t0 = time.time()
        got_rates = got_pd = False
        while time.time() - t0 < 660 and not (got_rates and got_pd):
            time.sleep(20)
            if not got_rates:
                try:
                    lm = s3.get_object(
                        Bucket=B,
                        Key="data/warm/nyfed/latest-summary.json"
                    )["LastModified"]
                    if base_ls is None or lm > base_ls:
                        got_rates = True
                        r.log("  rates history refreshed (%ss)"
                              % int(time.time() - t0))
                except Exception:
                    pass
            if not got_pd:
                pd2 = gj("data/warm/nyfed-markets/pd-state.json")
                if pd2 and pd2 != base_pd:
                    got_pd = True
                    r.log("  pd worklist state moved (%ss): %s"
                          % (int(time.time() - t0),
                             {k: pd2[k] for k in sorted(pd2)[:6]
                              if not isinstance(pd2[k], (list, dict))}))
        misses += contract(r, "nyfed", got_rates,
                           "rate full-history run wrote latest-summary")
        misses += contract(r, "nyfed", got_pd,
                           "markets-full advanced the PD worklist")
        try:
            rp = s3.list_objects_v2(
                Bucket=B, Prefix="data/warm/nyfed-markets/rp-")
            newest = max(rp.get("Contents", []),
                         key=lambda o: o["LastModified"], default=None)
            if newest:
                r.log("  repo-deep newest: %s @ %s (%.1f MB)"
                      % (newest["Key"].split("/")[-1],
                         newest["LastModified"].strftime("%H:%M"),
                         newest["Size"] / 1e6))
        except Exception:
            pass

        r.section("4. Sentinel sweep — nyfed at position 2")
        lam.invoke(FunctionName="justhodl-import-sentinel",
                   InvocationType="RequestResponse")
        ih = gj("data/import-health.json")
        names = [p.get("name") for p in (ih.get("pipelines") or [])]
        r.log("  pipeline order: %s" % names[:6])
        ny = next((p for p in (ih.get("pipelines") or [])
                   if p.get("name") == "nyfed"), {})
        misses += contract(r, "sentinel",
                           len(names) > 1 and names[1] == "nyfed",
                           "nyfed at position 2 (status=%s · %s)"
                           % (ny.get("status"),
                              str(ny.get("detail"))[:90]))

        r.section("verdict")
        if misses:
            r.fail("nyfed p2: %d red" % misses)
            sys.exit(1)
        r.ok("NY Fed running as priority #2 — cadences wired, deep pulls "
             "moving, strip shows fred then nyfed")


if __name__ == "__main__":
    main()
