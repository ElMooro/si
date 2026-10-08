"""ops 4600 — provider acceleration (Khalid: FRED as-is, speed the rest).

rev-A: sdmx-walker retry-failures pass (OECD 991 + StatCan 293 denied
recover-or-explain, constrained variants for OECD). rev-B: sec-bulk's
missing weekly schedule wired. This op: FRED-untouched guard, both
retry sweeps with before/after + reason histograms, schedule creation
+ kick for sec-bulk.
"""
import json
import sys
import time

import boto3
from botocore.config import Config

from ops_report import report

B = "justhodl-dashboard-live"
ACCT = "857687956942"
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=120,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name="us-east-1")
sch = boto3.client("scheduler", region_name="us-east-1")
SCHED_ROLE = "arn:aws:iam::857687956942:role/justhodl-scheduler-role"


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
    with report("4600_provider_accel") as r:
        r.heading("ops 4600 — provider acceleration")
        misses = 0

        r.section("0. FRED untouched (guard, read-only)")
        st = gj("data/_state/fred-scoped-import.json")
        ssm = boto3.client("ssm", region_name="us-east-1")
        knob = "?"
        try:
            knob = ssm.get_parameter(
                Name="/justhodl/fred/rate-ceiling")["Parameter"]["Value"]
        except Exception:
            pass
        misses += contract(r, "fred-guard",
                           st.get("import_scope") == "full_catalog"
                           and str(knob) == "100",
                           "scope=%s knob=%s ver=%s rpm=%s imported=%s "
                           "— nothing written"
                           % (st.get("import_scope"), knob,
                              st.get("engine_version"),
                              st.get("rate_rpm"),
                              st.get("series_imported")))

        r.section("1. Settle walker, launch retry sweeps")
        t0 = time.time()
        while time.time() - t0 < 300:
            try:
                c = lam.get_function(
                    FunctionName="justhodl-sdmx-walker")["Configuration"]
                if c.get("LastUpdateStatus") == "Successful" \
                        and c.get("State") == "Active":
                    break
            except Exception:
                pass
            time.sleep(6)
        before = {}
        for ag in ("oecd", "statcan"):
            s0 = gj("data/_state/sdmx-walk-%s.json" % ag)
            before[ag] = {"fail": len(s0.get("failures") or {}),
                          "ok": s0.get("retried_ok", 0),
                          "rfail": s0.get("retried_fail", 0)}
            r.log("  %s before: n_failures=%d retried_ok=%s"
                  % (ag, before[ag]["fail"], before[ag]["ok"]))
            lam.invoke(FunctionName="justhodl-sdmx-walker",
                       InvocationType="Event",
                       Payload=json.dumps({"agency": ag,
                                           "retry_failures": 1}).encode())
            r.log("  %s retry sweep launched" % ag)

        r.section("2. Poll sweeps (walker budget ~700s)")
        t0 = time.time()
        pend = {"oecd", "statcan"}
        after = {}
        last_move = {ag: time.time() for ag in pend}
        last_cnt = {ag: before[ag]["ok"] + before[ag]["rfail"]
                    for ag in pend}
        while pend and time.time() - t0 < 760:
            time.sleep(20)
            for ag in list(pend):
                s1 = gj("data/_state/sdmx-walk-%s.json" % ag)
                cnt = (s1.get("retried_ok", 0)
                       + s1.get("retried_fail", 0))
                if cnt > last_cnt[ag]:
                    last_cnt[ag] = cnt
                    last_move[ag] = time.time()
                    r.log("  %s progress: attempted=%d failures_now=%d"
                          % (ag, cnt - before[ag]["ok"]
                             - before[ag]["rfail"],
                             len(s1.get("failures") or {})))
                lease_free = (s1.get("lease_until") or 0) < time.time()
                # done = sweep quiet 90s AND lease free AND ran >=120s
                if (lease_free and time.time() - last_move[ag] > 90
                        and time.time() - t0 > 120):
                    after[ag] = s1
                    pend.discard(ag)
        for ag in pend:
            after[ag] = gj("data/_state/sdmx-walk-%s.json" % ag)
            r.warn("  %s sweep still running at poll end — asserting on "
                   "current state" % ag)

        r.section("3. Sweep results")
        from collections import Counter
        for ag in ("oecd", "statcan"):
            s1 = after.get(ag) or {}
            nf = len(s1.get("failures") or {})
            ok_d = s1.get("retried_ok", 0) - before[ag]["ok"]
            fl_d = s1.get("retried_fail", 0) - before[ag]["rfail"]
            hist = Counter(str(v)[:34]
                           for v in (s1.get("failures") or {}).values())
            r.log("  %s: recovered=%d refailed=%d failures %d->%d; "
                  "remaining top: %s"
                  % (ag, ok_d, fl_d, before[ag]["fail"], nf,
                     hist.most_common(4)))
            misses += contract(r, ag,
                               ok_d + fl_d >= 1,
                               "retry pass executed (%d attempted)"
                               % (ok_d + fl_d))
            if ag == "oecd":
                misses += contract(r, ag, ok_d >= 1 or nf == 0,
                                   "constrained variants recovered %d of "
                                   "the denied set" % ok_d)

        r.section("4. sec-bulk: schedule + kick")
        arn = ("arn:aws:lambda:us-east-1:%s:function:justhodl-sec-bulk"
               % ACCT)
        try:
            sch.get_schedule(Name="justhodl-sec-bulk-weekly")
            r.log("  schedule exists")
        except Exception:
            sch.create_schedule(
                Name="justhodl-sec-bulk-weekly",
                ScheduleExpression="cron(0 9 ? * MON *)",
                FlexibleTimeWindow={"Mode": "OFF"},
                Target={"Arn": arn, "RoleArn": SCHED_ROLE},
                Description="SEC bulk XBRL weekly (was never wired)")
            r.ok("  schedule created: Mon 09 UTC")
        try:
            resp = s3.list_objects_v2(Bucket=B,
                                      Prefix="data/warm/sec-bulk/")
            for o in resp.get("Contents", [])[:4]:
                r.log("  %s  %s  %.1f MB"
                      % (o["Key"].split("/")[-1],
                         o["LastModified"].strftime("%m-%d %H:%M"),
                         o["Size"] / 1e6))
        except Exception:
            pass
        lam.invoke(FunctionName="justhodl-sec-bulk",
                   InvocationType="Event")
        r.ok("  kicked (multi-GB pull — freshness reads next check-in)")

        r.section("verdict")
        if misses:
            r.fail("provider acceleration: %d red" % misses)
            sys.exit(1)
        r.ok("acceleration live — denied ledgers shrinking or explained, "
             "sec-bulk on cadence, FRED untouched")


if __name__ == "__main__":
    main()
