"""ops 4597 — measure the COMBINED drain rate (Khalid: ~2-day ETA).

Two lanes merged: the parallel session's ops 4594 raised the SSM
ceiling /justhodl/fred/rate-ceiling 60->100 (the live value WAS 60 —
the 60-era cap was real); this session's v2.3.0 removed the serial
dumps+put from the cycle. This op measures the compound effect.

v2.3.0 moved json.dumps+put_object onto a 3-worker pool; the API fetch
stays serial and AIMD-paced (ceiling knob already 100/min — pre-built,
verified). Predicted cycle collapses from ~1.2s to ~0.65s/series. This
op waits out the old-code lease, kicks the chain on new code, measures
a 7-minute banked delta, and reports the honest new rate + ETA.
Contracts: engine 2.3 live, no block/key error, no bank_put error burst,
measured rate > 55/min (target ~85+; logged either way).
"""
import json
import sys
import time
from datetime import datetime, timezone

import boto3
from botocore.config import Config

from ops_report import report

B = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=120,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name="us-east-1")
STATE = "data/_state/fred-scoped-import.json"


def get_json(key):
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
    with report("4597_drain_rate") as r:
        r.heading("ops 4597 — combined drain rate (ceiling 100 x threaded banking)")
        misses = 0

        r.section("1. Settle deploy, roll past the old-code lease")
        t0 = time.time()
        while time.time() - t0 < 240:
            try:
                c = lam.get_function(
                    FunctionName="justhodl-fred-catalog")["Configuration"]
                if c.get("LastUpdateStatus") == "Successful" \
                        and c.get("State") == "Active":
                    break
            except Exception:
                pass
            time.sleep(6)
        st = get_json(STATE)
        lease = float(st.get("lease_until") or 0)
        wait = max(0.0, lease - time.time())
        r.log("  lease_until in %.0fs (old-code invoke may hold it)" % wait)
        time.sleep(min(wait + 10, 320))
        st = get_json(STATE)
        if float(st.get("lease_until") or 0) <= time.time():
            try:
                lam.invoke(FunctionName="justhodl-fred-catalog",
                           InvocationType="Event")
                r.log("  lease free — chain kicked on new code")
            except Exception as e:
                r.warn("  kick failed: %s" % str(e)[:90])
        else:
            r.log("  chain already running — will pick up new code on "
                  "its next self-invoke")
            time.sleep(60)

        r.section("2. Seven-minute banked-delta measurement")
        s0 = get_json(STATE)
        n0 = int(s0.get("series_imported") or 0)
        c0 = int(s0.get("queue_cursor") or 0)
        r.log("  t0: imported=%d cursor=%d rpm=%s ver=%s"
              % (n0, c0, s0.get("rate_rpm"),
                 s0.get("engine_version")))
        time.sleep(150)
        time.sleep(150)
        time.sleep(120)
        s1 = get_json(STATE)
        n1 = int(s1.get("series_imported") or 0)
        c1 = int(s1.get("queue_cursor") or 0)
        mins = 7.0
        rate = (n1 - n0) / mins
        r.log("  t1: imported=%d cursor=%d rpm=%s ver=%s"
              % (n1, c1, s1.get("rate_rpm"),
                 s1.get("engine_version")))
        r.log("  measured: +%d series in %.0f min = %.1f/min (%.0f/h)"
              % (n1 - n0, mins, rate, rate * 60))

        r.section("3. Contracts + honest ETA")
        misses += contract(r, "drain",
                           str(s1.get("engine_version")) == "2.3",
                           "v2.3 live in state (ver=%s)"
                           % s1.get("engine_version"))
        misses += contract(r, "drain",
                           not s1.get("blocked_at")
                           and s1.get("status") != "KEY_INVALID",
                           "no 403/key block (blocked_at=%s status=%s)"
                           % (s1.get("blocked_at"), s1.get("status")))
        bank_errs = sum(1 for v in (s1.get("errors") or {}).values()
                        if "bank_put" in str(v))
        misses += contract(r, "drain", bank_errs < 5,
                           "threaded banking clean (%d bank_put errors)"
                           % bank_errs)
        misses += contract(r, "drain", rate > 55,
                           "rate %.1f/min beats the serial-era 49/min "
                           "(target ~85+)" % rate)
        q = {}
        try:
            import gzip as _gz
            q = json.loads(_gz.decompress(s3.get_object(
                Bucket=B,
                Key="data/_state/fred-queue.json.gz")["Body"].read()))
        except Exception:
            pass
        remaining = max(0, len(q.get("rows") or []) - c1)
        if rate > 0:
            eta_h = remaining / (rate * 60)
            r.log("  remaining=%d → ETA %.1f h (~%.1f days), finish ≈ %s"
                  % (remaining, eta_h, eta_h / 24,
                     datetime.fromtimestamp(
                         time.time() + eta_h * 3600,
                         tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")))

        r.section("4. 4592 stragglers — post-invoke CloudWatch truth")
        logs = boto3.client("logs", region_name="us-east-1")
        for fn2 in ("justhodl-catalyst-skew-premove",
                    "justhodl-failed-pattern-reversal"):
            try:
                ev = logs.filter_log_events(
                    logGroupName="/aws/lambda/" + fn2,
                    startTime=int((time.time() - 3600) * 1000),
                    limit=200)["events"]
                tail = [e["message"].rstrip() for e in ev]
                sigs = [m for m in tail if "Traceback" in m
                        or "[ERROR]" in m or "Task timed out" in m]
                r.log("  %s: %d lines last-60m, %d death-sigs"
                      % (fn2, len(tail), len(sigs)))
                for m in (sigs[-3:] or tail[-4:]):
                    r.log("    | %s" % m[:230])
            except Exception as e:
                r.warn("  %s log pull: %s" % (fn2, str(e)[:90]))

        r.section("verdict")
        if misses:
            r.fail("drain rate: %d red" % misses)
            sys.exit(1)
        r.ok("v2.3.0 drain measured live — sentinel keeps the 10-min "
             "watch (403 hands-off, AIMD backs off on 429 by itself)")


if __name__ == "__main__":
    main()
