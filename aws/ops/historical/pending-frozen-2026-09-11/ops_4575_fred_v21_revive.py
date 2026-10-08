"""ops 4575 — FRED priority-drain v2.1 revive (reports 4572/4573).

The v2 crash decomposed into five legs (fixed in the v2.1 lambda patch):
  F1  the 5-min cron sent no payload → default phase ran the COMPLETE
      phase-1 crawl forever; scoped_import never ran on schedule
  F2  reserved-concurrency-1 + 5-min async cron + async self-chain
      → 105 Lambda throttles (the storm in CW)
  F3  429 backoffs slept minutes past BUDGET_S → 850s hard timeout
      mid-drain → state unsaved, lease wedged, chain died on lease_held
  F4  everything after lease acquisition was unprotected — one raise
      wedged the lease ~14.5 min and vaporized counters
  F5  the FRED key rotation is pending (leaked key); a dead key 400s
      every call, which was indistinguishable from a crash

This op: settles the deploy, probes the key FROM THE RUNNER before
touching anything (403-incident rule: never hammer FRED blind), repairs
the EXISTING cron rule (rate 15 min + explicit scoped_import payload —
update of an existing rule, not a new one against the saturated cap),
un-wedges the lease, kicks one budgeted run with log capture, and gates:
v2.1 markers live, rate in [floor, ceiling], lease released, progress
made, checkpoints advancing, throttle storm dead.
"""
import json
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

import boto3
from botocore.config import Config

from ops_report import report

REGION = "us-east-1"
B = "justhodl-dashboard-live"
FN = "justhodl-fred-catalog"
RULE = "justhodl-fred-catalog-5min"
STATE_KEY = "data/_state/fred-scoped-import.json"

lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=845,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)
ev = boto3.client("events", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)


def get_state():
    try:
        return json.loads(s3.get_object(Bucket=B, Key=STATE_KEY)["Body"].read())
    except Exception:
        return {}


def fred_key():
    for name, dec in (("/justhodl/fred-api-key", True),
                      ("/justhodl/fred/api-key", False)):
        try:
            v = ssm.get_parameter(Name=name, WithDecryption=dec
                                  )["Parameter"]["Value"]
            if v and len(v) >= 16 and v != "PLACEHOLDER":
                return v, name
        except Exception:
            continue
    return None, None


def probe_key(key):
    """One call, from the runner. Returns (status, detail)."""
    url = ("https://api.stlouisfed.org/fred/category"
           f"?category_id=125&api_key={key}&file_type=json")
    try:
        req = urllib.request.Request(
            url, headers={"User-Agent": "JustHodl ops admin@justhodl.ai"})
        with urllib.request.urlopen(req, timeout=20) as r:
            json.loads(r.read())
            return "OK", "HTTP 200"
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8", "replace")[:200]
        except Exception:
            pass
        if e.code == 400 and "api_key" in body.lower():
            return "KEY_INVALID", body[:150]
        if e.code == 403:
            return "BLOCKED_403", body[:150]
        if e.code == 429:
            return "THROTTLED", "429 on a single probe"
        return "HTTP_%d" % e.code, body[:150]
    except Exception as e:
        return "ERROR", str(e)[:150]


def throttle_sum(minutes):
    end = datetime.now(timezone.utc)
    r = cw.get_metric_statistics(
        Namespace="AWS/Lambda", MetricName="Throttles",
        Dimensions=[{"Name": "FunctionName", "Value": FN}],
        StartTime=end - timedelta(minutes=minutes), EndTime=end,
        Period=60, Statistics=["Sum"])
    return int(sum(p["Sum"] for p in r.get("Datapoints", [])))


def main():
    fails = []
    with report("4575_fred_v21_revive") as r:
        r.heading("ops 4575 — FRED priority-drain v2.1 revive")

        r.section("1. Settle the v2.1 deploy")
        deadline = time.time() + 420
        while time.time() < deadline:
            c = lam.get_function(FunctionName=FN)["Configuration"]
            lm = c.get("LastModified", "")
            if (c.get("State") == "Active"
                    and c.get("LastUpdateStatus") == "Successful"
                    and lm[:10] == datetime.now(timezone.utc)
                    .strftime("%Y-%m-%d")):
                r.ok("settled: LastModified %s" % lm)
                break
            time.sleep(8)
        else:
            fails.append("deploy did not settle with today's code")
            r.fail(fails[-1])

        r.section("2. Key probe (runner-side, one call — 403-incident rule)")
        key, src = fred_key()
        if not key:
            fails.append("no FRED key in SSM at either path")
            r.fail(fails[-1])
        else:
            status, detail = probe_key(key)
            r.kv(key_source=src, probe=status)
            if status == "KEY_INVALID":
                r.fail("FRED KEY IS DEAD (%s). Rotation pending on Khalid's "
                       "side. The v2.1 preflight makes this state harmless — "
                       "one cheap probe per cycle, state=KEY_INVALID, no "
                       "storm — and the walk SELF-HEALS the moment the new "
                       "key lands in SSM /justhodl/fred-api-key. Not kicking "
                       "a drain against a dead key." % detail)
                fails.append("key invalid — rotate /justhodl/fred-api-key")
            elif status == "BLOCKED_403":
                r.fail("FRED 403 on a single probe — IP-level block active; "
                       "do NOT kick. %s" % detail)
                fails.append("fred 403 block")
            elif status != "OK":
                r.warn("probe %s (%s) — transient; proceeding carefully"
                       % (status, detail))

        r.section("3. Cron repair — existing rule, 15-min watchdog, "
                  "explicit phase payload")
        try:
            cur = ev.describe_rule(Name=RULE)
            tgts = ev.list_targets_by_rule(Rule=RULE).get("Targets", [])
            r.kv(rule_before=cur.get("ScheduleExpression"),
                 target_input_before=(tgts[0].get("Input")
                                      if tgts else None))
            # UPDATE of an existing classic rule — no new rule against the
            # saturated cap. 15 min: the chain is the engine, the cron is
            # only the watchdog; 5-min async invokes against a held
            # single-concurrency slot were the throttle storm (F2).
            ev.put_rule(Name=RULE, ScheduleExpression="rate(15 minutes)",
                        State="ENABLED",
                        Description=("FRED scoped-import watchdog — v2.1 "
                                     "ops 4575; chain carries duty cycle"))
            if tgts:
                t = tgts[0]
                ev.put_targets(Rule=RULE, Targets=[{
                    "Id": t["Id"], "Arn": t["Arn"],
                    "Input": json.dumps({"phase": "scoped_import"})}])
                r.ok("rule → rate(15 minutes), target input → "
                     "{\"phase\": \"scoped_import\"}")
            else:
                fails.append("rule has no targets to repair")
                r.fail(fails[-1])
        except Exception as e:
            fails.append("cron repair failed: %s" % str(e)[:120])
            r.fail(fails[-1])

        r.section("4. Un-wedge the lease")
        st = get_state()
        r.kv(lease_before=st.get("lease_until"),
             status_before=st.get("status"),
             imported=st.get("series_imported"),
             cats_done=len(st.get("cats_done") or []))
        st["lease_until"] = 0
        s3.put_object(Bucket=B, Key=STATE_KEY,
                      Body=json.dumps(st, default=str).encode(),
                      ContentType="application/json")
        r.ok("lease cleared")

        if fails:
            r.section("VERDICT")
            r.fail("pre-kick gate failed — %s" % "; ".join(fails))
            sys.exit(1)

        r.section("5. Kick one budgeted run (log tail captured)")
        t_kick = time.time()
        resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                          LogType="Tail",
                          Payload=json.dumps({"phase": "scoped_import",
                                              "ops": 4575}).encode())
        body_raw = resp["Payload"].read().decode("utf-8", "replace")
        if resp.get("FunctionError"):
            fails.append("FunctionError on kick")
            r.fail("kick FunctionError: %s" % body_raw[:500])
        m = {}
        try:
            outer = json.loads(body_raw)
            m = json.loads(outer.get("body") or "{}")
        except Exception:
            r.warn("kick body unparsed: %s" % body_raw[:200])
        r.kv(kick_seconds=round(time.time() - t_kick, 1),
             status=m.get("status"), chained=m.get("chained"),
             cats_done=m.get("categories_done"),
             imported=m.get("series_imported"),
             qtotal=m.get("queue_total"), qcursor=m.get("queue_cursor"),
             rpm=m.get("rate_rpm"))
        if m.get("status") == "KEY_INVALID":
            fails.append("engine reports KEY_INVALID — rotate the key")
            r.fail(fails[-1])

        r.section("6. Contract gates")
        st2 = get_state()

        def gate(cond, why):
            if cond:
                r.ok(why)
            else:
                fails.append(why)
                r.fail("GATE MISS — " + why)

        gate(st2.get("engine_version") == "2.1", "v2.1 markers in state")
        gate(st2.get("phase2") in ("discovery", "drain"),
             "phase2 populated (got %s)" % st2.get("phase2"))
        rpm = st2.get("rate_rpm")
        gate(isinstance(rpm, (int, float)) and 24 <= rpm <= 130,
             "rate_rpm %s inside [24, ceiling]" % rpm)
        prog = ((len(st2.get("cats_done") or [])
                 > len(st.get("cats_done") or []))
                or ((st2.get("queue_cursor") or 0)
                    > (st.get("queue_cursor") or 0))
                or str(st2.get("status", "")).startswith("COMPLETE"))
        gate(prog, "progress: cats/cursor advanced or COMPLETE "
             "(status=%s, cursor=%s)" % (st2.get("status"),
                                         st2.get("queue_cursor")))
        lease2 = st2.get("lease_until") or 0
        successor = lease2 > time.time()   # chain may already hold it
        gate(lease2 == 0 or successor,
             "lease released or handed to a live successor")

        if str(st2.get("status")) == "walking":
            r.log("walking — verifying the chain heartbeat (90s "
                  "checkpoints must advance updated_at)")
            u1 = st2.get("updated_at")
            time.sleep(115)
            st3 = get_state()
            gate(st3.get("updated_at") != u1,
                 "chain alive: updated_at advanced (%s → %s)"
                 % (u1, st3.get("updated_at")))

        r.section("7. Throttle storm check")
        time.sleep(60)
        tt = throttle_sum(10)
        gate(tt < 10, "Throttles(10m)=%d — storm dead (was 105)" % tt)

        r.section("VERDICT")
        if fails:
            r.fail("%d gate(s) failed: %s" % (len(fails), "; ".join(fails)))
        else:
            r.ok("FRED v2.1 live: crash-proof lease, budget-walled "
                 "backoff, phase-safe 15-min watchdog, chain verified, "
                 "storm dead")
        r.kv(gates_failed=len(fails))
        if fails:
            sys.exit(1)


if __name__ == "__main__":
    main()
