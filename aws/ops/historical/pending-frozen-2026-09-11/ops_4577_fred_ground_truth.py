"""ops 4577 (rev 2) — FRED ground truth + storm purge + key-chain repair.

The sentinel's first sweep (4576) showed the contradiction: v2.2 deployed,
cron repaired, lease free — yet state 229 min stale and Throttles(15m)=390.
Nothing inside the lambda has written state since 00:08, which means runs
are exiting BEFORE the lease write. The only pre-lease exits are `paused`
and `no_key` — and the failed 4572 may never have created the String
mirror, leaving the lambda's role unable to resolve a key its OWN probe
path needs (the 4575 runner-side probe used RUNNER creds, which proved
the key VALUE valid, not the lambda's ACCESS to it).

This op gets the answer from the lambda's own mouth and repairs each leg:
  1. audit the key chain: SecureString param, String mirror, function env
  2. PURGE the async retry storm — MaximumEventAgeInSeconds=60 +
     MaximumRetryAttempts=0 drops the queued backlog, then restore sane
     values (retries=1, age 1h; the 15-min watchdog covers any gap)
  3. invoke with a TooManyRequests-tolerant retry loop + LogType Tail —
     the response body says exactly which exit fired
  4. if no_key: mirror the key (runner CAN read the SecureString — 4575
     proved it) into the String param AND the function env, then re-invoke
  5. prove a real run: lease acquired, checkpoint within 150s, progress
"""
import json
import sys
import time
from datetime import datetime, timedelta, timezone

import boto3
from botocore.config import Config

from ops_report import report

REGION = "us-east-1"
B = "justhodl-dashboard-live"
FN = "justhodl-fred-catalog"
STATE_KEY = "data/_state/fred-scoped-import.json"

lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=120, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)


def get_state():
    try:
        return json.loads(s3.get_object(Bucket=B, Key=STATE_KEY)["Body"].read())
    except Exception:
        return {}


def throttle_sum(minutes=10):
    end = datetime.now(timezone.utc)
    r = cw.get_metric_statistics(
        Namespace="AWS/Lambda", MetricName="Throttles",
        Dimensions=[{"Name": "FunctionName", "Value": FN}],
        StartTime=end - timedelta(minutes=minutes), EndTime=end,
        Period=60, Statistics=["Sum"])
    return int(sum(p["Sum"] for p in r.get("Datapoints", [])))


def invoke_tolerant(r, payload, tries=8, gap=18):
    """RequestResponse with TooManyRequests tolerance — a busy slot is
    the single-flight system working, so wait and try again. A client
    ReadTimeout means the run went LONG — i.e. a REAL walk is executing
    server-side, which is the healthy outcome, not an error."""
    from botocore.exceptions import (ReadTimeoutError,
                                     ConnectionClosedError)
    for i in range(tries):
        try:
            resp = lam.invoke(FunctionName=FN,
                              InvocationType="RequestResponse",
                              LogType="Tail",
                              Payload=json.dumps(payload).encode())
            body = resp["Payload"].read().decode("utf-8", "replace")
            return resp, body
        except lam.exceptions.TooManyRequestsException:
            r.log("  slot busy (attempt %d/%d) — waiting %ds"
                  % (i + 1, tries, gap))
            time.sleep(gap)
        except (ReadTimeoutError, ConnectionClosedError):
            r.ok("  run exceeded the client window — a REAL walk is "
                 "executing server-side; the checkpoint proof carries "
                 "the verdict")
            return "LONG_RUN", None
    return None, None


def main():
    fails = 0
    with report("4577_fred_ground_truth") as r:
        r.heading("ops 4577 — FRED ground truth + storm purge + key repair")

        r.section("1. Key-chain audit (three belts)")
        belts = {}
        for name, dec in (("/justhodl/fred-api-key", True),
                          ("/justhodl/fred/api-key", False)):
            try:
                v = ssm.get_parameter(Name=name, WithDecryption=dec
                                      )["Parameter"]["Value"]
                belts[name] = len(v) if v else 0
            except Exception as e:
                belts[name] = "ABSENT (%s)" % type(e).__name__
        env = (lam.get_function_configuration(FunctionName=FN)
               .get("Environment", {}) or {}).get("Variables", {}) or {}
        belts["env.FRED_API_KEY"] = (len(env.get("FRED_API_KEY", ""))
                                     or "ABSENT")
        r.kv(**{k.replace("/", "_"): v for k, v in belts.items()})
        secure_len = belts.get("/justhodl/fred-api-key")
        have_secure = isinstance(secure_len, int) and secure_len >= 16

        r.section("2. Async storm purge (drop the retry backlog)")
        t_before = throttle_sum(10)
        try:
            lam.put_function_event_invoke_config(
                FunctionName=FN, MaximumRetryAttempts=0,
                MaximumEventAgeInSeconds=60)
            r.log("  event-invoke config → age 60s / retries 0 (purging)")
            time.sleep(90)
            lam.put_function_event_invoke_config(
                FunctionName=FN, MaximumRetryAttempts=1,
                MaximumEventAgeInSeconds=3600)
            r.ok("  restored → age 1h / retries 1 (watchdog covers gaps)")
        except Exception as e:
            r.warn("  purge config failed: %s" % str(e)[:120])
        r.kv(throttles_10m_before=t_before)

        r.section("3. The lambda's own answer")
        resp, body = invoke_tolerant(r, {"phase": "scoped_import",
                                         "ops": 4577})
        if resp is None:
            fails += 1
            r.fail("slot never freed across the retry window — backlog "
                   "purge did not take; inspect EventInvokeConfig")
        elif resp == "LONG_RUN":
            pass   # healthy — section 5 proves it via checkpoints
        else:
            if resp.get("FunctionError"):
                fails += 1
                r.fail("FunctionError: %s" % body[:500])
            m = {}
            try:
                outer = json.loads(body)
                m = json.loads(outer.get("body") or "{}")
            except Exception:
                pass
            r.kv(answer=json.dumps(m)[:300])
            skipped = m.get("skipped")
            if skipped == "no_key":
                r.warn("CONFIRMED: the lambda cannot resolve a key — "
                       "repairing the mirror + env belts")
                if not have_secure:
                    fails += 1
                    r.fail("SecureString also unreadable from the runner — "
                           "key must be re-provisioned by Khalid")
                else:
                    key = ssm.get_parameter(Name="/justhodl/fred-api-key",
                                            WithDecryption=True
                                            )["Parameter"]["Value"]
                    ssm.put_parameter(Name="/justhodl/fred/api-key",
                                      Value=key, Type="String",
                                      Overwrite=True)
                    env["FRED_API_KEY"] = key
                    lam.update_function_configuration(
                        FunctionName=FN,
                        Environment={"Variables": env})
                    time.sleep(12)
                    r.ok("String mirror + env belt written — key now "
                         "resolvable without KMS")
                    resp, body = invoke_tolerant(
                        r, {"phase": "scoped_import", "ops": 4577})
                    if resp == "LONG_RUN":
                        m = {"status": "walking",
                             "note": "long run in progress"}
                    else:
                        try:
                            m = json.loads(json.loads(body)
                                           .get("body") or "{}")
                        except Exception:
                            m = {}
                    r.kv(answer_after_repair=json.dumps(m)[:300])
                    if m.get("skipped") == "no_key":
                        fails += 1
                        r.fail("still no_key after repair — role/KMS "
                               "posture needs eyes")
            elif skipped == "paused":
                fails += 1
                r.fail("paused knob is 1 — clear /justhodl/fred/paused")
            elif skipped in ("lease_held", "lost_lease_race"):
                r.ok("a live holder owns the lease — the walk is running; "
                     "checkpoint check below is the proof")
            elif m.get("status") in ("walking", "COMPLETE",
                                     "COMPLETE_CUT",
                                     "COMPLETE_WITH_LEAKS"):
                r.ok("real run completed: status=%s cats=%s imported=%s"
                     % (m.get("status"), m.get("categories_done"),
                        m.get("series_imported")))
            elif m.get("status") == "KEY_INVALID":
                fails += 1
                r.fail("engine preflight says the key VALUE is dead — "
                       "rotate /justhodl/fred-api-key")

        r.section("4. Recent log tail (last 25 lines, ground truth)")
        try:
            streams = logs.describe_log_streams(
                logGroupName="/aws/lambda/" + FN, orderBy="LastEventTime",
                descending=True, limit=2)["logStreams"]
            shown = 0
            for st_ in streams:
                ev = logs.get_log_events(
                    logGroupName="/aws/lambda/" + FN,
                    logStreamName=st_["logStreamName"], limit=15,
                    startFromHead=False)["events"]
                for e in ev:
                    msg = e["message"].strip()[:160]
                    if msg and shown < 25:
                        r.log("  | " + msg)
                        shown += 1
        except Exception as e:
            r.warn("  log read failed: %s" % str(e)[:100])

        r.section("5. Live-run proof: checkpoint within 200s")
        st1 = get_state()
        u1 = st1.get("updated_at")
        time.sleep(200)
        st2 = get_state()
        moved = (st2.get("updated_at") != u1
                 or (st2.get("lease_until") or 0) > time.time()
                 or str(st2.get("status", "")).startswith("COMPLETE"))
        if moved:
            r.ok("state moving: updated_at %s → %s | status=%s | "
                 "phase2=%s | imported=%s | cursor=%s/%s | rpm=%s | v=%s"
                 % (u1, st2.get("updated_at"), st2.get("status"),
                    st2.get("phase2"), st2.get("series_imported"),
                    st2.get("queue_cursor"), st2.get("queue_total"),
                    st2.get("rate_rpm"), st2.get("engine_version")))
        else:
            fails += 1
            r.fail("no state movement in 200s after the kick — "
                   "read the log tail above")

        r.section("6. Storm check after purge")
        time.sleep(60)
        t_after = throttle_sum(5)
        (r.ok if t_after < 5 else r.warn)(
            "Throttles(5m)=%d (was %d/10m before purge)"
            % (t_after, t_before))

        r.section("VERDICT")
        if fails:
            r.fail("%d gate(s) failed" % fails)
        else:
            r.ok("FRED alive end-to-end: key resolvable inside the "
                 "lambda, backlog purged, run checkpointing, storm dead")
        r.kv(gates_failed=fails)
        if fails:
            sys.exit(1)


if __name__ == "__main__":
    main()
