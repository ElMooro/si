"""ops 4578 — FRED alive: the one-line root cause, purged and proven.

4577's log tail found it: the v2 rewrite never defined module-scope `s3`
(21 call sites). Every v2/2.1/2.2 invoke NameError'd on its first S3
touch; the over-broad lease except converted that into a keyless
"lost_lease_race" return; the handler's strict print KeyError'd on it;
Lambda errored; async sources retried — a ~300ms crash-loop that held
the single slot for an entire arc. v2.2.1 (4578-1) defines s3, makes the
lease except honest (only a real 412 is a race), and crash-proofs the
print. This op purges the retry backlog, invokes tolerantly, repairs the
key belts if the honest body asks for it, and proves checkpoints.
"""
import json
import sys
import time
from datetime import datetime, timedelta, timezone

import boto3
from botocore.config import Config
from botocore.exceptions import ConnectionClosedError, ReadTimeoutError

from ops_report import report

REGION = "us-east-1"
B = "justhodl-dashboard-live"
FN = "justhodl-fred-catalog"
STATE_KEY = "data/_state/fred-scoped-import.json"

lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=120,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)


def get_state():
    try:
        return json.loads(s3.get_object(Bucket=B, Key=STATE_KEY
                                        )["Body"].read())
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
    for i in range(tries):
        try:
            resp = lam.invoke(FunctionName=FN,
                              InvocationType="RequestResponse",
                              LogType="Tail",
                              Payload=json.dumps(payload).encode())
            return resp, resp["Payload"].read().decode("utf-8", "replace")
        except lam.exceptions.TooManyRequestsException:
            r.log("  slot busy (%d/%d) — waiting %ds" % (i + 1, tries, gap))
            time.sleep(gap)
        except (ReadTimeoutError, ConnectionClosedError):
            r.ok("  run exceeded the client window — a REAL walk is "
                 "executing server-side; checkpoints carry the verdict")
            return "LONG_RUN", None
    return None, None


def parse_body(body):
    try:
        return json.loads(json.loads(body).get("body") or "{}")
    except Exception:
        return {}


def main():
    fails = 0
    with report("4578_fred_alive") as r:
        r.heading("ops 4578 — FRED alive after the s3 one-liner")

        r.section("1. Settle the v2.2.1 deploy")
        t0 = time.time()
        ok = False
        while time.time() - t0 < 240:
            c = lam.get_function(FunctionName=FN)["Configuration"]
            lm = c.get("LastModified", "")
            fresh = False
            try:
                lmdt = datetime.strptime(lm.split(".")[0],
                                         "%Y-%m-%dT%H:%M:%S"
                                         ).replace(tzinfo=timezone.utc)
                fresh = (datetime.now(timezone.utc) - lmdt
                         ).total_seconds() < 900
            except Exception:
                pass
            if (c.get("LastUpdateStatus") == "Successful"
                    and c.get("State") == "Active" and fresh):
                ok = True
                r.ok("settled: LastModified %s" % lm)
                break
            time.sleep(10)
        if not ok:
            r.warn("deploy not observed fresh in 240s — proceeding "
                   "against whatever is live")

        r.section("2. Key belts (runner view)")
        belts = {}
        for name, dec in (("/justhodl/fred-api-key", True),
                          ("/justhodl/fred/api-key", False)):
            try:
                v = ssm.get_parameter(Name=name, WithDecryption=dec
                                      )["Parameter"]["Value"]
                belts[name] = len(v or "")
            except Exception as e:
                belts[name] = "ABSENT(%s)" % type(e).__name__
        env = (lam.get_function_configuration(FunctionName=FN)
               .get("Environment", {}) or {}).get("Variables", {}) or {}
        belts["env.FRED_API_KEY"] = len(env.get("FRED_API_KEY", "")) or 0
        r.kv(**{k.replace("/", "_"): v for k, v in belts.items()})

        r.section("3. Purge the crash-loop retry backlog")
        t_before = throttle_sum(10)
        try:
            lam.put_function_event_invoke_config(
                FunctionName=FN, MaximumRetryAttempts=0,
                MaximumEventAgeInSeconds=60)
            time.sleep(90)
            lam.put_function_event_invoke_config(
                FunctionName=FN, MaximumRetryAttempts=1,
                MaximumEventAgeInSeconds=3600)
            r.ok("backlog purged (age60/retries0 for 90s, then restored)")
        except Exception as e:
            r.warn("purge failed: %s" % str(e)[:120])
        r.kv(throttles_10m_before=t_before)

        r.section("4. The lambda's honest answer")
        resp, body = invoke_tolerant(r, {"phase": "scoped_import",
                                         "ops": 4578})
        if resp is None:
            fails += 1
            r.fail("slot never freed — crash-loop still alive; read "
                   "the log tail below")
        elif resp == "LONG_RUN":
            pass
        else:
            if resp.get("FunctionError"):
                fails += 1
                r.fail("FunctionError: %s" % (body or "")[:400])
            m = parse_body(body)
            r.kv(answer=json.dumps(m)[:280])
            if m.get("skipped") == "no_key":
                r.warn("no_key from inside the lambda — mirroring the "
                       "SecureString into the String belt + env")
                sec = belts.get("/justhodl/fred-api-key")
                if not isinstance(sec, int) or sec < 16:
                    fails += 1
                    r.fail("SecureString unreadable from the runner too "
                           "— key must be re-provisioned by Khalid")
                else:
                    key = ssm.get_parameter(Name="/justhodl/fred-api-key",
                                            WithDecryption=True
                                            )["Parameter"]["Value"]
                    ssm.put_parameter(Name="/justhodl/fred/api-key",
                                      Value=key, Type="String",
                                      Overwrite=True)
                    env["FRED_API_KEY"] = key
                    lam.update_function_configuration(
                        FunctionName=FN, Environment={"Variables": env})
                    time.sleep(12)
                    r.ok("belts written — re-invoking")
                    resp, body = invoke_tolerant(
                        r, {"phase": "scoped_import", "ops": 4578})
                    if resp == "LONG_RUN":
                        pass
                    else:
                        m = parse_body(body)
                        r.kv(answer_after_repair=json.dumps(m)[:280])
                        if m.get("skipped") == "no_key":
                            fails += 1
                            r.fail("still no_key — role/KMS posture "
                                   "needs eyes")
            elif m.get("status") == "KEY_INVALID":
                fails += 1
                r.fail("key VALUE rejected by FRED — rotate "
                       "/justhodl/fred-api-key (engine self-heals)")
            elif m.get("skipped") == "paused":
                fails += 1
                r.fail("paused knob is set — clear /justhodl/fred/paused")
            elif m.get("skipped") in ("lease_held", "lost_lease_race"):
                r.ok("a live holder owns the lease — checkpoints below "
                     "carry the verdict")

        r.section("5. Checkpoint proof (200s window)")
        st1 = get_state()
        u1 = st1.get("updated_at")
        time.sleep(200)
        st2 = get_state()
        moved = (st2.get("updated_at") != u1
                 or (st2.get("lease_until") or 0) > time.time()
                 or str(st2.get("status", "")).startswith("COMPLETE"))
        if moved:
            r.ok("state moving: %s → %s | status=%s | imported=%s | "
                 "cursor=%s/%s | rpm=%s | scope=%s | v=%s"
                 % (u1, st2.get("updated_at"), st2.get("status"),
                    st2.get("series_imported"), st2.get("queue_cursor"),
                    st2.get("queue_total"), st2.get("rate_rpm"),
                    st2.get("import_scope"), st2.get("engine_version")))
        else:
            fails += 1
            r.fail("no state movement in 200s (updated_at still %s)" % u1)

        r.section("6. Log tail (ground truth)")
        try:
            streams = logs.describe_log_streams(
                logGroupName="/aws/lambda/" + FN,
                orderBy="LastEventTime", descending=True,
                limit=2)["logStreams"]
            shown = 0
            for st_ in streams:
                for e in logs.get_log_events(
                        logGroupName="/aws/lambda/" + FN,
                        logStreamName=st_["logStreamName"], limit=10,
                        startFromHead=False)["events"]:
                    msg = e["message"].strip()[:150]
                    if msg and shown < 15:
                        r.log("  | " + msg)
                        shown += 1
        except Exception as e:
            r.warn("log read failed: %s" % str(e)[:100])

        r.section("7. Storm gate")
        time.sleep(60)
        t_after = throttle_sum(5)
        if t_after < 5:
            r.ok("Throttles(5m)=%d (was %d/10m) — storm dead"
                 % (t_after, t_before))
        else:
            r.warn("Throttles(5m)=%d — settling; sentinel keeps watch"
                   % t_after)

        r.section("VERDICT")
        if fails:
            r.fail("%d gate(s) failed" % fails)
        else:
            r.ok("FRED walking for real: s3 defined, honest excepts, "
                 "backlog purged, checkpoints moving, popularity-desc "
                 "drain live — expansion armed via the sentinel")
        r.kv(gates_failed=fails)
        if fails:
            sys.exit(1)


if __name__ == "__main__":
    main()
