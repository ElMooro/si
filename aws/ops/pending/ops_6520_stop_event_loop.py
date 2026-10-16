"""ops 6520 - STOP THE BLEEDING: break the miss-calibrator <-> event-coordinator EventBridge loop.

Evidence (ops 6519, read-only, 2026-10-16 19:35 UTC):
  * October MTD $2,351 vs the $500 budget; daily spend went $20 -> $336 (Oct 12) -> $516 -> $556 -> $588.
  * 7-day Lambda estimate: justhodl-miss-calibrator $1,542 (12.0M invocations, 92M GB-s),
    justhodl-event-coordinator $428 (20.8M invocations).  Every other function < $6.
  * Code path (repo HEAD): miss-calibrator.handler publishes `near_miss.extreme` (lines ~378-397) and
    event-coordinator.ROUTES["near_miss.extreme"] invokes justhodl-miss-calibrator -> self-sustaining loop
    through the justhodl-system-events bus (EventBridge hops reset Lambda's recursion lineage, so the
    16-hop breaker never fires).
  * S3: coordinator.write_audit appends by READ + REWRITE of system-events/audit/<day>.jsonl on every
    routed event; the bucket is versioned and system-events/ has no noncurrent-version expiry, so every
    rewrite left a full noncurrent copy.  BucketSizeBytes(Standard) 105,871 GB; guard alarms
    s3-bytes-growth-1d / s3-objects-growth-1d / invocations-1h / concurrency are all in ALARM, but their
    only delivery path is Telegram, whose token returns 401.

Actions (quarantine FIRST, measure after - every hour of loop is ~$20):
  1. Reversal ledger -> s3://justhodl-dashboard-live/data/ops/event-loop-quarantine-6520.json (before-states).
  2. Reserved concurrency 0 on justhodl-miss-calibrator AND justhodl-event-coordinator (AWS's reversible
     kill switch; throttles are not billed).  The router's other routes are accelerators for engines that
     also run on their own schedules, so pausing it until the fixed code is live is harmless.
  3. Event-invoke config MaximumEventAgeInSeconds=60 / MaximumRetryAttempts=0 on both so the async
     backlog of loop events is discarded instead of replayed for 6 h when concurrency comes back.
  4. Lifecycle rule ops6520-purge-dead-versions-system-events (prefix system-events/): noncurrent versions
     expire after 1 day + expired delete markers removed -> the dead audit copies purge at zero request cost.
  5. Measure: hourly invocations/throttles 24h, 1-minute post-quarantine window, bucket size series,
     today's audit object size + version sample, and S3 server-access-log attribution of PUT volume by
     writer session and prefix for the last full hour (ground truth for "which engine is writing").

Un-quarantine is a separate op, gated on the fixed engine code being live (by content).
"""

from datetime import datetime, timezone, timedelta
from pathlib import Path
import concurrent.futures as cf
import json
import re
import sys
import time

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
ACCOUNT = "857687956942"
BUCKET = "justhodl-dashboard-live"
LOG_BUCKET = "justhodl-s3-access-logs-857687956942"
LOG_PREFIX = "live/%s/%s/%s/" % (ACCOUNT, REGION, BUCKET)
FNS = ("justhodl-miss-calibrator", "justhodl-event-coordinator")
LEDGER_KEY = "data/ops/event-loop-quarantine-6520.json"
RULE_ID = "ops6520-purge-dead-versions-system-events"
RULE_PREFIX = "system-events/"
OUT_JSON = ROOT / "aws" / "ops" / "reports" / "6520.json"
CFG = Config(retries={"max_attempts": 10, "mode": "adaptive"}, read_timeout=60, connect_timeout=15)

lam = boto3.client("lambda", region_name=REGION, config=CFG)
s3 = boto3.client("s3", region_name=REGION, config=CFG)
cw = boto3.client("cloudwatch", region_name=REGION, config=CFG)
NOW = datetime.now(timezone.utc)
out = {"ran_at": NOW.isoformat(), "op": 6520, "errors": {}}


def conc(fn):
    return lam.get_function_concurrency(FunctionName=fn).get("ReservedConcurrentExecutions")


def eic(fn):
    try:
        r = lam.get_function_event_invoke_config(FunctionName=fn)
        return {k: r.get(k) for k in ("MaximumRetryAttempts", "MaximumEventAgeInSeconds", "DestinationConfig")
                if r.get(k) is not None}
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            return None
        raise


def metric_series(fn, name, stat, period, start, end):
    r = cw.get_metric_statistics(Namespace="AWS/Lambda", MetricName=name,
                                 Dimensions=[{"Name": "FunctionName", "Value": fn}],
                                 StartTime=start, EndTime=end, Period=period, Statistics=[stat])
    return [(p["Timestamp"].strftime("%m-%d %H:%M"), round(p[stat], 1))
            for p in sorted(r.get("Datapoints", []), key=lambda p: p["Timestamp"])]


LOG_RE = re.compile(r'^(\S+) (\S+) \[([^\]]+)\] (\S+) (\S+) (\S+) (\S+) (\S+) "([^"]*)" (\S+) (\S+) (\S+) (\S+)')


def attribute_last_hour(r):
    """Ground truth from server access logs: PUT volume by writer session and key prefix."""
    hour = (NOW - timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    day_prefix = LOG_PREFIX + hour.strftime("%Y/%m/%d/") + hour.strftime("%Y-%m-%d-%H-")
    keys = []
    token = None
    while len(keys) < 2500:
        kw = {"Bucket": LOG_BUCKET, "Prefix": day_prefix, "MaxKeys": 1000}
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        keys += [o["Key"] for o in resp.get("Contents", [])]
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")
    agg, ops = {}, {}

    def read(k):
        try:
            return s3.get_object(Bucket=LOG_BUCKET, Key=k)["Body"].read().decode("utf-8", "replace")
        except Exception:  # noqa: BLE001
            return ""

    t0 = time.time()
    with cf.ThreadPoolExecutor(max_workers=32) as ex:
        for text in ex.map(read, keys[:2500]):
            for line in text.splitlines():
                m = LOG_RE.match(line)
                if not m:
                    continue
                requester, op, key, size = m.group(5), m.group(7), m.group(8), m.group(13)
                ops[op] = ops.get(op, 0) + 1
                if op != "REST.PUT.OBJECT":
                    continue
                session = requester.split("/")[-1] if "assumed-role" in requester else requester[-40:]
                prefix = "/".join(key.split("/")[:3]) if key != "-" else "-"
                a = agg.setdefault((session, prefix), [0, 0])
                a[0] += 1
                a[1] += int(size) if size.isdigit() else 0
    rows = sorted(({"writer": s, "prefix": p, "puts": v[0], "gb": round(v[1] / 1e9, 3)} for (s, p), v in agg.items()),
                  key=lambda x: -x["gb"])
    out["access_log_hour"] = {"hour": hour.isoformat(), "log_objects": len(keys), "read_s": round(time.time() - t0, 1),
                              "sampled": min(len(keys), 2500), "truncated_listing": len(keys) >= 2500,
                              "top_put_by_bytes": rows[:15],
                              "top_put_by_count": sorted(rows, key=lambda x: -x["puts"])[:10],
                              "ops_histogram_top": sorted(ops.items(), key=lambda x: -x[1])[:10]}
    for x in rows[:8]:
        r.log("PUT %-40s %-48s puts=%8d  %9.3f GB" % (x["writer"][:40], x["prefix"][:48], x["puts"], x["gb"]))


def main():
    with report("ops_6520_stop_event_loop") as r:
        r.heading("ops 6520 - break the miss-calibrator <-> event-coordinator loop")
        failed = []

        # 1. ledger of before-states, written BEFORE any mutation
        before = {fn: {"reserved": conc(fn), "event_invoke_config": eic(fn)} for fn in FNS}
        try:
            lc = s3.get_bucket_lifecycle_configuration(Bucket=BUCKET).get("Rules", [])
        except ClientError as e:
            lc = [] if e.response["Error"]["Code"] == "NoSuchLifecycleConfiguration" else None
            if lc is None:
                raise
        ledger = {"op": 6520, "created_at": NOW.isoformat(), "reason": "EventBridge loop near_miss.extreme",
                  "functions": before, "lifecycle_rule_added": RULE_ID,
                  "lifecycle_rule_preexisting": any(x.get("ID") == RULE_ID for x in lc),
                  "reverse": {"how": "un-quarantine op after fixed code is live: restore reserved concurrency "
                                     "(None -> delete_function_concurrency) and event invoke config (None -> delete)",
                              "lifecycle": "keep - the rule only expires noncurrent versions"}}
        existing_ledger = None
        try:
            existing_ledger = json.loads(s3.get_object(Bucket=BUCKET, Key=LEDGER_KEY)["Body"].read())
        except ClientError:
            pass
        if existing_ledger and existing_ledger.get("functions"):
            # idempotent re-run: keep the ORIGINAL before-states, never overwrite them with our own zeros
            ledger["functions"] = existing_ledger["functions"]
            ledger["rerun_at"] = NOW.isoformat()
            r.log("ledger exists - original before-states preserved")
        s3.put_object(Bucket=BUCKET, Key=LEDGER_KEY, Body=json.dumps(ledger, indent=1, default=str).encode(),
                      ContentType="application/json", CacheControl="no-store")
        r.ok("ledger written %s: %s" % (LEDGER_KEY, json.dumps(ledger["functions"])))

        # 2 + 3. quarantine
        for fn in FNS:
            lam.put_function_concurrency(FunctionName=fn, ReservedConcurrentExecutions=0)
            lam.put_function_event_invoke_config(FunctionName=fn, MaximumRetryAttempts=0, MaximumEventAgeInSeconds=60)
        quarantined_at = datetime.now(timezone.utc)
        for fn in FNS:
            c_now, e_now = conc(fn), eic(fn)
            ok = c_now == 0 and (e_now or {}).get("MaximumEventAgeInSeconds") == 60
            (r.ok if ok else r.fail)("%s reserved=%s eic=%s" % (fn, c_now, e_now))
            if not ok:
                failed.append(fn)
        out["quarantined_at"] = quarantined_at.isoformat()

        # 4. lifecycle (merge, never replace other rules)
        rules = [x for x in (lc or []) if x.get("ID") != RULE_ID]
        rules.append({"ID": RULE_ID, "Status": "Enabled", "Filter": {"Prefix": RULE_PREFIX},
                      "NoncurrentVersionExpiration": {"NoncurrentDays": 1},
                      "Expiration": {"ExpiredObjectDeleteMarker": True}})
        s3.put_bucket_lifecycle_configuration(Bucket=BUCKET, LifecycleConfiguration={"Rules": rules})
        seen = False
        for _ in range(12):  # lifecycle reads are eventually consistent - read back with patience
            got = s3.get_bucket_lifecycle_configuration(Bucket=BUCKET).get("Rules", [])
            if any(x.get("ID") == RULE_ID for x in got):
                seen = True
                break
            time.sleep(5)
        (r.ok if seen else r.warn)("lifecycle %s %s (rules now %d)" % (RULE_ID, "present" if seen else "not yet visible", len(rules)))
        out["lifecycle_rules"] = [{"id": x.get("ID"), "prefix": (x.get("Filter") or {}).get("Prefix", x.get("Prefix")),
                                   "noncurrent_days": (x.get("NoncurrentVersionExpiration") or {}).get("NoncurrentDays"),
                                   "status": x.get("Status")} for x in rules]

        # 5. measurements (non-blocking)
        r.section("Measurements")
        try:
            for fn in FNS:
                out.setdefault("hourly", {})[fn] = {
                    "invocations": metric_series(fn, "Invocations", "Sum", 3600, NOW - timedelta(hours=26), NOW),
                    "throttles": metric_series(fn, "Throttles", "Sum", 3600, NOW - timedelta(hours=6), NOW),
                    "concurrency_max": metric_series(fn, "ConcurrentExecutions", "Maximum", 3600, NOW - timedelta(hours=6), NOW),
                }
                inv = out["hourly"][fn]["invocations"]
                r.log("%s invocations/h last 6: %s" % (fn, inv[-6:]))
            age = metric_series("justhodl-event-coordinator", "AsyncEventAge", "Maximum", 3600, NOW - timedelta(hours=6), NOW)
            out["coordinator_async_event_age_ms_max"] = age
            r.log("coordinator AsyncEventAge max (ms) last hours: %s" % age[-4:])
        except Exception as e:  # noqa: BLE001
            out["errors"]["hourly"] = str(e)[:200]
        try:
            series = {}
            for st in ("StandardStorage", "AllStorageTypes"):
                metric = "BucketSizeBytes" if st == "StandardStorage" else "NumberOfObjects"
                rr = cw.get_metric_statistics(Namespace="AWS/S3", MetricName=metric,
                                              Dimensions=[{"Name": "BucketName", "Value": BUCKET},
                                                          {"Name": "StorageType", "Value": st}],
                                              StartTime=NOW - timedelta(days=16), EndTime=NOW, Period=86400,
                                              Statistics=["Average"])
                series[metric] = [(p["Timestamp"].strftime("%m-%d"),
                                   round(p["Average"] / (1e9 if metric == "BucketSizeBytes" else 1e6), 2))
                                  for p in sorted(rr.get("Datapoints", []), key=lambda p: p["Timestamp"])]
            out["bucket_series"] = series
            r.log("BucketSizeBytes GB by day: %s" % series.get("BucketSizeBytes"))
            r.log("NumberOfObjects (millions) by day: %s" % series.get("NumberOfObjects"))
        except Exception as e:  # noqa: BLE001
            out["errors"]["bucket_series"] = str(e)[:200]
        try:
            day_keys = {}
            for d in range(0, 6):
                k = "system-events/audit/%s.jsonl" % (NOW - timedelta(days=d)).strftime("%Y-%m-%d")
                try:
                    h = s3.head_object(Bucket=BUCKET, Key=k)
                    v = s3.list_object_versions(Bucket=BUCKET, Prefix=k, MaxKeys=1000)
                    sizes = [x["Size"] for x in v.get("Versions", [])]
                    day_keys[k] = {"current_mb": round(h["ContentLength"] / 1e6, 2),
                                   "versions_sampled": len(sizes), "truncated": v.get("IsTruncated"),
                                   "sample_gb": round(sum(sizes) / 1e9, 2)}
                except ClientError as e:
                    day_keys[k] = e.response["Error"]["Code"]
            out["audit_objects"] = day_keys
            for k, v in day_keys.items():
                r.log("%s -> %s" % (k, v))
        except Exception as e:  # noqa: BLE001
            out["errors"]["audit_objects"] = str(e)[:200]
        try:
            attribute_last_hour(r)
        except Exception as e:  # noqa: BLE001
            out["errors"]["access_logs"] = str(e)[:200]
            r.warn("access-log attribution failed: %s" % str(e)[:160])

        # post-quarantine proof: invocations in the minutes after the throttle went on
        wait_until = quarantined_at + timedelta(seconds=240)
        while datetime.now(timezone.utc) < wait_until:
            time.sleep(10)
        end = datetime.now(timezone.utc)
        post = {}
        for fn in FNS:
            post[fn] = {"invocations_1m": metric_series(fn, "Invocations", "Sum", 60, quarantined_at - timedelta(minutes=5), end),
                        "throttles_1m": metric_series(fn, "Throttles", "Sum", 60, quarantined_at - timedelta(minutes=5), end)}
            r.log("%s 1-min invocations around quarantine: %s" % (fn, post[fn]["invocations_1m"]))
            r.log("%s 1-min throttles: %s" % (fn, post[fn]["throttles_1m"]))
        out["post_quarantine"] = post
        out["final"] = {fn: {"reserved": conc(fn), "eic": eic(fn)} for fn in FNS}

        OUT_JSON.write_text(json.dumps(out, indent=1, default=str))
        r.ok("wrote %s" % OUT_JSON.relative_to(ROOT))
        if failed:
            r.fail("quarantine not confirmed for %s" % failed)
            sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
