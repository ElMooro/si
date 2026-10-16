"""ops 6521 - lift the ops 6520 quarantine ONLY if the loop fix is live, then prove the loop is gone.

Gate (by content, never by "deploy was green"):
  * the deployed justhodl-event-coordinator package contains is_self_loop() + AUDIT_MAX_BYTES
  * the deployed justhodl-miss-calibrator package contains EVENT_DEBOUNCE_S and no line emitting
    "near_miss.extreme"
Then restore exactly what ops 6520 changed, from its ledger data/ops/event-loop-quarantine-6520.json:
reserved concurrency (None -> delete) and the async event-invoke config (None -> delete).
Watch 10 minutes of 1-minute metrics; if either function runs hot (miss-calibrator > 20/min or
coordinator > 600/min for 3 consecutive minutes) re-apply the quarantine and go RED.
"""

from datetime import datetime, timezone, timedelta
from pathlib import Path
import io
import json
import sys
import time
import urllib.request
import zipfile

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
LEDGER_KEY = "data/ops/event-loop-quarantine-6520.json"
FNS = ("justhodl-miss-calibrator", "justhodl-event-coordinator")
HOT = {"justhodl-miss-calibrator": 20, "justhodl-event-coordinator": 600}
OUT_JSON = ROOT / "aws" / "ops" / "reports" / "6521.json"
CFG = Config(retries={"max_attempts": 10, "mode": "adaptive"}, read_timeout=60, connect_timeout=15)
lam = boto3.client("lambda", region_name=REGION, config=CFG)
s3 = boto3.client("s3", region_name=REGION, config=CFG)
cw = boto3.client("cloudwatch", region_name=REGION, config=CFG)
out = {"op": 6521, "ran_at": datetime.now(timezone.utc).isoformat()}


def deployed_source(fn):
    loc = lam.get_function(FunctionName=fn)["Code"]["Location"]
    with urllib.request.urlopen(loc, timeout=60) as r:
        z = zipfile.ZipFile(io.BytesIO(r.read()))
    return z.read("lambda_function.py").decode("utf-8", "replace")


def gate():
    coord = deployed_source("justhodl-event-coordinator")
    cal = deployed_source("justhodl-miss-calibrator")
    emitting = [ln for ln in cal.splitlines() if '"near_miss.extreme"' in ln and not ln.strip().startswith("#")]
    checks = {
        "coordinator.is_self_loop": "def is_self_loop(" in coord and "is_self_loop(fn, detail, source)" in coord,
        "coordinator.audit_cap": "AUDIT_MAX_BYTES" in coord,
        "calibrator.debounce": "EVENT_DEBOUNCE_S" in cal and "event_triggered(event)" in cal,
        "calibrator.no_reemission": not emitting,
    }
    return checks


def conc(fn):
    return lam.get_function_concurrency(FunctionName=fn).get("ReservedConcurrentExecutions")


def per_minute(fn, metric, start, end):
    r = cw.get_metric_statistics(Namespace="AWS/Lambda", MetricName=metric,
                                 Dimensions=[{"Name": "FunctionName", "Value": fn}],
                                 StartTime=start, EndTime=end, Period=60, Statistics=["Sum"])
    return [(p["Timestamp"].strftime("%H:%M"), int(p["Sum"])) for p in sorted(r.get("Datapoints", []), key=lambda p: p["Timestamp"])]


def quarantine(fn):
    lam.put_function_concurrency(FunctionName=fn, ReservedConcurrentExecutions=0)
    lam.put_function_event_invoke_config(FunctionName=fn, MaximumRetryAttempts=0, MaximumEventAgeInSeconds=60)


def restore(fn, before):
    if before.get("reserved") is None:
        lam.delete_function_concurrency(FunctionName=fn)
    else:
        lam.put_function_concurrency(FunctionName=fn, ReservedConcurrentExecutions=int(before["reserved"]))
    eic = before.get("event_invoke_config")
    if eic is None:
        try:
            lam.delete_function_event_invoke_config(FunctionName=fn)
        except ClientError as e:
            if e.response["Error"]["Code"] != "ResourceNotFoundException":
                raise
    else:
        kw = {k: v for k, v in eic.items() if k in ("MaximumRetryAttempts", "MaximumEventAgeInSeconds", "DestinationConfig")}
        lam.put_function_event_invoke_config(FunctionName=fn, **kw)


def main():
    with report("ops_6521_event_loop_unquarantine") as r:
        r.heading("ops 6521 - gated un-quarantine of the event loop pair")
        checks = gate()
        out["gate"] = checks
        for k, v in checks.items():
            (r.ok if v else r.fail)("gate %s" % k)
        if not all(checks.values()):
            r.fail("fix not live - quarantine stays")
            OUT_JSON.write_text(json.dumps(out, indent=1, default=str))
            sys.exit(1)

        ledger = json.loads(s3.get_object(Bucket=BUCKET, Key=LEDGER_KEY)["Body"].read())
        before = ledger["functions"]
        out["before"] = before
        for fn in FNS:
            restore(fn, before[fn])
            r.ok("%s restored: reserved=%s eic=%s" % (fn, conc(fn), before[fn].get("event_invoke_config")))
        restored_at = datetime.now(timezone.utc)
        out["restored_at"] = restored_at.isoformat()

        hot_streak = {fn: 0 for fn in FNS}
        tripped = []
        deadline = restored_at + timedelta(minutes=10)
        while datetime.now(timezone.utc) < deadline and not tripped:
            time.sleep(60)
            end = datetime.now(timezone.utc)
            for fn in FNS:
                pts = per_minute(fn, "Invocations", restored_at - timedelta(minutes=1), end)
                last = pts[-1][1] if pts else 0
                hot_streak[fn] = hot_streak[fn] + 1 if last > HOT[fn] else 0
                if hot_streak[fn] >= 3:
                    tripped.append(fn)
        series = {fn: per_minute(fn, "Invocations", restored_at - timedelta(minutes=3), datetime.now(timezone.utc)) for fn in FNS}
        out["post_restore_invocations_1m"] = series
        for fn in FNS:
            r.log("%s invocations/min after restore: %s" % (fn, series[fn]))
        if tripped:
            for fn in FNS:
                quarantine(fn)
            ledger["requarantined_at"] = datetime.now(timezone.utc).isoformat()
            ledger["requarantine_reason"] = "hot after restore: %s" % tripped
            s3.put_object(Bucket=BUCKET, Key=LEDGER_KEY, Body=json.dumps(ledger, indent=1).encode(),
                          ContentType="application/json", CacheControl="no-store")
            r.fail("loop still hot (%s) - both functions re-quarantined" % tripped)
            out["requarantined"] = True
            OUT_JSON.write_text(json.dumps(out, indent=1, default=str))
            sys.exit(1)
        ledger["lifted_at"] = out["restored_at"]
        ledger["lifted_by"] = "ops 6521 (fix live: coordinator self-loop guard + audit cap, calibrator debounce)"
        s3.put_object(Bucket=BUCKET, Key=LEDGER_KEY, Body=json.dumps(ledger, indent=1).encode(),
                      ContentType="application/json", CacheControl="no-store")
        out["final"] = {fn: conc(fn) for fn in FNS}
        r.ok("quarantine lifted; 10-minute watch clean: %s" % json.dumps(out["final"]))
        OUT_JSON.write_text(json.dumps(out, indent=1, default=str))
    sys.exit(0)


if __name__ == "__main__":
    main()
