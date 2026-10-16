"""ops 6528 - (1) justhodl-fleet-error-monitor 5-minute -> hourly; (2) verify the ops 6525 freeze properly.

(1) Cost.  The monitor read 3 CloudWatch metrics x ~924 functions every 5 minutes with GetMetricStatistics
($0.01 per 1,000 requests): ~600-800k requests/day, ~$6/day, most of the ~$9/day CloudWatch bill (ops 6519/6527).
v1.2.0 (separate push, after this op) reads Errors+Throttles first and Invocations only when needed, over a
60-minute window.  This op moves the LIVE rule first, because deploy-lambdas' existing-schedule guard refuses a
config.json whose cadence differs from the live binding:
    events.put_rule fleet-error-monitor-5min  rate(5 minutes) -> rate(1 hour)   (name kept: the classic rule
    cap is saturated; state, description and targets preserved)
and records it in the schedule manifest (repo copy committed with this op + the S3 copy the reconciler reads).
The manifest doctrine forbids put_rule from ops scripts because NEW rules/targets created that way piled up
duplicates; this is the one-field expression change the reconciler itself makes for EXPR_DRIFT on an existing
rule, with the manifest updated in the same step (running the reconciler in fleet-wide enforce mode instead
would also disable every undeclared rule).

(2) Freeze check.  ops 6525 forced FREEZE at 22:53 UTC (914 Lambdas + the async endpoint held, 1 entry failed to
apply) and then went red only on its own last step (a GetMetricData period that was not a whole minute).  Here:
the governor's ledger failures, the governor stage/plan, and every function with invocations in the last
15 whole minutes - which must all be exempt.
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import sys
import time

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
RULE = "fleet-error-monitor-5min"
NEW_EXPR = "rate(1 hour)"
MANIFEST_KEY = "config/schedule-manifest.json"
GOV = "justhodl-spend-governor"
STATE_BUCKET = "justhodl-ops-private-857687956942"
EXEMPT = {"justhodl-spend-governor", "justhodl-guardrail-notify", "justhodl-supabase-keepalive", "justhodl-dr-snapshot"}
OUT_JSON = ROOT / "aws" / "ops" / "reports" / "6528.json"
ev = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION, config=Config(retries={"max_attempts": 8, "mode": "standard"}))
lam_sync = boto3.client("lambda", region_name=REGION, config=Config(retries={"max_attempts": 0}, read_timeout=910))
out = {"op": 6528, "ran_at": datetime.now(timezone.utc).isoformat()}


def retime_rule(r):
    cur = ev.describe_rule(Name=RULE)
    out["rule_before"] = {k: cur.get(k) for k in ("ScheduleExpression", "State", "Description")}
    targets = ev.list_targets_by_rule(Rule=RULE).get("Targets", [])
    if cur.get("ScheduleExpression") != NEW_EXPR:
        kw = {"Name": RULE, "ScheduleExpression": NEW_EXPR, "State": cur["State"],
              "Description": "AUDIT P2 - fleet error rates + DLQ depth hourly (ops 6528; was every 5 minutes)"}
        if cur.get("RoleArn"):
            kw["RoleArn"] = cur["RoleArn"]
        ev.put_rule(**kw)
    after = ev.describe_rule(Name=RULE)
    targets_after = ev.list_targets_by_rule(Rule=RULE).get("Targets", [])
    out["rule_after"] = {k: after.get(k) for k in ("ScheduleExpression", "State", "Description")}
    ok = after["ScheduleExpression"] == NEW_EXPR and after["State"] == cur["State"] and \
        sorted(t["Arn"] for t in targets_after) == sorted(t["Arn"] for t in targets)
    (r.ok if ok else r.fail)("rule %s: %s -> %s (state %s, %d target(s) kept)" % (
        RULE, out["rule_before"]["ScheduleExpression"], after["ScheduleExpression"], after["State"], len(targets_after)))
    return ok


def update_manifest(r):
    """S3 copy (what the reconciler reads) gets the same one-field change as the repo copy in this commit."""
    try:
        doc = json.loads(s3.get_object(Bucket=BUCKET, Key=MANIFEST_KEY)["Body"].read())
    except ClientError as e:
        r.warn("S3 manifest unreadable (%s) - repo copy only" % e.response["Error"]["Code"])
        return None
    n = 0
    for rule in doc.get("rules", []):
        if rule.get("name") == RULE and rule.get("expr") != NEW_EXPR:
            rule["expr"] = NEW_EXPR
            n += 1
    if n:
        s3.put_object(Bucket=BUCKET, Key=MANIFEST_KEY, Body=json.dumps(doc, indent=1).encode(),
                      ContentType="application/json", CacheControl="no-store")
    r.ok("S3 schedule manifest: %d entry updated" % n)
    return n


def invoke_gov(payload):
    for i in range(8):
        try:
            res = lam_sync.invoke(FunctionName=GOV, InvocationType="RequestResponse", Payload=json.dumps(payload).encode())
            return json.loads(res["Payload"].read() or b"null")
        except ClientError as e:
            if e.response["Error"]["Code"] == "TooManyRequestsException" and i < 7:
                time.sleep(30)
                continue
            raise


def freeze_check(r):
    st = invoke_gov({"action": "status"})
    out["governor"] = {k: st.get(k) for k in ("mode", "stage", "stage_since", "held", "cap", "cap_source")}
    (r.ok if st.get("stage") == "FREEZE" else r.fail)("governor: %s" % json.dumps(out["governor"]))
    state = json.loads(s3.get_object(Bucket=STATE_BUCKET, Key="spend-governor/state.json")["Body"].read())
    failed = [{"key": e["key"], "error": e.get("error"), "status": e.get("status")}
              for e in (state.get("ledger") or {}).values() if e.get("status") not in ("applied", "reapplied")]
    out["ledger_not_applied"] = failed
    r.log("ledger entries not applied: %s" % json.dumps(failed)[:600])
    plan = invoke_gov({"action": "plan", "stage": "FREEZE"})
    out["plan"] = plan.get("would_act_on")
    r.log("plan FREEZE now: %s" % json.dumps(plan.get("would_act_on")))
    end = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    start = end - timedelta(minutes=15)
    names = [f["FunctionName"] for p in lam.get_paginator("list_functions").paginate() for f in p["Functions"]]
    q = [{"Id": "i%d" % i, "ReturnData": True, "MetricStat": {
        "Metric": {"Namespace": "AWS/Lambda", "MetricName": "Invocations",
                   "Dimensions": [{"Name": "FunctionName", "Value": n}]}, "Period": 900, "Stat": "Sum"}}
         for i, n in enumerate(names)]
    vals = {}
    for i in range(0, len(q), 500):
        token = None
        while True:
            kw = {"MetricDataQueries": q[i:i + 500], "StartTime": start, "EndTime": end}
            if token:
                kw["NextToken"] = token
            resp = cw.get_metric_data(**kw)
            for res in resp["MetricDataResults"]:
                vals[res["Id"]] = vals.get(res["Id"], 0.0) + sum(res.get("Values") or [])
            token = resp.get("NextToken")
            if not token:
                break
    running = {names[int(k[1:])]: int(v) for k, v in vals.items() if v > 0}
    rogue = {k: v for k, v in running.items() if k not in EXEMPT}
    out["invoking_last_15min"] = running
    (r.ok if not rogue else r.fail)("functions invoking %s-%s UTC: %s" % (start.strftime("%H:%M"), end.strftime("%H:%M"), json.dumps(running)))
    acct = cw.get_metric_statistics(Namespace="AWS/Lambda", MetricName="Throttles", StartTime=end - timedelta(hours=1),
                                    EndTime=end, Period=3600, Statistics=["Sum"]).get("Datapoints", [])
    out["account_throttles_1h"] = int(sum(p["Sum"] for p in acct))
    r.log("account throttles last hour (scheduled runs refused): %d" % out["account_throttles_1h"])
    return not rogue and st.get("stage") == "FREEZE"


def main():
    with report("ops_6528_error_monitor_hourly_and_freeze_check") as r:
        r.heading("ops 6528 - error monitor hourly + freeze verification")
        failed = []
        r.section("Error monitor cadence")
        if not retime_rule(r):
            failed.append("rule")
        update_manifest(r)
        r.section("Freeze check")
        try:
            if not freeze_check(r):
                failed.append("freeze")
        except Exception as e:  # noqa: BLE001
            failed.append("freeze_check")
            r.fail("freeze check raised %s: %s" % (type(e).__name__, str(e)[:300]))
        OUT_JSON.write_text(json.dumps(out, indent=1, default=str))
        r.ok("wrote %s" % OUT_JSON.relative_to(ROOT))
        if failed:
            r.fail("failed: %s" % failed)
            sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
