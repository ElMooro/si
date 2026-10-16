"""ops 6525 - FREEZE the fleet until 2026-11-01 (Khalid's decision, 2026-10-16).

Khalid, asked how the rest of October should run after the loop pushed the month to ~$2.4k:
"Freeze until Nov 1" (all engines stop updating, the site shows its last data, auto-restore on the 1st).

Through the spend governor (so every change is ledgered with its BEFORE value and restored exactly by the
governor's month rollover at the 2026-11-01 00:07 UTC tick):
    {"action": "force", "stage": "FREEZE", "confirm": "FREEZE"}
Exempt and still running: justhodl-spend-governor, justhodl-guardrail-notify, justhodl-supabase-keepalive
(Supabase pauses after 7 idle days -> login breaks), justhodl-dr-snapshot (daily backups).

Proof: plan FREEZE afterwards has nothing left to act on; governor stage FREEZE; account-level Lambda
invocations per minute before vs after; the functions still invoking after the freeze are all exempt.
"""

from datetime import datetime, timezone, timedelta
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
FN = "justhodl-spend-governor"
EXEMPT = {"justhodl-spend-governor", "justhodl-guardrail-notify", "justhodl-supabase-keepalive", "justhodl-dr-snapshot"}
OUT_JSON = ROOT / "aws" / "ops" / "reports" / "6525.json"
lam = boto3.client("lambda", region_name=REGION, config=Config(retries={"max_attempts": 8, "mode": "standard"}))
lam_sync = boto3.client("lambda", region_name=REGION, config=Config(retries={"max_attempts": 0}, read_timeout=910))
cw = boto3.client("cloudwatch", region_name=REGION)
out = {"op": 6525, "ran_at": datetime.now(timezone.utc).isoformat()}


def invoke(payload):
    for i in range(8):
        try:
            res = lam_sync.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=json.dumps(payload).encode())
            body = json.loads(res["Payload"].read() or b"null")
            return {"function_error": res["FunctionError"], "body": body} if res.get("FunctionError") else body
        except ClientError as e:
            if e.response["Error"]["Code"] == "TooManyRequestsException" and i < 7:
                time.sleep(30)  # the hourly tick holds the governor's single slot
                continue
            raise


def account_per_minute(metric, stat, start, end):
    r = cw.get_metric_statistics(Namespace="AWS/Lambda", MetricName=metric, StartTime=start, EndTime=end,
                                 Period=60, Statistics=[stat])
    return [(p["Timestamp"].strftime("%H:%M"), int(p[stat])) for p in sorted(r.get("Datapoints", []), key=lambda p: p["Timestamp"])]


def invoking_functions(names, start, end):
    """Functions with any invocation in [start, end) - one GetMetricData pass over the fleet."""
    q = [{"Id": "i%d" % i, "ReturnData": True, "MetricStat": {
        "Metric": {"Namespace": "AWS/Lambda", "MetricName": "Invocations",
                   "Dimensions": [{"Name": "FunctionName", "Value": n}]},
        "Period": int((end - start).total_seconds()), "Stat": "Sum"}} for i, n in enumerate(names)]
    vals = {}
    for i in range(0, len(q), 500):
        token = None
        while True:
            kw = {"MetricDataQueries": q[i:i + 500], "StartTime": start, "EndTime": end}
            if token:
                kw["NextToken"] = token
            r = cw.get_metric_data(**kw)
            for res in r["MetricDataResults"]:
                vals[res["Id"]] = vals.get(res["Id"], 0.0) + sum(res.get("Values") or [])
            token = r.get("NextToken")
            if not token:
                break
    return {names[int(k[1:])]: int(v) for k, v in vals.items() if v > 0}


def main():
    with report("ops_6525_october_freeze") as r:
        r.heading("ops 6525 - freeze the fleet until 2026-11-01 (Khalid's decision)")
        failed = []
        now = datetime.now(timezone.utc)
        out["before_invocations_per_min"] = account_per_minute("Invocations", "Sum", now - timedelta(minutes=20), now)
        r.log("account invocations/min before: %s" % out["before_invocations_per_min"][-8:])

        res = invoke({"action": "force", "stage": "FREEZE", "confirm": "FREEZE",
                      "reason": "Khalid 2026-10-16: freeze until Nov 1 (October already ~$2.4k from the event loop)"})
        out["force"] = res
        frozen_at = datetime.now(timezone.utc)
        out["frozen_at"] = frozen_at.isoformat()
        ok = isinstance(res, dict) and res.get("stage") == "FREEZE" and not res.get("function_error")
        (r.ok if ok else r.fail)("force FREEZE: %s" % json.dumps(res)[:700])
        if not ok:
            failed.append("force")

        plan = invoke({"action": "plan", "stage": "FREEZE"})
        out["plan_after"] = plan
        left = (plan or {}).get("would_act_on") or {}
        (r.ok if not left.get("lambda") else r.warn)("plan FREEZE after the force: %s (exempt %s)" % (json.dumps(left), plan.get("exempt")))
        st = invoke({"action": "status"})
        out["status"] = {k: st.get(k) for k in ("mode", "stage", "stage_since", "cap", "cap_source", "held")}
        (r.ok if st.get("stage") == "FREEZE" else r.fail)("governor status: %s" % json.dumps(out["status"]))
        if st.get("stage") != "FREEZE":
            failed.append("status")

        # let in-flight invocations drain, then prove nothing but exempt functions still runs
        while datetime.now(timezone.utc) < frozen_at + timedelta(minutes=9):
            time.sleep(15)
        end = datetime.now(timezone.utc)
        out["after_invocations_per_min"] = account_per_minute("Invocations", "Sum", frozen_at - timedelta(minutes=3), end)
        out["after_throttles_per_min"] = account_per_minute("Throttles", "Sum", frozen_at - timedelta(minutes=3), end)
        r.log("account invocations/min around the freeze: %s" % out["after_invocations_per_min"])
        r.log("account throttles/min around the freeze: %s" % out["after_throttles_per_min"])
        names = [f["FunctionName"] for p in lam.get_paginator("list_functions").paginate() for f in p["Functions"]]
        still = invoking_functions(names, frozen_at + timedelta(minutes=4), end)
        out["still_invoking"] = still
        rogue = {k: v for k, v in still.items() if k not in EXEMPT}
        (r.ok if not rogue else r.warn)("functions invoking 4-9 min after the freeze: %s" % json.dumps(still))
        if rogue:
            # a function can finish an in-flight async retry; it is a failure only if it is not throttled now
            unthrottled = [fn for fn in rogue if lam.get_function_concurrency(FunctionName=fn).get("ReservedConcurrentExecutions") != 0]
            out["rogue_unthrottled"] = unthrottled
            if unthrottled:
                failed.append("unthrottled %s" % unthrottled)
                r.fail("still running and NOT throttled: %s" % unthrottled)
        OUT_JSON.write_text(json.dumps(out, indent=1, default=str))
        r.ok("wrote %s" % OUT_JSON.relative_to(ROOT))
        if failed:
            r.fail("failed: %s" % failed)
            sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
