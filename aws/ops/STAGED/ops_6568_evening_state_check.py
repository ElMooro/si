"""ops 6568 - READ-ONLY end-of-day check (2026-10-20): governor version after the v1.4.2 deploy, stage, the two restored
functions' freeze state (the hourly tick must have throttled them or re-held their ledger entries), the operator
holds and the CAP-locked loop engines, the new scanner schedule, the alert-relay marker, the fleet count.
"""
from pathlib import Path
import json
import sys

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION, ACCOUNT = "us-east-1", "857687956942"
PRIVATE = "justhodl-ops-private-%s" % ACCOUNT
CFG = Config(retries={"max_attempts": 10, "mode": "standard"}, connect_timeout=10, read_timeout=120)
RESTORED = ["justhodl-insider-cluster-scanner", "justhodl-warroom-weights"]
WATCH = ["justhodl-miss-calibrator", "justhodl-event-coordinator", "macro-financial-intelligence", "ofrapi", "treasury-api"]


def main():
    lam = boto3.client("lambda", region_name=REGION, config=CFG)
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    scheduler = boto3.client("scheduler", region_name=REGION, config=CFG)
    with report("ops_6568_evening_state_check") as r:
        r.heading("ops 6568 - evening state check (read-only)")
        res = lam.invoke(FunctionName="justhodl-spend-governor", Payload=json.dumps({"action": "status"}).encode())
        st = json.loads(res["Payload"].read() or b"{}")
        r.log("governor: version %s, mode %s, stage %s, budget actual $%s (forecast $%s), held %s, capped %s, anomaly held %s" % (
            st.get("version"), st.get("mode"), st.get("stage"), st.get("budget_actual"), st.get("budget_forecast"),
            json.dumps(st.get("held")), st.get("capped"), st.get("anomaly_held")))
        state = json.loads(s3.get_object(Bucket=PRIVATE, Key="spend-governor/state.json")["Body"].read())
        ledger = state.get("ledger") or {}
        for fn in RESTORED + WATCH:
            try:
                cur = lam.get_function_concurrency(FunctionName=fn).get("ReservedConcurrentExecutions")
            except Exception as ex:  # noqa: BLE001
                cur = "err:%s" % type(ex).__name__
            e = ledger.get("lambda:" + fn) or {}
            r.log("%s: reserved concurrency %s; ledger stage %s (before %s)" % (fn, cur, e.get("stage"), (e.get("before") or {}).get("reserved")))
        sch = scheduler.get_schedule(Name="justhodl-insider-cluster-scanner-daily", GroupName="default")
        r.log("justhodl-insider-cluster-scanner-daily: %s %s -> %s" % (sch["ScheduleExpression"], sch["State"], sch["Target"]["Arn"].split(":")[-1]))
        try:
            marker = json.loads(s3.get_object(Bucket=PRIVATE, Key="spend-governor/alerts/github-issues.json")["Body"].read())
        except Exception as ex:  # noqa: BLE001
            marker = {"error": type(ex).__name__}
        r.log("alert relay marker: %s" % json.dumps(marker))
        n = sum(len(p["Functions"]) for p in lam.get_paginator("list_functions").paginate())
        r.kv(functions=n, governor_version=st.get("version"), stage=st.get("stage"), budget_actual=st.get("budget_actual"))
        r.ok("checked")


if __name__ == "__main__":
    main()
