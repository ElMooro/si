"""ops 6716 - READ-ONLY groundwork for the fleet-wide trigger cap (Khalid, 2026-11-02 22:28 ET: "on top of the existing
brakes I don't want engines to trigger each other or themselves in a never-ending loop; a max of how many engines get
triggered, past it it gets shut down and I have to manually turn it back on").

The cap will be enforced in one shared module at every place an engine starts another engine (system_events.publish,
the coordinator's routing, chain_guard's direct and self invokes), with its counters in DynamoDB and the shutdown done by
the spend governor's operator-hold lever (stage CAP, released only by release_cap).  This reads what that needs:
  A. the DynamoDB table justhodl-event-breaker (the coordinator's hourly budgets live there): key schema, TTL, billing,
     item count - to be reused for the trigger ledger (prefixes chain#/in#/out#/self#/breach#);
  B. every execution role in use (914 functions grouped by role) and, per role, IAM's answer to the three actions the
     guard needs: dynamodb:UpdateItem on that table, dynamodb:GetItem, lambda:InvokeFunction on justhodl-spend-governor;
  C. justhodl-spend-governor-canary: configuration + description (the self-test target for the live proof);
  D. the live master-ranker schedule (the coordinator's test suite pins it; the repo's two declarations disagree);
  E. the coordinator and governor as deployed: LastModified, reserved concurrency, env (names only).
Nothing is changed.  No key involved.
"""
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import json
import sys

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION, ACCOUNT = "us-east-1", "857687956942"
TABLE = "justhodl-event-breaker"
TABLE_ARN = "arn:aws:dynamodb:%s:%s:table/%s" % (REGION, ACCOUNT, TABLE)
GOV_ARN = "arn:aws:lambda:%s:%s:function:justhodl-spend-governor" % (REGION, ACCOUNT)
CFG = Config(retries={"max_attempts": 10, "mode": "adaptive"})


def main():
    lam = boto3.client("lambda", region_name=REGION, config=CFG)
    ddb = boto3.client("dynamodb", region_name=REGION, config=CFG)
    iam = boto3.client("iam", config=CFG)
    sch = boto3.client("scheduler", region_name=REGION, config=CFG)
    with report("ops_6716_trigger_cap_probe") as r:
        r.heading("ops 6716 - groundwork for the fleet-wide trigger cap (read-only)")
        r.section("A. the breaker table")
        t = ddb.describe_table(TableName=TABLE)["Table"]
        ttl = ddb.describe_time_to_live(TableName=TABLE).get("TimeToLiveDescription", {})
        r.log("%s: keys %s | attrs %s | billing %s | items %s | bytes %s | status %s | TTL %s on %s" % (
            TABLE, [(k["AttributeName"], k["KeyType"]) for k in t["KeySchema"]], [(a["AttributeName"], a["AttributeType"]) for a in t["AttributeDefinitions"]],
            (t.get("BillingModeSummary") or {}).get("BillingMode"), t.get("ItemCount"), t.get("TableSizeBytes"), t.get("TableStatus"),
            ttl.get("TimeToLiveStatus"), ttl.get("AttributeName")))
        sample = ddb.scan(TableName=TABLE, Limit=5).get("Items", [])
        r.log("sample keys: %s" % [i["pk"]["S"] for i in sample])

        r.section("B. execution roles and what IAM allows them")
        fns = []
        for page in lam.get_paginator("list_functions").paginate():
            fns += page["Functions"]
        by_role = defaultdict(list)
        for f in fns:
            by_role[f["Role"]].append(f["FunctionName"])
        r.log("%d functions, %d roles" % (len(fns), len(by_role)))

        def sim(role_arn):
            out = {}
            for action, res in (("dynamodb:UpdateItem", TABLE_ARN), ("dynamodb:GetItem", TABLE_ARN), ("dynamodb:Scan", TABLE_ARN),
                                ("lambda:InvokeFunction", GOV_ARN), ("events:PutEvents", "arn:aws:events:%s:%s:event-bus/justhodl-system-events" % (REGION, ACCOUNT))):
                try:
                    s = iam.simulate_principal_policy(PolicySourceArn=role_arn, ActionNames=[action], ResourceArns=[res])
                    out[action] = s["EvaluationResults"][0]["EvalDecision"]
                except Exception as e:  # noqa: BLE001
                    out[action] = "ERR " + type(e).__name__
            return role_arn, out
        with ThreadPoolExecutor(max_workers=4) as ex:
            results = dict(ex.map(sim, list(by_role)))
        for role_arn, names in sorted(by_role.items(), key=lambda kv: -len(kv[1])):
            d = results[role_arn]
            r.log("%-45s %4d fns | ddb Update %-12s Get %-12s Scan %-12s | invoke governor %-12s | PutEvents %-12s | e.g. %s" % (
                role_arn.split("/")[-1], len(names), d["dynamodb:UpdateItem"], d["dynamodb:GetItem"], d["dynamodb:Scan"],
                d["lambda:InvokeFunction"], d["events:PutEvents"], sorted(names)[:3]))
        relays = ["justhodl-event-coordinator", "justhodl-spend-governor", "justhodl-calibrator", "justhodl-13f-positions", "justhodl-a2a-bus",
                  "justhodl-backend-agent", "justhodl-symdir", "justhodl-tv-bars", "justhodl-fundamental-census", "justhodl-miss-calibrator",
                  "justhodl-outcome-checker", "justhodl-sec-13f", "justhodl-worldbank-full", "justhodl-chain-resumer", "justhodl-sdmx-walker",
                  "justhodl-equity-research", "justhodl-13f-clone-alpha", "justhodl-import-sentinel", "justhodl-stock-screener"]
        role_of = {f["FunctionName"]: f["Role"].split("/")[-1] for f in fns}
        r.log("roles of the relay / trigger engines: %s" % {n: role_of.get(n, "MISSING") for n in relays})

        r.section("C. the canary")
        try:
            c = lam.get_function_configuration(FunctionName="justhodl-spend-governor-canary")
            rc = lam.get_function_concurrency(FunctionName="justhodl-spend-governor-canary").get("ReservedConcurrentExecutions")
            r.log("justhodl-spend-governor-canary: %s MB / %s s, runtime %s, role %s, modified %s, reserved %s | %s" % (
                c["MemorySize"], c["Timeout"], c.get("Runtime"), c["Role"].split("/")[-1], c["LastModified"][:16], rc, (c.get("Description") or "")[:200]))
        except Exception as e:  # noqa: BLE001
            r.log("canary: %s" % type(e).__name__)

        r.section("D. master-ranker schedule as live")
        s = sch.get_schedule(Name="justhodl-master-ranker-weekday-close", GroupName="default")
        r.log("justhodl-master-ranker-weekday-close: %s %s %s target %s" % (s.get("ScheduleExpression"), s.get("ScheduleExpressionTimezone"), s.get("State"), s["Target"]["Arn"].split(":")[-1]))

        r.section("E. coordinator + governor as deployed")
        for fn in ("justhodl-event-coordinator", "justhodl-spend-governor", "justhodl-chain-resumer"):
            c = lam.get_function_configuration(FunctionName=fn)
            rc = lam.get_function_concurrency(FunctionName=fn).get("ReservedConcurrentExecutions")
            r.log("%s: modified %s, reserved %s, %s MB / %s s, env %s" % (fn, c["LastModified"][:19], rc, c["MemorySize"], c["Timeout"],
                                                                        sorted(((c.get("Environment") or {}).get("Variables") or {}))))
        r.ok("ops 6716 complete (read-only)")


if __name__ == "__main__":
    main()
    sys.exit(0)
