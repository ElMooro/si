"""ops 6717 - two inline grants for the fleet-wide trigger cap + the real invocation peaks its daily budgets are set from.

A. IAM (the only two roles the cap needs that lack rights; ops 6716): justhodl-spend-governor-role gets
   dynamodb Get/Put/Update/Scan/Query on the table justhodl-event-breaker (the governor reads a breach record there
   before holding anything, and sweeps pending breaches every tick); justhodl-student-rsi-role gets
   dynamodb Get/Put/Update on the same table + lambda:InvokeFunction on the governor (its sync call to
   justhodl-factory-grader goes through the guarded client; the factory is off but the code must not break when it
   returns).  Simulated after the write.  Reversible: delete the inline policies.
B. READ-ONLY: CloudWatch Invocations per day (last 60 days) for every engine that is started by another engine or by
   itself (the destination set of scripts/event_graph.py + the self-chaining walkers), with the peak day, p95 and the
   count of active days - the daily budgets in config/trigger-limits.json are >= 2x each engine's peak.
No key involved.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import statistics
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION, ACCOUNT = "us-east-1", "857687956942"
TABLE_ARN = "arn:aws:dynamodb:%s:%s:table/justhodl-event-breaker" % (REGION, ACCOUNT)
GOV_ARN = "arn:aws:lambda:%s:%s:function:justhodl-spend-governor" % (REGION, ACCOUNT)
GRANTS = {
    "justhodl-spend-governor-role": ("trigger-ledger-governor", {"Version": "2012-10-17", "Statement": [
        {"Sid": "TriggerLedger", "Effect": "Allow", "Action": ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:Scan", "dynamodb:Query"],
         "Resource": TABLE_ARN}]}),
    "justhodl-student-rsi-role": ("trigger-ledger-engine", {"Version": "2012-10-17", "Statement": [
        {"Sid": "TriggerLedger", "Effect": "Allow", "Action": ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem"], "Resource": TABLE_ARN},
        {"Sid": "TriggerBreachToGovernor", "Effect": "Allow", "Action": "lambda:InvokeFunction", "Resource": [GOV_ARN, GOV_ARN + ":*"]}]}),
}
ENGINES = sorted({
    # coordinator route targets
    "justhodl-alpha-compass", "justhodl-jh-fusion", "justhodl-signal-board", "justhodl-engine-signal-map", "justhodl-calibrator",
    "justhodl-alpha-calibrator", "justhodl-miss-calibrator",
    # direct invoke destinations
    "justhodl-page-ai-commentary", "justhodl-backend-agent", "justhodl-a2a-bus", "justhodl-bottleneck-research", "justhodl-research-critique",
    "justhodl-fundamental-graphs", "justhodl-census-us", "justhodl-fred-catalog", "justhodl-portfolio-snapshot", "justhodl-13f-positions",
    "justhodl-screener-alerts", "justhodl-factory-grader", "justhodl-tv-bars", "treasury-api", "openbb-websocket-broadcast",
    # self-chaining (raw) + chain_guard walkers
    "justhodl-13f-clone-alpha", "justhodl-equity-research", "justhodl-fundamental-census", "justhodl-sdmx-walker", "justhodl-symdir", "justhodl-boj-full",
    "justhodl-bls-full", "justhodl-ecb-deep", "justhodl-finra-full", "justhodl-fiscaldata-full", "justhodl-gdelt-full", "justhodl-hist-banker",
    "justhodl-imf-full", "justhodl-polygon-full", "justhodl-sec-midas", "justhodl-trend-reversal", "justhodl-worldbank-full",
    # sources (for the out budgets)
    "justhodl-outcome-checker", "justhodl-sec-13f", "justhodl-import-sentinel", "justhodl-equity-prewarm", "justhodl-streaming-fanout",
    "justhodl-stock-screener", "justhodl-audit-loop", "justhodl-bottleneck-boom", "justhodl-portfolio-admin", "justhodl-student-rsi",
})
CFG = Config(retries={"max_attempts": 10, "mode": "adaptive"})


def main():
    iam = boto3.client("iam", config=CFG)
    cw = boto3.client("cloudwatch", region_name=REGION, config=CFG)
    now = datetime.now(timezone.utc)
    with report("ops_6717_trigger_cap_grants_and_budgets") as r:
        r.heading("ops 6717 - trigger cap: the two role grants + 60-day invocation peaks for the budgets")
        r.section("A. IAM grants")
        for role, (name, doc) in GRANTS.items():
            iam.put_role_policy(RoleName=role, PolicyName=name, PolicyDocument=json.dumps(doc))
            arn = iam.get_role(RoleName=role)["Role"]["Arn"]
            want = [("dynamodb:UpdateItem", TABLE_ARN), ("dynamodb:GetItem", TABLE_ARN)]
            if role == "justhodl-spend-governor-role":
                want.append(("dynamodb:Scan", TABLE_ARN))
            else:
                want.append(("lambda:InvokeFunction", GOV_ARN))
            dec = {}
            for _ in range(10):
                dec = {a: iam.simulate_principal_policy(PolicySourceArn=arn, ActionNames=[a], ResourceArns=[res])["EvaluationResults"][0]["EvalDecision"] for a, res in want}
                if all(v == "allowed" for v in dec.values()):
                    break
                time.sleep(8)
            (r.ok if all(v == "allowed" for v in dec.values()) else r.fail)("%s += %s: %s" % (role, name, dec))

        r.section("B. invocations per day, last 60 days (CloudWatch Sum, period 1 day)")
        start, end = now - timedelta(days=60), now
        q = [{"Id": "i%d" % i, "MetricStat": {"Metric": {"Namespace": "AWS/Lambda", "MetricName": "Invocations",
              "Dimensions": [{"Name": "FunctionName", "Value": n}]}, "Period": 86400, "Stat": "Sum"}} for i, n in enumerate(ENGINES)]
        vals = {}
        for i in range(0, len(q), 100):
            tok = None
            while True:
                kw = {"MetricDataQueries": q[i:i + 100], "StartTime": start, "EndTime": end}
                if tok:
                    kw["NextToken"] = tok
                res = cw.get_metric_data(**kw)
                for m in res["MetricDataResults"]:
                    vals.setdefault(m["Id"], []).extend(zip(m["Timestamps"], m["Values"]))
                tok = res.get("NextToken")
                if not tok:
                    break
        rows = []
        for i, n in enumerate(ENGINES):
            pts = sorted(vals.get("i%d" % i, []))
            days = [v for _, v in pts if v > 0]
            # the Oct 12-16 storm is not a normal day: report it separately
            storm = [(t.date().isoformat(), int(v)) for t, v in pts if datetime(2026, 10, 12, tzinfo=timezone.utc) <= t <= datetime(2026, 10, 16, 23, tzinfo=timezone.utc) and v > 0]
            normal = [v for t, v in pts if v > 0 and not (datetime(2026, 10, 12, tzinfo=timezone.utc) <= t <= datetime(2026, 10, 16, 23, tzinfo=timezone.utc))]
            peak = max(normal) if normal else 0
            p95 = statistics.quantiles(normal, n=20)[-1] if len(normal) >= 20 else peak
            rows.append((n, len(days), int(peak), round(p95), round(statistics.mean(normal), 1) if normal else 0, storm[:2]))
        for n, active, peak, p95, mean, storm in sorted(rows, key=lambda x: -x[2]):
            r.log("%-36s active days %2d | peak %6d | p95 %6d | mean %8.1f | storm %s" % (n, active, peak, p95, mean, storm))
        r.kv(engines=len(rows))
        r.ok("ops 6717 complete")


if __name__ == "__main__":
    main()
    sys.exit(0)
