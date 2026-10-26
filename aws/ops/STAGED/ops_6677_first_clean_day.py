"""ops 6677 - READ-ONLY: the first full day with every 2026-10-23/24 cost fix in place (Oct 25), against Khalid's ~$200/month
target (docs/audit/2026-10-23/aws-bill-oct22.md section 4: steady day projected at $6.34).

Cost Explorer, Oct 23 -> today: account total and every service per day; S3, CloudWatch and Lambda by usage type -
direct egress (DataTransfer-Out-Bytes, expected ~3 GB/day), the Metrics Insights alarm line (expected $0), the archive's
copies (Requests-INT-Tier1 at $0.005/1k replacing Requests-SIA-Tier1 since Oct 24 14:48 UTC), Intelligent-Tiering storage
and monitoring lines, EarlyDelete-SIA (Standard-IA's 30-day minimum on replaced archive copies). Then a November
projection from the measured day: x30, minus the always-free allowances a fresh month gets (Lambda 400 k GB-s and 1 M
requests, 100 GB of data transfer out, 10 alarms), plus Business Support+ (the greater of $29 or 9% of usage).
Nothing is written but this report.
"""
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))

DAYS_FROM = date(2026, 10, 23)
PROJECTED_DAY = 6.34
LAMBDA_GBS_PRICE, LAMBDA_FREE_GBS = 0.0000166667, 400000
EGRESS_PRICE, EGRESS_FREE_GB = 0.09, 100
ALARM_FREE = 10 * 0.10
TYPES = {
    "Amazon Simple Storage Service": ["DataTransfer-Out-Bytes", "Requests-INT-Tier1", "Requests-SIA-Tier1", "EarlyDelete-SIA",
                                      "Monitoring-Automation-INT", "TimedStorage-INT-FA-ByteHrs", "TimedStorage-INT-IA-ByteHrs",
                                      "TimedStorage-INT-AIA-ByteHrs", "TimedStorage-SIA-ByteHrs", "TimedStorage-SIA-SmObjects",
                                      "TimedStorage-ByteHrs", "Requests-Tier1", "Requests-Tier2", "Retrieval-SIA"],
    "AmazonCloudWatch": ["CW:MetricInsightAlarmUsage", "CW:AlarmMonitorUsage", "CW:GMD-Metrics", "CW:Requests",
                         "DataProcessing-Bytes", "TimedStorage-ByteHrs", "CW:MetricMonitorUsage"],
    "AWS Lambda": ["Lambda-GB-Second", "Lambda-GB-Second-ARM", "Request", "Request-ARM"],
}


def projection(day_total, lambda_gbs_day, egress_gb_day):
    month = day_total * 30
    free = (min(lambda_gbs_day * 30, LAMBDA_FREE_GBS) * LAMBDA_GBS_PRICE + 0.20
            + min(egress_gb_day * 30, EGRESS_FREE_GB) * EGRESS_PRICE + ALARM_FREE)
    usage = max(0.0, month - free)
    support = max(29.0, 0.09 * usage)
    return round(usage, 2), round(support, 2), round(usage + support, 2), round(free, 2)


def selftest():
    u, s, t, f = projection(6.0, 20000, 3.0)
    assert 140 < u < 180 and s == 29.0 and abs(t - (u + s)) < 0.01 and f > 0
    print("selftest ok")


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report

    selftest()
    ce = boto3.client("ce", region_name="us-east-1", config=Config(retries={"max_attempts": 8, "mode": "standard"}))
    today = datetime.now(timezone.utc).date()
    end = today + timedelta(days=1)
    with report("ops_6677_first_clean_day") as r:
        r.heading("ops 6677 - the first clean day (Oct 25) against the ~$200/month target (read-only)")
        r.section("A. account and services per day")
        res = ce.get_cost_and_usage(TimePeriod={"Start": DAYS_FROM.isoformat(), "End": end.isoformat()}, Granularity="DAILY",
                                    Metrics=["UnblendedCost"], Filter={"Not": {"Dimensions": {"Key": "RECORD_TYPE", "Values": ["Credit", "Refund"]}}},
                                    GroupBy=[{"Type": "DIMENSION", "Key": "SERVICE"}])
        totals, est = {}, {}
        for day in res["ResultsByTime"]:
            d = day["TimePeriod"]["Start"]
            rows = sorted(((g["Keys"][0], float(g["Metrics"]["UnblendedCost"]["Amount"])) for g in day["Groups"]), key=lambda x: -x[1])
            totals[d] = sum(v for _, v in rows)
            est[d] = day.get("Estimated")
            r.log("%s%s total $%.2f | %s" % (d, " (today, partial)" if d == today.isoformat() else "", totals[d],
                                             ", ".join("%s $%.2f" % (k[:30], v) for k, v in rows if v >= 0.01)))
        usage = {}
        r.section("B. S3, CloudWatch and Lambda by usage type")
        for svc, types in TYPES.items():
            res = ce.get_cost_and_usage(TimePeriod={"Start": DAYS_FROM.isoformat(), "End": end.isoformat()}, Granularity="DAILY",
                                        Metrics=["UnblendedCost", "UsageQuantity"], Filter={"Dimensions": {"Key": "SERVICE", "Values": [svc]}},
                                        GroupBy=[{"Type": "DIMENSION", "Key": "USAGE_TYPE"}])
            for day in res["ResultsByTime"]:
                d = day["TimePeriod"]["Start"]
                rows = {g["Keys"][0]: (float(g["Metrics"]["UnblendedCost"]["Amount"]), float(g["Metrics"]["UsageQuantity"]["Amount"])) for g in day["Groups"]}
                for k, v in rows.items():
                    usage[(d, k)] = v
                listed = ", ".join("%s $%.3f (%.1f)" % (k, rows[k][0], rows[k][1]) for k in types if k in rows)
                other = sorted(((k, v[0]) for k, v in rows.items() if k not in types and v[0] >= 0.02), key=lambda x: -x[1])
                r.log("%s %-14s $%.2f | %s%s" % (d, svc.replace("Amazon Simple Storage Service", "S3")[:14], sum(v[0] for v in rows.values()),
                                                 listed, (" | other: " + ", ".join("%s $%.2f" % o for o in other[:5])) if other else ""))

        r.section("C. Oct 25 against the projection")
        d = "2026-10-25"
        if d not in totals:
            r.fail("Oct 25 not in Cost Explorer yet")
            sys.exit(1)

        def u(k):
            return usage.get((d, k), (0.0, 0.0))
        egress_gb = u("DataTransfer-Out-Bytes")[1]
        lambda_gbs = u("Lambda-GB-Second")[1] + u("Lambda-GB-Second-ARM")[1]
        r.log("Oct 25 = $%.2f (projected steady day $%.2f; %s)" % (totals[d], PROJECTED_DAY, "estimated" if est.get(d) else "final"))
        r.log("direct S3 egress %.1f GB ($%.2f); Metrics Insights alarm $%.2f; archive copies: Intelligent-Tiering %d ($%.2f), "
              "Standard-IA %d ($%.2f); IT monitoring $%.3f; EarlyDelete-SIA $%.2f" % (
                  egress_gb, u("DataTransfer-Out-Bytes")[0], u("CW:MetricInsightAlarmUsage")[0], u("Requests-INT-Tier1")[1],
                  u("Requests-INT-Tier1")[0], u("Requests-SIA-Tier1")[1], u("Requests-SIA-Tier1")[0], u("Monitoring-Automation-INT")[0],
                  u("EarlyDelete-SIA")[0]))
        use, sup, tot, free = projection(totals[d], lambda_gbs, egress_gb)
        r.log("November from Oct 25: $%.2f x 30 = $%.2f, minus the free allowances of a fresh month ~$%.2f = $%.2f of usage, "
              "plus Business Support+ $%.2f = ~$%.2f" % (totals[d], totals[d] * 30, free, use, sup, tot))
        r.kv(oct25_total=round(totals[d], 2), oct25_estimated=bool(est.get(d)), projected_day=PROJECTED_DAY,
             egress_gb=round(egress_gb, 1), int_puts=round(u("Requests-INT-Tier1")[1]), sia_puts=round(u("Requests-SIA-Tier1")[1]),
             metric_insights_usd=round(u("CW:MetricInsightAlarmUsage")[0], 2), early_delete_sia_usd=round(u("EarlyDelete-SIA")[0], 2),
             november_usage=use, november_support=sup, november_total=tot)
        r.ok("read-only; nothing changed")
    sys.exit(0)


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        main()
