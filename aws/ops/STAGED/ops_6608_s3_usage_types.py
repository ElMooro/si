"""ops 6608 - READ-ONLY: what is S3 billing $8-13 a day? (issue #123; daily health run 2026-10-22)

The governor's per-service record shows Amazon S3 at $12.71 (Oct 18), $8.49 (Oct 19) and $12.83 (Oct 20) against a $10 line,
while storage of ~1 TB is ~$0.8/day and the site egress measured by ops 6543 was ~$4.7/day. One Cost Explorer call
($0.01) grouped by USAGE_TYPE for S3 over the last 8 days names the rest. If the runner's IAM lacks ce:GetCostAndUsage
the report says so and nothing else is tried.
"""
from datetime import datetime, timezone, timedelta
import json
import sys
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

CFG = Config(retries={"max_attempts": 4, "mode": "standard"}, connect_timeout=10, read_timeout=60)


def main():
    now = datetime.now(timezone.utc)
    with report("ops_6608_s3_usage_types") as r:
        r.heading("ops 6608 - S3 cost by usage type, last 8 days (read-only), %s" % now.strftime("%Y-%m-%d %H:%M UTC"))
        ce = boto3.client("ce", region_name="us-east-1", config=CFG)
        end = now.date()
        start = end - timedelta(days=8)
        try:
            res = ce.get_cost_and_usage(
                TimePeriod={"Start": start.isoformat(), "End": end.isoformat()}, Granularity="DAILY", Metrics=["UnblendedCost", "UsageQuantity"],
                Filter={"Dimensions": {"Key": "SERVICE", "Values": ["Amazon Simple Storage Service"]}},
                GroupBy=[{"Type": "DIMENSION", "Key": "USAGE_TYPE"}])
        except Exception as ex:  # noqa: BLE001
            r.warn("Cost Explorer read refused for the runner: %s %s" % (type(ex).__name__, str(ex)[:300]))
            r.kv(read=False)
            sys.exit(0)
        total_by_day, by_type = {}, {}
        for day in res["ResultsByTime"]:
            d = day["TimePeriod"]["Start"]
            rows = []
            for g in day["Groups"]:
                usd = float(g["Metrics"]["UnblendedCost"]["Amount"])
                qty = float(g["Metrics"]["UsageQuantity"]["Amount"])
                ut = g["Keys"][0]
                total_by_day[d] = total_by_day.get(d, 0.0) + usd
                by_type.setdefault(ut, {})[d] = (round(usd, 3), round(qty, 1))
                if usd >= 0.05:
                    rows.append((usd, ut, qty))
            rows.sort(reverse=True)
            r.log("%s  S3 $%.2f  top: %s" % (d, total_by_day.get(d, 0.0), "; ".join("%s $%.2f (%.0f)" % (ut, usd, qty) for usd, ut, qty in rows[:8])))
        r.section("usage types with any day >= $0.50")
        for ut, days in sorted(by_type.items(), key=lambda kv: -max(v[0] for v in kv[1].values())):
            if max(v[0] for v in days.values()) >= 0.5:
                r.log("%s: %s" % (ut, json.dumps(sorted(days.items()))))
        r.kv(read=True, days=len(total_by_day), latest_day=max(total_by_day) if total_by_day else None,
             latest_usd=round(total_by_day[max(total_by_day)], 2) if total_by_day else None)
        r.ok("read-only; one Cost Explorer call")
    sys.exit(0)


if __name__ == "__main__":
    main()
