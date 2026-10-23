"""ops 6622 - READ-ONLY: what the AWS bill comes to at today's steady state (Khalid, 2026-10-23 15:11 CT: "How much
would my monthly bill be every month?").

Cost Explorer (unblended, daily): every service for the last 12 days; S3 and CloudWatch by usage type for the thawed
days (Oct 21 onward, after the 00:29 UTC thaw and the daily-cadence passes).  One-off lines are separated (the archive
backfill PUTs / Batch Operations, the Business Support charge) so the recurring run-rate can be projected to a month:
median of the clean thawed days x 30, plus the fixed monthly items (Business Support plan, the archive's Standard-IA),
with a low / high band from the best and worst thawed day.  Month-to-date and the governor's budget line are read for
context.  Nothing is changed; four Cost Explorer calls (~$0.01 each).
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
from collections import defaultdict
import json
import statistics
import sys

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

CFG = Config(retries={"max_attempts": 8, "mode": "standard"}, connect_timeout=10, read_timeout=120)
THAW_DAY = "2026-10-21"
ONE_OFF_USAGE = ("BatchOperations", "Requests-SIA-Tier1")      # the archive backfills (ops 6583 / 6612)
SUPPORT = "AWS Business Support+"


def ce_daily(ce, start, end, group_key, filt=None):
    kw = {"TimePeriod": {"Start": start, "End": end}, "Granularity": "DAILY", "Metrics": ["UnblendedCost"],
          "GroupBy": [{"Type": "DIMENSION", "Key": group_key}]}
    if filt:
        kw["Filter"] = filt
    out = defaultdict(dict)
    token = None
    while True:
        if token:
            kw["NextPageToken"] = token
        res = ce.get_cost_and_usage(**kw)
        for day in res.get("ResultsByTime", []):
            d = day["TimePeriod"]["Start"]
            for g in day.get("Groups", []):
                out[d][g["Keys"][0]] = out[d].get(g["Keys"][0], 0.0) + float(g["Metrics"]["UnblendedCost"]["Amount"])
        token = res.get("NextPageToken")
        if not token:
            break
    return out


def main():
    ce = boto3.client("ce", region_name="us-east-1", config=CFG)
    today = datetime.now(timezone.utc).date()
    start = (today - timedelta(days=12)).isoformat()
    end = today.isoformat()
    with report("ops_6622_monthly_bill_projection") as r:
        r.heading("ops 6622 - the monthly bill at today's steady state (Cost Explorer, read-only)")
        r.section("1. by service, per day")
        by_svc = ce_daily(ce, start, end, "SERVICE")
        days = sorted(by_svc)
        for d in days:
            tot = sum(by_svc[d].values())
            top = sorted(by_svc[d].items(), key=lambda kv: -kv[1])[:6]
            r.log("%s  $%7.2f  %s" % (d, tot, ", ".join("%s %.2f" % (k.replace("Amazon ", "").replace("AWS ", ""), v) for k, v in top)))
        r.section("2. S3, CloudWatch and Lambda by usage type on the thawed days")
        s3f = {"Dimensions": {"Key": "SERVICE", "Values": ["Amazon Simple Storage Service"]}}
        cwf = {"Dimensions": {"Key": "SERVICE", "Values": ["AmazonCloudWatch"]}}
        lmf = {"Dimensions": {"Key": "SERVICE", "Values": ["AWS Lambda"]}}
        s3u = ce_daily(ce, THAW_DAY, end, "USAGE_TYPE", s3f)
        cwu = ce_daily(ce, THAW_DAY, end, "USAGE_TYPE", cwf)
        lmu = ce_daily(ce, THAW_DAY, end, "USAGE_TYPE", lmf)
        for name, u in (("S3", s3u), ("CloudWatch", cwu), ("Lambda", lmu)):
            for d in sorted(u):
                top = sorted(u[d].items(), key=lambda kv: -kv[1])[:7]
                r.log("%s %s: $%.2f = %s" % (name, d, sum(u[d].values()), ", ".join("%s %.2f" % (k.replace("USE1-", ""), v) for k, v in top if v >= 0.005)))
        r.section("3. the projection")
        rows = []
        for d in days:
            if d < THAW_DAY:
                continue
            svc = by_svc[d]
            total = sum(svc.values())
            support = svc.get(SUPPORT, 0.0)
            one_off = sum(v for k, v in s3u.get(d, {}).items() if any(x in k for x in ONE_OFF_USAGE))
            egress = sum(v for k, v in s3u.get(d, {}).items() if "DataTransfer-Out" in k)
            storage = sum(v for k, v in s3u.get(d, {}).items() if "TimedStorage" in k)
            recurring = total - support - one_off
            rows.append({"day": d, "total": round(total, 2), "support": round(support, 2), "one_off_backfill": round(one_off, 2), "recurring": round(recurring, 2),
                         "lambda": round(svc.get("AWS Lambda", 0.0), 2), "cloudwatch": round(svc.get("AmazonCloudWatch", 0.0), 2), "s3_egress": round(egress, 2), "s3_storage": round(storage, 2),
                         "s3_other": round(svc.get("Amazon Simple Storage Service", 0.0) - one_off - egress - storage, 2),
                         "rest": round(recurring - svc.get("AWS Lambda", 0.0) - svc.get("AmazonCloudWatch", 0.0) - (svc.get("Amazon Simple Storage Service", 0.0) - one_off), 2)})
        for x in rows:
            r.log(json.dumps(x))
        # the last Cost Explorer day is often partial; use complete days only (all but the newest) when there are >= 2
        complete = rows[:-1] if len(rows) >= 2 else rows
        rec = [x["recurring"] for x in complete]
        med = statistics.median(rec) if rec else 0.0
        lo, hi = (min(rec), max(rec)) if rec else (0.0, 0.0)
        # fixed monthly items
        mtd = ce_daily(ce, today.replace(day=1).isoformat(), end, "SERVICE")
        support_mtd = sum(v.get(SUPPORT, 0.0) for v in mtd.values())
        archive_gb = 1032.0
        archive_month = archive_gb * 0.0125                       # Standard-IA us-east-1 list price per GB-month
        replication_puts_month = 30 * 0.15                        # ~15k daily replicated writes x $0.01/1k
        support_month = 100.0 if support_mtd > 0 else 0.0         # Business Support: the greater of $100 and 10% of usage
        proj_mid = med * 30 + archive_month + replication_puts_month + support_month
        proj_lo = lo * 30 + archive_month + replication_puts_month + support_month
        proj_hi = hi * 30 + archive_month + replication_puts_month + support_month
        r.log("recurring $/day on the complete thawed days %s: median %.2f, low %.2f, high %.2f" % ([x["day"] for x in complete], med, lo, hi))
        r.log("fixed monthly: Business Support %s (month-to-date charged $%.2f; the plan bills the greater of $100 and 10%% of usage), archive Standard-IA %.0f GB x $0.0125 = $%.2f, replication PUTs ~$%.2f" % (
            "$%.0f" % support_month if support_month else "none", support_mtd, archive_gb, archive_month, replication_puts_month))
        r.log("projection: ~$%.0f/month (range $%.0f - $%.0f); without Business Support ~$%.0f/month" % (proj_mid, proj_lo, proj_hi, proj_mid - support_month))
        mtd_total = sum(sum(v.values()) for v in mtd.values())
        r.log("October month-to-date (Cost Explorer, through %s): $%.2f - carries the Oct 12-16 loop" % (end, mtd_total))
        r.kv(recurring_median_per_day=round(med, 2), recurring_low=round(lo, 2), recurring_high=round(hi, 2), projection_month=round(proj_mid), projection_low=round(proj_lo), projection_high=round(proj_hi),
             support_month=support_month, archive_month=round(archive_month, 2), october_mtd=round(mtd_total, 2))
        r.ok("read-only; four Cost Explorer calls")


if __name__ == "__main__":
    main()
