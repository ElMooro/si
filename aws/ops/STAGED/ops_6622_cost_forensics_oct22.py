"""ops 6622 - READ-ONLY: why did AWS bill ~$30 on 2026-10-22 against a ~$6.50/day plan? (Khalid 2026-10-23)

Khalid: "My monthly aws is supposed to be $200. Yesterday alone aws consumed $30 which put the budget at $1000 a
month - investigate."

This op mutates nothing. It reads, from the runner:
  1. Cost Explorer DAILY 2026-10-01..today by SERVICE and by RECORD_TYPE (unblended + net unblended),
  2. DAILY 2026-10-10..today by SERVICE x USAGE_TYPE (every line >= $0.001, with usage quantities),
  3. S3 only, DAILY 2026-10-18..today by USAGE_TYPE x OPERATION,
  4. HOURLY by SERVICE for 2026-10-21..now (only if hourly data is enabled; otherwise noted),
  5. S3 cost by RESOURCE_ID (bucket) for the last 14 days (only if resource-level data is enabled; otherwise noted),
  6. MONTHLY Aug..Oct by SERVICE for context, and the Cost Explorer forecast for the rest of October and November,
  7. S3 Batch Operations jobs of the last 10 days (timing and task counts of the archive backfills),
  8. every AWS Budget with its notifications and, for daily budgets, the last 10 days of performance history,
  9. the live bucket's replication rule and the archive bucket's lifecycle (read-only gets).
About 14 Cost Explorer requests ($0.14). The raw numbers go to reports/latest/ops_6622_cost_forensics_oct22_data.md
(one JSON block) so the attribution can be recomputed off the runner.
"""
from datetime import datetime, timezone, timedelta, date
import json
import os
import sys
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

CFG = Config(retries={"max_attempts": 5, "mode": "standard"}, connect_timeout=10, read_timeout=90)
LIVE = "justhodl-dashboard-live"
ARCHIVE = "justhodl-warehouse-archive-857687956942"
S3_SERVICE = "Amazon Simple Storage Service"
NAME = "ops_6622_cost_forensics_oct22"


def ce_paged(ce, **kw):
    """get_cost_and_usage with NextPageToken handling; returns the merged ResultsByTime list."""
    out, token = [], None
    while True:
        if token:
            kw["NextPageToken"] = token
        res = ce.get_cost_and_usage(**kw)
        out.extend(res.get("ResultsByTime", []))
        token = res.get("NextPageToken")
        if not token:
            return out


def flat(results, metrics):
    """[{day, keys:[...], metric: value}] for grouped results, plus the per-period Total when ungrouped."""
    rows = []
    for period in results:
        start = period["TimePeriod"]["Start"]
        groups = period.get("Groups") or []
        if not groups and period.get("Total"):
            rows.append(dict({"t": start, "keys": ["TOTAL"]},
                             **{m: round(float(period["Total"][m]["Amount"]), 6) for m in metrics if m in period["Total"]}))
        for g in groups:
            row = {"t": start, "keys": g["Keys"]}
            for m in metrics:
                if m in g["Metrics"]:
                    row[m] = round(float(g["Metrics"][m]["Amount"]), 6)
            rows.append(row)
    return rows


def main():
    now = datetime.now(timezone.utc)
    today = now.date()
    tomorrow = today + timedelta(days=1)
    data = {"generated_at": now.isoformat(), "notes": []}
    with report(NAME) as r:
        r.heading("ops 6622 - AWS cost forensics for 2026-10-22 (read-only), %s" % now.strftime("%Y-%m-%d %H:%M UTC"))
        ce = boto3.client("ce", region_name="us-east-1", config=CFG)
        account = boto3.client("sts", config=CFG).get_caller_identity()["Account"]
        data["account"] = account

        # 1. daily by service and by record type
        r.section("1. daily totals by service and by record type, Oct 1 - today")
        try:
            svc = ce_paged(ce, TimePeriod={"Start": "2026-10-01", "End": tomorrow.isoformat()}, Granularity="DAILY",
                           Metrics=["UnblendedCost", "NetUnblendedCost"], GroupBy=[{"Type": "DIMENSION", "Key": "SERVICE"}])
            data["daily_service"] = flat(svc, ["UnblendedCost", "NetUnblendedCost"])
            rec = ce_paged(ce, TimePeriod={"Start": "2026-10-01", "End": tomorrow.isoformat()}, Granularity="DAILY",
                           Metrics=["UnblendedCost", "NetUnblendedCost"], GroupBy=[{"Type": "DIMENSION", "Key": "RECORD_TYPE"}])
            data["daily_record_type"] = flat(rec, ["UnblendedCost", "NetUnblendedCost"])
        except Exception as ex:  # noqa: BLE001
            r.fail("Cost Explorer daily read failed: %s %s" % (type(ex).__name__, str(ex)[:300]))
            sys.exit(1)
        by_day = {}
        for row in data["daily_service"]:
            d = by_day.setdefault(row["t"], [0.0, 0.0])
            d[0] += row.get("UnblendedCost", 0.0)
            d[1] += row.get("NetUnblendedCost", 0.0)
        for day in sorted(by_day):
            top = sorted((x for x in data["daily_service"] if x["t"] == day), key=lambda x: -x.get("UnblendedCost", 0))[:5]
            r.log("%s  unblended $%8.2f  net $%8.2f   top: %s" % (day, by_day[day][0], by_day[day][1],
                  "; ".join("%s $%.2f" % (x["keys"][0], x.get("UnblendedCost", 0)) for x in top if x.get("UnblendedCost", 0) >= 0.01)))
        mtd_unb = sum(v[0] for v in by_day.values())
        mtd_net = sum(v[1] for v in by_day.values())
        r.log("MTD unblended $%.2f, net unblended $%.2f" % (mtd_unb, mtd_net))
        rt = {}
        for row in data["daily_record_type"]:
            k = row["keys"][0]
            rt[k] = round(rt.get(k, 0.0) + row.get("UnblendedCost", 0.0), 2)
        r.log("MTD by record type (unblended): %s" % json.dumps(rt))
        for row in data["daily_record_type"]:
            if row["keys"][0] != "Usage" and abs(row.get("UnblendedCost", 0.0)) >= 0.01:
                r.log("  non-usage record %s on %s: $%.2f" % (row["keys"][0], row["t"], row["UnblendedCost"]))

        # 2. service x usage type, Oct 10 - today
        r.section("2. every line item by service x usage type, Oct 10 - today")
        try:
            sut = ce_paged(ce, TimePeriod={"Start": "2026-10-10", "End": tomorrow.isoformat()}, Granularity="DAILY",
                           Metrics=["UnblendedCost", "UsageQuantity"],
                           GroupBy=[{"Type": "DIMENSION", "Key": "SERVICE"}, {"Type": "DIMENSION", "Key": "USAGE_TYPE"}])
            data["daily_service_usage_type"] = [x for x in flat(sut, ["UnblendedCost", "UsageQuantity"]) if abs(x.get("UnblendedCost", 0)) >= 0.001]
        except Exception as ex:  # noqa: BLE001
            r.warn("service x usage type read failed: %s %s" % (type(ex).__name__, str(ex)[:300]))
            data["daily_service_usage_type"] = []
        for day in ("2026-10-20", "2026-10-21", "2026-10-22", today.isoformat()):
            items = sorted((x for x in data["daily_service_usage_type"] if x["t"] == day), key=lambda x: -x["UnblendedCost"])
            r.log("%s: %d lines, $%.2f; top: %s" % (day, len(items), sum(x["UnblendedCost"] for x in items),
                  "; ".join("%s/%s $%.2f (qty %.0f)" % (x["keys"][0][:22], x["keys"][1], x["UnblendedCost"], x.get("UsageQuantity", 0)) for x in items[:12])))

        # 3. S3 by usage type x operation
        r.section("3. S3 by usage type x operation, Oct 18 - today")
        try:
            s3o = ce_paged(ce, TimePeriod={"Start": "2026-10-18", "End": tomorrow.isoformat()}, Granularity="DAILY",
                           Metrics=["UnblendedCost", "UsageQuantity"],
                           Filter={"Dimensions": {"Key": "SERVICE", "Values": [S3_SERVICE]}},
                           GroupBy=[{"Type": "DIMENSION", "Key": "USAGE_TYPE"}, {"Type": "DIMENSION", "Key": "OPERATION"}])
            data["s3_usage_operation"] = [x for x in flat(s3o, ["UnblendedCost", "UsageQuantity"]) if abs(x.get("UnblendedCost", 0)) >= 0.001]
            for day in ("2026-10-21", "2026-10-22", today.isoformat()):
                items = sorted((x for x in data["s3_usage_operation"] if x["t"] == day), key=lambda x: -x["UnblendedCost"])
                r.log("%s S3: %s" % (day, "; ".join("%s|%s $%.2f (qty %.0f)" % (x["keys"][0], x["keys"][1], x["UnblendedCost"], x.get("UsageQuantity", 0)) for x in items[:10])))
        except Exception as ex:  # noqa: BLE001
            r.warn("S3 usage x operation read failed: %s %s" % (type(ex).__name__, str(ex)[:300]))

        # 4. hourly (only if enabled)
        r.section("4. hourly by service, Oct 21 - now (needs hourly granularity enabled)")
        try:
            end_h = now.replace(minute=0, second=0, microsecond=0)
            hr = ce_paged(ce, TimePeriod={"Start": "2026-10-21T00:00:00Z", "End": end_h.strftime("%Y-%m-%dT%H:%M:%SZ")},
                          Granularity="HOURLY", Metrics=["UnblendedCost"], GroupBy=[{"Type": "DIMENSION", "Key": "SERVICE"}])
            data["hourly_service"] = [x for x in flat(hr, ["UnblendedCost"]) if abs(x.get("UnblendedCost", 0)) >= 0.001]
            r.log("hourly rows: %d" % len(data["hourly_service"]))
        except Exception as ex:  # noqa: BLE001
            data["notes"].append("hourly: %s %s" % (type(ex).__name__, str(ex)[:200]))
            r.log("hourly granularity not available: %s %s" % (type(ex).__name__, str(ex)[:200]))

        # 5. S3 by bucket (only if resource-level data is enabled)
        r.section("5. S3 cost by bucket, last 14 days (needs resource-level data enabled)")
        try:
            start_r = (today - timedelta(days=13)).isoformat()
            out, token = [], None
            while True:
                kw = dict(TimePeriod={"Start": start_r, "End": tomorrow.isoformat()}, Granularity="DAILY",
                          Metrics=["UnblendedCost", "UsageQuantity"],
                          Filter={"Dimensions": {"Key": "SERVICE", "Values": [S3_SERVICE]}},
                          GroupBy=[{"Type": "DIMENSION", "Key": "RESOURCE_ID"}])
                if token:
                    kw["NextPageToken"] = token
                res = ce.get_cost_and_usage_with_resources(**kw)
                out.extend(res.get("ResultsByTime", []))
                token = res.get("NextPageToken")
                if not token:
                    break
            data["s3_by_bucket"] = [x for x in flat(out, ["UnblendedCost", "UsageQuantity"]) if abs(x.get("UnblendedCost", 0)) >= 0.001]
            for day in ("2026-10-21", "2026-10-22", today.isoformat()):
                items = sorted((x for x in data["s3_by_bucket"] if x["t"] == day), key=lambda x: -x["UnblendedCost"])
                r.log("%s by bucket: %s" % (day, "; ".join("%s $%.2f" % (x["keys"][0], x["UnblendedCost"]) for x in items[:8])))
        except Exception as ex:  # noqa: BLE001
            data["notes"].append("by-bucket: %s %s" % (type(ex).__name__, str(ex)[:200]))
            r.log("resource-level S3 data not available: %s %s" % (type(ex).__name__, str(ex)[:200]))

        # 6. monthly context + forecasts
        r.section("6. monthly totals Aug-Oct and the Cost Explorer forecast")
        try:
            mon = ce_paged(ce, TimePeriod={"Start": "2026-08-01", "End": "2026-11-01"}, Granularity="MONTHLY",
                           Metrics=["UnblendedCost", "NetUnblendedCost"], GroupBy=[{"Type": "DIMENSION", "Key": "SERVICE"}])
            data["monthly_service"] = [x for x in flat(mon, ["UnblendedCost", "NetUnblendedCost"]) if abs(x.get("UnblendedCost", 0)) >= 0.01]
            for m in ("2026-08-01", "2026-09-01", "2026-10-01"):
                items = [x for x in data["monthly_service"] if x["t"] == m]
                r.log("%s: unblended $%.2f net $%.2f" % (m[:7], sum(x["UnblendedCost"] for x in items), sum(x.get("NetUnblendedCost", 0) for x in items)))
        except Exception as ex:  # noqa: BLE001
            r.warn("monthly read failed: %s %s" % (type(ex).__name__, str(ex)[:200]))
        data["forecast"] = {}
        for label, start, end in (("rest_of_october", tomorrow.isoformat(), "2026-11-01"), ("november", "2026-11-01", "2026-12-01")):
            try:
                fc = ce.get_cost_forecast(TimePeriod={"Start": start, "End": end}, Metric="UNBLENDED_COST", Granularity="MONTHLY")
                data["forecast"][label] = round(float(fc["Total"]["Amount"]), 2)
                r.log("CE forecast %s: $%.2f" % (label, data["forecast"][label]))
            except Exception as ex:  # noqa: BLE001
                data["forecast"][label] = "%s %s" % (type(ex).__name__, str(ex)[:160])
                r.log("CE forecast %s unavailable: %s" % (label, data["forecast"][label]))

        # 7. S3 Batch Operations jobs
        r.section("7. S3 Batch Operations jobs, last 10 days")
        try:
            s3c = boto3.client("s3control", region_name="us-east-1", config=CFG)
            jobs, token = [], None
            while True:
                kw = dict(AccountId=account, MaxResults=100)
                if token:
                    kw["NextToken"] = token
                res = s3c.list_jobs(**kw)
                jobs.extend(res.get("Jobs", []))
                token = res.get("NextToken")
                if not token:
                    break
            cutoff = now - timedelta(days=10)
            data["batch_jobs"] = []
            for j in jobs:
                if j.get("CreationTime") and j["CreationTime"] >= cutoff:
                    ps = j.get("ProgressSummary") or {}
                    rowj = {"id": j.get("JobId"), "op": j.get("Operation"), "status": j.get("Status"),
                            "created": j["CreationTime"].isoformat(),
                            "ended": j["TerminationDate"].isoformat() if j.get("TerminationDate") else None,
                            "tasks": ps.get("TotalNumberOfTasks"), "ok": ps.get("NumberOfTasksSucceeded"),
                            "failed": ps.get("NumberOfTasksFailed"),
                            "active_s": (ps.get("Timers") or {}).get("ElapsedTimeInActiveSeconds"),
                            "desc": (j.get("Description") or "")[:120]}
                    data["batch_jobs"].append(rowj)
                    r.log("job %s %s %s created %s ended %s tasks %s ok %s failed %s" % (rowj["id"][:8], rowj["op"], rowj["status"], rowj["created"][:16], (rowj["ended"] or "-")[:16], rowj["tasks"], rowj["ok"], rowj["failed"]))
        except Exception as ex:  # noqa: BLE001
            r.warn("batch job listing failed: %s %s" % (type(ex).__name__, str(ex)[:200]))

        # 8. budgets
        r.section("8. AWS Budgets, notifications, daily performance")
        try:
            bud = boto3.client("budgets", region_name="us-east-1", config=CFG)
            budgets = bud.describe_budgets(AccountId=account, MaxResults=100).get("Budgets", [])
            data["budgets"] = []
            for b in budgets:
                name = b["BudgetName"]
                cs = b.get("CalculatedSpend") or {}
                entry = {"name": name, "unit": b.get("TimeUnit"), "type": b.get("BudgetType"),
                         "limit": (b.get("BudgetLimit") or {}).get("Amount"),
                         "actual": (cs.get("ActualSpend") or {}).get("Amount"),
                         "forecast": (cs.get("ForecastedSpend") or {}).get("Amount"),
                         "cost_types": b.get("CostTypes"), "notifications": []}
                try:
                    for n in bud.describe_notifications_for_budget(AccountId=account, BudgetName=name, MaxResults=100).get("Notifications", []):
                        entry["notifications"].append({k: n.get(k) for k in ("NotificationType", "ComparisonOperator", "Threshold", "ThresholdType", "NotificationState")})
                except Exception as ex:  # noqa: BLE001
                    entry["notifications"] = "%s" % type(ex).__name__
                if b.get("TimeUnit") == "DAILY":
                    try:
                        hist = bud.describe_budget_performance_history(
                            AccountId=account, BudgetName=name,
                            TimePeriod={"Start": datetime.combine(today - timedelta(days=10), datetime.min.time()),
                                        "End": datetime.combine(tomorrow, datetime.min.time())})
                        entry["history"] = [{"start": h["TimePeriod"]["Start"].isoformat()[:10],
                                             "actual": (h.get("ActualAmount") or {}).get("Amount"),
                                             "budgeted": (h.get("BudgetedAmount") or {}).get("Amount")}
                                            for h in (hist.get("BudgetPerformanceHistory") or {}).get("BudgetedAndActualAmountsList", [])]
                    except Exception as ex:  # noqa: BLE001
                        entry["history"] = "%s %s" % (type(ex).__name__, str(ex)[:120])
                data["budgets"].append(entry)
                r.log("budget %s (%s): limit $%s actual $%s forecast $%s; notifications %s" % (
                    name, entry["unit"], entry["limit"], entry["actual"], entry["forecast"],
                    json.dumps(entry["notifications"])[:400]))
                if isinstance(entry.get("history"), list):
                    r.log("  daily history: %s" % json.dumps(entry["history"]))
        except Exception as ex:  # noqa: BLE001
            r.warn("budgets read failed: %s %s" % (type(ex).__name__, str(ex)[:200]))

        # 9. replication + archive lifecycle (read-only)
        r.section("9. live-bucket replication rule and archive lifecycle")
        s3 = boto3.client("s3", region_name="us-east-1", config=CFG)
        try:
            rep = s3.get_bucket_replication(Bucket=LIVE)["ReplicationConfiguration"]
            data["live_replication"] = [{k: rule.get(k) for k in ("ID", "Status", "Priority", "Filter", "DeleteMarkerReplication")} |
                                        {"dest_bucket": rule["Destination"].get("Bucket"), "dest_class": rule["Destination"].get("StorageClass"),
                                         "rtc": rule["Destination"].get("ReplicationTime"), "metrics": rule["Destination"].get("Metrics")}
                                        for rule in rep.get("Rules", [])]
            r.log("live replication: %s" % json.dumps(data["live_replication"], default=str))
        except Exception as ex:  # noqa: BLE001
            r.log("live replication read: %s %s" % (type(ex).__name__, str(ex)[:200]))
        for b in (ARCHIVE, LIVE):
            try:
                lc = s3.get_bucket_lifecycle_configuration(Bucket=b).get("Rules", [])
                data.setdefault("lifecycle", {})[b] = lc
                r.log("%s lifecycle: %s" % (b, json.dumps([{k: x.get(k) for k in ("ID", "Status", "NoncurrentVersionExpiration", "Expiration", "Transitions", "NoncurrentVersionTransitions")} for x in lc], default=str)[:900]))
            except Exception as ex:  # noqa: BLE001
                r.log("%s lifecycle read: %s %s" % (b, type(ex).__name__, str(ex)[:200]))

        # raw data file for off-runner recomputation
        latest = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd())) / "aws" / "ops" / "reports" / "latest"
        latest.mkdir(parents=True, exist_ok=True)
        (latest / (NAME + "_data.md")).write_text(
            "# ops 6622 raw data (JSON)\n\n```json\n" + json.dumps(data, default=str, separators=(",", ":")) + "\n```\n", encoding="utf-8")
        r.kv(mtd_unblended=round(mtd_unb, 2), mtd_net=round(mtd_net, 2),
             oct22_unblended=round(by_day.get("2026-10-22", [0, 0])[0], 2),
             oct21_unblended=round(by_day.get("2026-10-21", [0, 0])[0], 2),
             lines=len(data.get("daily_service_usage_type", [])), hourly=bool(data.get("hourly_service")),
             by_bucket=bool(data.get("s3_by_bucket")))
        r.ok("read-only; nothing changed (Cost Explorer ~14 requests)")
    sys.exit(0)


if __name__ == "__main__":
    main()
