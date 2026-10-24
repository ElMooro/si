"""ops 6642 - the AWS Cost Anomaly Detection e-mail of 2026-10-23 (Khalid 2026-10-24: "Investigate this and fix it").

The e-mail: Amazon S3, start 2026-10-21, last detected 2026-10-22, 2 days, max daily impact $20.55, total $38.88; root
causes Requests-SIA-Tier1 ($21.83) and BatchOperations-Objects ($2.09) (+1 more). Those are the usage types of the two
archive backfills Khalid approved (S3 Batch Replication into the Standard-IA archive): job 523b902a on Oct 21 (1.28 M
objects, ops 6583) and job 646ccb69 on Oct 22 (2,086,159 objects, ops 6612). This op proves or refutes that, and that
it is over:
  A. Cost Anomaly Detection: every anomaly since Oct 15 with its dates, impact, root causes and feedback;
  B. Cost Explorer: S3 per day by usage type (cost and quantity) Oct 19 -> today, the account total per day - Oct 23
     must be back to the archive's daily copying (~74 k Standard-IA PUTs, ~$0.74) with no Batch Operations;
  C. S3 Batch Operations: no job may be queued, running or paused; the backfill jobs are listed with their counts;
  D. the replication configuration is still the single whole-bucket rule (no test rule left behind).
Fix: when all four agree that the anomaly was the finished backfill (its root causes are only backfill usage types,
no job is active, Oct 23 is back to normal), record AWS feedback PLANNED_ACTIVITY on that anomaly - the anomaly model
learns it was intended, and the record shows it was reviewed. Nothing else is changed.
"""
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import json
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))

REGION, ACCOUNT = "us-east-1", "857687956942"
LIVE = "justhodl-dashboard-live"
BACKFILL_TYPES = {"Requests-SIA-Tier1", "BatchOperations-Objects", "BatchOperations-Jobs", "Retrieval-SIA"}
BACKFILL_JOBS = {"523b902a-5e7f-4ade-bf38-8c92eddcc947": "backfill #1 (ops 6583)", "646ccb69": "backfill #2 (ops 6612)"}
ACTIVE = ["Active", "New", "Preparing", "Ready", "Suspended", "Pausing", "Paused", "Cancelling", "Completing", "Failing"]
WATCH = ["Requests-SIA-Tier1", "BatchOperations-Objects", "BatchOperations-Jobs", "Retrieval-SIA", "EarlyDelete-SIA",
         "TimedStorage-SIA-ByteHrs", "TimedStorage-ByteHrs", "Requests-Tier1", "Requests-Tier2", "DataTransfer-Out-Bytes"]
NORMAL_SIA_PUTS = 150000          # a normal day copies ~74 k objects; a backfill day copies millions


def is_backfill_anomaly(an):
    """An S3 anomaly whose every root cause is a usage type the backfills used, starting Oct 21-22."""
    causes = an.get("RootCauses") or []
    types = {c.get("UsageType") for c in causes if c.get("UsageType")}
    svc = {c.get("Service") for c in causes if c.get("Service")}
    start = (an.get("AnomalyStartDate") or "")[:10]
    return bool(types) and types <= BACKFILL_TYPES and svc <= {"Amazon Simple Storage Service"} and start in ("2026-10-21", "2026-10-22")


def selftest():
    an = {"AnomalyStartDate": "2026-10-21T00:00:00Z", "RootCauses": [
        {"Service": "Amazon Simple Storage Service", "UsageType": "BatchOperations-Objects"},
        {"Service": "Amazon Simple Storage Service", "UsageType": "Requests-SIA-Tier1"},
        {"Service": "Amazon Simple Storage Service", "UsageType": "Retrieval-SIA"}]}
    assert is_backfill_anomaly(an)
    assert not is_backfill_anomaly(dict(an, RootCauses=an["RootCauses"] + [{"Service": "Amazon Simple Storage Service", "UsageType": "DataTransfer-Out-Bytes"}]))
    assert not is_backfill_anomaly(dict(an, AnomalyStartDate="2026-10-12T00:00:00Z"))
    assert not is_backfill_anomaly(dict(an, RootCauses=[]))
    print("selftest ok")


def main():
    import boto3
    from botocore.config import Config
    from botocore.exceptions import ClientError
    from ops_report import report

    selftest()
    cfg = Config(retries={"max_attempts": 8, "mode": "standard"}, connect_timeout=10, read_timeout=60)
    ce = boto3.client("ce", region_name="us-east-1", config=cfg)
    today = datetime.now(timezone.utc).date()
    with report("ops_6642_s3_anomaly_backfill_check") as r:
        r.heading("ops 6642 - the S3 cost anomaly of Oct 21-22: cause, is it over, AWS feedback")
        problems, facts = [], {}

        r.section("A. Cost Anomaly Detection - anomalies since Oct 15")
        anomalies = []
        try:
            kw = {"DateInterval": {"StartDate": "2026-10-15", "EndDate": today.isoformat()}, "MaxResults": 100}
            while True:
                res = ce.get_anomalies(**kw)
                anomalies += res.get("Anomalies", [])
                if not res.get("NextPageToken"):
                    break
                kw["NextPageToken"] = res["NextPageToken"]
        except ClientError as ex:
            problems.append("get_anomalies: %s" % ex.response["Error"]["Code"])
        for an in anomalies:
            imp = an.get("Impact") or {}
            causes = ["%s/%s $%.2f" % (c.get("Service", "?")[:20], c.get("UsageType", "?"),
                                       float(((c.get("Impact") or {}).get("Contribution")) or 0)) for c in (an.get("RootCauses") or [])]
            r.log("%s %s -> %s | max $%.2f/day, total $%.2f (actual $%s vs expected $%s) | feedback %s | %s | %s" % (
                an["AnomalyId"][:12], (an.get("AnomalyStartDate") or "")[:10], (an.get("AnomalyEndDate") or "open")[:10],
                float(imp.get("MaxImpact") or 0), float(imp.get("TotalImpact") or 0), imp.get("TotalActualSpend"), imp.get("TotalExpectedSpend"),
                an.get("Feedback"), an.get("DimensionValue"), "; ".join(causes) or "no root causes"))
        target = [an for an in anomalies if is_backfill_anomaly(an)]
        others = [an for an in anomalies if an not in target and (an.get("AnomalyStartDate") or "") >= "2026-10-17"]
        facts["anomalies"] = len(anomalies)
        facts["backfill_anomalies"] = [an["AnomalyId"] for an in target]
        if others:
            for an in others:
                r.warn("another anomaly since Oct 17, NOT the backfill: %s %s %s" % (an["AnomalyId"][:12], an.get("AnomalyStartDate"), an.get("DimensionValue")))

        r.section("B. Cost Explorer - S3 by usage type per day, account total per day")
        start, end = date(2026, 10, 19), today + timedelta(days=1)
        by_day = {}
        try:
            res = ce.get_cost_and_usage(TimePeriod={"Start": start.isoformat(), "End": end.isoformat()}, Granularity="DAILY",
                                        Metrics=["UnblendedCost", "UsageQuantity"],
                                        Filter={"Dimensions": {"Key": "SERVICE", "Values": ["Amazon Simple Storage Service"]}},
                                        GroupBy=[{"Type": "DIMENSION", "Key": "USAGE_TYPE"}])
            for day in res["ResultsByTime"]:
                d = day["TimePeriod"]["Start"]
                rows = {g["Keys"][0]: (float(g["Metrics"]["UnblendedCost"]["Amount"]), float(g["Metrics"]["UsageQuantity"]["Amount"])) for g in day["Groups"]}
                by_day[d] = rows
                s3_total = sum(v[0] for v in rows.values())
                parts = ", ".join("%s $%.2f (%.0f)" % (k, rows[k][0], rows[k][1]) for k in WATCH if k in rows and rows[k][0] >= 0.005)
                r.log("%s%s S3 $%6.2f | %s" % (d, " (partial)" if d == today.isoformat() else "", s3_total, parts))
            tot = ce.get_cost_and_usage(TimePeriod={"Start": start.isoformat(), "End": end.isoformat()}, Granularity="DAILY",
                                        Metrics=["UnblendedCost"], Filter={"Not": {"Dimensions": {"Key": "RECORD_TYPE", "Values": ["Credit", "Refund"]}}})
            for day in tot["ResultsByTime"]:
                r.log("%s account total (usage, before credits) $%.2f%s" % (day["TimePeriod"]["Start"], float(day["Total"]["UnblendedCost"]["Amount"]),
                                                                       " (estimated)" if day.get("Estimated") else ""))
        except ClientError as ex:
            problems.append("cost explorer: %s" % ex.response["Error"]["Code"])
        oct23 = by_day.get("2026-10-23", {})
        sia23 = oct23.get("Requests-SIA-Tier1", (0.0, 0.0))
        bo23 = sum(oct23.get(k, (0.0, 0.0))[0] for k in ("BatchOperations-Objects", "BatchOperations-Jobs"))
        facts["oct23_sia_puts"] = round(sia23[1])
        facts["oct23_sia_put_usd"] = round(sia23[0], 2)
        facts["oct23_batch_ops_usd"] = round(bo23, 2)
        normal23 = bool(oct23) and sia23[1] < NORMAL_SIA_PUTS and bo23 < 0.01
        r.log("Oct 23: %d Standard-IA PUTs ($%.2f), Batch Operations $%.2f -> %s" % (sia23[1], sia23[0], bo23,
              "back to the archive's daily copying" if normal23 else ("NOT normal" if oct23 else "not in Cost Explorer yet")))

        r.section("C. S3 Batch Operations jobs")
        s3c = boto3.client("s3control", region_name=REGION, config=cfg)
        active = []
        try:
            jobs, kw = [], {"AccountId": ACCOUNT, "MaxResults": 100}
            while True:
                res = s3c.list_jobs(**kw)
                jobs += res.get("Jobs", [])
                if not res.get("NextToken"):
                    break
                kw["NextToken"] = res["NextToken"]
            for j in sorted(jobs, key=lambda x: str(x.get("CreationTime")), reverse=True)[:10]:
                p = j.get("ProgressSummary") or {}
                label = next((v for k, v in BACKFILL_JOBS.items() if j["JobId"].startswith(k)), "")
                r.log("job %s %s created %s: %s succeeded %s / %s, failed %s %s" % (j["JobId"][:8], j.get("Operation"), str(j.get("CreationTime"))[:16],
                      j.get("Status"), p.get("NumberOfTasksSucceeded"), p.get("TotalNumberOfTasks"), p.get("NumberOfTasksFailed"), label))
            active = [j for j in jobs if j.get("Status") in ACTIVE]
            facts["batch_jobs_total"] = len(jobs)
            facts["batch_jobs_active"] = len(active)
            if active:
                problems.append("Batch Operations job(s) still active: %s" % [(j["JobId"][:8], j["Status"]) for j in active])
        except ClientError as ex:
            problems.append("list_jobs: %s" % ex.response["Error"]["Code"])

        r.section("D. replication configuration")
        s3 = boto3.client("s3", region_name=REGION, config=cfg)
        rules = s3.get_bucket_replication(Bucket=LIVE)["ReplicationConfiguration"]["Rules"]
        shape = [(x.get("ID"), x.get("Status"), (x.get("Filter") or {}).get("Prefix"), x["Destination"].get("StorageClass")) for x in rules]
        r.log("rules: %s" % shape)
        if shape != [("warehouse-archive-all", "Enabled", "", "STANDARD_IA")]:
            problems.append("replication configuration is not the single whole-bucket rule: %s" % shape)

        r.section("E. fix: AWS feedback on the backfill anomaly")
        if not target:
            r.warn("no anomaly matched the backfill signature - nothing marked")
        elif active or not normal23:
            r.warn("not marking: a job is active or Oct 23 is not back to normal - this needs a closer look first")
        else:
            for an in target:
                if an.get("Feedback") == "PLANNED_ACTIVITY":
                    r.log("%s already marked PLANNED_ACTIVITY" % an["AnomalyId"][:12])
                    continue
                try:
                    ce.provide_anomaly_feedback(AnomalyId=an["AnomalyId"], Feedback="PLANNED_ACTIVITY")
                    r.ok("%s (S3, %s, total $%.2f) marked PLANNED_ACTIVITY: the approved archive backfills" % (
                        an["AnomalyId"][:12], (an.get("AnomalyStartDate") or "")[:10], float((an.get("Impact") or {}).get("TotalImpact") or 0)))
                    facts.setdefault("marked", []).append(an["AnomalyId"])
                except ClientError as ex:
                    r.warn("feedback refused: %s - the record stays unmarked (harmless)" % ex.response["Error"]["Code"])

        r.kv(**{k: (json.dumps(v) if isinstance(v, list) else v) for k, v in facts.items()}, problems=len(problems))
        if problems:
            for p in problems:
                r.warn(p)
            r.fail("%d problem(s)" % len(problems))
            sys.exit(1)
        r.ok("the anomaly is the two approved archive backfills (Oct 21-22); nothing is still running")
    sys.exit(0)


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        main()
