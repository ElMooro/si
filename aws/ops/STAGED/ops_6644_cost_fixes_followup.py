"""ops 6644 - READ-ONLY follow-up of the 2026-10-23 cost fixes (scheduled check, Khalid's "$30 day" investigation).

  A. Cost Explorer, Oct 21 -> today: account total per day, S3 and CloudWatch by usage type - did direct S3 egress
     (DataTransfer-Out-Bytes) fall after the Applebot deny (ops 6626, Oct 23 18:10 UTC) and the crawler gate (ops 6628 +
     Worker), is CW:MetricInsightAlarmUsage gone after ops 6627 (Oct 23 ~18:12 UTC), did the archive's Standard-IA PUTs
     (Requests-SIA-Tier1) give way to Intelligent-Tiering PUTs (Requests-INT-Tier1) after ops 6637 (Oct 24 14:48 UTC),
     and has EarlyDelete-SIA (30-day minimum on replaced archive copies) appeared;
  B. S3 server access logs of Oct 24 so far (sampled, scaled to a day): billed GB by requester class (direct = anonymous
     non-Cloudflare, cloudflare = the Worker's origin fetches, runner), and every Applebot request (17.x / UA) by status.
Nothing is written but this report.
"""
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import concurrent.futures as cf
import re
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))

REGION, ACCOUNT = "us-east-1", "857687956942"
LIVE = "justhodl-dashboard-live"
LOG_RE = re.compile(r'^(\S+) (\S+) \[([^\]]+)\] (\S+) (\S+) (\S+) (\S+) (\S+) "([^"]*)" (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) '
                    r'"([^"]*)" "([^"]*)"')
CF_NETS = ("162.158.", "162.159.", "172.64.", "172.65.", "172.66.", "172.67.", "172.68.", "172.69.", "172.70.", "172.71.",
           "104.16.", "104.17.", "104.18.", "104.19.", "104.20.", "104.21.", "104.22.", "104.23.", "104.24.",
           "104.25.", "104.26.", "104.27.", "108.162.", "141.101.", "190.93.", "188.114.", "197.234.", "198.41.",
           "173.245.", "103.21.", "103.22.", "103.31.", "131.0.72.")
S3_TYPES = ["DataTransfer-Out-Bytes", "Requests-SIA-Tier1", "Requests-INT-Tier1", "EarlyDelete-SIA", "Retrieval-SIA",
            "TimedStorage-ByteHrs", "TimedStorage-SIA-ByteHrs", "TimedStorage-INT-FA-ByteHrs", "Monitoring-Automation-INT",
            "Requests-Tier1", "Requests-Tier2", "BatchOperations-Objects"]
CW_TYPES = ["CW:MetricInsightAlarmUsage", "CW:AlarmMonitorUsage", "CW:GMD-Metrics", "CW:Requests", "DataProcessing-Bytes",
            "TimedStorage-ByteHrs"]


def billed_class(req, ip):
    if "assumed-role" in req:
        return None
    if ":user/" in req:
        return "runner"
    if req == "-":
        return "cloudflare" if ip.startswith(CF_NETS) else "direct"
    return None


def selftest():
    assert billed_class("-", "162.158.1.2") == "cloudflare" and billed_class("-", "17.241.1.1") == "direct"
    assert billed_class("arn:aws:iam::1:user/github-actions-justhodl", "1.1.1.1") == "runner"
    assert billed_class("arn:aws:sts::1:assumed-role/x/y", "1.1.1.1") is None
    print("selftest ok")


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report

    selftest()
    cfg = Config(retries={"max_attempts": 8, "mode": "standard"}, connect_timeout=10, read_timeout=60, max_pool_connections=64)
    ce = boto3.client("ce", region_name="us-east-1", config=cfg)
    s3 = boto3.client("s3", region_name=REGION, config=cfg)
    today = datetime.now(timezone.utc).date()
    with report("ops_6644_cost_fixes_followup") as r:
        r.heading("ops 6644 - did the Oct 23-24 cost fixes show up? (Cost Explorer + Oct 24 access logs, read-only)")
        facts = {}
        r.section("A. Cost Explorer per day")
        start, end = date(2026, 10, 21), today + timedelta(days=1)
        tot = ce.get_cost_and_usage(TimePeriod={"Start": start.isoformat(), "End": end.isoformat()}, Granularity="DAILY",
                                    Metrics=["UnblendedCost"], Filter={"Not": {"Dimensions": {"Key": "RECORD_TYPE", "Values": ["Credit", "Refund"]}}},
                                    GroupBy=[{"Type": "DIMENSION", "Key": "SERVICE"}])
        for day in tot["ResultsByTime"]:
            rows = sorted(((g["Keys"][0], float(g["Metrics"]["UnblendedCost"]["Amount"])) for g in day["Groups"]), key=lambda x: -x[1])
            r.log("%s%s total $%.2f | %s" % (day["TimePeriod"]["Start"], " (partial)" if day["TimePeriod"]["Start"] == today.isoformat() else "",
                                             sum(v for _, v in rows), ", ".join("%s $%.2f" % (k[:28], v) for k, v in rows[:7] if v >= 0.01)))
            facts["total_" + day["TimePeriod"]["Start"]] = round(sum(v for _, v in rows), 2)
        for svc, types in (("Amazon Simple Storage Service", S3_TYPES), ("AmazonCloudWatch", CW_TYPES)):
            res = ce.get_cost_and_usage(TimePeriod={"Start": start.isoformat(), "End": end.isoformat()}, Granularity="DAILY",
                                        Metrics=["UnblendedCost", "UsageQuantity"], Filter={"Dimensions": {"Key": "SERVICE", "Values": [svc]}},
                                        GroupBy=[{"Type": "DIMENSION", "Key": "USAGE_TYPE"}])
            for day in res["ResultsByTime"]:
                rows = {g["Keys"][0]: (float(g["Metrics"]["UnblendedCost"]["Amount"]), float(g["Metrics"]["UsageQuantity"]["Amount"])) for g in day["Groups"]}
                d = day["TimePeriod"]["Start"]
                shown = ", ".join("%s $%.2f (%.1f)" % (k, rows[k][0], rows[k][1]) for k in types if k in rows and (rows[k][0] >= 0.005 or rows[k][1] >= 1))
                others = sorted(((k, v[0]) for k, v in rows.items() if k not in types and v[0] >= 0.05), key=lambda x: -x[1])
                r.log("%s %s $%.2f | %s%s" % (d, svc.replace("Amazon Simple Storage Service", "S3"), sum(v[0] for v in rows.values()), shown,
                                              (" | other: " + ", ".join("%s $%.2f" % o for o in others[:5])) if others else ""))
                for k in ("DataTransfer-Out-Bytes", "Requests-SIA-Tier1", "Requests-INT-Tier1", "EarlyDelete-SIA", "CW:MetricInsightAlarmUsage"):
                    if k in rows:
                        facts["%s_%s" % (k, d)] = (round(rows[k][0], 2), round(rows[k][1], 1))

        r.section("B. access logs of Oct 24 so far: billed GB by class, Applebot")
        lg = s3.get_bucket_logging(Bucket=LIVE).get("LoggingEnabled") or {}
        tb, tp = lg.get("TargetBucket"), lg.get("TargetPrefix") or ""
        base = "%s%s/%s/%s/%s/" % (tp, ACCOUNT, REGION, LIVE, today.strftime("%Y/%m/%d"))
        keys, tok = [], None
        while True:
            kw = {"Bucket": tb, "Prefix": base, "MaxKeys": 1000}
            if tok:
                kw["ContinuationToken"] = tok
            res = s3.list_objects_v2(**kw)
            keys += [(o["Key"], o["LastModified"]) for o in res.get("Contents", [])]
            if not res.get("IsTruncated") or len(keys) >= 900000:
                break
            tok = res.get("NextContinuationToken")
        if keys:
            newest, oldest = max(lm for _, lm in keys), min(lm for _, lm in keys)
            n_sample = 4000
            sample = [k for k, _ in keys[::max(1, len(keys) // n_sample)]][:n_sample]
            span_h = max(1.0, (newest - oldest).total_seconds() / 3600.0)
            f = len(keys) / float(len(sample) or 1) * 24.0 / span_h

            def read(k):
                try:
                    return s3.get_object(Bucket=tb, Key=k)["Body"].read().decode("utf-8", "replace")
                except Exception:  # noqa: BLE001
                    return ""
            gb, statuses, apple, direct_ua = Counter(), Counter(), Counter(), Counter()
            with cf.ThreadPoolExecutor(max_workers=48) as ex:
                for text in ex.map(read, sample):
                    for line in text.splitlines():
                        m = LOG_RE.match(line)
                        if not m or m.group(7) not in ("REST.GET.OBJECT", "REST.HEAD.OBJECT"):
                            continue
                        ua, ip = m.group(17) or "", m.group(4)
                        if "applebot" in ua.lower() or ip.startswith("17."):
                            apple[m.group(10)] += 1
                        cls = billed_class(m.group(5), ip)
                        if not cls:
                            continue
                        b = int(m.group(12)) if m.group(12).isdigit() else 0
                        gb[cls] += b
                        statuses["%s %s" % (cls, m.group(10))] += 1
                        if cls == "direct":
                            direct_ua[ua[:80]] += b
            facts["logs_gb_day"] = {c: round(v * f / 1e9, 3) for c, v in gb.items()}
            facts["applebot_requests_day"] = {s: round(v * f) for s, v in apple.items()}
            r.log("log objects %d over %.1f h (read %d, x%.1f to a day)" % (len(keys), span_h, len(sample), f))
            r.log("billed GB/day by class: %s" % facts["logs_gb_day"])
            r.log("requests/day by class+status: %s" % {k: round(v * f) for k, v in statuses.most_common(12)})
            r.log("Applebot requests/day by status: %s" % (facts["applebot_requests_day"] or "none seen"))
            r.log("direct GB/day by user agent: %s" % [(k, round(v * f / 1e9, 3)) for k, v in direct_ua.most_common(5)])
        else:
            r.warn("no access-log objects for %s yet" % today)

        r.kv(**{k: str(v) for k, v in facts.items() if not k.startswith(("Requests-Tier", "TimedStorage"))})
        r.ok("read-only; nothing changed")
    sys.exit(0)


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        main()
