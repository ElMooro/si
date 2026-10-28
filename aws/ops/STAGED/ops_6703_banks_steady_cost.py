"""ops 6703 - READ-ONLY: the steady daily cost once the new provider banks have filled themselves (scheduled check).

ops 6678 found ~1.05 M writes/day on Oct 26 (justhodl-bank-histories 639 k, -polygon-eod 213 k, -bank-factors 104 k,
-worldbank-full 45 k, -polygon-bank 27 k) - their first population; all of them write only changed files afterwards.
Oct 27 (Tuesday) is the first steady weekday. This op reads:
  A. Cost Explorer Oct 25 -> today: account total and services per day; S3, Lambda and CloudWatch by usage type (keyed
     by service AND usage type - ops 6677 let Lambda's DataTransfer-Out-Bytes overwrite S3's);
  B. the access logs of Oct 27 (sampled, scaled to the day): writes per writer and per folder, against Oct 26;
  C. each bank's own run record (state/manifest last_run: written vs unchanged) - a bank that rewrites unchanged files
     shows it there;
  D. a November projection from Oct 27: x30, minus a fresh month's free allowances, plus Business Support+.
Nothing is written but this report.
"""
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import concurrent.futures as cf
import json
import re
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))

REGION, ACCOUNT = "us-east-1", "857687956942"
LIVE = "justhodl-dashboard-live"
DAY = "2026-10-27"
OCT26 = {"justhodl-bank-histories": 639059, "justhodl-polygon-eod": 212962, "justhodl-bank-factors": 104248,
         "justhodl-worldbank-full": 45082, "justhodl-polygon-bank": 26792}
BANK_STATE = {"justhodl-bank-histories": ["data/warm/history-book/_state/state.json", "data/warm/history-book/manifest.json"],
              "justhodl-bank-factors": ["data/warm/factor-book/manifest.json"],
              "justhodl-polygon-eod": ["data/warm/polygon-full/eod/_state/state.json", "data/warm/polygon-full/eod/manifest.json"],
              "justhodl-polygon-bank": ["data/warm/polygon-bank/_state/state.json", "data/warm/polygon-bank/manifest.json"]}
LOG_RE = re.compile(r'^(\S+) (\S+) \[([^\]]+)\] (\S+) (\S+) (\S+) (\S+) (\S+) "([^"]*)" (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) '
                    r'"([^"]*)" "([^"]*)"')
WRITE_OPS = ("REST.PUT.OBJECT", "REST.COPY.OBJECT", "REST.POST.UPLOAD", "REST.POST.UPLOADS")
LAMBDA_GBS_PRICE, LAMBDA_FREE_GBS, EGRESS_PRICE, EGRESS_FREE_GB, ALARM_FREE = 0.0000166667, 400000, 0.09, 100, 1.00
SERVICES = {"Amazon Simple Storage Service": "S3", "AWS Lambda": "Lambda", "AmazonCloudWatch": "CloudWatch"}


def family(key):
    parts = key.split("/")
    if len(parts) == 1:
        return "(root files)"
    if parts[0] == "data" and len(parts) >= 3 and parts[1] in ("warm", "providers"):
        return "/".join(parts[:3]) + "/" if len(parts) > 3 else "data/%s/(files)" % parts[1]
    if parts[0] == "audit-private" and len(parts) >= 4:
        return "/".join(parts[:3]) + "/"
    if len(parts) == 2:
        return parts[0] + "/(files)"
    return "/".join(parts[:2]) + "/"


def writer(req):
    if "assumed-role" in req:
        return req.split("/")[-1][:48]
    if ":user/" in req:
        return "runner:" + req.split("/")[-1]
    return req[:30]


def projection(day_total, lambda_gbs_day, s3_egress_gb_day):
    month = day_total * 30
    free = (min(lambda_gbs_day * 30, LAMBDA_FREE_GBS) * LAMBDA_GBS_PRICE + 0.20
            + min(s3_egress_gb_day * 30, EGRESS_FREE_GB) * EGRESS_PRICE + ALARM_FREE)
    usage = max(0.0, month - free)
    support = max(29.0, 0.09 * usage)
    return round(usage, 2), round(support, 2), round(usage + support, 2), round(free, 2)


def last_runs(doc, depth=0):
    """Every last_run-like record in a state/manifest document (bounded walk)."""
    out = []
    if depth > 3 or not isinstance(doc, dict):
        return out
    for k, v in doc.items():
        if k in ("last_run", "run", "runs_last") and isinstance(v, dict):
            out.append(v)
        elif isinstance(v, dict):
            out += last_runs(v, depth + 1)
    return out


def selftest():
    assert writer("arn:aws:sts::1:assumed-role/r/justhodl-bank-histories") == "justhodl-bank-histories"
    assert family("data/warm/history-book/symbols/AAPL.json.gz") == "data/warm/history-book/"
    u, s, t, f = projection(6.5, 110000, 3.3)
    assert s == 29.0 and 170 < u < 190 and f > 15
    assert last_runs({"a": {"last_run": {"puts": 3}}, "last_run": {"unchanged": 9}}) == [{"puts": 3}, {"unchanged": 9}] or \
        last_runs({"a": {"last_run": {"puts": 3}}, "last_run": {"unchanged": 9}}) == [{"unchanged": 9}, {"puts": 3}]
    print("selftest ok")


def main():
    import boto3
    from botocore.config import Config
    from urllib.parse import unquote
    from ops_report import report

    selftest()
    cfg = Config(retries={"max_attempts": 8, "mode": "standard"}, connect_timeout=10, read_timeout=60, max_pool_connections=64)
    ce = boto3.client("ce", region_name="us-east-1", config=cfg)
    s3 = boto3.client("s3", region_name=REGION, config=cfg)
    today = datetime.now(timezone.utc).date()
    end = today + timedelta(days=1)
    with report("ops_6703_banks_steady_cost") as r:
        r.heading("ops 6703 - the steady day after the banks filled themselves (Oct 27), and November (read-only)")
        r.section("A. Cost Explorer per day")
        res = ce.get_cost_and_usage(TimePeriod={"Start": "2026-10-25", "End": end.isoformat()}, Granularity="DAILY", Metrics=["UnblendedCost"],
                                    Filter={"Not": {"Dimensions": {"Key": "RECORD_TYPE", "Values": ["Credit", "Refund"]}}},
                                    GroupBy=[{"Type": "DIMENSION", "Key": "SERVICE"}])
        totals, est = {}, {}
        for day in res["ResultsByTime"]:
            d = day["TimePeriod"]["Start"]
            rows = sorted(((g["Keys"][0], float(g["Metrics"]["UnblendedCost"]["Amount"])) for g in day["Groups"]), key=lambda x: -x[1])
            totals[d], est[d] = sum(v for _, v in rows), day.get("Estimated")
            r.log("%s%s total $%.2f | %s" % (d, " (today, partial)" if d == today.isoformat() else "", totals[d],
                                             ", ".join("%s $%.2f" % (k[:28], v) for k, v in rows if v >= 0.02)))
        usage = {}
        for svc, short in SERVICES.items():
            res = ce.get_cost_and_usage(TimePeriod={"Start": "2026-10-25", "End": end.isoformat()}, Granularity="DAILY",
                                        Metrics=["UnblendedCost", "UsageQuantity"], Filter={"Dimensions": {"Key": "SERVICE", "Values": [svc]}},
                                        GroupBy=[{"Type": "DIMENSION", "Key": "USAGE_TYPE"}])
            for day in res["ResultsByTime"]:
                d = day["TimePeriod"]["Start"]
                rows = {g["Keys"][0]: (float(g["Metrics"]["UnblendedCost"]["Amount"]), float(g["Metrics"]["UsageQuantity"]["Amount"])) for g in day["Groups"]}
                for k, v in rows.items():
                    usage[(d, short, k)] = v
                top = sorted(rows.items(), key=lambda x: -x[1][0])[:9]
                r.log("%s %-10s $%.2f | %s" % (d, short, sum(v[0] for v in rows.values()), ", ".join("%s $%.3f (%.0f)" % (k, v[0], v[1]) for k, v in top if v[0] >= 0.005)))

        r.section("B. writes on %s per writer (access logs, sampled) against Oct 26" % DAY)
        lg = s3.get_bucket_logging(Bucket=LIVE).get("LoggingEnabled") or {}
        tb, tp = lg.get("TargetBucket"), lg.get("TargetPrefix") or ""
        base = "%s%s/%s/%s/%s/" % (tp, ACCOUNT, REGION, LIVE, DAY.replace("-", "/"))
        keys, tok = [], None
        while True:
            kw = {"Bucket": tb, "Prefix": base, "MaxKeys": 1000}
            if tok:
                kw["ContinuationToken"] = tok
            res = s3.list_objects_v2(**kw)
            keys += [o["Key"] for o in res.get("Contents", [])]
            if not res.get("IsTruncated") or len(keys) >= 900000:
                break
            tok = res.get("NextContinuationToken")
        who, fam = Counter(), Counter()
        total_writes = 0.0
        if keys:
            sample = keys[::max(1, len(keys) // 6000)][:6000]
            f = len(keys) / float(len(sample))

            def read(k):
                try:
                    return s3.get_object(Bucket=tb, Key=k)["Body"].read().decode("utf-8", "replace")
                except Exception:  # noqa: BLE001
                    return ""
            with cf.ThreadPoolExecutor(max_workers=48) as ex:
                for text in ex.map(read, sample):
                    for line in text.splitlines():
                        m = LOG_RE.match(line)
                        if m and m.group(7) in WRITE_OPS and m.group(10).startswith("2"):
                            who[writer(m.group(5))] += f
                            fam[family(unquote(m.group(8)))] += f
            total_writes = sum(who.values())
            r.log("%d log objects, read %d (x%.1f): ~%d writes on %s" % (len(keys), len(sample), f, total_writes, DAY))
            for w, n in who.most_common(15):
                r.log("  %-46s %8d%s" % (w, n, ("   (Oct 26: %d)" % OCT26[w]) if w in OCT26 else ""))
            for w in OCT26:
                if w not in who:
                    r.log("  %-46s %8d   (Oct 26: %d)" % (w, 0, OCT26[w]))
            r.log("top folders: %s" % ", ".join("%s %d" % (k, v) for k, v in fam.most_common(10)))
        else:
            r.warn("no access logs for %s" % DAY)

        r.section("C. the banks' own run records")
        for bank, paths in BANK_STATE.items():
            for p in paths:
                try:
                    body = s3.get_object(Bucket=LIVE, Key=p)["Body"].read()
                    doc = json.loads(body)
                except Exception as ex:  # noqa: BLE001
                    r.log("%s %s: %s" % (bank, p, type(ex).__name__))
                    continue
                runs = last_runs(doc)
                if runs:
                    for run in runs[:2]:
                        brief = {k: v for k, v in run.items() if not isinstance(v, (list, dict))}
                        r.log("%s %s last_run: %s" % (bank, p.rsplit("/", 2)[-2] + "/" + p.rsplit("/", 1)[-1], json.dumps(brief, default=str)[:420]))
                    break

        r.section("D. November from %s" % DAY)
        if DAY not in totals:
            r.fail("%s not in Cost Explorer yet" % DAY)
            sys.exit(1)

        def u(short, k):
            return usage.get((DAY, short, k), (0.0, 0.0))
        egress_gb = u("S3", "DataTransfer-Out-Bytes")[1]
        lambda_gbs = u("Lambda", "Lambda-GB-Second")[1] + u("Lambda", "Lambda-GB-Second-ARM")[1]
        use, sup, tot, free = projection(totals[DAY], lambda_gbs, egress_gb)
        r.log("%s = $%.2f (%s): S3 $%.2f (live PUT/COPY/LIST %d = $%.2f, archive copies %d = $%.2f, egress %.1f GB = $%.2f), "
              "Lambda $%.2f (%.0f GB-s)" % (DAY, totals[DAY], "estimated" if est.get(DAY) else "final",
                                           sum(v[0] for (d, s, k), v in usage.items() if d == DAY and s == "S3"),
                                           u("S3", "Requests-Tier1")[1], u("S3", "Requests-Tier1")[0], u("S3", "Requests-INT-Tier1")[1],
                                           u("S3", "Requests-INT-Tier1")[0], egress_gb, u("S3", "DataTransfer-Out-Bytes")[0],
                                           sum(v[0] for (d, s, k), v in usage.items() if d == DAY and s == "Lambda"), lambda_gbs))
        r.log("November from %s: $%.2f x 30 = $%.2f, minus a fresh month's free allowances ~$%.2f = $%.2f of usage, plus Business "
              "Support+ $%.2f = ~$%.2f" % (DAY, totals[DAY], totals[DAY] * 30, free, use, sup, tot))
        r.kv(day=DAY, day_total=round(totals[DAY], 2), estimated=bool(est.get(DAY)), writes=round(total_writes),
             live_puts=round(u("S3", "Requests-Tier1")[1]), archive_copies=round(u("S3", "Requests-INT-Tier1")[1]),
             egress_gb=round(egress_gb, 1), lambda_gbs=round(lambda_gbs), november_usage=use, november_support=sup, november_total=tot)
        r.ok("read-only; nothing changed")
    sys.exit(0)


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        main()
