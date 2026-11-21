"""ops 6870 - READ-ONLY: where this month's AWS money goes, and what part of it moves with how often data is refreshed.

Khalid 2026-11-20 22:12 CT: "the next thing im curious about is how to keep data updating as needed without racking
up my monthly bill". The daily health run reads the month at $177.85 on day 20 (governor projects $275, he expects
about $200), S3 at $4 one day and $7 the next, Lambda steady near $2.7. This reads, month to date:
  A. Cost Explorer per service, and S3 / Lambda / CloudWatch per usage type (cost and quantity), summed for the month
     and day by day for S3 - which fee it is that doubles every other day;
  B. the live bucket's access logs for the newest dear day and the newest cheap day (sampled, scaled to the day):
     writes per writer and per folder - who writes the files the dear day pays for;
  C. the banks' own run records (written against unchanged).
Nothing is written but this report.
"""
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import unquote
import concurrent.futures as cf
import importlib.util
import json
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
_spec = importlib.util.spec_from_file_location("ops6703", str(Path(__file__).resolve().parent / "ops_6703_banks_steady_cost.py"))
B = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(B)


def writes_on(s3, r, tb, tp, day):
    base = "%s%s/%s/%s/%s/" % (tp, B.ACCOUNT, B.REGION, B.LIVE, day.replace("-", "/"))
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
    who, fam, hours = Counter(), Counter(), Counter()
    if not keys:
        r.warn("no access logs for %s" % day)
        return who, fam, hours, 0
    sample = keys[::max(1, len(keys) // 5000)][:5000]
    f = len(keys) / float(len(sample))

    def read(k):
        try:
            return s3.get_object(Bucket=tb, Key=k)["Body"].read().decode("utf-8", "replace")
        except Exception:  # noqa: BLE001
            return ""
    with cf.ThreadPoolExecutor(max_workers=48) as ex:
        for text in ex.map(read, sample):
            for line in text.splitlines():
                m = B.LOG_RE.match(line)
                if m and m.group(7) in B.WRITE_OPS and m.group(10).startswith("2"):
                    w = B.writer(m.group(5))
                    who[w] += f
                    fam[B.family(unquote(m.group(8)))] += f
                    hours[(w, m.group(3)[12:14])] += f
    r.log("%s: %d log objects, read %d (x%.1f): ~%d writes" % (day, len(keys), len(sample), f, sum(who.values())))
    return who, fam, hours, sum(who.values())


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report

    cfg = Config(retries={"max_attempts": 8, "mode": "standard"}, connect_timeout=10, read_timeout=60, max_pool_connections=64)
    ce = boto3.client("ce", region_name="us-east-1", config=cfg)
    s3 = boto3.client("s3", region_name=B.REGION, config=cfg)
    today = datetime.now(timezone.utc).date()
    start, end = today.replace(day=1), today + timedelta(days=1)
    with report("ops_6870_where_the_month_goes") as r:
        r.heading("ops 6870 - where %s's money goes (read-only), %s" % (start.strftime("%B"), datetime.now(timezone.utc).isoformat(timespec="minutes")))
        r.section("A. Cost Explorer, %s to %s" % (start, today))
        res = ce.get_cost_and_usage(TimePeriod={"Start": start.isoformat(), "End": end.isoformat()}, Granularity="DAILY", Metrics=["UnblendedCost"],
                                    Filter={"Not": {"Dimensions": {"Key": "RECORD_TYPE", "Values": ["Credit", "Refund"]}}},
                                    GroupBy=[{"Type": "DIMENSION", "Key": "SERVICE"}])
        totals, svc_mtd, s3_day = {}, Counter(), {}
        for day in res["ResultsByTime"]:
            d = day["TimePeriod"]["Start"]
            rows = {g["Keys"][0]: float(g["Metrics"]["UnblendedCost"]["Amount"]) for g in day["Groups"]}
            totals[d] = sum(rows.values())
            s3_day[d] = rows.get("Amazon Simple Storage Service", 0.0)
            for k, v in rows.items():
                svc_mtd[k] += v
        final = sorted(d for d in totals if d < (today - timedelta(days=1)).isoformat())      # the last two days are still estimates
        mtd = sum(totals.values())
        r.log("month to date $%.2f over %d days (%d final): %s" % (mtd, len(totals), len(final), ", ".join("%s $%.2f (%.0f%%)" % (k[:30], v, v / mtd * 100) for k, v in svc_mtd.most_common(9))))
        r.log("per day, total / S3: %s" % ", ".join("%s $%.2f/$%.2f" % (d[8:], totals[d], s3_day[d]) for d in sorted(totals)))
        usage, mtd_usage = {}, defaultdict(lambda: [0.0, 0.0])
        for svc, short in B.SERVICES.items():
            res = ce.get_cost_and_usage(TimePeriod={"Start": start.isoformat(), "End": end.isoformat()}, Granularity="DAILY",
                                        Metrics=["UnblendedCost", "UsageQuantity"], Filter={"Dimensions": {"Key": "SERVICE", "Values": [svc]}},
                                        GroupBy=[{"Type": "DIMENSION", "Key": "USAGE_TYPE"}])
            for day in res["ResultsByTime"]:
                d = day["TimePeriod"]["Start"]
                for g in day["Groups"]:
                    c, q = float(g["Metrics"]["UnblendedCost"]["Amount"]), float(g["Metrics"]["UsageQuantity"]["Amount"])
                    usage[(d, short, g["Keys"][0])] = (c, q)
                    mtd_usage[(short, g["Keys"][0])][0] += c
                    mtd_usage[(short, g["Keys"][0])][1] += q
        for short in sorted(set(k[0] for k in mtd_usage)):
            rows = sorted(((k[1], v) for k, v in mtd_usage.items() if k[0] == short), key=lambda x: -x[1][0])
            r.log("%s month to date $%.2f: %s" % (short, sum(v[0] for _, v in rows), ", ".join("%s $%.2f (%s)" % (k, v[0], "{:,.0f}".format(v[1])) for k, v in rows[:12] if v[0] >= 0.05)))
        # the dear day and the cheap day: the newest pair of final days, S3 line by line
        hi = max(final[-6:], key=lambda d: s3_day[d]) if final else None
        lo = min(final[-6:], key=lambda d: s3_day[d]) if final else None
        if hi and lo:
            keys = sorted({k[2] for k in usage if k[1] == "S3" and k[0] in (hi, lo)}, key=lambda k: -(usage.get((hi, "S3", k), (0, 0))[0] - usage.get((lo, "S3", k), (0, 0))[0]))
            r.log("S3 on the dear day %s ($%.2f) against the cheap day %s ($%.2f), by fee:" % (hi, s3_day[hi], lo, s3_day[lo]))
            for k in keys:
                a, b = usage.get((hi, "S3", k), (0.0, 0.0)), usage.get((lo, "S3", k), (0.0, 0.0))
                if max(a[0], b[0]) >= 0.02:
                    r.log("  %-34s $%.2f (%s)  against  $%.2f (%s)  = %+.2f" % (k, a[0], "{:,.0f}".format(a[1]), b[0], "{:,.0f}".format(b[1]), a[0] - b[0]))
        r.section("B. who writes: the access logs of the dear day and the cheap day")
        lg = s3.get_bucket_logging(Bucket=B.LIVE).get("LoggingEnabled") or {}
        tb, tp = lg.get("TargetBucket"), lg.get("TargetPrefix") or ""
        seen = {}
        for day in [d for d in (hi, lo) if d]:
            who, fam, hours, n = writes_on(s3, r, tb, tp, day)
            seen[day] = (who, n)
            for w, c in who.most_common(14):
                hrs = sorted(((h, v) for (ww, h), v in hours.items() if ww == w), key=lambda x: -x[1])[:4]
                r.log("  %-44s %9s  (%.0f%%)  busiest hours UTC %s" % (w, "{:,.0f}".format(c), c / n * 100 if n else 0, ", ".join("%s:00 %s" % (h, "{:,.0f}".format(v)) for h, v in hrs)))
            r.log("  folders: %s" % ", ".join("%s %s" % (k, "{:,.0f}".format(v)) for k, v in fam.most_common(10)))
        if hi and lo and hi in seen and lo in seen:
            diff = sorted(((w, seen[hi][0].get(w, 0) - seen[lo][0].get(w, 0)) for w in set(seen[hi][0]) | set(seen[lo][0])), key=lambda x: -abs(x[1]))[:8]
            r.log("the dear day's extra writes, by writer: %s" % ", ".join("%s %+d" % (w, v) for w, v in diff))
        r.section("C. the banks' own run records (written against unchanged)")
        states = dict(B.BANK_STATE)
        states.update({"justhodl-fmp-bank": ["data/warm/fmp/_state/bank.json", "data/warm/fmp/manifest.json"], "justhodl-fmp-walker": ["data/warm/fmp/_state/walker.json", "data/warm/fmp/walker-manifest.json"],
                       "justhodl-eia-bank": ["data/warm/eia/manifest.json"]})
        for bank, paths in states.items():
            for p in paths:
                try:
                    doc = json.loads(s3.get_object(Bucket=B.LIVE, Key=p)["Body"].read())
                except Exception as ex:  # noqa: BLE001
                    r.log("%s %s: %s" % (bank, p, type(ex).__name__))
                    continue
                runs = B.last_runs(doc)
                top = {k: v for k, v in doc.items() if not isinstance(v, (list, dict))}
                r.log("%s %s: %s" % (bank, p.rsplit("/", 1)[-1], json.dumps(top, default=str)[:300]))
                for run in runs[:2]:
                    r.log("    last_run: %s" % json.dumps({k: v for k, v in run.items() if not isinstance(v, (list, dict))}, default=str)[:420])
                break
        s3_mtd = svc_mtd.get("Amazon Simple Storage Service", 0.0)
        lam_mtd = svc_mtd.get("AWS Lambda", 0.0)
        days_final = max(1, len(final))
        avg = sum(totals[d] for d in final) / days_final
        r.kv(month_to_date=round(mtd, 2), days=len(totals), per_day_final=round(avg, 2), month_at_this_pace=round(avg * 30, 2), s3_mtd=round(s3_mtd, 2), s3_share_pct=round(s3_mtd / mtd * 100) if mtd else None,
             lambda_mtd=round(lam_mtd, 2), dear_day=hi, dear_day_s3=round(s3_day.get(hi, 0), 2) if hi else None, cheap_day=lo, cheap_day_s3=round(s3_day.get(lo, 0), 2) if lo else None,
             writes_dear=round(seen.get(hi, ({}, 0))[1]) if hi else None, writes_cheap=round(seen.get(lo, ({}, 0))[1]) if lo else None)
        r.ok("read-only; nothing changed")
    sys.exit(0)


if __name__ == "__main__":
    main()
