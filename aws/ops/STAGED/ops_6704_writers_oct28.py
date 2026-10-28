"""ops 6704 - READ-ONLY: who writes on Oct 28 so far, against Oct 27 (Cost Explorer showed 460 k live writes by midday).

Same method as ops 6678 (access logs sampled, scaled to a day; writes per writer and per folder, then what grew).
Nothing is written but this report.
"""
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
import concurrent.futures as cf
import re
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))

REGION, ACCOUNT = "us-east-1", "857687956942"
LIVE = "justhodl-dashboard-live"
DAYS = ["2026/10/27", "2026/10/28"]
LOG_RE = re.compile(r'^(\S+) (\S+) \[([^\]]+)\] (\S+) (\S+) (\S+) (\S+) (\S+) "([^"]*)" (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) '
                    r'"([^"]*)" "([^"]*)"')
WRITE_OPS = ("REST.PUT.OBJECT", "REST.COPY.OBJECT", "REST.POST.UPLOAD", "REST.POST.UPLOADS")


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
    if re.fullmatch(r"\d{4}-?\d{2}-?\d{2}", parts[1]):
        return parts[0] + "/"
    return "/".join(parts[:2]) + "/"


def writer(req):
    if "assumed-role" in req:
        return req.split("/")[-1][:48]
    if ":user/" in req:
        return "runner:" + req.split("/")[-1]
    return req[:30]


def selftest():
    assert family("data/warm/fmp/x/y.json") == "data/warm/fmp/" and family("edge-data.json") == "(root files)"
    assert writer("arn:aws:sts::1:assumed-role/role/justhodl-fmp-bank") == "justhodl-fmp-bank"
    assert writer("arn:aws:iam::1:user/github-actions-justhodl") == "runner:github-actions-justhodl"
    print("selftest ok")


def main():
    import boto3
    from botocore.config import Config
    from urllib.parse import unquote
    from ops_report import report

    selftest()
    s3 = boto3.client("s3", region_name=REGION, config=Config(retries={"max_attempts": 8, "mode": "standard"}, max_pool_connections=64))
    with report("ops_6704_writers_oct28") as r:
        r.heading("ops 6704 - who writes on Oct 28 so far vs Oct 27 (access logs, read-only)")
        lg = s3.get_bucket_logging(Bucket=LIVE).get("LoggingEnabled") or {}
        tb, tp = lg.get("TargetBucket"), lg.get("TargetPrefix") or ""
        per_day = {}
        for day in DAYS:
            base = "%s%s/%s/%s/%s/" % (tp, ACCOUNT, REGION, LIVE, day)
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
            if not keys:
                r.warn("%s: no access logs" % day)
                continue
            n_sample = 6000
            sample = [k for k, _ in keys[::max(1, len(keys) // n_sample)]][:n_sample]
            newest, oldest = max(lm for _, lm in keys), min(lm for _, lm in keys)
            span_h = max(1.0, (newest - oldest).total_seconds() / 3600.0)
            f = len(keys) / float(len(sample) or 1) * (24.0 / span_h if span_h < 23 else 1.0)

            def read(k):
                try:
                    return s3.get_object(Bucket=tb, Key=k)["Body"].read().decode("utf-8", "replace")
                except Exception:  # noqa: BLE001
                    return ""
            fam, who, pair = Counter(), Counter(), Counter()
            with cf.ThreadPoolExecutor(max_workers=48) as ex:
                for text in ex.map(read, sample):
                    for line in text.splitlines():
                        m = LOG_RE.match(line)
                        if not m or m.group(7) not in WRITE_OPS or not m.group(10).startswith("2"):
                            continue
                        fm, w = family(unquote(m.group(8))), writer(m.group(5))
                        fam[fm] += 1
                        who[w] += 1
                        pair[(w, fm)] += 1
            per_day[day] = {"fam": {k: v * f for k, v in fam.items()}, "who": {k: v * f for k, v in who.items()},
                            "pair": {k: v * f for k, v in pair.items()}, "total": sum(fam.values()) * f}
            r.log("%s: %d log objects over %.1f h, read %d (x%.1f to a day): ~%d writes/day" % (day, len(keys), span_h, len(sample), f, per_day[day]["total"]))

        r.section("writers per day (scaled), biggest first by the latest day")
        last = DAYS[-1] if DAYS[-1] in per_day else DAYS[-2]
        base_day = DAYS[0]
        allw = set().union(*[set(v["who"]) for v in per_day.values()])
        for w in sorted(allw, key=lambda x: -per_day.get(last, {}).get("who", {}).get(x, 0))[:20]:
            r.log("%-50s %s" % (w, " | ".join("%s %7d" % (d[5:], per_day.get(d, {}).get("who", {}).get(w, 0)) for d in DAYS)))
        r.section("what grew most (latest day vs Oct 23), writer x folder")
        grow = Counter()
        for k, v in per_day.get(last, {}).get("pair", {}).items():
            grow[k] = v - per_day.get(base_day, {}).get("pair", {}).get(k, 0)
        for (w, fm), dv in grow.most_common(15):
            r.log("+%7d/day  %-44s %s" % (dv, w, fm))
        r.kv(**{"writes_%s" % d.replace("/", "-"): round(v["total"]) for d, v in per_day.items()})
        r.ok("read-only; nothing changed")
    sys.exit(0)


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        main()
