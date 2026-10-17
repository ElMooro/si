"""ops 6543 - READ-ONLY: who downloads ~57 GB/day from justhodl-dashboard-live (S3 DataTransfer-Out ~$4.70/day)?

ops 6529 proved the server access logs deliver (folders Sep 14 -> today) but sampled only the newest hour,
which is inside the fleet freeze: 17 MB/hour, all anonymous through Cloudflare. Egress was ~55 GB/day on
Oct 12-16 and ~57 GB/day on normal days, so the downloader is something the freeze stopped. This op samples
whole hours of a normal day (Oct 10: 03, 13, 21 UTC) and one storm hour (Oct 15 13 UTC) and attributes
GET bytes by requester class, user agent, referer, key prefix and source network. In-region GETs by Lambda
roles are free; anonymous/Cloudflare and anything from outside AWS is the billed egress.
"""

from collections import Counter
from pathlib import Path
import concurrent.futures as cf
import re
import sys

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
ACCOUNT = "857687956942"
BUCKET = "justhodl-dashboard-live"
LOG_RE = re.compile(r'^(\S+) (\S+) \[([^\]]+)\] (\S+) (\S+) (\S+) (\S+) (\S+) "([^"]*)" (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) '
                    r'"([^"]*)" "([^"]*)"')
HOURS = [("2026/10/10", "2026-10-10-03"), ("2026/10/10", "2026-10-10-13"), ("2026/10/10", "2026-10-10-21"),
         ("2026/10/15", "2026-10-15-13")]
s3 = boto3.client("s3", region_name=REGION, config=Config(retries={"max_attempts": 6, "mode": "standard"},
                                                          max_pool_connections=40))
CF_NETS = ("162.158.", "172.64.", "172.65.", "172.66.", "172.67.", "172.68.", "172.69.", "172.70.", "172.71.",
           "104.16.", "104.17.", "104.18.", "104.19.", "104.20.", "104.21.", "104.22.", "104.23.", "104.24.",
           "104.25.", "104.26.", "104.27.", "108.162.", "141.101.", "190.93.", "188.114.", "197.234.", "198.41.",
           "173.245.", "103.21.", "103.22.", "103.31.", "131.0.72.")


def requester_class(req, ip):
    if "assumed-role" in req:
        return "lambda/role:" + req.split("/")[-2] if req.count("/") >= 2 else "role"
    if ":user/" in req:
        return "iam-user:" + req.split("/")[-1]
    if req == "-":
        return "anonymous-cloudflare" if ip.startswith(CF_NETS) else "anonymous-other"
    return req.split(":")[-1][:40]


def sample_hour(r, tb, base, day, tag):
    keys, token = [], None
    while True:
        kw = {"Bucket": tb, "Prefix": base + day + "/" + tag, "MaxKeys": 1000}
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        keys += [o["Key"] for o in resp.get("Contents", [])]
        if not resp.get("IsTruncated") or len(keys) >= 20000:
            break
        token = resp.get("NextContinuationToken")

    def read(k):
        try:
            return s3.get_object(Bucket=tb, Key=k)["Body"].read().decode("utf-8", "replace")
        except Exception:  # noqa: BLE001
            return ""

    by_class, by_ua, by_ref, by_prefix, by_net, by_key = (Counter() for _ in range(6))
    gets, total = 0, 0
    with cf.ThreadPoolExecutor(max_workers=32) as ex:
        for text in ex.map(read, keys):
            for line in text.splitlines():
                m = LOG_RE.match(line)
                if not m or m.group(7) != "REST.GET.OBJECT":
                    continue
                b = int(m.group(12)) if m.group(12).isdigit() else 0
                ip, req, key = m.group(4), m.group(5), m.group(8)
                cls = requester_class(req, ip)
                gets += 1
                total += b
                by_class[cls] += b
                if not cls.startswith("lambda/"):
                    by_ua[(m.group(17) or "-")[:60]] += b
                    by_ref[(m.group(16) or "-").split("/")[2] if (m.group(16) or "").startswith("http") else "-"] += b
                    by_prefix["/".join(key.split("/")[:2])] += b
                    by_key[key] += b
                    by_net[".".join(ip.split(".")[:2]) + ".*"] += b
    gb = lambda c, n=8: [(k, round(v / 1e9, 3)) for k, v in c.most_common(n)]  # noqa: E731
    billed = sum(v for k, v in by_class.items() if not k.startswith("lambda/"))
    r.log("%s: %d log objects, %d GETs, %.2f GB total, %.2f GB not-from-Lambda (x24 = %.1f GB/day)" % (
        tag, len(keys), gets, total / 1e9, billed / 1e9, billed * 24 / 1e9))
    r.log("   by requester class: %s" % gb(by_class))
    r.log("   non-Lambda by user agent: %s" % gb(by_ua, 6))
    r.log("   non-Lambda by referer host: %s" % gb(by_ref, 6))
    r.log("   non-Lambda by key prefix: %s" % gb(by_prefix, 10))
    r.log("   non-Lambda top keys: %s" % gb(by_key, 8))
    r.log("   non-Lambda by network /16: %s" % gb(by_net, 6))
    return billed


def main():
    with report("ops_6543_egress_attribution") as r:
        r.heading("ops 6543 - who downloads ~57 GB/day from justhodl-dashboard-live (read-only)")
        lg = s3.get_bucket_logging(Bucket=BUCKET).get("LoggingEnabled") or {}
        tb, tp = lg.get("TargetBucket"), lg.get("TargetPrefix") or ""
        if not tb:
            r.fail("server access logging is off")
            sys.exit(1)
        base = "%s%s/%s/%s/" % (tp, ACCOUNT, REGION, BUCKET)
        r.log("logs: s3://%s/%s" % (tb, base))
        done = 0
        for day, tag in HOURS:
            r.section(tag)
            try:
                sample_hour(r, tb, base, day, tag)
                done += 1
            except Exception as e:  # noqa: BLE001
                r.warn("%s failed: %s" % (tag, str(e)[:200]))
        if not done:
            r.fail("no hour could be sampled")
            sys.exit(1)
        r.ok("sampled %d hours" % done)
    sys.exit(0)


if __name__ == "__main__":
    main()
