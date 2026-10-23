"""ops 6625 - READ-ONLY: who is the Safari-UA reader on 17.166.0.0/16 pulling ~10-30 GB/day straight from S3?

ops 6624: the direct (not-Cloudflare, not-AWS) GETs of the live bucket were 10.2 GB on Oct 22 and a 30.4 GB/day pace on
Oct 23, round the clock, all from 17.166.x addresses with a Safari-on-Mac user agent, ~6,000 requests/day on each of the
eleven files why.html's fleet-forensic block loads directly from S3 (forensic-screen, share-flows, buyback-engine,
industry-rotation, master-ranker, accumulation-radar, insider-radar, best-setups, finra-short, ai-rerating-radar,
estimate-revisions), and 64 k of the day's 75 k direct requests answered 403. This op, read-only:
  1. full user-agent strings, referers and error codes of the direct requests (200 vs 403) from today's logs,
  2. reverse DNS of the busiest 17.166.x addresses (Applebot resolves to *.applebot.apple.com),
  3. the live bucket policy's statements (Sid, effect, condition keys) - which rule answers the 403s.
Nothing is written to AWS. Cost: ~3 k small GETs of log objects + listings (< $0.02).
"""
from collections import Counter
from datetime import datetime, timezone
import concurrent.futures as cf
import json
import re
import socket
import sys
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
ACCOUNT = "857687956942"
BUCKET = "justhodl-dashboard-live"
CFG = Config(retries={"max_attempts": 6, "mode": "standard"}, connect_timeout=10, read_timeout=60, max_pool_connections=64)
LOG_RE = re.compile(r'^(\S+) (\S+) \[([^\]]+)\] (\S+) (\S+) (\S+) (\S+) (\S+) "([^"]*)" (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) '
                    r'"([^"]*)" "([^"]*)"')
s3 = boto3.client("s3", region_name=REGION, config=CFG)


def main():
    now = datetime.now(timezone.utc)
    with report("ops_6625_direct_reader_identity") as r:
        r.heading("ops 6625 - identity of the direct S3 reader on 17.166.x (read-only), %s" % now.strftime("%Y-%m-%d %H:%M UTC"))
        lg = s3.get_bucket_logging(Bucket=BUCKET).get("LoggingEnabled") or {}
        tb, tp = lg.get("TargetBucket"), lg.get("TargetPrefix") or ""
        base = "%s%s/%s/%s/%s/" % (tp, ACCOUNT, REGION, BUCKET, now.strftime("%Y/%m/%d"))
        keys, token = [], None
        while True:
            kw = {"Bucket": tb, "Prefix": base, "MaxKeys": 1000}
            if token:
                kw["ContinuationToken"] = token
            resp = s3.list_objects_v2(**kw)
            keys += [o["Key"] for o in resp.get("Contents", [])]
            if not resp.get("IsTruncated") or len(keys) >= 900000:
                break
            token = resp.get("NextContinuationToken")
        sample = keys[::max(1, len(keys) // 3000)][:3000]

        def read(k):
            try:
                return s3.get_object(Bucket=tb, Key=k)["Body"].read().decode("utf-8", "replace")
            except Exception:  # noqa: BLE001
                return ""

        ua, ref, err, ips, uri = Counter(), Counter(), Counter(), Counter(), Counter()
        with cf.ThreadPoolExecutor(max_workers=48) as ex:
            for text in ex.map(read, sample):
                for line in text.splitlines():
                    m = LOG_RE.match(line)
                    if not m or not m.group(4).startswith("17.") or m.group(5) != "-":
                        continue
                    st = m.group(10)
                    ua["%s | %s" % (st, m.group(17))] += 1
                    ref["%s | %s" % (st, m.group(16))] += 1
                    err["%s %s" % (st, m.group(11))] += 1
                    ips[m.group(4)] += 1
                    uri["%s | %s" % (st, m.group(9)[:140])] += 1
        r.log("log objects today %d, read %d" % (len(keys), len(sample)))
        r.section("1. direct requests from 17.x: status/error, user agents, referers")
        r.log("status+error: %s" % err.most_common(10))
        for k, v in ua.most_common(8):
            r.log("UA x%d: %s" % (v, k))
        for k, v in ref.most_common(8):
            r.log("Referer x%d: %s" % (v, k))
        for k, v in uri.most_common(12):
            r.log("URI x%d: %s" % (v, k))
        r.section("2. reverse DNS of the busiest addresses")
        rdns = {}
        for ip, n in ips.most_common(12):
            try:
                rdns[ip] = socket.gethostbyaddr(ip)[0]
            except Exception as ex:  # noqa: BLE001
                rdns[ip] = "no PTR (%s)" % type(ex).__name__
            r.log("%s x%d -> %s" % (ip, n, rdns[ip]))
        r.section("3. live bucket policy statements")
        pol = json.loads(s3.get_bucket_policy(Bucket=BUCKET)["Policy"])
        for st in pol.get("Statement", []):
            res = st.get("Resource")
            n_res = len(res) if isinstance(res, list) else 1
            r.log("%s %s action=%s resources=%d condition=%s" % (st.get("Sid"), st.get("Effect"), st.get("Action"), n_res,
                                                                json.dumps(st.get("Condition"))[:300]))
        applebot = sum(v for k, v in ua.items() if "applebot" in k.lower()) + sum(1 for h in rdns.values() if "applebot" in h.lower())
        r.kv(direct_lines=sum(ips.values()), applebot_signals=applebot, distinct_ips=len(ips))
        r.ok("read-only; nothing changed")
    sys.exit(0)


if __name__ == "__main__":
    main()
