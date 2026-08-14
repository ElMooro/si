"""ops 4679 — unmask the rev-2 socket error.

4678 read STALE failures: the hourly schedule had already fired with
pre-fix code, so the failures dict was full of old-format messages
("handshake: b'...'") from catalog symbols, hiding what rev-2 actually
returned for the test symbols. rev-2 errors carry the endpoint name.

This op: clear the failures dict, disable the schedule for the moment
so it cannot re-pollute mid-test, invoke on 3 symbols, and print the
FULL new-format error per endpoint. Then re-enable the schedule.
"""
import gzip
import json
import sys
import time

import boto3
from botocore.config import Config

from ops_report import report

B = "justhodl-dashboard-live"
FN = "justhodl-tv-bars"
STATE = "data/warm/tv-bars/_state.json"
RULE = "justhodl-tv-bars-hourly"
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=600,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name="us-east-1")
ev = boto3.client("events", region_name="us-east-1")
TEST = ["BAMLH0A0HYM2", "BAMLC0A2CAA", "BAMLH0A3HYC"]


def gj(key, gz=False):
    try:
        raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
        return json.loads(gzip.decompress(raw) if gz else raw)
    except Exception:
        return {}


def main():
    with report("4679_tv_unmask") as r:
        r.heading("ops 4679 — unmask rev-2 socket error")
        misses = 0

        r.section("1. Quiesce: disable schedule, clear stale state")
        try:
            ev.disable_rule(Name=RULE)
            r.log("  schedule disabled for the test window")
        except Exception as e:
            r.warn("  disable: %s" % str(e)[:80])
        st = gj(STATE)
        old_n = len(st.get("failures") or {})
        st["failures"] = {}
        st["lease_until"] = 0
        s3.put_object(Bucket=B, Key=STATE,
                      Body=json.dumps(st, default=str).encode(),
                      ContentType="application/json")
        r.log("  cleared %d stale failure entries" % old_n)

        r.section("2. Invoke rev-2 on 3 symbols")
        t0 = time.time()
        resp = lam.invoke(FunctionName=FN,
                          InvocationType="RequestResponse",
                          Payload=json.dumps(
                              {"symbols": TEST}).encode())
        raw = resp["Payload"].read().decode("utf-8", "replace")
        r.log("  %.0fs · handler: %s" % (time.time() - t0, raw[:300]))

        r.section("3. The REAL rev-2 errors (per endpoint)")
        st = gj(STATE)
        fails = st.get("failures") or {}
        if not fails:
            r.log("  no failures recorded")
        for k, v in fails.items():
            r.log("  %s:" % k)
            for part in str(v).split(" | "):
                r.log("      %s" % part[:200])
        got = 0
        for sid in TEST:
            d = gj("data/warm/tv-bars/%s.json.gz" % sid, gz=True)
            if d:
                got += 1
                r.log("  BANKED %s: n=%s %s -> %s"
                      % (sid, d.get("n"), d.get("first_date"),
                         d.get("last_date")))

        r.section("4. Restore schedule")
        try:
            if got:
                ev.enable_rule(Name=RULE)
                r.log("  schedule re-enabled (rail works)")
            else:
                r.log("  schedule LEFT DISABLED — a broken rail "
                      "should not burn hourly invokes or hammer TV "
                      "with a failing handshake")
        except Exception as e:
            r.warn("  restore: %s" % str(e)[:80])

        r.section("verdict")
        if not got:
            r.fail("socket still refused — full per-endpoint evidence "
                   "above; TV may reject non-browser TLS/JA3 "
                   "fingerprints from AWS, which no header change "
                   "fixes")
            sys.exit(1)
        r.ok("rail works: %d/%d symbols banked" % (got, len(TEST)))


if __name__ == "__main__":
    main()
