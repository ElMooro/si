"""ops 4684 — arm the browser grabber + publish it ready-to-paste.

Deploys the kind="series" ingest route, verifies it end-to-end with a
synthetic post, injects the live ingest token into the console script,
and publishes the armed script to S3 so Khalid copies from a URL rather
than assembling anything. Also prints the exact Wayback page URLs for
the 27 target series so there is no hunting.
"""
import json
import sys
import time
import urllib.request

import boto3
from botocore.config import Config

from ops_report import report

B = "justhodl-dashboard-live"
FN = "justhodl-tv-notes-ingest"
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=120,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")

CORE = ["BAMLH0A0HYM2", "BAMLC0A0CM", "BAMLC0A1CAAA", "BAMLC0A2CAA",
        "BAMLC0A3CA", "BAMLC0A4CBBB", "BAMLH0A1HYBB", "BAMLH0A2HYB",
        "BAMLH0A3HYC", "BAMLC0A0CMEY", "BAMLC0A1CAAAEY",
        "BAMLC0A2CAAEY", "BAMLC0A3CAEY", "BAMLC0A4CBBBEY",
        "BAMLH0A0HYM2EY", "BAMLH0A1HYBBEY", "BAMLH0A2HYBEY",
        "BAMLH0A3HYCEY", "BAMLCC0A0CMTRIV", "BAMLCC0A1AAATRIV",
        "BAMLCC0A2AATRIV", "BAMLCC0A3ATRIV", "BAMLCC0A4BBBTRIV",
        "BAMLHYH0A0HYM2TRIV", "BAMLHYH0A1BBTRIV", "BAMLHYH0A2BTRIV",
        "BAMLHYH0A3CMTRIV"]


def main():
    with report("4684_arm_grabber") as r:
        r.heading("ops 4684 — re-arm grabber v2 (chart-independent)")
        misses = 0

        r.section("1. Settle the ingest deploy")
        t0, ok = time.time(), False
        while time.time() - t0 < 240:
            c = lam.get_function(FunctionName=FN)["Configuration"]
            if c.get("State") == "Active" and \
                    c.get("LastUpdateStatus") == "Successful":
                ok = True
                break
            time.sleep(8)
        if not ok:
            r.fail("  ingest not settled")
            sys.exit(1)
        r.ok("  ingest live")

        r.section("2. Verify kind='series' with a synthetic post")
        # 4684 rev: /justhodl/tvnotes/ingest-url does not exist —
        # resolve the same way the handler does (SSM token) and take
        # the URL from Lambda itself, which is authoritative.
        token = ""
        for pn in ("/justhodl/tvnotes/ingest-token",
                   "/justhodl/tv-notes/ingest-token"):
            try:
                token = ssm.get_parameter(
                    Name=pn, WithDecryption=True)["Parameter"]["Value"]
                break
            except Exception:
                continue
        if not token:
            env = (lam.get_function(FunctionName=FN)["Configuration"]
                   .get("Environment") or {}).get("Variables") or {}
            token = env.get("INGEST_TOKEN") or ""
        if not token:
            r.fail("  no ingest token resolvable")
            sys.exit(1)
        url = ""
        try:
            url = lam.get_function_url_config(
                FunctionName=FN)["FunctionUrl"]
        except Exception as e:
            r.warn("  url config: %s" % str(e)[:80])
        if not url:
            r.fail("  no function URL on %s" % FN)
            sys.exit(1)
        r.log("  ingest URL: %s (token len=%d, never printed)"
              % (url, len(token)))
        rows = [["1999-%02d-%02d" % (m, d), 1.0 + m * 0.01]
                for m in range(1, 13) for d in range(1, 6)]
        body = json.dumps({"token": token, "kind": "series",
                           "series": [{"id": "JHSELFTEST",
                                       "rows": rows,
                                       "source": "ops-selftest"}]})
        rq = urllib.request.Request(
            url, data=body.encode(),
            headers={"Content-Type": "application/json"})
        resp = json.loads(urllib.request.urlopen(rq, timeout=60).read())
        r.log("  response: %s" % json.dumps(resp)[:260])
        good = bool(resp.get("ok")) and resp.get("banked") == 1
        if good:
            r.ok("  [route] kind='series' banks correctly")
        else:
            misses += 1
            r.fail("  [route] synthetic post failed")
        try:
            d = json.loads(s3.get_object(
                Bucket=B,
                Key="data/warm/archived-fred/JHSELFTEST.json"
            )["Body"].read())
            r.log("  selftest doc: n=%s %s -> %s"
                  % (d.get("n"), d.get("first"), d.get("last")))
            s3.delete_object(
                Bucket=B,
                Key="data/warm/archived-fred/JHSELFTEST.json")
            r.log("  selftest doc removed")
        except Exception as e:
            r.warn("  selftest readback: %s" % str(e)[:90])

        r.section("3. Publish the ARMED script")
        js = open("tools/wayback-ice-grab.js").read()
        js = js.replace("__TOKEN__", token)
        js = js.replace("__INGEST__", url)     # v3: the miss that made
        # every extraction post into the void
        js = js.replace(
            "https://w4osroryszvlifgk4boofkh7cm0selzf.lambda-url."
            "us-east-1.on.aws/", url)
        key = "tools/wayback-ice-grab.js"
        if "__INGEST__" in js or "__TOKEN__" in js:
            r.fail("  placeholders still present after injection")
            sys.exit(1)
        s3.put_object(Bucket=B, Key=key, Body=js.encode(),
                      ContentType="application/javascript",
                      CacheControl="no-cache")
        r.ok("  armed script -> s3://%s/%s" % (B, key))
        r.log("  (token injected in S3 copy only; the repo copy keeps "
              "the __TOKEN__ placeholder)")

        r.section("4. Target pages")
        r.log("  For each series, open:")
        r.log("    https://web.archive.org/web/2024/https://"
              "fred.stlouisfed.org/series/<ID>")
        r.log("  click MAX, wait for the chart, paste the script.")
        for i in range(0, len(CORE), 3):
            r.log("    " + "  ".join(CORE[i:i + 3]))
        r.log("  Priority order: the 9 OAS series first (they drive "
              "the credit engines), then effective yields, then "
              "total-return.")

        r.section("verdict")
        if misses:
            r.fail("arming: %d red" % misses)
            sys.exit(1)
        r.ok("grabber armed and verified — each paste banks a series "
             "to data/warm/archived-fred/, merge is a follow-up "
             "audited pass")


if __name__ == "__main__":
    main()
