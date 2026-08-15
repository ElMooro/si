"""ops 4705 — isolate why stream.ashx said "Dates not valid".

4704's key finding: adding d1/d2 flipped the response from silent
empty (200, 0 bytes) to an actual validation message (200, 15 bytes,
"Dates not valid") -- meaning the endpoint recognizes s=/d1=/d2= and
is actively processing them. Narrow test: is it the DATE FORMAT
(YYYY-MM-DD) or the RANGE (1996 too old / too wide) that's rejected?
Varies one axis at a time.
"""
import json
import sys
import time
import urllib.error
import urllib.request

import boto3

from ops_report import report

B = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")
SLUG = ("bofa-merrill-lynch-private-sector-issuers-emerging-markets-"
       "corporate-plus-sub-index-semi-annual-yield-to-worst")


def get(url, timeout=15):
    rq = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/124.0.0.0 Safari/537.36"})
    with urllib.request.urlopen(rq, timeout=timeout) as r:
        return r.status, r.read()


def main():
    with report("4705_te_date_format") as r:
        r.heading("ops 4705 — isolate the real date format stream.ashx "
                  "wants")
        base = "https://tradingeconomics.com/ws/stream.ashx?s=" + SLUG
        tests = [
            ("YYYY-MM-DD narrow (2022-2023)",
             base + "&d1=2022-01-01&d2=2023-08-14"),
            ("MM/DD/YYYY",
             base + "&d1=01/01/2022&d2=08/14/2023"),
            ("MM-DD-YYYY",
             base + "&d1=01-01-2022&d2=08-14-2023"),
            ("no d1, only d2",
             base + "&d2=2023-08-14"),
            ("span=10y (no explicit dates)",
             base + "&span=10y"),
            ("span=max (no explicit dates)",
             base + "&span=max"),
            ("d1 only, YYYY-MM-DD, very recent",
             base + "&d1=2026-01-01"),
        ]
        hits = []
        for nm, url in tests:
            try:
                st, body = get(url)
                r.log("  [%s] status=%s bytes=%d body=%r"
                     % (nm, st, len(body), body[:300]))
                if len(body) > 20 and b"not valid" not in body.lower(
                        ) and b"error" not in body.lower()[:60]:
                    hits.append((nm, url, len(body)))
                    r.ok("    ^ looks like real data, not an error!")
            except urllib.error.HTTPError as e:
                r.log("  [%s] HTTP %s" % (nm, e.code))
            except Exception as e:
                r.log("  [%s] %s" % (nm, str(e)[:100]))
            time.sleep(0.6)

        r.section("verdict")
        doc = {"as_of": time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                      time.gmtime()),
               "hits": [[a, b, c] for a, b, c in hits]}
        try:
            cov = json.loads(s3.get_object(
                Bucket=B,
                Key="data/ice-alt-claims-investig.json")["Body"
                                                        ].read())
        except Exception:
            cov = {}
        cov["te_date_format_probe"] = doc
        s3.put_object(Bucket=B, Key="data/ice-alt-claims-investig.json",
                      Body=json.dumps(cov, default=str).encode(),
                      ContentType="application/json")
        if not hits:
            r.fail("no format/range variant produced real data — "
                  "the endpoint validates but rejects every date "
                  "shape tried")
            sys.exit(1)
        r.ok("found a working format: %s" % hits[0][0])


if __name__ == "__main__":
    main()
