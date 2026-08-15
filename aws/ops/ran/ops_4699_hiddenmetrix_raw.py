"""ops 4699 — inspect the two real hiddenmetrix hits directly (raw
content, not regex) + scan the homepage for its real API structure.

4698's "deeper found" verdict for claim 1 was a bug (string "2023" <
"2023-08-15" via prefix comparison on release/dates, which is a
publication SCHEDULE, not observation depth) -- retracted, not
repeated here. This op is purely claim 2: api.hiddenmetrix.com
answered JSON (58 bytes, content unseen) and hiddenmetrix.com/data/
BAMLH0A1HYBB returned 29,938 bytes that the prior regex failed to
parse. Print both raw. Also scan the homepage HTML for script/API
references to find the REAL endpoint pattern instead of guessing.
"""
import json
import re
import sys
import time
import urllib.error
import urllib.request

import boto3

from ops_report import report

B = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")
TEST_SID = "BAMLH0A1HYBB"


def get(url, timeout=25):
    rq = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/124.0.0.0 Safari/537.36"})
    with urllib.request.urlopen(rq, timeout=timeout) as r:
        return r.status, dict(r.headers), r.read()


def main():
    with report("4699_hiddenmetrix_raw") as r:
        r.heading("ops 4699 — hiddenmetrix: raw content, not regex")

        r.section("1. api.hiddenmetrix.com raw body (58 bytes seen "
                  "before, never printed)")
        api_ok = False
        try:
            st, hd, body = get("https://api.hiddenmetrix.com")
            r.log("  status=%s ct=%s body=%r"
                 % (st, hd.get("Content-Type"), body[:500]))
            api_ok = True
        except Exception as e:
            r.log("  %s" % str(e)[:150])

        r.section("2. hiddenmetrix.com/data/BAMLH0A1HYBB raw body "
                  "(29,938 bytes — regex found nothing, look "
                  "directly)")
        data_body = b""
        data_ok = False
        try:
            st, hd, data_body = get(
                "https://hiddenmetrix.com/data/" + TEST_SID)
            data_ok = True
            r.log("  status=%s ct=%s" % (st, hd.get("Content-Type")))
            txt = data_body.decode("utf-8", "replace")
            r.log("  first 800 chars:\n%s" % txt[:800])
            r.log("  ---")
            r.log("  last 800 chars:\n%s" % txt[-800:])
            is_json = False
            try:
                json.loads(txt)
                is_json = True
            except Exception:
                pass
            r.log("  valid JSON: %s | contains 'BAML': %s | "
                 "contains '7735' or '7,735': %s | contains "
                 "'1996': %s"
                 % (is_json, "BAML" in txt, ("7735" in txt or
                                             "7,735" in txt),
                    "1996" in txt))
        except Exception as e:
            r.log("  %s" % str(e)[:150])

        r.section("3. Homepage — hunt for the REAL API pattern "
                  "(script refs, fetch/XHR hints, JSON blobs)")
        try:
            st, hd, home = get("https://hiddenmetrix.com")
            htxt = home.decode("utf-8", "replace")
            scripts = re.findall(
                r'<script[^>]+src="([^"]+)"', htxt)[:20]
            r.log("  script srcs found: %s" % scripts)
            api_hints = sorted(set(re.findall(
                r'["\'](/[a-zA-Z0-9_/-]*api[a-zA-Z0-9_/-]*)["\']',
                htxt)))[:20]
            r.log("  '/...api...' path hints in HTML: %s" % api_hints)
            batch_hints = sorted(set(re.findall(
                r'["\'](/[a-zA-Z0-9_/-]*batch[a-zA-Z0-9_/-]*)["\']',
                htxt)))
            r.log("  '/...batch...' path hints: %s" % batch_hints)
            bb_hints = sorted(set(re.findall(
                r'["\'](/[a-zA-Z0-9_/-]{2,40}bb[a-zA-Z0-9_/-]{0,20}'
                r')["\']', htxt, re.I)))[:15]
            r.log("  '...bb...' path hints (possible BB page route): "
                 "%s" % bb_hints)
        except Exception as e:
            r.log("  %s" % str(e)[:150])

        r.section("verdict")
        doc = {"as_of": time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                      time.gmtime()),
               "api_root_body": None, "data_path_body_len":
                   len(data_body)}
        try:
            cov = json.loads(s3.get_object(
                Bucket=B,
                Key="data/ice-alt-claims-investig.json")["Body"
                                                        ].read())
        except Exception:
            cov = {}
        cov["hiddenmetrix_raw_probe"] = doc
        s3.put_object(Bucket=B, Key="data/ice-alt-claims-investig.json",
                      Body=json.dumps(cov, default=str).encode(),
                      ContentType="application/json")
        if not (api_ok or data_ok):
            r.fail("both hiddenmetrix probes threw — could not "
                   "retrieve ANY real content to inspect")
            sys.exit(1)
        r.ok("raw content inspected — see log above for whether "
             "real history is actually there")


if __name__ == "__main__":
    main()
