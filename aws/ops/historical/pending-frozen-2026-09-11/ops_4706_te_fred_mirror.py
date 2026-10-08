"""ops 4706 — Khalid's exact lead: TE's dedicated FRED-mirror
endpoint, api.tradingeconomics.com/fred/historical/{lowercased-fred-
mnemonic}. Different from everything tested tonight (not /country/,
not /markets/, not the public site's stream.ashx). Test with the
existing key first (single test, quota-conscious), then check if the
pattern generalizes across the gap.
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
ssm = boto3.client("ssm", region_name="us-east-1")
KHALID_SID = "bamlemptprvicrpisytw"
KHALID_FRED_ID = "BAMLEMPTPRVICRPISYTW"


def get(url, timeout=25):
    rq = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/124.0.0.0 Safari/537.36"})
    with urllib.request.urlopen(rq, timeout=timeout) as r:
        return r.status, dict(r.headers), r.read()


def main():
    with report("4706_te_fred_mirror") as r:
        r.heading("ops 4706 — TE's dedicated /fred/historical/ mirror "
                  "endpoint")
        key = ssm.get_parameter(
            Name="/justhodl/te_api",
            WithDecryption=True)["Parameter"]["Value"]
        r.log("  stored key length=%d (never printed; testing "
             "whether it's already key:secret formatted)" % len(key))

        r.section("1. THE EXACT test — Khalid's symbol, existing key")
        url = ("https://api.tradingeconomics.com/fred/historical/"
              + KHALID_SID + "?c=" + key)
        real_data = False
        try:
            st, hd, body = get(url)
            txt = body.decode("utf-8", "replace")
            r.log("  status=%s bytes=%d" % (st, len(body)))
            try:
                d = json.loads(txt)
                if isinstance(d, list):
                    dates = sorted(str(x.get("DATE") or x.get("Date")
                                       or x.get("date") or "")[:10]
                                   for x in d if isinstance(x, dict))
                    dates = [x for x in dates
                            if x[:2] in ("19", "20")]
                    r.log("  parsed %d rows, dates %s -> %s"
                         % (len(d), dates[0] if dates else None,
                            dates[-1] if dates else None))
                    if dates and dates[0] < "2020":
                        real_data = True
                        r.log("  first row: %s" % d[0])
                        r.log("  last row: %s" % d[-1])
                else:
                    r.log("  not a list — raw: %s" % txt[:300])
            except Exception as e:
                r.log("  not JSON (%s) — raw: %s"
                     % (str(e)[:60], txt[:300]))
        except urllib.error.HTTPError as e:
            body2 = e.read()[:300].decode("utf-8", "replace")
            r.log("  HTTP %s: %s" % (e.code, body2))
        except Exception as e:
            r.log("  %s" % str(e)[:150])

        r.section("verdict 1")
        if real_data:
            r.ok("CONFIRMED — Khalid's exact lead works with the "
                "existing key. This is the breakthrough.")
        else:
            r.log("Did not confirm on the first shape — see raw "
                 "response above for the real reason before "
                 "abandoning (auth error vs symbol error vs format "
                 "error all look different)")

        r.section("2. If it worked — does the pattern generalize? "
                  "Test 2 more: a core-27 series and a still-gap "
                  "series")
        if real_data:
            for sid in ("BAMLH0A0HYM2", "BAMLCC0A2AATRIV"):
                lc = sid.lower()
                u2 = ("https://api.tradingeconomics.com/fred/"
                     "historical/" + lc + "?c=" + key)
                try:
                    st2, hd2, body2 = get(u2, timeout=20)
                    d2 = json.loads(body2)
                    n2 = len(d2) if isinstance(d2, list) else 0
                    dates2 = sorted(str(x.get("DATE") or x.get(
                        "Date") or "")[:10] for x in (d2 or [])
                        if isinstance(x, dict))
                    dates2 = [x for x in dates2
                             if x[:2] in ("19", "20")]
                    r.log("  %s -> %d rows, %s -> %s"
                         % (sid, n2, dates2[0] if dates2 else None,
                            dates2[-1] if dates2 else None))
                except Exception as e:
                    r.log("  %s -> %s" % (sid, str(e)[:100]))
                time.sleep(0.8)

        r.section("verdict")
        doc = {"as_of": time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                      time.gmtime()),
               "confirmed": real_data}
        try:
            cov = json.loads(s3.get_object(
                Bucket=B,
                Key="data/ice-alt-claims-investig.json")["Body"
                                                        ].read())
        except Exception:
            cov = {}
        cov["te_fred_mirror_endpoint"] = doc
        s3.put_object(Bucket=B, Key="data/ice-alt-claims-investig.json",
                      Body=json.dumps(cov, default=str).encode(),
                      ContentType="application/json")
        if not real_data:
            r.fail("existing key did not unlock the /fred/historical/ "
                  "endpoint on this pass — raw response above shows "
                  "exactly why")
            sys.exit(1)
        r.ok("BREAKTHROUGH CONFIRMED — the dedicated FRED-mirror "
            "endpoint works with the existing key")


if __name__ == "__main__":
    main()
