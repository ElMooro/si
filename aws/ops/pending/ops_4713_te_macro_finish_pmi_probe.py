"""ops 4713 — (a) finish the 33 macro series queued behind ICE,
(b) probe whether TE's NATIVE /historical/country/.../indicator/
endpoint (not the /fred/ mirror) carries real historical PMI depth,
before building phase 2.

Phase 1 (ICE) used /fred/historical/{mnemonic} -- a FRED mirror, so it
only carries what FRED itself has. FRED does not natively carry most
countries' PMI data, so phase 2 (Khalid: "PMI and other important
global data") needs TE's OWN historical endpoint instead -- already
proven tonight (ops 4701/4702) to serve real dated depth for non-FRED
categories like housing/credit. Confirming that specifically works for
PMI, across a few countries, before scoping a full buildout.
"""
import json
import sys
import time
import urllib.error
import urllib.request

import boto3
from botocore.config import Config

from ops_report import report

B = "justhodl-dashboard-live"
FN = "justhodl-te-fred-mirror"
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=300,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")


def get(url, timeout=20):
    rq = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/124.0.0.0 Safari/537.36"})
    with urllib.request.urlopen(rq, timeout=timeout) as r:
        return r.status, r.read()


def gj(key, dflt=None):
    try:
        return json.loads(s3.get_object(Bucket=B,
                                        Key=key)["Body"].read())
    except Exception:
        return dflt if dflt is not None else {}


def main():
    with report("4713_te_macro_finish_pmi_probe") as r:
        r.heading("ops 4713 — finish macro-33 + probe TE native PMI "
                  "historical depth")

        r.section("1. Drive the 33 core-macro series to completion")
        st0 = gj("data/warm/te-mirror/_state.json")
        r.log("  before: done=%d catalog=%d"
             % (len(set(st0.get("done") or [])),
                len(st0.get("catalog") or [])))
        for i in range(3):
            resp = lam.invoke(FunctionName=FN,
                             InvocationType="RequestResponse",
                             Payload=b"{}")
            raw = resp["Payload"].read().decode("utf-8", "replace")
            r.log("  round %d: %s" % (i + 1, raw[:260]))
            d = json.loads(raw)
            if d.get("status") == "COMPLETE-maintaining":
                r.ok("  full catalog (225) converged")
                break
            time.sleep(2)

        r.section("2. Probe TE's NATIVE indicator-history endpoint "
                  "for real PMI depth (not the /fred/ mirror)")
        key = ssm.get_parameter(
            Name="/justhodl/te_api",
            WithDecryption=True)["Parameter"]["Value"]
        countries = ["united states", "china", "germany", "japan",
                    "united kingdom"]
        pmi_hits = []
        for c in countries:
            url = ("https://api.tradingeconomics.com/historical/"
                  "country/" + urllib.request.quote(c)
                  + "/indicator/manufacturing%20pmi?c=" + key
                  + "&f=json")
            try:
                st, body = get(url, timeout=25)
                d = json.loads(body)
                n = len(d) if isinstance(d, list) else 0
                dates = sorted(set(
                    str(x.get("DATE") or x.get("Date") or "")[:10]
                    for x in (d or []) if isinstance(x, dict)))
                dates = [x for x in dates if x[:2] in ("19", "20")]
                r.log("  %-16s status=%s rows=%d dates=%s..%s"
                     % (c, st, n, dates[0] if dates else None,
                        dates[-1] if dates else None))
                if n > 12:
                    pmi_hits.append((c, n, dates[0] if dates else None,
                                    dates[-1] if dates else None))
                    if d:
                        r.log("    sample row: %s" % d[0])
            except urllib.error.HTTPError as e:
                r.log("  %-16s HTTP %s" % (c, e.code))
            except Exception as e:
                r.log("  %-16s %s" % (c, str(e)[:100]))
            time.sleep(0.6)

        r.section("3. Also check services PMI + a non-PMI global "
                  "indicator, for scope")
        extra = [
            ("united states", "services pmi"),
            ("germany", "inflation rate"),
            ("china", "gdp growth rate"),
        ]
        for c, cat in extra:
            url = ("https://api.tradingeconomics.com/historical/"
                  "country/" + urllib.request.quote(c)
                  + "/indicator/" + urllib.request.quote(cat)
                  + "?c=" + key + "&f=json")
            try:
                st, body = get(url, timeout=20)
                d = json.loads(body)
                n = len(d) if isinstance(d, list) else 0
                r.log("  %-16s / %-20s status=%s rows=%d"
                     % (c, cat, st, n))
            except Exception as e:
                r.log("  %-16s / %-20s %s" % (c, cat, str(e)[:80]))
            time.sleep(0.5)

        r.section("verdict")
        doc = {"as_of": time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                      time.gmtime()),
               "pmi_hits": [[a, b, c, d] for a, b, c, d in pmi_hits]}
        cov = gj("data/repo-coverage.json", {})
        cov["te_pmi_scope_probe"] = doc
        s3.put_object(Bucket=B, Key="data/repo-coverage.json",
                     Body=json.dumps(cov, default=str).encode(),
                     ContentType="application/json")
        if not pmi_hits:
            r.fail("no country returned real PMI historical depth — "
                  "TE's native indicator endpoint may need a "
                  "different category name for PMI, or genuinely "
                  "doesn't carry it")
            sys.exit(1)
        r.ok("PMI historical depth CONFIRMED via TE's native "
            "indicator endpoint on %d/%d countries -- phase 2 is "
            "buildable" % (len(pmi_hits), len(countries)))


if __name__ == "__main__":
    main()
