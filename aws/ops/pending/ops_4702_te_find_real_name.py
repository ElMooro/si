"""ops 4702 — find TE's REAL category name for credit spreads.

4701 proved the key is live (200, 405 rows) but the historical call
came back 200-with-empty-array for a GUESSED indicator name. te-feed's
own code silently drops any category not in its ~170-entry allowlist
(no "spread"/"yield"/"credit" entries in it) -- so the raw snapshot
that already succeeded may contain exactly what we need, invisible
because nothing ever looked for it. This inspects the RAW response,
unfiltered, for anything spread/yield/credit/bond-shaped, then
immediately re-tries the historical endpoint with the REAL name found.
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


def get(url, timeout=25):
    rq = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/124.0.0.0 Safari/537.36"})
    with urllib.request.urlopen(rq, timeout=timeout) as r:
        return r.status, dict(r.headers), r.read()


def main():
    with report("4702_te_find_real_name") as r:
        r.heading("ops 4702 — find TE's real credit-spread category "
                  "name, then re-test historical")
        key = ssm.get_parameter(
            Name="/justhodl/te_api",
            WithDecryption=True)["Parameter"]["Value"]

        r.section("1. Raw, UNFILTERED country snapshot — every "
                  "category, not just te-feed's ~170-entry allowlist")
        st, hd, body = get(
            "https://api.tradingeconomics.com/country/united%20"
            "states?c=" + key + "&f=json")
        rows = json.loads(body)
        r.log("  total rows: %d" % len(rows))
        keywords = ("spread", "yield", "credit", "bond", "corporate",
                   "ice", "baml", "high yield", "junk")
        hits = [row for row in rows
               if any(k in str(row.get("Category", "")).lower()
                     for k in keywords)]
        r.log("  categories matching spread/yield/credit/bond/junk: "
             "%d" % len(hits))
        for row in hits:
            r.log("    Category=%r Title=%r LatestValue=%s "
                 "LatestValueDate=%s Unit=%r"
                 % (row.get("Category"), row.get("Title"),
                    row.get("LatestValue"), row.get("LatestValueDate"),
                    row.get("Unit")))
        if not hits:
            r.log("  sample of 15 real category names on this plan "
                 "(so we know what IS available even if not this):")
            for row in rows[:15]:
                r.log("    %r" % row.get("Category"))

        r.section("2. Re-test historical endpoint with the REAL "
                  "category name(s) found")
        confirmed = None
        for row in hits[:5]:
            cat_real = row.get("Category")
            for enc in (cat_real, cat_real.lower()):
                url = ("https://api.tradingeconomics.com/historical/"
                      "country/united%20states/indicator/"
                      + urllib.request.quote(enc) + "?c=" + key
                      + "&f=json")
                try:
                    st2, hd2, body2 = get(url, timeout=25)
                    d = json.loads(body2)
                    n_rows = len(d) if isinstance(d, list) else 0
                    dates = sorted(set(
                        str(x.get("DATE") or x.get("Date") or "")[:10]
                        for x in (d or []) if isinstance(x, dict)))
                    dates = [x for x in dates if x[:2] in ("19", "20")]
                    r.log("  historical[%r]: status=%s rows=%d "
                         "dates=%s..%s"
                         % (enc, st2, n_rows,
                            dates[0] if dates else None,
                            dates[-1] if dates else None))
                    if dates and dates[0] < "2020":
                        confirmed = (cat_real, dates[0], dates[-1],
                                    n_rows)
                        r.ok("    ^ REAL 2017-era daily data!")
                        break
                except urllib.error.HTTPError as e:
                    r.log("  historical[%r]: HTTP %s" % (enc, e.code))
                except Exception as e:
                    r.log("  historical[%r]: %s" % (enc, str(e)[:100]))
                time.sleep(0.7)
            if confirmed:
                break

        r.section("verdict")
        doc = {"as_of": time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                      time.gmtime()),
               "categories_found": [h.get("Category") for h in hits],
               "confirmed": confirmed}
        try:
            cov = json.loads(s3.get_object(
                Bucket=B,
                Key="data/ice-alt-claims-investig.json")["Body"
                                                        ].read())
        except Exception:
            cov = {}
        cov["te_real_category_search"] = doc
        s3.put_object(Bucket=B, Key="data/ice-alt-claims-investig.json",
                      Body=json.dumps(cov, default=str).encode(),
                      ContentType="application/json")
        if not confirmed:
            r.fail("no matching category on this plan returned deep "
                  "historical data — see categories_found for what "
                  "IS actually on the plan")
            sys.exit(1)
        r.ok("CONFIRMED: %s -> %s to %s (%d rows)" % confirmed)


if __name__ == "__main__":
    main()
