"""ops 4701 — real paid TE key exists (/justhodl/te_api, used by
justhodl-te-feed for macro snapshots only, no credit-spread history).
This tests whether the SAME real key, pointed at TE's historical
endpoint instead of the country-snapshot one, can pull real 2017-2023
daily credit-spread history -- the thing te-feed was never built to do.

Minimal, quota-conscious: te-feed's own code shows real 403 quota
limits exist on this plan, so this probes ONE indicator first before
expanding to the full credit family.
"""
import json
import sys
import time
import urllib.error
import urllib.parse
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
    with report("4701_te_real_key") as r:
        r.heading("ops 4701 — real paid TE key vs the historical "
                  "credit-spread endpoint")

        r.section("1. Confirm the real key resolves")
        try:
            key = ssm.get_parameter(
                Name="/justhodl/te_api",
                WithDecryption=True)["Parameter"]["Value"]
        except Exception as e:
            r.fail("  key not resolvable: %s" % str(e)[:120])
            sys.exit(1)
        r.log("  key resolved, len=%d (never printed)" % len(key))

        r.section("2. Minimal validity check — one cheap call, "
                  "exactly what te-feed already calls successfully")
        try:
            st, hd, body = get(
                "https://api.tradingeconomics.com/country/"
                "united%20states?c=" + key + "&f=json")
            d = json.loads(body)
            r.log("  status=%s rows=%s (matches te-feed's own "
                 "working call shape)"
                 % (st, len(d) if isinstance(d, list) else "n/a"))
            key_valid = st == 200 and isinstance(d, list) and len(
                d) > 0
        except Exception as e:
            r.log("  %s" % str(e)[:150])
            key_valid = False
        if not key_valid:
            r.fail("  key appears invalid/expired even on the "
                  "known-working call shape — stop here, do not "
                  "burn quota on the historical test")
            sys.exit(1)
        r.ok("  key is LIVE and valid")

        r.section("3. THE REAL TEST — historical endpoint for a "
                  "credit-spread indicator (te-feed never calls this "
                  "path)")
        cand = [
            ("historical/indicator (spread)",
             "https://api.tradingeconomics.com/historical/country/"
             "united%20states/indicator/high%20yield%20bond%20spread"
             "?c=" + key + "&f=json"),
            ("historical w/ explicit date range",
             "https://api.tradingeconomics.com/historical/country/"
             "united%20states/indicator/high%20yield%20bond%20spread"
             "?c=" + key + "&d1=2015-01-01&d2=2023-08-14&f=json"),
            ("markets bond-spread search",
             "https://api.tradingeconomics.com/markets/search/"
             "high%20yield?c=" + key + "&f=json"),
        ]
        best_hit = None
        for nm, url in cand:
            try:
                st, hd, body = get(url, timeout=25)
                txt = body.decode("utf-8", "replace")
                try:
                    d = json.loads(txt)
                except Exception:
                    d = None
                n_rows = len(d) if isinstance(d, list) else 0
                dates = sorted(set(
                    str(row.get("DATE") or row.get("Date") or "")[:10]
                    for row in (d or []) if isinstance(row, dict)))
                dates = [x for x in dates if x[:2] in ("19", "20")]
                r.log("  [%s] status=%s rows=%d dates=%s..%s "
                     "sample=%s"
                     % (nm, st, n_rows,
                        dates[0] if dates else None,
                        dates[-1] if dates else None,
                        (d[0] if isinstance(d, list) and d
                         else txt[:200])))
                if dates and dates[0] < "2020":
                    best_hit = (nm, dates[0], dates[-1], n_rows)
                    r.ok("    ^ REAL 2017-era daily data confirmed!")
            except urllib.error.HTTPError as e:
                body2 = e.read()[:200].decode("utf-8", "replace")
                r.log("  [%s] HTTP %s: %s" % (nm, e.code, body2))
            except Exception as e:
                r.log("  [%s] %s" % (nm, str(e)[:150]))
            time.sleep(1.0)

        r.section("verdict")
        doc = {"as_of": time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                      time.gmtime()),
               "key_valid": key_valid,
               "historical_hit": bool(best_hit),
               "hit_detail": best_hit}
        try:
            cov = json.loads(s3.get_object(
                Bucket=B,
                Key="data/ice-alt-claims-investig.json")["Body"
                                                        ].read())
        except Exception:
            cov = {}
        cov["te_real_paid_key_test"] = doc
        s3.put_object(Bucket=B, Key="data/ice-alt-claims-investig.json",
                      Body=json.dumps(cov, default=str).encode(),
                      ContentType="application/json")
        if not best_hit:
            r.fail("real paid key works for country-snapshots (as "
                  "te-feed already proves) but the historical "
                  "credit-spread endpoint did not return deep dated "
                  "data on this pass")
            sys.exit(1)
        r.ok("CONFIRMED: real paid TE key + historical endpoint -> "
             "%s, %s to %s (%d rows). Ready to build the targeted "
             "importer for the full credit family."
             % best_hit)


if __name__ == "__main__":
    main()
