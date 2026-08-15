"""ops 4707 — decisive test: is /fred/historical/ a licensing wall,
or was Khalid's specific symbol just wrong?

4706 got a clean 200-empty-array (bytes=2, "[]") for
bamlemptprvicrpisytw -- structurally identical to the earlier
out-of-plan-indicator pattern. Test a CORE, unambiguous series
(BAMLH0A0HYM2 -- can't be mistyped, it's the most basic one) against
the SAME endpoint. Empty too -> licensing wall, endpoint not
entitled. Real data -> it's specifically Khalid's symbol that's off,
go find the correct FRED id for that EM sub-index next.
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


def get(url, timeout=20):
    rq = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/124.0.0.0 Safari/537.36"})
    with urllib.request.urlopen(rq, timeout=timeout) as r:
        return r.status, r.read()


def main():
    with report("4707_te_fred_mirror_decisive") as r:
        r.heading("ops 4707 — core series test: licensing wall vs "
                  "wrong symbol")
        key = ssm.get_parameter(
            Name="/justhodl/te_api",
            WithDecryption=True)["Parameter"]["Value"]

        core_tests = ["bamlh0a0hym2", "bamlc0a0cm", "baa10y",
                     "dgs10"]
        results = {}
        for sid in core_tests:
            url = ("https://api.tradingeconomics.com/fred/"
                  "historical/" + sid + "?c=" + key)
            try:
                st, body = get(url)
                txt = body.decode("utf-8", "replace")
                try:
                    d = json.loads(txt)
                    n = len(d) if isinstance(d, list) else -1
                except Exception:
                    n = -2
                results[sid] = (st, len(body), n)
                r.log("  %-16s status=%s bytes=%d parsed_rows=%s"
                     % (sid, st, len(body), n))
                if n and n > 0:
                    r.log("    sample row: %s" % d[0])
                    r.log("    last row: %s" % d[-1])
            except urllib.error.HTTPError as e:
                b2 = e.read()[:200].decode("utf-8", "replace")
                results[sid] = (e.code, 0, None)
                r.log("  %-16s HTTP %s: %s" % (sid, e.code, b2))
            except Exception as e:
                results[sid] = (None, 0, None)
                r.log("  %-16s %s" % (sid, str(e)[:100]))
            time.sleep(0.7)

        r.section("verdict")
        any_real = any(n and n > 0 for _st, _b, n in results.values())
        doc = {"as_of": time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                      time.gmtime()),
               "results": {k: list(v) for k, v in results.items()},
               "any_real_data": any_real}
        try:
            cov = json.loads(s3.get_object(
                Bucket=B,
                Key="data/ice-alt-claims-investig.json")["Body"
                                                        ].read())
        except Exception:
            cov = {}
        cov["te_fred_mirror_decisive"] = doc
        s3.put_object(Bucket=B, Key="data/ice-alt-claims-investig.json",
                      Body=json.dumps(cov, default=str).encode(),
                      ContentType="application/json")
        if not any_real:
            r.fail("EVERY core series (including plain rate series "
                  "like DGS10/BAA10Y, not just BAML) came back empty "
                  "-- this is a licensing wall on the /fred/ "
                  "endpoint itself, not a symbol problem")
            sys.exit(1)
        r.ok("real data confirmed on at least one core series -- the "
            "endpoint IS entitled; Khalid's specific symbol needs "
            "checking")


if __name__ == "__main__":
    main()
