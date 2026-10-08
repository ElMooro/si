"""ops 4704 — extract the REAL stream.ashx call signature from the
page's own JS, not another guess.

4703 found: chart data is NOT embedded in the HTML (only 4 stray
dates in 235KB), and /ws/stream.ashx?s={slug} returns a genuine 200
with empty body -- a real live endpoint, just missing whatever param
actually triggers a response. Rather than guess more params, grep the
page's own 36KB chart-init script block for every "stream.ashx" or
".ashx" reference and print real surrounding context -- the page's own
JS knows the correct call shape; read it instead of guessing it.
"""
import json
import re as _re2
import sys
import time
import urllib.error
import urllib.request

import boto3

from ops_report import report

B = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")
PAGE_URL = ("https://tradingeconomics.com/united-states/"
           "bofa-merrill-lynch-private-sector-issuers-emerging-"
           "markets-corporate-plus-sub-index-semi-annual-yield-to-"
           "worst-fed-data.html")


def get(url, timeout=25, headers=None):
    h = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/124.0.0.0 Safari/537.36"}
    if headers:
        h.update(headers)
    rq = urllib.request.Request(url, headers=h)
    with urllib.request.urlopen(rq, timeout=timeout) as r:
        return r.status, dict(r.headers), r.read()


def main():
    with report("4704_te_stream_signature") as r:
        r.heading("ops 4704 — extract the real stream.ashx call "
                  "signature from the page's own JS")

        r.section("1. Refetch the page, isolate every script block")
        st, hd, body = get(PAGE_URL)
        txt = body.decode("utf-8", "replace")
        blocks = _re2.findall(r"<script[^>]*>(.*?)</script>", txt,
                              _re2.S)
        r.log("  %d script blocks, %d bytes total"
             % (len(blocks), sum(len(b) for b in blocks)))

        r.section("2. Find every .ashx / stream reference across ALL "
                  "blocks, with real context")
        found_any = False
        for i, blk in enumerate(blocks):
            for m in _re2.finditer(
                    r".{0,180}(?:stream\.ashx|\.ashx\?|ws/"
                    r"[a-zA-Z]+\.ashx).{0,180}", blk):
                found_any = True
                r.log("  [block %d] %s" % (i, m.group(0)
                                           .replace("\n", " ")
                                           .strip()))
        if not found_any:
            r.log("  no .ashx reference found in any script block — "
                 "checking for other chart-data call patterns "
                 "(fetch/ajax/XMLHttpRequest/symbol=)")
            for i, blk in enumerate(blocks):
                for m in _re2.finditer(
                        r".{0,150}(?:XMLHttpRequest|\$\.ajax|"
                        r"fetch\(|symbol\s*[:=]|Highcharts\."
                        r"chart).{0,150}", blk):
                    r.log("  [block %d, generic] %s"
                         % (i, m.group(0).replace("\n", " ")
                            .strip()))

        r.section("3. Look specifically at the biggest block "
                  "(36KB — confirmed chart+data keywords)")
        biggest = max(blocks, key=len) if blocks else ""
        r.log("  length=%d" % len(biggest))
        # print it in readable chunks around any promising keyword
        for kw in ("symbol", "ticker", "chartdata", "getdata",
                  "loadchart", "\"s\":", "'s':"):
            idx = biggest.lower().find(kw.lower())
            if idx >= 0:
                r.log("  found %r at offset %d: ...%s..."
                     % (kw, idx,
                        biggest[max(0, idx - 100):idx + 200]
                        .replace("\n", " ")))

        r.section("4. Re-probe stream.ashx with parameter variants "
                  "informed by whatever the JS scan revealed (falls "
                  "back to educated guesses if JS scan found "
                  "nothing usable)")
        variants = [
            "?s=bofaml-em-corp-plus-sytw",
            "?symbol=bofa-merrill-lynch-private-sector-issuers-"
            "emerging-markets-corporate-plus-sub-index-semi-annual-"
            "yield-to-worst",
            "?s=bofa-merrill-lynch-private-sector-issuers-emerging-"
            "markets-corporate-plus-sub-index-semi-annual-yield-to-"
            "worst&span=max",
            "?s=bofa-merrill-lynch-private-sector-issuers-emerging-"
            "markets-corporate-plus-sub-index-semi-annual-yield-to-"
            "worst&d1=1996-01-01&d2=2023-08-14",
        ]
        for v in variants:
            u = "https://tradingeconomics.com/ws/stream.ashx" + v
            try:
                st2, hd2, body2 = get(u, timeout=15)
                r.log("  %s -> %s bytes=%d" % (v, st2, len(body2)))
                if len(body2) > 10:
                    r.log("    sample: %s"
                         % body2[:300].decode("utf-8", "replace"))
            except urllib.error.HTTPError as e:
                r.log("  %s -> HTTP %s" % (v, e.code))
            except Exception as e:
                r.log("  %s -> %s" % (v, str(e)[:90]))
            time.sleep(0.5)

        r.section("verdict")
        doc = {"as_of": time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                      time.gmtime())}
        try:
            cov = json.loads(s3.get_object(
                Bucket=B,
                Key="data/ice-alt-claims-investig.json")["Body"
                                                        ].read())
        except Exception:
            cov = {}
        cov["te_stream_signature_probe"] = doc
        s3.put_object(Bucket=B, Key="data/ice-alt-claims-investig.json",
                      Body=json.dumps(cov, default=str).encode(),
                      ContentType="application/json")
        if not found_any:
            r.fail("no .ashx call signature found in page JS — "
                  "parameter variants tried blind, see results above")
            sys.exit(1)
        r.ok("call signature evidence found — see log for the real "
            "parameter shape")


if __name__ == "__main__":
    main()
