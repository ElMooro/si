"""ops 4703 — Khalid's screenshot: TE's public LIVE page shows 26yr
history for an EM sub-index ICE series (one of the "44 unfound"
tonight), with visible Export/API buttons. This is Markets territory,
not the Indicators endpoint I tested (4701/4702) -- different TE
product area, explains why my category scan never surfaced it.

Live site, not archive.org -- no throttle concern. Fetch the exact
page, look for embedded chart-hydration JSON (the modern-site pattern
that worked for finding Hiddenmetrix's 404, and could work here too),
and probe the page's own Export/API mechanisms directly.
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
ssm = boto3.client("ssm", region_name="us-east-1")
PAGE_URL = ("https://tradingeconomics.com/united-states/"
           "bofa-merrill-lynch-private-sector-issuers-emerging-"
           "markets-corporate-plus-sub-index-semi-annual-yield-to-"
           "worst-fed-data.html")


def get(url, timeout=25, headers=None):
    h = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/124.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,*/*"}
    if headers:
        h.update(headers)
    rq = urllib.request.Request(url, headers=h)
    with urllib.request.urlopen(rq, timeout=timeout) as r:
        return r.status, dict(r.headers), r.read()


def main():
    with report("4703_te_live_page") as r:
        r.heading("ops 4703 — TE live public page (Khalid's "
                  "screenshot) — Markets territory, not Indicators")

        r.section("1. Fetch the exact page")
        try:
            st, hd, body = get(PAGE_URL)
        except urllib.error.HTTPError as e:
            r.log("  exact URL HTTP %s — trying without '.h' cutoff "
                 "variants" % e.code)
            body = b""
            for alt in (PAGE_URL[:-5], PAGE_URL.rstrip(".html")
                       + ".html"):
                try:
                    st, hd, body = get(alt)
                    r.log("  alt worked: %s" % alt)
                    break
                except Exception:
                    continue
        if not body:
            r.fail("  could not fetch the page under any URL variant")
            sys.exit(1)
        txt = body.decode("utf-8", "replace")
        r.log("  fetched %d bytes, status=%s" % (len(body), st))

        r.section("2. Hunt for embedded chart data (hydration JSON)")
        # Look for large numeric-date arrays anywhere in the page —
        # the pattern that would feed a client-side chart.
        date_like = _re2.findall(r"(19|20)\d{2}-\d{2}-\d{2}", txt)
        r.log("  raw date-like substrings found in HTML: %d "
             "(sample: %s)"
             % (len(date_like),
                _re2.findall(r"(?:19|20)\d{2}-\d{2}-\d{2}",
                            txt)[:10]))
        unix_ts = _re2.findall(r'"\d{10}"|\[\d{10},', txt)
        r.log("  unix-timestamp-like tokens: %d (sample: %s)"
             % (len(unix_ts), unix_ts[:8]))
        script_blocks = _re2.findall(
            r"<script[^>]*>(.*?)</script>", txt, _re2.S)
        r.log("  <script> blocks on page: %d" % len(script_blocks))
        biggest = max(script_blocks, key=len) if script_blocks else ""
        r.log("  biggest script block: %d chars, contains "
             "'chart'=%s contains 'data'=%s"
             % (len(biggest), "chart" in biggest.lower(),
                "data" in biggest.lower()))

        r.section("3. Locate the Export/API buttons' real targets")
        exports = _re2.findall(
            r'href=["\']([^"\']*(?:export|download)[^"\']*)["\']',
            txt, _re2.I)
        apis = _re2.findall(
            r'href=["\']([^"\']*api[^"\']*)["\']', txt, _re2.I)
        onclicks = _re2.findall(
            r'(?:onclick|data-[a-z-]+)=["\']([^"\']{0,120}'
            r'(?:export|download|xhr|fetch)[^"\']{0,60})["\']',
            txt, _re2.I)
        r.log("  export/download hrefs: %s" % exports[:10])
        r.log("  api hrefs: %s" % apis[:10])
        r.log("  export-related onclick/data-* attrs: %s"
             % onclicks[:10])

        r.section("4. Direct XHR-style probe — TE's own chart-data "
                  "endpoint convention")
        page_slug = PAGE_URL.rstrip("/").rsplit("/", 1)[-1].replace(
            "-fed-data.html", "").replace("-fed-data", "")
        candidates = [
            "https://markets.tradingeconomics.com/chart?s=" +
            page_slug,
            "https://tradingeconomics.com/chart/" + page_slug,
            "https://tradingeconomics.com/ws/stream.ashx?s=" +
            page_slug,
        ]
        for u in candidates:
            try:
                st2, hd2, body2 = get(u, timeout=15)
                r.log("  %s -> %s bytes=%d ct=%s"
                     % (u, st2, len(body2),
                        hd2.get("Content-Type", "")[:40]))
                if len(body2) > 200:
                    r.log("    sample: %s"
                         % body2[:200].decode("utf-8", "replace"))
            except urllib.error.HTTPError as e:
                r.log("  %s -> HTTP %s" % (u, e.code))
            except Exception as e:
                r.log("  %s -> %s" % (u, str(e)[:90]))
            time.sleep(0.4)

        r.section("verdict")
        doc = {"as_of": time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                      time.gmtime()),
               "page_fetched": bool(body), "page_bytes": len(body),
               "date_strings_found": len(date_like),
               "export_hrefs": exports[:10], "api_hrefs": apis[:10]}
        try:
            cov = json.loads(s3.get_object(
                Bucket=B,
                Key="data/ice-alt-claims-investig.json")["Body"
                                                        ].read())
        except Exception:
            cov = {}
        cov["te_live_page_probe"] = doc
        s3.put_object(Bucket=B, Key="data/ice-alt-claims-investig.json",
                      Body=json.dumps(cov, default=str).encode(),
                      ContentType="application/json")
        r.ok("page inspected — see log for embedded data / real "
             "export mechanism, if any")


if __name__ == "__main__":
    main()
