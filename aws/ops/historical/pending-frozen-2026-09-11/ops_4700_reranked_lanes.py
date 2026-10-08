"""ops 4700 — Khalid's reranked lanes: multi-series fredgraph (a
"Data List" IS this under the hood, testable live) · Trading Economics
guest:guest (his historically-proven bridge) · Common Crawl Feb-2026
(different infra than Wayback, untested tonight) · one gentle Wayback
Data-List probe.

#1 (release-209/v2) already exhaustively tested in ops 4698 -- literal
404 on the bulk endpoint -- not repeated here without a new URL.
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
TEST_IDS = ["BAMLH0A1HYBB", "BAMLC0A2CAA", "BAMLCC0A2AATRIV"]


def fred_key():
    for pn, dec in (("/justhodl/fred-api-key", True),
                    ("/justhodl/fred/api-key", False)):
        try:
            v = ssm.get_parameter(Name=pn, WithDecryption=dec
                                  )["Parameter"]["Value"]
            if v and len(v) >= 16:
                return v
        except Exception:
            continue
    return ""


def get(url, timeout=25, headers=None):
    h = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/124.0.0.0 Safari/537.36"}
    if headers:
        h.update(headers)
    rq = urllib.request.Request(url, headers=h)
    with urllib.request.urlopen(rq, timeout=timeout) as r:
        return r.status, dict(r.headers), r.read()


def parse_csv(txt, id_col=None):
    lines = [ln for ln in txt.splitlines() if ln.strip()]
    if not lines:
        return []
    out = []
    for ln in lines[1:]:
        p = ln.split(",")
        if p and p[0][:2] in ("19", "20"):
            out.append(p)
    return out


def main():
    with report("4700_reranked_lanes") as r:
        r.heading("ops 4700 — multi-series fredgraph · TE guest:guest "
                  "· Common Crawl Feb-2026 · Wayback Data List")

        r.section("1. LIVE multi-series fredgraph.csv — does a "
                  "Data-List-style comma query bypass the per-series "
                  "cap? (untested tonight, cheap to check)")
        multi = ",".join(TEST_IDS)
        try:
            st, hd, body = get(
                "https://fred.stlouisfed.org/graph/fredgraph.csv"
                "?id=" + multi)
            txt = body.decode("utf-8", "replace")
            rows = parse_csv(txt)
            r.log("  multi-series csv (%s): %d bytes, %d dated rows, "
                 "first=%s last=%s"
                 % (multi, len(body), len(rows),
                    rows[0][0] if rows else None,
                    rows[-1][0] if rows else None))
            r.log("  header: %s" % (txt.splitlines()[0]
                                    if txt.splitlines() else None))
        except Exception as e:
            r.log("  multi-series csv failed: %s" % str(e)[:150])

        r.section("2. Trading Economics guest:guest — the "
                  "historically-proven bridge, tested across "
                  "plausible endpoint shapes")
        te_candidates = [
            ("historical/country/indicator",
             "https://api.tradingeconomics.com/historical/country/"
             "united%20states/indicator/high%20yield%20bond%20spread"
             "?c=guest:guest&f=json"),
            ("historical w/ dates",
             "https://api.tradingeconomics.com/historical/country/"
             "united%20states/indicator/high%20yield%20bond%20spread"
             "?c=guest:guest&d1=2015-01-01&d2=2023-08-14&f=json"),
            ("markets/bonds",
             "https://api.tradingeconomics.com/markets/bonds"
             "?c=guest:guest&f=json"),
            ("search high yield",
             "https://api.tradingeconomics.com/search/high%20yield"
             "?c=guest:guest&f=json"),
            ("indicators list US",
             "https://api.tradingeconomics.com/country/united%20"
             "states?c=guest:guest&f=json"),
        ]
        te_hit = False
        for nm, url in te_candidates:
            try:
                st, hd, body = get(url, timeout=20)
                txt = body.decode("utf-8", "replace")
                has_2017 = "2017-" in txt
                r.log("  [%s] status=%s bytes=%d contains-2017=%s | "
                     "sample=%s"
                     % (nm, st, len(body), has_2017,
                        txt[:220].replace("\n", " ")))
                if has_2017:
                    te_hit = True
                    r.ok("    ^ 2017 dated content present!")
            except urllib.error.HTTPError as e:
                r.log("  [%s] HTTP %s: %s"
                     % (nm, e.code,
                        e.read()[:150].decode("utf-8", "replace")))
            except Exception as e:
                r.log("  [%s] %s" % (nm, str(e)[:120]))
            time.sleep(0.5)

        r.section("3. Common Crawl (Feb 2026) — different "
                  "infrastructure than Wayback, untested tonight")
        cc_hit = False
        try:
            st, hd, body = get(
                "https://index.commoncrawl.org/collinfo.json")
            crawls = json.loads(body)
            feb26 = [c for c in crawls
                    if "2026" in c.get("id", "")
                    and ("02" in c.get("id", "")
                         or "01" in c.get("id", ""))]
            r.log("  crawls near Feb-2026: %s"
                 % [c["id"] for c in feb26][:5])
            target = feb26[0] if feb26 else (crawls[0]
                                             if crawls else None)
            if target:
                cdx = target["cdx-api"]
                for sid in TEST_IDS[:1]:
                    q = (cdx + "?url=fred.stlouisfed.org%2Fdata%2F"
                        + sid + ".txt&output=json")
                    try:
                        st2, hd2, body2 = get(q, timeout=20)
                        lines = [l for l in
                                body2.decode("utf-8", "replace"
                                            ).splitlines() if l]
                        r.log("  CDX (%s) for %s: %d records"
                             % (target["id"], sid, len(lines)))
                        if lines:
                            rec = json.loads(lines[0])
                            r.log("    record: %s" % rec)
                            fn, off, ln2 = (rec.get("filename"),
                                           int(rec.get("offset", 0)),
                                           int(rec.get("length", 0)))
                            if fn:
                                st3, hd3, warc = get(
                                    "https://data.commoncrawl.org/"
                                    + fn, timeout=25,
                                    headers={"Range": "bytes=%d-%d"
                                            % (off, off + ln2 - 1)})
                                wtxt = warc.decode("utf-8", "replace")
                                cc_hit = "1996-" in wtxt or \
                                    "2017-" in wtxt
                                r.log("    WARC fetch: %d bytes, "
                                     "contains 1996/2017 dates=%s"
                                     % (len(warc), cc_hit))
                                r.log("    sample: %s"
                                     % wtxt[-400:].replace(
                                         "\n", " "))
                    except Exception as e:
                        r.log("    CDX query failed: %s"
                             % str(e)[:120])
            else:
                r.log("  no crawl index available")
        except Exception as e:
            r.log("  Common Crawl collinfo failed: %s" % str(e)[:150])

        r.section("4. ONE gentle Wayback Data-List probe (not a "
                  "sweep — archive.org throttle should have cleared "
                  "by now, treated calmly either way)")
        wb_hit = False
        try:
            st, hd, body = get(
                "https://web.archive.org/web/20231001id_/https://"
                "fred.stlouisfed.org/graph/fredgraph.csv?id="
                + multi, timeout=20)
            txt = body.decode("utf-8", "replace")
            rows = parse_csv(txt)
            r.log("  wayback multi-series: %d bytes, %d dated rows"
                 % (len(body), len(rows)))
            if rows and rows[0][0] < "2020":
                wb_hit = True
                r.ok("    ^ deep multi-series data found via "
                    "Wayback!")
        except urllib.error.HTTPError as e:
            r.log("  wayback multi-series: HTTP %s (throttle or "
                 "not archived — not alarming, single gentle try)"
                 % e.code)
        except Exception as e:
            r.log("  wayback multi-series: %s" % str(e)[:120])

        r.section("verdict")
        doc = {"as_of": time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                      time.gmtime()),
               "te_guest_hit": te_hit, "commoncrawl_hit": cc_hit,
               "wayback_datalist_hit": wb_hit}
        try:
            cov = json.loads(s3.get_object(
                Bucket=B,
                Key="data/ice-alt-claims-investig.json")["Body"
                                                        ].read())
        except Exception:
            cov = {}
        cov["reranked_lanes"] = doc
        s3.put_object(Bucket=B, Key="data/ice-alt-claims-investig.json",
                      Body=json.dumps(cov, default=str).encode(),
                      ContentType="application/json")
        any_hit = te_hit or cc_hit or wb_hit
        if not any_hit:
            r.fail("none of the four lanes produced a confirmed "
                  "deep-history hit this pass — see per-lane log for "
                  "exact evidence")
            sys.exit(1)
        r.ok("at least one lane confirmed live: TE=%s CC=%s "
             "Wayback-DataList=%s" % (te_hit, cc_hit, wb_hit))


if __name__ == "__main__":
    main()
