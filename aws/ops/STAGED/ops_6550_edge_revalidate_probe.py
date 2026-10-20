"""ops 6550 - read-only proof that the data-proxy's revalidated edge copy works on the live site (ops 6551 deploy).

Why: 58 GB/day of the bucket's egress is the worker re-reading the same 1-15 MB files from S3.  The worker now keeps
a copy per Cloudflare location and reuses it only when S3 answers a conditional GET with 304.  This probe checks, from
the GitHub runner (a real client on the real internet):
  1. for the biggest files: HEAD twice through https://justhodl.ai/data/<key> - the second answer must say
     X-Edge-Cache: REVALIDATED (a 304 from S3, no body) while Cache-Control stays no-store;
  2. never stale: the ETag the site returns equals the ETag S3 itself returns for the object at that moment;
  3. ?exact=1 reads never use a copy (no REVALIDATED);
  4. the bucket still refuses a Lightpanda user agent (ops 6549) and serves a browser.
READ-ONLY.  Each file is read from S3 at most twice (the first MISS, plus a conditional GET): < 100 MB, < $0.01.
"""
from pathlib import Path
import json
import sys
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))

SITE = "https://justhodl.ai"
S3 = "https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com"
KEYS = ["data/share-flows.json", "data/best-setups.json", "data/forensic-screen.json",
        "data/provider-fund-flow-research.json", "data/dark-pool.json", "data/futures-research.json",
        "data/estimate-revisions.json", "data/buyback-engine.json"]
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36 jh-ops-6550"


def head(url, ua=UA):
    req = urllib.request.Request(url, method="HEAD", headers={"User-Agent": ua, "Cache-Control": "no-cache"})
    try:
        with urllib.request.urlopen(req, timeout=45) as r:
            return r.status, {k.lower(): v for k, v in r.getheaders()}
    except urllib.error.HTTPError as e:
        return e.code, {k.lower(): v for k, v in e.headers.items()}
    except Exception as e:  # noqa: BLE001 - recorded, not fatal
        return "ERR:%s" % type(e).__name__, {}


def main():
    from ops_report import report
    with report("ops_6550_edge_revalidate_probe") as r:
        r.heading("ops 6550 - live proof of the revalidated edge copy (read-only)")
        bad = []
        for key in KEYS:
            s1, h1 = head("%s/%s" % (SITE, key))
            time.sleep(1.0)
            s2, h2 = head("%s/%s" % (SITE, key))
            s3, h3 = head("%s/%s" % (S3, key))
            s4, h4 = head("%s/%s?exact=1" % (SITE, key))
            site_etag, s3_etag = h2.get("etag"), h3.get("etag")
            r.log("%s | first %s %s | second %s %s cc=%s | etag site=%s s3=%s | exact %s %s" % (
                key, s1, h1.get("x-edge-cache"), s2, h2.get("x-edge-cache"), h2.get("cache-control"),
                (site_etag or "-")[:14], (s3_etag or "-")[:14], s4, h4.get("x-edge-cache")))
            if s2 == 200:
                if h2.get("x-edge-cache") != "REVALIDATED":
                    bad.append("%s second answer was %s, not REVALIDATED" % (key, h2.get("x-edge-cache")))
                if h2.get("cache-control") != "no-store":
                    bad.append("%s Cache-Control %s" % (key, h2.get("cache-control")))
                if site_etag and s3_etag and site_etag != s3_etag:
                    bad.append("%s STALE: site etag %s != S3 etag %s" % (key, site_etag, s3_etag))
            elif s2 in (403, 404):
                r.log("  (%s not served: %s - skipped)" % (key, s2))
            else:
                bad.append("%s second answer status %s" % (key, s2))
            if h4.get("x-edge-cache") == "REVALIDATED":
                bad.append("%s ?exact=1 used a revalidated copy" % key)
        bucket = {ua: head("%s/data/master-ranker.json" % S3, ua)[0] for ua in ("Lightpanda/1.0", UA)}
        r.log("bucket: Lightpanda -> %s (want 403), browser -> %s (want 200)" % (bucket["Lightpanda/1.0"], bucket[UA]))
        if bucket["Lightpanda/1.0"] != 403 or bucket[UA] != 200:
            bad.append("bucket policy answers %s" % bucket)
        if bad:
            for b in bad:
                r.fail(b)
            raise SystemExit(1)
        r.ok("second reads are REVALIDATED, never stale, exact reads bypass, Lightpanda refused")


if __name__ == "__main__":
    main()
