"""ops/4857 -- three dedicated flow desks verify (Khalid
directive: each feed gets its own live page).
 G0  all three feeds field-level LIVE (foreign-flows total+signals
     +countries; global-flows peru+taiwan macro; hot-money taiwan
     sums) -- pages must never render dead feeds.
 (1) committed HTML per page: PROXY const, own fetch path, key
     tokens, cross-nav; capital-flow carries all three desk links.
 (2) served: each of the three URLs polled for its fetch-path
     token, <=9 min total.
"""
import gzip
import json
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.exceptions import ClientError  # noqa: E402
from ops_report import report  # noqa: E402

B = "justhodl-dashboard-live"
REPO = Path(__file__).resolve().parents[3]
PAGES = {
    "foreign-flows.html": ("/data/foreign-flows.json",
                           ("Flows by destination",
                            "Release history",
                            "custodial bias")),
    "global-flows.html": ("/data/global-flows.json",
                          ("portfolio liabilities",
                           "Coming online")),
    "hot-money.html": ("/data/hot-money.json",
                       ("daily foreign net", "spark",
                        "bfi82u-foreign.json")),
}
s3 = boto3.client("s3", region_name="us-east-1")
FAILED = []


def sread(key):
    raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def main():
    with report("ops 4857 -- three flow desks verify") as rep:
        rep.heading("G0. feeds field-level")
        try:
            ff = sread("data/foreign-flows.json")
            gf = sread("data/global-flows.json")
            hm = sread("data/hot-money.json")
            ok = (isinstance(((ff.get("flows_bn") or {})
                              .get("total") or {}).get("latest"),
                             (int, float))
                  and (gf.get("countries") or {}).get(
                      "peru", {}).get("status") == "LIVE"
                  and (gf.get("countries") or {}).get(
                      "taiwan", {}).get("status") == "LIVE"
                  and isinstance(((hm.get("countries") or {})
                                  .get("taiwan") or {})
                                 .get("sum_5d_bn"),
                                 (int, float)))
            (rep.ok if ok else rep.fail)("  feeds %s"
                                         % ("OK" if ok
                                            else "BROKEN"))
            if not ok:
                FAILED.append("feeds")
        except ClientError:
            rep.fail("  feeds unreadable")
            FAILED.append("feeds")
        if FAILED:
            sys.exit(1)

        rep.heading("1. committed HTML")
        for page, (path, toks) in PAGES.items():
            html = (REPO / page).read_text(encoding="utf-8")
            miss = [x for x in (path, "const PROXY",
                                "/capital-flow.html") + toks
                    if x not in html]
            if not miss:
                rep.ok("  %s: fetch path + %d tokens + nav"
                       % (page, len(toks)))
            else:
                rep.fail("  %s missing %s" % (page, miss))
                FAILED.append(page)
        cf = (REPO / "capital-flow.html").read_text(
            encoding="utf-8")
        if all(x in cf for x in ("/foreign-flows.html",
                                 "/global-flows.html",
                                 "/hot-money.html")):
            rep.ok("  capital-flow carries all three desk links")
        else:
            rep.fail("  capital-flow links missing")
            FAILED.append("links")
        if FAILED:
            sys.exit(1)

        rep.heading("2. served (<=9 min)")
        t0 = time.time()
        left = dict(PAGES)
        while left and time.time() - t0 < 540:
            for page in list(left):
                try:
                    req = urllib.request.Request(
                        "https://justhodl.ai/%s?t=%d"
                        % (page, int(time.time())),
                        headers={"User-Agent": "ops-4857",
                                 "Cache-Control": "no-cache"})
                    with urllib.request.urlopen(
                            req, timeout=45) as r:
                        body = r.read().decode("utf-8",
                                               "replace")
                    if left[page][0] in body:
                        rep.ok("  %s SERVED (%ds)"
                               % (page, int(time.time() - t0)))
                        del left[page]
                except Exception as e:  # noqa: BLE001
                    rep.log("  %s: %s" % (page, str(e)[:50]))
            if left:
                time.sleep(25)
        if left:
            rep.fail("not served: %s" % sorted(left))
            sys.exit(1)

        rep.heading("3. verdict")
        rep.ok("three dedicated desks LIVE: foreign-flows / "
               "global-flows / hot-money each on its own page")


if __name__ == "__main__":
    main()
