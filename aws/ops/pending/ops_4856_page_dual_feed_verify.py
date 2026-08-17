"""ops/4856 -- world-card dual-feed verify (hot money from its
own engine).
 G0  both live feeds: global-flows taiwan macro LIVE + hot_money
     MOVED; hot-money.json taiwan LIVE with sums.
 (1) committed HTML: hot-money.json fetch present; order intact.
 (2) served check.
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
PAGE = Path(__file__).resolve().parents[3] / "capital-flow.html"
URL = "https://justhodl.ai/capital-flow.html"
s3 = boto3.client("s3", region_name="us-east-1")
FAILED = []


def sread(key):
    raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def main():
    with report("ops 4856 -- page dual-feed verify") as rep:
        rep.heading("G0. feeds")
        try:
            g = sread("data/global-flows.json")
            h = sread("data/hot-money.json")
            tw_g = (g.get("countries") or {}).get("taiwan") or {}
            tw_h = (h.get("countries") or {}).get("taiwan") or {}
            ok = (tw_g.get("status") == "LIVE"
                  and (tw_g.get("hot_money") or {}).get("status")
                  == "MOVED"
                  and tw_h.get("status") == "LIVE"
                  and isinstance(tw_h.get("sum_5d_bn"),
                                 (int, float)))
            (rep.ok if ok else rep.fail)(
                "  feeds %s (hm 5d %+0.2f)"
                % ("OK" if ok else "BROKEN",
                   tw_h.get("sum_5d_bn") or 0))
            if not ok:
                FAILED.append("feeds")
        except ClientError:
            rep.fail("  feeds unreadable")
            FAILED.append("feeds")
        if FAILED:
            sys.exit(1)

        rep.heading("1. committed HTML")
        html = PAGE.read_text(encoding="utf-8")
        for tok in ("/data/hot-money.json",
                    'id="global-ff-card"', 'id="tic-ff-card"'):
            if tok in html:
                rep.ok("  token %r present" % tok)
            else:
                rep.fail("  token %r MISSING" % tok)
                FAILED.append("tok")
        if html.index('id="tic-ff-card"') \
                < html.index('id="global-ff-card"') \
                < html.index("13F funds"):
            rep.ok("  order intact")
        else:
            rep.fail("  order broken")
            FAILED.append("order")
        if FAILED:
            sys.exit(1)

        rep.heading("2. served (<=8 min)")
        t0 = time.time()
        while time.time() - t0 < 480:
            try:
                req = urllib.request.Request(
                    "%s?t=%d" % (URL, int(time.time())),
                    headers={"User-Agent": "ops-4856",
                             "Cache-Control": "no-cache"})
                with urllib.request.urlopen(req, timeout=45) as r:
                    body = r.read().decode("utf-8", "replace")
                if "/data/hot-money.json" in body:
                    rep.ok("dual-feed card SERVED after %ds"
                           % int(time.time() - t0))
                    break
                rep.log("  settling...")
            except Exception as e:  # noqa: BLE001
                rep.log("  fetch: %s" % str(e)[:60])
            time.sleep(30)
        else:
            rep.fail("not served in 8 min")
            sys.exit(1)

        rep.heading("3. verdict")
        rep.ok("hot money renders from its own engine; macro and "
               "fast layers formally separated")


if __name__ == "__main__":
    main()
