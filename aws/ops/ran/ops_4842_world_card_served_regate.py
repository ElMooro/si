"""ops/4842 -- world card served regate (4840 failed only on
serving: that commit's Pages deploy flaked; the self-heal retry
has since deployed).
 G0  live global-flows.json: taiwan LIVE with macro.series.
     portfolio_liab_total.latest numeric + hot_money sums; peru
     LIVE.
 (1) committed HTML: generic renderer tokens (hot_money, macro,
     FLAG map) + order tic < global < legacy preserved.
 (2) served check both ids.
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
    with report("ops 4842 -- world card served regate") as rep:
        rep.heading("G0. live feed bindings")
        try:
            gf = sread("data/global-flows.json")
            tw = (gf.get("countries") or {}).get("taiwan") or {}
            mac = ((tw.get("macro") or {}).get("series")
                   or {}).get("portfolio_liab_total") or {}
            hm = tw.get("hot_money") or {}
            ok = (tw.get("status") == "LIVE"
                  and isinstance(mac.get("latest"), (int, float))
                  and isinstance(hm.get("latest_bn"),
                                 (int, float))
                  and (gf.get("countries") or {}).get(
                      "peru", {}).get("status") == "LIVE")
            (rep.ok if ok else rep.fail)(
                "  taiwan+peru bindings %s (tw macro %+0.1fM, "
                "hot %.2fbn)" % ("OK" if ok else "BROKEN",
                                 mac.get("latest") or 0,
                                 hm.get("latest_bn") or 0))
            if not ok:
                FAILED.append("gf")
        except ClientError:
            rep.fail("  global-flows.json unreadable")
            FAILED.append("gf")
        if FAILED:
            sys.exit(1)

        rep.heading("1. committed HTML + order")
        html = PAGE.read_text(encoding="utf-8")
        for tok in ('id="global-ff-card"', "hot_money",
                    "const FLAG", "Hot money"):
            if tok in html:
                rep.ok("  token %r present" % tok)
            else:
                rep.fail("  token %r MISSING" % tok)
                FAILED.append("tok")
        it = html.index('id="tic-ff-card"')
        ig = html.index('id="global-ff-card"')
        il = html.index("13F funds")
        if it < ig < il:
            rep.ok("  ORDER preserved (US @%d < world @%d < "
                   "legacy @%d)" % (it, ig, il))
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
                    headers={"User-Agent": "ops-4842",
                             "Cache-Control": "no-cache"})
                with urllib.request.urlopen(req, timeout=45) as r:
                    body = r.read().decode("utf-8", "replace")
                if ('id="global-ff-card"' in body
                        and "const FLAG" in body):
                    rep.ok("generic world card SERVED after %ds"
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
        rep.ok("world card v1.1 live -- Taiwan renders beside "
               "Peru with the hot-money strip")


if __name__ == "__main__":
    main()
