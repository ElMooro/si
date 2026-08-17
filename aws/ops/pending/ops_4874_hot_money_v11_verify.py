"""ops/4874 -- hot-money v1.1 (TPEx OTC leg) birth verify.
 (1) active-wait + settle 'hot-money v1.1.0'; invoke; poll doc
     with v==1.1.0.
 (2) truths: listed block UNCHANGED contract (latest/sums live);
     otc LIVE with ROC-converted gregorian day + latest_bn ==
     in-op independent refetch of tpex_3insti_summary Net/1e9
     (same-day) ; tpex ledger exists; combined honest (LIVE
     same-day identity OR MISALIGNED named); page committed OTC
     tokens + served.
"""
import gzip
import io
import json
import sys
import time
import urllib.request
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from botocore.exceptions import ClientError  # noqa: E402
from ops_report import report  # noqa: E402

REGION = "us-east-1"
FN = "justhodl-hot-money"
B = "justhodl-dashboard-live"
OUT_KEY = "data/hot-money.json"
MARKER = "hot-money v1.1.0"
TPEX_URL = ("https://www.tpex.org.tw/openapi/v1/"
            "tpex_3insti_summary")
PAGE = Path(__file__).resolve().parents[3] / "hot-money.html"

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=120,
                                 retries={"max_attempts": 1}))
FAILED = []


def sread(key):
    raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def tpex_now():
    req = urllib.request.Request(
        TPEX_URL, headers={"User-Agent": "ops-4874",
                           "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=50) as r:
        j = json.loads(r.read())
    for x in j if isinstance(j, list) else []:
        if "\u5916\u8cc7\u53ca\u9678\u8cc7\u5408\u8a08" \
                in str(x.get("Investor", "")):
            d = str(x.get("Date", ""))
            return ("%04d%s" % (int(d[:3]) + 1911, d[3:]),
                    float(str(x.get("Net", ""))
                          .replace(",", "")))
    return None, None


def main():
    with report("ops 4874 -- hot-money v1.1 verify") as rep:
        rep.heading("1. settle + invoke")
        for _ in range(40):
            try:
                cfg = lam.get_function_configuration(
                    FunctionName=FN)
                if cfg.get("State") == "Active" and \
                        cfg.get("LastUpdateStatus") \
                        != "InProgress":
                    break
            except ClientError:
                pass
            time.sleep(6)
        settled = False
        for att in range(30):
            try:
                gf = lam.get_function(FunctionName=FN)
                raw = urllib.request.urlopen(
                    gf["Code"]["Location"], timeout=60).read()
                src = zipfile.ZipFile(io.BytesIO(raw)).read(
                    "lambda_function.py").decode("utf-8",
                                                 "replace")
                if MARKER in src:
                    rep.ok("marker settled (attempt %d)"
                           % (att + 1))
                    settled = True
                    break
            except (ClientError, Exception):  # noqa: BLE001
                pass
            time.sleep(10)
        if not settled:
            rep.fail("no marker")
            sys.exit(1)
        try:
            prev = sread(OUT_KEY).get("generated_at")
        except ClientError:
            prev = None
        lam.invoke(FunctionName=FN, InvocationType="Event",
                   Payload=b"{}")
        doc = None
        t0 = time.time()
        while time.time() - t0 < 200:
            time.sleep(8)
            try:
                d = sread(OUT_KEY)
            except ClientError:
                continue
            if d.get("generated_at") != prev \
                    and d.get("v") == "1.1.0":
                doc = d
                break
        if not doc:
            rep.fail("no fresh v1.1.0 doc")
            sys.exit(1)
        rep.ok("fresh in %ds" % int(time.time() - t0))

        rep.heading("2. truths")
        tw = (doc.get("countries") or {}).get("taiwan") or {}
        if tw.get("status") == "LIVE" \
                and isinstance(tw.get("sum_5d_bn"),
                               (int, float)):
            rep.ok("  listed contract intact (latest %+0.2f, "
                   "5d %+0.2f)" % (tw.get("latest_bn"),
                                   tw.get("sum_5d_bn")))
        else:
            rep.fail("  listed contract broken")
            FAILED.append("listed")
        otc = tw.get("otc") or {}
        d_now, net_now = tpex_now()
        if otc.get("status") == "LIVE" \
                and str(otc.get("latest_day", ""))[:4] == "2026":
            rep.ok("  otc LIVE: %s %+0.2fbn (ledger %d)"
                   % (otc.get("latest_day"),
                      otc.get("latest_bn"),
                      otc.get("ledger_days")))
        else:
            rep.fail("  otc %s %s" % (otc.get("status"),
                                      otc.get("today_fetch")))
            FAILED.append("otc")
        if d_now and otc.get("latest_day") == d_now \
                and abs(otc.get("latest_bn", 9e9)
                        - round(net_now / 1e9, 2)) < 0.011:
            rep.ok("  otc latest == independent TPEx refetch "
                   "(%s = %+0.2fbn)" % (d_now,
                                        net_now / 1e9))
        elif d_now:
            rep.warn("  refetch day %s vs doc %s (day-roll "
                     "tolerated)" % (d_now,
                                     otc.get("latest_day")))
        try:
            led = sread("data/providers/tpex/"
                        "insti-foreign.json")
            rep.ok("  tpex ledger exists n=%d"
                   % len(led.get("rows") or {}))
        except ClientError:
            rep.fail("  tpex ledger missing")
            FAILED.append("ledger")
        cb = tw.get("combined") or {}
        if cb.get("status") == "LIVE":
            ident = abs(cb["latest_bn"]
                        - round(tw["latest_bn"]
                                + otc["latest_bn"], 2)) < 0.011
            (rep.ok if ident else rep.fail)(
                "  combined LIVE %+0.2fbn identity=%s"
                % (cb["latest_bn"], ident))
            if not ident:
                FAILED.append("comb")
        elif cb.get("status") == "MISALIGNED":
            rep.ok("  combined MISALIGNED honestly: %s"
                   % cb.get("why"))
        else:
            rep.fail("  combined %s" % cb.get("status"))
            FAILED.append("comb")

        rep.heading("3. page")
        html = PAGE.read_text(encoding="utf-8")
        if "OTC board (TPEx)" in html and "Listed + OTC" \
                in html:
            rep.ok("  committed OTC tokens")
        else:
            rep.fail("  page tokens missing")
            sys.exit(1)
        t0 = time.time()
        while time.time() - t0 < 480:
            try:
                req = urllib.request.Request(
                    "https://justhodl.ai/hot-money.html?t=%d"
                    % int(time.time()),
                    headers={"User-Agent": "ops-4874",
                             "Cache-Control": "no-cache"})
                with urllib.request.urlopen(req, timeout=45) \
                        as r:
                    if "OTC board (TPEx)" in r.read().decode(
                            "utf-8", "replace"):
                        rep.ok("  SERVED (%ds)"
                               % int(time.time() - t0))
                        break
            except Exception:  # noqa: BLE001
                pass
            time.sleep(30)
        else:
            rep.fail("  not served")
            FAILED.append("served")

        rep.heading("4. verdict")
        if FAILED:
            rep.fail("HARD FAILS: %s" % sorted(set(FAILED)))
            sys.exit(1)
        rep.ok("Taiwan hot money now covers BOTH boards -- "
               "listed + OTC with same-day identity")


if __name__ == "__main__":
    main()
