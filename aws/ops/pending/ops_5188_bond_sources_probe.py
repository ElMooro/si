"""ops_5188 -- bond war-room source probe (READ-ONLY): Stooq global 10Y/2Y yields, Japan MOF JGB daily
curve CSV, ^MOVE + ETFs via the worker, the ICE BofA OAS family on FRED, fleet feeds (usd-funding,
eurodollar-plumbing, move-index, auction-desk)."""
import csv
import io
import json
import sys
import urllib.request
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
PROXY = "https://justhodl-data-proxy.raafouis.workers.dev"


def get(url, timeout=25):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) justhodl-ops5188", "Accept": "*/*"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, b""
    except Exception as e:
        return -1, str(e).encode()


with report("ops_5188_bond_sources_probe") as R:
    R.heading("ops 5188 -- bond war-room sources")
    R.section("A. Stooq government bond yields (daily CSV)")
    for sym in ("10ydey.b", "10yity.b", "10yesy.b", "10yfry.b", "10yuky.b", "10yjpy.b", "10yauy.b", "10ycay.b", "10ychy.b", "2ydey.b", "2yity.b", "2yjpy.b", "30ydey.b", "30yjpy.b", "10yusy.b", "2yusy.b", "10ynly.b", "10ypty.b", "10ygry.b", "10ykry.b", "10ycny.b", "10yiny.b", "10ybry.b", "10ymxy.b"):
        st, b = get("https://stooq.com/q/d/l/?s=%s&i=d" % sym)
        rows = list(csv.reader(io.StringIO(b.decode("utf-8", "ignore")))) if st == 200 else []
        ok = len(rows) > 2 and rows[0][:1] == ["Date"]
        R.log("   %-10s -> %s rows=%d last=%s" % (sym, st, len(rows), rows[-1][:5] if ok else b[:80]))
    R.section("B. Japan MOF JGB daily curve")
    st, b = get("https://www.mof.go.jp/english/policy/jgbs/reference/interest_rate/jgbcm.csv")
    txt = b.decode("shift_jis", "ignore") if st == 200 else ""
    lines = txt.splitlines()
    R.log("   jgbcm.csv -> %s lines=%d head=%s last=%s" % (st, len(lines), lines[:2], lines[-1][:120] if lines else None))
    st, b = get("https://www.mof.go.jp/english/policy/jgbs/reference/interest_rate/jgbcme.csv")
    R.log("   jgbcme.csv -> %s bytes=%d head=%s" % (st, len(b), b[:160]))
    R.section("C. worker routes")
    for u in ("/yf-ohlc?symbol=^MOVE&range=1y&interval=1d", "/yf-ohlc?symbol=TLT&range=1y&interval=1d", "/yf-ohlc?symbol=^TNX&range=1y&interval=1d", "/fred?series=DGS10&obs=30", "/fred?series=BAMLH0A0HYM2&obs=30", "/fred?series=BAMLC0A0CM&obs=30", "/fred?series=BAMLH0A3HYC&obs=5", "/fred?series=BAMLEMCBPIOAS&obs=5", "/fred?series=BAMLC0A4CBBB&obs=5", "/fred?series=BAMLH0A1HYBB&obs=5", "/fred?series=BAMLEMPBPUBSICRPIOAS&obs=5", "/fred?series=BAMLHE00EHYIOAS&obs=5", "/fred?series=BAMLEMHBHYCRPIOAS&obs=5", "/fred?series=BAMLC0A1CAAA&obs=5", "/fred?series=IRLTLT01ITM156N&obs=3", "/fred?series=DTWEXBGS&obs=5", "/fred?series=VIXCLS&obs=5", "/fred?series=T10Y2Y&obs=5", "/fred?series=DFII10&obs=5", "/fred?series=T10YIE&obs=5", "/fred?series=SOFR&obs=5", "/fred?series=DTB3&obs=5"):
        st, b = get(PROXY + u)
        try:
            j = json.loads(b)
            bars = j.get("bars") or []
            R.log("   %-48s -> %s bars=%d last=%s" % (u, st, len(bars), {k: bars[-1].get(k) for k in ("date", "time", "close", "value")} if bars else j if isinstance(j, dict) else None))
        except Exception:
            R.log("   %-48s -> %s %s" % (u, st, b[:100]))
    R.section("D. fleet feeds")
    for key in ("data/usd-funding.json", "data/eurodollar-plumbing.json", "data/move-index.json", "data/auction-desk.json", "data/crisis-plumbing.json"):
        try:
            d = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key=key)["Body"].read())
            top = list(d.keys())[:14]
            R.log("   %-32s keys=%s" % (key, top))
            for k in ("composite", "composite_score", "regime", "generated_at", "as_of", "updated_at", "score", "level", "latest"):
                if k in d:
                    R.log("      %s=%s" % (k, str(d[k])[:120]))
        except Exception as e:
            R.log("   %-32s -> %s" % (key, str(e)[:80]))
    R.ok("probe complete")
    if False:
        sys.exit(1)
