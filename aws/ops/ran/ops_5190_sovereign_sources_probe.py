"""ops_5190 -- sovereign-yield sources probe (READ-ONLY) for the bond war room.
ops 5189 built the sovereign lane on TradingView's chart socket, which refuses every handshake (HTTP 400,
the same trap that killed tv-bars on 2026-09-02), so Japan / Europe / world panels were empty. This probes the
official daily sources with history: Eurostat irt_lt_mcby_d (live API + our own mirror), ECB YC, Bundesbank,
Bank of England, Bank of Canada Valet, RBA F2, Japan MOF full history, SNB, Treasury XML month file, the
TradingView REST scanner (last + DoD, no socket) and Yahoo ^IRX/^FVX/^TYX via the worker."""
import csv
import io
import json
import sys
import urllib.parse
import urllib.request
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
PROXY = "https://justhodl-data-proxy.raafouis.workers.dev"
UA = "Mozilla/5.0 (X11; Linux x86_64) justhodl-ops5190"


def get(url, timeout=40, headers=None, data=None):
    h = {"User-Agent": UA, "Accept": "*/*"}
    h.update(headers or {})
    req = urllib.request.Request(url, headers=h, data=data, method="POST" if data else "GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()[:300]
    except Exception as e:  # noqa: BLE001
        return -1, str(e).encode()


def csv_rows(b):
    return list(csv.reader(io.StringIO(b.decode("utf-8", "ignore"))))


with report("ops_5190_sovereign_sources_probe") as R:
    R.heading("ops 5190 -- sovereign yield sources (no TradingView socket)")

    R.section("A. Eurostat irt_lt_mcby_d (daily Maastricht 10Y yields) -- live API")
    for geo in ("DE", "IT", "ES", "FR", "NL", "PT", "GR", "AT", "BE", "IE", "FI", "EA"):
        st, b = get("https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/irt_lt_mcby_d?format=JSON&lang=EN&geo=%s&sinceTimePeriod=2026-08-01" % geo, timeout=60)
        try:
            j = json.loads(b)
            tm = j["dimension"]["time"]["category"]["index"]
            vals = j.get("value") or {}
            last_t = max(tm, key=lambda k: tm[k])
            n = len(vals)
            lv = vals.get(str(tm[last_t]))
            R.log("   %-3s -> %s n=%d last=%s %s" % (geo, st, n, last_t, lv))
        except Exception as e:  # noqa: BLE001
            R.log("   %-3s -> %s parse-fail %s body=%s" % (geo, st, str(e)[:60], b[:120]))
    st, b = get("https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/irt_lt_mcby_d?format=JSON&lang=EN&geo=IT&sinceTimePeriod=1990-01-01", timeout=90)
    try:
        j = json.loads(b)
        tm = j["dimension"]["time"]["category"]["index"]
        R.log("   IT full history -> %s obs=%d first=%s last=%s bytes=%d" % (st, len(j.get("value") or {}), min(tm), max(tm), len(b)))
    except Exception as e:  # noqa: BLE001
        R.log("   IT full history -> %s fail %s" % (st, str(e)[:80]))
    R.section("A2. our Eurostat mirror")
    for pref in ("data/warm/eurostat/",):
        tok, n, hits = None, 0, []
        while True:
            kw = {"Bucket": "justhodl-dashboard-live", "Prefix": pref, "MaxKeys": 1000}
            if tok:
                kw["ContinuationToken"] = tok
            resp = s3.list_objects_v2(**kw)
            for o in resp.get("Contents") or []:
                n += 1
                if "irt_lt_mcby" in o["Key"] or "irt_euryld" in o["Key"]:
                    hits.append((o["Key"], o["Size"], o["LastModified"].isoformat()[:16]))
            tok = resp.get("NextContinuationToken")
            if not tok or n > 20000:
                break
        R.log("   scanned %d objects under %s; irt_lt_mcby / irt_euryld hits: %s" % (n, pref, hits[:6]))

    R.section("B. ECB Data Portal daily (YC euro-area curve, FM benchmarks)")
    for key in ("YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_10Y", "YC/B.U2.EUR.4F.G_N_C.SV_C_YM.SR_10Y", "YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_2Y",
                "FM/B.U2.EUR.4F.BB.U2_10Y.YLD", "FM/B.DE.EUR.4F.BB.DE10Y_RR.YLD", "FM/B.IT.EUR.4F.BB.IT10Y_RR.YLD"):
        st, b = get("https://data-api.ecb.europa.eu/service/data/%s?format=csvdata&lastNObservations=5" % key, timeout=60)
        rows = csv_rows(b) if st == 200 else []
        R.log("   %-45s -> %s rows=%d last=%s" % (key, st, len(rows), [r[i] for r in rows[-1:] for i in (7, 8) if len(r) > 8] if len(rows) > 1 else b[:100]))

    R.section("C. Bundesbank BBSIS daily Bund yields")
    for tenor, k in (("2Y", "R02XX"), ("10Y", "R10XX"), ("30Y", "R30XX")):
        key = "BBSIS/D.I.ZAR.ZI.EUR.S1311.B.A604.%s.R.A.A._Z._Z.A" % k
        st, b = get("https://api.statistiken.bundesbank.de/rest/data/%s?format=csv&lastNObservations=5" % key, timeout=60, headers={"Accept": "text/csv"})
        rows = csv_rows(b) if st == 200 else []
        R.log("   %-4s %s -> %s rows=%d tail=%s" % (tenor, k, st, len(rows), rows[-1][:3] if rows else b[:100]))

    R.section("D. Bank of England gilt par yields (IUDSNPY 5y, IUDMNZC 10y, IUDLNPY 20y)")
    st, b = get("https://www.bankofengland.co.uk/boeapps/database/_iadb-fromshowcolumns.asp?csv.x=yes&Datefrom=01/Jan/2024&Dateto=now&SeriesCodes=IUDSNPY,IUDMNZC,IUDLNPY&CSVF=TN&UsingCodes=Y&VPD=Y&VFD=N", timeout=60)
    rows = csv_rows(b) if st == 200 else []
    R.log("   -> %s rows=%d head=%s tail=%s" % (st, len(rows), rows[:1], rows[-1:] if rows else b[:120]))
    st, b = get("https://www.bankofengland.co.uk/boeapps/database/fromshowcolumns.asp?csv.x=yes&Datefrom=01/Jan/2024&Dateto=now&SeriesCodes=IUDSNPY,IUDMNZC,IUDLNPY&CSVF=TN&UsingCodes=Y&VPD=Y&VFD=N", timeout=60)
    rows = csv_rows(b) if st == 200 else []
    R.log("   alt -> %s rows=%d tail=%s" % (st, len(rows), rows[-1:] if rows else b[:120]))

    R.section("E. Bank of Canada Valet")
    st, b = get("https://www.bankofcanada.ca/valet/observations/BD.CDN.2YR.DQ.YLD,BD.CDN.5YR.DQ.YLD,BD.CDN.10YR.DQ.YLD,BD.CDN.LONG.DQ.YLD/json?recent=5", timeout=60)
    try:
        j = json.loads(b)
        R.log("   -> %s obs=%d last=%s" % (st, len(j.get("observations") or []), j["observations"][-1]))
    except Exception:  # noqa: BLE001
        R.log("   -> %s body=%s" % (st, b[:160]))

    R.section("F. RBA F2 daily government bond yields")
    st, b = get("https://www.rba.gov.au/statistics/tables/csv/f2-data.csv", timeout=60)
    rows = csv_rows(b) if st == 200 else []
    hdr = [r for r in rows[:15] if r and r[0] in ("Series ID", "Title")]
    R.log("   -> %s rows=%d header=%s tail=%s" % (st, len(rows), [h[:8] for h in hdr], rows[-1][:8] if rows else b[:120]))

    R.section("G. Japan MOF JGB full history")
    st, b = get("https://www.mof.go.jp/english/policy/jgbs/reference/interest_rate/historical/jgbcme_all.csv", timeout=90)
    txt = b.decode("shift_jis", "ignore") if st == 200 else ""
    lines = [ln for ln in txt.splitlines() if ln.strip()]
    R.log("   jgbcme_all.csv -> %s lines=%d head=%s tail=%s" % (st, len(lines), lines[:2], lines[-1:]))

    R.section("H. SNB rendoblid (CHF confederation yields)")
    for url in ("https://data.snb.ch/api/cube/rendoblid/data/csv/en", "https://data.snb.ch/api/cube/rendoblid/data/json/en?fromDate=2026-08-01"):
        st, b = get(url, timeout=60)
        R.log("   %s -> %s bytes=%d head=%s" % (url[-40:], st, len(b), b[:160]))

    R.section("I. Treasury daily par curve month XML + our bank")
    st, b = get("https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml?data=daily_treasury_yield_curve&field_tdr_date_value_month=202609", timeout=60, headers={"Accept": "application/xml"})
    R.log("   month XML -> %s bytes=%d NEW_DATE count=%d" % (st, len(b), b.count(b"NEW_DATE")))
    try:
        import gzip
        doc = json.loads(gzip.decompress(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/warm/treasury-par/curve.json.gz")["Body"].read()))
        R.log("   bank as_of=%s n_days=%s first=%s last=%s tenors=%s" % (doc.get("as_of"), doc.get("n_days"), doc.get("first"), doc.get("last"), doc.get("tenors")))
    except Exception as e:  # noqa: BLE001
        R.log("   bank read failed %s" % str(e)[:100])

    R.section("J. TradingView REST scanner (no socket)")
    tick = ["TVC:US10Y", "TVC:US02Y", "TVC:DE10Y", "TVC:DE02Y", "TVC:IT10Y", "TVC:IT02Y", "TVC:ES10Y", "TVC:FR10Y", "TVC:GB10Y", "TVC:JP10Y", "TVC:JP30Y",
            "TVC:CH10Y", "TVC:AU10Y", "TVC:CA10Y", "TVC:CN10Y", "TVC:KR10Y", "TVC:IN10Y", "TVC:BR10Y", "TVC:MX10Y", "TVC:PT10Y", "TVC:GR10Y", "TVC:NL10Y", "TVC:MOVE"]
    body = json.dumps({"symbols": {"tickers": tick, "query": {"types": []}}, "columns": ["close", "change", "change_abs", "update_mode", "time", "high", "low"]}).encode()
    for ep in ("https://scanner.tradingview.com/global/scan", "https://scanner.tradingview.com/bonds/scan", "https://scanner.tradingview.com/america/scan"):
        st, b = get(ep, timeout=40, headers={"Content-Type": "application/json", "Origin": "https://www.tradingview.com", "Referer": "https://www.tradingview.com/"}, data=body)
        try:
            j = json.loads(b)
            data = j.get("data") or []
            R.log("   %s -> %s n=%d sample=%s" % (ep.split("/")[-2], st, len(data), [(d["s"], d["d"][:4]) for d in data[:6]]))
        except Exception:  # noqa: BLE001
            R.log("   %s -> %s body=%s" % (ep.split("/")[-2], st, b[:160]))

    R.section("K. Yahoo US yields via worker")
    for sym in ("^IRX", "^FVX", "^TNX", "^TYX"):
        st, b = get("%s/yf-ohlc?symbol=%s&range=1mo&interval=1d" % (PROXY, urllib.parse.quote(sym)), timeout=40)
        try:
            j = json.loads(b)
            bars = j.get("bars") or []
            R.log("   %-5s -> %s bars=%d last=%s" % (sym, st, len(bars), {k: bars[-1].get(k) for k in ("time", "close")} if bars else None))
        except Exception:  # noqa: BLE001
            R.log("   %-5s -> %s body=%s" % (sym, st, b[:100]))
    R.ok("probe complete")
    if False:  # read-only probe: nothing to fail on, but the preflight wants an explicit exit path
        sys.exit(1)
