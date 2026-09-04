"""ops_5190 -- why did the TradingView socket refuse (400) and what fallbacks exist (READ-ONLY):
tv-bars engine log tail, TV scanner REST endpoint, MOF JGB history CSV, Bundesbank daily 10Y, ECB YC daily."""
import json
import sys
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

logs = boto3.client("logs", region_name="us-east-1")


def http(url, data=None, headers=None, timeout=25):
    req = urllib.request.Request(url, data=data, headers=dict({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/128 Safari/537.36", "Accept": "*/*"}, **(headers or {})))
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()[:300] if e.fp else b""
    except Exception as e:
        return -1, str(e).encode()


with report("ops_5190_tv_fallback_probe") as R:
    R.heading("ops 5190 -- TradingView refusal + fallbacks")
    R.section("A. tv-bars engine recent log lines")
    try:
        ev = logs.filter_log_events(logGroupName="/aws/lambda/justhodl-tv-bars", startTime=int((datetime.now(timezone.utc) - timedelta(hours=48)).timestamp() * 1000),
                                    filterPattern="?refused ?handshake ?banked ?pulled ?symbol_error ?bars", limit=12)
        for e in ev.get("events", [])[-12:]:
            R.log("   %s %s" % (datetime.fromtimestamp(e["timestamp"] / 1000, tz=timezone.utc).strftime("%m-%d %H:%M"), e["message"].strip()[:200]))
        if not ev.get("events"):
            R.log("   (no matching log lines in 48h)")
    except Exception as e:
        R.warn("   logs: %s" % str(e)[:120])
    R.section("B. TradingView scanner REST (no auth)")
    body = json.dumps({"symbols": {"tickers": ["TVC:US10Y", "TVC:DE10Y", "TVC:IT10Y", "TVC:ES10Y", "TVC:FR10Y", "TVC:GB10Y", "TVC:JP10Y", "TVC:JP02Y", "TVC:JP30Y", "TVC:AU10Y", "TVC:CA10Y", "TVC:CN10Y", "TVC:KR10Y", "TVC:IN10Y", "TVC:BR10Y", "TVC:MX10Y", "TVC:DE02Y", "TVC:IT02Y", "TVC:GB02Y", "TVC:CH10Y", "TVC:NL10Y", "TVC:PT10Y", "TVC:GR10Y", "TVC:US02Y", "TVC:US30Y", "TVC:MOVE"], "query": {"types": []}},
                       "columns": ["close", "change", "change_abs", "open", "high", "low", "update_mode", "description"]}).encode()
    for ep in ("https://scanner.tradingview.com/global/scan", "https://scanner.tradingview.com/bonds/scan", "https://scanner.tradingview.com/america/scan"):
        st, b = http(ep, data=body, headers={"Content-Type": "application/json", "Origin": "https://www.tradingview.com", "Referer": "https://www.tradingview.com/"})
        try:
            j = json.loads(b)
            rows = j.get("data") or []
            R.log("   %s -> %s rows=%d sample=%s" % (ep, st, len(rows), [(r["s"], r["d"][:3]) for r in rows[:6]]))
        except Exception:
            R.log("   %s -> %s %s" % (ep, st, b[:160]))
    R.section("C. MOF JGB history CSV")
    for u in ("https://www.mof.go.jp/english/policy/jgbs/reference/interest_rate/historical/jgbcme_all.csv", "https://www.mof.go.jp/jgbs/reference/interest_rate/data/jgbcm_all.csv", "https://www.mof.go.jp/english/policy/jgbs/reference/interest_rate/jgbcme_all.csv"):
        st, b = http(u)
        R.log("   %s -> %s bytes=%d tail=%s" % (u[-60:], st, len(b), b[-120:] if st == 200 else b[:80]))
    R.section("D. Bundesbank + ECB daily")
    for u in ("https://api.statistiken.bundesbank.de/rest/data/BBSSY/D.REN.EUR.A630.000000WT1010.A?lastNObservations=5&format=csv",
              "https://api.statistiken.bundesbank.de/rest/data/BBSSY/D.REN.EUR.A630.000000WT1010.A?lastNObservations=5&format=sdmx_json",
              "https://data-api.ecb.europa.eu/service/data/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_10Y?lastNObservations=3&format=csvdata",
              "https://data-api.ecb.europa.eu/service/data/YC/B.U2.EUR.4F.G_N_C.SV_C_YM.SR_10Y?lastNObservations=3&format=csvdata",
              "https://data-api.ecb.europa.eu/service/data/FM/B.U2.EUR.4F.BB.U2_10Y.YLD?lastNObservations=3&format=csvdata",
              "https://data-api.ecb.europa.eu/service/data/FM/B.IT.EUR.4F.BB.IT_10Y.YLD?lastNObservations=3&format=csvdata",
              "https://data-api.ecb.europa.eu/service/data/IRS/M.IT.L.L40.CI.0000.EUR.N.Z?lastNObservations=2&format=csvdata"):
        st, b = http(u, headers={"Accept": "text/csv, application/json"})
        R.log("   %s -> %s %s" % (u.split("/service/data/")[-1][:70] if "ecb" in u else u.split("/rest/data/")[-1][:60], st, b[:200].decode("utf-8", "ignore").replace("\n", " | ")))
    R.ok("probe complete")
    if False:
        sys.exit(1)
