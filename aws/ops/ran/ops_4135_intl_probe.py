"""ops_4135 — INTL PRICE DISCOVERY: yahoo bulk auth, suffix spot-checks,
and Khalid's actual exchange census from the watchlists artifact."""
import json
import sys
import urllib.request
from collections import Counter
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64)"}


def fetch(url, timeout=25, headers=None):
    try:
        req = urllib.request.Request(url, headers=headers or UA)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read().decode("utf-8", "ignore")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "ignore")[:300]
    except Exception as e:
        return -1, str(e)[:200]


def main():
    with report("4135_intl_probe") as rep:
        rep.heading("ops 4135 — intl price discovery")

        rep.section("A. yahoo v7/quote BULK — crumb wall or open?")
        st, x = fetch("https://query1.finance.yahoo.com/v7/finance/quote"
                      "?symbols=2330.TW,SAP.DE,0700.HK")
        rep.kv(v7_status=st, bytes=len(x))
        if st == 200:
            try:
                res = json.loads(x)["quoteResponse"]["result"]
                for r in res:
                    rep.log(f"  {r.get('symbol')}: "
                            f"{r.get('regularMarketPrice')} "
                            f"{r.get('currency')}")
            except Exception:
                rep.log("  parse miss: " + x[:200])
        else:
            rep.log("  body: " + x[:220])

        rep.section("B. chart endpoint fallback (per-symbol, no crumb)")
        st2, x2 = fetch("https://query1.finance.yahoo.com/v8/finance/chart/"
                        "2330.TW?range=1d&interval=1d")
        ok2 = st2 == 200 and "regularMarketPrice" in x2
        rep.kv(chart_status=st2, has_price=ok2)
        if ok2:
            i = x2.find("regularMarketPrice")
            rep.log("  " + x2[i:i + 60])

        rep.section("C. HIS intl universe — exchange census from watchlists")
        wl = json.loads(s3.get_object(Bucket=BUCKET,
                                      Key="data/tv-watchlists.json")["Body"].read())
        lists = wl.get("lists") or wl.get("watchlists") or []
        ex = Counter()
        seen = set()
        for l in lists:
            for sy in l.get("symbols") or []:
                if ":" in str(sy) and sy not in seen:
                    seen.add(sy)
                    ex[str(sy).split(":")[0]] += 1
        US = {"AMEX", "NASDAQ", "NYSE", "OTC", "BATS", "CBOE", "ARCA"}
        NONEQ = {"ECONOMICS", "FRED", "TVC", "OANDA", "FX_IDC", "FX",
                 "CRYPTOCAP", "COINBASE", "BINANCE", "CME", "CME_MINI",
                 "CBOT", "NYMEX", "COMEX", "ICEUS", "ICEEUR", "EUREX",
                 "SPCFD", "INDEX", "CAPITALCOM", "KRAKEN", "BITSTAMP"}
        intl = {k: v for k, v in ex.items()
                if k not in US and k not in NONEQ}
        rep.kv(distinct_prefixed=len(seen),
               us_equities=sum(v for k, v in ex.items() if k in US),
               intl_candidates=sum(intl.values()))
        for k, v in sorted(intl.items(), key=lambda x3: -x3[1])[:22]:
            rep.log(f"  {v:5d}  {k}")
        rep.ok(f"DISCOVERY — v7={'OPEN' if st == 200 else st} "
               f"chart={'OK' if ok2 else st2} "
               f"intl={sum(intl.values())} across {len(intl)} exchanges")


if __name__ == "__main__":
    main()
