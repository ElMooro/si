"""ops 1082 — find usable ETF earnings yield + dividend yield sources.

ops 1081 proved /stable/ratios-ttm is stocks-only.
Profile endpoint works for all ETFs — inspect its fields.
Also test the SPX index symbol (^SPX) for trailing PE, and FRED for SP500.
"""
import json, os, urllib.request, urllib.error
from datetime import datetime, timezone

REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())
FMP_KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
FRED_KEY = "2f057499936072679d8843d7fce99989"


def fmp(path, symbol=None, extra=""):
    if symbol:
        url = f"https://financialmodelingprep.com/stable/{path}?symbol={symbol}&apikey={FMP_KEY}{extra}"
    else:
        url = f"https://financialmodelingprep.com/stable/{path}?apikey={FMP_KEY}{extra}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/ProfileDebug"})
        with urllib.request.urlopen(req, timeout=12) as r:
            return json.loads(r.read().decode("utf-8", errors="replace"))
    except Exception as e:
        return {"err": str(e)[:200]}


def fred(series):
    """Return latest float."""
    try:
        url = f"https://api.stlouisfed.org/fred/series/observations?series_id={series}&api_key={FRED_KEY}&file_type=json&sort_order=desc&limit=5"
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/RatioDebug"})
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read())
        for obs in data.get("observations", []):
            v = obs.get("value", ".")
            if v not in (".", "", None):
                return {"value": float(v), "date": obs.get("date")}
        return {"err": "no data"}
    except Exception as e:
        return {"err": str(e)[:200]}


def main():
    report = {"started_at": datetime.now(timezone.utc).isoformat()}

    # 1. /stable/profile for each ETF — see what fields are available
    etf_symbols = ["SPY", "QQQ", "IWM", "EFA", "EEM", "VNQ", "GLD", "TLT", "IEF", "BIL", "HYG", "LQD", "TIP", "DBC"]
    report["profile_full"] = {}
    for sym in etf_symbols:
        r = fmp("profile", sym)
        if isinstance(r, list) and r:
            report["profile_full"][sym] = r[0]
        else:
            report["profile_full"][sym] = {"raw": str(r)[:200]}

    # 2. /stable/quote with detail
    report["quote_full"] = {}
    for sym in ["SPY", "QQQ", "VNQ", "AAPL"]:
        r = fmp("quote", sym)
        if isinstance(r, list) and r:
            report["quote_full"][sym] = r[0]

    # 3. Index symbols
    report["index_attempts"] = {}
    for sym in ["^SPX", "SPX", "^GSPC", "^IXIC", "^NDX", "^RUT"]:
        r = fmp("quote", sym)
        report["index_attempts"][sym] = {"len": len(r) if isinstance(r, list) else 0, "first": r[0] if isinstance(r, list) and r else r}

    # 4. dividend-calendar already shipped from earlier — let's also test directly
    report["dividends_etf"] = {}
    for sym in ["SPY", "VNQ"]:
        r = fmp("dividends", sym, "&limit=4")
        report["dividends_etf"][sym] = {"len": len(r) if isinstance(r, list) else 0, "sample": r[:2] if isinstance(r, list) else r}

    # 5. Direct FRED queries for SP500 trailing PE, dividend yield, etc.
    report["fred"] = {
        "SP500": fred("SP500"),  # S&P 500 level
        "SP500_PE": fred("MULTPL/SP500_PE_RATIO_MONTH"),  # might not work
        "DGS10": fred("DGS10"),
    }
    # Try alternative names:
    # FRED has the Shiller CAPE under "PE/CAPE" but it's not a free public series — skip

    out = os.path.join(REPO_ROOT, "aws/ops/reports/1082.json")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w") as f:
        json.dump(report, f, indent=2, default=str)

    # Concise summary
    print("=" * 60)
    print("PROFILE FIELDS for SPY (top 5 ETFs sampled):")
    if "SPY" in report["profile_full"]:
        for k in list(report["profile_full"]["SPY"].keys())[:30]:
            v = report["profile_full"]["SPY"][k]
            print(f"  {k}: {str(v)[:60]}")
    print("\nQUOTE FIELDS for SPY:")
    if "SPY" in report["quote_full"]:
        for k in list(report["quote_full"]["SPY"].keys())[:30]:
            v = report["quote_full"]["SPY"][k]
            print(f"  {k}: {str(v)[:60]}")
    print("\nINDEX ATTEMPTS:")
    for sym, info in report["index_attempts"].items():
        print(f"  {sym}: len={info['len']}")
    print("\nFRED:")
    for k, v in report["fred"].items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
