"""ops/735 — data-provider entitlement probe.

Hits one representative endpoint per recommended data feed with the live
keys and records whether the plan unlocks it. This tells us exactly what
to build against before writing any Lambda — no point coding to an
endpoint the tier doesn't include.

Keys are never written to the report (URLs are stored key-redacted).
"""
import json, os, urllib.request, urllib.error
from datetime import datetime, timezone

POLY = "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"
FMP = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
AV = "EOLGKSGAYZUXKPUL"
CMC = "17ba8e87-53f0-46f4-abe5-014d9cd99597"

report = {"ops": 735, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "data-provider entitlement probe"}


def fetch(url, headers=None):
    req = urllib.request.Request(url, headers=headers or {
        "User-Agent": "justhodl-ops/735"})
    try:
        with urllib.request.urlopen(req, timeout=25) as r:
            return r.status, r.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace")
    except Exception as e:
        return None, str(e)[:200]


def redact(url):
    for k in (POLY, FMP, AV, CMC):
        url = url.replace(k, "***")
    return url


# (provider, label, url, headers, must_contain) — must_contain proves the
# feature is really present in the payload, not just a 200 with an error body.
PROBES = [
    # ── Polygon ──
    ("polygon", "options snapshot + Greeks/IV (#1 build)",
     f"https://api.polygon.io/v3/snapshot/options/SPY?limit=5&apiKey={POLY}",
     None, "greeks"),
    ("polygon", "daily short volume",
     f"https://api.polygon.io/stocks/v1/short-volume?ticker=AAPL&limit=1&apiKey={POLY}",
     None, "results"),
    ("polygon", "indices snapshot",
     f"https://api.polygon.io/v3/snapshot/indices?ticker=I:SPX&apiKey={POLY}",
     None, "results"),
    ("polygon", "treasury yields",
     f"https://api.polygon.io/fed/v1/treasury-yields?limit=1&apiKey={POLY}",
     None, "results"),
    ("polygon", "dividends (corporate actions)",
     f"https://api.polygon.io/v3/reference/dividends?ticker=AAPL&limit=1&apiKey={POLY}",
     None, "results"),
    ("polygon", "options trades (flow)",
     f"https://api.polygon.io/v3/trades/O:SPY251219C00600000?limit=1&apiKey={POLY}",
     None, "results"),
    # ── FMP /stable/ ──
    ("fmp", "revenue product segmentation (#3)",
     f"https://financialmodelingprep.com/stable/revenue-product-segmentation?symbol=AAPL&apikey={FMP}",
     None, "["),
    ("fmp", "revenue geographic segmentation (#3)",
     f"https://financialmodelingprep.com/stable/revenue-geographic-segmentation?symbol=AAPL&apikey={FMP}",
     None, "["),
    ("fmp", "price target consensus",
     f"https://financialmodelingprep.com/stable/price-target-consensus?symbol=AAPL&apikey={FMP}",
     None, "["),
    ("fmp", "discounted cash flow",
     f"https://financialmodelingprep.com/stable/discounted-cash-flow?symbol=AAPL&apikey={FMP}",
     None, "["),
    ("fmp", "financial scores (Altman/Piotroski)",
     f"https://financialmodelingprep.com/stable/financial-scores?symbol=AAPL&apikey={FMP}",
     None, "["),
    ("fmp", "ratios",
     f"https://financialmodelingprep.com/stable/ratios?symbol=AAPL&limit=1&apikey={FMP}",
     None, "["),
    ("fmp", "insider trading search",
     f"https://financialmodelingprep.com/stable/insider-trading/search?symbol=AAPL&page=0&apikey={FMP}",
     None, "["),
    ("fmp", "senate trades",
     f"https://financialmodelingprep.com/stable/senate-trades?symbol=AAPL&apikey={FMP}",
     None, "["),
    ("fmp", "earnings calendar",
     f"https://financialmodelingprep.com/stable/earnings-calendar?apikey={FMP}",
     None, "["),
    ("fmp", "economic calendar",
     f"https://financialmodelingprep.com/stable/economic-calendar?apikey={FMP}",
     None, "["),
    ("fmp", "ETF sector weightings",
     f"https://financialmodelingprep.com/stable/etf-sector-weightings?symbol=SPY&apikey={FMP}",
     None, "["),
    # ── AlphaVantage (returns 200 even when premium-blocked — inspect body) ──
    ("alphavantage", "intraday equity bars",
     f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=5min&apikey={AV}",
     None, "Time Series"),
    ("alphavantage", "analytics sliding window",
     f"https://www.alphavantage.co/query?function=ANALYTICS_SLIDING_WINDOW&SYMBOLS=AAPL,MSFT&RANGE=2month&INTERVAL=DAILY&OHLC=close&WINDOW_SIZE=20&CALCULATIONS=CORRELATION&apikey={AV}",
     None, "payload"),
    ("alphavantage", "WTI crude commodity",
     f"https://www.alphavantage.co/query?function=WTI&interval=monthly&apikey={AV}",
     None, "data"),
    # ── CoinMarketCap ──
    ("cmc", "crypto categories (narratives)",
     "https://pro-api.coinmarketcap.com/v1/cryptocurrency/categories",
     {"X-CMC_PRO_API_KEY": CMC}, "data"),
    ("cmc", "fear & greed latest",
     "https://pro-api.coinmarketcap.com/v3/fear-and-greed/latest",
     {"X-CMC_PRO_API_KEY": CMC}, "data"),
    ("cmc", "OHLCV historical",
     "https://pro-api.coinmarketcap.com/v2/cryptocurrency/ohlcv/historical?symbol=BTC&count=2",
     {"X-CMC_PRO_API_KEY": CMC}, "data"),
]

results = []
for provider, label, url, headers, must in PROBES:
    status, body = fetch(url, headers)
    body_l = (body or "").lower()
    blocked = any(m in body_l for m in (
        "not authorized", "not_authorized", "premium", "upgrade your plan",
        "exclusive endpoint", "subscription does not", "this is a premium",
        "higher tier", "plan does not", "your current plan"))
    has_signal = bool(must and must.lower() in body_l)
    if status == 200 and has_signal and not blocked:
        verdict = "YES"
    elif status in (401, 402, 403) or blocked:
        verdict = "NO"
    else:
        verdict = "CHECK"
    results.append({
        "provider": provider, "feature": label, "verdict": verdict,
        "status": status, "url": redact(url),
        "body_snippet": (body or "")[:220].replace("\n", " ")})

report["probes"] = results
by = {}
for r in results:
    by.setdefault(r["provider"], {"YES": 0, "NO": 0, "CHECK": 0})
    by[r["provider"]][r["verdict"]] += 1
report["summary_by_provider"] = by
report["entitled"] = [r["feature"] for r in results if r["verdict"] == "YES"]
report["not_entitled"] = [r["feature"] for r in results if r["verdict"] == "NO"]
report["needs_review"] = [r["feature"] for r in results if r["verdict"] == "CHECK"]

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/735_provider_entitlement_probe.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/735_provider_entitlement_probe.json")
