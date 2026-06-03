"""1251 — full QA sweep: every chart-pro worker route + live-page feature markers."""
import json, urllib.request
from datetime import datetime, timezone
REPORT = "aws/ops/reports/1251_chartpro_qa_sweep.json"
PROXY = "https://justhodl-data-proxy.raafouis.workers.dev"
PAGE = "https://justhodl.ai/chart-pro.html"
out = {"started": datetime.now(timezone.utc).isoformat(), "routes": {}, "page_markers": {}}

def get(p, raw=False):
    try:
        req = urllib.request.Request((PROXY+p) if not p.startswith("http") else p,
            headers={"User-Agent": "Mozilla/5.0", "Origin": "https://justhodl.ai"})
        with urllib.request.urlopen(req, timeout=25) as r:
            b = r.read().decode("utf-8", "replace")
            return b if raw else json.loads(b)
    except Exception as e:
        return {"_error": str(e)[:120]}

# ── Worker routes ──
checks = [
    ("quotes",       "/quotes?tickers=AAPL,MSFT,NVDA",                lambda d: len(d.get("tickers",{}))),
    ("ohlc_daily",   "/ohlc?ticker=AAPL&mult=1&span=day&days=250",    lambda d: d.get("count")),
    ("ohlc_hourly",  "/ohlc?ticker=AAPL&mult=1&span=hour&days=10",    lambda d: d.get("count")),
    ("ohlc_weekly",  "/ohlc?ticker=AAPL&mult=1&span=week&days=1825",  lambda d: d.get("count")),
    ("yf_crypto",    "/yf-ohlc?symbol=BTC-USD&range=1y",              lambda d: d.get("count")),
    ("yf_forex",     "/yf-ohlc?symbol=EURUSD=X&range=1y",             lambda d: d.get("count")),
    ("fred",         "/fred?series=DGS10&obs=300",                    lambda d: d.get("count")),
    ("fred_search",  "/fred-search?text=inflation",                   lambda d: len(d.get("series",[]))),
    ("dbnomics",     "/dbnomics-search?q=germany%20gdp",              lambda d: len(d.get("series",[]))),
    ("tv_search",    "/tv-search?text=NVDA",                          lambda d: len(d.get("symbols",[]))),
    ("news",         "/news?ticker=NVDA",                             lambda d: len(d.get("news",[]))),
    ("fundamentals", "/fundamentals?ticker=AAPL",                     lambda d: 1 if d.get("price") else 0),
]
for name, path, count in checks:
    d = get(path)
    if isinstance(d, dict) and "_error" in d:
        out["routes"][name] = {"ok": False, "err": d["_error"]}
    else:
        try: n = count(d)
        except: n = "?"
        out["routes"][name] = {"ok": bool(n), "n": n}

# fundamentals detail
f = get("/fundamentals?ticker=AAPL")
out["fundamentals_detail"] = {k: f.get(k) for k in ["pe","pb","roe","netMargin","beta","dividendYield"]}

# ── Live page feature markers ──
html = get(PAGE, raw=True)
if isinstance(html, str) and len(html) > 1000:
    markers = {
        "header_search": 'id="search-input"', "live_dot": 'id="live-indicator"',
        "timeframe_bar": 'class="tf-bar"', "change_modes": 'data-chg="yoy"',
        "indicators": 'id="indicators-btn"', "ratio": 'id="ratio-btn"',
        "seasonality": 'id="season-btn"', "basket": 'id="basket-btn"',
        "correlation": 'id="corr-btn"', "chart_sync": 'id="sync-btn"',
        "alert_center": 'id="alertcenter-btn"', "ai_read": 'id="ai-read-btn"',
        "price_alert": 'id="price-alert-btn"', "draw": 'id="draw-btn"',
        "setups_board": 'id="setups-drawer"', "stat_block": 'id="statblock-btn"',
        "fundamentals_tab": 'data-info-tab="fund"', "news_tab": 'data-info-tab="news"',
        "live_quotes_js": "class LiveQuotes", "basket_risk": "computeBasketRisk",
        "chartsync_js": "class ChartSync", "alertcenter_js": "class AlertCenter",
    }
    for k, m in markers.items():
        out["page_markers"][k] = m in html
    out["page_size_kb"] = round(len(html)/1024, 1)
else:
    out["page_markers"]["_error"] = "page fetch failed (sandbox may block justhodl.ai)"

# summary
route_ok = sum(1 for v in out["routes"].values() if v.get("ok"))
marker_ok = sum(1 for v in out["page_markers"].values() if v is True)
out["summary"] = {"routes_ok": f"{route_ok}/{len(checks)}", "markers_ok": f"{marker_ok}/{len(out['page_markers'])}"}
out["finished"] = datetime.now(timezone.utc).isoformat()
open(REPORT, "w").write(json.dumps(out, indent=2, default=str))
print(json.dumps(out, indent=2))
