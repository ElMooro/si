"""1248 — verify /fundamentals route + ratio data availability."""
import json, urllib.request
from datetime import datetime, timezone
REPORT = "aws/ops/reports/1248_chart_views_verify.json"
PROXY = "https://justhodl-data-proxy.raafouis.workers.dev"
out = {"started": datetime.now(timezone.utc).isoformat()}
def get(p):
    try:
        req = urllib.request.Request(PROXY + p, headers={"User-Agent": "Mozilla/5.0", "Origin": "https://justhodl.ai"})
        with urllib.request.urlopen(req, timeout=20) as r: return json.loads(r.read().decode())
    except Exception as e: return {"_error": str(e)[:120]}
f = get("/fundamentals?ticker=AAPL")
out["fundamentals"] = {"name": f.get("name"), "pe": f.get("pe"), "mcap": f.get("marketCap"), "sector": f.get("sector"), "roe": f.get("roe"), "err": f.get("_error")}
# ratio needs two equity series — both via /ohlc
a = get("/ohlc?ticker=NVDA&mult=1&span=day&days=250"); b = get("/ohlc?ticker=SPY&mult=1&span=day&days=250")
out["ratio_inputs"] = {"nvda_bars": a.get("count"), "spy_bars": b.get("count")}
out["finished"] = datetime.now(timezone.utc).isoformat()
open(REPORT, "w").write(json.dumps(out, indent=2, default=str))
print(json.dumps(out, indent=2))
