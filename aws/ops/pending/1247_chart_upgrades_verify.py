"""1247 — verify new chart worker routes (interval /ohlc + /news) return data."""
import json, urllib.request
from datetime import datetime, timezone
REPORT = "aws/ops/reports/1247_chart_upgrades_verify.json"
PROXY = "https://justhodl-data-proxy.raafouis.workers.dev"
out = {"started": datetime.now(timezone.utc).isoformat()}

def get(path):
    try:
        req = urllib.request.Request(PROXY + path, headers={"User-Agent": "Mozilla/5.0", "Origin": "https://justhodl.ai"})
        with urllib.request.urlopen(req, timeout=20) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        return {"_error": str(e)[:120]}

# Intraday hourly
h = get("/ohlc?ticker=AAPL&mult=1&span=hour&days=10")
out["ohlc_hour"] = {"count": h.get("count"), "span": h.get("span"), "err": h.get("_error")}
# Weekly
w = get("/ohlc?ticker=AAPL&mult=1&span=week&days=730")
out["ohlc_week"] = {"count": w.get("count"), "span": w.get("span"), "err": w.get("_error")}
# News
n = get("/news?ticker=NVDA")
nl = n.get("news", [])
out["news"] = {"count": len(nl), "sample": [x.get("title","")[:60] for x in nl[:3]], "err": n.get("_error")}

out["finished"] = datetime.now(timezone.utc).isoformat()
open(REPORT, "w").write(json.dumps(out, indent=2, default=str))
print("[1247]", json.dumps(out, indent=2))
