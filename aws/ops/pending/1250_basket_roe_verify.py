"""1250 — verify ROE now populates + basket constituent data available."""
import json, urllib.request
from datetime import datetime, timezone
REPORT = "aws/ops/reports/1250_basket_roe_verify.json"
PROXY = "https://justhodl-data-proxy.raafouis.workers.dev"
out = {"started": datetime.now(timezone.utc).isoformat()}
def get(p):
    try:
        req = urllib.request.Request(PROXY + p, headers={"User-Agent": "Mozilla/5.0", "Origin": "https://justhodl.ai"})
        with urllib.request.urlopen(req, timeout=20) as r: return json.loads(r.read().decode())
    except Exception as e: return {"_error": str(e)[:120]}
f = get("/fundamentals?ticker=AAPL")
out["aapl_roe"] = f.get("roe"); out["aapl_netMargin"] = f.get("netMargin"); out["aapl_pe"] = f.get("pe")
# basket inputs: NVDA, MSFT, GOOGL daily closes
for t in ["NVDA","MSFT","GOOGL"]:
    d = get(f"/ohlc?ticker={t}&mult=1&span=day&days=250")
    out[f"{t}_bars"] = d.get("count")
out["finished"] = datetime.now(timezone.utc).isoformat()
open(REPORT, "w").write(json.dumps(out, indent=2, default=str))
print(json.dumps(out, indent=2))
