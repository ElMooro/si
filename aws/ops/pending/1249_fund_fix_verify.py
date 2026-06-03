"""1249 — verify fundamentals field fix (ROE/margins now populate)."""
import json, urllib.request
from datetime import datetime, timezone
REPORT = "aws/ops/reports/1249_fund_fix_verify.json"
PROXY = "https://justhodl-data-proxy.raafouis.workers.dev"
out = {"started": datetime.now(timezone.utc).isoformat()}
def get(p):
    try:
        req = urllib.request.Request(PROXY + p, headers={"User-Agent": "Mozilla/5.0", "Origin": "https://justhodl.ai"})
        with urllib.request.urlopen(req, timeout=20) as r: return json.loads(r.read().decode())
    except Exception as e: return {"_error": str(e)[:120]}
f = get("/fundamentals?ticker=AAPL")
out["aapl"] = {k: f.get(k) for k in ["pe","pb","ps","roe","netMargin","dividendYield","debtToEquity","grossMargin","beta"]}
out["finished"] = datetime.now(timezone.utc).isoformat()
open(REPORT, "w").write(json.dumps(out, indent=2, default=str))
print(json.dumps(out, indent=2))
