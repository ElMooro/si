"""1162 — Re-smoke EDGAR after baseline-guard fix."""
import json, time, urllib.request, ssl
from datetime import datetime, timezone

EDGAR_URL = "https://ru3djltl3oucvsocjrih37sowu0fxgkm.lambda-url.us-east-1.on.aws/"
REPORT = "aws/ops/reports/1162_edgar_baseline_guard.json"
ctx = ssl.create_default_context()

def smoke(t):
    url = f"{EDGAR_URL}?ticker={t}&refresh=1"
    t0 = time.time()
    try:
        req = urllib.request.Request(url, headers={"User-Agent":"JustHodl-Smoke/1.0"})
        with urllib.request.urlopen(req, timeout=120, context=ctx) as r:
            j = json.loads(r.read())
            return {
                "ticker": t, "http": r.status, "elapsed_s": round(time.time()-t0,2),
                "cik": j.get("cik"),
                "n_buys": j.get("n_buys"),
                "n_sells": j.get("n_sells"),
                "total_dollars_sell": j.get("total_dollars_sell"),
                "prior_dollars_sell": j.get("prior_dollars_sell"),
                "sell_acceleration": j.get("sell_acceleration"),
                "n_csuite_sellers": j.get("n_csuite_sellers"),
                "signal_label": j.get("signal_label"),
                "signal_score": j.get("signal_score"),
                "signal_note": j.get("signal_note"),
            }
    except Exception as e:
        return {"ticker": t, "error": str(e)[:300]}

out = {"started": datetime.now(timezone.utc).isoformat(), "smokes": []}
for t in ["NVDA", "TSLA", "META", "AAPL", "MSFT"]:
    res = smoke(t)
    out["smokes"].append(res)
    if "error" in res:
        print(f"  {t}: ERROR {res['error']}")
    else:
        print(f"  {t}: {res['signal_label']:20s} ({res['signal_score']}/100) "
                f"sells={res['n_sells']:3d} (${res['total_dollars_sell']:>12,.0f})  "
                f"prior=${res['prior_dollars_sell']:>12,.0f}  "
                f"accel={res['sell_acceleration']:>6.2f}×  cs={res['n_csuite_sellers']}")

out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT,"w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1162] DONE")
