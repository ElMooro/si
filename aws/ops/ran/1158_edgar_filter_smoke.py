"""1158 — Force-refresh EDGAR for NVDA + TSLA + AAPL to verify filter fix."""
import json, time, urllib.request, urllib.error, ssl
from datetime import datetime, timezone

EDGAR_URL = "https://ru3djltl3oucvsocjrih37sowu0fxgkm.lambda-url.us-east-1.on.aws/"
REPORT = "aws/ops/reports/1158_edgar_filter_smoke.json"
ctx = ssl.create_default_context()

def smoke(t):
    url = f"{EDGAR_URL}?ticker={t}&refresh=1"
    t0 = time.time()
    try:
        req = urllib.request.Request(url, headers={"User-Agent":"JustHodl-Smoke/1.0"})
        with urllib.request.urlopen(req, timeout=120, context=ctx) as r:
            j = json.loads(r.read())
            txns = j.get("transactions", [])
            sample = [{"date":x.get("date"),"filer":x.get("filer","")[:30],
                         "role":(x.get("role","") or "")[:25],"dir":x.get("direction"),
                         "code":x.get("code"),"shares":x.get("shares"),
                         "dollars":round(x.get("dollars",0),0)} for x in txns[:5]]
            return {
                "ticker": t, "http": r.status, "elapsed_s": round(time.time()-t0,2),
                "cik": j.get("cik"), "n_filings_90d": j.get("n_filings_90d"),
                "n_filings_parsed_ok": j.get("n_filings_parsed_ok"),
                "n_buys": j.get("n_buys"), "n_sells": j.get("n_sells"),
                "net_dollars_90d": j.get("net_dollars_90d"),
                "signal_label": j.get("signal_label"),
                "signal_score": j.get("signal_score"),
                "cluster_detected": j.get("cluster_detected"),
                "by_filer_n": len(j.get("by_filer", {})),
                "sample_txns": sample,
            }
    except Exception as e:
        return {"ticker": t, "error": str(e)[:300]}

out = {"started": datetime.now(timezone.utc).isoformat()}
out["smokes"] = []
for t in ["NVDA","TSLA","AAPL","META"]:
    out["smokes"].append(smoke(t))
    print(f"  {t}: {out['smokes'][-1]}")

out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT,"w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1158] DONE")
