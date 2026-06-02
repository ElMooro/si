"""1160 — Final EDGAR smoke after XSL prefix fix."""
import json, time, urllib.request, ssl
from datetime import datetime, timezone

EDGAR_URL = "https://ru3djltl3oucvsocjrih37sowu0fxgkm.lambda-url.us-east-1.on.aws/"
REPORT = "aws/ops/reports/1160_edgar_final.json"
ctx = ssl.create_default_context()

def smoke(t, refresh=True):
    url = f"{EDGAR_URL}?ticker={t}" + ("&refresh=1" if refresh else "")
    t0 = time.time()
    try:
        req = urllib.request.Request(url, headers={"User-Agent":"JustHodl-Smoke/1.0"})
        with urllib.request.urlopen(req, timeout=120, context=ctx) as r:
            j = json.loads(r.read())
            txns = j.get("transactions", []) or []
            sample = [{"date":x.get("date"),"filer":(x.get("filer","") or "")[:28],
                         "role":(x.get("role","") or "")[:24],"dir":x.get("direction"),
                         "code":x.get("code"),"sh":int(x.get("shares",0) or 0),
                         "$":int(x.get("dollars",0) or 0)} for x in txns[:6]]
            return {
                "ticker": t, "http": r.status, "elapsed_s": round(time.time()-t0,2),
                "cik": j.get("cik"),
                "n_filings_90d": j.get("n_filings_90d"),
                "n_filings_parsed_ok": j.get("n_filings_parsed_ok"),
                "n_filings_parsed_fail": j.get("n_filings_parsed_fail"),
                "parse_errors_sample": j.get("parse_errors_sample"),
                "n_buys": j.get("n_buys"),
                "n_sells": j.get("n_sells"),
                "by_filer_n": len(j.get("by_filer", {}) or {}),
                "signal_label": j.get("signal_label"),
                "signal_score": j.get("signal_score"),
                "net_dollars_90d": j.get("net_dollars_90d"),
                "cluster_detected": j.get("cluster_detected"),
                "sample_txns": sample,
            }
    except Exception as e:
        return {"ticker": t, "error": str(e)[:300]}

out = {"started": datetime.now(timezone.utc).isoformat(), "smokes": []}
for t in ["NVDA", "TSLA", "META", "AAPL"]:
    res = smoke(t, refresh=True)
    out["smokes"].append(res)
    if "error" in res:
        print(f"  {t}: ERROR {res['error']}")
    else:
        print(f"  {t}: parsed {res['n_filings_parsed_ok']}/{res['n_filings_90d']}  buys={res['n_buys']} sells={res['n_sells']}  signal={res['signal_label']}  elapsed={res['elapsed_s']}s")

out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT,"w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1160] DONE")
