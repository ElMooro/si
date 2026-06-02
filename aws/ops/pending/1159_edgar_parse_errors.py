"""1159 — Force-refresh EDGAR for NVDA + show parse_errors_sample."""
import json, time, urllib.request, urllib.error, ssl
from datetime import datetime, timezone

EDGAR_URL = "https://ru3djltl3oucvsocjrih37sowu0fxgkm.lambda-url.us-east-1.on.aws/"
REPORT = "aws/ops/reports/1159_edgar_parse_errors.json"
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
                "n_filings_90d": j.get("n_filings_90d"),
                "n_filings_parsed_ok": j.get("n_filings_parsed_ok"),
                "n_filings_parsed_fail": j.get("n_filings_parsed_fail"),
                "parse_errors_sample": j.get("parse_errors_sample"),
                "n_buys": j.get("n_buys"),
                "n_sells": j.get("n_sells"),
                "by_filer_n": len(j.get("by_filer", {}) or {}),
                "signal_label": j.get("signal_label"),
            }
    except Exception as e:
        return {"ticker": t, "error": str(e)[:300]}

out = {"started": datetime.now(timezone.utc).isoformat(), "smokes": []}
for t in ["NVDA", "TSLA"]:
    res = smoke(t)
    out["smokes"].append(res)
    print(f"  {t}: errors={res.get('parse_errors_sample')} parsed_ok={res.get('n_filings_parsed_ok')}/{res.get('n_filings_90d')}")

out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT,"w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1159] DONE")
