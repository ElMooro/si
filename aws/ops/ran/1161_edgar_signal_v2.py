"""1161 — Smoke EDGAR signal v2: should NOT flag megacaps as BEARISH for routine RSU selling."""
import json, time, urllib.request, ssl
from datetime import datetime, timezone

EDGAR_URL = "https://ru3djltl3oucvsocjrih37sowu0fxgkm.lambda-url.us-east-1.on.aws/"
REPORT = "aws/ops/reports/1161_edgar_signal_v2.json"
ctx = ssl.create_default_context()

def smoke(t, refresh=True):
    url = f"{EDGAR_URL}?ticker={t}" + ("&refresh=1" if refresh else "")
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
                "n_buys": j.get("n_buys"),
                "n_sells": j.get("n_sells"),
                "total_dollars_buy": j.get("total_dollars_buy"),
                "total_dollars_sell": j.get("total_dollars_sell"),
                "prior_n_sells": j.get("prior_n_sells"),
                "prior_dollars_sell": j.get("prior_dollars_sell"),
                "sell_acceleration": j.get("sell_acceleration"),
                "buy_acceleration": j.get("buy_acceleration"),
                "n_csuite_sellers": j.get("n_csuite_sellers"),
                "signal_label": j.get("signal_label"),
                "signal_score": j.get("signal_score"),
                "signal_note": j.get("signal_note"),
                "top_sellers_count": len(j.get("top_sellers", []) or []),
                "top_buyers_count":  len(j.get("top_buyers",  []) or []),
                "top_sellers_sample": [{"f":s.get("filer","")[:25],"r":(s.get("role","") or "")[:25],"$":int(s.get("dollars",0))} for s in (j.get("top_sellers") or [])[:3]],
                "top_buyers_sample":  [{"f":s.get("filer","")[:25],"r":(s.get("role","") or "")[:25],"$":int(s.get("dollars",0))} for s in (j.get("top_buyers")  or [])[:3]],
            }
    except Exception as e:
        return {"ticker": t, "error": str(e)[:300]}

out = {"started": datetime.now(timezone.utc).isoformat(), "smokes": []}
# Test megacaps (expect: ROUTINE_SELLING — not BEARISH anymore)
# Plus a small-cap that might have actual insider buying for contrast
for t in ["NVDA", "TSLA", "META", "AAPL", "MSFT"]:
    res = smoke(t, refresh=True)
    out["smokes"].append(res)
    if "error" in res:
        print(f"  {t}: ERROR {res['error']}")
    else:
        print(f"  {t}: {res['signal_label']:20s} ({res['signal_score']}/100) "
                f"buys={res['n_buys']:2d} sells={res['n_sells']:3d} "
                f"sell_accel={res['sell_acceleration']:.2f}× cs={res['n_csuite_sellers']} "
                f"top_sellers={res['top_sellers_count']}")

out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT,"w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1161] DONE")
