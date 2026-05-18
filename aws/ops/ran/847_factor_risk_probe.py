"""
ops/847 - factor-risk-model build probe (read-only).

WHY
---
Before building justhodl-factor-risk - the Barra/Axioma-style factor
risk model that will decompose the consolidated firm book into its
systematic factor exposures - two things must be verified, not assumed:

  1. THE INPUT.  data/firm-book.json must carry a clean netted equity
     book: each name with a signed firm weight (net_pct), a sector and
     ideally a price. The factor model multiplies per-name factor betas
     by net_pct, so that field has to be there and sane.

  2. THE DATA FEED.  The factor model needs daily price history for
     ~8 tradable factor-proxy ETFs (SPY, IWM, MTUM, VLUE, QUAL, USMV,
     IWD, IWF) plus every name in the book, to estimate factor betas by
     time-series regression. Polygon is the planned feed. This probe
     confirms Polygon serves clean adjusted daily bars for both ETFs and
     single stocks, AND measures the rate limit by firing a burst of
     calls back-to-back - that number dictates whether the engine can
     fetch the whole book in one run or must warm a loadings cache over
     several runs.

Pure probe. No deploy, no Lambda. Writes aws/ops/reports/847_*.json.
"""
import json
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta

import boto3

S3_BUCKET = "justhodl-dashboard-live"
POLYGON_KEY = "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"

FACTOR_ETFS = ["SPY", "IWM", "MTUM", "VLUE", "QUAL", "USMV", "IWD", "IWF"]

s3 = boto3.client("s3", region_name="us-east-1")
now = datetime.now(timezone.utc)
rep = {"ops": 847, "ts": now.isoformat(),
       "subject": "Factor-risk-model build probe - firm-book input + "
                  "Polygon factor-data feed",
       "checks": [], "firm_book": {}, "polygon": {}}


def check(name, ok, detail=""):
    rep["checks"].append({"check": name, "ok": bool(ok),
                          "detail": str(detail)[:240]})
    return ok


def poly_daily(ticker, days=40):
    """Fetch `days` calendar days of adjusted daily bars from Polygon.

    Returns (ok, n_bars, first_date, last_date, http_status, err).
    """
    to_d = now.date()
    from_d = to_d - timedelta(days=days)
    url = ("https://api.polygon.io/v2/aggs/ticker/%s/range/1/day/%s/%s"
           "?adjusted=true&sort=asc&limit=120&apiKey=%s"
           % (ticker, from_d, to_d, POLYGON_KEY))
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "jh-probe"})
        with urllib.request.urlopen(req, timeout=25) as r:
            status = r.status
            body = json.loads(r.read().decode("utf-8"))
        results = body.get("results") or []
        if not results:
            return (False, 0, None, None, status,
                    "no results (status=%s)" % body.get("status"))
        f = datetime.fromtimestamp(results[0]["t"] / 1000,
                                   timezone.utc).date().isoformat()
        l = datetime.fromtimestamp(results[-1]["t"] / 1000,
                                   timezone.utc).date().isoformat()
        return (True, len(results), f, l, status, "")
    except urllib.error.HTTPError as e:
        return (False, 0, None, None, e.code, "HTTP %s" % e.code)
    except Exception as e:
        return (False, 0, None, None, None,
                "%s: %s" % (type(e).__name__, e))


# ---- 1) firm-book input --------------------------------------------------
fb = {}
try:
    fb = json.loads(s3.get_object(
        Bucket=S3_BUCKET, Key="data/firm-book.json")["Body"].read())
    check("firm_book_readable", True, "ok")
except Exception as e:
    check("firm_book_readable", False, "%s: %s" % (type(e).__name__, e))

# the netted equity sleeve - try the likely container shapes
eq = None
if isinstance(fb.get("equity_book"), dict):
    eq = (fb["equity_book"].get("entries")
          or fb["equity_book"].get("book")
          or fb["equity_book"].get("positions"))
elif isinstance(fb.get("equity_book"), list):
    eq = fb["equity_book"]
if eq is None:
    eq = fb.get("equity") or fb.get("equity_entries")

if isinstance(eq, list) and eq:
    sample = eq[:2]
    fields = sorted({k for row in eq for k in (row or {}).keys()})
    n_priced = sum(1 for r in eq if r.get("price"))
    n_signed = sum(1 for r in eq
                   if isinstance(r.get("net_pct"), (int, float)))
    sectors = sorted({r.get("sector") for r in eq if r.get("sector")})
    longs = [r for r in eq if r.get("side") == "LONG"]
    shorts = [r for r in eq if r.get("side") == "SHORT"]
    rep["firm_book"] = {
        "equity_names": len(eq),
        "entry_fields": fields,
        "n_with_price": n_priced,
        "n_with_net_pct": n_signed,
        "n_sectors": len(sectors),
        "sectors": sectors,
        "n_long": len(longs), "n_short": len(shorts),
        "sample": sample,
        "top5_by_gross": [
            {"symbol": r.get("symbol"), "net_pct": r.get("net_pct"),
             "gross_pct": r.get("gross_pct"), "sector": r.get("sector")}
            for r in eq[:5]],
    }
    check("firm_book_equity_sleeve", len(eq) > 50,
          "%d netted equity names" % len(eq))
    check("firm_book_has_net_pct", n_signed == len(eq),
          "%d/%d carry signed net_pct" % (n_signed, len(eq)))
    check("firm_book_has_sectors", len(sectors) >= 5,
          "%d distinct sectors" % len(sectors))
    check("firm_book_priced", n_priced >= len(eq) * 0.8,
          "%d/%d priced" % (n_priced, len(eq)))
else:
    check("firm_book_equity_sleeve", False,
          "no equity sleeve found; top-level keys=%s"
          % sorted(fb.keys())[:20])

# ---- 2) Polygon factor-ETF coverage --------------------------------------
etf_res = {}
for t in FACTOR_ETFS:
    ok, n, f, l, st, err = poly_daily(t)
    etf_res[t] = {"ok": ok, "bars": n, "first": f, "last": l,
                  "http": st, "err": err}
    time.sleep(0.2)
rep["polygon"]["factor_etfs"] = etf_res
etf_ok = sum(1 for v in etf_res.values() if v["ok"])
check("polygon_factor_etfs", etf_ok == len(FACTOR_ETFS),
      "%d/%d factor ETFs returned daily bars"
      % (etf_ok, len(FACTOR_ETFS)))

# ---- 3) Polygon single-stock coverage (sample from the book) -------------
stock_syms = [r.get("symbol") for r in (eq or [])[:5]] or ["AAPL", "MSFT"]
stk_res = {}
for t in stock_syms:
    if not t:
        continue
    ok, n, f, l, st, err = poly_daily(t)
    stk_res[t] = {"ok": ok, "bars": n, "first": f, "last": l,
                  "http": st, "err": err}
    time.sleep(0.2)
rep["polygon"]["sample_stocks"] = stk_res
stk_ok = sum(1 for v in stk_res.values() if v["ok"])
check("polygon_single_stocks", stk_ok >= max(1, len(stk_res) - 1),
      "%d/%d sample book names returned daily bars"
      % (stk_ok, len(stk_res)))

# ---- 4) rate-limit burst test --------------------------------------------
# fire 12 calls back-to-back with no pause; count 429s and total wall time.
burst_syms = (FACTOR_ETFS + ["AAPL", "MSFT", "NVDA", "JPM"])[:12]
t0 = time.time()
http_codes = []
for t in burst_syms:
    ok, n, f, l, st, err = poly_daily(t, days=10)
    http_codes.append(st)
elapsed = round(time.time() - t0, 1)
n_429 = sum(1 for c in http_codes if c == 429)
n_ok = sum(1 for c in http_codes if c == 200)
rep["polygon"]["rate_limit_burst"] = {
    "calls": len(burst_syms),
    "wall_seconds": elapsed,
    "http_200": n_ok,
    "http_429": n_429,
    "calls_per_sec_observed": round(len(burst_syms) / max(elapsed, 0.1), 2),
    "verdict": ("UNTHROTTLED - whole book fetchable in one Lambda run"
                if n_429 == 0 else
                "THROTTLED - engine must warm a loadings cache over "
                "several runs (%d/%d calls hit 429)"
                % (n_429, len(burst_syms))),
}
check("polygon_burst_completed", n_ok >= 8,
      "%d/%d burst calls ok, %d throttled, %.1fs wall"
      % (n_ok, len(burst_syms), n_429, elapsed))

# ---- verdict -------------------------------------------------------------
rep["all_pass"] = all(c["ok"] for c in rep["checks"])
fails = [c["check"] for c in rep["checks"] if not c["ok"]]
fb_n = rep["firm_book"].get("equity_names", 0)
rl = rep["polygon"].get("rate_limit_burst", {})
rep["verdict"] = (
    "PROBE GREEN - firm book exposes %d netted equity names with signed "
    "weights; Polygon serves clean daily bars for factor ETFs and single "
    "stocks. Rate limit: %s. Cleared to build justhodl-factor-risk."
    % (fb_n, rl.get("verdict", "?"))
    if rep["all_pass"]
    else "REVIEW before building - failed: %s" % fails)

out = json.dumps(rep, indent=2, default=str)
print(out)
try:
    with open("aws/ops/reports/847_factor_risk_probe.json", "w") as f:
        f.write(out)
except Exception as e:
    print("report write skipped:", e)
