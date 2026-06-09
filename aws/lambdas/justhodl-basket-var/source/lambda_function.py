"""justhodl-basket-var — historical VaR / CVaR / beta on a live equal-weighted
basket (the top conviction names), so the audit's #8 risk metric is VISIBLE
without the user having to enter a real portfolio.

The position-based engine (justhodl-portfolio-risk) computes parametric VaR but
only when positions exist. This computes TRUE HISTORICAL VaR (np.percentile of
the basket's daily returns) + CVaR (mean of the tail beyond VaR) + portfolio beta
to SPY, on the current best-setups basket. Real, fat-tail-honest risk numbers.

OUTPUT: data/basket-var.json · SCHEDULE: daily 14:10 UTC.
"""
import json, time, math, statistics
import urllib.request
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3

REGION = "us-east-1"; BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/basket-var.json"
OHLC = "https://justhodl-data-proxy.raafouis.workers.dev/ohlc?ticker=%s&mult=1&span=day&days=160"
s3 = boto3.client("s3", region_name=REGION)


def read_json(key, default=None):
    try: return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception: return default


def fetch_returns(ticker):
    try:
        req = urllib.request.Request(OHLC % ticker, headers={"User-Agent": "JustHodl-VaR/1.0", "Origin": "https://justhodl.ai"})
        d = json.loads(urllib.request.urlopen(req, timeout=15).read().decode())
        bars = d.get("bars") or d.get("results") or []
        closes = [b.get("close") if "close" in b else b.get("c") for b in bars]
        closes = [c for c in closes if c]
        rets = [(closes[i] / closes[i-1] - 1) for i in range(1, len(closes))]
        return ticker, rets
    except Exception:
        return ticker, []


def lambda_handler(event=None, context=None):
    t0 = time.time()
    bs = read_json("data/best-setups.json") or {}
    basket = [s.get("ticker") for s in (bs.get("top_setups") or [])[:15] if s.get("ticker")]
    if not basket:
        basket = ["NVDA", "MU", "AMD", "AAPL", "MSFT", "GOOGL", "META", "AVGO"]

    syms = list({*basket, "SPY"})
    series = {}
    with ThreadPoolExecutor(max_workers=10) as ex:
        for fut in as_completed([ex.submit(fetch_returns, t) for t in syms]):
            t, r = fut.result()
            if len(r) >= 60: series[t] = r

    spy = series.get("SPY", [])
    names = [t for t in basket if t in series]
    if not names:
        out = {"engine": "basket-var", "generated_at": datetime.now(timezone.utc).isoformat(), "ok": False, "note": "no return data"}
        s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out).encode(), ContentType="application/json")
        return {"statusCode": 200, "body": "no data"}

    # align to the shortest length
    n = min(len(series[t]) for t in names)
    if spy: n = min(n, len(spy))
    w = 1.0 / len(names)
    port = []
    for i in range(-n, 0):
        port.append(sum(series[t][i] * w for t in names))

    port_sorted = sorted(port)
    def pctl(p): 
        idx = max(0, min(len(port_sorted)-1, int(p/100 * len(port_sorted))))
        return port_sorted[idx]
    # historical VaR (loss is negative return); report as positive % loss
    var95 = -pctl(5) * 100
    var99 = -pctl(1) * 100
    # CVaR = mean of returns at or below the VaR threshold (the tail)
    thr95 = pctl(5); tail95 = [r for r in port if r <= thr95]
    thr99 = pctl(1); tail99 = [r for r in port if r <= thr99]
    cvar95 = -statistics.mean(tail95) * 100 if tail95 else None
    cvar99 = -statistics.mean(tail99) * 100 if tail99 else None
    vol_daily = statistics.stdev(port) * 100
    vol_annual = vol_daily * math.sqrt(252)
    worst = -min(port) * 100
    # basket beta to SPY
    beta = None
    if spy and len(spy) >= n:
        sp = spy[-n:]
        try:
            cov = statistics.covariance(port, sp); varsp = statistics.variance(sp)
            beta = round(cov / varsp, 2) if varsp else None
        except Exception:
            beta = None

    out = {
        "engine": "basket-var", "version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "ok": True, "duration_s": round(time.time() - t0, 1),
        "basket": names, "n_names": len(names), "weighting": "equal", "lookback_days": n,
        "var_method": "historical (empirical percentile of basket daily returns)",
        "var_1d_95_pct": round(var95, 2), "var_1d_99_pct": round(var99, 2),
        "cvar_1d_95_pct": round(cvar95, 2) if cvar95 is not None else None,
        "cvar_1d_99_pct": round(cvar99, 2) if cvar99 is not None else None,
        "vol_daily_pct": round(vol_daily, 2), "vol_annual_pct": round(vol_annual, 1),
        "worst_day_pct": round(worst, 2), "basket_beta_spy": beta,
        "interpretation": f"On a $100k equal-weight basket of these {len(names)} names, a 1-day 95% VaR of {round(var95,2)}% means ~${round(var95*1000):,} expected to be the worst loss on 19 of 20 days; CVaR {round(cvar95,1) if cvar95 else '—'}% is the average loss on the worst 1-in-20 days.",
        "note": "Research, not advice. Historical VaR on the live top-conviction basket — add real positions for portfolio-specific VaR.",
    }
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=1800")
    print(f"[basket-var] VaR95={var95:.2f}% CVaR95={cvar95} beta={beta} n={len(names)}")
    return {"statusCode": 200, "body": json.dumps({"var95": round(var95,2), "n": len(names)})}
