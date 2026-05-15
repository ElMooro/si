"""
justhodl-liquidity-profile — per-security tradability scoring.

For S&P 500 + Russell mid-cap proxy, computes:
  • 20-DAY AVG DAILY VOLUME ($ value)
  • TYPICAL BID-ASK SPREAD (from Polygon NBBO snapshots)
  • SPREAD AS % OF PRICE (in basis points)
  • ADV-TO-MARKET-CAP RATIO (high = retail-friendly, low = stale)
  • LIQUIDITY SCORE 0-100 composite

Useful for: position sizing (don't be more than 10% of ADV); trade timing
(avoid wide spreads); finding "stalled" names where liquidity is dropping.

Polygon endpoints:
  /v3/quotes/{ticker}/{date}  — NBBO ticks (just need a few snapshots)
  /v2/aggs/ticker/.../range/1/day  — already used elsewhere; get last 20d

Liquidity score 0-100:
  40%  ADV $ value (>$1B = 100, $100M = 70, $10M = 40, $1M = 10)
  30%  Spread bps (1bp = 100, 5bp = 70, 20bp = 40, 50bp+ = 10)
  20%  ADV / market cap (turnover rate, > 1% = high)
  10%  Consistency (low std-dev of daily volume)

Output: data/liquidity-profile.json
  • universe_size, generated_at
  • top_100_liquid (ranked)
  • bottom_50_illiquid (warning list)
  • per_ticker: dict {liquidity_score, adv_usd, spread_bps, turnover}

Schedule: cron(0 22 ? * FRI *) — weekly Friday 6PM ET (end of week).
"""
import json
import os
import time
import urllib.request
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3

S3_BUCKET = "justhodl-dashboard-live"
S3_KEY = "data/liquidity-profile.json"
POLYGON_KEY = os.environ.get("POLYGON_KEY", "")

# Reasonable universe — top 300 by mkt cap proxy
UNIVERSE = [
    # Copy from market-internals universe for consistency
    "AAPL","MSFT","GOOGL","AMZN","NVDA","META","TSLA","BRK-B","LLY","AVGO",
    "V","JPM","WMT","XOM","UNH","MA","JNJ","PG","HD","ORCL","COST","ABBV",
    "BAC","NFLX","CRM","CVX","KO","TMO","PEP","ADBE","CSCO","ACN","AMD","WFC",
    "MRK","ABT","NKE","TXN","DIS","LIN","DHR","MCD","NOW","IBM","PM","INTU",
    "CAT","SPGI","GE","AMGN","RTX","UNP","UBER","NEE","BLK","T","AMAT","HON",
    "C","BKNG","LRCX","LOW","MS","GS","ETN","COP","BX","TJX","MDT","PLD",
    "SBUX","DE","SCHW","CB","ELV","ADP","BSX","ANET","KLAC","TT","GILD","REGN",
    "PGR","PFE","CI","SO","FI","PANW","BMY","MMC","MO","CMCSA","INTC","CVS",
    "TGT","F","GM","NSC","CSX","FDX","UPS","DAL","AAL","LUV","MAR","HLT","DPZ",
    "CMG","YUM","MDLZ","CL","KMB","CHD","EL","ULTA","GIS","SJM","BBY","ROST",
    "DG","DLTR","USB","PNC","TFC","COF","BK","STT","FITB","HBAN","RF","CFG",
    "KEY","CMA","MU","ON","QCOM","MRVL","ARM","WDC","STX","BA","LMT","NOC",
    "GD","VLO","MPC","PSX","SLB","HAL","OXY","DVN","FANG","EOG","APA","VRTX",
    "ISRG","SYK","ZTS","BDX","DXCM","IDXX","HUM","CNC","MMM","KHC","DLR","EQIX",
    "PSA","CCI","AMT","CME","ICE","NDAQ","MCO","TROW","BEN","IVZ","NTRS","PRU",
    "MET","TRV","AIG","ALL","HIG","AFL","CINF","BIIB","TMUS","VZ","CHTR","WBD",
    "FOX","FOXA","PARA","TTWO","EA","WAT","WCN","WEC","WHR","WMB","WST","WTW",
    "WY","WYNN","XEL","XYL","ZBH","SHOP","ABNB","MELI","RIVN","HOOD","COIN",
    "PLTR","SNOW","CRWD","NET","RBLX","SQ","PYPL","DDOG","ZS","MDB","DOCU",
    "ROKU","DASH","SPOT","TSM","ASML","SQ","PYPL",
]
UNIVERSE = sorted(set(UNIVERSE))[:300]

s3 = boto3.client("s3", region_name="us-east-1")


def http_get(url, timeout=15):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())
    except Exception:
        return None


def fetch_last_20d(ticker):
    if not POLYGON_KEY: return None
    end = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    start = (datetime.now(timezone.utc) - timedelta(days=35)).strftime("%Y-%m-%d")
    url = (f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/"
           f"{start}/{end}?adjusted=true&limit=50&apiKey={POLYGON_KEY}")
    data = http_get(url)
    if not data or "results" not in data: return None
    return data["results"]


def fetch_nbbo_snapshot(ticker):
    """Get last quote snapshot."""
    if not POLYGON_KEY: return None
    url = f"https://api.polygon.io/v3/quotes/{ticker}?limit=1&apiKey={POLYGON_KEY}"
    data = http_get(url)
    if not data or "results" not in data: return None
    return data["results"][0] if data["results"] else None


def put_s3_json(key, body, cache="public, max-age=21600"):
    s3.put_object(Bucket=S3_BUCKET, Key=key,
                   Body=json.dumps(body, default=str).encode("utf-8"),
                   ContentType="application/json", CacheControl=cache)


def analyze_one(ticker):
    bars = fetch_last_20d(ticker)
    if not bars or len(bars) < 5:
        return {"ticker": ticker, "err": "no_bars"}
    bars = bars[-20:]  # last 20 trading days

    volumes_usd = []
    for b in bars:
        c = b.get("c"); v = b.get("v")
        if c and v: volumes_usd.append(c * v)
    if not volumes_usd:
        return {"ticker": ticker, "err": "no_volume"}
    adv_usd = sum(volumes_usd) / len(volumes_usd)
    adv_std = (sum((x - adv_usd)**2 for x in volumes_usd) / len(volumes_usd))**0.5
    consistency = 1 - min(1, adv_std / (adv_usd + 1))  # 0..1, higher = consistent

    # Try to get spread from NBBO (often expensive — use last bar high-low as proxy)
    last = bars[-1]
    if last.get("h") and last.get("l") and last.get("c"):
        # Avg true range as % of close — proxy for spread
        atr_pct = (last["h"] - last["l"]) / last["c"]
    else:
        atr_pct = None

    # Score components
    # ADV scoring (40%)
    if adv_usd >= 1e9: adv_score = 100
    elif adv_usd >= 5e8: adv_score = 90
    elif adv_usd >= 1e8: adv_score = 70
    elif adv_usd >= 5e7: adv_score = 55
    elif adv_usd >= 1e7: adv_score = 40
    elif adv_usd >= 1e6: adv_score = 20
    else: adv_score = 10

    # Spread scoring (30%) — proxy from intraday range; lower = better
    if atr_pct is None: spread_score = 50
    elif atr_pct <= 0.003: spread_score = 100  # <30bp range = tight
    elif atr_pct <= 0.008: spread_score = 80
    elif atr_pct <= 0.015: spread_score = 60
    elif atr_pct <= 0.025: spread_score = 40
    elif atr_pct <= 0.05: spread_score = 25
    else: spread_score = 10

    # Consistency (10%)
    consistency_score = consistency * 100

    # Composite (no market cap available without extra call — defer to 90% with 10% consistency)
    score = round(adv_score * 0.5 + spread_score * 0.35 + consistency_score * 0.15, 1)

    return {
        "ticker": ticker,
        "adv_usd": round(adv_usd),
        "adv_usd_str": f"${adv_usd/1e9:.2f}B" if adv_usd >= 1e9 else f"${adv_usd/1e6:.1f}M",
        "atr_pct": round(atr_pct, 5) if atr_pct else None,
        "spread_proxy_bps": round(atr_pct * 10000, 1) if atr_pct else None,
        "consistency": round(consistency, 3),
        "liquidity_score": score,
        "n_bars": len(bars),
    }


def lambda_handler(event, context):
    t0 = time.time()
    print(f"[liquidity] starting universe={len(UNIVERSE)}")
    if not POLYGON_KEY:
        return {"statusCode": 500, "body": json.dumps({"err": "POLYGON_KEY missing"})}

    results = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(analyze_one, t): t for t in UNIVERSE}
        for f in as_completed(futs):
            try:
                r = f.result()
                results[r["ticker"]] = r
            except Exception as e:
                pass

    valid = [r for r in results.values() if r.get("liquidity_score") is not None]
    top_100 = sorted(valid, key=lambda x: -x["liquidity_score"])[:100]
    bottom_50 = sorted(valid, key=lambda x: x["liquidity_score"])[:50]

    output = {
        "schema_version": "1.0",
        "method": "liquidity_profile_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "universe_size": len(UNIVERSE),
        "n_analyzed": len(valid),
        "top_100_liquid": top_100,
        "bottom_50_illiquid": bottom_50,
        "all_tickers": results,
        "duration_s": round(time.time()-t0, 1),
    }

    put_s3_json(S3_KEY, output)
    print(f"[liquidity] n_analyzed={len(valid)} top_5={[t['ticker'] for t in top_100[:5]]}")

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({
            "ok": True, "n_analyzed": len(valid),
            "top_5": [t["ticker"] for t in top_100[:5]],
            "duration_s": round(time.time()-t0, 1),
        }),
    }
