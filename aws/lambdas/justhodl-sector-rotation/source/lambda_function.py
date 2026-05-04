"""
justhodl-sector-rotation — 11 SPDR sectors with returns/relative-strength/momentum.

For each sector ETF (XLF, XLE, XLU, XLV, XLY, XLP, XLB, XLI, XLRE, XLK, XLC):
  - 1d, 5d, 20d, 63d, 126d, 252d returns
  - relative strength vs SPY (return_sector - return_spy) over each window
  - momentum quintile (0-4: 0=worst, 4=best) based on 63d-vs-252d trend
  - capital flow z-score (joins with data/etf-flows.json by ticker)
  - new high/low flags (252d high/low touched in last 5d)
  - regime classification (LEADER / RECOVERING / LAGGING / FALLING)

Output: data/sector-rotation.json
Schedule: every 6 hours
"""
import json
import os
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
import math
import boto3

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
KEY = "data/sector-rotation.json"

POLYGON_KEY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")

SECTORS = [
    ("XLF",  "Financials",            "🏦"),
    ("XLE",  "Energy",                "⛽"),
    ("XLU",  "Utilities",             "💡"),
    ("XLV",  "Healthcare",            "⚕️"),
    ("XLY",  "Consumer Discretionary","🛍️"),
    ("XLP",  "Consumer Staples",      "🛒"),
    ("XLB",  "Materials",             "⛏️"),
    ("XLI",  "Industrials",           "🏭"),
    ("XLRE", "Real Estate",           "🏢"),
    ("XLK",  "Technology",            "💻"),
    ("XLC",  "Communications",        "📱"),
]

WINDOWS = [1, 5, 20, 63, 126, 252]


def polygon_bars(ticker, days=300):
    end = datetime.now(timezone.utc).date().isoformat()
    start = (datetime.now(timezone.utc).date() - timedelta(days=days + 100)).isoformat()
    url = (
        f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start}/{end}"
        f"?adjusted=true&sort=asc&limit=5000&apiKey={POLYGON_KEY}"
    )
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "justhodl-sector/1.0"})
        with urllib.request.urlopen(req, timeout=30) as r:
            data = json.loads(r.read().decode())
        if data.get("results"):
            return [(b["t"], b["c"], b["h"], b["l"]) for b in data["results"]]
    except Exception as e:
        print(f"[poly] {ticker} failed: {e}")
    return []


def returns_for_windows(bars):
    """Return dict {window_days: pct_return}."""
    if len(bars) < 2:
        return {}
    last = bars[-1][1]
    out = {}
    for w in WINDOWS:
        if len(bars) > w:
            prev = bars[-1 - w][1]
            if prev > 0:
                out[w] = round((last / prev - 1) * 100, 3)
    return out


def near_extreme(bars, window=252, tolerance=0.02):
    """Returns ('NEW_HIGH', 'NEW_LOW', or 'NORMAL')."""
    if len(bars) < window:
        return "NORMAL"
    recent = bars[-window:]
    last_close = bars[-1][1]
    high_252 = max(b[2] for b in recent)
    low_252 = min(b[3] for b in recent)
    if last_close >= high_252 * (1 - tolerance):
        return "NEW_HIGH"
    if last_close <= low_252 * (1 + tolerance):
        return "NEW_LOW"
    return "NORMAL"


def classify_regime(rs_20, rs_63, ret_20, ret_63):
    """Classify sector by relative-strength vs SPY across 20d/63d."""
    rs20 = rs_20 if rs_20 is not None else 0
    rs63 = rs_63 if rs_63 is not None else 0
    if rs20 > 0 and rs63 > 0:
        return "LEADER", "Outperforming SPY on both 20d and 63d"
    if rs20 > 0 and rs63 <= 0:
        return "RECOVERING", "Outperforming SPY on 20d but lagging on 63d"
    if rs20 <= 0 and rs63 > 0:
        return "FATIGUING", "Was leading on 63d but lagging on 20d"
    return "LAGGING", "Underperforming SPY on both 20d and 63d"


def fetch_etf_flows():
    """Pull existing data/etf-flows.json to enrich with capital flow z-scores."""
    try:
        obj = S3.get_object(Bucket=BUCKET, Key="data/etf-flows.json")
        data = json.loads(obj["Body"].read())
        # build ticker -> flow record map
        flows_by_ticker = {}
        for cat, etfs in (data.get("by_category") or {}).items():
            for etf in etfs:
                flows_by_ticker[etf.get("ticker")] = etf
        return flows_by_ticker
    except Exception as e:
        print(f"[flows] {e}")
        return {}


def lambda_handler(event=None, context=None):
    started = time.time()
    print(f"[sector-rot] start, {len(SECTORS)} sectors")

    # Fetch SPY first as benchmark
    bars_by_ticker = {}
    tickers = ["SPY"] + [t for t, _, _ in SECTORS]
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(polygon_bars, t): t for t in tickers}
        for f in as_completed(futs):
            t = futs[f]
            bars = f.result()
            if bars:
                bars_by_ticker[t] = bars
            print(f"[sector-rot] {t}: {len(bars)} bars")

    if "SPY" not in bars_by_ticker:
        return {"statusCode": 500, "body": "SPY data missing"}

    spy_returns = returns_for_windows(bars_by_ticker["SPY"])

    # ETF flow enrichment
    flows = fetch_etf_flows()

    # Per-sector
    sectors_out = []
    for ticker, name, emoji in SECTORS:
        if ticker not in bars_by_ticker:
            continue
        bars = bars_by_ticker[ticker]
        rets = returns_for_windows(bars)
        rs = {}
        for w in WINDOWS:
            if w in rets and w in spy_returns:
                rs[w] = round(rets[w] - spy_returns[w], 3)

        regime, regime_desc = classify_regime(rs.get(20), rs.get(63), rets.get(20), rets.get(63))
        extreme = near_extreme(bars, 252)

        # Momentum quintile: 0-4 by 63d return
        # Will compute after all sectors loaded
        flow_rec = flows.get(ticker, {})

        sectors_out.append({
            "ticker": ticker,
            "name": name,
            "emoji": emoji,
            "last_close": bars[-1][1],
            "returns": rets,
            "rs_vs_spy": rs,
            "regime": regime,
            "regime_desc": regime_desc,
            "extreme": extreme,
            "flow_z": flow_rec.get("dollar_volume_z_60d"),
            "flow_signal": flow_rec.get("signal"),
            "aum_billions": flow_rec.get("aum_billions"),
        })

    # Compute momentum quintile (0=worst, 4=best) by 63d return
    rets_63 = sorted([(s["returns"].get(63, -999), s["ticker"]) for s in sectors_out])
    rank_by_ticker = {ticker: i for i, (_, ticker) in enumerate(rets_63)}
    n = len(sectors_out)
    for s in sectors_out:
        rank = rank_by_ticker[s["ticker"]]
        # 0-4 quintile
        s["momentum_quintile"] = min(4, int(rank * 5 / n))

    # Build sector-by-sector view sorted by 63d RS
    sectors_out.sort(key=lambda x: -(x["rs_vs_spy"].get(63) or -999))

    # Top + bottom
    top_3 = sectors_out[:3]
    bottom_3 = sectors_out[-3:]
    leaders = [s for s in sectors_out if s["regime"] == "LEADER"]
    laggards = [s for s in sectors_out if s["regime"] == "LAGGING"]
    recovering = [s for s in sectors_out if s["regime"] == "RECOVERING"]
    fatiguing = [s for s in sectors_out if s["regime"] == "FATIGUING"]

    # Headline takeaway
    if len(leaders) >= 5:
        market_breadth = "BROAD_LEADERSHIP"
        breadth_desc = f"{len(leaders)} sectors leading SPY — broad strength"
    elif len(laggards) >= 5:
        market_breadth = "NARROW_LEADERSHIP"
        breadth_desc = f"{len(laggards)} sectors lagging — narrow leadership in {len(leaders)} sector(s)"
    else:
        market_breadth = "MIXED"
        breadth_desc = f"{len(leaders)} leaders, {len(laggards)} laggards, {len(recovering)} recovering"

    out = {
        "version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "duration_s": round(time.time() - started, 2),
        "spy_close": bars_by_ticker["SPY"][-1][1],
        "spy_returns": spy_returns,
        "market_breadth": market_breadth,
        "market_breadth_description": breadth_desc,
        "sectors": sectors_out,
        "leaders": leaders,
        "recovering": recovering,
        "fatiguing": fatiguing,
        "laggards": laggards,
        "top_3_63d_rs": top_3,
        "bottom_3_63d_rs": bottom_3,
        "data_sources": {"prices": "Polygon", "flows": "data/etf-flows.json"},
    }

    body = json.dumps(out, default=str).encode("utf-8")
    S3.put_object(Bucket=BUCKET, Key=KEY, Body=body, ContentType="application/json", CacheControl="public, max-age=3600")
    print(f"[sector-rot] wrote {len(body):,}b in {out['duration_s']}s — leaders={len(leaders)} laggards={len(laggards)}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "market_breadth": market_breadth,
            "n_leaders": len(leaders),
            "n_laggards": len(laggards),
            "top_sector": top_3[0]["ticker"] if top_3 else None,
            "bottom_sector": bottom_3[-1]["ticker"] if bottom_3 else None,
            "duration_s": out["duration_s"],
        }),
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler({}, None), indent=2, default=str))
