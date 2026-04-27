"""
justhodl-exchange-flows — Net BTC/ETH inflows + outflows from major exchanges

Why this matters
================
When investors withdraw coins to cold storage = accumulation phase
(bullish — they're holding for longer term, supply leaving exchanges).
When they deposit coins to exchanges = distribution phase (bearish —
preparing to sell, supply arriving on exchanges).

The aggregate "exchange balance" (total BTC sitting on exchanges) is
one of the cleanest on-chain demand signals in crypto. At cyclical
extremes:
  Falling exchange balance + rising price → strong accumulation
  Rising exchange balance + falling price → distribution / capitulation

Data sources (all free, no auth):
  - Blockchain.info charts/exchange-trade-volume — daily BTC vol
  - Bitnodes.io                                  — node count
  - CoinMetrics community API                    — exchange supply ratios

Glassnode has the cleanest data here but the free tier of their API
only exposes a small subset. We use CoinMetrics' SplyAct1d (active
supply 1d) as a proxy for exchange-side activity, plus blockchain.info's
n-transactions-excluding-popular series which approximates retail tx flow.

Output (data/exchange-flows.json):
  {
    "generated_at": ...,
    "btc": {
      "active_supply_1d_btc": 250000,
      "active_supply_30d_btc": 4_800_000,
      "txn_volume_24h_btc": 480_000,
      "ratio_1d_to_30d": 0.052,    # higher = more recent activity
      "ratio_z_score_30d": +0.4,   # vs 30-day mean
      "regime": "accumulation" | "distribution" | "neutral",
    },
    "eth": {
      "active_supply_1d_eth": ...,
      "regime": ...,
    },
    "interpretation": "<plain English>",
    "history_90d": [...]  # rolling chart data
  }

Schedule: rate(6 hours) — CoinMetrics community is updated daily but
extra refreshes catch regime shifts faster.
"""
from __future__ import annotations
import json
import os
import time
import urllib.request
import statistics
from datetime import datetime, timezone, timedelta

import boto3

S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = os.environ.get("S3_KEY", "data/exchange-flows.json")
USER_AGENT = os.environ.get("USER_AGENT", "JustHodl Research raafouis@gmail.com")

COINMETRICS_BASE = "https://community-api.coinmetrics.io/v4"


def _fetch(url, timeout=15):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def _fetch_json(url, timeout=15):
    return json.loads(_fetch(url, timeout))


def fetch_coinmetrics_series(asset: str, metrics: list, days_back: int = 120):
    """Fetch CoinMetrics community-API time series for an asset.
    Returns list of {time, asset, metric_name: value, ...} sorted ascending.

    Some CoinMetrics metrics are not available on the free 'community' tier
    (you'd need a paid plan). When the requested set fails, retries with a
    smaller set known to be community-available.
    """
    end = datetime.utcnow().date()
    start = (end - timedelta(days=days_back)).isoformat()

    def _try(metric_list):
        metrics_str = ",".join(metric_list)
        url = (f"{COINMETRICS_BASE}/timeseries/asset-metrics"
               f"?assets={asset}&metrics={metrics_str}"
               f"&start_time={start}&end_time={end.isoformat()}"
               f"&pretty=false&page_size=1000")
        try:
            data = _fetch_json(url)
            rows = data.get("data", [])
            if not rows and data.get("error"):
                # API returned a structured error response
                print(f"  coinmetrics {asset}: {data.get('error')} | {data.get('message','')}")
                return None
            rows.sort(key=lambda r: r.get("time", ""))
            return rows
        except Exception as e:
            print(f"  coinmetrics fetch fail {asset} ({metrics_str}): {e}")
            return None

    # Try requested metrics first
    rows = _try(metrics)
    if rows:
        return rows

    # Fallback 1: the 4 metrics we want as separate calls (some may need
    # paid tier; some are free)
    print(f"  coinmetrics {asset}: full set failed, trying core metrics only")
    core = ["PriceUSD", "TxCnt", "CapMrktCurUSD"]
    rows = _try(core)
    if rows:
        return rows

    # Fallback 2: minimal — just price (always available)
    print(f"  coinmetrics {asset}: core set failed, trying price-only")
    return _try(["PriceUSD"]) or []


def _z_score(series, current):
    """z-score of current value vs the series."""
    cleaned = [v for v in series if v is not None and v == v]
    if len(cleaned) < 8:
        return None
    mean = statistics.mean(cleaned)
    sd = statistics.stdev(cleaned) if len(cleaned) > 1 else 0
    if sd == 0:
        return 0.0
    return round((current - mean) / sd, 2)


def analyze_asset(asset: str, days_back: int = 120):
    """Pull CoinMetrics data and compute exchange-flow signals."""
    # Active supply 1d/30d → ratio is a flow proxy
    # TxCnt → transaction volume
    # SplyAct1d → active supply 1d
    # SplyAct30d → active supply 30d
    metrics = ["SplyAct1d", "SplyAct30d", "TxCnt", "PriceUSD"]
    rows = fetch_coinmetrics_series(asset, metrics, days_back=days_back)
    if not rows:
        return {"error": "no_data", "asset": asset}

    latest = rows[-1] if rows else {}
    out = {"asset": asset.upper(), "as_of": (latest.get("time") or "")[:10]}

    try:
        active_1d = float(latest.get("SplyAct1d") or 0)
        active_30d = float(latest.get("SplyAct30d") or 0)
        tx_count = float(latest.get("TxCnt") or 0)
        price = float(latest.get("PriceUSD") or 0)

        out["active_supply_1d"] = round(active_1d, 0)
        out["active_supply_30d"] = round(active_30d, 0)
        out["txn_count_24h"] = int(tx_count)
        out["price_usd"] = round(price, 2)
        if active_30d > 0:
            out["ratio_1d_to_30d"] = round(active_1d / active_30d, 4)
        else:
            out["ratio_1d_to_30d"] = None

        # z-score of ratio over 30 days
        ratios = []
        for r in rows[-30:]:
            try:
                a1 = float(r.get("SplyAct1d") or 0)
                a30 = float(r.get("SplyAct30d") or 0)
                if a30 > 0:
                    ratios.append(a1 / a30)
            except (ValueError, TypeError):
                continue
        if ratios and out["ratio_1d_to_30d"] is not None:
            out["ratio_z_30d"] = _z_score(ratios, out["ratio_1d_to_30d"])

        # Active-supply momentum: is 1d-active rising or falling?
        recent_1d_active = []
        for r in rows[-7:]:
            try:
                recent_1d_active.append(float(r.get("SplyAct1d") or 0))
            except (ValueError, TypeError):
                continue
        if len(recent_1d_active) >= 5:
            wk_ago = recent_1d_active[0]
            now_val = recent_1d_active[-1]
            if wk_ago > 0:
                out["active_supply_1d_7d_change"] = round((now_val / wk_ago - 1), 4)

        # Regime classification:
        # - Rising active supply + rising price = accumulation (capital flowing in)
        # - Rising active supply + falling price = distribution (supply hitting market)
        # - Falling active supply = quiet / equilibrium

        # Get price 7 days ago for comparison
        if len(rows) >= 8:
            try:
                wk_ago_price = float(rows[-8].get("PriceUSD") or 0)
                if wk_ago_price > 0:
                    out["price_7d_change"] = round((price / wk_ago_price - 1), 4)
            except (ValueError, TypeError):
                pass

        regime = "neutral"
        change_active = out.get("active_supply_1d_7d_change")
        change_price = out.get("price_7d_change")
        if change_active is not None and change_price is not None:
            if change_active > 0.05 and change_price > 0:
                regime = "accumulation"
            elif change_active > 0.05 and change_price < -0.02:
                regime = "distribution"
            elif change_active < -0.05:
                regime = "quiet"
        out["regime"] = regime

        # History (last 90 days for chart)
        history = []
        for r in rows[-90:]:
            try:
                history.append({
                    "date": (r.get("time") or "")[:10],
                    "active_1d": round(float(r.get("SplyAct1d") or 0), 0),
                    "price": round(float(r.get("PriceUSD") or 0), 2),
                })
            except (ValueError, TypeError):
                continue
        out["history_90d"] = history

    except (ValueError, TypeError) as e:
        out["error"] = f"parse_{type(e).__name__}: {e}"

    return out


def interpret(btc: dict, eth: dict) -> str:
    parts = []
    if btc and not btc.get("error"):
        regime = btc.get("regime", "neutral")
        change_active = btc.get("active_supply_1d_7d_change")
        change_price = btc.get("price_7d_change")
        if regime == "accumulation":
            parts.append(f"BTC active supply rising ({change_active*100:+.1f}% / 7d) with price up ({change_price*100:+.1f}%) — accumulation phase")
        elif regime == "distribution":
            parts.append(f"BTC active supply rising while price falling ({change_price*100:+.1f}%) — distribution / supply hitting market")
        elif regime == "quiet":
            parts.append(f"BTC active supply contracting ({change_active*100:+.1f}% / 7d) — equilibrium phase")

    if eth and not eth.get("error"):
        regime_e = eth.get("regime", "neutral")
        if regime_e == "accumulation":
            parts.append("ETH showing accumulation pattern alongside BTC")
        elif regime_e == "distribution":
            parts.append("ETH showing distribution pattern (supply rising, price weakening)")

    if not parts:
        return "Crypto on-chain flows in normal range. No clear regime signal."
    return ". ".join(parts) + "."


def lambda_handler(event, context):
    s3 = boto3.client("s3")
    started = time.time()

    btc = analyze_asset("btc", days_back=120)
    eth = analyze_asset("eth", days_back=120)

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "btc": btc,
        "eth": eth,
        "interpretation": interpret(btc, eth),
        "fetch_duration_s": round(time.time() - started, 1),
    }

    s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                  Body=json.dumps(output).encode(),
                  ContentType="application/json", CacheControl="no-cache")

    print(f"exchange-flows: BTC regime={btc.get('regime', '?')} | "
          f"ETH regime={eth.get('regime', '?')}")
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        "body": json.dumps({
            "ok": True,
            "btc_regime": btc.get("regime"),
            "eth_regime": eth.get("regime"),
        }),
    }
