"""
justhodl-onchain-ratios — On-chain BTC/ETH ratios

Glassnode's full Studio is paid, but the most-watched on-chain ratios can
be derived from free public APIs:

  - Bitcoin:
      mempool.space         -> mempool size, fee estimates, hashrate
      blockchain.info       -> network value, market cap, txn count
      CoinMetrics community -> realized cap, MVRV, NVT
  - Ethereum:
      etherscan.io           -> gas, txn count
      ultrasound.money       -> ETH burn rate, supply growth/decay

This Lambda computes:
  - BTC: MVRV (Market Value / Realized Value), NVT, hash ribbon, mempool stress
  - ETH: gas, supply delta (issuance - burn), staked ratio
  - Combined: aggregator score, flag extreme readings

Output (data/onchain-ratios.json):
  {
    "generated_at": ...,
    "btc": {
      "price": 67432, "market_cap": ..., "realized_cap": ...,
      "mvrv": 1.83, "mvrv_z": +0.4,
      "nvt": 78,
      "hash_rate_eh": 580,
      "mempool_kb": 215000, "fee_sat_vb": 12,
      "extreme_signals": ["mvrv_above_2 (overheated)"],
    },
    "eth": {
      "price": 3200, "market_cap": ...,
      "gas_gwei": 8, "burn_rate_eth_24h": 1840,
      "supply_growth_24h": -1200,    (ETH supply DECREASING — deflationary)
      "extreme_signals": [...]
    },
    "interpretation": "<plain English roll-up>"
  }
"""
from __future__ import annotations
import json
import os
import time
import urllib.request
from datetime import datetime, timezone

import boto3

S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = os.environ.get("S3_KEY", "data/onchain-ratios.json")
USER_AGENT = os.environ.get("USER_AGENT", "JustHodl Research raafouis@gmail.com")
COINMETRICS_BASE = "https://community-api.coinmetrics.io/v4"


def _fetch(url: str, timeout: int = 15) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def _fetch_json(url: str, timeout: int = 15):
    return json.loads(_fetch(url, timeout))


def fetch_btc_metrics():
    out = {"errors": []}
    # CoinMetrics community API — provides MVRV, realized cap, etc.
    try:
        url = (
            f"{COINMETRICS_BASE}/timeseries/asset-metrics"
            f"?assets=btc&metrics=PriceUSD,CapMVRVCur,SplyCur"
            f"&pretty=false&page_size=1&end_time={datetime.utcnow().strftime('%Y-%m-%d')}"
        )
        # NOTE: CapRealUSD / NVTAdj / FlowInExUSD went CoinMetrics PRO-tier (403) and a single paid
        # metric fails the WHOLE request — which had silently zeroed every on-chain value here. Price,
        # MVRV and supply remain free, so realized cap/price + NUPL are DERIVED from MVRV (= market /
        # realized). Exchange flows are owned by the dedicated justhodl-crypto-exchange-flows engine.
        d = _fetch_json(url)
        rows = d.get("data", [])
        if rows:
            r = rows[-1]
            out["price"] = float(r.get("PriceUSD") or 0)
            out["mvrv"] = float(r.get("CapMVRVCur") or 0)
            out["supply"] = float(r.get("SplyCur") or 0)
            if out["price"] and out["supply"]:
                out["market_cap"] = round(out["price"] * out["supply"])
            if out["mvrv"]:
                out["realized_price"] = round(out["price"] / out["mvrv"], 0)
                out["price_vs_realized_pct"] = round((out["mvrv"] - 1) * 100, 1)
                if out.get("market_cap"):
                    out["realized_cap"] = round(out["market_cap"] / out["mvrv"])
                _nupl = 1 - 1 / out["mvrv"]
                out["nupl"] = round(_nupl, 4)
                out["nupl_zone"] = ("CAPITULATION" if _nupl < 0 else "HOPE/FEAR" if _nupl < 0.25
                                    else "OPTIMISM/ANXIETY" if _nupl < 0.5 else "BELIEF/DENIAL" if _nupl < 0.75
                                    else "EUPHORIA/GREED")
    except Exception as e:
        out["errors"].append(f"coinmetrics_btc: {type(e).__name__}")

    # Mempool.space — current mempool + fees + hash rate
    try:
        m = _fetch_json("https://mempool.space/api/mempool")
        out["mempool_count"] = m.get("count", 0)
        out["mempool_vsize"] = m.get("vsize", 0)
        fees = _fetch_json("https://mempool.space/api/v1/fees/recommended")
        out["fee_sat_vb"] = fees.get("fastestFee", 0)
        out["fee_30min_sat_vb"] = fees.get("halfHourFee", 0)
    except Exception as e:
        out["errors"].append(f"mempool: {type(e).__name__}")

    try:
        hr = _fetch_json("https://blockchain.info/q/hashrate")
        out["hash_rate_th"] = float(hr) if hr else None
        if out.get("hash_rate_th"):
            out["hash_rate_eh"] = round(out["hash_rate_th"] / 1e6, 1)
    except Exception as e:
        out["errors"].append(f"hashrate: {type(e).__name__}")

    # Extreme signal flags
    flags = []
    mvrv = out.get("mvrv", 0)
    if mvrv:
        if mvrv > 3.5:   flags.append("mvrv_above_3.5 (extreme overheated, historical sell signal)")
        elif mvrv > 2:   flags.append("mvrv_above_2 (overheated; 6-12mo top territory)")
        elif mvrv < 1:   flags.append("mvrv_below_1 (oversold; historical accumulation zone)")
        elif mvrv < 0.8: flags.append("mvrv_below_0.8 (deep capitulation; rare buy zone)")
    out["extreme_signals"] = flags
    return out


def fetch_eth_metrics():
    out = {"errors": []}
    # CoinMetrics for ETH price + cap
    try:
        url = (
            f"{COINMETRICS_BASE}/timeseries/asset-metrics"
            f"?assets=eth&metrics=PriceUSD,CapMrktCurUSD,CapMVRVCur,IssContPctAnn"
            f"&pretty=false&page_size=1&end_time={datetime.utcnow().strftime('%Y-%m-%d')}"
        )
        d = _fetch_json(url)
        rows = d.get("data", [])
        if rows:
            r = rows[-1]
            out["price"] = float(r.get("PriceUSD") or 0)
            out["market_cap"] = float(r.get("CapMrktCurUSD") or 0)
            out["mvrv"] = float(r.get("CapMVRVCur") or 0)
            out["issuance_pct_annual"] = float(r.get("IssContPctAnn") or 0)
    except Exception as e:
        out["errors"].append(f"coinmetrics_eth: {type(e).__name__}")

    # Etherscan gas oracle (free, no key required for gas price)
    try:
        # The /v2/api endpoint is the modern one
        d = _fetch_json("https://api.etherscan.io/api?module=gastracker&action=gasoracle")
        if d.get("status") == "1":
            r = d["result"]
            out["gas_safe_gwei"] = int(r.get("SafeGasPrice", 0))
            out["gas_propose_gwei"] = int(r.get("ProposeGasPrice", 0))
            out["gas_fast_gwei"] = int(r.get("FastGasPrice", 0))
    except Exception as e:
        out["errors"].append(f"etherscan_gas: {type(e).__name__}")

    flags = []
    mvrv = out.get("mvrv", 0)
    if mvrv:
        if mvrv > 2.5: flags.append("eth_mvrv_above_2.5 (overheated)")
        elif mvrv < 1: flags.append("eth_mvrv_below_1 (oversold)")
    iss = out.get("issuance_pct_annual", 0)
    if iss:
        if iss < 0:    flags.append(f"eth_supply_deflationary ({iss:.2f}% annual)")
        elif iss > 1:  flags.append(f"eth_supply_inflationary ({iss:.2f}% annual)")
    out["extreme_signals"] = flags
    return out


def interpret(btc: dict, eth: dict) -> str:
    parts = []
    if btc.get("mvrv"):
        m = btc["mvrv"]
        if m > 2.5:    parts.append(f"BTC MVRV at {m:.2f} — historically near cyclical tops")
        elif m < 1:    parts.append(f"BTC MVRV at {m:.2f} — historical accumulation zone")
        else:          parts.append(f"BTC MVRV at {m:.2f} — fair value range")
    if btc.get("fee_sat_vb"):
        f = btc["fee_sat_vb"]
        if f < 5:    parts.append(f"BTC mempool quiet ({f} sat/vB) — low on-chain demand")
        elif f > 50: parts.append(f"BTC mempool stressed ({f} sat/vB) — heavy demand")
    if eth.get("issuance_pct_annual"):
        if eth["issuance_pct_annual"] < 0:
            parts.append(f"ETH supply is DEFLATIONARY ({eth['issuance_pct_annual']:.2f}% annual) — burns exceeding issuance")
        else:
            parts.append(f"ETH supply growth {eth['issuance_pct_annual']:.2f}% annualized")
    return ". ".join(parts) if parts else "On-chain metrics in normal range."


def _cq_join(doc, s3c, bucket):
    """CryptoQuant fusion (ops 2742): whale ratio, MVRV, SOPR, netflow, MPI join the free-source ratios."""
    try:
        cq = json.loads(s3c.get_object(Bucket=bucket, Key="data/cryptoquant-onchain.json")["Body"].read())
        if cq.get("status") == "LIVE":
            cm = cq.get("metrics") or {}
            doc["cryptoquant"] = {k: cm.get(k) for k in ("btc_whale_ratio", "btc_mvrv", "btc_sopr", "btc_exchange_netflow", "btc_mpi", "btc_nupl", "btc_puell", "btc_nvt_golden", "btc_realized_price", "btc_ssr")}
            doc["cryptoquant"]["composite_onchain_risk_z"] = cq.get("composite_onchain_risk_z")
            doc["cryptoquant"]["grading"] = "PROVISIONAL"
            doc["resurrected"] = "ops 2742 - daily 21:20 UTC"
    except Exception as e:
        print("[ratios] cq join skipped:", str(e)[:80])
    return doc


# ═══════════════════════════════════════════════════════════════════════════
# CYCLE BANDS — the two metrics the fleet genuinely lacked (grep: 0 hits each)
# ═══════════════════════════════════════════════════════════════════════════
# Audit before building (ops 3821): MVRV(15) NUPL(7) Puell(7) Mayer(4) all
# already exist across the fleet, so nothing here recomputes them. The only
# TRUE zeros were the Bitcoin Rainbow log-regression bands and the Pi Cycle
# Top. Both are added HERE rather than in a new engine because this engine
# already owns the on-chain cycle-valuation vocabulary and its own page.
#
# HONESTY, SHIPPED IN THE FEED — the popular versions of both are weak:
#   • The Rainbow chart as circulated is HAND-DRAWN bands on a log chart with
#     no stated fit. We instead fit ln(price) = a*ln(days) + b by ordinary
#     least squares over the real history and place bands at +/- k*sigma of
#     the residuals, then publish a, b, R^2, sigma and n. It remains a curve
#     FIT TO ITS OWN HISTORY with no predictive claim, and every cycle has
#     come in lower against it — it is a context band, never a target.
#   • Pi Cycle called the 2013/2017/2021 tops within days. That is n=3. We
#     publish n with the signal so it can never read as a law.

import math as _math


def _cm_price_history(asset="btc", start="2010-07-18"):
    """Daily close history from the CoinMetrics community API (free tier).
       Returns [(YYYY-MM-DD, price)] oldest-first, paging until exhausted."""
    rows, token, guard = [], None, 0
    while guard < 12:
        guard += 1
        url = (f"{COINMETRICS_BASE}/timeseries/asset-metrics?assets={asset}"
               f"&metrics=PriceUSD&frequency=1d&page_size=10000"
               f"&start_time={start}&pretty=false")
        if token:
            url += f"&next_page_token={token}"
        d = _fetch_json(url)
        if not d:
            break
        for r in d.get("data", []):
            try:
                px = float(r.get("PriceUSD"))
            except (TypeError, ValueError):
                continue
            if px > 0:
                rows.append((str(r.get("time", ""))[:10], px))
        token = d.get("next_page_token")
        if not token:
            break
    rows.sort(key=lambda x: x[0])
    return rows


def _sma(vals, n):
    return sum(vals[-n:]) / n if len(vals) >= n else None


def rainbow_bands(hist):
    """OLS log-regression rainbow. ln(P) = a*ln(t) + b, bands at +/- k*sigma."""
    if len(hist) < 800:
        return {"available": False,
                "reason": f"only {len(hist)} daily closes; need >=800 for a fit"}
    t0 = datetime.strptime(hist[0][0], "%Y-%m-%d")
    xs, ys = [], []
    for ds, px in hist:
        try:
            day = (datetime.strptime(ds, "%Y-%m-%d") - t0).days + 1
        except ValueError:
            continue
        if day > 0 and px > 0:
            xs.append(_math.log(day))
            ys.append(_math.log(px))
    n = len(xs)
    if n < 800:
        return {"available": False, "reason": f"only {n} usable points"}
    mx, my = sum(xs) / n, sum(ys) / n
    sxy = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    sxx = sum((x - mx) ** 2 for x in xs)
    if sxx == 0:
        return {"available": False, "reason": "degenerate regressor"}
    a = sxy / sxx
    b = my - a * mx
    resid = [y - (a * x + b) for x, y in zip(xs, ys)]
    sst = sum((y - my) ** 2 for y in ys)
    sse = sum(r * r for r in resid)
    r2 = 1 - sse / sst if sst else None
    sigma = _math.sqrt(sse / (n - 2))
    cur_x, cur_y = xs[-1], ys[-1]
    fair = a * cur_x + b
    z = (cur_y - fair) / sigma if sigma else None
    BANDS = [(-2.0, "DEEP VALUE — max accumulation"), (-1.25, "ACCUMULATE"),
             (-0.5, "CHEAP"), (0.5, "FAIR / HOLD"), (1.25, "WARM — begin trimming"),
             (2.0, "HOT — distribution zone"), (99, "EUPHORIA — historic froth")]
    label = next(lbl for thr, lbl in BANDS if z is not None and z <= thr)
    return {
        "available": True,
        "method": "OLS fit of ln(price) on ln(days since first observation)",
        "slope_a": round(a, 4), "intercept_b": round(b, 4),
        "r_squared": round(r2, 4) if r2 is not None else None,
        "residual_sigma": round(sigma, 4), "n_days": n,
        "history_starts": hist[0][0],
        "price": round(_math.exp(cur_y), 2),
        "fair_value": round(_math.exp(fair), 2),
        "z_sigma": round(z, 2) if z is not None else None,
        "band": label,
        "band_prices": {f"{k:+.2f}sigma": round(_math.exp(fair + k * sigma), 2)
                        for k in (-2.0, -1.25, -0.5, 0, 0.5, 1.25, 2.0)},
        "caveat": ("A regression fit to its OWN history — it cannot predict, only "
                   "describe where price sits versus its past log trend. Bands "
                   "re-fit every run and drift as history grows; every cycle has "
                   "come in lower against this line. Context band, never a target, "
                   "and never a standalone signal."),
    }


def pi_cycle_top(hist):
    """111DMA crossing above 2x350DMA has marked the 2013/2017/2021 tops. n=3."""
    closes = [p for _, p in hist]
    if len(closes) < 400:
        return {"available": False,
                "reason": f"only {len(closes)} closes; need >=400 for the 350DMA"}
    ma111, ma350 = _sma(closes, 111), _sma(closes, 350)
    if not ma111 or not ma350:
        return {"available": False, "reason": "moving averages unavailable"}
    target = 2 * ma350
    # walk history for the most recent cross
    last_cross, prior = None, None
    for i in range(400, len(closes)):
        w = closes[:i + 1]
        m1, m3 = _sma(w, 111), _sma(w, 350)
        if not m1 or not m3:
            continue
        state = m1 > 2 * m3
        if prior is not None and state != prior:
            last_cross = {"date": hist[i][0],
                          "direction": "111DMA crossed ABOVE 2x350DMA (TOP SIGNAL)"
                          if state else "111DMA fell back below 2x350DMA"}
        prior = state
    gap_pct = (ma111 / target - 1) * 100 if target else None
    return {
        "available": True,
        "ma_111d": round(ma111, 2), "ma_350d": round(ma350, 2),
        "trigger_level": round(target, 2),
        "distance_to_trigger_pct": round(gap_pct, 2) if gap_pct is not None else None,
        "signal": "TOP SIGNAL ACTIVE" if ma111 > target else "no signal",
        "last_cross": last_cross,
        "historical_n": 3,
        "caveat": ("Called the 2013 / 2017 / 2021 tops within days — but that is "
                   "n=3 on a single asset, and the post-2024 spot-ETF regime may "
                   "have changed the cycle shape entirely. Treat as one input, "
                   "never a law."),
    }


def mayer_and_200w(hist):
    closes = [p for _, p in hist]
    out = {}
    ma200d = _sma(closes, 200)
    if ma200d:
        mayer = closes[-1] / ma200d
        hist_m = [closes[i] / (sum(closes[i - 199:i + 1]) / 200)
                  for i in range(199, len(closes))]
        pct = (100 * sum(1 for v in hist_m if v <= mayer) / len(hist_m)) if hist_m else None
        out["mayer_multiple"] = {
            "value": round(mayer, 3), "ma_200d": round(ma200d, 2),
            "percentile_all_history": round(pct, 1) if pct is not None else None,
            "read": ("OVERBOUGHT (>2.4)" if mayer > 2.4 else
                     "CHEAP (<1.0)" if mayer < 1.0 else "NEUTRAL"),
        }
    if len(closes) >= 1400:
        ma200w = _sma(closes, 1400)  # 200 weeks of daily closes
        out["ma_200_week"] = {
            "value": round(ma200w, 2),
            "pct_above": round((closes[-1] / ma200w - 1) * 100, 2),
            "note": "the historical cycle-bottom support; price has rarely closed below it",
        }
    return out


def build_cycle_bands():
    hist = _cm_price_history()
    if not hist:
        return {"available": False, "reason": "CoinMetrics price history unavailable"}
    out = {"available": True, "as_of": hist[-1][0], "n_daily_closes": len(hist),
           "price": round(hist[-1][1], 2),
           "rainbow": rainbow_bands(hist), "pi_cycle": pi_cycle_top(hist)}
    out.update(mayer_and_200w(hist))
    out["joins"] = ("MVRV / NUPL / Puell / SOPR are computed elsewhere in this "
                    "same document and across the fleet — not recomputed here.")
    return out


def lambda_handler(event, context):
    s3 = boto3.client("s3")
    started = time.time()

    btc = fetch_btc_metrics()
    eth = fetch_eth_metrics()
    interp = interpret(btc, eth)

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "btc": btc,
        "eth": eth,
        "interpretation": interp,
        "fetch_duration_s": round(time.time() - started, 1),
    }
    output = _cq_join(output, s3, S3_BUCKET)
    try:
        output["cycle_bands"] = build_cycle_bands()
    except Exception as e:
        print(f"[cycle_bands] failed: {e}")
        output["cycle_bands"] = {"available": False, "reason": str(e)[:200]}
    s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                  Body=json.dumps(output).encode(),
                  ContentType="application/json", CacheControl="no-cache")

    print(f"on-chain ratios written | BTC mvrv={btc.get('mvrv')} | ETH gas={eth.get('gas_propose_gwei')}gwei")
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        "body": json.dumps({"ok": True, "btc_mvrv": btc.get("mvrv"), "eth_gas": eth.get("gas_propose_gwei")}),
    }
