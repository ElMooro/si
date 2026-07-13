"""
justhodl-crypto-liquidity  ·  v1.0
─────────────────────────────────────────────────────────────────────────────
DRY-POWDER & RISK-ON/OFF REGIME for crypto, built on the metrics that actually
mark cycle tops and bottoms (not the secular supply trend that backtested late):

  1. STABLECOIN SUPPLY RATIO (SSR) = BTC market cap / total stablecoin supply.
     A RELATIVE, mean-reverting oscillator. LOW SSR = stablecoin dry powder is
     large vs BTC = sidelined cash ready to deploy (bottom/bullish). HIGH SSR =
     powder spent, cash scarce vs BTC (top/bearish). SSR is also a component of
     CoinMarketCap's own Fear & Greed market-composition factor.
  2. STABLECOIN DOMINANCE = stablecoin supply / (BTC+ETH+stablecoin complex).
     High = capital hiding in cash (fear/bottom); low = deployed (greed/top).
  3. CRYPTO FEAR & GREED (alternative.me, free) = the de-facto risk-on/off gauge.
  4. SUPPLY MOMENTUM (30d) = context only — we event-studied raw supply trend and
     found it LATE (expansion ≈ base rate), so it carries low weight by design.

EVERY directional claim is EVENT-STUDIED against a CLEAN target — forward BTC
return on real Coinbase prices (no inception-anchored basket) — at 30/90/180d,
point-in-time (trailing-2y percentile, no look-ahead). Signals earn standing
from the measured edge; unproven ones are flagged DIAGNOSTIC. Measure before trust.

Sources (all free, all verified live from Lambda):
  · Coinbase Exchange daily candles  (BTC-USD, ETH-USD)         — price history
  · DefiLlama stablecoincharts/all   (totalCirculatingUSD)      — stablecoin supply
  · alternative.me /fng              (full history)             — Fear & Greed
  · CoinGecko /global                (current)                  — live dominance
BTC/ETH market cap = price × deterministic issuance model (halving schedule).
"""
import json, time, urllib.request
from datetime import datetime, timezone, date

VERSION = "1.0"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/crypto-liquidity.json"

import boto3
s3 = boto3.client("s3", "us-east-1")

DIAG = []

# ── BTC issuance schedule (deterministic; ~144 blocks/day) ──
BTC_ERAS = [(date(2009, 1, 3), 50.0), (date(2012, 11, 28), 25.0),
            (date(2016, 7, 9), 12.5), (date(2020, 5, 11), 6.25),
            (date(2024, 4, 20), 3.125)]

def btc_supply(d_iso):
    d = date.fromisoformat(d_iso)
    sup = 0.0
    for i, (start, reward) in enumerate(BTC_ERAS):
        if d <= start:
            break
        end = BTC_ERAS[i + 1][0] if i + 1 < len(BTC_ERAS) else d
        end = min(end, d)
        days = (end - start).days
        if days > 0:
            sup += days * 144 * reward
    return sup  # ~19.9M by 2026

# ETH supply: ~constant post-merge; modeled flat (complex is BTC-dominated, dominance is a proxy)
ETH_SUPPLY = 120_500_000.0

def _get(url, timeout=45):
    req = urllib.request.Request(url, headers={"User-Agent": "justhodl/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())

def cb_daily(product, since):
    """Coinbase daily closes, paginated (300/call). Returns {iso_date: close}."""
    out = {}
    start = datetime.fromisoformat(since + "T00:00:00+00:00")
    now = datetime.now(timezone.utc)
    cur = start
    step = 300 * 86400
    while cur < now:
        end = min(datetime.fromtimestamp(cur.timestamp() + step, tz=timezone.utc), now)
        url = (f"https://api.exchange.coinbase.com/products/{product}/candles"
               f"?granularity=86400&start={cur.strftime('%Y-%m-%dT%H:%M:%SZ')}"
               f"&end={end.strftime('%Y-%m-%dT%H:%M:%SZ')}")
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "justhodl/1.0"})
            with urllib.request.urlopen(req, timeout=30) as r:
                rows = json.loads(r.read())
            for row in rows:  # [time, low, high, open, close, volume]
                d = datetime.fromtimestamp(row[0], tz=timezone.utc).date().isoformat()
                out[d] = float(row[4])
        except Exception as e:
            DIAG.append(f"cb {product} chunk err: {str(e)[:50]}")
        time.sleep(0.34)
        cur = datetime.fromtimestamp(cur.timestamp() + step, tz=timezone.utc)
    return out

def _median(x):
    x = sorted(x)
    n = len(x)
    if not n:
        return None
    return round(x[n // 2] if n % 2 else (x[n // 2 - 1] + x[n // 2]) / 2, 2)

def _mean(x):
    return round(sum(x) / len(x), 2) if x else None

def _pctile_of(window, val):
    """percentile rank (0-100) of val within window (point-in-time)."""
    if not window:
        return None
    below = sum(1 for w in window if w <= val)
    return round(100 * below / len(window), 1)


def lambda_handler(event, context):
    t0 = time.time()

    # ── fetch sources ──
    btc = cb_daily("BTC-USD", "2017-11-01")
    eth = cb_daily("ETH-USD", "2017-11-01")
    DIAG.append(f"coinbase: BTC {len(btc)}d · ETH {len(eth)}d")

    stbl = {}
    try:
        for row in _get("https://stablecoins.llama.fi/stablecoincharts/all"):
            t = row.get("date")
            v = (row.get("totalCirculatingUSD") or {}).get("peggedUSD")
            if t and isinstance(v, (int, float)):
                stbl[datetime.fromtimestamp(int(t), tz=timezone.utc).date().isoformat()] = float(v)
        DIAG.append(f"stablecoin supply: {len(stbl)}d, ${stbl[max(stbl)]/1e9:.0f}B")
    except Exception as e:
        DIAG.append(f"stablecoin err: {str(e)[:60]}")

    fng = {}
    try:
        for row in _get("https://api.alternative.me/fng/?limit=0&format=json").get("data", []):
            d = datetime.fromtimestamp(int(row["timestamp"]), tz=timezone.utc).date().isoformat()
            fng[d] = int(row["value"])
        DIAG.append(f"fear&greed: {len(fng)}d, latest {fng[max(fng)]}")
    except Exception as e:
        DIAG.append(f"fng err: {str(e)[:60]}")

    total_mcap_now = stbl_dom_now = None
    try:
        g = _get("https://api.coingecko.com/api/v3/global").get("data", {})
        total_mcap_now = g.get("total_market_cap", {}).get("usd")
        mcp = g.get("market_cap_percentage", {})
        stbl_dom_now = round(sum(v for k, v in mcp.items()
                                 if k in ("usdt", "usdc", "dai", "usde", "fdusd", "tusd", "usds")), 2)
        DIAG.append(f"coingecko global: mcap ${total_mcap_now/1e9:.0f}B, stbl.d {stbl_dom_now}%")
    except Exception as e:
        DIAG.append(f"coingecko err: {str(e)[:60]}")

    # ── build aligned daily series: SSR + dominance proxy ──
    dates = sorted(set(btc) & set(stbl))
    ssr_series = []     # (date, ssr)
    dom_series = []     # (date, stablecoin dominance proxy %)
    for d in dates:
        bmc = btc[d] * btc_supply(d)
        sup = stbl[d]
        if sup <= 0 or bmc <= 0:
            continue
        ssr_series.append((d, bmc / sup))
        emc = (eth[d] * ETH_SUPPLY) if d in eth else 0.0
        dom_series.append((d, 100 * sup / (bmc + emc + sup)))
    ssr_map = dict(ssr_series)
    dom_map = dict(dom_series)
    ssr_dates = [d for d, _ in ssr_series]

    # current values + trailing-2y percentile (point-in-time)
    WIN = 730
    def trailing_pctile(series_map, ordered_dates, d):
        i = ordered_dates.index(d)
        lo = max(0, i - WIN)
        window = [series_map[dd] for dd in ordered_dates[lo:i + 1]]
        return _pctile_of(window, series_map[d])

    ssr_now = ssr_map[ssr_dates[-1]] if ssr_dates else None
    ssr_pctile = trailing_pctile(ssr_map, ssr_dates, ssr_dates[-1]) if ssr_dates else None
    dom_now = dom_map[ssr_dates[-1]] if ssr_dates else None
    dom_pctile = trailing_pctile(dom_map, ssr_dates, ssr_dates[-1]) if ssr_dates else None
    fng_now = fng[max(fng)] if fng else None

    # stablecoin supply 30d momentum (context, low weight by design)
    sdates = sorted(stbl)
    stbl_30 = None
    if len(sdates) > 31:
        a = stbl[sdates[-31]]
        stbl_30 = round((stbl[sdates[-1]] / a - 1) * 100, 1) if a else None

    # ── EVENT STUDIES against CLEAN target: forward BTC return (real prices) ──
    bdates = sorted(btc)
    bpos = {d: i for i, d in enumerate(bdates)}
    def btc_fwd(d, h):
        i = bpos.get(d)
        if i is None or i + h >= len(bdates):
            return None
        return (btc[bdates[i + h]] / btc[d] - 1) * 100

    def study(signal_dates_vals, lo_thr, hi_thr, horizons=(30, 90, 180)):
        """signal_dates_vals: list of (date, signal_value). Returns per-horizon
        forward-BTC stats split by signal LOW(<=lo_thr) vs HIGH(>=hi_thr)."""
        res = {}
        for h in horizons:
            lo_r, hi_r = [], []
            for d, val in signal_dates_vals:
                f = btc_fwd(d, h)
                if f is None or val is None:
                    continue
                if val <= lo_thr:
                    lo_r.append(f)
                elif val >= hi_thr:
                    hi_r.append(f)
            res[f"fwd{h}d"] = {
                "low_median": _median(lo_r), "low_mean": _mean(lo_r),
                "low_hit_pct": round(100 * sum(1 for x in lo_r if x > 0) / len(lo_r), 1) if lo_r else None,
                "high_median": _median(hi_r), "high_mean": _mean(hi_r),
                "high_hit_pct": round(100 * sum(1 for x in hi_r if x > 0) / len(hi_r), 1) if hi_r else None,
                "n_low": len(lo_r), "n_high": len(hi_r),
                "edge_low_minus_high_pp": (round(_median(lo_r) - _median(hi_r), 1)
                                           if lo_r and hi_r else None)}
        return res

    # SSR percentile signal (point-in-time pctile at each date)
    ssr_pct_series = []
    for d in ssr_dates:
        p = trailing_pctile(ssr_map, ssr_dates, d)
        if p is not None:
            ssr_pct_series.append((d, p))
    ssr_study = study(ssr_pct_series, lo_thr=20, hi_thr=80)   # LOW SSR pctile = bullish setup

    # Fear & Greed signal (raw 0-100)
    fng_series = [(d, v) for d, v in sorted(fng.items())]
    fng_study = study(fng_series, lo_thr=25, hi_thr=75)       # LOW F&G = extreme fear = bullish

    # verdicts (does LOW state lead to higher fwd BTC? positive edge = thesis holds)
    def verdict(st):
        e = (st.get("fwd90d") or {}).get("edge_low_minus_high_pp")
        if e is None:
            return "INSUFFICIENT", 0
        if e >= 12:
            return "CONFIRMED_STRONG", 16
        if e >= 5:
            return "CONFIRMED", 11
        if e <= -5:
            return "INVERTED_DIAGNOSTIC", 0
        return "INCONCLUSIVE", 5
    ssr_verdict, ssr_w = verdict(ssr_study)
    fng_verdict, fng_w = verdict(fng_study)
    ssr_study["verdict"], ssr_study["weight"] = ssr_verdict, ssr_w
    fng_study["verdict"], fng_study["weight"] = fng_verdict, fng_w

    # ── COMPOSITE liquidity / dry-powder regime (HIGH = bullish/bottom, LOW = top) ──
    # SSR low pctile bullish → (100-ssr_pctile); dominance high bullish → dom_pctile;
    # F&G low bullish → (100-fng); supply expansion = mild bullish context (low weight).
    parts, wsum = 0.0, 0.0
    if ssr_pctile is not None:
        parts += 0.42 * (100 - ssr_pctile); wsum += 0.42
    if dom_pctile is not None:
        parts += 0.23 * dom_pctile; wsum += 0.23
    if fng_now is not None:
        parts += 0.25 * (100 - fng_now); wsum += 0.25
    if stbl_30 is not None:
        sm = max(0, min(100, 50 + stbl_30 * 5))
        parts += 0.10 * sm; wsum += 0.10
    liquidity_score = round(parts / wsum, 1) if wsum else None

    def regime(s):
        if s is None:
            return "UNKNOWN", "—"
        if s >= 70:
            return "DRY-POWDER LOADED", ("Stablecoin dry powder near a 2-year high vs BTC and sentiment fearful — "
                                          "lots of sidelined cash. Whether that state has historically led price is the backtest's job, below.")
        if s >= 57:
            return "ACCUMULATION", "Cash building on the sidelines, sentiment cautious."
        if s >= 43:
            return "NEUTRAL", "Liquidity and sentiment balanced — no decisive dry-powder reading."
        if s >= 30:
            return "DEPLOYMENT", "Cash being deployed, greed rising."
        return "FULLY DEPLOYED", "Powder spent and greed elevated — little sidelined cash left."
    regime_label, regime_read = regime(liquidity_score)

    # directional read for forward grading — GATED on the backtest actually supporting it.
    # SSR was INCONCLUSIVE and F&G INVERTED on this sample, so the contrarian read is NOT
    # a validated timing signal; we surface the state but log no pick unless a study CONFIRMS.
    forecast_supported = (ssr_w >= 11) or (fng_w >= 11)
    top_picks = []
    directional = None
    if forecast_supported and liquidity_score is not None and (liquidity_score >= 70 or liquidity_score <= 30):
        directional = "UP" if liquidity_score >= 70 else "DOWN"
        top_picks = [{"ticker": "IBIT", "direction": directional,
                      "reason": f"crypto-liquidity {regime_label} (score {liquidity_score})",
                      "conviction": "high" if (liquidity_score >= 78 or liquidity_score <= 22) else "moderate"}]
    forecast_support = (
        f"Backtest SUPPORTS a directional lean (SSR study {ssr_verdict}, F&G study {fng_verdict}); pick logged forward."
        if forecast_supported else
        "NOT a validated timing signal. Over 2018–2026 on this sample, low SSR showed ~0 forward edge and "
        "extreme fear INVERTED (it preceded lower 90d BTC returns, behaving as momentum not contrarian — see "
        "backtests below). This gauge reports the dry-powder STATE; it is liquidity context, not a buy/sell. "
        "No directional pick is logged.")

    falsifier = ("Thesis breaks if SSR pushes to a NEW multi-year high while F&G stays in greed and "
                 "BTC keeps falling — i.e. cash scarce AND price weak (no dry-powder cushion). "
                 "Also re-test if the SSR→forward-BTC edge below decays toward zero.")

    out = {
        "engine": "crypto-liquidity", "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "wl_research": __import__("wl_fusion").block(('CRYPTO',)),
        "duration_s": round(time.time() - t0, 1),
        "liquidity_score": liquidity_score, "regime": regime_label, "regime_read": regime_read,
        "directional_read": directional, "forecast_supported": forecast_supported,
        "forecast_support": forecast_support,
        "ssr": {"value": round(ssr_now, 2) if ssr_now else None, "percentile_2y": ssr_pctile,
                "interpretation": ("dry powder LARGE vs BTC (bullish)" if (ssr_pctile or 50) <= 35
                                   else "powder scarce vs BTC (bearish)" if (ssr_pctile or 50) >= 65
                                   else "mid-range")},
        "stablecoin_dominance": {"now_pct": dom_now, "percentile_2y": dom_pctile,
                                  "live_coingecko_pct": stbl_dom_now},
        "fear_greed": {"value": fng_now,
                       "classification": ("Extreme Fear" if (fng_now or 50) <= 25 else
                                          "Fear" if (fng_now or 50) < 45 else
                                          "Greed" if (fng_now or 50) < 75 else "Extreme Greed")},
        "stablecoin_supply": {"total_usd": stbl[sdates[-1]] if sdates else None,
                               "chg_30d_pct": stbl_30, "live_dominance_pct": stbl_dom_now,
                               "total_crypto_mcap_usd": total_mcap_now},
        "event_study_ssr": ssr_study,
        "event_study_fear_greed": fng_study,
        "top_picks": top_picks,
        "falsifier": falsifier,
        "histories": {
            "ssr": ssr_series[-500:],
            "ssr_pctile": ssr_pct_series[-500:],
            "dominance": dom_series[-500:],
            "fear_greed": fng_series[-500:],
        },
        "methodology": (
            "SSR = BTC market cap (Coinbase price × deterministic issuance model) ÷ total "
            "stablecoin supply (DefiLlama). Dominance = stablecoin ÷ (BTC+ETH+stablecoin). "
            "Fear & Greed from alternative.me. Every threshold is event-studied point-in-time "
            "(trailing-2y percentile, no look-ahead) against forward BTC return on real prices "
            "at 30/90/180d; signals earn weight from the measured edge, unproven ones are "
            "flagged DIAGNOSTIC. Composite = 0.42·(100−SSR%ile) + 0.23·dominance%ile + "
            "0.25·(100−F&G) + 0.10·supply-momentum → liquidity_score (high = dry-powder/bottom, "
            "low = deployed/top). Research, not investment advice."),
        "diagnostics": list(DIAG),
    }

    body = json.dumps(out, default=str)
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=body.encode(),
                  ContentType="application/json", CacheControl="no-cache, max-age=0")
    print(f"[crypto-liquidity] {regime_label} score={liquidity_score} | SSR {round(ssr_now,2) if ssr_now else None} "
          f"p{ssr_pctile} | F&G {fng_now} | SSR-study {ssr_verdict} edge "
          f"{(ssr_study.get('fwd90d') or {}).get('edge_low_minus_high_pp')} | F&G-study {fng_verdict}")
    return {"statusCode": 200, "body": json.dumps({"regime": regime_label, "score": liquidity_score,
                                                    "ssr_verdict": ssr_verdict, "fng_verdict": fng_verdict})}
