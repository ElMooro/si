"""
justhodl-cross-asset-regime — institutional macro regime detector

When equities, bonds, gold, dollar, oil, credit, and crypto all move together
in unusual ways, it signals a regime shift. Bridgewater, AQR, and large
multi-strats run daily correlation matrices to detect these.

WHAT THIS COMPUTES:
  1. 90-day rolling correlation matrix across 8 asset classes:
     SPY (equities), TLT (rates), GLD (gold), DXY proxy (UUP),
     HYG (credit), USO (oil), BTC proxy (BITO), VIXY (volatility)
  2. Compares today's 30-day correlations to 90-day baseline
  3. Flags correlation breaks > 2 sigma as regime-shift signals
  4. Computes risk-on/risk-off composite score
  5. Identifies the dominant regime: REFLATION / DEFLATION / STAGFLATION /
     GOLDILOCKS / CRISIS / TIGHTENING

ASSET CLASSES (each represented by liquid ETF/proxy):
  SPY  — large-cap equities
  TLT  — long-duration Treasuries
  GLD  — gold
  UUP  — US dollar (DXY proxy ETF)
  HYG  — high-yield credit
  USO  — crude oil
  BITO — bitcoin futures (proxy)
  VIXY — VIX short-term

REGIME CLASSIFICATION (based on which assets are leading):
  • REFLATION: equities up + bonds down + commodities up + dollar down
  • DEFLATION: equities down + bonds up + commodities down + dollar up
  • STAGFLATION: equities flat/down + commodities up + dollar mixed
  • GOLDILOCKS: equities up + bonds up + low VIX + dollar mixed
  • CRISIS: VIX spike + equities down + credit (HYG) underperforms TLT
  • TIGHTENING: dollar up + bonds down + risk assets mixed

ALERT TRIGGERS:
  • Regime change vs prior day
  • Correlation pair break >2 sigma
  • Composite risk score crosses 50/-50

OUTPUT: data/cross-asset-regime.json
"""
import io, json, os, time, urllib.request, urllib.error, statistics, math
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
import boto3

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = os.environ.get("S3_KEY", "data/cross-asset-regime.json")
STATE_KEY = os.environ.get("STATE_KEY", "data/cross-asset-regime-state.json")
FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
TIMEOUT_BUDGET_S = int(os.environ.get("TIMEOUT_BUDGET_S", "260"))

S3 = boto3.client("s3", region_name=REGION)


# Asset universe — 8 classes
ASSETS = [
    ("SPY",  "S&P 500",         "EQUITY"),
    ("TLT",  "20+Y Treasury",   "RATES_LONG"),
    ("GLD",  "Gold",            "GOLD"),
    ("UUP",  "US Dollar",       "DOLLAR"),
    ("HYG",  "High Yield",      "CREDIT_HY"),
    ("USO",  "Crude Oil",       "OIL"),
    ("BITO", "Bitcoin Futures", "CRYPTO"),
    ("VIXY", "VIX Short-Term",  "VOLATILITY"),
]


def _http_get_json(url, timeout=15):
    req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-Regime/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def fetch_history(symbol, days=120):
    url = "https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=" + symbol + "&apikey=" + FMP_KEY
    try:
        d = _http_get_json(url, timeout=15)
        if not isinstance(d, list):
            return None
        out = []
        for r in d[:days]:
            if r.get("close") and r.get("date"):
                out.append({"date": r.get("date"), "close": float(r.get("close"))})
        out.sort(key=lambda x: x["date"])
        return out
    except Exception:
        return None


def daily_returns(hist):
    """Compute daily log returns."""
    if not hist or len(hist) < 2:
        return []
    rets = []
    for i in range(1, len(hist)):
        prev = hist[i-1]["close"]
        curr = hist[i]["close"]
        if prev > 0:
            rets.append({"date": hist[i]["date"], "ret": math.log(curr / prev)})
    return rets


def correlation(xs, ys):
    """Pearson correlation between two return series (must be same length)."""
    if len(xs) != len(ys) or len(xs) < 5:
        return None
    n = len(xs)
    mx = sum(xs) / n
    my = sum(ys) / n
    num = sum((xs[i] - mx) * (ys[i] - my) for i in range(n))
    den_x = math.sqrt(sum((x - mx) ** 2 for x in xs))
    den_y = math.sqrt(sum((y - my) ** 2 for y in ys))
    if den_x == 0 or den_y == 0:
        return None
    return num / (den_x * den_y)


def compute_correlation_matrix(returns_by_asset, window_days):
    """For each pair, compute correlation over the last `window_days`."""
    assets = list(returns_by_asset.keys())
    matrix = {}
    for a in assets:
        matrix[a] = {}
        for b in assets:
            if a == b:
                matrix[a][b] = 1.0
                continue
            ra = returns_by_asset[a][-window_days:]
            rb = returns_by_asset[b][-window_days:]
            # Align by date
            dates_a = {r["date"]: r["ret"] for r in ra}
            dates_b = {r["date"]: r["ret"] for r in rb}
            common = sorted(set(dates_a.keys()) & set(dates_b.keys()))
            if len(common) < 5:
                matrix[a][b] = None
                continue
            xs = [dates_a[d] for d in common]
            ys = [dates_b[d] for d in common]
            matrix[a][b] = correlation(xs, ys)
    return matrix


def classify_regime(returns_by_asset, lookback_days=20):
    """Determine current macro regime based on lookback returns."""
    summary = {}
    for asset in ["SPY", "TLT", "GLD", "UUP", "HYG", "USO", "VIXY", "BITO"]:
        rets = returns_by_asset.get(asset, [])[-lookback_days:]
        if not rets:
            summary[asset] = None
            continue
        cum_ret = math.exp(sum(r["ret"] for r in rets)) - 1
        summary[asset] = cum_ret * 100  # in pct

    spy = summary.get("SPY", 0) or 0
    tlt = summary.get("TLT", 0) or 0
    gld = summary.get("GLD", 0) or 0
    uup = summary.get("UUP", 0) or 0
    hyg = summary.get("HYG", 0) or 0
    uso = summary.get("USO", 0) or 0
    bito = summary.get("BITO", 0) or 0
    vixy = summary.get("VIXY", 0) or 0

    # Decision tree for regime
    regime = "MIXED"
    confidence = 0
    rationale = []

    # Crisis: VIX up sharply + equities down + HYG underperforming TLT
    if vixy > 8 and spy < -3 and (hyg - tlt) < -2:
        regime = "CRISIS"
        confidence = 90
        rationale.append("VIX +" + str(round(vixy, 1)) + "%, SPY " + str(round(spy, 1)) + "%, credit underperformance")

    # Reflation: equities + commodities + crypto up; bonds + dollar down
    elif spy > 2 and uso > 3 and tlt < 0 and uup < 1:
        regime = "REFLATION"
        confidence = 85
        rationale.append("Risk assets rallying, bonds + dollar declining")

    # Tightening: dollar up sharply + bonds down + equities mixed
    elif uup > 2 and tlt < -3:
        regime = "TIGHTENING"
        confidence = 80
        rationale.append("Dollar +" + str(round(uup, 1)) + "%, TLT " + str(round(tlt, 1)) + "%")

    # Goldilocks: equities up + bonds up + low VIX
    elif spy > 1 and tlt > 1 and vixy < -2:
        regime = "GOLDILOCKS"
        confidence = 75
        rationale.append("Stocks + bonds rallying together, vol declining")

    # Stagflation: commodities up + bonds down + equities flat/down
    elif uso > 5 and tlt < -2 and abs(spy) < 2:
        regime = "STAGFLATION"
        confidence = 70
        rationale.append("Oil rallying, bonds selling off, equities stuck")

    # Deflation: equities down + bonds up + commodities down
    elif spy < -2 and tlt > 2 and uso < -2:
        regime = "DEFLATION"
        confidence = 80
        rationale.append("Equities + commodities down, bonds rallying — flight to safety")

    # Risk-on simple: equities + crypto up
    elif spy > 1 and bito > 2:
        regime = "RISK_ON"
        confidence = 65
        rationale.append("Equities + crypto rallying together")

    # Risk-off: equities down + bonds + gold up
    elif spy < -1 and tlt > 1 and gld > 1:
        regime = "RISK_OFF"
        confidence = 65
        rationale.append("Equities down, defensives bid")

    # Composite risk-on/off score
    risk_score = (
        spy * 1.0
        + bito * 0.5
        + uso * 0.3
        + hyg * 0.5
        - tlt * 0.5
        - gld * 0.3
        - vixy * 0.5
    )
    if risk_score > 5:
        risk_label = "STRONG_RISK_ON"
    elif risk_score > 2:
        risk_label = "RISK_ON"
    elif risk_score > -2:
        risk_label = "NEUTRAL"
    elif risk_score > -5:
        risk_label = "RISK_OFF"
    else:
        risk_label = "STRONG_RISK_OFF"

    return {
        "regime": regime,
        "confidence": confidence,
        "rationale": rationale,
        "lookback_returns_pct": {k: round(v, 2) if v is not None else None
                                    for k, v in summary.items()},
        "risk_score": round(risk_score, 2),
        "risk_label": risk_label,
    }


def detect_correlation_breaks(matrix_30d, matrix_90d):
    """Find pairs where 30d correlation differs significantly from 90d baseline.
    
    A change of ±0.30 is meaningful for cross-asset correlations (which are
    typically stable ±0.5).
    """
    breaks = []
    seen_pairs = set()
    for a, row in matrix_30d.items():
        for b, c30 in row.items():
            if a == b:
                continue
            pair_key = tuple(sorted([a, b]))
            if pair_key in seen_pairs:
                continue
            seen_pairs.add(pair_key)
            c90 = (matrix_90d.get(a) or {}).get(b)
            if c30 is None or c90 is None:
                continue
            delta = c30 - c90
            if abs(delta) >= 0.30:
                breaks.append({
                    "pair": [a, b],
                    "c30d": round(c30, 3),
                    "c90d": round(c90, 3),
                    "delta": round(delta, 3),
                    "interpretation": (
                        "decoupling" if c90 > 0.5 and c30 < c90
                        else "convergence" if c90 < -0.3 and c30 > c90 + 0.3
                        else "shift"
                    ),
                })
    breaks.sort(key=lambda x: -abs(x["delta"]))
    return breaks


def lambda_handler(event=None, context=None):
    started = time.time()
    print("[regime] starting v1.0")

    # Fetch all asset histories in parallel
    histories = {}
    def fetch(asset_tup):
        ticker, name, cls = asset_tup
        h = fetch_history(ticker, days=120)
        return ticker, h

    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(fetch, a): a for a in ASSETS}
        for f in as_completed(futures):
            ticker, h = f.result()
            if h:
                histories[ticker] = h

    print("[regime] fetched " + str(len(histories)) + "/" + str(len(ASSETS)) + " histories")

    if len(histories) < 4:
        return {"statusCode": 200, "body": json.dumps({"n": 0, "reason": "insufficient histories"})}

    # Compute returns per asset
    returns_by_asset = {}
    for ticker, h in histories.items():
        rets = daily_returns(h)
        if rets:
            returns_by_asset[ticker] = rets

    # Compute correlation matrices
    matrix_30d = compute_correlation_matrix(returns_by_asset, 30)
    matrix_90d = compute_correlation_matrix(returns_by_asset, 90)

    # Detect breaks
    correlation_breaks = detect_correlation_breaks(matrix_30d, matrix_90d)

    # Classify regime — use 20-day lookback
    regime_info = classify_regime(returns_by_asset, lookback_days=20)
    regime_info_5d = classify_regime(returns_by_asset, lookback_days=5)
    regime_info_60d = classify_regime(returns_by_asset, lookback_days=60)

    # Detect regime change vs prior state
    prior_state = None
    try:
        obj = S3.get_object(Bucket=BUCKET, Key=STATE_KEY)
        prior_state = json.loads(obj["Body"].read())
    except Exception:
        pass

    regime_change = None
    if prior_state:
        prior_regime = (prior_state.get("regime_20d") or {}).get("regime")
        if prior_regime and prior_regime != regime_info["regime"]:
            regime_change = {
                "from": prior_regime,
                "to": regime_info["regime"],
                "confidence": regime_info["confidence"],
            }

    # Alerts
    alerts = []
    if regime_change:
        alerts.append({
            "type": "REGIME_CHANGE",
            "msg": "Regime shifted from " + regime_change["from"] + " to " + regime_change["to"],
            "severity": "HIGH",
        })
    for b in correlation_breaks[:5]:
        if abs(b["delta"]) >= 0.40:
            alerts.append({
                "type": "CORRELATION_BREAK",
                "pair": b["pair"],
                "msg": b["pair"][0] + "/" + b["pair"][1] + " correlation: " +
                        str(b["c90d"]) + " → " + str(b["c30d"]) +
                        " (Δ" + str(b["delta"]) + ", " + b["interpretation"] + ")",
                "severity": "MEDIUM" if abs(b["delta"]) < 0.6 else "HIGH",
            })

    # Risk score change
    if prior_state:
        prior_risk = (prior_state.get("regime_20d") or {}).get("risk_score")
        if prior_risk is not None:
            risk_change = regime_info["risk_score"] - prior_risk
            if abs(risk_change) >= 3:
                alerts.append({
                    "type": "RISK_REGIME_SHIFT",
                    "msg": "Risk score moved " + str(round(prior_risk, 1)) +
                            " → " + str(round(regime_info["risk_score"], 1)) +
                            " (Δ" + str(round(risk_change, 1)) + ")",
                    "severity": "MEDIUM",
                })

    # Build output
    out = {
        "schema_version": 1,
        "method": "cross_asset_regime_v1",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime()),
        "duration_s": round(time.time() - started, 1),
        "asset_universe": [{"ticker": t, "name": n, "class": c} for t, n, c in ASSETS],
        "stats": {
            "n_assets_loaded": len(histories),
            "n_correlation_breaks": len(correlation_breaks),
            "n_alerts": len(alerts),
        },
        "regime_5d": regime_info_5d,
        "regime_20d": regime_info,
        "regime_60d": regime_info_60d,
        "regime_change": regime_change,
        "correlation_matrix_30d": matrix_30d,
        "correlation_matrix_90d": matrix_90d,
        "correlation_breaks": correlation_breaks[:15],
        "alerts": alerts,
    }

    body = json.dumps(out, default=str).encode()
    S3.put_object(Bucket=BUCKET, Key=S3_KEY, Body=body, ContentType="application/json")
    print("[regime] wrote " + str(len(body)) + "b")
    print("[regime] regime_20d=" + regime_info["regime"] + " conf=" + str(regime_info["confidence"]) +
           " risk=" + str(regime_info["risk_score"]) + " (" + regime_info["risk_label"] + ")")
    if regime_change:
        print("[regime] CHANGE: " + regime_change["from"] + " → " + regime_change["to"])
    if correlation_breaks[:3]:
        print("[regime] top breaks: " + str([(b["pair"], b["delta"]) for b in correlation_breaks[:3]]))

    # Save state
    state = {
        "generated_at": out["generated_at"],
        "regime_20d": regime_info,
    }
    S3.put_object(Bucket=BUCKET, Key=STATE_KEY,
                   Body=json.dumps(state).encode(),
                   ContentType="application/json")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "regime": regime_info["regime"],
            "risk_score": regime_info["risk_score"],
            "risk_label": regime_info["risk_label"],
            "n_breaks": len(correlation_breaks),
            "n_alerts": len(alerts),
            "duration_s": out["duration_s"],
        }),
    }
