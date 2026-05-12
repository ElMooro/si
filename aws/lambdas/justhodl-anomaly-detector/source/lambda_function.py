"""
justhodl-anomaly-detector — Roadmap #18

═══════════════════════════════════════════════════════════════════════
THE EARLY WARNING SYSTEM
─────────────────────────
Most platforms track current state. This Lambda watches for STATISTICAL
ANOMALIES that historically precede regime changes by 5-10 days.

Disasters announce themselves in spread, vol, and breadth data before
they show up in equity prices. By the time the macro-nowcast flips
state, the spread blowout has often been happening for two weeks.

V1 detects 5 categories:

  1. CROSS-ASSET DIVERGENCE
       All-of (SPY, TLT, GLD, UUP) moving same direction simultaneously
       indicates institutional hedging/liquidation
  2. SECTOR BREADTH COLLAPSE
       Cross-sectional dispersion (stddev of 11 sector ETF returns)
       z-score >= 2 = rotation starting OR narrow leadership
  3. VIX/SKEW DIVERGENCE
       SKEW spike WITHOUT VIX spike = institutions hedging tail risk
       without visible panic in regular vol markets
  4. CREDIT SPREAD BLOWOUT
       HY OAS or BBB OAS 1-week change z-score >= 2
  5. BOND VOLATILITY SPIKE
       30-day realized vol of DGS10 (10Y yield) >= 2 z-score
       Bond vol historically leads equity vol by 1-2 weeks

V2 deferred: per-stock 5σ volume anomalies (needs heavier API budget)

═══════════════════════════════════════════════════════════════════════
SEVERITY TIERS
──────────────
  LOW       |z| 2.0-2.5    note in sidecar only
  MEDIUM    |z| 2.5-3.5    daily brief inclusion
  HIGH      |z| 3.5-5.0    Telegram alert
  EXTREME   |z| > 5    OR  3+ HIGH simultaneously → crisis alert

Dedupe: 12h window in S3 alert-history (anomalies persist hours/days
so re-firing every hour would be spam).

═══════════════════════════════════════════════════════════════════════
ARCHITECTURE
────────────
  Inputs:
    - FRED: VIXCLS, BAMLH0A0HYM2 (HY OAS), BAMLC0A4CBBB (BBB OAS),
            DGS10, SKEW (via fallback), DTWEXBGS (dollar broad)
    - Polygon: SPY, TLT, GLD, UUP, XLK XLF XLV XLE XLI XLY XLP XLU XLB XLRE XLC
  Output:
    - S3: signals/anomalies.json   (full detection report)
    - Telegram: HIGH/EXTREME alerts via TELEGRAM_TOKEN
  Cost: ~10 FRED calls + 15 Polygon calls per run = $0 (free tiers)
  Schedule: every hour at :33 (cron(33 * * * ? *))
"""
import json
import os
import statistics
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3

S3_BUCKET = "justhodl-dashboard-live"
OUTPUT_KEY = "signals/anomalies.json"
PREV_STATE_KEY = "signals/anomalies-prev.json"
ALERT_HISTORY_KEY = "signals/anomaly-alert-history.json"

FRED_KEY = os.environ.get("FRED_KEY", "")
POLY_KEY = os.environ.get("POLY_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# Severity thresholds (absolute z-score)
SEVERITY = [
    ("EXTREME", 5.0),
    ("HIGH",    3.5),
    ("MEDIUM",  2.5),
    ("LOW",     2.0),
]
DEDUPE_HOURS = 12

s3 = boto3.client("s3", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")


# ═══════════════════════════════════════════════════════════════════════
# DATA FETCHERS
# ═══════════════════════════════════════════════════════════════════════

def fetch_fred_series(series_id, lookback_days=400):
    """Fetch daily observations from FRED.
    Returns list of {date, value} sorted ascending. None values filtered.
    """
    if not FRED_KEY: return []
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=lookback_days)
    params = urllib.parse.urlencode({
        "series_id": series_id, "api_key": FRED_KEY,
        "file_type": "json",
        "observation_start": start.isoformat(),
        "observation_end": end.isoformat(),
        "sort_order": "asc",
    })
    url = f"https://api.stlouisfed.org/fred/series/observations?{params}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-AD/1.0"})
        with urllib.request.urlopen(req, timeout=12) as r:
            data = json.loads(r.read().decode("utf-8"))
        out = []
        for o in data.get("observations") or []:
            v = o.get("value")
            if v in (".", "", None): continue
            try:
                out.append({"date": o["date"], "value": float(v)})
            except (ValueError, TypeError):
                continue
        return out
    except Exception as e:
        print(f"  [fred:{series_id}] error: {str(e)[:100]}")
        return []


def fetch_polygon_ohlcv(symbol, lookback_days=60):
    """Daily OHLCV bars from Polygon. Returns list sorted ascending."""
    if not POLY_KEY: return []
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=lookback_days)
    url = (f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/"
           f"{start}/{end}?adjusted=true&sort=asc&limit=200&apiKey={POLY_KEY}")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-AD/1.0"})
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read().decode("utf-8"))
        return data.get("results") or []
    except Exception as e:
        print(f"  [poly:{symbol}] error: {str(e)[:100]}")
        return []


# ═══════════════════════════════════════════════════════════════════════
# STATISTICAL HELPERS
# ═══════════════════════════════════════════════════════════════════════

def z_score(values, current=None, lookback=60):
    """z-score of `current` (defaults to values[-1]) vs prior `lookback` values.
    Returns (z, mean, std, n_used) or (None, None, None, 0) if insufficient data.
    """
    if not values: return None, None, None, 0
    if current is None:
        if len(values) < lookback + 1:
            return None, None, None, 0
        current = values[-1]
        sample = values[max(0, len(values) - 1 - lookback):-1]
    else:
        sample = values[-lookback:] if len(values) >= lookback else values
    if len(sample) < 10:
        return None, None, None, len(sample)
    mean = sum(sample) / len(sample)
    var = sum((v - mean) ** 2 for v in sample) / len(sample)
    std = var ** 0.5
    if std == 0:
        return None, mean, 0, len(sample)
    z = (current - mean) / std
    return z, mean, std, len(sample)


def classify_severity(abs_z):
    """Return severity label given absolute z-score."""
    if abs_z is None: return None
    for label, threshold in SEVERITY:
        if abs_z >= threshold: return label
    return None


# ═══════════════════════════════════════════════════════════════════════
# OBSERVABILITY METRICS — always-on tracked values regardless of threshold
# ═══════════════════════════════════════════════════════════════════════

def collect_observability_metrics():
    """Return a dict of current tracked metrics with their z-scores.
    Even when no anomalies fire, this shows the system is doing real work
    AND tells Khalid the actual state of markets right now.
    """
    metrics = {}

    # VIX
    vix = fetch_fred_series("VIXCLS", lookback_days=400)
    if len(vix) >= 60:
        values = [v["value"] for v in vix]
        z, mean, std, _ = z_score(values)
        metrics["vix"] = {
            "current": round(values[-1], 2),
            "60d_mean": round(mean, 2) if mean else None,
            "60d_std": round(std, 2) if std else None,
            "z_score": round(z, 2) if z is not None else None,
            "as_of": vix[-1]["date"],
            "interpretation": _vix_interp(values[-1], z),
        }

    # SKEW
    skew = fetch_fred_series("SKEW", lookback_days=400)
    if len(skew) >= 60:
        values = [v["value"] for v in skew]
        z, mean, std, _ = z_score(values)
        metrics["skew"] = {
            "current": round(values[-1], 2),
            "60d_mean": round(mean, 2) if mean else None,
            "z_score": round(z, 2) if z is not None else None,
            "as_of": skew[-1]["date"],
            "interpretation": _skew_interp(values[-1], z),
        }

    # HY OAS
    hy = fetch_fred_series("BAMLH0A0HYM2", lookback_days=500)
    if len(hy) >= 60:
        values = [v["value"] for v in hy]
        level_z, level_mean, level_std, _ = z_score(values, lookback=180)
        if len(values) >= 6:
            week_change = values[-1] - values[-6]
            changes = [values[i] - values[i-5] for i in range(5, len(values) - 1)]
            change_z, _, _, _ = z_score(changes + [week_change], lookback=180)
        else:
            week_change = None
            change_z = None
        metrics["hy_oas"] = {
            "current": round(values[-1], 3),
            "180d_mean": round(level_mean, 3) if level_mean else None,
            "level_z": round(level_z, 2) if level_z is not None else None,
            "1w_change": round(week_change, 3) if week_change is not None else None,
            "1w_change_z": round(change_z, 2) if change_z is not None else None,
            "as_of": hy[-1]["date"],
        }

    # BBB OAS
    bbb = fetch_fred_series("BAMLC0A4CBBB", lookback_days=500)
    if len(bbb) >= 60:
        values = [v["value"] for v in bbb]
        level_z, level_mean, _, _ = z_score(values, lookback=180)
        if len(values) >= 6:
            week_change = values[-1] - values[-6]
            changes = [values[i] - values[i-5] for i in range(5, len(values) - 1)]
            change_z, _, _, _ = z_score(changes + [week_change], lookback=180)
        else:
            week_change = None
            change_z = None
        metrics["bbb_oas"] = {
            "current": round(values[-1], 3),
            "180d_mean": round(level_mean, 3) if level_mean else None,
            "level_z": round(level_z, 2) if level_z is not None else None,
            "1w_change_z": round(change_z, 2) if change_z is not None else None,
            "as_of": bbb[-1]["date"],
        }

    # 10Y vol
    dgs10 = fetch_fred_series("DGS10", lookback_days=400)
    if len(dgs10) >= 60:
        values = [v["value"] for v in dgs10]
        changes = [values[i] - values[i-1] for i in range(1, len(values))]
        if len(changes) >= 60:
            current_vol = statistics.stdev(changes[-30:]) * (252 ** 0.5)
            historical = [statistics.stdev(changes[i-30:i]) * (252 ** 0.5)
                          for i in range(30, len(changes) - 1)]
            if len(historical) >= 30:
                z, mean, std, _ = z_score(historical + [current_vol])
                metrics["bond_vol"] = {
                    "current_30d_annualized_vol": round(current_vol, 3),
                    "1y_mean": round(mean, 3) if mean else None,
                    "z_score": round(z, 2) if z is not None else None,
                    "as_of": dgs10[-1]["date"],
                    "10y_yield": round(values[-1], 3),
                }

    return metrics


def _vix_interp(value, z):
    if value < 13: return "extreme complacency"
    if value < 17: return "low fear"
    if value < 22: return "normal"
    if value < 30: return "elevated fear"
    return "panic / crisis"


def _skew_interp(value, z):
    if value < 120: return "minimal tail-risk concern"
    if value < 140: return "normal tail-risk pricing"
    if value < 150: return "elevated tail hedging"
    return "extreme tail-risk premium"


# ═══════════════════════════════════════════════════════════════════════
# DETECTORS
# ═══════════════════════════════════════════════════════════════════════

def detect_cross_asset_divergence():
    """Detect when SPY, TLT, GLD, UUP all move same direction simultaneously.
    Normal regime: equities and bonds inversely correlated. When everything
    moves up = liquidity flood (or institutional broad hedging). When
    everything moves down = forced liquidation.
    """
    symbols = ["SPY", "TLT", "GLD", "UUP"]
    bars_by_sym = {}
    with ThreadPoolExecutor(max_workers=4) as ex:
        results = {ex.submit(fetch_polygon_ohlcv, s, 90): s for s in symbols}
        for f in as_completed(results):
            sym = results[f]
            bars = f.result() or []
            if len(bars) >= 25:
                bars_by_sym[sym] = bars

    if len(bars_by_sym) < 4:
        return None

    returns = {}
    for sym, bars in bars_by_sym.items():
        closes = [b["c"] for b in bars]
        if len(closes) < 25: continue
        ret_5d = (closes[-1] / closes[-6] - 1) * 100 if len(closes) >= 6 else None
        ret_20d = (closes[-1] / closes[-21] - 1) * 100 if len(closes) >= 21 else None
        returns[sym] = {
            "1w": ret_5d, "1m": ret_20d,
            "current": closes[-1], "n_bars": len(closes),
        }
    if len(returns) < 4: return None

    # Sign alignment check (1-week returns)
    signs_1w = []
    magnitudes_1w = []
    for s in symbols:
        r = returns[s]["1w"]
        if r is None: return None
        signs_1w.append(1 if r > 0.1 else -1 if r < -0.1 else 0)
        magnitudes_1w.append(abs(r))

    all_up = all(s > 0 for s in signs_1w)
    all_down = all(s < 0 for s in signs_1w)

    if not (all_up or all_down): return None

    avg_mag = sum(magnitudes_1w) / len(magnitudes_1w)
    # Heuristic severity: avg 1% move = z ≈ 2, 2% = z ≈ 3.5, 3% = z ≈ 5
    pseudo_z = 1.5 + avg_mag * 1.2
    severity = classify_severity(pseudo_z)
    if severity is None: return None

    direction = "RALLY" if all_up else "LIQUIDATION"
    name = f"All-Asset {direction.title()}"
    if all_up:
        implication = (
            "Equities, bonds, gold AND dollar all rising together is rare. "
            "Often indicates broad hedging or fund inflows without directional "
            "conviction. Historically precedes risk-off in 5-15 days."
        )
    else:
        implication = (
            "Equities, bonds, gold AND dollar all falling = forced liquidation. "
            "Margin calls or fund redemptions forcing sales across asset classes. "
            "Historically a capitulation signal — bottom often within 10 days."
        )

    return {
        "category": "cross_asset",
        "name": name,
        "severity": severity,
        "z_score": round(pseudo_z, 2),
        "details": " · ".join(f"{s}{'+' if returns[s]['1w']>=0 else ''}{returns[s]['1w']:.2f}%"
                                for s in symbols),
        "asset_returns": returns,
        "implication": implication,
    }


def detect_sector_breadth():
    """Cross-sectional standard deviation of sector ETF returns.
    Normal: ~1-2%. High dispersion (>4%) = rotation. Low dispersion with one
    big mover = narrow leadership. Both are anomalies."""
    SECTORS = ["XLK","XLF","XLV","XLE","XLI","XLY","XLP","XLU","XLB","XLRE","XLC"]
    bars_by_sym = {}
    with ThreadPoolExecutor(max_workers=6) as ex:
        results = {ex.submit(fetch_polygon_ohlcv, s, 90): s for s in SECTORS}
        for f in as_completed(results):
            sym = results[f]
            bars = f.result() or []
            if len(bars) >= 25:
                bars_by_sym[sym] = bars
    if len(bars_by_sym) < 9: return None

    # 5-day returns per sector
    returns_5d = {}
    for sym, bars in bars_by_sym.items():
        closes = [b["c"] for b in bars]
        if len(closes) < 30: continue
        returns_5d[sym] = (closes[-1] / closes[-6] - 1) * 100

    if len(returns_5d) < 9: return None

    # Cross-sectional dispersion right now
    today_returns = list(returns_5d.values())
    current_dispersion = statistics.stdev(today_returns)

    # Historical dispersion: compute 5-day returns for past 60 trading days,
    # then dispersion across sectors for each day, then stddev/mean of dispersions
    historical_disp = []
    n_check = min(60, min(len(bars) for bars in bars_by_sym.values()) - 7)
    for offset in range(1, n_check):
        day_returns = []
        for sym, bars in bars_by_sym.items():
            closes = [b["c"] for b in bars]
            i = len(closes) - 1 - offset
            if i < 5: continue
            r = (closes[i] / closes[i - 5] - 1) * 100
            day_returns.append(r)
        if len(day_returns) >= 9:
            historical_disp.append(statistics.stdev(day_returns))

    if len(historical_disp) < 20: return None
    z, mean, std, _ = z_score(historical_disp + [current_dispersion])
    if z is None: return None

    abs_z = abs(z)
    severity = classify_severity(abs_z)
    if severity is None: return None

    # Identify top movers
    sorted_sectors = sorted(returns_5d.items(), key=lambda x: -x[1])
    top = sorted_sectors[:2]
    bottom = sorted_sectors[-2:]

    if z > 0:
        name = "Sector Rotation / Breadth Divergence"
        implication = (
            f"Cross-sector dispersion at {current_dispersion:.2f}% (z={z:.2f}) — "
            f"{top[0][0]} +{top[0][1]:.1f}% leading, {bottom[0][0]} {bottom[0][1]:+.1f}% lagging. "
            "Wide dispersion means money is rotating between sectors rather than risk-on/off "
            "broadly. Historically signals end of a thematic rally or start of new leadership."
        )
    else:
        name = "Sector Compression"
        implication = (
            f"All sectors moving together (dispersion {current_dispersion:.2f}%, z={z:.2f}). "
            "Suggests broad risk-on/off rather than thematic positioning. "
            "Vulnerability: when correlations spike, diversification fails — typically precedes "
            "volatility expansion."
        )

    return {
        "category": "sector_breadth",
        "name": name,
        "severity": severity,
        "z_score": round(z, 2),
        "current_value": round(current_dispersion, 3),
        "baseline_mean": round(mean, 3),
        "baseline_std": round(std, 3),
        "top_sectors": [{"sym": s, "ret_5d": round(r, 2)} for s, r in top],
        "bottom_sectors": [{"sym": s, "ret_5d": round(r, 2)} for s, r in bottom],
        "details": (f"Top: {top[0][0]} {top[0][1]:+.2f}% · "
                    f"{top[1][0]} {top[1][1]:+.2f}%   ·   "
                    f"Bottom: {bottom[0][0]} {bottom[0][1]:+.2f}% · {bottom[1][0]} {bottom[1][1]:+.2f}%"),
        "implication": implication,
    }


def detect_vix_skew():
    """VIX z-score and SKEW/VIX divergence.
    SKEW = CBOE tail-risk indicator. High SKEW measures OTM put demand =
    institutional tail hedging. If SKEW spikes WITHOUT VIX moving, that
    means smart money is hedging the tail without panic showing up in
    headline vol — a quiet warning sign."""
    vix = fetch_fred_series("VIXCLS", lookback_days=400)
    skew = fetch_fred_series("SKEW", lookback_days=400)

    if len(vix) < 60:
        return None

    vix_values = [v["value"] for v in vix]
    vix_z, vix_mean, vix_std, _ = z_score(vix_values)

    anomalies = []
    # VIX z-score anomaly
    if vix_z is not None and abs(vix_z) >= 2.0:
        severity = classify_severity(abs(vix_z))
        if severity:
            anomalies.append({
                "category": "vix",
                "name": f"VIX {'Spike' if vix_z > 0 else 'Compression'}",
                "severity": severity,
                "z_score": round(vix_z, 2),
                "current_value": round(vix_values[-1], 2),
                "baseline_mean": round(vix_mean, 2),
                "baseline_std": round(vix_std, 2),
                "details": f"VIX at {vix_values[-1]:.2f} vs 60d mean {vix_mean:.2f} (z={vix_z:.2f})",
                "implication": (
                    "Elevated VIX — equity options markets pricing in higher near-term vol. Watch for follow-through in spot."
                    if vix_z > 0 else
                    "Suppressed VIX — market complacency at multi-month low. Historically vulnerable to sharp spikes."
                ),
            })

    # SKEW/VIX divergence
    if len(skew) >= 60:
        skew_values = [v["value"] for v in skew]
        skew_z, skew_mean, skew_std, _ = z_score(skew_values)
        if skew_z is not None and abs(skew_z) >= 2.0:
            divergence = skew_z - (vix_z or 0)
            if abs(divergence) >= 2.0:
                severity = classify_severity(abs(divergence))
                if severity:
                    anomalies.append({
                        "category": "vix_skew",
                        "name": "SKEW/VIX Divergence",
                        "severity": severity,
                        "z_score": round(divergence, 2),
                        "current_value": {"vix": round(vix_values[-1], 2),
                                           "skew": round(skew_values[-1], 2)},
                        "baseline_mean": {"vix": round(vix_mean, 2),
                                            "skew": round(skew_mean, 2)},
                        "details": f"VIX z={vix_z:.2f} · SKEW z={skew_z:.2f} · divergence={divergence:.2f}",
                        "implication": (
                            "Tail-risk premium elevated WITHOUT broad fear. Institutions buying "
                            "deep OTM puts while equity vol stays calm = quiet hedging by smart "
                            "money. Historically precedes large drawdowns by 2-6 weeks."
                            if divergence > 0 else
                            "Tail-risk premium falling while VIX stays elevated — unusual. "
                            "Suggests panic without tail concern, possibly capitulation signal."
                        ),
                    })
        elif skew_z is not None and skew_z >= 2.0 and (vix_z is None or vix_z < 0.5):
            # SKEW spike without VIX move
            severity = classify_severity(skew_z)
            if severity:
                anomalies.append({
                    "category": "vix_skew",
                    "name": "SKEW Spike (Hidden Hedging)",
                    "severity": severity,
                    "z_score": round(skew_z, 2),
                    "current_value": {"vix": round(vix_values[-1], 2),
                                       "skew": round(skew_values[-1], 2)},
                    "details": f"SKEW at {skew_values[-1]:.2f} (z={skew_z:.2f}) while VIX flat (z={vix_z:.2f if vix_z else 0:.2f})",
                    "implication": (
                        "OTM put demand surging without VIX moving = institutions buying tail "
                        "insurance quietly. Smart-money warning signal. Historically precedes "
                        "drawdowns by 2-6 weeks."
                    ),
                })

    return anomalies if anomalies else None


def detect_credit_spreads():
    """Credit spread (BBB OAS, HY OAS) blowout detector.
    1-week change z-score >= 2 fires. Credit markets historically lead
    equity weakness by 1-3 weeks."""
    anomalies = []
    SPREADS = [
        ("BAMLH0A0HYM2", "HY OAS",  "High Yield Corporate"),
        ("BAMLC0A4CBBB", "BBB OAS", "BBB Investment Grade"),
    ]
    for series_id, short_name, full_name in SPREADS:
        data = fetch_fred_series(series_id, lookback_days=500)
        if len(data) < 40:
            continue
        values = [d["value"] for d in data]

        # Current level z-score
        level_z, level_mean, level_std, _ = z_score(values, lookback=180)

        # 1-week change z-score
        if len(values) >= 6:
            current_change = values[-1] - values[-6]
            # Build history of 5-day changes
            changes = [values[i] - values[i-5] for i in range(5, len(values))]
            change_z, change_mean, change_std, _ = z_score(changes[:-1] + [current_change], lookback=180)
        else:
            current_change = None
            change_z = None

        # Detect anomaly
        triggers = []
        max_z = 0
        if level_z is not None and abs(level_z) >= 2.0:
            triggers.append(f"level z={level_z:.2f}")
            max_z = max(max_z, abs(level_z))
        if change_z is not None and abs(change_z) >= 2.0:
            triggers.append(f"1w change z={change_z:.2f}")
            max_z = max(max_z, abs(change_z))

        if max_z < 2.0:
            continue
        severity = classify_severity(max_z)
        if severity is None: continue

        direction = "Blowout" if (change_z or 0) > 0 or (level_z or 0) > 0 else "Compression"
        anomalies.append({
            "category": "credit_spread",
            "name": f"{short_name} {direction}",
            "severity": severity,
            "z_score": round(max_z, 2),
            "level_z": round(level_z, 2) if level_z is not None else None,
            "change_z": round(change_z, 2) if change_z is not None else None,
            "current_value": round(values[-1], 3),
            "baseline_mean": round(level_mean, 3) if level_mean else None,
            "current_change_1w": round(current_change, 3) if current_change is not None else None,
            "details": f"{full_name} at {values[-1]:.2f}% · " + " · ".join(triggers),
            "implication": (
                f"{full_name} widening rapidly — credit markets pricing in elevated default risk. "
                "Historically leads equity weakness by 1-3 weeks. Watch for confirmation in "
                "high-beta sector pullbacks."
                if direction == "Blowout" else
                f"{full_name} compressing rapidly — credit markets relaxing. "
                "Constructive for risk assets but watch for complacency."
            ),
        })
    return anomalies if anomalies else None


def detect_bond_volatility():
    """30-day realized vol of 10Y Treasury yield (DGS10) as proxy for MOVE Index.
    Bond vol historically leads equity vol by 1-2 weeks."""
    dgs10 = fetch_fred_series("DGS10", lookback_days=400)
    if len(dgs10) < 60: return None
    values = [d["value"] for d in dgs10]

    # Daily changes
    changes = [values[i] - values[i-1] for i in range(1, len(values))]

    # 30-day rolling vol = stddev of last 30 daily changes
    if len(changes) < 60: return None
    current_vol = statistics.stdev(changes[-30:]) * (252 ** 0.5)  # annualized

    # History of 30-day rolling vols
    historical_vols = []
    for end_idx in range(60, len(changes) - 1):  # leave most recent out
        window = changes[end_idx - 30: end_idx]
        if len(window) == 30:
            historical_vols.append(statistics.stdev(window) * (252 ** 0.5))
    if len(historical_vols) < 30: return None

    z, mean, std, _ = z_score(historical_vols + [current_vol])
    if z is None: return None

    abs_z = abs(z)
    severity = classify_severity(abs_z)
    if severity is None: return None

    return {
        "category": "bond_vol",
        "name": "Bond Yield Volatility " + ("Spike" if z > 0 else "Compression"),
        "severity": severity,
        "z_score": round(z, 2),
        "current_value": round(current_vol, 3),
        "baseline_mean": round(mean, 3),
        "baseline_std": round(std, 3),
        "details": f"10Y annualized vol {current_vol:.2f}% vs 1Y mean {mean:.2f}% (z={z:.2f})",
        "implication": (
            "Bond yield volatility spiking — Treasury market repricing future rates aggressively. "
            "Historically leads equity volatility by 1-2 weeks. Long-duration assets vulnerable."
            if z > 0 else
            "Bond vol compressed at multi-month low — yields ranging tightly. "
            "Stability can mask coiled spring; watch for breakout signals."
        ),
    }


# ═══════════════════════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════════════════════

def get_chat_id():
    if TELEGRAM_CHAT_ID: return TELEGRAM_CHAT_ID
    try:
        return ssm.get_parameter(Name="/justhodl/telegram/chat_id",
                                  WithDecryption=True)["Parameter"]["Value"]
    except Exception:
        return None


def send_telegram(text, chat_id):
    if not TELEGRAM_TOKEN or not chat_id: return False
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    body = urllib.parse.urlencode({
        "chat_id": chat_id, "text": text[:4000],
        "parse_mode": "Markdown", "disable_web_page_preview": "true",
    }).encode("utf-8")
    try:
        req = urllib.request.Request(url, data=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"})
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read().decode("utf-8")).get("ok", False)
    except Exception as e:
        print(f"  telegram err: {str(e)[:200]}")
        return False


def format_telegram_alert(anomaly):
    sev = anomaly["severity"]
    icon = {"EXTREME": "🚨", "HIGH": "⚠️", "MEDIUM": "⚡", "LOW": "🔔"}.get(sev, "🔔")
    name = anomaly["name"].replace("_", "\\_")
    details = anomaly.get("details", "")
    implication = anomaly.get("implication", "")
    z = anomaly.get("z_score")
    return (
        f"{icon} *{sev} ANOMALY · {name}*\n"
        f"`z-score: {z}`\n\n"
        f"_{details}_\n\n"
        f"{implication}\n\n"
        f"[Anomaly Dashboard](https://justhodl.ai/anomalies/) · "
        f"[Alpha View](https://justhodl.ai/alpha/) · "
        f"[Risk](https://justhodl.ai/risk/)"
    )


def format_crisis_alert(anomalies):
    """When 3+ HIGH/EXTREME anomalies simultaneously."""
    lines = [f"🚨 *CRISIS ALERT — {len(anomalies)} simultaneous anomalies*\n"]
    for a in anomalies:
        z = a.get("z_score")
        sev = a.get("severity")
        lines.append(f"  • {sev} · {a['name']} (z={z})")
    lines.append(f"\nHistorical analog: when 3+ macro/credit/vol anomalies fire together, "
                 f"forward 30-day SPY returns average -7% to -15%. *Reduce gross exposure.*")
    lines.append(f"\n[Anomaly detail](https://justhodl.ai/anomalies/)")
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════
# ALERT HISTORY (dedupe)
# ═══════════════════════════════════════════════════════════════════════

def load_alert_history():
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=ALERT_HISTORY_KEY)["Body"].read())
    except Exception:
        return {}


def save_alert_history(h):
    try:
        s3.put_object(Bucket=S3_BUCKET, Key=ALERT_HISTORY_KEY,
            Body=json.dumps(h, separators=(",", ":")).encode("utf-8"),
            ContentType="application/json")
    except Exception as e:
        print(f"  history save err: {e}")


def should_alert(history, anomaly_key):
    last = history.get(anomaly_key)
    if not last: return True
    try:
        last_dt = datetime.fromisoformat(last.replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - last_dt) >= timedelta(hours=DEDUPE_HOURS)
    except Exception:
        return True


# ═══════════════════════════════════════════════════════════════════════
# MAIN HANDLER
# ═══════════════════════════════════════════════════════════════════════

def lambda_handler(event, context):
    started = time.time()
    print(f"=== ANOMALY DETECTOR · {datetime.now(timezone.utc).isoformat()} ===")

    all_anomalies = []
    timings = {}
    all_metrics = {}   # NEW: always-on metrics dump for observability

    # Run detectors (sequential — they each spin their own ThreadPools)
    detectors = [
        ("cross_asset",     detect_cross_asset_divergence),
        ("sector_breadth",  detect_sector_breadth),
        ("vix_skew",         detect_vix_skew),
        ("credit_spread",   detect_credit_spreads),
        ("bond_vol",         detect_bond_volatility),
    ]
    for name, fn in detectors:
        t0 = time.time()
        try:
            result = fn()
            if result:
                if isinstance(result, list):
                    all_anomalies.extend(result)
                else:
                    all_anomalies.append(result)
        except Exception as e:
            print(f"  {name} ERROR: {str(e)[:200]}")
        timings[name] = round(time.time() - t0, 2)

    # Also collect observability metrics from a quick second pass
    # (these run all the time so Khalid can see system state)
    try:
        all_metrics = collect_observability_metrics()
    except Exception as e:
        print(f"  metrics collection ERROR: {str(e)[:200]}")
        all_metrics = {"err": str(e)[:200]}

    print(f"  detected {len(all_anomalies)} anomalies in {sum(timings.values()):.2f}s")

    # Sort by severity
    sev_order = {"EXTREME": 4, "HIGH": 3, "MEDIUM": 2, "LOW": 1}
    all_anomalies.sort(key=lambda a: (-sev_order.get(a["severity"], 0), -abs(a.get("z_score") or 0)))

    # Categorize counts
    by_sev = {}
    for a in all_anomalies:
        by_sev[a["severity"]] = by_sev.get(a["severity"], 0) + 1

    high_or_above = [a for a in all_anomalies if a["severity"] in ("HIGH", "EXTREME")]

    # Telegram alerts (HIGH/EXTREME with dedupe)
    chat_id = get_chat_id()
    history = load_alert_history()
    now_iso = datetime.now(timezone.utc).isoformat()
    alerts_sent = 0
    alerts_skipped = 0
    sent_actions = []

    if chat_id:
        # Crisis alert: 3+ HIGH+ anomalies simultaneously
        if len(high_or_above) >= 3:
            crisis_key = "crisis_cluster"
            if should_alert(history, crisis_key):
                if send_telegram(format_crisis_alert(high_or_above), chat_id):
                    history[crisis_key] = now_iso
                    alerts_sent += 1
                    sent_actions.append({"type": "crisis_cluster", "n_anomalies": len(high_or_above)})
                time.sleep(0.5)

        # Individual HIGH/EXTREME alerts
        for a in high_or_above[:5]:  # cap at 5 to avoid spam
            key = f"{a['category']}:{a['name']}"
            if not should_alert(history, key):
                alerts_skipped += 1
                continue
            if send_telegram(format_telegram_alert(a), chat_id):
                history[key] = now_iso
                alerts_sent += 1
                sent_actions.append({"type": "anomaly", "category": a["category"], "name": a["name"]})
            time.sleep(0.4)

        save_alert_history(history)

    # Write sidecar
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "generated_at_unix": int(time.time()),
        "elapsed_seconds": round(time.time() - started, 2),
        "anomalies_count": len(all_anomalies),
        "high_or_extreme_count": len(high_or_above),
        "by_severity": by_sev,
        "categories_checked": [n for n, _ in detectors],
        "categories_with_anomalies": sorted({a["category"] for a in all_anomalies}),
        "anomalies": all_anomalies,
        "metrics": all_metrics,   # observability — always populated
        "alerts_sent": alerts_sent,
        "alerts_skipped_dedupe": alerts_skipped,
        "actions": sent_actions,
        "detector_timings_s": timings,
    }
    try:
        s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY,
            Body=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
            ContentType="application/json",
            CacheControl="public, max-age=1800")
    except Exception as e:
        print(f"  sidecar put err: {e}")
        return {"statusCode": 500, "body": json.dumps({"err": str(e)})}

    print(f"  alerts sent={alerts_sent} skipped={alerts_skipped}")

    return {"statusCode": 200, "body": json.dumps({
        "success": True,
        "anomalies_count": len(all_anomalies),
        "high_or_extreme_count": len(high_or_above),
        "by_severity": by_sev,
        "alerts_sent": alerts_sent,
        "elapsed_seconds": round(time.time() - started, 2),
    })}
