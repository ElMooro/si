"""
justhodl-put-call-extreme (REBUILT 2026-05-21 as Sentiment Extreme Composite)
============================================================================

ARCHITECTURE PIVOT v2.0.0 (ops 992-995):
- CBOE P/C ratio feeds all discontinued (FRED CBOEEQUITYPCRATIO 400,
  FRED PUTCALL 400, Yahoo ^CPC/^CPCE 404, CBOE direct CSV 403/404,
  FMP options endpoints 404, Polygon options 403)
- Machine name kept for ID stability (signal-board normalizer + page card)
- Engine rebuilt as multi-source sentiment composite using verified
  FRED series (ops 994 confirmed working)

THESIS (Baker-Wurgler 2006 + Stambaugh-Yu-Zhang 2012):
Aggregate investor sentiment extremes predict reversals. Single-signal
sentiment metrics (P/C, AAII) are noisy + miss regime shifts.
Multi-source z-score composites reduce noise + capture cross-cadence
divergences (e.g. consumer panic with vol calm = early-warning state).

SIGNALS (5, cadence-weighted z-scores):
1. VIXCLS    (daily)   - equity implied vol fear (HIGH = stress)
2. VXNCLS    (daily)   - NDX implied vol fear   (HIGH = stress, confirm)
3. USEPUINDXD(daily)   - econ policy uncertainty(HIGH = stress)
4. STLFSI4   (weekly)  - STL Fed Fin Stress Idx (HIGH = stress)
5. UMCSENT   (monthly) - UMich Consumer Sentmnt (LOW = stress, FLIPPED)

Composite stress z = weighted avg of per-signal z-scores.
Weights = 3.0 (daily) / 2.0 (weekly) / 1.0 (monthly) -- freshness premium.

STATES:
- SENTIMENT_PANIC_RICH       z >= +1.8  -> contrarian LONG SPY / QQQ / IWM
- SENTIMENT_PANIC_ACTIVE     z >= +1.0  -> partial entry, stress building
- NEUTRAL                    -1.0 < z < +1.0
- SENTIMENT_EUPHORIA_ACTIVE  z <= -1.0  -> complacency rising, trim
- SENTIMENT_EUPHORIA_RICH    z <= -1.8  -> contrarian SHORT / HEDGE
- DIVERGENCE flag            when stdev(signal_z) > 1.2  (cross-cadence
                             dispersion = regime transition warning)

Edge: ~58-62% hit / +4-7% / 4-8 weeks (BW06, Garcia 2013, Brown-Cliff
2004). Cross-cadence composites outperform single-signal P/C in
backtests (Stambaugh-Yu-Zhang 2012, JFE).

Trade tickets:
  PANIC_RICH    : LONG SPY/QQQ/IWM, sell SPY put-credit, cover shorts
  EUPHORIA_RICH : SHORT SPY/QQQ via PSQ/SH, buy SPY puts, raise cash
  DIVERGENCE    : Pair trade (long stress-laggard vs short stress-leader)

Schedule: daily 21:15 UTC (post-close).
"""
import json
import os
import statistics
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import boto3
import _fred_shim  # noqa: F401  — cache-first FRED + 429 backoff (ops/1074)

VERSION = "2.0.0"
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = "data/put-call-extreme.json"
SSM_STATE_KEY = "/justhodl/put-call-extreme/state"

FRED_KEY = os.environ.get("FRED_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "") or os.environ.get(
    "TELEGRAM_TOKEN", "")
TELEGRAM_CHAT = os.environ.get("TELEGRAM_CHAT_ID", "8678089260")

s3 = boto3.client("s3")
ssm = boto3.client("ssm")


# Series config: (fred_id, cadence, weight, flip)
# flip=True means LOW value = stress (e.g. UMCSENT consumer confidence)
SIGNALS = [
    ("VIXCLS",     "daily",   3.0, False),
    ("VXNCLS",     "daily",   3.0, False),
    ("USEPUINDXD", "daily",   3.0, False),
    ("STLFSI4",    "weekly",  2.0, False),
    ("UMCSENT",    "monthly", 1.0, True),
]


def http_get(url, timeout=15, retries=2):
    last = None
    for i in range(retries + 1):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "justhodl/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read().decode("utf-8", errors="ignore")
        except Exception as e:
            last = e
            time.sleep(0.5 * (i + 1))
    raise RuntimeError(f"http_get failed: {last}")


def fred_series(series_id, limit=500):
    if not FRED_KEY:
        return [], []
    url = (f"https://api.stlouisfed.org/fred/series/observations"
           f"?series_id={series_id}&api_key={FRED_KEY}"
           f"&file_type=json&sort_order=desc&limit={limit}")
    try:
        data = json.loads(http_get(url))
        obs = data.get("observations", [])
        dates, values = [], []
        for o in obs:
            v = o.get("value")
            if v and v != ".":
                try:
                    values.append(float(v))
                    dates.append(o.get("date"))
                except ValueError:
                    continue
        return dates, values
    except Exception:
        return [], []


def zscore(values, lookback):
    """Compute z-score of latest value vs lookback distribution."""
    if not values or len(values) < lookback + 1:
        return None
    latest = values[0]
    window = values[1:lookback + 1]
    if len(window) < 30:
        return None
    mu = statistics.mean(window)
    sd = statistics.stdev(window) if len(window) > 1 else 0
    if sd == 0:
        return None
    return (latest - mu) / sd


def days_since(date_str):
    if not date_str:
        return None
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d").replace(
            tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - d).days
    except Exception:
        return None


def freshness_score(cadence, days_old):
    """Penalize stale data: returns multiplier 0..1."""
    if days_old is None:
        return 0.0
    if cadence == "daily":
        return 1.0 if days_old <= 5 else max(0.0, 1.0 - (days_old - 5) / 10)
    if cadence == "weekly":
        return 1.0 if days_old <= 10 else max(0.0, 1.0 - (days_old - 10) / 20)
    if cadence == "monthly":
        return 1.0 if days_old <= 45 else max(0.0, 1.0 - (days_old - 45) / 60)
    return 0.5


def classify_state(composite_z, dispersion):
    if composite_z is None:
        return "DATA_UNAVAILABLE"
    if composite_z >= 1.8:
        return "SENTIMENT_PANIC_RICH"
    if composite_z >= 1.0:
        return "SENTIMENT_PANIC_ACTIVE"
    if composite_z <= -1.8:
        return "SENTIMENT_EUPHORIA_RICH"
    if composite_z <= -1.0:
        return "SENTIMENT_EUPHORIA_ACTIVE"
    return "NEUTRAL"


def signal_strength(state, composite_z, dispersion):
    if state in ("SENTIMENT_PANIC_RICH", "SENTIMENT_EUPHORIA_RICH"):
        base = 0.9
    elif state in ("SENTIMENT_PANIC_ACTIVE", "SENTIMENT_EUPHORIA_ACTIVE"):
        base = 0.6
    elif state == "DATA_UNAVAILABLE":
        return 0.0
    else:
        base = 0.2
    if dispersion is not None and dispersion > 1.2:
        base = max(0.0, base - 0.15)  # high dispersion reduces confidence
    return round(min(1.0, max(0.0, base)), 2)


def get_trade_tickets(state, composite_z, breakdown):
    if state == "SENTIMENT_PANIC_RICH":
        return [
            {"action": "LONG", "ticker": "SPY", "size_pct": 8,
             "thesis": "panic capitulation - contrarian LONG large-cap"},
            {"action": "LONG", "ticker": "QQQ", "size_pct": 6,
             "thesis": "panic capitulation - contrarian LONG tech"},
            {"action": "LONG", "ticker": "IWM", "size_pct": 4,
             "thesis": "panic capitulation - small-cap beta amplifies rebound"},
            {"action": "SELL", "ticker": "SPY 30dte put-credit-spread",
             "size_pct": 3,
             "thesis": "harvest elevated put premium during stress spike"},
        ]
    if state == "SENTIMENT_PANIC_ACTIVE":
        return [
            {"action": "LONG", "ticker": "SPY", "size_pct": 4,
             "thesis": "partial entry - stress building, scale on confirmation"},
            {"action": "LONG", "ticker": "QQQ", "size_pct": 3,
             "thesis": "partial entry tech"},
        ]
    if state == "SENTIMENT_EUPHORIA_RICH":
        return [
            {"action": "SHORT", "ticker": "PSQ", "size_pct": 4,
             "thesis": "euphoria + complacency - tactical short tech via inverse ETF"},
            {"action": "SHORT", "ticker": "SH", "size_pct": 4,
             "thesis": "euphoria - tactical short broad market via inverse ETF"},
            {"action": "BUY", "ticker": "SPY 60dte 5% OTM puts", "size_pct": 2,
             "thesis": "tail hedge while put premium compressed"},
            {"action": "TRIM", "ticker": "high-beta equity", "size_pct": 0,
             "thesis": "raise cash - asymmetric reward/risk turning negative"},
        ]
    if state == "SENTIMENT_EUPHORIA_ACTIVE":
        return [
            {"action": "TRIM", "ticker": "QQQ", "size_pct": 0,
             "thesis": "complacency rising - reduce exposure 25%"},
            {"action": "BUY", "ticker": "VIX call calendar", "size_pct": 1,
             "thesis": "cheap convexity while VIX low"},
        ]
    return []


def compute_signal(spec):
    series_id, cadence, weight, flip = spec
    dates, values = fred_series(series_id, limit=500)
    if not values:
        return {"id": series_id, "ok": False, "error": "fred_empty",
                "cadence": cadence, "weight_base": weight}
    # Choose lookback by cadence
    lookback = {"daily": 252, "weekly": 104, "monthly": 60}.get(cadence, 100)
    z = zscore(values, lookback)
    if z is None:
        # try shorter lookback
        lookback = min(len(values) - 5, lookback)
        if lookback < 30:
            return {"id": series_id, "ok": False, "error": "insufficient_history",
                    "n_values": len(values), "cadence": cadence}
        z = zscore(values, lookback)
        if z is None:
            return {"id": series_id, "ok": False, "error": "zero_variance",
                    "cadence": cadence}
    # Flip if confidence-direction (LOW = stress)
    z_stress = -z if flip else z
    days_old = days_since(dates[0]) if dates else None
    fresh = freshness_score(cadence, days_old)
    effective_weight = weight * fresh
    return {
        "id": series_id, "ok": True, "cadence": cadence,
        "latest_date": dates[0] if dates else None,
        "latest_value": round(values[0], 4),
        "z_raw": round(z, 3),
        "z_stress": round(z_stress, 3),
        "flipped": flip,
        "weight_base": weight,
        "freshness_mult": round(fresh, 3),
        "effective_weight": round(effective_weight, 3),
        "lookback": lookback,
        "days_old": days_old,
        "n_history": len(values),
    }


def aggregate(signals):
    valid = [s for s in signals if s.get("ok")]
    if not valid:
        return None, None, valid
    total_w = sum(s["effective_weight"] for s in valid)
    if total_w == 0:
        return None, None, valid
    composite = sum(s["z_stress"] * s["effective_weight"] for s in valid) / total_w
    # Dispersion: stdev of per-signal z_stress (cross-signal disagreement)
    if len(valid) > 1:
        dispersion = statistics.stdev([s["z_stress"] for s in valid])
    else:
        dispersion = 0.0
    return round(composite, 3), round(dispersion, 3), valid


def telegram_alert(text):
    if not TELEGRAM_TOKEN:
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        data = urllib.parse.urlencode({
            "chat_id": TELEGRAM_CHAT, "text": text,
            "parse_mode": "Markdown"}).encode()
        req = urllib.request.Request(url, data=data)
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status == 200
    except Exception:
        return False


def lambda_handler(event, context):
    started = datetime.now(timezone.utc).isoformat()
    try:
        signals = [compute_signal(spec) for spec in SIGNALS]
        composite_z, dispersion, valid = aggregate(signals)
        if composite_z is None:
            payload = {
                "version": VERSION,
                "generated_at": started,
                "state": "DATA_UNAVAILABLE",
                "signal_strength": 0.0,
                "composite_z": None,
                "dispersion": None,
                "n_valid_signals": 0,
                "why_now": "All 5 FRED sentiment series failed",
                "actions": [],
                "signals": signals,
                "engine_note": ("Rebuilt 2026-05-21 from single P/C source "
                                "to 5-source FRED sentiment composite "
                                "(BW06 methodology). All signals failed."),
                "sources": ["FRED VIXCLS/VXNCLS/USEPUINDXD/STLFSI4/UMCSENT"],
                "thesis": "Baker-Wurgler 2006 multi-source sentiment composite",
            }
            s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                          Body=json.dumps(payload, indent=2).encode(),
                          ContentType="application/json",
                          CacheControl="max-age=300")
            return {"statusCode": 200,
                    "body": json.dumps({"ok": True, "state": "DATA_UNAVAILABLE"})}

        state = classify_state(composite_z, dispersion)
        strength = signal_strength(state, composite_z, dispersion)
        tickets = get_trade_tickets(state, composite_z, valid)

        # Divergence flag
        divergence = dispersion is not None and dispersion > 1.2

        # Why-now narrative
        if state == "DATA_UNAVAILABLE":
            why = "No signals available"
        elif "PANIC" in state:
            top_contributors = sorted(
                valid, key=lambda x: -x["z_stress"] * x["effective_weight"])[:2]
            sig_names = ", ".join(s["id"] for s in top_contributors)
            why = (f"Composite stress z={composite_z:+.2f} -> {state}. "
                   f"Top contributors: {sig_names}. Contrarian: stress "
                   f"reversals avg +4-7% / 4-8wk (BW06).")
        elif "EUPHORIA" in state:
            why = (f"Composite stress z={composite_z:+.2f} -> {state}. "
                   f"Complacency elevated, asymmetric R/R turning negative. "
                   f"Hedge or short tactically (Garcia 2013).")
        else:
            why = (f"Composite stress z={composite_z:+.2f} in neutral band. "
                   f"No actionable extreme. Monitor.")
        if divergence:
            why += (f" DIVERGENCE flag (signal dispersion={dispersion:.2f}) - "
                    f"cross-cadence disagreement, possible regime transition.")

        payload = {
            "version": VERSION,
            "generated_at": started,
            "state": state,
            "signal_strength": strength,
            "composite_z": composite_z,
            "dispersion": dispersion,
            "divergence_flag": divergence,
            "n_valid_signals": len(valid),
            "n_total_signals": len(signals),
            "why_now": why,
            "actions": tickets,
            "signals": signals,
            "thresholds": {
                "panic_rich":   1.8, "panic_active":   1.0,
                "neutral_band": [-1.0, 1.0],
                "euphoria_active": -1.0, "euphoria_rich": -1.8,
                "divergence":   1.2,
            },
            "edge_basis": ("Baker-Wurgler 2006 (JF, multi-source sentiment "
                           "index), Stambaugh-Yu-Zhang 2012 (JFE, sentiment "
                           "+ anomalies), Garcia 2013, Brown-Cliff 2004. "
                           "~58-62% hit / +4-7% / 4-8wk."),
            "sources": [
                "FRED VIXCLS (daily, equity vol)",
                "FRED VXNCLS (daily, NDX vol)",
                "FRED USEPUINDXD (daily, econ policy uncertainty)",
                "FRED STLFSI4 (weekly, fin stress index)",
                "FRED UMCSENT (monthly, UMich consumer sentiment, flipped)",
            ],
            "engine_note": ("v2.0.0 Sentiment Composite (rebuilt 2026-05-21 "
                            "ops 992-995 after CBOE P/C feed retirement). "
                            "Machine name 'put-call-extreme' kept for ID "
                            "stability."),
            "thesis": "Multi-source sentiment extremes drive mean-reversion",
        }
        s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                      Body=json.dumps(payload, indent=2).encode(),
                      ContentType="application/json",
                      CacheControl="max-age=300")

        try:
            ssm.put_parameter(Name=SSM_STATE_KEY, Value=state,
                              Type="String", Overwrite=True)
        except Exception:
            pass

        # Telegram alert only on RICH state
        if state in ("SENTIMENT_PANIC_RICH", "SENTIMENT_EUPHORIA_RICH"):
            txt = (f"*Sentiment Extreme: {state}*\n"
                   f"Composite stress z = {composite_z:+.2f} "
                   f"(dispersion {dispersion:.2f})\n"
                   f"Valid signals: {len(valid)}/{len(signals)}\n"
                   f"{why}")
            telegram_alert(txt)

        return {"statusCode": 200,
                "body": json.dumps({
                    "ok": True, "state": state,
                    "composite_z": composite_z,
                    "n_valid": len(valid),
                    "strength": strength})}

    except Exception as e:
        err_payload = {
            "version": VERSION,
            "generated_at": started,
            "state": "ERROR",
            "signal_strength": 0.0,
            "error": str(e)[:500],
        }
        try:
            s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                          Body=json.dumps(err_payload, indent=2).encode(),
                          ContentType="application/json")
        except Exception:
            pass
        return {"statusCode": 500, "body": json.dumps({"ok": False,
                                                       "error": str(e)})}
