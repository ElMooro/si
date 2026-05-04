"""
justhodl-wave-signal-logger — Reads Wave 1+2 outputs from S3 and logs distinct
signal_types to justhodl-signals DDB so Loop 1 can score them.

This is INDEPENDENT from the legacy justhodl-signal-logger (which handles
26 morning-intel signals). They write to the same DDB table with disjoint
signal_type names, no collisions.

Sources (each produces 1+ signals):
  1. data/earnings-tracker.json     → earnings_pead (per-ticker)
  2. data/short-interest.json       → squeeze_risk (per-ticker)
  3. data/etf-flows.json            → etf_flow_extreme (per-ticker)
  4. data/macro-surprise.json       → macro_composite_z
  5. data/yield-curve.json          → yc_regime
  6. data/historical-analogs.json   → analog_signal
  7. data/event-study.json          → event_signal
  8. data/auction-crisis.json       → auction_crisis
  9. data/sector-rotation.json      → sector_breadth
 10. data/momentum-scanner.json     → momentum_top_pick (top 3)
 11. data/correlation-surface.json  → SKIP (existing logger handles)
 12. data/eurodollar-stress.json    → SKIP (file not produced)

Each log entry includes baseline_price (so outcome-checker can score it)
and check_windows for [day_3, day_14, day_21] outcomes.

Schedule: rate(6 hours), staggered offset to avoid simultaneous DDB writes
with the legacy logger.
"""

import json
import os
import time
import uuid
import urllib.request
from decimal import Decimal
from datetime import datetime, timezone, timedelta
import boto3

S3 = boto3.client("s3", region_name="us-east-1")
DDB = boto3.resource("dynamodb", region_name="us-east-1")
SIGNALS_TBL = DDB.Table("justhodl-signals")

BUCKET = "justhodl-dashboard-live"
POLYGON_KEY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")

WINDOWS_DEFAULT = [3, 14, 21]
WINDOWS_LONG = [7, 21, 60]

_PRICE_CACHE = {}


def _polygon_prev(ticker):
    """Fetch previous day close from Polygon."""
    try:
        url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/prev?apiKey={POLYGON_KEY}"
        req = urllib.request.Request(url, headers={"User-Agent": "wave-logger/1.0"})
        with urllib.request.urlopen(req, timeout=8) as r:
            d = json.loads(r.read().decode())
        results = d.get("results") or []
        if results:
            return float(results[0].get("c"))
    except Exception as e:
        print(f"[poly] {ticker}: {e}")
    return None


def _fmp_quote(ticker):
    """FMP quote fallback."""
    try:
        url = f"https://financialmodelingprep.com/stable/quote?symbol={ticker}&apikey={FMP_KEY}"
        with urllib.request.urlopen(url, timeout=8) as r:
            d = json.loads(r.read().decode())
        if isinstance(d, list) and d:
            return float(d[0].get("price"))
    except Exception as e:
        print(f"[fmp] {ticker}: {e}")
    return None


def get_price(ticker):
    if not ticker:
        return None
    if ticker in _PRICE_CACHE:
        return _PRICE_CACHE[ticker]
    p = _polygon_prev(ticker) or _fmp_quote(ticker)
    _PRICE_CACHE[ticker] = p
    return p


def f2d(obj):
    if isinstance(obj, float):
        return Decimal(str(round(obj, 6)))
    if isinstance(obj, dict):
        return {k: f2d(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [f2d(v) for v in obj]
    return obj


def fs3(key):
    try:
        obj = S3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except Exception as e:
        print(f"[s3] {key}: {e}")
        return {}


def log_sig(stype, val, pred, conf, against, windows=None, magnitude=None, rationale=None):
    """Write one signal record matching existing logger schema."""
    if windows is None:
        windows = WINDOWS_DEFAULT
    now = datetime.now(timezone.utc)
    sid = str(uuid.uuid4())
    ts = {f"day_{d}": (now + timedelta(days=d)).isoformat() for d in windows}
    price = get_price(against) if against else None
    target = None
    if magnitude is not None and price:
        target = float(price) * (1.0 + float(magnitude) / 100.0)

    item = {
        "signal_id": sid,
        "signal_type": stype,
        "signal_value": str(val),
        "predicted_direction": pred,
        "confidence": f2d(float(conf)),
        "measure_against": against,
        "baseline_price": f2d(float(price)) if price else None,
        "check_windows": [str(d) for d in windows],
        "check_timestamps": ts,
        "outcomes": {},
        "accuracy_scores": {},
        "logged_at": now.isoformat(),
        "logged_epoch": int(now.timestamp()),
        "status": "pending",
        "metadata": {},
        "ttl": int((now + timedelta(days=365)).timestamp()),
        "schema_version": "2",
        "predicted_magnitude_pct": f2d(float(magnitude)) if magnitude is not None else None,
        "predicted_target_price": f2d(float(target)) if target else None,
        "horizon_days_primary": int(max(windows)),
        "rationale": str(rationale) if rationale else None,
        "source": "wave-signal-logger-v1",
    }
    SIGNALS_TBL.put_item(Item=item)
    bp = f"${price:.2f}" if price else "no-price"
    print(f"[LOG] {stype}={val} {pred} conf={conf:.2f} against={against} {bp}")
    return sid


# ─────────────────────────────────────────────────────────────────
# Per-source translators


def log_earnings_pead():
    d = fs3("data/earnings-tracker.json")
    out = []
    for s in d.get("pead_signals", []) or []:
        ticker = s.get("ticker")
        label = s.get("signal")
        score = s.get("drift_score") or 0
        if not ticker or not label:
            continue
        if label == "STRONG_POSITIVE_DRIFT":
            sid = log_sig("earnings_pead", label, "UP",
                          conf=min(0.85, max(0.3, abs(score) / 100)),
                          against=ticker, windows=WINDOWS_DEFAULT,
                          rationale=f"PEAD positive drift score {score}")
            out.append((ticker, "UP", sid))
        elif label == "STRONG_NEGATIVE_DRIFT":
            sid = log_sig("earnings_pead", label, "DOWN",
                          conf=min(0.85, max(0.3, abs(score) / 100)),
                          against=ticker, windows=WINDOWS_DEFAULT,
                          rationale=f"PEAD negative drift score {score}")
            out.append((ticker, "DOWN", sid))
    return out


def log_squeeze_risk():
    d = fs3("data/short-interest.json")
    out = []
    for s in d.get("top_squeeze_risk", []) or []:
        ticker = s.get("ticker")
        dtc = s.get("days_to_cover") or s.get("polygon_days_to_cover")
        if not ticker or dtc is None:
            continue
        try:
            dtc = float(dtc)
        except Exception:
            continue
        if dtc < 8:
            continue
        sid = log_sig("squeeze_risk", f"DTC_{dtc:.1f}", "UP",
                      conf=min(0.7, dtc / 15),
                      against=ticker, windows=WINDOWS_DEFAULT,
                      rationale=f"Days-to-cover {dtc:.1f}")
        out.append((ticker, "UP", sid))
    return out[:5]  # cap at 5


def log_etf_flows():
    d = fs3("data/etf-flows.json")
    out = []
    for cat, etfs in (d.get("by_category") or {}).items():
        for e in etfs:
            ticker = e.get("ticker")
            sig = e.get("signal")
            z = e.get("dollar_volume_z_60d")
            if not ticker or not sig or z is None:
                continue
            try:
                z = float(z)
            except Exception:
                continue
            if abs(z) < 2.5:
                continue
            if sig == "HEAVY_INFLOW":
                sid = log_sig("etf_flow_extreme", f"INFLOW_z{z:.1f}", "UP",
                              conf=min(0.7, abs(z) / 4),
                              against=ticker, windows=WINDOWS_DEFAULT,
                              rationale=f"{cat} heavy inflow z={z:.2f}")
                out.append((ticker, "UP", sid))
            elif sig == "HEAVY_OUTFLOW":
                sid = log_sig("etf_flow_extreme", f"OUTFLOW_z{z:.1f}", "DOWN",
                              conf=min(0.7, abs(z) / 4),
                              against=ticker, windows=WINDOWS_DEFAULT,
                              rationale=f"{cat} heavy outflow z={z:.2f}")
                out.append((ticker, "DOWN", sid))
    return out[:6]


def log_macro_surprise():
    d = fs3("data/macro-surprise.json")
    z = d.get("composite_z")
    regime = d.get("regime", "UNKNOWN")
    if z is None:
        return []
    try:
        z = float(z)
    except Exception:
        return []
    if abs(z) < 1.0:
        return []
    pred = "UP" if z > 0 else "DOWN"
    sid = log_sig("macro_composite_z", f"z_{z:.2f}_{regime}", pred,
                  conf=min(0.7, abs(z) / 3),
                  against="SPY", windows=WINDOWS_LONG,
                  rationale=f"Macro surprise composite z={z:.2f}, regime={regime}")
    return [("SPY", pred, sid)]


def log_yield_curve():
    d = fs3("data/yield-curve.json")
    spreads = d.get("spreads_bps", {}) or {}
    s2y10y = spreads.get("2s10s") or spreads.get("2Y10Y") or spreads.get("DGS10_DGS2")
    regime = d.get("regime", "UNKNOWN")
    if s2y10y is None:
        return []
    try:
        s2y10y = float(s2y10y)
    except Exception:
        return []
    out = []
    if s2y10y < -10:  # inverted
        sid = log_sig("yc_regime", f"INVERTED_{s2y10y:.0f}", "DOWN",
                      conf=0.45, against="SPY", windows=[21, 60],
                      rationale=f"2s10s={s2y10y:.0f} bps inverted, regime={regime}")
        out.append(("SPY", "DOWN", sid))
    elif s2y10y > 100:  # very steep
        sid = log_sig("yc_regime", f"STEEP_{s2y10y:.0f}", "DOWN",
                      conf=0.4, against="SPY", windows=[21, 60],
                      rationale=f"2s10s={s2y10y:.0f} bps steep, regime={regime}")
        out.append(("SPY", "DOWN", sid))
    return out


def log_analogs():
    d = fs3("data/historical-analogs.json")
    call = d.get("directional_call")
    fwd = d.get("forward_distribution") or {}
    if not call or call.upper() in ("NEUTRAL", "MIXED", "UNCLEAR"):
        return []
    pred = "UP" if "BULL" in call.upper() else "DOWN" if "BEAR" in call.upper() else None
    if pred is None:
        return []
    # Use 21d hit rate as confidence proxy
    hit = fwd.get("hit_rate_21d") or fwd.get("hit_rate") or 0.55
    if isinstance(hit, str):
        try:
            hit = float(hit.replace("%", "")) / 100
        except Exception:
            hit = 0.55
    mean_ret = fwd.get("mean_return_21d") or fwd.get("mean_return")
    sid = log_sig("analog_signal", call, pred,
                  conf=min(0.85, max(0.4, hit)),
                  against="SPY", windows=[21],
                  rationale=f"Top analog {call}, mean_21d={mean_ret}")
    return [("SPY", pred, sid)]


def log_event_study():
    d = fs3("data/event-study.json")
    expected = d.get("expected_21d_return_from_active_pct")
    themes = d.get("active_themes", [])
    if expected is None:
        return []
    try:
        expected = float(expected)
    except Exception:
        return []
    if abs(expected) < 0.5:
        return []
    pred = "UP" if expected > 0 else "DOWN"
    label = ",".join(themes[:3]) or "events_active"
    sid = log_sig("event_signal", label, pred,
                  conf=min(0.7, abs(expected) / 5),
                  against="SPY", windows=[21],
                  rationale=f"Expected 21d return {expected:+.2f}% from {len(themes)} active themes")
    return [("SPY", pred, sid)]


def log_auction_crisis():
    d = fs3("data/auction-crisis.json")
    score = d.get("composite_score")
    regime = d.get("regime", "UNKNOWN")
    if score is None:
        return []
    try:
        score = float(score)
    except Exception:
        return []
    out = []
    if score >= 60:
        # Auction stress → bearish for risk, UP for safe haven
        sid_spy = log_sig("auction_crisis", f"STRESS_{score:.0f}", "DOWN",
                          conf=min(0.7, score / 100),
                          against="SPY", windows=[7, 21],
                          rationale=f"Auction crisis score={score:.0f}, regime={regime}")
        out.append(("SPY", "DOWN", sid_spy))
        sid_tlt = log_sig("auction_crisis_tlt", f"STRESS_{score:.0f}", "DOWN",
                          conf=min(0.7, score / 100),
                          against="TLT", windows=[7, 21],
                          rationale=f"Bond stress (auctions) score={score:.0f}")
        out.append(("TLT", "DOWN", sid_tlt))
    return out


def log_sector_breadth():
    d = fs3("data/sector-rotation.json")
    breadth = d.get("market_breadth")
    if not breadth:
        return []
    out = []
    if breadth == "BROAD_LEADERSHIP":
        sid = log_sig("sector_breadth", breadth, "UP",
                      conf=0.55, against="SPY", windows=[7, 21],
                      rationale=d.get("market_breadth_description", ""))
        out.append(("SPY", "UP", sid))
    elif breadth == "NARROW_LEADERSHIP":
        # Narrow breadth historically precedes corrections
        sid = log_sig("sector_breadth", breadth, "DOWN",
                      conf=0.45, against="SPY", windows=[21, 60],
                      rationale=d.get("market_breadth_description", ""))
        out.append(("SPY", "DOWN", sid))
    return out


def log_momentum_top_picks():
    d = fs3("data/momentum-scanner.json")
    top = d.get("top_composite") or d.get("top_50") or d.get("ranked", [])
    if not isinstance(top, list):
        return []
    out = []
    for s in top[:3]:
        ticker = s.get("ticker")
        score = s.get("composite_score") or s.get("composite") or 0
        if not ticker:
            continue
        try:
            score = float(score)
        except Exception:
            continue
        if score < 90:
            continue
        sid = log_sig("momentum_top_pick", f"composite_{score:.1f}", "UP",
                      conf=min(0.75, score / 100),
                      against=ticker, windows=[7, 21],
                      rationale=f"Universe momentum composite={score:.1f}/100")
        out.append((ticker, "UP", sid))
    return out


def lambda_handler(event=None, context=None):
    started = time.time()
    print(f"[wave-logger] starting at {datetime.now(timezone.utc).isoformat()}")

    summary = {}
    handlers = [
        ("earnings_pead", log_earnings_pead),
        ("squeeze_risk", log_squeeze_risk),
        ("etf_flow_extreme", log_etf_flows),
        ("macro_composite_z", log_macro_surprise),
        ("yc_regime", log_yield_curve),
        ("analog_signal", log_analogs),
        ("event_signal", log_event_study),
        ("auction_crisis", log_auction_crisis),
        ("sector_breadth", log_sector_breadth),
        ("momentum_top_pick", log_momentum_top_picks),
    ]
    for name, fn in handlers:
        try:
            res = fn() or []
            summary[name] = len(res)
            print(f"[wave-logger] {name}: {len(res)} signals logged")
        except Exception as e:
            summary[name] = f"ERR:{e}"
            print(f"[wave-logger] {name} ERROR: {e}")

    total = sum(v for v in summary.values() if isinstance(v, int))
    duration = round(time.time() - started, 2)
    print(f"[wave-logger] DONE — {total} signals logged in {duration}s")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "total_signals_logged": total,
            "by_type": summary,
            "duration_s": duration,
        }),
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler({}, None), indent=2))
