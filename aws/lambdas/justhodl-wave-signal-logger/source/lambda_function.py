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
        "source": "wave-signal-logger-v3",
    }
    SIGNALS_TBL.put_item(Item=item)
    bp = f"${price:.2f}" if price else "no-price"
    print(f"[LOG] {stype}={val} {pred} conf={conf:.2f} against={against} {bp}")
    return sid


# ─────────────────────────────────────────────────────────────────
# Per-source translators


def log_earnings_pead():
    """Schema: pead_signals[].pead_label / pead_score (NOT 'signal' / 'drift_score')."""
    d = fs3("data/earnings-tracker.json")
    out = []
    for s in d.get("pead_signals", []) or []:
        ticker = s.get("ticker")
        label = s.get("pead_label") or s.get("signal")  # tolerate either
        score = s.get("pead_score") or s.get("drift_score") or 0
        if not ticker or not label:
            continue
        try:
            score = float(score)
        except Exception:
            score = 0
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
    """Schema: by_category is dict-of-summaries (NOT lists of ETFs).
    Per-ETF data is in heavy_inflow / heavy_outflow / rotation_in / rotation_out arrays."""
    d = fs3("data/etf-flows.json")
    out = []
    # Heavy: high-confidence extreme inflow/outflow
    for e in (d.get("heavy_inflow") or [])[:5]:
        if not isinstance(e, dict):
            continue
        ticker = e.get("ticker")
        z = e.get("dollar_volume_z_60d") or e.get("dvol_z_60d")
        if not ticker:
            continue
        try:
            z = float(z) if z is not None else None
        except Exception:
            z = None
        sid = log_sig("etf_flow_extreme",
                      f"INFLOW_z{z:.1f}" if z else "INFLOW",
                      "UP",
                      conf=min(0.7, abs(z) / 4) if z else 0.5,
                      against=ticker, windows=WINDOWS_DEFAULT,
                      rationale=f"{e.get('category','')} heavy inflow z={z}")
        out.append((ticker, "UP", sid))
    for e in (d.get("heavy_outflow") or [])[:5]:
        if not isinstance(e, dict):
            continue
        ticker = e.get("ticker")
        z = e.get("dollar_volume_z_60d") or e.get("dvol_z_60d")
        if not ticker:
            continue
        try:
            z = float(z) if z is not None else None
        except Exception:
            z = None
        sid = log_sig("etf_flow_extreme",
                      f"OUTFLOW_z{z:.1f}" if z else "OUTFLOW",
                      "DOWN",
                      conf=min(0.7, abs(z) / 4) if z else 0.5,
                      against=ticker, windows=WINDOWS_DEFAULT,
                      rationale=f"{e.get('category','')} heavy outflow z={z}")
        out.append((ticker, "DOWN", sid))
    # Rotation: lower confidence (price+flow combination)
    for e in (d.get("rotation_in") or [])[:3]:
        if not isinstance(e, dict):
            continue
        ticker = e.get("ticker")
        if not ticker:
            continue
        sid = log_sig("etf_rotation", "ROTATION_IN", "UP",
                      conf=0.45,
                      against=ticker, windows=WINDOWS_DEFAULT,
                      rationale=f"{e.get('category','')} rotation in (5d:{e.get('return_5d_pct')}%)")
        out.append((ticker, "UP", sid))
    for e in (d.get("rotation_out") or [])[:3]:
        if not isinstance(e, dict):
            continue
        ticker = e.get("ticker")
        if not ticker:
            continue
        sid = log_sig("etf_rotation", "ROTATION_OUT", "DOWN",
                      conf=0.45,
                      against=ticker, windows=WINDOWS_DEFAULT,
                      rationale=f"{e.get('category','')} rotation out (20d:{e.get('return_20d_pct')}%)")
        out.append((ticker, "DOWN", sid))
    return out


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
    # Regime-based logging — historically:
    #   BEAR_STEEPENER (long rates rising) → bad for SPY+TLT (cost of capital up)
    #   BULL_STEEPENER (Fed cutting → short rates fall) → good for SPY (rate-cut tailwind)
    #   BEAR_FLATTENER (Fed hiking) → mixed/bad for risk
    #   BULL_FLATTENER (long rates falling, recession-pricing) → bad for SPY in time
    #   INVERTED → recession leading indicator → DOWN for SPY
    if "INVERT" in regime or s2y10y < -10:
        sid = log_sig("yc_regime", regime or f"INV_{s2y10y:.0f}", "DOWN",
                      conf=0.45, against="SPY", windows=[21, 60],
                      rationale=f"2s10s={s2y10y:.0f} bps, regime={regime}")
        out.append(("SPY", "DOWN", sid))
    elif regime == "BEAR_STEEPENER":
        sid = log_sig("yc_regime", regime, "DOWN",
                      conf=0.45, against="TLT", windows=[7, 21],
                      rationale=f"2s10s={s2y10y:.0f} bps bear-steepening (long rates rising)")
        out.append(("TLT", "DOWN", sid))
    elif regime == "BULL_STEEPENER":
        sid = log_sig("yc_regime", regime, "UP",
                      conf=0.5, against="SPY", windows=[21, 60],
                      rationale=f"2s10s={s2y10y:.0f} bps bull-steepening (Fed-cut tailwind)")
        out.append(("SPY", "UP", sid))
    elif regime == "BULL_FLATTENER":
        sid = log_sig("yc_regime", regime, "DOWN",
                      conf=0.4, against="SPY", windows=[21, 60],
                      rationale=f"2s10s={s2y10y:.0f} bps bull-flattening (long rates falling — slowdown signal)")
        out.append(("SPY", "DOWN", sid))
    return out


def log_analogs():
    d = fs3("data/historical-analogs.json")
    call = d.get("directional_call")
    fwd = d.get("forward_distribution") or {}
    fwd21 = fwd.get("21d") if isinstance(fwd, dict) else None
    if not call or call.upper() in ("NEUTRAL", "MIXED", "UNCLEAR"):
        return []
    pred = "UP" if "BULL" in call.upper() else "DOWN" if "BEAR" in call.upper() else None
    if pred is None:
        return []
    # Hit rate as confidence proxy. Schema: {"21d": {"hit_rate_pct": 100, "mean_pct": 2.54, ...}}
    hit = 0.55
    mean_ret = None
    if isinstance(fwd21, dict):
        hr = fwd21.get("hit_rate_pct")
        if hr is not None:
            try:
                hit = float(hr) / 100
            except Exception:
                pass
        mean_ret = fwd21.get("mean_pct")
    sid = log_sig("analog_signal", call, pred,
                  conf=min(0.85, max(0.4, hit)),
                  against="SPY", windows=[21],
                  rationale=f"Top analog {call}, mean_21d={mean_ret}%, hit_rate={hit:.0%}")
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
    """Schema: top_50_composite[].symbol (NOT 'ticker') / composite_score."""
    d = fs3("data/momentum-scanner.json")
    top = d.get("top_50_composite") or d.get("top_composite") or d.get("ranked", [])
    if not isinstance(top, list):
        return []
    out = []
    for s in top[:3]:
        ticker = s.get("symbol") or s.get("ticker")  # tolerate either
        score = s.get("composite_score") or s.get("composite") or s.get("score") or 0
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
                      rationale=f"Universe momentum composite={score:.1f}/100, sector={s.get('sector', '?')}")
        out.append((ticker, "UP", sid))
    return out


def log_correlation_breaks():
    """Log top regime correlation breaks (|delta_30d_vs_90d| >= 0.30) — per-pair, max 5.
    Each break is logged as a directional signal on the dependent ticker:
      - corr collapsing (delta >>0 toward decoupling) → predict mean-reversion in ticker_a
      - delta sign tells direction of recent change, not future — we score whether
        ticker_a continues to outperform/underperform ticker_b after the break.
    """
    d = fs3("data/correlation-surface.json") or {}
    out = []
    breaks = (d.get("regime_breaks") or [])[:5]
    for b in breaks:
        ta, tb = b.get("ticker_a"), b.get("ticker_b")
        delta = b.get("delta_30d_vs_90d")
        if not ta or not tb or delta is None:
            continue
        if abs(delta) < 0.30:
            continue
        # Direction: positive delta = correlation rose vs 90d (more coupled now)
        # negative delta = correlation fell (decoupled). Both are mean-reversion candidates.
        # We predict ticker_a will revert toward its 90d relationship with ticker_b.
        # For scoring: if 30d corr was positive and now negative, expect ticker_a to
        # underperform ticker_b over next 21d (mean-reversion of relative strength).
        c30, c90 = b.get("corr_30d"), b.get("corr_90d")
        if c30 is None or c90 is None:
            continue
        # Net mean-reversion direction depends on which way the relationship moved
        if c30 > c90:  # got more coupled — expect decoupling reversion
            pred = "DOWN"  # ticker_a relative to spy generally underperforms
        else:  # got less coupled — expect re-coupling, depends on ticker_a recent move
            pred = "UP"
        sid = log_sig(
            stype="correlation_break",
            val=f"{ta}_{tb}_d{delta:+.2f}",
            pred=pred,
            conf=min(0.85, abs(delta)),
            against=ta,
            windows=WINDOWS_DEFAULT,
            magnitude=None,
            rationale=f"30d corr {c30:+.2f} vs 90d {c90:+.2f}, Δ={delta:+.2f}",
        )
        out.append(sid)
    return out


def log_divergence_extremes():
    """Log divergences from divergence/current.json (schema: relationships[]).
    Field names: z_score, asset_a (e.g. 'stocks:QQQ'), asset_b ('fred:DGS10' or 'stocks:SPY'),
    extreme (bool), alert_worthy (bool), mispricing (str).
    Threshold lowered to |z|>=2.0 since current data has no z>=2.5.
    """
    d = fs3("divergence/current.json") or {}
    out = []
    relationships = d.get("relationships") or []
    for rel in relationships:
        # z_score arrives as a string ("2.16") in real data — must cast.
        z_raw = rel.get("z_score")
        try:
            z = float(z_raw)
        except (TypeError, ValueError):
            continue
        # Trigger if extreme flag is set OR |z| above 2.0 threshold.
        ext_flag = str(rel.get("extreme", "")).lower() in ("true", "high", "low")
        if not ext_flag and abs(z) < 2.0:
            continue
        asset_a_raw = rel.get("asset_a", "") or ""
        asset_b_raw = rel.get("asset_b", "") or ""
        # Only fire if asset_a is a tradable equity/ETF we can price-track for scoring.
        if not asset_a_raw.startswith("stocks:"):
            continue
        ticker_a = asset_a_raw.split(":", 1)[1]
        ticker_b = asset_b_raw.split(":", 1)[1] if ":" in asset_b_raw else asset_b_raw
        # Mispricing semantics: "QQQ appears RICH vs DGS10" → predict QQQ DOWN (mean revert).
        mispricing = (rel.get("mispricing") or "").upper()
        if "RICH" in mispricing:
            pred = "DOWN"
        elif "CHEAP" in mispricing:
            pred = "UP"
        else:
            pred = "DOWN" if z > 0 else "UP"
        sid = log_sig(
            stype="divergence_extreme",
            val=f"{ticker_a}_vs_{ticker_b}_z{z:+.2f}",
            pred=pred,
            conf=min(0.85, abs(z) / 4.0),
            against=ticker_a,
            windows=WINDOWS_DEFAULT,
            magnitude=None,
            rationale=f"{rel.get('name', '?')}: z={z:+.2f}, {mispricing.lower()}, intact={rel.get('relationship_intact')}",
        )
        out.append(sid)
    return out


def log_cot_extremes():
    """Log COT speculator-net positioning extremes (≤5 or ≥95 percentile rank).
    Real path: cot/extremes/current.json. Contract symbols are CME codes (GC, CL, ES, etc).
    """
    d = fs3("cot/extremes/current.json") or {}
    out = []
    extremes = d.get("extremes") or d.get("contracts") or []
    # Map CME futures symbols to tradable ETFs for price tracking
    proxy = {
        "GC": "GLD", "SI": "SLV", "CL": "USO", "BZ": "USO",  # Brent
        "NG": "UNG", "HG": "JJC", "PL": "PPLT",
        "ES": "SPY", "NQ": "QQQ", "RTY": "IWM", "YM": "DIA",
        "ZN": "TLT", "ZB": "TLT", "ZT": "SHY",
        "6E": "FXE", "6J": "FXY", "6B": "FXB", "6C": "FXC", "6S": "FXF",
        "DX": "UUP",
        "BTC": "BITO",
        "KC": None, "CT": None, "SB": None, "RB": None, "HO": None,  # no clean ETF proxies
    }
    for e in extremes[:10]:
        # 'percentile' arrives as a string ("98.5") in real data — must cast.
        pct_raw = e.get("percentile_rank") or e.get("pct_rank") or e.get("percentile")
        try:
            pct = float(pct_raw)
        except (TypeError, ValueError):
            continue
        contract = (e.get("contract") or e.get("name") or e.get("symbol") or "").upper()
        # Use 'extreme' flag as the primary trigger ('high'/'low'), falling back to numeric thresholds.
        ext_flag = str(e.get("extreme", "")).lower()
        if ext_flag == "high" or pct >= 95:
            pred = "DOWN"; label = "EXTREME_LONG"
        elif ext_flag == "low" or pct <= 5:
            pred = "UP"; label = "EXTREME_SHORT"
        else:
            continue
        proxy_ticker = proxy.get(contract)
        if not proxy_ticker:
            continue
        sid = log_sig(
            stype="cot_extreme",
            val=f"{contract}_{label}_p{pct:.0f}",
            pred=pred,
            conf=min(0.85, abs(pct - 50) / 50.0),
            against=proxy_ticker,
            windows=WINDOWS_LONG,
            magnitude=None,
            rationale=f"Spec net at {pct:.0f}th pct (5y) — contrarian {label}",
        )
        out.append(sid)
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
        ("correlation_break", log_correlation_breaks),
        ("divergence_extreme", log_divergence_extremes),
        ("cot_extreme", log_cot_extremes),
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
