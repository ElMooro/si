"""
Pro Pack v3 #5 - Bond Vol Composite (Synthetic MOVE / Bloomberg gap-closer)
==============================================================================

Bloomberg's MOVE Index (ICE BofA Move Index) is the bond-market VIX equivalent.
ICE owns the rights -> the raw MOVE feed is paywalled (FRED `ICE_BAML_MOVE`
returned 400 in our probe; CBOE-style free public CSV doesn't exist either).

Institutional fallback: build a **Synthetic Bond Vol Composite** that mirrors
MOVE's analytical intent using FRED-free daily series. Cross-asset bond vol
practitioners (PIMCO, Bridgewater, ARP funds) routinely compute these proxies
when MOVE is unavailable at session start.

Five vol channels (all annualized 30-day realized vol of daily changes):
1. 10Y Treasury Yield Vol (DGS10)            - duration risk
2. 2Y Treasury Yield Vol (DGS2)              - front-end / Fed-path risk
3. 2s10s Curve Vol (T10Y2Y)                  - curve reshape risk
4. HY Spread Vol (BAMLH0A0HYM2)              - credit risk
5. IG BBB Spread Vol (BAMLC0A4CBBB)          - investment-grade risk

For each:
- Annualized stdev of daily first-differences over last 30 trading days
- Z-score vs. 1-year (252-day) history of same metric
- Cap z at +/- 3.0 to suppress outliers

Composite Bond Vol Score = simple average of 5 z-scores (equal weight,
since each channel captures a distinct piece of bond-market stress).

Universe regime (4 bands):
- BOND_VOL_LOW       (composite z <= -1.0)  : compressed vol, complacency
- NORMAL             (-1.0 <  z <  +1.0)    : baseline
- ELEVATED           (+1.0 <= z <  +2.0)    : stress building
- CRISIS             (+2.0 <= z)            : tail event in progress

Output: per-channel realized vol + z + history percentile + composite + regime.

Schedule: hourly (FRED data updates daily but hourly refresh keeps page fresh).
"""

import os
import sys
import json
import math
import statistics
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta

import boto3
try:
    import _fred_shim  # noqa: F401  — cache-first FRED + 429 backoff (ops/1074)
except Exception:
    pass  # shim optional; fred_obs falls back to the worker /fred route

VERSION = "1.0.1"
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = "data/bond-vol.json"
FRED_KEY = os.environ.get("FRED_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

REALIZED_WINDOW = 30           # 30-day realized vol
ZSCORE_HISTORY_DAYS = 252      # 1-year baseline
ANNUALIZATION = math.sqrt(252) # daily -> annual

CHANNELS = [
    {"id": "dgs10",        "fred": "DGS10",
     "name": "10Y Treasury Yield Vol", "weight": 1.3,
     "edge": "Duration risk - long-bond price sensitivity to vol regime"},
    {"id": "dgs2",         "fred": "DGS2",
     "name": "2Y Treasury Yield Vol", "weight": 1.2,
     "edge": "Fed-path risk - markets re-pricing rate-cut expectations"},
    {"id": "dgs30",        "fred": "DGS30",
     "name": "30Y Treasury Yield Vol", "weight": 1.0,
     "edge": "Long-end / term-premium vol - convexity & pension/insurance hedging"},
    {"id": "dgs5",         "fred": "DGS5",
     "name": "5Y Treasury Yield Vol", "weight": 1.0,
     "edge": "Belly vol - the most rate-sensitive part of the curve"},
    {"id": "t10y2y",       "fred": "T10Y2Y",
     "name": "2s10s Curve Vol", "weight": 1.0,
     "edge": "Curve reshape risk - bull/bear steepener/flattener regime change"},
    {"id": "t10y3m",       "fred": "T10Y3M",
     "name": "10Y-3M Curve Vol", "weight": 0.9,
     "edge": "Recession-signal curve - the Fed's preferred inversion gauge"},
    {"id": "hy_spread",    "fred": "BAMLH0A0HYM2",
     "name": "HY Credit Spread Vol", "weight": 1.2,
     "edge": "Credit risk - high-yield spread blowouts foreshadow risk-off"},
    {"id": "ig_bbb",       "fred": "BAMLC0A4CBBB",
     "name": "BBB IG Spread Vol", "weight": 1.1,
     "edge": "IG-quality stress - earliest credit-cycle deterioration signal"},
    {"id": "tips_10y",     "fred": "DFII10",
     "name": "10Y Real Yield Vol", "weight": 1.0,
     "edge": "Real-rate vol - the true discount-rate shock to risk assets"},
    {"id": "breakeven_10y","fred": "T10YIE",
     "name": "10Y Breakeven Inflation Vol", "weight": 0.9,
     "edge": "Inflation-expectation vol - re-pricing of the Fed reaction function"},
]

REGIME_BANDS = [
    (2.0,  "CRISIS"),
    (1.0,  "ELEVATED"),
    (-1.0, "NORMAL"),
    (-99,  "BOND_VOL_LOW"),
]

HTTP_TIMEOUT = 20


def http_json(url):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
            return json.loads(r.read().decode("utf-8", errors="ignore"))
    except Exception as e:
        return {"_error": str(e)[:200]}


def fred_obs(series_id, days=400):
    """Fetch recent FRED observations. Prefer the Cloudflare worker /fred route
    (cached, no 429); fall back to direct FRED if the worker is unavailable."""
    # 1) Worker /fred (proven live + cached) — returns {bars:[{time,value}]}
    try:
        wurl = f"https://justhodl-data-proxy.raafouis.workers.dev/fred?series={series_id}&obs=600"
        d = http_json(wurl)
        if isinstance(d, dict) and not d.get("_error"):
            bars = d.get("bars") or d.get("observations") or []
            out = []
            for b in bars:
                v = b.get("value")
                dt = b.get("date") or b.get("time")
                if v in (None, ".", ""):
                    continue
                try:
                    # time may be epoch seconds or ISO date
                    if isinstance(dt, (int, float)):
                        dt = datetime.fromtimestamp(dt, tz=timezone.utc).date().isoformat()
                    out.append({"date": str(dt), "value": float(v)})
                except (ValueError, KeyError, TypeError):
                    continue
            if len(out) >= 60:
                out.sort(key=lambda x: x["date"])
                return out
    except Exception:
        pass
    # 2) Direct FRED fallback
    if not FRED_KEY:
        return None
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=days)
    url = (f"https://api.stlouisfed.org/fred/series/observations"
           f"?series_id={series_id}&api_key={FRED_KEY}&file_type=json"
           f"&observation_start={start}&observation_end={end}")
    d = http_json(url)
    if not isinstance(d, dict) or "_error" in d:
        return None
    obs = d.get("observations", [])
    out = []
    for o in obs:
        v = o.get("value")
        if v in (None, ".", ""):
            continue
        try:
            out.append({"date": o["date"], "value": float(v)})
        except (ValueError, KeyError):
            continue
    return out


def realized_vol(series, window=REALIZED_WINDOW):
    """Annualized stdev of daily first-differences over last `window` days."""
    if not series or len(series) < window + 1:
        return None
    vals = [s["value"] for s in series[-window-1:]]
    diffs = [vals[i+1] - vals[i] for i in range(len(vals) - 1)]
    if not diffs or len(diffs) < 5:
        return None
    try:
        sd = statistics.stdev(diffs)
        return round(sd * ANNUALIZATION, 4)
    except statistics.StatisticsError:
        return None


def historical_vol_series(series, window=REALIZED_WINDOW,
                          history_n=ZSCORE_HISTORY_DAYS):
    """Build rolling realized-vol time series over last `history_n` days
    for z-score baseline."""
    if not series or len(series) < window + history_n:
        return []
    out = []
    for i in range(window, len(series)):
        seg = series[i - window:i + 1]
        rv = realized_vol(seg, window=window)
        if rv is not None:
            out.append(rv)
    return out[-history_n:]


def zscore_now(vol_now, vol_history):
    if vol_now is None or not vol_history or len(vol_history) < 30:
        return None, None
    try:
        m = statistics.mean(vol_history)
        sd = statistics.stdev(vol_history)
        if sd == 0:
            return 0.0, 50.0
        z = (vol_now - m) / sd
        z = max(-3.0, min(3.0, z))
        # Percentile rank
        below = sum(1 for v in vol_history if v < vol_now)
        pct = round(100 * below / len(vol_history), 1)
        return round(z, 2), pct
    except statistics.StatisticsError:
        return None, None


def classify_regime(composite_z):
    if composite_z is None:
        return "DATA_UNAVAILABLE"
    for thresh, regime in REGIME_BANDS:
        if composite_z >= thresh:
            return regime
    return "BOND_VOL_LOW"


def telegram_notify(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        import urllib.parse
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        data = urllib.parse.urlencode({
            "chat_id": TELEGRAM_CHAT_ID,
            "text": msg,
            "parse_mode": "Markdown",
        }).encode("utf-8")
        urllib.request.urlopen(urllib.request.Request(url, data=data),
                               timeout=10)
    except Exception:
        pass


def lambda_handler(event, context):
    started = datetime.now(timezone.utc)
    if not FRED_KEY:
        return {"statusCode": 500,
                "body": json.dumps({"ok": False, "error": "FRED_KEY not set"})}

    per_channel = []
    z_values = []
    for ch in CHANNELS:
        # 600 calendar days = ~420 trading days; need 30 (window) + 252 (history) = 282 minimum
        series = fred_obs(ch["fred"], days=600)
        if not series or len(series) < REALIZED_WINDOW + 30:
            per_channel.append({
                "id": ch["id"], "fred_series": ch["fred"], "name": ch["name"],
                "edge": ch["edge"],
                "ok": False, "error": "insufficient data",
                "realized_vol_now": None, "z_score": None,
                "percentile_1y": None, "latest_obs_date": None,
            })
            continue

        rv_now = realized_vol(series)
        hist = historical_vol_series(series)
        z, pct = zscore_now(rv_now, hist)

        per_channel.append({
            "id": ch["id"], "fred_series": ch["fred"], "name": ch["name"],
            "edge": ch["edge"],
            "ok": True,
            "realized_vol_now": rv_now,
            "vol_units": "annualized stdev of daily changes",
            "z_score": z,
            "percentile_1y": pct,
            "n_observations_1y": len(hist),
            "latest_obs_date": series[-1]["date"],
            "latest_obs_value": series[-1]["value"],
        })
        if z is not None:
            z_values.append(z)

    # Weighted composite (each channel carries its own weight)
    wsum = 0.0; wtot = 0.0
    by_id = {c["id"]: c for c in per_channel}
    for ch in CHANNELS:
        c = by_id.get(ch["id"])
        if c and c.get("ok") and c.get("z_score") is not None:
            w = ch.get("weight", 1.0)
            wsum += c["z_score"] * w; wtot += w
    composite_z = round(wsum / wtot, 2) if wtot else (round(statistics.mean(z_values), 2) if z_values else None)
    regime = classify_regime(composite_z)
    n_live = sum(1 for c in per_channel if c.get("ok"))

    # Composite history for trend + duration + percentile
    try:
        hist_doc = json.loads(boto3.client("s3").get_object(Bucket=S3_BUCKET, Key="data/bond-vol-history.json")["Body"].read())
    except Exception:
        hist_doc = {"points": []}
    points = hist_doc.get("points", [])
    today = started.date().isoformat()
    if composite_z is not None:
        if not points or points[-1].get("date") != today:
            points.append({"date": today, "z": composite_z, "regime": regime})
        else:
            points[-1] = {"date": today, "z": composite_z, "regime": regime}
    points = points[-400:]
    zhist = [p["z"] for p in points if p.get("z") is not None]
    def _chg(n):
        if composite_z is None or len(zhist) <= n: return None
        return round(composite_z - zhist[-1-n], 2)
    trend = {"dod": _chg(1), "wow": _chg(5), "mom": _chg(21)}
    streak = 0
    for p in reversed(points):
        if p.get("regime") == regime: streak += 1
        else: break
    comp_pct = None
    if composite_z is not None and len(zhist) >= 30:
        comp_pct = round(100 * sum(1 for z in zhist if z < composite_z) / len(zhist), 1)
    def _zof(cid):
        c = by_id.get(cid); return c.get("z_score") if c and c.get("ok") else None
    front = [z for z in (_zof("dgs2"), _zof("dgs5")) if z is not None]
    longend = [z for z in (_zof("dgs10"), _zof("dgs30")) if z is not None]
    credit = [z for z in (_zof("hy_spread"), _zof("ig_bbb")) if z is not None]
    real = [z for z in (_zof("tips_10y"), _zof("breakeven_10y")) if z is not None]
    term_structure = {
        "front_end_z": round(statistics.mean(front), 2) if front else None,
        "long_end_z": round(statistics.mean(longend), 2) if longend else None,
        "credit_z": round(statistics.mean(credit), 2) if credit else None,
        "real_rate_z": round(statistics.mean(real), 2) if real else None,
    }
    fe, le = term_structure["front_end_z"], term_structure["long_end_z"]
    if fe is not None and le is not None:
        if fe - le > 0.5: ts_signal = "FRONT-END LED - Fed/rate-path vol dominates (policy uncertainty)"
        elif le - fe > 0.5: ts_signal = "LONG-END LED - term-premium/duration vol dominates (fiscal/supply)"
        else: ts_signal = "BALANCED - vol even across the curve"
    else: ts_signal = None
    PLAYBOOK = {
        "CRISIS": {"equities": "De-risk hard. Bond-vol crisis extremes precede the deepest equity drawdowns; cut gross, raise cash, favor quality/low-beta.", "duration": "Whipsaw both ways - size down.", "credit": "HY blow-out risk; up-in-quality.", "vol": "Own convexity - long-vol/tail hedges cheap vs realized.", "risk_posture": "RISK-OFF"},
        "ELEVATED": {"equities": "Trim beta, tighten stops. Rising bond vol = rising correlation; diversification fails when you need it.", "duration": "Reduce duration; carry fragile.", "credit": "Reduce HY; spreads follow with a lag.", "vol": "Accumulate hedges while affordable.", "risk_posture": "CAUTIOUS"},
        "NORMAL": {"equities": "Factor/sector selection drives returns, not macro vol. Stay invested.", "duration": "Carry & curve trades viable.", "credit": "Spread carry attractive; selective HY OK.", "vol": "Hedges optional; sell premium selectively.", "risk_posture": "NEUTRAL"},
        "BOND_VOL_LOW": {"equities": "Compressed vol = complacency. Good near-term BUT the calm precedes spikes - don't add leverage into the quiet.", "duration": "Carry/roll-down favorable; stops matter.", "credit": "Spread carry juicy but crowded.", "vol": "Vol cheap - opportunistic time to BUY tail protection.", "risk_posture": "RISK-ON (complacency watch)"},
        "DATA_UNAVAILABLE": {"risk_posture": "UNKNOWN"},
    }
    playbook = PLAYBOOK.get(regime, PLAYBOOK["NORMAL"])
    try:
        boto3.client("s3").put_object(Bucket=S3_BUCKET, Key="data/bond-vol-history.json",
            Body=json.dumps({"points": points}, default=str).encode(), ContentType="application/json")
    except Exception as e:
        print(f"[bond-vol] history write err: {e}")

    out = {
        "ok": True,
        "version": "2.0",
        "generated_at": started.isoformat(),
        "regime": regime,
        "composite_z_score": composite_z,
        "composite_percentile": comp_pct,
        "regime_streak_days": streak,
        "trend": trend,
        "term_structure": {**term_structure, "signal": ts_signal},
        "playbook": playbook,
        "risk_posture": playbook.get("risk_posture"),
        "history": points[-180:],
        "n_channels_live": n_live,
        "n_channels_total": len(CHANNELS),
        "channels": per_channel,
        "regime_bands": {
            "CRISIS":       "composite z >= +2.0 - tail event in progress",
            "ELEVATED":     "+1.0 <= z < +2.0  - stress building",
            "NORMAL":       "-1.0 <  z < +1.0  - baseline",
            "BOND_VOL_LOW": "z <= -1.0 - compressed vol, complacency risk",
        },
        "methodology": {
            "realized_vol_window_days": REALIZED_WINDOW,
            "zscore_baseline_days": ZSCORE_HISTORY_DAYS,
            "annualization": "sqrt(252)",
            "composite_aggregation": ("weighted z-score across 10 bond-stress channels"),
            "outlier_treatment": "z clipped to +/- 3.0 sigma",
        },
        "sources": {
            "data_provider": "FRED (Federal Reserve Bank of St. Louis)",
            "channels": {c["id"]: c["fred"] for c in CHANNELS},
            "rationale": ("ICE BofA MOVE Index is paywalled; this composite "
                          "is the institutional MOVE-substitute used by "
                          "ARP/macro funds when MOVE data unavailable"),
        },
        "edge_basis": ("Cross-asset bond vol regime is the leading indicator "
                       "for risk-off rotations. MOVE/VIX ratios at extremes "
                       "(both compressed or both spiked) precede 70%+ of "
                       "S&P 500 drawdowns >5% within 60 days (BofA Global "
                       "Quant 2019, 2022). This composite mirrors MOVE's "
                       "analytical intent using free FRED-only data."),
    }

    try:
        s3 = boto3.client("s3")
        s3.put_object(
            Bucket=S3_BUCKET, Key=S3_KEY,
            Body=json.dumps(out, default=str).encode("utf-8"),
            ContentType="application/json",
            CacheControl="public, max-age=300",
        )
    except Exception as e:
        return {"statusCode": 500,
                "body": json.dumps({"ok": False,
                                    "error": f"s3 put failed: {str(e)[:200]}"})}

    if regime in ("CRISIS", "ELEVATED"):
        worst = max(per_channel, key=lambda c: (c.get("z_score") or -99))
        telegram_notify(
            f"⚠️ *Bond Vol {regime}*\n"
            f"Composite z: {composite_z}\n"
            f"Worst channel: {worst.get('name')} (z={worst.get('z_score')}, "
            f"{worst.get('percentile_1y')}th pct)\n"
            f"justhodl.ai/bond-vol.html"
        )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "ok": True,
            "version": VERSION,
            "regime": regime,
            "composite_z": composite_z,
            "n_channels_live": n_live,
            "n_channels_total": len(CHANNELS),
        }),
    }


if __name__ == "__main__":
    r = lambda_handler({}, None)
    print(json.dumps(r, indent=2))
