"""
justhodl-fx-intelligence — Institutional FX Regime & Carry Engine
═══════════════════════════════════════════════════════════════════════
WHY THIS EXISTS
───────────────
The platform pulls raw FX rates (dollar-strength-agent) and cross-currency
basis (xccy-basis-agent) but had NO analytical FX layer — the strategy view
a global-macro desk actually trades from. This engine adds it:

  • Per-currency scorecard — trend, momentum, realised vol & vol regime,
    all expressed as strength vs USD (so + = currency beating the dollar).
  • Broad USD regime — trend / momentum on the trade-weighted dollar.
  • FX Risk Barometer — high-beta/EM basket vs JPY+CHF haven basket. This
    reads the carry regime straight off currency BEHAVIOUR (no rate data
    needed, so it never goes stale): havens bid = deleveraging / carry
    unwind; high-beta bid = risk-on / carry-friendly.
  • Rate differentials — US 2y/10y vs core foreign yields (best effort,
    freshness-flagged — FRED's OECD monthly yields can lag).
  • CFTC FX positioning — net spec positioning + crowding, read best-effort
    off the platform's own CFTC engine.
  • USD regime call — WRECKING BALL / STRENGTH / DOWNTREND / RANGE with a
    0-100 dollar-pressure score and an explicit, decisive headline.

OUTPUT: data/fx-intelligence.json     SCHEDULE: daily 13:30 UTC
═══════════════════════════════════════════════════════════════════════
"""
import json
import os
import math
import urllib.request
import urllib.parse
from datetime import datetime, timezone, date

import boto3

S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/fx-intelligence.json"
FRED_KEY = os.environ.get("FRED_API_KEY", "2f057499936072679d8843d7fce99989")
CFTC_URL = os.environ.get(
    "CFTC_URL",
    "https://35t3serkv4gn2hk7utwvp7t2sa0flbum.lambda-url.us-east-1.on.aws/")

s3 = boto3.client("s3", region_name="us-east-1")

# quote convention: USD_PER_FX -> series already = FX strength (higher=FX up)
#                   FX_PER_USD -> invert (1/series) to get FX strength
CURRENCIES = [
    # code, name, FRED series, quote convention, bucket
    ("EUR", "Euro",            "DEXUSEU", "USD_PER_FX", "core"),
    ("JPY", "Japanese yen",    "DEXJPUS", "FX_PER_USD", "haven"),
    ("GBP", "British pound",   "DEXUSUK", "USD_PER_FX", "core"),
    ("CHF", "Swiss franc",     "DEXSZUS", "FX_PER_USD", "haven"),
    ("CAD", "Canadian dollar", "DEXCAUS", "FX_PER_USD", "high_beta"),
    ("AUD", "Australian dollar", "DEXUSAL", "USD_PER_FX", "high_beta"),
    ("NZD", "NZ dollar",       "DEXUSNZ", "USD_PER_FX", "high_beta"),
    ("CNY", "Chinese yuan",    "DEXCHUS", "FX_PER_USD", "em"),
    ("MXN", "Mexican peso",    "DEXMXUS", "FX_PER_USD", "high_beta"),
    ("BRL", "Brazilian real",  "DEXBZUS", "FX_PER_USD", "high_beta"),
    ("INR", "Indian rupee",    "DEXINUS", "FX_PER_USD", "em"),
    ("KRW", "Korean won",      "DEXKOUS", "FX_PER_USD", "high_beta"),
    ("ZAR", "South African rand", "DEXZAUS", "FX_PER_USD", "high_beta"),
]
USD_BROAD = "DTWEXBGS"   # trade-weighted broad dollar index
USD_REAL = "RTWEXBGS"    # real broad dollar (monthly)

# best-effort foreign long yields for rate differentials (OECD monthly)
FOREIGN_YIELDS = {
    "EUR": "IRLTLT01EZM156N", "JPY": "IRLTLT01JPM156N",
    "GBP": "IRLTLT01GBM156N", "CAD": "IRLTLT01CAM156N",
}


# ───────────────────────── data access ──────────────────────────
def fred(series_id, limit=420):
    """Return [(date, value), ...] newest-first; [] on failure."""
    url = (f"https://api.stlouisfed.org/fred/series/observations"
           f"?series_id={series_id}&api_key={FRED_KEY}&file_type=json"
           f"&sort_order=desc&limit={limit}")
    for _ in range(2):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "justhodl-fx/1.0"})
            with urllib.request.urlopen(req, timeout=25) as r:
                obs = json.loads(r.read()).get("observations", [])
            out = []
            for o in obs:
                try:
                    out.append((o["date"], float(o["value"])))
                except (ValueError, KeyError, TypeError):
                    continue
            return out
        except Exception as e:  # noqa: BLE001
            last = e
    print(f"[fx] FRED {series_id}: {last}")
    return []


def age_days(dstr):
    try:
        return (datetime.now(timezone.utc).date()
                - date.fromisoformat(dstr[:10])).days
    except Exception:
        return None


# ───────────────────────── statistics ───────────────────────────
def mean(xs):
    return sum(xs) / len(xs) if xs else 0.0


def stdev(xs):
    if len(xs) < 2:
        return 0.0
    m = mean(xs)
    return math.sqrt(sum((x - m) ** 2 for x in xs) / (len(xs) - 1))


def pct_change(series, n):
    """series newest-first; % change of latest vs n obs ago."""
    if len(series) <= n or series[n] == 0:
        return None
    return (series[0] / series[n] - 1.0) * 100.0


def realised_vol(series, win):
    """Annualised realised vol from daily log returns. series newest-first."""
    s = series[:win + 1]
    if len(s) < 5:
        return None
    rets = [math.log(s[i] / s[i + 1])
            for i in range(len(s) - 1) if s[i] > 0 and s[i + 1] > 0]
    return stdev(rets) * math.sqrt(252) * 100.0 if rets else None


def strength_series(obs, quote):
    """obs newest-first [(date,val)] -> FX-strength values newest-first."""
    vals = [v for _, v in obs if v and v > 0]
    if quote == "FX_PER_USD":
        vals = [1.0 / v for v in vals]
    return vals


# ───────────────────────── per-currency ─────────────────────────
def analyse_currency(code, name, series_id, quote, bucket):
    obs = fred(series_id)
    base = {"code": code, "name": name, "bucket": bucket,
            "fred_series": series_id}
    if len(obs) < 60:
        return {**base, "available": False, "reason": "insufficient data"}
    s = strength_series(obs, quote)
    if len(s) < 60:
        return {**base, "available": False, "reason": "insufficient data"}
    ma50 = mean(s[:50])
    ma200 = mean(s[:200]) if len(s) >= 200 else mean(s)
    latest = s[0]
    momz = ((latest - mean(s[:60])) / stdev(s[:60])) if stdev(s[:60]) else 0.0
    above50, above200 = latest > ma50, latest > ma200
    if above50 and above200:
        trend = "STRENGTHENING"
    elif not above50 and not above200:
        trend = "WEAKENING"
    else:
        trend = "MIXED"
    v20 = realised_vol(s, 20)
    v60 = realised_vol(s, 60)
    # vol regime: 20d vol vs trailing 1y mean of rolling 20d vols
    roll = [realised_vol(s[i:], 20) for i in range(0, min(252, len(s) - 25), 5)]
    roll = [x for x in roll if x is not None]
    vol_regime = "n/a"
    if v20 is not None and roll:
        rm = mean(roll)
        ratio = v20 / rm if rm else 1.0
        vol_regime = ("HIGH" if ratio > 1.5 else "ELEVATED" if ratio > 1.15
                      else "LOW" if ratio < 0.7 else "NORMAL")
    # directional signal -2..+2 (strength vs USD)
    sig = 0
    if trend == "STRENGTHENING":
        sig = 2 if momz > 1 else 1
    elif trend == "WEAKENING":
        sig = -2 if momz < -1 else -1
    return {**base, "available": True,
            "strength_index": round(latest, 5),
            "vs_usd_chg_1m": _r(pct_change(s, 21)),
            "vs_usd_chg_3m": _r(pct_change(s, 63)),
            "vs_usd_chg_6m": _r(pct_change(s, 126)),
            "vs_usd_chg_1y": _r(pct_change(s, 252)),
            "trend": trend, "momentum_z": round(momz, 2),
            "vol_20d": _r(v20), "vol_60d": _r(v60), "vol_regime": vol_regime,
            "signal": sig, "as_of": obs[0][0], "age_days": age_days(obs[0][0])}


def _r(x, n=2):
    return round(x, n) if isinstance(x, (int, float)) else None


# ───────────────────── rate differentials ───────────────────────
def rate_differentials():
    us2 = fred("DGS2", 30)
    us10 = fred("DGS10", 30)
    out = {"us_2y": _r(us2[0][1]) if us2 else None,
           "us_10y": _r(us10[0][1]) if us10 else None, "pairs": []}
    if not us10:
        return out
    for code, sid in FOREIGN_YIELDS.items():
        fy = fred(sid, 12)
        if not fy:
            continue
        a = age_days(fy[0][0])
        out["pairs"].append({
            "code": code, "foreign_10y": _r(fy[0][1]),
            "us_minus_foreign_10y": _r(us10[0][1] - fy[0][1]),
            "as_of": fy[0][0], "age_days": a,
            "stale": (a is not None and a > 95)})
    return out


# ───────────────────── CFTC positioning ─────────────────────────
def cftc_positioning():
    """Best-effort: read the platform's own CFTC engine for FX contracts."""
    fx_keys = ("EURO", "JAPANESE YEN", "BRITISH POUND", "SWISS FRANC",
               "CANADIAN DOLLAR", "AUSTRALIAN DOLLAR", "MEXICAN PESO",
               "NEW ZEALAND", "U.S. DOLLAR INDEX")
    try:
        req = urllib.request.Request(
            CFTC_URL.rstrip("/") + "/cot/all",
            headers={"User-Agent": "justhodl-fx/1.0"})
        with urllib.request.urlopen(req, timeout=12) as r:
            data = json.loads(r.read())
    except Exception as e:  # noqa: BLE001
        print(f"[fx] CFTC positioning unavailable: {e}")
        return {"available": False, "reason": "CFTC engine not reachable"}
    rows = data if isinstance(data, list) else (
        data.get("contracts") or data.get("data") or [])
    out = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        nm = str(row.get("contract") or row.get("name") or
                 row.get("market") or "").upper()
        if not any(k in nm for k in fx_keys):
            continue
        out.append({
            "contract": nm.title(),
            "net_position": row.get("net_position") or row.get("net"),
            "net_pctile": row.get("percentile") or row.get("net_pctile"),
            "signal": row.get("signal") or row.get("bias")})
    return {"available": bool(out), "contracts": out[:12]} if out else {
        "available": False, "reason": "no FX contracts in CFTC feed"}


# ───────────────────── USD regime / barometer ───────────────────
def usd_regime(usd_obs, currencies):
    if len(usd_obs) < 60:
        return {"available": False}
    s = [v for _, v in usd_obs if v]
    latest = s[0]
    ma50, ma200 = mean(s[:50]), mean(s[:200] if len(s) >= 200 else s)
    momz = ((latest - mean(s[:60])) / stdev(s[:60])) if stdev(s[:60]) else 0.0
    chg3m = pct_change(s, 63)
    usd_up = latest > ma200 and momz > 0
    usd_dn = latest < ma200 and momz < 0

    # FX Risk Barometer — high-beta basket vs JPY+CHF havens (3m strength)
    def basket(bk):
        xs = [c["vs_usd_chg_3m"] for c in currencies
              if c.get("available") and c["bucket"] == bk
              and c.get("vs_usd_chg_3m") is not None]
        return mean(xs) if xs else None
    hb = basket("high_beta")
    hv = mean([c["vs_usd_chg_3m"] for c in currencies
               if c.get("available") and c["bucket"] == "haven"
               and c.get("vs_usd_chg_3m") is not None] or [0])
    barometer = round(hb - hv, 2) if hb is not None else None
    risk_state = ("RISK-ON / carry-friendly" if (barometer or 0) > 1.5
                  else "RISK-OFF / carry unwind" if (barometer or 0) < -1.5
                  else "NEUTRAL")

    # regime label
    if usd_up and (barometer or 0) < -1.0:
        label = "USD WRECKING BALL"
        desc = ("Dollar bid as a haven while high-beta FX sells off — the "
                "dangerous regime: tightening global financial conditions.")
    elif usd_up:
        label = "USD STRENGTH"
        desc = "Dollar uptrend on US growth / yield advantage; orderly."
    elif usd_dn and (barometer or 0) > 1.0:
        label = "USD DOWNTREND — REFLATION"
        desc = ("Dollar weakening with high-beta / EM FX bid — easy global "
                "liquidity, carry-friendly tailwind for risk assets.")
    elif usd_dn:
        label = "USD DOWNTREND"
        desc = "Dollar drifting lower without a clear risk-on impulse."
    else:
        label = "USD RANGE"
        desc = "No decisive dollar trend; FX driven by idiosyncratic stories."

    # dollar-pressure 0-100 (high = strong USD / tight conditions)
    score = 50.0
    score += max(-25, min(25, momz * 12))
    score += max(-12, min(12, (chg3m or 0) * 2.5))
    score += -(barometer or 0) * 3.0   # haven bid for USD raises pressure
    score = max(0, min(100, round(score, 1)))
    return {"available": True, "broad_index": _r(latest), "ma50": _r(ma50),
            "ma200": _r(ma200), "momentum_z": round(momz, 2),
            "chg_3m": _r(chg3m), "regime": label, "description": desc,
            "dollar_pressure_0_100": score,
            "risk_barometer": barometer, "risk_state": risk_state}


# ───────────────────────── handler ──────────────────────────────
def lambda_handler(event, context):
    currencies = [analyse_currency(*c) for c in CURRENCIES]
    avail = [c for c in currencies if c.get("available")]
    usd_obs = fred(USD_BROAD)
    regime = usd_regime(usd_obs, currencies)
    rates = rate_differentials()
    positioning = cftc_positioning()

    strongest = sorted(
        [c for c in avail if c.get("vs_usd_chg_3m") is not None],
        key=lambda c: c["vs_usd_chg_3m"], reverse=True)
    headline = "FX engine: insufficient data"
    if regime.get("available"):
        top = strongest[0]["code"] if strongest else "n/a"
        bot = strongest[-1]["code"] if strongest else "n/a"
        headline = (f"{regime['regime']} (dollar pressure "
                    f"{regime['dollar_pressure_0_100']}/100, "
                    f"{regime['risk_state']}). Strongest vs USD: {top}; "
                    f"weakest: {bot}.")

    oldest = max([c.get("age_days") or 0 for c in avail] or [0])
    out = {
        "engine": "justhodl-fx-intelligence", "version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "headline": headline,
        "usd_regime": regime,
        "currencies": sorted(currencies,
                             key=lambda c: c.get("vs_usd_chg_3m") or -999,
                             reverse=True),
        "rate_differentials": rates,
        "positioning": positioning,
        "freshness": {"n_currencies": len(avail),
                      "oldest_fx_obs_days": oldest,
                      "fx_data_stale": oldest > 10},
    }
    body = json.dumps(out, default=str).encode()
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY, Body=body,
                  ContentType="application/json",
                  CacheControl="max-age=300")
    print(f"[fx] {headline}  ({len(body)} bytes)")
    return {"statusCode": 200, "body": headline}


if __name__ == "__main__":
    print(json.dumps(lambda_handler({}, None)))
