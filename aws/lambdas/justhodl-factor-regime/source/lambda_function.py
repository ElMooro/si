"""justhodl-factor-regime -- STYLE-ETF RATIO TREND/THRUST DETECTOR
(factor momentum: factors themselves trend -- Ehsani-Linnainmaa line of
research, and the practical ratio system: track QQQ/SPY, IWM/SPY,
RSP/SPY, high-beta/low-vol, growth/value, semis/QQQ; when a ratio
bottoms below its 50d MA and thrusts above it, that can mark a new
"stock altseason" for that style).

DISTINCT FROM justhodl-factor-returns: that engine computes daily
cross-sectional LONG-SHORT factor returns from the ~5k-stock FinViz
universe (yesterday's factor P&L + crowding). THIS engine measures
tradeable style-ETF ratio REGIMES over weeks/months -- which side of
each style pair is trending, and whether a basing style just thrust.
Both exist at real funds; they answer different questions. Do not
merge or rebuild either into the other.

Per pair: ratio series (aligned trailing closes), above/below the
ratio's own 50d MA, 20d and 63d slopes, 1y z-score, state
LEADING/FADING/BASING/LAGGING, and THRUST = crossed above the 50d MA
within the last 10 sessions after spending >=40 of the prior 60 below
it. risk_appetite_score [-100..100] = signed mean z over
appetite-positive pairs minus defensives.

Out: data/factor-regime.json (CacheControl 900s). Zero LLM, all real
Polygon closes.
"""
import json
import os
import time
import urllib.request
from datetime import datetime, timedelta, timezone

import boto3

S3 = boto3.client("s3")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/factor-regime.json"
POLY = (os.environ.get("POLYGON_API_KEY")
        or os.environ.get("POLYGON_KEY") or "")

PAIRS = [
    ("QQQ", "SPY", "GROWTH_LARGE", +1,
     "mega-growth vs the market"),
    ("IWM", "SPY", "SMALL_SIZE", +1,
     "small caps vs the market (size factor)"),
    ("RSP", "SPY", "EQUAL_WEIGHT_BREADTH", +1,
     "equal-weight vs cap-weight -- breadth of participation"),
    ("SPHB", "SPLV", "HIGH_BETA_VS_LOWVOL", +1,
     "risk appetite: high beta vs low vol (BAB in ETF form)"),
    ("VUG", "VTV", "GROWTH_VS_VALUE", 0,
     "growth vs value style"),
    ("MTUM", "SPY", "MOMENTUM_FACTOR", 0,
     "momentum factor ETF vs market"),
    ("QUAL", "SPY", "QUALITY", 0,
     "quality/profitability factor vs market"),
    ("USMV", "SPY", "MINVOL_DEFENSE", -1,
     "min-vol defense vs market (leads when fear rises)"),
    ("ARKK", "SPY", "SPECULATIVE_APPETITE", +1,
     "hard-to-value speculative growth (Baker-Wurgler sentiment)"),
    ("SMH", "QQQ", "SEMIS_LEADERSHIP", +1,
     "semis vs tech -- the cycle's tip of the spear"),
    ("XBI", "SPY", "BIOTECH_RISK_APPETITE", +1,
     "small biotech vs market -- long-duration risk appetite"),
]


def _http(url, timeout=25, tries=2):
    for a in range(tries):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "justhodl-factor-regime"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode())
        except Exception:
            time.sleep(1.0 + a)
    return None


def polygon_daily(tkr, days=560):
    to = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    frm = (datetime.now(timezone.utc) - timedelta(days=days)
           ).strftime("%Y-%m-%d")
    d = _http("https://api.polygon.io/v2/aggs/ticker/%s/range/1/day/"
              "%s/%s?adjusted=true&sort=asc&limit=5000&apiKey=%s"
              % (tkr, frm, to, POLY))
    return [float(b["c"]) for b in (d or {}).get("results") or []]


def sma(v, n):
    return sum(v[-n:]) / n if len(v) >= n else None


def ratio_series(a, b):
    m = min(len(a), len(b))
    return [a[-m + i] / b[-m + i] for i in range(m) if b[-m + i]]


def pair_row(num, den, label, sign, note, closes):
    a, b = closes.get(num), closes.get(den)
    if not a or not b or min(len(a), len(b)) < 140:
        return {"pair": "%s/%s" % (num, den), "label": label,
                "error": "insufficient history"}
    r = ratio_series(a, b)
    r50 = sma(r, 50)
    above = bool(r50 and r[-1] > r50)
    s20 = (round((r[-1] / r[-21] - 1) * 100, 2)
           if len(r) >= 21 and r[-21] else None)
    s63 = (round((r[-1] / r[-64] - 1) * 100, 2)
           if len(r) >= 64 and r[-64] else None)
    z = None
    if len(r) >= 252:
        w = r[-252:]
        mu = sum(w) / len(w)
        sd = (sum((x - mu) ** 2 for x in w) / len(w)) ** 0.5
        if sd > 0:
            z = round((r[-1] - mu) / sd, 2)
    # thrust: above 50dMA now or within last 10 sessions' cross,
    # after >=40 of the prior 60 sessions BELOW the (rolling) 50dMA
    thrust = False
    if len(r) >= 121:
        rel = []
        for i in range(len(r) - 70, len(r)):
            m50 = sum(r[i - 49:i + 1]) / 50
            rel.append(r[i] > m50)
        recent, prior = rel[-10:], rel[-70:-10]
        was_below = sum(1 for x in prior if not x) >= 40
        fresh_cross = rel[-1] and any(
            rel[-k] and not rel[-k - 1] for k in range(1, 11))
        thrust = bool(was_below and fresh_cross)
    state = ("LEADING" if above and (s20 or 0) > 0 else
             "FADING" if above else
             "BASING" if (s20 or 0) > 0 else "LAGGING")
    return {"pair": "%s/%s" % (num, den), "label": label,
            "note": note, "sign": sign,
            "ratio": round(r[-1], 4), "above_50d": above,
            "slope_20d_pct": s20, "slope_63d_pct": s63,
            "z_1y": z, "state": state, "thrust": thrust}


def lambda_handler(event=None, context=None):
    tickers = sorted({t for p in PAIRS for t in (p[0], p[1])})
    closes = {}
    for t in tickers:
        closes[t] = polygon_daily(t)
        time.sleep(0.14)
    if len(closes.get("SPY") or []) < 140:
        raise RuntimeError("SPY short: %d (poly_key=%s)"
                           % (len(closes.get("SPY") or []), bool(POLY)))
    rows = [pair_row(*p, closes) for p in PAIRS]
    ok = [r for r in rows if "error" not in r]
    app = [r["z_1y"] * r["sign"] for r in ok
           if r.get("z_1y") is not None and r["sign"]]
    risk_appetite = (round(max(-100.0, min(100.0,
                     (sum(app) / len(app)) * 40)), 1) if app else None)
    thrusts = [r["pair"] + " (" + r["label"] + ")"
               for r in ok if r.get("thrust")]
    leading = [r["label"] for r in ok if r["state"] == "LEADING"]
    out = {
        "engine": "justhodl-factor-regime", "version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "doctrine": "factors themselves trend; a style ratio bottoming "
                    "below its 50d MA then thrusting above it can mark "
                    "a new leadership regime for that style. "
                    "Complementary to factor-returns (daily "
                    "cross-sectional L/S factor P&L).",
        "risk_appetite_score": risk_appetite,
        "risk_appetite_read": (
            None if risk_appetite is None else
            "RISK_ON" if risk_appetite >= 25 else
            "RISK_OFF" if risk_appetite <= -25 else "MIXED"),
        "thrusts": thrusts,
        "leading_styles": leading,
        "pairs": rows,
        "method": {
            "state": "above/below ratio 50dMA x 20d slope -> "
                     "LEADING/FADING/BASING/LAGGING",
            "thrust": "crossed above rolling 50dMA within 10 sessions "
                      "after >=40 of prior 60 below it",
            "risk_appetite": "mean(sign x 1y ratio z) x 40, clamp "
                             "+-100; +pairs: high-beta, spec, small, "
                             "growth-large, semis, biotech, breadth; "
                             "-: min-vol"},
        "errors": [r for r in rows if "error" in r][:5] or None}
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, separators=(",", ":")).encode(),
                  ContentType="application/json",
                  CacheControl="public, max-age=900")
    return {"statusCode": 200,
            "body": json.dumps({"ok": True,
                                "risk_appetite": risk_appetite,
                                "thrusts": len(thrusts),
                                "leading": len(leading)})}
