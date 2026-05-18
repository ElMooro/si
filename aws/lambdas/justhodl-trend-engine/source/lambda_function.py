"""
justhodl-trend-engine  the cross-asset systematic trend desk.

The whole opportunity stack is long-biased single-name equity plus
discretionary macro. The one strategy ARCHETYPE missing is the CTA /
managed-futures engine: rules-based time-series (absolute) momentum across
a multi-asset universe, volatility-targeted. It is the classic crisis
diversifier - time-series momentum is the strategy that historically makes
money WHEN the long-only book and even a market-neutral pairs book are
correlated and bleeding. Adding it maximises the decorrelation of the
overall system.

The naive trend model walks into five well-documented traps; this engine
is built to dodge each:

  1. One lookback. A single 12-month signal is a timing-luck bet. -> blend
     three horizons (63 / 126 / 252 trading days); the slow legs catch
     persistent trends, the fast leg catches turns.
  2. Raw return as signal. +20% in 8%-vol Treasuries and +20% in 40%-vol
     crude are not the same conviction. -> score RISK-ADJUSTED momentum
     (a momentum t-stat: cumulative return / (daily vol * sqrt(L))).
  3. Equal dollar weight. Equal notional in crude and bonds is just a
     crude-vol bet. -> inverse-volatility sizing with a portfolio
     volatility target (the core of every real CTA).
  4. Trade every wiggle. Whipsaw in chop kills trend models. -> a signal
     deadband (flat in the middle) and a slow-tilted ensemble.
  5. Chase blow-offs / hold into v-reversals. -> a trend-maturity read
     (fresh / developing / extended) haircuts stretched trends, and a
     counter-trend shock guard haircuts a position that just took a
     violent move against its signal. Trim, never fight the tape.

UNIVERSE  21 liquid, plain-vanilla 1x ETF proxies across six asset
          classes (Equities / Rates / Credit / Commodities / FX / Crypto).
INPUT     FMP /stable/historical-price-eod/light  (real daily closes).
OUTPUT    data/trend-engine.json                  SCHEDULE  daily 23:50 UTC
Real data only. Research, not investment advice.
"""
import json
import math
import time
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import boto3

s3 = boto3.client("s3")
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/trend-engine.json"
FMP = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
BASE = "https://financialmodelingprep.com/stable"
WORKERS = 8

# ----------------------------------------------------------------- config --
HORIZONS = [63, 126, 252]          # ~3m / 6m / 12m trading days
HORIZON_W = {63: 0.25, 126: 0.35, 252: 0.40}   # slow-tilted ensemble
VOL_WINDOW = 63                    # daily-return window for volatility
MA_WINDOW = 200                    # trend-maturity reference moving average
DEADBAND = 0.35                    # |blended t-stat| below this -> FLAT
TARGET_VOL = 0.10                  # 10% annualised portfolio vol target
MIN_BARS = 260                     # need >252 + buffer for the 12m leg
ANNUAL = 252.0

# --------------------------------------------------------------- universe --
# (symbol, name, asset_class).  All US-listed, liquid, plain 1x.
UNIVERSE = [
    ("SPY", "S&P 500", "Equities"),
    ("QQQ", "Nasdaq 100", "Equities"),
    ("IWM", "US Small Cap (Russell 2000)", "Equities"),
    ("EFA", "Developed ex-US Equity", "Equities"),
    ("EEM", "Emerging Market Equity", "Equities"),
    ("SHY", "1-3y US Treasury", "Rates"),
    ("IEF", "7-10y US Treasury", "Rates"),
    ("TLT", "20y+ US Treasury", "Rates"),
    ("LQD", "Investment-Grade Credit", "Credit"),
    ("HYG", "US High-Yield Credit", "Credit"),
    ("DBC", "Broad Commodities", "Commodities"),
    ("GLD", "Gold", "Commodities"),
    ("SLV", "Silver", "Commodities"),
    ("CPER", "Copper", "Commodities"),
    ("USO", "Crude Oil", "Commodities"),
    ("DBA", "Agriculture", "Commodities"),
    ("UUP", "US Dollar Index (bull)", "FX"),
    ("FXE", "Euro", "FX"),
    ("FXY", "Japanese Yen", "FX"),
    ("FXC", "Canadian Dollar", "FX"),
    ("IBIT", "Bitcoin (spot ETF)", "Crypto"),
]
CLASSES = ["Equities", "Rates", "Credit", "Commodities", "FX", "Crypto"]


# ------------------------------------------------------------------ fetch --
def fmp_closes(symbol):
    """Return oldest-first list of daily closes, or None on failure."""
    url = (f"{BASE}/historical-price-eod/light?symbol={symbol}"
           f"&apikey={FMP}")
    try:
        req = urllib.request.Request(url, headers={"User-Agent":
                                                   "justhodl-trend"})
        with urllib.request.urlopen(req, timeout=25) as r:
            data = json.loads(r.read())
    except Exception:
        return None
    if not isinstance(data, list) or not data:
        return None
    # FMP light endpoint is newest-first {date, price, volume}
    rows = []
    for d in data:
        p = d.get("price")
        if isinstance(p, (int, float)) and p > 0:
            rows.append((d.get("date"), float(p)))
    if len(rows) < MIN_BARS:
        return None
    rows.sort(key=lambda x: x[0])           # oldest-first
    return [p for _, p in rows]


# -------------------------------------------------------------- statistics --
def log_returns(closes):
    out = []
    for i in range(1, len(closes)):
        prev, cur = closes[i - 1], closes[i]
        if prev > 0 and cur > 0:
            out.append(math.log(cur / prev))
    return out


def stdev(xs):
    n = len(xs)
    if n < 2:
        return 0.0
    m = sum(xs) / n
    return math.sqrt(sum((x - m) ** 2 for x in xs) / (n - 1))


def annualised_vol(rets, window):
    seg = rets[-window:] if len(rets) >= window else rets
    return stdev(seg) * math.sqrt(ANNUAL)


def momentum_tstat(rets, horizon):
    """Risk-adjusted momentum: cumulative log return over the horizon
    divided by (daily vol * sqrt(horizon)) - a dimensionless t-stat
    that is comparable across assets of very different volatility.

    The daily-vol denominator is floored at 0.02%/day and the result is
    clamped to +/-12: no real liquid asset trades that quietly or carries
    a momentum t-stat past ~4, so anything beyond the clamp is degenerate
    or stale data, not a signal."""
    if len(rets) < horizon:
        return None, None
    seg = rets[-horizon:]
    cum = sum(seg)
    dvol = max(stdev(seg), 2e-4)
    tstat = cum / (dvol * math.sqrt(horizon))
    tstat = max(-12.0, min(12.0, tstat))
    return tstat, cum


# ---------------------------------------------------------------- scoring --
def squash(x):
    """Map a magnitude to a 0-100 conviction via a smooth saturating curve.
    |t-stat| ~0.35 -> ~30, ~1.0 -> ~68, ~2.0 -> ~88, saturates near 100."""
    return round(100.0 * (1.0 - math.exp(-abs(x) / 0.9)), 1)


def maturity_label(sigma):
    a = abs(sigma)
    if a < 1.0:
        return "fresh"
    if a < 2.5:
        return "developing"
    return "extended"


def analyse(symbol, name, asset_class, closes):
    rets = log_returns(closes)
    if len(rets) < HORIZONS[-1]:
        return None
    px = closes[-1]
    avol = annualised_vol(rets, VOL_WINDOW)
    if avol <= 0:
        return None

    # multi-horizon risk-adjusted momentum
    tstats, raw_rets = {}, {}
    for L in HORIZONS:
        t, cum = momentum_tstat(rets, L)
        if t is None:
            return None
        tstats[L] = t
        raw_rets[L] = math.exp(cum) - 1.0     # simple % return over horizon

    blended = sum(HORIZON_W[L] * tstats[L] for L in HORIZONS)

    # trend maturity vs the 200d moving average, in volatility units
    ma = sum(closes[-MA_WINDOW:]) / min(MA_WINDOW, len(closes))
    dist = (px / ma - 1.0) if ma > 0 else 0.0
    maturity_sigma = dist / avol if avol > 0 else 0.0
    maturity = maturity_label(maturity_sigma)

    # counter-trend shock guard: a >2.5 daily-sigma move against the
    # blended signal inside the last 3 sessions
    dvol_recent = stdev(rets[-VOL_WINDOW:]) or 1e-9
    sign = 1 if blended > 0 else (-1 if blended < 0 else 0)
    stress = False
    for r in rets[-3:]:
        if abs(r) > 2.5 * dvol_recent and sign != 0 and \
           (r > 0) != (blended > 0):
            stress = True
            break

    # direction with the deadband
    if abs(blended) < DEADBAND:
        direction = "FLAT"
    elif blended > 0:
        direction = "LONG"
    else:
        direction = "SHORT"

    notes = []
    if maturity == "extended":
        notes.append("Trend is stretched (>2.5 sigma vs the 200d average) - "
                      "position size trimmed; trail rather than chase.")
    elif maturity == "fresh":
        notes.append("Trend is young - higher continuation odds, lower "
                      "reversal risk.")
    if stress:
        notes.append("A violent counter-trend session just printed - size "
                      "halved while the tape confirms whether the trend holds.")
    if direction == "FLAT":
        notes.append("Signal inside the deadband - no trend edge; stay flat "
                      "and avoid the whipsaw.")
    horizon_split = sorted(set(1 if tstats[L] > 0 else -1 for L in HORIZONS))
    if direction != "FLAT" and len(horizon_split) > 1:
        notes.append("Horizons disagree (fast vs slow) - the trend is "
                      "turning or choppy; treat conviction as provisional.")

    return {
        "symbol": symbol,
        "name": name,
        "asset_class": asset_class,
        "price": round(px, 4),
        "direction": direction,
        "blended_tstat": round(blended, 3),
        "conviction": squash(blended) if direction != "FLAT" else 0.0,
        "mom_63d": round(tstats[63], 3),
        "mom_126d": round(tstats[126], 3),
        "mom_252d": round(tstats[252], 3),
        "ret_63d_pct": round(raw_rets[63] * 100, 2),
        "ret_126d_pct": round(raw_rets[126] * 100, 2),
        "ret_252d_pct": round(raw_rets[252] * 100, 2),
        "annual_vol_pct": round(avol * 100, 1),
        "maturity": maturity,
        "maturity_sigma": round(maturity_sigma, 2),
        "stress_flag": stress,
        "_sign": sign,
        "_avol": avol,
        "notes": notes,
    }


# -------------------------------------------------------- vol targeting --
def size_book(positions):
    """Inverse-volatility sizing scaled to a portfolio volatility target.

    For an inverse-vol book the per-asset risk contribution w_i*vol_i is a
    constant k (since w_i = k*sign/vol_i). Ignoring correlation, portfolio
    vol = k*sqrt(n_active); set that to TARGET_VOL to solve k. Maturity and
    shock haircuts are applied afterwards (they only pull realised vol
    slightly below target - deliberately conservative)."""
    active = [p for p in positions if p["direction"] != "FLAT"]
    n = len(active)
    if n == 0:
        for p in positions:
            p["target_weight_pct"] = 0.0
        return 0.0, 0.0
    k = TARGET_VOL / math.sqrt(n)
    gross = 0.0
    net_eq = 0.0
    for p in positions:
        if p["direction"] == "FLAT":
            p["target_weight_pct"] = 0.0
            continue
        w = k * p["_sign"] / p["_avol"]            # inverse-vol, signed
        if p["maturity"] == "extended":
            w *= 0.60
        elif p["maturity"] == "developing":
            w *= 0.85
        if p["stress_flag"]:
            w *= 0.50
        wpct = round(w * 100, 2)
        p["target_weight_pct"] = wpct
        gross += abs(wpct)
        if p["asset_class"] == "Equities":
            net_eq += wpct
    return round(gross, 1), round(net_eq, 1)


# ------------------------------------------------------------- aggregate --
def class_breakdown(positions):
    out = []
    for c in CLASSES:
        members = [p for p in positions if p["asset_class"] == c]
        if not members:
            continue
        net = sum(p["target_weight_pct"] for p in members)
        gross = sum(abs(p["target_weight_pct"]) for p in members)
        longs = sum(1 for p in members if p["direction"] == "LONG")
        shorts = sum(1 for p in members if p["direction"] == "SHORT")
        flats = sum(1 for p in members if p["direction"] == "FLAT")
        if net > 1.0:
            stance = "NET LONG"
        elif net < -1.0:
            stance = "NET SHORT"
        else:
            stance = "NEUTRAL"
        out.append({
            "asset_class": c,
            "stance": stance,
            "net_weight_pct": round(net, 1),
            "gross_weight_pct": round(gross, 1),
            "longs": longs, "shorts": shorts, "flats": flats,
        })
    return out


def regime_read(positions):
    active = [p for p in positions if p["direction"] != "FLAT"]
    total = len(positions)
    breadth = round(100.0 * len(active) / total, 0) if total else 0.0
    if breadth >= 65:
        regime = "BROAD TREND"
    elif breadth >= 40:
        regime = "MIXED"
    else:
        regime = "CHOP"
    return breadth, regime


# ------------------------------------------------------------------ main --
def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)

    closes_by_sym = {}
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(fmp_closes, s): s for s, _, _ in UNIVERSE}
        for f in as_completed(futs):
            sym = futs[f]
            c = f.result()
            if c:
                closes_by_sym[sym] = c

    positions = []
    for sym, name, cls in UNIVERSE:
        closes = closes_by_sym.get(sym)
        if not closes:
            continue
        rec = analyse(sym, name, cls, closes)
        if rec:
            positions.append(rec)

    gross, net_eq = size_book(positions)
    breadth, regime = regime_read(positions)
    classes = class_breakdown(positions)

    # strip internals before serialising
    for p in positions:
        p.pop("_sign", None)
        p.pop("_avol", None)

    longs = [p for p in positions if p["direction"] == "LONG"]
    shorts = [p for p in positions if p["direction"] == "SHORT"]
    flats = [p for p in positions if p["direction"] == "FLAT"]
    ranked = sorted(positions, key=lambda x: x["conviction"], reverse=True)

    strongest = ranked[0] if ranked and ranked[0]["conviction"] > 0 else None
    up = sorted([p for p in longs], key=lambda x: x["conviction"],
                reverse=True)
    dn = sorted([p for p in shorts], key=lambda x: x["conviction"],
                reverse=True)

    if strongest is None:
        headline = ("No asset class clears the trend deadband - the cross-"
                    "asset tape is in chop. The systematic book is flat; "
                    "wait for a trend to establish before adding risk.")
    else:
        up_s = ", ".join(p["symbol"] for p in up[:3]) or "none"
        dn_s = ", ".join(p["symbol"] for p in dn[:3]) or "none"
        headline = (
            f"Trend regime: {regime} ({breadth:.0f}% of the universe "
            f"trending). Long: {up_s}. Short: {dn_s}. Strongest signal: "
            f"{strongest['name']} ({strongest['symbol']}) "
            f"{strongest['direction']}, conviction {strongest['conviction']}.")

    payload = {
        "schema_version": "1.0",
        "engine": "justhodl-trend-engine",
        "generated_at": now.isoformat(),
        "build_seconds": round(time.time() - t0, 1),
        "headline": headline,
        "summary": {
            "universe_count": len(UNIVERSE),
            "scored": len(positions),
            "n_long": len(longs),
            "n_short": len(shorts),
            "n_flat": len(flats),
            "trend_breadth_pct": breadth,
            "regime": regime,
            "portfolio_gross_pct": gross,
            "net_equity_tilt_pct": net_eq,
            "target_vol_pct": round(TARGET_VOL * 100, 0),
        },
        "positions": ranked,
        "by_class": classes,
        "strongest_long": up[0] if up else None,
        "strongest_short": dn[0] if dn else None,
        "how_to_read": (
            "Each instrument carries a risk-adjusted momentum t-stat blended "
            "across three horizons (3m/6m/12m). A signal past the deadband "
            "becomes a LONG or SHORT; target weights are inverse-volatility "
            "sized so every position contributes roughly equal risk, scaled "
            "to a 10% portfolio-vol target. Extended or shocked trends are "
            "trimmed, never reversed. Read the regime first: in CHOP the "
            "book is mostly flat by design."),
        "methodology": (
            "Time-series (absolute) momentum on real daily closes for 21 "
            "liquid 1x ETF proxies across six asset classes. Per asset: "
            "annualised vol (63d), a momentum t-stat per horizon "
            "(cumulative return / (daily vol * sqrt(horizon))), a slow-"
            "tilted blend (0.25/0.35/0.40 for 63/126/252d), a trend-maturity "
            "read vs the 200d average in vol units, and a counter-trend "
            "shock guard. Inverse-vol sizing with a portfolio-vol target. "
            "This is the canonical CTA / managed-futures construction."),
        "disclaimer": (
            "Research, not investment advice. Trend-following endures long "
            "flat and drawdown periods; ETF proxies carry tracking and "
            "financing costs not modelled here. Target weights are signal "
            "output, not a guaranteed return."),
    }

    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                  Body=json.dumps(payload, separators=(",", ":")).encode(),
                  ContentType="application/json", CacheControl="max-age=300")
    return {"statusCode": 200,
            "body": json.dumps({"scored": len(positions),
                                "regime": regime,
                                "n_long": len(longs),
                                "n_short": len(shorts),
                                "gross": gross})}


if __name__ == "__main__":
    print(json.dumps(lambda_handler({}, None), indent=1))
