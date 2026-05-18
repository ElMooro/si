"""
justhodl-risk-radar  the platform's defensive desk.

The platform carries a dozen long-idea screens and zero risk screen.
Every serious book runs a paired risk / short lens; this is it - a
market-wide FUNDAMENTAL-DETERIORATION screen over the full S&P 500.

It is deliberately a different lens from the three risk engines that
already exist:
  * finra-short / short-interest / short-pressure  - short POSITIONING
    (what the crowd is shorting, squeeze setups). A crowding lens.
  * redflag-alerter                                - 8-K EVENT alerts
    (bankruptcy / restatement filings). A catalyst lens.
This engine is the missing third lens: a ranked, bucketed screen of
names whose BUSINESS is deteriorating - solvency stress, earnings-quality
erosion, an analyst exodus, momentum breakdown and overvaluation - fused
into one deterioration score, then split into genuine SHORT CANDIDATES
vs names to simply AVOID.

It cross-confirms with the short-positioning desk: a name whose
fundamentals are breaking down AND that the tape is already shorting is
a higher-conviction setup than fundamentals alone.

INPUT   screener/data.json          (503-name S&P universe, ~100% field
                                     population: altmanZ, interestCoverage,
                                     piotroski, FCF, downgrades, DCF gap,
                                     trailing returns, moving averages,
                                     insider / institutional flows)
        data/short-pressure.json    (optional confirming overlay)
        data/asymmetric-scorer.json (optional value-trap overlay)
OUTPUT  data/risk-radar.json        SCHEDULE  daily 14:30 UTC
Real data only. A risk screen flags danger; it does not time entries.
Research, not investment advice.
"""
import json
import time
from datetime import datetime, timezone

import boto3

s3 = boto3.client("s3")
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/risk-radar.json"

# sectors where Altman-Z and debt/equity are structurally meaningless
NO_SOLVENCY_SECTORS = {"Financial Services", "Financials",
                       "Real Estate", "Banks", "Insurance"}

CARRY_FLOOR = 35.0   # below this deterioration score a name is not carried
STACK_CAP = 90


# ------------------------------------------------------------- helpers --
def num(v):
    try:
        f = float(v)
        return f if f == f else None
    except (TypeError, ValueError):
        return None


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


def step(v, bands):
    """bands = [(threshold, value), ...] ascending by threshold; returns the
    value of the first band whose threshold v is <= , else the last value."""
    for thr, out in bands:
        if v <= thr:
            return out
    return bands[-1][1]


def usd(v):
    if v is None:
        return "n/a"
    a = abs(v)
    sign = "-" if v < 0 else ""
    if a >= 1e9:
        return f"{sign}${a / 1e9:.1f}B"
    if a >= 1e6:
        return f"{sign}${a / 1e6:.0f}M"
    return f"{sign}${a:,.0f}"


# ------------------------------------------------------------- inputs --
def load_universe():
    sc = json.loads(s3.get_object(
        Bucket=S3_BUCKET, Key="screener/data.json")["Body"].read())
    rows = sc.get("stocks")
    if not isinstance(rows, list):
        bs = sc.get("by_symbol") or {}
        rows = list(bs.values()) if isinstance(bs, dict) else []
    return [r for r in rows if isinstance(r, dict) and r.get("symbol")]


def load_short_pressure():
    """symbol -> True when the tape is building short pressure. Defensive:
    the engine works whether or not this feed parses."""
    out = {}
    try:
        d = json.loads(s3.get_object(
            Bucket=S3_BUCKET, Key="data/short-pressure.json")["Body"].read())
        pools = []
        for v in (d.values() if isinstance(d, dict) else []):
            if isinstance(v, list):
                pools.append(v)
        for pool in pools:
            for r in pool:
                if not isinstance(r, dict):
                    continue
                sym = (r.get("ticker") or r.get("symbol") or "").upper()
                state = str(r.get("state") or "").upper()
                if sym and "BUILDING" in state:
                    out[sym] = True
    except Exception:
        pass
    return out


def load_value_traps():
    """symbol -> trap_reason from the asymmetric desk. Defensive."""
    out = {}
    try:
        d = json.loads(s3.get_object(
            Bucket=S3_BUCKET,
            Key="data/asymmetric-scorer.json")["Body"].read())
        for r in d.get("value_traps", []) or []:
            if isinstance(r, dict) and r.get("symbol"):
                out[r["symbol"].upper()] = r.get("trap_reason") or "value trap"
    except Exception:
        pass
    return out


# --------------------------------------------------------- axis scores --
def axis_solvency(r, flags):
    """Balance-sheet stress, 0-100. None for financials / real estate."""
    parts = []
    z = num(r.get("altmanZ"))
    if z is not None:
        s = step(z, [(1.0, 1.0), (1.8, 0.75), (3.0, 0.40), (1e9, 0.0)])
        parts.append(s)
        if z < 1.8:
            flags.append(f"Altman-Z {z:.1f} - distress zone, elevated "
                         f"bankruptcy risk")
        elif z < 3.0:
            flags.append(f"Altman-Z {z:.1f} - grey zone, balance sheet "
                         f"not yet safe")
    ic = num(r.get("interestCoverage"))
    if ic is not None:
        s = step(ic, [(0.0, 1.0), (1.0, 0.9), (2.0, 0.65),
                      (4.0, 0.35), (1e9, 0.0)])
        parts.append(s)
        if ic < 1.0:
            flags.append(f"Interest coverage {ic:.1f}x - operating profit "
                         f"does not cover interest expense")
        elif ic < 2.0:
            flags.append(f"Interest coverage {ic:.1f}x - thin cushion on "
                         f"debt service")
    cr = num(r.get("currentRatio"))
    if cr is not None:
        parts.append(step(cr, [(0.8, 0.8), (1.0, 0.5),
                               (1.5, 0.2), (1e9, 0.0)]))
        if cr < 1.0:
            flags.append(f"Current ratio {cr:.2f} - short-term liabilities "
                         f"exceed short-term assets")
    fcf = num(r.get("freeCashFlow"))
    if fcf is not None and fcf < 0:
        parts.append(0.7)
        flags.append(f"Free cash flow {usd(fcf)} - burning cash")
    de = num(r.get("debtToEquity"))
    if de is not None:
        if de < 0:
            parts.append(0.85)
            flags.append("Negative book equity - liabilities exceed assets")
        else:
            s = step(de, [(1.0, 0.0), (2.0, 0.2), (3.0, 0.45), (1e9, 0.7)])
            parts.append(s)
            if de > 3.0:
                flags.append(f"Debt/equity {de:.1f} - heavily leveraged")
    if not parts:
        return None
    return round(100.0 * sum(parts) / len(parts), 1)


def axis_earnings(r, flags):
    """Earnings-quality erosion, 0-100."""
    parts = []
    p = num(r.get("piotroski"))
    if p is not None:
        parts.append(step(p, [(2, 1.0), (3, 0.8), (4, 0.55),
                              (5, 0.3), (99, 0.0)]))
        if p <= 3:
            flags.append(f"Piotroski {int(p)}/9 - weak fundamental quality")
    nm = num(r.get("netMargin"))
    if nm is not None:
        parts.append(step(nm, [(-10, 1.0), (0, 0.7), (3, 0.25), (1e9, 0.0)]))
        if nm < 0:
            flags.append(f"Net margin {nm:.1f}% - unprofitable at the "
                         f"bottom line")
    om = num(r.get("operatingMargin"))
    if om is not None and om < 0:
        parts.append(0.6)
        flags.append(f"Operating margin {om:.1f}% - core operations lose "
                     f"money")
    roic = num(r.get("roic"))
    if roic is not None:
        parts.append(step(roic, [(0, 0.7), (3, 0.35), (1e9, 0.0)]))
        if roic < 0:
            flags.append(f"ROIC {roic:.1f}% - destroying invested capital")
    eg = num(r.get("epsGrowth"))
    rg = num(r.get("revenueGrowth"))
    if eg is not None and rg is not None:
        if eg < 0 and rg < 0:
            parts.append(0.8)
            flags.append(f"Revenue {rg:.0f}% and EPS {eg:.0f}% - shrinking "
                         f"on both lines")
        elif eg < -20:
            parts.append(0.5)
            flags.append(f"EPS down {abs(eg):.0f}% - earnings contracting")
    asp = num(r.get("avgSurprisePct"))
    bs = num(r.get("beatStreak"))
    if asp is not None and asp < 0:
        extra = 0.4 + (0.2 if (bs is not None and bs == 0) else 0.0)
        parts.append(min(extra, 0.7))
        flags.append(f"Average earnings surprise {asp:.1f}% - a pattern of "
                     f"missing estimates")
    if not parts:
        return None
    return round(100.0 * sum(parts) / len(parts), 1)


def axis_analyst(r, flags):
    """Analyst / estimate deterioration, 0-100."""
    parts = []
    net90 = num(r.get("upgradeNet90d"))
    dg90 = num(r.get("downgrades90d"))
    if net90 is not None:
        parts.append(step(net90, [(-4, 0.9), (-1, 0.6), (1, 0.2),
                                  (1e9, 0.0)]))
        if net90 < 0:
            dgt = int(dg90) if dg90 is not None else 0
            flags.append(f"{dgt} downgrades in 90d, net rating momentum "
                         f"{int(net90):+d} - the street is cutting")
    dg30 = num(r.get("downgrades30d"))
    if dg30 is not None and dg30 >= 3:
        parts.append(0.6)
        flags.append(f"{int(dg30)} downgrades in the last 30 days - "
                     f"accelerating")
    sells = (num(r.get("gradesSell")) or 0) + \
            (num(r.get("gradesStrongSell")) or 0)
    buys = (num(r.get("gradesBuy")) or 0) + \
           (num(r.get("gradesStrongBuy")) or 0)
    if sells or buys:
        if sells > buys:
            parts.append(0.7)
            flags.append(f"Sell-side consensus is bearish "
                         f"({int(sells)} sell vs {int(buys)} buy ratings)")
    pt = num(r.get("priceTargetUpsidePct"))
    if pt is not None:
        parts.append(step(pt, [(-25, 0.9), (-10, 0.6), (0, 0.3),
                               (1e9, 0.0)]))
        if pt < 0:
            flags.append(f"Trades {abs(pt):.0f}% ABOVE the street's median "
                         f"price target")
    if not parts:
        return None
    return round(100.0 * sum(parts) / len(parts), 1)


def axis_valuation(r, flags):
    """Overvaluation risk, 0-100. A soft axis - expensive is not distressed."""
    parts = []
    dcf = num(r.get("dcfUpsidePct"))
    if dcf is not None:
        parts.append(step(dcf, [(-50, 1.0), (-25, 0.7), (-10, 0.4),
                                (0, 0.15), (1e9, 0.0)]))
        if dcf < -25:
            flags.append(f"DCF intrinsic value sits {abs(dcf):.0f}% below "
                         f"the share price")
    fpe = num(r.get("forwardPE"))
    if fpe is not None:
        if fpe < 0:
            parts.append(0.5)
        else:
            parts.append(step(fpe, [(25, 0.0), (40, 0.2),
                                    (60, 0.45), (1e9, 0.7)]))
            if fpe > 60:
                flags.append(f"Forward P/E {fpe:.0f} - priced for "
                             f"perfection")
    ev = num(r.get("evEbitda"))
    if ev is not None and ev > 30:
        parts.append(0.5)
    if not parts:
        return None
    return round(100.0 * sum(parts) / len(parts), 1)


def axis_momentum(r, flags):
    """Momentum breakdown / institutional distribution, 0-100."""
    parts = []
    price = num(r.get("price"))
    s50 = num(r.get("sma50"))
    s200 = num(r.get("sma200"))
    if price and s50 and s200:
        if price < s50 < s200:
            parts.append(0.8)
            flags.append("Below both the 50- and 200-day averages - "
                         "confirmed downtrend")
        elif price < s200:
            parts.append(0.45)
            flags.append("Trading below the 200-day average")
        else:
            parts.append(0.0)
    c6 = num(r.get("chg6m"))
    if c6 is not None:
        parts.append(step(c6, [(-30, 0.9), (-15, 0.6), (0, 0.3),
                               (1e9, 0.0)]))
        if c6 < -15:
            flags.append(f"Down {abs(c6):.0f}% over six months")
    c3 = num(r.get("chg3m"))
    if c3 is not None and c3 < -15:
        parts.append(0.5)
    ichg = num(r.get("instSharesChangePct"))
    if ichg is not None and ichg < -5:
        parts.append(0.5)
        flags.append(f"Institutions cut {abs(ichg):.1f}% of their holdings "
                     f"last quarter")
    inet = num(r.get("insiderNet90dUsd"))
    if inet is not None and inet < 0:
        parts.append(0.3)
        flags.append(f"Insiders net sellers ({usd(inet)}) over 90 days")
    if not parts:
        return None
    return round(100.0 * sum(parts) / len(parts), 1)


# ---------------------------------------------------------------- build --
MODE_LABEL = {
    "solvency": "SOLVENCY RISK", "earnings": "EARNINGS EROSION",
    "analyst": "ANALYST EXODUS", "momentum": "MOMENTUM BREAKDOWN",
    "valuation": "OVERVALUED",
}
BASE_W = {"solvency": 0.28, "earnings": 0.24, "analyst": 0.20,
          "momentum": 0.16, "valuation": 0.12}


def assess(r, short_pressure, value_traps):
    sector = r.get("sector") or "Unknown"
    no_solv = sector in NO_SOLVENCY_SECTORS
    flags = []

    ax = {
        "solvency": None if no_solv else axis_solvency(r, flags),
        "earnings": axis_earnings(r, flags),
        "analyst": axis_analyst(r, flags),
        "valuation": axis_valuation(r, flags),
        "momentum": axis_momentum(r, flags),
    }
    have = {k: v for k, v in ax.items() if v is not None}
    if len(have) < 2:
        return None

    # weighted deterioration score over the axes that have data
    wsum = sum(BASE_W[k] for k in have)
    score = round(sum(BASE_W[k] * have[k] for k in have) / wsum, 1)
    if score < CARRY_FLOOR:
        return None

    axes_triggered = sum(1 for v in have.values() if v >= 50)
    dom = max(have, key=have.get)
    failure_mode = MODE_LABEL[dom]

    solv = have.get("solvency", 0.0)
    earn = have.get("earnings", 0.0)
    hard_severe = solv >= 55 or earn >= 55

    if score >= 55 and axes_triggered >= 3 and hard_severe:
        tier = "SHORT CANDIDATE"
    elif score >= 45 and axes_triggered >= 2:
        tier = "AVOID"
    else:
        tier = "DETERIORATING"

    sym = r["symbol"].upper()
    shorts_building = bool(short_pressure.get(sym))
    trap_reason = value_traps.get(sym)
    if trap_reason:
        flags.append(f"Independently flagged a value trap by the asymmetric "
                     f"desk ({str(trap_reason).replace('_', ' ')})")
    if shorts_building:
        flags.append("Short-volume pressure is building above this name's "
                     "own 20-day norm - the tape is already positioning "
                     "against it")

    # thesis
    name = r.get("name") or sym
    if tier == "SHORT CANDIDATE":
        thesis = (f"{name} is breaking down on {axes_triggered} independent "
                  f"axes - {failure_mode.lower()} is the lead. This is a "
                  f"multi-dimensional fundamental deterioration, not a single "
                  f"weak metric.")
        if shorts_building:
            thesis += (" Short positioning is already building, which "
                       "confirms the fundamental read.")
    elif tier == "AVOID":
        thesis = (f"{name} carries real {failure_mode.lower()} risk and is "
                  f"best avoided on the long side. The breakdown is not yet "
                  f"broad enough to be a clean short - treat as a name to "
                  f"own only with a specific turnaround thesis.")
    else:
        thesis = (f"{name} shows early {failure_mode.lower()} - one axis is "
                  f"deteriorating. A watch-list flag, not yet an actionable "
                  f"short.")

    return {
        "symbol": sym, "name": name, "sector": sector,
        "price": num(r.get("price")), "market_cap": num(r.get("marketCap")),
        "deterioration_score": score,
        "failure_mode": failure_mode,
        "tier": tier,
        "axes_triggered": axes_triggered,
        "axis_scores": {k: round(v, 1) for k, v in have.items()},
        "solvency_excluded": no_solv,
        "shorts_building": shorts_building,
        "value_trap_confirmed": bool(trap_reason),
        "red_flags": flags[:7],
        "short_thesis": thesis,
        "caveat": ("No borrow cost or short-interest level is modelled here, "
                   "so squeeze risk is unquantified and shorting carries "
                   "unlimited risk. A deterioration screen flags danger - it "
                   "does not time entries, and some names are turnaround "
                   "candidates rather than permanent shorts."),
    }


def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)

    universe = load_universe()
    short_pressure = load_short_pressure()
    value_traps = load_value_traps()

    carried = []
    for r in universe:
        a = assess(r, short_pressure, value_traps)
        if a:
            carried.append(a)
    carried.sort(key=lambda x: x["deterioration_score"], reverse=True)
    carried = carried[:STACK_CAP]

    shorts = [c for c in carried if c["tier"] == "SHORT CANDIDATE"]
    avoid = [c for c in carried if c["tier"] == "AVOID"]
    by_mode = {}
    for c in carried:
        by_mode[c["failure_mode"]] = by_mode.get(c["failure_mode"], 0) + 1

    if shorts:
        lead = shorts[0]
        headline = (f"{len(carried)} S&P 500 names are deteriorating - "
                    f"{len(shorts)} qualify as genuine short candidates, "
                    f"{len(avoid)} more are simply best avoided. Worst "
                    f"breakdown: {lead['name']} ({lead['symbol']}), "
                    f"{lead['failure_mode'].lower()}.")
    elif carried:
        headline = (f"{len(carried)} S&P 500 names show fundamental "
                    f"deterioration, but none is a clean multi-axis short - "
                    f"the screen is an avoid list today, not a short book.")
    else:
        headline = ("No S&P 500 name clears the deterioration floor - "
                    "broad fundamental health across the index.")

    payload = {
        "schema_version": "1.0",
        "engine": "justhodl-risk-radar",
        "generated_at": now.isoformat(),
        "build_seconds": round(time.time() - t0, 2),
        "headline": headline,
        "universe_screened": len(universe),
        "n_carried": len(carried),
        "n_short_candidates": len(shorts),
        "n_avoid": len(avoid),
        "by_failure_mode": by_mode,
        "short_candidates": shorts,
        "avoid_list": avoid,
        "stack": carried,
        "methodology": (
            "Every S&P 500 name is scored on five orthogonal deterioration "
            "axes: solvency stress (Altman-Z, interest coverage, current "
            "ratio, free cash flow, leverage), earnings-quality erosion "
            "(Piotroski, margins, ROIC, shrinking top and bottom line, "
            "chronic misses), analyst exodus (downgrades, net rating "
            "momentum, sell-side consensus, price target gap), momentum "
            "breakdown (price vs moving averages, trailing returns, "
            "institutional and insider distribution) and overvaluation "
            "(DCF gap, forward multiples). Altman-Z and leverage axes are "
            "dropped for financials and real estate, where they are "
            "structurally meaningless, and the remaining weights are "
            "renormalised. A SHORT CANDIDATE breaks down on at least three "
            "axes with a severe solvency or earnings signal; an AVOID name "
            "carries real risk on two axes. Short-positioning data "
            "(short-pressure desk) and the asymmetric desk's value traps "
            "are cross-confirmation overlays."),
        "disclaimer": (
            "Research, not investment advice. Short selling carries "
            "unlimited risk; borrow cost and squeeze risk are not modelled. "
            "A deterioration screen identifies fundamental danger - it does "
            "not time entries or exits."),
    }

    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                  Body=json.dumps(payload, separators=(",", ":")).encode(),
                  ContentType="application/json",
                  CacheControl="max-age=300")
    return {"statusCode": 200,
            "body": json.dumps({"carried": len(carried),
                                "short_candidates": len(shorts),
                                "avoid": len(avoid)})}
