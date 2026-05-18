"""
justhodl-liquidity-capacity -- the firm Liquidity & Capacity Monitor.
=====================================================================
WHY THIS EXISTS
---------------
The Risk Monitor polices concentration and exposure. It does not answer
the other question every trading desk asks about its book: "if we had
to get OUT, how long does it take, and which positions are trapped?"
Liquidity risk is its own discipline. A 4% position is small on the
exposure report and lethal on the liquidity report if the name trades
$2m a day -- you cannot sell it without becoming the market.

Big desks (Millennium / Citadel / Point72) run a dedicated liquidity
function: every position is measured in DAYS-TO-LIQUIDATE at a sane
participation rate, the book is bucketed into liquidity tiers, and the
illiquid tail is watched because it is the part that cannot be cut in a
drawdown. Capacity is the same maths read forward -- how much capital a
name can absorb before the position is too big for its own volume.

THE METHOD
----------
  1. Read the consolidated firm book (justhodl-firm-book) -- every net
     equity position as a percent of capital.
  2. Pull live price and session volume for the whole US equity
     universe from the FMP screener in three bulk calls, and index the
     firm book's names against it -- dollar volume is the ADV proxy.
  3. Scale each position to dollars against a NOTIONAL book AUM (a
     clearly-labelled assumption -- liquidity is meaningless without a
     size) and compute days-to-liquidate = position$ / (dollar_ADV *
     participation cap). 20% of a name's daily volume is the most a desk
     can be without moving the print.
  4. Bucket each name into a liquidity TIER, roll the book up: the share
     liquidatable in one day / three days / a week, the dollar-weighted
     average days-to-exit, a 0-100 firm LIQUIDITY SCORE, the trapped
     tail, and the liquidity profile broken out by desk and by sector.
  5. Capacity: the trapped names are the capacity-constrained positions
     -- at this AUM the strategy is already too big for that name's
     volume; the engine sizes the comfortable position for each.

Reads the firm book sidecar + the FMP screener. Real data only. The
liquidity overlay above the firm book; pairs with the Risk Monitor.

OUTPUT   data/liquidity-capacity.json          SCHEDULE  daily 02:00 UTC
"""
import json
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import boto3

S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/liquidity-capacity.json"
FIRM_KEY = "data/firm-book.json"
SCHEMA = "1.0"

s3 = boto3.client("s3", region_name="us-east-1")

FMP_KEY = os.environ.get("FMP_KEY") or "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
FMP = "https://financialmodelingprep.com/stable"
HTTP_TIMEOUT = 50

# ---- model assumptions (clearly labelled - liquidity needs a $ size) -------
NOTIONAL_AUM = 50_000_000.0    # notional book AUM the % weights scale to
PARTICIPATION_CAP = 0.20       # max share of a name's daily volume per day
ADV_FLOOR = 5.0e5              # floor on dollar ADV so micro names do not
#                                divide by ~zero - treated as very illiquid
BUILD_DAYS = 1.0               # comfortable position = 1 day at the cap

# liquidity tiers, in days-to-liquidate
TIER_T1 = 0.5    # highly liquid - out within half a session
TIER_T2 = 2.0    # liquid - a couple of days
TIER_T3 = 5.0    # moderate - up to a week
#                  above TIER_T3 -> illiquid / trapped tail


def http_json(url, timeout=HTTP_TIMEOUT):
    req = urllib.request.Request(url, headers={"User-Agent": "justhodl/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8", "ignore"))


def num(v):
    try:
        f = float(v)
        return f if f == f else None
    except (TypeError, ValueError):
        return None


def get_json(key):
    try:
        return json.loads(s3.get_object(
            Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def fetch_volume_universe():
    """Live price + session volume for the US equity universe, FMP screener.

    Returns {symbol: {price, volume, dollar_vol}}. Three bulk calls; the
    screener's volume is the latest session, used as an ADV proxy.
    """
    rows = {}
    for exch in ("NYSE", "NASDAQ", "AMEX"):
        qs = urllib.parse.urlencode({
            "isEtf": "false", "isFund": "false",
            "isActivelyTrading": "true", "country": "US",
            "exchange": exch, "limit": 7000, "apikey": FMP_KEY})
        try:
            data = http_json(f"{FMP}/company-screener?{qs}", timeout=50)
        except Exception:
            data = []
        for r in data or []:
            sym = (r.get("symbol") or "").upper().strip()
            price = num(r.get("price"))
            vol = num(r.get("volume"))
            if not sym or price is None or price <= 0:
                continue
            dv = price * (vol or 0.0)
            rows[sym] = {"price": price, "volume": vol or 0.0,
                         "dollar_vol": dv}
    return rows


def tier_of(days):
    if days is None:
        return "UNKNOWN"
    if days <= TIER_T1:
        return "T1 HIGHLY LIQUID"
    if days <= TIER_T2:
        return "T2 LIQUID"
    if days <= TIER_T3:
        return "T3 MODERATE"
    return "T4 ILLIQUID"


def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)

    firm = get_json(FIRM_KEY)
    if not firm or not firm.get("equity_book"):
        out = {"schema_version": SCHEMA,
               "engine": "justhodl-liquidity-capacity",
               "generated_at": now.isoformat(), "ok": False,
               "error": "firm-book.json unavailable or empty - cannot "
                        "measure book liquidity without the consolidated "
                        "book"}
        s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                      Body=json.dumps(out).encode("utf-8"),
                      ContentType="application/json")
        return {"statusCode": 200,
                "body": json.dumps({"ok": False, "reason": "no firm-book"})}

    equity_book = firm.get("equity_book") or []
    vol_map = fetch_volume_universe()

    positions = []
    n_unknown = 0
    for b in equity_book:
        sym = (b.get("symbol") or "").upper().strip()
        net = num(b.get("net_pct"))
        if not sym or net is None or abs(net) < 1e-6:
            continue
        pos_usd = abs(net) / 100.0 * NOTIONAL_AUM
        vinfo = vol_map.get(sym)
        # price preference: firm book price, else screener price
        price = num(b.get("price")) or (vinfo or {}).get("price")
        dollar_vol = (vinfo or {}).get("dollar_vol")
        if dollar_vol is None or dollar_vol <= 0:
            days = None
            n_unknown += 1
        else:
            capacity_per_day = max(dollar_vol, ADV_FLOOR) * PARTICIPATION_CAP
            days = pos_usd / capacity_per_day if capacity_per_day > 0 else None
        comfortable = (max(dollar_vol or 0.0, ADV_FLOOR)
                       * PARTICIPATION_CAP * BUILD_DAYS)
        over_x = (pos_usd / comfortable) if comfortable > 0 else None
        positions.append({
            "symbol": sym,
            "name": b.get("name"),
            "sector": b.get("sector") or "Unknown",
            "side": b.get("side"),
            "net_pct": round(net, 4),
            "position_usd": round(pos_usd, 0),
            "dollar_volume_usd": (round(dollar_vol, 0)
                                  if dollar_vol is not None else None),
            "days_to_liquidate": (round(days, 2)
                                  if days is not None else None),
            "liquidity_tier": tier_of(days),
            "comfortable_position_usd": round(comfortable, 0),
            "size_vs_comfortable_x": (round(over_x, 2)
                                      if over_x is not None else None),
            "desks": list((b.get("desks") or {}).keys()),
        })

    measured = [p for p in positions if p["days_to_liquidate"] is not None]
    book_usd = sum(p["position_usd"] for p in positions)
    measured_usd = sum(p["position_usd"] for p in measured)

    # ---- firm-level liquidity roll-up ----
    def liquidatable_within(day_bar):
        got = sum(p["position_usd"] for p in measured
                  if p["days_to_liquidate"] <= day_bar)
        return (100.0 * got / measured_usd) if measured_usd > 0 else 0.0

    pct_1d = liquidatable_within(1.0)
    pct_3d = liquidatable_within(3.0)
    pct_5d = liquidatable_within(5.0)

    wavg_days = (sum(p["position_usd"] * p["days_to_liquidate"]
                     for p in measured) / measured_usd
                 if measured_usd > 0 else None)

    tiers = {}
    for p in positions:
        t = p["liquidity_tier"]
        e = tiers.setdefault(t, {"tier": t, "n": 0, "book_usd": 0.0})
        e["n"] += 1
        e["book_usd"] += p["position_usd"]
    tier_rows = sorted(tiers.values(), key=lambda x: x["tier"])
    for e in tier_rows:
        e["book_usd"] = round(e["book_usd"], 0)
        e["book_pct"] = round(100.0 * e["book_usd"] / book_usd, 2) \
            if book_usd > 0 else 0.0

    trapped = sorted([p for p in measured
                      if p["liquidity_tier"] == "T4 ILLIQUID"],
                     key=lambda p: -p["days_to_liquidate"])

    # liquidity score 0-100: most weight on same-day liquidatability,
    # the rest on a low weighted-average days-to-exit
    if wavg_days is not None:
        days_term = max(0.0, min(1.0, 1.0 - wavg_days / 5.0))
        score = round(100.0 * (0.60 * pct_1d / 100.0 + 0.40 * days_term), 1)
    else:
        score = None

    # ---- by desk ----
    desk_agg = {}
    for p in measured:
        for dk in p["desks"]:
            e = desk_agg.setdefault(dk, {"desk": dk, "n": 0, "usd": 0.0,
                                         "wsum": 0.0, "trapped": 0})
            e["n"] += 1
            e["usd"] += p["position_usd"]
            e["wsum"] += p["position_usd"] * p["days_to_liquidate"]
            if p["liquidity_tier"] == "T4 ILLIQUID":
                e["trapped"] += 1
    desk_rows = []
    for e in desk_agg.values():
        desk_rows.append({
            "desk": e["desk"], "n_names": e["n"],
            "avg_days_to_liquidate": round(e["wsum"] / e["usd"], 2)
            if e["usd"] > 0 else None,
            "trapped_names": e["trapped"]})
    desk_rows.sort(key=lambda x: -(x["avg_days_to_liquidate"] or 0))

    # ---- by sector ----
    sec_agg = {}
    for p in measured:
        e = sec_agg.setdefault(p["sector"],
                               {"sector": p["sector"], "n": 0, "usd": 0.0,
                                "wsum": 0.0})
        e["n"] += 1
        e["usd"] += p["position_usd"]
        e["wsum"] += p["position_usd"] * p["days_to_liquidate"]
    sector_rows = []
    for e in sec_agg.values():
        sector_rows.append({
            "sector": e["sector"], "n_names": e["n"],
            "book_pct": round(100.0 * e["usd"] / measured_usd, 2)
            if measured_usd > 0 else 0.0,
            "avg_days_to_liquidate": round(e["wsum"] / e["usd"], 2)
            if e["usd"] > 0 else None})
    sector_rows.sort(key=lambda x: -(x["avg_days_to_liquidate"] or 0))

    # ---- least-liquid names (the tail the desk watches) ----
    least_liquid = sorted(measured,
                          key=lambda p: -p["days_to_liquidate"])[:25]

    if score is None:
        posture = "UNKNOWN"
    elif score >= 75:
        posture = "LIQUID"
    elif score >= 50:
        posture = "MODERATE"
    elif score >= 30:
        posture = "TIGHT"
    else:
        posture = "ILLIQUID"

    headline = (
        "Firm book liquidity %s - score %s/100. %.0f%% of the book clears "
        "in one day, %.0f%% within a week; weighted-average exit %s days. "
        "%d trapped name(s) in the illiquid tail."
        % (posture, score if score is not None else "n/a", pct_1d, pct_5d,
           round(wavg_days, 1) if wavg_days is not None else "n/a",
           len(trapped)))

    payload = {
        "schema_version": SCHEMA,
        "engine": "justhodl-liquidity-capacity",
        "generated_at": now.isoformat(),
        "build_seconds": round(time.time() - t0, 2),
        "headline": headline,
        "liquidity_posture": posture,
        "liquidity_score": score,
        "firm": {
            "notional_aum_usd": NOTIONAL_AUM,
            "n_equity_names": len(positions),
            "n_measured": len(measured),
            "n_unknown_volume": n_unknown,
            "book_usd": round(book_usd, 0),
            "pct_liquidatable_1d": round(pct_1d, 2),
            "pct_liquidatable_3d": round(pct_3d, 2),
            "pct_liquidatable_5d": round(pct_5d, 2),
            "wavg_days_to_liquidate": (round(wavg_days, 2)
                                       if wavg_days is not None else None),
            "n_trapped": len(trapped),
            "trapped_book_pct": round(
                100.0 * sum(p["position_usd"] for p in trapped) / book_usd, 2)
            if book_usd > 0 else 0.0,
        },
        "tiers": tier_rows,
        "least_liquid_names": least_liquid,
        "trapped_names": trapped,
        "by_desk": desk_rows,
        "by_sector": sector_rows,
        "parameters": {
            "notional_aum_usd": NOTIONAL_AUM,
            "participation_cap_pct": PARTICIPATION_CAP * 100,
            "tier_breaks_days": {"T1": TIER_T1, "T2": TIER_T2,
                                 "T3": TIER_T3},
        },
        "how_to_read": (
            "Days-to-liquidate is how long it takes to exit a position "
            "without trading more than 20% of the name's daily volume - "
            "the rate above which a desk starts moving the price against "
            "itself. T1 names clear within half a session; T4 names take "
            "more than a week and are the trapped tail - the part of the "
            "book that cannot be cut quickly in a drawdown. The liquidity "
            "score weights same-day liquidatability and a low average "
            "exit. Capacity is the same maths forward: a name flagged "
            "well above its comfortable size is one the strategy has "
            "outgrown at this book size."),
        "methodology": (
            "Positions come from the consolidated firm book as a percent "
            "of capital and are scaled to dollars against a notional "
            "%s book AUM - a labelled assumption, since liquidity is "
            "undefined without a size. Dollar volume is the FMP screener's "
            "latest session price * volume, used as an ADV proxy; a single "
            "session is noisier than a trailing average and a spike day "
            "overstates liquidity. Days-to-liquidate = position$ / "
            "(dollar_ADV * 20%%). Short positions are measured on "
            "magnitude and do not model securities-borrow constraints, "
            "which make a real short harder to cover than this shows."
            % f"${NOTIONAL_AUM/1e6:.0f}m"),
        "disclaimer": (
            "Research and education only - not investment advice. The "
            "AUM and participation assumptions are a risk-budgeting "
            "framework applied to the model desk book, not the user's "
            "actual portfolio."),
    }

    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                  Body=json.dumps(payload, default=str).encode("utf-8"),
                  ContentType="application/json", CacheControl="max-age=300")

    return {"statusCode": 200,
            "body": json.dumps({"ok": True, "liquidity_posture": posture,
                                "liquidity_score": score,
                                "n_measured": len(measured),
                                "n_trapped": len(trapped)})}
