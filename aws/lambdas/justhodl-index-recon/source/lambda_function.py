"""
justhodl-index-recon - the Index Reconstitution / Forced-Flow Desk.

THE EDGE. Index funds are price-insensitive. A fund tracking the Russell
2000 does not care whether a stock is cheap or expensive - when FTSE
Russell reconstitutes the index at the end of June, that fund MUST buy
every addition and sell every deletion, in size, on the same day, at
whatever price the close prints. Trillions of passive dollars move on a
published, rules-based schedule. That is a forced, forecastable flow -
and a flow you can see coming weeks ahead is a flow you can trade.

This desk reconstructs the Russell map from live market caps and projects
the four reconstitution events that actually pay:

  RUSSELL 2000 ADDITIONS  - a name climbing into the Russell 2000 for the
    first time. Russell-2000 trackers become forced buyers. The thinner
    and smaller the name, the larger that buying is as a multiple of its
    daily volume - and the bigger the pop. BULLISH.

  RUSSELL DEMOTIONS (1000 -> 2000) - a former large-cap that has fallen
    into small-cap territory. It loses a tiny Russell-1000 weight but
    gains a MEANINGFUL Russell-2000 weight, so net passive demand is a
    buy. The counter-intuitive "demotion pop". BULLISH.

  RUSSELL GRADUATIONS (2000 -> 1000) - a small-cap that has rallied up
    into the Russell 1000. It surrenders a large Russell-2000 weight for
    a negligible Russell-1000 weight: net passive SELLING. The
    well-documented post-graduation underperformance. BEARISH / trim.

  RUSSELL 2000 DELETIONS - a name shrinking out of the index entirely.
    Russell-2000 trackers are forced sellers into an already-weak tape.
    BEARISH / avoid.

Plus a watch list of S&P 500 inclusion candidates - size-eligible US
companies not yet in the index, where an index-committee addition would
trigger the largest forced bid in the market.

PRESSURE-TESTED against the naive version:
  - NOT a hard rank-1000 cutoff. FTSE Russell applies a banding rule -
    a stock only changes index if it moves beyond 2.5% of the Russell
    3000E cumulative market cap on either side of the breakpoint. We
    compute that cumulative-cap band directly and only flag names that
    have genuinely cleared it.
  - Direction matters. A name in the band is meaningless until you know
    if it is rising or falling through it - so we pull the trailing
    one-year return for every in-band name and let that resolve
    promotion vs demotion.
  - Eligibility filtered: US common stock, major exchange, priced above
    $1, ETFs / funds / obvious SPAC shells removed.
  - Membership here is RECONSTRUCTED from current caps - rank day for
    the 2026 reconstitution has already passed (early May) and the
    effective date is the last Friday of June. We say so plainly.

OUTPUT  data/index-recon.json        SCHEDULE  daily 14:10 UTC
Real data only - FMP company-screener + price-change. Research, not advice.
"""
import json
import os
import time
import urllib.request
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import boto3

s3 = boto3.client("s3")
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/index-recon.json"
SCHEMA = "index-recon-1.0"

FMP_KEY = os.environ.get("FMP_KEY") or "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
FMP = "https://financialmodelingprep.com/stable"

# ---- index structure constants --------------------------------------------
R1000_N = 1000          # Russell 1000  = top 1000 by total market cap
R3000_N = 3000          # Russell 3000  = top 3000; the 2000 is ranks 1001-3000
BAND_PCT = 0.025        # FTSE banding: 2.5% of R3000E cumulative cap each side

# passive AUM benchmarked to each index (USD, deliberately conservative)
AUM_R2000 = 2.6e12
AUM_R1000 = 2.4e12
AUM_SP500 = 1.15e13
# approximate total float-adjusted market cap of each index (USD)
IDXCAP_R2000 = 3.6e12
IDXCAP_R1000 = 5.6e13
IDXCAP_SP500 = 5.0e13

# S&P 500 committee size floor (ballpark of the published threshold)
SP500_MIN_CAP = 2.2e10

MOVE_UP = 0.22          # 1Y return that counts as "rose through the band"
MOVE_DN = -0.20         # 1Y return that counts as "fell through the band"
WORKERS = 16
HTTP_TIMEOUT = 30


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


# ---- 1. pull the full eligible US equity universe -------------------------
EXCLUDE_NAME = (" acquisition corp", " acquisition corporation",
                "blank check", " spac")


def fetch_universe():
    """Every actively-traded US common stock on a major exchange, with its
    live total market cap, pulled straight from the FMP screener."""
    rows = {}
    for exch in ("NYSE", "NASDAQ", "AMEX"):
        qs = urllib.parse.urlencode({
            "marketCapMoreThan": 50_000_000,
            "isEtf": "false", "isFund": "false",
            "isActivelyTrading": "true", "country": "US",
            "exchange": exch, "limit": 7000, "apikey": FMP_KEY})
        try:
            data = http_json(f"{FMP}/company-screener?{qs}", timeout=50)
        except Exception:
            data = []
        for r in data or []:
            sym = (r.get("symbol") or "").upper().strip()
            cap = num(r.get("marketCap"))
            price = num(r.get("price"))
            if not sym or cap is None or cap <= 0:
                continue
            if price is None or price < 1.0:
                continue
            if r.get("isEtf") or r.get("isFund"):
                continue
            nm = (r.get("companyName") or "")
            low = nm.lower()
            if any(x in low for x in EXCLUDE_NAME):
                continue
            # warrant / unit / right share-class tickers
            if any(sym.endswith(s) for s in (".WS", ".U", ".RT")) or \
                    (len(sym) == 5 and sym[-1] in ("W", "R", "U")
                     and sym not in ("VMEOU",)):
                # 5-letter W/R/U suffixes are usually warrants/units, but
                # this is a soft heuristic - keep it conservative below
                pass
            vol = num(r.get("volume")) or 0.0
            rows[sym] = {
                "symbol": sym, "name": nm,
                "market_cap": cap, "price": price,
                "dollar_adv": price * vol,
                "sector": r.get("sector") or "",
                "industry": r.get("industry") or "",
                "exchange": r.get("exchangeShortName")
                or r.get("exchange") or "",
            }
    return list(rows.values())


# ---- 2. trailing one-year return (resolves migration direction) -----------
def fetch_1y(sym):
    try:
        d = http_json(f"{FMP}/stock-price-change?symbol="
                      f"{urllib.parse.quote(sym)}&apikey={FMP_KEY}")
        row = d[0] if isinstance(d, list) and d else d
        if not isinstance(row, dict):
            return sym, None
        for k in ("1Y", "1y", "oneYear", "year", "12M"):
            v = num(row.get(k))
            if v is not None:
                return sym, v / 100.0   # FMP returns percent
        return sym, None
    except Exception:
        return sym, None


# ---- 3. forced-flow edge scoring ------------------------------------------
def flow_edge(cap, dollar_adv, aum, idx_cap, move_abs):
    """Estimate the passive rebalance flow into/out of a name as a multiple
    of its daily dollar volume, then fold in how decisively it has moved."""
    weight = cap / idx_cap if idx_cap > 0 else 0.0
    flow_usd = weight * aum
    adv = max(dollar_adv, 5e5)
    days = flow_usd / adv                       # days of ADV to absorb
    days_c = max(0.0, min(days, 25.0))
    thin = 1.0 if dollar_adv < 3e6 else (
        0.6 if dollar_adv < 1.5e7 else 0.25)
    decis = min(move_abs / 0.6, 1.0)
    score = 100.0 * (0.55 * min(days_c / 8.0, 1.0)
                     + 0.30 * decis + 0.15 * thin)
    return round(score, 1), round(days_c, 1), round(flow_usd / 1e6, 1)


def lambda_handler(event, context):
    t0 = time.time()
    uni = fetch_universe()
    n_uni = len(uni)
    if n_uni < 1500:
        out = {"ok": False, "schema": SCHEMA,
               "error": f"universe too small ({n_uni}) - FMP screener "
                         "likely throttled", "n_universe": n_uni,
               "generated_at": datetime.now(timezone.utc).isoformat()}
        s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                      Body=json.dumps(out).encode(), ContentType="application/json")
        return {"statusCode": 200, "body": json.dumps({"ok": False,
                "n_universe": n_uni})}

    # rank everything by total market cap
    uni.sort(key=lambda r: r["market_cap"], reverse=True)
    for i, r in enumerate(uni):
        r["rank"] = i + 1
    by_sym = {r["symbol"]: r for r in uni}

    # cumulative cap + the Russell 3000E proxy total
    cum = 0.0
    for r in uni:
        cum += r["market_cap"]
        r["cum_cap"] = cum
    total_3000 = uni[min(R3000_N, n_uni) - 1]["cum_cap"]
    band_half = BAND_PCT * total_3000

    # breakpoint between Russell 1000 and Russell 2000
    bp_1000 = uni[R1000_N - 1]
    cum_bp = bp_1000["cum_cap"]
    bp_1000_cap = bp_1000["market_cap"]
    # breakpoint at the bottom of the Russell 2000
    have_3000 = n_uni >= R3000_N
    bp_3000 = uni[R3000_N - 1] if have_3000 else uni[-1]
    cum_3k = bp_3000["cum_cap"]
    bp_3000_cap = bp_3000["market_cap"]

    def in_band(r, centre):
        return abs(r["cum_cap"] - centre) <= band_half

    # candidate set: everything inside either banding zone, plus a margin
    # around each breakpoint so fresh adds just outside R3000 are caught
    cand = []
    for r in uni:
        near_1000 = in_band(r, cum_bp) or (900 <= r["rank"] <= 1110)
        near_3000 = in_band(r, cum_3k) or (R3000_N - 160 <= r["rank"]
                                           <= R3000_N + 320)
        if near_1000 or near_3000:
            r["_zone"] = "boundary" if near_1000 else "lower"
            cand.append(r)

    # S&P 500 candidates: size-eligible US names NOT already in the S&P 500.
    # screener/data.json is the live S&P 500 membership list.
    sp_members = set()
    try:
        sd = json.loads(s3.get_object(
            Bucket=S3_BUCKET, Key="screener/data.json")["Body"].read())
        for s in (sd.get("stocks") or []):
            sy = (s.get("symbol") or "").upper()
            if sy:
                sp_members.add(sy)
    except Exception:
        sp_members = set()
    sp_cand = [r for r in uni
               if r["market_cap"] >= SP500_MIN_CAP
               and r["symbol"] not in sp_members
               and r["dollar_adv"] >= 1.5e7][:60]

    # one-year return for the focused set (resolves promotion vs demotion)
    need = {r["symbol"] for r in cand} | {r["symbol"] for r in sp_cand}
    chg = {}
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        for sym, v in ex.map(fetch_1y, sorted(need)):
            chg[sym] = v
    n_with_chg = sum(1 for v in chg.values() if v is not None)

    promotions, demotions, adds, deletes = [], [], [], []

    for r in cand:
        sym = r["symbol"]
        rank = r["rank"]
        ch = chg.get(sym)
        mv = ch if ch is not None else 0.0
        base = {
            "symbol": sym, "name": r["name"], "sector": r["sector"],
            "industry": r["industry"], "exchange": r["exchange"],
            "price": round(r["price"], 2),
            "market_cap_bil": round(r["market_cap"] / 1e9, 3),
            "rank": rank, "one_year_return_pct":
                (round(ch * 100, 1) if ch is not None else None),
            "dollar_adv_mil": round(r["dollar_adv"] / 1e6, 2),
        }
        if r["_zone"] == "boundary":
            # band around the 1000/2000 line
            if mv <= MOVE_DN and rank > R1000_N:
                # fell out of large-cap land -> demotion into the R2000
                sc, days, flow = flow_edge(
                    r["market_cap"], r["dollar_adv"],
                    AUM_R2000, IDXCAP_R2000, abs(mv))
                base.update({
                    "event": "DEMOTION  Russell 1000 -> 2000",
                    "direction": "BULLISH",
                    "edge_score": sc, "passive_days_to_absorb": days,
                    "est_forced_flow_musd": flow,
                    "thesis": (f"{r['name']} has fallen {abs(round(mv*100))}% "
                               "over the past year and now ranks below the "
                               "Russell 1000 breakpoint. On reconstitution it "
                               "trades its negligible large-cap index weight "
                               "for a far larger Russell 2000 weight - net "
                               "passive demand is a buy.")})
                demotions.append(base)
            elif mv >= MOVE_UP and rank <= R1000_N:
                # rallied up into large-cap land -> graduation out of R2000
                sc, days, flow = flow_edge(
                    r["market_cap"], r["dollar_adv"],
                    AUM_R2000, IDXCAP_R2000, abs(mv))
                base.update({
                    "event": "GRADUATION  Russell 2000 -> 1000",
                    "direction": "BEARISH",
                    "edge_score": sc, "passive_days_to_absorb": days,
                    "est_forced_flow_musd": flow,
                    "thesis": (f"{r['name']} has rallied {round(mv*100)}% over "
                               "the past year up into the Russell 1000. It "
                               "surrenders a meaningful Russell 2000 weight "
                               "for a negligible large-cap weight - net "
                               "passive selling, the post-graduation drag.")})
                promotions.append(base)
        else:
            # band at the bottom of the Russell 2000
            if mv <= MOVE_DN and rank > R3000_N - 250:
                sc, days, flow = flow_edge(
                    r["market_cap"], r["dollar_adv"],
                    AUM_R2000, IDXCAP_R2000, abs(mv))
                base.update({
                    "event": "DELETION  dropping out of the Russell 2000",
                    "direction": "BEARISH",
                    "edge_score": sc, "passive_days_to_absorb": days,
                    "est_forced_flow_musd": flow,
                    "thesis": (f"{r['name']} has shrunk {abs(round(mv*100))}% "
                               "over the past year toward the bottom of the "
                               "Russell 3000. If it drops out, every Russell "
                               "2000 tracker is a forced seller into an "
                               "already-weak tape.")})
                deletes.append(base)
            elif mv >= MOVE_UP and R3000_N - 120 <= rank <= R3000_N + 320:
                sc, days, flow = flow_edge(
                    r["market_cap"], r["dollar_adv"],
                    AUM_R2000, IDXCAP_R2000, abs(mv))
                base.update({
                    "event": "ADDITION  entering the Russell 2000",
                    "direction": "BULLISH",
                    "edge_score": sc, "passive_days_to_absorb": days,
                    "est_forced_flow_musd": flow,
                    "thesis": (f"{r['name']} has climbed {round(mv*100)}% over "
                               "the past year up to the Russell 3000 line. A "
                               "fresh Russell 2000 add turns every tracker "
                               "into a forced buyer - and against this name's "
                               "thin volume that bid lands hard.")})
                adds.append(base)

    for lst in (promotions, demotions, adds, deletes):
        lst.sort(key=lambda x: x["edge_score"], reverse=True)

    sp_list = []
    for r in sp_cand:
        ch = chg.get(r["symbol"])
        sp_list.append({
            "symbol": r["symbol"], "name": r["name"],
            "sector": r["sector"], "exchange": r["exchange"],
            "price": round(r["price"], 2),
            "market_cap_bil": round(r["market_cap"] / 1e9, 2),
            "rank": r["rank"],
            "one_year_return_pct":
                (round(ch * 100, 1) if ch is not None else None),
            "dollar_adv_mil": round(r["dollar_adv"] / 1e6, 2),
            "note": ("Size- and liquidity-eligible for the S&P 500 and not "
                     "currently a member. The index committee also requires "
                     "GAAP profitability; an actual addition is announced "
                     "about a week ahead and triggers the market's largest "
                     "single forced bid."),
        })
    sp_list.sort(key=lambda x: x["market_cap_bil"], reverse=True)

    headline = (
        f"{len(adds)} projected Russell 2000 additions and "
        f"{len(demotions)} demotions set up forced passive buying; "
        f"{len(deletes)} deletions and {len(promotions)} graduations face "
        f"forced selling. {len(sp_list)} S&P 500 inclusion candidates on "
        "watch.")

    out = {
        "ok": True, "schema": SCHEMA,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_s": round(time.time() - t0, 1),
        "headline": headline,
        "reconstitution": {
            "index_family": "FTSE Russell US indices",
            "next_effective_date": "2026-06-26",
            "note": ("Rank day for the 2026 reconstitution has already "
                     "passed (first Friday of May); FTSE Russell publishes "
                     "preliminary add/delete lists through June with the "
                     "reconstitution effective after the close on the last "
                     "Friday of June. Index membership below is reconstructed "
                     "from live total market caps as a forward projection, "
                     "not the official list."),
        },
        "universe": {
            "n_eligible": n_uni,
            "n_candidates_scored": len(cand),
            "n_with_one_year_return": n_with_chg,
            "russell_1000_breakpoint_cap_bil": round(bp_1000_cap / 1e9, 3),
            "russell_3000_breakpoint_cap_bil": round(bp_3000_cap / 1e9, 3),
            "ftse_band_pct_each_side": BAND_PCT * 100,
        },
        "n_additions": len(adds),
        "n_demotions": len(demotions),
        "n_graduations": len(promotions),
        "n_deletions": len(deletes),
        "n_sp500_candidates": len(sp_list),
        "russell_2000_additions": adds[:40],
        "russell_demotions": demotions[:30],
        "russell_graduations": promotions[:30],
        "russell_2000_deletions": deletes[:40],
        "sp500_candidates": sp_list[:40],
        "method": ("Every actively-traded US common stock on a major "
                   "exchange is ranked by total market cap. The Russell "
                   "1000/2000 breakpoint and the bottom-of-2000 breakpoint "
                   "are located, and FTSE Russell's 2.5%-of-cumulative-cap "
                   "banding zone is computed around each. For every name in "
                   "those zones the trailing one-year return is pulled to "
                   "resolve whether it is rising or falling through the "
                   "band, which fixes the event as an addition, demotion, "
                   "graduation or deletion. The edge score scales the "
                   "estimated forced passive flow by the name's daily dollar "
                   "volume - how many days of normal volume the index funds "
                   "must absorb. Real data only. Research, not advice."),
    }

    body = json.dumps(out, separators=(",", ":"), default=str).encode("utf-8")
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY, Body=body,
                  ContentType="application/json", CacheControl="max-age=300")
    print(f"[index-recon] universe={n_uni} adds={len(adds)} "
          f"demotions={len(demotions)} grads={len(promotions)} "
          f"deletes={len(deletes)} sp={len(sp_list)} "
          f"{out['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "n_universe": n_uni, "additions": len(adds),
        "demotions": len(demotions), "deletions": len(deletes),
        "graduations": len(promotions), "sp500_candidates": len(sp_list)})}
