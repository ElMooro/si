"""
justhodl-regime-map - THE RISK MAP: what is booming and what is getting destroyed, across every asset class
============================================================================================================
A single risk-on / risk-off number hides the most important thing about a tape: DISPERSION. A multi-strategy desk
reads the cross-asset MAP: which sleeves are ripping, which are bleeding, and whether a rally is BROAD, NARROW
(megacap-led) or BIFURCATED (equities up, crypto down).

2.0.0 (2026-11-24, Khalid: "improve this engine so that it covers all asset classes ... make sure whats booming is
actually booming and whats destroyed is actually destroyed"):

  * every asset class - US stocks by size, style, sector, industry and theme; world stocks by region and country;
    bonds; currencies; commodities; crypto; real estate; volatility; alternatives - about 300 instruments
    (risk_universe.py), all read from the site's own warehouse: the Polygon EOD bank and today's forming bars for
    funds, the banked crypto bars, the FMP price bank for currencies, front-month futures and indexes. No provider is
    asked for anything and no key is read (1.0.0 asked Polygon for each of its 38 instruments).
  * windows by the calendar (risk_math.py): 1.0.0 counted bars, so a coin's "six months" was 126 days.
  * a split the bank still holds unadjusted is rebased before anything is measured (leaders_tape.py, the leaders
    engine's file, byte for byte).
  * BOOMING and DESTROYED are held to the plain record (up or down over one and three months, by 5% or more over
    one of them, on the right side of both averages); every row is checked against itself before publication; the two lists hold only instruments in
    that state - 1.0.0 listed its six highest and six lowest scores whatever they were.

What 1.0.0 published is still published under the same names, for the same 38 instruments and by the same regime
rule, because other engines read it: regime, sleeves, ranked, booming, destroyed, transitions. The whole map is added
beside it under `map`, with `cross_asset`, `as_of`, `strongest` / `weakest`.

Output: data/regime-map.json -> regime-map.html (and the chart's Risk Map button). Schedule: once a day, 21:00 UTC.
"""
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
import gzip
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import boto3  # noqa: E402

import leaders_tape as T  # noqa: E402
import risk_math as M  # noqa: E402
import risk_universe as U  # noqa: E402

VERSION = "2.0.0"
CONTRACT = "risk-map.v2"
REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY, PREV_KEY = "data/regime-map.json", "data/regime-map-prev.json"
EOD_ROOT = "data/warm/polygon-full/eod/"
SPLITS_ROOT = "data/warm/polygon-bank/splits/"
TODAY_KEY = "data/warm/polygon-full/today.json.gz"
COIN_ROOT = "data/warm/katlin/crypto-bars/"
FMP_ROOT = "data/warm/fmp/historical-price-eod-full/"
CMC_ROOT = "data/warm/provider-bank/cmc/listings-latest/daily/"

FUND_BARS = 420                 # sessions read back: a year for the longest window, 252 for the usual move, 200 for the average
SERIES_DAYS = 800               # calendar days of a coin / currency / futures / index history kept
MIN_DOLLAR_VOLUME = 250_000.0   # a fund trading under this a day (20 sessions) is left out: its closes are not a market
STALE_DAYS = 7                  # an instrument with no bar for longer than this behind its kind's newest is left out
WAREHOUSE_LATE_DAYS = 5         # a whole kind whose newest bar is older than this at the run is said so: the map is dated, never silently old
KIND_WORDS = {"fund": "funds", "coin": "coins", "fx": "currencies", "fut": "futures", "idx": "indexes"}
FORMING_BAND = (0.56, 1.8)      # a forming bar outside this multiple of the last close is a split in progress or a bad print: not used
JUMP_WINDOW = 130               # bars: an unexplained one-day move inside it keeps a row out of BOOMING / DESTROYED
JUMP_LIMIT = {"fx": 0.12, "fut": 0.35, "idx": 0.35}       # |one-day change| past this is not believed for these kinds
VOL_INDEXES = ("VIX", "VVIX", "MOVE")
MIN_LEGACY, MIN_SHARE = 26, 0.6
THREADS = 32


# ----------------------------------------------------------------------------------------------------------------- s3
def _s3():
    return boto3.client("s3", REGION)


def missing(exc):
    code = str(((getattr(exc, "response", None) or {}).get("Error") or {}).get("Code") or "")
    return type(exc).__name__ in ("NoSuchKey", "KeyError") or code in ("NoSuchKey", "404", "NotFound") or "NoSuchKey" in str(exc)


def get_json(s3, key):
    raw = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    if key.endswith(".gz") or raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def optional(s3, key):
    try:
        return get_json(s3, key), None
    except Exception as exc:  # noqa: BLE001
        return None, ("not there" if missing(exc) else "%s: %s" % (type(exc).__name__, str(exc)[:120]))


def bank_key(ticker):
    return EOD_ROOT + "".join(ch if (ch.isalnum() or ch in "._-^") else "_" for ch in str(ticker).upper())[:80] + ".json.gz"


def fmp_key(symbol):
    return FMP_ROOT + "".join(ch if (ch.isalnum() or ch in "._-^") else "_" for ch in str(symbol)) + ".json.gz"


# -------------------------------------------------------------------------------------------------------------- readers
def fund_bars(doc):
    """{date, o, h, l, c, v} oldest first from one EOD bank document. Pure."""
    out = []
    for r in (doc or {}).get("rows") or []:
        if not isinstance(r, list) or len(r) < 6 or r[0] is None or r[4] is None:
            continue
        b = {k: v for k, v in zip(("o", "h", "l", "c", "v"), r[1:6]) if v is not None}
        b["date"] = datetime.fromtimestamp(r[0] / 1000.0, tz=timezone.utc).date().isoformat()
        out.append(b)
    return out


def months_between(first_iso, last_iso):
    y, m = int(first_iso[:4]), int(first_iso[5:7])
    ly, lm = int(last_iso[:4]), int(last_iso[5:7])
    out = []
    while (y, m) <= (ly, lm):
        out.append("%04d-%02d" % (y, m))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def recorded_splits(s3, first_iso, last_iso, want):
    """Polygon's split reference rows for the tickers in `want`: ({ticker: [{date, split_from, split_to}]}, files read)."""
    out, n = {}, 0

    def one(month):
        doc, _ = optional(s3, SPLITS_ROOT + month + ".json.gz")
        return doc if isinstance(doc, list) else (doc.get("results") if isinstance(doc, dict) else None)
    with ThreadPoolExecutor(max_workers=8) as pool:
        for rows in pool.map(one, months_between(first_iso, last_iso)):
            if rows is None:
                continue
            n += 1
            for r in rows:
                if not isinstance(r, dict):
                    continue
                t, d = str(r.get("ticker") or "").upper(), str(r.get("execution_date") or "")
                if t in want and first_iso <= d <= last_iso and r.get("split_from") and r.get("split_to"):
                    out.setdefault(t, []).append({"date": d, "split_from": r["split_from"], "split_to": r["split_to"]})
    return out, n


def repair(raw, recorded):
    """A fund's bars as one continuous, split-adjusted history: (bars, events applied, unexplained jumps). Pure."""
    own, _ = T.own_history(raw[-(FUND_BARS + 40):])
    bars, events = T.rebase(own, recorded or [], [])
    good = []
    for b in bars:
        try:
            c = float(b["c"])
        except (TypeError, ValueError, KeyError):
            continue
        if c > 0 and c == c:
            good.append({"date": b["date"], "c": c, "o": b.get("o"), "v": float(b.get("v") or 0.0)})
    return good[-FUND_BARS:], events, T.jumps_of(good[-FUND_BARS:], events)


def with_forming(bars, today, ticker):
    """(bars, note): the bars with today's forming bar appended when the forming-bar file holds a later session for
    this ticker at a believable price. note is None, 'forming', or why the forming bar was not used. Pure."""
    if not bars or not isinstance(today, dict):
        return bars, None
    session, row = str(today.get("session") or ""), (today.get("rows") or {}).get(ticker)
    if len(session) != 10 or session <= bars[-1]["date"] or not isinstance(row, list) or len(row) < 4:
        return bars, None
    try:
        c = float(row[3])
    except (TypeError, ValueError):
        return bars, None
    if not c > 0:
        return bars, None
    ratio = c / bars[-1]["c"]
    if not FORMING_BAND[0] < ratio < FORMING_BAND[1]:
        return bars, "today's bar is %.2f times the last close: not used" % ratio
    return bars + [{"date": session, "c": c, "o": row[0], "v": float(row[4] or 0.0) if len(row) > 4 else 0.0, "forming": True}], "forming"


def dollar_volume(bars, sessions=20):
    """Average close x volume over the last `sessions` completed bars; None when the bars carry no volume. Pure."""
    done = [b for b in bars if not b.get("forming")][-sessions:]
    vals = [b["c"] * b["v"] for b in done if b.get("v")]
    return sum(vals) / len(vals) if len(vals) >= max(5, sessions // 2) else None


def coin_series(doc):
    """([(date, close)], [(date, close, volume)]) from a banked coin document. Pure."""
    rows = [(str(r[0])[:10], r[4], (r[5] if len(r) > 5 else 0) or 0) for r in (doc or {}).get("rows") or [] if isinstance(r, list) and len(r) >= 5]
    return M.clean([(d, c) for d, c, _ in rows]), rows


def fmp_series(doc, invert=False):
    """[(date, close)] from an FMP price-bank document (a list of {date, close}, or one under rows / historical). Pure."""
    rows = doc if isinstance(doc, list) else (doc or {}).get("rows") or (doc or {}).get("historical") or (doc or {}).get("data") or []
    return M.clean([(r.get("date"), r.get("close")) for r in rows if isinstance(r, dict)], invert=invert)


def recent(series, days=SERIES_DAYS):
    if not series:
        return series
    floor = (date.fromisoformat(series[-1][0]) - timedelta(days=days)).isoformat()
    return [p for p in series if p[0] >= floor]


def big_move(series, limit, window=JUMP_WINDOW):
    """The latest one-bar change beyond `limit` inside the last `window` bars: {date, ratio} or None. Pure."""
    tail = series[-(window + 1):]
    hit = None
    for i in range(1, len(tail)):
        r = tail[i][1] / tail[i - 1][1]
        if abs(r - 1.0) > limit:
            hit = {"date": tail[i][0], "ratio": round(r, 4)}
    return hit


def pick_coins(listing, banked, shown=U.COINS_SHOWN):
    """The coins to map, largest market value first: [(symbol, name, market value or None)].

    `listing` is CoinMarketCap's banked listing (rows by rank), `banked` the symbols with a current banked history.
    A symbol CoinMarketCap lists more than once belongs to its highest-ranked coin. Stablecoins and wrapped or
    tokenized assets are not coins for this purpose. The three coins of the first map are always kept. Pure."""
    out, seen = [], set()
    for c in listing or []:
        sym = str(c.get("symbol") or "").upper()
        if not sym or sym in seen:
            continue
        seen.add(sym)
        tags = [str(t) for t in c.get("tags") or []]
        if sym not in banked or any(t in U.NOT_A_COIN_TAG for t in tags):
            continue
        usd = (c.get("quote") or {}).get("USD") or {}
        try:
            mc = float(usd.get("market_cap") or 0)
        except (TypeError, ValueError):
            mc = 0.0
        out.append((sym, str(c.get("name") or sym), mc if mc > 0 else None))
    if not out:
        out = [(s, s, None) for s in U.COIN_FALLBACK if s in banked]
    out = out[:shown]
    have = {s for s, _, _ in out}
    for s in U.LEGACY_COINS:
        if s not in have and s in banked:
            out.append((s, s, None))
    return out


# ---------------------------------------------------------------------------------------------------------------- build
def sig(x, digits=8):
    """A price to eight significant figures: a split-adjusted close need not carry sixteen."""
    return float("%.*g" % (digits, x))


def tile_of(member, row, extra):
    """One instrument as the document carries it. Pure."""
    t = {"t": member["t"], "n": member["name"], "k": member["kind"], "cls": member["cls"], "grp": member["grp"], "role": member["role"],
         "sym": member["sym"], "score": row["score"], "state": row["state"], "c": sig(row["c"]), "d": row["d"], "p": sig(row["p"]), "pd": row["pd"],
         "r": row["r"], "a": {k: [v[0], sig(v[1]), v[2]] for k, v in row["a"].items()}, "u": row["u"], "sd": row["sd"],
         "v50": row["v50"], "v200": row["v200"], "off_hi": row["off_hi"], "off_lo": row["off_lo"]}
    if row["held"]:
        t["held"] = row["held"]
    t.update({k: v for k, v in extra.items() if v is not None})
    return t


def demote(row, why):
    """A row with an unexplained move is not called booming or destroyed. Pure (returns a new row)."""
    if row["state"] not in ("BOOMING", "DESTROYED"):
        return row
    out = dict(row)
    out["state"] = "RISK-ON" if row["state"] == "BOOMING" else "RISK-OFF"
    out["held"] = why
    return out


def group_stats(tiles):
    """Counts, the average score, the share above zero and the median change of each window. Pure."""
    scores = [t["score"] for t in tiles]
    states = {s: sum(1 for t in tiles if t["state"] == s) for s in reversed(M.STATES)}
    best = max(tiles, key=lambda t: t["score"]) if tiles else None
    worst = min(tiles, key=lambda t: t["score"]) if tiles else None
    return {"n": len(tiles), "score": None if not scores else round(sum(scores) / len(scores), 1),
            "rising_pct": None if not scores else round(100.0 * sum(1 for s in scores if s > 0) / len(scores)),
            "states": states, "r": {k: (None if M.median([t["r"].get(k) for t in tiles]) is None else round(M.median([t["r"].get(k) for t in tiles]), 2)) for k in M.WINDOWS},
            "best": best and best["t"], "worst": worst and worst["t"]}


def reference(prev, now):
    """The snapshot a run is compared with: the last run of an earlier UTC day. A second run on the same day is compared with
    what the first one was compared with, so running the engine again by hand never hides the day's turns. Pure."""
    prev = prev or {}
    base = prev.get("base")
    if str(prev.get("generated_at") or "")[:10] == now.date().isoformat() and isinstance(base, dict) and base.get("generated_at"):
        return base
    return prev


def turns(now_states, prev_states, names):
    """What changed state, or moved 18 points, since the run it is compared with. The first engine's rule, except its last line:
    that called every change it had no other name for WEAKENING, whichever way it went (a rise of 17 points from NEUTRAL to
    RISK-ON was "weakening"). A change with no other name is now named by its direction. Pure."""
    rank = {s: i for i, s in enumerate(M.STATES)}
    out = []
    for key, cur in now_states.items():
        p = (prev_states or {}).get(key)
        if not p:
            continue
        d_ro = cur["risk_on"] - p.get("risk_on", 0)
        if cur["state"] == p.get("state") and abs(d_ro) < 18:
            continue
        pr, cr = rank.get(p.get("state"), 2), rank[cur["state"]]
        if cr > pr and cr >= 2 and pr <= 1:
            kind = "BOTTOMING / RECOVERING"
        elif cr >= 4 and pr < 4:
            kind = "ACCELERATING"
        elif cr < pr and pr >= 3 and cr <= 2:
            kind = "ROLLING OVER"
        elif cr == 0 and pr > 0:
            kind = "CAPITULATING"
        elif p.get("risk_on", 0) <= 0 < cur["risk_on"]:
            kind = "FLIPPED RISK-ON"
        elif p.get("risk_on", 0) >= 0 > cur["risk_on"]:
            kind = "FLIPPED RISK-OFF"
        elif cr > pr or (cr == pr and d_ro > 0):
            kind = "STRENGTHENING"
        else:
            kind = "WEAKENING"
        out.append(dict(names[key], **{"ticker": key, "from": p.get("state"), "to": cur["state"], "delta": d_ro, "kind": kind}))
    out.sort(key=lambda x: -abs(x["delta"]))
    return out


def legacy_regime(lookup):
    """The first engine's regime read, on its own 38 instruments, by its own rule. `lookup` maps ticker -> legacy row. Pure."""
    def avg(keys):
        vals = [lookup[k]["risk_on"] for k in keys if k in lookup]
        return round(sum(vals) / len(vals), 1) if vals else 0.0
    eq_keys = [t for sleeve, rows in U.LEGACY.items() if sleeve.startswith("Equity") for t, _ in rows]
    eq_avg = avg(eq_keys)
    crypto_avg = avg(["BTC", "ETH", "SOL"])
    rates_avg = avg(["TLT", "IEF", "LQD", "HYG", "TIP"])
    commod_avg = avg(["GLD", "SLV", "USO", "DBC", "UUP"])
    intl_avg = avg(["EFA", "EEM", "FXI"])
    eq_breadth_pct = round(100 * sum(1 for k in eq_keys if k in lookup and lookup[k]["risk_on"] > 0) / max(1, sum(1 for k in eq_keys if k in lookup)))
    conc = round(lookup.get("SPY", {}).get("r3m", 0.0) - lookup.get("RSP", {}).get("r3m", 0.0), 1)
    if eq_avg > 8 and crypto_avg < -15:
        label = "BIFURCATED"
        summary = (f"Two regimes at once: equities broadly risk-on (avg {eq_avg:+}) while crypto is risk-off "
                   f"(avg {crypto_avg:+}). The index strength is real and broad — it is NOT masking weak breadth.")
    elif eq_avg > 8 and eq_breadth_pct >= 62 and conc < 4:
        label = "BROAD RISK-ON"
        summary = (f"Broad-based equity strength: {eq_breadth_pct}% of equity sleeves risk-on, equal-weight "
                   f"keeping pace (concentration spread only {conc:+}pts). Healthy participation, not a narrow melt-up.")
    elif eq_avg > 8 and (eq_breadth_pct < 45 or conc >= 5):
        label = "NARROW / CONCENTRATED"
        summary = (f"Index up but breadth thin — concentration spread {conc:+}pts, only {eq_breadth_pct}% of "
                   f"equity sleeves risk-on. A megacap-led tape sitting over a weak median stock.")
    elif eq_avg < -8:
        label = "BROAD RISK-OFF"
        summary = (f"Broad equity weakness (avg {eq_avg:+}, breadth {eq_breadth_pct}%). Risk-off across the board.")
    else:
        label = "MIXED / ROTATIONAL"
        summary = (f"No dominant regime — equities mixed (avg {eq_avg:+}, breadth {eq_breadth_pct}%), "
                   f"crypto {crypto_avg:+}, rates {rates_avg:+}. A rotational, stock-picker's tape.")
    return {"label": label, "summary": summary, "concentration_spread_3m": conc, "equity_breadth_pct": eq_breadth_pct, "equity_avg": eq_avg,
            "crypto_avg": crypto_avg, "rates_avg": rates_avg, "commodities_avg": commod_avg, "intl_avg": intl_avg}


CROSS_RULE = ("Risk assets = the average, over every group of instruments that rise when investors take risk, of the group's average score. "
              "Stores of value = the average of the Treasuries' average score, gold's score and the average of the yen and the Swiss franc. "
              "The dollar = the dollar index. At -15 or under for risk assets: FLIGHT TO SAFETY when the stores of value are at +15 or over, "
              "LIQUIDATION when they are not and the dollar is at +15 or over, otherwise BROAD SELL-OFF. At +15 or over for risk assets: "
              "EVERYTHING UP when the stores of value are at +15 or over too, otherwise RISK-ON. Anything else: MIXED.")


def cross_asset(tiles):
    """The whole map in one line: risk assets against the stores of value and the dollar. Pure."""
    by_group = {}
    for t in tiles:
        if t["role"] == "risk":
            by_group.setdefault(t["grp"], []).append(t["score"])
    risk = M.mean([sum(v) / len(v) for v in by_group.values()])
    pick = {t["t"]: t["score"] for t in tiles}
    ust = M.mean([t["score"] for t in tiles if t["grp"] == "bond_ust" and t["role"] == "haven"])
    gold = pick.get("GLD")
    yen_franc = M.mean([pick.get("JPY"), pick.get("CHF")])
    safe = M.mean([ust, gold, yen_franc])
    dollar = pick.get("DXY", pick.get("UUP"))
    vix = pick.get("VIX")
    rnd = lambda v: None if v is None else round(v, 1)      # noqa: E731
    out = {"risk": rnd(risk), "safe": rnd(safe), "treasuries": rnd(ust), "gold": gold, "yen_franc": rnd(yen_franc), "dollar": dollar, "vix": vix,
           "risk_groups": len(by_group), "rule": CROSS_RULE}
    if risk is None or safe is None:
        out.update(label="NOT READ", summary="Too few instruments were readable to compare risk assets with the stores of value.")
        return out
    d = dollar if dollar is not None else 0
    if risk <= M.OFF:
        if safe >= M.ON:
            label, text = "FLIGHT TO SAFETY", "Risk assets are falling and the stores of value are rising: money is running to safety."
        elif d >= M.ON:
            label, text = "LIQUIDATION", "Risk assets are falling and the usual stores of value are not rising with them; the dollar is. Cash is the haven."
        else:
            label, text = "BROAD SELL-OFF", "Risk assets are falling; neither the stores of value nor the dollar is clearly bid."
    elif risk >= M.ON:
        if safe >= M.ON:
            label, text = "EVERYTHING UP", "Risk assets and the stores of value are rising together."
        else:
            label, text = "RISK-ON", "Risk assets are rising and the stores of value are not being sought."
    else:
        label, text = "MIXED", "Risk assets are neither rising nor falling as a whole."
    out.update(label=label, summary="%s Risk assets %+.1f, stores of value %+.1f (Treasuries %s, gold %s, yen and franc %s), dollar %s." % (
        text, risk, safe, "n/a" if ust is None else "%+.1f" % ust, "n/a" if gold is None else "%+d" % gold,
        "n/a" if yen_franc is None else "%+.1f" % yen_franc, "n/a" if dollar is None else "%+d" % dollar))
    return out


def build(s3, now=None, prev=None, log=print):
    """The whole document from the warehouse. Reads only. Raises when too little could be read to publish."""
    t0 = time.time()
    now = now or datetime.now(timezone.utc)
    fixed = U.members()
    notes, left_out = [], []

    # ---- funds: the EOD bank, splits rebased, today's forming bar
    fund_members = [m for m in fixed if m["kind"] == "fund"]
    tickers = [m["sym"] for m in fund_members]

    def read_fund(t):
        doc, err = optional(s3, bank_key(t))
        return t, (fund_bars(doc) if doc else []), err
    with ThreadPoolExecutor(max_workers=THREADS) as pool:
        raw = {t: (bars, err) for t, bars, err in pool.map(read_fund, tickers)}
    lasts = [bars[-1]["date"] for bars, _ in raw.values() if bars]
    if not lasts:
        raise RuntimeError("no fund could be read from the EOD bank: the previous publication stands")
    bank_last = max(lasts)
    first_needed = (date.fromisoformat(bank_last) - timedelta(days=int(FUND_BARS * 1.5) + 30)).isoformat()
    recorded, split_files = recorded_splits(s3, first_needed, bank_last, set(tickers))
    today, today_err = optional(s3, TODAY_KEY)
    series, extras = {}, {}
    forming_n = 0
    for m in fund_members:
        t = m["sym"]
        bars, err = raw[t]
        if not bars:
            left_out.append({"t": m["t"], "why": "no banked bars (%s)" % (err or "empty")})
            continue
        bars, events, jumps = repair(bars, recorded.get(t))
        bars, note = with_forming(bars, today, t)
        if note == "forming":
            forming_n += 1
        elif note:
            notes.append("%s: %s" % (t, note))
        dv = dollar_volume(bars)
        if dv is not None and dv < MIN_DOLLAR_VOLUME:
            left_out.append({"t": m["t"], "why": "trades $%.0f a day over 20 sessions, under the $%.0f floor" % (dv, MIN_DOLLAR_VOLUME)})
            continue
        series[m["t"]] = [(b["date"], b["c"]) for b in bars]
        jump = next((j for j in reversed(jumps) if j.get("ago", 10 ** 6) <= JUMP_WINDOW), None)
        extras[m["t"]] = {"dv": None if dv is None else round(dv), "f": True if (bars and bars[-1].get("forming")) else None,
                          "splits": [{"date": e["date"], "factor": e["factor"], "source": e.get("source")} for e in events] or None,
                          "jump": jump and {"date": jump["date"], "ratio": jump["ratio"]}, "chart": t}

    # ---- coins: the banked bars; which coins, and how large, from CoinMarketCap's banked listing
    listing, listing_day = None, None
    try:
        keys = []
        tok = None
        while True:
            kw = {"Bucket": BUCKET, "Prefix": CMC_ROOT}
            if tok:
                kw["ContinuationToken"] = tok
            page = s3.list_objects_v2(**kw)
            keys += [o["Key"] for o in page.get("Contents") or [] if o["Key"].endswith(".json.gz")]
            tok = page.get("NextContinuationToken")
            if not tok:
                break
        if keys:
            newest = sorted(keys)[-1]
            doc = get_json(s3, newest)
            data = doc.get("data") if isinstance(doc, dict) else doc
            data = data.get("data") if isinstance(data, dict) and "data" in data else data
            if isinstance(data, list) and data:
                listing, listing_day = data, newest[len(CMC_ROOT):-len(".json.gz")]
    except Exception as exc:  # noqa: BLE001
        notes.append("CoinMarketCap listing not read (%s): coins ordered by a fixed list, no market value" % type(exc).__name__)
    coin_keys = []
    try:
        tok = None
        while True:
            kw = {"Bucket": BUCKET, "Prefix": COIN_ROOT}
            if tok:
                kw["ContinuationToken"] = tok
            page = s3.list_objects_v2(**kw)
            coin_keys += [o["Key"] for o in page.get("Contents") or [] if o["Key"].endswith(".json.gz")]
            tok = page.get("NextContinuationToken")
            if not tok:
                break
    except Exception as exc:  # noqa: BLE001
        notes.append("the coin folder could not be listed (%s): the fixed list of coins was read by name" % type(exc).__name__)
    if not coin_keys:
        coin_keys = [COIN_ROOT + c + ".json.gz" for c in dict.fromkeys(U.COIN_FALLBACK + U.LEGACY_COINS)]

    def read_coin(key):
        doc, _ = optional(s3, key)
        return key[len(COIN_ROOT):-len(".json.gz")].upper(), coin_series(doc) if doc else ([], [])
    with ThreadPoolExecutor(max_workers=THREADS) as pool:
        coins = dict(pool.map(read_coin, coin_keys))
    coin_last = max((s[-1][0] for s, _ in coins.values() if s), default=None)
    current = {sym for sym, (s, _) in coins.items() if s and coin_last and (date.fromisoformat(coin_last) - date.fromisoformat(s[-1][0])).days <= 2 and len(s) >= M.MIN_BARS}
    members = list(fixed)
    for i, (sym, name, mc) in enumerate(pick_coins(listing, current)):
        s, rows = coins[sym]
        members.append({"cls": "crypto", "grp": "cr_major" if i < U.COINS_MAJOR else "cr_alt", "kind": "coin", "sym": "X:%sUSD" % sym, "t": sym, "name": name, "role": "risk"})
        series[sym] = recent(s)
        vols = [float(c) * float(v) for _, c, v in rows[-20:] if c and v]
        extras[sym] = {"mc": mc and round(mc), "dv": round(sum(vols) / len(vols)) if len(vols) >= 10 else None, "chart": "%s-USD" % sym}

    # ---- currencies, front-month futures, indexes: the FMP price bank
    def read_fmp(m):
        doc, err = optional(s3, fmp_key(m["sym"]))
        return m["t"], (fmp_series(doc, invert=(m["kind"] == "fx" and U.inverted(m["sym"]))) if doc else []), err
    others = [m for m in fixed if m["kind"] in ("fx", "fut", "idx")]
    with ThreadPoolExecutor(max_workers=16) as pool:
        for t, s, err in pool.map(read_fmp, others):
            m = next(x for x in others if x["t"] == t)
            if not s:
                left_out.append({"t": t, "why": "no banked prices (%s)" % (err or "empty")})
                continue
            series[t] = recent(s)
            limit = None if t in VOL_INDEXES else JUMP_LIMIT.get(m["kind"])
            extras[t] = {"jump": big_move(series[t], limit) if limit else None, "inv": True if (m["kind"] == "fx" and U.inverted(m["sym"])) else None}

    # ---- measure, check, date
    newest = {}
    for m in members:
        if m["t"] in series and series[m["t"]]:
            newest[m["kind"]] = max(newest.get(m["kind"], ""), series[m["t"]][-1][0])
    for kind, last in sorted(newest.items()):
        age = (now.date() - date.fromisoformat(last)).days
        if age > WAREHOUSE_LATE_DAYS:
            notes.append("the newest bar of the %s is %s, %d days before this run: the warehouse has not been fed" % (KIND_WORDS.get(kind, kind), last, age))
    tiles, faults = [], []
    for m in members:
        s = series.get(m["t"])
        if not s:
            continue
        behind = (date.fromisoformat(newest[m["kind"]]) - date.fromisoformat(s[-1][0])).days
        if behind > STALE_DAYS:
            left_out.append({"t": m["t"], "why": "no bar since %s (%d days behind the newest %s)" % (s[-1][0], behind, m["kind"])})
            continue
        row, why = M.measure(s)
        if not row:
            left_out.append({"t": m["t"], "why": why})
            continue
        bad = M.check(row)
        if bad:
            faults.append("%s: %s" % (m["t"], "; ".join(bad)))
            left_out.append({"t": m["t"], "why": "failed its own check: " + "; ".join(bad)})
            continue
        extra = dict(extras.get(m["t"]) or {})
        if extra.get("jump"):
            row = demote(row, "a one-day move of %+.0f%% on %s with no split on record: not called" % ((extra["jump"]["ratio"] - 1) * 100, extra["jump"]["date"]))
        if behind:
            extra["late"] = behind
        tiles.append(tile_of(m, row, extra))
    if faults:
        log("[regime-map] rows that failed their own check: %s" % faults)
    universe_n = len(members)
    if len(tiles) < MIN_SHARE * universe_n:
        raise RuntimeError("only %d of %d instruments could be measured: the previous publication stands" % (len(tiles), universe_n))
    by_t = {t["t"]: t for t in tiles}

    # ---- what 1.0.0 published, for its 38 instruments
    sleeves, ranked, lookup = {s: [] for s in U.LEGACY}, [], {}
    for sleeve, rows in U.LEGACY.items():
        for t, name in rows:
            x = by_t.get(t)
            if not x:
                continue
            item = {"ticker": t, "name": name, "sleeve": sleeve, "risk_on": x["score"], "state": x["state"],
                    "r1m": round(x["r"]["m"], 1), "r3m": round(x["r"]["q"], 1), "r6m": round(x["r"]["h"], 1), "as_of": x["d"]}
            sleeves[sleeve].append(item)
            ranked.append(item)
            lookup[t] = item
    if len(ranked) < MIN_LEGACY:
        raise RuntimeError("only %d of the first map's 38 instruments could be measured: the previous publication stands" % len(ranked))
    for s in sleeves:
        sleeves[s].sort(key=lambda x: -x["risk_on"])
    ranked.sort(key=lambda x: -x["risk_on"])
    regime = legacy_regime(lookup)
    prev = reference(prev, now)
    transitions = turns({i["ticker"]: i for i in ranked}, prev.get("states"), {i["ticker"]: {"name": i["name"], "sleeve": i["sleeve"]} for i in ranked})
    prev_label = prev.get("label")

    # ---- the whole map
    order = U.layout()
    stats = {"classes": {}, "groups": {}}
    for c in order:
        stats["classes"][c["id"]] = group_stats([t for t in tiles if t["cls"] == c["id"]])
        for g in c["groups"]:
            stats["groups"][g["id"]] = group_stats([t for t in tiles if t["grp"] == g["id"]])
    booming = sorted((t for t in tiles if t["state"] == "BOOMING"), key=lambda t: (-t["score"], -t["r"]["q"]))
    destroyed = sorted((t for t in tiles if t["state"] == "DESTROYED"), key=lambda t: (t["score"], t["r"]["q"]))
    by_score = sorted(tiles, key=lambda t: -t["score"])
    map_turns = turns({t["t"]: {"risk_on": t["score"], "state": t["state"]} for t in tiles}, prev.get("tiles"),
                      {t["t"]: {"name": t["n"], "cls": t["cls"], "grp": t["grp"]} for t in tiles})
    funds = [t for t in tiles if t["k"] == "fund"]
    as_of = {k: newest.get(k) for k in ("fund", "coin", "fx", "fut", "idx")}
    as_of.update(funds_forming=forming_n, funds_forming_as_of=(today or {}).get("as_of") if forming_n else None, bank_last_session=bank_last,
                 coin_listing_day=listing_day)
    payload = {
        "engine": "justhodl-regime-map", "version": VERSION, "contract": CONTRACT, "ok": True,
        "generated_at": now.isoformat(),
        "thesis": "Cross-asset dispersion map — what's booming (risk-on) vs getting destroyed (risk-off), "
                  "with a regime classifier that tells broad from narrow from bifurcated.",
        "regime": regime,
        "regime_changed": bool(prev_label and prev_label != regime["label"]),
        "prev_regime": prev_label,
        "transitions": transitions[:14],
        "n_transitions": len(transitions),
        "turns_since": prev.get("generated_at"),
        "booming": [x for x in ranked if x["state"] == "BOOMING"][:6],
        "destroyed": list(reversed([x for x in ranked if x["state"] == "DESTROYED"]))[:6],
        "strongest": ranked[:6],
        "weakest": list(reversed(ranked[-6:])),
        "sleeves": sleeves,
        "ranked": ranked,
        "n_instruments": len(ranked),
        "windows": "risk_on = multi-window momentum (1m/3m/6m, by the calendar) + trend (vs 50/200d MA), −100..+100",
        "as_of": as_of,
        "cross_asset": cross_asset(tiles),
        "map": {
            "classes": order,
            "tiles": tiles,
            "stats": stats,
            "booming": [t["t"] for t in booming],
            "destroyed": [t["t"] for t in destroyed],
            "strongest": [t["t"] for t in by_score[:12]],
            "weakest": [t["t"] for t in reversed(by_score[-12:])],
            "transitions": map_turns[:40],
            "n_transitions": len(map_turns),
            "left_out": left_out,
            "n": len(tiles), "n_universe": universe_n,
            "counts": {k: sum(1 for t in tiles if t["k"] == k) for k in ("fund", "coin", "fx", "fut", "idx")},
        },
        "rules": {
            "windows": "Every change is measured by the calendar from the last close on or before the same day one week, one, three, six or twelve months earlier (or the last close of the previous year); the day's change from the close before.",
            "score": "score = 220 x (0.45 x three months + 0.30 x six months + 0.25 x one month, as decimals) + 25 x trend, held to -100..+100; trend runs from -1 to +1 on three tests: the close above its 50-day average, above its 200-day average, the 50 above the 200.",
            "state": "BOOMING at +45 or more, RISK-ON at +15, NEUTRAL above -15, RISK-OFF above -45, DESTROYED below. BOOMING also needs one month and three months both up, one of them by 5% or more, and the close above both averages; DESTROYED needs both down, one of them by 5% or more, and the close below both. A score that fails those tests is shown one state in, with the reason.",
            "unusual": "How many of its own usual moves a change is: the log change divided by (the standard deviation of its last 252 daily log changes x the square root of the bars in the window).",
            "currencies": "A currency tile is the currency against the dollar: green means the currency rose.",
            "left_out": "A fund trading under $250,000 a day, an instrument with under 130 daily bars or no bar for a week, and any row that fails its own arithmetic are left out and named.",
        },
        "sources": [
            {"what": "funds", "key": EOD_ROOT + "<TICKER>.json.gz", "engine": "justhodl-polygon-eod", "note": "daily bars of every US-listed fund; splits rebased from " + SPLITS_ROOT},
            {"what": "funds, today", "key": TODAY_KEY, "engine": "justhodl-polygon-today", "note": "today's forming bar, 15 minutes delayed"},
            {"what": "coins", "key": COIN_ROOT + "<COIN>.json.gz", "engine": "justhodl-katlin", "note": "Polygon X:<COIN>USD daily, the UTC day"},
            {"what": "coins, market value", "key": CMC_ROOT + "<day>.json.gz", "engine": "justhodl-provider-bank", "note": "CoinMarketCap listing"},
            {"what": "currencies, front-month futures, indexes", "key": FMP_ROOT + "<SYMBOL>.json.gz", "engine": "justhodl-fmp-walker", "note": "FMP end-of-day prices"},
        ],
        "notes": notes,
        "split_files_read": split_files,
        "elapsed_s": round(time.time() - t0, 1),
    }
    return payload


SNAPSHOT_KEYS = ("generated_at", "label", "states", "tiles")


def snapshot(payload, prev=None):
    """What the next run compares against, with the last run of an earlier UTC day kept beside it as `base` (see reference). Pure."""
    snap = {"generated_at": payload["generated_at"], "label": payload["regime"]["label"],
            "states": {i["ticker"]: {"risk_on": i["risk_on"], "state": i["state"]} for i in payload["ranked"]},
            "tiles": {t["t"]: {"risk_on": t["score"], "state": t["state"]} for t in payload["map"]["tiles"]}}
    prev = prev or {}
    same_day = str(prev.get("generated_at") or "")[:10] == payload["generated_at"][:10]
    base = prev.get("base") if same_day else {k: prev[k] for k in SNAPSHOT_KEYS if k in prev}
    if isinstance(base, dict) and base.get("generated_at"):
        snap["base"] = {k: base[k] for k in SNAPSHOT_KEYS if k in base}
    return snap


def lambda_handler(event=None, context=None):
    s3 = _s3()
    prev, _ = optional(s3, PREV_KEY)
    payload = build(s3, prev=prev)
    if isinstance(event, dict) and event.get("dry_run"):
        m = payload["map"]
        return {"statusCode": 200, "dry_run": True, "regime": payload["regime"]["label"], "cross_asset": payload["cross_asset"]["label"],
                "tiles": m["n"], "universe": m["n_universe"], "booming": m["booming"], "destroyed": m["destroyed"], "left_out": m["left_out"]}
    body = json.dumps(payload, separators=(",", ":")).encode()
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=body, ContentType="application/json", CacheControl="public, max-age=900")
    try:
        s3.put_object(Bucket=BUCKET, Key=PREV_KEY, Body=json.dumps(snapshot(payload, prev), separators=(",", ":")).encode(), ContentType="application/json")
    except Exception as exc:  # noqa: BLE001
        print("[regime-map] the snapshot for the next run was not written: %s" % type(exc).__name__)
    r, m = payload["regime"], payload["map"]
    print("[regime-map %s] %d of %d instruments | regime=%s | eq %+.1f crypto %+.1f breadth %d%% conc %+.1f | cross-asset=%s | booming %d destroyed %d left out %d | %d bytes | %ss" % (
        VERSION, m["n"], m["n_universe"], r["label"], r["equity_avg"], r["crypto_avg"], r["equity_breadth_pct"], r["concentration_spread_3m"],
        payload["cross_asset"]["label"], len(m["booming"]), len(m["destroyed"]), len(m["left_out"]), len(body), payload["elapsed_s"]))
    return {"statusCode": 200, "body": json.dumps({"ok": True, "regime": r["label"], "n": len(payload["ranked"]), "tiles": m["n"],
                                                   "equity_avg": r["equity_avg"], "crypto_avg": r["crypto_avg"]})}
