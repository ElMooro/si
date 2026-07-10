"""
justhodl-accumulation-radar  ·  v1.0  —  ACCUMULATION / DISTRIBUTION + TOPS / BOTTOMS
================================================================================
A unified Wyckoff-style cycle engine across STOCKS, ETFs and COUNTRIES. It answers:
  • Which names are under ACCUMULATION (smart money buying) vs DISTRIBUTION (selling)?
  • Which are most likely at a TOP, and which at a BOTTOM?

Per name it computes the classic volume + price tells institutions use:
    • OBV (On-Balance Volume) trend        — is volume confirming or fading price?
    • Chaikin Money Flow (CMF, 20d)         — buying vs selling pressure inside the bar
    • RSI(14)                               — overbought / oversold
    • price vs 50- and 200-DMA              — trend + over-extension
    • position in the 200-day range (0-100) — near highs (top risk) or lows (bottom)
    • price-vs-OBV DIVERGENCE               — the Wyckoff tell:
         price up + OBV down  = bearish divergence → distribution / top
         price down + OBV up  = bullish divergence → accumulation / bottom

These resolve into a Wyckoff PHASE (ACCUMULATION / MARKUP / DISTRIBUTION / MARKDOWN)
and two composite scores — top_score and bottom_score — that flag LIKELY_TOP and
LIKELY_BOTTOM. Output is split by asset class. LIKELY_BOTTOM (UP) and LIKELY_TOP
(DOWN) calls are logged to the scorecard and graded on forward excess-vs-benchmark
(SPY for stocks/ETFs, ACWX for countries) — measure-before-trust, like everything else.

Data: maintains its own compact OHLCV-derived buffer (close + volume + money-flow-volume)
from Polygon grouped-daily — ONE call per day covers the whole market. Self-bootstraps
~200 trading days on first runs (time-budgeted, idempotent upsert) and self-heals date order.
"""
import os, json, time, math, boto3
import urllib.request
from datetime import datetime, timezone, timedelta
from decimal import Decimal

S3 = boto3.client("s3", "us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/accumulation-radar.json"
BUF_KEY = "data/_cycle/pv.json"
POLY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
VERSION = "1.4.2"
MAXDAYS = 235          # buffer depth (v1.4: +35 so the 50/200 cross scan has a 15-session window)
MIN_PTS = 60           # minimum history to score a name
N_STOCKS = 420         # liquid US stocks in the universe (+ ETFs below)

# country ETFs → classified as "country"
COUNTRY_ETF = {
    "EWZ": "Brazil", "EWW": "Mexico", "ARGT": "Argentina", "ECH": "Chile", "INDA": "India",
    "FXI": "China", "MCHI": "China", "KWEB": "China", "EWH": "Hong Kong", "EWT": "Taiwan",
    "EWY": "South Korea", "EWJ": "Japan", "EWS": "Singapore", "THD": "Thailand", "EIDO": "Indonesia",
    "EPHE": "Philippines", "VNM": "Vietnam", "EWM": "Malaysia", "AAXJ": "Asia ex-Japan",
    "EWG": "Germany", "EWU": "UK", "EWQ": "France", "EWI": "Italy", "EWP": "Spain", "EWL": "Switzerland",
    "EWN": "Netherlands", "EWD": "Sweden", "EPOL": "Poland", "GREK": "Greece", "TUR": "Turkey",
    "EZA": "South Africa", "KSA": "Saudi Arabia", "EIS": "Israel", "EWA": "Australia", "EWC": "Canada",
    "EEM": "EM broad", "VWO": "EM broad", "EFA": "DM ex-US", "ACWX": "World ex-US",
}
# sector / thematic ETFs → classified as "etf"
SECTOR_ETF = {
    "XLK": "Technology", "XLF": "Financials", "XLE": "Energy", "XLV": "Health Care",
    "XLI": "Industrials", "XLY": "Cons. Disc.", "XLP": "Cons. Staples", "XLU": "Utilities",
    "XLB": "Materials", "XLRE": "Real Estate", "XLC": "Comm. Services", "SMH": "Semiconductors",
    "SOXX": "Semiconductors", "IGV": "Software", "XBI": "Biotech", "KRE": "Regional Banks",
    "ITB": "Homebuilders", "XRT": "Retail", "XOP": "Oil & Gas", "GDX": "Gold Miners",
    "ARKK": "Innovation", "IWM": "Small Caps", "QQQ": "Nasdaq 100", "SPY": "S&P 500",
    "DIA": "Dow", "KWEB": "China Internet", "IBB": "Biotech", "TAN": "Solar", "LIT": "Lithium",
    "URA": "Uranium", "JETS": "Airlines", "HACK": "Cybersecurity", "BOTZ": "Robotics/AI",
    "IYT": "Transports", "XME": "Metals & Mining", "PAVE": "Infrastructure",
}


def _get(url, timeout=30):
    try:
        return json.loads(urllib.request.urlopen(url, timeout=timeout).read())
    except Exception as e:
        return {"_err": str(e)[:60]}


def grouped(d):
    j = _get(f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{d}"
             f"?adjusted=true&apiKey={POLY}", timeout=40)
    return j.get("results") or []


def _read(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return {}


def _mfv(o, h, l, c, v):
    """Money-flow volume for one bar (Chaikin): where did it close within the range?"""
    rng = h - l
    if not rng or v is None:
        return 0.0
    return (((c - l) - (h - c)) / rng) * v


def _rsi(closes, n=14):
    if len(closes) <= n:
        return None
    gains = losses = 0.0
    for i in range(-n, 0):
        ch = closes[i] - closes[i - 1]
        if ch >= 0:
            gains += ch
        else:
            losses -= ch
    if losses == 0:
        return 100.0
    rs = (gains / n) / (losses / n)
    return round(100 - 100 / (1 + rs), 1)


def _sma(xs, n):
    if len(xs) < n:
        n = len(xs)
    return sum(xs[-n:]) / n if n else None


def build_universe(latest_results):
    """Top liquid US stocks by dollar volume + the curated ETF maps."""
    stocks = []
    for r in latest_results:
        t = r.get("T", "")
        c, v = r.get("c"), r.get("v")
        if not (t and c and v):
            continue
        if t in COUNTRY_ETF or t in SECTOR_ETF:
            continue
        if not t.isalpha() or len(t) > 5 or c < 5:
            continue
        stocks.append((t, c * v))
    stocks.sort(key=lambda x: -x[1])
    uni = [t for t, _ in stocks[:N_STOCKS]]
    cls = {t: "stock" for t in uni}
    for t in SECTOR_ETF:
        cls[t] = "etf"
    for t in COUNTRY_ETF:
        cls[t] = "country"
    uni = list(dict.fromkeys(uni + list(SECTOR_ETF) + list(COUNTRY_ETF)))
    return uni, cls


def lambda_handler(event=None, context=None):
    t0 = time.time()
    buf = _read(BUF_KEY)
    if not isinstance(buf, dict) or "tickers" not in buf:
        buf = {"dates": [], "tickers": {}, "universe": [], "classmap": {}}

    # ── universe ──
    if not buf.get("universe"):
        d = datetime.now(timezone.utc).date()
        latest = []
        for _ in range(6):
            latest = grouped(d.isoformat())
            if latest:
                break
            d -= timedelta(days=1)
        uni, cls = build_universe(latest)
        buf["universe"] = uni
        buf["classmap"] = cls
    uni = set(buf["universe"])
    cls = buf["classmap"]

    # ── backfill missing trading days (oldest-first, time-budgeted) + latest ──
    have = set(buf["dates"])
    today = datetime.now(timezone.utc).date()
    want = []
    dd = today
    while len(want) < MAXDAYS and (today - dd).days < 330:
        ds = dd.isoformat()
        if ds not in have and dd.weekday() < 5:
            want.append(ds)
        dd -= timedelta(days=1)
    want.sort()                                   # oldest first
    added = 0
    for ds in want:
        if time.time() - t0 > 720:
            break
        res = grouped(ds)
        if not res:
            continue                              # holiday / no data
        bymap = {r.get("T"): r for r in res}
        for tk in uni:
            r = bymap.get(tk)
            if not r:
                continue
            rec = buf["tickers"].setdefault(tk, {"d": [], "c": [], "v": [], "m": []})
            if ds in rec["d"]:
                continue
            rec["d"].append(ds)
            rec["c"].append(round(r.get("c") or 0, 4))
            rec["v"].append(int(r.get("v") or 0))
            rec["m"].append(round(_mfv(r.get("o") or r.get("c"), r.get("h"), r.get("l"),
                                       r.get("c"), r.get("v")), 1))
        if ds not in buf["dates"]:
            buf["dates"].append(ds)
        added += 1

    # ── self-heal: sort each ticker series by date; keep only retained dates ──
    buf["dates"] = sorted(set(buf["dates"]))[-MAXDAYS:]
    keep = set(buf["dates"])
    for tk, rec in buf["tickers"].items():
        order = sorted(range(len(rec["d"])), key=lambda i: rec["d"][i])
        order = [i for i in order if rec["d"][i] in keep]
        rec["d"] = [rec["d"][i] for i in order]
        rec["c"] = [rec["c"][i] for i in order]
        rec["v"] = [rec["v"][i] for i in order]
        rec["m"] = [rec["m"][i] for i in order]

    S3.put_object(Bucket=BUCKET, Key=BUF_KEY, Body=json.dumps(buf, default=str).encode(),
                  ContentType="application/json")

    # ── score each name ──
    SPY = buf["tickers"].get("SPY", {})
    spy_c = SPY.get("c") or []

    def exret(c, n):
        """Excess return vs SPY over n sessions (relative strength)."""
        if len(c) > n and len(spy_c) > n and c[-1 - n] and spy_c[-1 - n]:
            return round((c[-1] / c[-1 - n] - 1) * 100 - (spy_c[-1] / spy_c[-1 - n] - 1) * 100, 1)
        return None

    def absret(c, n):
        return round((c[-1] / c[-1 - n] - 1) * 100, 1) if len(c) > n and c[-1 - n] else None

    # hot-money cross-corroboration: country -> conviction (so accumulation + foreign flow can agree)
    hm = _read("data/hot-money.json") or {}
    hm_conv = {c.get("country"): c.get("conviction") for c in (hm.get("all_countries") or [])}
    INFLOW_CONV = {"TWIN_ENGINE", "CONFIRMED_INFLOW", "EARLY_ACCUMULATION"}
    OUTFLOW_CONV = {"CONFIRMED_OUTFLOW", "OUTFLOW"}
    # short-interest map — a bottom with crowded shorts has squeeze fuel (bigger bounce)
    _si = _read("data/short-interest.json") or {}
    si_map = {}
    for _bk in ("top_squeeze_risk", "top_crowded_shorts", "top_high_dtc"):
        for _it in (_si.get(_bk) or []):
            t = (_it.get("ticker") or "").upper()
            if t and t not in si_map:
                si_map[t] = {"short_pct": _it.get("latest_short_pct"), "dtc": _it.get("days_to_cover")}
    # capitulation (market washout gauge) — bottoms are more reliable when the tape is capitulating
    _cap = _read("data/capitulation.json") or {}
    _cap_sig = str(_cap.get("signal") or "").upper()
    _cap_score = _cap.get("capitulation_score")
    market_washout = ("CAPITUL" in _cap_sig or "WASHOUT" in _cap_sig
                      or (isinstance(_cap_score, (int, float)) and _cap_score >= 65))
    # consensus-bottom — independent bottom engine; cross-confirm our LIKELY_BOTTOM names
    _cb = _read("data/consensus-bottom.json") or {}
    cb_set = set()
    for _bk in ("qualified", "near_qualified"):
        for _it in (_cb.get(_bk) or []):
            t = (_it.get("ticker") or _it.get("symbol") or "").upper() if isinstance(_it, dict) else str(_it).upper()
            if t:
                cb_set.add(t)
    def feed(key):
        try:
            return json.loads(S3.get_object(
                Bucket=BUCKET, Key=key)["Body"].read())
        except Exception:
            return None

    rows = []
    for tk in buf["universe"]:
        rec = buf["tickers"].get(tk)
        if not rec or len(rec["c"]) < MIN_PTS:
            continue
        c = rec["c"]; v = rec["v"]; m = rec["m"]
        px = c[-1]
        ma50 = _sma(c, 50); ma200 = _sma(c, 200)
        pct50 = (px / ma50 - 1) * 100 if ma50 else None
        pct200 = (px / ma200 - 1) * 100 if ma200 else None
        lo = min(c[-MAXDAYS:]); hi = max(c[-MAXDAYS:])
        rng_pos = (px - lo) / (hi - lo) * 100 if hi > lo else 50.0
        rsi = _rsi(c)
        # OBV
        obv = [0.0]
        for i in range(1, len(c)):
            obv.append(obv[-1] + (v[i] if c[i] > c[i - 1] else (-v[i] if c[i] < c[i - 1] else 0)))
        avgv = (sum(v[-20:]) / 20) or 1
        obv_chg20 = (obv[-1] - obv[-21]) / avgv if len(obv) > 21 else 0.0   # in "days of volume"
        # CMF(20)
        sv = sum(v[-20:]) or 1
        cmf = sum(m[-20:]) / sv
        # divergence (20d): price direction vs OBV direction
        price_chg20 = (c[-1] / c[-21] - 1) * 100 if len(c) > 21 else 0.0
        bearish_div = price_chg20 > 1 and obv_chg20 < -1
        bullish_div = price_chg20 < -1 and obv_chg20 > 1
        vol_surge = (sum(v[-5:]) / 5) / avgv if avgv else None
        # accumulation / distribution state
        accumulating = cmf > 0.05 or obv_chg20 > 2
        distributing = cmf < -0.05 or obv_chg20 < -2
        over_up = (pct200 or 0) > 12 and ((rsi or 0) > 68 or rng_pos > 85)
        over_dn = (pct200 or 0) < -8 and ((rsi or 100) < 35 or rng_pos < 18)
        # composite top / bottom scores (0-100)
        top_score = max(0.0, min(100.0,
            max(0, (pct200 or 0)) * 1.4 + max(0, (rsi or 0) - 55) * 1.2 + max(0, rng_pos - 60) * 0.8
            + (18 if distributing else 0) + (22 if bearish_div else 0)))
        bottom_score = max(0.0, min(100.0,
            max(0, -(pct200 or 0)) * 1.4 + max(0, 45 - (rsi or 100)) * 1.2 + max(0, 40 - rng_pos) * 0.8
            + (18 if accumulating else 0) + (22 if bullish_div else 0)))
        # squeeze fuel — a name accumulated at a bottom with crowded shorts bounces harder
        _sd = si_map.get(tk)
        squeeze_fuel = False
        if _sd and isinstance(_sd.get("short_pct"), (int, float)) and _sd["short_pct"] >= 15:
            if bottom_score > 20 or (pct200 or 0) < 0:
                bottom_score = min(100.0, bottom_score + min(15.0, _sd["short_pct"] * 0.5))
                squeeze_fuel = True
        # consensus-bottom cross-confirmation + market washout context
        cons_bottom = tk in cb_set
        if cons_bottom and bottom_score > 15:
            bottom_score = min(100.0, bottom_score + 10.0)
        if market_washout and bottom_score > 25:
            bottom_score = min(100.0, bottom_score + 6.0)
        # phase + flags
        if over_up and (distributing or bearish_div):
            phase = "DISTRIBUTION"; flag = "LIKELY_TOP"
        elif over_dn and (accumulating or bullish_div):
            phase = "ACCUMULATION"; flag = "LIKELY_BOTTOM"
        elif (pct50 or 0) > 0 and (pct200 or 0) > 0 and accumulating:
            phase = "MARKUP"; flag = None
        elif (pct50 or 0) < 0 and (pct200 or 0) < 0 and distributing:
            phase = "MARKDOWN"; flag = None
        elif distributing and (pct200 or 0) > 0:
            phase = "DISTRIBUTION"; flag = None
        elif accumulating and (pct200 or 0) < 0:
            phase = "ACCUMULATION"; flag = None
        else:
            phase = "NEUTRAL"; flag = None
        label = COUNTRY_ETF.get(tk) or SECTOR_ETF.get(tk) or ""
        # ── relative strength + leadership (market-leader tracking) ──
        rs21, rs63, rs126 = exret(c, 21), exret(c, 63), exret(c, 126)
        rs_vals = [x for x in (rs21, rs63, rs126) if x is not None]
        rs_blend = sum(rs_vals) / len(rs_vals) if rs_vals else None
        lead = 50.0
        if rs_blend is not None:
            lead += max(-40, min(40, rs_blend * 1.5))      # relative strength dominates
        lead += 6 if (pct50 or 0) > 0 else -6
        lead += 6 if (pct200 or 0) > 0 else -6
        lead += 4 if (ma50 and ma200 and ma50 > ma200) else -4
        lead += (rng_pos - 50) * 0.12                       # leaders make/hold new highs
        lead += 8 if cmf > 0.05 else (-8 if cmf < -0.05 else 0)
        lead = round(max(0.0, min(100.0, lead)), 1)
        is_leader = (lead >= 70 and (pct200 or 0) > 0 and rng_pos >= 60
                     and (cmf > 0 or obv_chg20 > 0))
        row = {
            "ticker": tk, "class": cls.get(tk, "stock"), "label": label, "price": round(px, 2),
            "phase": phase, "flag": flag,
            "rsi": rsi, "pct_vs_50dma": round(pct50, 1) if pct50 is not None else None,
            "pct_vs_200dma": round(pct200, 1) if pct200 is not None else None,
            "range_pos_pct": round(rng_pos, 0), "cmf": round(cmf, 3),
            "obv_trend": round(obv_chg20, 1),
            "short_pct": (_sd or {}).get("short_pct"), "days_to_cover": (_sd or {}).get("dtc"),
            "squeeze_fuel": squeeze_fuel, "consensus_bottom_confirm": cons_bottom,
            "divergence": ("bearish" if bearish_div else "bullish" if bullish_div else None),
            "vol_surge": round(vol_surge, 2) if vol_surge else None,
            "top_score": round(top_score, 1), "bottom_score": round(bottom_score, 1),
            "rs_21d": rs21, "rs_63d": rs63, "rs_126d": rs126,
            "ret_21d": absret(c, 21), "ret_63d": absret(c, 63), "ret_126d": absret(c, 126),
            "leadership_score": lead, "is_leader": is_leader}
        # cross-engine: for countries, does cross-border flow corroborate the cycle phase?
        if cls.get(tk) == "country" and label in hm_conv:
            conv = hm_conv[label]
            row["hot_money_conviction"] = conv
            acc_side = phase == "ACCUMULATION" or flag == "LIKELY_BOTTOM"
            dist_side = phase == "DISTRIBUTION" or flag == "LIKELY_TOP"
            if acc_side and conv in INFLOW_CONV:
                row["flow_confirm"] = "FLOW_CONFIRMED"     # accumulation + foreign buying
            elif dist_side and conv in OUTFLOW_CONV:
                row["flow_confirm"] = "FLOW_CONFIRMED"     # distribution + foreign selling
            elif (acc_side and conv in OUTFLOW_CONV) or (dist_side and conv in INFLOW_CONV):
                row["flow_confirm"] = "FLOW_DIVERGENT"
            else:
                row["flow_confirm"] = None
        rows.append(row)

    def by(cl, key, flag=None, n=20):
        xs = [r for r in rows if r["class"] == cl and (flag is None or r["flag"] == flag)]
        xs.sort(key=lambda r: -r[key])
        return xs[:n]

    # ── market leaders: highest relative-strength names under accumulation, near highs ──
    def spark(tk):
        c = (buf["tickers"].get(tk) or {}).get("c") or []
        return [round(x, 2) for x in c[-60:][::2]]      # ~30-pt sparkline for the chart

    def enrich(r):
        d = dict(r); d["spark"] = spark(r["ticker"]); return d

    market_leaders = [enrich(r) for r in sorted(
        [r for r in rows if r["class"] in ("stock", "etf") and r["is_leader"]],
        key=lambda r: -r["leadership_score"])[:25]]
    leaders_fading = [enrich(r) for r in sorted(
        [r for r in rows if r["class"] == "stock" and (r.get("rs_126d") or 0) > 5
         and (r["phase"] == "DISTRIBUTION" or ((r.get("pct_vs_50dma") or 0) < 0 and r["obv_trend"] < 0))],
        key=lambda r: -(r.get("rs_126d") or 0))[:12]]


    # ── v1.3.0 SMART-MONEY CONFLUENCE (Khalid 2026-07-10): join the
    # fleet's independent accumulation/distribution lenses per name --
    # dark-pool prints, 13F whale $ flow, Wyckoff dated phases, insider
    # clusters -- and count agreements with the radar's own read. ──
    dp_map, wh_map, ph_map, ins_buy, ins_sell = {}, {}, {}, set(), set()
    try:
        for r in (feed("data/dark-pool.json") or {}).get("board") or []:
            if r.get("ticker"):
                dp_map[r["ticker"].upper()] = {
                    "state": r.get("state"),
                    "pct": r.get("dark_pool_pct"),
                    "accel": r.get("dark_accel")}
    except Exception as e:
        print("[join] dark-pool: %s" % e)
    try:
        for sym, v in ((feed("data/whales.json") or {}).get("stocks")
                       or {}).items():
            wh_map[sym.upper()] = v.get("conviction_flow_usd")
    except Exception as e:
        print("[join] whales: %s" % e)
    try:
        for sym, v in ((feed("data/phase-detector.json") or {}
                        ).get("tickers") or {}).items():
            ph_map[sym.upper()] = {"phase": v.get("phase"),
                                   "begin": v.get("begin"),
                                   "days": v.get("days_in_phase")}
    except Exception as e:
        print("[join] phase-detector: %s" % e)
    try:
        ir = feed("data/insider-radar.json") or {}
        for c in (ir.get("clusters") or []) + (ir.get("decline_clusters")
                                               or []):
            t = (c.get("ticker") or "").upper()
            if t:
                ins_buy.add(t)
    except Exception as e:
        print("[join] insider-radar: %s" % e)
    try:
        for c in ((feed("data/insider-sell-clusters.json") or {}
                   ).get("clusters") or []):
            t = (c.get("ticker") or "").upper()
            if t:
                ins_sell.add(t)
    except Exception:
        pass                                   # optional feed

    WHALE_MIN = 25_000_000
    for row in rows:
        tk = row["ticker"].upper()
        confirms_b, confirms_t = [], []
        dp = dp_map.get(tk)
        if dp:
            row["dark_pool"] = dp
            if dp.get("state") == "ACCUMULATION":
                confirms_b.append("DARK_POOL")
            elif dp.get("state") == "DISTRIBUTION":
                confirms_t.append("DARK_POOL")
        wf = wh_map.get(tk)
        if wf is not None:
            row["whale_flow_usd"] = wf
            if wf >= WHALE_MIN:
                confirms_b.append("WHALES_13F")
            elif wf <= -WHALE_MIN:
                confirms_t.append("WHALES_13F")
        ph = ph_map.get(tk)
        if ph and ph.get("phase") not in (None, "NEUTRAL"):
            row["wyckoff"] = ph
            if ph["phase"] in ("ACCUMULATION", "MARKUP"):
                confirms_b.append("WYCKOFF_PHASE")
            elif ph["phase"] in ("DISTRIBUTION", "MARKDOWN"):
                confirms_t.append("WYCKOFF_PHASE")
        if tk in ins_buy:
            row["insider"] = "BUY_CLUSTER"
            confirms_b.append("INSIDERS")
        elif tk in ins_sell:
            row["insider"] = "SELL_CLUSTER"
            confirms_t.append("INSIDERS")
        side_b = row["flag"] == "LIKELY_BOTTOM" or \
            row["phase"] == "ACCUMULATION"
        side_t = row["flag"] == "LIKELY_TOP" or \
            row["phase"] == "DISTRIBUTION"
        row["confirms"] = confirms_b if side_b else \
            confirms_t if side_t else []
        row["confirm_n"] = len(row["confirms"])
    joined = {"dark_pool": len(dp_map), "whales": len(wh_map),
              "wyckoff": len(ph_map),
              "insiders": len(ins_buy) + len(ins_sell)}
    print("[join] coverage %s" % joined)

    out = {
        "engine": "accumulation-radar", "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "duration_s": round(time.time() - t0, 1), "buffer_days": len(buf["dates"]),
        "n_scored": len(rows), "days_added": added,
        "market_context": {"capitulation_signal": _cap.get("signal"), "capitulation_score": _cap_score,
                           "market_washout": market_washout, "consensus_bottom_names": len(cb_set),
                           "note": ("Tape is capitulating — bottoms are more reliable here." if market_washout
                                    else "No broad capitulation; treat single-name bottoms on their own merit.")},
        "thesis": ("Wyckoff cycle radar — OBV + Chaikin money flow + RSI + position vs moving averages + "
                   "price/volume divergence classify each stock, ETF and country into accumulation, markup, "
                   "distribution or markdown, and flag the names most likely at a top or a bottom."),
        "legend": {"LIKELY_TOP": "over-extended + distribution / bearish divergence",
                   "LIKELY_BOTTOM": "oversold + accumulation / bullish divergence",
                   "cmf": "Chaikin Money Flow 20d (+ = buying pressure)",
                   "obv_trend": "OBV 20d change in days-of-volume (+ = volume confirming)"},
        "tops": {"stocks": by("stock", "top_score", "LIKELY_TOP"),
                 "etfs": by("etf", "top_score", "LIKELY_TOP"),
                 "countries": by("country", "top_score", "LIKELY_TOP")},
        "bottoms": {"stocks": by("stock", "bottom_score", "LIKELY_BOTTOM"),
                    "etfs": by("etf", "bottom_score", "LIKELY_BOTTOM"),
                    "countries": by("country", "bottom_score", "LIKELY_BOTTOM")},
        "accumulating": {"stocks": [r for r in by("stock", "bottom_score") if r["phase"] == "ACCUMULATION"][:15],
                         "etfs": [r for r in by("etf", "bottom_score") if r["phase"] == "ACCUMULATION"][:12],
                         "countries": [r for r in by("country", "bottom_score") if r["phase"] == "ACCUMULATION"][:12]},
        "distributing": {"stocks": [r for r in by("stock", "top_score") if r["phase"] == "DISTRIBUTION"][:15],
                         "etfs": [r for r in by("etf", "top_score") if r["phase"] == "DISTRIBUTION"][:12],
                         "countries": [r for r in by("country", "top_score") if r["phase"] == "DISTRIBUTION"][:12]},
        "market_leaders": market_leaders,
        "leaders_fading": leaders_fading,
        "confirmed_bottoms": sorted(
            [r for r in rows
             if (r["flag"] == "LIKELY_BOTTOM"
                 or r["phase"] == "ACCUMULATION")
             and r.get("confirm_n", 0) >= 2],
            key=lambda r: (-r["confirm_n"], -r["bottom_score"]))[:15],
        "confirmed_tops": sorted(
            [r for r in rows
             if (r["flag"] == "LIKELY_TOP"
                 or r["phase"] == "DISTRIBUTION")
             and r.get("confirm_n", 0) >= 2],
            key=lambda r: (-r["confirm_n"], -r["top_score"]))[:15],
        "join_coverage": joined,
        # v1.3.1: compact per-name MA state for fleet-wide breadth
        # joins (industry-rotation internal breadth etc.)
        "ma_state": {r["ticker"]: [1 if (r.get("pct_vs_50dma") or 0) > 0
                                   else 0,
                                   1 if (r.get("pct_vs_200dma") or 0) > 0
                                   else 0]
                     for r in rows if r.get("pct_vs_50dma") is not None},
    }

    # ══ v1.4.0 REVERSAL TRANSITIONS — the fresh turn, volume-proven ══
    # Wyckoff spring/UTAD transitions; Weinstein Stage 1->2 / 3->4
    # across the long MA on volume; O'Neil/IBD >=150% breakout volume
    # + distribution-day clusters; Granville OBV leads price;
    # capitulation = seller exhaustion.
    def _sma_s(c, n, i=None):
        i = len(c) if i is None else i
        if i < n:
            return None
        return sum(c[i - n:i]) / n

    def _obv(c, v):
        o, out_ = 0, [0]
        for i in range(1, len(c)):
            o += v[i] if c[i] > c[i - 1] else (-v[i] if c[i] < c[i - 1]
                                               else 0)
            out_.append(o)
        return out_

    def _cross_within(c, fast, slow, k, direction):
        for back in range(1, k + 1):
            i = len(c) - back
            f1, s1 = _sma_s(c, fast, i + 1), _sma_s(c, slow, i + 1)
            f0, s0 = _sma_s(c, fast, i), _sma_s(c, slow, i)
            if None in (f1, s1, f0, s0):
                return None
            if direction == "up" and f0 <= s0 and f1 > s1:
                return back
            if direction == "dn" and f0 >= s0 and f1 < s1:
                return back
        return None

    rev_bottoms, rev_tops = [], []
    dma_up, dma_dn = [], []

    def _v50_at(v, i):
        if i < 50:
            return None
        return sum(v[i - 50:i]) / 50.0 or None

    for r in rows:
        if r.get("class") not in ("stock", "etf"):
            continue
        rec = buf.get("tickers", {}).get(r["ticker"]) or {}
        c, v = rec.get("c") or [], rec.get("v") or []
        if len(c) < 205 or len(v) != len(c) or not c[-1]:
            continue
        sma50 = _sma_s(c, 50)
        sma50_p5 = _sma_s(c, 50, len(c) - 5)
        sma200 = _sma_s(c, 200)
        if None in (sma50, sma50_p5, sma200):
            continue
        hi252 = max(c[-252:] if len(c) >= 252 else c)
        lo252 = min(c[-252:] if len(c) >= 252 else c)
        v50 = sum(v[-51:-1]) / 50.0 or 1.0
        vr_today = v[-1] / v50
        upv = sum(v[-i] for i in range(1, 21) if c[-i] > c[-i - 1])
        dnv = sum(v[-i] for i in range(1, 21) if c[-i] < c[-i - 1])
        ud = round(upv / dnv, 2) if dnv else None
        obv = _obv(c, v)
        px_hh = max(c[-20:]) >= max(c[-40:-20])
        obv_hh = max(obv[-20:]) >= max(obv[-40:-20])

        # ── universe-wide 200DMA break scan (v1.4.2, no gates) ──
        for back in range(1, 13):
            i = len(c) - back
            s1 = _sma_s(c, 200, i + 1)
            s0 = _sma_s(c, 200, i)
            if None in (s1, s0):
                break
            crossed_up = c[i - 1] <= s0 and c[i] > s1
            crossed_dn = c[i - 1] >= s0 and c[i] < s1
            if not (crossed_up or crossed_dn):
                continue
            v50b = _v50_at(v, i) or 1.0
            vrb = round(v[i] / v50b, 2)
            row200 = {"ticker": r["ticker"], "class": r["class"],
                      "sessions_ago": back,
                      "vol_ratio_on_break": vrb,
                      "vol_confirm": vrb >= 1.5,
                      "pct_vs_200dma": round(
                          (c[-1] / (sma200 or c[-1]) - 1) * 100, 1),
                      "still_beyond": (c[-1] > sma200) if crossed_up
                      else (c[-1] < sma200),
                      "phase": r.get("phase"),
                      "radar_flag": r.get("flag")}
            (dma_up if crossed_up else dma_dn).append(row200)
            break  # most recent cross only

        down_ctx = (c[-45] < (_sma_s(c, 200, len(c) - 45) or c[-45])
                    and min(c[-60:]) <= lo252 * 1.03)
        if down_ctx:
            ev, score, dated = [], 0, False
            if min(c[-20:]) > min(c[-40:-20]) * 1.005:
                score += 15
                ev.append("higher low: 20d floor %.2f > prior %.2f"
                          % (min(c[-20:]), min(c[-40:-20])))
            reclaim = (c[-1] > sma50
                       and any(c[-i] < (_sma_s(c, 50, len(c) - i)
                                        or 9e9)
                               for i in range(5, 13)))
            if reclaim:
                score += 20
                dated = True
                ev.append("reclaimed the 50DMA within 12 sessions")
            up200 = (c[-1] > sma200
                     and any(c[-i] < (_sma_s(c, 200, len(c) - i)
                                      or 9e9)
                             for i in range(5, 13)))
            if up200:
                score += 20
                dated = True
                ev.append("BROKE ABOVE the 200DMA within 12 "
                          "sessions (Weinstein Stage 2 transition)")
            if sma50 > sma50_p5:
                score += 10
                ev.append("50DMA slope turned up")
            brk = c[-1] > max(c[-64:-1])
            if brk:
                score += 20
                dated = True
                ev.append("BREAKOUT above the 3-month range")
            volc = (brk or reclaim or up200) and vr_today >= 1.5
            if volc:
                score += 15
                ev.append("volume-confirmed: %.1fx the 50d average "
                          "on the trigger (O'Neil >=1.5x)" % vr_today)
            if ud and ud >= 1.3:
                score += 8
                ev.append("accumulation tape: up/down volume %.2f"
                          % ud)
            if obv_hh and not px_hh:
                score += 8
                ev.append("OBV higher high while price lags "
                          "(Granville bull divergence)")
            gc = _cross_within(c, 50, 200, 15, "up")
            if gc:
                score += 8
                dated = True
                ev.append("GOLDEN CROSS %d session(s) ago "
                          "(lagging confirmation)" % gc)
            lo40 = min(c[-40:])
            lo_i = max(i for i in range(len(c) - 40, len(c))
                       if c[i] == lo40) if lo40 in c[-40:] else None
            capit = (lo_i is not None and v[lo_i] > 2.5 * v50)
            if capit:
                score += 6
                ev.append("capitulation volume at the low "
                          "(seller exhaustion)")
            if dated and score >= 45:
                rev_bottoms.append({
                    "ticker": r["ticker"], "class": r["class"],
                    "score": min(100, score),
                    "tier": "CONFIRMED" if volc else "EARLY",
                    "evidence": ev, "breakout": brk,
                    "broke_200dma_up": bool(up200),
                    "pct_vs_200dma": round(
                        (c[-1] / sma200 - 1) * 100, 1),
                    "vol_confirm": bool(volc),
                    "vol_ratio_today": round(vr_today, 2),
                    "up_down_vol_20d": ud,
                    "obv_bull_div": bool(obv_hh and not px_hh),
                    "golden_cross_sessions_ago": gc,
                    "capitulation": bool(capit),
                    "pct_off_252d_low": round(
                        (c[-1] / lo252 - 1) * 100, 1),
                    "phase": r.get("phase"),
                    "radar_flag": r.get("flag"),
                    "confirm_n": r.get("confirm_n", 0)})
            continue

        up_ctx = (c[-45] > (_sma_s(c, 200, len(c) - 45) or 0)
                  and max(c[-60:]) >= hi252 * 0.97)
        if up_ctx:
            ev, score, dated = [], 0, False
            if max(c[-20:]) < max(c[-40:-20]) * 0.995:
                score += 15
                ev.append("lower high: 20d peak %.2f < prior %.2f"
                          % (max(c[-20:]), max(c[-40:-20])))
            lose = (c[-1] < sma50
                    and any(c[-i] > (_sma_s(c, 50, len(c) - i) or 0)
                            for i in range(5, 13)))
            if lose:
                score += 20
                dated = True
                ev.append("lost the 50DMA within 12 sessions")
            dn200 = (c[-1] < sma200
                     and any(c[-i] > (_sma_s(c, 200, len(c) - i)
                                      or 0)
                             for i in range(5, 13)))
            if dn200:
                score += 20
                dated = True
                ev.append("BROKE BELOW the 200DMA within 12 "
                          "sessions (Weinstein Stage 4 transition)")
            if sma50 < sma50_p5:
                score += 10
                ev.append("50DMA slope rolled over")
            brkdn = c[-1] < min(c[-64:-1])
            if brkdn:
                score += 20
                dated = True
                ev.append("BREAKDOWN below the 3-month range")
            volc = (brkdn or lose or dn200) and vr_today >= 1.5
            if volc:
                score += 15
                ev.append("volume-confirmed: %.1fx the 50d average "
                          "on the break (conviction selling)"
                          % vr_today)
            dist = sum(1 for i in range(1, 26)
                       if len(c) > i + 1
                       and c[-i] < c[-i - 1] * 0.998
                       and v[-i] > v[-i - 1])
            if dist >= 5:
                score += 10
                ev.append("distribution cluster: %d distribution "
                          "days in 25 sessions (IBD >=5)" % dist)
            if px_hh and not obv_hh:
                score += 8
                ev.append("OBV lower high vs price high "
                          "(Granville bear divergence)")
            dc = _cross_within(c, 50, 200, 15, "dn")
            if dc:
                score += 8
                dated = True
                ev.append("DEATH CROSS %d session(s) ago "
                          "(lagging confirmation)" % dc)
            churn = vr_today >= 1.8 and abs(c[-1] / c[-2] - 1) < 0.004
            if churn:
                score += 5
                ev.append("churn: heavy volume, no progress near "
                          "highs (distribution signature)")
            if dated and score >= 45:
                rev_tops.append({
                    "ticker": r["ticker"], "class": r["class"],
                    "score": min(100, score),
                    "tier": "CONFIRMED" if volc else "EARLY",
                    "evidence": ev, "breakdown": brkdn,
                    "broke_200dma_down": bool(dn200),
                    "pct_vs_200dma": round(
                        (c[-1] / sma200 - 1) * 100, 1),
                    "vol_confirm": bool(volc),
                    "vol_ratio_today": round(vr_today, 2),
                    "distribution_days_25": dist,
                    "obv_bear_div": bool(px_hh and not obv_hh),
                    "death_cross_sessions_ago": dc,
                    "pct_off_252d_high": round(
                        (c[-1] / hi252 - 1) * 100, 1),
                    "phase": r.get("phase"),
                    "radar_flag": r.get("flag"),
                    "confirm_n": r.get("confirm_n", 0)})

    rev_bottoms.sort(key=lambda x: -x["score"])
    rev_tops.sort(key=lambda x: -x["score"])
    for arr in (dma_up, dma_dn):
        arr.sort(key=lambda x: (x["sessions_ago"],
                                -x["vol_ratio_on_break"]))
    out["dma200_breaks"] = {
        "up": dma_up[:25], "down": dma_dn[:25],
        "note": ("every scanned name that crossed the 200-day "
                 "moving average within 12 sessions -- no reversal "
                 "gates; vol_confirm = break-day volume >=1.5x its "
                 "own 50d average (institutional participation); "
                 "still_beyond = the break is holding today")}
    out["reversals"] = {
        "bottoms": rev_bottoms[:15], "tops": rev_tops[:15],
        "n_scanned": len(rows),
        "method": {
            "bottom_signals": [
                "context gate: prior downtrend (below the 200DMA 45 "
                "sessions ago) + traded within 3% of the 252d low in "
                "the last 60 sessions",
                "a DATED trigger within 12-15 sessions is required: "
                "50DMA reclaim, a break ABOVE the 200DMA "
                "(Weinstein Stage 2), 3-month breakout, or golden "
                "cross",
                "volume must confirm for the CONFIRMED tier: trigger "
                "volume >=1.5x the 50d average (O'Neil/IBD)"],
            "top_signals": [
                "context gate: prior uptrend + within 3% of the 252d "
                "high in the last 60 sessions",
                "dated trigger: 50DMA loss, a break BELOW the 200DMA "
                "(Weinstein Stage 4), 3-month breakdown, or death "
                "cross within 15 sessions",
                "conviction selling: >=1.5x volume on the break "
                "and/or >=5 distribution days in 25 sessions (IBD)"],
            "volume_guide": [
                "volume = participation and conviction; a move on "
                "expanding volume is institutions, a move on "
                "shrinking volume is drift",
                "breakouts WITHOUT >=1.5x volume fail far more often "
                "-- that is why CONFIRMED requires it",
                "up/down volume ratio >1.3 over 20 sessions = "
                "accumulation tape; <0.8 = distribution tape",
                "OBV (cumulative signed volume) tends to LEAD price "
                "at turns -- divergence flags the reversal early",
                "climactic volume at a low = seller exhaustion; "
                "heavy volume with no progress at a high = churn"],
            "citations": [
                "Wyckoff: springs / UTAD, phase transitions",
                "Weinstein Stage Analysis: 1->2 and 3->4 across the "
                "long-term MA on volume",
                "O'Neil / IBD: >=150% breakout volume; "
                "distribution-day clusters",
                "Granville: On-Balance Volume leads price"]},
    }

    # ── closed loop: log LIKELY_BOTTOM (UP) + LIKELY_TOP (DOWN), graded vs benchmark ──
    try:
        nowt = datetime.now(timezone.utc)
        tbl = boto3.resource("dynamodb", "us-east-1").Table("justhodl-signals")
        logged = 0
        cand = ([(r, "UP", "bottom_score") for cl in ("stocks", "etfs", "countries") for r in out["bottoms"][cl][:4]]
                + [(r, "DOWN", "top_score") for cl in ("stocks", "etfs", "countries") for r in out["tops"][cl][:4]])
        for r, direction, sk in cand:
            bench = "ACWX" if r["class"] == "country" else "SPY"
            tbl.put_item(Item={
                "signal_id": f"cycle-{direction}#{r['ticker']}#{nowt.date().isoformat()}",
                "signal_type": "cycle_top" if direction == "DOWN" else "cycle_bottom",
                "predicted_direction": direction, "signal_value": str(r[sk]),
                "confidence": Decimal("0.55"), "measure_against": "ticker_vs_benchmark",
                "baseline_price": str(r["price"]), "benchmark": bench,
                "check_windows": ["day_5", "day_21", "day_63"], "outcomes": {}, "accuracy_scores": {},
                "status": "pending", "logged_at": nowt.isoformat(), "logged_epoch": int(nowt.timestamp()),
                "horizon_days_primary": 21, "schema_version": "2",
                "ttl": int(nowt.timestamp()) + 120 * 86400,
                "metadata": {"engine": "accumulation-radar", "v": VERSION, "class": r["class"],
                             "phase": r["phase"], "flag": r["flag"]},
                "rationale": f"{r['flag']} {r['ticker']} ({r['class']}) phase {r['phase']} "
                             f"top {r['top_score']} / bottom {r['bottom_score']}"})
            logged += 1
        out["signals_logged"] = logged
    except Exception as e:
        print(f"[loop] {str(e)[:80]}")

    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")
    nt = sum(len(out["tops"][k]) for k in out["tops"])
    nb = sum(len(out["bottoms"][k]) for k in out["bottoms"])
    print(f"[accum-radar] scored={len(rows)} buf={len(buf['dates'])}d tops={nt} bottoms={nb} "
          f"added={added} {out['duration_s']}s")
    return {"statusCode": 200, "body": json.dumps({"scored": len(rows), "tops": nt, "bottoms": nb})}
