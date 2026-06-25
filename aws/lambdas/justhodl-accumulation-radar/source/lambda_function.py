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
VERSION = "1.1.0"
MAXDAYS = 200          # buffer depth
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
    # hot-money cross-corroboration: country -> conviction (so accumulation + foreign flow can agree)
    hm = _read("data/hot-money.json") or {}
    hm_conv = {c.get("country"): c.get("conviction") for c in (hm.get("all_countries") or [])}
    INFLOW_CONV = {"TWIN_ENGINE", "CONFIRMED_INFLOW", "EARLY_ACCUMULATION"}
    OUTFLOW_CONV = {"CONFIRMED_OUTFLOW", "OUTFLOW"}
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
        row = {
            "ticker": tk, "class": cls.get(tk, "stock"), "label": label, "price": round(px, 2),
            "phase": phase, "flag": flag,
            "rsi": rsi, "pct_vs_50dma": round(pct50, 1) if pct50 is not None else None,
            "pct_vs_200dma": round(pct200, 1) if pct200 is not None else None,
            "range_pos_pct": round(rng_pos, 0), "cmf": round(cmf, 3),
            "obv_trend": round(obv_chg20, 1),
            "divergence": ("bearish" if bearish_div else "bullish" if bullish_div else None),
            "vol_surge": round(vol_surge, 2) if vol_surge else None,
            "top_score": round(top_score, 1), "bottom_score": round(bottom_score, 1)}
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

    out = {
        "engine": "accumulation-radar", "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "duration_s": round(time.time() - t0, 1), "buffer_days": len(buf["dates"]),
        "n_scored": len(rows), "days_added": added,
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
