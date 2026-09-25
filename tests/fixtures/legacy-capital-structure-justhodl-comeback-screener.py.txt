"""justhodl-comeback-screener v1.0 — the resurrection desk (ops 3288).

Khalid: comeback companies (American-Eagle-style penny→70c multi-baggers
off the floor) were a coverage hole. This engine scans the FULL FinViz
Elite universe (~11,300 names, one authenticated export — zero per-name
API storm) for the crash-then-recover shape, with the BMNR dilution
lesson baked in: a comeback fueled by exploding share count is a TRAP,
not a comeback.

DEFINITION (all from real export fields; alias-defensive parsing)
  crashed     price still ≥50% below 52w high  (asymmetric room left)
  recovering  price ≥ +75% off the 52w low
  tradeable   price ≥ $0.10 and avg $-volume ≥ $300k/day
  confirm     perf(quarter) > 0
TIERS
  EARLY_TURN  off_low 75–200% and below SMA200
  CONFIRMED   above SMA200 (institutional trend reclaimed)
  MOONSHOT    off_low ≥ 300% (the penny→70c class)
DILUTION GUARD (gap #4 cross-wire)
  join data/share-flows.json when covered; else finalists get one FMP
  /stable/income-statement quarter pull → sh_1y_cagr. Shares +40%/yr
  ⇒ DILUTION_FUELED flag, score −35, routed to the dilution_traps
  board instead of the buy boards. Real enterprise comebacks only.
SCORE  0–100: log-scaled off_low + trend confirms + liquidity −
  dilution − distance-to-high-too-small penalty.

OUTPUT  data/comeback-screener.json {boards:{early_turn, confirmed,
  moonshots, dilution_traps}, stats, methodology}
CONSUMED BY  comeback.html (new desk), opportunities.html COMEBACKS
  section. Schedule: Scheduler daily 20:45 UTC (ops 3288).
"""
import json
import math
import os
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import boto3

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"


# ── CENSUS OVERLAY (ops 3550 · CB-CENSUS): conviction / combo / risk / patterns /
# whale-flow per ticker from the Fundamental Census matrix ──
_CENSUS_CACHE = None


def census_idx(s3_client, bucket):
    global _CENSUS_CACHE
    if _CENSUS_CACHE is not None:
        return _CENSUS_CACHE
    import json as _cj
    out = {}
    try:
        mx = _cj.loads(s3_client.get_object(
            Bucket=bucket,
            Key="data/fundamental-census-matrix.json")["Body"].read())
        C = mx.get("cols") or {}
        rk = C.get("risk_score") or []
        xs = sorted(v for v in rk if isinstance(v, (int, float)))
        lo = xs[len(xs)//3] if len(xs) >= 3 else None
        hi = xs[2*len(xs)//3] if len(xs) >= 3 else None
        col = lambda k: C.get(k) or [None] * len(mx.get("tickers") or [])
        for i, t in enumerate(mx.get("tickers") or []):
            pats = [lbl for lbl, k in
                    (("double_bottom", "double_bottom"),
                     ("double_top", "double_top"),
                     ("golden_cross", "golden_cross_10_40w"),
                     ("breakout_20w", "breakout_20w"))
                    if col(k)[i] == 1]
            rv = col("risk_score")[i]
            tier = (None if not isinstance(rv, (int, float)) or lo is None
                    else "LOW" if rv <= lo else "HIGH" if rv >= hi
                    else "MED")
            out[t] = {"conviction": col("conviction_score")[i],
                      "combo": col("combo_score")[i],
                      "risk": rv, "risk_tier": tier,
                      "turn": (mx.get("turn") or [None]*(i+1))[i]
                      if i < len(mx.get("turn") or []) else None,
                      "patterns": pats,
                      "whale_usd_m": col("whale_net_usd_m")[i]}
    except Exception as _e:  # noqa: BLE001
        print("[census-overlay]", str(_e)[:80])
    _CENSUS_CACHE = out
    return out

OUT_KEY = "data/comeback-screener.json"
SCHEMA = "1.0"
S3 = boto3.client("s3", region_name=REGION)
FMP = (os.environ.get("FMP_API_KEY") or os.environ.get("FMP_KEY")
       or "").strip()
UA = {"User-Agent": "Mozilla/5.0 (jh-comeback)"}
MAX_FINALIST_FMP = 60


def g(row, *names):
    """Alias-defensive field getter across FinViz header variants."""
    for n in names:
        for k in (n, n.lower(), n.replace(" ", "_").lower(),
                  n.replace("-", " ").lower()):
            if k in row and row[k] not in (None, "", "-"):
                return row[k]
    lk = {str(k).strip().lower(): v for k, v in row.items()}
    for n in names:
        v = lk.get(n.strip().lower())
        if v not in (None, "", "-"):
            return v
    return None


def num(x):
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).replace(",", "").replace("%", "").strip()
    mult = 1.0
    if s[-1:].upper() in ("K", "M", "B", "T"):
        mult = {"K": 1e3, "M": 1e6, "B": 1e9, "T": 1e12}[s[-1].upper()]
        s = s[:-1]
    try:
        return float(s) * mult
    except Exception:
        return None


def fmp_sh_cagr(tkr):
    """1y share-count CAGR from quarterly diluted shares (5 quarters)."""
    if not FMP:
        return None
    url = ("https://financialmodelingprep.com/stable/income-statement?"
           + urllib.parse.urlencode({"symbol": tkr, "period": "quarter",
                                     "limit": 5, "apikey": FMP}))
    try:
        req = urllib.request.Request(url, headers=UA)
        rows = json.loads(urllib.request.urlopen(
            req, timeout=20).read().decode())
        sh = [r.get("weightedAverageShsOutDil")
              or r.get("weightedAverageShsOut") for r in rows]
        sh = [s for s in sh if s]
        if len(sh) >= 5 and sh[-1]:
            return round((sh[0] / sh[-1] - 1) * 100, 1)
    except Exception:
        pass
    return None


def lambda_handler(event=None, context=None):
    t0 = time.time()
    warns = []
    from finviz import fetch_custom  # shared layer, Elite token via env
    rows = fetch_custom() or []
    if len(rows) < 3000:
        raise RuntimeError("finviz universe too thin: %d" % len(rows))

    sf = {}
    try:
        sfj = json.loads(S3.get_object(
            Bucket=BUCKET, Key="data/share-flows.json")["Body"].read())
        sf = sfj.get("tickers") or {}
    except Exception as e:
        warns.append("share-flows join unavailable: %s" % str(e)[:60])

    # FinViz export "52-Week Low/High" are usually PERCENT distances
    # ("120.45%", "-55.30%"), not prices. Detect semantics on a sample:
    # in price-mode, lo <= px <= hi holds nearly always.
    probe_ok = probe_n = 0
    for r in rows[:400]:
        px_ = num(g(r, "Price", "price"))
        lo_ = num(g(r, "52-Week Low", "52W Low", "52w low", "low_52w"))
        hi_ = num(g(r, "52-Week High", "52W High", "52w high",
                    "high_52w"))
        if px_ and lo_ is not None and hi_ is not None:
            probe_n += 1
            if 0 < lo_ <= px_ * 1.02 and hi_ >= px_ * 0.98:
                probe_ok += 1
    price_mode = probe_n >= 50 and probe_ok / probe_n > 0.85
    warns.append("52w_semantics=%s (probe %d/%d)"
                 % ("price" if price_mode else "percent",
                    probe_ok, probe_n))

    funnel = {"parsed": 0, "tradeable": 0, "comeback": 0, "confirm": 0}
    cands = []
    for r in rows:
        tkr = g(r, "Ticker", "ticker")
        px = num(g(r, "Price", "price"))
        lo = num(g(r, "52-Week Low", "52W Low", "52w low", "low_52w"))
        hi = num(g(r, "52-Week High", "52W High", "52w high",
                   "high_52w"))
        av = num(g(r, "Average Volume", "Avg Volume", "avgvol",
                   "average_volume"))
        vt = num(g(r, "Volume", "volume"))
        # FinViz export sometimes ships Average Volume denominated in
        # thousands; today's Volume is raw shares. Use the max of both
        # interpretations so no liquid name is dropped by a unit quirk.
        av_cands = [x for x in (av, (av or 0) * 1e3 if av and av < 5e4
                                else None, vt) if x]
        av = max(av_cands) if av_cands else None
        if not tkr or not px or lo is None or hi is None or px < 0.10:
            continue
        funnel["parsed"] += 1
        if price_mode:
            if lo <= 0 or hi <= 0:
                continue
            off_low = (px / lo - 1) * 100
            below_high = (px / hi - 1) * 100
        else:
            off_low = lo          # "% above 52w low" straight from export
            below_high = hi       # "% below 52w high" (negative)
        dvol = (av or 0) * px
        if dvol < 3e5:
            continue
        funnel["tradeable"] += 1
        if funnel["tradeable"] <= 3:
            warns.append("liq_sample %s av_raw=%r vol_raw=%r px=%s"
                         % (tkr, g(r, "Average Volume", "Avg Volume"),
                            g(r, "Volume", "volume"), px))
        if off_low < 75 or below_high > -50:
            continue
        funnel["comeback"] += 1
        pq = num(g(r, "Performance (Quarter)", "Perf Quart",
                   "perf_quarter", "performance quarter"))
        if pq is not None and pq <= 0:
            continue
        funnel["confirm"] += 1
        s200 = num(g(r, "200-Day Simple Moving Average", "SMA200",
                     "sma200", "200-day simple moving average"))
        s50 = num(g(r, "50-Day Simple Moving Average", "SMA50",
                    "sma50", "50-day simple moving average"))
        cands.append({
            "ticker": tkr,
            "company": g(r, "Company", "company") or "",
            "sector": g(r, "Sector", "sector") or "",
            "industry": g(r, "Industry", "industry") or "",
            "price": round(px, 3),
            "off_low_pct": round(off_low, 1),
            "below_high_pct": round(below_high, 1),
            "dollar_vol": round(dvol),
            "market_cap": num(g(r, "Market Cap", "market_cap",
                                "marketcap")),
            "above_sma200": bool(s200 is not None and s200 > 0),
            "above_sma50": bool(s50 is not None and s50 > 0),
            "perf_q_pct": pq,
            "perf_h_pct": num(g(r, "Performance (Half Year)",
                                "perf_half_y", "perf half year")),
        })

    cands.sort(key=lambda x: -x["off_low_pct"])
    print("universe=%d raw_candidates=%d funnel=%s"
          % (len(rows), len(cands), funnel))
    warns.append("funnel=%s" % funnel)

    # dilution guard: share-flows join first, FMP for uncovered finalists
    fmp_used = 0
    for c in cands:
        row = sf.get(c["ticker"]) or {}
        cagr = row.get("sh_1y_cagr_pct")
        if cagr is None:
            cagr = row.get("sh_3y_cagr_pct")
        if cagr is None and fmp_used < MAX_FINALIST_FMP:
            cagr = fmp_sh_cagr(c["ticker"])
            fmp_used += 1
            time.sleep(0.15)
        c["sh_1y_cagr_pct"] = cagr
        c["dilution_fueled"] = bool(cagr is not None and cagr >= 40)
        c["dilution_note"] = (
            "share count +%.0f%%/yr — price recovery is being paid for "
            "with your ownership (BMNR pattern)" % cagr
            if c["dilution_fueled"] else
            ("shares %+.1f%%/yr" % cagr if cagr is not None
             else "share data pending"))

    for c in cands:
        s = 25 * min(1.0, math.log10(max(
            1.0, c["off_low_pct"])) / math.log10(1000))
        s += 20 if c["above_sma200"] else 0
        s += 10 if c["above_sma50"] else 0
        s += 10 if (c["perf_h_pct"] or 0) > 0 else 0
        s += 10 if c["dollar_vol"] >= 2e6 else (
            5 if c["dollar_vol"] >= 7.5e5 else 0)
        s += 15 * min(1.0, abs(c["below_high_pct"]) / 90.0)
        s += 10 if (c["sh_1y_cagr_pct"] is not None
                    and c["sh_1y_cagr_pct"] <= 5) else 0
        if c["dilution_fueled"]:
            s -= 35
        c["comeback_score"] = round(max(0, min(100, s)), 1)
        if c["dilution_fueled"]:
            c["tier"] = "DILUTION_TRAP"
        elif c["off_low_pct"] >= 300:
            c["tier"] = "MOONSHOT"
        elif c["above_sma200"]:
            c["tier"] = "CONFIRMED"
        else:
            c["tier"] = "EARLY_TURN"
        c["read"] = ("+%.0f%% off the 52w low, still %.0f%% below the "
                     "52w high — %s. %s." % (
                         c["off_low_pct"], abs(c["below_high_pct"]),
                         "trend reclaimed (>SMA200)"
                         if c["above_sma200"]
                         else "trend not yet reclaimed",
                         c["dilution_note"]))

    _cidx = census_idx(S3, BUCKET if "BUCKET" in globals() else
                       "justhodl-dashboard-live")
    for _c in cands:
        _cc = _cidx.get(str(_c.get("ticker") or "").upper())
        if _cc:
            _c["census_context"] = _cc

    def board(tier, n=25):
        b = [c for c in cands if c["tier"] == tier]
        b.sort(key=lambda x: -x["comeback_score"])
        return b[:n]

    out = {
        "schema": SCHEMA, "engine": "justhodl-comeback-screener",
        "as_of": datetime.now(timezone.utc).isoformat(),
        "universe_n": len(rows), "candidates_n": len(cands),
        "fmp_dilution_pulls": fmp_used, "warns": warns,
        "boards": {"confirmed": board("CONFIRMED"),
                   "early_turn": board("EARLY_TURN"),
                   "moonshots": board("MOONSHOT"),
                   "dilution_traps": board("DILUTION_TRAP", 15)},
        "methodology": (
            "Full FinViz universe scan. Comeback = ≥+75% off the 52w "
            "low while still ≥50% below the 52w high (crashed, "
            "recovering, asymmetric room left), positive quarter, "
            "≥$300k/day traded. Tiers: EARLY_TURN (below SMA200), "
            "CONFIRMED (SMA200 reclaimed), MOONSHOT (≥+300% off low). "
            "Dilution guard: share count ≥+40%/yr reroutes the name to "
            "DILUTION_TRAPS — the BMNR lesson: per-share recovery "
            "funded by issuance is not a comeback."),
        "duration_s": round(time.time() - t0, 1),
    }
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out).encode(),
                  ContentType="application/json",
                  CacheControl="public, max-age=900")
    print("boards: " + json.dumps({k: len(v) for k, v
                                   in out["boards"].items()}))
    return {"statusCode": 200,
            "body": json.dumps({"ok": True,
                                "candidates": len(cands)})}
