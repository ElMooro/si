"""
justhodl-resilience  ·  v2.0   —   RESILIENCE RADAR
─────────────────────────────────────────────────────────────────────────────
"Price refusing to fall when it had every reason to." Pre-breakout ABSORPTION.

v2.0 upgrades (Levers 1-2):
  · TYPED BAD NEWS — adverse days are no longer just "down tape." Each adverse
    day is classified by TYPE using DATED catalysts:
        EARNINGS_MISS  (earnings-tracker recent_results, eps/rev surprise < 0)
        GUIDANCE_CUT   (analyst-actions guidance_cuts)
        DOWNGRADE      (analyst-actions downgrades, rating_dir=DOWNGRADE)
        PT_CUT         (analyst-actions pt_cuts)
        GAP_DOWN       (open <= 0.985*prev, no catalyst found)
        SECTOR_ROUT    (best-fit sector ETF <= -1.5%)
        MARKET_ROUT    (SPY <= -0.75%)
    Idiosyncratic bad news (a stock RISING the day it misses earnings or gets
    downgraded) carries far more information than holding on a soft tape, so
    each type gets a prior weight; these priors will be REPLACED by forward-
    graded weights once the scorecard matures (Lever 4). Picks are logged with
    their dominant adverse type so the forward record accumulates BY TYPE.
  · MULTI-FACTOR RESIDUAL — abnormal return is the residual of a 2-factor OLS
    on [SPY, best-fit sector ETF], not a single SPY beta. Strips out market AND
    sector lift, so what remains is genuine idiosyncratic strength (kills the
    "just riding a hot sector" false positives). Sector is data-driven: the SPDR
    whose returns the stock correlates with most (no metadata needed).

Stages ABSORBING -> COILED -> IGNITING. Anti-falling-knife AND anti-blow-off
guards. Picks -> harvester for forward excess-vs-SPY grading (measure-before-trust).
"""
import json, time, urllib.request, urllib.parse
from datetime import datetime, timezone, timedelta

VERSION = "2.0"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/resilience.json"
POLY = "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"

import boto3
s3 = boto3.client("s3", "us-east-1")
DIAG = []

SECTOR_ETFS = ["XLK", "XLF", "XLE", "XLV", "XLI", "XLY", "XLP", "XLU", "XLB", "XLRE", "XLC"]
KEEP_ETF = set(SECTOR_ETFS) | {"SPY"}

# adverse-type prior weights (idiosyncratic bad news >> tape). PROVISIONAL - to be
# replaced by forward-graded per-type weights in Lever 4.
TYPE_W = {"EARNINGS_MISS": 1.6, "GUIDANCE_CUT": 1.5, "DOWNGRADE": 1.4, "PT_CUT": 1.35,
          "GAP_DOWN": 1.25, "SECTOR_ROUT": 1.0, "MARKET_ROUT": 0.7}
IDIO_TYPES = {"EARNINGS_MISS", "GUIDANCE_CUT", "DOWNGRADE", "PT_CUT"}


def _poly(path, params=None):
    p = dict(params or {}); p["apiKey"] = POLY
    url = "https://api.polygon.io" + path + "?" + urllib.parse.urlencode(p)
    req = urllib.request.Request(url, headers={"User-Agent": "justhodl/1.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())

def _poly_url(url):
    u = url + ("&" if "?" in url else "?") + "apiKey=" + POLY
    req = urllib.request.Request(u, headers={"User-Agent": "justhodl/1.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())

def _read(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def common_stock_universe(max_pages=8):
    out = set()
    for t in ("CS", "ADRC"):
        url = ("https://api.polygon.io/v3/reference/tickers?type=%s&market=stocks"
               "&active=true&limit=1000" % t)
        pages = 0
        while url and pages < max_pages:
            try:
                j = _poly_url(url)
            except Exception:
                break
            for row in j.get("results", []):
                out.add(row.get("ticker"))
            url = j.get("next_url")
            pages += 1
            time.sleep(0.2)
    return out


def lin_beta(xs, ys):
    n = len(xs)
    if n < 20:
        return None
    mx = sum(xs) / n; my = sum(ys) / n
    cov = sum((xs[i] - mx) * (ys[i] - my) for i in range(n))
    var = sum((x - mx) ** 2 for x in xs)
    return cov / var if var else None

def corr(xs, ys):
    n = len(xs)
    if n < 20:
        return 0.0
    mx = sum(xs) / n; my = sum(ys) / n
    cov = sum((xs[i] - mx) * (ys[i] - my) for i in range(n))
    sx = (sum((x - mx) ** 2 for x in xs)) ** 0.5
    sy = (sum((y - my) ** 2 for y in ys)) ** 0.5
    return cov / (sx * sy) if sx and sy else 0.0

def ols2_resid(y, x1, x2):
    """2-factor OLS residuals: y = a + b1*x1 + b2*x2. Returns (residuals, b1, b2)."""
    n = len(y)
    my = sum(y) / n; m1 = sum(x1) / n; m2 = sum(x2) / n
    cy = [v - my for v in y]; c1 = [v - m1 for v in x1]; c2 = [v - m2 for v in x2]
    S11 = sum(v * v for v in c1); S22 = sum(v * v for v in c2)
    S12 = sum(c1[i] * c2[i] for i in range(n))
    S1y = sum(c1[i] * cy[i] for i in range(n)); S2y = sum(c2[i] * cy[i] for i in range(n))
    det = S11 * S22 - S12 * S12
    if abs(det) < 1e-12:
        b1 = (S1y / S11) if S11 else 0.0; b2 = 0.0
    else:
        b1 = (S22 * S1y - S12 * S2y) / det
        b2 = (S11 * S2y - S12 * S1y) / det
    resid = [cy[i] - b1 * c1[i] - b2 * c2[i] for i in range(n)]
    return resid, b1, b2

def clamp(v, lo=0.0, hi=100.0):
    return max(lo, min(hi, v))


def lambda_handler(event, context):
    t0 = time.time()
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=210)

    spy = _poly("/v2/aggs/ticker/SPY/range/1/day/%s/%s" % (start, end),
                {"adjusted": "true", "sort": "asc", "limit": 400})
    spy_rows = spy.get("results", [])
    alldays = [datetime.fromtimestamp(r["t"] / 1000, tz=timezone.utc).date().isoformat() for r in spy_rows]
    spy_close = {alldays[i]: spy_rows[i]["c"] for i in range(len(alldays))}
    days = alldays[-130:]
    if len(days) < 60:
        return {"statusCode": 500, "body": "insufficient SPY history"}
    dayset = set(days)
    DIAG.append("calendar %d days %s->%s" % (len(days), days[0], days[-1]))

    cs = common_stock_universe()
    DIAG.append("CS/ADRC %d" % len(cs))

    bars = {}
    for d in days:
        try:
            j = _poly("/v2/aggs/grouped/locale/us/market/stocks/%s" % d, {"adjusted": "true"})
        except Exception:
            continue
        for row in j.get("results", []):
            T = row.get("T")
            if T not in KEEP_ETF and cs and T not in cs:
                continue
            o, h, l, c, v = row.get("o"), row.get("h"), row.get("l"), row.get("c"), row.get("v")
            if None in (o, h, l, c, v) or c <= 0:
                continue
            bars.setdefault(T, {})[d] = (o, h, l, c, v)
        time.sleep(0.05)
    DIAG.append("matrix %d tickers" % len(bars))

    spy_ret = {}
    for i in range(1, len(days)):
        a, b = spy_close.get(days[i - 1]), spy_close.get(days[i])
        if a and b:
            spy_ret[days[i]] = b / a - 1
    etf_ret = {}
    for e in SECTOR_ETFS:
        sd = bars.get(e, {})
        ds = [d for d in days if d in sd]
        rr = {}
        for i in range(1, len(ds)):
            rr[ds[i]] = sd[ds[i]][3] / sd[ds[i - 1]][3] - 1
        if len(rr) > 50:
            etf_ret[e] = rr

    def to_tradingday(dstr):
        if dstr in dayset:
            return dstr
        cand = [d for d in days if d >= dstr]
        return min(cand) if cand else None

    catalysts = {}
    def add_cat(tk, dstr, typ, window1=False):
        if not tk or not dstr:
            return
        td = to_tradingday(dstr[:10])
        if td:
            catalysts.setdefault(tk, {}).setdefault(td, typ)
            if window1:
                idx = days.index(td)
                if idx + 1 < len(days):
                    catalysts.setdefault(tk, {}).setdefault(days[idx + 1], typ)

    et = _read("data/earnings-tracker.json") or {}
    n_miss = 0
    for it in (et.get("recent_results_30d") or []):
        eps = it.get("eps_surprise_pct"); rev = it.get("revenue_surprise_pct")
        if (isinstance(eps, (int, float)) and eps < -0.5) or (isinstance(rev, (int, float)) and rev < -0.5):
            add_cat(it.get("ticker"), it.get("filing_date") or it.get("period_end", ""), "EARNINGS_MISS", window1=True)
            n_miss += 1
    aa = _read("data/analyst-actions.json") or {}
    for it in (aa.get("downgrades") or []):
        if str(it.get("rating_dir", "")).upper() == "DOWNGRADE":
            add_cat(it.get("ticker"), it.get("date", ""), "DOWNGRADE")
    for it in (aa.get("pt_cuts") or []):
        add_cat(it.get("ticker"), it.get("date", ""), "PT_CUT")
    for it in (aa.get("guidance_cuts") or []):
        add_cat(it.get("ticker"), it.get("date", ""), "GUIDANCE_CUT")
    DIAG.append("catalysts: %d miss, %d dg, %d ptcut, %d gcut; %d tickers" % (
        n_miss, len(aa.get("downgrades") or []), len(aa.get("pt_cuts") or []),
        len(aa.get("guidance_cuts") or []), len(catalysts)))

    recent = days[-20:]
    universe = []
    for T, sd in bars.items():
        if T in KEEP_ETF:
            continue
        rd = [sd[d] for d in recent if d in sd]
        if len(rd) < 15:
            continue
        last_c = rd[-1][3]; dvol = sum(x[3] * x[4] for x in rd) / len(rd)
        if 3 <= last_c <= 2500 and dvol >= 25e6:
            universe.append(T)
    DIAG.append("liquid universe %d" % len(universe))

    results = []
    for T in universe:
        sd = bars[T]
        ds = [d for d in days if d in sd]
        if len(ds) < 70:
            continue
        closes = [sd[d][3] for d in ds]
        vols = [sd[d][4] for d in ds]
        rs, ms, rdates = [], [], []
        for i in range(1, len(ds)):
            d0, d1 = ds[i - 1], ds[i]
            if d1 not in spy_ret:
                continue
            rs.append(sd[d1][3] / sd[d0][3] - 1); ms.append(spy_ret[d1]); rdates.append(d1)
        if len(rs) < 50:
            continue

        best_etf, best_c = None, 0.20
        for e, rr in etf_ret.items():
            es = [rr.get(d, 0.0) for d in rdates]
            c = corr(es, rs)
            if c > best_c:
                best_c, best_etf = c, e
        if best_etf:
            es = [etf_ret[best_etf].get(d, 0.0) for d in rdates]
            resid, b_mkt, b_sec = ols2_resid(rs, ms, es)
            basis = "SPY+%s" % best_etf
        else:
            beta0 = lin_beta(ms, rs) or 1.0
            resid = [rs[i] - beta0 * ms[i] for i in range(len(rs))]
            b_mkt, b_sec = beta0, 0.0
            basis = "SPY"

        beta_mkt = lin_beta(ms, rs) or 1.0
        dn = [(ms[i], rs[i]) for i in range(len(ms)) if ms[i] < 0]
        up = [(ms[i], rs[i]) for i in range(len(ms)) if ms[i] > 0]
        downbeta = lin_beta([x[0] for x in dn], [x[1] for x in dn]) if len(dn) >= 20 else beta_mkt
        upbeta = lin_beta([x[0] for x in up], [x[1] for x in up]) if len(up) >= 20 else beta_mkt
        asym = (upbeta - downbeta) if (upbeta is not None and downbeta is not None) else 0.0

        sec_rr = etf_ret.get(best_etf, {}) if best_etf else {}
        cats = catalysts.get(T, {})
        n = len(rs)
        vmean = sum(vols) / len(vols)
        vstd = (sum((v - vmean) ** 2 for v in vols) / len(vols)) ** 0.5 or 1.0

        adverse = []
        by_type = {}
        events_shrugged = []
        for i in range(n):
            d1 = rdates[i]; idx = ds.index(d1)
            prev_c = sd[ds[idx - 1]][3]; o1 = sd[d1][0]
            gapped = o1 <= prev_c * 0.985
            typ = None
            if d1 in cats:
                typ = cats[d1]
            elif gapped:
                typ = "GAP_DOWN"
            elif best_etf and sec_rr.get(d1, 0) <= -0.015:
                typ = "SECTOR_ROUT"
            elif ms[i] <= -0.0075:
                typ = "MARKET_ROUT"
            if typ is None:
                continue
            w = TYPE_W.get(typ, 1.0) * (0.5 + 0.5 * (i / n))
            adverse.append((typ, resid[i], w, d1, rs[i] * 100))
            bt = by_type.setdefault(typ, {"n": 0, "held": 0, "sum_resid": 0.0})
            bt["n"] += 1; bt["sum_resid"] += resid[i]
            if resid[i] > 0:
                bt["held"] += 1
            if typ in IDIO_TYPES and rs[i] > -0.01:
                events_shrugged.append({"type": typ, "date": d1, "day_return_pct": round(rs[i] * 100, 1),
                                        "abnormal_pct": round(resid[i] * 100, 2)})

        n_adv = len(adverse)
        if n_adv < 5:
            continue
        wsum = sum(a[2] for a in adverse)
        mean_abn = sum(a[1] * a[2] for a in adverse) / wsum
        hit = sum(1 for a in adverse if a[1] > 0) / n_adv
        advolz = [(vols[ds.index(a[3])] - vmean) / vstd for a in adverse]
        volz = sum(advolz) / len(advolz) if advolz else 0.0

        ma50 = sum(closes[-50:]) / 50 if len(closes) >= 50 else sum(closes) / len(closes)
        ma200 = sum(closes[-200:]) / len(closes[-200:]) if len(closes) >= 120 else ma50
        last = closes[-1]; lo60 = min(closes[-60:]); hi60 = max(closes[-60:])
        above50 = last > ma50; above200 = last > ma200
        dist_low = (last / lo60 - 1) if lo60 else 0
        near_high = (last / hi60) if hi60 else 0
        ret20 = closes[-1] / closes[-21] - 1 if len(closes) >= 21 else 0
        def rv(wn):
            seg = rs[-wn:]
            if len(seg) < 5:
                return None
            m = sum(seg) / len(seg)
            return (sum((x - m) ** 2 for x in seg) / len(seg)) ** 0.5
        rv10, rv60 = rv(10), rv(60)
        coil = (1 - rv10 / rv60) if (rv10 and rv60 and rv60 > 0) else 0.0

        if mean_abn <= 0 or dist_low < 0.05 or ret20 < -0.18:
            continue

        s_abn = clamp(50 + mean_abn * 100 * 23)
        s_hit = clamp(hit * 100)
        s_asym = clamp(50 + asym * 40)
        s_vol = clamp(50 + volz * 20)
        s_struct = clamp(40 + (18 if above50 else 0) + (18 if above200 else 0) + min(24, dist_low * 60))
        s_coil = clamp(50 + coil * 80)
        resilience = round(0.30 * s_abn + 0.18 * s_hit + 0.16 * s_asym
                           + 0.12 * s_vol + 0.14 * s_struct + 0.10 * s_coil, 1)

        idio_n = sum(by_type.get(t, {}).get("n", 0) for t in IDIO_TYPES)
        idio_held = sum(by_type.get(t, {}).get("held", 0) for t in IDIO_TYPES)
        if idio_n >= 1 and idio_held >= 1:
            resilience = round(min(100, resilience + 4 + 2 * min(idio_held, 3)), 1)
        dominant_type = (max(by_type.items(), key=lambda kv: TYPE_W.get(kv[0], 1) * kv[1]["n"])[0]
                         if by_type else None)

        breaking = last >= hi60 * 0.995
        coiled = coil >= 0.20 and near_high >= 0.90
        consistent = hit >= 0.55
        blowoff = ret20 > 0.60
        if blowoff:
            stage = "WATCH"
        elif resilience >= 62 and breaking and consistent:
            stage = "IGNITING"
        elif resilience >= 60 and coiled and consistent:
            stage = "COILED"
        elif resilience >= 57 and hit >= 0.50:
            stage = "ABSORBING"
        else:
            stage = "WATCH"
        if stage == "WATCH":
            continue

        type_breakdown = {t: {"n": v["n"], "held_pct": round(100 * v["held"] / v["n"]),
                              "mean_abn_pct": round(100 * v["sum_resid"] / v["n"], 2)}
                          for t, v in by_type.items()}
        results.append({
            "ticker": T, "resilience": resilience, "stage": stage,
            "price": round(last, 2), "ret_20d_pct": round(ret20 * 100, 1),
            "abnormal_basis": basis,
            "mean_abnormal_on_adverse_pct": round(mean_abn * 100, 2),
            "adverse_hit_rate_pct": round(hit * 100), "n_adverse_days": n_adv,
            "dominant_adverse_type": dominant_type,
            "has_idiosyncratic_evidence": idio_n >= 1 and idio_held >= 1,
            "adverse_by_type": type_breakdown,
            "events_shrugged": events_shrugged[-6:],
            "beta": round(beta_mkt, 2), "sector_loading": round(b_sec, 2),
            "downside_beta": round(downbeta, 2) if downbeta else None,
            "upside_beta": round(upbeta, 2) if upbeta else None, "beta_asymmetry": round(asym, 2),
            "adverse_volume_z": round(volz, 2),
            "above_50d": above50, "above_200d": above200,
            "pct_above_60d_low": round(dist_low * 100), "pct_of_60d_high": round(near_high * 100),
            "coil": round(coil, 2),
            "falsifier": "breaks below 60d low %s on rising volume -> supply wins." % round(lo60, 2),
        })

    results.sort(key=lambda x: x["resilience"], reverse=True)
    booming = [r for r in results if r["stage"] in ("IGNITING", "COILED") and r["resilience"] >= 62]
    booming.sort(key=lambda r: (r["has_idiosyncratic_evidence"], r["resilience"]), reverse=True)

    top_picks = [{"ticker": r["ticker"], "direction": "UP", "resilience": r["resilience"],
                  "stage": r["stage"], "dominant_adverse_type": r["dominant_adverse_type"],
                  "has_idiosyncratic_evidence": r["has_idiosyncratic_evidence"],
                  "reason": ("held +%s%% abnormal (%s) on %s adverse days, dominant %s"
                             % (r["mean_abnormal_on_adverse_pct"], r["abnormal_basis"],
                                r["n_adverse_days"], r["dominant_adverse_type"])),
                  "conviction": "high" if (r["resilience"] >= 70 or r["has_idiosyncratic_evidence"]) else "moderate"}
                 for r in booming[:15]]

    out = {
        "engine": "resilience", "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "duration_s": round(time.time() - t0, 1), "universe_size": len(universe),
        "thesis": ("Stocks posting positive abnormal returns (residual vs SPY + their own sector) on the days "
                   "they should have fallen - and especially on REAL idiosyncratic bad news (earnings miss, "
                   "downgrade, guidance cut). Bad news already priced, strong hand absorbing. The conditional "
                   "asymmetry P(holds | bad news), typed by the kind of bad news, is the tell."),
        "abnormal_basis": "2-factor residual vs [SPY, best-fit sector ETF]",
        "type_weights_provisional": TYPE_W,
        "about_to_boom": booming[:20], "all_resilient": results[:60], "top_picks": top_picks,
        "counts": {"absorbing": sum(1 for r in results if r["stage"] == "ABSORBING"),
                   "coiled": sum(1 for r in results if r["stage"] == "COILED"),
                   "igniting": sum(1 for r in results if r["stage"] == "IGNITING"),
                   "with_idiosyncratic_evidence": sum(1 for r in results if r["has_idiosyncratic_evidence"])},
        "methodology": (
            "Abnormal return = residual of a 2-factor OLS on [SPY, best-fit SPDR sector ETF] (sector chosen by "
            "max return-correlation - strips market AND sector lift). Adverse days are TYPED by dated catalyst: "
            "EARNINGS_MISS (earnings-tracker surprise<0), GUIDANCE_CUT/DOWNGRADE/PT_CUT (analyst-actions, dated), "
            "else GAP_DOWN / SECTOR_ROUT (best-fit ETF <=-1.5%) / MARKET_ROUT (SPY <=-0.75%). Idiosyncratic types "
            "carry higher PROVISIONAL prior weight (to be replaced by forward-graded weights). Resilience (0-100) "
            "= 0.30*type-weighted abnormal-on-adverse + 0.18*hit-rate + 0.16*beta-asymmetry + 0.12*volume "
            "absorption + 0.14*structure + 0.10*coil, with an idiosyncratic-evidence boost. Guards: positive mean "
            "abnormal, >5% above 60d low, 20d>-18%, exclude blow-offs (20d>60%), hit>=55% for COILED/IGNITING. "
            "Picks logged WITH dominant adverse type for forward excess-vs-SPY grading. Research, not advice."),
        "diagnostics": DIAG,
    }
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache, max-age=0")
    print("[resilience v2] universe %d | resilient %d | boom %d | idio %d | top: %s" % (
        len(universe), len(results), len(booming), out["counts"]["with_idiosyncratic_evidence"],
        ", ".join("%s(%s,%s,%s%s)" % (r["ticker"], r["resilience"], r["stage"], r["dominant_adverse_type"],
                                      "*" if r["has_idiosyncratic_evidence"] else "") for r in booming[:8])))
    return {"statusCode": 200, "body": json.dumps({"resilient": len(results), "boom": len(booming),
                                                    "idio": out["counts"]["with_idiosyncratic_evidence"]})}
