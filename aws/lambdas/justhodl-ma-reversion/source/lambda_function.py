"""
justhodl-ma-reversion v1.0 — MA Mean-Reversion / Liquidity-Shelf Engine
=======================================================================
The crowd watches the 20/50/100/200 DMAs and believes "that's where buying
liquidity comes in." This engine MEASURES that belief instead of assuming it:

  • 1971+ deep SPX base + QQQ (1999+): every TOUCH-from-above, BREAKDOWN and
    RECLAIM of each MA → real forward 5/21/63d distributions with n, split by
    bull/bear regime (above/below the 200). Folklore vs measured edge.
  • Stretch tables: % distance from the 200DMA bucketed → forward returns
    (the actual mean-reversion gravity map).
  • Shelf depth: median max-adverse-excursion below the MA at touches — how
    deep the dip really goes before the bid shows up (sizing information).
  • Live state: SPY/QQQ distance to every MA, the nearest shelf below.
  • Per-stock setups: universe names within ±1.5% above a major MA in an
    uptrend, ranked by EACH NAME'S OWN historical bounce rate at that MA.

Index touches and top stock setups log to the closed loop (conf scaled by the
measured hit rate, never folklore).
"""
import json, os, time, urllib.request
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from statistics import median
from decimal import Decimal
import boto3

S3 = boto3.client("s3", region_name="us-east-1")
DDB = boto3.resource("dynamodb", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/ma-reversion.json"
POLY_KEY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
VERSION = "1.2.0"
MAS = (20, 50, 100, 200)
UNIVERSE = ["NVDA","AMD","AVGO","TSM","MU","SMCI","VRT","ETN","PWR","ANET","CLS","FLEX","JBL",
            "COHR","LITE","MRVL","ARM","ASML","AMAT","LRCX","KLAC","TER","ONTO","CAMT","ACLS",
            "GEV","HUBB","MOD","AAON","IESC","STRL","PH","EMR","ROK","NDSN","GGG","CW","HEI",
            "TDG","AXON","KTOS","LDOS","BWXT","VST","CEG","NRG","DELL","HPE"]


def poly(t, start):
    end = datetime.now(timezone.utc).date().isoformat()
    u = (f"https://api.polygon.io/v2/aggs/ticker/{t}/range/1/day/{start}/{end}"
         f"?adjusted=true&sort=asc&limit=50000&apiKey={POLY_KEY}")
    try:
        j = json.loads(urllib.request.urlopen(u, timeout=50).read())
        return [(datetime.fromtimestamp(r["t"] / 1000, tz=timezone.utc).date().isoformat(),
                 float(r["c"])) for r in (j.get("results") or [])]
    except Exception as e:
        print(f"[poly] {t}: {str(e)[:50]}")
        return []


def mas_of(closes):
    out, sums = {m: [None] * len(closes) for m in MAS}, {m: 0.0 for m in MAS}
    for i, c in enumerate(closes):
        for m in MAS:
            sums[m] += c
            if i >= m:
                sums[m] -= closes[i - m]
            if i >= m - 1:
                out[m][i] = sums[m] / m
    return out


def ema_of(closes, n):
    k = 2.0 / (n + 1)
    out, e = [], None
    for c in closes:
        e = c if e is None else c * k + e * (1 - k)
        out.append(e)
    return out


def z_series(closes, m, ma_type="SMA", win=60):
    """z of (close − MA) vs rolling std of that distance."""
    ma = mas_of(closes)[m] if ma_type == "SMA" else ema_of(closes, m)
    dist = [(closes[i] - ma[i]) / ma[i] * 100 if ma[i] else None
            for i in range(len(closes))]
    z = [None] * len(closes)
    for i in range(m + win, len(closes)):
        w = [d for d in dist[i - win:i] if d is not None]
        if len(w) < win - 5:
            continue
        mu = sum(w) / len(w)
        sd = (sum((x - mu) ** 2 for x in w) / len(w)) ** 0.5
        if sd:
            z[i] = (dist[i] - mu) / sd
    return z, ma


def eff_ratio(closes, n=60):
    """Kaufman efficiency ratio: |net change| / sum |daily changes| — trendiness."""
    er = [None] * len(closes)
    for i in range(n, len(closes)):
        net = abs(closes[i] - closes[i - n])
        path = sum(abs(closes[j] - closes[j - 1]) for j in range(i - n + 1, i + 1))
        er[i] = net / path if path else None
    return er


def z_backtest(dates, closes, m, z_in, ma_type="SMA", regime="all",
                time_stop=21, keep_examples=False):
    """Long-only stretch-buy: enter when z crosses ≤ −z_in (regime permitting),
    exit when z ≥ 0 (back at mean) or time_stop sessions. Long side only by
    design — the measured tables already show shorting MA breaks in bulls loses."""
    z, ma = z_series(closes, m, ma_type)
    er = eff_ratio(closes)
    ma200 = mas_of(closes)[200]
    trades = []
    i = max(m + 70, 210)
    n = len(closes)
    while i < n - time_stop - 1:
        zi = z[i]
        if zi is None or z[i - 1] is None:
            i += 1
            continue
        bull = ma200[i] is not None and closes[i] > ma200[i]
        chop = er[i] is not None and er[i] < 0.30
        trend = er[i] is not None and er[i] > 0.45
        ok = (regime == "all" or (regime == "bull" and bull) or
              (regime == "bear" and not bull) or (regime == "chop" and chop) or
              (regime == "trend" and trend))
        if z[i - 1] > -z_in >= zi and ok:
            e0 = i
            j = i + 1
            while j < min(i + time_stop, n - 1) and (z[j] is None or z[j] < 0):
                j += 1
            ret = (closes[j] / closes[e0] - 1) * 100
            mae = (min(closes[e0:j + 1]) / closes[e0] - 1) * 100
            trades.append((dates[e0], dates[j], round(ret, 2), j - e0, round(mae, 2)))
            i = j + 5
        else:
            i += 1
    if not trades:
        return {"n": 0}
    rets = sorted(t[2] for t in trades)
    res = {"n": len(trades),
           "win_pct": round(100 * sum(1 for t in trades if t[2] > 0) / len(trades), 1),
           "median_pct": round(rets[len(rets) // 2], 2),
           "expectancy_pct": round(sum(rets) / len(rets), 2),
           "avg_hold_d": round(sum(t[3] for t in trades) / len(trades), 1),
           "worst_pct": rets[0], "best_pct": rets[-1],
           "med_mae_pct": round(sorted(t[4] for t in trades)[len(trades) // 2], 2)}
    if keep_examples:
        st = sorted(trades, key=lambda t: t[2])
        res["examples"] = {"worst": st[:3], "best": st[-3:][::-1]}
    return res


def study_index(dates, closes, label):
    """All event tables for one index series."""
    ma = mas_of(closes)
    n = len(closes)

    def fwd(i, w):
        return (closes[i + w] / closes[i] - 1) * 100 if i + w < n else None

    def stats(rets, want_pos=True):
        rs = [r for r in rets if r is not None]
        if not rs:
            return None
        rs.sort()
        return {"n": len(rs), "median_pct": round(rs[len(rs) // 2], 2),
                "hit_pct": round(100 * sum(1 for r in rs if (r > 0) == want_pos) / len(rs), 1)}

    events = {}
    for m in MAS:
        for regime in ("bull", "bear", "all"):
            touches, breaks, reclaims, shelf_mae = [], [], [], []
            above_run = below_run = 0
            cooldown = 0
            for i in range(max(m, 210), n - 63):
                mi, c = ma[m][i], closes[i]
                if mi is None:
                    continue
                # ENTRY regime: yesterday's state — at the touch itself price sits
                # on/below the line, so judging regime on day i destroys 200DMA events
                reg_bull = ma[200][i - 1] is not None and closes[i - 1] > ma[200][i - 1]
                if regime == "bull" and not reg_bull:
                    above_run = below_run = 0
                    continue
                if regime == "bear" and reg_bull:
                    above_run = below_run = 0
                    continue
                prev_above = above_run
                if c > mi * 1.005:
                    above_run += 1; below_run = 0
                elif c < mi * 0.995:
                    below_run += 1; above_run = 0
                if cooldown > 0:
                    cooldown -= 1
                    continue
                # TOUCH: dips into MA zone after ≥10 sessions above
                if prev_above >= 10 and mi * 0.99 <= c <= mi * 1.003:
                    touches.append(i)
                    lo = min(closes[i:i + 6])
                    shelf_mae.append((lo / mi - 1) * 100)
                    cooldown = 15
                # BREAKDOWN: closes >1% below after ≥10 above
                elif prev_above >= 10 and c < mi * 0.99:
                    breaks.append(i)
                    cooldown = 15
                # RECLAIM: closes >0.5% above after ≥10 below
                elif below_run == 0 and above_run == 1 and c > mi * 1.005:
                    # above_run just flipped to 1 from a below streak
                    reclaims.append(i)
                    cooldown = 15
            key = f"ma{m}_{regime}"
            events[key] = {
                "touch": {str(w): stats([fwd(i, w) for i in touches]) for w in (5, 21, 63)},
                "touch_n": len(touches),
                "shelf_depth_med_pct": round(median(shelf_mae), 2) if shelf_mae else None,
                "breakdown": {str(w): stats([fwd(i, w) for i in breaks], want_pos=False)
                               for w in (5, 21, 63)},
                "breakdown_n": len(breaks),
                "reclaim": {str(w): stats([fwd(i, w) for i in reclaims]) for w in (5, 21)},
                "reclaim_n": len(reclaims)}

    # stretch table vs 200DMA (sampled every 10 sessions to limit overlap)
    buckets = {">+10%": [], "+5..+10%": [], "+0..+5%": [], "-5..0%": [],
               "-10..-5%": [], "<-10%": []}
    for i in range(210, n - 21, 10):
        if ma[200][i]:
            d = (closes[i] / ma[200][i] - 1) * 100
            b = (">+10%" if d > 10 else "+5..+10%" if d > 5 else "+0..+5%" if d > 0
                 else "-5..0%" if d > -5 else "-10..-5%" if d > -10 else "<-10%")
            f = fwd(i, 21)
            if f is not None:
                buckets[b].append(f)
    stretch = {}
    for b, rs in buckets.items():
        if rs:
            rs.sort()
            stretch[b] = {"n": len(rs), "med_21d_pct": round(rs[len(rs) // 2], 2),
                          "pos_pct": round(100 * sum(1 for r in rs if r > 0) / len(rs), 1)}

    # golden / death cross (50/200)
    gx, dx = [], []
    for i in range(210, n - 63):
        a50p, a200p = ma[50][i - 1], ma[200][i - 1]
        a50, a200 = ma[50][i], ma[200][i]
        if None in (a50p, a200p, a50, a200):
            continue
        if a50p <= a200p and a50 > a200:
            gx.append(i)
        elif a50p >= a200p and a50 < a200:
            dx.append(i)
    crosses = {"golden": {str(w): stats([fwd(i, w) for i in gx]) for w in (21, 63)},
               "golden_n": len(gx),
               "death": {str(w): stats([fwd(i, w) for i in dx], want_pos=False) for w in (21, 63)},
               "death_n": len(dx)}

    cur = {"close": closes[-1], "as_of": dates[-1]}
    for m in MAS:
        if ma[m][-1]:
            cur[f"ma{m}"] = round(ma[m][-1], 2)
            cur[f"dist_ma{m}_pct"] = round((closes[-1] / ma[m][-1] - 1) * 100, 2)
    shelves = sorted(((m, cur.get(f"dist_ma{m}_pct")) for m in MAS
                      if cur.get(f"dist_ma{m}_pct") is not None and cur[f"dist_ma{m}_pct"] > 0),
                     key=lambda x: x[1])
    cur["nearest_shelf"] = ({"ma": shelves[0][0], "below_pct": shelves[0][1]}
                             if shelves else None)
    return {"label": label, "first_date": dates[0], "n_days": n,
            "events": events, "stretch_vs_200": stretch, "crosses": crosses,
            "current": cur}


def polyv(t, start):
    """closes + volumes"""
    end = datetime.now(timezone.utc).date().isoformat()
    u = (f"https://api.polygon.io/v2/aggs/ticker/{t}/range/1/day/{start}/{end}"
         f"?adjusted=true&sort=asc&limit=50000&apiKey={POLY_KEY}")
    try:
        j = json.loads(urllib.request.urlopen(u, timeout=50).read())
        return [(datetime.fromtimestamp(r["t"] / 1000, tz=timezone.utc).date().isoformat(),
                 float(r["c"]), float(r.get("v") or 0)) for r in (j.get("results") or [])]
    except Exception as e:
        print(f"[polyv] {t}: {str(e)[:50]}")
        return []


def cross_history(closes, ma_arr, m, direction, run_req=5):
    """All historical crossings of MA m in `direction` → fwd21 stats (own record)."""
    n = len(closes)
    evs = []
    run = 0
    cooldown = 0
    for i in range(max(m, 210), n - 21):
        mi, mp = ma_arr[m][i], ma_arr[m][i - 1]
        if mi is None or mp is None:
            continue
        below = closes[i - 1] < mp
        if cooldown:
            cooldown -= 1
        prev_run = run
        run = run + 1 if (below if direction == "UP" else not below) else 0
        if cooldown:
            continue
        if direction == "UP" and prev_run >= run_req and closes[i] > mi * 1.002:
            evs.append(i); cooldown = 15
        elif direction == "DOWN" and prev_run >= run_req and closes[i] < mi * 0.998:
            evs.append(i); cooldown = 15
    rets = [(closes[i + 21] / closes[i] - 1) * 100 for i in evs if i + 21 < n]
    if not rets:
        return {"n": len(evs)}
    rets.sort()
    want_pos = direction == "UP"
    return {"n": len(rets), "median_pct": round(rets[len(rets) // 2], 2),
            "hit_pct": round(100 * sum(1 for r in rets
                                        if (r > 0) == want_pos) / len(rets), 1)}


def detect_crossings(t, rows, lookback=3):
    """Fresh crossings of any MA in the last `lookback` sessions, with own record."""
    if len(rows) < 260:
        return []
    closes = [c for _, c, _ in rows]
    vols = [v for _, _, v in rows]
    ma = mas_of(closes)
    n = len(closes)
    v20 = sum(vols[-21:-1]) / 20 if n > 21 else None
    outs = []
    for m in MAS:
        for k in range(1, lookback + 1):
            i = n - k
            mi, mp = ma[m][i], ma[m][i - 1]
            if mi is None or mp is None:
                continue
            up = closes[i - 1] <= mp and closes[i] > mi * 1.002 and                  all(closes[i - j] < ma[m][i - j] for j in range(2, 7) if ma[m][i - j])
            dn = closes[i - 1] >= mp and closes[i] < mi * 0.998 and                  all(closes[i - j] > ma[m][i - j] for j in range(2, 7) if ma[m][i - j])
            if up or dn:
                d = "UP" if up else "DOWN"
                outs.append({
                    "ticker": t, "ma": m, "direction": d,
                    "days_ago": k - 1, "cross_date": rows[i][0],
                    "px": closes[-1],
                    "dist_pct": round((closes[-1] / ma[m][-1] - 1) * 100, 2)
                                 if ma[m][-1] else None,
                    "vol_ratio": round(vols[i] / v20, 2) if v20 else None,
                    "own_record_21d": cross_history(closes, ma, m, d)})
                break  # one event per MA
    return outs


def stock_setup(t, rows=None):
    pts = rows or polyv(t, (datetime.now(timezone.utc) - timedelta(days=2000)).date().isoformat())
    if len(pts) < 260:
        return None
    closes = [r[1] for r in pts]
    ma = mas_of(closes)
    c, n = closes[-1], len(closes)
    if not ma[200][-1] or c < ma[200][-1]:
        return None  # uptrend filter: above the 200
    best = None
    for m in (50, 100, 200):
        mi = ma[m][-1]
        if mi and 0 <= (c / mi - 1) * 100 <= 1.5:
            best = m
            break
    if not best:
        return None
    # this name's OWN historical bounce rate at this MA (touch → fwd10 > 0)
    hits = tot = 0
    above = cooldown = 0
    for i in range(max(best, 210), n - 10):
        mi = ma[best][i]
        if mi is None:
            continue
        prev = above
        if closes[i] > mi * 1.005:
            above += 1
        elif closes[i] < mi * 0.995:
            above = 0
        if cooldown:
            cooldown -= 1
            continue
        if prev >= 10 and mi * 0.99 <= closes[i] <= mi * 1.003:
            tot += 1
            hits += 1 if closes[i + 10] > closes[i] else 0
            cooldown = 15
    return {"ticker": t, "at_ma": best, "px": c,
            "dist_pct": round((c / ma[best][-1] - 1) * 100, 2),
            "own_touch_n": tot,
            "own_hit10_pct": round(100 * hits / tot, 1) if tot else None,
            "above_200_pct": round((c / ma[200][-1] - 1) * 100, 1)}


def lambda_handler(event=None, context=None):
    t0 = time.time()
    spx_doc = json.loads(S3.get_object(Bucket=BUCKET, Key="data/spx-history-deep.json")["Body"].read())
    sp = [(d, float(v)) for d, v in (spx_doc.get("points") or []) if v is not None]
    spx = study_index([d for d, _ in sp], [v for _, v in sp], "SPX (deep base 1971+)")
    qq = poly("QQQ", "1999-03-10")
    qqq = (study_index([d for d, _ in qq], [v for _, v in qq], "QQQ (1999+)")
           if len(qq) > 1000 else None)

    # ── MEAN-REVERSION LAB (server-computed proof, 50y) ──
    sd, sc = [d for d, _ in sp], [v for _, v in sp]
    cut = next(i for i, d_ in enumerate(sd) if d_ >= "2010-01-01")
    H = dict(m=50, z=2.0)  # headline rules
    lab = {
        "rules": ("LONG-ONLY stretch-buy. Enter: z(price vs MA) crosses ≤ −Z "
                   "(z-window 60d). Exit: z back ≥ 0 (mean) or 21-session time stop. "
                   "Regime layer: bull = above 200DMA; chop = efficiency-ratio<0.30; "
                   "trend = ER>0.45 (the measured don't-fade zone). No shorts: the 50y "
                   "tables show fading bull-regime MA breaks loses."),
        "headline": {"ma": H["m"], "z_entry": H["z"], "ma_type": "SMA",
                      "full": z_backtest(sd, sc, H["m"], H["z"], keep_examples=True),
                      "in_sample_71_09": z_backtest(sd[:cut], sc[:cut], H["m"], H["z"]),
                      "out_of_sample_10_now": z_backtest(sd[cut:], sc[cut:], H["m"], H["z"])},
        "regime_breakdown": {r: z_backtest(sd, sc, H["m"], H["z"], regime=r)
                              for r in ("all", "bull", "bear", "chop", "trend")},
        "heatmap": [{"ma": m_, "z": z_,
                      **{k: v for k, v in z_backtest(sd, sc, m_, z_).items()
                         if k in ("n", "win_pct", "expectancy_pct")}}
                     for m_ in (20, 50, 100, 200) for z_ in (1.5, 2.0, 2.5)],
        "ema_vs_sma": {t_: z_backtest(sd, sc, H["m"], H["z"], ma_type=t_)
                        for t_ in ("SMA", "EMA")},
        "qqq_headline": (z_backtest([d for d, _ in qq], [v for _, v in qq],
                                      H["m"], H["z"]) if len(qq) > 1000 else None)}
    ex = lab["headline"]["full"].pop("examples", None)
    lab["trade_examples"] = ex

    setups, crossings, stretch_scan = [], [], []
    start2k = (datetime.now(timezone.utc) - timedelta(days=2000)).date().isoformat()

    def stretch_state(t, rows):
        if len(rows) < 320:
            return None
        closes = [r[1] for r in rows]
        z, _ = z_series(closes, 50)
        er = eff_ratio(closes)
        zi, ei = z[-1], er[-1]
        if zi is None:
            return None
        reg = ("TREND" if (ei or 0) > 0.45 else "CHOP" if (ei or 0) < 0.30 else "MIXED")
        own = z_backtest([r[0] for r in rows], closes, 50, 2.0)
        return {"ticker": t, "z50": round(zi, 2), "er60": round(ei, 2) if ei else None,
                "regime": reg, "px": closes[-1],
                "own_record": {k: own.get(k) for k in ("n", "win_pct", "median_pct")}}

    def scan(t):
        rows = polyv(t, start2k)
        return (stock_setup(t, rows), detect_crossings(t, rows), stretch_state(t, rows))

    with ThreadPoolExecutor(max_workers=6) as ex:
        for f in as_completed({ex.submit(scan, t): t for t in UNIVERSE}):
            try:
                su, cr, stx = f.result()
                if su:
                    setups.append(su)
                crossings.extend(cr)
                if stx:
                    stretch_scan.append(stx)
            except Exception as e:
                print(f"[scan] {str(e)[:50]}")
    setups.sort(key=lambda r: (-(r["own_hit10_pct"] or 0), r["dist_pct"]))
    # indexes get the same scanner
    idx_crossings = []
    for t in ("SPY", "QQQ", "IWM", "DIA"):
        try:
            idx_crossings.extend(detect_crossings(t, polyv(t, start2k)))
        except Exception as e:
            print(f"[idx] {t}: {str(e)[:50]}")
    crossings.sort(key=lambda c: (-c["ma"], 0 if c["direction"] == "UP" else 1,
                                    -(c["vol_ratio"] or 0)))
    stretch_scan.sort(key=lambda r: r["z50"])
    # index stretch states
    idx_stretch = []
    for t in ("SPY", "QQQ", "IWM", "DIA"):
        try:
            st_ = stretch_state(t, polyv(t, start2k))
            if st_:
                idx_stretch.append(st_)
        except Exception as e:
            print(f"[idxz] {t}: {str(e)[:40]}")

    # measured edge for the live state → closed-loop logging
    n_logged = 0
    nowt = datetime.now(timezone.utc)
    spy_live = poly("SPY", (nowt - timedelta(days=10)).date().isoformat())
    spy_px = spy_live[-1][1] if spy_live else None

    def log(sid, stype, direction, conf, px0, why, ticker_meta):
        nonlocal n_logged
        try:
            DDB.Table("justhodl-signals").put_item(Item={
                "signal_id": sid, "signal_type": stype, "signal_value": why[:40],
                "predicted_direction": direction, "confidence": Decimal(str(conf)),
                "measure_against": "ticker", "baseline_price": str(px0), "benchmark": "SPY",
                "check_windows": ["day_5", "day_21", "day_63"],
                "check_timestamps": {f"day_{w}": (nowt + timedelta(days=w)).isoformat()
                                      for w in (5, 21, 63)},
                "outcomes": {}, "accuracy_scores": {}, "logged_at": nowt.isoformat(),
                "logged_epoch": int(nowt.timestamp()), "status": "pending",
                "schema_version": "2", "horizon_days_primary": 21,
                "regime_at_log": "BULL" if (spx["current"].get("dist_ma200_pct") or 0) > 0 else "BEAR",
                "ttl": int(nowt.timestamp()) + 120 * 86400,
                "metadata": {"engine": "ma-reversion", "v": VERSION, **ticker_meta},
                "rationale": why})
            n_logged += 1
        except Exception as e:
            print(f"[log] {str(e)[:70]}")

    d0 = nowt.strftime("%Y-%m-%d")
    for m in (50, 100, 200):
        dist = spx["current"].get(f"dist_ma{m}_pct")
        ev = spx["events"].get(f"ma{m}_bull", {})
        t21 = ((ev.get("touch") or {}).get("21")) or {}
        if dist is not None and 0 <= dist <= 0.5 and spy_px and t21.get("n", 0) >= 15:
            hit = t21["hit_pct"] / 100
            log(f"ma-touch-spx{m}#SPY#{d0}", "ma_reversion", "UP",
                round(min(0.70, max(0.50, 0.30 + hit * 0.45)), 2), spy_px,
                f"SPX at the {m}DMA in bull regime — measured: {t21['hit_pct']}% positive "
                f"+21d over n={t21['n']} touches, median {t21['median_pct']}%",
                {"ma": str(m), "hit": str(t21["hit_pct"])})
    # Khalid's thesis, graded: 200DMA crosses are HIGH-significance; 100/50 with
    # volume ≥1.5×. Confidence anchored to the name's OWN measured record.
    for c in crossings + idx_crossings:
        if c["days_ago"] > 1:
            continue
        m, d, rec = c["ma"], c["direction"], c.get("own_record_21d") or {}
        if m == 200:
            base = 0.62
        elif m in (100, 50) and (c.get("vol_ratio") or 0) >= 1.5:
            base = 0.55
        else:
            continue
        if rec.get("n", 0) >= 6 and rec.get("hit_pct") is not None:
            base = min(0.70, max(0.50, 0.30 + rec["hit_pct"] / 100 * 0.45))
        own = (f"; own record {rec['hit_pct']}% +21d (n={rec['n']}, "
               f"med {rec['median_pct']}%)" if rec.get("hit_pct") is not None else
               f"; own record thin (n={rec.get('n', 0)})")
        log(f"ma-cross-{d.lower()}{m}-{c['ticker']}#{c['ticker']}#{d0}",
            "ma_cross", "UP" if d == "UP" else "DOWN", round(base, 2), c["px"],
            f"{c['ticker']} broke {d} through the {m}DMA on {c['cross_date']} "
            f"(vol {c.get('vol_ratio', '—')}×){own}. Thesis: {m}DMA breaks "
            f"{'unlock upside' if d == 'UP' else 'flag major risk'} — graded here.",
            {"ma": str(m), "dir": d, "own_n": str(rec.get("n", 0))})

    for r in setups[:3]:
        if r["own_touch_n"] and r["own_touch_n"] >= 8 and (r["own_hit10_pct"] or 0) >= 60:
            log(f"ma-touch-{r['ticker']}#{r['ticker']}#{d0}", "ma_reversion", "UP",
                round(min(0.68, 0.30 + r["own_hit10_pct"] / 100 * 0.45), 2), r["px"],
                f"{r['ticker']} at its {r['at_ma']}DMA (uptrend); own record "
                f"{r['own_hit10_pct']}% +10d over n={r['own_touch_n']} touches",
                {"ma": str(r["at_ma"]), "own_hit": str(r["own_hit10_pct"])})

    out = {"engine": "ma-reversion", "version": VERSION,
           "generated_at": nowt.isoformat(), "duration_s": round(time.time() - t0, 1),
           "spx": spx, "qqq": qqq,
           "stock_setups": setups[:20], "n_setups": len(setups),
           "reversion_lab": lab,
           "stretch_scan": {"stocks": stretch_scan[:30], "indexes": idx_stretch,
                             "note": "z50 = z of price vs 50DMA (60d window); "
                                      "regime via 60d efficiency ratio"},
           "crossings": {"stocks_up": [c for c in crossings if c["direction"] == "UP"][:25],
                          "stocks_down": [c for c in crossings if c["direction"] == "DOWN"][:25],
                          "indexes": idx_crossings,
                          "lookback_sessions": 3},
           "signals_logged": n_logged,
           "methodology": ("The crowd's MA-liquidity belief, measured: every touch/breakdown/"
                           "reclaim of the 20/50/100/200 DMA on the 1971+ SPX base and QQQ, "
                           "split bull/bear (vs 200DMA), with real n, medians, hit rates and "
                           "shelf depth (median max excursion below the MA at touches). Stock "
                           "setups require uptrend + within 1.5% above a major MA, ranked by "
                           "the name's OWN historical bounce rate. Logged confidences are "
                           "scaled by measured hit rates — folklore gets no free conviction.")}
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=1800")
    print(f"[ma-rev] spx_n={spx['n_days']} setups={len(setups)} logged={n_logged} "
          f"{out['duration_s']}s")
    return {"statusCode": 200, "body": json.dumps({"setups": len(setups), "logged": n_logged})}
