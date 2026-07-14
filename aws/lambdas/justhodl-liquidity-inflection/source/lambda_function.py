"""
justhodl-liquidity-inflection v1.0 — Liquidity 2nd-Derivative + Lead/Lag Table
==============================================================================
Item 6/7/8 of the edge map. Levels are consensus; INFLECTIONS are the trade.

  USD net liquidity  = WALCL − TGA − RRP (FRED, daily-aligned, WALCL ffilled)
  EUR excess liq     = platform ecb-hist series (probe)
  CN credit impulse  = BIS credit-to-private-nonfin China YoY accel (probe)
  Stablecoin accel   = platform stablecoin-flow brief 2nd derivative (probe)

  Impulse  = 13-week regression slope of net liquidity, z-scored 3y
  Flip     = impulse sign change with |Δz| ≥ 0.25 debounce
  Edge     = event-study of historical flips → forward 5/21/63d distribution
             for SPX (deep base), BTC, HYG — the published lead/lag table.
New flips (≤5 sessions old) are logged to the closed loop vs SPY.
"""
import json, os, time, urllib.request, urllib.parse, bisect
from datetime import datetime, timezone, timedelta
from statistics import mean, stdev
from decimal import Decimal
import boto3

S3 = boto3.client("s3", region_name="us-east-1")
DDB = boto3.resource("dynamodb", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/liquidity-inflection.json"
FRED_KEY = os.environ.get("FRED_KEY", "2f057499936072679d8843d7fce99989")
POLY_KEY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
VERSION = "2.5.0"
W_SLOPE = 65          # ~13 weeks of business days
Z_LOOKBACK = 756      # 3y
DEBOUNCE = 0.25


def fred(sid, start="2010-01-01"):
    u = ("https://api.stlouisfed.org/fred/series/observations?"
         + urllib.parse.urlencode({"series_id": sid, "api_key": FRED_KEY,
                                   "file_type": "json", "observation_start": start, "limit": 100000}))
    try:
        j = json.loads(urllib.request.urlopen(u, timeout=40).read())
        return {o["date"]: float(o["value"]) for o in j.get("observations", [])
                if o.get("value") not in (".", "", None)}
    except Exception as e:
        print(f"[fred] {sid}: {str(e)[:60]}")
        return {}


def brain_predictors():
    """Two operator-documented liquidity LEADS mined from the brain, wired ahead of the
    engine's own net-liquidity impulse:
      (1) 10y HQM corporate spread WIDENING front-runs liquidity trend reversals;
      (2) a sharp 30y Treasury-yield DUMP has historically preceded QE."""
    import bisect
    out = {}
    # (1) HQM 10y - Treasury 10y quality-credit spread + its trend
    try:
        hqm = fred("HQMCB10YR", "2005-01-01")     # monthly {date: yield}
        t10 = fred("DGS10", "2005-01-01")          # daily
        if hqm and t10:
            t10s = sorted(t10.items()); tdates = [d for d, _ in t10s]
            spread = {}
            for d, v in sorted(hqm.items()):
                i = bisect.bisect_right(tdates, d) - 1
                if i >= 0:
                    spread[d] = round(v - t10s[i][1], 3)
            sd = sorted(spread)
            if len(sd) >= 40:
                cur = spread[sd[-1]]
                d3 = (spread[sd[-1]] - spread[sd[-4]]) if len(sd) >= 4 else None
                vals = [spread[d] for d in sd[-60:]]
                mu = sum(vals) / len(vals)
                sdv = (sum((x - mu) ** 2 for x in vals) / len(vals)) ** 0.5
                z = round((cur - mu) / sdv, 2) if sdv else None
                widening = (d3 is not None and d3 > 0.05)
                state = ("WIDENING — liquidity-reversal warning" if widening and z and z > 0.5
                         else "WIDENING" if widening
                         else "COMPRESSED — benign" if (z is not None and z < -0.3) else "STABLE")
                out["hqm_reversal_lead"] = {
                    "spread_ppt": cur, "chg_3m_ppt": round(d3, 3) if d3 is not None else None,
                    "z_5y": z, "state": state, "as_of": sd[-1],
                    "read": ("Even top-rated (HQM) issuers are paying a widening premium over Treasuries — "
                             "a documented early tell that the liquidity tide is about to turn down, ahead "
                             "of the net-liquidity impulse itself." if widening else
                             "The HQM-Treasury premium is contained — no early liquidity-reversal warning "
                             "from quality credit.")}
    except Exception as e:
        out["hqm_reversal_lead"] = {"error": str(e)[:80]}
    # (2) 30y Treasury-yield dump -> QE precursor
    try:
        t30 = fred("DGS30", "2005-01-01")
        if t30:
            s = sorted(t30.items()); dts = [d for d, _ in s]; vv = [v for _, v in s]
            cur = vv[-1]
            c3m = round(cur - vv[-63], 1) if len(vv) > 63 else None
            c6m = round(cur - vv[-126], 1) if len(vv) > 126 else None
            dump = (c3m is not None and c3m <= -0.40)
            hard = (c3m is not None and c3m <= -0.70)
            state = ("QE_WATCH — sharp 30y dump" if hard else
                     "QE_LEAN — 30y falling" if dump else "NEUTRAL")
            out["qe_precursor_30y"] = {
                "yield_pct": round(cur, 2), "chg_3m_ppt": c3m, "chg_6m_ppt": c6m,
                "state": state, "as_of": dts[-1],
                "read": ("The long bond is collapsing — a hard 30-year dump reflects a deflation / "
                         "flight-to-quality scramble that has historically forced the Fed toward QE."
                         if hard else
                         "The 30-year yield is easing lower — a mild move; watch for acceleration as a "
                         "QE precursor." if dump else
                         "The 30-year yield is not dumping — no QE-precursor signal.")}
    except Exception as e:
        out["qe_precursor_30y"] = {"error": str(e)[:80]}
    return out


def polygon_closes(ticker, start="2015-01-01"):
    end = datetime.now(timezone.utc).date().isoformat()
    u = (f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start}/{end}"
         f"?adjusted=true&sort=asc&limit=50000&apiKey={POLY_KEY}")
    try:
        j = json.loads(urllib.request.urlopen(u, timeout=45).read())
        return {datetime.fromtimestamp(r["t"] / 1000, tz=timezone.utc).date().isoformat(): float(r["c"])
                for r in (j.get("results") or [])}
    except Exception as e:
        print(f"[poly] {ticker}: {str(e)[:60]}")
        return {}


def s3_json(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def slope(xs):
    n = len(xs)
    if n < 3:
        return 0.0
    xb = (n - 1) / 2; yb = mean(xs)
    den = sum((i - xb) ** 2 for i in range(n))
    return sum((i - xb) * (y - yb) for i, y in enumerate(xs)) / den if den else 0.0


def build_impulse(series):
    """series: {date: level} → sorted dates, impulse z list aligned to dates[W_SLOPE:]"""
    dates = sorted(series)
    vals = [series[d] for d in dates]
    raw = []
    for i in range(W_SLOPE, len(vals)):
        raw.append(slope(vals[i - W_SLOPE:i]))
    z = []
    for i in range(len(raw)):
        w = raw[max(0, i - Z_LOOKBACK):i + 1]
        m = mean(w); sd = stdev(w) if len(w) > 30 else 0
        z.append(round((raw[i] - m) / sd, 3) if sd else 0.0)
    return dates[W_SLOPE:], z


def find_flips(dates, z, th=0.15):
    """Schmitt-trigger hysteresis: a flip is recorded when the impulse crosses
    ±th from the OPPOSITE armed state. Robust to smooth slow-moving z."""
    flips = []
    state = None
    for i, zz in enumerate(z):
        if zz is None:
            continue
        if zz > th and state != "UP":
            if state == "DOWN":
                flips.append({"date": dates[i], "direction": "UP", "z": zz})
            state = "UP"
        elif zz < -th and state != "DOWN":
            if state == "UP":
                flips.append({"date": dates[i], "direction": "DOWN", "z": zz})
            state = "DOWN"
    return flips


def event_study(flips, px, dates_all):
    idx = {d: i for i, d in enumerate(dates_all)}
    out = {}
    for w in (5, 21, 63):
        ups, dns = [], []
        for f in flips:
            d = f["date"]
            j = idx.get(d) or idx.get(min((x for x in dates_all if x >= d), default=None))
            if j is None or j + w >= len(dates_all):
                continue
            p0, p1 = px.get(dates_all[j]), px.get(dates_all[j + w])
            if not p0 or not p1:
                continue
            r = (p1 / p0 - 1) * 100
            (ups if f["direction"] == "UP" else dns).append(r)
        def stats(xs, want_pos):
            if not xs:
                return None
            xs2 = sorted(xs)
            med = xs2[len(xs2) // 2]
            hit = sum(1 for x in xs if (x > 0) == want_pos) / len(xs)
            return {"n": len(xs), "median_pct": round(med, 2), "hit_dir_pct": round(hit * 100, 1)}
        out[f"d{w}"] = {"after_UP_flip": stats(ups, True), "after_DOWN_flip": stats(dns, False)}
    return out


def best_lead(z_dates, z, px, max_k=30):
    """k maximizing corr(impulse_z, fwd 21d return shifted k) — naive lead estimate."""
    pdates = sorted(px)
    pidx = {d: i for i, d in enumerate(pdates)}
    pairs = []
    for d, zz in zip(z_dates, z):
        j = pidx.get(d)
        if j is not None:
            pairs.append((j, zz))
    best = (0, 0.0)
    for k in range(0, max_k + 1):
        xs, ys = [], []
        for j, zz in pairs:
            if j + k + 21 < len(pdates):
                r = px[pdates[j + k + 21]] / px[pdates[j + k]] - 1
                xs.append(zz); ys.append(r)
        if len(xs) > 60:
            mx, my = mean(xs), mean(ys)
            sx = (sum((a - mx) ** 2 for a in xs)) ** .5
            sy = (sum((b - my) ** 2 for b in ys)) ** .5
            c = sum((a - mx) * (b - my) for a, b in zip(xs, ys)) / (sx * sy) if sx and sy else 0
            if abs(c) > abs(best[1]):
                best = (k, round(c, 3))
    return {"lead_days": best[0], "corr_21d_fwd": best[1]}


def _corr(xs, ys):
    n = len(xs)
    if n < 3:
        return 0.0
    mx, my = mean(xs), mean(ys)
    sx = (sum((a - mx) ** 2 for a in xs)) ** .5
    sy = (sum((b - my) ** 2 for b in ys)) ** .5
    return (sum((a - mx) * (b - my) for a, b in zip(xs, ys)) / (sx * sy)) if sx and sy else 0.0


def regime_conditioned_study(z_dates, z, px):
    """Forward returns conditioned on the impulse STATE across ALL history (large n),
    not just the ~4 rare flips. Reports mean/median/hit, EXCESS over the unconditional
    baseline, and a t-stat of that excess so significance is explicit."""
    pdates = sorted(px)
    pidx = {d: i for i, d in enumerate(pdates)}
    H = (5, 21, 63)
    base = {}
    for w in H:
        rs = [(px[pdates[i + w]] / px[pdates[i]] - 1) * 100
              for i in range(len(pdates) - w) if px[pdates[i]] and px[pdates[i + w]]]
        base[w] = mean(rs) if rs else 0.0

    def state_of(zz):
        if zz > 0.5:
            return "EXPANDING_FAST"
        if zz > 0.05:
            return "EXPANDING"
        if zz >= -0.05:
            return "FLAT"
        if zz >= -0.5:
            return "CONTRACTING"
        return "CONTRACTING_FAST"
    buckets = {}
    for d, zz in zip(z_dates, z):
        j = pidx.get(d)
        if j is None:
            continue
        st = state_of(zz)
        for w in H:
            if j + w < len(pdates) and px[pdates[j]] and px[pdates[j + w]]:
                buckets.setdefault(st, {}).setdefault(w, []).append((px[pdates[j + w]] / px[pdates[j]] - 1) * 100)
    states = {}
    for st in ("EXPANDING_FAST", "EXPANDING", "FLAT", "CONTRACTING", "CONTRACTING_FAST"):
        hz = buckets.get(st)
        if not hz:
            continue
        row = {}
        for w in H:
            xs = hz.get(w, [])
            if len(xs) >= 12:
                m = mean(xs)
                sd = stdev(xs) if len(xs) > 1 else 0
                exc = m - base[w]
                t = (exc / (sd / len(xs) ** .5)) if sd else 0.0
                row[f"d{w}"] = {"n": len(xs), "mean": round(m, 2), "median": round(sorted(xs)[len(xs) // 2], 2),
                                "excess": round(exc, 2), "hit_pct": round(sum(1 for x in xs if x > 0) / len(xs) * 100, 1),
                                "t": round(t, 2), "sig": abs(t) >= 2.0}
        if row:
            states[st] = row
    return {"baseline": {f"d{w}": round(base[w], 2) for w in H}, "states": states,
            "n_total": sum(len(v.get(21, [])) for v in buckets.values())}


def lead_curve(z_dates, z, px, max_k=40):
    """Full lead/lag curve corr(impulse_z(t), fwd-21d return at t+k) with t-stats, so a
    real peak is distinguishable from a cherry-picked one."""
    pdates = sorted(px)
    pidx = {d: i for i, d in enumerate(pdates)}
    pairs = [(pidx[d], zz) for d, zz in zip(z_dates, z) if d in pidx]
    curve = []
    for k in range(0, max_k + 1):
        xs, ys = [], []
        for j, zz in pairs:
            if j + k + 21 < len(pdates) and px[pdates[j + k]]:
                xs.append(zz); ys.append(px[pdates[j + k + 21]] / px[pdates[j + k]] - 1)
        if len(xs) > 80:
            c = _corr(xs, ys)
            n = len(xs)
            t = c * (((n - 2) / (1 - c * c)) ** .5) if abs(c) < 0.999 else 0.0
            curve.append({"lead": k, "corr": round(c, 3), "n": n, "t": round(t, 1)})
    best = max(curve, key=lambda r: abs(r["corr"])) if curve else None
    return {"best": best, "curve": curve[::2]}


def flip_log(flips, px, dates_all):
    """The actual flip events, dated, with realized forward returns — the honest small-n view."""
    idx = {d: i for i, d in enumerate(dates_all)}
    rows = []
    for f in flips:
        d = f["date"]
        j = idx.get(d) or idx.get(min((x for x in dates_all if x >= d), default=None))
        if j is None:
            continue
        rec = {"date": d, "direction": f["direction"]}
        for w in (5, 21, 63):
            p0 = px.get(dates_all[j]) if j < len(dates_all) else None
            p1 = px.get(dates_all[j + w]) if j + w < len(dates_all) else None
            rec[f"d{w}"] = round((p1 / p0 - 1) * 100, 2) if (p0 and p1) else None
        rows.append(rec)
    return rows[-8:]


def _pd(s):
    from datetime import date as _date
    y, m, d = s.split("-")
    return _date(int(y), int(m), int(d))


def series_state(series, start_cut="2015-01-01", invert=False):
    """Adaptive level + ~13-week impulse-z + direction + last flip for any {date:level}
    series. Detects cadence (daily/weekly/monthly) and sizes the slope window to ≈13 weeks
    so the 'second derivative' is comparable across feeds."""
    if not series or len(series) < 40:
        return None
    dates = sorted(series)
    vals = [series[d] for d in dates]
    gaps = [(_pd(dates[i]) - _pd(dates[i - 1])).days for i in range(1, min(len(dates), 40))]
    med_gap = sorted(gaps)[len(gaps) // 2] if gaps else 1
    win = max(4, min(90, round(91 / max(med_gap, 1))))      # ~13 weeks ≈ 91 days
    zlb = min(len(vals), max(60, win * 12))
    raw = [slope(vals[i - win:i]) for i in range(win, len(vals))]
    if len(raw) < 10:
        return None
    sd_dates = dates[win:]
    z = []
    for i in range(len(raw)):
        w = raw[max(0, i - zlb):i + 1]
        m = mean(w)
        sd = stdev(w) if len(w) > 20 else 0
        z.append(round((raw[i] - m) / sd, 3) if sd else 0.0)
    flips = [f for f in find_flips(sd_dates, z) if f["date"] >= start_cut]
    zz = z[-1]
    last_d = dates[-1]
    direction = "RISING" if zz > 0.25 else "FALLING" if zz < -0.25 else "FLAT"
    return {"level": round(series[last_d], 2), "as_of": last_d, "impulse_z": round(zz, 3),
            "eff_z": round(-zz if invert else zz, 3), "direction": direction,
            "tail_180": [[d, zv] for d, zv in zip(sd_dates[-180:], z[-180:])],
            "last_flip": flips[-1] if flips else None, "n_flips": len(flips)}


def pull(key):
    return s3_json(key) or {}


def project_net_liquidity(net, walcl, tga, rrp, horizon_wk=13):
    """Project net liquidity forward from recent mechanical flows, reconstructed from the
    component paces (Fed runoff − TGA cash path − RRP) so the headline and driver
    decomposition are internally consistent. √-time band. Mechanical, not a forecast."""
    if not net or len(net) < 14:
        return None
    nd = sorted(net)
    gaps = [(_pd(nd[i]) - _pd(nd[i - 1])).days for i in range(1, min(len(nd), 30))]
    mg = sorted(gaps)[len(gaps) // 2] if gaps else 7
    stride = max(1, round(7 / max(mg, 1)))          # daily→7, weekly→1
    wk_dates = nd[::stride]
    wk_vals = [net[d] for d in wk_dates]
    if len(wk_vals) < 10:
        wk_dates, wk_vals = nd, [net[d] for d in nd]
    cur = wk_vals[-1]

    def wpace(series, in_bn=False, daily=False):
        if not series or len(series) < 8:
            return 0.0
        sd = sorted(series)
        scale = 1000.0 if in_bn else 1.0
        if daily:
            i0 = sd[-31] if len(sd) > 31 else sd[0]
            wks = max(1.0, (len(sd[-31:]) - 1) / 5.0)
            return (series[sd[-1]] - series[i0]) * scale / wks
        return (series[sd[-1]] - series[sd[-7]]) * scale / 6.0
    dW = wpace(walcl)                                # WALCL runoff $M/wk
    dT = wpace(tga)                                  # TGA $M/wk (rising drains)
    rrp_now_bn = rrp[sorted(rrp)[-1]] if rrp else 0.0
    dR = wpace(rrp, in_bn=True, daily=True)          # RRP $M/wk
    # RRP near-empty can only release what's left → cap cumulative contribution
    rrp_room_M = max(0.0, rrp_now_bn) * 1000.0
    dnet = dW - dT - dR                              # reconstructed net-liq Δ/wk ($M)
    contrib = {"walcl": dW, "tga": -dT, "rrp": -dR}
    primary = max(contrib, key=lambda k: abs(contrib[k]))
    pnames = {"walcl": "Fed balance-sheet runoff (QT)", "tga": "Treasury cash (TGA) rebuild",
              "rrp": "RRP drain"}
    alldiffs = [wk_vals[i] - wk_vals[i - 1] for i in range(1, len(wk_vals))]
    vol = stdev(alldiffs) if len(alldiffs) > 5 else abs(dnet) or 1000.0
    DECAY = 0.85
    last_d = _pd(nd[-1])
    path, lvl, rrp_used = [], cur, 0.0
    for w in range(1, horizon_wk + 1):
        step_W = dW * (DECAY ** (w - 1))
        step_T = -dT * (DECAY ** (w - 1))
        step_R = -dR * (DECAY ** (w - 1))
        # cap RRP release at remaining room
        if step_R > 0 and rrp_used + step_R > rrp_room_M:
            step_R = max(0.0, rrp_room_M - rrp_used)
        rrp_used += max(0.0, step_R)
        lvl += step_W + step_T + step_R
        band = vol * (w ** 0.5)
        path.append({"week": w, "date": (last_d + timedelta(weeks=w)).isoformat(),
                     "net_liq_bn": round(lvl / 1000, 1),
                     "lo_bn": round((lvl - band) / 1000, 1), "hi_bn": round((lvl + band) / 1000, 1)})
    hist = [{"date": wk_dates[i], "net_liq_bn": round(wk_vals[i] / 1000, 1)}
            for i in range(max(0, len(wk_dates) - 14), len(wk_dates))]
    chg = (lvl - cur) / 1000
    direction = "fall" if chg < 0 else "rise"
    return {"horizon_weeks": horizon_wk, "current_net_liq_bn": round(cur / 1000, 1),
            "projected_net_liq_bn": round(lvl / 1000, 1), "projected_change_bn": round(chg, 1),
            "weekly_pace_bn": round(dnet / 1000, 1), "history": hist, "path": path,
            "drivers_per_wk_bn": {"walcl_runoff": round(dW / 1000, 2), "tga": round(-dT / 1000, 2),
                                  "rrp": round(-dR / 1000, 2)},
            "primary_driver": pnames[primary],
            "headline": (f"Net liquidity projected to {direction} ~${abs(round(chg)):,}bn over {horizon_wk} weeks "
                         f"(to ~${round(lvl / 1000):,}bn by {path[-1]['date']}), driven mainly by "
                         f"{pnames[primary].lower()}."),
            "assumptions": ("Reconstructs the path from recent component paces (WALCL runoff − TGA cash path − "
                            "RRP), each decayed over the quarter; RRP release capped at remaining balance; "
                            "band = ±1σ weekly × √weeks. Mechanical extrapolation, not a Fed/Treasury forecast.")}


def _asof(series):
    sd = sorted(series)
    vals = [series[d] for d in sd]

    def f(d):
        i = bisect.bisect_right(sd, d) - 1
        return vals[i] if i >= 0 else None
    return f


def _zcol(xs):
    valid = [x for x in xs if x is not None]
    if len(valid) < 10:
        return [0.0] * len(xs)
    m = mean(valid)
    sd = stdev(valid) or 1.0
    return [((x - m) / sd if x is not None else 0.0) for x in xs]


def historical_analogs(usd_dates, usd_z, hy_oas, dxy, nfci, wresbal, spx, k=4, comp_hist=None):
    """Represent each week as a z-scored liquidity FINGERPRINT and find the closest historical
    analogs to today — then show SPX's realized forward return after each. When a composite
    history is supplied, the multi-factor composite state is added as a fingerprint dimension so
    matches respect the whole liquidity configuration, not just net-liq impulse."""
    if not usd_dates or len(usd_dates) < 160 or not spx or len(spx) < 300:
        return None
    spine = usd_dates[::5]
    zmap = dict(zip(usd_dates, usd_z))
    a_hy = _asof(hy_oas) if hy_oas else (lambda d: None)
    a_dx = _asof(dxy) if dxy else (lambda d: None)
    a_nf = _asof(nfci) if nfci else (lambda d: None)
    wres_chg = {}
    if wresbal:
        wd = sorted(wresbal)
        for i in range(13, len(wd)):
            wres_chg[wd[i]] = wresbal[wd[i]] - wresbal[wd[i - 13]]
    a_wr = _asof(wres_chg) if wres_chg else (lambda d: None)
    a_comp = None
    if comp_hist and comp_hist.get("dates") and comp_hist.get("comp_z"):
        a_comp = _asof(dict(zip(comp_hist["dates"], comp_hist["comp_z"])))
    F = {"impulse": [], "hy_oas": [], "dollar": [], "nfci": [], "reserve_drain": []}
    if a_comp:
        F["composite"] = []
    rows = []
    for d in spine:
        rows.append(d)
        F["impulse"].append(zmap.get(d))
        F["hy_oas"].append(a_hy(d))
        F["dollar"].append(a_dx(d))
        F["nfci"].append(a_nf(d))
        F["reserve_drain"].append(-a_wr(d) if a_wr(d) is not None else None)
        if a_comp:
            F["composite"].append(a_comp(d))
    cols = {key: _zcol(v) for key, v in F.items()}
    nrow = len(rows)
    vecs = [[cols[key][i] for key in F] for i in range(nrow)]
    today, today_date = vecs[-1], rows[-1]
    cutoff = nrow - 13
    dists = sorted((sum((today[j] - vecs[i][j]) ** 2 for j in range(len(today))) ** 0.5, i)
                   for i in range(cutoff))
    sd = sorted(spx)

    def spx_fwd(d, days):
        i = bisect.bisect_left(sd, d)
        if i >= len(sd) or i + days >= len(sd):
            return None
        p0, p1 = spx[sd[i]], spx[sd[i + days]]
        return round((p1 / p0 - 1) * 100, 1) if (p0 and p1) else None
    analogs = []
    for dd, i in dists:
        d = rows[i]
        if any(abs((_pd(d) - _pd(a["date"])).days) < 70 for a in analogs):
            continue
        analogs.append({"date": d, "similarity_pct": round(100 / (1 + dd), 1), "distance": round(dd, 2),
                        "spx_fwd_21d": spx_fwd(d, 21), "spx_fwd_63d": spx_fwd(d, 63),
                        "fingerprint": {key: round(cols[key][i], 2) for key in F}})
        if len(analogs) >= k:
            break
    return {"as_of": today_date, "features": list(F.keys()),
            "fingerprint_now": {key: round(cols[key][-1], 2) for key in F},
            "analogs": analogs, "composite_aware": bool(a_comp),
            "note": ("Each week is a z-scored liquidity fingerprint" + (" including the multi-factor composite state"
                     if a_comp else "") + " (net-liq impulse, HY OAS, broad dollar, NFCI, reserve-drain speed). "
                     "Closest historical matches to today, with SPX's realized forward return after each. "
                     "Pattern recognition, not a forecast.")}


def liquidity_backtest(usd_dates, usd_z, spx, cost_bps=2):
    """Honest backtest of a simple, pre-specified liquidity-timing rule (long SPX when the
    net-liq impulse ≥ 0, cash otherwise, 1-day signal lag, net of cost) vs buy-and-hold.
    Reports equity curves + risk stats. Either it earns its keep or it doesn't."""
    if not usd_dates or not spx or len(spx) < 500:
        return None
    zmap = dict(zip(usd_dates, usd_z))
    za = _asof(zmap)
    start_d = usd_dates[0] if usd_dates else sorted(spx)[0]
    sd = [d for d in sorted(spx) if d >= start_d]
    if len(sd) < 500:
        return None
    cost = cost_bps / 10000.0
    eq_s = eq_b = 1.0
    peak_s = peak_b = 1.0
    maxdd_s = maxdd_b = 0.0
    prev_pos = 0
    switches = in_mkt = ndays = 0
    curve, daily_s, daily_b = [], [], []
    for i in range(1, len(sd)):
        p0, p1 = spx[sd[i - 1]], spx[sd[i]]
        if not p0 or not p1:
            continue
        ret = p1 / p0 - 1
        z_prior = za(sd[i - 1])
        pos = 1 if (z_prior is not None and z_prior >= 0) else 0
        c = cost if pos != prev_pos else 0.0
        if pos != prev_pos:
            switches += 1
        prev_pos = pos
        eq_s *= (1 + pos * ret - c)
        eq_b *= (1 + ret)
        peak_s = max(peak_s, eq_s); maxdd_s = min(maxdd_s, eq_s / peak_s - 1)
        peak_b = max(peak_b, eq_b); maxdd_b = min(maxdd_b, eq_b / peak_b - 1)
        in_mkt += pos; ndays += 1
        daily_s.append(pos * ret); daily_b.append(ret)
        if i % 5 == 0:
            curve.append({"date": sd[i], "strat": round(eq_s, 3), "bh": round(eq_b, 3)})
    if ndays < 250:
        return None
    yrs = ndays / 252.0

    def cagr(eq):
        return round((eq ** (1 / yrs) - 1) * 100, 1) if yrs > 0 and eq > 0 else 0.0

    def sharpe(rets):
        if len(rets) < 30:
            return 0.0
        s = stdev(rets) * (252 ** 0.5)
        return round((mean(rets) * 252) / s, 2) if s else 0.0
    sh_s, sh_b = sharpe(daily_s), sharpe(daily_b)
    return {"start": sd[0], "end": sd[-1], "years": round(yrs, 1),
            "strategy": {"name": "Long SPX when net-liq impulse ≥ 0, else cash",
                         "total_return_pct": round((eq_s - 1) * 100, 1), "cagr_pct": cagr(eq_s),
                         "sharpe": sh_s, "max_drawdown_pct": round(maxdd_s * 100, 1),
                         "time_in_market_pct": round(in_mkt / ndays * 100, 1), "switches": switches},
            "buy_hold": {"name": "Buy & hold SPX", "total_return_pct": round((eq_b - 1) * 100, 1),
                         "cagr_pct": cagr(eq_b), "sharpe": sh_b, "max_drawdown_pct": round(maxdd_b * 100, 1),
                         "time_in_market_pct": 100.0, "switches": 0},
            "curve": curve,
            "verdict": ("Liquidity timing beat buy-and-hold on risk-adjusted return (higher Sharpe)" if sh_s > sh_b
                        else "Liquidity timing did NOT beat buy-and-hold — staying invested won the melt-up"),
            "edge_on_drawdown": maxdd_s > maxdd_b,
            "note": (f"Simple pre-specified rule · 1-day signal lag · {cost_bps}bp per switch. In-sample test on "
                     "the same impulse series — illustrative of whether liquidity timing adds value, not a live "
                     "track record.")}


def cycle_clock(usd_dates, usd_z, n_orbit=26):
    """Phase-space orbit of the net-liquidity impulse (x) vs its acceleration (y). The cycle
    normally rotates counter-clockwise through 4 phases — early/late expansion, early/late
    contraction. Returns the recent orbit trail, current phase, and rotation direction."""
    if not usd_dates or len(usd_dates) < 80:
        return None
    pairs = list(zip(usd_dates, usd_z))[::5]
    wd = [p[0] for p in pairs]
    wz = [p[1] for p in pairs]
    if len(wz) < 30:
        return None
    accel = [None] * len(wz)
    for i in range(4, len(wz)):
        accel[i] = wz[i] - wz[i - 4]
    av = [a for a in accel if a is not None]
    am = mean(av)
    asd = stdev(av) or 1.0
    accel_z = [((a - am) / asd if a is not None else 0.0) for a in accel]
    start = max(4, len(wz) - n_orbit)
    orbit = [{"date": wd[i], "x": round(wz[i], 2), "y": round(accel_z[i], 2)} for i in range(start, len(wz))]
    cx, cy = wz[-1], accel_z[-1]
    if cx >= 0 and cy >= 0:
        phase, read = "EARLY EXPANSION", "Liquidity is expanding and still accelerating — the most supportive part of the cycle for risk."
    elif cx >= 0 and cy < 0:
        phase, read = "LATE EXPANSION", "Still expanding but decelerating — momentum is fading; this is where the cycle tends to roll over."
    elif cx < 0 and cy < 0:
        phase, read = "EARLY CONTRACTION", "Contracting and accelerating downward — the most defensive part of the cycle."
    else:
        phase, read = "LATE CONTRACTION", "Contracting but the downside is decelerating — momentum is easing and a turn may be forming."
    rot = "indeterminate"
    if len(orbit) >= 3:
        ax_, ay_ = orbit[-2]["x"] - orbit[-3]["x"], orbit[-2]["y"] - orbit[-3]["y"]
        bx_, by_ = orbit[-1]["x"] - orbit[-2]["x"], orbit[-1]["y"] - orbit[-2]["y"]
        cross = ax_ * by_ - ay_ * bx_
        rot = ("clockwise — normal cyclical progression" if cross < 0
               else "counter-clockwise — atypical (possible whipsaw / stalling)")
    return {"as_of": wd[-1], "impulse": round(cx, 2), "acceleration": round(cy, 2),
            "phase": phase, "phase_read": read, "rotation": rot, "orbit": orbit,
            "quadrants": {"q1": "Early expansion", "q2": "Late expansion",
                          "q3": "Early contraction", "q4": "Late contraction"},
            "note": ("Phase-space orbit: net-liquidity impulse (x) vs its acceleration (y). The cycle normally "
                     "rotates counter-clockwise through the four phases; the trail is the recent path and the "
                     "dot is now.")}


def desk_briefing(o):
    """Deterministic, data-bound desk synthesis written to the decisive-call feed the AI panel
    reads. Genuinely derived from the measured tables — a real call now, with the LLM router
    free to overwrite it with a richer narrative once credits return."""
    comp = o.get("composite") or {}
    traj = o.get("trajectory") or {}
    proj = o.get("projection") or {}
    clk = o.get("cycle_clock") or {}
    ten = o.get("tensions") or {}
    fe = o.get("forward_expectation") or {}
    an = (o.get("analogs") or {}).get("analogs") or []
    bt = o.get("backtest") or {}
    rw = o.get("reserve_runway") or {}
    usd = o.get("usd") or {}
    regime = comp.get("regime", "—")
    score = comp.get("liquidity_score")
    heading = traj.get("heading", "—")
    phase = clk.get("phase", "—")
    headline = f"Liquidity {regime} ({score}/100), {phase.lower()}, mechanics point {heading.lower()}."
    call_bits = [f"Net-liquidity impulse is {usd.get('state', '—')} (z {usd.get('impulse_z')}); the composite reads "
                 f"{regime} at {score}/100."]
    if proj.get("headline"):
        call_bits.append(proj["headline"])
    if clk.get("phase_read"):
        call_bits.append(clk["phase_read"])
    fwd = ""
    if fe.get("assets"):
        parts = [f"{k} {v['mean']:+.1f}% (excess {v['excess']:+.1f}%{', significant' if v['sig'] else ''})"
                 for k, v in fe["assets"].items()]
        fwd = f"Given the {fe['state']} state, the model's 21-day expectation — " + "; ".join(parts) + "."
    risks = [f"[{t['severity'].upper()}] {t['note']}" for t in (ten.get("items") or [])] or \
            ["No hidden divergences — sub-signals are aligned with the headline."]
    analog = ""
    if an:
        a0 = an[0]
        analog = (f"Today's fingerprint most resembles {a0['date']} ({a0['similarity_pct']}% match); SPX then "
                  f"returned {a0['spx_fwd_21d']:+.1f}% over 21d and {a0['spx_fwd_63d']:+.1f}% over 63d.")
    return {"title": "Liquidity inflection — desk call", "headline": headline,
            "the_call": " ".join(call_bits), "forward_expectation": fwd, "hidden_risks": risks,
            "nearest_analog": analog, "reserve_runway": rw.get("read", ""),
            "does_timing_work": bt.get("verdict", ""),
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "model": "deterministic synthesis — bound to measured tables (LLM router offline; will upgrade on top-up)"}


def build_composite_history(usd_dates, usd_z, wresbal, dxy, hy_oas, nfci):
    """Reconstruct a weekly multi-factor composite-liquidity z over history from the FRED-backed
    components (net-liq impulse, reserve drain, dollar, credit, conditions), z-harmonised so +
    always means more liquidity, then weight-blended. ~70% of the live composite's weight but
    with FULL history — enough to drive a cycle clock and a forward projection on the composite."""
    gaps = [(_pd(usd_dates[i]) - _pd(usd_dates[i - 1])).days for i in range(1, min(len(usd_dates), 40))]
    mg = sorted(gaps)[len(gaps) // 2] if gaps else 7
    stride = max(1, round(7 / max(mg, 1)))           # → ~weekly spine regardless of input cadence
    pairs = list(zip(usd_dates, usd_z))[::stride]
    spine = [p[0] for p in pairs]
    z_netliq = {p[0]: p[1] for p in pairs}
    if len(spine) < 60:
        return None

    def chg_series(series, wks):
        if not series or len(series) < wks + 5:
            return None
        sd = sorted(series)
        return {sd[i]: series[sd[i]] - series[sd[i - wks]] for i in range(wks, len(sd))}
    res_chg = chg_series(wresbal, 13)
    dxy_chg = chg_series(dxy, 13)
    a_res = _asof(res_chg) if res_chg else (lambda d: None)
    a_dxy = _asof(dxy_chg) if dxy_chg else (lambda d: None)
    a_hy = _asof(hy_oas) if hy_oas else (lambda d: None)
    a_nf = _asof(nfci) if nfci else (lambda d: None)
    raw = {"net_liquidity": [], "reserves": [], "dollar": [], "credit": [], "conditions": []}
    for d in spine:
        raw["net_liquidity"].append(z_netliq.get(d))
        raw["reserves"].append(a_res(d))
        raw["dollar"].append(a_dxy(d))
        raw["credit"].append(a_hy(d))
        raw["conditions"].append(a_nf(d))
    signs = {"net_liquidity": 1, "reserves": 1, "dollar": -1, "credit": -1, "conditions": -1}
    zc = {k: [signs[k] * x for x in _zcol(v)] for k, v in raw.items()}
    weights = {"net_liquidity": 0.28, "reserves": 0.16, "dollar": 0.12, "credit": 0.10, "conditions": 0.08}
    active = {k: w for k, w in weights.items() if any(abs(x) > 1e-9 for x in zc[k])}
    if not active:
        return None
    wsum = sum(active.values())
    comp_z = [sum(zc[k][i] * active[k] for k in active) / wsum for i in range(len(spine))]
    return {"dates": spine, "comp_z": comp_z, "component_z": {k: zc[k] for k in active},
            "weights": active, "wsum": wsum, "components_used": list(active.keys()),
            "source": "fred-reconstruction"}


def project_composite(hist, horizon_wk=13):
    """Project the multi-factor composite forward by extrapolating EACH component's recent z-trend
    (damped) and re-blending by weight — so all components drive it. Decomposes the projected move
    by component. A momentum extrapolation (no mechanical calendar like net-liq), with a √-time band."""
    if not hist:
        return None
    cz = hist["comp_z"]
    comp = hist["component_z"]
    W = hist["weights"]
    wsum = hist["wsum"]
    dates = hist["dates"]
    if len(cz) < 20:
        return None
    cur = cz[-1]
    mom = {}
    for k, series in comp.items():
        recent = [series[i] - series[i - 1] for i in range(len(series) - 6, len(series))]
        mom[k] = sorted(recent)[len(recent) // 2]
    DECAY = 0.8
    diffs = [cz[i] - cz[i - 1] for i in range(1, len(cz))]
    vol = stdev(diffs) if len(diffs) > 5 else 0.2
    last_d = _pd(dates[-1])

    def score(z):
        return round(max(0.0, min(100.0, 50 + z * 16.5)), 1)
    path, lvl = [], cur
    for w in range(1, horizon_wk + 1):
        step = sum(W[k] * mom[k] * (DECAY ** (w - 1)) / wsum for k in comp)
        lvl += step
        band = vol * (w ** 0.5)
        path.append({"week": w, "date": (last_d + timedelta(weeks=w)).isoformat(),
                     "comp_z": round(lvl, 3), "score": score(lvl),
                     "lo": round(lvl - band, 3), "hi": round(lvl + band, 3)})
    contrib = {k: round(W[k] * sum(mom[k] * (DECAY ** (w - 1)) for w in range(1, horizon_wk + 1)) / wsum, 3)
               for k in comp}
    primary = max(contrib, key=lambda k: abs(contrib[k])) if contrib else None
    chg = lvl - cur
    LAB = {"net_liquidity": "Net liquidity", "reserves": "Reserves", "dollar": "Dollar",
           "credit": "Credit spreads", "conditions": "Financial conditions"}
    hist_out = [{"date": dates[i], "comp_z": round(cz[i], 3), "score": score(cz[i])}
                for i in range(max(0, len(dates) - 14), len(dates))]
    direction = "ease" if chg > 0 else "tighten"
    return {"horizon_weeks": horizon_wk, "current_z": round(cur, 3), "current_score": score(cur),
            "projected_z": round(lvl, 3), "projected_score": score(lvl), "projected_change_z": round(chg, 3),
            "history": hist_out, "path": path,
            "contributions": {LAB.get(k, k): contrib[k] for k in contrib},
            "primary_driver": LAB.get(primary, primary),
            "components_used": [LAB.get(k, k) for k in comp],
            "headline": (f"Composite liquidity projected to {direction} ({'+' if chg > 0 else ''}{round(chg, 2)} z, "
                         f"to {round(score(lvl))}/100) over {horizon_wk} weeks on current component momentum, led by "
                         f"{LAB.get(primary, primary).lower()}."),
            "note": ("Momentum projection: each component's recent z-trend is extrapolated (decayed) and re-blended "
                     "by weight. Unlike the net-liquidity projection there is no mechanical calendar — this is a "
                     "damped-momentum extrapolation of the multi-factor state, with a √-time band.")}


def snapshot_composite(out):
    """Append today's FULL composite reading (all components incl. feed-based) to a rolling S3
    history, so over time a true all-component series accumulates that the composite clock and
    projection can use in place of the FRED-backed reconstruction."""
    comp = out.get("composite")
    if not comp or comp.get("composite_z") is None:
        return None
    key = "data/composite-snapshots.json"
    try:
        snaps = json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read()).get("snapshots", [])
    except Exception:
        snaps = []
    today = datetime.now(timezone.utc).date().isoformat()
    comps = comp.get("components") or []
    rec = {"date": today, "comp_z": comp.get("composite_z"), "score": comp.get("liquidity_score"),
           "regime": comp.get("regime"),
           "components": {c["name"]: c["eff_z"] for c in comps if "name" in c and "eff_z" in c},
           "weights": {c["name"]: c["weight"] for c in comps if "name" in c and "weight" in c}}
    snaps = [s for s in snaps if s.get("date") != today]
    snaps.append(rec)
    snaps.sort(key=lambda s: s.get("date", ""))
    snaps = snaps[-1500:]
    body = {"snapshots": snaps, "count": len(snaps),
            "first": snaps[0]["date"] if snaps else None, "last": snaps[-1]["date"] if snaps else None,
            "note": "Rolling daily snapshots of the full multi-factor composite (all components, including "
                    "feed-based ones). Builds a true all-component history for the cycle clock & projection."}
    S3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(body, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=1800")
    return {"count": len(snaps), "first": body["first"], "last": body["last"]}


def composite_history_from_snapshots():
    """Build the composite history from accumulated real snapshots — but only once there are
    enough of them spanning enough time. Until then returns None and the FRED reconstruction is
    used. This makes the composite clock/projection self-upgrading with no future code change."""
    try:
        snaps = json.loads(S3.get_object(Bucket=BUCKET, Key="data/composite-snapshots.json")["Body"].read()).get("snapshots", [])
    except Exception:
        return None
    snaps = [s for s in snaps if s.get("comp_z") is not None and s.get("date")]
    if len(snaps) < 52:
        return None
    snaps.sort(key=lambda s: s["date"])
    if (_pd(snaps[-1]["date"]) - _pd(snaps[0]["date"])).days < 300:
        return None
    seen = {}
    for s in snaps:
        ic = _pd(s["date"]).isocalendar()
        seen[(ic[0], ic[1])] = s
    wk = [seen[k] for k in sorted(seen)]
    dates = [s["date"] for s in wk]
    comp_z = [s["comp_z"] for s in wk]
    names = []
    for s in wk:
        for nmc in (s.get("components") or {}):
            if nmc not in names:
                names.append(nmc)
    component_z = {nmc: [(s.get("components") or {}).get(nmc, 0.0) or 0.0 for s in wk] for nmc in names}
    weights = {}
    for s in reversed(wk):
        if s.get("weights"):
            weights = {nmc: s["weights"].get(nmc, 0.0) for nmc in names if s["weights"].get(nmc)}
            break
    if not weights:
        weights = {nmc: 1.0 / len(names) for nmc in names}
    wsum = sum(weights.values()) or 1.0
    return {"dates": dates, "comp_z": comp_z, "component_z": component_z, "weights": weights,
            "wsum": wsum, "components_used": names, "source": "snapshots", "n_snapshots": len(snaps)}


def stablecoin_signal(sc):
    """Properly extract the ALREADY-LIVE stablecoin-flow engine (crypto-dollar liquidity) instead
    of the crude best-effort probe this page used before — it has a validated state machine,
    forward expectations and mint/burn detail that were being discarded."""
    if not sc or not isinstance(sc, dict):
        return None
    agg = sc.get("aggregate") or {}
    state = sc.get("state")
    d30 = agg.get("delta_30d_pct")
    eff_z = None
    if isinstance(d30, (int, float)):
        eff_z = max(-3.0, min(3.0, d30 / 4.0))
    return {"state": state, "state_description": sc.get("state_description"),
            "signal_strength": sc.get("signal_strength"), "eff_z": eff_z,
            "total_usd_bn": round(agg.get("total_usd", 0) / 1e9, 1) if agg.get("total_usd") else None,
            "delta_24h_pct": agg.get("delta_24h_pct"), "delta_7d_pct": agg.get("delta_7d_pct"),
            "delta_30d_pct": d30, "top_chains": (agg.get("top_chains") or [])[:5],
            "top_minters_30d": (sc.get("top_5_minters_30d") or [])[:5],
            "top_burners_30d": (sc.get("top_3_burners_30d") or [])[:3],
            "forward_expectations": sc.get("forward_expectations"),
            "as_of": (sc.get("as_of") or "")[:10]}


def treasury_auction_signal(ac, net_dates_last=None, tga_pace_bn_wk=None):
    """Surface the ALREADY-LIVE auction-crisis-detector (real Treasury auction results +
    forward calendar via api.fiscaldata.treasury.gov) — auction demand health, and the
    confirmed near-term bill-issuance calendar (a real, scheduled number, not an extrapolation)."""
    if not ac or not isinstance(ac, dict):
        return None
    td = ac.get("tenor_decomposition") or {}
    highlights = sorted(
        ({"bucket": k, "label": v.get("label"), "composite": v.get("composite"),
          "risk_profile": v.get("risk_profile"), "dominant_signal": v.get("dominant_signal")}
         for k, v in td.items() if isinstance(v, dict) and v.get("composite") is not None),
        key=lambda r: -(r["composite"] or 0))[:3]
    tail = ((ac.get("indicator_aggregate_14d") or {}).get("tail_stress") or {})
    fc = ac.get("forward_calendar") or []
    near_term = None
    if fc:
        today = datetime.now(timezone.utc).date()
        window_end = today + timedelta(days=14)
        bills = [a for a in fc if a.get("security_type") == "Bill" and a.get("offering_amount_billions")]
        in_window = [a for a in bills if a.get("issue_date") and today.isoformat() <= a["issue_date"] <= window_end.isoformat()]
        known_bn = round(sum(a["offering_amount_billions"] for a in in_window), 1)
        near_term = {"coverage_days": 14, "n_auctions": len(in_window),
                     "scheduled_bill_issuance_bn": known_bn,
                     "note": ("Confirmed GROSS new T-bill issuance settling over the next 14 days, from the real "
                              "Treasury auction calendar — not a net figure (doesn't subtract maturing bills, "
                              "coupon issuance, spending or receipts), so it isn't directly comparable to the "
                              "net-liquidity projection's weekly pace. Shown as a real, confirmed data point in "
                              "its own right, not a cross-check.")}
    return {"regime": ac.get("regime"), "composite_score": ac.get("composite_score"),
            "interpretation": ac.get("interpretation"), "issuance_anomaly": ac.get("issuance_anomaly"),
            "tenor_highlights": highlights, "tail_stress_14d": {"n_fired": tail.get("n_fired"), "max_score": tail.get("max_score")},
            "curve_slope": (ac.get("cross_signals") or {}).get("curve_slope"),
            "near_term_calendar": near_term, "as_of": (ac.get("generated_at") or "")[:10],
            "source": "auction-crisis-detector (api.fiscaldata.treasury.gov, live)"}


def _onshore_funding():
    """ops 3304: OFR STFM context (non-scoring — fingerprint history untouched)."""
    try:
        ofr = s3_json("data/ofr-stfm.json") or {}
        ven = ((ofr.get("repo") or {}).get("venues")) or {}
        g, t3, dv = (ven.get("GCF") or {}), (ven.get("TRI") or {}), (ven.get("DVP") or {})
        out = {}
        if g.get("rate_pct") is not None and t3.get("rate_pct") is not None:
            sp = round((g["rate_pct"] - t3["rate_pct"]) * 100, 1)
            out["gcf_minus_tri_bp"] = sp
            out["tone"] = "STRAIN" if sp >= 8 else ("WATCH" if sp >= 3 else "OK")
        for k, v in (("dvp_t", dv), ("tri_t", t3)):
            if v.get("vol_mn"):
                out[k] = round(v["vol_mn"] / 1e12, 2)
        if g.get("vol_mn"):
            out["gcf_b"] = round(g["vol_mn"] / 1e9, 0)
        rp = (((ofr.get("mmf") or {}).get("picks")) or {}).get("repo_holdings") or {}
        if rp.get("latest"):
            out["mmf_repo_b"] = round(rp["latest"] / 1e9, 0)
        if out:
            out["note"] = ("Onshore repo plumbing (OFR): interdealer premium %sbp — %s"
                           % (out.get("gcf_minus_tri_bp", "?"), out.get("tone", "n/a")))
            return out
    except Exception as e:
        print("[liq] onshore skip %s" % str(e)[:60])
    return None


def lambda_handler(event=None, context=None):
    t0 = time.time()
    avail = {}

    # USD net liquidity
    walcl = fred("WALCL"); tga = fred("WTREGEN"); rrp = fred("RRPONTSYD")
    avail["usd"] = bool(walcl and tga and rrp)
    usd_dates, usd_z, usd_flips = [], [], []
    net = {}
    if avail["usd"]:
        alld = sorted(set(tga) | set(rrp) | set(walcl))
        lastw = None
        for d in alld:
            if d in walcl:
                lastw = walcl[d]
            if lastw is not None and d in tga and d in rrp:
                net[d] = lastw - tga[d] - rrp[d]
        usd_dates, usd_z = build_impulse(net)
        usd_flips = find_flips(usd_dates, usd_z)
    projection = project_net_liquidity(net, walcl, tga, rrp) if net else None
    clock = cycle_clock(usd_dates, usd_z) if usd_dates else None

    # EUR excess liquidity (platform store)
    eur = None
    for k in ("data/ecb-hist/excess_liquidity.json", "data/ecb-hist/excess-liquidity.json"):
        j = s3_json(k)
        if j:
            pts = j.get("points") or j.get("observations") or j
            if isinstance(pts, list):
                eur = {p[0]: float(p[1]) for p in pts if isinstance(p, (list, tuple)) and p[1] is not None}
            elif isinstance(pts, dict):
                eur = {d: float(v) for d, v in pts.items() if v is not None}
            break
    avail["eur"] = bool(eur and len(eur) > 200)
    eur_state = None
    if avail["eur"]:
        ed, ez = build_impulse(eur)
        eur_state = {"z": ez[-1], "as_of": ed[-1], "flips_5y": len([f for f in find_flips(ed, ez) if f["date"] >= "2021-01-01"])}

    # China credit impulse proxy (BIS quarterly via FRED)
    cn = None
    for sid in ("CRDQCNAPABIS", "QCNPAM770A", "CRDQCNBPABIS"):
        s = fred(sid, start="2006-01-01")
        if len(s) > 30:
            cn = (sid, s)
            break
    avail["china"] = bool(cn)
    cn_state = None
    if cn:
        sid, s = cn
        ds = sorted(s)
        yoy = [(ds[i], s[ds[i]] / s[ds[i - 4]] - 1) for i in range(4, len(ds)) if s[ds[i - 4]]]
        acc = [(d, yoy[i][1] - yoy[i - 4][1]) for i, (d, _) in enumerate(yoy) if i >= 4]
        vals = [a for _, a in acc]
        m, sd = mean(vals[-40:]), stdev(vals[-40:])
        cn_state = {"series": sid, "as_of": acc[-1][0],
                    "credit_yoy_pct": round(yoy[-1][1] * 100, 1),
                    "impulse_z": round((vals[-1] - m) / sd, 2) if sd else 0}

    # ── Stablecoin flow — crypto-dollar liquidity (already-live engine, properly extracted) ──
    sc = s3_json("data/stablecoin-flow.json")
    stablecoin_full = stablecoin_signal(sc)
    sc_state = ({"accel_z": stablecoin_full["eff_z"], "state": stablecoin_full["state"],
                "signal_strength": stablecoin_full["signal_strength"], "as_of": stablecoin_full["as_of"]}
                if stablecoin_full else None)
    avail["stablecoin"] = bool(stablecoin_full)
    sc_schema_hint = None

    # ── Treasury auction health — already-live auction-crisis-detector (real fiscaldata.treasury.gov) ──
    ac = pull("data/auction-crisis.json")
    treasury_auctions = treasury_auction_signal(ac)

    # Lead/lag event-study vs SPX / BTC / HYG
    spx_doc = s3_json("data/spx-history-deep.json") or {}
    spx = {d: float(v) for d, v in (spx_doc.get("points") or []) if v is not None}
    btc = polygon_closes("X:BTCUSD")
    hyg = polygon_closes("HYG")
    studies, leads = {}, {}
    regime_returns, lead_curves, flip_logs = {}, {}, {}
    flips10 = [f for f in usd_flips if f["date"] >= "2015-06-01"]
    for name, px in (("SPX_proxy", spx), ("BTC", btc), ("HYG", hyg)):
        if len(px) > 500:
            dd = sorted(px)
            if flips10:
                studies[name] = event_study(flips10, px, dd)
                flip_logs[name] = flip_log(flips10, px, dd)
            leads[name] = best_lead(usd_dates, usd_z, px)
            regime_returns[name] = regime_conditioned_study(usd_dates, usd_z, px)
            lead_curves[name] = lead_curve(usd_dates, usd_z, px)

    # closed-loop: log a NEW flip (≤5 sessions)
    n_logged = 0
    new_flip = usd_flips[-1] if usd_flips and usd_flips[-1]["date"] >= usd_dates[-5] else None
    if new_flip:
        try:
            spy = polygon_closes("SPY", start=(datetime.now(timezone.utc) - timedelta(days=10)).date().isoformat())
            px0 = spy[sorted(spy)[-1]] if spy else None
            if px0:
                nowt = datetime.now(timezone.utc)
                DDB.Table("justhodl-signals").put_item(Item={
                    "signal_id": f"liquidity-inflection#USD#{new_flip['date']}",
                    "signal_type": "liquidity_inflection", "signal_value": str(new_flip["z"]),
                    "predicted_direction": new_flip["direction"],
                    "confidence": Decimal("0.58"), "measure_against": "ticker",
                    "baseline_price": str(px0), "benchmark": "SPY",
                    "check_windows": ["day_5", "day_21", "day_63"],
                    "check_timestamps": {f"day_{w}": (nowt + timedelta(days=w)).isoformat() for w in (5, 21, 63)},
                    "outcomes": {}, "accuracy_scores": {}, "logged_at": nowt.isoformat(),
                    "logged_epoch": int(nowt.timestamp()), "status": "pending", "schema_version": "2",
                    "horizon_days_primary": 21, "regime_at_log": "UNKNOWN",
                    "ttl": int(nowt.timestamp()) + 120 * 86400,
                    "metadata": {"engine": "liquidity-inflection", "v": VERSION, "flip_z": str(new_flip["z"])},
                    "rationale": f"USD net-liquidity impulse flipped {new_flip['direction']} on {new_flip['date']} (z {new_flip['z']})"})
                n_logged = 1
        except Exception as e:
            print(f"[signals] {str(e)[:80]}")

    # US money block (brain-gap: M2 30×, real-money mirror of EU real-M1)
    us_money = None
    try:
        m2 = fred("M2SL", "1990-01-01")
        cpi = fred("CPIAUCSL", "1990-01-01")
        if len(m2) > 30 and len(cpi) > 30:
            dm = dict(m2); dc = dict(cpi)
            common = sorted(set(dm) & set(dc))
            yy = [(d, (dm[d] / dm[common[i - 12]] - 1) * 100
                       - (dc[d] / dc[common[i - 12]] - 1) * 100)
                  for i, d in enumerate(common) if i >= 12]
            vals = [v for _, v in yy]
            m_, sd_ = mean(vals[-360:]), stdev(vals[-360:])
            us_money = {"real_m2_yoy_pct": round(vals[-1], 2),
                         "z": round((vals[-1] - m_) / sd_, 2) if sd_ else 0,
                         "as_of": yy[-1][0],
                         "note": "M2 YoY − CPI YoY; negative real M2 preceded 2008 and the "
                                 "2022-23 drawdown (brain: track M2/GDP, real money)"}
    except Exception as e:
        print(f"[us_money] {str(e)[:60]}")

    # ── Reserve & buffer mechanics (the 2026 reserve-scarcity regime) ──
    wresbal = fred("WRESBAL", "2010-01-01")
    reserves = series_state(wresbal)
    if reserves and wresbal:
        rl = wresbal[sorted(wresbal)[-1]]
        reserves["level_usd_bn"] = round(rl / 1000, 1)
        reserves["scarcity_note"] = ("Below ~$3.0T comfort zone — QT/issuance now draining real reserves"
                                     if rl / 1000 < 3000 else "Above comfort zone")
    rrp_state = series_state(rrp) if rrp else None
    if rrp_state and rrp:
        rv = rrp[sorted(rrp)[-1]]
        rrp_state["level_usd_bn"] = round(rv, 1)
        rrp_state["buffer_note"] = ("Buffer effectively exhausted — RRP near zero, drains now hit reserves directly"
                                    if rv < 100 else "Buffer still cushioning the reserve drain")
    tga_state = series_state(tga) if tga else None
    if tga_state and tga:
        tga_state["level_usd_bn"] = round(tga[sorted(tga)[-1]] / 1000, 1)

    # ── Funding & plumbing stress (money-market signals institutions watch) ──
    sofr = fred("SOFR", "2018-01-01")
    iorb = fred("IORB", "2021-07-01")
    sofr_iorb = None
    if sofr and iorb:
        cd = sorted(set(sofr) & set(iorb))
        if cd:
            ld = cd[-1]
            spread_bps = round((sofr[ld] - iorb[ld]) * 100, 1)
            tail = [round((sofr[d] - iorb[d]) * 100, 1) for d in cd[-20:]]
            sofr_iorb = {"spread_bps": spread_bps, "as_of": ld,
                         "trend_20d_bps": round(spread_bps - tail[0], 1) if len(tail) > 1 else 0,
                         "stress": ("REPO STRESS — SOFR at/above IORB = reserve scarcity" if spread_bps >= -2
                                    else "abundant — SOFR comfortably below IORB"),
                         "tail_20d": [[d, round((sofr[d] - iorb[d]) * 100, 1)] for d in cd[-20:]]}
    fp = pull("data/funding-plumbing.json")
    eds = pull("data/eurodollar-stress.json")
    edp = pull("data/eurodollar-plumbing.json")
    mv = pull("data/move-index.json")
    funding_stress = {
        "sofr_iorb": sofr_iorb,
        "funding_plumbing": ({"score": fp.get("plumbing_stress_score"), "regime": fp.get("regime"),
                              "balance_sheet": fp.get("balance_sheet_direction"),
                              "drivers": (fp.get("top_drivers") or [])[:4]} if fp else None),
        "eurodollar_stress": ({"score": eds.get("composite_score"), "regime": eds.get("regime"),
                               "hot": (eds.get("hot_signals") or [])[:4]} if eds else None),
        "eurodollar_plumbing": ({"health": edp.get("plumbing_health"), "regime": edp.get("stress_regime"),
                                 "verdict": edp.get("verdict")} if edp else None),
        "move": ({"level": mv.get("level"), "regime": mv.get("regime"),
                  "percentile": mv.get("percentile"), "change_20d": mv.get("change_20d")} if mv else None),
    }

    # ── Global liquidity (4 central banks, via global-liquidity engine) ──
    gl = pull("data/global-liquidity.json")
    global_liq = ({"index": gl.get("global_liquidity_index"), "regime": gl.get("regime"),
                   "regime_read": gl.get("regime_read"), "impulse_13w_pct": gl.get("global_impulse_13w_pct"),
                   "fed_net_liquidity": gl.get("fed_net_liquidity"), "broad_money": gl.get("broad_money")}
                  if gl else None)

    # ── Dollar (broad trade-weighted) — a rising USD tightens global dollar liquidity ──
    dxy = fred("DTWEXBGS", "2015-01-01")
    dollar = series_state(dxy, invert=True)
    if dollar and dxy:
        dollar["level"] = round(dxy[sorted(dxy)[-1]], 2)

    # ── Credit & systemic stress (regime label from engine; HY OAS scalar from FRED) ──
    cs = pull("data/credit-stress.json")
    gs = pull("data/global-stress.json")
    hy_oas = fred("BAMLH0A0HYM2", "2010-01-01")
    hy_bps = round(hy_oas[sorted(hy_oas)[-1]] * 100, 0) if hy_oas else None
    credit = None
    if cs or hy_bps is not None:
        credit = {"regime": cs.get("composite_regime") if cs else None, "hy_oas_bps": hy_bps,
                  "signal": cs.get("composite_signal") if cs else None}
    systemic = ({"global_stress_index": gs.get("global_stress_index"), "level": gs.get("global_stress_level"),
                 "bond_stress": gs.get("bond_stress")} if gs else None)

    # ── Financial conditions (Chicago Fed NFCI; negative = loose) ──
    nfci = fred("NFCI", "2010-01-01")
    fin_cond = None
    if nfci:
        nd = sorted(nfci)
        lvl = nfci[nd[-1]]
        prev = nfci[nd[-13]] if len(nd) > 13 else lvl
        fin_cond = {"nfci": round(lvl, 3), "as_of": nd[-1], "trend_13w": round(lvl - prev, 3),
                    "read": ("loose / accommodative" if lvl < 0 else "tight / restrictive"),
                    "tightening": lvl > prev}

    # ── China liquidity engine (augment the BIS credit-impulse with the live regime) ──
    cl = pull("data/china-liquidity.json")
    china_engine = ({"regime": cl.get("regime"), "regime_read": cl.get("regime_read")} if cl else None)

    # ── Composite history (snapshots once accumulated, else FRED reconstruction) ──
    try:
        chist = (composite_history_from_snapshots()
                 or build_composite_history(usd_dates, usd_z, wresbal, dxy, hy_oas, nfci))
    except Exception as e:
        print(f"[chist] {str(e)[:80]}")
        chist = None
    chist_src = chist.get("source") if chist else None
    # composite-conditioned forward-return study (same buckets, but on the FULL composite state)
    regime_returns_composite = {}
    if chist:
        for _nm, _px in (("SPX_proxy", spx), ("BTC", btc), ("HYG", hyg)):
            if len(_px) > 500:
                try:
                    rrc = regime_conditioned_study(chist["dates"], chist["comp_z"], _px)
                    if rrc:
                        rrc["source"] = chist_src
                        regime_returns_composite[_nm] = rrc
                except Exception as e:
                    print(f"[rrc {_nm}] {str(e)[:60]}")

    # ── Historical analogs — nearest liquidity fingerprints in history (composite-aware) ──
    try:
        analogs = historical_analogs(usd_dates, usd_z, hy_oas, dxy, nfci, wresbal, spx, comp_hist=chist)
    except Exception as e:
        print(f"[analogs] {str(e)[:80]}")
        analogs = None
    # ── Liquidity-timed backtest vs buy-and-hold ──
    try:
        backtest = liquidity_backtest(usd_dates, usd_z, spx)
    except Exception as e:
        print(f"[backtest] {str(e)[:80]}")
        backtest = None
    # ── Composite (all-component) cycle clock + projection ──
    composite_clock = composite_projection = None
    try:
        if chist:
            composite_clock = cycle_clock(chist["dates"], chist["comp_z"])
            if composite_clock:
                composite_clock["components_used"] = chist["components_used"]
                composite_clock["source"] = chist_src
            composite_projection = project_composite(chist)
            if composite_projection:
                composite_projection["source"] = chist_src
    except Exception as e:
        print(f"[composite-dyn] {str(e)[:80]}")

    # ── Dollar shortage / cross-currency strain (offshore USD funding) ──
    def _layer_metric(edp_doc, layer, mid):
        for m in (((edp_doc.get("layers") or {}).get(layer) or {}).get("metrics") or []):
            if m.get("id") == mid:
                return m
        return {}
    edp_doc = edp if isinstance(edp, dict) else {}
    fx_m = _layer_metric(edp_doc, "fx", "broad_dollar")
    cp_m = _layer_metric(edp_doc, "bank_funding", "cp_ois")
    mfx = edp_doc.get("massive_fx") or {}
    swpt = fred("SWPT", "2008-01-01")        # central-bank liquidity swaps, weekly $M
    swpt_bn = swpt_trend = None
    if swpt:
        sd = sorted(swpt)
        swpt_bn = round(swpt[sd[-1]] / 1000.0, 2)
        swpt_trend = round((swpt[sd[-1]] - swpt[sd[-5]]) / 1000.0, 2) if len(sd) >= 5 else 0.0
    cp_bps = cp_m.get("value")
    usd_synth = mfx.get("usd_synthetic_20d_pct")
    ds_flags = []
    if isinstance(swpt_bn, (int, float)) and swpt_bn > 5:
        ds_flags.append(f"Fed swap lines drawn ${swpt_bn}bn — offshore dollar shortage")
    if isinstance(cp_bps, (int, float)) and cp_bps > 50:
        ds_flags.append(f"CP−SOFR {cp_bps}bp — bank dollar funding tightening")
    if isinstance(usd_synth, (int, float)) and usd_synth > 3:
        ds_flags.append(f"Synthetic USD +{usd_synth}% 20d — FX-implied dollar funding getting expensive")
    ds_status = ("SCRAMBLE" if len(ds_flags) >= 2 else "WATCH" if ds_flags else "CALM")
    dollar_shortage = {
        "status": ds_status, "fed_swap_lines_bn": swpt_bn, "swap_trend_bn": swpt_trend,
        "cp_ois_bps": cp_bps, "broad_dollar": fx_m.get("value"), "broad_dollar_pctile": fx_m.get("pctile"),
        "usd_synthetic_20d_pct": usd_synth,
        "flags": ds_flags or ["Offshore USD funding calm — no scramble for dollars"],
        "note": "Swap lines ~0 normally; ANY sustained rise = acute global dollar shortage (peaked ~$450bn in 2020)."}

    # ── Settlement fails (fails-to-deliver + fails-to-receive) — collateral scarcity ──
    sfd = pull("data/settlement-fails.json")
    settlement_fails = None
    if sfd:
        sig = sfd.get("signal") or {}
        hd = sfd.get("headline") or {}
        settlement_fails = {
            "regime": sig.get("regime"), "score": sig.get("score"),
            "ust_ftd_bn": hd.get("ftd_bn"), "ust_ftr_bn": hd.get("ftr_bn"),
            "ust_combined_bn": hd.get("combined_bn"), "pctile": hd.get("pctile"),
            "z": hd.get("z"), "max_bn": hd.get("max_bn"), "drivers": (sig.get("drivers") or [])[:3],
            "note": "Fails-to-deliver + fails-to-receive (NY Fed FR2004). Spikes = collateral hard to source — repo squeeze / scarcity."}

    # ── Central-bank swap lines & discount-window backstop usage ──
    dw = fred("WLCFLPCL", "2010-01-01")      # primary credit (discount window), weekly $M
    dw_bn = round(dw[sorted(dw)[-1]] / 1000.0, 2) if dw else None
    srf = fred("RPONTSYD", "2021-07-01")     # Fed Standing Repo Facility usage (dealers borrowing FROM Fed) — distinct from RRP
    srf_bn = round(srf[sorted(srf)[-1]] / 1000.0, 3) if srf else None
    srf_active_days = None
    if srf:
        sd_ = sorted(srf)
        streak = 0
        for d_ in reversed(sd_):
            if (srf[d_] or 0) > 50:   # >$50M = meaningfully non-zero for a facility that normally prints ~$0
                streak += 1
            else:
                break
        srf_active_days = streak
    swap_lines = {
        "fed_swaps_bn": swpt_bn, "swap_trend_bn": swpt_trend, "discount_window_bn": dw_bn,
        "srf_bn": srf_bn, "srf_active_days": srf_active_days,
        "status": ("STRESS" if (isinstance(swpt_bn, (int, float)) and swpt_bn > 5)
                   or (isinstance(dw_bn, (int, float)) and dw_bn > 15)
                   or (isinstance(srf_bn, (int, float)) and srf_bn > 1) else "CALM"),
        "note": ("Fed FX swap lines + discount-window primary credit + Standing Repo Facility (SRF) usage — "
                 "crisis backstops. SRF normally prints ~$0; any real usage means dealers needed the Fed's "
                 "emergency repo backstop, a harder signal than reserve-scarcity proxies.")}

    # ── Cross-asset flow divergence (dash-for-cash / flight-to-safety) ──
    cflow = pull("data/capital-flow.json")
    flow_divergence = None
    if cflow:
        rot = {r.get("category"): r for r in (cflow.get("category_rotation") or []) if r.get("category")}

        def _flow(cat):
            return (rot.get(cat) or {}).get("net_flow_5d_usd")

        def _bn(x):
            return round(x / 1e9, 2) if isinstance(x, (int, float)) else None
        bonds, equity = _flow("RATES_TREASURIES"), _flow("BROAD_EQUITY_US")
        credit_f, crypto, commod = _flow("CREDIT"), _flow("CRYPTO"), _flow("COMMODITIES")
        bonds_in = isinstance(bonds, (int, float)) and bonds > 0
        equity_out = isinstance(equity, (int, float)) and equity < 0
        crypto_out = isinstance(crypto, (int, float)) and crypto < 0
        commod_out = isinstance(commod, (int, float)) and commod < 0
        equity_in = isinstance(equity, (int, float)) and equity > 0
        crypto_in = isinstance(crypto, (int, float)) and crypto > 0
        if bonds_in and equity_out and crypto_out and commod_out:
            fr, fread = "DASH_FOR_CASH", ("Bonds bid while equities, crypto AND gold are all sold — a dash-for-cash / "
                                         "deleveraging scramble. Classic dollar-shortage tell: everything sold for cash.")
        elif bonds_in and equity_out:
            fr, fread = "FLIGHT_TO_SAFETY", ("Money rotating from equities into Treasuries — risk-off flight to safety, "
                                             "gold/crypto not yet dumped (orderly de-risking).")
        elif equity_in and crypto_in:
            fr, fread = "RISK_SEEKING", ("Equities and crypto both taking inflows — risk-seeking, liquidity flowing "
                                         "out along the risk curve.")
        else:
            fr, fread = "NEUTRAL", "No strong cross-asset flow divergence."
        flow_divergence = {"regime": fr, "read": fread,
                           "flows_5d_usd_bn": {"treasuries": _bn(bonds), "equity": _bn(equity),
                                               "credit": _bn(credit_f), "crypto": _bn(crypto),
                                               "commodities_gold": _bn(commod)}}

    # ── Leverage stress (margin debt + repo leverage + securities lending) ──
    rl = pull("data/repo-lending.json")
    leverage_stress = None
    if rl:
        md = rl.get("margin_debt") or {}
        rp_ = rl.get("repo") or {}
        sl_ = rl.get("securities_lending") or {}
        leverage_stress = {
            "score": rl.get("composite_leverage_stress"), "regime": rl.get("regime"),
            "margin_debt_bn": md.get("level_billions"), "margin_pct_mcap": md.get("pct_of_market_cap"),
            "margin_yoy_pct": md.get("yoy_growth_pct"), "margin_danger": md.get("danger_zone"),
            "margin_read": md.get("interpretation"),
            "repo_score": rp_.get("score"),
            "sec_lending_high_util": (sl_.get("components") or {}).get("n_high_utilization"),
            "note": "Composite of margin debt vs market cap, repo leverage (RRP+SOFR-IORB) and securities-lending "
                    "utilization. Low = abundant headroom; high/rising = crowded, fragile leverage."}

    # ── Dealer survey (NY Fed primary-dealer survey — reference; PDF parsing deferred) ──
    dsv = pull("data/dealer-survey.json")
    dealer_survey = None
    if dsv:
        ls = dsv.get("latest_survey") or {}
        dealer_survey = {
            "status": dsv.get("last_check_status"),
            "last_survey": (ls.get("fomc_date") or (ls.get("source_url") or "").split("/")[-1] or None),
            "source_url": ls.get("source_url"), "discovered_at": ls.get("discovered_at"),
            "note": "NY Fed Survey of Primary Dealers (funding/balance-sheet expectations). Structured extraction "
                    "pending a PDF-parsing layer — shown here as a reference until expectations are parsed."}

    # ── Composite liquidity regime — synthesize the impulses into ONE inflection read ──
    comp_parts = []

    def _num(x):
        return x if isinstance(x, (int, float)) else None

    def add_part(name, eff_z, weight):
        if isinstance(eff_z, (int, float)):
            comp_parts.append((name, max(-3.0, min(3.0, eff_z)), weight))

    add_part("net_liquidity", usd_z[-1] if usd_z else None, 0.28)
    add_part("reserves", reserves["eff_z"] if reserves else None, 0.16)
    if us_money and isinstance(us_money.get("real_m2_yoy_pct"), (int, float)):
        add_part("m2_growth", us_money["real_m2_yoy_pct"] / 3.0, 0.05)
    if stablecoin_full and isinstance(stablecoin_full.get("eff_z"), (int, float)):
        add_part("stablecoin_flow", stablecoin_full["eff_z"], 0.05)
    _gli = _num(global_liq.get("impulse_13w_pct")) if global_liq else None
    if _gli is not None:
        add_part("global_liquidity", _gli / 1.5, 0.16)
    add_part("dollar", dollar["eff_z"] if dollar else None, 0.12)
    _fps = _num(fp.get("plumbing_stress_score")) if fp else None
    if _fps is not None:
        add_part("funding_stress", -(_fps - 50) / 20.0, 0.12)
    _mvl = _num(mv.get("level")) if mv else None
    if _mvl is not None:
        add_part("move", -(_mvl - 90) / 30.0, 0.06)
    if hy_bps is not None:
        add_part("credit", -(hy_bps - 350) / 150.0, 0.10)
    # stress overlays — dollar shortage, settlement fails, dash-for-cash flows drag liquidity down
    if ds_status == "SCRAMBLE":
        add_part("dollar_shortage", -1.5, 0.06)
    elif ds_status == "WATCH":
        add_part("dollar_shortage", -0.6, 0.06)
    if settlement_fails and isinstance(settlement_fails.get("z"), (int, float)):
        add_part("settlement_fails", -settlement_fails["z"], 0.05)
    if flow_divergence:
        _fr = flow_divergence["regime"]
        if _fr == "DASH_FOR_CASH":
            add_part("flow_divergence", -1.5, 0.05)
        elif _fr == "FLIGHT_TO_SAFETY":
            add_part("flow_divergence", -0.7, 0.05)
        elif _fr == "RISK_SEEKING":
            add_part("flow_divergence", 0.5, 0.05)
    if leverage_stress and isinstance(leverage_stress.get("score"), (int, float)):
        add_part("leverage_stress", -(leverage_stress["score"] - 50) / 30.0, 0.05)
    composite = None
    if comp_parts:
        wsum = sum(w for _, _, w in comp_parts)
        comp_z = sum(z * w for _, z, w in comp_parts) / wsum
        comp_score = max(0.0, min(100.0, round(50 + comp_z * 16.5, 1)))
        comp_regime = ("EXPANDING" if comp_z > 0.3 else "CONTRACTING" if comp_z < -0.3 else "NEUTRAL")
        composite = {"liquidity_score": comp_score, "composite_z": round(comp_z, 3), "regime": comp_regime,
                     "n_components": len(comp_parts),
                     "components": [{"name": n, "eff_z": round(z, 2), "weight": w} for n, z, w in comp_parts],
                     "read": {
                         "EXPANDING": "Liquidity is inflecting UP — the tide is turning supportive for risk. Historically a tailwind 1-3 months out.",
                         "CONTRACTING": "Liquidity is inflecting DOWN — draining conditions. Historically a headwind; favor quality and hedges.",
                         "NEUTRAL": "Liquidity is roughly flat — no strong second-derivative push. Levels, earnings and rates dominate."}[comp_regime]}

    # ── Trajectory — where liquidity is HEADED (forward plumbing mechanics) ──
    tvotes, treasons = [], []

    def _vote(cond_down, cond_up, reason_down, reason_up):
        if cond_down:
            tvotes.append(-1); treasons.append(reason_down)
        elif cond_up:
            tvotes.append(1); treasons.append(reason_up)
    if usd_z:
        _vote(usd_z[-1] < -0.25, usd_z[-1] > 0.25,
              "Net-liquidity impulse falling", "Net-liquidity impulse rising")
    if rrp_state and isinstance(rrp_state.get("level_usd_bn"), (int, float)) and rrp_state["level_usd_bn"] < 100:
        tvotes.append(-1); treasons.append("RRP buffer exhausted — further drains land on reserves directly")
    if tga_state:
        _vote(tga_state.get("direction") == "RISING", tga_state.get("direction") == "FALLING",
              "TGA rebuilding — pulls cash out of the system", "TGA drawing down — releases cash into the system")
    if reserves and isinstance(reserves.get("scarcity_note"), str) and "Below" in reserves["scarcity_note"]:
        tvotes.append(-1); treasons.append("Reserves below comfort floor — little room before funding stress")
    if global_liq and isinstance(global_liq.get("impulse_13w_pct"), (int, float)):
        _vote(global_liq["impulse_13w_pct"] < -0.5, global_liq["impulse_13w_pct"] > 0.5,
              "Global central-bank liquidity contracting", "Global central-bank liquidity expanding")
    if dollar:
        _vote(dollar.get("direction") == "RISING", dollar.get("direction") == "FALLING",
              "Broad dollar strengthening — global liquidity headwind", "Broad dollar weakening — global liquidity tailwind")
    if ds_status == "SCRAMBLE":
        tvotes.append(-1); treasons.append("Dollar-shortage scramble underway")
    if flow_divergence and flow_divergence["regime"] == "DASH_FOR_CASH":
        tvotes.append(-1); treasons.append("Cross-asset dash-for-cash — deleveraging in progress")
    tv = sum(tvotes)
    heading = ("TIGHTENING AHEAD" if tv <= -2 else "EASING AHEAD" if tv >= 2 else "STABLE / MIXED")
    trajectory = {"heading": heading, "vote": tv, "n_signals": len(tvotes), "drivers": treasons,
                  "read": {
                      "TIGHTENING AHEAD": "Forward mechanics point to draining liquidity — buffers thin and drains landing on reserves. A headwind is building; favor quality, keep hedges on.",
                      "EASING AHEAD": "Forward mechanics point to improving liquidity — buffers refilling or drains reversing. A tailwind is building for risk assets.",
                      "STABLE / MIXED": "Forward drivers are mixed — no decisive path. Liquidity likely range-bound near current conditions."}[heading]}

    # ── FEATURE 5 quick wins: runway / forward expectation / tensions / data-health ──
    # (a) reserve runway countdown
    reserve_runway = None
    if wresbal and len(wresbal) > 10:
        wdr = sorted(wresbal)
        lvl = wresbal[wdr[-1]] / 1000.0                         # $bn
        base = wresbal[wdr[-9]] / 1000.0 if len(wdr) > 9 else wresbal[wdr[0]] / 1000.0
        pace = (lvl - base) / 8.0                               # $bn/wk
        DANGER, COMFORT = 2700.0, 3000.0
        status = "DRAINING" if pace < -1 else ("RISING" if pace > 1 else "FLAT")
        wks = round((lvl - DANGER) / abs(pace)) if (pace < -1 and lvl > DANGER) else None
        reserve_runway = {"level_usd_bn": round(lvl), "weekly_pace_bn": round(pace, 1), "status": status,
                          "danger_floor_bn": DANGER, "comfort_floor_bn": COMFORT,
                          "below_comfort": lvl < COMFORT, "weeks_to_danger": wks, "as_of": wdr[-1],
                          "read": (f"Reserves ${round(lvl):,}bn are {'below' if lvl < COMFORT else 'above'} the "
                                   f"~${int(COMFORT):,}bn ample floor and {status.lower()} ~${abs(round(pace,1))}bn/wk. "
                                   + (f"With RRP near-empty, at this pace they reach the ~${int(DANGER):,}bn scarcity "
                                      f"zone in ~{wks} weeks." if wks else
                                      "No drain countdown — reserves are stable or rising."))}

    # (b) live forward-expectation readout (map current impulse state → regime study)
    def _state_of(zz):
        if zz > 0.5:
            return "EXPANDING_FAST"
        if zz > 0.05:
            return "EXPANDING"
        if zz >= -0.05:
            return "FLAT"
        if zz >= -0.5:
            return "CONTRACTING"
        return "CONTRACTING_FAST"
    forward_expectation = None
    if usd_z:
        cz = usd_z[-1]
        cst = _state_of(cz)
        assets = {}
        for asset, rr in (regime_returns or {}).items():
            d21 = ((rr.get("states") or {}).get(cst) or {}).get("d21")
            if d21:
                assets[asset] = {"mean": d21["mean"], "excess": d21["excess"], "n": d21["n"],
                                 "sig": d21["sig"], "hit_pct": d21["hit_pct"],
                                 "baseline": (rr.get("baseline") or {}).get("d21")}
        if assets:
            forward_expectation = {"state": cst, "impulse_z": round(cz, 2), "horizon": "21d", "assets": assets,
                                   "note": ("Reads the live impulse state off the large-n regime study — the model's "
                                            "21-day forward expectation conditioned on where liquidity is now. Excess "
                                            "is vs each asset's unconditional baseline.")}

    # composite forward expectation — conditioned on the FULL multi-factor state
    forward_expectation_composite = None
    if chist and chist.get("comp_z") and regime_returns_composite:
        czc = chist["comp_z"][-1]
        cstc = _state_of(czc)
        assets_c = {}
        for asset, rr in regime_returns_composite.items():
            d21 = ((rr.get("states") or {}).get(cstc) or {}).get("d21")
            if d21:
                assets_c[asset] = {"mean": d21["mean"], "excess": d21["excess"], "n": d21["n"],
                                   "sig": d21["sig"], "hit_pct": d21["hit_pct"],
                                   "baseline": (rr.get("baseline") or {}).get("d21")}
        if assets_c:
            forward_expectation_composite = {
                "state": cstc, "composite_z": round(czc, 3), "horizon": "21d", "assets": assets_c,
                "source": chist_src, "components_used": chist.get("components_used"),
                "note": ("Same readout but conditioned on the FULL multi-factor composite state (all components, "
                         "not just net-liq impulse). Currently driven by the "
                         + ("real all-component snapshot history." if chist_src == "snapshots"
                            else "FRED-backed composite reconstruction; auto-upgrades to the snapshot history once "
                            "enough has accumulated."))}

    # (c) tension / divergence detector — hidden fragility under a calm headline
    tensions = []
    calm = bool(composite and composite.get("liquidity_score", 0) >= 45)
    if composite and trajectory and composite.get("regime") in ("ABUNDANT", "AMPLE", "NEUTRAL") and heading == "TIGHTENING AHEAD":
        tensions.append({"severity": "medium", "signal": "headline vs forward",
                         "note": f"Composite reads {composite['regime']} but forward mechanics are TIGHTENING AHEAD — today's calm may not persist."})
    fp = (funding_stress or {}).get("funding_plumbing") or {}
    if calm and fp.get("score") is not None and fp["score"] < 60:
        tensions.append({"severity": "high", "signal": "funding plumbing",
                         "note": f"Headline liquidity benign, but funding plumbing is stressed ({fp.get('score')}/100, {fp.get('regime')}) — repo/reserve plumbing tightening beneath the surface."})
    if swap_lines and isinstance(swap_lines.get("srf_bn"), (int, float)) and swap_lines["srf_bn"] > 1:
        tensions.append({"severity": "high", "signal": "SRF backstop usage",
                         "note": f"The Fed's Standing Repo Facility is being used (${swap_lines['srf_bn']}bn, {swap_lines.get('srf_active_days')}d active) — normally ~$0; dealers needed the emergency backstop."})
    if treasury_auctions and treasury_auctions.get("regime") in ("STRESS", "CRISIS") and calm:
        tensions.append({"severity": "high" if treasury_auctions["regime"] == "CRISIS" else "medium", "signal": "Treasury auction demand",
                         "note": f"Headline calm, but Treasury auctions are showing {treasury_auctions['regime']} ({treasury_auctions.get('composite_score')}/100) — {treasury_auctions.get('interpretation', '')}"})
    if stablecoin_full and stablecoin_full.get("state") == "CONTRACTING" and calm:
        tensions.append({"severity": "medium", "signal": "stablecoin outflow",
                         "note": f"Fiat liquidity calm, but stablecoin supply is contracting (signal {stablecoin_full.get('signal_strength')}/100) — crypto-dollar liquidity draining, a distinct channel."})
    if dollar_shortage and dollar_shortage.get("status") in ("WATCH", "SCRAMBLE"):
        tensions.append({"severity": "high" if dollar_shortage["status"] == "SCRAMBLE" else "medium", "signal": "dollar shortage",
                         "note": f"Offshore USD funding shows {dollar_shortage['status']} while the headline is calm — cross-currency strain building."})
    if settlement_fails and (settlement_fails.get("pctile") or 0) > 75:
        tensions.append({"severity": "medium", "signal": "settlement fails",
                         "note": f"UST settlement fails at the {settlement_fails.get('pctile')}th percentile — collateral plumbing stress not reflected in the headline."})
    if reserve_runway and reserve_runway.get("weeks_to_danger") and reserve_runway["weeks_to_danger"] < 26 and calm:
        tensions.append({"severity": "high", "signal": "reserve runway",
                         "note": f"Reserves on pace to hit the scarcity zone in ~{reserve_runway['weeks_to_danger']} weeks with RRP empty — drains now land directly on reserves."})
    tension_state = {"count": len(tensions), "level": ("ELEVATED" if any(t["severity"] == "high" for t in tensions)
                     else "WATCH" if tensions else "ALIGNED"), "items": tensions,
                     "read": ("Sub-signals disagree with the calm headline — hidden fragility worth respecting." if tensions
                              else "Sub-signals are broadly aligned with the headline; no hidden divergences flagged.")}

    # (d) data-health strip — per-feed freshness
    def _age_days(s):
        try:
            return round((datetime.now(timezone.utc).date() - _pd(s[:10])).days)
        except Exception:
            return None

    def _feed_asof(key):
        fd = pull(key) or {}
        return (fd.get("as_of") or fd.get("generated_at") or fd.get("updated_at") or
                fd.get("last_updated") or (fd.get("headline") or {}).get("as_of")
                or (fd.get("signal") or {}).get("as_of"))
    data_health = []
    for nm, asof, maxd in [("Net liquidity (FRED)", usd_dates[-1] if usd_dates else None, 9),
                           ("Reserves (WRESBAL)", reserve_runway.get("as_of") if reserve_runway else None, 9),
                           ("Funding plumbing", _feed_asof("data/funding-plumbing.json"), 4),
                           ("Settlement fails", _feed_asof("data/settlement-fails.json"), 12),
                           ("Eurodollar plumbing", _feed_asof("data/eurodollar-plumbing.json"), 5),
                           ("MOVE (rates vol)", _feed_asof("data/move-index.json"), 5)]:
        ag = _age_days(asof) if asof else None
        data_health.append({"feed": nm, "as_of": (asof[:10] if asof else None), "age_days": ag,
                            "status": ("stale" if (ag is not None and ag > maxd) else "fresh" if ag is not None else "n/a")})

    try:
        _bp = brain_predictors()
    except Exception as e:
        print(f"[brain_predictors] {str(e)[:80]}"); _bp = {}
    out = {"engine": "liquidity-inflection", "version": VERSION,
           "generated_at": datetime.now(timezone.utc).isoformat(),
           "duration_s": round(time.time() - t0, 1), "availability": avail,
           "usd": {"as_of": usd_dates[-1] if usd_dates else None,
                   "impulse_z": usd_z[-1] if usd_z else None,
                   "net_liq_usd_bn": round(net[sorted(net)[-1]] / 1000, 1) if net else None,
                   "state": ("EXPANDING" if usd_z and usd_z[-1] > 0.25 else
                             "CONTRACTING" if usd_z and usd_z[-1] < -0.25 else "NEUTRAL"),
                   "last_flip": usd_flips[-1] if usd_flips else None,
                   "n_flips_10y": len(flips10),
                   "impulse_tail_180d": [[d, z] for d, z in zip(usd_dates[-180:], usd_z[-180:])]},
           "us_money": us_money,
           "composite": composite, "trajectory": trajectory, "projection": projection,
           "onshore_funding": _onshore_funding(),
           "treasury_auctions": treasury_auctions, "stablecoin_full": stablecoin_full,
           "cycle_clock": clock, "composite_clock": composite_clock,
           "composite_projection": composite_projection,
           "reserve_runway": reserve_runway, "forward_expectation": forward_expectation,
           "forward_expectation_composite": forward_expectation_composite,
           "regime_returns_composite": regime_returns_composite,
           "tensions": tension_state, "data_health": data_health,
           "analogs": analogs, "backtest": backtest,
           "reserves": reserves, "rrp": rrp_state, "tga": tga_state,
           "funding_stress": funding_stress, "global_liquidity": global_liq,
           "dollar": dollar, "dollar_shortage": dollar_shortage,
           "settlement_fails": settlement_fails, "swap_lines": swap_lines,
           "flow_divergence": flow_divergence,
           "leverage_stress": leverage_stress, "dealer_survey": dealer_survey,
           "credit": credit, "systemic_stress": systemic,
           "financial_conditions": fin_cond, "china_engine": china_engine,
           "eur": eur_state, "china": cn_state, "stablecoin": sc_state,
           "stablecoin_schema_hint": sc_schema_hint,
           "event_study_after_flips": studies, "lead_estimates": leads,
           "regime_returns": regime_returns, "lead_curves": lead_curves, "flip_log": flip_logs,
           "signals_logged": n_logged,
           "brain_predictors": _bp,
           "methodology": ("Net-liquidity impulse = 13-week slope of WALCL−TGA−RRP, 3y z-score; flips "
                           "debounced |Δz|≥0.25. The composite liquidity regime blends the second "
                           "derivatives of net liquidity, bank reserves, global central-bank liquidity and "
                           "the broad dollar with funding-stress (SOFR-IORB, eurodollar plumbing), MOVE and "
                           "credit spreads (inverted). Edge tables are real event studies over the last "
                           "decade's net-liq flips (n shown), not assertions. New flips are logged to the "
                           "closed loop vs SPY at 5/21/63d.")}
    try:
        snap_meta = snapshot_composite(out)
        if snap_meta:
            out["composite_snapshots"] = snap_meta
    except Exception as e:
        print(f"[snapshot] {str(e)[:80]}")
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=1800")
    try:
        brief = desk_briefing(out)
        S3.put_object(Bucket=BUCKET, Key="data/liquidity-inflection-decisive-call.json",
                      Body=json.dumps(brief, default=str).encode(),
                      ContentType="application/json", CacheControl="public, max-age=1800")
    except Exception as e:
        print(f"[briefing] {str(e)[:80]}")
    print(f"[liq-inflect] z={out['usd']['impulse_z']} state={out['usd']['state']} flips10y={len(flips10)} {out['duration_s']}s")
    return {"statusCode": 200, "body": json.dumps({"state": out["usd"]["state"], "logged": n_logged})}
