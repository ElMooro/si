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
import json, os, time, urllib.request, urllib.parse
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
VERSION = "1.2.0"
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

    # Stablecoin acceleration (platform brief)
    sc = s3_json("data/stablecoin-flow.json")
    sc_state = None
    if sc:
        hist = None
        for f in ("history", "series", "mcap_history", "daily"):
            if isinstance(sc.get(f), list) and len(sc[f]) > 30:
                hist = sc[f]
                break
        if hist:
            vals = []
            for h in hist:
                v = h.get("total_mcap") or h.get("mcap") or h.get("value") or h.get("total")
                if v is not None:
                    vals.append(float(v))
            if len(vals) > 30:
                d1 = [vals[i] - vals[i - 1] for i in range(1, len(vals))]
                d2 = [d1[i] - d1[i - 1] for i in range(1, len(d1))]
                m, sd = mean(d2[-180:]), (stdev(d2[-180:]) if len(d2) > 30 else 0)
                sc_state = {"accel_z": round((d2[-1] - m) / sd, 2) if sd else 0,
                            "n_points": len(vals), "as_of": sc.get("generated_at", "")[:10]}
    if not sc_state and isinstance(sc, dict):
        ag = sc.get("aggregate") or {}
        if isinstance(ag, dict) and ag:
            nums = {k: v for k, v in ag.items() if isinstance(v, (int, float))}
            sc_state = {"mode": "state_passthrough", "state": sc.get("state"),
                        "signal_strength": sc.get("signal_strength"),
                        "aggregate": dict(list(nums.items())[:6]),
                        "as_of": sc.get("as_of")}
    avail["stablecoin"] = bool(sc_state)
    sc_schema_hint = (sorted(sc.keys())[:12] if (sc and not sc_state) else None)

    # Lead/lag event-study vs SPX / BTC / HYG
    spx_doc = s3_json("data/spx-history-deep.json") or {}
    spx = {d: float(v) for d, v in (spx_doc.get("points") or []) if v is not None}
    btc = polygon_closes("X:BTCUSD")
    hyg = polygon_closes("HYG")
    studies, leads = {}, {}
    flips10 = [f for f in usd_flips if f["date"] >= "2015-06-01"]
    for name, px in (("SPX_proxy", spx), ("BTC", btc), ("HYG", hyg)):
        if len(px) > 500 and flips10:
            dd = sorted(px)
            studies[name] = event_study(flips10, px, dd)
            leads[name] = best_lead(usd_dates, usd_z, px)

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

    # ── Composite liquidity regime — synthesize the impulses into ONE inflection read ──
    comp_parts = []

    def _num(x):
        return x if isinstance(x, (int, float)) else None

    def add_part(name, eff_z, weight):
        if isinstance(eff_z, (int, float)):
            comp_parts.append((name, max(-3.0, min(3.0, eff_z)), weight))

    add_part("net_liquidity", usd_z[-1] if usd_z else None, 0.28)
    add_part("reserves", reserves["eff_z"] if reserves else None, 0.16)
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
           "composite": composite,
           "reserves": reserves, "rrp": rrp_state, "tga": tga_state,
           "funding_stress": funding_stress, "global_liquidity": global_liq,
           "dollar": dollar, "credit": credit, "systemic_stress": systemic,
           "financial_conditions": fin_cond, "china_engine": china_engine,
           "eur": eur_state, "china": cn_state, "stablecoin": sc_state,
           "stablecoin_schema_hint": sc_schema_hint,
           "event_study_after_flips": studies, "lead_estimates": leads,
           "signals_logged": n_logged,
           "methodology": ("Net-liquidity impulse = 13-week slope of WALCL−TGA−RRP, 3y z-score; flips "
                           "debounced |Δz|≥0.25. The composite liquidity regime blends the second "
                           "derivatives of net liquidity, bank reserves, global central-bank liquidity and "
                           "the broad dollar with funding-stress (SOFR-IORB, eurodollar plumbing), MOVE and "
                           "credit spreads (inverted). Edge tables are real event studies over the last "
                           "decade's net-liq flips (n shown), not assertions. New flips are logged to the "
                           "closed loop vs SPY at 5/21/63d.")}
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=1800")
    print(f"[liq-inflect] z={out['usd']['impulse_z']} state={out['usd']['state']} flips10y={len(flips10)} {out['duration_s']}s")
    return {"statusCode": 200, "body": json.dumps({"state": out["usd"]["state"], "logged": n_logged})}
