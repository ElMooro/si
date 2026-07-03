"""justhodl-gex-desk v1.0 — DEALER GAMMA EXPOSURE (GEX) desk.

For an index + mega-cap universe, pulls the Polygon options chain (greeks + OI),
and computes institutional-grade gamma positioning:
  - Net GEX ($ per 1%% move), call vs put dealer gamma
  - Gamma-flip level (zero-gamma) via Black-Scholes re-pricing across a spot grid
    (proper method using each contract's IV/T, not a cumulative-strike proxy)
  - Call wall / put wall (gamma-concentration barriers)
  - Max pain (OI-weighted writer-loss minimum)
  - 0DTE GEX vs full near-term chain
  - Regime tag: net GEX>0 = vol-suppressed/mean-reverting (dealers long gamma,
    sell rallies/buy dips); net GEX<0 = vol-amplifying/trending
Index-level market read from SPY. Refreshes intraday. Real Polygon data only.

Convention: dealer gamma = call_gamma·call_OI − put_gamma·put_OI (SpotGamma-style;
positive = net long dealer gamma = stabilizing). Dollar gamma = γ·OI·100·S²·0.01.

Feeds: data/gex-desk.json (+ data/history/gex-desk.json, 120 snapshots).
"""
import os, json, math, time, urllib.request, urllib.error
from datetime import datetime, timezone
import boto3

BUCKET = "justhodl-dashboard-live"
OUT, HIST = "data/gex-desk.json", "data/history/gex-desk.json"
s3 = boto3.client("s3", region_name="us-east-1")
KEY = os.environ.get("POLYGON_KEY") or os.environ.get("POLY_KEY") or os.environ.get("POLYGON_API_KEY")
BASE = "https://api.polygon.io"
R_RATE = 0.045  # risk-free proxy for BS
SQRT2PI = math.sqrt(2 * math.pi)

UNIVERSE = ["SPY", "QQQ", "IWM", "DIA", "NVDA", "AAPL", "MSFT", "TSLA", "AMD", "META",
            "AMZN", "GOOGL", "AVGO", "NFLX", "MU", "PLTR", "COIN", "SMCI"]
INDEX_ETFS = {"SPY", "QQQ", "IWM", "DIA"}
EXP_MAX_DAYS = 60      # near-term chain that carries the gamma
STRIKE_BAND = 0.15     # ±15% around spot


def _get(url, tries=4):
    for i in range(tries):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "JustHodl/1.0"}), timeout=25) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code == 429 and i < tries - 1:
                time.sleep(3 + 4 * i); continue
            if e.code in (401, 403):
                raise
            return {}
        except Exception:
            if i < tries - 1:
                time.sleep(1); continue
            return {}
    return {}


def spot(sym):
    d = _get("%s/v2/snapshot/locale/us/markets/stocks/tickers/%s?apikey=%s" % (BASE, sym, KEY))
    t = d.get("ticker") or {}
    for v in (((t.get("lastTrade") or {}).get("p")), ((t.get("min") or {}).get("c")),
              ((t.get("day") or {}).get("c")), ((t.get("prevDay") or {}).get("c"))):
        if v:
            return float(v)
    d = _get("%s/v2/aggs/ticker/%s/prev?adjusted=true&apikey=%s" % (BASE, sym, KEY))
    r = (d.get("results") or [{}])
    return float(r[0].get("c")) if r and r[0].get("c") else None


def chain(sym, S):
    lo, hi = S * (1 - STRIKE_BAND), S * (1 + STRIKE_BAND)
    exp_max = (datetime.now(timezone.utc)).date()
    from datetime import timedelta
    exp_max = (datetime.now(timezone.utc) + timedelta(days=EXP_MAX_DAYS)).strftime("%Y-%m-%d")
    url = ("%s/v3/snapshot/options/%s?strike_price.gte=%.2f&strike_price.lte=%.2f"
           "&expiration_date.lte=%s&limit=250&apikey=%s" % (BASE, sym, lo, hi, exp_max, KEY))
    out, pages = [], 0
    while url and pages < 8:
        doc = _get(url)
        out += (doc.get("results") or [])
        nxt = doc.get("next_url")
        url = (nxt + "&apikey=" + KEY) if nxt else None
        pages += 1
        if url:
            time.sleep(0.12)
    return out


def _norm_pdf(x):
    return math.exp(-x * x / 2.0) / SQRT2PI


def _bs_gamma(S, K, T, sig):
    if S <= 0 or K <= 0 or T <= 0 or sig <= 0:
        return 0.0
    d1 = (math.log(S / K) + (R_RATE + sig * sig / 2.0) * T) / (sig * math.sqrt(T))
    return _norm_pdf(d1) / (S * sig * math.sqrt(T))


def dealer_gex(contracts, S):
    """Net dealer $-gamma at spot using Polygon's per-contract gamma."""
    net = cg = pg = 0.0
    by_strike = {}
    oi_c, oi_p = {}, {}
    for c in contracts:
        det = c.get("details") or {}
        g = (c.get("greeks") or {}).get("gamma")
        oi = c.get("open_interest") or 0
        K = det.get("strike_price")
        ctype = det.get("contract_type")
        if not K or not oi:
            continue
        dg = (g or 0.0) * oi * 100 * S * S * 0.01
        if ctype == "call":
            net += dg; cg += dg; oi_c[K] = oi_c.get(K, 0) + oi
            by_strike[K] = by_strike.get(K, 0.0) + dg
        elif ctype == "put":
            net -= dg; pg += dg; oi_p[K] = oi_p.get(K, 0) + oi
            by_strike[K] = by_strike.get(K, 0.0) - dg
    return net, cg, pg, by_strike, oi_c, oi_p


def gamma_flip(contracts, S):
    """Zero-gamma level via BS re-pricing of the whole chain across a spot grid."""
    now = datetime.now(timezone.utc)
    grid = [S * (0.90 + 0.01 * i) for i in range(21)]  # ±10%
    prepped = []
    for c in contracts:
        det = c.get("details") or {}
        oi = c.get("open_interest") or 0
        K = det.get("strike_price")
        iv = c.get("implied_volatility")
        exp = det.get("expiration_date")
        ctype = det.get("contract_type")
        if not K or not oi or not iv or not exp:
            continue
        try:
            ed = datetime.fromisoformat(exp).replace(tzinfo=timezone.utc)
        except Exception:
            continue
        T = max((ed - now).total_seconds() / 86400.0, 0.5) / 365.0
        prepped.append((K, float(iv), T, 1.0 if ctype == "call" else -1.0, oi))
    curve, prev, flip = [], None, None
    for Sx in grid:
        net = 0.0
        for K, iv, T, sign, oi in prepped:
            net += sign * _bs_gamma(Sx, K, T, iv) * oi * 100 * Sx * Sx * 0.01
        curve.append([round(Sx, 2), round(net / 1e9, 4)])
        if prev is not None and (prev[1] < 0) != (net < 0) and (net - prev[1]) != 0:
            flip = prev[0] + (Sx - prev[0]) * (-prev[1]) / (net - prev[1])
        prev = (Sx, net)
    return (round(flip, 2) if flip else None), curve


def max_pain(oi_c, oi_p):
    strikes = sorted(set(list(oi_c) + list(oi_p)))
    if not strikes:
        return None
    best, bestpain = None, None
    for Kp in strikes:
        pain = 0.0
        for K in strikes:
            if Kp > K:
                pain += (Kp - K) * oi_c.get(K, 0)
            elif Kp < K:
                pain += (K - Kp) * oi_p.get(K, 0)
        if bestpain is None or pain < bestpain:
            bestpain, best = pain, Kp
    return best


def analyze(sym):
    S = spot(sym)
    if not S:
        return {"status": "no_spot"}
    contracts = chain(sym, S)
    if not contracts:
        return {"status": "no_chain", "spot": S}
    net, cg, pg, by_strike, oi_c, oi_p = dealer_gex(contracts, S)
    flip, curve = gamma_flip(contracts, S)
    cw = max(by_strike, key=lambda k: by_strike[k]) if by_strike else None
    pw = min(by_strike, key=lambda k: by_strike[k]) if by_strike else None
    mp = max_pain(oi_c, oi_p)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    zdte = [c for c in contracts if (c.get("details") or {}).get("expiration_date") == today]
    znet = dealer_gex(zdte, S)[0] if zdte else None
    regime = "vol-suppressed" if net > 0 else "vol-amplifying"
    return {"status": "LIVE", "spot": round(S, 2), "n_contracts": len(contracts),
            "net_gex_bn": round(net / 1e9, 3), "call_gamma_bn": round(cg / 1e9, 3),
            "put_gamma_bn": round(pg / 1e9, 3), "gamma_flip": flip,
            "dist_to_flip_pct": (round((S / flip - 1) * 100, 2) if flip else None),
            "call_wall": cw, "put_wall": pw, "max_pain": mp,
            "zero_dte_gex_bn": (round(znet / 1e9, 3) if znet is not None else None),
            "regime": regime, "is_index": sym in INDEX_ETFS,
            "flip_curve": curve}


def lambda_handler(event=None, context=None):
    now = datetime.now(timezone.utc)
    names = {}
    for sym in UNIVERSE:
        try:
            names[sym] = analyze(sym)
        except urllib.error.HTTPError as e:
            if e.code in (401, 403):
                return {"ok": False, "error": "polygon options not authorized (%s)" % e.code}
            names[sym] = {"status": "err", "code": e.code}
        except Exception as e:
            names[sym] = {"status": "err", "msg": str(e)[:80]}
        time.sleep(0.1)

    live = {k: v for k, v in names.items() if v.get("status") == "LIVE"}
    spy = names.get("SPY", {})
    # market read from SPY
    read = None
    if spy.get("status") == "LIVE":
        rg = spy["regime"]; flip = spy.get("gamma_flip"); S = spy["spot"]
        read = ("SPY net GEX %s ($%.2fB/1%%) — dealers %s → %s. Spot %.2f vs gamma-flip %s (%s); "
                "call wall %s, put wall %s, max pain %s." % (
                    "POSITIVE" if spy["net_gex_bn"] > 0 else "NEGATIVE", spy["net_gex_bn"],
                    "long gamma, sell rallies/buy dips" if spy["net_gex_bn"] > 0 else "short gamma, chase moves",
                    "vol suppressed, mean-reverting" if spy["net_gex_bn"] > 0 else "vol amplified, trend-prone",
                    S, flip if flip else "n/a",
                    ("above flip" if flip and S > flip else "below flip" if flip else "—"),
                    spy.get("call_wall"), spy.get("put_wall"), spy.get("max_pain")))
    # index aggregate net gamma sign
    idx_net = sum(v["net_gex_bn"] for k, v in live.items() if v.get("is_index"))
    doc = {"engine": "justhodl-gex-desk", "version": "1.0.0",
           "generated_at": now.isoformat(timespec="seconds"),
           "convention": "dealer gamma = call_g*call_OI - put_g*put_OI; +=long gamma/stabilizing; $ per 1%% move; near-term <=%dDTE, strikes +/-%d%%" % (EXP_MAX_DAYS, int(STRIKE_BAND * 100)),
           "universe": UNIVERSE, "n_live": len(live), "index_net_gex_bn": round(idx_net, 3),
           "read": read, "names": names, "status": "LIVE" if live else "DEGRADED"}
    # history (light snapshot)
    try:
        hist = json.loads(s3.get_object(Bucket=BUCKET, Key=HIST)["Body"].read())
    except Exception:
        hist = {"snapshots": {}}
    hist["snapshots"][now.strftime("%Y-%m-%dT%H:%M")] = {
        k: {"net_gex_bn": v.get("net_gex_bn"), "spot": v.get("spot"), "gamma_flip": v.get("gamma_flip")}
        for k, v in live.items()}
    hist["snapshots"] = dict(sorted(hist["snapshots"].items())[-120:])
    s3.put_object(Bucket=BUCKET, Key=HIST, Body=json.dumps(hist, separators=(",", ":"), allow_nan=False).encode(),
                  ContentType="application/json", CacheControl="public, max-age=120")
    s3.put_object(Bucket=BUCKET, Key=OUT, Body=json.dumps(doc, separators=(",", ":"), allow_nan=False).encode(),
                  ContentType="application/json", CacheControl="public, max-age=120")
    return {"ok": True, "status": doc["status"], "n_live": len(live),
            "spy_net_gex_bn": spy.get("net_gex_bn"), "spy_flip": spy.get("gamma_flip"),
            "index_net_gex_bn": round(idx_net, 3), "read": read}
