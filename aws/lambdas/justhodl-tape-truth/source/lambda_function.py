"""justhodl-tape-truth -- is the move genuine or manufactured?
Marker: tape-truth v1.0.0
Honesty ledger: CVD is BAR-APPROXIMATED (minute close-in-range
delta, never claimed as tick data); GEX uses the standard
dealer-positioning assumption (long calls, short puts vs
customers); verdicts are hypotheses that cite every number they
rest on, and go INSUFFICIENT rather than guess.
"""
import gzip
import json
import os
import urllib.request
from datetime import datetime, timezone, timedelta

import boto3

VERSION = "1.0.0"
REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/tape-truth.json"
CVD_LEDGER = "data/providers/tape/cvd-daily.json"
FINRA_LEDGER = "data/providers/finra/shortvol.json"
WATCH = ["SPY", "QQQ", "IWM", "DIA", "NVDA", "MSFT", "AAPL",
         "AMZN", "META", "GOOGL", "TSLA", "AVGO"]
GEX_SYMS = ["SPY", "QQQ", "IWM", "_SPX", "NVDA", "TSLA"]
LEDGER_KEEP = 60
NEAR_DTE = 45

s3 = boto3.client("s3", region_name=REGION)


def _g(key):
    try:
        raw = s3.get_object(Bucket=BUCKET,
                            Key=key)["Body"].read()
        if raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        return json.loads(raw)
    except Exception:  # noqa: BLE001
        return None


def _put(key, obj):
    s3.put_object(Bucket=BUCKET, Key=key,
                  Body=json.dumps(obj).encode(),
                  ContentType="application/json",
                  CacheControl="no-cache")


def http_raw(url, headers=None):
    """Seam."""
    req = urllib.request.Request(
        url, headers=headers or
        {"User-Agent": "justhodl-tape-truth"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return r.read()


def bar_delta(o, h, l, c, v):
    """Close-in-range volume delta: v * (2*(c-l)/(h-l) - 1).
    Flat bar -> 0.  The classic bar approximation of CVD."""
    if h is None or l is None or c is None or v is None \
            or h <= l:
        return 0.0
    return v * (2.0 * (c - l) / (h - l) - 1.0)


def session_cvd(poly_key, sym, day):
    j = json.loads(http_raw(
        "https://api.polygon.io/v2/aggs/ticker/%s/range/1/"
        "minute/%s/%s?limit=50000&apiKey=%s"
        % (sym, day, day, poly_key)))
    rs = j.get("results") or []
    cvd = 0.0
    close = None
    for b in rs:
        cvd += bar_delta(b.get("o"), b.get("h"), b.get("l"),
                         b.get("c"), b.get("v"))
        close = b.get("c", close)
    return (round(cvd, 0), close, len(rs))


def finra_day(day):
    txt = http_raw(
        "https://cdn.finra.org/equity/regsho/daily/"
        "CNMSshvol%s.txt" % day.strftime("%Y%m%d")) \
        .decode("utf-8", "replace")
    out = {}
    for ln in txt.splitlines()[1:]:
        p = ln.split("|")
        if len(p) >= 5 and p[1] in WATCH:
            try:
                sv, tv = float(p[2]), float(p[4])
                if tv > 0:
                    out[p[1]] = round(sv / tv, 4)
            except ValueError:
                pass
    return out


def parse_occ(opt):
    """SPY261002P00810000 -> (exp yymmdd, 'P', strike)."""
    tail = opt[-15:]
    return tail[:6], tail[6], int(tail[7:]) / 1000.0


def gex_block(sym, today):
    try:
        raw = http_raw(
            "https://cdn.cboe.com/api/global/delayed_quotes/"
            "options/%s.json" % sym,
            headers={"User-Agent": "Mozilla/5.0",
                     "Referer": "https://www.cboe.com/"})
        j = json.loads(raw)
    except Exception as e:  # noqa: BLE001
        return {"status": "MISSING",
                "why": "cboe fetch %s" % str(e)[:50]}
    d = j.get("data") or {}
    spot = d.get("current_price") or d.get("close")
    opts = d.get("options") or []
    if not spot or not opts:
        return {"status": "MISSING", "why": "empty chain"}
    cutoff = (today + timedelta(days=NEAR_DTE)) \
        .strftime("%y%m%d")
    tstr = today.strftime("%y%m%d")
    call_g = put_g = 0.0
    by_strike = {}
    pc_oi = {"C": 0.0, "P": 0.0}
    pc_vol = {"C": 0.0, "P": 0.0}
    n_used = 0
    for o in opts:
        try:
            exp, cp, k = parse_occ(str(o.get("option", "")))
        except (ValueError, IndexError):
            continue
        if exp < tstr or exp > cutoff:
            continue
        g = o.get("gamma") or 0.0
        oi = o.get("open_interest") or 0.0
        if not oi:
            continue
        n_used += 1
        dollar = g * oi * 100.0 * spot * spot * 0.01
        pc_oi[cp] = pc_oi.get(cp, 0) + oi
        pc_vol[cp] = pc_vol.get(cp, 0) \
            + (o.get("volume") or 0)
        signed = dollar if cp == "C" else -dollar
        if cp == "C":
            call_g += dollar
        else:
            put_g += dollar
        if abs(k - spot) / spot <= 0.10:
            by_strike[k] = by_strike.get(k, 0.0) + signed
    net = call_g - put_g
    strikes = sorted(by_strike)
    flip = None
    cum = 0.0
    for k in strikes:
        prev = cum
        cum += by_strike[k]
        if prev < 0 <= cum or prev > 0 >= cum:
            flip = k
    walls = sorted(by_strike.items(),
                   key=lambda x: -abs(x[1]))[:5]
    return {"status": "LIVE", "spot": spot,
            "n_contracts_used": n_used,
            "net_gex_bn": round(net / 1e9, 2),
            "call_gex_bn": round(call_g / 1e9, 2),
            "put_gex_bn": round(put_g / 1e9, 2),
            "regime": "POSITIVE" if net > 0 else "NEGATIVE",
            "flip_approx": flip,
            "walls": [{"strike": k,
                       "net_gex_bn": round(v / 1e9, 2)}
                      for k, v in walls],
            "put_call_oi": round(pc_oi["P"]
                                 / max(1.0, pc_oi["C"]), 2),
            "put_call_vol": round(pc_vol["P"]
                                  / max(1.0, pc_vol["C"]), 2),
            "dte_window": NEAR_DTE,
            "audit": {"sum_call_dollar": round(call_g, 0),
                      "sum_put_dollar": round(put_g, 0)}}


def zlast(vals):
    if len(vals) < 8:
        return None
    m = sum(vals) / len(vals)
    var = sum((x - m) ** 2 for x in vals) / len(vals)
    if var == 0:
        return None
    return round((vals[-1] - m) / var ** 0.5, 2)


def verdict(sym, cv, fin, gex):
    ev = []
    if cv.get("status") != "LIVE" or cv.get("n_days", 0) < 5:
        return {"call": "WARMING",
                "why": "cvd ledger %d days -- verdicts need "
                       ">=5" % cv.get("n_days", 0)}
    p5 = cv["price_chg_5d_pct"]
    c5 = cv["cvd_5d"]
    ev.append("price 5d %+0.2f%%" % p5)
    ev.append("bar-CVD 5d %+0.0f sh" % c5)
    call = "NEUTRAL"
    if p5 > 0.5 and c5 < 0:
        call = "FAKE_UP_DISTRIBUTION"
        ev.append("price up on negative delta -- classic "
                  "exit-liquidity pattern")
    elif p5 < -0.5 and c5 > 0:
        call = "SHAKEOUT_ACCUMULATION"
        ev.append("price down on positive delta -- "
                  "accumulation into fear")
    elif p5 > 0.5 and c5 > 0:
        call = "GENUINE_UP"
    elif p5 < -0.5 and c5 < 0:
        call = "GENUINE_DOWN"
    if cv.get("top_divergence"):
        ev.append("20d price high with lower CVD high -- "
                  "top-divergence flag")
    if cv.get("bottom_divergence"):
        ev.append("20d price low with higher CVD low -- "
                  "bottom-divergence flag")
    if fin and fin.get("z_20d") is not None:
        ev.append("short-vol ratio %.2f (z %+0.2f)"
                  % (fin["ratio"], fin["z_20d"]))
        if fin["z_20d"] > 1.2 and p5 > 0.5:
            ev.append("elevated short-flow into strength -- "
                      "absorption watch")
    if gex and gex.get("status") == "LIVE":
        ev.append("net GEX %+0.2fbn (%s gamma)"
                  % (gex["net_gex_bn"],
                     gex["regime"].lower()))
        if gex["regime"] == "NEGATIVE" \
                and call.startswith("GENUINE"):
            ev.append("negative gamma -- dealers accelerate; "
                      "moves overshoot, genuine != stable")
        if gex["regime"] == "POSITIVE" \
                and call == "FAKE_UP_DISTRIBUTION":
            ev.append("positive gamma pins -- distribution "
                      "can proceed quietly near walls")
    return {"call": call, "evidence": ev}


def build(event=None):
    now = datetime.now(timezone.utc)
    today = now.date()
    poly = os.environ.get("POLYGON_API_KEY", "")
    doc = {"v": VERSION, "engine": "justhodl-tape-truth",
           "as_of": today.isoformat(),
           "generated_at": now.isoformat(),
           "status": "LIVE",
           "method": {
               "cvd": "bar-approximated: per-minute "
                      "v*(2*(c-l)/(h-l)-1), summed per "
                      "session; NEVER tick data",
               "gex": "CBOE delayed chain, DTE<=%d, "
                      "gamma*OI*100*spot^2*1%%; calls +, "
                      "puts - (standard dealer assumption); "
                      "flip = strike-ladder crossover "
                      "approximation" % NEAR_DTE,
               "verdicts": "evidence-cited hypotheses; "
                           "INSUFFICIENT/WARMING over "
                           "guessing"}}
    led = _g(CVD_LEDGER) or {"note": "session bar-CVD in "
                             "shares + close", "rows": {}}
    fled = _g(FINRA_LEDGER) or {"rows": {}}

    d = today
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    dstr = d.isoformat()
    cvd_fetch_err = None
    if poly:
        for sym in WATCH:
            try:
                cvd, close, nbars = session_cvd(poly, sym,
                                                dstr)
                if nbars > 100:
                    led["rows"].setdefault(sym, {})[dstr] = \
                        {"cvd": cvd, "close": close}
            except Exception as e:  # noqa: BLE001
                cvd_fetch_err = str(e)[:60]
    for sym in led["rows"]:
        days = sorted(led["rows"][sym])[-LEDGER_KEEP:]
        led["rows"][sym] = {k: led["rows"][sym][k]
                            for k in days}
    _put(CVD_LEDGER, led)

    try:
        ratios = finra_day(d)
    except Exception:  # noqa: BLE001
        try:
            d2 = d - timedelta(days=1)
            while d2.weekday() >= 5:
                d2 -= timedelta(days=1)
            ratios = finra_day(d2)
            dstr_f = d2.isoformat()
        except Exception as e:  # noqa: BLE001
            ratios, dstr_f = {}, None
            doc["finra_why"] = str(e)[:60]
        else:
            pass
    else:
        dstr_f = dstr
    if ratios and dstr_f:
        for sym, rt in ratios.items():
            fled["rows"].setdefault(sym, {})[dstr_f] = rt
        for sym in fled["rows"]:
            days = sorted(fled["rows"][sym])[-LEDGER_KEEP:]
            fled["rows"][sym] = {k: fled["rows"][sym][k]
                                 for k in days}
        _put(FINRA_LEDGER, fled)

    gex = {}
    for sym in GEX_SYMS:
        gex[sym] = gex_block(sym, today)

    symbols = {}
    for sym in WATCH:
        rows = led["rows"].get(sym) or {}
        days = sorted(rows)
        cv = {"status": "MISSING", "n_days": len(days)}
        if days:
            closes = [rows[k]["close"] for k in days]
            cvds = [rows[k]["cvd"] for k in days]
            cum = []
            s = 0.0
            for x in cvds:
                s += x
                cum.append(s)
            cv = {"status": "LIVE", "n_days": len(days),
                  "last_day": days[-1],
                  "session_cvd": cvds[-1],
                  "close": closes[-1],
                  "cvd_5d": round(sum(cvds[-5:]), 0),
                  "price_chg_5d_pct": round(
                      (closes[-1] / closes[max(0, len(closes)
                                               - 6)] - 1)
                      * 100, 2) if len(closes) >= 2 else 0.0,
                  "series": [{"d": days[i],
                              "close": closes[i],
                              "cum_cvd": round(cum[i], 0)}
                             for i in range(len(days))]}
            if len(days) >= 20:
                w_c = closes[-20:]
                w_v = cum[-20:]
                if w_c[-1] >= max(w_c) * 0.999 \
                        and w_v[-1] < max(w_v) * 0.999:
                    cv["top_divergence"] = True
                if w_c[-1] <= min(w_c) * 1.001 \
                        and w_v[-1] > min(w_v) * 1.001:
                    cv["bottom_divergence"] = True
        f = fled["rows"].get(sym) or {}
        fdays = sorted(f)
        fin = None
        if fdays:
            vals = [f[k] for k in fdays]
            fin = {"ratio": vals[-1], "n_days": len(vals),
                   "z_20d": zlast(vals[-20:])}
        symbols[sym] = {"cvd": cv, "short_vol": fin,
                        "gex": gex.get(sym),
                        "verdict": verdict(sym, cv, fin,
                                           gex.get(sym))}
    if cvd_fetch_err and not poly:
        doc["status"] = "PARTIAL"
    if not poly:
        doc["why"] = "POLYGON_API_KEY absent -- CVD leg dead"
    doc["symbols"] = symbols
    doc["gex_index"] = gex.get("_SPX")
    _put(OUT_KEY, doc)
    return doc


def lambda_handler(event, context):
    doc = build(event)
    return {"statusCode": 200,
            "body": json.dumps({"v": doc.get("v"),
                                "status": doc.get("status")})}
