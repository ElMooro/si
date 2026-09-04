"""justhodl-bond-warroom -- the global bond heartbeat (ops 5189, 2026-09-04).

Khalid: "bonds.html: an intelligence war-room command center -- monitor all the
main bond centers worldwide so I can read the bond heartbeat at a glance:
daily Treasury-auction signals, bond volatility (a daily sell-off = huge dump
risk for stocks; big daily buying = pump for risk assets), day-over-day
percentage change in Treasury yields flashing red when huge, Japan JGBs the
same way, every ICE BofA spread, the European metrics (BTP-Bund, IT-ES ...)
that detect a eurodollar shortage. Keep what is there; add these."

Sources (all real, refreshed every run):
  * TradingView chart bars (server-side WebSocket, same session the fleet's
    tv-bars engine uses) for sovereign yields: US02Y/05Y/10Y/30Y, DE02Y/10Y/30Y,
    FR10Y, IT02Y/10Y, ES10Y, NL10Y, PT10Y, GR10Y, GB02Y/10Y, CH10Y, JP02Y/10Y/30Y,
    AU10Y, CA10Y, CN10Y, KR10Y, IN10Y, BR10Y, MX10Y -- and TVC:MOVE.
  * Japan MOF daily JGB curve CSV (1Y..40Y, authoritative) -- current month.
  * FRED via the fleet proxy: DGS3MO/2/5/10/30, DFII10, T10YIE, SOFR, DTB3,
    DTWEXBGS, VIXCLS and the ICE BofA OAS family (US HY/IG/AAA/BBB/BB/B/CCC,
    Euro HY, EM corp / EM HY / EM sovereign).
  * Yahoo via the fleet proxy: ^MOVE, TLT, IEF, SHY, HYG, LQD, EMB, ^TNX.
  * Fleet feeds: data/auction-desk.json (today's auction verdict + cross-asset
    read), data/usd-funding.json, data/eurodollar-plumbing.json,
    data/move-index.json, data/crisis-plumbing.json.

Every series gets: last, previous, day-over-day change (bp for yields/spreads,
% for prices/indices) and its z-score against the trailing 250 daily changes,
5-day and 20-day change, 1-year percentile of the level, a 30-point sparkline
and a flag (GREEN / AMBER / RED) from fixed shock thresholds OR |z| >= 1.5/2.5.

Verdicts (deterministic, explained in words):
  * equity_risk  -- from TLT %, US10Y bp, MOVE and HY OAS: DUMP RISK when
    bonds sell off hard; PUMP SETUP when bonds are bought hard with credit calm;
    FLIGHT TO SAFETY when bonds are bought while credit widens.
  * eurodollar_shortage -- BTP-Bund / periphery widening + dollar up + EM and
    Euro HY spreads widening + the fleet's eurodollar-plumbing composite.
  * heartbeat -- 0..100 from the share of AMBER/RED across US rates, volatility,
    Japan, Europe, rest-of-world, credit and funding, with the loudest signals
    named in the headline.

Output: data/bond-warroom.json (+ data/warm/bond-warroom/tv.json.gz cache).
"""
import gzip
import json
import os
import statistics
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import boto3

try:
    import tv_pull
except Exception:  # pragma: no cover
    tv_pull = None

VERSION = "1.0.0"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/bond-warroom.json"
TV_KEY = "data/warm/bond-warroom/tv.json.gz"
PROXY = "https://justhodl-data-proxy.raafouis.workers.dev"
UA = "justhodl-bond-warroom/" + VERSION
s3 = boto3.client("s3", region_name="us-east-1")

TV_SYMBOLS = {
    "US02Y": ("TVC:US02Y", "US 2Y", "us", "yield"), "US05Y": ("TVC:US05Y", "US 5Y", "us", "yield"),
    "US10Y": ("TVC:US10Y", "US 10Y", "us", "yield"), "US30Y": ("TVC:US30Y", "US 30Y", "us", "yield"),
    "DE02Y": ("TVC:DE02Y", "Germany 2Y (Schatz)", "europe", "yield"), "DE10Y": ("TVC:DE10Y", "Germany 10Y (Bund)", "europe", "yield"),
    "DE30Y": ("TVC:DE30Y", "Germany 30Y", "europe", "yield"), "FR10Y": ("TVC:FR10Y", "France 10Y (OAT)", "europe", "yield"),
    "IT02Y": ("TVC:IT02Y", "Italy 2Y", "europe", "yield"), "IT10Y": ("TVC:IT10Y", "Italy 10Y (BTP)", "europe", "yield"),
    "ES10Y": ("TVC:ES10Y", "Spain 10Y (Bono)", "europe", "yield"), "NL10Y": ("TVC:NL10Y", "Netherlands 10Y", "europe", "yield"),
    "PT10Y": ("TVC:PT10Y", "Portugal 10Y", "europe", "yield"), "GR10Y": ("TVC:GR10Y", "Greece 10Y", "europe", "yield"),
    "GB02Y": ("TVC:GB02Y", "UK 2Y", "europe", "yield"), "GB10Y": ("TVC:GB10Y", "UK 10Y (Gilt)", "europe", "yield"),
    "CH10Y": ("TVC:CH10Y", "Switzerland 10Y", "europe", "yield"),
    "JP02Y": ("TVC:JP02Y", "Japan 2Y (JGB)", "japan", "yield"), "JP10Y": ("TVC:JP10Y", "Japan 10Y (JGB)", "japan", "yield"),
    "JP30Y": ("TVC:JP30Y", "Japan 30Y (JGB)", "japan", "yield"),
    "AU10Y": ("TVC:AU10Y", "Australia 10Y", "world", "yield"), "CA10Y": ("TVC:CA10Y", "Canada 10Y", "world", "yield"),
    "CN10Y": ("TVC:CN10Y", "China 10Y", "world", "yield"), "KR10Y": ("TVC:KR10Y", "Korea 10Y", "world", "yield"),
    "IN10Y": ("TVC:IN10Y", "India 10Y", "world", "yield"), "BR10Y": ("TVC:BR10Y", "Brazil 10Y", "world", "yield"),
    "MX10Y": ("TVC:MX10Y", "Mexico 10Y", "world", "yield"),
    "MOVE_TV": ("TVC:MOVE", "MOVE (TV)", "vol", "index"),
}
FRED_SERIES = {
    "DGS3MO": ("US 3M bill", "us", "yield"), "DGS2": ("US 2Y (FRED)", "us", "yield"), "DGS5": ("US 5Y (FRED)", "us", "yield"),
    "DGS10": ("US 10Y (FRED)", "us", "yield"), "DGS30": ("US 30Y (FRED)", "us", "yield"),
    "DFII10": ("US 10Y real (TIPS)", "us", "yield"), "T10YIE": ("10Y breakeven inflation", "us", "yield"),
    "SOFR": ("SOFR", "funding", "yield"), "DTB3": ("3M T-bill (secondary)", "funding", "yield"),
    "DTWEXBGS": ("Broad dollar index", "funding", "index"), "VIXCLS": ("VIX", "vol", "index"),
    "BAMLH0A0HYM2": ("US High Yield OAS", "credit", "spread"), "BAMLC0A0CM": ("US IG Corporate OAS", "credit", "spread"),
    "BAMLC0A1CAAA": ("US AAA OAS", "credit", "spread"), "BAMLC0A4CBBB": ("US BBB OAS", "credit", "spread"),
    "BAMLH0A1HYBB": ("US BB OAS", "credit", "spread"), "BAMLH0A2HYB": ("US B OAS", "credit", "spread"),
    "BAMLH0A3HYC": ("US CCC & lower OAS", "credit", "spread"), "BAMLHE00EHYIOAS": ("Euro High Yield OAS", "credit", "spread"),
    "BAMLEMCBPIOAS": ("EM Corporate OAS", "credit", "spread"), "BAMLEMHBHYCRPIOAS": ("EM High Yield OAS", "credit", "spread"),
    "BAMLEMPBPUBSICRPIOAS": ("EM Sovereign OAS", "credit", "spread"), "BAMLEMIBHGCRPIOAS": ("EM IG OAS", "credit", "spread"),
}
YAHOO = {"^MOVE": ("MOVE index", "vol", "index"), "TLT": ("TLT 20Y+ Treasury ETF", "vol", "price"), "IEF": ("IEF 7-10Y ETF", "vol", "price"),
         "SHY": ("SHY 1-3Y ETF", "vol", "price"), "HYG": ("HYG high-yield ETF", "vol", "price"), "LQD": ("LQD IG ETF", "vol", "price"),
         "EMB": ("EMB EM bond ETF", "vol", "price"), "^TNX": ("US 10Y (Yahoo)", "us", "yield")}

# shock thresholds: (RED, AMBER) in the series' own DoD unit (bp for yields/spreads, % for prices, points for indices)
THRESH = {"yield_us": (10, 6), "yield_dm": (10, 6), "yield_jp": (7, 4), "yield_em": (15, 9), "spread_periph": (10, 6),
          "oas_hy": (20, 12), "oas_ig": (7, 4), "oas_ccc": (35, 20), "oas_em": (15, 10), "price_bond": (1.5, 0.9), "move": (10, 6), "index": (999, 999), "vix": (5, 3)}


def _now():
    return datetime.now(timezone.utc)


def _iso():
    return _now().isoformat(timespec="seconds")


def _get(url, timeout=30, binary=False):
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "*/*", "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read() if binary else json.loads(r.read())


def _s3_json(key, default=None):
    try:
        raw = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        if key.endswith(".gz"):
            raw = gzip.decompress(raw)
        return json.loads(raw)
    except Exception:
        return default


def _put_json(key, obj, gz=False):
    body = json.dumps(obj, separators=(",", ":"), default=str).encode()
    kw = {"Bucket": BUCKET, "Key": key, "ContentType": "application/json", "CacheControl": "no-cache"}
    if gz:
        body = gzip.compress(body)
        kw["ContentEncoding"] = "gzip"
    s3.put_object(Body=body, **kw)


def _f(v):
    try:
        if v is None or v == "" or v == "null" or v == ".":
            return None
        return float(str(v).replace(",", ""))
    except Exception:
        return None


# ─────────────────────────── fetchers ────────────────────────────────
def tv_fetch(cache):
    """Sovereign yields + MOVE from TradingView; incremental when cached."""
    out = dict(cache.get("series") or {})
    errors = {}
    if not tv_pull:
        return out, {"tv": "module unavailable"}
    token, cookie = tv_pull._session()
    if not cookie:
        return out, {"tv": "no TradingView session in SSM"}

    def one(key):
        sym = TV_SYMBOLS[key][0]
        have = out.get(key) or {}
        countback = 40 if have.get("dates") else 800
        try:
            bars = tv_pull.pull(sym, token, cookie, countback=countback, budget=20)
        except Exception as e:
            return key, None, str(e)[:100]
        if not bars:
            return key, None, "no bars"
        merged = dict(zip(have.get("dates") or [], have.get("closes") or []))
        for b in bars:
            d = datetime.fromtimestamp(int(b[0]), tz=timezone.utc).date().isoformat()
            merged[d] = float(b[4])
        ds = sorted(merged)[-1200:]
        return key, {"dates": ds, "closes": [merged[d] for d in ds]}, None

    with ThreadPoolExecutor(max_workers=6) as ex:
        for key, ser, err in ex.map(one, list(TV_SYMBOLS)):
            if ser:
                out[key] = ser
            elif err:
                errors[key] = err
    return out, errors


def fred_fetch(series, obs=900):
    body = _get("%s/fred?series=%s&obs=%d" % (PROXY, series, obs), timeout=40)
    ds, cs = [], []
    for b in body.get("bars") or []:
        d = b.get("date") or (datetime.fromtimestamp(int(b["time"]), tz=timezone.utc).date().isoformat() if b.get("time") else None)
        v = _f(b.get("value"))
        if d and v is not None:
            ds.append(d[:10])
            cs.append(v)
    order = sorted(range(len(ds)), key=lambda i: ds[i])
    return {"dates": [ds[i] for i in order], "closes": [cs[i] for i in order]}


def yahoo_fetch(symbol, rng="2y"):
    body = _get("%s/yf-ohlc?symbol=%s&range=%s&interval=1d" % (PROXY, urllib.parse.quote(symbol), rng), timeout=40)
    ds, cs = [], []
    for b in body.get("bars") or []:
        t, c = b.get("time"), _f(b.get("close"))
        if t is None or c is None:
            continue
        ds.append(datetime.fromtimestamp(int(t), tz=timezone.utc).date().isoformat())
        cs.append(c)
    return {"dates": ds, "closes": cs}


def mof_jgb():
    """Japan MOF daily JGB curve (current month CSV, shift_jis)."""
    try:
        raw = _get("https://www.mof.go.jp/english/policy/jgbs/reference/interest_rate/jgbcme.csv", timeout=30, binary=True)
        text = raw.decode("shift_jis", "ignore")
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        hdr_i = next(i for i, ln in enumerate(lines) if ln.startswith("Date,"))
        cols = [c.strip() for c in lines[hdr_i].split(",")]
        rows = []
        for ln in lines[hdr_i + 1:]:
            parts = [p.strip() for p in ln.split(",")]
            if len(parts) < 3:
                continue
            try:
                d = datetime.strptime(parts[0], "%Y/%m/%d").date().isoformat()
            except Exception:
                continue
            rows.append({"date": d, **{cols[i]: _f(parts[i]) for i in range(1, min(len(cols), len(parts)))}})
        return {"tenors": cols[1:], "rows": rows}
    except Exception as e:
        return {"error": str(e)[:120], "rows": []}


# ─────────────────────────── metrics ─────────────────────────────────
def metrics(ser, kind, unit_dod, thresh_key, label, group, source):
    """kind: yield | spread | price | index. unit_dod: 'bp' (x100 of a % level), 'pct' (relative %), 'pts'."""
    ds, cs = ser.get("dates") or [], ser.get("closes") or []
    if len(cs) < 3:
        return None
    last, prev = cs[-1], cs[-2]
    def chg(i):
        if len(cs) <= i:
            return None
        base = cs[-1 - i]
        if unit_dod == "bp":
            return (last - base) * 100
        if unit_dod == "pct":
            return (last / base - 1) * 100 if base else None
        return last - base
    dod, d5, d20 = chg(1), chg(5), chg(20)
    dods = []
    for i in range(1, min(len(cs), 251)):
        a, b = cs[-i], cs[-i - 1]
        dods.append((a - b) * 100 if unit_dod == "bp" else ((a / b - 1) * 100 if (unit_dod == "pct" and b) else a - b))
    dods = [x for x in dods if x is not None]
    z = None
    if len(dods) >= 40 and dod is not None:
        sd = statistics.pstdev(dods)
        z = round((dod - statistics.fmean(dods)) / sd, 2) if sd > 1e-9 else 0.0
    window = cs[-250:]
    pct = round(100.0 * sum(1 for x in window if x <= last) / len(window)) if window else None
    red, amber = THRESH.get(thresh_key, (999, 999))
    a = abs(dod) if dod is not None else 0
    flag = "RED" if (a >= red or (z is not None and abs(z) >= 2.5)) else "AMBER" if (a >= amber or (z is not None and abs(z) >= 1.5)) else "GREEN"
    if thresh_key == "move" and last >= 120:
        flag = "RED"
    elif thresh_key == "move" and last >= 100 and flag == "GREEN":
        flag = "AMBER"
    return {"label": label, "group": group, "kind": kind, "unit": unit_dod, "source": source, "last": round(last, 4), "prev": round(prev, 4), "asof": ds[-1],
            "dod": round(dod, 2) if dod is not None else None, "dod_pct": round((last / prev - 1) * 100, 2) if prev else None,
            "d5": round(d5, 2) if d5 is not None else None, "d20": round(d20, 2) if d20 is not None else None, "z": z, "pct1y": pct,
            "flag": flag, "spark": [round(x, 4) for x in cs[-30:]], "spark_dates": ds[-30:]}


def spread_series(a, b):
    """Difference of two yield series aligned on dates (bp-level kept in %)."""
    if not a or not b:
        return None
    mb = dict(zip(b["dates"], b["closes"]))
    ds, cs = [], []
    for d, v in zip(a["dates"], a["closes"]):
        if d in mb:
            ds.append(d)
            cs.append(v - mb[d])
    return {"dates": ds, "closes": cs} if len(cs) > 3 else None


def word_dod(m):
    if not m or m.get("dod") is None:
        return "no change data"
    u = "bp" if m["unit"] == "bp" else ("%" if m["unit"] == "pct" else "pts")
    return "%+.1f%s%s" % (m["dod"], u, (" (z %+.1f)" % m["z"]) if m.get("z") is not None else "")


# ─────────────────────────── verdicts ────────────────────────────────
def equity_risk(m):
    tlt, us10, move, hy = m.get("TLT"), m.get("US10Y") or m.get("DGS10"), m.get("^MOVE") or m.get("MOVE_TV"), m.get("BAMLH0A0HYM2")
    tlt_d = tlt["dod"] if tlt else None
    y_d = us10["dod"] if us10 else None
    mv_d = move["dod"] if move else None
    hy_d = hy["dod"] if hy else None
    sell = (tlt_d is not None and tlt_d <= -1.5) or (y_d is not None and y_d >= 10) or (mv_d is not None and mv_d >= 10 and (tlt_d or 0) < 0)
    soft_sell = (tlt_d is not None and tlt_d <= -0.9) or (y_d is not None and y_d >= 6)
    buy = (tlt_d is not None and tlt_d >= 1.5) or (y_d is not None and y_d <= -10)
    soft_buy = (tlt_d is not None and tlt_d >= 0.9) or (y_d is not None and y_d <= -6)
    credit_widening = hy_d is not None and hy_d >= 8
    score = 50
    for v, w in ((tlt_d, -12), (y_d, 1.4), (mv_d, 0.9), (hy_d, 0.6)):
        if v is not None:
            score += v * w
    score = max(0, min(100, round(score)))
    parts = ["TLT %s" % word_dod(tlt) if tlt else None, "10Y %s" % word_dod(us10) if us10 else None, "MOVE %s%s" % (("%.0f " % move["last"]) if move else "", word_dod(move)) if move else None, "HY OAS %s" % word_dod(hy) if hy else None]
    parts = [p for p in parts if p]
    if sell:
        return {"state": "DUMP RISK", "level": "HIGH", "score": score, "tone": "bearish",
                "text": "Bonds sold off hard today (%s). Sharp Treasury sell-offs raise the discount rate for every risk asset and force de-risking -- a daily rate shock of this size is a HUGE DUMP RISK for stocks and crypto." % "; ".join(parts)}
    if buy and credit_widening:
        return {"state": "FLIGHT TO SAFETY", "level": "HIGH", "score": score, "tone": "bearish",
                "text": "Bonds were bought hard (%s) while credit spreads widened -- that is fear money hiding in Treasuries, not an easy-money bid. Risk-off, not a pump." % "; ".join(parts)}
    if buy:
        return {"state": "PUMP SETUP", "level": "HIGH", "score": score, "tone": "bullish",
                "text": "Big daily bond buying with credit calm (%s). Falling yields with spreads steady is the classic fuel for a risk-asset pump: lower discount rates, easier financial conditions." % "; ".join(parts)}
    if soft_sell:
        return {"state": "SELL-OFF", "level": "MODERATE", "score": score, "tone": "cautious", "text": "Bonds sold off (%s). Not a shock yet, but stocks and crypto trade with a rates headwind until it settles." % "; ".join(parts)}
    if soft_buy:
        return {"state": "BUYING", "level": "MODERATE", "score": score, "tone": "supportive", "text": "Bonds bid (%s). Mildly supportive for risk assets." % "; ".join(parts)}
    return {"state": "CALM", "level": "LOW", "score": score, "tone": "neutral", "text": "Bond prices and yields are inside their normal daily range (%s). No rates-driven pressure on stocks either way." % "; ".join(parts)}


def eurodollar_shortage(m, fleet):
    btp = m.get("BTP-Bund")
    ites = m.get("IT-ES")
    oat = m.get("OAT-Bund")
    dxy = m.get("DTWEXBGS")
    ehy = m.get("BAMLHE00EHYIOAS")
    emhy = m.get("BAMLEMHBHYCRPIOAS")
    ems = m.get("BAMLEMPBPUBSICRPIOAS")
    plumb = (fleet.get("eurodollar_plumbing") or {}).get("composite_score")
    funding = (fleet.get("usd_funding") or {}).get("stress_z")
    pts, notes = 0, []
    for lab, mm, red, amb in (("BTP-Bund", btp, 10, 6), ("OAT-Bund", oat, 6, 4), ("IT-ES", ites, 6, 4)):
        if mm and mm.get("dod") is not None:
            if mm["dod"] >= red or (mm.get("d5") or 0) >= 15:
                pts += 2
                notes.append("%s widening %s (5d %+.0fbp)" % (lab, word_dod(mm), mm.get("d5") or 0))
            elif mm["dod"] >= amb:
                pts += 1
                notes.append("%s %s" % (lab, word_dod(mm)))
    if dxy and dxy.get("dod_pct") is not None and dxy["dod_pct"] >= 0.4:
        pts += 1
        notes.append("dollar +%.2f%%" % dxy["dod_pct"])
    for lab, mm in (("Euro HY", ehy), ("EM HY", emhy), ("EM sovereign", ems)):
        if mm and mm.get("dod") is not None and mm["dod"] >= 8:
            pts += 1
            notes.append("%s OAS %s" % (lab, word_dod(mm)))
    if plumb is not None and plumb >= 40:
        pts += 2
        notes.append("eurodollar-plumbing composite %.0f" % plumb)
    elif plumb is not None and plumb >= 25:
        pts += 1
        notes.append("eurodollar-plumbing composite %.0f" % plumb)
    if isinstance(funding, (int, float)) and funding >= 1.5:
        pts += 1
        notes.append("USD funding stress z %.1f" % funding)
    score = min(100, pts * 12)
    state = "SHORTAGE SIGNAL" if pts >= 5 else "WATCH" if pts >= 3 else "NONE"
    text = ("Periphery spreads, the dollar and offshore credit are moving together -- the signature of a eurodollar (offshore dollar) squeeze: " + "; ".join(notes)) if pts >= 3 else \
           ("No eurodollar-shortage signature: periphery spreads, the dollar and EM/Euro credit are quiet" + ((" (" + "; ".join(notes) + ")") if notes else "") + ".")
    return {"state": state, "score": score, "points": pts, "text": text, "inputs": {"btp_bund": btp and btp["last"], "it_es": ites and ites["last"], "oat_bund": oat and oat["last"], "plumbing": plumb, "funding_z": funding}}


def heartbeat(m, eq, ed):
    groups = {}
    for k, mm in m.items():
        if not mm:
            continue
        g = groups.setdefault(mm["group"], {"n": 0, "amber": 0, "red": 0, "loud": []})
        g["n"] += 1
        if mm["flag"] == "RED":
            g["red"] += 1
            g["loud"].append((abs(mm.get("z") or 0) + 3, "%s %s" % (mm["label"], word_dod(mm))))
        elif mm["flag"] == "AMBER":
            g["amber"] += 1
            g["loud"].append((abs(mm.get("z") or 0), "%s %s" % (mm["label"], word_dod(mm))))
    weights = {"us": 3, "vol": 3, "credit": 2.5, "europe": 2, "japan": 1.5, "world": 1, "funding": 1.5}
    num = den = 0.0
    loud = []
    for g, v in groups.items():
        w = weights.get(g, 1)
        stress = (v["red"] * 1.0 + v["amber"] * 0.45) / max(v["n"], 1)
        num += w * stress
        den += w
        loud += v["loud"]
    score = round(100 * num / den) if den else 0
    score = max(score, eq["score"] - 50 if eq["state"] in ("DUMP RISK", "FLIGHT TO SAFETY") else 0, ed["score"] // 2)
    regime = "ACUTE" if score >= 70 else "ELEVATED" if score >= 45 else "WATCH" if score >= 22 else "CALM"
    loud.sort(reverse=True)
    top = [t for _, t in loud[:5]]
    head = {"ACUTE": "Bond markets are in shock", "ELEVATED": "Bond stress is building", "WATCH": "A few bond signals are moving", "CALM": "Bond markets are calm"}[regime]
    headline = head + (": " + "; ".join(top) if top else " -- every monitored market inside its normal daily range") + "."
    return {"score": score, "regime": regime, "headline": headline, "loudest": top, "groups": {g: {k: v[k] for k in ("n", "amber", "red")} for g, v in groups.items()}}


# ─────────────────────────── main ────────────────────────────────────
def lambda_handler(event, ctx):
    t0 = time.time()
    event = event or {}
    notes, freshness = [], {}
    m = {}

    # TradingView sovereign yields + MOVE
    cache = _s3_json(TV_KEY) or {}
    tv, tv_err = tv_fetch(cache)
    if tv:
        _put_json(TV_KEY, {"version": VERSION, "as_of": _iso(), "series": tv}, gz=True)
    if tv_err:
        notes.append("tv errors: %s" % json.dumps(tv_err)[:300])
    for key, (sym, label, group, kind) in TV_SYMBOLS.items():
        ser = tv.get(key)
        if not ser:
            continue
        if kind == "index":
            mm = metrics(ser, "index", "pts", "move", label, group, "TradingView " + sym)
        else:
            tk = "yield_jp" if group == "japan" else "yield_em" if key in ("BR10Y", "MX10Y", "IN10Y") else "yield_us" if group == "us" else "yield_dm"
            mm = metrics(ser, "yield", "bp", tk, label, group, "TradingView " + sym)
        if mm:
            m[key] = mm
    freshness["tradingview"] = max((v["asof"] for k, v in m.items() if v["source"].startswith("TradingView")), default=None)

    # FRED
    fred = {}
    for sid, (label, group, kind) in FRED_SERIES.items():
        try:
            fred[sid] = fred_fetch(sid)
            tk = ("oas_ccc" if sid == "BAMLH0A3HYC" else "oas_hy" if sid in ("BAMLH0A0HYM2", "BAMLH0A1HYBB", "BAMLH0A2HYB", "BAMLHE00EHYIOAS") else "oas_em" if sid.startswith("BAMLEM") else "oas_ig") if kind == "spread" else \
                 ("vix" if sid == "VIXCLS" else "index" if kind == "index" else "yield_us")
            unit = "bp" if kind in ("spread", "yield") else "pct" if sid == "DTWEXBGS" else "pts"
            mm = metrics(fred[sid], kind, unit, tk, label, group, "FRED " + sid)
            if mm:
                m[sid] = mm
        except Exception as e:
            notes.append("fred %s: %s" % (sid, str(e)[:60]))
    freshness["fred"] = max((v["asof"] for k, v in m.items() if v["source"].startswith("FRED")), default=None)

    # Yahoo
    for sym, (label, group, kind) in YAHOO.items():
        try:
            ser = yahoo_fetch(sym)
            if sym == "^TNX":
                mm = metrics(ser, "yield", "bp", "yield_us", label, group, "Yahoo " + sym)
            elif kind == "index":
                mm = metrics(ser, "index", "pts", "move", label, group, "Yahoo " + sym)
            else:
                mm = metrics(ser, "price", "pct", "price_bond", label, group, "Yahoo " + sym)
            if mm:
                m[sym] = mm
        except Exception as e:
            notes.append("yahoo %s: %s" % (sym, str(e)[:60]))
    freshness["yahoo"] = max((v["asof"] for k, v in m.items() if v["source"].startswith("Yahoo")), default=None)

    # spreads (bp-level in %)
    def y(k):
        return tv.get(k)
    pairs = {"BTP-Bund": ("IT10Y", "DE10Y", "Italy - Germany 10Y (BTP-Bund)"), "OAT-Bund": ("FR10Y", "DE10Y", "France - Germany 10Y (OAT-Bund)"),
             "Bono-Bund": ("ES10Y", "DE10Y", "Spain - Germany 10Y"), "IT-ES": ("IT10Y", "ES10Y", "Italy - Spain 10Y"), "GR-Bund": ("GR10Y", "DE10Y", "Greece - Germany 10Y"),
             "PT-Bund": ("PT10Y", "DE10Y", "Portugal - Germany 10Y"), "Gilt-Bund": ("GB10Y", "DE10Y", "UK - Germany 10Y"), "US-Bund": ("US10Y", "DE10Y", "US - Germany 10Y"),
             "US-JGB": ("US10Y", "JP10Y", "US - Japan 10Y"), "US2s10s": ("US10Y", "US02Y", "US 2s10s curve"), "US5s30s": ("US30Y", "US05Y", "US 5s30s curve"),
             "DE2s10s": ("DE10Y", "DE02Y", "Germany 2s10s"), "IT2s10s": ("IT10Y", "IT02Y", "Italy 2s10s"), "JP2s30s": ("JP30Y", "JP02Y", "Japan 2s30s")}
    for key, (a, b, label) in pairs.items():
        ser = spread_series(y(a), y(b))
        if ser:
            grp = "europe" if key in ("BTP-Bund", "OAT-Bund", "Bono-Bund", "IT-ES", "GR-Bund", "PT-Bund", "Gilt-Bund", "DE2s10s", "IT2s10s") else "japan" if key in ("US-JGB", "JP2s30s") else "us"
            mm = metrics(ser, "spread", "bp", "spread_periph" if grp == "europe" and key != "DE2s10s" else "yield_us", label, grp, "TradingView spread")
            if mm:
                m[key] = mm
    # SOFR - 3M bill (funding) from FRED
    if fred.get("SOFR") and fred.get("DTB3"):
        ser = spread_series(fred["SOFR"], fred["DTB3"])
        mm = metrics(ser, "spread", "bp", "oas_ig", "SOFR minus 3M bill", "funding", "FRED") if ser else None
        if mm:
            m["SOFR-TB3"] = mm

    # Japan MOF curve
    jgb = mof_jgb()
    if jgb.get("rows"):
        rows = jgb["rows"]
        last, prev = rows[-1], rows[-2] if len(rows) > 1 else None
        jgb["today"] = last["date"]
        jgb["curve"] = [{"tenor": t, "last": last.get(t), "prev": prev.get(t) if prev else None, "dod_bp": round((last.get(t) - prev.get(t)) * 100, 1) if (prev and last.get(t) is not None and prev.get(t) is not None) else None} for t in jgb["tenors"]]
        freshness["mof_jgb"] = last["date"]
    # fleet feeds
    fleet = {"auction_desk": _s3_json("data/auction-desk.json") or {}, "usd_funding": _s3_json("data/usd-funding.json") or {},
             "eurodollar_plumbing": _s3_json("data/eurodollar-plumbing.json") or {}, "move_index": _s3_json("data/move-index.json") or {},
             "crisis_plumbing": _s3_json("data/crisis-plumbing.json") or {}}
    ad = fleet["auction_desk"]
    auction = {"generated_at": ad.get("generated_at"), "today": (ad.get("today") or {}).get("date"), "verdict": (ad.get("today") or {}).get("verdict"),
               "auctions": [{k: a.get(k) for k in ("term", "type", "grade", "btc", "tail_bp", "indirect_pct", "pd_pct", "total_accepted", "verdict")} for a in ((ad.get("today") or {}).get("auctions") or [])],
               "buybacks": [{k: b.get(k) for k in ("operation_date", "accepted", "max_par", "fill_pct", "coverage", "tags", "verdict")} for b in ((ad.get("today") or {}).get("buybacks") or [])],
               "recent_days": [{k: d.get(k) for k in ("date", "headline", "tags", "risk_assets")} for d in (ad.get("recent_days") or [])[:6]],
               "prediction": [{k: p.get(k) for k in ("symbol", "name", "basis_label", "call", "confidence", "d1", "d5")} for p in ((ad.get("reactions") or {}).get("prediction") or []) if p.get("symbol") in ("SPY", "QQQ", "TLT", "HYG", "BTC-USD", "GLD")],
               "composite_now": ((ad.get("composite_history") or {}).get("series") or [{}])[-1] if ad.get("composite_history") else None}
    ff = {"usd_funding_stress_z": fleet["usd_funding"].get("stress_z"), "usd_funding_generated": fleet["usd_funding"].get("generated"),
          "eurodollar_plumbing": {k: fleet["eurodollar_plumbing"].get(k) for k in ("composite_score", "verdict", "severity", "generated_at")},
          "move_index": {k: fleet["move_index"].get(k) for k in ("level", "change_1d", "regime", "percentile", "generated_at")},
          "crisis_plumbing": (fleet["crisis_plumbing"].get("composite") or {}) | {"generated_at": fleet["crisis_plumbing"].get("generated_at")} if isinstance(fleet["crisis_plumbing"].get("composite"), dict) else None}

    eq = equity_risk(m)
    ed = eurodollar_shortage(m, fleet)
    hb = heartbeat(m, eq, ed)

    # panels in display order
    def rows(keys):
        return [dict(m[k], key=k) for k in keys if k in m]
    panels = {
        "us_rates": rows(["US02Y", "US05Y", "US10Y", "US30Y", "DGS3MO", "DFII10", "T10YIE", "US2s10s", "US5s30s"]),
        "volatility": rows(["^MOVE", "MOVE_TV", "VIXCLS", "TLT", "IEF", "SHY", "HYG", "LQD", "EMB"]),
        "japan": rows(["JP02Y", "JP10Y", "JP30Y", "JP2s30s", "US-JGB"]),
        "europe": rows(["DE02Y", "DE10Y", "DE30Y", "FR10Y", "IT02Y", "IT10Y", "ES10Y", "NL10Y", "PT10Y", "GR10Y", "GB02Y", "GB10Y", "CH10Y"]),
        "europe_spreads": rows(["BTP-Bund", "OAT-Bund", "Bono-Bund", "IT-ES", "PT-Bund", "GR-Bund", "Gilt-Bund", "US-Bund", "DE2s10s", "IT2s10s"]),
        "world": rows(["AU10Y", "CA10Y", "CN10Y", "KR10Y", "IN10Y", "BR10Y", "MX10Y"]),
        "credit": rows(["BAMLH0A0HYM2", "BAMLC0A0CM", "BAMLC0A1CAAA", "BAMLC0A4CBBB", "BAMLH0A1HYBB", "BAMLH0A2HYB", "BAMLH0A3HYC", "BAMLHE00EHYIOAS", "BAMLEMCBPIOAS", "BAMLEMIBHGCRPIOAS", "BAMLEMHBHYCRPIOAS", "BAMLEMPBPUBSICRPIOAS"]),
        "funding": rows(["SOFR", "DTB3", "SOFR-TB3", "DTWEXBGS"]),
    }
    flags = {"RED": [k for k, v in m.items() if v["flag"] == "RED"], "AMBER": [k for k, v in m.items() if v["flag"] == "AMBER"]}
    out = {"version": VERSION, "generated_at": _iso(), "elapsed_s": round(time.time() - t0, 1), "notes": notes, "freshness": freshness,
           "heartbeat": hb, "equity_risk": eq, "eurodollar_shortage": ed, "flags": flags, "panels": panels, "jgb_curve": {k: jgb.get(k) for k in ("today", "tenors", "curve", "error")},
           "auction": auction, "fleet": ff, "n_series": len(m),
           "methodology": {"dod": "day-over-day change: bp for yields and spreads, % for bond ETFs and the dollar, points for MOVE/VIX; z = today's change vs the trailing 250 daily changes",
                           "flags": "RED = shock threshold hit (US/DM 10Y 10bp, JGB 7bp, periphery spreads 10bp, HY OAS 20bp, IG OAS 7bp, CCC 35bp, EM 15bp, bond ETFs 1.5%, MOVE +10 or level >= 120) or |z| >= 2.5; AMBER = 60% of those or |z| >= 1.5",
                           "equity_risk": "DUMP RISK: TLT <= -1.5% or US10Y >= +10bp or MOVE +10 with bonds down; PUMP SETUP: TLT >= +1.5% or US10Y <= -10bp with HY OAS not widening >= 8bp; FLIGHT TO SAFETY: the same buying with HY OAS widening",
                           "eurodollar": "points from BTP-Bund / OAT-Bund / IT-ES widening, dollar up >= 0.4%, Euro-HY / EM spreads widening >= 8bp, the fleet's eurodollar-plumbing composite and USD-funding stress z; SHORTAGE SIGNAL at 5+ points",
                           "heartbeat": "weighted share of RED/AMBER across US rates, volatility, credit, Europe, Japan, rest of world and funding; ACUTE >= 70, ELEVATED >= 45, WATCH >= 22"}}
    _put_json(OUT_KEY, out)
    return {"ok": True, "elapsed_s": out["elapsed_s"], "n_series": len(m), "heartbeat": hb["score"], "regime": hb["regime"], "equity": eq["state"], "eurodollar": ed["state"],
            "red": flags["RED"][:8], "notes": notes[:4], "freshness": freshness}
