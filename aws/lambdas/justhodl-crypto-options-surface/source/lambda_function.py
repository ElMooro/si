"""justhodl-crypto-options-surface · v1.0 — the directional half of the crypto vol complex.

DVOL (justhodl-crypto-dvol) gives the LEVEL of implied vol (the crypto-VIX). This engine gives
the SHAPE of the surface, which is where the directional information lives:

  - 25-delta RISK REVERSAL  (25d call IV − 25d put IV): the cleanest options positioning tell.
        RR > 0  = upside calls bid up  → bullish skew / chasing upside.
        RR < 0  = downside puts bid up → fear / hedging demand.
  - 25-delta BUTTERFLY      (wing IV − ATM IV): convexity / how richly the tails are priced.
  - VOL TERM STRUCTURE      (ATM IV across ~7/30/90d):
        backwardation (front > back) = near-term stress priced in;
        contango      (back > front) = calm / normal.

SOURCE: Deribit (free) — get_instruments (strike/type/expiry) + get_book_summary (mark_iv,
underlying_price). The 25-delta strikes are located by computing Black-Scholes delta from the
quoted mark_iv (Deribit options are European index options, r≈0), then linearly interpolating IV
in delta-space — one bulk call per currency, no per-instrument ticker storm. BTC + ETH.

HONESTY: no free historical option surface exists, so unlike DVOL this can NOT be event-studied
in-sample on day one. Instead it (a) self-accumulates a daily snapshot to
data/crypto-options-surface-history.json, and (b) the RR sign is registered in the central FDR
ledger (crypto_options_rr via signal-logger) for honest, live, out-of-sample grading.
"""
import json
import math
import time
import urllib.request
from datetime import datetime, timezone

import boto3

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/crypto-options-surface.json"
HIST_KEY = "data/crypto-options-surface-history.json"
DERIBIT = "https://www.deribit.com/api/v2/public/"


def _get(url, timeout=30):
    req = urllib.request.Request(url, headers={"User-Agent": "justhodl/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def _ncdf(x):
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def bs_delta(S, K, T, sigma, is_call, r=0.0):
    """Black-Scholes delta. sigma in decimal (mark_iv/100), T in years."""
    if not (S > 0 and K > 0 and T > 0 and sigma > 0):
        return None
    d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
    nd1 = _ncdf(d1)
    return nd1 if is_call else nd1 - 1.0


def _interp(points, target):
    """points: list of (x, y); linear-interpolate y at x=target, clamped to the endpoints."""
    pts = sorted((p for p in points if p[0] is not None and p[1] is not None), key=lambda p: p[0])
    if not pts:
        return None
    if target <= pts[0][0]:
        return pts[0][1]
    if target >= pts[-1][0]:
        return pts[-1][1]
    for i in range(1, len(pts)):
        if pts[i][0] >= target:
            x0, y0 = pts[i - 1]
            x1, y1 = pts[i]
            if x1 == x0:
                return y1
            w = (target - x0) / (x1 - x0)
            return y0 + w * (y1 - y0)
    return pts[-1][1]


def surface(ccy):
    now = int(time.time() * 1000)
    inst = _get(DERIBIT + "get_instruments?currency=%s&kind=option&expired=false" % ccy)["result"]
    imap = {i["instrument_name"]: i for i in inst}
    book = _get(DERIBIT + "get_book_summary_by_currency?currency=%s&kind=option" % ccy)["result"]

    by_exp = {}
    S_global = None
    for b in book:
        nm = b.get("instrument_name")
        iv = b.get("mark_iv")
        S = b.get("underlying_price")
        meta = imap.get(nm)
        if not meta or iv is None or not S:
            continue
        exp = meta["expiration_timestamp"]
        K = meta["strike"]
        is_call = meta["option_type"] == "call"
        T = (exp - now) / (365.0 * 86400000.0)
        if T <= 0:
            continue
        d = bs_delta(S, K, T, iv / 100.0, is_call)
        if d is None:
            continue
        slot = by_exp.setdefault(exp, {"S": S, "calls": [], "puts": [], "atm": []})
        if is_call:
            slot["calls"].append((d, iv))
        else:
            slot["puts"].append((d, iv))
        slot["atm"].append((abs(K - S), iv))
        S_global = S

    rows = []
    for exp, dat in sorted(by_exp.items()):
        days = round((exp - now) / 86400000.0, 1)
        if days <= 0:
            continue
        atm = min(dat["atm"], key=lambda t: t[0])[1] if dat["atm"] else None
        iv25c = _interp(dat["calls"], 0.25) if dat["calls"] else None
        iv25p = _interp(dat["puts"], -0.25) if dat["puts"] else None
        rr25 = round(iv25c - iv25p, 2) if (iv25c is not None and iv25p is not None) else None
        bf25 = (round((iv25c + iv25p) / 2.0 - atm, 2)
                if (iv25c is not None and iv25p is not None and atm is not None) else None)
        rows.append({
            "days": days,
            "expiry": datetime.fromtimestamp(exp / 1000, tz=timezone.utc).date().isoformat(),
            "atm_iv": round(atm, 2) if atm is not None else None,
            "iv_25d_call": round(iv25c, 2) if iv25c is not None else None,
            "iv_25d_put": round(iv25p, 2) if iv25p is not None else None,
            "rr_25d": rr25,
            "bf_25d": bf25,
        })

    def nearest(tdays):
        cand = [r for r in rows if r["atm_iv"] is not None]
        return min(cand, key=lambda r: abs(r["days"] - tdays)) if cand else None

    n7, n30, n90 = nearest(7), nearest(30), nearest(90)
    term = None
    if n7 and n30 and n90:
        slope = round(n90["atm_iv"] - n7["atm_iv"], 2)
        regime = ("BACKWARDATION (near-term stress)" if slope < -1
                  else "CONTANGO (calm)" if slope > 1 else "FLAT")
        term = {"atm_7d": n7["atm_iv"], "atm_30d": n30["atm_iv"], "atm_90d": n90["atm_iv"],
                "slope_90_7": slope, "regime": regime}

    head = n30 or (rows[len(rows) // 2] if rows else None)
    read = None
    if head and head.get("rr_25d") is not None:
        rr = head["rr_25d"]
        read = ("CALL SKEW — upside calls bid, bullish positioning" if rr > 1
                else "PUT SKEW — downside puts bid, hedging/fear" if rr < -1
                else "BALANCED skew")
    return {"underlying": round(S_global, 1) if S_global else None,
            "n_strikes": sum(len(d["calls"]) + len(d["puts"]) for d in by_exp.values()),
            "expiries": rows, "headline_30d": head, "term_structure": term, "interpretation": read}


def _load(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def lambda_handler(event, context):
    t0 = time.time()
    out = {"generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), "version": "1.0"}
    diag = []
    for ccy in ("BTC", "ETH"):
        try:
            out[ccy.lower()] = surface(ccy)
        except Exception as e:
            out[ccy.lower()] = {"_err": str(e)[:120]}
            diag.append("%s:%s" % (ccy, str(e)[:60]))

    btc = out.get("btc") or {}
    h = btc.get("headline_30d") or {}
    out["regime"] = btc.get("interpretation")
    out["rr_25d_30d"] = h.get("rr_25d")
    out["term_regime"] = (btc.get("term_structure") or {}).get("regime")

    try:
        hist = _load(HIST_KEY) or {"series": []}
        ser = hist.get("series", [])
        today = datetime.now(timezone.utc).date().isoformat()
        snap = {"date": today, "btc_rr25_30d": h.get("rr_25d"), "btc_atm_30d": h.get("atm_iv"),
                "btc_bf25_30d": h.get("bf_25d"),
                "term_slope": (btc.get("term_structure") or {}).get("slope_90_7"),
                "eth_rr25_30d": ((out.get("eth") or {}).get("headline_30d") or {}).get("rr_25d")}
        ser = [x for x in ser if x.get("date") != today] + [snap]
        ser = ser[-365:]
        hist["series"] = ser
        hist["updated_at"] = out["generated_at"]
        s3.put_object(Bucket=BUCKET, Key=HIST_KEY, Body=json.dumps(hist, default=str).encode(),
                      ContentType="application/json")
        out["history_n"] = len(ser)
    except Exception as e:
        diag.append("hist:%s" % str(e)[:60])

    out["event_study_note"] = ("No free historical option surface; RR sign graded live via the "
                               "central FDR ledger (crypto_options_rr) + self-history accumulating.")
    out["duration_s"] = round(time.time() - t0, 1)
    if diag:
        out["_diag"] = diag
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=900")
    return {"statusCode": 200, "body": json.dumps({"btc_rr_25d": out.get("rr_25d_30d"),
                                                    "term": out.get("term_regime"),
                                                    "history_n": out.get("history_n")})}
