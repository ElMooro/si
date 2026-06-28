"""
justhodl-crypto-options  ·  v1.0 — Crypto options SURFACE (skew / risk-reversal / term structure).

DVOL (crypto-dvol) gives the LEVEL of option-implied fear — crypto's VIX. This engine adds the
rest of the surface, the part that carries DIRECTION and TIMING:

  · 25-delta RISK REVERSAL  (call_iv - put_iv) per tenor — the cleanest directional positioning
    tell in crypto. Negative = downside puts bid up (hedging / bearish / fear); positive = upside
    calls bid up (bullish / chase).
  · 25-delta BUTTERFLY (smile convexity) — how richly the tails are priced vs ATM.
  · ATM vol TERM STRUCTURE (≈7d / 30d / 90d) — backwardation = near-term stress priced,
    contango = calm/normal.
  · Put/Call OPEN-INTEREST ratio per tenor.

Deribit publishes no historical option-chain, so there is no honest in-sample backtest from a
single fetch. Instead this engine (a) SELF-ACCUMULATES 25d-RR history to data/crypto-options-
history.json so a forward event study can mature, and (b) is registered in the central signal
ledger (crypto_options_rr) for live FDR grading — the same measure-before-trust path DVOL took.

Writes data/crypto-options.json. Source: Deribit public API (get_book_summary_by_currency).
Greeks computed locally (Black-Scholes delta, r=q=0) from each instrument's mark_iv.
"""
import json
import math
import time
import urllib.request
from datetime import datetime, timezone

import boto3

VERSION = "1.0"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/crypto-options.json"
HIST_KEY = "data/crypto-options-history.json"
s3 = boto3.client("s3", region_name="us-east-1")


def _get(url, tmo=20):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    return json.loads(urllib.request.urlopen(req, timeout=tmo).read())


def _load(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def _ncdf(x):
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _bs_delta(S, K, T, sigma, is_call):
    """Black-Scholes delta with r=q=0 (Deribit options are inverse/coin-margined but delta sign
    and ~magnitude are fine for locating the 25-delta wings)."""
    if S <= 0 or K <= 0 or T <= 0 or sigma <= 0:
        return None
    d1 = (math.log(S / K) + 0.5 * sigma * sigma * T) / (sigma * math.sqrt(T))
    return _ncdf(d1) if is_call else _ncdf(d1) - 1.0


def _parse_inst(name):
    # e.g. BTC-27JUN25-60000-C
    p = name.split("-")
    if len(p) != 4:
        return None
    try:
        exp = datetime.strptime(p[1], "%d%b%y").replace(hour=8, tzinfo=timezone.utc)
        return {"expiry": exp, "strike": float(p[2]), "is_call": p[3] == "C"}
    except Exception:
        return None


def analyze(ccy):
    rows = _get("https://www.deribit.com/api/v2/public/get_book_summary_by_currency"
                "?currency=%s&kind=option" % ccy)["result"]
    now = datetime.now(timezone.utc)
    opts, underlying = [], None
    for r in rows:
        m = _parse_inst(r.get("instrument_name", ""))
        if not m:
            continue
        iv = r.get("mark_iv")
        if not iv or iv <= 0:
            continue
        up = r.get("underlying_price") or r.get("estimated_delivery_price")
        if up:
            underlying = up
        tte = (m["expiry"] - now).total_seconds() / (365.25 * 86400)
        if tte <= 0:
            continue
        m.update({"iv": iv, "tte": tte, "oi": r.get("open_interest") or 0})
        opts.append(m)
    if not opts or not underlying:
        return {"_error": "no liquid options / underlying"}

    expiries = sorted(set(o["expiry"] for o in opts))
    by_exp = {e: [o for o in opts if o["expiry"] == e] for e in expiries}

    def dte_of(e):
        return max(0.0, (e - now).total_seconds() / 86400)

    def nearest(target):
        cand = [e for e in expiries if dte_of(e) >= 1]
        return min(cand, key=lambda e: abs(dte_of(e) - target)) if cand else None

    def find_delta(lst, target, is_call, S):
        best, bd = None, 1e9
        for o in lst:
            dl = _bs_delta(S, o["strike"], o["tte"], o["iv"] / 100.0, is_call)
            if dl is None:
                continue
            if abs(dl - target) < bd:
                bd, best = abs(dl - target), o
        return best

    S = underlying
    surface, atm_by = {}, {}
    for label, t in (("7d", 7), ("30d", 30), ("90d", 90)):
        e = nearest(t)
        if not e:
            continue
        chain = by_exp[e]
        atm_strike = min(set(o["strike"] for o in chain), key=lambda k: abs(k - S))
        atm_ivs = [o["iv"] for o in chain if o["strike"] == atm_strike]
        atm_iv = round(sum(atm_ivs) / len(atm_ivs), 1) if atm_ivs else None
        calls = [o for o in chain if o["is_call"]]
        puts = [o for o in chain if not o["is_call"]]
        c25 = find_delta(calls, 0.25, True, S)
        p25 = find_delta(puts, -0.25, False, S)
        rr = round(c25["iv"] - p25["iv"], 1) if c25 and p25 else None       # call_iv - put_iv
        bf = round((c25["iv"] + p25["iv"]) / 2 - atm_iv, 1) if (c25 and p25 and atm_iv) else None
        coi, poi = sum(o["oi"] for o in calls), sum(o["oi"] for o in puts)
        pc_oi = round(poi / coi, 2) if coi > 0 else None
        surface[label] = {"dte": int(dte_of(e)), "expiry": e.date().isoformat(), "atm_iv": atm_iv,
                          "rr_25d": rr, "butterfly_25d": bf, "put_call_oi": pc_oi,
                          "c25_iv": round(c25["iv"], 1) if c25 else None,
                          "p25_iv": round(p25["iv"], 1) if p25 else None}
        if atm_iv:
            atm_by[label] = atm_iv

    slope, ts_regime = None, None
    if "7d" in atm_by and "90d" in atm_by:
        slope = round(atm_by["90d"] - atm_by["7d"], 1)
    elif "30d" in atm_by and "90d" in atm_by:
        slope = round(atm_by["90d"] - atm_by["30d"], 1)
    if slope is not None:
        ts_regime = "BACKWARDATION" if slope < -1 else "CONTANGO" if slope > 1 else "FLAT"

    rr30 = (surface.get("30d") or {}).get("rr_25d")
    positioning = "NEUTRAL"
    if rr30 is not None:
        positioning = "BULLISH" if rr30 >= 1.5 else "BEARISH" if rr30 <= -1.5 else "NEUTRAL"

    parts = []
    if rr30 is not None:
        if positioning == "BEARISH":
            parts.append("downside puts bid up (25d RR %.1f) — hedging / bearish positioning" % rr30)
        elif positioning == "BULLISH":
            parts.append("upside calls bid up (25d RR +%.1f) — bullish / chase positioning" % rr30)
        else:
            parts.append("balanced call/put skew (25d RR %.1f)" % rr30)
    if ts_regime == "BACKWARDATION":
        parts.append("vol term structure in backwardation — near-term stress priced (front > back)")
    elif ts_regime == "CONTANGO":
        parts.append("vol term structure in contango — calm / normal (front < back)")
    elif ts_regime == "FLAT":
        parts.append("flat vol term structure")

    return {"underlying": round(underlying, 1), "n_options": len(opts), "n_expiries": len(expiries),
            "surface": surface, "term_slope_iv": slope, "term_regime": ts_regime,
            "positioning": positioning, "rr_30d": rr30,
            "put_call_oi_30d": (surface.get("30d") or {}).get("put_call_oi"),
            "interpretation": "; ".join(parts) if parts else None}


def lambda_handler(event, context):
    t0 = time.time()
    out = {"version": VERSION,
           "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
           "sources": ["Deribit get_book_summary_by_currency (option chain); BS delta computed locally"]}
    for ccy in ("BTC", "ETH"):
        try:
            out[ccy.lower()] = analyze(ccy)
        except Exception as e:
            out[ccy.lower()] = {"_error": str(e)[:120]}

    btc = out.get("btc") or {}
    eth = out.get("eth") or {}
    out["crypto_options_positioning"] = btc.get("positioning")
    out["crypto_options_regime"] = btc.get("term_regime")
    out["interpretation"] = btc.get("interpretation")

    # self-accumulate 25d-RR history for a future forward event study
    try:
        hist = _load(HIST_KEY) or []
        today = datetime.now(timezone.utc).date().isoformat()
        hist = [h for h in hist if h.get("date") != today]
        hist.append({"date": today, "btc_rr_30d": btc.get("rr_30d"),
                     "btc_atm_30d": (btc.get("surface") or {}).get("30d", {}).get("atm_iv"),
                     "btc_term_slope": btc.get("term_slope_iv"),
                     "btc_pc_oi": btc.get("put_call_oi_30d"), "eth_rr_30d": eth.get("rr_30d")})
        hist = hist[-400:]
        s3.put_object(Bucket=BUCKET, Key=HIST_KEY, Body=json.dumps(hist),
                      ContentType="application/json")
        out["history_n"] = len(hist)
    except Exception as e:
        out["_hist_err"] = str(e)[:80]

    out["duration_s"] = round(time.time() - t0, 1)
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str),
                  ContentType="application/json", CacheControl="public, max-age=900")
    return {"statusCode": 200, "body": json.dumps({"ok": True, "btc_rr_30d": btc.get("rr_30d"),
            "positioning": out.get("crypto_options_positioning"),
            "term_regime": out.get("crypto_options_regime")})}
