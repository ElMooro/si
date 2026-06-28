"""justhodl-crypto-gex · v1.0 — dealer gamma exposure + max-pain by strike.

The vol complex already has DVOL, skew (25d RR/BF), term structure and VRP — but nothing on
positioning BY STRIKE. This engine reads Deribit option open interest per strike and derives the
dealer-gamma picture that drives intraday pinning vs trending:

  - net GEX (gamma exposure): convention = dealers long call gamma, short put gamma. Positive =
    dealers stabilise (sell rallies / buy dips) -> vol SUPPRESSED, price pins. Negative = dealers
    amplify moves -> vol EXPANSION, trending/violent.
  - gamma-flip level: the spot at which net GEX crosses zero (above = positive/pinned regime,
    below = negative/unstable regime).
  - gamma walls: strike with the most call gamma*OI (resistance) and put gamma*OI (support).
  - max-pain: the nearest-monthly-expiry price that minimises total option-holder payout.

SOURCE: Deribit get_instruments + get_book_summary_by_currency (free, same as options-surface).
BS gamma computed from mark_iv. Feeds crypto-intel / cycle-clock / crypto-confluence / page.
"""
import json
import math
import time
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone

import boto3

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/crypto-gex.json"
DERIBIT = "https://www.deribit.com/api/v2/public/"


def _get(url, timeout=30):
    req = urllib.request.Request(url, headers={"User-Agent": "justhodl/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def bs_gamma(S, K, T, sigma):
    if not (S > 0 and K > 0 and T > 0 and sigma > 0):
        return 0.0
    d1 = (math.log(S / K) + 0.5 * sigma * sigma * T) / (sigma * math.sqrt(T))
    npd1 = math.exp(-0.5 * d1 * d1) / math.sqrt(2 * math.pi)
    return npd1 / (S * sigma * math.sqrt(T))


def build(ccy):
    now = int(time.time() * 1000)
    inst = _get(DERIBIT + "get_instruments?currency=%s&kind=option&expired=false" % ccy)["result"]
    imap = {i["instrument_name"]: i for i in inst}
    book = _get(DERIBIT + "get_book_summary_by_currency?currency=%s&kind=option" % ccy)["result"]

    opts = []           # (K, T, iv, oi, is_call)
    spot_samples = []
    by_strike = defaultdict(lambda: {"call_oi": 0.0, "put_oi": 0.0, "call_g": 0.0, "put_g": 0.0})
    by_exp_oi = defaultdict(lambda: defaultdict(lambda: {"c": 0.0, "p": 0.0}))  # exp -> strike -> oi
    for b in book:
        nm = b.get("instrument_name")
        iv = b.get("mark_iv")
        S = b.get("underlying_price")
        oi = b.get("open_interest") or 0
        meta = imap.get(nm)
        if not meta or iv is None or not S or oi <= 0:
            continue
        exp = meta["expiration_timestamp"]
        K = meta["strike"]
        is_call = meta["option_type"] == "call"
        T = (exp - now) / (365.0 * 86400000.0)
        if T <= 0 or T > 1.0:      # within 1y
            continue
        spot_samples.append(S)
        sigma = iv / 100.0
        opts.append((K, T, sigma, float(oi), is_call))
        g = bs_gamma(S, K, T, sigma)
        rec = by_strike[K]
        if is_call:
            rec["call_oi"] += oi; rec["call_g"] += g * oi
        else:
            rec["put_oi"] += oi; rec["put_g"] += g * oi
        e = by_exp_oi[exp][K]
        e["c" if is_call else "p"] += oi

    if not opts:
        return {"_err": "no option data"}
    spot = sorted(spot_samples)[len(spot_samples) // 2]   # median underlying

    def net_gex_at(Sg):
        tot = 0.0
        for K, T, sigma, oi, is_call in opts:
            gex = bs_gamma(Sg, K, T, sigma) * oi * Sg * Sg * 0.01
            tot += gex if is_call else -gex
        return tot

    total_net_gex = net_gex_at(spot)

    # gamma-flip via zero-crossing on a +-25% spot grid
    grid = [spot * (0.75 + 0.5 * i / 50) for i in range(51)]
    vals = [(g, net_gex_at(g)) for g in grid]
    flip = None
    for i in range(1, len(vals)):
        y0, y1 = vals[i - 1][1], vals[i][1]
        if y0 == 0 or (y0 < 0) != (y1 < 0):
            x0, x1 = vals[i - 1][0], vals[i][0]
            flip = round(x0 - y0 * (x1 - x0) / (y1 - y0)) if y1 != y0 else round(x0)
            break

    # gamma walls (largest gamma*OI by strike)
    call_wall = max(by_strike.items(), key=lambda kv: kv[1]["call_g"], default=(None, {}))
    put_wall = max(by_strike.items(), key=lambda kv: kv[1]["put_g"], default=(None, {}))

    # max-pain for the relevant NEAR-TERM expiry (nearest within 10d with OI; else soonest)
    exp_oi_tot = {e: sum(s["c"] + s["p"] for s in strikes.values()) for e, strikes in by_exp_oi.items()}
    soon = sorted(exp_oi_tot)
    near_window = [e for e in soon if (e - now) <= 10 * 86400000 and exp_oi_tot[e] > 0]
    near_exp = (max(near_window, key=exp_oi_tot.get) if near_window else (soon[0] if soon else None))
    max_pain = None
    if near_exp:
        strikes = by_exp_oi[near_exp]
        ks = sorted(strikes)
        best = None
        for Sp in ks:
            pain = 0.0
            for K in ks:
                if K < Sp:
                    pain += strikes[K]["c"] * (Sp - K)   # ITM calls
                elif K > Sp:
                    pain += strikes[K]["p"] * (K - Sp)   # ITM puts
            if best is None or pain < best[1]:
                best = (Sp, pain)
        max_pain = best[0] if best else None

    tot_call_oi = sum(v["call_oi"] for v in by_strike.values())
    tot_put_oi = sum(v["put_oi"] for v in by_strike.values())
    regime = ("POSITIVE GAMMA (pinned / vol-suppressed)" if total_net_gex > 0
              else "NEGATIVE GAMMA (unstable / vol-expansion)")

    return {
        "spot": round(spot), "net_gex_usd": round(total_net_gex),
        "gamma_flip": flip,
        "spot_vs_flip": (round((spot / flip - 1) * 100, 1) if flip else None),
        "regime": regime,
        "call_wall": call_wall[0], "put_wall": put_wall[0],
        "max_pain": max_pain,
        "max_pain_exp": (datetime.fromtimestamp(near_exp / 1000, timezone.utc).strftime("%d%b%y") if near_exp else None),
        "total_call_oi": round(tot_call_oi), "total_put_oi": round(tot_put_oi),
        "put_call_oi_ratio": round(tot_put_oi / tot_call_oi, 2) if tot_call_oi else None,
        "n_strikes": len(by_strike),
    }


def lambda_handler(event, context):
    t0 = time.time()
    out = {"generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), "version": "1.0"}
    for ccy in ("BTC", "ETH"):
        try:
            out[ccy.lower()] = build(ccy)
        except Exception as e:
            out[ccy.lower()] = {"_err": str(e)[:120]}

    b = out.get("btc") or {}
    if b.get("regime"):
        out["interpretation"] = (
            "BTC dealer gamma %s; net GEX $%.0fM, flip ~$%s (spot %s%% %s). Call wall $%s (resistance), "
            "put wall $%s (support). Max-pain $%s (%s)." % (
                "POSITIVE" if (b.get("net_gex_usd") or 0) > 0 else "NEGATIVE",
                (b.get("net_gex_usd") or 0) / 1e6, b.get("gamma_flip"),
                b.get("spot_vs_flip"), "above" if (b.get("spot_vs_flip") or 0) >= 0 else "below",
                b.get("call_wall"), b.get("put_wall"), b.get("max_pain"), b.get("max_pain_exp")))
    out["duration_s"] = round(time.time() - t0, 1)
    out["sources"] = ["Deribit get_instruments + get_book_summary_by_currency (BS gamma from mark_iv)"]
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=900")
    return {"statusCode": 200, "body": json.dumps({"btc_regime": b.get("regime"),
                                                    "net_gex_usd": b.get("net_gex_usd"),
                                                    "gamma_flip": b.get("gamma_flip"),
                                                    "max_pain": b.get("max_pain")})}
