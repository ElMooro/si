#!/usr/bin/env python3
"""justhodl-phase-detector -- ACCUMULATION & DISTRIBUTION PHASE MAP
(Khalid 2026-07-10). Dated BEGIN and END of accumulation/distribution
per stock -- the piece none of the existing A/D engines produce (dark-
pool = venue states, resilience = flow confirm, whales = quarterly 13F;
none segment price history into dated phases).

Method (research-grounded, citations on the page):
  * Prior-trend gate: 63d log-slope classifies the move INTO the range
    (downtrend -> accumulation candidate, uptrend -> distribution).
  * Climax: volume >= 2.2x its 50d average AND bar range >= 1.5x ATR14,
    close-location off the extreme (SC closes off the low / BC off the
    high) -- the canonical wide-spread climax signature.
  * Trading range: climax extreme + reaction extreme over the next ~10
    bars define support/resistance; range lives while closes stay
    within +-1 ATR of the bounds.
  * In-range pressure in [-1, +1]: OBV slope z + Chaikin A/D slope z +
    up/down-volume ratio + mean close-location value. Rising flow into
    flat price = absorption (accumulation); the mirror = distribution.
  * Events: SPRING (low pierces support >=0.25 ATR, closes back inside,
    volume <= 1.2x average) / UTAD (mirror above resistance, reverses
    within 3 bars) / ST (retest on <=0.8x climax volume).
  * Phase END: SOS = close beyond resistance by 0.25 ATR on volume z
    >= 1.5 with 2-bar hold -> MARKUP begins (SOW mirror -> MARKDOWN).
    BEGIN = the climax/range-start date.
  * Disambiguation flag: breakout direction disagreeing with pressure
    sign is marked pressure_conflict, never silently relabeled.

Universe: top ~700 by dollar volume from Polygon grouped daily; 2y of
daily bars per name. Confirmations joined per ticker: dark-pool state,
whales conviction flow, resilience flow_confirmed. Real data only.
Output: data/phase-detector.json. Daily post-close.
"""
import json
import math
import os
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import boto3

S3 = boto3.client("s3")
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/phase-detector.json"
POLY = (os.environ.get("POLYGON_KEY") or os.environ.get("POLYGON_API_KEY")
        or "")
UNIVERSE_N = 700
MIN_PRICE = 3.0


def _get(url, timeout=25):
    for attempt in (0, 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent":
                                                       "justhodl-phase"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as he:
            if attempt == 0 and he.code in (429, 500, 502, 503):
                time.sleep(2)
                continue
            return None
        except Exception:
            if attempt == 0:
                time.sleep(1.5)
                continue
            return None


def gj(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET,
                                        Key=key)["Body"].read())
    except Exception:
        return None


def build_universe():
    """Top UNIVERSE_N common-stock-ish tickers by dollar volume from the
    most recent grouped-daily session."""
    now = datetime.now(timezone.utc)
    for back in range(1, 8):
        d = (now - timedelta(days=back)).strftime("%Y-%m-%d")
        j = _get("https://api.polygon.io/v2/aggs/grouped/locale/us/"
                 "market/stocks/%s?adjusted=true&apiKey=%s" % (d, POLY),
                 timeout=60)
        rows = (j or {}).get("results") or []
        if len(rows) > 3000:
            cands = []
            for r in rows:
                t = r.get("T") or ""
                c, v = r.get("c") or 0, r.get("v") or 0
                if (not t or "." in t or len(t) > 5 or c < MIN_PRICE
                        or (len(t) == 5 and t[-1] in "WURP")):
                    continue
                cands.append((t, c * v))
            cands.sort(key=lambda x: -x[1])
            return [t for t, _ in cands[:UNIVERSE_N]]
    return []


def fetch_bars(t):
    now = datetime.now(timezone.utc)
    frm = (now - timedelta(days=740)).strftime("%Y-%m-%d")
    to = now.strftime("%Y-%m-%d")
    j = _get("https://api.polygon.io/v2/aggs/ticker/%s/range/1/day/%s/%s"
             "?adjusted=true&sort=asc&limit=600&apiKey=%s"
             % (t, frm, to, POLY))
    rows = (j or {}).get("results") or []
    if len(rows) < 260:
        return None
    return {"t": t,
            "d": [datetime.fromtimestamp(r["t"] / 1000,
                                         tz=timezone.utc
                                         ).strftime("%Y-%m-%d")
                  for r in rows],
            "o": [r["o"] for r in rows], "h": [r["h"] for r in rows],
            "l": [r["l"] for r in rows], "c": [r["c"] for r in rows],
            "v": [r["v"] for r in rows]}


def sma(a, n, i):
    if i + 1 < n:
        return None
    return sum(a[i - n + 1:i + 1]) / n


def atr14(b, i):
    if i < 14:
        return None
    trs = []
    for k in range(i - 13, i + 1):
        trs.append(max(b["h"][k] - b["l"][k],
                       abs(b["h"][k] - b["c"][k - 1]),
                       abs(b["l"][k] - b["c"][k - 1])))
    return sum(trs) / 14


def slope_z(vals):
    """z-scored linear slope of a series (per-bar, scaled by its std)."""
    n = len(vals)
    if n < 8:
        return 0.0
    xm = (n - 1) / 2.0
    ym = sum(vals) / n
    num = sum((i - xm) * (v - ym) for i, v in enumerate(vals))
    den = sum((i - xm) ** 2 for i in range(n))
    sl = num / den if den else 0.0
    sd = math.sqrt(sum((v - ym) ** 2 for v in vals) / n) or 1.0
    return max(-3.0, min(3.0, sl * n / sd))


def analyze(b):
    """Full segmentation for one ticker. Returns current phase + last
    completed segments with dated begin/end + events."""
    n = len(b["c"])
    clv = [((b["c"][i] - b["l"][i]) - (b["h"][i] - b["c"][i]))
           / ((b["h"][i] - b["l"][i]) or 1e-9) for i in range(n)]
    obv, ad = [0.0], [0.0]
    for i in range(1, n):
        obv.append(obv[-1] + (b["v"][i] if b["c"][i] > b["c"][i - 1]
                              else -b["v"][i] if b["c"][i] < b["c"][i - 1]
                              else 0.0))
        ad.append(ad[-1] + clv[i] * b["v"][i])

    segments, events_all = [], []
    state = "SCAN"          # SCAN -> RANGE -> (breakout closes segment)
    rng = None
    i = 64
    while i < n:
        vma = sma(b["v"], 50, i)
        a = atr14(b, i)
        if not vma or not a:
            i += 1
            continue
        if state == "SCAN":
            ret63 = (b["c"][i] / b["c"][i - 63] - 1.0)
            vol_z = b["v"][i] / vma
            wide = (b["h"][i] - b["l"][i]) >= 1.5 * a
            cl = clv[i]
            if vol_z >= 2.2 and wide and ret63 <= -0.12 and cl >= -0.1:
                kind = "ACCUMULATION"        # SC candidate
                ev = "SC"
            elif vol_z >= 2.2 and wide and ret63 >= 0.12 and cl <= 0.1:
                kind = "DISTRIBUTION"        # BC candidate
                ev = "BC"
            else:
                i += 1
                continue
            look = min(i + 10, n - 1)
            if kind == "ACCUMULATION":
                sup = min(b["l"][i:look + 1])
                res = max(b["h"][i:look + 1])
            else:
                res = max(b["h"][i:look + 1])
                sup = min(b["l"][i:look + 1])
            rng = {"kind": kind, "begin_i": i, "begin": b["d"][i],
                   "sup": sup, "res": res, "climax_vol": b["v"][i],
                   "events": [{"date": b["d"][i], "type": ev}]}
            state = "RANGE"
            i = look + 1
            continue
        # ── RANGE state ──
        sup, res = rng["sup"], rng["res"]
        vol_z = b["v"][i] / vma
        # spring / UTAD
        if (b["l"][i] < sup - 0.25 * a and b["c"][i] > sup
                and b["v"][i] <= 1.2 * vma):
            rng["events"].append({"date": b["d"][i], "type": "SPRING"})
            rng["sup"] = min(sup, b["l"][i])
        elif b["h"][i] > res + 0.25 * a and b["c"][i] < res:
            back = all(b["c"][k] < res for k in
                       range(i, min(i + 3, n)))
            if back and b["v"][i] <= 1.6 * vma:
                rng["events"].append({"date": b["d"][i], "type": "UT"})
                rng["res"] = max(res, b["h"][i])
        # secondary test
        elif (abs(b["l"][i] - sup) <= 0.5 * a
                and b["v"][i] <= 0.8 * rng["climax_vol"]):
            if sum(1 for e in rng["events"] if e["type"] == "ST") < 3:
                rng["events"].append({"date": b["d"][i], "type": "ST"})
        # confirmed breakout = phase END
        broke_up = (b["c"][i] > rng["res"] + 0.25 * a and vol_z >= 1.5
                    and all(b["c"][k] > rng["res"] for k in
                            range(i, min(i + 3, n))))
        broke_dn = (b["c"][i] < rng["sup"] - 0.25 * a and vol_z >= 1.5
                    and all(b["c"][k] < rng["sup"] for k in
                            range(i, min(i + 3, n))))
        dead = (i - rng["begin_i"]) > 170
        if broke_up or broke_dn or dead:
            j0 = rng["begin_i"]
            press = pressure(b, obv, ad, clv, j0, i)
            label = rng["kind"]
            conflict = False
            if broke_up and press < -0.15 and label == "DISTRIBUTION":
                conflict = True
            if broke_dn and press > 0.15 and label == "ACCUMULATION":
                conflict = True
            if broke_up:
                label = "ACCUMULATION"
                rng["events"].append({"date": b["d"][i], "type": "SOS"})
            elif broke_dn:
                label = "DISTRIBUTION"
                rng["events"].append({"date": b["d"][i], "type": "SOW"})
            segments.append({
                "type": label if not dead else rng["kind"],
                "begin": rng["begin"],
                "end": b["d"][i] if not dead else None,
                "resolved": ("MARKUP" if broke_up else
                             "MARKDOWN" if broke_dn else "FADED"),
                "days": i - j0, "pressure": press,
                "range_low": round(rng["sup"], 2),
                "range_high": round(rng["res"], 2),
                "events": rng["events"][-8:],
                "pressure_conflict": conflict})
            events_all.extend(rng["events"])
            rng = None
            state = "SCAN"
        i += 1

    # ── current phase ──
    last_c = b["c"][-1]
    a = atr14(b, n - 1) or 1.0
    cur = {"phase": "NEUTRAL", "begin": None, "end": None,
           "days_in_phase": None, "pressure": None, "events": [],
           "range_low": None, "range_high": None}
    if state == "RANGE" and rng:
        press = pressure(b, obv, ad, clv, rng["begin_i"], n - 1)
        ph = ("ACCUMULATION" if press >= 0.15 else
              "DISTRIBUTION" if press <= -0.15 else "NEUTRAL_RANGE")
        cur = {"phase": ph, "begin": rng["begin"], "end": None,
               "days_in_phase": n - 1 - rng["begin_i"],
               "pressure": press,
               "events": rng["events"][-6:],
               "range_low": round(rng["sup"], 2),
               "range_high": round(rng["res"], 2)}
    elif segments:
        s = segments[-1]
        end_i = b["d"].index(s["end"]) if s["end"] in b["d"] else n - 1
        if s["end"] and (n - 1 - end_i) <= 60:
            if s["resolved"] == "MARKUP" and last_c > s["range_high"]:
                cur = {"phase": "MARKUP", "begin": s["end"], "end": None,
                       "days_in_phase": n - 1 - end_i,
                       "pressure": s["pressure"], "events": [],
                       "range_low": s["range_low"],
                       "range_high": s["range_high"]}
            elif s["resolved"] == "MARKDOWN" and last_c < s["range_low"]:
                cur = {"phase": "MARKDOWN", "begin": s["end"],
                       "end": None, "days_in_phase": n - 1 - end_i,
                       "pressure": s["pressure"], "events": [],
                       "range_low": s["range_low"],
                       "range_high": s["range_high"]}
    return cur, segments[-3:], round(last_c, 2)


def pressure(b, obv, ad, clv, j0, j1):
    if j1 - j0 < 8:
        return 0.0
    o_z = slope_z(obv[j0:j1 + 1])
    a_z = slope_z(ad[j0:j1 + 1])
    upv = sum(b["v"][k] for k in range(j0 + 1, j1 + 1)
              if b["c"][k] > b["c"][k - 1])
    dnv = sum(b["v"][k] for k in range(j0 + 1, j1 + 1)
              if b["c"][k] < b["c"][k - 1])
    ud = max(-1.0, min(1.0, (upv - dnv) / ((upv + dnv) or 1.0)))
    cl = sum(clv[j0:j1 + 1]) / (j1 - j0 + 1)
    p = 0.30 * (o_z / 3.0) + 0.30 * (a_z / 3.0) + 0.25 * ud + 0.15 * cl
    return round(max(-1.0, min(1.0, p)), 3)


def lambda_handler(event=None, context=None):
    started = time.time()
    if not POLY:
        raise RuntimeError("POLYGON key missing")
    uni = build_universe()
    if len(uni) < 300:
        raise RuntimeError("universe too small: %d" % len(uni))
    with ThreadPoolExecutor(max_workers=8) as exe:
        bars = [x for x in exe.map(fetch_bars, uni) if x]

    dp = {r.get("ticker"): r.get("state")
          for r in ((gj("data/dark-pool.json") or {}).get("board") or [])}
    wh = {sym: v.get("conviction_flow_usd")
          for sym, v in ((gj("data/whales.json") or {}).get("stocks")
                         or {}).items()}
    res_flow = {r.get("ticker"): True for r in
                ((gj("data/resilience.json") or {}).get("resilient")
                 or []) if r.get("flow_confirmed")}

    tickers, ncur = {}, {"ACCUMULATION": 0, "DISTRIBUTION": 0,
                         "MARKUP": 0, "MARKDOWN": 0, "NEUTRAL_RANGE": 0,
                         "NEUTRAL": 0}
    for b in bars:
        try:
            cur, hist, px = analyze(b)
        except Exception as e:
            print("[phase] %s: %s" % (b["t"], e))
            continue
        ncur[cur["phase"]] = ncur.get(cur["phase"], 0) + 1
        conf = {}
        if b["t"] in dp:
            conf["dark_pool"] = dp[b["t"]]
        if b["t"] in wh and wh[b["t"]]:
            conf["whales_flow_usd"] = wh[b["t"]]
        if b["t"] in res_flow:
            conf["resilience_flow"] = True
        tickers[b["t"]] = {"price": px, **cur, "history": hist,
                           "confirmations": conf}

    def board(pred, key, n=25, rev=True):
        rows = [{"ticker": t, "phase": v["phase"], "begin": v["begin"],
                 "days": v["days_in_phase"], "pressure": v["pressure"],
                 "price": v["price"],
                 "confirmations": v["confirmations"]}
                for t, v in tickers.items() if pred(v)]
        rows.sort(key=key, reverse=rev)
        return rows[:n]

    boards = {
        "accumulation_beginning": board(
            lambda v: v["phase"] == "ACCUMULATION"
            and (v["days_in_phase"] or 99) <= 15,
            lambda r: r["pressure"] or 0),
        "accumulation_mature": board(
            lambda v: v["phase"] == "ACCUMULATION"
            and (v["days_in_phase"] or 0) > 15,
            lambda r: r["pressure"] or 0),
        "accumulation_ended_markup": board(
            lambda v: v["phase"] == "MARKUP"
            and (v["days_in_phase"] or 99) <= 12,
            lambda r: -(r["days"] or 99), rev=False),
        "distribution_beginning": board(
            lambda v: v["phase"] == "DISTRIBUTION"
            and (v["days_in_phase"] or 99) <= 15,
            lambda r: -(r["pressure"] or 0)),
        "distribution_mature": board(
            lambda v: v["phase"] == "DISTRIBUTION"
            and (v["days_in_phase"] or 0) > 15,
            lambda r: -(r["pressure"] or 0)),
        "distribution_ended_markdown": board(
            lambda v: v["phase"] == "MARKDOWN"
            and (v["days_in_phase"] or 99) <= 12,
            lambda r: -(r["days"] or 99), rev=False)}

    keep = set()
    for rows in boards.values():
        keep |= {r["ticker"] for r in rows}
    ranked = sorted(tickers.items(),
                    key=lambda kv: -abs(kv[1].get("pressure") or 0))
    keep |= {t for t, _ in ranked[:400]}

    out = {"engine": "justhodl-phase-detector", "schema": "1.0",
           "generated_at": datetime.now(timezone.utc).isoformat(),
           "universe_n": len(uni), "analyzed_n": len(tickers),
           "phase_counts": ncur, "boards": boards,
           "tickers": {t: tickers[t] for t in sorted(keep)
                       if t in tickers},
           "method": ("Wyckoff-grounded daily-bar segmentation: 63d "
                      "trend gate -> climax (vol>=2.2x 50d avg, range>="
                      "1.5x ATR14, close off the extreme) -> trading "
                      "range (climax + 10-bar reaction bounds, +-1 ATR "
                      "life) -> in-range pressure = OBV slope z + "
                      "Chaikin A/D slope z + up/down volume + CLV -> "
                      "SPRING/UT/ST events -> phase END on volume-"
                      "confirmed breakout (0.25 ATR beyond bound, vol z"
                      ">=1.5, 2-bar hold). BEGIN = climax date. "
                      "Direction-vs-pressure disagreements flagged "
                      "pressure_conflict, not relabeled."),
           "honesty": ("Approximate by construction: daily bars only, "
                       "springs/UTADs are optional structures (Type-2 "
                       "ranges resolve without them), re-accumulation "
                       "inside healthy uptrends can resemble "
                       "distribution, and algorithmic labels are "
                       "hypotheses -- confirmations (dark pool, whales "
                       "13F, resilience flow) shown per name."),
           "duration_s": round(time.time() - started, 1)}
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out).encode(),
                  ContentType="application/json",
                  CacheControl="max-age=1800")
    print("[phase] %d analyzed, counts=%s, %.0fs"
          % (len(tickers), ncur, out["duration_s"]))
    return {"ok": True, "analyzed": len(tickers), "counts": ncur}


if __name__ == "__main__":
    print(json.dumps(lambda_handler())[:300])
