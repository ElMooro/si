"""justhodl-short-book v1.0 — the composed SHORT & AVOID book (ops 3408).

The audit: raw bear inputs existed everywhere (dark-pool distribution,
share-flows forensic flags, Beneish fails, 200-DMA breakdowns, Wyckoff
distribution) but nothing composed them into a graded short book. This does.

Per ticker, independent bear lenses:
  dark_distribution   dark-pool DISTRIBUTION list (quiet selling)      +25
  forensic_flags      SBC_WASH / MGMT_SELLING_INTO_BUYBACK             +20 each (cap 40)
  beneish_fail        M-Score > -1.78 (earnings-manipulation zone)     +20
  ladder_break        fresh 200-DMA cross DOWN (chart-patterns)        +20
  wyckoff_dist        accumulation-radar DISTRIBUTION / LIKELY_TOP     +15
  double_top          chart-patterns double top (stocks scan)          +10

Book = tickers with >=2 lenses, top 20 by score. Each is PAIRED vs its
sector ETF (beta-stripped): graded as direction DOWN with benchmark=pair —
so the scorecard measures pure relative alpha, and the composer can one day
run it net-long/short on merit.

Feed: data/short-book.json · Signals: type "short-book", windows 5/21/63.
"""

import json
import os
import time
from datetime import datetime, timezone

import boto3

from signals_emit import log_signal, yprice

VERSION = "1.1.0"
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")


# ── CENSUS OVERLAY (ops 3550 · SB-CENSUS): conviction / combo / risk / patterns /
# whale-flow per ticker from the Fundamental Census matrix ──
_CENSUS_CACHE = None


def census_idx(s3_client, bucket):
    global _CENSUS_CACHE
    if _CENSUS_CACHE is not None:
        return _CENSUS_CACHE
    import json as _cj
    out = {}
    try:
        mx = _cj.loads(s3_client.get_object(
            Bucket=bucket,
            Key="data/fundamental-census-matrix.json")["Body"].read())
        C = mx.get("cols") or {}
        rk = C.get("risk_score") or []
        xs = sorted(v for v in rk if isinstance(v, (int, float)))
        lo = xs[len(xs)//3] if len(xs) >= 3 else None
        hi = xs[2*len(xs)//3] if len(xs) >= 3 else None
        col = lambda k: C.get(k) or [None] * len(mx.get("tickers") or [])
        for i, t in enumerate(mx.get("tickers") or []):
            pats = [lbl for lbl, k in
                    (("double_bottom", "double_bottom"),
                     ("double_top", "double_top"),
                     ("golden_cross", "golden_cross_10_40w"),
                     ("breakout_20w", "breakout_20w"))
                    if col(k)[i] == 1]
            rv = col("risk_score")[i]
            tier = (None if not isinstance(rv, (int, float)) or lo is None
                    else "LOW" if rv <= lo else "HIGH" if rv >= hi
                    else "MED")
            out[t] = {"conviction": col("conviction_score")[i],
                      "combo": col("combo_score")[i],
                      "risk": rv, "risk_tier": tier,
                      "turn": (mx.get("turn") or [None]*(i+1))[i]
                      if i < len(mx.get("turn") or []) else None,
                      "patterns": pats,
                      "whale_usd_m": col("whale_net_usd_m")[i]}
    except Exception as _e:  # noqa: BLE001
        print("[census-overlay]", str(_e)[:80])
    _CENSUS_CACHE = out
    return out

OUT_KEY = "data/short-book.json"
ETF_OF = {"Technology": "XLK", "Financials": "XLF", "Financial Services": "XLF",
          "Healthcare": "XLV", "Consumer Discretionary": "XLY",
          "Consumer Cyclical": "XLY", "Consumer Staples": "XLP",
          "Consumer Defensive": "XLP", "Energy": "XLE", "Industrials": "XLI",
          "Materials": "XLB", "Basic Materials": "XLB", "Utilities": "XLU",
          "Real Estate": "XLRE", "Communication Services": "XLC"}

s3 = boto3.client("s3", "us-east-1")
ddb = boto3.resource("dynamodb", "us-east-1")


def rj(key):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception:
        return {}


def add(lens_map, tk, lens, pts):
    tk = str(tk or "").upper()
    if not tk or len(tk) > 8:
        return
    e = lens_map.setdefault(tk, {"score": 0, "lenses": []})
    if lens in e["lenses"]:
        return
    e["lenses"].append(lens)
    e["score"] += pts


def lambda_handler(event, context):
    t0 = time.time()
    L = {}

    dp = rj("data/dark-pool.json")

    def _dpwalk(o, in_dist=False):
        if isinstance(o, dict):
            for k, v in o.items():
                _dpwalk(v, in_dist or "distribution" in str(k).lower())
            tk = o.get("ticker") or o.get("symbol")
            if in_dist and tk and isinstance(o.get("score"), (int, float)):
                add(L, tk, "dark_distribution", 25)
        elif isinstance(o, list):
            for v in o:
                _dpwalk(v, in_dist)
    _dpwalk(dp)

    sf = rj("data/share-flows.json")

    def _sfwalk(o):
        if isinstance(o, dict):
            tk = o.get("ticker") or o.get("symbol")
            fl = o.get("flags") or []
            if tk and isinstance(fl, list):
                hits = [f for f in fl if f in ("SBC_WASH",
                                               "MGMT_SELLING_INTO_BUYBACK",
                                               "DEATH_SPIRAL")]
                for i, f in enumerate(hits[:2]):
                    add(L, tk, "forensic:" + f, 20)
            for v in o.values():
                _sfwalk(v)
        elif isinstance(o, list):
            for v in o:
                _sfwalk(v)
    _sfwalk(sf)

    fs = rj("data/forensic-screen.json")

    def _fswalk(o):
        if isinstance(o, dict):
            tk = o.get("ticker") or o.get("symbol")
            m = o.get("m_score")
            if tk and isinstance(m, (int, float)) and m > -1.78:
                add(L, tk, "beneish_fail", 20)
            for v in o.values():
                _fswalk(v)
        elif isinstance(o, list):
            for v in o:
                _fswalk(v)
    _fswalk(fs)

    cp = rj("data/chart-patterns.json")
    for r in (cp.get("cross_down") or []):
        add(L, r.get("symbol"), "ladder_break_200", 20)
    for r in (cp.get("double_tops") or []):
        add(L, r.get("symbol"), "double_top", 10)

    ar = rj("data/accumulation-radar.json")

    def _arwalk(o):
        if isinstance(o, dict):
            tk = o.get("ticker") or o.get("symbol")
            if tk and (o.get("phase") == "DISTRIBUTION"
                       or o.get("flag") == "LIKELY_TOP"):
                add(L, tk, "wyckoff_distribution", 15)
            for v in o.values():
                _arwalk(v)
        elif isinstance(o, list):
            for v in o:
                _arwalk(v)
    _arwalk(ar)

    # ops 3415 (#2): squeeze guard — never short your own squeeze cockpit
    sq = rj("data/squeeze-fuel.json")
    sq_map = {}

    def _sqwalk(o):
        if isinstance(o, dict):
            tk = o.get("ticker") or o.get("symbol")
            sc = o.get("squeeze_score") or o.get("score")
            if tk and isinstance(sc, (int, float)):
                k = str(tk).upper()
                sq_map[k] = max(sq_map.get(k, 0.0), float(sc))
            for v in o.values():
                _sqwalk(v)
        elif isinstance(o, list):
            for v in o:
                _sqwalk(v)
    _sqwalk(sq)

    uni = rj("data/universe.json")
    uni_list = uni if isinstance(uni, list) else (uni.get("stocks")
                                                  or uni.get("universe") or [])
    sec_of = {str(u.get("symbol") or u.get("ticker")).upper(): u.get("sector")
              for u in uni_list if isinstance(u, dict) and u.get("sector")}

    rows = [dict(ticker=tk, **v) for tk, v in L.items()
            if len(v["lenses"]) >= 2 and tk not in ETF_OF.values()
            and tk not in ("SPY", "QQQ")]
    squeeze_excluded = []
    kept = []
    for r in rows:
        sqs = sq_map.get(r["ticker"])
        if sqs is not None and sqs >= 70:
            squeeze_excluded.append({"ticker": r["ticker"], "squeeze": sqs})
            continue
        if sqs is not None and sqs >= 40:
            r["squeeze_risk"] = round(sqs, 1)
        kept.append(r)
    rows = kept
    rows.sort(key=lambda r: -r["score"])
    _cidx = census_idx(s3, S3_BUCKET)
    for _r in rows:
        _cc = _cidx.get(str(_r.get("ticker") or "").upper())
        if _cc:
            _r["census_context"] = _cc
    rows = rows[:20]

    tbl = ddb.Table("justhodl-signals")
    logged = 0
    for r in rows:
        sec = sec_of.get(r["ticker"])
        r["sector"] = sec
        r["pair_etf"] = ETF_OF.get(sec, "SPY")
        r["mark"] = yprice(r["ticker"])
        time.sleep(0.1)
        if r["mark"] and log_signal(
                tbl, "short-book", r["ticker"], "DOWN", [5, 21, 63], r["mark"],
                confidence=min(0.85, (0.45 + r["score"] / 250.0)
                               * (0.7 if r.get("squeeze_risk") else 1.0)),
                rationale="composed bear lenses: " + ", ".join(r["lenses"][:4]),
                benchmark=r["pair_etf"],
                signal_value=str(r["score"]),
                metadata={"engine": "short-book"}):
            logged += 1

    out = {"ok": True, "version": VERSION,
           "generated_at": datetime.now(timezone.utc).isoformat(),
           "elapsed_s": round(time.time() - t0, 2),
           "n_candidates": len(L), "n_book": len(rows), "logged": logged,
           "squeeze_excluded": squeeze_excluded,
           "book": rows,
           "lens_weights": {"dark_distribution": 25, "forensic_flag": 20,
                            "beneish_fail": 20, "ladder_break_200": 20,
                            "wyckoff_distribution": 15, "double_top": 10},
           "methodology": ("Independent bear lenses composed per ticker; "
                           ">=2 lenses required; top 20. Each graded DOWN "
                           "vs its sector ETF (benchmark pair) so the "
                           "scorecard measures beta-stripped alpha.")}
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, separators=(",", ":")).encode(),
                  ContentType="application/json", CacheControl="max-age=300")
    print(f"[short-book] cand={len(L)} book={len(rows)} logged={logged} "
          f"{round(time.time() - t0, 1)}s")
    return {"statusCode": 200,
            "body": json.dumps({"ok": True, "n": len(rows), "logged": logged})}
