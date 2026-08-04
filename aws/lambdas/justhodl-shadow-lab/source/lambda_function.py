"""justhodl-shadow-lab v1.0 — where borrowed ideas earn their seats.
Reads data/methodology-gaps.json, spawns the first two shadow
indicators (ATR volatility-regime, ADX trend-strength) on real OHLC,
logs signals via the shared emitter (hereditary fabric stamping from
birth), and lets the leaderboard/Wilson machinery decide adoption.
Constitution enforced: shadows are candidates, grades are law.
Output: data/shadow-lab.json"""
import json, os, time, urllib.request
from datetime import datetime, timezone

import boto3
from signals_emit import log_signal

s3 = boto3.client("s3", region_name="us-east-1")
B = "justhodl-dashboard-live"
FMP = next((v for k, v in os.environ.items()
            if "FMP" in k.upper() and v), "")


def rd(key):
    try:
        return json.loads(s3.get_object(Bucket=B, Key=key)
                          ["Body"].read())
    except Exception:
        return None


def ohlc(sym, n=60):
    for url in (
            "https://financialmodelingprep.com/stable/"
            "historical-price-eod/full?symbol=%s&apikey=%s"
            % (sym, FMP),
            "https://financialmodelingprep.com/api/v3/"
            "historical-price-full/%s?timeseries=%d&apikey=%s"
            % (sym, n + 5, FMP)):
        try:
            d = json.loads(urllib.request.urlopen(
                url, timeout=20).read().decode())
            rows = (d if isinstance(d, list)
                    else d.get("historical", []))
            rows = sorted(rows,
                          key=lambda r: r.get("date", ""))[-n:]
            out = [(float(r["high"]), float(r["low"]),
                    float(r["close"])) for r in rows
                   if r.get("close")]
            if out:
                return out
        except Exception:
            continue
    return []


def atr_adx(bars, per=14):
    if len(bars) < per * 2 + 2:
        return None
    trs, pdms, ndms = [], [], []
    for i in range(1, len(bars)):
        h, l, c = bars[i]
        ph, pl, pc = bars[i - 1]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
        up, dn = h - ph, pl - l
        pdms.append(up if up > dn and up > 0 else 0.0)
        ndms.append(dn if dn > up and dn > 0 else 0.0)

    def wilder(xs):
        a = sum(xs[:per]) / per
        out = [a]
        for x in xs[per:]:
            a = (a * (per - 1) + x) / per
            out.append(a)
        return out
    atr = wilder(trs)
    pdi = [100 * p / a if a else 0
           for p, a in zip(wilder(pdms), atr)]
    ndi = [100 * n0 / a if a else 0
           for n0, a in zip(wilder(ndms), atr)]
    dx = [100 * abs(p - n0) / (p + n0) if (p + n0) else 0
          for p, n0 in zip(pdi, ndi)]
    adx = wilder(dx)[-1] if len(dx) >= per else None
    c0 = bars[-1][2]
    atr_pct = 100.0 * atr[-1] / c0 if c0 else None
    # ATR percentile vs its own trail
    trail = [100.0 * a / b[2] for a, b in
             zip(atr, bars[per:]) if b[2]]
    pr = (100.0 * sum(1 for x in trail if x <= atr_pct)
          / len(trail)) if trail else None
    return {"atr_pct": round(atr_pct, 2),
            "atr_pctile": round(pr, 0) if pr else None,
            "adx": round(adx, 1) if adx else None,
            "plus_di": round(pdi[-1], 1),
            "minus_di": round(ndi[-1], 1),
            "close": c0}


def lambda_handler(event=None, context=None):
    t0 = time.time()
    gaps = rd("data/methodology-gaps.json") or {}
    roster = [g["name"] for g in (gaps.get("gaps") or [])[:12]]
    fab = rd("data/signal-fabric.json") or {}
    syms = [t["ticker"] for t in (fab.get("tickers") or [])[:40]]
    tbl = boto3.resource("dynamodb",
                         region_name="us-east-1"
                         ).Table("justhodl-signals")
    rows, logged = [], 0
    for sym in syms:
        m = atr_adx(ohlc(sym))
        if not m:
            continue
        sig = None
        if m["adx"] and m["adx"] >= 25:
            d0 = ("UP" if m["plus_di"] > m["minus_di"]
                  else "DOWN")
            sig = ("shadow-adx-trend", d0,
                   "ADX %.0f trending %s" % (m["adx"], d0),
                   min(0.85, 0.4 + m["adx"] / 100.0))
        elif m["atr_pctile"] is not None \
                and m["atr_pctile"] <= 25 \
                and m["adx"] and m["adx"] < 20:
            sig = ("shadow-atr-squeeze", "UP",
                   "vol compression: ATR pctile %.0f, ADX %.0f"
                   % (m["atr_pctile"], m["adx"]), 0.55)
        row = {"ticker": sym, **m,
               "signal": sig[0] if sig else None}
        rows.append(row)
        if sig:
            try:
                if log_signal(tbl, sig[0], sym, sig[1],
                              [7, 21], m["close"],
                              confidence=sig[3],
                              rationale=sig[2],
                              metadata={"engine": sig[0],
                                        "born_of":
                                        "methodology-gaps"}):
                    logged += 1
            except Exception as e:
                print("[shadow] log fail %s: %s"
                      % (sym, str(e)[:60]))
    out = {"engine": "justhodl-shadow-lab", "version": "1.0",
           "debug_key_len": len(FMP),
           "generated_at": datetime.now(timezone.utc).isoformat(),
           "elapsed_s": round(time.time() - t0, 1),
           "constitution": ("shadow candidates; adoption only by "
                            "Wilson-proven grades on the "
                            "leaderboard"),
           "gap_roster_next": roster,
           "n_computed": len(rows), "n_logged": logged,
           "rows": sorted(rows,
                          key=lambda r: -(r.get("adx") or 0)
                          )[:60]}
    s3.put_object(Bucket=B, Key="data/shadow-lab.json",
                  Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json",
                  CacheControl="no-cache")
    print(json.dumps({"ok": True, "computed": len(rows),
                      "logged": logged}))
    return {"ok": True}
