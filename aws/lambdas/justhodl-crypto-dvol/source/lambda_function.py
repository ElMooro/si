"""
justhodl-crypto-dvol  ·  v1.0 — Crypto implied-volatility complex (the missing crypto-VIX).

The crypto stack already covers stablecoin liquidity (crypto-liquidity / stablecoin-flow) and
perp funding+OI (crypto-funding). The one missing layer is OPTION-IMPLIED VOLATILITY — crypto's
VIX/MOVE equivalent. Deribit's DVOL is the market-standard 30-day implied-vol index for BTC & ETH,
free and US-reachable (unlike Binance/Bybit). This engine surfaces:

  · BTC & ETH DVOL (current), trailing-1y percentile (point-in-time), regime
  · 30-day vol trend (rising / falling) and BTC-vs-ETH vol spread
  · a composite crypto-vol regime for the cycle clock's volatility complex

Writes data/crypto-dvol.json. Sources: Deribit public API (get_volatility_index_data).
"""
import json
import time
import urllib.request
from datetime import datetime, timezone

import boto3

VERSION = "1.0"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/crypto-dvol.json"
s3 = boto3.client("s3", region_name="us-east-1")


def _get(url, tmo=15):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    return json.loads(urllib.request.urlopen(req, timeout=tmo).read())


def _pctile(series, val):
    if not series or val is None:
        return None
    return round(100 * sum(1 for x in series if x <= val) / len(series))


def _regime(v):
    if v is None:
        return None
    return "LOW" if v < 40 else "NORMAL" if v < 60 else "ELEVATED" if v < 80 else "HIGH"


def dvol_dated(ccy, days=1100):
    """Daily DVOL closes keyed by ISO date, ~3y, for the event study."""
    now = int(time.time() * 1000)
    d = _get(f"https://www.deribit.com/api/v2/public/get_volatility_index_data"
             f"?currency={ccy}&start_timestamp={now - days * 86400000}&end_timestamp={now}"
             f"&resolution=86400")["result"]["data"]
    out = {}
    for row in d:
        if row and row[4]:
            iso = datetime.fromtimestamp(row[0] / 1000, tz=timezone.utc).date().isoformat()
            out[iso] = row[4]
    return out


def cb_daily(product, since):
    """Coinbase daily closes, paginated (300/call). Returns {iso_date: close}."""
    out = {}
    cur = datetime.fromisoformat(since + "T00:00:00+00:00")
    now = datetime.now(timezone.utc)
    step = 300 * 86400
    while cur < now:
        end = min(datetime.fromtimestamp(cur.timestamp() + step, tz=timezone.utc), now)
        url = (f"https://api.exchange.coinbase.com/products/{product}/candles"
               f"?granularity=86400&start={cur.strftime('%Y-%m-%dT%H:%M:%SZ')}"
               f"&end={end.strftime('%Y-%m-%dT%H:%M:%SZ')}")
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "justhodl/1.0"})
            with urllib.request.urlopen(req, timeout=30) as r:
                rows = json.loads(r.read())
            for row in rows:  # [time, low, high, open, close, volume]
                d = datetime.fromtimestamp(row[0], tz=timezone.utc).date().isoformat()
                out[d] = float(row[4])
        except Exception:
            pass
        time.sleep(0.34)
        cur = datetime.fromtimestamp(cur.timestamp() + step, tz=timezone.utc)
    return out


def dvol_event_study():
    """Point-in-time (no look-ahead) test: does DVOL percentile predict forward BTC?
    Hypothesis (buy-fear, like VIX): HIGH DVOL pctile (fear) → stronger forward BTC than
    LOW DVOL pctile (complacency). Trailing-2y percentile at each date; clean target = real
    Coinbase BTC forward return. DIAGNOSTIC until the central FDR scorecard grades it live."""
    es = {}
    try:
        dser = dvol_dated("BTC", 1100)
        ddates = sorted(dser)
        if len(ddates) < 200:
            return {"standing": "INSUFFICIENT", "n_days": len(ddates)}
        btc = cb_daily("BTC-USD", ddates[0])
        bdates = sorted(btc); bpos = {d: i for i, d in enumerate(bdates)}

        def bfwd(d, h):
            i = bpos.get(d)
            if i is None or i + h >= len(bdates):
                return None
            return (btc[bdates[i + h]] / btc[d] - 1) * 100

        dvals = [dser[d] for d in ddates]
        pit = []  # (date, trailing-2y percentile of DVOL)
        for i, d in enumerate(ddates):
            win = dvals[max(0, i - 729):i + 1]; cur = dser[d]
            pit.append((d, round(100 * sum(1 for x in win if x <= cur) / len(win))))
        for h in (30, 90, 180):
            lo_r, hi_r = [], []
            for d, p in pit:
                f = bfwd(d, h)
                if f is None:
                    continue
                if p <= 25:
                    lo_r.append(f)        # complacency
                elif p >= 75:
                    hi_r.append(f)        # fear
            lm = sum(lo_r) / len(lo_r) if lo_r else None
            hm = sum(hi_r) / len(hi_r) if hi_r else None
            es[f"fwd{h}d"] = {
                "low_pctile_mean": round(lm, 1) if lm is not None else None,
                "high_pctile_mean": round(hm, 1) if hm is not None else None,
                "low_hit_pct": round(100 * sum(1 for x in lo_r if x > 0) / len(lo_r)) if lo_r else None,
                "high_hit_pct": round(100 * sum(1 for x in hi_r if x > 0) / len(hi_r)) if hi_r else None,
                "n_low": len(lo_r), "n_high": len(hi_r),
                "edge_high_minus_low_pp": round(hm - lm, 1) if (lm is not None and hm is not None) else None,
            }
        e90 = (es.get("fwd90d") or {}).get("edge_high_minus_low_pp")
        es["hypothesis"] = "buy-fear: HIGH DVOL pctile (fear) > LOW DVOL pctile (complacency) on forward BTC"
        es["verdict"] = ("INSUFFICIENT" if e90 is None else
                         "CONFIRMED_STRONG" if e90 >= 12 else "CONFIRMED" if e90 >= 5 else
                         "INVERTED" if e90 <= -5 else "INCONCLUSIVE")
        es["standing"] = "DIAGNOSTIC"  # earns trust only via the central FDR scorecard over live outcomes
        es["n_days"] = len(ddates)
    except Exception as e:
        es = {"_err": str(e)[:80]}
    return es


def dvol(ccy):
    """Daily DVOL closes for ~1y → current, 1y percentile, regime, 30d trend."""
    now = int(time.time() * 1000)
    d = _get(f"https://www.deribit.com/api/v2/public/get_volatility_index_data"
             f"?currency={ccy}&start_timestamp={now - 370 * 86400000}&end_timestamp={now}"
             f"&resolution=86400")["result"]["data"]
    rows = [r for r in d if r and r[4]]
    closes = [r[4] for r in rows]
    cur = closes[-1] if closes else None
    chg_30d = None
    if len(closes) > 31 and closes[-31]:
        chg_30d = round(cur - closes[-31], 1)
    return {
        "dvol": round(cur, 1) if cur is not None else None,
        "pctile_1y": _pctile(closes, cur),
        "regime": _regime(cur),
        "chg_30d": chg_30d,
        "trend": ("RISING" if (chg_30d or 0) > 2 else "FALLING" if (chg_30d or 0) < -2 else "FLAT"),
        "n_days": len(closes),
    }


def lambda_handler(event=None, context=None):
    t0 = time.time()
    out = {"engine": "crypto-dvol", "version": VERSION,
           "generated_at": datetime.now(timezone.utc).isoformat()}
    errs = []
    btc = eth = {}
    try:
        btc = dvol("BTC")
    except Exception as e:
        errs.append(f"BTC: {str(e)[:60]}")
    try:
        eth = dvol("ETH")
    except Exception as e:
        errs.append(f"ETH: {str(e)[:60]}")
    out["btc"] = btc
    out["eth"] = eth

    bd = btc.get("dvol"); ed = eth.get("dvol")
    out["btc_eth_spread"] = round(ed - bd, 1) if (bd is not None and ed is not None) else None
    # composite regime — driven by BTC DVOL (the headline crypto-vol gauge)
    reg = btc.get("regime")
    out["crypto_vol_regime"] = reg
    out["crypto_vol_pctile"] = btc.get("pctile_1y")
    out["interpretation"] = {
        "LOW": "crypto implied vol suppressed — complacency / low hedging demand (can precede vol expansion)",
        "NORMAL": "crypto implied vol normal",
        "ELEVATED": "crypto implied vol elevated — rising hedging demand / stress building",
        "HIGH": "crypto implied vol high — fear / crash-hedge demand (often near capitulation)",
    }.get(reg)
    out["duration_s"] = round(time.time() - t0, 1)
    out["event_study_dvol"] = dvol_event_study()
    out["sources"] = ["Deribit get_volatility_index_data (DVOL)", "Coinbase BTC-USD (event-study target)"]
    if errs:
        out["errors"] = errs

    s3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, default=str).encode("utf-8"),
                  ContentType="application/json", CacheControl="no-cache, max-age=0")
    print(f"[crypto-dvol] BTC DVOL {btc.get('dvol')} ({btc.get('regime')}, "
          f"{btc.get('pctile_1y')}th pctile, {btc.get('trend')}) | ETH {eth.get('dvol')}")
    return {"statusCode": 200, "body": json.dumps({"ok": True, "btc_dvol": btc.get("dvol"),
                                                   "regime": reg})}


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), indent=2))
