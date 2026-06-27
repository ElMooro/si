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
    out["sources"] = ["Deribit get_volatility_index_data (DVOL)"]
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
