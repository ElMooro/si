"""justhodl-symbol-feed v1.0 ops4136 — bulk yahoo-chart resolver for the
classes FinViz can't price: intl equities (suffix map), OANDA FX (=X),
SSE numerics (.SS), major indices. Rotating 700/run cap; self-completing.
Writes data/symbol-feed.json keyed by BARE symbol."""
import json
import re
import time
import urllib.request
from datetime import datetime, timezone

import boto3

MARKER = "symbol-feed v1.0 ops4136"
S3 = boto3.client("s3")
BUCKET = "justhodl-dashboard-live"
UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64)"}

SUFFIX = {"SGX": ".SI", "LSE": ".L", "SIX": ".SW", "SWB": ".SG",
          "TRADEGATE": ".DE", "XETR": ".DE", "FWB": ".F", "TWSE": ".TW",
          "TPEX": ".TWO", "HKEX": ".HK", "KRX": ".KS", "TSE": ".T",
          "JPX": ".T", "ASX": ".AX", "TSX": ".TO", "TSXV": ".V",
          "BMV": ".MX", "BIST": ".IS", "OMXSTO": ".ST", "OMXHEX": ".HE",
          "OMXCOP": ".CO", "OSL": ".OL", "BME": ".MC", "MIL": ".MI",
          "SSE": ".SS", "SZSE": ".SZ", "NSE": ".NS", "BSE": ".BO",
          "IDX": ".JK", "SET": ".BK", "MYX": ".KL", "HOSE": ".VN",
          "TASE": ".TA", "EGX": ".CA", "JSE": ".JO", "BVMF": ".SA",
          "BCBA": ".BA", "BVL": ".LM", "WSE": ".WA", "GPW": ".WA"}
EURONEXT_TRY = (".PA", ".AS", ".BR", ".LS")
INDEX_MAP = {"FTSE:UKX": "^FTSE", "DJ:DJI": "^DJI", "HSI:HSI": "^HSI",
             "SSE:000001": "000001.SS", "XETR:DAX": "^GDAXI",
             "TVC:NI225": "^N225", "TVC:SPX": "^GSPC"}


def chart_price(ysym):
    try:
        req = urllib.request.Request(
            "https://query1.finance.yahoo.com/v8/finance/chart/"
            + urllib.request.quote(ysym) + "?range=1d&interval=1d",
            headers=UA)
        with urllib.request.urlopen(req, timeout=8) as r:
            t = r.read().decode("utf-8", "ignore")
        m = re.search(r'"regularMarketPrice":([\d\.eE\+\-]+)', t)
        if m:
            return float(m.group(1))
    except Exception:
        pass
    return None


def targets():
    wl = json.loads(S3.get_object(Bucket=BUCKET,
                                  Key="data/tv-watchlists.json")["Body"].read())
    lists = wl.get("lists") or wl.get("watchlists") or []
    out = {}
    for l in lists:
        for sy in l.get("symbols") or []:
            sy = str(sy)
            if ":" not in sy or sy in out:
                continue
            ex, bare = sy.split(":", 1)
            if sy in INDEX_MAP:
                out[sy] = [INDEX_MAP[sy]]
            elif ex == "OANDA" and re.fullmatch(r"[A-Z]{6}", bare):
                out[sy] = [bare + "=X"]
            elif ex == "EURONEXT":
                out[sy] = [bare + sfx for sfx in EURONEXT_TRY]
            elif ex in SUFFIX:
                out[sy] = [bare + SUFFIX[ex]]
    return out


def lambda_handler(event, context):
    t0 = time.time()
    try:
        prev = json.loads(S3.get_object(
            Bucket=BUCKET, Key="data/symbol-feed.json")["Body"].read())
        store = prev.get("prices") or {}
    except Exception:
        store = {}
    tg = targets()
    todo = [k for k in tg if k not in store]
    if not todo:
        todo = sorted(tg, key=lambda k: str(
            (store.get(k) or {}).get("asof")))[:300]
    ok = err = 0
    for full in todo[:700]:
        if time.time() - t0 > 250:
            break
        bare = full.split(":", 1)[1]
        pv = None
        used = None
        for ysym in tg[full]:
            pv = chart_price(ysym)
            if pv is not None:
                used = ysym
                break
        if pv is None:
            err += 1
            store.setdefault(full, {"miss": True})
            continue
        ok += 1
        store[full] = {"value": round(pv, 4), "ysym": used,
                       "asof": datetime.now(timezone.utc)
                       .isoformat()[:16]}
        store[bare] = store[full]
    doc = {"generated_at": datetime.now(timezone.utc).isoformat(),
           "marker": MARKER, "targets": len(tg),
           "resolved": sum(1 for v in store.values()
                           if isinstance(v, dict) and "value" in v),
           "prices": store, "run_ok": ok, "run_err": err,
           "elapsed_s": round(time.time() - t0, 1)}
    S3.put_object(Bucket=BUCKET, Key="data/symbol-feed.json",
                  Body=json.dumps(doc).encode(),
                  ContentType="application/json", CacheControl="max-age=600")
    print("[symbol-feed] ok=%d err=%d resolved=%d/%d %.0fs"
          % (ok, err, doc["resolved"], len(tg), doc["elapsed_s"]))
    return {"ok": ok, "err": err, "resolved": doc["resolved"]}
