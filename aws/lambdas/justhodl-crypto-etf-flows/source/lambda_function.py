"""justhodl-crypto-etf-flows · v1.0 — spot BTC & ETH ETF net flows (the marginal buyer).

Since the Jan-2024 (BTC) and Jul-2024 (ETH) US spot-ETF launches, daily ETF creations/
redemptions are the dominant marginal flow in crypto. justhodl-etf-fund-flows already pulls
Polygon's accurate fund_flow (= daily creation/redemption) for a few crypto tickers but lumps
them into a generic "crypto" bucket. This engine is the focused signal:

  - FULL US spot roster, split BTC vs ETH
  - daily / 5d / 30d aggregate net flow (USD), trailing percentile, regime
  - per-ETF leaderboard (who's getting the bid)
  - point-in-time event study: does aggregate ETF flow LEAD spot price?

SOURCE: Polygon /etf-global/v1/fund-flows (fund_flow per ETF per day) + Polygon crypto aggs for
spot price. Feeds crypto-intel / cycle-clock / crypto-confluence / morning-intelligence, and the
ETH leg directly informs BMNR (ETH treasury equity). Self-history + central FDR ledger.
"""
import json
import os
import time
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone, timedelta

import boto3

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/crypto-etf-flows.json"
HIST_KEY = "data/crypto-etf-flows-history.json"
POLY = "https://api.polygon.io"
KEY = os.environ.get("POLYGON_KEY", "")
TIMEOUT = 15

BTC_ETFS = ["IBIT", "FBTC", "BITB", "ARKB", "BTCO", "EZBC", "BRRR", "HODL", "BTCW", "GBTC", "BTC"]
ETH_ETFS = ["ETHA", "FETH", "ETHW", "CETH", "ETHV", "EZET", "QETH", "ETHE", "ETH"]


def _get(url):
    req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-CryptoETF/1.0"})
    with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
        return json.loads(r.read())


def _num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def fetch_flows(ticker, days=150):
    """{date: fund_flow_usd} for one ETF from Polygon."""
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=days + 10)
    url = ("%s/etf-global/v1/fund-flows?composite_ticker=%s&processed_date.gte=%s&processed_date.lte=%s"
           "&order=desc&sort=processed_date&limit=200&apiKey=%s"
           % (POLY, ticker, start.isoformat(), end.isoformat(), KEY))
    try:
        d = _get(url)
    except Exception:
        return {}, None
    out = {}
    latest_aum = None
    for i, r in enumerate(sorted(d.get("results") or [], key=lambda x: x.get("processed_date") or "", reverse=True)):
        dt = r.get("processed_date")
        f = _num(r.get("fund_flow"))
        if dt and f is not None:
            out[dt] = f
        if i == 0:
            nav = _num(r.get("nav")); sh = _num(r.get("shares_outstanding"))
            latest_aum = (nav * sh) if (nav and sh) else None
    return out, latest_aum


def poly_price(sym, days=160):
    """{date: close} for X:BTCUSD / X:ETHUSD."""
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=days)
    url = ("%s/v2/aggs/ticker/X:%sUSD/range/1/day/%s/%s?adjusted=true&sort=asc&limit=400&apiKey=%s"
           % (POLY, sym, start.isoformat(), end.isoformat(), KEY))
    try:
        d = _get(url)
    except Exception:
        return {}
    out = {}
    for b in d.get("results") or []:
        dt = datetime.fromtimestamp(b["t"] / 1000, timezone.utc).date().isoformat()
        out[dt] = b.get("c")
    return out


def _pctile(v, arr):
    return round(100 * sum(1 for x in arr if x <= v) / len(arr)) if arr else None


def build_leg(tickers, price, label):
    daily = defaultdict(float)
    per_etf = {}
    aum_total = 0.0
    for t in tickers:
        rows, aum = fetch_flows(t)
        if not rows:
            continue
        for dt, f in rows.items():
            daily[dt] += f
        latest_dt = max(rows)
        per_etf[t] = round(rows[latest_dt])
        if aum:
            aum_total += aum
    if not daily:
        return {"_err": "no flow data"}
    dates = sorted(daily)
    vals = [daily[d] for d in dates]
    today = vals[-1]
    cum5 = sum(vals[-5:])
    cum30 = sum(vals[-30:])
    # rolling 30d-cum series for percentile
    roll = [sum(vals[max(0, i - 29):i + 1]) for i in range(len(vals))]
    pct = _pctile(cum30, roll)
    regime = ("STRONG INFLOW" if pct is not None and pct >= 80 else "INFLOW" if cum30 > 0
              else "STRONG OUTFLOW" if pct is not None and pct <= 20 else "OUTFLOW" if cum30 < 0 else "FLAT")
    top_in = sorted(per_etf.items(), key=lambda x: -x[1])[:3]
    top_out = sorted(per_etf.items(), key=lambda x: x[1])[:3]

    # event study: 5d-cum flow vs forward spot return (point-in-time percentile)
    es = {}
    pdates = sorted(price)
    ppos = {d: i for i, d in enumerate(pdates)}

    def fwd(d, h):
        i = ppos.get(d)
        if i is None or i + h >= len(pdates):
            return None
        return (price[pdates[i + h]] / price[d] - 1) * 100

    cum5_series = {dates[i]: sum(vals[max(0, i - 4):i + 1]) for i in range(len(dates))}
    cdates = sorted(cum5_series)
    cvals = [cum5_series[d] for d in cdates]
    for h in (10, 20):
        hi, lo = [], []
        for i, d in enumerate(cdates):
            win = cvals[max(0, i - 119):i + 1]
            p = _pctile(cum5_series[d], win)
            f = fwd(d, h)
            if f is None or p is None:
                continue
            if p >= 75:
                hi.append(f)
            elif p <= 25:
                lo.append(f)
        hm = sum(hi) / len(hi) if hi else None
        lm = sum(lo) / len(lo) if lo else None
        es["fwd%dd" % h] = {"inflow_mean": round(hm, 1) if hm is not None else None,
                            "outflow_mean": round(lm, 1) if lm is not None else None,
                            "edge_pp": round(hm - lm, 1) if (hm is not None and lm is not None) else None,
                            "n_in": len(hi), "n_out": len(lo)}
    e20 = (es.get("fwd20d") or {}).get("edge_pp")
    es["hypothesis"] = "heavy ETF inflow days > heavy outflow days on forward %s price" % label
    es["verdict"] = ("INSUFFICIENT" if e20 is None else "CONFIRMED_STRONG" if e20 >= 8
                     else "CONFIRMED" if e20 >= 3 else "INVERTED" if e20 <= -3 else "INCONCLUSIVE")

    return {"flow_today_usd": round(today), "cum_5d_usd": round(cum5), "cum_30d_usd": round(cum30),
            "cum_30d_pctile": pct, "regime": regime, "n_etfs": len(per_etf), "aum_total_usd": round(aum_total),
            "top_inflow": [{"etf": t, "flow_usd": v} for t, v in top_in],
            "top_outflow": [{"etf": t, "flow_usd": v} for t, v in top_out],
            "last_date": dates[-1], "event_study": es}


def lambda_handler(event, context):
    t0 = time.time()
    out = {"generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), "version": "1.0"}
    diag = []
    btc_px = poly_price("BTC")
    eth_px = poly_price("ETH")
    try:
        out["btc_etf"] = build_leg(BTC_ETFS, btc_px, "BTC")
    except Exception as e:
        out["btc_etf"] = {"_err": str(e)[:120]}; diag.append("btc:" + str(e)[:60])
    try:
        out["eth_etf"] = build_leg(ETH_ETFS, eth_px, "ETH")
    except Exception as e:
        out["eth_etf"] = {"_err": str(e)[:120]}; diag.append("eth:" + str(e)[:60])

    b = out.get("btc_etf") or {}
    e = out.get("eth_etf") or {}
    out["btc_flow_30d_usd"] = b.get("cum_30d_usd")
    out["eth_flow_30d_usd"] = e.get("cum_30d_usd")
    out["interpretation"] = None
    if b.get("regime"):
        out["interpretation"] = ("Spot BTC ETFs %s ($%.1fB/30d); ETH ETFs %s ($%.1fB/30d)."
                                 % (b.get("regime"), (b.get("cum_30d_usd") or 0) / 1e9,
                                    e.get("regime"), (e.get("cum_30d_usd") or 0) / 1e9))

    # self-history
    try:
        try:
            hist = json.loads(s3.get_object(Bucket=BUCKET, Key=HIST_KEY)["Body"].read())
        except Exception:
            hist = {"series": []}
        ser = hist.get("series", [])
        today = datetime.now(timezone.utc).date().isoformat()
        ser = [x for x in ser if x.get("date") != today] + [{
            "date": today, "btc_30d": b.get("cum_30d_usd"), "btc_today": b.get("flow_today_usd"),
            "eth_30d": e.get("cum_30d_usd"), "eth_today": e.get("flow_today_usd")}]
        hist["series"] = ser[-365:]
        s3.put_object(Bucket=BUCKET, Key=HIST_KEY, Body=json.dumps(hist, default=str).encode(),
                      ContentType="application/json")
        out["history_n"] = len(hist["series"])
    except Exception as ex:
        diag.append("hist:" + str(ex)[:50])

    out["duration_s"] = round(time.time() - t0, 1)
    out["sources"] = ["Polygon /etf-global/v1/fund-flows (creation/redemption) + crypto aggs"]
    if diag:
        out["_diag"] = diag
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")
    return {"statusCode": 200, "body": json.dumps({"btc_etf_regime": b.get("regime"),
                                                    "btc_30d_usd": b.get("cum_30d_usd"),
                                                    "eth_etf_regime": e.get("regime")})}
