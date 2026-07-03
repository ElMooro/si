"""justhodl-crypto-exchange-flows · v1.0 — exchange netflows (accumulation / distribution).

Coins moving ONTO exchanges = potential sell pressure; coins moving OFF (to cold storage) =
accumulation. One of the most-cited on-chain signals, and — unlike SOPR/NUPL/cohort supply,
which are CoinMetrics PRO-tier — the raw exchange flows ARE in the free community tier
(FlowInExNtv / FlowOutExNtv), so this is buildable with real data at no cost.

  netflow = FlowIn − FlowOut  (native units; + = net INFLOW = supply hitting exchanges = bearish;
                               − = net OUTFLOW = coins leaving to storage = accumulation/bullish)

Computes current + 7d/30d cumulative netflow, a trailing percentile, and a POINT-IN-TIME event
study of 30d-cumulative-netflow vs forward BTC return (trailing percentile => no look-ahead),
exactly like DVOL. BTC + ETH. Self-history + central FDR ledger registration.
"""
import json
import time
import urllib.request
from datetime import datetime, timezone

import boto3

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/crypto-exchange-flows.json"
HIST_KEY = "data/crypto-exchange-flows-history.json"
CM = "https://community-api.coinmetrics.io/v4/timeseries/asset-metrics"


def _get(url, timeout=45):
    req = urllib.request.Request(url, headers={"User-Agent": "JustHodl Research raafouis@gmail.com"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def cm_series(asset, metrics, start="2023-01-01"):
    """{date: {metric: float}} from CoinMetrics community, paginated."""
    out = {}
    url = (CM + "?assets=%s&metrics=%s&frequency=1d&start_time=%s&page_size=10000"
           % (asset, ",".join(metrics), start))
    pages = 0
    while url and pages < 6:
        d = _get(url)
        for row in d.get("data", []):
            dt = str(row.get("time", ""))[:10]
            if not dt:
                continue
            rec = {}
            for m in metrics:
                v = row.get(m)
                if v is not None:
                    try:
                        rec[m] = float(v)
                    except (TypeError, ValueError):
                        pass
            if rec:
                out[dt] = rec
        url = d.get("next_page_url")
        pages += 1
    return out


def _cum(series_map, dates, end_i, window):
    lo = max(0, end_i - window + 1)
    return sum(series_map[dates[i]] for i in range(lo, end_i + 1))


def build(asset):
    s = cm_series(asset, ["FlowInExNtv", "FlowOutExNtv", "PriceUSD"])
    dates = sorted(s)
    if len(dates) < 120:
        return {"_err": "insufficient history (%d days)" % len(dates)}
    netf = {}      # daily netflow (native): + inflow / − outflow
    price = {}
    for d in dates:
        fi = s[d].get("FlowInExNtv"); fo = s[d].get("FlowOutExNtv")
        if fi is not None and fo is not None:
            netf[d] = fi - fo
        if s[d].get("PriceUSD") is not None:
            price[d] = s[d]["PriceUSD"]
    ndates = sorted(netf)
    if len(ndates) < 90:
        return {"_err": "insufficient netflow history"}
    nvals = [netf[d] for d in ndates]
    cur = ndates[-1]
    ci = len(ndates) - 1
    cum7 = _cum(netf, ndates, ci, 7)
    cum30 = _cum(netf, ndates, ci, 30)
    # 30d-cumulative-netflow series for percentile + event study
    cum30_series = {}
    for i in range(29, len(ndates)):
        cum30_series[ndates[i]] = _cum(netf, ndates, i, 30)
    c30vals = sorted(cum30_series.values())
    pct = round(100 * sum(1 for x in c30vals if x <= cum30) / len(c30vals)) if c30vals else None
    # trend of 30d cum (last vs 7d ago)
    c30_dates = sorted(cum30_series)
    trend = None
    if len(c30_dates) > 7:
        prev = cum30_series[c30_dates[-8]]
        trend = "RISING (more inflow)" if cum30 > prev else "FALLING (more outflow)"

    # regime: heavy outflow = accumulation (bullish); heavy inflow = distribution (bearish)
    regime = None
    if pct is not None:
        regime = ("HEAVY DISTRIBUTION (inflow)" if pct >= 80 else "DISTRIBUTION" if pct >= 60
                  else "HEAVY ACCUMULATION (outflow)" if pct <= 20 else "ACCUMULATION" if pct <= 40
                  else "BALANCED")

    # ── event study: 30d cum netflow vs forward price (point-in-time percentile) ──
    es = {}
    pdates = sorted(price)
    ppos = {d: i for i, d in enumerate(pdates)}

    def fwd(d, h):
        i = ppos.get(d)
        if i is None or i + h >= len(pdates):
            return None
        return (price[pdates[i + h]] / price[d] - 1) * 100

    pit = []  # (date, trailing-pctile of 30d cum netflow)
    cvals = [cum30_series[d] for d in c30_dates]
    for i, d in enumerate(c30_dates):
        win = cvals[max(0, i - 729):i + 1]
        cv = cum30_series[d]
        pit.append((d, round(100 * sum(1 for x in win if x <= cv) / len(win))))
    for h in (30, 90):
        out_r, in_r = [], []   # outflow(low pctile) vs inflow(high pctile)
        for d, p in pit:
            f = fwd(d, h)
            if f is None:
                continue
            if p <= 25:
                out_r.append(f)     # accumulation
            elif p >= 75:
                in_r.append(f)      # distribution
        om = sum(out_r) / len(out_r) if out_r else None
        im = sum(in_r) / len(in_r) if in_r else None
        es["fwd%dd" % h] = {
            "outflow_mean": round(om, 1) if om is not None else None,
            "inflow_mean": round(im, 1) if im is not None else None,
            "edge_outflow_minus_inflow_pp": round(om - im, 1) if (om is not None and im is not None) else None,
            "n_outflow": len(out_r), "n_inflow": len(in_r)}
    e90 = (es.get("fwd90d") or {}).get("edge_outflow_minus_inflow_pp")
    es["hypothesis"] = "net OUTFLOW (accumulation) > net INFLOW (distribution) on forward price"
    es["verdict"] = ("INSUFFICIENT" if e90 is None else "CONFIRMED_STRONG" if e90 >= 12
                     else "CONFIRMED" if e90 >= 5 else "INVERTED" if e90 <= -5 else "INCONCLUSIVE")
    es["standing"] = "DIAGNOSTIC"
    es["n_days"] = len(ndates)

    return {"netflow_today": round(netf[cur], 1), "cum_7d": round(cum7, 1), "cum_30d": round(cum30, 1),
            "cum_30d_pctile": pct, "regime": regime, "trend": trend,
            "flow_in_today": round(s[cur].get("FlowInExNtv", 0), 1) if cur in s else None,
            "flow_out_today": round(s[cur].get("FlowOutExNtv", 0), 1) if cur in s else None,
            "price": round(price.get(cur, 0), 1), "event_study": es}


def lambda_handler(event, context):
    t0 = time.time()
    out = {"generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), "version": "1.0"}
    diag = []
    for a in ("btc", "eth"):
        try:
            out[a] = build(a)
        except Exception as e:
            out[a] = {"_err": str(e)[:120]}
            diag.append("%s:%s" % (a, str(e)[:60]))

    btc = out.get("btc") or {}
    out["regime"] = btc.get("regime")
    out["cum_30d_btc"] = btc.get("cum_30d")
    out["interpretation"] = (("BTC 30d exchange netflow %s BTC (%sth pctile) — %s"
                              % (btc.get("cum_30d"), btc.get("cum_30d_pctile"), btc.get("regime")))
                             if btc.get("regime") else None)

    try:
        try:
            hist = json.loads(s3.get_object(Bucket=BUCKET, Key=HIST_KEY)["Body"].read())
        except Exception:
            hist = {"series": []}
        ser = hist.get("series", [])
        today = datetime.now(timezone.utc).date().isoformat()
        snap = {"date": today, "btc_netflow_30d": btc.get("cum_30d"), "btc_pctile": btc.get("cum_30d_pctile"),
                "btc_regime": btc.get("regime"), "eth_netflow_30d": (out.get("eth") or {}).get("cum_30d")}
        ser = [x for x in ser if x.get("date") != today] + [snap]
        ser = ser[-365:]
        hist["series"] = ser
        hist["updated_at"] = out["generated_at"]
        s3.put_object(Bucket=BUCKET, Key=HIST_KEY, Body=json.dumps(hist, default=str).encode(),
                      ContentType="application/json")
        out["history_n"] = len(ser)
    except Exception as e:
        diag.append("hist:" + str(e)[:60])

    out["duration_s"] = round(time.time() - t0, 1)
    out["sources"] = ["CoinMetrics community: FlowInExNtv / FlowOutExNtv / PriceUSD"]
    if diag:
        out["_diag"] = diag
    # CryptoQuant fusion (ops 2742): entity-labeled netflow/reserves cross-check; free CM stack retained.
    try:
        cq = json.loads(s3.get_object(Bucket=BUCKET, Key="data/cryptoquant-onchain.json")["Body"].read())
        if cq.get("status") == "LIVE":
            cm = cq.get("metrics") or {}
            out["cryptoquant"] = {"btc_netflow": cm.get("btc_exchange_netflow"),
                "btc_reserve": cm.get("btc_exchange_reserve"), "eth_reserve": cm.get("eth_exchange_reserve"),
                "stablecoin_reserve": cm.get("stablecoin_exchange_reserve"),
                "usdt_reserve": cm.get("usdt_exchange_reserve"), "usdc_reserve": cm.get("usdc_exchange_reserve"),
                "btc_inflow": cm.get("btc_exchange_inflow"), "btc_outflow": cm.get("btc_exchange_outflow"),
                "eth_netflow": cm.get("eth_exchange_netflow"),
                "composite_onchain_risk_z": cq.get("composite_onchain_risk_z"),
                "grading": "PROVISIONAL", "source": "cryptoquant"}
    except Exception as e:
        print("[xf] cq join skipped:", str(e)[:80])

    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")
    return {"statusCode": 200, "body": json.dumps({"regime": out.get("regime"),
                                                    "cum_30d_btc": out.get("cum_30d_btc"),
                                                    "verdict": (btc.get("event_study") or {}).get("verdict")})}
