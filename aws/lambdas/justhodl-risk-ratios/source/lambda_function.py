"""
justhodl-risk-ratios -- daily market-priced risk-appetite ratios.

Feeds the canary grid (and anything else) the fast, causal, market-priced
equity leads that FRED cannot provide:

  hyg_lqd   -- HYG/LQD: junk vs investment-grade credit ETFs. The cleanest
               daily risk-appetite ratio; credit reprices before equities.
  angl_hyg  -- ANGL/HYG: fallen angels vs broad HY. Fallen-angel relative
               weakness = the quality seam inside junk starting to tear.
  hyg       -- HYG itself (the high-yield bond ETF tape).
  acwi      -- ACWI (MSCI All-Country World ETF): the global equity tape.
  rxi       -- iShares Global Consumer Discretionary: the investable proxy
               for global consumer products & services demand.
  eem_rvol  -- EEM 21-day realized volatility, annualized %: emerging-market
               volatility without the discontinued VXEEM.
  oil_term  -- WTI front-month minus 2nd-month ($, Yahoo futures): positive
               = backwardation. Flagged per the operator's doctrine that a
               flip in the oil curve has preceded major stress events.

Output: data/risk-ratios.json -- each metric at TOP LEVEL as
{latest, asof, unit, history: [[YYYY-MM-DD, value], ...] oldest-first}
so the canary grid can consume it via  feed:risk-ratios:<key>.history .
Real data only (Polygon + Yahoo), stdlib + boto3, no shared imports.
"""
import json
import math
import os
import time
import urllib.request
from datetime import datetime, timedelta, timezone

import boto3

S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/risk-ratios.json"
s3 = boto3.client("s3")

POLY_KEY = (os.environ.get("POLYGON_KEY") or os.environ.get("POLYGON_API_KEY")
            or os.environ.get("POLY_KEY") or "")


def http_json(url, timeout=25, tries=3, ua="justhodl-risk-ratios/1.0"):
    req = urllib.request.Request(url, headers={"User-Agent": ua,
                                               "Accept": "application/json"})
    for attempt in range(tries):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode("utf-8", "replace"))
        except Exception as e:
            if attempt == tries - 1:
                print("[http] %s -> %s" % (url[:90], e))
                return None
            time.sleep(1 + attempt)


def poly_daily(ticker, days=640):
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=days)
    url = ("https://api.polygon.io/v2/aggs/ticker/%s/range/1/day/%s/%s"
           "?adjusted=true&sort=asc&limit=50000&apiKey=%s"
           % (ticker, start.isoformat(), end.isoformat(), POLY_KEY))
    d = http_json(url) or {}
    out = []
    for r in d.get("results") or []:
        try:
            dt = datetime.fromtimestamp(r["t"] / 1000,
                                        tz=timezone.utc).date().isoformat()
            out.append((dt, float(r["c"])))
        except Exception:
            continue
    return out  # oldest-first


def ratio_series(a, b):
    bmap = dict(b)
    out = []
    for d, v in a:
        w = bmap.get(d)
        if w not in (None, 0):
            out.append([d, round(v / w, 5)])
    return out


def realized_vol(series, win=21):
    out = []
    for i in range(win, len(series)):
        rets = []
        for j in range(i - win + 1, i + 1):
            p0, p1 = series[j - 1][1], series[j][1]
            if p0 and p1:
                rets.append(math.log(p1 / p0))
        if len(rets) >= win - 2:
            m = sum(rets) / len(rets)
            var = sum((x - m) ** 2 for x in rets) / (len(rets) - 1)
            out.append([series[i][0],
                        round((var ** 0.5) * (252 ** 0.5) * 100.0, 2)])
    return out


def yahoo_daily(symbol, rng="6mo"):
    url = ("https://query1.finance.yahoo.com/v8/finance/chart/%s"
           "?range=%s&interval=1d"
           % (urllib.request.quote(symbol), rng))
    d = http_json(url, ua="Mozilla/5.0 (justhodl)") or {}
    try:
        res = d["chart"]["result"][0]
        ts = res["timestamp"]
        cl = res["indicators"]["quote"][0]["close"]
        out = []
        for t, c in zip(ts, cl):
            if c is not None:
                out.append((datetime.fromtimestamp(
                    t, tz=timezone.utc).date().isoformat(), float(c)))
        return out
    except Exception as e:
        print("[yahoo] %s: %s" % (symbol, e))
        return []


MONTH_CODES = "FGHJKMNQUVXZ"


def wti_second_month_symbol(today=None):
    """The active 2nd WTI contract: front is usually next calendar month, so
    2nd = month after that (roll subtleties are immaterial for a sign-level
    backwardation canary)."""
    d = today or datetime.now(timezone.utc).date()
    m, y = d.month + 2, d.year
    if m > 12:
        m -= 12
        y += 1
    return "CL%s%s.NYM" % (MONTH_CODES[m - 1], str(y)[-2:])


def metric(latest_pair, unit, history):
    if not history:
        return {"available": False, "unit": unit, "history": []}
    return {"available": True, "latest": history[-1][1],
            "asof": history[-1][0], "unit": unit,
            "history": history[-520:]}


def lambda_handler(event, context):
    t0 = time.time()
    px = {t: poly_daily(t) for t in
          ("HYG", "LQD", "ANGL", "ACWI", "RXI", "EEM")}
    for t, s in px.items():
        print("[px] %s %d rows" % (t, len(s)))

    out = {
        "schema_version": "1.0",
        "engine": "justhodl-risk-ratios",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "how_to_read": ("Market-priced risk-appetite ratios, daily. These "
                        "move BEFORE the macro data because credit and the "
                        "marginal risk trade reprice first. Consumed by the "
                        "canary grid as feed: canaries."),
    }
    out["hyg_lqd"] = metric(None, "ratio",
                            ratio_series(px["HYG"], px["LQD"]))
    out["hyg_lqd"]["name"] = "HYG/LQD junk-vs-IG risk appetite"
    out["angl_hyg"] = metric(None, "ratio",
                             ratio_series(px["ANGL"], px["HYG"]))
    out["angl_hyg"]["name"] = "Fallen angels vs broad HY (ANGL/HYG)"
    out["hyg"] = metric(None, "$", [[d, v] for d, v in px["HYG"]])
    out["hyg"]["name"] = "High-yield bond ETF (HYG)"
    out["acwi"] = metric(None, "$", [[d, v] for d, v in px["ACWI"]])
    out["acwi"]["name"] = "MSCI All-Country World (ACWI)"
    out["rxi"] = metric(None, "$", [[d, v] for d, v in px["RXI"]])
    out["rxi"]["name"] = "Global consumer discretionary (RXI)"
    out["eem_rvol"] = metric(None, "% ann.",
                             realized_vol([[d, v] for d, v in px["EEM"]]))
    out["eem_rvol"]["name"] = "Emerging-market realized vol (EEM 21d)"

    # oil term structure: WTI front vs 2nd month (Yahoo futures, best-effort)
    front = yahoo_daily("CL=F")
    second_sym = wti_second_month_symbol()
    second = yahoo_daily(second_sym)
    term = []
    if front and second:
        smap = dict(second)
        for d, v in front:
            w = smap.get(d)
            if w is not None:
                term.append([d, round(v - w, 3)])
    out["oil_term"] = metric(None, "$/bbl (front-2nd)", term)
    out["oil_term"]["name"] = "WTI term structure (backwardation +)"
    out["oil_term"]["second_contract"] = second_sym
    out["build_seconds"] = round(time.time() - t0, 1)

    n_live = sum(1 for k in ("hyg_lqd", "angl_hyg", "hyg", "acwi", "rxi",
                             "eem_rvol", "oil_term")
                 if out[k].get("available"))
    out["n_live"] = n_live
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, allow_nan=False),
                  ContentType="application/json", CacheControl="max-age=300")
    print(json.dumps({"n_live": n_live,
                      "hyg_lqd": out["hyg_lqd"].get("latest"),
                      "oil_term": out["oil_term"].get("latest")}))
    if n_live < 4:
        return {"statusCode": 500,
                "body": json.dumps({"ok": False, "n_live": n_live})}
    return {"statusCode": 200,
            "body": json.dumps({"ok": True, "n_live": n_live})}
