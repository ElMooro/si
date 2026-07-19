"""justhodl-screen-backtest v1.0.0 (ops 3542) — one-click screen
backtest for the Fundamental Census Explorer. Takes the CURRENT
members of a screen (<=25 tickers), builds an equal-weight weekly
basket from the cached fundamental-graphs price series, and compares
it to SPY (data/spx-history-deep.json) over 1/3/5y.

HONEST LABEL: this is the hindsight performance of TODAY'S members —
NOT a point-in-time backtest (screen membership is not reconstructed
historically). Stated in every response as `method`.

GET/POST Function URL: ?tickers=AAA,BBB,... (or JSON body
{"tickers":[...]})  → {stats:{cagr_1y/3y/5y basket vs spx, maxdd,
excess}, series:{dates,basket,spx} trimmed 260w}
"""
import json
import bisect
from datetime import datetime, timezone

import boto3

VERSION = "1.0.0"
BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name="us-east-1")
CACHE_TPL = "data/fundgraph/cache/{sym}_quarter_v21.json"


def load_prices(sym):
    try:
        doc = json.loads(S3.get_object(
            Bucket=BUCKET, Key=CACHE_TPL.format(sym=sym))["Body"].read())
        return [(str(d)[:10], float(v)) for d, v in doc.get("price") or []
                if isinstance(v, (int, float)) and v > 0]
    except Exception:  # noqa: BLE001
        return []


def spx_series():
    d = json.loads(S3.get_object(
        Bucket=BUCKET, Key="data/spx-history-deep.json")["Body"].read())
    out = []
    for pt in d.get("points") or []:
        dd, vv = (pt[0], pt[1]) if isinstance(pt, list) else \
            (pt.get("date"), pt.get("close"))
        if dd and isinstance(vv, (int, float)):
            out.append((str(dd)[:10], float(vv)))
    return out


def cagr(series, weeks):
    if len(series) < weeks + 1 or series[-weeks - 1] <= 0:
        return None
    yrs = weeks / 52.0
    return round(((series[-1] / series[-weeks - 1]) ** (1 / yrs) - 1)
                 * 100, 1)


def maxdd(series):
    pk, dd = series[0], 0.0
    for v in series:
        pk = max(pk, v)
        dd = min(dd, v / pk - 1)
    return round(dd * 100, 1)


def lambda_handler(event, context):
    event = event or {}
    qs = event.get("queryStringParameters") or {}
    tick = qs.get("tickers")
    if not tick and event.get("body"):
        try:
            body = event["body"]
            if event.get("isBase64Encoded"):
                import base64
                body = base64.b64decode(body).decode()
            tick = (json.loads(body) or {}).get("tickers")
        except Exception:  # noqa: BLE001
            tick = None
    if isinstance(tick, str):
        tick = [t.strip().upper() for t in tick.split(",") if t.strip()]
    tick = [t for t in (tick or []) if t][:25]
    if len(tick) < 2:
        return _resp({"ok": False,
                      "error": "need 2..25 tickers"}, 400)

    px = {t: load_prices(t) for t in tick}
    px = {t: p for t, p in px.items() if len(p) >= 60}
    if len(px) < 2:
        return _resp({"ok": False,
                      "error": "insufficient cached price history"}, 400)

    spx = spx_series()
    sdates = [d for d, _ in spx]
    smap = dict(spx)

    # union of weekly dates from members, last 6y
    all_dates = sorted({d for p in px.values() for d, _ in p})[-320:]
    def at(p, d):
        i = bisect.bisect_right([x[0] for x in p], d) - 1
        return p[i][1] if i >= 0 else None
    basket = []
    spy = []
    dates = []
    for d in all_dates:
        rels = []
        for t, p in px.items():
            v0 = p[0][1]
            v = at(p, d)
            if v and v0:
                rels.append(v)
        if len(rels) < max(2, int(0.6 * len(px))):
            continue
        j = bisect.bisect_right(sdates, d) - 1
        if j < 0:
            continue
        dates.append(d)
        basket.append(rels)
        spy.append(smap[sdates[j]])
    if len(dates) < 60:
        return _resp({"ok": False, "error": "insufficient overlap"}, 400)

    # normalize each member at first common date; EW index
    first = {t: at(px[t], dates[0]) for t in px}
    idx = []
    for k, d in enumerate(dates):
        vals = []
        for t in px:
            v = at(px[t], d)
            f = first.get(t)
            if v and f:
                vals.append(v / f)
        idx.append(100.0 * sum(vals) / len(vals))
    spyn = [100.0 * v / spy[0] for v in spy]

    stats = {"n_members": len(px),
             "dropped": sorted(set(tick) - set(px)),
             "weeks": len(dates),
             "basket": {"cagr_1y": cagr(idx, 52),
                        "cagr_3y": cagr(idx, 156),
                        "cagr_5y": cagr(idx, 260),
                        "maxdd_pct": maxdd(idx)},
             "spx": {"cagr_1y": cagr(spyn, 52),
                     "cagr_3y": cagr(spyn, 156),
                     "cagr_5y": cagr(spyn, 260),
                     "maxdd_pct": maxdd(spyn)}}
    for w, lab in ((52, "excess_1y"), (156, "excess_3y"),
                   (260, "excess_5y")):
        a, b = cagr(idx, w), cagr(spyn, w)
        stats[lab] = round(a - b, 1) if a is not None and b is not None \
            else None
    keep = 260
    return _resp({"ok": True, "version": VERSION,
                  "generated_at": datetime.now(timezone.utc).isoformat(),
                  "method": ("hindsight EW basket of TODAY'S screen "
                             "members vs SPX — not point-in-time"),
                  "stats": stats,
                  "series": {"dates": dates[-keep:],
                             "basket": [round(v, 2) for v in idx[-keep:]],
                             "spx": [round(v, 2) for v in
                                     spyn[-keep:]]}})


def _resp(obj, code=200):
    return {"statusCode": code,
            "headers": {"Content-Type": "application/json",
                        "Access-Control-Allow-Origin": "*"},
            "body": json.dumps(obj)}
