"""
justhodl-fed-collateral — NY Fed open-market collateral operations.

  • Securities Lending — daily SOMA securities-lending take-up. When dealers
    borrow heavily against the Fed's portfolio it signals collateral scarcity
    / specials pressure in the repo market (a plumbing stress tell).
  • AMBS — agency-MBS operations (purchases / reinvestment). Mostly small under
    QT, but flags any return to MBS support.

Source: markets.newyorkfed.org/api/{seclending,ambs}
OUTPUT: data/fed-collateral.json     SCHEDULE: daily 12:40 UTC (after the ops post)
"""
import json
import urllib.request
from datetime import datetime, timezone

import boto3

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/fed-collateral.json"
NYFED = "https://markets.newyorkfed.org/api"
UA = {"User-Agent": "JustHodl Research raafouis@gmail.com"}


def _get(url, t=45):
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=t) as r:
            return json.loads(r.read().decode("utf-8", "ignore"))
    except Exception as e:
        print("fetch fail %s: %s" % (url[:70], e))
        return None


def _bn(x):
    try:
        return round(float(x) / 1e9, 3)
    except Exception:
        return None


def lambda_handler(event=None, context=None):
    now = datetime.now(timezone.utc).isoformat()

    # ---- Securities Lending: daily total accepted (collateral demand) ----
    sl = _get(NYFED + "/seclending/all/results/summary/last/250.json") or {}
    ops = (sl.get("seclending") or {}).get("operations", [])
    by_day = {}
    for o in ops:
        d = o.get("operationDate")
        v = _bn(o.get("totalParAmtAccepted"))
        if d and v is not None:
            by_day[d] = round(by_day.get(d, 0) + v, 3)
    sl_series = sorted(([d, v] for d, v in by_day.items()), key=lambda x: x[0])

    # top borrowed securities (latest op, by outstanding loans) — specials watch
    det = _get(NYFED + "/seclending/all/results/details/latest.json") or {}
    dops = (det.get("seclending") or {}).get("operations", [])
    top_specials = []
    if dops:
        rows = dops[0].get("details", [])
        ranked = sorted(rows, key=lambda r: (r.get("outstandingLoans") or 0), reverse=True)
        for r in ranked[:12]:
            ol = r.get("outstandingLoans") or 0
            if ol <= 0:
                continue
            top_specials.append({"security": r.get("securityDescription"),
                                 "cusip": r.get("cusip"),
                                 "outstanding_loans_bn": _bn(ol),
                                 "soma_holdings_bn": _bn(r.get("somaHoldings")),
                                 "rate": r.get("weightedAverageRate")})

    sl_stats = {}
    if sl_series:
        vals = [v for _, v in sl_series]; latest = vals[-1]
        sl_stats = {"latest_bn": latest, "as_of": sl_series[-1][0],
                    "avg_bn": round(sum(vals) / len(vals), 2),
                    "max_bn": round(max(vals), 2),
                    "pctile": round(sum(1 for v in vals if v <= latest) / len(vals) * 100, 1)}

    # ---- AMBS operations ----
    amb = _get(NYFED + "/ambs/all/results/details/last/40.json") or {}
    aops = (amb.get("ambs") or {}).get("auctions", [])
    ambs_recent = []
    amb_by_month = {}
    for a in aops:
        d = a.get("operationDate"); par = _bn(a.get("totalAmtAcceptedPar") or a.get("totalAcceptedCurrFace"))
        ambs_recent.append({"date": d, "type": a.get("operationType"),
                            "direction": a.get("operationDirection"), "accepted_par_bn": par})
        if d and par:
            amb_by_month[d[:7]] = round(amb_by_month.get(d[:7], 0) + par, 3)
    ambs_series = sorted(([d, v] for d, v in amb_by_month.items()), key=lambda x: x[0])

    # ---- read ----
    drivers = []
    if sl_stats:
        lvl = ("elevated" if sl_stats["pctile"] >= 75 else "subdued" if sl_stats["pctile"] <= 25 else "normal")
        drivers.append("SOMA securities-lending take-up $%.1fbn (%.0f%%ile, %s) — collateral-demand gauge"
                       % (sl_stats["latest_bn"], sl_stats["pctile"], lvl))
    if top_specials:
        drivers.append("%d issues on special (largest: %s, $%.1fbn on loan)"
                       % (len(top_specials), top_specials[0]["security"], top_specials[0]["outstanding_loans_bn"]))
    if ambs_series:
        drivers.append("AMBS ops last month $%.1fbn accepted par (QT-era reinvestment)" % ambs_series[-1][1])

    out = {
        "engine": "fed-collateral", "version": "1.0.0", "generated_at": now,
        "reads": drivers or ["No collateral-operations signals."],
        "securities_lending": {"daily_total_bn": sl_series, "stats": sl_stats,
                               "top_specials": top_specials,
                               "source": "NY Fed — SOMA Securities Lending"},
        "ambs": {"monthly_accepted_bn": ambs_series, "recent_ops": ambs_recent[:20],
                 "source": "NY Fed — Agency MBS operations"},
    }
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, separators=(",", ":")).encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")
    return {"statusCode": 200, "body": json.dumps({
        "seclending_days": len(sl_series), "seclending_latest_bn": sl_stats.get("latest_bn"),
        "specials": len(top_specials), "ambs_months": len(ambs_series)})}
