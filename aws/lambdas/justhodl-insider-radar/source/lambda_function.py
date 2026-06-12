"""
justhodl-insider-radar v1.0 — Form-4 open-market buys, cluster + decline logic
==============================================================================
Finviz Module 4, desk-grade. Source ladder (every rung lands in DIAG):
  1. FMP /stable insider endpoints (may be plan-gated — captured honestly)
  2. existing desk SEC briefs on S3 (probed)
  3. honest 'unavailable' brief
Signals computed when data flows:
  • latest open-market BUYS (P-type), officer-rank weighted
  • CLUSTERS: ≥2 distinct insiders, same ticker, ≤14 days
  • BUYS-AFTER-DECLINE: ticker −25%+ over 60d (from upside rings) + insider buy
    — the highest-value pattern; cluster∩decline logs to the closed loop.
"""
import json, os, time, gzip, urllib.request
from datetime import datetime, timezone, timedelta
from decimal import Decimal
import boto3

S3 = boto3.client("s3", region_name="us-east-1")
DDB = boto3.resource("dynamodb", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/insider-radar.json"
UP_STATE = "data/_upside/state.json.gz"
FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
POLYGON_KEY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
VERSION = "1.0.1"
DIAG = []
RANK = (("CEO", 3.0), ("CHIEF EXECUTIVE", 3.0), ("CFO", 2.8), ("CHIEF FINANCIAL", 2.8),
         ("PRESIDENT", 2.4), ("COO", 2.2), ("CHAIR", 2.2), ("DIRECTOR", 1.8),
         ("10%", 1.5), ("OFFICER", 1.5))


def jget(url, timeout=35):
    req = urllib.request.Request(url, headers={"User-Agent": "JustHodl Research admin@justhodl.ai"})
    return json.loads(urllib.request.urlopen(req, timeout=timeout).read())


def f(x):
    try:
        return float(x)
    except Exception:
        return None


_PX60 = {}
def poly_ret60(t):
    if t in _PX60:
        return _PX60[t]
    from datetime import date
    end = datetime.now(timezone.utc).date().isoformat()
    start = (datetime.now(timezone.utc) - timedelta(days=95)).date().isoformat()
    val = None
    try:
        u = (f"https://api.polygon.io/v2/aggs/ticker/{t}/range/1/day/{start}/{end}"
             f"?adjusted=true&sort=asc&limit=200&apiKey={POLYGON_KEY}")
        rows = [float(r["c"]) for r in (jget(u, timeout=15).get("results") or [])]
        if len(rows) > 40:
            base = rows[-61] if len(rows) > 61 else rows[0]
            val = round((rows[-1] / base - 1) * 100, 1)
    except Exception as e:
        DIAG.append(f"poly_ret60 {t}: {str(e)[:40]}")
    _PX60[t] = val
    return val


def fetch_fmp():
    rows = []
    for label, url in (
        ("fmp/search", f"https://financialmodelingprep.com/stable/insider-trading/search?page=0&limit=1000&apikey={FMP_KEY}"),
        ("fmp/latest", f"https://financialmodelingprep.com/stable/insider-trading/latest?page=0&limit=1000&apikey={FMP_KEY}"),
    ):
        try:
            j = jget(url)
            if isinstance(j, list) and j:
                DIAG.append(f"{label}: {len(j)} rows")
                rows = j
                break
            DIAG.append(f"{label}: empty/shape {str(j)[:60]}")
        except Exception as e:
            DIAG.append(f"{label}: {str(e)[:70]}")
    return rows


def fetch_s3_fallback():
    try:
        r = S3.list_objects_v2(Bucket=BUCKET, Prefix="data/sec-filings")
        keys = [o["Key"] for o in r.get("Contents", [])]
        DIAG.append(f"s3 sec-filings keys: {keys[:4]}")
        for k in keys:
            try:
                j = json.loads(S3.get_object(Bucket=BUCKET, Key=k)["Body"].read())
                for field in ("insider_trades", "insider", "form4", "trades"):
                    arr = j.get(field) if isinstance(j, dict) else None
                    if isinstance(arr, list) and arr:
                        DIAG.append(f"s3 {k}.{field}: {len(arr)} rows")
                        return arr
            except Exception:
                continue
    except Exception as e:
        DIAG.append(f"s3 fallback: {str(e)[:60]}")
    return []


def normalize(raw):
    out = []
    for r in raw:
        tick = r.get("symbol") or r.get("ticker")
        typ = str(r.get("transactionType") or r.get("type") or "")
        d = str(r.get("transactionDate") or r.get("filingDate") or r.get("date") or "")[:10]
        if not tick or not d:
            continue
        sh = f(r.get("securitiesTransacted") or r.get("shares"))
        px = f(r.get("price") or r.get("transactionPrice"))
        val = f(r.get("value")) or ((sh or 0) * (px or 0) or None)
        out.append({"ticker": str(tick).upper(), "date": d,
                     "insider": (r.get("reportingName") or r.get("insiderName") or "?")[:40],
                     "title": (r.get("typeOfOwner") or r.get("title") or "")[:50],
                     "type": typ, "shares": sh, "price": px, "value": val})
    return out


def weight(title):
    t = (title or "").upper()
    for k, w in RANK:
        if k in t:
            return w
    return 1.0


def lambda_handler(event=None, context=None):
    t0 = time.time()
    DIAG.clear()
    raw = fetch_fmp()
    source = "fmp" if raw else None
    if not raw:
        raw = fetch_s3_fallback()
        source = "s3_sec_briefs" if raw else "unavailable"
    rows = normalize(raw)
    cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).date().isoformat()
    buys = [r for r in rows if r["date"] >= cutoff and r["type"].upper().startswith("P")]
    sells_n = sum(1 for r in rows if r["date"] >= cutoff and r["type"].upper().startswith("S"))
    for b in buys:
        b["w"] = weight(b["title"])
        b["score"] = round((b["w"] * ( (b["value"] or 0) ** 0.5 )) / 100, 1)
    buys.sort(key=lambda x: -(x["value"] or 0))

    # rings for decline join + baseline px
    rings = {}
    try:
        up = json.loads(gzip.decompress(S3.get_object(Bucket=BUCKET, Key=UP_STATE)["Body"].read()))
        rings = up.get("rings") or {}
        DIAG.append(f"rings: {len(rings)}")
    except Exception as e:
        DIAG.append(f"rings: {str(e)[:50]}")

    def ret60(t):
        r = rings.get(t)
        if r and len(r) > 61 and r[-61]:
            return round((r[-1] / r[-61] - 1) * 100, 1)
        return poly_ret60(t)

    # clusters: ≥2 distinct insiders / ticker / ≤14d span
    byt = {}
    for b in buys:
        byt.setdefault(b["ticker"], []).append(b)
    clusters = []
    for t, arr in byt.items():
        names = {a["insider"] for a in arr}
        if len(names) < 2:
            continue
        ds = sorted(a["date"] for a in arr)
        span = (datetime.strptime(ds[-1], "%Y-%m-%d") - datetime.strptime(ds[0], "%Y-%m-%d")).days
        if span > 14:
            continue
        tv = sum(a["value"] or 0 for a in arr)
        clusters.append({"ticker": t, "n_insiders": len(names), "n_buys": len(arr),
                          "total_value": round(tv), "span_days": span,
                          "first": ds[0], "last": ds[-1],
                          "max_rank_w": max(a["w"] for a in arr),
                          "ret_60d_pct": ret60(t),
                          "names": sorted(names)[:4]})
    clusters.sort(key=lambda c: -(c["total_value"] or 0))
    decline_buys = sorted([b | {"ret_60d_pct": ret60(b["ticker"])} for b in buys
                            if (ret60(b["ticker"]) or 0) <= -25],
                           key=lambda x: x["ret_60d_pct"])[:25]
    decline_clusters = [c for c in clusters if (c["ret_60d_pct"] or 0) <= -25]

    # closed-loop: log strongest cluster-after-decline
    logged = []
    try:
        if decline_clusters:
            c = decline_clusters[0]
            r = rings.get(c["ticker"]) or []
            if r:
                tbl = DDB.Table("justhodl-signals")
                today = datetime.now(timezone.utc).date().isoformat()
                sid = f"insider_cluster_decline#{c['ticker']}#{today}"
                conf = min(0.7, 0.55 + 0.05 * (c["n_insiders"] - 2))
                tbl.put_item(Item={
                    "signal_id": sid, "signal_type": "insider_cluster_decline",
                    "predicted_direction": "UP", "confidence": Decimal(str(round(conf, 2))),
                    "baseline_price": Decimal(str(round(r[-1], 4))),
                    "measure_against": "ticker", "ticker": c["ticker"], "benchmark": "SPY",
                    "horizon_days_primary": 63, "check_windows": [21, 63],
                    "status": "pending", "logged_epoch": int(time.time()),
                    "ttl": int(time.time()) + 150 * 86400,
                    "rationale": (f"{c['n_insiders']} insiders bought ${c['total_value']:,.0f} "
                                   f"within {c['span_days']}d after {c['ret_60d_pct']}% 60d decline"),
                }, ConditionExpression="attribute_not_exists(signal_id)")
                logged.append(sid)
    except Exception as e:
        if "ConditionalCheckFailed" not in str(e):
            DIAG.append(f"loop: {str(e)[:60]}")

    out = {"engine": "insider-radar", "version": VERSION,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "source_used": source, "n_raw": len(rows),
            "window_days": 30, "n_buys": len(buys), "n_sells": sells_n,
            "latest_buys": buys[:40],
            "clusters": clusters[:20],
            "decline_buys": decline_buys,
            "decline_clusters": decline_clusters[:10],
            "logged": logged, "diagnostics": list(DIAG),
            "methodology": ("Form-4 open-market purchases (P-type), 30d window. Clusters = "
                             "≥2 distinct insiders ≤14d. Decline join = ticker −25%+ over 60 "
                             "sessions from the desk's own close-rings (Polygon 60d fallback for micro-caps outside the rings). Officer rank weights "
                             "CEO 3.0 → director 1.8. Cluster∩decline logs to the graded "
                             "closed loop (insider_cluster_decline, 63d horizon). "
                             "Research, not advice.")}
    clean = json.loads(json.dumps(out, default=str), parse_constant=lambda c: None)
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(clean).encode(),
                  ContentType="application/json", CacheControl="public, max-age=1800")
    print(f"[insider] src={source} buys={len(buys)} clusters={len(clusters)} "
          f"decline_clusters={len(decline_clusters)} {round(time.time()-t0,1)}s")
    return {"statusCode": 200, "body": json.dumps({"src": source, "buys": len(buys),
                                                     "clusters": len(clusters)})}
