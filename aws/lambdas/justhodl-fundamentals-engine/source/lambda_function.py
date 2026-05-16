"""
justhodl-fundamentals-engine — FMP Fundamentals X-Ray

═══════════════════════════════════════════════════════════════════════
WHY THIS EXISTS
───────────────
The entitlement probe (ops/735) confirmed the FMP plan unlocks a deep
fundamentals layer the platform was not using. This engine adds three
institutional reads that no existing Lambda produces:

  1. Revenue segmentation — PRODUCT and GEOGRAPHIC. Sell-side analysts
     live in segment data; it is genuine fundamental edge and surfaces
     each name's regional exposure (relevant to macro / Gulf themes).
  2. DCF intrinsic value vs price — over/under-valuation gap.
  3. Financial health — Altman-Z (distress) + Piotroski (quality).

Price-target consensus is deliberately NOT duplicated here — that is
already covered by justhodl-analyst-consensus.

UNIVERSE: the master-ranker top-tickers list (the names that matter
today); falls back to a mega-cap core list if that sidecar is absent.

OUTPUT: data/fundamentals.json     SCHEDULE: daily 13:00 UTC
═══════════════════════════════════════════════════════════════════════
"""
import json
import os
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

import boto3

S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = "data/fundamentals.json"
RANKER_KEY = "data/master-ranker.json"
FMP_KEY = os.environ.get("FMP_KEY", "")
MAX_UNIVERSE = 30
FALLBACK_UNIVERSE = ["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "AVGO",
                     "LLY", "JPM", "XOM", "UNH", "V", "COST", "HD", "PG"]

s3 = boto3.client("s3", region_name="us-east-1")


def fmp_get(path, params=None, retries=2, timeout=20):
    """GET /stable/{path} → parsed JSON, or None on failure."""
    if not FMP_KEY:
        return None
    p = {**(params or {}), "apikey": FMP_KEY}
    qs = "&".join(f"{k}={v}" for k, v in p.items())
    url = f"https://financialmodelingprep.com/stable/{path}?{qs}"
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code in (429, 503) and attempt < retries:
                time.sleep(2 ** attempt)
                continue
            return None
        except Exception:
            if attempt < retries:
                time.sleep(1)
                continue
            return None
    return None


def _first(x):
    """First element of a list payload, or the dict itself, or {}."""
    if isinstance(x, list):
        return x[0] if x else {}
    if isinstance(x, dict):
        return x
    return {}


def _mix(record):
    """Turn an FMP segmentation record into {label: pct} + metadata."""
    rec = _first(record)
    data = rec.get("data") if isinstance(rec.get("data"), dict) else None
    if data is None:
        # some payloads put the segment map at the top level
        data = {k: v for k, v in rec.items()
                if isinstance(v, (int, float)) and k not in (
                    "fiscalYear", "calendarYear", "year")}
    nums = {k: float(v) for k, v in (data or {}).items()
            if isinstance(v, (int, float)) and v}
    total = sum(abs(v) for v in nums.values())
    if not total:
        return {}, None, None, 0, None
    mix = {k: round(v / total * 100, 1) for k, v in nums.items()}
    top = max(mix.items(), key=lambda kv: kv[1])
    return (mix, top[0], top[1], len(mix),
            rec.get("fiscalYear") or rec.get("date"))


def analyse(ticker):
    """Pull the three FMP reads for one ticker and fuse them."""
    try:
        dcf = _first(fmp_get("discounted-cash-flow", {"symbol": ticker}))
        scores = _first(fmp_get("financial-scores", {"symbol": ticker}))
        geo = fmp_get("revenue-geographic-segmentation", {"symbol": ticker})
        prod = fmp_get("revenue-product-segmentation", {"symbol": ticker})

        price = (dcf.get("price") or dcf.get("Stock Price")
                 or dcf.get("stockPrice"))
        dcf_val = dcf.get("dcf") or dcf.get("dcfValue")
        gap = None
        if price and dcf_val:
            gap = round((float(dcf_val) - float(price)) / float(price) * 100, 1)
        val_label = ("UNDERVALUED" if gap is not None and gap >= 20 else
                     "OVERVALUED" if gap is not None and gap <= -20 else
                     "FAIR" if gap is not None else None)

        az = scores.get("altmanZScore")
        pio = scores.get("piotroskiScore")
        az = round(float(az), 2) if az is not None else None
        pio = int(pio) if pio is not None else None
        if az is not None and az < 1.8:
            q_label = "DISTRESS RISK"
        elif az is not None and az >= 3 and (pio or 0) >= 7:
            q_label = "STRONG"
        elif az is not None:
            q_label = "OK"
        else:
            q_label = None

        geo_mix, top_region, top_region_pct, n_regions, fy = _mix(geo)
        prod_mix, top_product, top_product_pct, n_products, _ = _mix(prod)

        return {
            "ticker": ticker,
            "price": round(float(price), 2) if price else None,
            "dcf": round(float(dcf_val), 2) if dcf_val else None,
            "dcf_gap_pct": gap,
            "valuation_label": val_label,
            "altman_z": az,
            "piotroski": pio,
            "quality_label": q_label,
            "top_region": top_region,
            "top_region_pct": top_region_pct,
            "n_regions": n_regions,
            "geo_mix": geo_mix,
            "top_product": top_product,
            "top_product_pct": top_product_pct,
            "n_products": n_products,
            "product_mix": prod_mix,
            "fiscal_year": fy,
            "ok": True,
        }
    except Exception as e:
        return {"ticker": ticker, "ok": False, "error": str(e)[:160]}


def get_universe():
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=RANKER_KEY)
        ranker = json.loads(obj["Body"].read())
        tickers = [t.get("ticker") for t in (ranker.get("top_tickers") or [])
                   if t.get("ticker")]
        tickers = [t for t in dict.fromkeys(tickers)]  # dedupe, keep order
        if tickers:
            return tickers[:MAX_UNIVERSE], "master-ranker top tickers"
    except Exception:
        pass
    return FALLBACK_UNIVERSE, "mega-cap fallback list"


def lambda_handler(event, context):
    t0 = time.time()
    if not FMP_KEY:
        return {"statusCode": 500,
                "body": json.dumps({"ok": False, "err": "FMP_KEY missing"})}

    universe, src = get_universe()
    with ThreadPoolExecutor(max_workers=6) as ex:
        rows = list(ex.map(analyse, universe))

    companies = [r for r in rows if r.get("ok")]
    failed = [r["ticker"] for r in rows if not r.get("ok")]

    gaps = [c["dcf_gap_pct"] for c in companies if c["dcf_gap_pct"] is not None]
    summary = {
        "n_undervalued": sum(1 for c in companies
                             if c["valuation_label"] == "UNDERVALUED"),
        "n_overvalued": sum(1 for c in companies
                            if c["valuation_label"] == "OVERVALUED"),
        "n_distress_risk": sum(1 for c in companies
                               if c["quality_label"] == "DISTRESS RISK"),
        "n_strong_quality": sum(1 for c in companies
                                if c["quality_label"] == "STRONG"),
        "avg_dcf_gap_pct": round(sum(gaps) / len(gaps), 1) if gaps else None,
    }

    # rank: most undervalued + highest quality first
    companies.sort(key=lambda c: (c["dcf_gap_pct"]
                                  if c["dcf_gap_pct"] is not None else -999),
                   reverse=True)

    out = {
        "schema_version": "1.0",
        "method": "fmp_fundamentals_xray",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_s": round(time.time() - t0, 1),
        "universe_source": src,
        "n_covered": len(companies),
        "n_failed": len(failed),
        "failed_tickers": failed,
        "summary": summary,
        "companies": companies,
        "note": ("DCF intrinsic value, revenue segmentation (product + "
                 "geographic) and financial-health scores (Altman-Z, "
                 "Piotroski) from FMP. A research view, not advice — DCF is "
                 "model-dependent; segment mixes are last reported fiscal year."),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                  Body=json.dumps(out, default=str).encode("utf-8"),
                  ContentType="application/json",
                  CacheControl="public, max-age=3600")
    print(f"[fundamentals] {len(companies)} covered, {len(failed)} failed, "
          f"{out['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "n_covered": len(companies), "n_failed": len(failed),
        "summary": summary})}
