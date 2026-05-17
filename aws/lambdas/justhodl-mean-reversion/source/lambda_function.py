"""
justhodl-mean-reversion — Historical-Multiple Mean-Reversion Price

═══════════════════════════════════════════════════════════════════════
WHY THIS EXISTS
───────────────
The screener already carries a DCF intrinsic value (cash-flow based).
This adds the SECOND fair-value anchor it lacked: a statistical
mean-reversion price — where a stock would trade if its valuation
multiple snapped back to its OWN historical norm.

  median_PE      = median of the name's last ~6 annual P/E ratios (FMP)
  mr_price       = current_price * (median_PE / current_PE)
  mr_upside_pct  = (median_PE / current_PE - 1) * 100

Reverting to a stock's own median multiple — not a market average —
correctly handles structurally high-multiple names (a 40x compounder
reverts toward 40x, not toward the market's 18x).

Reads the screener universe from screener/data.json; writes a slim,
symbol-keyed sidecar the screener page merges client-side. The
protected screener Lambda is NOT modified.

OUTPUT: screener/mean-reversion.json   SCHEDULE: daily 13:30 UTC
═══════════════════════════════════════════════════════════════════════
"""
import json
import os
import time
import statistics
import urllib.request
import urllib.error
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

import boto3

S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
SCREENER_KEY = "screener/data.json"
OUT_KEY = "screener/mean-reversion.json"
FMP_KEY = os.environ.get("FMP_KEY", "")
MIN_YEARS = 3            # need at least 3 positive annual P/Es to trust a median
RICH_CHEAP_BAND = 15.0   # % gap that flags RICH / CHEAP vs own history

s3 = boto3.client("s3", region_name="us-east-1")


def fmp_ratios(symbol):
    """Annual ratios for one symbol → list of historical P/E values."""
    if not FMP_KEY:
        return []
    url = ("https://financialmodelingprep.com/stable/ratios"
           f"?symbol={symbol}&period=annual&limit=6&apikey={FMP_KEY}")
    for attempt in range(2):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/1.0"})
            with urllib.request.urlopen(req, timeout=18) as r:
                rows = json.loads(r.read())
            pes = []
            for row in (rows or []):
                pe = (row.get("priceToEarningsRatio")
                      or row.get("peRatio")
                      or row.get("priceEarningsRatio"))
                try:
                    pe = float(pe)
                except (TypeError, ValueError):
                    continue
                if 0 < pe < 500:        # drop negative + absurd outliers
                    pes.append(pe)
            return pes
        except urllib.error.HTTPError as e:
            if e.code in (429, 503) and attempt < 1:
                time.sleep(2)
                continue
            return []
        except Exception:
            if attempt < 1:
                time.sleep(1)
                continue
            return []
    return []


def analyse(stock):
    sym = stock.get("symbol")
    price = stock.get("price")
    cur_pe = stock.get("peRatio")
    try:
        price = float(price)
        cur_pe = float(cur_pe)
    except (TypeError, ValueError):
        return {"symbol": sym, "mr_price": None, "mr_upside_pct": None,
                "label": "n/a — no current P/E"}
    if cur_pe <= 0 or price <= 0:
        return {"symbol": sym, "mr_price": None, "mr_upside_pct": None,
                "label": "n/a — negative / no earnings"}

    pes = fmp_ratios(sym)
    if len(pes) < MIN_YEARS:
        return {"symbol": sym, "mr_price": None, "mr_upside_pct": None,
                "label": "n/a — insufficient history"}

    median_pe = statistics.median(pes)
    mr_price = round(price * (median_pe / cur_pe), 2)
    mr_upside = round((median_pe / cur_pe - 1) * 100, 1)
    if mr_upside >= RICH_CHEAP_BAND:
        label = "CHEAP vs own history"
    elif mr_upside <= -RICH_CHEAP_BAND:
        label = "RICH vs own history"
    else:
        label = "IN LINE"
    return {
        "symbol": sym,
        "mr_price": mr_price,
        "mr_upside_pct": mr_upside,
        "median_pe": round(median_pe, 1),
        "current_pe": round(cur_pe, 1),
        "n_years": len(pes),
        "label": label,
    }


def lambda_handler(event, context):
    t0 = time.time()
    if not FMP_KEY:
        return {"statusCode": 500,
                "body": json.dumps({"ok": False, "err": "FMP_KEY missing"})}

    try:
        screener = json.loads(
            s3.get_object(Bucket=S3_BUCKET, Key=SCREENER_KEY)["Body"].read())
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps(
            {"ok": False, "err": f"cannot read screener data: {str(e)[:160]}"})}

    universe = [s for s in screener.get("stocks", []) if s.get("symbol")]
    with ThreadPoolExecutor(max_workers=8) as ex:
        rows = list(ex.map(analyse, universe))

    priced = [r for r in rows if r.get("mr_price") is not None]
    cheap = sum(1 for r in priced if r["label"] == "CHEAP vs own history")
    rich = sum(1 for r in priced if r["label"] == "RICH vs own history")

    out = {
        "schema_version": "1.0",
        "method": "historical_pe_mean_reversion",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_seconds": round(time.time() - t0, 1),
        "count": len(universe),
        "n_priced": len(priced),
        "n_cheap_vs_history": cheap,
        "n_rich_vs_history": rich,
        "stocks": rows,
        "note": ("Mean-reversion price = current price scaled by the ratio of "
                 "the name's median 6-year P/E to its current P/E. A "
                 "statistical fair value vs the stock's own valuation history "
                 "— a complement to DCF, not advice."),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, separators=(",", ":")).encode("utf-8"),
                  ContentType="application/json",
                  CacheControl="public, max-age=14400")
    print(f"[mean-reversion] {len(priced)}/{len(universe)} priced — "
          f"{cheap} cheap, {rich} rich vs own history, "
          f"{out['elapsed_seconds']}s")
    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "n_priced": len(priced),
        "n_cheap_vs_history": cheap, "n_rich_vs_history": rich})}
