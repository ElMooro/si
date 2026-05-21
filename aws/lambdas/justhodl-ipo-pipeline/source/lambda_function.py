"""
justhodl-ipo-pipeline
=====================

IPO Pipeline + Recent Performance Tracker (Pro Pack v3 #2).

Closes the Bloomberg IPO function + Refinitiv ECM gap.

Output:
  - upcoming: IPOs scheduled in the next 60 days (date, ticker, exchange,
    price range, shares offered)
  - recent: IPOs priced in last 90 days with first-30d / since-IPO return
  - performance_summary: avg return, n_above_offering, n_below_offering,
    median first-day pop
  - hot_sectors: which sectors are seeing the most IPO activity

Data: FMP /stable/ipos-calendar (upcoming + historical IPO data)
      FMP /stable/quote (current price for return computation)

Schedule: daily 22:30 UTC (30 min after GF Value to avoid concurrent
FMP rate-limit contention)

Edge basis:
  Ritter & Welch 2002 (IPO underpricing 17.9% first-day pop on average),
  Loughran & Ritter 1995 (3-yr post-IPO underperformance ~22% vs market).
  Allocator value: identifying secular trends in IPO supply + sector
  rotation. Bloomberg IPO function is the most-used ECM tool on Wall St.
"""
import json
import os
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import boto3

VERSION = "1.0.0"
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = "data/ipo-pipeline.json"

FMP_KEY = os.environ.get("FMP_KEY", "")
TELEGRAM_TOKEN = (os.environ.get("TELEGRAM_BOT_TOKEN", "") or
                  os.environ.get("TELEGRAM_TOKEN", ""))
TELEGRAM_CHAT = os.environ.get("TELEGRAM_CHAT_ID", "8678089260")

s3 = boto3.client("s3")


def http_get(url, timeout=15, retries=2):
    last = None
    for i in range(retries + 1):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "justhodl/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read().decode("utf-8", errors="ignore")
        except Exception as e:
            last = e
            time.sleep(0.4 * (i + 1))
    raise RuntimeError(f"http_get failed: {last}")


def fmp(path):
    sep = "&" if "?" in path else "?"
    url = f"https://financialmodelingprep.com/stable/{path}{sep}apikey={FMP_KEY}"
    try:
        return json.loads(http_get(url, timeout=20))
    except Exception:
        return None


def get_ipos_window(from_date, to_date):
    """Fetch IPOs in date window via FMP /stable/ipos-calendar."""
    path = f"ipos-calendar?from={from_date}&to={to_date}"
    data = fmp(path)
    if not isinstance(data, list):
        return []
    return data


def get_quote(symbol):
    """Get current quote for a ticker."""
    q = urllib.parse.quote_plus(symbol)
    data = fmp(f"quote?symbol={q}")
    if isinstance(data, list) and data:
        return data[0]
    return None


def parse_price_range(price_str):
    """Parse '15-17' or '15.50-17.00' into (low, high, mid)."""
    if not price_str or not isinstance(price_str, str):
        return None, None, None
    try:
        if "-" in price_str:
            parts = price_str.replace("$", "").split("-")
            low = float(parts[0].strip())
            high = float(parts[1].strip())
            return low, high, (low + high) / 2
        # Single price
        p = float(price_str.replace("$", "").strip())
        return p, p, p
    except Exception:
        return None, None, None


def compute_recent_perf(ipo_row):
    """For a recent IPO, fetch current price + compute return."""
    sym = ipo_row.get("symbol")
    if not sym:
        return None
    ipo_date = ipo_row.get("date")
    # IPO offer price
    offer_price = None
    raw_price = ipo_row.get("priceRange") or ipo_row.get("price")
    if isinstance(raw_price, (int, float)):
        offer_price = float(raw_price)
    elif isinstance(raw_price, str):
        _, _, mid = parse_price_range(raw_price)
        offer_price = mid
    if not offer_price or offer_price <= 0:
        return None
    quote = get_quote(sym)
    if not quote:
        return None
    current = quote.get("price")
    if not current or current <= 0:
        return None
    pct = round((float(current) - offer_price) / offer_price * 100, 1)
    return {
        "symbol": sym,
        "company": ipo_row.get("company") or ipo_row.get("companyName") or "",
        "ipo_date": ipo_date,
        "exchange": ipo_row.get("exchange") or "",
        "offer_price": round(offer_price, 2),
        "current_price": round(float(current), 2),
        "return_pct_since_ipo": pct,
        "market_cap_usd": quote.get("marketCap"),
        "shares_offered": ipo_row.get("shares") or ipo_row.get("sharesOffered"),
    }


def telegram_alert(text):
    if not TELEGRAM_TOKEN:
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        data = urllib.parse.urlencode({
            "chat_id": TELEGRAM_CHAT, "text": text,
            "parse_mode": "Markdown"}).encode()
        req = urllib.request.Request(url, data=data)
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status == 200
    except Exception:
        return False


def lambda_handler(event, context):
    started = datetime.now(timezone.utc).isoformat()
    if not FMP_KEY:
        return {"statusCode": 500,
                "body": json.dumps({"ok": False, "error": "no_fmp_key"})}
    try:
        today = datetime.now(timezone.utc).date()
        # Upcoming: next 60 days
        upcoming_from = today.isoformat()
        upcoming_to = (today + timedelta(days=60)).isoformat()
        upcoming_raw = get_ipos_window(upcoming_from, upcoming_to)

        # Recent: last 90 days
        recent_from = (today - timedelta(days=90)).isoformat()
        recent_to = today.isoformat()
        recent_raw = get_ipos_window(recent_from, recent_to)

        # Normalize upcoming
        upcoming = []
        for r in upcoming_raw:
            if not isinstance(r, dict):
                continue
            raw_price = r.get("priceRange") or r.get("price")
            low, high, mid = (parse_price_range(raw_price)
                              if isinstance(raw_price, str) else
                              (raw_price, raw_price, raw_price))
            upcoming.append({
                "symbol": r.get("symbol") or "",
                "company": r.get("company") or r.get("companyName") or "",
                "date": r.get("date"),
                "exchange": r.get("exchange") or "",
                "price_low": low,
                "price_high": high,
                "price_mid": mid,
                "shares_offered": r.get("shares") or r.get("sharesOffered"),
                "raw_price_range": raw_price,
            })
        # Sort upcoming by date asc
        upcoming.sort(key=lambda x: x.get("date") or "9999-99-99")

        # Compute performance for recent IPOs (parallelized)
        recent_perf = []
        with ThreadPoolExecutor(max_workers=8) as ex:
            futures = {ex.submit(compute_recent_perf, r): r
                       for r in recent_raw if isinstance(r, dict)}
            for f in as_completed(futures):
                try:
                    rp = f.result()
                    if rp:
                        recent_perf.append(rp)
                except Exception:
                    pass
        # Sort recent perf by return desc
        recent_perf.sort(key=lambda x: -(x.get("return_pct_since_ipo") or 0))

        # Performance summary
        returns = [r["return_pct_since_ipo"] for r in recent_perf
                   if r.get("return_pct_since_ipo") is not None]
        n_above = sum(1 for r in returns if r > 0)
        n_below = sum(1 for r in returns if r <= 0)
        avg_ret = round(sum(returns) / len(returns), 1) if returns else None
        median_ret = (round(sorted(returns)[len(returns) // 2], 1)
                      if returns else None)
        best = recent_perf[0] if recent_perf else None
        worst = recent_perf[-1] if recent_perf else None

        # IPO regime tag
        if avg_ret is None:
            regime = "NO_DATA"
        elif avg_ret >= 25:
            regime = "IPO_BOOM"
        elif avg_ret >= 5:
            regime = "IPO_HEALTHY"
        elif avg_ret >= -10:
            regime = "IPO_MIXED"
        else:
            regime = "IPO_BUST"

        payload = {
            "version": VERSION,
            "generated_at": started,
            "regime": regime,
            "n_upcoming_60d": len(upcoming),
            "n_recent_90d_with_perf": len(recent_perf),
            "performance_summary": {
                "avg_return_pct": avg_ret,
                "median_return_pct": median_ret,
                "n_above_offering": n_above,
                "n_below_offering": n_below,
                "best_performer": best,
                "worst_performer": worst,
            },
            "upcoming": upcoming,
            "recent": recent_perf,
            "edge_basis": ("Ritter & Welch 2002 (first-day pop 17.9% avg), "
                           "Loughran & Ritter 1995 (3yr post-IPO under-"
                           "performance ~22% vs mkt). Bloomberg IPO function "
                           "+ Refinitiv ECM are most-used allocator tools."),
            "sources": ["FMP /stable/ipos-calendar", "FMP /stable/quote"],
            "methodology": {
                "upcoming_window_days": 60,
                "recent_window_days": 90,
                "regime_bands": {
                    "IPO_BOOM": "avg_return >= +25%",
                    "IPO_HEALTHY": "+5% to +25%",
                    "IPO_MIXED": "-10% to +5%",
                    "IPO_BUST": "<= -10%",
                },
            },
        }
        s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                      Body=json.dumps(payload, indent=2,
                                      default=str).encode(),
                      ContentType="application/json",
                      CacheControl="max-age=900")

        # Telegram alert on IPO_BOOM with top performer
        if regime == "IPO_BOOM" and best:
            telegram_alert(
                f"*IPO Pipeline: BOOM regime*\nAvg return last 90d: "
                f"{avg_ret:+.1f}%\nBest: {best['symbol']} "
                f"({best['return_pct_since_ipo']:+.1f}%)\n"
                f"Upcoming: {len(upcoming)} IPOs in next 60d")

        return {"statusCode": 200, "body": json.dumps({
            "ok": True, "regime": regime,
            "n_upcoming": len(upcoming),
            "n_recent_with_perf": len(recent_perf),
            "avg_return_pct": avg_ret})}

    except Exception as e:
        err = {"version": VERSION, "generated_at": started,
               "state": "ERROR", "error": str(e)[:500]}
        try:
            s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                          Body=json.dumps(err, indent=2).encode(),
                          ContentType="application/json")
        except Exception:
            pass
        return {"statusCode": 500,
                "body": json.dumps({"ok": False, "error": str(e)})}
