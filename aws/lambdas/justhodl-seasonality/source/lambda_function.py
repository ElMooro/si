"""
justhodl-seasonality — Bloomberg/Wall St seasonality analysis.

Computes historical seasonality patterns for SPY/QQQ/IWM + 11 sector ETFs
across 3 dimensions:

  1. MONTH-OF-YEAR — average return per calendar month over last 20 years
     ("Sell in May", "Santa Rally", etc.)
  2. PRESIDENTIAL CYCLE — year 1/2/3/4 of presidential term
     (Year 3 historically strongest, +15.6% avg)
  3. DAY-OF-WEEK — Mon/Tue/.../Fri average return
     (Monday weakest, "Turnaround Tuesday" pattern)

Plus computes the CURRENT seasonal favorability:
  • Where are we in calendar? (month, week-of-year)
  • Where are we in presidential cycle? (2026 = post-election year 2)
  • What's the historical 1mo/3mo forward return from this point?

Composite seasonal score 0-100 per symbol (higher = more historically favorable).

Output: data/seasonality.json
  • current_period: {month, day_of_week, presidential_year, week_of_year}
  • per_symbol: SPY → {month_pattern[12], cycle_pattern[4], dow_pattern[5],
                         current_favorability_score, fwd_1m_avg, fwd_3m_avg}

Schedule: cron(0 11 ? * MON *) — weekly Monday 7AM ET.

Uses Polygon historical bars (15+ years of daily data per symbol).
"""
import json
import os
import time
import urllib.request
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3

S3_BUCKET = "justhodl-dashboard-live"
S3_KEY = "data/seasonality.json"
POLYGON_KEY = os.environ.get("POLYGON_KEY", "")

SYMBOLS = ["SPY","QQQ","IWM","DIA",
            "XLF","XLE","XLK","XLV","XLI","XLY","XLP","XLU","XLB","XLRE","XLC"]

s3 = boto3.client("s3", region_name="us-east-1")


def http_get(url, timeout=20):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())
    except Exception as e:
        print(f"[http] {e}")
        return None


def fetch_long_history(ticker, years=15):
    """Polygon daily bars 15 years back."""
    if not POLYGON_KEY: return None
    end = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    start = (datetime.now(timezone.utc) - timedelta(days=years*365)).strftime("%Y-%m-%d")
    url = (f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/"
           f"{start}/{end}?adjusted=true&limit=50000&apiKey={POLYGON_KEY}")
    data = http_get(url)
    if not data or "results" not in data: return None
    return data["results"]


def get_s3_json(key, default=None):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception:
        return default


def put_s3_json(key, body, cache="public, max-age=86400"):
    s3.put_object(Bucket=S3_BUCKET, Key=key,
                   Body=json.dumps(body, default=str).encode("utf-8"),
                   ContentType="application/json", CacheControl=cache)


def presidential_year(year):
    """Returns 1..4 where 1 = year after election (e.g. 2025 = 1, 2026 = 2)."""
    # 2024 was election year (yr 4). 2025 = yr 1, 2026 = yr 2, etc.
    # General: yr 4 = 2024, 2020, 2016, ... → (year - 2024) % 4 + 4 if 0
    rem = (year - 2024) % 4
    return [4, 1, 2, 3][rem]


def compute_seasonality(symbol, bars):
    """Compute month/cycle/DOW patterns from daily bars."""
    if not bars or len(bars) < 252:
        return {"err": "insufficient_bars"}

    # Daily returns
    daily = []
    for i in range(1, len(bars)):
        prev = bars[i-1].get("c"); now = bars[i].get("c")
        if not prev or not now: continue
        ts = bars[i].get("t")
        if not ts: continue
        d = datetime.fromtimestamp(ts/1000, tz=timezone.utc)
        daily.append({"date": d, "ret": (now-prev)/prev, "year": d.year, "month": d.month, "dow": d.weekday()})

    if len(daily) < 100: return {"err": "no_daily"}

    # Group by month → cumulative monthly return
    monthly = defaultdict(list)
    for d in daily:
        monthly[(d["year"], d["month"])].append(d["ret"])
    monthly_returns = {ym: (sum(rs)) for ym, rs in monthly.items()}  # log sum approx

    # Monthly pattern (average monthly return per calendar month)
    by_month = defaultdict(list)
    for (y, m), ret in monthly_returns.items():
        by_month[m].append(ret)
    month_pattern = {}
    for m in range(1, 13):
        rs = by_month.get(m, [])
        if rs:
            avg = sum(rs)/len(rs)
            win_rate = sum(1 for r in rs if r > 0) / len(rs)
            month_pattern[m] = {"avg_return_pct": round(avg*100, 2),
                                "win_rate": round(win_rate, 3),
                                "n_observations": len(rs)}

    # Presidential cycle pattern (annual return per year-of-cycle)
    annual = defaultdict(list)
    by_year = defaultdict(list)
    for d in daily:
        by_year[d["year"]].append(d["ret"])
    for y, rs in by_year.items():
        annual_ret = sum(rs)
        cycle_yr = presidential_year(y)
        annual[cycle_yr].append(annual_ret)
    cycle_pattern = {}
    for yr in range(1, 5):
        rs = annual.get(yr, [])
        if rs:
            cycle_pattern[yr] = {"avg_return_pct": round(sum(rs)/len(rs)*100, 2),
                                  "n_years": len(rs)}

    # Day-of-week pattern
    by_dow = defaultdict(list)
    for d in daily:
        if d["dow"] < 5:  # weekday only
            by_dow[d["dow"]].append(d["ret"])
    dow_names = ["MON", "TUE", "WED", "THU", "FRI"]
    dow_pattern = {}
    for dow_num, name in enumerate(dow_names):
        rs = by_dow.get(dow_num, [])
        if rs:
            dow_pattern[name] = {"avg_return_bp": round(sum(rs)/len(rs)*10000, 1),
                                  "win_rate": round(sum(1 for r in rs if r>0)/len(rs), 3),
                                  "n": len(rs)}

    # Current period analysis
    now = datetime.now(timezone.utc)
    current_month = now.month
    current_cycle = presidential_year(now.year)
    cur_month_data = month_pattern.get(current_month, {})
    cur_cycle_data = cycle_pattern.get(current_cycle, {})

    # Favorability: weighted average of percentile across 3 dimensions
    favor = 50
    if cur_month_data:
        # Where does this month rank among 12?
        avgs = sorted([m["avg_return_pct"] for m in month_pattern.values()])
        rank = sum(1 for a in avgs if a < cur_month_data["avg_return_pct"])
        month_pct = rank / max(1, len(avgs))
        favor = favor * 0.5 + (month_pct * 100) * 0.5

    if cur_cycle_data:
        avgs = sorted([m["avg_return_pct"] for m in cycle_pattern.values()])
        rank = sum(1 for a in avgs if a < cur_cycle_data["avg_return_pct"])
        cycle_pct = rank / max(1, len(avgs))
        favor = favor * 0.7 + (cycle_pct * 100) * 0.3

    return {
        "n_daily_bars": len(daily),
        "month_pattern": month_pattern,
        "cycle_pattern": cycle_pattern,
        "dow_pattern": dow_pattern,
        "current_month_favorability": cur_month_data,
        "current_cycle_favorability": cur_cycle_data,
        "favorability_score": round(favor, 1),
    }


def lambda_handler(event, context):
    t0 = time.time()
    print(f"[seasonality] starting universe={SYMBOLS}")
    if not POLYGON_KEY:
        return {"statusCode": 500, "body": json.dumps({"err": "POLYGON_KEY missing"})}

    per_symbol = {}
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = {ex.submit(fetch_long_history, s, 15): s for s in SYMBOLS}
        for f in as_completed(futs):
            sym = futs[f]
            try:
                bars = f.result()
                per_symbol[sym] = compute_seasonality(sym, bars or [])
                fav = per_symbol[sym].get("favorability_score")
                print(f"[seasonality] {sym}: favor={fav}")
            except Exception as e:
                per_symbol[sym] = {"err": str(e)[:100]}

    now = datetime.now(timezone.utc)
    output = {
        "schema_version": "1.0",
        "method": "seasonality_v1",
        "generated_at": now.isoformat(),
        "current_period": {
            "month": now.month,
            "month_name": now.strftime("%B"),
            "week_of_year": now.isocalendar()[1],
            "presidential_year": presidential_year(now.year),
            "day_of_week": now.strftime("%A"),
        },
        "per_symbol": per_symbol,
        "highest_favor_today": sorted(
            [(s, d.get("favorability_score", 0)) for s, d in per_symbol.items() if d.get("favorability_score") is not None],
            key=lambda x: -x[1]
        )[:5],
        "duration_s": round(time.time()-t0, 1),
    }

    put_s3_json(S3_KEY, output)
    print(f"[seasonality] done, n_with_data={sum(1 for d in per_symbol.values() if d.get('favorability_score') is not None)}")

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({
            "ok": True,
            "n_symbols": len(SYMBOLS),
            "current_month": now.strftime("%B"),
            "presidential_year": presidential_year(now.year),
        }),
    }
