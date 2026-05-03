"""
justhodl-earnings-tracker — Earnings calendar + beat/miss reactivity + PEAD signals

Tracks upcoming earnings (next 14 days) and recent earnings results
(past 30 days) for the watchlist. Computes:
  - Upcoming earnings dates per ticker (from Nasdaq earnings calendar — free, public)
  - Beat/miss vs consensus where data available (Polygon financials for actuals)
  - Post-earnings drift (PEAD) — 1d, 5d, 20d returns
  - Aggregate metrics: beat rate, % positive reactions

Data sources:
  - Nasdaq earnings calendar API (free, no auth, daily by date)
  - Polygon stocks-financials API (for actuals when reported)
  - Polygon aggregates API (for post-earnings 1d/5d/20d returns)

Output: data/earnings-tracker.json
"""
import json
import os
import time
import boto3
import urllib.request
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
KEY = "data/earnings-tracker.json"

POLYGON_KEY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")

# Watchlist — 165 high-priority tickers
WATCHLIST = [
    # Mega caps
    "AAPL", "MSFT", "GOOGL", "GOOG", "AMZN", "NVDA", "META", "TSLA", "AVGO",
    "BRK-B", "TSM", "JPM", "WMT", "LLY", "V", "MA", "ORCL", "XOM", "UNH", "JNJ",
    "HD", "COST", "BAC", "PG", "ABBV", "NFLX", "CVX", "MRK", "KO", "AMD",
    "ADBE", "PEP", "CRM", "PM", "TMO", "LIN", "MCD", "ACN", "GE", "ABT",
    "CSCO", "WFC", "DHR", "AXP", "DIS", "VZ", "INTU", "MS", "T", "RTX",
    "AMGN", "GS", "IBM", "PFE", "QCOM", "BX", "ISRG", "TMUS", "CAT", "NOW",
    "AMAT", "BLK", "LOW", "ELV", "SCHW", "SPGI", "DE", "NKE", "C", "BKNG",
    "PLD", "SYK", "BSX", "PANW", "ETN", "MDT", "KKR", "ADP", "MMC", "REGN",
    "MU", "GILD", "VRTX", "FI", "LMT", "TJX", "INTC", "ADI", "CB", "AMT",
    "PYPL", "MO", "CI", "BA", "CME", "SHW", "ZTS", "EQIX", "HCA", "ICE",
    # High-velocity / high-reactivity
    "PLTR", "COIN", "MARA", "RIOT", "CLSK", "SOFI", "RBLX", "U", "NET",
    "SNOW", "DDOG", "CRWD", "ZS", "OKTA", "DOCU", "SHOP", "MELI", "PDD", "NU",
    "ABNB", "DASH", "RIVN", "LCID", "F", "GM", "STLA", "TM", "HMC", "UBER",
    "LYFT", "SQ", "AFRM", "HOOD", "RDDT", "DJT", "ASML", "WBA", "TGT", "DLTR",
    "CVS", "WBD", "PARA", "ROKU", "SPOT", "FDX", "UPS", "NOC", "GD", "HON",
    "EMR", "ITW", "SLB", "EOG", "OXY", "FANG", "MPC", "VLO", "PSX",
    # Liquid ETFs (no earnings but useful for reference returns)
    "SPY", "QQQ", "IWM", "GLD", "SLV", "USO", "TLT", "HYG", "LQD", "EEM", "EFA", "VWO",
]

WATCHLIST_SET = set(WATCHLIST)


# ────────────────────────── HTTP helpers ──────────────────────────
def http_get(url, timeout=15, ua="justhodl-earnings/1.0"):
    """Generic HTTP GET with browser UA (Nasdaq blocks default urllib UA)."""
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (compatible; " + ua + ")",
        "Accept": "application/json,text/plain,*/*",
    })
    with urllib.request.urlopen(req, timeout=timeout) as r:
        body = r.read()
        try:
            return json.loads(body)
        except Exception:
            return body.decode("utf-8", errors="replace")


# ────────────────────────── Nasdaq earnings calendar ──────────────────────────
def fetch_nasdaq_earnings_for_date(date_yyyy_mm_dd):
    """Pull Nasdaq earnings calendar for a single date. Returns list of rows."""
    url = f"https://api.nasdaq.com/api/calendar/earnings?date={date_yyyy_mm_dd}"
    try:
        d = http_get(url, timeout=20)
        if not isinstance(d, dict):
            return []
        rows = (d.get("data") or {}).get("rows") or []
        return rows
    except Exception:
        return []


def collect_upcoming_earnings(days_ahead=14):
    """Collect upcoming earnings for the next N weekdays."""
    upcoming = []
    today = datetime.now(timezone.utc).date()
    days_added = 0
    days_offset = 0
    while days_added < days_ahead and days_offset < days_ahead + 14:
        d = today + timedelta(days=days_offset)
        days_offset += 1
        # Skip weekends — markets closed, no earnings reports
        if d.weekday() >= 5:
            continue
        rows = fetch_nasdaq_earnings_for_date(d.isoformat())
        for r in rows:
            sym = (r.get("symbol") or "").strip().upper()
            if not sym or sym not in WATCHLIST_SET:
                continue
            time_str = r.get("time", "")
            if "pre-market" in time_str:
                tcode = "BMO"
            elif "after-hours" in time_str:
                tcode = "AMC"
            else:
                tcode = "TBD"
            eps_est = None
            eps_str = r.get("epsForecast", "")
            if eps_str:
                try:
                    s = eps_str.replace("$", "").replace(",", "").strip()
                    if s.startswith("(") and s.endswith(")"):
                        eps_est = -float(s[1:-1])
                    elif s and s != "N/A":
                        eps_est = float(s)
                except Exception:
                    pass
            upcoming.append({
                "ticker": sym,
                "name": r.get("name", ""),
                "earnings_date": d.isoformat(),
                "time": tcode,
                "eps_consensus": eps_est,
                "n_estimates": r.get("noOfEsts"),
                "fiscal_quarter_ending": r.get("fiscalQuarterEnding", ""),
                "last_year_eps": r.get("lastYearEPS", ""),
                "market_cap": r.get("marketCap", ""),
            })
        days_added += 1
        time.sleep(0.15)
    return upcoming


# ────────────────────────── Polygon financials (actual results) ──────────────────────────
def fetch_polygon_financials(ticker, limit=4):
    """Return last N quarterly filings with EPS + revenue."""
    url = f"https://api.polygon.io/vX/reference/financials?ticker={urllib.parse.quote(ticker)}&timeframe=quarterly&limit={limit}&apiKey={POLYGON_KEY}"
    try:
        d = http_get(url, timeout=15)
        results = []
        for r in (d.get("results") or []):
            fin = r.get("financials", {}) or {}
            ic = fin.get("income_statement", {}) or {}
            eps = (ic.get("basic_earnings_per_share") or {}).get("value")
            rev = (ic.get("revenues") or {}).get("value")
            results.append({
                "period_start": r.get("start_date"),
                "period_end": r.get("end_date"),
                "filing_date": r.get("filing_date") or r.get("acceptance_datetime", "")[:10],
                "eps_actual": eps,
                "revenue_actual": rev,
            })
        return results
    except Exception:
        return []


def fetch_polygon_aggs(ticker, from_date, to_date):
    """Get daily bars for a ticker between two dates."""
    url = (
        f"https://api.polygon.io/v2/aggs/ticker/{urllib.parse.quote(ticker)}"
        f"/range/1/day/{from_date}/{to_date}?adjusted=true&sort=asc&limit=120&apiKey={POLYGON_KEY}"
    )
    try:
        d = http_get(url, timeout=15)
        return d.get("results") or []
    except Exception:
        return []


# ────────────────────────── Recent results + post-earnings drift ──────────────────────────
def compute_pead_signal(eps_actual, eps_estimate, returns):
    """Score PEAD signal 0-100."""
    if eps_actual is None or eps_estimate is None or eps_estimate == 0:
        return None, "INSUFFICIENT_DATA", 50
    surprise_pct = (eps_actual - eps_estimate) / abs(eps_estimate) * 100
    r1d = returns.get("1d", 0) or 0
    if surprise_pct > 5 and r1d > 2:
        return surprise_pct, "STRONG_POSITIVE_DRIFT", 80
    if surprise_pct > 0 and r1d > 0:
        return surprise_pct, "POSITIVE_DRIFT", 65
    if surprise_pct < -5 and r1d < -2:
        return surprise_pct, "NEGATIVE_DRIFT", 20
    if surprise_pct < 0 and r1d < 0:
        return surprise_pct, "MODERATE_NEGATIVE_DRIFT", 35
    if abs(surprise_pct) < 2 and abs(r1d) < 1:
        return surprise_pct, "INLINE_NO_DRIFT", 50
    if surprise_pct > 0 and r1d < 0:
        return surprise_pct, "BEAT_BUT_FELL", 45
    if surprise_pct < 0 and r1d > 0:
        return surprise_pct, "MISS_BUT_ROSE", 55
    return surprise_pct, "MIXED", 50


def compute_returns(bars, anchor_date):
    """Compute 1d/5d/20d returns from anchor date."""
    if not bars:
        return {"1d": None, "5d": None, "20d": None}
    bars_sorted = sorted(bars, key=lambda b: b.get("t", 0))
    try:
        anchor_ts = int(datetime.fromisoformat(anchor_date).replace(tzinfo=timezone.utc).timestamp() * 1000)
    except Exception:
        return {"1d": None, "5d": None, "20d": None}
    anchor_idx = None
    for i, b in enumerate(bars_sorted):
        if b.get("t", 0) >= anchor_ts:
            anchor_idx = i
            break
    if anchor_idx is None or anchor_idx >= len(bars_sorted) - 1:
        return {"1d": None, "5d": None, "20d": None}
    base = bars_sorted[anchor_idx].get("c")
    if not base:
        return {"1d": None, "5d": None, "20d": None}
    out = {}
    for n_days, key in [(1, "1d"), (5, "5d"), (20, "20d")]:
        target_idx = anchor_idx + n_days
        if target_idx < len(bars_sorted):
            target = bars_sorted[target_idx].get("c")
            if target and base:
                out[key] = round(((target - base) / base) * 100, 2)
            else:
                out[key] = None
        else:
            out[key] = None
    return out


def build_recent_result(ticker):
    """For a single ticker, find recent earnings + compute drift."""
    try:
        fins = fetch_polygon_financials(ticker, limit=2)
        if not fins:
            return None
        latest = fins[0]
        filing_date = latest.get("filing_date")
        if not filing_date:
            return None
        try:
            fdt = datetime.fromisoformat(filing_date)
            if (datetime.now() - fdt).days > 35:
                return None
        except Exception:
            return None
        from_d = filing_date
        try:
            to_d = (datetime.fromisoformat(filing_date) + timedelta(days=35)).date().isoformat()
        except Exception:
            to_d = (datetime.now() + timedelta(days=2)).date().isoformat()
        bars = fetch_polygon_aggs(ticker, from_d, to_d)
        returns = compute_returns(bars, filing_date)
        prev_eps = None
        if len(fins) > 1:
            prev_eps = fins[1].get("eps_actual")
        eps_actual = latest.get("eps_actual")
        eps_yoy_pct = None
        if eps_actual is not None and prev_eps is not None and prev_eps != 0:
            eps_yoy_pct = round(((eps_actual - prev_eps) / abs(prev_eps)) * 100, 2)
        pead_surprise, pead_label, pead_score = compute_pead_signal(
            eps_actual, prev_eps, returns
        )
        return {
            "ticker": ticker,
            "filing_date": filing_date,
            "period_end": latest.get("period_end"),
            "eps_actual": eps_actual,
            "eps_prior_quarter": prev_eps,
            "revenue_actual": latest.get("revenue_actual"),
            "eps_yoy_pct": eps_yoy_pct,
            "returns": returns,
            "pead_label": pead_label,
            "pead_score": pead_score,
        }
    except Exception:
        return None


def collect_recent_results():
    """Parallel fetch recent results across watchlist."""
    out = []
    eq_tickers = [t for t in WATCHLIST if t not in {
        "SPY", "QQQ", "IWM", "GLD", "SLV", "USO", "TLT", "HYG", "LQD", "EEM", "EFA", "VWO",
    }]
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = {ex.submit(build_recent_result, t): t for t in eq_tickers}
        for fut in as_completed(futures):
            res = fut.result()
            if res:
                out.append(res)
    out.sort(key=lambda x: x.get("filing_date", ""), reverse=True)
    return out


# ────────────────────────── Aggregate stats ──────────────────────────
def aggregate_stats(recent):
    if not recent:
        return {
            "n_reported": 0,
            "beat_rate_eps_yoy": None,
            "median_1d_return_pct": None,
            "pct_positive_reactions": None,
            "best_reaction": None,
            "worst_reaction": None,
        }
    n = len(recent)
    beats = sum(1 for r in recent if (r.get("eps_yoy_pct") or 0) > 0)
    r1d = [r["returns"].get("1d") for r in recent if r.get("returns", {}).get("1d") is not None]
    r1d_sorted = sorted(r1d)
    med = r1d_sorted[len(r1d_sorted) // 2] if r1d_sorted else None
    pos = sum(1 for x in r1d if x > 0)
    valid_recent = [r for r in recent if r["returns"].get("1d") is not None]
    best = max(valid_recent, key=lambda r: r["returns"].get("1d") or -999) if valid_recent else None
    worst = min(valid_recent, key=lambda r: r["returns"].get("1d") or 999) if valid_recent else None
    return {
        "n_reported": n,
        "beat_rate_eps_yoy": round(beats / n * 100, 1) if n else None,
        "median_1d_return_pct": med,
        "pct_positive_reactions": round(pos / len(r1d) * 100, 1) if r1d else None,
        "best_reaction": {"ticker": best["ticker"], "1d": best["returns"].get("1d")} if best else None,
        "worst_reaction": {"ticker": worst["ticker"], "1d": worst["returns"].get("1d")} if worst else None,
    }


def top_pead_signals(recent, n=10):
    """Return top N PEAD signals by absolute drift score."""
    scored = [r for r in recent if r.get("pead_label") not in (None, "INSUFFICIENT_DATA", "INLINE_NO_DRIFT")]
    scored.sort(key=lambda r: abs((r.get("pead_score") or 50) - 50), reverse=True)
    return scored[:n]


# ────────────────────────── Lambda handler ──────────────────────────
def lambda_handler(event=None, context=None):
    started = time.time()
    print(f"[earnings] starting — watchlist={len(WATCHLIST)} tickers")

    upcoming = collect_upcoming_earnings(days_ahead=14)
    print(f"[earnings] upcoming: {len(upcoming)} reports in next 14d")

    recent = collect_recent_results()
    print(f"[earnings] recent: {len(recent)} reports in past 35d")

    stats = aggregate_stats(recent)
    pead = top_pead_signals(recent, n=10)
    print(f"[earnings] aggregates: beat_rate={stats.get('beat_rate_eps_yoy')}% median_1d={stats.get('median_1d_return_pct')}%")
    print(f"[earnings] PEAD top: {len(pead)}")

    out = {
        "version": "1.1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "watchlist_size": len(WATCHLIST),
        "upcoming_14d": upcoming,
        "n_upcoming": len(upcoming),
        "recent_results_30d": recent,
        "n_recent": len(recent),
        "pead_signals": pead,
        "n_pead": len(pead),
        "aggregate_stats": stats,
        "duration_s": round(time.time() - started, 2),
        "data_sources": {
            "upcoming": "Nasdaq earnings calendar API (free)",
            "actuals": "Polygon stocks-financials API",
            "returns": "Polygon aggregates API",
        },
    }

    body = json.dumps(out, default=str).encode("utf-8")
    S3.put_object(
        Bucket=BUCKET,
        Key=KEY,
        Body=body,
        ContentType="application/json",
        CacheControl="public, max-age=900",
    )
    print(f"[earnings] wrote s3://{BUCKET}/{KEY} — {len(body):,}b in {out['duration_s']}s")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "n_upcoming": out["n_upcoming"],
            "n_recent": out["n_recent"],
            "n_pead": out["n_pead"],
            "duration_s": out["duration_s"],
        }),
    }


if __name__ == "__main__":
    import json as _j
    print(_j.dumps(lambda_handler({}, None), indent=2, default=str))
