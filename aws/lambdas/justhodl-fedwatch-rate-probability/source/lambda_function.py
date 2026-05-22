"""
justhodl-fedwatch-rate-probability -- CME FedWatch-equivalent rate-path tracker.

═══════════════════════════════════════════════════════════════════════════════
INSTITUTIONAL THESIS
────────────────────
CME's FedWatch tool is the industry standard for inferring market-implied
rate-path probabilities from Fed Funds Futures (ZQ contract). Every macro
desk stares at it daily. CME publishes the UI; their API is messy. This
engine computes equivalent probabilities from Yahoo Finance's free ZQ
futures end-of-day quotes + FRED Fed Funds target series.

DISTINCTION FROM EXISTING ENGINES
──────────────────────────────────
  justhodl-cb-stance              FOMC statement NLP (qualitative)
  justhodl-fed-speak              Fed speech sentiment
  justhodl-nyfed-dealer-survey    quarterly primary dealer survey
  THIS engine                      MARKET-IMPLIED probabilities per meeting
                                   (quantitative, refreshed daily)

METHODOLOGY (replicates CME FedWatch math)
───────────────────────────────────────────
  Fed Funds Futures (ZQ) settle to the monthly avg Effective Fed Funds
  Rate (EFFR). Each contract is priced as 100 - implied_avg_rate.

  For an FOMC meeting M on date D within month K:
    - Pull the K-month ZQ contract price → implied_avg_K
    - Compute days-before-M and days-after-M within month K
    - Pre-meeting period rate = current Fed Funds target midpoint
    - Post-meeting period rate = implied_post_meeting_rate (solve)
        implied_avg_K = (n_before * current + n_after * post) / total_days
        → post = (total*implied_avg_K - n_before*current) / n_after
    - The "post" rate vs current = market-implied move from meeting M

  Quarter-point bucketing on the implied move:
    - 0 bps (hold)
    - -25 bps (cut)
    - -50 bps (cut-50)
    - +25 bps (hike)
    - +50 bps (hike-50)

  Probabilities derived from distance to each bucket (linear interpolation
  by default; more sophisticated v2 uses options on futures).

DATA SOURCES
────────────
  FRED DFEDTARU + DFEDTARL → current Fed Funds target range
  Yahoo Finance ZQ futures → contract prices per month
    URL: https://query1.finance.yahoo.com/v8/finance/chart/ZQ{MMMYY}.CBT

  Free, no auth. Yahoo Finance rate-limits but well within our daily run.

FOMC CALENDAR
─────────────
  Hardcoded next-12-months FOMC meeting dates (8 meetings/yr).
  Reviewed quarterly. Source: federalreserve.gov/monetarypolicy/fomccalendars.htm

OUTPUT
──────
  s3://justhodl-dashboard-live/data/fedwatch.json
  Schedule: daily 23:00 UTC (after futures market close)

ACADEMIC BASIS
──────────────
- Carlson, Craig, Higgins (2005). Using Fed Funds Futures to predict
  monetary policy. Federal Reserve Bank of Cleveland Working Paper.
- Gürkaynak, Sack, Swanson (2007). Market-based measures of monetary
  policy expectations. JBES, 25(2), 201-212.
═══════════════════════════════════════════════════════════════════════════════
"""
import json
import os
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

import boto3

VERSION = "1.0.0"
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = "data/fedwatch.json"

FRED_KEY = os.environ.get("FRED_API_KEY") or os.environ.get(
    "FRED_KEY", "2f057499936072679d8843d7fce99989")
FRED_BASE = "https://api.stlouisfed.org/fred"
HTTP_TIMEOUT = 20

# FOMC meeting calendar (next ~12 months as of 2026-05-22)
# Source: federalreserve.gov/monetarypolicy/fomccalendars.htm
# Updated quarterly. Statement releases ~14:00 ET typically.
FOMC_CALENDAR_2026 = [
    "2026-01-28",
    "2026-03-18",
    "2026-04-29",
    "2026-06-17",
    "2026-07-29",
    "2026-09-16",
    "2026-10-28",
    "2026-12-09",
]
FOMC_CALENDAR_2027 = [
    "2027-01-27",
    "2027-03-17",
    "2027-04-28",
    "2027-06-16",
]

# CME ZQ Fed Funds Futures contract month codes
MONTH_CODES = {
    1: "F", 2: "G", 3: "H", 4: "J", 5: "K", 6: "M",
    7: "N", 8: "Q", 9: "U", 10: "V", 11: "X", 12: "Z",
}


s3 = boto3.client("s3", region_name="us-east-1")


def http_json(url, timeout=HTTP_TIMEOUT, ua="JustHodl-FedWatch/1.0"):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": ua})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        print(f"[http] {e.code}: {url[:90]}")
        return None
    except Exception as e:
        print(f"[http] err: {str(e)[:80]}")
        return None


# ---------- FRED helpers ----------
def fred_latest(series_id):
    url = (f"{FRED_BASE}/series/observations?series_id={series_id}"
           f"&api_key={FRED_KEY}&file_type=json&sort_order=desc&limit=1")
    d = http_json(url)
    obs = (d.get("observations") if isinstance(d, dict) else None) or []
    if obs and obs[0].get("value") not in (".", None):
        try:
            return float(obs[0]["value"]), obs[0].get("date")
        except (ValueError, TypeError):
            return None, None
    return None, None


def get_current_fed_funds_range():
    upper, date_u = fred_latest("DFEDTARU")
    lower, date_l = fred_latest("DFEDTARL")
    if upper is None or lower is None:
        return None
    return {
        "lower": lower,
        "upper": upper,
        "midpoint": (lower + upper) / 2,
        "as_of": date_u or date_l,
    }


# ---------- Yahoo Finance futures fetcher ----------
def yahoo_zq_close(month, year):
    """Get ZQ futures last close price for month M (1-12) and year YY.
    Yahoo ticker format: ZQM25.CBT (June 2025)."""
    code = MONTH_CODES.get(month)
    if not code:
        return None
    yy = year % 100
    ticker = f"ZQ{code}{yy:02d}.CBT"
    url = (f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
           f"?range=10d&interval=1d")
    d = http_json(url, ua="Mozilla/5.0 (compatible; JustHodl)")
    if not isinstance(d, dict):
        return None
    chart = (d.get("chart") or {}).get("result") or []
    if not chart:
        return None
    result = chart[0]
    closes = (((result.get("indicators") or {}).get("quote") or [{}])[0]
                .get("close") or [])
    # Find latest non-null close
    for c in reversed(closes):
        if c is not None:
            return float(c)
    return None


# ---------- FedWatch math ----------
def fed_funds_rate_buckets(current_midpoint):
    """Return rate buckets in 25bp steps around current."""
    return {
        "hike_50":  current_midpoint + 0.50,
        "hike_25":  current_midpoint + 0.25,
        "hold":     current_midpoint,
        "cut_25":   current_midpoint - 0.25,
        "cut_50":   current_midpoint - 0.50,
        "cut_75":   current_midpoint - 0.75,
    }


def implied_post_meeting_rate(implied_avg_month_rate, current_midpoint,
                                 meeting_date_str, month_year):
    """Solve for the rate that prevailed AFTER meeting given month avg."""
    try:
        meet_dt = datetime.strptime(meeting_date_str, "%Y-%m-%d")
    except ValueError:
        return None
    month, year = month_year
    if meet_dt.month != month or meet_dt.year != year:
        return None
    # Days in month
    if month == 12:
        next_month = datetime(year + 1, 1, 1)
    else:
        next_month = datetime(year, month + 1, 1)
    days_in_month = (next_month - datetime(year, month, 1)).days
    n_before = meet_dt.day - 1
    n_after = days_in_month - n_before
    if n_after <= 0:
        return None
    # avg = (n_before * current + n_after * post) / total
    # post = (total * avg - n_before * current) / n_after
    post = ((days_in_month * implied_avg_month_rate
              - n_before * current_midpoint) / n_after)
    return round(post, 4)


def assign_probabilities(implied_post_rate, current_midpoint):
    """Assign probabilities to discrete rate buckets via linear interpolation.

    Standard CME methodology: snap the implied rate to nearest two
    quarter-point buckets and split probability proportionally to distance.
    """
    buckets = fed_funds_rate_buckets(current_midpoint)
    # Sort by rate ascending
    sorted_buckets = sorted(buckets.items(), key=lambda x: x[1])
    rates = [r for _, r in sorted_buckets]
    names = [n for n, _ in sorted_buckets]
    # Out of range
    if implied_post_rate <= rates[0]:
        return {names[0]: 100.0}
    if implied_post_rate >= rates[-1]:
        return {names[-1]: 100.0}
    # Find the two adjacent buckets
    for i in range(len(rates) - 1):
        lo = rates[i]
        hi = rates[i + 1]
        if lo <= implied_post_rate <= hi:
            span = hi - lo
            if span <= 0:
                return {names[i]: 100.0}
            w_hi = (implied_post_rate - lo) / span
            w_lo = 1 - w_hi
            out = {}
            if w_lo > 0:
                out[names[i]] = round(w_lo * 100, 1)
            if w_hi > 0:
                out[names[i + 1]] = round(w_hi * 100, 1)
            return out
    return {"hold": 100.0}


def get_fomc_meetings_ahead(today=None, n=8):
    """Return next N FOMC meetings ahead of today."""
    if today is None:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    all_meetings = sorted(set(FOMC_CALENDAR_2026 + FOMC_CALENDAR_2027))
    ahead = [m for m in all_meetings if m >= today]
    return ahead[:n]


# ---------- Main ----------
def lambda_handler(event=None, context=None):
    started = time.time()
    print(f"[fedwatch] start v{VERSION}")

    # 1) Current Fed Funds target range
    ff_range = get_current_fed_funds_range()
    if not ff_range:
        return {"statusCode": 500, "body": json.dumps({
            "ok": False, "error": "could not fetch FRED DFEDTARU/DFEDTARL"})}
    current_mid = ff_range["midpoint"]
    print(f"[fedwatch] current FF target {ff_range['lower']:.2f}-"
          f"{ff_range['upper']:.2f} (mid {current_mid:.2f})")

    # 2) FOMC meetings ahead
    meetings = get_fomc_meetings_ahead(n=8)

    # 3) For each meeting, pull relevant ZQ futures contract
    today_dt = datetime.now(timezone.utc)
    out_meetings = []
    for meeting_date in meetings:
        try:
            md = datetime.strptime(meeting_date, "%Y-%m-%d")
        except ValueError:
            continue
        days_until = (md - today_dt).days

        # Pull the futures contract for the meeting's month
        contract_price = yahoo_zq_close(md.month, md.year)
        time.sleep(0.3)
        if contract_price is None:
            out_meetings.append({
                "date": meeting_date,
                "days_until": days_until,
                "status": "no_futures_data",
                "futures_contract": (f"ZQ{MONTH_CODES.get(md.month)}"
                                       f"{md.year % 100:02d}"),
            })
            continue

        implied_avg_rate = 100 - contract_price
        post_rate = implied_post_meeting_rate(
            implied_avg_rate, current_mid, meeting_date,
            (md.month, md.year))
        if post_rate is None:
            out_meetings.append({
                "date": meeting_date,
                "days_until": days_until,
                "status": "calculation_failed",
                "implied_avg_month_rate": round(implied_avg_rate, 3),
            })
            continue

        probabilities = assign_probabilities(post_rate, current_mid)
        implied_move_bps = round((post_rate - current_mid) * 100)

        out_meetings.append({
            "date": meeting_date,
            "days_until": days_until,
            "status": "ok",
            "futures_contract": (f"ZQ{MONTH_CODES.get(md.month)}"
                                    f"{md.year % 100:02d}"),
            "futures_close": round(contract_price, 3),
            "implied_avg_month_rate_pct": round(implied_avg_rate, 3),
            "implied_post_meeting_rate_pct": post_rate,
            "implied_move_bps": implied_move_bps,
            "probabilities_pct": probabilities,
            "rate_buckets_pct": fed_funds_rate_buckets(current_mid),
        })

    # 4) Aggregate dominant scenario for next 6 months
    next_six = [m for m in out_meetings
                  if m.get("status") == "ok"
                     and m.get("days_until", 0) <= 180]
    if next_six:
        total_implied_move_bps = sum(m["implied_move_bps"]
                                       for m in next_six)
        avg_implied_move = (total_implied_move_bps / len(next_six)
                                if next_six else 0)
        if total_implied_move_bps <= -50:
            scenario = "AGGRESSIVE_CUTTING"
        elif total_implied_move_bps <= -25:
            scenario = "MODERATE_CUTTING"
        elif total_implied_move_bps < 25:
            scenario = "HOLD_OR_LIGHT_CUT"
        elif total_implied_move_bps < 50:
            scenario = "LIGHT_HIKING"
        else:
            scenario = "AGGRESSIVE_HIKING"
    else:
        total_implied_move_bps = None
        avg_implied_move = None
        scenario = "INSUFFICIENT_DATA"

    output = {
        "engine": "fedwatch-rate-probability",
        "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "current_fed_funds_range": ff_range,
        "next_meeting": out_meetings[0] if out_meetings else None,
        "meetings_ahead": out_meetings,
        "n_meetings_with_data": sum(1 for m in out_meetings
                                      if m.get("status") == "ok"),
        "next_6mo_summary": {
            "scenario": scenario,
            "cumulative_implied_move_bps": total_implied_move_bps,
            "avg_implied_move_per_meeting_bps": (
                round(avg_implied_move, 1)
                if avg_implied_move is not None else None),
            "n_meetings_evaluated": len(next_six),
        },
        "methodology": {
            "framework": "CME FedWatch methodology — Fed Funds Futures-implied",
            "math": (
                "Implied avg month rate = 100 - ZQ_contract_price. "
                "Solve post-meeting rate from weighted avg formula:  "
                "avg = (n_before*current + n_after*post)/total_days. "
                "Probabilities = linear interpolation between adjacent "
                "25bp buckets."),
            "data_sources": (
                "FRED DFEDTARU/DFEDTARL (current target range); "
                "Yahoo Finance ZQ futures end-of-day quotes (free, no auth)."),
            "fomc_calendar": (
                "Hardcoded next 12 months; reviewed quarterly per "
                "federalreserve.gov/monetarypolicy/fomccalendars.htm."),
            "caveat": (
                "v1 uses linear interpolation; CME uses skew from options "
                "on futures for asymmetric probabilities. v2 will add."),
        },
        "academic_basis": [
            "Carlson, Craig, Higgins (2005). Using Fed Funds Futures to "
            "predict monetary policy. FRB Cleveland WP.",
            "Gürkaynak, Sack, Swanson (2007). Market-based measures of "
            "monetary policy expectations. JBES.",
        ],
        "duration_seconds": round(time.time() - started, 1),
    }

    s3.put_object(
        Bucket=S3_BUCKET, Key=S3_KEY,
        Body=json.dumps(output, default=str).encode("utf-8"),
        ContentType="application/json",
        CacheControl="public, max-age=3600")

    print(f"[fedwatch] complete: scenario={scenario} "
          f"meetings_with_data={output['n_meetings_with_data']}/8")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "ok": True, "version": VERSION,
            "scenario": scenario,
            "current_ff_range": ff_range,
            "next_meeting": output["next_meeting"],
            "next_6mo_cumulative_bps": total_implied_move_bps,
        }),
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), indent=2))
