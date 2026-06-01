"""
justhodl-auction-crisis-detector — phase 10.

Treasury Auction Crisis Detection. Reads every settled US Treasury auction from
the fiscaldata.treasury.gov API and scores it against the historical-crisis
signature patterns extracted from 9 PDFs covering:

  - GFC peak (Sep 17, 18, 23 + Oct 8, 2008)
  - COVID crash (Mar 11, 19, 26 — pre/during/post Fed bazooka, 2020)
  - 2021 crypto-top bill auction (calm complacency benchmark)
  - 2024 normal-market notes (healthy benchmark)

The 6 quantified crisis patterns:

  1. ZERO-RATE BILL FLOOR
       low_rate <= 0.001% on tenor < 90d AND policy rate > 0%
       (money parking at any cost while Fed has room — only happens in crisis)

  2. BID-TO-COVER EXTREMES
       BTC > 3.5 on bills = stampede flight to safety
       BTC < 2.0 on coupons = failed-auction warning

  3. ALLOTTED-AT-HIGH (TAIL) STRESS
       AAH > 90% on coupons = WEAK TAIL, dealer absorption
       AAH < 20% on bills during otherwise-stressed period =
         panic clustering at the low end

  4. PRIMARY DEALER SHARE
       PD share > 35% of accepted competitive = dealers stuck with paper
       PD share < 15% = strong indirect bid (healthy)

  5. INDIRECT (FOREIGN) SHARE COLLAPSE
       Indirect share < 50% on coupons = foreign demand exodus
       Especially diagnostic during USD-stress events

  6. BILL ISSUANCE SIZE EXPLOSION
       Trailing 4-week issuance > +30% above 1Y average
       = Treasury liquidity injection, fiscal panic, or both

Composite crisis score 0-100. Aggregates each indicator's signal:
  0-25     CALM          (2024-style normal)
  25-50    WATCH         (one indicator stressed)
  50-75    ELEVATED      (multiple indicators stressed, COVID-Q1-style early)
  75-100   ACUTE_STRESS  (GFC Lehman-week / COVID-Mar-19 pattern)

Output: s3://justhodl-dashboard-live/data/auction-crisis.json
Schedule: hourly (auctions settle daily but ratings can update intraday)

Front-end: treasury-auctions.html (existing) and a new auction-crisis section
on bonds.html.
"""
import json
import math
import os
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta
import boto3
from concurrent.futures import ThreadPoolExecutor
import _fred_shim  # noqa: F401  — cache-first FRED + 429 backoff (ops/1074)

# v2 expansion module — tenor decomp, forward calendar, analog matching,
# cross-signals, composite history, tail risk, triggers. Each function is
# pure (no S3, no env). See auction_crisis_v2.py for full implementation.
from auction_crisis_v2 import (
    compute_tenor_decomposition,
    fetch_upcoming_auctions,
    build_forward_calendar,
    find_historical_analogs,
    compute_cross_signals,
    build_composite_history,
    compute_tail_risk,
    build_triggers,
)

S3_BUCKET = "justhodl-dashboard-live"
S3_KEY = "data/auction-crisis.json"
FISCAL_BASE = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/od/auctions_query"

s3 = boto3.client("s3", region_name="us-east-1")


# ─────────────────────────────────────────────────────────────────────
# Historical baselines (computed from the PDFs you uploaded)
# These are the "normal market" benchmarks used as null-hypothesis anchors.
# ─────────────────────────────────────────────────────────────────────
NORMAL_2024_BASELINE = {
    "bills_lt_90d": {
        "btc_mean": 2.85,         # bills BTC normal range
        "btc_std": 0.6,
        "low_rate_floor_pct": 4.0, # 4-5% = current Fed-on-hold
        "indirect_share_pct": 35,  # avg indirect on bills
        "pd_share_pct": 50,        # higher PD% normal on bills
        "aah_mean": 50,
    },
    "coupons_gt_3y": {
        "btc_mean": 2.45,
        "btc_std": 0.25,
        "indirect_share_pct": 70,  # foreign CBs heavy in coupons
        "pd_share_pct": 18,        # very low PD share is healthy
        "aah_mean": 75,            # aah varies widely on coupons; high but well-bid
    },
    "tips": {
        "btc_mean": 2.40,
        "btc_std": 0.35,
        "indirect_share_pct": 65,
        "pd_share_pct": 20,
    },
}


# Reference crisis points for triangulation
CRISIS_REFERENCE = [
    {"date": "2008-09-17", "regime": "GFC_PEAK",  "btc": 2.45, "low_rate": 0.000, "aah": 83.72,  "pd_share_pct": 46.7,  "indirect_share_pct": 51.1},
    {"date": "2008-09-18", "regime": "GFC_PEAK",  "btc": 2.16, "low_rate": 0.000, "aah": 76.68,  "pd_share_pct": 69.0,  "indirect_share_pct": 29.3},
    {"date": "2008-09-23", "regime": "GFC_PEAK",  "btc": 2.86, "low_rate": 0.000, "aah":  6.92,  "pd_share_pct": 52.1,  "indirect_share_pct": 47.1},
    {"date": "2008-10-08", "regime": "GFC_PEAK",  "btc": 2.22, "low_rate": 2.359, "aah": 73.90,  "pd_share_pct": 52.4,  "indirect_share_pct": 47.5},  # TIPS
    {"date": "2020-03-11", "regime": "COVID_CRASH","btc": 2.36, "low_rate": 0.080, "aah": 74.94, "pd_share_pct": 29.8,  "indirect_share_pct": 61.0},
    {"date": "2020-03-19", "regime": "COVID_CRASH","btc": 2.91, "low_rate": 0.000, "aah": 53.83, "pd_share_pct": 41.8,  "indirect_share_pct": 46.4},
    {"date": "2020-03-26", "regime": "COVID_BAZOOKA","btc": 4.74, "low_rate": 0.000, "aah": 28.74, "pd_share_pct": 44.3,  "indirect_share_pct": 51.3},
    {"date": "2021-10-21", "regime": "COMPLACENCY","btc": 3.52, "low_rate": 0.020, "aah": 79.47, "pd_share_pct": 42.1,  "indirect_share_pct": 53.0},
    {"date": "2024-04-10", "regime": "NORMAL",    "btc": 2.34, "low_rate": 4.400, "aah": 54.10,  "pd_share_pct": 24.0,  "indirect_share_pct": 61.8},
    {"date": "2024-10-09", "regime": "NORMAL",    "btc": 2.48, "low_rate": 2.880, "aah": 99.31,  "pd_share_pct": 13.9,  "indirect_share_pct": 77.6},
]


def fetch_fiscal_auctions(start_date, end_date, page_size=200):
    """Pull auction data from fiscaldata.treasury.gov. Returns list of records.

    Always pulls fresh — no caching. The fiscaldata API publishes auction
    results within ~1-2 hours of auction close. We pull on every Lambda
    invocation so the dashboard reflects whatever Treasury has published
    most recently, including same-day auctions.

    URL contract:
      https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/
        accounting/od/auctions_query?
        filter=auction_date:gte:YYYY-MM-DD,auction_date:lte:YYYY-MM-DD&
        sort=-auction_date&
        format=json&
        page[size]=200&
        page[number]=N
    """
    all_records = []
    page = 1
    while True:
        params = {
            "filter": f"auction_date:gte:{start_date},auction_date:lte:{end_date}",
            "sort": "-auction_date",  # most recent first
            "format": "json",
            "page[size]": page_size,
            "page[number]": page,
        }
        url = FISCAL_BASE + "?" + urllib.parse.urlencode(params, safe=":,")
        try:
            req = urllib.request.Request(url, headers={
                "Accept": "application/json",
                "User-Agent": "justhodl-auction-crisis-detector/1.0",
                "Cache-Control": "no-cache, no-store, must-revalidate",
                "Pragma": "no-cache",
            })
            with urllib.request.urlopen(req, timeout=20) as r:
                body = json.loads(r.read())
        except Exception as e:
            print(f"[fiscal] error page {page}: {e}")
            break
        data = body.get("data", [])
        if not data:
            break
        all_records.extend(data)
        meta = body.get("meta", {})
        total_pages = meta.get("total-pages", 1)
        if page >= total_pages or len(data) < page_size:
            break
        page += 1
        if page > 30:  # safety
            break
    return all_records


def parse_float(v, default=None):
    if v in (None, "", "null"):
        return default
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def classify_tenor_bucket(record):
    """Returns 'bills_lt_90d', 'bills_gte_90d', 'coupons_lt_3y', 'coupons_gt_3y', 'tips', 'frn'."""
    sec_type = (record.get("security_type") or "").upper()
    sec_term = (record.get("security_term") or "").upper()
    if "TIPS" in sec_type or "TIIN" in sec_type or "TIPS" in sec_term:
        return "tips"
    if "FRN" in sec_type or "FRN" in sec_term:
        return "frn"
    if "BILL" in sec_type:
        # parse tenor from term: e.g. "28-DAY", "13-WEEK", "26-WEEK"
        days = parse_term_to_days(sec_term)
        if days is None:
            return "bills_lt_90d"
        return "bills_lt_90d" if days < 90 else "bills_gte_90d"
    if "NOTE" in sec_type or "BOND" in sec_type:
        days = parse_term_to_days(sec_term)
        if days is None or days >= 365 * 3:
            return "coupons_gt_3y"
        return "coupons_lt_3y"
    return "coupons_gt_3y"


def parse_term_to_days(term):
    """'28-DAY' → 28, '13-WEEK' → 91, '10-YEAR' → 3650."""
    if not term:
        return None
    t = term.upper().strip()
    parts = t.replace("-", " ").split()
    try:
        n = int(parts[0])
    except (ValueError, IndexError):
        return None
    if "DAY" in t:
        return n
    if "WEEK" in t:
        return n * 7
    if "YEAR" in t:
        return n * 365
    if "MONTH" in t:
        return n * 30
    return None


def compute_record_metrics(record):
    """Extract the 6 diagnostic metrics from a fiscaldata auction record.

    Field names from API:
      - bid_to_cover_ratio
      - high_yield (or high_discnt_rate for bills)
      - low_yield (or low_discnt_rate)
      - allocation_pctage  (% allotted at high)
      - primary_dealer_accepted
      - direct_bidder_accepted
      - indirect_bidder_accepted
      - total_accepted
    """
    btc = parse_float(record.get("bid_to_cover_ratio"))

    # Bills use discount rate, coupons use yield
    high_rate = parse_float(record.get("high_yield"))
    if high_rate is None:
        high_rate = parse_float(record.get("high_discnt_rate"))
    low_rate = parse_float(record.get("low_yield"))
    if low_rate is None:
        low_rate = parse_float(record.get("low_discnt_rate"))
    median_rate = parse_float(record.get("median_yield"))
    if median_rate is None:
        median_rate = parse_float(record.get("median_discnt_rate"))

    aah = parse_float(record.get("allocation_pctage"))

    pd = parse_float(record.get("primary_dealer_accepted"), 0) or 0
    direct = parse_float(record.get("direct_bidder_accepted"), 0) or 0
    indirect = parse_float(record.get("indirect_bidder_accepted"), 0) or 0
    total_competitive = pd + direct + indirect
    pd_share = (pd / total_competitive * 100) if total_competitive > 0 else None
    indirect_share = (indirect / total_competitive * 100) if total_competitive > 0 else None
    direct_share = (direct / total_competitive * 100) if total_competitive > 0 else None

    accepted_total = parse_float(record.get("total_accepted"))

    tail_bp = None
    if high_rate is not None and median_rate is not None:
        tail_bp = (high_rate - median_rate) * 100  # basis points

    return {
        "auction_date": record.get("auction_date"),
        "issue_date": record.get("issue_date"),
        "security_type": record.get("security_type"),
        "security_term": record.get("security_term"),
        "cusip": record.get("cusip"),
        "tenor_bucket": classify_tenor_bucket(record),
        "high_rate": high_rate,
        "low_rate": low_rate,
        "median_rate": median_rate,
        "allocated_at_high_pct": aah,
        "btc": btc,
        "primary_dealer_pct": pd_share,
        "indirect_pct": indirect_share,
        "direct_pct": direct_share,
        "tail_bp": tail_bp,
        "accepted_billions": accepted_total / 1e9 if accepted_total else None,
    }


def score_indicators(metrics, fed_funds_rate):
    """Run each metric through the historical-pattern crisis test.

    Returns dict of {indicator_name: score 0-100}.
    """
    bucket = metrics["tenor_bucket"]
    scores = {}

    # ── 1. ZERO-RATE BILL FLOOR ──
    # only diagnostic when Fed has positive policy rate (else it's just at the lower bound)
    if bucket == "bills_lt_90d" and metrics["low_rate"] is not None:
        if fed_funds_rate is not None and fed_funds_rate > 1.0:
            # In a normal-rate environment, floor at 0% = panic
            if metrics["low_rate"] <= 0.001:
                scores["zero_rate_floor"] = 100  # 2008 / 2020 GFC pattern
            elif metrics["low_rate"] < fed_funds_rate * 0.3:
                scores["zero_rate_floor"] = 70   # severe undercut
            elif metrics["low_rate"] < fed_funds_rate * 0.6:
                scores["zero_rate_floor"] = 40
            else:
                scores["zero_rate_floor"] = 0    # normal
        else:
            # Fed already at zero-bound — signal undefined
            scores["zero_rate_floor"] = None

    # ── 2. BID-TO-COVER EXTREMES ──
    if metrics["btc"] is not None:
        if bucket.startswith("bills_"):
            # On bills, EXTREMELY HIGH BTC = panic (2020-Mar-26 had 4.74)
            base = NORMAL_2024_BASELINE["bills_lt_90d"]
            z = (metrics["btc"] - base["btc_mean"]) / base["btc_std"]
            if z >= 3.0:
                scores["btc_extreme"] = 100
            elif z >= 2.0:
                scores["btc_extreme"] = 70
            elif z >= 1.5:
                scores["btc_extreme"] = 45
            elif z <= -1.5:
                scores["btc_extreme"] = 50  # weak demand also stressful
            elif z <= -2.0:
                scores["btc_extreme"] = 80  # failed auction territory
            else:
                scores["btc_extreme"] = 0
        elif bucket in ("coupons_lt_3y", "coupons_gt_3y", "tips"):
            # On coupons, LOW BTC = failed auction warning
            base = NORMAL_2024_BASELINE.get("coupons_gt_3y" if bucket == "coupons_gt_3y" else "tips" if bucket == "tips" else "coupons_gt_3y")
            z = (metrics["btc"] - base["btc_mean"]) / base["btc_std"]
            if z <= -2.5:
                scores["btc_extreme"] = 100  # near-failure
            elif z <= -1.5:
                scores["btc_extreme"] = 70
            elif z <= -1.0:
                scores["btc_extreme"] = 40
            else:
                scores["btc_extreme"] = 0

    # ── 3. ALLOTTED-AT-HIGH (TAIL SEVERITY) ──
    if metrics["allocated_at_high_pct"] is not None:
        aah = metrics["allocated_at_high_pct"]
        if bucket.startswith("bills_"):
            # On bills during stress, LOW AAH = panic clustering at the low end
            # Only diagnostic when paired with other stress indicators
            if aah < 15:
                scores["tail_stress"] = 60  # extreme clustering — wait for confirmation
            elif aah > 90:
                scores["tail_stress"] = 50  # weak last leg
            else:
                scores["tail_stress"] = 0
        elif bucket in ("coupons_lt_3y", "coupons_gt_3y", "tips"):
            if aah > 95:
                scores["tail_stress"] = 75  # near tail-out, dealers absorbed
            elif aah > 90:
                scores["tail_stress"] = 50
            elif aah < 30:
                scores["tail_stress"] = 70  # below-norm = investors stepped back
            else:
                scores["tail_stress"] = 0

    # ── 4. PRIMARY DEALER SHARE ──
    if metrics["primary_dealer_pct"] is not None:
        pd_pct = metrics["primary_dealer_pct"]
        if bucket in ("coupons_lt_3y", "coupons_gt_3y", "tips"):
            # PD > 35% on coupons = dealer absorption (2008 had 46-70%)
            if pd_pct > 50:
                scores["pd_absorption"] = 100  # GFC-extreme
            elif pd_pct > 35:
                scores["pd_absorption"] = 70
            elif pd_pct > 25:
                scores["pd_absorption"] = 35
            else:
                scores["pd_absorption"] = 0   # healthy
        else:
            # Bills naturally have higher PD shares; only flag if MUCH higher than baseline
            base = NORMAL_2024_BASELINE["bills_lt_90d"]["pd_share_pct"]
            if pd_pct > base + 25:
                scores["pd_absorption"] = 70
            elif pd_pct > base + 15:
                scores["pd_absorption"] = 35
            else:
                scores["pd_absorption"] = 0

    # ── 5. INDIRECT (FOREIGN) SHARE COLLAPSE ──
    if metrics["indirect_pct"] is not None:
        ind_pct = metrics["indirect_pct"]
        if bucket in ("coupons_lt_3y", "coupons_gt_3y", "tips"):
            # Foreign demand on coupons. <50% = exodus pattern (2008-09-18 had 29.3%)
            if ind_pct < 30:
                scores["indirect_collapse"] = 100
            elif ind_pct < 50:
                scores["indirect_collapse"] = 65
            elif ind_pct < 60:
                scores["indirect_collapse"] = 30
            else:
                scores["indirect_collapse"] = 0
        # Bills indirect varies more; not as diagnostic

    return {k: v for k, v in scores.items() if v is not None}


def get_fed_funds_rate():
    """Fetch latest Fed funds effective rate from FRED. Returns float or None."""
    fred_key = os.environ.get("FRED_API_KEY", "2f057499936072679d8843d7fce99989")
    url = (
        "https://api.stlouisfed.org/fred/series/observations"
        f"?series_id=DFF&api_key={fred_key}&file_type=json&limit=5&sort_order=desc"
    )
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            data = json.loads(r.read())
        for obs in data.get("observations", []):
            v = obs.get("value")
            if v not in (".", "", None):
                return float(v)
    except Exception as e:
        print(f"[fred] DFF error: {e}")
    return None


def detect_issuance_explosion(records):
    """Pattern 6: 4-week bill issuance vs. trailing 1Y average.
    
    Returns score 0-100.
    """
    if not records:
        return None
    bills = [r for r in records if "BILL" in (r.get("security_type") or "").upper()]
    if not bills:
        return None
    today = datetime.now(timezone.utc).date()
    cutoff_4w = today - timedelta(days=28)
    cutoff_1y = today - timedelta(days=365)

    total_4w = 0
    total_1y = 0
    days_1y = 0
    days_4w = 0
    seen_dates_4w = set()
    seen_dates_1y = set()

    for r in bills:
        d_str = r.get("auction_date")
        accepted = parse_float(r.get("total_accepted"))
        if not d_str or not accepted:
            continue
        try:
            d = datetime.strptime(d_str[:10], "%Y-%m-%d").date()
        except ValueError:
            continue
        if d >= cutoff_4w:
            total_4w += accepted
            seen_dates_4w.add(d)
        if d >= cutoff_1y:
            total_1y += accepted
            seen_dates_1y.add(d)

    if not seen_dates_1y or not seen_dates_4w:
        return None

    avg_4w_amount = total_4w / max(1, len(seen_dates_4w))
    avg_1y_amount = total_1y / max(1, len(seen_dates_1y))
    if avg_1y_amount == 0:
        return None
    pct_above_baseline = (avg_4w_amount - avg_1y_amount) / avg_1y_amount * 100

    if pct_above_baseline > 50:
        return {"score": 90, "pct_above_baseline": round(pct_above_baseline, 1)}
    if pct_above_baseline > 30:
        return {"score": 60, "pct_above_baseline": round(pct_above_baseline, 1)}
    if pct_above_baseline > 15:
        return {"score": 30, "pct_above_baseline": round(pct_above_baseline, 1)}
    return {"score": 0, "pct_above_baseline": round(pct_above_baseline, 1)}


def lambda_handler(event, context):
    t0 = time.time()
    print(f"[auction-crisis] start {datetime.now(timezone.utc).isoformat()}")

    # Pull last 90 days of auctions to give us a window for issuance analysis
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=400)  # +1y for issuance baseline

    raw = fetch_fiscal_auctions(start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
    print(f"[auction-crisis] fetched {len(raw)} auction records")

    if not raw:
        body = {
            "schema_version": "1.0",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "status": "no_data",
            "message": "No auction records returned from fiscaldata API",
        }
        s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY, Body=json.dumps(body, indent=2),
                      ContentType="application/json", CacheControl="max-age=600")
        return {"statusCode": 200, "body": json.dumps({"status": "no_data"})}

    fed_rate = get_fed_funds_rate()
    print(f"[auction-crisis] Fed funds rate: {fed_rate}")

    # Compute metrics + scores for each auction
    scored_auctions = []
    for r in raw:
        m = compute_record_metrics(r)
        if not m["btc"]:
            continue  # skip incomplete records
        s = score_indicators(m, fed_rate)
        if not s:
            continue
        # Composite for THIS auction = max-of-indicators (most-stressed wins)
        max_score = max(s.values()) if s else 0
        avg_score = sum(s.values()) / len(s) if s else 0
        # Use 70% max + 30% avg — emphasis on the worst signal
        composite = 0.7 * max_score + 0.3 * avg_score
        scored_auctions.append({
            **m,
            "indicator_scores": s,
            "max_indicator_score": round(max_score, 1),
            "avg_indicator_score": round(avg_score, 1),
            "composite_score": round(composite, 1),
        })

    # Sort by date descending
    scored_auctions.sort(key=lambda x: x.get("auction_date") or "", reverse=True)

    # Most-recent auctions (last 14d)
    today = datetime.now(timezone.utc).date()
    recent_cutoff = today - timedelta(days=14)
    recent = [a for a in scored_auctions
              if a.get("auction_date") and
              datetime.strptime(a["auction_date"][:10], "%Y-%m-%d").date() >= recent_cutoff]

    # Composite system-wide score: 14d average of per-auction composites,
    # weighted by accepted size. Then add the issuance-explosion overlay.
    if recent:
        total_size = sum(a.get("accepted_billions") or 1 for a in recent)
        weighted_sum = sum((a["composite_score"] * (a.get("accepted_billions") or 1)) for a in recent)
        recent_composite = weighted_sum / total_size
    else:
        recent_composite = 0

    issuance = detect_issuance_explosion(raw)
    if issuance and issuance.get("score"):
        # Issuance explosion adds 0-15 points to composite
        recent_composite = min(100, recent_composite + issuance["score"] * 0.15)

    # Regime classification
    if recent_composite >= 75:
        regime = "ACUTE_STRESS"
        interp = "Multiple auction-stress indicators firing in last 14d. GFC-Lehman-week / COVID-Mar-19 pattern. Immediate action required."
    elif recent_composite >= 50:
        regime = "ELEVATED"
        interp = "Auction stress indicators warming up. Pre-crisis pattern. Reduce risk now."
    elif recent_composite >= 25:
        regime = "WATCH"
        interp = "One or two stress indicators flickering. Tighten stops, prepare hedges."
    else:
        regime = "CALM"
        interp = "Auctions clearing normally. Healthy demand profile, no flight-to-safety pattern."

    # Build report
    # Freshness tracking: detect the most-recent auction in the data and
    # how stale it is. If Treasury hasn't published a new auction in >36h
    # AND it's a weekday business window, that itself is a red flag
    # (publication delay during stress).
    latest_auction_date = None
    latest_cusip = None
    latest_auction_iso = None
    if scored_auctions:
        # scored_auctions is sorted desc by auction_date already
        latest = scored_auctions[0]
        latest_auction_date = latest.get("auction_date")
        latest_cusip = latest.get("cusip")
        if latest_auction_date:
            latest_auction_iso = latest_auction_date

    # How fresh? Compute hours since latest auction settlement.
    hours_since_latest = None
    if latest_auction_date:
        try:
            la = datetime.strptime(latest_auction_date[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
            hours_since_latest = round((datetime.now(timezone.utc) - la).total_seconds() / 3600, 1)
        except Exception:
            pass

    # Compare against PREVIOUS run's latest_cusip to detect if THIS run
    # picked up a brand-new auction (used for change-detection / alerts).
    is_new_auction_this_run = False
    previous_latest_cusip = None
    try:
        prev_body = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)["Body"].read()
        prev = json.loads(prev_body)
        previous_latest_cusip = prev.get("freshness", {}).get("latest_cusip")
        if previous_latest_cusip and latest_cusip and previous_latest_cusip != latest_cusip:
            is_new_auction_this_run = True
    except Exception:
        # First run, or no prior file
        pass

    # ═══════════════════════════════════════════════════════════════════
    # V2 EXPANSION (ops/1089) — institutional-grade analytical layers
    # Each computation is fail-soft: errors degrade the corresponding
    # section to {} or [] rather than crashing the whole Lambda.
    # ═══════════════════════════════════════════════════════════════════
    v2_start = time.time()
    print(f"[auction-crisis] v2: building expansion layers…")

    # 1. Tenor decomposition
    try:
        tenor_decomp = compute_tenor_decomposition(scored_auctions, window_days=14)
    except Exception as e:
        print(f"[v2] tenor_decomp error: {e}")
        tenor_decomp = {}

    # 2. Forward calendar
    try:
        upcoming_raw = fetch_upcoming_auctions(days_ahead=30)
        forward_calendar = build_forward_calendar(upcoming_raw, tenor_decomp, scored_auctions)
    except Exception as e:
        print(f"[v2] forward_calendar error: {e}")
        forward_calendar = []

    # 3. Historical analog matching
    try:
        issuance_score_for_analog = (issuance.get("score") if issuance else 0) or 0
        historical_analog = find_historical_analogs(
            scored_auctions, CRISIS_REFERENCE,
            issuance_score=issuance_score_for_analog, top_n=3,
        )
    except Exception as e:
        print(f"[v2] historical_analog error: {e}")
        historical_analog = {}

    # 4. Cross-signals from FRED
    try:
        cross_signals = compute_cross_signals()
    except Exception as e:
        print(f"[v2] cross_signals error: {e}")
        cross_signals = {}

    # 5. Composite history (30-day trend)
    try:
        composite_history = build_composite_history(scored_auctions, days=30)
    except Exception as e:
        print(f"[v2] composite_history error: {e}")
        composite_history = {"series": [], "change_points": [], "current": None}

    # Need indicator_aggregate before triggers
    indicator_aggregate = _aggregate_indicators(recent)

    # 6. Tail risk probabilities
    try:
        tail_risk = compute_tail_risk(
            scored_auctions, composite_history, tenor_decomp,
            historical_analog, cross_signals,
        )
    except Exception as e:
        print(f"[v2] tail_risk error: {e}")
        tail_risk = {}

    # 7. Triggers
    try:
        triggers = build_triggers(scored_auctions, indicator_aggregate, composite_history)
    except Exception as e:
        print(f"[v2] triggers error: {e}")
        triggers = []

    v2_elapsed = round(time.time() - v2_start, 2)
    print(f"[auction-crisis] v2 elapsed: {v2_elapsed}s")

    # Build report
    report = {
        "schema_version": "2.0",  # v2: full institutional-grade expansion (ops/1089)
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_sec": round(time.time() - t0, 2),
        "elapsed_v2_sec": v2_elapsed,
        "regime": regime,
        "composite_score": round(recent_composite, 1),
        "interpretation": interp,
        "n_recent_auctions_14d": len(recent),
        "issuance_anomaly": issuance,
        "fed_funds_rate": fed_rate,

        # Freshness tracking — confirms wiring is pulling latest data
        "freshness": {
            "latest_auction_date": latest_auction_iso,
            "latest_cusip": latest_cusip,
            "hours_since_latest_auction": hours_since_latest,
            "previous_latest_cusip": previous_latest_cusip,
            "is_new_auction_this_run": is_new_auction_this_run,
            "n_total_auctions_pulled": len(raw),
            "data_window_start": start.strftime("%Y-%m-%d"),
            "data_window_end": end.strftime("%Y-%m-%d"),
            "fetched_via": "https://api.fiscaldata.treasury.gov (no-cache headers, format=json)",
        },

        # Last 10 auctions with scores
        "recent_auctions": scored_auctions[:10],

        # Aggregate indicator counts: how many of last 14d auctions fired each pattern
        "indicator_aggregate_14d": indicator_aggregate,

        # Historical reference: where do current readings sit vs. crisis benchmarks?
        "historical_reference": CRISIS_REFERENCE,

        # ════════════ V2 EXPANSION (ops/1089) ════════════
        # Per-tenor stress breakdown — shows WHERE in the curve stress concentrates.
        "tenor_decomposition": tenor_decomp,

        # Next 30 days of upcoming Treasury auctions with per-auction
        # forward stress forecasts (offering size, tenor stress, contagion).
        "forward_calendar": forward_calendar,

        # Cosine-similarity match of current crisis vector to 9 historical
        # anchor auctions, with "what happened next" forward implications.
        "historical_analog": historical_analog,

        # FRED-sourced corroborating signals: repo stress (SOFR-IORB), USD
        # strength (DTWEXBGS), curve slope (T10Y2Y), and forward inflation.
        "cross_signals": cross_signals,

        # 30-day rolling composite score time series + detected regime
        # changeover points.
        "composite_history": composite_history,

        # Forward-looking probability estimates: failed-auction risk,
        # regime escalation risk, supply-driven volatility risk.
        "tail_risk": tail_risk,

        # Specific, named conditions that would flip the regime, with
        # current value, threshold, and recommended action.
        "triggers": triggers,
        # ════════════════════════════════════════════════════

        "data_source": "https://api.fiscaldata.treasury.gov (auctions_query) + treasurydirect.gov + FRED",
        "methodology": (
            "Crisis pattern signatures extracted from 9 historical auction PDFs "
            "covering 2008-09-17 through 2024-10-09. Each live auction scored 0-100 "
            "against 6 patterns. v2 adds: tenor-level decomposition, 30-day forward "
            "calendar with per-auction stress forecasts, cosine-similarity matching "
            "vs 9 historical crisis anchors, 4 corroborating FRED signals (repo, USD, "
            "curve, inflation), 30-day composite history, 3 forward-looking tail risk "
            "probabilities, and named actionable triggers."
        ),
    }

    body = json.dumps(report, default=str, indent=2)
    s3.put_object(
        Bucket=S3_BUCKET, Key=S3_KEY, Body=body,
        ContentType="application/json", CacheControl="max-age=600",
    )
    archive_key = f"data/archive/auction-crisis/{datetime.now(timezone.utc).strftime('%Y%m%d')}.json"
    s3.put_object(Bucket=S3_BUCKET, Key=archive_key, Body=body, ContentType="application/json")

    summary = {
        "status": "ok",
        "elapsed_sec": report["elapsed_sec"],
        "regime": regime,
        "composite_score": report["composite_score"],
        "n_recent": len(recent),
        "issuance_anomaly_pct": issuance.get("pct_above_baseline") if issuance else None,
        "latest_auction_date": latest_auction_iso,
        "latest_cusip": latest_cusip,
        "hours_since_latest_auction": hours_since_latest,
        "is_new_auction_this_run": is_new_auction_this_run,
    }
    print(f"[auction-crisis] done: {summary}")
    return {"statusCode": 200, "body": json.dumps(summary)}


def _aggregate_indicators(recent):
    """Count how many recent auctions fired each indicator at score >= 50."""
    out = {}
    for a in recent:
        for name, score in a.get("indicator_scores", {}).items():
            if score >= 50:
                out.setdefault(name, {"n_fired": 0, "max_score": 0, "auctions": []})
                out[name]["n_fired"] += 1
                out[name]["max_score"] = max(out[name]["max_score"], score)
                out[name]["auctions"].append({
                    "date": a.get("auction_date"),
                    "term": a.get("security_term"),
                    "score": score,
                })
    return out
