"""
justhodl-catalyst-calendar — Forward-looking market catalyst aggregator.

WHAT IT AGGREGATES (60-day forward window)
───────────────────────────────────────────
  1. FOMC meetings (8/year) — 2026 dates from Fed schedule
  2. Treasury auctions — treasurydirect.gov public API
  3. Earnings — re-uses existing data/earnings-tracker.json (no duplication)
  4. Triple/quad-witching — third Friday of Mar/Jun/Sep/Dec
  5. S&P 500 quarterly rebalance — third Friday of Mar/Jun/Sep/Dec (after close)
  6. Russell reconstitution — last Friday of June (rebalance) + announcement (~mid-June)
  7. Bank earnings cluster — second week of Jan/Apr/Jul/Oct (JPM/BAC/C/WFC)

PAST WINDOW (45-day trailing, added 2026-07-25)
────────────────────────────────────────────────
  8. EARNINGS_ACTUAL — recent_earnings_events() surfaces earnings-tracker's
     recent_results_30d as past-dated events: real Benzinga actual EPS/revenue,
     surprise %, and post-earnings 1d/5d/20d returns/PEAD label. Fixes a real
     gap found investigating a semi-sector flow/price divergence: this feed
     was forward-only, so "what already happened" had nowhere to look even
     though the data existed one S3 key over. No new external dependency —
     reuses data the fleet already computes via justhodl-earnings-tracker.

OUTPUT
──────
  data/catalyst-calendar.json
  {
    as_of, window_days,
    events: [
      {
        date,             # YYYY-MM-DD
        time,             # HH:MM UTC if known, else null
        type,             # FOMC | AUCTION | EARNINGS | EARNINGS_ACTUAL | WITCHING
                          # | REBALANCE | BANK_EARNINGS | FDA
        title,            # short label
        subtitle,         # detail
        impact,           # HIGH | MEDIUM | LOW
        source,           # primary source
        url,              # link to authoritative info
        days_to,          # countdown (NEGATIVE for EARNINGS_ACTUAL — already happened)
        size_billions,    # for auctions
        consensus,        # for econ data
        previous          # for econ data
        # EARNINGS_ACTUAL rows additionally carry: eps_actual, eps_estimate,
        # eps_surprise_pct, revenue_actual, revenue_surprise_pct, pead_label,
        # pead_score, return_1d_pct, return_5d_pct, return_20d_pct
      }, ...
    ],
    by_type:    {FOMC: n, AUCTION: n, ..., EARNINGS_ACTUAL: n},
    high_impact_next_7d:   N,
    high_impact_next_30d:  N
  }

INSTITUTIONAL-GRADE SAFEGUARDS
───────────────────────────────
  ✓ Multiple sources with fallback — Treasury API + hardcoded FOMC + computed dates
  ✓ Ratelimit-safe — Treasury API has 1 call, no rate concerns
  ✓ Date validation — pytz-style timezone handling, YYYY-MM-DD only
  ✓ Impact scoring — ratings reflect typical market reaction (FOMC >> auction > rebalance)
  ✓ De-duplication — events on same date with same type get merged
  ✓ Failure-safe — if a source fails, skip that source, keep others
  ✓ Bank earnings cluster pre-computed (don't fetch — JPM/BAC/C/WFC report ~same days)
"""

try:  # bank-first FMP (aws/shared/fmp_bankfirst): /stable answered from the warehouse banks when fresh, else live
    from fmp_bankfirst import install as _fmp_bankfirst_install
    _fmp_bankfirst_install()
except Exception:  # noqa: BLE001 - the shim never blocks the engine
    pass
import json
import os
import time
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone

import boto3
from managed_secret import managed_secret  # audit 2026-09-08 INST-06: no literal credentials

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY_OUT = os.environ.get("S3_KEY_OUT", "data/catalyst-calendar.json")
WINDOW_DAYS = int(os.environ.get("WINDOW_DAYS", "60"))

S3 = boto3.client("s3", region_name=REGION)


# ─── FOMC dates (publicly known in advance — Fed publishes 1-2 years ahead) ───
# Source: federalreserve.gov/monetarypolicy/fomccalendars.htm
# These are the official 2026 dates published by the Fed.
FOMC_DATES_2026 = [
    ("2026-01-27", "2026-01-28", False),  # (start, end, has_dot_plot)
    ("2026-03-17", "2026-03-18", True),
    ("2026-04-28", "2026-04-29", False),
    ("2026-06-09", "2026-06-10", True),
    ("2026-07-28", "2026-07-29", False),
    ("2026-09-15", "2026-09-16", True),
    ("2026-10-27", "2026-10-28", False),
    ("2026-12-15", "2026-12-16", True),
]
# 2027: the Fed's revised tentative schedule, press release of 2026-11-12
# (federalreserve.gov/newsevents/pressreleases/monetary20261112a.htm). The October 26-27 meeting is a strategy
# discussion with no policy decision; the year's last decision moved from December 7-8 to November 16-17.
# Whether a meeting carries projections is known for none of them yet, so none is claimed.
# (start, end, has_dot_plot, policy_decision)
FOMC_DATES_2027 = [
    ("2027-01-26", "2027-01-27", None, True),
    ("2027-03-16", "2027-03-17", None, True),
    ("2027-04-27", "2027-04-28", None, True),
    ("2027-06-08", "2027-06-09", None, True),
    ("2027-07-27", "2027-07-28", None, True),
    ("2027-09-14", "2027-09-15", None, True),
    ("2027-10-26", "2027-10-27", None, False),
    ("2027-11-16", "2027-11-17", None, True),
]
FOMC_DATES_2028 = [
    ("2028-01-25", "2028-01-26", None, True),
]

# Treasury auction date can be looked up dynamically (see fetch_treasury_auctions)
# but for fallback / verification, here's the typical weekly cadence:
#   Bills (4w/8w/13w/26w):  Mon/Tue auctions, settle Thu
#   Notes (2y/3y/5y/7y/10y): mid-month
#   30y bond: 2nd week of the month
#   TIPS: quarterly


# ─── Source: Treasury auctions ──────────────────────────────────────────────
def fetch_treasury_auctions(window_days):
    """Pull announced + future auctions from Treasury Direct.
    Returns list of dicts: {date, type, term, size_billions, ...}"""
    url = "https://www.treasurydirect.gov/TA_WS/securities/announced?format=json"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "justhodl-catalyst/1.0"})
        with urllib.request.urlopen(req, timeout=20) as r:
            data = json.loads(r.read().decode("utf-8"))
    except Exception as e:
        print(f"[catalyst] Treasury fetch failed: {e}")
        return []

    today = date.today()
    cutoff = today + timedelta(days=window_days)
    events = []
    for entry in data:
        try:
            issue_date_str = entry.get("issueDate") or entry.get("auctionDate")
            if not issue_date_str:
                continue
            auction_date_str = entry.get("auctionDate", "")[:10]
            if not auction_date_str:
                continue
            ad = datetime.strptime(auction_date_str, "%Y-%m-%d").date()
            if ad < today or ad > cutoff:
                continue
        except (ValueError, TypeError):
            continue

        # Size in millions → billions
        size_offered = entry.get("offeringAmt") or "0"
        try:
            size_b = float(size_offered) / 1000.0
        except (ValueError, TypeError):
            size_b = None

        sec_type = (entry.get("securityType") or "").strip()
        sec_term = (entry.get("securityTerm") or "").strip()

        # Only flag NOTE / BOND / TIPS as material (bills are routine + small impact)
        impact = "MEDIUM" if sec_type in ("Note", "Bond", "TIPS") else "LOW"
        if size_b and size_b >= 50:
            impact = "MEDIUM" if impact == "LOW" else "HIGH"

        events.append({
            "date": auction_date_str,
            "time": None,
            "type": "AUCTION",
            "title": f"{sec_term} {sec_type} auction",
            "subtitle": f"Size ~${size_b:.0f}B" if size_b else "Size TBD",
            "impact": impact,
            "source": "TreasuryDirect",
            "url": "https://www.treasurydirect.gov/auctions/upcoming/",
            "size_billions": size_b,
            "cusip": entry.get("cusip"),
        })
    return events


# ─── Source: FOMC ────────────────────────────────────────────────────────────
def fomc_events(window_days):
    today = date.today()
    cutoff = today + timedelta(days=window_days)
    events = []
    for row in FOMC_DATES_2026 + FOMC_DATES_2027 + FOMC_DATES_2028:
        start, end, has_dots = row[0], row[1], row[2]
        decision = row[3] if len(row) > 3 else True
        try:
            end_date = datetime.strptime(end, "%Y-%m-%d").date()
        except ValueError:
            continue
        if end_date < today or end_date > cutoff:
            continue
        events.append({
            "date": end,
            "time": "18:00" if decision else None,  # ~2:00 PM ET = 18:00 UTC (rate decision release)
            "type": "FOMC",
            "title": "FOMC Statement" if decision else "FOMC meeting (strategy only, no policy decision)",
            "subtitle": ("Monetary policy strategy discussion" if not decision
                         else "Rate decision + dot plot + presser" if has_dots
                         else "Rate decision + presser"),
            "impact": "HIGH" if decision else "MEDIUM",
            "policy_decision": decision,
            "meeting_start": start,
            "source": "Federal Reserve",
            "url": "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm",
            "size_billions": None,
            "has_dot_plot": has_dots,
        })
    return events


# ─── Source: Witching dates ──────────────────────────────────────────────────
def witching_events(window_days):
    """Triple/quad-witching: third Friday of Mar/Jun/Sep/Dec.
    Quad-witching = stock options + index options + stock futures + index futures.
    Major liquidity event — institutional rebalances + opex cluster."""
    today = date.today()
    cutoff = today + timedelta(days=window_days)
    events = []
    # Compute next 4 quad-witching days
    for year in (today.year, today.year + 1):
        for month in (3, 6, 9, 12):
            # Find third Friday
            d = date(year, month, 1)
            while d.weekday() != 4:  # Friday = 4
                d += timedelta(days=1)
            d += timedelta(days=14)  # 1st Friday + 14 = 3rd Friday
            if today <= d <= cutoff:
                events.append({
                    "date": d.isoformat(),
                    "time": "21:00",  # 4:00 PM ET = 21:00 UTC (close)
                    "type": "WITCHING",
                    "title": "Quad-Witching",
                    "subtitle": "Stock + index options + futures expire — high volume",
                    "impact": "HIGH",
                    "source": "CBOE/CME",
                    "url": None,
                    "size_billions": None,
                })
    return events


# ─── Source: S&P quarterly rebalance ─────────────────────────────────────────
def sp_rebalance_events(window_days):
    """S&P 500 quarterly rebalance — third Friday of Mar/Jun/Sep/Dec close.
    Index funds force-trade ~$1T+ AUM mirroring changes. Same day as witching."""
    # Combine with witching above is fine — separate event for clarity
    today = date.today()
    cutoff = today + timedelta(days=window_days)
    events = []
    for year in (today.year, today.year + 1):
        for month in (3, 6, 9, 12):
            d = date(year, month, 1)
            while d.weekday() != 4:
                d += timedelta(days=1)
            d += timedelta(days=14)
            if today <= d <= cutoff:
                events.append({
                    "date": d.isoformat(),
                    "time": "20:00",  # close
                    "type": "REBALANCE",
                    "title": "S&P 500 Quarterly Rebalance",
                    "subtitle": "Index funds force-trade adds/drops at close",
                    "impact": "MEDIUM",
                    "source": "S&P Dow Jones Indices",
                    "url": "https://www.spglobal.com/spdji/en/indices/equity/sp-500/",
                    "size_billions": None,
                })
    return events


# ─── Source: Bank earnings cluster ──────────────────────────────────────────
def bank_earnings_cluster(window_days):
    """JPM/BAC/C/WFC report 2nd Tue/Wed/Fri of Jan/Apr/Jul/Oct. Major macro signal."""
    today = date.today()
    cutoff = today + timedelta(days=window_days)
    events = []
    for year in (today.year, today.year + 1):
        for month in (1, 4, 7, 10):
            # Find 2nd Friday (typical bank earnings day)
            d = date(year, month, 1)
            while d.weekday() != 4:
                d += timedelta(days=1)
            d += timedelta(days=7)  # 2nd Friday
            if today <= d <= cutoff:
                events.append({
                    "date": d.isoformat(),
                    "time": "12:30",  # ~7:30 AM ET pre-market
                    "type": "BANK_EARNINGS",
                    "title": "Big-Bank Earnings (JPM/C/WFC)",
                    "subtitle": "Usual date (second Friday), not yet announced by the banks — credit conditions, NIM, deposits",
                    "confirmed": False,
                    "impact": "HIGH",
                    "source": "Various",
                    "url": None,
                    "size_billions": None,
                })
    return events


# ─── Source: Earnings — Benzinga precision + FMP breadth (hybrid) ────────────
def recent_earnings_events(lookback_days=45):
    """Surface earnings-tracker's recent_results_30d as PAST catalyst events.

    This is the fix for the gap found 2026-07-25: catalyst-calendar was
    forward-only (earnings_events() above explicitly drops anything with
    ed < today), so a "what already happened" investigation had nowhere to
    look inside this feed even though the data it needed — real Benzinga
    actual-vs-estimate EPS/revenue, surprise %, and post-earnings 1d/5d/20d
    returns — was already being computed one S3 key over, in
    data/earnings-tracker.json's recent_results_30d array. No new external
    API call: this reuses data the fleet already pays for and computes.

    Deliberately a SEPARATE function from earnings_events() (not a shared
    code path) so the well-tested forward-calendar logic is untouched —
    this only ADDS a new event type, never modifies the existing one.
    """
    try:
        obj = S3.get_object(Bucket=BUCKET, Key="data/earnings-tracker.json")
        d = json.loads(obj["Body"].read())
    except Exception as e:
        print(f"[catalyst] recent_earnings_events fetch failed: {e}")
        return []

    today = date.today()
    floor = today - timedelta(days=lookback_days)
    events = []

    def _imp(importance):
        try:
            importance = float(importance or 0)
        except (ValueError, TypeError):
            importance = 0
        if importance >= 4:
            return "HIGH"
        if importance == 3:
            return "MEDIUM"
        return "LOW"

    for r in (d.get("recent_results_30d") or []):
        try:
            fd_str = (r.get("filing_date") or "")[:10]
            if not fd_str:
                continue
            try:
                fd = datetime.strptime(fd_str, "%Y-%m-%d").date()
            except ValueError:
                continue
            if fd < floor or fd > today:
                continue
            tk = r.get("ticker")
            eps_a, eps_e = r.get("eps_actual"), r.get("eps_estimate")
            surprise = r.get("eps_surprise_pct")
            returns = r.get("returns") or {}
            r1d = returns.get("1d")
            beat_txt = (f"EPS {eps_a} vs {eps_e} est ({surprise:+.1f}% surprise)"
                        if surprise is not None else "earnings reported")
            react_txt = f" · 1d reaction {r1d:+.1f}%" if r1d is not None else ""
            events.append({
                "date": fd_str,
                "time": None,
                "type": "EARNINGS_ACTUAL",
                "title": f"{tk or '?'} reported",
                "subtitle": (beat_txt + react_txt)[:140],
                "impact": _imp(r.get("importance")),
                "source": "Benzinga (via Massive) — actual results",
                "url": None,
                "size_billions": None,
                "ticker": tk,
                "period_end": r.get("period_end"),
                "eps_actual": eps_a,
                "eps_estimate": eps_e,
                "eps_surprise_pct": surprise,
                "revenue_actual": r.get("revenue_actual"),
                "revenue_surprise_pct": r.get("revenue_surprise_pct"),
                "pead_label": r.get("pead_label"),
                "pead_score": r.get("pead_score"),
                "return_1d_pct": r1d,
                "return_5d_pct": returns.get("5d"),
                "return_20d_pct": returns.get("20d"),
            })
        except Exception as e:
            print(f"[catalyst] recent earnings row skipped: {e}")
            continue
    return events


def earnings_events(window_days):
    """Hybrid earnings catalysts:
      1) Benzinga forward_calendar (authoritative) — importance 1-5 drives impact,
         BMO/AMC session drives precise timing, consensus EPS attached.
      2) FMP upcoming_14d — breadth supplement for names Benzinga doesn't rate.
    De-duplicated by (ticker, date); Benzinga wins ties."""
    try:
        obj = S3.get_object(Bucket=BUCKET, Key="data/earnings-tracker.json")
        d = json.loads(obj["Body"].read())
    except Exception as e:
        print(f"[catalyst] earnings-tracker fetch failed: {e}")
        return []

    today = date.today()
    cutoff = today + timedelta(days=window_days)
    events = []
    seen = set()

    def _imp(importance):
        try:
            importance = float(importance or 0)
        except (ValueError, TypeError):
            importance = 0
        if importance >= 4:
            return "HIGH"
        if importance == 3:
            return "MEDIUM"
        return "LOW"

    def _num(v):
        try:
            return float(v or 0)
        except (ValueError, TypeError):
            return 0.0

    # 1) Benzinga forward calendar — precise importance + session timing
    for u in (d.get("forward_calendar") or []):
        try:
            ed_str = (u.get("date") or "")[:10]
            if not ed_str:
                continue
            try:
                ed = datetime.strptime(ed_str, "%Y-%m-%d").date()
            except ValueError:
                continue
            if ed < today or ed > cutoff:
                continue
            tk = u.get("ticker")
            key = (tk, ed_str)
            if key in seen:
                continue
            seen.add(key)
            sess = u.get("session") or ""
            fp = f"{u.get('fiscal_period', '')}{u.get('fiscal_year', '')}".strip()
            events.append({
                "date": ed_str,
                "time": "12:30" if sess == "BMO" else ("21:00" if sess == "AMC" else None),
                "type": "EARNINGS",
                "title": f"{tk or '?'} earnings",
                "subtitle": ((u.get("company") or "")[:70]) + (f" · {sess}" if sess and sess != "—" else ""),
                "impact": _imp(u.get("importance")),
                "source": "Benzinga (via Massive)",
                "url": None,
                "size_billions": None,
                "ticker": tk,
                "session": sess or None,
                "importance": int(_num(u.get("importance"))),
                "consensus": u.get("estimated_eps"),
                "fiscal_period": fp or None,
            })
        except Exception as e:
            print(f"[catalyst] earnings row (bz) skipped: {e}")
            continue

    # 2) FMP upcoming_14d — breadth supplement for unrated names
    for u in (d.get("upcoming_14d") or []):
        try:
            ed_str = (u.get("earnings_date", "") or "")[:10]
            if not ed_str:
                continue
            try:
                ed = datetime.strptime(ed_str, "%Y-%m-%d").date()
            except ValueError:
                continue
            if ed < today or ed > cutoff:
                continue
            tk = u.get("ticker")
            key = (tk, ed_str)
            if key in seen:
                continue
            seen.add(key)
            mcap = _num(u.get("market_cap"))
            impact = "HIGH" if mcap >= 100_000_000_000 else ("MEDIUM" if mcap >= 10_000_000_000 else "LOW")
            events.append({
                "date": ed_str,
                "time": (u.get("time") or "").upper() if u.get("time") else None,
                "type": "EARNINGS",
                "title": f"{tk or '?'} earnings",
                "subtitle": (u.get("name") or "")[:80],
                "impact": impact,
                "source": "FMP",
                "url": None,
                "size_billions": None,
                "ticker": tk,
                "consensus": u.get("eps_consensus"),
                "n_estimates": u.get("n_estimates"),
                "market_cap": mcap,
            })
        except Exception as e:
            print(f"[catalyst] earnings row (fmp) skipped: {e}")
            continue
    return events


# ─── Main handler ───────────────────────────────────────────────────────────
FMP_KEY = managed_secret(('FMP_KEY', 'FMP_API_KEY'), ("/justhodl/fmp/api-key",))
GOV_NAME_TO_TICKER = {
    "LOCKHEED MARTIN": "LMT", "RAYTHEON": "RTX", "RTX": "RTX", "BOEING": "BA",
    "GENERAL DYNAMICS": "GD", "NORTHROP GRUMMAN": "NOC", "L3HARRIS": "LHX",
    "HUNTINGTON INGALLS": "HII", "LEIDOS": "LDOS", "BOOZ ALLEN": "BAH",
    "SAIC": "SAIC", "PALANTIR": "PLTR", "ACCENTURE": "ACN", "GENERAL ELECTRIC": "GE",
    "HONEYWELL": "HON", "TEXTRON": "TXT", "CACI": "CACI", "KBR": "KBR",
    "CURTISS-WRIGHT": "CW", "AXON": "AXON", "PALO ALTO": "PANW", "KRATOS": "KTOS",
    "AEROVIRONMENT": "AVAV", "MERCURY SYSTEMS": "MRCY", "V2X": "VVX",
}

def _http(url, data=None, headers=None, t=20):
    try:
        h = {"User-Agent": "JustHodl/1.0", "Content-Type": "application/json"}
        if headers: h.update(headers)
        req = urllib.request.Request(url, data=data, headers=h, method="POST" if data else "GET")
        with urllib.request.urlopen(req, timeout=t) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        print(f"[catalyst] fetch err {url[:48]}: {str(e)[:70]}"); return None

def fda_events(window_days):
    """FDA PDUFA / catalyst events via FMP (graceful if endpoint not on plan)."""
    out = []
    for ep in ("fda-calendar", "pharma-calendar"):
        d = _http(f"https://financialmodelingprep.com/stable/{ep}?apikey={FMP_KEY}")
        if isinstance(d, list) and d:
            for r in d[:120]:
                dt = (r.get("pdufa") or r.get("date") or r.get("catalystDate") or "")[:10]
                if not dt: continue
                out.append({
                    "date": dt, "type": "FDA",
                    "ticker": (r.get("symbol") or r.get("ticker") or "").upper(),
                    "title": (r.get("type") or r.get("catalyst") or "FDA event") + (f" — {r.get('drug')}" if r.get("drug") else ""),
                    "impact": "HIGH", "source": "FMP FDA calendar",
                })
            break
    return out

def gov_contract_events():
    """Recent material federal contract awards via USAspending.gov (free)."""
    end = date.today(); start = end - timedelta(days=30)
    body = json.dumps({
        "filters": {"award_type_codes": ["A", "B", "C", "D"],
                    "time_period": [{"start_date": start.isoformat(), "end_date": end.isoformat(), "date_type": "action_date"}]},
        "fields": ["Recipient Name", "Award Amount", "Awarding Agency", "Action Date", "Start Date", "Base Obligation Date", "Last Modified Date", "Description"],
        "sort": "Award Amount", "order": "desc", "limit": 60, "page": 1,
    }).encode()
    d = _http("https://api.usaspending.gov/api/v2/search/spending_by_award/", data=body)
    out = []
    if isinstance(d, dict):
        for r in (d.get("results") or []):
            amt = r.get("Award Amount") or 0
            if amt < 10_000_000: continue
            recip = (r.get("Recipient Name") or "").upper(); tk = None
            for nm, t in GOV_NAME_TO_TICKER.items():
                if nm in recip: tk = t; break
            if not tk: continue   # only ticker-mapped awards as catalysts
            out.append({
                "date": (r.get("Action Date") or r.get("Last Modified Date") or r.get("Base Obligation Date") or r.get("Start Date") or "")[:10], "type": "GOV_CONTRACT",
                "date_basis": next((f for f in ("Action Date", "Last Modified Date", "Base Obligation Date", "Start Date") if r.get(f)), None),
                "ticker": tk, "title": f"${round(amt/1e6,1)}M federal award · {(r.get('Awarding Agency') or '')[:28]}",
                "impact": "MEDIUM", "source": "USAspending.gov",
            })
    return out


VERSION = "2.0.0"
KEPT = {  # the kept documents the calendar draws on, and how old each may be before it is left out
    "econ": ("data/econ-calendar.json", 48),
    "earnings_wide": ("data/finviz-earnings-calendar.json", 96),
    "ipo": ("data/ipo-pipeline.json", 96),
    "lockup": ("data/lockup-expiration.json", 96),
    "quarter_end": ("data/rebalance-radar.json", 96),
    "russell": ("data/russell-recon-frontrun.json", 96),
}
HOLIDAYS_PREFIX = "data/warm/polygon-bank/market-holidays/"


def kept(key, max_hours, now=None):
    """A kept document and its age in hours, or (None, age, why) when it is missing, unreadable or too old."""
    now = now or datetime.now(timezone.utc)
    try:
        obj = S3.get_object(Bucket=BUCKET, Key=key)
        raw = obj["Body"].read()
        if raw[:2] == b"\x1f\x8b":
            import gzip
            raw = gzip.decompress(raw)
        hours = round((now - obj["LastModified"]).total_seconds() / 3600.0, 1)
        if hours > max_hours:
            return None, hours, "older than %d h" % max_hours
        return json.loads(raw), hours, None
    except Exception as e:  # noqa: BLE001 - one missing document never stops the calendar
        return None, None, "%s: %s" % (type(e).__name__, str(e)[:80])


def kept_holidays(now=None):
    """The newest banked market-holidays file: (rows, key, hours, why)."""
    now = now or datetime.now(timezone.utc)
    try:
        newest = None
        for page in S3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=HOLIDAYS_PREFIX):
            for o in page.get("Contents", []):
                if o["Key"].endswith("/") or "/_" in o["Key"]:
                    continue
                if newest is None or o["LastModified"] > newest["LastModified"]:
                    newest = o
        if newest is None:
            return [], None, None, "no file under " + HOLIDAYS_PREFIX
        doc, hours, why = kept(newest["Key"], 24 * 14, now)
        if doc is None:
            return [], newest["Key"], hours, why
        rows = doc if isinstance(doc, list) else next((doc[k] for k in ("results", "data", "payload", "rows", "items", "body") if isinstance(doc.get(k), list)), [])
        return rows, newest["Key"], hours, None if rows else "no rows in the file"
    except Exception as e:  # noqa: BLE001
        return [], None, None, "%s: %s" % (type(e).__name__, str(e)[:80])


def build(today=None, now=None):
    """The whole calendar as a document. Nothing is written here."""
    import catalyst_sources as CS

    started = time.time()
    today = today or date.today()
    now = now or datetime.now(timezone.utc)
    all_events, src_counts, report = [], {}, []

    def take(name, fetcher, document=None, hours=None, why=None):
        try:
            evs = fetcher() or []
        except Exception as e:  # noqa: BLE001 - a failed source is skipped, the others stand
            print(f"[catalyst] Source {name} failed: {e}")
            evs, why = [], "%s: %s" % (type(e).__name__, str(e)[:80])
        src_counts[name] = src_counts.get(name, 0) + len(evs)
        all_events.extend(evs)
        report.append({"source": name, "events": len(evs), "document": document, "hours_old": hours, "note": why})

    print("[catalyst] Aggregating sources\u2026")
    take("FOMC", lambda: fomc_events(WINDOW_DAYS), "Federal Reserve schedule (in the engine)")
    take("AUCTION", lambda: fetch_treasury_auctions(WINDOW_DAYS), "TreasuryDirect announced auctions")
    take("WITCHING", lambda: witching_events(WINDOW_DAYS), "calendar rule")
    take("REBALANCE", lambda: sp_rebalance_events(WINDOW_DAYS), "calendar rule")
    take("BANK_EARNINGS", lambda: bank_earnings_cluster(WINDOW_DAYS), "calendar rule (usual date, not announced)")
    take("EARNINGS", lambda: earnings_events(WINDOW_DAYS), "data/earnings-tracker.json")
    take("EARNINGS_ACTUAL", lambda: recent_earnings_events(45), "data/earnings-tracker.json")
    take("FDA", lambda: fda_events(WINDOW_DAYS), "FMP FDA calendar")
    take("GOV_CONTRACT", lambda: [e for e in gov_contract_events() if e.get("date") and e["date"] >= (today - timedelta(days=45)).isoformat()], "USAspending.gov award search")

    docs = {name: kept(key, limit, now) for name, (key, limit) in KEPT.items()}
    have = {str(e.get("ticker") or "").upper() for e in all_events if e.get("type") == "EARNINGS"}
    wide = {
        "econ": ("ECON", lambda d: CS.econ_events(d, today, WINDOW_DAYS)),
        "earnings_wide": ("EARNINGS", lambda d: CS.earnings_wide_events(d, today, WINDOW_DAYS, have)),
        "ipo": ("IPO", lambda d: CS.ipo_events(d, today, WINDOW_DAYS)),
        "lockup": ("LOCKUP", lambda d: CS.lockup_events(d, today, WINDOW_DAYS)),
        "quarter_end": ("QUARTER_END", lambda d: CS.quarter_end_events(d, today, WINDOW_DAYS)),
        "russell": ("REBALANCE", lambda d: CS.russell_events(d, today, WINDOW_DAYS)),
    }
    for name, (label, fn) in wide.items():
        doc, hours, why = docs[name]
        take(label, (lambda d=doc, f=fn: f(d) if d is not None else []), KEPT[name][0], hours, why)
    rows, hkey, hhours, hwhy = kept_holidays(now)
    take("MARKET_HOLIDAY", lambda: CS.holiday_events(rows, today, WINDOW_DAYS), hkey or HOLIDAYS_PREFIX, hhours, hwhy)
    take("OPEX", lambda: CS.opex_events(today, WINDOW_DAYS), "calendar rule (third Friday)")
    take("VIX_EXPIRY", lambda: CS.vix_expiry_events(today, WINDOW_DAYS), "calendar rule (30 days before the next third Friday)")

    # Compute days_to + sort
    valid = []
    for e in CS.dedupe(all_events):
        try:
            ed = datetime.strptime(e["date"], "%Y-%m-%d").date()
            e["days_to"] = (ed - today).days
            valid.append(e)
        except (ValueError, KeyError, TypeError):
            pass
    valid.sort(key=lambda e: (e["date"], e.get("type", "")))

    high_impact = [e for e in valid if e.get("impact") == "HIGH"]
    next_7d = sum(1 for e in high_impact if 0 <= e["days_to"] <= 7)
    next_30d = sum(1 for e in high_impact if 0 <= e["days_to"] <= 30)

    by_type = {}
    for e in valid:
        by_type[e["type"]] = by_type.get(e["type"], 0) + 1
    ahead = [e for e in valid if e["days_to"] >= 0]

    return {
        "schema_version": "1.1",
        "method": "catalyst_calendar_v2",
        "version": VERSION,
        "as_of": now.isoformat(timespec="seconds"),
        "window_days": WINDOW_DAYS,
        "n_events": len(valid),
        "n_ahead": len(ahead),
        "n_past": len(valid) - len(ahead),
        "events": valid,
        "by_type": by_type,
        "by_type_ahead": {t: sum(1 for e in ahead if e["type"] == t) for t in sorted({e["type"] for e in ahead})},
        "by_source": src_counts,
        "sources": report,
        "high_impact_next_7d": next_7d,
        "high_impact_next_30d": next_30d,
        "duration_s": round(time.time() - started, 1),
    }


def lambda_handler(event, context):
    payload = build()
    dry = isinstance(event, dict) and bool(event.get("dry_run"))
    if not dry:
        S3.put_object(
            Bucket=BUCKET, Key=S3_KEY_OUT, Body=json.dumps(payload, indent=2, default=str).encode("utf-8"),
            ContentType="application/json", CacheControl="max-age=600",
        )
    print(f"[catalyst] {'DRY RUN' if dry else 'DONE'} in {payload['duration_s']}s \u00b7 {payload['n_events']} events ({payload['n_ahead']} ahead) \u00b7 "
          f"{payload['high_impact_next_7d']} high-impact next 7d \u00b7 sources: {payload['by_source']}")
    return {
        "statusCode": 200,
        "body": json.dumps({
            "ok": True,
            "dry_run": dry,
            "n_events": payload["n_events"],
            "n_ahead": payload["n_ahead"],
            "high_impact_next_7d": payload["high_impact_next_7d"],
            "by_type": payload["by_type"],
            "duration_s": payload["duration_s"],
        }),
    }
