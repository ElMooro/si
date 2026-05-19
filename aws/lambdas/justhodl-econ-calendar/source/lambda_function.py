"""justhodl-econ-calendar -- the forward economic release calendar.

The single most-watched screen on a real macro desk is the economic
calendar: what scheduled data prints this week, what the consensus
expects, and -- once it prints -- how far the actual ran from
consensus. The platform had FOMC meetings and Treasury auctions (in
catalyst-calendar) and a backward-looking surprise diffusion index
(macro-surprise), but not the calendar itself: the forward schedule of
CPI, Nonfarm Payrolls, PCE, ISM, JOLTS, retail sales, GDP, PPI,
jobless claims and the rest, each with a real consensus estimate.

This engine is that screen. Daily it pulls the economic calendar from
FMP across a trailing-and-forward window and assembles:

  * UPCOMING -- the forward schedule, every Medium/High-impact release
    with its date, consensus estimate and prior;
  * RECENT  -- releases that have printed, with the actual, the
    consensus it was measured against, and the surprise;
  * NEXT MAJOR -- the soonest tier-one US market-mover and a countdown;
  * a recent surprise tally, bucketed by data family (inflation,
    labour, growth, housing, sentiment), so the desk can see at a
    glance which side of the economy is running hot or cold.

It is a calendar and a surprise tape -- it does not re-derive the
macro-surprise index, which remains the aggregate read.
"""
import json
import os
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta

import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/econ-calendar.json"
SCHEMA = "1.0"
FMP_KEY = os.environ.get("FMP_KEY", "")

LOOKBACK_DAYS = 12
LOOKAHEAD_DAYS = 24

# Country codes / names kept -- US leads, plus the majors a US desk watches.
KEEP_COUNTRIES = {
    "US": "US", "USA": "US", "UNITED STATES": "US",
    "EA": "Euro Area", "EMU": "Euro Area", "EUR": "Euro Area",
    "EURO AREA": "Euro Area", "EUROZONE": "Euro Area",
    "DE": "Germany", "GERMANY": "Germany",
    "CN": "China", "CHINA": "China",
    "GB": "UK", "UK": "UK", "UNITED KINGDOM": "UK",
    "JP": "Japan", "JAPAN": "Japan",
}

# Tier-one US market-movers -- substring match, case-insensitive.
TIER1 = ["nonfarm payroll", "non-farm payroll", "cpi", "core cpi", "pce",
         "core pce", "fed interest rate", "fomc", "federal funds",
         "interest rate decision", "gdp", "ism manufacturing",
         "ism services", "ism non-manufacturing", "unemployment rate",
         "retail sales", "ppi", "initial jobless claims",
         "jolts", "average hourly earnings"]

FAMILIES = [
    ("INFLATION", ["cpi", "pce", "ppi", "inflation", "price index",
                   "import price", "export price"]),
    ("LABOR", ["payroll", "unemployment", "jobless", "jolts",
               "hourly earnings", "employment", "adp", "challenger",
               "labor", "labour"]),
    ("GROWTH", ["gdp", "ism", "pmi", "industrial production",
                "durable goods", "factory orders", "retail sales",
                "trade balance", "wholesale", "business inventories"]),
    ("HOUSING", ["housing", "home sales", "building permits", "mortgage",
                 "case-shiller", "case shiller", "construction spending",
                 "nahb"]),
    ("SENTIMENT", ["sentiment", "confidence", "michigan", "sentix",
                   "ifo", "zew"]),
]

s3 = boto3.client("s3", region_name=REGION)


# --------------------------------------------------------------------------
def num(v):
    """Robust numeric parse -- strips %, commas and K/M/B/T suffixes."""
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().replace(",", "").replace("%", "")
    if not s or s in ("-", "n/a", "N/A"):
        return None
    mult = 1.0
    if s and s[-1] in "KkMmBbTt":
        mult = {"k": 1e3, "m": 1e6, "b": 1e9, "t": 1e12}[s[-1].lower()]
        s = s[:-1]
    try:
        return float(s) * mult
    except Exception:
        return None


def norm_impact(v):
    s = str(v or "").strip().lower()
    if s in ("high", "3"):
        return "HIGH"
    if s in ("medium", "moderate", "2"):
        return "MEDIUM"
    if s in ("low", "1"):
        return "LOW"
    return "NONE"


def norm_country(v):
    s = str(v or "").strip().upper()
    return KEEP_COUNTRIES.get(s)


def family_of(event_name):
    n = (event_name or "").lower()
    for fam, kws in FAMILIES:
        if any(k in n for k in kws):
            return fam
    return "OTHER"


def is_tier1(event_name, country):
    if country != "US":
        return False
    n = (event_name or "").lower()
    return any(k in n for k in TIER1)


def fmp_calendar(d_from, d_to):
    if not FMP_KEY:
        return [], "missing_fmp_key"
    url = ("https://financialmodelingprep.com/stable/economic-calendar"
           "?from=%s&to=%s&apikey=%s"
           % (d_from, d_to, urllib.parse.quote(FMP_KEY)))
    try:
        req = urllib.request.Request(
            url, headers={"User-Agent": "justhodl-econ-calendar/1.0"})
        with urllib.request.urlopen(req, timeout=20) as r:
            data = json.loads(r.read())
        if isinstance(data, list):
            return data, "ok"
        if isinstance(data, dict) and data.get("Error Message"):
            return [], "fmp_error: " + str(data.get("Error Message"))[:120]
        return [], "unexpected_shape"
    except Exception as e:
        return [], "%s: %s" % (type(e).__name__, e)


# --------------------------------------------------------------------------
def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)
    today = now.date()
    d_from = (today - timedelta(days=LOOKBACK_DAYS)).isoformat()
    d_to = (today + timedelta(days=LOOKAHEAD_DAYS)).isoformat()

    raw, src_status = fmp_calendar(d_from, d_to)

    events = []
    for row in raw:
        if not isinstance(row, dict):
            continue
        country = norm_country(row.get("country"))
        if country is None:
            continue
        impact = norm_impact(row.get("impact"))
        if impact not in ("HIGH", "MEDIUM"):
            continue
        name = str(row.get("event") or "").strip()
        if not name:
            continue
        date_raw = str(row.get("date") or "")
        day = date_raw[:10]
        if len(day) != 10:
            continue
        tm = date_raw[11:16] if len(date_raw) >= 16 else None

        prev = num(row.get("previous"))
        est = num(row.get("estimate"))
        act = num(row.get("actual"))
        released = act is not None

        surprise = None
        vs_consensus = None
        if released and est is not None:
            surprise = round(act - est, 4)
            if surprise > 1e-9:
                vs_consensus = "ABOVE"
            elif surprise < -1e-9:
                vs_consensus = "BELOW"
            else:
                vs_consensus = "IN-LINE"
        vs_prior = None
        if released and prev is not None:
            if act > prev + 1e-9:
                vs_prior = "UP"
            elif act < prev - 1e-9:
                vs_prior = "DOWN"
            else:
                vs_prior = "FLAT"

        events.append({
            "date": day, "time_utc": tm, "country": country,
            "event": name, "impact": impact,
            "family": family_of(name), "tier1": is_tier1(name, country),
            "previous": prev, "consensus": est, "actual": act,
            "released": released, "surprise": surprise,
            "vs_consensus": vs_consensus, "vs_prior": vs_prior,
            "raw_previous": row.get("previous"),
            "raw_estimate": row.get("estimate"),
            "raw_actual": row.get("actual"),
            "unit": row.get("unit"),
        })

    today_iso = today.isoformat()
    week_iso = (today + timedelta(days=7)).isoformat()

    upcoming = sorted(
        [e for e in events if e["date"] >= today_iso and not e["released"]],
        key=lambda e: (e["date"], e["time_utc"] or "", not e["tier1"]))
    recent = sorted(
        [e for e in events if e["released"]],
        key=lambda e: (e["date"], e["time_utc"] or ""), reverse=True)
    this_week = [e for e in upcoming if e["date"] <= week_iso]

    # next major US tier-one release + countdown
    next_major = None
    for e in upcoming:
        if e["tier1"]:
            try:
                dd = (datetime.fromisoformat(e["date"]).date()
                      - today).days
            except Exception:
                dd = None
            next_major = {
                "event": e["event"], "date": e["date"],
                "time_utc": e["time_utc"], "consensus": e["consensus"],
                "previous": e["previous"], "family": e["family"],
                "days_until": dd,
            }
            break

    # recent surprise tally -- factual counts, bucketed by data family
    recent_scored = [e for e in recent if e["vs_consensus"] in
                     ("ABOVE", "BELOW", "IN-LINE")]
    tally = {"above": 0, "below": 0, "in_line": 0, "total": 0}
    by_family = {}
    for e in recent_scored:
        tally["total"] += 1
        key = {"ABOVE": "above", "BELOW": "below",
               "IN-LINE": "in_line"}[e["vs_consensus"]]
        tally[key] += 1
        fam = e["family"]
        bf = by_family.setdefault(fam, {"above": 0, "below": 0,
                                        "in_line": 0})
        bf[key] += 1

    # headline
    n_week = len(this_week)
    n_week_t1 = sum(1 for e in this_week if e["tier1"])
    if src_status != "ok":
        headline = ("Economic calendar feed unavailable (%s) -- the "
                    "schedule could not be loaded this run." % src_status)
    elif next_major:
        du = next_major["days_until"]
        when = ("today" if du == 0 else "tomorrow" if du == 1
                else "in %d days" % du if du is not None else "soon")
        headline = ("Next major US release: %s %s. %d release(s) on the "
                    "calendar this week, %d tier-one."
                    % (next_major["event"], when, n_week, n_week_t1))
    else:
        headline = ("%d Medium/High-impact release(s) on the calendar this "
                    "week. No tier-one US market-mover in the next window."
                    % n_week)

    out = {
        "schema_version": SCHEMA,
        "engine": "justhodl-econ-calendar",
        "generated_at": now.isoformat(),
        "build_seconds": round(time.time() - t0, 2),
        "data_source": "FMP economic-calendar",
        "feed_status": src_status,
        "window": {"from": d_from, "to": d_to},

        "headline": headline,
        "next_major": next_major,

        "counts": {
            "upcoming": len(upcoming),
            "recent_released": len(recent),
            "this_week": n_week,
            "this_week_tier1": n_week_t1,
        },
        "this_week": this_week,
        "upcoming": upcoming[:80],
        "recent": recent[:60],

        "recent_surprise_tally": tally,
        "recent_surprise_by_family": by_family,

        "how_to_read": (
            "The forward economic release calendar -- the desk ECO screen. "
            "UPCOMING is the schedule with the market's consensus estimate "
            "and the prior; RECENT is what has printed, with the actual and "
            "the surprise versus consensus. Tier-one flags the genuine US "
            "market-movers (payrolls, CPI, PCE, ISM, GDP, the Fed). The "
            "surprise tally is a factual count of beats and misses by data "
            "family -- it is not the macro-surprise index, which remains "
            "the aggregate diffusion read."),
        "disclaimer": (
            "Consensus and actuals as published via FMP. Research and "
            "education only, not investment advice."),
    }

    try:
        s3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                      Body=json.dumps(out, default=str).encode("utf-8"),
                      ContentType="application/json")
    except Exception as e:
        print("output write fail: %s" % e)

    return {"statusCode": 200, "body": json.dumps({
        "ok": src_status == "ok", "feed_status": src_status,
        "upcoming": len(upcoming), "recent": len(recent),
        "this_week": n_week,
        "next_major": next_major["event"] if next_major else None,
        "surprise_tally": tally})}
