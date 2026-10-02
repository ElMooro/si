"""justhodl-usaspending: USAspending.gov federal contract awards -> S3.

FREE API, no key required.
POSTs https://api.usaspending.gov/api/v2/search/spending_by_award/ for contract
award types (A, B, C, D) over the last 5 years, pages results (limit 100/page,
capped at ~50 pages to stay inside Lambda time), maps recipient names to major
government-contractor tickers, and writes data/usaspending-contracts.json to
s3://justhodl-dashboard-live.

FAIL-SOFT: everything is wrapped in try/except; the handler never crashes and
always writes the artifact, even if partial or empty.
"""

import boto3
import datetime
import json
import urllib.error
import urllib.request

BUCKET = "justhodl-dashboard-live"
S3_KEY = "data/usaspending-contracts.json"
API_URL = "https://api.usaspending.gov/api/v2/search/spending_by_award/"
PAGE_LIMIT = 100
MAX_PAGES = 50
HTTP_TIMEOUT = 40

# recipient-name fragment (lowercase) -> ticker. Matched case-insensitively via
# "fragment in recipient_name.lower()", with longest fragments checked first so
# specific names win (e.g. "leonardo drs" before a generic "drs"-style hit).
CONTRACTOR_TICKERS = {
    "lockheed martin": "LMT",
    "aerojet rocketdyne": "LMT",
    "raytheon": "RTX",
    "pratt & whitney": "RTX",
    "collins aerospace": "RTX",
    "northrop grumman": "NOC",
    "orbital atk": "NOC",
    "general dynamics": "GD",
    "gulfstream aerospace": "GD",
    "boeing": "BA",
    "spirit aerosystems": "BA",
    "palantir": "PLTR",
    "leidos": "LDOS",
    "huntington ingalls": "HII",
    "l3harris": "LHX",
    "harris corporation": "LHX",
    "science applications international": "SAIC",
    "caci": "CACI",
    "bae systems": "BAESY",
    "general electric": "GE",
    "ge aviation": "GE",
    "honeywell": "HON",
    "textron": "TXT",
    "bell helicopter": "TXT",
    "oshkosh": "OSK",
    "booz allen": "BAH",
    "accenture": "ACN",
    "kbr": "KBR",
    "jacobs": "J",
    "amentum": "AMTM",
    "dyncorp": "AMTM",
    "parsons": "PSN",
    "maximus": "MMS",
    "serco": "SRP",
    "cgi federal": "GIB",
    "v2x": "VVX",
    "vectrus": "VVX",
    "leonardo drs": "DRS",
    "elbit systems": "ESLT",
    "bwx technologies": "BWXT",
    "fluor": "FLR",
    "aecom": "ACM",
    "tetra tech": "TTEK",
    "icf international": "ICFI",
    "curtiss-wright": "CW",
    "parker hannifin": "PH",
    "moog": "MOG.A",
    "kratos": "KTOS",
    "mercury systems": "MRCY",
    "aerovironment": "AVAV",
    "axon": "AXON",
    "motorola": "MSI",
    "microsoft": "MSFT",
    "amazon": "AMZN",
    "google": "GOOGL",
    "alphabet": "GOOGL",
    "oracle": "ORCL",
    "ibm": "IBM",
    "hewlett packard": "HPE",
    "dell": "DELL",
    "salesforce": "CRM",
    "servicenow": "NOW",
    "crowdstrike": "CRWD",
    "at&t": "T",
    "verizon": "VZ",
    "fedex": "FDX",
    "united parcel": "UPS",
    "waste management": "WM",
    "ameresco": "AMRC",
    "triumph group": "TGI",
    "rolls-royce": "RYCEY",
}

_SORTED_FRAGMENTS = sorted(CONTRACTOR_TICKERS.keys(), key=len, reverse=True)


def _map_ticker(recipient_name):
    """Map a recipient name to a ticker, or None if no known contractor matches."""
    if not recipient_name:
        return None
    lowered = recipient_name.lower()
    for frag in _SORTED_FRAGMENTS:
        if frag in lowered:
            return CONTRACTOR_TICKERS[frag]
    return None


def _num(value):
    try:
        return float(value) if value is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


def _fetch_page(page, start_date, end_date):
    payload = {
        "filters": {
            "award_type_codes": ["A", "B", "C", "D"],
            "time_period": [{"start_date": start_date, "end_date": end_date}],
        },
        "fields": [
            "Award ID",
            "Recipient Name",
            "Award Amount",
            "Awarding Agency",
            "Awarding Sub Agency",
            "Start Date",
            "NAICS Code",
            "Award Type",
            "Description",
        ],
        "limit": PAGE_LIMIT,
        "page": page,
        "sort": "Award Amount",
        "order": "desc",
    }
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        API_URL,
        data=data,
        headers={
            "Content-Type": "application/json",
            "User-Agent": "justhodl-ai/1.0 (data pipeline)",
        },
    )
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _collect(errors):
    """Fetch up to MAX_PAGES of contract awards over the last 5 years."""
    today = datetime.date.today()
    start_date = (today - datetime.timedelta(days=5 * 365)).isoformat()
    end_date = today.isoformat()
    awards = []
    pages_fetched = 0
    try:
        for page in range(1, MAX_PAGES + 1):
            body = _fetch_page(page, start_date, end_date)
            results = body.get("results") or []
            pages_fetched += 1
            for r in results:
                desc = (r.get("Description") or "")[:220]
                awards.append(
                    {
                        "award_id": r.get("Award ID"),
                        "recipient": r.get("Recipient Name"),
                        "amount": _num(r.get("Award Amount")),
                        "agency": r.get("Awarding Agency"),
                        "sub_agency": r.get("Awarding Sub Agency"),
                        "award_date": r.get("Start Date"),
                        "naics": r.get("NAICS Code"),
                        "award_type": r.get("Award Type"),
                        "description": desc,
                        "ticker": _map_ticker(r.get("Recipient Name")),
                    }
                )
            meta = body.get("page_metadata") or {}
            if not meta.get("hasNext") or len(results) < PAGE_LIMIT:
                break
    except Exception as e:  # fail-soft: keep whatever we got
        errors.append("usaspending fetch stopped early after %d pages: %s" % (pages_fetched, e))
    return awards, pages_fetched, start_date, end_date


def _by_ticker(awards):
    agg = {}
    for a in awards:
        t = a.get("ticker")
        if not t:
            continue
        entry = agg.setdefault(t, {"total_awards": 0, "total_value": 0.0, "recent_awards": []})
        entry["total_awards"] += 1
        entry["total_value"] += a.get("amount") or 0.0
        entry["recent_awards"].append(a)
    for t, entry in agg.items():
        entry["recent_awards"] = sorted(
            entry["recent_awards"], key=lambda x: x.get("award_date") or "", reverse=True
        )[:10]
        entry["total_value"] = round(entry["total_value"], 2)
    return agg


def lambda_handler(event, context):
    errors = []
    awards = []
    pages_fetched = 0
    start_date = end_date = None
    try:
        awards, pages_fetched, start_date, end_date = _collect(errors)
    except Exception as e:
        errors.append("collect failed: %s" % e)

    output = {
        "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "window": {
            "years": 5,
            "start_date": start_date,
            "end_date": end_date,
            "pages_fetched": pages_fetched,
        },
        "award_count": len(awards),
        "awards": awards,
        "by_ticker": _by_ticker(awards),
    }
    if errors:
        output["errors"] = errors

    try:
        boto3.client("s3").put_object(
            Bucket=BUCKET,
            Key=S3_KEY,
            Body=json.dumps(output),
            ContentType="application/json",
        )
        output["s3"] = "s3://%s/%s" % (BUCKET, S3_KEY)
    except Exception as e:
        output["s3_error"] = str(e)
        errors.append("s3 write failed: %s" % e)
        output["errors"] = errors

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "ok": True,
                "s3_key": S3_KEY,
                "award_count": len(awards),
                "tickers_mapped": len(output["by_ticker"]),
                "errors": errors,
            }
        ),
    }
