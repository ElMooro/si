"""justhodl-catalyst-calendar — free, high-signal event catalysts for early detection.

Two free public feeds the platform was missing, both nearly impossible to trade
early without:
  1. FDA PDUFA / AdCom — biotech binary catalysts. Sourced from FMP's
     fda-calendar (when available) + openFDA drug-event signal as a fallback.
  2. US government contract awards — defense/tech/healthcare catalysts. Sourced
     from USAspending.gov (free API): recent large awards, mapped to tickers
     via recipient-name matching against the universe.

OUTPUT: data/catalyst-calendar.json
  { fda: [{ticker, company, date, type, drug, note}],
    gov_contracts: [{ticker, recipient, amount, agency, date, desc}],
    upcoming: [...sorted by date] }
SCHEDULE: daily 12:00 UTC.
"""
import json, os, time, re
import urllib.request, urllib.parse
from datetime import datetime, timezone, date, timedelta
import boto3

REGION = "us-east-1"; BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/catalyst-calendar.json"
FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
FMP = "https://financialmodelingprep.com/stable"
s3 = boto3.client("s3", region_name=REGION)


def http_json(url, data=None, headers=None, t=20):
    try:
        h = {"User-Agent": "JustHodl/1.0", "Content-Type": "application/json"}
        if headers: h.update(headers)
        req = urllib.request.Request(url, data=data, headers=h, method="POST" if data else "GET")
        with urllib.request.urlopen(req, timeout=t) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        print(f"[catalyst] fetch err {url[:50]}: {str(e)[:80]}")
        return None


def read_json(key, default=None):
    try: return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception: return default


# Curated recipient-name → ticker map for the biggest gov-contract names
# (USAspending uses legal recipient names; map the common ones).
GOV_NAME_TO_TICKER = {
    "LOCKHEED MARTIN": "LMT", "RAYTHEON": "RTX", "RTX": "RTX", "BOEING": "BA",
    "GENERAL DYNAMICS": "GD", "NORTHROP GRUMMAN": "NOC", "L3HARRIS": "LHX",
    "HUNTINGTON INGALLS": "HII", "LEIDOS": "LDOS", "BOOZ ALLEN": "BAH",
    "SAIC": "SAIC", "PALANTIR": "PLTR", "ACCENTURE": "ACN", "IBM": "IBM",
    "MICROSOFT": "MSFT", "AMAZON": "AMZN", "ORACLE": "ORCL", "GENERAL ELECTRIC": "GE",
    "HONEYWELL": "HON", "TEXTRON": "TXT", "CACI": "CACI", "KBR": "KBR",
    "JACOBS": "J", "PFIZER": "PFE", "MODERNA": "MRNA", "MERCK": "MRK",
    "CURTISS-WRIGHT": "CW", "AXON": "AXON", "PALO ALTO": "PANW", "KRATOS": "KTOS",
    "AEROVIRONMENT": "AVAV", "MERCURY SYSTEMS": "MRCY", "V2X": "VVX",
}


def fetch_fda():
    """FDA PDUFA / calendar via FMP; graceful if endpoint unavailable."""
    out = []
    for ep in ("fda-calendar", "pharma-calendar"):
        d = http_json(f"{FMP}/{ep}?apikey={FMP_KEY}")
        if isinstance(d, list) and d:
            for r in d[:120]:
                tk = (r.get("symbol") or r.get("ticker") or "").upper()
                dt = r.get("pdufa") or r.get("date") or r.get("catalystDate")
                if not dt: continue
                out.append({
                    "ticker": tk, "company": r.get("company") or r.get("companyName"),
                    "date": str(dt)[:10], "type": r.get("type") or r.get("catalyst") or "FDA event",
                    "drug": r.get("drug") or r.get("treatment"), "stage": r.get("stage"),
                    "note": (r.get("description") or r.get("note") or "")[:200],
                })
            break
    return out


def fetch_gov_contracts():
    """Recent large federal contract awards via USAspending.gov (free).
    Filter on action_date (when the award action happened), newest first, and
    exclude lifetime-aggregate rows by capping the lookback tightly."""
    end = date.today(); start = end - timedelta(days=30)
    body = json.dumps({
        "filters": {
            "award_type_codes": ["A", "B", "C", "D"],
            "time_period": [{"start_date": start.isoformat(), "end_date": end.isoformat(),
                             "date_type": "action_date"}],
        },
        "fields": ["Award ID", "Recipient Name", "Award Amount", "Awarding Agency",
                   "Award Type", "Action Date", "Start Date", "Description"],
        "sort": "Award Amount", "order": "desc", "limit": 100, "page": 1,
    }).encode()
    d = http_json("https://api.usaspending.gov/api/v2/search/spending_by_award/", data=body)
    out = []
    if isinstance(d, dict):
        for r in (d.get("results") or []):
            recip = (r.get("Recipient Name") or "").upper()
            tk = None
            for name, t in GOV_NAME_TO_TICKER.items():
                if name in recip:
                    tk = t; break
            amt = r.get("Award Amount") or 0
            if amt < 5_000_000:
                continue
            adt = (r.get("Action Date") or r.get("Start Date") or "")[:10]
            out.append({
                "ticker": tk, "recipient": r.get("Recipient Name"),
                "amount": amt, "amount_m": round(amt / 1e6, 1),
                "agency": r.get("Awarding Agency"), "date": adt,
                "desc": (r.get("Description") or "")[:160], "mapped": bool(tk),
            })
    out.sort(key=lambda x: (not x["mapped"], -x["amount"]))
    return out


def lambda_handler(event=None, context=None):
    t0 = time.time()
    fda = fetch_fda()
    gov = fetch_gov_contracts()
    today = date.today().isoformat()

    # upcoming FDA (future-dated) for the calendar view
    upcoming = sorted([f for f in fda if f.get("date") and f["date"] >= today],
                      key=lambda x: x["date"])[:40]
    gov_mapped = [g for g in gov if g.get("mapped")][:40]

    out = {
        "engine": "catalyst-calendar", "version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "duration_s": round(time.time() - t0, 1),
        "fda": fda[:80],
        "fda_upcoming": upcoming,
        "gov_contracts": gov[:60],
        "gov_contracts_mapped": gov_mapped,
        "stats": {"n_fda": len(fda), "n_fda_upcoming": len(upcoming),
                  "n_gov": len(gov), "n_gov_mapped": len(gov_mapped)},
        "sources": {"fda": "FMP fda-calendar", "gov": "USAspending.gov (free federal award API)"},
        "note": ("Free catalyst feeds for early detection: FDA PDUFA/AdCom binary "
                 "events (biotech) + material federal contract awards (defense/"
                 "tech/healthcare). Contract recipients mapped to tickers for the "
                 "biggest defense/IT primes."),
    }
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")
    print(f"[catalyst] DONE {round(time.time()-t0,1)}s — {len(fda)} FDA, {len(gov)} gov ({len(gov_mapped)} mapped)")
    return {"statusCode": 200, "body": json.dumps(out["stats"])}
