"""
justhodl-universe-discovery — YOU CAN'T CATCH IT EARLY IF IT'S NOT IN THE UNIVERSE YET
=========================================================================================
Thesis: master-ranker, ai-rerating-radar, and every other screen on this platform runs
against a PRE-DEFINED universe. None of them notice when a genuinely new candidate
appears — a fresh IPO, a new SEC registrant, or a small-cap quietly crossing into
coverage range. This is the structural fix: a feed of what's NEW, so it can flow into
the existing engines instead of waiting for a human to notice a ticker exists.

THREE DISCOVERY MECHANISMS
════════════════════════════
  1. IPO CALENDAR — FMP's ipos-calendar, upcoming and recently-priced offerings.
  2. NEW SEC REGISTRANTS — full-text search for Form S-1 (registration for a public
     offering) and Form 10-12G (registration of a class of securities without an
     IPO, e.g. spin-offs and direct listings) in a rolling window. Same proven
     efts.sec.gov infrastructure used elsewhere in this build.
  3. THRESHOLD CROSSERS — small/micro caps quietly crossing into the ~$300M broad-
     coverage floor used by re-rating-radar's universe. Compares today's company-
     screener snapshot against the PREVIOUS run's saved snapshot (persisted to S3)
     to find tickers that are newly present — names nobody's screen was covering
     yesterday that are covered today.

OUTPUT  data/universe-discovery.json   SCHEDULE daily 12:10 UTC.
"""
import json
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta

import boto3

VERSION = "1.0.0"
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/universe-discovery.json"
SNAPSHOT_KEY = "state/universe-discovery-snapshot.json"   # persisted between runs
FMP = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
LOOKBACK_DAYS = 10
s3 = boto3.client("s3", region_name="us-east-1")


def _fmp(path, tries=3):
    url = f"https://financialmodelingprep.com/stable/{path}{'&' if '?' in path else '?'}apikey={FMP}"
    for i in range(tries):
        try:
            raw = urllib.request.urlopen(
                urllib.request.Request(url, headers={"User-Agent": "jh-discovery"}), timeout=15).read()
            return json.loads(raw)
        except Exception:
            time.sleep(0.8 * (i + 1))
    return None


def _edgar(q, forms, startdt, enddt, tries=3):
    url = ("https://efts.sec.gov/LATEST/search-index?q=" + urllib.parse.quote(q)
           + f"&forms={forms}&startdt={startdt}&enddt={enddt}")
    for i in range(tries):
        try:
            raw = urllib.request.urlopen(
                urllib.request.Request(url, headers={"User-Agent": "JustHodl.AI research contact@justhodl.ai"}),
                timeout=20).read()
            return json.loads(raw), None
        except Exception as e:
            err = f"{type(e).__name__}: {str(e)[:150]}"
            time.sleep(1.0 * (i + 1))
    return None, err


import re
TICKER_RE_PAREN = re.compile(r"\(([A-Z][A-Z0-9\-\.]{0,6}(?:,\s*[A-Z][A-Z0-9\-\.]{0,6})*)\)\s*\(CIK")


def parse_registrant_hit(h):
    src = h.get("_source", {})
    names = src.get("display_names") or []
    primary = names[0] if names else ""
    m = TICKER_RE_PAREN.search(primary)
    ticker = m.group(1).split(",")[0].strip() if m else None
    company = primary.split("  (")[0].strip() if primary else None
    adsh = src.get("adsh", "").replace("-", "")
    cik = (src.get("ciks") or [""])[0].lstrip("0")
    doc_id = h.get("_id", "")
    filename = doc_id.split(":")[-1] if ":" in doc_id else None
    filing_url = (f"https://www.sec.gov/Archives/edgar/data/{cik}/{adsh}/{filename}"
                 if (cik and adsh and filename) else None)
    return {"ticker": ticker, "company": company, "cik": cik, "form": src.get("form"),
            "file_date": src.get("file_date"), "sic": (src.get("sics") or [None])[0],
            "location": (src.get("biz_locations") or [None])[0], "filing_url": filing_url}


def load_prior_snapshot():
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=SNAPSHOT_KEY)["Body"].read())
    except Exception:
        return None


def lambda_handler(event=None, context=None):
    t0 = time.time()
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=LOOKBACK_DAYS)
    startdt, enddt = start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")

    # ── 1) IPO calendar ──
    ipo_from = (end - timedelta(days=7)).strftime("%Y-%m-%d")
    ipo_to = (end + timedelta(days=45)).strftime("%Y-%m-%d")
    ipos_raw = _fmp(f"ipos-calendar?from={ipo_from}&to={ipo_to}") or []
    ipos = []
    for r in ipos_raw:
        if not isinstance(r, dict):
            continue
        company = r.get("company") or ""
        if "ETF" in company.upper():
            continue                       # ETF launches aren't the "new company" signal this is for
        if any(x in company.upper() for x in ("WARRANT", "WHEN ISSUED", "WHEN-ISSUED", "EX-DISTRIBUTION")):
            continue                       # derivative securities / spin-off technical listings, not new companies
        ipos.append({"symbol": r.get("symbol"), "company": company,
                     "date": r.get("date"), "exchange": r.get("exchange"),
                     "actions": r.get("actions"), "shares": r.get("shares"),
                     "price_range": r.get("priceRange"), "market_cap": r.get("marketCap")})
    ipos.sort(key=lambda r: r.get("date") or "")

    # ── 2) new SEC registrants: Form S-1 (IPO registration) + Form 10-12G (spinoffs /
    #    direct listings without a traditional IPO) ──
    s1_data, s1_err = _edgar('"registration statement"', "S-1", startdt, enddt)
    reg10_data, reg10_err = _edgar('"registration"', "10-12G", startdt, enddt)
    new_registrants, seen_adsh = [], set()
    for data, form_label in [(s1_data, "S-1"), (reg10_data, "10-12G")]:
        if not data:
            continue
        for h in data.get("hits", {}).get("hits", [])[:40]:
            adsh = (h.get("_source") or {}).get("adsh")
            key = adsh or h.get("_id")
            if key in seen_adsh:
                continue                    # same filing, different exhibit document
            seen_adsh.add(key)
            rec = parse_registrant_hit(h)
            rec["registration_type"] = form_label
            new_registrants.append(rec)
    new_registrants.sort(key=lambda r: r.get("file_date") or "", reverse=True)

    # ── 3) threshold crossers: today's broad screen vs the saved prior snapshot ──
    screen = _fmp("company-screener?marketCapMoreThan=300000000&limit=3000"
                  "&isActivelyTrading=true&country=US") or []
    US_EXCHANGES = {"NYSE", "NASDAQ", "AMEX", "NYSE American", "NASDAQ Global Select",
                    "NASDAQ Global Market", "NASDAQ Capital Market"}
    current = {}
    for r in screen:
        sym = r.get("symbol")
        if not sym or r.get("isEtf") or r.get("isFund") or not r.get("sector"):
            continue
        if r.get("exchangeShortName") not in US_EXCHANGES and r.get("exchange") not in US_EXCHANGES:
            continue
        current[sym] = {"name": r.get("companyName"), "sector": r.get("sector"),
                        "industry": r.get("industry"), "market_cap": r.get("marketCap")}

    prior = load_prior_snapshot()
    threshold_crossers = []
    if prior and prior.get("tickers"):
        prior_set = set(prior["tickers"])
        new_syms = [s for s in current if s not in prior_set]
        for s in new_syms:
            threshold_crossers.append({"ticker": s, **current[s]})
        threshold_crossers.sort(key=lambda r: -(r.get("market_cap") or 0))
        crossers_note = (f"compared against a snapshot from {prior.get('snapshot_date')} "
                         f"({len(prior_set)} tickers then vs {len(current)} now)")
    else:
        crossers_note = "no prior snapshot yet — this is the first run; threshold-crossing " \
                        "detection begins on the NEXT run once today's snapshot is saved"

    # save today's snapshot for tomorrow's comparison
    s3.put_object(Bucket=S3_BUCKET, Key=SNAPSHOT_KEY,
                  Body=json.dumps({"snapshot_date": end.strftime("%Y-%m-%d"),
                                   "tickers": list(current.keys())}).encode(),
                  ContentType="application/json")

    out = {
        "engine": "universe-discovery", "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "thesis": "Every screen on this platform runs against a predefined universe. This finds "
                  "what's NEW — fresh IPOs, new SEC registrants, and small caps quietly crossing "
                  "into coverage range — so it can flow into the existing engines instead of "
                  "waiting for a human to notice a ticker exists.",
        "ipo_calendar": {"n": len(ipos), "items": ipos[:60],
                         "window": {"from": ipo_from, "to": ipo_to}, "source": "FMP ipos-calendar"},
        "new_registrants": {"n": len(new_registrants), "items": new_registrants[:60],
                            "window": {"start": startdt, "end": enddt},
                            "source": "SEC EDGAR full-text search, Form S-1 (IPO registration) + "
                                    "Form 10-12G (registration without a traditional IPO — spin-offs, "
                                    "direct listings)",
                            "_debug_errors": {"s1": s1_err, "reg10": reg10_err}},
        "threshold_crossers": {"n": len(threshold_crossers), "items": threshold_crossers[:60],
                               "note": crossers_note},
        "coverage": {"n_current_broad_universe": len(current)},
        "sources": ["FMP ipos-calendar", "SEC EDGAR full-text search (efts.sec.gov)",
                   "FMP company-screener"],
        "disclaimer": "Real data, research only — not investment advice. A new listing or "
                     "registration is not itself a trade signal; these are discovery inputs meant "
                     "to feed the platform's other screening engines, not standalone calls.",
        "elapsed_s": round(time.time() - t0, 2),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=1800")
    print(f"[discovery] ipos={len(ipos)} new_registrants={len(new_registrants)} "
          f"threshold_crossers={len(threshold_crossers)} {out['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({"ok": True, "n_ipos": len(ipos),
            "n_new_registrants": len(new_registrants), "n_threshold_crossers": len(threshold_crossers)})}
