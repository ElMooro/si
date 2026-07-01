"""
justhodl-structural-pre-signals — MANDATED DISCLOSURE, NOT VOLUNTARY NEWS
============================================================================
Thesis: WARN Act layoffs (60-90 day mandated advance notice) and utility
interconnection queues (2-5yr lead time) are genuinely powerful pre-signals,
but a live, reliable, free national API for either wasn't found within the
research budget for this build (state-by-state WARN portals are inconsistent;
ERCOT's queue requires a numeric report ID with no keyword-search discovery
path; PJM requires a registered API key this platform doesn't have).

Rather than fabricate an integration against an unverified endpoint, this
engine builds on what IS verified, free, and reliable: SEC EDGAR's own full-
text search (efts.sec.gov), which is federally mandated and fast:

  RESTRUCTURING (downside pre-signal): 8-K Item 2.05 ("Costs Associated with
  Exit or Disposal Activities") is SEC's OWN mandated restructuring
  disclosure — filed within 4 business days of the board committing to a
  plan, often layoff/facility-closure related. Same spirit as WARN Act
  (legally mandated, fast, ahead of the news cycle), sourced from
  infrastructure already proven working elsewhere on this build.

  BUILDOUT (upside pre-signal, ties to the platform's AI-infra thesis):
  full-text search for data-center/power-capacity language ("data center",
  "megawatts", "gigawatt", "power purchase agreement", "hyperscale") in
  recent 8-K/10-Q/10-K filings — corporate disclosure of major capex
  commitments, often years before it shows up in reported revenue.

HONEST NOTE: this is company self-disclosure, not the raw utility queue or
state labor filing — a real, valuable, but different vantage point than the
original WARN/interconnection-queue idea. A direct integration with either
is a documented, real, identified future enhancement (needs either a state-
by-state WARN scraper build-out or a PJM API key Khalid would need to
register for).

OUTPUT  data/structural-pre-signals.json   SCHEDULE daily 11:20 UTC.
"""
import json
import re
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3

VERSION = "1.0.0"
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/structural-pre-signals.json"
FMP = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
LOOKBACK_DAYS = 14
s3 = boto3.client("s3", region_name="us-east-1")

BUILDOUT_TERMS = ['"data center" "megawatts"', '"power purchase agreement" "data center"',
                  '"gigawatt" "data center"', '"hyperscale"']


def _edgar(q, forms, startdt, enddt, tries=3):
    url = ("https://efts.sec.gov/LATEST/search-index?q=" + urllib.parse.quote(q)
           + f"&forms={forms}&startdt={startdt}&enddt={enddt}")
    last_err = None
    for i in range(tries):
        try:
            raw = urllib.request.urlopen(
                urllib.request.Request(url, headers={"User-Agent": "JustHodl.AI research contact@justhodl.ai"}),
                timeout=20).read()
            return json.loads(raw), url, None
        except Exception as e:
            last_err = f"{type(e).__name__}: {str(e)[:200]}"
            time.sleep(1.0 * (i + 1))
    return None, url, last_err


def _fmp(path, tries=2):
    url = f"https://financialmodelingprep.com/stable/{path}{'&' if '?' in path else '?'}apikey={FMP}"
    for i in range(tries):
        try:
            raw = urllib.request.urlopen(
                urllib.request.Request(url, headers={"User-Agent": "jh-structural"}), timeout=12).read()
            return json.loads(raw)
        except Exception:
            time.sleep(0.5)
    return None


TICKER_RE = re.compile(r"\(([A-Z][A-Z0-9\-\.]{0,6}(?:,\s*[A-Z][A-Z0-9\-\.]{0,6})*)\)\s*\(CIK")


def parse_hit(h):
    src = h.get("_source", {})
    names = src.get("display_names") or []
    primary = names[0] if names else ""
    m = TICKER_RE.search(primary)
    tickers = [t.strip() for t in m.group(1).split(",")] if m else []
    ticker = tickers[0] if tickers else None
    company = primary.split("  (")[0].strip() if primary else None
    adsh = src.get("adsh", "").replace("-", "")
    cik = (src.get("ciks") or [""])[0].lstrip("0")
    doc_id = h.get("_id", "")
    filename = doc_id.split(":")[-1] if ":" in doc_id else None
    filing_url = (f"https://www.sec.gov/Archives/edgar/data/{cik}/{adsh}/{filename}"
                 if (cik and adsh and filename) else None)
    return {
        "ticker": ticker, "company": company, "cik": cik,
        "form": src.get("form"), "file_date": src.get("file_date"),
        "period_ending": src.get("period_ending"), "items": src.get("items") or [],
        "sic": (src.get("sics") or [None])[0], "location": (src.get("biz_locations") or [None])[0],
        "filing_url": filing_url,
    }


def enrich(ticker):
    """Sector/industry/market-cap context, best-effort — never blocks the core signal."""
    if not ticker:
        return {}
    d = _fmp(f"company-screener?symbol={urllib.parse.quote(ticker)}")
    if isinstance(d, list) and d:
        return {"sector": d[0].get("sector"), "industry": d[0].get("industry"),
                "market_cap": d[0].get("marketCap")}
    return {}


def lambda_handler(event=None, context=None):
    t0 = time.time()
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=LOOKBACK_DAYS)
    startdt, enddt = start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")

    # ── restructuring: 8-K Item 2.05, SEC's own mandated exit/disposal disclosure ──
    restructuring = []
    debug_info = {}
    d, req_url, err = _edgar('"Item 2.05"', "8-K", startdt, enddt)
    debug_info["restructuring_query_url"] = req_url
    debug_info["restructuring_error"] = err
    debug_info["restructuring_raw_hit_count"] = (d.get("hits", {}).get("total", {}).get("value")
                                                 if d else None)
    if d:
        for h in d.get("hits", {}).get("hits", [])[:100]:
            rec = parse_hit(h)
            if "2.05" in (rec.get("items") or []):    # confirm genuine item presence, not just text match
                restructuring.append(rec)
    debug_info["restructuring_after_filter"] = len(restructuring)

    # ── buildout: datacenter/power-capacity capex language, several term variants ──
    buildout_raw, seen_ids = [], set()
    debug_info["buildout_per_term"] = []
    for term in BUILDOUT_TERMS:
        d, req_url, err = _edgar(term, "8-K,10-Q,10-K", startdt, enddt)
        debug_info["buildout_per_term"].append({"term": term, "error": err,
            "raw_hits": (d.get("hits", {}).get("total", {}).get("value") if d else None)})
        if not d:
            continue
        for h in d.get("hits", {}).get("hits", [])[:50]:
            hid = h.get("_id")
            if hid in seen_ids:
                continue
            seen_ids.add(hid)
            buildout_raw.append(parse_hit(h))

    # ── enrich tickers (sector/mkt-cap) concurrently, best-effort ──
    all_tickers = list({r["ticker"] for r in restructuring + buildout_raw if r.get("ticker")})
    context = {}
    with ThreadPoolExecutor(max_workers=10) as ex:
        futs = {ex.submit(enrich, t): t for t in all_tickers}
        for f in as_completed(futs):
            t = futs[f]
            try:
                context[t] = f.result() or {}
            except Exception:
                context[t] = {}

    for r in restructuring:
        r.update(context.get(r.get("ticker"), {}))
    for r in buildout_raw:
        r.update(context.get(r.get("ticker"), {}))

    restructuring.sort(key=lambda r: r.get("file_date") or "", reverse=True)
    buildout_raw.sort(key=lambda r: r.get("file_date") or "", reverse=True)

    sector_counts = {}
    for r in buildout_raw:
        s = r.get("sector") or "?"
        sector_counts[s] = sector_counts.get(s, 0) + 1

    out = {
        "engine": "structural-pre-signals", "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "window": {"start": startdt, "end": enddt, "days": LOOKBACK_DAYS},
        "thesis": "Federally-mandated disclosure, not voluntary news — SEC Item 2.05 restructuring "
                  "filings are legally required within 4 business days of the decision, ahead of the "
                  "news cycle. Capex/buildout language in fresh filings surfaces major infrastructure "
                  "commitments years before they show up in reported revenue.",
        "honest_scope": "This is company self-disclosure via SEC EDGAR full-text search, not a direct "
                       "integration with state WARN Act portals or utility interconnection queues — "
                       "those were researched (both are real and valuable) but a reliable free national "
                       "API wasn't found for WARN within the research budget, and the utility queues "
                       "(ERCOT/PJM) require either a registered API key this platform doesn't have or "
                       "more work to discover a stable report endpoint. Documented as a real future "
                       "enhancement, not fabricated here.",
        "restructuring": {
            "n": len(restructuring), "items": restructuring[:60],
            "source": "SEC EDGAR full-text search, 8-K Item 2.05 (Costs Associated with Exit or "
                     "Disposal Activities)",
        },
        "buildout": {
            "n": len(buildout_raw), "items": buildout_raw[:60],
            "by_sector": sector_counts,
            "source": "SEC EDGAR full-text search, data-center/power-capacity language in 8-K/10-Q/10-K",
        },
        "sources": ["SEC EDGAR full-text search (efts.sec.gov)", "FMP company-screener (enrichment)"],
        "_debug": debug_info,
        "disclaimer": "Real SEC filings, research only — not investment advice. A company filing "
                     "Item 2.05 or mentioning data-center capacity is not itself a trade signal; "
                     "read the actual filing before acting.",
        "elapsed_s": round(time.time() - t0, 2),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=1800")
    print(f"[structural] restructuring={len(restructuring)} buildout={len(buildout_raw)} "
          f"{out['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({"ok": True, "n_restructuring": len(restructuring),
            "n_buildout": len(buildout_raw)})}
