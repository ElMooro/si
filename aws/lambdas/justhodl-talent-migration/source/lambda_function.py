"""
justhodl-talent-migration — KEY PEOPLE MOVE BEFORE THE STORY DOES
=====================================================================
Thesis: a key executive or scientist leaving a leader to join a smaller
competitor, or a company suddenly bringing in a new CFO/CEO, is a real,
documented early signal — VCs and specialist funds track this by hand.
SEC Item 5.02 ("Departure of Directors or Certain Officers; Election of
Directors; Appointment of Certain Officers") is the mandated disclosure
for exactly this — filed within 4 business days.

TWO LAYERS
════════════
  1. METADATA (all hits): company, ticker, filing date, filing link —
     reliable, from the proven efts.sec.gov search API alone.
  2. CLASSIFICATION (bounded subset — fetching full filing text is
     slower, so this runs on the most recent ~25 hits only): fetches the
     actual filing document and applies simple, transparent keyword
     matching to flag DEPARTURE language ("resign", "resignation",
     "departure") vs APPOINTMENT language ("appoint", "elected",
     "will serve as") — a filing can be both (someone left, someone else
     was named). This is a lightweight heuristic, not NLP — it is
     explicitly labeled as such in the output, not oversold as certain.

OUTPUT  data/talent-migration.json   SCHEDULE daily 13:05 UTC.
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
OUT_KEY = "data/talent-migration.json"
FMP = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
LOOKBACK_DAYS = 10
CLASSIFY_TOP_N = 25       # only fetch full text for the most recent N -- bounded runtime
s3 = boto3.client("s3", region_name="us-east-1")

TICKER_RE = re.compile(r"\(([A-Z][A-Z0-9\-\.]{0,6}(?:,\s*[A-Z][A-Z0-9\-\.]{0,6})*)\)\s*\(CIK")
DEPARTURE_RE = re.compile(r"\b(resign(?:ed|ation|s)?|departure|stepping down|will depart)\b", re.I)
APPOINT_RE = re.compile(r"\b(appoint(?:ed|s|ment)?|elect(?:ed|ion)?|will serve as|named (?:as )?(?:chief|president|ceo|cfo|coo))\b", re.I)
ROLE_RE = re.compile(r"\b(Chief Executive Officer|CEO|Chief Financial Officer|CFO|Chief Operating Officer|COO|"
                     r"Chief Technology Officer|CTO|President|Chairman|Director)\b", re.I)


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


def _fmp(path, tries=2):
    url = f"https://financialmodelingprep.com/stable/{path}{'&' if '?' in path else '?'}apikey={FMP}"
    for i in range(tries):
        try:
            raw = urllib.request.urlopen(
                urllib.request.Request(url, headers={"User-Agent": "jh-talent"}), timeout=12).read()
            return json.loads(raw)
        except Exception:
            time.sleep(0.5)
    return None


def parse_hit(h):
    src = h.get("_source", {})
    names = src.get("display_names") or []
    primary = names[0] if names else ""
    m = TICKER_RE.search(primary)
    ticker = m.group(1).split(",")[0].strip() if m else None
    company = primary.split("  (")[0].strip() if primary else None
    adsh = src.get("adsh", "").replace("-", "")
    cik = (src.get("ciks") or [""])[0].lstrip("0")
    doc_id = h.get("_id", "")
    filename = doc_id.split(":")[-1] if ":" in doc_id else None
    filing_url = (f"https://www.sec.gov/Archives/edgar/data/{cik}/{adsh}/{filename}"
                 if (cik and adsh and filename) else None)
    return {"ticker": ticker, "company": company, "cik": cik, "form": src.get("form"),
            "file_date": src.get("file_date"), "items": src.get("items") or [],
            "sic": (src.get("sics") or [None])[0], "filing_url": filing_url, "adsh": adsh}


def classify(rec):
    """Best-effort keyword classification from the actual filing text. Transparent
    heuristic, not NLP -- labeled as such downstream."""
    if not rec.get("filing_url"):
        return rec
    try:
        raw = urllib.request.urlopen(
            urllib.request.Request(rec["filing_url"], headers={"User-Agent": "JustHodl.AI research contact@justhodl.ai"}),
            timeout=15).read().decode("utf-8", "ignore")
        text = re.sub(r"<[^>]+>", " ", raw)[:20000]   # strip tags, first ~20K chars (5.02 usually near the top)
        has_departure = bool(DEPARTURE_RE.search(text))
        has_appoint = bool(APPOINT_RE.search(text))
        roles = list(dict.fromkeys(m.upper() if len(m) <= 4 else m.title()
                                   for m in ROLE_RE.findall(text)))[:4]
        rec["classification"] = ("both" if (has_departure and has_appoint) else
                                 "departure" if has_departure else
                                 "appointment" if has_appoint else "unclear")
        rec["roles_mentioned"] = roles
    except Exception:
        rec["classification"] = "unfetched"
        rec["roles_mentioned"] = []
    return rec


def enrich(ticker):
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

    d, err = _edgar('"Item 5.02"', "8-K", startdt, enddt)
    moves, seen_adsh = [], set()
    if d:
        for h in d.get("hits", {}).get("hits", [])[:150]:
            rec = parse_hit(h)
            if "5.02" not in (rec.get("items") or []):
                continue
            if rec["adsh"] in seen_adsh:
                continue                    # same filing, different exhibit document
            seen_adsh.add(rec["adsh"])
            moves.append(rec)
    moves.sort(key=lambda r: r.get("file_date") or "", reverse=True)

    # classify the most recent subset (bounded runtime — full-text fetch is the expensive part)
    to_classify = moves[:CLASSIFY_TOP_N]
    with ThreadPoolExecutor(max_workers=10) as ex:
        futs = {ex.submit(classify, r): r for r in to_classify}
        for f in as_completed(futs):
            f.result()   # classify() mutates rec in place
    for r in moves[CLASSIFY_TOP_N:]:
        r["classification"] = "not_checked"
        r["roles_mentioned"] = []

    # enrich sector/market-cap context for tickers we have
    all_tickers = list({r["ticker"] for r in moves if r.get("ticker")})
    context = {}
    with ThreadPoolExecutor(max_workers=10) as ex:
        futs = {ex.submit(enrich, t): t for t in all_tickers}
        for f in as_completed(futs):
            t = futs[f]
            try:
                context[t] = f.result() or {}
            except Exception:
                context[t] = {}
    for r in moves:
        r.update(context.get(r.get("ticker"), {}))

    departures = [r for r in moves if r.get("classification") in ("departure", "both")]
    appointments = [r for r in moves if r.get("classification") in ("appointment", "both")]
    sector_counts = {}
    for r in moves:
        s = r.get("sector") or "?"
        sector_counts[s] = sector_counts.get(s, 0) + 1

    out = {
        "engine": "talent-migration", "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "window": {"start": startdt, "end": enddt, "days": LOOKBACK_DAYS},
        "thesis": "A key executive or scientist leaving one company for another is a real, "
                  "documented early signal specialist funds track by hand. SEC Item 5.02 is the "
                  "mandated disclosure for exactly this, filed within 4 business days.",
        "methodology_note": "Classification (departure vs appointment vs both) is a transparent "
                           "keyword heuristic run against the actual filing text for the most "
                           "recent 25 filings — NOT NLP, and not certain. Read the filing before "
                           "concluding anything. Older filings in this window are metadata-only "
                           "(classification='not_checked').",
        "n_total": len(moves), "n_classified": len(to_classify),
        "n_departures": len(departures), "n_appointments": len(appointments),
        "by_sector": sector_counts,
        "recent_moves": moves[:60],
        "departures": departures[:30], "appointments": appointments[:30],
        "sources": ["SEC EDGAR full-text search (efts.sec.gov)", "SEC EDGAR filing documents",
                   "FMP company-screener (enrichment)"],
        "disclaimer": "Real SEC filings, research only — not investment advice. An executive "
                     "change disclosure is not itself a trade signal; read the actual filing.",
        "elapsed_s": round(time.time() - t0, 2),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=1800")
    print(f"[talent] total={len(moves)} departures={len(departures)} appointments={len(appointments)} "
          f"{out['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({"ok": True, "n_total": len(moves),
            "n_departures": len(departures), "n_appointments": len(appointments)})}
