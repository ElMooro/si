"""justhodl-activist-radar — scans SEC EDGAR daily indexes for 13D/13G filings.

Discovery: https://www.sec.gov/Archives/edgar/daily-index/YYYY/QTRn/master.YYYYMMDD.idx
(last 90 days on first run). For each 13D/13G(/A) filing the accession doc is
fetched to extract the SUBJECT company (name + CIK) and the reported percent
owned. Subject CIK -> ticker via SEC company_tickers.json.

New activist positions = accessions not present in the previous run's output.
Incremental by design: seen accessions persist in the output file, so each run
only parses docs it has not parsed before (DOC_BUDGET per run).

Fail-soft: every network/parse step is wrapped; the handler always writes
data/activist-radar.json even if partial/empty. SEC rate limit respected
(~0.25s sleep between requests, ~4 req/s max).

Output: data/activist-radar.json
  {generated_at, new_activist_positions: [...], recent_filings: [...]}
"""
import json
import os
import re
import time
import urllib.request
from datetime import datetime, timedelta, timezone

import boto3

S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/activist-radar.json"
USER_AGENT = os.environ.get("USER_AGENT", "JustHodl contact@justhodl.ai")

HISTORY_DAYS = 90          # full-history window on first run
DOC_BUDGET = 90            # max filing docs parsed per run
IDX_TIMEOUT = 30
DOC_TIMEOUT = 30
SLEEP = 0.25               # ~4 req/s, under SEC's limit
SEEN_CAP = 6000
RECENT_CAP = 250

TARGET_FORMS = {"13D", "13G", "13D/A", "13G/A"}

s3 = boto3.client("s3")


def _fetch(url, timeout=25):
    req = urllib.request.Request(url, headers={
        "User-Agent": USER_AGENT, "Accept": "*/*", "Accept-Encoding": "gzip"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "ignore")


def _sleep():
    time.sleep(SLEEP)


def get_j(key, default=None):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception:
        return default if default is not None else {}


def put_j(key, body):
    s3.put_object(Bucket=S3_BUCKET, Key=key,
                  Body=json.dumps(body, separators=(",", ":")),
                  ContentType="application/json")


def quarter_of(dt):
    return (dt.month - 1) // 3 + 1


def day_index_dates(n):
    today = datetime.now(timezone.utc).date()
    return [today - timedelta(days=i) for i in range(n)]


def fetch_index(day):
    """Return list of (filer_cik, filer_name, form, filed, path) for target forms."""
    url = ("https://www.sec.gov/Archives/edgar/daily-index/%d/QTR%d/"
           "master.%s.idx" % (day.year, quarter_of(day), day.strftime("%Y%m%d")))
    try:
        txt = _fetch(url, timeout=IDX_TIMEOUT)
    except Exception:
        return []
    out = []
    for line in txt.splitlines():
        parts = line.split("|")
        if len(parts) != 5 or not parts[0].strip().isdigit():
            continue
        cik, name, form, filed, path = [p.strip() for p in parts]
        if form in TARGET_FORMS:
            out.append((cik.zfill(10), name, form, filed, path))
    return out


def accession_of(path):
    m = re.search(r"(\d{10}-\d{2}-\d{6})", path)
    return m.group(1) if m else path.rsplit("/", 1)[-1]


def parse_subject(txt):
    """Extract subject company name + CIK from the filing's SEC-HEADER."""
    name = cik = None
    m = re.search(r"SUBJECT COMPANY:\s*\n\s*COMPANY CONFORMED NAME:\s*([^\n\r]+)",
                  txt, re.IGNORECASE)
    if m:
        name = m.group(1).strip()
    m = re.search(r"SUBJECT COMPANY:[\s\S]{0,400}?CIK:\s*0*(\d+)", txt,
                  re.IGNORECASE)
    if m:
        cik = m.group(1).zfill(10)
    return name, cik


def parse_percent(txt):
    """Best-effort percent-of-class-owned extraction (13D/13G)."""
    body = re.sub(r"<[^>]+>", " ", txt)          # strip markup
    body = re.sub(r"\s+", " ", body)
    low = body.lower()
    patterns = [
        r"percent\s+of\s+class[^%\d]{0,80}?(\d+(?:\.\d+)?)\s*%",
        r"amount\s+beneficially\s+owned[^%\d]{0,80}?(\d+(?:\.\d+)?)\s*%",
        r"beneficially\s+owned[^%\d]{0,80}?(\d+(?:\.\d+)?)\s*%",
        r"represents?\s+(?:approximately\s+)?(\d+(?:\.\d+)?)\s*%\s+of",
        r"(\d+(?:\.\d+)?)\s*%\s+of\s+(?:the\s+)?(?:outstanding|class)",
    ]
    for pat in patterns:
        m = re.search(pat, low)
        if m:
            try:
                v = float(m.group(1))
                if 0 < v <= 100:
                    return round(v, 2)
            except ValueError:
                pass
    return None


def cik_ticker_map():
    try:
        raw = _fetch("https://www.sec.gov/files/company_tickers.json",
                     timeout=30)
        d = json.loads(raw)
        return {str(v["cik_str"]).zfill(10): v["ticker"] for v in d.values()
                if isinstance(v, dict) and v.get("cik_str") and v.get("ticker")}
    except Exception:
        return {}


def parse_filing(path):
    """Fetch accession doc -> (subject_name, subject_cik, pct_owned)."""
    url = "https://www.sec.gov/Archives/" + path
    txt = _fetch(url, timeout=DOC_TIMEOUT)
    name, cik = parse_subject(txt)
    return name, cik, parse_percent(txt)


def handler(event=None, context=None):
    generated_at = datetime.now(timezone.utc).isoformat()
    prev = get_j(OUT_KEY, {})
    seen = set(prev.get("seen_accessions") or [])
    recent = prev.get("recent_filings") or []
    errors = []

    # 1) scan daily indexes, newest first
    filings = {}  # accession -> entry
    idx_days, idx_hits = 0, 0
    try:
        for day in day_index_dates(HISTORY_DAYS):
            for cik, name, form, filed, path in fetch_index(day):
                acc = accession_of(path)
                if acc not in filings:
                    filings[acc] = {"accession": acc, "filer_cik": cik,
                                    "filer": name, "form": form,
                                    "filing_date": filed, "path": path}
                    idx_hits += 1
            idx_days += 1
            _sleep()
    except Exception as e:
        errors.append("index_scan: %s" % str(e)[:200])

    entries = sorted(filings.values(),
                     key=lambda e: e["filing_date"], reverse=True)

    # 2) ticker map (once)
    tickers = {}
    try:
        tickers = cik_ticker_map()
        _sleep()
    except Exception as e:
        errors.append("ticker_map: %s" % str(e)[:200])

    # 3) parse docs: prefer unparsed entries, newest first
    detail = {}  # accession -> details parsed this run
    parsed = 0
    try:
        for e in entries:
            if parsed >= DOC_BUDGET:
                break
            acc = e["accession"]
            if acc in seen and any(f.get("accession") == acc and f.get("subject_cik")
                                   for f in recent):
                continue  # already enriched in a prior run
            try:
                sname, scik, pct = parse_filing(e["path"])
                detail[acc] = {"subject_name": sname, "subject_cik": scik,
                               "pct_owned": pct}
                parsed += 1
            except Exception as ex:
                errors.append("doc %s: %s" % (acc, str(ex)[:120]))
            _sleep()
    except Exception as e:
        errors.append("doc_loop: %s" % str(e)[:200])

    # merge previously known details
    known_detail = {}
    for f in recent:
        acc = f.get("accession")
        if acc and (f.get("subject_cik") or f.get("pct_owned") is not None):
            known_detail[acc] = {"subject_name": f.get("subject"),
                                 "subject_cik": f.get("subject_cik"),
                                 "pct_owned": f.get("pct_owned")}

    # 4) build enriched filing records
    enriched = []
    for e in entries:
        acc = e["accession"]
        d = detail.get(acc) or known_detail.get(acc) or {}
        scik = d.get("subject_cik")
        enriched.append({
            "accession": acc,
            "ticker": tickers.get(scik) if scik else None,
            "company": d.get("subject_name") or e.get("company"),
            "subject_cik": scik,
            "filer": e["filer"],
            "filer_cik": e["filer_cik"],
            "form": e["form"],
            "filing_date": e["filing_date"],
            "pct_owned": d.get("pct_owned"),
            "is_new": acc not in seen,
        })

    new_positions = [f for f in enriched
                     if f["is_new"] and f["form"] in ("13D", "13G")]

    # 5) persist
    seen |= {e["accession"] for e in entries}
    seen_list = sorted(seen)[-SEEN_CAP:]
    out = {
        "generated_at": generated_at,
        "new_activist_positions": [
            {"ticker": f["ticker"], "company": f["company"],
             "filer": f["filer"], "form": f["form"],
             "filing_date": f["filing_date"], "pct_owned": f["pct_owned"]}
            for f in new_positions
        ],
        "recent_filings": enriched[:RECENT_CAP],
        "seen_accessions": seen_list,
        "stats": {"index_days_scanned": idx_days,
                  "filings_found_90d": len(entries),
                  "docs_parsed_this_run": parsed,
                  "new_positions": len(new_positions)},
        "errors": errors[:20],
    }
    try:
        put_j(OUT_KEY, out)
    except Exception as e:
        errors.append("s3_write: %s" % str(e)[:200])

    return {"ok": True, "new_positions": len(new_positions),
            "filings": len(entries), "errors": len(errors)}


if __name__ == "__main__":
    print(json.dumps(handler(), indent=2))
