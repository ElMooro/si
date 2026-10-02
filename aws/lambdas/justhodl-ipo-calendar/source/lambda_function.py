"""justhodl-ipo-calendar — tracks IPO pipeline from SEC EDGAR daily indexes.

Discovery: https://www.sec.gov/Archives/edgar/daily-index/YYYY/QTRn/master.YYYYMMDD.idx
(last 180 days on first run), filtering S-1, S-1/A (registrations), 424B4
(pricing prospectus -> priced), RW (request for withdrawal -> withdrawn).

For each S-1 the accession doc is fetched to extract a ticker guess
("(Nasdaq: XYZ)" style) and the proposed offering size ("aggregate offering
price $X million"). Incremental: IPO records persist in the output file; each
run only parses docs it has not parsed before (DOC_BUDGET per run).

Status lifecycle: filed -> amended (S-1/A seen) -> priced (424B4 for same CIK)
or withdrawn (RW for same CIK).

Fail-soft: every network/parse step is wrapped; the handler always writes
data/ipo-calendar.json even if partial/empty. SEC rate limit respected
(~0.25s sleep between requests, ~4 req/s max).

Output: data/ipo-calendar.json
  {generated_at, ipos: [{company, cik, filing_date, ticker_guess,
                         offering_size_usd, status, last_update}]}
"""
import json
import os
import re
import time
import urllib.request
from datetime import datetime, timedelta, timezone

import boto3

S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/ipo-calendar.json"
USER_AGENT = os.environ.get("USER_AGENT", "JustHodl contact@justhodl.ai")

HISTORY_DAYS = 180         # full-history window on first run
DOC_BUDGET = 100           # max S-1 docs parsed per run
SLEEP = 0.25               # ~4 req/s, under SEC's limit
IPO_CAP = 500

s3 = boto3.client("s3")


def _fetch(url, timeout=30):
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


def fetch_index(day):
    """(cik, company, form, filed, path) for S-1 / S-1/A / 424B4 / RW rows."""
    url = ("https://www.sec.gov/Archives/edgar/daily-index/%d/QTR%d/"
           "master.%s.idx" % (day.year, quarter_of(day), day.strftime("%Y%m%d")))
    try:
        txt = _fetch(url, timeout=30)
    except Exception:
        return []
    out = []
    for line in txt.splitlines():
        parts = line.split("|")
        if len(parts) != 5 or not parts[0].strip().isdigit():
            continue
        cik, name, form, filed, path = [p.strip() for p in parts]
        if form in ("S-1", "S-1/A", "424B4", "RW"):
            out.append((cik.zfill(10), name, form, filed, path))
    return out


def accession_of(path):
    m = re.search(r"(\d{10}-\d{2}-\d{6})", path)
    return m.group(1) if m else path.rsplit("/", 1)[-1]


def guess_ticker(txt):
    """Look for exchange-listing statements like '(Nasdaq: ABCD)'."""
    body = re.sub(r"<[^>]+>", " ", txt)
    body = re.sub(r"\s+", " ", body)
    pats = [
        r"(?:Nasdaq|NASDAQ)[^A-Za-z]{0,60}?(?:symbol|ticker)?[^A-Za-z]{0,10}?\(?\b([A-Z]{1,5})\b\)?",
        r"(?:NYSE)[^A-Za-z]{0,60}?(?:symbol|ticker)?[^A-Za-z]{0,10}?\(?\b([A-Z]{1,5})\b\)?",
        r"(?:common stock|ordinary shares)[^A-Za-z]{0,80}?under\s+(?:the\s+)?symbol\s+\"?\(?\b([A-Z]{1,5})\b\"?\)?",
        r"trading\s+symbol\s+\"?\(?\b([A-Z]{1,5})\b\"?\)?",
    ]
    for pat in pats:
        m = re.search(pat, body)
        if m:
            t = m.group(1)
            if t not in ("THE", "AND", "FOR", "COMMON", "CLASS", "STOCK",
                         "SHARES", "INC", "CORP", "LLC", "NYSE"):
                return t
    return None


def guess_offering_size(txt):
    """Proposed maximum aggregate offering price -> USD."""
    body = re.sub(r"<[^>]+>", " ", txt)
    body = re.sub(r"\s+", " ", body)
    pats = [
        r"aggregate\s+offering\s+price[^$]{0,60}?\$\s*([\d,]+(?:\.\d+)?)\s*(million|billion|thousand)?",
        r"proposed\s+maximum\s+aggregate\s+offering\s+price[^$]{0,60}?\$\s*([\d,]+(?:\.\d+)?)\s*(million|billion|thousand)?",
        r"offering\s+price[^$]{0,60}?\$\s*([\d,]+(?:\.\d+)?)\s*(million|billion|thousand)",
    ]
    for pat in pats:
        m = re.search(pat, body, re.IGNORECASE)
        if m:
            try:
                v = float(m.group(1).replace(",", ""))
                unit = (m.group(2) or "").lower()
                mult = {"billion": 1e9, "million": 1e6,
                        "thousand": 1e3}.get(unit, 1.0)
                return int(v * mult)
            except ValueError:
                pass
    return None


def parse_s1(path):
    url = "https://www.sec.gov/Archives/" + path
    txt = _fetch(url, timeout=30)
    return guess_ticker(txt), guess_offering_size(txt)


def handler(event=None, context=None):
    generated_at = datetime.now(timezone.utc).isoformat()
    prev = get_j(OUT_KEY, {})
    ipos = {i["cik"]: dict(i) for i in (prev.get("ipos") or []) if i.get("cik")}
    parsed_acc = set()
    for i in ipos.values():
        for a in (i.get("accessions") or []):
            parsed_acc.add(a)
    errors = []

    # 1) scan daily indexes, newest first
    rows = []
    days_scanned = 0
    try:
        today = datetime.now(timezone.utc).date()
        for n in range(HISTORY_DAYS):
            day = today - timedelta(days=n)
            for cik, name, form, filed, path in fetch_index(day):
                rows.append({"cik": cik, "company": name, "form": form,
                             "filing_date": filed, "path": path,
                             "accession": accession_of(path)})
            days_scanned += 1
            _sleep()
    except Exception as e:
        errors.append("index_scan: %s" % str(e)[:200])

    rows.sort(key=lambda r: r["filing_date"], reverse=True)

    # 2) fold 424B4 / RW / S-1 / S-1A events into per-CIK records
    parsed = 0
    for r in rows:
        cik, form = r["cik"], r["form"]
        rec = ipos.get(cik)
        if form == "S-1" and not rec:
            rec = {"company": r["company"], "cik": cik,
                   "filing_date": r["filing_date"], "accessions": [],
                   "ticker_guess": None, "offering_size_usd": None,
                   "status": "filed", "last_update": r["filing_date"]}
            ipos[cik] = rec
        if not rec:
            continue
        if r["accession"] not in rec.get("accessions", []):
            rec.setdefault("accessions", []).append(r["accession"])
        if form == "S-1/A" and rec["status"] == "filed":
            rec["status"] = "amended"
            rec["last_update"] = r["filing_date"]
        elif form == "424B4" and rec["status"] not in ("priced", "withdrawn"):
            rec["status"] = "priced"
            rec["last_update"] = r["filing_date"]
        elif form == "RW":
            rec["status"] = "withdrawn"
            rec["last_update"] = r["filing_date"]

    # 3) parse S-1 docs for ticker + size (unparsed first, newest first)
    try:
        for r in rows:
            if parsed >= DOC_BUDGET:
                break
            if r["form"] != "S-1" or r["accession"] in parsed_acc:
                continue
            rec = ipos.get(r["cik"])
            if not rec or (rec.get("ticker_guess")
                           and rec.get("offering_size_usd")):
                continue
            try:
                t, size = parse_s1(r["path"])
                if t and not rec.get("ticker_guess"):
                    rec["ticker_guess"] = t
                if size and not rec.get("offering_size_usd"):
                    rec["offering_size_usd"] = size
                parsed_acc.add(r["accession"])
                parsed += 1
            except Exception as ex:
                errors.append("s1 %s: %s" % (r["accession"], str(ex)[:120]))
            _sleep()
    except Exception as e:
        errors.append("doc_loop: %s" % str(e)[:200])

    # 4) emit, newest filing first, cap size
    out_list = sorted(ipos.values(),
                      key=lambda i: i.get("filing_date", ""), reverse=True)[:IPO_CAP]
    for i in out_list:
        i.pop("accessions", None)

    counts = {}
    for i in out_list:
        counts[i["status"]] = counts.get(i["status"], 0) + 1

    out = {"generated_at": generated_at, "ipos": out_list,
           "stats": {"index_days_scanned": days_scanned,
                     "rows_matched_180d": len(rows),
                     "s1_docs_parsed_this_run": parsed,
                     "ipos_tracked": len(out_list),
                     "status_counts": counts},
           "errors": errors[:20]}
    try:
        put_j(OUT_KEY, out)
    except Exception as e:
        errors.append("s3_write: %s" % str(e)[:200])

    return {"ok": True, "ipos": len(out_list), "status_counts": counts,
            "errors": len(errors)}


if __name__ == "__main__":
    print(json.dumps(handler(), indent=2))
