"""
justhodl-sec-10kq — SEC 10-K (annual) + 10-Q (quarterly) financial filings

10-K = annual report; 10-Q = quarterly report. Both contain audited (or
reviewed) financial statements: balance sheet, cash flow, income, notes,
risk factors, MD&A. The most authoritative single document about a public
company.

Use cases for tracking:
- Detect new filings for tickers in justhodl-stock-screener and update
  fundamental scores
- Surface "amended" filings (10-K/A, 10-Q/A) — sometimes signals issues
- Pre-warn for restatement risk (4.02 + 10-Q/A combo is classic)

Atom feed pulls latest 200 of each. Output keyed by accession with rolling
30-day window.

Output (data/10kq-filings.json):
  {
    "generated_at": ...,
    "stats": {total_10k, total_10q, total_amended, ...},
    "filings": [
      {ticker, company, form, accession, filed_at, primary_url,
       period_of_report, fiscal_year_end},
      ...
    ],
    "amended": [...]   (10-K/A or 10-Q/A — restated/corrected)
  }
"""
from __future__ import annotations
import json
import os
import re
import time
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta

import boto3

S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = os.environ.get("S3_KEY", "data/10kq-filings.json")
USER_AGENT = os.environ.get("USER_AGENT", "JustHodl Research raafouis@gmail.com")
WINDOW_DAYS = int(os.environ.get("WINDOW_DAYS", "30"))


def _fetch(url: str, timeout: int = 25) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def fetch_atom(form: str, count: int = 200):
    url = (f"https://www.sec.gov/cgi-bin/browse-edgar"
           f"?action=getcurrent&type={form}&output=atom&count={count}")
    raw = _fetch(url)
    root = ET.fromstring(raw)
    ns = {"a": "http://www.w3.org/2005/Atom"}
    out = []
    for entry in root.findall("a:entry", ns):
        title = (entry.findtext("a:title", default="", namespaces=ns) or "").strip()
        link_el = entry.find("a:link", ns)
        link = (link_el.get("href") if link_el is not None else "") or ""
        updated = entry.findtext("a:updated", default="", namespaces=ns)
        id_text = (entry.findtext("a:id", default="", namespaces=ns) or "").strip()
        summary = (entry.findtext("a:summary", default="", namespaces=ns) or "").strip()

        accession = None
        for src in (id_text, summary, link):
            m = re.search(r"\b(\d{10}-\d{2}-\d{6})\b", src)
            if m:
                accession = m.group(1)
                break
        if not accession:
            continue

        # Title format: "10-K - APPLE INC (0000320193) (Filer)"
        co_match = re.match(rf"{re.escape(form)}\s*[-–]\s*(.+?)\s*\(\d+\)", title)
        company = co_match.group(1).strip() if co_match else title[:80]

        # CIK from title
        cik_match = re.search(r"\((\d{10})\)", title)
        cik = cik_match.group(1) if cik_match else None

        out.append({
            "form": form,
            "company": company,
            "cik": cik,
            "accession": accession,
            "filed_at": updated,
            "filing_url": link,
            "summary": summary[:160],
        })
    return out


def merge_window(prior: list, fresh: list) -> list:
    cutoff = datetime.now(timezone.utc) - timedelta(days=WINDOW_DAYS)
    seen = set()
    out = []
    for f in fresh + prior:
        if f["accession"] in seen:
            continue
        try:
            dt = datetime.fromisoformat(f["filed_at"].replace("Z", "+00:00"))
        except Exception:
            dt = datetime.now(timezone.utc)
        if dt < cutoff:
            continue
        seen.add(f["accession"])
        out.append(f)
    out.sort(key=lambda x: x["filed_at"], reverse=True)
    return out


def lambda_handler(event, context):
    s3 = boto3.client("s3")
    started = time.time()

    fresh = []
    fetch_errors = []
    for form in ("10-K", "10-Q", "10-K/A", "10-Q/A"):
        try:
            fresh.extend(fetch_atom(form, count=200))
        except Exception as e:
            fetch_errors.append(f"{form}: {type(e).__name__}")
            continue
        time.sleep(0.3)  # gentle on SEC

    existing = {}
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
        existing = json.loads(obj["Body"].read())
    except Exception:
        pass
    prior = existing.get("filings", [])

    merged = merge_window(prior, fresh)

    by_form = {"10-K": [], "10-Q": [], "10-K/A": [], "10-Q/A": []}
    for f in merged:
        by_form.setdefault(f["form"], []).append(f)

    amended = [f for f in merged if "/A" in f["form"]]

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "window_days": WINDOW_DAYS,
        "stats": {
            "total": len(merged),
            "total_10k": len(by_form.get("10-K", [])),
            "total_10q": len(by_form.get("10-Q", [])),
            "total_10k_amended": len(by_form.get("10-K/A", [])),
            "total_10q_amended": len(by_form.get("10-Q/A", [])),
            "fetch_errors": fetch_errors,
            "fetch_duration_s": round(time.time() - started, 1),
        },
        "amended": amended[:30],
        "filings": merged[:300],
        "by_form_count": {k: len(v) for k, v in by_form.items()},
    }
    s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                  Body=json.dumps(output).encode(),
                  ContentType="application/json", CacheControl="no-cache")

    print(f"10-K/Q: {len(merged)} in window | 10-K {len(by_form.get('10-K', []))} | 10-Q {len(by_form.get('10-Q', []))} | amended {len(amended)}")
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        "body": json.dumps({"ok": True, "stats": output["stats"]}),
    }
