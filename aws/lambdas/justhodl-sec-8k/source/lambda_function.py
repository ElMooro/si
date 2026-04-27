"""
justhodl-sec-8k — SEC 8-K material event filings tracker

8-K filings disclose material events: acquisitions, leadership changes,
material agreements, restructurings, regulation FD events. Filed within
4 business days of the event. The most "fresh" company-specific signal
in equity markets.

We watch the SEC atom feed for the latest 8-K filings, classify by
"item number" (each Item designates a specific event type), and roll
into per-ticker activity.

Item codes (key ones):
  1.01  Material Definitive Agreement
  1.02  Termination of Material Definitive Agreement
  2.01  Completion of Acquisition or Disposition
  2.02  Results of Operations and Financial Condition (often = earnings preliminary)
  4.02  Non-reliance on Previously Issued Financial Statements (RED FLAG)
  5.02  Departure/Election of Officers/Directors (often material)
  7.01  Reg FD Disclosure
  8.01  Other Events

Output (data/8k-filings.json):
  {
    "generated_at": ...,
    "filings": [{ticker, company, items, filed_at, accession, primary_url}, ...]   (last 7 days)
    "by_item": {
      "2.02": [...],     (earnings)
      "5.02": [...],     (officer changes)
      ...
    },
    "stats": { total, last_24h, top_items, ... }
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
from collections import defaultdict

import boto3

S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = os.environ.get("S3_KEY", "data/8k-filings.json")
USER_AGENT = os.environ.get("USER_AGENT", "JustHodl Research raafouis@gmail.com")
WINDOW_DAYS = int(os.environ.get("WINDOW_DAYS", "7"))


ITEM_LABELS = {
    "1.01": "Material Definitive Agreement",
    "1.02": "Termination of Agreement",
    "1.03": "Bankruptcy or Receivership",
    "2.01": "Completion of Acquisition/Disposition",
    "2.02": "Results of Operations (often preliminary earnings)",
    "2.03": "Material Direct Financial Obligation",
    "2.04": "Triggering Events Accelerating a Direct Financial Obligation",
    "2.05": "Costs Associated with Exit/Disposal Activities",
    "2.06": "Material Impairments",
    "3.01": "Notice of Delisting / Failure to Satisfy Listing",
    "3.02": "Unregistered Sales of Equity Securities",
    "4.01": "Changes in Registrant's Certifying Accountant",
    "4.02": "Non-Reliance on Previously Issued Financial Statements",
    "5.01": "Changes in Control of Registrant",
    "5.02": "Departure/Election of Officers/Directors",
    "5.03": "Amendments to Articles or Bylaws",
    "5.04": "Temporary Suspension of Trading",
    "5.07": "Submission of Matters to Vote of Security Holders",
    "7.01": "Regulation FD Disclosure",
    "8.01": "Other Events",
    "9.01": "Financial Statements and Exhibits",
}

# High-impact items (used to flag in UI)
RED_FLAG_ITEMS = {"4.02", "1.03", "3.01", "5.04"}
HIGH_IMPACT_ITEMS = {"2.01", "2.02", "5.01", "5.02", "1.01"}


def _fetch(url: str, timeout: int = 20) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def fetch_recent_8k_filings(count: int = 100):
    """Fetch atom feed of latest 8-K filings."""
    url = (f"https://www.sec.gov/cgi-bin/browse-edgar"
           f"?action=getcurrent&type=8-K&output=atom&count={count}")
    raw = _fetch(url)
    root = ET.fromstring(raw)
    ns = {"a": "http://www.w3.org/2005/Atom"}

    filings = []
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

        # Extract company name from title: "8-K - APPLE INC (0000320193) (Filer)"
        company_match = re.match(r"8-K\s*[-–]\s*(.+?)\s*\(\d+\)", title)
        company = company_match.group(1).strip() if company_match else title[:80]

        # Try to extract Items from summary
        items = []
        items_match = re.findall(r"Item\s*(\d+\.\d+)", summary)
        items.extend(items_match)
        # Also from title
        title_items = re.findall(r"\b(\d\.\d{2})\b", title)
        items.extend(title_items)
        items = sorted(set(items))

        filings.append({
            "company": company,
            "accession": accession,
            "filed_at": updated,
            "items": items,
            "filing_url": link,
            "summary_snippet": summary[:200],
        })
    return filings


def lambda_handler(event, context):
    s3 = boto3.client("s3")
    started = time.time()

    try:
        filings = fetch_recent_8k_filings(count=200)
    except Exception as e:
        return {"statusCode": 502, "body": json.dumps({"error": f"SEC atom failed: {e}"})}

    # Load + merge with rolling window
    cutoff = datetime.now(timezone.utc) - timedelta(days=WINDOW_DAYS)
    existing = {}
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
        existing = json.loads(obj["Body"].read())
    except Exception:
        pass
    prior_filings = existing.get("filings", [])

    seen_accessions = set()
    merged = []
    for f in filings + prior_filings:
        if f["accession"] in seen_accessions:
            continue
        try:
            filed_dt = datetime.fromisoformat(f["filed_at"].replace("Z", "+00:00"))
        except Exception:
            filed_dt = datetime.now(timezone.utc)
        if filed_dt < cutoff:
            continue
        seen_accessions.add(f["accession"])
        merged.append(f)

    merged.sort(key=lambda x: x["filed_at"], reverse=True)

    # Aggregate by item
    by_item = defaultdict(list)
    for f in merged:
        for item in f["items"]:
            by_item[item].append({
                "company": f["company"],
                "accession": f["accession"],
                "filed_at": f["filed_at"],
                "filing_url": f.get("filing_url", ""),
            })

    last_24h = [
        f for f in merged
        if datetime.fromisoformat(f["filed_at"].replace("Z", "+00:00"))
           > datetime.now(timezone.utc) - timedelta(hours=24)
    ]

    # Red flags
    red_flag_filings = [
        f for f in merged
        if any(item in RED_FLAG_ITEMS for item in f["items"])
    ]
    red_flag_filings.sort(key=lambda x: x["filed_at"], reverse=True)

    high_impact = [
        f for f in merged
        if any(item in HIGH_IMPACT_ITEMS for item in f["items"])
    ]

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "window_days": WINDOW_DAYS,
        "stats": {
            "total_filings": len(merged),
            "last_24h": len(last_24h),
            "red_flag_filings": len(red_flag_filings),
            "high_impact_filings": len(high_impact),
            "fetch_duration_s": round(time.time() - started, 1),
        },
        "item_labels": ITEM_LABELS,
        "by_item_counts": {item: len(filings) for item, filings in by_item.items()},
        "red_flags": red_flag_filings[:20],
        "high_impact": high_impact[:30],
        "filings": merged[:300],
    }

    s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                  Body=json.dumps(output).encode(),
                  ContentType="application/json", CacheControl="no-cache")

    print(f"8-K: {len(merged)} in window, {len(last_24h)} last 24h, {len(red_flag_filings)} red flags")
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        "body": json.dumps({"ok": True, "stats": output["stats"]}),
    }
