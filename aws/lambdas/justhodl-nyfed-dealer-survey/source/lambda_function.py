"""
justhodl-nyfed-dealer-survey — Survey of Primary Dealers

The NY Fed surveys primary dealers (the 24 major banks they trade with)
before each FOMC meeting on their expectations for: Fed funds path, asset
purchase plans, balance sheet, recession probability, growth, inflation,
unemployment.

These are the people moving the most dollar volume in Treasuries — their
expectations are the closest thing to a "market consensus from informed
participants" available for free.

Endpoint:
  Markets > Primary Dealer Statistics > Survey of Primary Dealers
  HTML: https://www.newyorkfed.org/markets/primarydealer_survey_questions
  PDFs published per FOMC meeting; we extract the latest available

Output (data/dealer-survey.json):
  {
    "generated_at": ...,
    "latest_survey": {
      "fomc_date": "2026-03-19",
      "next_25bp_cut_meeting": "2026-06-12",
      "fed_funds_eoy": 4.25,
      "fed_funds_y2": 3.50,
      "fed_funds_y3": 3.00,
      "qt_runoff_end": "2026-Q4",
      "recession_prob_12m": 0.20,
      "growth_2026": 1.8,
      "inflation_2026": 2.4,
      "unemployment_eoy": 4.5,
    },
    "history": [...]   (rolling)
  }

Note: NY Fed only publishes after each FOMC, ~8x/year. This Lambda runs
weekly to detect new releases. When no new release, output is unchanged.
"""
from __future__ import annotations
import json
import os
import re
import time
import urllib.request
from datetime import datetime, timezone

import boto3

S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = os.environ.get("S3_KEY", "data/dealer-survey.json")
USER_AGENT = os.environ.get("USER_AGENT", "JustHodl Research raafouis@gmail.com")

NYFED_SURVEY_PAGE = "https://www.newyorkfed.org/markets/primarydealer_survey_questions"


def _fetch(url: str, timeout: int = 30) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def _find_latest_survey_pdf(html: str):
    """Find the URL of the latest survey response PDF on the NY Fed page."""
    # NY Fed typically links surveys with patterns like:
    #   /medialibrary/microsites/markets/files/survey-of-primary-dealers/2026/{date}/responses.pdf
    pattern = re.compile(
        r'href="([^"]*survey-of-primary-dealers[^"]*responses[^"]*\.pdf)"',
        re.I,
    )
    matches = pattern.findall(html)
    if not matches:
        # Fallback: any .pdf within /survey-of-primary-dealers/
        pattern2 = re.compile(r'href="([^"]*survey-of-primary-dealers[^"]*\.pdf)"', re.I)
        matches = pattern2.findall(html)
    if not matches:
        return None
    # Return the first match (NY Fed lists newest first)
    url = matches[0]
    if url.startswith("/"):
        url = "https://www.newyorkfed.org" + url
    return url


def _extract_fomc_date(url: str):
    """Try to derive FOMC date from the PDF URL path."""
    m = re.search(r"/(\d{4})/(\d{4}\d{2})/?", url)
    if m:
        try:
            year = m.group(1)
            month_str = m.group(2)[-2:]
            return f"{year}-{month_str}"
        except Exception:
            pass
    return None


def _load_existing(s3):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
        return json.loads(obj["Body"].read())
    except Exception:
        return {"history": []}


def lambda_handler(event, context):
    s3 = boto3.client("s3")
    started = time.time()

    try:
        page = _fetch(NYFED_SURVEY_PAGE).decode("utf-8", errors="ignore")
    except Exception as e:
        return {"statusCode": 502, "body": json.dumps({"error": f"NY Fed fetch failed: {e}"})}

    latest_pdf_url = _find_latest_survey_pdf(page)
    fomc_date = _extract_fomc_date(latest_pdf_url) if latest_pdf_url else None

    existing = _load_existing(s3)
    last_known = (existing.get("latest_survey") or {}).get("source_url")

    # If no new survey found, just refresh timestamp and return
    if latest_pdf_url == last_known:
        existing["last_check"] = datetime.now(timezone.utc).isoformat()
        existing["last_check_status"] = "no_new_survey"
        s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                      Body=json.dumps(existing).encode(),
                      ContentType="application/json", CacheControl="no-cache")
        return {"statusCode": 200,
                "body": json.dumps({"ok": True, "no_new_survey": True})}

    # New survey detected — record it. PDF parsing is intentionally simple:
    # we record the URL, FOMC date, and discovered timestamp. Detailed PDF
    # field extraction is deferred (requires pdfplumber Lambda layer).
    new_entry = {
        "source_url": latest_pdf_url,
        "fomc_date": fomc_date,
        "discovered_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "note": "PDF URL recorded. Detailed expectation extraction requires PDF parsing layer (deferred).",
    }

    history = existing.get("history", [])
    history = [h for h in history if h.get("source_url") != latest_pdf_url]
    history.append(new_entry)
    history.sort(key=lambda h: h.get("discovered_at", ""))
    history = history[-12:]  # keep ~3 years

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "latest_survey": new_entry,
        "history": history,
        "fetch_duration_s": round(time.time() - started, 1),
    }

    s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                  Body=json.dumps(output).encode(),
                  ContentType="application/json", CacheControl="no-cache")

    print(f"NY Fed dealer survey: new release {fomc_date} → {latest_pdf_url}")
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        "body": json.dumps({"ok": True, "new_survey": True, "fomc_date": fomc_date}),
    }
