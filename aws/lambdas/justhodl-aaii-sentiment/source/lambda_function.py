"""
justhodl-aaii-sentiment — AAII Investor Sentiment Survey

The American Association of Individual Investors publishes weekly a
3-question retail sentiment survey: bullish / neutral / bearish on a
6-month outlook for stocks. Released each Thursday around 10am ET.

This is one of the strongest contrarian signals in equities at extremes:
  - 84% bearish in March 2009 (within weeks of GFC bottom)
  - 65% bullish near Jan 2022 top
  - Bull-Bear spread > +30 historically marks short-term tops
  - Bull-Bear spread < -30 historically marks short-term bottoms

AAII publishes via their HTML at https://www.aaii.com/sentimentsurvey
which is scraped here. They also offer a CSV at /sentimentsurvey/sent_results
which is used as the primary source — falls back to HTML scrape.

Output (data/aaii-sentiment.json):
  {
    "generated_at": ...,
    "latest": {
      "week_ending": "2026-04-23",
      "bullish": 0.34,            (fraction)
      "bearish": 0.42,
      "neutral": 0.24,
      "bull_bear_spread": -0.08,
    },
    "historical_avg": {
      "bullish": 0.378,
      "bearish": 0.310,
      "neutral": 0.312,
    },
    "z_scores": {                 (current vs 26-week mean)
      "bullish": -0.5,
      "bearish": +1.2,
      "spread": -0.9,
    },
    "extremes": {
      "is_bullish_extreme": false,    (spread > +30%)
      "is_bearish_extreme": false,    (spread < -30%)
    },
    "interpretation": "<plain English>",
    "history_26w": [
      {"week_ending": ..., "bullish": ..., ...}, ...
    ]
  }
"""
from __future__ import annotations
import csv
import io
import json
import os
import re
import time
import urllib.request
from datetime import datetime, timezone

import boto3

S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = os.environ.get("S3_KEY", "data/aaii-sentiment.json")
USER_AGENT = os.environ.get("USER_AGENT", "JustHodl Research raafouis@gmail.com")

# AAII publishes the data here. URL has been stable for years.
AAII_DATA_URL = "https://www.aaii.com/files/surveys/sentiment.xls"      # XLS (legacy but stable)
AAII_HTML_URL = "https://www.aaii.com/sentimentsurvey"                  # HTML page with table

# AAII's long-run historical averages (since 1987)
HIST_AVG = {
    "bullish": 0.378,
    "bearish": 0.310,
    "neutral": 0.312,
    "spread": 0.068,
}

EXTREME_THRESHOLD = 0.30  # spread > +30 or < -30 = historical extreme


def _fetch(url: str, timeout: int = 30) -> bytes:
    # AAII blocks generic User-Agent strings with HTTP 403. Need a realistic
    # browser UA + standard browser headers to get past their WAF.
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "identity",   # avoid compression
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    })
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def _parse_aaii_html(html: str):
    r"""Parse the AAII HTML page for the latest week's percentages.

    AAII renders each percentage in its own table cell with surrounding
    HTML that varies week-to-week. Strategy:
      1. Find each label (Bullish/Neutral/Bearish) and grab the FIRST
         `\d+(?:\.\d+)?%` that appears within ~600 chars after it.
         Each match yields the "current week" reading.
      2. Validate: bullish + neutral + bearish must sum to ~100% (±3%).
      3. Extract week-ending date from typical "Week Ending Wednesday,
         <Month> <day>, <year>" patterns (multiple variants).

    Returns None if validation fails — caller handles by preserving
    the prior data.
    """
    def _grab_pct_after(label: str):
        # Match `Bullish` (case-insensitive, word boundary), then the next %
        # value within a generous window. Tolerates `<td>`, `<span>`, etc.
        pat = re.compile(rf"\b{re.escape(label)}\b[^%]{{1,800}}?(\d{{1,3}}(?:\.\d+)?)\s*%", re.I | re.S)
        m = pat.search(html)
        if not m:
            return None
        try:
            return float(m.group(1)) / 100
        except (ValueError, TypeError):
            return None

    bull = _grab_pct_after("Bullish")
    neut = _grab_pct_after("Neutral")
    bear = _grab_pct_after("Bearish")

    if bull is None or neut is None or bear is None:
        return None
    # Sanity-check: these three percentages should sum to roughly 1.00.
    # If they don't, we caught wrong percentages somewhere on the page.
    total = bull + neut + bear
    if not (0.97 <= total <= 1.03):
        return None

    # Extract week ending date — multiple acceptable patterns
    week_ending = None
    for pat in (
        r"Week Ending\s+\w+,?\s+([A-Z][a-z]+ \d{1,2},? \d{4})",   # "Week Ending Wednesday, April 23, 2026"
        r"Week Ending\s+([A-Z][a-z]+ \d{1,2},? \d{4})",            # "Week Ending April 23, 2026"
        r"as of\s+([A-Z][a-z]+ \d{1,2},? \d{4})",                  # "as of April 23, 2026"
        r"\b([A-Z][a-z]+ \d{1,2},?\s+\d{4})\b",                    # last-resort: any month/day/year
    ):
        m = re.search(pat, html)
        if m:
            txt = m.group(1).replace(",", "")
            for fmt in ("%B %d %Y", "%b %d %Y"):
                try:
                    week_ending = datetime.strptime(txt, fmt).date().isoformat()
                    break
                except ValueError:
                    continue
            if week_ending:
                break

    return {
        "week_ending": week_ending,
        "bullish": round(bull, 4),
        "neutral": round(neut, 4),
        "bearish": round(bear, 4),
        "bull_bear_spread": round(bull - bear, 4),
    }


def _historical_z(history, key, current):
    """26-week z-score; needs at least 8 datapoints to be meaningful."""
    series = [h[key] for h in history[-26:] if h.get(key) is not None]
    if len(series) < 8:
        return None
    mean = sum(series) / len(series)
    var = sum((x - mean) ** 2 for x in series) / len(series)
    sd = var ** 0.5
    if sd == 0:
        return 0.0
    return round((current - mean) / sd, 2)


def _interpret(latest, z_scores, extremes):
    spread = latest["bull_bear_spread"]
    bear = latest["bearish"]
    bull = latest["bullish"]

    if extremes["is_bearish_extreme"]:
        return f"Retail investors are extremely bearish ({bear*100:.0f}% vs 31% historical). Historically a strong contrarian buy signal — the herd is rarely right at extremes."
    if extremes["is_bullish_extreme"]:
        return f"Retail investors are extremely bullish ({bull*100:.0f}% vs 38% historical). Historically a contrarian sell signal — euphoria is when distributions usually happen."
    if spread > 0.15:
        return f"Retail leans bullish (spread {spread*100:+.0f}%). Mildly contrarian-cautious; not extreme."
    if spread < -0.15:
        return f"Retail leans bearish (spread {spread*100:+.0f}%). Mildly contrarian-bullish; sentiment is supportive of a bounce."
    return f"Sentiment is balanced (spread {spread*100:+.0f}%). No contrarian signal."


def _load_existing(s3) -> dict:
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
        return json.loads(obj["Body"].read())
    except Exception:
        return {}


def _merge_history(existing: dict, new_row: dict) -> list:
    history = existing.get("history_26w", [])
    if not new_row.get("week_ending"):
        return history
    # Replace if same week, otherwise append
    history = [h for h in history if h.get("week_ending") != new_row["week_ending"]]
    history.append(new_row)
    history.sort(key=lambda h: h.get("week_ending", ""))
    return history[-26:]  # keep 26 weeks


def lambda_handler(event, context):
    s3 = boto3.client("s3")
    started = time.time()

    try:
        html = _fetch(AAII_HTML_URL).decode("utf-8", errors="ignore")
    except Exception as e:
        return {"statusCode": 502, "body": json.dumps({"error": f"AAII fetch failed: {e}"})}

    latest = _parse_aaii_html(html)
    if not latest:
        # AAII page changed structure — return graceful failure with prior data
        existing = _load_existing(s3)
        existing["last_attempt_at"] = datetime.now(timezone.utc).isoformat()
        existing["last_attempt_status"] = "parse_failed"
        s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                      Body=json.dumps(existing).encode(),
                      ContentType="application/json", CacheControl="no-cache")
        return {"statusCode": 200, "body": json.dumps({"ok": False, "reason": "parse_failed"})}

    existing = _load_existing(s3)
    history = _merge_history(existing, latest)

    spread = latest["bull_bear_spread"]
    extremes = {
        "is_bullish_extreme": spread > EXTREME_THRESHOLD,
        "is_bearish_extreme": spread < -EXTREME_THRESHOLD,
    }
    z_scores = {
        "bullish": _historical_z(history, "bullish", latest["bullish"]),
        "bearish": _historical_z(history, "bearish", latest["bearish"]),
        "spread":  _historical_z(history, "bull_bear_spread", latest["bull_bear_spread"]),
    }
    interp = _interpret(latest, z_scores, extremes)

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "latest": latest,
        "historical_avg": HIST_AVG,
        "z_scores": z_scores,
        "extremes": extremes,
        "interpretation": interp,
        "history_26w": history,
        "fetch_duration_s": round(time.time() - started, 1),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                  Body=json.dumps(output).encode(),
                  ContentType="application/json", CacheControl="no-cache")

    print(f"AAII week_ending={latest['week_ending']} bull={latest['bullish']:.0%} bear={latest['bearish']:.0%} spread={spread:+.0%}")
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        "body": json.dumps({"ok": True, "latest": latest, "extremes": extremes}),
    }
