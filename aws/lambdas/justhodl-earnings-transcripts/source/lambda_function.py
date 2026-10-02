"""
justhodl-earnings-transcripts — Quarterly earnings call transcripts + tone signals.

SOURCE: The Motley Fool (fool.com/earnings/call-transcripts).
Why: it is the only free, full-text, non-paywalled transcript source.
Seeking Alpha transcripts are paywalled and Yahoo Finance transcripts
require a paid subscription (only a ~2.4k-char preview is free).

Discovery: fool.com monthly sitemaps (/sitemap/YYYY/MM) enumerate every
transcript URL (slug contains ticker, quarter, year). The transcript URL
pattern is /earnings/call-transcripts/YYYY/MM/DD/<slug>-qN-YYYY-earnings-call-transcript/.

Writes: data/earnings-transcripts.json
  {generated_at, source, by_ticker: {TICKER: {quarter, call_date, tone_score,
   positive_hits, negative_hits, excerpt, source_url, history: [...]}}, coverage}

Tone: word-boundary counts of positive words
  (growth, strong, record, momentum, confident) vs negative words
  (headwind, decline, challenging, weak, cautious);
  tone_score = (pos - neg) / max(1, pos + neg)  in [-1, 1].

Fail-soft: every network call is wrapped in try/except; the handler never
crashes and always writes the output file, even if partial or empty.

Time budget: total fetch work is capped at ~660s (leaving headroom under
the 900s Lambda timeout). Phase 1: sitemaps. Phase 2: latest quarter per
ticker. Phase 3: older quarters (up to 4) with whatever budget remains.
"""
from __future__ import annotations

import json
import re
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone

import boto3

BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/earnings-transcripts.json"
VERSION = "1.0"

s3 = boto3.client("s3", "us-east-1")

TICKERS = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "AVGO",
    "LLY", "JPM", "V", "XOM", "UNH", "MA", "HD", "PG", "ORCL", "COST",
    "NFLX", "CRM", "AMD", "BAC", "WMT", "ABBV", "KO", "DIS", "MRK",
    "PFE", "INTC", "T",
]

POSITIVE_WORDS = ["growth", "strong", "record", "momentum", "confident"]
NEGATIVE_WORDS = ["headwind", "decline", "challenging", "weak", "cautious"]

FOOL_BASE = "https://www.fool.com"
SITEMAP_MONTHS = 12          # look back this many months for transcripts
MAX_QUARTERS = 4            # latest N quarters per ticker
BUDGET_S = 660              # hard cap on fetch work (timeout is 900s)
FETCH_TIMEOUT = 20          # per-request timeout (seconds)
MAX_HTML_BYTES = 600_000    # cap per transcript page read
EXCERPT_LEN = 2000

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/126.0.0.0 Safari/537.36"),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "identity",
}

TRANSCRIPT_URL_RE = re.compile(
    r"https://www\.fool\.com/earnings/call-transcripts/"
    r"\d{4}/\d{2}/\d{2}/[a-z0-9\-]+-earnings-call-transcript/?",
    re.IGNORECASE,
)


def _fetch(url: str) -> str | None:
    """Fetch a URL with browser-like headers; None on any failure."""
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=FETCH_TIMEOUT) as resp:
            if getattr(resp, "status", 200) != 200:
                return None
            return resp.read(MAX_HTML_BYTES).decode("utf-8", "replace")
    except Exception:  # noqa: BLE001 - fail-soft
        return None


def _sitemap_months() -> list[str]:
    """Last SITEMAP_MONTHS months as YYYY/MM strings, newest first."""
    now = datetime.now(timezone.utc)
    months = []
    y, m = now.year, now.month
    for _ in range(SITEMAP_MONTHS):
        months.append(f"{y:04d}/{m:02d}")
        m -= 1
        if m == 0:
            m, y = 12, y - 1
    return months


def _ticker_from_slug(slug: str) -> str | None:
    """Extract ticker from a transcript slug.

    Slugs look like `walmart-wmt-q4-2026-earnings-call-transcript` or
    `axt-axti-q1-2026-earnings-call-transcript` — the token immediately
    before the qN token is the ticker.
    """
    parts = slug.split("-")
    for i, part in enumerate(parts):
        if re.fullmatch(r"q[1-4]", part) and i > 0:
            return parts[i - 1].upper()
    return None


def _quarter_year_from_slug(slug: str) -> tuple[str, str] | None:
    """Return (quarter_label, year) e.g. ('Q2 2026', '2026') from slug."""
    parts = slug.split("-")
    for i, part in enumerate(parts):
        m = re.fullmatch(r"q([1-4])", part)
        if m and i + 1 < len(parts) and re.fullmatch(r"\d{4}", parts[i + 1]):
            return f"Q{m.group(1)} {parts[i + 1]}", parts[i + 1]
    return None


def _discover_transcripts(deadline: float) -> dict:
    """Scrape fool.com sitemaps -> {TICKER: [(call_date, url, quarter)]} newest first."""
    found: dict = {}
    for ym in _sitemap_months():
        if time.time() > deadline:
            break
        body = _fetch(f"{FOOL_BASE}/sitemap/{ym}")
        if not body:
            continue
        for url in set(TRANSCRIPT_URL_RE.findall(body)):
            slug = url.rstrip("/").rsplit("/", 1)[-1].lower()
            ticker = _ticker_from_slug(slug)
            if ticker not in TICKERS:
                continue
            qy = _quarter_year_from_slug(slug)
            if not qy:
                continue
            quarter, _year = qy
            dm = re.search(r"/call-transcripts/(\d{4})/(\d{2})/(\d{2})/", url)
            call_date = f"{dm.group(1)}-{dm.group(2)}-{dm.group(3)}" if dm else ""
            entry = (call_date, url, quarter)
            if entry not in found.setdefault(ticker, []):
                found[ticker].append(entry)
    for ticker in found:
        # newest first; keep latest MAX_QUARTERS
        found[ticker] = sorted(found[ticker], reverse=True)[:MAX_QUARTERS]
    return found


def _extract_transcript_text(html: str) -> str:
    """Pull the transcript body text out of a Fool transcript page."""
    html = re.sub(r"(?is)<script.*?</script>|<style.*?</style>", " ", html)
    start = html.find("Full Conference Call Transcript")
    if start < 0:
        start = 0
    seg = html[start:]
    # cut off footer / recommendations sections
    for marker in ("More Like This", "Recommended for you", "Related Articles",
                   "id=\"comments\"", "<footer", "Need a quote from a Motley Fool"):
        idx = seg.find(marker)
        if idx > len(seg) // 2:  # only cut if marker is well past the start
            seg = seg[:idx]
            break
    text = re.sub(r"(?s)<[^>]+>", " ", seg)
    text = re.sub(r"Full Conference Call Transcript", " ", text, count=1)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _tone(text: str) -> tuple[float, int, int]:
    low = text.lower()
    pos = sum(len(re.findall(r"\b%s\b" % w, low)) for w in POSITIVE_WORDS)
    neg = sum(len(re.findall(r"\b%s\b" % w, low)) for w in NEGATIVE_WORDS)
    score = round((pos - neg) / max(1, pos + neg), 3)
    return score, pos, neg


def _analyze(url: str) -> dict | None:
    """Fetch + analyze one transcript page; None on failure."""
    html = _fetch(url)
    if not html or len(html) < 2000:
        return None
    text = _extract_transcript_text(html)
    if len(text) < 500:
        return None
    score, pos, neg = _tone(text)
    slug = url.rstrip("/").rsplit("/", 1)[-1]
    qy = _quarter_year_from_slug(slug)
    quarter = qy[0] if qy else ""
    dm = re.search(r"/call-transcripts/(\d{4})/(\d{2})/(\d{2})/", url)
    call_date = f"{dm.group(1)}-{dm.group(2)}-{dm.group(3)}" if dm else ""
    excerpt = text[:EXCERPT_LEN]
    return {
        "quarter": quarter,
        "call_date": call_date,
        "tone_score": score,
        "positive_hits": pos,
        "negative_hits": neg,
        "excerpt": excerpt,
        "source_url": url,
    }


def lambda_handler(event, context):
    t0 = time.time()
    deadline = t0 + BUDGET_S
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    by_ticker: dict = {}
    errors: list = []

    try:
        discovered = _discover_transcripts(deadline - 240)
    except Exception as e:  # noqa: BLE001
        discovered = {}
        errors.append(f"sitemap discovery failed: {type(e).__name__}")

    fetches = 0
    # Phase 2: latest quarter for every discovered ticker
    pending = []
    for ticker in TICKERS:
        entries = discovered.get(ticker)
        if not entries:
            continue
        pending.append((ticker, 0, entries[0]))
    # Phase 3: older quarters with remaining budget
    for ticker in TICKERS:
        entries = discovered.get(ticker)
        if not entries:
            continue
        for qi in range(1, len(entries)):
            pending.append((ticker, qi, entries[qi]))

    history: dict = {}
    for ticker, qi, (call_date, url, quarter) in pending:
        if time.time() > deadline:
            errors.append("fetch budget exhausted; some quarters skipped")
            break
        fetches += 1
        try:
            row = _analyze(url)
        except Exception:  # noqa: BLE001
            row = None
        if row:
            history.setdefault(ticker, []).append(row)

    for ticker in TICKERS:
        rows = history.get(ticker)
        if not rows:
            continue
        latest = rows[0]
        by_ticker[ticker] = {
            "quarter": latest["quarter"],
            "call_date": latest["call_date"],
            "tone_score": latest["tone_score"],
            "positive_hits": latest["positive_hits"],
            "negative_hits": latest["negative_hits"],
            "excerpt": latest["excerpt"],
            "source_url": latest["source_url"],
            "history": rows,  # latest 4 quarters, newest first
        }

    payload = {
        "contract": "earnings-transcripts.v1",
        "version": VERSION,
        "generated_at": now,
        "source": "motley-fool",
        "by_ticker": by_ticker,
        "coverage": len(by_ticker),
        "universe": len(TICKERS),
        "missed_tickers": [t for t in TICKERS if t not in by_ticker],
        "fetches": fetches,
        "elapsed_s": round(time.time() - t0, 1),
        "errors": errors[:10],
    }

    try:
        body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False,
                          allow_nan=False, default=str)
        s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=body.encode(),
                      ContentType="application/json",
                      CacheControl="max-age=3600")
        written = True
    except Exception as e:  # noqa: BLE001
        written = False
        errors.append(f"s3 write failed: {type(e).__name__}")

    return {
        "ok": True,
        "out": OUT_KEY,
        "coverage": len(by_ticker),
        "tickers": sorted(by_ticker),
        "written": written,
        "errors": errors[:10],
        "elapsed_s": round(time.time() - t0, 1),
    }
