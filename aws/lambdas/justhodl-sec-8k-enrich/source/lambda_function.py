"""
justhodl-sec-8k-enrich — 8-K enrichment layer (Bloomberg parity 2/10)

The canonical 8-K pipeline (justhodl-sec-8k) is UNTOUCHED. This Lambda reads
data/8k-filings.json and produces enriched derivatives:

  data/8k-filings-enriched.json — same top-level shape; each filing carries:
      ticker, cik, items_source, items_confidence, headline,
      items_fulltext, primary_doc_url, red_flag
  data/8k-by-ticker.json        — {TICKER: [{accession, filed_at, items,
                                    headline, filing_url, red_flag}]}
                                    newest-first per ticker

Enrichment sources (all free, all fail-soft):
  - SEC company_tickers.json       -> CIK -> ticker (weekly S3 cache)
  - data.sec.gov submissions JSON  -> accession -> primaryDocument URL
  - Full filing HTML               -> Item headings + headline extraction

Rate discipline: 0.3s pacing between SEC requests (SEC limit: 10 req/s).
Full-text is fetched ONLY when needed: items empty, or items carry a
red-flag / high-impact code. Docs are cached at data/8k-docs/{acc}.txt
so steady-state runs are nearly free.
"""
from __future__ import annotations

import gzip
import html
import json
import os
import re
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_INPUT_KEY = os.environ.get("S3_INPUT_KEY", "data/8k-filings.json")
S3_ENRICHED_KEY = "data/8k-filings-enriched.json"
S3_BY_TICKER_KEY = "data/8k-by-ticker.json"
S3_CIK_MAP_KEY = "data/sec-cik-ticker-map.json"
S3_DOC_PREFIX = "data/8k-docs/"

USER_AGENT = "JustHodl.ai contact@justhodl.ai"
SEC_PACING_S = 0.3            # SEC allows 10 req/s; stay well under it
CIK_MAP_MAX_AGE_DAYS = 7
DOC_CACHE_MAX_AGE_S = 7 * 86400

RED_FLAG_ITEMS = {"4.02", "1.03", "3.01", "5.04"}
HIGH_IMPACT_ITEMS = {"2.01", "2.02", "5.01", "5.02", "1.01"}
FULLTEXT_TRIGGER_ITEMS = RED_FLAG_ITEMS | HIGH_IMPACT_ITEMS | {"2.04", "2.06"}

_BOILERPLATE_RES = [
    re.compile(p, re.IGNORECASE) for p in (
        r"united states", r"securities and exchange commission", r"form 8-k",
        r"commission file number", r"exact name of registrant",
        r"table of contents", r"check the appropriate box",
        r"emerging growth company",
    )
]

s3 = boto3.client("s3", region_name="us-east-1")
_last_sec_call = 0.0


# ═════════════════════════════════════════════════════════════════════
# SEC HTTP with rate pacing (fail-soft: bytes or None)
# ═════════════════════════════════════════════════════════════════════
def _sec_get(url: str, timeout: int = 25) -> bytes | None:
    """GET a SEC URL with pacing. Returns body bytes, or None on any failure."""
    global _last_sec_call
    wait = SEC_PACING_S - (time.monotonic() - _last_sec_call)
    if wait > 0:
        time.sleep(wait)
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": USER_AGENT,
            "Accept-Encoding": "gzip",
            "Accept": "*/*",
        })
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read()
            if resp.headers.get("Content-Encoding") == "gzip":
                body = gzip.decompress(body)
            return body
    except urllib.error.HTTPError as e:
        print(f"[8k-enrich] SEC HTTP {e.code}: {url[:90]}")
    except urllib.error.URLError as e:
        print(f"[8k-enrich] SEC URL error ({e.reason}): {url[:90]}")
    except (TimeoutError, OSError) as e:
        print(f"[8k-enrich] SEC network error ({type(e).__name__}): {url[:90]}")
    finally:
        _last_sec_call = time.monotonic()
    return None


# ═════════════════════════════════════════════════════════════════════
# CIK -> ticker map (weekly-refreshed S3 cache)
# ═════════════════════════════════════════════════════════════════════
def load_cik_ticker_map() -> dict:
    """Return {cik_str: ticker}. Uses S3 cache; refreshes from SEC when stale."""
    now = datetime.now(timezone.utc)
    try:
        head = s3.head_object(Bucket=S3_BUCKET, Key=S3_CIK_MAP_KEY)
        age_days = (now - head["LastModified"]).total_seconds() / 86400
        obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_CIK_MAP_KEY)
        cached = json.loads(obj["Body"].read().decode("utf-8"))
        if isinstance(cached.get("map"), dict) and age_days <= CIK_MAP_MAX_AGE_DAYS:
            print(f"[8k-enrich] CIK map cache hit "
                  f"({len(cached['map'])} entries, {age_days:.1f}d old)")
            return cached["map"]
        print(f"[8k-enrich] CIK map stale ({age_days:.1f}d), refreshing")
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") not in ("404", "NoSuchKey"):
            print(f"[8k-enrich] CIK map cache read failed: {e}")
    except (ValueError, KeyError, UnicodeDecodeError) as e:
        print(f"[8k-enrich] CIK map cache corrupt ({type(e).__name__}), refreshing")

    raw = _sec_get("https://www.sec.gov/files/company_tickers.json")
    if not raw:
        return {}
    try:
        data = json.loads(raw.decode("utf-8"))
        cmap = {str(int(v["cik_str"])): v["ticker"] for v in data.values()
                if isinstance(v, dict) and v.get("cik_str") and v.get("ticker")}
    except (ValueError, KeyError, TypeError) as e:
        print(f"[8k-enrich] company_tickers.json parse failed: {e}")
        return {}
    try:
        s3.put_object(
            Bucket=S3_BUCKET, Key=S3_CIK_MAP_KEY,
            Body=json.dumps({
                "generated_at": now.isoformat(), "count": len(cmap), "map": cmap,
            }).encode(),
            ContentType="application/json", CacheControl="public, max-age=86400")
    except ClientError as e:
        print(f"[8k-enrich] CIK map cache write failed: {e}")
    print(f"[8k-enrich] CIK map refreshed: {len(cmap)} entries")
    return cmap


# ═════════════════════════════════════════════════════════════════════
# Accession -> primary document URL
# ═════════════════════════════════════════════════════════════════════
def _cik_from_filing(filing: dict) -> str | None:
    """Extract numeric CIK from the filing's SEC archive URL (/data/{cik}/...)."""
    url = filing.get("filing_url") or ""
    m = (re.search(r"/Archives/edgar/data/(\d+)/", url)
         or re.search(r"[?&]CIK=(\d+)", url))
    if m:
        return str(int(m.group(1)))
    return None


def resolve_primary_doc(cik: str, accession: str) -> str | None:
    """Resolve an 8-K accession to its primary document URL via submissions JSON.

    Returns the full https://www.sec.gov/Archives/... URL, or None (fail-soft).
    """
    raw = _sec_get(f"https://data.sec.gov/submissions/CIK{cik.zfill(10)}.json")
    if not raw:
        return None
    try:
        subs = json.loads(raw.decode("utf-8"))
        recent = subs["filings"]["recent"]
        accs = recent["accessionNumber"]
        docs = recent["primaryDocument"]
        forms = recent.get("form", [])
    except (ValueError, KeyError, TypeError) as e:
        print(f"[8k-enrich] submissions parse failed for CIK {cik}: {e}")
        return None
    for i, acc in enumerate(accs):
        if acc == accession and i < len(docs):
            if forms and i < len(forms) and forms[i] != "8-K":
                continue
            if docs[i]:
                noslash = accession.replace("-", "")
                return (f"https://www.sec.gov/Archives/edgar/data/"
                        f"{int(cik)}/{noslash}/{docs[i]}")
    return None


# ═════════════════════════════════════════════════════════════════════
# Full-text item + headline extraction
# ═════════════════════════════════════════════════════════════════════
def _strip_html(raw: str) -> str:
    """Remove tags/scripts/comments, unescape entities, collapse whitespace.

    Block-level tags become newlines so paragraphs survive as units.
    """
    text = re.sub(r"(?is)<(script|style).*?</\1>", "\n", raw)
    text = re.sub(r"(?s)<!--.*?-->", "\n", text)
    text = re.sub(r"(?i)</?(p|div|h[1-6]|br|tr|td|li|hr|table|section|article)[^>]*>",
                  "\n", text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    text = re.sub(r"[ \t\xa0]+", " ", text)
    text = re.sub(r"\n[ \t]*\n+", "\n\n", text)
    return text.strip()


def _is_boilerplate(paragraph: str) -> bool:
    """True for SEC cover-page text, tables, and signature blocks."""
    if len(paragraph) < 40:
        return True
    if sum(c.isdigit() for c in paragraph) > len(paragraph) * 0.4:
        return True
    return any(rx.search(paragraph) for rx in _BOILERPLATE_RES)


def extract_items_and_headline(doc_text: str) -> tuple[list, str | None]:
    """Extract Item headings and a headline from filing HTML/text.

    Returns (items, headline). items is a sorted list like ["1.01", "5.02"].
    headline is the first 2-3 sentences of the first meaningful paragraph
    (>100 chars, not boilerplate), or None when nothing suitable is found.
    """
    text = _strip_html(doc_text)
    items = sorted({m.group(1)
                    for m in re.finditer(r"Item\s+(\d\.\d{2})", text, re.IGNORECASE)})

    headline = None
    for para in text.split("\n\n"):
        para = para.strip()
        if len(para) < 100 or _is_boilerplate(para):
            continue
        sentences = re.split(r"(?<=[.!?])\s+", para)
        chosen, total = [], 0
        for s in sentences:
            s = s.strip()
            if len(s) < 20:
                continue
            chosen.append(s)
            total += len(s)
            if len(chosen) >= 3 or total > 400:
                break
        if total > 100 and len(chosen) >= 2:
            headline = " ".join(chosen)
            break
    return items, headline


def _needs_fulltext(filing: dict) -> bool:
    """Fetch full text when atom items are missing or carry red-flag/high-impact codes."""
    items = set(filing.get("items") or [])
    return not items or bool(items & FULLTEXT_TRIGGER_ITEMS)


# ═════════════════════════════════════════════════════════════════════
# Enrichment orchestration
# ═════════════════════════════════════════════════════════════════════
def enrich_filings() -> dict:
    """Enrich canonical 8-K filings with tickers + full-text headlines."""
    started = time.time()
    stats = {"input_filings": 0, "reused": 0, "newly_enriched": 0,
             "tickers_resolved": 0, "fulltext_attempted": 0,
             "fulltext_ok": 0, "fulltext_cache_hits": 0, "red_flags": 0}

    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_INPUT_KEY)
        canonical = json.loads(obj["Body"].read().decode("utf-8"))
    except ClientError as e:
        print(f"[8k-enrich] cannot read {S3_INPUT_KEY}: {e}; nothing to enrich")
        return {"ok": False, "error": "input_missing", **stats}
    except (ValueError, UnicodeDecodeError) as e:
        print(f"[8k-enrich] input JSON corrupt: {e}")
        return {"ok": False, "error": "input_corrupt", **stats}

    filings = canonical.get("filings", []) or []
    stats["input_filings"] = len(filings)

    # Dedupe: skip accessions already enriched in a prior run.
    prior: dict = {}
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_ENRICHED_KEY)
        for rec in json.loads(obj["Body"].read().decode("utf-8")).get("filings", []):
            if rec.get("accession"):
                prior[rec["accession"]] = rec
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") not in ("404", "NoSuchKey"):
            print(f"[8k-enrich] prior enriched read failed: {e}")
    except (ValueError, UnicodeDecodeError) as e:
        print(f"[8k-enrich] prior enriched corrupt ({type(e).__name__}); re-enriching all")

    cik_map = load_cik_ticker_map()

    enriched = []
    for filing in filings:
        acc = filing.get("accession")
        if not acc:
            continue
        if acc in prior:
            enriched.append(prior[acc])
            stats["reused"] += 1
            continue

        cik = _cik_from_filing(filing)
        ticker = cik_map.get(cik) if cik else None
        if ticker:
            stats["tickers_resolved"] += 1

        atom_items = sorted(set(filing.get("items") or []))
        items = atom_items
        source = "atom"
        confidence = 0.6 if atom_items else 0.0
        headline = None
        items_fulltext = None
        primary_doc_url = None

        if _needs_fulltext(filing) and cik:
            stats["fulltext_attempted"] += 1
            doc_key = f"{S3_DOC_PREFIX}{acc}.txt"
            doc_text = None
            try:
                obj = s3.get_object(Bucket=S3_BUCKET, Key=doc_key)
                doc_text = obj["Body"].read().decode("utf-8", errors="replace")
                stats["fulltext_cache_hits"] += 1
            except ClientError as e:
                if e.response.get("Error", {}).get("Code") not in ("404", "NoSuchKey"):
                    print(f"[8k-enrich] doc cache read failed for {acc}: {e}")
            except (UnicodeDecodeError, ValueError) as e:
                print(f"[8k-enrich] doc cache corrupt for {acc}: {e}")

            if doc_text is None:
                primary_doc_url = resolve_primary_doc(cik, acc)
                if primary_doc_url:
                    raw = _sec_get(primary_doc_url)
                    if raw:
                        doc_text = raw.decode("utf-8", errors="replace")
                        try:
                            s3.put_object(
                                Bucket=S3_BUCKET, Key=doc_key,
                                Body=doc_text.encode("utf-8"),
                                ContentType="text/plain; charset=utf-8",
                                CacheControl="public, max-age=604800")
                        except ClientError as e:
                            print(f"[8k-enrich] doc cache write failed for {acc}: {e}")

            if doc_text:
                ft_items, headline = extract_items_and_headline(doc_text)
                ft_set, atom_set = set(ft_items), set(atom_items)
                if atom_set and ft_set:
                    items = sorted(atom_set | ft_set)
                    source = "atom+fulltext"
                    confidence = 1.0 if atom_set == ft_set else 0.7
                elif ft_set:
                    items = sorted(ft_set)
                    source = "fulltext"
                    confidence = 0.85
                items_fulltext = sorted(ft_set) if ft_set else []
                if ft_set:
                    stats["fulltext_ok"] += 1

        red_flag = bool(set(items) & RED_FLAG_ITEMS)
        if red_flag:
            stats["red_flags"] += 1

        enriched.append({
            **filing,
            "ticker": ticker,
            "cik": cik,
            "items": items,
            "items_source": source,
            "items_confidence": confidence,
            "items_fulltext": items_fulltext,
            "headline": headline,
            "primary_doc_url": primary_doc_url,
            "red_flag": red_flag,
        })
        stats["newly_enriched"] += 1

    enriched.sort(key=lambda r: r.get("filed_at", ""), reverse=True)

    by_ticker: dict[str, list] = {}
    for rec in enriched:
        t = rec.get("ticker")
        if not t:
            continue
        by_ticker.setdefault(t, []).append({
            "accession": rec["accession"],
            "filed_at": rec.get("filed_at"),
            "items": rec.get("items", []),
            "headline": rec.get("headline"),
            "filing_url": rec.get("filing_url"),
            "red_flag": rec.get("red_flag", False),
        })
    for t in by_ticker:
        by_ticker[t].sort(key=lambda e: e.get("filed_at", ""), reverse=True)
        by_ticker[t] = by_ticker[t][:50]

    now = datetime.now(timezone.utc)
    red_flag_recs = [r for r in enriched if r.get("red_flag")][:20]
    hi_recs = [r for r in enriched
               if set(r.get("items", [])) & (RED_FLAG_ITEMS | HIGH_IMPACT_ITEMS)][:30]

    output = {
        "generated_at": now.isoformat(timespec="seconds"),
        "window_days": canonical.get("window_days"),
        "stats": {**canonical.get("stats", {}),
                  "enrichment": stats,
                  "enriched_total": len(enriched),
                  "tickers_covered": len(by_ticker)},
        "item_labels": canonical.get("item_labels", {}),
        "by_item_counts": canonical.get("by_item_counts", {}),
        "red_flags": red_flag_recs,
        "high_impact": hi_recs,
        "filings": enriched[:300],
    }
    try:
        s3.put_object(Bucket=S3_BUCKET, Key=S3_ENRICHED_KEY,
                      Body=json.dumps(output).encode(),
                      ContentType="application/json", CacheControl="no-cache")
        s3.put_object(Bucket=S3_BUCKET, Key=S3_BY_TICKER_KEY,
                      Body=json.dumps({
                          "generated_at": now.isoformat(timespec="seconds"),
                          "tickers": len(by_ticker),
                          "by_ticker": by_ticker,
                      }).encode(),
                      ContentType="application/json", CacheControl="no-cache")
    except ClientError as e:
        print(f"[8k-enrich] output write failed: {e}")
        return {"ok": False, "error": "output_write_failed", **stats}

    stats["duration_s"] = round(time.time() - started, 1)
    print(f"[8k-enrich] {stats['newly_enriched']} new, {stats['reused']} reused, "
          f"{stats['tickers_resolved']} tickers, {stats['fulltext_ok']} fulltext, "
          f"{stats['red_flags']} red flags")
    return {"ok": True, **stats}


def lambda_handler(event, context):
    """Enrich the canonical 8-K feed with tickers and full-text headlines."""
    if isinstance(event, dict) and ("httpMethod" in event or "requestContext" in event):
        return {"statusCode": 409,
                "body": "Enrichment runs on schedule; HTTP does not acquire SEC data."}
    try:
        result = enrich_filings()
    except ClientError as e:
        print(f"[8k-enrich] S3 failure: {e}")
        return {"statusCode": 503,
                "body": json.dumps({"ok": False, "error": "s3_failure"})}
    except (ValueError, KeyError, TypeError) as e:
        print(f"[8k-enrich] unexpected {type(e).__name__}: {e}")
        return {"statusCode": 500,
                "body": json.dumps({"ok": False, "error": "internal"})}
    status = 200 if result.get("ok") else 502
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json",
                    "Access-Control-Allow-Origin": "*"},
        "body": json.dumps(result),
    }
