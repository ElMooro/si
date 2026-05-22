"""
justhodl-transcript-query -- TF-IDF query handler over earnings transcripts.

═══════════════════════════════════════════════════════════════════════════════
HOW TO QUERY
────────────
Invoke directly via Lambda invoke OR via function URL with POST body:
  {
    "query": "China headwinds pricing pressure",
    "ticker": "AAPL"  (optional filter; case-insensitive)
    "quarter_prefix": "2024" (optional filter; YYYY or YYYYQN)
    "limit": 10  (optional; default 10, max 25)
  }

RETURNS
───────
  {
    "ok": true,
    "query": "...",
    "n_results": ...,
    "results": [
      {
        "score": 12.34,
        "ticker": "...",
        "quarter": "...",
        "filed": "...",
        "text": "... full chunk text ...",
        "matched_terms": ["china", "headwinds", ...]
      }, ...
    ]
  }

RANKING
───────
Standard TF-IDF dot product per chunk:
  score = sum over query terms t in chunk of: tf(t, chunk) * idf(t)
Filtered chunks are excluded from ranking.

PROFESSIONAL CONSIDERATIONS
────────────────────────────
- Reads cached index from S3 once per cold start (5-30MB typical)
- Subsequent queries hot (no S3 read)
- Lambda invocations are stateless across cold starts so consumer
  apps should cache results on their side too
═══════════════════════════════════════════════════════════════════════════════
"""
import json
import math
import os
import re
import time
from collections import Counter
from datetime import datetime, timezone

import boto3

VERSION = "1.0.0"
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_INDEX_KEY = "data/transcripts-index.json"
MIN_TOKEN_LEN = 2

STOPWORDS = set("""
a an the and or but if then else when while do does did is am are was were
be been being have has had this that these those i you he she it we they
my your his its our their to of in on at by for with from
into onto upon out off as not no nor so very too just only also can could
will would may might must already still about across after during
operator thank thanks question questions next welcome ladies gentlemen
""".split())

_INDEX_CACHE = {"data": None, "loaded_at": 0}

s3 = boto3.client("s3", region_name="us-east-1")

_TOKEN_RE = re.compile(r"[a-z][a-z0-9'-]+")


def tokenize(text):
    text = text.lower()
    return [t for t in _TOKEN_RE.findall(text)
             if len(t) >= MIN_TOKEN_LEN and t not in STOPWORDS]


def load_index():
    """Cache index across warm invocations."""
    if _INDEX_CACHE["data"] is not None:
        return _INDEX_CACHE["data"]
    print(f"[query] loading index from s3://{S3_BUCKET}/{S3_INDEX_KEY}")
    obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_INDEX_KEY)
    data = json.loads(obj["Body"].read().decode("utf-8"))
    # Normalize tf dict keys back to int (they may be string after JSON)
    for c in data.get("chunks") or []:
        tf = c.get("tf") or {}
        if tf and any(isinstance(k, str) for k in tf):
            c["tf"] = {int(k): v for k, v in tf.items()}
    _INDEX_CACHE["data"] = data
    _INDEX_CACHE["loaded_at"] = time.time()
    return data


def rank(query, index, ticker_filter=None, quarter_prefix=None, limit=10):
    """Rank chunks by TF-IDF dot product with query."""
    q_tokens = tokenize(query)
    if not q_tokens:
        return []
    vocab = index["vocab"]
    idf = index["idf"]
    q_term_indices = []
    matched_terms = []
    for tk in q_tokens:
        if tk in vocab:
            q_term_indices.append(vocab[tk])
            matched_terms.append(tk)
    if not q_term_indices:
        return []

    results = []
    for c in index["chunks"]:
        if ticker_filter and c["ticker"].upper() != ticker_filter.upper():
            continue
        if quarter_prefix:
            if not (c.get("quarter") or "").startswith(quarter_prefix):
                continue
        tf = c.get("tf") or {}
        if not tf:
            continue
        score = 0.0
        chunk_matched = []
        for q_idx, tk in zip(q_term_indices, matched_terms):
            v = tf.get(q_idx, 0)
            if v:
                score += v * idf[q_idx]
                chunk_matched.append(tk)
        if score <= 0:
            continue
        results.append({
            "score": round(score, 3),
            "ticker": c["ticker"],
            "quarter": c["quarter"],
            "filed": c.get("filed"),
            "text": c["text"],
            "matched_terms": chunk_matched,
        })

    results.sort(key=lambda x: -x["score"])
    return results[:limit]


def extract_event_body(event):
    """Function URL events nest the body; direct invokes don't."""
    if not isinstance(event, dict):
        return {}
    # Function URL invocation
    body = event.get("body")
    if isinstance(body, str):
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            return {}
    if isinstance(body, dict):
        return body
    # Direct invocation
    return event


def lambda_handler(event=None, context=None):
    started = time.time()
    params = extract_event_body(event or {})
    query = (params.get("query") or "").strip()
    ticker = params.get("ticker")
    quarter_prefix = params.get("quarter_prefix")
    limit = min(int(params.get("limit") or 10), 25)

    if not query:
        return {
            "statusCode": 400,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({
                "ok": False,
                "error": "missing 'query' field in request body"}),
        }

    try:
        index = load_index()
    except Exception as e:
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({
                "ok": False,
                "error": f"index load failed: {str(e)[:200]}"}),
        }

    results = rank(query, index, ticker_filter=ticker,
                    quarter_prefix=quarter_prefix, limit=limit)

    payload = {
        "ok": True,
        "version": VERSION,
        "query": query,
        "filters": {
            "ticker": ticker,
            "quarter_prefix": quarter_prefix,
            "limit": limit,
        },
        "index_built_at": index.get("built_at"),
        "n_chunks_in_corpus": index.get("n_chunks"),
        "n_transcripts_in_corpus": index.get("n_transcripts"),
        "n_results": len(results),
        "results": results,
        "duration_ms": int((time.time() - started) * 1000),
    }
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json",
                      "Access-Control-Allow-Origin": "*"},
        "body": json.dumps(payload),
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler(
        {"query": "China headwinds", "limit": 5}), indent=2))
