"""
justhodl-earnings-sentiment — Earnings Call Transcript Sentiment Analyzer

WHY THIS IS A KILLER FEATURE
─────────────────────────────
Earnings call transcripts contain rich forward-looking signals that
basic financials miss: tone shifts, hedged language, confidence cues,
specific product mentions, customer churn warnings. FMP Ultimate unlocks
the raw transcript text. This Lambda feeds each transcript through
claude-haiku-4-5 for structured sentiment + theme extraction.

PIPELINE (runs daily at 10:00 UTC):
  1. Fetch earnings calendar for past 7 days
  2. For each company with a transcript, check if we already scored it
     (S3 sidecar at screener/earnings-sentiment-state.json holds processed keys)
  3. Fetch full transcript via /stable/earning-call-transcript
  4. Truncate to ~12K chars (keeps prompt cost low)
  5. Send to claude-haiku-4-5 with structured-output prompt asking for:
       - overall_sentiment (-100 to +100)
       - confidence_score (-100 to +100) - how confident does mgmt sound
       - forward_guidance ('raised'|'lowered'|'maintained'|'withdrawn'|'none')
       - key_positives (3-5 bullets)
       - key_concerns (3-5 bullets)
       - one_line_summary
  6. Store result in S3 at screener/earnings-sentiment.json (cumulative)
  7. Update state sidecar with processed transcript_key

The screener Lambda will later read this S3 file to enrich stocks
with their latest call's sentiment score.

OUTPUT SCHEMA (screener/earnings-sentiment.json):
{
  "generated_at": iso8601,
  "transcripts": [
    {
      "symbol": "AAPL",
      "transcript_date": "2026-04-30",
      "fiscal_year": 2026, "quarter": 2,
      "scored_at": iso8601,
      "overall_sentiment": 42,
      "confidence_score": 65,
      "forward_guidance": "raised",
      "key_positives": ["AI feature uptake exceeded plan", ...],
      "key_concerns": ["China softness persists", ...],
      "one_line_summary": "Solid quarter with raised AI guidance...",
      "model": "claude-haiku-4-5-20251001",
      "tokens_in": 5234, "tokens_out": 412
    }, ...
  ],
  "summary": {
    "n_transcripts": int,
    "n_new_this_run": int,
    "most_bullish": [top 5 by sentiment],
    "most_bearish": [bottom 5],
    "guidance_changes": {raised: n, lowered: n, maintained: n, ...}
  }
}
"""
import json
import os
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor

import boto3

# ───── CONFIG ─────
FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
ANTHROPIC_MODEL = os.environ.get("ANTHROPIC_MODEL", "claude-haiku-4-5-20251001")
FMP_BASE = "https://financialmodelingprep.com/stable"
ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"

S3_BUCKET = "justhodl-dashboard-live"
S3_KEY = "screener/earnings-sentiment.json"
S3_STATE_KEY = "screener/earnings-sentiment-state.json"

# How far back to scan for new transcripts
LOOKBACK_DAYS = 7
# Max transcript length sent to Claude (~12K chars ≈ 3K tokens)
TRANSCRIPT_MAX_CHARS = 12000
# Max stocks scored per Lambda invocation (caps cost + runtime)
MAX_NEW_PER_RUN = 30
# How many cumulative records to retain in output (keep most-recent N)
MAX_RECORDS = 800

s3 = boto3.client("s3", region_name="us-east-1")


def fmp(path, params="", retries=2):
    url = f"{FMP_BASE}/{path}?apikey={FMP_KEY}{params}"
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-ES/1.0"})
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503) and attempt < retries:
                time.sleep(0.5 * (attempt + 1))
                continue
            print(f"[fmp] {path}: HTTP {e.code}")
            return None
        except Exception as e:
            if attempt < retries:
                time.sleep(0.5 * (attempt + 1))
                continue
            print(f"[fmp] {path}: {e}")
            return None
    return None


def call_claude(transcript_text, company_name, symbol):
    """Score a transcript with claude-haiku-4-5. Returns structured JSON."""
    if not ANTHROPIC_API_KEY:
        return {"error": "no_api_key"}

    truncated = transcript_text[:TRANSCRIPT_MAX_CHARS]
    prompt = f"""You are analyzing an earnings call transcript for {company_name} ({symbol}).

TRANSCRIPT (may be truncated):
{truncated}

Score this call and return ONLY a valid JSON object — no other text, no markdown fences.

SCHEMA:
{{
  "overall_sentiment": <int -100 to +100, where +100=extremely bullish tone, -100=extremely bearish>,
  "confidence_score": <int -100 to +100, where +100=executives sound very confident, -100=very hedged/uncertain>,
  "forward_guidance": <"raised" | "lowered" | "maintained" | "withdrawn" | "none">,
  "key_positives": [<3-5 short bullets, each 3-12 words>],
  "key_concerns": [<3-5 short bullets, each 3-12 words>],
  "one_line_summary": <one sentence 12-22 words capturing the call's main takeaway>,
  "themes": [<2-4 tags like "AI", "China", "margin pressure", "guidance raise", "buyback">]
}}

Focus on signal over noise. Look at:
- Tone shifts (e.g. "we're really excited" vs "we remain cautious")
- Specific numbers + comparisons (beats/misses, YoY changes)
- Hedged language (might/could/may vs will/expect)
- New vs reiterated guidance
- Question-and-answer hedging from management
- Anything that materially affects the forward outlook
"""

    body = {
        "model": ANTHROPIC_MODEL,
        "max_tokens": 800,
        "messages": [{"role": "user", "content": prompt}],
    }
    headers = {
        "content-type": "application/json",
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
    }
    try:
        req = urllib.request.Request(
            ANTHROPIC_URL,
            data=json.dumps(body).encode("utf-8"),
            headers=headers,
            method="POST")
        with urllib.request.urlopen(req, timeout=60) as r:
            resp = json.loads(r.read().decode("utf-8"))

        text = "".join(c.get("text", "") for c in resp.get("content", [])
                          if c.get("type") == "text").strip()
        # Strip markdown fences if any
        if text.startswith("```"):
            lines = text.split("\n")
            text = "\n".join(l for l in lines[1:] if not l.startswith("```"))
        parsed = json.loads(text)
        # Attach usage stats
        usage = resp.get("usage") or {}
        parsed["tokens_in"] = usage.get("input_tokens")
        parsed["tokens_out"] = usage.get("output_tokens")
        parsed["model"] = ANTHROPIC_MODEL
        return parsed
    except urllib.error.HTTPError as e:
        err_body = ""
        try: err_body = e.read().decode("utf-8")[:200]
        except: pass
        return {"error": f"http_{e.code}", "detail": err_body}
    except Exception as e:
        return {"error": str(e)[:200]}


def load_state():
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_STATE_KEY)
        return set(json.loads(obj["Body"].read()).get("processed_keys") or [])
    except s3.exceptions.NoSuchKey:
        return set()
    except Exception as e:
        print(f"[state] load err: {e}")
        return set()


def save_state(keys):
    body = json.dumps({
        "processed_keys": list(keys)[-5000:],
        "last_run": datetime.now(timezone.utc).isoformat(),
    }, separators=(",", ":"))
    s3.put_object(Bucket=S3_BUCKET, Key=S3_STATE_KEY, Body=body,
                   ContentType="application/json")


def load_existing_results():
    """Load cumulative results so we can append new + retain historical."""
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
        return json.loads(obj["Body"].read())
    except s3.exceptions.NoSuchKey:
        return {"transcripts": []}
    except Exception as e:
        print(f"[load] err: {e}")
        return {"transcripts": []}


def find_new_transcripts(state):
    """Return list of (symbol, year, quarter, date) tuples for transcripts
    we haven't yet scored. Uses earnings-calendar to find recent companies."""
    today = datetime.now(timezone.utc).date()
    start = (today - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    end = today.strftime("%Y-%m-%d")

    cal = fmp("earnings-calendar", f"&from={start}&to={end}")
    if not isinstance(cal, list):
        return []

    candidates = []
    seen = set()
    for e in cal:
        sym = e.get("symbol")
        if not sym or sym in seen:
            continue
        seen.add(sym)
        # Check if transcript exists by fetching dates list
        dates = fmp("earning-call-transcript-dates", f"&symbol={sym}")
        if not isinstance(dates, list) or not dates:
            continue
        # Pick most recent transcript
        dates.sort(key=lambda r: r.get("date", ""), reverse=True)
        latest = dates[0]
        year = latest.get("fiscalYear") or latest.get("year")
        quarter = latest.get("quarter")
        dt = latest.get("date", "")[:10]
        if not (year and quarter and dt):
            continue
        # Skip if too old (more than LOOKBACK_DAYS)
        try:
            dt_obj = datetime.strptime(dt, "%Y-%m-%d").date()
            if (today - dt_obj).days > LOOKBACK_DAYS:
                continue
        except Exception:
            pass
        key = f"{sym}|{year}|{quarter}"
        if key in state:
            continue
        candidates.append({"symbol": sym, "year": int(year),
                            "quarter": int(quarter), "date": dt,
                            "name": e.get("symbol")})
        if len(candidates) >= MAX_NEW_PER_RUN:
            break
    return candidates


def score_one(candidate):
    sym = candidate["symbol"]
    year = candidate["year"]
    quarter = candidate["quarter"]

    # Fetch transcript content
    transcript = fmp("earning-call-transcript",
                       f"&symbol={sym}&year={year}&quarter={quarter}")
    if not isinstance(transcript, list) or not transcript:
        return None
    text = transcript[0].get("content", "") or ""
    if len(text) < 500:
        return None  # Too short, probably empty

    # Get company name (best effort)
    company_name = sym
    try:
        prof = fmp("profile", f"&symbol={sym}")
        if isinstance(prof, list) and prof:
            company_name = prof[0].get("companyName") or sym
    except Exception:
        pass

    scored = call_claude(text, company_name, sym)
    if "error" in scored:
        print(f"[claude] {sym}: {scored.get('error')}")
        return None

    return {
        "symbol": sym,
        "name": company_name,
        "transcript_date": candidate["date"],
        "fiscal_year": year,
        "quarter": quarter,
        "scored_at": datetime.now(timezone.utc).isoformat(),
        "overall_sentiment": scored.get("overall_sentiment"),
        "confidence_score": scored.get("confidence_score"),
        "forward_guidance": scored.get("forward_guidance"),
        "key_positives": scored.get("key_positives") or [],
        "key_concerns": scored.get("key_concerns") or [],
        "one_line_summary": scored.get("one_line_summary"),
        "themes": scored.get("themes") or [],
        "tokens_in": scored.get("tokens_in"),
        "tokens_out": scored.get("tokens_out"),
        "model": scored.get("model"),
        "_key": f"{sym}|{year}|{quarter}",
    }


def build_summary(results):
    valid = [r for r in results if r.get("overall_sentiment") is not None]
    most_bullish = sorted(valid, key=lambda r: -(r.get("overall_sentiment") or 0))[:10]
    most_bearish = sorted(valid, key=lambda r: (r.get("overall_sentiment") or 0))[:10]
    guidance = {}
    for r in valid:
        g = r.get("forward_guidance") or "none"
        guidance[g] = guidance.get(g, 0) + 1
    return {
        "n_transcripts": len(valid),
        "most_bullish": [{"symbol": r["symbol"], "name": r.get("name"),
                            "sentiment": r["overall_sentiment"],
                            "confidence": r.get("confidence_score"),
                            "summary": r.get("one_line_summary"),
                            "date": r.get("transcript_date")}
                           for r in most_bullish],
        "most_bearish": [{"symbol": r["symbol"], "name": r.get("name"),
                            "sentiment": r["overall_sentiment"],
                            "confidence": r.get("confidence_score"),
                            "summary": r.get("one_line_summary"),
                            "date": r.get("transcript_date")}
                           for r in most_bearish],
        "guidance_changes": guidance,
    }


def lambda_handler(event, context):
    started = time.time()
    state = load_state()
    existing = load_existing_results()
    existing_records = existing.get("transcripts") or []
    print(f"[init] state: {len(state)} processed · existing: {len(existing_records)}")

    # Find new transcripts to score
    candidates = find_new_transcripts(state)
    print(f"[find] {len(candidates)} new transcripts to score")

    # Score in parallel (3 workers — Claude API rate limits + cost control)
    new_results = []
    if candidates:
        with ThreadPoolExecutor(max_workers=3) as ex:
            for result in ex.map(score_one, candidates):
                if result:
                    new_results.append(result)
                    state.add(result["_key"])

    print(f"[scored] {len(new_results)} successful · {len(state)} total state")

    # Merge: new results + existing (capped at MAX_RECORDS, most-recent kept)
    merged = new_results + existing_records
    # De-dup by _key
    seen_keys = set()
    deduped = []
    for r in merged:
        k = r.get("_key") or f"{r.get('symbol')}|{r.get('fiscal_year')}|{r.get('quarter')}"
        if k in seen_keys:
            continue
        seen_keys.add(k)
        deduped.append(r)
    deduped = deduped[:MAX_RECORDS]

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_seconds": round(time.time() - started, 1),
        "n_new_this_run": len(new_results),
        "n_candidates": len(candidates),
        "summary": build_summary(deduped),
        "transcripts": deduped,
    }

    try:
        s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                       Body=json.dumps(payload, default=str),
                       ContentType="application/json",
                       CacheControl="public, max-age=600")
        save_state(state)
        print(f"[s3] wrote {len(deduped)} records to {S3_KEY}")
    except Exception as e:
        print(f"[s3] write err: {e}")

    return {"statusCode": 200, "body": json.dumps({
        "candidates": len(candidates),
        "new_scored": len(new_results),
        "total_records": len(deduped),
        "elapsed_seconds": payload["elapsed_seconds"],
    })}
