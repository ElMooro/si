"""
justhodl-pump-earnings-nlp
══════════════════════════
NLP layer on earnings call transcripts, specifically for pump candidates.

DISTINCTION FROM justhodl-earnings-nlp
══════════════════════════════════════
The pre-existing justhodl-earnings-nlp covers a broad universe with general
tonal scoring. This version focuses ONLY on current pump candidates and
extracts the FORWARD-LOOKING tone shift that affects pump probability:

  - Compares last 2 quarters per ticker
  - Outputs: tone trajectory, emerging themes, cautionary signals,
    forward guidance posture, analyst Q&A pressure topics, key quotes,
    ai_synthesis (20-30 word summary of the tonal story)

Output at data/pump-earnings-nlp.json (distinct from data/earnings-nlp.json
so both Lambdas can coexist).

INPUTS
══════
data/convergence-radar.json   →  pump_candidates[]
FMP /stable/earning-call-transcript (year=YYYY, quarter=1-4)

SCHEDULE
════════
cron(0 3 * * ? *) — daily 03:00 UTC. Transcripts only update on earnings
cycle so daily is plenty.
"""
import json
import os
import re
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Dict, List, Optional

import boto3

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

S3_BUCKET     = "justhodl-dashboard-live"
RADAR_KEY     = "data/convergence-radar.json"
OUTPUT_KEY    = "data/pump-earnings-nlp.json"
MODEL         = "claude-haiku-4-5-20251001"
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
FMP_KEY       = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")

TOP_N_TICKERS = 8
TRANSCRIPT_MAX_CHARS = 18000

s3 = boto3.client("s3", region_name="us-east-1")


def fetch_transcript(ticker: str, year: int, quarter: int) -> Optional[dict]:
    try:
        url = (f"https://financialmodelingprep.com/stable/earning-call-transcript"
                f"?symbol={ticker}&year={year}&quarter={quarter}&apikey={FMP_KEY}")
        req = urllib.request.Request(url, headers={"User-Agent": "justhodl/pump-nlp"})
        with urllib.request.urlopen(req, timeout=20) as r:
            data = json.loads(r.read())
        if isinstance(data, list) and data:
            t = data[0]
            content = t.get("content", "")
            if not content or len(content) < 500:
                return None
            return {
                "ticker":  ticker,
                "period":  f"Q{t.get('period', quarter)}",
                "year":    t.get("year", year),
                "date":    t.get("date"),
                "content": content[:TRANSCRIPT_MAX_CHARS],
                "len":     len(content),
            }
        return None
    except Exception as e:
        print(f"[transcript] {ticker} {year}Q{quarter}: {str(e)[:100]}")
        return None


def fetch_recent_transcripts(ticker: str, n: int = 2) -> List[dict]:
    found: List[dict] = []
    now = datetime.now(timezone.utc)
    cur_year = now.year
    cur_q = (now.month - 1) // 3 + 1
    yq = (cur_year, cur_q)
    for _ in range(8):
        y, q = yq
        t = fetch_transcript(ticker, y, q)
        if t:
            found.append(t)
            if len(found) >= n:
                break
        if q == 1:
            yq = (y - 1, 4)
        else:
            yq = (y, q - 1)
    return found


SYSTEM_PROMPT = """You are a senior equity analyst specializing in earnings \
call transcript analysis for high-conviction pump candidates. For each ticker, \
you are given the last 2 quarters of earnings call transcripts. Your job is \
to extract tonal and substantive shifts that affect pump probability.

For each ticker, produce JSON with:

  tone_per_quarter:    {"Q2_2025": {"score": -100..+100, "summary": "1 sentence"}, ...}
                       Score = aggregate confidence (bullish guidance, positive surprises = +100;
                       cautionary, withdrawn guidance, customer churn = -100).

  tone_trajectory:     "rising" | "falling" | "stable"
                       Compare the two quarters' scores.

  tone_delta:          integer (Q_latest - Q_prior).

  emerging_themes:     ["topics management is NOW emphasizing that weren't last quarter", ...]

  fading_themes:       ["topics that have disappeared from latest call", ...]

  cautionary_signals:  ["specific phrases or topics suggesting caution", ...]
                       (Words like 'headwinds', 'mixed', 'uncertain', 'challenging',
                        guidance pull, customer pause, regulatory friction.)

  growth_language_freq: "high" | "medium" | "low"

  forward_guidance_posture: "raised" | "maintained" | "lowered" | "withdrawn" | "n/a"

  qa_pressure_topics:  ["analyst Q&A topics that drew sharp follow-ups", ...]

  key_quotes:          ["exact quote ≤30 words", ...]  (up to 3; reveal the tonal posture)

  pump_implication:    "1-2 sentence read on what this means for the current pump setup"

  ai_synthesis:        "20-30 word summary of the tonal story across the 2 quarters"

STYLE
═════
- Be specific to actual content, not generic templates
- Commit to a numeric score, don't hedge
- If transcripts are very similar (no shift), score them similarly and call it "stable"
- Don't invent themes — extract from text only

OUTPUT FORMAT — pure JSON, no markdown:
{
  "research": [ { ticker entry }, ... ]
}
Produce exactly one entry per ticker.
"""


def build_user_prompt(ticker_transcripts: Dict[str, List[dict]]) -> str:
    parts = [f"# Earnings call transcripts for {len(ticker_transcripts)} pump candidates\n"]
    for ticker, transcripts in ticker_transcripts.items():
        parts.append(f"\n\n═══ {ticker} ═══")
        for t in transcripts:
            parts.append(f"\n--- {ticker} {t['period']} {t['year']} (date: {t.get('date')}) ---")
            parts.append(t.get("content", "")[:TRANSCRIPT_MAX_CHARS])
    parts.append(f"\n\nProduce the JSON research array per the system prompt. ")
    parts.append(f"Include exactly {len(ticker_transcripts)} entries.")
    return "\n".join(parts)


def call_anthropic(system: str, user: str, max_tokens: int = 16000) -> str:
    if not ANTHROPIC_KEY:
        raise RuntimeError("ANTHROPIC_API_KEY not set in env")
    payload = json.dumps({
        "model": MODEL, "max_tokens": max_tokens,
        "system": system, "messages": [{"role": "user", "content": user}],
    }).encode("utf-8")
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages", data=payload,
        headers={"Content-Type": "application/json",
                  "x-api-key": ANTHROPIC_KEY,
                  "anthropic-version": "2023-06-01"},
        method="POST")
    with urllib.request.urlopen(req, timeout=120) as r:
        data = json.loads(r.read().decode("utf-8"))
    if not data.get("content"):
        raise RuntimeError(f"Empty response: {data}")
    text = ""
    for block in data["content"]:
        if block.get("type") == "text":
            text += block.get("text", "")
    return text.strip()


def extract_json(text: str) -> dict:
    text = re.sub(r"^```(?:json)?\s*", "", text.strip(), flags=re.MULTILINE)
    text = re.sub(r"\s*```\s*$", "", text.strip(), flags=re.MULTILINE)
    depth = 0; start = -1
    for i, ch in enumerate(text):
        if ch == "{":
            if depth == 0: start = i
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0 and start >= 0:
                try: return json.loads(text[start:i+1])
                except json.JSONDecodeError: continue
    return json.loads(text)


def try_partial_recovery(text: str) -> Optional[dict]:
    text = re.sub(r"^```(?:json)?\s*", "", text.strip(), flags=re.MULTILINE)
    m = re.search(r'"research"\s*:\s*\[', text)
    if not m: return None
    i = m.end(); n = len(text); dossiers = []
    while i < n:
        while i < n and text[i] in " \t\n\r,": i += 1
        if i >= n or text[i] == "]": break
        if text[i] != "{": break
        depth = 0; start = i; in_str = False; esc = False; valid = False
        while i < n:
            ch = text[i]
            if in_str:
                if esc: esc = False
                elif ch == "\\": esc = True
                elif ch == '"': in_str = False
            else:
                if ch == '"': in_str = True
                elif ch == "{": depth += 1
                elif ch == "}":
                    depth -= 1
                    if depth == 0:
                        valid = True; i += 1; break
            i += 1
        if not valid: break
        try: dossiers.append(json.loads(text[start:i]))
        except json.JSONDecodeError: break
    return {"research": dossiers} if dossiers else None


def lambda_handler(event, context):
    t0 = time.time()
    print(f"[pump-nlp] start {datetime.now(timezone.utc).isoformat()}")

    try:
        radar = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=RADAR_KEY)["Body"].read())
    except Exception as e:
        return _write_error(f"Failed to load radar: {e}")
    candidates = (radar.get("pump_candidates") or [])[:TOP_N_TICKERS]
    if not candidates:
        return _write_error("No pump candidates")

    tickers = [c["ticker"] for c in candidates]
    print(f"[pump-nlp] fetching transcripts for {len(tickers)} tickers")

    transcript_map: Dict[str, List[dict]] = {}
    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = {ex.submit(fetch_recent_transcripts, t, 2): t for t in tickers}
        for fut in as_completed(futures, timeout=120):
            ticker = futures[fut]
            try:
                ts = fut.result()
                if ts and len(ts) >= 1:
                    transcript_map[ticker] = ts
                    print(f"[pump-nlp]  {ticker}: {len(ts)} transcripts")
            except Exception as e:
                print(f"[pump-nlp]  {ticker}: err {str(e)[:100]}")

    if not transcript_map:
        return _write_error("No transcripts retrieved")

    user_prompt = build_user_prompt(transcript_map)
    print(f"[pump-nlp] prompt: {len(user_prompt)} chars, calling Claude")

    try:
        t_claude = time.time()
        response_text = call_anthropic(SYSTEM_PROMPT, user_prompt, max_tokens=16000)
        claude_elapsed = round(time.time() - t_claude, 2)
        print(f"[pump-nlp] Claude: {len(response_text)} chars in {claude_elapsed}s")
    except Exception as e:
        return _write_error(f"Claude error: {e}")

    parsed = None
    try:
        parsed = extract_json(response_text)
    except Exception as e:
        print(f"[pump-nlp] JSON parse failed: {e}; trying recovery")
        parsed = try_partial_recovery(response_text)
        if not parsed:
            return _write_error(f"JSON parse error: {e}", raw_preview=response_text[:600])

    research_list = parsed.get("research") or []
    research_by_ticker = {}
    for r in research_list:
        t = r.get("ticker")
        if not t or t not in transcript_map: continue
        research_by_ticker[t] = {
            "ticker":             t,
            "transcripts_used":   [{"period": x["period"], "year": x["year"], "date": x.get("date")}
                                     for x in transcript_map[t]],
            "tone_per_quarter":   r.get("tone_per_quarter", {}),
            "tone_trajectory":    r.get("tone_trajectory"),
            "tone_delta":         r.get("tone_delta"),
            "emerging_themes":    r.get("emerging_themes", []),
            "fading_themes":      r.get("fading_themes", []),
            "cautionary_signals": r.get("cautionary_signals", []),
            "growth_language_freq": r.get("growth_language_freq"),
            "forward_guidance_posture": r.get("forward_guidance_posture"),
            "qa_pressure_topics": r.get("qa_pressure_topics", []),
            "key_quotes":         r.get("key_quotes", []),
            "pump_implication":   r.get("pump_implication"),
            "ai_synthesis":       r.get("ai_synthesis"),
        }

    output = {
        "schema_version": "1.0",
        "generated_at":   datetime.now(timezone.utc).isoformat(),
        "model":          MODEL,
        "elapsed_sec":    round(time.time() - t0, 2),
        "claude_elapsed": claude_elapsed,
        "n_tickers":      len(research_by_ticker),
        "tickers":        list(research_by_ticker.keys()),
        "research":       research_by_ticker,
        "disclaimer":     ("AI-extracted tonal analysis of management commentary. "
                            "Verify against actual call audio for high-stakes decisions."),
    }

    body = json.dumps(output, indent=2, default=str)
    s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY, Body=body,
                    ContentType="application/json", CacheControl="max-age=3600")
    try:
        archive_key = (f"data/archive/pump-earnings-nlp/"
                        f"{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M')}.json")
        s3.put_object(Bucket=S3_BUCKET, Key=archive_key, Body=body,
                        ContentType="application/json")
    except Exception:
        pass

    summary = {
        "status":         "ok",
        "elapsed_sec":    output["elapsed_sec"],
        "claude_elapsed": claude_elapsed,
        "n_tickers":      output["n_tickers"],
        "tickers":        output["tickers"],
    }
    print(f"[pump-nlp] done: {summary}")
    return {"statusCode": 200, "body": json.dumps(summary)}


def _write_error(message: str, **extras) -> dict:
    payload = {"schema_version": "1.0", "generated_at": datetime.now(timezone.utc).isoformat(),
                "status": "error", "error": message, **extras}
    try:
        s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY,
                        Body=json.dumps(payload, default=str, indent=2),
                        ContentType="application/json", CacheControl="max-age=300")
    except Exception: pass
    print(f"[pump-nlp] ERROR: {message}")
    return {"statusCode": 500, "body": json.dumps({"status": "error", "error": message})}
