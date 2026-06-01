"""
justhodl-ticker-deep-research
═════════════════════════════
After convergence-radar runs, generate AI research dossiers for the top
pump candidates. One Claude haiku-4-5 call produces a structured JSON
array of research for the top 15 tickers.

OUTPUT
══════
s3://justhodl-dashboard-live/data/ticker-research-bundle.json
{
  "schema_version":  "1.0",
  "generated_at":    "...",
  "model":           "claude-haiku-4-5-20251001",
  "elapsed_sec":     ...,
  "claude_elapsed":  ...,
  "n_tickers":       15,
  "research": {
    "PLTR": {
      "ticker":              "PLTR",
      "convergence_summary": {
        "n_engines":         8,
        "pump_likelihood":   54.5,
        "directional_score": 59,
        "tier":              "ULTRA"
      },
      "bull_thesis": {
        "headline":          "Single-sentence bull case",
        "thesis":            "3-4 sentence why this should pump up",
        "key_catalysts":     ["...", "...", "..."],
        "leading_signals":   ["...", "...", "..."]
      },
      "risk_assessment": {
        "headline":          "Single-sentence risk view",
        "primary_risks":     ["...", "...", "..."],
        "valuation_concern": "...",
        "what_breaks_thesis": ["...", "..."]
      },
      "trade_framework": {
        "conviction":          "HIGH | MED | LOW",
        "time_horizon":        "1-3d | 1-2w | 1-3mo",
        "what_to_watch":       ["...", "..."],
        "invalidation_signal": "...",
        "historical_analog":   "..."
      },
      "ai_one_liner":  "30-word actionable takeaway"
    },
    ...
  }
}

SCHEDULE
════════
cron(5 * * * ? *) — hourly at :05, runs ~5 min after convergence-radar :30/:00.
Updates twice an hour to keep research fresh.

CLAUDE PROMPT
═════════════
One bulk call (cost-efficient + faster than 15 parallel calls). Claude is
given the convergence-radar data for top 15 candidates and asked to produce
a JSON array of dossiers.

FRAMING (important):
- This is ANALYTICAL synthesis, not financial advice
- Output describes WHAT SIGNALS SAY, not WHAT TO DO
- Risks are observational (from engine data), not predictive
- Page UI adds a clear disclaimer ('not financial advice')
"""
import json
import os
import re
import sys
import time
import urllib.request
from datetime import datetime, timezone
from typing import Dict, List

import boto3

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

S3_BUCKET    = "justhodl-dashboard-live"
INPUT_KEY    = "data/convergence-radar.json"
OUTPUT_KEY   = "data/ticker-research-bundle.json"
MODEL        = "claude-haiku-4-5-20251001"
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

# Number of top pump candidates to research per Lambda invocation
# Lower number = more depth per ticker, less risk of token overflow.
# Each dossier needs ~1500-2000 tokens (bull + risk + framework). 10 tickers
# × 1500 = 15K tokens for response → max_tokens=16000 gives headroom.
TOP_N_TICKERS = 10

s3 = boto3.client("s3", region_name="us-east-1")


# ═════════════════════════════════════════════════════════════════════
# Prompt engineering
# ═════════════════════════════════════════════════════════════════════

SYSTEM_PROMPT = """You are a senior equity analyst writing institutional \
research dossiers. You are given convergence-radar data for the top \
pump candidates — tickers being flagged by multiple independent \
engines (options flow, momentum breakouts, earnings drift, EPS \
revisions, insider activity, etc.).

For each ticker, write a research dossier with TWO sides:
1. The BULL case — why the convergence of signals suggests upside
2. The RISK side — what could invalidate the thesis, valuation concerns, bearish signals

STYLE RULES
═══════════
- Direct, institutional-grade prose
- Specific to the engine signals provided — reference them by name
- No hedging like "could possibly" — say what the signals say
- Risks are observational from the data, not generic warnings
- Each thesis 2-3 sentences max — concise
- Per-ticker output must be valid JSON

CRITICAL GUARDRAILS
═══════════════════
- This is ANALYTICAL synthesis of engine signals, NOT trading advice
- Use language like "signals suggest", "convergence indicates", "data shows"
- NEVER include specific price targets, stop losses, or position sizes
- The "conviction" field is the analyst's confidence in the SIGNAL strength,
  not a recommendation to act
- Always include 2+ risks per ticker — pumps fail; the data must reveal what
  could go wrong

OUTPUT FORMAT — pure JSON, no markdown, no preamble
═══════════════════════════════════════════════════
{
  "research": [
    {
      "ticker":                "TICKER",
      "bull_thesis": {
        "headline":            "Single-sentence bull case",
        "thesis":              "2-3 sentence why convergence suggests upside",
        "key_catalysts":       ["catalyst 1", "catalyst 2", "catalyst 3"],
        "leading_signals":     ["engine 1 + what it shows", "engine 2 ..."]
      },
      "risk_assessment": {
        "headline":            "Single-sentence risk view",
        "primary_risks":       ["risk 1", "risk 2", "risk 3"],
        "valuation_concern":   "what valuation engines say (or 'none flagged')",
        "what_breaks_thesis":  ["specific event/signal that would invalidate", "..."]
      },
      "trade_framework": {
        "conviction":          "HIGH | MED | LOW",
        "time_horizon":        "1-3d | 1-2w | 1-3mo | 3mo+",
        "what_to_watch":       ["specific signal to confirm/deny", "..."],
        "invalidation_signal": "single signal that breaks the bull case",
        "historical_analog":   "brief comparison to past similar setup"
      },
      "ai_one_liner":  "20-30 word actionable summary"
    },
    ... (one per ticker)
  ]
}

Produce exactly one entry per ticker provided. Do not skip any.
"""


def build_user_prompt(candidates: List[dict]) -> str:
    """Compact prompt with engine signals per ticker."""
    parts = ["# Top pump candidates — convergence-radar snapshot\n"]
    parts.append(f"Generated: {datetime.now(timezone.utc).isoformat()}\n")
    parts.append(f"Total candidates to research: {len(candidates)}\n\n")

    for c in candidates:
        ticker = c["ticker"]
        parts.append(f"\n## {ticker}")
        parts.append(f"  pump_likelihood:     {c.get('pump_likelihood')}/100  ({c.get('pump_category')})")
        parts.append(f"  convergence_score:   {c.get('convergence_score')}/100")
        parts.append(f"  directional_score:   {c.get('directional_score'):+}  (range -100..+100)")
        parts.append(f"  n_engines:           {c.get('n_engines')}")
        parts.append(f"  tier:                {c.get('tier')}")
        parts.append(f"  domains:             {', '.join(c.get('domain_coverage', []))}")
        parts.append(f"  pump_components:     {c.get('pump_components', {})}")
        parts.append(f"  prior_n_engines:     {c.get('prior_n_engines', 0)}  (acceleration = {c.get('n_engines',0) - c.get('prior_n_engines',0):+})")

        # Bullish drivers (the leading signals)
        parts.append(f"\n  Bullish drivers (top {len(c.get('bullish_engines', []))}):")
        for b in c.get("bullish_engines", []):
            parts.append(f"    + {b.get('weighted', 0):>5.2f}  {b.get('engine', '')}: {b.get('note', '')}")

        # Bearish drag
        if c.get("bearish_engines"):
            parts.append(f"  Bearish drag:")
            for b in c.get("bearish_engines", []):
                parts.append(f"    {b.get('weighted', 0):>+5.2f}  {b.get('engine', '')}: {b.get('note', '')}")

        # Raw engine signal details (compact)
        parts.append(f"\n  Engine signal details:")
        for eng_name, sig in c.get("engines", {}).items():
            sig_compact = {k: v for k, v in sig.items()
                            if k not in ("raw_score", "domain") and v is not None}
            sig_str = json.dumps(sig_compact, default=str)[:240]
            parts.append(f"    {eng_name}: {sig_str}")

    parts.append("\n\nProduce the JSON research array per the system prompt format. ")
    parts.append(f"Include exactly {len(candidates)} entries in the 'research' array, one per ticker.")
    return "\n".join(parts)


def call_anthropic(system: str, user: str, max_tokens: int = 8000) -> str:
    """Call Anthropic API, return the text response."""
    if not ANTHROPIC_KEY:
        raise RuntimeError("ANTHROPIC_API_KEY not set in env")
    payload = json.dumps({
        "model": MODEL,
        "max_tokens": max_tokens,
        "system": system,
        "messages": [{"role": "user", "content": user}],
    }).encode("utf-8")
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=payload,
        headers={
            "Content-Type":      "application/json",
            "x-api-key":         ANTHROPIC_KEY,
            "anthropic-version": "2023-06-01",
        },
        method="POST",
    )
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
    """Extract first balanced JSON object from response."""
    text = re.sub(r"^```(?:json)?\s*", "", text.strip(), flags=re.MULTILINE)
    text = re.sub(r"\s*```\s*$", "", text.strip(), flags=re.MULTILINE)
    depth = 0
    start = -1
    for i, ch in enumerate(text):
        if ch == "{":
            if depth == 0:
                start = i
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0 and start >= 0:
                candidate = text[start:i+1]
                try:
                    return json.loads(candidate)
                except json.JSONDecodeError:
                    continue
    return json.loads(text)


def _try_partial_recovery(text: str) -> dict:
    """If Claude's response is truncated mid-JSON, try to recover the complete
    dossier entries that were already produced.

    Strategy: find the {"research": [ opener, then walk through dossier objects,
    keeping the ones that close balanced. Stop at the first unbalanced/truncated one.
    """
    text = re.sub(r"^```(?:json)?\s*", "", text.strip(), flags=re.MULTILINE)
    # Find research array opener
    m = re.search(r'"research"\s*:\s*\[', text)
    if not m:
        return None
    array_start = m.end()

    # Walk through the array, finding balanced top-level objects
    dossiers = []
    i = array_start
    n = len(text)
    while i < n:
        # Skip whitespace + commas
        while i < n and text[i] in " \t\n\r,":
            i += 1
        if i >= n:
            break
        if text[i] == "]":
            break
        if text[i] != "{":
            # Unexpected — likely truncated mid-object
            break
        # Walk balanced braces, tracking string state
        depth = 0
        start = i
        in_str = False
        esc = False
        valid = False
        while i < n:
            ch = text[i]
            if in_str:
                if esc:
                    esc = False
                elif ch == "\\":
                    esc = True
                elif ch == '"':
                    in_str = False
            else:
                if ch == '"':
                    in_str = True
                elif ch == "{":
                    depth += 1
                elif ch == "}":
                    depth -= 1
                    if depth == 0:
                        valid = True
                        i += 1
                        break
            i += 1
        if not valid:
            # truncated mid-object — stop, return what we have
            break
        candidate = text[start:i]
        try:
            dossiers.append(json.loads(candidate))
        except json.JSONDecodeError:
            break
    if not dossiers:
        return None
    return {"research": dossiers}


# ═════════════════════════════════════════════════════════════════════
# Lambda handler
# ═════════════════════════════════════════════════════════════════════

def lambda_handler(event, context):
    t0 = time.time()
    print(f"[deep-research] start {datetime.now(timezone.utc).isoformat()}")

    # 1. Read convergence-radar.json
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=INPUT_KEY)
        radar = json.loads(obj["Body"].read())
        radar_age_min = (datetime.now(timezone.utc) -
                          obj["LastModified"]).total_seconds() / 60
        print(f"[dr] convergence-radar loaded ({radar_age_min:.0f}min old)")
    except Exception as e:
        return _write_error(f"Failed to load radar: {e}")

    # 2. Take top N pump candidates with the highest likelihood
    pump_candidates = radar.get("pump_candidates") or []
    if not pump_candidates:
        # Fall back to top by convergence_score if no pump candidates
        all_tickers = radar.get("tickers") or []
        # Take top with directional_score > 0
        pump_candidates = [t for t in all_tickers if t.get("directional_score", 0) > 10][:TOP_N_TICKERS]
        if not pump_candidates:
            return _write_error("No pump candidates or directional tickers found")

    selected = pump_candidates[:TOP_N_TICKERS]
    print(f"[dr] researching {len(selected)} tickers: {[t['ticker'] for t in selected]}")

    # 3. Build prompt + call Claude
    user_prompt = build_user_prompt(selected)
    print(f"[dr] prompt {len(user_prompt)} chars")

    try:
        t_claude = time.time()
        response_text = call_anthropic(SYSTEM_PROMPT, user_prompt, max_tokens=16000)
        claude_elapsed = round(time.time() - t_claude, 2)
        print(f"[dr] Claude response in {claude_elapsed}s, {len(response_text)} chars")
    except Exception as e:
        return _write_error(f"Claude error: {e}")

    # 4. Parse JSON — with partial-recovery fallback for truncated responses
    parsed = None
    try:
        parsed = extract_json(response_text)
    except Exception as e:
        print(f"[dr] full JSON parse failed: {e}; trying partial recovery…")
        # Try to recover: find the last complete dossier object before the truncation
        parsed = _try_partial_recovery(response_text)
        if not parsed:
            return _write_error(f"JSON parse error: {e}",
                                  raw_preview=response_text[:600])
        else:
            print(f"[dr] recovered partial JSON with {len(parsed.get('research', []))} complete dossiers")

    research_array = parsed.get("research") or []
    if not isinstance(research_array, list):
        return _write_error("research field is not an array",
                              raw_preview=response_text[:600])

    # 5. Convert to dict keyed by ticker + attach convergence_summary
    research_by_ticker = {}
    cand_by_ticker = {c["ticker"]: c for c in selected}
    for r in research_array:
        t = r.get("ticker")
        if not t or t not in cand_by_ticker:
            continue
        cand = cand_by_ticker[t]
        research_by_ticker[t] = {
            "ticker":              t,
            "convergence_summary": {
                "n_engines":         cand.get("n_engines"),
                "pump_likelihood":   cand.get("pump_likelihood"),
                "pump_category":     cand.get("pump_category"),
                "directional_score": cand.get("directional_score"),
                "convergence_score": cand.get("convergence_score"),
                "tier":              cand.get("tier"),
                "domain_coverage":   cand.get("domain_coverage"),
                "n_bullish_eng":     cand.get("n_bullish_eng"),
                "n_bearish_eng":     cand.get("n_bearish_eng"),
            },
            "bull_thesis":      r.get("bull_thesis", {}),
            "risk_assessment":  r.get("risk_assessment", {}),
            "trade_framework":  r.get("trade_framework", {}),
            "ai_one_liner":     r.get("ai_one_liner", ""),
        }

    # 6. Build bundle + write
    bundle = {
        "schema_version":  "1.0",
        "generated_at":    datetime.now(timezone.utc).isoformat(),
        "model":           MODEL,
        "elapsed_sec":     round(time.time() - t0, 2),
        "claude_elapsed":  claude_elapsed,
        "radar_age_min":   round(radar_age_min, 1),
        "n_tickers":       len(research_by_ticker),
        "tickers":         list(research_by_ticker.keys()),
        "research":        research_by_ticker,
        "disclaimer":      ("Analytical synthesis of engine signals. Not financial advice. "
                              "Engine readings change continuously; verify before acting."),
    }

    body = json.dumps(bundle, indent=2, default=str)
    s3.put_object(
        Bucket=S3_BUCKET, Key=OUTPUT_KEY, Body=body,
        ContentType="application/json", CacheControl="max-age=600",
    )
    archive_key = (f"data/archive/ticker-research/"
                    f"{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M')}.json")
    try:
        s3.put_object(Bucket=S3_BUCKET, Key=archive_key, Body=body,
                        ContentType="application/json")
    except Exception:
        pass

    summary = {
        "status":            "ok",
        "elapsed_sec":       bundle["elapsed_sec"],
        "claude_elapsed":    claude_elapsed,
        "n_tickers":         bundle["n_tickers"],
        "tickers":           bundle["tickers"],
    }
    print(f"[deep-research] done: {summary}")
    return {"statusCode": 200, "body": json.dumps(summary)}


def _write_error(message: str, **extras) -> dict:
    """Write degraded payload."""
    payload = {
        "schema_version": "1.0",
        "generated_at":   datetime.now(timezone.utc).isoformat(),
        "status":         "error",
        "error":          message,
        **extras,
    }
    try:
        s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY,
                        Body=json.dumps(payload, default=str, indent=2),
                        ContentType="application/json", CacheControl="max-age=300")
    except Exception as e:
        print(f"[dr] error-payload write fail: {e}")
    print(f"[deep-research] ERROR: {message}")
    return {"statusCode": 500, "body": json.dumps({"status": "error", "error": message})}
