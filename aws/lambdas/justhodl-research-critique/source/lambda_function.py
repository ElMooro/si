"""justhodl-research-critique

Devil's Advocate review of the equity-research Lambda's verdict.

A second model (default Anthropic Sonnet 4.6, optionally OpenAI o3-mini
when OPENAI_API_KEY is provisioned) pressure-tests the analyst's thesis
and produces:
  - anti_thesis           : the strongest 200-word case AGAINST the analyst
  - data_reinterpretations: 3-5 metrics analyst read too charitably
  - underweighted_risks   : risks insufficiently weighted in the analyst's verdict
  - bear_case_strengtheners: factors that push bear case probability higher
  - alternative_rating    : independent verdict (might disagree with analyst)
  - alternative_pt        : alternative 12m price target
  - disagreement_score    : 0-100 (0 = full concur, 100 = total disagreement)
  - summary_1liner        : 10-15 word headline

WHY THIS PATTERN (not naive dual-ensemble)
══════════════════════════════════════════
Two analysts producing independent verdicts and averaging is what consulting
firms do. Hedge funds do differently: senior risk officers ATTACK the
junior analyst's thesis specifically. The signal isn't "AI #2 says BUY too" —
it's "AI #2 says this specific data point is being read too favorably."
That structured pushback is what actually moves a PM's conviction.

MODELS
══════
Default: claude-sonnet-4-6  (different model size + family vs research Haiku,
                              works today with existing ANTHROPIC_API_KEY)
Override: OPENAI_MODEL=o3-mini-2025-XX-XX + OPENAI_API_KEY → uses OpenAI
          for true cross-architecture diversity

Lambda is invoked async by the prewarm pipeline after each research succeeds.
Doesn't slow down the research path because it runs in parallel.
"""
import json
import os
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from typing import Optional

import boto3

S3_BUCKET = "justhodl-dashboard-live"
RESEARCH_PREFIX = "equity-research/"
OUTPUT_PREFIX = "equity-critique/"

ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
OPENAI_KEY = os.environ.get("OPENAI_API_KEY", "")

# Model selection:
#   - If OPENAI_API_KEY is provisioned → use OpenAI for true cross-architecture ensemble
#   - Otherwise → Claude Sonnet 4.6 (different model size + reasoning depth vs research Haiku)
ANTHROPIC_MODEL = os.environ.get("ANTHROPIC_CRITIC_MODEL", "claude-sonnet-4-6")
OPENAI_MODEL = os.environ.get("OPENAI_CRITIC_MODEL", "o3-mini")
CRITIQUE_TIMEOUT = int(os.environ.get("CRITIQUE_TIMEOUT", "90"))

s3 = boto3.client("s3", region_name="us-east-1")


# ═════════════════════════════════════════════════════════════════════
# Critique prompt (institutional Devil's Advocate framing)
# ═════════════════════════════════════════════════════════════════════
CRITIQUE_SYSTEM = """You are a senior risk officer at a hedge fund reviewing a research analyst's verdict.

Your job is NOT to write your own research from scratch. Your job is to find what's
WRONG, weak, or under-weighted in the analyst's existing thesis. You bring an
adversarial, data-driven, deeply skeptical eye.

═══════════════════════════════════════════════════════════════════════════
WHAT MAKES A GOOD DEVIL'S ADVOCATE CRITIQUE
═══════════════════════════════════════════════════════════════════════════

GOOD critique:
- Specific data citations ("ROIC of 12% looks healthy in isolation, but it's
  been declining from 18% over 3 years — that trend is the story, not the level")
- Identifies CONFIRMATION BIAS in the analyst's framing ("analyst cites strong
  Q4 beats but doesn't mention the magnitude has shrunk from $0.15 to $0.04
  over 4 quarters — beats are becoming pyrrhic")
- Names UNDERWEIGHTED risks with evidence ("competitive intensity wasn't even
  mentioned, despite 3 new entrants in the last 18 months and gross margin
  compression from 68% → 62%")
- Constructs a credible BEAR PATH that the analyst dismissed too quickly
- Reinterprets the same data through a more skeptical lens

BAD critique (avoid):
- Generic skepticism ("but what if recession?")
- Inventing data that wasn't in the source
- Pushing back on every point — pick the 3-5 strongest pushbacks
- Reciting the analyst's bear case back to them — go BEYOND what they listed

═══════════════════════════════════════════════════════════════════════════
DISAGREEMENT SCORE RUBRIC
═══════════════════════════════════════════════════════════════════════════

You ALSO output a 0-100 disagreement score. This is what a PM uses to weight
your critique. Use this rubric:

   0-15  : You concur with the analyst on rating + PT. Their thesis holds up.
           Minor data quibbles only.
  15-35  : Same rating direction. Some material pushbacks but the thesis
           survives. PT within 15% of analyst's.
  35-60  : Meaningful disagreement. You'd shade the rating one step
           (BUY → HOLD or HOLD → SELL). PT spread 15-30%.
  60-85  : Strong disagreement. You'd rate it in the opposite direction
           (BUY → SELL or vice versa). PT spread > 30%.
  85-100 : Total disagreement. The analyst's thesis is fundamentally wrong
           and the evidence in the data points to the opposite conclusion.

Be honest. Most calls produce 15-35 disagreement (the analyst is competent
but PMs care most about the things you specifically pushed back on).

═══════════════════════════════════════════════════════════════════════════
INPUT YOU'LL RECEIVE
═══════════════════════════════════════════════════════════════════════════

You'll receive a JSON object with:
  - The full financial data (income/balance/cash flow, ratios, growth,
    returns, peer comparisons, insider activity, earnings transcript)
  - The analyst's verdict (rating, conviction, PT, scenarios, key drivers,
    risks they identified, valuation assessment, etc.)

═══════════════════════════════════════════════════════════════════════════
OUTPUT FORMAT
═══════════════════════════════════════════════════════════════════════════

OUTPUT JSON ONLY, no markdown fences, no preamble. Schema:

{
  "anti_thesis": "150-200 word case AGAINST the analyst's verdict. Construct
                  a credible path where the thesis breaks. Cite specific
                  numbers from the data.",
  "data_reinterpretations": [
    {
      "metric": "specific metric name + value, e.g. 'ROIC 12% (down from 18% in 2022)'",
      "analyst_view": "how the analyst framed it",
      "skeptical_view": "more skeptical framing with the same data"
    },
    ... (3-5 of these)
  ],
  "underweighted_risks": [
    {
      "risk": "specific risk the analyst didn't adequately weight",
      "evidence": "data citation that supports this risk being material"
    },
    ... (2-4 of these)
  ],
  "bear_case_strengtheners": [
    "specific factor that would push bear case probability higher",
    ... (2-3 of these)
  ],
  "alternative_rating": "STRONG_BUY|BUY|HOLD|SELL|STRONG_SELL",
  "alternative_pt": <number — your independent 12m PT in USD>,
  "alternative_thesis_1liner": "10-15 word summary of why you'd rate it differently (or 'concur' if same rating)",
  "disagreement_score": <0-100 per rubric above>,
  "key_disagreement_1liner": "10-15 word headline naming where you disagree MOST with the analyst"
}"""


# ═════════════════════════════════════════════════════════════════════
# Model callers
# ═════════════════════════════════════════════════════════════════════
def _post_json(url: str, headers: dict, payload: bytes, timeout: int) -> dict:
    req = urllib.request.Request(url, data=payload, headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def call_anthropic(system: str, user: str, max_tokens: int = 4000) -> tuple:
    """Returns (text, usage_dict)."""
    if not ANTHROPIC_KEY:
        raise RuntimeError("ANTHROPIC_API_KEY not set")
    payload = json.dumps({
        "model": ANTHROPIC_MODEL,
        "max_tokens": max_tokens,
        # Cache the system prompt — same 1h TTL pattern as research Lambda
        "system": [{
            "type": "text",
            "text": system,
            "cache_control": {"type": "ephemeral", "ttl": "1h"},
        }],
        "messages": [{"role": "user", "content": user}],
    }).encode()
    headers = {
        "Content-Type": "application/json",
        "x-api-key": ANTHROPIC_KEY,
        "anthropic-version": "2023-06-01",
        "anthropic-beta": "extended-cache-ttl-2025-04-11",
    }
    data = _post_json("https://api.anthropic.com/v1/messages",
                       headers, payload, CRITIQUE_TIMEOUT)
    if not data.get("content"):
        raise RuntimeError(f"Empty Anthropic response: {data}")
    text = "".join(b.get("text", "") for b in data["content"] if b.get("type") == "text").strip()
    return text, data.get("usage", {}) or {}


def call_openai(system: str, user: str, max_tokens: int = 4000) -> tuple:
    """Returns (text, usage_dict). Uses chat/completions for compat."""
    if not OPENAI_KEY:
        raise RuntimeError("OPENAI_API_KEY not set")
    payload = json.dumps({
        "model": OPENAI_MODEL,
        "max_completion_tokens": max_tokens,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
    }).encode()
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {OPENAI_KEY}",
    }
    data = _post_json("https://api.openai.com/v1/chat/completions",
                       headers, payload, CRITIQUE_TIMEOUT)
    choices = data.get("choices") or []
    if not choices:
        raise RuntimeError(f"Empty OpenAI response: {data}")
    text = (choices[0].get("message") or {}).get("content", "").strip()
    return text, data.get("usage", {}) or {}


def critique_via_chosen_model(system: str, user: str) -> dict:
    """Tries OpenAI first if key available, else Anthropic Sonnet.

    Returns dict with text, usage, model_used, cost_estimate_usd, elapsed_s.
    """
    t0 = time.time()
    if OPENAI_KEY:
        try:
            text, usage = call_openai(system, user)
            # OpenAI o3-mini pricing (rough): $0.30/MTok in, $1.20/MTok out
            in_tok = usage.get("prompt_tokens", 0)
            out_tok = usage.get("completion_tokens", 0)
            cost = round((in_tok * 0.30 + out_tok * 1.20) / 1_000_000, 6)
            return {
                "model_used": OPENAI_MODEL, "provider": "openai",
                "text": text, "usage": usage, "cost_usd": cost,
                "elapsed_s": round(time.time() - t0, 1),
            }
        except Exception as e:
            print(f"[critique] OpenAI failed, falling back to Anthropic: {e}")

    text, usage = call_anthropic(system, user)
    # Sonnet 4.6 pricing: $3/MTok in, $15/MTok out, cache_read $0.30/MTok
    in_tok = usage.get("input_tokens", 0)
    cache_read = usage.get("cache_read_input_tokens", 0)
    cache_create = usage.get("cache_creation_input_tokens", 0)
    out_tok = usage.get("output_tokens", 0)
    cost = round(
        (in_tok * 3.00 + cache_create * 3.75 + cache_read * 0.30 + out_tok * 15.00) / 1_000_000,
        6,
    )
    return {
        "model_used": ANTHROPIC_MODEL, "provider": "anthropic",
        "text": text, "usage": usage, "cost_usd": cost,
        "elapsed_s": round(time.time() - t0, 1),
    }


# ═════════════════════════════════════════════════════════════════════
# JSON parsing
# ═════════════════════════════════════════════════════════════════════
def parse_critique_json(text: str) -> dict:
    """Strip markdown fences if present, find balanced JSON object."""
    import re
    text = re.sub(r"^```(?:json)?\s*", "", text.strip(), flags=re.MULTILINE)
    text = re.sub(r"\s*```\s*$", "", text.strip(), flags=re.MULTILINE)
    depth, start = 0, -1
    for i, ch in enumerate(text):
        if ch == "{":
            if start == -1:
                start = i
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0 and start != -1:
                return json.loads(text[start:i+1])
    return json.loads(text)


def fetch_research(ticker: str) -> Optional[dict]:
    """Read cached research from S3."""
    key = f"{RESEARCH_PREFIX}{ticker}.json"
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception as e:
        print(f"[fetch_research] {key}: {e}")
        return None


def write_critique(ticker: str, critique: dict) -> str:
    """Persist critique to S3."""
    key = f"{OUTPUT_PREFIX}{ticker}.json"
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(critique, default=str).encode(),
        ContentType="application/json",
        CacheControl="public, max-age=600",
    )
    return key


# ═════════════════════════════════════════════════════════════════════
# Handler
# ═════════════════════════════════════════════════════════════════════
def lambda_handler(event, context):
    t0 = time.time()
    # Routing: HTTP query string OR direct invoke payload
    ticker = None
    research = None
    if isinstance(event, dict):
        qs = event.get("queryStringParameters") or {}
        if qs.get("ticker"):
            ticker = (qs["ticker"] or "").upper().strip()
        elif event.get("ticker"):
            ticker = str(event["ticker"]).upper().strip()
            research = event.get("research")  # optional inline research

    if not ticker:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "ticker is required (?ticker=AAPL or {ticker: AAPL})"})
        }

    print(f"[critique] {ticker} starting")

    # Get research data
    if not research:
        research = fetch_research(ticker)
        if not research:
            return {
                "statusCode": 404,
                "body": json.dumps({
                    "error": f"No cached research for {ticker}. Run /equity-research?ticker={ticker} first.",
                    "ticker": ticker,
                }),
            }

    # Strip the giant 'transcript' field if present (saves prompt tokens)
    # The analyst already incorporated its analysis; the critique doesn't need
    # raw transcript text.
    research_compact = {k: v for k, v in research.items() if k != "transcript"}

    # ETF flow context — the critic's most powerful new attack vector.
    # If the analyst is bullish a sector but sector ETF flows are negative,
    # that's structured pushback supported by institutional positioning data.
    flow_context_str = ""
    try:
        sector = (research.get("company") or {}).get("sector")
        if sector:
            obj = s3.get_object(Bucket=S3_BUCKET, Key="etf-flows/per-ticker-context.json")
            ctx = json.loads(obj["Body"].read())
            by_sector = (ctx.get("context") or {}).get("by_sector") or {}
            sector_ctx = by_sector.get(sector)
            if sector_ctx and sector_ctx.get("prompt_snippet"):
                flow_context_str = (
                    "\n\n[ETF FLOW CONTEXT — use this as ammunition for pressure-testing]\n"
                    + sector_ctx["prompt_snippet"]
                    + "\nIf the analyst is bullish but the sector has heavy outflow, "
                    "or vice versa, this is a powerful contradicting signal worth "
                    "raising in your underweighted_risks or data_reinterpretations.\n"
                )
    except Exception as e:
        print(f"[critique] flow context unavailable: {e}")

    # Build user prompt
    user_prompt = (
        f"Pressure-test the analyst's research on "
        f"{(research.get('company') or {}).get('name','?')} ({ticker}).\n\n"
        "Full research payload follows. The analyst's verdict is in the "
        "'verdict' field. Their thesis/risks/scenarios are in 'investment_thesis', "
        "'risk_factors', 'scenarios'.\n\n"
        "```json\n" + json.dumps(research_compact, indent=2, default=str)[:55000] + "\n```"
        + flow_context_str
    )

    # Call the chosen model
    try:
        model_call = critique_via_chosen_model(CRITIQUE_SYSTEM, user_prompt)
    except Exception as e:
        print(f"[critique] model call failed: {e}")
        return {
            "statusCode": 502,
            "body": json.dumps({
                "error": f"Critic model failed: {str(e)[:300]}",
                "ticker": ticker,
            }),
        }

    # Parse the critique JSON
    try:
        critique_data = parse_critique_json(model_call["text"])
    except Exception as e:
        # If parsing fails, return raw text alongside error
        return {
            "statusCode": 502,
            "body": json.dumps({
                "error": f"Failed to parse critique JSON: {e}",
                "ticker": ticker,
                "raw_text": model_call["text"][:2000],
                "model": model_call.get("model_used"),
            }),
        }

    # Compose final output
    out = {
        "ticker": ticker,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "analyst_verdict": {
            "rating": (research.get("verdict") or {}).get("rating"),
            "conviction_grade": (research.get("verdict") or {}).get("conviction_grade"),
            "price_target_12m": (research.get("verdict") or {}).get("price_target_12m"),
            "upside_pct": (research.get("verdict") or {}).get("upside_pct"),
        },
        "critique": critique_data,
        "critic": {
            "model": model_call.get("model_used"),
            "provider": model_call.get("provider"),
            "cost_usd": model_call.get("cost_usd"),
            "elapsed_s": model_call.get("elapsed_s"),
            "usage": model_call.get("usage"),
        },
        "elapsed_s_total": round(time.time() - t0, 1),
    }

    # Persist to S3
    s3_key = write_critique(ticker, out)
    out["s3_key"] = s3_key
    print(f"[critique] {ticker} DONE — disagreement={critique_data.get('disagreement_score')} cost=${model_call.get('cost_usd')} elapsed={model_call.get('elapsed_s')}s")

    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Cache-Control": "public, max-age=600",
        },
        "body": json.dumps(out, default=str),
    }
