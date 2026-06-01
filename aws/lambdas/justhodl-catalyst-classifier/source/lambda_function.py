"""
justhodl-catalyst-classifier
═══════════════════════════════
Every pump has a catalyst. This Lambda identifies and grades the catalyst
driving each name in our universe.

THE INSIGHT
═══════════
Pure momentum without a catalyst is chasing. Names that pump AND keep
pumping always have a nameable reason: earnings beat with raised guidance,
FDA approval, sector rotation, macro tailwind (rates/dollar/oil), product
launch, M&A, index inclusion, squeeze trigger, geopolitical shock.

Currently the system MENTIONS catalysts inside Claude's per-ticker
dossiers but doesn't have a structured catalyst layer. We know "earnings
in 5 days" from the calendar, and we know "tone is rising" from earnings
NLP, but we don't know WHAT IS DRIVING THE MOVE as a first-class field.

This Lambda fills that gap. For every candidate in the aggressive
universe, Claude reads the available context (research dossier, earnings
NLP, mechanics, momentum data) and outputs a structured catalyst record.

CATALYST RECORD SCHEMA
══════════════════════
{
  "ticker": "WDC",
  "primary_catalyst": "HAMR commercialization ramp + AI data-center HDD pricing power",
  "catalyst_type": "PRODUCT_LAUNCH",       # see CATALYST_TYPES below
  "catalyst_grade": "A",                   # A: durable + measurable, D: rumor / one-off
  "catalyst_date":  "2026-07-15",          # next dated event driving move, ISO format or null
  "days_to_catalyst": 44,                  # convenience field, null if undated
  "thesis_durability": "MULTI_QUARTER",    # 1D, 1W, 1M, MULTI_QUARTER, STRUCTURAL
  "secondary_catalysts": [...],            # supporting reasons
  "invalidation":       "...",             # what would kill the trade
  "claude_reasoning":   "..."              # 2-3 sentence explanation
}

CATALYST TYPES
══════════════
EARNINGS_BEAT      — recent or upcoming earnings, beat history
GUIDANCE_RAISE     — management raised forward guidance
PRODUCT_LAUNCH     — new product/feature/service driving revenue
FDA_APPROVAL       — drug/device approval or clinical data
M&A                — acquisition, spin-off, take-private rumor
MACRO_TAILWIND     — sector rotation, rates/dollar/oil/copper move
INDEX_INCLUSION    — S&P 500/Nasdaq 100 addition imminent
SQUEEZE_SETUP      — high short interest + catalyst combination
INSIDER_BUYING     — meaningful insider purchases recent
GEOPOLITICAL       — sanctions, trade, war, election outcome
SYMPATHY_MOVE      — peer just pumped, this is the second-wave
NO_CLEAR_CATALYST  — momentum without a nameable reason (FLAG)

CATALYST GRADES
═══════════════
A — Durable, measurable, named event with a known date or recent trigger.
    e.g. 'NVDA earnings 5/28 beat by 15% + raised guidance +20%'
B — Clear but lower-precision. e.g. 'Copper supercycle thesis intact'
C — Soft/rumored. e.g. 'M&A speculation circulating'
D — Naked momentum or rumor only. Flag these.

INPUT LAYERS (read in this order)
═════════════════════════════════
data/pump-positioning.json     (aggressive_basket positions + research)
data/momentum-leaders.json     (full top-30 list)
data/ticker-research-bundle.json (Claude dossiers for context)
data/pump-earnings-nlp.json    (transcript NLP)
data/earnings-tracker.json     (upcoming earnings dates)
data/themes.json               (active themes)

PROCESS
═══════
1. Build candidate list (aggressive basket + top momentum-leaders, deduped)
2. Per ticker, compile a structured CONTEXT BUNDLE (compact JSON, ~3K chars)
3. ONE BULK Claude call: send all bundles, get back array of catalyst records
4. Validate + structure the output
5. Write to data/catalysts.json

ONE BULK CALL because:
- Per-ticker calls would multiply latency by N (each ~30s Claude call)
- Bulk lets Claude see the universe context — it can cross-reference and
  identify SYMPATHY_MOVES that solo calls would miss
- Costs less in tokens
- Max ~15 tickers per call to stay within token limits

OUTPUT
══════
data/catalysts.json
{
  "schema_version": "1.0",
  "generated_at":   "...",
  "model":          "claude-haiku-4-5-20251001",
  "n_classified":   12,
  "catalysts":      [...],
  "by_grade":       {"A": [...], "B": [...], "C": [...], "D": [...]},
  "by_type":        {"PRODUCT_LAUNCH": [...], ...},
  "flagged":        [...],   # NO_CLEAR_CATALYST or grade D
  "ticker_to_catalyst": {ticker: catalyst_record}
}

SCHEDULE
════════
cron(0 14 * * ? *) — daily 14:00 UTC (9 AM ET, after morning brief)
Catalysts change slowly — once a day is plenty.
"""
import json
import os
import re
import sys
import time
import urllib.request
from datetime import datetime, timezone
from typing import Dict, List, Optional

import boto3

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

S3_BUCKET     = "justhodl-dashboard-live"
OUTPUT_KEY    = "data/catalysts.json"
MODEL         = "claude-haiku-4-5-20251001"
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

INPUT_KEYS = {
    "positioning":  "data/pump-positioning.json",
    "momentum":     "data/momentum-leaders.json",
    "research":     "data/ticker-research-bundle.json",
    "nlp":          "data/pump-earnings-nlp.json",
    "earnings_cal": "data/earnings-tracker.json",
    "themes":       "data/themes.json",
    "mechanics":    "data/pump-mechanics.json",
}

MAX_CANDIDATES = 15  # don't classify more than 15 per run (token cost)

VALID_TYPES = {
    "EARNINGS_BEAT", "GUIDANCE_RAISE", "PRODUCT_LAUNCH", "FDA_APPROVAL",
    "M&A", "MACRO_TAILWIND", "INDEX_INCLUSION", "SQUEEZE_SETUP",
    "INSIDER_BUYING", "GEOPOLITICAL", "SYMPATHY_MOVE", "NO_CLEAR_CATALYST",
}
VALID_GRADES = {"A", "B", "C", "D"}
VALID_DURABILITY = {"1D", "1W", "1M", "MULTI_QUARTER", "STRUCTURAL"}

s3 = boto3.client("s3", region_name="us-east-1")


# ═════════════════════════════════════════════════════════════════════
# S3 helpers
# ═════════════════════════════════════════════════════════════════════

def load_s3_json(key: str) -> Optional[dict]:
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception as e:
        print(f"[load] {key}: {str(e)[:120]}")
        return None


# ═════════════════════════════════════════════════════════════════════
# Build candidate universe + context bundles
# ═════════════════════════════════════════════════════════════════════

def build_candidate_universe(raw: dict) -> List[str]:
    """Aggressive basket positions + top momentum leaders, deduped."""
    universe = []
    seen = set()

    # 1. Aggressive basket positions (priority)
    pos_doc = raw.get("positioning") or {}
    agg = pos_doc.get("aggressive_basket") or {}
    for p in (agg.get("positions") or []):
        t = p.get("ticker")
        if t and t not in seen:
            universe.append(t); seen.add(t)

    # 2. Top momentum leaders (top 10)
    mom_doc = raw.get("momentum") or {}
    for l in (mom_doc.get("leaders") or [])[:10]:
        t = l.get("ticker")
        if t and t not in seen:
            universe.append(t); seen.add(t)
        if len(universe) >= MAX_CANDIDATES: break

    return universe[:MAX_CANDIDATES]


def compact_research_for_ticker(raw: dict, ticker: str) -> dict:
    """Pull the Claude dossier for this ticker if available."""
    research_doc = raw.get("research") or {}
    inner = research_doc.get("research")
    item = None
    # Handle dict-keyed or list shapes
    if isinstance(inner, dict):
        item = inner.get(ticker)
    elif isinstance(inner, list):
        item = next((r for r in inner if isinstance(r, dict) and r.get("ticker") == ticker), None)
    if not isinstance(item, dict):
        return {}
    return {
        "bull_headline":      ((item.get("bull_thesis") or {}).get("headline") or "")[:200],
        "bull_drivers":       [(d if isinstance(d, str) else d.get("driver", ""))
                                for d in (item.get("bull_thesis") or {}).get("drivers", [])[:3]],
        "risk_headline":      ((item.get("risk_assessment") or {}).get("headline") or "")[:200],
        "ai_one_liner":       (item.get("ai_one_liner") or "")[:200],
        "conviction":         (item.get("trade_framework") or {}).get("conviction"),
    }


def compact_nlp_for_ticker(raw: dict, ticker: str) -> dict:
    nlp_doc = raw.get("nlp") or {}
    research = nlp_doc.get("research") or {}
    item = research.get(ticker)
    if not isinstance(item, dict):
        return {}
    return {
        "tone_trajectory":  item.get("tone_trajectory"),
        "guidance":         item.get("forward_guidance_posture"),
        "emerging_themes":  (item.get("emerging_themes") or [])[:2],
        "pump_implication": (item.get("pump_implication") or "")[:200],
    }


def compact_position_for_ticker(raw: dict, ticker: str) -> dict:
    pos_doc = raw.get("positioning") or {}
    agg = pos_doc.get("aggressive_basket") or {}
    item = next((p for p in (agg.get("positions") or []) if p.get("ticker") == ticker), None)
    if not item:
        return {}
    return {
        "position_pct":   item.get("position_pct"),
        "pump_score":     item.get("pump_score"),
        "momentum_score": item.get("momentum_score"),
        "combined_score": item.get("combined_score"),
        "pump_confirmed": item.get("pump_confirmed"),
        "sector":         item.get("sector"),
        "mom_tags":       item.get("mom_tags", [])[:4],
    }


def compact_momentum_for_ticker(raw: dict, ticker: str) -> dict:
    mom_doc = raw.get("momentum") or {}
    item = next((l for l in (mom_doc.get("leaders") or []) if l.get("ticker") == ticker), None)
    if not item:
        return {}
    return {
        "momentum_score":  item.get("momentum_score"),
        "perf_20d":        item.get("perf_20d_pct"),
        "perf_60d":        item.get("perf_60d_pct"),
        "rs_spy_20d":      item.get("rs_spy_20d_pct"),
        "tags":            item.get("tags", [])[:5],
        "pump_confirmed":  item.get("pump_confirmed"),
        "n_engines":       item.get("n_engines"),
    }


def compact_earnings_for_ticker(raw: dict, ticker: str) -> dict:
    cal = raw.get("earnings_cal") or {}
    upcoming = cal.get("upcoming_14d") or []
    item = next((u for u in upcoming if u.get("ticker") == ticker), None)
    if not item:
        return {}
    return {
        "next_earnings_date": item.get("earnings_date"),
        "earnings_time":      item.get("time"),
        "eps_consensus":      item.get("eps_consensus"),
    }


def compact_mechanics_for_ticker(raw: dict, ticker: str) -> dict:
    mech_doc = raw.get("mechanics") or {}
    candidates = mech_doc.get("candidates") or []
    item = next((c for c in candidates if c.get("ticker") == ticker), None)
    if not item:
        return {}
    sq = item.get("squeeze_profile") or {}
    op = item.get("options_structure") or {}
    return {
        "squeeze_potential":  sq.get("squeeze_potential"),
        "options_skew":       op.get("skew"),
        "iv_rank_proxy":      op.get("iv_rank_proxy"),
    }


def get_theme_for_ticker(raw: dict, ticker: str) -> Optional[dict]:
    themes_doc = raw.get("themes") or {}
    industry = (themes_doc.get("ticker_to_theme") or {}).get(ticker)
    if not industry:
        return None
    theme_meta = (themes_doc.get("themes") or {}).get(industry)
    if not theme_meta:
        return None
    return {
        "industry":      industry,
        "label":         theme_meta.get("label"),
        "n_leaders":     theme_meta.get("n_leaders"),
        "avg_momentum":  theme_meta.get("avg_momentum"),
    }


def build_context_bundle(ticker: str, raw: dict) -> dict:
    """Compile all available context for one ticker into a compact bundle."""
    return {
        "ticker":     ticker,
        "research":   compact_research_for_ticker(raw, ticker),
        "nlp":        compact_nlp_for_ticker(raw, ticker),
        "position":   compact_position_for_ticker(raw, ticker),
        "momentum":   compact_momentum_for_ticker(raw, ticker),
        "earnings":   compact_earnings_for_ticker(raw, ticker),
        "mechanics":  compact_mechanics_for_ticker(raw, ticker),
        "theme":      get_theme_for_ticker(raw, ticker),
    }


# ═════════════════════════════════════════════════════════════════════
# Claude prompt + call
# ═════════════════════════════════════════════════════════════════════

SYSTEM_PROMPT = """You are a hedge-fund analyst whose job is to identify and \
grade the CATALYST driving each name. Every pump has a catalyst. Names without \
a nameable catalyst are speculation, not investment.

For each ticker in the input list, output a structured CATALYST RECORD.

CATALYST TYPES (pick ONE primary)
═════════════════════════════════
EARNINGS_BEAT      — recent or upcoming earnings, especially with beat history
GUIDANCE_RAISE     — management raised forward guidance (most powerful catalyst)
PRODUCT_LAUNCH     — new product/feature/service driving revenue
FDA_APPROVAL       — drug/device approval or clinical data milestone
M&A                — acquisition, spin-off, take-private rumor
MACRO_TAILWIND     — sector rotation, rates/dollar/oil/copper move
INDEX_INCLUSION    — S&P 500/Nasdaq 100 addition imminent
SQUEEZE_SETUP      — high short interest + catalyst combination
INSIDER_BUYING     — meaningful insider purchases recent
GEOPOLITICAL       — sanctions, trade, war, election outcome
SYMPATHY_MOVE      — peer just pumped, this is the second-wave
NO_CLEAR_CATALYST  — momentum without a nameable reason (FLAG IT)

CATALYST GRADES
═══════════════
A — Durable, measurable, named event with a known date or recent trigger.
    e.g. "NVDA earnings 2026-05-28 beat by 15% + raised FY guidance +20%"
    e.g. "PLTR FY2026 guidance raised to $7.66B (+71% growth)"
B — Clear but lower-precision. e.g. "Copper supercycle thesis intact + Indonesia
    regulatory certainty through 2041"
C — Soft/rumored. e.g. "M&A speculation circulating; not confirmed"
D — Naked momentum or rumor only. FLAG these as NO_CLEAR_CATALYST grade D.

REQUIRED FIELDS PER TICKER
══════════════════════════
{
  "ticker": "WDC",
  "primary_catalyst": "<25-word punchy description>",
  "catalyst_type":    "<one of CATALYST TYPES>",
  "catalyst_grade":   "A|B|C|D",
  "catalyst_date":    "YYYY-MM-DD or null",
  "thesis_durability": "1D|1W|1M|MULTI_QUARTER|STRUCTURAL",
  "secondary_catalysts": ["<list of 0-3 supporting reasons>"],
  "invalidation":     "<what would kill the trade — 15 words>",
  "claude_reasoning": "<2-3 sentence explanation citing specific evidence
                        from the context bundle>"
}

STYLE RULES
═══════════
- Be SPECIFIC. "Strong fundamentals" is a D-grade catalyst. "Q1 FCF record
  + buyback authorization" is A-grade.
- Reference NUMBERS from the context bundle when possible.
- catalyst_date: only use a real ISO date if you can name it. If the
  catalyst has no specific date (e.g. ongoing macro), set to null.
- thesis_durability: MULTI_QUARTER and STRUCTURAL are reserved for theses
  spanning 3+ months. 1W means "this thesis is news-cycle dependent".
- If you see no clear catalyst in the context, USE NO_CLEAR_CATALYST grade D.
  Don't fabricate. Honest naming beats invented catalysts.

OUTPUT FORMAT — pure JSON array, no markdown:
[
  { ...catalyst record 1... },
  { ...catalyst record 2... },
  ...
]
"""


def build_user_prompt(bundles: List[dict]) -> str:
    parts = [
        f"# Catalyst classification for {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "",
        f"Classify the catalyst for each of these {len(bundles)} tickers. "
        f"Use ONLY the context bundle provided — don't invent. Return a JSON "
        f"array with EXACTLY {len(bundles)} elements in the same order.",
        "",
    ]
    for i, b in enumerate(bundles, 1):
        parts.append(f"## Ticker #{i}: {b['ticker']}")
        parts.append("```json")
        parts.append(json.dumps(b, indent=2, default=str)[:3500])
        parts.append("```")
        parts.append("")
    parts.append("Produce the JSON array per the system prompt.")
    return "\n".join(parts)


def call_anthropic(system: str, user: str, max_tokens: int = 8000) -> str:
    if not ANTHROPIC_KEY:
        raise RuntimeError("ANTHROPIC_API_KEY not set")
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
    with urllib.request.urlopen(req, timeout=180) as r:
        data = json.loads(r.read().decode("utf-8"))
    if not data.get("content"):
        raise RuntimeError(f"Empty response")
    text = ""
    for block in data["content"]:
        if block.get("type") == "text":
            text += block.get("text", "")
    return text.strip()


def extract_json_array(text: str) -> list:
    """Robustly extract a JSON array from text response (handles ```json fences)."""
    text = re.sub(r"^```(?:json)?\s*", "", text.strip(), flags=re.MULTILINE)
    text = re.sub(r"\s*```\s*$", "", text.strip(), flags=re.MULTILINE)
    # Find first [ and matching ]
    depth = 0
    start = -1
    for i, ch in enumerate(text):
        if ch == "[":
            if depth == 0: start = i
            depth += 1
        elif ch == "]":
            depth -= 1
            if depth == 0 and start >= 0:
                try: return json.loads(text[start:i+1])
                except json.JSONDecodeError: continue
    return json.loads(text)


def validate_record(record: dict, expected_ticker: str) -> dict:
    """Sanity-check + normalize a single catalyst record."""
    out = {"ticker": record.get("ticker") or expected_ticker}
    out["primary_catalyst"] = (record.get("primary_catalyst") or "—")[:300]
    out["catalyst_type"]    = (record.get("catalyst_type") or "NO_CLEAR_CATALYST").upper()
    if out["catalyst_type"] not in VALID_TYPES:
        out["catalyst_type"] = "NO_CLEAR_CATALYST"
    out["catalyst_grade"]   = (record.get("catalyst_grade") or "D").upper()
    if out["catalyst_grade"] not in VALID_GRADES:
        out["catalyst_grade"] = "D"
    out["catalyst_date"]    = record.get("catalyst_date")
    # Parse days_to_catalyst if date present
    if out["catalyst_date"]:
        try:
            ed = datetime.strptime(out["catalyst_date"][:10], "%Y-%m-%d").date()
            today = datetime.now(timezone.utc).date()
            out["days_to_catalyst"] = (ed - today).days
        except Exception:
            out["catalyst_date"] = None
            out["days_to_catalyst"] = None
    else:
        out["days_to_catalyst"] = None
    out["thesis_durability"]  = (record.get("thesis_durability") or "1M").upper()
    if out["thesis_durability"] not in VALID_DURABILITY:
        out["thesis_durability"] = "1M"
    out["secondary_catalysts"] = (record.get("secondary_catalysts") or [])[:5]
    out["invalidation"]        = (record.get("invalidation") or "—")[:300]
    out["claude_reasoning"]    = (record.get("claude_reasoning") or "")[:500]
    return out


# ═════════════════════════════════════════════════════════════════════
# Lambda handler
# ═════════════════════════════════════════════════════════════════════

def lambda_handler(event, context):
    t0 = time.time()
    print(f"[catalysts] start {datetime.now(timezone.utc).isoformat()}")

    raw = {name: load_s3_json(key) for name, key in INPUT_KEYS.items()}
    universe = build_candidate_universe(raw)
    if not universe:
        return _write_error("No candidates to classify")
    print(f"[catalysts] {len(universe)} candidates: {universe}")

    bundles = [build_context_bundle(t, raw) for t in universe]

    user_prompt = build_user_prompt(bundles)
    print(f"[catalysts] prompt: {len(user_prompt)} chars; calling Claude")
    t_claude = time.time()
    try:
        response_text = call_anthropic(SYSTEM_PROMPT, user_prompt, max_tokens=8000)
        claude_elapsed = round(time.time() - t_claude, 2)
        print(f"[catalysts] Claude: {len(response_text)} chars in {claude_elapsed}s")
    except Exception as e:
        return _write_error(f"Claude error: {str(e)[:200]}")

    try:
        records_raw = extract_json_array(response_text)
    except Exception as e:
        return _write_error(f"JSON parse error: {str(e)[:200]}",
                              raw_preview=response_text[:600])
    if not isinstance(records_raw, list):
        return _write_error(f"Expected JSON array, got {type(records_raw).__name__}",
                              raw_preview=response_text[:600])

    # Validate + align to universe order
    catalysts: List[dict] = []
    ticker_to_catalyst: Dict[str, dict] = {}
    by_grade: Dict[str, List[str]] = {"A": [], "B": [], "C": [], "D": []}
    by_type:  Dict[str, List[str]] = {}
    flagged: List[str] = []

    for i, ticker in enumerate(universe):
        record_raw = records_raw[i] if i < len(records_raw) else {}
        if not isinstance(record_raw, dict):
            record_raw = {}
        rec = validate_record(record_raw, ticker)
        catalysts.append(rec)
        ticker_to_catalyst[ticker] = rec
        by_grade.setdefault(rec["catalyst_grade"], []).append(ticker)
        by_type.setdefault(rec["catalyst_type"], []).append(ticker)
        if rec["catalyst_grade"] == "D" or rec["catalyst_type"] == "NO_CLEAR_CATALYST":
            flagged.append(ticker)

    output = {
        "schema_version":    "1.0",
        "generated_at":      datetime.now(timezone.utc).isoformat(),
        "model":             MODEL,
        "elapsed_sec":       round(time.time() - t0, 2),
        "claude_elapsed":    claude_elapsed,
        "n_classified":      len(catalysts),
        "catalysts":         catalysts,
        "ticker_to_catalyst": ticker_to_catalyst,
        "by_grade":          by_grade,
        "by_type":           by_type,
        "flagged":           flagged,
        "schema": {
            "catalyst_types":   sorted(VALID_TYPES),
            "catalyst_grades":  sorted(VALID_GRADES),
            "durability":       sorted(VALID_DURABILITY),
        },
        "disclaimer": ("Catalyst classification by Claude AI based on context bundles "
                        "synthesized from quant engines. Catalyst grades reflect Claude's "
                        "interpretation; verify before sizing. Grade D / NO_CLEAR_CATALYST "
                        "items should be downgraded or excluded from aggressive sizing."),
    }

    body = json.dumps(output, indent=2, default=str)
    s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY, Body=body,
                    ContentType="application/json", CacheControl="max-age=900")
    try:
        archive_key = (f"data/archive/catalysts/"
                        f"{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M')}.json")
        s3.put_object(Bucket=S3_BUCKET, Key=archive_key, Body=body,
                        ContentType="application/json")
    except Exception: pass

    summary = {
        "status":         "ok",
        "elapsed_sec":    output["elapsed_sec"],
        "claude_elapsed": claude_elapsed,
        "n_classified":   output["n_classified"],
        "n_grade_A":      len(by_grade["A"]),
        "n_grade_B":      len(by_grade["B"]),
        "n_grade_C":      len(by_grade["C"]),
        "n_grade_D":      len(by_grade["D"]),
        "n_flagged":      len(flagged),
        "by_type_counts": {k: len(v) for k, v in by_type.items()},
    }
    print(f"[catalysts] done: {summary}")
    return {"statusCode": 200, "body": json.dumps(summary)}


def _write_error(message: str, **extras) -> dict:
    payload = {"schema_version": "1.0", "generated_at": datetime.now(timezone.utc).isoformat(),
                "status": "error", "error": message, **extras}
    try:
        s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY,
                        Body=json.dumps(payload, default=str, indent=2),
                        ContentType="application/json", CacheControl="max-age=300")
    except Exception: pass
    print(f"[catalysts] ERROR: {message}")
    return {"statusCode": 500, "body": json.dumps({"status": "error", "error": message})}
