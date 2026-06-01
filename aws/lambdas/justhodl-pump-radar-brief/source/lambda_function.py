"""
justhodl-pump-radar-brief
═════════════════════════
The synthesis layer. Hedge fund analysts spend hours integrating:
  - convergence radar (which engines see this)
  - positioning framework (ATR stops, sized basket)
  - mechanics (squeeze proxy, IV rank)
  - portfolio analytics (correlations, factor exposure)
  - pair trades (relative value setups)
  - earnings NLP (management tone shift)
  - deep research (Claude AI dossiers)
  - macro regime
... into a single coherent INSTITUTIONAL BRIEF that a PM consumes at 7 AM.

This Lambda automates that synthesis. One bulk Claude call produces:

  morning_brief:
    macro_frame:         2-3 sentences on current regime + what favors longs
    market_temperature:  composite 0-100 score (risk-on appetite)
    top_3_long_ideas:    per-ticker bull thesis + size + stop + TPs + key risk
    top_2_pair_trades:   long/short setups with hedge quality + expected α
    what_to_watch_today: earnings calendar + macro catalysts within 5 days
    risk_warnings:       portfolio-level concerns (concentration, regime risk)
    whats_changed:       diff vs yesterday's brief (new signals, removed signals)
    conviction_grade:    A/B/C/D overall — how confident we are in today's setup

Claude reads ALL the layers as structured inputs (not transcripts), so the
brief reflects actual numbers, not vague platitudes.

INPUTS
══════
data/convergence-radar.json
data/pump-positioning.json
data/pump-mechanics.json
data/portfolio-analytics.json
data/pair-trades.json
data/pump-earnings-nlp.json
data/ticker-research-bundle.json
data/ai-website-synthesis.json     (macro regime)
data/pump-radar-brief.json         (prior brief — for whats_changed)

OUTPUT
══════
data/pump-radar-brief.json
{
  "schema_version":  "1.0",
  "generated_at":    "...",
  "model":           "claude-haiku-4-5-20251001",
  "macro_frame":     "...",
  "market_temperature": 56,
  "top_3_long_ideas": [...],
  "top_2_pair_trades": [...],
  "what_to_watch_today": [...],
  "risk_warnings":   [...],
  "whats_changed":   {...},
  "conviction_grade": "B+",
  "executive_summary": "...",
  "source_versions": {       # which data versions were synthesized
    "convergence":  "2026-...",
    "positioning":  "2026-...",
    "mechanics":    "2026-...",
    ...
  }
}

SCHEDULE
════════
cron(30 13 * * ? *) — daily 13:30 UTC (8:30 AM ET — before US market open,
                                          after all overnight layers run)
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
OUTPUT_KEY    = "data/pump-radar-brief.json"
MODEL         = "claude-haiku-4-5-20251001"
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

INPUT_KEYS = {
    "convergence":   "data/convergence-radar.json",
    "positioning":   "data/pump-positioning.json",
    "mechanics":     "data/pump-mechanics.json",
    "analytics":     "data/portfolio-analytics.json",
    "pairs":         "data/pair-trades.json",
    "nlp":           "data/pump-earnings-nlp.json",
    "research":      "data/ticker-research-bundle.json",
    "synthesis":     "data/ai-website-synthesis.json",
    "earnings_cal":  "data/earnings-tracker.json",
}
PRIOR_KEY = "data/pump-radar-brief.json"

s3 = boto3.client("s3", region_name="us-east-1")


# ═════════════════════════════════════════════════════════════════════
# Data loaders — small footprint, only fields we need for synthesis
# ═════════════════════════════════════════════════════════════════════

def load_s3_json(key: str) -> Optional[dict]:
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception as e:
        print(f"[load] {key}: {str(e)[:120]}")
        return None


def compact_radar(d: dict) -> dict:
    """Pull only the pump-relevant slice."""
    if not d: return {}
    candidates = d.get("pump_candidates") or []
    return {
        "generated_at": d.get("generated_at"),
        "n_pump_candidates": len(candidates),
        "top_candidates": [
            {
                "ticker":          c["ticker"],
                "tier":            c.get("tier"),
                "n_engines":       c.get("n_engines"),
                "convergence":     c.get("convergence_score"),
                "directional":     c.get("directional_score"),
                "pump_likelihood": c.get("pump_likelihood"),
                "pump_category":   c.get("pump_category"),
                "bullish_engines": [e.get("engine") for e in (c.get("bullish_engines") or [])[:5]],
                "bearish_engines": [e.get("engine") for e in (c.get("bearish_engines") or [])[:3]],
                "is_new_high":     c.get("is_new_high"),
                "is_accelerating": c.get("is_accelerating"),
            }
            for c in candidates[:8]
        ],
    }


def compact_positioning(d: dict) -> dict:
    if not d: return {}
    basket = d.get("portfolio_basket") or {}
    return {
        "generated_at": d.get("generated_at"),
        "macro_regime": d.get("macro_regime"),
        "basket": {
            "n_positions":    basket.get("n_positions"),
            "total_exposure": basket.get("total_exposure"),
            "cash_pct":       basket.get("cash_pct"),
            "sector_breakdown": basket.get("sector_breakdown"),
            "positions": [
                {k: v for k, v in p.items()
                 if k in ("ticker","position_pct","pump_score","sector","entry","stop","tp1","tp2","tp3","rr_ratio")}
                for p in (basket.get("positions") or [])[:8]
            ],
        },
        "candidates_framework": [
            {
                "ticker":          c.get("ticker"),
                "pump_likelihood": c.get("pump_likelihood"),
                "atr_pct":         (c.get("price_data") or {}).get("atr_pct"),
                "hv_30":           (c.get("price_data") or {}).get("hv_30"),
                "perf_20d":        (c.get("price_data") or {}).get("perf_20d_pct"),
                "days_to_earnings": (c.get("context") or {}).get("days_to_earnings"),
                "liquidity_tier":  (c.get("context") or {}).get("liquidity_tier"),
                "stop_loss":       (c.get("trade_framework") or {}).get("stop_loss"),
                "stop_pct":        (c.get("trade_framework") or {}).get("stop_loss_pct"),
                "position_size":   (c.get("trade_framework") or {}).get("position_size_pct"),
                "warnings":        c.get("warnings", [])[:3],
            }
            for c in (d.get("candidates") or [])[:8]
        ],
    }


def compact_mechanics(d: dict) -> dict:
    if not d: return {}
    return {
        "generated_at": d.get("generated_at"),
        "per_ticker": [
            {
                "ticker":          c.get("ticker"),
                "squeeze_score":   (c.get("squeeze_profile") or {}).get("squeeze_proxy_score"),
                "squeeze_potential": (c.get("squeeze_profile") or {}).get("squeeze_potential"),
                "float_tier":      (c.get("squeeze_profile") or {}).get("float_tier"),
                "rotation_accel":  (c.get("squeeze_profile") or {}).get("rotation_accel"),
                "options_skew":    (c.get("options_structure") or {}).get("skew"),
                "options_tier":    (c.get("options_structure") or {}).get("tier"),
                "iv_rank":         (c.get("options_structure") or {}).get("iv_rank_proxy"),
                "term_structure":  (c.get("options_structure") or {}).get("term_structure"),
            }
            for c in (d.get("candidates") or [])[:8]
        ],
    }


def compact_analytics(d: dict) -> dict:
    if not d: return {}
    corr = d.get("correlations") or {}
    fact = d.get("factor_exposure") or {}
    return {
        "generated_at": d.get("generated_at"),
        "clusters":     [c for c in (corr.get("clusters") or []) if c.get("size", 0) >= 2][:5],
        "most_diversifying_top_5": corr.get("most_diversifying_top_5", []),
        "most_correlated_top_5":   corr.get("most_correlated_top_5", []),
        "factor_exposure": {
            t: {k: v for k, v in (fact.get("per_ticker", {}).get(t, {}) or {}).items()
                if k in ("beta_spy","beta_qqq","beta_sector","sector_etf","r_sq_spy","alpha_spy_ann","idio_alpha_20d_pct")}
            for t in list((fact.get("per_ticker") or {}).keys())[:8]
        },
    }


def compact_pairs(d: dict) -> dict:
    if not d: return {}
    return {
        "generated_at": d.get("generated_at"),
        "pairs": [
            {k: v for k, v in p.items()
              if k in ("long_ticker","short_ticker","short_company","long_perf_20d","short_perf_20d",
                          "spread_20d_pct","correlation_90d","hedge_quality","ratio_long_short",
                          "expected_alpha_1m","long_pump_likelihood","thesis_one_liner")}
            for p in (d.get("pairs") or [])[:6]
        ],
    }


def compact_nlp(d: dict) -> dict:
    if not d: return {}
    research = d.get("research") or {}
    return {
        "generated_at": d.get("generated_at"),
        "per_ticker": [
            {
                "ticker":            t,
                "tone_trajectory":   r.get("tone_trajectory"),
                "tone_delta":        r.get("tone_delta"),
                "guidance":          r.get("forward_guidance_posture"),
                "growth_lang":       r.get("growth_language_freq"),
                "emerging_themes":   (r.get("emerging_themes") or [])[:3],
                "cautionary":        (r.get("cautionary_signals") or [])[:3],
                "pump_implication":  (r.get("pump_implication") or "")[:280],
                "ai_synthesis":      (r.get("ai_synthesis") or "")[:240],
            }
            for t, r in list(research.items())[:8]
        ],
    }


def compact_synthesis(d: dict) -> dict:
    if not d: return {}
    s = d.get("synthesis") or {}
    return {
        "generated_at":   d.get("generated_at"),
        "global_posture": s.get("global_posture"),
        "regime_summary": (s.get("regime_summary") or "")[:600],
        "macro_drivers":  (s.get("macro_drivers") or [])[:5],
    }


def compact_earnings_cal(d: dict) -> dict:
    if not d: return {}
    return {
        "upcoming_5d": [
            {k: v for k, v in u.items() if k in ("ticker","earnings_date","time","eps_consensus")}
            for u in (d.get("upcoming_14d") or [])[:25]
        ],
    }


def _compact_research(research_doc) -> list:
    """Ticker-research-bundle.json may store 'research' as either a list of
    dossiers OR a dict keyed by ticker. Normalize to a list of compacted
    summaries."""
    if not isinstance(research_doc, dict):
        return []
    inner = research_doc.get("research")
    # Case 1: array of dossiers
    if isinstance(inner, list):
        items = inner[:8]
    # Case 2: dict keyed by ticker
    elif isinstance(inner, dict):
        items = []
        for t, r in list(inner.items())[:8]:
            if isinstance(r, dict):
                if "ticker" not in r:
                    r = {**r, "ticker": t}
                items.append(r)
    else:
        return []
    out = []
    for r in items:
        if not isinstance(r, dict):
            continue
        tf  = r.get("trade_framework") or {}
        bt  = r.get("bull_thesis") or {}
        rsk = r.get("risk_assessment") or {}
        out.append({
            "ticker":        r.get("ticker"),
            "conviction":    tf.get("conviction") if isinstance(tf, dict) else None,
            "time_horizon":  tf.get("time_horizon") if isinstance(tf, dict) else None,
            "bull_headline": bt.get("headline") if isinstance(bt, dict) else None,
            "risk_headline": rsk.get("headline") if isinstance(rsk, dict) else None,
            "ai_one_liner":  r.get("ai_one_liner"),
        })
    return out


# ═════════════════════════════════════════════════════════════════════
# Compute market temperature (lightweight composite)
# ═════════════════════════════════════════════════════════════════════

def compute_market_temperature(radar_compact: dict, positioning_compact: dict,
                                  mechanics_compact: dict) -> dict:
    """0-100 score: how 'hot' is the current pump setup?

    Components:
      40 × max pump_likelihood from top candidate
      25 × avg pump_likelihood across top 5
      20 × n bullish-skew options across top 5
      15 × portfolio basket exposure (more = more conviction)
    """
    candidates = radar_compact.get("top_candidates", [])
    if not candidates:
        return {"score": 0, "rank": "COOL", "components": {}}

    top = candidates[0]
    max_pump = top.get("pump_likelihood", 0) or 0
    avg_top_5 = sum(c.get("pump_likelihood", 0) or 0 for c in candidates[:5]) / max(1, min(5, len(candidates)))

    mech_per = (mechanics_compact.get("per_ticker") or [])
    n_bullish_opts = sum(1 for m in mech_per[:5] if m.get("options_skew") == "bullish")
    n_bullish_pct = (n_bullish_opts / 5) * 100 if mech_per else 0

    exposure = (positioning_compact.get("basket") or {}).get("total_exposure", 0) or 0

    score = (
        0.40 * (max_pump) +
        0.25 * avg_top_5 +
        0.20 * n_bullish_pct +
        0.15 * exposure
    )
    score = max(0, min(100, score))

    if score >= 75:   rank = "HOT"
    elif score >= 60: rank = "WARM"
    elif score >= 45: rank = "TEPID"
    else:             rank = "COOL"

    return {
        "score": round(score, 1),
        "rank":  rank,
        "components": {
            "max_pump":         round(max_pump, 1),
            "avg_top5_pump":    round(avg_top_5, 1),
            "n_bullish_opts_5": n_bullish_opts,
            "basket_exposure":  round(exposure, 1),
        },
    }


# ═════════════════════════════════════════════════════════════════════
# Compute whats_changed vs yesterday's brief
# ═════════════════════════════════════════════════════════════════════

def compute_whats_changed(prior: Optional[dict], current_top: List[dict]) -> dict:
    """Compare current top candidates to yesterday's brief."""
    if not prior:
        return {"no_prior": True}
    prior_top = (prior.get("top_3_long_ideas") or [])
    prior_tickers = set(p.get("ticker") for p in prior_top if p.get("ticker"))
    current_tickers = set(c["ticker"] for c in current_top)

    new = current_tickers - prior_tickers
    removed = prior_tickers - current_tickers
    retained = current_tickers & prior_tickers

    return {
        "new_signals":       sorted(new),
        "removed_signals":   sorted(removed),
        "retained_signals":  sorted(retained),
        "n_new":             len(new),
        "n_removed":         len(removed),
        "n_retained":        len(retained),
        "prior_temperature": (prior.get("market_temperature") or {}).get("score"),
        "prior_conviction":  prior.get("conviction_grade"),
    }


# ═════════════════════════════════════════════════════════════════════
# Claude synthesis
# ═════════════════════════════════════════════════════════════════════

SYSTEM_PROMPT = """You are the chief investment officer's morning desk \
synthesizing 7 quantitative layers into a one-page institutional brief. \
You are NOT writing fluff. Every claim must reference a specific number \
from the data provided.

YOUR JOB
═════════
Read the structured data (no transcripts — these are summaries of 7 different
quant engines) and produce a single morning brief with these sections:

  macro_frame: 2-3 sentences. Current macro regime + what specifically favors
               or disfavors longs right now. Reference the regime summary.

  top_3_long_ideas: An array of EXACTLY 3 ticker recommendations, ranked by
                    conviction. For each, provide a JSON object with:
                      ticker:           (string)
                      conviction:       "HIGH" | "MEDIUM" | "MEDIUM-HIGH" | "LOW"
                      thesis_1liner:    25-word punchy thesis
                      bull_bullets:     array of EXACTLY 3 bullet strings, each
                                          referencing specific data (e.g. 'PLTR is
                                          in 8 engines with pump_likelihood 54.5;
                                          earnings tone rising +10pts')
                      key_risk:         one risk bullet
                      sized_position:   "X.X% position with stop at $Y (-Z%)
                                          targeting TP1 $A / TP2 $B"
                      catalyst_window:  "next earnings in X days" | "no near catalyst"

  top_2_pair_trades: An array of EXACTLY 2 pair trades from the pairs data.
                     For each:
                       long_ticker:     (string)
                       short_ticker:    (string)
                       why_hedged:      "correlation X.XX = excellent/good hedge"
                       expected_alpha:  (from pairs.expected_alpha_1m)
                       trade_thesis:    "long X for Y / short Z because W"

  what_to_watch_today: Array of 3-5 watch items. Each item is a short string:
                         "X reports earnings on YYYY-MM-DD AMC/BMO"
                         "FOMC at 2:00pm ET — likely to move yield-sensitive names"
                         "Pump basket has Z% exposure; tighten stops if SPY breaks below..."

  risk_warnings: Array of 1-4 risk items. Each is a short string. Focus on
                  PORTFOLIO-LEVEL concerns: concentration, regime risk, expired
                  edges, low-quality pairs. If basket is healthy, can be empty.

  whats_changed: 2-3 sentences on what shifted since yesterday's brief. Use the
                  whats_changed data provided to ground this.

  conviction_grade: One of "A+" "A" "A-" "B+" "B" "B-" "C+" "C" "C-" "D"
                     What's our overall confidence in the day's setup?
                     A: high conviction + multi-engine convergence + regime
                        supports longs + earnings tone bullish
                     C: mixed signals, defensive regime, no clear leader
                     D: avoid; warnings outweigh edges

  executive_summary: 40-60 word top-line. The one paragraph that goes on the
                       desk and at the top of the report. Lead with the verb,
                       e.g. 'Lean LONG with high conviction in technology
                       infrastructure: PLTR (sized 5%), APH (5%), MSFT (5%)...'

STYLE RULES
═══════════
- Every number you cite must be from the provided data. Don't invent.
- If data is missing for a candidate, say so ('no transcript data available').
- Be DIRECTIONAL. Hedge funds don't pay for 'it could go either way'.
- conviction_grade must reflect the actual setup. Don't grade-inflate.
- 'whats_changed' should be tactical: 'NEM and APH rotated in; LLY rotated out
  on tone-falling signal. Conviction lifted from B to B+ on PLTR's raised guidance.'

OUTPUT FORMAT — pure JSON, no markdown:
{
  "macro_frame": "...",
  "market_temperature_label": "HOT|WARM|TEPID|COOL",
  "top_3_long_ideas": [...],
  "top_2_pair_trades": [...],
  "what_to_watch_today": [...],
  "risk_warnings": [...],
  "whats_changed": "...",
  "conviction_grade": "...",
  "executive_summary": "..."
}
"""


def build_user_prompt(payload: dict) -> str:
    """Concatenate compacted layer data with section headers."""
    parts = [
        f"# Morning brief synthesis for {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "",
        "Below are 8 structured data layers. Synthesize into the brief.",
        "",
    ]
    for layer_name, layer_data in payload.items():
        parts.append(f"## {layer_name.upper()} LAYER")
        parts.append("```json")
        parts.append(json.dumps(layer_data, indent=2, default=str)[:10000])
        parts.append("```")
        parts.append("")
    parts.append("Produce the JSON brief per the system prompt.")
    return "\n".join(parts)


def call_anthropic(system: str, user: str, max_tokens: int = 12000) -> str:
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


# ═════════════════════════════════════════════════════════════════════
# Lambda handler
# ═════════════════════════════════════════════════════════════════════

def lambda_handler(event, context):
    t0 = time.time()
    print(f"[brief] start {datetime.now(timezone.utc).isoformat()}")

    # Load prior brief BEFORE writing new one (for whats_changed)
    prior_brief = load_s3_json(PRIOR_KEY)
    if prior_brief and "macro_frame" not in prior_brief:
        prior_brief = None  # malformed or first-run

    # Load all 9 input layers
    raw = {name: load_s3_json(key) for name, key in INPUT_KEYS.items()}
    print(f"[brief] loaded layers: {sum(1 for v in raw.values() if v)} of {len(raw)}")

    # Compact each layer
    payload = {
        "radar":        compact_radar(raw.get("convergence", {})),
        "positioning":  compact_positioning(raw.get("positioning", {})),
        "mechanics":    compact_mechanics(raw.get("mechanics", {})),
        "analytics":    compact_analytics(raw.get("analytics", {})),
        "pairs":        compact_pairs(raw.get("pairs", {})),
        "nlp":          compact_nlp(raw.get("nlp", {})),
        "research":     {  # ticker-research is large; only summaries
            "research": _compact_research(raw.get("research")),
        },
        "macro":        compact_synthesis(raw.get("synthesis", {})),
        "earnings_cal": compact_earnings_cal(raw.get("earnings_cal", {})),
    }

    # Compute market temperature (lightweight composite)
    temperature = compute_market_temperature(payload["radar"], payload["positioning"],
                                                payload["mechanics"])
    payload["market_temperature_precomputed"] = temperature

    # Compute whats_changed from prior brief
    cur_top = payload["radar"].get("top_candidates", [])[:5]
    whats_changed_data = compute_whats_changed(prior_brief, cur_top)
    payload["whats_changed_data"] = whats_changed_data

    print(f"[brief] payload size: {len(json.dumps(payload, default=str))} chars · "
          f"temperature: {temperature['score']} ({temperature['rank']})")

    # Build prompt + call Claude
    user_prompt = build_user_prompt(payload)
    print(f"[brief] prompt: {len(user_prompt)} chars; calling Claude")

    try:
        t_claude = time.time()
        response_text = call_anthropic(SYSTEM_PROMPT, user_prompt, max_tokens=10000)
        claude_elapsed = round(time.time() - t_claude, 2)
        print(f"[brief] Claude: {len(response_text)} chars in {claude_elapsed}s")
    except Exception as e:
        return _write_error(f"Claude error: {str(e)[:200]}")

    # Parse
    try:
        brief = extract_json(response_text)
    except Exception as e:
        return _write_error(f"JSON parse error: {str(e)[:200]}",
                              raw_preview=response_text[:600])

    # Build output
    output = {
        "schema_version":    "1.0",
        "generated_at":      datetime.now(timezone.utc).isoformat(),
        "model":             MODEL,
        "elapsed_sec":       round(time.time() - t0, 2),
        "claude_elapsed":    claude_elapsed,

        "executive_summary": brief.get("executive_summary"),
        "conviction_grade":  brief.get("conviction_grade"),
        "macro_frame":       brief.get("macro_frame"),

        "market_temperature": {
            **temperature,
            "label_from_ai":  brief.get("market_temperature_label"),
        },

        "top_3_long_ideas":   brief.get("top_3_long_ideas", []),
        "top_2_pair_trades":  brief.get("top_2_pair_trades", []),
        "what_to_watch_today": brief.get("what_to_watch_today", []),
        "risk_warnings":      brief.get("risk_warnings", []),

        "whats_changed_narrative": brief.get("whats_changed"),
        "whats_changed_data":      whats_changed_data,

        "source_versions": {name: (raw.get(name, {}) or {}).get("generated_at")
                              for name in INPUT_KEYS.keys()},

        "disclaimer": ("Synthesis of 9 quant data layers via Claude. Sized positions are "
                        "parametric (vol/Kelly-derived); pair-trade alpha estimates are "
                        "spread-mean-reversion proxies. Not financial advice. Verify before "
                        "acting and adjust to your own risk tolerance."),
    }

    body = json.dumps(output, indent=2, default=str)
    s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY, Body=body,
                    ContentType="application/json", CacheControl="max-age=600")
    # Archive yesterday's brief separately (rollover happens via OUTPUT_KEY write above)
    try:
        archive_key = (f"data/archive/pump-radar-brief/"
                        f"{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M')}.json")
        s3.put_object(Bucket=S3_BUCKET, Key=archive_key, Body=body,
                        ContentType="application/json")
    except Exception:
        pass

    summary = {
        "status":           "ok",
        "elapsed_sec":      output["elapsed_sec"],
        "claude_elapsed":   claude_elapsed,
        "conviction":       output["conviction_grade"],
        "temperature":      f"{temperature['score']} ({temperature['rank']})",
        "n_long_ideas":     len(output["top_3_long_ideas"]),
        "n_pair_trades":    len(output["top_2_pair_trades"]),
        "n_warnings":       len(output["risk_warnings"]),
    }
    print(f"[brief] done: {summary}")
    return {"statusCode": 200, "body": json.dumps(summary)}


def _write_error(message: str, **extras) -> dict:
    payload = {"schema_version": "1.0", "generated_at": datetime.now(timezone.utc).isoformat(),
                "status": "error", "error": message, **extras}
    try:
        s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY,
                        Body=json.dumps(payload, default=str, indent=2),
                        ContentType="application/json", CacheControl="max-age=300")
    except Exception: pass
    print(f"[brief] ERROR: {message}")
    return {"statusCode": 500, "body": json.dumps({"status": "error", "error": message})}
