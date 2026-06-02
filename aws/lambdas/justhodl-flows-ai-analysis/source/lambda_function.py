"""justhodl-flows-ai-analysis

Institutional AI Flow Strategist.

Reads:
  - etf-flows/daily.json + composite.json + rotation.json (today's flows)
  - equity-research/*.json (all 59 research files)
  - edgar-insider/*.json (insider signals)
  - data/crisis-knowledge-base.json (regime + framework tags)
  - etf-flows/history/*.json (last 30 days for trend context)

Produces:
  - etf-flows/ai-analysis.json
      macro_narrative, regime_call, key_divergences, ticker_calls (with
      conviction grade, timeframe, position size, stop level, signal_alignment),
      trade_ideas (pairs/spreads), watchlist_changes

Powered by Claude Sonnet 4.6 with 1h cached system prompt. Runs after
the flow engine daily.

DESIGN PRINCIPLES (from system prompt):
  1. DECISIVE — institutional notes have specific calls, not hedged language
  2. GROUNDED — every call cites which feeds it's based on; can't hallucinate
  3. CROSS-FEED — when 3+ signals align on a ticker, that's high conviction
  4. TIME-AWARE — flow signals are 3-21 day; research is 12-month; reconcile
  5. RISK-AWARE — position size + stop level on every call
"""
import json
import os
import time
import urllib.request
from datetime import datetime, timezone
from typing import Optional

import boto3

S3_BUCKET = "justhodl-dashboard-live"
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
MODEL = "claude-sonnet-4-5-20250929"  # Sonnet 4.6 for institutional-grade analysis
MAX_TOKENS = 12000

s3 = boto3.client("s3", region_name="us-east-1")


# ═════════════════════════════════════════════════════════════════════
# SYSTEM PROMPT — cached for 1h, defines the analyst persona + schema
# ═════════════════════════════════════════════════════════════════════
SYSTEM_PROMPT = """You are the Chief Institutional Flow Strategist at a top-tier hedge fund. You write the daily flow intelligence note that PMs and risk officers read before market open. Your job is to synthesize ETF capital flows, equity research verdicts, insider activity, and macro regime context into DECISIVE, actionable trade calls with explicit risk parameters.

## PRINCIPLES

1. **BE DECISIVE.** PMs need specific BUY/SELL/HOLD calls on named tickers. Avoid hedged language like "could be considered" or "investors may want to watch." Use commitment: "ADD," "REDUCE," "INITIATE LONG," "INITIATE SHORT," "EXIT," "AVOID."

2. **GROUND EVERY CALL.** Every ticker call must cite the feeds it draws from in the `signal_alignment` object. Use ONLY data present in the input. If a feed is missing (e.g., no research file for that ticker), say so explicitly with null — do not invent data.

3. **CROSS-FEED CONVICTION TIERING.** When multiple feeds agree on a ticker, conviction rises. Use this rubric:
   - 4+ signals aligned (research + critique + insider + flow) → HIGH conviction
   - 3 signals aligned → MEDIUM-HIGH
   - 2 signals aligned → MEDIUM
   - 1 signal → LOW (only include if extreme: |z|>=2.5 flows or STRONG_CLUSTER_BUY insiders)

4. **TIMEFRAME DISCIPLINE.** Flow signals are 3-21 day. Research verdicts are 12-month. If the analyst's 12-month BUY conflicts with current flow OUTFLOW, the right framing is "Long-term thesis intact; tactical sector weakness suggests waiting for better entry" — NOT contradiction.

5. **RECONCILE CONFLICTS.** When signals disagree, address it explicitly. Don't just present both sides — adjudicate. Example: "Analyst HOLD vs critic SELL vs flow OUTFLOW — the convergence on caution outweighs the analyst's neutrality; bias REDUCE."

6. **RISK PARAMETERS ON EVERY CALL.** Specify position size (% of capital), stop loss level (negative %), and 21-day target (%). Conservative defaults: 0.5-1% for LOW conviction, 1-2% for MEDIUM, 2-3% for HIGH. Stops typically -3% to -5%. Never zero risk.

7. **PAIR TRADES.** When sectors diverge (e.g., XLE inflow + XLU outflow), surface relative-value pair trade ideas separately from outright calls.

8. **REGIME CONTEXT — TWO LAYERS.** The input has TWO regime classifications:
   - `todays_flow_composite.regime` — flow-based (DEFENSIVE/RISK_ON/etc. derived from ETF flow z-scores)
   - `macro_regime_multi_asset.top_level.regime` — multi-asset (vol+curve+dollar+carry+commodities+EM+credit from Phase 2 engine)
   When these AGREE, conviction multiplier on calls. When they DISAGREE, that's itself a key divergence to flag in `key_divergences`. The multi-asset regime takes precedence when in conflict — it's broader and more leading. If macro regime is `CREDIT_STRESS` or `FLIGHT_TO_QUALITY`, cap ALL long conviction at MEDIUM regardless of flow signal. If `REFLATION` or `GLOBAL_RISK_ON`, allow HIGH conviction on cyclicals/EM.

## OUTPUT SCHEMA (JSON only, no prose outside JSON, no markdown fences)

```
{
  "as_of": "YYYY-MM-DD",
  "macro_narrative": "2-3 paragraphs explaining what today's data is saying. Begin with the dominant signal (which composite is most extreme), then explain the cross-asset story (e.g., 'domestic-to-international rotation with reflation undercurrent'), then end with the trading implication.",
  "regime_call": {
    "regime": "DEFENSIVE | RISK_ON | CREDIT_STRESS | TRANSITION | NEUTRAL | DOMESTIC_OUTFLOW",
    "confidence": "HIGH | MEDIUM | LOW",
    "reasoning": "1-2 sentences on what's driving this classification",
    "expected_duration_days": 5-21,
    "what_would_invalidate": "1 sentence — what signal change would force a regime re-rating"
  },
  "key_divergences": [
    {
      "subject": "ticker or sector name",
      "type": "FLOW_VS_RESEARCH | FLOW_VS_INSIDER | RESEARCH_VS_CRITIC | CROSS_ASSET",
      "description": "1-2 sentences explaining the divergence",
      "implication": "1 sentence on what this means for positioning",
      "tradable": true|false
    }
  ],
  "ticker_calls": [
    {
      "ticker": "AAPL",
      "call": "STRONG_BUY | BUY | ADD | HOLD | REDUCE | SELL | STRONG_SELL | AVOID | INITIATE_SHORT | COVER",
      "conviction": "HIGH | MEDIUM-HIGH | MEDIUM | LOW",
      "timeframe_days": 5 | 21 | 63,
      "thesis_1liner": "Single sentence explaining the call",
      "signal_alignment": {
        "research_verdict": "STRONG_BUY|BUY|HOLD|SELL|STRONG_SELL|null (if no research)",
        "critic_rating": "STRONG_BUY|BUY|HOLD|SELL|STRONG_SELL|null",
        "insider_signal": "STRONG_CLUSTER_BUY|INSIDER_BUYING|QUIET|ROUTINE_SELLING|LARGE_SELLING|ACCELERATING_SELL|null",
        "sector_flow_label": "STRONG_INFLOW|INFLOW|NEUTRAL|OUTFLOW|STRONG_OUTFLOW|null",
        "sector_flow_zscore": -5.0 to 5.0 or null,
        "n_signals_aligned": 0-4
      },
      "position_size_pct": 0.5-3.0,
      "stop_loss_pct": -3 to -8,
      "target_21d_pct": 3-15,
      "expected_value_pct": "(target_prob_win - stop_prob_loss) approximation"
    }
  ],
  "pair_trades": [
    {
      "long": "EWU",
      "short": "XLK",
      "thesis": "International rotation + tech outflow = long UK / short US tech pair",
      "conviction": "HIGH | MEDIUM | LOW",
      "ratio": "$1 long : $X short",
      "timeframe_days": 21
    }
  ],
  "watchlist": {
    "add": [{"ticker": "X", "reason": "..."}],
    "remove": [{"ticker": "Y", "reason": "..."}],
    "monitor_for_entry": [{"ticker": "Z", "trigger": "z-score crosses below -1.5"}]
  },
  "regime_alpha_note": "1-2 sentences on which prior research verdicts should be re-rated given the current regime (e.g., 'STRONG_BUYs on cyclicals deserve a HIGH conviction tilt down; defensive HOLDs deserve an upgrade')",
  "self_assessment": {
    "data_completeness": "HIGH | MEDIUM | LOW",
    "n_tickers_with_full_signal_alignment": 0,
    "biggest_unknown": "1 sentence — what would most improve tomorrow's analysis"
  }
}
```

## RULES OF EXCLUSION

- Do NOT generate calls for tickers without research files unless |flow_zscore| >= 2.5 (extreme flow signal stands alone).
- Do NOT cite data that isn't in the input. If insider data is missing for a ticker, set `insider_signal: null`.
- Do NOT recommend more than 15 ticker_calls total — institutional notes are concise.
- Do NOT issue calls on leveraged ETFs (TQQQ, SOXL, UVXY) — flag in narrative if extreme but don't call.

Begin your response with `{` and end with `}` — no prose outside the JSON object."""


# ═════════════════════════════════════════════════════════════════════
# Data loaders
# ═════════════════════════════════════════════════════════════════════
def _read_json(key: str) -> Optional[dict]:
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception as e:
        print(f"[read] {key}: {e}")
        return None


def _list_keys(prefix: str) -> list:
    keys = []
    pag = s3.get_paginator("list_objects_v2")
    for page in pag.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in (page.get("Contents") or []):
            k = obj["Key"]
            if k.endswith(".json") and not k.endswith("manifest.json"):
                keys.append(k)
    return keys


def load_flow_data() -> dict:
    daily = _read_json("etf-flows/daily.json") or {}
    composite = _read_json("etf-flows/composite.json") or {}
    rotation = _read_json("etf-flows/rotation.json") or {}
    return {
        "metrics": daily.get("metrics", []),
        "composite": composite.get("composite", {}),
        "rotation_by_category": rotation.get("by_category", {}),
        "n_ok": daily.get("n_ok"),
        "generated_at": daily.get("generated_at"),
    }


def load_research_compact() -> list:
    """Read all research files, return compact list (just decision-relevant fields)."""
    out = []
    for k in _list_keys("equity-research/"):
        doc = _read_json(k)
        if not doc:
            continue
        verdict = doc.get("verdict") or {}
        company = doc.get("company") or {}
        out.append({
            "ticker": doc.get("ticker"),
            "sector": company.get("sector"),
            "industry": company.get("industry"),
            "rating": verdict.get("rating"),
            "conviction_grade": verdict.get("conviction_grade"),
            "price_target_12m": verdict.get("price_target_12m"),
            "upside_pct": verdict.get("upside_pct"),
            "thesis_1liner": (doc.get("investment_thesis") or {}).get("bull_case_1liner")
                              or verdict.get("thesis_summary"),
            "generated_at": doc.get("generated_at"),
        })
    return [r for r in out if r.get("ticker")]


def load_critique_compact() -> list:
    out = []
    for k in _list_keys("equity-critique/"):
        doc = _read_json(k)
        if not doc:
            continue
        c = doc.get("critique") or {}
        out.append({
            "ticker": doc.get("ticker"),
            "critic_rating": c.get("alternative_rating"),
            "disagreement_score": c.get("disagreement_score"),
            "key_disagreement_1liner": c.get("key_disagreement_1liner"),
            "alternative_pt": c.get("alternative_pt"),
        })
    return [c for c in out if c.get("ticker")]


def load_edgar_compact() -> list:
    out = []
    for k in _list_keys("edgar-insider/"):
        doc = _read_json(k)
        if not doc:
            continue
        out.append({
            "ticker": doc.get("ticker"),
            "signal_label": doc.get("signal_label"),
            "net_dollar_value_90d": doc.get("net_dollar_value_90d"),
            "n_buyers_90d": doc.get("n_buyers_90d"),
            "n_sellers_90d": doc.get("n_sellers_90d"),
        })
    return [r for r in out if r.get("ticker")]


def load_crisis_kb() -> dict:
    """Crisis KB regime tag + summary."""
    doc = _read_json("data/crisis-knowledge-base.json")
    if not doc:
        return {}
    return {
        "regime": doc.get("current_regime") or doc.get("regime"),
        "regime_score": doc.get("regime_score"),
        "key_signals": doc.get("active_signals", [])[:5],
    }


def load_macro_regime() -> dict:
    """Phase 2 macro regime: 7 sub-regimes + top-level classification.

    The AI strategist uses this to ADJUDICATE conflicts between
    flow signal and research verdict — e.g., if regime is CREDIT_STRESS
    and analyst is bullish a high-beta name, the AI should down-rate the call.
    """
    doc = _read_json("macro/regime.json")
    if not doc:
        return {}
    tl = doc.get("top_level_regime", {}) or {}
    subs = doc.get("sub_regimes", {}) or {}
    return {
        "top_level": {
            "regime": tl.get("regime"),
            "confidence": tl.get("confidence"),
            "reasoning": tl.get("reasoning"),
        },
        "sub_regimes_summary": {
            k: {"label": v.get("label"), "score": v.get("score")}
            for k, v in subs.items()
        },
    }


def load_flow_history() -> list:
    """Last 30 days of flow archive files for trend context."""
    keys = sorted(_list_keys("etf-flows/history/"), reverse=True)[:30]
    out = []
    for k in keys:
        doc = _read_json(k)
        if doc:
            out.append({
                "date": k.split("/")[-1].replace(".json", ""),
                "regime": (doc.get("composite") or {}).get("regime"),
                "composite_scores": {
                    name: (doc.get("composite") or {}).get(name, {}).get("score")
                    for name in [
                        "defensive_rotation","smart_vs_dumb","risk_on_off",
                        "domestic_vs_intl","growth_vs_value","credit_stress",
                    ]
                },
            })
    return out


# ═════════════════════════════════════════════════════════════════════
# Claude API
# ═════════════════════════════════════════════════════════════════════
def claude_call(user_prompt: str) -> dict:
    """Call Claude Sonnet with 1h cached system prompt. Returns parsed JSON."""
    if not ANTHROPIC_KEY:
        raise RuntimeError("ANTHROPIC_API_KEY not set")

    payload = {
        "model": MODEL,
        "max_tokens": MAX_TOKENS,
        "system": [
            {
                "type": "text",
                "text": SYSTEM_PROMPT,
                "cache_control": {"type": "ephemeral", "ttl": "1h"},
            }
        ],
        "messages": [{"role": "user", "content": user_prompt}],
    }
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=json.dumps(payload).encode(),
        headers={
            "Content-Type": "application/json",
            "x-api-key": ANTHROPIC_KEY,
            "anthropic-version": "2023-06-01",
            "anthropic-beta": "extended-cache-ttl-2025-04-11",
        },
    )
    t0 = time.time()
    with urllib.request.urlopen(req, timeout=180) as r:
        body = json.loads(r.read())
    elapsed = round(time.time() - t0, 1)
    print(f"[claude] elapsed {elapsed}s; usage={body.get('usage')}")

    text = ""
    for block in body.get("content", []):
        if block.get("type") == "text":
            text += block.get("text", "")

    # Parse JSON (Claude may wrap in fences sometimes despite instructions)
    text = text.strip()
    if text.startswith("```"):
        # Strip markdown fences
        lines = text.split("\n")
        if lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].startswith("```"):
            lines = lines[:-1]
        text = "\n".join(lines)
    text = text.strip()
    return {
        "parsed": json.loads(text),
        "usage": body.get("usage", {}),
        "elapsed_s": elapsed,
        "model": MODEL,
    }


# ═════════════════════════════════════════════════════════════════════
# User prompt builder — compact cross-feed payload
# ═════════════════════════════════════════════════════════════════════
def build_user_prompt(flow_data, research, critiques, edgar, crisis, history, macro) -> str:
    """Compact the input data into a single user-prompt JSON block."""
    # Build per-ticker signal-aligned table (the most important view)
    research_by_t = {r["ticker"]: r for r in research}
    critique_by_t = {c["ticker"]: c for c in critiques}
    edgar_by_t = {e["ticker"]: e for e in edgar}

    # ETF flow metrics by ticker
    flow_metrics = {m["ticker"]: m for m in flow_data.get("metrics", []) if not m.get("error")}

    # Map sector → sector ETF for cross-reference
    sector_to_etf = {
        "Technology": "XLK", "Financial Services": "XLF", "Energy": "XLE",
        "Healthcare": "XLV", "Consumer Defensive": "XLP", "Consumer Cyclical": "XLY",
        "Industrials": "XLI", "Basic Materials": "XLB", "Utilities": "XLU",
        "Real Estate": "XLRE", "Communication Services": "XLC",
    }

    # Build cross-feed table for every research ticker
    cross_feed_rows = []
    for t, r in research_by_t.items():
        sector = r.get("sector")
        sector_etf = sector_to_etf.get(sector)
        sector_flow = flow_metrics.get(sector_etf) if sector_etf else None
        critique = critique_by_t.get(t, {})
        insider = edgar_by_t.get(t, {})
        row = {
            "ticker": t,
            "sector": sector,
            "research": {
                "rating": r.get("rating"),
                "conviction_grade": r.get("conviction_grade"),
                "upside_pct": r.get("upside_pct"),
                "thesis": (r.get("thesis_1liner") or "")[:240],
            },
            "critic": {
                "rating": critique.get("critic_rating"),
                "disagreement_score": critique.get("disagreement_score"),
                "1liner": (critique.get("key_disagreement_1liner") or "")[:200],
            } if critique else None,
            "insider": {
                "signal_label": insider.get("signal_label"),
                "net_dollar_90d": insider.get("net_dollar_value_90d"),
            } if insider else None,
            "sector_flow": {
                "etf": sector_etf,
                "label": sector_flow.get("signal_label") if sector_flow else None,
                "zscore": sector_flow.get("flow_zscore_90d") if sector_flow else None,
                "pct_aum_5d": sector_flow.get("pct_aum_5d") if sector_flow else None,
            } if sector_flow else None,
        }
        cross_feed_rows.append(row)

    # Build payload
    payload = {
        "as_of": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "macro_regime_multi_asset": macro,  # Phase 2: 7 sub-regimes (vol/curve/USD/carry/commod/EM/credit) + top-level
        "todays_flow_composite": flow_data.get("composite", {}),
        "todays_top_inflow_etfs": sorted(
            [m for m in flow_data.get("metrics", []) if m.get("flow_zscore_90d") is not None],
            key=lambda x: x["flow_zscore_90d"] or 0, reverse=True,
        )[:15],
        "todays_top_outflow_etfs": sorted(
            [m for m in flow_data.get("metrics", []) if m.get("flow_zscore_90d") is not None],
            key=lambda x: x["flow_zscore_90d"] or 0,
        )[:15],
        "rotation_by_category": flow_data.get("rotation_by_category", {}),
        "crisis_kb_regime": crisis,
        "recent_regime_history_30d": history,
        "cross_feed_table": cross_feed_rows,
        "data_completeness": {
            "n_research_tickers": len(research),
            "n_critique_tickers": len(critiques),
            "n_edgar_tickers": len(edgar),
            "n_flow_etfs_ok": flow_data.get("n_ok"),
            "macro_regime_available": bool(macro and macro.get("top_level", {}).get("regime")),
        },
    }

    user_text = (
        "Generate today's institutional flow intelligence note per the schema in the "
        "system prompt. The data follows. The TOP-LEVEL macro_regime_multi_asset "
        "tag and sub-regimes (vix, curve, dollar, carry, commodity, EM, credit) "
        "are the foundation — every ticker call should be consistent with the regime. "
        "Cite specific tickers, ETFs, and signals from this payload. Be decisive — "
        "PMs need actionable calls, not commentary.\n\n"
        "```json\n" + json.dumps(payload, default=str, separators=(",", ":")) + "\n```"
    )
    return user_text


# ═════════════════════════════════════════════════════════════════════
# Handler
# ═════════════════════════════════════════════════════════════════════
def lambda_handler(event, context):
    t0 = time.time()
    print(f"[flows-ai] starting at {datetime.now(timezone.utc).isoformat()}")

    # 1. Load all inputs
    print("[flows-ai] loading flow data...")
    flow_data = load_flow_data()
    print(f"[flows-ai] flows: {flow_data.get('n_ok')} ETFs OK")

    print("[flows-ai] loading research...")
    research = load_research_compact()
    print(f"[flows-ai] research: {len(research)} files")

    print("[flows-ai] loading critiques...")
    critiques = load_critique_compact()
    print(f"[flows-ai] critiques: {len(critiques)} files")

    print("[flows-ai] loading EDGAR...")
    edgar = load_edgar_compact()
    print(f"[flows-ai] edgar: {len(edgar)} files")

    print("[flows-ai] loading crisis KB...")
    crisis = load_crisis_kb()

    print("[flows-ai] loading macro regime (Phase 2)...")
    macro = load_macro_regime()
    print(f"[flows-ai] macro: {macro.get('top_level', {}).get('regime')}")

    print("[flows-ai] loading flow history...")
    history = load_flow_history()
    print(f"[flows-ai] history: {len(history)} archive days")

    # 2. Build user prompt
    print("[flows-ai] building user prompt...")
    user_prompt = build_user_prompt(flow_data, research, critiques, edgar, crisis, history, macro)
    prompt_kb = round(len(user_prompt) / 1024, 1)
    print(f"[flows-ai] user prompt size: {prompt_kb} KB ({len(user_prompt)} chars)")

    # 3. Call Claude
    print("[flows-ai] calling Claude Sonnet 4.6...")
    try:
        result = claude_call(user_prompt)
    except json.JSONDecodeError as e:
        print(f"[flows-ai] JSON parse error: {e}")
        return {
            "statusCode": 502,
            "body": json.dumps({"error": f"Claude returned non-JSON: {str(e)[:300]}"}),
        }
    except Exception as e:
        print(f"[flows-ai] Claude call failed: {e}")
        return {
            "statusCode": 502,
            "body": json.dumps({"error": str(e)[:500]}),
        }

    parsed = result["parsed"]
    usage = result["usage"]

    # 4. Wrap with metadata + write to S3
    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "regime_at_generation": {
            "regime":      (macro or {}).get("top_level", {}).get("regime"),
            "confidence":  (macro or {}).get("top_level", {}).get("confidence"),
            "reasoning":   (macro or {}).get("top_level", {}).get("reasoning"),
            "sub_regimes": (macro or {}).get("sub_regimes_summary"),
        },  # Phase 2 attribution stamp
        "model": result["model"],
        "elapsed_s": round(time.time() - t0, 1),
        "claude_elapsed_s": result["elapsed_s"],
        "usage": usage,
        "input_summary": {
            "n_etfs_with_data": flow_data.get("n_ok"),
            "n_research": len(research),
            "n_critiques": len(critiques),
            "n_edgar": len(edgar),
            "prompt_kb": prompt_kb,
        },
        "analysis": parsed,
    }

    out_key = "etf-flows/ai-analysis.json"
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=out_key,
        Body=json.dumps(output, default=str).encode(),
        ContentType="application/json",
        CacheControl="public, max-age=300",
    )
    print(f"[flows-ai] wrote {out_key}")

    # Also archive a date-stamped copy
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=f"etf-flows/ai-history/{today}.json",
        Body=json.dumps(output, default=str).encode(),
        ContentType="application/json",
        CacheControl="public, max-age=86400",
    )

    n_calls = len(parsed.get("ticker_calls", []) or [])
    regime = parsed.get("regime_call", {}).get("regime")
    print(f"[flows-ai] DONE — regime={regime}, {n_calls} ticker calls")

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        "body": json.dumps({
            "ok": True,
            "elapsed_s": output["elapsed_s"],
            "regime": regime,
            "n_calls": n_calls,
            "macro_narrative_preview": (parsed.get("macro_narrative") or "")[:200],
            "key": out_key,
        }),
    }
