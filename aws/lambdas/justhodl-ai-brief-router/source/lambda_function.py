"""
justhodl-ai-brief-router — Generic AI Decisive Call Generator
================================================================
Loads a context registry from S3 (config/ai-brief-contexts.json), then
generates institutional decisive-call briefs for every configured context
in PARALLEL. Each context defines its own inputs, KB keywords, analyst
persona, asset universe to predict over, and output key.

Adding a 7th/8th/Nth context = edit the S3 config. No Lambda redeploy.

USES jhcore (s3io, kb). Inline Claude call with 90s timeout (the longer
historical_predictions schema needs >30s on Haiku-4.5).

Output schema (per context, written to data/<context-output_key>.json):
  version, generated_at, model, context (id),
  regime, confidence, one_liner, thesis,
  supporting_evidence[], historical_analogs[], cross_asset[],
  trade_ideas[], tripwires[], next_event_watch[],
  historical_predictions[]
"""
import json
import os
import time
import urllib.request
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta

from jhcore import s3io, kb

REGISTRY_KEY = "config/ai-brief-contexts.json"
EPISODE_REF_KEY = "data/episode-reference.json"
MAX_WORKERS = 12  # 23 contexts → 2 batches; before was 6 → 4 batches
DEFAULT_TIMEOUT = 95


# ─────────────────────────────────────────────────────────────────────
# Local Claude wrapper — bypasses jhcore.claude's 30s default
# ─────────────────────────────────────────────────────────────────────
def claude_json(prompt, system, max_tokens=8000, temperature=0.25,
                 model="claude-haiku-4-5-20251001", timeout=DEFAULT_TIMEOUT):
    key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not key:
        return None, "missing ANTHROPIC_API_KEY"
    body = {
        "model": model,
        "max_tokens": max_tokens,
        "temperature": temperature,
        "messages": [{"role": "user", "content":
            prompt + "\n\nRespond ONLY with valid JSON, no preamble or markdown."}],
        "system": system,
    }
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=json.dumps(body).encode("utf-8"),
        headers={
            "x-api-key": key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            data = json.loads(r.read())
    except Exception as e:
        return None, f"http: {e}"
    parts = []
    for blk in data.get("content", []):
        if blk.get("type") == "text":
            parts.append(blk.get("text", ""))
    txt = "\n".join(parts).strip()
    if txt.startswith("```"):
        lines = [l for l in txt.split("\n") if not l.strip().startswith("```")]
        txt = "\n".join(lines).strip()
    try:
        return json.loads(txt), None
    except Exception as e:
        # Attempt recovery: Claude sometimes truncates mid-array if it hits max_tokens.
        # Try to find the last complete top-level key and close the JSON.
        repaired = _attempt_json_repair(txt)
        if repaired is not None:
            return repaired, f"recovered_truncated (orig err: {str(e)[:80]})"
        return None, f"parse: {e} head={txt[:200]}"


def _attempt_json_repair(txt):
    """Best-effort repair for max_tokens-truncated JSON. Returns dict or None."""
    if not txt.startswith("{"):
        return None
    # Strategy: trim back to the last complete `"key": value,` pattern, close braces.
    # Walk backwards looking for a complete value followed by ",\n" or "\n}"
    s = txt.rstrip()
    # Strip the dangling tail until we hit a likely end of a value (a quote, ], or })
    for trim_to in range(len(s), 0, -1):
        c = s[trim_to - 1]
        if c not in '}"]':
            continue
        candidate = s[:trim_to]
        # Try closing common patterns:
        for closer in ["}", "]}", "}]}", "]]}", "\"}"]:
            attempt = candidate.rstrip(",").rstrip() + closer
            try:
                return json.loads(attempt)
            except Exception:
                continue
    return None


# ─────────────────────────────────────────────────────────────────────
# Prompt builder — generic across contexts
# ─────────────────────────────────────────────────────────────────────
BASE_SYSTEM = (
    "You are a senior strategist at a top-tier hedge fund, writing the morning "
    "decisive-call brief for the firm's portfolio manager. You think regime-aware, "
    "position-size-conscious, brutal about evidence vs narrative. You ALWAYS cite "
    "specific numbers from the data provided. You compare to specific historical "
    "episodes by name. When you give a trade idea you name a specific instrument "
    "and a specific level. You never hand-wave. You flag divergences between the "
    "primary signal and the broader regime explicitly.\n\n"
    "CRITICAL — HISTORICAL PROJECTION DISCIPLINE: Every forecast must anchor to a "
    "specific named historical analog. You cite what each asset DID during that "
    "analog (% moves over weeks), then project a SIMILAR-SHAPE range — never "
    "invented dollar prices, always percentages with horizon in weeks. You assign "
    "a probability and confidence honestly — 50-65% is normal for forward views. "
    "You always give BOTH upside trigger AND downside scenario for every asset."
)


def build_prompt(ctx_id, cfg, primary_data, cross_data, episode_ref, kb_chunks):
    """Generate the brief-generation prompt for one context."""
    # Primary data — truncate if huge (leave token budget for richer output)
    primary_str = json.dumps(primary_data, indent=2, default=str)
    if len(primary_str) > 3500:
        primary_str = primary_str[:3500] + "\n... (truncated)"

    # Cross-regime summary
    cross_lines = []
    for cid, d in (cross_data or {}).items():
        if not d:
            continue
        field = (cfg.get("cross_regime_fields") or {}).get(cid)
        regime_val = d.get(field) if field else (d.get("regime") or d.get("severity") or d.get("composite_regime"))
        note = (d.get("interpretation") or d.get("summary") or "")[:100]
        cross_lines.append(f"  - {cid}: {regime_val}" + (f"  ({note})" if note else ""))
    cross_block = "\n".join(cross_lines) or "  (no cross-context regimes loaded)"

    # Episode reference — show relevant indicators
    ep_lines = []
    sids = cfg.get("episode_indicators_to_show", ["T10Y2Y", "DGS10", "DFII10", "VIXCLS", "BAMLH0A0HYM2"])
    for sid in sids[:6]:
        ind = (episode_ref.get("indicators") or {}).get(sid)
        if ind:
            ne = ind.get("nearest_episode") or {}
            ep_lines.append(
                f"  - {ind.get('label', sid)}: {ind.get('current')}{ind.get('unit','')} "
                f"({ind.get('percentile')}th pctile, analog: {ne.get('name','?')} [{ne.get('type','?')}])"
            )
    ep_block = "\n".join(ep_lines) or "  (episode-reference unavailable)"

    # KB excerpts
    kb_block = ""
    if kb_chunks:
        kb_block = "\n\nRELEVANT MACRO FRAMEWORKS:\n" + "\n\n".join(
            f"### {c['framework']}\n{c['excerpt'][:1100]}" for c in kb_chunks
        )

    # Asset list for predictions section
    assets_list_lines = []
    for i, a in enumerate(cfg["assets_to_predict"], 1):
        assets_list_lines.append(f"  {i}. \"{a['asset']}\"  ticker {a['ticker']}")
    assets_block = "\n".join(assets_list_lines)

    # Build the full prompt
    prompt = f"""# {cfg["title"]} — INSTITUTIONAL BRIEF

## CONTEXT (what you're analysing)
{cfg["prompt_intro"]}

## PRIMARY DATA ({cfg["primary_feed"]})
{primary_str}

## CROSS-CONTEXT REGIMES (other markets right now)
{cross_block}

## EPISODE REFERENCE (where macro indicators sit vs history)
{ep_block}
{kb_block}

## TASK

Write the institutional Decisive Call brief in this exact JSON shape:

{{
  "regime": "<RISK_ON_AGGRESSIVE | RISK_ON | NEUTRAL | RISK_OFF | CRISIS_PREP>",
  "confidence": "<HIGH | MEDIUM | LOW>",
  "one_liner": "<single decisive sentence ≤140 chars>",
  "thesis": "<3-5 sentence narrative. Cite SPECIFIC numbers from the primary data. Compare to historical period by NAME. Flag divergences between primary signal and cross-context regimes.>",
  "supporting_evidence": [
    {{"point": "<one fact, ≤25 words>", "data": "<specific numbers from primary data>"}}
    // 4-6 items
  ],
  "historical_analogs": [
    {{"period": "<specific period e.g. 2024 Q1, 2008-09 GFC, 1979 Volcker>",
      "similarity_pct": <0-100>,
      "what_happened": "<≤30 words>", "expectation": "<≤30 words>"}}
    // 1-3 items
  ],
  "cross_asset": [
    {{"asset": "<asset name>", "direction": "<BULLISH|MIXED|NEUTRAL|CAUTION|BEARISH>",
      "why": "<≤30 words citing data>", "instruments": "<comma-separated tickers>"}}
    // 6 items covering: US Equities, Credit (HY/IG), Crypto (BTC/ETH), Gold, US Dollar, Duration/Bonds
  ],
  "trade_ideas": [
    {{"setup": "<≤25 words>", "instrument": "<real ticker>",
      "level": "<specific entry/strike/spread>",
      "thesis_link": "<≤20 words>", "risk_reward": "<e.g. 1:3>"}}
    // 3-5 items — each MUST name a real instrument + level
  ],
  "tripwires": [
    {{"condition": "<specific numeric threshold>",
      "severity": "<LOW|MEDIUM|HIGH>",
      "action": "<≤20 words>"}}
    // 3-5 items — each MUST have a numeric threshold
  ],
  "next_event_watch": [
    {{"event": "<specific upcoming event or data release>",
      "date": "<approximate date or N/A>",
      "watch_thresholds": "<specific numeric thresholds>",
      "clean_signal_means": "<≤25 words>",
      "dirty_signal_means": "<≤25 words>"}}
    // 1-3 items
  ],
  "historical_predictions": [
    // 7 items covering — in THIS ORDER — these specific assets:
{assets_block}
    {{"asset": "<one of the 7 above>",
      "ticker": "<as listed above>",
      "best_analog_period": "<specific period e.g. '2024 Q1' or '2020-03 COVID' or '2008-09 GFC' or '1979 Volcker'>",
      "analog_outcome_summary": "<what THIS asset did during that analog — concrete % move and weeks, ≤40 words>",
      "prediction_direction": "<UPSIDE | DOWNSIDE | SIDEWAYS>",
      "prediction_range_low_pct": <number — e.g. 15 means +15%; for downside use negative>,
      "prediction_range_high_pct": <number — wider end same sign convention as low>,
      "prediction_horizon_weeks": <integer — 4, 8, 12, 16, 24>,
      "confidence": "<HIGH | MEDIUM | LOW>",
      "probability_pct": <integer 0-100 — probability range holds>,
      "upside_trigger": "<one specific numeric data condition, ≤25 words>",
      "downside_scenario_pct": <number — worst-case % move if downside triggers, e.g. -25>,
      "downside_trigger": "<one specific numeric data condition, ≤25 words>",
      "key_reasoning": "<why THIS analog applies given current data, ≤30 words>"
    }}
  ]
}}

## RULES (institutional standard, NO exceptions)
- Cite REAL numbers from the primary data. Don't invent.
- Trade ideas MUST name real ticker + real level. No "watch for stress" allowed.
- Tripwires MUST have numeric thresholds.
- Historical predictions MUST anchor to a SPECIFIC named historical period.
  Project in % over weeks, NEVER invent dollar prices. Probability should reflect
  honest uncertainty.
- For Bitcoin/crypto specifically, lean on cycle analogs (2017-Q4 top, 2019-Q1
  bottom, 2020-Q4 launch, 2022-Q2 capitulation, 2023-Q1 recovery, 2024-Q1 ATH).
- If the primary signal disagrees with cross-context regimes, flag it explicitly.
- confidence=LOW is appropriate when signals conflict. Don't force HIGH.
- Stay under ~900 words total."""

    return prompt


# ─────────────────────────────────────────────────────────────────────
# Per-NAME prompt builder — for top-N ticker briefs (baggers, screeners, etc)
# ─────────────────────────────────────────────────────────────────────
BASE_SYSTEM_NAMES = (
    "You are a senior equity / cross-asset analyst writing brief, decisive per-name "
    "investment briefs for the firm's portfolio manager. Each brief is 4-6 sentences "
    "max. You think in terms of catalyst + risk + asymmetric payoff. You ALWAYS "
    "anchor every thesis to a specific named historical analog (same sector or pattern). "
    "You cite specific numbers from the input data. You quantify the asymmetric payoff "
    "shape (e.g. '5-10x over 24-36 months at 15% probability'). You do NOT invent prices "
    "or fundamentals — only use what's in the input data and reason from current macro "
    "regime. If the macro regime is hostile to the setup type (e.g. risk-off vs growth "
    "names), say so via regime_fit=POOR_FIT."
)


def build_names_prompt(ctx_id, cfg, primary_data, cross_data, episode_ref, kb_chunks):
    """Per-name brief prompt. Extracts top N tickers from primary feed."""
    # Resolve top-N. Default is top 15, configurable per context.
    top_n = cfg.get("top_n", 15)
    tickers_field = cfg.get("primary_tickers_field", "names")
    score_field = cfg.get("primary_score_field", "score")
    ticker_id_field = cfg.get("ticker_id_field", "ticker")

    def _get_path(d, path):
        """Traverse a dotted path through a nested dict."""
        cur = d
        for k in str(path).split("."):
            if isinstance(cur, dict):
                cur = cur.get(k)
            else:
                return None
            if cur is None:
                return None
        return cur

    # Try several common shapes — first the configured path (supports dots), then fallbacks
    tickers = None
    if isinstance(primary_data, dict):
        # Configured path first (may be nested with dots)
        v = _get_path(primary_data, tickers_field)
        if isinstance(v, list) and v:
            tickers = v
        else:
            # Fallback: try common top-level keys
            for k in ("names", "top", "top_100", "all_qualifying", "clusters", "top_setups",
                      "results", "tickers", "candidates", "items"):
                v2 = primary_data.get(k)
                if isinstance(v2, list) and v2:
                    tickers = v2
                    break
    elif isinstance(primary_data, list):
        tickers = primary_data
    tickers = tickers or []

    # Take top N by score field if sortable
    if tickers and isinstance(tickers[0], dict):
        try:
            tickers = sorted(tickers,
                              key=lambda t: float(t.get(score_field) or t.get("score") or 0),
                              reverse=True)
        except Exception:
            pass
    tickers = tickers[:top_n]

    # Compact each ticker's data — keep what fits. Normalize the ID to "ticker" key
    # for Claude consistency.
    compact = []
    for t in tickers:
        if isinstance(t, str):
            compact.append({"ticker": t})
            continue
        # Make a trimmed dict with the most useful fields
        rec = dict(t)
        # If ID is under a non-"ticker" field, copy it
        if "ticker" not in rec and ticker_id_field in rec:
            rec["ticker"] = rec[ticker_id_field]
        # Truncate each ticker record to ~700 chars so Claude can see ~10-15 names
        s = json.dumps(rec, default=str)
        if len(s) > 700:
            # Try to keep the high-signal fields only
            keep = {}
            for k in (ticker_id_field, "ticker", "symbol", "name", "company", "sector", "industry",
                      score_field, "score", "rank", "flag", "rationale", "thesis", "signal_type",
                      "signal_types", "fundamentals", "n_buyers", "n_insiders", "n_funds_holding",
                      "legend_buyers", "has_ceo", "has_cfo", "pillars", "key_stats",
                      "ret_1m", "ret_3m", "ret_12m", "mom_12_1", "composite_score"):
                if k in rec:
                    keep[k] = rec[k]
            rec = keep
            s = json.dumps(rec, default=str)
            if len(s) > 700:
                s = s[:700] + "...}"
                try:
                    rec = json.loads(s)
                except Exception:
                    pass
        compact.append(rec)

    # Cross regime summary
    cross_lines = []
    for cid, d in (cross_data or {}).items():
        if not d:
            continue
        field = (cfg.get("cross_regime_fields") or {}).get(cid)
        regime_val = d.get(field) if field else (d.get("regime") or d.get("severity") or d.get("composite_regime"))
        if regime_val:
            cross_lines.append(f"  - {cid}: {regime_val}")
    cross_block = "\n".join(cross_lines) or "  (no cross-context regimes loaded)"

    kb_block = ""
    if kb_chunks:
        kb_block = "\n\nRELEVANT FRAMEWORKS:\n" + "\n\n".join(
            f"### {c['framework']}\n{c['excerpt'][:700]}" for c in kb_chunks
        )

    score_label = cfg.get("score_label", "Score")

    prompt = f"""# {cfg["title"]} — PER-NAME BRIEFS

## CONTEXT
{cfg["prompt_intro"]}

## CURRENT MACRO REGIME (cross-context)
{cross_block}
{kb_block}

## TOP {len(compact)} CANDIDATES FROM THE UNIVERSE (sorted by {score_label})
{json.dumps(compact, indent=2, default=str)[:7500]}

## TASK

For each of the {len(compact)} tickers above, generate a brief in this JSON shape.
You MUST cover ALL {len(compact)} tickers in the same order they appear above.

Return EXACTLY this JSON shape:
{{
  "regime_note": "<1-line overall macro-context note for this name-universe, ≤140 chars>",
  "names": [
    {{
      "ticker": "<as in input>",
      "rank": <1-{len(compact)}, position in input>,
      "primary_score": <the score value from input, or null>,
      "regime_fit": "<STRONG_FIT | NEUTRAL | POOR_FIT — is current macro a tailwind, neutral, or headwind for this name's setup type>",
      "one_liner": "<single decisive sentence ≤120 chars naming the specific edge or risk>",
      "thesis": "<2-3 sentences citing specific numbers from the input data. Explain WHY this name. NO invented prices.>",
      "catalyst": "<specific upcoming catalyst — earnings, sector rotation, regime confirmation, etc., ≤25 words>",
      "primary_risk": "<specific failure mode — sector crowding, balance-sheet risk, macro reversal, etc., ≤25 words>",
      "historical_analog": {{
        "ticker": "<a real comparable ticker that exhibited the same setup pattern>",
        "period": "<specific period e.g. 2016-2018>",
        "what_happened": "<concrete % move and duration, ≤30 words>"
      }},
      "confidence": "<HIGH | MEDIUM | LOW>",
      "asymmetric_estimate": "<e.g. '5-10x over 24-36 months at 15% probability'>"
    }}
    // ... {len(compact)} items total, one per input ticker
  ]
}}

## RULES
- Cite specific numbers from the input ticker data. Don't invent fundamentals.
- Historical analog must be a REAL ticker that traded similarly (e.g. AMD 2016, NVDA 2019,
  ENPH 2020, PLUG 2020-21, AEHR 2023, SMCI 2023). Cite the % move.
- regime_fit honesty: if the macro is risk-off and the name is growth, regime_fit=POOR_FIT.
- confidence=HIGH only when the score is extreme AND macro is aligned.
- Stay under ~150 words per name. Brief is the point."""

    return prompt


def generate_names_brief(ctx_id, cfg, episode_ref):
    """Per-name brief worker — different schema from per-regime."""
    t0 = time.time()
    result = {"context_id": ctx_id, "title": cfg.get("title"), "output_key": cfg.get("output_key"),
              "brief_type": "names"}
    try:
        primary = s3io.get_json(cfg["primary_feed"], default={})
        if not primary:
            result["status"] = "ERR_NO_PRIMARY"
            result["err"] = f"{cfg['primary_feed']} empty"
            return result

        cross_data = {}
        for cid, feed_key in (cfg.get("cross_feeds") or {}).items():
            d = s3io.get_json(feed_key, default={})
            if d:
                cross_data[cid] = d

        kb_chunks = kb.lookup(cfg.get("kb_keywords") or [], max_chunks=2)

        system = BASE_SYSTEM_NAMES
        if cfg.get("system_addendum"):
            system = system + "\n\n" + cfg["system_addendum"]

        prompt = build_names_prompt(ctx_id, cfg, primary, cross_data, episode_ref, kb_chunks)
        prompt_len = len(prompt)

        brief, err = claude_json(prompt, system=system,
                                  max_tokens=cfg.get("max_tokens", 8000),
                                  temperature=cfg.get("temperature", 0.3),
                                  timeout=cfg.get("timeout", DEFAULT_TIMEOUT))
        if err or not brief:
            result["status"] = "ERR_CLAUDE"
            result["err"] = err or "no parseable JSON"
            result["prompt_len"] = prompt_len
            return result

        brief["version"] = "1.0"
        brief["brief_type"] = "names"
        brief["generated_at"] = datetime.now(timezone.utc).isoformat()
        brief["model"] = "claude-haiku-4-5-20251001"
        brief["context"] = ctx_id
        brief["title"] = cfg.get("title")
        brief["input_state"] = {
            "primary_feed": cfg["primary_feed"],
            "cross_feeds_loaded": list(cross_data.keys()),
            "top_n": cfg.get("top_n", 15),
            "prompt_len_chars": prompt_len,
        }
        output_key = f"data/{cfg['output_key']}.json"
        s3io.put_json(output_key, brief, cache_control="public, max-age=900")

        result.update({
            "status": "OK",
            "output_key": output_key,
            "n_names": len(brief.get("names") or []),
            "regime_note": brief.get("regime_note"),
            "duration_s": round(time.time() - t0, 2),
        })
    except Exception as e:
        result["status"] = "ERR_EXC"
        result["err"] = str(e)[:300]
        result["traceback"] = traceback.format_exc()[-800:]
    return result


# ─────────────────────────────────────────────────────────────────────
# SYNTHESIS brief — meta-summary that reads ALL existing briefs
# ─────────────────────────────────────────────────────────────────────
BASE_SYSTEM_SYNTHESIS = (
    "You are the CHIEF INVESTMENT OFFICER of a multi-strategy hedge fund. You consolidate "
    "the views of 23 specialist macro/regime desks plus 6 per-name analyst teams into ONE "
    "master decisive read. You think in terms of: (a) where the consensus is and how strong, "
    "(b) where credible dissent is and which desks disagree, (c) where 3+ desks agree on a "
    "specific asset direction (high-signal because the desks use different frameworks), (d) "
    "per-name tickers appearing as STRONG_FIT across multiple screener desks (very rare), "
    "(e) the loudest single tripwire on the platform right now, (f) the single best actionable "
    "trade today with the most multi-desk support. You are decisive. You DO NOT HEDGE. You name names."
)


def build_synthesis_prompt(ctx_id, cfg, regime_briefs, name_briefs):
    """Build the master synthesis prompt — feeds Claude a tight summary of all 30 briefs."""
    # Compact each regime brief to its essence
    regime_summary = []
    for k, b in regime_briefs.items():
        if not b: continue
        # Take just the headline + 1-2 key fields per brief
        item = {
            "desk": k.replace("-decisive-call", ""),
            "regime": b.get("regime"),
            "confidence": b.get("confidence"),
            "one_liner": (b.get("one_liner") or "")[:200],
        }
        # Pull each desk's BTC and SPX prediction directions for cross-asset voting
        for p in (b.get("historical_predictions") or []):
            t = p.get("ticker")
            if t in ("BTC", "SPX", "GLD", "DXY", "TLT", "HYG"):
                item[f"pred_{t}"] = {
                    "dir": p.get("prediction_direction"),
                    "range": f"{p.get('prediction_range_low_pct')}% to {p.get('prediction_range_high_pct')}%",
                    "wk": p.get("prediction_horizon_weeks"),
                    "prob": p.get("probability_pct"),
                }
        # Highest-severity tripwire from each desk
        for tw in (b.get("tripwires") or []):
            if (tw.get("severity") or "").upper() == "HIGH":
                item["high_tripwire"] = tw.get("condition")[:150]
                break
        regime_summary.append(item)

    # Compact each per-name brief — just top 3 STRONG_FIT picks
    name_summary = []
    for k, b in name_briefs.items():
        if not b: continue
        desk_name = k.replace("-names", "")
        strong_picks = []
        for n in (b.get("names") or []):
            if (n.get("regime_fit") or "").upper() == "STRONG_FIT":
                strong_picks.append({
                    "ticker": n.get("ticker"),
                    "rank": n.get("rank"),
                    "score": n.get("primary_score"),
                    "one_liner": (n.get("one_liner") or "")[:120],
                    "analog": (n.get("historical_analog") or {}).get("ticker"),
                })
            if len(strong_picks) >= 3:
                break
        if strong_picks:
            name_summary.append({"desk": desk_name, "top_strong_picks": strong_picks})

    prompt = f"""# {cfg["title"]}

## YOUR ROLE
{cfg["prompt_intro"]}

## INPUT — 23 REGIME DESKS

{json.dumps(regime_summary, indent=2, default=str)}

## INPUT — 6 PER-NAME DESKS (top STRONG_FIT picks only)

{json.dumps(name_summary, indent=2, default=str)}

## TASK

Synthesize the above into a master CIO-grade decisive read. Return EXACTLY this JSON:

{{
  "consensus": {{
    "regime": "<RISK_ON | RISK_ON_AGGRESSIVE | NEUTRAL | RISK_OFF | RISK_OFF_AGGRESSIVE>",
    "confidence": "<HIGH | MEDIUM | LOW>",
    "one_liner": "<one decisive sentence ≤140 chars>",
    "n_supporting_desks": <integer>,
    "supporting_desks": ["<desk1>", "<desk2>", ...],
    "thesis": "<3-4 sentences explaining the consensus read and what's driving it>"
  }},
  "dissent": [
    {{
      "desk": "<desk name>",
      "their_call": "<their regime + confidence>",
      "key_signal": "<the most divergent specific signal they're flagging, ≤180 chars>",
      "why_credible": "<why this dissent could be right — what would have to happen for it to resolve their way, ≤150 chars>"
    }}
    // 1-3 dissenters max
  ],
  "asymmetric_setups": [
    {{
      "asset": "<BTC | SPX | GLD | DXY | TLT | HYG | etc>",
      "n_bullish": <int>,
      "n_bearish": <int>,
      "n_sideways": <int>,
      "consensus_direction": "<STRONG_UPSIDE | MILDLY_UPSIDE | SIDEWAYS | MILDLY_DOWNSIDE | STRONG_DOWNSIDE | SPLIT>",
      "best_horizon_weeks": <int>,
      "loudest_bull": "<desk: brief signal, ≤140 chars>",
      "loudest_bear": "<desk: brief signal, ≤140 chars>",
      "trade_implication": "<actionable implication ≤150 chars>"
    }}
    // 3-6 assets
  ],
  "convergent_names": [
    {{
      "ticker": "<ticker>",
      "appearing_in": ["<desk1>", "<desk2>", ...],
      "strongest_score": <score>,
      "best_analog": "<analog ticker>",
      "synthesis_one_liner": "<≤120 chars why this name is multi-desk-confirmed>"
    }}
    // ONLY tickers appearing in 2+ desks' STRONG_FIT picks. May be empty.
  ],
  "loudest_tripwire": {{
    "severity": "<HIGH | MEDIUM>",
    "from_desk": "<desk>",
    "condition": "<the trigger>",
    "action": "<what to do if triggered>"
  }},
  "today_action": {{
    "primary_trade": "<the one trade with most multi-desk support, instrument + direction + thesis, ≤200 chars>",
    "primary_hedge": "<the matched hedge, ≤200 chars>",
    "position_sizing_note": "<sizing guidance grounded in consensus confidence, ≤150 chars>"
  }}
}}

## RULES
- Be decisive. NO 'wait and see'. NO 'monitor for'.
- Cite specific desks by name when supporting claims.
- For convergent_names: only include tickers that genuinely appear in 2+ desks' STRONG_FIT lists. If none, return empty array.
- asymmetric_setups: count desks by their predicted direction for each asset. STRONG = 3+ agreement. SPLIT = 2+ each way.
- Today's trade must be implementable in liquid instruments (ETFs, futures, options on major names)."""

    return prompt


def generate_synthesis_brief(ctx_id, cfg, episode_ref):
    t0 = time.time()
    result = {"context_id": ctx_id, "title": cfg.get("title"), "output_key": cfg.get("output_key"),
              "brief_type": "synthesis"}
    try:
        # Read all the regime briefs in parallel via thread pool
        regime_briefs = {}
        name_briefs = {}
        for src in (cfg.get("synthesis_sources") or []):
            d = s3io.get_json(src, default={})
            if d:
                regime_briefs[src.replace("data/", "").replace(".json", "")] = d
        for src in (cfg.get("synthesis_name_sources") or []):
            d = s3io.get_json(src, default={})
            if d:
                name_briefs[src.replace("data/", "").replace(".json", "")] = d

        if not regime_briefs:
            result["status"] = "ERR_NO_INPUTS"
            result["err"] = "no regime briefs loaded"
            return result

        prompt = build_synthesis_prompt(ctx_id, cfg, regime_briefs, name_briefs)
        prompt_len = len(prompt)

        system = BASE_SYSTEM_SYNTHESIS
        if cfg.get("system_addendum"):
            system = system + "\n\n" + cfg["system_addendum"]

        brief, err = claude_json(prompt, system=system,
                                  max_tokens=cfg.get("max_tokens", 7000),
                                  temperature=cfg.get("temperature", 0.3),
                                  timeout=cfg.get("timeout", DEFAULT_TIMEOUT))
        if err or not brief:
            result["status"] = "ERR_CLAUDE"
            result["err"] = err or "no parseable JSON"
            result["prompt_len"] = prompt_len
            return result

        brief["version"] = "1.0"
        brief["brief_type"] = "synthesis"
        brief["generated_at"] = datetime.now(timezone.utc).isoformat()
        brief["model"] = "claude-haiku-4-5-20251001"
        brief["context"] = ctx_id
        brief["title"] = cfg.get("title")
        brief["input_state"] = {
            "n_regime_briefs": len(regime_briefs),
            "n_name_briefs": len(name_briefs),
            "prompt_len_chars": prompt_len,
        }
        output_key = f"data/{cfg['output_key']}.json"
        s3io.put_json(output_key, brief, cache_control="public, max-age=900")

        result.update({
            "status": "OK",
            "output_key": output_key,
            "regime": (brief.get("consensus") or {}).get("regime"),
            "confidence": (brief.get("consensus") or {}).get("confidence"),
            "n_dissent": len(brief.get("dissent") or []),
            "n_asymmetric": len(brief.get("asymmetric_setups") or []),
            "n_convergent_names": len(brief.get("convergent_names") or []),
            "duration_s": round(time.time() - t0, 2),
        })
    except Exception as e:
        result["status"] = "ERR_EXC"
        result["err"] = str(e)[:300]
        result["traceback"] = traceback.format_exc()[-800:]
    return result


# ─────────────────────────────────────────────────────────────────────
# PORTFOLIO brief — personalized to a specific portfolio
# ─────────────────────────────────────────────────────────────────────
BASE_SYSTEM_PORTFOLIO = (
    "You are the PRIVATE PORTFOLIO MANAGER for one specific client. You see their actual "
    "positions, sector concentrations, vol, beta, VAR, correlation matrix. You give DIRECT, "
    "PERSONAL advice grounded in their book vs the current macro regime. You highlight "
    "positions out-of-regime, concentration risk, hedge opportunities, position-sizing "
    "adjustments. Every observation must reference a SPECIFIC holding or exposure. You do "
    "not generalize. You write as if speaking directly to the client."
)


def build_portfolio_prompt(ctx_id, cfg, risk_data, holdings_data, history_data, cross_data, consensus, kb_chunks):
    cross_lines = []
    for cid, d in (cross_data or {}).items():
        if not d: continue
        field = (cfg.get("cross_regime_fields") or {}).get(cid)
        v = d.get(field) if field else (d.get("regime") or d.get("severity") or d.get("composite_regime"))
        if v: cross_lines.append(f"  - {cid}: {v}")

    kb_block = ""
    if kb_chunks:
        kb_block = "\n\nRELEVANT FRAMEWORKS:\n" + "\n\n".join(
            f"### {c['framework']}\n{c['excerpt'][:500]}" for c in kb_chunks
        )

    consensus_block = ""
    if consensus:
        cs = (consensus.get("consensus") or {})
        consensus_block = (f"\n## DESK CONSENSUS (from the 23-desk synthesis)\n"
                           f"  regime: {cs.get('regime')}  confidence: {cs.get('confidence')}\n"
                           f"  thesis: {cs.get('one_liner') or cs.get('thesis')}\n")
        if consensus.get("loudest_tripwire"):
            t = consensus["loudest_tripwire"]
            consensus_block += f"  loudest tripwire: [{t.get('severity')}] {t.get('condition')}\n"

    prompt = f"""# {cfg["title"]}

## YOUR ROLE
{cfg["prompt_intro"]}

## PORTFOLIO RISK PROFILE
{json.dumps(risk_data, indent=2, default=str)[:3000]}

## CURRENT HOLDINGS
{json.dumps(holdings_data, indent=2, default=str)[:3500] if holdings_data else "(no holdings snapshot)"}

## RECENT PM-DECISION HISTORY
{json.dumps((history_data.get("snapshots") or [])[-5:] if isinstance(history_data, dict) else [], indent=2, default=str)[:2000]}

## CROSS-CONTEXT REGIMES
{chr(10).join(cross_lines) or "  (none loaded)"}
{consensus_block}
{kb_block}

## TASK

Return EXACTLY this JSON shape — a personalized portfolio memo:

{{
  "headline": "<single decisive sentence ≤140 chars about the book's current alignment>",
  "regime_fit": "<STRONG_FIT | NEUTRAL | POOR_FIT — is the current book well-aligned with the consensus regime>",
  "thesis": "<3-4 sentences synthesizing book vs regime>",
  "biggest_strength": "<the position or exposure that's most well-aligned right now, name specific holding>",
  "biggest_concern": "<the position or exposure most at risk right now, name specific holding>",
  "concentration_flags": [
    {{
      "type": "<SECTOR | SINGLE_NAME | FACTOR | CORRELATION>",
      "what": "<specific concentration, name positions>",
      "severity": "<HIGH | MEDIUM | LOW>",
      "action": "<concrete trim/hedge suggestion>"
    }}
  ],
  "out_of_regime_holdings": [
    {{"ticker": "<specific holding>", "why": "<why it's wrong for current regime, ≤120 chars>"}}
  ],
  "this_weeks_action": {{
    "primary_trade": "<the single most important trade this week — specific instrument, direction, sizing>",
    "rationale": "<≤180 chars why>",
    "if_can_only_do_one_thing": "<the minimum-viable version of this>"
  }},
  "tripwires_for_this_book": [
    {{"severity": "<HIGH | MEDIUM>", "condition": "<specific condition>", "action": "<what to do>"}}
  ]
}}

## RULES
- Name SPECIFIC holdings by ticker. Never say 'tech exposure' — say 'NVDA + AMD + MSFT'.
- Be direct. The client wants opinions, not options.
- If holdings data is missing, work with what's present and note the gap in your response.
- this_weeks_action.primary_trade must be implementable with a specific ticker + direction."""

    return prompt


def generate_portfolio_brief(ctx_id, cfg, episode_ref):
    t0 = time.time()
    result = {"context_id": ctx_id, "title": cfg.get("title"), "output_key": cfg.get("output_key"),
              "brief_type": "portfolio"}
    try:
        risk_data = s3io.get_json(cfg["primary_feed"], default={})
        secondary = cfg.get("secondary_feeds") or {}
        holdings_data = s3io.get_json(secondary.get("holdings", ""), default={}) if secondary.get("holdings") else {}
        history_data = s3io.get_json(secondary.get("pm_history", ""), default={}) if secondary.get("pm_history") else {}

        cross_data = {}
        consensus = None
        for cid, feed_key in (cfg.get("cross_feeds") or {}).items():
            d = s3io.get_json(feed_key, default={})
            if d:
                cross_data[cid] = d
                if cid == "consensus":
                    consensus = d

        kb_chunks = kb.lookup(cfg.get("kb_keywords") or [], max_chunks=2)

        system = BASE_SYSTEM_PORTFOLIO
        if cfg.get("system_addendum"):
            system = system + "\n\n" + cfg["system_addendum"]

        prompt = build_portfolio_prompt(ctx_id, cfg, risk_data, holdings_data, history_data,
                                          cross_data, consensus, kb_chunks)
        prompt_len = len(prompt)

        brief, err = claude_json(prompt, system=system,
                                  max_tokens=cfg.get("max_tokens", 6000),
                                  temperature=cfg.get("temperature", 0.3),
                                  timeout=cfg.get("timeout", DEFAULT_TIMEOUT))
        if err or not brief:
            result["status"] = "ERR_CLAUDE"
            result["err"] = err or "no parseable JSON"
            result["prompt_len"] = prompt_len
            return result

        brief["version"] = "1.0"
        brief["brief_type"] = "portfolio"
        brief["generated_at"] = datetime.now(timezone.utc).isoformat()
        brief["model"] = "claude-haiku-4-5-20251001"
        brief["context"] = ctx_id
        brief["title"] = cfg.get("title")
        brief["input_state"] = {
            "risk_loaded": bool(risk_data),
            "holdings_loaded": bool(holdings_data),
            "history_loaded": bool(history_data),
            "n_cross": len(cross_data),
            "prompt_len_chars": prompt_len,
        }
        output_key = f"data/{cfg['output_key']}.json"
        s3io.put_json(output_key, brief, cache_control="private, max-age=300")

        result.update({
            "status": "OK",
            "output_key": output_key,
            "regime_fit": brief.get("regime_fit"),
            "headline": brief.get("headline"),
            "n_flags": len(brief.get("concentration_flags") or []),
            "duration_s": round(time.time() - t0, 2),
        })
    except Exception as e:
        result["status"] = "ERR_EXC"
        result["err"] = str(e)[:300]
        result["traceback"] = traceback.format_exc()[-800:]
    return result


# ─────────────────────────────────────────────────────────────────────
# FRONT-RUN SNIFFER — Institutional flow anomaly detection
# ─────────────────────────────────────────────────────────────────────
BASE_SYSTEM_FRONTRUN = (
    "You are the HEAD OF MARKET MICROSTRUCTURE / DEALER FLOW INTELLIGENCE at an "
    "elite multi-strategy fund. Your specialty is detecting institutional FRONT-RUNNING "
    "— moments when dealers, market makers, primary dealers, hedge funds, insiders, or "
    "sovereign wealth funds are positioning ahead of a market-moving event the rest of "
    "the market hasn't priced. You hunt CONVERGENT anomalies (3+ flow categories pointing "
    "same direction same asset same window aligned with an upcoming catalyst). Single-feed "
    "anomalies are noise. Cross-category convergence is signal. You are paranoid by training "
    "and write with the urgency of someone reading institutional smoke signals."
)


# ═════════════════════════════════════════════════════════════════════
# TELEGRAM ALERTING — convergence-aware, anti-spam, daily-capped
# ═════════════════════════════════════════════════════════════════════
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")
ALERT_DAILY_CAP     = 8   # max alerts per sniffer per UTC day
ALERT_COOLDOWN_MIN  = 60  # min minutes between same-type alerts (regime-transition exempt)


def _telegram_post(text, parse_mode="Markdown"):
    """POST to Telegram. Returns (ok, info_dict). Silently no-op if no token."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False, {"err": "no_token_or_chat"}
    api = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text[:4096],
        "parse_mode": parse_mode,
        "disable_web_page_preview": True,
    }
    try:
        req = urllib.request.Request(
            api, data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=12) as r:
            return True, json.loads(r.read().decode())
    except Exception as e:
        # Retry without Markdown in case parsing fails
        try:
            payload["parse_mode"] = ""
            req = urllib.request.Request(
                api, data=json.dumps(payload).encode(),
                headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=12) as r:
                return True, json.loads(r.read().decode())
        except Exception as e2:
            return False, {"err": str(e2)[:200]}


def _alert_state_io(state_key, write=None):
    """Read or write the alert-state JSON for a sniffer."""
    full_key = f"data/_alerts/{state_key}.json"
    if write is not None:
        s3io.put_json(full_key, write, cache_control="no-store")
        return write
    return s3io.get_json(full_key, default={}) or {}


def _is_stressed_state(state, calm_values):
    """Return True if `state` is anything OTHER than one of the calm_values."""
    if not state: return False
    s = str(state).upper()
    return all(s != cv.upper() for cv in calm_values)


def _macro_convergence_fingerprint(brief):
    """Detect the AUCTION + PRIMARY_DEALER + FUNDING_PLUMBING co-stress fingerprint.
    Returns (fired, details_dict). This is the highest-conviction macro pattern
    (Aug 2007 / Sep 2019 / Mar 2020 / Mar 2023)."""
    pillars = brief.get("pillars") or {}
    auc = pillars.get("auction_tape") or {}
    pd  = pillars.get("primary_dealer_positioning") or {}
    fp  = pillars.get("funding_plumbing") or {}

    auc_state = (auc.get("state") or "").upper()
    pd_state  = (pd.get("state") or "").upper()
    fp_state  = (fp.get("state") or "").upper()

    # An "stressed" state for each pillar
    auc_stressed = auc_state in ("STRESSED", "CRISIS")
    pd_stressed  = pd_state  in ("DURATION_BID", "STRESSED", "DURATION_SHORT")
    fp_stressed  = fp_state  in ("TIGHT", "STRESS", "CRISIS")

    n_stressed = sum([auc_stressed, pd_stressed, fp_stressed])
    fired = n_stressed >= 3

    return fired, {
        "n_pillars_stressed": n_stressed,
        "auction_tape_state":  auc.get("state"),
        "auction_stressed":   auc_stressed,
        "primary_dealer_state":pd.get("state"),
        "pd_stressed":        pd_stressed,
        "funding_state":      fp.get("state"),
        "funding_stressed":   fp_stressed,
    }


def _format_equity_alert(brief, label, page_url, kind, prev_state):
    """Build the Telegram message for an equity sniffer alert."""
    score = brief.get("overall_anomaly_score")
    regime = (brief.get("anomaly_regime") or "NORMAL").upper()
    headline = brief.get("headline") or ""
    most_actionable = brief.get("most_actionable_setup") or ""
    top_setups = brief.get("suspected_setups") or []
    top = top_setups[0] if top_setups else {}
    la = brief.get("loudest_anomaly") or {}

    if kind == "REGIME_UP":
        title = f"🚨 *FRONT-RUN ALERT — REGIME ESCALATION*"
    elif kind == "REGIME_DOWN":
        title = f"✅ *FRONT-RUN — Regime de-escalated*"
    elif kind == "SCORE_JUMP":
        title = f"📈 *FRONT-RUN — Score jump*"
    elif kind == "EXTREME":
        title = f"🔥 *FRONT-RUN — EXTREME REGIME ACTIVE*"
    else:
        title = f"🎯 *FRONT-RUN ALERT*"

    prev_score = prev_state.get("last_score")
    prev_regime = prev_state.get("last_regime")
    delta_line = ""
    if prev_score is not None:
        delta = score - prev_score
        sign = "+" if delta >= 0 else ""
        delta_line = f"Score: *{prev_score} → {score}* ({sign}{delta})"
    else:
        delta_line = f"Score: *{score}/100*"
    if prev_regime and prev_regime != regime:
        delta_line += f"  ({prev_regime} → *{regime}*)"
    else:
        delta_line += f"  regime: *{regime}*"

    msg = f"{title}\n\n{delta_line}\n\n"
    if headline:
        msg += f"📰 {headline[:240]}\n\n"
    if top.get("target_asset"):
        msg += (f"🎯 Top setup: *{top.get('target_asset')}* "
                f"{top.get('target_direction','')} "
                f"@ {top.get('probability_pct','?')}% prob "
                f"({top.get('confidence','?')})\n")
    if la.get("signal"):
        msg += f"⚠ Loudest anomaly: {la.get('signal')[:180]}\n\n"
    if most_actionable:
        msg += f"⚡ Action: {most_actionable[:280]}\n\n"
    msg += f"→ {page_url}"
    return msg


def _format_macro_alert(brief, label, page_url, kind, prev_state, conv_fired, conv_detail):
    """Build the Telegram message for a macro sniffer alert."""
    score = brief.get("overall_macro_score")
    regime = (brief.get("macro_regime") or "NORMAL").upper()
    headline = brief.get("headline") or ""
    most_actionable = brief.get("most_actionable_macro_trade") or ""
    setups = brief.get("macro_setups") or []
    top = setups[0] if setups else {}
    ts = (top.get("trade_specifics") or {})
    la = brief.get("loudest_macro_anomaly") or {}

    if conv_fired:
        title = f"🚨🏛 *MACRO CONVERGENCE FINGERPRINT* — Aug 2007 / Sep 2019 / Mar 2020 / Mar 2023 signature"
    elif kind == "REGIME_UP":
        title = f"🚨 *MACRO FRONT-RUN — REGIME ESCALATION*"
    elif kind == "REGIME_DOWN":
        title = f"✅ *MACRO FRONT-RUN — Regime de-escalated*"
    elif kind == "SCORE_JUMP":
        title = f"📈 *MACRO FRONT-RUN — Score jump*"
    elif kind == "EXTREME":
        title = f"🔥 *MACRO FRONT-RUN — EXTREME REGIME ACTIVE*"
    else:
        title = f"🏛 *MACRO FRONT-RUN ALERT*"

    prev_score = prev_state.get("last_score")
    prev_regime = prev_state.get("last_regime")
    delta_line = ""
    if prev_score is not None:
        delta = score - prev_score
        sign = "+" if delta >= 0 else ""
        delta_line = f"Score: *{prev_score} → {score}* ({sign}{delta})"
    else:
        delta_line = f"Score: *{score}/100*"
    if prev_regime and prev_regime != regime:
        delta_line += f"  ({prev_regime} → *{regime}*)"
    else:
        delta_line += f"  regime: *{regime}*"

    msg = f"{title}\n\n{delta_line}\n\n"

    if conv_fired:
        msg += (f"⚠ *Convergence fingerprint*:\n"
                f"  • Auction tape: `{conv_detail.get('auction_tape_state')}`\n"
                f"  • Primary dealer: `{conv_detail.get('primary_dealer_state')}`\n"
                f"  • Funding plumbing: `{conv_detail.get('funding_state')}`\n"
                f"  → 3/3 pillars stressed simultaneously\n\n")

    if headline:
        msg += f"📰 {headline[:240]}\n\n"

    if top.get("setup_type") and ts.get("primary_instrument"):
        msg += (f"🎯 Top trade: *{ts.get('primary_instrument')}* "
                f"{ts.get('direction','')}\n"
                f"  entry: `{ts.get('entry_level','—')}`  "
                f"target: `{ts.get('target_level','—')}`  "
                f"stop: `{ts.get('stop_level','—')}`\n"
                f"  size: {ts.get('size_pct_of_portfolio','—')}  "
                f"horizon: {ts.get('horizon','—')}\n\n")

    if la.get("signal"):
        msg += f"⚠ Loudest pillar anomaly ({la.get('pillar','?')}): {la.get('signal','')[:180]}\n\n"
    if most_actionable:
        msg += f"⚡ Action: {most_actionable[:280]}\n\n"
    msg += f"→ {page_url}"
    return msg


def _decide_alert_kind(prev_state, new_score, new_regime, conv_fired_now, conv_was_recent):
    """Return (kind, reason) or (None, reason) if no alert should fire."""
    prev_regime = (prev_state.get("last_regime") or "").upper()
    prev_score  = prev_state.get("last_score")
    last_alert_at = prev_state.get("last_alert_at")
    alerts_today = prev_state.get("alerts_today", 0)
    alerts_today_date = prev_state.get("alerts_today_date", "")

    # Reset daily counter
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if alerts_today_date != today_str:
        alerts_today = 0

    if alerts_today >= ALERT_DAILY_CAP:
        return None, f"daily_cap_reached ({alerts_today})"

    # Cooldown check (but convergence-fingerprint and regime-transitions bypass)
    cooldown_ok = True
    if last_alert_at:
        try:
            la_dt = datetime.fromisoformat(last_alert_at.replace("Z","+00:00"))
            mins_since = (datetime.now(timezone.utc) - la_dt).total_seconds() / 60
            if mins_since < ALERT_COOLDOWN_MIN:
                cooldown_ok = False
        except Exception: pass

    # PRIORITY 1: Convergence fingerprint fires for first time in 4h
    if conv_fired_now and not conv_was_recent:
        return "CONVERGENCE", "convergence_first_in_4h"

    # PRIORITY 2: Regime transitions (bypass cooldown)
    if prev_regime and new_regime != prev_regime:
        order = {"NORMAL": 0, "ELEVATED": 1, "EXTREME": 2}
        po = order.get(prev_regime, 0)
        no = order.get(new_regime, 0)
        if no > po:
            return "REGIME_UP", f"{prev_regime}->{new_regime}"
        elif no < po:
            return "REGIME_DOWN", f"{prev_regime}->{new_regime}"

    # PRIORITY 3: EXTREME persisting (every cooldown window)
    if new_regime == "EXTREME" and cooldown_ok:
        return "EXTREME", "extreme_persisting"

    # PRIORITY 4: Score jump (>= 15 points in either direction) — respect cooldown
    if prev_score is not None and cooldown_ok:
        delta = abs(new_score - prev_score)
        if delta >= 15:
            return "SCORE_JUMP", f"delta_{delta}"

    return None, ("cooldown" if not cooldown_ok else "no_threshold_met")


def _maybe_alert_equity_sniffer(brief, page_url="https://justhodl.ai/frontrun.html"):
    """Check the equity sniffer state and fire a Telegram alert if needed."""
    try:
        state_key = "frontrun-sniffer-alert-state"
        prev = _alert_state_io(state_key) or {}

        score = brief.get("overall_anomaly_score")
        regime = (brief.get("anomaly_regime") or "NORMAL").upper()
        if score is None: return {"ok": False, "reason": "no_score"}

        kind, reason = _decide_alert_kind(prev, score, regime, False, False)
        if kind is None:
            return {"ok": False, "reason": reason, "score": score, "regime": regime}

        msg = _format_equity_alert(brief, "Front-Run Sniffer", page_url, kind, prev)
        ok, info = _telegram_post(msg)

        # Update state
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if prev.get("alerts_today_date") != today_str:
            prev["alerts_today"] = 0
        new_state = {
            "last_regime": regime,
            "last_score":  score,
            "last_alert_at": datetime.now(timezone.utc).isoformat(),
            "last_alert_kind": kind,
            "last_alert_reason": reason,
            "alerts_today": (prev.get("alerts_today", 0) + 1),
            "alerts_today_date": today_str,
        }
        _alert_state_io(state_key, write=new_state)
        return {"ok": ok, "kind": kind, "reason": reason, "telegram": info}
    except Exception as e:
        return {"ok": False, "err": str(e)[:300]}


def _maybe_alert_macro_sniffer(brief, page_url="https://justhodl.ai/macro-frontrun.html"):
    """Check the macro sniffer state and fire a Telegram alert if needed,
    with the convergence-fingerprint priority detection."""
    try:
        state_key = "macro-frontrun-sniffer-alert-state"
        prev = _alert_state_io(state_key) or {}

        score = brief.get("overall_macro_score")
        regime = (brief.get("macro_regime") or "NORMAL").upper()
        if score is None: return {"ok": False, "reason": "no_score"}

        # Convergence fingerprint detection
        conv_fired, conv_detail = _macro_convergence_fingerprint(brief)
        conv_was_recent = False
        last_conv_at = prev.get("last_convergence_fingerprint_at")
        if last_conv_at:
            try:
                lc = datetime.fromisoformat(last_conv_at.replace("Z","+00:00"))
                hrs_since = (datetime.now(timezone.utc) - lc).total_seconds() / 3600
                conv_was_recent = hrs_since < 4
            except Exception: pass

        kind, reason = _decide_alert_kind(prev, score, regime, conv_fired, conv_was_recent)
        if kind is None:
            return {"ok": False, "reason": reason, "score": score, "regime": regime,
                    "conv_fired": conv_fired, "conv_detail": conv_detail}

        msg = _format_macro_alert(brief, "Macro Front-Run Sniffer", page_url, kind, prev, conv_fired, conv_detail)
        ok, info = _telegram_post(msg)

        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if prev.get("alerts_today_date") != today_str:
            prev["alerts_today"] = 0
        new_state = {
            "last_regime": regime,
            "last_score":  score,
            "last_alert_at": datetime.now(timezone.utc).isoformat(),
            "last_alert_kind": kind,
            "last_alert_reason": reason,
            "alerts_today": (prev.get("alerts_today", 0) + 1),
            "alerts_today_date": today_str,
            "last_convergence_fingerprint_at": (
                datetime.now(timezone.utc).isoformat() if conv_fired
                else prev.get("last_convergence_fingerprint_at")
            ),
            "last_convergence_detail": conv_detail if conv_fired else prev.get("last_convergence_detail"),
        }
        _alert_state_io(state_key, write=new_state)
        return {"ok": ok, "kind": kind, "reason": reason,
                "conv_fired": conv_fired, "telegram": info}
    except Exception as e:
        return {"ok": False, "err": str(e)[:300]}


def _compact_feed(d, cap=900):
    if not isinstance(d, dict):
        return d
    # High-signal field names common across all flow + macro/rates feeds
    keep_keys = {
        # Common scoring / regime
        "regime", "score", "composite_score", "anomaly_score", "anomaly_regime",
        "signal", "signal_strength", "signal_type", "signal_types", "flag",
        "state", "severity", "summary", "headline", "one_liner",
        "top_setups", "top_5", "top", "top_10", "clusters", "alerts",
        "warning", "warnings", "rationale", "as_of", "current_readings",
        "key_levels", "level", "threshold", "tripwire", "tripwires",

        # Equity microstructure
        "n_buyers", "n_funds_holding", "n_insiders", "legend_buyers",
        "gex", "gamma", "skew", "iv", "put_call_ratio", "directional_bias",
        "n_signals", "primary_signal",

        # ── RATES / AUCTION / MACRO SIGNAL FIELDS ──
        # Auction tape
        "btc_ratio", "indirect", "indirect_pct", "aah", "dealer_take",
        "tail_bps", "tail", "stop_yield", "high_yield", "issue_size",
        "auction_date", "recent_auctions", "upcoming_auctions",
        "crisis_pattern_indicators", "zero_rate_floor", "btc_extreme",
        "indirect_drought", "aah_dependency", "tail_blowout", "ratio_inversion",
        # Primary dealer survey
        "dealer_positions", "n_dealers", "median_forecast", "dispersion",
        "duration_bias", "credit_bias",
        # Funding plumbing
        "fra_ois", "sofr_iorb", "swap_lines", "btfp_balance", "discount_window",
        "repo_rate", "ted_spread", "ois_spread", "funding_severity",
        # MOVE / rates vol
        "move_index", "move_vix_ratio", "move_pctile",
        # Net liquidity
        "walcl", "tga", "rrp", "net_liquidity", "delta_net_liquidity",
        "soma", "m2", "fed_balance_sheet",
        # TIC flows
        "tic_net_flow", "foreign_holdings", "central_bank_holdings",
        "n_buyers_sovereign", "country_top_buyers",
        # Macro nowcast
        "gdpnow", "lei", "cli", "ism", "sahm", "nfp_nowcast",
        "cycle_phase", "expansion_probability",
        # Fed-speak NLP
        "hawk_dove_score", "fed_lean", "n_speeches",
        "recent_speakers", "tone_delta", "tone_change_7d",
        # Catalyst calendar
        "upcoming_fomc", "upcoming_nfp", "upcoming_cpi", "upcoming_refunding",
        "n_events_next_week", "events", "near_term_events",
        # Yield curve
        "twos_tens", "tens_thirties", "real_yield", "breakeven",
        "term_premium", "curve_velocity"
    }
    out = {}
    for k, v in d.items():
        if k in keep_keys:
            out[k] = v
        elif isinstance(v, (int, float, bool)):
            out[k] = v
        elif isinstance(v, str) and len(v) < 200:
            # Keep short strings (often regime/state labels)
            out[k] = v
    s = json.dumps(out, default=str)
    if len(s) > cap:
        s = s[:cap] + "...}"
        try: out = json.loads(s)
        except Exception: out = {"_truncated": s[:cap]}
    return out


def build_frontrun_prompt(ctx_id, cfg, feeds, kb_chunks):
    """Build the front-run sniffer prompt — feed Claude every flow signal we have.
    Covers BOTH equity microstructure AND rates/macro/auction front-running."""
    # 9 categories — rates/macro categories listed FIRST (most front-run leading-indicator value)
    categorized = {
        # ── RATES / MACRO FRONT-RUN CATEGORIES (often LEAD equity moves) ──
        "AUCTION TAPE + PRIMARY DEALER + SOVEREIGN FLOWS": [
            "auction_crisis", "dealer_survey", "tic_flows"
        ],
        "FUNDING PLUMBING / RATES VOL (PRE-STRESS SIGNAL)": [
            "bond_vol", "eurodollar_stress", "crisis_plumbing"
        ],
        "NET LIQUIDITY + FED PATH (WALCL/TGA/RRP + FED-SPEAK NLP)": [
            "liquidity_data", "liquidity_flow", "macro_nowcast", "fed_speak", "fed_nlp"
        ],

        # ── EQUITY MICROSTRUCTURE CATEGORIES ──
        "WHALE / SMART MONEY (13F, CFTC, CAPITULATION)": [
            "13f_divergence", "13f_positions", "smart_money_clusters",
            "consensus_bottom", "forced_selling", "cftc_deep"
        ],
        "DEALER / MM EQUITY (GEX, OPTIONS FLOW + GAMMA)": [
            "dealer_gex", "options_flow", "options_gamma"
        ],
        "VOL / SKEW (PROTECTION DEMAND + SQUEEZE)": [
            "skew_tail", "catalyst_skew", "earnings_iv_crush",
            "squeeze_pretrigger", "short_interest"
        ],
        "INSIDER / ACTIVIST": ["insider_clusters", "activist_13d"],
        "CROSS-EXCHANGE FLOWS (CRYPTO / ETF / TIC)": [
            "etf_flows", "exchange_flows", "stablecoin_flow"
        ],
        "SENTIMENT (CONTRA INDICATOR)": ["aaii_sentiment", "retail_sentiment"],

        # ── SYNTHESIS + MACRO CONTEXT ──
        "CATALYST CALENDAR (WHAT THEY'RE FRONT-RUNNING)": ["catalyst_calendar"],
        "CROSS-ASSET DIVERGENCE + DESK CONSENSUS + BONDS DESK": [
            "divergence", "desk_consensus", "bonds_decisive"
        ],
        "MACRO CONTEXT (VIX/RATES/CREDIT)": ["vix", "yield_curve", "credit"]
    }

    blocks = []
    feeds_loaded = 0
    feeds_missing = 0
    for category, feed_ids in categorized.items():
        category_blocks = []
        for fid in feed_ids:
            d = feeds.get(fid)
            if not d:
                feeds_missing += 1
                continue
            feeds_loaded += 1
            compact = _compact_feed(d, cap=900)
            category_blocks.append(f"  ## {fid}\n  {json.dumps(compact, default=str)}")
        if category_blocks:
            blocks.append(f"### {category}\n" + "\n\n".join(category_blocks))

    feed_block = "\n\n".join(blocks) or "(no flow feeds loaded)"

    kb_block = ""
    if kb_chunks:
        kb_block = "\n\nRELEVANT FRAMEWORKS:\n" + "\n\n".join(
            f"### {c['framework']}\n{c['excerpt'][:600]}" for c in kb_chunks
        )

    prompt = f"""# {cfg["title"]}

## YOUR ROLE
{cfg["prompt_intro"]}

## CURRENT FLOW + RATES + MACRO DATA — {feeds_loaded} feeds loaded ({feeds_missing} unavailable)

{feed_block[:26000]}
{kb_block}

## TASK

Return EXACTLY this JSON. Detect CONVERGENT anomalies (3+ flow/macro categories pointing same asset same direction). Single-feed anomalies are noise. Cross-category convergence is signal.

CRITICAL: Pay special attention to RATES/MACRO front-runs because they often LEAD equity moves. If you see primary-dealer positioning + auction-tape stress + funding-plumbing widening together, that's the highest-conviction macro fingerprint — flag it explicitly.

{{
  "overall_anomaly_score": <0-100, "how loud is the institutional positioning right now">,
  "anomaly_regime": "<NORMAL | ELEVATED | EXTREME>",
  "headline": "<single decisive sentence ≤140 chars naming THE most coordinated front-run setup right now>",
  "thesis": "<3-4 sentences on the dominant convergent pattern across categories>",

  "suspected_setups": [
    {{
      "rank": 1,
      "confidence": "<HIGH | MEDIUM | LOW>",
      "target_asset": "<specific instrument — equity ticker, sector ETF, bond ETF (TLT/IEF/HYG/LQD/TIP), currency, commodity, or rate>",
      "target_direction": "<UPSIDE | DOWNSIDE | SIDEWAYS_SQUEEZE | STEEPENER | FLATTENER | BREAKEVEN_BID>",
      "magnitude_pct": "<e.g. '-8% to -15%' OR '+15 to +30 bps' for rates>",
      "horizon": "<e.g. '4-8 weeks'>",
      "probability_pct": <integer>,

      "who_is_positioning": "<primary dealers / hedge funds / sovereign wealth / central banks / commercials / insiders / dealers MM — name the player class>",

      "smoking_gun_signals": [
        {{"category": "<DEALER_GEX | CFTC | SKEW | OPTIONS_FLOW | 13F | INSIDER | AUCTION | DEALER_SURVEY | TIC | FUNDING_PLUMBING | BOND_VOL | NET_LIQUIDITY | FED_SPEAK | etc>",
         "feed": "<the feed id this came from>",
         "signal": "<specific signal with the number, ≤150 chars>",
         "anomaly_pctile": <0-100, how unusual is this value>}}
        // 3-5 convergent signals minimum, from DIFFERENT categories
      ],

      "catalyst_being_front_run": "<the catalyst this positioning is pricing — earnings, FOMC, refunding announcement, NFP, CPI, OPEX, geopolitical, etc>",
      "catalyst_date": "<date or window>",

      "historical_analog": {{
        "period": "<specific period e.g. Aug 2007 jumbo auction tail OR Mar 2023 SVB BTFP launch>",
        "what_happened": "<concrete outcome ≤30 words>",
        "similarity_pct": <integer>
      }},

      "ride_this_flow": "<specific actionable trade to position alongside the smart money, with instrument + direction + sizing>",
      "fade_this_flow": "<specific reversal trade if it's a head-fake, with kill criteria>",
      "invalidation_tripwire": "<specific data condition that disproves the hypothesis>"
    }}
    // 2-5 setups, ranked by confidence × convergence × asymmetry
  ],

  "whale_alerts": [
    {{"asset": "<ticker/asset>",
     "channel": "<13F | OPTIONS | CFTC | DARK_POOL | ONCHAIN | TIC | AUCTION | CENTRAL_BANK | PRIMARY_DEALER | SOVEREIGN>",
     "actor": "<player type>", "size": "<dollar or % anomaly>", "direction": "<long/short>",
     "implication": "<what this means ≤120 chars>"}}
    // 2-5 alerts. Include AT LEAST ONE rates/macro channel (TIC/AUCTION/CENTRAL_BANK/PRIMARY_DEALER) if signal exists.
  ],

  "dealer_hedging_flows": [
    {{"flow_type": "<GEX_FLIP | PUT_WALL | SHORT_GAMMA | VOL_BID | AUCTION_ABSORPTION | TGA_BUILD | DURATION_BID | FUNDING_STRESS | CURVE_STEEPENER | CURVE_FLATTENER | BREAKEVEN_SPIKE | RRP_DRAIN | etc>",
     "asset": "<ticker/rate>", "signal": "<specific>",
     "implication": "<what dealers/primary dealers are positioning for>"}}
    // 1-4 dealer flows. Mix EQUITY (GEX/PUT_WALL/SHORT_GAMMA) AND RATES (AUCTION_ABSORPTION/DURATION_BID/FUNDING_STRESS/CURVE_*) signals.
  ],

  "insider_capitulation_alerts": [
    {{"ticker": "<ticker>", "pattern": "<CEO_CFO_CLUSTER | EXEC_SELL_WAVE | CLUSTER_AT_LOWS | etc>",
     "implication": "<what insiders see ≤120 chars>"}}
    // 0-5 alerts; empty array if no insider-specific signal
  ],

  "loudest_anomaly": {{
    "signal": "<the single most-extreme single-feed datapoint right now — equity OR rates>",
    "from_feed": "<feed id>",
    "value": "<the number>",
    "anomaly_pctile": <integer>,
    "interpretation": "<≤150 chars what it means>"
  }},

  "most_actionable_setup": "<which of the suspected_setups above is the single best risk-adjusted action right now, ≤200 chars>"
}}

## RULES
- Be specific. Name the feed each signal came from. Quote the actual number.
- Convergence requirement: every suspected_setup MUST cite 3+ smoking_gun_signals from DIFFERENT categories.
- WHEN RATES + MACRO + AUCTION signals converge, flag that explicitly — it's the highest-signal pattern.
- If no convergent setup exists, suspected_setups can be empty array — DO NOT FABRICATE.
- Historical analog must be a REAL past episode:
  * EQUITY: Aug 2007 quant melt, Dec 2018 dealer gamma, Mar 2020 cascade, Feb 2018 vol, Jan 2021 GME, Q3 2023 SMCI, Nov 2021 crypto top.
  * RATES/MACRO: 1979 Volcker, 1994 bond massacre, 1998 LTCM, 2007 jumbo auction tail, Sep 2019 repo, Mar 2020 Treasury basis unwind, Sep 2022 UK gilt LDI, Mar 2023 SVB BTFP, 2013 taper tantrum, BOJ 2022 JGB intervention, Druckenmiller 1992 pound short.
- ride_this_flow and fade_this_flow must name SPECIFIC instruments + entry levels.
- Invalidation tripwire must be a specific data condition that would prove you wrong."""

    return prompt


def generate_frontrun_brief(ctx_id, cfg, episode_ref):
    t0 = time.time()
    result = {"context_id": ctx_id, "title": cfg.get("title"),
              "output_key": cfg.get("output_key"), "brief_type": "frontrun"}
    try:
        # Read all flow feeds in parallel via individual S3 reads
        flow_sources = cfg.get("flow_sources") or {}
        feeds = {}
        for fid, feed_key in flow_sources.items():
            d = s3io.get_json(feed_key, default={})
            if d:
                feeds[fid] = d

        if not feeds:
            result["status"] = "ERR_NO_INPUTS"
            result["err"] = "no flow feeds loaded"
            return result

        kb_chunks = kb.lookup(cfg.get("kb_keywords") or [], max_chunks=2)

        system = BASE_SYSTEM_FRONTRUN
        if cfg.get("system_addendum"):
            system = system + "\n\n" + cfg["system_addendum"]

        prompt = build_frontrun_prompt(ctx_id, cfg, feeds, kb_chunks)
        prompt_len = len(prompt)

        brief, err = claude_json(prompt, system=system,
                                  max_tokens=cfg.get("max_tokens", 8000),
                                  temperature=cfg.get("temperature", 0.3),
                                  timeout=cfg.get("timeout", DEFAULT_TIMEOUT))
        if err or not brief:
            result["status"] = "ERR_CLAUDE"
            result["err"] = err or "no parseable JSON"
            result["prompt_len"] = prompt_len
            return result

        brief["version"] = "1.0"
        brief["brief_type"] = "frontrun"
        brief["generated_at"] = datetime.now(timezone.utc).isoformat()
        brief["model"] = "claude-haiku-4-5-20251001"
        brief["context"] = ctx_id
        brief["title"] = cfg.get("title")
        brief["input_state"] = {
            "n_feeds_attempted": len(flow_sources),
            "n_feeds_loaded": len(feeds),
            "loaded": sorted(feeds.keys()),
            "missing": sorted(set(flow_sources.keys()) - set(feeds.keys())),
            "prompt_len_chars": prompt_len,
        }
        output_key = f"data/{cfg['output_key']}.json"
        s3io.put_json(output_key, brief, cache_control="public, max-age=900")

        # ────────────────────────────────────────────────────────────
        # Append a compact snapshot to the 7-day history file
        # ────────────────────────────────────────────────────────────
        try:
            history_key = f"data/{cfg['output_key']}-history.json"
            history = s3io.get_json(history_key, default={}) or {}
            snaps = history.get("snapshots") if isinstance(history, dict) else None
            if not isinstance(snaps, list):
                snaps = []

            # Top setup compaction
            top_setups = brief.get("suspected_setups") or []
            top = top_setups[0] if top_setups else {}
            la = brief.get("loudest_anomaly") or {}

            snap = {
                "ts": brief["generated_at"],
                "score": brief.get("overall_anomaly_score"),
                "regime": brief.get("anomaly_regime"),
                "headline": (brief.get("headline") or "")[:200],
                "n_setups": len(top_setups),
                "n_whales": len(brief.get("whale_alerts") or []),
                "n_dealers": len(brief.get("dealer_hedging_flows") or []),
                "n_insiders": len(brief.get("insider_capitulation_alerts") or []),
                "top_setup_asset": top.get("target_asset"),
                "top_setup_dir": top.get("target_direction"),
                "top_setup_conf": top.get("confidence"),
                "top_setup_prob": top.get("probability_pct"),
                "top_setup_catalyst": (top.get("catalyst_being_front_run") or "")[:150],
                "loudest_signal": (la.get("signal") or "")[:200],
                "loudest_feed": la.get("from_feed"),
                "loudest_pctile": la.get("anomaly_pctile"),
                "most_actionable": (brief.get("most_actionable_setup") or "")[:200],
                "feeds_loaded": len(feeds),
            }
            snaps.append(snap)
            # 7-day cap. 168 = hourly × 7 days. 42 = 4h × 7 days. Keep up to 200 for safety.
            snaps = snaps[-200:]

            # ────────────────────────────────────────────────────────
            # Compute 7-day stats (numeric ones only — skip None)
            # ────────────────────────────────────────────────────────
            now = datetime.now(timezone.utc)
            week_ago = now - timedelta(days=7)
            recent = []
            for s in snaps:
                try:
                    t = datetime.fromisoformat((s.get("ts") or "").replace("Z", "+00:00"))
                    if t >= week_ago:
                        recent.append(s)
                except Exception:
                    pass

            scores_7d = [s["score"] for s in recent if isinstance(s.get("score"), (int, float))]
            stats_7d = {
                "n_snapshots_7d": len(recent),
                "score_mean": round(sum(scores_7d) / len(scores_7d), 1) if scores_7d else None,
                "score_min":  min(scores_7d) if scores_7d else None,
                "score_max":  max(scores_7d) if scores_7d else None,
                "score_latest": snap.get("score"),
                "score_delta_vs_mean_7d": (round(snap["score"] - sum(scores_7d) / len(scores_7d), 1)
                                            if (scores_7d and isinstance(snap.get("score"), (int, float))) else None),
                "n_extreme_7d":  sum(1 for s in recent if (s.get("regime") or "").upper() == "EXTREME"),
                "n_elevated_7d": sum(1 for s in recent if (s.get("regime") or "").upper() == "ELEVATED"),
                "n_normal_7d":   sum(1 for s in recent if (s.get("regime") or "").upper() == "NORMAL"),
                "max_setups_7d": max((s.get("n_setups") or 0) for s in recent) if recent else 0,
            }

            # Most common front-run target asset in last 7 days
            from collections import Counter
            tgt_counter = Counter([s.get("top_setup_asset") for s in recent if s.get("top_setup_asset")])
            stats_7d["most_targeted_assets"] = [
                {"asset": k, "n_times": v} for k, v in tgt_counter.most_common(5)
            ]

            # ────────────────────────────────────────────────────────
            # Extract HIGH-signal events (score >= 60 OR regime EXTREME)
            # ────────────────────────────────────────────────────────
            events = []
            for s in snaps[::-1]:  # newest first
                if ((s.get("score") or 0) >= 60) or ((s.get("regime") or "").upper() == "EXTREME"):
                    events.append({
                        "ts": s.get("ts"),
                        "score": s.get("score"),
                        "regime": s.get("regime"),
                        "headline": s.get("headline"),
                        "top_setup_asset": s.get("top_setup_asset"),
                        "top_setup_dir": s.get("top_setup_dir"),
                        "n_setups": s.get("n_setups"),
                    })
                if len(events) >= 12:
                    break

            history_out = {
                "version": "1.0",
                "generated_at": brief["generated_at"],
                "snapshots": snaps,
                "stats_7d": stats_7d,
                "events": events,
            }
            s3io.put_json(history_key, history_out, cache_control="public, max-age=300")
        except Exception as _h_e:
            # Don't fail the main brief generation on history errors
            print(f"[frontrun] history append failed: {_h_e}")

        # ────────────────────────────────────────────────────────────
        # Telegram alerting — anti-spam, daily-capped, regime-aware
        # ────────────────────────────────────────────────────────────
        try:
            alert_result = _maybe_alert_equity_sniffer(brief)
            print(f"[frontrun] alert: {alert_result}")
        except Exception as _a_e:
            print(f"[frontrun] alert failed: {_a_e}")

        result.update({
            "status": "OK",
            "output_key": output_key,
            "anomaly_score": brief.get("overall_anomaly_score"),
            "anomaly_regime": brief.get("anomaly_regime"),
            "n_setups": len(brief.get("suspected_setups") or []),
            "n_whale_alerts": len(brief.get("whale_alerts") or []),
            "n_dealer_flows": len(brief.get("dealer_hedging_flows") or []),
            "n_insider_alerts": len(brief.get("insider_capitulation_alerts") or []),
            "duration_s": round(time.time() - t0, 2),
        })
    except Exception as e:
        result["status"] = "ERR_EXC"
        result["err"] = str(e)[:300]
        result["traceback"] = traceback.format_exc()[-800:]
    return result


# ─────────────────────────────────────────────────────────────────────
# MACRO FRONT-RUN BRIEF — Rates / Auctions / Funding / Bonds deep dive
# ─────────────────────────────────────────────────────────────────────
BASE_SYSTEM_MACRO_FRONTRUN = (
    "You are a SENIOR MACRO STRATEGIST in the Stanley Druckenmiller / Ray Dalio / "
    "Bill Gross / Jeff Gundlach tradition. Your specialty is rates + auctions + "
    "Treasury + funding markets — NOT equity microstructure. You think in DV01, "
    "curve shape, breakevens, funding levels, Fed-path probability, and sovereign "
    "flow. You detect institutional FRONT-RUNNING in the macro complex: primary "
    "dealers loading duration before Fed pivots, sovereigns rotating TIC flows "
    "before currency moves, funding plumbing stress brewing before equity vol spikes, "
    "auction tape dealer-absorption fingerprints. You are decisive. You generate "
    "SPECIFIC rates trades (TLT/IEF/AGG/TIP/HYG/LQD/GLD/UUP, Treasury futures, "
    "curve spreads, breakeven trades) with entry/target/stop and DV01-aware sizing."
)


# Macro pillars — each pillar groups feeds for the prompt structure
MACRO_PILLAR_GROUPS = {
    "auction_tape":              "🏛 Auction Tape (BTC ratio, indirect %, AAH, tail bps)",
    "primary_dealer_survey":     "🤝 NY Fed Primary Dealer Survey",
    "tic_flows":                 "🌐 TIC Flows (sovereign Treasury holdings)",
    "bond_vol_move":             "📈 Bond Vol / MOVE Index",
    "eurodollar_stress":         "💸 Eurodollar / Funding Stress",
    "crisis_plumbing":           "🔧 Crisis Plumbing (repo / FRA-OIS / BTFP / swap lines)",
    "liquidity_data":            "💧 Net Liquidity (WALCL / TGA / RRP)",
    "liquidity_flow":            "💧 Liquidity Flow rate-of-change",
    "macro_nowcast":             "📊 Macro Nowcast (GDPNow / LEI / Sahm)",
    "fed_speak":                 "🎤 Fed-Speak (recent speeches)",
    "fed_nlp":                   "🎤 Fed-NLP (hawk-dove score)",
    "yield_curve":               "📉 Yield Curve (slope + velocity)",
    "bonds_desk_brief":          "📓 Duration Desk Decisive Call",
    "eurodollar_brief":          "📓 Eurodollar Desk Brief",
    "auction_brief":             "📓 Auction Desk Brief",
    "crisis_brief":              "📓 Crisis Desk Brief",
    "yield_curve_brief":         "📓 Yield Curve Desk Brief",
    "lce_brief":                 "📓 LCE Desk Brief",
    "catalyst_calendar":         "📅 Catalyst Calendar (FOMC / NFP / CPI / Refunding)",
    "cftc_deep":                 "📊 CFTC Rate-Futures Positioning"
}


def build_macro_frontrun_prompt(ctx_id, cfg, feeds, kb_chunks):
    """Build the macro front-run prompt with pillar-organized feed data."""
    blocks = []
    feeds_loaded = 0
    feeds_missing_list = []
    for pillar_id, pillar_label in MACRO_PILLAR_GROUPS.items():
        d = feeds.get(pillar_id)
        if not d:
            feeds_missing_list.append(pillar_id)
            continue
        feeds_loaded += 1
        compact = _compact_feed(d, cap=1000)
        blocks.append(f"### {pillar_label}\n  ## {pillar_id}\n  {json.dumps(compact, default=str)}")

    feed_block = "\n\n".join(blocks) or "(no macro feeds loaded)"

    kb_block = ""
    if kb_chunks:
        kb_block = "\n\nRELEVANT FRAMEWORKS:\n" + "\n\n".join(
            f"### {c['framework']}\n{c['excerpt'][:600]}" for c in kb_chunks
        )

    prompt = f"""# {cfg["title"]}

## YOUR ROLE
{cfg["prompt_intro"]}

## CURRENT MACRO + RATES + AUCTION + FUNDING DATA — {feeds_loaded} pillars loaded

{feed_block[:26000]}
{kb_block}

## TASK

Return EXACTLY this JSON. This is a PURE MACRO front-run read — equity microstructure is NOT in scope. You're scanning rates, auctions, funding, bonds, Fed path, sovereign flows for convergent institutional positioning.

{{
  "overall_macro_score": <0-100, "how loud is the macro/rates institutional positioning right now">,
  "macro_regime": "<NORMAL | ELEVATED | EXTREME>",
  "headline": "<single decisive sentence ≤140 chars naming THE most coordinated macro front-run setup right now>",
  "thesis": "<3-4 sentences on the dominant cross-pillar pattern. Cite specific numbers from pillar feeds.>",

  "pillars": {{
    "auction_tape": {{
      "state": "<CALM | STRESSED | CRISIS>",
      "anomaly_pctile": <0-100>,
      "key_signal": "<the loudest auction-tape datapoint, ≤140 chars>",
      "what_it_means": "<1 sentence interpretation>"
    }},
    "primary_dealer_positioning": {{
      "state": "<NEUTRAL | DURATION_BID | DURATION_SHORT | STRESSED>",
      "anomaly_pctile": <0-100>,
      "key_signal": "<NYFed survey / dealer behavior signal>",
      "what_it_means": "<1 sentence>"
    }},
    "funding_plumbing": {{
      "state": "<HEALED | TIGHT | STRESS | CRISIS>",
      "anomaly_pctile": <0-100>,
      "key_signal": "<FRA-OIS / SOFR-IORB / BTFP / repo signal>",
      "what_it_means": "<1 sentence>"
    }},
    "net_liquidity": {{
      "state": "<EXPANDING | NEUTRAL | CONTRACTING>",
      "delta_direction": "<UP | FLAT | DOWN>",
      "key_signal": "<WALCL-TGA-RRP rate-of-change>",
      "what_it_means": "<1 sentence on risk-asset implication>"
    }},
    "fed_path": {{
      "state": "<DOVISH_TILT | NEUTRAL | HAWKISH_TILT>",
      "hawk_dove_score": <number or null>,
      "key_signal": "<Fed-speak NLP shift / recent dovish-hawkish flip>",
      "what_it_means": "<1 sentence>"
    }},
    "yield_curve_velocity": {{
      "shape": "<NORMAL | FLATTENING | INVERTED | STEEPENING | BULL_STEEPENER | BEAR_FLATTENER>",
      "key_signal": "<2s10s / 3m10s / real yield / breakeven>",
      "what_it_means": "<1 sentence>"
    }},
    "tic_flows": {{
      "state": "<INFLOW | NEUTRAL | OUTFLOW>",
      "key_signal": "<foreign holding shift>",
      "what_it_means": "<1 sentence sovereign-positioning implication>"
    }}
  }},

  "macro_setups": [
    {{
      "rank": 1,
      "confidence": "<HIGH | MEDIUM | LOW>",
      "setup_type": "<DURATION_LONG | DURATION_SHORT | CURVE_STEEPENER | CURVE_FLATTENER | BREAKEVEN_LONG | BREAKEVEN_SHORT | FUNDING_SHORT | AUCTION_FRONTRUN | FED_PIVOT_LONG | DOLLAR_LONG | DOLLAR_SHORT | TIPS_LONG | HYG_SHORT>",
      "headline": "<1 decisive line ≤120 chars>",
      "thesis": "<2-3 sentences citing pillar data>",
      "trade_specifics": {{
        "primary_instrument": "<TLT | IEF | AGG | TIP | LQD | HYG | GLD | UUP | ZN futures | ZB futures | SOFR futures | etc>",
        "direction": "<LONG | SHORT | STEEPENER | FLATTENER | BARBELL>",
        "entry_level": "<price/yield/spread>",
        "target_level": "<price/yield/spread>",
        "stop_level": "<price/yield/spread>",
        "size_pct_of_portfolio": "<e.g. 3-5%>",
        "horizon": "<e.g. 4-8 weeks>",
        "expected_pnl_pct": "<e.g. +5 to +10%>",
        "dv01_aware_note": "<1 line on DV01 sizing or curve-trade neutrality>"
      }},
      "smoking_guns": [
        {{"pillar": "<auction_tape | primary_dealer | tic_flows | funding | net_liquidity | fed_path | yield_curve | etc>",
         "signal": "<specific datapoint with the number>",
         "anomaly_pctile": <0-100>}}
        // MINIMUM 3 from DIFFERENT pillars
      ],
      "front_running_catalyst": {{
        "event": "<FOMC | Refunding | NFP | CPI | OPEX | BOJ_meeting | OPEC | geo>",
        "date": "<date or window>",
        "consensus": "<what consensus expects>",
        "what_dealers_are_pricing": "<what positioning implies dealers expect>",
        "consensus_vs_dealer_position": "<aligned / divergent — and the implication>"
      }},
      "historical_analog": {{
        "period": "<specific episode e.g. Aug 2007 jumbo auction tail OR Mar 2023 SVB BTFP>",
        "what_happened": "<concrete outcome ≤30 words>",
        "similarity_pct": <0-100>
      }},
      "invalidation_tripwire": "<specific macro data condition that disproves the hypothesis>"
    }}
    // 2-5 setups, ranked by confidence × convergence × asymmetric payoff
  ],

  "upcoming_macro_catalysts": [
    {{
      "event": "<FOMC, NFP, CPI, refunding announcement, OPEC, etc>",
      "date": "<specific date or window>",
      "consensus": "<consensus expectation>",
      "front_run_signal_strength": "<STRONG | MODERATE | WEAK | NONE>",
      "what_to_watch": "<the specific pillar reading that would confirm or invalidate dealer positioning>"
    }}
    // 3-6 upcoming events
  ],

  "loudest_macro_anomaly": {{
    "pillar": "<which pillar>",
    "signal": "<the single most-extreme single-pillar datapoint>",
    "value": "<the number>",
    "anomaly_pctile": <0-100>,
    "interpretation": "<≤150 chars>"
  }},

  "most_actionable_macro_trade": "<which of the macro_setups above is the single best risk-adjusted action right now, ≤220 chars>"
}}

## RULES
- Be specific. Quote actual numbers from pillar feeds. Don't invent prices.
- Convergence requirement: every macro_setup MUST cite 3+ smoking_gun signals from DIFFERENT pillars.
- This is RATES/MACRO ONLY — no equity microstructure setups (no AMD options squeezes, no SPY options gamma, etc).
- Trade instruments must be RATES/MACRO instruments: bond ETFs (TLT/IEF/AGG/TIP/LQD/HYG), gold (GLD), dollar (UUP), Treasury futures (ZN/ZB/UB), SOFR/Eurodollar futures, or curve spreads (TLT/IEF, TIP/IEF).
- Historical analog must be a REAL macro episode (Aug 2007 jumbo tail, Sep 2019 repo, Mar 2020 basis unwind, Mar 2023 SVB BTFP, 2013 taper tantrum, 1994 massacre, Sep 2022 UK gilt LDI, 1979 Volcker, etc).
- Invalidation tripwire must be a specific pillar-data condition.
- DV01 sizing notes on curve trades, breakeven trades, duration trades."""

    return prompt


def generate_macro_frontrun_brief(ctx_id, cfg, episode_ref):
    t0 = time.time()
    result = {"context_id": ctx_id, "title": cfg.get("title"),
              "output_key": cfg.get("output_key"), "brief_type": "macro_frontrun"}
    try:
        pillar_feeds_cfg = cfg.get("pillar_feeds") or {}
        feeds = {}
        for pid, feed_key in pillar_feeds_cfg.items():
            d = s3io.get_json(feed_key, default={})
            if d:
                feeds[pid] = d

        if not feeds:
            result["status"] = "ERR_NO_INPUTS"
            result["err"] = "no macro pillar feeds loaded"
            return result

        kb_chunks = kb.lookup(cfg.get("kb_keywords") or [], max_chunks=2)

        system = BASE_SYSTEM_MACRO_FRONTRUN
        if cfg.get("system_addendum"):
            system = system + "\n\n" + cfg["system_addendum"]

        prompt = build_macro_frontrun_prompt(ctx_id, cfg, feeds, kb_chunks)
        prompt_len = len(prompt)

        brief, err = claude_json(prompt, system=system,
                                  max_tokens=cfg.get("max_tokens", 9000),
                                  temperature=cfg.get("temperature", 0.3),
                                  timeout=cfg.get("timeout", DEFAULT_TIMEOUT))
        if err or not brief:
            result["status"] = "ERR_CLAUDE"
            result["err"] = err or "no parseable JSON"
            result["prompt_len"] = prompt_len
            return result

        brief["version"] = "1.0"
        brief["brief_type"] = "macro_frontrun"
        brief["generated_at"] = datetime.now(timezone.utc).isoformat()
        brief["model"] = "claude-haiku-4-5-20251001"
        brief["context"] = ctx_id
        brief["title"] = cfg.get("title")
        brief["input_state"] = {
            "n_pillars_attempted": len(pillar_feeds_cfg),
            "n_pillars_loaded": len(feeds),
            "loaded": sorted(feeds.keys()),
            "missing": sorted(set(pillar_feeds_cfg.keys()) - set(feeds.keys())),
            "prompt_len_chars": prompt_len,
        }
        output_key = f"data/{cfg['output_key']}.json"
        s3io.put_json(output_key, brief, cache_control="public, max-age=900")

        # ────────────────────────────────────────────────────────────
        # 7-day history snapshot (separate file from equity sniffer)
        # ────────────────────────────────────────────────────────────
        try:
            hist_key = f"data/{cfg['output_key']}-history.json"
            history = s3io.get_json(hist_key, default={}) or {}
            snaps = history.get("snapshots") if isinstance(history, dict) else None
            if not isinstance(snaps, list):
                snaps = []

            top_setups = brief.get("macro_setups") or []
            top = top_setups[0] if top_setups else {}
            la = brief.get("loudest_macro_anomaly") or {}
            pillars = brief.get("pillars") or {}

            snap = {
                "ts": brief["generated_at"],
                "score": brief.get("overall_macro_score"),
                "regime": brief.get("macro_regime"),
                "headline": (brief.get("headline") or "")[:200],
                "n_setups": len(top_setups),
                "n_catalysts": len(brief.get("upcoming_macro_catalysts") or []),
                "top_setup_type": top.get("setup_type"),
                "top_setup_instr": (top.get("trade_specifics") or {}).get("primary_instrument"),
                "top_setup_dir":   (top.get("trade_specifics") or {}).get("direction"),
                "top_setup_conf":  top.get("confidence"),
                "top_setup_catalyst": (top.get("front_running_catalyst") or {}).get("event"),
                "loudest_pillar":  la.get("pillar"),
                "loudest_signal": (la.get("signal") or "")[:200],
                "loudest_pctile":  la.get("anomaly_pctile"),
                "auction_state":   (pillars.get("auction_tape") or {}).get("state"),
                "funding_state":   (pillars.get("funding_plumbing") or {}).get("state"),
                "liquidity_state": (pillars.get("net_liquidity") or {}).get("state"),
                "fed_path_state":  (pillars.get("fed_path") or {}).get("state"),
                "most_actionable": (brief.get("most_actionable_macro_trade") or "")[:200],
                "pillars_loaded":  len(feeds),
            }
            snaps.append(snap)
            snaps = snaps[-200:]

            now = datetime.now(timezone.utc)
            week_ago = now - timedelta(days=7)
            recent = []
            for s in snaps:
                try:
                    t = datetime.fromisoformat((s.get("ts") or "").replace("Z", "+00:00"))
                    if t >= week_ago: recent.append(s)
                except Exception: pass

            scores_7d = [s["score"] for s in recent if isinstance(s.get("score"), (int, float))]
            stats_7d = {
                "n_snapshots_7d": len(recent),
                "score_mean":   round(sum(scores_7d) / len(scores_7d), 1) if scores_7d else None,
                "score_min":    min(scores_7d) if scores_7d else None,
                "score_max":    max(scores_7d) if scores_7d else None,
                "score_latest": snap.get("score"),
                "score_delta_vs_mean_7d": (round(snap["score"] - sum(scores_7d) / len(scores_7d), 1)
                                            if (scores_7d and isinstance(snap.get("score"), (int, float))) else None),
                "n_extreme_7d":  sum(1 for s in recent if (s.get("regime") or "").upper() == "EXTREME"),
                "n_elevated_7d": sum(1 for s in recent if (s.get("regime") or "").upper() == "ELEVATED"),
                "n_normal_7d":   sum(1 for s in recent if (s.get("regime") or "").upper() == "NORMAL"),
                "max_setups_7d": max((s.get("n_setups") or 0) for s in recent) if recent else 0,
            }
            from collections import Counter
            instr_counter = Counter([s.get("top_setup_instr") for s in recent if s.get("top_setup_instr")])
            stats_7d["most_targeted_instruments"] = [
                {"instrument": k, "n_times": v} for k, v in instr_counter.most_common(5)
            ]

            events = []
            for s in snaps[::-1]:
                if ((s.get("score") or 0) >= 60) or ((s.get("regime") or "").upper() == "EXTREME"):
                    events.append({
                        "ts": s.get("ts"),
                        "score": s.get("score"),
                        "regime": s.get("regime"),
                        "headline": s.get("headline"),
                        "top_setup_instr": s.get("top_setup_instr"),
                        "top_setup_dir": s.get("top_setup_dir"),
                    })
                if len(events) >= 12: break

            history_out = {
                "version": "1.0",
                "generated_at": brief["generated_at"],
                "snapshots": snaps,
                "stats_7d": stats_7d,
                "events": events,
            }
            s3io.put_json(hist_key, history_out, cache_control="public, max-age=300")
        except Exception as _h_e:
            print(f"[macro_frontrun] history append failed: {_h_e}")

        # ────────────────────────────────────────────────────────────
        # Telegram alerting — convergence-fingerprint-aware
        # ────────────────────────────────────────────────────────────
        try:
            alert_result = _maybe_alert_macro_sniffer(brief)
            print(f"[macro_frontrun] alert: {alert_result}")
        except Exception as _a_e:
            print(f"[macro_frontrun] alert failed: {_a_e}")

        result.update({
            "status": "OK",
            "output_key": output_key,
            "macro_score": brief.get("overall_macro_score"),
            "macro_regime": brief.get("macro_regime"),
            "n_setups": len(brief.get("macro_setups") or []),
            "n_catalysts": len(brief.get("upcoming_macro_catalysts") or []),
            "duration_s": round(time.time() - t0, 2),
        })
    except Exception as e:
        result["status"] = "ERR_EXC"
        result["err"] = str(e)[:300]
        result["traceback"] = traceback.format_exc()[-800:]
    return result


# ─────────────────────────────────────────────────────────────────────
# Per-context worker — dispatches based on brief_type
# ─────────────────────────────────────────────────────────────────────
def generate_one_brief(ctx_id, cfg, episode_ref):
    """Generate the brief for a single context. Dispatches by brief_type."""
    bt = cfg.get("brief_type")
    if bt == "names":
        return generate_names_brief(ctx_id, cfg, episode_ref)
    if bt == "synthesis":
        return generate_synthesis_brief(ctx_id, cfg, episode_ref)
    if bt == "portfolio":
        return generate_portfolio_brief(ctx_id, cfg, episode_ref)
    if bt == "frontrun":
        return generate_frontrun_brief(ctx_id, cfg, episode_ref)
    if bt == "macro_frontrun":
        return generate_macro_frontrun_brief(ctx_id, cfg, episode_ref)
    # Default: regime brief
    return _generate_regime_brief(ctx_id, cfg, episode_ref)


def _generate_regime_brief(ctx_id, cfg, episode_ref):
    t0 = time.time()
    result = {"context_id": ctx_id, "title": cfg.get("title"), "output_key": cfg.get("output_key")}
    try:
        primary = s3io.get_json(cfg["primary_feed"], default={})
        if not primary:
            result["status"] = "ERR_NO_PRIMARY"
            result["err"] = f"{cfg['primary_feed']} empty or missing"
            return result

        cross_data = {}
        for cid, feed_key in (cfg.get("cross_feeds") or {}).items():
            d = s3io.get_json(feed_key, default={})
            if d:
                cross_data[cid] = d

        kb_chunks = kb.lookup(cfg.get("kb_keywords") or [], max_chunks=3)

        system = BASE_SYSTEM
        if cfg.get("system_addendum"):
            system = system + "\n\n" + cfg["system_addendum"]

        prompt = build_prompt(ctx_id, cfg, primary, cross_data, episode_ref, kb_chunks)
        prompt_len = len(prompt)

        brief, err = claude_json(prompt, system=system,
                                  max_tokens=cfg.get("max_tokens", 8000),
                                  temperature=cfg.get("temperature", 0.25),
                                  timeout=cfg.get("timeout", DEFAULT_TIMEOUT))
        if err or not brief:
            result["status"] = "ERR_CLAUDE"
            result["err"] = err or "no parseable JSON"
            result["prompt_len"] = prompt_len
            return result

        # Tag + write
        brief["version"] = "2.0"
        brief["generated_at"] = datetime.now(timezone.utc).isoformat()
        brief["model"] = "claude-haiku-4-5-20251001"
        brief["context"] = ctx_id
        brief["input_state"] = {
            "primary_feed": cfg["primary_feed"],
            "cross_feeds_loaded": list(cross_data.keys()),
            "kb_frameworks_used": [c.get("framework") for c in kb_chunks],
            "prompt_len_chars": prompt_len,
        }
        output_key = f"data/{cfg['output_key']}.json"
        s3io.put_json(output_key, brief, cache_control="public, max-age=900")

        result.update({
            "status": "OK",
            "output_key": output_key,
            "regime": brief.get("regime"),
            "confidence": brief.get("confidence"),
            "one_liner": brief.get("one_liner"),
            "n_predictions": len(brief.get("historical_predictions") or []),
            "n_trades": len(brief.get("trade_ideas") or []),
            "n_tripwires": len(brief.get("tripwires") or []),
            "duration_s": round(time.time() - t0, 2),
        })
    except Exception as e:
        result["status"] = "ERR_EXC"
        result["err"] = str(e)[:300]
        result["traceback"] = traceback.format_exc()[-800:]
    return result


# ─────────────────────────────────────────────────────────────────────
# Lambda handler — runs all contexts in the registry in parallel
# ─────────────────────────────────────────────────────────────────────
def lambda_handler(event=None, context=None):
    t0 = time.time()
    event = event or {}

    registry = s3io.get_json(REGISTRY_KEY, default={})
    contexts_cfg = (registry.get("contexts") or {})
    if not contexts_cfg:
        return {"statusCode": 503,
                "body": json.dumps({"err": f"{REGISTRY_KEY} empty or missing"})}

    # Allow event override to run a subset (useful for diagnostic runs):
    #   {"contexts": ["yield_curve", "vix_curve"]} → only those
    only = set(event.get("contexts") or [])
    if only:
        contexts_cfg = {k: v for k, v in contexts_cfg.items() if k in only}

    print(f"[ai-brief-router] running {len(contexts_cfg)} contexts in parallel: {list(contexts_cfg.keys())}")

    episode_ref = s3io.get_json(EPISODE_REF_KEY, default={})

    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(generate_one_brief, cid, cfg, episode_ref): cid
                   for cid, cfg in contexts_cfg.items()}
        for fut in as_completed(futures):
            try:
                r = fut.result()
            except Exception as e:
                r = {"context_id": futures[fut], "status": "ERR_FUT", "err": str(e)[:200]}
            results.append(r)
            print(f"[ai-brief-router] {r.get('context_id')}: {r.get('status')} {r.get('duration_s','?')}s {r.get('one_liner','')[:90]}")

    duration = round(time.time() - t0, 2)
    n_ok = sum(1 for r in results if r.get("status") == "OK")
    print(f"[ai-brief-router] done: {n_ok}/{len(results)} OK in {duration}s")

    return {"statusCode": 200, "body": json.dumps({
        "duration_s": duration,
        "n_contexts": len(results),
        "n_ok": n_ok,
        "results": [{k: v for k, v in r.items() if k != "traceback"} for r in results],
    }, default=str)}
