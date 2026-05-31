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


def _equity_convergence_fingerprint(brief):
    """Detect the equity gamma-squeeze / vol-cascade fingerprint.
    Looks across the smoking_gun_signals of the top 2 setups for 3+ of these
    4 cardinal equity-microstructure categories firing together:
      1. DEALER_GEX        (dealer gamma exposure shifts)
      2. OPTIONS_FLOW/GAMMA (unusual options activity, gamma exposure)
      3. SKEW/VOL          (skew steepening, tail-hedge bidding, IV crush)
      4. CFTC/SHORT_INTEREST (commercials flip, short interest stress)

    When 3+ cardinals fire simultaneously, that's the:
      - Jan 2021 GME gamma squeeze signature
      - Feb 2018 vol regime unwind signature
      - Dec 2018 dealer gamma cascade signature
      - Aug 2007 quant crowd same-trade signature

    Returns (fired, detail_dict).
    """
    setups = brief.get("suspected_setups") or []
    if not setups:
        return False, {"reason": "no_setups"}

    cardinals = {
        "DEALER_GEX": False,
        "OPTIONS_FLOW_GAMMA": False,
        "SKEW_VOL": False,
        "CFTC_SHORT": False,
    }
    contributing = []
    for sx in setups[:2]:  # Top 2 setups
        for g in (sx.get("smoking_gun_signals") or []):
            cat = (g.get("category") or "").upper()
            sig = (g.get("signal") or "")[:120]
            # DEALER_GEX
            if ("DEALER_GEX" in cat or "GEX" in cat) and not cardinals["DEALER_GEX"]:
                cardinals["DEALER_GEX"] = True
                contributing.append({"cardinal": "DEALER_GEX", "category": g.get("category"), "signal": sig})
            # OPTIONS_FLOW or OPTIONS_GAMMA (excluding pure DEALER_GEX which is separate)
            elif ("OPTIONS_FLOW" in cat or "OPTIONS_GAMMA" in cat or cat == "GAMMA") and not cardinals["OPTIONS_FLOW_GAMMA"]:
                cardinals["OPTIONS_FLOW_GAMMA"] = True
                contributing.append({"cardinal": "OPTIONS_FLOW_GAMMA", "category": g.get("category"), "signal": sig})
            # SKEW / VOL / TAIL HEDGING
            elif ("SKEW" in cat or "IV_CRUSH" in cat or "TAIL" in cat
                  or cat == "VOL" or "CATALYST_SKEW" in cat) and not cardinals["SKEW_VOL"]:
                cardinals["SKEW_VOL"] = True
                contributing.append({"cardinal": "SKEW_VOL", "category": g.get("category"), "signal": sig})
            # CFTC / SHORT_INTEREST / SQUEEZE
            elif ("CFTC" in cat or "SHORT_INTEREST" in cat
                  or "SHORT" == cat or "SQUEEZE" in cat) and not cardinals["CFTC_SHORT"]:
                cardinals["CFTC_SHORT"] = True
                contributing.append({"cardinal": "CFTC_SHORT", "category": g.get("category"), "signal": sig})

    n_present = sum(1 for v in cardinals.values() if v)
    fired = n_present >= 3

    return fired, {
        "n_cardinal_present": n_present,
        "n_cardinal_total": 4,
        "cardinals_fired": cardinals,
        "top_setup_target": (setups[0].get("target_asset") if setups else None),
        "top_setup_direction": (setups[0].get("target_direction") if setups else None),
        "contributing_signals": contributing[:6],
    }


def _format_equity_alert(brief, label, page_url, kind, prev_state, conv_fired=False, conv_detail=None):
    """Build the Telegram message for an equity sniffer alert."""
    score = brief.get("overall_anomaly_score")
    regime = (brief.get("anomaly_regime") or "NORMAL").upper()
    headline = brief.get("headline") or ""
    most_actionable = brief.get("most_actionable_setup") or ""
    top_setups = brief.get("suspected_setups") or []
    top = top_setups[0] if top_setups else {}
    la = brief.get("loudest_anomaly") or {}

    if conv_fired:
        title = f"🚨🎯 *EQUITY CONVERGENCE FINGERPRINT* — Jan 2021 GME / Feb 2018 vol unwind / Dec 2018 dealer cascade signature"
    elif kind == "PARTIAL_BUILDUP":
        title = f"⚠️ *EQUITY BUILDUP — 2/4 CARDINALS FIRING*"
    elif kind == "REGIME_UP":
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

    # Convergence pillar breakdown (for both full fire and partial buildup)
    if (conv_fired or kind == "PARTIAL_BUILDUP") and conv_detail:
        cardinals = conv_detail.get("cardinals_fired") or {}
        n_present = conv_detail.get("n_cardinal_present", 0)
        if conv_fired:
            hdr = f"⚠ *Equity convergence fingerprint* ({n_present}/4 cardinal categories):"
        else:
            hdr = f"⚠ *Equity convergence buildup* ({n_present}/4 cardinals firing):"
        msg += (f"{hdr}\n"
                f"  {'✓' if cardinals.get('DEALER_GEX') else '✗'} Dealer GEX\n"
                f"  {'✓' if cardinals.get('OPTIONS_FLOW_GAMMA') else '✗'} Options Flow / Gamma\n"
                f"  {'✓' if cardinals.get('SKEW_VOL') else '✗'} Skew / Vol / Tail Hedge\n"
                f"  {'✓' if cardinals.get('CFTC_SHORT') else '✗'} CFTC / Short Interest\n")
        if conv_detail.get("top_setup_target"):
            msg += f"  → target: *{conv_detail['top_setup_target']}* {conv_detail.get('top_setup_direction','')}\n"
        if kind == "PARTIAL_BUILDUP":
            msg += "  → If 1 more lights up: Jan 2021 GME / Feb 2018 / Dec 2018 signature.\n"
        msg += "\n"

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
    elif kind == "PARTIAL_BUILDUP":
        title = f"⚠️ *MACRO BUILDUP — 2/3 PILLARS STRESSED*"
    elif kind == "PARTIAL_EARLY":
        title = f"🔵 *MACRO EARLY WARNING — first pillar moved*"
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

    # Render the 3-pillar state for any convergence-related kind
    if (conv_fired or kind in ("PARTIAL_BUILDUP", "PARTIAL_EARLY")) and conv_detail:
        auc = conv_detail.get("auction_tape_state") or "—"
        pd  = conv_detail.get("primary_dealer_state") or "—"
        fp  = conv_detail.get("funding_state") or "—"
        n   = conv_detail.get("n_pillars_stressed", 0)
        # Stressed if NOT in the calm set
        def _icon(state, calm_set):
            return "✓" if str(state).upper() not in calm_set else "✗"
        auc_icon = _icon(auc, {"CALM"})
        pd_icon  = _icon(pd,  {"NEUTRAL"})
        fp_icon  = _icon(fp,  {"HEALED"})

        if conv_fired:
            hdr = "⚠ *Convergence fingerprint — all 3 pillars stressed*:"
        elif kind == "PARTIAL_BUILDUP":
            hdr = f"⚠ *Convergence buildup — {n}/3 pillars stressed*:"
        else:
            hdr = f"🔵 *Early warning — {n}/3 pillar(s) stressed*:"

        msg += (f"{hdr}\n"
                f"  {auc_icon} Auction tape: `{auc}`\n"
                f"  {pd_icon} Primary dealer: `{pd}`\n"
                f"  {fp_icon} Funding plumbing: `{fp}`\n")

        if kind == "PARTIAL_BUILDUP":
            msg += "  → If 1 more moves: Aug 2007 / Sep 2019 / Mar 2020 / Mar 2023 signature.\n\n"
        elif kind == "PARTIAL_EARLY":
            msg += "  → Earliest signal in the convergence pattern. Watch the other 2 pillars.\n\n"
        else:
            msg += "  → 3/3 pillars stressed simultaneously — highest-conviction macro pattern.\n\n"

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


def _decide_alert_kind(prev_state, new_score, new_regime, conv_fired_now, conv_was_recent,
                        n_cardinals=None, total_cardinals=None,
                        partial_buildup_threshold=None, partial_early_threshold=None):
    """Return (kind, reason) or (None, reason) if no alert should fire.

    Priority ladder:
      1. CONVERGENCE_FIRE      — full fingerprint (3/3 macro or 3-4/4 equity)
      2. REGIME transitions    — NORMAL/ELEVATED/EXTREME shifts
      3. PARTIAL_BUILDUP       — crossed into N≥2 stressed cardinals (transition)
      4. EXTREME persisting    — regime EXTREME continuing
      5. PARTIAL_EARLY         — crossed into N=1 stressed (macro only typically)
      6. SCORE_JUMP            — |Δ| ≥ 15

    Partial alerts fire ONLY on upward transitions across a threshold:
      prev_n < threshold AND new_n >= threshold → fire (bypasses cooldown).
      Otherwise (already at/above the threshold) → suppressed to avoid spam.

    For equity: partial_early_threshold can be None (skip 1/4 early-warning).
    For macro:  partial_early_threshold=1 → fires on first cardinal moving.
    """
    prev_regime = (prev_state.get("last_regime") or "").upper()
    prev_score  = prev_state.get("last_score")
    prev_n      = prev_state.get("last_cardinal_count", 0) or 0
    last_alert_at = prev_state.get("last_alert_at")
    alerts_today = prev_state.get("alerts_today", 0)
    alerts_today_date = prev_state.get("alerts_today_date", "")

    # Reset daily counter
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if alerts_today_date != today_str:
        alerts_today = 0

    if alerts_today >= ALERT_DAILY_CAP:
        return None, f"daily_cap_reached ({alerts_today})"

    # Cooldown check (priority alerts bypass)
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

    # PRIORITY 3: PARTIAL_BUILDUP — crossed upward into buildup threshold (bypasses cooldown)
    # Only fires when the fingerprint is NOT already firing (CONVERGENCE handles that case).
    if (not conv_fired_now and n_cardinals is not None and
        partial_buildup_threshold is not None and
        n_cardinals >= partial_buildup_threshold and
        prev_n < partial_buildup_threshold):
        return "PARTIAL_BUILDUP", f"crossed_to_{n_cardinals}_of_{total_cardinals}"

    # PRIORITY 4: EXTREME persisting (respects cooldown)
    if new_regime == "EXTREME" and cooldown_ok:
        return "EXTREME", "extreme_persisting"

    # PRIORITY 5: PARTIAL_EARLY — first cardinal moved (bypasses cooldown, gentler than buildup)
    # Macro only typically (1/3 = leading indicator). For equity skip to avoid noise.
    if (not conv_fired_now and n_cardinals is not None and
        partial_early_threshold is not None and
        n_cardinals >= partial_early_threshold and
        prev_n < partial_early_threshold):
        return "PARTIAL_EARLY", f"crossed_to_{n_cardinals}_of_{total_cardinals}"

    # PRIORITY 6: Score jump (>= 15 points) — respects cooldown
    if prev_score is not None and cooldown_ok:
        delta = abs(new_score - prev_score)
        if delta >= 15:
            return "SCORE_JUMP", f"delta_{delta}"

    return None, ("cooldown" if not cooldown_ok else "no_threshold_met")


def _maybe_alert_equity_sniffer(brief, page_url="https://justhodl.ai/frontrun.html"):
    """Check the equity sniffer state and fire a Telegram alert if needed.
    Now includes equity convergence-fingerprint detection (Jan 2021 GME /
    Feb 2018 vol unwind / Dec 2018 dealer cascade signature) — priority-1
    alert symmetric to the macro convergence fingerprint. Also fires
    PARTIAL_BUILDUP alerts on transitions to 2/4 cardinals."""
    try:
        state_key = "frontrun-sniffer-alert-state"
        prev = _alert_state_io(state_key) or {}

        score = brief.get("overall_anomaly_score")
        regime = (brief.get("anomaly_regime") or "NORMAL").upper()
        if score is None: return {"ok": False, "reason": "no_score"}

        # Equity convergence fingerprint detection (DEALER_GEX + OPTIONS_FLOW +
        # SKEW + CFTC across top 2 setups — 3-of-4 cardinals required for full fire)
        conv_fired, conv_detail = _equity_convergence_fingerprint(brief)
        n_cardinals = conv_detail.get("n_cardinal_present", 0) if conv_detail else 0
        conv_was_recent = False
        last_conv_at = prev.get("last_convergence_fingerprint_at")
        if last_conv_at:
            try:
                lc = datetime.fromisoformat(last_conv_at.replace("Z","+00:00"))
                hrs_since = (datetime.now(timezone.utc) - lc).total_seconds() / 3600
                conv_was_recent = hrs_since < 4
            except Exception: pass

        kind, reason = _decide_alert_kind(
            prev, score, regime, conv_fired, conv_was_recent,
            n_cardinals=n_cardinals, total_cardinals=4,
            partial_buildup_threshold=2,          # 2/4 cardinals = buildup
            partial_early_threshold=None,         # Skip 1/4 (too noisy for equity)
        )
        if kind is None:
            # Still update cardinal count even if no alert fires (for next cycle)
            silent_state = dict(prev)
            silent_state["last_cardinal_count"] = n_cardinals
            _alert_state_io(state_key, write=silent_state)
            return {"ok": False, "reason": reason, "score": score, "regime": regime,
                    "conv_fired": conv_fired, "conv_detail": conv_detail,
                    "n_cardinals": n_cardinals}

        msg = _format_equity_alert(brief, "Front-Run Sniffer", page_url, kind, prev, conv_fired, conv_detail)
        ok, info = _telegram_post(msg)

        # Update state
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if prev.get("alerts_today_date") != today_str:
            prev["alerts_today"] = 0
        new_state = {
            "last_regime": regime,
            "last_score":  score,
            "last_cardinal_count": n_cardinals,
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
                "conv_fired": conv_fired, "n_cardinals": n_cardinals, "telegram": info}
    except Exception as e:
        return {"ok": False, "err": str(e)[:300]}


def _maybe_alert_macro_sniffer(brief, page_url="https://justhodl.ai/macro-frontrun.html"):
    """Check the macro sniffer state and fire a Telegram alert if needed,
    with the convergence-fingerprint priority detection PLUS:
      - PARTIAL_EARLY  at 1/3 pillars (transition from 0)
      - PARTIAL_BUILDUP at 2/3 pillars (transition from <2)
      - CONVERGENCE   at 3/3 (full fingerprint, transition handled)"""
    try:
        state_key = "macro-frontrun-sniffer-alert-state"
        prev = _alert_state_io(state_key) or {}

        score = brief.get("overall_macro_score")
        regime = (brief.get("macro_regime") or "NORMAL").upper()
        if score is None: return {"ok": False, "reason": "no_score"}

        # Convergence fingerprint detection
        conv_fired, conv_detail = _macro_convergence_fingerprint(brief)
        n_pillars = conv_detail.get("n_pillars_stressed", 0) if conv_detail else 0
        conv_was_recent = False
        last_conv_at = prev.get("last_convergence_fingerprint_at")
        if last_conv_at:
            try:
                lc = datetime.fromisoformat(last_conv_at.replace("Z","+00:00"))
                hrs_since = (datetime.now(timezone.utc) - lc).total_seconds() / 3600
                conv_was_recent = hrs_since < 4
            except Exception: pass

        kind, reason = _decide_alert_kind(
            prev, score, regime, conv_fired, conv_was_recent,
            n_cardinals=n_pillars, total_cardinals=3,
            partial_buildup_threshold=2,         # 2/3 = buildup
            partial_early_threshold=1,           # 1/3 = early warning (first pillar moves)
        )
        if kind is None:
            # Still update cardinal count even if no alert fires
            silent_state = dict(prev)
            silent_state["last_cardinal_count"] = n_pillars
            _alert_state_io(state_key, write=silent_state)
            return {"ok": False, "reason": reason, "score": score, "regime": regime,
                    "conv_fired": conv_fired, "conv_detail": conv_detail,
                    "n_pillars": n_pillars}

        msg = _format_macro_alert(brief, "Macro Front-Run Sniffer", page_url, kind, prev, conv_fired, conv_detail)
        ok, info = _telegram_post(msg)

        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if prev.get("alerts_today_date") != today_str:
            prev["alerts_today"] = 0
        new_state = {
            "last_regime": regime,
            "last_score":  score,
            "last_cardinal_count": n_pillars,
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
                "conv_fired": conv_fired, "n_pillars": n_pillars, "telegram": info}
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
# DAILY ALERTS DIGEST — single 9am UTC summary across both sniffers
# ─────────────────────────────────────────────────────────────────────
def _events_in_last_24h(events_list):
    """Filter event list to last 24h. Newest first."""
    if not events_list: return []
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    recent = []
    for e in events_list:
        try:
            t = datetime.fromisoformat((e.get("ts") or "").replace("Z", "+00:00"))
            if t >= cutoff:
                recent.append(e)
        except Exception: pass
    recent.sort(key=lambda e: e.get("ts") or "", reverse=True)
    return recent


def _update_targets_index(equity_history, macro_history, session, today_str):
    """Maintain a rolling 30-day targets leaderboard from each digest's
    'most_targeted_assets' / 'most_targeted_instruments' snapshots.

    Each digest run upserts the current top-N targets into a persistent
    index. Targets whose last_seen is older than 30 days drop off. Sorted
    by n_digest_appearances DESC (which names KEEP showing up = sustained
    front-running pattern, vs. one-off flags).

    Returns the updated index object.
    """
    index_key = "data/_alerts/targets-index.json"
    try:
        idx = s3io.get_json(index_key, default={}) or {}
        eq_idx = idx.get("equity_targets") if isinstance(idx, dict) else []
        mc_idx = idx.get("macro_targets")  if isinstance(idx, dict) else []
        if not isinstance(eq_idx, list): eq_idx = []
        if not isinstance(mc_idx, list): mc_idx = []

        # Current top targets from history files (already rolling 7-day stats)
        eq_stats = (equity_history or {}).get("stats_7d") or {}
        mc_stats = (macro_history  or {}).get("stats_7d") or {}
        eq_today = eq_stats.get("most_targeted_assets") or []
        mc_today = mc_stats.get("most_targeted_instruments") or []

        # Build name → entry maps from existing index
        eq_map = {(e.get("asset") or ""): e for e in eq_idx if e.get("asset")}
        mc_map = {(e.get("instrument") or ""): e for e in mc_idx if e.get("instrument")}

        def _upsert(map_, target, key_field):
            name = target.get(key_field)
            if not name: return
            n_times = target.get("n_times", 1) or 1
            existing = map_.get(name) or {
                key_field:               name,
                "n_digest_appearances":  0,
                "max_n_times_in_window": 0,
                "first_seen":            today_str,
                "last_seen_session":     session,
                "sessions":              {"open": 0, "close": 0},
            }
            existing["n_digest_appearances"] = existing.get("n_digest_appearances", 0) + 1
            existing["max_n_times_in_window"] = max(existing.get("max_n_times_in_window", 0), n_times)
            existing["last_seen"] = today_str
            existing["last_seen_session"] = session
            if not existing.get("first_seen"): existing["first_seen"] = today_str
            sess = existing.get("sessions") or {"open": 0, "close": 0}
            sess[session] = sess.get(session, 0) + 1
            existing["sessions"] = sess
            map_[name] = existing

        for t in eq_today: _upsert(eq_map, t, "asset")
        for t in mc_today: _upsert(mc_map, t, "instrument")

        # Drop entries whose last_seen is older than 30 days
        cutoff_30d = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")
        eq_filt = [e for e in eq_map.values() if (e.get("last_seen") or "") >= cutoff_30d]
        mc_filt = [e for e in mc_map.values() if (e.get("last_seen") or "") >= cutoff_30d]

        # Sort: by n_digest_appearances DESC, then last_seen DESC
        def _rk(e): return (e.get("n_digest_appearances", 0), e.get("last_seen") or "")
        eq_filt.sort(key=_rk, reverse=True)
        mc_filt.sort(key=_rk, reverse=True)

        # Tag is_recent (last_seen within last 7 days = active pattern)
        cutoff_7d = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")
        for e in eq_filt: e["is_recent"] = (e.get("last_seen") or "") >= cutoff_7d
        for e in mc_filt: e["is_recent"] = (e.get("last_seen") or "") >= cutoff_7d

        # Cap each list at 50 (more than enough — typical Wow leaderboards are 10-20)
        eq_filt = eq_filt[:50]
        mc_filt = mc_filt[:50]

        out = {
            "version":           "1.0",
            "updated_at":        datetime.now(timezone.utc).isoformat(),
            "lookback_days":     30,
            "n_equity_targets":  len(eq_filt),
            "n_macro_targets":   len(mc_filt),
            "n_equity_recent":   sum(1 for e in eq_filt if e.get("is_recent")),
            "n_macro_recent":    sum(1 for e in mc_filt if e.get("is_recent")),
            "equity_targets":    eq_filt,
            "macro_targets":     mc_filt,
        }
        s3io.put_json(index_key, out, cache_control="no-store")
        return out
    except Exception as _e:
        print(f"[digest] targets-index update failed: {_e}")
        return None


def _format_digest(equity_brief, macro_brief,
                    equity_state, macro_state,
                    equity_hist, macro_hist, session="open",
                    targets_idx=None):
    """Build the daily digest Markdown message.

    session: 'open'  → morning brief (09:00 UTC pre-market, forward-looking)
             'close' → US session debrief (21:00 UTC post-close, retrospective)
    targets_idx: optional rolling 30-day targets-index dict from _update_targets_index.
                  When provided, adds a 'Sustained patterns (30d)' section.
    """
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if session == "close":
        title_emoji = "🔔"
        title_text = "DAILY CLOSE DIGEST"
        time_label = "21:00 UTC · US session debrief"
        events_window_label = "US session events"
    else:
        title_emoji = "📅"
        title_text = "DAILY OPEN DIGEST"
        time_label = "09:00 UTC · pre-market"
        events_window_label = "Last 24h events"

    # Current state
    eq_score  = (equity_brief or {}).get("overall_anomaly_score")
    eq_regime = ((equity_brief or {}).get("anomaly_regime") or "—").upper()
    mc_score  = (macro_brief or {}).get("overall_macro_score")
    mc_regime = ((macro_brief or {}).get("macro_regime") or "—").upper()

    eq_last_at = (equity_state or {}).get("last_alert_at")
    mc_last_at = (macro_state  or {}).get("last_alert_at")

    def age(iso):
        if not iso: return "never"
        try:
            ms = (datetime.now(timezone.utc) - datetime.fromisoformat(iso.replace("Z","+00:00"))).total_seconds()
            if ms < 60: return "just now"
            if ms < 3600: return f"{int(ms/60)}m ago"
            if ms < 86400: return f"{int(ms/3600)}h ago"
            return f"{int(ms/86400)}d ago"
        except Exception: return "—"

    eq_alerts_today = (equity_state or {}).get("alerts_today", 0)
    mc_alerts_today = (macro_state  or {}).get("alerts_today", 0)

    # Convergence fingerprints — current state
    eq_conv_fired, eq_conv_detail = _equity_convergence_fingerprint(equity_brief or {})
    mc_conv_fired, mc_conv_detail = _macro_convergence_fingerprint(macro_brief or {})
    eq_n = eq_conv_detail.get("n_cardinal_present", 0)
    mc_n = mc_conv_detail.get("n_pillars_stressed", 0)

    # Yesterday's events from both history files (events[] = score≥60 or EXTREME)
    eq_events_24h = _events_in_last_24h((equity_hist or {}).get("events") or [])
    mc_events_24h = _events_in_last_24h((macro_hist  or {}).get("events") or [])

    # Week stats from each history file
    eq_stats = (equity_hist or {}).get("stats_7d") or {}
    mc_stats = (macro_hist  or {}).get("stats_7d") or {}

    # ── Build message ──
    msg = f"{title_emoji} *{title_text}* — {today_str} {time_label}\n\n"

    # Current state
    msg += "📡 *Current state:*\n"
    msg += f"  🎯 Equity:  score *{eq_score if eq_score is not None else '—'}* · regime *{eq_regime}* · last alert {age(eq_last_at)} · {eq_alerts_today}/8 today\n"
    msg += f"  🏛 Macro:   score *{mc_score if mc_score is not None else '—'}* · regime *{mc_regime}* · last alert {age(mc_last_at)} · {mc_alerts_today}/8 today\n\n"

    # Convergence fingerprints
    msg += "⭐ *Convergence fingerprints:*\n"
    if eq_conv_detail and "cardinals_fired" in eq_conv_detail:
        c = eq_conv_detail["cardinals_fired"]
        msg += f"  🎯 Equity:  {'✓' if c.get('DEALER_GEX') else '✗'} GEX  {'✓' if c.get('OPTIONS_FLOW_GAMMA') else '✗'} OptFlow  {'✓' if c.get('SKEW_VOL') else '✗'} Skew/Vol  {'✓' if c.get('CFTC_SHORT') else '✗'} CFTC/Short  *({eq_n}/4{' — FIRED' if eq_conv_fired else ''})*\n"
    else:
        msg += "  🎯 Equity:  (no brief yet)\n"

    if mc_conv_detail and "auction_tape_state" in mc_conv_detail:
        msg += (f"  🏛 Macro:   `{mc_conv_detail.get('auction_tape_state','—')}` auction · "
                f"`{mc_conv_detail.get('primary_dealer_state','—')}` dealer · "
                f"`{mc_conv_detail.get('funding_state','—')}` funding  "
                f"*({mc_n}/3{' — FIRED' if mc_conv_fired else ''})*\n")
    else:
        msg += "  🏛 Macro:   (no brief yet)\n"
    msg += "\n"

    # Yesterday's events
    total_events = len(eq_events_24h) + len(mc_events_24h)
    if total_events:
        msg += f"🗓 *{events_window_label} ({total_events}):*\n"
        merged = []
        for e in eq_events_24h: merged.append({**e, "_src": "🎯"})
        for e in mc_events_24h: merged.append({**e, "_src": "🏛"})
        merged.sort(key=lambda x: x.get("ts") or "", reverse=True)
        for e in merged[:8]:
            try:
                t = datetime.fromisoformat((e.get("ts") or "").replace("Z","+00:00"))
                hh = t.strftime("%H:%M")
            except Exception: hh = "??:??"
            target = e.get("top_setup_asset") or e.get("top_setup_instr") or ""
            direction = e.get("top_setup_dir") or ""
            msg += f"  • {hh}  {e.get('_src')} score *{e.get('score')}* {(e.get('regime') or '').upper()}"
            if target: msg += f" — `{target}` {direction}"
            msg += "\n"
        msg += "\n"
    else:
        msg += f"🗓 *{events_window_label}:* none — tape was quiet ✓\n\n"

    # 7-day trajectory
    msg += "📊 *7-day score trajectory:*\n"
    if eq_stats:
        msg += (f"  🎯 Equity:  mean *{eq_stats.get('score_mean','—')}*, "
                f"range *{eq_stats.get('score_min','—')}-{eq_stats.get('score_max','—')}*, "
                f"{eq_stats.get('n_snapshots_7d',0)} cycles · "
                f"{eq_stats.get('n_extreme_7d',0)} extreme, {eq_stats.get('n_elevated_7d',0)} elevated\n")
    if mc_stats:
        msg += (f"  🏛 Macro:   mean *{mc_stats.get('score_mean','—')}*, "
                f"range *{mc_stats.get('score_min','—')}-{mc_stats.get('score_max','—')}*, "
                f"{mc_stats.get('n_snapshots_7d',0)} cycles · "
                f"{mc_stats.get('n_extreme_7d',0)} extreme, {mc_stats.get('n_elevated_7d',0)} elevated\n")
    msg += "\n"

    # Most targeted
    eq_targets = eq_stats.get("most_targeted_assets") if isinstance(eq_stats, dict) else None
    mc_instrs  = mc_stats.get("most_targeted_instruments") if isinstance(mc_stats, dict) else None
    if eq_targets or mc_instrs:
        msg += "🎯 *Most targeted (last 7d):*\n"
        if eq_targets:
            txt = ", ".join(f"`{t.get('asset')}`×{t.get('n_times')}" for t in eq_targets[:5])
            msg += f"  🎯 Equity:  {txt}\n"
        if mc_instrs:
            txt = ", ".join(f"`{t.get('instrument')}`×{t.get('n_times')}" for t in mc_instrs[:5])
            msg += f"  🏛 Macro:   {txt}\n"
        msg += "\n"

    # Sustained patterns from rolling 30-day targets index — shows which names
    # KEEP showing up across digest snapshots (vs one-off flags). Threshold ≥2
    # appearances filters out single observations. Each name marked recent ● if
    # last_seen within last 7 days, ○ if cooling.
    if targets_idx:
        eq_idx = targets_idx.get("equity_targets") or []
        mc_idx = targets_idx.get("macro_targets")  or []
        eq_sustained = [t for t in eq_idx if (t.get("n_digest_appearances", 0) or 0) >= 2][:3]
        mc_sustained = [t for t in mc_idx if (t.get("n_digest_appearances", 0) or 0) >= 2][:3]
        if eq_sustained or mc_sustained:
            msg += "📈 *Sustained patterns (30d rolling index):*\n"
            if eq_sustained:
                txt = ", ".join(
                    f"`{t.get('asset')}`×{t.get('n_digest_appearances')}"
                    f"{'●' if t.get('is_recent') else '○'}"
                    for t in eq_sustained
                )
                msg += f"  🎯 Equity:  {txt}\n"
            if mc_sustained:
                txt = ", ".join(
                    f"`{t.get('instrument')}`×{t.get('n_digest_appearances')}"
                    f"{'●' if t.get('is_recent') else '○'}"
                    for t in mc_sustained
                )
                msg += f"  🏛 Macro:   {txt}\n"
            msg += "  _(● = active in last 7d, ○ = cooling off)_\n"
            msg += "\n"

    msg += "→ https://justhodl.ai/targets.html · https://justhodl.ai/alerts.html"
    return msg


def generate_alerts_digest(ctx_id, cfg, episode_ref):
    """Run the daily alerts digest. Reads all 6 data sources, builds the
    summary message, sends via Telegram, writes an audit log to S3.

    session_flavor (from cfg, default 'open'):
      'open'  → morning 09:00 UTC pre-market brief
      'close' → 21:00 UTC US-session debrief
    """
    t0 = time.time()
    session = (cfg.get("session_flavor") or "open").lower()
    if session not in ("open", "close"): session = "open"

    result = {"context_id": ctx_id, "title": cfg.get("title"),
              "brief_type": "digest", "session": session,
              "output_key": cfg.get("output_key")}
    try:
        sources = cfg.get("read_sources") or {}
        feeds = {}
        for k, key in sources.items():
            feeds[k] = s3io.get_json(key, default={}) or {}

        equity_brief    = feeds.get("equity_brief")
        macro_brief     = feeds.get("macro_brief")
        equity_state    = feeds.get("equity_state")
        macro_state     = feeds.get("macro_state")
        equity_history  = feeds.get("equity_history")
        macro_history   = feeds.get("macro_history")

        # Update the 30-day rolling targets leaderboard FIRST so the digest
        # message can reflect the freshly-incremented counts (including
        # today's observation).
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        targets_idx = _update_targets_index(equity_history, macro_history,
                                             session, today_str)
        if targets_idx:
            result["targets_index"] = {
                "n_equity_targets": targets_idx.get("n_equity_targets"),
                "n_macro_targets":  targets_idx.get("n_macro_targets"),
                "n_equity_recent":  targets_idx.get("n_equity_recent"),
                "n_macro_recent":   targets_idx.get("n_macro_recent"),
            }

        msg = _format_digest(
            equity_brief, macro_brief,
            equity_state, macro_state,
            equity_history, macro_history,
            session=session,
            targets_idx=targets_idx,
        )

        # Send Telegram (with plain-text fallback)
        ok, info = _telegram_post(msg)

        # Write audit log — separate key per session
        audit = {
            "version": "1.0",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "date": today_str,
            "session": session,
            "telegram_ok": ok,
            "telegram_info": info,
            "message_chars": len(msg),
            "message_preview": msg[:1500],
            "equity_score": (equity_brief or {}).get("overall_anomaly_score"),
            "equity_regime": (equity_brief or {}).get("anomaly_regime"),
            "macro_score": (macro_brief or {}).get("overall_macro_score"),
            "macro_regime": (macro_brief or {}).get("macro_regime"),
            "n_equity_alerts_today": (equity_state or {}).get("alerts_today", 0),
            "n_macro_alerts_today":  (macro_state  or {}).get("alerts_today", 0),
        }
        # Audit key: open keeps legacy 'digest-{date}.json' for backward compat;
        # close gets 'digest-{date}-close.json'
        if session == "close":
            audit_key = f"data/_alerts/digest-{today_str}-close.json"
        else:
            audit_key = f"data/_alerts/digest-{today_str}.json"
        s3io.put_json(audit_key, audit, cache_control="no-store")
        # Latest alias: always points to whichever just ran
        s3io.put_json("data/_alerts/digest-latest.json", audit, cache_control="no-store")

        # ────────────────────────────────────────────────────────────
        # Maintain a rolling index so the archive page can list digests
        # ────────────────────────────────────────────────────────────
        try:
            index_key = "data/_alerts/digests-index.json"
            idx = s3io.get_json(index_key, default={}) or {}
            entries = idx.get("entries") if isinstance(idx, dict) else None
            if not isinstance(entries, list):
                entries = []

            # Compute activity level
            eq_extreme = (equity_brief or {}).get("anomaly_regime") == "EXTREME"
            mc_extreme = (macro_brief  or {}).get("macro_regime")   == "EXTREME"
            eq_score_v = audit.get("equity_score") or 0
            mc_score_v = audit.get("macro_score")  or 0
            n_alerts = audit.get("n_equity_alerts_today", 0) + audit.get("n_macro_alerts_today", 0)
            if eq_extreme or mc_extreme or eq_score_v >= 70 or mc_score_v >= 70:
                activity_level = "EXTREME"
            elif n_alerts > 0 or eq_score_v >= 45 or mc_score_v >= 45:
                activity_level = "ACTIVE"
            else:
                activity_level = "QUIET"

            # Compact entry for the index
            entry = {
                "date":                  today_str,
                "session":               session,
                "key":                   audit_key,
                "generated_at":          audit["generated_at"],
                "telegram_ok":           audit["telegram_ok"],
                "equity_score":          audit.get("equity_score"),
                "equity_regime":         audit.get("equity_regime"),
                "macro_score":           audit.get("macro_score"),
                "macro_regime":          audit.get("macro_regime"),
                "n_equity_alerts_today": audit.get("n_equity_alerts_today", 0),
                "n_macro_alerts_today":  audit.get("n_macro_alerts_today",  0),
                "activity_level":        activity_level,
                "message_chars":         audit.get("message_chars"),
            }

            # De-dupe by (date, session) — both sessions per day are kept.
            # Entries without a session field default to "open" (back-compat).
            def _key(e): return (e.get("date") or "", (e.get("session") or "open"))
            entries = [e for e in entries if _key(e) != (today_str, session)]
            entries.append(entry)
            # Sort newest first; within same date 'close' comes before 'open'
            def _rank(e):
                return (e.get("date") or "", 1 if (e.get("session") or "open") == "close" else 0)
            entries.sort(key=_rank, reverse=True)
            # Cap to 120 (~2 months × 2 sessions)
            entries = entries[:120]

            n_extreme = sum(1 for e in entries if e.get("activity_level") == "EXTREME")
            n_active  = sum(1 for e in entries if e.get("activity_level") == "ACTIVE")
            n_quiet   = sum(1 for e in entries if e.get("activity_level") == "QUIET")

            idx_out = {
                "version": "1.0",
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "n_entries": len(entries),
                "earliest_date": (entries[-1].get("date") if entries else None),
                "latest_date":   (entries[0].get("date")  if entries else None),
                "activity_breakdown": {
                    "extreme": n_extreme, "active": n_active, "quiet": n_quiet,
                },
                "entries": entries,
            }
            s3io.put_json(index_key, idx_out, cache_control="no-store")
        except Exception as _i_e:
            print(f"[digest] index update failed: {_i_e}")

        result.update({
            "status": "OK",
            "telegram_ok": ok,
            "message_chars": len(msg),
            "audit_key": audit_key,
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
    if bt == "digest":
        return generate_alerts_digest(ctx_id, cfg, episode_ref)
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
