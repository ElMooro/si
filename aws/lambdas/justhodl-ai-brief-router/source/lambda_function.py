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
from datetime import datetime, timezone

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
# Per-context worker — dispatches based on brief_type
# ─────────────────────────────────────────────────────────────────────
def generate_one_brief(ctx_id, cfg, episode_ref):
    """Generate the brief for a single context. Dispatches by brief_type."""
    # Per-name briefs use a totally different schema + prompt
    if cfg.get("brief_type") == "names":
        return generate_names_brief(ctx_id, cfg, episode_ref)
    # Default: regime brief (the original Tier-1/2/3 flow)
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
