"""
justhodl-auction-interpreter — Institutional Auction Brief AI Engine
======================================================================
Replaces the static 4-bucket Decisive Call on auctions.html with a
Claude-generated, crisis-KB-grounded institutional brief refreshed every 4h.

Inputs assembled per run:
  - Auction tape state          (data/auction-crisis.json)
  - Yield-curve regime         (data/yield-curve.json)
  - Dollar regime              (data/dollar-radar.json)
  - Eurodollar stress          (data/eurodollar-stress.json)
  - Credit spreads regime      (data/credit-spreads.json)
  - VIX/vol regime             (data/vix-curve.json)
  - Episode reference           (data/episode-reference.json)
  - Crisis KB framework excerpts (via jhcore.kb)

Output: data/auction-decisive-call.json — schema described below.

USES jhcore LAYER.

Schema:
{
  "version": "1.0",
  "generated_at": "...",
  "model": "claude-haiku-4-5-20251001",
  "regime": "RISK_ON_AGGRESSIVE | RISK_ON | NEUTRAL | RISK_OFF | CRISIS_PREP",
  "confidence": "HIGH | MEDIUM | LOW",
  "one_liner": "<single decisive sentence, ≤140 chars>",
  "thesis": "<3-5 sentence narrative citing specific numbers + composite + cross-regimes>",
  "supporting_evidence": [
     {"point": "<one fact>", "data": "<specific numbers>"}
  ],
  "historical_analogs": [
     {"period": "...", "similarity_pct": 87, "what_happened": "...", "expectation": "..."}
  ],
  "cross_asset": [
     {"asset": "...", "direction": "BULLISH|MIXED|NEUTRAL|CAUTION|BEARISH",
      "why": "...", "instruments": "..."}
  ],
  "trade_ideas": [
     {"setup": "...", "instrument": "...", "level": "...", "thesis_link": "...", "risk_reward": "..."}
  ],
  "tripwires": [
     {"condition": "<specific threshold>", "severity": "LOW|MEDIUM|HIGH", "action": "..."}
  ],
  "next_auctions_to_watch": [
     {"date": "...", "tenor": "...", "watch_thresholds": "...",
      "clean_signal_means": "...", "dirty_signal_means": "..."}
  ]
}
"""
import json
import os
import time
import urllib.request
from datetime import datetime, timezone

from jhcore import s3io, kb

# Local Claude wrapper with a longer timeout than jhcore.claude's 30s default.
# The historical_predictions section adds ~7 deeply-structured items so the
# response can take 40-70s with Haiku. We give it 90s of headroom.
def _claude_complete_json(prompt, system=None, max_tokens=5500, temperature=0.25,
                            model="claude-haiku-4-5-20251001", timeout=90):
    key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not key:
        print("[auction-interp] ANTHROPIC_API_KEY missing")
        return None
    body = {
        "model": model,
        "max_tokens": max_tokens,
        "temperature": temperature,
        "messages": [{"role": "user", "content": prompt + "\n\nRespond ONLY with valid JSON, no preamble or markdown."}],
    }
    if system:
        body["system"] = system
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
        print(f"[auction-interp] claude http err: {e}")
        return None
    parts = []
    for blk in data.get("content", []):
        if blk.get("type") == "text":
            parts.append(blk.get("text", ""))
    txt = "\n".join(parts).strip()
    if txt.startswith("```"):
        lines = [l for l in txt.split("\n") if not l.strip().startswith("```")]
        txt = "\n".join(lines).strip()
    try:
        return json.loads(txt)
    except Exception as e:
        print(f"[auction-interp] json parse err: {e} | head: {txt[:300]}")
        return None

REGIMES = ["RISK_ON_AGGRESSIVE", "RISK_ON", "NEUTRAL", "RISK_OFF", "CRISIS_PREP"]
CONFIDENCES = ["HIGH", "MEDIUM", "LOW"]
ASSET_DIRS = ["BULLISH", "MIXED", "NEUTRAL", "CAUTION", "BEARISH"]

ASSETS = [
    "US Equities (SPX/NDX)",
    "Credit (HY / IG)",
    "Crypto (BTC/ETH)",
    "Gold",
    "US Dollar (DXY)",
    "Duration / Bonds (TLT/IEF)",
]

KB_KEYWORDS = [
    "Treasury auction", "bid-to-cover", "indirect bidders", "primary dealer",
    "auction tail", "1979 Volcker", "2008 funding stress",
    "2020 COVID auctions", "Hugh Hendry", "issuance",
    "Reserve Bank Wednesday", "WALCL", "TGA",
]

# Cross-context data feeds (best-effort — Lambda continues if any missing)
CONTEXT_KEYS = {
    "yield_curve":   {"key": "data/yield-curve.json",       "regime_field": "regime"},
    "dollar":        {"key": "data/dollar-radar.json",      "regime_field": "regime"},
    "eurodollar":    {"key": "data/eurodollar-stress.json", "regime_field": "severity"},
    "credit":        {"key": "data/credit-spreads.json",    "regime_field": "regime"},
    "vix":           {"key": "data/vix-curve.json",         "regime_field": "composite_regime"},
}

SYSTEM_PROMPT = (
    "You are a senior macro/rates strategist at a top-tier hedge fund, writing "
    "the morning auction-tape brief for the firm's portfolio manager. You think "
    "like Druckenmiller — regime-aware, position-size-conscious, brutal about "
    "evidence vs narrative. You cite specific numbers (BTC ratios, Indirect %, AAH, "
    "tails in bps) and you compare to specific historical episodes (2024-Q1, "
    "2020-03, 2008-09, 1979). When you give a trade idea you name a specific "
    "instrument and a specific level. You never hand-wave. You flag divergences "
    "between the auction tape and the broader regime explicitly.\n\n"
    "CRITICAL — HISTORICAL PROJECTION DISCIPLINE: When you forecast risk-asset "
    "behaviour you ALWAYS anchor to a specific historical analog. You name the "
    "period, you cite what each asset DID during that analog (% moves over weeks), "
    "and you project a SIMILAR-SHAPE range — not exact prices. You quantify in "
    "percentage terms and time horizons (weeks), never invent dollar prices. You "
    "assign a probability to the projection and a confidence tier. You always give "
    "BOTH the upside trigger AND the downside scenario for every asset, because "
    "real PMs need to know what would invalidate the call."
)


def _safe_read(key, default=None):
    return s3io.get_json(key, default=default if default is not None else {})


def _fmt_indicator(d, name):
    """Format indicator summary from auction-crisis.json"""
    ind = (d.get("indicators") or {}).get(name, {})
    if not ind:
        return None
    return {
        "name": name,
        "score": ind.get("score"),
        "state": ind.get("state"),
        "value": ind.get("value"),
        "threshold": ind.get("threshold_short") or ind.get("threshold"),
    }


def _recent_auctions_summary(d, n=6):
    """Compact summary of recent auctions."""
    out = []
    for a in (d.get("recent_auctions") or [])[:n]:
        out.append({
            "date": a.get("auction_date") or a.get("date"),
            "tenor": a.get("tenor") or a.get("term"),
            "btc": a.get("bid_to_cover"),
            "indirect_pct": a.get("indirect_pct") or a.get("indirect_acceptance_pct"),
            "primary_dealer_pct": a.get("primary_dealer_pct") or a.get("aah_pct"),
            "tail_bps": a.get("tail_bps"),
        })
    return out


def build_prompt(auction, cross_regimes, kb_chunks, episode_ref):
    composite_score = auction.get("composite_score")
    regime = auction.get("regime")
    auctions_14d = auction.get("n_recent_auctions_14d")
    issuance = auction.get("issuance_anomaly") or {}
    fed_funds = auction.get("fed_funds_rate")

    indicators = []
    for ind_name in ["zero_rate_floor", "btc_extreme", "indirect_drought",
                      "aah_dependency", "tail_blowout", "ratio_inversion"]:
        i = _fmt_indicator(auction, ind_name)
        if i:
            indicators.append(i)

    recent = _recent_auctions_summary(auction, n=6)

    # Compact cross-context summary
    cross_lines = []
    for cid, ctx in cross_regimes.items():
        if ctx.get("regime"):
            cross_lines.append(f"  - {cid}: {ctx['regime']}")
            if ctx.get("note"):
                cross_lines[-1] += f"  ({ctx['note']})"
    cross_block = "\n".join(cross_lines) if cross_lines else "  (no cross-context regimes available)"

    # Compact episode-ref percentile context for primary indicators
    ep_lines = []
    for sid in ["T10Y2Y", "DGS10", "DFII10", "VIXCLS", "BAMLH0A0HYM2"]:
        ind = (episode_ref.get("indicators") or {}).get(sid)
        if ind:
            ne = ind.get("nearest_episode") or {}
            ep_lines.append(
                f"  - {ind.get('label', sid)}: {ind.get('current')}{ind.get('unit','')} "
                f"({ind.get('percentile')}th pctile, nearest analog: {ne.get('name','?')} [{ne.get('type','?')}])"
            )
    ep_block = "\n".join(ep_lines[:5]) if ep_lines else "  (episode-reference unavailable)"

    kb_block = ""
    if kb_chunks:
        kb_block = "\n\nRELEVANT MACRO FRAMEWORKS:\n" + "\n\n".join(
            f"### {c['framework']}\n{c['excerpt'][:1100]}" for c in kb_chunks
        )

    # Auctions calendar
    upcoming = auction.get("upcoming_auctions") or []
    upcoming_compact = []
    for u in upcoming[:8]:
        upcoming_compact.append({
            "date": u.get("auction_date") or u.get("date"),
            "tenor": u.get("tenor") or u.get("term") or u.get("security_type"),
            "amount_b": u.get("amount_billions") or u.get("size_billions"),
        })

    prompt = f"""# AUCTION TAPE BRIEF — INSTITUTIONAL ANALYSIS

## CURRENT STATE
- Composite score: {composite_score} / 100  (regime: {regime})
- Recent auctions (14d): {auctions_14d}
- Issuance anomaly: {issuance.get('pct_above_baseline')}% above baseline
- Fed funds rate: {fed_funds}%

## 6 CRISIS-PATTERN INDICATORS (auction-tape signatures)
{json.dumps(indicators, indent=2, default=str)}

## LAST 6 AUCTIONS (compact)
{json.dumps(recent, indent=2, default=str)}

## UPCOMING AUCTIONS (next ~14 days)
{json.dumps(upcoming_compact, indent=2, default=str)}

## CROSS-CONTEXT REGIMES (other markets right now)
{cross_block}

## EPISODE REFERENCE (where macro indicators sit vs history)
{ep_block}
{kb_block}

## TASK

Write the institutional Decisive Call brief in this JSON shape (and ONLY this JSON):

{{
  "regime": "<one of: {' | '.join(REGIMES)}>",
  "confidence": "<HIGH | MEDIUM | LOW>",
  "one_liner": "<single decisive sentence ≤140 chars — the call>",
  "thesis": "<3-5 sentence narrative. Cite specific numbers from above. Mention the cross-context regime where it confirms or contradicts the auction tape. Name historical analog explicitly.>",
  "supporting_evidence": [
    {{"point": "<one bullet>", "data": "<specific numbers>"}}
    // 4-6 items, each ≤25 words
  ],
  "historical_analogs": [
    {{"period": "<e.g. 2024 Q1-Q2>", "similarity_pct": <0-100>,
      "what_happened": "<≤30 words>", "expectation": "<≤30 words>"}}
    // 1-3 items
  ],
  "cross_asset": [
    {{"asset": "<one of the 6 listed below>", "direction": "<BULLISH|MIXED|NEUTRAL|CAUTION|BEARISH>",
      "why": "<≤30 words citing data>", "instruments": "<comma-separated tickers>"}}
    // exactly 6 items — one for each: {", ".join(ASSETS)}
  ],
  "trade_ideas": [
    {{"setup": "<≤25 words>", "instrument": "<specific ticker/instrument>",
      "level": "<specific entry / strike / spread>",
      "thesis_link": "<how it ties to the call, ≤20 words>",
      "risk_reward": "<e.g. 1:3>"}}
    // 3-5 items, each must be ACTIONABLE with a real instrument + level
  ],
  "tripwires": [
    {{"condition": "<specific numeric threshold>",
      "severity": "<LOW|MEDIUM|HIGH>",
      "action": "<≤20 words>"}}
    // 3-5 items, each tripwire must have a specific numeric threshold
  ],
  "next_auctions_to_watch": [
    {{"date": "<YYYY-MM-DD>", "tenor": "<e.g. 10Y reopen>",
      "watch_thresholds": "<specific BTC/Indirect/AAH/tail thresholds>",
      "clean_signal_means": "<≤25 words>",
      "dirty_signal_means": "<≤25 words>"}}
    // 1-3 items from the upcoming-auctions list above
  ],
  "historical_predictions": [
    // 6-7 items COVERING — in this order — these specific assets:
    //   1. "US Equities (SPX/NDX)"   ticker SPX
    //   2. "Bitcoin (BTC)"            ticker BTC
    //   3. "Ethereum (ETH)"           ticker ETH
    //   4. "Crypto Total Market"      ticker TOTAL
    //   5. "High-Yield Credit"        ticker HYG
    //   6. "Gold"                     ticker GLD
    //   7. "US Dollar (DXY)"          ticker DXY
    {{"asset": "<one of the 7 above>",
      "ticker": "<SPX|BTC|ETH|TOTAL|HYG|GLD|DXY>",
      "best_analog_period": "<specific period e.g. '2024 Q1-Q2' or '2020-03 COVID' or '2008-09 GFC'>",
      "analog_outcome_summary": "<what THIS asset did during that analog — concrete % move and weeks, ≤40 words>",
      "prediction_direction": "<UPSIDE | DOWNSIDE | SIDEWAYS>",
      "prediction_range_low_pct": <number — e.g. 15 means +15%; for downside use negative numbers>,
      "prediction_range_high_pct": <number — wider end of the range, same sign convention as low>,
      "prediction_horizon_weeks": <integer — 4, 8, 12, 16, 24>,
      "confidence": "<HIGH | MEDIUM | LOW>",
      "probability_pct": <integer 0-100 — probability the predicted range holds>,
      "upside_trigger": "<one specific numeric data condition that would confirm UPSIDE, ≤25 words>",
      "downside_scenario_pct": <number — worst-case % move if downside triggers e.g. -25>,
      "downside_trigger": "<one specific numeric data condition that would cause downside, ≤25 words>",
      "key_reasoning": "<why THIS analog applies given current auction-tape + macro state, ≤30 words>"
    }}
  ]
}}

## RULES (institutional standard, NO exceptions)
- Cite REAL numbers — pull them from the data above. Don't invent.
- Trade ideas MUST name a real ticker and a real level. No "watch for stress" or
  "consider rotating to defensives". Be specific: "TLT calls above 92, expiry Jul 18".
- Tripwires MUST have numeric thresholds. "If composite > 30 for 2 consecutive
  readings, trim equity beta by 25%" — not "if conditions worsen".
- Historical predictions MUST anchor to a SPECIFIC named historical period.
  Project in % terms over weeks, NEVER invent dollar prices. Probability must
  reflect honesty about uncertainty — 50-60% is normal for forward views, 80%+
  is exceptional. For Bitcoin/crypto specifically, lean on cycle analogs
  (2017-Q4 top, 2019-Q1 bottom, 2020-Q4 launch, 2022-Q2 capitulation, 2023-Q1
  recovery, 2024-Q1 ATH rally) — name the closest one explicitly.
- If auction-tape and cross-regimes diverge, flag the divergence explicitly in
  the thesis and explain which to trust and why.
- confidence=LOW is appropriate when data is sparse or signals conflict — don't
  force HIGH if the evidence isn't there.
- Stay under ~900 words total content."""

    return prompt


def lambda_handler(event=None, context=None):
    started = time.time()
    print("[auction-interp] starting")

    auction = _safe_read("data/auction-crisis.json")
    if not auction:
        return {"statusCode": 503,
                "body": json.dumps({"err": "auction-crisis.json not readable"})}

    # Cross-context regimes
    cross_regimes = {}
    for cid, conf in CONTEXT_KEYS.items():
        d = _safe_read(conf["key"])
        if d:
            cross_regimes[cid] = {
                "regime": d.get(conf["regime_field"]),
                "note": d.get("interpretation") or d.get("summary") or "",
            }

    episode_ref = _safe_read("data/episode-reference.json")
    kb_chunks = kb.lookup(KB_KEYWORDS, max_chunks=3)

    prompt = build_prompt(auction, cross_regimes, kb_chunks, episode_ref)
    print(f"[auction-interp] prompt {len(prompt)} chars, kb_chunks={len(kb_chunks)}, cross={len(cross_regimes)}")

    brief = _claude_complete_json(prompt,
                                    system=SYSTEM_PROMPT,
                                    max_tokens=5500,
                                    temperature=0.25,
                                    timeout=90)

    if not brief:
        return {"statusCode": 502,
                "body": json.dumps({"err": "claude returned no parseable JSON",
                                      "elapsed": round(time.time() - started, 2)})}

    # Tag + write
    brief["version"] = "1.1"
    brief["generated_at"] = datetime.now(timezone.utc).isoformat()
    brief["model"] = "claude-haiku-4-5-20251001"
    brief["context"] = "auctions"
    brief["input_state"] = {
        "auction_composite_score": auction.get("composite_score"),
        "auction_regime": auction.get("regime"),
        "cross_regimes": {k: v.get("regime") for k, v in cross_regimes.items()},
        "kb_frameworks_used": [c.get("framework") for c in kb_chunks],
    }

    s3io.put_json("data/auction-decisive-call.json", brief,
                   cache_control="public, max-age=900")

    duration = round(time.time() - started, 2)
    print(f"[auction-interp] OK — regime={brief.get('regime')} confidence={brief.get('confidence')} "
          f"trades={len(brief.get('trade_ideas') or [])} tripwires={len(brief.get('tripwires') or [])} "
          f"predictions={len(brief.get('historical_predictions') or [])} {duration}s")

    return {"statusCode": 200, "body": json.dumps({
        "regime": brief.get("regime"),
        "confidence": brief.get("confidence"),
        "one_liner": brief.get("one_liner"),
        "n_evidence": len(brief.get("supporting_evidence") or []),
        "n_analogs": len(brief.get("historical_analogs") or []),
        "n_cross_asset": len(brief.get("cross_asset") or []),
        "n_trades": len(brief.get("trade_ideas") or []),
        "n_tripwires": len(brief.get("tripwires") or []),
        "n_predictions": len(brief.get("historical_predictions") or []),
        "duration_s": duration,
    })}
