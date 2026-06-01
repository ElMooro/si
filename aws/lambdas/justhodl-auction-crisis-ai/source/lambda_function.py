"""
justhodl-auction-crisis-ai
═══════════════════════════
Reads the rich auction-crisis.json (v2 schema) produced by
justhodl-auction-crisis-detector, builds a structured prompt for Claude,
and writes a narrative + forward-prediction document to S3.

OUTPUT: s3://justhodl-dashboard-live/data/auction-crisis-ai.json

SCHEMA
══════
{
  "schema_version":   "1.0",
  "generated_at":     "...",
  "data_generated_at": "...",  # links to the source data file
  "model":             "claude-haiku-4-5-20251001",
  "regime":            "CALM",
  "composite":         9.3,
  "ai_commentary": {
      "executive_summary":     "...",          # 3-sentence top-level read
      "what_changed":          "...",          # 14-day delta narrative
      "indicator_interpretation": [            # per-firing-signal
          {"signal": "...", "narrative": "...", "implication": "..."}
      ],
      "historical_analog_discussion": "...",   # context for top match
      "forward_predictions": [                 # per upcoming auction
          {"auction_date": "...", "tenor": "...", "term": "...",
           "predicted_score": 22, "expected_outcome": "...",
           "what_to_watch": "..."}
      ],
      "tail_risk_assessment": "...",
      "actionable_triggers":  "...",
      "decisive_call":        "..."            # 1-line "do this"
  }
}

DESIGN
══════
- 1 Claude API call per invocation (max_tokens ~3500)
- Reads data/auction-crisis.json, never re-computes; pure narrative layer
- Schedule: hourly cron, 10 min offset from main Lambda
- If source data is stale (>2h) or AI call fails, write a degraded
  payload with status="stale_source" or "ai_error"
- Anthropic key from env (ANTHROPIC_API_KEY)
"""
import json
import os
import re
import sys
import time
import urllib.request
from datetime import datetime, timezone

import boto3

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _sentry_lite import track_errors  # noqa: F401  (auto-installs error tracking)

S3_BUCKET = "justhodl-dashboard-live"
INPUT_KEY = "data/auction-crisis.json"
OUTPUT_KEY = "data/auction-crisis-ai.json"
MODEL = "claude-haiku-4-5-20251001"

ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

s3 = boto3.client("s3", region_name="us-east-1")


# ═════════════════════════════════════════════════════════════════════
# Prompt engineering — the key value-add of this Lambda
# ═════════════════════════════════════════════════════════════════════

SYSTEM_PROMPT = """You are a senior fixed-income strategist at a hedge fund \
producing a narrative analysis layer on top of a Treasury auction crisis \
detector. Your audience is sophisticated investors (PMs, CIOs, traders) \
who need to know:

1. What's happening in Treasury auctions RIGHT NOW
2. What the data PATTERN means (not just the numbers)
3. What's likely to happen NEXT (concrete, actionable)
4. What to DO (positioning, hedges, triggers)

STYLE RULES
───────────
- Direct, decisive, no hedging clichés ("it depends", "time will tell")
- Use concrete numbers when they support a point
- Reference historical episodes by name (Sep 2008, Mar 2020, etc.)
- Plain English — not jargon-dense
- Avoid "could", "may", "might" — make a CALL even on probability claims
- Each section is concise: executive summary 3 sentences max,
  individual narratives 2-4 sentences each
- For forward predictions, give a specific expected outcome
- ABSOLUTELY DO NOT include preambles like "Here is the analysis"

OUTPUT FORMAT
─────────────
Return a JSON object MATCHING THIS EXACT STRUCTURE (no preamble, no \
markdown, just the JSON):

{
  "executive_summary":            "...",
  "what_changed":                 "...",
  "indicator_interpretation": [
    {"signal": "...", "narrative": "...", "implication": "..."}
  ],
  "historical_analog_discussion": "...",
  "forward_predictions": [
    {"auction_date": "...", "tenor": "...", "term": "...",
     "predicted_score": 0, "expected_outcome": "...",
     "what_to_watch": "..."}
  ],
  "tail_risk_assessment":         "...",
  "actionable_triggers":          "...",
  "decisive_call":                "..."
}

For forward_predictions: include up to 5 entries chosen from the \
forward_calendar — prioritize the largest auctions, highest-stress \
forecasts, and most diagnostic tenors (long coupons + TIPS).

The decisive_call MUST be a single sentence prescribing concrete action.\
"""


def build_user_prompt(data: dict) -> str:
    """Format the auction-crisis.json data into a structured user prompt."""
    # Pull only the most decision-relevant fields
    regime = data.get("regime", "?")
    composite = data.get("composite_score", "?")
    n_recent = data.get("n_recent_auctions_14d", 0)
    fed_rate = data.get("fed_funds_rate")
    issuance = data.get("issuance_anomaly") or {}
    interp = data.get("interpretation", "")

    tenor = data.get("tenor_decomposition") or {}
    forward = data.get("forward_calendar") or []
    analog = data.get("historical_analog") or {}
    cross = data.get("cross_signals") or {}
    history = data.get("composite_history") or {}
    tail = data.get("tail_risk") or {}
    triggers = data.get("triggers") or []
    indicator_agg = data.get("indicator_aggregate_14d") or {}
    recent = data.get("recent_auctions") or []
    freshness = data.get("freshness") or {}

    parts = []
    parts.append("# TREASURY AUCTION CRISIS DATA — current snapshot\n")
    parts.append(f"Regime: **{regime}** | Composite: {composite}/100 | "
                  f"n_recent_14d={n_recent} | Fed funds: "
                  f"{fed_rate:.2f}% | Generated: {data.get('generated_at','')}")
    parts.append(f"\nSystem interpretation (baseline): {interp}\n")
    parts.append(f"Issuance anomaly: {issuance.get('pct_above_baseline','?')}% above baseline "
                  f"(score {issuance.get('score','?')}).\n")
    parts.append(f"Freshness: latest auction {freshness.get('latest_auction_date','?')} "
                  f"({freshness.get('hours_since_latest_auction','?')}h ago).\n")

    # Indicator aggregate
    if indicator_agg:
        parts.append("\n## INDICATORS FIRING IN LAST 14 DAYS (score ≥ 50)\n")
        for sig, ag in indicator_agg.items():
            parts.append(f"  - {sig}: {ag.get('n_fired',0)} auctions fired, "
                          f"max score {ag.get('max_score',0)}")
    else:
        parts.append("\n## NO INDICATORS FIRING IN LAST 14 DAYS\n")

    # Tenor decomposition
    parts.append("\n## STRESS BY TENOR (14-day)\n")
    for tenor_name, td in tenor.items():
        if td.get("n_auctions"):
            parts.append(f"  - {td.get('label','?')}: composite "
                          f"{td.get('composite','?')}/100, "
                          f"{td.get('n_auctions',0)} auctions, "
                          f"dominant={td.get('dominant_signal','none')}, "
                          f"rank={td.get('rank','?')}")

    # Cross signals
    parts.append("\n## CROSS-SIGNALS (corroborating context)\n")
    if cross.get("repo_stress", {}).get("spread_bp") is not None:
        rs = cross["repo_stress"]
        parts.append(f"  - Repo (SOFR-IORB): {rs.get('spread_bp')}bp "
                      f"[{rs.get('regime','?')}] — {rs.get('interpretation','')}")
    if cross.get("dollar_strength", {}).get("change_30d_pct") is not None:
        ds = cross["dollar_strength"]
        parts.append(f"  - USD trade-weighted: level {ds.get('level','?')}, "
                      f"{ds.get('change_30d_pct',0):+.1f}% / 30d "
                      f"[{ds.get('regime','?')}]")
    if cross.get("curve_slope", {}).get("spread_bp") is not None:
        cs = cross["curve_slope"]
        parts.append(f"  - 10y-2y curve: {cs.get('spread_bp')}bp [{cs.get('regime','?')}]")
    if cross.get("inflation_expectations", {}).get("rate_pct") is not None:
        ie = cross["inflation_expectations"]
        parts.append(f"  - 5y5y forward inflation BE: {ie.get('rate_pct')}% "
                      f"[{ie.get('regime','?')}]")

    # Historical analog
    parts.append("\n## HISTORICAL ANALOG MATCHING\n")
    if analog.get("top_matches"):
        parts.append(f"Current 6D vector: {analog.get('current_vector')} "
                      f"(labels: {analog.get('vector_labels')})")
        parts.append("Top 3 historical analogs:")
        for m in analog["top_matches"]:
            parts.append(f"  - {m.get('date','?')} ({m.get('regime','?')}): "
                          f"similarity={m.get('similarity',0):.3f}")
            parts.append(f"      Context: {m.get('context','')}")
            parts.append(f"      What happened next: {m.get('what_happened_next','')}")

    # Composite history
    if history.get("current"):
        cur = history.get("current")
        parts.append(f"\n## 30-DAY COMPOSITE TRAJECTORY\n")
        parts.append(f"  Current: {cur.get('composite')}/100 [{cur.get('regime','?')}]")
        parts.append(f"  Range last 30d: min={history.get('min_composite')}, max={history.get('max_composite')}")
        cps = history.get("change_points", [])
        if cps:
            parts.append(f"  Regime change points: {len(cps)}")
            for cp in cps[:3]:
                parts.append(f"    - {cp.get('date')}: {cp.get('from')} → {cp.get('to')}")
        else:
            parts.append("  No regime changes detected in last 30 days.")

    # Forward calendar
    parts.append("\n## FORWARD CALENDAR (next ~30 days of upcoming auctions)\n")
    if forward:
        # Prioritize: highest forecast_score and largest auctions
        sorted_fwd = sorted(forward,
                              key=lambda x: (-(x.get("forecast", {}).get("forecast_score") or 0),
                                              -(x.get("offering_amount_billions") or 0)))[:8]
        for f in sorted_fwd:
            fc = f.get("forecast", {})
            parts.append(f"  - {f.get('auction_date','?')} ({f.get('days_ahead','?')}d) "
                          f"{f.get('security_term','?')} {f.get('security_type','?')}: "
                          f"forecast {fc.get('forecast_score','?')} [{fc.get('forecast_label','?')}], "
                          f"size ${f.get('offering_amount_billions','?')}B, "
                          f"confidence {fc.get('confidence','?')}")
            parts.append(f"      {fc.get('narrative','')}")
    else:
        parts.append("  (No upcoming auctions data available)")

    # Tail risk
    parts.append("\n## TAIL RISK PROBABILITIES (forward-looking)\n")
    for k, label in [("p_failed_auction_30d",  "Failed auction in next 30d"),
                       ("p_regime_escalation_14d", "Regime escalation in next 14d"),
                       ("p_supply_volatility_30d", "Supply-driven vol spike in next 30d")]:
        if tail.get(k):
            tr = tail[k]
            parts.append(f"  - {label}: ~{tr.get('probability','?')}% — {tr.get('interpretation','')}")

    # Triggers
    parts.append("\n## ACTIVE TRIGGERS\n")
    for t in triggers[:5]:
        parts.append(f"  - {t.get('name','?')}: current={t.get('current','?')}, "
                      f"threshold={t.get('threshold','?')}, "
                      f"distance={t.get('distance','?')}, "
                      f"urgency={t.get('urgency','?')}")
        parts.append(f"      Action: {t.get('action','')}")

    # Last auction detail
    if recent:
        latest = recent[0]
        parts.append(f"\n## LATEST AUCTION DETAIL\n")
        parts.append(f"  Date: {latest.get('auction_date')}, "
                      f"{latest.get('security_term')} {latest.get('security_type')}, "
                      f"CUSIP {latest.get('cusip')}")
        parts.append(f"  Bid-to-cover: {latest.get('btc')}, "
                      f"high rate: {latest.get('high_rate')}%, "
                      f"AAH: {latest.get('allocated_at_high_pct')}%, "
                      f"PD share: {latest.get('primary_dealer_pct'):.1f}%, "
                      f"indirect share: {latest.get('indirect_pct'):.1f}%")
        parts.append(f"  Composite score for this auction: {latest.get('composite_score')}/100")

    parts.append("\n\nNow produce the JSON narrative analysis per the system prompt format.")
    return "\n".join(parts)


def call_anthropic(system: str, user: str, max_tokens: int = 4000) -> str:
    """Call Anthropic API, return the text response. Raises on failure."""
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
            "Content-Type":     "application/json",
            "x-api-key":        ANTHROPIC_KEY,
            "anthropic-version": "2023-06-01",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=60) as r:
        data = json.loads(r.read().decode("utf-8"))
    if not data.get("content"):
        raise RuntimeError(f"Empty response from Anthropic: {data}")
    # Concatenate all text content blocks (usually just 1)
    text = ""
    for block in data["content"]:
        if block.get("type") == "text":
            text += block.get("text", "")
    return text.strip()


def extract_json(text: str) -> dict:
    """Pull the first balanced JSON object from response. Handles models
    that occasionally wrap output in markdown fences or add commentary."""
    # Strip markdown code fences if present
    text = re.sub(r"^```(?:json)?\s*", "", text.strip(), flags=re.MULTILINE)
    text = re.sub(r"\s*```\s*$", "", text.strip(), flags=re.MULTILINE)
    # Find first { ... } balanced block
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
    # Fall back to literal parse
    return json.loads(text)


def lambda_handler(event, context):
    t0 = time.time()
    print(f"[auction-crisis-ai] start {datetime.now(timezone.utc).isoformat()}")

    # 1. Read source data
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=INPUT_KEY)
        source_data = json.loads(obj["Body"].read())
        source_modified = obj["LastModified"]
    except Exception as e:
        print(f"[ai] source read err: {e}")
        return _write_error(f"source read error: {e}")

    # Stale check — source must be < 2h old
    age_s = (datetime.now(timezone.utc) - source_modified).total_seconds()
    if age_s > 7200:
        return _write_error(f"source is {age_s/3600:.1f}h stale (> 2h limit)",
                             source_modified=source_modified.isoformat())

    if source_data.get("schema_version", "0") < "2.0":
        return _write_error(
            f"source schema {source_data.get('schema_version')} pre-v2; "
            "AI commentary requires schema 2.0+",
            source_modified=source_modified.isoformat(),
        )

    print(f"[ai] source schema {source_data.get('schema_version')}, "
            f"age {age_s/60:.1f}min, regime {source_data.get('regime')}, "
            f"composite {source_data.get('composite_score')}")

    # 2. Build prompt
    user_prompt = build_user_prompt(source_data)
    print(f"[ai] prompt {len(user_prompt)} chars")

    # 3. Call Claude
    try:
        t_claude = time.time()
        response_text = call_anthropic(SYSTEM_PROMPT, user_prompt, max_tokens=4000)
        claude_elapsed = round(time.time() - t_claude, 2)
        print(f"[ai] Claude response in {claude_elapsed}s, {len(response_text)} chars")
    except Exception as e:
        print(f"[ai] Claude err: {e}")
        return _write_error(f"Claude API error: {e}",
                             source_modified=source_modified.isoformat())

    # 4. Parse JSON
    try:
        commentary = extract_json(response_text)
    except Exception as e:
        print(f"[ai] JSON parse err: {e}, raw text preview: {response_text[:400]}")
        return _write_error(
            f"failed to parse Claude response as JSON: {e}",
            source_modified=source_modified.isoformat(),
            raw_response_preview=response_text[:1000],
        )

    # 5. Validate minimum required structure
    required = ["executive_summary", "decisive_call"]
    missing = [k for k in required if k not in commentary]
    if missing:
        return _write_error(
            f"AI response missing required keys: {missing}",
            source_modified=source_modified.isoformat(),
            partial_commentary=commentary,
        )

    # 6. Build + write output
    output = {
        "schema_version":   "1.0",
        "generated_at":     datetime.now(timezone.utc).isoformat(),
        "data_generated_at": source_data.get("generated_at"),
        "data_last_modified": source_modified.isoformat(),
        "data_age_minutes": round(age_s / 60, 1),
        "model":            MODEL,
        "regime":           source_data.get("regime"),
        "composite":        source_data.get("composite_score"),
        "elapsed_sec":      round(time.time() - t0, 2),
        "claude_elapsed_sec": claude_elapsed,
        "ai_commentary":    commentary,
        # Echo a few key data points for the UI to display alongside
        "data_echo": {
            "n_recent_auctions_14d":  source_data.get("n_recent_auctions_14d"),
            "fed_funds_rate":         source_data.get("fed_funds_rate"),
            "latest_auction_date":    (source_data.get("freshness") or {}).get("latest_auction_date"),
            "n_upcoming_30d":         len(source_data.get("forward_calendar") or []),
            "top_analog_date":        (((source_data.get("historical_analog") or {})
                                          .get("top_matches") or [{}])[0].get("date")),
            "top_analog_regime":      (((source_data.get("historical_analog") or {})
                                          .get("top_matches") or [{}])[0].get("regime")),
            "top_analog_similarity":  (((source_data.get("historical_analog") or {})
                                          .get("top_matches") or [{}])[0].get("similarity")),
        },
    }

    body = json.dumps(output, indent=2, default=str)
    s3.put_object(
        Bucket=S3_BUCKET, Key=OUTPUT_KEY, Body=body,
        ContentType="application/json", CacheControl="max-age=600",
    )
    archive_key = f"data/archive/auction-crisis-ai/{datetime.now(timezone.utc).strftime('%Y%m%d_%H')}.json"
    s3.put_object(Bucket=S3_BUCKET, Key=archive_key, Body=body,
                    ContentType="application/json")

    # ═══════════════════════════════════════════════════════════════════
    # TELEGRAM REGIME-TRANSITION ALERT LAYER
    # Compares current state to prior alert state; sends a Telegram
    # message ONLY when an actionable transition is detected. State is
    # tracked in data/auction-crisis-alert-state.json.
    # ═══════════════════════════════════════════════════════════════════
    alerts = []
    try:
        alerts = maybe_send_alerts(source_data, commentary)
        output["alerts_sent"] = alerts
    except Exception as e:
        print(f"[alerts] error (non-fatal): {e}")
        output["alerts_error"] = str(e)[:160]

    summary = {
        "status":         "ok",
        "elapsed_sec":    output["elapsed_sec"],
        "claude_elapsed": claude_elapsed,
        "regime":         output["regime"],
        "composite":      output["composite"],
        "executive_summary_chars": len(commentary.get("executive_summary", "")),
        "forward_predictions_count": len(commentary.get("forward_predictions") or []),
        "alerts_sent":    len(alerts),
        "alert_types":    [a.get("type") for a in alerts],
    }
    print(f"[auction-crisis-ai] done: {summary}")
    return {"statusCode": 200, "body": json.dumps(summary)}


def _write_error(message: str, **extras) -> dict:
    """Write a degraded payload so the UI knows to show a notice."""
    error_payload = {
        "schema_version": "1.0",
        "generated_at":   datetime.now(timezone.utc).isoformat(),
        "status":         "error",
        "error":          message,
        **extras,
    }
    try:
        s3.put_object(
            Bucket=S3_BUCKET, Key=OUTPUT_KEY,
            Body=json.dumps(error_payload, default=str, indent=2),
            ContentType="application/json", CacheControl="max-age=300",
        )
    except Exception as e:
        print(f"[ai] failed to write error payload: {e}")
    print(f"[auction-crisis-ai] ERROR: {message}")
    return {"statusCode": 500, "body": json.dumps({"status": "error", "error": message})}


# ═════════════════════════════════════════════════════════════════════
# TELEGRAM ALERT LAYER
# State tracking + transition detection. Sends a Telegram message ONLY
# when a meaningful transition is detected. Avoids alert fatigue by
# requiring CHANGE in state, not just current high-stress levels.
# ═════════════════════════════════════════════════════════════════════

TELEGRAM_TOKEN   = "8679881066:AAHTE6TAhDqs0FuUelTL6Ppt1x8ihis1aGs"
TELEGRAM_CHAT_ID = "8678089260"
ALERT_STATE_KEY  = "data/auction-crisis-alert-state.json"

REGIME_RANK = {"CALM": 0, "WATCH": 1, "ELEVATED": 2, "ACUTE_STRESS": 3}


def compute_alert_state(data: dict) -> dict:
    """Distill source data into the minimal state needed for transition detection."""
    state = {
        "regime":           data.get("regime"),
        "composite":        data.get("composite_score"),
        "indicator_max":    {},
        "tail_p":           {},
        "snapshot_at":      datetime.now(timezone.utc).isoformat(),
    }
    for sig, agg in (data.get("indicator_aggregate_14d") or {}).items():
        state["indicator_max"][sig] = agg.get("max_score", 0)
    for k in ("p_failed_auction_30d", "p_regime_escalation_14d", "p_supply_volatility_30d"):
        state["tail_p"][k] = ((data.get("tail_risk") or {}).get(k) or {}).get("probability", 0)
    return state


def detect_transitions(prior_state: dict, current_state: dict) -> list:
    """Return a list of alert events."""
    alerts = []
    if not prior_state:
        # First-ever run: send 'system initialized' notification
        return [{
            "type":      "SYSTEM_INITIALIZED",
            "current_regime": current_state.get("regime"),
            "composite": current_state.get("composite"),
        }]

    # 1. Regime transitions
    cur_r = current_state.get("regime")
    prior_r = prior_state.get("regime")
    if cur_r != prior_r and cur_r and prior_r:
        cur_rank = REGIME_RANK.get(cur_r, 0)
        prior_rank = REGIME_RANK.get(prior_r, 0)
        if cur_rank > prior_rank:
            alerts.append({
                "type": "REGIME_ESCALATION",
                "from": prior_r,
                "to":   cur_r,
                "composite": current_state.get("composite"),
            })
        elif cur_rank < prior_rank:
            alerts.append({
                "type": "REGIME_DEESCALATION",
                "from": prior_r,
                "to":   cur_r,
                "composite": current_state.get("composite"),
            })

    # 2. New indicator firing at score >= 70 (was below 70)
    THRESH = 70
    cur_ind = current_state.get("indicator_max", {})
    prior_ind = prior_state.get("indicator_max", {})
    for sig, cur_max in cur_ind.items():
        prior_max = prior_ind.get(sig, 0)
        if cur_max >= THRESH and prior_max < THRESH:
            alerts.append({
                "type":      "INDICATOR_FIRED",
                "signal":    sig,
                "max_score": cur_max,
                "prior_max": prior_max,
            })
        elif cur_max < THRESH and prior_max >= THRESH:
            alerts.append({
                "type":      "INDICATOR_CLEARED",
                "signal":    sig,
                "max_score": cur_max,
                "prior_max": prior_max,
            })

    # 3. Tail risk probability crossing 50%
    TAIL_THRESH = 50
    cur_tail = current_state.get("tail_p", {})
    prior_tail = prior_state.get("tail_p", {})
    for key, cur_p in cur_tail.items():
        prior_p = prior_tail.get(key, 0)
        if cur_p >= TAIL_THRESH and prior_p < TAIL_THRESH:
            alerts.append({
                "type":        "TAIL_RISK_CROSSED",
                "key":         key,
                "probability": cur_p,
                "prior":       prior_p,
            })
        elif cur_p < TAIL_THRESH and prior_p >= TAIL_THRESH:
            alerts.append({
                "type":        "TAIL_RISK_CLEARED",
                "key":         key,
                "probability": cur_p,
                "prior":       prior_p,
            })

    # 4. Composite jump > 15 in either direction (rapid move)
    cur_c = current_state.get("composite", 0) or 0
    prior_c = prior_state.get("composite", 0) or 0
    delta = cur_c - prior_c
    if abs(delta) >= 15:
        alerts.append({
            "type":     "COMPOSITE_JUMP",
            "from":     prior_c,
            "to":       cur_c,
            "delta":    delta,
            "direction": "up" if delta > 0 else "down",
        })

    return alerts


def format_telegram_message(alerts: list, commentary: dict, data: dict) -> str:
    """Compose a Markdown Telegram message."""
    lines = []
    # Header
    has_escalation = any(a["type"] in ("REGIME_ESCALATION", "INDICATOR_FIRED", "TAIL_RISK_CROSSED") for a in alerts)
    has_deescal   = any(a["type"] in ("REGIME_DEESCALATION", "INDICATOR_CLEARED", "TAIL_RISK_CLEARED") for a in alerts)
    if has_escalation:
        lines.append("🚨 *Treasury Auction Crisis Alert* 🚨")
    elif has_deescal:
        lines.append("✅ *Treasury Auction — Stress Easing*")
    else:
        lines.append("📊 *Treasury Auction System Notice*")
    lines.append("")

    for a in alerts:
        t = a["type"]
        if t == "SYSTEM_INITIALIZED":
            lines.append(f"🆕 System ONLINE — alert layer initialized.")
            lines.append(f"   Regime: *{a['current_regime']}* · composite {a['composite']:.1f}/100")
        elif t == "REGIME_ESCALATION":
            lines.append(f"⚠️ Regime ESCALATED: *{a['from']} → {a['to']}* (composite {a['composite']:.1f})")
        elif t == "REGIME_DEESCALATION":
            lines.append(f"✅ Regime DEESCALATED: *{a['from']} → {a['to']}* (composite {a['composite']:.1f})")
        elif t == "INDICATOR_FIRED":
            sig = a["signal"].replace("_", " ").title()
            lines.append(f"🔴 *{sig}* fired: max score {a['max_score']} (was {a['prior_max']})")
        elif t == "INDICATOR_CLEARED":
            sig = a["signal"].replace("_", " ").title()
            lines.append(f"🟢 *{sig}* cleared: max score {a['max_score']} (was {a['prior_max']})")
        elif t == "TAIL_RISK_CROSSED":
            k = a["key"].replace("p_", "").replace("_", " ")
            lines.append(f"📈 Tail risk *{k}* crossed 50%: {a['probability']:.0f}% (was {a['prior']:.0f}%)")
        elif t == "TAIL_RISK_CLEARED":
            k = a["key"].replace("p_", "").replace("_", " ")
            lines.append(f"📉 Tail risk *{k}* cleared 50%: {a['probability']:.0f}% (was {a['prior']:.0f}%)")
        elif t == "COMPOSITE_JUMP":
            arrow = "↗️" if a["direction"] == "up" else "↘️"
            lines.append(f"{arrow} Composite jumped: {a['from']:.1f} → {a['to']:.1f} ({a['delta']:+.1f} in 1h)")

    # Add AI decisive call if present and we're escalating
    if has_escalation or any(a["type"] == "SYSTEM_INITIALIZED" for a in alerts):
        dc = (commentary or {}).get("decisive_call", "")
        if dc:
            lines.append("")
            lines.append(f"_Decisive call_: {dc[:600]}")

    lines.append("")
    lines.append(f"🔗 https://justhodl.ai/auction-crisis.html")
    return "\n".join(lines)


def send_telegram(text: str) -> bool:
    """Send a Markdown-formatted message via Telegram bot. Returns success bool."""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = json.dumps({
        "chat_id": TELEGRAM_CHAT_ID,
        "text":    text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": False,
    }).encode("utf-8")
    try:
        req = urllib.request.Request(
            url, data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            response = json.loads(r.read().decode("utf-8"))
            return bool(response.get("ok"))
    except Exception as e:
        print(f"[telegram] send error: {e}")
        return False


def maybe_send_alerts(source_data: dict, commentary: dict) -> list:
    """Main alert orchestration: load prior state, compute new state, detect transitions, send Telegram."""
    current_state = compute_alert_state(source_data)

    # Load prior state (None if first run)
    prior_state = None
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=ALERT_STATE_KEY)
        prior_state = json.loads(obj["Body"].read())
    except s3.exceptions.NoSuchKey:
        pass
    except Exception as e:
        print(f"[alerts] prior state load error: {e}")

    # Detect transitions
    alerts = detect_transitions(prior_state, current_state)
    print(f"[alerts] detected {len(alerts)} transition(s): {[a.get('type') for a in alerts]}")

    # Send Telegram if any alerts
    if alerts:
        msg = format_telegram_message(alerts, commentary, source_data)
        sent_ok = send_telegram(msg)
        print(f"[alerts] telegram sent: {sent_ok}")
        for a in alerts:
            a["telegram_sent"] = sent_ok

    # Save current state ALWAYS (so next run has comparison point)
    try:
        s3.put_object(
            Bucket=S3_BUCKET, Key=ALERT_STATE_KEY,
            Body=json.dumps(current_state, indent=2, default=str),
            ContentType="application/json",
        )
    except Exception as e:
        print(f"[alerts] state save error: {e}")

    return alerts
