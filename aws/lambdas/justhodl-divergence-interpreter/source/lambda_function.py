"""
justhodl-divergence-interpreter — Regime-conditional divergence analysis.

WHY THIS MATTERS
────────────────
Raw divergence z-scores are useful but missing CONTEXT. The same -3σ in
heavy_trucks_spy means very different things depending on regime:

  • In STRONG EXPANSION → likely noise, easily mean-reverts
  • In MUDDLE          → moderate concern, trim risk
  • In SLOWING         → high-conviction recession signal, raise cash
  • In CONTRACTION     → already priced in, look for capitulation lows

This Lambda reads the divergence-v2.json output PLUS macro-nowcast.json,
then uses Claude to produce a regime-aware narrative interpretation:
  - What the top divergences mean RIGHT NOW given the regime
  - Which divergences confirm vs contradict the regime
  - Specific actionable tilts (sectors, geographies, factors)
  - Confidence level + which signals would change the read

INPUTS
──────
  s3://justhodl-dashboard-live/data/divergence-v2.json    (70 cross-asset pairs)
  s3://justhodl-dashboard-live/data/macro-nowcast.json    (current regime)

OUTPUT
──────
  s3://justhodl-dashboard-live/data/divergence-interpreted.json

SCHEDULE
────────
  rate(4 hours) — runs 30 min after divergence-engine-v2 to ensure
  fresh data (engine runs every 2h)

TELEGRAM
────────
  Sends interpretation summary on regime-aware HIGH CONVICTION calls only:
    - When >2 crisis_leading divergences are extreme AND regime is SLOWING+
    - When divergences contradict the regime (regime change incoming?)
    - When the cross-asset stress crosses 50/30 thresholds

DOES NOT TOUCH ANY EXISTING LAMBDA. Pure consumer of divergence-v2.json
and macro-nowcast.json.
"""
import json
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import boto3

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────
REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY_OUT = os.environ.get("S3_KEY_OUT", "data/divergence-interpreted.json")
S3_KEY_DIV = os.environ.get("S3_KEY_DIVERGENCE", "data/divergence-v2.json")
S3_KEY_NOW = os.environ.get("S3_KEY_NOWCAST", "data/macro-nowcast.json")
S3_KEY_STATE = os.environ.get("S3_KEY_STATE", "data/divergence-interpreted-state.json")
ANTHROPIC_KEY_SSM = os.environ.get("ANTHROPIC_KEY_SSM", "/justhodl/anthropic/api_key")
ANTHROPIC_MODEL = os.environ.get("ANTHROPIC_MODEL", "claude-haiku-4-5-20251001")
TG_TOKEN_PARAM = "/justhodl/telegram/bot_token"
TG_CHAT_ID_PARAM = "/justhodl/telegram/chat_id"

S3 = boto3.client("s3", region_name=REGION)
SSM = boto3.client("ssm", region_name=REGION)

# Cache the API key between warm invocations
_anthropic_key = None


# ─────────────────────────────────────────────────────────────────────────────
# DATA LOADERS
# ─────────────────────────────────────────────────────────────────────────────
def load_s3_json(key, default=None):
    try:
        obj = S3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception as e:
        print(f"[interp] S3 load fail {key}: {e}")
        return default


def get_anthropic_key():
    global _anthropic_key
    if _anthropic_key:
        return _anthropic_key
    try:
        _anthropic_key = SSM.get_parameter(
            Name=ANTHROPIC_KEY_SSM, WithDecryption=True
        )["Parameter"]["Value"]
        return _anthropic_key
    except Exception:
        # Fallback to env var if SSM not set
        key = os.environ.get("ANTHROPIC_API_KEY")
        if key:
            _anthropic_key = key
            return key
        raise RuntimeError("Anthropic API key not found in SSM or env")


# ─────────────────────────────────────────────────────────────────────────────
# PROMPT BUILDER
# ─────────────────────────────────────────────────────────────────────────────
def build_interpretation_prompt(div_data, nowcast_data):
    """Compose the structured prompt for Claude."""
    # Extract the signal essentials
    composite = div_data.get("composite_divergence_index", 0)
    n_extreme = div_data.get("n_extreme", 0)
    n_flagged = div_data.get("n_flagged", 0)
    n_total = div_data.get("n_with_data", 0)

    extreme_pairs = div_data.get("extreme_alerts") or []
    flagged_pairs = div_data.get("flagged") or []

    # Top dislocations across all
    all_rels = div_data.get("all_relationships", [])
    with_data = [r for r in all_rels if r.get("status") in ("flagged", "extreme")]
    with_data.sort(key=lambda x: abs(x.get("divergence_z") or 0), reverse=True)

    # Macro nowcast extracts — defensive (components may be list or dict)
    regime = nowcast_data.get("regime", "UNKNOWN")
    composite_z = nowcast_data.get("composite_z", 0)
    components_raw = nowcast_data.get("components", {})

    # Normalize components into a consistent dict for display
    if isinstance(components_raw, dict):
        components = {k: v for k, v in components_raw.items()
                      if isinstance(v, (int, float))}
    elif isinstance(components_raw, list):
        # Each item likely has {name, z_score, weight, ...}
        components = {}
        for item in components_raw:
            if isinstance(item, dict):
                name = item.get("name") or item.get("series_id") or item.get("id")
                z = item.get("z_score") if "z_score" in item else item.get("value")
                if name and isinstance(z, (int, float)):
                    components[name] = z
    else:
        components = {}

    components_str = ", ".join(
        f"{k}={round(v, 2)}" for k, v in list(components.items())[:8]
    ) if components else "(no component breakdown available)"

    spy_forward_returns_raw = nowcast_data.get("spy_forward_returns_by_regime", {})
    spy_forward_returns = (spy_forward_returns_raw
                            if isinstance(spy_forward_returns_raw, dict) else {})

    # Build the prompt
    prompt_parts = [
        "You are a senior macro strategist analyzing cross-asset divergence signals.",
        "",
        f"## CURRENT MACRO REGIME: {regime}",
        f"  Composite z-score: {composite_z}",
        f"  Components: {components_str[:400]}",
        "",
        f"## SPY FORWARD RETURNS BY REGIME (from 10-year backtest)",
    ]
    if spy_forward_returns:
        for r, stats in spy_forward_returns.items():
            if isinstance(stats, dict):
                prompt_parts.append(
                    f"  {r}: 1m={stats.get('return_1m','?')}% / "
                    f"3m={stats.get('return_3m','?')}% / "
                    f"6m={stats.get('return_6m','?')}% / "
                    f"12m={stats.get('return_12m','?')}% (n={stats.get('count','?')})"
                )

    prompt_parts.extend([
        "",
        f"## CROSS-ASSET DIVERGENCE STATE (from 70-pair monitor)",
        f"  Composite Divergence Index: {composite}/100",
        f"  Pairs with data: {n_total}",
        f"  Extreme (>3σ): {n_extreme}",
        f"  Flagged (>2σ): {n_flagged}",
        "",
        "## TOP 12 DISLOCATIONS (sorted by |z-score|)",
    ])

    for r in with_data[:12]:
        prompt_parts.append(
            f"  • {r.get('name','?')} | z={r.get('divergence_z','?')} | "
            f"category={r.get('category','?')} | status={r.get('status','?')}"
        )
        if r.get("description"):
            prompt_parts.append(f"      ({r['description'][:140]})")

    prompt_parts.extend([
        "",
        "## YOUR TASK",
        "",
        "Produce a CONCISE (max 400 words) regime-aware interpretation. ",
        "Structure your response with these sections:",
        "",
        "1. **REGIME CONFIRMATION/CONTRADICTION**: Do these divergences confirm or",
        "   contradict the current regime? Which specific divergences matter most",
        "   given we're in this regime?",
        "",
        "2. **CRISIS-LEADING CHECK**: How many crisis_leading category pairs are",
        "   flagging? In the current regime, what's the implied lead-time before",
        "   real-economy weakness if these signals are accurate?",
        "",
        "3. **HIGH-CONVICTION CALL**: Based on the regime + divergences, give",
        "   ONE high-conviction tactical call: bullish/bearish/neutral on what?",
        "   Pick a specific instrument (SPY/IWM/EEM/sector) and a horizon (1m/3m).",
        "",
        "4. **WATCH LIST**: 2-3 specific divergences that, if they invert or extend",
        "   further, would change your call. Be specific (which pair, which direction).",
        "",
        "5. **CONFIDENCE**: Rate 1-10 your confidence in the call. Justify briefly.",
        "",
        "Be DECISIVE. No hedging. State the trade as if you're presenting to a CIO.",
        "If the data contradicts the regime, say so plainly — that's the most",
        "important signal of all.",
    ])

    return "\n".join(prompt_parts)


# ─────────────────────────────────────────────────────────────────────────────
# CLAUDE API CALL
# ─────────────────────────────────────────────────────────────────────────────
def call_claude(prompt, max_tokens=900):
    """Call Anthropic Messages API."""
    api_key = get_anthropic_key()
    body = {
        "model": ANTHROPIC_MODEL,
        "max_tokens": max_tokens,
        "messages": [{"role": "user", "content": prompt}],
    }
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=json.dumps(body).encode("utf-8"),
        method="POST",
        headers={
            "Content-Type": "application/json",
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
        },
    )
    started = time.time()
    with urllib.request.urlopen(req, timeout=60) as r:
        resp = json.loads(r.read().decode("utf-8"))
    duration = round(time.time() - started, 2)

    # Extract text from content blocks
    text = ""
    for block in resp.get("content", []):
        if block.get("type") == "text":
            text += block.get("text", "")

    return {
        "text": text.strip(),
        "model": resp.get("model"),
        "input_tokens": resp.get("usage", {}).get("input_tokens"),
        "output_tokens": resp.get("usage", {}).get("output_tokens"),
        "duration_s": duration,
        "stop_reason": resp.get("stop_reason"),
    }


# ─────────────────────────────────────────────────────────────────────────────
# TELEGRAM (regime-aware, high-conviction only)
# ─────────────────────────────────────────────────────────────────────────────
def send_telegram(msg, parse_mode="Markdown"):
    try:
        token = SSM.get_parameter(Name=TG_TOKEN_PARAM, WithDecryption=True)["Parameter"]["Value"]
        chat_id = SSM.get_parameter(Name=TG_CHAT_ID_PARAM, WithDecryption=True)["Parameter"]["Value"]
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        # Telegram message limit is 4096 chars; truncate if needed
        truncated = msg[:3900] + "…" if len(msg) > 4000 else msg
        data = urllib.parse.urlencode({
            "chat_id": chat_id, "text": truncated, "parse_mode": parse_mode,
        }).encode()
        urllib.request.urlopen(url, data=data, timeout=10).read()
        return True
    except Exception as e:
        print(f"[telegram] failed: {e}")
        return False


def should_alert(interpretation, div_data, nowcast_data, prior_state):
    """Determine if this interpretation warrants a Telegram alert.

    Alert criteria (any of):
      1. Number of extreme divergences crossed up through 5
      2. Composite index crossed up through 50 (high stress)
      3. Crisis_leading category has 3+ extreme AND regime is SLOWING/CONTRACTION
      4. Regime contradiction: regime is EXPANSION but composite > 40
      5. First run after gap of >24h (state file missing)
    """
    composite = div_data.get("composite_divergence_index", 0)
    n_extreme = div_data.get("n_extreme", 0)
    regime = nowcast_data.get("regime", "")

    # Count crisis_leading extremes
    crisis_extreme = 0
    for r in (div_data.get("by_category") or {}).get("crisis_leading", []):
        if r.get("status") == "extreme":
            crisis_extreme += 1

    prior_n_extreme = (prior_state or {}).get("n_extreme", 0)
    prior_composite = (prior_state or {}).get("composite_index", 0)

    reasons = []
    if n_extreme >= 5 and prior_n_extreme < 5:
        reasons.append(f"Extreme count crossed 5 ({prior_n_extreme}→{n_extreme})")
    if composite >= 50 and prior_composite < 50:
        reasons.append(f"Composite stress crossed 50 ({prior_composite}→{composite})")
    if crisis_extreme >= 3 and regime in ("SLOWING", "CONTRACTION RISK", "CONTRACTION"):
        reasons.append(f"{crisis_extreme} crisis_leading extreme + regime={regime}")
    if regime in ("EXPANSION", "STRONG EXPANSION") and composite > 40:
        reasons.append(f"Regime CONTRADICTION: regime={regime} but composite={composite}")
    if not prior_state:
        reasons.append("First run / state not found")

    return reasons


# ─────────────────────────────────────────────────────────────────────────────
# MAIN HANDLER
# ─────────────────────────────────────────────────────────────────────────────
def lambda_handler(event, context):
    started = time.time()

    # 1. Load fresh divergence-v2 + macro-nowcast
    div_data = load_s3_json(S3_KEY_DIV)
    nowcast_data = load_s3_json(S3_KEY_NOW)

    if not div_data or not nowcast_data:
        return {
            "statusCode": 500,
            "body": json.dumps({
                "ok": False,
                "error": "Missing input data",
                "div_loaded": bool(div_data),
                "nowcast_loaded": bool(nowcast_data),
            }),
        }

    div_age_s = None
    if div_data.get("as_of"):
        try:
            div_dt = datetime.fromisoformat(div_data["as_of"].replace("Z", "+00:00"))
            div_age_s = (datetime.now(timezone.utc) - div_dt).total_seconds()
        except Exception:
            pass

    # 2. Build the prompt + call Claude
    prompt = build_interpretation_prompt(div_data, nowcast_data)
    print(f"[interp] prompt length: {len(prompt)} chars")

    try:
        claude_resp = call_claude(prompt, max_tokens=900)
    except Exception as e:
        print(f"[interp] Claude call failed: {e}")
        return {
            "statusCode": 502,
            "body": json.dumps({"ok": False, "error": f"Claude call failed: {str(e)[:200]}"}),
        }

    interpretation = claude_resp["text"]

    # 3. Load prior state for delta detection
    prior_state = load_s3_json(S3_KEY_STATE, default={}) or {}

    # 4. Determine alerts
    alert_reasons = should_alert(interpretation, div_data, nowcast_data, prior_state)

    # 5. Build output payload
    payload = {
        "as_of": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "interpretation": interpretation,
        "regime": nowcast_data.get("regime"),
        "regime_composite_z": nowcast_data.get("composite_z"),
        "divergence_composite_index": div_data.get("composite_divergence_index"),
        "n_extreme": div_data.get("n_extreme"),
        "n_flagged": div_data.get("n_flagged"),
        "div_data_age_seconds": div_age_s,
        "claude_meta": {
            "model": claude_resp.get("model"),
            "input_tokens": claude_resp.get("input_tokens"),
            "output_tokens": claude_resp.get("output_tokens"),
            "duration_s": claude_resp.get("duration_s"),
            "stop_reason": claude_resp.get("stop_reason"),
        },
        "alert_reasons": alert_reasons,
        "duration_s": round(time.time() - started, 2),
    }

    # 6. Persist
    body_bytes = json.dumps(payload, indent=2, default=str).encode("utf-8")
    S3.put_object(
        Bucket=BUCKET, Key=S3_KEY_OUT, Body=body_bytes,
        ContentType="application/json", CacheControl="max-age=300",
    )

    # 7. Update state
    new_state = {
        "as_of": payload["as_of"],
        "n_extreme": payload["n_extreme"],
        "n_flagged": payload["n_flagged"],
        "composite_index": payload["divergence_composite_index"],
        "regime": payload["regime"],
    }
    S3.put_object(
        Bucket=BUCKET, Key=S3_KEY_STATE,
        Body=json.dumps(new_state, default=str).encode(),
        ContentType="application/json",
    )

    # 8. Telegram alert if criteria met
    if alert_reasons:
        msg_parts = [
            f"📊 *Divergence Interpreter — Regime: {payload['regime']}*",
            f"Composite stress: {payload['divergence_composite_index']}/100  "
            f"({payload['n_extreme']} extreme, {payload['n_flagged']} flagged)",
            "",
            f"_Trigger: {'; '.join(alert_reasons[:2])}_",
            "",
            interpretation,
        ]
        send_telegram("\n".join(msg_parts))

    print(f"[interp] done in {payload['duration_s']}s — alerts: {len(alert_reasons)}")
    return {
        "statusCode": 200,
        "body": json.dumps({
            "ok": True,
            "regime": payload["regime"],
            "composite_index": payload["divergence_composite_index"],
            "n_extreme": payload["n_extreme"],
            "n_flagged": payload["n_flagged"],
            "alert_reasons": alert_reasons,
            "interpretation_chars": len(interpretation),
            "duration_s": payload["duration_s"],
        }),
    }
