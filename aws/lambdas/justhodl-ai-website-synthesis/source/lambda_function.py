"""
justhodl-ai-website-synthesis
══════════════════════════════
Reads 12+ major engine outputs from S3 and calls Claude (haiku-4-5) to
produce a CROSS-ENGINE synthesis: a unified narrative + per-page
slug that the entire website surfaces via a global JS widget.

OUTPUT: s3://justhodl-dashboard-live/data/ai-website-synthesis.json

SCHEMA
══════
{
  "schema_version":   "1.0",
  "generated_at":     "...",
  "model":            "claude-haiku-4-5-20251001",
  "snapshot_age_min": {engine: minutes},
  "synthesis": {
      "global_posture":      RISK_ON | NEUTRAL | RISK_OFF | DEFENSIVE | EXTREME,
      "headline":            "single sentence what's happening",
      "thesis":              "3-4 sentence cross-engine read",
      "key_drivers":         ["..."],            # top 3-5 things moving
      "key_dissonances":     ["..."],            # signals contradicting each other
      "decisive_call":       "...",              # 1-line action
      "watch_list":          ["..."],            # what to monitor next 24h
      "per_page_focus": {                        # what each page should highlight
          "auction-crisis":  "...",
          "macro-frontrun":  "...",
          "crisis":          "...",
          "bonds":           "...",
          "repo":            "...",
          "regime":          "...",
          "correlation":     "...",
          "sentiment":       "...",
          "volatility":      "..."
      }
  }
}

DESIGN
══════
- One Claude API call per invocation (~30-60s)
- 12-engine read in parallel via ThreadPoolExecutor
- Stale engines (>4h) flagged but not blocking
- Schedule: hourly cron at 25min past
- Fail-soft: writes degraded payload with status=error on any failure
- Archives every output to data/archive/ai-website-synthesis/YYYYMMDD_HH.json
"""
import json
import os
import re
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import boto3

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    from _sentry_lite import track_errors  # noqa
except ImportError:
    pass

S3_BUCKET = "justhodl-dashboard-live"
OUTPUT_KEY = "data/ai-website-synthesis.json"
MODEL = "claude-haiku-4-5-20251001"
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

s3 = boto3.client("s3", region_name="us-east-1")

# ─── Engine inputs — distilled views of the most important state ───
ENGINE_INPUTS = {
    "signal_board":     {"key": "data/signal-board.json",
                           "fields": ["composite_posture", "composite_signal",
                                       "n_live", "n_stale", "categories"]},
    "auction_crisis":   {"key": "data/auction-crisis.json",
                           "fields": ["regime", "composite_score", "interpretation",
                                       "n_recent_auctions_14d", "tail_risk",
                                       "tenor_decomposition", "triggers"]},
    "auction_crisis_ai":{"key": "data/auction-crisis-ai.json",
                           "fields": ["regime", "composite", "ai_commentary"]},
    "macro_frontrun":   {"key": "data/macro-frontrun-sniffer.json",
                           "fields": ["overall_macro_score", "macro_regime",
                                       "headline", "thesis", "pillars",
                                       "loudest_macro_anomaly"]},
    "crisis_brief":     {"key": "data/crisis-brief.json",
                           "fields": ["regime", "score", "headline", "key_risks"]},
    "bonds":            {"key": "data/bond-trace.json",
                           "fields": ["regime", "hy_oas", "hy_oas_velocity",
                                       "ig_oas", "stress_score"]},
    "repo":             {"key": "data/repo.json",
                           "fields": ["sofr", "iorb", "spread", "regime"]},
    "regime":           {"key": "data/regime.json",
                           "fields": ["regime", "score", "drivers"]},
    "correlations":     {"key": "data/correlations.json",
                           "fields": ["regime", "breakdown_count", "headline"]},
    "global_stress":    {"key": "data/global-stress.json",
                           "fields": ["global_stress_index", "global_stress_level"]},
    "sentiment":        {"key": "data/sentiment.json",
                           "fields": ["regime", "score", "putcall", "vix"]},
    "volatility":       {"key": "data/vol-radar.json",
                           "fields": ["regime", "vix", "vvix", "move"]},
}


# ═════════════════════════════════════════════════════════════════════
# Prompt engineering
# ═════════════════════════════════════════════════════════════════════

SYSTEM_PROMPT = """You are the senior strategist running cross-engine \
synthesis for an institutional financial intelligence platform. The \
platform has 50+ specialized engines (auction crisis, macro front-run, \
credit spreads, repo stress, regime, correlations, sentiment, vol, \
liquidity, etc.). Your job is to read the LATEST outputs from 12 of \
the most decision-critical engines and produce a SINGLE COHERENT \
narrative that tells the user: what is happening across markets right \
now, what's the dominant theme, where are signals AGREEING (high \
conviction) vs DISAGREEING (uncertainty), what to do.

STYLE RULES
───────────
- Direct, decisive, no hedging
- Plain English, NOT bureaucratic
- Reference specific engine readings to support claims
- Highlight DISSONANCES between engines (the most diagnostic feature)
- Make a CALL even when uncertain
- Each section is concise (executive summary 3 sentences max)
- NO preambles like "Here is the analysis"
- The decisive_call MUST be a single sentence prescribing concrete action

OUTPUT FORMAT — pure JSON, no markdown, no preamble
═══════════════════════════════════════════════════
{
  "global_posture":       "RISK_ON | NEUTRAL | RISK_OFF | DEFENSIVE | EXTREME",
  "headline":             "single sentence — what's the dominant story",
  "thesis":               "3-4 sentence cross-engine read on what's actually happening",
  "key_drivers":          ["...", "...", "..."],
  "key_dissonances":      ["...", "..."],
  "decisive_call":        "single concrete action sentence",
  "watch_list":           ["...", "...", "..."],
  "per_page_focus": {
      "auction-crisis":   "1 sentence — what this page reveals right now",
      "macro-frontrun":   "...",
      "crisis":           "...",
      "bonds":            "...",
      "repo":             "...",
      "regime":           "...",
      "correlation":      "...",
      "sentiment":        "...",
      "volatility":       "..."
  }
}

The per_page_focus entries are what the global insights widget will \
display when the user is on that page — make them sharp and useful.
"""


def build_user_prompt(snapshots: dict) -> str:
    """Format engine snapshots into a structured user prompt."""
    parts = ["# Cross-Engine Market Snapshot — right now\n"]
    parts.append(f"Generated at: {datetime.now(timezone.utc).isoformat()}\n")

    for engine_name, snap in snapshots.items():
        if not snap or snap.get("_error"):
            parts.append(f"\n## {engine_name.upper()} — UNAVAILABLE")
            if snap and snap.get("_error"):
                parts.append(f"  Error: {snap['_error']}")
            continue
        age_min = snap.get("_age_min", "?")
        parts.append(f"\n## {engine_name.upper()} (age: {age_min}min)")
        for k, v in snap.items():
            if k.startswith("_"):
                continue
            if isinstance(v, (dict, list)):
                # Truncate nested structures for prompt density
                s = json.dumps(v, default=str)
                if len(s) > 600:
                    s = s[:600] + "..."
                parts.append(f"  {k}: {s}")
            else:
                parts.append(f"  {k}: {v}")

    parts.append("\n\nProduce the JSON cross-engine synthesis per the system prompt format.")
    return "\n".join(parts)


def call_anthropic(system: str, user: str, max_tokens: int = 4000) -> str:
    """Call Anthropic API, return the text response."""
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
    with urllib.request.urlopen(req, timeout=90) as r:
        data = json.loads(r.read().decode("utf-8"))
    if not data.get("content"):
        raise RuntimeError(f"Empty response: {data}")
    text = ""
    for block in data["content"]:
        if block.get("type") == "text":
            text += block.get("text", "")
    return text.strip()


def extract_json(text: str) -> dict:
    """Extract first balanced JSON object from response."""
    text = re.sub(r"^```(?:json)?\s*", "", text.strip(), flags=re.MULTILINE)
    text = re.sub(r"\s*```\s*$", "", text.strip(), flags=re.MULTILINE)
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
    return json.loads(text)


# ═════════════════════════════════════════════════════════════════════
# Engine data fetching
# ═════════════════════════════════════════════════════════════════════

def fetch_engine(engine_name: str, spec: dict) -> tuple:
    """Fetch a single engine output, return (name, distilled_dict)."""
    key = spec["key"]
    fields = spec["fields"]
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        d = json.loads(obj["Body"].read())
        last_modified = obj["LastModified"]
        age_s = (datetime.now(timezone.utc) - last_modified).total_seconds()
        age_min = round(age_s / 60, 1)
        # Distill — keep only specified fields
        distilled = {"_age_min": age_min, "_stale": age_s > 14400}  # >4h
        for f in fields:
            if f in d:
                v = d[f]
                # Heavy truncation for nested objects to keep prompt under 30K tokens
                if isinstance(v, list) and len(v) > 5:
                    v = v[:5]
                elif isinstance(v, dict) and len(v) > 10:
                    # Keep first 10 keys
                    v = dict(list(v.items())[:10])
                distilled[f] = v
        return engine_name, distilled
    except s3.exceptions.NoSuchKey:
        return engine_name, {"_error": "engine output not found in S3", "_age_min": None}
    except Exception as e:
        return engine_name, {"_error": str(e)[:140], "_age_min": None}


def fetch_all_engines() -> dict:
    """Parallel fetch all 12 engine outputs."""
    snapshots = {}
    with ThreadPoolExecutor(max_workers=12) as ex:
        futures = {ex.submit(fetch_engine, name, spec): name
                    for name, spec in ENGINE_INPUTS.items()}
        for fut in as_completed(futures, timeout=60):
            try:
                name, snap = fut.result()
                snapshots[name] = snap
            except Exception as e:
                name = futures[fut]
                snapshots[name] = {"_error": str(e)[:140]}
    return snapshots


# ═════════════════════════════════════════════════════════════════════
# Telegram notification on regime change
# ═════════════════════════════════════════════════════════════════════

TELEGRAM_TOKEN   = "8679881066:AAHTE6TAhDqs0FuUelTL6Ppt1x8ihis1aGs"
TELEGRAM_CHAT_ID = "8678089260"
PRIOR_STATE_KEY  = "data/_alerts/website-synthesis-state.json"


def send_telegram(text: str) -> bool:
    """Send Markdown msg via Telegram. Returns success bool."""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = json.dumps({
        "chat_id": TELEGRAM_CHAT_ID, "text": text,
        "parse_mode": "Markdown", "disable_web_page_preview": True,
    }).encode("utf-8")
    try:
        req = urllib.request.Request(url, data=payload,
                                        headers={"Content-Type": "application/json"},
                                        method="POST")
        with urllib.request.urlopen(req, timeout=10) as r:
            return bool(json.loads(r.read())["ok"])
    except Exception as e:
        print(f"[telegram] err: {e}")
        return False


def maybe_alert_posture_change(synthesis: dict) -> dict:
    """If global_posture changed since last run, send a Telegram alert."""
    cur_posture = synthesis.get("global_posture")
    if not cur_posture:
        return {"sent": False, "reason": "no_posture"}

    prior_posture = None
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=PRIOR_STATE_KEY)
        prior = json.loads(obj["Body"].read())
        prior_posture = prior.get("global_posture")
    except s3.exceptions.NoSuchKey:
        pass
    except Exception as e:
        print(f"[alert] state load err: {e}")

    sent = False
    reason = "no_change"
    if prior_posture is None:
        # First run — send init message
        msg = (f"🆕 *Website-Wide AI Synthesis ONLINE*\n\n"
                f"Global posture: *{cur_posture}*\n\n"
                f"_{synthesis.get('headline', '')}_\n\n"
                f"🔗 [Dashboard](https://justhodl.ai/)")
        sent = send_telegram(msg)
        reason = "system_initialized"
    elif prior_posture != cur_posture:
        # Posture transition
        severity_rank = {"RISK_ON": 0, "NEUTRAL": 1, "RISK_OFF": 2,
                          "DEFENSIVE": 3, "EXTREME": 4}
        prev_rank = severity_rank.get(prior_posture, 2)
        cur_rank = severity_rank.get(cur_posture, 2)
        arrow = "⬆️" if cur_rank > prev_rank else "⬇️"
        emoji = "🚨" if cur_rank > prev_rank else "✅"
        msg = (f"{emoji} *Cross-Engine Posture Change* {arrow}\n\n"
                f"*{prior_posture} → {cur_posture}*\n\n"
                f"_{synthesis.get('headline', '')}_\n\n"
                f"_Decisive call_: {synthesis.get('decisive_call', '')[:300]}\n\n"
                f"🔗 [Dashboard](https://justhodl.ai/)")
        sent = send_telegram(msg)
        reason = f"posture_transition_{prior_posture}_to_{cur_posture}"

    # Save current state
    try:
        s3.put_object(
            Bucket=S3_BUCKET, Key=PRIOR_STATE_KEY,
            Body=json.dumps({
                "global_posture": cur_posture,
                "snapshot_at":   datetime.now(timezone.utc).isoformat(),
            }, default=str),
            ContentType="application/json",
        )
    except Exception as e:
        print(f"[alert] state save err: {e}")

    return {"sent": sent, "reason": reason,
            "prior": prior_posture, "current": cur_posture}


# ═════════════════════════════════════════════════════════════════════
# Lambda handler
# ═════════════════════════════════════════════════════════════════════

def lambda_handler(event, context):
    t0 = time.time()
    print(f"[website-synthesis] start {datetime.now(timezone.utc).isoformat()}")

    # 1. Fetch all engine snapshots in parallel
    print("[ws] phase 1: fetch 12 engines in parallel…")
    snapshots = fetch_all_engines()
    engines_ok = sum(1 for v in snapshots.values() if v and not v.get("_error"))
    print(f"[ws] {engines_ok}/{len(ENGINE_INPUTS)} engines loaded")

    if engines_ok < 4:
        return _write_error(
            f"Too few engines loaded ({engines_ok}/{len(ENGINE_INPUTS)})",
            snapshots=snapshots,
        )

    # 2. Build prompt + call Claude
    user_prompt = build_user_prompt(snapshots)
    print(f"[ws] prompt {len(user_prompt)} chars")

    try:
        t_claude = time.time()
        response_text = call_anthropic(SYSTEM_PROMPT, user_prompt, max_tokens=4000)
        claude_elapsed = round(time.time() - t_claude, 2)
        print(f"[ws] Claude response in {claude_elapsed}s, {len(response_text)} chars")
    except Exception as e:
        return _write_error(f"Claude error: {e}", snapshots=snapshots)

    # 3. Parse JSON
    try:
        synthesis = extract_json(response_text)
    except Exception as e:
        return _write_error(f"JSON parse error: {e}",
                             raw_response_preview=response_text[:500])

    # 4. Validate
    if "global_posture" not in synthesis or "headline" not in synthesis:
        return _write_error("Missing required keys in AI response",
                             partial=synthesis)

    # 5. Build output + write
    snapshot_age_summary = {
        n: (s.get("_age_min") if s else None)
        for n, s in snapshots.items()
    }
    output = {
        "schema_version":   "1.0",
        "generated_at":     datetime.now(timezone.utc).isoformat(),
        "model":            MODEL,
        "elapsed_sec":      round(time.time() - t0, 2),
        "claude_elapsed_sec": claude_elapsed,
        "engines_loaded":   engines_ok,
        "engines_total":    len(ENGINE_INPUTS),
        "snapshot_age_min": snapshot_age_summary,
        "synthesis":        synthesis,
    }
    body = json.dumps(output, indent=2, default=str)
    s3.put_object(
        Bucket=S3_BUCKET, Key=OUTPUT_KEY, Body=body,
        ContentType="application/json", CacheControl="max-age=900",
    )
    archive_key = (f"data/archive/ai-website-synthesis/"
                    f"{datetime.now(timezone.utc).strftime('%Y%m%d_%H')}.json")
    s3.put_object(Bucket=S3_BUCKET, Key=archive_key, Body=body,
                    ContentType="application/json")

    # 6. Maybe send Telegram alert on posture change
    alert_info = maybe_alert_posture_change(synthesis)
    output["alert_info"] = alert_info

    summary = {
        "status":             "ok",
        "elapsed_sec":        output["elapsed_sec"],
        "claude_elapsed":     claude_elapsed,
        "engines_loaded":     f"{engines_ok}/{len(ENGINE_INPUTS)}",
        "global_posture":     synthesis.get("global_posture"),
        "headline":           (synthesis.get("headline") or "")[:140],
        "alert_sent":         alert_info.get("sent"),
        "alert_reason":       alert_info.get("reason"),
    }
    print(f"[website-synthesis] done: {summary}")
    return {"statusCode": 200, "body": json.dumps(summary)}


def _write_error(message: str, **extras) -> dict:
    """Write degraded payload."""
    payload = {
        "schema_version": "1.0",
        "generated_at":   datetime.now(timezone.utc).isoformat(),
        "status":         "error",
        "error":          message,
        **extras,
    }
    try:
        s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY,
                        Body=json.dumps(payload, default=str, indent=2),
                        ContentType="application/json", CacheControl="max-age=300")
    except Exception as e:
        print(f"[ws] error-payload write fail: {e}")
    print(f"[website-synthesis] ERROR: {message}")
    return {"statusCode": 500, "body": json.dumps({"status": "error", "error": message})}
