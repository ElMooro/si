"""justhodl-digest-trends-ai

AI-narrated digest. Reads ALL system state and uses Claude to generate
a coherent narrative + structured insights for digest-trends.html.

Reads:
  - data/cascade-calibration.json (self-improvement weights + history)
  - data/predictions-snapshots/latest.json (today's predictions + features)
  - data/cascade-validation-log.json (track record)
  - data/simulated-portfolio.json (P&L)
  - data/pnl-stats.json (win rate, expectancy)
  - data/theme-cascade.json (current cascade)
  - data/trade-tickets.json (active tickets)
  - data/retail-sentiment.json (HPQ, MRVL, DG, GRRR etc.)
  - data/digest-trends-rolling.json (60-day digest index)

Writes:
  data/digest-trends-ai.json — structured AI insights for HTML rendering
"""
import json
import os
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta
from typing import Optional

import boto3

S3_BUCKET = "justhodl-dashboard-live"
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
ANTHROPIC_MODEL = "claude-haiku-4-5-20251001"

s3 = boto3.client("s3", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")


def _read_json(key: str) -> Optional[dict]:
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def _get_anthropic_key() -> str:
    if ANTHROPIC_KEY:
        return ANTHROPIC_KEY
    try:
        return ssm.get_parameter(Name="/justhodl/anthropic/api_key",
                                  WithDecryption=True)["Parameter"]["Value"]
    except Exception:
        pass
    try:
        return ssm.get_parameter(Name="/anthropic/api_key",
                                  WithDecryption=True)["Parameter"]["Value"]
    except Exception:
        pass
    return ""


def call_claude(prompt: str, system: str = "") -> str:
    """Call Claude API. Returns narrative text."""
    api_key = _get_anthropic_key()
    if not api_key:
        return ""

    body = {
        "model": ANTHROPIC_MODEL,
        "max_tokens": 1500,
        "messages": [{"role": "user", "content": prompt}],
    }
    if system:
        body["system"] = system

    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=json.dumps(body).encode(),
        headers={
            "Content-Type": "application/json",
            "anthropic-version": "2023-06-01",
            "x-api-key": api_key,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=40) as r:
            data = json.loads(r.read().decode())
        content = data.get("content", [])
        for block in content:
            if block.get("type") == "text":
                return block.get("text", "")
        return ""
    except Exception as e:
        print(f"[claude] {e}")
        return ""


def gather_system_state() -> dict:
    """Read all relevant data files into a single state object."""
    state = {
        "calibration": _read_json("data/cascade-calibration.json") or {},
        "predictions_today": _read_json("data/predictions-snapshots/latest.json") or {},
        "validation": _read_json("data/cascade-validation-log.json") or {},
        "portfolio": _read_json("data/simulated-portfolio.json") or {},
        "pnl_stats": _read_json("data/pnl-stats.json") or {},
        "cascade": _read_json("data/theme-cascade.json") or {},
        "tickets": _read_json("data/trade-tickets.json") or {},
        "retail": _read_json("data/retail-sentiment.json") or {},
        "monitor": _read_json("data/trade-monitor-snapshots.json") or {},
    }
    return state


def build_context_for_ai(state: dict) -> dict:
    """Distill system state into a compact context object the AI can reason over."""
    # Calibration summary
    cal = state.get("calibration", {})
    cal_weights = cal.get("current_weights", {})
    cal_n_dp = (cal.get("feature_attribution") or {}).get("n_predictions_analyzed", 0)
    ranked_features = (cal.get("feature_attribution") or {}).get("ranked_by_hit_rate_lift", [])[:5]

    # Predictions snapshot
    preds = state.get("predictions_today", {})
    alert_dist = preds.get("alert_distribution", {})
    n_preds = preds.get("n_tickers", 0)

    # Validation hit rates
    val = state.get("validation", {})
    by_tier = val.get("by_tier_stats", {})
    n_validated = val.get("n_predictions_validated", 0)

    # Portfolio P&L
    port = state.get("portfolio", {})
    stats = port.get("stats", {}) or state.get("pnl_stats", {})

    # Top cascade picks today
    cascade_top = (state.get("cascade", {}).get("alert_tier") or [])[:5]
    cascade_lag = (state.get("cascade", {}).get("laggards_hot_themes") or [])[:5]

    # Active trade tickets
    tickets = (state.get("tickets", {}).get("tickets") or [])[:8]

    # Retail sentiment surges
    retail = state.get("retail", {})
    retail_surges = []
    for r in (((retail.get("ranked") or {}).get("biggest_velocity_surges"))
              or retail.get("velocity_surges") or [])[:6]:
        if isinstance(r, dict):
            retail_surges.append({
                "ticker": r.get("ticker"),
                "mentions": r.get("mentions"),
                "velocity_pct": r.get("velocity_pct") or r.get("change_pct"),
            })

    # Monitor / P&L
    monitor = state.get("monitor", {})
    pnl_snapshots = (monitor.get("snapshots") or [])[:8]

    return {
        "today": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "calibration_state": {
            "n_data_points": cal_n_dp,
            "n_weights": len(cal_weights),
            "top_predictive_features": ranked_features,
        },
        "predictions": {
            "n_today": n_preds,
            "alert_distribution": alert_dist,
        },
        "validation": {
            "n_validated": n_validated,
            "by_tier": by_tier,
        },
        "portfolio": {
            "n_open": stats.get("n_open", 0),
            "n_closed": stats.get("n_closed", 0),
            "realized_pnl_usd": stats.get("total_realized_usd", 0),
            "win_rate_pct": stats.get("win_rate_pct", 0),
        },
        "cascade_top_5_alert": [
            {"ticker": c.get("ticker"), "score": c.get("combined_score"),
             "industry": c.get("industry_label")}
            for c in cascade_top
        ],
        "cascade_laggards": [
            {"ticker": c.get("ticker"), "perf_5d": c.get("perf_5d_pct"),
             "industry": c.get("industry_label")}
            for c in cascade_lag
        ],
        "trade_tickets_top": [
            {"ticker": t.get("ticker"), "entry": t.get("entry"),
             "stop": t.get("stop_loss"), "tp3": t.get("tp3"),
             "rr": t.get("rr_tp3")}
            for t in tickets
        ],
        "retail_sentiment_surges": retail_surges,
        "pnl_snapshots_top": [
            {"ticker": p.get("ticker"), "pnl_pct": p.get("pnl_pct"),
             "pnl_usd": p.get("pnl_usd")}
            for p in pnl_snapshots
        ],
    }


def generate_ai_narrative(context: dict) -> dict:
    """Generate AI narrative + structured insights via Claude."""
    system_prompt = """You are JustHodl.AI's research analyst writing a daily digest summary
for an institutional-grade trading platform.

Tone: precise, candid, no hedging. Use specific tickers and numbers.
Length: ~250-400 words across 4 sections.
Format: return ONLY valid JSON, no markdown fences.

JSON schema:
{
  "overview": "2-3 sentences summarizing system state today",
  "system_performance": "2-3 sentences on calibration progress + portfolio P&L",
  "top_actionable": "2-3 sentences naming SPECIFIC tickers + entry/stop/TP from the cascade",
  "risks_watching": "2-3 sentences on red flags or stops approaching",
  "key_metrics": {
    "headline_metric": "string e.g. '15 open positions, $0 realized'",
    "calibration_status": "string e.g. 'Building dataset — 158 predictions captured today'",
    "best_opportunity": "string e.g. 'INTC: +53% TP3, R:R 3:1'",
    "retail_signal": "string e.g. 'HPQ +4600% mentions — social momentum surge'"
  }
}

Be direct. Skip filler. Use real data from the context."""

    user_prompt = f"""Today is {context['today']}. Here's the current system state:

CALIBRATION:
- Data points scored: {context['calibration_state']['n_data_points']}
- Features being tracked: {context['calibration_state']['n_weights']}
- Top predictive features so far: {json.dumps(context['calibration_state']['top_predictive_features'])}

PREDICTIONS TODAY: {context['predictions']['n_today']} tickers captured
Distribution: {json.dumps(context['predictions']['alert_distribution'])}

PORTFOLIO:
- Open: {context['portfolio']['n_open']}, Closed: {context['portfolio']['n_closed']}
- Realized P&L: ${context['portfolio']['realized_pnl_usd']:,.0f}
- Win rate: {context['portfolio']['win_rate_pct']}%

CASCADE TOP 5 ALERTS:
{json.dumps(context['cascade_top_5_alert'])}

CASCADE LAGGARDS:
{json.dumps(context['cascade_laggards'])}

TRADE TICKETS (entry/stop/tp3/RR):
{json.dumps(context['trade_tickets_top'])}

RETAIL SENTIMENT SURGES (velocity %):
{json.dumps(context['retail_sentiment_surges'])}

LIVE POSITIONS P&L:
{json.dumps(context['pnl_snapshots_top'])}

Write the JSON narrative now."""

    raw = call_claude(user_prompt, system=system_prompt)
    if not raw:
        return {"error": "no_response_from_claude",
                "fallback": True,
                "overview": "AI narrator unavailable. System state shown below from raw data."}

    # Parse JSON response (strip code fences if any)
    cleaned = raw.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.split("```", 2)[1] if "```" in cleaned[3:] else cleaned[3:]
        if cleaned.startswith("json"):
            cleaned = cleaned[4:].strip()
    try:
        parsed = json.loads(cleaned)
        return parsed
    except Exception:
        return {"narrative_raw": raw[:1500], "parse_error": True}


def lambda_handler(event, context):
    t0 = time.time()
    print(f"[ai-digest] starting")

    state = gather_system_state()
    ai_context = build_context_for_ai(state)
    print(f"[ai-digest] context built · {ai_context['predictions']['n_today']} preds, "
          f"{ai_context['portfolio']['n_open']} open positions")

    narrative = generate_ai_narrative(ai_context)
    print(f"[ai-digest] narrative generated: keys={list(narrative.keys())}")

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "date": ai_context["today"],
        "narrative": narrative,
        "system_state_snapshot": ai_context,
    }

    s3.put_object(
        Bucket=S3_BUCKET, Key="data/digest-trends-ai.json",
        Body=json.dumps(output, default=str).encode(),
        ContentType="application/json", CacheControl="public, max-age=600",
    )

    # Also save dated history
    today = ai_context["today"]
    s3.put_object(
        Bucket=S3_BUCKET, Key=f"data/digest-trends-ai-history/{today}.json",
        Body=json.dumps(output, default=str).encode(),
        ContentType="application/json", CacheControl="public, max-age=86400",
    )

    # Rolling compact history → powers the page's trend sparklines (makes "Trends" real)
    try:
        try:
            _h = json.loads(s3.get_object(Bucket=S3_BUCKET, Key="data/digest-trends-history.json")["Body"].read())
            series = _h.get("series", []) if isinstance(_h, dict) else []
        except Exception:
            series = []
        _pf = ai_context.get("portfolio", {}); _pr = ai_context.get("predictions", {})
        rec = {
            "date": today,
            "realized_pnl_usd": _pf.get("realized_pnl_usd", 0),
            "win_rate_pct": _pf.get("win_rate_pct", 0),
            "n_open": _pf.get("n_open", 0),
            "n_closed": _pf.get("n_closed", 0),
            "n_predictions": _pr.get("n_today", 0),
            "n_calibration": (ai_context.get("calibration_state") or {}).get("n_data_points", 0),
            "n_validated": (ai_context.get("validation") or {}).get("n_validated", 0),
            "total_alerts": sum((_pr.get("alert_distribution") or {}).values()),
            "top_retail_pct": max([abs(r.get("velocity_pct") or 0)
                                   for r in ai_context.get("retail_sentiment_surges", [])] or [0]),
        }
        series = [r for r in series if r.get("date") != today]
        series.append(rec)
        series = sorted(series, key=lambda r: r.get("date") or "")[-120:]
        s3.put_object(
            Bucket=S3_BUCKET, Key="data/digest-trends-history.json",
            Body=json.dumps({"generated_at": datetime.now(timezone.utc).isoformat(),
                             "series": series}, default=str).encode(),
            ContentType="application/json", CacheControl="public, max-age=600",
        )
        print(f"[ai-digest] history series now {len(series)} days")
    except Exception as e:
        print(f"[ai-digest] history accumulate fail: {str(e)[:120]}")

    elapsed = round(time.time() - t0, 1)
    print(f"[ai-digest] DONE in {elapsed}s")

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({
            "ok": True, "elapsed_s": elapsed,
            "n_predictions": ai_context["predictions"]["n_today"],
            "narrative_keys": list(narrative.keys()),
            "has_overview": "overview" in narrative,
        }),
    }
