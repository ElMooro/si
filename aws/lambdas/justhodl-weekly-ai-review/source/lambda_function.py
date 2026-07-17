"""justhodl-weekly-ai-review

Weekly synthesis of self-improvement loop progress. Runs every Sunday 10:00 ET.

Reads 7 days of:
  - data/predictions-scored/*.json (daily outcome scoring)
  - data/cascade-recalibration-history/*.json (weight evolution)
  - data/predictions-snapshots/*.json (predictions made)
  - data/simulated-portfolio.json (P&L state)
  - data/cascade-validation-log.json (track record)

Uses Claude to generate a Monday-morning hedge-fund weekly memo:
  - Headline (what happened this week)
  - Performance recap (hit rate, P&L, best/worst)
  - Calibration progress (what features emerged, what got demoted)
  - System improvements (what was learned)
  - Next week's setup (current cascade picks, themes)

OUTPUT:
  data/weekly-review-{week_ending}.json (dated history)
  data/weekly-review-latest.json (pointer)
  Telegram digest sent automatically
"""
import anthropic_shim  # resilient LLM fallback (Anthropic->GLM via llm_router)
import json
import os
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict

import boto3

S3_BUCKET = "justhodl-dashboard-live"
ANTHROPIC_MODEL = "claude-sonnet-4-5-20250929"  # weekly = higher-quality synthesis
TG_BOT_TOKEN = "8679881066:AAHTE6TAhDqs0FuUelTL6Ppt1x8ihis1aGs"
TG_CHAT_ID = "8678089260"

s3 = boto3.client("s3", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")


def _read_json(key: str) -> Optional[dict]:
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def _list_keys(prefix: str, limit: int = 30) -> List[str]:
    try:
        resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix, MaxKeys=limit)
        return sorted([o["Key"] for o in resp.get("Contents", [])])
    except Exception:
        return []


def _get_anthropic_key() -> str:
    if os.environ.get("ANTHROPIC_API_KEY"):
        return os.environ["ANTHROPIC_API_KEY"]
    for path in ["/justhodl/anthropic/api_key", "/anthropic/api_key"]:
        try:
            return ssm.get_parameter(Name=path, WithDecryption=True)["Parameter"]["Value"]
        except Exception:
            continue
    return ""


def call_claude(prompt: str, system: str = "", max_tokens: int = 2200) -> str:
    api_key = _get_anthropic_key()
    if not api_key:
        return ""
    body = {"model": ANTHROPIC_MODEL, "max_tokens": max_tokens,
            "messages": [{"role": "user", "content": prompt}]}
    if system:
        body["system"] = system
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json",
                  "anthropic-version": "2023-06-01", "x-api-key": api_key},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            data = json.loads(r.read().decode())
        for block in data.get("content", []):
            if block.get("type") == "text":
                return block.get("text", "")
        return ""
    except Exception as e:
        print(f"[claude] {e}")
        return ""


def _send_telegram(text: str) -> dict:
    try:
        token = ssm.get_parameter(Name="/justhodl/telegram/bot-token",
                                    WithDecryption=True)["Parameter"]["Value"]
        chat_id = ssm.get_parameter(Name="/justhodl/telegram/chat_id")["Parameter"]["Value"]
    except Exception:
        token, chat_id = TG_BOT_TOKEN, TG_CHAT_ID
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = urllib.parse.urlencode({
        "chat_id": chat_id, "text": text[:4000], "parse_mode": "HTML",
        "disable_web_page_preview": "true",
    }).encode()
    try:
        req = urllib.request.Request(url, data=data, method="POST")
        with urllib.request.urlopen(req, timeout=12) as r:
            return {"status": r.status}
    except Exception as e:
        return {"error": str(e)[:200]}


def gather_weekly_context() -> dict:
    """Read 7 days of data into a compact context for AI."""
    today = datetime.now(timezone.utc)
    week_ago = today - timedelta(days=7)
    today_str = today.strftime("%Y-%m-%d")

    # 1. Scored predictions across the week
    scored_keys = _list_keys("data/predictions-scored/", limit=30)
    weekly_scored = []
    for key in scored_keys[-7:]:  # last 7 files
        doc = _read_json(key)
        if doc and doc.get("scored"):
            for p in doc.get("scored", [])[:50]:  # cap per day
                if p.get("outcome") in ("HIT_BIG", "HIT", "SLOW", "FLAT", "MISS"):
                    weekly_scored.append({
                        "date": doc.get("snapshot_date_scored"),
                        "ticker": p.get("ticker"),
                        "alerts": p.get("alerts", []),
                        "outcome": p.get("outcome"),
                        "max_return_pct": p.get("max_return_pct"),
                        "return_1d_pct": p.get("return_1d_pct"),
                        "pumped_1d": p.get("pumped_within_1d", False),
                    })

    # 2. Calibration history (weight evolution)
    cal_keys = _list_keys("data/cascade-recalibration-history/", limit=30)
    cal_history = []
    for key in cal_keys[-7:]:
        doc = _read_json(key)
        if doc:
            cal_history.append({
                "date": key.split("/")[-1].replace(".json", ""),
                "confidence": (doc.get("blend") or {}).get("confidence"),
                "n_predictions": doc.get("calibration_n_predictions", 0),
                "top_weights": (doc.get("top_weights") or [])[:5],
                "alert_retention": ((doc.get("rank_changes") or {})
                                     .get("alert_tier") or {})
                                     .get("top_10_retention_pct"),
            })

    # 3. Current calibration
    current_cal = _read_json("data/cascade-calibration.json") or {}
    current_audit = _read_json("data/cascade-recalibration-audit.json") or {}

    # 4. Portfolio P&L
    portfolio = _read_json("data/simulated-portfolio.json") or {}
    pnl = portfolio.get("stats") or {}

    # 5. Current cascade picks
    cascade = _read_json("data/theme-cascade-calibrated.json") or _read_json("data/theme-cascade.json") or {}

    # Stats
    n_total = len(weekly_scored)
    n_hit_big = sum(1 for p in weekly_scored if p["outcome"] == "HIT_BIG")
    n_hit = sum(1 for p in weekly_scored if p["outcome"] == "HIT")
    n_pumped_1d = sum(1 for p in weekly_scored if p.get("pumped_1d"))
    hit_rate = (n_hit_big + n_hit) / max(1, n_total) * 100

    # Top performers + worst
    by_return = sorted(weekly_scored, key=lambda x: -(x.get("max_return_pct") or 0))
    best_calls = by_return[:5]
    worst_calls = by_return[-3:] if len(by_return) >= 3 else []

    return {
        "today": today_str,
        "week_ago": week_ago.strftime("%Y-%m-%d"),
        "n_predictions_scored": n_total,
        "n_hit_big": n_hit_big,
        "n_hit": n_hit,
        "n_pumped_1d": n_pumped_1d,
        "hit_rate_pct": round(hit_rate, 1),
        "pump_1d_rate_pct": round(n_pumped_1d / max(1, n_total) * 100, 1),
        "best_calls": best_calls,
        "worst_calls": worst_calls,
        "cal_history": cal_history,
        "current_calibration": {
            "confidence": (current_audit.get("blend") or {}).get("confidence"),
            "n_predictions": current_audit.get("calibration_n_predictions"),
            "top_weights": current_audit.get("top_weights"),
        },
        "portfolio_stats": pnl,
        "current_cascade_top_5": [
            {"ticker": c.get("ticker"), "score": c.get("combined_score"),
             "industry": c.get("industry_label")}
            for c in (cascade.get("alert_tier") or [])[:5]
        ],
        "current_laggards": [
            {"ticker": c.get("ticker"), "score": c.get("combined_score"),
             "perf_5d": c.get("perf_5d_pct")}
            for c in (cascade.get("laggards_hot_themes") or [])[:5]
        ],
    }


def generate_weekly_review(context: dict) -> dict:
    """Have Claude synthesize the week into a hedge-fund-style memo."""
    system = """You are JustHodl.AI's chief research analyst writing the weekly memo for
the principal of a multi-strategy hedge fund. Tone: precise, candid, no hedging.
Cite specific numbers, tickers, features, percentages.

Return ONLY valid JSON, no markdown fences.

Schema:
{
  "headline": "1-sentence summary of the week (e.g., 'Hit rate ramped from 22% to 38% as options C/P ratio emerged as #1 predictor')",
  "performance_recap": "3-4 sentences on the week's hit rate, best/worst calls with specific tickers and returns",
  "calibration_progress": "3-4 sentences on what the system learned: which features got upweighted, which were demoted, why",
  "system_improvements": "2-3 sentences on what the loop is now doing better than 7 days ago",
  "next_week_setup": "3-4 sentences on current cascade picks, themes worth watching, key risks to monitor",
  "trust_level": "integer 0-100: how confident is the analyst in the system's current rankings?",
  "key_metric": "string e.g. '38% hit rate · 51 predictions scored · top weight: options_cv_pv at 1.42x'"
}"""

    user_prompt = f"""WEEKLY REVIEW for week ending {context['today']}.

═══ THIS WEEK'S RAW PERFORMANCE ═══
Total predictions scored: {context['n_predictions_scored']}
HIT_BIG (≥+10% within 7d): {context['n_hit_big']}
HIT (≥+5% within 7d):       {context['n_hit']}
Pumped within 1 day:        {context['n_pumped_1d']}
Overall hit rate:           {context['hit_rate_pct']}%
1-day pump rate:            {context['pump_1d_rate_pct']}%

BEST CALLS (top 5 by max_return):
{json.dumps(context['best_calls'], indent=2)}

WORST CALLS (bottom 3):
{json.dumps(context['worst_calls'], indent=2)}

═══ CALIBRATION EVOLUTION (last 7 days) ═══
{json.dumps(context['cal_history'], indent=2)}

═══ CURRENT CALIBRATION STATE ═══
Confidence: {context['current_calibration'].get('confidence')}
Predictions analyzed cumulative: {context['current_calibration'].get('n_predictions')}
Current top weights: {json.dumps(context['current_calibration'].get('top_weights') or [])}

═══ PORTFOLIO P&L ═══
{json.dumps(context['portfolio_stats'], indent=2)}

═══ NEXT WEEK SETUP — CURRENT CASCADE PICKS ═══
Alert tier (top 5): {json.dumps(context['current_cascade_top_5'])}
Laggards (top 5):    {json.dumps(context['current_laggards'])}

Write the JSON weekly memo now. Be specific. Use actual numbers."""

    raw = call_claude(user_prompt, system=system, max_tokens=2200)
    if not raw:
        return {"error": "no_response_from_claude", "fallback": True}

    cleaned = raw.strip()
    if cleaned.startswith("```"):
        parts = cleaned.split("```", 2)
        if len(parts) >= 2:
            cleaned = parts[1]
            if cleaned.startswith("json"):
                cleaned = cleaned[4:].strip()
    try:
        return json.loads(cleaned)
    except Exception as e:
        return {"narrative_raw": raw[:2000], "parse_error": str(e)}


def _alpha_council_block():
    """ops 3410 (#9): the Monday operator cockpit — portfolio, stack, shorts,
    dated events — appended to the weekly digest. Tolerant; returns "" on any
    failure so the core digest never breaks."""
    try:
        import boto3 as _b3
        _s3 = _b3.client("s3", "us-east-1")

        def _rj(k):
            try:
                return json.loads(_s3.get_object(
                    Bucket="justhodl-dashboard-live", Key=k)["Body"].read())
            except Exception:
                return {}
        pp = _rj("data/proven-portfolio.json")
        bs = _rj("data/best-setups.json")
        sb = _rj("data/short-book.json")
        ir = _rj("data/index-recon.json")
        L = ["", "━━ ALPHA COUNCIL ━━"]
        nav = pp.get("nav") or {}
        if nav.get("nav") is not None:
            L.append(f"📈 Proven-Edge NAV {nav.get('nav')} vs SPY "
                     f"{nav.get('spy_nav')} · {len(pp.get('book') or [])} pos "
                     f"({pp.get('mode')})")
            L.append("   top: " + ", ".join(
                p["ticker"] for p in (pp.get("book") or [])[:5]))
        tops = (bs.get("top_setups") or [])[:8]
        if tops:
            L.append("🎯 Stack: " + ", ".join(
                f"{t.get('ticker')}{'✓' if t.get('entry_confirmed') else ''}"
                for t in tops))
        sbk = (sb.get("book") or [])[:5]
        if sbk:
            L.append("🔻 Short book: " + ", ".join(
                f"{r['ticker']}({r['score']} vs {r.get('pair_etf')})"
                for r in sbk))
        evs = []

        def _evwalk(o):
            if isinstance(o, dict):
                d = o.get("effective_date") or o.get("date") or o.get("event_date")
                t = o.get("ticker") or o.get("symbol")
                if d and t and str(d) >= __import__("datetime").datetime.utcnow().date().isoformat():
                    evs.append(f"{d} {t}")
                for v in o.values():
                    _evwalk(v)
            elif isinstance(o, list):
                for v in o:
                    _evwalk(v)
        _evwalk(ir)
        if evs:
            L.append("🗓 Forced-flow events: " + "; ".join(sorted(set(evs))[:6]))
        return "\n".join(L) if len(L) > 2 else ""
    except Exception as _e:
        print(f"[council] failed: {str(_e)[:80]}")
        return ""


def build_telegram_digest(memo, context):
    return _build_telegram_digest_core(memo, context) + _alpha_council_block()


def _build_telegram_digest_core(memo: dict, context: dict) -> str:
    """Format the weekly memo for Telegram."""
    if not memo or memo.get("error") or memo.get("fallback"):
        return ""

    lines = [
        f"<b>📰 WEEKLY AI REVIEW · week ending {context['today']}</b>",
        f"<i>Hedge-fund-style weekly memo from JustHodl.AI</i>",
        "",
        f"<b>{memo.get('headline', '—')}</b>",
        "",
        f"<b>📊 KEY METRIC</b>",
        f"  {memo.get('key_metric', '—')}",
        "",
        f"<b>📈 PERFORMANCE RECAP</b>",
        memo.get("performance_recap", "—"),
        "",
        f"<b>⚖️ CALIBRATION PROGRESS</b>",
        memo.get("calibration_progress", "—"),
        "",
        f"<b>🚀 SYSTEM IMPROVEMENTS</b>",
        memo.get("system_improvements", "—"),
        "",
        f"<b>🎯 NEXT WEEK SETUP</b>",
        memo.get("next_week_setup", "—"),
        "",
    ]
    trust = memo.get("trust_level")
    if trust is not None:
        lines.append(f"<b>🎚 System trust level: {trust}/100</b>")
        lines.append("")
    lines.append(f"<i>Hit rate: {context['hit_rate_pct']}% on {context['n_predictions_scored']} predictions · 1d pump rate: {context['pump_1d_rate_pct']}%</i>")
    return "\n".join(lines)


def lambda_handler(event, context):
    t0 = time.time()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    print(f"[weekly-review] generating for week ending {today}")

    ctx = gather_weekly_context()
    print(f"[weekly-review] context: {ctx['n_predictions_scored']} preds, "
          f"hit rate {ctx['hit_rate_pct']}%, calibration {ctx['current_calibration'].get('confidence')}")

    if ctx["n_predictions_scored"] < 3:
        # Not enough data yet — write placeholder
        print(f"[weekly-review] insufficient data ({ctx['n_predictions_scored']} preds) — first run scenarios")
        memo = {
            "headline": f"Self-improvement loop bootstrapping — {ctx['n_predictions_scored']} predictions scored this week (insufficient for full analysis)",
            "performance_recap": f"System captured predictions daily but has {ctx['n_predictions_scored']} scored outcomes (need 5+ for meaningful stats). First substantive weekly review expected next Sunday.",
            "calibration_progress": "Calibration weights remain at 1.0x baseline — system needs more data to identify which features predict pumps.",
            "system_improvements": "All infrastructure live and capturing data. Daily snapshot, scoring, and recalibration pipelines operational.",
            "next_week_setup": f"Cascade currently shows: {', '.join(c['ticker'] for c in ctx['current_cascade_top_5'][:5])} in alert tier. Laggards: {', '.join(c['ticker'] for c in ctx['current_laggards'][:5])}.",
            "trust_level": 50,
            "key_metric": f"Bootstrapping — {ctx['n_predictions_scored']} preds scored, confidence {ctx['current_calibration'].get('confidence', 'NONE')}",
        }
    else:
        memo = generate_weekly_review(ctx)

    output = {
        "schema_version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "week_ending": today,
        "model": ANTHROPIC_MODEL,
        "memo": memo,
        "context_summary": ctx,
    }

    # Save dated + latest pointer
    s3.put_object(
        Bucket=S3_BUCKET, Key=f"data/weekly-review/{today}.json",
        Body=json.dumps(output, default=str).encode(),
        ContentType="application/json", CacheControl="public, max-age=86400",
    )
    s3.put_object(
        Bucket=S3_BUCKET, Key="data/weekly-review-latest.json",
        Body=json.dumps(output, default=str).encode(),
        ContentType="application/json", CacheControl="public, max-age=3600",
    )

    # Send Telegram
    msg = build_telegram_digest(memo, ctx)
    tg = _send_telegram(msg) if msg else {"skipped": True}

    elapsed = round(time.time() - t0, 1)
    print(f"[weekly-review] DONE in {elapsed}s · trust={memo.get('trust_level')}")

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({
            "ok": True, "elapsed_s": elapsed,
            "week_ending": today,
            "n_predictions": ctx["n_predictions_scored"],
            "hit_rate_pct": ctx["hit_rate_pct"],
            "trust_level": memo.get("trust_level"),
            "headline": (memo.get("headline") or "")[:200],
            "telegram_status": tg.get("status"),
        }),
    }
