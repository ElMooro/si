"""justhodl-prepump-alerts-router

Real-time Telegram routing for pre-pump signals NOT covered by the existing
justhodl-alert-router (which handles macro/regime/divergence/short-interest).

This router monitors the 8 most important pre-pump engines:

  1. theme-cascade.json   → alert_tier additions + laggards changes
  2. velocity-acceleration.json → new fresh_fires / confirmed_today /
                                  EMERGING (45-59) / WATCH (30-44) entries
  3. convergence-radar.json → tickers with is_ultra_new=true
  4. early-movers.json      → alert_tier (early_score >= 35)
  5. flow-anomalies/alerts.json → severity >= 7
  6. macro/regime.json      → regime CHANGED vs yesterday
  7. data/catalysts.json    → earnings within 24h for tracked tickers
  8. data/_alerts/convergence-radar-alerted.json → first-time ULTRA alerts

Dedup state: data/_alerts/prepump-router-state.json
  per (signal_type, ticker, date) — once per day per signal

Sends HTML-formatted messages to @Justhodl_bot.
Runs every 30 minutes via EventBridge.
"""
import json
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta
from typing import Optional, List

import boto3

S3_BUCKET = "justhodl-dashboard-live"
STATE_KEY = "data/_alerts/prepump-router-state.json"
TG_BOT_TOKEN = "8679881066:AAHTE6TAhDqs0FuUelTL6Ppt1x8ihis1aGs"  # fallback
TG_CHAT_ID = "8678089260"

s3 = boto3.client("s3", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")


def _read_json(key: str) -> Optional[dict]:
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def _get_telegram_config():
    try:
        token = ssm.get_parameter(Name="/justhodl/telegram/bot-token",
                                    WithDecryption=True)["Parameter"]["Value"]
        chat_id = ssm.get_parameter(Name="/justhodl/telegram/chat_id")["Parameter"]["Value"]
        return token, chat_id
    except Exception:
        return TG_BOT_TOKEN, TG_CHAT_ID


def _html_escape(s: str) -> str:
    if not isinstance(s, str):
        s = str(s)
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _send_telegram(text: str) -> dict:
    token, chat_id = _get_telegram_config()
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = urllib.parse.urlencode({
        "chat_id": chat_id, "text": text, "parse_mode": "HTML",
        "disable_web_page_preview": "true",
    }).encode()
    try:
        req = urllib.request.Request(url, data=data, method="POST")
        with urllib.request.urlopen(req, timeout=12) as r:
            return {"status": r.status, "body": r.read().decode()[:200]}
    except Exception as e:
        return {"error": str(e)[:200]}


def _load_state() -> dict:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    s = _read_json(STATE_KEY) or {}
    if s.get("date") != today:
        # New day — clear daily state but retain history pointer
        return {"date": today, "alerted_by_signal": {}}
    return s


def _save_state(state: dict):
    s3.put_object(
        Bucket=S3_BUCKET, Key=STATE_KEY,
        Body=json.dumps(state, default=str).encode(),
        ContentType="application/json",
    )


def _is_new(state: dict, signal_type: str, key: str) -> bool:
    """Check if (signal_type, key) has been alerted today."""
    alerted = state.get("alerted_by_signal", {}).get(signal_type, [])
    return key not in alerted


def _mark_alerted(state: dict, signal_type: str, key: str):
    state.setdefault("alerted_by_signal", {}).setdefault(signal_type, []).append(key)


# ═════════════════════════════════════════════════════════════════════
# SIGNAL CHECKERS — each returns list of new-alert message lines
# ═════════════════════════════════════════════════════════════════════

def check_cascade_laggards(state: dict) -> List[str]:
    """New entries in theme-cascade.laggards_hot_themes."""
    cascade = _read_json("data/theme-cascade.json")
    if not cascade:
        return []
    laggards = cascade.get("laggards_hot_themes") or []
    lines = []
    new_alerts = []
    for l in laggards[:10]:
        ticker = l.get("ticker")
        if not ticker:
            continue
        # Use ticker as dedup key
        if not _is_new(state, "cascade_laggard", ticker):
            continue
        new_alerts.append(l)
        _mark_alerted(state, "cascade_laggard", ticker)

    if not new_alerts:
        return []

    lines.append(f"<b>🎯 NEW LAGGARDS IN HOT THEMES</b>")
    lines.append(f"<i>{len(new_alerts)} catch-up trade candidates (not pumping yet, in hot themes)</i>")
    lines.append("")
    for l in new_alerts[:5]:
        ticker = _html_escape(l.get("ticker"))
        perf5 = l.get("perf_5d_pct") or 0
        perf20 = l.get("perf_20d_pct") or 0
        n_top10 = l.get("n_etfs_in_top_10") or 0
        hot_etf = _html_escape(l.get("hot_etf") or "?")
        accel = l.get("max_rs_acceleration") or 0
        sizing = (l.get("position_sizing") or {}).get("final_pct") or 0
        lines.append(f"<b>{ticker}</b> · 5d <code>{perf5:+.1f}%</code> · 20d <code>{perf20:+.1f}%</code>")
        lines.append(f"  Hot ETF: <b>{hot_etf}</b> · {n_top10} top-10 ETFs · accel {accel:.0f}")
        lines.append(f"  💰 Size: <b>{sizing:.1f}%</b>")
        lines.append("")
    return lines


def check_velocity_transitions(state: dict) -> List[str]:
    """New entries in velocity-acceleration (FIRED / EMERGING / WATCH)."""
    velocity = _read_json("data/velocity-acceleration.json")
    if not velocity:
        return []
    lines = []
    new_groups = {}

    # FIRED (confirmed today + fresh fires)
    for tier_name, key in [("FIRED_CONFIRMED", "confirmed_today"),
                              ("FIRED_FRESH", "fresh_fires")]:
        for item in (velocity.get(key) or []):
            t = item.get("ticker")
            if not t:
                continue
            dkey = f"velocity_{tier_name}_{t}"
            if not _is_new(state, "velocity", dkey):
                continue
            new_groups.setdefault(tier_name, []).append(item)
            _mark_alerted(state, "velocity", dkey)

    # EMERGING (45-59) — NEW from this session
    for item in (velocity.get("emerging") or []):
        t = item.get("ticker")
        if not t:
            continue
        dkey = f"velocity_EMERGING_{t}"
        if not _is_new(state, "velocity", dkey):
            continue
        new_groups.setdefault("EMERGING", []).append(item)
        _mark_alerted(state, "velocity", dkey)

    # WATCH (30-44) — bottom tier, fire only ONCE when ticker first appears
    for item in (velocity.get("watch") or []):
        t = item.get("ticker")
        if not t:
            continue
        dkey = f"velocity_WATCH_{t}"
        if not _is_new(state, "velocity", dkey):
            continue
        new_groups.setdefault("WATCH", []).append(item)
        _mark_alerted(state, "velocity", dkey)

    if not new_groups:
        return []

    severity_emoji = {"FIRED_CONFIRMED": "🚨", "FIRED_FRESH": "🔥",
                       "EMERGING": "⚡", "WATCH": "👁️"}
    lines.append(f"<b>📡 VELOCITY-ACCELERATION TRANSITIONS</b>")
    lines.append(f"<i>{sum(len(v) for v in new_groups.values())} new tier entries</i>")
    lines.append("")
    for tier_name in ["FIRED_CONFIRMED", "FIRED_FRESH", "EMERGING", "WATCH"]:
        items = new_groups.get(tier_name, [])
        if not items:
            continue
        emoji = severity_emoji.get(tier_name, "•")
        lines.append(f"<b>{emoji} {tier_name}</b> ({len(items)} new)")
        for item in items[:5]:
            ticker = _html_escape(item.get("ticker"))
            score = (item.get("composite_score") or item.get("current_score") or 0)
            theme = _html_escape(item.get("theme_label") or item.get("theme") or "?")
            lines.append(f"  <b>{ticker}</b> · composite <code>{score:.1f}</code> · {theme}")
        lines.append("")
    return lines


def check_convergence_radar(state: dict) -> List[str]:
    """Tickers with is_ultra_new=true in convergence-radar."""
    rad = _read_json("data/convergence-radar.json")
    if not rad:
        return []
    items = rad.get("items") or rad.get("tickers") or rad.get("results") or []
    new_ultras = []
    for i in items:
        if not isinstance(i, dict):
            continue
        if not i.get("is_ultra_new"):
            continue
        ticker = i.get("ticker")
        if not ticker:
            continue
        if not _is_new(state, "convergence_ultra_new", ticker):
            continue
        new_ultras.append(i)
        _mark_alerted(state, "convergence_ultra_new", ticker)

    if not new_ultras:
        return []
    lines = [f"<b>🔥 NEW ULTRA-TIER CONVERGENCE</b>",
              f"<i>{len(new_ultras)} ticker(s) just upgraded to ULTRA tier</i>", ""]
    for r in new_ultras[:5]:
        ticker = _html_escape(r.get("ticker"))
        conv = r.get("convergence_score") or 0
        n_eng = r.get("n_engines") or 0
        prior = r.get("prior_n_engines") or 0
        cat = _html_escape(r.get("pump_category") or "?")
        lines.append(f"<b>{ticker}</b> · convergence <code>{conv:.1f}</code> · "
                     f"engines <code>{prior}→{n_eng}</code> · {cat}")
    lines.append("")
    return lines


def check_early_movers(state: dict) -> List[str]:
    """alert_tier (early_score >= 35) from early-movers."""
    em = _read_json("data/early-movers.json")
    if not em:
        return []
    alert_tier = em.get("alert_tier") or []
    new_alerts = []
    for c in alert_tier[:10]:
        ticker = c.get("ticker")
        if not ticker:
            continue
        if not _is_new(state, "early_mover_alert", ticker):
            continue
        new_alerts.append(c)
        _mark_alerted(state, "early_mover_alert", ticker)

    if not new_alerts:
        return []
    lines = [f"<b>🎯 EARLY-MOVERS ALERT TIER</b>",
              f"<i>{len(new_alerts)} new acceleration-scored candidates</i>", ""]
    for c in new_alerts[:5]:
        ticker = _html_escape(c.get("ticker"))
        score = c.get("early_score") or 0
        factors = (c.get("factors") or [])[:3]
        lines.append(f"<b>{ticker}</b> · score <code>{score}</code> · "
                     f"{_html_escape(', '.join(factors))}")
    lines.append("")
    return lines


def check_flow_anomalies(state: dict) -> List[str]:
    """High-severity flow anomalies (sev >= 7)."""
    fa = _read_json("flow-anomalies/alerts.json") or _read_json("flow-anomalies/daily.json")
    if not fa:
        return []
    anomalies = fa.get("alerts") or fa.get("anomalies") or []
    new_alerts = []
    for a in anomalies:
        if not isinstance(a, dict):
            continue
        sev = a.get("severity") or 0
        if sev < 7:
            continue
        # Dedup key: type + ticker (or just type if no ticker)
        ticker = a.get("ticker") or a.get("symbol") or ""
        atype = a.get("type") or a.get("anomaly_type") or "?"
        dkey = f"{atype}_{ticker}"
        if not _is_new(state, "flow_anomaly", dkey):
            continue
        new_alerts.append(a)
        _mark_alerted(state, "flow_anomaly", dkey)

    if not new_alerts:
        return []
    lines = [f"<b>⚠️ FLOW ANOMALIES (sev ≥ 7)</b>",
              f"<i>{len(new_alerts)} new high-severity anomalies</i>", ""]
    for a in new_alerts[:5]:
        atype = _html_escape(a.get("type") or a.get("anomaly_type") or "?")
        ticker = _html_escape(a.get("ticker") or a.get("symbol") or "")
        sev = a.get("severity") or 0
        msg = _html_escape((a.get("message") or a.get("description") or "")[:120])
        lines.append(f"<b>{atype}</b> [sev <code>{sev}</code>] {ticker}")
        if msg:
            lines.append(f"  {msg}")
    lines.append("")
    return lines


def check_macro_regime(state: dict) -> List[str]:
    """Detect regime changes vs yesterday's regime."""
    macro = _read_json("macro/regime.json")
    if not macro:
        return []
    current = (macro.get("top_level_regime") or {}).get("regime")
    if not current:
        return []
    # Yesterday's regime from state
    yesterday_regime = state.get("last_macro_regime")
    state["last_macro_regime"] = current
    if yesterday_regime and yesterday_regime != current:
        if not _is_new(state, "regime_change",
                       f"{yesterday_regime}_to_{current}"):
            return []
        _mark_alerted(state, "regime_change", f"{yesterday_regime}_to_{current}")
        return [
            f"<b>🌐 MACRO REGIME CHANGE</b>",
            f"<i>{yesterday_regime} → {current}</i>",
            "",
            f"Sub-regime scores affected: see /flows.html for full breakdown",
            "",
        ]
    return []


def check_earnings_imminent(state: dict) -> List[str]:
    """Earnings within 24h for any cascade-tracked ticker."""
    cats = _read_json("data/catalysts.json")
    if not cats:
        return []
    # Get list of cascade-tracked tickers
    cascade = _read_json("data/theme-cascade.json") or {}
    tracked = set()
    for tier_key in ["alert_tier", "medium_tier", "watch_tier", "laggards_hot_themes"]:
        for c in (cascade.get(tier_key) or []):
            t = c.get("ticker")
            if t:
                tracked.add(t)

    new_earnings = []
    calendar = cats.get("calendar") or cats.get("items") or []
    for item in calendar:
        if not isinstance(item, dict):
            continue
        t = item.get("ticker") or item.get("symbol")
        if not t or t not in tracked:
            continue
        days_out = item.get("days_out") or item.get("days_until")
        if days_out is None or days_out > 1:
            continue
        if not _is_new(state, "earnings_imminent", t):
            continue
        new_earnings.append(item)
        _mark_alerted(state, "earnings_imminent", t)

    if not new_earnings:
        return []
    lines = [f"<b>📅 EARNINGS WITHIN 24H</b>",
              f"<i>{len(new_earnings)} cascade-tracked tickers reporting</i>", ""]
    for e in new_earnings[:5]:
        ticker = _html_escape(e.get("ticker") or e.get("symbol"))
        when = _html_escape(e.get("when") or e.get("time") or "?")
        days = e.get("days_out") or e.get("days_until")
        lines.append(f"<b>{ticker}</b> · {days}d out · {when}")
    lines.append("")
    return lines


# ═════════════════════════════════════════════════════════════════════
# MAIN HANDLER
# ═════════════════════════════════════════════════════════════════════

def lambda_handler(event, context):
    t0 = time.time()
    print(f"[prepump-router] starting at {datetime.now(timezone.utc).isoformat()}")

    state = _load_state()

    # Collect all alert messages by checker
    sections = []
    counts = {}

    for name, checker in [
        ("cascade_laggards", check_cascade_laggards),
        ("velocity_transitions", check_velocity_transitions),
        ("convergence_radar", check_convergence_radar),
        ("early_movers", check_early_movers),
        ("flow_anomalies", check_flow_anomalies),
        ("macro_regime", check_macro_regime),
        ("earnings_imminent", check_earnings_imminent),
    ]:
        try:
            lines = checker(state)
            if lines:
                sections.append(lines)
                counts[name] = len([l for l in lines if l.strip().startswith("<b>")])
            print(f"  {name}: {len(lines)} lines")
        except Exception as e:
            print(f"  {name}: error {str(e)[:120]}")
            counts[name] = f"error: {str(e)[:80]}"

    if not sections:
        print(f"[prepump-router] no new alerts")
        _save_state(state)
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"ok": True, "n_alerts": 0,
                                "elapsed_s": round(time.time() - t0, 1)}),
        }

    # Build combined message
    header = [
        "<b>📡 JUSTHODL PRE-PUMP SIGNALS</b>",
        f"<i>{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}</i>",
        "",
    ]
    body_lines = []
    for s in sections:
        body_lines.extend(s)

    # Telegram has a 4096 char limit per message — split if needed
    full = "\n".join(header + body_lines).strip()
    results = []
    if len(full) <= 3800:
        results.append(_send_telegram(full))
    else:
        # Send header + each section as separate message
        results.append(_send_telegram("\n".join(header).strip()))
        for s in sections:
            chunk = "\n".join(s).strip()
            if len(chunk) > 3800:
                chunk = chunk[:3800] + "..."
            time.sleep(0.5)
            results.append(_send_telegram(chunk))

    state["last_send"] = datetime.now(timezone.utc).isoformat()
    state["last_send_results"] = results[:3]
    _save_state(state)

    elapsed = round(time.time() - t0, 1)
    print(f"[prepump-router] sent {len(results)} messages in {elapsed}s")
    print(f"[prepump-router] counts: {counts}")

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({
            "ok": True, "elapsed_s": elapsed,
            "n_messages_sent": len(results),
            "counts": counts,
            "first_result_status": results[0].get("status") if results else None,
        }),
    }
