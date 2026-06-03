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

def get_active_cascade() -> dict:
    """Return calibrated cascade if confidence >= MEDIUM, else original.

    Reads cascade-recalibration-audit.json to check calibration confidence.
    Once self-improvement has scored 20+ predictions, the system blends in
    learned weights. This consumer auto-switches without code changes.
    """
    try:
        audit = _read_json("data/cascade-recalibration-audit.json") or {}
        confidence = (audit.get("blend") or {}).get("confidence", "NONE")
        if confidence in ("MEDIUM", "HIGH"):
            cal = _read_json("data/theme-cascade-calibrated.json")
            if cal:
                print(f"[adaptive-cascade] using CALIBRATED cascade (confidence={confidence})")
                return cal
        print(f"[adaptive-cascade] using ORIGINAL cascade (confidence={confidence})")
    except Exception as e:
        print(f"[adaptive-cascade] err {e} — falling back to original")
    return _read_json("data/theme-cascade.json") or {}




# ═════════════════════════════════════════════════════════════════════
# TRADE TICKET INTEGRATION — embed entry/stop/TP1/TP2/TP3 into alerts
# ═════════════════════════════════════════════════════════════════════
_TRADE_TICKETS_CACHE = {"loaded": False, "by_ticker": {}}


def _load_trade_tickets() -> dict:
    """Load tickets keyed by ticker. Cached for the Lambda invocation."""
    if _TRADE_TICKETS_CACHE["loaded"]:
        return _TRADE_TICKETS_CACHE["by_ticker"]
    doc = _read_json("data/trade-tickets.json")
    by_ticker = {}
    if doc:
        for t in (doc.get("tickets") or []):
            tk = t.get("ticker")
            if tk:
                by_ticker[tk] = t
    _TRADE_TICKETS_CACHE["loaded"] = True
    _TRADE_TICKETS_CACHE["by_ticker"] = by_ticker
    return by_ticker


def _format_trade_ticket(ticker: str) -> List[str]:
    """Return Telegram lines for a ticker's trade ticket. Empty if not found."""
    tickets = _load_trade_tickets()
    t = tickets.get(ticker)
    if not t or t.get("error"):
        return []
    entry = t.get("entry") or 0
    stop = t.get("stop_loss") or 0
    risk_pct = t.get("risk_pct") or 0
    tp1 = t.get("tp1") or 0
    tp2 = t.get("tp2") or 0
    tp3 = t.get("tp3") or 0
    tp3_pct = t.get("tp3_pct") or 0
    rr = t.get("rr_tp3") or 0
    shares = t.get("shares") or 0
    maxloss = t.get("max_loss_usd") or 0
    return [
        f"  📍 Entry <b>${entry:.2f}</b> · 🛑 Stop <b>${stop:.2f}</b> ({-risk_pct:.1f}%)",
        f"  🎯 TP1 <code>${tp1:.0f}</code> · TP2 <code>${tp2:.0f}</code> · TP3 <code>${tp3:.0f}</code> (+{tp3_pct:.1f}%)",
        f"  R:R <b>{rr:.1f}:1</b> · {shares} sh · max loss <b>${maxloss:,.0f}</b>",
    ]


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
    cascade = get_active_cascade()
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
        # Append trade ticket if available
        ticket_lines = _format_trade_ticket(l.get("ticker"))
        if ticket_lines:
            lines.extend(ticket_lines)
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
            ticker_raw = item.get("ticker")
            ticker = _html_escape(ticker_raw)
            score = (item.get("composite_score") or item.get("current_score") or 0)
            theme = _html_escape(item.get("theme_label") or item.get("theme") or "?")
            lines.append(f"  <b>{ticker}</b> · composite <code>{score:.1f}</code> · {theme}")
            # Embed trade ticket for FIRED tier (high conviction) and EMERGING (worth tracking)
            if tier_name in ("FIRED_CONFIRMED", "FIRED_FRESH", "EMERGING"):
                ticket_lines = _format_trade_ticket(ticker_raw)
                if ticket_lines:
                    lines.extend(ticket_lines)
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
        ticker_raw = r.get("ticker")
        ticker = _html_escape(ticker_raw)
        conv = r.get("convergence_score") or 0
        n_eng = r.get("n_engines") or 0
        prior = r.get("prior_n_engines") or 0
        cat = _html_escape(r.get("pump_category") or "?")
        lines.append(f"<b>{ticker}</b> · convergence <code>{conv:.1f}</code> · "
                     f"engines <code>{prior}→{n_eng}</code> · {cat}")
        # ULTRA upgrades get full trade ticket
        ticket_lines = _format_trade_ticket(ticker_raw)
        if ticket_lines:
            lines.extend(ticket_lines)
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
        ticker_raw = c.get("ticker")
        ticker = _html_escape(ticker_raw)
        score = c.get("early_score") or 0
        factors = (c.get("factors") or [])[:3]
        lines.append(f"<b>{ticker}</b> · score <code>{score}</code> · "
                     f"{_html_escape(', '.join(factors))}")
        ticket_lines = _format_trade_ticket(ticker_raw)
        if ticket_lines:
            lines.extend(ticket_lines)
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
    cascade = get_active_cascade()
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


def check_options_flow(state: dict) -> List[str]:
    """Polygon options-flow extreme + bullish call activity."""
    doc = _read_json("data/polygon-options-flow.json")
    if not doc:
        return []
    extreme = doc.get("extreme_call_flow") or []
    bullish = doc.get("bullish_call_flow") or []
    new_alerts = []
    for c in (extreme + bullish):
        t = c.get("ticker")
        if not t:
            continue
        if not _is_new(state, "options_flow", t):
            continue
        new_alerts.append(c)
        _mark_alerted(state, "options_flow", t)

    if not new_alerts:
        return []
    lines = [f"<b>📊 UNUSUAL OPTIONS ACTIVITY</b>",
              f"<i>{len(new_alerts)} ticker(s) with bullish call flow detected</i>", ""]
    for c in new_alerts[:6]:
        ticker_raw = c.get("ticker")
        ticker = _html_escape(ticker_raw)
        cv = c.get("call_vol") or 0
        cv_pv = c.get("cv_pv_ratio") or 0
        sigs = (c.get("signals") or [])[:2]
        lines.append(f"<b>{ticker}</b> · call_vol <code>{cv:,}</code> · C/P <code>{cv_pv}</code>")
        if sigs:
            lines.append(f"  {_html_escape(' · '.join(sigs))}")
        # Options flow is THE pre-pump signal — always include ticket
        ticket_lines = _format_trade_ticket(ticker_raw)
        if ticket_lines:
            lines.extend(ticket_lines)
    lines.append("")
    return lines


def check_fx_regime(state: dict) -> List[str]:
    """Polygon FX regime — alert on new signals only."""
    doc = _read_json("data/polygon-fx-regime.json")
    if not doc:
        return []
    signals = doc.get("regime_signals") or []
    new_alerts = []
    for s in signals:
        # Strip values to dedup on signal type
        sig_type = s.split(" ")[0]
        if not _is_new(state, "fx_regime", sig_type):
            continue
        new_alerts.append(s)
        _mark_alerted(state, "fx_regime", sig_type)

    if not new_alerts:
        return []
    lines = [f"<b>💱 FX REGIME SHIFT</b>",
              f"<i>{len(new_alerts)} new currency signals</i>", ""]
    for s in new_alerts[:5]:
        lines.append(f"  • {_html_escape(s)}")
    lines.append("")
    return lines


def check_futures_curves(state: dict) -> List[str]:
    """Polygon futures curves — VIX backwardation, oil, metals breakouts."""
    doc = _read_json("data/polygon-futures-curves.json")
    if not doc:
        return []
    signals = doc.get("signals") or []
    new_alerts = []
    for s in signals:
        sig_type = s.split(" ")[0]
        if not _is_new(state, "futures_curves", sig_type):
            continue
        new_alerts.append(s)
        _mark_alerted(state, "futures_curves", sig_type)

    if not new_alerts:
        return []
    lines = [f"<b>⏩ FUTURES CURVE SIGNALS</b>",
              f"<i>{len(new_alerts)} new term-structure signals</i>", ""]
    for s in new_alerts[:5]:
        lines.append(f"  • {_html_escape(s)}")
    lines.append("")
    return lines


# ═════════════════════════════════════════════════════════════════════
# ALPHA SIGNALS — activist, insider, squeeze, GEX, breadth, warnings
# ═════════════════════════════════════════════════════════════════════

def check_activist_filings(state: dict) -> List[str]:
    """New 13D filings — activist took meaningful position (>5% stake)."""
    doc = _read_json("data/activist-13d.json")
    if not doc:
        return []
    items = (doc.get("filings") or doc.get("items") or doc.get("results")
              or doc.get("recent_filings") or [])
    new_alerts = []
    for f in items[:15]:
        if not isinstance(f, dict): continue
        t = f.get("ticker") or f.get("symbol") or f.get("issuer_ticker")
        filer = f.get("filer") or f.get("activist") or f.get("filer_name") or ""
        if not t: continue
        dkey = f"{t}_{filer[:20]}"
        if not _is_new(state, "activist_13d", dkey): continue
        new_alerts.append(f)
        _mark_alerted(state, "activist_13d", dkey)
    if not new_alerts: return []
    lines = [f"<b>🎯 NEW ACTIVIST 13D FILINGS</b>",
              f"<i>{len(new_alerts)} new activist positions</i>", ""]
    for f in new_alerts[:5]:
        t = _html_escape(f.get("ticker") or f.get("symbol") or "?")
        filer = _html_escape((f.get("filer") or f.get("activist") or "Unknown")[:40])
        pct = f.get("ownership_pct") or f.get("stake_pct") or f.get("percent_class")
        lines.append(f"<b>{t}</b> · {filer}" + (f" · <b>{pct}%</b> stake" if pct else ""))
    lines.append("")
    return lines


def check_insider_clusters(state: dict) -> List[str]:
    """Multiple insiders buying same ticker — high-conviction signal."""
    doc = _read_json("data/insider-clusters.json")
    if not doc:
        return []
    items = (doc.get("clusters") or doc.get("items") or doc.get("results") or [])
    new_alerts = []
    for c in items[:15]:
        if not isinstance(c, dict): continue
        t = c.get("ticker") or c.get("symbol")
        n_buyers = c.get("n_insiders") or c.get("cluster_size") or c.get("n_buyers") or 0
        if not t or n_buyers < 2: continue
        if not _is_new(state, "insider_cluster", t): continue
        new_alerts.append(c)
        _mark_alerted(state, "insider_cluster", t)
    if not new_alerts: return []
    lines = [f"<b>💼 INSIDER BUYING CLUSTERS</b>",
              f"<i>{len(new_alerts)} stocks with multiple insiders buying</i>", ""]
    for c in new_alerts[:6]:
        t = _html_escape(c.get("ticker") or c.get("symbol"))
        n = c.get("n_insiders") or c.get("cluster_size") or c.get("n_buyers") or 0
        amount = c.get("total_value_usd") or c.get("dollar_volume") or 0
        lines.append(f"<b>{t}</b> · <b>{n}</b> insiders buying" + 
                     (f" · ${amount/1e6:.1f}M total" if amount > 1e5 else ""))
    lines.append("")
    return lines


def check_squeeze_pretrigger(state: dict) -> List[str]:
    """Pre-squeeze setups — high short interest + technical setup."""
    doc = _read_json("data/squeeze-pretrigger.json")
    if not doc:
        return []
    items = (doc.get("candidates") or doc.get("setups") or doc.get("items")
              or doc.get("results") or [])
    new_alerts = []
    for s in items[:15]:
        if not isinstance(s, dict): continue
        t = s.get("ticker") or s.get("symbol")
        score = s.get("score") or s.get("composite_score") or s.get("setup_score") or 0
        if not t or score < 60: continue  # only high-quality setups
        if not _is_new(state, "squeeze_pretrigger", t): continue
        new_alerts.append(s)
        _mark_alerted(state, "squeeze_pretrigger", t)
    if not new_alerts: return []
    lines = [f"<b>🌀 PRE-SQUEEZE SETUPS</b>",
              f"<i>{len(new_alerts)} new pre-squeeze candidates (score ≥ 60)</i>", ""]
    for s in new_alerts[:5]:
        t = _html_escape(s.get("ticker") or s.get("symbol"))
        score = s.get("score") or s.get("composite_score") or s.get("setup_score") or 0
        si_pct = s.get("short_interest_pct") or s.get("short_pct") or s.get("si_pct")
        days_cover = s.get("days_to_cover") or s.get("days_cover")
        extras = []
        if si_pct: extras.append(f"SI {si_pct:.1f}%")
        if days_cover: extras.append(f"{days_cover:.1f}d cover")
        lines.append(f"<b>{t}</b> · score <code>{score:.0f}</code>" + 
                     (" · " + " · ".join(extras) if extras else ""))
    lines.append("")
    return lines


def check_dealer_gex(state: dict) -> List[str]:
    """Dealer gamma exposure regime changes — major market mover."""
    doc = _read_json("data/dealer-gex.json")
    if not doc:
        return []
    regime = (doc.get("regime") or doc.get("current_regime") or 
              doc.get("gex_state") or "")
    gex_value = doc.get("total_gex") or doc.get("aggregate_gex") or doc.get("gex_$")
    flip_point = doc.get("zero_gamma_level") or doc.get("flip_point")
    
    yesterday_regime = state.get("last_gex_regime")
    state["last_gex_regime"] = regime
    if regime and yesterday_regime and yesterday_regime != regime:
        dkey = f"{yesterday_regime}_to_{regime}"
        if not _is_new(state, "dealer_gex", dkey):
            return []
        _mark_alerted(state, "dealer_gex", dkey)
        lines = [f"<b>📐 DEALER GAMMA REGIME CHANGE</b>",
                  f"<i>{yesterday_regime} → <b>{regime}</b></i>", ""]
        if gex_value:
            lines.append(f"  GEX: ${gex_value/1e9:.2f}B")
        if flip_point:
            lines.append(f"  Zero gamma level: {flip_point}")
        regime_meaning = {
            "POSITIVE_GAMMA": "dealers buy dips, sell rips — volatility suppressed",
            "NEGATIVE_GAMMA": "dealers sell dips, buy rips — volatility amplified",
            "EXTREME_POSITIVE": "extreme pinning, low vol",
            "EXTREME_NEGATIVE": "fragile, large moves likely",
        }
        meaning = regime_meaning.get(regime.upper(), "")
        if meaning:
            lines.append(f"  <i>{meaning}</i>")
        lines.append("")
        return lines
    return []


def check_redflag_alerter(state: dict) -> List[str]:
    """Accounting fraud / red-flag alerts from 8K filings + Beneish."""
    doc = _read_json("data/redflag-alerter.json") or _read_json("data/redflags.json")
    if not doc:
        return []
    items = doc.get("alerts") or doc.get("flags") or doc.get("items") or []
    new_alerts = []
    for f in items[:15]:
        if not isinstance(f, dict): continue
        t = f.get("ticker") or f.get("symbol")
        sev = f.get("severity") or f.get("score") or 0
        if not t or sev < 5: continue
        if not _is_new(state, "redflag", t): continue
        new_alerts.append(f)
        _mark_alerted(state, "redflag", t)
    if not new_alerts: return []
    lines = [f"<b>🚩 RED-FLAG / ACCOUNTING ALERTS</b>",
              f"<i>{len(new_alerts)} new warnings (avoid blowups)</i>", ""]
    for f in new_alerts[:5]:
        t = _html_escape(f.get("ticker") or f.get("symbol"))
        sev = f.get("severity") or f.get("score") or 0
        reason = _html_escape((f.get("reason") or f.get("flag_type") or "")[:80])
        lines.append(f"<b>{t}</b> · sev <code>{sev}</code>" + (f" · {reason}" if reason else ""))
    lines.append("")
    return lines


def check_divcut_warning(state: dict) -> List[str]:
    """Dividend cut warnings — avoid blowups."""
    doc = _read_json("data/divcut-warning.json")
    if not doc:
        return []
    items = doc.get("warnings") or doc.get("at_risk") or doc.get("items") or []
    new_alerts = []
    for d in items[:15]:
        if not isinstance(d, dict): continue
        t = d.get("ticker") or d.get("symbol")
        risk_score = d.get("risk_score") or d.get("score") or 0
        if not t or risk_score < 60: continue
        if not _is_new(state, "divcut", t): continue
        new_alerts.append(d)
        _mark_alerted(state, "divcut", t)
    if not new_alerts: return []
    lines = [f"<b>✂️ DIVIDEND CUT WARNINGS</b>",
              f"<i>{len(new_alerts)} dividends at risk (score ≥ 60)</i>", ""]
    for d in new_alerts[:5]:
        t = _html_escape(d.get("ticker") or d.get("symbol"))
        score = d.get("risk_score") or d.get("score") or 0
        yield_pct = d.get("yield_pct") or d.get("dividend_yield")
        lines.append(f"<b>{t}</b> · risk <code>{score:.0f}</code>" + 
                     (f" · yield {yield_pct:.1f}%" if yield_pct else ""))
    lines.append("")
    return lines


def check_breadth_thrust(state: dict) -> List[str]:
    """Zweig breadth thrust — rare bullish signal."""
    doc = _read_json("data/breadth-thrust.json")
    if not doc:
        return []
    triggered = doc.get("triggered") or doc.get("thrust_triggered") or False
    score = doc.get("thrust_score") or doc.get("composite") or 0
    if not triggered and score < 80:
        return []
    today_key = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if not _is_new(state, "breadth_thrust", today_key):
        return []
    _mark_alerted(state, "breadth_thrust", today_key)
    lines = [f"<b>🚀 BREADTH THRUST FIRED</b>",
              f"<i>Zweig-style breadth thrust — rare bullish signal</i>", ""]
    advances = doc.get("advances_decline_ratio") or doc.get("a_d_ratio")
    sessions = doc.get("trigger_sessions") or doc.get("days_to_fire")
    if advances:
        lines.append(f"  Advances/Decline: {advances:.2f}")
    if sessions:
        lines.append(f"  Fired in {sessions} sessions")
    lines.append(f"  <i>Historical: 90%+ probability of higher prices 6m+ out</i>")
    lines.append("")
    return lines


def check_capitulation(state: dict) -> List[str]:
    """Capitulation buys — extreme washout, mean-reversion opportunity."""
    doc = _read_json("data/capitulation.json")
    if not doc:
        return []
    items = doc.get("candidates") or doc.get("signals") or doc.get("items") or []
    new_alerts = []
    for c in items[:15]:
        if not isinstance(c, dict): continue
        t = c.get("ticker") or c.get("symbol")
        score = c.get("score") or c.get("capitulation_score") or 0
        if not t or score < 70: continue
        if not _is_new(state, "capitulation", t): continue
        new_alerts.append(c)
        _mark_alerted(state, "capitulation", t)
    if not new_alerts: return []
    lines = [f"<b>💥 CAPITULATION BUYS</b>",
              f"<i>{len(new_alerts)} extreme washout candidates (mean-rev opp)</i>", ""]
    for c in new_alerts[:5]:
        t = _html_escape(c.get("ticker") or c.get("symbol"))
        score = c.get("score") or c.get("capitulation_score") or 0
        dd = c.get("drawdown_pct") or c.get("max_drawdown")
        lines.append(f"<b>{t}</b> · score <code>{score:.0f}</code>" + 
                     (f" · DD {dd:.1f}%" if dd else ""))
    lines.append("")
    return lines


def check_52wk_breakout(state: dict) -> List[str]:
    """52-week quality breakouts (fundamentals + 52wk high)."""
    doc = _read_json("data/52wk-quality-breakout.json")
    if not doc:
        return []
    items = doc.get("breakouts") or doc.get("candidates") or doc.get("items") or []
    new_alerts = []
    for b in items[:15]:
        if not isinstance(b, dict): continue
        t = b.get("ticker") or b.get("symbol")
        score = b.get("quality_score") or b.get("score") or 0
        if not t or score < 70: continue
        if not _is_new(state, "52wk_breakout", t): continue
        new_alerts.append(b)
        _mark_alerted(state, "52wk_breakout", t)
    if not new_alerts: return []
    lines = [f"<b>📈 52WK QUALITY BREAKOUTS</b>",
              f"<i>{len(new_alerts)} new highs with strong fundamentals</i>", ""]
    for b in new_alerts[:6]:
        t = _html_escape(b.get("ticker") or b.get("symbol"))
        score = b.get("quality_score") or b.get("score") or 0
        lines.append(f"<b>{t}</b> · quality <code>{score:.0f}</code>")
    lines.append("")
    return lines


def check_crisis_composite(state: dict) -> List[str]:
    """Composite crisis score level changes."""
    doc = _read_json("data/crisis-composite.json")
    if not doc:
        return []
    score = doc.get("composite_score") or doc.get("score") or 0
    regime = (doc.get("regime") or doc.get("crisis_state") or 
              doc.get("level") or "")
    if not regime:
        # Compute regime from score
        if score >= 80: regime = "ACUTE_CRISIS"
        elif score >= 60: regime = "ELEVATED_STRESS"
        elif score >= 40: regime = "WATCH"
        else: regime = "CALM"
    yesterday_regime = state.get("last_crisis_regime")
    state["last_crisis_regime"] = regime
    if yesterday_regime and yesterday_regime != regime:
        dkey = f"{yesterday_regime}_to_{regime}"
        if not _is_new(state, "crisis_composite", dkey): return []
        _mark_alerted(state, "crisis_composite", dkey)
        lines = [f"<b>🌐 CRISIS COMPOSITE REGIME CHANGE</b>",
                  f"<i>{yesterday_regime} → <b>{regime}</b> (score {score:.0f})</i>", ""]
        components = doc.get("components") or doc.get("sub_scores") or {}
        for k, v in list(components.items())[:6]:
            if isinstance(v, (int, float)):
                lines.append(f"  • {_html_escape(k)}: {v:.1f}")
        lines.append("")
        return lines
    # Also alert if score crosses 70+ for first time today
    if score >= 70:
        dkey = f"high_score_{datetime.now(timezone.utc).strftime('%Y-%m-%d')}"
        if _is_new(state, "crisis_composite", dkey):
            _mark_alerted(state, "crisis_composite", dkey)
            return [
                f"<b>⚠️ CRISIS COMPOSITE ELEVATED</b>",
                f"<i>Score {score:.0f} ({regime})</i>", "",
            ]
    return []


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
        ("polygon_options_flow", check_options_flow),
        ("polygon_fx_regime", check_fx_regime),
        ("polygon_futures_curves", check_futures_curves),
        # Alpha signals (Tier 1) — institutional positioning + warnings
        ("activist_filings", check_activist_filings),
        ("insider_clusters", check_insider_clusters),
        ("squeeze_pretrigger", check_squeeze_pretrigger),
        ("dealer_gex", check_dealer_gex),
        ("redflag_alerter", check_redflag_alerter),
        ("divcut_warning", check_divcut_warning),
        ("breadth_thrust", check_breadth_thrust),
        ("capitulation", check_capitulation),
        ("52wk_breakout", check_52wk_breakout),
        ("crisis_composite", check_crisis_composite),
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
