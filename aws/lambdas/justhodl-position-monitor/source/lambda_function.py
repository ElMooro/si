"""justhodl-position-monitor

Proactive Telegram alerts on signal-portfolio events:
  - A position approaches its stop-loss (within 2% of stop)
  - A position approaches its target (within 2% of target)
  - A position breaks through its stop or target
  - The decisive call changes (TRIM → EXIT, etc.)

Schedule: cron(*/30 * * * ? *) — every 30 minutes
Reads:
  - portfolio/signal-portfolio-state.json (positions with stop_price/target_price/current_price)
  - data/decisive-call-history.json (latest 2 entries to detect call changes)
Maintains:
  - portfolio/position-monitor-state.json (per-position last-alert timestamp + last-known status)
                                          (last_call_verb_alerted)

Dedupe: Once an alert fires for a given (signal_id, alert_kind), don't refire
within 6 hours unless status flips back and forth.

Telegram destination: SSM /justhodl/telegram/chat_id + bot token from env or
/justhodl/telegram/bot_token. Reuses alert-router's pattern.
"""
import json
import os
import time
import urllib.request
from datetime import datetime, timezone, timedelta
from decimal import Decimal

import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
DEDUP_HOURS = 6
STATE_KEY = "portfolio/position-monitor-state.json"

# Thresholds
NEAR_STOP_PCT = 0.02     # within 2% of stop
NEAR_TARGET_PCT = 0.02   # within 2% of target

S3 = boto3.client("s3", region_name=REGION)
SSM = boto3.client("ssm", region_name=REGION)
TG_BOT_TOKEN = os.environ.get("TELEGRAM_TOKEN") or os.environ.get("TELEGRAM_BOT_TOKEN")


def get_chat_id():
    try:
        return SSM.get_parameter(Name="/justhodl/telegram/chat_id")["Parameter"]["Value"]
    except Exception as e:
        print(f"[ssm] chat_id: {e}")
        return None


def get_token():
    if TG_BOT_TOKEN:
        return TG_BOT_TOKEN
    try:
        return SSM.get_parameter(Name="/justhodl/telegram/bot_token", WithDecryption=True)["Parameter"]["Value"]
    except Exception:
        return None


def send_telegram(text, chat_id):
    token = get_token()
    if not token or not chat_id:
        print("[tg] missing token/chat_id")
        return False, "missing creds"
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        body = json.dumps({
            "chat_id": chat_id,
            "text": text[:4096],
            "parse_mode": "Markdown",
            "disable_web_page_preview": True,
        }).encode()
        req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            return True, resp.read().decode()[:200]
    except Exception as e:
        # Fall back without parse_mode in case Markdown breaks
        try:
            payload = {"chat_id": chat_id, "text": text[:4096], "disable_web_page_preview": True}
            req = urllib.request.Request(
                f"https://api.telegram.org/bot{token}/sendMessage",
                data=json.dumps(payload).encode(),
                headers={"Content-Type": "application/json"},
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                return True, resp.read().decode()[:200]
        except Exception as e2:
            return False, f"{e} / fallback: {e2}"


def load_json(key, default=None):
    try:
        obj = S3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception as e:
        print(f"[load] {key}: {e}")
        return default if default is not None else {}


def write_json(key, data, cache_max_age=300):
    body = json.dumps(data, default=str).encode("utf-8")
    S3.put_object(
        Bucket=BUCKET, Key=key, Body=body,
        ContentType="application/json",
        CacheControl=f"public, max-age={cache_max_age}",
    )


def num(v):
    try:
        return float(v) if v is not None else None
    except Exception:
        return None


def evaluate_position(p):
    """Return list of alert events for one position. Each event is:
        {kind: 'near_stop'|'stop_hit'|'near_target'|'target_hit',
         severity: 'warning'|'critical', text: str}
    """
    events = []
    cur = num(p.get("current_price"))
    stop = num(p.get("stop_price"))
    tgt = num(p.get("target_price"))
    direction = (p.get("direction") or "LONG").upper()

    if cur is None or cur == 0:
        return events

    if direction == "LONG":
        # Stop is BELOW current — distance = (cur - stop) / cur, if cur < stop → already broke
        if stop and stop > 0:
            if cur <= stop:
                events.append({"kind": "stop_hit", "severity": "critical",
                               "stop": stop, "cur": cur})
            elif cur <= stop * (1 + NEAR_STOP_PCT):
                events.append({"kind": "near_stop", "severity": "warning",
                               "stop": stop, "cur": cur,
                               "dist_pct": ((cur - stop) / cur) * 100})
        if tgt and tgt > 0:
            if cur >= tgt:
                events.append({"kind": "target_hit", "severity": "info",
                               "target": tgt, "cur": cur})
            elif cur >= tgt * (1 - NEAR_TARGET_PCT):
                events.append({"kind": "near_target", "severity": "info",
                               "target": tgt, "cur": cur,
                               "dist_pct": ((tgt - cur) / cur) * 100})
    else:  # SHORT
        if stop and stop > 0:
            if cur >= stop:
                events.append({"kind": "stop_hit", "severity": "critical",
                               "stop": stop, "cur": cur})
            elif cur >= stop * (1 - NEAR_STOP_PCT):
                events.append({"kind": "near_stop", "severity": "warning",
                               "stop": stop, "cur": cur,
                               "dist_pct": ((stop - cur) / cur) * 100})
        if tgt and tgt > 0:
            if cur <= tgt:
                events.append({"kind": "target_hit", "severity": "info",
                               "target": tgt, "cur": cur})
            elif cur <= tgt * (1 + NEAR_TARGET_PCT):
                events.append({"kind": "near_target", "severity": "info",
                               "target": tgt, "cur": cur,
                               "dist_pct": ((cur - tgt) / cur) * 100})
    return events


def fmt_position_alert(p, ev):
    ticker = p.get("ticker", "?")
    source = p.get("source", "?")
    direction = p.get("direction", "LONG")
    cur = num(p.get("current_price"))
    pnl_pct = num(p.get("current_pnl_pct"))
    days = p.get("days_held")
    max_hold = p.get("max_hold_days")

    icons = {"stop_hit": "🛑", "near_stop": "⚠️", "target_hit": "🎯", "near_target": "✨"}
    titles = {
        "stop_hit": "STOP HIT — exit immediately",
        "near_stop": "Near stop-loss",
        "target_hit": "TARGET HIT — take profits",
        "near_target": "Approaching target",
    }
    icon = icons.get(ev["kind"], "ℹ️")
    title = titles.get(ev["kind"], ev["kind"])

    lines = [
        f"{icon} *{title}*",
        "",
        f"*{ticker}* ({direction}) · {source}",
        f"Current: ${cur:.2f}  PnL: {pnl_pct:+.2f}%" if pnl_pct is not None else f"Current: ${cur:.2f}",
    ]
    if ev["kind"] in ("stop_hit", "near_stop"):
        lines.append(f"Stop: ${ev['stop']:.2f}")
    if ev["kind"] in ("target_hit", "near_target"):
        lines.append(f"Target: ${ev['target']:.2f}")
    if "dist_pct" in ev:
        lines.append(f"Distance: {ev['dist_pct']:.2f}%")
    if days is not None:
        lines.append(f"Held {days}/{max_hold or '?'} days")
    lines.append("")
    lines.append("→ See https://justhodl.ai/performance.html")
    return "\n".join(lines)


def fmt_call_change_alert(prev, cur):
    icon = {
        "EXIT_ALL_RISK": "🚨", "EXIT": "🔴", "TRIM": "🟠", "HEDGE": "🟡",
        "WAIT": "🟣", "HOLD": "⚪", "LONG": "🟢", "LOAD": "🟢", "LEVER": "🚀",
    }.get(cur.get("call_verb"), "🔵")
    return (
        f"{icon} *Call changed*\n\n"
        f"`{prev.get('call_verb', '?')}` → *`{cur.get('call_verb', '?')}`*\n\n"
        f"Phase: {cur.get('phase', '—')}\n"
        f"Khalid Index: {cur.get('khalid_score', '—')}\n"
        f"Top signal: {cur.get('highest_weight_signal', '—')}\n"
        f"Weighted accuracy: {(cur.get('weighted_mean_accuracy', 0) or 0) * 100:.1f}%\n\n"
        f"→ Read full brief: https://justhodl.ai/brief.html\n"
        f"→ Call history: https://justhodl.ai/calls.html"
    )


def lambda_handler(event=None, context=None):
    started = time.time()
    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()
    print(f"[position-monitor] starting at {now_iso}")

    # 1. Pull current portfolio state
    portfolio = load_json("portfolio/signal-portfolio-state.json")
    open_positions = portfolio.get("open_positions") or []
    print(f"[position-monitor] {len(open_positions)} open positions")

    # 2. Pull dedup state
    state = load_json(STATE_KEY, default={"v": "1.0", "alerts": {}, "last_call_verb": None})
    sent_alerts = state.get("alerts", {})  # key: f"{signal_id}:{kind}" -> {sent_at, ...}
    last_call_verb = state.get("last_call_verb")

    chat_id = get_chat_id()

    cutoff = now - timedelta(hours=DEDUP_HOURS)
    # Garbage-collect old alert state entries
    fresh = {}
    for key, info in sent_alerts.items():
        try:
            sent_at = datetime.fromisoformat(info.get("sent_at", "").replace("Z", "+00:00"))
            if sent_at >= cutoff:
                fresh[key] = info
        except Exception:
            continue
    sent_alerts = fresh

    # 3. Evaluate every open position
    new_alerts_sent = []
    for p in open_positions:
        sid = p.get("signal_id") or f"{p.get('ticker')}:{p.get('entry_date')}"
        events = evaluate_position(p)
        for ev in events:
            key = f"{sid}:{ev['kind']}"
            if key in sent_alerts:
                continue  # already alerted within dedup window
            text = fmt_position_alert(p, ev)
            ok, info = send_telegram(text, chat_id) if chat_id else (False, "no_chat_id")
            sent_alerts[key] = {
                "sent_at": now_iso,
                "ticker": p.get("ticker"),
                "kind": ev["kind"],
                "severity": ev["severity"],
                "ok": ok,
            }
            new_alerts_sent.append({"ticker": p.get("ticker"), "kind": ev["kind"], "ok": ok})
            print(f"[position-monitor] {p.get('ticker')} {ev['kind']} → tg:{ok}")

    # 4. Detect call changes from decisive-call-history
    call_history = load_json("data/decisive-call-history.json")
    call_snaps = sorted(call_history.get("snapshots") or [],
                        key=lambda x: x.get("timestamp") or "")
    call_change_alert = None
    if call_snaps:
        latest = call_snaps[-1]
        cur_verb = latest.get("call_verb")
        # If call has changed since our last seen, alert
        if cur_verb and cur_verb != last_call_verb and last_call_verb is not None:
            prev_snap = None
            for s in reversed(call_snaps[:-1]):
                if s.get("call_verb") != cur_verb:
                    prev_snap = s
                    break
            prev_snap = prev_snap or {"call_verb": last_call_verb}
            text = fmt_call_change_alert(prev_snap, latest)
            ok, info = send_telegram(text, chat_id) if chat_id else (False, "no_chat_id")
            call_change_alert = {"from": prev_snap.get("call_verb"), "to": cur_verb, "ok": ok}
            print(f"[position-monitor] CALL CHANGE {prev_snap.get('call_verb')} → {cur_verb}: tg:{ok}")
        # Update last seen
        state["last_call_verb"] = cur_verb

    # 5. Save state
    state["alerts"] = sent_alerts
    state["last_run"] = now_iso
    state["v"] = "1.0"
    write_json(STATE_KEY, state)

    duration = round(time.time() - started, 2)
    return {
        "statusCode": 200,
        "body": json.dumps({
            "ok": True,
            "n_open_positions": len(open_positions),
            "n_position_alerts": len(new_alerts_sent),
            "alerts": new_alerts_sent,
            "call_change": call_change_alert,
            "duration_s": duration,
        }, default=str),
    }
