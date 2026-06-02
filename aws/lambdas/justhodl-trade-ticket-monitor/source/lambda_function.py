"""justhodl-trade-ticket-monitor

Real-time monitor for every active trade ticket. Polls Polygon prices every
10 minutes during US trading hours, compares to entry/stop/TP1/TP2/TP3 levels,
and fires Telegram alerts when key levels are hit.

ALERT TYPES (state-aware, fires ONCE per level per ticker per day):

  🚨 STOP_BREACHED        Price crossed below stop loss — CUT LOSS NOW
  ⚠️  APPROACHING_STOP    Price within 1.5% of stop — heads up
  ✅ TP1_HIT              Price hit TP1 — take 33% off
  ✅ TP2_HIT              Price hit TP2 — take another 33% off
  🎯 TP3_HIT              Price hit TP3 (moonshot) — final exit
  📈 BIG_GAIN_EARLY       Up 5%+ before any TP — momentum confirmation

OUTPUT:
  data/trade-monitor-snapshots.json   — current ticker P&L snapshot
  Telegram alerts → @Justhodl_bot
  State → data/_alerts/trade-monitor-state.json
"""
import json
import os
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, List, Dict

import boto3

S3_BUCKET = "justhodl-dashboard-live"
STATE_KEY = "data/_alerts/trade-monitor-state.json"
SNAPSHOT_KEY = "data/trade-monitor-snapshots.json"
POLYGON_KEY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
TG_BOT_TOKEN = "8679881066:AAHTE6TAhDqs0FuUelTL6Ppt1x8ihis1aGs"
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
        return {"date": today, "alerted_by_ticker": {}}
    return s


def _save_state(state: dict):
    s3.put_object(
        Bucket=S3_BUCKET, Key=STATE_KEY,
        Body=json.dumps(state, default=str).encode(),
        ContentType="application/json",
    )


def _is_alerted(state: dict, ticker: str, level: str) -> bool:
    return level in state.get("alerted_by_ticker", {}).get(ticker, [])


def _mark_alerted(state: dict, ticker: str, level: str):
    state.setdefault("alerted_by_ticker", {}).setdefault(ticker, []).append(level)


def fetch_current_price(ticker: str) -> Optional[float]:
    """Fetch latest price via Polygon snapshot endpoint."""
    url = (f"https://api.polygon.io/v3/snapshot/locale/us/markets/stocks/tickers/{ticker}"
           f"?apiKey={POLYGON_KEY}")
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            data = json.loads(r.read().decode())
        results = data.get("results")
        if results:
            # Try different price fields based on data freshness
            session = results.get("session") or {}
            last_trade = results.get("last_trade") or {}
            last_quote = results.get("last_quote") or {}
            # Order of preference: last_trade > session close > prev close
            price = (last_trade.get("price") or
                     session.get("price") or
                     session.get("close") or
                     session.get("last") or
                     results.get("value"))
            if price:
                return float(price)
        return None
    except Exception as e:
        print(f"[price] {ticker}: {e}")
        return None


def fetch_polygon_aggs_latest(ticker: str) -> Optional[float]:
    """Fallback: fetch latest daily aggregate."""
    url = (f"https://api.polygon.io/v2/aggs/ticker/{ticker}/prev"
           f"?adjusted=true&apiKey={POLYGON_KEY}")
    try:
        with urllib.request.urlopen(url, timeout=8) as r:
            data = json.loads(r.read().decode())
        results = data.get("results") or []
        if results:
            return results[0].get("c")
    except Exception:
        pass
    return None


def evaluate_ticket(ticket: dict, current_price: float, state: dict) -> List[dict]:
    """Compare current price to ticket levels and return list of alert dicts."""
    ticker = ticket.get("ticker")
    if not ticker or not current_price:
        return []

    entry = ticket.get("entry") or 0
    stop = ticket.get("stop_loss") or 0
    tp1 = ticket.get("tp1") or 0
    tp2 = ticket.get("tp2") or 0
    tp3 = ticket.get("tp3") or 0

    alerts = []

    # Compute P&L from entry
    if entry > 0:
        pnl_pct = ((current_price - entry) / entry) * 100
    else:
        pnl_pct = 0

    # 1. Stop loss breach (price <= stop)
    if stop > 0 and current_price <= stop:
        if not _is_alerted(state, ticker, "STOP_BREACHED"):
            alerts.append({
                "ticker": ticker, "type": "STOP_BREACHED", "severity": "P1",
                "current_price": current_price, "level": stop,
                "pnl_pct": pnl_pct, "ticket": ticket,
            })
            _mark_alerted(state, ticker, "STOP_BREACHED")

    # 2. Approaching stop (within 1.5% of stop)
    elif stop > 0 and current_price <= stop * 1.015:
        if not _is_alerted(state, ticker, "APPROACHING_STOP"):
            alerts.append({
                "ticker": ticker, "type": "APPROACHING_STOP", "severity": "P2",
                "current_price": current_price, "level": stop,
                "pnl_pct": pnl_pct, "ticket": ticket,
            })
            _mark_alerted(state, ticker, "APPROACHING_STOP")

    # 3. TP3 hit (moonshot)
    if tp3 > 0 and current_price >= tp3:
        if not _is_alerted(state, ticker, "TP3_HIT"):
            alerts.append({
                "ticker": ticker, "type": "TP3_HIT", "severity": "P1",
                "current_price": current_price, "level": tp3,
                "pnl_pct": pnl_pct, "ticket": ticket,
            })
            _mark_alerted(state, ticker, "TP3_HIT")
    # 4. TP2 hit
    elif tp2 > 0 and current_price >= tp2:
        if not _is_alerted(state, ticker, "TP2_HIT"):
            alerts.append({
                "ticker": ticker, "type": "TP2_HIT", "severity": "P1",
                "current_price": current_price, "level": tp2,
                "pnl_pct": pnl_pct, "ticket": ticket,
            })
            _mark_alerted(state, ticker, "TP2_HIT")
    # 5. TP1 hit
    elif tp1 > 0 and current_price >= tp1:
        if not _is_alerted(state, ticker, "TP1_HIT"):
            alerts.append({
                "ticker": ticker, "type": "TP1_HIT", "severity": "P1",
                "current_price": current_price, "level": tp1,
                "pnl_pct": pnl_pct, "ticket": ticket,
            })
            _mark_alerted(state, ticker, "TP1_HIT")
    # 6. Big early gain (>=5% but no TP hit yet)
    elif pnl_pct >= 5:
        if not _is_alerted(state, ticker, "BIG_GAIN_EARLY"):
            alerts.append({
                "ticker": ticker, "type": "BIG_GAIN_EARLY", "severity": "P3",
                "current_price": current_price, "level": entry,
                "pnl_pct": pnl_pct, "ticket": ticket,
            })
            _mark_alerted(state, ticker, "BIG_GAIN_EARLY")

    return alerts


def format_alert_message(alerts: List[dict]) -> str:
    """Build single Telegram message for batched alerts."""
    if not alerts:
        return ""

    type_emoji = {
        "STOP_BREACHED": "🚨", "APPROACHING_STOP": "⚠️",
        "TP1_HIT": "✅", "TP2_HIT": "✅", "TP3_HIT": "🎯",
        "BIG_GAIN_EARLY": "📈",
    }
    type_action = {
        "STOP_BREACHED": "CUT LOSS NOW — stop breached",
        "APPROACHING_STOP": "Heads up — price within 1.5% of stop",
        "TP1_HIT": "Take 33% off (TP1 hit)",
        "TP2_HIT": "Take another 33% off (TP2 hit)",
        "TP3_HIT": "Final exit — MOONSHOT TP3 hit",
        "BIG_GAIN_EARLY": "Big gain — momentum confirmation",
    }

    # Group by severity
    by_sev = {"P1": [], "P2": [], "P3": []}
    for a in alerts:
        by_sev[a.get("severity", "P3")].append(a)

    lines = [f"<b>📊 TRADE TICKET MONITOR</b>",
              f"<i>{datetime.now(timezone.utc).strftime('%H:%M UTC')} · {len(alerts)} level(s) hit</i>",
              ""]
    for sev in ["P1", "P2", "P3"]:
        for a in by_sev.get(sev, []):
            t = a.get("type", "?")
            ticker = a.get("ticker", "?")
            emoji = type_emoji.get(t, "•")
            action = type_action.get(t, "")
            cp = a.get("current_price") or 0
            level = a.get("level") or 0
            pnl = a.get("pnl_pct") or 0
            ticket = a.get("ticket") or {}
            shares = ticket.get("shares") or 0
            lines.append(f"{emoji} <b>{ticker}</b> · {t}")
            lines.append(f"  Current <b>${cp:.2f}</b> · Level <b>${level:.2f}</b> · P&L <code>{pnl:+.2f}%</code>")
            if t.startswith("TP") and shares > 0:
                profit = (cp - (ticket.get("entry") or cp)) * shares
                lines.append(f"  💰 ${profit:+,.0f} on {shares} shares")
            elif t in ("STOP_BREACHED", "APPROACHING_STOP") and shares > 0:
                loss = (cp - (ticket.get("entry") or cp)) * shares
                lines.append(f"  💸 ${loss:+,.0f} on {shares} shares")
            lines.append(f"  <i>{action}</i>")
            lines.append("")
    return "\n".join(lines).strip()


def lambda_handler(event, context):
    t0 = time.time()
    print(f"[monitor] starting at {datetime.now(timezone.utc).isoformat()}")

    # Load tickets
    tickets_doc = _read_json("data/trade-tickets.json")
    if not tickets_doc:
        return {"statusCode": 200, "body": json.dumps({"ok": True, "msg": "no tickets"})}

    tickets = tickets_doc.get("tickets") or []
    if not tickets:
        return {"statusCode": 200, "body": json.dumps({"ok": True, "msg": "empty tickets"})}

    print(f"[monitor] watching {len(tickets)} active tickets")

    # Fetch current prices in parallel
    def _fetch(t):
        ticker = t.get("ticker")
        price = fetch_current_price(ticker)
        if not price:
            price = fetch_polygon_aggs_latest(ticker)
        return ticker, price

    prices = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        for ticker, price in ex.map(_fetch, tickets):
            if price:
                prices[ticker] = price

    print(f"[monitor] fetched prices for {len(prices)}/{len(tickets)} tickers")

    # Load state, evaluate all tickets
    state = _load_state()
    all_alerts = []
    snapshots = []

    for t in tickets:
        ticker = t.get("ticker")
        current_price = prices.get(ticker)
        if not current_price:
            continue
        entry = t.get("entry") or 0
        pnl_pct = ((current_price - entry) / entry * 100) if entry > 0 else 0

        snapshots.append({
            "ticker": ticker,
            "current_price": current_price,
            "entry": entry,
            "stop_loss": t.get("stop_loss"),
            "tp1": t.get("tp1"),
            "tp2": t.get("tp2"),
            "tp3": t.get("tp3"),
            "pnl_pct": round(pnl_pct, 2),
            "shares": t.get("shares"),
            "pnl_usd": round((current_price - entry) * (t.get("shares") or 0), 2),
            "max_loss_usd": t.get("max_loss_usd"),
            "tier": t.get("tier"),
        })

        alerts = evaluate_ticket(t, current_price, state)
        all_alerts.extend(alerts)

    # Send Telegram if any alerts
    tg_result = None
    if all_alerts:
        msg = format_alert_message(all_alerts)
        tg_result = _send_telegram(msg)
        print(f"[monitor] Telegram sent: {tg_result}")

    # Persist state + snapshots
    _save_state(state)
    s3.put_object(
        Bucket=S3_BUCKET, Key=SNAPSHOT_KEY,
        Body=json.dumps({
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "n_watched": len(tickets),
            "n_priced": len(prices),
            "n_alerts": len(all_alerts),
            "snapshots": snapshots,
            "alerts_just_fired": all_alerts,
        }, default=str).encode(),
        ContentType="application/json", CacheControl="public, max-age=120",
    )

    elapsed = round(time.time() - t0, 1)
    print(f"[monitor] DONE — {len(snapshots)} snapshots, {len(all_alerts)} alerts in {elapsed}s")

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({
            "ok": True, "elapsed_s": elapsed,
            "n_watched": len(tickets),
            "n_priced": len(prices),
            "n_alerts": len(all_alerts),
            "alerts_by_type": {
                t: sum(1 for a in all_alerts if a["type"] == t)
                for t in set(a["type"] for a in all_alerts)
            } if all_alerts else {},
            "telegram_status": tg_result.get("status") if tg_result else None,
        }),
    }
