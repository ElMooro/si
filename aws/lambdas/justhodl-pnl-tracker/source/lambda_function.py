"""justhodl-pnl-tracker

Simulated portfolio P&L tracker.

PHILOSOPHY: If you had taken EVERY cascade alert exactly as the system
recommended (entry at trade-ticket price, shares from sizing, exits at
TP1/TP2/TP3 or stop loss), what would your realized P&L be?

This measures pure SYSTEM performance — no slippage, no missed entries,
no emotional overrides. It's the empirical benchmark.

AUTO-ENTRY RULES (simulated):
  Open a position when ticker is FIRST alerted in either:
    - cascade alert_tier (combined ≥ 80)
    - cascade laggards
    - velocity FIRED_CONFIRMED or FIRED_FRESH
  Use trade-ticket entry/shares for sizing.

EXIT RULES:
  TP1_HIT → sell 33% at TP1
  TP2_HIT → sell another 33% at TP2
  TP3_HIT → sell final 34% at TP3
  STOP_BREACHED → exit 100% at stop price
  Manual override (via state file) supported

OUTPUT:
  data/simulated-portfolio.json — open + closed positions, daily P&L
  data/pnl-stats.json — running win rate, expectancy, etc.
  Daily Telegram digest after market close

Schedule: weekdays 16:30 ET (cron 30 21 * * MON-FRI *)
"""
import json
import os
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone, date
from typing import Optional, List, Dict

import boto3

S3_BUCKET = "justhodl-dashboard-live"
PORTFOLIO_KEY = "data/simulated-portfolio.json"
STATS_KEY = "data/pnl-stats.json"
TG_BOT_TOKEN = "8679881066:AAHTE6TAhDqs0FuUelTL6Ppt1x8ihis1aGs"
TG_CHAT_ID = "8678089260"

s3 = boto3.client("s3", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")


def _read_json(key: str) -> Optional[dict]:
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def _write_json(key: str, data: dict, ttl: int = 3600):
    s3.put_object(
        Bucket=S3_BUCKET, Key=key,
        Body=json.dumps(data, default=str).encode(),
        ContentType="application/json",
        CacheControl=f"public, max-age={ttl}",
    )


def _get_tg_config():
    try:
        token = ssm.get_parameter(Name="/justhodl/telegram/bot-token",
                                    WithDecryption=True)["Parameter"]["Value"]
        chat_id = ssm.get_parameter(Name="/justhodl/telegram/chat_id")["Parameter"]["Value"]
        return token, chat_id
    except Exception:
        return TG_BOT_TOKEN, TG_CHAT_ID


def _send_telegram(text: str) -> dict:
    token, chat_id = _get_tg_config()
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


def get_alerted_tickets_today() -> Dict[str, str]:
    """Return {ticker: tier} of all tickets alerted today via cascade/velocity."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    alerted = {}

    # Cascade alert tier
    cascade_state = _read_json("data/_alerts/theme-cascade-alerted.json") or {}
    if cascade_state.get("date") == today:
        for t in (cascade_state.get("alerted_tickers") or []):
            alerted[t] = "ALERT_TIER"

    # Prepump router state — has cascade_laggard, velocity, etc.
    router_state = _read_json("data/_alerts/prepump-router-state.json") or {}
    if router_state.get("date") == today:
        by_signal = router_state.get("alerted_by_signal", {})

        for t in (by_signal.get("cascade_laggard") or []):
            if t not in alerted:
                alerted[t] = "LAGGARD"

        # Velocity signals: "velocity_FIRED_CONFIRMED_TICKER"
        for sig in (by_signal.get("velocity") or []):
            parts = sig.split("_", 2)
            if len(parts) >= 3 and parts[0] == "velocity":
                tier = parts[1]
                # Velocity sigs are formatted like "velocity_FIRED_CONFIRMED_MS" or "velocity_FIRED_FRESH_X"
                # Extract: skip first 2 (velocity_FIRED) and rest is CONFIRMED_TICKER or FRESH_TICKER
                rest = sig[len("velocity_"):]
                # rest = "FIRED_CONFIRMED_MS" or "WATCH_CRWD"
                if rest.startswith("FIRED_CONFIRMED_"):
                    t = rest[len("FIRED_CONFIRMED_"):]
                    if t and t not in alerted:
                        alerted[t] = "FIRED_CONFIRMED"
                elif rest.startswith("FIRED_FRESH_"):
                    t = rest[len("FIRED_FRESH_"):]
                    if t and t not in alerted:
                        alerted[t] = "FIRED_FRESH"
                elif rest.startswith("EMERGING_"):
                    t = rest[len("EMERGING_"):]
                    if t and t not in alerted:
                        alerted[t] = "EMERGING"
                # Skip WATCH tier — too early for auto-entry

    return alerted


def get_executed_levels_today() -> Dict[str, List[str]]:
    """Return {ticker: [level1, level2,...]} of levels hit today."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    state = _read_json("data/_alerts/trade-monitor-state.json") or {}
    if state.get("date") != today:
        return {}
    return state.get("alerted_by_ticker") or {}


def load_or_init_portfolio() -> dict:
    """Load simulated portfolio or initialize empty."""
    p = _read_json(PORTFOLIO_KEY)
    if not p:
        p = {
            "schema_version": "1.0",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "open_positions": [],
            "closed_positions": [],
            "realized_pnl_total_usd": 0,
            "stats": {},
        }
    return p


def open_new_positions(portfolio: dict, alerted_today: Dict[str, str],
                        tickets: List[dict]) -> List[str]:
    """For each newly-alerted ticker not in portfolio, open a simulated position."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    open_tickers = {p["ticker"] for p in portfolio.get("open_positions", [])}
    closed_today_tickers = {p["ticker"] for p in portfolio.get("closed_positions", [])
                              if p.get("close_date") == today}
    tickets_by_ticker = {t["ticker"]: t for t in tickets}

    newly_opened = []
    for ticker, tier in alerted_today.items():
        if ticker in open_tickers or ticker in closed_today_tickers:
            continue
        ticket = tickets_by_ticker.get(ticker)
        if not ticket or ticket.get("error"):
            continue
        position = {
            "ticker": ticker,
            "entry_date": today,
            "entry_price": ticket.get("entry"),
            "shares_total": ticket.get("shares"),
            "shares_remaining": ticket.get("shares"),
            "stop_loss": ticket.get("stop_loss"),
            "tp1": ticket.get("tp1"),
            "tp2": ticket.get("tp2"),
            "tp3": ticket.get("tp3"),
            "tier": tier,
            "entry_tier": tier,
            "exits": [],
            "realized_pnl_usd": 0,
            "status": "OPEN",
        }
        portfolio["open_positions"].append(position)
        newly_opened.append(ticker)
    return newly_opened


def execute_exits(portfolio: dict, executed_today: Dict[str, List[str]],
                  snapshots: Dict[str, float]) -> List[dict]:
    """Process TP/stop hits → apply partial/full exits, realize P&L."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    exit_events = []
    new_open = []
    new_closed = list(portfolio.get("closed_positions", []))

    for pos in portfolio.get("open_positions", []):
        ticker = pos["ticker"]
        levels_hit = executed_today.get(ticker, [])
        entry = pos.get("entry_price") or 0
        shares_remaining = pos.get("shares_remaining") or 0
        total_shares = pos.get("shares_total") or shares_remaining or 1

        # Process each level hit (in order if both TP1 + TP2 hit, sells each portion)
        # We only process levels NOT already in pos.exits
        prior_exit_types = {e["type"] for e in (pos.get("exits") or [])}

        for level in levels_hit:
            if level in prior_exit_types:
                continue

            if level == "TP1_HIT":
                shares_to_sell = int(total_shares * 0.33)
                exit_price = pos.get("tp1") or 0
            elif level == "TP2_HIT":
                shares_to_sell = int(total_shares * 0.33)
                exit_price = pos.get("tp2") or 0
            elif level == "TP3_HIT":
                shares_to_sell = shares_remaining
                exit_price = pos.get("tp3") or 0
            elif level == "STOP_BREACHED":
                shares_to_sell = shares_remaining
                exit_price = pos.get("stop_loss") or 0
            else:
                continue  # APPROACHING_STOP, BIG_GAIN_EARLY are not exits

            shares_to_sell = min(shares_to_sell, shares_remaining)
            if shares_to_sell <= 0 or exit_price <= 0:
                continue

            pnl_per_share = exit_price - entry
            pnl = pnl_per_share * shares_to_sell

            exit_record = {
                "type": level,
                "date": today,
                "shares_sold": shares_to_sell,
                "exit_price": exit_price,
                "pnl_usd": round(pnl, 2),
                "pnl_pct": round((exit_price - entry) / entry * 100, 2) if entry > 0 else 0,
            }
            pos.setdefault("exits", []).append(exit_record)
            pos["shares_remaining"] = shares_remaining - shares_to_sell
            pos["realized_pnl_usd"] = round(
                (pos.get("realized_pnl_usd") or 0) + pnl, 2)
            shares_remaining -= shares_to_sell
            exit_events.append({"ticker": ticker, "exit": exit_record})

        # If position fully closed, move to closed
        if pos.get("shares_remaining") and pos["shares_remaining"] <= 0:
            pos["status"] = "CLOSED"
            pos["close_date"] = today
            new_closed.append(pos)
        else:
            new_open.append(pos)

    portfolio["open_positions"] = new_open
    portfolio["closed_positions"] = new_closed
    return exit_events


def compute_stats(portfolio: dict) -> dict:
    """Compute aggregate P&L stats from closed positions."""
    closed = portfolio.get("closed_positions") or []
    open_pos = portfolio.get("open_positions") or []
    n_closed = len(closed)
    n_open = len(open_pos)

    realized_total = sum(p.get("realized_pnl_usd") or 0 for p in closed) + \
                     sum(p.get("realized_pnl_usd") or 0 for p in open_pos)

    if not closed:
        return {
            "n_total_trades": n_open,
            "n_open": n_open, "n_closed": 0,
            "n_winners": 0, "n_losers": 0,
            "win_rate_pct": 0,
            "total_realized_usd": round(realized_total, 2),
        }

    winners = [p for p in closed if (p.get("realized_pnl_usd") or 0) > 0]
    losers = [p for p in closed if (p.get("realized_pnl_usd") or 0) < 0]

    avg_winner = (sum((p["realized_pnl_usd"] or 0) for p in winners) / len(winners)) if winners else 0
    avg_loser = (sum((p["realized_pnl_usd"] or 0) for p in losers) / len(losers)) if losers else 0

    win_rate = (len(winners) / n_closed * 100) if n_closed > 0 else 0
    # Expectancy = (win_rate * avg_winner) - (loss_rate * |avg_loser|)
    if n_closed > 0:
        expectancy = (win_rate / 100 * avg_winner) + ((1 - win_rate / 100) * avg_loser)
    else:
        expectancy = 0

    return {
        "n_total_trades": n_closed + n_open,
        "n_open": n_open,
        "n_closed": n_closed,
        "n_winners": len(winners),
        "n_losers": len(losers),
        "win_rate_pct": round(win_rate, 1),
        "avg_winner_usd": round(avg_winner, 2),
        "avg_loser_usd": round(avg_loser, 2),
        "expectancy_per_trade_usd": round(expectancy, 2),
        "total_realized_usd": round(realized_total, 2),
        "best_winner": max(winners, key=lambda p: p.get("realized_pnl_usd") or 0,
                            default={"ticker": None}).get("ticker") if winners else None,
        "worst_loser": min(losers, key=lambda p: p.get("realized_pnl_usd") or 0,
                            default={"ticker": None}).get("ticker") if losers else None,
    }


def build_daily_digest(portfolio: dict, stats: dict,
                       new_opens: List[str], exit_events: List[dict]) -> str:
    """Build Telegram digest message."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    n_open = stats.get("n_open", 0)
    realized = stats.get("total_realized_usd", 0)

    lines = [
        f"<b>💼 DAILY P&L DIGEST · {today}</b>",
        f"<i>Simulated portfolio — if every alert was taken exactly</i>",
        "",
    ]

    # New opens today
    if new_opens:
        lines.append(f"<b>📥 NEW POSITIONS TODAY ({len(new_opens)})</b>")
        for t in new_opens[:8]:
            pos = next((p for p in portfolio.get("open_positions", [])
                        if p["ticker"] == t), None)
            if pos:
                lines.append(f"  • <b>{t}</b> · entry ${pos.get('entry_price'):.2f} · "
                             f"{pos.get('shares_total')} sh · {pos.get('tier')}")
        lines.append("")

    # Exit events today
    if exit_events:
        lines.append(f"<b>📤 EXITS TODAY ({len(exit_events)})</b>")
        for e in exit_events[:8]:
            exit_info = e.get("exit", {})
            t = e.get("ticker", "?")
            etype = exit_info.get("type", "?")
            pnl_usd = exit_info.get("pnl_usd", 0)
            pnl_pct = exit_info.get("pnl_pct", 0)
            emoji = "🎯" if etype == "TP3_HIT" else "✅" if etype.startswith("TP") else "🚨"
            lines.append(f"  {emoji} <b>{t}</b> · {etype} · "
                         f"<code>{pnl_usd:+,.0f}</code> ({pnl_pct:+.1f}%)")
        lines.append("")

    # Aggregate stats
    lines.append(f"<b>📊 PORTFOLIO STATS</b>")
    lines.append(f"  Open positions: <b>{n_open}</b>")
    lines.append(f"  Total realized P&L: <b>${realized:,.0f}</b>")
    if stats.get("n_closed", 0) > 0:
        lines.append(f"  Win rate: <b>{stats.get('win_rate_pct', 0):.1f}%</b> "
                     f"({stats.get('n_winners')}W / {stats.get('n_losers')}L)")
        lines.append(f"  Avg winner: <code>${stats.get('avg_winner_usd', 0):+,.0f}</code> · "
                     f"Avg loser: <code>${stats.get('avg_loser_usd', 0):+,.0f}</code>")
        lines.append(f"  Expectancy/trade: <b>${stats.get('expectancy_per_trade_usd', 0):+,.0f}</b>")
        if stats.get("best_winner"):
            lines.append(f"  🏆 Best: <b>{stats['best_winner']}</b>")
        if stats.get("worst_loser"):
            lines.append(f"  ⚠ Worst: <b>{stats['worst_loser']}</b>")
    else:
        lines.append(f"  <i>No closed positions yet — will populate as TP/stops fire</i>")
    lines.append("")
    lines.append(f"<i>Open positions tracked in pre-pump-radar.html</i>")

    return "\n".join(lines).strip()


def lambda_handler(event, context):
    t0 = time.time()
    print(f"[pnl] starting at {datetime.now(timezone.utc).isoformat()}")

    # Load inputs
    tickets_doc = _read_json("data/trade-tickets.json") or {}
    tickets = tickets_doc.get("tickets") or []
    snapshots_doc = _read_json("data/trade-monitor-snapshots.json") or {}
    snapshots = {s["ticker"]: s.get("current_price")
                 for s in (snapshots_doc.get("snapshots") or [])}

    # Find alerted tickers + executed levels for today
    alerted_today = get_alerted_tickets_today()
    executed_today = get_executed_levels_today()
    print(f"[pnl] alerted_today: {len(alerted_today)} tickers")
    print(f"[pnl] executed_today: {len(executed_today)} tickers with level hits")

    # Load portfolio
    portfolio = load_or_init_portfolio()

    # Open new positions
    newly_opened = open_new_positions(portfolio, alerted_today, tickets)
    print(f"[pnl] newly opened: {newly_opened}")

    # Execute exits from monitor
    exit_events = execute_exits(portfolio, executed_today, snapshots)
    print(f"[pnl] exit events: {len(exit_events)}")

    # Compute stats
    stats = compute_stats(portfolio)
    portfolio["stats"] = stats
    portfolio["last_updated"] = datetime.now(timezone.utc).isoformat()
    portfolio["realized_pnl_total_usd"] = stats.get("total_realized_usd", 0)

    # Save
    _write_json(PORTFOLIO_KEY, portfolio)
    _write_json(STATS_KEY, stats)

    # Send daily digest
    msg = build_daily_digest(portfolio, stats, newly_opened, exit_events)
    tg = _send_telegram(msg)
    print(f"[pnl] telegram: {tg}")

    elapsed = round(time.time() - t0, 1)
    print(f"[pnl] DONE — opened={len(newly_opened)} exits={len(exit_events)} "
          f"realized=${stats.get('total_realized_usd', 0):.0f} in {elapsed}s")

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({
            "ok": True, "elapsed_s": elapsed,
            "newly_opened": newly_opened,
            "n_exits": len(exit_events),
            "stats": stats,
            "telegram_status": tg.get("status") if tg else None,
        }),
    }
