"""
justhodl-portfolio-catalysts — Position-aware catalyst aggregator

═══════════════════════════════════════════════════════════════════════
LINKS YOUR POSITIONS TO UPCOMING MARKET CATALYSTS
─────────────────────────────────────────────────
Reads positions + watchlist from DDB and cross-references with:
  - data/catalyst-calendar.json    (FOMC, auctions, witching, rebalance)
  - data/earnings-tracker.json     (upcoming earnings 14d window)
  - data/earnings-pead.json        (post-earnings drift signals)
  - data/earnings-whisper.json     (analyst whisper numbers)

For each position/watchlist symbol, builds a forward-looking catalyst
schedule. Fires Telegram at T-3 days, T-1 days, and morning-of for any
material event affecting the book.

═══════════════════════════════════════════════════════════════════════
ALERT LADDER (per catalyst event, 24h dedupe)
─────────────────────────────────────────────
  T-7 days  → no alert (just sidecar)
  T-3 days  → 🗓 advance notice ("earnings in 3 days")
  T-1 day   → ⏰ pre-event preview (whisper #, consensus, prior reaction)
  T-0       → 📊 morning-of summary (sized exposure, hedge prompt)
  T+1       → 📈 post-event reaction (PEAD signal if available)

═══════════════════════════════════════════════════════════════════════
"""
import json
import os
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta, date
from decimal import Decimal

import boto3

VERSION = "1.0.0"

S3_BUCKET = "justhodl-dashboard-live"
OUTPUT_KEY = "portfolio/catalysts.json"
CATALYST_CAL_KEY = "data/catalyst-calendar.json"
EARNINGS_TRACKER_KEY = "data/earnings-tracker.json"
EARNINGS_PEAD_KEY = "data/earnings-pead.json"
EARNINGS_WHISPER_KEY = "data/earnings-whisper.json"
SNAPSHOT_KEY = "portfolio/snapshot.json"
ALERT_HISTORY_KEY = "portfolio/catalyst-alert-history.json"

DDB_TABLE = "justhodl-portfolio"

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# Alert config
ALERT_TIERS = [7, 3, 1, 0]  # days_to thresholds at which we generate flags
DEDUPE_HOURS = 24

s3 = boto3.client("s3", region_name="us-east-1")
ddb = boto3.resource("dynamodb", region_name="us-east-1")
table = ddb.Table(DDB_TABLE)
ssm = boto3.client("ssm", region_name="us-east-1")


# ═══════════════════════════════════════════════════════════════════════
# LOADERS
# ═══════════════════════════════════════════════════════════════════════

def load_s3_json(key):
    try:
        body = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
        return json.loads(body)
    except Exception as e:
        print(f"  load {key} err: {str(e)[:120]}")
        return None


def scan_ddb_all():
    """Returns (positions_dict, watchlist_dict)."""
    positions, watchlist = {}, {}
    last = None
    while True:
        kwargs = {}
        if last: kwargs["ExclusiveStartKey"] = last
        resp = table.scan(**kwargs)
        for item in resp.get("Items") or []:
            sym = item.get("symbol")
            if not sym: continue
            pk = item.get("pk")
            cleaned = _decimal_to_float(item)
            if pk == "POSITION":
                positions[sym] = cleaned
            elif pk == "WATCHLIST":
                watchlist[sym] = cleaned
        last = resp.get("LastEvaluatedKey")
        if not last: break
    return positions, watchlist


def _decimal_to_float(obj):
    if isinstance(obj, Decimal): return float(obj)
    if isinstance(obj, dict): return {k: _decimal_to_float(v) for k, v in obj.items()}
    if isinstance(obj, list): return [_decimal_to_float(v) for v in obj]
    return obj


# ═══════════════════════════════════════════════════════════════════════
# DATE HELPERS
# ═══════════════════════════════════════════════════════════════════════

def parse_date(s):
    """Parse YYYY-MM-DD or ISO datetime to date object."""
    if not s: return None
    try:
        if "T" in str(s):
            return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except Exception: return None


def days_to(target_date):
    if not target_date: return None
    today = datetime.now(timezone.utc).date()
    return (target_date - today).days


def alert_tier_for_days(d):
    """Map days_to to highest alert tier triggered."""
    if d is None: return None
    if d == 0: return "T-0"
    if d == 1: return "T-1"
    if d <= 3 and d > 1: return "T-3"
    if d <= 7 and d > 3: return "T-7"
    return None  # too far or already past


# ═══════════════════════════════════════════════════════════════════════
# CATALYST PROCESSORS
# ═══════════════════════════════════════════════════════════════════════

def build_macro_catalysts(calendar):
    """Macro events affect the entire book — list separately."""
    if not calendar: return []
    out = []
    for ev in calendar.get("events") or []:
        d = parse_date(ev.get("date"))
        dt = days_to(d)
        if dt is None or dt < 0 or dt > 60: continue
        tier = alert_tier_for_days(dt)
        out.append({
            "date": ev.get("date"),
            "type": ev.get("type"),
            "title": ev.get("title"),
            "subtitle": ev.get("subtitle"),
            "impact": ev.get("impact"),
            "days_to": dt,
            "alert_tier": tier,
            "url": ev.get("url"),
        })
    out.sort(key=lambda e: e.get("days_to") or 999)
    return out


def build_position_catalysts(positions, watchlist, earnings_tracker, pead, whisper):
    """For each symbol in book/watch, gather all upcoming catalysts."""
    all_symbols = set(positions.keys()) | set(watchlist.keys())
    # Empty-position-and-watchlist guard: must still return a dict with
    # all expected bucket keys, else lambda_handler line 426 raises
    # KeyError 'T-0' on len(earnings_buckets["T-0"]).  (audit 2026-05-22)
    if not all_symbols:
        return [], {"T-0": [], "T-1": [], "T-3": [], "T-7": [], "later": []}

    # Index upcoming earnings by symbol
    upcoming_earnings_by_sym = {}
    for e in (earnings_tracker.get("upcoming_14d") or []) if earnings_tracker else []:
        sym = e.get("symbol") or e.get("ticker")
        if sym: upcoming_earnings_by_sym[sym] = e

    # Index PEAD signals
    pead_by_sym = {}
    if pead:
        for s in (pead.get("pead_signals") or pead.get("signals") or []):
            sym = s.get("symbol") or s.get("ticker")
            if sym: pead_by_sym[sym] = s

    # Index whisper numbers
    whisper_by_sym = {}
    if whisper:
        for w in (whisper.get("whispers") or whisper.get("signals") or []):
            sym = w.get("symbol") or w.get("ticker")
            if sym: whisper_by_sym[sym] = w

    catalysts_by_sym = []
    earnings_buckets = {"T-0": [], "T-1": [], "T-3": [], "T-7": [], "later": []}

    for sym in sorted(all_symbols):
        is_position = sym in positions
        upcoming = upcoming_earnings_by_sym.get(sym)
        pead_signal = pead_by_sym.get(sym)
        whisper_data = whisper_by_sym.get(sym)

        events = []
        # Upcoming earnings
        if upcoming:
            earnings_date = parse_date(upcoming.get("date") or upcoming.get("report_date") or upcoming.get("earnings_date"))
            dt = days_to(earnings_date)
            if dt is not None and dt >= 0:
                tier = alert_tier_for_days(dt)
                ev = {
                    "type": "EARNINGS",
                    "date": earnings_date.isoformat() if earnings_date else None,
                    "days_to": dt,
                    "time": upcoming.get("time") or upcoming.get("timing"),  # BMO/AMC
                    "eps_estimate": upcoming.get("eps_estimate") or upcoming.get("epsEstimate") or upcoming.get("estimate"),
                    "revenue_estimate": upcoming.get("revenue_estimate") or upcoming.get("revenueEstimate"),
                    "alert_tier": tier,
                }
                if whisper_data:
                    ev["whisper_eps"] = whisper_data.get("whisper_eps") or whisper_data.get("whisper")
                    ev["whisper_vs_consensus_pct"] = whisper_data.get("delta_pct")
                events.append(ev)
                # Bucket for top-level summary
                if tier == "T-0": earnings_buckets["T-0"].append(sym)
                elif tier == "T-1": earnings_buckets["T-1"].append(sym)
                elif tier == "T-3": earnings_buckets["T-3"].append(sym)
                elif tier == "T-7": earnings_buckets["T-7"].append(sym)
                else: earnings_buckets["later"].append(sym)

        # PEAD signal if recently reported
        if pead_signal:
            events.append({
                "type": "PEAD_DRIFT",
                "direction": pead_signal.get("direction") or pead_signal.get("signal"),
                "magnitude": pead_signal.get("magnitude") or pead_signal.get("z_score"),
                "reported_on": pead_signal.get("reported_on") or pead_signal.get("date"),
                "post_earnings_return": pead_signal.get("post_earnings_return")
                                          or pead_signal.get("return_5d"),
            })

        if not events: continue

        catalysts_by_sym.append({
            "symbol": sym,
            "is_position": is_position,
            "position_value": positions.get(sym, {}).get("cost_basis_total"),
            "events": events,
            "next_event_days": min((e.get("days_to") for e in events
                                      if e.get("days_to") is not None), default=999),
        })

    # Sort by next-event proximity
    catalysts_by_sym.sort(key=lambda c: c.get("next_event_days") or 999)
    return catalysts_by_sym, earnings_buckets


# ═══════════════════════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════════════════════

def get_chat_id():
    if TELEGRAM_CHAT_ID: return TELEGRAM_CHAT_ID
    try:
        return ssm.get_parameter(Name="/justhodl/telegram/chat_id",
                                  WithDecryption=True)["Parameter"]["Value"]
    except Exception: return None


def send_telegram(text, chat_id):
    if not TELEGRAM_TOKEN or not chat_id: return False
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    body = urllib.parse.urlencode({
        "chat_id": chat_id, "text": text[:4000],
        "parse_mode": "Markdown", "disable_web_page_preview": "true",
    }).encode("utf-8")
    try:
        req = urllib.request.Request(url, data=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"})
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read().decode("utf-8")).get("ok", False)
    except Exception as e:
        print(f"  telegram err: {str(e)[:200]}")
        return False


def load_alert_history():
    try: return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=ALERT_HISTORY_KEY)["Body"].read())
    except Exception: return {}


def save_alert_history(h):
    try:
        s3.put_object(Bucket=S3_BUCKET, Key=ALERT_HISTORY_KEY,
            Body=json.dumps(h, separators=(",", ":")).encode("utf-8"),
            ContentType="application/json")
    except Exception as e: print(f"  hist err: {e}")


def should_alert(history, key):
    last = history.get(key)
    if not last: return True
    try:
        last_dt = datetime.fromisoformat(last.replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - last_dt) >= timedelta(hours=DEDUPE_HOURS)
    except Exception: return True


def format_earnings_alert(item, event, tier):
    sym = item["symbol"]
    is_pos = item.get("is_position")
    pos_emoji = "💼" if is_pos else "👁"
    icon = {"T-3": "🗓", "T-1": "⏰", "T-0": "📊"}.get(tier, "🔔")
    timing = event.get("time", "")
    timing_str = f" ({timing})" if timing else ""
    eps_est = event.get("eps_estimate")
    whisper = event.get("whisper_eps")
    whisper_delta = event.get("whisper_vs_consensus_pct")

    lines = [f"{icon} *EARNINGS {tier} · {sym}* {pos_emoji}"]
    lines.append(f"Reports {event.get('date')}{timing_str}")
    if eps_est:
        eps_line = f"EPS consensus: {eps_est}"
        if whisper:
            eps_line += f" · whisper {whisper}"
            if whisper_delta is not None:
                eps_line += f" ({whisper_delta:+.1f}% vs consensus)"
        lines.append(eps_line)
    if event.get("revenue_estimate"):
        lines.append(f"Revenue est: {event.get('revenue_estimate')}")
    if is_pos and item.get("position_value"):
        lines.append(f"_Position value: ${item.get('position_value'):,.0f}_")
    lines.append(f"\n[Catalyst Calendar](https://justhodl.ai/catalyst/) · "
                  f"[{sym} Analysis](https://justhodl.ai/stock/?symbol={sym})")
    return "\n".join(lines)


def format_macro_alert(ev):
    icon = {"FOMC": "🏛", "AUCTION": "📜", "WITCHING": "🎲",
            "REBALANCE": "♻️", "BANK_EARNINGS": "🏦"}.get(ev.get("type"), "📅")
    return (f"{icon} *{ev.get('alert_tier')} · {ev.get('type')}*\n"
            f"*{ev.get('title','')}*\n"
            f"{ev.get('subtitle') or ''}\n"
            f"Date: {ev.get('date')} · impact: {ev.get('impact')}\n\n"
            f"[Catalyst Calendar](https://justhodl.ai/catalyst/)")


# ═══════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════

def lambda_handler(event, context):
    started = time.time()
    print(f"=== PORTFOLIO CATALYSTS v{VERSION} · {datetime.now(timezone.utc).isoformat()} ===")

    # Load all inputs in parallel
    catalyst_cal = load_s3_json(CATALYST_CAL_KEY)
    earnings_tracker = load_s3_json(EARNINGS_TRACKER_KEY)
    pead = load_s3_json(EARNINGS_PEAD_KEY)
    whisper = load_s3_json(EARNINGS_WHISPER_KEY)
    snapshot = load_s3_json(SNAPSHOT_KEY) or {}

    print(f"  cal events: {len(((catalyst_cal or {}).get('events') or []))}")
    print(f"  upcoming earnings: {len(((earnings_tracker or {}).get('upcoming_14d') or []))}")

    # Scan DDB
    positions, watchlist = scan_ddb_all()
    print(f"  DDB: {len(positions)} positions + {len(watchlist)} watchlist")

    # Build catalyst views
    macro_catalysts = build_macro_catalysts(catalyst_cal)
    position_catalysts, earnings_buckets = build_position_catalysts(
        positions, watchlist, earnings_tracker, pead, whisper)

    # Counts
    in_book_count = sum(1 for c in position_catalysts if c.get("is_position"))
    watch_only_count = sum(1 for c in position_catalysts if not c.get("is_position"))

    # ─── Fire Telegram alerts ───
    chat_id = get_chat_id()
    history = load_alert_history()
    now_iso = datetime.now(timezone.utc).isoformat()
    alerts_sent = 0
    alerts_skipped = 0

    if chat_id and TELEGRAM_TOKEN:
        # 1. Per-symbol earnings alerts (T-3, T-1, T-0 only)
        for item in position_catalysts:
            for ev in item["events"]:
                tier = ev.get("alert_tier")
                if tier not in ("T-3", "T-1", "T-0"): continue
                # Only alert for POSITIONS, not watchlist
                if not item.get("is_position"): continue
                key = f"cat:{item['symbol']}:{tier}:{ev.get('date')}"
                if not should_alert(history, key):
                    alerts_skipped += 1; continue
                if send_telegram(format_earnings_alert(item, ev, tier), chat_id):
                    history[key] = now_iso
                    alerts_sent += 1
                time.sleep(0.4)

        # 2. Macro catalysts (T-0 and T-1 only, high-impact)
        for ev in macro_catalysts:
            tier = ev.get("alert_tier")
            if tier not in ("T-0", "T-1"): continue
            if ev.get("impact") != "HIGH": continue
            key = f"cat:macro:{ev.get('type')}:{tier}:{ev.get('date')}"
            if not should_alert(history, key):
                alerts_skipped += 1; continue
            if send_telegram(format_macro_alert(ev), chat_id):
                history[key] = now_iso
                alerts_sent += 1
            time.sleep(0.4)

        save_alert_history(history)

    # ─── Build payload ───
    next_event_global = position_catalysts[0] if position_catalysts else None
    next_macro = macro_catalysts[0] if macro_catalysts else None

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "generated_at_unix": int(time.time()),
        "version": VERSION,
        "elapsed_seconds": round(time.time() - started, 2),

        "summary": {
            "n_positions_tracked": len(positions),
            "n_watchlist_tracked": len(watchlist),
            "n_position_catalysts": in_book_count,
            "n_watch_catalysts": watch_only_count,
            "n_macro_events": len(macro_catalysts),
            "earnings_this_week": len(earnings_buckets["T-0"]) +
                                    len(earnings_buckets["T-1"]) +
                                    len(earnings_buckets["T-3"]) +
                                    len(earnings_buckets["T-7"]),
            "tomorrow_reporters": earnings_buckets["T-1"],
            "today_reporters": earnings_buckets["T-0"],
            "this_week_reporters": earnings_buckets["T-3"] + earnings_buckets["T-7"],
            "next_position_event_days": (next_event_global.get("next_event_days")
                                            if next_event_global else None),
            "next_position_event_symbol": (next_event_global.get("symbol")
                                              if next_event_global else None),
            "next_macro_event_days": next_macro.get("days_to") if next_macro else None,
            "next_macro_event": next_macro.get("title") if next_macro else None,
        },

        "position_catalysts": position_catalysts,
        "macro_catalysts": macro_catalysts,

        "alerts_sent": alerts_sent,
        "alerts_skipped_dedupe": alerts_skipped,
    }

    try:
        s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY,
            Body=json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8"),
            ContentType="application/json",
            CacheControl="public, max-age=1800")
        print(f"  ✓ catalysts.json written · {len(position_catalysts)} symbols + {len(macro_catalysts)} macro events")
    except Exception as e:
        # audit P2.5: emit EMF metric for silent put_object failure
        print(__import__('json').dumps({"_aws":{"Timestamp":int(__import__('time').time()*1000),"CloudWatchMetrics":[{"Namespace":"JustHodl/Reliability","Dimensions":[["Lambda"]],"Metrics":[{"Name":"S3PutFailure","Unit":"Count"}]}]},"Lambda":__import__('os').environ.get("AWS_LAMBDA_FUNCTION_NAME","?"),"S3PutFailure":1,"error":str(e)[:200] if 'e' in dir() else "unknown"}))
        print(f"  put_object err: {e}")
        return {"statusCode": 500, "body": json.dumps({"err": str(e)})}

    return {"statusCode": 200, "body": json.dumps({
        "success": True, "version": VERSION,
        "n_position_catalysts": len(position_catalysts),
        "n_macro_catalysts": len(macro_catalysts),
        "alerts_sent": alerts_sent,
        "next_position_event_days": payload["summary"]["next_position_event_days"],
        "elapsed_seconds": round(time.time() - started, 2),
    })}
# audit-P0-redeploy: 2026-05-22T09:56:37Z — force redeploy to land already-committed fix
