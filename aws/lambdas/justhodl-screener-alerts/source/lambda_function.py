"""
justhodl-screener-alerts — Telegram Alerts Lambda

Watches screener/just-crossed.json on S3 and sends Telegram notifications
to @Justhodl_bot for high-significance events that haven't been alerted yet.

Triggered every 30 min by EventBridge rule. Stateful via S3 sidecar file
screener/alert-state.json which holds the set of already-sent event keys
(symbol|type|date hash) so we never double-alert.

Event filtering rules:
  significance >= 100  → always alert (ENTERED STEAL, POLITICAL_CLUSTER_BUYING)
  significance >= 90   → alert if type in HIGH_PRIORITY_TYPES
  Below 90             → skip

Alert message format (markdown):
  🔥 *<TYPE>* — *<SYMBOL>* (<sector>)
  <Company name>
  <event-specific detail>
  Score: <stealScore>
  📊 https://justhodl.ai/screener/?s=<encoded-state>
"""
import os
import json
import time
import hashlib
import urllib.request
import urllib.parse
from datetime import datetime, timezone

import boto3

# ───────────── CONFIG ─────────────
S3_BUCKET = "justhodl-dashboard-live"
JUST_CROSSED_KEY = "screener/just-crossed.json"
ALERT_STATE_KEY = "screener/alert-state.json"

# Telegram credentials (from userMemories: token + chat_id)
# Stored in Lambda env vars; fall back to known values
TELEGRAM_TOKEN = os.environ.get(
    "TELEGRAM_TOKEN",
    "8679881066:AAHTE6TAhDqs0FuUelTL6Ppt1x8ihis1aGs"
)
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "8678089260")
TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"

# Maximum messages to send per invocation (cap to avoid notification spam)
MAX_ALERTS_PER_RUN = 12
# Maximum age of events to consider (avoid alerting on stale data after maint)
MAX_EVENT_AGE_HOURS = 36
# Cap stored event keys to recent N (prevent state file unbounded growth)
MAX_STATE_KEYS = 2000

# Which event types are HIGH PRIORITY (alert even at sig 90-99)
HIGH_PRIORITY_TYPES = {
    "ENTERED_TIER",          # ENTERED STEAL is sig 110, PREMIUM 100, QUALITY 90
    "POLITICAL_CLUSTER_BUYING",
    "POLITICIANS_TURNED_BUYING",
    "INSIDER_TURNED_BUYING",
    "GOLDEN_CROSS",
    "ANALYST_UPGRADE_SURGE",
    "HEDGE_FUND_ACCUMULATING",
    "DCF_UPSIDE_100",
    "DCF_UPSIDE_50",
    "FORWARD_GROWTH_50",
    "CHEAP_FORWARD_PE",
    "BEAT_STREAK_7",
    "BEAT_STREAK_10",
}

# Pretty-name table for event types
TYPE_PRETTY = {
    "ENTERED_TIER":             "🏆 ENTERED TIER",
    "EXITED_TIER":              "📉 EXITED TIER",
    "SCORE_JUMP":               "📈 SCORE JUMP",
    "SCORE_DROP":               "📉 SCORE DROP",
    "INSIDER_TURNED_BUYING":    "🎯 INSIDER BUYING",
    "INSIDER_TURNED_SELLING":   "🎯 INSIDER SELLING",
    "BEAT_STREAK_3":            "🔥 3-STREAK BEATS",
    "BEAT_STREAK_5":            "🔥🔥 5-STREAK BEATS",
    "BEAT_STREAK_7":            "🔥🔥🔥 7-STREAK BEATS",
    "BEAT_STREAK_10":           "🚀 10-STREAK BEATS",
    "GOLDEN_CROSS":             "▲ GOLDEN CROSS",
    "DEATH_CROSS":              "▼ DEATH CROSS",
    "BECAME_SUSTAINABLE_QUALITY":"✓ SUSTAINABLE QUALITY",
    "FCF_YIELD_5":              "💵 FCF Yield > 5%",
    "FCF_YIELD_10":             "💵 FCF Yield > 10%",
    "REV_GROWTH_15":            "🚀 Rev Growth > 15%",
    "REV_GROWTH_25":            "🚀 Rev Growth > 25%",
    "BUYBACK_2":                "🪙 Buyback > 2%",
    "BUYBACK_5":                "🪙 Buyback > 5%",
    "POLITICIANS_TURNED_BUYING":"🏛 POLITICIANS BUYING",
    "POLITICIANS_TURNED_SELLING":"🏛 POLITICIANS SELLING",
    "POLITICAL_CLUSTER_BUYING": "🏛🏛🏛 POLITICAL CLUSTER",
    "TARGET_UPSIDE_15":         "🎯 Target +15%",
    "TARGET_UPSIDE_25":         "🎯 Target +25%",
    "TARGET_UPSIDE_50":         "🎯 Target +50%",
    "ANALYST_UPGRADE_SURGE":    "⬆ UPGRADE SURGE",
    "GRADES_IMPROVED":          "⬆ Grades Improved",
    "DCF_UPSIDE_25":            "💰 DCF +25%",
    "DCF_UPSIDE_50":            "💰 DCF +50%",
    "DCF_UPSIDE_100":           "💰💰 DCF +100%",
    "ESG_RATING_IMPROVED":      "🌱 ESG Improved",
    "HEDGE_FUND_ACCUMULATING":  "🐋 HEDGE FUNDS BUYING",
    "HEDGE_FUND_EXITING":       "🐋 HEDGE FUNDS EXITING",
    "INST_HOLDERS_SURGE":       "🏦 INSTITUTIONAL SURGE",
    "FORWARD_GROWTH_15":        "🚀 Forward Growth 15%",
    "FORWARD_GROWTH_25":        "🚀 Forward Growth 25%",
    "FORWARD_GROWTH_50":        "🚀🚀 Forward Growth 50%",
    "CHEAP_FORWARD_PE":         "⚡ CHEAP FWD P/E",
}

s3 = boto3.client("s3", region_name="us-east-1")


def make_event_key(event):
    """Stable hash for de-duplication: symbol + type + comparison_date.
    Includes the comparison date so an event fired today vs yesterday
    is distinct from the same logical change tomorrow."""
    sym = event.get("symbol", "")
    typ = event.get("type", "")
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return hashlib.sha1(f"{sym}|{typ}|{today}".encode()).hexdigest()[:16]


def load_state():
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=ALERT_STATE_KEY)
        state = json.loads(obj["Body"].read())
        return set(state.get("sent_keys") or [])
    except s3.exceptions.NoSuchKey:
        return set()
    except Exception as e:
        print(f"[alert] load_state err (resetting): {e}")
        return set()


def save_state(keys):
    keys_list = list(keys)[-MAX_STATE_KEYS:]
    body = json.dumps({
        "sent_keys": keys_list,
        "last_run": datetime.now(timezone.utc).isoformat(),
    }, separators=(",", ":"))
    s3.put_object(
        Bucket=S3_BUCKET, Key=ALERT_STATE_KEY, Body=body,
        ContentType="application/json")


def format_event(event):
    """Build a Telegram markdown message for one event."""
    sym = event.get("symbol", "?")
    typ = event.get("type", "?")
    name = event.get("name", "")
    sector = event.get("sector", "")
    score = event.get("stealScore")
    bucket = event.get("stealBucket")

    label = TYPE_PRETTY.get(typ, typ.replace("_", " "))

    # Build event-specific detail line
    detail = ""
    if typ == "ENTERED_TIER":
        from_t = event.get("from") or "—"
        to_t = event.get("to") or "—"
        fs = event.get("from_score")
        ts = event.get("to_score")
        score_str = f" ({fs:.0f} → {ts:.0f})" if fs and ts else ""
        detail = f"Tier: *{from_t} → {to_t}*{score_str}"
    elif typ in ("SCORE_JUMP", "SCORE_DROP"):
        detail = f"Score: {event.get('from')} → *{event.get('to')}*  ({event.get('delta'):+})"
    elif typ.startswith("INSIDER_"):
        ins = event.get("insider_net")
        ins_str = ""
        if isinstance(ins, (int, float)) and abs(ins) > 0:
            ab = abs(ins)
            if ab >= 1e9: ins_str = f" · ${ins/1e9:+.1f}B"
            elif ab >= 1e6: ins_str = f" · ${ins/1e6:+.1f}M"
            else: ins_str = f" · ${ins/1e3:+.0f}K"
        detail = f"Insider signal: *{event.get('to')}*{ins_str}"
    elif typ in ("POLITICIANS_TURNED_BUYING", "POLITICIANS_TURNED_SELLING"):
        n = event.get("buyers")
        net = event.get("net_usd")
        net_str = ""
        if isinstance(net, (int, float)) and abs(net) > 0:
            ab = abs(net)
            net_str = f" · net ${net/1e3:+.0f}K" if ab < 1e6 else f" · net ${net/1e6:+.1f}M"
        detail = f"*{n}* politicians buying{net_str}"
    elif typ == "POLITICAL_CLUSTER_BUYING":
        detail = f"*{event.get('to')}* distinct politicians buying"
    elif typ.startswith("BEAT_STREAK_"):
        detail = f"Streak: {event.get('from')} → *{event.get('to')}* quarters"
    elif typ == "GOLDEN_CROSS":
        detail = "SMA50 crossed above SMA200 — *bullish technical signal*"
    elif typ == "DEATH_CROSS":
        detail = "SMA50 crossed below SMA200 — *bearish technical signal*"
    elif typ.startswith("TARGET_UPSIDE_"):
        detail = f"Analyst target: *{event.get('to')}%* upside"
    elif typ == "ANALYST_UPGRADE_SURGE":
        detail = f"Net upgrades 30d: *{event.get('to')}*"
    elif typ.startswith("DCF_UPSIDE_"):
        detail = f"DCF upside: *{event.get('to')}%*"
    elif typ == "HEDGE_FUND_ACCUMULATING":
        detail = f"13F shares Δ: *{event.get('to')}%* QoQ"
    elif typ == "HEDGE_FUND_EXITING":
        detail = f"13F shares Δ: *{event.get('to')}%* QoQ"
    elif typ == "INST_HOLDERS_SURGE":
        detail = f"Institutional holders +{event.get('to')}% ({event.get('holders')} total)"
    elif typ.startswith("FORWARD_GROWTH_"):
        detail = f"Forward revenue growth: *{event.get('to')}%*"
    elif typ == "CHEAP_FORWARD_PE":
        detail = f"Forward P/E: *{event.get('to'):.1f}*"
    elif typ.startswith("FCF_YIELD_"):
        detail = f"FCF Yield: {event.get('from')}% → *{event.get('to')}%*"
    elif typ.startswith("REV_GROWTH_"):
        detail = f"Revenue growth: {event.get('from')}% → *{event.get('to')}%*"
    elif typ.startswith("BUYBACK_"):
        detail = f"Buyback yield: {event.get('from')}% → *{event.get('to')}%*"
    elif typ == "BECAME_SUSTAINABLE_QUALITY":
        detail = "3y profit + ROE>15% + NetM>10%"
    else:
        detail = f"{event.get('from','—')} → {event.get('to','—')}"

    # Compose
    bucket_str = f" — {bucket}" if bucket else ""
    score_line = f"Score: *{score:.1f}*{bucket_str}" if score is not None else ""
    sector_chip = f"_{sector}_" if sector else ""

    msg = (f"{label} — *{sym}* {sector_chip}\n"
            f"{name}\n"
            f"{detail}\n"
            f"{score_line}\n"
            f"📊 https://justhodl.ai/stock/?symbol={sym}")
    return msg


def send_telegram(text):
    """Send a single message. Returns True on success."""
    try:
        data = urllib.parse.urlencode({
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "Markdown",
            "disable_web_page_preview": "true",
        }).encode()
        url = f"{TELEGRAM_API}/sendMessage"
        with urllib.request.urlopen(url, data=data, timeout=10) as r:
            r.read()
            return True
    except Exception as e:
        print(f"[alert] telegram send err: {e}")
        return False


def lambda_handler(event, context):
    """Main entry."""
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=JUST_CROSSED_KEY)
        jc = json.loads(obj["Body"].read())
    except Exception as e:
        print(f"[alert] cannot read just-crossed.json: {e}")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}

    # Check freshness — skip if generated too long ago (e.g. weekend without runs)
    gen = jc.get("generated_at", "")
    if gen:
        try:
            gen_dt = datetime.fromisoformat(gen.replace("Z", "+00:00"))
            age_h = (datetime.now(timezone.utc) - gen_dt).total_seconds() / 3600
            if age_h > MAX_EVENT_AGE_HOURS:
                print(f"[alert] just-crossed too old ({age_h:.1f}h) — skipping")
                return {"statusCode": 200, "body": json.dumps({
                    "skipped": "stale", "age_hours": round(age_h, 1)})}
        except Exception:
            pass

    state = load_state()
    events = jc.get("events") or []

    # Filter to candidates
    candidates = []
    for e in events:
        sig = e.get("significance", 0)
        typ = e.get("type", "")
        if sig >= 100:
            candidates.append(e)
        elif sig >= 90 and typ in HIGH_PRIORITY_TYPES:
            candidates.append(e)

    # De-dupe
    new_events = []
    for e in candidates:
        key = make_event_key(e)
        if key in state:
            continue
        e["_key"] = key
        new_events.append(e)

    new_events.sort(key=lambda e: -e.get("significance", 0))
    new_events = new_events[:MAX_ALERTS_PER_RUN]

    sent_count = 0
    for e in new_events:
        text = format_event(e)
        if send_telegram(text):
            state.add(e["_key"])
            sent_count += 1
            time.sleep(0.3)  # avoid Telegram rate limits

    # Header summary if we sent ≥3 alerts in this batch
    if sent_count >= 3:
        summary = (f"📊 *JUSTHODL SCREENER*\n"
                    f"{sent_count} new high-conviction events posted above.\n"
                    f"Comparison: {(jc.get('comparison') or {}).get('today')} vs "
                    f"{(jc.get('comparison') or {}).get('previous')}\n"
                    f"📊 https://justhodl.ai/screener/")
        send_telegram(summary)

    save_state(state)

    return {"statusCode": 200, "body": json.dumps({
        "candidates": len(candidates),
        "new_events": len(new_events),
        "sent": sent_count,
        "state_size": len(state),
    })}
