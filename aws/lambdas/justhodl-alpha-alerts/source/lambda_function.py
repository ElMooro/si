"""
justhodl-alpha-alerts — Roadmap #4 — Telegram Alpha Feed

═══════════════════════════════════════════════════════════════════════
THE ONLY ALERTS WORTH SENDING
─────────────────────────────
Reads signals/confluence.json after each refresh. Fires Telegram alerts ONLY
when something meaningful happens:

  ⭐ NEW TIER S CONFLUENCE  — stock just entered TIER S (6+ factors firing)
                              extremely rare, "drop everything" priority
  ⭐ NEW TIER S ALPHA        — stock hit alpha ≥90 for first time
  📈 UPGRADE TO TIER A      — stock moved up to TIER A (80+)
  📉 DOWNGRADE FROM A/S     — stock fell out of A/S tier
  🌀 REGIME CHANGE          — macro regime flipped

Dedupes via S3 alert-history.json — won't re-alert the same event within
24h unless the stock crosses thresholds AGAIN.

═══════════════════════════════════════════════════════════════════════
TELEGRAM DELIVERY
─────────────────
Uses TELEGRAM_TOKEN env var (matches existing screener-alerts pattern)
and CHAT_ID from SSM /justhodl/telegram/chat_id (per memory).

Markdown-formatted messages with emoji, signal bullets, and direct
links to the relevant stock detail page.
"""
import json
import os
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta
import boto3

S3_BUCKET = "justhodl-dashboard-live"
CONFLUENCE_KEY = "signals/confluence.json"
REGIME_KEY = "signals/regime-picks.json"
ALERT_HISTORY_KEY = "signals/alert-history.json"

DEDUPE_WINDOW_HOURS = 24

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")  # fallback if SSM fails

s3 = boto3.client("s3", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")


def get_chat_id():
    if TELEGRAM_CHAT_ID: return TELEGRAM_CHAT_ID
    try:
        return ssm.get_parameter(Name="/justhodl/telegram/chat_id",
                                  WithDecryption=True)["Parameter"]["Value"]
    except Exception as e:
        print(f"  SSM chat_id fetch failed: {e}")
        return None


def send_telegram(text, chat_id):
    """Send markdown-formatted message. Returns True on success."""
    if not TELEGRAM_TOKEN or not chat_id:
        print(f"  no token/chat_id — skipping send. token_len={len(TELEGRAM_TOKEN)} chat_id={chat_id}")
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    body = urllib.parse.urlencode({
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": "true",
    }).encode("utf-8")
    try:
        req = urllib.request.Request(url, data=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"})
        with urllib.request.urlopen(req, timeout=10) as r:
            resp = json.loads(r.read().decode("utf-8"))
        if resp.get("ok"): return True
        print(f"  telegram error: {resp}")
    except Exception as e:
        print(f"  telegram send failed: {str(e)[:200]}")
    return False


def load_alert_history():
    """Returns dict {alert_key: last_sent_iso}."""
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=ALERT_HISTORY_KEY)
        return json.loads(obj["Body"].read())
    except Exception:
        return {}


def save_alert_history(history):
    try:
        s3.put_object(
            Bucket=S3_BUCKET, Key=ALERT_HISTORY_KEY,
            Body=json.dumps(history, separators=(",", ":")).encode("utf-8"),
            ContentType="application/json")
    except Exception as e:
        # audit P2.5: emit EMF metric for silent put_object failure
        print(__import__('json').dumps({"_aws":{"Timestamp":int(__import__('time').time()*1000),"CloudWatchMetrics":[{"Namespace":"JustHodl/Reliability","Dimensions":[["Lambda"]],"Metrics":[{"Name":"S3PutFailure","Unit":"Count"}]}]},"Lambda":__import__('os').environ.get("AWS_LAMBDA_FUNCTION_NAME","?"),"S3PutFailure":1,"error":str(e)[:200] if 'e' in dir() else "unknown"}))
        print(f"  history save failed: {e}")


def should_alert(history, alert_key):
    """Check if we should fire this alert (not within dedupe window)."""
    last = history.get(alert_key)
    if not last: return True
    try:
        last_dt = datetime.fromisoformat(last.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        return (now - last_dt) >= timedelta(hours=DEDUPE_WINDOW_HOURS)
    except Exception:
        return True


def escape_md(s):
    """Escape Markdown special characters."""
    if not s: return ""
    return str(s).replace("_", "\\_").replace("*", "\\*").replace("[", "\\[").replace("`", "\\`")


def format_tier_s_alert(stock):
    sym = stock["symbol"]
    alpha = stock.get("alpha_score") or 0
    name = escape_md((stock.get("name") or sym)[:32])
    components = stock.get("components") or {}
    firing = stock.get("components_firing") or []
    n_firing = stock.get("confluence_count") or 0
    sector = stock.get("sector") or ""

    factor_labels = {"quality":"Q","growth":"G","momentum":"M",
                     "smart_money":"SM","sentiment":"News","analysts":"An","insiders":"Ins"}
    firing_str = ", ".join(f"{factor_labels.get(f['factor'], f['factor'])}={f['score']}" for f in firing[:6])

    signals_text = ""
    for sig in (stock.get("top_signals") or [])[:3]:
        signals_text += f"\n• {escape_md(sig)}"

    risk_text = ""
    for rf in (stock.get("risk_flags") or [])[:2]:
        risk_text += f"\n{escape_md(rf)}"

    return (
        f"⭐ *TIER S CONFLUENCE — {sym}*\n"
        f"{name} · {sector}\n"
        f"`Alpha {alpha} · {n_firing}/7 factors firing: {firing_str}`\n"
        f"{signals_text}"
        f"{risk_text}\n\n"
        f"[Detail](https://justhodl.ai/stock/?symbol={sym}) · "
        f"[Alpha view](https://justhodl.ai/alpha/) · "
        f"[Screener](https://justhodl.ai/screener/)"
    )


def format_new_tier_a(stock):
    sym = stock["symbol"]
    alpha = stock.get("alpha_score") or 0
    name = escape_md((stock.get("name") or sym)[:32])
    sector = stock.get("sector") or ""
    n_firing = stock.get("confluence_count") or 0
    return (
        f"📈 *NEW TIER A — {sym}*\n"
        f"{name} · {sector}\n"
        f"`Alpha {alpha} · {n_firing}/7 firing`\n"
        f"[Detail](https://justhodl.ai/stock/?symbol={sym})"
    )


def format_regime_change(new_regime, prev_regime, confidence):
    return (
        f"🌀 *REGIME CHANGE: {prev_regime} → {new_regime}*\n"
        f"`Confidence: {confidence:.0%}`\n\n"
        f"Sector preferences flipped. Re-rank watchlist now.\n\n"
        f"[Regime view](https://justhodl.ai/alpha/) · "
        f"[Macro](https://justhodl.ai/risk/)"
    )


def lambda_handler(event, context):
    started = time.time()
    print(f"=== ALPHA ALERTS · {datetime.now(timezone.utc).isoformat()} ===")

    chat_id = get_chat_id()
    if not chat_id:
        return {"statusCode": 500, "body": json.dumps({"err": "no chat_id"})}

    # Load confluence
    try:
        conf = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=CONFLUENCE_KEY)["Body"].read())
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"err": f"confluence read: {e}"})}

    # Load alert history (dedupe)
    history = load_alert_history()
    now_iso = datetime.now(timezone.utc).isoformat()

    # Track stats
    sent = 0
    skipped_dedupe = 0
    actions = []

    # 1. TIER S CONFLUENCE — top priority
    for s in (conf.get("tier_s_confluence") or [])[:5]:
        sym = s["symbol"]
        key = f"tier_s:{sym}"
        if not should_alert(history, key):
            skipped_dedupe += 1
            continue
        msg = format_tier_s_alert(s)
        if send_telegram(msg, chat_id):
            history[key] = now_iso
            sent += 1
            actions.append({"type": "tier_s", "symbol": sym})
        time.sleep(0.4)  # rate-limit politeness

    # 2. NEW TIER A+ from diffs
    diffs = conf.get("diffs") or {}
    for d in (diffs.get("new_tier_a_plus") or [])[:5]:
        sym = d["symbol"]
        key = f"tier_a_upgrade:{sym}"
        if not should_alert(history, key):
            skipped_dedupe += 1
            continue
        # Find full stock data
        full = next((s for s in conf.get("tier_a_confluence", [])
                     if s["symbol"] == sym), None) or next(
            (s for s in conf.get("tier_s_confluence", [])
                     if s["symbol"] == sym), None)
        if not full: continue
        msg = format_new_tier_a(full)
        if send_telegram(msg, chat_id):
            history[key] = now_iso
            sent += 1
            actions.append({"type": "tier_a_upgrade", "symbol": sym})
        time.sleep(0.4)

    # 3. REGIME CHANGE
    current_regime = conf.get("regime")
    prev_regime = history.get("__last_regime")
    if prev_regime and current_regime and current_regime != prev_regime:
        msg = format_regime_change(current_regime, prev_regime,
                                     conf.get("regime_confidence", 0.7))
        if send_telegram(msg, chat_id):
            history["__last_regime"] = current_regime
            sent += 1
            actions.append({"type": "regime_change", "from": prev_regime, "to": current_regime})
    elif current_regime and not prev_regime:
        history["__last_regime"] = current_regime  # initial save, no alert

    # Save updated history
    save_alert_history(history)

    elapsed = time.time() - started
    print(f"  sent {sent} alerts · skipped {skipped_dedupe} dedupes · {elapsed:.2f}s")

    return {"statusCode": 200, "body": json.dumps({
        "success": True,
        "sent": sent,
        "skipped_dedupe": skipped_dedupe,
        "actions": actions,
        "elapsed_seconds": round(elapsed, 2),
    })}
