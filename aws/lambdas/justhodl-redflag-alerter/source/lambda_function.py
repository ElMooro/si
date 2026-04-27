"""
justhodl-redflag-alerter — Telegram alerter for serious 8-K events

Subscribes to data/8k-filings.json on a 30-min cadence (matches the
sec-8k Lambda schedule). For any filing in the last hour with a red-flag
Item code, fires a Telegram message.

Red-flag items (in priority order — most serious first):
  4.02  Non-Reliance on Previously Issued Financial Statements
        (= the company is admitting prior financials were wrong.
         Historically precedes 30%+ drawdowns in ~2/3 of cases.)
  1.03  Bankruptcy or Receivership
  3.01  Notice of Delisting / Failure to Satisfy Listing
  5.04  Temporary Suspension of Trading Under Employee Benefit Plans
  2.04  Triggering Events Accelerating a Direct Financial Obligation
  2.06  Material Impairments

Deduplication: keeps a 24h window of accession numbers in S3 so we don't
re-alert on the same filing every 30 min.

Output:
  - Telegram message to the configured chat
  - data/redflag-alerts.json — log of recent alerts (rolling 7 days)

Schedule: rate(30 minutes), aligned to sec-8k publishing cadence.
"""
from __future__ import annotations
import json
import os
import ssl
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta

import boto3

S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_INPUT_KEY = os.environ.get("S3_INPUT_KEY", "data/8k-filings.json")
S3_OUTPUT_KEY = os.environ.get("S3_OUTPUT_KEY", "data/redflag-alerts.json")
TG_TOKEN_PARAM = os.environ.get("TG_TOKEN_PARAM", "/justhodl/telegram/bot_token")
TG_CHAT_ID_PARAM = os.environ.get("TG_CHAT_ID_PARAM", "/justhodl/telegram/chat_id")
LOOKBACK_MINUTES = int(os.environ.get("LOOKBACK_MINUTES", "60"))   # alert if filed in last hour
DEDUPE_WINDOW_HOURS = int(os.environ.get("DEDUPE_WINDOW_HOURS", "24"))


# Item code → (severity rank, label, urgency_emoji)
RED_FLAGS = {
    "4.02": (1, "Non-Reliance on Prior Financials", "🚨"),
    "1.03": (2, "Bankruptcy / Receivership",        "🆘"),
    "3.01": (3, "Notice of Delisting",              "⚠️"),
    "5.04": (4, "Trading Suspension (Benefit Plan)", "⛔"),
    "2.04": (5, "Accelerated Debt Obligation",      "🔻"),
    "2.06": (6, "Material Impairment",              "📉"),
}

s3 = boto3.client("s3")
ssm = boto3.client("ssm")


def get_s3_json(key, default=None):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception:
        return default


def put_s3_json(key, body):
    s3.put_object(
        Bucket=S3_BUCKET, Key=key,
        Body=json.dumps(body).encode(),
        ContentType="application/json",
        CacheControl="no-cache",
    )


def _telegram_creds():
    try:
        token = ssm.get_parameter(Name=TG_TOKEN_PARAM, WithDecryption=True)["Parameter"]["Value"]
        chat_id = ssm.get_parameter(Name=TG_CHAT_ID_PARAM)["Parameter"]["Value"]
        return token, chat_id
    except Exception as e:
        print(f"telegram creds error: {e}")
        return None, None


def send_telegram(text: str) -> bool:
    token, chat_id = _telegram_creds()
    if not token or not chat_id:
        print("missing telegram creds; skipping send")
        return False
    try:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        data = urllib.parse.urlencode({
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "Markdown",
            "disable_web_page_preview": "true",
        }).encode("utf-8")
        req = urllib.request.Request(url, data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"})
        with urllib.request.urlopen(req, timeout=15, context=ctx) as r:
            return r.status == 200
    except Exception as e:
        print(f"telegram send error: {e}")
        return False


def _build_message(filing) -> str:
    items = filing.get("items", [])
    red_items = [i for i in items if i in RED_FLAGS]
    # Pick the most-severe (lowest severity rank)
    primary = min(red_items, key=lambda i: RED_FLAGS[i][0])
    severity, label, emoji = RED_FLAGS[primary]

    lines = [f"{emoji} *RED FLAG 8-K · Item {primary}*"]
    lines.append(f"_{label}_\n")
    lines.append(f"*Company:* {filing.get('company', '?')}")
    lines.append(f"*Filed:* {filing.get('filed_at', '?')[:16].replace('T', ' ')} UTC")
    if len(red_items) > 1:
        also = ", ".join(f"Item {i}" for i in red_items if i != primary)
        lines.append(f"*Also:* {also}")
    if filing.get("filing_url"):
        lines.append(f"\n[View filing]({filing['filing_url']})")
    if primary == "4.02":
        lines.append(f"\n_Item 4.02 historically precedes 30%+ drawdowns in ~2/3 of cases. Audit the position._")
    return "\n".join(lines)


def lambda_handler(event, context):
    started = time.time()
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(minutes=LOOKBACK_MINUTES)

    # 1. Load 8-K data
    sec8k = get_s3_json(S3_INPUT_KEY, {})
    filings = sec8k.get("filings", []) or []
    if not filings:
        print(f"no 8-K data yet at {S3_INPUT_KEY}")
        return {"statusCode": 200,
                "body": json.dumps({"ok": True, "reason": "no_8k_data"})}

    # 2. Filter to red-flag filings within lookback window
    red_recent = []
    for f in filings:
        items = f.get("items", [])
        if not any(i in RED_FLAGS for i in items):
            continue
        try:
            filed_dt = datetime.fromisoformat(f["filed_at"].replace("Z", "+00:00"))
        except Exception:
            continue
        if filed_dt < cutoff:
            continue
        red_recent.append(f)

    print(f"  {len(red_recent)} red-flag filings in last {LOOKBACK_MINUTES}min (out of {len(filings)} total in window)")

    # 3. Load dedupe state — accession numbers we've already alerted on
    dedupe_state = get_s3_json(S3_OUTPUT_KEY, {"alerted_accessions": [], "log": []})
    dedupe_cutoff = now - timedelta(hours=DEDUPE_WINDOW_HOURS)
    alerted = set(dedupe_state.get("alerted_accessions", []))

    # 4. Send alerts for new red flags
    alerts_sent = []
    alerts_skipped = []
    for f in red_recent:
        acc = f.get("accession", "")
        if acc in alerted:
            alerts_skipped.append({"accession": acc, "reason": "already_alerted"})
            continue
        msg = _build_message(f)
        sent = send_telegram(msg)
        if sent:
            alerts_sent.append({
                "accession": acc,
                "company": f.get("company", "?"),
                "items": [i for i in f.get("items", []) if i in RED_FLAGS],
                "filed_at": f.get("filed_at"),
                "alerted_at": now.isoformat(timespec="seconds"),
            })
            alerted.add(acc)
        else:
            alerts_skipped.append({"accession": acc, "reason": "send_failed"})

    # 5. Persist updated dedupe state (drop entries older than DEDUPE_WINDOW_HOURS)
    log = dedupe_state.get("log", []) + alerts_sent
    log = [
        e for e in log
        if datetime.fromisoformat(e["alerted_at"]) >= dedupe_cutoff
    ] if log else []
    log = log[-500:]  # cap log size

    new_state = {
        "generated_at": now.isoformat(timespec="seconds"),
        "alerted_accessions": [e["accession"] for e in log],
        "log": log,
        "stats": {
            "alerts_sent_this_run": len(alerts_sent),
            "alerts_skipped_this_run": len(alerts_skipped),
            "total_in_log": len(log),
            "lookback_minutes": LOOKBACK_MINUTES,
            "dedupe_window_hours": DEDUPE_WINDOW_HOURS,
            "fetch_duration_s": round(time.time() - started, 1),
        },
    }
    put_s3_json(S3_OUTPUT_KEY, new_state)

    print(f"  Alerts sent: {len(alerts_sent)} | skipped: {len(alerts_skipped)} | log size: {len(log)}")

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({
            "ok": True,
            "alerts_sent": len(alerts_sent),
            "alerts_skipped": len(alerts_skipped),
        }),
    }
