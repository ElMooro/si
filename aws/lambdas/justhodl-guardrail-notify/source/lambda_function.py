"""
justhodl-guardrail-notify  v1.0.0  (ops 5250)

The delivery leg of the fleet cost/runaway guardrails.

    CloudWatch alarm  ->  SNS justhodl-fleet-alerts  ->  THIS  ->  Telegram
                                                              ->  S3 ledger

Why it exists: on 2026-08-01 and 2026-09-07 AWS Health mailed Khalid about a
Lambda recursion break, and in August a rewrite loop billed ~$309 before a
Cost Anomaly mail arrived days later. Both were visible in CloudWatch within
minutes. Alarms (ops 5250) now watch RecursiveInvocationsDropped, hourly
invocations, concurrency, hourly errors and S3 bucket growth. This function
turns those alarm transitions into a Telegram message and an S3 record so
they are seen immediately and audited afterwards.

Contract
  - SNS event: each record's Message is the CloudWatch alarm JSON.
  - Direct invoke {"test": true}: sends a test message, proves the path.
  - Never raises on delivery failure: a notifier that fails loudly would
    just create more alarm noise. Failures are logged and ledgered.

Telegram credentials: SSM /justhodl/telegram/bot_token (SecureString) and
/justhodl/telegram/chat_id — the same parameters justhodl-alert-router
reads. TELEGRAM_CHAT_ID env is the fallback chat.
"""
import json
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import boto3

VERSION = "1.0.0"
REGION = os.environ.get("AWS_REGION", "us-east-1")
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
LEDGER = os.environ.get("LEDGER_PREFIX", "data/ops/guardrails/")
ENV_CHAT = os.environ.get("TELEGRAM_CHAT_ID", "")

_ssm = None
_s3 = None
_cache = {}


def _clients():
    global _ssm, _s3
    if _ssm is None:
        _ssm = boto3.client("ssm", region_name=REGION)
    if _s3 is None:
        _s3 = boto3.client("s3", region_name=REGION)
    return _ssm, _s3


def _param(name, decrypt=False):
    if name in _cache:
        return _cache[name]
    ssm, _ = _clients()
    try:
        v = ssm.get_parameter(Name=name, WithDecryption=decrypt)["Parameter"]["Value"]
    except Exception as e:  # noqa: BLE001
        print("[ssm] %s: %s" % (name, str(e)[:120]))
        v = ""
    _cache[name] = v
    return v


def _telegram(text):
    token = _param("/justhodl/telegram/bot_token", True)
    chat = _param("/justhodl/telegram/chat_id") or ENV_CHAT
    if not token or not chat:
        return False, "no_token_or_chat"
    body = urllib.parse.urlencode({"chat_id": chat, "text": text[:3900],
                                   "disable_web_page_preview": "true"}).encode()
    req = urllib.request.Request(
        "https://api.telegram.org/bot%s/sendMessage" % token, data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded",
                 "User-Agent": "justhodl-guardrail-notify/%s" % VERSION})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            ok = r.status == 200
            return ok, "http_%s" % r.status
    except Exception as e:  # noqa: BLE001
        return False, str(e)[:160]


def _fmt_alarm(a):
    """CloudWatch alarm JSON -> one readable Telegram message."""
    name = a.get("AlarmName", "?")
    state = a.get("NewStateValue", "?")
    old = a.get("OldStateValue", "?")
    reason = a.get("NewStateReason", "")
    trig = a.get("Trigger") or {}
    metric = trig.get("MetricName") or (trig.get("Metrics") and "metric-math") or "?"
    icon = {"ALARM": "\U0001F6A8", "OK": "\u2705", "INSUFFICIENT_DATA": "\u26A0\ufe0f"}.get(state, "\u2022")
    lines = ["%s JustHodl guardrail: %s" % (icon, name),
             "state: %s -> %s" % (old, state),
             "metric: %s" % metric,
             "at: %s" % a.get("StateChangeTime", ""),
             "why: %s" % reason[:600]]
    if state == "ALARM":
        lines.append("action: see aws/ops/reports/latest/5250_* runbook; "
                     "kill switch = reserved concurrency 0 on the named function")
    return "\n".join(lines)


def _ledger(record, ok, info):
    _, s3 = _clients()
    now = datetime.now(timezone.utc)
    key = "%sevents/%s/%s-%s.json" % (
        LEDGER, now.strftime("%Y/%m/%d"), now.strftime("%H%M%S"),
        (record.get("AlarmName") or "event").replace("/", "_")[:60])
    doc = {"version": VERSION, "at": now.isoformat(), "telegram_ok": ok,
           "telegram_info": info, "alarm": record}
    try:
        s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(doc, default=str).encode(),
                      ContentType="application/json")
        s3.put_object(Bucket=BUCKET, Key=LEDGER + "latest.json",
                      Body=json.dumps(doc, default=str).encode(),
                      ContentType="application/json", CacheControl="no-cache")
    except Exception as e:  # noqa: BLE001
        print("[ledger] %s" % str(e)[:160])


def _records(event):
    """Yield alarm dicts from an SNS event; tolerate direct/test invokes."""
    for rec in (event or {}).get("Records") or []:
        msg = (rec.get("Sns") or {}).get("Message")
        if not msg:
            continue
        try:
            yield json.loads(msg)
        except Exception:  # noqa: BLE001
            yield {"AlarmName": (rec.get("Sns") or {}).get("Subject") or "sns",
                   "NewStateValue": "INFO", "NewStateReason": str(msg)[:800]}


def lambda_handler(event, context=None):
    event = event or {}
    out = {"version": VERSION, "sent": 0, "failed": 0}
    if event.get("test"):
        ok, info = _telegram("\u2705 JustHodl guardrails armed (ops 5250) - "
                             "this is the delivery test at %s" %
                             datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"))
        _ledger({"AlarmName": "guardrail-test", "NewStateValue": "TEST"}, ok, info)
        out["telegram_ok"], out["telegram_info"] = ok, info
        return out
    for a in _records(event):
        ok, info = _telegram(_fmt_alarm(a))
        _ledger(a, ok, info)
        out["sent" if ok else "failed"] += 1
        time.sleep(0.2)
    return out
