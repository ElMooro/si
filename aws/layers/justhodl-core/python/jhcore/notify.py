"""jhcore.notify — Telegram + (minimal) SES helpers for JustHodl Lambdas."""
import json
import os
import urllib.parse
import urllib.request

DEFAULT_BOT_TOKEN = "8679881066:AAHTE6TAhDqs0FuUelTL6Ppt1x8ihis1aGs"
DEFAULT_CHAT_ID = "8678089260"


def telegram(message, chat_id=None, bot_token=None, disable_web_page_preview=True, parse_mode="HTML"):
    """Send a Telegram message. Returns True on success."""
    token = bot_token or os.environ.get("TELEGRAM_BOT_TOKEN", DEFAULT_BOT_TOKEN)
    chat = chat_id or os.environ.get("TELEGRAM_CHAT_ID", DEFAULT_CHAT_ID)
    if not token or not chat:
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat,
        "text": message[:4090],
        "disable_web_page_preview": disable_web_page_preview,
    }
    if parse_mode:
        payload["parse_mode"] = parse_mode
    data = urllib.parse.urlencode(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/x-www-form-urlencoded"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status == 200
    except Exception as e:
        print(f"[jhcore.notify] telegram err: {e}")
        return False


def alert(severity, title, body=None, chat_id=None):
    """Helper: send a formatted alert (severity ∈ INFO/WARN/CRIT)."""
    emoji = {"INFO": "ℹ️", "WARN": "⚠️", "CRIT": "🚨"}.get(severity.upper(), "•")
    msg = f"{emoji} <b>{severity.upper()}</b>: {title}"
    if body:
        msg += f"\n\n{body}"
    return telegram(msg, chat_id=chat_id)


def ses_email(to_addr, subject, body_text, from_addr=None):
    """Minimal SES sendEmail. Returns True/False."""
    import boto3
    ses = boto3.client("ses", region_name="us-east-1")
    src = from_addr or os.environ.get("SES_FROM", "noreply@justhodl.ai")
    try:
        ses.send_email(
            Source=src,
            Destination={"ToAddresses": [to_addr]},
            Message={"Subject": {"Data": subject},
                     "Body": {"Text": {"Data": body_text}}}
        )
        return True
    except Exception as e:
        print(f"[jhcore.notify] ses err: {e}")
        return False
