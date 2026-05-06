#!/usr/bin/env python3
"""Step 273 — One-shot Telegram delivery of macro nowcast.

Reads data/macro-nowcast.json and pushes a clean institutional-format
message to @Justhodl_bot. Validates the end-to-end pipeline works
before promoting to a scheduled Lambda.

If Khalid likes the format, this becomes a daily/weekly Lambda (Mon 8AM)
on EventBridge.
"""
import json
import os
import urllib.request
from datetime import datetime, timezone

import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
KEY = "data/macro-nowcast.json"
REPORT_PATH = "aws/ops/reports/273_nowcast_telegram.json"

# Per memory:
TELEGRAM_TOKEN = "8679881066:AAHTE6TAhDqs0FuUelTL6Ppt1x8ihis1aGs"

s3 = boto3.client("s3", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)


def get_chat_id():
    try:
        return ssm.get_parameter(Name="/justhodl/telegram/chat_id")["Parameter"]["Value"]
    except Exception as e:
        return None


def send_telegram(token, chat_id, text):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text[:4096],
        "parse_mode": "Markdown",
        "disable_web_page_preview": True,
    }
    body = json.dumps(payload).encode()
    req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return True, r.read().decode()[:300]
    except Exception as e:
        return False, str(e)[:300]


def format_message(d):
    regime = d.get("regime", "—")
    score = d.get("normalized_score", 0)
    color = d.get("regime_color", "")
    icon = {"green": "🟢", "yellow": "🟡", "amber": "🟠", "red": "🔴"}.get(color, "⚪")

    coverage = d.get("coverage_pct", 0)
    n_used = d.get("n_components_used", 0)
    n_failed = d.get("n_components_failed", 0)
    components = d.get("components") or []

    lines = [
        f"{icon} *MACRO NOWCAST · {regime}*",
        f"`Composite z-score: {score:+.3f}`",
        f"`Coverage: {coverage:.0f}% ({n_used}/{n_used+n_failed} components)`",
        "",
        "*Top contributors* (sorted by |contribution|):",
    ]

    for c in components[:5]:
        z = c.get("z")
        contrib = c.get("contribution")
        raw = c.get("raw_value")
        if z is None or contrib is None:
            continue
        sign = "🟢" if contrib > 0 else "🔴"
        if c.get("transform") == "yoy_pct":
            raw_str = f"{raw:+.2f}% YoY"
        else:
            raw_str = f"{raw:.2f}"
        lines.append(
            f"{sign} `{c['fred_id']:<8} z={z:+.2f}  contrib={contrib:+.3f}  {raw_str}`"
        )

    lines.append("")
    lines.append(f"_Generated: {d.get('generated_at')}_")
    lines.append("[Open dashboard](https://justhodl.ai/macro-data.html)")
    return "\n".join(lines)


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        body = json.loads(s3.get_object(Bucket=BUCKET, Key=KEY)["Body"].read())
        out["nowcast_summary"] = {
            "regime": body.get("regime"),
            "score": body.get("normalized_score"),
            "coverage_pct": body.get("coverage_pct"),
            "n_used": body.get("n_components_used"),
            "generated_at": body.get("generated_at"),
        }

        chat_id = get_chat_id()
        if not chat_id:
            out["error"] = "no chat_id in SSM /justhodl/telegram/chat_id"
        else:
            out["chat_id"] = chat_id
            msg = format_message(body)
            out["message_preview"] = msg
            ok, info = send_telegram(TELEGRAM_TOKEN, chat_id, msg)
            out["telegram_ok"] = ok
            out["telegram_info"] = info[:300]
    except Exception as e:
        import traceback
        out["fatal_error"] = str(e)
        out["traceback"] = traceback.format_exc()

    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    with open(REPORT_PATH, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str)[:3000])
    return 0 if not out.get("fatal_error") else 1


if __name__ == "__main__":
    raise SystemExit(main())
