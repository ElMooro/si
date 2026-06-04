"""justhodl-setups-push — daily morning brief of the top conviction setups

Flips the platform from reactive (user must come look) to proactive. Each
morning, pushes the highest-conviction setups from the unified conviction
engine to Telegram, plus evaluates per-user custom alert rules and notifies.

Reads:
  data/best-setups.json              — the ranked conviction board
  data/user-alert-rules.json         — aggregated custom alert rules (per-user)
    (written by the chart-pro alert builder via the worker /userdata KV; a
     companion sync writes a flat index here for the Lambda to evaluate)

SCHEDULE: daily 8:45 ET (after best-setups + before market open).
"""
import json
import os
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone

import boto3

S3_BUCKET = "justhodl-dashboard-live"
TG_BOT_TOKEN = "8679881066:AAHTE6TAhDqs0FuUelTL6Ppt1x8ihis1aGs"
TG_CHAT_ID = "8678089260"
s3 = boto3.client("s3", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")


def _read_json(key, default=None):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception:
        return default


def _tg_config():
    try:
        token = ssm.get_parameter(Name="/justhodl/telegram/bot-token", WithDecryption=True)["Parameter"]["Value"]
        chat = ssm.get_parameter(Name="/justhodl/telegram/chat_id")["Parameter"]["Value"]
        return token, chat
    except Exception:
        return TG_BOT_TOKEN, TG_CHAT_ID


def send_telegram(text, chat_id=None):
    token, default_chat = _tg_config()
    chat = chat_id or default_chat
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = urllib.parse.urlencode({"chat_id": chat, "text": text[:4000],
                                    "parse_mode": "HTML", "disable_web_page_preview": "true"}).encode()
    try:
        with urllib.request.urlopen(urllib.request.Request(url, data=data, method="POST"), timeout=12) as r:
            return r.status
    except Exception as e:
        return f"err:{str(e)[:80]}"


def fmt_setup(s):
    sigs = ", ".join(k.replace("_", " ") for k in (s.get("signal_keys") or [])[:4])
    line = f"<b>{s['ticker']}</b> · {s['verdict']} · conviction {round(s.get('conviction') or 0)}"
    sub = f"  {s.get('n_signals')} signals: {sigs}"
    levels = ""
    if s.get("entry"):
        levels = f"\n  Entry ${s['entry']:.2f} · Stop ${(s.get('stop') or 0):.2f} · TP3 ${(s.get('tp3') or 0):.2f} ({round(s.get('rr') or 0,1)}R, {s.get('horizon_days','?')}d)"
    thesis = ""
    if s.get("thesis"):
        thesis = f"\n  <i>{s['thesis'][:200]}</i>"
    return line + "\n" + sub + levels + thesis


def lambda_handler(event, context):
    t0 = time.time()
    board = _read_json("data/best-setups.json") or {}
    setups = board.get("top_setups") or []
    # ── NEW: Triple-Threat alert — the rarest, highest-conviction setups ──
    quads = board.get("quad_threats") or [s for s in setups if s.get("quad_threat")]
    triples = (board.get("triple_threats") or [s for s in setups if s.get("triple_threat")])
    alerts = quads + [t for t in triples if t not in quads]
    if alerts:
        triples = alerts
        tlines = ["<b>🎯 TRIPLE THREAT ALERT</b>",
                  "<i>Cheap (dislocation) + durable grower (compounder) + a market/flow signal — all three lenses agree</i>", ""]
        for s in triples[:5]:
            vl = ", ".join((s.get("value_lenses") or []) + (s.get("flow_lenses") or [])[:2])
            tlines.append(f"<b>{s['ticker']}</b> · conviction {round(s.get('conviction') or 0)}\n  {vl}")
            if s.get("thesis"):
                tlines.append(f"  <i>{s['thesis'][:160]}</i>")
            tlines.append("")
        tlines.append("<i>justhodl.ai/chart-pro</i>")
        send_telegram("\n".join(tlines))
        print(f"[setups-push] triple-threat alert: {len(triples)}")

    strong = [s for s in setups if s.get("verdict") == "STRONG BUY"]
    buys = [s for s in setups if s.get("verdict") == "BUY"]
    headline = (strong + buys)[:6]
    if not headline:
        # show top watch names if nothing graded BUY+ yet
        headline = setups[:5]

    date = datetime.now(timezone.utc).strftime("%a %b %d")
    lines = [f"<b>⚡ JUSTHODL — TODAY'S TOP SETUPS</b>",
             f"<i>{date} · {board.get('stats',{}).get('strong_buy',0)} strong · {board.get('stats',{}).get('buy',0)} buy</i>",
             ""]
    for s in headline:
        lines.append(fmt_setup(s))
        lines.append("")
    lines.append("<i>Conviction = confluence of independent signals, weighted by learned hit-rate. Full board + charts on justhodl.ai/chart-pro</i>")
    msg = "\n".join(lines)
    status = send_telegram(msg)
    print(f"[setups-push] sent {len(headline)} setups, status={status}")

    # ── Per-user custom alert evaluation ──
    # The chart-pro alert builder stores rules in the per-user KV; a flat index
    # is mirrored to data/user-alert-rules.json for server-side evaluation.
    rules_doc = _read_json("data/user-alert-rules.json") or {}
    alerts_fired = 0
    price_alerts_fired = 0
    by_ticker = {s["ticker"]: s for s in setups}
    quotes = _read_json("data/quote-snapshot.json") or {}

    for uid, rules in (rules_doc.get("by_user") or {}).items():
        chat = (rules_doc.get("user_chat") or {}).get(uid)
        # Conviction-rule alerts
        for rule in (rules or []):
            matches = []
            for s in setups:
                if (s.get("conviction") or 0) < (rule.get("min_conviction") or 0):
                    continue
                req = set(rule.get("require_signals") or [])
                if req and not req.issubset(set(s.get("signal_keys") or [])):
                    continue
                if rule.get("require_committee") and "POLITICIAN_COMMITTEE" not in (s.get("signal_keys") or []):
                    continue
                matches.append(s)
            if matches and chat:
                m = [f"<b>🔔 Custom alert: {rule.get('name','rule')}</b>"]
                for s in matches[:5]:
                    m.append(fmt_setup(s)); m.append("")
                send_telegram("\n".join(m), chat_id=chat)
                alerts_fired += 1
        # Price-level alerts (cross detection vs latest quote)
        price_alerts = (rules_doc.get("price_alerts") or {}).get(uid) or {}
        for tk, levels in price_alerts.items():
            px = (quotes.get(tk) or {}).get("price")
            if px is None:
                continue
            for lvl in (levels or []):
                p = lvl.get("price")
                d = lvl.get("dir")
                hit = (d == "above" and px >= p) or (d == "below" and px <= p)
                if hit and chat:
                    send_telegram(f"<b>🔔 Price alert</b>\n{tk} crossed {d} ${p:.2f} (now ${px:.2f}) {lvl.get('note','')}", chat_id=chat)
                    price_alerts_fired += 1

    return {"statusCode": 200, "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"ok": True, "setups_pushed": len(headline),
                                 "telegram_status": status, "custom_alerts_fired": alerts_fired,
                                 "price_alerts_fired": price_alerts_fired,
                                 "elapsed_s": round(time.time() - t0, 1)})}
