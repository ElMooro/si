"""
justhodl-kill-switch v1.0 — blast-radius governor (promised 2026-06-05, finally real).

One SSM flag halts the entire EventBridge fleet; flipping it back restores
exactly what was running before. For runaway-cost or bad-deploy emergencies.

Modes (event or flag-driven, checked every 10 min):
  {"action":"engage"}   → save all ENABLED rule names to S3, disable everything
                          except this checker, set flag ON, Telegram.
  {"action":"restore"}  → re-enable the saved set, clear state, set flag OFF.
  {} (scheduled check)  → flag ON + no state file → engage;
                          flag OFF + state file present → restore.

SSM:   /justhodl/kill-switch  (ON | OFF)
State: data/kill-switch-state.json
"""
import json
import os
import urllib.request
from datetime import datetime, timezone

import boto3

S3 = boto3.client("s3")
EV = boto3.client("events")
SSM = boto3.client("ssm")
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
STATE_KEY = "data/kill-switch-state.json"
FLAG = "/justhodl/kill-switch"
KEEP = {"justhodl-kill-switch-check"}   # never self-disable

TG_TOKEN = os.environ.get("TG_TOKEN", "8679881066:AAHTE6TAhDqs0FuUelTL6Ppt1x8ihis1aGs")
TG_CHAT = os.environ.get("TG_CHAT", "8678089260")


def _tg(msg):
    try:
        data = json.dumps({"chat_id": TG_CHAT, "text": msg, "parse_mode": "HTML"}).encode()
        urllib.request.urlopen(urllib.request.Request(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage", data=data,
            headers={"Content-Type": "application/json"}), timeout=10)
    except Exception:
        pass


def _flag():
    try:
        return SSM.get_parameter(Name=FLAG)["Parameter"]["Value"].upper()
    except Exception:
        return "OFF"


def _set_flag(v):
    SSM.put_parameter(Name=FLAG, Value=v, Type="String", Overwrite=True)


def _state():
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=STATE_KEY)["Body"].read())
    except Exception:
        return None


def _all_rules():
    rules, tok = [], None
    while True:
        kw = {"Limit": 100}
        if tok:
            kw["NextToken"] = tok
        r = EV.list_rules(**kw)
        rules += r.get("Rules", [])
        tok = r.get("NextToken")
        if not tok:
            break
    return rules


def engage(now):
    enabled = [r["Name"] for r in _all_rules() if r["State"] == "ENABLED" and r["Name"] not in KEEP]
    errs = []
    for n in enabled:
        try:
            EV.disable_rule(Name=n)
        except Exception as e:
            errs.append(f"{n}: {str(e)[:50]}")
    S3.put_object(Bucket=BUCKET, Key=STATE_KEY, Body=json.dumps({
        "engaged_at": now.isoformat(), "disabled_rules": enabled, "errors": errs}).encode(),
        ContentType="application/json")
    _set_flag("ON")
    _tg(f"🛑 <b>KILL-SWITCH ENGAGED</b>\n{len(enabled)} rules halted, {len(errs)} errors.\n"
        f"Restore: set SSM {FLAG}=OFF or invoke with action=restore.")
    return {"engaged": len(enabled), "errors": errs[:5]}


def restore(now, st):
    names = (st or {}).get("disabled_rules", [])
    errs = []
    for n in names:
        try:
            EV.enable_rule(Name=n)
        except Exception as e:
            errs.append(f"{n}: {str(e)[:50]}")
    try:
        S3.delete_object(Bucket=BUCKET, Key=STATE_KEY)
    except Exception:
        pass
    _set_flag("OFF")
    _tg(f"✅ <b>KILL-SWITCH RESTORED</b>\n{len(names)} rules re-enabled, {len(errs)} errors.")
    return {"restored": len(names), "errors": errs[:5]}


def lambda_handler(event, context):
    now = datetime.now(timezone.utc)
    action = (event or {}).get("action", "").lower()
    flag = _flag()
    st = _state()
    if action == "engage" or (not action and flag == "ON" and st is None):
        res = engage(now)
        return {"statusCode": 200, "body": json.dumps({"mode": "ENGAGED", **res})}
    if action == "restore" or (not action and flag == "OFF" and st is not None):
        res = restore(now, st)
        return {"statusCode": 200, "body": json.dumps({"mode": "RESTORED", **res})}
    return {"statusCode": 200, "body": json.dumps({"mode": "ARMED" if flag == "OFF" else "ENGAGED",
                                                   "flag": flag, "state_present": st is not None})}
