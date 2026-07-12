"""ops 3141 — Telegram send-path forensics.

3140: token LIVE (@Justhodl_bot), env correct, lambda send still false.
This op captures the actual error verbatim, twice:

  1. Runner-side sendMessage replay — identical JSON payload the lambda
     builds — with HTTPError body capture (Telegram puts the reason in
     the 4xx body: 'chat not found', 'bot was blocked', …).
  2. test_telegram invoke, then the function's CloudWatch log tail —
     the lambda prints '[compass] telegram failed: <exc>'.

Read-only + one test message. The verbatim reasons decide the fix.
"""

import json
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import boto3

from ops_report import report

REGION = "us-east-1"
FN = "justhodl-alpha-compass"
HERE = Path(__file__).resolve().parent
AWS_DIR = HERE.parents[1]
TG_DONOR = AWS_DIR / "lambdas" / "justhodl-dollar-radar" / "config.json"

LAM = boto3.client("lambda", region_name=REGION)
LOGS = boto3.client("logs", region_name=REGION)

with report("3141_telegram_forensics") as rep:
    fails, warns = [], []
    rep.heading("ops 3141 — Telegram send-path forensics")

    donor = json.loads(TG_DONOR.read_text()).get("environment") or {}
    tok, chat = donor.get("TELEGRAM_TOKEN", ""), donor.get("TELEGRAM_CHAT_ID", "")

    rep.section("1. Runner-side replay (identical payload)")
    body = json.dumps({"chat_id": chat,
                       "text": "\u2705 <b>Alpha Compass</b> \u2014 "
                               "send-path replay (ops 3141)",
                       "parse_mode": "HTML",
                       "disable_web_page_preview": True}).encode()
    req = urllib.request.Request(
        f"https://api.telegram.org/bot{tok}/sendMessage",
        data=body, headers={"Content-Type": "application/json"})
    replay_ok = False
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            rep.ok(f"runner replay delivered (HTTP {r.status})")
            replay_ok = True
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", "replace")[:300]
        rep.log(f"runner replay HTTP {e.code}: {detail}")
    except Exception as e:
        rep.log(f"runner replay error: {str(e)[:200]}")

    rep.section("2. Lambda test invoke + CloudWatch tail")
    t0_ms = int(time.time() * 1000) - 5000
    try:
        resp = LAM.invoke(FunctionName=FN, InvocationType="RequestResponse",
                          Payload=json.dumps({"test_telegram": 1}).encode())
        payload = json.loads(resp["Payload"].read())
        rep.log(f"invoke payload: {json.dumps(payload)[:200]}")
    except Exception as e:
        fails.append(f"test invoke failed: {str(e)[:150]}")
    time.sleep(6)
    try:
        ev = LOGS.filter_log_events(
            logGroupName=f"/aws/lambda/{FN}", startTime=t0_ms,
            filterPattern="telegram", limit=20)
        lines = [e["message"].strip() for e in ev.get("events") or []]
        if lines:
            for ln in lines[-8:]:
                rep.log(f"CW: {ln[:220]}")
        else:
            rep.log("CW: no 'telegram' lines in window")
    except Exception as e:
        warns.append(f"CW read failed: {str(e)[:120]}")

    rep.section("3. Verdict")
    if replay_ok:
        rep.log("payload+token+chat are GOOD from the runner — fault is "
                "lambda-side; CW lines above name it")
    else:
        rep.log("payload itself rejected — CW irrelevant; fix per the "
                "HTTP body above")

    for w in warns:
        rep.warn(w)
    for f in fails:
        rep.fail(f)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        sys.exit(1)
    sys.exit(0)
