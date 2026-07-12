"""ops 3140 — Telegram tripwire: find a live token and wire it.

3139's test_telegram returned false. Two candidate tokens exist:
  a) donor config (dollar-radar env) — possibly the stale pre-rotation one
  b) the runner secret TELEGRAM_BOT_TOKEN — possibly current

This op getMe-probes both from the runner, picks the first live one,
MERGES it into the function env (never clobbering FMP), and re-runs the
armed test. If neither is live, the tripwire stays dark and that is
reported plainly (token rotation is PENDING-KHALID).
"""

import json
import os
import sys
import urllib.request
from pathlib import Path

import boto3

from ops_report import report
from _lambda_deploy_helpers import _retry_on_conflict

REGION = "us-east-1"
FN = "justhodl-alpha-compass"
HERE = Path(__file__).resolve().parent
AWS_DIR = HERE.parents[1]
TG_DONOR = AWS_DIR / "lambdas" / "justhodl-dollar-radar" / "config.json"

LAM = boto3.client("lambda", region_name=REGION)


def get_me(token):
    if not token:
        return False, "empty"
    try:
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{token}/getMe")
        with urllib.request.urlopen(req, timeout=10) as r:
            d = json.loads(r.read().decode())
        return bool(d.get("ok")), (d.get("result") or {}).get("username", "?")
    except Exception as e:
        return False, str(e)[:80]


with report("3140_telegram_wire") as rep:
    fails, warns = [], []
    rep.heading("ops 3140 — Telegram token probe + wire")

    rep.section("1. Probe candidates (runner-side getMe)")
    donor = (json.loads(TG_DONOR.read_text()).get("environment") or {})
    cands = [("donor-config", donor.get("TELEGRAM_TOKEN", "")),
             ("runner-secret", os.environ.get("TELEGRAM_BOT_TOKEN", ""))]
    live_token = None
    for name, tok in cands:
        ok, info = get_me(tok)
        rep.log(f"{name}: {'LIVE @' + info if ok else 'dead (' + info + ')'}")
        if ok and not live_token:
            live_token = tok
            live_name = name
    chat = donor.get("TELEGRAM_CHAT_ID", "")

    if not live_token:
        warns.append("no live Telegram token available — tripwire stays "
                     "dark until rotation (PENDING-KHALID)")
        for w in warns:
            rep.warn(w)
        rep.kv(n_fails=0, n_warns=len(warns), verdict="PASS")
        sys.exit(0)

    rep.section("2. Merge live token into function env")
    cfg = LAM.get_function_configuration(FunctionName=FN)
    env = (cfg.get("Environment") or {}).get("Variables") or {}
    env["TELEGRAM_TOKEN"] = live_token
    if chat:
        env["TELEGRAM_CHAT_ID"] = chat
    _retry_on_conflict(LAM.update_function_configuration,
                       FunctionName=FN,
                       Environment={"Variables": env})
    LAM.get_waiter("function_updated").wait(
        FunctionName=FN, WaiterConfig={"Delay": 3, "MaxAttempts": 40})
    rep.ok(f"env updated with {live_name} token "
           f"(keys: {sorted(env)})")

    rep.section("3. Armed test via lambda")
    resp = LAM.invoke(FunctionName=FN, InvocationType="RequestResponse",
                      Payload=json.dumps({"test_telegram": 1}).encode())
    body = json.loads(json.loads(resp["Payload"].read())["body"])
    if body.get("telegram"):
        rep.ok("tripwire armed — message delivered")
    else:
        fails.append("lambda send still failing with a proven-live token — "
                     "inspect send path next op")

    for w in warns:
        rep.warn(w)
    for f in fails:
        rep.fail(f)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        sys.exit(1)
    sys.exit(0)
