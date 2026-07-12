"""ops 3142 — deploy self-healing Telegram send path.

3141 verbatim: HTTP 403 'bot can't initiate conversation with a user' —
the chat id is a user who never pressed /start on @Justhodl_bot. No code
can bypass that Telegram rule; ONE tap by Khalid can (t.me/Justhodl_bot →
Start). v2.1.1 makes that tap the ONLY step ever needed:

  send → env chat → (S3-discovered chat) → on failure getUpdates
  discovery → retry → persist data/_telegram-chat.json for all future
  runs (and, later, the rest of the fleet).

Gates here: deploy clean · main output still fresh/PASS-shaped ·
test_telegram exercises the heal path (expected false until /start —
that is a WARN with the exact one-tap instruction, not a fail) · the
detailed error now lands in CloudWatch.
"""

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3

from ops_report import report
from _lambda_deploy_helpers import deploy_lambda

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-alpha-compass"
OUT_KEY = "data/alpha-compass.json"

HERE = Path(__file__).resolve().parent
AWS_DIR = HERE.parents[1]
SRC = AWS_DIR / "lambdas" / FN / "source"
CFG = json.loads((AWS_DIR / "lambdas" / FN / "config.json").read_text())
FMP_DONOR = AWS_DIR / "lambdas" / "justhodl-buyback-engine" / "config.json"
TG_DONOR = AWS_DIR / "lambdas" / "justhodl-dollar-radar" / "config.json"

S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
LOGS = boto3.client("logs", region_name=REGION)


def s3_json(key):
    o = S3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(o["Body"].read().decode("utf-8"))


with report("3142_telegram_selfheal") as rep:
    fails, warns = [], []
    t0 = datetime.now(timezone.utc)
    rep.heading("ops 3142 — self-healing Telegram send path")

    rep.section("1. Deploy v2.1.1")
    fmp = (json.loads(FMP_DONOR.read_text()).get("environment") or {}) \
        .get("FMP_API_KEY", "")
    tg = json.loads(TG_DONOR.read_text()).get("environment") or {}
    env_vars = {k: v for k, v in {
        "FMP_API_KEY": fmp,
        "TELEGRAM_TOKEN": tg.get("TELEGRAM_TOKEN", ""),
        "TELEGRAM_CHAT_ID": tg.get("TELEGRAM_CHAT_ID", ""),
    }.items() if v}
    sched = CFG.get("schedule") or {}
    try:
        deploy_lambda(
            report=rep, function_name=FN, source_dir=SRC,
            env_vars=env_vars,
            eb_rule_name=sched.get("rule_name"),
            eb_schedule=sched.get("cron"),
            timeout=CFG.get("timeout", 240), memory=CFG.get("memory", 512),
            description=CFG.get("description", ""),
        )
    except Exception as e:
        rep.fail(f"deploy failed: {str(e)[:200]}")
        rep.kv(n_fails=1, verdict="FAIL")
        sys.exit(1)

    rep.section("2. Output still healthy")
    doc = None
    deadline = time.time() + 180
    while time.time() < deadline:
        try:
            d = s3_json(OUT_KEY)
            if datetime.fromisoformat(d["generated_at"]) >= t0:
                doc = d
                break
        except Exception:
            pass
        time.sleep(8)
    if doc is None:
        rep.fail("output never freshened after deploy")
        rep.kv(n_fails=1, verdict="FAIL")
        sys.exit(1)
    tr = doc.get("track_record") or {}
    rep.ok(f"fresh · 30d n={((tr.get('trail_30d') or {}).get('n'))} · "
           f"90d n={((tr.get('trail_90d') or {}).get('n'))} · "
           f"regime={(doc.get('regime') or {}).get('label')}")

    rep.section("3. Heal-path exercise + CW detail")
    t_ms = int(time.time() * 1000) - 2000
    resp = LAM.invoke(FunctionName=FN, InvocationType="RequestResponse",
                      Payload=json.dumps({"test_telegram": 1}).encode())
    body = json.loads(json.loads(resp["Payload"].read())["body"])
    delivered = bool(body.get("telegram"))
    time.sleep(8)
    try:
        ev = LOGS.filter_log_events(
            logGroupName=f"/aws/lambda/{FN}", startTime=t_ms,
            filterPattern='"telegram"', limit=10)
        for e in (ev.get("events") or [])[-5:]:
            rep.log(f"CW: {e['message'].strip()[:200]}")
    except Exception as e:
        warns.append(f"CW read failed: {str(e)[:100]}")
    try:
        saved = s3_json("data/_telegram-chat.json")
        rep.ok(f"self-heal PERSISTED chat {saved.get('chat_id')} — "
               "tripwires fully armed")
    except Exception:
        if delivered:
            rep.ok("delivered via existing chat (no heal needed)")
        else:
            warns.append(
                "AWAITING ONE TAP: open t.me/Justhodl_bot and press Start — "
                "the next scheduled run (every 3h at :50) self-arms and "
                "persists the chat for the whole fleet to reuse")

    for w in warns:
        rep.warn(w)
    for f in fails:
        rep.fail(f)
    rep.kv(delivered=delivered, n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        sys.exit(1)
    sys.exit(0)
