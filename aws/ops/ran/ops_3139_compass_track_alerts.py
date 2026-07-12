"""ops 3139 — Compass v2.1: instant track record + Telegram tripwires.

The 3138 close left the track-record strip empty for ~2 weeks (first
grades at entry+H). But data/conviction/snapshots/*.json holds months of
dated top-3 calls. v2.1 lambda (committed with this op):

  • BACKFILL: one-time idempotent ingest of snapshot top-3 calls
    (≤120d lookback, dedup on date+subject, primary vehicle by direction)
  • FIXED-HORIZON GRADING: every call graded close-to-close at entry+14d
    from FMP EOD (/stable/historical-price-eod/light) — comparable across
    time, replaces since-call drift
  • TELEGRAM TRIPWIRE: top-call flips, entries/drops, |Δconv| ≥ 15
    (+ test_telegram event hook, house pattern)
  • degenerate-stop guard: p25 ≥ 0 is not a stop (3138 beta card)

Env for the function comes from existing repo configs only (FMP donor:
buyback-engine; Telegram donor: dollar-radar) — zero new key copies.

Gates: trail_30d.n ≥ 8 and trail_90d.n ≥ 20 expected from ~120d of daily
snapshots (warn below, fail at 0) · horizon==14 · hit/avg non-null ·
no magdist card with stop_pct ≥ 0 · telegram armed test returns ok.
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
HIST_KEY = "data/alpha-compass-history.json"

HERE = Path(__file__).resolve().parent
AWS_DIR = HERE.parents[1]
SRC = AWS_DIR / "lambdas" / FN / "source"
CFG = json.loads((AWS_DIR / "lambdas" / FN / "config.json").read_text())
FMP_DONOR = AWS_DIR / "lambdas" / "justhodl-buyback-engine" / "config.json"
TG_DONOR = AWS_DIR / "lambdas" / "justhodl-dollar-radar" / "config.json"

S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)


def s3_json(key):
    o = S3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(o["Body"].read().decode("utf-8"))


with report("3139_compass_track_alerts") as rep:
    fails, warns = [], []
    t0 = datetime.now(timezone.utc)
    rep.heading("ops 3139 — instant track record + Telegram tripwires")

    rep.section("1. Deploy (env from donor configs)")
    fmp = (json.loads(FMP_DONOR.read_text()).get("environment") or {}) \
        .get("FMP_API_KEY", "")
    tg_env = json.loads(TG_DONOR.read_text()).get("environment") or {}
    env_vars = {"FMP_API_KEY": fmp,
                "TELEGRAM_TOKEN": tg_env.get("TELEGRAM_TOKEN", ""),
                "TELEGRAM_CHAT_ID": tg_env.get("TELEGRAM_CHAT_ID", "")}
    env_vars = {k: v for k, v in env_vars.items() if v}
    rep.log(f"env keys wired: {sorted(env_vars)}")
    if "TELEGRAM_TOKEN" not in env_vars:
        warns.append("telegram token missing from donor — tripwire dark")
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

    rep.section("2. Fresh output (backfill runs inside this invoke)")
    doc = None
    deadline = time.time() + 240
    while time.time() < deadline:
        try:
            d = s3_json(OUT_KEY)
            if datetime.fromisoformat(d["generated_at"]) >= t0 \
                    and d.get("schema_version") == "2.0":
                doc = d
                break
        except Exception:
            pass
        time.sleep(8)
    if doc is None:
        rep.fail("v2 output never freshened")
        rep.kv(n_fails=1, verdict="FAIL")
        sys.exit(1)

    rep.section("3. Track-record gates")
    tr = doc.get("track_record") or {}
    t30, t90 = tr.get("trail_30d") or {}, tr.get("trail_90d") or {}
    rep.kv(horizon=tr.get("horizon_days"),
           backfilled=tr.get("backfilled_this_run"),
           graded=tr.get("graded_this_run"),
           open_calls=tr.get("open_calls"),
           t30=json.dumps(t30), t90=json.dumps(t90))
    if tr.get("horizon_days") != 14:
        fails.append(f"horizon={tr.get('horizon_days')} != 14")
    if not tr.get("quotes_available"):
        fails.append("EOD series unavailable — grading dark")
    if (t90.get("n") or 0) == 0:
        fails.append("trail_90d.n == 0 — backfill produced nothing")
    else:
        if (t30.get("n") or 0) < 8:
            warns.append(f"trail_30d.n={t30.get('n')} (<8 expected)")
        if (t90.get("n") or 0) < 20:
            warns.append(f"trail_90d.n={t90.get('n')} (<20 expected)")
        if t90.get("hit_rate") is None or t90.get("avg_ret") is None:
            fails.append("trail_90d metrics null despite n>0")
        rep.ok(f"track live: 30d {t30.get('hit_rate')} hit / "
               f"{t30.get('avg_ret')}% avg (n={t30.get('n')}) · "
               f"90d {t90.get('hit_rate')} / {t90.get('avg_ret')}% "
               f"(n={t90.get('n')})")
    for e in (tr.get("recent") or [])[:5]:
        rep.log(f"  · {e.get('d')} {e.get('subject')} {e.get('tk')} "
                f"→ {e.get('ret')}%")
    try:
        h = s3_json(HIST_KEY)
        rep.kv(history_entries=len(h.get("entries") or []),
               backfill_done=h.get("backfill_done"))
    except Exception as e:
        fails.append(f"history unreadable: {e}")

    rep.section("4. Degenerate-stop gate")
    bad = [c.get("subject") for c in
           (doc.get("top_calls") or []) + (doc.get("watchlist") or [])
           if (c.get("stats") or {}).get("source") == "magdist"
           and c.get("stop_pct") is not None and c["stop_pct"] >= 0]
    if bad:
        fails.append(f"degenerate stop (≥0) still on: {bad}")
    else:
        rep.ok("no non-negative stops on realised-dist cards")

    rep.section("5. Telegram armed test")
    try:
        resp = LAM.invoke(FunctionName=FN, InvocationType="RequestResponse",
                          Payload=json.dumps({"test_telegram": 1}).encode())
        body = json.loads(json.loads(resp["Payload"].read())["body"])
        if body.get("telegram"):
            rep.ok("tripwire armed message delivered to Telegram")
        else:
            warns.append("test_telegram returned false — token/chat check")
    except Exception as e:
        warns.append(f"test_telegram invoke failed: {str(e)[:100]}")

    for w in warns:
        rep.warn(w)
    for f in fails:
        rep.fail(f)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        sys.exit(1)
    sys.exit(0)
