"""ops 3138 — Compass evidence upgrade: subset-coverage matcher + RORO chip.

From 3137 forensics:
  • magdist stacks are ~1-signal each → Jaccard's union-penalty buried
    every true match (best raw 0.167, killed by horizon multiplier).
    Fix: a stack matches when ALL its signals are in the setup's candidate
    vocabulary; rank by (horizon==30, n). Expected: 4-5 of 7 cards jump
    from scorecard/prior to REALISED DIST — housing & scarcity honestly
    stay prior (no graded stacks exist for them).
  • RORO chip: real fields are risk_regime ('MILD_RISK_ON') +
    risk_regime_score (24.9) → regime strip back to 5 sources.
  • decisive_call 'UNKNOWN' suppressed from playbook.
  • Per-dir _sentry_lite.py DELETED — this deploy also e2e-proves the
    patched build_zip's aws/shared injection (no manual copy here).

Gates: label clean · sources ≥5 (warn 4) · magdist tier ≥1 (fail 0) ·
per-card stop/target populated at magdist tier · quotes still live.
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
DONOR = AWS_DIR / "lambdas" / "justhodl-buyback-engine" / "config.json"

S3 = boto3.client("s3", region_name=REGION)


def s3_json(key):
    o = S3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(o["Body"].read().decode("utf-8"))


with report("3138_compass_evidence") as rep:
    fails, warns = [], []
    t0 = datetime.now(timezone.utc)
    rep.heading("ops 3138 — subset-coverage evidence matcher + RORO chip")

    rep.section("1. Deploy (helpers inject aws/shared — no manual shim)")
    if (SRC / "_sentry_lite.py").exists():
        fails.append("per-dir shim still present — this op must prove "
                     "helper injection")
    fmp = (json.loads(DONOR.read_text()).get("environment") or {}) \
        .get("FMP_API_KEY", "")
    sched = CFG.get("schedule") or {}
    try:
        deploy_lambda(
            report=rep, function_name=FN, source_dir=SRC,
            env_vars={"FMP_API_KEY": fmp} if fmp else {},
            eb_rule_name=sched.get("rule_name"),
            eb_schedule=sched.get("cron"),
            timeout=CFG.get("timeout", 240), memory=CFG.get("memory", 512),
            description=CFG.get("description", ""),
        )
    except Exception as e:
        fails.append(f"deploy failed: {str(e)[:200]}")
    if fails:
        for f in fails:
            rep.fail(f)
        rep.kv(n_fails=len(fails), verdict="FAIL")
        sys.exit(1)

    rep.section("2. Fresh output")
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
        rep.fail("v2 output never freshened (import error? shim injection "
                 "regression?)")
        rep.kv(n_fails=1, verdict="FAIL")
        sys.exit(1)
    rep.ok(f"fresh doc {doc['generated_at']}")

    rep.section("3. Gates")
    reg = doc.get("regime") or {}
    lbl = reg.get("label")
    srcs = reg.get("sources") or []
    if not isinstance(lbl, str) or not lbl or lbl == "Unknown" \
            or "{" in lbl:
        fails.append(f"regime label malformed: {str(lbl)[:80]!r}")
    n_src = len(srcs)
    if n_src < 4:
        fails.append(f"regime sources={n_src} (<4)")
    elif n_src == 4:
        warns.append("regime sources=4 — RORO chip still missing")
    else:
        rep.ok(f"regime={lbl} sources={n_src}")
    roro_src = next((s for s in srcs if s.get("k") == "roro"), None)
    if roro_src:
        rep.ok(f"RORO chip live: {roro_src.get('value')} "
               f"({roro_src.get('score')})")

    cards = (doc.get("top_calls") or []) + (doc.get("watchlist") or [])
    tiers = {}
    for c in cards:
        st = c.get("stats") or {}
        tiers[st.get("source")] = tiers.get(st.get("source"), 0) + 1
    rep.kv(tiers=json.dumps(tiers), n_cards=len(cards))
    if tiers.get("magdist", 0) == 0:
        fails.append("subset matcher produced ZERO magdist-tier cards — "
                     "forensics predicted ≥4")
    elif tiers.get("magdist", 0) < 3:
        warns.append(f"only {tiers.get('magdist')} magdist cards — expected "
                     "~4-5 from forensics")

    for c in cards:
        st = c.get("stats") or {}
        if st.get("source") == "magdist":
            line = (f"· {c.get('subject')}: via={st.get('matched_signals')} "
                    f"n={st.get('n')} h={st.get('horizon_days')}d "
                    f"median={st.get('median')} win={st.get('win_rate')} "
                    f"stop/tgt={c.get('stop_pct')}/{c.get('target_pct')} "
                    f"kelly={(c.get('sizing') or {}).get('kelly_pct')}%")
            rep.log(line)
            if c.get("stop_pct") is None or c.get("target_pct") is None:
                warns.append(f"magdist card '{c.get('subject')}' missing "
                             "stop/target despite quantiles")

    tr = doc.get("track_record") or {}
    if not tr.get("quotes_available"):
        warns.append("quotes_available=False this run")
    rep.kv(open_calls=tr.get("open_calls"),
           quotes=tr.get("quotes_available"))

    for w in warns:
        rep.warn(w)
    for f in fails:
        rep.fail(f)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        sys.exit(1)
    sys.exit(0)
