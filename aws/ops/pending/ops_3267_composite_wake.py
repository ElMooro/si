"""ops 3267 — COMPOSITE MODE: waking the size-gated panels.

Small/sparse panels (1–5 members, or <6 z-scorable, or <100 joint
breadth weeks) could never activate no matter how well-mapped — HQM
sat dormant at coverage 1.00. They now run honestly on mean-member z
(composite mode): extremeness percentile vs own history, firing at
|z|≥1.5, real extreme-event study, FDR pool, grading pipeline, and
auto-join to the prediction layer via comps{}. Breadth engines
untouched by construction (composite only fires where breadth gates
fail).

Verify: deploy → fleet run → CENSUS (breadth vs composite vs residual
dormant reason histogram), HQM specifically (Khalid's original
example), one legacy breadth engine byte-identical semantics
(no mode field, study intact), predictions delta, drawer mode tag
live.
"""
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3

from ops_report import report

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "shared"))
from _lambda_deploy_helpers import deploy_lambda  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
FN = "justhodl-wl-engines"
AWS_DIR = Path(__file__).resolve().parents[2]
UA = {"User-Agent": "Mozilla/5.0 (jh-ops-3267)"}


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


with report("3267_composite_wake") as rep:
    fails, warns = [], []
    rep.heading("ops 3267 — composite mode wakes the size-gated "
                "panels")

    before = s3_json("data/wl-engines.json") or {}
    b_eng = before.get("engines") or []
    b_active = sum(1 for e in b_eng
                   if str(e.get("state")) == "ACTIVE")
    rep.kv(active_before=b_active, total=len(b_eng))

    cfg = json.loads((AWS_DIR / "lambdas" / FN / "config.json")
                     .read_text())
    sch = cfg.get("schedule")
    rule, cron = (sch.get("rule_name"), sch.get("cron")) \
        if isinstance(sch, dict) else (None, None)
    live = (LAM.get_function_configuration(FunctionName=FN)
            .get("Environment") or {}).get("Variables") or {}
    try:
        deploy_lambda(report=rep, function_name=FN,
                      source_dir=AWS_DIR / "lambdas" / FN / "source",
                      env_vars=live, eb_rule_name=rule,
                      eb_schedule=cron,
                      timeout=cfg.get("timeout", 900),
                      memory=cfg.get("memory", 3008),
                      description=str(cfg.get("description", ""))[:250],
                      smoke=False)
        LAM.get_waiter("function_updated_v2").wait(
            FunctionName=FN, WaiterConfig={"Delay": 2,
                                           "MaxAttempts": 30})
    except Exception as e:
        fails.append(f"deploy: {str(e)[:80]}")

    idx = None
    if not fails:
        rep.section("1. Fleet run")
        mark = datetime.now(timezone.utc).isoformat()
        LAM.invoke(FunctionName=FN, InvocationType="Event",
                   Payload=b"{}")
        for _ in range(70):
            time.sleep(10)
            d = s3_json("data/wl-engines.json") or {}
            if str(d.get("generated_at", "")) > mark:
                idx = d
                break
        if not idx:
            fails.append("index not fresh in window")

    if not fails:
        rep.section("2. Census")
        eng = idx.get("engines") or []
        act = [e for e in eng if str(e.get("state")) == "ACTIVE"]
        comp = [e for e in act if e.get("mode") == "composite"]
        breadth = [e for e in act if not e.get("mode")]
        dorm = [e for e in eng if str(e.get("state")) != "ACTIVE"]
        hist = {}
        for e in dorm:
            k = str(e.get("reason", ""))[:44]
            hist[k] = hist.get(k, 0) + 1
        rep.kv(active_after=len(act), breadth=len(breadth),
               composite=len(comp), dormant=len(dorm))
        for k, v in sorted(hist.items(), key=lambda x: -x[1])[:4]:
            rep.log(f"  residual {v:>3}× {k}")
        for e in comp[:6]:
            rep.log(f"  woke [{e.get('theme')}] "
                    f"{str(e.get('name'))[:52]} z="
                    f"{e.get('activation_now')} "
                    f"pct={e.get('activation_pctile')}"
                    f"{' FIRING' if e.get('firing') else ''}")
        if len(act) <= b_active:
            fails.append("no net wakes — composite mode did not "
                         "engage")

        rep.section("3. HQM — Khalid's original example")
        hq = next((e for e in eng if "high quality" in
                   str(e.get("name", "")).lower()), None)
        if hq and str(hq.get("state")) == "ACTIVE":
            doc = s3_json(f"data/engines/{hq['engine_id']}.json") or {}
            rep.ok(f"HQM AWAKE (mode={doc.get('mode')}): "
                   f"z={doc.get('activation_now')} "
                   f"pct={doc.get('activation_pctile')} "
                   f"n_weeks={doc.get('n_weeks')} members="
                   f"{doc.get('members_resolved')}/"
                   f"{doc.get('members_total')}")
        elif hq:
            warns.append(f"HQM still dormant: "
                         f"{str(hq.get('reason'))[:70]}")

        rep.section("4. Legacy breadth engine untouched")
        fx = s3_json("data/engines/wl-foreign-exchange-reserves.json")\
            or {}
        if fx.get("state") == "ACTIVE" and not fx.get("mode") \
                and (fx.get("event_study") or {}).get("w13"):
            rep.ok("foreign-exchange-reserves: breadth semantics "
                   f"intact (no mode field, w13 n="
                   f"{fx['event_study']['w13'].get('n')})")
        else:
            fails.append("legacy breadth engine altered — inspect")

        rep.section("5. Predictions delta + page")
        P = s3_json("data/wl-predictions.json") or {}
        rep.kv(prediction_theses=P.get("n_theses"))
        hqp = next((r for r in (P.get("predictions") or [])
                    if "high quality" in str(r.get("name", "")).lower()),
                   None)
        if hqp:
            rep.ok(f"HQM thesis LIVE in predictions: → "
                   f"{hqp.get('target')} corr {hqp.get('lead_corr')} "
                   f"call {hqp.get('current_call')}")
        okp = False
        for i in range(20):
            try:
                h = urllib.request.urlopen(urllib.request.Request(
                    "https://justhodl.ai/panels.html?t="
                    f"{int(time.time())}", headers=UA),
                    timeout=15).read().decode("utf-8", "replace")
                if "mode <b>" in h:
                    okp = True
                    rep.ok(f"drawer mode tag live (~{(i + 1) * 15}s)")
                    break
            except Exception:
                pass
            time.sleep(15)
        if not okp:
            warns.append("mode-tag literal not live in window")

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
