"""ops 3254 — the PREDICTION LAYER + yesterday's-additions audit.

  1. Deploy the runner (prediction block: panel theses tested AS
     predictions — composite vs real target, 13w lead corr, extreme hit
     rate, current calls with odds), fleet run.
  2. Verify data/wl-predictions.json: n_theses > 0, print the top calls
     with their evidence — including the HQM example if it qualifies.
  3. YESTERDAY'S TV ADDITIONS: diff harvest lists vs engine tv_ids —
     any list without an engine is named (should be zero; the spec set
     derives from the live harvest each run). tv-notes feed presence
     reported honestly (extension harvest is PENDING-KHALID).
  4. PREDICTIONS board live on panels.html (source literal).
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
UA = {"User-Agent": "Mozilla/5.0 (jh-ops-3254)"}


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


with report("3254_prediction_layer") as rep:
    fails, warns = [], []
    rep.heading("ops 3254 — prediction layer live + additions audit")

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
                      env_vars=live, eb_rule_name=rule, eb_schedule=cron,
                      timeout=cfg.get("timeout", 900),
                      memory=cfg.get("memory", 3008),
                      description=str(cfg.get("description", ""))[:250],
                      smoke=False)
        LAM.get_waiter("function_updated_v2").wait(
            FunctionName=FN, WaiterConfig={"Delay": 2, "MaxAttempts": 30})
    except Exception as e:
        fails.append(f"deploy: {str(e)[:80]}")

    if not fails:
        rep.section("1. Fleet run")
        mark = datetime.now(timezone.utc).isoformat()
        LAM.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
        idx = None
        for _ in range(70):
            time.sleep(10)
            d = s3_json("data/wl-engines.json") or {}
            if str(d.get("generated_at", "")) > mark:
                idx = d
                break
        if not idx:
            fails.append("index not fresh in window")

    if not fails:
        rep.section("2. Predictions feed")
        P = s3_json("data/wl-predictions.json") or {}
        preds = P.get("predictions") or []
        rep.kv(n_theses=P.get("n_theses"),
                 feed_generated=str(P.get("generated_at"))[:19])
        hqm = next((r for r in preds
                    if "high quality" in str(r.get("name", "")).lower()),
                   None)
        show = ([hqm] if hqm else []) + [r for r in preds
                                         if r is not hqm][:6]
        for r in show:
            if not r:
                continue
            rep.log(f"  {str(r['name'])[:52]:<52} → {r['target']:<16} "
                    f"corr {r['lead_corr']:+.3f} "
                    f"ext {r['extreme_hit_pct'] or '—'}% "
                    f"(n={r['extreme_n']}) base {r['base_up_pct']}% "
                    f"z={r['predictor_z_now']} · {r['current_call']}")
        if preds:
            rep.ok(f"{len(preds)} panel theses tested as predictions")
        else:
            fails.append("predictions feed empty")

        rep.section("3. Yesterday's TV additions")
        wl = s3_json("data/tv-watchlists.json") or {}
        lists = [l for l in (wl.get("lists") or [])
                 if not str(l.get("id", "")).startswith("e2e-")]
        tvids = {str(e.get("tv_id")) for e in (idx.get("engines") or [])}
        orphan = [l for l in lists if str(l.get("id")) not in tvids]
        rep.kv(harvest_lists=len(lists),
               engines=len(idx.get("engines") or []),
               lists_without_engine=len(orphan),
               harvest_generated=str(wl.get("generated_at"))[:19])
        for l in orphan[:6]:
            rep.log(f"  NEW/unengined: {str(l.get('name'))[:60]}")
        notes = s3_json("data/tv-notes.json")
        rep.kv(tv_notes_feed="PRESENT" if notes else
               "absent — extension harvest is PENDING-KHALID")

        rep.section("4. PREDICTIONS board live")
        okp = False
        for i in range(20):
            try:
                h = urllib.request.urlopen(urllib.request.Request(
                    "https://justhodl.ai/panels.html?t="
                    f"{int(time.time())}", headers=UA),
                    timeout=15).read().decode("utf-8", "replace")
                if "ops 3254: PREDICTIONS" in h:
                    okp = True
                    rep.ok(f"board live (~{(i + 1) * 15}s)")
                    break
            except Exception:
                pass
            time.sleep(15)
        if not okp:
            warns.append("board literal not live in window")

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
