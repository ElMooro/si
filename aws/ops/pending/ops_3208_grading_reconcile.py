"""ops 3208 — the scorecard learns his panels + the 207-vs-162 reconcile.

Two audit findings closed:
  1. GRADING NEVER WIRED: the 3176 design said the outcome-checker grades
     each watchlist as signal_type wl_<slug> — but the checker reads
     DynamoDB justhodl-signals and the runner never wrote there. Now every
     FIRING panel logs one guarded, deduped (engine-week conditional put)
     directional signal vs SPY with the panel's own historical 13w tilt as
     direction. The scorecard grows one row per watchlist; the fusion
     PROVEN gate eventually feeds off real out-of-sample hits, not just
     in-sample event studies.
  2. SILENT DEMOTION: 45 engines (207 specs → 162 index) vanished because
     two demotion paths `continue`d without appending a row. Demoted
     engines now land in the index as DORMANT with a NAMED reason
     ("members lack fetchable history" / "<100 weeks joint history") —
     which is itself a panel-coverage worklist for Khalid.

Gates: fresh index where engines == list count (nothing silent), reason
histogram reported, wl_ signals present in DDB, checker ingests clean.
"""
import json
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import boto3
from boto3.dynamodb.conditions import Attr

from ops_report import report

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "shared"))

from _lambda_deploy_helpers import deploy_lambda  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
AWS_DIR = Path(__file__).resolve().parents[2]
S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
DDB = boto3.resource("dynamodb", region_name=REGION)
FN = "justhodl-wl-engines"


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


with report("3208_grading_reconcile") as rep:
    fails, warns = [], []
    rep.heading("ops 3208 — panels enter the trust ledger; nothing is "
                "silently dropped")

    # ── 1. deploy + full run ───────────────────────────────────────────
    rep.section("1. Deploy patched runner + full run")
    cfg = json.loads((AWS_DIR / "lambdas" / FN / "config.json").read_text())
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
    except Exception as e:
        fails.append(f"deploy: {str(e)[:90]}")
    mark = datetime.now(timezone.utc).isoformat()
    try:
        LAM.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
    except Exception as e:
        fails.append(f"invoke: {str(e)[:80]}")
    idx = None
    for _ in range(66):
        time.sleep(10)
        d = s3_json("data/wl-engines.json") or {}
        if str(d.get("generated_at", "")) > mark:
            idx = d
            break
    if not idx:
        fails.append("index not regenerated — abort verification")

    # ── 2. reconcile: engines == lists, reasons named ──────────────────
    if idx:
        rep.section("2. Reconcile 207-vs-162")
        wl = s3_json("data/tv-watchlists.json") or {}
        n_lists = len([l for l in (wl.get("lists") or [])
                       if not str(l.get("id", "")).startswith("e2e-")])
        eng = idx.get("engines") or []
        active = sum(1 for e in eng if str(e.get("state")) == "ACTIVE")
        dormant = len(eng) - active
        firing = sum(1 for e in eng if e.get("firing"))
        rep.kv(lists=n_lists, index_engines=len(eng), active=active,
               dormant=dormant, firing=firing,
               series_cached=idx.get("series_cached"))
        reasons = Counter((e.get("reason") or "").split("(")[0].strip()
                          for e in eng if e.get("state") == "DORMANT")
        for rzn, n in reasons.most_common(5):
            rep.log(f"  DORMANT {n:>3} × {rzn[:70]}")
        if len(eng) < n_lists:
            fails.append(f"index still drops engines: {len(eng)} < "
                         f"{n_lists} lists")
        else:
            rep.ok(f"every list accounted for — {len(eng)}/{n_lists}, "
                   "zero silent drops")

    # ── 3. trust-ledger signals present ────────────────────────────────
    rep.section("3. wl_ signals in DynamoDB")
    n_sig, samples = 0, []
    try:
        tbl = DDB.Table("justhodl-signals")
        resp = tbl.scan(
            FilterExpression=Attr("signal_type").begins_with("wl_"),
            ProjectionExpression="signal_id, signal_type, "
                                 "predicted_direction, baseline_price",
            Limit=1000)
        items = resp.get("Items") or []
        n_sig = len(items)
        samples = items[:4]
    except Exception as e:
        fails.append(f"ddb scan: {str(e)[:80]}")
    rep.kv(wl_signals_in_ledger=n_sig)
    for it in samples:
        rep.log(f"  {it.get('signal_id', '')[:52]}  "
                f"{it.get('predicted_direction')}  "
                f"base {it.get('baseline_price')}")
    if idx and not n_sig and (firing or 0) > 0:
        fails.append("firing panels exist but zero wl_ signals landed")

    # ── 4. checker ingests clean ───────────────────────────────────────
    rep.section("4. Outcome-checker ingest")
    try:
        r = LAM.invoke(FunctionName="justhodl-outcome-checker",
                       InvocationType="RequestResponse", Payload=b"{}")
        code = r.get("StatusCode")
        err = r.get("FunctionError")
        rep.kv(checker_status=code, function_error=err or "none")
        if err:
            fails.append(f"checker errored on the new rows: {err}")
        else:
            rep.ok("checker swept the ledger clean — windows score as "
                   "they elapse (7/28/91d)")
    except Exception as e:
        warns.append(f"checker invoke: {str(e)[:80]}")

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
