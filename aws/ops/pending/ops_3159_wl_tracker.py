"""ops 3159 — watchlist-tracker engine live + harvest state check.

New engine justhodl-tv-watchlist-tracker (daily 21:15 UTC TUE-SAT):
each synced TradingView list = equal-weight basket; 5/21/63d returns
vs SPY from Polygon (90d backfill + incremental state); Mondays emit
one signal per list (signal_type tvwl_<slug>) into justhodl-signals →
scorecard grows ONE ROW PER WATCHLIST. WAITING_FIRST_SYNC when the
extension hasn't delivered lists yet.

THIS OP: report harvest state verbatim (did Khalid's sync land?) →
create+deploy the engine (env from donor justhodl-etf-fund-flows for
POLYGON_KEY) → invoke → gate on doc freshness + coherent status.
tv-notes.html board rides the pages deploy (marker warn-only).
"""

import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3

from ops_report import report
from _lambda_deploy_helpers import deploy_lambda

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-tv-watchlist-tracker"
DONOR = "justhodl-etf-fund-flows"
HERE = Path(__file__).resolve().parent
AWS_DIR = HERE.parents[1]

S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)


def s3_json(key):
    o = S3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(o["Body"].read().decode("utf-8"))


with report("3159_wl_tracker") as rep:
    fails, warns = [], []
    rep.heading("ops 3159 — watchlist tracker")

    rep.section("1. Harvest state (did the sync land?)")
    try:
        wl = s3_json("data/tv-watchlists.json")
    except Exception:
        wl = {}
    real = [l for l in (wl.get("lists") or [])
            if not str(l.get("id", "")).startswith("e2e-")]
    notes = {}
    try:
        notes = s3_json("data/tradingview-notes.json")
    except Exception:
        pass
    rep.kv(watchlists_synced=len(real),
           notes_mirror=len(notes.get("notes") or []),
           lists_doc_at=wl.get("generated_at"))
    if real:
        for l in real[:8]:
            rep.log(f"  · {l.get('name')} ({l.get('n')} symbols)")
        rep.ok(f"HARVEST LANDED: {len(real)} real watchlists")
    else:
        warns.append("no real watchlists yet — extension sync pending; "
                     "engine ships in WAITING state and self-activates")

    rep.section("2. Deploy engine")
    cfg = json.loads((AWS_DIR / "lambdas" / FN / "config.json").read_text())
    donor_env = (LAM.get_function_configuration(FunctionName=DONOR)
                 .get("Environment") or {}).get("Variables") or {}
    env = {k: v for k, v in donor_env.items()
           if k in ("POLYGON_KEY", "S3_BUCKET")}
    env.setdefault("S3_BUCKET", BUCKET)
    sched = cfg.get("schedule") or {}
    deploy_lambda(report=rep, function_name=FN,
                  source_dir=AWS_DIR / "lambdas" / FN / "source",
                  env_vars=env,
                  eb_rule_name=sched.get("rule_name"),
                  eb_schedule=sched.get("cron"),
                  timeout=cfg.get("timeout", 240),
                  memory=cfg.get("memory", 512),
                  description=cfg.get("description", "")[:250],
                  smoke=False)

    rep.section("3. Invoke + gate")
    t0 = datetime.now(timezone.utc)
    LAM.invoke(FunctionName=FN, InvocationType="Event",
               Payload=json.dumps({"force_emit": bool(real)}).encode())
    doc = None
    deadline = time.time() + 260
    while time.time() < deadline:
        try:
            d = s3_json("data/tv-watchlist-tracker.json")
            if datetime.fromisoformat(d["generated_at"]) >= t0:
                doc = d
                break
        except Exception:
            pass
        time.sleep(12)
    if doc is None:
        fails.append("tracker doc never freshened")
    else:
        rep.kv(status=doc.get("status"), n_lists=doc.get("n_lists"),
               signals_logged=doc.get("signals_logged"),
               elapsed_s=doc.get("elapsed_s"))
        if real:
            if doc.get("status") != "LIVE" or not doc.get("lists"):
                fails.append("lists synced but tracker not LIVE")
            else:
                for r in (doc.get("lists") or [])[:6]:
                    rep.log(f"  · {r.get('name')}: 21d "
                            f"{r.get('ret_21d')}% (excess "
                            f"{r.get('excess_21d')}%) "
                            f"n={r.get('n_priced')}/{r.get('n_symbols')}")
                rep.ok("TRACKER LIVE with real baskets")
                if not doc.get("signals_logged"):
                    warns.append("0 signals logged on force_emit — check "
                                 "n_priced thresholds")
        else:
            if doc.get("status") == "WAITING_FIRST_SYNC":
                rep.ok("engine armed in WAITING_FIRST_SYNC — "
                       "self-activates on tomorrow's run after sync")
            else:
                fails.append(f"unexpected status {doc.get('status')}")

    rep.section("4. Page board (warn-only)")
    try:
        r = urllib.request.urlopen(urllib.request.Request(
            f"https://justhodl.ai/tv-notes.html?t={int(time.time())}",
            headers={"User-Agent": "Mozilla/5.0 ops"}), timeout=15)
        if "JH_WL_TRACKER_V1" in r.read().decode("utf-8", "replace"):
            rep.ok("tracker board live on tv-notes.html")
        else:
            warns.append("CDN pre-board (self-heals)")
    except Exception as e:
        warns.append(f"page fetch: {str(e)[:60]}")

    for w in warns:
        rep.warn(w)
    for f in fails:
        rep.fail(f)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        sys.exit(1)
    sys.exit(0)
