"""ops 3199 — the runner died silently after 3191: diagnose with evidence,
patch the risk surface, prove it breathes again.

3198 measured the failure: data/wl-engines.json frozen at 02:46 UTC — the
last successful run was on the 3190 map. Every kick since (3191-3198)
produced nothing. Prime suspects: the DBnomics API (documented slow under
load) and Coin Metrics paging pushing total fetch time past the Lambda
timeout; the runner's FETCH_BUDGET gates new submissions but joined
threads still block exit.

This ops:
  1. EVIDENCE — function config + the CloudWatch tail since 03:00 UTC
     (timeouts and tracebacks land in the report, not in guesswork).
  2. PATCH — series_source hardened (DBnomics 12s, CoinMetrics 15s and
     3-page cap, COT 15s, USI rstrip bug fixed) + runner redeployed with
     the bundle.
  3. PROVE — Event re-run, poll for a FRESH index, report the real
     ACTIVE/DORMANT/FIRING split. Fails if the index stays stale.
"""
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3

from ops_report import report

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "shared"))

from _lambda_deploy_helpers import deploy_lambda  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
AWS_DIR = Path(__file__).resolve().parents[2]
S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
LOGS = boto3.client("logs", region_name=REGION)
SHARED_CONSUMERS = ("justhodl-wl-engines", "justhodl-thesis-engine",
                    "justhodl-symbol-dictionary")


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


with report("3199_runner_revive") as rep:
    fails, warns = [], []
    rep.heading("ops 3199 — diagnose the dead runner, patch, prove")

    # ── 1. evidence ────────────────────────────────────────────────────
    rep.section("1. Evidence: config + CloudWatch tail")
    try:
        cfgL = LAM.get_function_configuration(
            FunctionName="justhodl-wl-engines")
        rep.kv(timeout=cfgL.get("Timeout"), memory=cfgL.get("MemorySize"),
               last_modified=str(cfgL.get("LastModified"))[:19],
               state=cfgL.get("State"))
    except Exception as e:
        warns.append(f"get config: {str(e)[:70]}")
    try:
        grp = "/aws/lambda/justhodl-wl-engines"
        streams = LOGS.describe_log_streams(
            logGroupName=grp, orderBy="LastEventTime", descending=True,
            limit=3).get("logStreams") or []
        shown = 0
        for st in streams:
            evs = LOGS.get_log_events(
                logGroupName=grp, logStreamName=st["logStreamName"],
                limit=60, startFromHead=False).get("events") or []
            for e in evs:
                msg = (e.get("message") or "").rstrip()
                if any(k in msg for k in ("Task timed out", "Error",
                                          "Traceback", "MemoryError",
                                          "[wl]", "REPORT")):
                    rep.log("  " + msg[:150])
                    shown += 1
                if shown >= 26:
                    break
            if shown >= 26:
                break
        if not shown:
            warns.append("no diagnostic lines in the last 3 log streams")
    except Exception as e:
        warns.append(f"logs: {str(e)[:80]}")

    # ── 2. redeploy the patched bundle ─────────────────────────────────
    rep.section("2. Redeploy patched shared bundle")
    for fn in SHARED_CONSUMERS:
        try:
            cfg = {}
            p = AWS_DIR / "lambdas" / fn / "config.json"
            if p.exists():
                cfg = json.loads(p.read_text())
            live = (LAM.get_function_configuration(FunctionName=fn)
                    .get("Environment") or {}).get("Variables") or {}
            sch = cfg.get("schedule") or {}
            deploy_lambda(report=rep, function_name=fn,
                          source_dir=AWS_DIR / "lambdas" / fn / "source",
                          env_vars=live, eb_rule_name=sch.get("rule_name"),
                          eb_schedule=sch.get("cron"),
                          timeout=cfg.get("timeout", 900),
                          memory=cfg.get("memory", 1024),
                          description=str(cfg.get("description", ""))[:250],
                          smoke=False)
        except Exception as e:
            fails.append(f"deploy {fn}: {str(e)[:90]}")

    # ── 3. prove it breathes ───────────────────────────────────────────
    rep.section("3. Re-run + fresh-index gate")
    mark = datetime.now(timezone.utc).isoformat()
    try:
        LAM.invoke(FunctionName="justhodl-wl-engines",
                   InvocationType="Event", Payload=b"{}")
    except Exception as e:
        fails.append(f"invoke: {str(e)[:80]}")
    idx, gen = None, ""
    for _ in range(70):
        time.sleep(10)
        d = s3_json("data/wl-engines.json") or {}
        gen = str(d.get("generated_at", ""))
        if gen > mark:
            idx = d
            break
    if not idx:
        # one more evidence pull: what did THIS run say before dying?
        try:
            streams = LOGS.describe_log_streams(
                logGroupName="/aws/lambda/justhodl-wl-engines",
                orderBy="LastEventTime", descending=True,
                limit=1).get("logStreams") or []
            if streams:
                evs = LOGS.get_log_events(
                    logGroupName="/aws/lambda/justhodl-wl-engines",
                    logStreamName=streams[0]["logStreamName"], limit=14,
                    startFromHead=False).get("events") or []
                for e in evs:
                    rep.log("  tail: " + (e.get("message") or "")[:150])
        except Exception:
            pass
        fails.append(f"index STILL stale after patched re-run "
                     f"(generated_at={gen or 'missing'})")
    else:
        eng = idx.get("engines") or []
        active = sum(1 for e in eng if str(e.get("state")) == "ACTIVE")
        dormant = sum(1 for e in eng if str(e.get("state")) == "DORMANT")
        firing = sum(1 for e in eng
                     if any(str(e.get(k)) == "FIRING"
                            for k in ("signal", "fire", "status", "panel")))
        rep.kv(generated_at=gen[:19], engines=len(eng), active=active,
               dormant=dormant, firing=firing,
               series_cached=idx.get("series_cached"))
        rep.ok("runner ALIVE on the final map — index fresh")
        if active < 100:
            warns.append(f"active {active} < 100 — widening should not "
                         "reduce actives; audit next")

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
